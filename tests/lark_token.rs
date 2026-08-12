//! Token cache and error classification tests against a hand-rolled stub.

mod larkstub;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use lark_codex_bridge::lark::config::{LarkEndpoints, TenantBrand};
use lark_codex_bridge::lark::credentials::{
    CredentialStore, EnvCredentialsStore, FileCredentialStore, LarkCredentials,
};
use lark_codex_bridge::lark::error::{LarkError, LarkErrorKind};
use lark_codex_bridge::lark::http::LarkHttp;
use lark_codex_bridge::lark::token::TenantTokenProvider;
use lark_codex_bridge::limits::LARK_MAX_HTTP_BODY_BYTES;
use larkstub::{Handler, RecordedRequest, StubResponse, StubServer};
use secrecy::{ExposeSecret, SecretString};
use url::Url;

const TEST_APP_ID: &str = "cli_test_app";
const TEST_APP_SECRET: &str = "test-secret-material";

fn test_credentials() -> LarkCredentials {
    LarkCredentials::new(
        TEST_APP_ID.to_owned(),
        SecretString::from(TEST_APP_SECRET),
        TenantBrand::Feishu,
    )
}

fn endpoints_for(server: &StubServer) -> LarkEndpoints {
    let base = Url::parse(&server.url()).expect("stub URL should parse");
    LarkEndpoints {
        open_base: base.clone(),
        accounts_base: base,
    }
}

fn provider_for(server: &StubServer) -> TenantTokenProvider {
    let http = LarkHttp::new(endpoints_for(server)).expect("HTTP client should build");
    TenantTokenProvider::new(http, test_credentials())
}

fn token_handler(calls: &Arc<AtomicUsize>, expire: i64, delay: Duration) -> Handler {
    let calls = Arc::clone(calls);
    Arc::new(move |request: &RecordedRequest| {
        if request.path != "/open-apis/auth/v3/tenant_access_token/internal" {
            return StubResponse::json(404, r#"{"code":1}"#);
        }
        let sequence = calls.fetch_add(1, Ordering::SeqCst);
        StubResponse::json(
            200,
            &format!(r#"{{"code":0,"tenant_access_token":"token-{sequence}","expire":{expire}}}"#),
        )
        .with_delay(delay)
    })
}

#[tokio::test]
async fn first_fetch_caches_the_token() {
    let calls = Arc::new(AtomicUsize::new(0));
    let server = StubServer::start(token_handler(&calls, 7200, Duration::ZERO)).await;
    let provider = provider_for(&server);

    let first = provider.token().await.expect("first token should succeed");
    let second = provider.token().await.expect("cached token should succeed");

    assert_eq!(first.expose_secret(), "token-0");
    assert_eq!(second.expose_secret(), "token-0");
    assert_eq!(server.request_count(), 1, "cache hit must not refetch");

    let request = &server.requests()[0];
    assert_eq!(request.method, "POST");
    assert_eq!(
        request.path,
        "/open-apis/auth/v3/tenant_access_token/internal"
    );
    let body: serde_json::Value =
        serde_json::from_str(&request.body_text()).expect("token request should be JSON");
    assert_eq!(body["app_id"], TEST_APP_ID);
    assert_eq!(body["app_secret"], TEST_APP_SECRET);
}

#[tokio::test]
async fn token_inside_the_skew_window_is_refreshed() {
    let calls = Arc::new(AtomicUsize::new(0));
    // 120 s validity is shorter than the 3 minute refresh skew, so the token
    // is stale immediately and every call must refetch.
    let server = StubServer::start(token_handler(&calls, 120, Duration::ZERO)).await;
    let provider = provider_for(&server);

    let first = provider.token().await.expect("first token should succeed");
    let second = provider.token().await.expect("second token should succeed");

    assert_eq!(first.expose_secret(), "token-0");
    assert_eq!(second.expose_secret(), "token-1");
    assert_eq!(server.request_count(), 2, "stale token must refetch");
}

#[tokio::test]
async fn concurrent_callers_share_a_single_flight_fetch() {
    let calls = Arc::new(AtomicUsize::new(0));
    let server = StubServer::start(token_handler(&calls, 7200, Duration::from_millis(250))).await;
    let provider = provider_for(&server);

    let mut tasks = Vec::new();
    for _ in 0..8 {
        tasks.push(tokio::spawn({
            let provider = provider.clone();
            async move { provider.token().await }
        }));
    }
    for task in tasks {
        let token = task
            .await
            .expect("caller task should join")
            .expect("token should succeed");
        assert_eq!(token.expose_secret(), "token-0");
    }
    assert_eq!(
        server.request_count(),
        1,
        "concurrent callers must share one fetch"
    );
}

#[tokio::test]
async fn invalid_credentials_are_permanent_auth() {
    let server = StubServer::start(Arc::new(|_| {
        StubResponse::json(200, r#"{"code":99991663,"msg":"app secret mismatch"}"#)
    }))
    .await;
    let provider = provider_for(&server);

    let error = provider
        .token()
        .await
        .expect_err("bad credentials must fail");

    assert_eq!(error.kind(), LarkErrorKind::PermanentAuth);
    assert!(matches!(
        error,
        LarkError::PermanentAuth {
            code: Some(99_991_663),
            ..
        }
    ));
    let rendered = format!("{error} {error:?}");
    assert!(!rendered.contains(TEST_APP_SECRET));
    assert!(
        !rendered.contains("mismatch"),
        "server messages are discarded"
    );
}

#[tokio::test]
async fn server_error_is_retryable() {
    let server = StubServer::start(Arc::new(|_| StubResponse::json(500, r#"{"code":1}"#))).await;
    let provider = provider_for(&server);

    let error = provider.token().await.expect_err("HTTP 500 must fail");

    assert!(matches!(
        error,
        LarkError::Retryable {
            code: Some(500),
            ..
        }
    ));
}

#[tokio::test]
async fn connect_failure_is_retryable() {
    // Bind and immediately drop a listener to get an address that refuses
    // connections.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("probe listener should bind");
    let addr = listener.local_addr().expect("probe listener address");
    drop(listener);
    let base = Url::parse(&format!("http://{addr}")).expect("dead URL should parse");
    let endpoints = LarkEndpoints {
        open_base: base.clone(),
        accounts_base: base,
    };
    let http = LarkHttp::new(endpoints).expect("HTTP client should build");
    let provider = TenantTokenProvider::new(http, test_credentials());

    let error = provider.token().await.expect_err("connect must fail");

    assert_eq!(error.kind(), LarkErrorKind::Retryable);
}

#[tokio::test]
async fn oversized_content_length_is_rejected_before_parsing() {
    let oversize = (LARK_MAX_HTTP_BODY_BYTES + 1).to_string();
    let server = StubServer::start(Arc::new(move |_| StubResponse {
        status: 200,
        headers: vec![
            ("content-type".to_owned(), "application/json".to_owned()),
            ("content-length".to_owned(), oversize.clone()),
        ],
        body: b"{}".to_vec(),
        delay: Duration::ZERO,
        close_delimited: false,
    }))
    .await;
    let provider = provider_for(&server);

    let error = provider.token().await.expect_err("oversize body must fail");

    assert!(matches!(error, LarkError::Exhausted { .. }));
}

#[tokio::test]
async fn oversized_streaming_body_is_rejected_mid_body() {
    let server = StubServer::start(Arc::new(|_| {
        StubResponse::text(200, &"x".repeat(LARK_MAX_HTTP_BODY_BYTES + 1)).close_delimited()
    }))
    .await;
    let provider = provider_for(&server);

    let error = provider.token().await.expect_err("oversize body must fail");

    assert!(matches!(error, LarkError::Exhausted { .. }));
}

#[tokio::test]
async fn debug_output_never_contains_secret_material() {
    let server = StubServer::start(token_handler(
        &Arc::new(AtomicUsize::new(0)),
        7200,
        Duration::ZERO,
    ))
    .await;
    let provider = provider_for(&server);
    let token = provider.token().await.expect("token should succeed");

    let creds_debug = format!("{:?}", test_credentials());
    let provider_debug = format!("{provider:?}");

    assert!(!creds_debug.contains(TEST_APP_SECRET));
    assert!(creds_debug.contains("<redacted>"));
    assert!(!provider_debug.contains(TEST_APP_SECRET));
    assert!(
        !provider_debug.contains(token.expose_secret()),
        "cached tenant token must not leak through Debug"
    );
}

#[tokio::test]
async fn bot_info_returns_a_sanitized_identity() {
    let server = StubServer::start(Arc::new(|request: &RecordedRequest| {
        match request.path.as_str() {
            "/open-apis/auth/v3/tenant_access_token/internal" => StubResponse::json(
                200,
                r#"{"code":0,"tenant_access_token":"token-0","expire":7200}"#,
            ),
            "/open-apis/bot/v3/info" => StubResponse::json(
                200,
                r#"{"code":0,"bot":{"app_name":"Bridge Bot","open_id":"ou_bot"}}"#,
            ),
            _ => StubResponse::json(404, r#"{"code":1}"#),
        }
    }))
    .await;
    let provider = provider_for(&server);

    let info = provider.bot_info().await.expect("bot info should succeed");

    assert_eq!(info.app_name.as_deref(), Some("Bridge Bot"));
    assert_eq!(info.open_id.as_deref(), Some("ou_bot"));
    let info_request = server
        .requests()
        .into_iter()
        .find(|request| request.path == "/open-apis/bot/v3/info")
        .expect("bot info request should be recorded");
    assert_eq!(info_request.header("authorization"), Some("Bearer token-0"));
}

#[test]
fn credential_file_roundtrip_preserves_fields_and_permissions() {
    let dir = tempfile::tempdir().expect("tempdir should be created");
    let path = dir.path().join("nested").join("credentials.toml");
    let store = FileCredentialStore::new(path.clone());

    assert!(store.load().expect("empty load should succeed").is_none());

    store
        .save(&test_credentials())
        .expect("save should succeed");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&path)
            .expect("credentials file should exist")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "credentials file must be private");
    }

    let loaded = store
        .load()
        .expect("load should succeed")
        .expect("credentials should be stored");
    assert_eq!(loaded.app_id, TEST_APP_ID);
    assert_eq!(loaded.app_secret.expose_secret(), TEST_APP_SECRET);
    assert_eq!(loaded.tenant, TenantBrand::Feishu);

    let debug = format!("{loaded:?}");
    assert!(!debug.contains(TEST_APP_SECRET));
}

#[test]
fn credential_file_rejects_malformed_content() {
    let dir = tempfile::tempdir().expect("tempdir should be created");
    let path = dir.path().join("credentials.toml");
    std::fs::write(&path, "not = [valid").expect("fixture should be written");
    let store = FileCredentialStore::new(path);

    let error = store.load().expect_err("malformed file must fail");

    assert!(matches!(error, LarkError::ProtocolViolation { .. }));
}

#[test]
fn env_override_requires_all_three_variables() {
    let none = EnvCredentialsStore::from_lookup(|_| None).expect("empty env should succeed");
    assert!(none.is_none());

    let partial = EnvCredentialsStore::from_lookup(|key| {
        if key == "LARK_APP_ID" {
            Some(TEST_APP_ID.to_owned())
        } else {
            None
        }
    });
    assert!(matches!(partial, Err(LarkError::ProtocolViolation { .. })));

    let bad_tenant = EnvCredentialsStore::from_lookup(|key| match key {
        "LARK_APP_ID" => Some(TEST_APP_ID.to_owned()),
        "LARK_APP_SECRET" => Some(TEST_APP_SECRET.to_owned()),
        "LARK_TENANT" => Some("elsewhere".to_owned()),
        _ => None,
    });
    assert!(matches!(
        bad_tenant,
        Err(LarkError::ProtocolViolation { .. })
    ));

    let creds = EnvCredentialsStore::from_lookup(|key| match key {
        "LARK_APP_ID" => Some(TEST_APP_ID.to_owned()),
        "LARK_APP_SECRET" => Some(TEST_APP_SECRET.to_owned()),
        "LARK_TENANT" => Some("lark".to_owned()),
        _ => None,
    })
    .expect("full env should succeed")
    .expect("credentials should be present");
    assert_eq!(creds.tenant, TenantBrand::Lark);
    assert_eq!(creds.app_secret.expose_secret(), TEST_APP_SECRET);
}
