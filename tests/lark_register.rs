//! Registration device flow and existing-app onboarding tests against the
//! hand-rolled stub server.

mod larkstub;

use std::collections::VecDeque;
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use flate2::read::GzDecoder;
use lark_codex_bridge::lark::config::{LarkEndpoints, TenantBrand};
use lark_codex_bridge::lark::credentials::LarkCredentials;
use lark_codex_bridge::lark::error::{LarkError, LarkErrorKind};
use lark_codex_bridge::lark::http::LarkHttp;
use lark_codex_bridge::lark::register::{
    RegistrationFlow, RegistrationOutcome, encode_addons, validate_credentials,
};
use larkstub::{Handler, StubResponse, StubServer};
use secrecy::{ExposeSecret, SecretString};
use serde_json::{Value, json};
use url::Url;

fn endpoints_for(server: &StubServer) -> LarkEndpoints {
    let base = Url::parse(&server.url()).expect("stub URL should parse");
    LarkEndpoints {
        open_base: base.clone(),
        accounts_base: base,
    }
}

fn flow_for(server: &StubServer, addons: Option<Value>) -> RegistrationFlow {
    let http = LarkHttp::new(endpoints_for(server)).expect("HTTP client should build");
    RegistrationFlow::with_parts(
        http,
        Url::parse(&server.url()).expect("stub URL should parse"),
        addons,
        Duration::from_secs(60),
    )
}

fn begin_response() -> StubResponse {
    StubResponse::json(
        200,
        r#"{"device_code":"dc-1","verification_uri_complete":"https://accounts.feishu.cn/activate?code=xyz","expires_in":600,"interval":5}"#,
    )
}

fn scripted_handler(script: Vec<StubResponse>) -> Handler {
    let queue = Arc::new(Mutex::new(VecDeque::from(script)));
    Arc::new(move |_| {
        queue
            .lock()
            .expect("script lock")
            .pop_front()
            .unwrap_or_else(|| StubResponse::json(500, r#"{"error":"unexpected_request"}"#))
    })
}

#[tokio::test]
async fn begin_parses_the_challenge_and_builds_the_qr_url() {
    let server = StubServer::start(scripted_handler(vec![begin_response()])).await;
    let mut flow = flow_for(&server, None);

    let challenge = flow.begin().await.expect("begin should succeed");

    assert_eq!(challenge.expires_in, 600);
    assert_eq!(challenge.interval, 5);
    assert_eq!(flow.interval(), Duration::from_secs(5));
    let url = Url::parse(&challenge.url).expect("QR URL should parse");
    let params: Vec<(String, String)> = url
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    assert!(params.contains(&("code".to_owned(), "xyz".to_owned())));
    assert!(params.contains(&("from".to_owned(), "sdk".to_owned())));
    assert!(params.contains(&("source".to_owned(), "lark-codex-bridge".to_owned())));
    assert!(params.contains(&("tp".to_owned(), "sdk".to_owned())));

    let request = &server.requests()[0];
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/oauth/v1/app/registration");
    assert_eq!(
        request.header("content-type"),
        Some("application/x-www-form-urlencoded")
    );
    let body = request.body_text();
    for field in [
        "action=begin",
        "archetype=PersonalAgent",
        "auth_method=client_secret",
        "request_user_info=open_id",
    ] {
        assert!(body.contains(field), "begin form should contain {field}");
    }
}

#[tokio::test]
async fn begin_applies_server_defaults() {
    let server = StubServer::start(scripted_handler(vec![StubResponse::json(
        200,
        r#"{"device_code":"dc-1","verification_uri_complete":"https://accounts.feishu.cn/activate"}"#,
    )]))
    .await;
    let mut flow = flow_for(&server, None);

    let challenge = flow.begin().await.expect("begin should succeed");

    assert_eq!(challenge.expires_in, 600);
    assert_eq!(challenge.interval, 5);
}

#[tokio::test]
async fn qr_url_encodes_addons_as_gzip_base64url() {
    let server = StubServer::start(scripted_handler(vec![begin_response()])).await;
    let addons = json!({"scopes": {"tenant": ["im:message", "im:chat"]}});
    let mut flow = flow_for(&server, Some(addons.clone()));

    let challenge = flow.begin().await.expect("begin should succeed");

    let url = Url::parse(&challenge.url).expect("QR URL should parse");
    let encoded = url
        .query_pairs()
        .find(|(key, _)| key == "addons")
        .map(|(_, value)| value.into_owned())
        .expect("QR URL should carry addons");
    assert!(
        encoded
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
        "addons must be base64url without padding"
    );
    let gzip = URL_SAFE_NO_PAD
        .decode(&encoded)
        .expect("addons should be base64url");
    let mut json = String::new();
    GzDecoder::new(gzip.as_slice())
        .read_to_string(&mut json)
        .expect("addons should be gzipped JSON");
    let decoded: Value = serde_json::from_str(&json).expect("addons should decode to JSON");
    assert_eq!(decoded, addons);
}

#[test]
fn encode_addons_uses_the_reference_pipeline() {
    let encoded = encode_addons(&json!({"callbacks": {"items": ["card.action.trigger"]}}))
        .expect("addons should encode");
    assert!(!encoded.contains(['+', '/', '=']));
    let gzip = URL_SAFE_NO_PAD
        .decode(&encoded)
        .expect("addons should be base64url");
    let mut json = String::new();
    GzDecoder::new(gzip.as_slice())
        .read_to_string(&mut json)
        .expect("addons should be gzipped JSON");
    assert_eq!(json, r#"{"callbacks":{"items":["card.action.trigger"]}}"#);
}

#[tokio::test]
async fn pending_slow_down_then_success_follows_server_directed_intervals() {
    let server = StubServer::start(scripted_handler(vec![
        begin_response(),
        StubResponse::json(400, r#"{"error":"authorization_pending"}"#),
        StubResponse::json(400, r#"{"error":"slow_down"}"#),
        StubResponse::json(
            200,
            r#"{"client_id":"cli_new","client_secret":"new-secret","user_info":{"tenant_brand":"feishu","open_id":"ou_authorizer"}}"#,
        ),
    ]))
    .await;
    let mut flow = flow_for(&server, None);
    flow.begin().await.expect("begin should succeed");

    let first = flow.poll_once().await.expect("pending poll should succeed");
    assert!(matches!(first, RegistrationOutcome::Pending));
    assert_eq!(flow.interval(), Duration::from_secs(5));

    let second = flow
        .poll_once()
        .await
        .expect("slow_down poll should succeed");
    assert!(matches!(
        second,
        RegistrationOutcome::SlowDown { new_interval: 10 }
    ));
    assert_eq!(
        flow.interval(),
        Duration::from_secs(10),
        "slow_down must grow the interval by 5s"
    );

    let third = flow.poll_once().await.expect("success poll should succeed");
    let RegistrationOutcome::Credentials { creds, bot_hint } = third else {
        panic!("third poll should yield credentials");
    };
    assert_eq!(creds.app_id, "cli_new");
    assert_eq!(creds.app_secret.expose_secret(), "new-secret");
    assert_eq!(creds.tenant, TenantBrand::Feishu);
    assert_eq!(bot_hint.as_deref(), Some("ou_authorizer"));

    let poll = &server.requests()[2];
    let body = poll.body_text();
    assert!(body.contains("action=poll"));
    assert!(body.contains("device_code=dc-1"));
}

#[tokio::test]
async fn access_denied_is_terminal() {
    let server = StubServer::start(scripted_handler(vec![
        begin_response(),
        StubResponse::json(
            400,
            r#"{"error":"access_denied","error_description":"user refused"}"#,
        ),
    ]))
    .await;
    let mut flow = flow_for(&server, None);
    flow.begin().await.expect("begin should succeed");

    let error = flow.poll_once().await.expect_err("access_denied must fail");

    assert!(matches!(error, LarkError::PermanentAuth { .. }));
    assert!(
        !format!("{error}").contains("refused"),
        "server messages are discarded"
    );
}

#[tokio::test]
async fn expired_token_is_terminal() {
    let server = StubServer::start(scripted_handler(vec![
        begin_response(),
        StubResponse::json(400, r#"{"error":"expired_token"}"#),
    ]))
    .await;
    let mut flow = flow_for(&server, None);
    flow.begin().await.expect("begin should succeed");

    let error = flow.poll_once().await.expect_err("expired_token must fail");

    assert!(matches!(error, LarkError::Exhausted { .. }));
}

#[tokio::test]
async fn lark_tenant_brand_switches_the_accounts_domain_once() {
    let feishu = StubServer::start(scripted_handler(vec![
        begin_response(),
        StubResponse::json(200, r#"{"user_info":{"tenant_brand":"lark"}}"#),
    ]))
    .await;
    let lark = StubServer::start(scripted_handler(vec![
        // The Lark host still answers tenant_brand=lark; the flow must not
        // switch domains a second time.
        StubResponse::json(200, r#"{"user_info":{"tenant_brand":"lark"}}"#),
        StubResponse::json(
            200,
            r#"{"client_id":"cli_lark","client_secret":"lark-secret","user_info":{"tenant_brand":"lark","open_id":"ou_lark"}}"#,
        ),
    ]))
    .await;
    let http = LarkHttp::new(endpoints_for(&feishu)).expect("HTTP client should build");
    let mut flow = RegistrationFlow::with_parts(
        http,
        Url::parse(&lark.url()).expect("stub URL should parse"),
        None,
        Duration::from_secs(60),
    );
    flow.begin().await.expect("begin should succeed");

    let first = flow
        .poll_once()
        .await
        .expect("brand response should succeed");
    assert!(matches!(first, RegistrationOutcome::Pending));
    assert_eq!(
        lark.request_count(),
        0,
        "the brand response itself triggers the switch"
    );

    let second = flow
        .poll_once()
        .await
        .expect("switched poll should succeed");
    assert!(matches!(second, RegistrationOutcome::Pending));

    let third = flow.poll_once().await.expect("success poll should succeed");
    let RegistrationOutcome::Credentials { creds, .. } = third else {
        panic!("third poll should yield credentials");
    };
    assert_eq!(creds.tenant, TenantBrand::Lark);
    assert_eq!(creds.app_id, "cli_lark");

    assert_eq!(feishu.request_count(), 2, "begin plus one pre-switch poll");
    assert_eq!(lark.request_count(), 2, "all later polls hit the Lark host");
    assert!(lark.requests()[0].body_text().contains("action=poll"));
}

#[tokio::test]
async fn registration_deadline_stops_polling() {
    let server = StubServer::start(scripted_handler(vec![begin_response()])).await;
    let http = LarkHttp::new(endpoints_for(&server)).expect("HTTP client should build");
    let mut flow = RegistrationFlow::with_parts(
        http,
        Url::parse(&server.url()).expect("stub URL should parse"),
        None,
        Duration::ZERO,
    );
    flow.begin().await.expect("begin should succeed");

    let error = flow
        .poll_once()
        .await
        .expect_err("an expired deadline must fail");

    assert!(matches!(error, LarkError::Exhausted { .. }));
    assert_eq!(
        server.request_count(),
        1,
        "no poll request may be sent after the deadline"
    );
}

#[tokio::test]
async fn existing_app_validation_returns_the_bot_identity() {
    let server = StubServer::start(Arc::new(
        |request: &larkstub::RecordedRequest| match request.path.as_str() {
            "/open-apis/auth/v3/tenant_access_token/internal" => StubResponse::json(
                200,
                r#"{"code":0,"tenant_access_token":"token-0","expire":7200}"#,
            ),
            "/open-apis/bot/v3/info" => StubResponse::json(
                200,
                r#"{"code":0,"bot":{"app_name":"Bridge Bot","open_id":"ou_bot"}}"#,
            ),
            _ => StubResponse::json(404, r#"{"code":1}"#),
        },
    ))
    .await;
    let http = LarkHttp::new(endpoints_for(&server)).expect("HTTP client should build");
    let creds = LarkCredentials::new(
        "cli_existing".to_owned(),
        SecretString::from("existing-secret"),
        TenantBrand::Feishu,
    );

    let info = validate_credentials(http, creds)
        .await
        .expect("valid credentials should validate");

    assert_eq!(info.app_name.as_deref(), Some("Bridge Bot"));
    assert_eq!(info.open_id.as_deref(), Some("ou_bot"));
}

#[tokio::test]
async fn existing_app_validation_rejects_bad_credentials() {
    let server = StubServer::start(Arc::new(|_| {
        StubResponse::json(200, r#"{"code":99991663,"msg":"app secret mismatch"}"#)
    }))
    .await;
    let http = LarkHttp::new(endpoints_for(&server)).expect("HTTP client should build");
    let creds = LarkCredentials::new(
        "cli_bad".to_owned(),
        SecretString::from("bad-secret"),
        TenantBrand::Feishu,
    );

    let error = validate_credentials(http, creds)
        .await
        .expect_err("bad credentials must be rejected");

    assert_eq!(error.kind(), LarkErrorKind::PermanentAuth);
}
