mod larkstub;

use std::sync::Arc;

use lark_codex_bridge::lark::api::LarkApi;
use lark_codex_bridge::lark::config::{LarkEndpoints, TenantBrand};
use lark_codex_bridge::lark::credentials::LarkCredentials;
use lark_codex_bridge::lark::http::LarkHttp;
use lark_codex_bridge::lark::token::TenantTokenProvider;
use lark_codex_bridge::limits::QUOTE_CONTENT_MAX_BYTES;
use lark_codex_bridge::runtime::context::{DraftPart, MediaKind, QuoteStatus};
use lark_codex_bridge::runtime::quote::{LarkQuoteResolver, QuoteRequest, QuoteResolver};
use larkstub::{Handler, RecordedRequest, StubResponse, StubServer};
use secrecy::SecretString;
use serde_json::json;
use url::Url;

const TOKEN_PATH: &str = "/open-apis/auth/v3/tenant_access_token/internal";

fn api_for(server: &StubServer) -> LarkApi {
    let base = Url::parse(&server.url()).expect("stub URL");
    let endpoints = LarkEndpoints {
        open_base: base.clone(),
        accounts_base: base,
    };
    let http = LarkHttp::new(endpoints).expect("HTTP client");
    let credentials = LarkCredentials::new(
        "cli_quote_test".to_owned(),
        SecretString::from("quote-test-secret"),
        TenantBrand::Feishu,
    );
    let tokens = TenantTokenProvider::new(http.clone(), credentials);
    LarkApi::new(http, tokens)
}

fn with_token(
    response: impl Fn(&RecordedRequest) -> StubResponse + Send + Sync + 'static,
) -> Handler {
    Arc::new(move |request| {
        if request.path == TOKEN_PATH {
            StubResponse::json(
                200,
                r#"{"code":0,"tenant_access_token":"token","expire":7200}"#,
            )
        } else {
            response(request)
        }
    })
}

fn message_response(
    message_id: &str,
    chat_id: &str,
    message_type: &str,
    content: Option<&str>,
    deleted: bool,
) -> StubResponse {
    let body = content.map(|content| json!({"content": content}));
    StubResponse::json(
        200,
        &json!({
            "code": 0,
            "data": {"items": [{
                "message_id": message_id,
                "chat_id": chat_id,
                "chat_type": "group",
                "msg_type": message_type,
                "deleted": deleted,
                "body": body,
            }]}
        })
        .to_string(),
    )
}

fn request() -> QuoteRequest {
    QuoteRequest {
        parent_message_id: "om_parent".to_owned(),
        chat_id: "oc_allowed".to_owned(),
    }
}

#[tokio::test]
async fn one_hop_image_quote_normalizes_to_a_redacted_media_draft() {
    let server = StubServer::start(with_token(|_| {
        message_response(
            "om_parent",
            "oc_allowed",
            "image",
            Some(r#"{"image_key":"img_quote_secret"}"#),
            false,
        )
    }))
    .await;
    let resolver = LarkQuoteResolver::new(api_for(&server));

    let quote = resolver.resolve(request()).await;

    assert_eq!(quote.status, QuoteStatus::Available);
    assert_eq!(quote.message_type.as_deref(), Some("image"));
    let DraftPart::Media { kind, resource, .. } = &quote.parts[0] else {
        panic!("quoted image draft")
    };
    assert_eq!(*kind, MediaKind::Image);
    assert_eq!(resource.key, "img_quote_secret");
    assert!(!format!("{quote:?}").contains("img_quote_secret"));
    let message_gets = server
        .requests()
        .into_iter()
        .filter(|request| request.path.contains("/im/v1/messages/om_parent"))
        .count();
    assert_eq!(message_gets, 1, "the resolver performs exactly one lookup");
}

#[tokio::test]
async fn one_hop_audio_quote_preserves_a_lazy_file_descriptor() {
    let server = StubServer::start(with_token(|_| {
        message_response(
            "om_parent",
            "oc_allowed",
            "audio",
            Some(r#"{"file_key":"audio_quote_secret","duration":800}"#),
            false,
        )
    }))
    .await;

    let quote = LarkQuoteResolver::new(api_for(&server))
        .resolve(request())
        .await;

    assert_eq!(quote.status, QuoteStatus::Available);
    let DraftPart::Media {
        kind,
        resource,
        metadata,
        ..
    } = &quote.parts[0]
    else {
        panic!("quoted audio draft")
    };
    assert_eq!(*kind, MediaKind::Audio);
    assert_eq!(resource.key, "audio_quote_secret");
    assert_eq!(metadata.duration_ms, Some(800));
    assert!(!format!("{quote:?}").contains("audio_quote_secret"));
    assert_eq!(
        server
            .requests()
            .into_iter()
            .filter(|request| request.path.contains("/im/v1/messages/om_parent"))
            .count(),
        1
    );
}

#[tokio::test]
async fn deleted_mismatched_oversize_and_unsupported_quotes_have_stable_states() {
    let cases = [
        (
            "deleted",
            message_response(
                "om_parent",
                "oc_allowed",
                "image",
                Some(r#"{"image_key":"never_retained"}"#),
                true,
            ),
            QuoteStatus::Deleted,
        ),
        (
            "wrong_chat",
            message_response(
                "om_parent",
                "oc_other",
                "image",
                Some(r#"{"image_key":"never_retained"}"#),
                false,
            ),
            QuoteStatus::Unauthorized,
        ),
        (
            "unsupported",
            message_response(
                "om_parent",
                "oc_allowed",
                "location",
                Some(r#"{"name":"secret place"}"#),
                false,
            ),
            QuoteStatus::Unsupported,
        ),
    ];
    for (name, response, expected) in cases {
        let response = response.clone();
        let server = StubServer::start(with_token(move |_| response.clone())).await;
        let quote = LarkQuoteResolver::new(api_for(&server))
            .resolve(request())
            .await;
        assert_eq!(quote.status, expected, "case {name}");
    }

    let content = format!(r#"{{"text":"{}"}}"#, "x".repeat(QUOTE_CONTENT_MAX_BYTES));
    let server = StubServer::start(with_token(move |_| {
        message_response("om_parent", "oc_allowed", "text", Some(&content), false)
    }))
    .await;
    let quote = LarkQuoteResolver::new(api_for(&server))
        .resolve(request())
        .await;
    assert_eq!(quote.status, QuoteStatus::Oversize);
    assert!(quote.parts.is_empty());
}

#[tokio::test]
async fn missing_body_and_http_authorization_failures_do_not_expose_content() {
    let missing = StubServer::start(with_token(|_| {
        message_response("om_parent", "oc_allowed", "image", None, false)
    }))
    .await;
    let quote = LarkQuoteResolver::new(api_for(&missing))
        .resolve(request())
        .await;
    assert_eq!(quote.status, QuoteStatus::Unavailable);

    let denied = StubServer::start(with_token(|_| StubResponse::text(403, "private body"))).await;
    let quote = LarkQuoteResolver::new(api_for(&denied))
        .resolve(request())
        .await;
    assert_eq!(quote.status, QuoteStatus::Unauthorized);
    assert!(!format!("{quote:?}").contains("private body"));
}
