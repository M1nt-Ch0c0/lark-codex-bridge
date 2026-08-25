//! `OpenAPI` request shape, auth retry, and bounds tests against the stub.

mod larkstub;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use lark_codex_bridge::lark::api::{ChatMode, LarkApi, ResourceKind, post_markdown_reply_body_len};
use lark_codex_bridge::lark::config::{LarkEndpoints, TenantBrand};
use lark_codex_bridge::lark::credentials::LarkCredentials;
use lark_codex_bridge::lark::error::{LarkError, LarkErrorKind};
use lark_codex_bridge::lark::http::LarkHttp;
use lark_codex_bridge::lark::token::TenantTokenProvider;
use lark_codex_bridge::limits::{
    LARK_MAX_RESOURCE_BYTES, LARK_MAX_SEND_BODY_BYTES, LARK_MAX_UPLOAD_BYTES,
};
use larkstub::{Handler, RecordedRequest, StubResponse, StubServer};
use secrecy::SecretString;
use url::Url;

const TEST_APP_ID: &str = "cli_test_app";
const TEST_APP_SECRET: &str = "test-secret-material";
const TOKEN_PATH: &str = "/open-apis/auth/v3/tenant_access_token/internal";
const MESSAGES_PATH: &str = "/open-apis/im/v1/messages";
const MESSAGE_GET_FIXTURE: &str = include_str!("fixtures/lark/message_get_response.json");
const CHAT_GET_FIXTURE: &str = include_str!("fixtures/lark/chat_get_response.json");

fn api_for(server: &StubServer) -> LarkApi {
    let base = Url::parse(&server.url()).expect("stub URL should parse");
    let endpoints = LarkEndpoints {
        open_base: base.clone(),
        accounts_base: base,
    };
    let http = LarkHttp::new(endpoints).expect("HTTP client should build");
    let creds = LarkCredentials::new(
        TEST_APP_ID.to_owned(),
        SecretString::from(TEST_APP_SECRET),
        TenantBrand::Feishu,
    );
    let tokens = TenantTokenProvider::new(http.clone(), creds);
    LarkApi::new(http, tokens)
}

/// Serves rotating tenant tokens (`token-0`, `token-1`, …) and delegates all
/// other paths to `responder`.
fn token_plus(
    responder: impl Fn(&RecordedRequest) -> StubResponse + Send + Sync + 'static,
) -> Handler {
    let token_calls = Arc::new(AtomicUsize::new(0));
    Arc::new(move |request: &RecordedRequest| {
        if request.path == TOKEN_PATH {
            let sequence = token_calls.fetch_add(1, Ordering::SeqCst);
            return StubResponse::json(
                200,
                &format!(r#"{{"code":0,"tenant_access_token":"token-{sequence}","expire":7200}}"#),
            );
        }
        responder(request)
    })
}

fn ok_message(id: &str) -> StubResponse {
    StubResponse::json(
        200,
        &format!(r#"{{"code":0,"data":{{"message_id":"{id}"}}}}"#),
    )
}

fn requests_to(server: &StubServer, prefix: &str) -> Vec<RecordedRequest> {
    server
        .requests()
        .into_iter()
        .filter(|request| request.path.starts_with(prefix))
        .collect()
}

#[tokio::test]
async fn send_text_posts_the_exact_wire_shape() {
    let server = StubServer::start(token_plus(|_| ok_message("om_sent"))).await;
    let api = api_for(&server);

    let sent = api
        .send_text("oc_chat", "hello bridge")
        .await
        .expect("send should succeed");
    assert_eq!(sent.message_id, "om_sent");

    let request = &requests_to(&server, MESSAGES_PATH)[0];
    assert_eq!(request.method, "POST");
    assert_eq!(
        request.path,
        "/open-apis/im/v1/messages?receive_id_type=chat_id"
    );
    assert_eq!(request.header("authorization"), Some("Bearer token-0"));
    let body: serde_json::Value =
        serde_json::from_str(&request.body_text()).expect("send body should be JSON");
    assert_eq!(
        body,
        serde_json::json!({
            "receive_id": "oc_chat",
            "msg_type": "text",
            "content": "{\"text\":\"hello bridge\"}",
        })
    );
}

#[tokio::test]
async fn reply_text_posts_the_exact_wire_shape() {
    let server = StubServer::start(token_plus(|_| ok_message("om_reply"))).await;
    let api = api_for(&server);

    let sent = api
        .reply_text("om_parent", "pong")
        .await
        .expect("reply should succeed");
    assert_eq!(sent.message_id, "om_reply");

    let request = &requests_to(&server, MESSAGES_PATH)[0];
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/open-apis/im/v1/messages/om_parent/reply");
    assert_eq!(request.header("authorization"), Some("Bearer token-0"));
    let body: serde_json::Value =
        serde_json::from_str(&request.body_text()).expect("reply body should be JSON");
    assert_eq!(
        body,
        serde_json::json!({
            "msg_type": "text",
            "content": "{\"text\":\"pong\"}",
        }),
        "plain replies must not carry reply_in_thread"
    );
}

#[tokio::test]
async fn reply_text_in_thread_sets_the_flag() {
    let server = StubServer::start(token_plus(|_| ok_message("om_thread_reply"))).await;
    let api = api_for(&server);

    api.reply_text_in_thread("om_parent", "thread pong")
        .await
        .expect("thread reply should succeed");

    let request = &requests_to(&server, MESSAGES_PATH)[0];
    assert_eq!(request.path, "/open-apis/im/v1/messages/om_parent/reply");
    let body: serde_json::Value =
        serde_json::from_str(&request.body_text()).expect("reply body should be JSON");
    assert_eq!(
        body,
        serde_json::json!({
            "msg_type": "text",
            "content": "{\"text\":\"thread pong\"}",
            "reply_in_thread": true,
        })
    );
}

#[tokio::test]
async fn reply_markdown_post_uses_the_exact_lark_post_shape() {
    let server = StubServer::start(token_plus(|_| ok_message("om_post"))).await;
    let api = api_for(&server);
    let markdown = "**Result**\n\n- one\n- two";

    api.reply_post_markdown("om_parent", markdown)
        .await
        .expect("Markdown post should succeed");
    api.reply_post_markdown_in_thread("om_parent", markdown)
        .await
        .expect("thread Markdown post should succeed");

    let requests = requests_to(&server, MESSAGES_PATH);
    let plain: serde_json::Value =
        serde_json::from_str(&requests[0].body_text()).expect("post body should be JSON");
    assert_eq!(
        requests[0].body.len(),
        post_markdown_reply_body_len(markdown, false),
        "the splitter's size function must match the actual HTTP body"
    );
    assert_eq!(
        plain,
        serde_json::json!({
            "msg_type": "post",
            "content": serde_json::to_string(&serde_json::json!({
                "zh_cn": {"content": [[{"tag": "md", "text": markdown}]]},
            }))
            .expect("post content"),
        })
    );
    let plain_content: serde_json::Value = serde_json::from_str(
        plain["content"]
            .as_str()
            .expect("post content should be a JSON string"),
    )
    .expect("post content should be JSON");
    assert_eq!(
        plain_content,
        serde_json::json!({
            "zh_cn": {
                "content": [[{"tag": "md", "text": markdown}]],
            },
        })
    );

    let threaded: serde_json::Value =
        serde_json::from_str(&requests[1].body_text()).expect("post body should be JSON");
    assert_eq!(
        requests[1].body.len(),
        post_markdown_reply_body_len(markdown, true)
    );
    assert_eq!(
        threaded,
        serde_json::json!({
            "msg_type": "post",
            "content": serde_json::to_string(&serde_json::json!({
                "zh_cn": {"content": [[{"tag": "md", "text": markdown}]]},
            }))
            .expect("post content"),
            "reply_in_thread": true,
        })
    );
}

#[tokio::test]
async fn topic_card2_reply_uses_the_exact_interactive_contract() {
    let server = StubServer::start(token_plus(|_| ok_message("om_topic_card"))).await;
    let api = api_for(&server);
    let card = serde_json::json!({
        "schema": "2.0",
        "body": {"elements": [{"tag": "markdown", "content": "**working**"}]},
    });
    api.reply_card_in_thread("om_parent", card.clone())
        .await
        .expect("topic card reply");

    let request = &requests_to(&server, MESSAGES_PATH)[0];
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/open-apis/im/v1/messages/om_parent/reply");
    let body: serde_json::Value = serde_json::from_slice(&request.body).expect("topic card body");
    assert_eq!(
        body,
        serde_json::json!({
            "msg_type": "interactive",
            "content": serde_json::to_string(&card).expect("card content"),
            "reply_in_thread": true,
        })
    );
}

#[tokio::test]
async fn send_card_and_reply_card_use_the_interactive_type() {
    let server = StubServer::start(token_plus(|_| ok_message("om_card"))).await;
    let api = api_for(&server);
    let card = serde_json::json!({"elements": [], "header": {"title": {"content": "scrubbed"}}});
    let card_wire = serde_json::to_string(&card).expect("card should serialize");

    api.send_card("oc_chat", card.clone())
        .await
        .expect("card send should succeed");
    api.reply_card("om_parent", card)
        .await
        .expect("card reply should succeed");

    let requests = requests_to(&server, MESSAGES_PATH);
    assert_eq!(
        requests[0].path,
        "/open-apis/im/v1/messages?receive_id_type=chat_id"
    );
    let send_body: serde_json::Value =
        serde_json::from_str(&requests[0].body_text()).expect("card body should be JSON");
    assert_eq!(send_body["msg_type"], "interactive");
    assert_eq!(send_body["content"], card_wire);
    assert_eq!(
        requests[1].path,
        "/open-apis/im/v1/messages/om_parent/reply"
    );
    let reply_body: serde_json::Value =
        serde_json::from_str(&requests[1].body_text()).expect("card body should be JSON");
    assert_eq!(reply_body["msg_type"], "interactive");
    assert_eq!(reply_body["content"], card_wire);
}

#[tokio::test]
async fn update_card_patches_the_message() {
    let server = StubServer::start(token_plus(|_| {
        StubResponse::json(200, r#"{"code":0,"data":{}}"#)
    }))
    .await;
    let api = api_for(&server);
    let card = serde_json::json!({"elements": [{"tag": "div", "text": {"content": "done"}}]});

    api.update_card("om_card", card.clone())
        .await
        .expect("card update should succeed");

    let request = &requests_to(&server, MESSAGES_PATH)[0];
    assert_eq!(request.method, "PATCH");
    assert_eq!(request.path, "/open-apis/im/v1/messages/om_card");
    assert_eq!(request.header("authorization"), Some("Bearer token-0"));
    let body: serde_json::Value =
        serde_json::from_str(&request.body_text()).expect("update body should be JSON");
    assert_eq!(
        body,
        serde_json::json!({"content": serde_json::to_string(&card).expect("card should serialize")})
    );
}

#[tokio::test]
async fn token_invalid_code_forces_one_refresh_and_one_retry() {
    let message_calls = Arc::new(AtomicUsize::new(0));
    let server = StubServer::start(token_plus({
        let message_calls = Arc::clone(&message_calls);
        move |_| {
            if message_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                StubResponse::json(
                    200,
                    r#"{"code":99991663,"msg":"invalid tenant access token"}"#,
                )
            } else {
                ok_message("om_retried")
            }
        }
    }))
    .await;
    let api = api_for(&server);

    let sent = api
        .send_text("oc_chat", "hello")
        .await
        .expect("retry with a fresh token should succeed");
    assert_eq!(sent.message_id, "om_retried");

    let message_requests = requests_to(&server, MESSAGES_PATH);
    assert_eq!(message_requests.len(), 2, "exactly one retry is allowed");
    assert_eq!(
        message_requests[0].header("authorization"),
        Some("Bearer token-0")
    );
    assert_eq!(
        message_requests[1].header("authorization"),
        Some("Bearer token-1"),
        "the retry must carry the force-refreshed token"
    );
    assert_eq!(
        requests_to(&server, TOKEN_PATH).len(),
        2,
        "the stale token must be force-refreshed exactly once"
    );
}

#[tokio::test]
async fn http_401_forces_one_refresh_and_one_retry() {
    let message_calls = Arc::new(AtomicUsize::new(0));
    let server = StubServer::start(token_plus({
        let message_calls = Arc::clone(&message_calls);
        move |_| {
            if message_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                StubResponse::json(401, r#"{"code":99991663}"#)
            } else {
                ok_message("om_retried")
            }
        }
    }))
    .await;
    let api = api_for(&server);

    let sent = api
        .send_text("oc_chat", "hello")
        .await
        .expect("retry after 401 should succeed");
    assert_eq!(sent.message_id, "om_retried");
    assert_eq!(requests_to(&server, MESSAGES_PATH).len(), 2);
    assert_eq!(requests_to(&server, TOKEN_PATH).len(), 2);
}

#[tokio::test]
async fn persistent_token_failure_propagates_permanent_auth() {
    let server = StubServer::start(token_plus(|_| {
        StubResponse::json(
            200,
            r#"{"code":99991663,"msg":"invalid tenant access token"}"#,
        )
    }))
    .await;
    let api = api_for(&server);

    let error = api
        .send_text("oc_chat", "hello")
        .await
        .expect_err("a persistent token failure must fail");
    assert!(matches!(
        error,
        LarkError::PermanentAuth {
            code: Some(99_991_663),
            ..
        }
    ));
    assert_eq!(
        requests_to(&server, MESSAGES_PATH).len(),
        2,
        "exactly one retry, never more"
    );
    let rendered = format!("{error} {error:?}");
    assert!(
        !rendered.contains("invalid tenant"),
        "server messages are discarded"
    );
}

#[tokio::test]
async fn non_token_permanent_code_is_not_retried() {
    // 99991661 is in the permanent-auth range but is not a token error, so a
    // refresh could not fix it and no retry must happen.
    let server = StubServer::start(token_plus(|_| {
        StubResponse::json(200, r#"{"code":99991661,"msg":"app_ticket invalid"}"#)
    }))
    .await;
    let api = api_for(&server);

    let error = api
        .send_text("oc_chat", "hello")
        .await
        .expect_err("a permanent failure must fail");
    assert_eq!(error.kind(), LarkErrorKind::PermanentAuth);
    assert_eq!(requests_to(&server, MESSAGES_PATH).len(), 1);
    assert_eq!(requests_to(&server, TOKEN_PATH).len(), 1);
}

#[tokio::test]
async fn known_not_in_chat_code_is_a_permanent_business_rejection() {
    let server = StubServer::start(token_plus(|_| {
        StubResponse::json(200, r#"{"code":230001,"msg":"bot not in chat"}"#)
    }))
    .await;
    let api = api_for(&server);

    let error = api
        .send_text("oc_chat", "hello")
        .await
        .expect_err("a nonzero code must fail");
    assert!(matches!(
        error,
        LarkError::ProtocolViolation {
            code: Some(230_001),
            ..
        }
    ));
    assert_eq!(requests_to(&server, MESSAGES_PATH).len(), 1);
}

#[tokio::test]
async fn documented_frequency_limit_code_is_retryable() {
    let server = StubServer::start(token_plus(|_| {
        // Lark documents this application-frequency code with HTTP 400.
        StubResponse::json(400, r#"{"code":99991400,"msg":"limited"}"#)
    }))
    .await;
    let api = api_for(&server);

    let error = api
        .send_text("oc_chat", "hello")
        .await
        .expect_err("a rate limit must fail this call");
    assert!(matches!(
        error,
        LarkError::Retryable {
            code: Some(99_991_400),
            ..
        }
    ));
}

#[tokio::test]
async fn oversize_send_body_is_refused_before_io() {
    let server = StubServer::start(token_plus(|_| ok_message("om_never"))).await;
    let api = api_for(&server);

    let error = api
        .send_text("oc_chat", &"x".repeat(LARK_MAX_SEND_BODY_BYTES))
        .await
        .expect_err("an oversize body must fail");
    assert!(matches!(error, LarkError::Exhausted { .. }));
    assert_eq!(
        server.request_count(),
        0,
        "the cap must be enforced before any request I/O"
    );
}

#[tokio::test]
async fn get_message_preserves_the_thread_id() {
    let server =
        StubServer::start(token_plus(|_| StubResponse::json(200, MESSAGE_GET_FIXTURE))).await;
    let api = api_for(&server);

    let message = api
        .get_message("om_x100b5496d4b93cc0c73c1df0dc0000a")
        .await
        .expect("message get should succeed");

    assert_eq!(message.message_id, "om_x100b5496d4b93cc0c73c1df0dc0000a");
    assert_eq!(message.chat_id, "oc_a0553eda9014c201e6969b4788953000");
    assert_eq!(message.chat_type, "group");
    assert_eq!(message.message_type, "text");
    assert_eq!(
        message.sender_id.as_deref(),
        Some("ou_155184d1e73cb1458973df8d9e3000a")
    );
    assert_eq!(message.sender_type.as_deref(), Some("user"));
    assert_eq!(
        message.root_id.as_deref(),
        Some("om_x100b5496d4b93cc0c73c1df0dc00001")
    );
    assert_eq!(
        message.parent_id.as_deref(),
        Some("om_x100b5496d4b93cc0c73c1df0dc00002")
    );
    assert_eq!(message.thread_id.as_deref(), Some("omt_1a9c1d74fd104000"));
    assert_eq!(
        message.content.as_deref(),
        Some(r#"{"text":"scrubbed content"}"#)
    );
    assert!(!message.deleted);
    let debug = format!("{message:?}");
    assert!(!debug.contains("scrubbed content"));

    let request = &requests_to(&server, MESSAGES_PATH)[0];
    assert_eq!(request.method, "GET");
    assert_eq!(
        request.path,
        "/open-apis/im/v1/messages/om_x100b5496d4b93cc0c73c1df0dc0000a"
    );
    assert_eq!(request.header("authorization"), Some("Bearer token-0"));
}

#[tokio::test]
async fn get_chat_mode_maps_the_wire_values() {
    let server = StubServer::start(token_plus(|request: &RecordedRequest| {
        if request.path.ends_with("oc_topic") {
            StubResponse::json(200, CHAT_GET_FIXTURE)
        } else {
            StubResponse::json(
                200,
                r#"{"code":0,"data":{"chat_id":"oc_p2p","chat_mode":"p2p","chat_type":"p2p"}}"#,
            )
        }
    }))
    .await;
    let api = api_for(&server);

    let topic = api
        .get_chat_mode("oc_topic")
        .await
        .expect("topic chat should succeed");
    let p2p = api
        .get_chat_mode("oc_p2p")
        .await
        .expect("p2p chat should succeed");
    assert_eq!(topic, ChatMode::Topic);
    assert_eq!(p2p, ChatMode::P2p);

    let request = &requests_to(&server, "/open-apis/im/v1/chats/")[0];
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/open-apis/im/v1/chats/oc_topic");
    assert_eq!(request.header("authorization"), Some("Bearer token-0"));
}

#[tokio::test]
async fn unknown_chat_mode_is_a_protocol_violation() {
    let server = StubServer::start(token_plus(|_| {
        StubResponse::json(200, r#"{"code":0,"data":{"chat_mode":"forum"}}"#)
    }))
    .await;
    let api = api_for(&server);

    let error = api
        .get_chat_mode("oc_weird")
        .await
        .expect_err("an unknown chat_mode must fail");
    assert!(matches!(error, LarkError::ProtocolViolation { .. }));
}

#[tokio::test]
async fn group_chat_mode_maps_to_group() {
    let server = StubServer::start(token_plus(|_| {
        StubResponse::json(200, r#"{"code":0,"data":{"chat_mode":"group"}}"#)
    }))
    .await;
    let api = api_for(&server);

    let mode = api
        .get_chat_mode("oc_group")
        .await
        .expect("group chat should succeed");
    assert_eq!(mode, ChatMode::Group);
}

#[tokio::test]
async fn download_message_resource_uses_the_exact_path() {
    let server = StubServer::start(token_plus(|_| StubResponse {
        status: 200,
        headers: vec![("content-type".to_owned(), "image/png".to_owned())],
        body: b"png-bytes".to_vec(),
        delay: Duration::ZERO,
        close_delimited: false,
    }))
    .await;
    let api = api_for(&server);

    let image = api
        .download_message_resource("om_msg", "img_v3_key", ResourceKind::Image)
        .await
        .expect("image download should succeed");
    assert_eq!(image.bytes.as_ref(), b"png-bytes");

    let file = api
        .download_message_resource("om_msg", "file_v3_key", ResourceKind::File)
        .await
        .expect("file download should succeed");
    assert_eq!(file.bytes.as_ref(), b"png-bytes");

    let requests = requests_to(&server, MESSAGES_PATH);
    assert_eq!(requests[0].method, "GET");
    assert_eq!(
        requests[0].path,
        "/open-apis/im/v1/messages/om_msg/resources/img_v3_key?type=image"
    );
    assert_eq!(
        requests[1].path,
        "/open-apis/im/v1/messages/om_msg/resources/file_v3_key?type=file"
    );
    assert_eq!(requests[0].header("authorization"), Some("Bearer token-0"));
}

#[tokio::test]
async fn oversize_download_content_length_is_rejected_up_front() {
    let oversize = (LARK_MAX_RESOURCE_BYTES + 1).to_string();
    let server = StubServer::start(token_plus(move |_| StubResponse {
        status: 200,
        headers: vec![
            (
                "content-type".to_owned(),
                "application/octet-stream".to_owned(),
            ),
            ("content-length".to_owned(), oversize.clone()),
        ],
        body: b"tiny".to_vec(),
        delay: Duration::ZERO,
        close_delimited: false,
    }))
    .await;
    let api = api_for(&server);

    let error = api
        .download_message_resource("om_msg", "file_key", ResourceKind::File)
        .await
        .expect_err("an oversize content length must fail");
    assert!(matches!(error, LarkError::Exhausted { .. }));
}

#[tokio::test]
async fn oversize_download_stream_is_aborted_mid_body() {
    let server = StubServer::start(token_plus(|_| {
        StubResponse::text(200, &"x".repeat(LARK_MAX_RESOURCE_BYTES + 1)).close_delimited()
    }))
    .await;
    let api = api_for(&server);

    let error = api
        .download_message_resource("om_msg", "file_key", ResourceKind::File)
        .await
        .expect_err("an oversize stream must fail");
    assert!(matches!(
        error,
        LarkError::Exhausted { limit, .. } if limit == LARK_MAX_RESOURCE_BYTES as u64
    ));
}

#[tokio::test]
async fn download_classifies_a_json_error_envelope_instead_of_returning_it() {
    let server = StubServer::start(token_plus(|_| {
        StubResponse::json(200, r#"{"code":99991663,"msg":"scrubbed"}"#)
    }))
    .await;
    let api = api_for(&server);

    let error = api
        .download_message_resource("om_msg", "file_key", ResourceKind::File)
        .await
        .expect_err("a JSON error envelope must not be returned as resource bytes");
    assert!(
        matches!(error, LarkError::PermanentAuth { .. }),
        "token-invalid envelope should surface as PermanentAuth after one forced refresh, got {error}"
    );
}

#[tokio::test]
async fn upload_image_posts_the_exact_multipart_fields() {
    let server = StubServer::start(token_plus(|_| {
        StubResponse::json(200, r#"{"code":0,"data":{"image_key":"img_v3_uploaded"}}"#)
    }))
    .await;
    let api = api_for(&server);

    let key = api
        .upload_image(Bytes::from_static(b"fake-png"))
        .await
        .expect("image upload should succeed");
    assert_eq!(key, "img_v3_uploaded");

    let request = &requests_to(&server, "/open-apis/im/v1/images")[0];
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/open-apis/im/v1/images");
    assert_eq!(request.header("authorization"), Some("Bearer token-0"));
    let content_type = request
        .header("content-type")
        .expect("multipart content type");
    assert!(content_type.starts_with("multipart/form-data; boundary="));
    let body = request.body_text();
    assert!(body.contains("name=\"image_type\""), "body: {body}");
    assert!(body.contains("\r\n\r\nmessage\r\n"), "body: {body}");
    assert!(
        body.contains("name=\"image\"; filename=\"image\""),
        "body: {body}"
    );
    assert!(body.contains("fake-png"), "body: {body}");
}

#[tokio::test]
async fn upload_file_posts_the_exact_multipart_fields() {
    let server = StubServer::start(token_plus(|_| {
        StubResponse::json(200, r#"{"code":0,"data":{"file_key":"file_v3_uploaded"}}"#)
    }))
    .await;
    let api = api_for(&server);

    let key = api
        .upload_file("report.bin", Bytes::from_static(b"file-bytes"))
        .await
        .expect("file upload should succeed");
    assert_eq!(key, "file_v3_uploaded");

    let request = &requests_to(&server, "/open-apis/im/v1/files")[0];
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/open-apis/im/v1/files");
    assert_eq!(request.header("authorization"), Some("Bearer token-0"));
    let body = request.body_text();
    assert!(body.contains("name=\"file_type\""), "body: {body}");
    assert!(body.contains("\r\n\r\nstream\r\n"), "body: {body}");
    assert!(body.contains("name=\"file_name\""), "body: {body}");
    assert!(body.contains("\r\n\r\nreport.bin\r\n"), "body: {body}");
    assert!(
        body.contains("name=\"file\"; filename=\"report.bin\""),
        "body: {body}"
    );
    assert!(body.contains("file-bytes"), "body: {body}");
}

#[tokio::test]
async fn oversize_upload_is_refused_before_io() {
    let server = StubServer::start(token_plus(|_| ok_message("om_never"))).await;
    let api = api_for(&server);
    let oversize = Bytes::from(vec![0_u8; LARK_MAX_UPLOAD_BYTES + 1]);

    let image_error = api
        .upload_image(oversize.clone())
        .await
        .expect_err("an oversize image must fail");
    let file_error = api
        .upload_file("big.bin", oversize)
        .await
        .expect_err("an oversize file must fail");
    assert!(matches!(image_error, LarkError::Exhausted { .. }));
    assert!(matches!(file_error, LarkError::Exhausted { .. }));
    assert_eq!(
        server.request_count(),
        0,
        "oversize uploads must be refused before any request I/O"
    );
}

#[tokio::test]
async fn bot_info_returns_a_sanitized_identity() {
    let server = StubServer::start(token_plus(|_| {
        StubResponse::json(
            200,
            r#"{"code":0,"bot":{"app_name":"Bridge Bot","open_id":"ou_bot"}}"#,
        )
    }))
    .await;
    let api = api_for(&server);

    let info = api.bot_info().await.expect("bot info should succeed");

    assert_eq!(info.app_name.as_deref(), Some("Bridge Bot"));
    assert_eq!(info.open_id.as_deref(), Some("ou_bot"));
    let request = &requests_to(&server, "/open-apis/bot/v3/info")[0];
    assert_eq!(request.method, "GET");
    assert_eq!(request.header("authorization"), Some("Bearer token-0"));
}

#[tokio::test]
async fn debug_output_never_contains_secret_material() {
    let server = StubServer::start(token_plus(|_| {
        StubResponse::json(200, r#"{"code":0,"data":{"message_id":"om_x"}}"#)
    }))
    .await;
    let api = api_for(&server);
    api.send_text("oc_chat", "secret message text")
        .await
        .expect("send should succeed");

    let api_debug = format!("{api:?}");
    assert!(!api_debug.contains(TEST_APP_SECRET));
    assert!(!api_debug.contains("token-0"));

    let data = lark_codex_bridge::lark::api::ResourceData {
        bytes: Bytes::from_static(b"resource-bytes"),
    };
    let rendered = format!("{data:?}");
    assert!(!rendered.contains("resource-bytes"));
    assert!(rendered.contains("len"));
}

#[tokio::test]
async fn app_creator_id_returns_the_owner_not_the_bot() {
    let server = StubServer::start(token_plus(|request: &RecordedRequest| {
        assert!(
            request
                .path
                .starts_with("/open-apis/application/v6/applications/cli_test_app")
        );
        assert!(request.path.contains("user_id_type=open_id"));
        StubResponse::json(
            200,
            r#"{"code":0,"data":{"app":{"creator_id":"ou_creator"}}}"#,
        )
    }))
    .await;
    let api = api_for(&server);

    let creator = api
        .app_creator_id(TEST_APP_ID)
        .await
        .expect("creator lookup should succeed");

    assert_eq!(creator.as_deref(), Some("ou_creator"));
    assert_ne!(creator.as_deref(), Some("ou_bot"));
    let request = &requests_to(&server, "/open-apis/application/v6/applications")[0];
    assert_eq!(request.method, "GET");
    assert_eq!(request.header("authorization"), Some("Bearer token-0"));
}

#[tokio::test]
async fn app_creator_id_returns_none_when_creator_is_absent() {
    let server = StubServer::start(token_plus(|_| {
        StubResponse::json(200, r#"{"code":0,"data":{"app":{}}}"#)
    }))
    .await;
    let api = api_for(&server);

    let creator = api
        .app_creator_id(TEST_APP_ID)
        .await
        .expect("creator lookup should succeed");

    assert_eq!(creator, None);
}
