mod larkstub;

use std::collections::HashSet;
use std::sync::Arc;

use lark_codex_bridge::config::{BridgeConfig, WorkspacePolicy};
use lark_codex_bridge::lark::api::{ChatMode, LarkApi};
use lark_codex_bridge::lark::config::{LarkEndpoints, TenantBrand};
use lark_codex_bridge::lark::credentials::LarkCredentials;
use lark_codex_bridge::lark::http::LarkHttp;
use lark_codex_bridge::lark::token::TenantTokenProvider;
use lark_codex_bridge::limits::QUOTE_CONTENT_MAX_BYTES;
use lark_codex_bridge::runtime::context::{DraftPart, MediaKind, QuoteStatus};
use lark_codex_bridge::runtime::policy::AccessPolicy;
use lark_codex_bridge::runtime::quote::{
    LarkQuoteResolver, QuoteRequest, QuoteResolver, TopicContextRequest,
};
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
    message_response_from(
        message_id,
        chat_id,
        message_type,
        content,
        deleted,
        Some("ou_parent"),
        Some("user"),
    )
}

#[allow(clippy::too_many_arguments)]
fn message_response_from(
    message_id: &str,
    chat_id: &str,
    message_type: &str,
    content: Option<&str>,
    deleted: bool,
    sender_id: Option<&str>,
    sender_type: Option<&str>,
) -> StubResponse {
    message_response_from_with_chat_type(
        message_id,
        chat_id,
        Some("group"),
        message_type,
        content,
        deleted,
        sender_id,
        sender_type,
    )
}

#[allow(clippy::too_many_arguments)]
fn message_response_from_with_chat_type(
    message_id: &str,
    chat_id: &str,
    chat_type: Option<&str>,
    message_type: &str,
    content: Option<&str>,
    deleted: bool,
    sender_id: Option<&str>,
    sender_type: Option<&str>,
) -> StubResponse {
    let body = content.map(|content| json!({"content": content}));
    let mut item = json!({
        "message_id": message_id,
        "chat_id": chat_id,
        "sender": {"id": sender_id, "sender_type": sender_type},
        "msg_type": message_type,
        "deleted": deleted,
        "body": body,
    });
    if let Some(chat_type) = chat_type {
        item["chat_type"] = json!(chat_type);
    }
    StubResponse::json(
        200,
        &json!({
            "code": 0,
            "data": {"items": [item]}
        })
        .to_string(),
    )
}

fn policy() -> AccessPolicy {
    let workspace = std::env::current_dir().expect("current workspace");
    let config = BridgeConfig {
        owners: vec!["ou_parent".to_owned()],
        default_workspace: Some(workspace.clone()),
        workspace: WorkspacePolicy {
            allow_roots: vec![workspace],
            ..WorkspacePolicy::default()
        },
        ..BridgeConfig::default()
    };
    AccessPolicy::from_config(&config).expect("quote policy")
}

fn resolver(server: &StubServer) -> LarkQuoteResolver {
    LarkQuoteResolver::new(api_for(server), policy())
}

fn request() -> QuoteRequest {
    QuoteRequest {
        parent_message_id: "om_parent".to_owned(),
        chat_id: "oc_allowed".to_owned(),
        chat_mode: ChatMode::Group,
    }
}

fn group_allowlist_policy() -> AccessPolicy {
    let workspace = std::env::current_dir().expect("current workspace");
    let config = BridgeConfig {
        owners: vec!["ou_owner".to_owned()],
        allowed_groups: vec!["oc_allowed".to_owned()],
        default_workspace: Some(workspace.clone()),
        workspace: WorkspacePolicy {
            allow_roots: vec![workspace],
            ..WorkspacePolicy::default()
        },
        ..BridgeConfig::default()
    };
    AccessPolicy::from_config(&config).expect("group allowlist policy")
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
    let resolver = resolver(&server);

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
            Some(
                r#"{"file_key":"audio_quote_secret","duration":800,"text":"FETCHED RECOGNITION MUST NOT BYPASS ASR"}"#,
            ),
            false,
        )
    }))
    .await;

    let quote = resolver(&server).resolve(request()).await;

    assert_eq!(quote.status, QuoteStatus::Available);
    let DraftPart::Media {
        kind,
        resource,
        metadata,
        transcript_failure,
        ..
    } = &quote.parts[0]
    else {
        panic!("quoted audio draft")
    };
    assert_eq!(*kind, MediaKind::Audio);
    assert_eq!(resource.key, "audio_quote_secret");
    assert_eq!(metadata.duration_ms, Some(800));
    assert_eq!(
        *transcript_failure,
        Some(lark_codex_bridge::lark::normalize::TranscriptFailure::NotRetained)
    );
    assert!(!format!("{quote:?}").contains("audio_quote_secret"));
    assert!(!format!("{quote:?}").contains("FETCHED RECOGNITION"));
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
                "future_kind",
                Some(r#"{"secret":"opaque"}"#),
                false,
            ),
            QuoteStatus::Unsupported,
        ),
    ];
    for (name, response, expected) in cases {
        let response = response.clone();
        let server = StubServer::start(with_token(move |_| response.clone())).await;
        let quote = resolver(&server).resolve(request()).await;
        assert_eq!(quote.status, expected, "case {name}");
    }

    let content = format!(r#"{{"text":"{}"}}"#, "x".repeat(QUOTE_CONTENT_MAX_BYTES));
    let server = StubServer::start(with_token(move |_| {
        message_response("om_parent", "oc_allowed", "text", Some(&content), false)
    }))
    .await;
    let quote = resolver(&server).resolve(request()).await;
    assert_eq!(quote.status, QuoteStatus::Oversize);
    assert!(quote.parts.is_empty());
}

#[tokio::test]
async fn missing_body_and_http_authorization_failures_do_not_expose_content() {
    let missing = StubServer::start(with_token(|_| {
        message_response("om_parent", "oc_allowed", "image", None, false)
    }))
    .await;
    let quote = resolver(&missing).resolve(request()).await;
    assert_eq!(quote.status, QuoteStatus::Unavailable);

    let denied = StubServer::start(with_token(|_| StubResponse::text(403, "private body"))).await;
    let quote = resolver(&denied).resolve(request()).await;
    assert_eq!(quote.status, QuoteStatus::Unauthorized);
    assert!(!format!("{quote:?}").contains("private body"));
}

#[tokio::test]
async fn same_chat_colleague_or_bot_quote_is_available() {
    for sender_type in ["user", "app"] {
        let sender = sender_type.to_owned();
        let server = StubServer::start(with_token(move |_| {
            message_response_from(
                "om_parent",
                "oc_allowed",
                "image",
                Some(r#"{"image_key":"quoted_parent_secret"}"#),
                false,
                Some("ou_stranger"),
                Some(sender.as_str()),
            )
        }))
        .await;

        let quote = resolver(&server).resolve(request()).await;

        assert_eq!(quote.status, QuoteStatus::Available, "{sender_type}");
        assert_eq!(quote.sender_id.as_deref(), Some("ou_stranger"));
        assert!(!format!("{quote:?}").contains("quoted_parent_secret"));
    }
}

#[tokio::test]
async fn documented_lark_message_envelopes_degrade_conservatively() {
    for (code, expected) in [
        (230_002, QuoteStatus::Unauthorized),
        (230_006, QuoteStatus::Unauthorized),
        (230_027, QuoteStatus::Unauthorized),
        (230_050, QuoteStatus::Unauthorized),
        (230_011, QuoteStatus::Deleted),
        (230_001, QuoteStatus::Unavailable),
    ] {
        let server = StubServer::start(with_token(move |_| {
            StubResponse::json(
                200,
                &json!({"code": code, "msg": "must not escape"}).to_string(),
            )
        }))
        .await;
        let quote = resolver(&server).resolve(request()).await;
        assert_eq!(quote.status, expected, "Lark envelope code {code}");
        assert!(quote.parts.is_empty());
        assert!(!format!("{quote:?}").contains("must not escape"));
    }
}

#[tokio::test]
async fn missing_wire_chat_type_inherits_the_trigger_chat_mode() {
    let server = StubServer::start(with_token(|_| {
        message_response_from_with_chat_type(
            "om_parent",
            "oc_allowed",
            None,
            "text",
            Some(r#"{"text":"quoted human speech"}"#),
            false,
            Some("ou_coworker"),
            Some("user"),
        )
    }))
    .await;
    let resolver = LarkQuoteResolver::new(api_for(&server), group_allowlist_policy());

    let quote = resolver.resolve(request()).await;

    assert_eq!(quote.status, QuoteStatus::Available);
    assert_eq!(quote.sender_id.as_deref(), Some("ou_coworker"));
    assert_eq!(quote.parts.len(), 1);
}

#[tokio::test]
async fn mismatched_wire_chat_family_stays_unauthorized() {
    let server = StubServer::start(with_token(|_| {
        message_response_from_with_chat_type(
            "om_parent",
            "oc_allowed",
            Some("p2p"),
            "text",
            Some(r#"{"text":"should not leak"}"#),
            false,
            Some("ou_coworker"),
            Some("user"),
        )
    }))
    .await;
    let resolver = LarkQuoteResolver::new(api_for(&server), group_allowlist_policy());

    let quote = resolver.resolve(request()).await;

    assert_eq!(quote.status, QuoteStatus::Unauthorized);
    assert!(quote.parts.is_empty());
    assert!(!format!("{quote:?}").contains("should not leak"));
}

#[tokio::test]
async fn post_location_and_missing_sender_quotes_expose_readable_text() {
    let post = StubServer::start(with_token(|_| {
        message_response_from_with_chat_type(
            "om_parent",
            "oc_allowed",
            None,
            "post",
            Some(
                r#"{"title":"办公报告","content":[[{"tag":"text","text":"前 20% 用户消耗近九成算力"},{"tag":"text","text":"，头部效应明显","style":["bold"]}]]}"#,
            ),
            false,
            Some("ou_coworker"),
            Some("user"),
        )
    }))
    .await;
    let quote = resolver(&post).resolve(request()).await;
    assert_eq!(quote.status, QuoteStatus::Available);
    assert_eq!(quote.message_type.as_deref(), Some("post"));
    let DraftPart::Text(text) = &quote.parts[0] else {
        panic!("quoted post should flatten to text");
    };
    assert!(text.contains("办公报告"));
    assert!(text.contains("前 20% 用户消耗近九成算力"));
    assert!(text.contains("头部效应明显"));

    let location = StubServer::start(with_token(|_| {
        message_response(
            "om_parent",
            "oc_allowed",
            "location",
            Some(r#"{"name":"办公室","address":"南京路 1 号"}"#),
            false,
        )
    }))
    .await;
    let quote = resolver(&location).resolve(request()).await;
    assert_eq!(quote.status, QuoteStatus::Available);
    let DraftPart::Text(text) = &quote.parts[0] else {
        panic!("quoted location should flatten to text");
    };
    assert!(text.contains("办公室"));
    assert!(text.contains("南京路 1 号"));

    let missing_sender = StubServer::start(with_token(|_| {
        message_response_from(
            "om_parent",
            "oc_allowed",
            "text",
            Some(r#"{"text":"系统通知：会议改到三点"}"#),
            false,
            None,
            None,
        )
    }))
    .await;
    let quote = resolver(&missing_sender).resolve(request()).await;
    assert_eq!(quote.status, QuoteStatus::Available);
    assert_eq!(quote.sender_id, None);
    let DraftPart::Text(text) = &quote.parts[0] else {
        panic!("quoted text without sender should still be available");
    };
    assert!(text.contains("会议改到三点"));
}

#[tokio::test]
async fn common_quoted_wire_types_flatten_to_readable_text() {
    let cases = [
        (
            "locale_post",
            "post",
            r#"{"zh_cn":{"title":"办公报告","content":[[{"tag":"text","text":"前 20% 用户消耗近九成算力"}]]}}"#,
            "前 20% 用户消耗近九成算力",
        ),
        (
            "share_chat",
            "share_chat",
            r#"{"chat_id":"oc_shared"}"#,
            "shared chat",
        ),
        ("share_user", "share_user", r#"{}"#, "shared user"),
        (
            "system",
            "system",
            r#"{"template":"{from_user} recalled a message","from_user":["Alice"]}"#,
            "Alice",
        ),
        (
            "vote",
            "vote",
            r#"{"topic":"选餐厅","options":["A","B"]}"#,
            "选餐厅",
        ),
        ("hongbao", "hongbao", r#"{"text":"恭喜发财"}"#, "恭喜发财"),
        ("video_chat", "video_chat", r#"{"topic":"周会"}"#, "周会"),
        (
            "interactive",
            "interactive",
            r#"{"header":{"title":{"tag":"plain_text","content":"审批"}}}"#,
            "审批",
        ),
        ("calendar", "calendar", r#"{"summary":"评审"}"#, "评审"),
        (
            "todo",
            "todo",
            r#"{"summary":{"title":"写周报"}}"#,
            "写周报",
        ),
        ("folder", "folder", r#"{}"#, "shared folder"),
        ("empty_post", "post", r#"{"zh_cn":{}}"#, "rich text message"),
    ];
    for (name, message_type, content, expected) in cases {
        let server = StubServer::start(with_token({
            let content = content.to_owned();
            let message_type = message_type.to_owned();
            move |_| {
                message_response_from_with_chat_type(
                    "om_parent",
                    "oc_allowed",
                    None,
                    &message_type,
                    Some(&content),
                    false,
                    Some("ou_coworker"),
                    Some("app"),
                )
            }
        }))
        .await;
        let quote = resolver(&server).resolve(request()).await;
        assert_eq!(quote.status, QuoteStatus::Available, "{name}");
        let DraftPart::Text(text) = &quote.parts[0] else {
            panic!("{name} should flatten to text");
        };
        assert!(
            text.contains(expected),
            "{name} missing {expected:?} in {text:?}"
        );
    }
}

#[tokio::test]
async fn merge_forward_expands_child_text() {
    let server = StubServer::start(with_token(|request| {
        if request.path.contains("/im/v1/messages/om_child") {
            return message_response(
                "om_child",
                "oc_allowed",
                "text",
                Some(r#"{"text":"被转发的原文"}"#),
                false,
            );
        }
        message_response(
            "om_parent",
            "oc_allowed",
            "merge_forward",
            Some(r#"{"message_id":"om_child"}"#),
            false,
        )
    }))
    .await;
    let quote = resolver(&server).resolve(request()).await;
    assert_eq!(quote.status, QuoteStatus::Available);
    let DraftPart::Text(text) = &quote.parts[0] else {
        panic!("forward should expand to text");
    };
    assert!(text.contains("被转发的原文"), "{text}");
    assert!(text.contains("forwarded_messages"), "{text}");
}

fn thread_list_item(
    message_id: &str,
    chat_id: &str,
    text: &str,
    sender_id: &str,
) -> serde_json::Value {
    json!({
        "message_id": message_id,
        "chat_id": chat_id,
        "chat_type": "topic",
        "sender": {"id": sender_id, "sender_type": "user"},
        "msg_type": "text",
        "deleted": false,
        "body": {"content": json!({"text": text}).to_string()},
    })
}

#[tokio::test]
async fn topic_context_keeps_newest_messages_in_chronological_order() {
    let server = StubServer::start(with_token(|request| {
        assert!(
            request.path.contains("sort_type=ByCreateTimeDesc"),
            "topic list must request newest first: {}",
            request.path
        );
        assert!(request.path.contains("container_id_type=thread"));
        StubResponse::json(
            200,
            &json!({
                "code": 0,
                "data": {
                    "items": [
                        thread_list_item("om_new", "oc_topic", "最新一句", "ou_c"),
                        thread_list_item("om_mid", "oc_topic", "中间一句", "ou_b"),
                        thread_list_item("om_old", "oc_topic", "最早一句", "ou_a"),
                        thread_list_item("om_current", "oc_topic", "当前触发", "ou_me"),
                        thread_list_item("om_quoted", "oc_topic", "已被引用", "ou_q")
                    ],
                    "has_more": false
                }
            })
            .to_string(),
        )
    }))
    .await;
    let drafts = resolver(&server)
        .topic_context(TopicContextRequest {
            thread_id: "omt_topic".to_owned(),
            chat_id: "oc_topic".to_owned(),
            exclude_ids: HashSet::from(["om_current".to_owned(), "om_quoted".to_owned()]),
            max_messages: 2,
        })
        .await;
    assert_eq!(drafts.len(), 2);
    assert_eq!(drafts[0].message_id, "om_mid");
    assert_eq!(drafts[1].message_id, "om_new");
    let bodies = drafts
        .iter()
        .map(|draft| match &draft.parts[0] {
            DraftPart::Text(text) => text.as_str(),
            _ => "",
        })
        .collect::<Vec<_>>();
    assert_eq!(bodies, ["中间一句", "最新一句"]);
}

#[tokio::test]
async fn nested_forward_expands_one_more_level() {
    let server = StubServer::start(with_token(|request| {
        if request.path.contains("/im/v1/messages/om_inner") {
            return message_response(
                "om_inner",
                "oc_allowed",
                "text",
                Some(r#"{"text":"内层原文"}"#),
                false,
            );
        }
        if request.path.contains("/im/v1/messages/om_child") {
            return StubResponse::json(
                200,
                &json!({
                    "code": 0,
                    "data": {
                        "items": [{
                            "message_id": "om_child",
                            "chat_id": "oc_allowed",
                            "chat_type": "group",
                            "sender": {"id": "ou_mid", "sender_type": "user"},
                            "msg_type": "merge_forward",
                            "deleted": false,
                            "body": {"content": r#"{"message_id":"om_inner"}"#}
                        }]
                    }
                })
                .to_string(),
            );
        }
        message_response(
            "om_parent",
            "oc_allowed",
            "merge_forward",
            Some(r#"{"message_id":"om_child"}"#),
            false,
        )
    }))
    .await;
    let quote = resolver(&server).resolve(request()).await;
    assert_eq!(quote.status, QuoteStatus::Available);
    let DraftPart::Text(text) = &quote.parts[0] else {
        panic!("nested forward should expand to text");
    };
    assert!(text.contains("内层原文"), "{text}");
}
