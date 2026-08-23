//! Inbound event normalization tests: fixtures for p2p, group @, topic, and
//! quote shapes, plus backfill degradation, cache bounds, and redaction —
//! all against the shared stub HTTP server driving a real `LarkApi`.

mod larkstub;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use lark_codex_bridge::lark::api::ResourceKind;
use lark_codex_bridge::lark::config::{LarkEndpoints, TenantBrand};
use lark_codex_bridge::lark::credentials::LarkCredentials;
use lark_codex_bridge::lark::error::{LarkError, LarkErrorKind};
use lark_codex_bridge::lark::http::LarkHttp;
use lark_codex_bridge::lark::normalize::{
    Degradation, InboundEvent, MessagePart, NormalizeOutcome, Normalizer, PartStatus, ScopeKey,
    TranscriptFailure,
};
use lark_codex_bridge::lark::token::TenantTokenProvider;
use lark_codex_bridge::limits::{
    ASR_TRANSCRIPT_MAX_BYTES, LARK_CHAT_MODE_CACHE_CAPACITY, LARK_CHAT_MODE_CACHE_TTL,
    LARK_MAX_EVENT_PAYLOAD_BYTES,
};
use larkstub::{Handler, RecordedRequest, StubResponse, StubServer};
use secrecy::SecretString;
use url::Url;

const TEST_APP_ID: &str = "cli_test_app";
const TEST_APP_SECRET: &str = "test-secret-material";
const BOT_OPEN_ID: &str = "ou_bot";
const TOKEN_PATH: &str = "/open-apis/auth/v3/tenant_access_token/internal";
const CHATS_PREFIX: &str = "/open-apis/im/v1/chats/";
const MESSAGES_PREFIX: &str = "/open-apis/im/v1/messages/";

const P2P_TEXT_FIXTURE: &str = include_str!("fixtures/lark/event_p2p_text.json");
const GROUP_MENTION_FIXTURE: &str = include_str!("fixtures/lark/event_group_mention.json");
const TOPIC_REPLY_FIXTURE: &str = include_str!("fixtures/lark/event_topic_reply.json");
const QUOTE_FIXTURE: &str = include_str!("fixtures/lark/event_quote.json");

fn normalizer_for(server: &StubServer) -> Normalizer {
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
    Normalizer::new(
        lark_codex_bridge::lark::api::LarkApi::new(http, tokens),
        BOT_OPEN_ID,
    )
}

/// Serves tenant tokens and delegates `GET /chats/{id}` / `GET /messages/{id}`
/// to the supplied responders; anything else is a 404.
fn im_stub(
    chat_mode: impl Fn(&str) -> StubResponse + Send + Sync + 'static,
    message_get: impl Fn(&str) -> StubResponse + Send + Sync + 'static,
) -> Handler {
    Arc::new(move |request: &RecordedRequest| {
        if request.path == TOKEN_PATH {
            return StubResponse::json(
                200,
                r#"{"code":0,"tenant_access_token":"token-0","expire":7200}"#,
            );
        }
        if let Some(chat_id) = request.path.strip_prefix(CHATS_PREFIX) {
            return chat_mode(chat_id);
        }
        if let Some(message_id) = request.path.strip_prefix(MESSAGES_PREFIX) {
            return message_get(message_id);
        }
        StubResponse::text(404, "not found")
    })
}

fn chat_mode_ok(mode: &str) -> StubResponse {
    StubResponse::json(
        200,
        &format!(r#"{{"code":0,"data":{{"chat_mode":"{mode}"}}}}"#),
    )
}

fn message_ok(thread_id: Option<&str>) -> StubResponse {
    let thread = thread_id
        .map(|id| format!(r#","thread_id":"{id}""#))
        .unwrap_or_default();
    StubResponse::json(
        200,
        &format!(
            r#"{{"code":0,"data":{{"items":[{{"message_id":"om_backfill","chat_id":"oc_topic_chat","chat_type":"group","msg_type":"text"{thread}}}]}}}}"#
        ),
    )
}

fn failing(_: &str) -> StubResponse {
    StubResponse::text(500, "boom")
}

fn requests_to(server: &StubServer, prefix: &str) -> Vec<RecordedRequest> {
    server
        .requests()
        .into_iter()
        .filter(|request| request.path.starts_with(prefix))
        .collect()
}

/// Builds a minimal inline `im.message.receive_v1` payload.
fn make_event(
    chat_id: &str,
    chat_type: &str,
    message_id: &str,
    message_type: &str,
    content: &serde_json::Value,
    thread_id: Option<&str>,
    mentions: &serde_json::Value,
) -> String {
    serde_json::json!({
        "schema": "2.0",
        "header": {
            "event_id": format!("evt_{message_id}"),
            "event_type": "im.message.receive_v1",
        },
        "event": {
            "sender": {
                "sender_id": {"open_id": "ou_sender"},
                "sender_type": "user",
            },
            "message": {
                "message_id": message_id,
                "chat_id": chat_id,
                "chat_type": chat_type,
                "message_type": message_type,
                "content": content.to_string(),
                "create_time": "1700000004000",
                "thread_id": thread_id,
                "mentions": mentions,
            },
        },
    })
    .to_string()
}

fn text_event(chat_id: &str, message_id: &str, text: &str) -> String {
    make_event(
        chat_id,
        "group",
        message_id,
        "text",
        &serde_json::json!({"text": text}),
        None,
        &serde_json::json!([]),
    )
}

fn unwrap_event(outcome: NormalizeOutcome) -> (InboundEvent, Option<Degradation>) {
    match outcome {
        NormalizeOutcome::Event {
            event, degradation, ..
        } => (*event, degradation),
        NormalizeOutcome::Ignored { reason } => {
            panic!("expected an event outcome, got Ignored: {reason}");
        }
    }
}

#[tokio::test]
async fn p2p_text_normalizes_to_chat_scope_without_chat_lookup() {
    let server = StubServer::start(im_stub(failing, failing)).await;
    let normalizer = normalizer_for(&server);

    let outcome = normalizer
        .normalize(P2P_TEXT_FIXTURE.as_bytes())
        .await
        .expect("p2p fixture should normalize");
    let (event, degradation) = unwrap_event(outcome);

    assert_eq!(degradation, None);
    assert_eq!(event.event_id, "evt_p2p_scrubbed_001");
    assert_eq!(event.message_id, "om_p2p_001");
    assert_eq!(event.chat_id, "oc_p2p_chat");
    assert_eq!(event.sender_id, "ou_alice");
    assert_eq!(event.chat_type, lark_codex_bridge::lark::api::ChatMode::P2p);
    assert_eq!(event.thread_id, None);
    assert_eq!(event.root_id, None);
    assert_eq!(event.reply_to_message_id, None);
    assert_eq!(event.text, "hello bridge");
    assert!(!event.mentions_bot);
    assert!(!event.mention_all);
    assert!(event.mentions.is_empty());
    assert!(matches!(
        event.parts.as_slice(),
        [MessagePart::Text { text }] if text == "hello bridge"
    ));
    assert!(event.resources.is_empty());
    assert_eq!(event.message_type, "text");
    assert_eq!(event.create_time_ms, 1_700_000_000_123);
    assert_eq!(event.scope, ScopeKey::Chat("oc_p2p_chat".to_owned()));
    // p2p chats never need a chat-mode lookup.
    assert!(requests_to(&server, CHATS_PREFIX).is_empty());
}

#[tokio::test]
async fn group_mention_strips_tags_and_flags_the_bot() {
    let server = StubServer::start(im_stub(|_| chat_mode_ok("group"), failing)).await;
    let normalizer = normalizer_for(&server);

    let outcome = normalizer
        .normalize(GROUP_MENTION_FIXTURE.as_bytes())
        .await
        .expect("group fixture should normalize");
    let (event, degradation) = unwrap_event(outcome);

    assert_eq!(degradation, None);
    assert!(event.mentions_bot);
    assert!(!event.mention_all);
    assert_eq!(event.text, "status?");
    assert_eq!(event.mentions.len(), 2);
    assert_eq!(event.mentions[0].open_id.as_deref(), Some("ou_bot"));
    assert_eq!(
        event.mentions[0].user_id.as_deref(),
        Some("user_scrubbed_bot")
    );
    assert_eq!(
        event.mentions[0].union_id.as_deref(),
        Some("on_scrubbed_bot")
    );
    assert_eq!(event.mentions[0].name.as_deref(), Some("Bridge Bot"));
    assert_eq!(event.mentions[1].open_id.as_deref(), Some("ou_bob"));
    assert_eq!(
        event.chat_type,
        lark_codex_bridge::lark::api::ChatMode::Group
    );
    assert_eq!(event.scope, ScopeKey::Chat("oc_group_chat".to_owned()));
    assert_eq!(requests_to(&server, CHATS_PREFIX).len(), 1);
}

#[tokio::test]
async fn group_message_without_mention_does_not_flag_the_bot() {
    let server = StubServer::start(im_stub(|_| chat_mode_ok("group"), failing)).await;
    let normalizer = normalizer_for(&server);

    let payload = text_event("oc_group_chat", "om_plain", "plain message");
    let (event, degradation) = unwrap_event(
        normalizer
            .normalize(payload.as_bytes())
            .await
            .expect("plain group message should normalize"),
    );

    assert_eq!(degradation, None);
    assert!(!event.mentions_bot);
    assert!(!event.mention_all);
    assert_eq!(event.text, "plain message");
}

#[tokio::test]
async fn app_sender_type_is_not_treated_as_a_human() {
    let server = StubServer::start(im_stub(|_| chat_mode_ok("group"), failing)).await;
    let normalizer = normalizer_for(&server);

    let payload = serde_json::json!({
        "schema": "2.0",
        "header": {
            "event_id": "evt_app_sender",
            "event_type": "im.message.receive_v1",
        },
        "event": {
            "sender": {
                "sender_id": {"open_id": "ou_app_bot"},
                "sender_type": "app",
            },
            "message": {
                "message_id": "om_app_sender",
                "chat_id": "oc_group_chat",
                "chat_type": "group",
                "message_type": "text",
                "content": "{\"text\":\"plain\"}",
                "create_time": "1700000004000",
                "mentions": [],
            },
        },
    })
    .to_string();

    let (event, _) = unwrap_event(
        normalizer
            .normalize(payload.as_bytes())
            .await
            .expect("app sender event should normalize"),
    );
    assert!(!event.sender_is_human);
}

#[tokio::test]
async fn missing_sender_type_fails_closed_as_not_human() {
    let server = StubServer::start(im_stub(|_| chat_mode_ok("group"), failing)).await;
    let normalizer = normalizer_for(&server);

    let payload = serde_json::json!({
        "schema": "2.0",
        "header": {
            "event_id": "evt_no_sender_type",
            "event_type": "im.message.receive_v1",
        },
        "event": {
            "sender": {
                "sender_id": {"open_id": "ou_no_type"},
            },
            "message": {
                "message_id": "om_no_type",
                "chat_id": "oc_group_chat",
                "chat_type": "group",
                "message_type": "text",
                "content": "{\"text\":\"plain\"}",
                "create_time": "1700000004000",
                "mentions": [],
            },
        },
    })
    .to_string();

    let (event, _) = unwrap_event(
        normalizer
            .normalize(payload.as_bytes())
            .await
            .expect("missing sender type should normalize"),
    );
    assert!(!event.sender_is_human);
}

#[tokio::test]
async fn topic_reply_uses_thread_scope() {
    let server = StubServer::start(im_stub(|_| chat_mode_ok("topic"), failing)).await;
    let normalizer = normalizer_for(&server);

    let outcome = normalizer
        .normalize(TOPIC_REPLY_FIXTURE.as_bytes())
        .await
        .expect("topic fixture should normalize");
    let (event, degradation) = unwrap_event(outcome);

    assert_eq!(degradation, None);
    assert_eq!(
        event.chat_type,
        lark_codex_bridge::lark::api::ChatMode::Topic
    );
    assert_eq!(event.thread_id.as_deref(), Some("omt_topic_001"));
    assert_eq!(event.root_id.as_deref(), Some("om_topic_root"));
    assert_eq!(event.reply_to_message_id.as_deref(), Some("om_topic_root"));
    assert_eq!(event.text, "topic reply");
    assert!(event.mentions_bot);
    assert_eq!(
        event.scope,
        ScopeKey::Thread("oc_topic_chat".to_owned(), "omt_topic_001".to_owned())
    );
    // The event carried the thread id, so no backfill was needed.
    assert!(requests_to(&server, MESSAGES_PREFIX).is_empty());
}

#[tokio::test]
async fn topic_event_missing_thread_id_backfills_once() {
    let server = StubServer::start(im_stub(
        |_| chat_mode_ok("topic"),
        |_| message_ok(Some("omt_backfilled")),
    ))
    .await;
    let normalizer = normalizer_for(&server);

    let payload = text_event("oc_topic_chat", "om_no_thread", "thread-less topic message");
    let (event, degradation) = unwrap_event(
        normalizer
            .normalize(payload.as_bytes())
            .await
            .expect("topic event should normalize"),
    );

    assert_eq!(degradation, None);
    assert_eq!(event.thread_id.as_deref(), Some("omt_backfilled"));
    assert_eq!(
        event.scope,
        ScopeKey::Thread("oc_topic_chat".to_owned(), "omt_backfilled".to_owned())
    );
    let backfills = requests_to(&server, MESSAGES_PREFIX);
    assert_eq!(backfills.len(), 1);
    assert_eq!(backfills[0].method, "GET");
    assert_eq!(backfills[0].path, "/open-apis/im/v1/messages/om_no_thread");
}

#[tokio::test]
async fn backfill_failure_degrades_to_chat_scope() {
    let server = StubServer::start(im_stub(|_| chat_mode_ok("topic"), failing)).await;
    let normalizer = normalizer_for(&server);

    let payload = text_event("oc_topic_chat", "om_unbackfillable", "lost thread");
    let (event, degradation) = unwrap_event(
        normalizer
            .normalize(payload.as_bytes())
            .await
            .expect("degraded event should still normalize"),
    );

    assert_eq!(
        degradation,
        Some(Degradation::ThreadBackfillFailed {
            kind: LarkErrorKind::Retryable,
        })
    );
    assert_eq!(event.thread_id, None);
    assert_eq!(event.scope, ScopeKey::Chat("oc_topic_chat".to_owned()));
}

#[tokio::test]
async fn backfill_without_thread_id_degrades_to_chat_scope() {
    let server = StubServer::start(im_stub(|_| chat_mode_ok("topic"), |_| message_ok(None))).await;
    let normalizer = normalizer_for(&server);

    let payload = text_event("oc_topic_chat", "om_truly_threadless", "no thread anywhere");
    let (event, degradation) = unwrap_event(
        normalizer
            .normalize(payload.as_bytes())
            .await
            .expect("degraded event should still normalize"),
    );

    assert_eq!(degradation, Some(Degradation::ThreadBackfillMissing));
    assert_eq!(event.scope, ScopeKey::Chat("oc_topic_chat".to_owned()));
}

#[tokio::test]
async fn quoted_message_links_parent_without_history_fetch() {
    let server = StubServer::start(im_stub(|_| chat_mode_ok("group"), failing)).await;
    let normalizer = normalizer_for(&server);

    let outcome = normalizer
        .normalize(QUOTE_FIXTURE.as_bytes())
        .await
        .expect("quote fixture should normalize");
    let (event, degradation) = unwrap_event(outcome);

    assert_eq!(degradation, None);
    assert_eq!(event.root_id.as_deref(), Some("om_quoted_root"));
    assert_eq!(
        event.reply_to_message_id.as_deref(),
        Some("om_quoted_parent")
    );
    assert_eq!(event.text, "agreed, ship it");
    assert_eq!(event.scope, ScopeKey::Chat("oc_group_chat".to_owned()));
    // Quote linkage is a single hop from the event itself: no fetches.
    assert!(requests_to(&server, MESSAGES_PREFIX).is_empty());
}

#[tokio::test]
async fn image_and_file_messages_describe_resources_without_bytes() {
    let server = StubServer::start(im_stub(|_| chat_mode_ok("group"), failing)).await;
    let normalizer = normalizer_for(&server);

    let image = make_event(
        "oc_group_chat",
        "group",
        "om_image",
        "image",
        &serde_json::json!({"image_key": "img_scrubbed_key"}),
        None,
        &serde_json::json!([]),
    );
    let (event, _) = unwrap_event(
        normalizer
            .normalize(image.as_bytes())
            .await
            .expect("image message should normalize"),
    );
    assert_eq!(event.message_type, "image");
    assert!(event.text.is_empty());
    assert_eq!(event.resources.len(), 1);
    assert_eq!(event.resources[0].kind, ResourceKind::Image);
    assert_eq!(event.resources[0].key, "img_scrubbed_key");
    assert!(matches!(
        event.parts.as_slice(),
        [MessagePart::Image(media)]
            if media.key.as_deref() == Some("img_scrubbed_key")
                && media.status == PartStatus::Available
    ));

    let file = make_event(
        "oc_group_chat",
        "group",
        "om_file",
        "file",
        &serde_json::json!({
            "file_key": "file_scrubbed_key",
            "file_name": "secret plans.txt",
            "mime_type": "text/plain",
            "file_size": "42"
        }),
        None,
        &serde_json::json!([]),
    );
    let outcome = normalizer
        .normalize(file.as_bytes())
        .await
        .expect("file message should normalize");
    let (event, _) = unwrap_event(outcome.clone());
    assert_eq!(event.message_type, "file");
    assert_eq!(event.resources.len(), 1);
    assert_eq!(event.resources[0].kind, ResourceKind::File);
    assert_eq!(event.resources[0].key, "file_scrubbed_key");
    let [MessagePart::File(media)] = event.parts.as_slice() else {
        panic!("file message should have one typed file part");
    };
    assert_eq!(
        media.metadata.file_name.as_deref(),
        Some("secret plans.txt")
    );
    assert_eq!(media.metadata.mime_type.as_deref(), Some("text/plain"));
    assert_eq!(media.metadata.size_bytes, Some(42));
    assert_eq!(media.status, PartStatus::Available);
    // User-chosen metadata is retained for authorized resolution but remains
    // redacted from diagnostic output.
    let debug = format!("{outcome:?}");
    assert!(!debug.contains("secret plans"));
    assert!(!debug.contains("text/plain"));
}

#[tokio::test]
async fn sticker_is_a_typed_part_and_unknown_types_are_explicitly_unsupported() {
    let server = StubServer::start(im_stub(|_| chat_mode_ok("group"), failing)).await;
    let normalizer = normalizer_for(&server);

    let sticker = make_event(
        "oc_group_chat",
        "group",
        "om_sticker",
        "sticker",
        &serde_json::json!({"file_key": "stk_scrubbed"}),
        None,
        &serde_json::json!([]),
    );
    let (event, degradation) = unwrap_event(
        normalizer
            .normalize(sticker.as_bytes())
            .await
            .expect("unknown types should still normalize"),
    );

    assert_eq!(degradation, None);
    assert_eq!(event.message_type, "sticker");
    assert!(event.text.is_empty());
    assert!(event.resources.is_empty());
    assert!(matches!(
        event.parts.as_slice(),
        [MessagePart::Sticker(media)]
            if media.key.as_deref() == Some("stk_scrubbed")
                && media.status == PartStatus::Available
    ));

    let unknown = make_event(
        "oc_group_chat",
        "group",
        "om_unknown",
        "future_kind",
        &serde_json::json!({"secret": "opaque"}),
        None,
        &serde_json::json!([]),
    );
    let (event, _) = unwrap_event(
        normalizer
            .normalize(unknown.as_bytes())
            .await
            .expect("unknown types should still normalize"),
    );
    assert!(matches!(
        event.parts.as_slice(),
        [MessagePart::Unsupported { message_type, status }]
            if message_type == "future_kind" && *status == PartStatus::Unsupported
    ));
}

#[tokio::test]
async fn audio_video_card_and_forward_have_typed_availability() {
    let server = StubServer::start(im_stub(|_| chat_mode_ok("group"), failing)).await;
    let normalizer = normalizer_for(&server);

    let cases = [
        (
            "audio",
            serde_json::json!({"file_key":"aud_key","duration":1234}),
        ),
        (
            "media",
            serde_json::json!({"file_key":"vid_key","image_key":"thumb_key","duration_ms":"5678"}),
        ),
        ("interactive", serde_json::json!({"elements":[]})),
        (
            "merge_forward",
            serde_json::json!({"message_id":"om_forwarded"}),
        ),
    ];
    for (index, (kind, content)) in cases.into_iter().enumerate() {
        let payload = make_event(
            "oc_group_chat",
            "group",
            &format!("om_rich_{index}"),
            kind,
            &content,
            None,
            &serde_json::json!([]),
        );
        let (event, _) = unwrap_event(
            normalizer
                .normalize(payload.as_bytes())
                .await
                .expect("rich message should normalize"),
        );
        match (kind, event.parts.as_slice()) {
            ("audio", [MessagePart::Audio(media)]) => {
                assert_eq!(media.key.as_deref(), Some("aud_key"));
                assert_eq!(media.metadata.duration_ms, Some(1234));
                assert_eq!(media.status, PartStatus::Available);
            }
            ("media", [MessagePart::Video(media)]) => {
                assert_eq!(media.key.as_deref(), Some("vid_key"));
                assert_eq!(media.thumbnail_key.as_deref(), Some("thumb_key"));
                assert_eq!(media.metadata.duration_ms, Some(5678));
            }
            ("interactive", [MessagePart::Card { status }]) => {
                assert_eq!(*status, PartStatus::Unsupported);
            }
            ("merge_forward", [MessagePart::Forward { message_id, status }]) => {
                assert_eq!(message_id.as_deref(), Some("om_forwarded"));
                assert_eq!(*status, PartStatus::Available);
            }
            _ => panic!("unexpected typed part for {kind}"),
        }
    }
}

#[tokio::test]
async fn audio_client_transcript_is_absent_from_the_durable_event() {
    let server = StubServer::start(im_stub(|_| chat_mode_ok("group"), failing)).await;
    let normalizer = normalizer_for(&server);
    let payload = make_event(
        "oc_group_chat",
        "group",
        "om_audio_text",
        "audio",
        &serde_json::json!({
            "file_key": "aud_key",
            "duration": 2100,
            "text": "  please review the patch  "
        }),
        None,
        &serde_json::json!([]),
    );
    let outcome = normalizer
        .normalize(payload.as_bytes())
        .await
        .expect("audio with transcript should normalize");
    let NormalizeOutcome::Event {
        event,
        live_transcripts,
        ..
    } = outcome
    else {
        panic!("expected audio event")
    };
    assert!(!format!("{live_transcripts:?}").contains("please review the patch"));
    let event = *event;
    assert!(
        event.text.is_empty(),
        "inbound recognition must not bypass the configured tool limit"
    );
    match event.parts.as_slice() {
        [MessagePart::Audio(media)] => {
            assert_eq!(media.key.as_deref(), Some("aud_key"));
            assert_eq!(
                media.metadata.transcript_failure,
                Some(TranscriptFailure::NotRetained)
            );
            let serialized = serde_json::to_string(&media.metadata).expect("metadata JSON");
            assert!(!serialized.contains("please review the patch"));
        }
        _ => panic!("expected one audio part"),
    }
}

#[tokio::test]
async fn malformed_and_oversize_audio_transcripts_keep_non_content_failure_classification() {
    let server = StubServer::start(im_stub(|_| chat_mode_ok("group"), failing)).await;
    let normalizer = normalizer_for(&server);
    for (message_id, transcript, expected) in [
        (
            "om_audio_invalid",
            serde_json::json!({"nested": "must-not-fall-back"}),
            TranscriptFailure::Invalid,
        ),
        (
            "om_audio_oversize",
            serde_json::Value::String("x".repeat(ASR_TRANSCRIPT_MAX_BYTES + 1)),
            TranscriptFailure::TooLarge,
        ),
    ] {
        let payload = make_event(
            "oc_group_chat",
            "group",
            message_id,
            "audio",
            &serde_json::json!({
                "file_key": "aud_key",
                "duration": 2100,
                "text": transcript,
                "recognition": {"text": "must-not-replace-a-present-invalid-value"}
            }),
            None,
            &serde_json::json!([]),
        );
        let (event, _) = unwrap_event(
            normalizer
                .normalize(payload.as_bytes())
                .await
                .expect("audio rejection metadata should normalize"),
        );
        let [MessagePart::Audio(media)] = event.parts.as_slice() else {
            panic!("expected audio part")
        };
        assert_eq!(media.metadata.transcript_failure, Some(expected));
        let debug = format!("{:?}", media.metadata);
        assert!(!debug.contains("must-not"));
        assert!(
            !serde_json::to_string(&media.metadata)
                .expect("serialize normalized metadata")
                .contains("must-not")
        );
    }
}

#[tokio::test]
async fn unsafe_optional_media_metadata_is_dropped_without_losing_the_handle() {
    let server = StubServer::start(im_stub(|_| chat_mode_ok("group"), failing)).await;
    let normalizer = normalizer_for(&server);
    let payload = make_event(
        "oc_group_chat",
        "group",
        "om_unsafe_metadata",
        "file",
        &serde_json::json!({
            "file_key":"safe_key",
            "file_name":"../escape",
            "mime_type":"text/plain\nsecret"
        }),
        None,
        &serde_json::json!([]),
    );
    let (event, _) = unwrap_event(
        normalizer
            .normalize(payload.as_bytes())
            .await
            .expect("normalize"),
    );
    let [MessagePart::File(media)] = event.parts.as_slice() else {
        panic!("expected file part");
    };
    assert_eq!(media.key.as_deref(), Some("safe_key"));
    assert_eq!(media.metadata.file_name, None);
    assert_eq!(media.metadata.mime_type, None);
}

#[test]
fn scope_key_renders_both_forms() {
    assert_eq!(
        ScopeKey::Chat("oc_chat".to_owned()).to_string(),
        "im:oc_chat"
    );
    assert_eq!(
        ScopeKey::Thread("oc_chat".to_owned(), "omt_thread".to_owned()).to_string(),
        "im:oc_chat:thread:omt_thread"
    );
}

#[tokio::test]
async fn mention_all_is_detected_and_stripped() {
    let server = StubServer::start(im_stub(|_| chat_mode_ok("group"), failing)).await;
    let normalizer = normalizer_for(&server);

    let payload = text_event(
        "oc_group_chat",
        "om_all",
        "<at user_id=\"all\">所有人</at> meeting now",
    );
    let (event, _) = unwrap_event(
        normalizer
            .normalize(payload.as_bytes())
            .await
            .expect("mention-all message should normalize"),
    );

    assert!(event.mention_all);
    assert!(!event.mentions_bot);
    assert_eq!(event.text, "meeting now");
}

#[tokio::test]
async fn chat_mode_is_cached_per_chat() {
    let server = StubServer::start(im_stub(|_| chat_mode_ok("group"), failing)).await;
    let normalizer = normalizer_for(&server);

    for index in 0..3 {
        let payload = text_event("oc_group_chat", &format!("om_repeat_{index}"), "again");
        normalizer
            .normalize(payload.as_bytes())
            .await
            .expect("repeat message should normalize");
    }

    assert_eq!(requests_to(&server, CHATS_PREFIX).len(), 1);
    assert_eq!(normalizer.cached_chat_mode_count(), 1);
}

#[tokio::test]
async fn message_level_thread_id_invalidates_a_contradicting_cache_entry() {
    let calls = Arc::new(AtomicUsize::new(0));
    let server = StubServer::start(im_stub(
        {
            let calls = Arc::clone(&calls);
            move |_| {
                // First probe says plain group; after invalidation the chat
                // has become a topic group.
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    chat_mode_ok("group")
                } else {
                    chat_mode_ok("topic")
                }
            }
        },
        failing,
    ))
    .await;
    let normalizer = normalizer_for(&server);

    let first = text_event("oc_convertible", "om_before", "before conversion");
    let (event, _) = unwrap_event(
        normalizer
            .normalize(first.as_bytes())
            .await
            .expect("first message should normalize"),
    );
    assert_eq!(
        event.chat_type,
        lark_codex_bridge::lark::api::ChatMode::Group
    );

    let second = make_event(
        "oc_convertible",
        "group",
        "om_after",
        "text",
        &serde_json::json!({"text": "after conversion"}),
        Some("omt_new_topic"),
        &serde_json::json!([]),
    );
    let (event, degradation) = unwrap_event(
        normalizer
            .normalize(second.as_bytes())
            .await
            .expect("thread message should normalize"),
    );

    assert_eq!(degradation, None);
    // The cached "group" entry was contradicted, invalidated, and re-probed.
    assert_eq!(
        event.chat_type,
        lark_codex_bridge::lark::api::ChatMode::Topic
    );
    assert_eq!(
        event.scope,
        ScopeKey::Thread("oc_convertible".to_owned(), "omt_new_topic".to_owned())
    );
    assert_eq!(requests_to(&server, CHATS_PREFIX).len(), 2);
}

#[tokio::test]
async fn chat_mode_lookup_failure_falls_back_to_group_without_caching() {
    let server = StubServer::start(im_stub(failing, failing)).await;
    let normalizer = normalizer_for(&server);

    for index in 0..2 {
        let payload = text_event("oc_flaky", &format!("om_flaky_{index}"), "still here");
        let (event, degradation) = unwrap_event(
            normalizer
                .normalize(payload.as_bytes())
                .await
                .expect("fallback message should normalize"),
        );
        assert_eq!(
            event.chat_type,
            lark_codex_bridge::lark::api::ChatMode::Group
        );
        assert_eq!(event.scope, ScopeKey::Chat("oc_flaky".to_owned()));
        assert_eq!(degradation, Some(Degradation::ChatModeLookupFailed));
    }

    // The failure was not cached: each message retried the lookup.
    assert_eq!(requests_to(&server, CHATS_PREFIX).len(), 2);
    assert_eq!(normalizer.cached_chat_mode_count(), 0);
}

#[tokio::test]
async fn chat_mode_cache_entries_expire_after_the_ttl() {
    let server = StubServer::start(im_stub(|_| chat_mode_ok("group"), failing)).await;
    let normalizer = normalizer_for(&server);
    let start = Instant::now();

    let payload = text_event("oc_expiring", "om_ttl_0", "first");
    normalizer
        .normalize_at(payload.as_bytes(), start)
        .await
        .expect("first message should normalize");
    let payload = text_event("oc_expiring", "om_ttl_1", "second");
    normalizer
        .normalize_at(
            payload.as_bytes(),
            start + LARK_CHAT_MODE_CACHE_TTL + Duration::from_secs(1),
        )
        .await
        .expect("second message should normalize");

    assert_eq!(requests_to(&server, CHATS_PREFIX).len(), 2);
}

#[tokio::test]
async fn chat_mode_cache_count_stays_bounded() {
    let server = StubServer::start(im_stub(|_| chat_mode_ok("group"), failing)).await;
    let normalizer = normalizer_for(&server);

    for index in 0..LARK_CHAT_MODE_CACHE_CAPACITY + 8 {
        let payload = text_event(
            &format!("oc_hot_{index}"),
            &format!("om_hot_{index}"),
            "hot chat",
        );
        normalizer
            .normalize(payload.as_bytes())
            .await
            .expect("hot-chat message should normalize");
    }

    assert_eq!(
        requests_to(&server, CHATS_PREFIX).len(),
        LARK_CHAT_MODE_CACHE_CAPACITY + 8
    );
    assert!(normalizer.cached_chat_mode_count() <= LARK_CHAT_MODE_CACHE_CAPACITY);
}

#[tokio::test]
async fn debug_output_redacts_message_content_and_routing_ids() {
    let server = StubServer::start(im_stub(failing, failing)).await;
    let normalizer = normalizer_for(&server);

    let outcome = normalizer
        .normalize(P2P_TEXT_FIXTURE.as_bytes())
        .await
        .expect("p2p fixture should normalize");
    let debug = format!("{outcome:?}");

    for sensitive in [
        "hello bridge",
        "evt_p2p_scrubbed_001",
        "om_p2p_scrubbed_001",
        "oc_p2p_chat",
        "im:oc_p2p_chat",
    ] {
        assert!(
            !debug.contains(sensitive),
            "debug leaked {sensitive}: {debug}"
        );
    }
    assert!(debug.contains("sha256="));
    assert!(debug.contains("text_len: 12"));
}

#[tokio::test]
async fn malformed_payloads_are_protocol_violations() {
    let server = StubServer::start(im_stub(failing, failing)).await;
    let normalizer = normalizer_for(&server);

    let error = normalizer
        .normalize(b"not json")
        .await
        .expect_err("invalid JSON must fail");
    assert!(matches!(error, LarkError::ProtocolViolation { .. }));

    let missing_id = serde_json::json!({
        "header": {"event_id": "evt_bad", "event_type": "im.message.receive_v1"},
        "event": {
            "sender": {"sender_id": {"open_id": "ou_sender"}},
            "message": {
                "chat_id": "oc_chat",
                "chat_type": "p2p",
                "message_type": "text",
                "content": "{\"text\":\"hi\"}",
                "create_time": "1700000004000",
            },
        },
    })
    .to_string();
    let error = normalizer
        .normalize(missing_id.as_bytes())
        .await
        .expect_err("a missing message_id must fail");
    assert!(matches!(error, LarkError::ProtocolViolation { .. }));
}

#[tokio::test]
async fn oversize_payloads_are_rejected_before_parsing() {
    let server = StubServer::start(im_stub(failing, failing)).await;
    let normalizer = normalizer_for(&server);

    let payload = vec![b' '; LARK_MAX_EVENT_PAYLOAD_BYTES + 1];
    let error = normalizer
        .normalize(&payload)
        .await
        .expect_err("an oversize payload must fail");
    assert!(matches!(error, LarkError::Exhausted { .. }));
}

#[tokio::test]
async fn non_message_events_are_ignored() {
    let server = StubServer::start(im_stub(failing, failing)).await;
    let normalizer = normalizer_for(&server);

    let payload = serde_json::json!({
        "header": {"event_id": "evt_other", "event_type": "im.chat.member.user.added_v1"},
        "event": {},
    })
    .to_string();
    let outcome = normalizer
        .normalize(payload.as_bytes())
        .await
        .expect("other events should parse");

    assert!(matches!(outcome, NormalizeOutcome::Ignored { .. }));
}
