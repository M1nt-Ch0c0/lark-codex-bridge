//! Durable outbox: versioned payload codec, idempotent enqueue, and the
//! bounded pump's delivery semantics against the Lark HTTP stub.
//!
//! Everything is offline and deterministic: a hand-rolled HTTP stub plus an
//! in-memory store.

mod larkstub;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use lark_codex_bridge::codex::client::{ThreadId, TurnId, TurnOutcome};
use lark_codex_bridge::codex::types::{MessagePhase, ThreadItem, TurnStatus};
use lark_codex_bridge::lark::api::LarkApi;
use lark_codex_bridge::lark::config::{LarkEndpoints, TenantBrand};
use lark_codex_bridge::lark::credentials::LarkCredentials;
use lark_codex_bridge::lark::error::LarkError;
use lark_codex_bridge::lark::http::LarkHttp;
use lark_codex_bridge::lark::normalize::{InboundEvent, ScopeKey};
use lark_codex_bridge::lark::token::TenantTokenProvider;
use lark_codex_bridge::lark::transport::TransportState;
use lark_codex_bridge::limits::{LARK_CARD_MARKDOWN_ELEMENT_MAX_BYTES, STORE_OUTBOX_MAX_ATTEMPTS};
use lark_codex_bridge::outbox::{
    AppliedCertainty, DeliveryClass, OutboxError, OutboxOperation, OutboxPump, OutboxPumpConfig,
    OutboxReplySink, Retryability, classify_delivery, delivery_decision,
};
use lark_codex_bridge::render::{ProjectedReply, card_markdown_element_wire_len};
use lark_codex_bridge::runtime::intake::TenantNamespace;
use lark_codex_bridge::runtime::scope::{
    DurableReplySink, ReplySinkError, TurnFinalization, TurnProgress, TurnSource,
};
use lark_codex_bridge::store::{
    InboundKey, InboundRejectionKind, NewOutboxRow, OutboxEnqueue, OutboxState, StoreHandle,
    TurnResolution,
};
use larkstub::{Handler, RecordedRequest, StubResponse, StubServer};
use secrecy::SecretString;
use serde_json::Map;
use tempfile::tempdir;
use tokio::sync::watch;
use url::Url;

const TOKEN_PATH: &str = "/open-apis/auth/v3/tenant_access_token/internal";
const REPLY_PATH: &str = "/open-apis/im/v1/messages/om_parent/reply";

fn api_for(server: &StubServer) -> LarkApi {
    let base = Url::parse(&server.url()).expect("stub URL should parse");
    let endpoints = LarkEndpoints {
        open_base: base.clone(),
        accounts_base: base,
    };
    let http = LarkHttp::new(endpoints).expect("HTTP client should build");
    let creds = LarkCredentials::new(
        "cli_test_app".to_owned(),
        SecretString::from("test-secret-material"),
        TenantBrand::Feishu,
    );
    let tokens = TenantTokenProvider::new(http.clone(), creds);
    LarkApi::new(http, tokens)
}

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

fn reply_requests(server: &StubServer) -> Vec<RecordedRequest> {
    server
        .requests()
        .into_iter()
        .filter(|request| request.path.starts_with("/open-apis/im/v1/messages/"))
        .collect()
}

fn reply_text(request: &RecordedRequest) -> String {
    let envelope: serde_json::Value =
        serde_json::from_slice(&request.body).expect("reply request should be JSON");
    let content = envelope["content"]
        .as_str()
        .expect("reply request should carry a content string");
    let content: serde_json::Value =
        serde_json::from_str(content).expect("content should be a JSON string");
    content["text"]
        .as_str()
        .expect("content should carry text")
        .to_owned()
}

fn reply_markdown(request: &RecordedRequest) -> String {
    let envelope: serde_json::Value =
        serde_json::from_slice(&request.body).expect("reply request should be JSON");
    assert_eq!(envelope["msg_type"], "post");
    let content = envelope["content"]
        .as_str()
        .expect("reply request should carry a content string");
    let content: serde_json::Value =
        serde_json::from_str(content).expect("post content should be JSON");
    content["zh_cn"]["content"][0][0]["text"]
        .as_str()
        .expect("post should carry one Markdown element")
        .to_owned()
}

fn card_markdown(request: &RecordedRequest) -> String {
    let envelope: serde_json::Value =
        serde_json::from_slice(&request.body).expect("card request should be JSON");
    let content = envelope["content"]
        .as_str()
        .expect("card request should carry a content string");
    let card: serde_json::Value =
        serde_json::from_str(content).expect("card content should be JSON");
    card["body"]["elements"][0]["content"]
        .as_str()
        .expect("card should carry Markdown")
        .to_owned()
}

fn card_fences_are_balanced(markdown: &str) -> bool {
    let mut open: Option<(char, usize)> = None;
    for line in markdown.lines() {
        let trimmed = line.trim().trim_start_matches('>').trim_start();
        let Some(marker @ ('`' | '~')) = trimmed.chars().next() else {
            continue;
        };
        let run = trimmed
            .chars()
            .take_while(|character| *character == marker)
            .count();
        if run < 3 {
            continue;
        }
        match open {
            Some((current, length))
                if current == marker
                    && run >= length
                    && trimmed.chars().skip(run).all(char::is_whitespace) =>
            {
                open = None;
            }
            None => open = Some((marker, run)),
            _ => {}
        }
    }
    open.is_none()
}

fn fast_config() -> OutboxPumpConfig {
    OutboxPumpConfig {
        retry_base: Duration::from_millis(1),
        retry_max: Duration::from_millis(5),
        poll_interval: Duration::from_millis(5),
        claim_batch: 64,
    }
}

/// Claims one row per poll and backs off long enough that a deferred row stays
/// parked across several poll cycles, exposing cross-batch reordering.
fn slow_retry_config() -> OutboxPumpConfig {
    OutboxPumpConfig {
        retry_base: Duration::from_millis(200),
        retry_max: Duration::from_millis(500),
        poll_interval: Duration::from_millis(5),
        claim_batch: 1,
    }
}

fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}

fn source(event_id: &str, message_id: &str) -> TurnSource {
    TurnSource {
        event_id: event_id.to_owned(),
        message_id: message_id.to_owned(),
        chat_id: "oc_chat".to_owned(),
        thread_id: None,
    }
}

fn many_sources(n: usize) -> Vec<TurnSource> {
    (0..n)
        .map(|index| source(&format!("evt_{index}"), &format!("om_{index}")))
        .collect()
}

fn agent(text: &str, phase: Option<MessagePhase>) -> ThreadItem {
    ThreadItem::AgentMessage {
        id: "agent_1".to_owned(),
        text: text.to_owned(),
        phase,
        memory_citation: None,
        extra: Map::new(),
    }
}

fn completed_finalization(turn_row_id: i64, text: &str) -> TurnFinalization {
    TurnFinalization {
        turn_row_id,
        scope_key: "im:oc_chat".to_owned(),
        sources: vec![source("evt_1", "om_parent")],
        resolution: TurnResolution::Completed,
        outcome: Some(TurnOutcome {
            thread_id: ThreadId::from("thread_1"),
            turn_id: TurnId::from("turn_1"),
            status: TurnStatus::Completed,
            error: None,
            completed_items: vec![agent(text, Some(MessagePhase::FinalAnswer))],
            token_usage: None,
        }),
    }
}

fn inbound_event() -> InboundEvent {
    InboundEvent {
        event_id: "evt_1".to_owned(),
        message_id: "om_parent".to_owned(),
        chat_id: "oc_chat".to_owned(),
        sender_id: "ou_sender".to_owned(),
        chat_type: lark_codex_bridge::lark::api::ChatMode::P2p,
        thread_id: None,
        root_id: None,
        reply_to_message_id: None,
        text: "hello".to_owned(),
        mentions_bot: true,
        mention_all: false,
        sender_is_human: true,
        mentions: Vec::new(),
        parts: Vec::new(),
        resources: vec![],
        message_type: "text".to_owned(),
        create_time_ms: 0,
        scope: ScopeKey::Chat("oc_chat".to_owned()),
    }
}

fn test_inbound_key(event: &InboundEvent) -> InboundKey {
    let credentials = LarkCredentials::new(
        "cli_outbox_test".to_owned(),
        SecretString::from("outbox-test-secret"),
        TenantBrand::Feishu,
    );
    InboundKey::new(
        TenantNamespace::from_credentials(&credentials),
        event.event_id.clone(),
    )
}

async fn enqueue_reply(
    store: &StoreHandle,
    key: &str,
    message_id: &str,
    thread_id: Option<&str>,
    text: &str,
) -> i64 {
    let operation = OutboxOperation::ReplyText {
        message_id: message_id.to_owned(),
        thread_id: thread_id.map(str::to_owned),
        text: text.to_owned(),
    };
    let payload_json = operation.encode().expect("encode");
    match store
        .enqueue_outbox(NewOutboxRow {
            idempotency_key: key.to_owned(),
            scope_key: "im:oc_chat".to_owned(),
            kind: "final".to_owned(),
            payload_json,
            next_retry_ms: 0,
        })
        .await
        .expect("enqueue")
    {
        OutboxEnqueue::New(row) | OutboxEnqueue::Duplicate(row) => row.id,
    }
}

async fn wait_for_state(
    store: &StoreHandle,
    id: i64,
    want: OutboxState,
) -> lark_codex_bridge::store::OutboxRow {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let row = store
            .outbox_row(id)
            .await
            .expect("row read")
            .expect("row must exist");
        if row.state == want {
            return row;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {want:?}; last state {:?}",
            row.state
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_attempts(
    store: &StoreHandle,
    id: i64,
    want: u32,
) -> lark_codex_bridge::store::OutboxRow {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let row = store
            .outbox_row(id)
            .await
            .expect("row read")
            .expect("row must exist");
        if row.attempts == want {
            return row;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for attempts=={want}; last {row:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ---------------------------------------------------------------------------
// A2: versioned, strict payload codec.
// ---------------------------------------------------------------------------

#[test]
fn payload_roundtrips_and_redacts_text() {
    let operation = OutboxOperation::ReplyText {
        message_id: "om_parent".to_owned(),
        thread_id: Some("omt_thread".to_owned()),
        text: "secret reply body".to_owned(),
    };
    let json = operation.encode().expect("encode");
    assert!(json.contains("\"version\":2"));
    assert_eq!(OutboxOperation::decode(&json).expect("decode"), operation);

    let rendered = format!("{operation:?}");
    assert!(!rendered.contains("secret reply body"));
    assert!(rendered.contains("text_chars"));
}

#[test]
fn markdown_post_payload_roundtrips_without_losing_its_semantic_type() {
    let operation = OutboxOperation::ReplyMarkdownPost {
        message_id: "om_parent".to_owned(),
        thread_id: Some("omt_thread".to_owned()),
        markdown: "**private result**".to_owned(),
    };
    let json = operation.encode().expect("encode");
    assert!(json.contains("\"op\":\"reply_markdown_post\""));
    assert_eq!(OutboxOperation::decode(&json).expect("decode"), operation);
    assert!(!format!("{operation:?}").contains("private result"));
}

#[test]
fn payload_rejects_unknown_fields() {
    let json = r#"{"version":2,"op":"reply_text","message_id":"m","text":"t","bogus":1}"#;
    assert!(matches!(
        OutboxOperation::decode(json),
        Err(OutboxError::Deserialize)
    ));
}

#[test]
fn payload_rejects_wrong_version() {
    let json = r#"{"version":3,"op":"reply_text","message_id":"m","text":"t"}"#;
    assert!(matches!(
        OutboxOperation::decode(json),
        Err(OutboxError::UnsupportedVersion { version: 3 })
    ));
}

#[test]
fn payload_rejects_unknown_operation() {
    let json = r#"{"version":2,"op":"send_text","message_id":"m","text":"t"}"#;
    assert!(matches!(
        OutboxOperation::decode(json),
        Err(OutboxError::UnknownOperation)
    ));
}

#[test]
fn payload_v1_reader_preserves_text_carrier_and_safely_projects_card_fallback() {
    let text = r#"{"version":1,"op":"reply_text","message_id":"om_old","thread_id":null,"text":"**literal Markdown**"}"#;
    assert!(matches!(
        OutboxOperation::decode(text).expect("legacy text"),
        OutboxOperation::ReplyText { text, .. } if text == "**literal Markdown**"
    ));

    let final_card = r##"{"version":1,"op":"finalize_progress_card","message_id":"om_old","thread_id":null,"anchor_key":"old:progress","text":"# Done\n<at id=\"ou_bad\">name</at> `[^code]` [^note]"}"##;
    let decoded = OutboxOperation::decode(final_card).expect("legacy final card");
    let OutboxOperation::FinalizeProgressCard {
        text,
        fallback_markdown,
        ..
    } = decoded
    else {
        panic!("expected legacy progress finalization")
    };
    assert!(
        text.contains("ou_bad"),
        "card source remains a private field"
    );
    assert_eq!(
        fallback_markdown,
        "**Done**\nname `[^code]` (footnote note)"
    );
    assert!(!fallback_markdown.contains("<at"));
}

#[test]
fn payload_versions_are_strict_at_the_carrier_boundary() {
    let v1_with_v2_field = r#"{"version":1,"op":"finalize_progress_card","message_id":"m","anchor_key":"a","text":"t","fallback_markdown":"**t**"}"#;
    assert!(matches!(
        OutboxOperation::decode(v1_with_v2_field),
        Err(OutboxError::Deserialize)
    ));
    let v1_new_operation =
        r#"{"version":1,"op":"reply_markdown_post","message_id":"m","text":"**t**"}"#;
    assert!(matches!(
        OutboxOperation::decode(v1_new_operation),
        Err(OutboxError::UnknownOperation)
    ));
    let v2_missing_fallback = r#"{"version":2,"op":"finalize_progress_card","message_id":"m","anchor_key":"a","text":"t"}"#;
    assert!(matches!(
        OutboxOperation::decode(v2_missing_fallback),
        Err(OutboxError::Invalid { .. })
    ));
}

#[test]
fn payload_rejects_oversize_input() {
    let operation = OutboxOperation::ReplyText {
        message_id: "om_parent".to_owned(),
        thread_id: None,
        text: "x".repeat(300 * 1024),
    };
    assert!(matches!(
        operation.encode(),
        Err(OutboxError::PayloadTooLarge { .. })
    ));
}

// ---------------------------------------------------------------------------
// Delivery classification.
// ---------------------------------------------------------------------------

#[test]
fn delivery_classification_distinguishes_the_three_outcomes() {
    assert_eq!(
        classify_delivery(&LarkError::Retryable {
            context: "send",
            code: Some(99_991_400),
        }),
        DeliveryClass::Retryable
    );
    assert_eq!(
        classify_delivery(&LarkError::Retryable {
            context: "send",
            code: Some(500),
        }),
        DeliveryClass::Uncertain,
        "an HTTP 5xx can follow an applied POST"
    );
    assert_eq!(
        delivery_decision(&LarkError::Retryable {
            context: "send",
            code: Some(502),
        }),
        lark_codex_bridge::outbox::DeliveryDecision {
            applied: AppliedCertainty::Uncertain,
            retryability: Retryability::Retryable,
        },
        "5xx usefulness for idempotent PATCH is separate from POST certainty"
    );
    assert_eq!(
        classify_delivery(&LarkError::Retryable {
            context: "send",
            code: None,
        }),
        DeliveryClass::Uncertain
    );
    // An explicit 4xx rejection is definitely not applied, but validation and
    // business failures are permanent rather than retryable.
    assert_eq!(
        classify_delivery(&LarkError::ProtocolViolation {
            context: "send",
            code: Some(400),
        }),
        DeliveryClass::Permanent
    );
    // A protocol violation without a peer status (unparsable body, missing
    // fields) means the send may have been applied: uncertain.
    assert_eq!(
        classify_delivery(&LarkError::ProtocolViolation {
            context: "send",
            code: None,
        }),
        DeliveryClass::Uncertain
    );
    assert_eq!(
        classify_delivery(&LarkError::PermanentAuth {
            context: "send",
            code: Some(99_991_661),
        }),
        DeliveryClass::Permanent
    );
    assert_eq!(
        classify_delivery(&LarkError::Exhausted {
            context: "send",
            limit: 256 * 1024,
        }),
        DeliveryClass::Permanent
    );
    assert_eq!(
        delivery_decision(&LarkError::ProtocolViolation {
            context: "recalled message",
            code: Some(230_020),
        }),
        lark_codex_bridge::outbox::DeliveryDecision {
            applied: AppliedCertainty::DefinitelyNotApplied,
            retryability: Retryability::Permanent,
        }
    );
    assert_eq!(
        delivery_decision(&LarkError::protocol("malformed post-write response")),
        lark_codex_bridge::outbox::DeliveryDecision {
            applied: AppliedCertainty::Uncertain,
            retryability: Retryability::Permanent,
        }
    );
    assert_eq!(
        delivery_decision(&LarkError::invalid_request("local preflight")),
        lark_codex_bridge::outbox::DeliveryDecision {
            applied: AppliedCertainty::DefinitelyNotApplied,
            retryability: Retryability::Permanent,
        }
    );
}

// ---------------------------------------------------------------------------
// A5: DurableReplySink adapter.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn finalize_enqueues_the_final_row() {
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = OutboxReplySink::new(store.clone());

    sink.finalize(completed_finalization(1, "user@example.com"))
        .await
        .expect("finalize");

    let depth = store.outbox_depth().await.expect("depth");
    assert_eq!(depth.pending, 1);
    let row = store.outbox_row(1).await.expect("row").expect("exists");
    assert_eq!(row.idempotency_key, "1:final");
    assert_eq!(row.kind, "final");
    let decoded = OutboxOperation::decode(&row.payload_json).expect("decode");
    match decoded {
        OutboxOperation::ReplyMarkdownPost {
            message_id,
            markdown,
            ..
        } => {
            assert_eq!(message_id, "om_parent");
            assert_eq!(markdown, "user[at]example.com");
        }
        _ => panic!("expected a Markdown-post final"),
    }
}

#[tokio::test]
async fn standalone_final_is_delivered_as_a_markdown_post() {
    let server = StubServer::start(token_plus(|_| ok_message("om_final"))).await;
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = OutboxReplySink::new(store.clone());
    sink.finalize(completed_finalization(1, "# Result\n\n- done"))
        .await
        .expect("finalize");

    let (_, receiver) = watch::channel(TransportState::Connected);
    let pump = OutboxPump::spawn(store.clone(), api_for(&server), receiver, fast_config());
    wait_for_state(&store, 1, OutboxState::Sent).await;

    let requests = reply_requests(&server);
    assert_eq!(requests.len(), 1);
    assert_eq!(reply_markdown(&requests[0]), "**Result**\n\n- done");
    pump.shutdown().await;
}

#[tokio::test]
async fn finalize_is_idempotent_across_retries() {
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = OutboxReplySink::new(store.clone());

    // Two structurally identical finalizations (as a retry would produce):
    // the deterministic idempotency key must collapse them into one row.
    sink.finalize(completed_finalization(1, "the answer"))
        .await
        .expect("first finalize");
    sink.finalize(completed_finalization(1, "the answer"))
        .await
        .expect("second finalize");

    let depth = store.outbox_depth().await.expect("depth");
    assert_eq!(depth.pending, 1, "a re-enqueue must reuse the same row");
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "the lifecycle assertion keeps create, update, and finalization evidence together"
)]
async fn progress_cards_are_created_updated_and_finalized_through_the_outbox() {
    let server = StubServer::start(token_plus(|request| {
        if request.path.ends_with("/reply") {
            ok_message("om_progress")
        } else {
            StubResponse::json(200, r#"{"code":0,"data":{}}"#)
        }
    }))
    .await;
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = OutboxReplySink::new(store.clone());
    let progress_source = source("evt_1", "om_parent");

    sink.progress(TurnProgress {
        turn_row_id: 9,
        scope_key: "im:oc_chat".to_owned(),
        source: progress_source.clone(),
        sequence: 0,
        text: "working\n```rust\nlet x = 1;".to_owned(),
    })
    .await
    .expect("first progress");
    sink.progress(TurnProgress {
        turn_row_id: 9,
        scope_key: "im:oc_chat".to_owned(),
        source: progress_source,
        sequence: 1,
        text: "working\n```rust\nlet x = 1;\nlet y = 2;".to_owned(),
    })
    .await
    .expect("progress update");
    sink.finalize_projected(
        completed_finalization(9, "ignored final-only projection"),
        ProjectedReply::ProgressFinal {
            text: "working\n```rust\nlet x = 1;\nlet y = 2;\n```".to_owned(),
        },
    )
    .await
    .expect("progress finalization");

    let first = store.outbox_row(1).await.expect("row").expect("first");
    let second = store.outbox_row(2).await.expect("row").expect("second");
    let third = store.outbox_row(3).await.expect("row").expect("third");
    assert_eq!(first.idempotency_key, "9:progress");
    assert_eq!(second.idempotency_key, "9:progress:1");
    assert_eq!(third.idempotency_key, "9:progress:final");
    assert!(matches!(
        OutboxOperation::decode(&first.payload_json).expect("decode first"),
        OutboxOperation::ReplyProgressCard { .. }
    ));
    assert!(matches!(
        OutboxOperation::decode(&second.payload_json).expect("decode second"),
        OutboxOperation::UpdateProgressCard { .. }
    ));
    assert!(matches!(
        OutboxOperation::decode(&third.payload_json).expect("decode third"),
        OutboxOperation::FinalizeProgressCard { .. }
    ));

    let (_, rx) = watch::channel(TransportState::Connected);
    let pump = OutboxPump::spawn(store.clone(), api_for(&server), rx, fast_config());
    wait_for_state(&store, third.id, OutboxState::Sent).await;

    let requests = reply_requests(&server);
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, REPLY_PATH);
    assert_eq!(requests[1].method, "PATCH");
    assert_eq!(requests[1].path, "/open-apis/im/v1/messages/om_progress");
    assert_eq!(requests[2].method, "PATCH");
    assert_eq!(requests[2].path, "/open-apis/im/v1/messages/om_progress");
    assert_eq!(
        card_markdown(&requests[0]),
        "working\n```rust\nlet x = 1;\n```"
    );
    assert_eq!(
        card_markdown(&requests[1]),
        "working\n```rust\nlet x = 1;\nlet y = 2;\n```"
    );
    assert_eq!(
        card_markdown(&requests[2]),
        "working\n```rust\nlet x = 1;\nlet y = 2;\n```"
    );
    let initial_card = serde_json::to_string(&serde_json::json!({
        "schema": "2.0",
        "body": {"elements": [{
            "tag": "markdown",
            "content": "working\n```rust\nlet x = 1;\n```",
        }]},
    }))
    .expect("initial Card2");
    let final_card = serde_json::to_string(&serde_json::json!({
        "schema": "2.0",
        "body": {"elements": [{
            "tag": "markdown",
            "content": "working\n```rust\nlet x = 1;\nlet y = 2;\n```",
        }]},
    }))
    .expect("final Card2");
    let create_body: serde_json::Value =
        serde_json::from_slice(&requests[0].body).expect("Card2 create body");
    let update_body: serde_json::Value =
        serde_json::from_slice(&requests[1].body).expect("Card2 update body");
    let final_body: serde_json::Value =
        serde_json::from_slice(&requests[2].body).expect("Card2 final body");
    assert_eq!(
        create_body,
        serde_json::json!({"msg_type": "interactive", "content": initial_card})
    );
    assert_eq!(update_body, serde_json::json!({"content": final_card}));
    assert_eq!(final_body, serde_json::json!({"content": final_card}));

    pump.shutdown().await;
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one integration test covers the bounded projection across both card versions"
)]
async fn card2_projection_and_element_cap_cover_v2_and_v1_create_update_final_replay() {
    let server = StubServer::start(token_plus(|request| {
        if request.method == "POST" {
            ok_message("om_progress")
        } else {
            StubResponse::json(200, r#"{"code":0,"data":{}}"#)
        }
    }))
    .await;
    let store = StoreHandle::open_in_memory().await.expect("store");
    let raw = format!(
        "```text\n{}\n```\n&lt;at id=\"ou_encoded\"&gt;hidden&lt;/at&gt;",
        "界\"\\\n".repeat(8_000)
    );

    let sink = OutboxReplySink::new(store.clone());
    sink.progress(TurnProgress {
        turn_row_id: 201,
        scope_key: "im:oc_chat".to_owned(),
        source: source("evt_1", "om_parent"),
        sequence: 0,
        text: raw.clone(),
    })
    .await
    .expect("v2 create");
    sink.progress(TurnProgress {
        turn_row_id: 201,
        scope_key: "im:oc_chat".to_owned(),
        source: source("evt_1", "om_parent"),
        sequence: 1,
        text: raw.clone(),
    })
    .await
    .expect("v2 update");
    sink.finalize_projected(
        completed_finalization(201, "ignored"),
        ProjectedReply::ProgressFinal { text: raw.clone() },
    )
    .await
    .expect("v2 final");

    for (key, kind, payload) in [
        (
            "202:progress",
            "progress",
            serde_json::json!({
                "version": 1,
                "op": "reply_progress_card",
                "message_id": "om_parent",
                "text": raw.clone(),
            }),
        ),
        (
            "202:progress:1",
            "progress",
            serde_json::json!({
                "version": 1,
                "op": "update_progress_card",
                "anchor_key": "202:progress",
                "text": raw.clone(),
            }),
        ),
        (
            "202:progress:final",
            "final",
            serde_json::json!({
                "version": 1,
                "op": "finalize_progress_card",
                "message_id": "om_parent",
                "anchor_key": "202:progress",
                "text": raw.clone(),
            }),
        ),
    ] {
        store
            .enqueue_outbox(NewOutboxRow {
                idempotency_key: key.to_owned(),
                scope_key: "im:oc_chat".to_owned(),
                kind: kind.to_owned(),
                payload_json: serde_json::to_string(&payload).expect("legacy payload"),
                next_retry_ms: 0,
            })
            .await
            .expect("enqueue legacy replay");
    }

    let (_, rx) = watch::channel(TransportState::Connected);
    let pump = OutboxPump::spawn(store.clone(), api_for(&server), rx, fast_config());
    wait_for_state(&store, 6, OutboxState::Sent).await;
    let requests = reply_requests(&server);
    assert_eq!(requests.len(), 6);
    assert_eq!(
        requests
            .iter()
            .map(|request| request.method.as_str())
            .collect::<Vec<_>>(),
        ["POST", "PATCH", "PATCH", "POST", "PATCH", "PATCH"]
    );
    for request in &requests {
        let markdown = card_markdown(request);
        assert!(markdown.ends_with("…[truncated]"));
        assert!(!markdown.contains("<at"));
        assert!(!markdown.contains("ou_encoded"));
        assert!(markdown.len() <= LARK_CARD_MARKDOWN_ELEMENT_MAX_BYTES);
        assert!(card_markdown_element_wire_len(&markdown) <= LARK_CARD_MARKDOWN_ELEMENT_MAX_BYTES);
        assert!(card_fences_are_balanced(&markdown));
    }
    pump.shutdown().await;
}

#[tokio::test]
async fn permanent_final_patch_failure_persists_one_deterministic_post_fallback() {
    let server = StubServer::start(token_plus(|request| {
        if request.method == "PATCH" {
            StubResponse::json(200, r#"{"code":230002,"msg":"invalid card format"}"#)
        } else {
            let body: serde_json::Value =
                serde_json::from_slice(&request.body).expect("reply body");
            if body["msg_type"] == "interactive" {
                ok_message("om_progress")
            } else {
                ok_message("om_fallback")
            }
        }
    }))
    .await;
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = OutboxReplySink::new(store.clone());
    sink.progress(TurnProgress {
        turn_row_id: 91,
        scope_key: "im:oc_chat".to_owned(),
        source: source("evt_1", "om_parent"),
        sequence: 0,
        text: "working".to_owned(),
    })
    .await
    .expect("progress");
    let finalization = completed_finalization(91, "ignored");
    let projected = ProjectedReply::ProgressFinal {
        text: "# Finished\n<at id=\"ou_bad\">safe</at>".to_owned(),
    };
    sink.finalize_projected(finalization.clone(), projected.clone())
        .await
        .expect("finalize");

    let (_, rx) = watch::channel(TransportState::Connected);
    let pump = OutboxPump::spawn(store.clone(), api_for(&server), rx, fast_config());
    let row = wait_for_state(&store, 2, OutboxState::Sent).await;
    assert_eq!(row.idempotency_key, "91:progress:final");
    assert_eq!(row.receipt_message_id.as_deref(), Some("om_fallback"));
    assert!(matches!(
        OutboxOperation::decode(&row.payload_json).expect("persisted fallback"),
        OutboxOperation::ReplyMarkdownPost { markdown, .. }
            if markdown == "**Finished**\nsafe"
    ));
    let requests = reply_requests(&server);
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[1].method, "PATCH");
    assert_eq!(requests[2].method, "POST");
    assert_eq!(reply_markdown(&requests[2]), "**Finished**\nsafe");

    sink.finalize_projected(finalization, projected)
        .await
        .expect("idempotent replay");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        reply_requests(&server).len(),
        3,
        "fallback is never duplicated"
    );
    pump.shutdown().await;
}

#[tokio::test]
async fn exhausted_transient_final_patch_falls_back_only_after_attempt_cap() {
    let server = StubServer::start(token_plus(|request| {
        if request.method == "PATCH" {
            StubResponse::json(200, r#"{"code":99991400,"msg":"rate limited"}"#)
        } else {
            let body: serde_json::Value =
                serde_json::from_slice(&request.body).expect("reply body");
            if body["msg_type"] == "interactive" {
                ok_message("om_progress")
            } else {
                ok_message("om_after_exhaustion")
            }
        }
    }))
    .await;
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = OutboxReplySink::new(store.clone());
    sink.progress(TurnProgress {
        turn_row_id: 92,
        scope_key: "im:oc_chat".to_owned(),
        source: source("evt_1", "om_parent"),
        sequence: 0,
        text: "working".to_owned(),
    })
    .await
    .expect("progress");
    sink.finalize_projected(
        completed_finalization(92, "ignored"),
        ProjectedReply::ProgressFinal {
            text: "complete".to_owned(),
        },
    )
    .await
    .expect("finalize");

    let (_, rx) = watch::channel(TransportState::Connected);
    let pump = OutboxPump::spawn(store.clone(), api_for(&server), rx, fast_config());
    let row = wait_for_state(&store, 2, OutboxState::Sent).await;
    assert_eq!(
        row.receipt_message_id.as_deref(),
        Some("om_after_exhaustion")
    );
    let requests = reply_requests(&server);
    let patches = requests
        .iter()
        .filter(|request| request.method == "PATCH")
        .count();
    assert_eq!(patches, usize::try_from(STORE_OUTBOX_MAX_ATTEMPTS).unwrap());
    assert_eq!(requests.last().expect("fallback").method, "POST");
    pump.shutdown().await;
}

#[tokio::test]
async fn malformed_final_patch_response_is_uncertain_and_never_falls_back() {
    let server = StubServer::start(token_plus(|request| {
        if request.method == "PATCH" {
            StubResponse::json(200, "{")
        } else {
            ok_message("om_progress")
        }
    }))
    .await;
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = OutboxReplySink::new(store.clone());
    sink.progress(TurnProgress {
        turn_row_id: 93,
        scope_key: "im:oc_chat".to_owned(),
        source: source("evt_1", "om_parent"),
        sequence: 0,
        text: "working".to_owned(),
    })
    .await
    .expect("progress");
    sink.finalize_projected(
        completed_finalization(93, "ignored"),
        ProjectedReply::ProgressFinal {
            text: "complete".to_owned(),
        },
    )
    .await
    .expect("finalize");

    let (_, rx) = watch::channel(TransportState::Connected);
    let pump = OutboxPump::spawn(store.clone(), api_for(&server), rx, fast_config());
    wait_for_state(&store, 2, OutboxState::UncertainDelivery).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let requests = reply_requests(&server);
    assert_eq!(requests.len(), 2, "uncertain PATCH must not trigger a post");
    assert_eq!(requests[1].method, "PATCH");
    pump.shutdown().await;
}

#[tokio::test]
async fn applied_then_5xx_final_patch_exhaustion_is_uncertain_across_restart() {
    let patches = Arc::new(AtomicUsize::new(0));
    let server = StubServer::start(token_plus({
        let patches = Arc::clone(&patches);
        move |request| {
            if request.method == "PATCH" {
                let attempt = patches.fetch_add(1, Ordering::SeqCst);
                // The stub models a server that applied the idempotent update
                // and then lost the successful response behind a 500/502.
                let status = if attempt % 2 == 0 { 500 } else { 502 };
                StubResponse::json(status, r#"{"code":0,"data":{}}"#)
            } else {
                ok_message("om_progress")
            }
        }
    }))
    .await;
    let temp = tempdir().expect("tempdir");
    let db = temp.path().join("patch-restart.sqlite");
    let store = StoreHandle::open(&db).await.expect("store");
    let sink = OutboxReplySink::new(store.clone());
    sink.progress(TurnProgress {
        turn_row_id: 94,
        scope_key: "im:oc_chat".to_owned(),
        source: source("evt_1", "om_parent"),
        sequence: 0,
        text: "working".to_owned(),
    })
    .await
    .expect("progress");
    sink.finalize_projected(
        completed_finalization(94, "ignored"),
        ProjectedReply::ProgressFinal {
            text: "complete".to_owned(),
        },
    )
    .await
    .expect("finalize");

    let (_, rx) = watch::channel(TransportState::Connected);
    let pump = OutboxPump::spawn(store.clone(), api_for(&server), rx, fast_config());
    let row = wait_for_state(&store, 2, OutboxState::UncertainDelivery).await;
    assert_eq!(row.attempts, STORE_OUTBOX_MAX_ATTEMPTS);
    let requests = reply_requests(&server);
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method == "PATCH")
            .count(),
        usize::try_from(STORE_OUTBOX_MAX_ATTEMPTS).unwrap()
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method == "POST")
            .count(),
        1,
        "only the initial card create is a POST; no fallback may follow 5xx"
    );
    pump.shutdown().await;
    drop(sink);
    store.shutdown().await.expect("close store");

    let before_restart = reply_requests(&server).len();
    let reopened = StoreHandle::open(&db).await.expect("reopen store");
    let (_, rx) = watch::channel(TransportState::Connected);
    let restarted = OutboxPump::spawn(reopened.clone(), api_for(&server), rx, fast_config());
    tokio::time::sleep(Duration::from_millis(75)).await;
    assert_eq!(
        reply_requests(&server).len(),
        before_restart,
        "uncertain final PATCH is never replayed or converted after restart"
    );
    assert_eq!(
        reopened
            .outbox_row(2)
            .await
            .expect("row")
            .expect("exists")
            .state,
        OutboxState::UncertainDelivery
    );
    restarted.shutdown().await;
    reopened.shutdown().await.expect("close reopened store");
}

#[tokio::test]
async fn applied_then_5xx_progress_update_retries_idempotently_then_stays_uncertain() {
    let server = StubServer::start(token_plus(|request| {
        if request.method == "PATCH" {
            StubResponse::json(502, r#"{"code":0,"data":{}}"#)
        } else {
            ok_message("om_progress")
        }
    }))
    .await;
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = OutboxReplySink::new(store.clone());
    let progress_source = source("evt_1", "om_parent");
    sink.progress(TurnProgress {
        turn_row_id: 95,
        scope_key: "im:oc_chat".to_owned(),
        source: progress_source.clone(),
        sequence: 0,
        text: "working".to_owned(),
    })
    .await
    .expect("create");
    sink.progress(TurnProgress {
        turn_row_id: 95,
        scope_key: "im:oc_chat".to_owned(),
        source: progress_source,
        sequence: 1,
        text: "still working".to_owned(),
    })
    .await
    .expect("update");

    let (_, rx) = watch::channel(TransportState::Connected);
    let pump = OutboxPump::spawn(store.clone(), api_for(&server), rx, fast_config());
    let row = wait_for_state(&store, 2, OutboxState::UncertainDelivery).await;
    assert_eq!(row.attempts, STORE_OUTBOX_MAX_ATTEMPTS);
    let requests = reply_requests(&server);
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method == "PATCH")
            .count(),
        usize::try_from(STORE_OUTBOX_MAX_ATTEMPTS).unwrap()
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.method == "POST")
            .count(),
        1
    );
    pump.shutdown().await;
}

#[tokio::test]
async fn same_scope_cross_turn_progress_anchor_is_rejected_before_patch() {
    let server = StubServer::start(token_plus(|_| ok_message("om_progress"))).await;
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = OutboxReplySink::new(store.clone());
    sink.progress(TurnProgress {
        turn_row_id: 11,
        scope_key: "im:oc_chat".to_owned(),
        source: source("evt_1", "om_parent"),
        sequence: 0,
        text: "working".to_owned(),
    })
    .await
    .expect("anchor");

    let bad_update = OutboxOperation::UpdateProgressCard {
        anchor_key: "11:progress".to_owned(),
        text: "wrong turn update".to_owned(),
    };
    let bad_update_id = match store
        .enqueue_outbox(NewOutboxRow {
            idempotency_key: "12:progress:1".to_owned(),
            scope_key: "im:oc_chat".to_owned(),
            kind: "progress".to_owned(),
            payload_json: bad_update.encode().expect("encode update"),
            next_retry_ms: 0,
        })
        .await
        .expect("enqueue update")
    {
        OutboxEnqueue::New(row) | OutboxEnqueue::Duplicate(row) => row.id,
    };
    let bad_final = OutboxOperation::FinalizeProgressCard {
        anchor_key: "11:progress".to_owned(),
        message_id: "om_parent".to_owned(),
        thread_id: None,
        text: "wrong turn final".to_owned(),
        fallback_markdown: "wrong turn final".to_owned(),
    };
    let bad_final_id = match store
        .enqueue_outbox(NewOutboxRow {
            idempotency_key: "12:progress:final".to_owned(),
            scope_key: "im:oc_chat".to_owned(),
            kind: "final".to_owned(),
            payload_json: bad_final.encode().expect("encode final"),
            next_retry_ms: 0,
        })
        .await
        .expect("enqueue final")
    {
        OutboxEnqueue::New(row) | OutboxEnqueue::Duplicate(row) => row.id,
    };

    let (_, rx) = watch::channel(TransportState::Connected);
    let pump = OutboxPump::spawn(store.clone(), api_for(&server), rx, fast_config());
    wait_for_state(&store, 1, OutboxState::Sent).await;
    wait_for_state(&store, bad_update_id, OutboxState::Failed).await;
    wait_for_state(&store, bad_final_id, OutboxState::Failed).await;

    let requests = reply_requests(&server);
    assert_eq!(requests.len(), 1, "only the legitimate anchor is sent");
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, REPLY_PATH);
    pump.shutdown().await;
}

#[tokio::test]
async fn failed_initial_progress_card_falls_back_to_a_standalone_final() {
    let server = StubServer::start(token_plus(|request| {
        let body: serde_json::Value = serde_json::from_slice(&request.body).expect("message body");
        if body["msg_type"] == "interactive" {
            StubResponse::json(200, r#"{"code":99991661,"msg":"invalid token"}"#)
        } else {
            ok_message("om_final_fallback")
        }
    }))
    .await;
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = OutboxReplySink::new(store.clone());
    sink.progress(TurnProgress {
        turn_row_id: 10,
        scope_key: "im:oc_chat".to_owned(),
        source: source("evt_1", "om_parent"),
        sequence: 0,
        text: "working".to_owned(),
    })
    .await
    .expect("progress");
    sink.finalize_projected(
        completed_finalization(10, "ignored"),
        ProjectedReply::ProgressFinal {
            text: "complete fallback".to_owned(),
        },
    )
    .await
    .expect("finalize");

    let (_, rx) = watch::channel(TransportState::Connected);
    let pump = OutboxPump::spawn(store.clone(), api_for(&server), rx, fast_config());
    let first = wait_for_state(&store, 1, OutboxState::Failed).await;
    let final_row = wait_for_state(&store, 2, OutboxState::Sent).await;
    assert!(first.receipt_message_id.is_none());
    assert_eq!(
        final_row.receipt_message_id.as_deref(),
        Some("om_final_fallback")
    );
    let requests = reply_requests(&server);
    assert_eq!(
        reply_markdown(requests.last().expect("fallback request")),
        "complete fallback"
    );
    pump.shutdown().await;
}

#[tokio::test]
async fn uncertain_finalization_enqueues_a_deterministic_notice() {
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = OutboxReplySink::new(store.clone());
    let turn = TurnFinalization {
        turn_row_id: 7,
        scope_key: "im:oc_chat".to_owned(),
        sources: vec![source("evt_1", "om_parent")],
        resolution: TurnResolution::Uncertain,
        outcome: None,
    };

    sink.finalize(turn).await.expect("finalize");

    let row = store.outbox_row(1).await.expect("row").expect("exists");
    assert_eq!(row.idempotency_key, "7:notice");
    assert_eq!(row.kind, "notice");
}

#[tokio::test]
async fn rejection_notice_is_deterministic() {
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = OutboxReplySink::new(store.clone());
    let event = inbound_event();
    let key = test_inbound_key(&event);

    let notice = sink
        .rejection_notice(&key, &event, InboundRejectionKind::Policy)
        .expect("notice");
    assert_eq!(
        notice.idempotency_key,
        key.rejection_outbox_idempotency_key(InboundRejectionKind::Policy)
    );
    assert_eq!(notice.kind, "notice");
    let decoded = OutboxOperation::decode(&notice.payload_json).expect("decode");
    match decoded {
        OutboxOperation::ReplyText {
            message_id, text, ..
        } => {
            assert_eq!(message_id, "om_parent");
            assert!(!text.is_empty());
        }
        _ => panic!("expected a text notice"),
    }
}

#[tokio::test]
async fn control_reply_is_bounded_and_deterministic() {
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = OutboxReplySink::new(store.clone());
    let event = inbound_event();
    let key = test_inbound_key(&event);

    let reply = sink
        .control_reply(&key, &event, "ownership remains explicit")
        .expect("control reply");
    assert_eq!(reply.idempotency_key, key.control_outbox_idempotency_key());
    assert_eq!(reply.kind, "control");
    let decoded = OutboxOperation::decode(&reply.payload_json).expect("decode");
    assert!(matches!(
        decoded,
        OutboxOperation::ReplyText { message_id, text, .. }
            if message_id == "om_parent" && text == "ownership remains explicit"
    ));
    assert!(matches!(
        sink.control_reply(&key, &event, ""),
        Err(ReplySinkError::Invariant)
    ));
    assert!(matches!(
        sink.control_reply(&key, &event, &"x".repeat(4_001)),
        Err(ReplySinkError::Invariant)
    ));
}

#[tokio::test]
async fn final_rows_reply_only_to_the_last_source_bounded_by_parts() {
    // The reference implementation (feishu-claude-code-bridge @ e5d3ce5)
    // replies to the last message of a debounced batch (channel.ts
    // `replyTo: lastMsg.messageId`). One turn therefore emits one terminal
    // answer — only the last source, split into at most the part count —
    // instead of one reply per source.
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = OutboxReplySink::new(store.clone());

    // 7 * 4000 + 1 chars split into exactly 8 parts (max_splits).
    let text = "x".repeat(7 * 4000 + 1);
    let turn = TurnFinalization {
        turn_row_id: 42,
        scope_key: "im:oc_chat".to_owned(),
        sources: many_sources(64),
        resolution: TurnResolution::Completed,
        outcome: Some(TurnOutcome {
            thread_id: ThreadId::from("thread_1"),
            turn_id: TurnId::from("turn_1"),
            status: TurnStatus::Completed,
            error: None,
            completed_items: vec![agent(&text, Some(MessagePhase::FinalAnswer))],
            token_usage: None,
        }),
    };

    sink.finalize(turn).await.expect("finalize");

    let depth = store.outbox_depth().await.expect("depth");
    assert_eq!(
        depth.pending, 8,
        "only the parts of a single final answer are enqueued, never 64 * 8"
    );

    let first = store.outbox_row(1).await.expect("row").expect("exists");
    assert_eq!(first.idempotency_key, "42:final");
    assert_eq!(first.kind, "final");
    let decoded = OutboxOperation::decode(&first.payload_json).expect("decode");
    match decoded {
        OutboxOperation::ReplyMarkdownPost { message_id, .. } => {
            assert_eq!(message_id, "om_63", "the reply targets the last source");
        }
        _ => panic!("expected a Markdown-post final"),
    }

    let last = store.outbox_row(8).await.expect("row").expect("exists");
    assert_eq!(last.idempotency_key, "42:final:7");
}

#[tokio::test]
async fn empty_sources_produce_no_rows() {
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = OutboxReplySink::new(store.clone());

    let completed = TurnFinalization {
        turn_row_id: 1,
        scope_key: "im:oc_chat".to_owned(),
        sources: vec![],
        resolution: TurnResolution::Completed,
        outcome: Some(TurnOutcome {
            thread_id: ThreadId::from("thread_1"),
            turn_id: TurnId::from("turn_1"),
            status: TurnStatus::Completed,
            error: None,
            completed_items: vec![agent("the answer", Some(MessagePhase::FinalAnswer))],
            token_usage: None,
        }),
    };
    sink.finalize(completed).await.expect("finalize");
    assert_eq!(store.outbox_depth().await.expect("depth").pending, 0);

    let uncertain = TurnFinalization {
        turn_row_id: 2,
        scope_key: "im:oc_chat".to_owned(),
        sources: vec![],
        resolution: TurnResolution::Uncertain,
        outcome: None,
    };
    sink.finalize(uncertain).await.expect("finalize");
    assert_eq!(store.outbox_depth().await.expect("depth").pending, 0);
}

#[tokio::test]
async fn notice_rows_reply_only_to_the_last_source() {
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = OutboxReplySink::new(store.clone());
    let turn = TurnFinalization {
        turn_row_id: 77,
        scope_key: "im:oc_chat".to_owned(),
        sources: many_sources(64),
        resolution: TurnResolution::Uncertain,
        outcome: None,
    };

    sink.finalize(turn).await.expect("finalize");

    let depth = store.outbox_depth().await.expect("depth");
    assert_eq!(depth.pending, 1, "exactly one notice per turn");
    let row = store.outbox_row(1).await.expect("row").expect("exists");
    assert_eq!(row.idempotency_key, "77:notice");
    assert_eq!(row.kind, "notice");
    let decoded = OutboxOperation::decode(&row.payload_json).expect("decode");
    match decoded {
        OutboxOperation::ReplyText { message_id, .. } => {
            assert_eq!(message_id, "om_63", "the notice targets the last source");
        }
        _ => panic!("expected a text notice"),
    }
}

// ---------------------------------------------------------------------------
// A4: the pump.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pump_sends_and_records_the_receipt() {
    let server = StubServer::start(token_plus(|_| ok_message("om_delivered"))).await;
    let api = api_for(&server);
    let store = StoreHandle::open_in_memory().await.expect("store");
    let id = enqueue_reply(&store, "1:final:evt_1", "om_parent", None, "the answer").await;
    let (_, rx) = watch::channel(TransportState::Connected);
    let pump = OutboxPump::spawn(store.clone(), api, rx, fast_config());

    let row = wait_for_state(&store, id, OutboxState::Sent).await;
    assert_eq!(row.receipt_message_id.as_deref(), Some("om_delivered"));

    let requests = reply_requests(&server);
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path, REPLY_PATH);

    pump.shutdown().await;
}

#[tokio::test]
async fn retryable_failure_backs_off_and_eventually_succeeds() {
    let replies = Arc::new(AtomicUsize::new(0));
    let server = StubServer::start(token_plus({
        let replies = Arc::clone(&replies);
        move |_| {
            if replies.fetch_add(1, Ordering::SeqCst) == 0 {
                StubResponse::json(200, r#"{"code":99991400,"msg":"rate limited"}"#)
            } else {
                ok_message("om_retried")
            }
        }
    }))
    .await;
    let api = api_for(&server);
    let store = StoreHandle::open_in_memory().await.expect("store");
    let id = enqueue_reply(&store, "1:final:evt_1", "om_parent", None, "the answer").await;
    let (_, rx) = watch::channel(TransportState::Connected);
    let pump = OutboxPump::spawn(store.clone(), api, rx, fast_config());

    let row = wait_for_state(&store, id, OutboxState::Sent).await;
    assert_eq!(row.receipt_message_id.as_deref(), Some("om_retried"));
    assert_eq!(
        reply_requests(&server).len(),
        2,
        "exactly one retry then success"
    );

    pump.shutdown().await;
}

#[tokio::test]
async fn explicit_http_4xx_validation_rejection_is_permanent_not_uncertain() {
    // A definitive validation rejection was not applied, but sending the same
    // invalid body again cannot repair it.
    let server = StubServer::start(token_plus(|_| {
        StubResponse::json(400, r#"{"code":400,"msg":"bad request"}"#)
    }))
    .await;
    let api = api_for(&server);
    let store = StoreHandle::open_in_memory().await.expect("store");
    let id = enqueue_reply(&store, "1:final:evt_1", "om_parent", None, "the answer").await;
    let (_, rx) = watch::channel(TransportState::Connected);
    let pump = OutboxPump::spawn(store.clone(), api, rx, fast_config());

    let row = wait_for_state(&store, id, OutboxState::Failed).await;
    assert_eq!(row.state, OutboxState::Failed);
    assert_eq!(row.attempts, 0);
    assert_eq!(
        reply_requests(&server).len(),
        1,
        "a permanent validation rejection must not be retried"
    );

    // A failed row is terminal: it is never re-claimed.
    let requests_before = reply_requests(&server).len();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(reply_requests(&server).len(), requests_before);
    assert_eq!(
        store
            .outbox_row(id)
            .await
            .expect("row")
            .expect("exists")
            .state,
        OutboxState::Failed
    );

    pump.shutdown().await;
}

#[tokio::test]
async fn applied_then_http_500_post_is_uncertain_and_never_replayed_after_restart() {
    let server = StubServer::start(token_plus(|_| {
        // Model a non-idempotent reply that was committed before an upstream
        // proxy replaced the success response with HTTP 500.
        StubResponse::json(500, r#"{"code":0,"data":{}}"#)
    }))
    .await;
    let temp = tempdir().expect("tempdir");
    let db = temp.path().join("post-restart.sqlite");
    let store = StoreHandle::open(&db).await.expect("store");
    let id = enqueue_reply(&store, "1:final:evt_1", "om_parent", None, "the answer").await;
    let (_, rx) = watch::channel(TransportState::Connected);
    let pump = OutboxPump::spawn(store.clone(), api_for(&server), rx, fast_config());

    let row = wait_for_state(&store, id, OutboxState::UncertainDelivery).await;
    assert_eq!(row.attempts, 1);
    assert_eq!(
        reply_requests(&server).len(),
        1,
        "a POST that receives 5xx must not be blindly retried"
    );
    pump.shutdown().await;
    store.shutdown().await.expect("close store");

    let reopened = StoreHandle::open(&db).await.expect("reopen store");
    let (_, rx) = watch::channel(TransportState::Connected);
    let restarted = OutboxPump::spawn(reopened.clone(), api_for(&server), rx, fast_config());
    tokio::time::sleep(Duration::from_millis(75)).await;
    assert_eq!(reply_requests(&server).len(), 1);
    assert_eq!(
        reopened
            .outbox_row(id)
            .await
            .expect("row")
            .expect("exists")
            .state,
        OutboxState::UncertainDelivery
    );
    restarted.shutdown().await;
    reopened.shutdown().await.expect("close reopened store");
}

#[tokio::test]
async fn retryable_exhaustion_marks_row_failed_and_stops_claiming() {
    // A persistently retryable send failure must exhaust the bounded attempt
    // budget and terminally fail the row, never re-claiming it afterwards.
    let server = StubServer::start(token_plus(|_| {
        StubResponse::json(200, r#"{"code":99991400,"msg":"rate limited"}"#)
    }))
    .await;
    let api = api_for(&server);
    let store = StoreHandle::open_in_memory().await.expect("store");
    let id = enqueue_reply(&store, "1:final:evt_1", "om_parent", None, "the answer").await;
    let (_, rx) = watch::channel(TransportState::Connected);
    let pump = OutboxPump::spawn(store.clone(), api, rx, fast_config());

    let row = wait_for_state(&store, id, OutboxState::Failed).await;
    assert_eq!(row.state, OutboxState::Failed);
    assert_eq!(row.attempts, STORE_OUTBOX_MAX_ATTEMPTS);
    assert_eq!(
        reply_requests(&server).len(),
        usize::try_from(STORE_OUTBOX_MAX_ATTEMPTS).unwrap(),
        "exactly the attempt cap of send attempts before terminal failure"
    );

    let requests_before = reply_requests(&server).len();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(reply_requests(&server).len(), requests_before);
    assert_eq!(
        store
            .outbox_row(id)
            .await
            .expect("row")
            .expect("exists")
            .state,
        OutboxState::Failed
    );

    pump.shutdown().await;
}

#[tokio::test]
async fn permanent_auth_is_terminal_failed_without_resend() {
    let server = StubServer::start(token_plus(|_| {
        StubResponse::json(200, r#"{"code":99991661,"msg":"app_ticket invalid"}"#)
    }))
    .await;
    let api = api_for(&server);
    let store = StoreHandle::open_in_memory().await.expect("store");
    let id = enqueue_reply(&store, "1:final:evt_1", "om_parent", None, "the answer").await;
    let (_, rx) = watch::channel(TransportState::Connected);
    let pump = OutboxPump::spawn(store.clone(), api, rx, fast_config());

    let row = wait_for_state(&store, id, OutboxState::Failed).await;
    assert_eq!(row.state, OutboxState::Failed);
    assert_eq!(
        reply_requests(&server).len(),
        1,
        "a permanent failure must not be resent"
    );

    pump.shutdown().await;
}

#[tokio::test]
async fn undeliverable_payload_is_terminally_failed_without_send() {
    // A corrupt payload (unknown operation) must be classified as an
    // undeliverable permanent failure before any send: the row goes terminal
    // `failed` in one pass, without a send attempt, instead of being stranded
    // in `sending`.
    let server = StubServer::start(token_plus(|_| ok_message("om_unused"))).await;
    let api = api_for(&server);
    let store = StoreHandle::open_in_memory().await.expect("store");
    let id = store
        .enqueue_outbox(NewOutboxRow {
            idempotency_key: "1:final:evt_1".to_owned(),
            scope_key: "im:oc_chat".to_owned(),
            kind: "final".to_owned(),
            payload_json: r#"{"version":1,"op":"send_text","message_id":"m","text":"t"}"#
                .to_owned(),
            next_retry_ms: 0,
        })
        .await
        .expect("enqueue");
    let id = match id {
        OutboxEnqueue::New(row) | OutboxEnqueue::Duplicate(row) => row.id,
    };
    let (_, rx) = watch::channel(TransportState::Connected);
    let pump = OutboxPump::spawn(store.clone(), api, rx, fast_config());

    let row = wait_for_state(&store, id, OutboxState::Failed).await;
    assert_eq!(row.state, OutboxState::Failed);
    assert_eq!(row.attempts, 0, "a corrupt payload is never send-retried");
    assert_eq!(
        reply_requests(&server).len(),
        0,
        "a corrupt payload must fail before any Lark send"
    );

    pump.shutdown().await;
}

#[tokio::test]
async fn empty_message_id_is_not_recorded_as_delivered() {
    // A code-0 response without a usable message_id means the send outcome is
    // unknown: the row becomes uncertain and is never re-sent.
    let server = StubServer::start(token_plus(|_| {
        StubResponse::json(200, r#"{"code":0,"data":{"message_id":""}}"#)
    }))
    .await;
    let api = api_for(&server);
    let store = StoreHandle::open_in_memory().await.expect("store");
    let id = enqueue_reply(&store, "1:final:evt_1", "om_parent", None, "the answer").await;
    let (_, rx) = watch::channel(TransportState::Connected);
    let pump = OutboxPump::spawn(store.clone(), api, rx, fast_config());

    let row = wait_for_state(&store, id, OutboxState::UncertainDelivery).await;
    assert_eq!(row.state, OutboxState::UncertainDelivery);
    assert_eq!(
        reply_requests(&server).len(),
        1,
        "an uncertain delivery is never blindly resent"
    );

    pump.shutdown().await;
}

#[tokio::test]
async fn disconnect_keeps_rows_pending_and_reconnect_delivers_in_order() {
    let server = StubServer::start(token_plus(|_| ok_message("om_after_reconnect"))).await;
    let api = api_for(&server);
    let store = StoreHandle::open_in_memory().await.expect("store");
    let first = enqueue_reply(&store, "1:final:evt_1", "om_parent", None, "first").await;
    let second = enqueue_reply(&store, "2:final:evt_2", "om_parent", None, "second").await;

    let (tx, rx) = watch::channel(TransportState::Connecting { attempt: 1 });
    let pump = OutboxPump::spawn(store.clone(), api, rx, fast_config());

    // Give the pump time to (incorrectly) send while disconnected; it must not.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        reply_requests(&server).len(),
        0,
        "nothing may be sent while disconnected"
    );
    assert_eq!(
        store
            .outbox_row(first)
            .await
            .expect("row")
            .expect("exists")
            .state,
        OutboxState::Pending
    );

    tx.send(TransportState::Connected).expect("reconnect");
    let first_row = wait_for_state(&store, first, OutboxState::Sent).await;
    let second_row = wait_for_state(&store, second, OutboxState::Sent).await;
    assert_eq!(
        first_row.receipt_message_id.as_deref(),
        Some("om_after_reconnect")
    );
    assert_eq!(
        second_row.receipt_message_id.as_deref(),
        Some("om_after_reconnect")
    );
    let requests = reply_requests(&server);
    assert_eq!(requests.len(), 2);
    assert_eq!(
        reply_text(&requests[0]),
        "first",
        "the pump must deliver rows in the deterministic claim order (by id)"
    );
    assert_eq!(
        reply_text(&requests[1]),
        "second",
        "the pump must deliver rows in the deterministic claim order (by id)"
    );

    pump.shutdown().await;
}

#[tokio::test]
async fn startup_recovers_stranded_sending_rows_as_uncertain() {
    let store = StoreHandle::open_in_memory().await.expect("store");
    let id = enqueue_reply(&store, "1:final:evt_1", "om_parent", None, "the answer").await;
    // Simulate a prior process that claimed the row and died mid-send.
    let claimed = store.claim_outbox_batch(now_ms(), 1).await.expect("claim");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, id);

    let server = StubServer::start(token_plus(|_| ok_message("om_unused"))).await;
    let api = api_for(&server);
    let (_, rx) = watch::channel(TransportState::Connected);
    let pump = OutboxPump::spawn(store.clone(), api, rx, fast_config());

    let row = wait_for_state(&store, id, OutboxState::UncertainDelivery).await;
    assert_eq!(row.state, OutboxState::UncertainDelivery);
    assert_eq!(reply_requests(&server).len(), 0);

    pump.shutdown().await;
}

#[tokio::test]
async fn retryable_failure_does_not_reorder_the_batch() {
    let replies = Arc::new(AtomicUsize::new(0));
    let server = StubServer::start(token_plus({
        let replies = Arc::clone(&replies);
        move |request| {
            if request.path == REPLY_PATH && replies.fetch_add(1, Ordering::SeqCst) == 0 {
                StubResponse::json(200, r#"{"code":99991400,"msg":"rate limited"}"#)
            } else {
                ok_message("om_delivered")
            }
        }
    }))
    .await;
    let api = api_for(&server);
    let store = StoreHandle::open_in_memory().await.expect("store");
    let first = enqueue_reply(&store, "1:final:evt_1", "om_parent", None, "first").await;
    let second = enqueue_reply(&store, "2:final:evt_2", "om_parent", None, "second").await;
    let (_, rx) = watch::channel(TransportState::Connected);
    let pump = OutboxPump::spawn(store.clone(), api, rx, fast_config());

    let first_row = wait_for_state(&store, first, OutboxState::Sent).await;
    let second_row = wait_for_state(&store, second, OutboxState::Sent).await;
    assert_eq!(
        first_row.receipt_message_id.as_deref(),
        Some("om_delivered")
    );
    assert_eq!(
        second_row.receipt_message_id.as_deref(),
        Some("om_delivered")
    );

    let requests = reply_requests(&server);
    assert_eq!(
        requests.len(),
        3,
        "part1 fails once, is retried to success, then part2 follows"
    );
    assert_eq!(reply_text(&requests[0]), "first");
    assert_eq!(
        reply_text(&requests[1]),
        "first",
        "the retry of part1 precedes part2"
    );
    assert_eq!(
        reply_text(&requests[2]),
        "second",
        "part2 is never sent before part1 succeeds"
    );

    pump.shutdown().await;
}

#[tokio::test]
async fn retryable_failure_defers_pending_successors_across_poll_cycles() {
    // The first row fails retryably and is parked for a long retry; the two
    // successors stay `pending` (never claimed in the same one-row batch). The
    // cross-batch fix must defer those successors too, so the failed row is
    // retried before any later row is sent.
    let replies = Arc::new(AtomicUsize::new(0));
    let server = StubServer::start(token_plus({
        let replies = Arc::clone(&replies);
        move |_| {
            if replies.fetch_add(1, Ordering::SeqCst) == 0 {
                StubResponse::json(200, r#"{"code":99991400,"msg":"rate limited"}"#)
            } else {
                ok_message("om_delivered")
            }
        }
    }))
    .await;
    let api = api_for(&server);
    let store = StoreHandle::open_in_memory().await.expect("store");
    let first = enqueue_reply(&store, "1:final:evt_1", "om_parent", None, "first").await;
    let second = enqueue_reply(&store, "2:final:evt_2", "om_parent", None, "second").await;
    let third = enqueue_reply(&store, "3:final:evt_3", "om_parent", None, "third").await;
    let (_, rx) = watch::channel(TransportState::Connected);
    let pump = OutboxPump::spawn(store.clone(), api, rx, slow_retry_config());

    let first_row = wait_for_state(&store, first, OutboxState::Sent).await;
    let second_row = wait_for_state(&store, second, OutboxState::Sent).await;
    let third_row = wait_for_state(&store, third, OutboxState::Sent).await;
    assert_eq!(
        first_row.receipt_message_id.as_deref(),
        Some("om_delivered")
    );
    assert_eq!(
        second_row.receipt_message_id.as_deref(),
        Some("om_delivered")
    );
    assert_eq!(
        third_row.receipt_message_id.as_deref(),
        Some("om_delivered")
    );

    let texts: Vec<String> = reply_requests(&server).iter().map(reply_text).collect();
    assert_eq!(
        texts,
        vec![
            "first".to_owned(),
            "first".to_owned(),
            "second".to_owned(),
            "third".to_owned(),
        ],
        "the failed row must be retried before any pending successor is claimed"
    );

    pump.shutdown().await;
}

#[tokio::test]
async fn concurrent_enqueue_after_retry_does_not_overtake_the_failed_row() {
    let replies = Arc::new(AtomicUsize::new(0));
    let server = StubServer::start(token_plus({
        let replies = Arc::clone(&replies);
        move |_| {
            if replies.fetch_add(1, Ordering::SeqCst) == 0 {
                StubResponse::json(200, r#"{"code":99991400,"msg":"rate limited"}"#)
            } else {
                ok_message("om_delivered")
            }
        }
    }))
    .await;
    let api = api_for(&server);
    let store = StoreHandle::open_in_memory().await.expect("store");
    let first = enqueue_reply(&store, "1:final:evt_1", "om_parent", None, "first").await;
    let (_, rx) = watch::channel(TransportState::Connected);
    let pump = OutboxPump::spawn(store.clone(), api, rx, slow_retry_config());

    // The first row fails once and is parked for its retry; the deferral
    // snapshot has already been taken by now.
    let failed = wait_for_attempts(&store, first, 1).await;
    assert_eq!(failed.state, OutboxState::Pending);

    // A row enqueued after the snapshot must inherit the retry watermark and
    // therefore can never be claimed before the failed row.
    let second = enqueue_reply(&store, "2:final:evt_2", "om_parent", None, "second").await;
    let second_row = store
        .outbox_row(second)
        .await
        .expect("row")
        .expect("exists");
    assert!(
        second_row.next_retry_ms >= failed.next_retry_ms,
        "the concurrent successor must be parked no earlier than the failed row ({} >= {})",
        second_row.next_retry_ms,
        failed.next_retry_ms
    );

    let first_row = wait_for_state(&store, first, OutboxState::Sent).await;
    let second_row = wait_for_state(&store, second, OutboxState::Sent).await;
    assert_eq!(
        first_row.receipt_message_id.as_deref(),
        Some("om_delivered")
    );
    assert_eq!(
        second_row.receipt_message_id.as_deref(),
        Some("om_delivered")
    );

    let texts: Vec<String> = reply_requests(&server).iter().map(reply_text).collect();
    assert_eq!(
        texts,
        vec!["first".to_owned(), "first".to_owned(), "second".to_owned(),],
        "the failed row must be retried before the concurrent successor"
    );

    pump.shutdown().await;
}

#[tokio::test]
async fn shutdown_releases_the_claimed_tail_without_counting_attempts() {
    let replies = Arc::new(AtomicUsize::new(0));
    let server = StubServer::start(token_plus({
        let replies = Arc::clone(&replies);
        move |request| {
            if request.path == REPLY_PATH && replies.fetch_add(1, Ordering::SeqCst) == 0 {
                ok_message("om_first").with_delay(Duration::from_millis(800))
            } else {
                ok_message("om_delivered")
            }
        }
    }))
    .await;
    let api = api_for(&server);
    let store = StoreHandle::open_in_memory().await.expect("store");
    let first = enqueue_reply(&store, "1:final:evt_1", "om_parent", None, "first").await;
    let second = enqueue_reply(&store, "2:final:evt_2", "om_parent", None, "second").await;
    let third = enqueue_reply(&store, "3:final:evt_3", "om_parent", None, "third").await;
    let (_, rx) = watch::channel(TransportState::Connected);
    let pump = OutboxPump::spawn(store.clone(), api, rx, fast_config());

    // The whole batch is claimed atomically; wait for the first send to be in
    // flight (delayed) so the tail rows are provably claimed but unsent.
    wait_for_state(&store, second, OutboxState::Sending).await;
    let deadline = Instant::now() + Duration::from_secs(5);
    while reply_requests(&server).is_empty() {
        assert!(
            Instant::now() < deadline,
            "the first send never reached the stub"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(reply_requests(&server).len(), 1);

    let shutdown = tokio::spawn(async move { pump.shutdown().await });
    // Give the shutdown token a moment to cancel before the in-flight send
    // returns, so the tail is observed as claimed and released un-sent.
    tokio::time::sleep(Duration::from_millis(50)).await;
    shutdown.await.expect("shutdown");

    let first_row = store.outbox_row(first).await.expect("row").expect("exists");
    let second_row = store
        .outbox_row(second)
        .await
        .expect("row")
        .expect("exists");
    let third_row = store.outbox_row(third).await.expect("row").expect("exists");
    assert_eq!(
        first_row.state,
        OutboxState::Sent,
        "the in-flight send is completed"
    );
    assert_eq!(
        second_row.state,
        OutboxState::Pending,
        "the tail is re-parked"
    );
    assert_eq!(
        third_row.state,
        OutboxState::Pending,
        "the tail is re-parked"
    );
    assert_eq!(
        second_row.attempts, 0,
        "re-parking must not count a send attempt"
    );
    assert_eq!(
        third_row.attempts, 0,
        "re-parking must not count a send attempt"
    );
    assert_eq!(
        reply_requests(&server).len(),
        1,
        "the tail is never sent after shutdown"
    );
}
