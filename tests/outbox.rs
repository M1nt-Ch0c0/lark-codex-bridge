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
use lark_codex_bridge::outbox::{
    DeliveryClass, OutboxError, OutboxOperation, OutboxPump, OutboxPumpConfig, OutboxReplySink,
    classify_delivery,
};
use lark_codex_bridge::runtime::scope::{DurableReplySink, TurnFinalization, TurnSource};
use lark_codex_bridge::store::{
    InboundRejectionKind, NewOutboxRow, OutboxEnqueue, OutboxState, StoreHandle, TurnResolution,
};
use larkstub::{Handler, RecordedRequest, StubResponse, StubServer};
use secrecy::SecretString;
use serde_json::Map;
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

fn fast_config() -> OutboxPumpConfig {
    OutboxPumpConfig {
        retry_base: Duration::from_millis(1),
        retry_max: Duration::from_millis(5),
        poll_interval: Duration::from_millis(5),
        claim_batch: 64,
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
        resources: vec![],
        message_type: "text".to_owned(),
        create_time_ms: 0,
        scope: ScopeKey::Chat("oc_chat".to_owned()),
    }
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
    assert!(json.contains("\"version\":1"));
    assert_eq!(OutboxOperation::decode(&json).expect("decode"), operation);

    let rendered = format!("{operation:?}");
    assert!(!rendered.contains("secret reply body"));
    assert!(rendered.contains("text_chars"));
}

#[test]
fn payload_rejects_unknown_fields() {
    let json = r#"{"version":1,"op":"reply_text","message_id":"m","text":"t","bogus":1}"#;
    assert!(matches!(
        OutboxOperation::decode(json),
        Err(OutboxError::Deserialize)
    ));
}

#[test]
fn payload_rejects_wrong_version() {
    let json = r#"{"version":2,"op":"reply_text","message_id":"m","text":"t"}"#;
    assert!(matches!(
        OutboxOperation::decode(json),
        Err(OutboxError::UnsupportedVersion { version: 2 })
    ));
}

#[test]
fn payload_rejects_unknown_operation() {
    let json = r#"{"version":1,"op":"send_text","message_id":"m","text":"t"}"#;
    assert!(matches!(
        OutboxOperation::decode(json),
        Err(OutboxError::UnknownOperation)
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
            code: Some(230_001),
        }),
        DeliveryClass::Retryable
    );
    assert_eq!(
        classify_delivery(&LarkError::Retryable {
            context: "send",
            code: None,
        }),
        DeliveryClass::Uncertain
    );
    assert_eq!(
        classify_delivery(&LarkError::ProtocolViolation { context: "send" }),
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
    assert_eq!(row.idempotency_key, "1:final:evt_1");
    assert_eq!(row.kind, "final");
    let decoded = OutboxOperation::decode(&row.payload_json).expect("decode");
    match decoded {
        OutboxOperation::ReplyText {
            message_id, text, ..
        } => {
            assert_eq!(message_id, "om_parent");
            assert_eq!(text, "user[at]example.com");
        }
    }
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
    assert_eq!(row.idempotency_key, "7:notice:evt_1");
    assert_eq!(row.kind, "notice");
}

#[tokio::test]
async fn rejection_notice_is_deterministic() {
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = OutboxReplySink::new(store.clone());
    let event = inbound_event();

    let notice = sink
        .rejection_notice(&event, InboundRejectionKind::Policy)
        .expect("notice");
    assert_eq!(notice.idempotency_key, "evt_1:notice:policy");
    assert_eq!(notice.kind, "notice");
    let decoded = OutboxOperation::decode(&notice.payload_json).expect("decode");
    match decoded {
        OutboxOperation::ReplyText {
            message_id, text, ..
        } => {
            assert_eq!(message_id, "om_parent");
            assert!(!text.is_empty());
        }
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
                StubResponse::json(200, r#"{"code":230001,"msg":"busy"}"#)
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
    assert_eq!(reply_requests(&server).len(), 2);

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
