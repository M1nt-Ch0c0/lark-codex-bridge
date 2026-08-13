//! Bridge wiring tests: an in-process WebSocket server plus the shared HTTP
//! stub drive `LarkBridge` end to end — event → normalize → bounded channel,
//! full-channel `{code: 500}` receipts, card-action acks, degraded-event
//! delivery, and the one-shot `lark probe` round trip.

mod bridgews;
mod larkstub;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures_util::{FutureExt, StreamExt};
use lark_codex_bridge::lark::api::ChatMode;
use lark_codex_bridge::lark::bridge::{BridgeConfig, IntakeHook, IntakeVerdict, LarkBridge};
use lark_codex_bridge::lark::config::{LarkEndpoints, TenantBrand};
use lark_codex_bridge::lark::credentials::LarkCredentials;
use lark_codex_bridge::lark::frame::{Frame, Header, header_key};
use lark_codex_bridge::lark::http::LarkHttp;
use lark_codex_bridge::lark::normalize::ScopeKey;
use lark_codex_bridge::lark::transport::{LarkTransport, TransportHandle};
use lark_codex_bridge::runtime::intake::{DurableIntake, IntakeRuntime, TenantNamespace};
use lark_codex_bridge::store::{
    BeginTurnOutcome, InboundEventState, InboundKey, InboundTerminal, NewTurnRow, StoreHandle,
    TurnResolution, TurnState,
};
use larkstub::{RecordedRequest, StubResponse, StubServer};
use secrecy::SecretString;
use serde_json::Value;
use tokio::sync::{Notify, Semaphore, mpsc};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

use bridgews::TestWsServer;
const TEST_TIMEOUT: Duration = Duration::from_secs(10);
const TOKEN_PATH: &str = "/open-apis/auth/v3/tenant_access_token/internal";
const BOT_INFO_PATH: &str = "/open-apis/bot/v3/info";
const ENDPOINT_PATH: &str = "/callback/ws/endpoint";
const CHATS_PREFIX: &str = "/open-apis/im/v1/chats/";

const P2P_TEXT_FIXTURE: &str = include_str!("fixtures/lark/event_p2p_text.json");
const GROUP_MENTION_FIXTURE: &str = include_str!("fixtures/lark/event_group_mention.json");

fn test_credentials() -> LarkCredentials {
    LarkCredentials::new(
        "cli_test1234567890".to_owned(),
        SecretString::from("test-secret"),
        TenantBrand::Feishu,
    )
}

fn endpoints_for(stub: &StubServer) -> LarkEndpoints {
    let base = Url::parse(&stub.url()).expect("stub url");
    LarkEndpoints {
        open_base: base.clone(),
        accounts_base: base,
    }
}

fn endpoint_body(ws_addr: SocketAddr) -> String {
    format!(
        r#"{{"code":0,"msg":"ok","data":{{"URL":"ws://{ws_addr}/ws?device_id=dev-1&service_id=7","ClientConfig":{{"PingInterval":60,"ReconnectCount":-1,"ReconnectInterval":2,"ReconnectNonce":0}}}}}}"#
    )
}

fn p2p_fixture_with_ids(event_id: &str, message_id: &str) -> Vec<u8> {
    let mut payload: Value = serde_json::from_str(P2P_TEXT_FIXTURE).expect("fixture JSON");
    payload["header"]["event_id"] = Value::String(event_id.to_owned());
    payload["event"]["message"]["message_id"] = Value::String(message_id.to_owned());
    serde_json::to_vec(&payload).expect("encode fixture")
}

/// Serves tokens, bot info, and the endpoint bootstrap; chat-mode lookups
/// delegate to `chat_mode`.
fn bridge_stub(
    ws_addr: SocketAddr,
    chat_mode: impl Fn(&str) -> StubResponse + Send + Sync + 'static,
) -> larkstub::Handler {
    Arc::new(move |request: &RecordedRequest| {
        if request.path == TOKEN_PATH {
            return StubResponse::json(
                200,
                r#"{"code":0,"tenant_access_token":"token-0","expire":7200}"#,
            );
        }
        if request.path == BOT_INFO_PATH {
            return StubResponse::json(
                200,
                r#"{"code":0,"bot":{"app_name":"Bridge Bot","open_id":"ou_bot"}}"#,
            );
        }
        if request.path == ENDPOINT_PATH {
            return StubResponse::json(200, &endpoint_body(ws_addr));
        }
        if let Some(chat_id) = request.path.strip_prefix(CHATS_PREFIX) {
            return chat_mode(chat_id);
        }
        StubResponse::text(404, "not found")
    })
}

/// Starts the HTTP stub and WS server, then a bridge pointed at both.
async fn start_bridge(
    chat_mode: impl Fn(&str) -> StubResponse + Send + Sync + 'static,
    config: BridgeConfig,
) -> (
    StubServer,
    TestWsServer,
    TransportHandle,
    mpsc::Receiver<lark_codex_bridge::lark::bridge::QueuedInboundEvent>,
) {
    let ws_server = TestWsServer::start().await;
    let stub = StubServer::start(bridge_stub(ws_server.addr, chat_mode)).await;
    let (handle, events) = LarkBridge::start_with(endpoints_for(&stub), test_credentials(), config)
        .await
        .expect("bridge starts");
    (stub, ws_server, handle, events)
}

// ---------------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------------

#[tokio::test]
async fn event_is_normalized_and_queued_with_a_200_receipt() {
    let (_stub, mut ws_server, handle, mut events) = start_bridge(
        |_| StubResponse::text(500, "unused"),
        BridgeConfig::default(),
    )
    .await;

    let mut conn = ws_server.accept().await;
    let _ping = conn.recv_frame().await;
    conn.send_data("event", "m-p2p", P2P_TEXT_FIXTURE.as_bytes())
        .await;

    let (message_id, body) = conn.recv_receipt().await;
    assert_eq!(message_id, "m-p2p");
    assert_eq!(body["code"], 200);
    assert!(body.get("data").is_none());

    let queued = timeout(TEST_TIMEOUT, events.recv())
        .await
        .expect("an inbound event arrives")
        .expect("event channel stays open");
    let event = queued.into_event();
    assert_eq!(event.event_id, "evt_p2p_scrubbed_001");
    assert_eq!(event.message_id, "om_p2p_001");
    assert_eq!(event.chat_id, "oc_p2p_chat");
    assert_eq!(event.sender_id, "ou_alice");
    assert_eq!(event.chat_type, ChatMode::P2p);
    assert_eq!(event.text, "hello bridge");
    assert_eq!(event.scope, ScopeKey::Chat("oc_p2p_chat".to_owned()));

    handle.shutdown().await;
}

#[tokio::test]
async fn full_channel_fails_the_handler_with_a_500_receipt() {
    let config = BridgeConfig {
        event_capacity: 1,
        ..BridgeConfig::default()
    };
    let (_stub, mut ws_server, handle, mut events) =
        start_bridge(|_| StubResponse::text(500, "unused"), config).await;

    let mut conn = ws_server.accept().await;
    let _ping = conn.recv_frame().await;
    // The first event parks in the channel (capacity 1)...
    conn.send_data("event", "m-first", P2P_TEXT_FIXTURE.as_bytes())
        .await;
    let (message_id, body) = conn.recv_receipt().await;
    assert_eq!(message_id, "m-first");
    assert_eq!(body["code"], 200);
    // ...so the second one finds the channel full and the handler fails,
    // honestly reporting {code: 500} instead of silently dropping.
    conn.send_data("event", "m-second", P2P_TEXT_FIXTURE.as_bytes())
        .await;
    let (message_id, body) = conn.recv_receipt().await;
    assert_eq!(message_id, "m-second");
    assert_eq!(body["code"], 500);
    assert!(body.get("data").is_none());

    // Draining one slot lets the next event through again.
    let queued = timeout(TEST_TIMEOUT, events.recv())
        .await
        .expect("the parked event arrives")
        .expect("event channel stays open");
    assert_eq!(queued.event.event_id, "evt_p2p_scrubbed_001");
    conn.send_data("event", "m-third", P2P_TEXT_FIXTURE.as_bytes())
        .await;
    let (message_id, body) = conn.recv_receipt().await;
    assert_eq!(message_id, "m-third");
    assert_eq!(body["code"], 200);

    handle.shutdown().await;
}

#[tokio::test]
async fn card_action_is_acked_unsupported_and_not_routed() {
    let (_stub, mut ws_server, handle, mut events) = start_bridge(
        |_| StubResponse::text(500, "unused"),
        BridgeConfig::default(),
    )
    .await;

    let mut conn = ws_server.accept().await;
    let _ping = conn.recv_frame().await;
    conn.send_data("card", "m-card", br#"{"action":{"value":{}}}"#)
        .await;

    let (message_id, body) = conn.recv_receipt().await;
    assert_eq!(message_id, "m-card");
    assert_eq!(body["code"], 200);
    let data = BASE64
        .decode(body["data"].as_str().expect("card ack carries data"))
        .expect("base64 decodes");
    let data: Value = serde_json::from_slice(&data).expect("data is json");
    assert_eq!(data["status"], "unsupported");

    // Card actions are never routed into the inbound event channel.
    assert!(events.try_recv().is_err(), "no event queued for a card");

    handle.shutdown().await;
}

#[tokio::test]
async fn degraded_events_are_still_delivered() {
    // Chat-mode lookups fail, so the normalizer degrades to plain-group scope
    // but the event must still flow (and the receipt still says 200).
    let (_stub, mut ws_server, handle, mut events) =
        start_bridge(|_| StubResponse::text(500, "boom"), BridgeConfig::default()).await;

    let mut conn = ws_server.accept().await;
    let _ping = conn.recv_frame().await;
    conn.send_data("event", "m-degraded", GROUP_MENTION_FIXTURE.as_bytes())
        .await;

    let (message_id, body) = conn.recv_receipt().await;
    assert_eq!(message_id, "m-degraded");
    assert_eq!(body["code"], 200);

    let queued = timeout(TEST_TIMEOUT, events.recv())
        .await
        .expect("an inbound event arrives")
        .expect("event channel stays open");
    let event = queued.into_event();
    assert_eq!(event.chat_id, "oc_group_chat");
    assert_eq!(event.chat_type, ChatMode::Group);
    assert_eq!(event.scope, ScopeKey::Chat("oc_group_chat".to_owned()));
    // The bot open_id came from bot info, so mention detection works.
    assert!(event.mentions_bot);
    assert_eq!(event.text, "status?");

    handle.shutdown().await;
}

#[tokio::test]
async fn durable_runtime_persists_before_200_and_preloads_on_restart() {
    let store = StoreHandle::open_in_memory().await.expect("store");
    let credentials = test_credentials();
    let namespace = TenantNamespace::from_credentials(&credentials);
    let intake = DurableIntake::prepare(store.clone(), &credentials)
        .await
        .expect("prepare");
    let mut ws_server = TestWsServer::start().await;
    let stub = StubServer::start(bridge_stub(ws_server.addr, |_| {
        StubResponse::text(500, "unused")
    }))
    .await;
    let (handle, mut events) = LarkBridge::start_with_runtime(
        endpoints_for(&stub),
        credentials.clone(),
        BridgeConfig::default(),
        intake,
    )
    .await
    .expect("durable bridge starts");
    let mut conn = ws_server.accept().await;
    let _ping = conn.recv_frame().await;
    conn.send_data("event", "m-durable", P2P_TEXT_FIXTURE.as_bytes())
        .await;
    let (_, receipt) = conn.recv_receipt().await;
    assert_eq!(receipt["code"], 200);
    assert_eq!(
        store
            .inbound_state(&namespace, "evt_p2p_scrubbed_001")
            .await
            .expect("persisted"),
        Some(InboundEventState::Received),
        "receipt follows the SQLite commit"
    );
    let queued = events.recv().await.expect("live event");
    assert_eq!(queued.event.event_id, "evt_p2p_scrubbed_001");
    drop(queued);
    handle.shutdown().await;

    let restart = DurableIntake::prepare(store.clone(), &credentials)
        .await
        .expect("restart prepare");
    let (handle, mut replay) = LarkBridge::start_with_runtime(
        endpoints_for(&stub),
        credentials,
        BridgeConfig::default(),
        restart,
    )
    .await
    .expect("restart bridge");
    let replayed = replay.try_recv().expect("startup recovery is preloaded");
    assert_eq!(replayed.event.event_id, "evt_p2p_scrubbed_001");
    handle.shutdown().await;
    store.shutdown().await.expect("shutdown store");
}

#[tokio::test]
async fn durable_runtime_rejects_binding_mismatch_and_zero_limits() {
    let store = StoreHandle::open_in_memory().await.expect("store");
    let original = test_credentials();
    let mut ws_server = TestWsServer::start().await;
    let stub = StubServer::start(bridge_stub(ws_server.addr, |_| {
        StubResponse::text(500, "unused")
    }))
    .await;
    let mismatch = DurableIntake::prepare(store.clone(), &original)
        .await
        .expect("prepare mismatch");
    let other = LarkCredentials::new(
        "cli_other_app".to_owned(),
        SecretString::from("other-secret"),
        TenantBrand::Feishu,
    );
    let Err(error) = LarkBridge::start_with_runtime(
        endpoints_for(&stub),
        other,
        BridgeConfig::default(),
        mismatch,
    )
    .await
    else {
        panic!("credential mismatch must fail");
    };
    assert_eq!(
        error.kind(),
        lark_codex_bridge::lark::error::LarkErrorKind::ProtocolViolation
    );
    assert!(
        ws_server.incoming.try_recv().is_err(),
        "no WebSocket starts"
    );

    let zero = DurableIntake::prepare(store.clone(), &original)
        .await
        .expect("prepare zero");
    let Err(error) = LarkBridge::start_with_runtime(
        endpoints_for(&stub),
        original,
        BridgeConfig {
            event_capacity: 0,
            ..BridgeConfig::default()
        },
        zero,
    )
    .await
    else {
        panic!("zero count bound must fail");
    };
    assert_eq!(
        error.kind(),
        lark_codex_bridge::lark::error::LarkErrorKind::ProtocolViolation
    );
    assert!(ws_server.incoming.try_recv().is_err(), "still no WebSocket");
    store.shutdown().await.expect("shutdown store");
}

#[tokio::test]
async fn durable_hook_blocks_receipt_until_it_resolves() {
    let credentials = test_credentials();
    let entered = Arc::new(Notify::new());
    let gate = Arc::new(Semaphore::new(0));
    let hook: IntakeHook = {
        let entered = Arc::clone(&entered);
        let gate = Arc::clone(&gate);
        Arc::new(move |_event| {
            let entered = Arc::clone(&entered);
            let gate = Arc::clone(&gate);
            async move {
                entered.notify_one();
                let _permit = gate.acquire_owned().await.expect("gate stays open");
                Ok(IntakeVerdict::DropDuplicate)
            }
            .boxed()
        })
    };
    let runtime = IntakeRuntime::try_from_parts(&credentials, Vec::new(), hook).expect("runtime");
    let mut ws_server = TestWsServer::start().await;
    let stub = StubServer::start(bridge_stub(ws_server.addr, |_| {
        StubResponse::text(500, "unused")
    }))
    .await;
    let (handle, mut events) = LarkBridge::start_with_runtime(
        endpoints_for(&stub),
        credentials,
        BridgeConfig::default(),
        runtime,
    )
    .await
    .expect("start");
    let mut conn = ws_server.accept().await;
    let _ping = conn.recv_frame().await;
    conn.send_data("event", "m-hook", P2P_TEXT_FIXTURE.as_bytes())
        .await;
    timeout(TEST_TIMEOUT, entered.notified())
        .await
        .expect("hook entered");
    assert!(
        timeout(Duration::from_millis(100), conn.recv_receipt())
            .await
            .is_err(),
        "receipt waits for the durable hook"
    );
    assert!(events.try_recv().is_err(), "business queue is still empty");
    gate.add_permits(1);
    let (_, receipt) = conn.recv_receipt().await;
    assert_eq!(receipt["code"], 200);
    assert!(
        events.try_recv().is_err(),
        "duplicate verdict is not enqueued"
    );
    handle.shutdown().await;
}

#[tokio::test]
async fn terminal_duplicate_acks_200_even_when_durable_channel_is_full() {
    let store = StoreHandle::open_in_memory().await.expect("store");
    let credentials = test_credentials();
    let namespace = TenantNamespace::from_credentials(&credentials);
    let runtime = DurableIntake::prepare(store.clone(), &credentials)
        .await
        .expect("prepare");
    let mut ws_server = TestWsServer::start().await;
    let stub = StubServer::start(bridge_stub(ws_server.addr, |_| {
        StubResponse::text(500, "unused")
    }))
    .await;
    let (handle, mut events) = LarkBridge::start_with_runtime(
        endpoints_for(&stub),
        credentials,
        BridgeConfig {
            event_capacity: 1,
            ..BridgeConfig::default()
        },
        runtime,
    )
    .await
    .expect("start");
    let mut conn = ws_server.accept().await;
    let _ping = conn.recv_frame().await;
    conn.send_data("event", "m-new", P2P_TEXT_FIXTURE.as_bytes())
        .await;
    assert_eq!(conn.recv_receipt().await.1["code"], 200);

    let begun = store
        .begin_turn_and_claim_inbound(
            NewTurnRow {
                scope_key: "im:oc_p2p_chat".to_owned(),
                client_message_id: "bridge-terminal-turn".to_owned(),
                codex_thread_id: Some("thread-bridge".to_owned()),
                state: TurnState::Starting,
            },
            &[InboundKey::new(
                namespace,
                "evt_p2p_scrubbed_001".to_owned(),
            )],
        )
        .await
        .expect("claim");
    let BeginTurnOutcome::Started { turn_row_id, .. } = begun else {
        panic!("started")
    };
    store
        .set_turn_state(turn_row_id, TurnState::Running, Some("codex-bridge"))
        .await
        .expect("running");
    store
        .resolve_turn_and_finish_inbound_batch(
            turn_row_id,
            TurnResolution::Completed,
            InboundTerminal::Completed,
        )
        .await
        .expect("resolve");

    conn.send_data("event", "m-terminal-duplicate", P2P_TEXT_FIXTURE.as_bytes())
        .await;
    assert_eq!(conn.recv_receipt().await.1["code"], 200);
    assert_eq!(
        events
            .try_recv()
            .expect("original remains queued")
            .event
            .event_id,
        "evt_p2p_scrubbed_001"
    );
    handle.shutdown().await;
    store.shutdown().await.expect("shutdown store");
}

#[tokio::test]
async fn committed_row_replays_after_channel_full_500() {
    let store = StoreHandle::open_in_memory().await.expect("store");
    let credentials = test_credentials();
    let namespace = TenantNamespace::from_credentials(&credentials);
    let runtime = DurableIntake::prepare(store.clone(), &credentials)
        .await
        .expect("prepare");
    let mut ws_server = TestWsServer::start().await;
    let stub = StubServer::start(bridge_stub(ws_server.addr, |_| {
        StubResponse::text(500, "unused")
    }))
    .await;
    let (handle, mut events) = LarkBridge::start_with_runtime(
        endpoints_for(&stub),
        credentials,
        BridgeConfig {
            event_capacity: 1,
            ..BridgeConfig::default()
        },
        runtime,
    )
    .await
    .expect("start");
    let mut conn = ws_server.accept().await;
    let _ping = conn.recv_frame().await;
    let first = p2p_fixture_with_ids("event-capacity-first", "message-capacity-first");
    let second = p2p_fixture_with_ids("event-capacity-second", "message-capacity-second");
    conn.send_data("event", "m-capacity-first", &first).await;
    assert_eq!(conn.recv_receipt().await.1["code"], 200);
    conn.send_data("event", "m-capacity-second", &second).await;
    assert_eq!(conn.recv_receipt().await.1["code"], 500);
    assert_eq!(
        store
            .inbound_state(&namespace, "event-capacity-second")
            .await
            .expect("persisted after 500"),
        Some(InboundEventState::Received)
    );
    assert_eq!(
        events.recv().await.expect("drain first").event.event_id,
        "event-capacity-first"
    );
    conn.send_data("event", "m-capacity-retry", &second).await;
    assert_eq!(conn.recv_receipt().await.1["code"], 200);
    assert_eq!(
        events
            .recv()
            .await
            .expect("canonical replay")
            .event
            .event_id,
        "event-capacity-second"
    );
    handle.shutdown().await;
    store.shutdown().await.expect("shutdown store");
}

// ---------------------------------------------------------------------------
// Probe
// ---------------------------------------------------------------------------

#[tokio::test]
async fn probe_completes_a_ping_pong_round_trip() {
    let ws_server = TestWsServer::start().await;
    let stub = StubServer::start(bridge_stub(ws_server.addr, |_| {
        StubResponse::text(500, "unused")
    }))
    .await;
    let http = LarkHttp::new(endpoints_for(&stub)).expect("http client");

    let probe = {
        let http = http.clone();
        tokio::spawn(async move { LarkTransport::probe(&http, &test_credentials()).await })
    };
    let mut server = ws_server;
    let mut conn = server.accept().await;
    let ping = conn.recv_frame().await;
    assert_eq!(ping, Frame::ping(7));
    // The pong negotiates the ping interval down from the bootstrap 60 s.
    conn.send_pong(r#"{"PingInterval":5,"ReconnectCount":-1,"ReconnectNonce":0}"#)
        .await;

    let outcome = timeout(TEST_TIMEOUT, probe)
        .await
        .expect("probe finishes")
        .expect("probe task joins")
        .expect("probe succeeds");
    assert_eq!(outcome.endpoint_host, "127.0.0.1");
    assert_eq!(outcome.ping_interval, Duration::from_secs(5));

    // The probe closes the socket politely.
    let closed = timeout(TEST_TIMEOUT, async {
        loop {
            match conn.ws.next().await {
                Some(Ok(Message::Close(_)) | Err(_)) | None => break,
                Some(Ok(_)) => {}
            }
        }
    })
    .await;
    assert!(closed.is_ok(), "probe socket closed");
}

#[tokio::test]
async fn probe_reports_handshake_auth_errors_as_permanent() {
    let mut ws_server = TestWsServer::start().await;
    let stub = StubServer::start(bridge_stub(ws_server.addr, |_| {
        StubResponse::text(500, "unused")
    }))
    .await;
    let http = LarkHttp::new(endpoints_for(&stub)).expect("http client");

    let probe = {
        let http = http.clone();
        tokio::spawn(async move { LarkTransport::probe(&http, &test_credentials()).await })
    };
    let mut conn = ws_server.accept().await;
    let _ping = conn.recv_frame().await;
    let mut frame = Frame::ping(7);
    frame.headers = vec![Header {
        key: header_key::HANDSHAKE_AUTHERRCODE.to_owned(),
        value: "1000040351".to_owned(),
    }];
    conn.send_frame(&frame).await;

    let error = timeout(TEST_TIMEOUT, probe)
        .await
        .expect("probe finishes")
        .expect("probe task joins")
        .expect_err("probe fails");
    assert_eq!(
        error.kind(),
        lark_codex_bridge::lark::error::LarkErrorKind::PermanentAuth
    );
}
