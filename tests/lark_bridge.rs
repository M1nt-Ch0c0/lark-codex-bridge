//! Bridge wiring tests: an in-process WebSocket server plus the shared HTTP
//! stub drive `LarkBridge` end to end — event → normalize → bounded channel,
//! full-channel `{code: 500}` receipts, card-action acks, degraded-event
//! delivery, and the one-shot `lark probe` round trip.

mod larkstub;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use lark_codex_bridge::lark::api::ChatMode;
use lark_codex_bridge::lark::bridge::{BridgeConfig, LarkBridge};
use lark_codex_bridge::lark::config::{LarkEndpoints, TenantBrand};
use lark_codex_bridge::lark::credentials::LarkCredentials;
use lark_codex_bridge::lark::frame::{Frame, FrameMethod, Header, header_key};
use lark_codex_bridge::lark::http::LarkHttp;
use lark_codex_bridge::lark::normalize::ScopeKey;
use lark_codex_bridge::lark::transport::{LarkTransport, TransportHandle};
use larkstub::{RecordedRequest, StubResponse, StubServer};
use secrecy::SecretString;
use serde_json::Value;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

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

// ---------------------------------------------------------------------------
// In-process WebSocket server (same pattern as tests/lark_transport.rs)
// ---------------------------------------------------------------------------
struct TestWsServer {
    addr: SocketAddr,
    incoming: mpsc::UnboundedReceiver<TestWsConn>,
    task: JoinHandle<()>,
}

struct TestWsConn {
    ws: WebSocketStream<TcpStream>,
}

impl TestWsServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("ws listener binds");
        let addr = listener.local_addr().expect("ws addr");
        let (tx, incoming) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let tx = tx.clone();
                tokio::spawn(async move {
                    if let Ok(ws) = tokio_tungstenite::accept_async(stream).await {
                        let _ = tx.send(TestWsConn { ws });
                    }
                });
            }
        });
        Self {
            addr,
            incoming,
            task,
        }
    }

    async fn accept(&mut self) -> TestWsConn {
        timeout(TEST_TIMEOUT, self.incoming.recv())
            .await
            .expect("a connection arrives")
            .expect("connection channel stays open")
    }
}

impl Drop for TestWsServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl TestWsConn {
    async fn recv_frame(&mut self) -> Frame {
        let message = timeout(TEST_TIMEOUT, self.ws.next())
            .await
            .expect("a frame arrives")
            .expect("socket stays open")
            .expect("frame decodes at the ws layer");
        let Message::Binary(bytes) = message else {
            panic!("expected a binary frame, got {message:?}");
        };
        Frame::decode_bytes(&bytes).expect("pbbp2 frame decodes")
    }

    async fn send_frame(&mut self, frame: &Frame) {
        self.ws
            .send(Message::Binary(frame.encode_to_vec().into()))
            .await
            .expect("frame sends");
    }

    /// Sends a single-fragment data frame of the given type (`event`/`card`).
    async fn send_data(&mut self, ty: &str, message_id: &str, payload: &[u8]) {
        let mut frame = Frame::ping(7);
        frame.method = FrameMethod::Data.as_wire();
        frame.headers = vec![
            Header {
                key: header_key::TYPE.to_owned(),
                value: ty.to_owned(),
            },
            Header {
                key: header_key::MESSAGE_ID.to_owned(),
                value: message_id.to_owned(),
            },
            Header {
                key: header_key::SUM.to_owned(),
                value: "1".to_owned(),
            },
            Header {
                key: header_key::SEQ.to_owned(),
                value: "0".to_owned(),
            },
            Header {
                key: header_key::TRACE_ID.to_owned(),
                value: format!("tr-{message_id}"),
            },
        ];
        frame.payload = Some(Bytes::from(payload.to_vec()));
        self.send_frame(&frame).await;
    }

    /// Sends a control pong frame carrying a `ClientConfig` payload.
    async fn send_pong(&mut self, config_json: &str) {
        let mut frame = Frame::ping(7);
        frame.headers = vec![Header {
            key: header_key::TYPE.to_owned(),
            value: "pong".to_owned(),
        }];
        frame.payload = Some(Bytes::from(config_json.as_bytes().to_vec()));
        self.send_frame(&frame).await;
    }

    /// Reads frames until one has a `biz_rt` header (a receipt); returns its
    /// `message_id` and decoded JSON body.
    async fn recv_receipt(&mut self) -> (String, Value) {
        loop {
            let frame = self.recv_frame().await;
            let headers = frame.frame_headers();
            if headers.biz_rt().is_some() {
                let body: Value =
                    serde_json::from_slice(frame.payload.as_ref().expect("receipt payload"))
                        .expect("receipt payload is json");
                return (
                    headers.message_id().expect("receipt message_id").to_owned(),
                    body,
                );
            }
        }
    }
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
