//! WebSocket transport tests: bootstrap classification, ping/pong, receipts,
//! fragments, reconnect policy, and shutdown against an in-process server.

mod larkstub;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use lark_codex_bridge::codex::supervisor::AppServerSupervisor;
use lark_codex_bridge::lark::config::LarkEndpoints;
use lark_codex_bridge::lark::credentials::LarkCredentials;
use lark_codex_bridge::lark::error::{LarkError, LarkErrorKind};
use lark_codex_bridge::lark::frame::{Frame, FrameHeaders, FrameMethod, Header, header_key};
use lark_codex_bridge::lark::http::LarkHttp;
use lark_codex_bridge::lark::transport::{
    InboundFrameHandler, LarkTransport, TransportConfig, TransportEvent, TransportHandle,
    TransportState, WsEndpoint,
};
use larkstub::{StubResponse, StubServer};
use secrecy::SecretString;
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

fn test_credentials() -> LarkCredentials {
    LarkCredentials::new(
        "cli_test1234567890".to_owned(),
        SecretString::from("test-secret"),
        lark_codex_bridge::lark::config::TenantBrand::Feishu,
    )
}

fn endpoints_for(stub: &StubServer) -> LarkEndpoints {
    let base = Url::parse(&stub.url()).expect("stub url");
    LarkEndpoints {
        open_base: base.clone(),
        accounts_base: base,
    }
}

fn endpoint_body(ws_addr: SocketAddr, client_config: &str) -> String {
    format!(
        r#"{{"code":0,"msg":"ok","data":{{"URL":"ws://{ws_addr}/ws?device_id=dev-1&service_id=7","ClientConfig":{{{client_config}}}}}}}"#
    )
}

type SeenEvents = Arc<Mutex<Vec<(FrameHeaders, Bytes)>>>;

fn ok_handler() -> (InboundFrameHandler, SeenEvents) {
    let seen: SeenEvents = Arc::new(Mutex::new(Vec::new()));
    let handler_seen = Arc::clone(&seen);
    let handler: InboundFrameHandler = Arc::new(move |headers, payload| {
        let handler_seen = Arc::clone(&handler_seen);
        Box::pin(async move {
            handler_seen
                .lock()
                .expect("seen lock")
                .push((headers, payload));
            Ok(Some(json!({"ok": true})))
        })
    });
    (handler, seen)
}

// ---------------------------------------------------------------------------
// In-process WebSocket server
// ---------------------------------------------------------------------------

/// Accepts WebSocket connections and hands them to the test one at a time.
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

    /// Asserts no new connection arrives within `window`.
    async fn assert_no_connection(&mut self, window: Duration) {
        assert!(
            timeout(window, self.incoming.recv()).await.is_err(),
            "unexpected new connection"
        );
    }
}

impl Drop for TestWsServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl TestWsConn {
    /// Reads the next binary message and decodes it as a pbbp2 frame.
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

    /// Sends a data event frame; `payload` is the fragment body.
    async fn send_event(&mut self, message_id: &str, sum: u32, seq: u32, payload: &[u8]) {
        let mut frame = Frame::ping(7);
        frame.method = FrameMethod::Data.as_wire();
        frame.headers = vec![
            Header {
                key: header_key::TYPE.to_owned(),
                value: "event".to_owned(),
            },
            Header {
                key: header_key::MESSAGE_ID.to_owned(),
                value: message_id.to_owned(),
            },
            Header {
                key: header_key::SUM.to_owned(),
                value: sum.to_string(),
            },
            Header {
                key: header_key::SEQ.to_owned(),
                value: seq.to_string(),
            },
            Header {
                key: header_key::TRACE_ID.to_owned(),
                value: format!("tr-{message_id}"),
            },
        ];
        frame.payload = Some(Bytes::from(payload.to_vec()));
        self.send_frame(&frame).await;
    }

    /// Reads frames until one has a `biz_rt` header (a receipt).
    async fn recv_receipt(&mut self) -> Frame {
        loop {
            let frame = self.recv_frame().await;
            if frame
                .headers
                .iter()
                .any(|header| header.key == header_key::BIZ_RT)
            {
                return frame;
            }
        }
    }
}

/// Starts a stub serving the endpoint bootstrap plus a WS server, and a
/// transport pointed at both.
async fn start_transport(
    client_config: &str,
    handler: InboundFrameHandler,
    config: TransportConfig,
) -> (StubServer, TestWsServer, TransportHandle) {
    let ws_server = TestWsServer::start().await;
    let body = endpoint_body(ws_server.addr, client_config);
    let stub = StubServer::start(Arc::new(move |_| StubResponse::json(200, &body))).await;
    let http = LarkHttp::new(endpoints_for(&stub)).expect("http client");
    let handle = LarkTransport::start_with_config(http, test_credentials(), handler, config);
    (stub, ws_server, handle)
}

const DEFAULT_CLIENT_CONFIG: &str =
    r#""PingInterval":60,"ReconnectCount":-1,"ReconnectInterval":2,"ReconnectNonce":0"#;

async fn next_state(handle: &mut TransportHandle) -> TransportState {
    loop {
        let event = timeout(TEST_TIMEOUT, handle.next_event())
            .await
            .expect("a state event arrives")
            .expect("event channel stays open");
        if let TransportEvent::State(state) = event {
            return state;
        }
    }
}

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pull_endpoint_parses_query_and_client_config() {
    let ws_server = TestWsServer::start().await;
    let body = endpoint_body(ws_server.addr, DEFAULT_CLIENT_CONFIG);
    let stub = StubServer::start(Arc::new(move |_| StubResponse::json(200, &body))).await;
    let http = LarkHttp::new(endpoints_for(&stub)).expect("http client");

    let endpoint: WsEndpoint = LarkTransport::pull_endpoint(&http, &test_credentials())
        .await
        .expect("bootstrap succeeds");
    assert_eq!(endpoint.device_id, "dev-1");
    assert_eq!(endpoint.service_id, 7);
    assert_eq!(endpoint.ping_interval, Duration::from_secs(60));
    assert_eq!(endpoint.reconnect_count, -1);
    assert_eq!(endpoint.reconnect_interval, Duration::from_secs(2));
    assert_eq!(endpoint.reconnect_nonce, Duration::ZERO);
    assert_eq!(endpoint.url.host_str(), Some("127.0.0.1"));
    // The full URL (with tickets) must never appear in Debug output.
    let debug = format!("{endpoint:?}");
    assert!(debug.contains("dev-1"));
    assert!(!debug.contains("device_id=dev-1"));

    let request = &stub.requests()[0];
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/callback/ws/endpoint");
    assert_eq!(request.header("locale"), Some("zh"));
    let sent: Value = serde_json::from_str(&request.body_text()).expect("json body");
    assert_eq!(sent["AppID"], "cli_test1234567890");
    assert!(sent["AppSecret"].is_string());
}

#[tokio::test]
async fn pull_endpoint_classifies_response_codes() {
    let cases: &[(&str, u16, LarkErrorKind)] = &[
        (
            r#"{"code":1,"msg":"system busy"}"#,
            200,
            LarkErrorKind::Retryable,
        ),
        (
            r#"{"code":1000040343,"msg":"internal error"}"#,
            200,
            LarkErrorKind::Retryable,
        ),
        (
            r#"{"code":403,"msg":"forbidden"}"#,
            200,
            LarkErrorKind::PermanentAuth,
        ),
        (
            r#"{"code":514,"msg":"auth failed"}"#,
            200,
            LarkErrorKind::PermanentAuth,
        ),
        (
            r#"{"code":1000040350,"msg":"exceed connection limit"}"#,
            200,
            LarkErrorKind::Exhausted,
        ),
        // Unknown codes stay non-retryable, like the reference default.
        (
            r#"{"code":999,"msg":"mystery"}"#,
            200,
            LarkErrorKind::PermanentAuth,
        ),
        // HTTP-level classification.
        (r#"{"code":0}"#, 403, LarkErrorKind::PermanentAuth),
        (r#"{"code":0}"#, 500, LarkErrorKind::Retryable),
    ];
    for (body, status, expected) in cases {
        let stub = StubServer::start(Arc::new(move |_| StubResponse::json(*status, body))).await;
        let http = LarkHttp::new(endpoints_for(&stub)).expect("http client");
        let error = LarkTransport::pull_endpoint(&http, &test_credentials())
            .await
            .expect_err("bootstrap fails");
        assert_eq!(&error.kind(), expected, "body {body} status {status}");
    }
}

#[tokio::test]
async fn pull_endpoint_rejects_malformed_responses() {
    for body in [
        r#"{"code":0,"msg":"ok"}"#,                                   // no data
        r#"{"code":0,"data":{"URL":"not a url","ClientConfig":{}}}"#, // bad URL
        r#"{"code":0,"data":{"URL":"ws://127.0.0.1:1/ws","ClientConfig":{}}}"#, // no query
    ] {
        let stub = StubServer::start(Arc::new(move |_| StubResponse::json(200, body))).await;
        let http = LarkHttp::new(endpoints_for(&stub)).expect("http client");
        let error = LarkTransport::pull_endpoint(&http, &test_credentials())
            .await
            .expect_err("malformed response rejected");
        assert_eq!(
            error.kind(),
            LarkErrorKind::ProtocolViolation,
            "body {body}"
        );
    }
}

// ---------------------------------------------------------------------------
// Ping/pong, dispatch, receipts
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ping_frame_is_sent_on_connect_with_exact_wire_fields() {
    let (handler, _seen) = ok_handler();
    let (_stub, mut ws_server, handle) =
        start_transport(DEFAULT_CLIENT_CONFIG, handler, TransportConfig::default()).await;

    let mut conn = ws_server.accept().await;
    let ping = conn.recv_frame().await;
    assert_eq!(ping, Frame::ping(7));
    assert_eq!(ping.service, 7);
    assert_eq!(ping.method, FrameMethod::Control.as_wire());
    assert_eq!(ping.seq_id, 0);
    assert_eq!(ping.log_id, 0);
    assert_eq!(
        ping.frame_headers().ty(),
        Some(lark_codex_bridge::lark::frame::MessageType::Ping)
    );

    handle.shutdown().await;
}

#[tokio::test]
async fn pong_payload_updates_the_live_ping_interval() {
    let (handler, _seen) = ok_handler();
    // Bootstrap says 60 s; the pong drops it to 1 s.
    let (_stub, mut ws_server, handle) =
        start_transport(DEFAULT_CLIENT_CONFIG, handler, TransportConfig::default()).await;

    let mut conn = ws_server.accept().await;
    let _first_ping = conn.recv_frame().await;
    conn.send_pong(
        r#"{"PingInterval":1,"ReconnectCount":9,"ReconnectInterval":3,"ReconnectNonce":0}"#,
    )
    .await;
    // Without the pong update the next ping would be 60 s out.
    let second = timeout(Duration::from_secs(5), conn.recv_frame())
        .await
        .expect("the pong-updated interval schedules the next ping");
    assert_eq!(second, Frame::ping(7));

    handle.shutdown().await;
}

#[tokio::test]
async fn liveness_timeout_drops_the_socket_and_reconnects() {
    let (handler, _seen) = ok_handler();
    let config = TransportConfig {
        pong_timeout: Duration::from_millis(200),
    };
    let (stub, mut ws_server, handle) =
        start_transport(DEFAULT_CLIENT_CONFIG, handler, config).await;

    let mut conn = ws_server.accept().await;
    let _ping = conn.recv_frame().await;
    // Never answer: after the pong timeout the client must drop the socket,
    // pull a fresh endpoint, and open a new connection.
    let mut second = ws_server.accept().await;
    let _ping = second.recv_frame().await;
    assert!(
        stub.request_count() >= 2,
        "bootstrap pulled again on reconnect"
    );
    drop(conn);

    handle.shutdown().await;
}

#[tokio::test]
async fn receipt_200_with_base64_data_follows_handler_success() {
    let (handler, seen) = ok_handler();
    let (_stub, mut ws_server, handle) =
        start_transport(DEFAULT_CLIENT_CONFIG, handler, TransportConfig::default()).await;

    let mut conn = ws_server.accept().await;
    let _ping = conn.recv_frame().await;
    conn.send_event("m-1", 1, 0, br#"{"a":1}"#).await;

    let receipt = conn.recv_receipt().await;
    let headers = receipt.frame_headers();
    assert_eq!(headers.message_id(), Some("m-1"));
    assert!(headers.biz_rt().is_some());
    assert_eq!(receipt.method, FrameMethod::Data.as_wire());
    let body: Value = serde_json::from_slice(receipt.payload.as_ref().expect("payload"))
        .expect("receipt payload is json");
    assert_eq!(body["code"], 200);
    let data = BASE64
        .decode(body["data"].as_str().expect("data present"))
        .expect("base64 decodes");
    let data: Value = serde_json::from_slice(&data).expect("data is json");
    assert_eq!(data, json!({"ok": true}));

    // The handler observed exactly the dispatched payload.
    let seen = seen.lock().expect("seen lock").clone();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].0.message_id(), Some("m-1"));
    assert_eq!(seen[0].1.as_ref(), br#"{"a":1}"#);

    handle.shutdown().await;
}

#[tokio::test]
async fn receipt_500_follows_handler_failure() {
    let handler: InboundFrameHandler = Arc::new(|_headers, _payload| {
        Box::pin(async { Err(LarkError::retryable("handler failed")) })
    });
    let (_stub, mut ws_server, handle) =
        start_transport(DEFAULT_CLIENT_CONFIG, handler, TransportConfig::default()).await;

    let mut conn = ws_server.accept().await;
    let _ping = conn.recv_frame().await;
    conn.send_event("m-err", 1, 0, b"{}").await;

    let receipt = conn.recv_receipt().await;
    assert_eq!(receipt.frame_headers().message_id(), Some("m-err"));
    let body: Value = serde_json::from_slice(receipt.payload.as_ref().expect("payload"))
        .expect("receipt payload is json");
    assert_eq!(body["code"], 500);
    assert!(body.get("data").is_none());

    handle.shutdown().await;
}

#[tokio::test]
async fn single_and_multi_fragment_events_are_delivered_in_order() {
    let (handler, seen) = ok_handler();
    let (_stub, mut ws_server, handle) =
        start_transport(DEFAULT_CLIENT_CONFIG, handler, TransportConfig::default()).await;

    let mut conn = ws_server.accept().await;
    let _ping = conn.recv_frame().await;
    // Multi-fragment message arrives out of order...
    conn.send_event("m-multi", 2, 1, br"1}").await;
    conn.send_event("m-multi", 2, 0, br#"{"a":"#).await;
    // ...followed by a single-fragment message.
    conn.send_event("m-single", 1, 0, br#"{"b":2}"#).await;

    let first = conn.recv_receipt().await;
    assert_eq!(first.frame_headers().message_id(), Some("m-multi"));
    let second = conn.recv_receipt().await;
    assert_eq!(second.frame_headers().message_id(), Some("m-single"));

    let seen = seen.lock().expect("seen lock").clone();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].0.message_id(), Some("m-multi"));
    assert_eq!(seen[0].1.as_ref(), br#"{"a":1}"#);
    assert_eq!(seen[1].0.message_id(), Some("m-single"));
    assert_eq!(seen[1].1.as_ref(), br#"{"b":2}"#);

    handle.shutdown().await;
}

#[tokio::test]
async fn fragment_anomaly_surfaces_without_disconnecting() {
    let (handler, seen) = ok_handler();
    let (_stub, mut ws_server, mut handle) =
        start_transport(DEFAULT_CLIENT_CONFIG, handler, TransportConfig::default()).await;

    let mut conn = ws_server.accept().await;
    let _ping = conn.recv_frame().await;
    // seq >= sum is a protocol anomaly...
    conn.send_event("m-bad", 2, 5, b"x").await;
    // ...but the connection survives and later events still flow.
    conn.send_event("m-good", 1, 0, b"{}").await;
    let receipt = conn.recv_receipt().await;
    assert_eq!(receipt.frame_headers().message_id(), Some("m-good"));

    let mut saw_anomaly = false;
    while let Some(event) = timeout(TEST_TIMEOUT, handle.next_event())
        .await
        .expect("events arrive")
    {
        match event {
            TransportEvent::Anomaly { kind, message_id } => {
                assert_eq!(kind, "fragment-out-of-range");
                assert_eq!(message_id.as_deref(), Some("m-bad"));
                saw_anomaly = true;
            }
            TransportEvent::Message { headers, .. } => {
                assert_eq!(headers.message_id(), Some("m-good"));
                break;
            }
            TransportEvent::State(_) => {}
        }
    }
    assert!(saw_anomaly, "anomaly surfaced");
    assert_eq!(seen.lock().expect("seen lock").len(), 1);

    handle.shutdown().await;
}

#[tokio::test]
async fn malformed_frames_and_unknown_types_are_non_fatal_anomalies() {
    let (handler, _seen) = ok_handler();
    let (_stub, mut ws_server, mut handle) =
        start_transport(DEFAULT_CLIENT_CONFIG, handler, TransportConfig::default()).await;

    let mut conn = ws_server.accept().await;
    let _ping = conn.recv_frame().await;
    // Garbage bytes that are not a pbbp2 frame.
    conn.ws
        .send(Message::Binary(Bytes::from_static(b"\xff\xff\xff")))
        .await
        .expect("garbage sends");
    // A data frame with an unknown message type.
    let mut weird = Frame::ping(7);
    weird.method = FrameMethod::Data.as_wire();
    weird.headers = vec![Header {
        key: header_key::TYPE.to_owned(),
        value: "mystery".to_owned(),
    }];
    weird.payload = Some(Bytes::from_static(b"{}"));
    conn.send_frame(&weird).await;
    // The connection must survive both.
    conn.send_event("m-ok", 1, 0, b"{}").await;
    let receipt = conn.recv_receipt().await;
    assert_eq!(receipt.frame_headers().message_id(), Some("m-ok"));

    let mut kinds = Vec::new();
    while let Some(event) = timeout(TEST_TIMEOUT, handle.next_event())
        .await
        .expect("events arrive")
    {
        match event {
            TransportEvent::Anomaly { kind, .. } => kinds.push(kind),
            TransportEvent::Message { .. } => break,
            TransportEvent::State(_) => {}
        }
    }
    assert!(kinds.contains(&"frame-decode"), "kinds: {kinds:?}");
    assert!(kinds.contains(&"unknown-message-type"), "kinds: {kinds:?}");

    handle.shutdown().await;
}

// ---------------------------------------------------------------------------
// Reconnect policy
// ---------------------------------------------------------------------------

#[tokio::test]
async fn retryable_bootstrap_failures_backoff_with_supervisor_jitter() {
    let stub = StubServer::start(Arc::new(|_| {
        StubResponse::json(200, r#"{"code":1,"msg":"system busy"}"#)
    }))
    .await;
    let http = LarkHttp::new(endpoints_for(&stub)).expect("http client");
    let (handler, _seen) = ok_handler();
    let mut handle = LarkTransport::start(http, test_credentials(), handler);

    assert_eq!(
        next_state(&mut handle).await,
        TransportState::Connecting { attempt: 1 }
    );
    let expected_first = AppServerSupervisor::retry_delay(0, 1);
    let expected_second = AppServerSupervisor::retry_delay(0, 2);
    assert_eq!(
        next_state(&mut handle).await,
        TransportState::Backoff {
            attempt: 1,
            delay: expected_first
        }
    );
    assert_eq!(
        next_state(&mut handle).await,
        TransportState::Connecting { attempt: 2 }
    );
    assert_eq!(
        next_state(&mut handle).await,
        TransportState::Backoff {
            attempt: 2,
            delay: expected_second
        }
    );
    assert_eq!(
        next_state(&mut handle).await,
        TransportState::Connecting { attempt: 3 }
    );

    handle.shutdown().await;
    assert!(stub.request_count() >= 3);
}

#[tokio::test]
async fn reconnect_count_caps_attempts_and_degrades() {
    // Bootstrap succeeds but points at a closed port; ReconnectCount=2.
    let dead = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let dead_addr = dead.local_addr().expect("addr");
    drop(dead);
    let body = endpoint_body(
        dead_addr,
        r#""PingInterval":60,"ReconnectCount":2,"ReconnectInterval":1,"ReconnectNonce":0"#,
    );
    let stub = StubServer::start(Arc::new(move |_| StubResponse::json(200, &body))).await;
    let http = LarkHttp::new(endpoints_for(&stub)).expect("http client");
    let (handler, _seen) = ok_handler();
    let mut handle = LarkTransport::start(http, test_credentials(), handler);

    loop {
        match next_state(&mut handle).await {
            TransportState::Degraded { reason } => {
                assert!(reason.contains("exhausted"), "reason: {reason}");
                break;
            }
            TransportState::Stopped => panic!("stopped before degraded"),
            _ => {}
        }
    }
    // Exactly ReconnectCount bootstrap pulls, then no more.
    assert_eq!(stub.request_count(), 2);
    let before = stub.request_count();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(stub.request_count(), before, "no retries after Degraded");

    handle.shutdown().await;
}

#[tokio::test]
async fn permanent_bootstrap_codes_degrade_without_retry() {
    for code in [514, 403] {
        let body = format!(r#"{{"code":{code},"msg":"nope"}}"#);
        let stub = StubServer::start(Arc::new(move |_| StubResponse::json(200, &body))).await;
        let http = LarkHttp::new(endpoints_for(&stub)).expect("http client");
        let (handler, _seen) = ok_handler();
        let mut handle = LarkTransport::start(http, test_credentials(), handler);

        loop {
            match next_state(&mut handle).await {
                TransportState::Degraded { reason } => {
                    assert!(reason.contains("authentication"), "reason: {reason}");
                    break;
                }
                TransportState::Stopped => panic!("stopped before degraded"),
                _ => {}
            }
        }
        assert_eq!(stub.request_count(), 1, "no retry for code {code}");
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(stub.request_count(), 1);

        handle.shutdown().await;
    }
}

#[tokio::test]
async fn handshake_autherrcode_degrades_without_reconnect() {
    let (handler, _seen) = ok_handler();
    let (_stub, mut ws_server, mut handle) =
        start_transport(DEFAULT_CLIENT_CONFIG, handler, TransportConfig::default()).await;

    let mut conn = ws_server.accept().await;
    let _ping = conn.recv_frame().await;
    let mut frame = Frame::ping(7);
    frame.headers = vec![
        Header {
            key: header_key::HANDSHAKE_STATUS.to_owned(),
            value: "403".to_owned(),
        },
        Header {
            key: header_key::HANDSHAKE_MSG.to_owned(),
            value: "server text that must not leak".to_owned(),
        },
        Header {
            key: header_key::HANDSHAKE_AUTHERRCODE.to_owned(),
            value: "1000040351".to_owned(),
        },
    ];
    conn.send_frame(&frame).await;

    loop {
        match next_state(&mut handle).await {
            TransportState::Degraded { reason } => {
                assert!(reason.contains("1000040351"), "reason: {reason}");
                assert!(!reason.contains("must not leak"));
                break;
            }
            TransportState::Stopped => panic!("stopped before degraded"),
            _ => {}
        }
    }
    ws_server
        .assert_no_connection(Duration::from_millis(500))
        .await;

    handle.shutdown().await;
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

#[tokio::test]
async fn shutdown_closes_the_socket_and_joins_without_orphans() {
    let (handler, _seen) = ok_handler();
    let (_stub, mut ws_server, handle) =
        start_transport(DEFAULT_CLIENT_CONFIG, handler, TransportConfig::default()).await;

    let mut conn = ws_server.accept().await;
    let _ping = conn.recv_frame().await;

    let started = Instant::now();
    handle.shutdown().await;
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "shutdown is bounded"
    );

    // The server observes the closed socket promptly.
    let closed = timeout(TEST_TIMEOUT, async {
        loop {
            match conn.ws.next().await {
                Some(Ok(Message::Close(_)) | Err(_)) | None => break,
                Some(Ok(_)) => {}
            }
        }
    })
    .await;
    assert!(closed.is_ok(), "socket closed");
}
