use std::{
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use lark_codex_bridge::{
    codex::{
        external::{
            ExternalAuthentication, ExternalCapabilityProfile, ExternalEndpointConfig,
            ExternalEndpointGate,
        },
        external_transport::{ExternalReadOnlyConnection, ExternalTransportError},
        rpc::ConnectionEpoch,
        transport::{
            TransportExit, WebSocketCloseHandshake, WebSocketCloseInitiator, WebSocketCloseReport,
        },
        types::ThreadListParams,
    },
    limits::{EXTERNAL_WS_CLOSE_TIMEOUT, EXTERNAL_WS_MESSAGE_BYTES, RPC_INFLIGHT_CAPACITY},
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::Barrier, task::JoinHandle, time::timeout};
use tokio_tungstenite::{
    WebSocketStream, accept_hdr_async,
    tungstenite::{
        Message,
        handshake::server::{ErrorResponse, Request, Response},
        http::StatusCode,
        protocol::frame::{
            CloseFrame, Frame,
            coding::{CloseCode, Data, OpCode},
        },
    },
};
use tokio_util::sync::CancellationToken;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);
const TOKEN: &str = "transport-token-0123456789abcdef0123456789abcdef";
const SECRET_SENTINEL: &str = "TRANSPORT_SECRET_SENTINEL";

#[derive(Clone, Copy)]
enum Behavior {
    ReadThenClose,
    Binary,
    Malformed,
    OversizedText,
    DuplicateResponse,
    StaleResponse,
    UnknownNotification,
    FragmentedResponse,
    IncompleteFragment,
    PeerClose,
    ObserveAbort,
    DrainWithoutResponding,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ServerClose {
    handshake: WebSocketCloseHandshake,
    code: Option<u16>,
}

struct FakeExternalServer {
    endpoint: String,
    closes: Arc<Mutex<Vec<ServerClose>>>,
    runtime_requests: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl FakeExternalServer {
    #[allow(clippy::result_large_err)]
    async fn start(behaviors: Vec<Behavior>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("fake external listener binds");
        let address = listener.local_addr().expect("fake listener has address");
        let closes = Arc::new(Mutex::new(Vec::new()));
        let task_closes = Arc::clone(&closes);
        let runtime_requests = Arc::new(AtomicUsize::new(0));
        let task_requests = Arc::clone(&runtime_requests);
        let task = tokio::spawn(async move {
            for behavior in behaviors {
                let (stream, _) = listener.accept().await.expect("connection accepts");
                let mut socket = accept_authenticated(stream).await;
                serve_admission(&mut socket).await;
                serve_behavior(&mut socket, behavior, &task_closes, &task_requests).await;
            }
        });
        Self {
            endpoint: format!("ws://{address}/app-server"),
            closes,
            runtime_requests,
            task,
        }
    }

    fn runtime_request_count(&self) -> usize {
        self.runtime_requests.load(Ordering::Acquire)
    }

    async fn finish(self) -> Vec<ServerClose> {
        timeout(TEST_TIMEOUT, self.task)
            .await
            .expect("fake server finishes before timeout")
            .expect("fake server task succeeds");
        self.closes.lock().expect("close log lock").clone()
    }
}

#[allow(clippy::result_large_err)]
async fn accept_authenticated(
    stream: tokio::net::TcpStream,
) -> WebSocketStream<tokio::net::TcpStream> {
    accept_hdr_async(stream, |request: &Request, response: Response| {
        let authorized = request
            .headers()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value == format!("Bearer {TOKEN}"));
        if authorized {
            Ok(response)
        } else {
            let mut rejected = ErrorResponse::new(Some(SECRET_SENTINEL.to_owned()));
            *rejected.status_mut() = StatusCode::UNAUTHORIZED;
            Err(rejected)
        }
    })
    .await
    .expect("authorized client upgrades")
}

async fn serve_admission(socket: &mut WebSocketStream<tokio::net::TcpStream>) {
    let initialize = recv_json(socket).await;
    assert_eq!(initialize["id"], 1);
    assert_eq!(initialize["method"], "initialize");
    send_json(
        socket,
        json!({
            "id": 1,
            "result": {
                "codexHome": absolute_fake_home(),
                "platformFamily": "test",
                "platformOs": "test",
                "userAgent": "codex_cli_rs/0.149.0 (fake)"
            }
        }),
    )
    .await;
    assert_eq!(recv_json(socket).await, json!({"method": "initialized"}));
    let canary = recv_json(socket).await;
    assert_eq!(canary["id"], 2);
    assert_eq!(canary["method"], "thread/list");
    send_json(socket, json!({"id": 2, "result": {"data": []}})).await;
}

#[allow(clippy::too_many_lines)]
async fn serve_behavior(
    socket: &mut WebSocketStream<tokio::net::TcpStream>,
    behavior: Behavior,
    closes: &Arc<Mutex<Vec<ServerClose>>>,
    runtime_requests: &AtomicUsize,
) {
    match behavior {
        Behavior::ReadThenClose => {
            respond_to_runtime_list(socket).await;
            let observed = observe_close(socket).await;
            closes.lock().expect("close log lock").push(observed);
        }
        Behavior::Binary => {
            socket
                .send(Message::Binary(vec![0, 1, 2].into()))
                .await
                .expect("binary attack sends");
            let _ = timeout(TEST_TIMEOUT, socket.next()).await;
        }
        Behavior::Malformed => {
            socket
                .send(Message::Text(format!("{{\"{SECRET_SENTINEL}\"").into()))
                .await
                .expect("malformed attack sends");
            let _ = timeout(TEST_TIMEOUT, socket.next()).await;
        }
        Behavior::OversizedText => {
            let _ = socket
                .send(Message::Text(
                    "x".repeat(EXTERNAL_WS_MESSAGE_BYTES.saturating_add(1))
                        .into(),
                ))
                .await;
            let _ = timeout(TEST_TIMEOUT, socket.next()).await;
        }
        Behavior::DuplicateResponse => {
            let request = recv_json(socket).await;
            let response = json!({"id": request["id"].clone(), "result": {"data": []}});
            send_json(socket, response.clone()).await;
            send_json(socket, response).await;
            let _ = timeout(TEST_TIMEOUT, socket.next()).await;
        }
        Behavior::StaleResponse => {
            let request = recv_json(socket).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
            send_json(
                socket,
                json!({"id": request["id"].clone(), "result": {"data": []}}),
            )
            .await;
            let _ = timeout(TEST_TIMEOUT, socket.next()).await;
        }
        Behavior::UnknownNotification => {
            send_json(
                socket,
                json!({"method": "future/required", "params": {"secret": SECRET_SENTINEL}}),
            )
            .await;
            let _ = timeout(TEST_TIMEOUT, socket.next()).await;
        }
        Behavior::FragmentedResponse => {
            let request = recv_json(socket).await;
            let response = json!({"id": request["id"].clone(), "result": {"data": []}}).to_string();
            let split = response.len() / 2;
            socket
                .send(Message::Frame(Frame::message(
                    response.as_bytes()[..split].to_vec(),
                    OpCode::Data(Data::Text),
                    false,
                )))
                .await
                .expect("first fragment sends");
            socket
                .send(Message::Frame(Frame::message(
                    response.as_bytes()[split..].to_vec(),
                    OpCode::Data(Data::Continue),
                    true,
                )))
                .await
                .expect("final fragment sends");
            let observed = observe_close(socket).await;
            closes.lock().expect("close log lock").push(observed);
        }
        Behavior::IncompleteFragment => {
            socket
                .send(Message::Frame(Frame::message(
                    br#"{"id":"unfinished""#.to_vec(),
                    OpCode::Data(Data::Text),
                    false,
                )))
                .await
                .expect("unfinished fragment sends");
            let observed = observe_close(socket).await;
            closes.lock().expect("close log lock").push(observed);
        }
        Behavior::PeerClose => {
            socket
                .send(Message::Close(Some(CloseFrame {
                    code: CloseCode::Normal,
                    reason: "".into(),
                })))
                .await
                .expect("peer close sends");
            let observed = match timeout(TEST_TIMEOUT, socket.next()).await {
                Ok(Some(Ok(Message::Close(frame)))) => ServerClose {
                    handshake: WebSocketCloseHandshake::Complete,
                    code: frame.map(|frame| u16::from(frame.code)),
                },
                Ok(Some(Err(_) | Ok(_)) | None) | Err(_) => ServerClose {
                    handshake: WebSocketCloseHandshake::Incomplete,
                    code: None,
                },
            };
            closes.lock().expect("close log lock").push(observed);
        }
        Behavior::ObserveAbort => {
            let observed = observe_close(socket).await;
            closes.lock().expect("close log lock").push(observed);
        }
        Behavior::DrainWithoutResponding => loop {
            match timeout(TEST_TIMEOUT, socket.next()).await {
                Ok(Some(Ok(Message::Text(_)))) => {
                    runtime_requests.fetch_add(1, Ordering::AcqRel);
                }
                Ok(Some(Ok(Message::Close(frame)))) => {
                    let code = frame.map(|frame| u16::from(frame.code));
                    let _ = socket.flush().await;
                    closes.lock().expect("close log lock").push(ServerClose {
                        handshake: WebSocketCloseHandshake::Complete,
                        code,
                    });
                    break;
                }
                Ok(Some(Err(_) | Ok(_)) | None) | Err(_) => {
                    closes.lock().expect("close log lock").push(ServerClose {
                        handshake: WebSocketCloseHandshake::Incomplete,
                        code: None,
                    });
                    break;
                }
            }
        },
    }
}

async fn respond_to_runtime_list(socket: &mut WebSocketStream<tokio::net::TcpStream>) {
    let request = recv_json(socket).await;
    assert_eq!(request["method"], "thread/list");
    send_json(
        socket,
        json!({"id": request["id"].clone(), "result": {"data": []}}),
    )
    .await;
}

async fn observe_close(socket: &mut WebSocketStream<tokio::net::TcpStream>) -> ServerClose {
    match timeout(TEST_TIMEOUT, socket.next()).await {
        Ok(Some(Ok(Message::Close(frame)))) => {
            let code = frame.map(|frame| u16::from(frame.code));
            let _ = socket.flush().await;
            ServerClose {
                handshake: WebSocketCloseHandshake::Complete,
                code,
            }
        }
        Ok(Some(Err(_) | Ok(_)) | None) | Err(_) => ServerClose {
            handshake: WebSocketCloseHandshake::Incomplete,
            code: None,
        },
    }
}

async fn recv_json(socket: &mut WebSocketStream<tokio::net::TcpStream>) -> Value {
    let message = timeout(TEST_TIMEOUT, socket.next())
        .await
        .expect("client message arrives")
        .expect("socket remains open")
        .expect("client frame is valid");
    let Message::Text(text) = message else {
        panic!("client sends text RPC only");
    };
    serde_json::from_str(&text).expect("client text is JSON")
}

async fn send_json(socket: &mut WebSocketStream<tokio::net::TcpStream>, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .expect("fake response sends");
}

fn absolute_fake_home() -> String {
    std::env::temp_dir()
        .join("external-transport-fake-codex-home")
        .display()
        .to_string()
}

fn write_token(path: &Path) {
    std::fs::write(path, format!("{TOKEN}\n")).expect("token file writes");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = std::fs::metadata(path)
            .expect("token metadata reads")
            .permissions();
        permissions.set_mode(0o600);
        std::fs::set_permissions(path, permissions).expect("token permissions set");
    }
}

fn gate(endpoint: &str, token_path: &Path) -> ExternalEndpointGate {
    ExternalEndpointGate::new(ExternalEndpointConfig {
        endpoint: endpoint.to_owned(),
        expected_codex_version: "0.149.0".to_owned(),
        capability_profile: ExternalCapabilityProfile::ObserveShared,
        authentication: ExternalAuthentication::BearerTokenFile {
            path: token_path.to_path_buf(),
        },
    })
    .expect("test external gate is valid")
}

async fn connect(endpoint: &str, token_path: &Path, epoch: u64) -> ExternalReadOnlyConnection {
    ExternalReadOnlyConnection::connect(
        &gate(endpoint, token_path),
        ConnectionEpoch::new(epoch),
        CancellationToken::new(),
    )
    .await
    .expect("external read-only connection admits")
}

#[tokio::test]
async fn shutdown_abort_and_fresh_client_close_only_sockets_and_leave_server_available() {
    let server = FakeExternalServer::start(vec![
        Behavior::ReadThenClose,
        Behavior::ObserveAbort,
        Behavior::ObserveAbort,
        Behavior::ReadThenClose,
    ])
    .await;
    let scratch = tempfile::tempdir().expect("scratch creates");
    let token_path = scratch.path().join("bearer");
    write_token(&token_path);

    let mut first = connect(&server.endpoint, &token_path, 1).await;
    assert!(
        first
            .list_threads(&ThreadListParams::default())
            .await
            .expect("first read succeeds")
            .data
            .is_empty()
    );
    assert_eq!(
        first.shutdown().await,
        TransportExit::WebSocketClosed(WebSocketCloseReport {
            initiator: WebSocketCloseInitiator::Local,
            handshake: WebSocketCloseHandshake::Complete,
            code: Some(1000),
        })
    );

    let dropped = connect(&server.endpoint, &token_path, 2).await;
    drop(dropped);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut crashed = connect(&server.endpoint, &token_path, 3).await;
    assert_eq!(crashed.abort(), TransportExit::Aborted);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut fresh = connect(&server.endpoint, &token_path, 4).await;
    assert!(
        fresh
            .list_threads(&ThreadListParams::default())
            .await
            .expect("fresh client proves server survival")
            .data
            .is_empty()
    );
    assert!(matches!(
        fresh.shutdown().await,
        TransportExit::WebSocketClosed(WebSocketCloseReport {
            handshake: WebSocketCloseHandshake::Complete,
            ..
        })
    ));

    let closes = server.finish().await;
    assert_eq!(closes.len(), 4);
    assert_eq!(closes[0].handshake, WebSocketCloseHandshake::Complete);
    assert_eq!(closes[0].code, Some(1000));
    assert_eq!(closes[1].handshake, WebSocketCloseHandshake::Complete);
    assert_eq!(closes[2].handshake, WebSocketCloseHandshake::Incomplete);
    assert_eq!(closes[3].handshake, WebSocketCloseHandshake::Complete);
}

#[tokio::test]
async fn binary_malformed_and_unknown_required_messages_fault_the_epoch_without_leaks() {
    for (epoch, behavior) in [
        Behavior::Binary,
        Behavior::Malformed,
        Behavior::OversizedText,
        Behavior::UnknownNotification,
    ]
    .into_iter()
    .enumerate()
    {
        let server = FakeExternalServer::start(vec![behavior]).await;
        let scratch = tempfile::tempdir().expect("scratch creates");
        let token_path = scratch.path().join(format!("{SECRET_SENTINEL}-{epoch}"));
        write_token(&token_path);
        let mut connection = connect(&server.endpoint, &token_path, epoch as u64 + 10).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            connection.shutdown().await,
            TransportExit::ProtocolViolation
        );
        let debug = format!("{connection:?}");
        assert!(!debug.contains(SECRET_SENTINEL));
        server.finish().await;
    }
}

#[tokio::test]
async fn duplicate_and_stale_responses_fault_instead_of_completing_new_work() {
    let scratch = tempfile::tempdir().expect("scratch creates");
    let token_path = scratch.path().join("bearer");
    write_token(&token_path);

    let duplicate_server = FakeExternalServer::start(vec![Behavior::DuplicateResponse]).await;
    let mut duplicate = connect(&duplicate_server.endpoint, &token_path, 20).await;
    // The first frame is a valid response and can win the race with the immediately following
    // duplicate. The duplicate must still fault the epoch before any subsequent work completes.
    match duplicate.list_threads(&ThreadListParams::default()).await {
        Ok(result) => assert!(result.data.is_empty()),
        Err(ExternalTransportError::Rpc) => {}
        Err(error) => panic!("unexpected first duplicate-pair result: {error:?}"),
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(matches!(
        duplicate.list_threads(&ThreadListParams::default()).await,
        Err(ExternalTransportError::Rpc)
    ));
    assert_eq!(duplicate.shutdown().await, TransportExit::ProtocolViolation);
    duplicate_server.finish().await;

    let stale_server = FakeExternalServer::start(vec![Behavior::StaleResponse]).await;
    let mut stale = connect(&stale_server.endpoint, &token_path, 21).await;
    assert!(matches!(
        stale
            .list_threads_with_timeout(&ThreadListParams::default(), Duration::from_millis(20),)
            .await,
        Err(ExternalTransportError::Rpc)
    ));
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(stale.shutdown().await, TransportExit::ProtocolViolation);
    stale_server.finish().await;
}

#[tokio::test]
async fn peer_initiated_close_is_reported_separately_and_completes_the_handshake() {
    let server = FakeExternalServer::start(vec![Behavior::PeerClose]).await;
    let scratch = tempfile::tempdir().expect("scratch creates");
    let token_path = scratch.path().join("bearer");
    write_token(&token_path);

    let mut connection = connect(&server.endpoint, &token_path, 25).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        connection.shutdown().await,
        TransportExit::WebSocketClosed(WebSocketCloseReport {
            initiator: WebSocketCloseInitiator::Peer,
            handshake: WebSocketCloseHandshake::Complete,
            code: Some(1000),
        })
    );
    assert_eq!(
        server.finish().await,
        vec![ServerClose {
            handshake: WebSocketCloseHandshake::Complete,
            code: Some(1000),
        }]
    );
}

#[tokio::test]
async fn bounded_fragmentation_accepts_complete_text_and_cannot_block_shutdown() {
    let scratch = tempfile::tempdir().expect("scratch creates");
    let token_path = scratch.path().join("bearer");
    write_token(&token_path);

    let complete_server = FakeExternalServer::start(vec![Behavior::FragmentedResponse]).await;
    let mut complete = connect(&complete_server.endpoint, &token_path, 30).await;
    assert!(
        complete
            .list_threads(&ThreadListParams::default())
            .await
            .expect("bounded fragmented response reassembles")
            .data
            .is_empty()
    );
    assert!(matches!(
        complete.shutdown().await,
        TransportExit::WebSocketClosed(WebSocketCloseReport {
            handshake: WebSocketCloseHandshake::Complete,
            ..
        })
    ));
    complete_server.finish().await;

    let incomplete_server = FakeExternalServer::start(vec![Behavior::IncompleteFragment]).await;
    let mut incomplete = connect(&incomplete_server.endpoint, &token_path, 31).await;
    let started = tokio::time::Instant::now();
    let exit = incomplete.shutdown().await;
    assert!(
        matches!(
            exit,
            TransportExit::WebSocketClosed(WebSocketCloseReport { .. })
        ),
        "unexpected incomplete-fragment shutdown: {exit:?}"
    );
    assert!(
        started.elapsed() <= EXTERNAL_WS_CLOSE_TIMEOUT + Duration::from_secs(1),
        "an unfinished fragmented message must not make shutdown unbounded"
    );
    incomplete_server.finish().await;
}

#[tokio::test]
async fn pending_overload_is_count_bounded_and_every_waiter_has_a_deadline() {
    let server = FakeExternalServer::start(vec![Behavior::DrainWithoutResponding]).await;
    let scratch = tempfile::tempdir().expect("scratch creates");
    let token_path = scratch.path().join("bearer");
    write_token(&token_path);
    let connection = Arc::new(connect(&server.endpoint, &token_path, 40).await);

    let request_count = RPC_INFLIGHT_CAPACITY.saturating_mul(3);
    let start = Arc::new(Barrier::new(request_count.saturating_add(1)));
    let mut requests = Vec::new();
    for _ in 0..request_count {
        let connection = Arc::clone(&connection);
        let start = Arc::clone(&start);
        requests.push(tokio::spawn(async move {
            start.wait().await;
            connection
                .list_threads_with_timeout(&ThreadListParams::default(), Duration::from_secs(1))
                .await
        }));
    }
    start.wait().await;
    timeout(Duration::from_millis(500), async {
        while server.runtime_request_count() < RPC_INFLIGHT_CAPACITY {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the first bounded inflight window reaches the wire");
    assert_eq!(
        server.runtime_request_count(),
        RPC_INFLIGHT_CAPACITY,
        "wire-visible concurrent pending work must stop at the inflight cap"
    );

    timeout(Duration::from_secs(2), async {
        for request in requests {
            assert!(matches!(
                request.await.expect("request task does not panic"),
                Err(ExternalTransportError::Rpc)
            ));
        }
    })
    .await
    .expect("overloaded callers all finish under their deadlines");

    let mut connection = Arc::try_unwrap(connection).expect("request clones are dropped");
    assert!(matches!(
        connection.shutdown().await,
        TransportExit::WebSocketClosed(WebSocketCloseReport {
            handshake: WebSocketCloseHandshake::Complete,
            ..
        })
    ));
    server.finish().await;
}

#[test]
fn external_transport_source_has_no_process_ownership_import_or_operation() {
    let source = include_str!("../src/codex/external_transport.rs");
    for forbidden in [
        "codex::process",
        "std::process",
        "Command::new",
        "spawn_app_server",
        ".start_kill(",
        ".kill(",
        ".wait(",
        ".terminate(",
    ] {
        assert!(
            !source.contains(forbidden),
            "external transport must not contain process ownership operation {forbidden}"
        );
    }
}
