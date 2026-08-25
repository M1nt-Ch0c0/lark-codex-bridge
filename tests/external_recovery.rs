use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use lark_codex_bridge::{
    codex::{
        compat::WireAdapter,
        external::{
            ExternalAuthentication, ExternalCapabilityProfile, ExternalEndpointConfig,
            ExternalEndpointGate,
        },
        external_recovery::{
            ExternalRecoveryCoordinator, ExternalRecoverySettings, ExternalRecoveryState,
        },
        types::ThreadResumeParams,
    },
    limits::{EXTERNAL_RECONCILE_EVENT_CAPACITY, EXTERNAL_RECONCILE_PAGE_CAPACITY},
    store::{ExternalThreadState, ExternalUncertaintyReason, StoreHandle},
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::Notify, task::JoinHandle, time::timeout};
use tokio_tungstenite::{
    WebSocketStream, accept_hdr_async,
    tungstenite::{
        Message,
        handshake::server::{ErrorResponse, Request, Response},
        http::StatusCode,
    },
};
use tokio_util::sync::CancellationToken;

const TEST_TIMEOUT: Duration = Duration::from_secs(8);
const TOKEN: &str = "recovery-token-0123456789abcdef0123456789abcdef";
const THREAD_ID: &str = "thread-contract-1";

#[test]
fn resume_snapshot_request_promotes_only_exact_exclude_turns_field() {
    let mut params = ThreadResumeParams::new(THREAD_ID);
    params.overrides.exclude_turns = Some(true);
    assert_eq!(
        WireAdapter::V0_149_0
            .thread_resume_params(&params)
            .expect("promoted params"),
        json!({"threadId": THREAD_ID, "excludeTurns": true})
    );
    assert!(WireAdapter::V0_146_0.thread_resume_params(&params).is_err());
}

#[derive(Clone)]
enum SessionBehavior {
    DropAt(&'static str),
    HoldAt(&'static str),
    Healthy { duplicate_terminals: bool },
    Overflow,
    PageLimit,
    WrongThread,
    RemoteControlStatus(Arc<Notify>),
}

struct FakeRecoveryServer {
    endpoint: String,
    methods: Arc<Mutex<Vec<String>>>,
    task: JoinHandle<()>,
}

impl FakeRecoveryServer {
    #[allow(clippy::result_large_err)]
    async fn start(behaviors: Vec<SessionBehavior>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let methods = Arc::new(Mutex::new(Vec::new()));
        let task_methods = Arc::clone(&methods);
        let task = tokio::spawn(async move {
            for behavior in behaviors {
                let (stream, _) = listener.accept().await.expect("accept");
                let mut socket = accept_authenticated(stream).await;
                serve_admission(&mut socket).await;
                serve_session(&mut socket, behavior, &task_methods).await;
            }
        });
        Self {
            endpoint: format!("ws://{address}/app-server"),
            methods,
            task,
        }
    }

    async fn finish(self) -> Vec<String> {
        timeout(TEST_TIMEOUT, self.task)
            .await
            .expect("server finishes")
            .expect("server succeeds");
        self.methods.lock().expect("method log").clone()
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
            let mut rejected = ErrorResponse::new(None);
            *rejected.status_mut() = StatusCode::UNAUTHORIZED;
            Err(rejected)
        }
    })
    .await
    .expect("authenticated upgrade")
}

async fn serve_admission(socket: &mut WebSocketStream<tokio::net::TcpStream>) {
    let initialize = recv_json(socket).await;
    assert_eq!(initialize["method"], "initialize");
    assert_eq!(
        initialize["params"]["capabilities"]["experimentalApi"],
        true
    );
    send_result(socket, &initialize, contract_result("initialize")).await;
    assert_eq!(recv_json(socket).await, json!({"method": "initialized"}));
    let list = recv_json(socket).await;
    assert_eq!(list["method"], "thread/list");
    send_result(socket, &list, json!({"data": []})).await;
}

async fn serve_session(
    socket: &mut WebSocketStream<tokio::net::TcpStream>,
    behavior: SessionBehavior,
    methods: &Arc<Mutex<Vec<String>>>,
) {
    let mut turns_page = 0_usize;
    loop {
        let Some(request) = recv_json_or_close(socket).await else {
            return;
        };
        let method = request["method"].as_str().expect("request method");
        methods.lock().expect("method log").push(method.to_owned());
        assert!(
            matches!(
                method,
                "thread/resume" | "thread/read" | "thread/turns/list" | "thread/items/list"
            ),
            "recovery must never send write method {method}"
        );
        assert_eq!(request["params"]["threadId"], THREAD_ID);
        match method {
            "thread/resume" => assert_eq!(request["params"]["excludeTurns"], true),
            "thread/read" => assert_eq!(request["params"]["includeTurns"], false),
            "thread/turns/list" | "thread/items/list" => {
                assert_eq!(request["params"]["limit"], 100);
                assert_eq!(request["params"]["sortDirection"], "asc");
            }
            _ => unreachable!("method allowlist checked above"),
        }
        if matches!(behavior, SessionBehavior::DropAt(target) if method == target) {
            return;
        }
        if matches!(behavior, SessionBehavior::HoldAt(target) if method == target) {
            let _ = timeout(TEST_TIMEOUT, socket.next()).await;
            return;
        }
        if matches!(behavior, SessionBehavior::Overflow) && method == "thread/resume" {
            for _ in 0..=EXTERNAL_RECONCILE_EVENT_CAPACITY {
                send_json(socket, contract_notification("thread/status/changed")).await;
            }
        }
        if matches!(
            behavior,
            SessionBehavior::Healthy {
                duplicate_terminals: true
            }
        ) && method == "thread/read"
        {
            for _ in 0..2 {
                send_json(socket, contract_notification("item/completed")).await;
                send_json(socket, contract_notification("turn/completed")).await;
            }
        }
        let mut result = contract_result(method);
        if matches!(behavior, SessionBehavior::WrongThread) && method == "thread/read" {
            result["thread"]["id"] = Value::String("thread-cross-scope".to_owned());
        }
        if method == "thread/turns/list" {
            if matches!(behavior, SessionBehavior::PageLimit) {
                turns_page = turns_page.saturating_add(1);
                result["nextCursor"] = Value::String(format!("cursor-{turns_page}"));
            } else {
                result["data"][0]["status"] = Value::String("completed".to_owned());
                result["data"][0]["completedAt"] = json!(1_786_478_402_i64);
                result["data"][0]["items"] = json!([{
                    "id": "item-contract-1",
                    "phase": "final_answer",
                    "text": "bounded",
                    "type": "agentMessage"
                }]);
            }
        }
        send_result(socket, &request, result).await;
        if let (SessionBehavior::RemoteControlStatus(signal), "thread/items/list") =
            (&behavior, method)
        {
            signal.notified().await;
            send_json(socket, remote_control_notification()).await;
            let _ = timeout(TEST_TIMEOUT, socket.next()).await;
            return;
        }
    }
}

async fn seeded_coordinator(
    endpoint: &str,
    token_path: &Path,
) -> (StoreHandle, ExternalRecoveryCoordinator) {
    seeded_coordinator_with_settings(
        endpoint,
        token_path,
        ExternalRecoverySettings {
            request_timeout: Duration::from_millis(300),
            reconnect_initial_delay: Duration::from_millis(10),
            reconnect_max_delay: Duration::from_millis(40),
        },
    )
    .await
}

async fn seeded_coordinator_with_settings(
    endpoint: &str,
    token_path: &Path,
    settings: ExternalRecoverySettings,
) -> (StoreHandle, ExternalRecoveryCoordinator) {
    let store = StoreHandle::open_in_memory().await.expect("store");
    let gate = recovery_gate(endpoint, token_path);
    store
        .reserve_external_epoch(
            gate.endpoint_label().as_str(),
            ExternalUncertaintyReason::BridgeRestart,
        )
        .await
        .expect("seed epoch");
    store
        .register_external_thread(gate.endpoint_label().as_str(), THREAD_ID)
        .await
        .expect("seed managed thread");
    let coordinator =
        ExternalRecoveryCoordinator::start(gate, store.clone(), CancellationToken::new(), settings)
            .expect("coordinator");
    (store, coordinator)
}

#[tokio::test]
async fn request_timeout_is_persisted_as_unavailable_without_fabricating_completion() {
    let server = FakeRecoveryServer::start(vec![SessionBehavior::HoldAt("thread/read")]).await;
    let scratch = tempfile::tempdir().expect("scratch");
    let token_path = scratch.path().join("bearer");
    write_token(&token_path);
    let (store, coordinator) = seeded_coordinator_with_settings(
        &server.endpoint,
        &token_path,
        ExternalRecoverySettings {
            request_timeout: Duration::from_millis(100),
            reconnect_initial_delay: Duration::from_secs(2),
            reconnect_max_delay: Duration::from_secs(2),
        },
    )
    .await;
    let mut states = coordinator.subscribe_state();
    timeout(TEST_TIMEOUT, async {
        loop {
            if matches!(
                *states.borrow_and_update(),
                lark_codex_bridge::codex::external_recovery::ExternalRecoveryState::Unavailable {
                    reason: ExternalUncertaintyReason::RequestTimeout,
                    ..
                }
            ) {
                break;
            }
            states.changed().await.expect("recovery state remains open");
        }
    })
    .await
    .expect("request timeout becomes observable");
    let snapshot = coordinator
        .thread_snapshot(THREAD_ID)
        .await
        .expect("snapshot")
        .expect("managed");
    assert_eq!(snapshot.state, ExternalThreadState::Unavailable);
    assert_eq!(
        snapshot.reason,
        Some(ExternalUncertaintyReason::RequestTimeout)
    );
    assert!(snapshot.terminal_turns.is_empty());
    assert!(snapshot.terminal_items.is_empty());
    coordinator.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
    assert_read_only(server.finish().await);
}

#[tokio::test]
async fn disconnect_at_every_reconciliation_request_boundary_recovers_on_a_new_epoch() {
    for boundary in [
        "thread/resume",
        "thread/read",
        "thread/turns/list",
        "thread/items/list",
    ] {
        let server = FakeRecoveryServer::start(vec![
            SessionBehavior::DropAt(boundary),
            SessionBehavior::Healthy {
                duplicate_terminals: false,
            },
        ])
        .await;
        let scratch = tempfile::tempdir().expect("scratch");
        let token_path = scratch.path().join("bearer");
        write_token(&token_path);
        let (store, coordinator) = seeded_coordinator(&server.endpoint, &token_path).await;
        let ready_epoch = coordinator
            .wait_for_ready_after(0, TEST_TIMEOUT)
            .await
            .expect("recovered ready");
        assert!(
            ready_epoch >= 3,
            "disconnect must advance the durable epoch"
        );
        let snapshot = coordinator
            .thread_snapshot(THREAD_ID)
            .await
            .expect("snapshot")
            .expect("managed");
        assert_eq!(snapshot.epoch, ready_epoch);
        assert_eq!(snapshot.state, ExternalThreadState::Ready);
        assert_eq!(snapshot.terminal_turns.len(), 1);
        assert_eq!(snapshot.terminal_items.len(), 1);
        coordinator.shutdown().await.expect("shutdown");
        store.shutdown().await.expect("store shutdown");
        assert_read_only(server.finish().await);
    }
}

#[tokio::test]
async fn duplicate_snapshot_and_notifications_fold_once_by_stable_ids() {
    let server = FakeRecoveryServer::start(vec![SessionBehavior::Healthy {
        duplicate_terminals: true,
    }])
    .await;
    let scratch = tempfile::tempdir().expect("scratch");
    let token_path = scratch.path().join("bearer");
    write_token(&token_path);
    let (store, coordinator) = seeded_coordinator(&server.endpoint, &token_path).await;
    coordinator
        .wait_for_ready_after(0, TEST_TIMEOUT)
        .await
        .expect("ready");
    let snapshot = coordinator
        .thread_snapshot(THREAD_ID)
        .await
        .expect("snapshot")
        .expect("managed");
    assert_eq!(snapshot.terminal_turns.len(), 1);
    assert_eq!(snapshot.terminal_items.len(), 1);
    coordinator.shutdown().await.expect("shutdown");
    let stopped = store
        .external_thread_snapshot(
            recovery_gate(&server.endpoint, &token_path)
                .endpoint_label()
                .as_str(),
            THREAD_ID,
        )
        .await
        .expect("stopped snapshot")
        .expect("managed");
    assert_eq!(stopped.state, ExternalThreadState::Unavailable);
    assert_eq!(
        stopped.reason,
        Some(ExternalUncertaintyReason::BridgeRestart)
    );
    assert_eq!(stopped.terminal_turns.len(), 1);
    assert_eq!(stopped.terminal_items.len(), 1);
    store.shutdown().await.expect("store shutdown");
    assert_read_only(server.finish().await);
}

#[tokio::test]
async fn notification_overflow_marks_thread_uncertain_without_fabricating_completion() {
    let server = FakeRecoveryServer::start(vec![SessionBehavior::Overflow]).await;
    let scratch = tempfile::tempdir().expect("scratch");
    let token_path = scratch.path().join("bearer");
    write_token(&token_path);
    let (store, coordinator) = seeded_coordinator(&server.endpoint, &token_path).await;
    coordinator
        .wait_for_ready_after(0, TEST_TIMEOUT)
        .await
        .expect("endpoint ready with fenced thread");
    let snapshot = coordinator
        .thread_snapshot(THREAD_ID)
        .await
        .expect("snapshot")
        .expect("managed");
    assert_eq!(snapshot.state, ExternalThreadState::Uncertain);
    assert_eq!(
        snapshot.reason,
        Some(ExternalUncertaintyReason::BufferOverflow)
    );
    assert!(snapshot.terminal_turns.is_empty());
    assert!(snapshot.terminal_items.is_empty());
    coordinator.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
    assert_read_only(server.finish().await);
}

#[tokio::test]
async fn pagination_limit_marks_thread_uncertain_and_stops_before_unbounded_items_read() {
    let server = FakeRecoveryServer::start(vec![SessionBehavior::PageLimit]).await;
    let scratch = tempfile::tempdir().expect("scratch");
    let token_path = scratch.path().join("bearer");
    write_token(&token_path);
    let (store, coordinator) = seeded_coordinator(&server.endpoint, &token_path).await;
    coordinator
        .wait_for_ready_after(0, TEST_TIMEOUT)
        .await
        .expect("endpoint ready with fenced thread");
    let snapshot = coordinator
        .thread_snapshot(THREAD_ID)
        .await
        .expect("snapshot")
        .expect("managed");
    assert_eq!(snapshot.state, ExternalThreadState::Uncertain);
    assert_eq!(snapshot.reason, Some(ExternalUncertaintyReason::PageLimit));
    coordinator.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
    let methods = server.finish().await;
    assert_eq!(
        methods
            .iter()
            .filter(|method| method.as_str() == "thread/turns/list")
            .count(),
        EXTERNAL_RECONCILE_PAGE_CAPACITY
    );
    assert!(!methods.iter().any(|method| method == "thread/items/list"));
    assert_read_only(methods);
}

#[tokio::test]
async fn cross_thread_snapshot_is_fenced_as_protocol_uncertainty() {
    let server = FakeRecoveryServer::start(vec![SessionBehavior::WrongThread]).await;
    let scratch = tempfile::tempdir().expect("scratch");
    let token_path = scratch.path().join("bearer");
    write_token(&token_path);
    let (store, coordinator) = seeded_coordinator(&server.endpoint, &token_path).await;
    coordinator
        .wait_for_ready_after(0, TEST_TIMEOUT)
        .await
        .expect("endpoint ready with fenced thread");
    let snapshot = coordinator
        .thread_snapshot(THREAD_ID)
        .await
        .expect("snapshot")
        .expect("managed");
    assert_eq!(snapshot.state, ExternalThreadState::Uncertain);
    assert_eq!(
        snapshot.reason,
        Some(ExternalUncertaintyReason::ProtocolViolation)
    );
    assert!(snapshot.terminal_turns.is_empty());
    assert!(snapshot.terminal_items.is_empty());
    coordinator.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
    assert_read_only(server.finish().await);
}

#[tokio::test]
async fn ready_state_ignores_remote_control_status_notifications() {
    let signal = Arc::new(Notify::new());
    let server = FakeRecoveryServer::start(vec![SessionBehavior::RemoteControlStatus(Arc::clone(
        &signal,
    ))])
    .await;
    let scratch = tempfile::tempdir().expect("scratch");
    let token_path = scratch.path().join("bearer");
    write_token(&token_path);
    let (store, coordinator) = seeded_coordinator(&server.endpoint, &token_path).await;
    let ready_epoch = coordinator
        .wait_for_ready_after(0, TEST_TIMEOUT)
        .await
        .expect("ready");
    let mut states = coordinator.subscribe_state();
    signal.notify_one();
    let churned = timeout(Duration::from_millis(500), async {
        loop {
            states.changed().await.expect("recovery state remains open");
            let state = *states.borrow_and_update();
            if !matches!(state, ExternalRecoveryState::Ready { epoch } if epoch == ready_epoch) {
                break;
            }
        }
    })
    .await;
    assert!(
        churned.is_err(),
        "remoteControl/status/changed must not fence the ready epoch"
    );
    let snapshot = coordinator
        .thread_snapshot(THREAD_ID)
        .await
        .expect("snapshot")
        .expect("managed");
    assert_eq!(snapshot.epoch, ready_epoch);
    assert_eq!(snapshot.state, ExternalThreadState::Ready);
    coordinator.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
    assert_read_only(server.finish().await);
}

#[test]
fn recovery_source_has_no_process_or_write_ownership_surface() {
    let source = include_str!("../src/codex/external_recovery.rs");
    for forbidden in [
        "codex::process",
        "std::process",
        "Command::new",
        "spawn_app_server",
        "thread/start",
        "turn/start",
        "turn/steer",
        "turn/interrupt",
        "thread/queue",
        "requestApproval",
    ] {
        assert!(
            !source.contains(forbidden),
            "recovery source must not own or replay {forbidden}"
        );
    }
}

fn assert_read_only(methods: Vec<String>) {
    assert!(methods.into_iter().all(|method| matches!(
        method.as_str(),
        "thread/resume" | "thread/read" | "thread/turns/list" | "thread/items/list"
    )));
}

fn recovery_gate(endpoint: &str, token_path: &Path) -> ExternalEndpointGate {
    ExternalEndpointGate::new(ExternalEndpointConfig {
        endpoint: endpoint.to_owned(),
        expected_codex_version: "0.149.0".to_owned(),
        capability_profile: ExternalCapabilityProfile::ResumeShared,
        authentication: ExternalAuthentication::BearerTokenFile {
            path: token_path.to_path_buf(),
        },
    })
    .expect("valid recovery gate")
}

fn contract() -> Value {
    serde_json::from_str(include_str!("../protocol/codex/contracts/0.149.0.json"))
        .expect("contract")
}

fn contract_result(method: &str) -> Value {
    contract()["exchanges"]
        .as_array()
        .expect("exchanges")
        .iter()
        .find(|exchange| exchange["method"] == method)
        .unwrap_or_else(|| panic!("contract exchange {method}"))["result"]
        .clone()
}

fn contract_notification(method: &str) -> Value {
    contract()["notifications"]
        .as_array()
        .expect("notifications")
        .iter()
        .find(|notification| notification["method"] == method)
        .unwrap_or_else(|| panic!("contract notification {method}"))
        .clone()
}

fn remote_control_notification() -> Value {
    json!({
        "method": "remoteControl/status/changed",
        "params": {
            "environmentId": null,
            "installationId": "installation-contract-1",
            "serverName": "contract-server",
            "status": "ready"
        }
    })
}

async fn recv_json(socket: &mut WebSocketStream<tokio::net::TcpStream>) -> Value {
    recv_json_or_close(socket)
        .await
        .expect("client request before close")
}

async fn recv_json_or_close(socket: &mut WebSocketStream<tokio::net::TcpStream>) -> Option<Value> {
    loop {
        let message = timeout(TEST_TIMEOUT, socket.next())
            .await
            .expect("client activity")?;
        match message.expect("valid client frame") {
            Message::Text(text) => {
                return Some(serde_json::from_str(&text).expect("client JSON"));
            }
            Message::Close(frame) => {
                let _ = socket.send(Message::Close(frame)).await;
                return None;
            }
            Message::Ping(payload) => {
                socket.send(Message::Pong(payload)).await.expect("pong");
            }
            _ => {}
        }
    }
}

async fn send_result(
    socket: &mut WebSocketStream<tokio::net::TcpStream>,
    request: &Value,
    result: Value,
) {
    send_json(
        socket,
        json!({"id": request["id"].clone(), "result": result}),
    )
    .await;
}

async fn send_json(socket: &mut WebSocketStream<tokio::net::TcpStream>, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .expect("server JSON sends");
}

fn write_token(path: &Path) {
    std::fs::write(path, format!("{TOKEN}\n")).expect("token");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = std::fs::metadata(path).expect("metadata").permissions();
        permissions.set_mode(0o600);
        std::fs::set_permissions(path, permissions).expect("permissions");
    }
}
