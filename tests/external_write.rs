use std::{
    collections::HashMap,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
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
        external_write::{
            ExternalApprovalDecision, ExternalApprovalPromptKind, ExternalMutationApplied,
            ExternalWriteCoordinator, ExternalWriteError, ExternalWriteSettings,
        },
        types::{
            CommandExecutionApprovalDecision, CommandExecutionRequestApprovalResult,
            SimpleApprovalDecision, ThreadQueueAddParams, ThreadQueueStartParams,
            TurnInterruptParams, TurnStartParams, TurnSteerParams, UserInput,
        },
    },
    config::{BridgeConfig, CodexSection, ConcurrencyConfig, PathsSection, WorkspacePolicy},
    lark::{
        api::ChatMode,
        normalize::{InboundEvent, ScopeKey},
    },
    limits::{EXTERNAL_WRITE_SHUTDOWN_TIMEOUT, EXTERNAL_WS_CLOSE_TIMEOUT, EXTERNAL_WS_IO_TIMEOUT},
    runtime::policy::{AccessPolicy, AuthorizedLarkActor},
    store::{
        ExternalApprovalState, ExternalEndpointState, ExternalMutationState,
        ExternalUncertaintyReason, StoreHandle,
    },
};
use serde_json::{Value, json};
use tokio::{
    net::TcpListener,
    sync::{Mutex, Notify, Semaphore, mpsc},
    task::{JoinHandle, JoinSet},
    time::timeout,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, accept_hdr_async, connect_async,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        handshake::server::{ErrorResponse, Request, Response},
        http::{HeaderValue, StatusCode, header::AUTHORIZATION},
    },
};
use tokio_util::sync::CancellationToken;

const TEST_TIMEOUT: Duration = Duration::from_secs(8);
const REQUEST_TIMEOUT: Duration = Duration::from_millis(300);
const TOKEN: &str = "write-token-0123456789abcdef0123456789abcdef";
const THREAD_ID: &str = "thread-contract-1";
const APPROVAL_REVIEWER: &str = "user";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SessionRole {
    Bridge,
    Operator,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Failure {
    Hold(&'static str),
    Disconnect(&'static str),
}

#[derive(Clone)]
struct Queued {
    id: String,
    client_id: String,
    input: Value,
}

#[derive(Default)]
struct LiveState {
    active_turn: Option<String>,
    messages: HashMap<String, Vec<String>>,
    queue: Vec<Queued>,
    next_turn: usize,
    next_queue: usize,
    methods: Vec<(SessionRole, String)>,
}

struct SharedServer {
    live: Mutex<LiveState>,
    bridge_push: Mutex<Option<mpsc::UnboundedSender<Value>>>,
    bridge_push_changed: Notify,
    approval_responses: Mutex<Vec<Value>>,
    approval_response_changed: Notify,
    failure: Mutex<Option<Failure>>,
    pause_bridge_start: AtomicBool,
    bridge_start_seen: Semaphore,
    release_bridge_start: Semaphore,
    pause_bridge_steer: AtomicBool,
    bridge_steer_seen: Semaphore,
    release_bridge_steer: Semaphore,
    omit_bridge_steer_message: AtomicBool,
}

impl Default for SharedServer {
    fn default() -> Self {
        Self {
            live: Mutex::new(LiveState::default()),
            bridge_push: Mutex::new(None),
            bridge_push_changed: Notify::new(),
            approval_responses: Mutex::new(Vec::new()),
            approval_response_changed: Notify::new(),
            failure: Mutex::new(None),
            pause_bridge_start: AtomicBool::new(false),
            bridge_start_seen: Semaphore::new(0),
            release_bridge_start: Semaphore::new(0),
            pause_bridge_steer: AtomicBool::new(false),
            bridge_steer_seen: Semaphore::new(0),
            release_bridge_steer: Semaphore::new(0),
            omit_bridge_steer_message: AtomicBool::new(false),
        }
    }
}

struct FakeWriteServer {
    endpoint: String,
    shared: Arc<SharedServer>,
    cancellation: CancellationToken,
    task: JoinHandle<()>,
}

impl FakeWriteServer {
    #[allow(clippy::result_large_err)]
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let address = listener.local_addr().expect("address");
        let shared = Arc::new(SharedServer::default());
        let task_shared = Arc::clone(&shared);
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            let mut sessions = JoinSet::new();
            loop {
                tokio::select! {
                    () = task_cancellation.cancelled() => break,
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { break };
                        let session_shared = Arc::clone(&task_shared);
                        let session_cancel = task_cancellation.clone();
                        sessions.spawn(async move {
                            serve_connection(stream, session_shared, session_cancel).await;
                        });
                    }
                }
            }
            sessions.abort_all();
            while sessions.join_next().await.is_some() {}
        });
        Self {
            endpoint: format!("ws://{address}/app-server"),
            shared,
            cancellation,
            task,
        }
    }

    fn gate(&self, token_path: &Path) -> ExternalEndpointGate {
        write_gate(&self.endpoint, token_path)
    }

    async fn set_failure(&self, failure: Failure) {
        *self.shared.failure.lock().await = Some(failure);
    }

    async fn push_bridge(&self, value: Value) {
        timeout(TEST_TIMEOUT, async {
            loop {
                let changed = self.shared.bridge_push_changed.notified();
                if let Some(sender) = self.shared.bridge_push.lock().await.as_ref() {
                    sender.send(value.clone()).expect("bridge push channel");
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("bridge session registers");
    }

    async fn approval_responses(&self, expected: usize) -> Vec<Value> {
        timeout(TEST_TIMEOUT, async {
            loop {
                let changed = self.shared.approval_response_changed.notified();
                let responses = self.shared.approval_responses.lock().await.clone();
                if responses.len() >= expected {
                    return responses;
                }
                changed.await;
            }
        })
        .await
        .expect("approval response arrives")
    }

    async fn method_count(&self, role: SessionRole, method: &str) -> usize {
        self.shared
            .live
            .lock()
            .await
            .methods
            .iter()
            .filter(|(found_role, found_method)| *found_role == role && found_method == method)
            .count()
    }

    async fn finish(self) {
        self.cancellation.cancel();
        timeout(TEST_TIMEOUT, self.task)
            .await
            .expect("server stops")
            .expect("server task");
    }
}

#[allow(clippy::result_large_err)]
async fn serve_connection(
    stream: tokio::net::TcpStream,
    shared: Arc<SharedServer>,
    cancellation: CancellationToken,
) {
    let Ok(mut socket) = accept_hdr_async(stream, |request: &Request, response: Response| {
        let authorized = request
            .headers()
            .get(AUTHORIZATION)
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
    else {
        return;
    };
    let Some(initialize) = recv_json_or_close(&mut socket).await else {
        return;
    };
    let role = if initialize["params"]["clientInfo"]["name"] == "operator-test" {
        SessionRole::Operator
    } else {
        SessionRole::Bridge
    };
    send_result(&mut socket, &initialize, contract_result("initialize")).await;
    if recv_json_or_close(&mut socket).await != Some(json!({"method": "initialized"})) {
        return;
    }
    let Some(list) = recv_json_or_close(&mut socket).await else {
        return;
    };
    if list["method"] != "thread/list" {
        return;
    }
    send_result(&mut socket, &list, json!({"data": []})).await;
    let (push_tx, mut push_rx) = mpsc::unbounded_channel();
    if role == SessionRole::Bridge {
        *shared.bridge_push.lock().await = Some(push_tx);
        shared.bridge_push_changed.notify_waiters();
    }
    loop {
        tokio::select! {
            () = cancellation.cancelled() => return,
            pushed = push_rx.recv() => {
                let Some(value) = pushed else { return };
                if send_json_checked(&mut socket, value).await.is_err() {
                    return;
                }
            }
            message = socket.next() => {
                let Some(Ok(message)) = message else { return };
                match message {
                    Message::Text(text) => {
                        let Ok(value) = serde_json::from_str::<Value>(&text) else { return };
                        if value.get("method").is_none() {
                            shared.approval_responses.lock().await.push(value);
                            shared.approval_response_changed.notify_waiters();
                            continue;
                        }
                        if !handle_request(&mut socket, &shared, role, value, &cancellation).await {
                            return;
                        }
                    }
                    Message::Close(frame) => {
                        let _ = socket.send(Message::Close(frame)).await;
                        return;
                    }
                    Message::Ping(payload) => {
                        let _ = socket.send(Message::Pong(payload)).await;
                    }
                    _ => return,
                }
            }
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn handle_request(
    socket: &mut WebSocketStream<tokio::net::TcpStream>,
    shared: &SharedServer,
    role: SessionRole,
    request: Value,
    cancellation: &CancellationToken,
) -> bool {
    let Some(method) = request["method"].as_str() else {
        return false;
    };
    shared
        .live
        .lock()
        .await
        .methods
        .push((role, method.to_owned()));
    let failure = {
        let mut failure = shared.failure.lock().await;
        if failure.is_some_and(|failure| match failure {
            Failure::Hold(target) | Failure::Disconnect(target) => {
                role == SessionRole::Bridge && method == target
            }
        }) {
            failure.take()
        } else {
            None
        }
    };
    match failure {
        Some(Failure::Disconnect(_)) => return false,
        Some(Failure::Hold(_)) => {
            cancellation.cancelled().await;
            return false;
        }
        None => {}
    }
    if role == SessionRole::Bridge
        && method == "turn/start"
        && shared.pause_bridge_start.swap(false, Ordering::AcqRel)
    {
        shared.bridge_start_seen.add_permits(1);
        let Ok(permit) = shared.release_bridge_start.acquire().await else {
            return false;
        };
        permit.forget();
    }
    if role == SessionRole::Bridge
        && method == "turn/steer"
        && shared.pause_bridge_steer.swap(false, Ordering::AcqRel)
    {
        shared.bridge_steer_seen.add_permits(1);
        let Ok(permit) = shared.release_bridge_steer.acquire().await else {
            return false;
        };
        permit.forget();
    }
    let mut live = shared.live.lock().await;
    let result = match method {
        "thread/resume" => Ok(resume_result(&live)),
        "thread/read" => Ok(json!({"thread": thread_value(&live)})),
        "thread/turns/list" => Ok(turns_result(&live)),
        "thread/items/list" => Ok(items_result(&live, &request["params"])),
        "thread/queue/list" => Ok(queue_result(&live)),
        "turn/start" => start_turn_result(&mut live, &request["params"]),
        "turn/steer" => steer_turn_result(shared, role, &mut live, &request["params"]),
        "turn/interrupt" => interrupt_result(&mut live, &request["params"]),
        "thread/queue/add" => queue_add_result(&mut live, &request["params"]),
        "thread/queue/start" => queue_start_result(&mut live, &request["params"]),
        _ => Err(()),
    };
    drop(live);
    match result {
        Ok(result) => send_result_checked(socket, &request, result).await.is_ok(),
        Err(()) => send_error_checked(socket, &request).await.is_ok(),
    }
}

fn resume_result(live: &LiveState) -> Value {
    let mut result = contract_result("thread/resume");
    result["thread"] = thread_value(live);
    result
}

fn thread_value(live: &LiveState) -> Value {
    let mut thread = contract_result("thread/read")["thread"].clone();
    thread["status"] = if live.active_turn.is_some() {
        json!({"type": "active"})
    } else {
        json!({"type": "idle"})
    };
    thread["turns"] = json!([]);
    thread
}

fn turn_value(turn_id: &str) -> Value {
    let mut turn = contract_result("turn/start")["turn"].clone();
    turn["id"] = Value::String(turn_id.to_owned());
    turn["status"] = Value::String("inProgress".to_owned());
    turn
}

fn turns_result(live: &LiveState) -> Value {
    let data = live
        .active_turn
        .as_deref()
        .map(turn_value)
        .into_iter()
        .collect::<Vec<_>>();
    json!({"data": data, "nextCursor": null, "backwardsCursor": null})
}

fn items_result(live: &LiveState, params: &Value) -> Value {
    let turn_id = params["turnId"].as_str().unwrap_or_default();
    let data = live
        .messages
        .get(turn_id)
        .into_iter()
        .flatten()
        .enumerate()
        .map(|(index, client_id)| {
            json!({
                "turnId": turn_id,
                "item": {
                    "type": "userMessage",
                    "id": format!("user-item-{index}"),
                    "content": [{"type": "text", "text": "bounded"}],
                    "clientId": client_id,
                }
            })
        })
        .collect::<Vec<_>>();
    json!({"data": data, "nextCursor": null, "backwardsCursor": null})
}

fn queue_result(live: &LiveState) -> Value {
    let data = live
        .queue
        .iter()
        .map(|queued| {
            json!({
                "id": queued.id,
                "clientUserMessageId": queued.client_id,
                "input": queued.input,
            })
        })
        .collect::<Vec<_>>();
    json!({"data": data, "nextCursor": null})
}

fn start_turn_result(live: &mut LiveState, params: &Value) -> Result<Value, ()> {
    if live.active_turn.is_some() {
        return Err(());
    }
    live.next_turn = live.next_turn.saturating_add(1);
    let turn_id = format!("turn-live-{}", live.next_turn);
    let client_id = params["clientUserMessageId"].as_str().ok_or(())?;
    live.messages
        .entry(turn_id.clone())
        .or_default()
        .push(client_id.to_owned());
    live.active_turn = Some(turn_id.clone());
    Ok(json!({"turn": turn_value(&turn_id)}))
}

fn steer_turn_result(
    shared: &SharedServer,
    role: SessionRole,
    live: &mut LiveState,
    params: &Value,
) -> Result<Value, ()> {
    let expected = params["expectedTurnId"].as_str().ok_or(())?;
    if live.active_turn.as_deref() != Some(expected) {
        return Err(());
    }
    let omit = role == SessionRole::Bridge
        && shared
            .omit_bridge_steer_message
            .swap(false, Ordering::AcqRel);
    if !omit {
        let client_id = params["clientUserMessageId"].as_str().ok_or(())?;
        live.messages
            .entry(expected.to_owned())
            .or_default()
            .push(client_id.to_owned());
    }
    Ok(json!({"turnId": expected}))
}

fn interrupt_result(live: &mut LiveState, params: &Value) -> Result<Value, ()> {
    let turn_id = params["turnId"].as_str().ok_or(())?;
    if live.active_turn.as_deref() != Some(turn_id) {
        return Err(());
    }
    live.active_turn = None;
    Ok(json!({}))
}

fn queue_add_result(live: &mut LiveState, params: &Value) -> Result<Value, ()> {
    live.next_queue = live.next_queue.saturating_add(1);
    let queued = Queued {
        id: format!("queued-live-{}", live.next_queue),
        client_id: params["clientUserMessageId"].as_str().ok_or(())?.to_owned(),
        input: params["input"].clone(),
    };
    let result = json!({
        "queuedSubmission": {
            "id": queued.id,
            "clientUserMessageId": queued.client_id,
            "input": queued.input,
        }
    });
    live.queue.push(queued);
    Ok(result)
}

fn queue_start_result(live: &mut LiveState, params: &Value) -> Result<Value, ()> {
    if live.active_turn.is_some() {
        return Err(());
    }
    let queued_id = params["queuedSubmissionId"].as_str().ok_or(())?;
    let index = live
        .queue
        .iter()
        .position(|queued| queued.id == queued_id)
        .ok_or(())?;
    live.queue.remove(index);
    live.next_turn = live.next_turn.saturating_add(1);
    let turn_id = format!("turn-live-{}", live.next_turn);
    live.active_turn = Some(turn_id.clone());
    Ok(json!({"turn": turn_value(&turn_id)}))
}

type OperatorSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

async fn connect_operator(endpoint: &str) -> OperatorSocket {
    let mut request = endpoint
        .into_client_request()
        .expect("operator request builds");
    request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {TOKEN}")).expect("authorization header"),
    );
    let (mut socket, _) = connect_async(request).await.expect("operator connects");
    send_json(
        &mut socket,
        json!({
            "id": 1,
            "method": "initialize",
            "params": {
                "clientInfo": {"name": "operator-test", "version": "1"},
                "capabilities": {"experimentalApi": true}
            }
        }),
    )
    .await;
    assert_eq!(recv_json(&mut socket).await["id"], 1);
    send_json(&mut socket, json!({"method": "initialized"})).await;
    send_json(
        &mut socket,
        json!({"id": 2, "method": "thread/list", "params": {"limit": 1}}),
    )
    .await;
    assert_eq!(recv_json(&mut socket).await["id"], 2);
    socket
}

async fn operator_request(
    socket: &mut OperatorSocket,
    id: i64,
    method: &str,
    params: Value,
) -> Value {
    send_json(
        socket,
        json!({"id": id, "method": method, "params": params}),
    )
    .await;
    let response = recv_json(socket).await;
    assert_eq!(response["id"], id);
    response
}

async fn seeded_coordinator(
    server: &FakeWriteServer,
    token_path: &Path,
    source: &AuthorizedLarkActor,
    recipient: &AuthorizedLarkActor,
) -> (StoreHandle, ExternalWriteCoordinator, String) {
    let store = StoreHandle::open_in_memory().await.expect("store");
    let gate = server.gate(token_path);
    let label = gate.endpoint_label().as_str().to_owned();
    store
        .reserve_external_epoch(&label, ExternalUncertaintyReason::BridgeRestart)
        .await
        .expect("seed epoch");
    store
        .register_external_thread(&label, THREAD_ID)
        .await
        .expect("register managed thread");
    let coordinator = ExternalWriteCoordinator::connect(
        gate,
        store.clone(),
        CancellationToken::new(),
        ExternalWriteSettings {
            request_timeout: REQUEST_TIMEOUT,
            approval_timeout: Duration::from_secs(2),
            client_actor: "bridge-client-a".to_owned(),
            approval_actor: "bridge-approval-a".to_owned(),
            approval_reviewer: APPROVAL_REVIEWER.to_owned(),
            approval_recipient: recipient.clone(),
        },
    )
    .await
    .expect("write coordinator connects");
    assert!(!format!("{source:?}{recipient:?}{coordinator:?}").contains("ou_"));
    (store, coordinator, label)
}

#[tokio::test]
async fn orderly_shutdown_covers_transport_deadlines_and_leaves_server_available() {
    assert!(
        EXTERNAL_WRITE_SHUTDOWN_TIMEOUT
            > EXTERNAL_WS_IO_TIMEOUT.saturating_add(EXTERNAL_WS_CLOSE_TIMEOUT),
        "coordinator deadline must outlive its sequential transport deadlines"
    );

    let server = FakeWriteServer::start().await;
    let scratch = tempfile::tempdir().expect("scratch");
    let token_path = scratch.path().join("bearer");
    write_token(&token_path);
    let (source, recipient) = actors(scratch.path());
    let (store, coordinator, label) =
        seeded_coordinator(&server, &token_path, &source, &recipient).await;

    timeout(EXTERNAL_WRITE_SHUTDOWN_TIMEOUT, coordinator.shutdown())
        .await
        .expect("coordinator shutdown remains bounded")
        .expect("coordinator shuts down orderly");
    assert_eq!(
        store
            .external_endpoint_epoch(&label)
            .await
            .expect("endpoint read")
            .expect("endpoint")
            .state,
        ExternalEndpointState::Stopped
    );

    let operator = connect_operator(&server.endpoint).await;
    drop(operator);
    store.shutdown().await.expect("store shutdown");
    server.finish().await;
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn two_clients_cover_exact_start_steer_interrupt_queue_and_ambiguous_races() {
    let server = FakeWriteServer::start().await;
    let scratch = tempfile::tempdir().expect("scratch");
    let token_path = scratch.path().join("bearer");
    write_token(&token_path);
    let (source, recipient) = actors(scratch.path());
    let (store, coordinator, label) =
        seeded_coordinator(&server, &token_path, &source, &recipient).await;
    let mut operator = connect_operator(&server.endpoint).await;

    server
        .shared
        .pause_bridge_start
        .store(true, Ordering::Release);
    let bridge_start = coordinator.start_turn(
        source.clone(),
        "intent-raced-start",
        start_params("message-raced-start"),
    );
    let operator_start = async {
        let permit = server
            .shared
            .bridge_start_seen
            .acquire()
            .await
            .expect("bridge start reaches server");
        permit.forget();
        let response = operator_request(
            &mut operator,
            10,
            "turn/start",
            json!({
                "threadId": THREAD_ID,
                "input": [{"type": "text", "text": "operator"}],
                "clientUserMessageId": "operator-raced-start",
                "approvalsReviewer": APPROVAL_REVIEWER,
            }),
        )
        .await;
        server.shared.release_bridge_start.add_permits(1);
        response
    };
    let (bridge_result, operator_result) = tokio::join!(bridge_start, operator_start);
    assert_eq!(bridge_result, Err(ExternalWriteError::Conflict));
    let operator_turn = operator_result["result"]["turn"]["id"]
        .as_str()
        .expect("operator turn id")
        .to_owned();
    assert_eq!(
        store
            .external_mutation_intent(&label, THREAD_ID, "intent-raced-start")
            .await
            .expect("intent read")
            .expect("intent")
            .state,
        ExternalMutationState::Rejected
    );
    assert!(
        operator_request(
            &mut operator,
            11,
            "turn/interrupt",
            json!({"threadId": THREAD_ID, "turnId": operator_turn}),
        )
        .await
        .get("result")
        .is_some()
    );

    let started = coordinator
        .start_turn(
            source.clone(),
            "intent-start",
            start_params("message-start"),
        )
        .await
        .expect("bridge start");
    let turn_id = result_id(started);
    assert_eq!(
        result_id(
            coordinator
                .steer_turn(
                    source.clone(),
                    "intent-steer",
                    steer_params(&turn_id, "message-steer"),
                )
                .await
                .expect("bridge steer")
        ),
        turn_id
    );
    let queued_id = result_id(
        coordinator
            .queue_input(
                source.clone(),
                "intent-queue",
                &turn_id,
                ThreadQueueAddParams {
                    thread_id: THREAD_ID.to_owned(),
                    client_user_message_id: "message-queue".to_owned(),
                    input: vec![UserInput::text("queued")],
                },
            )
            .await
            .expect("bridge queue add"),
    );
    coordinator
        .interrupt_turn(
            source.clone(),
            "intent-interrupt",
            TurnInterruptParams::new(THREAD_ID, &turn_id),
        )
        .await
        .expect("bridge interrupt");
    let queued_turn = result_id(
        coordinator
            .start_queued(
                source.clone(),
                "intent-queue-start",
                ThreadQueueStartParams {
                    thread_id: THREAD_ID.to_owned(),
                    queued_submission_id: Some(queued_id),
                },
            )
            .await
            .expect("bridge queue start"),
    );
    coordinator
        .interrupt_turn(
            source.clone(),
            "intent-queue-interrupt",
            TurnInterruptParams::new(THREAD_ID, queued_turn),
        )
        .await
        .expect("queued turn interrupt");

    let final_turn = result_id(
        coordinator
            .start_turn(
                source.clone(),
                "intent-final-start",
                start_params("message-final-start"),
            )
            .await
            .expect("final start"),
    );
    server
        .shared
        .pause_bridge_steer
        .store(true, Ordering::Release);
    let bridge_steer = coordinator.steer_turn(
        source,
        "intent-raced-steer",
        steer_params(&final_turn, "message-raced-steer"),
    );
    let operator_steer = async {
        let permit = server
            .shared
            .bridge_steer_seen
            .acquire()
            .await
            .expect("bridge steer reaches server");
        permit.forget();
        let response = operator_request(
            &mut operator,
            12,
            "turn/steer",
            json!({
                "threadId": THREAD_ID,
                "expectedTurnId": final_turn,
                "input": [{"type": "text", "text": "operator steer"}],
                "clientUserMessageId": "operator-raced-steer",
            }),
        )
        .await;
        server
            .shared
            .omit_bridge_steer_message
            .store(true, Ordering::Release);
        server.shared.release_bridge_steer.add_permits(1);
        response
    };
    let (bridge_result, operator_result) = tokio::join!(bridge_steer, operator_steer);
    assert_eq!(bridge_result, Err(ExternalWriteError::Ambiguous));
    assert_eq!(operator_result["result"]["turnId"], final_turn);
    assert_eq!(
        store
            .external_mutation_intent(&label, THREAD_ID, "intent-raced-steer")
            .await
            .expect("intent read")
            .expect("intent")
            .state,
        ExternalMutationState::Uncertain
    );
    assert_eq!(
        server.method_count(SessionRole::Bridge, "turn/steer").await,
        2
    );
    drop(coordinator);
    store.shutdown().await.expect("store shutdown");
    server.finish().await;
}

#[tokio::test]
async fn timeout_and_disconnect_after_send_are_uncertain_and_never_replayed() {
    for failure in [
        Failure::Hold("turn/interrupt"),
        Failure::Disconnect("turn/interrupt"),
    ] {
        let server = FakeWriteServer::start().await;
        let scratch = tempfile::tempdir().expect("scratch");
        let token_path = scratch.path().join("bearer");
        write_token(&token_path);
        let (source, recipient) = actors(scratch.path());
        let (store, coordinator, label) =
            seeded_coordinator(&server, &token_path, &source, &recipient).await;
        let turn_id = result_id(
            coordinator
                .start_turn(
                    source.clone(),
                    "intent-before-failure",
                    start_params("message-before-failure"),
                )
                .await
                .expect("start before failure"),
        );
        server.set_failure(failure).await;
        assert_eq!(
            coordinator
                .interrupt_turn(
                    source.clone(),
                    "intent-failed-interrupt",
                    TurnInterruptParams::new(THREAD_ID, &turn_id),
                )
                .await,
            Err(ExternalWriteError::Uncertain)
        );
        assert_eq!(
            store
                .external_mutation_intent(&label, THREAD_ID, "intent-failed-interrupt")
                .await
                .expect("intent read")
                .expect("intent")
                .state,
            ExternalMutationState::Uncertain
        );
        let _ = coordinator
            .interrupt_turn(
                source,
                "intent-failed-interrupt",
                TurnInterruptParams::new(THREAD_ID, turn_id),
            )
            .await;
        assert_eq!(
            server
                .method_count(SessionRole::Bridge, "turn/interrupt")
                .await,
            1
        );
        drop(coordinator);
        store.shutdown().await.expect("store shutdown");
        server.finish().await;
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn approvals_have_one_recipient_one_response_deadlines_and_duplicate_fencing() {
    let server = FakeWriteServer::start().await;
    let scratch = tempfile::tempdir().expect("scratch");
    let token_path = scratch.path().join("bearer");
    write_token(&token_path);
    let (source, recipient) = actors(scratch.path());
    let (store, mut coordinator, label) =
        seeded_coordinator(&server, &token_path, &source, &recipient).await;
    let turn_id = result_id(
        coordinator
            .start_turn(
                source.clone(),
                "intent-approval-owner",
                start_params("message-approval-owner"),
            )
            .await
            .expect("approval owner turn"),
    );

    server
        .push_bridge(reverse_request(
            "approval-command-1",
            "item/commandExecution/requestApproval",
            &turn_id,
            None,
        ))
        .await;
    let command_prompt = timeout(TEST_TIMEOUT, coordinator.recv_approval())
        .await
        .expect("command prompt deadline")
        .expect("command prompt");
    assert_eq!(command_prompt.kind, ExternalApprovalPromptKind::Command);
    assert_eq!(
        coordinator
            .resolve_approval(
                source.clone(),
                command_prompt.approval_id.clone(),
                command_decline(),
            )
            .await,
        Err(ExternalWriteError::Unauthorized)
    );
    let left = coordinator.resolve_approval(
        recipient.clone(),
        command_prompt.approval_id.clone(),
        command_decline(),
    );
    let right = coordinator.resolve_approval(
        recipient.clone(),
        command_prompt.approval_id.clone(),
        command_decline(),
    );
    let (left, right) = tokio::join!(left, right);
    assert!(
        matches!((left, right), (Ok(()), Err(ExternalWriteError::Conflict)))
            || matches!((left, right), (Err(ExternalWriteError::Conflict), Ok(())))
    );
    let responses = server.approval_responses(1).await;
    assert_eq!(responses[0]["id"], "approval-command-1");
    assert_eq!(responses[0]["result"]["decision"], "decline");
    server
        .push_bridge(json!({
            "method": "serverRequest/resolved",
            "params": {"threadId": THREAD_ID, "requestId": "approval-command-1"}
        }))
        .await;
    wait_approval_state(
        &store,
        &label,
        &command_prompt.approval_id,
        ExternalApprovalState::Resolved,
    )
    .await;

    server
        .push_bridge(reverse_request(
            "approval-file-1",
            "item/fileChange/requestApproval",
            &turn_id,
            Some(10),
        ))
        .await;
    let file_prompt = timeout(TEST_TIMEOUT, coordinator.recv_approval())
        .await
        .expect("file prompt deadline")
        .expect("file prompt");
    assert_eq!(file_prompt.kind, ExternalApprovalPromptKind::FileChange);
    let responses = server.approval_responses(2).await;
    assert_eq!(responses[1]["id"], "approval-file-1");
    assert_eq!(responses[1]["result"]["decision"], "decline");
    wait_approval_state(
        &store,
        &label,
        &file_prompt.approval_id,
        ExternalApprovalState::Denied,
    )
    .await;
    server
        .push_bridge(json!({
            "method": "serverRequest/resolved",
            "params": {"threadId": THREAD_ID, "requestId": "approval-file-1"}
        }))
        .await;

    server
        .push_bridge(reverse_request(
            "approval-permissions-1",
            "item/permissions/requestApproval",
            &turn_id,
            Some(10),
        ))
        .await;
    let permissions_prompt = timeout(TEST_TIMEOUT, coordinator.recv_approval())
        .await
        .expect("permissions prompt deadline")
        .expect("permissions prompt");
    assert_eq!(
        permissions_prompt.kind,
        ExternalApprovalPromptKind::Permissions
    );
    let responses = server.approval_responses(3).await;
    assert_eq!(responses[2]["id"], "approval-permissions-1");
    assert_eq!(responses[2]["result"]["permissions"], json!({}));
    wait_approval_state(
        &store,
        &label,
        &permissions_prompt.approval_id,
        ExternalApprovalState::Denied,
    )
    .await;
    server
        .push_bridge(json!({
            "method": "serverRequest/resolved",
            "params": {"threadId": THREAD_ID, "requestId": "approval-permissions-1"}
        }))
        .await;

    server
        .push_bridge(reverse_request(
            "approval-command-1",
            "item/commandExecution/requestApproval",
            &turn_id,
            None,
        ))
        .await;
    timeout(TEST_TIMEOUT, async {
        loop {
            let endpoint = store
                .external_endpoint_epoch(&label)
                .await
                .expect("endpoint read")
                .expect("endpoint");
            if endpoint.state == ExternalEndpointState::Unavailable {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("duplicate approval fences epoch");
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(server.approval_responses(3).await.len(), 3);

    drop(coordinator);
    store.shutdown().await.expect("store shutdown");
    server.finish().await;
}

#[tokio::test]
async fn drained_approval_actor_reassignment_is_serialized_and_closes_the_old_coordinator() {
    let server = FakeWriteServer::start().await;
    let scratch = tempfile::tempdir().expect("scratch");
    let token_path = scratch.path().join("bearer");
    write_token(&token_path);
    let (source, recipient) = actors(scratch.path());
    let (store, coordinator, label) =
        seeded_coordinator(&server, &token_path, &source, &recipient).await;

    coordinator
        .reassign_approval_actor("bridge-approval-b")
        .await
        .expect("drained actor reassigns");
    assert_eq!(
        coordinator
            .start_turn(
                source,
                "intent-after-reassign",
                start_params("message-after-reassign")
            )
            .await,
        Err(ExternalWriteError::Closed)
    );
    assert_eq!(
        store
            .external_endpoint_epoch(&label)
            .await
            .expect("endpoint read")
            .expect("endpoint")
            .state,
        ExternalEndpointState::Stopped
    );

    drop(coordinator);
    store.shutdown().await.expect("store shutdown");
    server.finish().await;
}

fn command_decline() -> ExternalApprovalDecision {
    ExternalApprovalDecision::Command(CommandExecutionRequestApprovalResult {
        decision: CommandExecutionApprovalDecision::Simple(SimpleApprovalDecision::Decline),
    })
}

async fn wait_approval_state(
    store: &StoreHandle,
    endpoint_label: &str,
    approval_id: &str,
    expected: ExternalApprovalState,
) {
    timeout(TEST_TIMEOUT, async {
        loop {
            let claim = store
                .external_approval_claim(endpoint_label, THREAD_ID, approval_id)
                .await
                .expect("approval read")
                .expect("approval claim");
            if claim.state == expected {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("approval reaches expected state");
}

fn result_id(applied: ExternalMutationApplied) -> String {
    applied.result_id.expect("mutation result id")
}

fn start_params(message_id: &str) -> TurnStartParams {
    let mut params = TurnStartParams::new(THREAD_ID, vec![UserInput::text("start")]);
    params.client_user_message_id = Some(message_id.to_owned());
    params.approvals_reviewer = Some(APPROVAL_REVIEWER.to_owned());
    params
}

fn steer_params(turn_id: &str, message_id: &str) -> TurnSteerParams {
    TurnSteerParams {
        thread_id: THREAD_ID.to_owned(),
        expected_turn_id: turn_id.to_owned(),
        input: vec![UserInput::text("steer")],
        additional_context: None,
        client_user_message_id: Some(message_id.to_owned()),
        responsesapi_client_metadata: None,
    }
}

fn actors(_root: &Path) -> (AuthorizedLarkActor, AuthorizedLarkActor) {
    let config = BridgeConfig {
        owners: vec!["ou_owner_123456".to_owned()],
        allowed_senders: vec!["ou_sender_123456".to_owned()],
        allowed_groups: vec![],
        default_workspace: None,
        workspace: WorkspacePolicy {
            allow_roots: vec![std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))],
            network_access: false,
        },
        concurrency: ConcurrencyConfig::default(),
        codex: CodexSection::default(),
        paths: PathsSection::default(),
    };
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let source = policy
        .authorize_external_source(&event("ou_sender_123456"))
        .expect("source authorized");
    let recipient = policy
        .authorize_external_approval_recipient(&event("ou_owner_123456"))
        .expect("approval recipient authorized");
    (source, recipient)
}

fn event(sender_id: &str) -> InboundEvent {
    InboundEvent {
        event_id: "evt-write".to_owned(),
        message_id: "om-write".to_owned(),
        chat_id: "oc-write".to_owned(),
        sender_id: sender_id.to_owned(),
        chat_type: ChatMode::P2p,
        thread_id: None,
        root_id: None,
        reply_to_message_id: None,
        text: "bounded".to_owned(),
        mentions_bot: false,
        mention_all: false,
        sender_is_human: true,
        mentions: Vec::new(),
        parts: Vec::new(),
        resources: Vec::new(),
        message_type: "text".to_owned(),
        create_time_ms: 0,
        scope: ScopeKey::Chat("oc-write".to_owned()),
    }
}

fn write_gate(endpoint: &str, token_path: &Path) -> ExternalEndpointGate {
    ExternalEndpointGate::new(ExternalEndpointConfig {
        endpoint: endpoint.to_owned(),
        expected_codex_version: "0.149.0".to_owned(),
        capability_profile: ExternalCapabilityProfile::QueueShared,
        authentication: ExternalAuthentication::BearerTokenFile {
            path: token_path.to_path_buf(),
        },
    })
    .expect("write gate")
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

fn reverse_request(
    request_id: &str,
    method: &str,
    turn_id: &str,
    auto_resolution_ms: Option<u64>,
) -> Value {
    let mut params = contract()["reverseRequests"]
        .as_array()
        .expect("reverse requests")
        .iter()
        .find(|request| request["method"] == method)
        .unwrap_or_else(|| panic!("reverse request {method}"))["params"]
        .clone();
    params["threadId"] = Value::String(THREAD_ID.to_owned());
    params["turnId"] = Value::String(turn_id.to_owned());
    if let Some(deadline) = auto_resolution_ms {
        params["autoResolutionMs"] = json!(deadline);
    }
    json!({"id": request_id, "method": method, "params": params})
}

async fn recv_json<S>(socket: &mut WebSocketStream<S>) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    recv_json_or_close(socket).await.expect("JSON before close")
}

async fn recv_json_or_close<S>(socket: &mut WebSocketStream<S>) -> Option<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let message = timeout(TEST_TIMEOUT, socket.next())
            .await
            .expect("socket activity")?
            .ok()?;
        match message {
            Message::Text(text) => return serde_json::from_str(&text).ok(),
            Message::Close(frame) => {
                let _ = socket.send(Message::Close(frame)).await;
                return None;
            }
            Message::Ping(payload) => {
                let _ = socket.send(Message::Pong(payload)).await;
            }
            _ => return None,
        }
    }
}

async fn send_json<S>(socket: &mut WebSocketStream<S>, value: Value)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    send_json_checked(socket, value).await.expect("JSON sends");
}

async fn send_json_checked<S>(
    socket: &mut WebSocketStream<S>,
    value: Value,
) -> Result<(), tokio_tungstenite::tungstenite::Error>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    socket.send(Message::Text(value.to_string().into())).await
}

async fn send_result<S>(socket: &mut WebSocketStream<S>, request: &Value, result: Value)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    send_result_checked(socket, request, result)
        .await
        .expect("result sends");
}

async fn send_result_checked<S>(
    socket: &mut WebSocketStream<S>,
    request: &Value,
    result: Value,
) -> Result<(), tokio_tungstenite::tungstenite::Error>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    send_json_checked(
        socket,
        json!({"id": request["id"].clone(), "result": result}),
    )
    .await
}

async fn send_error_checked<S>(
    socket: &mut WebSocketStream<S>,
    request: &Value,
) -> Result<(), tokio_tungstenite::tungstenite::Error>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    send_json_checked(
        socket,
        json!({
            "id": request["id"].clone(),
            "error": {"code": -32000, "message": "conflict"}
        }),
    )
    .await
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
