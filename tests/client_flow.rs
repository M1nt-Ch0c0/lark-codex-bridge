use std::{sync::Arc, time::Duration};

use lark_codex_bridge::{
    codex::{
        client::{
            AppServerClient, AppServerEvent, ControlEvent, SubscriptionInvalidation, ThreadId,
            ThreadSubscription, TurnId, TurnOutcome,
        },
        compat::WireAdapter,
        rpc::{ConnectionEpoch, RpcConnection, initialize_connection, spawn_rpc},
        transport::spawn_stream_transport,
        types::{
            MessagePhase, ThreadItem, ThreadResumeParams, ThreadStartParams, TurnStartParams,
            TurnStatus, UserInput,
        },
    },
    limits::{MAX_JSONL_LINE_BYTES, THREAD_EVENT_CAPACITY, THREAD_TERMINAL_CAPACITY},
};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, duplex},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

const IO_TIMEOUT: Duration = Duration::from_secs(3);

struct ClientHarness {
    app_stdout: DuplexStream,
    app_stdin: BufReader<DuplexStream>,
    _app_stderr: DuplexStream,
    client: Arc<AppServerClient>,
}

async fn client_harness(stdin_capacity: usize, epoch: u64) -> ClientHarness {
    let (transport_stdout, app_stdout) = duplex(64 * 1024);
    let (transport_stdin, app_stdin) = duplex(stdin_capacity);
    let (transport_stderr, app_stderr) = duplex(64 * 1024);
    let cancellation = CancellationToken::new();
    let transport = spawn_stream_transport(
        transport_stdout,
        transport_stdin,
        transport_stderr,
        cancellation.clone(),
    );
    let connection = spawn_rpc(transport, ConnectionEpoch::new(epoch), cancellation);
    initialize(
        connection,
        app_stdout,
        BufReader::new(app_stdin),
        app_stderr,
    )
    .await
}

async fn initialize(
    connection: RpcConnection,
    mut app_stdout: DuplexStream,
    mut app_stdin: BufReader<DuplexStream>,
    app_stderr: DuplexStream,
) -> ClientHarness {
    let handle = connection.handle.clone();
    let initialize = tokio::spawn(async move { initialize_connection(&handle).await });

    let request = read_wire(&mut app_stdin).await;
    assert_eq!(request["method"], "initialize");
    respond(
        &mut app_stdout,
        &request,
        json!({
            "userAgent": "codex_cli_rs/0.146.0",
            "platformFamily": "unix",
            "platformOs": "linux",
            "codexHome": absolute_codex_home()
        }),
    )
    .await;
    let initialized = read_wire(&mut app_stdin).await;
    assert_eq!(initialized, json!({"method": "initialized", "params": {}}));
    initialize
        .await
        .expect("initialize task should not panic")
        .expect("fake app-server should initialize successfully");

    ClientHarness {
        app_stdout,
        app_stdin,
        _app_stderr: app_stderr,
        client: Arc::new(AppServerClient::spawn(connection, WireAdapter::V0_146_0)),
    }
}

fn absolute_codex_home() -> &'static str {
    if cfg!(windows) {
        r"C:\scrubbed-codex-home"
    } else {
        "/tmp/scrubbed-codex-home"
    }
}

async fn read_wire(reader: &mut BufReader<DuplexStream>) -> Value {
    let mut line = String::new();
    let bytes_read = timeout(IO_TIMEOUT, reader.read_line(&mut line))
        .await
        .expect("client should write a message before the timeout")
        .expect("client output should be readable");
    assert_ne!(bytes_read, 0, "client output should not close unexpectedly");
    assert!(
        bytes_read <= MAX_JSONL_LINE_BYTES,
        "test wire message should respect the production limit"
    );
    serde_json::from_str(&line).expect("client output should contain valid JSON")
}

async fn write_wire(writer: &mut DuplexStream, value: Value) {
    let mut bytes = serde_json::to_vec(&value).expect("test message should encode");
    bytes.push(b'\n');
    writer
        .write_all(&bytes)
        .await
        .expect("fake app-server message should be writable");
}

async fn write_wire_batch(writer: &mut DuplexStream, values: impl IntoIterator<Item = Value>) {
    let mut bytes = Vec::new();
    for value in values {
        bytes.extend(serde_json::to_vec(&value).expect("test message should encode"));
        bytes.push(b'\n');
    }
    writer
        .write_all(&bytes)
        .await
        .expect("fake app-server message batch should be writable");
}

async fn respond(writer: &mut DuplexStream, request: &Value, result: Value) {
    write_wire(
        writer,
        json!({
            "id": request.get("id").expect("request should contain an id"),
            "result": result
        }),
    )
    .await;
}

async fn recv_event(subscription: &mut ThreadSubscription) -> AppServerEvent {
    timeout(IO_TIMEOUT, subscription.recv())
        .await
        .expect("thread event should arrive before the timeout")
        .expect("thread subscription should remain open")
}

fn thread(thread_id: &str) -> Value {
    json!({
        "id": thread_id,
        "sessionId": thread_id,
        "preview": "",
        "modelProvider": "openai",
        "createdAt": 1_786_478_400_i64,
        "updatedAt": 1_786_478_400_i64,
        "status": {"type": "idle"},
        "ephemeral": false,
        "turns": [],
        "source": "appServer",
        "cliVersion": "0.146.0",
        "cwd": "/workspace"
    })
}

fn thread_result(thread_id: &str) -> Value {
    json!({
        "thread": thread(thread_id),
        "model": "gpt-5.6",
        "modelProvider": "openai",
        "cwd": "/workspace",
        "approvalPolicy": "on-request",
        "approvalsReviewer": "user",
        "sandbox": {
            "type": "workspaceWrite",
            "writableRoots": ["/workspace"],
            "networkAccess": false,
            "excludeTmpdirEnvVar": false,
            "excludeSlashTmp": false
        }
    })
}

#[allow(clippy::needless_pass_by_value)]
fn turn(turn_id: &str, status: &str, items: Value) -> Value {
    json!({
        "id": turn_id,
        "items": items,
        "status": status,
        "startedAt": 1_786_478_401_i64,
        "completedAt": if status == "inProgress" { Value::Null } else { json!(1_786_478_402_i64) },
        "durationMs": if status == "inProgress" { Value::Null } else { json!(1_500_i64) },
        "error": null
    })
}

fn completed_agent_item(item_id: &str, text: &str) -> Value {
    json!({
        "id": item_id,
        "type": "agentMessage",
        "text": text,
        "phase": "final_answer",
        "memoryCitation": null
    })
}

async fn send_delta(
    writer: &mut DuplexStream,
    thread_id: &str,
    turn_id: &str,
    item_id: &str,
    delta: &str,
) {
    write_wire(
        writer,
        json!({
            "method": "item/agentMessage/delta",
            "params": {
                "threadId": thread_id,
                "turnId": turn_id,
                "itemId": item_id,
                "delta": delta
            }
        }),
    )
    .await;
}

async fn send_item_completed(
    writer: &mut DuplexStream,
    thread_id: &str,
    turn_id: &str,
    item: Value,
) {
    write_wire(
        writer,
        json!({
            "method": "item/completed",
            "params": {
                "threadId": thread_id,
                "turnId": turn_id,
                "completedAtMs": 1_786_478_402_500_i64,
                "item": item
            }
        }),
    )
    .await;
}

async fn send_turn_completed(
    writer: &mut DuplexStream,
    thread_id: &str,
    turn_id: &str,
    status: &str,
    items: Value,
) {
    write_wire(
        writer,
        json!({
            "method": "turn/completed",
            "params": {
                "threadId": thread_id,
                "turn": turn(turn_id, status, items)
            }
        }),
    )
    .await;
}

fn assert_authoritative_outcome(outcome: &TurnOutcome, turn_id: &TurnId) {
    assert_eq!(&outcome.turn_id, turn_id);
    assert_eq!(outcome.status, TurnStatus::Completed);
    assert!(outcome.error.is_none());
    assert_eq!(outcome.completed_items.len(), 1);
    match &outcome.completed_items[0] {
        ThreadItem::AgentMessage {
            text,
            phase: Some(MessagePhase::FinalAnswer),
            ..
        } => assert_eq!(text, "Final answer."),
        item => panic!("expected one authoritative final agent item, got {item:?}"),
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn full_thread_turn_flow_projects_only_authoritative_completed_agent_item() {
    let mut harness = client_harness(64 * 1024, 21).await;
    let thread_id = ThreadId::new("thread-a");
    let turn_id = TurnId::new("turn-a");
    let mut subscription = harness
        .client
        .subscribe(thread_id.clone())
        .await
        .expect("thread should be subscribable before it starts");

    let client = Arc::clone(&harness.client);
    let start_thread =
        tokio::spawn(async move { client.start_thread(ThreadStartParams::default()).await });
    let request = read_wire(&mut harness.app_stdin).await;
    assert_eq!(request["method"], "thread/start");
    respond(&mut harness.app_stdout, &request, thread_result("thread-a")).await;
    let started_thread = start_thread
        .await
        .expect("thread/start task should not panic")
        .expect("thread/start should succeed");
    assert_eq!(started_thread.id, "thread-a");
    assert!(matches!(
        recv_event(&mut subscription).await,
        AppServerEvent::ThreadStarted { thread_id: event_thread }
            if event_thread == thread_id
    ));

    let client = Arc::clone(&harness.client);
    let start_turn = tokio::spawn(async move {
        client
            .start_turn(TurnStartParams::new(
                "thread-a",
                vec![UserInput::text("hello")],
            ))
            .await
    });
    let request = read_wire(&mut harness.app_stdin).await;
    assert_eq!(request["method"], "turn/start");
    assert_eq!(request["params"]["threadId"], "thread-a");

    write_wire(
        &mut harness.app_stdout,
        json!({
            "method": "turn/started",
            "params": {"threadId": "thread-a", "turn": turn("turn-a", "inProgress", json!([]))}
        }),
    )
    .await;
    respond(
        &mut harness.app_stdout,
        &request,
        json!({"turn": turn("turn-a", "inProgress", json!([]))}),
    )
    .await;
    let started_turn = start_turn
        .await
        .expect("turn/start task should not panic")
        .expect("turn/start should succeed");
    assert_eq!(started_turn.id, "turn-a");

    // The response and a duplicate notification describe one logical start.
    write_wire(
        &mut harness.app_stdout,
        json!({
            "method": "turn/started",
            "params": {"threadId": "thread-a", "turn": turn("turn-a", "inProgress", json!([]))}
        }),
    )
    .await;
    send_delta(
        &mut harness.app_stdout,
        "thread-a",
        "turn-a",
        "agent-a",
        "Draft that must not become the final answer.",
    )
    .await;

    assert!(matches!(
        recv_event(&mut subscription).await,
        AppServerEvent::TurnStarted { turn } if turn.id == "turn-a"
    ));
    assert!(matches!(
        recv_event(&mut subscription).await,
        AppServerEvent::AgentMessageDelta { turn_id: event_turn, item_id, delta }
            if event_turn == turn_id
                && item_id.as_str() == "agent-a"
                && delta == "Draft that must not become the final answer."
    ));
    assert!(subscription.outcome(&turn_id).is_none());

    let final_item = completed_agent_item("agent-a", "Final answer.");
    send_item_completed(
        &mut harness.app_stdout,
        "thread-a",
        "turn-a",
        final_item.clone(),
    )
    .await;
    // A replayed terminal item is idempotent and must not duplicate the answer.
    send_item_completed(
        &mut harness.app_stdout,
        "thread-a",
        "turn-a",
        final_item.clone(),
    )
    .await;
    send_turn_completed(
        &mut harness.app_stdout,
        "thread-a",
        "turn-a",
        "completed",
        json!([final_item]),
    )
    .await;

    assert!(matches!(
        recv_event(&mut subscription).await,
        AppServerEvent::ItemCompleted { turn_id: event_turn, item }
            if event_turn == turn_id && item.id() == Some("agent-a")
    ));
    let outcome = match recv_event(&mut subscription).await {
        AppServerEvent::TurnCompleted(outcome) => outcome,
        event => panic!("expected authoritative turn completion, got {event:?}"),
    };
    assert_authoritative_outcome(&outcome, &turn_id);
    assert_authoritative_outcome(
        &subscription
            .outcome(&turn_id)
            .expect("outcome should become visible only after turn/completed"),
        &turn_id,
    );

    harness
        .client
        .shutdown()
        .await
        .expect("client should shut down cleanly");
}

#[tokio::test]
async fn resume_thread_uses_the_stable_resume_rpc_and_returns_the_resumed_thread() {
    let mut harness = client_harness(64 * 1024, 22).await;
    let client = Arc::clone(&harness.client);
    let resume = tokio::spawn(async move {
        client
            .resume_thread(ThreadResumeParams::new("thread-resumed"))
            .await
    });

    let request = read_wire(&mut harness.app_stdin).await;
    assert_eq!(request["method"], "thread/resume");
    assert_eq!(request["params"]["threadId"], "thread-resumed");
    respond(
        &mut harness.app_stdout,
        &request,
        thread_result("thread-resumed"),
    )
    .await;
    let resumed = resume
        .await
        .expect("thread/resume task should not panic")
        .expect("thread/resume should succeed");
    assert_eq!(resumed.id, "thread-resumed");

    harness
        .client
        .shutdown()
        .await
        .expect("client should shut down cleanly");
}

#[tokio::test]
async fn interrupt_overtakes_a_backlog_of_normal_thread_requests() {
    // A tiny stdin pipe keeps the first normal line in progress while the RPC
    // queues fill, making priority observable without inspecting internals.
    let mut harness = client_harness(1, 23).await;
    let mut normal_tasks = Vec::new();
    for _ in 0..6 {
        let client = Arc::clone(&harness.client);
        normal_tasks.push(tokio::spawn(async move {
            client.start_thread(ThreadStartParams::default()).await
        }));
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    let client = Arc::clone(&harness.client);
    let interrupt = tokio::spawn(async move {
        client
            .interrupt_turn(
                &ThreadId::new("thread-running"),
                &TurnId::new("turn-running"),
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let mut normal_requests = Vec::new();
    let first = read_wire(&mut harness.app_stdin).await;
    if first["method"] == "turn/interrupt" {
        respond(&mut harness.app_stdout, &first, json!({})).await;
    } else {
        assert_eq!(first["method"], "thread/start");
        normal_requests.push(first);
        let second = read_wire(&mut harness.app_stdin).await;
        assert_eq!(
            second["method"], "turn/interrupt",
            "high-priority interrupt should overtake queued normal requests"
        );
        respond(&mut harness.app_stdout, &second, json!({})).await;
    }

    while normal_requests.len() < normal_tasks.len() {
        let request = read_wire(&mut harness.app_stdin).await;
        assert_eq!(request["method"], "thread/start");
        normal_requests.push(request);
    }
    for request in &normal_requests {
        respond(
            &mut harness.app_stdout,
            request,
            thread_result("normal-thread"),
        )
        .await;
    }

    interrupt
        .await
        .expect("interrupt task should not panic")
        .expect("interrupt acknowledgement should succeed");
    for task in normal_tasks {
        task.await
            .expect("normal task should not panic")
            .expect("normal request should eventually complete");
    }

    harness
        .client
        .shutdown()
        .await
        .expect("client should shut down cleanly");
}

#[tokio::test]
async fn unknown_notification_does_not_poison_the_thread_event_stream() {
    let mut harness = client_harness(64 * 1024, 24).await;
    let mut subscription = harness
        .client
        .subscribe(ThreadId::new("thread-known"))
        .await
        .expect("known thread should be subscribable");

    write_wire(
        &mut harness.app_stdout,
        json!({
            "method": "future/notification",
            "params": {"threadId": "thread-known", "newField": "safe"}
        }),
    )
    .await;
    send_delta(
        &mut harness.app_stdout,
        "thread-known",
        "turn-known",
        "agent-known",
        "still alive",
    )
    .await;

    assert!(matches!(
        recv_event(&mut subscription).await,
        AppServerEvent::Unknown { method } if method == "future/notification"
    ));
    assert!(matches!(
        recv_event(&mut subscription).await,
        AppServerEvent::AgentMessageDelta { delta, .. } if delta == "still alive"
    ));

    harness
        .client
        .shutdown()
        .await
        .expect("client should shut down cleanly");
}

#[tokio::test]
async fn interleaved_notifications_remain_isolated_by_thread() {
    let mut harness = client_harness(64 * 1024, 25).await;
    let mut thread_a = harness
        .client
        .subscribe(ThreadId::new("thread-a"))
        .await
        .expect("thread A should be subscribable");
    let mut thread_b = harness
        .client
        .subscribe(ThreadId::new("thread-b"))
        .await
        .expect("thread B should be subscribable");

    send_delta(
        &mut harness.app_stdout,
        "thread-b",
        "turn-b",
        "agent-b",
        "from B",
    )
    .await;
    send_delta(
        &mut harness.app_stdout,
        "thread-a",
        "turn-a",
        "agent-a",
        "from A",
    )
    .await;
    send_turn_completed(
        &mut harness.app_stdout,
        "thread-b",
        "turn-b",
        "completed",
        json!([]),
    )
    .await;
    send_turn_completed(
        &mut harness.app_stdout,
        "thread-a",
        "turn-a",
        "interrupted",
        json!([]),
    )
    .await;

    assert!(matches!(
        recv_event(&mut thread_a).await,
        AppServerEvent::AgentMessageDelta { turn_id, delta, .. }
            if turn_id.as_str() == "turn-a" && delta == "from A"
    ));
    assert!(matches!(
        recv_event(&mut thread_b).await,
        AppServerEvent::AgentMessageDelta { turn_id, delta, .. }
            if turn_id.as_str() == "turn-b" && delta == "from B"
    ));
    assert!(matches!(
        recv_event(&mut thread_a).await,
        AppServerEvent::TurnCompleted(TurnOutcome { turn_id, status: TurnStatus::Interrupted, .. })
            if turn_id.as_str() == "turn-a"
    ));
    assert!(matches!(
        recv_event(&mut thread_b).await,
        AppServerEvent::TurnCompleted(TurnOutcome { turn_id, status: TurnStatus::Completed, .. })
            if turn_id.as_str() == "turn-b"
    ));

    harness
        .client
        .shutdown()
        .await
        .expect("client should shut down cleanly");
}

#[tokio::test]
async fn dropping_one_subscription_does_not_block_another_thread() {
    let mut harness = client_harness(64 * 1024, 26).await;
    let dropped = harness
        .client
        .subscribe(ThreadId::new("thread-dropped"))
        .await
        .expect("first thread should be subscribable");
    let mut survivor = harness
        .client
        .subscribe(ThreadId::new("thread-survivor"))
        .await
        .expect("second thread should be subscribable");
    drop(dropped);

    send_delta(
        &mut harness.app_stdout,
        "thread-dropped",
        "turn-dropped",
        "agent-dropped",
        "must not block",
    )
    .await;
    send_delta(
        &mut harness.app_stdout,
        "thread-survivor",
        "turn-survivor",
        "agent-survivor",
        "delivered",
    )
    .await;

    assert!(matches!(
        recv_event(&mut survivor).await,
        AppServerEvent::AgentMessageDelta { turn_id, item_id, delta }
            if turn_id.as_str() == "turn-survivor"
                && item_id.as_str() == "agent-survivor"
                && delta == "delivered"
    ));

    harness
        .client
        .shutdown()
        .await
        .expect("client should shut down cleanly");
}

#[tokio::test]
async fn idle_thread_routes_are_reclaimed_for_long_running_connections() {
    use lark_codex_bridge::limits::CLIENT_PROJECTION_CAPACITY;

    let harness = client_harness(64 * 1024, 36).await;
    for index in 0..CLIENT_PROJECTION_CAPACITY {
        let subscription = harness
            .client
            .subscribe(ThreadId::new(format!("historical-thread-{index}")))
            .await
            .expect("historical route should fit while capacity remains");
        drop(subscription);
    }

    let _replacement = harness
        .client
        .subscribe(ThreadId::new("replacement-thread"))
        .await
        .expect("an idle historical route should be reclaimed");

    harness
        .client
        .shutdown()
        .await
        .expect("client should shut down cleanly");
}

#[tokio::test]
async fn turn_start_response_without_notification_still_emits_exactly_one_started_event() {
    let mut harness = client_harness(64 * 1024, 27).await;
    let mut subscription = harness
        .client
        .subscribe(ThreadId::new("thread-response-first"))
        .await
        .expect("thread should be subscribable");

    let client = Arc::clone(&harness.client);
    let start_turn = tokio::spawn(async move {
        client
            .start_turn(TurnStartParams::new(
                "thread-response-first",
                vec![UserInput::text("response-only")],
            ))
            .await
    });
    let request = read_wire(&mut harness.app_stdin).await;
    assert_eq!(request["method"], "turn/start");
    respond(
        &mut harness.app_stdout,
        &request,
        json!({"turn": turn("turn-response-first", "inProgress", json!([]))}),
    )
    .await;
    start_turn
        .await
        .expect("turn/start task should not panic")
        .expect("response-only turn/start should succeed");

    assert!(matches!(
        recv_event(&mut subscription).await,
        AppServerEvent::TurnStarted { turn } if turn.id == "turn-response-first"
    ));
    assert!(
        timeout(Duration::from_millis(75), subscription.recv())
            .await
            .is_err(),
        "the response must project exactly one logical TurnStarted event"
    );

    harness
        .client
        .shutdown()
        .await
        .expect("client should shut down cleanly");
}

#[tokio::test]
async fn cancelling_an_uncertain_turn_start_fails_the_epoch_closed() {
    let mut harness = client_harness(64 * 1024, 35).await;
    let _subscription = harness
        .client
        .subscribe(ThreadId::new("thread-cancelled-start"))
        .await
        .expect("thread should be subscribable");

    let client = Arc::clone(&harness.client);
    let cancelled = tokio::spawn(async move {
        client
            .start_turn(TurnStartParams::new(
                "thread-cancelled-start",
                vec![UserInput::text("cancel this request")],
            ))
            .await
    });
    let first_request = read_wire(&mut harness.app_stdin).await;
    assert_eq!(first_request["method"], "turn/start");
    cancelled.abort();
    let cancelled_result = cancelled.await;
    assert!(
        matches!(cancelled_result, Err(error) if error.is_cancelled()),
        "the test must cancel the in-flight turn/start future"
    );

    let result = harness
        .client
        .start_turn(TurnStartParams::new(
            "thread-cancelled-start",
            vec![UserInput::text("must not overlap the uncertain request")],
        ))
        .await;
    assert!(
        result.is_err(),
        "an uncertain non-idempotent turn/start must fail the connection closed"
    );

    harness
        .client
        .shutdown()
        .await
        .expect("client should shut down cleanly");
}

#[tokio::test]
async fn notification_first_duplicate_response_and_notifications_emit_one_started_event() {
    let mut harness = client_harness(64 * 1024, 28).await;
    let mut subscription = harness
        .client
        .subscribe(ThreadId::new("thread-notification-first"))
        .await
        .expect("thread should be subscribable");

    let client = Arc::clone(&harness.client);
    let start_turn = tokio::spawn(async move {
        client
            .start_turn(TurnStartParams::new(
                "thread-notification-first",
                vec![UserInput::text("notification-first")],
            ))
            .await
    });
    let request = read_wire(&mut harness.app_stdin).await;
    let started = json!({
        "method": "turn/started",
        "params": {
            "threadId": "thread-notification-first",
            "turn": turn("turn-deduplicated", "inProgress", json!([]))
        }
    });
    write_wire_batch(&mut harness.app_stdout, [started.clone(), started.clone()]).await;
    let result = json!({"turn": turn("turn-deduplicated", "inProgress", json!([]))});
    respond(&mut harness.app_stdout, &request, result.clone()).await;
    // A duplicate response is protocol drift, but must never duplicate the
    // logical turn-start projection.
    respond(&mut harness.app_stdout, &request, result).await;
    write_wire(&mut harness.app_stdout, started).await;
    start_turn
        .await
        .expect("turn/start task should not panic")
        .expect("turn/start should succeed despite duplicate wire traffic");

    assert!(matches!(
        recv_event(&mut subscription).await,
        AppServerEvent::TurnStarted { turn } if turn.id == "turn-deduplicated"
    ));
    assert!(
        timeout(Duration::from_millis(75), subscription.recv())
            .await
            .is_err(),
        "response and notification races must merge into one TurnStarted"
    );

    harness
        .client
        .shutdown()
        .await
        .expect("client should shut down cleanly");
}

#[tokio::test]
async fn one_read_burst_preserves_started_delta_item_and_turn_causality() {
    let mut harness = client_harness(64 * 1024, 29).await;
    let turn_id = TurnId::new("turn-burst");
    let mut subscription = harness
        .client
        .subscribe(ThreadId::new("thread-burst"))
        .await
        .expect("thread should be subscribable");
    let final_item = completed_agent_item("agent-burst", "Final burst answer.");

    write_wire_batch(
        &mut harness.app_stdout,
        [
            json!({
                "method": "turn/started",
                "params": {
                    "threadId": "thread-burst",
                    "turn": turn("turn-burst", "inProgress", json!([]))
                }
            }),
            json!({
                "method": "item/agentMessage/delta",
                "params": {
                    "threadId": "thread-burst",
                    "turnId": "turn-burst",
                    "itemId": "agent-burst",
                    "delta": "Final burst answer."
                }
            }),
            json!({
                "method": "item/completed",
                "params": {
                    "threadId": "thread-burst",
                    "turnId": "turn-burst",
                    "completedAtMs": 1_786_478_402_500_i64,
                    "item": final_item.clone()
                }
            }),
            json!({
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-burst",
                    "turn": turn("turn-burst", "completed", json!([final_item]))
                }
            }),
        ],
    )
    .await;

    assert!(matches!(
        recv_event(&mut subscription).await,
        AppServerEvent::TurnStarted { turn } if turn.id == "turn-burst"
    ));
    assert!(matches!(
        recv_event(&mut subscription).await,
        AppServerEvent::AgentMessageDelta { turn_id: event_turn, delta, .. }
            if event_turn == turn_id && delta == "Final burst answer."
    ));
    assert!(matches!(
        recv_event(&mut subscription).await,
        AppServerEvent::ItemCompleted { turn_id: event_turn, item }
            if event_turn == turn_id && item.id() == Some("agent-burst")
    ));
    assert!(matches!(
        recv_event(&mut subscription).await,
        AppServerEvent::TurnCompleted(TurnOutcome { turn_id: event_turn, .. })
            if event_turn == turn_id
    ));

    harness
        .client
        .shutdown()
        .await
        .expect("client should shut down cleanly");
}

#[tokio::test]
async fn terminal_overflow_reports_subscription_lag_instead_of_silent_incompleteness() {
    let mut harness = client_harness(64 * 1024, 30).await;
    let mut subscription = harness
        .client
        .subscribe(ThreadId::new("thread-lagged"))
        .await
        .expect("thread should be subscribable");

    let completions = (0..=THREAD_TERMINAL_CAPACITY).map(|index| {
        let turn_id = format!("turn-lagged-{index}");
        json!({
            "method": "turn/completed",
            "params": {
                "threadId": "thread-lagged",
                "turn": turn(&turn_id, "completed", json!([]))
            }
        })
    });
    write_wire_batch(&mut harness.app_stdout, completions).await;
    let last_turn = TurnId::new(format!("turn-lagged-{THREAD_TERMINAL_CAPACITY}"));
    timeout(IO_TIMEOUT, async {
        while subscription.outcome(&last_turn).is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("router should project the complete burst before the assertion drains the mailbox");

    let mut saw_lag = false;
    for _ in 0..=THREAD_TERMINAL_CAPACITY {
        match timeout(IO_TIMEOUT, subscription.recv())
            .await
            .expect("lagged subscription should resolve explicitly")
        {
            Some(AppServerEvent::SubscriptionInvalidated {
                thread_id,
                reason: SubscriptionInvalidation::Lagged,
            }) if thread_id.as_str() == "thread-lagged" => {
                saw_lag = true;
                break;
            }
            Some(_) => {}
            None => panic!("terminal overflow closed silently without a lag marker"),
        }
    }
    assert!(
        saw_lag,
        "terminal overflow must be observable by the consumer"
    );

    harness
        .client
        .shutdown()
        .await
        .expect("client should shut down cleanly");
}

#[tokio::test]
async fn malformed_terminal_notification_emits_redacted_protocol_drift() {
    let mut harness = client_harness(64 * 1024, 31).await;
    let mut control = harness
        .client
        .take_control_events()
        .expect("control stream should be subscribable");
    let secret = "PROMPT_BODY_MUST_NOT_LEAK";

    write_wire(
        &mut harness.app_stdout,
        json!({
            "method": "turn/completed",
            "params": {
                "threadId": "thread-malformed",
                "turn": {"id": "turn-malformed", "unexpectedBody": secret}
            }
        }),
    )
    .await;

    let event = timeout(IO_TIMEOUT, control.recv())
        .await
        .expect("malformed terminal notification should be surfaced")
        .expect("control stream should remain open");
    assert!(matches!(
        &event,
        ControlEvent::InvalidNotification {
            method,
            authoritative: true,
        } if method == "turn/completed"
    ));
    assert!(
        !format!("{event:?}").contains(secret),
        "protocol drift diagnostics must not reveal malformed payload contents"
    );

    harness
        .client
        .shutdown()
        .await
        .expect("client should shut down cleanly");
}

#[tokio::test]
async fn app_server_requests_are_delivered_on_the_global_control_stream() {
    let mut harness = client_harness(64 * 1024, 32).await;
    let mut control = harness
        .client
        .take_control_events()
        .expect("control stream should be subscribable");
    let secret = "COMMAND_BODY_MUST_NOT_LEAK";

    write_wire(
        &mut harness.app_stdout,
        json!({
            "id": "approval-request-1",
            "method": "item/commandExecution/requestApproval",
            "params": {
                "threadId": "thread-approval",
                "turnId": "turn-approval",
                "command": secret
            }
        }),
    )
    .await;

    let event = timeout(IO_TIMEOUT, control.recv())
        .await
        .expect("server request should reach the control stream")
        .expect("control stream should remain open");
    match &event {
        ControlEvent::ServerRequest(request) => {
            assert_eq!(request.method, "item/commandExecution/requestApproval");
            assert_eq!(request.epoch(), harness.client.epoch());
        }
        event => panic!("expected a server request, got {event:?}"),
    }
    assert!(
        !format!("{event:?}").contains(secret),
        "control-event diagnostics must redact request payload contents"
    );

    harness
        .client
        .shutdown()
        .await
        .expect("client should shut down cleanly");
}

#[tokio::test]
async fn thread_started_is_replayed_once_when_subscribing_after_start_completes() {
    let mut harness = client_harness(64 * 1024, 33).await;
    let client = Arc::clone(&harness.client);
    let start =
        tokio::spawn(async move { client.start_thread(ThreadStartParams::default()).await });
    let request = read_wire(&mut harness.app_stdin).await;
    assert_eq!(request["method"], "thread/start");

    // app-server can announce the new ID before the RPC caller has observed
    // the response and knows which thread it should subscribe to.
    write_wire(
        &mut harness.app_stdout,
        json!({
            "method": "thread/started",
            "params": {"thread": thread("thread-late-subscribe")}
        }),
    )
    .await;
    respond(
        &mut harness.app_stdout,
        &request,
        thread_result("thread-late-subscribe"),
    )
    .await;
    let started = start
        .await
        .expect("thread/start task should not panic")
        .expect("thread/start should succeed");
    assert_eq!(started.id, "thread-late-subscribe");

    let mut subscription = harness
        .client
        .subscribe(ThreadId::new("thread-late-subscribe"))
        .await
        .expect("completed thread/start should remain subscribable");
    assert!(matches!(
        recv_event(&mut subscription).await,
        AppServerEvent::ThreadStarted { thread_id }
            if thread_id.as_str() == "thread-late-subscribe"
    ));
    assert!(
        timeout(Duration::from_millis(75), subscription.recv())
            .await
            .is_err(),
        "notification-first and RPC observation must replay one ThreadStarted, not two"
    );

    harness
        .client
        .shutdown()
        .await
        .expect("client should shut down cleanly");
}

#[tokio::test]
async fn explicit_route_release_refuses_live_subscription_then_forgets_history() {
    let harness = client_harness(64 * 1024, 38).await;
    let thread_id = ThreadId::new("thread-explicit-release");
    let subscription = harness
        .client
        .subscribe(thread_id.clone())
        .await
        .expect("thread should be subscribable");
    assert!(matches!(
        harness.client.release_thread(&thread_id).await,
        Err(lark_codex_bridge::codex::client::ClientError::Capacity)
    ));
    drop(subscription);
    harness
        .client
        .release_thread(&thread_id)
        .await
        .expect("inactive route should be explicitly releasable");

    harness
        .client
        .shutdown()
        .await
        .expect("client should shut down cleanly");
}

#[tokio::test]
async fn delta_before_item_started_preserves_wire_causality() {
    let mut harness = client_harness(64 * 1024, 34).await;
    let mut subscription = harness
        .client
        .subscribe(ThreadId::new("thread-delta-first"))
        .await
        .expect("thread should be subscribable");
    let item = completed_agent_item("agent-delta-first", "snapshot after delta");

    write_wire_batch(
        &mut harness.app_stdout,
        [
            json!({
                "method": "item/agentMessage/delta",
                "params": {
                    "threadId": "thread-delta-first",
                    "turnId": "turn-delta-first",
                    "itemId": "agent-delta-first",
                    "delta": "arrived first"
                }
            }),
            json!({
                "method": "item/started",
                "params": {
                    "threadId": "thread-delta-first",
                    "turnId": "turn-delta-first",
                    "startedAtMs": 1_786_478_401_500_i64,
                    "item": item
                }
            }),
        ],
    )
    .await;

    assert!(matches!(
        recv_event(&mut subscription).await,
        AppServerEvent::AgentMessageDelta { turn_id, item_id, delta }
            if turn_id.as_str() == "turn-delta-first"
                && item_id == "agent-delta-first"
                && delta == "arrived first"
    ));
    assert!(matches!(
        recv_event(&mut subscription).await,
        AppServerEvent::ItemStarted { turn_id, item }
            if turn_id.as_str() == "turn-delta-first"
                && item.id() == Some("agent-delta-first")
    ));

    harness
        .client
        .shutdown()
        .await
        .expect("client should shut down cleanly");
}

#[tokio::test]
async fn last_token_usage_is_projected_into_the_terminal_outcome() {
    let mut harness = client_harness(64 * 1024, 35).await;
    let turn_id = TurnId::new("turn-usage");
    let mut subscription = harness
        .client
        .subscribe(ThreadId::new("thread-usage"))
        .await
        .expect("thread should be subscribable");

    write_wire_batch(
        &mut harness.app_stdout,
        [
            json!({
                "method": "thread/tokenUsage/updated",
                "params": {
                    "threadId": "thread-usage",
                    "turnId": "turn-usage",
                    "tokenUsage": {
                        "total": {
                            "inputTokens": 1_000,
                            "cachedInputTokens": 100,
                            "cacheWriteInputTokens": 10,
                            "outputTokens": 200,
                            "reasoningOutputTokens": 20,
                            "totalTokens": 1_200
                        },
                        "last": {
                            "inputTokens": 41,
                            "cachedInputTokens": 11,
                            "cacheWriteInputTokens": 3,
                            "outputTokens": 17,
                            "reasoningOutputTokens": 5,
                            "totalTokens": 58
                        },
                        "modelContextWindow": 200_000
                    }
                }
            }),
            json!({
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-usage",
                    "turn": turn("turn-usage", "completed", json!([]))
                }
            }),
        ],
    )
    .await;

    assert!(matches!(
        recv_event(&mut subscription).await,
        AppServerEvent::TokenUsageUpdated { turn_id: event_turn, usage }
            if event_turn == turn_id
                && usage.input_tokens == 41
                && usage.cached_input_tokens == 11
                && usage.cache_write_input_tokens == 3
                && usage.output_tokens == 17
                && usage.reasoning_output_tokens == 5
                && usage.total_tokens == 58
    ));
    let outcome = match recv_event(&mut subscription).await {
        AppServerEvent::TurnCompleted(outcome) => outcome,
        event => panic!("expected terminal outcome after usage, got {event:?}"),
    };
    let usage = outcome
        .token_usage
        .as_ref()
        .expect("terminal outcome should retain the last turn usage");
    assert_eq!(usage.input_tokens, 41);
    assert_eq!(usage.cached_input_tokens, 11);
    assert_eq!(usage.cache_write_input_tokens, 3);
    assert_eq!(usage.output_tokens, 17);
    assert_eq!(usage.reasoning_output_tokens, 5);
    assert_eq!(usage.total_tokens, 58);
    assert_eq!(
        subscription
            .outcome(&turn_id)
            .and_then(|outcome| outcome.token_usage)
            .expect("stored outcome should retain token usage")
            .total_tokens,
        58
    );

    harness
        .client
        .shutdown()
        .await
        .expect("client should shut down cleanly");
}

#[tokio::test]
async fn failed_turn_is_a_terminal_outcome_with_redacted_diagnostics() {
    let mut harness = client_harness(64 * 1024, 36).await;
    let turn_id = TurnId::new("turn-failed");
    let mut subscription = harness
        .client
        .subscribe(ThreadId::new("thread-failed"))
        .await
        .expect("thread should be subscribable");
    let private_message = "remote failure containing private prompt text";
    let private_details = "authorization=must-not-appear-in-debug";
    let mut failed_turn = turn("turn-failed", "failed", json!([]));
    failed_turn["error"] = json!({
        "message": private_message,
        "additionalDetails": private_details,
        "codexErrorInfo": {"type": "internalServerError"},
        "futureErrorField": "preserved"
    });

    write_wire(
        &mut harness.app_stdout,
        json!({
            "method": "turn/completed",
            "params": {"threadId": "thread-failed", "turn": failed_turn}
        }),
    )
    .await;

    let outcome = match recv_event(&mut subscription).await {
        AppServerEvent::TurnCompleted(outcome) => outcome,
        event => panic!("failed turn should still yield a terminal outcome, got {event:?}"),
    };
    assert_eq!(outcome.status, TurnStatus::Failed);
    let error = outcome
        .error
        .as_ref()
        .expect("failed outcome should retain structured error semantics");
    assert_eq!(error.message, private_message);
    assert_eq!(error.additional_details.as_deref(), Some(private_details));
    assert_eq!(error.extra["futureErrorField"], "preserved");
    let rendered = format!("{outcome:?}");
    assert!(rendered.contains("has_error: true"));
    assert!(!rendered.contains(private_message));
    assert!(!rendered.contains(private_details));
    assert!(matches!(
        subscription.outcome(&turn_id),
        Some(TurnOutcome {
            status: TurnStatus::Failed,
            error: Some(_),
            ..
        })
    ));

    harness
        .client
        .shutdown()
        .await
        .expect("client should shut down cleanly");
}

#[tokio::test]
async fn control_server_request_response_keeps_opaque_id_and_overtakes_normal_work() {
    let mut harness = client_harness(64, 37).await;
    let mut control = harness
        .client
        .take_control_events()
        .expect("control stream should be subscribable");
    let opaque_id = json!("approval:opaque/id-37");
    write_wire(
        &mut harness.app_stdout,
        json!({
            "id": opaque_id.clone(),
            "method": "item/fileChange/requestApproval",
            "params": {
                "threadId": "thread-control-priority",
                "turnId": "turn-control-priority",
                "grantRoot": "/workspace"
            }
        }),
    )
    .await;
    let mut request = match timeout(IO_TIMEOUT, control.recv())
        .await
        .expect("server request should reach the control owner")
        .expect("control stream should remain open")
    {
        ControlEvent::ServerRequest(request) => request,
        event => panic!("expected server request token, got {event:?}"),
    };

    let mut normal_tasks = Vec::new();
    for _ in 0..6 {
        let client = Arc::clone(&harness.client);
        normal_tasks.push(tokio::spawn(async move {
            client.start_thread(ThreadStartParams::default()).await
        }));
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    let client = Arc::clone(&harness.client);
    let response = tokio::spawn(async move {
        client
            .respond_request(&mut request, &json!({"decision": "accept"}))
            .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let mut normal_requests = Vec::new();
    let mut saw_server_response = false;
    while !saw_server_response {
        let outbound = read_wire(&mut harness.app_stdin).await;
        if outbound.get("method").is_none() {
            assert_eq!(outbound["id"], opaque_id);
            assert_eq!(outbound["result"], json!({"decision": "accept"}));
            saw_server_response = true;
        } else {
            assert_eq!(outbound["method"], "thread/start");
            normal_requests.push(outbound);
            assert!(
                normal_requests.len() < normal_tasks.len(),
                "high-priority server response must overtake the queued normal backlog"
            );
        }
    }

    while normal_requests.len() < normal_tasks.len() {
        let outbound = read_wire(&mut harness.app_stdin).await;
        assert_eq!(outbound["method"], "thread/start");
        normal_requests.push(outbound);
    }
    for request in &normal_requests {
        respond(
            &mut harness.app_stdout,
            request,
            thread_result("normal-control-thread"),
        )
        .await;
    }
    response
        .await
        .expect("server response task should not panic")
        .expect("server response should be admitted");
    for task in normal_tasks {
        task.await
            .expect("normal request task should not panic")
            .expect("normal request should eventually complete");
    }

    harness
        .client
        .shutdown()
        .await
        .expect("client should shut down cleanly");
}

#[tokio::test]
async fn item_completion_is_a_barrier_between_delta_fragments() {
    let mut harness = client_harness(64 * 1024, 38).await;
    let mut subscription = harness
        .client
        .subscribe(ThreadId::new("thread-delta-barrier"))
        .await
        .expect("thread should be subscribable");
    let item = completed_agent_item("agent-delta-barrier", "authoritative snapshot");

    write_wire_batch(
        &mut harness.app_stdout,
        [
            json!({
                "method": "item/agentMessage/delta",
                "params": {
                    "threadId": "thread-delta-barrier",
                    "turnId": "turn-delta-barrier",
                    "itemId": "agent-delta-barrier",
                    "delta": "A"
                }
            }),
            json!({
                "method": "item/completed",
                "params": {
                    "threadId": "thread-delta-barrier",
                    "turnId": "turn-delta-barrier",
                    "completedAtMs": 1_786_478_402_500_i64,
                    "item": item
                }
            }),
            json!({
                "method": "item/agentMessage/delta",
                "params": {
                    "threadId": "thread-delta-barrier",
                    "turnId": "turn-delta-barrier",
                    "itemId": "agent-delta-barrier",
                    "delta": "B"
                }
            }),
        ],
    )
    .await;

    assert!(matches!(
        recv_event(&mut subscription).await,
        AppServerEvent::AgentMessageDelta { delta, .. } if delta == "A"
    ));
    assert!(matches!(
        recv_event(&mut subscription).await,
        AppServerEvent::ItemCompleted { turn_id, item }
            if turn_id.as_str() == "turn-delta-barrier"
                && item.id() == Some("agent-delta-barrier")
    ));
    assert!(matches!(
        recv_event(&mut subscription).await,
        AppServerEvent::AgentMessageDelta { delta, .. } if delta == "B"
    ));

    harness
        .client
        .shutdown()
        .await
        .expect("client should shut down cleanly");
}

#[tokio::test]
async fn response_only_turn_start_is_projected_before_same_read_terminal_notifications() {
    let mut harness = client_harness(64 * 1024, 39).await;
    let turn_id = TurnId::new("turn-response-burst");
    let mut subscription = harness
        .client
        .subscribe(ThreadId::new("thread-response-burst"))
        .await
        .expect("thread should be subscribable");
    let client = Arc::clone(&harness.client);
    let start = tokio::spawn(async move {
        client
            .start_turn(TurnStartParams::new(
                "thread-response-burst",
                vec![UserInput::text("response controls start")],
            ))
            .await
    });
    let request = read_wire(&mut harness.app_stdin).await;
    assert_eq!(request["method"], "turn/start");
    let response_id = request
        .get("id")
        .cloned()
        .expect("turn/start request should contain an id");

    // One stdout read can contain the response and immediately-following
    // notifications; the client must establish the response-derived start
    // before releasing those notifications to the subscriber.
    write_wire_batch(
        &mut harness.app_stdout,
        [
            json!({
                "id": response_id,
                "result": {"turn": turn("turn-response-burst", "inProgress", json!([]))}
            }),
            json!({
                "method": "item/agentMessage/delta",
                "params": {
                    "threadId": "thread-response-burst",
                    "turnId": "turn-response-burst",
                    "itemId": "agent-response-burst",
                    "delta": "after response"
                }
            }),
            json!({
                "method": "turn/completed",
                "params": {
                    "threadId": "thread-response-burst",
                    "turn": turn("turn-response-burst", "completed", json!([]))
                }
            }),
        ],
    )
    .await;
    start
        .await
        .expect("turn/start task should not panic")
        .expect("response-only turn/start should succeed");

    assert!(matches!(
        recv_event(&mut subscription).await,
        AppServerEvent::TurnStarted { turn } if turn.id == "turn-response-burst"
    ));
    assert!(matches!(
        recv_event(&mut subscription).await,
        AppServerEvent::AgentMessageDelta { turn_id: event_turn, delta, .. }
            if event_turn == turn_id && delta == "after response"
    ));
    assert!(matches!(
        recv_event(&mut subscription).await,
        AppServerEvent::TurnCompleted(TurnOutcome { turn_id: event_turn, .. })
            if event_turn == turn_id
    ));

    harness
        .client
        .shutdown()
        .await
        .expect("client should shut down cleanly");
}

#[tokio::test]
async fn conflicting_item_completion_invalidates_instead_of_emitting_a_second_terminal() {
    let mut harness = client_harness(64 * 1024, 40).await;
    let thread_id = ThreadId::new("thread-item-conflict");
    let turn_id = TurnId::new("turn-item-conflict");
    let mut subscription = harness
        .client
        .subscribe(thread_id.clone())
        .await
        .expect("thread should be subscribable");

    send_item_completed(
        &mut harness.app_stdout,
        thread_id.as_str(),
        turn_id.as_str(),
        completed_agent_item("agent-conflict", "first authoritative value"),
    )
    .await;
    assert!(matches!(
        recv_event(&mut subscription).await,
        AppServerEvent::ItemCompleted { turn_id: event_turn, item }
            if event_turn == turn_id && item.id() == Some("agent-conflict")
    ));

    send_item_completed(
        &mut harness.app_stdout,
        thread_id.as_str(),
        turn_id.as_str(),
        completed_agent_item("agent-conflict", "conflicting authoritative value"),
    )
    .await;
    assert!(matches!(
        recv_event(&mut subscription).await,
        AppServerEvent::SubscriptionInvalidated {
            thread_id: invalidated_thread,
            reason: SubscriptionInvalidation::ProtocolDrift,
        } if invalidated_thread == thread_id
    ));
    assert!(
        subscription.recv().await.is_none(),
        "conflicting authoritative content must close rather than emit a second normal terminal"
    );
    assert!(subscription.outcome(&turn_id).is_none());

    harness
        .client
        .shutdown()
        .await
        .expect("client should shut down cleanly");
}

#[tokio::test]
async fn error_notification_survives_regular_backlog_and_redacts_debug_output() {
    let mut harness = client_harness(64 * 1024, 41).await;
    let mut subscription = harness
        .client
        .subscribe(ThreadId::new("thread-error-backlog"))
        .await
        .expect("thread should be subscribable");
    let private_message = "error includes private prompt and bearer token";
    let mut notifications = Vec::with_capacity(THREAD_EVENT_CAPACITY + 1);
    notifications.extend((0..THREAD_EVENT_CAPACITY).map(|index| {
        json!({
            "method": "future/noise",
            "params": {
                "threadId": "thread-error-backlog",
                "sequence": index
            }
        })
    }));
    notifications.push(json!({
        "method": "error",
        "params": {
            "threadId": "thread-error-backlog",
            "turnId": "turn-error-backlog",
            "error": {
                "message": private_message,
                "additionalDetails": "authorization=secret",
                "codexErrorInfo": {"type": "internalServerError"}
            },
            "willRetry": true
        }
    }));
    write_wire_batch(&mut harness.app_stdout, notifications).await;

    for _ in 0..THREAD_EVENT_CAPACITY {
        assert!(matches!(
            recv_event(&mut subscription).await,
            AppServerEvent::Unknown { method } if method == "future/noise"
        ));
    }
    let event = recv_event(&mut subscription).await;
    match &event {
        AppServerEvent::Error {
            turn_id,
            error,
            will_retry,
        } => {
            assert_eq!(turn_id.as_str(), "turn-error-backlog");
            assert_eq!(error.message, private_message);
            assert!(*will_retry);
        }
        event => panic!("error terminal should survive the regular backlog, got {event:?}"),
    }
    let rendered = format!("{event:?}");
    assert!(rendered.contains("will_retry: true"));
    assert!(!rendered.contains(private_message));
    assert!(!rendered.contains("authorization=secret"));

    harness
        .client
        .shutdown()
        .await
        .expect("client should shut down cleanly");
}
