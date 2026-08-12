use std::{collections::HashMap, time::Duration};

use lark_codex_bridge::codex::{
    protocol::RequestId,
    rpc::{ConnectionEpoch, RpcConnection, RpcError, RpcEvent, initialize_connection, spawn_rpc},
    transport::{TransportExit, spawn_stream_transport},
};
use lark_codex_bridge::limits::{
    EVENT_CAPACITY, MAX_JSONL_LINE_BYTES, RPC_HIGH_CAPACITY, RPC_INFLIGHT_CAPACITY,
    RPC_RELIABLE_EVENT_CAPACITY, RPC_TOTAL_PENDING_CAPACITY,
};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, duplex};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

const IO_TIMEOUT: Duration = Duration::from_secs(3);
const NO_MESSAGE_WINDOW: Duration = Duration::from_millis(50);

fn absolute_codex_home() -> &'static str {
    if cfg!(windows) {
        r"C:\scrubbed-codex-home"
    } else {
        "/tmp/scrubbed-codex-home"
    }
}

struct RpcHarness {
    app_stdout: DuplexStream,
    app_stdin: BufReader<DuplexStream>,
    _app_stderr: DuplexStream,
    connection: RpcConnection,
}

fn rpc_harness(stdin_capacity: usize, epoch: u64) -> RpcHarness {
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
    let connection = spawn_rpc(transport, ConnectionEpoch::new(epoch), cancellation.clone());

    RpcHarness {
        app_stdout,
        app_stdin: BufReader::new(app_stdin),
        _app_stderr: app_stderr,
        connection,
    }
}

async fn read_wire(reader: &mut BufReader<DuplexStream>) -> Value {
    let mut line = String::new();
    let bytes_read = timeout(IO_TIMEOUT, reader.read_line(&mut line))
        .await
        .expect("RPC should write a message before the timeout")
        .expect("RPC output should be readable");
    assert_ne!(
        bytes_read, 0,
        "RPC output should not reach EOF unexpectedly"
    );
    serde_json::from_str(&line).expect("RPC output should contain valid JSON")
}

async fn write_wire(writer: &mut DuplexStream, value: &Value) {
    let mut bytes = serde_json::to_vec(value).expect("test message should encode");
    bytes.push(b'\n');
    writer
        .write_all(&bytes)
        .await
        .expect("fake app-server message should be writable");
}

async fn recv_event(connection: &mut RpcConnection) -> RpcEvent {
    timeout(IO_TIMEOUT, connection.events.recv())
        .await
        .expect("RPC should emit an event before the timeout")
        .expect("RPC event channel should remain open")
}

fn wire_id(message: &Value) -> Value {
    message
        .get("id")
        .cloned()
        .expect("client request should contain an id")
}

#[tokio::test]
async fn notification_before_response_is_routed_without_stealing_the_response() {
    let mut harness = rpc_harness(64 * 1024, 7);
    assert_eq!(harness.connection.handle.epoch().get(), 7);

    let handle = harness.connection.handle.clone();
    let request = tokio::spawn(async move {
        let params = json!({"question": "safe"});
        handle
            .request::<_, Value>("example/read", &params, IO_TIMEOUT)
            .await
    });
    let outbound = read_wire(&mut harness.app_stdin).await;
    let id = wire_id(&outbound);

    write_wire(
        &mut harness.app_stdout,
        &json!({"method": "turn/started", "params": {"turnId": "turn-1"}}),
    )
    .await;
    write_wire(
        &mut harness.app_stdout,
        &json!({"id": id, "result": {"answer": 42}}),
    )
    .await;

    let event = recv_event(&mut harness.connection).await;
    assert!(matches!(
        event,
        RpcEvent::Notification { method, params }
            if method == "turn/started"
                && params == Some(json!({"turnId": "turn-1"}))
    ));
    let result = request
        .await
        .expect("request task should not panic")
        .expect("response should complete the matching request");
    assert_eq!(result, json!({"answer": 42}));

    harness.connection.shutdown().await;
}

#[tokio::test]
async fn concurrent_requests_complete_from_out_of_order_responses() {
    let mut harness = rpc_harness(64 * 1024, 3);
    let normal_handle = harness.connection.handle.clone();
    let normal = tokio::spawn(async move {
        let params = json!({"slot": "normal"});
        normal_handle
            .request::<_, Value>("example/normal", &params, IO_TIMEOUT)
            .await
    });
    let high_handle = harness.connection.handle.clone();
    let high = tokio::spawn(async move {
        let params = json!({"slot": "high"});
        high_handle
            .request_high::<_, Value>("example/high", &params, IO_TIMEOUT)
            .await
    });

    let first = read_wire(&mut harness.app_stdin).await;
    let second = read_wire(&mut harness.app_stdin).await;
    let requests = HashMap::from([
        (
            first["method"]
                .as_str()
                .expect("request should contain a method")
                .to_owned(),
            wire_id(&first),
        ),
        (
            second["method"]
                .as_str()
                .expect("request should contain a method")
                .to_owned(),
            wire_id(&second),
        ),
    ]);

    write_wire(
        &mut harness.app_stdout,
        &json!({
            "id": requests["example/normal"].clone(),
            "result": {"completed": "normal"}
        }),
    )
    .await;
    write_wire(
        &mut harness.app_stdout,
        &json!({
            "id": requests["example/high"].clone(),
            "result": {"completed": "high"}
        }),
    )
    .await;

    assert_eq!(
        normal
            .await
            .expect("normal request task should not panic")
            .expect("normal request should succeed"),
        json!({"completed": "normal"})
    );
    assert_eq!(
        high.await
            .expect("high request task should not panic")
            .expect("high request should succeed"),
        json!({"completed": "high"})
    );
    assert_eq!(harness.connection.handle.pending_count(), 0);

    harness.connection.shutdown().await;
}

#[tokio::test]
async fn high_request_reaches_wire_when_normal_inflight_is_saturated() {
    let mut harness = rpc_harness(64 * 1024, 39);
    let mut normal = Vec::with_capacity(RPC_INFLIGHT_CAPACITY);
    for sequence in 0..RPC_INFLIGHT_CAPACITY {
        let handle = harness.connection.handle.clone();
        normal.push(tokio::spawn(async move {
            handle
                .request::<_, Value>(
                    "example/normal-saturation",
                    &json!({"sequence": sequence}),
                    IO_TIMEOUT,
                )
                .await
        }));
    }
    for _ in 0..RPC_INFLIGHT_CAPACITY {
        let request = read_wire(&mut harness.app_stdin).await;
        assert_eq!(request["method"], "example/normal-saturation");
    }

    let handle = harness.connection.handle.clone();
    let high = tokio::spawn(async move {
        handle
            .request_high::<_, Value>(
                "turn/interrupt",
                &json!({"threadId": "thread-priority", "turnId": "turn-priority"}),
                IO_TIMEOUT,
            )
            .await
    });
    let request = timeout(
        Duration::from_millis(250),
        read_wire(&mut harness.app_stdin),
    )
    .await
    .expect("reserved high-priority inflight admission must make bounded progress");
    assert_eq!(request["method"], "turn/interrupt");

    high.abort();
    for task in normal {
        task.abort();
    }
    harness.connection.shutdown().await;
}

#[tokio::test]
async fn reliable_events_survive_a_full_normal_event_backlog_in_wire_order() {
    let mut harness = rpc_harness(256 * 1024, 40);
    let handle = harness.connection.handle.clone();
    let barrier = tokio::spawn(async move {
        handle
            .request::<_, Value>("example/reliable-barrier", &json!({}), IO_TIMEOUT)
            .await
    });
    let barrier_request = read_wire(&mut harness.app_stdin).await;
    for sequence in 0..EVENT_CAPACITY {
        write_wire(
            &mut harness.app_stdout,
            &json!({"method": "example/progress", "params": {"sequence": sequence}}),
        )
        .await;
    }
    write_wire(
        &mut harness.app_stdout,
        &json!({"id": "approval-reliable", "method": "approval/request", "params": {}}),
    )
    .await;
    write_wire(
        &mut harness.app_stdout,
        &json!({
            "method": "turn/completed",
            "params": {"threadId": "thread-reliable", "turn": {"id": "turn-reliable"}}
        }),
    )
    .await;
    write_wire(
        &mut harness.app_stdout,
        &json!({"id": wire_id(&barrier_request), "result": {"observed": true}}),
    )
    .await;
    assert_eq!(
        barrier
            .await
            .expect("barrier request should not panic")
            .expect("actor must process reliable events before the later response"),
        json!({"observed": true})
    );

    for sequence in 0..EVENT_CAPACITY {
        assert!(matches!(
            recv_event(&mut harness.connection).await,
            RpcEvent::Notification { method, params }
                if method == "example/progress"
                    && params == Some(json!({"sequence": sequence}))
        ));
    }
    let mut request = match recv_event(&mut harness.connection).await {
        RpcEvent::ServerRequest(request) => request,
        other => panic!("expected reliable server request, got {other:?}"),
    };
    assert_eq!(
        request.id(),
        &RequestId::String("approval-reliable".to_owned())
    );
    assert!(matches!(
        recv_event(&mut harness.connection).await,
        RpcEvent::Notification { method, .. } if method == "turn/completed"
    ));
    harness
        .connection
        .handle
        .respond_request(&mut request, &json!({"decision": "accept"}))
        .await
        .expect("reliably delivered request should remain answerable");
    assert_eq!(
        read_wire(&mut harness.app_stdin).await["id"],
        "approval-reliable"
    );

    harness.connection.shutdown().await;
}

#[tokio::test]
async fn timed_out_request_is_removed_from_the_pending_map() {
    let mut harness = rpc_harness(64 * 1024, 4);
    let handle = harness.connection.handle.clone();
    let request = tokio::spawn(async move {
        let params = json!({});
        handle
            .request::<_, Value>("example/slow", &params, Duration::from_millis(100))
            .await
    });
    let _outbound = read_wire(&mut harness.app_stdin).await;

    let error = request
        .await
        .expect("timed request task should not panic")
        .expect_err("request should time out without a response");
    assert!(matches!(error, RpcError::Timeout { .. }));
    assert_eq!(harness.connection.handle.pending_count(), 0);

    harness.connection.shutdown().await;
}

#[tokio::test]
async fn server_error_exposes_the_code_but_redacts_message_and_data() {
    let mut harness = rpc_harness(64 * 1024, 5);
    let handle = harness.connection.handle.clone();
    let request = tokio::spawn(async move {
        let params = json!({});
        handle
            .request::<_, Value>("example/fails", &params, IO_TIMEOUT)
            .await
    });
    let outbound = read_wire(&mut harness.app_stdin).await;
    write_wire(
        &mut harness.app_stdout,
        &json!({
            "id": wire_id(&outbound),
            "error": {
                "code": -32001,
                "message": "secret prompt material",
                "data": {"authorization": "secret-token"}
            }
        }),
    )
    .await;

    let error = request
        .await
        .expect("failed request task should not panic")
        .expect_err("error response should fail the request");
    assert!(matches!(&error, RpcError::Server { code: -32001, .. }));
    let rendered = format!("{error} {error:?}");
    assert!(!rendered.contains("secret prompt material"));
    assert!(!rendered.contains("secret-token"));
    assert!(rendered.contains("-32001"));

    harness.connection.shutdown().await;
}

#[tokio::test]
async fn integer_server_request_ids_are_preserved_in_success_and_error_responses() {
    let mut harness = rpc_harness(64 * 1024, 6);
    write_wire(
        &mut harness.app_stdout,
        &json!({
            "id": 42,
            "method": "item/commandExecution/requestApproval",
            "params": {"command": "redacted"}
        }),
    )
    .await;
    let event = recv_event(&mut harness.connection).await;
    let mut request = match event {
        RpcEvent::ServerRequest(request) => request,
        other => panic!("expected a server request, got {other:?}"),
    };
    assert_eq!(request.id(), &RequestId::Integer(42));
    assert_eq!(request.method, "item/commandExecution/requestApproval");
    assert_eq!(request.params, Some(json!({"command": "redacted"})));
    harness
        .connection
        .handle
        .respond_request(&mut request, &json!({"decision": "accept"}))
        .await
        .expect("server request response should queue");
    assert_eq!(
        read_wire(&mut harness.app_stdin).await,
        json!({"id": 42, "result": {"decision": "accept"}})
    );

    write_wire(
        &mut harness.app_stdout,
        &json!({"id": 43, "method": "example/reject", "params": {}}),
    )
    .await;
    let event = recv_event(&mut harness.connection).await;
    let mut request = match event {
        RpcEvent::ServerRequest(request) => request,
        other => panic!("expected a server request, got {other:?}"),
    };
    harness
        .connection
        .handle
        .respond_request_error(&mut request, -32601, "unsupported")
        .await
        .expect("server request error should queue");
    assert_eq!(
        read_wire(&mut harness.app_stdin).await,
        json!({"id": 43, "error": {"code": -32601, "message": "unsupported"}})
    );

    harness.connection.shutdown().await;
}

#[tokio::test]
async fn stale_server_request_tokens_cannot_answer_a_reused_id_on_a_new_epoch() {
    let mut old = rpc_harness(64 * 1024, 30);
    write_wire(
        &mut old.app_stdout,
        &json!({"id": 7, "method": "approval/request", "params": {}}),
    )
    .await;
    let mut stale = match recv_event(&mut old.connection).await {
        RpcEvent::ServerRequest(request) => request,
        other => panic!("expected a server request, got {other:?}"),
    };
    assert_eq!(stale.epoch().get(), 30);

    let mut current = rpc_harness(64 * 1024, 31);
    write_wire(
        &mut current.app_stdout,
        &json!({"id": 7, "method": "approval/request", "params": {}}),
    )
    .await;
    let mut current_request = match recv_event(&mut current.connection).await {
        RpcEvent::ServerRequest(request) => request,
        other => panic!("expected a server request, got {other:?}"),
    };

    let error = current
        .connection
        .handle
        .respond_request(&mut stale, &json!({"decision": "accept"}))
        .await
        .expect_err("an old-epoch token must not approve a new request with the same ID");
    assert!(matches!(error, RpcError::UnknownServerRequest));

    let mut unexpected = String::new();
    assert!(
        timeout(
            NO_MESSAGE_WINDOW,
            current.app_stdin.read_line(&mut unexpected)
        )
        .await
        .is_err(),
        "a stale request token must not write a response"
    );

    current
        .connection
        .handle
        .respond_request_error(&mut current_request, -32000, "declined")
        .await
        .expect("the current-epoch request should remain answerable");

    old.connection.shutdown().await;
    current.connection.shutdown().await;
}

#[tokio::test]
async fn rejected_oversized_response_keeps_the_server_request_token_retryable() {
    let mut harness = rpc_harness(64 * 1024, 32);
    write_wire(
        &mut harness.app_stdout,
        &json!({"id": 8, "method": "approval/request", "params": {}}),
    )
    .await;
    let mut request = match recv_event(&mut harness.connection).await {
        RpcEvent::ServerRequest(request) => request,
        other => panic!("expected a server request, got {other:?}"),
    };

    let oversized = "x".repeat(MAX_JSONL_LINE_BYTES + 1);
    let error = harness
        .connection
        .handle
        .respond_request(&mut request, &oversized)
        .await
        .expect_err("an oversized response should fail before consuming the token");
    assert!(matches!(error, RpcError::PayloadTooLarge { .. }));

    harness
        .connection
        .handle
        .respond_request_error(&mut request, -32000, "response too large")
        .await
        .expect("the same token should allow a bounded fallback response");
    assert_eq!(
        read_wire(&mut harness.app_stdin).await,
        json!({
            "id": 8,
            "error": {"code": -32000, "message": "response too large"}
        })
    );

    harness.connection.shutdown().await;
}

#[tokio::test]
async fn server_responses_overtake_a_backpressured_normal_queue() {
    const NORMAL_BACKLOG: usize = 8;

    let mut harness = rpc_harness(64, 8);
    write_wire(
        &mut harness.app_stdout,
        &json!({"id": 91, "method": "approval/request", "params": {}}),
    )
    .await;
    let mut approval = match recv_event(&mut harness.connection).await {
        RpcEvent::ServerRequest(request) => request,
        other => panic!("expected a server request, got {other:?}"),
    };

    let large_value = "x".repeat(16 * 1024);
    for sequence in 0..NORMAL_BACKLOG {
        harness
            .connection
            .handle
            .notify(
                "example/backlog",
                &json!({"sequence": sequence, "padding": large_value}),
            )
            .await
            .expect("normal notification should enter the bounded queue");
    }
    let handle = harness.connection.handle.clone();
    let response = tokio::spawn(async move {
        handle
            .respond_request(&mut approval, &json!({"decision": "accept"}))
            .await
    });

    let mut normal_before_response = 0;
    loop {
        let message = read_wire(&mut harness.app_stdin).await;
        if message.get("id") == Some(&json!(91)) {
            assert_eq!(message["result"], json!({"decision": "accept"}));
            break;
        }
        assert_eq!(message["method"], "example/backlog");
        normal_before_response += 1;
    }
    assert!(
        normal_before_response < NORMAL_BACKLOG,
        "a high-priority server response must overtake queued normal traffic"
    );
    response
        .await
        .expect("response task should not panic")
        .expect("high-priority response should be flushed to the wire");

    harness.connection.shutdown().await;
}

#[tokio::test]
async fn cancelled_server_response_still_reaches_the_wire_and_consumes_the_token() {
    let mut harness = rpc_harness(64, 34);
    write_wire(
        &mut harness.app_stdout,
        &json!({"id": "approval-cancel", "method": "approval/request", "params": {}}),
    )
    .await;
    let mut approval = match recv_event(&mut harness.connection).await {
        RpcEvent::ServerRequest(request) => request,
        other => panic!("expected a server request, got {other:?}"),
    };

    let handle = harness.connection.handle.clone();
    let responder = tokio::spawn(async move {
        handle
            .respond_request(&mut approval, &json!({"decision": "decline"}))
            .await
    });
    tokio::task::yield_now().await;
    responder.abort();
    let _ = responder.await;

    let response = read_wire(&mut harness.app_stdin).await;
    assert_eq!(response["id"], "approval-cancel");
    assert_eq!(response["result"]["decision"], "decline");

    let handle = harness.connection.handle.clone();
    let health = tokio::spawn(async move {
        handle
            .request::<_, Value>("example/after-cancelled-response", &json!({}), IO_TIMEOUT)
            .await
    });
    let request = read_wire(&mut harness.app_stdin).await;
    assert_eq!(request["method"], "example/after-cancelled-response");
    write_wire(
        &mut harness.app_stdout,
        &json!({"id": wire_id(&request), "result": {"healthy": true}}),
    )
    .await;
    assert_eq!(
        health
            .await
            .expect("health request should not panic")
            .expect("caller cancellation must not poison a successful background response"),
        json!({"healthy": true})
    );

    harness.connection.shutdown().await;
}

#[tokio::test]
async fn completion_pressure_cannot_deadlock_shutdown_or_terminal_delivery() {
    let mut harness = rpc_harness(1, 41);
    let mut requests = Vec::with_capacity(RPC_TOTAL_PENDING_CAPACITY);
    for sequence in 0..RPC_INFLIGHT_CAPACITY {
        let handle = harness.connection.handle.clone();
        requests.push(tokio::spawn(async move {
            handle
                .request::<_, Value>(
                    "example/completion-pressure-normal",
                    &json!({"sequence": sequence}),
                    IO_TIMEOUT,
                )
                .await
        }));
    }
    for sequence in 0..RPC_HIGH_CAPACITY {
        let handle = harness.connection.handle.clone();
        requests.push(tokio::spawn(async move {
            handle
                .request_high::<_, Value>(
                    "example/completion-pressure-high",
                    &json!({"sequence": sequence}),
                    IO_TIMEOUT,
                )
                .await
        }));
    }
    let expected_pending = RPC_TOTAL_PENDING_CAPACITY;
    timeout(IO_TIMEOUT, async {
        while harness.connection.handle.pending_count() != expected_pending {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all pressure requests should enter the actor before it is blocked");

    for sequence in 0..=RPC_RELIABLE_EVENT_CAPACITY {
        write_wire(
            &mut harness.app_stdout,
            &json!({
                "method": "turn/completed",
                "params": {"threadId": "thread-pressure", "sequence": sequence}
            }),
        )
        .await;
    }

    for _ in 0..expected_pending {
        let _ = read_wire(&mut harness.app_stdin).await;
    }
    let exit = timeout(Duration::from_millis(500), harness.connection.shutdown())
        .await
        .expect("completion pressure must not make shutdown wait on its own pumps");
    assert_eq!(exit, TransportExit::Cancelled);

    for _ in 0..RPC_RELIABLE_EVENT_CAPACITY {
        assert!(matches!(
            recv_event(&mut harness.connection).await,
            RpcEvent::Notification { method, .. } if method == "turn/completed"
        ));
    }
    assert!(matches!(
        recv_event(&mut harness.connection).await,
        RpcEvent::TransportClosed(TransportExit::Cancelled)
    ));
    for request in requests {
        request.abort();
    }
}

#[tokio::test]
async fn high_cancellations_survive_a_full_normal_cancellation_backlog() {
    let mut harness = rpc_harness(256 * 1024, 42);
    let total_pending = RPC_TOTAL_PENDING_CAPACITY;
    let mut normal = Vec::with_capacity(RPC_INFLIGHT_CAPACITY);
    for sequence in 0..RPC_INFLIGHT_CAPACITY {
        let handle = harness.connection.handle.clone();
        normal.push(tokio::spawn(async move {
            handle
                .request::<_, Value>(
                    "example/cancel-normal",
                    &json!({"sequence": sequence}),
                    IO_TIMEOUT,
                )
                .await
        }));
    }
    let mut high = Vec::with_capacity(RPC_HIGH_CAPACITY);
    for sequence in 0..RPC_HIGH_CAPACITY {
        let handle = harness.connection.handle.clone();
        high.push(tokio::spawn(async move {
            handle
                .request_high::<_, Value>(
                    "example/cancel-high",
                    &json!({"sequence": sequence}),
                    IO_TIMEOUT,
                )
                .await
        }));
    }
    for _ in 0..total_pending {
        let _ = read_wire(&mut harness.app_stdin).await;
    }
    assert_eq!(harness.connection.handle.pending_count(), total_pending);

    for sequence in 0..=RPC_RELIABLE_EVENT_CAPACITY {
        write_wire(
            &mut harness.app_stdout,
            &json!({
                "method": "turn/completed",
                "params": {"threadId": "thread-cancel-pressure", "sequence": sequence}
            }),
        )
        .await;
    }
    tokio::time::sleep(Duration::from_millis(25)).await;

    for request in normal {
        request.abort();
        let _ = request.await;
    }
    for request in high {
        request.abort();
        let _ = request.await;
    }
    for _ in 0..=RPC_RELIABLE_EVENT_CAPACITY {
        assert!(matches!(
            recv_event(&mut harness.connection).await,
            RpcEvent::Notification { method, .. } if method == "turn/completed"
        ));
    }
    timeout(Duration::from_millis(250), async {
        while harness.connection.handle.pending_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all normal and high cancellation IDs must fit the cancellation lane");

    let handle = harness.connection.handle.clone();
    let replacement = tokio::spawn(async move {
        handle
            .request_high::<_, Value>("turn/interrupt", &json!({}), IO_TIMEOUT)
            .await
    });
    let request = timeout(
        Duration::from_millis(250),
        read_wire(&mut harness.app_stdin),
    )
    .await
    .expect("cancelled high requests must release admission for a new interrupt");
    assert_eq!(request["method"], "turn/interrupt");
    replacement.abort();

    harness.connection.shutdown().await;
}

#[tokio::test]
async fn abandoned_server_request_fails_the_epoch_closed() {
    let mut harness = rpc_harness(64 * 1024, 35);
    write_wire(
        &mut harness.app_stdout,
        &json!({"id": "approval-abandon", "method": "approval/request", "params": {}}),
    )
    .await;
    let mut request = match recv_event(&mut harness.connection).await {
        RpcEvent::ServerRequest(request) => request,
        other => panic!("expected a server request, got {other:?}"),
    };
    harness
        .connection
        .handle
        .abandon_request(&mut request)
        .expect("request token should fail the epoch closed");

    let exit = harness.connection.shutdown().await;
    assert_eq!(exit, TransportExit::Cancelled);
}

#[tokio::test]
async fn dropping_an_unanswered_server_request_fails_the_epoch_closed() {
    let mut harness = rpc_harness(64 * 1024, 36);
    write_wire(
        &mut harness.app_stdout,
        &json!({"id": "approval-drop", "method": "approval/request", "params": {}}),
    )
    .await;
    let request = match recv_event(&mut harness.connection).await {
        RpcEvent::ServerRequest(request) => request,
        other => panic!("expected a server request, got {other:?}"),
    };
    drop(request);

    let exit = harness.connection.shutdown().await;
    assert_eq!(exit, TransportExit::Cancelled);
}

#[tokio::test]
async fn stdout_eof_fails_every_pending_request_for_the_connection_epoch() {
    let mut harness = rpc_harness(64 * 1024, 12);
    let first_handle = harness.connection.handle.clone();
    let first = tokio::spawn(async move {
        let params = json!({"request": 1});
        first_handle
            .request::<_, Value>("example/first", &params, IO_TIMEOUT)
            .await
    });
    let second_handle = harness.connection.handle.clone();
    let second = tokio::spawn(async move {
        let params = json!({"request": 2});
        second_handle
            .request::<_, Value>("example/second", &params, IO_TIMEOUT)
            .await
    });
    let _first_wire = read_wire(&mut harness.app_stdin).await;
    let _second_wire = read_wire(&mut harness.app_stdin).await;
    harness
        .app_stdout
        .shutdown()
        .await
        .expect("fake server stdout should close");

    for result in [first, second] {
        let error = result
            .await
            .expect("pending request task should not panic")
            .expect_err("EOF should fail pending requests");
        assert!(matches!(
            error,
            RpcError::ConnectionLost(epoch) if epoch.get() == 12
        ));
    }
    assert_eq!(harness.connection.handle.pending_count(), 0);
    assert!(matches!(
        recv_event(&mut harness.connection).await,
        RpcEvent::TransportClosed(TransportExit::StdoutEof)
    ));

    harness.connection.shutdown().await;
}

#[tokio::test]
async fn late_old_epoch_and_unknown_responses_are_drift_not_current_completion() {
    let mut harness = rpc_harness(64 * 1024, 20);
    let handle = harness.connection.handle.clone();
    let pending = tokio::spawn(async move {
        let params = json!({});
        handle
            .request::<_, Value>("example/current", &params, IO_TIMEOUT)
            .await
    });
    let current_request = read_wire(&mut harness.app_stdin).await;
    let current_id = wire_id(&current_request);

    write_wire(
        &mut harness.app_stdout,
        &json!({"id": "c:19:999", "result": {"wrong": true}}),
    )
    .await;
    assert!(matches!(
        recv_event(&mut harness.connection).await,
        RpcEvent::ProtocolDrift
    ));
    assert_eq!(harness.connection.handle.protocol_drift_count(), 1);
    assert!(
        !pending.is_finished(),
        "an old-epoch response must not complete a current request"
    );

    write_wire(
        &mut harness.app_stdout,
        &json!({"id": current_id, "result": {"right": true}}),
    )
    .await;
    assert_eq!(
        pending
            .await
            .expect("current request task should not panic")
            .expect("current response should complete its request"),
        json!({"right": true})
    );

    write_wire(
        &mut harness.app_stdout,
        &json!({"id": "c:20:999999", "result": null}),
    )
    .await;
    assert!(matches!(
        recv_event(&mut harness.connection).await,
        RpcEvent::ProtocolDrift
    ));
    assert_eq!(harness.connection.handle.protocol_drift_count(), 2);

    harness.connection.shutdown().await;
}

#[tokio::test]
async fn concurrent_initialize_calls_emit_one_request_and_reject_one_immediately() {
    let mut harness = rpc_harness(64 * 1024, 22);
    let first_handle = harness.connection.handle.clone();
    let second_handle = harness.connection.handle.clone();
    let mut first = tokio::spawn(async move { initialize_connection(&first_handle).await });
    let mut second = tokio::spawn(async move { initialize_connection(&second_handle).await });

    let initialize_request = read_wire(&mut harness.app_stdin).await;
    assert_eq!(initialize_request["method"], "initialize");

    let (first_was_rejected, rejected) = timeout(IO_TIMEOUT, async {
        tokio::select! {
            result = &mut first => (true, result),
            result = &mut second => (false, result),
        }
    })
    .await
    .expect("one concurrent initialize call should be rejected before any response");
    let Err(error) = rejected.expect("initialize task should not panic") else {
        panic!("the losing initialize call must not succeed");
    };
    assert!(matches!(error, RpcError::AlreadyInitialized));
    assert!(
        if first_was_rejected {
            !second.is_finished()
        } else {
            !first.is_finished()
        },
        "the winning initialize call should still be waiting for its response"
    );

    let mut unexpected = String::new();
    assert!(
        timeout(
            NO_MESSAGE_WINDOW,
            harness.app_stdin.read_line(&mut unexpected)
        )
        .await
        .is_err(),
        "concurrent initialization must emit exactly one initialize request"
    );

    write_wire(
        &mut harness.app_stdout,
        &json!({
            "id": wire_id(&initialize_request),
            "result": {
                "codexHome": absolute_codex_home(),
                "platformFamily": "unix",
                "platformOs": "linux",
                "userAgent": "codex-cli/0.146.0"
            }
        }),
    )
    .await;
    let accepted = if first_was_rejected {
        second.await
    } else {
        first.await
    }
    .expect("winning initialize task should not panic")
    .expect("winning initialize call should succeed");
    assert_eq!(accepted.user_agent, "codex-cli/0.146.0");
    assert_eq!(
        read_wire(&mut harness.app_stdin).await,
        json!({"method": "initialized", "params": {}})
    );

    harness.connection.shutdown().await;
}

#[tokio::test]
async fn aborting_a_caller_removes_its_registered_pending_request() {
    let mut harness = rpc_harness(64 * 1024, 23);
    let handle = harness.connection.handle.clone();
    let request = tokio::spawn(async move {
        let params = json!({});
        handle
            .request::<_, Value>("example/abandoned", &params, IO_TIMEOUT)
            .await
    });
    let _outbound = read_wire(&mut harness.app_stdin).await;
    assert_eq!(harness.connection.handle.pending_count(), 1);

    request.abort();
    assert!(
        request
            .await
            .expect_err("aborted caller should not finish normally")
            .is_cancelled()
    );
    timeout(IO_TIMEOUT, async {
        while harness.connection.handle.pending_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("aborted caller should eventually be removed from pending state");

    harness.connection.shutdown().await;
}

#[tokio::test]
async fn unread_notification_events_do_not_block_an_interleaved_response_below_capacity() {
    const NOTIFICATION_COUNT: usize = 128;

    let mut harness = rpc_harness(64 * 1024, 24);
    let handle = harness.connection.handle.clone();
    let request = tokio::spawn(async move {
        let params = json!({});
        handle
            .request::<_, Value>("example/after-notifications", &params, IO_TIMEOUT)
            .await
    });
    let outbound = read_wire(&mut harness.app_stdin).await;

    for sequence in 0..NOTIFICATION_COUNT {
        write_wire(
            &mut harness.app_stdout,
            &json!({"method": "example/progress", "params": {"sequence": sequence}}),
        )
        .await;
    }
    write_wire(
        &mut harness.app_stdout,
        &json!({"id": wire_id(&outbound), "result": {"completed": true}}),
    )
    .await;

    assert_eq!(
        request
            .await
            .expect("request task should not panic")
            .expect("response should complete without draining notification events"),
        json!({"completed": true})
    );
    assert_eq!(harness.connection.handle.pending_count(), 0);

    harness.connection.shutdown().await;
}

#[tokio::test]
async fn excess_progress_notifications_are_dropped_without_losing_rpc_correlation() {
    let mut harness = rpc_harness(128 * 1024, 26);
    for sequence in 0..EVENT_CAPACITY + 64 {
        write_wire(
            &mut harness.app_stdout,
            &json!({"method": "example/progress", "params": {"sequence": sequence}}),
        )
        .await;
    }

    let handle = harness.connection.handle.clone();
    let request = tokio::spawn(async move {
        handle
            .request::<_, Value>("example/after-lag", &json!({}), IO_TIMEOUT)
            .await
    });
    let outbound = read_wire(&mut harness.app_stdin).await;
    write_wire(
        &mut harness.app_stdout,
        &json!({"id": wire_id(&outbound), "result": {"completed": true}}),
    )
    .await;

    assert_eq!(
        request
            .await
            .expect("request task should not panic")
            .expect("progress lag must not close or block response correlation"),
        json!({"completed": true})
    );
    assert!(
        harness.connection.handle.dropped_notification_count() > 0,
        "overflowed progress must be observable through a bounded counter"
    );

    harness.connection.shutdown().await;
}

#[tokio::test]
async fn caller_deadline_fires_during_a_continuous_notification_stream() {
    let RpcHarness {
        mut app_stdout,
        mut app_stdin,
        _app_stderr,
        mut connection,
    } = rpc_harness(64 * 1024, 27);
    let handle = connection.handle.clone();
    let mut request = tokio::spawn(async move {
        handle
            .request::<_, Value>(
                "example/times-out-under-load",
                &json!({}),
                Duration::from_millis(100),
            )
            .await
    });
    let _outbound = read_wire(&mut app_stdin).await;

    let flood = tokio::spawn(async move {
        let mut sequence = 0_u64;
        loop {
            write_wire(
                &mut app_stdout,
                &json!({"method": "example/progress", "params": {"sequence": sequence}}),
            )
            .await;
            sequence = sequence.wrapping_add(1);
        }
    });

    let error = timeout(IO_TIMEOUT, async {
        loop {
            tokio::select! {
                result = &mut request => {
                    break result
                        .expect("request task should not panic")
                        .expect_err("missing response should reach its absolute deadline");
                }
                event = connection.events.recv() => {
                    assert!(matches!(event, Some(RpcEvent::Notification { .. })));
                }
            }
        }
    })
    .await
    .expect("continuous input must not starve the caller's deadline");
    assert!(matches!(error, RpcError::Timeout { .. }));

    flood.abort();
    let _ = flood.await;
    timeout(IO_TIMEOUT, async {
        while connection.handle.pending_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("timed-out request should be removed under notification load");
    connection.shutdown().await;
}

#[tokio::test]
async fn oversized_params_are_rejected_before_a_rpc_value_is_queued() {
    let mut harness = rpc_harness(64 * 1024, 28);
    let oversized = "x".repeat(MAX_JSONL_LINE_BYTES + 1);
    let error = harness
        .connection
        .handle
        .notify("example/oversized", &oversized)
        .await
        .expect_err("oversized params should be rejected by the RPC admission boundary");
    assert!(matches!(error, RpcError::PayloadTooLarge { .. }));
    let mut unexpected = String::new();
    assert!(
        timeout(
            NO_MESSAGE_WINDOW,
            harness.app_stdin.read_line(&mut unexpected)
        )
        .await
        .is_err(),
        "rejected params must not reach transport"
    );
    harness.connection.shutdown().await;
}

#[tokio::test]
async fn request_after_shutdown_fails_immediately_with_the_closed_epoch() {
    let mut harness = rpc_harness(64 * 1024, 25);
    let handle = harness.connection.handle.clone();
    harness.connection.shutdown().await;

    let params = json!({});
    let result = timeout(
        Duration::from_millis(250),
        handle.request::<_, Value>("example/after-shutdown", &params, IO_TIMEOUT),
    )
    .await
    .expect("request after shutdown should fail without waiting for its deadline");
    assert!(matches!(
        result,
        Err(RpcError::ConnectionLost(epoch)) if epoch.get() == 25
    ));
}

#[tokio::test]
async fn initialize_then_initialized_is_exactly_ordered_and_only_runs_once() {
    let mut harness = rpc_harness(64 * 1024, 21);
    let handle = harness.connection.handle.clone();
    let initialize = tokio::spawn(async move { initialize_connection(&handle).await });

    let initialize_request = read_wire(&mut harness.app_stdin).await;
    assert_eq!(initialize_request["method"], "initialize");
    assert_eq!(
        initialize_request["params"]["clientInfo"],
        json!({
            "name": "lark_codex_bridge",
            "title": "Lark Codex Bridge",
            "version": env!("CARGO_PKG_VERSION")
        })
    );
    assert!(
        !initialize_request.to_string().contains("experimentalApi"),
        "stable handshake must not opt into experimental APIs"
    );

    let mut unexpected = String::new();
    assert!(
        timeout(
            NO_MESSAGE_WINDOW,
            harness.app_stdin.read_line(&mut unexpected)
        )
        .await
        .is_err(),
        "initialized must not be sent before initialize succeeds"
    );

    write_wire(
        &mut harness.app_stdout,
        &json!({
            "id": wire_id(&initialize_request),
            "result": {
                "codexHome": absolute_codex_home(),
                "platformFamily": "unix",
                "platformOs": "linux",
                "userAgent": "codex-cli/0.146.0"
            }
        }),
    )
    .await;
    let result = initialize
        .await
        .expect("initialize task should not panic")
        .expect("initialize handshake should succeed");
    assert_eq!(result.user_agent, "codex-cli/0.146.0");

    let initialized = read_wire(&mut harness.app_stdin).await;
    assert_eq!(initialized, json!({"method": "initialized", "params": {}}));

    let Err(error) = initialize_connection(&harness.connection.handle).await else {
        panic!("a second initialize call should fail locally");
    };
    assert!(matches!(error, RpcError::AlreadyInitialized));
    let mut unexpected = String::new();
    assert!(
        timeout(
            NO_MESSAGE_WINDOW,
            harness.app_stdin.read_line(&mut unexpected)
        )
        .await
        .is_err(),
        "a rejected second initialize must not write another request"
    );

    harness.connection.shutdown().await;
}
