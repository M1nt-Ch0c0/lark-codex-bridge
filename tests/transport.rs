use std::{
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use lark_codex_bridge::{
    codex::{
        process::{CodexProcessConfig, probe_version},
        protocol::{InboundMessage, OutboundMessage, RequestId},
        transport::{
            TransportEvent, TransportEventReceiver, TransportHandle, spawn_stream_transport,
        },
    },
    limits::{EVENT_CAPACITY, MAX_JSONL_LINE_BYTES, MAX_STDERR_LINE_BYTES},
};
use serde_json::{Value, json};
use tokio::{
    io::{
        AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, DuplexStream, duplex,
    },
    time::timeout,
};
use tokio_util::sync::CancellationToken;

const EVENT_TIMEOUT: Duration = Duration::from_secs(3);

struct TestTransport {
    app_stdout: DuplexStream,
    app_stdin: DuplexStream,
    app_stderr: DuplexStream,
    cancellation: CancellationToken,
    handle: TransportHandle,
}

fn test_transport(stdin_capacity: usize) -> TestTransport {
    let (transport_stdout, app_stdout) = duplex(64 * 1024);
    let (transport_stdin, app_stdin) = duplex(stdin_capacity);
    let (transport_stderr, app_stderr) = duplex(64 * 1024);
    let cancellation = CancellationToken::new();
    let handle = spawn_stream_transport(
        transport_stdout,
        transport_stdin,
        transport_stderr,
        cancellation.clone(),
    );

    TestTransport {
        app_stdout,
        app_stdin,
        app_stderr,
        cancellation,
        handle,
    }
}

async fn next_event(events: &mut TransportEventReceiver) -> TransportEvent {
    timeout(EVENT_TIMEOUT, events.recv())
        .await
        .expect("transport should emit an event before the timeout")
        .expect("transport event channel should remain open")
}

fn notification(method: &str) -> OutboundMessage {
    OutboundMessage::Notification {
        method: method.to_owned(),
        params: None,
    }
}

async fn read_method(reader: &mut BufReader<DuplexStream>) -> String {
    let mut line = String::new();
    timeout(EVENT_TIMEOUT, reader.read_line(&mut line))
        .await
        .expect("transport should write a line before the timeout")
        .expect("written line should be readable");
    let value: Value = serde_json::from_str(&line).expect("written line should contain JSON");
    value["method"]
        .as_str()
        .expect("written notification should contain a method")
        .to_owned()
}

#[tokio::test]
async fn stdout_waits_for_a_complete_line_across_partial_reads() {
    let mut transport = test_transport(1024);

    transport
        .app_stdout
        .write_all(br#"{"id":"request-1","res"#)
        .await
        .expect("first fragment should be writable");
    tokio::task::yield_now().await;
    assert!(
        transport.handle.events.try_recv().is_err(),
        "an incomplete JSONL record must not emit an event"
    );

    transport
        .app_stdout
        .write_all(b"ult\":{\"thread\":{\"id\":\"thread-1\"}}}\n")
        .await
        .expect("second fragment should be writable");

    let event = next_event(&mut transport.handle.events).await;
    assert!(matches!(
        event,
        TransportEvent::Message(message)
            if matches!(message.message(), InboundMessage::Response {
            id: RequestId::String(id),
            result,
        } if id == "request-1" && result == &json!({"thread": {"id": "thread-1"}}))
    ));
}

#[tokio::test]
async fn stdout_decodes_several_records_from_one_read_in_wire_order() {
    let mut transport = test_transport(1024);
    transport
        .app_stdout
        .write_all(
            b"{\"method\":\"turn/started\",\"params\":{\"turnId\":\"turn-1\"}}\n\
              {\"id\":7,\"result\":{\"ok\":true}}\n",
        )
        .await
        .expect("records should be writable together");

    let first = next_event(&mut transport.handle.events).await;
    let second = next_event(&mut transport.handle.events).await;
    assert!(matches!(
        first,
        TransportEvent::Message(message)
            if matches!(message.message(), InboundMessage::Notification { method, .. }
                if method == "turn/started")
    ));
    assert!(matches!(
        second,
        TransportEvent::Message(message)
            if matches!(message.message(), InboundMessage::Response {
                id: RequestId::Integer(7), result,
            } if result == &json!({"ok": true}))
    ));
}

#[tokio::test]
async fn malformed_stdout_record_is_reported_without_losing_the_next_record() {
    let mut transport = test_transport(1024);
    transport
        .app_stdout
        .write_all(b"this is not JSON\n{\"method\":\"thread/started\"}\n")
        .await
        .expect("records should be writable");

    assert!(matches!(
        next_event(&mut transport.handle.events).await,
        TransportEvent::ProtocolError(_)
    ));
    assert!(matches!(
        next_event(&mut transport.handle.events).await,
        TransportEvent::Message(message)
            if matches!(message.message(), InboundMessage::Notification { method, .. }
                if method == "thread/started")
    ));
}

#[tokio::test]
async fn oversized_stdout_record_reports_protocol_error_and_cancels_connection() {
    let mut transport = test_transport(1024);
    let mut app_stdout = transport.app_stdout;
    let oversized_writer = tokio::spawn(async move {
        let oversized = vec![b'x'; MAX_JSONL_LINE_BYTES + 1];
        let _ = app_stdout.write_all(&oversized).await;
        let _ = app_stdout.write_all(b"\n").await;
    });

    assert!(matches!(
        next_event(&mut transport.handle.events).await,
        TransportEvent::ProtocolError(_)
    ));
    assert!(transport.handle.high_tx.is_closed());
    assert!(
        !transport.cancellation.is_cancelled(),
        "a child transport failure must not cancel its parent scope"
    );

    oversized_writer.abort();
}

#[tokio::test]
async fn high_priority_record_overtakes_an_already_queued_normal_record() {
    let mut transport = test_transport(1);
    transport
        .handle
        .normal_tx
        .send(notification("blocker"))
        .await
        .expect("blocker should enqueue");

    let mut first_byte = [0_u8; 1];
    transport
        .app_stdin
        .read_exact(&mut first_byte)
        .await
        .expect("writer should begin the blocker");
    assert_eq!(first_byte, [b'{']);

    transport
        .handle
        .normal_tx
        .send(notification("normal"))
        .await
        .expect("normal record should enqueue");
    transport
        .handle
        .high_tx
        .send(notification("high"))
        .await
        .expect("high record should enqueue");

    let mut reader = BufReader::new(transport.app_stdin);
    let mut blocker_remainder = String::new();
    reader
        .read_line(&mut blocker_remainder)
        .await
        .expect("blocker remainder should be readable");
    assert_eq!(read_method(&mut reader).await, "high");
    assert_eq!(read_method(&mut reader).await, "normal");
}

#[tokio::test]
async fn normal_record_is_served_after_at_most_eight_continuous_high_records() {
    let mut transport = test_transport(1);
    transport
        .handle
        .normal_tx
        .send(notification("blocker"))
        .await
        .expect("blocker should enqueue");

    let mut first_byte = [0_u8; 1];
    transport
        .app_stdin
        .read_exact(&mut first_byte)
        .await
        .expect("writer should begin the blocker");

    transport
        .handle
        .normal_tx
        .send(notification("normal-marker"))
        .await
        .expect("normal marker should enqueue");
    for index in 0..16 {
        transport
            .handle
            .high_tx
            .send(notification(&format!("high-{index}")))
            .await
            .expect("high record should enqueue");
    }

    let mut reader = BufReader::new(transport.app_stdin);
    let mut blocker_remainder = String::new();
    reader
        .read_line(&mut blocker_remainder)
        .await
        .expect("blocker remainder should be readable");

    let mut observed_normal = false;
    for _ in 0..=8 {
        if read_method(&mut reader).await == "normal-marker" {
            observed_normal = true;
            break;
        }
    }
    assert!(
        observed_normal,
        "normal traffic must be served within the configured high-priority burst"
    );
}

struct FlushCountingWriter<W> {
    inner: W,
    flushes: Arc<AtomicUsize>,
}

impl<W: AsyncWrite + Unpin> AsyncWrite for FlushCountingWriter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(context, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match Pin::new(&mut self.inner).poll_flush(context) {
            Poll::Ready(Ok(())) => {
                self.flushes.fetch_add(1, Ordering::SeqCst);
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

#[tokio::test]
async fn writer_flushes_each_complete_record_without_waiting_for_sender_close() {
    let (transport_stdout, _app_stdout) = duplex(1024);
    let (transport_stdin, app_stdin) = duplex(1024);
    let (transport_stderr, _app_stderr) = duplex(1024);
    let flushes = Arc::new(AtomicUsize::new(0));
    let cancellation = CancellationToken::new();
    let mut handle = spawn_stream_transport(
        transport_stdout,
        FlushCountingWriter {
            inner: transport_stdin,
            flushes: Arc::clone(&flushes),
        },
        transport_stderr,
        cancellation.clone(),
    );

    handle
        .normal_tx
        .send(notification("initialized"))
        .await
        .expect("record should enqueue");
    let mut reader = BufReader::new(app_stdin);
    assert_eq!(read_method(&mut reader).await, "initialized");
    timeout(EVENT_TIMEOUT, async {
        while flushes.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("writer should flush the record promptly");

    cancellation.cancel();
    assert!(matches!(
        next_event(&mut handle.events).await,
        TransportEvent::Cancelled
    ));
}

#[tokio::test]
async fn closed_app_stdin_is_reported_as_a_redacted_write_error() {
    let mut transport = test_transport(1024);
    drop(transport.app_stdin);

    transport
        .handle
        .normal_tx
        .send(notification("initialized"))
        .await
        .expect("record should enqueue before the writer observes closure");

    let error = loop {
        match next_event(&mut transport.handle.events).await {
            TransportEvent::WriteError(error) => break error,
            TransportEvent::Cancelled => panic!("write error must be reported before cancellation"),
            _ => {}
        }
    };
    assert!(matches!(
        error.kind,
        io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset | io::ErrorKind::NotConnected
    ));
}

#[tokio::test]
async fn cancellation_stops_transport_and_closes_the_event_stream() {
    let mut transport = test_transport(1024);
    transport.cancellation.cancel();

    assert!(matches!(
        next_event(&mut transport.handle.events).await,
        TransportEvent::Cancelled
    ));
    assert!(
        timeout(EVENT_TIMEOUT, transport.handle.events.recv())
            .await
            .expect("event stream should close after cancellation")
            .is_none(),
        "transport must release all event senders after cancellation"
    );
}

#[tokio::test]
async fn stdout_eof_is_reported_without_becoming_a_protocol_error() {
    let mut transport = test_transport(1024);
    drop(transport.app_stdout);

    assert!(matches!(
        next_event(&mut transport.handle.events).await,
        TransportEvent::StdoutEof
    ));
}

#[tokio::test]
async fn unterminated_final_record_is_delivered_before_stdout_eof() {
    let mut transport = test_transport(1024);
    transport
        .app_stdout
        .write_all(b"{\"method\":\"thread/started\"}")
        .await
        .expect("unterminated final record should be writable");
    drop(transport.app_stdout);

    assert!(matches!(
        next_event(&mut transport.handle.events).await,
        TransportEvent::Message(message)
            if matches!(message.message(), InboundMessage::Notification { method, .. }
                if method == "thread/started")
    ));
    assert!(matches!(
        next_event(&mut transport.handle.events).await,
        TransportEvent::StdoutEof
    ));
}

#[tokio::test]
async fn shutdown_waits_until_the_writer_has_dropped_stdin() {
    let mut transport = test_transport(1);
    let mut app_stdin = transport.app_stdin;
    let eof = tokio::spawn(async move {
        let mut bytes = Vec::new();
        app_stdin
            .read_to_end(&mut bytes)
            .await
            .expect("app-side stdin should reach EOF");
    });

    assert_eq!(
        transport.handle.shutdown().await,
        lark_codex_bridge::codex::transport::TransportExit::Cancelled
    );
    timeout(EVENT_TIMEOUT, eof)
        .await
        .expect("shutdown must close stdin before returning")
        .expect("EOF observer should not panic");
}

#[tokio::test]
async fn stderr_json_is_redacted_metadata_and_never_a_protocol_message() {
    let mut transport = test_transport(1024);
    let stderr_line =
        b"{\"method\":\"turn/completed\",\"params\":{\"secret\":\"do-not-expose\"}}\n";
    transport
        .app_stderr
        .write_all(stderr_line)
        .await
        .expect("stderr should be writable");

    let event = next_event(&mut transport.handle.events).await;
    assert!(matches!(
        event,
        TransportEvent::StderrLine { byte_len }
            if byte_len == stderr_line.len() - 1
    ));

    transport.cancellation.cancel();
}

#[tokio::test]
async fn oversized_unterminated_stderr_is_drained_without_blocking_stdout() {
    let mut transport = test_transport(1024);
    let mut app_stderr = transport.app_stderr;
    let stderr_writer = tokio::spawn(async move {
        app_stderr
            .write_all(&vec![b'x'; MAX_STDERR_LINE_BYTES * 4])
            .await
            .expect("stderr drain must keep accepting oversized data");
    });
    transport
        .app_stdout
        .write_all(b"{\"method\":\"turn/started\"}\n")
        .await
        .expect("stdout should remain writable");

    assert!(matches!(
        next_event(&mut transport.handle.events).await,
        TransportEvent::Message(message)
            if matches!(message.message(), InboundMessage::Notification { method, .. }
                if method == "turn/started")
    ));
    timeout(EVENT_TIMEOUT, stderr_writer)
        .await
        .expect("stderr must not fill its pipe")
        .expect("stderr writer should not panic");
}

#[tokio::test]
async fn full_event_queue_and_broken_stdin_do_not_deadlock_terminal_delivery() {
    let mut transport = test_transport(1024);
    for index in 0..EVENT_CAPACITY {
        transport
            .app_stdout
            .write_all(format!("{{\"method\":\"event/{index}\"}}\n").as_bytes())
            .await
            .expect("fixture notification should be writable");
    }
    tokio::task::yield_now().await;

    drop(transport.app_stdin);
    transport
        .handle
        .high_tx
        .send(notification("interrupt"))
        .await
        .expect("message should enter the bounded writer queue");

    let terminal = timeout(EVENT_TIMEOUT, async {
        loop {
            if matches!(
                transport.handle.events.recv().await,
                Some(TransportEvent::WriteError(_))
            ) {
                break;
            }
        }
    })
    .await;
    terminal.expect("terminal delivery must not deadlock behind a full event queue");
}

#[cfg(unix)]
fn fake_codex(version_output: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("temporary directory should be created");
    let binary = directory.path().join("codex binary with spaces");
    let script = format!(
        "#!/bin/sh\n[ \"$1\" = \"--version\" ] || exit 91\nprintf '%s\\n' '{version_output}'\n"
    );
    std::fs::write(&binary, script).expect("fake Codex binary should be written");
    let mut permissions = std::fs::metadata(&binary)
        .expect("fake Codex metadata should be readable")
        .permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&binary, permissions).expect("fake Codex binary should be executable");
    (directory, binary)
}

#[cfg(unix)]
fn fake_app_server() -> (tempfile::TempDir, std::path::PathBuf) {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("temporary directory should be created");
    let binary = directory.path().join("codex app server fixture");
    let script = "#!/bin/sh\n\
        if [ \"$1\" = \"--version\" ]; then printf '%s\\n' 'codex-cli 0.146.0'; exit 0; fi\n\
        [ \"$1\" = \"app-server\" ] || exit 91\n\
        [ \"$2\" = \"--listen\" ] || exit 92\n\
        [ \"$3\" = \"stdio://\" ] || exit 93\n\
        while IFS= read -r line; do :; done\n";
    std::fs::write(&binary, script).expect("fake app-server should be written");
    let mut permissions = std::fs::metadata(&binary)
        .expect("fake app-server metadata should be readable")
        .permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&binary, permissions).expect("fake app-server should be executable");
    (directory, binary)
}

#[cfg(unix)]
#[tokio::test]
async fn version_probe_executes_the_binary_directly_with_version_argument() {
    let (_directory, binary) = fake_codex("codex-cli 0.146.0");
    let version = probe_version(&CodexProcessConfig {
        binary,
        codex_home: None,
    })
    .await
    .expect("supported version should be accepted");

    assert_eq!(version, semver::Version::new(0, 146, 0));
}

#[cfg(unix)]
#[tokio::test]
async fn version_probe_rejects_malformed_and_unsupported_versions() {
    for output in ["codex 0.146.0", "codex-cli 0.145.9", "codex-cli 0.147.0"] {
        let (_directory, binary) = fake_codex(output);
        let result = probe_version(&CodexProcessConfig {
            binary,
            codex_home: None,
        })
        .await;
        assert!(result.is_err(), "probe must reject {output:?}");
    }
}

#[cfg(unix)]
#[tokio::test]
async fn app_server_stdio_is_transferred_once_and_shutdown_by_eof() {
    use lark_codex_bridge::codex::{process::spawn_app_server, transport::TransportExit};

    let (_directory, binary) = fake_app_server();
    let mut process = spawn_app_server(&CodexProcessConfig {
        binary,
        codex_home: None,
    })
    .await
    .expect("fake app-server should start");
    assert_ne!(process.id(), 0);
    let (stdout, stdin, stderr) = process
        .take_stdio()
        .expect("stdio should transfer exactly once");
    assert!(process.take_stdio().is_err());

    let cancellation = CancellationToken::new();
    let mut transport = spawn_stream_transport(stdout, stdin, stderr, cancellation);
    assert_eq!(transport.shutdown().await, TransportExit::Cancelled);
    let exit = timeout(EVENT_TIMEOUT, process.wait())
        .await
        .expect("stdin EOF should stop app-server promptly")
        .expect("app-server wait should succeed");
    assert!(exit.success);
    assert_eq!(exit.pid, process.id());
}

#[cfg(unix)]
#[tokio::test]
async fn invalid_codex_home_is_rejected_without_leaking_its_path_in_debug() {
    let (_directory, binary) = fake_codex("codex-cli 0.146.0");
    let secret_path = std::path::PathBuf::from("/secret/nonexistent/codex-home-sentinel");
    let config = CodexProcessConfig {
        binary,
        codex_home: Some(secret_path.clone()),
    };
    assert!(!format!("{config:?}").contains(secret_path.to_string_lossy().as_ref()));
    let error = probe_version(&config)
        .await
        .expect_err("invalid Codex home must be rejected before spawn");
    assert!(!format!("{error:?}").contains(secret_path.to_string_lossy().as_ref()));
}
