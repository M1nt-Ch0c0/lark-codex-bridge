//! Fake-sidecar contract tests. These exercise process supervision and the
//! real bounded NDJSON implementation without contacting Feishu/Lark.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures_util::FutureExt;
use secrecy::SecretString;
use tokio::sync::Notify;

use lark_codex_bridge::channel::ConnectionState;
use lark_codex_bridge::channel::sidecar::{NodeSidecar, NodeSidecarConfig};
use lark_codex_bridge::codex::supervisor::AppServerSupervisor;
use lark_codex_bridge::lark::bridge::InboundEventHandler;
use lark_codex_bridge::lark::config::TenantBrand;
use lark_codex_bridge::lark::credentials::LarkCredentials;
use lark_codex_bridge::lark::error::{LarkError, LarkErrorKind};

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("fake_channel_sidecar.cjs")
}

fn credentials() -> LarkCredentials {
    LarkCredentials::new(
        "cli_0123456789abcdef".to_owned(),
        SecretString::from("fake-secret-never-log"),
        TenantBrand::Feishu,
    )
}

fn config(mode: &str, marker: &Path) -> NodeSidecarConfig {
    NodeSidecarConfig {
        node_binary: PathBuf::from("node"),
        entrypoint: fixture(),
        arguments: vec![mode.to_owned(), marker.to_string_lossy().into_owned()],
        event_capacity: 1,
        write_capacity: 16,
        handshake_timeout: Duration::from_secs(2),
        handler_timeout: Duration::from_secs(2),
        shutdown_grace: Duration::from_secs(2),
        ..NodeSidecarConfig::default()
    }
}

fn fast_config(mode: &str, marker: &Path) -> NodeSidecarConfig {
    NodeSidecarConfig {
        handshake_timeout: Duration::from_millis(500),
        initial_connect_timeout: Duration::from_millis(750),
        healthy_uptime: Duration::from_secs(2),
        handler_timeout: Duration::from_secs(1),
        shutdown_grace: Duration::from_millis(250),
        ..config(mode, marker)
    }
}

async fn wait_for_file(path: &Path) {
    tokio::time::timeout(Duration::from_secs(8), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("fake sidecar marker timeout: {}", path.display()));
}

async fn assert_heartbeat_stops(marker: &Path) {
    let heartbeat = PathBuf::from(format!("{}.heartbeat-1", marker.display()));
    wait_for_file(&heartbeat).await;
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let before = std::fs::metadata(&heartbeat)
                .expect("heartbeat metadata")
                .len();
            tokio::time::sleep(Duration::from_millis(120)).await;
            let after = std::fs::metadata(&heartbeat)
                .expect("heartbeat metadata")
                .len();
            if before == after {
                tokio::time::sleep(Duration::from_millis(120)).await;
                let confirmed = std::fs::metadata(&heartbeat)
                    .expect("heartbeat metadata")
                    .len();
                if confirmed == after {
                    return;
                }
            }
        }
    })
    .await
    .expect("descendant heartbeat kept advancing after process-tree termination");
}

async fn wait_connected(state: &mut tokio::sync::watch::Receiver<ConnectionState>) {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            if matches!(*state.borrow(), ConnectionState::Connected) {
                return;
            }
            state.changed().await.expect("sidecar state channel");
        }
    })
    .await
    .expect("sidecar did not connect");
}

#[tokio::test]
async fn fake_sidecar_covers_durable_ack_backpressure_restart_and_shutdown() {
    let temp = tempfile::tempdir().expect("tempdir");
    let marker = temp.path().join("lifecycle.json");
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let durable = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(AtomicUsize::new(0));
    let handler: InboundEventHandler = Arc::new({
        let entered = Arc::clone(&entered);
        let release = Arc::clone(&release);
        let durable = Arc::clone(&durable);
        let calls = Arc::clone(&calls);
        move |_payload| {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            let durable = Arc::clone(&durable);
            let ordinal = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if ordinal == 0 {
                    entered.notify_one();
                    release.notified().await;
                    durable.store(true, Ordering::SeqCst);
                    Ok(None)
                } else {
                    Err(LarkError::retryable("fake durable intake failure"))
                }
            }
            .boxed()
        }
    });

    let handle = NodeSidecar::start(config("lifecycle", &marker), credentials(), handler)
        .await
        .expect("sidecar startup handshake");
    let mut state = handle.subscribe_state();
    wait_connected(&mut state).await;
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .expect("first event reached Rust");

    // The fake can observe backpressure while the first handler is blocked,
    // but cannot write its completion marker until Rust returns a positive
    // ack. This is the upstream durable-ack ordering assertion.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!marker.exists());
    assert!(!durable.load(Ordering::SeqCst));
    release.notify_one();

    wait_for_file(&marker).await;
    assert!(durable.load(Ordering::SeqCst));
    let evidence = std::fs::read_to_string(&marker).expect("lifecycle evidence");
    assert!(evidence.contains("\"positive\":true"));
    assert!(evidence.contains("\"backpressure\":true"));
    assert!(evidence.contains("\"durableFailure\":true"));
    assert!(evidence.contains("\"unknown\":true"));

    // The first fake exits with 42. Supervision must start a second process,
    // complete a fresh handshake, and publish connected again.
    wait_for_file(&PathBuf::from(format!("{}.second", marker.display()))).await;
    wait_connected(&mut state).await;

    handle.shutdown().await;
    wait_for_file(&PathBuf::from(format!("{}.shutdown", marker.display()))).await;
}

#[tokio::test]
async fn durable_handler_timeout_is_a_negative_ack() {
    let temp = tempfile::tempdir().expect("tempdir");
    let marker = temp.path().join("handler-timeout");
    let handler: InboundEventHandler = Arc::new(|_payload| {
        async {
            std::future::pending::<()>().await;
            Ok(None)
        }
        .boxed()
    });
    let mut sidecar_config = config("handler-timeout", &marker);
    sidecar_config.handler_timeout = Duration::from_millis(75);

    let handle = NodeSidecar::start(sidecar_config, credentials(), handler)
        .await
        .expect("sidecar startup handshake");
    wait_for_file(&marker).await;
    assert_eq!(
        std::fs::read_to_string(&marker).expect("timeout evidence"),
        "durable_intake_timeout"
    );
    handle.shutdown().await;
}

#[tokio::test]
async fn incompatible_version_and_configuration_timeout_fail_startup() {
    let temp = tempfile::tempdir().expect("tempdir");
    let marker = temp.path().join("unused");
    let handler: InboundEventHandler = Arc::new(|_payload| async { Ok(None) }.boxed());

    let version_error = NodeSidecar::start(
        config("bad-version", &marker),
        credentials(),
        Arc::clone(&handler),
    )
    .await
    .expect_err("wire version must be rejected");
    assert_eq!(version_error.kind(), LarkErrorKind::ProtocolViolation);
    assert!(!format!("{version_error:?}").contains("fake-secret-never-log"));

    let frame_error = NodeSidecar::start(
        config("oversize-hello", &marker),
        credentials(),
        Arc::clone(&handler),
    )
    .await
    .expect_err("oversized hello must be rejected before JSON parsing");
    assert_eq!(frame_error.kind(), LarkErrorKind::ProtocolViolation);

    let mut timeout_config = config("silence", &marker);
    timeout_config.handshake_timeout = Duration::from_millis(150);
    let timeout_error = NodeSidecar::start(timeout_config, credentials(), Arc::clone(&handler))
        .await
        .expect_err("configuration response must be bounded");
    assert_eq!(timeout_error.kind(), LarkErrorKind::Retryable);
    assert!(!format!("{timeout_error:?}").contains("fake-secret-never-log"));

    let mut invalid_bounds = config("lifecycle", &marker);
    invalid_bounds.event_capacity = 65;
    let bound_error = NodeSidecar::start(invalid_bounds, credentials(), Arc::clone(&handler))
        .await
        .expect_err("queue capacity cannot exceed the hard bound");
    assert_eq!(bound_error.kind(), LarkErrorKind::ProtocolViolation);

    let mut invalid_timeout = config("lifecycle", &marker);
    invalid_timeout.handler_timeout = Duration::from_secs(61);
    let timeout_bound_error =
        NodeSidecar::start(invalid_timeout, credentials(), Arc::clone(&handler))
            .await
            .expect_err("time bounds cannot exceed the production maxima");
    assert_eq!(timeout_bound_error.kind(), LarkErrorKind::ProtocolViolation);
}

#[tokio::test]
async fn initial_connection_failure_is_terminal_for_this_start_and_kills_the_tree() {
    let temp = tempfile::tempdir().expect("tempdir");
    let marker = temp.path().join("configure-failed");
    let handler: InboundEventHandler = Arc::new(|_payload| async { Ok(None) }.boxed());

    let error = NodeSidecar::start(
        fast_config("configure-failed", &marker),
        credentials(),
        handler,
    )
    .await
    .expect_err("configure acceptance is not provider connection readiness");

    assert_eq!(error.kind(), LarkErrorKind::Retryable);
    assert_heartbeat_stops(&marker).await;
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert_eq!(
        std::fs::read_to_string(PathBuf::from(format!("{}.runs", marker.display())))
            .expect("bootstrap run count"),
        "1",
        "an initial SDK connection failure must return to assembly for its configured native fallback, not restart forever",
    );
}

#[tokio::test]
async fn startup_protocol_and_timeout_paths_kill_non_exec_descendants() {
    let handler: InboundEventHandler = Arc::new(|_payload| async { Ok(None) }.boxed());

    for (mode, expected_kind) in [
        ("startup-descendant", LarkErrorKind::ProtocolViolation),
        ("timeout-descendant", LarkErrorKind::Retryable),
    ] {
        let temp = tempfile::tempdir().expect("tempdir");
        let marker = temp.path().join(mode);
        let mut sidecar_config = fast_config(mode, &marker);
        if mode == "timeout-descendant" {
            sidecar_config.handshake_timeout = Duration::from_millis(150);
        }
        let error = NodeSidecar::start(sidecar_config, credentials(), Arc::clone(&handler))
            .await
            .expect_err("bootstrap failure must be returned");
        assert_eq!(error.kind(), expected_kind);
        assert_heartbeat_stops(&marker).await;
    }
}

#[tokio::test]
async fn protocol_crash_and_stdout_eof_restart_after_killing_non_exec_descendants() {
    let handler: InboundEventHandler = Arc::new(|_payload| async { Ok(None) }.boxed());

    for mode in ["protocol-descendant", "crash-descendant", "eof-descendant"] {
        let temp = tempfile::tempdir().expect("tempdir");
        let marker = temp.path().join(mode);
        let handle = NodeSidecar::start(
            fast_config(mode, &marker),
            credentials(),
            Arc::clone(&handler),
        )
        .await
        .expect("first sidecar connection");

        wait_for_file(&PathBuf::from(format!("{}.second", marker.display()))).await;
        assert_heartbeat_stops(&marker).await;
        handle.shutdown().await;
    }
}

#[tokio::test]
async fn graceful_timeout_and_handle_drop_kill_non_exec_descendants() {
    let handler: InboundEventHandler = Arc::new(|_payload| async { Ok(None) }.boxed());

    let shutdown_temp = tempfile::tempdir().expect("tempdir");
    let shutdown_marker = shutdown_temp.path().join("shutdown-descendant");
    let shutdown_handle = NodeSidecar::start(
        fast_config("shutdown-descendant", &shutdown_marker),
        credentials(),
        Arc::clone(&handler),
    )
    .await
    .expect("shutdown sidecar connection");
    wait_for_file(&PathBuf::from(format!(
        "{}.heartbeat-1",
        shutdown_marker.display()
    )))
    .await;
    tokio::time::timeout(Duration::from_secs(2), shutdown_handle.shutdown())
        .await
        .expect("shutdown must remain bounded");
    wait_for_file(&PathBuf::from(format!(
        "{}.shutdown-requested",
        shutdown_marker.display()
    )))
    .await;
    assert_heartbeat_stops(&shutdown_marker).await;

    let drop_temp = tempfile::tempdir().expect("tempdir");
    let drop_marker = drop_temp.path().join("drop-descendant");
    let drop_handle = NodeSidecar::start(
        fast_config("drop-descendant", &drop_marker),
        credentials(),
        handler,
    )
    .await
    .expect("drop sidecar connection");
    wait_for_file(&PathBuf::from(format!(
        "{}.heartbeat-1",
        drop_marker.display()
    )))
    .await;
    drop(drop_handle);
    assert_heartbeat_stops(&drop_marker).await;
}

#[tokio::test]
async fn oversized_unterminated_stderr_is_discarded_while_protocol_work_continues() {
    let temp = tempfile::tempdir().expect("tempdir");
    let marker = temp.path().join("stderr-oversize");
    let handler: InboundEventHandler = Arc::new(|_payload| async { Ok(None) }.boxed());

    let handle = NodeSidecar::start(
        fast_config("stderr-oversize", &marker),
        credentials(),
        handler,
    )
    .await
    .expect("sidecar connection");
    wait_for_file(&marker).await;
    assert_eq!(
        std::fs::read_to_string(&marker).expect("stderr evidence"),
        "acked-after-oversized-stderr",
    );
    handle.shutdown().await;
}

#[tokio::test]
async fn connected_immediate_crashes_escalate_restart_backoff() {
    let temp = tempfile::tempdir().expect("tempdir");
    let marker = temp.path().join("connect-crash");
    let handler: InboundEventHandler = Arc::new(|_payload| async { Ok(None) }.boxed());
    let handle = NodeSidecar::start(
        fast_config("connect-crash", &marker),
        credentials(),
        handler,
    )
    .await
    .expect("initial sidecar connection");
    let mut state = handle.subscribe_state();
    let mut observed = Vec::new();

    tokio::time::timeout(Duration::from_secs(8), async {
        while observed.len() < 3 {
            if let ConnectionState::Backoff { attempt, delay } = *state.borrow() {
                if observed.last().is_none_or(|(seen, _)| *seen != attempt) {
                    observed.push((attempt, delay));
                }
            }
            state.changed().await.expect("sidecar state channel");
        }
    })
    .await
    .expect("three escalating restart observations");

    assert_eq!(
        observed,
        (1..=3)
            .map(|attempt| (attempt, AppServerSupervisor::retry_delay(0, attempt)))
            .collect::<Vec<_>>(),
    );
    assert!(observed.windows(2).all(|window| window[0].1 < window[1].1));
    assert!(
        (1..=64)
            .all(|attempt| AppServerSupervisor::retry_delay(0, attempt) <= Duration::from_secs(30)),
        "restart backoff must remain capped",
    );
    handle.shutdown().await;
}

#[tokio::test]
async fn duplicate_active_ids_fault_once_and_ids_clear_across_process_epochs() {
    let temp = tempfile::tempdir().expect("tempdir");
    let marker = temp.path().join("duplicate-active");
    let calls = Arc::new(AtomicUsize::new(0));
    let handler: InboundEventHandler = Arc::new({
        let calls = Arc::clone(&calls);
        move |_payload: Bytes| {
            let ordinal = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if ordinal == 0 {
                    std::future::pending::<()>().await;
                }
                Ok(None)
            }
            .boxed()
        }
    });
    let mut sidecar_config = fast_config("duplicate-active", &marker);
    sidecar_config.event_capacity = 2;

    let handle = NodeSidecar::start(sidecar_config, credentials(), handler)
        .await
        .expect("initial sidecar connection");
    wait_for_file(&PathBuf::from(format!("{}.second", marker.display()))).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "only the first active event and the post-restart reuse may invoke intake",
    );
    handle.shutdown().await;
}

#[tokio::test]
async fn distinct_event_ids_correlate_when_durable_completions_reverse_order() {
    let temp = tempfile::tempdir().expect("tempdir");
    let marker = temp.path().join("reverse-acks");
    let calls = Arc::new(AtomicUsize::new(0));
    let handler: InboundEventHandler = Arc::new({
        let calls = Arc::clone(&calls);
        move |payload: Bytes| {
            calls.fetch_add(1, Ordering::SeqCst);
            async move {
                let value: serde_json::Value =
                    serde_json::from_slice(&payload).expect("fake event payload");
                if value["ordinal"] == "slow" {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                } else {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Ok(None)
            }
            .boxed()
        }
    });
    let mut sidecar_config = fast_config("reverse-acks", &marker);
    sidecar_config.event_capacity = 2;

    let handle = NodeSidecar::start(sidecar_config, credentials(), handler)
        .await
        .expect("sidecar connection");
    wait_for_file(&marker).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        std::fs::read_to_string(&marker).expect("ack order"),
        r#"["event-fast","event-slow"]"#,
    );
    handle.shutdown().await;
}
