//! Fake-sidecar contract tests. These exercise process supervision and the
//! real bounded NDJSON implementation without contacting Feishu/Lark.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use futures_util::FutureExt;
use secrecy::SecretString;
use tokio::sync::Notify;

use lark_codex_bridge::channel::ConnectionState;
use lark_codex_bridge::channel::sidecar::{NodeSidecar, NodeSidecarConfig};
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

async fn wait_for_file(path: &Path) {
    tokio::time::timeout(Duration::from_secs(8), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("fake sidecar marker timeout");
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
