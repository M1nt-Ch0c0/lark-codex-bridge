use std::{
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

use lark_codex_bridge::codex::{
    process::ProcessError,
    sidecar::{
        CODEX_SIDECAR_PROTOCOL, CODEX_SIDECAR_VERSION, CodexSidecarConfig,
        REQUIRED_SIDECAR_CAPABILITIES, spawn_codex_sidecar,
    },
    supervisor::{AppServerSupervisor, ProtocolInfo, SupervisorHandle, SupervisorState},
    types::{ThreadListParams, ThreadStartParams},
};
use serde_json::Value;
use tempfile::TempDir;
use tokio::time::{sleep, timeout};

const TEST_TIMEOUT: Duration = Duration::from_secs(15);
const POLL_INTERVAL: Duration = Duration::from_millis(20);

fn repository_file(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn node_binary() -> Option<PathBuf> {
    let candidate =
        std::env::var_os("CODEX_SIDECAR_NODE").map_or_else(|| PathBuf::from("node"), PathBuf::from);
    let available = Command::new(&candidate)
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success());
    if available {
        return Some(candidate);
    }
    assert_ne!(
        std::env::var_os("CODEX_SIDECAR_TEST_REQUIRED").as_deref(),
        Some(std::ffi::OsStr::new("1")),
        "the dedicated sidecar test job requires a working Node.js binary"
    );
    eprintln!("skipping Codex sidecar integration test because Node.js is unavailable");
    None
}

fn upstream_config(node: &Path, version: &str, mode: &str, marker: &Path) -> CodexSidecarConfig {
    CodexSidecarConfig {
        node_binary: node.to_path_buf(),
        entrypoint: repository_file("codex-sidecar/index.cjs"),
        codex_binary: Some(node.to_path_buf()),
        codex_arguments: vec![
            repository_file("tests/fixtures/fake_codex_sidecar_upstream.cjs")
                .to_string_lossy()
                .into_owned(),
            format!("--fake-version={version}"),
            format!("--fake-mode={mode}"),
            format!("--fake-marker={}", marker.to_string_lossy()),
        ],
        ..CodexSidecarConfig::default()
    }
}

fn bootstrap_config(node: &Path, fixture: &str) -> CodexSidecarConfig {
    CodexSidecarConfig {
        node_binary: node.to_path_buf(),
        entrypoint: repository_file(fixture),
        codex_binary: Some(node.to_path_buf()),
        ..CodexSidecarConfig::default()
    }
}

fn marker_events(marker: &Path) -> Vec<Value> {
    let Ok(contents) = std::fs::read_to_string(marker) else {
        return Vec::new();
    };
    contents
        .split_inclusive('\n')
        .filter(|line| line.ends_with('\n'))
        .map(|line| serde_json::from_str(line.trim_end()).expect("marker line must be valid JSON"))
        .collect()
}

fn event_count(events: &[Value], event: &str, method: Option<&str>) -> usize {
    events
        .iter()
        .filter(|value| {
            value["event"] == event && method.is_none_or(|expected| value["method"] == expected)
        })
        .count()
}

fn descendant_endpoint_is_alive(port: u16) -> bool {
    TcpStream::connect_timeout(
        &SocketAddr::from(([127, 0, 0, 1], port)),
        Duration::from_millis(250),
    )
    .is_ok()
}

async fn wait_for_event(marker: &Path, event: &str) {
    timeout(TEST_TIMEOUT, async {
        loop {
            if event_count(&marker_events(marker), event, None) > 0 {
                return;
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect("fake upstream should record the lifecycle event");
}

async fn wait_for_ready_epoch(
    handle: &mut SupervisorHandle,
    minimum_epoch: u64,
) -> SupervisorState {
    timeout(TEST_TIMEOUT, async {
        loop {
            let state = handle.state();
            if matches!(state, SupervisorState::Ready { epoch, .. } if epoch >= minimum_epoch) {
                return state;
            }
            handle
                .changed()
                .await
                .expect("sidecar supervisor should keep running");
        }
    })
    .await
    .expect("sidecar supervisor should reach a replacement ready epoch")
}

async fn assert_supported_version(version: &str) {
    let Some(node) = node_binary() else {
        return;
    };
    let temp = TempDir::new().expect("temporary marker directory");
    let marker = temp.path().join("events.jsonl");
    let handle =
        AppServerSupervisor::start_sidecar(upstream_config(&node, version, "normal", &marker))
            .await
            .expect("sidecar supervisor starts");

    let SupervisorState::Ready {
        epoch,
        version: ready_version,
        peer,
        protocol,
    } = handle.state()
    else {
        panic!("supported sidecar version must become ready");
    };
    assert_eq!(epoch, 1);
    assert_eq!(ready_version.to_string(), version);
    assert_eq!(peer.user_agent, format!("fake-codex/{version}"));
    assert_eq!(peer.platform_family, "test");
    match protocol {
        ProtocolInfo::SidecarV1 {
            protocol,
            version,
            capabilities,
        } => {
            assert_eq!(protocol, CODEX_SIDECAR_PROTOCOL);
            assert_eq!(version, CODEX_SIDECAR_VERSION);
            assert_eq!(capabilities, REQUIRED_SIDECAR_CAPABILITIES);
        }
        ProtocolInfo::NativeStdio => panic!("sidecar epoch must report the sidecar protocol"),
    }

    let threads = handle
        .client()
        .expect("ready sidecar client")
        .list_threads(ThreadListParams::default())
        .await
        .expect("stable thread/list RPC succeeds");
    assert!(threads.data.is_empty());
    assert!(threads.next_cursor.is_none());

    let before_shutdown = marker_events(&marker);
    assert_eq!(
        event_count(&before_shutdown, "request", Some("initialize")),
        1
    );
    assert_eq!(
        event_count(&before_shutdown, "request", Some("thread/list")),
        1
    );
    assert_eq!(event_count(&before_shutdown, "initialized", None), 1);

    handle.shutdown().await.expect("clean sidecar shutdown");
    wait_for_event(&marker, "stdin-eof").await;
}

#[tokio::test]
async fn sidecar_0_149_reaches_ready_and_serves_stable_rpc() {
    assert_supported_version("0.149.0").await;
}

#[tokio::test]
async fn sidecar_0_151_reaches_ready_and_serves_stable_rpc() {
    assert_supported_version("0.151.0").await;
}

#[tokio::test]
async fn unreviewed_upstream_version_is_rejected_before_ready() {
    let Some(node) = node_binary() else {
        return;
    };
    let temp = TempDir::new().expect("temporary marker directory");
    let marker = temp.path().join("events.jsonl");
    let handle =
        AppServerSupervisor::start_sidecar(upstream_config(&node, "0.150.0", "normal", &marker))
            .await
            .expect("supervisor exposes the permanent negotiation failure");

    let SupervisorState::Degraded { reason } = handle.state() else {
        panic!("unreviewed upstream version must fail closed");
    };
    assert_eq!(
        reason,
        "the configured Codex version has no reviewed sidecar adapter"
    );
    handle
        .shutdown()
        .await
        .expect("degraded supervisor shutdown");
}

#[tokio::test]
async fn malformed_or_missing_sidecar_hello_fails_closed() {
    let Some(node) = node_binary() else {
        return;
    };
    for (fixture, expected_io_failure) in [
        ("tests/fixtures/fake_sidecar_bad_hello.cjs", false),
        ("tests/fixtures/fake_sidecar_eof.cjs", true),
    ] {
        let result = spawn_codex_sidecar(&bootstrap_config(&node, fixture)).await;
        match result {
            Err(ProcessError::SidecarBootstrapIo) if expected_io_failure => {}
            Err(ProcessError::SidecarProtocol) if !expected_io_failure => {}
            Err(error) => panic!("bootstrap fixture returned an unexpected error: {error}"),
            Ok(_) => panic!("bootstrap fixture must not negotiate successfully"),
        }
    }
}

#[tokio::test]
async fn replacement_epoch_does_not_replay_an_uncertain_mutation() {
    let Some(node) = node_binary() else {
        return;
    };
    let temp = TempDir::new().expect("temporary marker directory");
    let marker = temp.path().join("events.jsonl");
    let mut handle = AppServerSupervisor::start_sidecar(upstream_config(
        &node,
        "0.151.0",
        "crash-once-mutation",
        &marker,
    ))
    .await
    .expect("sidecar supervisor starts");
    assert!(matches!(
        handle.state(),
        SupervisorState::Ready { epoch: 1, .. }
    ));

    let client = handle.client().expect("first epoch client");
    let mutation = timeout(
        TEST_TIMEOUT,
        client.start_thread(ThreadStartParams::default()),
    )
    .await
    .expect("uncertain mutation should resolve when its epoch exits");
    assert!(
        mutation.is_err(),
        "a response lost with the epoch cannot succeed"
    );

    let ready = wait_for_ready_epoch(&mut handle, 2).await;
    assert!(matches!(ready, SupervisorState::Ready { epoch: 2, .. }));
    let events = marker_events(&marker);
    assert_eq!(event_count(&events, "crash-before-response", None), 1);
    assert_eq!(
        event_count(&events, "request", Some("thread/start")),
        1,
        "a non-idempotent request with an uncertain outcome must never cross an epoch boundary"
    );
    assert_eq!(event_count(&events, "request", Some("initialize")), 2);

    handle.shutdown().await.expect("replacement epoch shutdown");
}

#[tokio::test]
async fn crash_cleans_the_owned_descendant_before_replacement_epoch() {
    let Some(node) = node_binary() else {
        return;
    };
    let temp = TempDir::new().expect("temporary marker directory");
    let marker = temp.path().join("events.jsonl");
    let mut handle = AppServerSupervisor::start_sidecar(upstream_config(
        &node,
        "0.151.0",
        "crash-once-with-descendant",
        &marker,
    ))
    .await
    .expect("sidecar supervisor starts");
    assert!(matches!(
        handle.state(),
        SupervisorState::Ready { epoch: 1, .. }
    ));

    wait_for_event(&marker, "descendant").await;
    let descendant = marker_events(&marker)
        .into_iter()
        .find(|event| event["event"] == "descendant")
        .expect("fake upstream records its long-lived descendant");
    let descendant_pid = descendant["pid"]
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
        .expect("descendant PID fits u32");
    let descendant_token = descendant["token"]
        .as_str()
        .expect("descendant marker includes a unique token")
        .to_owned();

    #[cfg(unix)]
    let _cleanup = DescendantCleanup {
        pid: descendant_pid,
        token: descendant_token.clone(),
    };
    #[cfg(windows)]
    let _cleanup = WindowsDescendantCleanup {
        node: node.clone(),
        pid: descendant_pid,
    };

    let descendant_port = descendant["port"]
        .as_u64()
        .and_then(|port| u16::try_from(port).ok())
        .expect("descendant marker includes its loopback port");
    assert!(descendant_token.starts_with("bridge-sidecar-descendant:"));
    assert!(
        descendant_endpoint_is_alive(descendant_port),
        "descendant must be alive before the first epoch crashes"
    );

    let stale = handle.client().expect("first epoch client");
    let mutation = timeout(
        TEST_TIMEOUT,
        stale.start_thread(ThreadStartParams::default()),
    )
    .await
    .expect("crashing mutation resolves when the first epoch exits");
    assert!(mutation.is_err(), "the crashed mutation cannot succeed");

    let ready = wait_for_ready_epoch(&mut handle, 2).await;
    assert!(matches!(ready, SupervisorState::Ready { epoch: 2, .. }));
    let events = marker_events(&marker);
    assert_eq!(event_count(&events, "start", None), 2);
    assert_eq!(event_count(&events, "descendant", None), 1);
    assert_eq!(event_count(&events, "crash-before-response", None), 1);
    assert_eq!(
        event_count(&events, "request", Some("thread/start")),
        1,
        "the uncertain mutation must not replay in the replacement epoch"
    );
    let check_index = events
        .iter()
        .position(|event| event["event"] == "replacement-descendant-check")
        .expect("replacement must record the pre-initialize descendant check");
    let replacement_initialize_index = events
        .iter()
        .rposition(|event| event["event"] == "request" && event["method"] == "initialize")
        .expect("replacement epoch performs initialize");
    assert!(
        check_index < replacement_initialize_index,
        "descendant cleanup must be checked before replacement initialization"
    );
    assert_eq!(events[check_index]["observed"], true);
    assert_eq!(
        events[check_index]["alive"], false,
        "the old descendant must be gone when the replacement process starts"
    );
    assert!(
        !descendant_endpoint_is_alive(descendant_port),
        "the old descendant endpoint must remain closed after replacement"
    );
    #[cfg(unix)]
    assert!(
        !unix_process_matches(descendant_pid, &descendant_token),
        "the old descendant process must be absent after replacement"
    );
    #[cfg(windows)]
    assert!(
        !windows_process_is_alive(&node, descendant_pid),
        "the old descendant process must be absent after replacement"
    );

    handle.shutdown().await.expect("replacement epoch shutdown");
}

#[cfg(unix)]
fn unix_process_matches(pid: u32, token: &str) -> bool {
    let output = Command::new("/bin/ps")
        .args([
            "-ww",
            "-p",
            &pid.to_string(),
            "-o",
            "stat=",
            "-o",
            "command=",
        ])
        .stderr(std::process::Stdio::null())
        .output();
    let Ok(output) = output else {
        return false;
    };
    if !output.status.success() {
        return false;
    }
    let process = String::from_utf8_lossy(&output.stdout);
    let mut fields = process.trim_start().splitn(2, char::is_whitespace);
    let state = fields.next().unwrap_or_default();
    let command = fields.next().unwrap_or_default().trim_start();
    !state.starts_with('Z') && command.contains(token)
}

#[cfg(unix)]
struct DescendantCleanup {
    pid: u32,
    token: String,
}

#[cfg(unix)]
impl Drop for DescendantCleanup {
    fn drop(&mut self) {
        if !unix_process_matches(self.pid, &self.token) {
            return;
        }
        let _ = Command::new("/bin/kill")
            .args(["-KILL", &self.pid.to_string()])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
}

#[cfg(windows)]
fn windows_process_is_alive(node: &Path, pid: u32) -> bool {
    Command::new(node)
        .args([
            "-e",
            "try { process.kill(Number(process.argv[1]), 0); } catch { process.exitCode = 1; }",
            &pid.to_string(),
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

#[cfg(windows)]
struct WindowsDescendantCleanup {
    node: PathBuf,
    pid: u32,
}

#[cfg(windows)]
impl Drop for WindowsDescendantCleanup {
    fn drop(&mut self) {
        if !windows_process_is_alive(&self.node, self.pid) {
            return;
        }
        let _ = Command::new(&self.node)
            .args([
                "-e",
                "try { process.kill(Number(process.argv[1]), 'SIGKILL'); } catch {}",
                &self.pid.to_string(),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
}

#[cfg(unix)]
#[tokio::test]
async fn shutdown_kills_a_descendant_left_outside_node_cleanup() {
    let Some(node) = node_binary() else {
        return;
    };
    let temp = TempDir::new().expect("temporary marker directory");
    let marker = temp.path().join("events.jsonl");
    let handle = AppServerSupervisor::start_sidecar(upstream_config(
        &node,
        "0.151.0",
        "leave-descendant",
        &marker,
    ))
    .await
    .expect("sidecar supervisor starts");
    assert!(matches!(handle.state(), SupervisorState::Ready { .. }));
    wait_for_event(&marker, "descendant").await;
    let descendant = marker_events(&marker)
        .into_iter()
        .find(|event| event["event"] == "descendant")
        .expect("fake upstream records its long-lived descendant PID");
    let descendant_pid = descendant["pid"]
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
        .expect("descendant PID fits u32");
    let descendant_token = descendant["token"]
        .as_str()
        .expect("descendant marker includes the unique argv token")
        .to_owned();
    let _cleanup = DescendantCleanup {
        pid: descendant_pid,
        token: descendant_token.clone(),
    };
    assert!(
        unix_process_matches(descendant_pid, &descendant_token),
        "descendant must be alive before the Rust owner shuts down"
    );

    handle.shutdown().await.expect("sidecar tree shutdown");
    timeout(TEST_TIMEOUT, async {
        while unix_process_matches(descendant_pid, &descendant_token) {
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect("Rust process-group ownership must terminate the left-behind descendant");
}

#[cfg(unix)]
#[tokio::test]
async fn failed_bootstrap_confirms_its_spawned_process_group_is_empty() {
    let Some(node) = node_binary() else {
        return;
    };
    let temp = TempDir::new().expect("temporary marker directory");
    let marker = temp.path().join("bootstrap-events.jsonl");
    let mut config = bootstrap_config(&node, "tests/fixtures/fake_sidecar_hanging_bootstrap.cjs");
    config.codex_binary = Some(marker.clone());
    config.handshake_timeout = Duration::from_millis(100);
    config.shutdown_grace = Duration::from_secs(2);

    let Err(error) = spawn_codex_sidecar(&config).await else {
        panic!("the fixture never completes its configure response");
    };
    assert!(matches!(error, ProcessError::SidecarHandshakeTimeout(_)));

    let descendant = marker_events(&marker)
        .into_iter()
        .find(|event| event["event"] == "descendant")
        .expect("bootstrap fixture records its long-lived descendant PID");
    let descendant_pid = descendant["pid"]
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
        .expect("descendant PID fits u32");
    let descendant_token = descendant["token"]
        .as_str()
        .expect("descendant marker includes the unique argv token")
        .to_owned();
    let _cleanup = DescendantCleanup {
        pid: descendant_pid,
        token: descendant_token.clone(),
    };
    assert!(
        !unix_process_matches(descendant_pid, &descendant_token),
        "returning the original bootstrap error requires an empty owned group"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn cancelling_bootstrap_kills_the_owned_process_group() {
    let Some(node) = node_binary() else {
        return;
    };
    let temp = TempDir::new().expect("temporary marker directory");
    let marker = temp.path().join("bootstrap-events.jsonl");
    let mut config = bootstrap_config(&node, "tests/fixtures/fake_sidecar_hanging_bootstrap.cjs");
    config.codex_binary = Some(marker.clone());

    let spawn = tokio::spawn(async move { AppServerSupervisor::start_sidecar(config).await });
    wait_for_event(&marker, "descendant").await;
    let descendant = marker_events(&marker)
        .into_iter()
        .find(|event| event["event"] == "descendant")
        .expect("bootstrap fixture records its long-lived descendant PID");
    let descendant_pid = descendant["pid"]
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
        .expect("descendant PID fits u32");
    let descendant_token = descendant["token"]
        .as_str()
        .expect("descendant marker includes the unique argv token")
        .to_owned();
    let _cleanup = DescendantCleanup {
        pid: descendant_pid,
        token: descendant_token.clone(),
    };
    assert!(unix_process_matches(descendant_pid, &descendant_token));

    spawn.abort();
    assert!(
        matches!(spawn.await, Err(error) if error.is_cancelled()),
        "the test must exercise future cancellation rather than handshake timeout"
    );
    timeout(TEST_TIMEOUT, async {
        while unix_process_matches(descendant_pid, &descendant_token) {
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect("bootstrap cancellation must terminate the owned process group");
}
