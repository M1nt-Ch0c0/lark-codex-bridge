//! Explicit exact-binary smoke for external resume/read reconciliation and socket-only recovery.
//!
//! Ordinary suites ignore this test. Its exact invocation is fail-closed: configuration, exact
//! version, authenticated startup, durable reconciliation, bridge-side reconnect, operator server
//! restart, post-shutdown health, or no-write-replay evidence must all succeed.

use std::{
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use futures_util::{SinkExt, StreamExt};
use lark_codex_bridge::{
    codex::{
        external::{
            ExternalAuthentication, ExternalCapabilityProfile, ExternalEndpointConfig,
            ExternalEndpointGate,
        },
        external_recovery::{
            ExternalRecoveryCoordinator, ExternalRecoverySettings, ExternalRecoveryState,
        },
        process::{CodexProcessConfig, probe_version},
    },
    store::{ExternalThreadState, StoreHandle},
};
use semver::Version;
use serde_json::{Value, json};
use tokio::{net::TcpStream, process::Command, time::timeout};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest, http::HeaderValue},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(20);
const READY_TIMEOUT: Duration = Duration::from_secs(30);
const CHILD_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

struct ChildGuard {
    child: tokio::process::Child,
}

impl ChildGuard {
    fn ensure_running(&mut self) -> Result<()> {
        ensure!(
            self.child
                .try_wait()
                .context("unable to inspect the smoke-owned app-server")?
                .is_none(),
            "external app-server exited during recovery smoke"
        );
        Ok(())
    }

    async fn stop(mut self) -> Result<()> {
        self.child
            .start_kill()
            .context("unable to stop the smoke-owned app-server")?;
        timeout(CHILD_SHUTDOWN_TIMEOUT, self.child.wait())
            .await
            .context("timed out reaping the smoke-owned app-server")??;
        Ok(())
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

#[tokio::test]
#[ignore = "requires explicit exact Codex binary gate; missing configuration is a failure"]
#[allow(clippy::too_many_lines)]
async fn real_exact_binary_reconciles_across_socket_and_operator_server_restarts_without_write_replay()
-> Result<()> {
    ensure!(
        required_env("CODEX_EXTERNAL_RECONCILIATION_E2E")? == "1",
        "CODEX_EXTERNAL_RECONCILIATION_E2E must equal 1"
    );
    let binary = PathBuf::from(required_env("CODEX_EXTERNAL_RECONCILIATION_BINARY")?);
    ensure!(
        binary.is_absolute(),
        "CODEX_EXTERNAL_RECONCILIATION_BINARY must be an absolute path"
    );
    let expected_version = required_env("CODEX_EXTERNAL_RECONCILIATION_EXPECTED_VERSION")?;
    let expected_version_parsed =
        Version::parse(&expected_version).context("expected version must be exact semver")?;
    ensure!(
        expected_version_parsed.pre.is_empty()
            && expected_version_parsed.build.is_empty()
            && expected_version_parsed.to_string() == expected_version,
        "expected version must be canonical exact semver"
    );
    let probed = probe_version(&CodexProcessConfig {
        binary: binary.clone(),
        codex_home: None,
    })
    .await
    .context("exact binary version probe failed")?;
    ensure!(
        probed == expected_version_parsed,
        "configured binary did not match the exact expected version"
    );

    let scratch = tempfile::tempdir().context("unable to create smoke scratch directory")?;
    let codex_home = scratch.path().join("codex-home");
    std::fs::create_dir(&codex_home).context("unable to create isolated Codex home")?;
    let workspace = scratch.path().join("workspace");
    std::fs::create_dir(&workspace).context("unable to create isolated workspace")?;
    let token_path = scratch.path().join("reconciliation-bearer");
    let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    write_private_token(&token_path, &token)?;
    let store_path = scratch.path().join("bridge.sqlite");

    let probe_listener = std::net::TcpListener::bind("127.0.0.1:0")
        .context("unable to reserve a loopback smoke port")?;
    let port = probe_listener
        .local_addr()
        .context("unable to inspect the loopback smoke port")?
        .port();
    drop(probe_listener);
    let listen_endpoint = format!("ws://127.0.0.1:{port}");
    let endpoint = format!("{listen_endpoint}/");

    let mut child = spawn_server(&binary, &listen_endpoint, &token_path, &codex_home)?;
    wait_until_listening(port).await?;
    exact_health(port).await?;

    // The operator harness alone creates the thread. Production recovery has no thread/start API.
    let (thread_id, seeded_turn_id) = operator_start_thread(&endpoint, &token, &workspace).await?;
    let gate = configured_gate(&endpoint, &expected_version, &token_path)?;
    let endpoint_label = gate.endpoint_label().as_str().to_owned();
    let store = StoreHandle::open(&store_path)
        .await
        .context("unable to open durable recovery store")?;
    store
        .reserve_external_epoch(
            &endpoint_label,
            lark_codex_bridge::store::ExternalUncertaintyReason::BridgeRestart,
        )
        .await
        .context("unable to seed durable external epoch")?;
    store
        .register_external_thread(&endpoint_label, &thread_id)
        .await
        .context("unable to adopt operator-created thread")?;
    let coordinator = ExternalRecoveryCoordinator::start(
        gate,
        store.clone(),
        CancellationToken::new(),
        ExternalRecoverySettings::default(),
    )
    .context("unable to start external recovery coordinator")?;

    let first_epoch = wait_for_ready_with_evidence(&coordinator, 0, "initial").await?;
    assert_ready_snapshot(&coordinator, &thread_id, first_epoch).await?;
    child.ensure_running()?;
    exact_health(port).await?;

    coordinator
        .request_socket_reconnect()
        .await
        .context("socket-only reconnect request failed")?;
    let socket_epoch =
        wait_for_ready_with_evidence(&coordinator, first_epoch, "socket reconnect").await?;
    assert_ready_snapshot(&coordinator, &thread_id, socket_epoch).await?;
    child.ensure_running()?;
    exact_health(port).await?;

    // The harness, not the bridge, owns and restarts the process. The bridge only records the
    // operator-announced restart, fences its socket, and reconnects with backoff.
    let restart_state = coordinator.subscribe_state();
    let unavailable = tokio::spawn(wait_for_unavailable(restart_state, socket_epoch));
    child.stop().await?;
    coordinator
        .note_operator_server_restart()
        .await
        .context("operator restart notification failed")?;
    unavailable
        .await
        .context("unavailability observer task failed")??;
    child = spawn_server(&binary, &listen_endpoint, &token_path, &codex_home)?;
    wait_until_listening(port).await?;
    let server_restart_epoch =
        wait_for_ready_with_evidence(&coordinator, socket_epoch, "server restart").await?;
    assert_ready_snapshot(&coordinator, &thread_id, server_restart_epoch).await?;
    child.ensure_running()?;
    exact_health(port).await?;

    let thread_ids = operator_list_threads(&endpoint, &token).await?;
    ensure!(
        thread_ids.iter().filter(|id| *id == &thread_id).count() == 1,
        "recovery must retain exactly the operator-created thread and replay no thread/start"
    );
    let turn_ids = operator_read_turns(&endpoint, &token, &thread_id).await?;
    ensure!(
        turn_ids.iter().filter(|id| *id == &seeded_turn_id).count() == 1,
        "recovery must retain exactly the operator-created turn and replay no turn/start"
    );

    coordinator
        .shutdown()
        .await
        .context("socket-only recovery shutdown failed")?;
    child.ensure_running()?;
    exact_health(port).await?;
    store.shutdown().await.context("store shutdown failed")?;
    child.stop().await?;

    eprintln!(
        "external_reconciliation_epochs initial={first_epoch} socket={socket_epoch} server_restart={server_restart_epoch}"
    );
    Ok(())
}

async fn assert_ready_snapshot(
    coordinator: &ExternalRecoveryCoordinator,
    thread_id: &str,
    epoch: u64,
) -> Result<()> {
    let snapshot = coordinator
        .thread_snapshot(thread_id)
        .await
        .context("unable to read durable external thread snapshot")?
        .context("managed external thread snapshot is missing")?;
    ensure!(snapshot.epoch == epoch, "thread snapshot epoch is stale");
    ensure!(
        snapshot.state == ExternalThreadState::Ready,
        "thread did not finish authoritative reconciliation"
    );
    ensure!(
        snapshot.reason.is_none(),
        "ready thread retained uncertainty"
    );
    Ok(())
}

async fn wait_for_ready_with_evidence(
    coordinator: &ExternalRecoveryCoordinator,
    prior_epoch: u64,
    phase: &str,
) -> Result<u64> {
    let mut state = coordinator.subscribe_state();
    timeout(READY_TIMEOUT, async {
        loop {
            let observed = *state.borrow_and_update();
            eprintln!("external_reconciliation_state phase={phase} state={observed:?}");
            if let Some(epoch) = observed.ready_epoch()
                && epoch > prior_epoch
            {
                return Ok(epoch);
            }
            state
                .changed()
                .await
                .context("external recovery state channel closed")?;
        }
    })
    .await
    .with_context(|| {
        format!(
            "{phase} reconciliation did not become ready; last state: {:?}",
            coordinator.state()
        )
    })?
}

async fn wait_for_unavailable(
    mut state: tokio::sync::watch::Receiver<ExternalRecoveryState>,
    minimum_epoch: u64,
) -> Result<()> {
    timeout(READY_TIMEOUT, async {
        loop {
            if matches!(
                *state.borrow_and_update(),
                ExternalRecoveryState::Unavailable { epoch, .. } if epoch >= minimum_epoch
            ) {
                return;
            }
            if state.changed().await.is_err() {
                return;
            }
        }
    })
    .await
    .context("recovery did not expose operator restart unavailability")?;
    Ok(())
}

fn spawn_server(
    binary: &Path,
    listen_endpoint: &str,
    token_path: &Path,
    codex_home: &Path,
) -> Result<ChildGuard> {
    let child = Command::new(binary)
        .arg("app-server")
        .arg("--listen")
        .arg(listen_endpoint)
        .arg("--ws-auth")
        .arg("capability-token")
        .arg("--ws-token-file")
        .arg(token_path)
        .env("CODEX_HOME", codex_home)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("unable to start the exact external app-server binary")?;
    Ok(ChildGuard { child })
}

async fn operator_start_thread(
    endpoint: &str,
    token: &str,
    cwd: &Path,
) -> Result<(String, String)> {
    let mut socket = operator_connect(endpoint, token).await?;
    let result = operator_request(
        &mut socket,
        2,
        "thread/start",
        json!({"cwd": cwd, "ephemeral": false, "historyMode": "paginated"}),
    )
    .await?;
    let thread_id = result["thread"]["id"]
        .as_str()
        .context("operator thread/start response omitted thread id")?
        .to_owned();
    let turn = operator_request(
        &mut socket,
        3,
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [{"type": "text", "text": "Reply only OK."}],
            "approvalPolicy": "never"
        }),
    )
    .await?;
    let turn_id = turn["turn"]["id"]
        .as_str()
        .context("operator turn/start response omitted turn id")?
        .to_owned();
    tokio::time::sleep(Duration::from_millis(100)).await;
    operator_request(
        &mut socket,
        4,
        "turn/interrupt",
        json!({"threadId": thread_id, "turnId": turn_id}),
    )
    .await?;
    close_operator(socket).await;
    Ok((thread_id, turn_id))
}

async fn operator_list_threads(endpoint: &str, token: &str) -> Result<Vec<String>> {
    let mut socket = operator_connect(endpoint, token).await?;
    let result = operator_request(
        &mut socket,
        2,
        "thread/list",
        json!({"limit": 100, "sortDirection": "desc"}),
    )
    .await?;
    let ids = result["data"]
        .as_array()
        .context("operator thread/list result omitted data")?
        .iter()
        .map(|thread| {
            thread["id"]
                .as_str()
                .map(str::to_owned)
                .context("operator thread/list row omitted id")
        })
        .collect::<Result<Vec<_>>>()?;
    close_operator(socket).await;
    Ok(ids)
}

async fn operator_read_turns(endpoint: &str, token: &str, thread_id: &str) -> Result<Vec<String>> {
    let mut socket = operator_connect(endpoint, token).await?;
    let result = operator_request(
        &mut socket,
        2,
        "thread/turns/list",
        json!({"threadId": thread_id, "limit": 100, "sortDirection": "asc"}),
    )
    .await?;
    let ids = result["data"]
        .as_array()
        .context("operator thread/turns/list omitted data")?
        .iter()
        .map(|turn| {
            turn["id"]
                .as_str()
                .map(str::to_owned)
                .context("operator thread/read turn omitted id")
        })
        .collect::<Result<Vec<_>>>()?;
    close_operator(socket).await;
    Ok(ids)
}

async fn operator_connect(
    endpoint: &str,
    token: &str,
) -> Result<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
> {
    let mut request = endpoint
        .into_client_request()
        .context("unable to build operator WebSocket request")?;
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token}"))
            .context("operator bearer header is invalid")?,
    );
    let (mut socket, _) = timeout(STARTUP_TIMEOUT, connect_async(request))
        .await
        .context("operator WebSocket connect timed out")??;
    let initialized = operator_request(
        &mut socket,
        1,
        "initialize",
        json!({
            "clientInfo": {"name": "external-recovery-smoke-operator", "version": "1.0.0"},
            "capabilities": {"experimentalApi": true}
        }),
    )
    .await?;
    ensure!(
        initialized["userAgent"].as_str().is_some(),
        "operator initialize response omitted user agent"
    );
    socket
        .send(Message::Text(
            json!({"method": "initialized"}).to_string().into(),
        ))
        .await
        .context("unable to send operator initialized notification")?;
    Ok(socket)
}

async fn operator_request<S>(
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
    id: i64,
    method: &str,
    params: Value,
) -> Result<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    socket
        .send(Message::Text(
            json!({"id": id, "method": method, "params": params})
                .to_string()
                .into(),
        ))
        .await
        .with_context(|| format!("unable to send operator {method}"))?;
    timeout(STARTUP_TIMEOUT, async {
        loop {
            let message = socket
                .next()
                .await
                .context("operator socket closed before response")??;
            let Message::Text(text) = message else {
                continue;
            };
            let value: Value = serde_json::from_str(&text)
                .with_context(|| format!("operator {method} response was not JSON"))?;
            if value["id"] == id {
                ensure!(
                    value.get("error").is_none(),
                    "operator {method} returned an error"
                );
                return value
                    .get("result")
                    .cloned()
                    .context("operator response omitted result");
            }
        }
    })
    .await
    .with_context(|| format!("operator {method} timed out"))?
}

async fn close_operator<S>(mut socket: tokio_tungstenite::WebSocketStream<S>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let _ = socket.close(None).await;
}

async fn exact_health(port: u16) -> Result<()> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(5))
        .build()
        .context("unable to create bounded health client")?;
    let response = client
        .get(format!("http://127.0.0.1:{port}/healthz"))
        .send()
        .await
        .context("external server health request failed")?;
    ensure!(
        response.status() == reqwest::StatusCode::OK,
        "external server health was not exact HTTP 200"
    );
    Ok(())
}

fn configured_gate(
    endpoint: &str,
    expected_version: &str,
    token_path: &Path,
) -> Result<ExternalEndpointGate> {
    ExternalEndpointGate::new(ExternalEndpointConfig {
        endpoint: endpoint.to_owned(),
        expected_codex_version: expected_version.to_owned(),
        capability_profile: ExternalCapabilityProfile::ResumeShared,
        authentication: ExternalAuthentication::BearerTokenFile {
            path: token_path.to_path_buf(),
        },
    })
    .context("explicit external reconciliation gate configuration was rejected")
}

fn required_env(name: &str) -> Result<String> {
    let value =
        std::env::var(name).with_context(|| format!("required gate variable {name} is missing"))?;
    if value.is_empty() {
        bail!("required gate variable {name} is empty");
    }
    Ok(value)
}

fn write_private_token(path: &Path, token: &str) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .context("unable to create private smoke bearer")?;
    file.write_all(format!("{token}\n").as_bytes())
        .context("unable to write smoke bearer")?;
    file.sync_all().context("unable to sync smoke bearer")?;
    Ok(())
}

async fn wait_until_listening(port: u16) -> Result<()> {
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    loop {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("exact external app-server did not start before the deadline");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
