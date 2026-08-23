//! Explicit exact-binary lifecycle smoke for the authenticated external transport.
//!
//! Ordinary suites ignore this test. Its exact invocation is fail-closed: missing configuration,
//! a skipped test, an inexact binary, failed authentication/admission, absent health, or server
//! death after orderly/abrupt client loss is a hard failure.

use std::{
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use lark_codex_bridge::codex::{
    external::{
        ExternalAuthentication, ExternalCapabilityProfile, ExternalEndpointConfig,
        ExternalEndpointGate,
    },
    external_transport::ExternalReadOnlyConnection,
    process::{CodexProcessConfig, probe_version},
    rpc::ConnectionEpoch,
    transport::{TransportExit, WebSocketCloseHandshake, WebSocketCloseReport},
    types::ThreadListParams,
};
use semver::Version;
use tokio::{net::TcpStream, process::Command, time::timeout};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(20);
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
            "external app-server exited while bridge sockets were being exercised"
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
async fn real_exact_binary_transport_preserves_external_server_across_two_clients_and_fresh_reuse()
-> Result<()> {
    ensure!(
        required_env("CODEX_EXTERNAL_TRANSPORT_E2E")? == "1",
        "CODEX_EXTERNAL_TRANSPORT_E2E must equal 1"
    );
    let binary = PathBuf::from(required_env("CODEX_EXTERNAL_TRANSPORT_BINARY")?);
    ensure!(
        binary.is_absolute(),
        "CODEX_EXTERNAL_TRANSPORT_BINARY must be an absolute path"
    );
    let expected_version = required_env("CODEX_EXTERNAL_TRANSPORT_EXPECTED_VERSION")?;
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
        "configured exact binary did not match the expected version"
    );

    let scratch = tempfile::tempdir().context("unable to create smoke scratch directory")?;
    let token_path = scratch.path().join("transport-bearer");
    let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    write_private_token(&token_path, &token)?;

    let probe_listener = std::net::TcpListener::bind("127.0.0.1:0")
        .context("unable to reserve a loopback smoke port")?;
    let port = probe_listener
        .local_addr()
        .context("unable to inspect the loopback smoke port")?
        .port();
    drop(probe_listener);
    let listen_endpoint = format!("ws://127.0.0.1:{port}");
    let endpoint = format!("{listen_endpoint}/");

    let child = Command::new(&binary)
        .arg("app-server")
        .arg("--listen")
        .arg(&listen_endpoint)
        .arg("--ws-auth")
        .arg("capability-token")
        .arg("--ws-token-file")
        .arg(&token_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("unable to start the exact external app-server binary")?;
    let mut child = ChildGuard { child };
    wait_until_listening(port).await?;

    let gate = configured_gate(&endpoint, &expected_version, &token_path)?;
    let (first, second) = tokio::join!(
        ExternalReadOnlyConnection::connect(
            &gate,
            ConnectionEpoch::new(1),
            CancellationToken::new(),
        ),
        ExternalReadOnlyConnection::connect(
            &gate,
            ConnectionEpoch::new(2),
            CancellationToken::new(),
        )
    );
    let mut first = first.context("first authenticated read-only client failed")?;
    let mut second = second.context("second authenticated read-only client failed")?;
    let first_params = one_row_list();
    let second_params = one_row_list();
    let (first_list, second_list) = tokio::join!(
        first.list_threads(&first_params),
        second.list_threads(&second_params),
    );
    ensure!(
        first_list
            .context("first read-only client list failed")?
            .data
            .len()
            <= 1,
        "first client exceeded its one-row response bound"
    );
    ensure!(
        second_list
            .context("second read-only client list failed")?
            .data
            .len()
            <= 1,
        "second client exceeded its one-row response bound"
    );

    let first_exit = first.shutdown().await;
    let first_close = close_report(first_exit)
        .context("orderly bridge shutdown did not return WebSocket close evidence")?;
    ensure!(
        second.abort() == TransportExit::Aborted,
        "bridge crash path was not abrupt"
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    child.ensure_running()?;
    exact_health(port).await?;

    let mut fresh = ExternalReadOnlyConnection::connect(
        &gate,
        ConnectionEpoch::new(3),
        CancellationToken::new(),
    )
    .await
    .context("fresh authenticated client could not initialize after bridge socket loss")?;
    ensure!(
        fresh
            .list_threads(&one_row_list())
            .await
            .context("fresh client list failed")?
            .data
            .len()
            <= 1,
        "fresh client exceeded its one-row response bound"
    );
    let fresh_close = close_report(fresh.shutdown().await)
        .context("fresh client did not return WebSocket close evidence")?;
    child.ensure_running()?;
    exact_health(port).await?;

    // Sanitized and truthful: the exact binary may complete the close handshake or reproduce the
    // known 1006/unclean behavior. The two states are never collapsed into one success boolean.
    eprintln!(
        "external_transport_close_observation first_complete={} fresh_complete={}",
        first_close.handshake == WebSocketCloseHandshake::Complete,
        fresh_close.handshake == WebSocketCloseHandshake::Complete,
    );

    child.stop().await?;
    Ok(())
}

fn one_row_list() -> ThreadListParams {
    ThreadListParams {
        limit: Some(1),
        ..ThreadListParams::default()
    }
}

fn close_report(exit: TransportExit) -> Option<WebSocketCloseReport> {
    match exit {
        TransportExit::WebSocketClosed(report) => Some(report),
        TransportExit::Cancelled
        | TransportExit::StdoutEof
        | TransportExit::ProtocolViolation
        | TransportExit::ReadError(_)
        | TransportExit::WriteError(_)
        | TransportExit::ConnectionFailed
        | TransportExit::Aborted
        | TransportExit::TaskFailed => None,
    }
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
        capability_profile: ExternalCapabilityProfile::ObserveShared,
        authentication: ExternalAuthentication::BearerTokenFile {
            path: token_path.to_path_buf(),
        },
    })
    .context("explicit external transport gate configuration was rejected")
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
