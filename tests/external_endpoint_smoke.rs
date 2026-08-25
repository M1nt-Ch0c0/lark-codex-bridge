//! Explicit real-binary acceptance smoke for the external endpoint admission gate.
//!
//! This test is ignored by the ordinary suite, but unlike a best-effort smoke it never treats a
//! missing gate or missing configuration as success. Invoke it by exact name with all documented
//! environment variables; any omitted variable, invalid credential acceptance, version mismatch,
//! or capability failure is a hard test failure.

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
        ExternalEndpointGate, ExternalGateError,
    },
    process::{CodexProcessConfig, probe_version},
};
use semver::Version;
use tokio::{net::TcpStream, process::Command, time::timeout};
use uuid::Uuid;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(20);
const CHILD_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
struct ChildGuard {
    child: tokio::process::Child,
}

impl ChildGuard {
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
async fn real_exact_binary_enforces_external_auth_version_and_capability_gate() -> Result<()> {
    ensure!(
        required_env("CODEX_EXTERNAL_GATE_E2E")? == "1",
        "CODEX_EXTERNAL_GATE_E2E must equal 1"
    );
    let binary = PathBuf::from(required_env("CODEX_EXTERNAL_GATE_BINARY")?);
    ensure!(
        binary.is_absolute(),
        "CODEX_EXTERNAL_GATE_BINARY must be an absolute path"
    );
    let expected_version = required_env("CODEX_EXTERNAL_GATE_EXPECTED_VERSION")?;
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
    let valid_token_path = scratch.path().join("valid-bearer");
    let invalid_token_path = scratch.path().join("invalid-bearer");
    let valid_token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let invalid_token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    write_private_token(&valid_token_path, &valid_token)?;
    write_private_token(&invalid_token_path, &invalid_token)?;

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
        .arg(&valid_token_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("unable to start the exact external app-server binary")?;
    let child = ChildGuard { child };
    wait_until_listening(port).await?;

    let invalid = configured_gate(&endpoint, &expected_version, &invalid_token_path)?;
    ensure!(
        invalid.check().await == Err(ExternalGateError::AuthenticationRejected),
        "the exact external app-server did not reject an invalid bearer"
    );

    let wrong_expected = neighboring_version(&expected_version_parsed);
    let wrong_version = configured_gate(&endpoint, &wrong_expected.to_string(), &valid_token_path);
    ensure!(
        matches!(
            wrong_version,
            Err(ref error)
                if error.downcast_ref::<ExternalGateError>().is_some_and(|error| matches!(
                    error,
                    ExternalGateError::InvalidExpectedVersion
                        | ExternalGateError::UnsupportedCapabilityProfile
                ))
        ),
        "the external gate did not reject an unpromoted mismatched exact version"
    );

    let accepted = configured_gate(&endpoint, &expected_version, &valid_token_path)?
        .check()
        .await
        .context("authenticated exact-version capability gate failed")?;
    ensure!(
        accepted.codex_version == expected_version_parsed,
        "gate report did not retain the exact reviewed version"
    );

    child.stop().await?;
    Ok(())
}

fn neighboring_version(version: &Version) -> Version {
    let patch = if version.patch == 0 {
        1
    } else {
        version.patch - 1
    };
    Version::new(version.major, version.minor, patch)
}

fn required_env(name: &str) -> Result<String> {
    let value =
        std::env::var(name).with_context(|| format!("required gate variable {name} is missing"))?;
    if value.is_empty() {
        bail!("required gate variable {name} is empty");
    }
    Ok(value)
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
    .context("explicit external gate configuration was rejected")
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
