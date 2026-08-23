//! Exact-binary reproduction for Codex's Unix-socket WebSocket listener.
//!
//! The ordinary suite compiles but ignores this test. Acceptance invokes it by exact name on
//! Linux and macOS with an explicitly selected native Codex binary. Missing gates, unsupported
//! framing, weak filesystem permissions, unverified peer identity, unsafe collision behavior, or
//! unclassified cleanup are hard failures. Evidence is deliberately content-free and path-free.

#![cfg(unix)]

use std::{
    fs::{self, File, OpenOptions},
    io::{ErrorKind, Read, Write},
    os::unix::{
        fs::{FileTypeExt, MetadataExt, PermissionsExt, symlink},
        net::UnixListener as StdUnixListener,
    },
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use lark_codex_bridge::codex::process::{CodexProcessConfig, probe_version};
use semver::Version;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    process::{Child, Command},
    time::timeout,
};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(20);
const IO_TIMEOUT: Duration = Duration::from_secs(3);
const REJECTION_TIMEOUT: Duration = Duration::from_secs(5);
const CHILD_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_HTTP_HEADER_BYTES: usize = 4 * 1024;
const WEBSOCKET_KEY: &str = "dGhlIHNhbXBsZSBub25jZQ==";
const WEBSOCKET_ACCEPT: &str = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

struct ChildGuard {
    child: Child,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GracefulCleanup {
    Removed,
    Stale,
}

impl GracefulCleanup {
    const fn evidence(self) -> &'static str {
        match self {
            Self::Removed => "removed",
            Self::Stale => "stale-recovered",
        }
    }
}

impl ChildGuard {
    fn pid(&self) -> Result<u32> {
        self.child
            .id()
            .context("smoke-owned app-server omitted its process id")
    }

    fn ensure_running(&mut self) -> Result<()> {
        ensure!(
            self.child
                .try_wait()
                .context("unable to inspect the smoke-owned app-server")?
                .is_none(),
            "Unix listener app-server exited before the probe completed"
        );
        Ok(())
    }

    async fn crash(mut self) -> Result<()> {
        self.child
            .start_kill()
            .context("unable to crash the smoke-owned app-server")?;
        timeout(CHILD_SHUTDOWN_TIMEOUT, self.child.wait())
            .await
            .context("timed out reaping the crashed app-server")??;
        Ok(())
    }

    async fn terminate(mut self) -> Result<()> {
        let pid = self.pid()?;
        let status = timeout(
            CHILD_SHUTDOWN_TIMEOUT,
            Command::new("/bin/kill")
                .arg("-TERM")
                .arg(pid.to_string())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status(),
        )
        .await
        .context("timed out requesting graceful app-server termination")??;
        ensure!(
            status.success(),
            "unable to request graceful app-server termination"
        );
        timeout(CHILD_SHUTDOWN_TIMEOUT, self.child.wait())
            .await
            .context("timed out reaping the gracefully terminated app-server")??;
        Ok(())
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

#[tokio::test]
#[ignore = "requires explicit exact Codex Unix-listener gate; missing configuration is a failure"]
#[allow(clippy::too_many_lines)]
async fn real_exact_binary_exposes_websocket_framing_and_safe_unix_socket_boundaries() -> Result<()>
{
    ensure!(
        required_env("CODEX_UNIX_WS_E2E")? == "1",
        "CODEX_UNIX_WS_E2E must equal 1"
    );
    let binary = PathBuf::from(required_env("CODEX_UNIX_WS_BINARY")?);
    ensure!(
        binary.is_absolute(),
        "CODEX_UNIX_WS_BINARY must be an absolute path"
    );
    ensure_native_server_binary(&binary)?;
    let expected_version = required_env("CODEX_UNIX_WS_EXPECTED_VERSION")?;
    let expected_version_parsed = exact_version(&expected_version)?;
    let probed = probe_version(&CodexProcessConfig {
        binary: binary.clone(),
        codex_home: None,
    })
    .await
    .context("exact Unix-listener binary version probe failed")?;
    ensure!(
        probed == expected_version_parsed,
        "configured Unix-listener binary did not match the exact expected version"
    );

    let scratch = tempfile::Builder::new()
        .prefix("lcb-ux-")
        .tempdir_in("/tmp")
        .context("unable to create a short private Unix-listener scratch directory")?;
    fs::set_permissions(scratch.path(), fs::Permissions::from_mode(0o700))
        .context("unable to restrict the Unix-listener scratch directory")?;
    let parent = fs::symlink_metadata(scratch.path())
        .context("unable to inspect the Unix-listener scratch directory")?;
    ensure!(
        parent.file_type().is_dir() && parent.mode() & 0o777 == 0o700,
        "Unix listener parent must be an owner-only directory"
    );

    let primary_home = create_private_home(scratch.path(), "home-a")?;
    let alternate_home = create_private_home(scratch.path(), "home-b")?;
    let collision_home = create_private_home(scratch.path(), "home-c")?;
    let socket_path = scratch.path().join("app.sock");

    assert_regular_file_collision_is_refused(&binary, &primary_home, &socket_path).await?;
    assert_symlink_collision_is_refused(&binary, &primary_home, &socket_path).await?;

    let stale = StdUnixListener::bind(&socket_path)
        .context("unable to create a stale Unix socket fixture")?;
    let stale_metadata = fs::symlink_metadata(&socket_path)
        .context("unable to inspect the stale Unix socket fixture")?;
    ensure!(
        stale_metadata.file_type().is_socket(),
        "stale fixture was not a Unix socket"
    );
    drop(stale);

    let mut child = spawn_server(&binary, &primary_home, &socket_path)?;
    wait_until_ready(&mut child, &socket_path).await?;
    let live_metadata = secure_socket_metadata(&socket_path, parent.uid())?;
    ensure!(
        (live_metadata.dev(), live_metadata.ino()) != (stale_metadata.dev(), stale_metadata.ino()),
        "exact app-server did not replace the stale socket inode"
    );
    raw_websocket_upgrade(&socket_path, &live_metadata, child.pid()?).await?;
    raw_jsonl_is_rejected(&socket_path).await?;

    expect_start_refused(&binary, &collision_home, &socket_path, "active listener").await?;
    child.ensure_running()?;
    raw_websocket_upgrade(&socket_path, &live_metadata, child.pid()?).await?;

    child.crash().await?;
    let crashed_metadata = fs::symlink_metadata(&socket_path)
        .context("crashed app-server did not leave the expected stale socket evidence")?;
    ensure!(
        crashed_metadata.file_type().is_socket(),
        "abnormal app-server death did not leave a stale Unix socket"
    );

    let mut recovered = spawn_server(&binary, &alternate_home, &socket_path)?;
    wait_until_ready(&mut recovered, &socket_path).await?;
    let recovered_metadata = secure_socket_metadata(&socket_path, parent.uid())?;
    ensure!(
        (recovered_metadata.dev(), recovered_metadata.ino())
            != (crashed_metadata.dev(), crashed_metadata.ino()),
        "exact app-server did not replace the crash-stale socket inode"
    );
    raw_websocket_upgrade(&socket_path, &recovered_metadata, recovered.pid()?).await?;
    recovered.terminate().await?;
    let graceful_cleanup = classify_graceful_cleanup(&socket_path, &recovered_metadata).await?;
    if graceful_cleanup == GracefulCleanup::Stale {
        let mut cleanup_recovery = spawn_server(&binary, &collision_home, &socket_path)?;
        wait_until_ready(&mut cleanup_recovery, &socket_path).await?;
        let cleanup_metadata = secure_socket_metadata(&socket_path, parent.uid())?;
        ensure!(
            (cleanup_metadata.dev(), cleanup_metadata.ino())
                != (recovered_metadata.dev(), recovered_metadata.ino()),
            "exact app-server did not replace the graceful-shutdown stale socket inode"
        );
        raw_websocket_upgrade(&socket_path, &cleanup_metadata, cleanup_recovery.pid()?).await?;
        cleanup_recovery.crash().await?;
    }

    eprintln!(
        "codex_unix_ws_probe platform={} arch={} version={} framing=http-websocket-upgrade socket_mode=0600 owner_match=true peer_uid_match=true peer_pid_match=true peer_gid_observed=true raw_jsonl=eof regular_collision=refused symlink_collision=refused live_collision=refused stale_socket=recovered graceful_cleanup={}",
        std::env::consts::OS,
        std::env::consts::ARCH,
        expected_version_parsed,
        graceful_cleanup.evidence()
    );
    Ok(())
}

fn exact_version(value: &str) -> Result<Version> {
    let parsed = Version::parse(value).context("expected version must be exact semver")?;
    ensure!(
        parsed.pre.is_empty() && parsed.build.is_empty() && parsed.to_string() == value,
        "expected version must be canonical exact semver"
    );
    Ok(parsed)
}

fn create_private_home(parent: &Path, name: &str) -> Result<PathBuf> {
    let path = parent.join(name);
    fs::create_dir(&path).context("unable to create an isolated Codex home")?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
        .context("unable to restrict an isolated Codex home")?;
    Ok(path)
}

async fn assert_regular_file_collision_is_refused(
    binary: &Path,
    codex_home: &Path,
    socket_path: &Path,
) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(socket_path)
        .context("unable to create the regular collision fixture")?;
    file.write_all(b"sentinel")
        .context("unable to write the regular collision fixture")?;
    file.sync_all()
        .context("unable to sync the regular collision fixture")?;
    drop(file);
    expect_start_refused(binary, codex_home, socket_path, "regular file").await?;
    ensure!(
        fs::read(socket_path).context("unable to verify the regular collision fixture")?
            == b"sentinel",
        "exact app-server modified a pre-existing regular file"
    );
    fs::remove_file(socket_path).context("unable to remove the regular collision fixture")?;
    Ok(())
}

async fn assert_symlink_collision_is_refused(
    binary: &Path,
    codex_home: &Path,
    socket_path: &Path,
) -> Result<()> {
    let target = socket_path.with_extension("target");
    fs::write(&target, b"sentinel").context("unable to create the symlink target fixture")?;
    symlink(&target, socket_path).context("unable to create the symlink collision fixture")?;
    expect_start_refused(binary, codex_home, socket_path, "symbolic link").await?;
    ensure!(
        fs::symlink_metadata(socket_path)
            .context("unable to verify the symlink collision fixture")?
            .file_type()
            .is_symlink(),
        "exact app-server replaced a pre-existing symbolic link"
    );
    ensure!(
        fs::read(&target).context("unable to verify the symlink target fixture")? == b"sentinel",
        "exact app-server modified a symbolic-link target"
    );
    fs::remove_file(socket_path).context("unable to remove the symlink collision fixture")?;
    fs::remove_file(target).context("unable to remove the symlink target fixture")?;
    Ok(())
}

fn spawn_server(binary: &Path, codex_home: &Path, socket_path: &Path) -> Result<ChildGuard> {
    let endpoint = format!("unix://{}", socket_path.display());
    let child = Command::new(binary)
        .arg("app-server")
        .arg("--listen")
        .arg(endpoint)
        .env("CODEX_HOME", codex_home)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("unable to start the exact Unix-listener app-server")?;
    Ok(ChildGuard { child })
}

async fn expect_start_refused(
    binary: &Path,
    codex_home: &Path,
    socket_path: &Path,
    fixture: &str,
) -> Result<()> {
    let mut candidate = spawn_server(binary, codex_home, socket_path)?;
    let status = if let Ok(status) = timeout(REJECTION_TIMEOUT, candidate.child.wait()).await {
        status.context("unable to reap a rejected Unix listener")?
    } else {
        let _ = candidate.child.start_kill();
        let _ = candidate.child.wait().await;
        bail!("exact app-server did not reject the {fixture} collision")
    };
    ensure!(
        !status.success(),
        "exact app-server accepted the {fixture} collision"
    );
    Ok(())
}

async fn wait_until_ready(child: &mut ChildGuard, socket_path: &Path) -> Result<()> {
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    loop {
        child.ensure_running()?;
        if fs::symlink_metadata(socket_path).is_ok_and(|metadata| metadata.file_type().is_socket())
            && UnixStream::connect(socket_path).await.is_ok()
        {
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "exact Unix listener did not become ready before the deadline"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn secure_socket_metadata(socket_path: &Path, expected_uid: u32) -> Result<fs::Metadata> {
    let metadata =
        fs::symlink_metadata(socket_path).context("unable to inspect the exact Unix listener")?;
    ensure!(
        metadata.file_type().is_socket(),
        "exact Unix listener path is not a socket"
    );
    ensure!(
        metadata.uid() == expected_uid,
        "exact Unix listener owner differs from its private parent owner"
    );
    ensure!(
        metadata.mode() & 0o777 == 0o600,
        "exact Unix listener mode is not owner-only 0600"
    );
    Ok(metadata)
}

async fn raw_websocket_upgrade(
    socket_path: &Path,
    before: &fs::Metadata,
    expected_pid: u32,
) -> Result<()> {
    let mut stream = timeout(IO_TIMEOUT, UnixStream::connect(socket_path))
        .await
        .context("raw Unix WebSocket connect timed out")??;
    let peer = stream
        .peer_cred()
        .context("platform did not expose Unix peer credentials")?;
    let _peer_gid = peer.gid();
    ensure!(
        peer.uid() == before.uid(),
        "connected Unix peer UID did not match the listener owner"
    );
    ensure!(
        peer.pid().and_then(|pid| u32::try_from(pid).ok()) == Some(expected_pid),
        "connected Unix peer PID did not match the spawned app-server"
    );
    let after = fs::symlink_metadata(socket_path)
        .context("unable to revalidate the Unix listener after connect")?;
    ensure!(
        after.file_type().is_socket() && (after.dev(), after.ino()) == (before.dev(), before.ino()),
        "Unix listener inode changed across connect"
    );

    let request = format!(
        "GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {WEBSOCKET_KEY}\r\nSec-WebSocket-Version: 13\r\n\r\n"
    );
    timeout(IO_TIMEOUT, stream.write_all(request.as_bytes()))
        .await
        .context("raw WebSocket Upgrade write timed out")??;
    let response = read_http_headers(&mut stream).await?;
    let response =
        std::str::from_utf8(&response).context("raw WebSocket Upgrade response was not UTF-8")?;
    let normalized = response.to_ascii_lowercase();
    ensure!(
        response.starts_with("HTTP/1.1 101 Switching Protocols\r\n"),
        "Unix listener did not return an exact HTTP 101 WebSocket Upgrade"
    );
    ensure!(
        normalized.contains("upgrade: websocket\r\n")
            && normalized.contains("connection: upgrade\r\n")
            && normalized.contains(&format!(
                "sec-websocket-accept: {}\r\n",
                WEBSOCKET_ACCEPT.to_ascii_lowercase()
            )),
        "Unix listener returned an incomplete WebSocket Upgrade response"
    );
    Ok(())
}

async fn read_http_headers(stream: &mut UnixStream) -> Result<Vec<u8>> {
    timeout(IO_TIMEOUT, async {
        let mut response = Vec::new();
        let mut byte = [0_u8; 1];
        loop {
            let read = stream
                .read(&mut byte)
                .await
                .context("unable to read the raw WebSocket Upgrade response")?;
            ensure!(
                read == 1,
                "Unix listener closed before completing HTTP headers"
            );
            response.push(byte[0]);
            ensure!(
                response.len() <= MAX_HTTP_HEADER_BYTES,
                "raw WebSocket Upgrade headers exceeded the evidence bound"
            );
            if response.ends_with(b"\r\n\r\n") {
                return Ok(response);
            }
        }
    })
    .await
    .context("raw WebSocket Upgrade response timed out")?
}

async fn raw_jsonl_is_rejected(socket_path: &Path) -> Result<()> {
    let mut stream = timeout(IO_TIMEOUT, UnixStream::connect(socket_path))
        .await
        .context("raw JSONL negative connect timed out")??;
    timeout(
        IO_TIMEOUT,
        stream.write_all(b"{\"id\":1,\"method\":\"initialize\",\"params\":{}}\n"),
    )
    .await
    .context("raw JSONL negative write timed out")??;
    timeout(IO_TIMEOUT, stream.shutdown())
        .await
        .context("raw JSONL negative half-close timed out")??;
    let mut byte = [0_u8; 1];
    let read = timeout(IO_TIMEOUT, stream.read(&mut byte))
        .await
        .context("Unix listener did not reject raw JSONL before the deadline")??;
    ensure!(
        read == 0,
        "Unix listener emitted stream bytes in response to raw JSONL"
    );
    Ok(())
}

async fn classify_graceful_cleanup(
    socket_path: &Path,
    prior: &fs::Metadata,
) -> Result<GracefulCleanup> {
    let metadata = match fs::symlink_metadata(socket_path) {
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(GracefulCleanup::Removed),
        Ok(metadata) => metadata,
        Err(error) => return Err(error).context("unable to inspect graceful listener cleanup"),
    };
    ensure!(
        metadata.file_type().is_socket()
            && (metadata.dev(), metadata.ino()) == (prior.dev(), prior.ino()),
        "graceful shutdown left a substituted or non-socket listener path"
    );
    let connected = timeout(IO_TIMEOUT, UnixStream::connect(socket_path))
        .await
        .context("graceful listener liveness check timed out")?;
    ensure!(
        connected.is_err(),
        "gracefully terminated app-server left a live Unix listener"
    );
    match fs::symlink_metadata(socket_path) {
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(GracefulCleanup::Removed),
        Ok(after)
            if after.file_type().is_socket()
                && (after.dev(), after.ino()) == (prior.dev(), prior.ino()) =>
        {
            Ok(GracefulCleanup::Stale)
        }
        Ok(_) => bail!("Unix listener path changed during graceful cleanup classification"),
        Err(error) => Err(error).context("unable to revalidate graceful listener cleanup"),
    }
}

fn required_env(name: &str) -> Result<String> {
    let value = std::env::var(name)
        .with_context(|| format!("required Unix-listener gate variable {name} is missing"))?;
    if value.is_empty() {
        bail!("required Unix-listener gate variable {name} is empty");
    }
    Ok(value)
}

fn ensure_native_server_binary(path: &Path) -> Result<()> {
    let mut magic = [0_u8; 2];
    File::open(path)
        .context("unable to open the exact Unix-listener binary")?
        .read_exact(&mut magic)
        .context("unable to inspect the exact Unix-listener binary")?;
    ensure!(
        magic != *b"#!",
        "CODEX_UNIX_WS_BINARY must name the native Codex executable, not a launcher script"
    );
    Ok(())
}
