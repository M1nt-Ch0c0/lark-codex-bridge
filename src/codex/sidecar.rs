//! Supervised Codex protocol sidecar process and bootstrap handshake.
//!
//! The Node child and the Codex app-server it spawns share one owned process
//! group (or Windows Job object). Rust consumes the bounded hello/configure
//! exchange before transferring the remaining stable domain JSON-RPC stream to
//! the existing transport and RPC actors.

use std::{
    collections::BTreeSet, ffi::OsString, fmt, path::PathBuf, process::Stdio, time::Duration,
};

#[cfg(unix)]
use process_wrap::tokio::ProcessGroup;
#[cfg(windows)]
use process_wrap::tokio::{JobObject, KillOnDrop};
use process_wrap::tokio::{TokioChildWrapper, TokioCommandWrap};
use semver::Version;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{ChildStderr, ChildStdin, ChildStdout, Command},
    time::{Instant, timeout, timeout_at},
};
use uuid::Uuid;

use crate::{
    codex::{
        compat::WireAdapter,
        process::{
            ProcessCleanupState, ProcessError, ProcessExit, SidecarBootstrapFailure,
            SidecarSpawnFailure, drop_or_forget_unreaped_child, process_exit,
            reap_owned_process_tree, record_wait_identity_loss, wait_owned_leader_or_job,
        },
        supervisor::{AppServerProcess, ProcessStdio, ProtocolInfo},
    },
    limits::{
        CODEX_SIDECAR_FRAME_BYTES, CODEX_SIDECAR_HANDSHAKE_TIMEOUT, CODEX_SIDECAR_PENDING_CAPACITY,
        CODEX_SIDECAR_SHUTDOWN_GRACE,
    },
};

#[cfg(unix)]
use crate::codex::process::wait_for_owned_leader_exit_without_reaping;

/// Stable local protocol identifier. It is deliberately independent of an
/// upstream Codex release.
pub const CODEX_SIDECAR_PROTOCOL: &str = "codex-sidecar-wire";
/// First and currently only stable local protocol version.
pub const CODEX_SIDECAR_VERSION: u32 = 1;
/// Exact upstream releases independently adapted by the checked-in sidecar.
pub const SUPPORTED_SIDECAR_CODEX_VERSIONS: &[&str] = &["0.149.0", "0.151.0"];
/// Capabilities that must be negotiated before any JSON-RPC frame is accepted.
pub const REQUIRED_SIDECAR_CAPABILITIES: &[&str] = &[
    "bounded-ndjson",
    "correlated-requests",
    "correlated-server-requests",
    "epoch-on-restart",
    "no-mutation-replay",
    "priority-control-lane",
    "stable-domain-jsonrpc",
];

fn inherited_sidecar_environment() -> Vec<(&'static str, OsString)> {
    let mut selected = Vec::new();
    if let Some(value) = std::env::var_os("PATH") {
        selected.push(("PATH", value));
    }
    #[cfg(unix)]
    if let Some(value) = std::env::var_os("HOME") {
        selected.push(("HOME", value));
    }
    #[cfg(windows)]
    for name in ["PATHEXT", "SystemRoot", "USERPROFILE", "WINDIR"] {
        if let Some(value) = std::env::var_os(name) {
            selected.push((name, value));
        }
    }
    selected
}

/// Process and bootstrap configuration. `Debug` never prints configured paths
/// or adapter arguments.
#[derive(Clone, Eq, PartialEq)]
pub struct CodexSidecarConfig {
    pub node_binary: PathBuf,
    pub entrypoint: PathBuf,
    /// Optional upstream override. `None` selects the sidecar package's exact
    /// pinned `@openai/codex` dependency.
    pub codex_binary: Option<PathBuf>,
    pub codex_home: Option<PathBuf>,
    /// Non-secret prefix arguments for wrappers and portable fake binaries.
    pub codex_arguments: Vec<String>,
    pub max_frame_bytes: usize,
    pub max_pending: usize,
    pub handshake_timeout: Duration,
    pub shutdown_grace: Duration,
}

impl Default for CodexSidecarConfig {
    fn default() -> Self {
        Self {
            node_binary: PathBuf::from("node"),
            entrypoint: PathBuf::from("codex-sidecar/index.cjs"),
            codex_binary: None,
            codex_home: None,
            codex_arguments: Vec::new(),
            max_frame_bytes: CODEX_SIDECAR_FRAME_BYTES,
            max_pending: CODEX_SIDECAR_PENDING_CAPACITY,
            handshake_timeout: CODEX_SIDECAR_HANDSHAKE_TIMEOUT,
            shutdown_grace: CODEX_SIDECAR_SHUTDOWN_GRACE,
        }
    }
}

impl fmt::Debug for CodexSidecarConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexSidecarConfig")
            .field("node_binary", &"[configured]")
            .field("entrypoint", &"[configured]")
            .field("codex_binary_configured", &self.codex_binary.is_some())
            .field(
                "codex_home",
                &self.codex_home.as_ref().map(|_| "[configured]"),
            )
            .field("codex_argument_count", &self.codex_arguments.len())
            .field("max_frame_bytes", &self.max_frame_bytes)
            .field("max_pending", &self.max_pending)
            .field("handshake_timeout", &self.handshake_timeout)
            .field("shutdown_grace", &self.shutdown_grace)
            .finish()
    }
}

impl CodexSidecarConfig {
    /// Checks only local, non-secret shape and hard bounds. The sidecar owns
    /// upstream binary discovery and exact version probing.
    ///
    /// # Errors
    ///
    /// Returns a fail-closed configuration error when a path, argument, or
    /// resource limit is outside the reviewed local contract.
    pub fn validate(&self) -> Result<(), ProcessError> {
        if self.node_binary.as_os_str().is_empty()
            || self.entrypoint.as_os_str().is_empty()
            || self
                .codex_binary
                .as_ref()
                .is_some_and(|binary| binary.as_os_str().is_empty())
            || self.codex_arguments.len() > 8
            || self.codex_arguments.iter().any(|argument| {
                argument.is_empty() || argument.len() > 1024 || argument.contains('\0')
            })
            || self.max_frame_bytes == 0
            || self.max_frame_bytes > CODEX_SIDECAR_FRAME_BYTES
            || self.max_pending == 0
            || self.max_pending > CODEX_SIDECAR_PENDING_CAPACITY
            || self.handshake_timeout.is_zero()
            || self.handshake_timeout > CODEX_SIDECAR_HANDSHAKE_TIMEOUT
            || self.shutdown_grace.is_zero()
            || self.shutdown_grace > CODEX_SIDECAR_SHUTDOWN_GRACE
        {
            return Err(ProcessError::InvalidSidecarConfig);
        }
        if let Some(home) = &self.codex_home {
            let metadata =
                std::fs::metadata(home).map_err(|source| ProcessError::InvalidCodexHome {
                    source: Some(source),
                })?;
            if !metadata.is_dir() {
                return Err(ProcessError::InvalidCodexHome { source: None });
            }
        }
        Ok(())
    }
}

/// One configured sidecar epoch. Construction does not return until the
/// sidecar has probed and started exactly one supported upstream Codex child.
pub struct CodexSidecarProcess {
    child: Option<Box<dyn TokioChildWrapper>>,
    version: Version,
    stdout: Option<BufReader<ChildStdout>>,
    stdin: Option<ChildStdin>,
    stderr: Option<ChildStderr>,
    exit: Option<ProcessExit>,
    pid: u32,
    shutdown_grace: Duration,
    tree_reaped: bool,
    group_signal_authorized: bool,
    identity_lost: bool,
}

/// Owns the process-tree boundary from the instant the Node leader is spawned.
///
/// The supervisor is allowed to cancel a factory future while bootstrap is in
/// progress. Unix deliberately avoids an irreversible inner `kill_on_drop`;
/// this authority-aware guard invokes the outer process-group wrapper when an
/// in-flight bootstrap future is dropped. Successful bootstrap transfers the
/// same wrapper and authority into [`CodexSidecarProcess`].
struct BootstrapProcessGuard {
    child: Option<Box<dyn TokioChildWrapper>>,
    cleanup_confirmed: bool,
    group_signal_authorized: bool,
    identity_lost: bool,
}

impl BootstrapProcessGuard {
    fn new(child: Box<dyn TokioChildWrapper>) -> Self {
        #[cfg(unix)]
        let group_signal_authorized = child.inner().id().is_some();
        #[cfg(not(unix))]
        let group_signal_authorized = true;
        Self {
            child: Some(child),
            cleanup_confirmed: false,
            group_signal_authorized,
            identity_lost: false,
        }
    }

    fn child_mut(&mut self) -> &mut Box<dyn TokioChildWrapper> {
        self.child
            .as_mut()
            .expect("bootstrap process guard always owns its child")
    }

    fn into_child(mut self) -> Box<dyn TokioChildWrapper> {
        self.child
            .take()
            .expect("bootstrap process guard always owns its child")
    }

    fn mark_cleanup_confirmed(&mut self) {
        self.cleanup_confirmed = true;
    }

    fn cleanup_parts(&mut self) -> (&mut Box<dyn TokioChildWrapper>, &mut bool, &mut bool) {
        (
            self.child
                .as_mut()
                .expect("bootstrap process guard always owns its child"),
            &mut self.group_signal_authorized,
            &mut self.identity_lost,
        )
    }
}

impl Drop for BootstrapProcessGuard {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        if !self.cleanup_confirmed && self.group_signal_authorized {
            // The outer wrapper targets the full POSIX process group or
            // Windows Job object. Waiting is impossible from Drop, but the
            // synchronous kill request closes the cancellation leak window.
            #[cfg(unix)]
            let leader_identity_reserved = child.inner().id().is_some();
            #[cfg(not(unix))]
            let leader_identity_reserved = true;
            if leader_identity_reserved {
                let _ = child.start_kill();
                self.group_signal_authorized = false;
            }
        }
        drop_or_forget_unreaped_child(child, self.identity_lost);
    }
}

impl CodexSidecarProcess {
    async fn wait_leader(&mut self) -> Result<ProcessExit, ProcessError> {
        if let Some(exit) = self.exit {
            return Ok(exit);
        }
        if self.identity_lost {
            return Err(ProcessError::ProcessTreeCleanupUnconfirmed);
        }
        let child = self
            .child
            .as_mut()
            .ok_or(ProcessError::ProcessTreeCleanupUnconfirmed)?;
        let status = child.inner_mut().wait().await;
        match &status {
            #[cfg(unix)]
            Ok(_) => self.group_signal_authorized = false,
            Err(source) => record_wait_identity_loss(
                source,
                &mut self.group_signal_authorized,
                &mut self.identity_lost,
            ),
            #[cfg(not(unix))]
            Ok(_) => {}
        }
        let status = status.map_err(ProcessError::Wait)?;
        let exit = process_exit(self.pid, status);
        self.exit = Some(exit);
        Ok(exit)
    }

    async fn wait_for_exit_without_reaping(&mut self) -> Result<(), ProcessError> {
        if self.exit.is_some() {
            return Ok(());
        }
        if self.identity_lost {
            return Err(ProcessError::ProcessTreeCleanupUnconfirmed);
        }
        #[cfg(unix)]
        {
            wait_for_owned_leader_exit_without_reaping(
                self.pid,
                &mut self.group_signal_authorized,
                &mut self.identity_lost,
            )
            .await
            .map_err(ProcessError::Wait)
        }
        #[cfg(not(unix))]
        {
            self.wait_leader().await.map(|_| ())
        }
    }

    async fn terminate_group(&mut self, grace: Duration) -> Result<ProcessExit, ProcessError> {
        if self.tree_reaped {
            return self.exit.ok_or_else(|| {
                ProcessError::Wait(std::io::Error::other(
                    "Codex sidecar process tree was reaped without a leader status",
                ))
            });
        }
        drop(self.stdin.take());
        drop(self.stdout.take());
        drop(self.stderr.take());

        #[cfg(unix)]
        if self.exit.is_none() {
            tokio::time::sleep(grace).await;
        }
        #[cfg(not(unix))]
        if self.exit.is_none()
            && let Ok(result) = timeout(grace, self.wait_leader()).await
        {
            result?;
        }

        let child = self
            .child
            .as_mut()
            .ok_or(ProcessError::ProcessTreeCleanupUnconfirmed)?;
        let state = ProcessCleanupState {
            cached_exit: &mut self.exit,
            tree_reaped: &mut self.tree_reaped,
            group_signal_authorized: &mut self.group_signal_authorized,
            identity_lost: &mut self.identity_lost,
        };
        reap_owned_process_tree(
            child,
            self.pid,
            grace,
            state,
            "Codex sidecar process tree did not exit within its bound",
        )
        .await
    }
}

impl Drop for CodexSidecarProcess {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        #[cfg(unix)]
        let leader_identity_reserved = child.inner().id().is_some();
        #[cfg(not(unix))]
        let leader_identity_reserved = true;
        if !self.tree_reaped && self.group_signal_authorized && leader_identity_reserved {
            let _ = child.start_kill();
            self.group_signal_authorized = false;
        }
        drop_or_forget_unreaped_child(child, self.identity_lost);
    }
}

impl AppServerProcess for CodexSidecarProcess {
    fn version(&self) -> &Version {
        &self.version
    }

    fn wire_adapter(&self) -> Option<WireAdapter> {
        Some(WireAdapter::SidecarV1)
    }

    fn protocol_info(&self) -> ProtocolInfo {
        ProtocolInfo::SidecarV1 {
            protocol: CODEX_SIDECAR_PROTOCOL,
            version: CODEX_SIDECAR_VERSION,
            capabilities: REQUIRED_SIDECAR_CAPABILITIES.to_vec(),
        }
    }

    fn take_stdio(&mut self) -> Result<ProcessStdio, ProcessError> {
        if self.stdout.is_none() && self.stdin.is_none() && self.stderr.is_none() {
            return Err(ProcessError::StdioAlreadyTaken);
        }
        let stdout = self
            .stdout
            .take()
            .ok_or(ProcessError::StdioUnavailable("stdout"))?;
        let stdin = self
            .stdin
            .take()
            .ok_or(ProcessError::StdioUnavailable("stdin"))?;
        let stderr = self
            .stderr
            .take()
            .ok_or(ProcessError::StdioUnavailable("stderr"))?;
        Ok(ProcessStdio {
            stdout: Box::new(stdout),
            stdin: Box::new(stdin),
            stderr: Box::new(stderr),
        })
    }

    fn wait(
        &mut self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<ProcessExit, ProcessError>> + Send + '_>,
    > {
        Box::pin(self.wait_leader())
    }

    fn wait_for_exit(
        &mut self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ProcessError>> + Send + '_>>
    {
        Box::pin(self.wait_for_exit_without_reaping())
    }

    fn terminate(
        &mut self,
        grace: Duration,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<ProcessExit, ProcessError>> + Send + '_>,
    > {
        let effective = grace.min(self.shutdown_grace);
        Box::pin(self.terminate_group(effective))
    }
}

/// Starts the local adapter, negotiates its exact contract, and transfers no
/// message or credential content during bootstrap.
///
/// # Errors
///
/// Returns a sanitized process, timeout, unsupported-version, or local-wire
/// contract error. A failed bootstrap always terminates the owned process tree.
pub async fn spawn_codex_sidecar(
    config: &CodexSidecarConfig,
) -> Result<CodexSidecarProcess, ProcessError> {
    config.validate()?;
    let mut command = Command::new(&config.node_binary);
    command
        .arg(&config.entrypoint)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_clear()
        .env("NO_COLOR", "1");
    for (name, value) in inherited_sidecar_environment() {
        command.env(name, value);
    }

    let mut command = TokioCommandWrap::from(command);
    #[cfg(windows)]
    command.wrap(KillOnDrop);
    #[cfg(unix)]
    command.wrap(ProcessGroup::leader());
    #[cfg(windows)]
    command.wrap(JobObject);
    let child = command
        .spawn()
        .map_err(|source| ProcessError::SidecarSpawn {
            failure: SidecarSpawnFailure::classify(&source),
        })?;
    let mut child = BootstrapProcessGuard::new(child);
    let Some(pid) = child.child_mut().inner_mut().id() else {
        cleanup_bootstrap_process(&mut child, None, config.shutdown_grace).await?;
        return Err(ProcessError::ProcessTreeCleanupUnconfirmed);
    };
    let (stdin, stdout, stderr) = {
        let inner = child.child_mut().inner_mut();
        (inner.stdin.take(), inner.stdout.take(), inner.stderr.take())
    };
    let (Some(mut stdin), Some(stdout), Some(stderr)) = (stdin, stdout, stderr) else {
        cleanup_bootstrap_process(&mut child, Some(pid), config.shutdown_grace).await?;
        return Err(ProcessError::StdioUnavailable("sidecar stdio"));
    };
    let mut stdout = BufReader::new(stdout);

    let handshake = timeout(config.handshake_timeout, async {
        let hello: HelloFrame = read_frame(&mut stdout, config.max_frame_bytes).await?;
        validate_hello(&hello, config)?;

        let id = format!("configure-{}", Uuid::new_v4());
        let configure = ConfigureFrame {
            v: CODEX_SIDECAR_VERSION,
            kind: "configure",
            id: &id,
            codex_binary: config.codex_binary.as_deref(),
            codex_home: config.codex_home.as_ref(),
            codex_arguments: &config.codex_arguments,
            max_frame_bytes: config.max_frame_bytes,
            max_pending: config.max_pending,
        };
        write_frame(&mut stdin, &configure, config.max_frame_bytes).await?;
        let response: ConfigureResponse = read_frame(&mut stdout, config.max_frame_bytes).await?;
        validate_configure_response(response, &id)
    })
    .await;

    let version = match handshake {
        Ok(Ok(version)) => version,
        Ok(Err(error)) => {
            drop(stdin);
            drop(stdout);
            drop(stderr);
            cleanup_bootstrap_process(&mut child, Some(pid), config.shutdown_grace).await?;
            return Err(error);
        }
        Err(_) => {
            drop(stdin);
            drop(stdout);
            drop(stderr);
            cleanup_bootstrap_process(&mut child, Some(pid), config.shutdown_grace).await?;
            return Err(ProcessError::SidecarHandshakeTimeout(
                config.handshake_timeout,
            ));
        }
    };

    Ok(CodexSidecarProcess {
        child: Some(child.into_child()),
        version,
        stdout: Some(stdout),
        stdin: Some(stdin),
        stderr: Some(stderr),
        exit: None,
        pid,
        shutdown_grace: config.shutdown_grace,
        tree_reaped: false,
        group_signal_authorized: true,
        identity_lost: false,
    })
}

async fn cleanup_bootstrap_process(
    child: &mut BootstrapProcessGuard,
    pid: Option<u32>,
    grace: Duration,
) -> Result<(), ProcessError> {
    let cleanup_deadline = Instant::now() + grace.max(Duration::from_secs(1));
    {
        let (process, group_signal_authorized, identity_lost) = child.cleanup_parts();
        if *identity_lost {
            return Err(ProcessError::ProcessTreeCleanupUnconfirmed);
        }
        #[cfg(unix)]
        let leader_identity_reserved = {
            if process.inner().id().is_none() {
                *group_signal_authorized = false;
            }
            *group_signal_authorized
        };
        #[cfg(not(unix))]
        let leader_identity_reserved = {
            let _ = group_signal_authorized;
            true
        };
        if leader_identity_reserved {
            let _ = process.start_kill();
            #[cfg(unix)]
            {
                *group_signal_authorized = false;
            }
        }
        match timeout_at(cleanup_deadline, wait_owned_leader_or_job(process)).await {
            Ok(Ok(_)) => {}
            Ok(Err(source)) => {
                record_wait_identity_loss(&source, group_signal_authorized, identity_lost);
                return Err(ProcessError::ProcessTreeCleanupUnconfirmed);
            }
            Err(_) => return Err(ProcessError::ProcessTreeCleanupUnconfirmed),
        }
    }

    // A leader wait alone is not Issue #4 process-tree evidence. On POSIX,
    // only an `ESRCH` signal-0 group probe confirms cleanup, and a missing
    // leader PID therefore fails closed. Adoption never launches on other
    // platforms; ordinary sidecar startup preserves its existing wrapper
    // cleanup semantics there and returns the original bootstrap error.
    #[cfg(unix)]
    {
        let pid = pid.ok_or(ProcessError::ProcessTreeCleanupUnconfirmed)?;
        crate::codex::process::wait_for_owned_process_group_empty(pid, cleanup_deadline)
            .await
            .map_err(|_| ProcessError::ProcessTreeCleanupUnconfirmed)?;
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
    }
    child.mark_cleanup_confirmed();
    Ok(())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HelloFrame {
    protocol: String,
    v: u32,
    #[serde(rename = "type")]
    kind: String,
    max_frame_bytes: usize,
    capabilities: Vec<String>,
}

fn validate_hello(hello: &HelloFrame, config: &CodexSidecarConfig) -> Result<(), ProcessError> {
    let capabilities = capability_set(&hello.capabilities);
    if hello.protocol != CODEX_SIDECAR_PROTOCOL
        || hello.v != CODEX_SIDECAR_VERSION
        || hello.kind != "hello"
        || hello.max_frame_bytes != config.max_frame_bytes
        || capabilities.len() != hello.capabilities.len()
        || capabilities != required_capability_set()
    {
        return Err(ProcessError::SidecarProtocol);
    }
    Ok(())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfigureFrame<'a> {
    v: u32,
    #[serde(rename = "type")]
    kind: &'static str,
    id: &'a str,
    codex_binary: Option<&'a std::path::Path>,
    codex_home: Option<&'a PathBuf>,
    codex_arguments: &'a [String],
    max_frame_bytes: usize,
    max_pending: usize,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConfigureResponse {
    v: u32,
    #[serde(rename = "type")]
    kind: String,
    id: String,
    ok: bool,
    #[serde(default)]
    data: Option<ConfigureData>,
    #[serde(default)]
    error: Option<SidecarBootstrapFailure>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConfigureData {
    upstream_version: String,
    adapter_version: String,
    capabilities: Vec<String>,
}

fn validate_configure_response(
    response: ConfigureResponse,
    expected_id: &str,
) -> Result<Version, ProcessError> {
    if response.v != CODEX_SIDECAR_VERSION
        || response.kind != "response"
        || response.id != expected_id
    {
        return Err(ProcessError::SidecarProtocol);
    }
    let data = match (response.ok, response.data, response.error) {
        (true, Some(data), None) => data,
        (false, None, Some(failure)) => {
            return Err(ProcessError::SidecarBootstrapRejected { failure });
        }
        _ => return Err(ProcessError::SidecarProtocol),
    };
    let capabilities = capability_set(&data.capabilities);
    if data.adapter_version != data.upstream_version
        || capabilities.len() != data.capabilities.len()
        || capabilities != required_capability_set()
    {
        return Err(ProcessError::SidecarProtocol);
    }
    let version =
        Version::parse(&data.upstream_version).map_err(|_| ProcessError::SidecarProtocol)?;
    if !version.pre.is_empty()
        || !version.build.is_empty()
        || version.to_string() != data.upstream_version
    {
        return Err(ProcessError::SidecarProtocol);
    }
    if !SUPPORTED_SIDECAR_CODEX_VERSIONS.contains(&data.upstream_version.as_str()) {
        return Err(ProcessError::UnsupportedSidecarVersion { found: version });
    }
    Ok(version)
}

fn capability_set(values: &[String]) -> BTreeSet<&str> {
    values.iter().map(String::as_str).collect()
}

fn required_capability_set() -> BTreeSet<&'static str> {
    REQUIRED_SIDECAR_CAPABILITIES.iter().copied().collect()
}

async fn read_frame<T>(
    reader: &mut BufReader<ChildStdout>,
    maximum: usize,
) -> Result<T, ProcessError>
where
    T: for<'de> Deserialize<'de>,
{
    let mut bytes = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .await
            .map_err(|_| ProcessError::SidecarBootstrapIo)?;
        if available.is_empty() {
            return Err(ProcessError::SidecarBootstrapIo);
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(available.len(), |position| position + 1);
        let payload_bytes = consumed.saturating_sub(usize::from(newline.is_some()));
        if bytes.len().saturating_add(payload_bytes) > maximum {
            return Err(ProcessError::SidecarProtocol);
        }
        bytes.extend_from_slice(&available[..consumed]);
        reader.consume(consumed);
        if newline.is_some() {
            break;
        }
    }
    bytes.pop();
    if bytes.last() == Some(&b'\r') {
        bytes.pop();
    }
    serde_json::from_slice(&bytes).map_err(|_| ProcessError::SidecarProtocol)
}

async fn write_frame<T>(
    writer: &mut ChildStdin,
    value: &T,
    maximum: usize,
) -> Result<(), ProcessError>
where
    T: Serialize,
{
    let mut bytes = serde_json::to_vec(value).map_err(|_| ProcessError::SidecarProtocol)?;
    if bytes.len() > maximum {
        return Err(ProcessError::SidecarProtocol);
    }
    bytes.push(b'\n');
    writer
        .write_all(&bytes)
        .await
        .map_err(|_| ProcessError::SidecarBootstrapIo)?;
    writer
        .flush()
        .await
        .map_err(|_| ProcessError::SidecarBootstrapIo)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;

    #[derive(Debug)]
    struct CountingChild {
        inner: Box<dyn TokioChildWrapper>,
        start_kill_calls: Arc<AtomicUsize>,
    }

    impl TokioChildWrapper for CountingChild {
        fn inner(&self) -> &tokio::process::Child {
            self.inner.inner()
        }

        fn inner_mut(&mut self) -> &mut tokio::process::Child {
            self.inner.inner_mut()
        }

        fn into_inner(self: Box<Self>) -> tokio::process::Child {
            self.inner.into_inner()
        }

        fn start_kill(&mut self) -> std::io::Result<()> {
            self.start_kill_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.start_kill()
        }

        fn wait(
            &mut self,
        ) -> Box<
            dyn std::future::Future<Output = std::io::Result<std::process::ExitStatus>> + Send + '_,
        > {
            self.inner.wait()
        }
    }

    #[tokio::test]
    async fn confirmed_bootstrap_cleanup_disarms_the_drop_kill() {
        let executable = std::env::current_exe().expect("current test executable");
        let mut command = Command::new(executable);
        command
            .arg("--help")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut command = TokioCommandWrap::from(command);
        #[cfg(windows)]
        command.wrap(KillOnDrop);
        #[cfg(unix)]
        command.wrap(ProcessGroup::leader());
        #[cfg(windows)]
        command.wrap(JobObject);
        let child = command.spawn().expect("spawn cleanup fixture");
        let pid = child.inner().id().expect("fixture exposes its PID");
        let start_kill_calls = Arc::new(AtomicUsize::new(0));
        let child = CountingChild {
            inner: child,
            start_kill_calls: Arc::clone(&start_kill_calls),
        };
        let mut guard = BootstrapProcessGuard::new(Box::new(child));

        cleanup_bootstrap_process(&mut guard, Some(pid), Duration::from_millis(100))
            .await
            .expect("cleanup proves the process tree is empty");
        drop(guard);

        assert_eq!(
            start_kill_calls.load(Ordering::SeqCst),
            1,
            "Drop must not signal a process group after its absence was proved"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn reaped_leader_is_never_targeted_by_group_cleanup() {
        let executable = std::env::current_exe().expect("current test executable");
        let mut command = Command::new(executable);
        command
            .arg("--help")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut command = TokioCommandWrap::from(command);
        command.wrap(ProcessGroup::leader());
        let mut child = command.spawn().expect("spawn cleanup fixture");
        let pid = child.inner().id().expect("fixture exposes its PID");
        let status = child
            .inner_mut()
            .wait()
            .await
            .expect("reap fixture leader first");
        assert!(
            child.inner().id().is_none(),
            "leader PID must be released before cleanup regression"
        );

        let start_kill_calls = Arc::new(AtomicUsize::new(0));
        let child = CountingChild {
            inner: child,
            start_kill_calls: Arc::clone(&start_kill_calls),
        };
        let mut child: Box<dyn TokioChildWrapper> = Box::new(child);
        let mut exit = Some(process_exit(pid, status));
        let mut tree_reaped = false;
        let mut group_signal_authorized = true;
        let mut identity_lost = false;
        let state = ProcessCleanupState {
            cached_exit: &mut exit,
            tree_reaped: &mut tree_reaped,
            group_signal_authorized: &mut group_signal_authorized,
            identity_lost: &mut identity_lost,
        };

        reap_owned_process_tree(
            &mut child,
            pid,
            Duration::from_millis(100),
            state,
            "fixture cleanup timed out",
        )
        .await
        .expect("read-only absence proof succeeds after leader reap");

        assert!(tree_reaped);
        assert!(!group_signal_authorized);
        assert!(!identity_lost);
        assert_eq!(
            start_kill_calls.load(Ordering::SeqCst),
            0,
            "a released leader PID must never be reused for a group kill"
        );
    }

    #[test]
    fn debug_redacts_every_configured_path_and_argument() {
        let config = CodexSidecarConfig {
            node_binary: PathBuf::from("secret-node"),
            entrypoint: PathBuf::from("secret-entrypoint"),
            codex_binary: Some(PathBuf::from("secret-codex")),
            codex_home: Some(PathBuf::from("secret-home")),
            codex_arguments: vec!["secret-argument".to_owned()],
            ..CodexSidecarConfig::default()
        };
        let debug = format!("{config:?}");
        for secret in [
            "secret-node",
            "secret-entrypoint",
            "secret-codex",
            "secret-home",
            "secret-argument",
        ] {
            assert!(!debug.contains(secret));
        }
    }

    #[test]
    fn inherited_environment_is_an_explicit_home_aware_allowlist() {
        let inherited = inherited_sidecar_environment();
        #[cfg(unix)]
        let allowed = ["HOME", "PATH"];
        #[cfg(windows)]
        let allowed = ["PATH", "PATHEXT", "SystemRoot", "USERPROFILE", "WINDIR"];
        #[cfg(not(any(unix, windows)))]
        let allowed = ["PATH"];
        assert!(
            inherited.iter().all(|(name, _)| allowed.contains(name)),
            "the sidecar environment must remain a fixed allowlist"
        );
        #[cfg(unix)]
        assert_eq!(
            inherited
                .iter()
                .find(|(name, _)| *name == "HOME")
                .map(|(_, value)| value),
            std::env::var_os("HOME").as_ref()
        );
        #[cfg(windows)]
        assert_eq!(
            inherited
                .iter()
                .find(|(name, _)| *name == "USERPROFILE")
                .map(|(_, value)| value),
            std::env::var_os("USERPROFILE").as_ref()
        );
    }

    #[tokio::test]
    async fn spawn_error_redacts_the_configured_node_path_and_os_text() {
        let marker = format!("bridge-sensitive-node-path-{}", std::process::id());
        let node_binary = std::env::temp_dir().join(&marker).join("missing-node");
        let config = CodexSidecarConfig {
            node_binary,
            ..CodexSidecarConfig::default()
        };
        let Err(error) = spawn_codex_sidecar(&config).await else {
            panic!("the deliberately missing Node binary cannot start");
        };
        assert!(matches!(
            &error,
            ProcessError::SidecarSpawn {
                failure: SidecarSpawnFailure::Failed
            }
        ));
        for rendered in [format!("{error}"), format!("{error:?}")] {
            assert!(!rendered.contains(&marker));
            assert!(!rendered.contains("missing-node"));
        }
    }

    #[test]
    fn hello_requires_the_exact_negotiated_capability_set() {
        let config = CodexSidecarConfig::default();
        let valid = HelloFrame {
            protocol: CODEX_SIDECAR_PROTOCOL.to_owned(),
            v: CODEX_SIDECAR_VERSION,
            kind: "hello".to_owned(),
            max_frame_bytes: config.max_frame_bytes,
            capabilities: REQUIRED_SIDECAR_CAPABILITIES
                .iter()
                .map(ToString::to_string)
                .collect(),
        };
        assert!(validate_hello(&valid, &config).is_ok());
        let mut unknown = valid;
        unknown
            .capabilities
            .push("future-unsafe-capability".to_owned());
        assert!(matches!(
            validate_hello(&unknown, &config),
            Err(ProcessError::SidecarProtocol)
        ));
    }

    #[test]
    fn pending_bound_covers_both_rpc_directions() {
        assert_eq!(
            CODEX_SIDECAR_PENDING_CAPACITY,
            crate::limits::RPC_TOTAL_PENDING_CAPACITY + crate::limits::RPC_SERVER_REQUEST_CAPACITY
        );
    }

    #[test]
    fn wrapper_arguments_must_be_nonempty() {
        let config = CodexSidecarConfig {
            codex_arguments: vec![String::new()],
            ..CodexSidecarConfig::default()
        };
        assert!(matches!(
            config.validate(),
            Err(ProcessError::InvalidSidecarConfig)
        ));
    }

    #[test]
    fn configure_failures_are_closed_typed_classifications() {
        let retryable = ConfigureResponse {
            v: CODEX_SIDECAR_VERSION,
            kind: "response".to_owned(),
            id: "configure-test".to_owned(),
            ok: false,
            data: None,
            error: Some(SidecarBootstrapFailure::VersionProbeTimeout),
        };
        assert!(matches!(
            validate_configure_response(retryable, "configure-test"),
            Err(ProcessError::SidecarBootstrapRejected {
                failure: SidecarBootstrapFailure::VersionProbeTimeout
            })
        ));

        let unknown = serde_json::from_value::<ConfigureResponse>(serde_json::json!({
            "v": CODEX_SIDECAR_VERSION,
            "type": "response",
            "id": "configure-test",
            "ok": false,
            "error": "future_unreviewed_failure"
        }));
        assert!(unknown.is_err(), "unknown classifications must fail closed");
    }
}
