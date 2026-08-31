use std::{fmt, path::PathBuf, process::Stdio, time::Duration};

#[cfg(unix)]
use nix::{errno::Errno, sys::signal::killpg, unistd::Pid};
#[cfg(windows)]
use process_wrap::tokio::JobObject;
#[cfg(unix)]
use process_wrap::tokio::ProcessGroup;
use process_wrap::tokio::{KillOnDrop, TokioChildWrapper, TokioCommandWrap};
use semver::Version;
use thiserror::Error;
#[cfg(unix)]
use tokio::time::sleep_until;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{ChildStderr, ChildStdin, ChildStdout, Command},
    time::{Instant, timeout, timeout_at},
};

use crate::codex::wire::is_supported_codex_version;
use crate::limits::{MAX_VERSION_OUTPUT_BYTES, VERSION_PROBE_TIMEOUT};

/// Static, content-free bootstrap failures emitted by the protocol sidecar.
///
/// Keeping this as a closed enum prevents a sidecar from turning provider or
/// filesystem text into an operator-visible error. It also lets the supervisor
/// distinguish retryable resource failures from deterministic configuration or
/// compatibility failures without parsing prose.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SidecarBootstrapFailure {
    InvalidConfiguration,
    PinnedCodexMissing,
    VersionProbeSpawnFailed,
    VersionProbeSpawnUnavailable,
    VersionProbeTimeout,
    VersionProbeIo,
    VersionProbeFailed,
    VersionOutputTooLarge,
    UnsupportedUpstreamVersion,
    UpstreamSpawnFailed,
    UpstreamSpawnUnavailable,
    SidecarFailed,
}

impl SidecarBootstrapFailure {
    #[must_use]
    pub const fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::VersionProbeSpawnUnavailable
                | Self::VersionProbeTimeout
                | Self::VersionProbeIo
                | Self::UpstreamSpawnUnavailable
        )
    }
}

/// Content-free classification for failure to start the local protocol
/// sidecar itself. The originating path and operating-system error are
/// deliberately discarded at the process boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SidecarSpawnFailure {
    ResourceUnavailable,
    Failed,
}

impl SidecarSpawnFailure {
    pub(crate) fn classify(error: &std::io::Error) -> Self {
        if spawn_failure_is_retryable(error) {
            Self::ResourceUnavailable
        } else {
            Self::Failed
        }
    }

    #[must_use]
    pub const fn is_retryable(self) -> bool {
        matches!(self, Self::ResourceUnavailable)
    }
}

pub(crate) fn spawn_failure_is_retryable(error: &std::io::Error) -> bool {
    if matches!(
        error.kind(),
        std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::OutOfMemory
    ) {
        return true;
    }
    spawn_raw_os_error_is_retryable(error.raw_os_error())
}

#[cfg(unix)]
fn spawn_raw_os_error_is_retryable(code: Option<i32>) -> bool {
    matches!(
        code,
        Some(libc::EAGAIN | libc::EMFILE | libc::ENFILE | libc::ENOMEM)
    )
}

#[cfg(not(unix))]
const fn spawn_raw_os_error_is_retryable(_code: Option<i32>) -> bool {
    false
}

#[derive(Clone, Eq, PartialEq)]
pub struct CodexProcessConfig {
    pub binary: PathBuf,
    pub codex_home: Option<PathBuf>,
}

impl fmt::Debug for CodexProcessConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexProcessConfig")
            .field("binary", &self.binary)
            .field(
                "codex_home",
                &self.codex_home.as_ref().map(|_| "[configured]"),
            )
            .finish()
    }
}

impl Default for CodexProcessConfig {
    fn default() -> Self {
        Self {
            binary: PathBuf::from("codex"),
            codex_home: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum ProcessError {
    #[error("configured Codex home must be an existing directory")]
    InvalidCodexHome {
        #[source]
        source: Option<std::io::Error>,
    },
    #[error("unable to start Codex binary {binary}")]
    Spawn {
        binary: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("unable to start Codex protocol sidecar")]
    SidecarSpawn { failure: SidecarSpawnFailure },
    #[error("Codex version probe timed out after {0:?}")]
    ProbeTimeout(Duration),
    #[error("Codex version probe exited unsuccessfully (code: {code:?})")]
    ProbeFailed { code: Option<i32> },
    #[error("Codex version {stream} exceeded the {maximum}-byte limit")]
    VersionOutputTooLong {
        stream: &'static str,
        maximum: usize,
    },
    #[error("unable to read Codex version {stream}")]
    ProbeIo {
        stream: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("Codex version output must exactly match `codex-cli X.Y.Z`")]
    InvalidVersionOutput,
    #[error("Codex {found} is unsupported; expected an exact reviewed version")]
    UnsupportedVersion { found: Version },
    #[error("Codex protocol sidecar configuration is invalid")]
    InvalidSidecarConfig,
    #[error("Codex protocol sidecar handshake timed out after {0:?}")]
    SidecarHandshakeTimeout(Duration),
    #[error("Codex protocol sidecar bootstrap I/O failed")]
    SidecarBootstrapIo,
    #[error("Codex owned process-tree cleanup could not be confirmed")]
    ProcessTreeCleanupUnconfirmed,
    #[error("Codex protocol sidecar rejected bootstrap")]
    SidecarBootstrapRejected { failure: SidecarBootstrapFailure },
    #[error("Codex protocol sidecar violated its local wire contract")]
    SidecarProtocol,
    #[error("Codex {found} is unsupported by the configured protocol sidecar")]
    UnsupportedSidecarVersion { found: Version },
    #[error("Codex app-server stdio was already transferred")]
    StdioAlreadyTaken,
    #[error("Codex app-server did not expose piped {0}")]
    StdioUnavailable(&'static str),
    #[error("Codex app-server did not expose a process id")]
    MissingProcessId,
    #[error("unable to wait for Codex app-server")]
    Wait(#[source] std::io::Error),
    #[error("unable to terminate Codex app-server")]
    Terminate(#[source] std::io::Error),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessExit {
    pub pid: u32,
    pub success: bool,
    pub code: Option<i32>,
    pub signal: Option<i32>,
}

#[cfg(unix)]
const PROCESS_GROUP_POLL_INTERVAL: Duration = Duration::from_millis(10);
const VERSION_PROBE_CLEANUP_RESERVE: Duration = Duration::from_secs(1);

/// Waits until the owned POSIX process group no longer exists.
///
/// `process-wrap` waits for the leader, which is not sufficient evidence that
/// every descendant has left the group. A signal-0 probe is side-effect free:
/// only `ESRCH` proves absence. Success and `EPERM` both prove that the group
/// still exists, while every other OS error fails closed.
#[cfg(unix)]
pub(crate) async fn wait_for_owned_process_group_empty(
    leader_pid: u32,
    deadline: Instant,
) -> std::io::Result<()> {
    let raw_pid = i32::try_from(leader_pid).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "owned process-group id is outside the platform range",
        )
    })?;
    let process_group = Pid::from_raw(raw_pid);
    loop {
        match killpg(process_group, None) {
            Err(Errno::ESRCH) => return Ok(()),
            Ok(()) | Err(Errno::EPERM) => {}
            Err(error) => return Err(std::io::Error::from(error)),
        }

        let now = Instant::now();
        if now >= deadline {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "owned process group did not become empty within its bound",
            ));
        }
        sleep_until((now + PROCESS_GROUP_POLL_INTERVAL).min(deadline)).await;
    }
}

/// Force-terminates an owned process tree and records proof that it is empty.
///
/// Native and sidecar-backed app servers use the same release-authority
/// sequence. Callers retain their own graceful phase and error wording, but
/// the force-kill, bounded wrapper wait, POSIX absence proof, exit caching,
/// and `tree_reaped` transition must not drift apart.
pub(crate) async fn reap_owned_process_tree(
    child: &mut Box<dyn TokioChildWrapper>,
    leader_pid: u32,
    grace: Duration,
    cached_exit: &mut Option<ProcessExit>,
    tree_reaped: &mut bool,
    timeout_message: &'static str,
) -> Result<ProcessExit, ProcessError> {
    let cleanup_deadline = Instant::now() + grace.max(Duration::from_secs(1));
    let kill_error = child.start_kill().err();
    match timeout_at(cleanup_deadline, Box::into_pin(child.wait())).await {
        Ok(Ok(status)) => {
            let exit = cached_exit.unwrap_or_else(|| process_exit(leader_pid, status));
            *cached_exit = Some(exit);
            #[cfg(unix)]
            wait_for_owned_process_group_empty(leader_pid, cleanup_deadline)
                .await
                .map_err(ProcessError::Terminate)?;
            *tree_reaped = true;
            Ok(exit)
        }
        Ok(Err(source)) => Err(ProcessError::Wait(source)),
        Err(_) => {
            let _ = child.start_kill();
            Err(ProcessError::Terminate(kill_error.unwrap_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, timeout_message)
            })))
        }
    }
}

pub struct CodexProcess {
    child: Box<dyn TokioChildWrapper>,
    version: Version,
    stdout: Option<ChildStdout>,
    stdin: Option<ChildStdin>,
    stderr: Option<ChildStderr>,
    exit: Option<ProcessExit>,
    pid: u32,
    tree_reaped: bool,
}

impl CodexProcess {
    #[must_use]
    pub const fn version(&self) -> &Version {
        &self.version
    }

    #[must_use]
    pub const fn id(&self) -> u32 {
        self.pid
    }

    /// Transfers exclusive ownership of all three app-server pipes.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessError::StdioAlreadyTaken`] if the pipes were transferred,
    /// or [`ProcessError::StdioUnavailable`] for an incomplete pipe set.
    pub fn take_stdio(&mut self) -> Result<(ChildStdout, ChildStdin, ChildStderr), ProcessError> {
        if self.stdout.is_none() && self.stdin.is_none() && self.stderr.is_none() {
            return Err(ProcessError::StdioAlreadyTaken);
        }
        if self.stdout.is_none() || self.stdin.is_none() || self.stderr.is_none() {
            return Err(ProcessError::StdioUnavailable("incomplete stdio set"));
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
        Ok((stdout, stdin, stderr))
    }

    /// Waits for app-server to exit and caches its sanitized status.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessError::Wait`] if the operating system wait fails.
    pub async fn wait(&mut self) -> Result<ProcessExit, ProcessError> {
        if let Some(exit) = self.exit {
            return Ok(exit);
        }
        let status = self
            .child
            .inner_mut()
            .wait()
            .await
            .map_err(ProcessError::Wait)?;
        let exit = process_exit(self.pid, status);
        self.exit = Some(exit);
        Ok(exit)
    }

    /// Closes any process-owned stdin, waits for the grace period, then force-kills.
    ///
    /// If stdio was transferred to a transport, cancel or drop that transport first so
    /// its writer closes stdin during the graceful portion.
    ///
    /// # Errors
    ///
    /// Returns an error if waiting for or killing the process fails.
    pub async fn terminate(&mut self, grace: Duration) -> Result<ProcessExit, ProcessError> {
        if self.tree_reaped {
            return self.exit.ok_or_else(|| {
                ProcessError::Wait(std::io::Error::other(
                    "Codex process tree was reaped without a leader status",
                ))
            });
        }
        drop(self.stdin.take());
        drop(self.stdout.take());
        drop(self.stderr.take());

        if self.exit.is_none() {
            let _ = timeout(grace, self.wait()).await;
        }

        reap_owned_process_tree(
            &mut self.child,
            self.pid,
            grace,
            &mut self.exit,
            &mut self.tree_reaped,
            "Codex app-server process tree did not exit within its bound",
        )
        .await
    }
}

impl Drop for CodexProcess {
    fn drop(&mut self) {
        if !self.tree_reaped {
            let _ = self.child.start_kill();
        }
    }
}

/// Probes and validates the installed Codex CLI version without invoking a shell.
///
/// # Errors
///
/// Returns an actionable [`ProcessError`] for spawn, timeout, output, exit, or
/// compatibility failures.
pub async fn probe_version(config: &CodexProcessConfig) -> Result<Version, ProcessError> {
    let mut command = base_command(config)?;
    command
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = owned_command(command)
        .spawn()
        .map_err(|source| ProcessError::Spawn {
            binary: config.binary.clone(),
            source,
        })?;
    // Reserve the final second of the advertised probe bound for confirmed
    // process-tree cleanup. The output collection, passive group-empty check,
    // and forced cleanup all share `probe_deadline`; no failure path receives
    // a second full probe interval.
    let probe_deadline = Instant::now() + VERSION_PROBE_TIMEOUT;
    let collection_deadline = probe_deadline
        .checked_sub(VERSION_PROBE_CLEANUP_RESERVE.min(VERSION_PROBE_TIMEOUT))
        .unwrap_or(probe_deadline);
    let pid = child.inner().id();
    let (stdout, stderr) = {
        let inner = child.inner_mut();
        (inner.stdout.take(), inner.stderr.take())
    };
    let Some(stdout) = stdout else {
        cleanup_probe_process(&mut child, pid, probe_deadline).await?;
        return Err(ProcessError::StdioUnavailable("version stdout"));
    };
    let Some(stderr) = stderr else {
        cleanup_probe_process(&mut child, pid, probe_deadline).await?;
        return Err(ProcessError::StdioUnavailable("version stderr"));
    };

    let collected = timeout_at(collection_deadline, async {
        tokio::join!(
            read_limited(stdout),
            read_limited(stderr),
            Box::into_pin(child.wait())
        )
    })
    .await;

    let (stdout, stderr, status) = match collected {
        Ok((stdout, stderr, Ok(status))) => (stdout, stderr, status),
        Ok((_, _, Err(source))) => {
            cleanup_probe_process(&mut child, pid, probe_deadline).await?;
            return Err(ProcessError::ProbeIo {
                stream: "process status",
                source,
            });
        }
        Err(_) => {
            cleanup_probe_process(&mut child, pid, probe_deadline).await?;
            return Err(ProcessError::ProbeTimeout(VERSION_PROBE_TIMEOUT));
        }
    };

    #[cfg(unix)]
    {
        let Some(group_pid) = pid else {
            cleanup_probe_process(&mut child, pid, probe_deadline).await?;
            return Err(ProcessError::ProcessTreeCleanupUnconfirmed);
        };
        if wait_for_owned_process_group_empty(group_pid, collection_deadline)
            .await
            .is_err()
        {
            cleanup_probe_process(&mut child, pid, probe_deadline).await?;
            return Err(ProcessError::ProbeTimeout(VERSION_PROBE_TIMEOUT));
        }
    }

    let stdout = stdout.map_err(|source| ProcessError::ProbeIo {
        stream: "stdout",
        source,
    })?;
    let stderr = stderr.map_err(|source| ProcessError::ProbeIo {
        stream: "stderr",
        source,
    })?;

    if stdout.too_long {
        return Err(ProcessError::VersionOutputTooLong {
            stream: "stdout",
            maximum: MAX_VERSION_OUTPUT_BYTES,
        });
    }
    if stderr.too_long {
        return Err(ProcessError::VersionOutputTooLong {
            stream: "stderr",
            maximum: MAX_VERSION_OUTPUT_BYTES,
        });
    }
    if !status.success() {
        return Err(ProcessError::ProbeFailed {
            code: status.code(),
        });
    }

    let version = parse_version(&stdout.bytes)?;
    ensure_supported(version)
}

/// Starts a supported, long-lived Codex app-server with fully piped stdio.
///
/// # Errors
///
/// Returns an error if version validation or process startup fails.
pub async fn spawn_app_server(config: &CodexProcessConfig) -> Result<CodexProcess, ProcessError> {
    let version = probe_version(config).await?;
    let mut command = base_command(config)?;
    command
        .args(["app-server", "--listen", "stdio://"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut command = owned_command(command);
    let mut child = command.spawn().map_err(|source| ProcessError::Spawn {
        binary: config.binary.clone(),
        source,
    })?;
    let Some(pid) = child.inner_mut().id() else {
        let _ = child.start_kill();
        let _ = timeout(VERSION_PROBE_TIMEOUT, Box::into_pin(child.wait())).await;
        // Without the leader PID there is no process-group identity to probe.
        // A wrapper wait therefore cannot prove that the owned tree is empty.
        return Err(ProcessError::ProcessTreeCleanupUnconfirmed);
    };
    let (stdout, stdin, stderr) = {
        let inner = child.inner_mut();
        (inner.stdout.take(), inner.stdin.take(), inner.stderr.take())
    };

    Ok(CodexProcess {
        child,
        version,
        stdout,
        stdin,
        stderr,
        exit: None,
        pid,
        tree_reaped: false,
    })
}

fn owned_command(command: Command) -> TokioCommandWrap {
    let mut command = TokioCommandWrap::from(command);
    command.wrap(KillOnDrop);
    #[cfg(unix)]
    command.wrap(ProcessGroup::leader());
    #[cfg(windows)]
    command.wrap(JobObject);
    command
}

async fn cleanup_probe_process(
    child: &mut Box<dyn TokioChildWrapper>,
    pid: Option<u32>,
    cleanup_deadline: Instant,
) -> Result<(), ProcessError> {
    let _ = child.start_kill();
    if !matches!(
        timeout_at(cleanup_deadline, Box::into_pin(child.wait())).await,
        Ok(Ok(_))
    ) {
        let _ = child.start_kill();
        return Err(ProcessError::ProcessTreeCleanupUnconfirmed);
    }
    #[cfg(unix)]
    {
        let pid = pid.ok_or(ProcessError::ProcessTreeCleanupUnconfirmed)?;
        wait_for_owned_process_group_empty(pid, cleanup_deadline)
            .await
            .map_err(|_| ProcessError::ProcessTreeCleanupUnconfirmed)?;
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
    }
    Ok(())
}

fn base_command(config: &CodexProcessConfig) -> Result<Command, ProcessError> {
    let mut command = Command::new(&config.binary);
    if let Some(codex_home) = &config.codex_home {
        let metadata =
            std::fs::metadata(codex_home).map_err(|source| ProcessError::InvalidCodexHome {
                source: Some(source),
            })?;
        if !metadata.is_dir() {
            return Err(ProcessError::InvalidCodexHome { source: None });
        }
        command.env("CODEX_HOME", codex_home);
    }
    Ok(command)
}

struct LimitedOutput {
    bytes: Vec<u8>,
    too_long: bool,
}

async fn read_limited<R>(mut reader: R) -> std::io::Result<LimitedOutput>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = Vec::new();
    let mut too_long = false;
    let mut buffer = vec![0_u8; 8 * 1024];
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            return Ok(LimitedOutput { bytes, too_long });
        }
        let remaining = MAX_VERSION_OUTPUT_BYTES.saturating_sub(bytes.len());
        let retained = count.min(remaining);
        bytes.extend_from_slice(&buffer[..retained]);
        too_long |= retained < count;
    }
}

fn parse_version(bytes: &[u8]) -> Result<Version, ProcessError> {
    let output = std::str::from_utf8(bytes).map_err(|_| ProcessError::InvalidVersionOutput)?;
    let output = output
        .strip_suffix("\r\n")
        .or_else(|| output.strip_suffix('\n'))
        .unwrap_or(output);
    if output.contains(['\r', '\n']) {
        return Err(ProcessError::InvalidVersionOutput);
    }
    let version_text = output
        .strip_prefix("codex-cli ")
        .ok_or(ProcessError::InvalidVersionOutput)?;
    if version_text.is_empty() || version_text.contains(char::is_whitespace) {
        return Err(ProcessError::InvalidVersionOutput);
    }
    let version = Version::parse(version_text).map_err(|_| ProcessError::InvalidVersionOutput)?;
    if !version.pre.is_empty() || !version.build.is_empty() || version.to_string() != version_text {
        return Err(ProcessError::InvalidVersionOutput);
    }
    Ok(version)
}

fn ensure_supported(version: Version) -> Result<Version, ProcessError> {
    if is_supported_codex_version(&version) {
        Ok(version)
    } else {
        Err(ProcessError::UnsupportedVersion { found: version })
    }
}

pub(crate) fn process_exit(pid: u32, status: std::process::ExitStatus) -> ProcessExit {
    ProcessExit {
        pid,
        success: status.success(),
        code: status.code(),
        signal: exit_signal(status),
    }
}

#[cfg(unix)]
fn exit_signal(status: std::process::ExitStatus) -> Option<i32> {
    use std::os::unix::process::ExitStatusExt;
    status.signal()
}

#[cfg(not(unix))]
fn exit_signal(_status: std::process::ExitStatus) -> Option<i32> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_only_the_exact_codex_version_shape() {
        assert_eq!(
            parse_version(b"codex-cli 0.146.0\n").unwrap(),
            Version::new(0, 146, 0)
        );
        for invalid in [
            b"codex 0.146.0".as_slice(),
            b"codex-cli v0.146.0".as_slice(),
            b"codex-cli 0.146.0 extra".as_slice(),
            b"0.146.0".as_slice(),
            b" codex-cli 0.146.0".as_slice(),
            b"codex-cli  0.146.0".as_slice(),
            b"codex-cli\t0.146.0".as_slice(),
            b"codex-cli 0.146.0 \n".as_slice(),
            b"codex-cli 0.146.0-beta.1".as_slice(),
            b"codex-cli 0.146.0+build".as_slice(),
            b"codex-cli 0.146.0\nextra".as_slice(),
        ] {
            assert!(matches!(
                parse_version(invalid),
                Err(ProcessError::InvalidVersionOutput)
            ));
        }
    }

    #[test]
    fn enforces_the_exact_reviewed_schema_versions() {
        assert!(ensure_supported(Version::new(0, 146, 0)).is_ok());
        assert!(ensure_supported(Version::new(0, 149, 0)).is_ok());
        for version in [
            Version::new(0, 145, 9),
            Version::new(0, 147, 0),
            Version::new(0, 150, 0),
            Version::new(1, 0, 0),
        ] {
            assert!(matches!(
                ensure_supported(version),
                Err(ProcessError::UnsupportedVersion { .. })
            ));
        }
    }
}
