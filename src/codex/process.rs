use std::{fmt, path::PathBuf, process::Stdio, time::Duration};

use semver::Version;
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command},
    time::timeout,
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
    #[error("Codex protocol sidecar bootstrap process cleanup could not be confirmed")]
    SidecarBootstrapCleanupFailed,
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

pub struct CodexProcess {
    child: Child,
    version: Version,
    stdout: Option<ChildStdout>,
    stdin: Option<ChildStdin>,
    stderr: Option<ChildStderr>,
    exit: Option<ProcessExit>,
    pid: u32,
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
        let status = self.child.wait().await.map_err(ProcessError::Wait)?;
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
        if let Some(exit) = self.exit {
            return Ok(exit);
        }
        drop(self.stdin.take());

        if let Ok(result) = timeout(grace, self.wait()).await {
            result
        } else {
            if let Err(source) = self.child.start_kill() {
                return match self.child.try_wait().map_err(ProcessError::Wait)? {
                    Some(status) => Ok(self.cache_exit(status)),
                    None => Err(ProcessError::Terminate(source)),
                };
            }
            self.wait().await
        }
    }

    fn cache_exit(&mut self, status: std::process::ExitStatus) -> ProcessExit {
        let exit = process_exit(self.pid, status);
        self.exit = Some(exit);
        exit
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
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = command.spawn().map_err(|source| ProcessError::Spawn {
        binary: config.binary.clone(),
        source,
    })?;
    let stdout = child
        .stdout
        .take()
        .ok_or(ProcessError::StdioUnavailable("version stdout"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or(ProcessError::StdioUnavailable("version stderr"))?;

    let collected = timeout(VERSION_PROBE_TIMEOUT, async {
        tokio::join!(read_limited(stdout), read_limited(stderr), child.wait())
    })
    .await;

    let (stdout, stderr, status) = if let Ok((stdout, stderr, status)) = collected {
        (
            stdout.map_err(|source| ProcessError::ProbeIo {
                stream: "stdout",
                source,
            })?,
            stderr.map_err(|source| ProcessError::ProbeIo {
                stream: "stderr",
                source,
            })?,
            status.map_err(|source| ProcessError::ProbeIo {
                stream: "process status",
                source,
            })?,
        )
    } else {
        let _ = child.start_kill();
        let _ = child.wait().await;
        return Err(ProcessError::ProbeTimeout(VERSION_PROBE_TIMEOUT));
    };

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
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = command.spawn().map_err(|source| ProcessError::Spawn {
        binary: config.binary.clone(),
        source,
    })?;
    let pid = child.id().ok_or(ProcessError::MissingProcessId)?;
    let stdout = child.stdout.take();
    let stdin = child.stdin.take();
    let stderr = child.stderr.take();

    Ok(CodexProcess {
        child,
        version,
        stdout,
        stdin,
        stderr,
        exit: None,
        pid,
    })
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
