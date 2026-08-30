//! Sole owner of one Codex app-server child, its transport, RPC, and client epoch.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use semver::Version;
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::watch,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    codex::{
        client::AppServerClient,
        compat::WireAdapter,
        process::{
            CodexProcess, CodexProcessConfig, ProcessError, ProcessExit, SidecarBootstrapFailure,
            spawn_app_server,
        },
        rpc::{ConnectionEpoch, RpcError, initialize_connection_with_dynamic_tools, spawn_rpc},
        sidecar::{CodexSidecarConfig, spawn_codex_sidecar},
        transport::spawn_stream_transport,
        types::InitializeResult,
        wire::SUPPORTED_CODEX_VERSIONS,
    },
    limits::SUPERVISOR_SHUTDOWN_GRACE,
};

/// Exponential backoff bases before jitter: 0.5, 1, 2, 4, 8, 16, and 30 seconds.
const BASE_DELAYS: [Duration; 7] = [
    Duration::from_millis(500),
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
    Duration::from_secs(16),
    Duration::from_secs(30),
];
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);
const CLEANUP_FAILED_REASON: &str =
    "Codex process cleanup failed; replacement is fenced until bridge restart";

/// Object-safe stdio bundle transferred from a spawned app-server.
pub struct ProcessStdio {
    pub stdout: Box<dyn AsyncRead + Unpin + Send>,
    pub stdin: Box<dyn AsyncWrite + Unpin + Send>,
    pub stderr: Box<dyn AsyncRead + Unpin + Send>,
}

/// Object-safe view of one spawned app-server child owned by the supervisor.
pub trait AppServerProcess: Send {
    fn version(&self) -> &Version;
    /// Selects the local codec independently from the reported upstream
    /// version. Native processes use their exact generated adapter; a protocol
    /// sidecar overrides this with the stable bridge-domain codec.
    fn wire_adapter(&self) -> Option<WireAdapter> {
        WireAdapter::for_version(self.version())
    }
    /// Sanitized local transport identity exposed to operators.
    fn protocol_info(&self) -> ProtocolInfo {
        ProtocolInfo::NativeStdio
    }
    /// Transfers exclusive ownership of all three app-server pipes, once.
    ///
    /// # Errors
    ///
    /// Returns a [`ProcessError`] when the pipes are unavailable or were taken.
    fn take_stdio(&mut self) -> Result<ProcessStdio, ProcessError>;
    /// Waits for the child to exit, reaping it exactly once.
    fn wait(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<ProcessExit, ProcessError>> + Send + '_>>;
    /// Closes process-owned stdin, waits for `grace`, then force-kills and waits.
    fn terminate(
        &mut self,
        grace: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<ProcessExit, ProcessError>> + Send + '_>>;
}

impl AppServerProcess for CodexProcess {
    fn version(&self) -> &Version {
        CodexProcess::version(self)
    }

    fn take_stdio(&mut self) -> Result<ProcessStdio, ProcessError> {
        let (stdout, stdin, stderr) = CodexProcess::take_stdio(self)?;
        Ok(ProcessStdio {
            stdout: Box::new(stdout),
            stdin: Box::new(stdin),
            stderr: Box::new(stderr),
        })
    }

    fn wait(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<ProcessExit, ProcessError>> + Send + '_>> {
        Box::pin(CodexProcess::wait(self))
    }

    fn terminate(
        &mut self,
        grace: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<ProcessExit, ProcessError>> + Send + '_>> {
        Box::pin(CodexProcess::terminate(self, grace))
    }
}

/// Boxed spawn future produced by a [`ProcessFactory`].
pub type SpawnFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Box<dyn AppServerProcess>, ProcessError>> + Send + 'a>>;

/// Spawns supervised app-server children; faked in tests for determinism.
pub trait ProcessFactory: Send + Sync {
    fn spawn<'a>(&'a self, config: &'a CodexProcessConfig) -> SpawnFuture<'a>;
}

struct CodexProcessFactory;

impl ProcessFactory for CodexProcessFactory {
    fn spawn<'a>(&'a self, config: &'a CodexProcessConfig) -> SpawnFuture<'a> {
        Box::pin(async move {
            let process = spawn_app_server(config).await?;
            Ok(Box::new(process) as Box<dyn AppServerProcess>)
        })
    }
}

struct CodexSidecarProcessFactory {
    config: CodexSidecarConfig,
}

impl ProcessFactory for CodexSidecarProcessFactory {
    fn spawn<'a>(&'a self, _config: &'a CodexProcessConfig) -> SpawnFuture<'a> {
        Box::pin(async move {
            let process = spawn_codex_sidecar(&self.config).await?;
            Ok(Box::new(process) as Box<dyn AppServerProcess>)
        })
    }
}

/// Sanitized initialize facts that are safe to print or log.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerInfo {
    pub user_agent: String,
    pub platform_family: String,
    pub platform_os: String,
}

/// Sanitized protocol/backend facts for one ready epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProtocolInfo {
    NativeStdio,
    SidecarV1 {
        protocol: &'static str,
        version: u32,
        capabilities: Vec<&'static str>,
    },
}

impl ProtocolInfo {
    #[must_use]
    pub const fn backend_label(&self) -> &'static str {
        match self {
            Self::NativeStdio => "spawned_stdio",
            Self::SidecarV1 { .. } => "protocol_sidecar",
        }
    }

    #[must_use]
    pub const fn wire_label(&self) -> &'static str {
        match self {
            Self::NativeStdio => "codex-app-server",
            Self::SidecarV1 { protocol, .. } => protocol,
        }
    }
}

impl From<&InitializeResult> for PeerInfo {
    fn from(result: &InitializeResult) -> Self {
        Self {
            user_agent: result.user_agent.clone(),
            platform_family: result.platform_family.clone(),
            platform_os: result.platform_os.clone(),
        }
    }
}

/// Observable lifecycle state of the supervised app-server.
#[derive(Clone, Debug)]
pub enum SupervisorState {
    Starting {
        epoch: u64,
    },
    Ready {
        epoch: u64,
        version: Version,
        peer: PeerInfo,
        protocol: ProtocolInfo,
    },
    Backoff {
        epoch: u64,
        attempt: u32,
        delay: Duration,
    },
    Degraded {
        reason: String,
    },
    Stopped,
}

/// Safe supervisor failures.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SupervisorError {
    #[error("no app-server epoch is currently ready")]
    NotReady,
    #[error("the supervisor has stopped")]
    Stopped,
    #[error("the supervisor task failed")]
    TaskFailed,
    #[error("Codex process cleanup could not be confirmed")]
    CleanupFailed,
}

type RetryDelay = Arc<dyn Fn(u64, u32) -> Duration + Send + Sync>;

/// Restart and shutdown tuning for one supervisor.
#[derive(Clone)]
pub struct SupervisorSettings {
    shutdown_grace: Duration,
    retry_delay: RetryDelay,
}

impl SupervisorSettings {
    #[must_use]
    pub fn new(
        shutdown_grace: Duration,
        retry_delay: impl Fn(u64, u32) -> Duration + Send + Sync + 'static,
    ) -> Self {
        Self {
            shutdown_grace,
            retry_delay: Arc::new(retry_delay),
        }
    }

    #[must_use]
    pub const fn shutdown_grace(&self) -> Duration {
        self.shutdown_grace
    }

    fn retry_delay(&self, epoch: u64, attempt: u32) -> Duration {
        (self.retry_delay)(epoch, attempt)
    }
}

impl Default for SupervisorSettings {
    fn default() -> Self {
        Self::new(SUPERVISOR_SHUTDOWN_GRACE, AppServerSupervisor::retry_delay)
    }
}

impl fmt::Debug for SupervisorSettings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SupervisorSettings")
            .field("shutdown_grace", &self.shutdown_grace)
            .finish_non_exhaustive()
    }
}

/// Entry points for the app-server supervisor.
pub struct AppServerSupervisor;

impl AppServerSupervisor {
    /// Starts a supervisor for the real installed Codex binary.
    ///
    /// The returned handle is ready to observe once the first epoch attempt has
    /// concluded (ready, backoff, or degraded).
    ///
    /// # Errors
    ///
    /// Returns an error if the supervisor task cannot be started.
    pub async fn start(config: CodexProcessConfig) -> Result<SupervisorHandle, SupervisorError> {
        Self::start_with_factory(
            config,
            Arc::new(CodexProcessFactory),
            SupervisorSettings::default(),
        )
        .await
    }

    /// Starts a supervisor whose owned child is the stable Codex protocol
    /// sidecar. Backend selection is fixed for the supervisor lifetime; there
    /// is no live fallback to native stdio after readiness.
    ///
    /// # Errors
    ///
    /// Returns an error if the supervisor task cannot be started.
    pub async fn start_sidecar(
        config: CodexSidecarConfig,
    ) -> Result<SupervisorHandle, SupervisorError> {
        let factory = Arc::new(CodexSidecarProcessFactory { config });
        Self::start_with_factory(
            CodexProcessConfig::default(),
            factory,
            SupervisorSettings::default(),
        )
        .await
    }

    /// Starts a supervisor with an injected process factory and settings.
    ///
    /// # Errors
    ///
    /// Returns an error if the supervisor task cannot be started.
    pub async fn start_with_factory(
        config: CodexProcessConfig,
        factory: Arc<dyn ProcessFactory>,
        settings: SupervisorSettings,
    ) -> Result<SupervisorHandle, SupervisorError> {
        let (state_tx, mut state_rx) = watch::channel(SupervisorState::Starting { epoch: 0 });
        state_rx.borrow_and_update();
        let client_slot: Arc<Mutex<Option<Arc<AppServerClient>>>> = Arc::new(Mutex::new(None));
        let shutdown = CancellationToken::new();
        let mut startup_guard = SupervisorStartupGuard::new(shutdown.clone());
        let mut task = tokio::spawn(run_supervisor(
            config,
            factory,
            settings,
            state_tx.clone(),
            Arc::clone(&client_slot),
            shutdown.clone(),
        ));

        // Return only after the first epoch attempt concluded so callers never
        // observe the synthetic `Starting { epoch: 0 }` state.
        let mut initial = state_tx.subscribe();
        while matches!(*initial.borrow(), SupervisorState::Starting { .. }) {
            tokio::select! {
                biased;
                _ = &mut task => return Err(SupervisorError::TaskFailed),
                changed = initial.changed() => {
                    if changed.is_err() {
                        return Err(SupervisorError::TaskFailed);
                    }
                }
            }
        }
        drop(state_tx);
        startup_guard.disarm();

        Ok(SupervisorHandle {
            state: state_rx,
            client_slot,
            shutdown,
            task: Some(task),
        })
    }

    /// Deterministic jittered backoff: `base * [0.75, 1.25]`, capped at 30s.
    ///
    /// The jitter is a bijection of `seed` for a fixed `attempt`, so distinct
    /// seeds always pick distinct delays within the bounded window.
    #[must_use]
    pub fn retry_delay(seed: u64, attempt: u32) -> Duration {
        let index = usize::try_from(attempt.saturating_sub(1))
            .unwrap_or(usize::MAX)
            .min(BASE_DELAYS.len() - 1);
        let base_millis = BASE_DELAYS[index].as_millis();
        let mixed = splitmix64(u64::from(attempt).wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let spread = u128::from(mixed.wrapping_add(seed)) % (base_millis / 2 + 1);
        let jittered = base_millis * 3 / 4 + spread;
        let delay = Duration::from_millis(u64::try_from(jittered).unwrap_or(u64::MAX));
        delay.min(MAX_RETRY_DELAY)
    }
}

const fn splitmix64(value: u64) -> u64 {
    let mut z = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Cancels the detached supervisor task if its public startup future is
/// dropped before ownership can be transferred to a [`SupervisorHandle`].
/// The task remains alive just long enough to run its normal process-tree
/// termination and reaping path.
struct SupervisorStartupGuard {
    shutdown: CancellationToken,
    armed: bool,
}

impl SupervisorStartupGuard {
    fn new(shutdown: CancellationToken) -> Self {
        Self {
            shutdown,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SupervisorStartupGuard {
    fn drop(&mut self) {
        if self.armed {
            self.shutdown.cancel();
        }
    }
}

/// Observation and shutdown handle for a running supervisor.
pub struct SupervisorHandle {
    state: watch::Receiver<SupervisorState>,
    client_slot: Arc<Mutex<Option<Arc<AppServerClient>>>>,
    shutdown: CancellationToken,
    task: Option<JoinHandle<Result<(), SupervisorError>>>,
}

impl SupervisorHandle {
    #[cfg(test)]
    pub(crate) fn test_state_channel(
        initial: SupervisorState,
    ) -> (
        Self,
        watch::Sender<SupervisorState>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        let (state_tx, state) = watch::channel(initial);
        let (stopped_tx, stopped) = tokio::sync::oneshot::channel();
        let client_slot = Arc::new(Mutex::new(None));
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let task = tokio::spawn(async move {
            task_shutdown.cancelled().await;
            let _ = stopped_tx.send(());
            Ok(())
        });
        (
            Self {
                state,
                client_slot,
                shutdown,
                task: Some(task),
            },
            state_tx,
            stopped,
        )
    }

    /// Waits for the next state transition and returns the new state.
    ///
    /// # Errors
    ///
    /// Returns [`SupervisorError::Stopped`] once the supervisor has stopped.
    pub async fn changed(&mut self) -> Result<SupervisorState, SupervisorError> {
        self.state
            .changed()
            .await
            .map_err(|_| SupervisorError::Stopped)?;
        Ok(self.state.borrow_and_update().clone())
    }

    /// Returns the most recently published state without waiting.
    #[must_use]
    pub fn state(&self) -> SupervisorState {
        self.state.borrow().clone()
    }

    /// Clones the lifecycle observation stream for startup assembly barriers.
    pub(crate) fn subscribe_state(&self) -> watch::Receiver<SupervisorState> {
        self.state.clone()
    }

    /// Returns the client for the current ready epoch.
    ///
    /// # Errors
    ///
    /// Returns [`SupervisorError::NotReady`] while no epoch is ready.
    pub fn client(&self) -> Result<Arc<AppServerClient>, SupervisorError> {
        self.client_slot
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .ok_or(SupervisorError::NotReady)
    }

    /// Cancels the epoch, closes stdin, waits the grace period, then kills the
    /// child and waits for it so no orphan is left behind.
    ///
    /// # Errors
    ///
    /// Returns [`SupervisorError::TaskFailed`] if the supervisor task panicked
    /// or was aborted, and [`SupervisorError::CleanupFailed`] if owned process
    /// cleanup could not be confirmed.
    pub async fn shutdown(mut self) -> Result<(), SupervisorError> {
        self.shutdown.cancel();
        match self.task.take() {
            Some(task) => task.await.map_err(|_| SupervisorError::TaskFailed)?,
            None => Ok(()),
        }
    }
}

impl Drop for SupervisorHandle {
    fn drop(&mut self) {
        // Never leave a supervised child running without its owner; the
        // supervisor task observes the cancellation and terminates the child.
        self.shutdown.cancel();
    }
}

struct RunningEpoch {
    process: Box<dyn AppServerProcess>,
    client: Arc<AppServerClient>,
    version: Version,
    peer: PeerInfo,
    protocol: ProtocolInfo,
}

struct EpochStartError {
    process: Box<dyn AppServerProcess>,
    permanent: Option<String>,
}

fn activate_ready_epoch(
    state_tx: &watch::Sender<SupervisorState>,
    client_slot: &Mutex<Option<Arc<AppServerClient>>>,
    epoch: u64,
    running: &RunningEpoch,
) {
    *client_slot.lock().unwrap_or_else(PoisonError::into_inner) = Some(running.client.clone());
    publish(
        state_tx,
        SupervisorState::Ready {
            epoch,
            version: running.version.clone(),
            peer: running.peer.clone(),
            protocol: running.protocol.clone(),
        },
    );
}

async fn run_supervisor(
    config: CodexProcessConfig,
    factory: Arc<dyn ProcessFactory>,
    settings: SupervisorSettings,
    state_tx: watch::Sender<SupervisorState>,
    client_slot: Arc<Mutex<Option<Arc<AppServerClient>>>>,
    shutdown: CancellationToken,
) -> Result<(), SupervisorError> {
    let mut epoch = 0_u64;
    let mut attempt = 0_u32;
    let mut outcome = Ok(());
    loop {
        if attempt > 0 {
            let next_epoch = epoch.saturating_add(1);
            let delay = settings.retry_delay(next_epoch, attempt);
            publish(
                &state_tx,
                SupervisorState::Backoff {
                    epoch: next_epoch,
                    attempt,
                    delay,
                },
            );
            tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                () = tokio::time::sleep(delay) => {}
            }
        }
        if shutdown.is_cancelled() {
            break;
        }
        epoch = epoch.saturating_add(1);
        publish(&state_tx, SupervisorState::Starting { epoch });
        // Let observers catch the Starting state before a fast handshake
        // could replace it with Ready under the coalescing watch channel.
        tokio::task::yield_now().await;

        let spawned = tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            spawned = factory.spawn(&config) => spawned,
        };
        let process = match spawned {
            Ok(process) => process,
            Err(error) => {
                if let Some(reason) = permanent_process_reason(&error) {
                    publish(&state_tx, SupervisorState::Degraded { reason });
                    shutdown.cancelled().await;
                    break;
                }
                attempt = attempt.saturating_add(1);
                continue;
            }
        };

        match connect_epoch(process, epoch, &shutdown).await {
            Ok(mut running) => {
                attempt = 0;
                activate_ready_epoch(&state_tx, &client_slot, epoch, &running);
                let graceful = tokio::select! {
                    biased;
                    () = shutdown.cancelled() => true,
                    _ = running.process.wait() => false,
                };
                *client_slot.lock().unwrap_or_else(PoisonError::into_inner) = None;
                if let Err(error) = cleanup_running_epoch(
                    &mut running,
                    settings.shutdown_grace(),
                    &state_tx,
                    &shutdown,
                )
                .await
                {
                    outcome = Err(error);
                    break;
                }
                if graceful {
                    break;
                }
                attempt = attempt.saturating_add(1);
            }
            Err(start_error) => {
                let EpochStartError {
                    mut process,
                    permanent,
                } = start_error;
                if let Err(error) = cleanup_owned_process(
                    process.as_mut(),
                    settings.shutdown_grace(),
                    &state_tx,
                    &shutdown,
                )
                .await
                {
                    outcome = Err(error);
                    break;
                }
                if let Some(reason) = permanent {
                    publish(&state_tx, SupervisorState::Degraded { reason });
                    shutdown.cancelled().await;
                    break;
                }
                attempt = attempt.saturating_add(1);
            }
        }
    }
    *client_slot.lock().unwrap_or_else(PoisonError::into_inner) = None;
    publish(&state_tx, SupervisorState::Stopped);
    outcome
}

async fn cleanup_running_epoch(
    running: &mut RunningEpoch,
    grace: Duration,
    state_tx: &watch::Sender<SupervisorState>,
    shutdown: &CancellationToken,
) -> Result<(), SupervisorError> {
    // Fail the old epoch first so its writer closes app-server stdin. The
    // sidecar leader may exit before its upstream child, so process-tree
    // termination is still mandatory before a replacement can start.
    let shutdown_bound = grace.saturating_mul(2);
    let _ = tokio::time::timeout(shutdown_bound, running.client.shutdown()).await;
    cleanup_owned_process(running.process.as_mut(), grace, state_tx, shutdown).await
}

async fn cleanup_owned_process(
    process: &mut dyn AppServerProcess,
    grace: Duration,
    state_tx: &watch::Sender<SupervisorState>,
    shutdown: &CancellationToken,
) -> Result<(), SupervisorError> {
    if process.terminate(grace).await.is_ok() {
        return Ok(());
    }
    publish(
        state_tx,
        SupervisorState::Degraded {
            reason: CLEANUP_FAILED_REASON.to_owned(),
        },
    );
    shutdown.cancelled().await;
    Err(SupervisorError::CleanupFailed)
}

fn publish(state_tx: &watch::Sender<SupervisorState>, state: SupervisorState) {
    match &state {
        SupervisorState::Starting { epoch } => {
            tracing::info!(epoch, "Codex supervisor epoch starting");
        }
        SupervisorState::Ready {
            epoch,
            version,
            protocol,
            ..
        } => {
            tracing::info!(
                epoch,
                version = %version,
                backend = protocol.backend_label(),
                wire = protocol.wire_label(),
                "Codex supervisor epoch ready"
            );
        }
        SupervisorState::Backoff {
            epoch,
            attempt,
            delay,
        } => {
            tracing::warn!(
                epoch,
                attempt,
                delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                "Codex supervisor retry backoff"
            );
        }
        SupervisorState::Degraded { .. } => {
            // The operator-facing state may carry an actionable local error,
            // including a configured path. Terminal tracing keeps only the
            // static lifecycle classification.
            tracing::warn!("Codex supervisor degraded");
        }
        SupervisorState::Stopped => tracing::info!("Codex supervisor stopped"),
    }
    let _ = state_tx.send(state);
}

/// Builds the transport/RPC/client stack for one epoch over the child's stdio.
async fn connect_epoch(
    mut process: Box<dyn AppServerProcess>,
    epoch: u64,
    shutdown: &CancellationToken,
) -> Result<RunningEpoch, EpochStartError> {
    let version = process.version().clone();
    let protocol = process.protocol_info();
    let Some(wire) = process.wire_adapter() else {
        return Err(EpochStartError {
            process,
            permanent: Some(format!(
                "Codex {version} has no promoted wire compatibility adapter"
            )),
        });
    };
    let stdio = match process.take_stdio() {
        Ok(stdio) => stdio,
        Err(error) => {
            return Err(EpochStartError {
                process,
                permanent: permanent_process_reason(&error),
            });
        }
    };
    let transport =
        spawn_stream_transport(stdio.stdout, stdio.stdin, stdio.stderr, shutdown.clone());
    let mut connection = spawn_rpc(transport, ConnectionEpoch::new(epoch), shutdown.clone());
    let handle = connection.handle.clone();
    let initialize = match initialize_connection_with_dynamic_tools(&handle, wire).await {
        Ok(initialize) => initialize,
        Err(error) => {
            let _ = connection.shutdown().await;
            return Err(EpochStartError {
                process,
                permanent: permanent_rpc_reason(&error),
            });
        }
    };
    let peer = PeerInfo::from(&initialize);
    let client = Arc::new(AppServerClient::spawn(connection, wire));
    Ok(RunningEpoch {
        process,
        client,
        version,
        peer,
        protocol,
    })
}

/// Classifies spawn failures that retrying cannot fix (version/auth/config).
fn permanent_process_reason(error: &ProcessError) -> Option<String> {
    match error {
        ProcessError::InvalidCodexHome { .. } => {
            Some("configured Codex home must be an existing directory".to_owned())
        }
        ProcessError::Spawn { source, .. } => {
            (!spawn_failure_is_retryable(source)).then(|| "unable to run Codex binary".to_owned())
        }
        ProcessError::SidecarSpawn { failure } => {
            (!failure.is_retryable()).then(|| "unable to run Codex protocol sidecar".to_owned())
        }
        ProcessError::ProbeFailed { code } => Some(format!(
            "Codex version probe exited unsuccessfully (code: {code:?})"
        )),
        ProcessError::VersionOutputTooLong { maximum, .. } => Some(format!(
            "Codex version output exceeded the {maximum}-byte limit"
        )),
        ProcessError::InvalidVersionOutput => {
            Some("Codex version output must exactly match `codex-cli X.Y.Z`".to_owned())
        }
        ProcessError::UnsupportedVersion { found } => Some(format!(
            "Codex {found} is unsupported; expected an exact reviewed version ({})",
            SUPPORTED_CODEX_VERSIONS.join(", ")
        )),
        ProcessError::InvalidSidecarConfig => {
            Some("configured Codex protocol sidecar is invalid".to_owned())
        }
        ProcessError::SidecarBootstrapRejected { failure } => {
            permanent_sidecar_bootstrap_reason(*failure)
        }
        ProcessError::SidecarProtocol => {
            Some("Codex protocol sidecar negotiation failed closed".to_owned())
        }
        ProcessError::SidecarBootstrapCleanupFailed => Some(CLEANUP_FAILED_REASON.to_owned()),
        ProcessError::UnsupportedSidecarVersion { found } => {
            Some(format!("Codex {found} has no reviewed sidecar adapter"))
        }
        ProcessError::ProbeTimeout(_)
        | ProcessError::SidecarHandshakeTimeout(_)
        | ProcessError::SidecarBootstrapIo
        | ProcessError::ProbeIo { .. }
        | ProcessError::Wait(_)
        | ProcessError::Terminate(_) => None,
        ProcessError::StdioAlreadyTaken
        | ProcessError::StdioUnavailable(_)
        | ProcessError::MissingProcessId => {
            Some("Codex app-server process contract is unavailable".to_owned())
        }
    }
}

fn permanent_sidecar_bootstrap_reason(failure: SidecarBootstrapFailure) -> Option<String> {
    let reason = match failure {
        SidecarBootstrapFailure::InvalidConfiguration => {
            "Codex protocol sidecar rejected its bootstrap configuration"
        }
        SidecarBootstrapFailure::PinnedCodexMissing => {
            "the package-lock-pinned Codex sidecar artifact is unavailable"
        }
        SidecarBootstrapFailure::VersionProbeSpawnFailed => {
            "unable to run the configured Codex binary for sidecar version probing"
        }
        SidecarBootstrapFailure::VersionProbeFailed => {
            "the configured Codex binary failed its exact sidecar version probe"
        }
        SidecarBootstrapFailure::VersionOutputTooLarge => {
            "the configured Codex version output exceeded the sidecar bound"
        }
        SidecarBootstrapFailure::UnsupportedUpstreamVersion => {
            "the configured Codex version has no reviewed sidecar adapter"
        }
        SidecarBootstrapFailure::UpstreamSpawnFailed => {
            "unable to start the configured Codex app-server through the sidecar"
        }
        SidecarBootstrapFailure::SidecarFailed => "Codex protocol sidecar bootstrap failed closed",
        SidecarBootstrapFailure::VersionProbeSpawnUnavailable
        | SidecarBootstrapFailure::VersionProbeTimeout
        | SidecarBootstrapFailure::VersionProbeIo
        | SidecarBootstrapFailure::UpstreamSpawnUnavailable => return None,
    };
    Some(reason.to_owned())
}

fn spawn_failure_is_retryable(error: &std::io::Error) -> bool {
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

/// Classifies handshake failures; a server rejection indicates permanent
/// authentication or configuration problems and must not be retried forever.
fn permanent_rpc_reason(error: &RpcError) -> Option<String> {
    match error {
        RpcError::Server { code, .. } if matches!(*code, -32_700 | -32_600 | -32_601 | -32_602) => {
            Some(format!(
                "app-server rejected the initialize contract (server code {code}); \
                 check the exact Codex version and local configuration"
            ))
        }
        RpcError::Server { .. } | RpcError::Timeout { .. } | RpcError::ConnectionLost(_) => None,
        RpcError::AlreadyInitialized
        | RpcError::Serialize { .. }
        | RpcError::Deserialize { .. }
        | RpcError::PayloadTooLarge { .. }
        | RpcError::UnknownServerRequest
        | RpcError::RequestIdExhausted => {
            Some("app-server initialize violated the local RPC contract".to_owned())
        }
    }
}

#[cfg(test)]
mod classification_tests {
    use super::*;

    #[test]
    fn bootstrap_failure_classification_is_typed_and_fail_closed() {
        let retryable = ProcessError::SidecarBootstrapRejected {
            failure: SidecarBootstrapFailure::VersionProbeTimeout,
        };
        assert!(permanent_process_reason(&retryable).is_none());

        let permanent = ProcessError::SidecarBootstrapRejected {
            failure: SidecarBootstrapFailure::UnsupportedUpstreamVersion,
        };
        assert!(permanent_process_reason(&permanent).is_some());
    }

    #[test]
    fn initialize_classification_retries_only_uncertain_or_transient_failures() {
        assert!(
            permanent_rpc_reason(&RpcError::Server {
                method: "initialize",
                code: -32_001,
            })
            .is_none()
        );
        assert!(
            permanent_rpc_reason(&RpcError::Server {
                method: "initialize",
                code: -32_602,
            })
            .is_some()
        );
        assert!(
            permanent_rpc_reason(&RpcError::Deserialize {
                method: "initialize",
            })
            .is_some()
        );
    }

    #[cfg(unix)]
    #[test]
    fn unix_resource_pressure_spawn_failures_retry() {
        let error = std::io::Error::from_raw_os_error(libc::EMFILE);
        assert!(spawn_failure_is_retryable(&error));
        assert!(!spawn_failure_is_retryable(&std::io::Error::from(
            std::io::ErrorKind::NotFound
        )));
    }
}
