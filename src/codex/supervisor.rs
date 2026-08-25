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
        process::{CodexProcess, CodexProcessConfig, ProcessError, ProcessExit, spawn_app_server},
        rpc::{ConnectionEpoch, RpcError, initialize_connection_with_dynamic_tools, spawn_rpc},
        transport::spawn_stream_transport,
        types::InitializeResult,
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

/// Object-safe stdio bundle transferred from a spawned app-server.
pub struct ProcessStdio {
    pub stdout: Box<dyn AsyncRead + Unpin + Send>,
    pub stdin: Box<dyn AsyncWrite + Unpin + Send>,
    pub stderr: Box<dyn AsyncRead + Unpin + Send>,
}

/// Object-safe view of one spawned app-server child owned by the supervisor.
pub trait AppServerProcess: Send {
    fn version(&self) -> &Version;
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

/// Sanitized initialize facts that are safe to print or log.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerInfo {
    pub user_agent: String,
    pub platform_family: String,
    pub platform_os: String,
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
        let task = tokio::spawn(run_supervisor(
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
            if initial.changed().await.is_err() {
                break;
            }
        }
        drop(state_tx);

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

/// Observation and shutdown handle for a running supervisor.
pub struct SupervisorHandle {
    state: watch::Receiver<SupervisorState>,
    client_slot: Arc<Mutex<Option<Arc<AppServerClient>>>>,
    shutdown: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl SupervisorHandle {
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
    /// Returns [`SupervisorError::TaskFailed`] if the supervisor task failed.
    pub async fn shutdown(mut self) -> Result<(), SupervisorError> {
        self.shutdown.cancel();
        match self.task.take() {
            Some(task) => task.await.map_err(|_| SupervisorError::TaskFailed),
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
}

struct EpochStartError {
    process: Box<dyn AppServerProcess>,
    permanent: Option<String>,
}

async fn run_supervisor(
    config: CodexProcessConfig,
    factory: Arc<dyn ProcessFactory>,
    settings: SupervisorSettings,
    state_tx: watch::Sender<SupervisorState>,
    client_slot: Arc<Mutex<Option<Arc<AppServerClient>>>>,
    shutdown: CancellationToken,
) {
    let mut epoch = 0_u64;
    let mut attempt = 0_u32;
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
                *client_slot.lock().unwrap_or_else(PoisonError::into_inner) =
                    Some(running.client.clone());
                publish(
                    &state_tx,
                    SupervisorState::Ready {
                        epoch,
                        version: running.version.clone(),
                        peer: running.peer.clone(),
                    },
                );
                let graceful = tokio::select! {
                    biased;
                    () = shutdown.cancelled() => true,
                    result = running.process.wait() => {
                        if result.is_err() {
                            // The OS wait failed while the child may still be
                            // alive; kill it before a replacement can spawn.
                            let _ = running.process.terminate(Duration::ZERO).await;
                        }
                        false
                    }
                };
                *client_slot.lock().unwrap_or_else(PoisonError::into_inner) = None;
                // Fail the old epoch immediately: cancel client/transport so the
                // writer closes app-server stdin before termination. Bound the
                // wait so a stuck shutdown chain cannot postpone termination.
                let shutdown_bound = settings.shutdown_grace().saturating_mul(2);
                let _ = tokio::time::timeout(shutdown_bound, running.client.shutdown()).await;
                if graceful {
                    let _ = running.process.terminate(settings.shutdown_grace()).await;
                    break;
                }
                attempt = attempt.saturating_add(1);
            }
            Err(start_error) => {
                let EpochStartError {
                    mut process,
                    permanent,
                } = start_error;
                let _ = process.terminate(settings.shutdown_grace()).await;
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
}

fn publish(state_tx: &watch::Sender<SupervisorState>, state: SupervisorState) {
    match &state {
        SupervisorState::Starting { epoch } => {
            tracing::info!(epoch, "Codex supervisor epoch starting");
        }
        SupervisorState::Ready { epoch, version, .. } => {
            tracing::info!(epoch, version = %version, "Codex supervisor epoch ready");
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
    let initialize = match initialize_connection_with_dynamic_tools(&handle).await {
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
    let client = Arc::new(AppServerClient::spawn(connection));
    Ok(RunningEpoch {
        process,
        client,
        version,
        peer,
    })
}

/// Classifies spawn failures that retrying cannot fix (version/auth/config).
fn permanent_process_reason(error: &ProcessError) -> Option<String> {
    match error {
        ProcessError::InvalidCodexHome { .. } => {
            Some("configured Codex home must be an existing directory".to_owned())
        }
        ProcessError::Spawn { .. } => Some("unable to run Codex binary".to_owned()),
        ProcessError::ProbeFailed { code } => Some(format!(
            "Codex version probe exited unsuccessfully (code: {code:?})"
        )),
        ProcessError::VersionOutputTooLong { maximum, .. } => Some(format!(
            "Codex version output exceeded the {maximum}-byte limit"
        )),
        ProcessError::InvalidVersionOutput => {
            Some("Codex version output must exactly match `codex-cli X.Y.Z`".to_owned())
        }
        ProcessError::UnsupportedVersion { found } => {
            Some(format!("Codex {found} is unsupported; expected >=0.146.0"))
        }
        ProcessError::ProbeTimeout(_)
        | ProcessError::ProbeIo { .. }
        | ProcessError::StdioAlreadyTaken
        | ProcessError::StdioUnavailable(_)
        | ProcessError::MissingProcessId
        | ProcessError::Wait(_)
        | ProcessError::Terminate(_) => None,
    }
}

/// Classifies handshake failures; a server rejection indicates permanent
/// authentication or configuration problems and must not be retried forever.
fn permanent_rpc_reason(error: &RpcError) -> Option<String> {
    match error {
        RpcError::Server { code, .. } => Some(format!(
            "app-server rejected the initialize handshake (server code {code}); \
             check Codex authentication with `codex login` and the local configuration"
        )),
        RpcError::Timeout { .. }
        | RpcError::ConnectionLost(_)
        | RpcError::AlreadyInitialized
        | RpcError::Serialize { .. }
        | RpcError::Deserialize { .. }
        | RpcError::PayloadTooLarge { .. }
        | RpcError::UnknownServerRequest
        | RpcError::RequestIdExhausted => None,
    }
}
