//! Exclusive process-ownership domains for explicitly adopted Codex threads.
//!
//! An externally persisted thread is never routed through the bridge's shared
//! app-server.  Acquisition launches one non-restarting, bridge-owned process
//! domain, proves that it uses the same Codex profile as the shared discovery
//! client, revalidates the exact target, and only then commits the durable
//! mapping.  Process-tree termination and confirmed reaping are the release
//! authority; local route or subscription removal is not.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use sha2::{Digest, Sha256};
use tokio::{sync::Notify, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    codex::{
        client::{AppServerClient, ClientError, ControlEvent},
        external::CodexBackendConfig,
        process::CodexProcessConfig,
        rpc::RpcError,
        sidecar::CodexSidecarConfig,
        supervisor::{
            AppServerSupervisor, OneShotSupervisorHandle, ProfileIdentity, SupervisorError,
        },
        types::{
            ApprovalPolicy, SandboxMode, Thread, ThreadListParams, ThreadListResult,
            ThreadReadParams, ThreadResumeOverrides, ThreadResumeParams,
        },
    },
    lark::normalize::ScopeKey,
    limits::THREAD_ADOPTION_SELECTOR_MAX_BYTES,
    runtime::{
        adoption::{
            CandidateValidationError, ThreadCandidatePage, ThreadDiscoveryError, discovery_params,
            project_candidate_page, thread_adoption_platform_supported,
            validate_candidate_for_resume,
        },
        policy::AccessPolicy,
    },
    store::{
        StoreError, StoreHandle, ThreadAdoptionOutcome, ThreadAdoptionReleaseResult,
        ThreadAdoptionSaga, ThreadAdoptionState, ThreadOrigin,
    },
};

const ADOPTED_SERVER_REQUEST_ERROR: i64 = -32_601;
const ADOPTED_SERVER_REQUEST_MESSAGE: &str =
    "server requests are unavailable on externally adopted threads";
/// Maximum age of the actor-local `/threads` selection proof accepted by an
/// ownership-changing operation.
pub const CANDIDATE_SELECTION_PROOF_TTL: Duration = Duration::from_secs(5 * 60);
const CANDIDATE_PROOF_SCOPE_DOMAIN: &[u8] = b"lark-codex-bridge/candidate-proof-scope/v1\0";

type DomainFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

trait CandidateDiscoverySource: Sync {
    fn supports_adoption_contract(&self) -> bool;
    fn list_candidate_page(
        &self,
        params: ThreadListParams,
    ) -> DomainFuture<'_, Result<ThreadListResult, ()>>;
}

impl CandidateDiscoverySource for AppServerClient {
    fn supports_adoption_contract(&self) -> bool {
        self.thread_adoption_contract().is_some()
    }

    fn list_candidate_page(
        &self,
        params: ThreadListParams,
    ) -> DomainFuture<'_, Result<ThreadListResult, ()>> {
        Box::pin(async move { self.list_threads(params).await.map_err(|_| ()) })
    }
}

/// Scope-bound, one-page evidence created by the most recent successful
/// `/threads` command. The proof is intentionally non-cloneable,
/// non-serializable, short-lived, and consumed by an ownership operation.
pub struct CandidateSelectionProof {
    scope_fingerprint: [u8; 32],
    cursor: Option<String>,
    selectors: Vec<String>,
    issued_at: Instant,
}

impl CandidateSelectionProof {
    fn new(scope: &ScopeKey, cursor: Option<String>, page: &ThreadCandidatePage) -> Self {
        Self {
            scope_fingerprint: candidate_proof_scope_fingerprint(scope),
            cursor,
            selectors: page
                .candidates
                .iter()
                .map(|candidate| candidate.selector.clone())
                .collect(),
            issued_at: Instant::now(),
        }
    }

    fn verify(
        &self,
        scope: &ScopeKey,
        selector: &str,
    ) -> Result<(), ThreadAdoptionCoordinatorError> {
        self.verify_scope_and_age(scope)?;
        if !self.selectors.iter().any(|candidate| candidate == selector) {
            return Err(ThreadAdoptionCoordinatorError::CandidateProofMismatch);
        }
        Ok(())
    }

    fn verify_scope_and_age(&self, scope: &ScopeKey) -> Result<(), ThreadAdoptionCoordinatorError> {
        if self.scope_fingerprint != candidate_proof_scope_fingerprint(scope) {
            return Err(ThreadAdoptionCoordinatorError::CandidateProofMismatch);
        }
        let Some(age) = Instant::now().checked_duration_since(self.issued_at) else {
            return Err(ThreadAdoptionCoordinatorError::CandidateProofExpired);
        };
        if age > CANDIDATE_SELECTION_PROOF_TTL {
            return Err(ThreadAdoptionCoordinatorError::CandidateProofExpired);
        }
        Ok(())
    }
}

impl fmt::Debug for CandidateSelectionProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CandidateSelectionProof")
            .field("candidate_count", &self.selectors.len())
            .field("cursor_bytes", &self.cursor.as_ref().map(String::len))
            .field("scope_bound", &true)
            .finish_non_exhaustive()
    }
}

/// One bounded candidate page and its one-shot scope-local selection proof.
pub struct ThreadDiscovery {
    pub page: ThreadCandidatePage,
    pub proof: CandidateSelectionProof,
}

impl fmt::Debug for ThreadDiscovery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ThreadDiscovery")
            .field("page", &self.page)
            .field("proof", &self.proof)
            .finish()
    }
}

/// Proof supplied only by the owner-only command path after the operator has
/// explicitly handed off all other writers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExplicitHandoff {
    Confirmed,
}

/// Reviewed settings that may be applied while acquiring an existing thread.
#[derive(Clone)]
pub struct AdoptionResumeSettings {
    pub sandbox: SandboxMode,
    pub approval_policy: ApprovalPolicy,
    pub model: Option<String>,
}

impl AdoptionResumeSettings {
    fn overrides(&self, cwd: std::path::PathBuf) -> ThreadResumeOverrides {
        ThreadResumeOverrides {
            exclude_turns: Some(true),
            cwd: Some(cwd),
            sandbox: Some(self.sandbox),
            approval_policy: Some(self.approval_policy.clone()),
            model: self.model.clone(),
            ..ThreadResumeOverrides::default()
        }
    }
}

impl fmt::Debug for AdoptionResumeSettings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdoptionResumeSettings")
            .field("sandbox", &self.sandbox)
            .field("model_configured", &self.model.is_some())
            .finish_non_exhaustive()
    }
}

/// Durable result of one successful acquisition.
#[derive(Clone, Eq, PartialEq)]
pub struct AdoptionReceipt {
    pub thread_id: String,
    pub generation: u64,
}

impl fmt::Debug for AdoptionReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdoptionReceipt")
            .field("thread_id_bytes", &self.thread_id.len())
            .field("generation", &self.generation)
            .finish()
    }
}

/// Successful terminal effect of an explicit release request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReleaseOutcome {
    /// A committed externally-adopted mapping was retired after confirmed reap.
    AdoptedMappingReleased,
    /// A pre-commit acquisition was reaped and terminalized without removing a mapping.
    UncommittedAcquisitionCleaned,
}

/// Durable result of one confirmed release or pre-commit cleanup.
#[derive(Clone, Eq, PartialEq)]
pub struct ReleaseReceipt {
    pub thread_id: String,
    pub generation: u64,
    pub outcome: ReleaseOutcome,
}

impl fmt::Debug for ReleaseReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReleaseReceipt")
            .field("thread_id_bytes", &self.thread_id.len())
            .field("generation", &self.generation)
            .field("outcome", &self.outcome)
            .finish()
    }
}

/// Exact route for an owned externally-adopted thread.
#[derive(Clone)]
pub struct DedicatedThreadRoute {
    pub client: Arc<AppServerClient>,
    pub thread_id: String,
    pub generation: u64,
}

impl fmt::Debug for DedicatedThreadRoute {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DedicatedThreadRoute")
            .field("thread_id_bytes", &self.thread_id.len())
            .field("generation", &self.generation)
            .field("epoch", &self.client.epoch())
            .finish()
    }
}

/// Aggregate shutdown result.  Mappings are deliberately left fenced rather
/// than inferred to be released during bridge shutdown.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AdoptionShutdownReport {
    pub fenced: usize,
    pub reaped: usize,
    pub failures: usize,
}

/// Static, content-free coordinator failures.
#[derive(Debug, thiserror::Error)]
pub enum ThreadAdoptionCoordinatorError {
    #[error("persisted-thread ownership requires a bridge-owned spawned backend")]
    UnsupportedBackend,
    #[error("the app-server wire does not support the reviewed adoption contract")]
    UnsupportedContract,
    #[error("persisted-thread ownership startup fencing has not completed")]
    StartupFenceRequired,
    #[error("persisted-thread ownership is shutting down")]
    ShuttingDown,
    #[error("the persisted-thread ownership-domain limit is reached")]
    Capacity,
    #[error("the scope already has an ownership operation in progress")]
    ScopeBusy,
    #[error("the persisted-thread selector is invalid")]
    InvalidSelector,
    #[error("rerun /threads before retrying this persisted-thread operation")]
    CandidateProofRequired,
    #[error("the /threads selection expired; rerun /threads before retrying")]
    CandidateProofExpired,
    #[error("the /threads selection does not match this scope and target; rerun /threads")]
    CandidateProofMismatch,
    #[error("the selected target is no longer in the fresh active page; rerun /threads")]
    CandidateRefreshRequired,
    #[error("the dedicated app-server did not use the shared Codex profile")]
    ProfileMismatch,
    #[error("the dedicated app-server was not ready")]
    DedicatedNotReady,
    #[error("the dedicated app-server control stream was unavailable")]
    ControlStreamUnavailable,
    #[error(
        "the selected target may have disappeared, been archived, or changed; rerun /threads before retrying"
    )]
    CandidateReadFailed,
    #[error("the selected persisted thread is missing or archived")]
    CandidateUnavailable,
    #[error("the selected persisted thread is not idle")]
    CandidateNotIdle,
    #[error("the selected persisted thread workspace is not allowed")]
    CandidateWorkspaceDenied,
    #[error("the selected persisted thread is already bound")]
    CandidateAlreadyBound,
    #[error("the selected persisted thread changed during acquisition")]
    CandidateChanged,
    #[error("the selected persisted thread has another active writer")]
    ActiveWriterConflict,
    #[error(
        "the selected target may have disappeared, been archived, or changed; rerun /threads before an explicit retry"
    )]
    ResumeFailed,
    #[error("the dedicated app-server process tree could not be confirmed reaped")]
    CleanupUnconfirmed,
    #[error("the scope does not own a local dedicated thread domain")]
    DomainMissing,
    #[error("the scope mapping is not an externally adopted thread")]
    NotExternallyAdopted,
    #[error("the externally adopted thread is fenced")]
    Fenced,
    #[error("the dedicated client route is unavailable")]
    DedicatedClientUnavailable,
    #[error("persisted-thread discovery failed")]
    Discovery(#[source] ThreadDiscoveryError),
    #[error("persisted-thread candidate validation failed")]
    Candidate(#[source] CandidateValidationError),
    #[error("persisted-thread durable state failed")]
    Store(#[source] StoreError),
}

impl ThreadAdoptionCoordinatorError {
    /// Stable path- and identifier-free classification for durable replies.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::UnsupportedBackend => "unsupported_backend",
            Self::UnsupportedContract => "unsupported_contract",
            Self::StartupFenceRequired => "startup_fence_required",
            Self::ShuttingDown => "shutting_down",
            Self::Capacity => "capacity",
            Self::ScopeBusy => "scope_busy",
            Self::InvalidSelector => "invalid_selector",
            Self::CandidateProofRequired => "candidate_proof_required",
            Self::CandidateProofExpired => "candidate_proof_expired",
            Self::CandidateProofMismatch => "candidate_proof_mismatch",
            Self::CandidateRefreshRequired => "candidate_refresh_required",
            Self::ProfileMismatch => "profile_mismatch",
            Self::DedicatedNotReady => "dedicated_not_ready",
            Self::ControlStreamUnavailable => "control_stream_unavailable",
            Self::CandidateReadFailed => "candidate_read_failed",
            Self::CandidateUnavailable => "candidate_unavailable",
            Self::CandidateNotIdle => "candidate_not_idle",
            Self::CandidateWorkspaceDenied => "candidate_workspace_denied",
            Self::CandidateAlreadyBound => "candidate_already_bound",
            Self::CandidateChanged => "candidate_changed",
            Self::ActiveWriterConflict => "active_writer_conflict",
            Self::ResumeFailed => "resume_failed",
            Self::CleanupUnconfirmed => "cleanup_unconfirmed",
            Self::DomainMissing => "domain_missing",
            Self::NotExternallyAdopted => "not_externally_adopted",
            Self::Fenced => "fenced",
            Self::DedicatedClientUnavailable => "dedicated_client_unavailable",
            Self::Discovery(_) => "discovery_failed",
            Self::Candidate(_) => "candidate_rejected",
            Self::Store(_) => "store_failed",
        }
    }
}

impl From<StoreError> for ThreadAdoptionCoordinatorError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<ThreadDiscoveryError> for ThreadAdoptionCoordinatorError {
    fn from(error: ThreadDiscoveryError) -> Self {
        Self::Discovery(error)
    }
}

impl From<CandidateValidationError> for ThreadAdoptionCoordinatorError {
    fn from(error: CandidateValidationError) -> Self {
        Self::Candidate(error)
    }
}

#[derive(Clone)]
enum LaunchBackend {
    Spawned(CodexProcessConfig),
    Sidecar(CodexSidecarConfig),
    Unsupported,
}

impl From<CodexBackendConfig> for LaunchBackend {
    fn from(config: CodexBackendConfig) -> Self {
        if let Some(config) = config.spawned_process_config() {
            Self::Spawned(config)
        } else if let Some(config) = config.protocol_sidecar_config() {
            Self::Sidecar(config)
        } else {
            Self::Unsupported
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DomainOperationError {
    NotReady,
    ActiveWriter,
    ReadFailed,
    ResumeFailed,
    CleanupUnconfirmed,
}

trait DomainOwner: Send {
    fn route_client(&self) -> Option<Arc<AppServerClient>>;
    fn supports_adoption_contract(&self) -> bool;
    fn profile_matches(&self, shared: &ProfileIdentity) -> Result<bool, DomainOperationError>;
    fn read_thread<'a>(
        &'a self,
        selector: &'a str,
    ) -> DomainFuture<'a, Result<Thread, DomainOperationError>>;
    fn resume_thread(
        &self,
        params: ThreadResumeParams,
    ) -> DomainFuture<'_, Result<Thread, DomainOperationError>>;
    fn shutdown(self: Box<Self>) -> DomainFuture<'static, Result<(), DomainOperationError>>;
}

trait DomainLauncher: Send + Sync {
    fn supported(&self) -> bool;
    fn launch(&self) -> DomainFuture<'static, Result<Box<dyn DomainOwner>, DomainOperationError>>;
}

struct ProductionLauncher {
    backend: LaunchBackend,
}

impl DomainLauncher for ProductionLauncher {
    fn supported(&self) -> bool {
        thread_adoption_platform_supported() && !matches!(self.backend, LaunchBackend::Unsupported)
    }

    fn launch(&self) -> DomainFuture<'static, Result<Box<dyn DomainOwner>, DomainOperationError>> {
        if !thread_adoption_platform_supported() {
            // Independent defense in depth: even a caller that bypasses the
            // public capability gate cannot create an issue #4 ownership
            // domain on Windows or another unproved platform. Ordinary bridge
            // supervisors do not use ProductionLauncher and remain available.
            return Box::pin(async { Err(DomainOperationError::NotReady) });
        }
        let backend = self.backend.clone();
        Box::pin(async move {
            let handle = match backend {
                LaunchBackend::Spawned(config) => AppServerSupervisor::start_once(config).await,
                LaunchBackend::Sidecar(config) => {
                    AppServerSupervisor::start_sidecar_once(config).await
                }
                LaunchBackend::Unsupported => return Err(DomainOperationError::NotReady),
            }
            .map_err(|_| DomainOperationError::NotReady)?;
            Ok(Box::new(ProductionOwner {
                handle: Some(handle),
            }) as Box<dyn DomainOwner>)
        })
    }
}

struct ProductionOwner {
    handle: Option<OneShotSupervisorHandle>,
}

impl ProductionOwner {
    fn client(&self) -> Result<Arc<AppServerClient>, DomainOperationError> {
        self.handle
            .as_ref()
            .ok_or(DomainOperationError::NotReady)?
            .client()
            .map_err(|_| DomainOperationError::NotReady)
    }
}

impl DomainOwner for ProductionOwner {
    fn route_client(&self) -> Option<Arc<AppServerClient>> {
        self.client().ok()
    }

    fn supports_adoption_contract(&self) -> bool {
        self.client()
            .ok()
            .and_then(|client| client.thread_adoption_contract())
            .is_some()
    }

    fn profile_matches(&self, shared: &ProfileIdentity) -> Result<bool, DomainOperationError> {
        let profile = self
            .handle
            .as_ref()
            .ok_or(DomainOperationError::NotReady)?
            .profile_identity()
            .map_err(|_| DomainOperationError::NotReady)?;
        Ok(profile == *shared)
    }

    fn read_thread<'a>(
        &'a self,
        selector: &'a str,
    ) -> DomainFuture<'a, Result<Thread, DomainOperationError>> {
        Box::pin(async move {
            self.client()?
                .read_thread(ThreadReadParams::new(selector))
                .await
                .map(|result| result.thread)
                .map_err(|_| DomainOperationError::ReadFailed)
        })
    }

    fn resume_thread(
        &self,
        params: ThreadResumeParams,
    ) -> DomainFuture<'_, Result<Thread, DomainOperationError>> {
        Box::pin(async move {
            self.client()?
                .resume_thread(params)
                .await
                .map_err(|error| match error {
                    ClientError::Rpc(RpcError::ThreadResumeActiveWriter) => {
                        DomainOperationError::ActiveWriter
                    }
                    _ => DomainOperationError::ResumeFailed,
                })
        })
    }

    fn shutdown(mut self: Box<Self>) -> DomainFuture<'static, Result<(), DomainOperationError>> {
        let handle = self.handle.take();
        Box::pin(async move {
            let Some(handle) = handle else {
                return Err(DomainOperationError::CleanupUnconfirmed);
            };
            handle.shutdown().await.map_err(|error| match error {
                SupervisorError::CleanupFailed
                | SupervisorError::TaskFailed
                | SupervisorError::NotReady
                | SupervisorError::Stopped => DomainOperationError::CleanupUnconfirmed,
            })
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CoordinatorLifecycle {
    New,
    Ready,
    ShuttingDown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OperationStage {
    Acquiring,
    Committing,
    Releasing,
    RecoveryRelease,
    FenceAndReap,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LocalDomainState {
    Owned,
    Releasing,
}

struct OwnedDomain {
    saga: ThreadAdoptionSaga,
    owner: Box<dyn DomainOwner>,
    client: Option<Arc<AppServerClient>>,
    drain: Option<ControlDrain>,
    healthy: Arc<AtomicBool>,
    intentional_transition: Arc<AtomicBool>,
    #[cfg(test)]
    reap_signal: Arc<ReapSignal>,
    monitor_cancel: Option<MonitorCancellation>,
    state: LocalDomainState,
}

struct ReapSignal {
    requested: AtomicBool,
    notify: Notify,
}

impl ReapSignal {
    fn new() -> Self {
        Self {
            requested: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    fn request(&self) {
        self.requested.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            if self.requested.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

struct MonitorCancellation(CancellationToken);

impl Drop for MonitorCancellation {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

struct CoordinatorState {
    lifecycle: CoordinatorLifecycle,
    operations: HashMap<String, OperationStage>,
    domains: HashMap<String, OwnedDomain>,
}

impl Default for CoordinatorState {
    fn default() -> Self {
        Self {
            lifecycle: CoordinatorLifecycle::New,
            operations: HashMap::new(),
            domains: HashMap::new(),
        }
    }
}

/// Coordinator for bounded, exact per-thread ownership domains.
#[derive(Clone)]
pub struct ThreadAdoptionCoordinator {
    store: StoreHandle,
    launcher: Arc<dyn DomainLauncher>,
    max_domains: usize,
    state: Arc<Mutex<CoordinatorState>>,
    operations_changed: Arc<Notify>,
    #[cfg(test)]
    allow_missing_test_client: bool,
}

impl fmt::Debug for ThreadAdoptionCoordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = lock(&self.state);
        formatter
            .debug_struct("ThreadAdoptionCoordinator")
            .field("lifecycle", &state.lifecycle)
            .field("domain_count", &state.domains.len())
            .field("operation_count", &state.operations.len())
            .field("max_domains", &self.max_domains)
            .finish_non_exhaustive()
    }
}

impl ThreadAdoptionCoordinator {
    /// Constructs a coordinator for a fixed backend.  Backend selection never
    /// falls back at runtime.
    #[must_use]
    pub fn new(store: StoreHandle, backend: CodexBackendConfig, max_domains: usize) -> Self {
        let launcher = ProductionLauncher {
            backend: backend.into(),
        };
        Self {
            store,
            launcher: Arc::new(launcher),
            max_domains,
            state: Arc::new(Mutex::new(CoordinatorState::default())),
            operations_changed: Arc::new(Notify::new()),
            #[cfg(test)]
            allow_missing_test_client: false,
        }
    }

    #[cfg(test)]
    fn with_test_launcher(
        store: StoreHandle,
        launcher: Arc<dyn DomainLauncher>,
        max_domains: usize,
    ) -> Self {
        Self {
            store,
            launcher,
            max_domains,
            state: Arc::new(Mutex::new(CoordinatorState::default())),
            operations_changed: Arc::new(Notify::new()),
            allow_missing_test_client: true,
        }
    }

    /// Fences every non-terminal generation before this process serves work.
    ///
    /// # Errors
    ///
    /// Returns a static coordinator or durable-store failure when startup
    /// cannot establish the recovery fence.
    pub async fn startup_fence(&self) -> Result<u64, ThreadAdoptionCoordinatorError> {
        {
            let state = lock(&self.state);
            match state.lifecycle {
                CoordinatorLifecycle::New => {}
                CoordinatorLifecycle::Ready => return Ok(0),
                CoordinatorLifecycle::ShuttingDown => {
                    return Err(ThreadAdoptionCoordinatorError::ShuttingDown);
                }
            }
        }
        let changed = self.store.fence_thread_adoptions_on_startup().await?;
        let mut state = lock(&self.state);
        if state.lifecycle == CoordinatorLifecycle::New {
            state.lifecycle = CoordinatorLifecycle::Ready;
            Ok(changed)
        } else {
            Err(ThreadAdoptionCoordinatorError::ShuttingDown)
        }
    }

    /// Lists bounded, policy-filtered candidates through the shared read-only
    /// client and returns an actor-local one-shot selection proof. The page
    /// metadata and proof do not prove writer ownership.
    ///
    /// # Errors
    ///
    /// Returns a static backend, lifecycle, RPC, projection, or store failure.
    pub async fn discover(
        &self,
        scope: &ScopeKey,
        shared_client: &AppServerClient,
        cursor: Option<String>,
        policy: &AccessPolicy,
    ) -> Result<ThreadDiscovery, ThreadAdoptionCoordinatorError> {
        self.discover_from_source(scope, shared_client, cursor, policy)
            .await
    }

    async fn discover_from_source<S: CandidateDiscoverySource + ?Sized>(
        &self,
        scope: &ScopeKey,
        source: &S,
        cursor: Option<String>,
        policy: &AccessPolicy,
    ) -> Result<ThreadDiscovery, ThreadAdoptionCoordinatorError> {
        self.require_ready_and_supported()?;
        if !source.supports_adoption_contract() {
            return Err(ThreadAdoptionCoordinatorError::UnsupportedContract);
        }
        let result = source
            .list_candidate_page(discovery_params(cursor.clone())?)
            .await
            .map_err(|()| ThreadAdoptionCoordinatorError::CandidateReadFailed)?;
        let page = self.project_active_page(result, policy, None).await?;
        let proof = CandidateSelectionProof::new(scope, cursor, &page);
        Ok(ThreadDiscovery { page, proof })
    }

    async fn refresh_selection<S: CandidateDiscoverySource + ?Sized>(
        &self,
        scope: &ScopeKey,
        selector: &str,
        source: &S,
        proof: CandidateSelectionProof,
        policy: &AccessPolicy,
        allow_exact_existing_binding: bool,
    ) -> Result<(), ThreadAdoptionCoordinatorError> {
        proof.verify(scope, selector)?;
        if !source.supports_adoption_contract() {
            return Err(ThreadAdoptionCoordinatorError::UnsupportedContract);
        }
        let result = source
            .list_candidate_page(discovery_params(proof.cursor)?)
            .await
            .map_err(|()| ThreadAdoptionCoordinatorError::CandidateReadFailed)?;
        let allowed_bound = allow_exact_existing_binding.then_some(selector);
        let page = self
            .project_active_page(result, policy, allowed_bound)
            .await?;
        if page
            .candidates
            .iter()
            .any(|candidate| candidate.selector == selector)
        {
            Ok(())
        } else {
            Err(ThreadAdoptionCoordinatorError::CandidateRefreshRequired)
        }
    }

    async fn refresh_recovery_selection<S: CandidateDiscoverySource + ?Sized>(
        &self,
        scope: &ScopeKey,
        selector: &str,
        source: &S,
        proof: Option<CandidateSelectionProof>,
        policy: &AccessPolicy,
    ) -> Result<(), ThreadAdoptionCoordinatorError> {
        let cursor = match proof {
            Some(proof) => {
                proof.verify_scope_and_age(scope)?;
                proof.cursor
            }
            None => None,
        };
        if !source.supports_adoption_contract() {
            return Err(ThreadAdoptionCoordinatorError::UnsupportedContract);
        }
        let result = source
            .list_candidate_page(discovery_params(cursor)?)
            .await
            .map_err(|()| ThreadAdoptionCoordinatorError::CandidateReadFailed)?;
        let page = self
            .project_active_page(result, policy, Some(selector))
            .await?;
        if page
            .candidates
            .iter()
            .any(|candidate| candidate.selector == selector)
        {
            Ok(())
        } else {
            Err(ThreadAdoptionCoordinatorError::CandidateRefreshRequired)
        }
    }

    async fn project_active_page(
        &self,
        result: ThreadListResult,
        policy: &AccessPolicy,
        allowed_bound: Option<&str>,
    ) -> Result<ThreadCandidatePage, ThreadAdoptionCoordinatorError> {
        let mut bound = HashSet::new();
        for thread in &result.data {
            if allowed_bound == Some(thread.id.as_str()) {
                continue;
            }
            if !self
                .store
                .thread_adoption_target_available(&thread.id)
                .await?
            {
                bound.insert(thread.id.clone());
            }
        }
        Ok(project_candidate_page(result, policy, &bound)?)
    }

    /// Acquires one exact persisted thread into its own non-restarting process
    /// domain and atomically commits the mapping only after authoritative
    /// `thread/resume` success.
    ///
    /// # Errors
    ///
    /// Returns a static validation, ownership, cleanup, capacity, or durable
    /// transition failure. Failed acquisitions never commit a new mapping.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub async fn adopt(
        &self,
        scope: &ScopeKey,
        selector: &str,
        shared_client: &AppServerClient,
        shared_profile: &ProfileIdentity,
        proof: Option<CandidateSelectionProof>,
        policy: &AccessPolicy,
        settings: AdoptionResumeSettings,
        handoff: ExplicitHandoff,
    ) -> Result<AdoptionReceipt, ThreadAdoptionCoordinatorError> {
        self.adopt_from_source(
            scope,
            selector,
            shared_client,
            shared_profile,
            proof,
            policy,
            settings,
            handoff,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn adopt_from_source<S: CandidateDiscoverySource + ?Sized>(
        &self,
        scope: &ScopeKey,
        selector: &str,
        source: &S,
        shared_profile: &ProfileIdentity,
        proof: Option<CandidateSelectionProof>,
        policy: &AccessPolicy,
        settings: AdoptionResumeSettings,
        handoff: ExplicitHandoff,
    ) -> Result<AdoptionReceipt, ThreadAdoptionCoordinatorError> {
        self.require_ready_and_supported()?;
        validate_selector(selector)?;
        if let Some(receipt) = self
            .idempotent_adoption(scope, selector, shared_profile)
            .await?
        {
            return Ok(receipt);
        }
        let proof = proof.ok_or(ThreadAdoptionCoordinatorError::CandidateProofRequired)?;
        self.refresh_selection(scope, selector, source, proof, policy, false)
            .await?;
        self.adopt_after_fresh_selection(scope, selector, shared_profile, policy, settings, handoff)
            .await
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn adopt_after_fresh_selection(
        &self,
        scope: &ScopeKey,
        selector: &str,
        shared_profile: &ProfileIdentity,
        policy: &AccessPolicy,
        settings: AdoptionResumeSettings,
        _handoff: ExplicitHandoff,
    ) -> Result<AdoptionReceipt, ThreadAdoptionCoordinatorError> {
        self.require_ready_and_supported()?;
        validate_selector(selector)?;
        if let Some(receipt) = self
            .idempotent_adoption(scope, selector, shared_profile)
            .await?
        {
            return Ok(receipt);
        }
        let mut operation = self.begin_acquisition(scope)?;
        let reservation = self.store.reserve_thread_adoption(scope, selector).await?;

        let Ok(owner) = self.launcher.launch().await else {
            let error = ThreadAdoptionCoordinatorError::DedicatedNotReady;
            return Err(self
                .finish_acquisition_failure(&reservation, None, None, error)
                .await);
        };

        if !owner.supports_adoption_contract() {
            return Err(self
                .finish_acquisition_failure(
                    &reservation,
                    Some(owner),
                    None,
                    ThreadAdoptionCoordinatorError::UnsupportedContract,
                )
                .await);
        }

        let profile_matches = owner.profile_matches(shared_profile);
        match profile_matches {
            Ok(true) => {}
            Ok(false) => {
                let error = ThreadAdoptionCoordinatorError::ProfileMismatch;
                return Err(self
                    .finish_acquisition_failure(&reservation, Some(owner), None, error)
                    .await);
            }
            Err(_) => {
                let error = ThreadAdoptionCoordinatorError::DedicatedNotReady;
                return Err(self
                    .finish_acquisition_failure(&reservation, Some(owner), None, error)
                    .await);
            }
        }

        let client = owner.route_client();
        #[cfg(not(test))]
        if client.is_none() {
            let error = ThreadAdoptionCoordinatorError::DedicatedClientUnavailable;
            return Err(self
                .finish_acquisition_failure(&reservation, Some(owner), None, error)
                .await);
        }
        #[cfg(test)]
        if client.is_none() && !self.allow_missing_test_client {
            let error = ThreadAdoptionCoordinatorError::DedicatedClientUnavailable;
            return Err(self
                .finish_acquisition_failure(&reservation, Some(owner), None, error)
                .await);
        }

        let healthy = Arc::new(AtomicBool::new(true));
        let intentional_transition = Arc::new(AtomicBool::new(false));
        let reap_signal = Arc::new(ReapSignal::new());
        let mut drain = match client.as_ref() {
            Some(client) => {
                let Ok(drain) = ControlDrain::start(
                    Arc::clone(client),
                    self.store.clone(),
                    reservation.clone(),
                    Arc::clone(&healthy),
                    Arc::clone(&intentional_transition),
                    Arc::clone(&reap_signal),
                ) else {
                    let error = ThreadAdoptionCoordinatorError::ControlStreamUnavailable;
                    return Err(self
                        .finish_acquisition_failure(&reservation, Some(owner), None, error)
                        .await);
                };
                Some(drain)
            }
            None => None,
        };

        let read = owner.read_thread(selector).await;
        let Ok(thread) = read else {
            let error = ThreadAdoptionCoordinatorError::CandidateReadFailed;
            return Err(self
                .finish_acquisition_failure(&reservation, Some(owner), drain.take(), error)
                .await);
        };
        let validated =
            match validate_candidate_for_resume(&thread, selector, policy, &HashSet::new()) {
                Ok(validated) => validated,
                Err(error) => {
                    return Err(self
                        .finish_acquisition_failure(
                            &reservation,
                            Some(owner),
                            drain.take(),
                            candidate_error(error),
                        )
                        .await);
                }
            };
        if !healthy.load(Ordering::Acquire) {
            let error = ThreadAdoptionCoordinatorError::Fenced;
            return Err(self
                .finish_acquisition_failure(&reservation, Some(owner), drain.take(), error)
                .await);
        }

        let mut params = ThreadResumeParams::new(&validated.thread_id);
        params.overrides = settings.overrides(validated.cwd.clone());
        let resumed = owner.resume_thread(params).await;
        let resumed = match resumed {
            Ok(thread) => thread,
            Err(DomainOperationError::ActiveWriter) => {
                let error = ThreadAdoptionCoordinatorError::ActiveWriterConflict;
                return Err(self
                    .finish_acquisition_failure(&reservation, Some(owner), drain.take(), error)
                    .await);
            }
            Err(_) => {
                let error = ThreadAdoptionCoordinatorError::ResumeFailed;
                return Err(self
                    .finish_acquisition_failure(&reservation, Some(owner), drain.take(), error)
                    .await);
            }
        };
        let resumed_cwd = policy.validate_workspace(&resumed.cwd).ok();
        if resumed.id != validated.thread_id
            || resumed_cwd.as_ref() != Some(&validated.cwd)
            || !healthy.load(Ordering::Acquire)
        {
            let error = ThreadAdoptionCoordinatorError::CandidateChanged;
            return Err(self
                .finish_acquisition_failure(&reservation, Some(owner), drain.take(), error)
                .await);
        }

        if let Err(error) = operation.mark_committing() {
            return Err(self
                .finish_acquisition_failure(&reservation, Some(owner), drain.take(), error)
                .await);
        }
        let Ok(fingerprint) = policy.fingerprint(&validated.cwd) else {
            let error = ThreadAdoptionCoordinatorError::CandidateChanged;
            return Err(self
                .finish_acquisition_failure(&reservation, Some(owner), drain.take(), error)
                .await);
        };
        if let Err(error) = self
            .store
            .commit_thread_adoption(&reservation, &validated.cwd, fingerprint.as_str())
            .await
        {
            return Err(self
                .finish_acquisition_failure(
                    &reservation,
                    Some(owner),
                    drain.take(),
                    ThreadAdoptionCoordinatorError::Store(error),
                )
                .await);
        }
        if !healthy.load(Ordering::Acquire) {
            if let Some(drain) = drain.take() {
                drain.stop().await;
            }
            let cleanup = owner.shutdown().await;
            let fence = self.store.fence_thread_adoption(&reservation).await;
            if cleanup.is_err() {
                return Err(ThreadAdoptionCoordinatorError::CleanupUnconfirmed);
            }
            fence?;
            return Err(ThreadAdoptionCoordinatorError::Fenced);
        }

        let receipt = AdoptionReceipt {
            thread_id: reservation.codex_thread_id.clone(),
            generation: reservation.generation,
        };
        let domain = OwnedDomain {
            saga: ThreadAdoptionSaga {
                state: ThreadAdoptionState::Owned,
                ..reservation
            },
            owner,
            client,
            drain,
            healthy,
            intentional_transition,
            #[cfg(test)]
            reap_signal: Arc::clone(&reap_signal),
            monitor_cancel: None,
            state: LocalDomainState::Owned,
        };
        operation.install_domain(domain);
        self.start_reap_monitor(scope, reap_signal);
        Ok(receipt)
    }

    /// Returns the exact dedicated client for an externally-adopted scope.
    /// There is deliberately no shared-client fallback.
    ///
    /// # Errors
    ///
    /// Returns a static failure when the durable mapping, saga generation,
    /// local domain, or connection health does not agree exactly.
    pub async fn route(
        &self,
        scope: &ScopeKey,
    ) -> Result<DedicatedThreadRoute, ThreadAdoptionCoordinatorError> {
        self.require_ready_and_supported()?;
        let mapping = self
            .store
            .active_thread(scope)
            .await?
            .ok_or(ThreadAdoptionCoordinatorError::NotExternallyAdopted)?;
        if mapping.origin != ThreadOrigin::ExternallyAdopted {
            return Err(ThreadAdoptionCoordinatorError::NotExternallyAdopted);
        }
        let generation = mapping
            .adoption_generation
            .ok_or(ThreadAdoptionCoordinatorError::Fenced)?;
        let saga = self
            .store
            .active_thread_adoption(scope)
            .await?
            .ok_or(ThreadAdoptionCoordinatorError::Fenced)?;
        if saga.state != ThreadAdoptionState::Owned
            || saga.generation != generation
            || saga.codex_thread_id != mapping.codex_thread_id
        {
            return Err(ThreadAdoptionCoordinatorError::Fenced);
        }
        let state = lock(&self.state);
        let domain = state
            .domains
            .get(&scope.to_string())
            .ok_or(ThreadAdoptionCoordinatorError::DomainMissing)?;
        if domain.state != LocalDomainState::Owned
            || domain.saga.generation != generation
            || domain.saga.codex_thread_id != mapping.codex_thread_id
            || !domain.healthy.load(Ordering::Acquire)
        {
            return Err(ThreadAdoptionCoordinatorError::Fenced);
        }
        let client = domain
            .client
            .clone()
            .ok_or(ThreadAdoptionCoordinatorError::DedicatedClientUnavailable)?;
        Ok(DedicatedThreadRoute {
            client,
            thread_id: mapping.codex_thread_id,
            generation,
        })
    }

    /// Fences a locally owned generation after any uncertain domain failure.
    ///
    /// # Errors
    ///
    /// Returns a static lifecycle or durable-store failure.
    pub async fn fence(&self, scope: &ScopeKey) -> Result<(), ThreadAdoptionCoordinatorError> {
        self.require_ready_and_supported()?;
        let scope_key = scope.to_string();
        {
            let state = lock(&self.state);
            if state.operations.contains_key(&scope_key) {
                return Err(ThreadAdoptionCoordinatorError::ScopeBusy);
            }
            let domain = state
                .domains
                .get(&scope_key)
                .ok_or(ThreadAdoptionCoordinatorError::DomainMissing)?;
            domain.healthy.store(false, Ordering::Release);
        }
        let saga = self
            .store
            .active_thread_adoption(scope)
            .await?
            .ok_or(ThreadAdoptionCoordinatorError::DomainMissing)?;
        self.store.fence_thread_adoption(&saga).await?;
        Ok(())
    }

    /// Durably fences an uncertain domain, then stops and reaps its process
    /// tree without retiring the mapping. Subsequent work must use explicit
    /// recovery and can never fall back to the shared app-server.
    ///
    /// # Errors
    ///
    /// Returns a static lifecycle, durable-fence, or cleanup failure. The
    /// mapping is never retired by this operation.
    pub async fn fence_and_reap(
        &self,
        scope: &ScopeKey,
    ) -> Result<(), ThreadAdoptionCoordinatorError> {
        self.require_ready_and_supported()?;
        let mut operation = self.begin_domain_stop(scope, OperationStage::FenceAndReap)?;
        let saga = operation.saga()?;
        let fence_result = self.store.fence_thread_adoption(&saga).await;
        let mut domain = operation.take_domain()?;
        domain.monitor_cancel.take();
        domain.intentional_transition.store(true, Ordering::Release);
        if let Some(drain) = domain.drain.take() {
            drain.stop().await;
        }
        let cleanup_result = domain.owner.shutdown().await;
        if cleanup_result.is_err() {
            return Err(ThreadAdoptionCoordinatorError::CleanupUnconfirmed);
        }
        fence_result?;
        Ok(())
    }

    /// Releases one domain only after the durable release fence, and removes
    /// the mapping only after confirmed process-tree reap.
    ///
    /// # Errors
    ///
    /// Returns a static lifecycle, durable-transition, or cleanup failure.
    /// Unconfirmed cleanup retains a fenced active mapping.
    pub async fn release(
        &self,
        scope: &ScopeKey,
    ) -> Result<ReleaseReceipt, ThreadAdoptionCoordinatorError> {
        self.require_ready_and_supported()?;
        if let Some(receipt) = self.idempotent_release(scope).await? {
            return Ok(receipt);
        }
        let mut operation = self.begin_release(scope)?;
        let saga = operation.saga()?;
        let releasing = match self.store.begin_thread_adoption_release(&saga).await {
            Ok(releasing) => releasing,
            Err(error) => {
                operation.restore_owned();
                return Err(ThreadAdoptionCoordinatorError::Store(error));
            }
        };
        let mut domain = operation.take_domain()?;
        domain.monitor_cancel.take();
        domain.intentional_transition.store(true, Ordering::Release);
        if let Some(drain) = domain.drain.take() {
            drain.stop().await;
        }
        let cleanup = domain.owner.shutdown().await;
        if cleanup.is_err() {
            self.finish_release_or_fence(&releasing, ThreadAdoptionReleaseResult::Failed)
                .await?;
            return Err(ThreadAdoptionCoordinatorError::CleanupUnconfirmed);
        }
        self.finish_release_or_fence(&releasing, ThreadAdoptionReleaseResult::Released)
            .await?;
        Ok(ReleaseReceipt {
            thread_id: releasing.codex_thread_id,
            generation: releasing.generation,
            outcome: ReleaseOutcome::AdoptedMappingReleased,
        })
    }

    /// Recovers a fenced mapping solely for explicit release. The durable saga
    /// supplies the exact selector, so no new user selection proof is required.
    /// When a proof is supplied, its fresh active page is an additional
    /// preflight. A new non-restarting owner then proves the same profile,
    /// revalidates and resumes the exact target, and is immediately reaped. An
    /// active-writer conflict or uncertain cleanup leaves the durable fence in
    /// place.
    ///
    /// # Errors
    ///
    /// Returns a static validation, conflict, backend, durable-transition, or
    /// cleanup failure. Failure never clears the pre-existing fence.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub async fn recover_release(
        &self,
        scope: &ScopeKey,
        shared_client: &AppServerClient,
        shared_profile: &ProfileIdentity,
        proof: Option<CandidateSelectionProof>,
        policy: &AccessPolicy,
        settings: AdoptionResumeSettings,
        handoff: ExplicitHandoff,
    ) -> Result<ReleaseReceipt, ThreadAdoptionCoordinatorError> {
        self.recover_release_from_source(
            scope,
            shared_client,
            shared_profile,
            proof,
            policy,
            settings,
            handoff,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn recover_release_from_source<S: CandidateDiscoverySource + ?Sized>(
        &self,
        scope: &ScopeKey,
        source: &S,
        shared_profile: &ProfileIdentity,
        proof: Option<CandidateSelectionProof>,
        policy: &AccessPolicy,
        settings: AdoptionResumeSettings,
        handoff: ExplicitHandoff,
    ) -> Result<ReleaseReceipt, ThreadAdoptionCoordinatorError> {
        self.require_ready_and_supported()?;
        if let Some(receipt) = self.idempotent_release(scope).await? {
            return Ok(receipt);
        }
        let saga = self
            .store
            .active_thread_adoption(scope)
            .await?
            .ok_or(ThreadAdoptionCoordinatorError::Fenced)?;
        if !source.supports_adoption_contract() {
            return Err(ThreadAdoptionCoordinatorError::UnsupportedContract);
        }
        if proof.is_some() {
            self.refresh_recovery_selection(scope, &saga.codex_thread_id, source, proof, policy)
                .await?;
        }
        self.recover_release_after_fresh_selection(scope, shared_profile, policy, settings, handoff)
            .await
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn recover_release_after_fresh_selection(
        &self,
        scope: &ScopeKey,
        shared_profile: &ProfileIdentity,
        policy: &AccessPolicy,
        settings: AdoptionResumeSettings,
        _handoff: ExplicitHandoff,
    ) -> Result<ReleaseReceipt, ThreadAdoptionCoordinatorError> {
        self.require_ready_and_supported()?;
        let _operation = self.begin_recovery_release(scope)?;
        let saga = self
            .store
            .active_thread_adoption(scope)
            .await?
            .ok_or(ThreadAdoptionCoordinatorError::Fenced)?;
        // A previous automatic reap may have outlived a transient store
        // failure. Reassert the durable fence before interpreting any
        // non-terminal exact generation for recovery.
        let saga = self.store.fence_thread_adoption(&saga).await?;
        let mapping = self.store.active_thread(scope).await?;
        let committed_mapping = match mapping {
            Some(mapping) => match mapping.origin {
                ThreadOrigin::ExternallyAdopted => {
                    if mapping.adoption_generation != Some(saga.generation)
                        || mapping.codex_thread_id != saga.codex_thread_id
                    {
                        return Err(ThreadAdoptionCoordinatorError::Fenced);
                    }
                    true
                }
                // A reservation deliberately preserves the prior ordinary
                // mapping until the dedicated owner is acquired and the
                // adoption commit succeeds atomically. Recovery of a crashed
                // pre-commit generation must reap its probe and terminalize
                // that generation without disturbing this mapping.
                ThreadOrigin::BridgeCreated => false,
            },
            None => false,
        };

        let owner = self
            .launcher
            .launch()
            .await
            .map_err(|_| ThreadAdoptionCoordinatorError::DedicatedNotReady)?;
        if !owner.supports_adoption_contract() {
            return Err(self
                .finish_recovery_probe(
                    owner,
                    None,
                    ThreadAdoptionCoordinatorError::UnsupportedContract,
                )
                .await);
        }
        let Ok(profile_matches) = owner.profile_matches(shared_profile) else {
            return Err(self
                .finish_recovery_probe(
                    owner,
                    None,
                    ThreadAdoptionCoordinatorError::DedicatedNotReady,
                )
                .await);
        };
        if !profile_matches {
            return Err(self
                .finish_recovery_probe(owner, None, ThreadAdoptionCoordinatorError::ProfileMismatch)
                .await);
        }
        let client = owner.route_client();
        #[cfg(not(test))]
        if client.is_none() {
            return Err(self
                .finish_recovery_probe(
                    owner,
                    None,
                    ThreadAdoptionCoordinatorError::DedicatedClientUnavailable,
                )
                .await);
        }
        #[cfg(test)]
        if client.is_none() && !self.allow_missing_test_client {
            return Err(self
                .finish_recovery_probe(
                    owner,
                    None,
                    ThreadAdoptionCoordinatorError::DedicatedClientUnavailable,
                )
                .await);
        }
        let healthy = Arc::new(AtomicBool::new(true));
        let intentional_transition = Arc::new(AtomicBool::new(false));
        let reap_signal = Arc::new(ReapSignal::new());
        let mut drain = match client {
            Some(client) => {
                let Ok(drain) = ControlDrain::start(
                    client,
                    self.store.clone(),
                    saga.clone(),
                    Arc::clone(&healthy),
                    Arc::clone(&intentional_transition),
                    reap_signal,
                ) else {
                    return Err(self
                        .finish_recovery_probe(
                            owner,
                            None,
                            ThreadAdoptionCoordinatorError::ControlStreamUnavailable,
                        )
                        .await);
                };
                Some(drain)
            }
            None => None,
        };
        let Ok(thread) = owner.read_thread(&saga.codex_thread_id).await else {
            return Err(self
                .finish_recovery_probe(
                    owner,
                    drain.take(),
                    ThreadAdoptionCoordinatorError::CandidateReadFailed,
                )
                .await);
        };
        let validated = match validate_candidate_for_resume(
            &thread,
            &saga.codex_thread_id,
            policy,
            &HashSet::new(),
        ) {
            Ok(validated) => validated,
            Err(error) => {
                return Err(self
                    .finish_recovery_probe(owner, drain.take(), candidate_error(error))
                    .await);
            }
        };
        if !healthy.load(Ordering::Acquire) {
            return Err(self
                .finish_recovery_probe(owner, drain.take(), ThreadAdoptionCoordinatorError::Fenced)
                .await);
        }
        let mut params = ThreadResumeParams::new(&validated.thread_id);
        params.overrides = settings.overrides(validated.cwd.clone());
        let resumed = match owner.resume_thread(params).await {
            Ok(thread) => thread,
            Err(DomainOperationError::ActiveWriter) => {
                return Err(self
                    .finish_recovery_probe(
                        owner,
                        drain.take(),
                        ThreadAdoptionCoordinatorError::ActiveWriterConflict,
                    )
                    .await);
            }
            Err(_) => {
                return Err(self
                    .finish_recovery_probe(
                        owner,
                        drain.take(),
                        ThreadAdoptionCoordinatorError::ResumeFailed,
                    )
                    .await);
            }
        };
        let resumed_cwd = policy.validate_workspace(&resumed.cwd).ok();
        if resumed.id != validated.thread_id
            || resumed_cwd.as_ref() != Some(&validated.cwd)
            || !healthy.load(Ordering::Acquire)
        {
            return Err(self
                .finish_recovery_probe(
                    owner,
                    drain.take(),
                    ThreadAdoptionCoordinatorError::CandidateChanged,
                )
                .await);
        }

        intentional_transition.store(true, Ordering::Release);
        if !committed_mapping {
            if let Some(drain) = drain.take() {
                drain.stop().await;
            }
            if owner.shutdown().await.is_err() {
                return Err(ThreadAdoptionCoordinatorError::CleanupUnconfirmed);
            }
            return match self
                .store
                .finish_thread_adoption_acquisition_failure(&saga)
                .await
            {
                Ok(terminal) => Ok(ReleaseReceipt {
                    thread_id: terminal.codex_thread_id,
                    generation: terminal.generation,
                    outcome: ReleaseOutcome::UncommittedAcquisitionCleaned,
                }),
                Err(error) => {
                    let _ = self.store.fence_thread_adoption(&saga).await;
                    Err(ThreadAdoptionCoordinatorError::Store(error))
                }
            };
        }
        let releasing = match self.store.begin_thread_adoption_release(&saga).await {
            Ok(releasing) => releasing,
            Err(error) => {
                return Err(self
                    .finish_recovery_probe(
                        owner,
                        drain.take(),
                        ThreadAdoptionCoordinatorError::Store(error),
                    )
                    .await);
            }
        };
        if let Some(drain) = drain.take() {
            drain.stop().await;
        }
        if owner.shutdown().await.is_err() {
            self.finish_release_or_fence(&releasing, ThreadAdoptionReleaseResult::Failed)
                .await?;
            return Err(ThreadAdoptionCoordinatorError::CleanupUnconfirmed);
        }
        self.finish_release_or_fence(&releasing, ThreadAdoptionReleaseResult::Released)
            .await?;
        Ok(ReleaseReceipt {
            thread_id: releasing.codex_thread_id,
            generation: releasing.generation,
            outcome: ReleaseOutcome::AdoptedMappingReleased,
        })
    }

    /// Fences every live mapping and then reaps every locally owned process.
    /// Shutdown never retires a mapping because it is not an explicit release.
    pub async fn shutdown_fence_and_reap(&self) -> AdoptionShutdownReport {
        {
            let mut state = lock(&self.state);
            state.lifecycle = CoordinatorLifecycle::ShuttingDown;
        }
        self.wait_for_operations().await;
        let domains = {
            let mut state = lock(&self.state);
            state
                .domains
                .drain()
                .map(|(_, domain)| domain)
                .collect::<Vec<_>>()
        };
        let mut report = AdoptionShutdownReport::default();
        for mut domain in domains {
            domain.monitor_cancel.take();
            domain.intentional_transition.store(true, Ordering::Release);
            match self.store.fence_thread_adoption(&domain.saga).await {
                Ok(_) => report.fenced = report.fenced.saturating_add(1),
                Err(_) => report.failures = report.failures.saturating_add(1),
            }
            if let Some(drain) = domain.drain.take() {
                drain.stop().await;
            }
            match domain.owner.shutdown().await {
                Ok(()) => report.reaped = report.reaped.saturating_add(1),
                Err(_) => report.failures = report.failures.saturating_add(1),
            }
        }
        report
    }

    fn start_reap_monitor(&self, scope: &ScopeKey, signal: Arc<ReapSignal>) {
        let scope_key = scope.to_string();
        let cancellation = CancellationToken::new();
        {
            let mut state = lock(&self.state);
            let Some(domain) = state.domains.get_mut(&scope_key) else {
                return;
            };
            domain.monitor_cancel = Some(MonitorCancellation(cancellation.clone()));
        }
        let weak_state = Arc::downgrade(&self.state);
        let store = self.store.clone();
        let operations_changed = Arc::clone(&self.operations_changed);
        tokio::spawn(async move {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => {}
                () = signal.wait() => {
                    reap_signaled_domain(weak_state, store, operations_changed, scope_key).await;
                }
            }
        });
    }

    async fn idempotent_adoption(
        &self,
        scope: &ScopeKey,
        selector: &str,
        shared_profile: &ProfileIdentity,
    ) -> Result<Option<AdoptionReceipt>, ThreadAdoptionCoordinatorError> {
        let Some(mapping) = self.store.active_thread(scope).await? else {
            return Ok(None);
        };
        if mapping.origin != ThreadOrigin::ExternallyAdopted || mapping.codex_thread_id != selector
        {
            return Ok(None);
        }
        let Some(generation) = mapping.adoption_generation else {
            return Err(ThreadAdoptionCoordinatorError::Fenced);
        };
        let saga = self
            .store
            .active_thread_adoption(scope)
            .await?
            .ok_or(ThreadAdoptionCoordinatorError::Fenced)?;
        if saga.state != ThreadAdoptionState::Owned
            || saga.generation != generation
            || saga.codex_thread_id != selector
        {
            return Err(ThreadAdoptionCoordinatorError::Fenced);
        }
        let state = lock(&self.state);
        let domain = state
            .domains
            .get(&scope.to_string())
            .ok_or(ThreadAdoptionCoordinatorError::DomainMissing)?;
        if domain.state != LocalDomainState::Owned
            || domain.saga.generation != generation
            || domain.saga.codex_thread_id != selector
            || !domain.healthy.load(Ordering::Acquire)
            || domain.owner.profile_matches(shared_profile) != Ok(true)
        {
            return Err(ThreadAdoptionCoordinatorError::Fenced);
        }
        Ok(Some(AdoptionReceipt {
            thread_id: selector.to_owned(),
            generation,
        }))
    }

    async fn idempotent_release(
        &self,
        scope: &ScopeKey,
    ) -> Result<Option<ReleaseReceipt>, ThreadAdoptionCoordinatorError> {
        let Some(saga) = self.store.thread_adoption_saga(scope).await? else {
            return Ok(None);
        };
        if saga.state != ThreadAdoptionState::Terminal {
            return Ok(None);
        }
        let mapping = self.store.active_thread(scope).await?;
        let outcome = match saga.outcome {
            Some(ThreadAdoptionOutcome::Released) if mapping.is_none() => {
                ReleaseOutcome::AdoptedMappingReleased
            }
            Some(ThreadAdoptionOutcome::AcquisitionFailed)
                if mapping.as_ref().is_none_or(|mapping| {
                    mapping.origin == ThreadOrigin::BridgeCreated
                        && mapping.adoption_generation.is_none()
                        && mapping.codex_thread_id != saga.codex_thread_id
                }) =>
            {
                ReleaseOutcome::UncommittedAcquisitionCleaned
            }
            Some(ThreadAdoptionOutcome::Released | ThreadAdoptionOutcome::AcquisitionFailed)
            | None => return Err(ThreadAdoptionCoordinatorError::Fenced),
        };
        Ok(Some(ReleaseReceipt {
            thread_id: saga.codex_thread_id,
            generation: saga.generation,
            outcome,
        }))
    }

    fn require_ready_and_supported(&self) -> Result<(), ThreadAdoptionCoordinatorError> {
        if !self.launcher.supported() {
            return Err(ThreadAdoptionCoordinatorError::UnsupportedBackend);
        }
        match lock(&self.state).lifecycle {
            CoordinatorLifecycle::Ready => Ok(()),
            CoordinatorLifecycle::New => Err(ThreadAdoptionCoordinatorError::StartupFenceRequired),
            CoordinatorLifecycle::ShuttingDown => Err(ThreadAdoptionCoordinatorError::ShuttingDown),
        }
    }

    fn begin_acquisition(
        &self,
        scope: &ScopeKey,
    ) -> Result<OperationGuard, ThreadAdoptionCoordinatorError> {
        let key = scope.to_string();
        let mut state = lock(&self.state);
        if state.lifecycle != CoordinatorLifecycle::Ready {
            return Err(ThreadAdoptionCoordinatorError::ShuttingDown);
        }
        if state.operations.contains_key(&key) || state.domains.contains_key(&key) {
            return Err(ThreadAdoptionCoordinatorError::ScopeBusy);
        }
        let acquisitions = state
            .operations
            .values()
            .filter(|stage| {
                matches!(
                    stage,
                    OperationStage::Acquiring | OperationStage::Committing
                )
            })
            .count();
        if state.domains.len().saturating_add(acquisitions) >= self.max_domains {
            return Err(ThreadAdoptionCoordinatorError::Capacity);
        }
        state
            .operations
            .insert(key.clone(), OperationStage::Acquiring);
        Ok(OperationGuard {
            state: Arc::clone(&self.state),
            changed: Arc::clone(&self.operations_changed),
            key,
            installed: false,
        })
    }

    fn begin_release(
        &self,
        scope: &ScopeKey,
    ) -> Result<OperationGuard, ThreadAdoptionCoordinatorError> {
        self.begin_domain_stop(scope, OperationStage::Releasing)
    }

    fn begin_domain_stop(
        &self,
        scope: &ScopeKey,
        operation: OperationStage,
    ) -> Result<OperationGuard, ThreadAdoptionCoordinatorError> {
        debug_assert!(matches!(
            operation,
            OperationStage::Releasing | OperationStage::FenceAndReap
        ));
        let key = scope.to_string();
        let mut state = lock(&self.state);
        if state.lifecycle != CoordinatorLifecycle::Ready {
            return Err(ThreadAdoptionCoordinatorError::ShuttingDown);
        }
        if state.operations.contains_key(&key) {
            return Err(ThreadAdoptionCoordinatorError::ScopeBusy);
        }
        let domain = state
            .domains
            .get_mut(&key)
            .ok_or(ThreadAdoptionCoordinatorError::DomainMissing)?;
        if domain.state != LocalDomainState::Owned {
            return Err(ThreadAdoptionCoordinatorError::ScopeBusy);
        }
        domain.state = LocalDomainState::Releasing;
        domain.intentional_transition.store(true, Ordering::Release);
        state.operations.insert(key.clone(), operation);
        Ok(OperationGuard {
            state: Arc::clone(&self.state),
            changed: Arc::clone(&self.operations_changed),
            key,
            installed: false,
        })
    }

    fn begin_recovery_release(
        &self,
        scope: &ScopeKey,
    ) -> Result<OperationGuard, ThreadAdoptionCoordinatorError> {
        let key = scope.to_string();
        let mut state = lock(&self.state);
        if state.lifecycle != CoordinatorLifecycle::Ready {
            return Err(ThreadAdoptionCoordinatorError::ShuttingDown);
        }
        if state.operations.contains_key(&key) || state.domains.contains_key(&key) {
            return Err(ThreadAdoptionCoordinatorError::ScopeBusy);
        }
        let launched = state
            .operations
            .values()
            .filter(|stage| {
                matches!(
                    stage,
                    OperationStage::Acquiring
                        | OperationStage::Committing
                        | OperationStage::RecoveryRelease
                )
            })
            .count();
        if state.domains.len().saturating_add(launched) >= self.max_domains {
            return Err(ThreadAdoptionCoordinatorError::Capacity);
        }
        state
            .operations
            .insert(key.clone(), OperationStage::RecoveryRelease);
        Ok(OperationGuard {
            state: Arc::clone(&self.state),
            changed: Arc::clone(&self.operations_changed),
            key,
            installed: false,
        })
    }

    async fn finish_acquisition_failure(
        &self,
        reservation: &ThreadAdoptionSaga,
        owner: Option<Box<dyn DomainOwner>>,
        drain: Option<ControlDrain>,
        primary: ThreadAdoptionCoordinatorError,
    ) -> ThreadAdoptionCoordinatorError {
        if let Some(drain) = drain {
            drain.stop().await;
        }
        if let Some(owner) = owner {
            if owner.shutdown().await.is_err() {
                let _ = self.store.fence_thread_adoption(reservation).await;
                return ThreadAdoptionCoordinatorError::CleanupUnconfirmed;
            }
        }
        if let Err(error) = self
            .store
            .finish_thread_adoption_acquisition_failure(reservation)
            .await
        {
            let _ = self.store.fence_thread_adoption(reservation).await;
            return ThreadAdoptionCoordinatorError::Store(error);
        }
        primary
    }

    async fn finish_recovery_probe(
        &self,
        owner: Box<dyn DomainOwner>,
        drain: Option<ControlDrain>,
        primary: ThreadAdoptionCoordinatorError,
    ) -> ThreadAdoptionCoordinatorError {
        if let Some(drain) = drain {
            drain.stop().await;
        }
        if owner.shutdown().await.is_err() {
            ThreadAdoptionCoordinatorError::CleanupUnconfirmed
        } else {
            primary
        }
    }

    async fn finish_release_or_fence(
        &self,
        releasing: &ThreadAdoptionSaga,
        result: ThreadAdoptionReleaseResult,
    ) -> Result<(), ThreadAdoptionCoordinatorError> {
        match self
            .store
            .finish_thread_adoption_release(releasing, result)
            .await
        {
            Ok(_) => Ok(()),
            Err(error) => {
                // The process owner has already been consumed. Preserve an
                // explicit recovery fence if the terminal transition could
                // not be made durable.
                let _ = self.store.fence_thread_adoption(releasing).await;
                Err(ThreadAdoptionCoordinatorError::Store(error))
            }
        }
    }

    async fn wait_for_operations(&self) {
        loop {
            let notified = self.operations_changed.notified();
            if lock(&self.state).operations.is_empty() {
                break;
            }
            notified.await;
        }
    }
}

async fn reap_signaled_domain(
    weak_state: std::sync::Weak<Mutex<CoordinatorState>>,
    store: StoreHandle,
    operations_changed: Arc<Notify>,
    scope_key: String,
) {
    let Some(state) = weak_state.upgrade() else {
        return;
    };
    {
        let mut coordinator = lock(&state);
        if coordinator.lifecycle != CoordinatorLifecycle::Ready
            || coordinator.operations.contains_key(&scope_key)
        {
            return;
        }
        let Some(domain) = coordinator.domains.get_mut(&scope_key) else {
            return;
        };
        if domain.state != LocalDomainState::Owned {
            return;
        }
        domain.state = LocalDomainState::Releasing;
        domain.intentional_transition.store(true, Ordering::Release);
        coordinator
            .operations
            .insert(scope_key.clone(), OperationStage::FenceAndReap);
    }
    let mut operation = OperationGuard {
        state,
        changed: operations_changed,
        key: scope_key,
        installed: false,
    };
    let Ok(saga) = operation.saga() else {
        return;
    };
    let first_fence = store.fence_thread_adoption(&saga).await;
    let Ok(mut domain) = operation.take_domain() else {
        return;
    };
    domain.monitor_cancel.take();
    if let Some(drain) = domain.drain.take() {
        drain.stop().await;
    }
    let _ = domain.owner.shutdown().await;
    if first_fence.is_err() {
        let _ = store.fence_thread_adoption(&saga).await;
    }
}

struct OperationGuard {
    state: Arc<Mutex<CoordinatorState>>,
    changed: Arc<Notify>,
    key: String,
    installed: bool,
}

impl OperationGuard {
    fn mark_committing(&mut self) -> Result<(), ThreadAdoptionCoordinatorError> {
        let mut state = lock(&self.state);
        if state.lifecycle != CoordinatorLifecycle::Ready {
            return Err(ThreadAdoptionCoordinatorError::ShuttingDown);
        }
        let operation_stage = state
            .operations
            .get_mut(&self.key)
            .ok_or(ThreadAdoptionCoordinatorError::ScopeBusy)?;
        if *operation_stage != OperationStage::Acquiring {
            return Err(ThreadAdoptionCoordinatorError::ScopeBusy);
        }
        *operation_stage = OperationStage::Committing;
        Ok(())
    }

    fn install_domain(&mut self, domain: OwnedDomain) {
        let mut state = lock(&self.state);
        debug_assert_eq!(
            state.operations.get(&self.key),
            Some(&OperationStage::Committing)
        );
        let previous = state.domains.insert(self.key.clone(), domain);
        debug_assert!(previous.is_none());
        state.operations.remove(&self.key);
        self.installed = true;
        self.changed.notify_waiters();
    }

    fn saga(&self) -> Result<ThreadAdoptionSaga, ThreadAdoptionCoordinatorError> {
        let state = lock(&self.state);
        state
            .domains
            .get(&self.key)
            .map(|domain| domain.saga.clone())
            .ok_or(ThreadAdoptionCoordinatorError::DomainMissing)
    }

    fn restore_owned(&mut self) {
        let mut state = lock(&self.state);
        if let Some(domain) = state.domains.get_mut(&self.key) {
            domain.state = LocalDomainState::Owned;
            domain
                .intentional_transition
                .store(false, Ordering::Release);
        }
    }

    fn take_domain(&mut self) -> Result<OwnedDomain, ThreadAdoptionCoordinatorError> {
        lock(&self.state)
            .domains
            .remove(&self.key)
            .ok_or(ThreadAdoptionCoordinatorError::DomainMissing)
    }
}

impl Drop for OperationGuard {
    fn drop(&mut self) {
        if !self.installed {
            lock(&self.state).operations.remove(&self.key);
            self.changed.notify_waiters();
        }
    }
}

struct ControlDrain {
    shutdown: CancellationToken,
    task: JoinHandle<()>,
}

impl ControlDrain {
    fn start(
        client: Arc<AppServerClient>,
        store: StoreHandle,
        saga: ThreadAdoptionSaga,
        healthy: Arc<AtomicBool>,
        intentional_transition: Arc<AtomicBool>,
        reap_signal: Arc<ReapSignal>,
    ) -> Result<Self, ()> {
        let mut events = client.take_control_events().map_err(|_| ())?;
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let task = tokio::spawn(async move {
            let mut fence = false;
            loop {
                tokio::select! {
                    biased;
                    () = task_shutdown.cancelled() => break,
                    event = events.recv() => {
                        let Some(event) = event else {
                            fence = true;
                            break;
                        };
                        match event {
                            ControlEvent::ServerRequest(mut request) => {
                                let response = client.respond_request_error(
                                    &mut request,
                                    ADOPTED_SERVER_REQUEST_ERROR,
                                    ADOPTED_SERVER_REQUEST_MESSAGE,
                                );
                                tokio::select! {
                                    biased;
                                    () = task_shutdown.cancelled() => break,
                                    result = response => {
                                        if result.is_err() {
                                            fence = true;
                                            break;
                                        }
                                    }
                                }
                            }
                            ControlEvent::ConnectionClosed(_)
                            | ControlEvent::ProtocolDrift
                            | ControlEvent::InvalidNotification { authoritative: true, .. } => {
                                fence = true;
                                break;
                            }
                            ControlEvent::UnknownNotification { .. }
                            | ControlEvent::InvalidNotification { authoritative: false, .. } => {}
                        }
                    }
                }
            }
            if fence {
                healthy.store(false, Ordering::Release);
                if !intentional_transition.load(Ordering::Acquire) {
                    reap_signal.request();
                    let _ = store.fence_thread_adoption(&saga).await;
                }
            }
        });
        Ok(Self { shutdown, task })
    }

    async fn stop(self) {
        self.shutdown.cancel();
        let _ = self.task.await;
    }
}

fn validate_selector(selector: &str) -> Result<(), ThreadAdoptionCoordinatorError> {
    if selector.is_empty()
        || selector.len() > THREAD_ADOPTION_SELECTOR_MAX_BYTES
        || selector.bytes().any(|byte| byte.is_ascii_control())
    {
        Err(ThreadAdoptionCoordinatorError::InvalidSelector)
    } else {
        Ok(())
    }
}

fn candidate_proof_scope_fingerprint(scope: &ScopeKey) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(CANDIDATE_PROOF_SCOPE_DOMAIN);
    digest.update(scope.to_string().as_bytes());
    digest.finalize().into()
}

fn candidate_error(error: CandidateValidationError) -> ThreadAdoptionCoordinatorError {
    match error {
        CandidateValidationError::SelectorMismatch => {
            ThreadAdoptionCoordinatorError::CandidateChanged
        }
        CandidateValidationError::NotIdlePersisted => {
            ThreadAdoptionCoordinatorError::CandidateNotIdle
        }
        CandidateValidationError::WorkspaceDenied => {
            ThreadAdoptionCoordinatorError::CandidateWorkspaceDenied
        }
        CandidateValidationError::AlreadyBound => {
            ThreadAdoptionCoordinatorError::CandidateAlreadyBound
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        path::{Path, PathBuf},
        sync::atomic::{AtomicUsize, Ordering},
    };

    use serde_json::json;
    use tempfile::TempDir;

    use super::*;
    use crate::{
        config::BridgeConfig,
        runtime::policy::PlatformRoots,
        store::{ThreadAdoptionOutcome, ThreadAdoptionState},
    };

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    #[tokio::test]
    async fn production_launcher_rejects_managed_adoption_on_unproved_platforms() {
        let launcher = ProductionLauncher {
            backend: LaunchBackend::Spawned(CodexProcessConfig::default()),
        };
        assert!(!launcher.supported());
        assert!(matches!(
            launcher.launch().await,
            Err(DomainOperationError::NotReady)
        ));
    }

    #[derive(Clone, Copy)]
    enum FakeResume {
        Success,
        Conflict,
    }

    #[derive(Clone)]
    struct FakeSpec {
        profile_matches: bool,
        contract_supported: bool,
        thread: Thread,
        resume: FakeResume,
        cleanup_succeeds: bool,
        cleanup_calls: Arc<AtomicUsize>,
    }

    struct FakeLauncher {
        specs: Mutex<VecDeque<FakeSpec>>,
        spawn_count: Arc<AtomicUsize>,
    }

    impl FakeLauncher {
        fn new(specs: impl IntoIterator<Item = FakeSpec>) -> (Arc<Self>, Arc<AtomicUsize>) {
            let spawn_count = Arc::new(AtomicUsize::new(0));
            (
                Arc::new(Self {
                    specs: Mutex::new(specs.into_iter().collect()),
                    spawn_count: Arc::clone(&spawn_count),
                }),
                spawn_count,
            )
        }
    }

    impl DomainLauncher for FakeLauncher {
        fn supported(&self) -> bool {
            true
        }

        fn launch(
            &self,
        ) -> DomainFuture<'static, Result<Box<dyn DomainOwner>, DomainOperationError>> {
            self.spawn_count.fetch_add(1, Ordering::Relaxed);
            let spec = lock(&self.specs).pop_front();
            Box::pin(async move {
                spec.map(|spec| Box::new(FakeOwner { spec }) as Box<dyn DomainOwner>)
                    .ok_or(DomainOperationError::NotReady)
            })
        }
    }

    struct FakeDiscoverySource {
        pages: Mutex<VecDeque<Result<ThreadListResult, ()>>>,
        cursors: Mutex<Vec<Option<String>>>,
        contract_supported: bool,
    }

    impl FakeDiscoverySource {
        fn new(pages: impl IntoIterator<Item = ThreadListResult>) -> Self {
            Self {
                pages: Mutex::new(pages.into_iter().map(Ok).collect()),
                cursors: Mutex::new(Vec::new()),
                contract_supported: true,
            }
        }
    }

    impl CandidateDiscoverySource for FakeDiscoverySource {
        fn supports_adoption_contract(&self) -> bool {
            self.contract_supported
        }

        fn list_candidate_page(
            &self,
            params: ThreadListParams,
        ) -> DomainFuture<'_, Result<ThreadListResult, ()>> {
            lock(&self.cursors).push(params.cursor);
            let result = lock(&self.pages).pop_front().unwrap_or(Err(()));
            Box::pin(async move { result })
        }
    }

    struct FakeOwner {
        spec: FakeSpec,
    }

    impl DomainOwner for FakeOwner {
        fn route_client(&self) -> Option<Arc<AppServerClient>> {
            None
        }

        fn supports_adoption_contract(&self) -> bool {
            self.spec.contract_supported
        }

        fn profile_matches(&self, _shared: &ProfileIdentity) -> Result<bool, DomainOperationError> {
            Ok(self.spec.profile_matches)
        }

        fn read_thread<'a>(
            &'a self,
            _selector: &'a str,
        ) -> DomainFuture<'a, Result<Thread, DomainOperationError>> {
            let thread = self.spec.thread.clone();
            Box::pin(async move { Ok(thread) })
        }

        fn resume_thread(
            &self,
            _params: ThreadResumeParams,
        ) -> DomainFuture<'_, Result<Thread, DomainOperationError>> {
            let thread = self.spec.thread.clone();
            let resume = self.spec.resume;
            Box::pin(async move {
                match resume {
                    FakeResume::Success => Ok(thread),
                    FakeResume::Conflict => Err(DomainOperationError::ActiveWriter),
                }
            })
        }

        fn shutdown(self: Box<Self>) -> DomainFuture<'static, Result<(), DomainOperationError>> {
            let cleanup_succeeds = self.spec.cleanup_succeeds;
            let cleanup_calls = Arc::clone(&self.spec.cleanup_calls);
            Box::pin(async move {
                cleanup_calls.fetch_add(1, Ordering::Relaxed);
                if cleanup_succeeds {
                    Ok(())
                } else {
                    Err(DomainOperationError::CleanupUnconfirmed)
                }
            })
        }
    }

    struct PolicyFixture {
        _temporary: TempDir,
        cwd: PathBuf,
        policy: AccessPolicy,
        shared_profile: ProfileIdentity,
    }

    fn policy_fixture() -> PolicyFixture {
        let temporary = tempfile::tempdir().expect("temporary root");
        let home = temporary.path().join("home");
        let cwd = home.join("workspace");
        std::fs::create_dir_all(&cwd).expect("workspace");
        let roots =
            PlatformRoots::new(&home, Vec::new(), Vec::new(), Vec::new()).expect("platform roots");
        let mut config = BridgeConfig {
            owners: vec!["owner".to_owned()],
            ..BridgeConfig::default()
        };
        config.workspace.allow_roots = vec![cwd.clone()];
        let policy = AccessPolicy::with_platform_roots(&config, &roots).expect("policy");
        PolicyFixture {
            _temporary: temporary,
            cwd,
            policy,
            shared_profile: ProfileIdentity::from_codex_home(Path::new("/profile/shared")),
        }
    }

    fn idle_thread(id: &str, cwd: &Path) -> Thread {
        serde_json::from_value(json!({
            "id": id,
            "sessionId": id,
            "preview": "private",
            "modelProvider": "openai",
            "createdAt": 1,
            "updatedAt": 2,
            "status": {"type": "idle"},
            "ephemeral": false,
            "turns": [],
            "source": "appServer",
            "cliVersion": "0.149.0",
            "cwd": cwd,
            "name": "private title"
        }))
        .expect("thread fixture")
    }

    fn not_loaded_thread(id: &str, cwd: &Path) -> Thread {
        let mut thread = idle_thread(id, cwd);
        thread.status = json!({"type": "notLoaded"});
        thread
    }

    fn candidate_page(threads: Vec<Thread>, next_cursor: Option<&str>) -> ThreadListResult {
        ThreadListResult {
            data: threads,
            next_cursor: next_cursor.map(str::to_owned),
            backwards_cursor: None,
        }
    }

    fn settings() -> AdoptionResumeSettings {
        AdoptionResumeSettings {
            sandbox: SandboxMode::WorkspaceWrite,
            approval_policy: ApprovalPolicy::Named("never".to_owned()),
            model: None,
        }
    }

    async fn seed_scope(
        store: &StoreHandle,
        label: &str,
        cwd: &Path,
        policy: &AccessPolicy,
    ) -> ScopeKey {
        let scope = ScopeKey::Chat(label.to_owned());
        let fingerprint = policy.fingerprint(cwd).expect("fingerprint");
        store
            .upsert_scope(&scope, cwd, fingerprint.as_str())
            .await
            .expect("scope");
        scope
    }

    fn spec(
        thread: Thread,
        profile_matches: bool,
        resume: FakeResume,
        cleanup_succeeds: bool,
    ) -> (FakeSpec, Arc<AtomicUsize>) {
        let cleanup_calls = Arc::new(AtomicUsize::new(0));
        (
            FakeSpec {
                profile_matches,
                contract_supported: true,
                thread,
                resume,
                cleanup_succeeds,
                cleanup_calls: Arc::clone(&cleanup_calls),
            },
            cleanup_calls,
        )
    }

    #[tokio::test]
    async fn not_loaded_active_page_proof_round_trip_allows_authoritative_adoption() {
        let fixture = policy_fixture();
        let store = StoreHandle::open_in_memory().await.expect("store");
        let scope = seed_scope(&store, "proof-not-loaded", &fixture.cwd, &fixture.policy).await;
        let selector = "persisted-not-loaded";
        let source = FakeDiscoverySource::new([
            candidate_page(
                vec![not_loaded_thread(selector, &fixture.cwd)],
                Some("next-page"),
            ),
            candidate_page(vec![not_loaded_thread(selector, &fixture.cwd)], None),
        ]);
        let (spec, cleanup_calls) = spec(
            not_loaded_thread(selector, &fixture.cwd),
            true,
            FakeResume::Success,
            true,
        );
        let (launcher, spawn_count) = FakeLauncher::new([spec]);
        let coordinator = ThreadAdoptionCoordinator::with_test_launcher(store.clone(), launcher, 4);
        coordinator.startup_fence().await.expect("startup fence");

        let discovery = coordinator
            .discover_from_source(
                &scope,
                &source,
                Some("selected-page".to_owned()),
                &fixture.policy,
            )
            .await
            .expect("discovery");
        assert_eq!(discovery.page.candidates.len(), 1);
        assert_eq!(
            discovery.page.candidates[0].observable_state,
            "not_loaded_preflight_only"
        );
        let proof_debug = format!("{:?}", discovery.proof);
        assert!(!proof_debug.contains(selector));
        assert!(!proof_debug.contains("proof-not-loaded"));

        let receipt = coordinator
            .adopt_from_source(
                &scope,
                selector,
                &source,
                &fixture.shared_profile,
                Some(discovery.proof),
                &fixture.policy,
                settings(),
                ExplicitHandoff::Confirmed,
            )
            .await
            .expect("authoritative adoption");
        assert_eq!(receipt.thread_id, selector);
        assert_eq!(spawn_count.load(Ordering::Relaxed), 1);
        assert_eq!(
            *lock(&source.cursors),
            vec![
                Some("selected-page".to_owned()),
                Some("selected-page".to_owned())
            ]
        );

        let report = coordinator.shutdown_fence_and_reap().await;
        assert_eq!(report.reaped, 1);
        assert_eq!(cleanup_calls.load(Ordering::Relaxed), 1);
        store.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn archived_target_absent_from_fresh_active_page_blocks_before_launch() {
        let fixture = policy_fixture();
        let store = StoreHandle::open_in_memory().await.expect("store");
        let scope = seed_scope(&store, "proof-archived", &fixture.cwd, &fixture.policy).await;
        let selector = "persisted-now-archived";
        let source = FakeDiscoverySource::new([
            candidate_page(vec![not_loaded_thread(selector, &fixture.cwd)], None),
            candidate_page(Vec::new(), None),
        ]);
        let (spec, _) = spec(
            not_loaded_thread(selector, &fixture.cwd),
            true,
            FakeResume::Success,
            true,
        );
        let (launcher, spawn_count) = FakeLauncher::new([spec]);
        let coordinator = ThreadAdoptionCoordinator::with_test_launcher(store.clone(), launcher, 4);
        coordinator.startup_fence().await.expect("startup fence");
        let discovery = coordinator
            .discover_from_source(&scope, &source, None, &fixture.policy)
            .await
            .expect("initial active page");

        let error = coordinator
            .adopt_from_source(
                &scope,
                selector,
                &source,
                &fixture.shared_profile,
                Some(discovery.proof),
                &fixture.policy,
                settings(),
                ExplicitHandoff::Confirmed,
            )
            .await
            .expect_err("archived target must disappear from active page");
        assert_eq!(error.code(), "candidate_refresh_required");
        assert_eq!(spawn_count.load(Ordering::Relaxed), 0);
        assert!(
            store
                .thread_adoption_saga(&scope)
                .await
                .expect("saga query")
                .is_none()
        );
        assert!(
            store
                .active_thread(&scope)
                .await
                .expect("mapping query")
                .is_none()
        );
        store.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn candidate_proof_is_scope_bound_selector_exact_and_expiring() {
        let fixture = policy_fixture();
        let store = StoreHandle::open_in_memory().await.expect("store");
        let scope = seed_scope(&store, "proof-scope", &fixture.cwd, &fixture.policy).await;
        let other_scope = seed_scope(&store, "proof-other", &fixture.cwd, &fixture.policy).await;
        let selector = "persisted-proof";
        let source = FakeDiscoverySource::new([candidate_page(
            vec![idle_thread(selector, &fixture.cwd)],
            None,
        )]);
        let (launcher, _) = FakeLauncher::new([]);
        let coordinator = ThreadAdoptionCoordinator::with_test_launcher(store.clone(), launcher, 4);
        coordinator.startup_fence().await.expect("startup fence");
        let mut proof = coordinator
            .discover_from_source(&scope, &source, None, &fixture.policy)
            .await
            .expect("discovery")
            .proof;

        assert_eq!(
            proof
                .verify(&other_scope, selector)
                .expect_err("scope bound")
                .code(),
            "candidate_proof_mismatch"
        );
        assert_eq!(
            proof
                .verify(&scope, "another-selector")
                .expect_err("selector exact")
                .code(),
            "candidate_proof_mismatch"
        );
        proof.issued_at = Instant::now()
            .checked_sub(CANDIDATE_SELECTION_PROOF_TTL + Duration::from_secs(1))
            .expect("test duration is representable");
        assert_eq!(
            proof.verify(&scope, selector).expect_err("expired").code(),
            "candidate_proof_expired"
        );
        store.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn active_writer_conflict_reaps_before_terminalizing_without_mapping() {
        let fixture = policy_fixture();
        let store = StoreHandle::open_in_memory().await.expect("store");
        let scope = seed_scope(&store, "conflict", &fixture.cwd, &fixture.policy).await;
        let (spec, cleanup_calls) = spec(
            idle_thread("persisted-conflict", &fixture.cwd),
            true,
            FakeResume::Conflict,
            true,
        );
        let (launcher, _) = FakeLauncher::new([spec]);
        let coordinator = ThreadAdoptionCoordinator::with_test_launcher(store.clone(), launcher, 4);
        coordinator.startup_fence().await.expect("startup fence");

        let error = coordinator
            .adopt_after_fresh_selection(
                &scope,
                "persisted-conflict",
                &fixture.shared_profile,
                &fixture.policy,
                settings(),
                ExplicitHandoff::Confirmed,
            )
            .await
            .expect_err("conflict");
        assert_eq!(error.code(), "active_writer_conflict");
        assert_eq!(cleanup_calls.load(Ordering::Relaxed), 1);
        assert!(
            store
                .active_thread(&scope)
                .await
                .expect("mapping")
                .is_none()
        );
        let saga = store
            .thread_adoption_saga(&scope)
            .await
            .expect("saga")
            .expect("saga row");
        assert_eq!(saga.state, ThreadAdoptionState::Terminal);
        assert_eq!(saga.outcome, Some(ThreadAdoptionOutcome::AcquisitionFailed));
        store.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn profile_mismatch_reaps_and_never_resumes_or_maps() {
        let fixture = policy_fixture();
        let store = StoreHandle::open_in_memory().await.expect("store");
        let scope = seed_scope(&store, "profile", &fixture.cwd, &fixture.policy).await;
        let (spec, cleanup_calls) = spec(
            idle_thread("persisted-profile", &fixture.cwd),
            false,
            FakeResume::Success,
            true,
        );
        let (launcher, _) = FakeLauncher::new([spec]);
        let coordinator = ThreadAdoptionCoordinator::with_test_launcher(store.clone(), launcher, 4);
        coordinator.startup_fence().await.expect("startup fence");

        let error = coordinator
            .adopt_after_fresh_selection(
                &scope,
                "persisted-profile",
                &fixture.shared_profile,
                &fixture.policy,
                settings(),
                ExplicitHandoff::Confirmed,
            )
            .await
            .expect_err("profile mismatch");
        assert_eq!(error.code(), "profile_mismatch");
        assert_eq!(cleanup_calls.load(Ordering::Relaxed), 1);
        assert!(
            store
                .active_thread(&scope)
                .await
                .expect("mapping")
                .is_none()
        );
        store.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn unsupported_dedicated_wire_reaps_and_never_maps() {
        let fixture = policy_fixture();
        let store = StoreHandle::open_in_memory().await.expect("store");
        let scope = seed_scope(&store, "wire", &fixture.cwd, &fixture.policy).await;
        let (mut spec, cleanup_calls) = spec(
            idle_thread("persisted-wire", &fixture.cwd),
            true,
            FakeResume::Success,
            true,
        );
        spec.contract_supported = false;
        let (launcher, spawn_count) = FakeLauncher::new([spec]);
        let coordinator = ThreadAdoptionCoordinator::with_test_launcher(store.clone(), launcher, 4);
        coordinator.startup_fence().await.expect("startup fence");

        let error = coordinator
            .adopt_after_fresh_selection(
                &scope,
                "persisted-wire",
                &fixture.shared_profile,
                &fixture.policy,
                settings(),
                ExplicitHandoff::Confirmed,
            )
            .await
            .expect_err("unsupported wire");
        assert_eq!(error.code(), "unsupported_contract");
        assert_eq!(spawn_count.load(Ordering::Relaxed), 1);
        assert_eq!(cleanup_calls.load(Ordering::Relaxed), 1);
        assert!(
            store
                .active_thread(&scope)
                .await
                .expect("mapping")
                .is_none()
        );
        store.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn release_cleanup_failure_keeps_mapping_and_release_fence() {
        let fixture = policy_fixture();
        let store = StoreHandle::open_in_memory().await.expect("store");
        let scope = seed_scope(&store, "release", &fixture.cwd, &fixture.policy).await;
        let (spec, cleanup_calls) = spec(
            idle_thread("persisted-release", &fixture.cwd),
            true,
            FakeResume::Success,
            false,
        );
        let (launcher, _) = FakeLauncher::new([spec]);
        let coordinator = ThreadAdoptionCoordinator::with_test_launcher(store.clone(), launcher, 4);
        coordinator.startup_fence().await.expect("startup fence");
        coordinator
            .adopt_after_fresh_selection(
                &scope,
                "persisted-release",
                &fixture.shared_profile,
                &fixture.policy,
                settings(),
                ExplicitHandoff::Confirmed,
            )
            .await
            .expect("adopt");

        let error = coordinator
            .release(&scope)
            .await
            .expect_err("cleanup fails");
        assert_eq!(error.code(), "cleanup_unconfirmed");
        assert_eq!(cleanup_calls.load(Ordering::Relaxed), 1);
        let mapping = store
            .active_thread(&scope)
            .await
            .expect("mapping")
            .expect("mapping retained");
        assert_eq!(mapping.origin, ThreadOrigin::ExternallyAdopted);
        let saga = store
            .active_thread_adoption(&scope)
            .await
            .expect("saga")
            .expect("fenced saga");
        assert_eq!(saga.state, ThreadAdoptionState::ReleaseFailed);
        store.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn successful_adopt_and_release_replay_without_relaunch_or_rereap() {
        let fixture = policy_fixture();
        let store = StoreHandle::open_in_memory().await.expect("store");
        let scope = seed_scope(&store, "idempotent", &fixture.cwd, &fixture.policy).await;
        let (spec, cleanup_calls) = spec(
            idle_thread("persisted-idempotent", &fixture.cwd),
            true,
            FakeResume::Success,
            true,
        );
        let (launcher, spawn_count) = FakeLauncher::new([spec]);
        let coordinator = ThreadAdoptionCoordinator::with_test_launcher(store.clone(), launcher, 4);
        coordinator.startup_fence().await.expect("startup fence");

        let first_adoption = coordinator
            .adopt_after_fresh_selection(
                &scope,
                "persisted-idempotent",
                &fixture.shared_profile,
                &fixture.policy,
                settings(),
                ExplicitHandoff::Confirmed,
            )
            .await
            .expect("first adoption");
        let replayed_adoption = coordinator
            .adopt_after_fresh_selection(
                &scope,
                "persisted-idempotent",
                &fixture.shared_profile,
                &fixture.policy,
                settings(),
                ExplicitHandoff::Confirmed,
            )
            .await
            .expect("replayed adoption");
        assert_eq!(replayed_adoption, first_adoption);
        assert_eq!(spawn_count.load(Ordering::Relaxed), 1);

        let first_release = coordinator.release(&scope).await.expect("first release");
        let replayed_release = coordinator.release(&scope).await.expect("replayed release");
        assert_eq!(replayed_release, first_release);
        assert_eq!(
            first_release.outcome,
            ReleaseOutcome::AdoptedMappingReleased
        );
        assert_eq!(cleanup_calls.load(Ordering::Relaxed), 1);
        assert_eq!(spawn_count.load(Ordering::Relaxed), 1);
        assert!(
            store
                .active_thread(&scope)
                .await
                .expect("mapping query")
                .is_none()
        );
        store.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn control_reap_request_fences_mapping_and_confirms_domain_cleanup() {
        let fixture = policy_fixture();
        let store = StoreHandle::open_in_memory().await.expect("store");
        let scope = seed_scope(&store, "control-reap", &fixture.cwd, &fixture.policy).await;
        let (spec, cleanup_calls) = spec(
            idle_thread("persisted-control-reap", &fixture.cwd),
            true,
            FakeResume::Success,
            true,
        );
        let (launcher, _) = FakeLauncher::new([spec]);
        let coordinator = ThreadAdoptionCoordinator::with_test_launcher(store.clone(), launcher, 4);
        coordinator.startup_fence().await.expect("startup fence");
        coordinator
            .adopt_after_fresh_selection(
                &scope,
                "persisted-control-reap",
                &fixture.shared_profile,
                &fixture.policy,
                settings(),
                ExplicitHandoff::Confirmed,
            )
            .await
            .expect("adoption");
        let reap_signal = {
            let state = lock(&coordinator.state);
            Arc::clone(
                &state
                    .domains
                    .get(&scope.to_string())
                    .expect("owned domain")
                    .reap_signal,
            )
        };
        reap_signal.request();

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let saga = store
                    .active_thread_adoption(&scope)
                    .await
                    .expect("saga")
                    .expect("active saga");
                if saga.state == ThreadAdoptionState::RecoveryRequired
                    && cleanup_calls.load(Ordering::Acquire) == 1
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("automatic reap should finish");
        assert!(
            store
                .active_thread(&scope)
                .await
                .expect("mapping")
                .is_some()
        );
        assert!(
            !lock(&coordinator.state)
                .domains
                .contains_key(&scope.to_string())
        );
        store.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn cross_scope_duplicate_is_rejected_before_a_second_launch() {
        let fixture = policy_fixture();
        let store = StoreHandle::open_in_memory().await.expect("store");
        let first = seed_scope(&store, "first", &fixture.cwd, &fixture.policy).await;
        let second = seed_scope(&store, "second", &fixture.cwd, &fixture.policy).await;
        let (first_spec, first_cleanup) = spec(
            idle_thread("persisted-shared", &fixture.cwd),
            true,
            FakeResume::Success,
            true,
        );
        let (second_spec, _) = spec(
            idle_thread("persisted-shared", &fixture.cwd),
            true,
            FakeResume::Success,
            true,
        );
        let (launcher, spawn_count) = FakeLauncher::new([first_spec, second_spec]);
        let coordinator = ThreadAdoptionCoordinator::with_test_launcher(store.clone(), launcher, 4);
        coordinator.startup_fence().await.expect("startup fence");
        coordinator
            .adopt_after_fresh_selection(
                &first,
                "persisted-shared",
                &fixture.shared_profile,
                &fixture.policy,
                settings(),
                ExplicitHandoff::Confirmed,
            )
            .await
            .expect("first adoption");

        let error = coordinator
            .adopt_after_fresh_selection(
                &second,
                "persisted-shared",
                &fixture.shared_profile,
                &fixture.policy,
                settings(),
                ExplicitHandoff::Confirmed,
            )
            .await
            .expect_err("duplicate rejected");
        assert_eq!(error.code(), "store_failed");
        assert_eq!(spawn_count.load(Ordering::Relaxed), 1);
        assert!(
            store
                .active_thread(&second)
                .await
                .expect("mapping")
                .is_none()
        );
        let report = coordinator.shutdown_fence_and_reap().await;
        assert_eq!(report.fenced, 1);
        assert_eq!(report.reaped, 1);
        assert_eq!(first_cleanup.load(Ordering::Relaxed), 1);
        store.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn recovery_release_reacquires_only_to_reap_and_retire_mapping() {
        let fixture = policy_fixture();
        let store = StoreHandle::open_in_memory().await.expect("store");
        let scope = seed_scope(&store, "recover-release", &fixture.cwd, &fixture.policy).await;
        let reservation = store
            .reserve_thread_adoption(&scope, "persisted-recover-release")
            .await
            .expect("reserve");
        let fingerprint = fixture
            .policy
            .fingerprint(&fixture.cwd)
            .expect("fingerprint");
        store
            .commit_thread_adoption(&reservation, &fixture.cwd, fingerprint.as_str())
            .await
            .expect("commit");
        let (spec, cleanup_calls) = spec(
            idle_thread("persisted-recover-release", &fixture.cwd),
            true,
            FakeResume::Success,
            true,
        );
        let (launcher, _) = FakeLauncher::new([spec]);
        let coordinator = ThreadAdoptionCoordinator::with_test_launcher(store.clone(), launcher, 4);
        coordinator.startup_fence().await.expect("startup fence");

        let released = coordinator
            .recover_release_after_fresh_selection(
                &scope,
                &fixture.shared_profile,
                &fixture.policy,
                settings(),
                ExplicitHandoff::Confirmed,
            )
            .await
            .expect("recover release");
        assert_eq!(released.thread_id, "persisted-recover-release");
        assert_eq!(released.outcome, ReleaseOutcome::AdoptedMappingReleased);
        assert_eq!(cleanup_calls.load(Ordering::Relaxed), 1);
        assert!(
            store
                .active_thread(&scope)
                .await
                .expect("mapping")
                .is_none()
        );
        let saga = store
            .thread_adoption_saga(&scope)
            .await
            .expect("saga")
            .expect("terminal saga");
        assert_eq!(saga.state, ThreadAdoptionState::Terminal);
        assert_eq!(saga.outcome, Some(ThreadAdoptionOutcome::Released));
        store.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn recovery_release_terminalizes_precommit_acquisition_and_preserves_old_mapping() {
        let fixture = policy_fixture();
        let store = StoreHandle::open_in_memory().await.expect("store");
        let scope = seed_scope(&store, "orphan-acquisition", &fixture.cwd, &fixture.policy).await;
        store
            .record_active_thread(&scope, "preserved-bridge-thread")
            .await
            .expect("preserved bridge mapping");
        let reservation = store
            .reserve_thread_adoption(&scope, "persisted-orphan-acquisition")
            .await
            .expect("reserve");
        store
            .fence_thread_adoption(&reservation)
            .await
            .expect("recovery fence");
        let (spec, cleanup_calls) = spec(
            idle_thread("persisted-orphan-acquisition", &fixture.cwd),
            true,
            FakeResume::Success,
            true,
        );
        let (launcher, spawn_count) = FakeLauncher::new([spec]);
        let coordinator = ThreadAdoptionCoordinator::with_test_launcher(store.clone(), launcher, 4);
        coordinator.startup_fence().await.expect("startup fence");

        let released = coordinator
            .recover_release_after_fresh_selection(
                &scope,
                &fixture.shared_profile,
                &fixture.policy,
                settings(),
                ExplicitHandoff::Confirmed,
            )
            .await
            .expect("orphan acquisition recovery");
        assert_eq!(released.thread_id, "persisted-orphan-acquisition");
        assert_eq!(released.generation, reservation.generation);
        assert_eq!(
            released.outcome,
            ReleaseOutcome::UncommittedAcquisitionCleaned
        );
        let replayed = coordinator
            .release(&scope)
            .await
            .expect("replayed precommit cleanup");
        assert_eq!(replayed, released);
        assert_eq!(spawn_count.load(Ordering::Relaxed), 1);
        assert_eq!(cleanup_calls.load(Ordering::Relaxed), 1);
        let mapping = store
            .active_thread(&scope)
            .await
            .expect("mapping")
            .expect("preserved bridge mapping");
        assert_eq!(mapping.origin, ThreadOrigin::BridgeCreated);
        assert_eq!(mapping.codex_thread_id, "preserved-bridge-thread");
        assert_eq!(mapping.adoption_generation, None);
        let saga = store
            .thread_adoption_saga(&scope)
            .await
            .expect("saga")
            .expect("terminal saga");
        assert_eq!(saga.state, ThreadAdoptionState::Terminal);
        assert_eq!(saga.outcome, Some(ThreadAdoptionOutcome::AcquisitionFailed));
        store.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn recovery_release_conflict_reaps_probe_and_keeps_recovery_fence() {
        let fixture = policy_fixture();
        let store = StoreHandle::open_in_memory().await.expect("store");
        let scope = seed_scope(&store, "recover-conflict", &fixture.cwd, &fixture.policy).await;
        let reservation = store
            .reserve_thread_adoption(&scope, "persisted-recover-conflict")
            .await
            .expect("reserve");
        let fingerprint = fixture
            .policy
            .fingerprint(&fixture.cwd)
            .expect("fingerprint");
        store
            .commit_thread_adoption(&reservation, &fixture.cwd, fingerprint.as_str())
            .await
            .expect("commit");
        let (spec, cleanup_calls) = spec(
            idle_thread("persisted-recover-conflict", &fixture.cwd),
            true,
            FakeResume::Conflict,
            true,
        );
        let (launcher, _) = FakeLauncher::new([spec]);
        let coordinator = ThreadAdoptionCoordinator::with_test_launcher(store.clone(), launcher, 4);
        coordinator.startup_fence().await.expect("startup fence");

        let error = coordinator
            .recover_release_after_fresh_selection(
                &scope,
                &fixture.shared_profile,
                &fixture.policy,
                settings(),
                ExplicitHandoff::Confirmed,
            )
            .await
            .expect_err("active writer conflict");
        assert_eq!(error.code(), "active_writer_conflict");
        assert_eq!(cleanup_calls.load(Ordering::Relaxed), 1);
        assert!(
            store
                .active_thread(&scope)
                .await
                .expect("mapping")
                .is_some()
        );
        let saga = store
            .active_thread_adoption(&scope)
            .await
            .expect("saga")
            .expect("recovery saga");
        assert_eq!(saga.state, ThreadAdoptionState::RecoveryRequired);
        store.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn startup_fences_preexisting_owned_generation_without_releasing_mapping() {
        let fixture = policy_fixture();
        let store = StoreHandle::open_in_memory().await.expect("store");
        let scope = seed_scope(&store, "startup", &fixture.cwd, &fixture.policy).await;
        let reservation = store
            .reserve_thread_adoption(&scope, "persisted-startup")
            .await
            .expect("reserve");
        let fingerprint = fixture
            .policy
            .fingerprint(&fixture.cwd)
            .expect("fingerprint");
        store
            .commit_thread_adoption(&reservation, &fixture.cwd, fingerprint.as_str())
            .await
            .expect("commit");
        let (launcher, _) = FakeLauncher::new([]);
        let coordinator = ThreadAdoptionCoordinator::with_test_launcher(store.clone(), launcher, 4);

        assert_eq!(coordinator.startup_fence().await.expect("fence"), 1);
        let saga = store
            .active_thread_adoption(&scope)
            .await
            .expect("saga")
            .expect("saga retained");
        assert_eq!(saga.state, ThreadAdoptionState::RecoveryRequired);
        assert!(
            store
                .active_thread(&scope)
                .await
                .expect("mapping")
                .is_some()
        );
        let error = coordinator
            .route(&scope)
            .await
            .expect_err("no local domain");
        assert_eq!(error.code(), "fenced");
        store.shutdown().await.expect("shutdown");
    }
}
