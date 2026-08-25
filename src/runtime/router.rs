//! Bounded tenant-scoped routing into one actor per Lark scope.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use futures_util::future::join_all;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval, timeout};
use tokio_util::sync::CancellationToken;

use crate::codex::client::ControlEvent;
use crate::codex::supervisor::{SupervisorError, SupervisorHandle, SupervisorState};
use crate::codex::types::{ApprovalPolicy, SandboxMode};
use crate::config::{AsrSection, BridgeConfig};
use crate::lark::api::ChatMode;
use crate::lark::bridge::QueuedInboundEvent;
use crate::limits::{
    PENDING_MEDIA_MAX_COUNT, PENDING_MEDIA_MAX_METADATA_BYTES, PENDING_MEDIA_TTL,
    ROUTER_ACTIVE_TURN_HARD_LIMIT, ROUTER_COMMAND_BYTE_BUDGET, ROUTER_COMMAND_CAPACITY,
    ROUTER_CONTROL_BYTE_BUDGET, ROUTER_CONTROL_CAPACITY, ROUTER_RETRY_BYTE_BUDGET,
    ROUTER_RETRY_CAPACITY, ROUTER_SCOPE_ACTOR_HARD_LIMIT, STORE_INBOUND_SCOPE_MAX_BYTES,
};
use crate::runtime::attachments::AttachmentCache;
use crate::runtime::context::ContextRegistry;
use crate::runtime::intake::TenantNamespace;
use crate::runtime::policy::AccessPolicy;
use crate::runtime::quote::QuoteResolver;
use crate::runtime::scope::{
    ActorRouteError, DurableReplySink, InterruptOutcome, ReplySinkError, ScopeActorHandle,
    ScopeSnapshot, SupervisorAccess,
};
use crate::runtime::tools::handle_server_request;
use crate::store::{InboundKey, InboundRejectionKind, StoreError, StoreHandle};

/// Redacted, validated inputs used by the scope runtime.
#[derive(Clone)]
pub struct RouterSettings {
    pub(crate) default_workspace: Option<PathBuf>,
    pub(crate) active_turn_permits: usize,
    pub(crate) max_scope_actors: usize,
    pub(crate) sandbox: SandboxMode,
    pub(crate) approval_policy: ApprovalPolicy,
    pub(crate) model: Option<String>,
    pub(crate) network_access: bool,
    pub(crate) debounce: Duration,
    pub(crate) message_max_age: Duration,
    pub(crate) finalization_retry: Duration,
    pub(crate) shutdown_cleanup_timeout: Duration,
    pub(crate) asr: AsrSection,
    pub(crate) pending_media_ttl: Duration,
    pub(crate) pending_media_max_count: usize,
    pub(crate) pending_media_max_metadata_bytes: usize,
}

impl RouterSettings {
    /// Copies only runtime policy values from an already validated config.
    #[must_use]
    pub fn from_config(config: &BridgeConfig) -> Self {
        Self {
            default_workspace: config.default_workspace.clone(),
            active_turn_permits: config.concurrency.active_turn_permits,
            max_scope_actors: config.concurrency.max_scope_actors,
            sandbox: config.codex.sandbox,
            approval_policy: config.codex.approval_policy.clone(),
            model: config.codex.model.clone(),
            network_access: config.workspace.network_access,
            debounce: Duration::from_millis(600),
            message_max_age: Duration::from_secs(15 * 60),
            finalization_retry: Duration::from_secs(1),
            shutdown_cleanup_timeout: Duration::from_secs(5),
            asr: config.asr.clone(),
            pending_media_ttl: PENDING_MEDIA_TTL,
            pending_media_max_count: PENDING_MEDIA_MAX_COUNT,
            pending_media_max_metadata_bytes: PENDING_MEDIA_MAX_METADATA_BYTES,
        }
    }

    /// Overrides only scheduling timings for deterministic integration tests.
    ///
    /// The same non-zero validation and every production count/byte limit
    /// remain enforced by [`Router::start`].
    #[doc(hidden)]
    #[must_use]
    pub fn with_test_timings(
        mut self,
        debounce: Duration,
        message_max_age: Duration,
        finalization_retry: Duration,
    ) -> Self {
        self.debounce = debounce;
        self.message_max_age = message_max_age;
        self.finalization_retry = finalization_retry;
        self
    }

    /// Overrides the bounded best-effort finalization window used after an
    /// actor's normal cancellation token has already fired.
    #[doc(hidden)]
    #[must_use]
    pub fn with_test_shutdown_cleanup_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_cleanup_timeout = timeout;
        self
    }

    /// Overrides P2P pending-media bounds for deterministic tests.
    #[doc(hidden)]
    #[must_use]
    pub fn with_test_pending_media_limits(
        mut self,
        ttl: Duration,
        max_count: usize,
        max_metadata_bytes: usize,
    ) -> Self {
        self.pending_media_ttl = ttl;
        self.pending_media_max_count = max_count;
        self.pending_media_max_metadata_bytes = max_metadata_bytes;
        self
    }

    fn validate(&self) -> Result<(), RouteError> {
        if self.active_turn_permits == 0
            || self.active_turn_permits > ROUTER_ACTIVE_TURN_HARD_LIMIT
            || self.max_scope_actors == 0
            || self.max_scope_actors > ROUTER_SCOPE_ACTOR_HARD_LIMIT
            || self.debounce.is_zero()
            || self.message_max_age.is_zero()
            || self.finalization_retry.is_zero()
            || self.shutdown_cleanup_timeout.is_zero()
            || self.pending_media_ttl.is_zero()
            || self.pending_media_max_count == 0
            || self.pending_media_max_count > PENDING_MEDIA_MAX_COUNT
            || self.pending_media_max_metadata_bytes == 0
            || self.pending_media_max_metadata_bytes > PENDING_MEDIA_MAX_METADATA_BYTES
        {
            return Err(RouteError::InvalidSettings);
        }
        Ok(())
    }
}

impl fmt::Debug for RouterSettings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let approval_policy_kind = match &self.approval_policy {
            ApprovalPolicy::Named(_) => "named",
            ApprovalPolicy::Granular { .. } => "granular",
        };
        formatter
            .debug_struct("RouterSettings")
            .field(
                "default_workspace_configured",
                &self.default_workspace.is_some(),
            )
            .field("active_turn_permits", &self.active_turn_permits)
            .field("max_scope_actors", &self.max_scope_actors)
            .field("sandbox", &self.sandbox)
            .field("approval_policy_kind", &approval_policy_kind)
            .field("model_configured", &self.model.is_some())
            .field("network_access", &self.network_access)
            .field("debounce", &self.debounce)
            .field("message_max_age", &self.message_max_age)
            .field("finalization_retry", &self.finalization_retry)
            .field("shutdown_cleanup_timeout", &self.shutdown_cleanup_timeout)
            .field("asr", &self.asr)
            .field("pending_media_ttl", &self.pending_media_ttl)
            .field("pending_media_max_count", &self.pending_media_max_count)
            .field(
                "pending_media_max_metadata_bytes",
                &self.pending_media_max_metadata_bytes,
            )
            .finish()
    }
}

/// Static route failure classifications safe to expose at process boundaries.
#[derive(Debug, thiserror::Error)]
pub enum RouteError {
    #[error("scope router settings are invalid")]
    InvalidSettings,
    #[error("scope router capacity is exhausted")]
    Capacity,
    #[error("scope router is closed")]
    Closed,
    #[error("durable store operation failed")]
    Store,
    #[error("durable reply projection failed")]
    ReplySink,
    #[error("the app-server supervisor failed")]
    Supervisor,
    #[error("scope actor routing is not available")]
    ActorUnavailable,
    #[error("attachment cache cleanup failed")]
    Attachment,
}

impl From<StoreError> for RouteError {
    fn from(_error: StoreError) -> Self {
        Self::Store
    }
}

impl From<ReplySinkError> for RouteError {
    fn from(_error: ReplySinkError) -> Self {
        Self::ReplySink
    }
}

impl From<SupervisorError> for RouteError {
    fn from(_error: SupervisorError) -> Self {
        Self::Supervisor
    }
}

/// One route failure together with the event only when the router never
/// accepted ownership of it.
pub(crate) struct RouteAttemptError {
    error: RouteError,
    event: Option<Box<QueuedInboundEvent>>,
}

impl RouteAttemptError {
    pub(crate) fn into_parts(self) -> (RouteError, Option<Box<QueuedInboundEvent>>) {
        (self.error, self.event)
    }

    fn retained(error: RouteError, event: Box<QueuedInboundEvent>) -> Self {
        Self {
            error,
            event: Some(event),
        }
    }

    fn consumed(error: RouteError) -> Self {
        Self { error, event: None }
    }
}

/// Bounded structural router diagnostics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RouterSnapshot {
    pub scope_count: usize,
    pub queued_commands: usize,
    pub active_turns: usize,
}

/// Entry point for the router task.
pub struct Router;

impl Router {
    /// Starts one router that owns the supplied supervisor.
    ///
    /// # Errors
    ///
    /// Returns a static classification for invalid bounds or task startup.
    pub async fn start(
        store: StoreHandle,
        tenant: TenantNamespace,
        policy: AccessPolicy,
        settings: RouterSettings,
        supervisor: SupervisorHandle,
        sink: Arc<dyn DurableReplySink>,
    ) -> Result<RouterHandle, RouteError> {
        Self::start_inner(
            store, tenant, policy, settings, supervisor, sink, None, None, None,
        )
        .await
    }

    /// Starts the production router with attachment download/cache support.
    /// The plain [`Self::start`] constructor remains available for runtimes
    /// and tests that intentionally do not resolve message resources.
    ///
    /// # Errors
    ///
    /// Returns the same static classifications as [`Self::start`].
    pub async fn start_with_attachments(
        store: StoreHandle,
        tenant: TenantNamespace,
        policy: AccessPolicy,
        settings: RouterSettings,
        supervisor: SupervisorHandle,
        sink: Arc<dyn DurableReplySink>,
        attachments: Arc<AttachmentCache>,
    ) -> Result<RouterHandle, RouteError> {
        Self::start_inner(
            store,
            tenant,
            policy,
            settings,
            supervisor,
            sink,
            Some(attachments),
            None,
            None,
        )
        .await
    }

    /// Starts the production router with lazy, turn-scoped bridge context and
    /// on-demand attachment retrieval.
    ///
    /// # Errors
    ///
    /// Returns the same static classifications as [`Self::start`].
    #[allow(clippy::too_many_arguments)]
    pub async fn start_with_contexts(
        store: StoreHandle,
        tenant: TenantNamespace,
        policy: AccessPolicy,
        settings: RouterSettings,
        supervisor: SupervisorHandle,
        sink: Arc<dyn DurableReplySink>,
        attachments: Arc<AttachmentCache>,
        contexts: Arc<ContextRegistry>,
    ) -> Result<RouterHandle, RouteError> {
        Self::start_inner(
            store,
            tenant,
            policy,
            settings,
            supervisor,
            sink,
            Some(attachments),
            Some(contexts),
            None,
        )
        .await
    }

    /// Starts the production router with lazy contexts and authorized,
    /// one-hop quote resolution.
    ///
    /// # Errors
    ///
    /// Returns the same static classifications as [`Self::start`].
    #[allow(clippy::too_many_arguments)]
    pub async fn start_with_contexts_and_quotes(
        store: StoreHandle,
        tenant: TenantNamespace,
        policy: AccessPolicy,
        settings: RouterSettings,
        supervisor: SupervisorHandle,
        sink: Arc<dyn DurableReplySink>,
        attachments: Arc<AttachmentCache>,
        contexts: Arc<ContextRegistry>,
        quote_resolver: Arc<dyn QuoteResolver>,
    ) -> Result<RouterHandle, RouteError> {
        Self::start_inner(
            store,
            tenant,
            policy,
            settings,
            supervisor,
            sink,
            Some(attachments),
            Some(contexts),
            Some(quote_resolver),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_inner(
        store: StoreHandle,
        tenant: TenantNamespace,
        policy: AccessPolicy,
        settings: RouterSettings,
        supervisor: SupervisorHandle,
        sink: Arc<dyn DurableReplySink>,
        attachments: Option<Arc<AttachmentCache>>,
        contexts: Option<Arc<ContextRegistry>>,
        quote_resolver: Option<Arc<dyn QuoteResolver>>,
    ) -> Result<RouterHandle, RouteError> {
        if let Err(error) = settings.validate() {
            supervisor.shutdown().await?;
            return Err(error);
        }
        let (sender, receiver) = mpsc::channel(ROUTER_COMMAND_CAPACITY);
        let (control_sender, control_receiver) = mpsc::channel(ROUTER_CONTROL_CAPACITY);
        let snapshot = Arc::new(RwLock::new(RouterSnapshot::default()));
        let task_snapshot = Arc::clone(&snapshot);
        let active_turn_capacity = settings.active_turn_permits;
        let active_turns = Arc::new(Semaphore::new(active_turn_capacity));
        let task = tokio::spawn(run_router(
            receiver,
            control_receiver,
            store,
            tenant,
            policy,
            settings,
            Arc::clone(&active_turns),
            supervisor,
            sink,
            attachments,
            contexts,
            quote_resolver,
            task_snapshot,
        ));
        Ok(RouterHandle {
            sender,
            control_sender,
            byte_budget: Arc::new(Semaphore::new(ROUTER_COMMAND_BYTE_BUDGET)),
            control_byte_budget: Arc::new(Semaphore::new(ROUTER_CONTROL_BYTE_BUDGET)),
            snapshot,
            active_turns,
            active_turn_capacity,
            task: Some(task),
        })
    }
}

/// Client handle for routing and orderly shutdown.
pub struct RouterHandle {
    sender: mpsc::Sender<RouterCommand>,
    control_sender: mpsc::Sender<RouterControl>,
    byte_budget: Arc<Semaphore>,
    control_byte_budget: Arc<Semaphore>,
    snapshot: Arc<RwLock<RouterSnapshot>>,
    active_turns: Arc<Semaphore>,
    active_turn_capacity: usize,
    task: Option<JoinHandle<Result<(), RouteError>>>,
}

impl RouterHandle {
    /// Routes one already-durable inbound event.
    ///
    /// # Errors
    ///
    /// Returns a static classification when the bounded queue, store, or sink
    /// cannot accept the event.
    pub async fn route(&self, event: QueuedInboundEvent) -> Result<(), RouteError> {
        self.route_recoverable(event)
            .await
            .map_err(|failure| failure.error)
    }

    /// Routes one durable event while returning it only if this handle never
    /// transferred ownership to the router task.
    pub(crate) async fn route_recoverable(
        &self,
        event: QueuedInboundEvent,
    ) -> Result<(), RouteAttemptError> {
        let event = Box::new(event);
        let bytes = event.permit.num_permits();
        let Ok(bytes) = u32::try_from(bytes) else {
            return Err(RouteAttemptError::retained(RouteError::Capacity, event));
        };
        let Ok(queue_permit) = self.byte_budget.clone().try_acquire_many_owned(bytes) else {
            return Err(RouteAttemptError::retained(RouteError::Capacity, event));
        };
        let (respond, wait) = oneshot::channel();
        let command = RouterCommand::Route {
            event,
            _queue_permit: queue_permit,
            respond,
        };
        match self.sender.try_send(command) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(command)) => {
                return Err(retained_command_failure(RouteError::Capacity, command));
            }
            Err(mpsc::error::TrySendError::Closed(command)) => {
                return Err(retained_command_failure(RouteError::Closed, command));
            }
        }
        match wait.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(RouteAttemptError::consumed(error)),
            Err(_) => Err(RouteAttemptError::consumed(RouteError::Closed)),
        }
    }

    /// Returns the latest bounded structural snapshot.
    #[must_use]
    pub fn snapshot(&self) -> RouterSnapshot {
        let mut snapshot = self
            .snapshot
            .read()
            .map_or_else(|_| RouterSnapshot::default(), |snapshot| *snapshot);
        snapshot.active_turns = self
            .active_turn_capacity
            .saturating_sub(self.active_turns.available_permits());
        snapshot
    }

    /// Requests interruption of the active turn for one scope.
    ///
    /// The control travels a dedicated bounded lane so ordinary inbound
    /// backlog cannot block `/stop` behind user messages.
    ///
    /// # Errors
    ///
    /// Returns a static classification when the control lane or router is
    /// unavailable, or when the app-server rejects the interrupt RPC.
    pub async fn interrupt(
        &self,
        scope: &crate::lark::normalize::ScopeKey,
    ) -> Result<InterruptOutcome, RouteError> {
        let scope_key = scope.to_string();
        if scope_key.len() > STORE_INBOUND_SCOPE_MAX_BYTES {
            return Err(RouteError::Capacity);
        }
        let bytes = u32::try_from(scope_key.len()).map_err(|_| RouteError::Capacity)?;
        let permit = self
            .control_byte_budget
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|_| RouteError::Capacity)?;
        let (respond, wait) = oneshot::channel();
        self.control_sender
            .try_send(RouterControl::Interrupt {
                scope_key,
                _queue_permit: permit,
                respond,
            })
            .map_err(|_| RouteError::Capacity)?;
        wait.await.map_err(|_| RouteError::Closed)?
    }

    /// Returns a redacted structural snapshot for one resident scope actor.
    ///
    /// # Errors
    ///
    /// Returns a static classification when the bounded control lane or
    /// router is unavailable.
    pub async fn scope_snapshot(
        &self,
        scope: &crate::lark::normalize::ScopeKey,
    ) -> Result<Option<ScopeSnapshot>, RouteError> {
        let scope_key = scope.to_string();
        if scope_key.len() > STORE_INBOUND_SCOPE_MAX_BYTES {
            return Err(RouteError::Capacity);
        }
        let bytes = u32::try_from(scope_key.len()).map_err(|_| RouteError::Capacity)?;
        let permit = self
            .control_byte_budget
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|_| RouteError::Capacity)?;
        let (respond, wait) = oneshot::channel();
        self.control_sender
            .try_send(RouterControl::Snapshot {
                scope_key,
                _queue_permit: permit,
                respond,
            })
            .map_err(|_| RouteError::Capacity)?;
        wait.await.map_err(|_| RouteError::Closed)
    }

    /// Stops the router, its actors, and finally the owned supervisor.
    ///
    /// # Errors
    ///
    /// Returns a static classification if the router task failed.
    pub async fn shutdown(mut self) -> Result<(), RouteError> {
        let (respond, wait) = oneshot::channel();
        self.sender
            .send(RouterCommand::Shutdown { respond })
            .await
            .map_err(|_| RouteError::Closed)?;
        wait.await.map_err(|_| RouteError::Closed)?;
        match self.task.take() {
            Some(task) => task.await.map_err(|_| RouteError::Closed)?,
            None => Ok(()),
        }
    }
}

fn retained_command_failure(error: RouteError, command: RouterCommand) -> RouteAttemptError {
    let RouterCommand::Route { event, .. } = command else {
        unreachable!("route submission only constructs route commands");
    };
    RouteAttemptError::retained(error, event)
}

enum RouterCommand {
    Route {
        event: Box<QueuedInboundEvent>,
        _queue_permit: OwnedSemaphorePermit,
        respond: oneshot::Sender<Result<(), RouteError>>,
    },
    Shutdown {
        respond: oneshot::Sender<()>,
    },
}

enum RouterControl {
    Interrupt {
        scope_key: String,
        _queue_permit: OwnedSemaphorePermit,
        respond: oneshot::Sender<Result<InterruptOutcome, RouteError>>,
    },
    Snapshot {
        scope_key: String,
        _queue_permit: OwnedSemaphorePermit,
        respond: oneshot::Sender<Option<ScopeSnapshot>>,
    },
}

struct RouterRetry {
    event: QueuedInboundEvent,
    _queue_permit: OwnedSemaphorePermit,
}

struct ContextToolTask {
    epoch: crate::codex::rpc::ConnectionEpoch,
    shutdown: CancellationToken,
    task: JoinHandle<()>,
}

impl ContextToolTask {
    async fn stop(mut self, cleanup_timeout: Duration) {
        self.shutdown.cancel();
        if timeout(cleanup_timeout, &mut self.task).await.is_err() {
            self.task.abort();
            let _ = (&mut self.task).await;
        }
    }
}

impl Drop for ContextToolTask {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.task.abort();
    }
}

struct RouteFailure {
    error: RouteError,
    event: QueuedInboundEvent,
    retryable: bool,
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn run_router(
    mut receiver: mpsc::Receiver<RouterCommand>,
    mut control_receiver: mpsc::Receiver<RouterControl>,
    store: StoreHandle,
    tenant: TenantNamespace,
    policy: AccessPolicy,
    settings: RouterSettings,
    active_turns: Arc<Semaphore>,
    mut supervisor: SupervisorHandle,
    sink: Arc<dyn DurableReplySink>,
    attachments: Option<Arc<AttachmentCache>>,
    contexts: Option<Arc<ContextRegistry>>,
    quote_resolver: Option<Arc<dyn QuoteResolver>>,
    snapshot: Arc<RwLock<RouterSnapshot>>,
) -> Result<(), RouteError> {
    let (supervisor_tx, supervisor_rx) = watch::channel(supervisor_access(&supervisor));
    let mut actors = HashMap::<String, ScopeActorHandle>::new();
    let retry_budget = Arc::new(Semaphore::new(ROUTER_RETRY_BYTE_BUDGET));
    let mut retries = VecDeque::<RouterRetry>::new();
    let mut retry_tick = interval(Duration::from_millis(250));
    retry_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let asr_configured = settings.asr.is_configured();
    let mut stale_sweep_task = Some(run_stale_sweep(
        crate::runtime::asr::StaleWorkspaceSweeper::for_private_root(),
        asr_configured,
    ));
    let mut stale_sweeper = crate::runtime::asr::StaleWorkspaceSweeper::for_private_root();
    let mut stale_sweep = interval(crate::runtime::asr::ASR_STALE_SWEEP_INTERVAL);
    stale_sweep.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // The startup round above replaces the interval's immediate first tick.
    stale_sweep.tick().await;
    let mut supervisor_open = true;
    let mut tool_task = start_context_tool_task(
        &supervisor,
        attachments.as_ref(),
        contexts.as_ref(),
        settings.asr.clone(),
    );
    loop {
        tokio::select! {
            biased;
            control = control_receiver.recv() => {
                let Some(control) = control else { break };
                match control {
                    RouterControl::Interrupt { scope_key, respond, .. } => {
                        let result = match actors.get(&scope_key) {
                            Some(actor) => actor.interrupt().await.map_err(|()| RouteError::ActorUnavailable),
                            None => Ok(InterruptOutcome::NoActiveTurn),
                        };
                        let _ = respond.send(result);
                    }
                    RouterControl::Snapshot { scope_key, respond, .. } => {
                        let result = actors.get(&scope_key).map(ScopeActorHandle::snapshot);
                        let _ = respond.send(result);
                    }
                }
            }
            state = supervisor.changed(), if supervisor_open => {
                if state.is_ok() {
                    supervisor_tx.send_replace(supervisor_access(&supervisor));
                    let current_epoch = supervisor.client().ok().map(|client| client.epoch());
                    if tool_task.as_ref().map(|task| task.epoch) != current_epoch {
                        if let Some(task) = tool_task.take() {
                            task.stop(settings.shutdown_cleanup_timeout).await;
                        }
                        tool_task = start_context_tool_task(
                            &supervisor,
                            attachments.as_ref(),
                            contexts.as_ref(),
                            settings.asr.clone(),
                        );
                    }
                } else {
                    if let Some(task) = tool_task.take() {
                        task.stop(settings.shutdown_cleanup_timeout).await;
                    }
                    supervisor_open = false;
                    supervisor_tx.send_replace(SupervisorAccess { epoch: 0, client: None });
                }
            }
            _ = retry_tick.tick(), if !retries.is_empty() => {
                retry_one(
                    &mut retries,
                    &store,
                    &tenant,
                    &policy,
                    &settings,
                    &supervisor_rx,
                    Arc::clone(&active_turns),
                    Arc::clone(&sink),
                    attachments.as_ref(),
                    contexts.as_ref(),
                    quote_resolver.as_ref(),
                    &mut actors,
                ).await;
                update_runtime_snapshot(
                    &snapshot, &actors, &receiver, &retries, &active_turns, &settings,
                );
            }
            completed = async {
                stale_sweep_task
                    .as_mut()
                    .expect("stale sweep task exists behind select guard")
                    .await
            }, if stale_sweep_task.is_some() => {
                stale_sweep_task = None;
                if let Ok((sweeper, result)) = completed {
                    stale_sweeper = sweeper;
                    if result.is_err() {
                        tracing::warn!("private ASR stale workspace sweep failed");
                    }
                } else {
                    tracing::warn!("private ASR stale workspace sweep task failed");
                    stale_sweeper = crate::runtime::asr::StaleWorkspaceSweeper::for_private_root();
                }
            }
            _ = stale_sweep.tick(), if stale_sweep_task.is_none() => {
                stale_sweep_task = Some(run_stale_sweep(stale_sweeper, asr_configured));
                stale_sweeper = crate::runtime::asr::StaleWorkspaceSweeper::for_private_root();
            }
            command = receiver.recv() => {
                let Some(command) = command else { break };
                match command {
                    RouterCommand::Route { event, respond, .. } => {
                        let result = match route_one(
                            &store,
                            &tenant,
                            &policy,
                            &settings,
                            &supervisor_rx,
                            Arc::clone(&active_turns),
                            Arc::clone(&sink),
                            attachments.as_ref(),
                            contexts.as_ref(),
                            quote_resolver.as_ref(),
                            &mut actors,
                            *event,
                        ).await {
                            Ok(()) => Ok(()),
                            Err(failure) if failure.retryable => {
                                let error = failure.error;
                                match enqueue_retry(
                                    &mut retries,
                                    &retry_budget,
                                    failure.event,
                                ) {
                                    Ok(()) => Ok(()),
                                    Err(()) => Err(error),
                                }
                            }
                            Err(failure) => Err(failure.error),
                        };
                        update_runtime_snapshot(
                            &snapshot, &actors, &receiver, &retries, &active_turns, &settings,
                        );
                        let _ = respond.send(result);
                    }
                    RouterCommand::Shutdown { respond } => {
                        shutdown_actors(actors).await;
                        if let Some(task) = tool_task.take() {
                            task.stop(settings.shutdown_cleanup_timeout).await;
                        }
                        finish_stale_sweep(stale_sweep_task).await;
                        supervisor.shutdown().await?;
                        reconcile_terminal_attachments(attachments.as_deref()).await?;
                        let _ = respond.send(());
                        return Ok(());
                    }
                }
            }
        }
    }
    shutdown_actors(actors).await;
    if let Some(task) = tool_task.take() {
        task.stop(settings.shutdown_cleanup_timeout).await;
    }
    finish_stale_sweep(stale_sweep_task).await;
    supervisor.shutdown().await?;
    reconcile_terminal_attachments(attachments.as_deref()).await?;
    Ok(())
}

type StaleSweepTask = JoinHandle<(
    crate::runtime::asr::StaleWorkspaceSweeper,
    Result<(), crate::runtime::asr::AsrError>,
)>;

fn run_stale_sweep(
    mut sweeper: crate::runtime::asr::StaleWorkspaceSweeper,
    asr_configured: bool,
) -> StaleSweepTask {
    tokio::task::spawn_blocking(move || {
        let result = if asr_configured {
            sweeper.sweep_once()
        } else {
            sweeper.sweep_existing_once()
        };
        (sweeper, result)
    })
}

async fn finish_stale_sweep(task: Option<StaleSweepTask>) {
    let Some(task) = task else { return };
    match task.await {
        Ok((_, Ok(()))) => {}
        Ok((_, Err(_))) => tracing::warn!("private ASR stale workspace sweep failed"),
        Err(_) => tracing::warn!("private ASR stale workspace sweep task failed"),
    }
}

#[allow(clippy::ref_option, clippy::too_many_arguments)]
fn start_context_tool_task(
    supervisor: &SupervisorHandle,
    attachments: Option<&Arc<AttachmentCache>>,
    contexts: Option<&Arc<ContextRegistry>>,
    asr: AsrSection,
) -> Option<ContextToolTask> {
    let attachments = attachments.map(Arc::clone)?;
    let contexts = contexts.map(Arc::clone)?;
    let client = supervisor.client().ok()?;
    let epoch = client.epoch();
    let mut events = client.take_control_events().ok()?;
    let shutdown = CancellationToken::new();
    let task_shutdown = shutdown.clone();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                () = task_shutdown.cancelled() => break,
                event = events.recv() => {
                    let Some(event) = event else { break };
                    match event {
                        ControlEvent::ServerRequest(request) => {
                            handle_server_request(
                                client.as_ref(),
                                request,
                                contexts.as_ref(),
                                attachments.as_ref(),
                                &asr,
                                &task_shutdown,
                            )
                            .await;
                        }
                        ControlEvent::ConnectionClosed(_) => break,
                        ControlEvent::ProtocolDrift
                        | ControlEvent::UnknownNotification { .. }
                        | ControlEvent::InvalidNotification { .. } => {}
                    }
                }
            }
        }
    });
    Some(ContextToolTask {
        epoch,
        shutdown,
        task,
    })
}

async fn reconcile_terminal_attachments(
    attachments: Option<&AttachmentCache>,
) -> Result<(), RouteError> {
    let Some(cache) = attachments else {
        return Ok(());
    };
    cache
        .reconcile()
        .await
        .map(|_| ())
        .map_err(|_| RouteError::Attachment)
}

#[allow(clippy::too_many_arguments)]
async fn retry_one(
    retries: &mut VecDeque<RouterRetry>,
    store: &StoreHandle,
    tenant: &TenantNamespace,
    policy: &AccessPolicy,
    settings: &RouterSettings,
    supervisor: &watch::Receiver<SupervisorAccess>,
    active_turns: Arc<Semaphore>,
    sink: Arc<dyn DurableReplySink>,
    attachments: Option<&Arc<AttachmentCache>>,
    contexts: Option<&Arc<ContextRegistry>>,
    quote_resolver: Option<&Arc<dyn QuoteResolver>>,
    actors: &mut HashMap<String, ScopeActorHandle>,
) {
    let Some(mut retry) = retries.pop_front() else {
        return;
    };
    match route_one(
        store,
        tenant,
        policy,
        settings,
        supervisor,
        active_turns,
        sink,
        attachments,
        contexts,
        quote_resolver,
        actors,
        retry.event,
    )
    .await
    {
        Err(failure) if failure.retryable => {
            retry.event = failure.event;
            retries.push_back(retry);
        }
        Ok(()) | Err(_) => {}
    }
}

#[allow(clippy::result_large_err, clippy::too_many_arguments, clippy::too_many_lines)]
async fn route_one(
    store: &StoreHandle,
    tenant: &TenantNamespace,
    policy: &AccessPolicy,
    settings: &RouterSettings,
    supervisor: &watch::Receiver<SupervisorAccess>,
    active_turns: Arc<Semaphore>,
    sink: Arc<dyn DurableReplySink>,
    attachments: Option<&Arc<AttachmentCache>>,
    contexts: Option<&Arc<ContextRegistry>>,
    quote_resolver: Option<&Arc<dyn QuoteResolver>>,
    actors: &mut HashMap<String, ScopeActorHandle>,
    queued: QueuedInboundEvent,
) -> Result<(), Box<RouteFailure>> {
    let key = InboundKey::new(tenant.clone(), queued.event.event_id.clone());
    if queued.event.chat_type != ChatMode::P2p && is_conversation_media(&queued.event.message_type)
    {
        return store
            .complete_received_without_turn(&key)
            .await
            .map(|_| ())
            .map_err(|_| {
                Box::new(RouteFailure {
                    error: RouteError::Store,
                    event: queued,
                    retryable: true,
                })
            });
    }
    let decision = policy.decide(&queued.event);
    if let Some(kind) = decision.rejection_kind() {
        return reject_with_notice(store, sink.as_ref(), &key, &queued.event, kind)
            .await
            .map_err(|error| {
                Box::new(RouteFailure {
                    error,
                    event: queued,
                    retryable: true,
                })
            });
    }
    let scope_key = queued.event.scope.to_string();
    if !actors.contains_key(&scope_key) {
        if actors.len() >= settings.max_scope_actors {
            let idle = actors
                .iter()
                .find_map(|(key, actor)| actor.is_idle_and_empty().then(|| key.clone()));
            if let Some(idle) = idle {
                if let Some(actor) = actors.remove(&idle) {
                    actor.shutdown().await;
                }
            } else {
                return reject_with_notice(
                    store,
                    sink.as_ref(),
                    &key,
                    &queued.event,
                    InboundRejectionKind::Overloaded,
                )
                .await
                .map_err(|error| {
                    Box::new(RouteFailure {
                        error,
                        event: queued,
                        retryable: true,
                    })
                });
            }
        }
        actors.insert(
            scope_key.clone(),
            ScopeActorHandle::spawn(
                queued.event.scope.clone(),
                store.clone(),
                policy.clone(),
                settings.clone(),
                supervisor.clone(),
                active_turns,
                Arc::clone(&sink),
                attachments.map(Arc::clone),
                contexts.map(Arc::clone),
                quote_resolver.map(Arc::clone),
            ),
        );
    }
    let Some(actor) = actors.get(&scope_key) else {
        return Err(Box::new(RouteFailure {
            error: RouteError::ActorUnavailable,
            event: queued,
            retryable: false,
        }));
    };
    let route = actor.try_route(key.clone(), queued);
    match route {
        Ok(()) => {
            tracing::debug!(
                queued_messages = actor.snapshot().queued_messages,
                "inbound event queued for scope"
            );
            Ok(())
        }
        Err(ActorRouteError::Capacity(queued)) => {
            let queued = *queued;
            reject_with_notice(
                store,
                sink.as_ref(),
                &key,
                &queued.event,
                InboundRejectionKind::Overloaded,
            )
            .await
            .map_err(|error| {
                Box::new(RouteFailure {
                    error,
                    event: queued,
                    retryable: true,
                })
            })
        }
        Err(ActorRouteError::Closed(queued)) => Err(Box::new(RouteFailure {
            error: RouteError::ActorUnavailable,
            event: *queued,
            retryable: false,
        })),
    }
}

fn is_conversation_media(message_type: &str) -> bool {
    matches!(message_type, "image" | "video" | "media" | "file" | "audio")
}

fn enqueue_retry(
    retries: &mut VecDeque<RouterRetry>,
    budget: &Arc<Semaphore>,
    event: QueuedInboundEvent,
) -> Result<(), ()> {
    if retries.len() >= ROUTER_RETRY_CAPACITY {
        return Err(());
    }
    let bytes = u32::try_from(event.permit.num_permits()).map_err(|_| ())?;
    let permit = budget
        .clone()
        .try_acquire_many_owned(bytes)
        .map_err(|_| ())?;
    retries.push_back(RouterRetry {
        event,
        _queue_permit: permit,
    });
    Ok(())
}

async fn reject_with_notice(
    store: &StoreHandle,
    sink: &dyn DurableReplySink,
    key: &InboundKey,
    event: &crate::lark::normalize::InboundEvent,
    reason: InboundRejectionKind,
) -> Result<(), RouteError> {
    let notice = sink.rejection_notice(event, reason)?;
    store
        .reject_received_and_enqueue_notice(key, reason, notice)
        .await?;
    tracing::info!(reason = reason.as_str(), "inbound event rejected by policy");
    Ok(())
}

fn supervisor_access(supervisor: &SupervisorHandle) -> SupervisorAccess {
    let epoch = match supervisor.state() {
        SupervisorState::Starting { epoch }
        | SupervisorState::Ready { epoch, .. }
        | SupervisorState::Backoff { epoch, .. } => epoch,
        SupervisorState::Degraded { .. } | SupervisorState::Stopped => 0,
    };
    SupervisorAccess {
        epoch,
        client: supervisor.client().ok(),
    }
}

async fn shutdown_actors(actors: HashMap<String, ScopeActorHandle>) {
    join_all(actors.into_values().map(ScopeActorHandle::shutdown)).await;
}

fn update_snapshot(
    snapshot: &RwLock<RouterSnapshot>,
    scope_count: usize,
    queued_commands: usize,
    active_turns: usize,
) {
    if let Ok(mut current) = snapshot.write() {
        *current = RouterSnapshot {
            scope_count,
            queued_commands,
            active_turns,
        };
    }
}

fn update_runtime_snapshot(
    snapshot: &RwLock<RouterSnapshot>,
    actors: &HashMap<String, ScopeActorHandle>,
    receiver: &mpsc::Receiver<RouterCommand>,
    retries: &VecDeque<RouterRetry>,
    active_turns: &Semaphore,
    settings: &RouterSettings,
) {
    update_snapshot(
        snapshot,
        actors.len(),
        receiver.len().saturating_add(retries.len()),
        settings
            .active_turn_permits
            .saturating_sub(active_turns.available_permits()),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lark::api::ChatMode;
    use crate::lark::normalize::{InboundEvent, ScopeKey};

    fn handle(sender: mpsc::Sender<RouterCommand>, byte_budget: usize) -> RouterHandle {
        let (control_sender, _control_receiver) = mpsc::channel(1);
        RouterHandle {
            sender,
            control_sender,
            byte_budget: Arc::new(Semaphore::new(byte_budget)),
            control_byte_budget: Arc::new(Semaphore::new(1)),
            snapshot: Arc::new(RwLock::new(RouterSnapshot::default())),
            active_turns: Arc::new(Semaphore::new(1)),
            active_turn_capacity: 1,
            task: None,
        }
    }

    async fn queued(event_id: &str) -> QueuedInboundEvent {
        let permit = Arc::new(Semaphore::new(1))
            .acquire_owned()
            .await
            .expect("permit");
        QueuedInboundEvent::new(
            InboundEvent {
                event_id: event_id.to_owned(),
                message_id: format!("message-{event_id}"),
                chat_id: "chat-router-attempt".to_owned(),
                sender_id: "owner-router-attempt".to_owned(),
                chat_type: ChatMode::P2p,
                thread_id: None,
                root_id: None,
                reply_to_message_id: None,
                text: "hello".to_owned(),
                mentions_bot: false,
                mention_all: false,
                sender_is_human: true,
                mentions: Vec::new(),
                parts: Vec::new(),
                resources: Vec::new(),
                message_type: "text".to_owned(),
                create_time_ms: 1,
                scope: ScopeKey::Chat("chat-router-attempt".to_owned()),
            },
            permit,
        )
    }

    #[tokio::test]
    async fn recoverable_route_returns_event_when_byte_budget_is_full() {
        let (sender, _receiver) = mpsc::channel(1);
        let handle = handle(sender, 0);

        let failure = handle
            .route_recoverable(queued("capacity").await)
            .await
            .expect_err("capacity");
        let (error, event) = failure.into_parts();

        assert!(matches!(error, RouteError::Capacity));
        assert_eq!(event.expect("retained event").event.event_id, "capacity");
    }

    #[tokio::test]
    async fn recoverable_route_returns_event_when_router_channel_is_closed() {
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);
        let handle = handle(sender, 1);

        let failure = handle
            .route_recoverable(queued("closed").await)
            .await
            .expect_err("closed");
        let (error, event) = failure.into_parts();

        assert!(matches!(error, RouteError::Closed));
        assert_eq!(event.expect("retained event").event.event_id, "closed");
    }
}
