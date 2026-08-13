//! Bounded tenant-scoped routing into one actor per Lark scope.

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::codex::supervisor::{SupervisorError, SupervisorHandle, SupervisorState};
use crate::codex::types::{ApprovalPolicy, SandboxMode};
use crate::config::BridgeConfig;
use crate::lark::bridge::QueuedInboundEvent;
use crate::limits::{
    ROUTER_ACTIVE_TURN_HARD_LIMIT, ROUTER_COMMAND_BYTE_BUDGET, ROUTER_COMMAND_CAPACITY,
    ROUTER_SCOPE_ACTOR_HARD_LIMIT,
};
use crate::runtime::intake::TenantNamespace;
use crate::runtime::policy::{AccessDecision, AccessPolicy};
use crate::runtime::scope::{
    ActorRouteError, DurableReplySink, ReplySinkError, ScopeActorHandle, SupervisorAccess,
};
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
        }
    }

    fn validate(&self) -> Result<(), RouteError> {
        if self.active_turn_permits == 0
            || self.active_turn_permits > ROUTER_ACTIVE_TURN_HARD_LIMIT
            || self.max_scope_actors == 0
            || self.max_scope_actors > ROUTER_SCOPE_ACTOR_HARD_LIMIT
            || self.debounce.is_zero()
            || self.message_max_age.is_zero()
            || self.finalization_retry.is_zero()
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
        if let Err(error) = settings.validate() {
            supervisor.shutdown().await?;
            return Err(error);
        }
        let (sender, receiver) = mpsc::channel(ROUTER_COMMAND_CAPACITY);
        let snapshot = Arc::new(RwLock::new(RouterSnapshot::default()));
        let task_snapshot = Arc::clone(&snapshot);
        let task = tokio::spawn(run_router(
            receiver,
            store,
            tenant,
            policy,
            settings,
            supervisor,
            sink,
            task_snapshot,
        ));
        Ok(RouterHandle {
            sender,
            byte_budget: Arc::new(Semaphore::new(ROUTER_COMMAND_BYTE_BUDGET)),
            snapshot,
            task: Some(task),
        })
    }
}

/// Client handle for routing and orderly shutdown.
pub struct RouterHandle {
    sender: mpsc::Sender<RouterCommand>,
    byte_budget: Arc<Semaphore>,
    snapshot: Arc<RwLock<RouterSnapshot>>,
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
        let bytes = event.permit.num_permits();
        let bytes = u32::try_from(bytes).map_err(|_| RouteError::Capacity)?;
        let queue_permit = self
            .byte_budget
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|_| RouteError::Capacity)?;
        let (respond, wait) = oneshot::channel();
        self.sender
            .try_send(RouterCommand::Route {
                event: Box::new(event),
                _queue_permit: queue_permit,
                respond,
            })
            .map_err(|_| RouteError::Capacity)?;
        wait.await.map_err(|_| RouteError::Closed)?
    }

    /// Returns the latest bounded structural snapshot.
    #[must_use]
    pub fn snapshot(&self) -> RouterSnapshot {
        self.snapshot
            .read()
            .map_or_else(|_| RouterSnapshot::default(), |snapshot| *snapshot)
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

#[allow(clippy::too_many_arguments)]
async fn run_router(
    mut receiver: mpsc::Receiver<RouterCommand>,
    store: StoreHandle,
    tenant: TenantNamespace,
    policy: AccessPolicy,
    settings: RouterSettings,
    mut supervisor: SupervisorHandle,
    sink: Arc<dyn DurableReplySink>,
    snapshot: Arc<RwLock<RouterSnapshot>>,
) -> Result<(), RouteError> {
    let active_turns = Arc::new(Semaphore::new(settings.active_turn_permits));
    let (supervisor_tx, supervisor_rx) = watch::channel(supervisor_access(&supervisor));
    let mut actors = HashMap::<String, ScopeActorHandle>::new();
    let mut supervisor_open = true;
    loop {
        tokio::select! {
            state = supervisor.changed(), if supervisor_open => {
                if state.is_ok() {
                    supervisor_tx.send_replace(supervisor_access(&supervisor));
                } else {
                    supervisor_open = false;
                    supervisor_tx.send_replace(SupervisorAccess { epoch: 0, client: None });
                }
            }
            command = receiver.recv() => {
                let Some(command) = command else { break };
                match command {
                    RouterCommand::Route { event, respond, .. } => {
                        let result = route_one(
                            &store,
                            &tenant,
                            &policy,
                            &settings,
                            &supervisor_rx,
                            Arc::clone(&active_turns),
                            Arc::clone(&sink),
                            &mut actors,
                            *event,
                        ).await;
                        update_snapshot(
                            &snapshot,
                            actors.len(),
                            receiver.len(),
                            settings.active_turn_permits.saturating_sub(active_turns.available_permits()),
                        );
                        let _ = respond.send(result);
                    }
                    RouterCommand::Shutdown { respond } => {
                        shutdown_actors(actors).await;
                        supervisor.shutdown().await?;
                        let _ = respond.send(());
                        return Ok(());
                    }
                }
            }
        }
    }
    shutdown_actors(actors).await;
    supervisor.shutdown().await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn route_one(
    store: &StoreHandle,
    tenant: &TenantNamespace,
    policy: &AccessPolicy,
    settings: &RouterSettings,
    supervisor: &watch::Receiver<SupervisorAccess>,
    active_turns: Arc<Semaphore>,
    sink: Arc<dyn DurableReplySink>,
    actors: &mut HashMap<String, ScopeActorHandle>,
    queued: QueuedInboundEvent,
) -> Result<(), RouteError> {
    let decision = policy.decide(&queued.event);
    let key = InboundKey::new(tenant.clone(), queued.event.event_id.clone());
    if decision != AccessDecision::Allow {
        return reject_with_notice(
            store,
            sink.as_ref(),
            &key,
            &queued.event,
            InboundRejectionKind::Policy,
        )
        .await;
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
                .await;
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
            ),
        );
    }
    let event = queued.event.clone();
    let route = actors
        .get(&scope_key)
        .ok_or(RouteError::ActorUnavailable)?
        .try_route(key.clone(), queued);
    match route {
        Ok(()) => Ok(()),
        Err(ActorRouteError::Capacity) => {
            reject_with_notice(
                store,
                sink.as_ref(),
                &key,
                &event,
                InboundRejectionKind::Overloaded,
            )
            .await
        }
        Err(ActorRouteError::Closed) => Err(RouteError::ActorUnavailable),
    }
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
    for actor in actors.into_values() {
        actor.shutdown().await;
    }
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
