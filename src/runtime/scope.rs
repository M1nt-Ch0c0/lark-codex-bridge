//! One-scope runtime contracts shared by the router and reply projector.

use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::future::BoxFuture;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, sleep_until};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::codex::client::{AppServerClient, AppServerEvent, ThreadId, TurnId, TurnOutcome};
use crate::codex::types::{
    SandboxMode, ThreadResumeParams, ThreadStartParams, TurnSandboxPolicy, TurnStartParams,
    TurnStatus, UserInput,
};
use crate::lark::bridge::QueuedInboundEvent;
use crate::lark::normalize::{InboundEvent, ScopeKey};
use crate::limits::{
    SCOPE_MAILBOX_BYTE_BUDGET, SCOPE_MAILBOX_CAPACITY, TURN_BATCH_MAX_MESSAGES,
    TURN_BATCH_TEXT_BYTE_BUDGET,
};
use crate::runtime::policy::AccessPolicy;
use crate::runtime::router::RouterSettings;
use crate::store::{
    BeginTurnOutcome, InboundKey, InboundRejectionKind, InboundTerminal, NewOutboxRow, NewTurnRow,
    StoreHandle, TurnResolution, TurnState,
};

/// Static, content-free failure from the durable reply projection boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ReplySinkError {
    /// The durable outbox is temporarily unavailable.
    #[error("the durable reply sink is temporarily unavailable")]
    Unavailable,
    /// A bounded reply collection cannot accept more work.
    #[error("the durable reply sink is at capacity")]
    Capacity,
    /// The requested projection violates a closed invariant.
    #[error("the durable reply projection is invalid")]
    Invariant,
}

/// Minimal Lark routing metadata retained after prompt assembly.
#[derive(Clone, PartialEq, Eq)]
pub struct TurnSource {
    /// Canonical inbound event ID.
    pub event_id: String,
    /// Message that should receive the projected reply.
    pub message_id: String,
    /// Chat containing the message.
    pub chat_id: String,
    /// Topic thread, when the message belongs to one.
    pub thread_id: Option<String>,
}

impl fmt::Debug for TurnSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TurnSource")
            .field("event_id_len", &self.event_id.len())
            .field("message_id_len", &self.message_id.len())
            .field("chat_id_len", &self.chat_id.len())
            .field("has_thread", &self.thread_id.is_some())
            .finish()
    }
}

/// Authoritative turn result whose outbound effects must become durable first.
pub struct TurnFinalization {
    /// Store row resolved only after the sink succeeds.
    pub turn_row_id: i64,
    /// Redacted-by-Debug owning scope key.
    pub scope_key: String,
    /// Original Lark reply targets, bounded by the turn batch limit.
    pub sources: Vec<TurnSource>,
    /// Deterministic store resolution selected by the actor.
    pub resolution: TurnResolution,
    /// Authoritative Codex terminal outcome; absent only for uncertainty.
    pub outcome: Option<TurnOutcome>,
}

impl fmt::Debug for TurnFinalization {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TurnFinalization")
            .field("turn_row_id", &self.turn_row_id)
            .field("scope_key_len", &self.scope_key.len())
            .field("source_count", &self.sources.len())
            .field("resolution", &self.resolution)
            .field("has_outcome", &self.outcome.is_some())
            .finish()
    }
}

/// Durable outbound boundary used by the scope runtime.
///
/// Rejection notices are returned to the router so the store can atomically
/// enqueue them with the inbound rejection. Turn finalization futures must
/// persist every deterministic outbox row before returning success.
pub trait DurableReplySink: Send + Sync {
    /// Builds one deterministic notice without performing I/O.
    ///
    /// # Errors
    ///
    /// Returns a static classification when the event cannot be projected.
    fn rejection_notice(
        &self,
        event: &InboundEvent,
        reason: InboundRejectionKind,
    ) -> Result<NewOutboxRow, ReplySinkError>;

    /// Persists the terminal reply effects before the caller resolves store state.
    fn finalize(&self, turn: TurnFinalization) -> BoxFuture<'static, Result<(), ReplySinkError>>;
}

/// Observable per-scope state. Payload and filesystem details are never held.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScopeState {
    Idle,
    Debouncing,
    WaitingPermit,
    StartingTurn,
    Running { turn_row_id: i64 },
    Finalizing { turn_row_id: i64 },
    Failed { kind: ScopeFailureKind },
}

/// Result of a high-priority interruption request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InterruptOutcome {
    /// The app-server accepted an interrupt for the active turn. The actor
    /// still waits for the authoritative `turn/completed` notification.
    Requested,
    /// The scope has no active Codex turn.
    NoActiveTurn,
}

/// Redacted per-scope diagnostics safe for `/status` assembly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScopeSnapshot {
    /// Current actor state without message or filesystem contents.
    pub state: ScopeState,
    /// Inbound messages waiting in the actor mailbox; the item currently
    /// executing is represented by `state` rather than this count.
    pub queued_messages: usize,
}

/// Static scope failure category safe for snapshots and logs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScopeFailureKind {
    Store,
    Policy,
    Supervisor,
    Projection,
    Client,
    Capacity,
}

#[derive(Clone)]
pub(crate) struct SupervisorAccess {
    pub(crate) epoch: u64,
    pub(crate) client: Option<Arc<AppServerClient>>,
}

impl fmt::Debug for SupervisorAccess {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SupervisorAccess")
            .field("epoch", &self.epoch)
            .field("ready", &self.client.is_some())
            .finish()
    }
}

pub(crate) struct ActorInbound {
    pub(crate) key: InboundKey,
    pub(crate) queued: QueuedInboundEvent,
    pub(crate) _mailbox_permit: OwnedSemaphorePermit,
}

enum ScopeCommand {
    Inbound(Box<ActorInbound>),
}

pub(crate) struct ScopeActorHandle {
    scope: ScopeKey,
    sender: mpsc::Sender<ScopeCommand>,
    budget: Arc<Semaphore>,
    state: Arc<RwLock<ScopeState>>,
    active_turn: Arc<RwLock<Option<ActiveTurn>>>,
    store: StoreHandle,
    supervisor: watch::Receiver<SupervisorAccess>,
    shutdown: CancellationToken,
    join: Option<JoinHandle<()>>,
}

impl ScopeActorHandle {
    pub(crate) fn spawn(
        scope: ScopeKey,
        store: StoreHandle,
        policy: AccessPolicy,
        settings: RouterSettings,
        supervisor: watch::Receiver<SupervisorAccess>,
        active_turns: Arc<Semaphore>,
        sink: Arc<dyn DurableReplySink>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(SCOPE_MAILBOX_CAPACITY);
        let state = Arc::new(RwLock::new(ScopeState::Idle));
        let task_state = Arc::clone(&state);
        let active_turn = Arc::new(RwLock::new(None));
        let task_active_turn = Arc::clone(&active_turn);
        let shutdown = CancellationToken::new();
        let join = tokio::spawn(run_scope_actor(
            scope.clone(),
            receiver,
            store.clone(),
            policy,
            settings,
            supervisor.clone(),
            active_turns,
            sink,
            task_state,
            task_active_turn,
            shutdown.clone(),
        ));
        Self {
            scope,
            sender,
            budget: Arc::new(Semaphore::new(SCOPE_MAILBOX_BYTE_BUDGET)),
            state,
            active_turn,
            store,
            supervisor,
            shutdown,
            join: Some(join),
        }
    }

    pub(crate) fn try_route(
        &self,
        key: InboundKey,
        queued: QueuedInboundEvent,
    ) -> Result<(), ActorRouteError> {
        let bytes =
            u32::try_from(queued.permit.num_permits()).map_err(|_| ActorRouteError::Capacity)?;
        let permit = self
            .budget
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|_| ActorRouteError::Capacity)?;
        self.sender
            .try_send(ScopeCommand::Inbound(Box::new(ActorInbound {
                key,
                queued,
                _mailbox_permit: permit,
            })))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => ActorRouteError::Capacity,
                mpsc::error::TrySendError::Closed(_) => ActorRouteError::Closed,
            })
    }

    pub(crate) fn state(&self) -> ScopeState {
        self.state.read().map_or(
            ScopeState::Failed {
                kind: ScopeFailureKind::Client,
            },
            |state| *state,
        )
    }

    pub(crate) fn is_idle_and_empty(&self) -> bool {
        self.state() == ScopeState::Idle && self.sender.capacity() == SCOPE_MAILBOX_CAPACITY
    }

    pub(crate) fn snapshot(&self) -> ScopeSnapshot {
        ScopeSnapshot {
            state: self.state(),
            queued_messages: self.sender.max_capacity() - self.sender.capacity(),
        }
    }

    pub(crate) async fn interrupt(&self) -> Result<InterruptOutcome, ()> {
        let active = self.active_turn.read().map_err(|_| ())?.clone();
        let Some(active) = active else {
            return Ok(InterruptOutcome::NoActiveTurn);
        };
        active
            .client
            .interrupt_turn(&active.thread_id, &active.turn_id)
            .await
            .map_err(|_| ())?;
        Ok(InterruptOutcome::Requested)
    }

    pub(crate) async fn shutdown(mut self) {
        self.shutdown.cancel();
        if let Some(join) = self.join.take() {
            let _ = join.await;
        }
        release_thread_route(&self.scope, &self.store, &self.supervisor).await;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ActorRouteError {
    Capacity,
    Closed,
}

#[derive(Clone)]
struct ActiveTurn {
    client: Arc<AppServerClient>,
    thread_id: ThreadId,
    turn_id: TurnId,
}

#[allow(clippy::too_many_arguments)]
async fn run_scope_actor(
    scope: ScopeKey,
    mut receiver: mpsc::Receiver<ScopeCommand>,
    store: StoreHandle,
    policy: AccessPolicy,
    settings: RouterSettings,
    supervisor: watch::Receiver<SupervisorAccess>,
    active_turns: Arc<Semaphore>,
    sink: Arc<dyn DurableReplySink>,
    state: Arc<RwLock<ScopeState>>,
    active_turn: Arc<RwLock<Option<ActiveTurn>>>,
    shutdown: CancellationToken,
) {
    let mut deferred = None;
    'actor: loop {
        let command = if let Some(deferred) = deferred.take() {
            Some(ScopeCommand::Inbound(deferred))
        } else {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                command = receiver.recv() => command,
            }
        };
        let Some(command) = command else { break };
        match command {
            ScopeCommand::Inbound(first) => {
                let mut batch = vec![*first];
                let mut text_bytes = batch[0].queued.event.text.len();
                set_state(&state, ScopeState::Debouncing);
                let deadline = Instant::now() + settings.debounce;
                loop {
                    if batch.len() >= TURN_BATCH_MAX_MESSAGES
                        || text_bytes >= TURN_BATCH_TEXT_BYTE_BUDGET
                    {
                        break;
                    }
                    tokio::select! {
                        biased;
                        () = shutdown.cancelled() => break 'actor,
                        () = sleep_until(deadline) => break,
                        command = receiver.recv() => match command {
                            Some(ScopeCommand::Inbound(next)) => {
                                let next_bytes = next.queued.event.text.len();
                                if text_bytes.saturating_add(next_bytes)
                                    > TURN_BATCH_TEXT_BYTE_BUDGET
                                {
                                    deferred = Some(next);
                                    break;
                                }
                                text_bytes = text_bytes.saturating_add(next_bytes);
                                batch.push(*next);
                            }
                            None => return,
                        }
                    }
                }
                set_state(&state, ScopeState::WaitingPermit);
                let result = process_batch(
                    &scope,
                    batch,
                    &store,
                    &policy,
                    &settings,
                    supervisor.clone(),
                    Arc::clone(&active_turns),
                    Arc::clone(&sink),
                    &state,
                    &active_turn,
                    &shutdown,
                )
                .await;
                if shutdown.is_cancelled() {
                    break;
                }
                if let Err(kind) = result {
                    set_state(&state, ScopeState::Failed { kind });
                } else {
                    set_state(&state, ScopeState::Idle);
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn process_batch(
    scope: &ScopeKey,
    batch: Vec<ActorInbound>,
    store: &StoreHandle,
    policy: &AccessPolicy,
    settings: &RouterSettings,
    mut supervisor: watch::Receiver<SupervisorAccess>,
    active_turns: Arc<Semaphore>,
    sink: Arc<dyn DurableReplySink>,
    state: &Arc<RwLock<ScopeState>>,
    active_turn: &RwLock<Option<ActiveTurn>>,
    shutdown: &CancellationToken,
) -> Result<(), ScopeFailureKind> {
    let batch = deduplicate_batch(batch);
    let _active_permit = tokio::select! {
        biased;
        () = shutdown.cancelled() => return Err(ScopeFailureKind::Supervisor),
        permit = active_turns.acquire_owned() => {
            permit.map_err(|_| ScopeFailureKind::Capacity)?
        }
    };
    let mut eligible = Vec::with_capacity(batch.len());
    for item in batch {
        let reason = if item.queued.event.text.len() > TURN_BATCH_TEXT_BYTE_BUDGET {
            Some(InboundRejectionKind::Overloaded)
        } else if is_stale(&item.queued.event, settings.message_max_age) {
            Some(InboundRejectionKind::Stale)
        } else if policy.decide(&item.queued.event) != crate::runtime::policy::AccessDecision::Allow
        {
            Some(InboundRejectionKind::Policy)
        } else {
            None
        };
        if let Some(reason) = reason {
            reject_item(store, sink.as_ref(), &item, reason).await?;
        } else {
            eligible.push(item);
        }
    }
    if eligible.is_empty() {
        return Ok(());
    }
    let batch = eligible;
    let (cwd, fingerprint) = match prepare_workspace(scope, store, policy, settings).await {
        Ok(workspace) => workspace,
        Err(ScopeFailureKind::Policy) => {
            for item in &batch {
                reject_item(store, sink.as_ref(), item, InboundRejectionKind::Policy).await?;
            }
            return Ok(());
        }
        Err(kind) => return Err(kind),
    };
    let client = wait_for_client(&mut supervisor, shutdown).await?;
    set_state(state, ScopeState::StartingTurn);
    let thread_id = tokio::select! {
        biased;
        () = shutdown.cancelled() => return Err(ScopeFailureKind::Supervisor),
        result = ensure_thread(scope, store, policy, settings, &client, &cwd, &fingerprint) => {
            result?
        }
    };
    let mut subscription = tokio::select! {
        biased;
        () = shutdown.cancelled() => return Err(ScopeFailureKind::Supervisor),
        result = client.subscribe(thread_id.as_str().into()) => {
            result.map_err(|_| ScopeFailureKind::Client)?
        }
    };
    let client_message_id = Uuid::new_v4().to_string();
    let keys = batch
        .iter()
        .map(|item| item.key.clone())
        .collect::<Vec<_>>();
    let begun = store
        .begin_turn_and_claim_inbound(
            NewTurnRow {
                scope_key: scope.to_string(),
                client_message_id: client_message_id.clone(),
                codex_thread_id: Some(thread_id.clone()),
                state: TurnState::Starting,
            },
            &keys,
        )
        .await
        .map_err(|_| ScopeFailureKind::Store)?;
    let BeginTurnOutcome::Started {
        turn_row_id,
        claimed,
        ..
    } = begun
    else {
        return Ok(());
    };
    let inputs = claimed
        .iter()
        .map(|claimed| UserInput::text(claimed.retained.event().text.clone()))
        .collect::<Vec<_>>();
    let sources = claimed
        .iter()
        .map(|claimed| TurnSource {
            event_id: claimed.retained.event().event_id.clone(),
            message_id: claimed.retained.event().message_id.clone(),
            chat_id: claimed.retained.event().chat_id.clone(),
            thread_id: claimed.retained.event().thread_id.clone(),
        })
        .collect::<Vec<_>>();
    let rpc_cwd = revalidate_workspace(policy, &cwd, &fingerprint)?;
    let mut params = TurnStartParams::new(&thread_id, inputs);
    params.client_user_message_id = Some(client_message_id);
    params.cwd = Some(rpc_cwd.clone());
    params.approval_policy = Some(settings.approval_policy.clone());
    params.model.clone_from(&settings.model);
    params.sandbox_policy = Some(turn_sandbox(settings, rpc_cwd));
    let started = tokio::select! {
        biased;
        () = shutdown.cancelled() => None,
        result = client.start_turn(params) => result.ok(),
    };
    let Some(started) = started else {
        finalize_uncertain(
            store,
            sink.as_ref(),
            settings,
            turn_row_id,
            scope,
            sources,
            shutdown,
        )
        .await?;
        return Ok(());
    };
    if store
        .set_turn_state(turn_row_id, TurnState::Running, Some(&started.id))
        .await
        .is_err()
    {
        finalize_uncertain(
            store,
            sink.as_ref(),
            settings,
            turn_row_id,
            scope,
            sources,
            shutdown,
        )
        .await?;
        return Ok(());
    }
    set_state(state, ScopeState::Running { turn_row_id });
    set_active_turn(
        active_turn,
        Some(ActiveTurn {
            client: Arc::clone(&client),
            thread_id: ThreadId::from(thread_id.as_str()),
            turn_id: TurnId::from(started.id.as_str()),
        }),
    )?;
    let outcome = loop {
        let event = tokio::select! {
            biased;
            () = shutdown.cancelled() => None,
            event = subscription.recv() => event,
        };
        match event {
            Some(AppServerEvent::TurnCompleted(outcome))
                if outcome.turn_id.as_str() == started.id =>
            {
                break Some(outcome);
            }
            Some(AppServerEvent::ConnectionClosed { .. }) | None => break None,
            _ => {}
        }
    };
    set_active_turn(active_turn, None)?;
    set_state(state, ScopeState::Finalizing { turn_row_id });
    let Some(outcome) = outcome else {
        finalize_uncertain(
            store,
            sink.as_ref(),
            settings,
            turn_row_id,
            scope,
            sources,
            shutdown,
        )
        .await?;
        return Ok(());
    };
    let (resolution, inbound) = resolution_for(&outcome.status);
    persist_finalization(
        sink.as_ref(),
        settings,
        TurnFinalization {
            turn_row_id,
            scope_key: scope.to_string(),
            sources,
            resolution,
            outcome: Some(outcome),
        },
        shutdown,
    )
    .await?;
    store
        .resolve_turn_and_finish_inbound_batch(turn_row_id, resolution, inbound)
        .await
        .map_err(|_| ScopeFailureKind::Store)?;
    Ok(())
}

fn deduplicate_batch(batch: Vec<ActorInbound>) -> Vec<ActorInbound> {
    let mut unique = HashSet::new();
    let mut retained = Vec::new();
    for item in batch {
        if !unique.insert(item.key.clone()) {
            continue;
        }
        retained.push(item);
    }
    retained
}

fn is_stale(event: &InboundEvent, max_age: std::time::Duration) -> bool {
    let now = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX);
    let max_age = i64::try_from(max_age.as_millis()).unwrap_or(i64::MAX);
    now.saturating_sub(event.create_time_ms) > max_age
}

async fn reject_item(
    store: &StoreHandle,
    sink: &dyn DurableReplySink,
    item: &ActorInbound,
    reason: InboundRejectionKind,
) -> Result<(), ScopeFailureKind> {
    let notice = sink
        .rejection_notice(&item.queued.event, reason)
        .map_err(|_| ScopeFailureKind::Projection)?;
    store
        .reject_received_and_enqueue_notice(&item.key, reason, notice)
        .await
        .map_err(|_| ScopeFailureKind::Store)?;
    Ok(())
}

async fn prepare_workspace(
    scope: &ScopeKey,
    store: &StoreHandle,
    policy: &AccessPolicy,
    settings: &RouterSettings,
) -> Result<(PathBuf, String), ScopeFailureKind> {
    if let Some(row) = store
        .scope_row(scope)
        .await
        .map_err(|_| ScopeFailureKind::Store)?
    {
        let canonical = policy
            .validate_workspace(&row.cwd)
            .map_err(|_| ScopeFailureKind::Policy)?;
        if canonical != row.cwd {
            return Err(ScopeFailureKind::Policy);
        }
        let fingerprint = policy
            .fingerprint(&canonical)
            .map_err(|_| ScopeFailureKind::Policy)?;
        if fingerprint.as_str() != row.policy_fingerprint {
            store
                .archive_active_thread(scope)
                .await
                .map_err(|_| ScopeFailureKind::Store)?;
            store
                .upsert_scope(scope, &canonical, fingerprint.as_str())
                .await
                .map_err(|_| ScopeFailureKind::Store)?;
        }
        return Ok((canonical, fingerprint.as_str().to_owned()));
    }
    let cwd = settings
        .default_workspace
        .as_deref()
        .ok_or(ScopeFailureKind::Policy)?;
    let canonical = policy
        .validate_workspace(cwd)
        .map_err(|_| ScopeFailureKind::Policy)?;
    let fingerprint = policy
        .fingerprint(&canonical)
        .map_err(|_| ScopeFailureKind::Policy)?;
    store
        .upsert_scope(scope, &canonical, fingerprint.as_str())
        .await
        .map_err(|_| ScopeFailureKind::Store)?;
    Ok((canonical, fingerprint.as_str().to_owned()))
}

async fn wait_for_client(
    supervisor: &mut watch::Receiver<SupervisorAccess>,
    shutdown: &CancellationToken,
) -> Result<Arc<AppServerClient>, ScopeFailureKind> {
    loop {
        if let Some(client) = supervisor.borrow().client.clone() {
            return Ok(client);
        }
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return Err(ScopeFailureKind::Supervisor),
            changed = supervisor.changed() => {
                changed.map_err(|_| ScopeFailureKind::Supervisor)?;
            }
        }
    }
}

async fn release_thread_route(
    scope: &ScopeKey,
    store: &StoreHandle,
    supervisor: &watch::Receiver<SupervisorAccess>,
) {
    let Ok(Some(active)) = store.active_thread(scope).await else {
        return;
    };
    let client = supervisor.borrow().client.clone();
    if let Some(client) = client {
        let _ = client
            .release_thread(&ThreadId::from(active.codex_thread_id))
            .await;
    }
}

async fn ensure_thread(
    scope: &ScopeKey,
    store: &StoreHandle,
    policy: &AccessPolicy,
    settings: &RouterSettings,
    client: &AppServerClient,
    cwd: &Path,
    fingerprint: &str,
) -> Result<String, ScopeFailureKind> {
    if let Some(active) = store
        .active_thread(scope)
        .await
        .map_err(|_| ScopeFailureKind::Store)?
    {
        let rpc_cwd = revalidate_workspace(policy, cwd, fingerprint)?;
        let mut params = ThreadResumeParams::new(&active.codex_thread_id);
        params.overrides.cwd = Some(rpc_cwd);
        params.overrides.sandbox = Some(settings.sandbox);
        params.overrides.approval_policy = Some(settings.approval_policy.clone());
        params.overrides.model.clone_from(&settings.model);
        let thread = client
            .resume_thread(params)
            .await
            .map_err(|_| ScopeFailureKind::Client)?;
        return Ok(thread.id);
    }
    let rpc_cwd = revalidate_workspace(policy, cwd, fingerprint)?;
    let params = ThreadStartParams {
        cwd: Some(rpc_cwd),
        sandbox: Some(settings.sandbox),
        approval_policy: Some(settings.approval_policy.clone()),
        model: settings.model.clone(),
        ..ThreadStartParams::default()
    };
    let thread = client
        .start_thread(params)
        .await
        .map_err(|_| ScopeFailureKind::Client)?;
    store
        .record_active_thread(scope, &thread.id)
        .await
        .map_err(|_| ScopeFailureKind::Store)?;
    Ok(thread.id)
}

fn revalidate_workspace(
    policy: &AccessPolicy,
    cwd: &Path,
    fingerprint: &str,
) -> Result<PathBuf, ScopeFailureKind> {
    let canonical = policy
        .validate_workspace(cwd)
        .map_err(|_| ScopeFailureKind::Policy)?;
    if canonical != cwd {
        return Err(ScopeFailureKind::Policy);
    }
    let current = policy
        .fingerprint(&canonical)
        .map_err(|_| ScopeFailureKind::Policy)?;
    if current.as_str() != fingerprint {
        return Err(ScopeFailureKind::Policy);
    }
    Ok(canonical)
}

fn turn_sandbox(settings: &RouterSettings, cwd: PathBuf) -> TurnSandboxPolicy {
    match settings.sandbox {
        SandboxMode::ReadOnly => TurnSandboxPolicy::ReadOnly {
            network_access: settings.network_access,
        },
        SandboxMode::WorkspaceWrite => TurnSandboxPolicy::WorkspaceWrite {
            writable_roots: vec![cwd],
            network_access: settings.network_access,
            exclude_slash_tmp: false,
            exclude_tmpdir_env_var: false,
        },
        SandboxMode::DangerFullAccess => TurnSandboxPolicy::DangerFullAccess,
    }
}

fn resolution_for(status: &TurnStatus) -> (TurnResolution, InboundTerminal) {
    match status {
        TurnStatus::Completed => (TurnResolution::Completed, InboundTerminal::Completed),
        TurnStatus::Interrupted => (TurnResolution::Interrupted, InboundTerminal::Rejected),
        TurnStatus::Failed => (TurnResolution::Failed, InboundTerminal::Rejected),
        TurnStatus::InProgress | TurnStatus::Unknown(_) => {
            (TurnResolution::Uncertain, InboundTerminal::Rejected)
        }
    }
}

async fn finalize_uncertain(
    store: &StoreHandle,
    sink: &dyn DurableReplySink,
    settings: &RouterSettings,
    turn_row_id: i64,
    scope: &ScopeKey,
    sources: Vec<TurnSource>,
    shutdown: &CancellationToken,
) -> Result<(), ScopeFailureKind> {
    persist_finalization(
        sink,
        settings,
        TurnFinalization {
            turn_row_id,
            scope_key: scope.to_string(),
            sources,
            resolution: TurnResolution::Uncertain,
            outcome: None,
        },
        shutdown,
    )
    .await?;
    store
        .resolve_turn_and_finish_inbound_batch(
            turn_row_id,
            TurnResolution::Uncertain,
            InboundTerminal::Rejected,
        )
        .await
        .map_err(|_| ScopeFailureKind::Store)?;
    Ok(())
}

async fn persist_finalization(
    sink: &dyn DurableReplySink,
    settings: &RouterSettings,
    finalization: TurnFinalization,
    shutdown: &CancellationToken,
) -> Result<(), ScopeFailureKind> {
    loop {
        let attempt = TurnFinalization {
            turn_row_id: finalization.turn_row_id,
            scope_key: finalization.scope_key.clone(),
            sources: finalization.sources.clone(),
            resolution: finalization.resolution,
            outcome: finalization.outcome.clone(),
        };
        let result = tokio::select! {
            biased;
            result = sink.finalize(attempt) => result,
            () = shutdown.cancelled() => return Err(ScopeFailureKind::Supervisor),
        };
        match result {
            Ok(()) => return Ok(()),
            Err(ReplySinkError::Invariant) => return Err(ScopeFailureKind::Projection),
            Err(ReplySinkError::Unavailable | ReplySinkError::Capacity) => {
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => return Err(ScopeFailureKind::Supervisor),
                    () = sleep(settings.finalization_retry) => {}
                }
            }
        }
    }
}

fn set_state(state: &RwLock<ScopeState>, next: ScopeState) {
    if let Ok(mut state) = state.write() {
        *state = next;
    }
}

fn set_active_turn(
    active_turn: &RwLock<Option<ActiveTurn>>,
    next: Option<ActiveTurn>,
) -> Result<(), ScopeFailureKind> {
    let mut current = active_turn.write().map_err(|_| ScopeFailureKind::Client)?;
    *current = next;
    Ok(())
}
