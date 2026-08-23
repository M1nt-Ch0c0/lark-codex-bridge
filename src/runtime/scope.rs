//! One-scope runtime contracts shared by the router and reply projector.

use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Instant as StdInstant, SystemTime, UNIX_EPOCH};

use futures_util::future::BoxFuture;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, sleep_until, timeout};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::codex::client::{AppServerClient, AppServerEvent, ThreadId, TurnId, TurnOutcome};
use crate::codex::types::{
    SandboxMode, ThreadResumeParams, ThreadStartParams, TurnSandboxPolicy, TurnStartParams,
    TurnStatus, UserInput,
};
use crate::lark::api::ResourceKind;
use crate::lark::bridge::QueuedInboundEvent;
use crate::lark::normalize::{InboundEvent, ScopeKey};
use crate::limits::{
    REPLY_MESSAGE_MAX_CHARS, SCOPE_MAILBOX_BYTE_BUDGET, SCOPE_MAILBOX_CAPACITY,
    TURN_BATCH_MAX_MESSAGES, TURN_BATCH_TEXT_BYTE_BUDGET,
};
use crate::render::{ProjectedReply, ProjectorOutput, ReplyProjector};
use crate::runtime::attachments::{AttachError, AttachmentCache};
use crate::runtime::context::{
    ContextDraft, ContextId, ContextRegistry, PendingBinding, RevocationReason,
};
use crate::runtime::policy::AccessPolicy;
use crate::runtime::router::RouterSettings;
use crate::runtime::tools::{CONTEXT_TOOLS_VERSION, bridge_dynamic_tools};
use crate::store::{
    BeginTurnOutcome, ClaimedInbound, InboundKey, InboundRejectionKind, InboundTerminal,
    NewOutboxRow, NewTurnRow, StoreHandle, TurnResolution, TurnState,
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
#[derive(Clone)]
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

/// One durable progress-card snapshot for a running turn.
#[derive(Clone, PartialEq, Eq)]
pub struct TurnProgress {
    /// Store turn row owning the deterministic progress key.
    pub turn_row_id: i64,
    /// Owning scope key.
    pub scope_key: String,
    /// Original Lark reply target.
    pub source: TurnSource,
    /// Zero-based progress update sequence.
    pub sequence: u32,
    /// Bounded, already-masked cumulative progress text.
    pub text: String,
}

impl fmt::Debug for TurnProgress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TurnProgress")
            .field("turn_row_id", &self.turn_row_id)
            .field("scope_key_len", &self.scope_key.len())
            .field("source", &self.source)
            .field("sequence", &self.sequence)
            .field("text_chars", &self.text.chars().count())
            .finish()
    }
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

    /// Persists a progress-card snapshot. The default no-op keeps test and
    /// alternate sinks source-compatible; production overrides it with the
    /// durable outbox implementation.
    fn progress(&self, _progress: TurnProgress) -> BoxFuture<'static, Result<(), ReplySinkError>> {
        Box::pin(async { Ok(()) })
    }

    /// Persists the terminal reply effects before the caller resolves store state.
    fn finalize(&self, turn: TurnFinalization) -> BoxFuture<'static, Result<(), ReplySinkError>>;

    /// Persists a terminal reply already projected with live progress state.
    /// The default falls back to [`DurableReplySink::finalize`].
    fn finalize_projected(
        &self,
        turn: TurnFinalization,
        _reply: ProjectedReply,
    ) -> BoxFuture<'static, Result<(), ReplySinkError>> {
        self.finalize(turn)
    }
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
    Attachment,
    Context,
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
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn(
        scope: ScopeKey,
        store: StoreHandle,
        policy: AccessPolicy,
        settings: RouterSettings,
        supervisor: watch::Receiver<SupervisorAccess>,
        active_turns: Arc<Semaphore>,
        sink: Arc<dyn DurableReplySink>,
        attachments: Option<Arc<AttachmentCache>>,
        contexts: Option<Arc<ContextRegistry>>,
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
            attachments,
            contexts,
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
        let Ok(bytes) = u32::try_from(queued.permit.num_permits()) else {
            return Err(ActorRouteError::Capacity(Box::new(queued)));
        };
        let Ok(permit) = self.budget.clone().try_acquire_many_owned(bytes) else {
            return Err(ActorRouteError::Capacity(Box::new(queued)));
        };
        let command = ScopeCommand::Inbound(Box::new(ActorInbound {
            key,
            queued,
            _mailbox_permit: permit,
        }));
        self.sender.try_send(command).map_err(|error| match error {
            mpsc::error::TrySendError::Full(ScopeCommand::Inbound(item)) => {
                ActorRouteError::Capacity(Box::new(item.queued))
            }
            mpsc::error::TrySendError::Closed(ScopeCommand::Inbound(item)) => {
                ActorRouteError::Closed(Box::new(item.queued))
            }
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

pub(crate) enum ActorRouteError {
    Capacity(Box<QueuedInboundEvent>),
    Closed(Box<QueuedInboundEvent>),
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
    attachments: Option<Arc<AttachmentCache>>,
    contexts: Option<Arc<ContextRegistry>>,
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
                    attachments.as_deref(),
                    contexts.as_deref(),
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
    attachments: Option<&AttachmentCache>,
    contexts: Option<&ContextRegistry>,
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
        } else {
            policy.decide(&item.queued.event).rejection_kind()
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
    let (turn_epoch, client) = wait_for_client(&mut supervisor, shutdown).await?;
    set_state(state, ScopeState::StartingTurn);
    let thread_id = tokio::select! {
        biased;
        () = shutdown.cancelled() => return Err(ScopeFailureKind::Supervisor),
        result = ensure_thread(
            scope,
            store,
            policy,
            settings,
            &client,
            &cwd,
            &fingerprint,
            contexts.is_some(),
        ) => {
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
    let sources = claimed
        .iter()
        .map(|claimed| TurnSource {
            event_id: claimed.retained.event().event_id.clone(),
            message_id: claimed.retained.event().message_id.clone(),
            chat_id: claimed.retained.event().chat_id.clone(),
            thread_id: claimed.retained.event().thread_id.clone(),
        })
        .collect::<Vec<_>>();
    let assembly = match assemble_turn_inputs(
        &claimed,
        attachments,
        contexts,
        &thread_id,
        turn_row_id,
        shutdown,
    )
    .await
    {
        Ok(inputs) => inputs,
        Err(AttachmentAssemblyError::Failed | AttachmentAssemblyError::Cancelled) => {
            finalize_failed(
                store,
                sink.as_ref(),
                settings,
                turn_row_id,
                scope,
                sources,
                shutdown,
            )
            .await?;
            release_attachments(attachments, turn_row_id).await?;
            return Ok(());
        }
    };
    let Ok(rpc_cwd) = revalidate_workspace(policy, &cwd, &fingerprint) else {
        finalize_failed(
            store,
            sink.as_ref(),
            settings,
            turn_row_id,
            scope,
            sources,
            shutdown,
        )
        .await?;
        release_attachments(attachments, turn_row_id).await?;
        return Ok(());
    };
    let mut context_lease = assembly.contexts;
    let mut params = TurnStartParams::new(&thread_id, assembly.inputs);
    params.client_user_message_id = Some(client_message_id);
    params.cwd = Some(rpc_cwd.clone());
    params.approval_policy = Some(settings.approval_policy.clone());
    params.model.clone_from(&settings.model);
    params.sandbox_policy = Some(turn_sandbox(settings, rpc_cwd));
    let start_result = tokio::select! {
        biased;
        () = shutdown.cancelled() => None,
        result = client.start_turn(params) => Some(result),
    };
    let started = match start_result {
        Some(Ok(started)) => started,
        Some(Err(error)) if error.turn_start_definitely_not_applied() => {
            finalize_failed(
                store,
                sink.as_ref(),
                settings,
                turn_row_id,
                scope,
                sources,
                shutdown,
            )
            .await?;
            release_attachments(attachments, turn_row_id).await?;
            return Ok(());
        }
        None | Some(Err(_)) => {
            finalize_uncertain_and_settle_attachments(
                store,
                sink.as_ref(),
                settings,
                turn_row_id,
                scope,
                sources,
                attachments,
                &mut supervisor,
                turn_epoch,
                shutdown,
            )
            .await?;
            return Ok(());
        }
    };
    if let Some(lease) = context_lease.as_ref() {
        if lease.activate(&started.id).is_err() {
            finalize_uncertain_and_settle_attachments(
                store,
                sink.as_ref(),
                settings,
                turn_row_id,
                scope,
                sources,
                attachments,
                &mut supervisor,
                turn_epoch,
                shutdown,
            )
            .await?;
            return Ok(());
        }
    }
    if store
        .set_turn_state(turn_row_id, TurnState::Running, Some(&started.id))
        .await
        .is_err()
    {
        finalize_uncertain_and_settle_attachments(
            store,
            sink.as_ref(),
            settings,
            turn_row_id,
            scope,
            sources,
            attachments,
            &mut supervisor,
            turn_epoch,
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
    let mut projector = ReplyProjector::with_defaults();
    let mut progress_sequence = 0_u32;
    let mut progress_snapshot = String::new();
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
            Some(event) if event_belongs_to_turn(&event, &started.id) => {
                if let ProjectorOutput::Progress { text } =
                    projector.observe(&event, StdInstant::now())
                {
                    let Some(source) = sources.last().cloned() else {
                        projector.restore_progress(&text);
                        continue;
                    };
                    let snapshot = append_progress_snapshot(&progress_snapshot, &text);
                    let progress = TurnProgress {
                        turn_row_id,
                        scope_key: scope.to_string(),
                        source,
                        sequence: progress_sequence,
                        text: snapshot.clone(),
                    };
                    let progress_result = tokio::select! {
                        biased;
                        result = sink.progress(progress) => result,
                        () = shutdown.cancelled() => {
                            projector.restore_progress(&text);
                            break None;
                        }
                    };
                    match progress_result {
                        Ok(()) => {
                            progress_snapshot = snapshot;
                            progress_sequence = progress_sequence.saturating_add(1);
                        }
                        Err(error) => {
                            projector.restore_progress(&text);
                            tracing::warn!(
                                error = %error,
                                turn_row_id,
                                "durable progress projection was rejected"
                            );
                        }
                    }
                }
            }
            Some(_) => {}
        }
    };
    set_active_turn(active_turn, None)?;
    set_state(state, ScopeState::Finalizing { turn_row_id });
    let Some(outcome) = outcome else {
        finalize_uncertain_and_settle_attachments(
            store,
            sink.as_ref(),
            settings,
            turn_row_id,
            scope,
            sources,
            attachments,
            &mut supervisor,
            turn_epoch,
            shutdown,
        )
        .await?;
        return Ok(());
    };
    let (resolution, inbound) = resolution_for(&outcome.status);
    let projected_reply = projector.finish(&outcome);
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
        Some(projected_reply),
        shutdown,
    )
    .await?;
    store
        .resolve_turn_and_finish_inbound_batch(turn_row_id, resolution, inbound)
        .await
        .map_err(|_| ScopeFailureKind::Store)?;
    if let Some(lease) = context_lease.as_mut() {
        lease.reason = match resolution {
            TurnResolution::Completed => RevocationReason::Completed,
            TurnResolution::Interrupted => RevocationReason::Cancelled,
            TurnResolution::Failed | TurnResolution::Uncertain => RevocationReason::Failed,
        };
    }
    if resolution != TurnResolution::Uncertain {
        release_attachments(attachments, turn_row_id).await?;
    }
    Ok(())
}

fn event_belongs_to_turn(event: &AppServerEvent, turn_id: &str) -> bool {
    match event {
        AppServerEvent::AgentMessageDelta {
            turn_id: event_turn,
            ..
        }
        | AppServerEvent::CommandOutputDelta {
            turn_id: event_turn,
            ..
        }
        | AppServerEvent::ItemStarted {
            turn_id: event_turn,
            ..
        }
        | AppServerEvent::ItemCompleted {
            turn_id: event_turn,
            ..
        }
        | AppServerEvent::TokenUsageUpdated {
            turn_id: event_turn,
            ..
        }
        | AppServerEvent::Error {
            turn_id: event_turn,
            ..
        } => event_turn.as_str() == turn_id,
        AppServerEvent::TurnCompleted(outcome) => outcome.turn_id.as_str() == turn_id,
        AppServerEvent::ThreadStarted { .. }
        | AppServerEvent::TurnStarted { .. }
        | AppServerEvent::SubscriptionInvalidated { .. }
        | AppServerEvent::Unknown { .. }
        | AppServerEvent::ConnectionClosed { .. } => false,
    }
}

fn append_progress_snapshot(current: &str, next: &str) -> String {
    let mut snapshot = String::with_capacity(current.len().saturating_add(next.len()));
    snapshot.push_str(current);
    snapshot.push_str(next);
    let byte = snapshot
        .char_indices()
        .nth(REPLY_MESSAGE_MAX_CHARS)
        .map_or(snapshot.len(), |(index, _)| index);
    snapshot.truncate(byte);
    snapshot
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

async fn assemble_turn_inputs(
    claimed: &[ClaimedInbound],
    attachments: Option<&AttachmentCache>,
    contexts: Option<&ContextRegistry>,
    codex_thread_id: &str,
    turn_row_id: i64,
    shutdown: &CancellationToken,
) -> Result<TurnInputAssembly, AttachmentAssemblyError> {
    // Resource counts are untrusted until each message passes the cache's
    // hard limit, so reserve only the already bounded claimed-message count.
    let mut inputs = Vec::with_capacity(claimed.len());
    let mut turn_bytes = 0_u64;
    let mut attachment_sequence = 0_u32;
    let binding = PendingBinding {
        codex_thread_id: codex_thread_id.to_owned(),
        local_turn_row_id: turn_row_id,
    };
    let context_lease = contexts.map(|registry| TurnContextLease {
        registry: registry.clone(),
        binding: binding.clone(),
        context_ids: Vec::with_capacity(claimed.len()),
        reason: RevocationReason::Failed,
    });
    let mut context_lease = context_lease;

    for claimed in claimed {
        if shutdown.is_cancelled() {
            return Err(AttachmentAssemblyError::Cancelled);
        }
        let event = claimed.retained.event();
        inputs.push(UserInput::text(event.text.clone()));
        if let (Some(registry), Some(lease)) = (contexts, context_lease.as_mut()) {
            let registered = registry
                .register_pending(binding.clone(), ContextDraft::from_inbound(event))
                .map_err(|_| AttachmentAssemblyError::Failed)?;
            let wake = if event.mentions_bot {
                "mention"
            } else {
                "message"
            };
            let reference = serde_json::to_string(&serde_json::json!({
                "id": registered.context_id.as_str(),
                "wake": wake,
                "mentioned_self": event.mentions_bot,
            }))
            .map_err(|_| AttachmentAssemblyError::Failed)?;
            inputs.push(UserInput::text(format!(
                "<bridge_context>{reference}</bridge_context>"
            )));
            lease.context_ids.push(registered.context_id);
            continue;
        }
        let Some(cache) = attachments else {
            continue;
        };
        let limits = cache.limits();
        limits
            .check_resource_batch(&event.resources)
            .map_err(|_| AttachmentAssemblyError::Failed)?;
        for resource in &event.resources {
            let cached = cache
                .fetch_cancellable(&event.message_id, resource, turn_row_id, shutdown)
                .await
                .map_err(|error| match error {
                    AttachError::Cancelled { .. } => AttachmentAssemblyError::Cancelled,
                    _ => AttachmentAssemblyError::Failed,
                })?;
            turn_bytes = turn_bytes.saturating_add(cached.bytes);
            limits
                .check_turn_total(turn_bytes)
                .map_err(|_| AttachmentAssemblyError::Failed)?;
            attachment_sequence = attachment_sequence.saturating_add(1);
            match cached.kind {
                ResourceKind::Image => inputs.push(UserInput::LocalImage {
                    path: cached.path,
                    detail: None,
                }),
                ResourceKind::File => {
                    let path = cached
                        .path
                        .to_str()
                        .ok_or(AttachmentAssemblyError::Failed)?;
                    let context = serde_json::to_string(&serde_json::json!({
                        "attachment": {
                            "kind": "file",
                            "name": format!("attachment-{attachment_sequence}"),
                            "path": path,
                            "sha256": cached.sha256,
                            "bytes": cached.bytes,
                        }
                    }))
                    .map_err(|_| AttachmentAssemblyError::Failed)?;
                    inputs.push(UserInput::text(context));
                }
            }
        }
    }
    Ok(TurnInputAssembly {
        inputs,
        contexts: context_lease,
    })
}

struct TurnInputAssembly {
    inputs: Vec<UserInput>,
    contexts: Option<TurnContextLease>,
}

struct TurnContextLease {
    registry: ContextRegistry,
    binding: PendingBinding,
    context_ids: Vec<ContextId>,
    reason: RevocationReason,
}

impl TurnContextLease {
    fn activate(&self, codex_turn_id: &str) -> Result<(), ScopeFailureKind> {
        for context_id in &self.context_ids {
            self.registry
                .activate(context_id, &self.binding, codex_turn_id)
                .map_err(|_| ScopeFailureKind::Context)?;
        }
        Ok(())
    }
}

impl Drop for TurnContextLease {
    fn drop(&mut self) {
        let _ = self.registry.revoke_turn(&self.binding, self.reason);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AttachmentAssemblyError {
    Failed,
    Cancelled,
}

async fn release_attachments(
    attachments: Option<&AttachmentCache>,
    turn_row_id: i64,
) -> Result<(), ScopeFailureKind> {
    let Some(cache) = attachments else {
        return Ok(());
    };
    cache
        .release_turn(turn_row_id)
        .await
        .map_err(|_| ScopeFailureKind::Attachment)?;
    cache.gc().await.map_err(|_| ScopeFailureKind::Attachment)?;
    Ok(())
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
) -> Result<(u64, Arc<AppServerClient>), ScopeFailureKind> {
    loop {
        let access = supervisor.borrow().clone();
        if let Some(client) = access.client {
            return Ok((access.epoch, client));
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

#[allow(
    clippy::if_not_else,
    clippy::too_many_arguments,
    reason = "thread recovery keeps its explicit dependencies and fail-closed branch ordering"
)]
async fn ensure_thread(
    scope: &ScopeKey,
    store: &StoreHandle,
    policy: &AccessPolicy,
    settings: &RouterSettings,
    client: &AppServerClient,
    cwd: &Path,
    fingerprint: &str,
    context_tools: bool,
) -> Result<String, ScopeFailureKind> {
    if let Some(active) = store
        .active_thread(scope)
        .await
        .map_err(|_| ScopeFailureKind::Store)?
    {
        let required_version = if context_tools {
            CONTEXT_TOOLS_VERSION
        } else {
            0
        };
        if active.context_tools_version != required_version {
            store
                .archive_active_thread(scope)
                .await
                .map_err(|_| ScopeFailureKind::Store)?;
            let _ = client
                .release_thread(&ThreadId::from(active.codex_thread_id.as_str()))
                .await;
        } else {
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
    }
    let rpc_cwd = revalidate_workspace(policy, cwd, fingerprint)?;
    let params = ThreadStartParams {
        cwd: Some(rpc_cwd),
        sandbox: Some(settings.sandbox),
        approval_policy: Some(settings.approval_policy.clone()),
        model: settings.model.clone(),
        dynamic_tools: context_tools.then(bridge_dynamic_tools),
        ..ThreadStartParams::default()
    };
    let thread = client
        .start_thread(params)
        .await
        .map_err(|_| ScopeFailureKind::Client)?;
    store
        .record_active_thread_with_context_tools(
            scope,
            &thread.id,
            if context_tools {
                CONTEXT_TOOLS_VERSION
            } else {
                0
            },
        )
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

async fn finalize_failed(
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
            resolution: TurnResolution::Failed,
            outcome: None,
        },
        None,
        shutdown,
    )
    .await?;
    store
        .resolve_turn_and_finish_inbound_batch(
            turn_row_id,
            TurnResolution::Failed,
            InboundTerminal::Rejected,
        )
        .await
        .map_err(|_| ScopeFailureKind::Store)?;
    Ok(())
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
        None,
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

#[allow(clippy::too_many_arguments)]
async fn finalize_uncertain_and_settle_attachments(
    store: &StoreHandle,
    sink: &dyn DurableReplySink,
    settings: &RouterSettings,
    turn_row_id: i64,
    scope: &ScopeKey,
    sources: Vec<TurnSource>,
    attachments: Option<&AttachmentCache>,
    supervisor: &mut watch::Receiver<SupervisorAccess>,
    turn_epoch: u64,
    shutdown: &CancellationToken,
) -> Result<(), ScopeFailureKind> {
    finalize_uncertain(store, sink, settings, turn_row_id, scope, sources, shutdown).await?;
    let Some(_) = attachments else {
        return Ok(());
    };
    if wait_for_epoch_end(supervisor, turn_epoch, shutdown).await? {
        release_attachments(attachments, turn_row_id).await?;
    }
    Ok(())
}

async fn wait_for_epoch_end(
    supervisor: &mut watch::Receiver<SupervisorAccess>,
    turn_epoch: u64,
    shutdown: &CancellationToken,
) -> Result<bool, ScopeFailureKind> {
    loop {
        let access = supervisor.borrow().clone();
        if access.epoch != turn_epoch || access.client.is_none() {
            return Ok(true);
        }
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return Ok(false),
            changed = supervisor.changed() => {
                changed.map_err(|_| ScopeFailureKind::Supervisor)?;
            }
        }
    }
}

async fn persist_finalization(
    sink: &dyn DurableReplySink,
    settings: &RouterSettings,
    finalization: TurnFinalization,
    projected_reply: Option<ProjectedReply>,
    shutdown: &CancellationToken,
) -> Result<(), ScopeFailureKind> {
    let retry = finalization.clone();
    let retry_reply = projected_reply.clone();
    if !shutdown.is_cancelled() {
        let result =
            persist_finalization_inner(sink, settings, finalization, projected_reply, shutdown)
                .await;
        if !matches!(result, Err(ScopeFailureKind::Supervisor)) || !shutdown.is_cancelled() {
            return result;
        }
    }

    let cleanup = CancellationToken::new();
    timeout(
        settings.shutdown_cleanup_timeout,
        persist_finalization_inner(sink, settings, retry, retry_reply, &cleanup),
    )
    .await
    .map_err(|_| ScopeFailureKind::Supervisor)?
}

async fn persist_finalization_inner(
    sink: &dyn DurableReplySink,
    settings: &RouterSettings,
    finalization: TurnFinalization,
    projected_reply: Option<ProjectedReply>,
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
        let operation = if let Some(reply) = projected_reply.clone() {
            sink.finalize_projected(attempt, reply)
        } else {
            sink.finalize(attempt)
        };
        let result = tokio::select! {
            biased;
            result = operation => result,
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
