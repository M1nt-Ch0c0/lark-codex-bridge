//! One-scope runtime contracts shared by the router and reply projector.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Instant as StdInstant, SystemTime, UNIX_EPOCH};

use futures_util::future::BoxFuture;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, sleep_until, timeout};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::channel::MediaKind as ResourceKind;
use crate::codex::client::{
    AppServerClient, AppServerEvent, ClientError, ThreadId, TurnId, TurnOutcome,
};
use crate::codex::rpc::RpcError;
use crate::codex::supervisor::ProfileIdentity;
use crate::codex::types::{
    SandboxMode, ThreadResumeParams, ThreadStartParams, TurnSandboxPolicy, TurnStartParams,
    TurnStatus, UserInput,
};
use crate::lark::api::ChatMode;
use crate::lark::bridge::QueuedInboundEvent;
use crate::lark::normalize::{InboundEvent, MessagePart, ScopeKey};
use crate::limits::{
    REPLY_MESSAGE_MAX_CHARS, SCOPE_MAILBOX_BYTE_BUDGET, SCOPE_MAILBOX_CAPACITY,
    TURN_BATCH_MAX_MESSAGES, TURN_BATCH_TEXT_BYTE_BUDGET,
};
use crate::render::{ProjectedReply, ProjectorOutput, ReplyProjector};
use crate::runtime::adoption::ThreadCandidatePage;
use crate::runtime::adoption_coordinator::{
    AdoptionResumeSettings, CandidateSelectionProof, ExplicitHandoff, ReleaseOutcome,
    ThreadAdoptionCoordinator, ThreadAdoptionCoordinatorError,
};
use crate::runtime::attachments::{AttachError, AttachmentCache};
use crate::runtime::commands::{BridgeCommand, CommandParseError};
use crate::runtime::context::{
    ContextDraft, ContextId, ContextRegistry, PendingBinding, RevocationReason,
};
use crate::runtime::policy::AccessPolicy;
use crate::runtime::quote::{QuoteRequest, QuoteResolver};
use crate::runtime::router::RouterSettings;
use crate::runtime::tools::{CONTEXT_TOOLS_VERSION, bridge_dynamic_tools};
use crate::store::{
    BeginTurnOutcome, ClaimedInbound, InboundKey, InboundRejectionKind, InboundTerminal,
    NewOutboxRow, NewTurnRow, StoreHandle, ThreadAdoptionState, ThreadOrigin, TurnResolution,
    TurnState,
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
        key: &InboundKey,
        event: &InboundEvent,
        reason: InboundRejectionKind,
    ) -> Result<NewOutboxRow, ReplySinkError>;

    /// Builds one deterministic bridge-control reply without performing I/O.
    ///
    /// The scope actor passes the returned row to the store's atomic
    /// command-completion boundary. The default refusal keeps alternate and
    /// test sinks source-compatible until they opt into visible commands.
    ///
    /// # Errors
    ///
    /// Returns a static projection classification when the event or bounded
    /// reply cannot be represented as one deterministic outbox row.
    fn control_reply(
        &self,
        _key: &InboundKey,
        _event: &InboundEvent,
        _text: &str,
    ) -> Result<NewOutboxRow, ReplySinkError> {
        Err(ReplySinkError::Invariant)
    }

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
    /// P2P attachment descriptors waiting for ordinary text.
    pub pending_media: usize,
    /// Aggregate variable metadata bytes in the pending queue.
    pub pending_media_bytes: usize,
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

enum ThreadPreparationError {
    Scope(ScopeFailureKind),
    Client(ClientError),
}

impl ThreadPreparationError {
    fn scope_kind(self) -> ScopeFailureKind {
        match self {
            Self::Scope(kind) => kind,
            Self::Client(_) => ScopeFailureKind::Client,
        }
    }
}

#[derive(Clone)]
pub(crate) struct SupervisorAccess {
    pub(crate) epoch: u64,
    pub(crate) client: Option<Arc<AppServerClient>>,
    pub(crate) profile_identity: Option<ProfileIdentity>,
    /// True once no future client can become ready. This carries no failure
    /// reason so local paths or secrets cannot cross into actor diagnostics.
    pub(crate) terminal: bool,
}

impl fmt::Debug for SupervisorAccess {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SupervisorAccess")
            .field("epoch", &self.epoch)
            .field("ready", &self.client.is_some())
            .field("profile_ready", &self.profile_identity.is_some())
            .field("terminal", &self.terminal)
            .finish()
    }
}

pub(crate) struct ActorInbound {
    pub(crate) key: InboundKey,
    pub(crate) queued: QueuedInboundEvent,
    pub(crate) _mailbox_permit: OwnedSemaphorePermit,
}

#[derive(Clone)]
struct PendingMediaItem {
    draft: ContextDraft,
    metadata_bytes: usize,
    expires_at: Instant,
}

struct PendingMediaQueue {
    items: VecDeque<PendingMediaItem>,
    metadata_bytes: usize,
    ttl: std::time::Duration,
    max_count: usize,
    max_metadata_bytes: usize,
    generation: u64,
}

impl PendingMediaQueue {
    fn new(settings: &RouterSettings) -> Self {
        Self {
            items: VecDeque::new(),
            metadata_bytes: 0,
            ttl: settings.pending_media_ttl,
            max_count: settings.pending_media_max_count,
            max_metadata_bytes: settings.pending_media_max_metadata_bytes,
            generation: 0,
        }
    }

    fn stage(&mut self, event: &InboundEvent) {
        self.expire(Instant::now());
        let metadata_bytes = pending_metadata_bytes(event);
        if metadata_bytes > self.max_metadata_bytes {
            return;
        }
        while self.items.len() >= self.max_count
            || self.metadata_bytes.saturating_add(metadata_bytes) > self.max_metadata_bytes
        {
            let Some(evicted) = self.items.pop_front() else {
                break;
            };
            self.metadata_bytes = self.metadata_bytes.saturating_sub(evicted.metadata_bytes);
        }
        let mut draft = ContextDraft::from_inbound(event);
        // Pending association is implicit media only. A reply attached to the
        // media message itself must not become a second quote traversal.
        draft.quote = None;
        self.items.push_back(PendingMediaItem {
            draft,
            metadata_bytes,
            expires_at: Instant::now() + self.ttl,
        });
        self.metadata_bytes = self.metadata_bytes.saturating_add(metadata_bytes);
    }

    fn reserve_all(&mut self) -> (u64, Vec<PendingMediaItem>) {
        self.expire(Instant::now());
        self.metadata_bytes = 0;
        (self.generation, self.items.drain(..).collect())
    }

    fn restore(&mut self, generation: u64, items: Vec<PendingMediaItem>) {
        if generation != self.generation {
            return;
        }
        let now = Instant::now();
        self.expire(now);
        // Reserved items necessarily predate anything staged while the
        // reservation was held. Put them first so stable expiry sorting also
        // preserves FIFO order when two `Instant`s compare equal.
        let mut merged = items
            .into_iter()
            .chain(self.items.drain(..))
            .collect::<Vec<_>>();
        merged.retain(|item| item.expires_at > now);
        merged.sort_by_key(|item| item.expires_at);
        self.metadata_bytes = merged
            .iter()
            .map(|item| item.metadata_bytes)
            .fold(0_usize, usize::saturating_add);
        self.items = merged.into();
        while self.items.len() > self.max_count || self.metadata_bytes > self.max_metadata_bytes {
            let Some(evicted) = self.items.pop_front() else {
                break;
            };
            self.metadata_bytes = self.metadata_bytes.saturating_sub(evicted.metadata_bytes);
        }
    }

    fn expire(&mut self, now: Instant) {
        while self
            .items
            .front()
            .is_some_and(|item| now >= item.expires_at)
        {
            if let Some(expired) = self.items.pop_front() {
                self.metadata_bytes = self.metadata_bytes.saturating_sub(expired.metadata_bytes);
            }
        }
    }

    fn next_expiry(&self) -> Option<Instant> {
        self.items.front().map(|item| item.expires_at)
    }

    fn clear(&mut self) {
        self.items.clear();
        self.metadata_bytes = 0;
        self.generation = self.generation.wrapping_add(1);
    }

    fn stats(&mut self) -> (usize, usize) {
        self.expire(Instant::now());
        (self.items.len(), self.metadata_bytes)
    }
}

struct PendingMediaReservation {
    queue: Arc<Mutex<PendingMediaQueue>>,
    generation: u64,
    items: Option<Vec<PendingMediaItem>>,
}

impl PendingMediaReservation {
    fn drafts(&self) -> Vec<ContextDraft> {
        let pending = self
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if pending.generation != self.generation {
            return Vec::new();
        }
        let now = Instant::now();
        let drafts = self
            .items
            .as_deref()
            .unwrap_or_default()
            .iter()
            .filter(|item| item.expires_at > now)
            .map(|item| item.draft.clone())
            .collect();
        // Keep the generation check and copy linearized against a concurrent
        // control-lane interrupt. Once `clear()` returns, this reservation can
        // no longer materialize into a later turn assembly.
        drop(pending);
        drafts
    }

    fn commit(&mut self) {
        self.items = None;
    }
}

impl Drop for PendingMediaReservation {
    fn drop(&mut self) {
        let Some(items) = self.items.take() else {
            return;
        };
        self.queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .restore(self.generation, items);
    }
}

fn pending_metadata_bytes(event: &InboundEvent) -> usize {
    let identifiers = [
        event.event_id.len(),
        event.message_id.len(),
        event.chat_id.len(),
        event.sender_id.len(),
        event.thread_id.as_deref().map_or(0, str::len),
        event.root_id.as_deref().map_or(0, str::len),
        event.reply_to_message_id.as_deref().map_or(0, str::len),
        event.message_type.len(),
    ]
    .into_iter()
    .fold(0_usize, usize::saturating_add);
    let mentions = event.mentions.iter().fold(0_usize, |total, mention| {
        [
            mention.key.as_deref(),
            mention.open_id.as_deref(),
            mention.user_id.as_deref(),
            mention.union_id.as_deref(),
            mention.name.as_deref(),
        ]
        .into_iter()
        .flatten()
        .map(str::len)
        .fold(total, usize::saturating_add)
    });
    let resources = event
        .resources
        .iter()
        .map(|resource| resource.key.len())
        .fold(0_usize, usize::saturating_add);
    event.parts.iter().fold(
        identifiers
            .saturating_add(mentions)
            .saturating_add(resources),
        |total, part| {
            let bytes = match part {
                MessagePart::Image(media)
                | MessagePart::File(media)
                | MessagePart::Sticker(media)
                | MessagePart::Audio(media)
                | MessagePart::Video(media) => media
                    .key
                    .as_deref()
                    .map_or(0, str::len)
                    .saturating_add(media.thumbnail_key.as_deref().map_or(0, str::len))
                    .saturating_add(media.metadata.file_name.as_deref().map_or(0, str::len))
                    .saturating_add(media.metadata.mime_type.as_deref().map_or(0, str::len)),
                MessagePart::Text { text } => text.len(),
                MessagePart::Forward { message_id, .. } => {
                    message_id.as_deref().map_or(0, str::len)
                }
                MessagePart::Unsupported { message_type, .. } => message_type.len(),
                MessagePart::Card { .. } => 0,
            };
            total.saturating_add(bytes)
        },
    )
}

struct TurnInbound {
    inbound: ActorInbound,
    pending_media: Option<PendingMediaReservation>,
}

/// Parsed owner-only work that shares the scope actor's ordered mailbox with
/// ordinary messages. Unknown slash-prefixed text never becomes this type.
pub(crate) enum ScopeControl {
    Command(BridgeCommand),
    Malformed(CommandParseError),
}

struct ActorControl {
    inbound: ActorInbound,
    control: ScopeControl,
}

enum ScopeCommand {
    Inbound(Box<ActorInbound>),
    Control(Box<ActorControl>),
}

enum DeferredScopeWork {
    Prepared(Box<TurnInbound>),
    Control(Box<ActorControl>),
}

pub(crate) struct ScopeActorHandle {
    scope: ScopeKey,
    sender: mpsc::Sender<ScopeCommand>,
    budget: Arc<Semaphore>,
    state: Arc<RwLock<ScopeState>>,
    active_turn: Arc<RwLock<Option<ActiveTurn>>>,
    pending_media: Arc<Mutex<PendingMediaQueue>>,
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
        quote_resolver: Option<Arc<dyn QuoteResolver>>,
        adoption: Arc<ThreadAdoptionCoordinator>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(SCOPE_MAILBOX_CAPACITY);
        let state = Arc::new(RwLock::new(ScopeState::Idle));
        let task_state = Arc::clone(&state);
        let active_turn = Arc::new(RwLock::new(None));
        let task_active_turn = Arc::clone(&active_turn);
        let shutdown = CancellationToken::new();
        let pending_media = Arc::new(Mutex::new(PendingMediaQueue::new(&settings)));
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
            quote_resolver,
            adoption,
            task_state,
            task_active_turn,
            Arc::clone(&pending_media),
            shutdown.clone(),
        ));
        Self {
            scope,
            sender,
            budget: Arc::new(Semaphore::new(SCOPE_MAILBOX_BYTE_BUDGET)),
            state,
            active_turn,
            pending_media,
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
        self.try_route_kind(key, queued, None)
    }

    pub(crate) fn try_route_control(
        &self,
        key: InboundKey,
        queued: QueuedInboundEvent,
        control: ScopeControl,
    ) -> Result<(), ActorRouteError> {
        self.try_route_kind(key, queued, Some(control))
    }

    fn try_route_kind(
        &self,
        key: InboundKey,
        queued: QueuedInboundEvent,
        control: Option<ScopeControl>,
    ) -> Result<(), ActorRouteError> {
        let Ok(bytes) = u32::try_from(queued.permit.num_permits()) else {
            return Err(ActorRouteError::Capacity(Box::new(queued)));
        };
        let Ok(permit) = self.budget.clone().try_acquire_many_owned(bytes) else {
            return Err(ActorRouteError::Capacity(Box::new(queued)));
        };
        let inbound = ActorInbound {
            key,
            queued,
            _mailbox_permit: permit,
        };
        let command = match control {
            Some(control) => ScopeCommand::Control(Box::new(ActorControl { inbound, control })),
            None => ScopeCommand::Inbound(Box::new(inbound)),
        };
        self.sender.try_send(command).map_err(|error| match error {
            mpsc::error::TrySendError::Full(ScopeCommand::Inbound(item)) => {
                ActorRouteError::Capacity(Box::new(item.queued))
            }
            mpsc::error::TrySendError::Full(ScopeCommand::Control(item)) => {
                ActorRouteError::Capacity(Box::new(item.inbound.queued))
            }
            mpsc::error::TrySendError::Closed(ScopeCommand::Inbound(item)) => {
                ActorRouteError::Closed(Box::new(item.queued))
            }
            mpsc::error::TrySendError::Closed(ScopeCommand::Control(item)) => {
                ActorRouteError::Closed(Box::new(item.inbound.queued))
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
        let (pending_media, pending_media_bytes) = self
            .pending_media
            .lock()
            .map_or((0, 0), |mut pending| pending.stats());
        ScopeSnapshot {
            state: self.state(),
            queued_messages: self.sender.max_capacity() - self.sender.capacity(),
            pending_media,
            pending_media_bytes,
        }
    }

    pub(crate) async fn interrupt(&self) -> Result<InterruptOutcome, ()> {
        if let Ok(mut pending) = self.pending_media.lock() {
            pending.clear();
        }
        let active = self.active_turn.read().map_err(|_| ())?.clone();
        let Some(active) = active else {
            return Ok(InterruptOutcome::NoActiveTurn);
        };
        if let Some((registry, binding)) = &active.context_binding {
            // Revoke before asking Codex to acknowledge the interrupt. Any
            // response that already committed is allowed to finish first;
            // every other media read is forced to return only cancellation.
            // This ordering makes it impossible for transcript/media content
            // to follow a successful interrupt acknowledgement.
            let _ = registry
                .revoke_turn_and_wait(binding, RevocationReason::Cancelled)
                .await;
        }
        active
            .client
            .interrupt_turn(&active.thread_id, &active.turn_id)
            .await
            .map_err(|_| ())?;
        Ok(InterruptOutcome::Requested)
    }

    pub(crate) async fn shutdown(mut self) {
        if let Ok(mut pending) = self.pending_media.lock() {
            pending.clear();
        }
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
    context_binding: Option<(ContextRegistry, PendingBinding)>,
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
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
    quote_resolver: Option<Arc<dyn QuoteResolver>>,
    adoption: Arc<ThreadAdoptionCoordinator>,
    state: Arc<RwLock<ScopeState>>,
    active_turn: Arc<RwLock<Option<ActiveTurn>>>,
    pending_media: Arc<Mutex<PendingMediaQueue>>,
    shutdown: CancellationToken,
) {
    let mut deferred: Option<DeferredScopeWork> = None;
    // A discovery proof is intentionally confined to this actor. Eviction,
    // restart, or one ownership-changing attempt drops it permanently.
    let mut candidate_proof: Option<CandidateSelectionProof> = None;
    'actor: loop {
        let first = match deferred.take() {
            Some(DeferredScopeWork::Prepared(first)) => Some(*first),
            Some(DeferredScopeWork::Control(control)) => {
                let result = process_control(
                    &scope,
                    *control,
                    &store,
                    &policy,
                    &settings,
                    &supervisor,
                    sink.as_ref(),
                    adoption.as_ref(),
                    &pending_media,
                    &mut candidate_proof,
                    &shutdown,
                )
                .await;
                if let Err(kind) = result {
                    set_state(&state, ScopeState::Failed { kind });
                } else {
                    set_state(&state, ScopeState::Idle);
                }
                continue;
            }
            None => {
                let next_expiry = pending_media
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .next_expiry();
                let command = if let Some(next_expiry) = next_expiry {
                    tokio::select! {
                        biased;
                        () = shutdown.cancelled() => break,
                        command = receiver.recv() => command,
                        () = sleep_until(next_expiry) => {
                            pending_media
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .expire(Instant::now());
                            continue;
                        }
                    }
                } else {
                    tokio::select! {
                        biased;
                        () = shutdown.cancelled() => break,
                        command = receiver.recv() => command,
                    }
                };
                match command {
                    Some(ScopeCommand::Inbound(first)) => match prepare_inbound(
                        *first,
                        &store,
                        &policy,
                        &settings,
                        sink.as_ref(),
                        &pending_media,
                    )
                    .await
                    {
                        Ok(first) => first,
                        Err(kind) => {
                            set_state(&state, ScopeState::Failed { kind });
                            continue;
                        }
                    },
                    Some(ScopeCommand::Control(control)) => {
                        let result = process_control(
                            &scope,
                            *control,
                            &store,
                            &policy,
                            &settings,
                            &supervisor,
                            sink.as_ref(),
                            adoption.as_ref(),
                            &pending_media,
                            &mut candidate_proof,
                            &shutdown,
                        )
                        .await;
                        if let Err(kind) = result {
                            set_state(&state, ScopeState::Failed { kind });
                        } else {
                            set_state(&state, ScopeState::Idle);
                        }
                        continue;
                    }
                    None => break,
                }
            }
        };
        if let Some(first) = first {
            let first_is_audio = is_audio_event(&first.inbound.queued.event);
            let mut batch = vec![first];
            let mut text_bytes = batch[0].inbound.queued.event.text.len();
            set_state(&state, ScopeState::Debouncing);
            let deadline = Instant::now() + settings.debounce;
            if !first_is_audio {
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
                                let next = match prepare_inbound(
                                    *next,
                                    &store,
                                    &policy,
                                    &settings,
                                    sink.as_ref(),
                                    &pending_media,
                                ).await {
                                    Ok(Some(next)) => next,
                                    Ok(None) => continue,
                                    Err(kind) => {
                                        // Match the first-item path: drop only the
                                        // failed item and keep the actor alive so the
                                        // assembled batch is still processed.
                                        set_state(&state, ScopeState::Failed { kind });
                                        break;
                                    }
                                };
                                if is_audio_event(&next.inbound.queued.event) {
                                    deferred = Some(DeferredScopeWork::Prepared(Box::new(next)));
                                    break;
                                }
                                let next_bytes = next.inbound.queued.event.text.len();
                                if text_bytes.saturating_add(next_bytes)
                                    > TURN_BATCH_TEXT_BYTE_BUDGET
                                {
                                    deferred = Some(DeferredScopeWork::Prepared(Box::new(next)));
                                    break;
                                }
                                text_bytes = text_bytes.saturating_add(next_bytes);
                                batch.push(next);
                            }
                            Some(ScopeCommand::Control(control)) => {
                                // Preserve mailbox order: the already assembled
                                // ordinary batch finishes first, then the
                                // command runs before any later message.
                                deferred = Some(DeferredScopeWork::Control(control));
                                break;
                            }
                            None => return,
                        }
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
                quote_resolver.as_deref(),
                adoption.as_ref(),
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

#[allow(clippy::too_many_arguments)]
async fn process_control(
    scope: &ScopeKey,
    control: ActorControl,
    store: &StoreHandle,
    policy: &AccessPolicy,
    settings: &RouterSettings,
    supervisor: &watch::Receiver<SupervisorAccess>,
    sink: &dyn DurableReplySink,
    adoption: &ThreadAdoptionCoordinator,
    pending_media: &Arc<Mutex<PendingMediaQueue>>,
    candidate_proof: &mut Option<CandidateSelectionProof>,
    shutdown: &CancellationToken,
) -> Result<(), ScopeFailureKind> {
    if is_stale(&control.inbound.queued.event, settings.message_max_age) {
        return reject_item(store, sink, &control.inbound, InboundRejectionKind::Stale).await;
    }
    if let Some(reason) = policy
        .decide_command(&control.inbound.queued.event)
        .rejection_kind()
    {
        return reject_item(store, sink, &control.inbound, reason).await;
    }

    let reply = match control.control {
        ScopeControl::Malformed(error) => {
            format!("Command rejected: {error}. Use /help for exact syntax.")
        }
        ScopeControl::Command(command) => {
            execute_control(
                scope,
                command,
                store,
                policy,
                settings,
                supervisor,
                adoption,
                pending_media,
                candidate_proof,
            )
            .await
        }
    };
    let row = sink
        .control_reply(&control.inbound.key, &control.inbound.queued.event, &reply)
        .map_err(|_| ScopeFailureKind::Projection)?;
    complete_control_reply(store, settings, &control.inbound.key, row, shutdown).await
}

async fn complete_control_reply(
    store: &StoreHandle,
    settings: &RouterSettings,
    key: &InboundKey,
    row: NewOutboxRow,
    shutdown: &CancellationToken,
) -> Result<(), ScopeFailureKind> {
    loop {
        match store
            .complete_received_and_enqueue_control_reply(key, row.clone())
            .await
        {
            Ok(_) => return Ok(()),
            Err(crate::store::StoreError::QueueFull) => {
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => return Err(ScopeFailureKind::Supervisor),
                    () = sleep(settings.finalization_retry) => {}
                }
            }
            Err(_) => return Err(ScopeFailureKind::Store),
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_control(
    scope: &ScopeKey,
    command: BridgeCommand,
    store: &StoreHandle,
    policy: &AccessPolicy,
    settings: &RouterSettings,
    supervisor: &watch::Receiver<SupervisorAccess>,
    adoption: &ThreadAdoptionCoordinator,
    pending_media: &Arc<Mutex<PendingMediaQueue>>,
    candidate_proof: &mut Option<CandidateSelectionProof>,
) -> String {
    match command {
        BridgeCommand::Threads { cursor } => {
            execute_threads_control(scope, cursor, policy, supervisor, adoption, candidate_proof)
                .await
        }
        BridgeCommand::Adopt { selector } => {
            execute_adopt_control(
                scope,
                &selector,
                store,
                policy,
                settings,
                supervisor,
                adoption,
                pending_media,
                candidate_proof,
            )
            .await
        }
        BridgeCommand::Release => {
            execute_release_control(scope, policy, settings, supervisor, adoption).await
        }
        BridgeCommand::New
        | BridgeCommand::Stop
        | BridgeCommand::Status
        | BridgeCommand::Cd { .. }
        | BridgeCommand::Help => unreachable!("router admits only adoption controls"),
    }
}

async fn execute_threads_control(
    scope: &ScopeKey,
    cursor: Option<String>,
    policy: &AccessPolicy,
    supervisor: &watch::Receiver<SupervisorAccess>,
    adoption: &ThreadAdoptionCoordinator,
    candidate_proof: &mut Option<CandidateSelectionProof>,
) -> String {
    let access = supervisor.borrow().clone();
    let Some(client) = access.client else {
        return "Thread discovery is unavailable because the shared app-server is not ready. Existing adopted ownership can still be released with /release."
            .to_owned();
    };
    match adoption
        .discover(scope, client.as_ref(), cursor, policy)
        .await
    {
        Ok(discovery) => {
            let reply = render_candidate_page(&discovery.page);
            *candidate_proof = Some(discovery.proof);
            reply
        }
        Err(error) => adoption_error_reply("Thread discovery", &error),
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_adopt_control(
    scope: &ScopeKey,
    selector: &str,
    store: &StoreHandle,
    policy: &AccessPolicy,
    settings: &RouterSettings,
    supervisor: &watch::Receiver<SupervisorAccess>,
    adoption: &ThreadAdoptionCoordinator,
    pending_media: &Arc<Mutex<PendingMediaQueue>>,
    candidate_proof: &mut Option<CandidateSelectionProof>,
) -> String {
    if exact_healthy_adoption(store, adoption, scope, selector).await {
        clear_pending_media(pending_media);
        return adoption_complete_reply();
    }
    let Some(proof) = candidate_proof.take() else {
        return "Adoption requires a fresh one-page selection proof. Run /threads, then copy one exact /adopt command from that reply; no ownership mapping was changed."
            .to_owned();
    };
    if prepare_workspace(scope, store, policy, settings)
        .await
        .is_err()
    {
        return "Adoption refused because this scope has no policy-valid workspace.".to_owned();
    }
    let access = supervisor.borrow().clone();
    let Some(shared_client) = access.client else {
        return "Adoption is unavailable because the shared app-server is not ready. The one-shot selection proof was consumed; run /threads again after recovery."
            .to_owned();
    };
    let Some(shared_profile) = access.profile_identity else {
        return "Adoption is unavailable because the shared app-server profile cannot be verified. The one-shot selection proof was consumed; run /threads again."
            .to_owned();
    };
    match adoption
        .adopt(
            scope,
            selector,
            shared_client.as_ref(),
            &shared_profile,
            Some(proof),
            policy,
            adoption_resume_settings(settings),
            ExplicitHandoff::Confirmed,
        )
        .await
    {
        Ok(_) => {
            clear_pending_media(pending_media);
            adoption_complete_reply()
        }
        Err(error) => adoption_error_reply("Adoption", &error),
    }
}

async fn execute_release_control(
    scope: &ScopeKey,
    policy: &AccessPolicy,
    settings: &RouterSettings,
    supervisor: &watch::Receiver<SupervisorAccess>,
    adoption: &ThreadAdoptionCoordinator,
) -> String {
    let released = match adoption.release(scope).await {
        Ok(receipt) => Ok(receipt),
        Err(
            error @ (ThreadAdoptionCoordinatorError::DomainMissing
            | ThreadAdoptionCoordinatorError::Fenced),
        ) => {
            let access = supervisor.borrow().clone();
            let Some(shared_client) = access.client else {
                return "Release recovery is unavailable because the shared app-server is not ready. The durable ownership state remains fenced; no fallback writer was opened."
                    .to_owned();
            };
            let Some(shared_profile) = access.profile_identity else {
                return adoption_error_reply("Release recovery", &error);
            };
            adoption
                .recover_release(
                    scope,
                    shared_client.as_ref(),
                    &shared_profile,
                    None,
                    policy,
                    adoption_resume_settings(settings),
                    ExplicitHandoff::Confirmed,
                )
                .await
        }
        Err(error) => Err(error),
    };
    match released {
        Ok(receipt) => release_complete_reply(receipt.outcome).to_owned(),
        Err(error) => adoption_error_reply("Release", &error),
    }
}

fn release_complete_reply(outcome: ReleaseOutcome) -> &'static str {
    match outcome {
        ReleaseOutcome::AdoptedMappingReleased => {
            "Release complete. The dedicated app-server process tree was confirmed reaped and the adopted mapping was removed; the persisted thread itself was not archived or deleted."
        }
        ReleaseOutcome::UncommittedAcquisitionCleaned => {
            "Release recovery complete. The uncommitted acquisition was durably closed after confirmed cleanup; no adopted mapping was removed, and any pre-existing bridge mapping remains active."
        }
    }
}

async fn exact_healthy_adoption(
    store: &StoreHandle,
    adoption: &ThreadAdoptionCoordinator,
    scope: &ScopeKey,
    selector: &str,
) -> bool {
    let Ok(Some(mapping)) = store.active_thread(scope).await else {
        return false;
    };
    if mapping.origin != ThreadOrigin::ExternallyAdopted || mapping.codex_thread_id != selector {
        return false;
    }
    adoption
        .route(scope)
        .await
        .is_ok_and(|route| route.thread_id == selector)
}

fn adoption_complete_reply() -> String {
    "Adoption complete. This scope now uses the explicitly selected persisted thread through a dedicated non-restarting app-server owner. Keep every other client closed until /release confirms process-tree cleanup."
        .to_owned()
}

fn adoption_resume_settings(settings: &RouterSettings) -> AdoptionResumeSettings {
    AdoptionResumeSettings {
        sandbox: settings.sandbox,
        approval_policy: settings.approval_policy.clone(),
        model: settings.model.clone(),
    }
}

fn clear_pending_media(pending_media: &Arc<Mutex<PendingMediaQueue>>) {
    pending_media
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clear();
}

fn adoption_error_reply(action: &str, error: &ThreadAdoptionCoordinatorError) -> String {
    match error {
        ThreadAdoptionCoordinatorError::ActiveWriterConflict if action.starts_with("Release") => {
            format!(
                "{action} refused: the persisted thread still has another active writer. The durable ownership state remains fenced; close the other Desktop/CLI/app-server owner, then retry /release explicitly."
            )
        }
        ThreadAdoptionCoordinatorError::ActiveWriterConflict => {
            format!(
                "{action} refused: the selected persisted thread still has another active writer. No adopted mapping was committed. Finish handoff, close the other Desktop/CLI/app-server owner, then retry explicitly."
            )
        }
        ThreadAdoptionCoordinatorError::Fenced | ThreadAdoptionCoordinatorError::DomainMissing => {
            format!(
                "{action} is fenced because this process cannot prove the exact dedicated owner. Keep other clients closed; no shared-client fallback or implicit ownership release was attempted."
            )
        }
        ThreadAdoptionCoordinatorError::CleanupUnconfirmed => format!(
            "{action} could not confirm process-tree cleanup. The adopted mapping remains fenced and active; do not open another writer."
        ),
        _ if action.starts_with("Release") => format!(
            "{action} failed: {error}. The durable ownership state remains fenced; no shared-client fallback or implicit release was attempted."
        ),
        _ => format!("{action} failed: {error}."),
    }
}

fn render_candidate_page(page: &ThreadCandidatePage) -> String {
    const FOOTER_RESERVE: usize = 850;
    let mut output = String::from(
        "Persisted-thread candidates (read-only preflight; writer ownership is unverified):\n",
    );
    if page.candidates.is_empty() {
        output.push_str("No eligible idle persisted threads were found.\n");
    }
    let mut omitted = 0_usize;
    for candidate in &page.candidates {
        let selector = json_display(&candidate.selector);
        let title = json_display(&candidate.title);
        let line = format!(
            "/adopt {selector} --handoff-complete\n  workspace={} | source={} | title={} | updated={}\n",
            candidate.workspace_alias, candidate.source, title, candidate.updated_at
        );
        if output
            .chars()
            .count()
            .saturating_add(line.chars().count())
            .saturating_add(FOOTER_RESERVE)
            > REPLY_MESSAGE_MAX_CHARS
        {
            omitted = omitted.saturating_add(1);
        } else {
            output.push_str(&line);
        }
    }
    if omitted != 0 {
        let _ = writeln!(
            output,
            "{omitted} additional candidates omitted by reply bounds."
        );
    }
    if let Some(cursor) = page.next_cursor.as_deref().filter(|cursor| {
        !cursor.is_empty()
            && !cursor.chars().any(char::is_control)
            && !cursor.chars().any(char::is_whitespace)
    }) {
        output.push_str("Next page: /threads ");
        output.push_str(cursor);
        output.push('\n');
    }
    output.push_str(
        "Before adoption, close every other Desktop, CLI, or app-server writer, then run /adopt <selector> --handoff-complete. Discovery alone does not acquire ownership.",
    );
    if output.chars().count() > REPLY_MESSAGE_MAX_CHARS {
        return "Candidate results exceeded the safe reply bound. Narrow the page with its cursor and retry; discovery did not acquire ownership."
            .to_owned();
    }
    output
}

fn json_display(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"[unavailable]\"".to_owned())
}

async fn prepare_inbound(
    inbound: ActorInbound,
    store: &StoreHandle,
    policy: &AccessPolicy,
    settings: &RouterSettings,
    sink: &dyn DurableReplySink,
    pending_media: &Arc<Mutex<PendingMediaQueue>>,
) -> Result<Option<TurnInbound>, ScopeFailureKind> {
    let event = &inbound.queued.event;
    if is_pending_media_event(event)
        || (event.chat_type != ChatMode::P2p && is_conversation_media_event(event))
    {
        let reason = if is_stale(event, settings.message_max_age) {
            Some(InboundRejectionKind::Stale)
        } else {
            policy.decide(event).rejection_kind()
        };
        if let Some(reason) = reason {
            reject_item(store, sink, &inbound, reason).await?;
            return Ok(None);
        }
        store
            .complete_received_without_turn(&inbound.key)
            .await
            .map_err(|_| ScopeFailureKind::Store)?;
        if is_pending_media_event(event) {
            pending_media
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .stage(event);
        }
        return Ok(None);
    }

    let mut pending = pending_media
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    pending.expire(Instant::now());
    let explicit_quote = event.reply_to_message_id.is_some();
    let reset = clears_pending_media(&event.text);
    let pending_media_reservation = if explicit_quote || reset {
        pending.clear();
        None
    } else if event.chat_type == ChatMode::P2p
        && event.message_type == "text"
        && !is_audio_event(event)
    {
        let (generation, items) = pending.reserve_all();
        (!items.is_empty()).then(|| PendingMediaReservation {
            queue: Arc::clone(pending_media),
            generation,
            items: Some(items),
        })
    } else {
        None
    };
    drop(pending);
    Ok(Some(TurnInbound {
        inbound,
        pending_media: pending_media_reservation,
    }))
}

fn is_pending_media_event(event: &InboundEvent) -> bool {
    event.chat_type == ChatMode::P2p
        && matches!(
            event.message_type.as_str(),
            "image" | "video" | "media" | "file"
        )
}

fn is_conversation_media_event(event: &InboundEvent) -> bool {
    matches!(
        event.message_type.as_str(),
        "image" | "video" | "media" | "file" | "audio"
    )
}

fn is_audio_event(event: &InboundEvent) -> bool {
    event.message_type == "audio"
}

fn external_batch_uses_unsupported_features(batch: &[TurnInbound]) -> bool {
    batch.iter().any(|item| {
        let event = &item.inbound.queued.event;
        item.pending_media.is_some()
            || event.message_type != "text"
            || event.reply_to_message_id.is_some()
            || !event.resources.is_empty()
            || event
                .parts
                .iter()
                .any(|part| !matches!(part, MessagePart::Text { .. }))
    })
}

fn clears_pending_media(text: &str) -> bool {
    matches!(text.trim(), "/cancel" | "/new" | "/stop")
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn process_batch(
    scope: &ScopeKey,
    batch: Vec<TurnInbound>,
    store: &StoreHandle,
    policy: &AccessPolicy,
    settings: &RouterSettings,
    mut supervisor: watch::Receiver<SupervisorAccess>,
    active_turns: Arc<Semaphore>,
    sink: Arc<dyn DurableReplySink>,
    attachments: Option<&AttachmentCache>,
    contexts: Option<&ContextRegistry>,
    quote_resolver: Option<&dyn QuoteResolver>,
    adoption: &ThreadAdoptionCoordinator,
    state: &Arc<RwLock<ScopeState>>,
    active_turn: &RwLock<Option<ActiveTurn>>,
    shutdown: &CancellationToken,
) -> Result<(), ScopeFailureKind> {
    let batch = deduplicate_batch(batch);
    tracing::debug!(batch_messages = batch.len(), "scope batch ready");
    let active_adoption = store
        .active_thread_adoption(scope)
        .await
        .map_err(|_| ScopeFailureKind::Store)?;
    let active_mapping = store
        .active_thread(scope)
        .await
        .map_err(|_| ScopeFailureKind::Store)?;
    let externally_adopted = match (&active_mapping, &active_adoption) {
        (Some(mapping), Some(saga))
            if mapping.origin == ThreadOrigin::ExternallyAdopted
                && mapping.adoption_generation == Some(saga.generation)
                && mapping.codex_thread_id == saga.codex_thread_id
                && saga.state == ThreadAdoptionState::Owned =>
        {
            true
        }
        (Some(mapping), None) if mapping.origin == ThreadOrigin::ExternallyAdopted => {
            reject_terminal_batch(store, sink.as_ref(), &batch).await?;
            return Ok(());
        }
        (_, Some(_)) => {
            reject_terminal_batch(store, sink.as_ref(), &batch).await?;
            return Ok(());
        }
        _ => false,
    };
    let _active_permit = loop {
        if !externally_adopted && supervisor.borrow().terminal {
            reject_terminal_batch(store, sink.as_ref(), &batch).await?;
            return Ok(());
        }
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return Err(ScopeFailureKind::Supervisor),
            permit = Arc::clone(&active_turns).acquire_owned() => {
                break permit.map_err(|_| ScopeFailureKind::Capacity)?;
            }
            changed = supervisor.changed(), if !externally_adopted => {
                changed.map_err(|_| ScopeFailureKind::Supervisor)?;
            }
        }
    };
    let mut eligible = Vec::with_capacity(batch.len());
    for item in batch {
        let event = &item.inbound.queued.event;
        let reason = if event.text.len() > TURN_BATCH_TEXT_BYTE_BUDGET {
            Some(InboundRejectionKind::Overloaded)
        } else if is_stale(event, settings.message_max_age) {
            Some(InboundRejectionKind::Stale)
        } else {
            policy.decide(event).rejection_kind()
        };
        if let Some(reason) = reason {
            reject_item(store, sink.as_ref(), &item.inbound, reason).await?;
        } else {
            eligible.push(item);
        }
    }
    if eligible.is_empty() {
        return Ok(());
    }
    let mut batch = eligible;
    let (cwd, fingerprint, policy_changed) =
        match prepare_workspace(scope, store, policy, settings).await {
            Ok(workspace) => workspace,
            Err(ScopeFailureKind::Policy) => {
                for item in &batch {
                    reject_item(
                        store,
                        sink.as_ref(),
                        &item.inbound,
                        InboundRejectionKind::Policy,
                    )
                    .await?;
                }
                return Ok(());
            }
            Err(kind) => return Err(kind),
        };
    set_state(state, ScopeState::StartingTurn);
    let (turn_epoch, client, thread_id) = if externally_adopted {
        if external_batch_uses_unsupported_features(&batch) {
            reject_terminal_batch(store, sink.as_ref(), &batch).await?;
            return Ok(());
        }
        if policy_changed {
            let _ = adoption.fence_and_reap(scope).await;
            reject_terminal_batch(store, sink.as_ref(), &batch).await?;
            return Ok(());
        }
        let Ok(route) = adoption.route(scope).await else {
            let _ = adoption.fence_and_reap(scope).await;
            reject_terminal_batch(store, sink.as_ref(), &batch).await?;
            return Ok(());
        };
        let Some(mapping) = active_mapping.as_ref() else {
            let _ = adoption.fence_and_reap(scope).await;
            reject_terminal_batch(store, sink.as_ref(), &batch).await?;
            return Ok(());
        };
        if route.thread_id != mapping.codex_thread_id
            || Some(route.generation) != mapping.adoption_generation
        {
            let _ = adoption.fence_and_reap(scope).await;
            reject_terminal_batch(store, sink.as_ref(), &batch).await?;
            return Ok(());
        }
        (route.client.epoch().get(), route.client, route.thread_id)
    } else {
        let (turn_epoch, client) = match wait_for_client(&mut supervisor, shutdown).await {
            Ok(ready) => ready,
            Err(ScopeFailureKind::Supervisor) if supervisor.borrow().terminal => {
                reject_terminal_batch(store, sink.as_ref(), &batch).await?;
                return Ok(());
            }
            Err(kind) => return Err(kind),
        };
        let thread_result = tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                Err(ThreadPreparationError::Scope(ScopeFailureKind::Supervisor))
            }
            result = ensure_thread(
                scope,
                store,
                policy,
                settings,
                &client,
                &cwd,
                &fingerprint,
                policy_changed,
                contexts.is_some(),
            ) => result,
        };
        let thread_id = match thread_result {
            Ok(thread_id) => thread_id,
            Err(ThreadPreparationError::Client(error)) if thread_failure_ends_epoch(&error) => {
                return settle_preclaim_epoch_loss(
                    &mut supervisor,
                    turn_epoch,
                    shutdown,
                    store,
                    sink.as_ref(),
                    &batch,
                )
                .await;
            }
            Err(error) => return Err(error.scope_kind()),
        };
        (turn_epoch, client, thread_id)
    };
    let subscription_result = tokio::select! {
        biased;
        () = shutdown.cancelled() => return Err(ScopeFailureKind::Supervisor),
        result = client.subscribe(thread_id.as_str().into()) => result,
    };
    let mut subscription = match subscription_result {
        Ok(subscription) => subscription,
        Err(_) if externally_adopted => {
            let _ = adoption.fence_and_reap(scope).await;
            reject_terminal_batch(store, sink.as_ref(), &batch).await?;
            return Ok(());
        }
        Err(error) if subscription_failure_ends_epoch(&error) => {
            return settle_preclaim_epoch_loss(
                &mut supervisor,
                turn_epoch,
                shutdown,
                store,
                sink.as_ref(),
                &batch,
            )
            .await;
        }
        Err(_) => return Err(ScopeFailureKind::Client),
    };
    // Externally adopted threads intentionally expose only the first-stage
    // plain-text path. They never inherit the shared server's dynamic context
    // tools, attachment cache, or quote resolver.
    let attachments = if externally_adopted {
        None
    } else {
        attachments
    };
    let contexts = if externally_adopted { None } else { contexts };
    let quote_resolver = if externally_adopted {
        None
    } else {
        quote_resolver
    };
    let client_message_id = Uuid::new_v4().to_string();
    let mut live_transcripts = HashMap::with_capacity(batch.len());
    for item in &mut batch {
        let handoff = item.inbound.queued.take_live_transcripts();
        if !handoff.is_empty() {
            live_transcripts.insert(item.inbound.key.clone(), handoff);
        }
    }
    let pending_contexts = batch
        .iter()
        .map(|item| {
            (
                item.inbound.queued.event.event_id.clone(),
                item.pending_media
                    .as_ref()
                    .map_or_else(Vec::new, PendingMediaReservation::drafts),
            )
        })
        .collect::<HashMap<_, _>>();
    let live_events = batch
        .iter()
        .map(|item| (item.inbound.key.clone(), item.inbound.queued.event.clone()))
        .collect::<Vec<_>>();
    let begun = store
        .begin_turn_and_claim_inbound_live(
            NewTurnRow {
                scope_key: scope.to_string(),
                client_message_id: client_message_id.clone(),
                codex_thread_id: Some(thread_id.clone()),
                state: TurnState::Starting,
            },
            live_events,
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
        &mut live_transcripts,
        attachments,
        contexts,
        quote_resolver,
        &pending_contexts,
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
        if externally_adopted {
            let _ = adoption.fence_and_reap(scope).await;
        }
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
    let input_count = assembly.inputs.len();
    let source_count = sources.len();
    let mut params = TurnStartParams::new(&thread_id, assembly.inputs);
    params.client_user_message_id = Some(client_message_id);
    params.cwd = Some(rpc_cwd.clone());
    params.approval_policy = Some(settings.approval_policy.clone());
    params.model.clone_from(&settings.model);
    params.effort.clone_from(&settings.effort);
    params.sandbox_policy = Some(turn_sandbox(settings, rpc_cwd));
    let turn_started_at = StdInstant::now();
    tracing::info!(
        epoch = turn_epoch,
        source_count,
        input_count,
        "Codex turn starting"
    );
    let start_result = tokio::select! {
        biased;
        () = shutdown.cancelled() => None,
        result = client.start_turn(params) => Some(result),
    };
    let started = match start_result {
        Some(Ok(started)) => started,
        Some(Err(error)) if error.turn_start_definitely_not_applied() => {
            tracing::warn!(
                epoch = turn_epoch,
                elapsed_ms =
                    u64::try_from(turn_started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
                outcome = "rejected",
                "Codex turn start failed"
            );
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
            tracing::warn!(
                epoch = turn_epoch,
                elapsed_ms =
                    u64::try_from(turn_started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
                outcome = "uncertain",
                "Codex turn start outcome is uncertain"
            );
            // The request may have reached Codex even though the response was
            // lost. Restoring its implicit media would risk submitting the
            // same attachment association in a later turn.
            if externally_adopted {
                let _ = adoption.fence_and_reap(scope).await;
            }
            commit_pending_media(&mut batch);
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
    commit_pending_media(&mut batch);
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
        if externally_adopted {
            let _ = adoption.fence_and_reap(scope).await;
        }
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
    tracing::info!(
        epoch = turn_epoch,
        start_elapsed_ms = u64::try_from(turn_started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
        "Codex turn running"
    );
    set_active_turn(
        active_turn,
        Some(ActiveTurn {
            client: Arc::clone(&client),
            thread_id: ThreadId::from(thread_id.as_str()),
            turn_id: TurnId::from(started.id.as_str()),
            context_binding: context_lease
                .as_ref()
                .map(TurnContextLease::cancellation_binding),
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
        if externally_adopted {
            let _ = adoption.fence_and_reap(scope).await;
        }
        if let Some(lease) = context_lease.as_mut() {
            lease.reason = RevocationReason::Failed;
        }
        drop(context_lease.take());
        tracing::warn!(
            epoch = turn_epoch,
            elapsed_ms = u64::try_from(turn_started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
            outcome = "uncertain",
            "Codex turn ended without an authoritative completion"
        );
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
    tracing::info!(
        epoch = turn_epoch,
        resolution = ?resolution,
        elapsed_ms = u64::try_from(turn_started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
        source_count,
        "Codex turn completed"
    );
    if let Some(lease) = context_lease.as_mut() {
        lease.reason = match resolution {
            TurnResolution::Completed => RevocationReason::Completed,
            TurnResolution::Interrupted => RevocationReason::Cancelled,
            TurnResolution::Failed | TurnResolution::Uncertain => RevocationReason::Failed,
        };
    }
    // Revoke/cancel tool capabilities before the attachment release pass.
    // A cancelled fetch that commits at the boundary then compensates its
    // exact lease instead of recreating one after finalization.
    drop(context_lease.take());
    if resolution != TurnResolution::Uncertain {
        release_attachments(attachments, turn_row_id).await?;
    }
    Ok(())
}

fn commit_pending_media(batch: &mut [TurnInbound]) {
    for item in batch {
        if let Some(pending) = item.pending_media.as_mut() {
            pending.commit();
        }
    }
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

fn deduplicate_batch(batch: Vec<TurnInbound>) -> Vec<TurnInbound> {
    let mut unique = HashSet::new();
    let mut retained = Vec::new();
    for item in batch {
        if !unique.insert(item.inbound.key.clone()) {
            continue;
        }
        retained.push(item);
    }
    retained
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn assemble_turn_inputs(
    claimed: &[ClaimedInbound],
    live_transcripts: &mut HashMap<InboundKey, crate::lark::normalize::LiveTranscriptHandoff>,
    attachments: Option<&AttachmentCache>,
    contexts: Option<&ContextRegistry>,
    quote_resolver: Option<&dyn QuoteResolver>,
    pending_contexts: &HashMap<String, Vec<ContextDraft>>,
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
        context_ids: Vec::with_capacity(
            claimed
                .len()
                .saturating_add(pending_contexts.values().map(Vec::len).sum()),
        ),
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
            if let Some(pending) = pending_contexts.get(&event.event_id) {
                for draft in pending {
                    register_context_input(
                        &mut inputs,
                        registry,
                        lease,
                        &binding,
                        draft.clone(),
                        crate::lark::normalize::LiveTranscriptHandoff::empty(),
                        "pending_media",
                        false,
                    )?;
                }
            }
            let mut draft = ContextDraft::from_inbound(event);
            if let (Some(parent_message_id), Some(resolver)) =
                (event.reply_to_message_id.as_ref(), quote_resolver)
            {
                draft.quote = Some(
                    resolver
                        .resolve(QuoteRequest {
                            parent_message_id: parent_message_id.clone(),
                            chat_id: event.chat_id.clone(),
                        })
                        .await,
                );
            }
            let wake = if event.mentions_bot {
                "mention"
            } else {
                "message"
            };
            register_context_input(
                &mut inputs,
                registry,
                lease,
                &binding,
                draft,
                live_transcripts.remove(&claimed.key).unwrap_or_default(),
                wake,
                event.mentions_bot,
            )?;
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

#[allow(clippy::too_many_arguments)]
fn register_context_input(
    inputs: &mut Vec<UserInput>,
    registry: &ContextRegistry,
    lease: &mut TurnContextLease,
    binding: &PendingBinding,
    draft: ContextDraft,
    live_transcripts: crate::lark::normalize::LiveTranscriptHandoff,
    wake: &'static str,
    mentioned_self: bool,
) -> Result<(), AttachmentAssemblyError> {
    let registered = registry
        .register_pending_with_transcripts(binding.clone(), draft, live_transcripts)
        .map_err(|_| AttachmentAssemblyError::Failed)?;
    let reference = serde_json::to_string(&serde_json::json!({
        "id": registered.context_id.as_str(),
        "wake": wake,
        "mentioned_self": mentioned_self,
    }))
    .map_err(|_| AttachmentAssemblyError::Failed)?;
    inputs.push(UserInput::text(format!(
        "<bridge_context>{reference}</bridge_context>"
    )));
    lease.context_ids.push(registered.context_id);
    Ok(())
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

    fn cancellation_binding(&self) -> (ContextRegistry, PendingBinding) {
        (self.registry.clone(), self.binding.clone())
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
        .rejection_notice(&item.key, &item.queued.event, reason)
        .map_err(|_| ScopeFailureKind::Projection)?;
    store
        .reject_received_and_enqueue_notice(&item.key, reason, notice)
        .await
        .map_err(|_| ScopeFailureKind::Store)?;
    tracing::info!(reason = reason.as_str(), "inbound event rejected");
    Ok(())
}

async fn reject_terminal_batch(
    store: &StoreHandle,
    sink: &dyn DurableReplySink,
    batch: &[TurnInbound],
) -> Result<(), ScopeFailureKind> {
    for item in batch {
        reject_item(store, sink, &item.inbound, InboundRejectionKind::Internal).await?;
    }
    Ok(())
}

fn thread_failure_ends_epoch(error: &ClientError) -> bool {
    !error.turn_start_definitely_not_applied()
        || matches!(
            error,
            ClientError::RouterClosed(_) | ClientError::RouterTaskFailed(_)
        )
}

fn subscription_failure_ends_epoch(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::Rpc(RpcError::ConnectionLost(_))
            | ClientError::RouterClosed(_)
            | ClientError::RouterTaskFailed(_)
    )
}

async fn settle_preclaim_epoch_loss(
    supervisor: &mut watch::Receiver<SupervisorAccess>,
    failed_epoch: u64,
    shutdown: &CancellationToken,
    store: &StoreHandle,
    sink: &dyn DurableReplySink,
    batch: &[TurnInbound],
) -> Result<(), ScopeFailureKind> {
    loop {
        let access = supervisor.borrow().clone();
        if access.terminal {
            reject_terminal_batch(store, sink, batch).await?;
            return Ok(());
        }
        if access.client.is_some() && access.epoch != failed_epoch {
            // Preserve the existing replay contract for a transient restart.
            // The batch was never claimed, so a later intake replay remains
            // authoritative rather than retrying an uncertain thread RPC.
            return Err(ScopeFailureKind::Client);
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

async fn prepare_workspace(
    scope: &ScopeKey,
    store: &StoreHandle,
    policy: &AccessPolicy,
    settings: &RouterSettings,
) -> Result<(PathBuf, String, bool), ScopeFailureKind> {
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
        let policy_changed = fingerprint.as_str() != row.policy_fingerprint;
        return Ok((canonical, fingerprint.as_str().to_owned(), policy_changed));
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
    Ok((canonical, fingerprint.as_str().to_owned(), false))
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
        if access.terminal {
            return Err(ScopeFailureKind::Supervisor);
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
    if active.origin != ThreadOrigin::BridgeCreated {
        // Actor eviction and router shutdown do not constitute an explicit
        // adopted-owner release. The coordinator retains or fences that
        // dedicated domain independently of actor residency.
        return;
    }
    let client = supervisor.borrow().client.clone();
    if let Some(client) = client {
        let _ = client
            .release_thread(&ThreadId::from(active.codex_thread_id))
            .await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn ensure_thread(
    scope: &ScopeKey,
    store: &StoreHandle,
    policy: &AccessPolicy,
    settings: &RouterSettings,
    client: &AppServerClient,
    cwd: &Path,
    fingerprint: &str,
    policy_changed: bool,
    context_tools: bool,
) -> Result<String, ThreadPreparationError> {
    if let Some(active) = store
        .active_thread(scope)
        .await
        .map_err(|_| ThreadPreparationError::Scope(ScopeFailureKind::Store))?
    {
        let required_version = if context_tools {
            CONTEXT_TOOLS_VERSION
        } else {
            0
        };
        if active.context_tools_version == required_version {
            let rpc_cwd = revalidate_workspace(policy, cwd, fingerprint)
                .map_err(ThreadPreparationError::Scope)?;
            let mut params = ThreadResumeParams::new(&active.codex_thread_id);
            params.overrides.cwd = Some(rpc_cwd);
            params.overrides.sandbox = Some(settings.sandbox);
            params.overrides.approval_policy = Some(settings.approval_policy.clone());
            params.overrides.model.clone_from(&settings.model);
            let thread = client
                .resume_thread(params)
                .await
                .map_err(ThreadPreparationError::Client)?;
            if policy_changed {
                store
                    .upsert_scope(scope, cwd, fingerprint)
                    .await
                    .map_err(|_| ThreadPreparationError::Scope(ScopeFailureKind::Store))?;
            }
            return Ok(thread.id);
        }
        store
            .archive_active_thread(scope)
            .await
            .map_err(|_| ThreadPreparationError::Scope(ScopeFailureKind::Store))?;
        let _ = client
            .release_thread(&ThreadId::from(active.codex_thread_id.as_str()))
            .await;
    }
    let rpc_cwd =
        revalidate_workspace(policy, cwd, fingerprint).map_err(ThreadPreparationError::Scope)?;
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
        .map_err(ThreadPreparationError::Client)?;
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
        .map_err(|_| ThreadPreparationError::Scope(ScopeFailureKind::Store))?;
    if policy_changed {
        store
            .upsert_scope(scope, cwd, fingerprint)
            .await
            .map_err(|_| ThreadPreparationError::Scope(ScopeFailureKind::Store))?;
    }
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

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, time::Duration};

    use super::*;
    use crate::{
        config::BridgeConfig,
        lark::{config::TenantBrand, credentials::LarkCredentials, normalize::InboundEvent},
        runtime::{intake::TenantNamespace, policy::PlatformRoots},
        store::{InboundEventState, ThreadAdoptionOutcome, ThreadAdoptionState},
    };
    use secrecy::SecretString;
    use tempfile::TempDir;
    use tokio::sync::Notify;

    #[derive(Clone, Default)]
    struct OrderedSink {
        calls: Arc<Mutex<Vec<String>>>,
        control_replies: Arc<Mutex<Vec<String>>>,
        changed: Arc<Notify>,
    }

    impl OrderedSink {
        fn record(&self, value: String) {
            self.calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(value);
            self.changed.notify_waiters();
        }

        fn snapshot(&self) -> Vec<String> {
            self.calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }

        fn control_replies(&self) -> Vec<String> {
            self.control_replies
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }

        async fn wait_for(&self, count: usize) {
            timeout(Duration::from_secs(2), async {
                loop {
                    let changed = self.changed.notified();
                    if self.snapshot().len() >= count {
                        break;
                    }
                    changed.await;
                }
            })
            .await
            .expect("ordered sink calls");
        }
    }

    impl DurableReplySink for OrderedSink {
        fn rejection_notice(
            &self,
            key: &InboundKey,
            event: &InboundEvent,
            reason: InboundRejectionKind,
        ) -> Result<NewOutboxRow, ReplySinkError> {
            self.record(format!("notice:{}", event.event_id));
            Ok(NewOutboxRow {
                idempotency_key: key.rejection_outbox_idempotency_key(reason),
                scope_key: event.scope.to_string(),
                kind: "notice".to_owned(),
                payload_json: format!("notice:{}", reason.as_str()),
                next_retry_ms: 0,
            })
        }

        fn control_reply(
            &self,
            key: &InboundKey,
            event: &InboundEvent,
            text: &str,
        ) -> Result<NewOutboxRow, ReplySinkError> {
            self.control_replies
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(text.to_owned());
            self.record(format!("control:{}", event.event_id));
            Ok(NewOutboxRow {
                idempotency_key: key.control_outbox_idempotency_key(),
                scope_key: event.scope.to_string(),
                kind: "control".to_owned(),
                payload_json: "control".to_owned(),
                next_retry_ms: 0,
            })
        }

        fn finalize(
            &self,
            _turn: TurnFinalization,
        ) -> BoxFuture<'static, Result<(), ReplySinkError>> {
            Box::pin(async { Err(ReplySinkError::Invariant) })
        }
    }

    struct ActorFixture {
        _temporary: TempDir,
        cwd: PathBuf,
        policy: AccessPolicy,
        settings: RouterSettings,
        tenant: TenantNamespace,
    }

    fn actor_fixture() -> ActorFixture {
        let temporary = tempfile::tempdir().expect("temporary root");
        let home = temporary.path().join("home");
        let cwd = home.join("workspace");
        std::fs::create_dir_all(&cwd).expect("workspace");
        let roots =
            PlatformRoots::new(&home, Vec::new(), Vec::new(), Vec::new()).expect("platform roots");
        let config = BridgeConfig {
            owners: vec!["owner".to_owned()],
            default_workspace: Some(cwd.clone()),
            workspace: crate::config::WorkspacePolicy {
                allow_roots: vec![cwd.clone()],
                ..crate::config::WorkspacePolicy::default()
            },
            ..BridgeConfig::default()
        };
        let policy = AccessPolicy::with_platform_roots(&config, &roots).expect("policy");
        let mut settings = RouterSettings::from_config(&config);
        settings.debounce = Duration::from_millis(20);
        let credentials = LarkCredentials::new(
            "scope_actor_test".to_owned(),
            SecretString::from("secret".to_owned()),
            TenantBrand::Feishu,
        );
        ActorFixture {
            _temporary: temporary,
            cwd,
            policy,
            settings,
            tenant: TenantNamespace::from_credentials(&credentials),
        }
    }

    fn actor_event(scope: &ScopeKey, event_id: &str, text: &str) -> InboundEvent {
        let (chat_id, thread_id) = match scope {
            ScopeKey::Chat(chat_id) => (chat_id.clone(), None),
            ScopeKey::Thread(chat_id, thread_id) => (chat_id.clone(), Some(thread_id.clone())),
        };
        InboundEvent {
            event_id: event_id.to_owned(),
            message_id: format!("message-{event_id}"),
            chat_id,
            sender_id: "owner".to_owned(),
            chat_type: ChatMode::P2p,
            thread_id,
            root_id: None,
            reply_to_message_id: None,
            text: text.to_owned(),
            mentions_bot: false,
            mention_all: false,
            sender_is_human: true,
            mentions: Vec::new(),
            parts: vec![MessagePart::Text {
                text: text.to_owned(),
            }],
            resources: Vec::new(),
            message_type: "text".to_owned(),
            create_time_ms: i64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis(),
            )
            .unwrap_or(i64::MAX),
            scope: scope.clone(),
        }
    }

    async fn queued_actor_event(
        store: &StoreHandle,
        tenant: &TenantNamespace,
        event: InboundEvent,
    ) -> (InboundKey, QueuedInboundEvent) {
        store
            .register_inbound(tenant, &event)
            .await
            .expect("register inbound");
        let key = InboundKey::new(tenant.clone(), event.event_id.clone());
        let permit = Arc::new(Semaphore::new(1))
            .acquire_owned()
            .await
            .expect("queued permit");
        (key, QueuedInboundEvent::new(event, permit))
    }

    fn pending_event(event_id: &str, key: &str) -> InboundEvent {
        InboundEvent {
            event_id: event_id.to_owned(),
            message_id: format!("message-{event_id}"),
            chat_id: "oc_pending_test".to_owned(),
            sender_id: "ou_pending_owner".to_owned(),
            chat_type: ChatMode::P2p,
            thread_id: None,
            root_id: None,
            reply_to_message_id: None,
            text: String::new(),
            mentions_bot: false,
            mention_all: false,
            sender_is_human: true,
            mentions: Vec::new(),
            parts: vec![MessagePart::Image(crate::lark::normalize::MediaPart {
                key: Some(key.to_owned()),
                thumbnail_key: None,
                metadata: crate::lark::normalize::MediaMetadata::default(),
                status: crate::lark::normalize::PartStatus::Available,
            })],
            resources: Vec::new(),
            message_type: "image".to_owned(),
            create_time_ms: 1,
            scope: ScopeKey::Chat("oc_pending_test".to_owned()),
        }
    }

    fn queue(ttl: Duration, max_count: usize, max_metadata_bytes: usize) -> PendingMediaQueue {
        PendingMediaQueue {
            items: VecDeque::new(),
            metadata_bytes: 0,
            ttl,
            max_count,
            max_metadata_bytes,
            generation: 0,
        }
    }

    #[test]
    fn rendered_adoption_command_round_trips_opaque_selector_without_injection() {
        use crate::runtime::adoption::{CandidateOwnership, ThreadCandidate};
        use crate::runtime::commands::parse_command;

        let selector = r#"opaque selector | "/release" \\ suffix"#;
        let page = ThreadCandidatePage {
            candidates: vec![ThreadCandidate {
                selector: selector.to_owned(),
                title: "Safe title".to_owned(),
                workspace_alias: "ws-0123456789abcdef01234567".to_owned(),
                source: "cli",
                updated_at: 42,
                observable_state: "idle_preflight_only",
                ownership: CandidateOwnership::Unverified,
            }],
            next_cursor: None,
            ownership_note: "ownership unverified",
        };

        let rendered = render_candidate_page(&page);
        let commands = rendered
            .lines()
            .filter(|line| line.starts_with("/adopt "))
            .collect::<Vec<_>>();
        assert_eq!(commands.len(), 1);
        assert_eq!(
            parse_command(commands[0]),
            Ok(Some(BridgeCommand::Adopt {
                selector: selector.to_owned()
            }))
        );
        assert!(!rendered.lines().any(|line| line == "/release"));
    }

    #[tokio::test]
    async fn control_arrival_ends_debounce_and_runs_after_the_ordinary_batch() {
        let mut fixture = actor_fixture();
        fixture.settings.debounce = Duration::from_millis(250);
        let store = StoreHandle::open_in_memory().await.expect("store");
        let scope = ScopeKey::Chat("serialized-control".to_owned());
        let (_supervisor_tx, supervisor) = watch::channel(SupervisorAccess {
            epoch: 0,
            client: None,
            profile_identity: None,
            terminal: true,
        });
        let adoption = Arc::new(ThreadAdoptionCoordinator::new(
            store.clone(),
            fixture.settings.backend.clone(),
            4,
        ));
        adoption.startup_fence().await.expect("startup fence");
        let sink = OrderedSink::default();
        let actor = ScopeActorHandle::spawn(
            scope.clone(),
            store.clone(),
            fixture.policy,
            fixture.settings,
            supervisor,
            Arc::new(Semaphore::new(1)),
            Arc::new(sink.clone()),
            None,
            None,
            None,
            adoption,
        );

        let (ordinary_key, ordinary) = queued_actor_event(
            &store,
            &fixture.tenant,
            actor_event(&scope, "ordinary-first", "ordinary message"),
        )
        .await;
        assert!(actor.try_route(ordinary_key.clone(), ordinary).is_ok());
        timeout(Duration::from_secs(1), async {
            while actor.state() != ScopeState::Debouncing {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("ordinary debounce");

        let (control_key, control) = queued_actor_event(
            &store,
            &fixture.tenant,
            actor_event(&scope, "control-second", "/threads"),
        )
        .await;
        assert!(
            actor
                .try_route_control(
                    control_key.clone(),
                    control,
                    ScopeControl::Command(BridgeCommand::Threads { cursor: None }),
                )
                .is_ok()
        );

        sink.wait_for(2).await;
        assert_eq!(
            sink.snapshot(),
            ["notice:ordinary-first", "control:control-second"]
        );
        timeout(Duration::from_secs(1), async {
            loop {
                let ordinary_state = store
                    .inbound_state(&fixture.tenant, &ordinary_key.event_id)
                    .await
                    .expect("ordinary state");
                let control_state = store
                    .inbound_state(&fixture.tenant, &control_key.event_id)
                    .await
                    .expect("control state");
                if ordinary_state == Some(InboundEventState::Rejected)
                    && control_state == Some(InboundEventState::Completed)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("durable ordered settlement");
        actor.shutdown().await;
        store.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn adopted_mapping_never_waits_for_or_falls_back_to_the_shared_client() {
        let mut fixture = actor_fixture();
        fixture.settings.debounce = Duration::from_millis(1);
        let store = StoreHandle::open_in_memory().await.expect("store");
        let scope = ScopeKey::Chat("external-no-fallback".to_owned());
        let fingerprint = fixture
            .policy
            .fingerprint(&fixture.cwd)
            .expect("fingerprint");
        store
            .upsert_scope(&scope, &fixture.cwd, fingerprint.as_str())
            .await
            .expect("scope");
        let reservation = store
            .reserve_thread_adoption(&scope, "persisted-no-fallback")
            .await
            .expect("reserve adoption");
        store
            .commit_thread_adoption(&reservation, &fixture.cwd, fingerprint.as_str())
            .await
            .expect("commit adoption");

        let adoption = Arc::new(ThreadAdoptionCoordinator::new(
            store.clone(),
            fixture.settings.backend.clone(),
            4,
        ));
        adoption.startup_fence().await.expect("startup fence");
        let (_supervisor_tx, supervisor) = watch::channel(SupervisorAccess {
            epoch: 7,
            client: None,
            profile_identity: None,
            // If the adopted branch accidentally enters `wait_for_client`, it
            // will wait forever because this non-terminal channel never changes.
            terminal: false,
        });
        let sink = OrderedSink::default();
        let actor = ScopeActorHandle::spawn(
            scope.clone(),
            store.clone(),
            fixture.policy,
            fixture.settings,
            supervisor,
            Arc::new(Semaphore::new(1)),
            Arc::new(sink.clone()),
            None,
            None,
            None,
            adoption,
        );
        let (key, ordinary) = queued_actor_event(
            &store,
            &fixture.tenant,
            actor_event(&scope, "external-ordinary", "must not start a replacement"),
        )
        .await;
        assert!(actor.try_route(key.clone(), ordinary).is_ok());

        sink.wait_for(1).await;
        timeout(Duration::from_secs(1), async {
            while store
                .inbound_state(&fixture.tenant, &key.event_id)
                .await
                .expect("inbound state")
                != Some(InboundEventState::Rejected)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("external route fails closed without shared readiness");
        actor.shutdown().await;

        let mapping = store
            .active_thread(&scope)
            .await
            .expect("active mapping")
            .expect("actor shutdown preserves explicit mapping");
        assert_eq!(mapping.origin, ThreadOrigin::ExternallyAdopted);
        assert_eq!(mapping.codex_thread_id, "persisted-no-fallback");
        let saga = store
            .active_thread_adoption(&scope)
            .await
            .expect("active saga")
            .expect("fenced saga");
        assert_eq!(saga.state, ThreadAdoptionState::RecoveryRequired);
        store.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn startup_fenced_precommit_adoptions_block_ordinary_routes_but_not_release_control() {
        let mut fixture = actor_fixture();
        fixture.settings.debounce = Duration::from_millis(1);
        let store = StoreHandle::open_in_memory().await.expect("store");
        let mapped_scope = ScopeKey::Chat("precommit-preserved-mapping".to_owned());
        let unmapped_scope = ScopeKey::Chat("precommit-without-mapping".to_owned());
        let fingerprint = fixture
            .policy
            .fingerprint(&fixture.cwd)
            .expect("fingerprint");
        for scope in [&mapped_scope, &unmapped_scope] {
            store
                .upsert_scope(scope, &fixture.cwd, fingerprint.as_str())
                .await
                .expect("scope");
        }
        store
            .record_active_thread(&mapped_scope, "preserved-bridge-thread")
            .await
            .expect("preserved bridge mapping");
        for (scope, target) in [
            (&mapped_scope, "persisted-mapped-target"),
            (&unmapped_scope, "persisted-unmapped-target"),
        ] {
            store
                .reserve_thread_adoption(scope, target)
                .await
                .expect("reserve adoption");
        }

        let adoption = Arc::new(ThreadAdoptionCoordinator::new(
            store.clone(),
            fixture.settings.backend.clone(),
            4,
        ));
        assert_eq!(adoption.startup_fence().await.expect("startup fence"), 2);
        let (_supervisor_tx, supervisor) = watch::channel(SupervisorAccess {
            epoch: 7,
            client: None,
            profile_identity: None,
            // A regression into either ordinary mapping branch would wait
            // forever for this deliberately non-terminal shared client.
            terminal: false,
        });
        let sink = OrderedSink::default();
        let active_turns = Arc::new(Semaphore::new(1));
        let mapped_actor = ScopeActorHandle::spawn(
            mapped_scope.clone(),
            store.clone(),
            fixture.policy.clone(),
            fixture.settings.clone(),
            supervisor.clone(),
            Arc::clone(&active_turns),
            Arc::new(sink.clone()),
            None,
            None,
            None,
            Arc::clone(&adoption),
        );
        let unmapped_actor = ScopeActorHandle::spawn(
            unmapped_scope.clone(),
            store.clone(),
            fixture.policy,
            fixture.settings,
            supervisor,
            active_turns,
            Arc::new(sink.clone()),
            None,
            None,
            None,
            adoption,
        );

        let (mapped_key, mapped_ordinary) = queued_actor_event(
            &store,
            &fixture.tenant,
            actor_event(
                &mapped_scope,
                "precommit-mapped-ordinary",
                "must not resume the preserved mapping",
            ),
        )
        .await;
        assert!(
            mapped_actor
                .try_route(mapped_key.clone(), mapped_ordinary)
                .is_ok()
        );
        let (release_key, release) = queued_actor_event(
            &store,
            &fixture.tenant,
            actor_event(&mapped_scope, "precommit-release", "/release"),
        )
        .await;
        assert!(
            mapped_actor
                .try_route_control(
                    release_key.clone(),
                    release,
                    ScopeControl::Command(BridgeCommand::Release),
                )
                .is_ok()
        );
        let (unmapped_key, unmapped_ordinary) = queued_actor_event(
            &store,
            &fixture.tenant,
            actor_event(
                &unmapped_scope,
                "precommit-unmapped-ordinary",
                "must not start a replacement thread",
            ),
        )
        .await;
        assert!(
            unmapped_actor
                .try_route(unmapped_key.clone(), unmapped_ordinary)
                .is_ok()
        );

        sink.wait_for(3).await;
        timeout(Duration::from_secs(1), async {
            loop {
                let mapped_state = store
                    .inbound_state(&fixture.tenant, &mapped_key.event_id)
                    .await
                    .expect("mapped ordinary state");
                let unmapped_state = store
                    .inbound_state(&fixture.tenant, &unmapped_key.event_id)
                    .await
                    .expect("unmapped ordinary state");
                let release_state = store
                    .inbound_state(&fixture.tenant, &release_key.event_id)
                    .await
                    .expect("release state");
                if mapped_state == Some(InboundEventState::Rejected)
                    && unmapped_state == Some(InboundEventState::Rejected)
                    && release_state == Some(InboundEventState::Completed)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fenced ordinary work and accessible release control settle");
        let calls = sink.snapshot();
        let mapped_notice = calls
            .iter()
            .position(|call| call == "notice:precommit-mapped-ordinary")
            .expect("mapped rejection notice");
        let release_reply = calls
            .iter()
            .position(|call| call == "control:precommit-release")
            .expect("release control reply");
        assert!(mapped_notice < release_reply, "scope mailbox order");

        mapped_actor.shutdown().await;
        unmapped_actor.shutdown().await;
        let mapping = store
            .active_thread(&mapped_scope)
            .await
            .expect("mapping")
            .expect("preserved bridge mapping");
        assert_eq!(mapping.origin, ThreadOrigin::BridgeCreated);
        assert_eq!(mapping.codex_thread_id, "preserved-bridge-thread");
        assert!(
            store
                .active_thread(&unmapped_scope)
                .await
                .expect("unmapped mapping")
                .is_none()
        );
        for scope in [&mapped_scope, &unmapped_scope] {
            let saga = store
                .active_thread_adoption(scope)
                .await
                .expect("active saga")
                .expect("recovery fence");
            assert_eq!(saga.state, ThreadAdoptionState::RecoveryRequired);
        }
        store.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn terminal_precommit_cleanup_replays_release_control_with_accurate_atomic_reply() {
        let mut fixture = actor_fixture();
        fixture.settings.debounce = Duration::from_millis(1);
        let store = StoreHandle::open_in_memory().await.expect("store");
        let scope = ScopeKey::Chat("precommit-release-replay".to_owned());
        let fingerprint = fixture
            .policy
            .fingerprint(&fixture.cwd)
            .expect("fingerprint");
        store
            .upsert_scope(&scope, &fixture.cwd, fingerprint.as_str())
            .await
            .expect("scope");
        store
            .record_active_thread(&scope, "preserved-before-cleanup")
            .await
            .expect("preserved bridge mapping");
        let reservation = store
            .reserve_thread_adoption(&scope, "uncommitted-cleanup-target")
            .await
            .expect("reserve adoption");
        let fenced = store
            .fence_thread_adoption(&reservation)
            .await
            .expect("recovery fence");
        // Model the crash gap precisely: the control is durably Received,
        // cleanup commits, and the process dies before the atomic reply
        // enqueue/Completed transition.
        let (crash_key, crash_control) = queued_actor_event(
            &store,
            &fixture.tenant,
            actor_event(&scope, "precommit-release-after-crash", "/release"),
        )
        .await;
        store
            .finish_thread_adoption_acquisition_failure(&fenced)
            .await
            .expect("durable cleanup before reply");

        let adoption = Arc::new(ThreadAdoptionCoordinator::new(
            store.clone(),
            fixture.settings.backend.clone(),
            4,
        ));
        assert_eq!(adoption.startup_fence().await.expect("startup fence"), 0);
        let (_supervisor_tx, supervisor) = watch::channel(SupervisorAccess {
            epoch: 7,
            client: None,
            profile_identity: None,
            terminal: false,
        });
        let sink = OrderedSink::default();
        let actor = ScopeActorHandle::spawn(
            scope.clone(),
            store.clone(),
            fixture.policy,
            fixture.settings,
            supervisor,
            Arc::new(Semaphore::new(1)),
            Arc::new(sink.clone()),
            None,
            None,
            None,
            adoption,
        );

        assert!(
            actor
                .try_route_control(
                    crash_key.clone(),
                    crash_control,
                    ScopeControl::Command(BridgeCommand::Release),
                )
                .is_ok()
        );
        let (retry_key, retry_control) = queued_actor_event(
            &store,
            &fixture.tenant,
            actor_event(&scope, "precommit-release-retry", "/release"),
        )
        .await;
        assert!(
            actor
                .try_route_control(
                    retry_key.clone(),
                    retry_control,
                    ScopeControl::Command(BridgeCommand::Release),
                )
                .is_ok()
        );
        let keys = [crash_key, retry_key];

        sink.wait_for(2).await;
        timeout(Duration::from_secs(1), async {
            loop {
                let mut completed = true;
                for key in &keys {
                    completed &= store
                        .inbound_state(&fixture.tenant, &key.event_id)
                        .await
                        .expect("release state")
                        == Some(InboundEventState::Completed);
                }
                if completed {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("replayed release controls settle atomically");
        let expected = "Release recovery complete. The uncommitted acquisition was durably closed after confirmed cleanup; no adopted mapping was removed, and any pre-existing bridge mapping remains active.";
        assert_eq!(
            sink.control_replies(),
            [expected.to_owned(), expected.to_owned()]
        );
        assert_eq!(store.outbox_depth().await.expect("outbox").pending, 2);

        actor.shutdown().await;
        let mapping = store
            .active_thread(&scope)
            .await
            .expect("mapping")
            .expect("preserved bridge mapping");
        assert_eq!(mapping.origin, ThreadOrigin::BridgeCreated);
        assert_eq!(mapping.codex_thread_id, "preserved-before-cleanup");
        let saga = store
            .thread_adoption_saga(&scope)
            .await
            .expect("saga")
            .expect("terminal saga");
        assert_eq!(saga.state, ThreadAdoptionState::Terminal);
        assert_eq!(saga.outcome, Some(ThreadAdoptionOutcome::AcquisitionFailed));
        store.shutdown().await.expect("shutdown");
    }

    #[test]
    fn reservation_restore_merges_chronologically_under_count_and_byte_caps() {
        let first = pending_event("restore-a", "key-a");
        let second = pending_event("restore-b", "key-b");
        let third = pending_event("restore-c", "key-c");
        let two_items = pending_metadata_bytes(&second) + pending_metadata_bytes(&third);
        let mut pending = queue(Duration::from_secs(60), 2, two_items);
        pending.stage(&first);
        pending.stage(&second);
        let (generation, mut reserved) = pending.reserve_all();
        pending.stage(&third);
        let tied_expiry = Instant::now() + Duration::from_secs(30);
        for item in &mut reserved {
            item.expires_at = tied_expiry;
        }
        pending.items[0].expires_at = tied_expiry;

        pending.restore(generation, reserved);

        let event_ids = pending
            .items
            .iter()
            .map(|item| item.draft.event_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(event_ids, ["restore-b", "restore-c"]);
        assert_eq!(pending.items.len(), 2);
        assert!(pending.metadata_bytes <= two_items);
        assert!(pending.items[0].expires_at <= pending.items[1].expires_at);
    }

    #[test]
    fn reservation_restore_never_extends_ttl_or_survives_generation_clear() {
        let old = pending_event("restore-expired", "key-old");
        let fresh = pending_event("restore-fresh", "key-fresh");
        let mut pending = queue(Duration::from_millis(10), 2, usize::MAX);
        pending.stage(&old);
        let (generation, reserved) = pending.reserve_all();
        std::thread::sleep(Duration::from_millis(20));
        pending.stage(&fresh);
        pending.restore(generation, reserved);
        assert_eq!(pending.items.len(), 1);
        assert_eq!(pending.items[0].draft.event_id, "restore-fresh");

        let (generation, reserved) = pending.reserve_all();
        pending.clear();
        pending.restore(generation, reserved);
        assert!(pending.items.is_empty());
        assert_eq!(pending.stats(), (0, 0));
    }
}
