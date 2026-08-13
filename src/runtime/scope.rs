//! One-scope runtime contracts shared by the router and reply projector.

use std::fmt;

use futures_util::future::BoxFuture;

use crate::codex::client::TurnOutcome;
use crate::lark::normalize::InboundEvent;
use crate::store::{InboundRejectionKind, NewOutboxRow, TurnResolution};

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
