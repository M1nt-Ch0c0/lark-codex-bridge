#![allow(clippy::doc_markdown)]

//! Inbound event dedup registration and its state machine.
//!
//! Legal transitions are `received → accepted → completed|rejected` plus
//! `received → rejected`; terminal states are final. Anything else is a
//! [`StoreError::InvalidTransition`], so a duplicate redelivery within the
//! TTL can never restart Codex (design §5.3).

use rusqlite::params;

use super::{StoreError, StoreHandle, now_ms, query_optional, request_bytes, sqlite_error};
use crate::lark::normalize::InboundEvent;
use crate::limits::{
    STORE_INBOUND_MAX_BYTES, STORE_INBOUND_MAX_ROWS, STORE_REJECTION_REASON_MAX_BYTES,
};

/// Processing state of one registered inbound event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundEventState {
    /// Durably registered, not yet picked up by the scope runtime.
    Received,
    /// Accepted by the scope runtime; work is in flight.
    Accepted,
    /// Fully processed (terminal).
    Completed,
    /// Refused by policy, overload, or validation (terminal).
    Rejected,
}

impl InboundEventState {
    /// Stable database representation.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::Accepted => "accepted",
            Self::Completed => "completed",
            Self::Rejected => "rejected",
        }
    }

    /// Parses the database representation.
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "received" => Some(Self::Received),
            "accepted" => Some(Self::Accepted),
            "completed" => Some(Self::Completed),
            "rejected" => Some(Self::Rejected),
            _ => None,
        }
    }

    /// Whether the state is terminal (`completed`/`rejected`).
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Rejected)
    }
}

/// Outcome of registering one inbound event against the dedup table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DedupOutcome {
    /// First delivery within the TTL; the row was inserted as `received`.
    New,
    /// This `(tenant, event_id)` was already registered; carries the prior
    /// state so the caller can log or absorb the redelivery.
    Duplicate {
        /// State of the existing row.
        state: InboundEventState,
    },
}

impl StoreHandle {
    /// Registers one inbound event under `(tenant, event_id)`.
    ///
    /// Only IDs and the scope key are persisted — never message text.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or SQLite fails.
    pub async fn register_inbound(
        &self,
        tenant: &str,
        event: &InboundEvent,
    ) -> Result<DedupOutcome, StoreError> {
        let tenant = tenant.to_owned();
        let event_id = event.event_id.clone();
        let message_id = event.message_id.clone();
        let scope_key = event.scope.to_string();
        let persisted_bytes = request_bytes(&[&tenant, &event_id, &message_id, &scope_key]);
        self.run_sized(persisted_bytes, move |connection| {
            let exists: bool = connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM inbound_events WHERE tenant = ?1 AND event_id = ?2)",
                    params![tenant, event_id],
                    |row| row.get(0),
                )
                .map_err(|error| sqlite_error("checking an inbound event", &error))?;
            if !exists {
                let (count, stored_bytes): (i64, i64) = connection
                    .query_row(
                        "SELECT COUNT(*), COALESCE(SUM(
                             LENGTH(CAST(tenant AS BLOB)) + LENGTH(CAST(event_id AS BLOB)) +
                             LENGTH(CAST(message_id AS BLOB)) + LENGTH(CAST(scope_key AS BLOB))
                         ), 0) FROM inbound_events",
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(|error| sqlite_error("checking inbound capacity", &error))?;
                if u64::try_from(count).unwrap_or(u64::MAX) >= STORE_INBOUND_MAX_ROWS
                    || u64::try_from(stored_bytes).unwrap_or(u64::MAX)
                        .saturating_add(u64::try_from(persisted_bytes).unwrap_or(u64::MAX))
                        > STORE_INBOUND_MAX_BYTES
                {
                    return Err(StoreError::CapacityExceeded {
                        context: "registering an inbound event",
                    });
                }
            }
            let now = now_ms();
            let inserted = connection
                .execute(
                    "INSERT OR IGNORE INTO inbound_events
                     (tenant, event_id, message_id, scope_key, state, first_seen_ms, updated_ms)
                     VALUES (?1, ?2, ?3, ?4, 'received', ?5, ?5)",
                    params![tenant, event_id, message_id, scope_key, now],
                )
                .map_err(|error| sqlite_error("registering an inbound event", &error))?;
            if inserted == 1 {
                return Ok(DedupOutcome::New);
            }
            let state = read_inbound_state(connection, &tenant, &event_id)?.ok_or(
                StoreError::NotFound {
                    context: "re-reading a duplicate inbound event",
                },
            )?;
            Ok(DedupOutcome::Duplicate { state })
        })
        .await
    }

    /// Returns the current state of one registered inbound event.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or SQLite fails.
    pub async fn inbound_state(
        &self,
        tenant: &str,
        event_id: &str,
    ) -> Result<Option<InboundEventState>, StoreError> {
        let tenant = tenant.to_owned();
        let event_id = event_id.to_owned();
        let request_size = request_bytes(&[&tenant, &event_id]);
        self.run_sized(request_size, move |connection| {
            read_inbound_state(connection, &tenant, &event_id)
        })
        .await
    }

    /// Transitions one inbound event along the legal state machine.
    ///
    /// `reason` is stored (truncated to
    /// [`STORE_REJECTION_REASON_MAX_BYTES`]) when the target state is
    /// `rejected`; it must be an operator-facing classification, never
    /// message content.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`] for an unknown event and
    /// [`StoreError::InvalidTransition`] for any transition outside
    /// `received → accepted → completed|rejected` / `received → rejected`.
    pub async fn transition_inbound(
        &self,
        tenant: &str,
        event_id: &str,
        to: InboundEventState,
        reason: Option<&str>,
    ) -> Result<(), StoreError> {
        let tenant = tenant.to_owned();
        let event_id = event_id.to_owned();
        let reason = reason.map(truncate_reason);
        let bytes = request_bytes(&[&tenant, &event_id, reason.as_deref().unwrap_or_default()]);
        self.run_sized(bytes, move |connection| {
            let from = read_inbound_state(connection, &tenant, &event_id)?.ok_or(
                StoreError::NotFound {
                    context: "transitioning an unknown inbound event",
                },
            )?;
            if !legal_inbound_transition(from, to) {
                return Err(StoreError::InvalidTransition {
                    context: "transitioning an inbound event",
                });
            }
            let updated = connection
                .execute(
                    "UPDATE inbound_events
                     SET state = ?3, updated_ms = ?4, rejection_reason = ?5
                     WHERE tenant = ?1 AND event_id = ?2",
                    params![tenant, event_id, to.as_str(), now_ms(), reason],
                )
                .map_err(|error| sqlite_error("transitioning an inbound event", &error))?;
            debug_assert_eq!(updated, 1);
            Ok(())
        })
        .await
    }

    /// Deletes terminal (`completed`/`rejected`) inbound rows last updated
    /// before `older_than_ms`, returning the number of pruned rows. Non-
    /// terminal rows are never swept.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or SQLite fails.
    pub async fn sweep_inbound(&self, older_than_ms: i64) -> Result<u64, StoreError> {
        self.run(move |connection| {
            let deleted = connection
                .execute(
                    "DELETE FROM inbound_events
                     WHERE state IN ('completed', 'rejected') AND updated_ms < ?1",
                    params![older_than_ms],
                )
                .map_err(|error| sqlite_error("sweeping inbound events", &error))?;
            Ok(u64::try_from(deleted).unwrap_or(u64::MAX))
        })
        .await
    }
}

fn read_inbound_state(
    connection: &rusqlite::Connection,
    tenant: &str,
    event_id: &str,
) -> Result<Option<InboundEventState>, StoreError> {
    let row = connection.query_row(
        "SELECT state FROM inbound_events WHERE tenant = ?1 AND event_id = ?2",
        params![tenant, event_id],
        |row| row.get::<_, String>(0),
    );
    query_optional(row, "reading an inbound event state")?
        .map(|state| {
            InboundEventState::parse(&state).ok_or(StoreError::Sqlite {
                context: "decoding an inbound event state",
                code: None,
            })
        })
        .transpose()
}

fn legal_inbound_transition(from: InboundEventState, to: InboundEventState) -> bool {
    use InboundEventState::{Accepted, Completed, Received, Rejected};
    matches!(
        (from, to),
        (Received, Accepted | Rejected) | (Accepted, Completed | Rejected)
    )
}

fn truncate_reason(reason: &str) -> String {
    if reason.len() <= STORE_REJECTION_REASON_MAX_BYTES {
        return reason.to_owned();
    }
    let mut end = STORE_REJECTION_REASON_MAX_BYTES;
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    reason[..end].to_owned()
}
