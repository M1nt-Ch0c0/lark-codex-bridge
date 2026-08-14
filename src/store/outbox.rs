#![allow(clippy::doc_markdown)]

//! Durable outbound queue: enqueue with idempotency keys, atomic claim,
//! receipts, retry scheduling, and explicit `uncertain_delivery`.
//!
//! A Lark send whose outcome is unknown is recorded as
//! `uncertain_delivery` and never blindly retried as if it had failed
//! (handoff §2 rule 6, design §9).

use rusqlite::params;

use super::{StoreError, StoreHandle, now_ms, query_optional, request_bytes, sqlite_error};
use crate::limits::{
    OUTBOX_SWEEP_BATCH, STORE_OUTBOX_CLAIM_MAX_BATCH, STORE_OUTBOX_CLAIM_MAX_BYTES,
    STORE_OUTBOX_MAX_ATTEMPTS, STORE_OUTBOX_MAX_QUEUED_BYTES, STORE_OUTBOX_MAX_ROWS,
    STORE_OUTBOX_PAYLOAD_MAX_BYTES,
};

/// Outbox row lifecycle states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxState {
    /// Awaiting its retry window; claimable.
    Pending,
    /// Claimed by the pump; a send attempt is in flight.
    Sending,
    /// Delivered; Lark returned a receipt `message_id` (terminal).
    Sent,
    /// Retry budget exhausted (terminal).
    Failed,
    /// The send outcome is unknown; never auto-retried (terminal).
    UncertainDelivery,
}

impl OutboxState {
    /// Stable database representation.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Sending => "sending",
            Self::Sent => "sent",
            Self::Failed => "failed",
            Self::UncertainDelivery => "uncertain_delivery",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        match text {
            "pending" => Some(Self::Pending),
            "sending" => Some(Self::Sending),
            "sent" => Some(Self::Sent),
            "failed" => Some(Self::Failed),
            "uncertain_delivery" => Some(Self::UncertainDelivery),
            _ => None,
        }
    }
}

/// One row of the `outbox` table.
///
/// `Debug` reports IDs, kind, state, and byte size only — never the payload
/// body, which carries message content.
#[derive(Clone, PartialEq, Eq)]
pub struct OutboxRow {
    /// Row ID.
    pub id: i64,
    /// Idempotency key (unique).
    pub idempotency_key: String,
    /// Target scope key.
    pub scope_key: String,
    /// Message kind (open string: `progress`, `final`, `notice`, …).
    pub kind: String,
    /// Serialized Lark send body.
    pub payload_json: String,
    /// `payload_json` size in bytes, mirrored for byte-budget queries.
    pub payload_bytes: u64,
    /// Lifecycle state.
    pub state: OutboxState,
    /// Send attempts so far.
    pub attempts: u32,
    /// Earliest next claim time, milliseconds since the Unix epoch.
    pub next_retry_ms: i64,
    /// Lark receipt `message_id`, once sent.
    pub receipt_message_id: Option<String>,
    /// Creation time, milliseconds since the Unix epoch.
    pub created_ms: i64,
    /// Last update, milliseconds since the Unix epoch.
    pub updated_ms: i64,
}

impl std::fmt::Debug for OutboxRow {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OutboxRow")
            .field("id", &self.id)
            .field("idempotency_key", &self.idempotency_key)
            .field("scope_key", &self.scope_key)
            .field("kind", &self.kind)
            .field("payload_bytes", &self.payload_bytes)
            .field("state", &self.state)
            .field("attempts", &self.attempts)
            .field("next_retry_ms", &self.next_retry_ms)
            .field("receipt_message_id", &self.receipt_message_id)
            .field("created_ms", &self.created_ms)
            .field("updated_ms", &self.updated_ms)
            .finish_non_exhaustive()
    }
}

/// Fields needed to enqueue one outbox row.
#[derive(Clone, PartialEq, Eq)]
pub struct NewOutboxRow {
    /// Idempotency key (unique); re-enqueues return the existing row.
    pub idempotency_key: String,
    /// Target scope key.
    pub scope_key: String,
    /// Message kind (open string).
    pub kind: String,
    /// Serialized Lark send body.
    pub payload_json: String,
    /// Earliest claim time, milliseconds since the Unix epoch (`0` = now).
    pub next_retry_ms: i64,
}

impl std::fmt::Debug for NewOutboxRow {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NewOutboxRow")
            .field("idempotency_key", &self.idempotency_key)
            .field("scope_key", &self.scope_key)
            .field("kind", &self.kind)
            .field("payload_bytes", &self.payload_json.len())
            .field("next_retry_ms", &self.next_retry_ms)
            .finish()
    }
}

/// Outcome of an idempotent enqueue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboxEnqueue {
    /// The row was inserted.
    New(OutboxRow),
    /// The idempotency key already existed; carries the existing row, which
    /// was left untouched.
    Duplicate(OutboxRow),
}

/// Queue depth snapshot for `/status`: counts and queued bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OutboxDepth {
    /// Rows waiting to be claimed.
    pub pending: u64,
    /// Rows claimed but not yet completed.
    pub sending: u64,
    /// Terminally failed rows.
    pub failed: u64,
    /// Rows with unknown delivery outcome.
    pub uncertain: u64,
    /// Total payload bytes parked in `pending` + `sending` rows.
    pub queued_bytes: u64,
}

impl StoreHandle {
    /// Enqueues one outbound message, deduplicated by `idempotency_key`.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::PayloadTooLarge`] when the serialized body
    /// exceeds [`STORE_OUTBOX_PAYLOAD_MAX_BYTES`] (rejected before the row
    /// enters the writer channel), or when the writer task or SQLite fails.
    pub async fn enqueue_outbox(&self, row: NewOutboxRow) -> Result<OutboxEnqueue, StoreError> {
        let payload_bytes = row.payload_json.len();
        if payload_bytes > STORE_OUTBOX_PAYLOAD_MAX_BYTES {
            return Err(StoreError::PayloadTooLarge {
                context: "enqueueing an outbox payload",
                limit: STORE_OUTBOX_PAYLOAD_MAX_BYTES as u64,
            });
        }
        let payload_bytes = u64::try_from(payload_bytes).unwrap_or(u64::MAX);
        let payload_bytes_sql = i64::try_from(payload_bytes).unwrap_or(i64::MAX);
        let request_size = request_bytes(&[
            &row.idempotency_key,
            &row.scope_key,
            &row.kind,
            &row.payload_json,
        ]);
        self.run_sized(request_size, move |connection| {
            let exists: bool = connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM outbox WHERE idempotency_key = ?1)",
                    params![row.idempotency_key],
                    |result| result.get(0),
                )
                .map_err(|error| sqlite_error("checking an outbox idempotency key", &error))?;
            if !exists {
                let (count, bytes): (i64, i64) = connection
                    .query_row(
                        "SELECT COUNT(*), COALESCE(SUM(payload_bytes), 0) FROM outbox
                         WHERE state IN ('pending', 'sending')",
                        [],
                        |result| Ok((result.get(0)?, result.get(1)?)),
                    )
                    .map_err(|error| sqlite_error("checking outbox capacity", &error))?;
                let count = u64::try_from(count).unwrap_or(u64::MAX);
                let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
                if count >= STORE_OUTBOX_MAX_ROWS
                    || bytes.saturating_add(payload_bytes) > STORE_OUTBOX_MAX_QUEUED_BYTES
                {
                    return Err(StoreError::CapacityExceeded {
                        context: "enqueueing an outbox row",
                    });
                }
            }
            let now = now_ms();
            let inserted = connection
                .execute(
                    "INSERT OR IGNORE INTO outbox
                     (idempotency_key, scope_key, kind, payload_json, payload_bytes,
                      state, attempts, next_retry_ms, created_ms, updated_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5, 'pending', 0, ?6, ?7, ?7)",
                    params![
                        row.idempotency_key,
                        row.scope_key,
                        row.kind,
                        row.payload_json,
                        payload_bytes_sql,
                        row.next_retry_ms,
                        now,
                    ],
                )
                .map_err(|error| sqlite_error("enqueueing an outbox row", &error))?;
            let existing = connection.query_row(
                "SELECT id, idempotency_key, scope_key, kind, payload_json, payload_bytes,
                        state, attempts, next_retry_ms, receipt_message_id, created_ms, updated_ms
                 FROM outbox WHERE idempotency_key = ?1",
                params![row.idempotency_key],
                read_outbox_row,
            );
            let stored = query_optional(existing, "re-reading an outbox row")?.ok_or(
                StoreError::NotFound {
                    context: "re-reading an outbox row",
                },
            )?;
            Ok(if inserted == 1 {
                OutboxEnqueue::New(stored)
            } else {
                OutboxEnqueue::Duplicate(stored)
            })
        })
        .await
    }

    /// Atomically claims up to `limit` due `pending` rows, transitioning them
    /// to `sending` in one statement. `limit` is clamped to
    /// [`STORE_OUTBOX_CLAIM_MAX_BATCH`].
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or SQLite fails.
    pub async fn claim_outbox_batch(
        &self,
        now_ms_value: i64,
        limit: u32,
    ) -> Result<Vec<OutboxRow>, StoreError> {
        let limit = limit.min(STORE_OUTBOX_CLAIM_MAX_BATCH);
        self.run(move |connection| {
            let transaction = connection
                .transaction()
                .map_err(|error| sqlite_error("starting an outbox claim", &error))?;
            let mut statement = transaction
                .prepare(
                    "SELECT id, idempotency_key, scope_key, kind, payload_json, payload_bytes,
                            state, attempts, next_retry_ms, receipt_message_id, created_ms, updated_ms
                     FROM outbox WHERE state = 'pending' AND next_retry_ms <= ?1 ORDER BY id",
                )
                .map_err(|error| sqlite_error("reading an outbox claim", &error))?;
            let candidates = statement
                .query_map(params![now_ms_value], read_outbox_row)
                .map_err(|error| sqlite_error("reading an outbox claim", &error))?;
            let mut rows = Vec::new();
            let mut total_bytes = 0_u64;
            for candidate in candidates {
                let row = candidate.map_err(|error| sqlite_error("decoding an outbox claim", &error))?;
                if rows.len() >= usize::try_from(limit).unwrap_or(usize::MAX)
                    || total_bytes.saturating_add(row.payload_bytes)
                        > u64::try_from(STORE_OUTBOX_CLAIM_MAX_BYTES).unwrap_or(u64::MAX)
                {
                    break;
                }
                total_bytes = total_bytes.saturating_add(row.payload_bytes);
                rows.push(row);
            }
            drop(statement);
            for row in &rows {
                transaction
                    .execute(
                        "UPDATE outbox SET state = 'sending', updated_ms = ?2 WHERE id = ?1",
                        params![row.id, now_ms()],
                    )
                    .map_err(|error| sqlite_error("claiming an outbox batch", &error))?;
            }
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing an outbox claim", &error))?;
            for row in &mut rows {
                row.state = OutboxState::Sending;
            }
            Ok(rows)
        })
        .await
    }

    /// Marks a claimed row `sent` and records the Lark receipt `message_id`.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`] for an unknown row and
    /// [`StoreError::InvalidTransition`] when the row is not `sending`.
    pub async fn complete_outbox(
        &self,
        id: i64,
        receipt_message_id: &str,
    ) -> Result<(), StoreError> {
        let receipt_message_id = receipt_message_id.to_owned();
        let request_size = request_bytes(&[&receipt_message_id]);
        self.run_sized(request_size, move |connection| {
            require_sending(connection, id, "completing an outbox row")?;
            connection
                .execute(
                    "UPDATE outbox
                     SET state = 'sent', receipt_message_id = ?2, updated_ms = ?3
                     WHERE id = ?1",
                    params![id, receipt_message_id, now_ms()],
                )
                .map_err(|error| sqlite_error("completing an outbox row", &error))?;
            Ok(())
        })
        .await
    }

    /// Records a failed send attempt on a claimed row.
    ///
    /// `uncertain` marks the row `uncertain_delivery` (unknown outcome,
    /// never auto-retried). Otherwise the row goes back to `pending` with
    /// the given attempt count and retry time, or to terminal `failed` once
    /// `attempts` reaches [`STORE_OUTBOX_MAX_ATTEMPTS`].
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`] for an unknown row and
    /// [`StoreError::InvalidTransition`] when the row is not `sending`.
    pub async fn fail_outbox(
        &self,
        id: i64,
        attempts: u32,
        next_retry_ms: i64,
        uncertain: bool,
    ) -> Result<(), StoreError> {
        self.run(move |connection| {
            let stored_attempts = require_sending(connection, id, "failing an outbox row")?;
            let expected_attempts = stored_attempts.saturating_add(1);
            if attempts != expected_attempts {
                return Err(StoreError::InvalidTransition {
                    context: "recording a non-monotonic outbox attempt",
                });
            }
            let state = if uncertain {
                OutboxState::UncertainDelivery
            } else if attempts >= STORE_OUTBOX_MAX_ATTEMPTS {
                OutboxState::Failed
            } else {
                OutboxState::Pending
            };
            connection
                .execute(
                    "UPDATE outbox
                     SET state = ?2, attempts = ?3, next_retry_ms = ?4, updated_ms = ?5
                     WHERE id = ?1",
                    params![
                        id,
                        state.as_str(),
                        i64::from(attempts),
                        next_retry_ms,
                        now_ms()
                    ],
                )
                .map_err(|error| sqlite_error("failing an outbox row", &error))?;
            Ok(())
        })
        .await
    }

    /// Marks a claimed row terminally `failed` without further attempts.
    ///
    /// Used for definitive failures — permanent authentication rejection, an
    /// oversize body, or a corrupt payload — where a bounded retry can never
    /// succeed. The row is kept (with its attempts and payload) for
    /// diagnostics.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`] for an unknown row and
    /// [`StoreError::InvalidTransition`] when the row is not `sending`.
    pub async fn fail_outbox_terminal(&self, id: i64) -> Result<(), StoreError> {
        self.run(move |connection| {
            require_sending(connection, id, "terminally failing an outbox row")?;
            connection
                .execute(
                    "UPDATE outbox SET state = 'failed', updated_ms = ?2 WHERE id = ?1",
                    params![id, now_ms()],
                )
                .map_err(|error| sqlite_error("terminally failing an outbox row", &error))?;
            Ok(())
        })
        .await
    }

    /// Returns a claimed `sending` row to `pending` without counting a send
    /// attempt.
    ///
    /// The pump uses this to re-park rows when the Lark transport disconnects
    /// after a claim but before any send, so a disconnect churn can never
    /// exhaust the retry budget without a single real send attempt.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`] for an unknown row and
    /// [`StoreError::InvalidTransition`] when the row is not `sending`.
    pub async fn release_outbox_claim(&self, id: i64) -> Result<(), StoreError> {
        self.release_outbox_claim_at(id, now_ms()).await
    }

    /// Returns a claimed `sending` row to `pending` without counting a send
    /// attempt, parked until the given retry time.
    ///
    /// The pump uses this to re-park a batch tail after an earlier row defers
    /// its retry: the tail is released with a retry time no earlier than the
    /// deferred row's, so a later row can never overtake it.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`] for an unknown row and
    /// [`StoreError::InvalidTransition`] when the row is not `sending`.
    pub async fn release_outbox_claim_at(
        &self,
        id: i64,
        next_retry_ms: i64,
    ) -> Result<(), StoreError> {
        self.run(move |connection| {
            require_sending(connection, id, "releasing an outbox claim")?;
            connection
                .execute(
                    "UPDATE outbox SET state = 'pending', next_retry_ms = ?2, updated_ms = ?3
                     WHERE id = ?1",
                    params![id, next_retry_ms, now_ms()],
                )
                .map_err(|error| sqlite_error("releasing an outbox claim", &error))?;
            Ok(())
        })
        .await
    }

    /// Marks rows stranded in `sending` by a prior process as explicitly
    /// uncertain. They are not silently replayed because delivery may have
    /// reached Lark before the process died.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or `SQLite` fails.
    pub async fn recover_sending_outbox(&self) -> Result<u64, StoreError> {
        self.run(|connection| {
            let changed = connection
                .execute(
                    "UPDATE outbox SET state = 'uncertain_delivery', updated_ms = ?1
                     WHERE state = 'sending'",
                    params![now_ms()],
                )
                .map_err(|error| sqlite_error("recovering sending outbox rows", &error))?;
            Ok(u64::try_from(changed).unwrap_or(u64::MAX))
        })
        .await
    }

    /// Deletes terminal (`sent`/`failed`/`uncertain_delivery`) outbox rows
    /// last updated before `older_than_ms`, in ascending `updated_ms` order,
    /// returning the number of pruned rows. `max_rows` is clamped to
    /// [`OUTBOX_SWEEP_BATCH`]; `pending` and `sending` rows are never swept.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or SQLite fails.
    pub async fn sweep_terminal_outbox(
        &self,
        older_than_ms: i64,
        max_rows: u32,
    ) -> Result<u64, StoreError> {
        let max_rows = max_rows.min(OUTBOX_SWEEP_BATCH);
        if max_rows == 0 {
            return Ok(0);
        }
        self.run(move |connection| {
            let deleted = connection
                .execute(
                    "DELETE FROM outbox
                     WHERE id IN (
                         SELECT id FROM outbox
                         WHERE state IN ('sent', 'failed', 'uncertain_delivery')
                           AND updated_ms < ?1
                         ORDER BY updated_ms, id LIMIT ?2
                     )",
                    params![older_than_ms, max_rows],
                )
                .map_err(|error| sqlite_error("sweeping terminal outbox rows", &error))?;
            Ok(u64::try_from(deleted).unwrap_or(u64::MAX))
        })
        .await
    }

    /// Reads one outbox row by ID.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or SQLite fails.
    pub async fn outbox_row(&self, id: i64) -> Result<Option<OutboxRow>, StoreError> {
        self.run(move |connection| {
            let row = connection.query_row(
                "SELECT id, idempotency_key, scope_key, kind, payload_json, payload_bytes,
                        state, attempts, next_retry_ms, receipt_message_id, created_ms, updated_ms
                 FROM outbox WHERE id = ?1",
                params![id],
                read_outbox_row,
            );
            query_optional(row, "reading an outbox row")
        })
        .await
    }

    /// Queue depth snapshot (counts and queued bytes) for `/status`.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or SQLite fails.
    pub async fn outbox_depth(&self) -> Result<OutboxDepth, StoreError> {
        self.run(|connection| {
            let mut statement = connection
                .prepare(
                    "SELECT state, COUNT(*), COALESCE(SUM(payload_bytes), 0)
                     FROM outbox GROUP BY state",
                )
                .map_err(|error| sqlite_error("reading the outbox depth", &error))?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })
                .map_err(|error| sqlite_error("reading the outbox depth", &error))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| sqlite_error("reading the outbox depth", &error))?;
            let mut depth = OutboxDepth::default();
            for (state, count, bytes) in rows {
                let count = u64::try_from(count).unwrap_or(0);
                let bytes = u64::try_from(bytes).unwrap_or(0);
                match OutboxState::parse(&state) {
                    Some(OutboxState::Pending) => {
                        depth.pending = count;
                        depth.queued_bytes += bytes;
                    }
                    Some(OutboxState::Sending) => {
                        depth.sending = count;
                        depth.queued_bytes += bytes;
                    }
                    Some(OutboxState::Failed) => depth.failed = count,
                    Some(OutboxState::UncertainDelivery) => depth.uncertain = count,
                    Some(OutboxState::Sent) | None => {}
                }
            }
            Ok(depth)
        })
        .await
    }
}

fn require_sending(
    connection: &rusqlite::Connection,
    id: i64,
    context: &'static str,
) -> Result<u32, StoreError> {
    let (state, attempts): (String, i64) = connection
        .query_row(
            "SELECT state, attempts FROM outbox WHERE id = ?1",
            params![id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| match error {
            rusqlite::Error::QueryReturnedNoRows => StoreError::NotFound { context },
            other => sqlite_error("reading an outbox state", &other),
        })?;
    if OutboxState::parse(&state) != Some(OutboxState::Sending) {
        return Err(StoreError::InvalidTransition { context });
    }
    Ok(u32::try_from(attempts).unwrap_or(u32::MAX))
}

fn read_outbox_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<OutboxRow> {
    let state: String = row.get(6)?;
    Ok(OutboxRow {
        id: row.get(0)?,
        idempotency_key: row.get(1)?,
        scope_key: row.get(2)?,
        kind: row.get(3)?,
        payload_json: row.get(4)?,
        payload_bytes: u64::try_from(row.get::<_, i64>(5)?).unwrap_or(0),
        state: OutboxState::parse(&state).ok_or_else(|| {
            rusqlite::Error::InvalidColumnType(6, "state".to_owned(), rusqlite::types::Type::Text)
        })?,
        attempts: u32::try_from(row.get::<_, i64>(7)?).unwrap_or(0),
        next_retry_ms: row.get(8)?,
        receipt_message_id: row.get(9)?,
        created_ms: row.get(10)?,
        updated_ms: row.get(11)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn seed_row(store: &StoreHandle, key: &str, state: OutboxState, updated_ms: i64) {
        let key = key.to_owned();
        store
            .run(move |connection| {
                connection
                    .execute(
                        "INSERT INTO outbox
                         (idempotency_key, scope_key, kind, payload_json, payload_bytes,
                          state, attempts, next_retry_ms, receipt_message_id, created_ms, updated_ms)
                         VALUES (?1, 'im:oc', 'final', '{}', 2, ?2, 0, 0, NULL, 1, ?3)",
                        params![key, state.as_str(), updated_ms],
                    )
                    .map_err(|error| sqlite_error("seeding an outbox sweep row", &error))?;
                Ok(())
            })
            .await
            .expect("seed row");
    }

    async fn remaining_keys(store: &StoreHandle) -> Vec<String> {
        store
            .run(|connection| {
                let mut statement = connection
                    .prepare("SELECT idempotency_key FROM outbox ORDER BY idempotency_key")
                    .map_err(|error| sqlite_error("reading remaining outbox rows", &error))?;
                let keys = statement
                    .query_map([], |row| row.get::<_, String>(0))
                    .map_err(|error| sqlite_error("mapping remaining outbox rows", &error))?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| sqlite_error("collecting remaining outbox rows", &error))?;
                Ok(keys)
            })
            .await
            .expect("remaining keys")
    }

    #[tokio::test]
    async fn sweep_deletes_only_overage_terminal_rows() {
        let store = StoreHandle::open_in_memory().await.expect("store");
        seed_row(&store, "sent_old", OutboxState::Sent, 1_000).await;
        seed_row(&store, "failed_old", OutboxState::Failed, 1_000).await;
        seed_row(
            &store,
            "uncertain_old",
            OutboxState::UncertainDelivery,
            1_000,
        )
        .await;
        seed_row(&store, "sent_recent", OutboxState::Sent, 1_000_000).await;
        seed_row(&store, "pending_old", OutboxState::Pending, 1_000).await;
        seed_row(&store, "sending_old", OutboxState::Sending, 1_000).await;

        let deleted = store
            .sweep_terminal_outbox(5_000, 256)
            .await
            .expect("sweep");
        assert_eq!(deleted, 3, "the three over-age terminal rows are deleted");

        let depth = store.outbox_depth().await.expect("depth");
        assert_eq!(depth.failed, 0);
        assert_eq!(depth.uncertain, 0);
        assert_eq!(depth.pending, 1, "pending rows are never swept");
        assert_eq!(depth.sending, 1, "sending rows are never swept");
        assert_eq!(
            remaining_keys(&store).await,
            vec![
                "pending_old".to_owned(),
                "sending_old".to_owned(),
                "sent_recent".to_owned(),
            ],
            "only the over-age terminal rows are deleted"
        );
    }

    #[tokio::test]
    async fn sweep_is_bounded_and_ordered_by_updated_ms() {
        let store = StoreHandle::open_in_memory().await.expect("store");
        seed_row(&store, "a", OutboxState::Sent, 1_000).await;
        seed_row(&store, "b", OutboxState::Sent, 1_001).await;
        seed_row(&store, "c", OutboxState::Sent, 1_002).await;
        seed_row(&store, "d", OutboxState::Sent, 1_003).await;
        seed_row(&store, "e", OutboxState::Sent, 1_004).await;

        let deleted = store.sweep_terminal_outbox(2_000, 3).await.expect("sweep");
        assert_eq!(deleted, 3, "at most max_rows are deleted in one sweep");
        assert_eq!(
            remaining_keys(&store).await,
            vec!["d".to_owned(), "e".to_owned()],
            "the newest rows survive, so deletion follows ascending updated_ms"
        );
    }

    #[tokio::test]
    async fn sweep_clamps_to_the_global_batch_bound() {
        let store = StoreHandle::open_in_memory().await.expect("store");
        for index in 0..OUTBOX_SWEEP_BATCH + 2 {
            seed_row(&store, &format!("row-{index}"), OutboxState::Sent, 1_000).await;
        }

        let deleted = store
            .sweep_terminal_outbox(2_000, u32::MAX)
            .await
            .expect("sweep");
        assert_eq!(
            deleted,
            u64::from(OUTBOX_SWEEP_BATCH),
            "an oversized request is clamped to OUTBOX_SWEEP_BATCH"
        );
        assert_eq!(
            remaining_keys(&store).await.len(),
            2,
            "the two newest rows survive the clamped sweep"
        );
    }

    #[tokio::test]
    async fn sweep_is_idempotent() {
        let store = StoreHandle::open_in_memory().await.expect("store");
        seed_row(&store, "a", OutboxState::Sent, 1_000).await;
        seed_row(&store, "b", OutboxState::Failed, 1_000).await;

        let first = store
            .sweep_terminal_outbox(2_000, 256)
            .await
            .expect("first sweep");
        assert_eq!(first, 2);
        let second = store
            .sweep_terminal_outbox(2_000, 256)
            .await
            .expect("second sweep");
        assert_eq!(second, 0, "a repeated sweep deletes nothing further");
    }

    #[tokio::test]
    async fn release_outbox_claim_at_parks_until_the_given_retry_time() {
        let store = StoreHandle::open_in_memory().await.expect("store");
        store
            .enqueue_outbox(NewOutboxRow {
                idempotency_key: "1:final:evt_1".to_owned(),
                scope_key: "im:oc".to_owned(),
                kind: "final".to_owned(),
                payload_json: r#"{"version":1,"op":"reply_text","message_id":"m","text":"t"}"#
                    .to_owned(),
                next_retry_ms: 0,
            })
            .await
            .expect("enqueue");
        let claimed = store.claim_outbox_batch(now_ms(), 1).await.expect("claim");
        assert_eq!(claimed.len(), 1);

        store
            .release_outbox_claim_at(claimed[0].id, 123_456)
            .await
            .expect("release");

        let row = store
            .outbox_row(claimed[0].id)
            .await
            .expect("read")
            .expect("exists");
        assert_eq!(row.state, OutboxState::Pending);
        assert_eq!(
            row.next_retry_ms, 123_456,
            "the tail is parked until the retry time"
        );
        assert_eq!(
            row.attempts, 0,
            "releasing a claim never counts a send attempt"
        );
    }
}
