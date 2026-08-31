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
    OUTBOX_SWEEP_BATCH, OUTBOX_TERMINAL_MAX_BYTES, OUTBOX_TERMINAL_MAX_ROWS,
    OUTBOX_TERMINAL_RETENTION_MS, STORE_OUTBOX_CLAIM_MAX_BATCH, STORE_OUTBOX_CLAIM_MAX_BYTES,
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
/// `Debug` reports the row ID, kind, state, and byte/length values only —
/// never the idempotency key, scope key, receipt message id, or payload body.
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
            .field("idempotency_key_len", &self.idempotency_key.len())
            .field("scope_key_len", &self.scope_key.len())
            .field("kind", &self.kind)
            .field("payload_bytes", &self.payload_bytes)
            .field("state", &self.state)
            .field("attempts", &self.attempts)
            .field("next_retry_ms", &self.next_retry_ms)
            .field(
                "receipt_message_id_len",
                &self.receipt_message_id.as_ref().map(String::len),
            )
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
            .field("idempotency_key_len", &self.idempotency_key.len())
            .field("scope_key_len", &self.scope_key.len())
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
    /// enters the writer channel), [`StoreError::CapacityExceeded`] when a
    /// durable bound would be crossed, or an error when the writer task or
    /// SQLite fails.
    pub async fn enqueue_outbox(&self, row: NewOutboxRow) -> Result<OutboxEnqueue, StoreError> {
        validate_outbox_payload(&row)?;
        let request_size = request_bytes(&[
            &row.idempotency_key,
            &row.scope_key,
            &row.kind,
            &row.payload_json,
        ]);
        self.run_sized(request_size, move |connection| {
            let transaction = connection
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(|error| sqlite_error("starting an outbox enqueue", &error))?;
            let result = enqueue_one(&transaction, &row);
            if result.is_ok() {
                transaction
                    .commit()
                    .map_err(|error| sqlite_error("committing an outbox enqueue", &error))?;
            }
            result
        })
        .await
    }

    /// Atomically enqueues a batch of outbound messages in one transaction.
    ///
    /// Every row runs the same idempotency and capacity logic as
    /// [`StoreHandle::enqueue_outbox`]; if any row fails, the whole batch is
    /// rolled back, so a partial final answer can never be persisted and later
    /// sent. The returned vector has exactly one [`OutboxEnqueue`] per input
    /// row, in input order.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::PayloadTooLarge`] when any serialized body
    /// exceeds [`STORE_OUTBOX_PAYLOAD_MAX_BYTES`] (rejected before the batch
    /// enters the writer channel), [`StoreError::CapacityExceeded`] when a
    /// durable bound would be crossed, or an error when the writer task or
    /// SQLite fails.
    pub async fn enqueue_outbox_batch(
        &self,
        rows: &[NewOutboxRow],
    ) -> Result<Vec<OutboxEnqueue>, StoreError> {
        if u64::try_from(rows.len()).unwrap_or(u64::MAX) > STORE_OUTBOX_MAX_ROWS {
            return Err(StoreError::CapacityExceeded {
                context: "enqueueing an outbox batch",
            });
        }
        for row in rows {
            validate_outbox_payload(row)?;
        }
        let request_size = rows.iter().fold(0_usize, |total, row| {
            total.saturating_add(request_bytes(&[
                &row.idempotency_key,
                &row.scope_key,
                &row.kind,
                &row.payload_json,
            ]))
        });
        let rows = rows.to_vec();
        self.run_sized(request_size, move |connection| {
            let transaction = connection
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(|error| sqlite_error("starting an outbox batch enqueue", &error))?;
            let mut results = Vec::with_capacity(rows.len());
            for row in &rows {
                match enqueue_one(&transaction, row) {
                    Ok(value) => results.push(value),
                    Err(error) => return Err(error),
                }
            }
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing an outbox batch enqueue", &error))?;
            Ok(results)
        })
        .await
    }

    /// Defers every `pending` successor row after `after_id` whose retry time
    /// is earlier than `retry_ms`, parking it until `retry_ms` in one bounded
    /// `UPDATE`.
    ///
    /// This closes the cross-batch reordering hole: a failed row that will be
    /// retried at `retry_ms` must not let already-`pending` successors (newly
    /// enqueued, or never claimed) overtake it in the next poll. Rows with an
    /// id `<= after_id`, and successors already parked no earlier than
    /// `retry_ms`, are left untouched.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or SQLite fails.
    pub async fn defer_outbox_after(
        &self,
        after_id: i64,
        retry_ms: i64,
    ) -> Result<u64, StoreError> {
        self.run(move |connection| {
            let changed = connection
                .execute(
                    "UPDATE outbox SET next_retry_ms = ?2, updated_ms = ?3
                     WHERE state = 'pending' AND id > ?1 AND next_retry_ms < ?2",
                    params![after_id, retry_ms, now_ms()],
                )
                .map_err(|error| sqlite_error("deferring outbox successors", &error))?;
            Ok(u64::try_from(changed).unwrap_or(u64::MAX))
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

    /// Atomically records a retryable failure and parks every later live row
    /// behind the failed row's retry window.
    ///
    /// Updating the failed row and deferring its successors in separate
    /// transactions would leave a crash window in which a later row could be
    /// claimed first after restart. Later rows already claimed by the single
    /// pump are returned to `pending`; rows not yet claimed move forward only
    /// when necessary.
    ///
    /// Returns `true` when the failed row remains retryable, or `false` when
    /// the attempt budget made it terminal.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`] for an unknown row,
    /// [`StoreError::InvalidTransition`] when the row is not `sending` or the
    /// attempt is non-monotonic, and an error when SQLite fails.
    pub async fn fail_outbox_and_defer_successors(
        &self,
        id: i64,
        attempts: u32,
        next_retry_ms: i64,
    ) -> Result<bool, StoreError> {
        self.run(move |connection| {
            let transaction = connection
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(|error| sqlite_error("starting an ordered outbox retry", &error))?;
            let stored_attempts =
                require_sending(&transaction, id, "retrying an ordered outbox row")?;
            if attempts != stored_attempts.saturating_add(1) {
                return Err(StoreError::InvalidTransition {
                    context: "recording a non-monotonic outbox attempt",
                });
            }
            let deferred = attempts < STORE_OUTBOX_MAX_ATTEMPTS;
            let state = if deferred {
                OutboxState::Pending
            } else {
                OutboxState::Failed
            };
            transaction
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
                .map_err(|error| sqlite_error("retrying an ordered outbox row", &error))?;
            if deferred {
                transaction
                    .execute(
                        "UPDATE outbox
                         SET state = 'pending',
                             next_retry_ms = CASE
                                 WHEN next_retry_ms < ?2 THEN ?2
                                 ELSE next_retry_ms
                             END,
                             updated_ms = ?3
                         WHERE id > ?1
                           AND state IN ('pending', 'sending')",
                        params![id, next_retry_ms, now_ms()],
                    )
                    .map_err(|error| sqlite_error("deferring ordered outbox successors", &error))?;
            }
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing an ordered outbox retry", &error))?;
            Ok(deferred)
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

    /// Atomically replaces a definitely-not-applied final-card update with its
    /// durable standalone fallback and returns the row to `pending`.
    ///
    /// The existing deterministic idempotency key is retained, so replaying
    /// turn finalization cannot enqueue a second fallback. Attempts reset
    /// because the standalone post is a different delivery operation. Any
    /// already-claimed tail is released in the same transaction to preserve
    /// global FIFO ordering.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::PayloadTooLarge`] before enqueue, or a state/store
    /// error when the target is absent or no longer `sending`.
    pub async fn replace_outbox_with_fallback(
        &self,
        id: i64,
        payload_json: String,
    ) -> Result<(), StoreError> {
        if payload_json.len() > STORE_OUTBOX_PAYLOAD_MAX_BYTES {
            return Err(StoreError::PayloadTooLarge {
                context: "persisting an outbox fallback",
                limit: STORE_OUTBOX_PAYLOAD_MAX_BYTES as u64,
            });
        }
        self.run(move |connection| {
            let transaction = connection
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(|error| sqlite_error("starting an outbox fallback", &error))?;
            require_sending(&transaction, id, "persisting an outbox fallback")?;
            let now = now_ms();
            let payload_bytes = i64::try_from(payload_json.len()).unwrap_or(i64::MAX);
            let old_payload_bytes: i64 = transaction
                .query_row(
                    "SELECT payload_bytes FROM outbox WHERE id = ?1",
                    [id],
                    |row| row.get(0),
                )
                .map_err(|error| sqlite_error("reading the replaced outbox payload", &error))?;
            let live_bytes: i64 = transaction
                .query_row(
                    "SELECT COALESCE(SUM(payload_bytes), 0) FROM outbox
                     WHERE state IN ('pending', 'sending')",
                    [],
                    |row| row.get(0),
                )
                .map_err(|error| sqlite_error("checking fallback outbox capacity", &error))?;
            let (_, total_bytes) = total_outbox_count_bytes(&transaction)?;
            let replacement_delta = payload_bytes.saturating_sub(old_payload_bytes).max(0);
            if u64::try_from(live_bytes.saturating_add(replacement_delta)).unwrap_or(u64::MAX)
                > STORE_OUTBOX_MAX_QUEUED_BYTES
                || total_bytes.saturating_add(u64::try_from(replacement_delta).unwrap_or(u64::MAX))
                    > OUTBOX_TERMINAL_MAX_BYTES
            {
                return Err(StoreError::CapacityExceeded {
                    context: "persisting an outbox fallback",
                });
            }
            transaction
                .execute(
                    "UPDATE outbox
                     SET payload_json = ?2, payload_bytes = ?3, state = 'pending',
                         attempts = 0, next_retry_ms = ?4, updated_ms = ?4
                     WHERE id = ?1",
                    params![id, payload_json, payload_bytes, now],
                )
                .map_err(|error| sqlite_error("persisting an outbox fallback", &error))?;
            transaction
                .execute(
                    "UPDATE outbox
                     SET state = 'pending',
                         next_retry_ms = CASE
                             WHEN next_retry_ms < ?2 THEN ?2
                             ELSE next_retry_ms
                         END,
                         updated_ms = ?2
                     WHERE id > ?1 AND state IN ('pending', 'sending')",
                    params![id, now],
                )
                .map_err(|error| sqlite_error("releasing the outbox fallback tail", &error))?;
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing an outbox fallback", &error))
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

    /// Deletes `sent`/`failed` outbox rows last updated before
    /// `older_than_ms`, in ascending `updated_ms` order, returning the number
    /// of pruned rows. `max_rows` is clamped to [`OUTBOX_SWEEP_BATCH`];
    /// `pending`, `sending`, and `uncertain_delivery` rows are never swept
    /// (an uncertain row stays parked and visible in `/status` until an
    /// operator resolves it manually).
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
        self.run(move |connection| sweep_terminal_rows_inline(connection, older_than_ms, max_rows))
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

    /// Reads one outbox row by its deterministic idempotency key.
    ///
    /// Progress-card update operations use this to resolve the receipt of the
    /// first progress row without persisting a server-issued message ID in a
    /// later payload.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or SQLite fails.
    pub async fn outbox_row_by_key(&self, key: &str) -> Result<Option<OutboxRow>, StoreError> {
        let key = key.to_owned();
        let request_size = request_bytes(&[&key]);
        self.run_sized(request_size, move |connection| {
            let row = connection.query_row(
                "SELECT id, idempotency_key, scope_key, kind, payload_json, payload_bytes,
                        state, attempts, next_retry_ms, receipt_message_id, created_ms, updated_ms
                 FROM outbox WHERE idempotency_key = ?1",
                params![key],
                read_outbox_row,
            );
            query_optional(row, "reading an outbox row by key")
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

/// Rejects an oversized serialized payload before it reaches the writer.
fn validate_outbox_payload(row: &NewOutboxRow) -> Result<(), StoreError> {
    if row.payload_json.len() > STORE_OUTBOX_PAYLOAD_MAX_BYTES {
        return Err(StoreError::PayloadTooLarge {
            context: "enqueueing an outbox payload",
            limit: STORE_OUTBOX_PAYLOAD_MAX_BYTES as u64,
        });
    }
    Ok(())
}

/// Inserts or deduplicates one outbox row on `connection`.
///
/// `connection` is the active `IMMEDIATE` transaction; the payload size must
/// already have been validated by the caller. The pending/sending capacity
/// bounds, the all-states hard cap (with its bounded inline sweep), and the
/// retry watermark are enforced here, so both the single-row and batch entry
/// points share one code path.
fn enqueue_one(
    connection: &rusqlite::Connection,
    row: &NewOutboxRow,
) -> Result<OutboxEnqueue, StoreError> {
    let payload_bytes = u64::try_from(row.payload_json.len()).unwrap_or(u64::MAX);
    let payload_bytes_sql = i64::try_from(row.payload_json.len()).unwrap_or(i64::MAX);
    let exists: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM outbox WHERE idempotency_key = ?1)",
            params![row.idempotency_key],
            |result| result.get(0),
        )
        .map_err(|error| sqlite_error("checking an outbox idempotency key", &error))?;
    let mut next_retry_ms = row.next_retry_ms;
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
        enforce_total_cap(connection, payload_bytes)?;
        // Sequence watermark: a newly enqueued row must never be claimable
        // before a row that is already parked for retry. The highest retry
        // time among live (`pending`/`sending`) rows is the watermark; a new
        // row inherits it so no later enqueue can overtake a failed row whose
        // retry was already scheduled (global FIFO across poll cycles). A `0`
        // ("now") next-retry time is preserved: only a positive watermark
        // raises it, and the watermark is monotonically non-increasing over
        // time as parked rows are claimed and resolved.
        let watermark: i64 = connection
            .query_row(
                "SELECT COALESCE(MAX(next_retry_ms), 0) FROM outbox
                 WHERE state IN ('pending', 'sending')",
                [],
                |result| result.get(0),
            )
            .map_err(|error| sqlite_error("reading the outbox retry watermark", &error))?;
        next_retry_ms = next_retry_ms.max(watermark);
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
                next_retry_ms,
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
    let stored =
        query_optional(existing, "re-reading an outbox row")?.ok_or(StoreError::NotFound {
            context: "re-reading an outbox row",
        })?;
    Ok(if inserted == 1 {
        OutboxEnqueue::New(stored)
    } else {
        OutboxEnqueue::Duplicate(stored)
    })
}

/// Enforces the all-states hard cap before one new row is persisted.
///
/// The cap counts every outbox row regardless of state, so no state transition
/// can ever push the table past the bound: a row only moves between states and
/// never changes the total. The incoming row is the proxy for the post-insert
/// total; if its addition would cross [`OUTBOX_TERMINAL_MAX_ROWS`] or
/// [`OUTBOX_TERMINAL_MAX_BYTES`], one bounded inline sweep first frees the
/// oldest overage `sent`/`failed` rows (same SQL as
/// [`StoreHandle::sweep_terminal_outbox`], clamped to [`OUTBOX_SWEEP_BATCH`]).
/// Only if the cap would still be crossed does the enqueue fail closed.
pub(crate) fn enforce_total_cap(
    connection: &rusqlite::Connection,
    payload_bytes: u64,
) -> Result<(), StoreError> {
    let (count, bytes) = total_outbox_count_bytes(connection)?;
    if count.saturating_add(1) <= OUTBOX_TERMINAL_MAX_ROWS
        && bytes.saturating_add(payload_bytes) <= OUTBOX_TERMINAL_MAX_BYTES
    {
        return Ok(());
    }
    sweep_terminal_rows_inline(
        connection,
        now_ms().saturating_sub(OUTBOX_TERMINAL_RETENTION_MS),
        OUTBOX_SWEEP_BATCH,
    )?;
    let (count, bytes) = total_outbox_count_bytes(connection)?;
    if count.saturating_add(1) > OUTBOX_TERMINAL_MAX_ROWS
        || bytes.saturating_add(payload_bytes) > OUTBOX_TERMINAL_MAX_BYTES
    {
        return Err(StoreError::CapacityExceeded {
            context: "enqueueing an outbox row",
        });
    }
    Ok(())
}

/// Total outbox row count and payload byte total across all states.
fn total_outbox_count_bytes(connection: &rusqlite::Connection) -> Result<(u64, u64), StoreError> {
    let (count, bytes): (i64, i64) = connection
        .query_row(
            "SELECT COUNT(*), COALESCE(SUM(payload_bytes), 0) FROM outbox",
            [],
            |result| Ok((result.get(0)?, result.get(1)?)),
        )
        .map_err(|error| sqlite_error("checking total outbox capacity", &error))?;
    Ok((
        u64::try_from(count).unwrap_or(u64::MAX),
        u64::try_from(bytes).unwrap_or(u64::MAX),
    ))
}

/// Deletes up to `max_rows` `sent`/`failed` rows last updated before
/// `older_than_ms`, in ascending `updated_ms` order. Shared by the periodic
/// pump sweep and the inline enqueue sweep.
///
/// `uncertain_delivery` rows are deliberately excluded: they are parked and
/// must stay visible in `/status` until an operator resolves them manually
/// (design §9, handoff §2 rule 6), so an automatic sweep must never delete
/// the evidence of an unknown delivery outcome.
fn sweep_terminal_rows_inline(
    connection: &rusqlite::Connection,
    older_than_ms: i64,
    max_rows: u32,
) -> Result<u64, StoreError> {
    let max_rows = max_rows.min(OUTBOX_SWEEP_BATCH);
    if max_rows == 0 {
        return Ok(0);
    }
    let deleted = connection
        .execute(
            "DELETE FROM outbox
             WHERE id IN (
                 SELECT candidate.id FROM outbox AS candidate
                 WHERE candidate.state IN ('sent', 'failed')
                   AND candidate.updated_ms < ?1
                   AND NOT (
                       candidate.kind = 'progress'
                       AND EXISTS (
                           SELECT 1 FROM outbox AS dependent
                           WHERE dependent.state IN ('pending', 'sending')
                             AND json_valid(dependent.payload_json)
                             AND json_extract(dependent.payload_json, '$.anchor_key')
                                 = candidate.idempotency_key
                       )
                   )
                 ORDER BY candidate.updated_ms, candidate.id LIMIT ?2
             )",
            params![older_than_ms, max_rows],
        )
        .map_err(|error| sqlite_error("sweeping terminal outbox rows", &error))?;
    Ok(u64::try_from(deleted).unwrap_or(u64::MAX))
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
    async fn sweep_deletes_only_overage_sent_and_failed_rows() {
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
        assert_eq!(
            deleted, 2,
            "only the over-age sent and failed rows are deleted"
        );

        let depth = store.outbox_depth().await.expect("depth");
        assert_eq!(depth.failed, 0);
        assert_eq!(depth.uncertain, 1, "uncertain rows are never auto-swept");
        assert_eq!(depth.pending, 1, "pending rows are never swept");
        assert_eq!(depth.sending, 1, "sending rows are never swept");
        assert_eq!(
            remaining_keys(&store).await,
            vec![
                "pending_old".to_owned(),
                "sending_old".to_owned(),
                "sent_recent".to_owned(),
                "uncertain_old".to_owned(),
            ],
            "only the over-age sent/failed rows are deleted"
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
    async fn sweep_retains_a_progress_anchor_while_a_live_update_depends_on_it() {
        let store = StoreHandle::open_in_memory().await.expect("store");
        store
            .enqueue_outbox(NewOutboxRow {
                idempotency_key: "turn:progress".to_owned(),
                scope_key: "im:oc".to_owned(),
                kind: "progress".to_owned(),
                payload_json: r#"{"version":1,"op":"reply_progress_card","message_id":"om_parent","thread_id":null,"text":"work"}"#.to_owned(),
                next_retry_ms: 0,
            })
            .await
            .expect("anchor");
        let anchor = store.claim_outbox_batch(now_ms(), 1).await.expect("claim");
        store
            .complete_outbox(anchor[0].id, "om_progress")
            .await
            .expect("complete anchor");
        store
            .enqueue_outbox(NewOutboxRow {
                idempotency_key: "turn:progress:1".to_owned(),
                scope_key: "im:oc".to_owned(),
                kind: "progress".to_owned(),
                payload_json: r#"{"version":1,"op":"update_progress_card","thread_id":null,"anchor_key":"turn:progress","text":"more"}"#.to_owned(),
                next_retry_ms: i64::MAX,
            })
            .await
            .expect("dependent");
        let anchor_id = anchor[0].id;
        store
            .run(move |connection| {
                connection
                    .execute(
                        "UPDATE outbox SET updated_ms = 1 WHERE id = ?1",
                        params![anchor_id],
                    )
                    .map_err(|error| sqlite_error("aging progress anchor", &error))?;
                Ok(())
            })
            .await
            .expect("age anchor");

        assert_eq!(
            store
                .sweep_terminal_outbox(2, OUTBOX_SWEEP_BATCH)
                .await
                .expect("sweep"),
            0,
            "a live dependent must keep the anchor receipt addressable"
        );
        assert!(
            store
                .outbox_row(anchor_id)
                .await
                .expect("read anchor")
                .is_some()
        );
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

    fn new_row(key: &str, next_retry_ms: i64) -> NewOutboxRow {
        NewOutboxRow {
            idempotency_key: key.to_owned(),
            scope_key: "im:oc".to_owned(),
            kind: "final".to_owned(),
            payload_json: "{}".to_owned(),
            next_retry_ms,
        }
    }

    async fn seed_terminal_rows(store: &StoreHandle, count: usize, updated_ms: i64) {
        store
            .run(move |connection| {
                let transaction = connection
                    .transaction()
                    .map_err(|error| sqlite_error("seeding terminal rows", &error))?;
                {
                    let mut statement = transaction
                        .prepare(
                            "INSERT INTO outbox
                             (idempotency_key, scope_key, kind, payload_json, payload_bytes,
                              state, attempts, next_retry_ms, receipt_message_id, created_ms, updated_ms)
                             VALUES (?1, 'im:oc', 'final', '{}', 2, 'sent', 1, 0, 'om_r', 1, ?2)",
                        )
                        .map_err(|error| sqlite_error("preparing terminal seed", &error))?;
                    for index in 0..count {
                        statement
                            .execute(params![format!("term:{index}"), updated_ms])
                            .map_err(|error| sqlite_error("inserting terminal seed", &error))?;
                    }
                }
                transaction
                    .commit()
                    .map_err(|error| sqlite_error("committing terminal seed", &error))?;
                Ok(())
            })
            .await
            .expect("seed terminal rows");
    }

    async fn seed_pending_rows(store: &StoreHandle, count: usize) {
        store
            .run(move |connection| {
                let transaction = connection
                    .transaction()
                    .map_err(|error| sqlite_error("seeding pending rows", &error))?;
                {
                    let mut statement = transaction
                        .prepare(
                            "INSERT INTO outbox
                             (idempotency_key, scope_key, kind, payload_json, payload_bytes,
                              state, attempts, next_retry_ms, receipt_message_id, created_ms, updated_ms)
                             VALUES (?1, 'im:oc', 'final', '{}', 2, 'pending', 0, 0, NULL, 1, 1)",
                        )
                        .map_err(|error| sqlite_error("preparing pending seed", &error))?;
                    for index in 0..count {
                        statement
                            .execute(params![format!("seed:{index}")])
                            .map_err(|error| sqlite_error("inserting pending seed", &error))?;
                    }
                }
                transaction
                    .commit()
                    .map_err(|error| sqlite_error("committing pending seed", &error))?;
                Ok(())
            })
            .await
            .expect("seed pending rows");
    }

    async fn total_rows(store: &StoreHandle) -> u64 {
        store
            .run(|connection| {
                let (count, _bytes) = total_outbox_count_bytes(connection)?;
                Ok(count)
            })
            .await
            .expect("total outbox row count")
    }

    #[tokio::test]
    async fn retry_failure_and_successor_deferral_are_one_atomic_transition() {
        let store = StoreHandle::open_in_memory().await.expect("store");
        store.enqueue_outbox(new_row("a", 0)).await.expect("a");
        store.enqueue_outbox(new_row("b", 0)).await.expect("b");
        store.enqueue_outbox(new_row("c", 0)).await.expect("c");
        let retry_ms = now_ms() + 60_000;
        store
            .enqueue_outbox(new_row("d", retry_ms + 5_000))
            .await
            .expect("d");

        // Claim a + b. The retry transaction must re-park a and the already
        // claimed b, while also deferring pending c. Row d is already later.
        let claimed = store.claim_outbox_batch(now_ms(), 2).await.expect("claim");
        assert_eq!(claimed.len(), 2);
        let after_id = claimed[0].id;
        let deferred = store
            .fail_outbox_and_defer_successors(after_id, 1, retry_ms)
            .await
            .expect("fail a");
        assert!(deferred);

        let tail = store
            .outbox_row(claimed[1].id)
            .await
            .expect("read b")
            .expect("b exists");
        assert_eq!(tail.state, OutboxState::Pending);
        assert_eq!(tail.next_retry_ms, retry_ms);

        assert!(
            store
                .claim_outbox_batch(now_ms(), 8)
                .await
                .expect("claim")
                .is_empty(),
            "nothing may be claimed before the deferred row's retry"
        );
        let next = store.claim_outbox_batch(retry_ms, 8).await.expect("claim");
        assert_eq!(
            next.iter()
                .map(|row| row.idempotency_key.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b", "c"],
            "global id order is restored once the deferred row is due"
        );
    }

    #[tokio::test]
    async fn enqueue_sweeps_old_rows_inline_when_at_total_hard_cap() {
        let store = StoreHandle::open_in_memory().await.expect("store");
        let cap = usize::try_from(OUTBOX_TERMINAL_MAX_ROWS).unwrap();
        seed_terminal_rows(&store, cap, 1_000).await;

        let result = store
            .enqueue_outbox(new_row("fresh", 0))
            .await
            .expect("enqueue");
        assert!(matches!(result, OutboxEnqueue::New(_)));

        assert_eq!(
            total_rows(&store).await,
            u64::try_from(cap).unwrap() - u64::from(OUTBOX_SWEEP_BATCH) + 1,
            "one bounded inline sweep frees the oldest sent/failed rows before the new row lands"
        );
        assert_eq!(store.outbox_depth().await.expect("depth").pending, 1);
    }

    #[tokio::test]
    async fn enqueue_fails_closed_when_rows_are_recent_and_at_total_hard_cap() {
        let store = StoreHandle::open_in_memory().await.expect("store");
        let cap = usize::try_from(OUTBOX_TERMINAL_MAX_ROWS).unwrap();
        seed_terminal_rows(&store, cap, now_ms()).await;

        let result = store.enqueue_outbox(new_row("blocked", 0)).await;
        assert!(
            matches!(result, Err(StoreError::CapacityExceeded { .. })),
            "{result:?}"
        );
        assert_eq!(
            total_rows(&store).await,
            u64::try_from(cap).unwrap(),
            "recent rows within retention are never swept"
        );
        assert_eq!(
            store.outbox_depth().await.expect("depth").pending,
            0,
            "the rejected row was never persisted"
        );
    }

    #[tokio::test]
    async fn enqueue_within_total_hard_cap_is_unaffected() {
        let store = StoreHandle::open_in_memory().await.expect("store");
        seed_terminal_rows(&store, 3, now_ms()).await;

        let result = store
            .enqueue_outbox(new_row("normal", 0))
            .await
            .expect("enqueue");
        assert!(matches!(result, OutboxEnqueue::New(_)));
        assert_eq!(
            total_rows(&store).await,
            4,
            "the new row adds exactly one to the total"
        );
        assert_eq!(store.outbox_depth().await.expect("depth").pending, 1);
    }

    #[tokio::test]
    async fn pending_to_sent_transition_stays_within_the_total_hard_cap() {
        let store = StoreHandle::open_in_memory().await.expect("store");
        let cap = usize::try_from(OUTBOX_TERMINAL_MAX_ROWS).unwrap();
        seed_terminal_rows(&store, cap - 1, now_ms()).await;

        let enqueued = store
            .enqueue_outbox(new_row("p", 0))
            .await
            .expect("enqueue");
        assert!(matches!(enqueued, OutboxEnqueue::New(_)));
        assert_eq!(
            total_rows(&store).await,
            u64::try_from(cap).unwrap(),
            "the table is exactly at the hard cap"
        );

        // pending -> sending -> sent: the transition preserves the total, so it
        // can never push the table past the hard cap (the cap counts all states).
        let claimed = store.claim_outbox_batch(now_ms(), 1).await.expect("claim");
        assert_eq!(claimed.len(), 1);
        store
            .complete_outbox(claimed[0].id, "om_r")
            .await
            .expect("complete");
        assert_eq!(
            total_rows(&store).await,
            u64::try_from(cap).unwrap(),
            "a state transition preserves the total count"
        );

        // A further enqueue is now over the cap and fails closed.
        let result = store.enqueue_outbox(new_row("over", 0)).await;
        assert!(matches!(result, Err(StoreError::CapacityExceeded { .. })));
    }

    #[tokio::test]
    async fn enqueue_applies_the_retry_watermark_to_new_rows() {
        let store = StoreHandle::open_in_memory().await.expect("store");
        let retry_ms = now_ms() + 60_000;
        let first = store
            .enqueue_outbox(new_row("first", 0))
            .await
            .expect("first");
        let first_id = match &first {
            OutboxEnqueue::New(row) | OutboxEnqueue::Duplicate(row) => row.id,
        };
        let claimed = store.claim_outbox_batch(now_ms(), 1).await.expect("claim");
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, first_id);
        store
            .fail_outbox(first_id, 1, retry_ms, false)
            .await
            .expect("fail first");

        // A row enqueued after the deferral snapshot inherits the watermark, so
        // it can never be claimed before the retrying row.
        let second = store
            .enqueue_outbox(new_row("second", 0))
            .await
            .expect("second");
        let second_row = match &second {
            OutboxEnqueue::New(row) | OutboxEnqueue::Duplicate(row) => row.clone(),
        };
        assert!(
            second_row.next_retry_ms >= retry_ms,
            "new rows must be parked no earlier than the retrying row ({} >= {})",
            second_row.next_retry_ms,
            retry_ms
        );

        // The failed row claims first once due, preserving global id order.
        let due = store.claim_outbox_batch(retry_ms, 8).await.expect("claim");
        assert_eq!(
            due.iter()
                .map(|row| row.idempotency_key.as_str())
                .collect::<Vec<_>>(),
            vec!["first", "second"],
            "the retrying row must be claimable before its later successor"
        );
    }

    #[tokio::test]
    async fn enqueue_without_retry_keeps_next_retry_zero() {
        let store = StoreHandle::open_in_memory().await.expect("store");
        store.enqueue_outbox(new_row("a", 0)).await.expect("a");
        let second = store.enqueue_outbox(new_row("b", 0)).await.expect("b");
        let row = match second {
            OutboxEnqueue::New(row) | OutboxEnqueue::Duplicate(row) => row,
        };
        assert_eq!(
            row.next_retry_ms, 0,
            "with no parked retry the watermark is 0"
        );
    }

    #[tokio::test]
    async fn retry_watermark_ignores_terminal_rows() {
        let store = StoreHandle::open_in_memory().await.expect("store");
        // A terminal row parked far in the future must not raise the watermark.
        store
            .run(|connection| {
                connection
                    .execute(
                        "INSERT INTO outbox
                         (idempotency_key, scope_key, kind, payload_json, payload_bytes,
                          state, attempts, next_retry_ms, receipt_message_id, created_ms, updated_ms)
                         VALUES ('done', 'im:oc', 'final', '{}', 2, 'sent', 1, ?1, 'om_r', 1, 1)",
                        params![now_ms() + 9_999_999],
                    )
                    .map_err(|error| sqlite_error("seeding a sent row", &error))?;
                Ok(())
            })
            .await
            .expect("seed sent row");
        let enqueued = store
            .enqueue_outbox(new_row("new", 0))
            .await
            .expect("enqueue");
        let row = match enqueued {
            OutboxEnqueue::New(row) | OutboxEnqueue::Duplicate(row) => row,
        };
        assert_eq!(
            row.next_retry_ms, 0,
            "terminal next_retry_ms must not raise the watermark"
        );
    }

    #[tokio::test]
    async fn batch_enqueue_is_atomic_on_capacity_failure() {
        let store = StoreHandle::open_in_memory().await.expect("store");
        // Fill to two rows below the pending/sending cap, so the third row of
        // the batch is the one that crosses it.
        let seed = usize::try_from(STORE_OUTBOX_MAX_ROWS).unwrap() - 2;
        seed_pending_rows(&store, seed).await;

        let rows = vec![
            new_row("batch:0", 0),
            new_row("batch:1", 0),
            new_row("batch:2", 0),
        ];
        let result = store.enqueue_outbox_batch(&rows).await;
        assert!(
            matches!(result, Err(StoreError::CapacityExceeded { .. })),
            "{result:?}"
        );
        for key in ["batch:0", "batch:1", "batch:2"] {
            let owned = key.to_owned();
            let exists: bool = store
                .run(move |connection| {
                    connection
                        .query_row(
                            "SELECT EXISTS(SELECT 1 FROM outbox WHERE idempotency_key = ?1)",
                            params![owned],
                            |row| row.get(0),
                        )
                        .map_err(|error| sqlite_error("checking batch row", &error))
                })
                .await
                .expect("batch row existence");
            assert!(!exists, "{key} must have been rolled back");
        }
        assert_eq!(
            store.outbox_depth().await.expect("depth").pending,
            u64::try_from(seed).unwrap(),
            "only the seeded rows survive"
        );
    }

    #[tokio::test]
    async fn batch_enqueue_is_idempotent_on_retry() {
        let store = StoreHandle::open_in_memory().await.expect("store");
        let rows = vec![new_row("k0", 0), new_row("k1", 0)];

        let first = store.enqueue_outbox_batch(&rows).await.expect("first");
        assert!(matches!(first[0], OutboxEnqueue::New(_)));
        assert!(matches!(first[1], OutboxEnqueue::New(_)));

        let second = store.enqueue_outbox_batch(&rows).await.expect("second");
        assert!(matches!(second[0], OutboxEnqueue::Duplicate(_)));
        assert!(matches!(second[1], OutboxEnqueue::Duplicate(_)));
        assert_eq!(store.outbox_depth().await.expect("depth").pending, 2);
    }

    #[tokio::test]
    async fn batch_enqueue_rejects_an_unbounded_row_count_before_cloning() {
        let store = StoreHandle::open_in_memory().await.expect("store");
        let count = usize::try_from(STORE_OUTBOX_MAX_ROWS).expect("row cap fits") + 1;
        let rows = (0..count)
            .map(|index| new_row(&format!("oversized:{index}"), 0))
            .collect::<Vec<_>>();

        let result = store.enqueue_outbox_batch(&rows).await;
        assert!(matches!(
            result,
            Err(StoreError::CapacityExceeded {
                context: "enqueueing an outbox batch"
            })
        ));
        assert_eq!(total_rows(&store).await, 0);
    }

    #[tokio::test]
    async fn batch_enqueue_matches_single_enqueue_semantics() {
        let store = StoreHandle::open_in_memory().await.expect("store");
        let a = new_row("a", 0);
        let b = new_row("b", 0);

        let single = store.enqueue_outbox(a.clone()).await.expect("single");
        let single_id = match &single {
            OutboxEnqueue::New(row) | OutboxEnqueue::Duplicate(row) => row.id,
        };

        let batch = store.enqueue_outbox_batch(&[a, b]).await.expect("batch");
        assert!(matches!(batch[0], OutboxEnqueue::Duplicate(_)));
        assert!(matches!(batch[1], OutboxEnqueue::New(_)));
        if let OutboxEnqueue::Duplicate(row) = &batch[0] {
            assert_eq!(row.id, single_id);
        } else {
            unreachable!("batch[0] must be a duplicate");
        }
        assert_eq!(store.outbox_depth().await.expect("depth").pending, 2);
    }

    #[test]
    fn debug_output_redacts_sensitive_outbox_fields() {
        let row = OutboxRow {
            id: 7,
            idempotency_key: "secret-key".to_owned(),
            scope_key: "im:secret-scope".to_owned(),
            kind: "final".to_owned(),
            payload_json: "{}".to_owned(),
            payload_bytes: 2,
            state: OutboxState::Pending,
            attempts: 0,
            next_retry_ms: 0,
            receipt_message_id: Some("om_secret_receipt".to_owned()),
            created_ms: 1,
            updated_ms: 1,
        };
        let rendered = format!("{row:?}");
        assert!(!rendered.contains("secret-key"));
        assert!(!rendered.contains("im:secret-scope"));
        assert!(!rendered.contains("om_secret_receipt"));
        assert!(rendered.contains("idempotency_key_len"));
        assert!(rendered.contains("scope_key_len"));
        assert!(rendered.contains("receipt_message_id_len"));

        let new_row = NewOutboxRow {
            idempotency_key: "secret-key".to_owned(),
            scope_key: "im:secret-scope".to_owned(),
            kind: "final".to_owned(),
            payload_json: "{}".to_owned(),
            next_retry_ms: 0,
        };
        let rendered = format!("{new_row:?}");
        assert!(!rendered.contains("secret-key"));
        assert!(!rendered.contains("im:secret-scope"));
        assert!(rendered.contains("idempotency_key_len"));
        assert!(rendered.contains("scope_key_len"));
    }
}
