//! Durable epoch fences and terminal projections for operator-owned external Codex endpoints.

use rusqlite::{OptionalExtension, params};

use super::{StoreError, StoreHandle, now_ms, request_bytes, sqlite_error};
use crate::limits::{
    EXTERNAL_MANAGED_THREAD_CAPACITY, EXTERNAL_RECONCILE_ENTRY_CAPACITY, ROUTING_ID_BYTE_LIMIT,
};

const ENDPOINT_LABEL_BYTE_LIMIT: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalEndpointState {
    Connecting,
    Reconciling,
    Ready,
    Unavailable,
    Stopped,
}

impl ExternalEndpointState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Connecting => "connecting",
            Self::Reconciling => "reconciling",
            Self::Ready => "ready",
            Self::Unavailable => "unavailable",
            Self::Stopped => "stopped",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "connecting" => Some(Self::Connecting),
            "reconciling" => Some(Self::Reconciling),
            "ready" => Some(Self::Ready),
            "unavailable" => Some(Self::Unavailable),
            "stopped" => Some(Self::Stopped),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalThreadState {
    Unavailable,
    Reconciling,
    Ready,
    Uncertain,
}

impl ExternalThreadState {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "unavailable" => Some(Self::Unavailable),
            "reconciling" => Some(Self::Reconciling),
            "ready" => Some(Self::Ready),
            "uncertain" => Some(Self::Uncertain),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalUncertaintyReason {
    BridgeRestart,
    SocketDisconnect,
    RequestTimeout,
    BufferOverflow,
    PageLimit,
    ServerRestart,
    ProtocolViolation,
    ConflictingTerminal,
}

impl ExternalUncertaintyReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::BridgeRestart => "bridge_restart",
            Self::SocketDisconnect => "socket_disconnect",
            Self::RequestTimeout => "request_timeout",
            Self::BufferOverflow => "buffer_overflow",
            Self::PageLimit => "page_limit",
            Self::ServerRestart => "server_restart",
            Self::ProtocolViolation => "protocol_violation",
            Self::ConflictingTerminal => "conflicting_terminal",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "bridge_restart" => Some(Self::BridgeRestart),
            "socket_disconnect" => Some(Self::SocketDisconnect),
            "request_timeout" => Some(Self::RequestTimeout),
            "buffer_overflow" => Some(Self::BufferOverflow),
            "page_limit" => Some(Self::PageLimit),
            "server_restart" => Some(Self::ServerRestart),
            "protocol_violation" => Some(Self::ProtocolViolation),
            "conflicting_terminal" => Some(Self::ConflictingTerminal),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalTerminalStatus {
    Completed,
    Failed,
    Interrupted,
}

impl ExternalTerminalStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Interrupted => "interrupted",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "interrupted" => Some(Self::Interrupted),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalEpochReservation {
    pub epoch: u64,
    pub state: ExternalEndpointState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalTurnTerminal {
    pub turn_id: String,
    pub status: ExternalTerminalStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalItemTerminal {
    pub turn_id: String,
    pub item_id: String,
}

#[derive(Clone, Eq, PartialEq)]
pub struct ExternalThreadSnapshot {
    pub endpoint_label: String,
    pub thread_id: String,
    pub epoch: u64,
    pub state: ExternalThreadState,
    pub reason: Option<ExternalUncertaintyReason>,
    pub terminal_turns: Vec<ExternalTurnTerminal>,
    pub terminal_items: Vec<ExternalItemTerminal>,
}

impl std::fmt::Debug for ExternalThreadSnapshot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExternalThreadSnapshot")
            .field("endpoint_label", &self.endpoint_label)
            .field("thread_id", &"[redacted]")
            .field("epoch", &self.epoch)
            .field("state", &self.state)
            .field("reason", &self.reason)
            .field("terminal_turn_count", &self.terminal_turns.len())
            .field("terminal_item_count", &self.terminal_items.len())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalFenceOutcome {
    Current,
    Stale,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalApplyOutcome {
    Applied {
        inserted_turns: usize,
        inserted_items: usize,
    },
    Stale,
    ConflictingTerminal,
}

impl StoreHandle {
    /// Atomically advances an endpoint epoch and fences its non-uncertain managed threads.
    ///
    /// # Errors
    ///
    /// Returns a validation, capacity, or SQLite error if the reservation cannot be persisted.
    pub async fn reserve_external_epoch(
        &self,
        endpoint_label: &str,
        restart_reason: ExternalUncertaintyReason,
    ) -> Result<ExternalEpochReservation, StoreError> {
        validate_id(
            endpoint_label,
            ENDPOINT_LABEL_BYTE_LIMIT,
            "reserving an external epoch",
        )?;
        let endpoint_label = endpoint_label.to_owned();
        self.run_sized(endpoint_label.len(), move |connection| {
            let transaction = connection
                .transaction()
                .map_err(|error| sqlite_error("starting an external epoch transaction", &error))?;
            let previous: Option<i64> = transaction
                .query_row(
                    "SELECT current_epoch FROM external_endpoint_epochs WHERE endpoint_label = ?1",
                    params![endpoint_label],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|error| sqlite_error("reading an external epoch", &error))?;
            let next = previous
                .unwrap_or(0)
                .checked_add(1)
                .ok_or(StoreError::CapacityExceeded {
                    context: "incrementing an external epoch",
                })?;
            transaction
                .execute(
                    "INSERT INTO external_endpoint_epochs(endpoint_label, current_epoch, state, updated_ms)
                     VALUES (?1, ?2, 'connecting', ?3)
                     ON CONFLICT(endpoint_label) DO UPDATE SET
                       current_epoch = excluded.current_epoch,
                       state = excluded.state,
                       updated_ms = excluded.updated_ms",
                    params![endpoint_label, next, now_ms()],
                )
                .map_err(|error| sqlite_error("reserving an external epoch", &error))?;
            transaction
                .execute(
                    "UPDATE external_managed_threads
                     SET epoch = ?2, state = 'unavailable', reason = ?3, updated_ms = ?4
                     WHERE endpoint_label = ?1 AND state != 'uncertain'",
                    params![endpoint_label, next, restart_reason.as_str(), now_ms()],
                )
                .map_err(|error| sqlite_error("fencing external managed threads", &error))?;
            transaction
                .execute(
                    "UPDATE external_mutation_intents
                     SET state = CASE state WHEN 'prepared' THEN 'rejected' ELSE 'uncertain' END,
                         updated_ms = ?2
                     WHERE endpoint_label = ?1 AND state IN ('prepared', 'sent')",
                    params![endpoint_label, now_ms()],
                )
                .map_err(|error| sqlite_error("fencing external mutation intents", &error))?;
            transaction
                .execute(
                    "UPDATE external_approval_claims SET state = 'uncertain', updated_ms = ?2
                     WHERE endpoint_label = ?1
                       AND state IN ('received', 'claimed', 'responding')",
                    params![endpoint_label, now_ms()],
                )
                .map_err(|error| sqlite_error("fencing external approval claims", &error))?;
            transaction
                .execute(
                    "UPDATE external_write_fences
                     SET epoch = ?2,
                         state = CASE WHEN
                           EXISTS(SELECT 1 FROM external_mutation_intents i
                                  WHERE i.endpoint_label = external_write_fences.endpoint_label
                                    AND i.thread_id = external_write_fences.thread_id
                                    AND i.state = 'uncertain')
                           OR EXISTS(SELECT 1 FROM external_approval_claims a
                                     WHERE a.endpoint_label = external_write_fences.endpoint_label
                                       AND a.thread_id = external_write_fences.thread_id
                                       AND a.state = 'uncertain')
                           THEN 'uncertain' ELSE 'open' END,
                         active_intent_id = NULL, updated_ms = ?3
                     WHERE endpoint_label = ?1",
                    params![endpoint_label, next, now_ms()],
                )
                .map_err(|error| sqlite_error("advancing external write fences", &error))?;
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing an external epoch", &error))?;
            Ok(ExternalEpochReservation {
                epoch: u64::try_from(next).map_err(|_| StoreError::CorruptData {
                    context: "decoding an external epoch",
                })?,
                state: ExternalEndpointState::Connecting,
            })
        })
        .await
    }

    /// Updates an endpoint state only when `epoch` is still current.
    ///
    /// # Errors
    ///
    /// Returns a validation or SQLite error while checking or applying the fence.
    pub async fn set_external_endpoint_state(
        &self,
        endpoint_label: &str,
        epoch: u64,
        state: ExternalEndpointState,
    ) -> Result<ExternalFenceOutcome, StoreError> {
        validate_id(
            endpoint_label,
            ENDPOINT_LABEL_BYTE_LIMIT,
            "setting external endpoint state",
        )?;
        let endpoint_label = endpoint_label.to_owned();
        self.run_sized(endpoint_label.len(), move |connection| {
            let changed = connection
                .execute(
                    "UPDATE external_endpoint_epochs SET state = ?3, updated_ms = ?4
                     WHERE endpoint_label = ?1 AND current_epoch = ?2",
                    params![endpoint_label, epoch_i64(epoch)?, state.as_str(), now_ms()],
                )
                .map_err(|error| sqlite_error("setting external endpoint state", &error))?;
            Ok(if changed == 1 {
                ExternalFenceOutcome::Current
            } else {
                ExternalFenceOutcome::Stale
            })
        })
        .await
    }

    /// Reads the current durable epoch and endpoint state.
    ///
    /// # Errors
    ///
    /// Returns a validation, corruption, capacity, or SQLite error.
    pub async fn external_endpoint_epoch(
        &self,
        endpoint_label: &str,
    ) -> Result<Option<ExternalEpochReservation>, StoreError> {
        validate_id(
            endpoint_label,
            ENDPOINT_LABEL_BYTE_LIMIT,
            "reading an external epoch",
        )?;
        let endpoint_label = endpoint_label.to_owned();
        self.run_sized(endpoint_label.len(), move |connection| {
            let raw: Option<(i64, String)> = connection
                .query_row(
                    "SELECT current_epoch, state FROM external_endpoint_epochs WHERE endpoint_label = ?1",
                    params![endpoint_label],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(|error| sqlite_error("reading an external epoch", &error))?;
            raw.map(|(epoch, state)| {
                Ok(ExternalEpochReservation {
                    epoch: u64::try_from(epoch).map_err(|_| StoreError::CorruptData {
                        context: "decoding an external epoch",
                    })?,
                    state: ExternalEndpointState::parse(&state).ok_or(StoreError::CorruptData {
                        context: "decoding external endpoint state",
                    })?,
                })
            })
            .transpose()
        })
        .await
    }

    /// Durably registers one thread under an already reserved external endpoint.
    ///
    /// # Errors
    ///
    /// Returns a validation, not-found, capacity, or SQLite error.
    pub async fn register_external_thread(
        &self,
        endpoint_label: &str,
        thread_id: &str,
    ) -> Result<(), StoreError> {
        validate_pair(endpoint_label, thread_id, "registering an external thread")?;
        let endpoint_label = endpoint_label.to_owned();
        let thread_id = thread_id.to_owned();
        let request_size = request_bytes(&[&endpoint_label, &thread_id]);
        self.run_sized(request_size, move |connection| {
            let transaction = connection
                .transaction()
                .map_err(|error| sqlite_error("starting external thread registration", &error))?;
            let epoch: i64 = transaction
                .query_row(
                    "SELECT current_epoch FROM external_endpoint_epochs WHERE endpoint_label = ?1",
                    params![endpoint_label],
                    |row| row.get(0),
                )
                .map_err(|error| match error {
                    rusqlite::Error::QueryReturnedNoRows => StoreError::NotFound {
                        context: "registering a thread for an unknown external endpoint",
                    },
                    other => sqlite_error("reading an external epoch for registration", &other),
                })?;
            let exists: bool = transaction
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM external_managed_threads
                                   WHERE endpoint_label = ?1 AND thread_id = ?2)",
                    params![endpoint_label, thread_id],
                    |row| row.get(0),
                )
                .map_err(|error| {
                    sqlite_error("checking an external thread registration", &error)
                })?;
            if !exists {
                let count: i64 = transaction
                    .query_row(
                        "SELECT COUNT(*) FROM external_managed_threads WHERE endpoint_label = ?1",
                        params![endpoint_label],
                        |row| row.get(0),
                    )
                    .map_err(|error| sqlite_error("counting external managed threads", &error))?;
                if usize::try_from(count).unwrap_or(usize::MAX) >= EXTERNAL_MANAGED_THREAD_CAPACITY
                {
                    return Err(StoreError::CapacityExceeded {
                        context: "registering an external managed thread",
                    });
                }
                transaction
                    .execute(
                        "INSERT INTO external_managed_threads
                         (endpoint_label, thread_id, epoch, state, reason, updated_ms)
                         VALUES (?1, ?2, ?3, 'unavailable', 'bridge_restart', ?4)",
                        params![endpoint_label, thread_id, epoch, now_ms()],
                    )
                    .map_err(|error| sqlite_error("registering an external thread", &error))?;
            }
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing external thread registration", &error))
        })
        .await
    }

    /// Marks a managed thread as reconciling only under the current endpoint epoch.
    ///
    /// # Errors
    ///
    /// Returns a validation or SQLite error while checking or applying the fence.
    pub async fn begin_external_reconciliation(
        &self,
        endpoint_label: &str,
        thread_id: &str,
        epoch: u64,
    ) -> Result<ExternalFenceOutcome, StoreError> {
        validate_pair(
            endpoint_label,
            thread_id,
            "beginning external reconciliation",
        )?;
        let endpoint_label = endpoint_label.to_owned();
        let thread_id = thread_id.to_owned();
        let request_size = request_bytes(&[&endpoint_label, &thread_id]);
        self.run_sized(request_size, move |connection| {
            let changed = connection
                .execute(
                    "UPDATE external_managed_threads
                     SET epoch = ?3, state = 'reconciling', reason = NULL, updated_ms = ?4
                     WHERE endpoint_label = ?1 AND thread_id = ?2 AND state != 'uncertain'
                       AND EXISTS(SELECT 1 FROM external_endpoint_epochs
                                  WHERE endpoint_label = ?1 AND current_epoch = ?3)",
                    params![endpoint_label, thread_id, epoch_i64(epoch)?, now_ms()],
                )
                .map_err(|error| sqlite_error("beginning external reconciliation", &error))?;
            Ok(if changed == 1 {
                ExternalFenceOutcome::Current
            } else {
                ExternalFenceOutcome::Stale
            })
        })
        .await
    }

    /// Marks the current endpoint epoch and all active threads unavailable.
    ///
    /// # Errors
    ///
    /// Returns a validation or SQLite error while persisting unavailability.
    pub async fn mark_external_unavailable(
        &self,
        endpoint_label: &str,
        epoch: u64,
        reason: ExternalUncertaintyReason,
    ) -> Result<ExternalFenceOutcome, StoreError> {
        validate_id(
            endpoint_label,
            ENDPOINT_LABEL_BYTE_LIMIT,
            "marking an external endpoint unavailable",
        )?;
        let endpoint_label = endpoint_label.to_owned();
        self.run_sized(endpoint_label.len(), move |connection| {
            let transaction = connection.transaction().map_err(|error| {
                sqlite_error("starting external unavailability transaction", &error)
            })?;
            let changed = transaction
                .execute(
                    "UPDATE external_endpoint_epochs SET state = 'unavailable', updated_ms = ?3
                     WHERE endpoint_label = ?1 AND current_epoch = ?2",
                    params![endpoint_label, epoch_i64(epoch)?, now_ms()],
                )
                .map_err(|error| {
                    sqlite_error("marking an external endpoint unavailable", &error)
                })?;
            if changed == 0 {
                return Ok(ExternalFenceOutcome::Stale);
            }
            transaction
                .execute(
                    "UPDATE external_managed_threads
                     SET state = 'unavailable', reason = ?3, updated_ms = ?4
                     WHERE endpoint_label = ?1 AND epoch = ?2 AND state != 'uncertain'",
                    params![endpoint_label, epoch_i64(epoch)?, reason.as_str(), now_ms()],
                )
                .map_err(|error| sqlite_error("marking external threads unavailable", &error))?;
            transaction
                .execute(
                    "UPDATE external_mutation_intents
                     SET state = CASE state WHEN 'prepared' THEN 'rejected' ELSE 'uncertain' END,
                         updated_ms = ?3
                     WHERE endpoint_label = ?1 AND epoch = ?2 AND state IN ('prepared', 'sent')",
                    params![endpoint_label, epoch_i64(epoch)?, now_ms()],
                )
                .map_err(|error| sqlite_error("fencing unavailable mutations", &error))?;
            transaction
                .execute(
                    "UPDATE external_approval_claims SET state = 'uncertain', updated_ms = ?3
                     WHERE endpoint_label = ?1 AND epoch = ?2
                       AND state IN ('received', 'claimed', 'responding')",
                    params![endpoint_label, epoch_i64(epoch)?, now_ms()],
                )
                .map_err(|error| sqlite_error("fencing unavailable approvals", &error))?;
            transaction
                .execute(
                    "UPDATE external_write_fences
                     SET state = CASE WHEN
                           EXISTS(SELECT 1 FROM external_mutation_intents i
                                  WHERE i.endpoint_label = external_write_fences.endpoint_label
                                    AND i.thread_id = external_write_fences.thread_id
                                    AND i.state = 'uncertain')
                           OR EXISTS(SELECT 1 FROM external_approval_claims a
                                     WHERE a.endpoint_label = external_write_fences.endpoint_label
                                       AND a.thread_id = external_write_fences.thread_id
                                       AND a.state = 'uncertain')
                           THEN 'uncertain' ELSE 'open' END,
                         active_intent_id = NULL, updated_ms = ?3
                     WHERE endpoint_label = ?1 AND epoch = ?2",
                    params![endpoint_label, epoch_i64(epoch)?, now_ms()],
                )
                .map_err(|error| sqlite_error("fencing unavailable external writes", &error))?;
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing external unavailability", &error))?;
            Ok(ExternalFenceOutcome::Current)
        })
        .await
    }

    /// Durably fences one current managed thread as uncertain.
    ///
    /// # Errors
    ///
    /// Returns a validation or SQLite error while checking or applying the fence.
    pub async fn mark_external_thread_uncertain(
        &self,
        endpoint_label: &str,
        thread_id: &str,
        epoch: u64,
        reason: ExternalUncertaintyReason,
    ) -> Result<ExternalFenceOutcome, StoreError> {
        validate_pair(
            endpoint_label,
            thread_id,
            "marking an external thread uncertain",
        )?;
        let endpoint_label = endpoint_label.to_owned();
        let thread_id = thread_id.to_owned();
        let request_size = request_bytes(&[&endpoint_label, &thread_id]);
        self.run_sized(request_size, move |connection| {
            let changed = connection
                .execute(
                    "UPDATE external_managed_threads
                     SET state = 'uncertain', reason = ?4, updated_ms = ?5
                     WHERE endpoint_label = ?1 AND thread_id = ?2 AND epoch = ?3
                       AND EXISTS(SELECT 1 FROM external_endpoint_epochs
                                  WHERE endpoint_label = ?1 AND current_epoch = ?3)",
                    params![
                        endpoint_label,
                        thread_id,
                        epoch_i64(epoch)?,
                        reason.as_str(),
                        now_ms()
                    ],
                )
                .map_err(|error| sqlite_error("marking an external thread uncertain", &error))?;
            Ok(if changed == 1 {
                ExternalFenceOutcome::Current
            } else {
                ExternalFenceOutcome::Stale
            })
        })
        .await
    }

    /// Atomically commits a reconciled terminal projection and marks the thread ready.
    ///
    /// # Errors
    ///
    /// Returns a validation, capacity, or SQLite error; a stale epoch is a typed outcome.
    pub async fn apply_external_reconciliation(
        &self,
        endpoint_label: &str,
        thread_id: &str,
        epoch: u64,
        turns: Vec<ExternalTurnTerminal>,
        items: Vec<ExternalItemTerminal>,
    ) -> Result<ExternalApplyOutcome, StoreError> {
        validate_terminals(endpoint_label, thread_id, &turns, &items)?;
        let endpoint_label = endpoint_label.to_owned();
        let thread_id = thread_id.to_owned();
        let request_size = terminal_request_bytes(&endpoint_label, &thread_id, &turns, &items);
        self.run_sized(request_size, move |connection| {
            apply_terminals(
                connection,
                &endpoint_label,
                &thread_id,
                epoch,
                &turns,
                &items,
                true,
            )
        })
        .await
    }

    /// Records one live terminal observation under the ready thread's current epoch.
    ///
    /// # Errors
    ///
    /// Returns a validation, capacity, or SQLite error; stale/conflict cases are typed outcomes.
    pub async fn record_external_terminal(
        &self,
        endpoint_label: &str,
        thread_id: &str,
        epoch: u64,
        turn: Option<ExternalTurnTerminal>,
        item: Option<ExternalItemTerminal>,
    ) -> Result<ExternalApplyOutcome, StoreError> {
        let turns = turn.into_iter().collect::<Vec<_>>();
        let items = item.into_iter().collect::<Vec<_>>();
        validate_terminals(endpoint_label, thread_id, &turns, &items)?;
        let endpoint_label = endpoint_label.to_owned();
        let thread_id = thread_id.to_owned();
        let request_size = terminal_request_bytes(&endpoint_label, &thread_id, &turns, &items);
        self.run_sized(request_size, move |connection| {
            apply_terminals(
                connection,
                &endpoint_label,
                &thread_id,
                epoch,
                &turns,
                &items,
                false,
            )
        })
        .await
    }

    /// Reads one managed thread's durable state and terminal projection.
    ///
    /// # Errors
    ///
    /// Returns a validation, corruption, capacity, or SQLite error.
    pub async fn external_thread_snapshot(
        &self,
        endpoint_label: &str,
        thread_id: &str,
    ) -> Result<Option<ExternalThreadSnapshot>, StoreError> {
        validate_pair(
            endpoint_label,
            thread_id,
            "reading an external thread snapshot",
        )?;
        let endpoint_label = endpoint_label.to_owned();
        let thread_id = thread_id.to_owned();
        let request_size = request_bytes(&[&endpoint_label, &thread_id]);
        self.run_sized(request_size, move |connection| {
            read_snapshot(connection, &endpoint_label, &thread_id)
        })
        .await
    }

    /// Lists the bounded durable managed-thread set for one external endpoint.
    ///
    /// # Errors
    ///
    /// Returns a validation, corruption, capacity, or SQLite error.
    pub async fn external_managed_threads(
        &self,
        endpoint_label: &str,
    ) -> Result<Vec<ExternalThreadSnapshot>, StoreError> {
        validate_id(
            endpoint_label,
            ENDPOINT_LABEL_BYTE_LIMIT,
            "listing external managed threads",
        )?;
        let endpoint_label = endpoint_label.to_owned();
        self.run_sized(endpoint_label.len(), move |connection| {
            let mut statement = connection
                .prepare(
                    "SELECT thread_id FROM external_managed_threads
                     WHERE endpoint_label = ?1 ORDER BY thread_id LIMIT ?2",
                )
                .map_err(|error| sqlite_error("listing external managed threads", &error))?;
            let ids = statement
                .query_map(
                    params![
                        endpoint_label,
                        i64::try_from(EXTERNAL_MANAGED_THREAD_CAPACITY + 1).unwrap_or(i64::MAX)
                    ],
                    |row| row.get::<_, String>(0),
                )
                .map_err(|error| sqlite_error("listing external managed threads", &error))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| sqlite_error("listing external managed threads", &error))?;
            drop(statement);
            if ids.len() > EXTERNAL_MANAGED_THREAD_CAPACITY {
                return Err(StoreError::CapacityExceeded {
                    context: "listing external managed threads",
                });
            }
            ids.into_iter()
                .map(|thread_id| {
                    read_snapshot(connection, &endpoint_label, &thread_id)?.ok_or(
                        StoreError::CorruptData {
                            context: "reading a listed external thread",
                        },
                    )
                })
                .collect()
        })
        .await
    }
}

#[allow(clippy::too_many_lines)]
fn apply_terminals(
    connection: &mut rusqlite::Connection,
    endpoint_label: &str,
    thread_id: &str,
    epoch: u64,
    turns: &[ExternalTurnTerminal],
    items: &[ExternalItemTerminal],
    finish_reconciliation: bool,
) -> Result<ExternalApplyOutcome, StoreError> {
    let epoch = epoch_i64(epoch)?;
    let transaction = connection
        .transaction()
        .map_err(|error| sqlite_error("starting an external terminal transaction", &error))?;
    let expected_state = if finish_reconciliation {
        "reconciling"
    } else {
        "ready"
    };
    let current: bool = transaction
        .query_row(
            "SELECT EXISTS(
               SELECT 1 FROM external_managed_threads t
               JOIN external_endpoint_epochs e USING(endpoint_label)
               WHERE t.endpoint_label = ?1 AND t.thread_id = ?2 AND t.epoch = ?3
                 AND t.state = ?4 AND e.current_epoch = ?3
             )",
            params![endpoint_label, thread_id, epoch, expected_state],
            |row| row.get(0),
        )
        .map_err(|error| sqlite_error("checking an external terminal fence", &error))?;
    if !current {
        return Ok(ExternalApplyOutcome::Stale);
    }
    for turn in turns {
        let prior: Option<String> = transaction
            .query_row(
                "SELECT status FROM external_turn_terminals
                 WHERE endpoint_label = ?1 AND thread_id = ?2 AND turn_id = ?3",
                params![endpoint_label, thread_id, turn.turn_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| sqlite_error("checking an external turn terminal", &error))?;
        if prior
            .as_deref()
            .is_some_and(|prior| prior != turn.status.as_str())
        {
            transaction
                .execute(
                    "UPDATE external_managed_threads
                     SET state = 'uncertain', reason = 'conflicting_terminal', updated_ms = ?4
                     WHERE endpoint_label = ?1 AND thread_id = ?2 AND epoch = ?3",
                    params![endpoint_label, thread_id, epoch, now_ms()],
                )
                .map_err(|error| sqlite_error("fencing a conflicting terminal", &error))?;
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing a terminal conflict", &error))?;
            return Ok(ExternalApplyOutcome::ConflictingTerminal);
        }
    }
    let mut inserted_turns = 0_usize;
    for turn in turns {
        inserted_turns = inserted_turns.saturating_add(
            transaction
                .execute(
                    "INSERT OR IGNORE INTO external_turn_terminals
                     (endpoint_label, thread_id, turn_id, status, observed_epoch)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        endpoint_label,
                        thread_id,
                        turn.turn_id,
                        turn.status.as_str(),
                        epoch
                    ],
                )
                .map_err(|error| sqlite_error("recording an external turn terminal", &error))?,
        );
    }
    let mut inserted_items = 0_usize;
    for item in items {
        inserted_items = inserted_items.saturating_add(
            transaction
                .execute(
                    "INSERT OR IGNORE INTO external_item_terminals
                     (endpoint_label, thread_id, turn_id, item_id, observed_epoch)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![endpoint_label, thread_id, item.turn_id, item.item_id, epoch],
                )
                .map_err(|error| sqlite_error("recording an external item terminal", &error))?,
        );
    }
    if finish_reconciliation {
        transaction
            .execute(
                "UPDATE external_managed_threads
                 SET state = 'ready', reason = NULL, updated_ms = ?4
                 WHERE endpoint_label = ?1 AND thread_id = ?2 AND epoch = ?3",
                params![endpoint_label, thread_id, epoch, now_ms()],
            )
            .map_err(|error| sqlite_error("finishing external reconciliation", &error))?;
    }
    transaction
        .commit()
        .map_err(|error| sqlite_error("committing external terminals", &error))?;
    Ok(ExternalApplyOutcome::Applied {
        inserted_turns,
        inserted_items,
    })
}

fn read_snapshot(
    connection: &rusqlite::Connection,
    endpoint_label: &str,
    thread_id: &str,
) -> Result<Option<ExternalThreadSnapshot>, StoreError> {
    let raw: Option<(i64, String, Option<String>)> = connection
        .query_row(
            "SELECT epoch, state, reason FROM external_managed_threads
             WHERE endpoint_label = ?1 AND thread_id = ?2",
            params![endpoint_label, thread_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(|error| sqlite_error("reading an external thread snapshot", &error))?;
    let Some((epoch, state, reason)) = raw else {
        return Ok(None);
    };
    let terminal_turns = {
        let mut statement = connection
            .prepare(
                "SELECT turn_id, status FROM external_turn_terminals
                 WHERE endpoint_label = ?1 AND thread_id = ?2 ORDER BY turn_id LIMIT ?3",
            )
            .map_err(|error| sqlite_error("reading external turn terminals", &error))?;
        statement
            .query_map(
                params![
                    endpoint_label,
                    thread_id,
                    i64::try_from(EXTERNAL_RECONCILE_ENTRY_CAPACITY + 1).unwrap_or(i64::MAX)
                ],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .map_err(|error| sqlite_error("reading external turn terminals", &error))?
            .map(|row| {
                let (turn_id, status) =
                    row.map_err(|error| sqlite_error("reading external turn terminals", &error))?;
                Ok(ExternalTurnTerminal {
                    turn_id,
                    status: ExternalTerminalStatus::parse(&status).ok_or(
                        StoreError::CorruptData {
                            context: "decoding an external turn terminal",
                        },
                    )?,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?
    };
    let terminal_items = {
        let mut statement = connection
            .prepare(
                "SELECT turn_id, item_id FROM external_item_terminals
                 WHERE endpoint_label = ?1 AND thread_id = ?2 ORDER BY turn_id, item_id LIMIT ?3",
            )
            .map_err(|error| sqlite_error("reading external item terminals", &error))?;
        statement
            .query_map(
                params![
                    endpoint_label,
                    thread_id,
                    i64::try_from(EXTERNAL_RECONCILE_ENTRY_CAPACITY + 1).unwrap_or(i64::MAX)
                ],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .map_err(|error| sqlite_error("reading external item terminals", &error))?
            .map(|row| {
                let (turn_id, item_id) =
                    row.map_err(|error| sqlite_error("reading external item terminals", &error))?;
                Ok(ExternalItemTerminal { turn_id, item_id })
            })
            .collect::<Result<Vec<_>, StoreError>>()?
    };
    if terminal_turns.len() > EXTERNAL_RECONCILE_ENTRY_CAPACITY
        || terminal_items.len() > EXTERNAL_RECONCILE_ENTRY_CAPACITY
    {
        return Err(StoreError::CapacityExceeded {
            context: "reading external terminal projections",
        });
    }
    Ok(Some(ExternalThreadSnapshot {
        endpoint_label: endpoint_label.to_owned(),
        thread_id: thread_id.to_owned(),
        epoch: u64::try_from(epoch).map_err(|_| StoreError::CorruptData {
            context: "decoding an external thread epoch",
        })?,
        state: ExternalThreadState::parse(&state).ok_or(StoreError::CorruptData {
            context: "decoding an external thread state",
        })?,
        reason: reason
            .map(|value| {
                ExternalUncertaintyReason::parse(&value).ok_or(StoreError::CorruptData {
                    context: "decoding an external uncertainty reason",
                })
            })
            .transpose()?,
        terminal_turns,
        terminal_items,
    }))
}

fn validate_terminals(
    endpoint_label: &str,
    thread_id: &str,
    turns: &[ExternalTurnTerminal],
    items: &[ExternalItemTerminal],
) -> Result<(), StoreError> {
    validate_pair(endpoint_label, thread_id, "recording external terminals")?;
    if turns.len() > EXTERNAL_RECONCILE_ENTRY_CAPACITY
        || items.len() > EXTERNAL_RECONCILE_ENTRY_CAPACITY
    {
        return Err(StoreError::CapacityExceeded {
            context: "recording external terminal projections",
        });
    }
    for turn in turns {
        validate_id(
            &turn.turn_id,
            ROUTING_ID_BYTE_LIMIT,
            "recording an external turn terminal",
        )?;
    }
    for item in items {
        validate_id(
            &item.turn_id,
            ROUTING_ID_BYTE_LIMIT,
            "recording an external item terminal",
        )?;
        validate_id(
            &item.item_id,
            ROUTING_ID_BYTE_LIMIT,
            "recording an external item terminal",
        )?;
    }
    Ok(())
}

pub(super) fn validate_pair(
    endpoint_label: &str,
    thread_id: &str,
    context: &'static str,
) -> Result<(), StoreError> {
    validate_id(endpoint_label, ENDPOINT_LABEL_BYTE_LIMIT, context)?;
    validate_id(thread_id, ROUTING_ID_BYTE_LIMIT, context)
}

pub(super) fn validate_id(
    value: &str,
    limit: usize,
    context: &'static str,
) -> Result<(), StoreError> {
    if value.is_empty() || value.len() > limit {
        return Err(StoreError::PayloadTooLarge {
            context,
            limit: u64::try_from(limit).unwrap_or(u64::MAX),
        });
    }
    Ok(())
}

fn terminal_request_bytes(
    endpoint_label: &str,
    thread_id: &str,
    turns: &[ExternalTurnTerminal],
    items: &[ExternalItemTerminal],
) -> usize {
    request_bytes(&[endpoint_label, thread_id])
        .saturating_add(turns.iter().map(|turn| turn.turn_id.len()).sum::<usize>())
        .saturating_add(
            items
                .iter()
                .map(|item| item.turn_id.len().saturating_add(item.item_id.len()))
                .sum::<usize>(),
        )
}

pub(super) fn epoch_i64(epoch: u64) -> Result<i64, StoreError> {
    if epoch == 0 {
        return Err(StoreError::InvalidTransition {
            context: "using a zero external epoch",
        });
    }
    i64::try_from(epoch).map_err(|_| StoreError::CapacityExceeded {
        context: "encoding an external epoch",
    })
}
