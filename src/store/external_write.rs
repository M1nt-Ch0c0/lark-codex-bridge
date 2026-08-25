//! Durable external mutation intents, per-thread write fences, and approval claims.

use rusqlite::{OptionalExtension, params};

use super::{StoreError, StoreHandle, now_ms, request_bytes, sqlite_error};
use crate::{
    limits::{EXTERNAL_MANAGED_THREAD_CAPACITY, ROUTING_ID_BYTE_LIMIT},
    store::external::{epoch_i64, validate_id, validate_pair},
};

const ENDPOINT_LABEL_BYTE_LIMIT: usize = 128;
const ACTOR_BYTE_LIMIT: usize = 256;
const INTENT_ID_BYTE_LIMIT: usize = 128;
const REQUEST_KEY_BYTE_LIMIT: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalMutationKind {
    TurnStart,
    TurnSteer,
    TurnInterrupt,
    QueueAdd,
    QueueStart,
}

impl ExternalMutationKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::TurnStart => "turn_start",
            Self::TurnSteer => "turn_steer",
            Self::TurnInterrupt => "turn_interrupt",
            Self::QueueAdd => "queue_add",
            Self::QueueStart => "queue_start",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "turn_start" => Some(Self::TurnStart),
            "turn_steer" => Some(Self::TurnSteer),
            "turn_interrupt" => Some(Self::TurnInterrupt),
            "queue_add" => Some(Self::QueueAdd),
            "queue_start" => Some(Self::QueueStart),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalMutationState {
    Prepared,
    Sent,
    Applied,
    Rejected,
    Uncertain,
}

impl ExternalMutationState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Prepared => "prepared",
            Self::Sent => "sent",
            Self::Applied => "applied",
            Self::Rejected => "rejected",
            Self::Uncertain => "uncertain",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "prepared" => Some(Self::Prepared),
            "sent" => Some(Self::Sent),
            "applied" => Some(Self::Applied),
            "rejected" => Some(Self::Rejected),
            "uncertain" => Some(Self::Uncertain),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalWriteFenceState {
    Open,
    Active,
    Uncertain,
}

impl ExternalWriteFenceState {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "open" => Some(Self::Open),
            "active" => Some(Self::Active),
            "uncertain" => Some(Self::Uncertain),
            _ => None,
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct NewExternalMutationIntent {
    pub endpoint_label: String,
    pub thread_id: String,
    pub intent_id: String,
    pub epoch: u64,
    pub kind: ExternalMutationKind,
    pub expected_turn_id: Option<String>,
    pub client_message_id: Option<String>,
    pub source_actor: String,
    pub client_actor: String,
    pub approval_actor: String,
}

impl std::fmt::Debug for NewExternalMutationIntent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NewExternalMutationIntent")
            .field("epoch", &self.epoch)
            .field("kind", &self.kind)
            .field("has_expected_turn", &self.expected_turn_id.is_some())
            .field("has_client_message", &self.client_message_id.is_some())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ExternalMutationIntent {
    pub epoch: u64,
    pub kind: ExternalMutationKind,
    pub state: ExternalMutationState,
    pub expected_turn_id: Option<String>,
    pub result_id: Option<String>,
    pub source_actor: String,
    pub client_actor: String,
    pub approval_actor: String,
}

#[derive(Clone, Eq, PartialEq)]
pub struct ExternalMutationOwner {
    pub source_actor: String,
    pub client_actor: String,
    pub approval_actor: String,
}

impl std::fmt::Debug for ExternalMutationOwner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExternalMutationOwner")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for ExternalMutationIntent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExternalMutationIntent")
            .field("epoch", &self.epoch)
            .field("kind", &self.kind)
            .field("state", &self.state)
            .field("has_expected_turn", &self.expected_turn_id.is_some())
            .field("has_result", &self.result_id.is_some())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalPrepareOutcome {
    Prepared,
    Duplicate(ExternalMutationState),
    Busy,
    Uncertain,
    NotReady,
    StaleEpoch,
    ApprovalHandlerMismatch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalMutationResolution<'a> {
    Applied { result_id: Option<&'a str> },
    Rejected,
    Uncertain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalTransitionOutcome {
    Applied,
    Stale,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalApprovalKind {
    Command,
    FileChange,
    Permissions,
}

impl ExternalApprovalKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Command => "command",
            Self::FileChange => "file_change",
            Self::Permissions => "permissions",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "command" => Some(Self::Command),
            "file_change" => Some(Self::FileChange),
            "permissions" => Some(Self::Permissions),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalApprovalState {
    Received,
    Claimed,
    Responding,
    Resolved,
    Denied,
    Uncertain,
}

impl ExternalApprovalState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::Claimed => "claimed",
            Self::Responding => "responding",
            Self::Resolved => "resolved",
            Self::Denied => "denied",
            Self::Uncertain => "uncertain",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "received" => Some(Self::Received),
            "claimed" => Some(Self::Claimed),
            "responding" => Some(Self::Responding),
            "resolved" => Some(Self::Resolved),
            "denied" => Some(Self::Denied),
            "uncertain" => Some(Self::Uncertain),
            _ => None,
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct NewExternalApprovalClaim {
    pub endpoint_label: String,
    pub thread_id: String,
    pub approval_id: String,
    pub request_key: String,
    pub epoch: u64,
    pub turn_id: String,
    pub item_id: String,
    pub kind: ExternalApprovalKind,
    pub client_actor: String,
    pub approval_actor: String,
    pub recipient_actor: String,
    pub deadline_ms: i64,
}

impl std::fmt::Debug for NewExternalApprovalClaim {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NewExternalApprovalClaim")
            .field("epoch", &self.epoch)
            .field("kind", &self.kind)
            .field("deadline_ms", &self.deadline_ms)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ExternalApprovalClaim {
    pub epoch: u64,
    pub turn_id: String,
    pub item_id: String,
    pub kind: ExternalApprovalKind,
    pub state: ExternalApprovalState,
    pub source_actor: String,
    pub client_actor: String,
    pub approval_actor: String,
    pub recipient_actor: String,
    pub deadline_ms: i64,
}

impl std::fmt::Debug for ExternalApprovalClaim {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExternalApprovalClaim")
            .field("epoch", &self.epoch)
            .field("kind", &self.kind)
            .field("state", &self.state)
            .field("deadline_ms", &self.deadline_ms)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExternalApprovalReceiveOutcome {
    Received,
    Duplicate {
        approval_id: String,
        state: ExternalApprovalState,
    },
    NotOwned,
    NotReady,
    StaleEpoch,
    ThreadFenced,
    ApprovalHandlerMismatch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalApprovalClaimOutcome {
    Claimed,
    Duplicate,
    Unauthorized,
    Stale,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalApprovalResolution {
    Responding,
    Resolved,
    Denied,
    Uncertain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalApprovalReassignmentOutcome {
    Reassigned,
    NotDrained,
    Stale,
}

impl StoreHandle {
    /// Atomically records an intent and acquires the current thread's write fence.
    ///
    /// # Errors
    ///
    /// Returns a validation, capacity, transition, or SQLite error without acquiring the fence.
    #[allow(clippy::too_many_lines)]
    pub async fn prepare_external_mutation(
        &self,
        intent: NewExternalMutationIntent,
    ) -> Result<ExternalPrepareOutcome, StoreError> {
        validate_intent(&intent)?;
        let request_size = request_bytes(&[
            &intent.endpoint_label,
            &intent.thread_id,
            &intent.intent_id,
            intent.expected_turn_id.as_deref().unwrap_or_default(),
            intent.client_message_id.as_deref().unwrap_or_default(),
            &intent.source_actor,
            &intent.client_actor,
            &intent.approval_actor,
        ]);
        self.run_sized(request_size, move |connection| {
            let transaction = connection.transaction().map_err(|error| {
                sqlite_error("starting an external mutation transaction", &error)
            })?;
            let duplicate: Option<String> = transaction
                .query_row(
                    "SELECT state FROM external_mutation_intents
                     WHERE endpoint_label = ?1 AND thread_id = ?2 AND intent_id = ?3",
                    params![intent.endpoint_label, intent.thread_id, intent.intent_id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|error| sqlite_error("checking an external mutation intent", &error))?;
            if let Some(state) = duplicate {
                return Ok(ExternalPrepareOutcome::Duplicate(
                    ExternalMutationState::parse(&state).ok_or(StoreError::CorruptData {
                        context: "decoding an external mutation state",
                    })?,
                ));
            }
            let readiness: Option<(i64, String, i64, String)> = transaction
                .query_row(
                    "SELECT e.current_epoch, e.state, t.epoch, t.state
                     FROM external_endpoint_epochs e
                     JOIN external_managed_threads t ON t.endpoint_label = e.endpoint_label
                     WHERE e.endpoint_label = ?1 AND t.thread_id = ?2",
                    params![intent.endpoint_label, intent.thread_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()
                .map_err(|error| sqlite_error("checking external mutation readiness", &error))?;
            let Some((endpoint_epoch, endpoint_state, thread_epoch, thread_state)) = readiness
            else {
                return Ok(ExternalPrepareOutcome::NotReady);
            };
            let epoch = epoch_i64(intent.epoch)?;
            if endpoint_epoch != epoch || thread_epoch != epoch {
                return Ok(ExternalPrepareOutcome::StaleEpoch);
            }
            if endpoint_state != "ready" || thread_state != "ready" {
                return Ok(ExternalPrepareOutcome::NotReady);
            }
            transaction
                .execute(
                    "INSERT OR IGNORE INTO external_write_fences
                     (endpoint_label, thread_id, epoch, state, active_intent_id,
                      approval_actor, updated_ms)
                     VALUES (?1, ?2, ?3, 'open', NULL, ?4, ?5)",
                    params![
                        intent.endpoint_label,
                        intent.thread_id,
                        epoch,
                        intent.approval_actor,
                        now_ms()
                    ],
                )
                .map_err(|error| sqlite_error("creating an external write fence", &error))?;
            let fence: (String, String) = transaction
                .query_row(
                    "SELECT state, approval_actor FROM external_write_fences
                     WHERE endpoint_label = ?1 AND thread_id = ?2",
                    params![intent.endpoint_label, intent.thread_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(|error| sqlite_error("reading an external write fence", &error))?;
            if fence.1 != intent.approval_actor {
                return Ok(ExternalPrepareOutcome::ApprovalHandlerMismatch);
            }
            match ExternalWriteFenceState::parse(&fence.0).ok_or(StoreError::CorruptData {
                context: "decoding an external write fence",
            })? {
                ExternalWriteFenceState::Open => {}
                ExternalWriteFenceState::Active => return Ok(ExternalPrepareOutcome::Busy),
                ExternalWriteFenceState::Uncertain => {
                    return Ok(ExternalPrepareOutcome::Uncertain);
                }
            }
            let now = now_ms();
            transaction
                .execute(
                    "INSERT INTO external_mutation_intents
                     (endpoint_label, thread_id, intent_id, epoch, kind, expected_turn_id,
                      client_message_id, source_actor, client_actor, approval_actor, state,
                      result_id, created_ms, updated_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'prepared', NULL, ?11, ?11)",
                    params![
                        intent.endpoint_label,
                        intent.thread_id,
                        intent.intent_id,
                        epoch,
                        intent.kind.as_str(),
                        intent.expected_turn_id,
                        intent.client_message_id,
                        intent.source_actor,
                        intent.client_actor,
                        intent.approval_actor,
                        now,
                    ],
                )
                .map_err(|error| sqlite_error("recording an external mutation intent", &error))?;
            let changed = transaction
                .execute(
                    "UPDATE external_write_fences
                     SET epoch = ?3, state = 'active', active_intent_id = ?4, updated_ms = ?5
                     WHERE endpoint_label = ?1 AND thread_id = ?2 AND state = 'open'",
                    params![
                        intent.endpoint_label,
                        intent.thread_id,
                        epoch,
                        intent.intent_id,
                        now
                    ],
                )
                .map_err(|error| sqlite_error("acquiring an external write fence", &error))?;
            if changed != 1 {
                return Err(StoreError::InvalidTransition {
                    context: "acquiring an external write fence",
                });
            }
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing an external mutation intent", &error))?;
            Ok(ExternalPrepareOutcome::Prepared)
        })
        .await
    }

    /// Marks the exact prepared intent as possibly written to the socket.
    ///
    /// # Errors
    ///
    /// Returns a validation, transition, or SQLite error.
    pub async fn mark_external_mutation_sent(
        &self,
        endpoint_label: &str,
        thread_id: &str,
        intent_id: &str,
        epoch: u64,
    ) -> Result<ExternalTransitionOutcome, StoreError> {
        transition_sent(self, endpoint_label, thread_id, intent_id, epoch).await
    }

    /// Resolves an exact intent and either opens or permanently fences its thread.
    ///
    /// # Errors
    ///
    /// Returns a validation, transition, or SQLite error.
    #[allow(clippy::too_many_lines)]
    pub async fn resolve_external_mutation(
        &self,
        endpoint_label: &str,
        thread_id: &str,
        intent_id: &str,
        epoch: u64,
        resolution: ExternalMutationResolution<'_>,
    ) -> Result<ExternalTransitionOutcome, StoreError> {
        validate_pair(endpoint_label, thread_id, "resolving an external mutation")?;
        validate_id(
            intent_id,
            INTENT_ID_BYTE_LIMIT,
            "resolving an external mutation",
        )?;
        let result_id = match resolution {
            ExternalMutationResolution::Applied { result_id } => result_id,
            ExternalMutationResolution::Rejected | ExternalMutationResolution::Uncertain => None,
        };
        if let Some(result_id) = result_id {
            validate_id(
                result_id,
                ROUTING_ID_BYTE_LIMIT,
                "resolving an external mutation",
            )?;
        }
        let endpoint_label = endpoint_label.to_owned();
        let thread_id = thread_id.to_owned();
        let intent_id = intent_id.to_owned();
        let result_id = result_id.map(str::to_owned);
        let (target_state, allowed_states, fence_state) = match resolution {
            ExternalMutationResolution::Applied { .. } => {
                (ExternalMutationState::Applied, "sent", "open")
            }
            ExternalMutationResolution::Rejected => {
                (ExternalMutationState::Rejected, "prepared,sent", "open")
            }
            ExternalMutationResolution::Uncertain => {
                (ExternalMutationState::Uncertain, "sent", "uncertain")
            }
        };
        let request_size = request_bytes(&[
            &endpoint_label,
            &thread_id,
            &intent_id,
            result_id.as_deref().unwrap_or_default(),
        ]);
        self.run_sized(request_size, move |connection| {
            let transaction = connection.transaction().map_err(|error| {
                sqlite_error("starting external mutation resolution", &error)
            })?;
            let current: Option<String> = transaction
                .query_row(
                    "SELECT state FROM external_mutation_intents
                     WHERE endpoint_label = ?1 AND thread_id = ?2 AND intent_id = ?3
                       AND epoch = ?4",
                    params![endpoint_label, thread_id, intent_id, epoch_i64(epoch)?],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|error| sqlite_error("reading an external mutation state", &error))?;
            let Some(current) = current else {
                return Ok(ExternalTransitionOutcome::Stale);
            };
            if !allowed_states.split(',').any(|allowed| allowed == current) {
                if current == target_state.as_str() {
                    return Ok(ExternalTransitionOutcome::Applied);
                }
                return Err(StoreError::InvalidTransition {
                    context: "resolving an external mutation",
                });
            }
            transaction
                .execute(
                    "UPDATE external_mutation_intents SET state = ?5, result_id = ?6, updated_ms = ?7
                     WHERE endpoint_label = ?1 AND thread_id = ?2 AND intent_id = ?3 AND epoch = ?4",
                    params![
                        endpoint_label,
                        thread_id,
                        intent_id,
                        epoch_i64(epoch)?,
                        target_state.as_str(),
                        result_id,
                        now_ms()
                    ],
                )
                .map_err(|error| sqlite_error("resolving an external mutation", &error))?;
            let changed = transaction
                .execute(
                    "UPDATE external_write_fences
                     SET state = ?5, active_intent_id = NULL, updated_ms = ?6
                     WHERE endpoint_label = ?1 AND thread_id = ?2 AND epoch = ?3
                       AND active_intent_id = ?4 AND state = 'active'",
                    params![
                        endpoint_label,
                        thread_id,
                        epoch_i64(epoch)?,
                        intent_id,
                        fence_state,
                        now_ms()
                    ],
                )
                .map_err(|error| sqlite_error("releasing an external write fence", &error))?;
            if changed != 1 {
                return Err(StoreError::InvalidTransition {
                    context: "releasing an external write fence",
                });
            }
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing external mutation resolution", &error))?;
            Ok(ExternalTransitionOutcome::Applied)
        })
        .await
    }

    /// Reads one intent without exposing its identifiers through `Debug`.
    ///
    /// # Errors
    ///
    /// Returns a validation or SQLite error, including corrupt durable state.
    pub async fn external_mutation_intent(
        &self,
        endpoint_label: &str,
        thread_id: &str,
        intent_id: &str,
    ) -> Result<Option<ExternalMutationIntent>, StoreError> {
        validate_pair(endpoint_label, thread_id, "reading an external mutation")?;
        validate_id(
            intent_id,
            INTENT_ID_BYTE_LIMIT,
            "reading an external mutation",
        )?;
        let endpoint_label = endpoint_label.to_owned();
        let thread_id = thread_id.to_owned();
        let intent_id = intent_id.to_owned();
        let request_size = request_bytes(&[&endpoint_label, &thread_id, &intent_id]);
        self.run_sized(request_size, move |connection| {
            let raw = connection
                .query_row(
                    "SELECT epoch, kind, state, expected_turn_id, result_id,
                            source_actor, client_actor, approval_actor
                     FROM external_mutation_intents
                     WHERE endpoint_label = ?1 AND thread_id = ?2 AND intent_id = ?3",
                    params![endpoint_label, thread_id, intent_id],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, Option<String>>(3)?,
                            row.get::<_, Option<String>>(4)?,
                            row.get::<_, String>(5)?,
                            row.get::<_, String>(6)?,
                            row.get::<_, String>(7)?,
                        ))
                    },
                )
                .optional()
                .map_err(|error| sqlite_error("reading an external mutation", &error))?;
            raw.map(decode_intent).transpose()
        })
        .await
    }

    /// Reads the latest applied bridge owner for an unterminated turn or queued submission.
    ///
    /// # Errors
    ///
    /// Returns a validation or SQLite error.
    pub async fn external_mutation_owner(
        &self,
        endpoint_label: &str,
        thread_id: &str,
        result_id: &str,
    ) -> Result<Option<ExternalMutationOwner>, StoreError> {
        validate_pair(
            endpoint_label,
            thread_id,
            "reading an external mutation owner",
        )?;
        validate_id(
            result_id,
            ROUTING_ID_BYTE_LIMIT,
            "reading an external mutation owner",
        )?;
        let endpoint_label = endpoint_label.to_owned();
        let thread_id = thread_id.to_owned();
        let result_id = result_id.to_owned();
        let request_size = request_bytes(&[&endpoint_label, &thread_id, &result_id]);
        self.run_sized(request_size, move |connection| {
            connection
                .query_row(
                    "SELECT source_actor, client_actor, approval_actor
                     FROM external_mutation_intents i
                     WHERE endpoint_label = ?1 AND thread_id = ?2 AND result_id = ?3
                       AND state = 'applied'
                       AND (kind IN ('queue_add') OR NOT EXISTS(
                           SELECT 1 FROM external_turn_terminals x
                           WHERE x.endpoint_label = i.endpoint_label
                             AND x.thread_id = i.thread_id AND x.turn_id = i.result_id
                       ))
                     ORDER BY updated_ms DESC LIMIT 1",
                    params![endpoint_label, thread_id, result_id],
                    |row| {
                        Ok(ExternalMutationOwner {
                            source_actor: row.get(0)?,
                            client_actor: row.get(1)?,
                            approval_actor: row.get(2)?,
                        })
                    },
                )
                .optional()
                .map_err(|error| sqlite_error("reading an external mutation owner", &error))
        })
        .await
    }

    /// Records one exact approval request only when its turn is owned by an applied bridge intent.
    ///
    /// # Errors
    ///
    /// Returns a validation, capacity, transition, or SQLite error without recording a claim.
    #[allow(clippy::too_many_lines)]
    pub async fn receive_external_approval(
        &self,
        claim: NewExternalApprovalClaim,
    ) -> Result<ExternalApprovalReceiveOutcome, StoreError> {
        validate_approval_claim(&claim)?;
        let request_size = request_bytes(&[
            &claim.endpoint_label,
            &claim.thread_id,
            &claim.approval_id,
            &claim.request_key,
            &claim.turn_id,
            &claim.item_id,
            &claim.client_actor,
            &claim.approval_actor,
            &claim.recipient_actor,
        ]);
        self.run_sized(request_size, move |connection| {
            let transaction = connection.transaction().map_err(|error| {
                sqlite_error("starting an external approval transaction", &error)
            })?;
            let duplicate: Option<(String, String)> = transaction
                .query_row(
                    "SELECT approval_id, state FROM external_approval_claims
                     WHERE endpoint_label = ?1 AND epoch = ?2 AND request_key = ?3",
                    params![
                        claim.endpoint_label,
                        epoch_i64(claim.epoch)?,
                        claim.request_key
                    ],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(|error| sqlite_error("checking an external approval request", &error))?;
            if let Some((approval_id, state)) = duplicate {
                return Ok(ExternalApprovalReceiveOutcome::Duplicate {
                    approval_id,
                    state: ExternalApprovalState::parse(&state).ok_or(StoreError::CorruptData {
                        context: "decoding an external approval state",
                    })?,
                });
            }
            let readiness: Option<(i64, String, i64, String, Option<String>)> = transaction
                .query_row(
                    "SELECT e.current_epoch, e.state, t.epoch, t.state, f.state
                     FROM external_endpoint_epochs e
                     JOIN external_managed_threads t ON t.endpoint_label = e.endpoint_label
                     LEFT JOIN external_write_fences f
                       ON f.endpoint_label = t.endpoint_label AND f.thread_id = t.thread_id
                     WHERE e.endpoint_label = ?1 AND t.thread_id = ?2",
                    params![claim.endpoint_label, claim.thread_id],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    },
                )
                .optional()
                .map_err(|error| sqlite_error("checking external approval readiness", &error))?;
            let Some((endpoint_epoch, endpoint_state, thread_epoch, thread_state, fence_state)) =
                readiness
            else {
                return Ok(ExternalApprovalReceiveOutcome::NotReady);
            };
            let epoch = epoch_i64(claim.epoch)?;
            if endpoint_epoch != epoch || thread_epoch != epoch {
                return Ok(ExternalApprovalReceiveOutcome::StaleEpoch);
            }
            if endpoint_state != "ready" || thread_state != "ready" {
                return Ok(ExternalApprovalReceiveOutcome::NotReady);
            }
            if fence_state.as_deref() != Some("open") {
                return Ok(ExternalApprovalReceiveOutcome::ThreadFenced);
            }
            let owner: Option<(String, String, String)> = transaction
                .query_row(
                    "SELECT source_actor, client_actor, approval_actor
                     FROM external_mutation_intents
                     WHERE endpoint_label = ?1 AND thread_id = ?2 AND result_id = ?3
                       AND state = 'applied'
                       AND kind IN ('turn_start', 'turn_steer', 'queue_start')
                       AND NOT EXISTS(
                           SELECT 1 FROM external_turn_terminals x
                           WHERE x.endpoint_label = ?1 AND x.thread_id = ?2 AND x.turn_id = ?3
                       )
                     ORDER BY updated_ms DESC LIMIT 1",
                    params![claim.endpoint_label, claim.thread_id, claim.turn_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()
                .map_err(|error| sqlite_error("reading an external turn owner", &error))?;
            let Some((source_actor, client_actor, approval_actor)) = owner else {
                return Ok(ExternalApprovalReceiveOutcome::NotOwned);
            };
            if client_actor != claim.client_actor {
                return Ok(ExternalApprovalReceiveOutcome::NotOwned);
            }
            if approval_actor != claim.approval_actor {
                return Ok(ExternalApprovalReceiveOutcome::ApprovalHandlerMismatch);
            }
            let now = now_ms();
            transaction
                .execute(
                    "INSERT INTO external_approval_claims
                     (endpoint_label, thread_id, approval_id, request_key, epoch, turn_id, item_id,
                      kind, source_actor, client_actor, approval_actor, recipient_actor, state,
                      deadline_ms, created_ms, updated_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                             'received', ?13, ?14, ?14)",
                    params![
                        claim.endpoint_label,
                        claim.thread_id,
                        claim.approval_id,
                        claim.request_key,
                        epoch,
                        claim.turn_id,
                        claim.item_id,
                        claim.kind.as_str(),
                        source_actor,
                        claim.client_actor,
                        claim.approval_actor,
                        claim.recipient_actor,
                        claim.deadline_ms,
                        now,
                    ],
                )
                .map_err(|error| sqlite_error("recording an external approval", &error))?;
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing an external approval", &error))?;
            Ok(ExternalApprovalReceiveOutcome::Received)
        })
        .await
    }

    /// Atomically claims one approval for its sole configured recipient.
    ///
    /// # Errors
    ///
    /// Returns a validation or SQLite error.
    pub async fn claim_external_approval(
        &self,
        endpoint_label: &str,
        thread_id: &str,
        approval_id: &str,
        recipient_actor: &str,
        epoch: u64,
    ) -> Result<ExternalApprovalClaimOutcome, StoreError> {
        validate_approval_key(
            endpoint_label,
            thread_id,
            approval_id,
            recipient_actor,
            "claiming an external approval",
        )?;
        let endpoint_label = endpoint_label.to_owned();
        let thread_id = thread_id.to_owned();
        let approval_id = approval_id.to_owned();
        let recipient_actor = recipient_actor.to_owned();
        let request_size =
            request_bytes(&[&endpoint_label, &thread_id, &approval_id, &recipient_actor]);
        self.run_sized(request_size, move |connection| {
            let changed = connection
                .execute(
                    "UPDATE external_approval_claims SET state = 'claimed', updated_ms = ?6
                     WHERE endpoint_label = ?1 AND thread_id = ?2 AND approval_id = ?3
                       AND recipient_actor = ?4 AND epoch = ?5 AND state = 'received'
                       AND deadline_ms > ?6",
                    params![
                        endpoint_label,
                        thread_id,
                        approval_id,
                        recipient_actor,
                        epoch_i64(epoch)?,
                        now_ms()
                    ],
                )
                .map_err(|error| sqlite_error("claiming an external approval", &error))?;
            if changed == 1 {
                return Ok(ExternalApprovalClaimOutcome::Claimed);
            }
            let current: Option<(String, String, i64)> = connection
                .query_row(
                    "SELECT state, recipient_actor, epoch FROM external_approval_claims
                     WHERE endpoint_label = ?1 AND thread_id = ?2 AND approval_id = ?3",
                    params![endpoint_label, thread_id, approval_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()
                .map_err(|error| sqlite_error("checking an external approval claim", &error))?;
            Ok(match current {
                Some((state, actor, found_epoch))
                    if found_epoch == epoch_i64(epoch)? && actor == recipient_actor =>
                {
                    if state == "claimed" {
                        ExternalApprovalClaimOutcome::Duplicate
                    } else {
                        ExternalApprovalClaimOutcome::Stale
                    }
                }
                Some(_) => ExternalApprovalClaimOutcome::Unauthorized,
                None => ExternalApprovalClaimOutcome::Stale,
            })
        })
        .await
    }

    /// Advances one claimed approval through response, resolution, denial, or uncertainty.
    ///
    /// # Errors
    ///
    /// Returns a validation, transition, or SQLite error.
    pub async fn resolve_external_approval(
        &self,
        endpoint_label: &str,
        thread_id: &str,
        approval_id: &str,
        epoch: u64,
        resolution: ExternalApprovalResolution,
    ) -> Result<ExternalTransitionOutcome, StoreError> {
        validate_pair(endpoint_label, thread_id, "resolving an external approval")?;
        validate_id(
            approval_id,
            INTENT_ID_BYTE_LIMIT,
            "resolving an external approval",
        )?;
        let endpoint_label = endpoint_label.to_owned();
        let thread_id = thread_id.to_owned();
        let approval_id = approval_id.to_owned();
        let (state, allowed) = match resolution {
            ExternalApprovalResolution::Responding => {
                (ExternalApprovalState::Responding, "claimed")
            }
            ExternalApprovalResolution::Resolved => (
                ExternalApprovalState::Resolved,
                "received,claimed,responding,denied",
            ),
            ExternalApprovalResolution::Denied => {
                (ExternalApprovalState::Denied, "received,claimed,responding")
            }
            ExternalApprovalResolution::Uncertain => (
                ExternalApprovalState::Uncertain,
                "received,claimed,responding",
            ),
        };
        let request_size = request_bytes(&[&endpoint_label, &thread_id, &approval_id]);
        self.run_sized(request_size, move |connection| {
            let transaction = connection
                .transaction()
                .map_err(|error| sqlite_error("starting external approval resolution", &error))?;
            let current: Option<String> = transaction
                .query_row(
                    "SELECT state FROM external_approval_claims
                     WHERE endpoint_label = ?1 AND thread_id = ?2 AND approval_id = ?3
                       AND epoch = ?4",
                    params![endpoint_label, thread_id, approval_id, epoch_i64(epoch)?],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|error| sqlite_error("reading an external approval state", &error))?;
            let Some(current) = current else {
                return Ok(ExternalTransitionOutcome::Stale);
            };
            if current == state.as_str() {
                return Ok(ExternalTransitionOutcome::Applied);
            }
            if !allowed.split(',').any(|value| value == current) {
                return Err(StoreError::InvalidTransition {
                    context: "resolving an external approval",
                });
            }
            transaction
                .execute(
                    "UPDATE external_approval_claims SET state = ?5, updated_ms = ?6
                     WHERE endpoint_label = ?1 AND thread_id = ?2 AND approval_id = ?3
                       AND epoch = ?4",
                    params![
                        endpoint_label,
                        thread_id,
                        approval_id,
                        epoch_i64(epoch)?,
                        state.as_str(),
                        now_ms()
                    ],
                )
                .map_err(|error| sqlite_error("resolving an external approval", &error))?;
            if state == ExternalApprovalState::Uncertain {
                transaction
                    .execute(
                        "UPDATE external_write_fences
                         SET state = 'uncertain', active_intent_id = NULL, updated_ms = ?4
                         WHERE endpoint_label = ?1 AND thread_id = ?2 AND epoch = ?3",
                        params![endpoint_label, thread_id, epoch_i64(epoch)?, now_ms()],
                    )
                    .map_err(|error| sqlite_error("fencing an uncertain approval", &error))?;
            }
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing external approval resolution", &error))?;
            Ok(ExternalTransitionOutcome::Applied)
        })
        .await
    }

    /// Resolves the claim correlated to one exact epoch-scoped server request key.
    ///
    /// # Errors
    ///
    /// Returns a validation or SQLite error.
    pub async fn resolve_external_approval_request(
        &self,
        endpoint_label: &str,
        request_key: &str,
        epoch: u64,
    ) -> Result<ExternalTransitionOutcome, StoreError> {
        validate_id(
            endpoint_label,
            ENDPOINT_LABEL_BYTE_LIMIT,
            "resolving an external approval request",
        )?;
        validate_id(
            request_key,
            REQUEST_KEY_BYTE_LIMIT,
            "resolving an external approval request",
        )?;
        let endpoint_label = endpoint_label.to_owned();
        let request_key = request_key.to_owned();
        let request_size = request_bytes(&[&endpoint_label, &request_key]);
        self.run_sized(request_size, move |connection| {
            let changed = connection
                .execute(
                    "UPDATE external_approval_claims SET state = 'resolved', updated_ms = ?4
                     WHERE endpoint_label = ?1 AND request_key = ?2 AND epoch = ?3
                       AND state IN ('received', 'claimed', 'responding', 'denied')",
                    params![endpoint_label, request_key, epoch_i64(epoch)?, now_ms()],
                )
                .map_err(|error| sqlite_error("resolving an external approval request", &error))?;
            Ok(if changed == 1 {
                ExternalTransitionOutcome::Applied
            } else {
                ExternalTransitionOutcome::Stale
            })
        })
        .await
    }

    /// Reads one durable approval claim.
    ///
    /// # Errors
    ///
    /// Returns a validation or SQLite error, including corrupt durable state.
    pub async fn external_approval_claim(
        &self,
        endpoint_label: &str,
        thread_id: &str,
        approval_id: &str,
    ) -> Result<Option<ExternalApprovalClaim>, StoreError> {
        validate_pair(endpoint_label, thread_id, "reading an external approval")?;
        validate_id(
            approval_id,
            INTENT_ID_BYTE_LIMIT,
            "reading an external approval",
        )?;
        let endpoint_label = endpoint_label.to_owned();
        let thread_id = thread_id.to_owned();
        let approval_id = approval_id.to_owned();
        let request_size = request_bytes(&[&endpoint_label, &thread_id, &approval_id]);
        self.run_sized(request_size, move |connection| {
            let raw = connection
                .query_row(
                    "SELECT epoch, turn_id, item_id, kind, state, source_actor, client_actor,
                            approval_actor, recipient_actor, deadline_ms
                     FROM external_approval_claims
                     WHERE endpoint_label = ?1 AND thread_id = ?2 AND approval_id = ?3",
                    params![endpoint_label, thread_id, approval_id],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, String>(5)?,
                            row.get::<_, String>(6)?,
                            row.get::<_, String>(7)?,
                            row.get::<_, String>(8)?,
                            row.get::<_, i64>(9)?,
                        ))
                    },
                )
                .optional()
                .map_err(|error| sqlite_error("reading an external approval", &error))?;
            raw.map(decode_approval).transpose()
        })
        .await
    }

    /// Changes the static approval actor only after all durable work has drained.
    ///
    /// # Errors
    ///
    /// Returns a validation, capacity, or SQLite error without changing the handler.
    pub async fn reassign_external_approval_actor(
        &self,
        endpoint_label: &str,
        epoch: u64,
        old_actor: &str,
        new_actor: &str,
    ) -> Result<ExternalApprovalReassignmentOutcome, StoreError> {
        validate_id(
            endpoint_label,
            ENDPOINT_LABEL_BYTE_LIMIT,
            "reassigning an external approval actor",
        )?;
        validate_id(
            old_actor,
            ACTOR_BYTE_LIMIT,
            "reassigning an external approval actor",
        )?;
        validate_id(
            new_actor,
            ACTOR_BYTE_LIMIT,
            "reassigning an external approval actor",
        )?;
        let endpoint_label = endpoint_label.to_owned();
        let old_actor = old_actor.to_owned();
        let new_actor = new_actor.to_owned();
        let request_size = request_bytes(&[&endpoint_label, &old_actor, &new_actor]);
        self.run_sized(request_size, move |connection| {
            let current: bool = connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM external_endpoint_epochs
                                   WHERE endpoint_label = ?1 AND current_epoch = ?2
                                     AND state = 'ready')",
                    params![endpoint_label, epoch_i64(epoch)?],
                    |row| row.get(0),
                )
                .map_err(|error| sqlite_error("checking approval reassignment epoch", &error))?;
            if !current {
                return Ok(ExternalApprovalReassignmentOutcome::Stale);
            }
            let busy: bool = connection
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM external_write_fences
                         WHERE endpoint_label = ?1 AND state != 'open'
                         UNION ALL
                         SELECT 1 FROM external_mutation_intents
                         WHERE endpoint_label = ?1 AND state IN ('prepared', 'sent', 'uncertain')
                         UNION ALL
                         SELECT 1 FROM external_mutation_intents i
                         WHERE i.endpoint_label = ?1 AND i.state = 'applied'
                           AND i.kind IN ('turn_start', 'turn_steer', 'queue_start')
                           AND i.result_id IS NOT NULL
                           AND NOT EXISTS(
                               SELECT 1 FROM external_turn_terminals x
                               WHERE x.endpoint_label = i.endpoint_label
                                 AND x.thread_id = i.thread_id AND x.turn_id = i.result_id
                           )
                         UNION ALL
                         SELECT 1 FROM external_approval_claims
                         WHERE endpoint_label = ?1
                           AND state IN ('received', 'claimed', 'responding', 'uncertain')
                     )",
                    params![endpoint_label],
                    |row| row.get(0),
                )
                .map_err(|error| sqlite_error("checking approval reassignment drain", &error))?;
            if busy {
                return Ok(ExternalApprovalReassignmentOutcome::NotDrained);
            }
            let changed = connection
                .execute(
                    "UPDATE external_write_fences SET approval_actor = ?3, updated_ms = ?4
                     WHERE endpoint_label = ?1 AND approval_actor = ?2 AND state = 'open'",
                    params![endpoint_label, old_actor, new_actor, now_ms()],
                )
                .map_err(|error| sqlite_error("reassigning an external approval actor", &error))?;
            if changed > EXTERNAL_MANAGED_THREAD_CAPACITY {
                return Err(StoreError::CapacityExceeded {
                    context: "reassigning external approval actors",
                });
            }
            Ok(ExternalApprovalReassignmentOutcome::Reassigned)
        })
        .await
    }
}

async fn transition_sent(
    store: &StoreHandle,
    endpoint_label: &str,
    thread_id: &str,
    intent_id: &str,
    epoch: u64,
) -> Result<ExternalTransitionOutcome, StoreError> {
    validate_pair(endpoint_label, thread_id, "sending an external mutation")?;
    validate_id(
        intent_id,
        INTENT_ID_BYTE_LIMIT,
        "sending an external mutation",
    )?;
    let endpoint_label = endpoint_label.to_owned();
    let thread_id = thread_id.to_owned();
    let intent_id = intent_id.to_owned();
    let request_size = request_bytes(&[&endpoint_label, &thread_id, &intent_id]);
    store
        .run_sized(request_size, move |connection| {
            let changed = connection
                .execute(
                    "UPDATE external_mutation_intents SET state = 'sent', updated_ms = ?5
                     WHERE endpoint_label = ?1 AND thread_id = ?2 AND intent_id = ?3
                       AND epoch = ?4 AND state = 'prepared'
                       AND EXISTS(SELECT 1 FROM external_write_fences
                                  WHERE endpoint_label = ?1 AND thread_id = ?2 AND epoch = ?4
                                    AND state = 'active' AND active_intent_id = ?3)",
                    params![
                        endpoint_label,
                        thread_id,
                        intent_id,
                        epoch_i64(epoch)?,
                        now_ms()
                    ],
                )
                .map_err(|error| sqlite_error("sending an external mutation", &error))?;
            if changed == 1 {
                return Ok(ExternalTransitionOutcome::Applied);
            }
            let state: Option<String> = connection
                .query_row(
                    "SELECT state FROM external_mutation_intents
                     WHERE endpoint_label = ?1 AND thread_id = ?2 AND intent_id = ?3
                       AND epoch = ?4",
                    params![endpoint_label, thread_id, intent_id, epoch_i64(epoch)?],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|error| sqlite_error("checking a sent external mutation", &error))?;
            match state.as_deref() {
                Some("sent") => Ok(ExternalTransitionOutcome::Applied),
                None => Ok(ExternalTransitionOutcome::Stale),
                Some(_) => Err(StoreError::InvalidTransition {
                    context: "sending an external mutation",
                }),
            }
        })
        .await
}

fn validate_intent(intent: &NewExternalMutationIntent) -> Result<(), StoreError> {
    validate_id(
        &intent.endpoint_label,
        ENDPOINT_LABEL_BYTE_LIMIT,
        "preparing an external mutation",
    )?;
    validate_id(
        &intent.thread_id,
        ROUTING_ID_BYTE_LIMIT,
        "preparing an external mutation",
    )?;
    validate_id(
        &intent.intent_id,
        INTENT_ID_BYTE_LIMIT,
        "preparing an external mutation",
    )?;
    for actor in [
        &intent.source_actor,
        &intent.client_actor,
        &intent.approval_actor,
    ] {
        validate_id(actor, ACTOR_BYTE_LIMIT, "preparing an external mutation")?;
    }
    if let Some(turn_id) = &intent.expected_turn_id {
        validate_id(
            turn_id,
            ROUTING_ID_BYTE_LIMIT,
            "preparing an external mutation",
        )?;
    }
    if let Some(message_id) = &intent.client_message_id {
        validate_id(
            message_id,
            ROUTING_ID_BYTE_LIMIT,
            "preparing an external mutation",
        )?;
    }
    let requires_turn = matches!(
        intent.kind,
        ExternalMutationKind::TurnSteer
            | ExternalMutationKind::TurnInterrupt
            | ExternalMutationKind::QueueAdd
    );
    let requires_message = matches!(
        intent.kind,
        ExternalMutationKind::TurnStart
            | ExternalMutationKind::TurnSteer
            | ExternalMutationKind::QueueAdd
    );
    if requires_turn != intent.expected_turn_id.is_some()
        || requires_message != intent.client_message_id.is_some()
    {
        return Err(StoreError::InvalidTransition {
            context: "validating an external mutation shape",
        });
    }
    Ok(())
}

#[allow(clippy::type_complexity)]
fn decode_intent(
    raw: (
        i64,
        String,
        String,
        Option<String>,
        Option<String>,
        String,
        String,
        String,
    ),
) -> Result<ExternalMutationIntent, StoreError> {
    Ok(ExternalMutationIntent {
        epoch: u64::try_from(raw.0).map_err(|_| StoreError::CorruptData {
            context: "decoding an external mutation epoch",
        })?,
        kind: ExternalMutationKind::parse(&raw.1).ok_or(StoreError::CorruptData {
            context: "decoding an external mutation kind",
        })?,
        state: ExternalMutationState::parse(&raw.2).ok_or(StoreError::CorruptData {
            context: "decoding an external mutation state",
        })?,
        expected_turn_id: raw.3,
        result_id: raw.4,
        source_actor: raw.5,
        client_actor: raw.6,
        approval_actor: raw.7,
    })
}

fn validate_approval_claim(claim: &NewExternalApprovalClaim) -> Result<(), StoreError> {
    validate_approval_key(
        &claim.endpoint_label,
        &claim.thread_id,
        &claim.approval_id,
        &claim.recipient_actor,
        "recording an external approval",
    )?;
    validate_id(
        &claim.request_key,
        REQUEST_KEY_BYTE_LIMIT,
        "recording an external approval",
    )?;
    validate_id(
        &claim.turn_id,
        ROUTING_ID_BYTE_LIMIT,
        "recording an external approval",
    )?;
    validate_id(
        &claim.item_id,
        ROUTING_ID_BYTE_LIMIT,
        "recording an external approval",
    )?;
    validate_id(
        &claim.client_actor,
        ACTOR_BYTE_LIMIT,
        "recording an external approval",
    )?;
    validate_id(
        &claim.approval_actor,
        ACTOR_BYTE_LIMIT,
        "recording an external approval",
    )?;
    if claim.deadline_ms <= now_ms() {
        return Err(StoreError::InvalidTransition {
            context: "recording an expired external approval",
        });
    }
    Ok(())
}

fn validate_approval_key(
    endpoint_label: &str,
    thread_id: &str,
    approval_id: &str,
    recipient_actor: &str,
    context: &'static str,
) -> Result<(), StoreError> {
    validate_id(endpoint_label, ENDPOINT_LABEL_BYTE_LIMIT, context)?;
    validate_id(thread_id, ROUTING_ID_BYTE_LIMIT, context)?;
    validate_id(approval_id, INTENT_ID_BYTE_LIMIT, context)?;
    validate_id(recipient_actor, ACTOR_BYTE_LIMIT, context)
}

#[allow(clippy::type_complexity)]
fn decode_approval(
    raw: (
        i64,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        i64,
    ),
) -> Result<ExternalApprovalClaim, StoreError> {
    Ok(ExternalApprovalClaim {
        epoch: u64::try_from(raw.0).map_err(|_| StoreError::CorruptData {
            context: "decoding an external approval epoch",
        })?,
        turn_id: raw.1,
        item_id: raw.2,
        kind: ExternalApprovalKind::parse(&raw.3).ok_or(StoreError::CorruptData {
            context: "decoding an external approval kind",
        })?,
        state: ExternalApprovalState::parse(&raw.4).ok_or(StoreError::CorruptData {
            context: "decoding an external approval state",
        })?,
        source_actor: raw.5,
        client_actor: raw.6,
        approval_actor: raw.7,
        recipient_actor: raw.8,
        deadline_ms: raw.9,
    })
}
