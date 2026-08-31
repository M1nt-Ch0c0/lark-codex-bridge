#![allow(clippy::doc_markdown)]

//! Inbound event dedup registration and its state machine.
//!
//! Legal transitions are `received → accepted → completed|rejected` plus
//! `received → completed|rejected`; terminal states are final. The direct
//! `received → completed` edge settles intentionally staged or ignored work
//! that must never create a Codex turn. Anything else is a
//! [`StoreError::InvalidTransition`], so a duplicate redelivery within the
//! TTL can never restart Codex (design §5.3).

use std::collections::HashSet;

use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};

use super::outbox::enforce_total_cap;
use super::{StoreError, StoreHandle, now_ms, query_optional, request_bytes, sqlite_error};
use crate::channel::{ConversationMode as ChatMode, MediaKind as ResourceKind};
use crate::lark::bridge::RetainedInbound;
use crate::lark::normalize::ShortId;
use crate::lark::normalize::{
    InboundEvent, MediaMetadata, MediaPart, MentionIdentity, MessagePart, PartStatus, ResourceDesc,
    ScopeKey,
};
use crate::limits::{
    ATTACHMENT_FILE_NAME_MAX_BYTES, ATTACHMENT_MIME_MAX_BYTES, DEDUP_SWEEP_BATCH,
    STORE_INBOUND_BEGIN_MAX_KEY_BYTES, STORE_INBOUND_BEGIN_MAX_KEYS, STORE_INBOUND_ID_MAX_BYTES,
    STORE_INBOUND_MAX_BYTES, STORE_INBOUND_MAX_ROWS, STORE_INBOUND_MESSAGE_TYPE_MAX_BYTES,
    STORE_INBOUND_PAYLOAD_MAX_BYTES, STORE_INBOUND_RECEIVED_MAX_BYTES,
    STORE_INBOUND_RECEIVED_MAX_ROWS, STORE_INBOUND_RESOURCE_KEY_MAX_BYTES,
    STORE_INBOUND_RESOURCE_KEY_MAX_TOTAL_BYTES, STORE_INBOUND_RESOURCE_MAX_COUNT,
    STORE_INBOUND_SCOPE_MAX_BYTES, STORE_INBOUND_TEXT_MAX_BYTES, STORE_OUTBOX_MAX_QUEUED_BYTES,
    STORE_OUTBOX_MAX_ROWS, STORE_OUTBOX_PAYLOAD_MAX_BYTES, STORE_RECOVERY_TURN_MAX_BYTES,
    STORE_RECOVERY_TURN_MAX_ROWS, STORE_REJECTION_REASON_MAX_BYTES,
};
use crate::runtime::intake::TenantNamespace;

const INBOUND_PAYLOAD_VERSION: i64 = 1;
const TENANT_NAMESPACE_HEX_BYTES: usize = 64;
const INBOUND_REPLY_KEY_PREFIX: &str = "inbound:v1:";
const INBOUND_REPLY_NOTICE_PREFIX: &str = "notice:";
const INBOUND_REPLY_KEY_MAX_BYTES: usize = INBOUND_REPLY_KEY_PREFIX.len()
    + TENANT_NAMESPACE_HEX_BYTES
    + 1
    + STORE_INBOUND_ID_MAX_BYTES
    + 1
    + INBOUND_REPLY_NOTICE_PREFIX.len()
    + STORE_REJECTION_REASON_MAX_BYTES;

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
#[derive(Debug)]
pub enum DedupOutcome {
    /// First delivery within the TTL; the row was inserted as `received`.
    New(RetainedInbound),
    /// A same-message delivery replayed the existing canonical received row.
    ReplayReceived(RetainedInbound),
    /// This `(tenant, event_id)` was already registered; carries the prior
    /// state so the caller can log or absorb the redelivery.
    Duplicate {
        /// Canonical key of the existing row.
        key: InboundKey,
        /// State of the existing row.
        state: InboundEventState,
        /// Associated turn for an accepted or terminal row, when present.
        turn_row_id: Option<i64>,
    },
}

/// Stable identity of one inbound row.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct InboundKey {
    pub(crate) tenant: TenantNamespace,
    pub(crate) event_id: String,
}

impl std::fmt::Debug for InboundKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InboundKey")
            .field("tenant", &self.tenant)
            .field("event_id", &ShortId(&self.event_id))
            .finish()
    }
}

impl InboundKey {
    /// Builds an inbound key. Store APIs validate its bounds before use.
    #[must_use]
    pub fn new(tenant: TenantNamespace, event_id: String) -> Self {
        Self { tenant, event_id }
    }

    /// Derives the globally unique durable-outbox identity for this event's
    /// bridge-control reply.
    ///
    /// The tenant namespace is already a one-way, credential-free digest.
    /// Including it here mirrors the `(tenant, event_id)` inbox primary key so
    /// two applications receiving the same provider event ID cannot reuse one
    /// another's outbound effect.
    #[must_use]
    pub fn control_outbox_idempotency_key(&self) -> String {
        self.outbox_idempotency_key("control")
    }

    /// Derives the globally unique durable-outbox identity for this event's
    /// classified rejection notice.
    #[must_use]
    pub fn rejection_outbox_idempotency_key(&self, reason: InboundRejectionKind) -> String {
        self.outbox_idempotency_key(&format!("notice:{}", reason.as_str()))
    }

    fn outbox_idempotency_key(&self, suffix: &str) -> String {
        inbound_reply_outbox_key(&self.tenant.as_hex(), &self.event_id, suffix)
    }
}

fn inbound_reply_outbox_key(tenant: &str, event_id: &str, suffix: &str) -> String {
    format!("{INBOUND_REPLY_KEY_PREFIX}{tenant}:{event_id}:{suffix}")
}

/// Canonical inbound row claimed by a newly-created turn.
#[derive(Debug)]
pub struct ClaimedInbound {
    /// Stable canonical row key.
    pub key: InboundKey,
    /// Persisted canonical event and byte accounting.
    pub retained: RetainedInbound,
}

/// Existing terminal/claimed row skipped while beginning a turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedInbound {
    /// Stable canonical row key.
    pub key: InboundKey,
    /// Observed durable state.
    pub state: InboundEventState,
    /// Associated turn, when one exists.
    pub turn_row_id: Option<i64>,
}

/// Atomic turn creation and inbound-claim result.
#[derive(Debug)]
pub enum BeginTurnOutcome {
    /// At least one received row was claimed by a new starting turn.
    Started {
        /// New turn row ID.
        turn_row_id: i64,
        /// Canonical received rows claimed by the turn.
        claimed: Vec<ClaimedInbound>,
        /// Existing terminal/claimed rows skipped.
        skipped: Vec<SkippedInbound>,
    },
    /// Every input was already claimed or terminal, so no turn was created.
    NoReceived {
        /// Existing rows that prevented turn creation.
        skipped: Vec<SkippedInbound>,
    },
}

/// Closed terminal state requested for linked inbound markers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundTerminal {
    /// Business work completed.
    Completed,
    /// Business work did not complete.
    Rejected,
}

/// Closed turn resolution used by the combined terminal transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnResolution {
    /// Turn completed normally.
    Completed,
    /// Turn failed.
    Failed,
    /// Turn was interrupted.
    Interrupted,
    /// Turn start/execution outcome is uncertain and is terminalized.
    Uncertain,
}

/// Idempotent result of the combined terminal transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolveTurnOutcome {
    /// This call terminalized the turn and its accepted markers.
    Resolved { inbound_rows: usize },
    /// The same resolution had already been committed.
    AlreadyResolved { inbound_rows: usize },
}

/// Closed operator-facing rejection classifications.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundRejectionKind {
    /// Local bounded capacity is exhausted.
    Overloaded,
    /// Sender is a human P2P caller with no owner/sender/group grant.
    NotOwner,
    /// Sender is not an ordinary human (bot/app/system/anonymous).
    NotSender,
    /// Sender is a human group/topic caller with no owner/sender/allowed-group
    /// grant.
    NotGroup,
    /// A group/topic message lacked a real direct bot mention.
    MissingMention,
    /// An owner-only control command arrived from a non-owner sender.
    OwnerCommandRequired,
    /// Runtime policy refused the event for another reason.
    Policy,
    /// Event is too old to process safely.
    Stale,
    /// Internal processing refused the event without exposing content.
    Internal,
}

impl InboundRejectionKind {
    const ALL: [Self; 9] = [
        Self::Overloaded,
        Self::NotOwner,
        Self::NotSender,
        Self::NotGroup,
        Self::MissingMention,
        Self::OwnerCommandRequired,
        Self::Policy,
        Self::Stale,
        Self::Internal,
    ];

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Overloaded => "overloaded",
            Self::NotOwner => "not_owner",
            Self::NotSender => "not_sender",
            Self::NotGroup => "not_group",
            Self::MissingMention => "missing_mention",
            Self::OwnerCommandRequired => "owner_command_required",
            Self::Policy => "policy",
            Self::Stale => "stale",
            Self::Internal => "internal",
        }
    }
}

/// Result of an idempotent received-row rejection attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundDisposition {
    /// This call completed a received row without associating a turn.
    Completed,
    /// This call changed received to rejected.
    Rejected,
    /// The same rejection was already committed.
    AlreadyRejected,
    /// A turn already claimed the event.
    AlreadyClaimed { turn_row_id: i64 },
    /// The row was already completed.
    AlreadyCompleted,
}

impl StoreHandle {
    /// Registers one inbound event under `(tenant, event_id)`.
    ///
    /// Persists the bounded, versioned normalized payload for crash replay.
    /// Payload content stays private to this module and never appears in
    /// `Debug` output or dynamic error text.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or SQLite fails.
    pub async fn register_inbound(
        &self,
        tenant: &TenantNamespace,
        event: &InboundEvent,
    ) -> Result<DedupOutcome, StoreError> {
        validate_incoming_key(event)?;
        let tenant_namespace = tenant.clone();
        let tenant = tenant.as_hex();
        let incoming = event.clone();
        let event_id = event.event_id.clone();
        let message_id = event.message_id.clone();
        // The queued closure owns a full normalized event and later materializes
        // a JSON payload while the same permit is held. Count both string-backed
        // representations plus a conservative structural allowance without
        // performing logical validation before duplicate lookup.
        let captured_event_bytes = inbound_event_variable_bytes(event);
        let request_size = request_bytes(&[&tenant, &event_id, &message_id])
            .saturating_add(captured_event_bytes.saturating_mul(2))
            .saturating_add(2 * 1024);
        self.run_sized(request_size, move |connection| {
            let transaction = connection
                .transaction()
                .map_err(|error| sqlite_error("starting inbound registration", &error))?;

            if let Some(stored) = read_inbound_row(&transaction, &tenant, &event_id)? {
                return registration_from_stored(stored, tenant_namespace, true, Some(&incoming));
            }

            let candidates = read_message_candidates(&transaction, &tenant, &message_id)?;
            match candidates.as_slice() {
                [] => {}
                [stored] => {
                    return registration_from_stored(stored.clone(), tenant_namespace, false, None);
                }
                _ => {
                    return Err(StoreError::CorruptData {
                        context: "selecting a canonical inbound message",
                    });
                }
            }

            let scope_key = incoming.scope.to_string();
            let payload = encode_event(&incoming)?;
            let payload_bytes = payload.len();
            let logical_bytes = request_bytes(&[&tenant, &event_id, &message_id, &scope_key])
                .saturating_add(payload_bytes);
            ensure_inbound_capacity(&transaction, logical_bytes, payload_bytes)?;
            let now = now_ms();
            transaction
                .execute(
                    "INSERT INTO inbound_events
                     (tenant, event_id, message_id, scope_key, state, first_seen_ms, updated_ms,
                      payload_version, payload_blob, payload_bytes, turn_row_id)
                     VALUES (?1, ?2, ?3, ?4, 'received', ?5, ?5, ?6, ?7, ?8, NULL)",
                    params![
                        tenant,
                        event_id,
                        message_id,
                        scope_key,
                        now,
                        INBOUND_PAYLOAD_VERSION,
                        payload,
                        i64::try_from(payload_bytes).unwrap_or(i64::MAX)
                    ],
                )
                .map_err(|error| sqlite_error("registering an inbound event", &error))?;
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing inbound registration", &error))?;
            Ok(DedupOutcome::New(RetainedInbound::new(
                Box::new(incoming),
                payload_bytes,
            )))
        })
        .await
    }

    /// Atomically creates a starting turn and claims its received inbound rows.
    ///
    /// # Errors
    ///
    /// Returns a classified store error for invalid keys/state, capacity,
    /// corruption, or any failed SQLite transaction.
    #[allow(clippy::too_many_lines)]
    pub async fn begin_turn_and_claim_inbound(
        &self,
        turn: super::NewTurnRow,
        events: &[InboundKey],
    ) -> Result<BeginTurnOutcome, StoreError> {
        self.begin_turn_and_claim_inbound_inner(
            turn,
            events.iter().cloned().map(|key| (key, None)).collect(),
        )
        .await
    }

    /// Runtime-only claim path that substitutes the authenticated live event
    /// after validating the corresponding durable, secret-free descriptor.
    /// Crash recovery deliberately uses [`Self::begin_turn_and_claim_inbound`]
    /// and therefore degrades media to unavailable instead of persisting a
    /// reusable Lark resource key or transcript.
    pub(crate) async fn begin_turn_and_claim_inbound_live(
        &self,
        turn: super::NewTurnRow,
        events: Vec<(InboundKey, InboundEvent)>,
    ) -> Result<BeginTurnOutcome, StoreError> {
        self.begin_turn_and_claim_inbound_inner(
            turn,
            events
                .into_iter()
                .map(|(key, event)| (key, Some(event)))
                .collect(),
        )
        .await
    }

    #[allow(clippy::too_many_lines)]
    async fn begin_turn_and_claim_inbound_inner(
        &self,
        turn: super::NewTurnRow,
        events: Vec<(InboundKey, Option<InboundEvent>)>,
    ) -> Result<BeginTurnOutcome, StoreError> {
        if turn.state != super::TurnState::Starting {
            return Err(StoreError::InvalidTransition {
                context: "creating an inbound turn outside the starting state",
            });
        }
        if events.len() > STORE_INBOUND_BEGIN_MAX_KEYS {
            return Err(StoreError::CapacityExceeded {
                context: "claiming too many inbound rows",
            });
        }
        let mut unique = HashSet::with_capacity(events.len());
        let mut key_bytes = 0_usize;
        let mut live_bytes = 0_usize;
        for (key, live) in &events {
            validate_id(&key.event_id, "validating a claimed inbound event ID")?;
            let tenant = key.tenant.as_hex();
            key_bytes = key_bytes
                .saturating_add(tenant.len())
                .saturating_add(key.event_id.len());
            if !unique.insert((tenant, key.event_id.clone())) {
                return Err(StoreError::CorruptData {
                    context: "validating unique inbound claim keys",
                });
            }
            if let Some(live) = live {
                validate_event(live)?;
                if live.event_id != key.event_id || live.scope.to_string() != turn.scope_key {
                    return Err(StoreError::CorruptData {
                        context: "validating a live inbound claim identity",
                    });
                }
                live_bytes = live_bytes.saturating_add(inbound_event_variable_bytes(live));
            }
        }
        if key_bytes > STORE_INBOUND_BEGIN_MAX_KEY_BYTES {
            return Err(StoreError::PayloadTooLarge {
                context: "claiming inbound row keys",
                limit: u64::try_from(STORE_INBOUND_BEGIN_MAX_KEY_BYTES).unwrap_or(u64::MAX),
            });
        }
        let request_size = key_bytes
            .saturating_add(live_bytes)
            .saturating_add(request_bytes(&[
                &turn.scope_key,
                &turn.client_message_id,
                turn.codex_thread_id.as_deref().unwrap_or_default(),
            ]));
        self.run_sized(request_size, move |connection| {
            let transaction = connection
                .transaction()
                .map_err(|error| sqlite_error("starting inbound turn claim", &error))?;
            let mut received = Vec::new();
            let mut skipped = Vec::new();
            for (key, live) in events {
                let tenant = key.tenant.as_hex();
                let stored = read_inbound_row(&transaction, &tenant, &key.event_id)?.ok_or(
                    StoreError::NotFound {
                        context: "claiming an unknown inbound row",
                    },
                )?;
                if stored.scope_key != turn.scope_key {
                    return Err(StoreError::CorruptData {
                        context: "claiming an inbound row from another scope",
                    });
                }
                validate_stored_inbound_row(&transaction, &tenant, &stored)?;
                match stored.state {
                    InboundEventState::Received => {
                        if stored.turn_row_id.is_some() {
                            return Err(StoreError::CorruptData {
                                context: "claiming an associated received row",
                            });
                        }
                        let persisted = retained_from_stored(stored)?;
                        let retained = if let Some(live) = live {
                            validate_live_claim(&live, persisted.event())?;
                            RetainedInbound::new(Box::new(live), persisted.retained_bytes())
                        } else {
                            persisted
                        };
                        received.push((key, retained));
                    }
                    InboundEventState::Accepted
                    | InboundEventState::Completed
                    | InboundEventState::Rejected => {
                        skipped.push(SkippedInbound {
                            key,
                            state: stored.state,
                            turn_row_id: stored.turn_row_id,
                        });
                    }
                }
            }
            if received.is_empty() {
                return Ok(BeginTurnOutcome::NoReceived { skipped });
            }

            ensure_new_turn_capacity(&transaction, &turn)?;
            let now = now_ms();
            transaction
                .execute(
                    "INSERT INTO turns
                     (scope_key, client_message_id, codex_thread_id, state, uncertain,
                      created_ms, updated_ms, inbound_count)
                     VALUES (?1, ?2, ?3, 'starting', 0, ?4, ?4, ?5)",
                    params![
                        turn.scope_key,
                        turn.client_message_id,
                        turn.codex_thread_id,
                        now,
                        i64::try_from(received.len()).unwrap_or(i64::MAX)
                    ],
                )
                .map_err(|error| sqlite_error("recording an inbound turn", &error))?;
            let turn_row_id = transaction.last_insert_rowid();
            for (key, _) in &received {
                let changed = transaction
                    .execute(
                        "UPDATE inbound_events
                         SET state = 'accepted', turn_row_id = ?3, updated_ms = ?4
                         WHERE tenant = ?1 AND event_id = ?2
                           AND state = 'received' AND turn_row_id IS NULL",
                        params![key.tenant.as_hex(), key.event_id, turn_row_id, now],
                    )
                    .map_err(|error| sqlite_error("claiming an inbound row", &error))?;
                if changed != 1 {
                    return Err(StoreError::CorruptData {
                        context: "claiming a concurrently changed inbound row",
                    });
                }
            }
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing inbound turn claim", &error))?;
            let claimed = received
                .into_iter()
                .map(|(key, retained)| ClaimedInbound { key, retained })
                .collect();
            Ok(BeginTurnOutcome::Started {
                turn_row_id,
                claimed,
                skipped,
            })
        })
        .await
    }

    /// Atomically terminalizes a runtime turn and all linked accepted rows.
    ///
    /// # Errors
    ///
    /// Returns a classified store error for an illegal/conflicting resolution,
    /// broken cross-row invariants, or any failed SQLite transaction.
    #[allow(clippy::too_many_lines)]
    pub async fn resolve_turn_and_finish_inbound_batch(
        &self,
        turn_row_id: i64,
        turn: TurnResolution,
        inbound: InboundTerminal,
    ) -> Result<ResolveTurnOutcome, StoreError> {
        let (target_turn, target_inbound, reason) = match turn {
            TurnResolution::Completed => ("completed", InboundTerminal::Completed, None),
            TurnResolution::Failed => ("failed", InboundTerminal::Rejected, Some("turn_failed")),
            TurnResolution::Interrupted => (
                "interrupted",
                InboundTerminal::Rejected,
                Some("turn_interrupted"),
            ),
            TurnResolution::Uncertain => (
                "uncertain",
                InboundTerminal::Rejected,
                Some("turn_uncertain"),
            ),
        };
        if inbound != target_inbound {
            return Err(StoreError::InvalidTransition {
                context: "resolving a turn with conflicting inbound state",
            });
        }
        self.run(move |connection| {
            let transaction = connection
                .transaction()
                .map_err(|error| sqlite_error("starting inbound turn resolution", &error))?;
            let (current, uncertain, inbound_count, turn_scope): (String, i64, i64, String) =
                transaction
                .query_row(
                    "SELECT state, uncertain, inbound_count, scope_key FROM turns WHERE id = ?1",
                    params![turn_row_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .map_err(|error| match error {
                    rusqlite::Error::QueryReturnedNoRows => StoreError::NotFound {
                        context: "resolving an unknown inbound turn",
                    },
                    other => sqlite_error("reading an inbound turn resolution", &other),
                })?;
            let inbound_count =
                usize::try_from(inbound_count).map_err(|_| StoreError::CorruptData {
                    context: "validating an inbound turn claim count",
                })?;
            if inbound_count == 0 {
                return Err(StoreError::CorruptData {
                    context: "resolving a turn without historical inbound links",
                });
            }
            let expected_inbound = match target_inbound {
                InboundTerminal::Completed => "completed",
                InboundTerminal::Rejected => "rejected",
            };
            if current == target_turn && uncertain == 0 {
                validate_resolved_markers(
                    &transaction,
                    turn_row_id,
                    inbound_count,
                    expected_inbound,
                )?;
                return Ok(ResolveTurnOutcome::AlreadyResolved {
                    inbound_rows: inbound_count,
                });
            }
            if current == "uncertain" && uncertain == 0 {
                if !matches!(turn, TurnResolution::Failed | TurnResolution::Interrupted) {
                    return Err(StoreError::InvalidTransition {
                        context: "refining a resolved uncertain turn",
                    });
                }
                validate_resolved_markers(&transaction, turn_row_id, inbound_count, "rejected")?;
                let now = now_ms();
                transaction
                    .execute(
                        "UPDATE turns SET state = ?2, uncertain = 0, updated_ms = ?3
                         WHERE id = ?1",
                        params![turn_row_id, target_turn, now],
                    )
                    .map_err(|error| sqlite_error("refining an uncertain turn", &error))?;
                transaction
                    .execute(
                        "UPDATE inbound_events SET rejection_reason = ?2, updated_ms = ?3
                         WHERE turn_row_id = ?1 AND state = 'rejected'",
                        params![turn_row_id, reason, now],
                    )
                    .map_err(|error| sqlite_error("refining uncertain inbound markers", &error))?;
                transaction.commit().map_err(|error| {
                    sqlite_error("committing uncertain turn refinement", &error)
                })?;
                return Ok(ResolveTurnOutcome::Resolved {
                    inbound_rows: inbound_count,
                });
            }
            if matches!(current.as_str(), "completed" | "failed" | "interrupted") {
                return Err(StoreError::InvalidTransition {
                    context: "resolving a terminal turn differently",
                });
            }
            let legal = match current.as_str() {
                "starting" => matches!(
                    turn,
                    TurnResolution::Failed
                        | TurnResolution::Interrupted
                        | TurnResolution::Uncertain
                ),
                "running" => true,
                "uncertain" if uncertain != 0 => true,
                _ => false,
            };
            if !legal {
                return Err(StoreError::InvalidTransition {
                    context: "resolving an inbound turn",
                });
            }
            validate_unresolved_markers(&transaction, turn_row_id, &turn_scope, inbound_count)?;
            let now = now_ms();
            transaction
                .execute(
                    "UPDATE turns
                     SET state = ?2, uncertain = 0, updated_ms = ?3
                     WHERE id = ?1",
                    params![turn_row_id, target_turn, now],
                )
                .map_err(|error| sqlite_error("resolving an inbound turn", &error))?;
            let changed = transaction
                .execute(
                    "UPDATE inbound_events
                     SET state = ?2, rejection_reason = ?3, payload_version = NULL,
                         payload_blob = NULL, payload_bytes = 0, updated_ms = ?4
                     WHERE turn_row_id = ?1 AND state = 'accepted'",
                    params![turn_row_id, expected_inbound, reason, now],
                )
                .map_err(|error| sqlite_error("finishing accepted inbound rows", &error))?;
            if changed != inbound_count {
                return Err(StoreError::CorruptData {
                    context: "finishing every accepted inbound row",
                });
            }
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing inbound turn resolution", &error))?;
            Ok(ResolveTurnOutcome::Resolved {
                inbound_rows: inbound_count,
            })
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
        tenant: &TenantNamespace,
        event_id: &str,
    ) -> Result<Option<InboundEventState>, StoreError> {
        let tenant = tenant.as_hex();
        let event_id = event_id.to_owned();
        let request_size = request_bytes(&[&tenant, &event_id]);
        self.run_sized(request_size, move |connection| {
            read_inbound_state(connection, &tenant, &event_id)
        })
        .await
    }

    /// Recovers the complete bounded current-tenant received set.
    ///
    /// # Errors
    ///
    /// Returns a classified store error when any global or tenant row violates
    /// strict payload, association, canonical-message, count, or byte bounds.
    pub async fn recover_received(
        &self,
        tenant: &TenantNamespace,
    ) -> Result<Vec<RetainedInbound>, StoreError> {
        let tenant = tenant.as_hex();
        self.run_sized(tenant.len(), move |connection| {
            validate_inbound_collection(connection)?;
            let mut statement = connection
                .prepare(
                    "SELECT event_id, message_id, scope_key, state, payload_version,
                            payload_blob, payload_bytes, turn_row_id, rejection_reason,
                            reply_outbox_key
                     FROM inbound_events
                     WHERE tenant = ?1 AND state = 'received'
                     ORDER BY first_seen_ms, event_id",
                )
                .map_err(|error| sqlite_error("preparing received recovery", &error))?;
            let stored = statement
                .query_map(params![tenant], decode_stored_row_with_reply_effect)
                .map_err(|error| sqlite_error("reading received recovery", &error))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| sqlite_error("decoding received recovery", &error))?;
            stored.into_iter().map(retained_from_stored).collect()
        })
        .await
    }

    /// Idempotently rejects one currently received row.
    ///
    /// # Errors
    ///
    /// Returns a classified store error for an unknown/corrupt row, conflicting
    /// terminal disposition, or failed SQLite transaction.
    pub async fn reject_received(
        &self,
        key: &InboundKey,
        reason: InboundRejectionKind,
    ) -> Result<InboundDisposition, StoreError> {
        let key = key.clone();
        let request_size = key.tenant.as_hex().len() + key.event_id.len();
        self.run_sized(request_size, move |connection| {
            let transaction = connection
                .transaction()
                .map_err(|error| sqlite_error("starting inbound rejection", &error))?;
            let disposition = reject_received_in_transaction(&transaction, &key, reason)?;
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing inbound rejection", &error))?;
            Ok(disposition)
        })
        .await
    }

    /// Idempotently completes one received row without creating a turn.
    ///
    /// This is the durable settlement boundary for intentionally staged P2P
    /// media metadata and ignored group/topic media. The replay payload is
    /// erased atomically, and no user content or resource key is copied into
    /// a reason column.
    ///
    /// # Errors
    ///
    /// Returns a classified store error for an unknown/corrupt row, an
    /// already-claimed row, or a failed SQLite transaction.
    pub async fn complete_received_without_turn(
        &self,
        key: &InboundKey,
    ) -> Result<InboundDisposition, StoreError> {
        let key = key.clone();
        let request_size = key.tenant.as_hex().len() + key.event_id.len();
        self.run_sized(request_size, move |connection| {
            let transaction = connection
                .transaction()
                .map_err(|error| sqlite_error("starting no-turn inbound completion", &error))?;
            let tenant = key.tenant.as_hex();
            let stored = read_inbound_row(&transaction, &tenant, &key.event_id)?.ok_or(
                StoreError::NotFound {
                    context: "completing an unknown inbound row without a turn",
                },
            )?;
            let disposition = match stored.state {
                InboundEventState::Received => {
                    let _ = retained_from_stored(stored)?;
                    let changed = transaction
                        .execute(
                            "UPDATE inbound_events
                             SET state = 'completed', payload_version = NULL,
                                 payload_blob = NULL, payload_bytes = 0, updated_ms = ?3
                             WHERE tenant = ?1 AND event_id = ?2
                               AND state = 'received' AND turn_row_id IS NULL",
                            params![tenant, key.event_id, now_ms()],
                        )
                        .map_err(|error| {
                            sqlite_error("completing a received row without a turn", &error)
                        })?;
                    if changed != 1 {
                        return Err(StoreError::CorruptData {
                            context: "completing a concurrently changed inbound row",
                        });
                    }
                    InboundDisposition::Completed
                }
                InboundEventState::Completed if stored.turn_row_id.is_none() => {
                    InboundDisposition::AlreadyCompleted
                }
                InboundEventState::Accepted => {
                    if stored.turn_row_id.is_none() {
                        return Err(StoreError::CorruptData {
                            context: "completing an unassociated accepted inbound row",
                        });
                    }
                    return Err(StoreError::InvalidTransition {
                        context: "completing an already-claimed inbound row without a turn",
                    });
                }
                InboundEventState::Completed | InboundEventState::Rejected => {
                    return Err(StoreError::InvalidTransition {
                        context: "completing a terminal inbound row without a turn",
                    });
                }
            };
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing no-turn inbound completion", &error))?;
            Ok(disposition)
        })
        .await
    }

    /// Atomically enqueues a deterministic control reply and completes one
    /// received row without creating a Codex turn.
    ///
    /// This is the settlement boundary for bridge-owned slash commands. The
    /// inbound payload is erased in the same transaction that makes the reply
    /// durable, so a crash cannot lose a command response after marking the
    /// event complete. A marker-less legacy completion may backfill the exact
    /// same idempotent outbox row once. Within the bounded dedup window, the
    /// terminal inbound marker or same-key outbox row prevents recreation; an
    /// extremely late webhook after both retention horizons is treated as new.
    ///
    /// # Errors
    ///
    /// Returns a classified error for an unknown/corrupt inbound row, a reply
    /// whose scope or idempotency identity does not match, an already claimed
    /// or rejected row, outbox capacity, or failed SQLite persistence.
    #[allow(clippy::too_many_lines)]
    pub async fn complete_received_and_enqueue_control_reply(
        &self,
        key: &InboundKey,
        reply: super::NewOutboxRow,
    ) -> Result<InboundDisposition, StoreError> {
        let key = key.clone();
        let request_size = request_bytes(&[
            &key.tenant.as_hex(),
            &key.event_id,
            &reply.idempotency_key,
            &reply.scope_key,
            &reply.kind,
            &reply.payload_json,
        ]);
        self.run_sized(request_size, move |connection| {
            let transaction = connection
                .transaction()
                .map_err(|error| sqlite_error("starting control reply completion", &error))?;
            let tenant = key.tenant.as_hex();
            if reply.idempotency_key != key.control_outbox_idempotency_key()
                || reply.kind != "control"
                || reply.next_retry_ms != 0
            {
                return Err(StoreError::CorruptData {
                    context: "validating a control reply identity",
                });
            }
            let stored = read_inbound_row(&transaction, &tenant, &key.event_id)?.ok_or(
                StoreError::NotFound {
                    context: "completing an unknown control inbound row",
                },
            )?;
            let recorded_effect = read_inbound_reply_effect(&transaction, &tenant, &key.event_id)?;
            if reply.scope_key != stored.scope_key {
                return Err(StoreError::CorruptData {
                    context: "validating a control reply scope",
                });
            }
            let disposition = match stored.state {
                InboundEventState::Received => {
                    if recorded_effect.is_some() {
                        return Err(StoreError::CorruptData {
                            context: "validating a received control reply effect marker",
                        });
                    }
                    let _ = retained_from_stored(stored)?;
                    enqueue_inbound_reply_in_transaction(&transaction, &reply)?;
                    let changed = transaction
                        .execute(
                            "UPDATE inbound_events
                             SET state = 'completed', payload_version = NULL,
                                 payload_blob = NULL, payload_bytes = 0,
                                 reply_outbox_key = ?3, updated_ms = ?4
                             WHERE tenant = ?1 AND event_id = ?2
                               AND state = 'received' AND turn_row_id IS NULL
                               AND reply_outbox_key IS NULL",
                            params![tenant, key.event_id, reply.idempotency_key, now_ms()],
                        )
                        .map_err(|error| {
                            sqlite_error("completing a control inbound row", &error)
                        })?;
                    if changed != 1 {
                        return Err(StoreError::CorruptData {
                            context: "completing a concurrently changed control inbound row",
                        });
                    }
                    InboundDisposition::Completed
                }
                InboundEventState::Completed if stored.turn_row_id.is_none() => {
                    match recorded_effect {
                        Some(recorded) if recorded == reply.idempotency_key => {
                            let _ = validate_existing_inbound_reply(&transaction, &reply)?;
                        }
                        Some(_) => {
                            return Err(StoreError::CorruptData {
                                context: "validating a completed control reply effect marker",
                            });
                        }
                        None => {
                            enqueue_inbound_reply_in_transaction(&transaction, &reply)?;
                            record_inbound_reply_effect(
                                &transaction,
                                &tenant,
                                &key.event_id,
                                &reply.idempotency_key,
                            )?;
                        }
                    }
                    InboundDisposition::AlreadyCompleted
                }
                InboundEventState::Accepted => {
                    return Err(StoreError::InvalidTransition {
                        context: "completing an already-claimed control inbound row",
                    });
                }
                InboundEventState::Completed | InboundEventState::Rejected => {
                    return Err(StoreError::InvalidTransition {
                        context: "completing a terminal non-command inbound row as control",
                    });
                }
            };
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing control reply completion", &error))?;
            Ok(disposition)
        })
        .await
    }

    /// Atomically enqueues a notice and rejects one currently received row.
    ///
    /// # Errors
    ///
    /// Returns a classified store error for invalid notice identity/scope,
    /// outbox capacity, row races/invariants, or failed SQLite persistence.
    pub async fn reject_received_and_enqueue_notice(
        &self,
        key: &InboundKey,
        reason: InboundRejectionKind,
        notice: super::NewOutboxRow,
    ) -> Result<InboundDisposition, StoreError> {
        let key = key.clone();
        let request_size = request_bytes(&[
            &key.tenant.as_hex(),
            &key.event_id,
            &notice.idempotency_key,
            &notice.scope_key,
            &notice.kind,
            &notice.payload_json,
        ]);
        self.run_sized(request_size, move |connection| {
            let transaction = connection
                .transaction()
                .map_err(|error| sqlite_error("starting rejection notice", &error))?;
            let tenant = key.tenant.as_hex();
            if notice.idempotency_key != key.rejection_outbox_idempotency_key(reason)
                || notice.kind != "notice"
                || notice.next_retry_ms != 0
            {
                return Err(StoreError::CorruptData {
                    context: "validating an inbound rejection notice identity",
                });
            }
            let stored = read_inbound_row(&transaction, &tenant, &key.event_id)?.ok_or(
                StoreError::NotFound {
                    context: "rejecting an unknown inbound row",
                },
            )?;
            let recorded_effect = read_inbound_reply_effect(&transaction, &tenant, &key.event_id)?;
            let existing = terminal_disposition(&stored, reason)?;
            if notice.scope_key != stored.scope_key {
                return Err(StoreError::CorruptData {
                    context: "validating an inbound rejection notice scope",
                });
            }
            if let Some(disposition) = existing {
                if disposition == InboundDisposition::AlreadyRejected {
                    match recorded_effect {
                        Some(recorded) if recorded == notice.idempotency_key => {
                            let _ = validate_existing_inbound_reply(&transaction, &notice)?;
                        }
                        Some(_) => {
                            return Err(StoreError::CorruptData {
                                context: "validating a rejected reply effect marker",
                            });
                        }
                        None => {
                            enqueue_inbound_reply_in_transaction(&transaction, &notice)?;
                            record_inbound_reply_effect(
                                &transaction,
                                &tenant,
                                &key.event_id,
                                &notice.idempotency_key,
                            )?;
                            transaction.commit().map_err(|error| {
                                sqlite_error("committing a backfilled rejection notice", &error)
                            })?;
                        }
                    }
                }
                return Ok(disposition);
            }
            if recorded_effect.is_some() {
                return Err(StoreError::CorruptData {
                    context: "validating a received rejection reply effect marker",
                });
            }
            let _ = retained_from_stored(stored)?;
            enqueue_inbound_reply_in_transaction(&transaction, &notice)?;
            let disposition = reject_received_in_transaction(&transaction, &key, reason)?;
            record_inbound_reply_effect(
                &transaction,
                &tenant,
                &key.event_id,
                &notice.idempotency_key,
            )?;
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing rejection notice", &error))?;
            Ok(disposition)
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
    pub async fn sweep_inbound(
        &self,
        older_than_ms: i64,
        max_rows: u32,
    ) -> Result<u64, StoreError> {
        let max_rows = max_rows.min(DEDUP_SWEEP_BATCH);
        if max_rows == 0 {
            return Ok(0);
        }
        self.run(move |connection| {
            let deleted = connection
                .execute(
                    "DELETE FROM inbound_events
                     WHERE rowid IN (
                         SELECT rowid FROM inbound_events
                         WHERE state IN ('completed', 'rejected') AND updated_ms < ?1
                         ORDER BY updated_ms, tenant, event_id LIMIT ?2
                     )",
                    params![older_than_ms, max_rows],
                )
                .map_err(|error| sqlite_error("sweeping inbound events", &error))?;
            Ok(u64::try_from(deleted).unwrap_or(u64::MAX))
        })
        .await
    }
}

#[derive(Clone)]
struct StoredInbound {
    event_id: String,
    message_id: String,
    scope_key: String,
    state: InboundEventState,
    payload_version: Option<i64>,
    payload_blob: Option<Vec<u8>>,
    payload_bytes: i64,
    turn_row_id: Option<i64>,
    rejection_reason: Option<String>,
    reply_outbox_key: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InboundPayloadV1 {
    event_id: String,
    message_id: String,
    chat_id: String,
    sender_id: String,
    chat_type: ChatModeWire,
    thread_id: Option<String>,
    root_id: Option<String>,
    reply_to_message_id: Option<String>,
    text: String,
    mentions_bot: bool,
    mention_all: bool,
    /// Whether the wire sender is a human user. Defaults to `false` (fail
    /// closed) for legacy payloads recorded before sender discrimination.
    #[serde(default)]
    sender_is_human: bool,
    #[serde(default)]
    mentions: Vec<MentionIdentity>,
    #[serde(default)]
    parts: Vec<MessagePart>,
    resources: Vec<ResourceWire>,
    message_type: String,
    create_time_ms: i64,
    scope: ScopeWire,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ChatModeWire {
    P2p,
    Group,
    Topic,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceWire {
    kind: ResourceKindWire,
    key: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ResourceKindWire {
    Image,
    File,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopeWire {
    kind: ScopeKindWire,
    chat_id: String,
    thread_id: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ScopeKindWire {
    Chat,
    Thread,
}

fn validate_incoming_key(event: &InboundEvent) -> Result<(), StoreError> {
    validate_id(&event.event_id, "validating an inbound event ID")?;
    validate_id(&event.message_id, "validating an inbound message ID")
}

fn inbound_event_variable_bytes(event: &InboundEvent) -> usize {
    let scope_bytes = match &event.scope {
        ScopeKey::Chat(chat_id) => chat_id.len(),
        ScopeKey::Thread(chat_id, thread_id) => chat_id.len().saturating_add(thread_id.len()),
    };
    [
        event.event_id.len(),
        event.message_id.len(),
        event.chat_id.len(),
        event.sender_id.len(),
        event.thread_id.as_deref().map_or(0, str::len),
        event.root_id.as_deref().map_or(0, str::len),
        event.reply_to_message_id.as_deref().map_or(0, str::len),
        event.text.len(),
        event.message_type.len(),
        scope_bytes,
        event
            .resources
            .iter()
            .map(|resource| resource.key.len())
            .sum(),
        event.mentions.iter().map(mention_variable_bytes).sum(),
        event.parts.iter().map(part_variable_bytes).sum(),
    ]
    .into_iter()
    .fold(0_usize, usize::saturating_add)
}

fn validate_id(value: &str, context: &'static str) -> Result<(), StoreError> {
    if value.is_empty() || value.len() > STORE_INBOUND_ID_MAX_BYTES {
        return Err(StoreError::PayloadTooLarge {
            context,
            limit: u64::try_from(STORE_INBOUND_ID_MAX_BYTES).unwrap_or(u64::MAX),
        });
    }
    Ok(())
}

fn validate_optional_id(value: Option<&str>, context: &'static str) -> Result<(), StoreError> {
    if let Some(value) = value {
        validate_id(value, context)?;
    }
    Ok(())
}

fn encode_event(event: &InboundEvent) -> Result<Vec<u8>, StoreError> {
    validate_event(event)?;
    let persistable = persistable_event(event);
    let dto = InboundPayloadV1::from_event(&persistable);
    let payload = serde_json::to_vec(&dto).map_err(|_| StoreError::CorruptData {
        context: "encoding an inbound payload",
    })?;
    if payload.len() > STORE_INBOUND_PAYLOAD_MAX_BYTES {
        return Err(StoreError::PayloadTooLarge {
            context: "encoding an inbound payload",
            limit: u64::try_from(STORE_INBOUND_PAYLOAD_MAX_BYTES).unwrap_or(u64::MAX),
        });
    }
    Ok(payload)
}

/// Produces the crash-replay descriptor. Resource keys are bearer-like
/// capabilities, so they are never handed to SQLite (including its WAL).
/// Live transcripts travel outside [`InboundEvent`] entirely. A restarted
/// bridge can still settle and route the message, but media is explicitly
/// unavailable until Lark redelivers the authenticated live event.
fn persistable_event(event: &InboundEvent) -> InboundEvent {
    let mut persisted = event.clone();
    persisted.resources.clear();
    for part in &mut persisted.parts {
        let media = match part {
            MessagePart::Image(media)
            | MessagePart::File(media)
            | MessagePart::Sticker(media)
            | MessagePart::Audio(media)
            | MessagePart::Video(media) => media,
            MessagePart::Text { .. }
            | MessagePart::Forward { .. }
            | MessagePart::Card { .. }
            | MessagePart::Unsupported { .. } => continue,
        };
        media.key = None;
        media.thumbnail_key = None;
        if media.status == PartStatus::Available {
            media.status = PartStatus::Unavailable;
        }
    }
    persisted
}

fn validate_live_claim(live: &InboundEvent, persisted: &InboundEvent) -> Result<(), StoreError> {
    // Bind the in-memory capability-bearing object to every identity and
    // policy-relevant field retained durably. Media keys and the resulting
    // availability status are intentionally excluded. Transcript content is
    // structurally absent from both event values.
    if live.event_id != persisted.event_id
        || live.message_id != persisted.message_id
        || live.chat_id != persisted.chat_id
        || live.sender_id != persisted.sender_id
        || live.chat_type != persisted.chat_type
        || live.thread_id != persisted.thread_id
        || live.root_id != persisted.root_id
        || live.reply_to_message_id != persisted.reply_to_message_id
        || live.text != persisted.text
        || live.mentions_bot != persisted.mentions_bot
        || live.mention_all != persisted.mention_all
        || live.sender_is_human != persisted.sender_is_human
        || live.mentions != persisted.mentions
        || live.message_type != persisted.message_type
        || live.create_time_ms != persisted.create_time_ms
        || live.scope != persisted.scope
    {
        return Err(StoreError::CorruptData {
            context: "binding a live inbound event to its durable descriptor",
        });
    }
    let live_parts = if live.parts.is_empty() {
        parts_from_v1(&live.message_type, &live.text, &live.resources)
    } else {
        live.parts.clone()
    };
    if live_parts.len() != persisted.parts.len() {
        return Err(StoreError::CorruptData {
            context: "binding live inbound part count",
        });
    }
    for (live_part, persisted_part) in live_parts.iter().zip(&persisted.parts) {
        let same_shape = match (live_part, persisted_part) {
            (MessagePart::Text { text: left }, MessagePart::Text { text: right }) => left == right,
            (MessagePart::Image(left), MessagePart::Image(right))
            | (MessagePart::File(left), MessagePart::File(right))
            | (MessagePart::Sticker(left), MessagePart::Sticker(right))
            | (MessagePart::Audio(left), MessagePart::Audio(right))
            | (MessagePart::Video(left), MessagePart::Video(right)) => {
                same_persisted_media_metadata(left, right)
            }
            (
                MessagePart::Forward {
                    message_id: left_id,
                    status: left_status,
                },
                MessagePart::Forward {
                    message_id: right_id,
                    status: right_status,
                },
            ) => left_id == right_id && left_status == right_status,
            (
                MessagePart::Card {
                    status: left_status,
                },
                MessagePart::Card {
                    status: right_status,
                },
            ) => left_status == right_status,
            (
                MessagePart::Unsupported {
                    message_type: left_type,
                    status: left_status,
                },
                MessagePart::Unsupported {
                    message_type: right_type,
                    status: right_status,
                },
            ) => left_type == right_type && left_status == right_status,
            _ => false,
        };
        if !same_shape {
            return Err(StoreError::CorruptData {
                context: "binding live inbound part shapes",
            });
        }
    }
    Ok(())
}

fn same_persisted_media_metadata(left: &MediaPart, right: &MediaPart) -> bool {
    let expected_status = if left.status == PartStatus::Available {
        PartStatus::Unavailable
    } else {
        left.status
    };
    right.key.is_none()
        && right.thumbnail_key.is_none()
        && right.status == expected_status
        && left.metadata.file_name == right.metadata.file_name
        && left.metadata.mime_type == right.metadata.mime_type
        && left.metadata.size_bytes == right.metadata.size_bytes
        && left.metadata.duration_ms == right.metadata.duration_ms
        && left.metadata.transcript_failure == right.metadata.transcript_failure
}

#[allow(clippy::too_many_lines)]
fn validate_event(event: &InboundEvent) -> Result<(), StoreError> {
    validate_incoming_key(event)?;
    validate_id(&event.chat_id, "validating an inbound chat ID")?;
    validate_id(&event.sender_id, "validating an inbound sender ID")?;
    validate_optional_id(
        event.thread_id.as_deref(),
        "validating an inbound thread ID",
    )?;
    validate_optional_id(event.root_id.as_deref(), "validating an inbound root ID")?;
    validate_optional_id(
        event.reply_to_message_id.as_deref(),
        "validating an inbound reply ID",
    )?;
    let scope_key = event.scope.to_string();
    if scope_key.len() > STORE_INBOUND_SCOPE_MAX_BYTES {
        return Err(StoreError::PayloadTooLarge {
            context: "validating an inbound scope",
            limit: u64::try_from(STORE_INBOUND_SCOPE_MAX_BYTES).unwrap_or(u64::MAX),
        });
    }
    match &event.scope {
        ScopeKey::Chat(chat_id) => {
            if chat_id != &event.chat_id || event.thread_id.is_some() {
                return Err(StoreError::CorruptData {
                    context: "validating inbound chat scope consistency",
                });
            }
        }
        ScopeKey::Thread(chat_id, thread_id) => {
            if chat_id != &event.chat_id
                || event.thread_id.as_deref() != Some(thread_id)
                || event.chat_type != ChatMode::Topic
            {
                return Err(StoreError::CorruptData {
                    context: "validating inbound thread scope consistency",
                });
            }
        }
    }
    if event.message_type.len() > STORE_INBOUND_MESSAGE_TYPE_MAX_BYTES {
        return Err(StoreError::PayloadTooLarge {
            context: "validating an inbound message type",
            limit: u64::try_from(STORE_INBOUND_MESSAGE_TYPE_MAX_BYTES).unwrap_or(u64::MAX),
        });
    }
    if event.text.len() > STORE_INBOUND_TEXT_MAX_BYTES {
        return Err(StoreError::PayloadTooLarge {
            context: "validating inbound text",
            limit: u64::try_from(STORE_INBOUND_TEXT_MAX_BYTES).unwrap_or(u64::MAX),
        });
    }
    if event.resources.len() > STORE_INBOUND_RESOURCE_MAX_COUNT {
        return Err(StoreError::CapacityExceeded {
            context: "validating inbound resource count",
        });
    }
    let mut resource_bytes = 0_usize;
    for resource in &event.resources {
        if resource.key.is_empty() || resource.key.len() > STORE_INBOUND_RESOURCE_KEY_MAX_BYTES {
            return Err(StoreError::PayloadTooLarge {
                context: "validating an inbound resource key",
                limit: u64::try_from(STORE_INBOUND_RESOURCE_KEY_MAX_BYTES).unwrap_or(u64::MAX),
            });
        }
        resource_bytes = resource_bytes.saturating_add(resource.key.len());
    }
    if resource_bytes > STORE_INBOUND_RESOURCE_KEY_MAX_TOTAL_BYTES {
        return Err(StoreError::CapacityExceeded {
            context: "validating aggregate inbound resource keys",
        });
    }
    if event.mentions.len() > STORE_INBOUND_RESOURCE_MAX_COUNT {
        return Err(StoreError::CapacityExceeded {
            context: "validating inbound mention count",
        });
    }
    for mention in &event.mentions {
        for value in [
            mention.key.as_deref(),
            mention.open_id.as_deref(),
            mention.user_id.as_deref(),
            mention.union_id.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if value.len() > STORE_INBOUND_ID_MAX_BYTES {
                return Err(StoreError::PayloadTooLarge {
                    context: "validating an inbound mention identity",
                    limit: u64::try_from(STORE_INBOUND_ID_MAX_BYTES).unwrap_or(u64::MAX),
                });
            }
        }
        if mention
            .name
            .as_deref()
            .is_some_and(|name| name.len() > STORE_INBOUND_TEXT_MAX_BYTES)
        {
            return Err(StoreError::PayloadTooLarge {
                context: "validating an inbound mention name",
                limit: u64::try_from(STORE_INBOUND_TEXT_MAX_BYTES).unwrap_or(u64::MAX),
            });
        }
    }
    if event.parts.len() > STORE_INBOUND_RESOURCE_MAX_COUNT {
        return Err(StoreError::CapacityExceeded {
            context: "validating inbound part count",
        });
    }
    for part in &event.parts {
        validate_part(part)?;
    }
    Ok(())
}

fn validate_part(part: &MessagePart) -> Result<(), StoreError> {
    match part {
        MessagePart::Text { text } => {
            if text.len() > STORE_INBOUND_TEXT_MAX_BYTES {
                return Err(StoreError::PayloadTooLarge {
                    context: "validating an inbound text part",
                    limit: u64::try_from(STORE_INBOUND_TEXT_MAX_BYTES).unwrap_or(u64::MAX),
                });
            }
        }
        MessagePart::Image(media)
        | MessagePart::File(media)
        | MessagePart::Sticker(media)
        | MessagePart::Audio(media)
        | MessagePart::Video(media) => validate_media_part(media)?,
        MessagePart::Forward { message_id, .. } => validate_optional_id(
            message_id.as_deref(),
            "validating an inbound forwarded message ID",
        )?,
        MessagePart::Card { .. } => {}
        MessagePart::Unsupported { message_type, .. } => {
            if message_type.is_empty() || message_type.len() > STORE_INBOUND_MESSAGE_TYPE_MAX_BYTES
            {
                return Err(StoreError::PayloadTooLarge {
                    context: "validating an unsupported inbound message type",
                    limit: u64::try_from(STORE_INBOUND_MESSAGE_TYPE_MAX_BYTES).unwrap_or(u64::MAX),
                });
            }
        }
    }
    Ok(())
}

fn validate_media_part(media: &MediaPart) -> Result<(), StoreError> {
    for key in [media.key.as_deref(), media.thumbnail_key.as_deref()]
        .into_iter()
        .flatten()
    {
        if key.is_empty() || key.len() > STORE_INBOUND_RESOURCE_KEY_MAX_BYTES {
            return Err(StoreError::PayloadTooLarge {
                context: "validating an inbound media key",
                limit: u64::try_from(STORE_INBOUND_RESOURCE_KEY_MAX_BYTES).unwrap_or(u64::MAX),
            });
        }
    }
    if media.status == PartStatus::Available && media.key.is_none() {
        return Err(StoreError::CorruptData {
            context: "validating an available inbound media part",
        });
    }
    if let Some(name) = media.metadata.file_name.as_deref() {
        if name.is_empty()
            || name.len() > ATTACHMENT_FILE_NAME_MAX_BYTES
            || matches!(name, "." | "..")
            || name
                .bytes()
                .any(|byte| byte.is_ascii_control() || matches!(byte, b'/' | b'\\'))
        {
            return Err(StoreError::CorruptData {
                context: "validating inbound media file name metadata",
            });
        }
    }
    if let Some(mime) = media.metadata.mime_type.as_deref() {
        if mime.is_empty()
            || mime.len() > ATTACHMENT_MIME_MAX_BYTES
            || !mime.bytes().all(|byte| (0x20..=0x7e).contains(&byte))
        {
            return Err(StoreError::CorruptData {
                context: "validating inbound media MIME metadata",
            });
        }
    }
    Ok(())
}

fn mention_variable_bytes(mention: &MentionIdentity) -> usize {
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
    .fold(0_usize, usize::saturating_add)
}

fn metadata_variable_bytes(metadata: &MediaMetadata) -> usize {
    metadata
        .file_name
        .as_deref()
        .map_or(0, str::len)
        .saturating_add(metadata.mime_type.as_deref().map_or(0, str::len))
}

fn part_variable_bytes(part: &MessagePart) -> usize {
    match part {
        MessagePart::Text { text } => text.len(),
        MessagePart::Image(media)
        | MessagePart::File(media)
        | MessagePart::Sticker(media)
        | MessagePart::Audio(media)
        | MessagePart::Video(media) => media
            .key
            .as_deref()
            .map_or(0, str::len)
            .saturating_add(media.thumbnail_key.as_deref().map_or(0, str::len))
            .saturating_add(metadata_variable_bytes(&media.metadata)),
        MessagePart::Forward { message_id, .. } => message_id.as_deref().map_or(0, str::len),
        MessagePart::Card { .. } => 0,
        MessagePart::Unsupported { message_type, .. } => message_type.len(),
    }
}

impl InboundPayloadV1 {
    fn from_event(event: &InboundEvent) -> Self {
        let chat_type = match event.chat_type {
            ChatMode::P2p => ChatModeWire::P2p,
            ChatMode::Group => ChatModeWire::Group,
            ChatMode::Topic => ChatModeWire::Topic,
        };
        let resources = event
            .resources
            .iter()
            .map(|resource| ResourceWire {
                kind: match resource.kind {
                    ResourceKind::Image => ResourceKindWire::Image,
                    ResourceKind::File => ResourceKindWire::File,
                },
                key: resource.key.clone(),
            })
            .collect();
        let scope = match &event.scope {
            ScopeKey::Chat(chat_id) => ScopeWire {
                kind: ScopeKindWire::Chat,
                chat_id: chat_id.clone(),
                thread_id: None,
            },
            ScopeKey::Thread(chat_id, thread_id) => ScopeWire {
                kind: ScopeKindWire::Thread,
                chat_id: chat_id.clone(),
                thread_id: Some(thread_id.clone()),
            },
        };
        Self {
            event_id: event.event_id.clone(),
            message_id: event.message_id.clone(),
            chat_id: event.chat_id.clone(),
            sender_id: event.sender_id.clone(),
            chat_type,
            thread_id: event.thread_id.clone(),
            root_id: event.root_id.clone(),
            reply_to_message_id: event.reply_to_message_id.clone(),
            text: event.text.clone(),
            mentions_bot: event.mentions_bot,
            mention_all: event.mention_all,
            sender_is_human: event.sender_is_human,
            mentions: event.mentions.clone(),
            parts: event.parts.clone(),
            resources,
            message_type: event.message_type.clone(),
            create_time_ms: event.create_time_ms,
            scope,
        }
    }

    fn into_event(self) -> Result<InboundEvent, StoreError> {
        let scope = match (self.scope.kind, self.scope.thread_id) {
            (ScopeKindWire::Chat, None) => ScopeKey::Chat(self.scope.chat_id),
            (ScopeKindWire::Thread, Some(thread_id)) => {
                ScopeKey::Thread(self.scope.chat_id, thread_id)
            }
            _ => {
                return Err(StoreError::CorruptData {
                    context: "decoding an inbound scope",
                });
            }
        };
        let resources: Vec<ResourceDesc> = self
            .resources
            .into_iter()
            .map(|resource| ResourceDesc {
                kind: match resource.kind {
                    ResourceKindWire::Image => ResourceKind::Image,
                    ResourceKindWire::File => ResourceKind::File,
                },
                key: resource.key,
            })
            .collect();
        let parts = if self.parts.is_empty() {
            parts_from_v1(&self.message_type, &self.text, &resources)
        } else {
            self.parts
        };
        let event = InboundEvent {
            event_id: self.event_id,
            message_id: self.message_id,
            chat_id: self.chat_id,
            sender_id: self.sender_id,
            chat_type: match self.chat_type {
                ChatModeWire::P2p => ChatMode::P2p,
                ChatModeWire::Group => ChatMode::Group,
                ChatModeWire::Topic => ChatMode::Topic,
            },
            thread_id: self.thread_id,
            root_id: self.root_id,
            reply_to_message_id: self.reply_to_message_id,
            text: self.text,
            mentions_bot: self.mentions_bot,
            mention_all: self.mention_all,
            sender_is_human: self.sender_is_human,
            mentions: self.mentions,
            parts,
            resources,
            message_type: self.message_type,
            create_time_ms: self.create_time_ms,
            scope,
        };
        validate_event(&event)?;
        Ok(event)
    }
}

fn parts_from_v1(message_type: &str, text: &str, resources: &[ResourceDesc]) -> Vec<MessagePart> {
    match message_type {
        "text" => vec![MessagePart::Text {
            text: text.to_owned(),
        }],
        "image" => vec![MessagePart::Image(legacy_media_part(
            resources,
            ResourceKind::Image,
        ))],
        "file" => vec![MessagePart::File(legacy_media_part(
            resources,
            ResourceKind::File,
        ))],
        _ => vec![MessagePart::Unsupported {
            message_type: message_type.to_owned(),
            status: PartStatus::Unsupported,
        }],
    }
}

fn legacy_media_part(resources: &[ResourceDesc], kind: ResourceKind) -> MediaPart {
    let key = resources
        .iter()
        .find(|resource| resource.kind == kind)
        .map(|resource| resource.key.clone());
    MediaPart {
        status: if key.is_some() {
            PartStatus::Available
        } else {
            PartStatus::Unavailable
        },
        key,
        thumbnail_key: None,
        metadata: MediaMetadata::default(),
    }
}

fn read_inbound_row(
    connection: &rusqlite::Connection,
    tenant: &str,
    event_id: &str,
) -> Result<Option<StoredInbound>, StoreError> {
    connection
        .query_row(
            "SELECT event_id, message_id, scope_key, state, payload_version,
                    payload_blob, payload_bytes, turn_row_id, rejection_reason,
                    reply_outbox_key
             FROM inbound_events WHERE tenant = ?1 AND event_id = ?2",
            params![tenant, event_id],
            decode_stored_row_with_reply_effect,
        )
        .optional()
        .map_err(|error| sqlite_error("reading an inbound row", &error))
}

fn read_inbound_reply_effect(
    connection: &rusqlite::Connection,
    tenant: &str,
    event_id: &str,
) -> Result<Option<String>, StoreError> {
    connection
        .query_row(
            "SELECT reply_outbox_key FROM inbound_events
             WHERE tenant = ?1 AND event_id = ?2",
            params![tenant, event_id],
            |row| row.get(0),
        )
        .map_err(|error| sqlite_error("reading an inbound reply effect marker", &error))
}

fn record_inbound_reply_effect(
    transaction: &rusqlite::Transaction<'_>,
    tenant: &str,
    event_id: &str,
    idempotency_key: &str,
) -> Result<(), StoreError> {
    let logical_bytes: i64 = transaction
        .query_row(
            "SELECT COALESCE(SUM(
                 LENGTH(CAST(tenant AS BLOB)) + LENGTH(CAST(event_id AS BLOB)) +
                 LENGTH(CAST(message_id AS BLOB)) + LENGTH(CAST(scope_key AS BLOB)) +
                 COALESCE(LENGTH(CAST(rejection_reason AS BLOB)), 0) +
                 COALESCE(LENGTH(CAST(reply_outbox_key AS BLOB)), 0) + payload_bytes
             ), 0)
             FROM inbound_events",
            [],
            |row| row.get(0),
        )
        .map_err(|error| sqlite_error("checking inbound reply effect capacity", &error))?;
    if u64::try_from(logical_bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(u64::try_from(idempotency_key.len()).unwrap_or(u64::MAX))
        > STORE_INBOUND_MAX_BYTES
    {
        return Err(StoreError::CapacityExceeded {
            context: "recording an inbound reply effect marker",
        });
    }
    let changed = transaction
        .execute(
            "UPDATE inbound_events
             SET reply_outbox_key = ?3, updated_ms = ?4
             WHERE tenant = ?1 AND event_id = ?2 AND reply_outbox_key IS NULL",
            params![tenant, event_id, idempotency_key, now_ms()],
        )
        .map_err(|error| sqlite_error("recording an inbound reply effect marker", &error))?;
    if changed != 1 {
        return Err(StoreError::CorruptData {
            context: "recording an inbound reply effect marker",
        });
    }
    Ok(())
}

fn read_message_candidates(
    connection: &rusqlite::Connection,
    tenant: &str,
    message_id: &str,
) -> Result<Vec<StoredInbound>, StoreError> {
    let mut statement = connection
        .prepare(
            "SELECT event_id, message_id, scope_key, state, payload_version,
                    payload_blob, payload_bytes, turn_row_id, rejection_reason,
                    reply_outbox_key
             FROM inbound_events
             WHERE tenant = ?1 AND message_id = ?2 AND state != 'rejected'
             ORDER BY event_id",
        )
        .map_err(|error| sqlite_error("reading inbound message candidates", &error))?;
    statement
        .query_map(
            params![tenant, message_id],
            decode_stored_row_with_reply_effect,
        )
        .map_err(|error| sqlite_error("reading inbound message candidates", &error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| sqlite_error("decoding inbound message candidates", &error))
}

fn decode_stored_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredInbound> {
    let state: String = row.get(3)?;
    let Some(state) = InboundEventState::parse(&state) else {
        return Err(rusqlite::Error::InvalidColumnType(
            3,
            "state".to_owned(),
            rusqlite::types::Type::Text,
        ));
    };
    Ok(StoredInbound {
        event_id: row.get(0)?,
        message_id: row.get(1)?,
        scope_key: row.get(2)?,
        state,
        payload_version: row.get(4)?,
        payload_blob: row.get(5)?,
        payload_bytes: row.get(6)?,
        turn_row_id: row.get(7)?,
        rejection_reason: row.get(8)?,
        reply_outbox_key: None,
    })
}

fn decode_stored_row_with_reply_effect(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredInbound> {
    let mut stored = decode_stored_row(row)?;
    stored.reply_outbox_key = row.get(9)?;
    Ok(stored)
}

fn registration_from_stored(
    stored: StoredInbound,
    tenant: TenantNamespace,
    exact: bool,
    live: Option<&InboundEvent>,
) -> Result<DedupOutcome, StoreError> {
    let key = InboundKey::new(tenant, stored.event_id.clone());
    if stored.state == InboundEventState::Received {
        let persisted = retained_from_stored(stored)?;
        let retained = if exact
            && live.is_some_and(|event| {
                validate_event(event).is_ok()
                    && validate_live_claim(event, persisted.event()).is_ok()
            }) {
            RetainedInbound::new(
                Box::new(live.expect("checked live event").clone()),
                persisted.retained_bytes(),
            )
        } else {
            persisted
        };
        return Ok(DedupOutcome::ReplayReceived(retained));
    }
    if stored.state == InboundEventState::Accepted && stored.turn_row_id.is_none() {
        return Err(StoreError::CorruptData {
            context: "validating an accepted inbound association",
        });
    }
    let _ = exact;
    Ok(DedupOutcome::Duplicate {
        key,
        state: stored.state,
        turn_row_id: stored.turn_row_id,
    })
}

fn retained_from_stored(stored: StoredInbound) -> Result<RetainedInbound, StoreError> {
    if stored.state != InboundEventState::Received && stored.state != InboundEventState::Accepted {
        return Err(StoreError::CorruptData {
            context: "decoding a terminal inbound payload",
        });
    }
    let payload_version = stored.payload_version.ok_or(StoreError::CorruptData {
        context: "decoding an inbound payload version",
    })?;
    if payload_version != INBOUND_PAYLOAD_VERSION || stored.payload_bytes < 0 {
        return Err(StoreError::CorruptData {
            context: "decoding an inbound payload version",
        });
    }
    let payload = stored.payload_blob.ok_or(StoreError::CorruptData {
        context: "decoding a missing inbound payload",
    })?;
    if usize::try_from(stored.payload_bytes).ok() != Some(payload.len())
        || payload.len() > STORE_INBOUND_PAYLOAD_MAX_BYTES
    {
        return Err(StoreError::CorruptData {
            context: "validating an inbound payload length",
        });
    }
    let event = serde_json::from_slice::<InboundPayloadV1>(&payload)
        .map_err(|_| StoreError::CorruptData {
            context: "decoding a strict inbound payload v1",
        })?
        .into_event()?;
    if event.event_id != stored.event_id
        || event.message_id != stored.message_id
        || event.scope.to_string() != stored.scope_key
    {
        return Err(StoreError::CorruptData {
            context: "validating inbound row and payload identity",
        });
    }
    Ok(RetainedInbound::new(Box::new(event), payload.len()))
}

/// Rewrites pre-v6 replay payloads so an upgrade cannot leave Lark resource
/// capabilities or transcripts in live SQLite rows. The writer follows this
/// with a WAL truncation and `VACUUM` to remove historical page images too.
pub(crate) fn scrub_persisted_inbound_secrets(
    connection: &mut rusqlite::Connection,
) -> Result<(), StoreError> {
    let transaction = connection
        .transaction()
        .map_err(|error| sqlite_error("starting inbound privacy migration", &error))?;
    let rows = {
        let mut statement = transaction
            .prepare(
                "SELECT event_id, message_id, scope_key, state, payload_version,
                        payload_blob, payload_bytes, turn_row_id, rejection_reason, tenant
                 FROM inbound_events
                 WHERE state IN ('received', 'accepted')
                 ORDER BY tenant, event_id",
            )
            .map_err(|error| sqlite_error("preparing inbound privacy migration", &error))?;
        statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(9)?, decode_stored_row(row)?))
            })
            .map_err(|error| sqlite_error("reading inbound privacy migration", &error))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| sqlite_error("decoding inbound privacy migration", &error))?
    };
    for (tenant, mut stored) in rows {
        let state = stored.state;
        let turn_row_id = stored.turn_row_id;
        let event_id = stored.event_id.clone();
        scrub_legacy_inbound_payload(&mut stored)?;
        let old = retained_from_stored(stored)?;
        let payload = encode_event(old.event())?;
        let payload_bytes = i64::try_from(payload.len()).unwrap_or(i64::MAX);
        let changed = transaction
            .execute(
                "UPDATE inbound_events
                 SET payload_blob = ?1, payload_bytes = ?2
                 WHERE tenant = ?3 AND event_id = ?4 AND state = ?5 AND turn_row_id IS ?6",
                params![
                    payload,
                    payload_bytes,
                    tenant,
                    event_id,
                    state.as_str(),
                    turn_row_id,
                ],
            )
            .map_err(|error| sqlite_error("rewriting inbound privacy payload", &error))?;
        if changed != 1 {
            return Err(StoreError::CorruptData {
                context: "rewriting one inbound privacy payload",
            });
        }
    }
    transaction
        .commit()
        .map_err(|error| sqlite_error("committing inbound privacy migration", &error))
}

/// Removes the two pre-v6 secret-bearing fields before strict typed decoding.
/// This compatibility decoder exists only on the one-way v5 -> v6 migration
/// path; normal reads continue to reject unknown metadata fields.
fn scrub_legacy_inbound_payload(stored: &mut StoredInbound) -> Result<(), StoreError> {
    let payload = stored
        .payload_blob
        .as_ref()
        .ok_or(StoreError::CorruptData {
            context: "scrubbing a missing inbound privacy payload",
        })?;
    if payload.len() > STORE_INBOUND_PAYLOAD_MAX_BYTES
        || usize::try_from(stored.payload_bytes).ok() != Some(payload.len())
    {
        return Err(StoreError::CorruptData {
            context: "scrubbing an invalid inbound privacy payload length",
        });
    }
    let mut value = serde_json::from_slice::<serde_json::Value>(payload).map_err(|_| {
        StoreError::CorruptData {
            context: "decoding an inbound privacy payload",
        }
    })?;
    let root = value.as_object_mut().ok_or(StoreError::CorruptData {
        context: "scrubbing an inbound privacy payload object",
    })?;
    root.insert("resources".to_owned(), serde_json::Value::Array(Vec::new()));
    let parts = root
        .get_mut("parts")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or(StoreError::CorruptData {
            context: "scrubbing inbound privacy payload parts",
        })?;
    for part in parts {
        let Some(part) = part.as_object_mut() else {
            continue;
        };
        let is_media = part
            .get("kind")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|kind| matches!(kind, "image" | "file" | "sticker" | "audio" | "video"));
        if !is_media {
            continue;
        }
        let Some(media) = part
            .get_mut("value")
            .and_then(serde_json::Value::as_object_mut)
        else {
            continue;
        };
        media.insert("key".to_owned(), serde_json::Value::Null);
        media.insert("thumbnail_key".to_owned(), serde_json::Value::Null);
        if media.get("status").and_then(serde_json::Value::as_str) == Some("available") {
            media.insert(
                "status".to_owned(),
                serde_json::Value::String("unavailable".to_owned()),
            );
        }
        if let Some(metadata) = media
            .get_mut("metadata")
            .and_then(serde_json::Value::as_object_mut)
        {
            if metadata.remove("transcript").is_some()
                && !metadata.contains_key("transcript_failure")
            {
                metadata.insert(
                    "transcript_failure".to_owned(),
                    serde_json::Value::String("not_retained".to_owned()),
                );
            }
        }
    }
    let payload = serde_json::to_vec(&value).map_err(|_| StoreError::CorruptData {
        context: "encoding a scrubbed inbound privacy payload",
    })?;
    stored.payload_bytes = i64::try_from(payload.len()).unwrap_or(i64::MAX);
    stored.payload_blob = Some(payload);
    Ok(())
}

fn ensure_inbound_capacity(
    connection: &rusqlite::Connection,
    additional_logical_bytes: usize,
    additional_payload_bytes: usize,
) -> Result<(), StoreError> {
    let (rows, logical_bytes, received_rows, received_bytes): (i64, i64, i64, i64) = connection
        .query_row(
            "SELECT COUNT(*), COALESCE(SUM(
                 LENGTH(CAST(tenant AS BLOB)) + LENGTH(CAST(event_id AS BLOB)) +
                 LENGTH(CAST(message_id AS BLOB)) + LENGTH(CAST(scope_key AS BLOB)) +
                 COALESCE(LENGTH(CAST(rejection_reason AS BLOB)), 0) +
                 COALESCE(LENGTH(CAST(reply_outbox_key AS BLOB)), 0) + payload_bytes
             ), 0),
             COALESCE(SUM(CASE WHEN state = 'received' THEN 1 ELSE 0 END), 0),
             COALESCE(SUM(CASE WHEN state = 'received' THEN payload_bytes ELSE 0 END), 0)
             FROM inbound_events",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .map_err(|error| sqlite_error("checking inbound capacity", &error))?;
    let rows = u64::try_from(rows).unwrap_or(u64::MAX);
    let logical_bytes = u64::try_from(logical_bytes).unwrap_or(u64::MAX);
    let received_rows = u64::try_from(received_rows).unwrap_or(u64::MAX);
    let received_bytes = u64::try_from(received_bytes).unwrap_or(u64::MAX);
    if rows >= STORE_INBOUND_MAX_ROWS
        || logical_bytes.saturating_add(u64::try_from(additional_logical_bytes).unwrap_or(u64::MAX))
            > STORE_INBOUND_MAX_BYTES
        || received_rows >= STORE_INBOUND_RECEIVED_MAX_ROWS
        || received_bytes
            .saturating_add(u64::try_from(additional_payload_bytes).unwrap_or(u64::MAX))
            > STORE_INBOUND_RECEIVED_MAX_BYTES
    {
        return Err(StoreError::CapacityExceeded {
            context: "registering an inbound event",
        });
    }
    Ok(())
}

fn validate_inbound_collection(connection: &rusqlite::Connection) -> Result<(), StoreError> {
    let (rows, logical_bytes, received_rows, received_bytes): (i64, i64, i64, i64) = connection
        .query_row(
            "SELECT COUNT(*), COALESCE(SUM(
                 LENGTH(CAST(tenant AS BLOB)) + LENGTH(CAST(event_id AS BLOB)) +
                 LENGTH(CAST(message_id AS BLOB)) + LENGTH(CAST(scope_key AS BLOB)) +
                 COALESCE(LENGTH(CAST(rejection_reason AS BLOB)), 0) +
                 COALESCE(LENGTH(CAST(reply_outbox_key AS BLOB)), 0) + payload_bytes
             ), 0),
             COALESCE(SUM(CASE WHEN state = 'received' THEN 1 ELSE 0 END), 0),
             COALESCE(SUM(CASE WHEN state = 'received' THEN payload_bytes ELSE 0 END), 0)
             FROM inbound_events",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .map_err(|error| sqlite_error("validating inbound collection bounds", &error))?;
    if u64::try_from(rows).unwrap_or(u64::MAX) > STORE_INBOUND_MAX_ROWS
        || u64::try_from(logical_bytes).unwrap_or(u64::MAX) > STORE_INBOUND_MAX_BYTES
        || u64::try_from(received_rows).unwrap_or(u64::MAX) > STORE_INBOUND_RECEIVED_MAX_ROWS
        || u64::try_from(received_bytes).unwrap_or(u64::MAX) > STORE_INBOUND_RECEIVED_MAX_BYTES
    {
        return Err(StoreError::CapacityExceeded {
            context: "recovering the inbound collection",
        });
    }
    let conflicting: bool = connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM inbound_events WHERE state != 'rejected'
                 GROUP BY tenant, message_id HAVING COUNT(*) > 1
             )",
            [],
            |row| row.get(0),
        )
        .map_err(|error| sqlite_error("checking canonical inbound messages", &error))?;
    if conflicting {
        return Err(StoreError::CorruptData {
            context: "validating canonical inbound messages",
        });
    }
    let mut statement = connection
        .prepare(
            "SELECT tenant, event_id, message_id, scope_key, state, payload_version,
                    payload_blob, payload_bytes, turn_row_id, rejection_reason,
                    reply_outbox_key
             FROM inbound_events ORDER BY tenant, event_id",
        )
        .map_err(|error| sqlite_error("preparing inbound integrity scan", &error))?;
    let rows = statement
        .query_map([], |row| {
            let tenant: String = row.get(0)?;
            let state: String = row.get(4)?;
            let Some(state) = InboundEventState::parse(&state) else {
                return Err(rusqlite::Error::InvalidColumnType(
                    4,
                    "state".to_owned(),
                    rusqlite::types::Type::Text,
                ));
            };
            Ok((
                tenant,
                StoredInbound {
                    event_id: row.get(1)?,
                    message_id: row.get(2)?,
                    scope_key: row.get(3)?,
                    state,
                    payload_version: row.get(5)?,
                    payload_blob: row.get(6)?,
                    payload_bytes: row.get(7)?,
                    turn_row_id: row.get(8)?,
                    rejection_reason: row.get(9)?,
                    reply_outbox_key: row.get(10)?,
                },
            ))
        })
        .map_err(|error| sqlite_error("reading inbound integrity scan", &error))?;
    for row in rows {
        let (tenant, stored) =
            row.map_err(|error| sqlite_error("decoding inbound integrity scan", &error))?;
        validate_stored_inbound_row(connection, &tenant, &stored)?;
    }
    validate_runtime_turn_groups(connection)?;
    Ok(())
}

fn validate_stored_inbound_row(
    connection: &rusqlite::Connection,
    tenant: &str,
    stored: &StoredInbound,
) -> Result<(), StoreError> {
    validate_nonempty_bounded(
        tenant,
        STORE_INBOUND_ID_MAX_BYTES,
        "validating an inbound tenant",
    )?;
    validate_nonempty_bounded(
        &stored.event_id,
        STORE_INBOUND_ID_MAX_BYTES,
        "validating a stored inbound event ID",
    )?;
    validate_nonempty_bounded(
        &stored.message_id,
        STORE_INBOUND_ID_MAX_BYTES,
        "validating a stored inbound message ID",
    )?;
    validate_nonempty_bounded(
        &stored.scope_key,
        STORE_INBOUND_SCOPE_MAX_BYTES,
        "validating a stored inbound scope",
    )?;
    if stored
        .rejection_reason
        .as_ref()
        .is_some_and(|reason| reason.is_empty() || reason.len() > STORE_REJECTION_REASON_MAX_BYTES)
    {
        return Err(StoreError::CorruptData {
            context: "validating an inbound rejection reason",
        });
    }
    if stored.payload_bytes < 0
        || usize::try_from(stored.payload_bytes).unwrap_or(usize::MAX)
            > STORE_INBOUND_PAYLOAD_MAX_BYTES
    {
        return Err(StoreError::CorruptData {
            context: "validating stored inbound payload bytes",
        });
    }

    let current_namespace = is_tenant_namespace(tenant);
    match stored.state {
        InboundEventState::Received => {
            if !current_namespace
                || stored.turn_row_id.is_some()
                || stored.rejection_reason.is_some()
            {
                return Err(StoreError::CorruptData {
                    context: "validating a received inbound row",
                });
            }
            let _ = retained_from_stored(stored.clone())?;
        }
        InboundEventState::Accepted => {
            if !current_namespace
                || stored.turn_row_id.is_none()
                || stored.rejection_reason.is_some()
            {
                return Err(StoreError::CorruptData {
                    context: "validating an accepted inbound row",
                });
            }
            let _ = retained_from_stored(stored.clone())?;
            validate_turn_association(connection, stored)?;
        }
        InboundEventState::Completed | InboundEventState::Rejected => {
            if stored.payload_version.is_some()
                || stored.payload_blob.is_some()
                || stored.payload_bytes != 0
                || (stored.state == InboundEventState::Completed
                    && stored.rejection_reason.is_some())
            {
                return Err(StoreError::CorruptData {
                    context: "validating a terminal inbound row",
                });
            }
            if stored.turn_row_id.is_some() {
                if !current_namespace {
                    return Err(StoreError::CorruptData {
                        context: "validating a terminal inbound tenant",
                    });
                }
                validate_turn_association(connection, stored)?;
            }
        }
    }
    validate_inbound_reply_effect(tenant, stored)
}

fn validate_inbound_reply_effect(tenant: &str, stored: &StoredInbound) -> Result<(), StoreError> {
    let Some(recorded) = stored.reply_outbox_key.as_deref() else {
        return Ok(());
    };
    if !is_tenant_namespace(tenant)
        || recorded.is_empty()
        || recorded.len() > INBOUND_REPLY_KEY_MAX_BYTES
    {
        return Err(StoreError::CorruptData {
            context: "validating an inbound reply effect marker size",
        });
    }
    let suffix = match (stored.state, stored.turn_row_id) {
        (InboundEventState::Completed, None) if stored.rejection_reason.is_none() => {
            "control".to_owned()
        }
        (InboundEventState::Rejected, None) => {
            let reason = stored
                .rejection_reason
                .as_deref()
                .ok_or(StoreError::CorruptData {
                    context: "validating an inbound reply effect rejection reason",
                })?;
            if !is_classified_inbound_rejection(reason) {
                return Err(StoreError::CorruptData {
                    context: "validating an inbound reply effect rejection class",
                });
            }
            format!("{INBOUND_REPLY_NOTICE_PREFIX}{reason}")
        }
        _ => {
            return Err(StoreError::CorruptData {
                context: "validating an inbound reply effect state association",
            });
        }
    };
    let expected = inbound_reply_outbox_key(tenant, &stored.event_id, &suffix);
    if recorded != expected {
        return Err(StoreError::CorruptData {
            context: "validating an inbound reply effect identity",
        });
    }
    Ok(())
}

fn is_classified_inbound_rejection(reason: &str) -> bool {
    InboundRejectionKind::ALL
        .into_iter()
        .any(|kind| kind.as_str() == reason)
}

fn validate_turn_association(
    connection: &rusqlite::Connection,
    stored: &StoredInbound,
) -> Result<(), StoreError> {
    let turn_row_id = stored.turn_row_id.ok_or(StoreError::CorruptData {
        context: "validating a missing inbound turn association",
    })?;
    let turn: Option<(String, String, i64, i64)> = connection
        .query_row(
            "SELECT scope_key, state, uncertain, inbound_count FROM turns WHERE id = ?1",
            params![turn_row_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(|error| sqlite_error("validating an inbound turn association", &error))?;
    let Some((turn_scope, turn_state, uncertain, inbound_count)) = turn else {
        return Err(StoreError::CorruptData {
            context: "validating a missing inbound turn",
        });
    };
    if turn_scope != stored.scope_key || inbound_count <= 0 {
        return Err(StoreError::CorruptData {
            context: "validating an inbound turn scope and claim count",
        });
    }
    let unresolved = matches!(turn_state.as_str(), "starting" | "running") && uncertain == 0
        || turn_state == "uncertain" && uncertain == 1;
    let resolved = matches!(
        turn_state.as_str(),
        "completed" | "failed" | "interrupted" | "uncertain"
    ) && uncertain == 0;
    match stored.state {
        InboundEventState::Accepted if unresolved => Ok(()),
        InboundEventState::Completed if resolved && turn_state == "completed" => Ok(()),
        InboundEventState::Rejected if resolved => {
            let expected_reason = match turn_state.as_str() {
                "failed" => "turn_failed",
                "interrupted" => "turn_interrupted",
                "uncertain" => "turn_uncertain",
                _ => {
                    return Err(StoreError::CorruptData {
                        context: "validating a rejected inbound turn outcome",
                    });
                }
            };
            if stored.rejection_reason.as_deref() == Some(expected_reason) {
                Ok(())
            } else {
                Err(StoreError::CorruptData {
                    context: "validating an inbound turn rejection reason",
                })
            }
        }
        _ => Err(StoreError::CorruptData {
            context: "validating an inbound turn state association",
        }),
    }?;
    validate_runtime_turn_group(
        connection,
        turn_row_id,
        &turn_scope,
        &turn_state,
        uncertain,
        inbound_count,
    )
}

fn validate_runtime_turn_groups(connection: &rusqlite::Connection) -> Result<(), StoreError> {
    let mut statement = connection
        .prepare(
            "SELECT id, scope_key, state, uncertain, inbound_count
             FROM turns
             WHERE inbound_count > 0
             ORDER BY id",
        )
        .map_err(|error| sqlite_error("preparing inbound turn integrity scan", &error))?;
    let turns = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })
        .map_err(|error| sqlite_error("reading inbound turn integrity scan", &error))?;
    for turn in turns {
        let (id, scope, state, uncertain, inbound_count) =
            turn.map_err(|error| sqlite_error("decoding inbound turn integrity scan", &error))?;
        validate_runtime_turn_group(connection, id, &scope, &state, uncertain, inbound_count)?;
    }
    Ok(())
}

fn validate_runtime_turn_group(
    connection: &rusqlite::Connection,
    turn_row_id: i64,
    scope: &str,
    state: &str,
    uncertain: i64,
    inbound_count: i64,
) -> Result<(), StoreError> {
    let inbound_count = usize::try_from(inbound_count).map_err(|_| StoreError::CorruptData {
        context: "validating an inbound turn historical count",
    })?;
    let unresolved = matches!(state, "starting" | "running") && uncertain == 0
        || state == "uncertain" && uncertain == 1;
    if unresolved {
        return validate_unresolved_markers(connection, turn_row_id, scope, inbound_count);
    }
    if uncertain != 0 || !matches!(state, "completed" | "failed" | "interrupted" | "uncertain") {
        return Err(StoreError::CorruptData {
            context: "validating an inbound turn resolution state",
        });
    }
    let expected_state = if state == "completed" {
        "completed"
    } else {
        "rejected"
    };
    validate_resolved_markers(connection, turn_row_id, inbound_count, expected_state)
}

fn validate_nonempty_bounded(
    value: &str,
    limit: usize,
    context: &'static str,
) -> Result<(), StoreError> {
    if value.is_empty() || value.len() > limit {
        return Err(StoreError::CorruptData { context });
    }
    Ok(())
}

fn is_tenant_namespace(value: &str) -> bool {
    value.len() == TENANT_NAMESPACE_HEX_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn terminal_disposition(
    stored: &StoredInbound,
    reason: InboundRejectionKind,
) -> Result<Option<InboundDisposition>, StoreError> {
    match stored.state {
        InboundEventState::Received => Ok(None),
        InboundEventState::Accepted => stored
            .turn_row_id
            .map(|turn_row_id| Some(InboundDisposition::AlreadyClaimed { turn_row_id }))
            .ok_or(StoreError::CorruptData {
                context: "rejecting an unassociated accepted row",
            }),
        InboundEventState::Completed => Ok(Some(InboundDisposition::AlreadyCompleted)),
        InboundEventState::Rejected
            if stored.rejection_reason.as_deref() == Some(reason.as_str()) =>
        {
            Ok(Some(InboundDisposition::AlreadyRejected))
        }
        InboundEventState::Rejected => Err(StoreError::InvalidTransition {
            context: "rejecting an inbound row with a conflicting reason",
        }),
    }
}

fn reject_received_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    key: &InboundKey,
    reason: InboundRejectionKind,
) -> Result<InboundDisposition, StoreError> {
    let tenant = key.tenant.as_hex();
    let stored =
        read_inbound_row(transaction, &tenant, &key.event_id)?.ok_or(StoreError::NotFound {
            context: "rejecting an unknown inbound row",
        })?;
    if let Some(disposition) = terminal_disposition(&stored, reason)? {
        return Ok(disposition);
    }
    let _ = retained_from_stored(stored)?;
    let changed = transaction
        .execute(
            "UPDATE inbound_events
             SET state = 'rejected', rejection_reason = ?3,
                 payload_version = NULL, payload_blob = NULL, payload_bytes = 0,
                 updated_ms = ?4
             WHERE tenant = ?1 AND event_id = ?2 AND state = 'received'",
            params![tenant, key.event_id, reason.as_str(), now_ms()],
        )
        .map_err(|error| sqlite_error("rejecting a received inbound row", &error))?;
    if changed != 1 {
        return Err(StoreError::CorruptData {
            context: "rejecting a concurrently changed inbound row",
        });
    }
    Ok(InboundDisposition::Rejected)
}

#[allow(clippy::too_many_lines)]
fn enqueue_inbound_reply_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    notice: &super::NewOutboxRow,
) -> Result<(), StoreError> {
    if validate_existing_inbound_reply(transaction, notice)? {
        return Ok(());
    }
    let payload_bytes = u64::try_from(notice.payload_json.len()).unwrap_or(u64::MAX);
    let (count, bytes): (i64, i64) = transaction
        .query_row(
            "SELECT COUNT(*), COALESCE(SUM(payload_bytes), 0) FROM outbox
             WHERE state IN ('pending', 'sending')",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| sqlite_error("checking rejection notice capacity", &error))?;
    if u64::try_from(count).unwrap_or(u64::MAX) >= STORE_OUTBOX_MAX_ROWS
        || u64::try_from(bytes)
            .unwrap_or(u64::MAX)
            .saturating_add(payload_bytes)
            > STORE_OUTBOX_MAX_QUEUED_BYTES
    {
        return Err(StoreError::CapacityExceeded {
            context: "enqueueing an inbound rejection notice",
        });
    }

    enqueue_new_inbound_reply(transaction, notice, payload_bytes)
}

fn validate_existing_inbound_reply(
    transaction: &rusqlite::Transaction<'_>,
    notice: &super::NewOutboxRow,
) -> Result<bool, StoreError> {
    if notice.payload_json.len() > STORE_OUTBOX_PAYLOAD_MAX_BYTES {
        return Err(StoreError::PayloadTooLarge {
            context: "enqueueing an inbound rejection notice",
            limit: u64::try_from(STORE_OUTBOX_PAYLOAD_MAX_BYTES).unwrap_or(u64::MAX),
        });
    }
    let existing: Option<(String, String, String, i64)> = transaction
        .query_row(
            "SELECT scope_key, kind, payload_json, payload_bytes
             FROM outbox WHERE idempotency_key = ?1",
            params![notice.idempotency_key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(|error| sqlite_error("checking a rejection notice key", &error))?;
    let payload_bytes = u64::try_from(notice.payload_json.len()).unwrap_or(u64::MAX);
    if let Some((scope_key, kind, payload_json, stored_bytes)) = existing {
        if scope_key != notice.scope_key
            || kind != notice.kind
            || payload_json != notice.payload_json
            || u64::try_from(stored_bytes).ok() != Some(payload_bytes)
        {
            return Err(StoreError::CorruptData {
                context: "validating an inbound rejection notice idempotency key",
            });
        }
        return Ok(true);
    }
    Ok(false)
}

fn enqueue_new_inbound_reply(
    transaction: &rusqlite::Transaction<'_>,
    notice: &super::NewOutboxRow,
    payload_bytes: u64,
) -> Result<(), StoreError> {
    // Reuse the ordinary enqueue all-state guard. Its one bounded terminal
    // sweep executes inside this same SQLite transaction, so a later reply or
    // inbound-marker failure rolls the deletions back together with the insert.
    enforce_total_cap(transaction, payload_bytes)?;
    // Sequence watermark (same rule as `enqueue_one` in outbox.rs): a newly
    // enqueued notice must never be claimable before a row already parked for
    // retry. The notice's requested retry time is raised to the highest live
    // (`pending`/`sending`) retry time, so a rejection notice can never
    // overtake a failed row whose retry was already scheduled (global FIFO).
    let watermark: i64 = transaction
        .query_row(
            "SELECT COALESCE(MAX(next_retry_ms), 0) FROM outbox
             WHERE state IN ('pending', 'sending')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| sqlite_error("reading the outbox retry watermark", &error))?;
    let next_retry_ms = notice.next_retry_ms.max(watermark);
    let now = now_ms();
    let inserted = transaction
        .execute(
            "INSERT INTO outbox
             (idempotency_key, scope_key, kind, payload_json, payload_bytes,
              state, attempts, next_retry_ms, created_ms, updated_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, 'pending', 0, ?6, ?7, ?7)",
            params![
                notice.idempotency_key,
                notice.scope_key,
                notice.kind,
                notice.payload_json,
                i64::try_from(payload_bytes).unwrap_or(i64::MAX),
                next_retry_ms,
                now
            ],
        )
        .map_err(|error| sqlite_error("enqueueing an inbound rejection notice", &error))?;
    if inserted != 1 {
        return Err(StoreError::CorruptData {
            context: "inserting an inbound rejection notice",
        });
    }
    Ok(())
}

fn ensure_new_turn_capacity(
    connection: &rusqlite::Connection,
    turn: &super::NewTurnRow,
) -> Result<(), StoreError> {
    let (count, bytes): (i64, i64) = connection
        .query_row(
            "SELECT COUNT(*), COALESCE(SUM(
                 LENGTH(CAST(scope_key AS BLOB)) + LENGTH(CAST(client_message_id AS BLOB)) +
                 COALESCE(LENGTH(CAST(codex_thread_id AS BLOB)), 0) +
                 COALESCE(LENGTH(CAST(codex_turn_id AS BLOB)), 0)
             ), 0)
             FROM turns
             WHERE state IN ('starting', 'running')
                OR (state = 'uncertain' AND uncertain = 1)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| sqlite_error("reading live turn recovery usage", &error))?;
    let count = usize::try_from(count).unwrap_or(usize::MAX);
    let bytes = usize::try_from(bytes).unwrap_or(usize::MAX);
    let row_bytes = request_bytes(&[
        &turn.scope_key,
        &turn.client_message_id,
        turn.codex_thread_id.as_deref().unwrap_or_default(),
    ]);
    if count >= STORE_RECOVERY_TURN_MAX_ROWS
        || bytes.saturating_add(row_bytes) > STORE_RECOVERY_TURN_MAX_BYTES
    {
        return Err(StoreError::CapacityExceeded {
            context: "recording a live inbound turn",
        });
    }
    Ok(())
}

fn validate_resolved_markers(
    connection: &rusqlite::Connection,
    turn_row_id: i64,
    inbound_count: usize,
    expected_state: &str,
) -> Result<(), StoreError> {
    let (accepted, total, matching): (i64, i64, i64) = connection
        .query_row(
            "SELECT
                 COALESCE(SUM(CASE WHEN state = 'accepted' THEN 1 ELSE 0 END), 0),
                 COUNT(*),
                 COALESCE(SUM(CASE WHEN state = ?2 THEN 1 ELSE 0 END), 0)
             FROM inbound_events WHERE turn_row_id = ?1",
            params![turn_row_id, expected_state],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(|error| sqlite_error("validating resolved inbound markers", &error))?;
    let total = usize::try_from(total).unwrap_or(usize::MAX);
    let matching = usize::try_from(matching).unwrap_or(usize::MAX);
    if accepted != 0 || total > inbound_count || matching != total {
        return Err(StoreError::CorruptData {
            context: "validating resolved inbound markers",
        });
    }
    Ok(())
}

fn validate_unresolved_markers(
    connection: &rusqlite::Connection,
    turn_row_id: i64,
    turn_scope: &str,
    inbound_count: usize,
) -> Result<(), StoreError> {
    let (total, accepted, mismatched_scope): (i64, i64, i64) = connection
        .query_row(
            "SELECT COUNT(*),
                    COALESCE(SUM(CASE WHEN state = 'accepted' THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN scope_key != ?2 THEN 1 ELSE 0 END), 0)
             FROM inbound_events WHERE turn_row_id = ?1",
            params![turn_row_id, turn_scope],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(|error| sqlite_error("validating unresolved inbound markers", &error))?;
    if usize::try_from(total).ok() != Some(inbound_count)
        || usize::try_from(accepted).ok() != Some(inbound_count)
        || mismatched_scope != 0
    {
        return Err(StoreError::CorruptData {
            context: "validating unresolved inbound markers",
        });
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_payload_without_sender_human_flag_decodes_as_non_human() {
        let payload = serde_json::json!({
            "event_id": "evt_legacy",
            "message_id": "om_legacy",
            "chat_id": "oc_legacy",
            "sender_id": "ou_legacy",
            "chat_type": "group",
            "thread_id": null,
            "root_id": null,
            "reply_to_message_id": null,
            "text": "hello",
            "mentions_bot": true,
            "mention_all": false,
            "resources": [],
            "message_type": "text",
            "create_time_ms": 1,
            "scope": {"kind": "chat", "chat_id": "oc_legacy", "thread_id": null},
        });
        let dto: InboundPayloadV1 =
            serde_json::from_value(payload).expect("legacy payload should decode");
        assert!(!dto.sender_is_human);
    }
}
