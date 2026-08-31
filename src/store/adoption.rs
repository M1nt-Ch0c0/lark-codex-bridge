#![allow(clippy::doc_markdown)]

//! Durable saga for explicit persisted-thread adoption.
//!
//! Reserving an adoption never changes the scope mapping. A successful remote
//! `thread/resume` is followed by [`StoreHandle::commit_thread_adoption`],
//! which writes the externally-adopted mapping and advances the saga in one
//! SQLite transaction. Release mirrors that ordering: the mapping remains
//! active while the owner process is being reaped and is retired only by a
//! confirmed release finish.

use std::path::Path;

use rusqlite::{Transaction, TransactionBehavior, params};

use super::sessions::{ThreadOrigin, ThreadRow, read_thread_row};
use super::{StoreError, StoreHandle, now_ms, query_optional, request_bytes, sqlite_error};
use crate::lark::normalize::ScopeKey;

/// Durable adoption saga states.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThreadAdoptionState {
    /// The target is reserved while the dedicated owner attempts acquisition.
    Acquiring,
    /// The remote writer was acquired and the mapping is committed.
    Owned,
    /// New work is fenced while the dedicated owner is being stopped and reaped.
    Releasing,
    /// A crash or uncertain transition requires explicit recovery before writes.
    RecoveryRequired,
    /// A release attempt failed; the mapping remains active and fenced.
    ReleaseFailed,
    /// The latest saga generation reached a durable terminal outcome.
    Terminal,
}

impl ThreadAdoptionState {
    /// Stable database representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Acquiring => "acquiring",
            Self::Owned => "owned",
            Self::Releasing => "releasing",
            Self::RecoveryRequired => "recovery_required",
            Self::ReleaseFailed => "release_failed",
            Self::Terminal => "terminal",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "acquiring" => Some(Self::Acquiring),
            "owned" => Some(Self::Owned),
            "releasing" => Some(Self::Releasing),
            "recovery_required" => Some(Self::RecoveryRequired),
            "release_failed" => Some(Self::ReleaseFailed),
            "terminal" => Some(Self::Terminal),
            _ => None,
        }
    }
}

/// Durable outcome of a terminal adoption generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThreadAdoptionOutcome {
    /// Acquisition did not produce a committed mapping and the owner was reaped.
    AcquisitionFailed,
    /// Release and process-tree reap completed before the mapping was retired.
    Released,
}

impl ThreadAdoptionOutcome {
    /// Stable database representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AcquisitionFailed => "acquisition_failed",
            Self::Released => "released",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "acquisition_failed" => Some(Self::AcquisitionFailed),
            "released" => Some(Self::Released),
            _ => None,
        }
    }
}

/// Result supplied after one bounded release/reap attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThreadAdoptionReleaseResult {
    /// The dedicated owner and its process tree were confirmed reaped.
    Released,
    /// Reap could not be confirmed; the mapping must remain fenced and active.
    Failed,
}

/// Latest durable saga generation for one scope.
#[derive(Clone, Eq, PartialEq)]
pub struct ThreadAdoptionSaga {
    /// Owning Lark scope key.
    pub scope_key: String,
    /// Reserved or owned Codex thread identifier.
    pub codex_thread_id: String,
    /// Monotonically increasing per-scope fence token.
    pub generation: u64,
    /// Current durable state.
    pub state: ThreadAdoptionState,
    /// Present only when `state` is terminal.
    pub outcome: Option<ThreadAdoptionOutcome>,
    /// Saga-generation creation time in milliseconds.
    pub created_ms: i64,
    /// Last durable transition time in milliseconds.
    pub updated_ms: i64,
}

impl std::fmt::Debug for ThreadAdoptionSaga {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ThreadAdoptionSaga")
            .field("scope_key_len", &self.scope_key.len())
            .field("codex_thread_id_len", &self.codex_thread_id.len())
            .field("generation", &self.generation)
            .field("state", &self.state)
            .field("outcome", &self.outcome)
            .field("created_ms", &self.created_ms)
            .field("updated_ms", &self.updated_ms)
            .finish_non_exhaustive()
    }
}

impl StoreHandle {
    /// Reserves one persisted thread for explicit acquisition.
    ///
    /// This transaction verifies that the scope exists, has no starting,
    /// running, or uncertain turn, and that neither an active mapping nor a
    /// non-terminal saga in another scope names the target. It does not change
    /// the scope's current thread mapping.
    ///
    /// # Errors
    ///
    /// Returns a content-free transition error when the scope or target is not
    /// eligible, or a store error when the reservation cannot be persisted.
    pub async fn reserve_thread_adoption(
        &self,
        scope: &ScopeKey,
        codex_thread_id: &str,
    ) -> Result<ThreadAdoptionSaga, StoreError> {
        if codex_thread_id.is_empty() {
            return Err(StoreError::InvalidTransition {
                context: "reserving an empty persisted thread identifier",
            });
        }
        let scope_key = scope.to_string();
        let codex_thread_id = codex_thread_id.to_owned();
        let request_size = request_bytes(&[&scope_key, &codex_thread_id]);
        self.run_sized(request_size, move |connection| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|error| sqlite_error("starting an adoption reservation", &error))?;
            require_scope(&transaction, &scope_key)?;
            require_no_live_turn(&transaction, &scope_key, "reserving a thread adoption")?;
            require_target_available(&transaction, &scope_key, &codex_thread_id)?;
            require_scope_available(&transaction, &scope_key)?;

            let previous = read_saga_by_scope(&transaction, &scope_key)?;
            let generation = previous.as_ref().map_or(Ok(1_u64), |row| {
                if row.state != ThreadAdoptionState::Terminal {
                    return Err(StoreError::InvalidTransition {
                        context: "replacing a live thread adoption saga",
                    });
                }
                row.generation
                    .checked_add(1)
                    .ok_or(StoreError::CapacityExceeded {
                        context: "advancing a thread adoption generation",
                    })
            })?;
            let generation_i64 = generation_i64(generation)?;
            let now = now_ms();
            if previous.is_some() {
                transaction
                    .execute(
                        "UPDATE thread_adoption_sagas
                         SET generation = ?2, codex_thread_id = ?3,
                             state = 'acquiring', outcome = NULL,
                             created_ms = ?4, updated_ms = ?4
                         WHERE scope_key = ?1 AND state = 'terminal'",
                        params![scope_key, generation_i64, codex_thread_id, now],
                    )
                    .map_err(|error| sqlite_error("replacing a terminal adoption saga", &error))?;
            } else {
                transaction
                    .execute(
                        "INSERT INTO thread_adoption_sagas (
                             scope_key, generation, codex_thread_id, state, outcome,
                             created_ms, updated_ms
                         ) VALUES (?1, ?2, ?3, 'acquiring', NULL, ?4, ?4)",
                        params![scope_key, generation_i64, codex_thread_id, now],
                    )
                    .map_err(|error| sqlite_error("recording an adoption reservation", &error))?;
            }
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing an adoption reservation", &error))?;
            Ok(ThreadAdoptionSaga {
                scope_key,
                codex_thread_id,
                generation,
                state: ThreadAdoptionState::Acquiring,
                outcome: None,
                created_ms: now,
                updated_ms: now,
            })
        })
        .await
    }

    /// Commits a mapping only after authoritative remote writer acquisition.
    ///
    /// An existing active `bridge_created` mapping is retired in the same
    /// transaction. An externally-adopted mapping can never be replaced by
    /// this operation; it must complete its release saga first. The canonical
    /// workspace and policy fingerprint are updated in that same transaction.
    ///
    /// # Errors
    ///
    /// Returns a transition error for a stale reservation, a live turn, or a
    /// conflicting mapping. A failed transaction leaves the reservation in
    /// `acquiring` and preserves the previous mapping.
    #[allow(clippy::too_many_lines)]
    pub async fn commit_thread_adoption(
        &self,
        reservation: &ThreadAdoptionSaga,
        canonical_cwd: &Path,
        policy_fingerprint: &str,
    ) -> Result<ThreadRow, StoreError> {
        let scope_key = reservation.scope_key.clone();
        let codex_thread_id = reservation.codex_thread_id.clone();
        let generation = reservation.generation;
        let generation_i64 = generation_i64(generation)?;
        let canonical_cwd = canonical_cwd
            .to_str()
            .ok_or(StoreError::InvalidPath {
                context: "persisting a non-UTF-8 adopted workspace path",
            })?
            .to_owned();
        let policy_fingerprint = policy_fingerprint.to_owned();
        let request_size = request_bytes(&[
            &scope_key,
            &codex_thread_id,
            &canonical_cwd,
            &policy_fingerprint,
        ]);
        self.run_sized(request_size, move |connection| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|error| sqlite_error("starting an adoption commit", &error))?;
            let saga = require_exact_saga(&transaction, &scope_key, &codex_thread_id, generation)?;
            require_state(
                &saga,
                &[ThreadAdoptionState::Acquiring],
                "committing an adoption",
            )?;
            require_no_live_turn(&transaction, &scope_key, "committing a thread adoption")?;

            let now = now_ms();
            let updated_scope = transaction
                .execute(
                    "UPDATE scopes
                     SET cwd = ?2, policy_fingerprint = ?3, updated_ms = ?4
                     WHERE scope_key = ?1",
                    params![scope_key, canonical_cwd, policy_fingerprint, now],
                )
                .map_err(|error| sqlite_error("updating an adopted thread workspace", &error))?;
            if updated_scope != 1 {
                return Err(StoreError::NotFound {
                    context: "committing adoption for an unknown scope",
                });
            }
            require_target_mapping_available(&transaction, &codex_thread_id)?;

            if let Some((current_thread_id, origin)) =
                active_mapping_identity(&transaction, &scope_key)?
            {
                if origin != ThreadOrigin::BridgeCreated {
                    return Err(StoreError::InvalidTransition {
                        context: "replacing an externally adopted mapping without release",
                    });
                }
                transaction
                    .execute(
                        "UPDATE threads SET status = 'archived', archived_ms = ?3
                         WHERE scope_key = ?1 AND codex_thread_id = ?2
                           AND status = 'active' AND origin = 'bridge_created'",
                        params![scope_key, current_thread_id, now],
                    )
                    .map_err(|error| sqlite_error("retiring a bridge-created mapping", &error))?;
            }

            let advanced = transaction
                .execute(
                    "UPDATE thread_adoption_sagas
                     SET state = 'owned', updated_ms = ?4
                     WHERE scope_key = ?1 AND generation = ?2
                       AND codex_thread_id = ?3 AND state = 'acquiring'",
                    params![scope_key, generation_i64, codex_thread_id, now],
                )
                .map_err(|error| sqlite_error("advancing an acquired thread saga", &error))?;
            if advanced != 1 {
                return Err(StoreError::InvalidTransition {
                    context: "committing a stale thread adoption reservation",
                });
            }
            let inserted = transaction
                .execute(
                    "INSERT INTO threads (
                         scope_key, codex_thread_id, status, created_ms, archived_ms,
                         context_tools_version, origin, adoption_generation
                     ) VALUES (?1, ?2, 'active', ?4, NULL, 0,
                               'externally_adopted', ?3)
                     ON CONFLICT(scope_key, codex_thread_id) DO UPDATE SET
                         status = 'active', created_ms = excluded.created_ms,
                         archived_ms = NULL, context_tools_version = 0,
                         origin = 'externally_adopted',
                         adoption_generation = excluded.adoption_generation
                     WHERE threads.status = 'archived'",
                    params![scope_key, codex_thread_id, generation_i64, now],
                )
                .map_err(|error| sqlite_error("committing an adopted thread mapping", &error))?;
            if inserted != 1 {
                return Err(StoreError::InvalidTransition {
                    context: "committing a conflicting adopted thread mapping",
                });
            }
            let row = transaction
                .query_row(
                    "SELECT scope_key, codex_thread_id, status, created_ms, archived_ms,
                            context_tools_version, origin, adoption_generation
                     FROM threads
                     WHERE scope_key = ?1 AND codex_thread_id = ?2 AND status = 'active'",
                    params![scope_key, codex_thread_id],
                    read_thread_row,
                )
                .map_err(|error| sqlite_error("reading a committed adopted mapping", &error))?;
            transaction.commit().map_err(|error| {
                sqlite_error("committing an adopted mapping transaction", &error)
            })?;
            Ok(row)
        })
        .await
    }

    /// Records a known acquisition failure after the attempted owner process is reaped.
    ///
    /// This terminal transition is valid only when no mapping was committed.
    ///
    /// # Errors
    ///
    /// Returns a transition error for a stale generation, a committed mapping,
    /// or a live turn, and a store error when persistence fails.
    pub async fn finish_thread_adoption_acquisition_failure(
        &self,
        reservation: &ThreadAdoptionSaga,
    ) -> Result<ThreadAdoptionSaga, StoreError> {
        let scope_key = reservation.scope_key.clone();
        let codex_thread_id = reservation.codex_thread_id.clone();
        let generation = reservation.generation;
        let generation_i64 = generation_i64(generation)?;
        let request_size = request_bytes(&[&scope_key, &codex_thread_id]);
        self.run_sized(request_size, move |connection| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|error| sqlite_error("starting acquisition failure finish", &error))?;
            let saga = require_exact_saga(&transaction, &scope_key, &codex_thread_id, generation)?;
            require_state(
                &saga,
                &[
                    ThreadAdoptionState::Acquiring,
                    ThreadAdoptionState::RecoveryRequired,
                ],
                "finishing a failed acquisition",
            )?;
            require_no_live_turn(&transaction, &scope_key, "finishing a failed acquisition")?;
            if owned_mapping_exists(&transaction, &scope_key, &codex_thread_id, generation_i64)? {
                return Err(StoreError::InvalidTransition {
                    context: "failing an acquisition with a committed mapping",
                });
            }
            let now = now_ms();
            transaction
                .execute(
                    "UPDATE thread_adoption_sagas
                     SET state = 'terminal', outcome = 'acquisition_failed', updated_ms = ?4
                     WHERE scope_key = ?1 AND generation = ?2 AND codex_thread_id = ?3",
                    params![scope_key, generation_i64, codex_thread_id, now],
                )
                .map_err(|error| sqlite_error("terminalizing a failed acquisition", &error))?;
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing a failed acquisition", &error))?;
            Ok(ThreadAdoptionSaga {
                state: ThreadAdoptionState::Terminal,
                outcome: Some(ThreadAdoptionOutcome::AcquisitionFailed),
                updated_ms: now,
                ..saga
            })
        })
        .await
    }

    /// Fences new work and begins release of one committed adopted mapping.
    ///
    /// Recovery-required and release-failed generations may retry release, but
    /// acquiring generations without a committed mapping may not.
    ///
    /// # Errors
    ///
    /// Returns a transition error for a stale generation, missing exact
    /// mapping, or live turn, and a store error when persistence fails.
    pub async fn begin_thread_adoption_release(
        &self,
        reservation: &ThreadAdoptionSaga,
    ) -> Result<ThreadAdoptionSaga, StoreError> {
        let scope_key = reservation.scope_key.clone();
        let codex_thread_id = reservation.codex_thread_id.clone();
        let generation = reservation.generation;
        let generation_i64 = generation_i64(generation)?;
        let request_size = request_bytes(&[&scope_key, &codex_thread_id]);
        self.run_sized(request_size, move |connection| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|error| sqlite_error("starting an adoption release", &error))?;
            let saga = require_exact_saga(&transaction, &scope_key, &codex_thread_id, generation)?;
            require_state(
                &saga,
                &[
                    ThreadAdoptionState::Owned,
                    ThreadAdoptionState::RecoveryRequired,
                    ThreadAdoptionState::ReleaseFailed,
                ],
                "beginning an adoption release",
            )?;
            require_no_live_turn(
                &transaction,
                &scope_key,
                "beginning a thread adoption release",
            )?;
            require_owned_mapping(&transaction, &scope_key, &codex_thread_id, generation_i64)?;
            let now = now_ms();
            transaction
                .execute(
                    "UPDATE thread_adoption_sagas
                     SET state = 'releasing', outcome = NULL, updated_ms = ?4
                     WHERE scope_key = ?1 AND generation = ?2 AND codex_thread_id = ?3",
                    params![scope_key, generation_i64, codex_thread_id, now],
                )
                .map_err(|error| sqlite_error("recording an adoption release fence", &error))?;
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing an adoption release fence", &error))?;
            Ok(ThreadAdoptionSaga {
                state: ThreadAdoptionState::Releasing,
                outcome: None,
                updated_ms: now,
                ..saga
            })
        })
        .await
    }

    /// Finishes one release attempt without ever dropping an unconfirmed mapping.
    ///
    /// `Released` retires the mapping and terminalizes the saga atomically.
    /// `Failed` retains the active mapping and records `release_failed` so a
    /// later explicit recovery can retry.
    ///
    /// # Errors
    ///
    /// Returns a transition error for a stale generation, missing exact
    /// mapping, or live turn, and a store error when persistence fails.
    pub async fn finish_thread_adoption_release(
        &self,
        reservation: &ThreadAdoptionSaga,
        result: ThreadAdoptionReleaseResult,
    ) -> Result<ThreadAdoptionSaga, StoreError> {
        let scope_key = reservation.scope_key.clone();
        let codex_thread_id = reservation.codex_thread_id.clone();
        let generation = reservation.generation;
        let generation_i64 = generation_i64(generation)?;
        let request_size = request_bytes(&[&scope_key, &codex_thread_id]);
        self.run_sized(request_size, move |connection| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|error| sqlite_error("starting an adoption release finish", &error))?;
            let saga = require_exact_saga(&transaction, &scope_key, &codex_thread_id, generation)?;
            require_state(
                &saga,
                &[ThreadAdoptionState::Releasing],
                "finishing an adoption release",
            )?;
            require_no_live_turn(
                &transaction,
                &scope_key,
                "finishing a thread adoption release",
            )?;
            require_owned_mapping(&transaction, &scope_key, &codex_thread_id, generation_i64)?;
            let now = now_ms();
            let (state, outcome) = match result {
                ThreadAdoptionReleaseResult::Released => {
                    let retired = transaction
                        .execute(
                            "UPDATE threads SET status = 'archived', archived_ms = ?4
                             WHERE scope_key = ?1 AND codex_thread_id = ?2
                               AND adoption_generation = ?3 AND status = 'active'
                               AND origin = 'externally_adopted'",
                            params![scope_key, codex_thread_id, generation_i64, now],
                        )
                        .map_err(|error| {
                            sqlite_error("retiring a released adopted mapping", &error)
                        })?;
                    if retired != 1 {
                        return Err(StoreError::InvalidTransition {
                            context: "retiring a stale adopted mapping",
                        });
                    }
                    transaction
                        .execute(
                            "UPDATE thread_adoption_sagas
                             SET state = 'terminal', outcome = 'released', updated_ms = ?4
                             WHERE scope_key = ?1 AND generation = ?2
                               AND codex_thread_id = ?3 AND state = 'releasing'",
                            params![scope_key, generation_i64, codex_thread_id, now],
                        )
                        .map_err(|error| {
                            sqlite_error("terminalizing a released adoption", &error)
                        })?;
                    (
                        ThreadAdoptionState::Terminal,
                        Some(ThreadAdoptionOutcome::Released),
                    )
                }
                ThreadAdoptionReleaseResult::Failed => {
                    transaction
                        .execute(
                            "UPDATE thread_adoption_sagas
                             SET state = 'release_failed', outcome = NULL, updated_ms = ?4
                             WHERE scope_key = ?1 AND generation = ?2
                               AND codex_thread_id = ?3 AND state = 'releasing'",
                            params![scope_key, generation_i64, codex_thread_id, now],
                        )
                        .map_err(|error| {
                            sqlite_error("recording a failed adoption release", &error)
                        })?;
                    (ThreadAdoptionState::ReleaseFailed, None)
                }
            };
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing an adoption release finish", &error))?;
            Ok(ThreadAdoptionSaga {
                state,
                outcome,
                updated_ms: now,
                ..saga
            })
        })
        .await
    }

    /// Fences one uncertain non-terminal adoption generation.
    ///
    /// This safety operation deliberately remains available while a live turn
    /// exists: fencing must not depend on the condition it is meant to contain.
    /// It never changes a scope mapping.
    ///
    /// # Errors
    ///
    /// Returns a transition error for a stale or terminal generation and a
    /// store error when the fence cannot be persisted.
    pub async fn fence_thread_adoption(
        &self,
        reservation: &ThreadAdoptionSaga,
    ) -> Result<ThreadAdoptionSaga, StoreError> {
        let scope_key = reservation.scope_key.clone();
        let codex_thread_id = reservation.codex_thread_id.clone();
        let generation = reservation.generation;
        let generation_i64 = generation_i64(generation)?;
        let request_size = request_bytes(&[&scope_key, &codex_thread_id]);
        self.run_sized(request_size, move |connection| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|error| sqlite_error("starting an adoption fence", &error))?;
            let saga = require_exact_saga(&transaction, &scope_key, &codex_thread_id, generation)?;
            if saga.state == ThreadAdoptionState::Terminal {
                return Err(StoreError::InvalidTransition {
                    context: "fencing a terminal thread adoption saga",
                });
            }
            if saga.state == ThreadAdoptionState::RecoveryRequired {
                transaction.commit().map_err(|error| {
                    sqlite_error("committing an idempotent adoption fence", &error)
                })?;
                return Ok(saga);
            }
            let now = now_ms();
            transaction
                .execute(
                    "UPDATE thread_adoption_sagas
                     SET state = 'recovery_required', outcome = NULL, updated_ms = ?4
                     WHERE scope_key = ?1 AND generation = ?2 AND codex_thread_id = ?3
                       AND state != 'terminal'",
                    params![scope_key, generation_i64, codex_thread_id, now],
                )
                .map_err(|error| sqlite_error("recording an adoption recovery fence", &error))?;
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing an adoption recovery fence", &error))?;
            Ok(ThreadAdoptionSaga {
                state: ThreadAdoptionState::RecoveryRequired,
                outcome: None,
                updated_ms: now,
                ..saga
            })
        })
        .await
    }

    /// Fences every non-terminal adoption generation after process startup.
    ///
    /// The operation validates that every active externally-adopted mapping has
    /// a matching non-terminal saga before changing any state. Mappings remain
    /// active; recovery must either reacquire safely or reap the prior owner and
    /// finish release explicitly.
    ///
    /// # Errors
    ///
    /// Returns a corruption error for an inconsistent mapping/saga pair and a
    /// store error when startup fencing cannot be persisted.
    pub async fn fence_thread_adoptions_on_startup(&self) -> Result<u64, StoreError> {
        self.run(|connection| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(|error| sqlite_error("starting startup adoption fencing", &error))?;
            let orphaned_mappings: i64 = transaction
                .query_row(
                    "SELECT COUNT(*)
                     FROM threads AS t
                     LEFT JOIN thread_adoption_sagas AS s
                       ON s.scope_key = t.scope_key
                      AND s.codex_thread_id = t.codex_thread_id
                      AND s.generation = t.adoption_generation
                      AND s.state != 'terminal'
                     WHERE t.status = 'active' AND t.origin = 'externally_adopted'
                       AND s.scope_key IS NULL",
                    [],
                    |row| row.get(0),
                )
                .map_err(|error| sqlite_error("validating startup adopted mappings", &error))?;
            if orphaned_mappings != 0 {
                return Err(StoreError::CorruptData {
                    context: "fencing an adopted mapping without a live saga",
                });
            }
            let missing_mappings: i64 = transaction
                .query_row(
                    "SELECT COUNT(*)
                     FROM thread_adoption_sagas AS s
                     LEFT JOIN threads AS t
                       ON t.scope_key = s.scope_key
                      AND t.codex_thread_id = s.codex_thread_id
                      AND t.adoption_generation = s.generation
                      AND t.status = 'active'
                      AND t.origin = 'externally_adopted'
                     WHERE s.state IN ('owned', 'releasing', 'release_failed')
                       AND t.scope_key IS NULL",
                    [],
                    |row| row.get(0),
                )
                .map_err(|error| sqlite_error("validating startup adoption sagas", &error))?;
            if missing_mappings != 0 {
                return Err(StoreError::CorruptData {
                    context: "fencing an owned adoption saga without its mapping",
                });
            }
            let now = now_ms();
            let changed = transaction
                .execute(
                    "UPDATE thread_adoption_sagas
                     SET state = 'recovery_required', outcome = NULL, updated_ms = ?1
                     WHERE state IN ('acquiring', 'owned', 'releasing', 'release_failed')",
                    params![now],
                )
                .map_err(|error| sqlite_error("fencing startup adoption sagas", &error))?;
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing startup adoption fencing", &error))?;
            u64::try_from(changed).map_err(|_| StoreError::CapacityExceeded {
                context: "counting startup adoption fences",
            })
        })
        .await
    }

    /// Reads the latest adoption saga generation for one scope.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or SQLite fails.
    pub async fn thread_adoption_saga(
        &self,
        scope: &ScopeKey,
    ) -> Result<Option<ThreadAdoptionSaga>, StoreError> {
        let scope_key = scope.to_string();
        let request_size = request_bytes(&[&scope_key]);
        self.run_sized(request_size, move |connection| {
            read_saga_by_scope(connection, &scope_key)
        })
        .await
    }

    /// Reads the scope's non-terminal adoption saga, when present.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or SQLite fails.
    pub async fn active_thread_adoption(
        &self,
        scope: &ScopeKey,
    ) -> Result<Option<ThreadAdoptionSaga>, StoreError> {
        let scope_key = scope.to_string();
        let request_size = request_bytes(&[&scope_key]);
        self.run_sized(request_size, move |connection| {
            let row = connection.query_row(
                "SELECT scope_key, generation, codex_thread_id, state, outcome,
                        created_ms, updated_ms
                 FROM thread_adoption_sagas
                 WHERE scope_key = ?1 AND state != 'terminal'",
                params![scope_key],
                read_saga_row,
            );
            query_optional(row, "reading an active thread adoption saga")
        })
        .await
    }

    /// Reports whether a persisted-thread target can still be offered for
    /// adoption.
    ///
    /// Active mappings and non-terminal reservations make the target
    /// unavailable bridge-wide. Archived mappings and terminal saga history
    /// do not exclude it from later discovery.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or SQLite fails.
    pub async fn thread_adoption_target_available(
        &self,
        codex_thread_id: &str,
    ) -> Result<bool, StoreError> {
        if codex_thread_id.is_empty() {
            return Ok(false);
        }
        let codex_thread_id = codex_thread_id.to_owned();
        let request_size = request_bytes(&[&codex_thread_id]);
        self.run_sized(request_size, move |connection| {
            let unavailable: i64 = connection
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM threads
                         WHERE codex_thread_id = ?1 AND status = 'active'
                         UNION ALL
                         SELECT 1 FROM thread_adoption_sagas
                         WHERE codex_thread_id = ?1 AND state != 'terminal'
                     )",
                    params![codex_thread_id],
                    |row| row.get(0),
                )
                .map_err(|error| {
                    sqlite_error("checking persisted thread adoption availability", &error)
                })?;
            Ok(unavailable == 0)
        })
        .await
    }
}

fn require_scope(transaction: &Transaction<'_>, scope_key: &str) -> Result<(), StoreError> {
    let exists: i64 = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM scopes WHERE scope_key = ?1)",
            params![scope_key],
            |row| row.get(0),
        )
        .map_err(|error| sqlite_error("checking an adoption scope", &error))?;
    if exists == 0 {
        return Err(StoreError::NotFound {
            context: "reserving adoption for an unknown scope",
        });
    }
    Ok(())
}

fn require_no_live_turn(
    transaction: &Transaction<'_>,
    scope_key: &str,
    context: &'static str,
) -> Result<(), StoreError> {
    let live: i64 = transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM turns
                 WHERE scope_key = ?1 AND state IN ('starting', 'running', 'uncertain')
             )",
            params![scope_key],
            |row| row.get(0),
        )
        .map_err(|error| sqlite_error("checking live turns for thread adoption", &error))?;
    if live != 0 {
        return Err(StoreError::InvalidTransition { context });
    }
    Ok(())
}

fn require_scope_available(
    transaction: &Transaction<'_>,
    scope_key: &str,
) -> Result<(), StoreError> {
    if let Some((_, origin)) = active_mapping_identity(transaction, scope_key)? {
        if origin == ThreadOrigin::ExternallyAdopted {
            return Err(StoreError::InvalidTransition {
                context: "reserving while the scope owns an adopted thread",
            });
        }
    }
    if read_saga_by_scope(transaction, scope_key)?
        .is_some_and(|row| row.state != ThreadAdoptionState::Terminal)
    {
        return Err(StoreError::InvalidTransition {
            context: "reserving while the scope has a live adoption saga",
        });
    }
    Ok(())
}

fn require_target_available(
    transaction: &Transaction<'_>,
    scope_key: &str,
    codex_thread_id: &str,
) -> Result<(), StoreError> {
    require_target_mapping_available(transaction, codex_thread_id)?;
    let reserved_elsewhere: i64 = transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM thread_adoption_sagas
                 WHERE codex_thread_id = ?1 AND state != 'terminal' AND scope_key != ?2
             )",
            params![codex_thread_id, scope_key],
            |row| row.get(0),
        )
        .map_err(|error| sqlite_error("checking a reserved adoption target", &error))?;
    if reserved_elsewhere != 0 {
        return Err(StoreError::InvalidTransition {
            context: "reserving a thread already claimed by another scope",
        });
    }
    Ok(())
}

fn require_target_mapping_available(
    transaction: &Transaction<'_>,
    codex_thread_id: &str,
) -> Result<(), StoreError> {
    let active: i64 = transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM threads
                 WHERE codex_thread_id = ?1 AND status = 'active'
             )",
            params![codex_thread_id],
            |row| row.get(0),
        )
        .map_err(|error| sqlite_error("checking an active thread adoption target", &error))?;
    if active != 0 {
        return Err(StoreError::InvalidTransition {
            context: "adopting a thread with an active bridge mapping",
        });
    }
    Ok(())
}

fn active_mapping_identity(
    transaction: &Transaction<'_>,
    scope_key: &str,
) -> Result<Option<(String, ThreadOrigin)>, StoreError> {
    let row = transaction.query_row(
        "SELECT codex_thread_id, origin
         FROM threads WHERE scope_key = ?1 AND status = 'active'",
        params![scope_key],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
    );
    let Some((thread_id, origin)) = query_optional(row, "reading an active mapping origin")? else {
        return Ok(None);
    };
    let origin = ThreadOrigin::parse(&origin).ok_or(StoreError::CorruptData {
        context: "decoding an active mapping origin",
    })?;
    Ok(Some((thread_id, origin)))
}

fn require_owned_mapping(
    transaction: &Transaction<'_>,
    scope_key: &str,
    codex_thread_id: &str,
    generation: i64,
) -> Result<(), StoreError> {
    if !owned_mapping_exists(transaction, scope_key, codex_thread_id, generation)? {
        return Err(StoreError::InvalidTransition {
            context: "transitioning an adoption without its exact active mapping",
        });
    }
    Ok(())
}

fn owned_mapping_exists(
    transaction: &Transaction<'_>,
    scope_key: &str,
    codex_thread_id: &str,
    generation: i64,
) -> Result<bool, StoreError> {
    let exists: i64 = transaction
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM threads
                 WHERE scope_key = ?1 AND codex_thread_id = ?2
                   AND adoption_generation = ?3 AND status = 'active'
                   AND origin = 'externally_adopted'
             )",
            params![scope_key, codex_thread_id, generation],
            |row| row.get(0),
        )
        .map_err(|error| sqlite_error("checking an adopted mapping generation", &error))?;
    Ok(exists != 0)
}

fn require_exact_saga(
    transaction: &Transaction<'_>,
    scope_key: &str,
    codex_thread_id: &str,
    generation: u64,
) -> Result<ThreadAdoptionSaga, StoreError> {
    let saga = read_saga_by_scope(transaction, scope_key)?.ok_or(StoreError::NotFound {
        context: "transitioning an unknown thread adoption saga",
    })?;
    if saga.generation != generation || saga.codex_thread_id != codex_thread_id {
        return Err(StoreError::InvalidTransition {
            context: "transitioning a stale thread adoption generation",
        });
    }
    Ok(saga)
}

fn require_state(
    saga: &ThreadAdoptionSaga,
    allowed: &[ThreadAdoptionState],
    context: &'static str,
) -> Result<(), StoreError> {
    if !allowed.contains(&saga.state) {
        return Err(StoreError::InvalidTransition { context });
    }
    Ok(())
}

fn read_saga_by_scope(
    connection: &rusqlite::Connection,
    scope_key: &str,
) -> Result<Option<ThreadAdoptionSaga>, StoreError> {
    let row = connection.query_row(
        "SELECT scope_key, generation, codex_thread_id, state, outcome,
                created_ms, updated_ms
         FROM thread_adoption_sagas WHERE scope_key = ?1",
        params![scope_key],
        read_saga_row,
    );
    query_optional(row, "reading a thread adoption saga")
}

fn read_saga_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ThreadAdoptionSaga> {
    let generation_value: i64 = row.get(1)?;
    let generation = u64::try_from(generation_value).map_err(|_| {
        rusqlite::Error::InvalidColumnType(
            1,
            "generation".to_owned(),
            rusqlite::types::Type::Integer,
        )
    })?;
    let state_value: String = row.get(3)?;
    let state = ThreadAdoptionState::parse(&state_value).ok_or_else(|| {
        rusqlite::Error::InvalidColumnType(3, "state".to_owned(), rusqlite::types::Type::Text)
    })?;
    let outcome = row
        .get::<_, Option<String>>(4)?
        .map(|value| {
            ThreadAdoptionOutcome::parse(&value).ok_or_else(|| {
                rusqlite::Error::InvalidColumnType(
                    4,
                    "outcome".to_owned(),
                    rusqlite::types::Type::Text,
                )
            })
        })
        .transpose()?;
    if (state == ThreadAdoptionState::Terminal) != outcome.is_some() {
        return Err(rusqlite::Error::InvalidColumnType(
            4,
            "outcome".to_owned(),
            rusqlite::types::Type::Text,
        ));
    }
    Ok(ThreadAdoptionSaga {
        scope_key: row.get(0)?,
        codex_thread_id: row.get(2)?,
        generation,
        state,
        outcome,
        created_ms: row.get(5)?,
        updated_ms: row.get(6)?,
    })
}

fn generation_i64(generation: u64) -> Result<i64, StoreError> {
    i64::try_from(generation).map_err(|_| StoreError::CapacityExceeded {
        context: "encoding a thread adoption generation",
    })
}
