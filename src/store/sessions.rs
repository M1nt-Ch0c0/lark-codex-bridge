#![allow(clippy::doc_markdown)]

//! Scope, thread, and turn rows plus their queries (design §8).
//!
//! A scope maps to at most one `active` Codex thread at a time (enforced by
//! a partial unique index); `/new` and `/cd` archive it first. Turn rows
//! are recorded with the bridge-generated `client_message_id` *before* the
//! `turn/start` RPC so a crash mid-call leaves an `uncertain` row instead
//! of a silent gap.

use std::path::{Path, PathBuf};

use rusqlite::params;

use super::{StoreError, StoreHandle, now_ms, query_optional, request_bytes, sqlite_error};
use crate::lark::normalize::ScopeKey;
use crate::limits::{STORE_RECOVERY_TURN_MAX_BYTES, STORE_RECOVERY_TURN_MAX_ROWS};

/// Lifecycle status of a scope→thread mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadStatus {
    /// The thread Codex work for this scope currently resumes into.
    Active,
    /// Retired by `/new` or `/cd`; kept for history, never resumed.
    Archived,
}

impl ThreadStatus {
    /// Stable database representation.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Archived => "archived",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        match text {
            "active" => Some(Self::Active),
            "archived" => Some(Self::Archived),
            _ => None,
        }
    }
}

/// Turn lifecycle states (design §7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnState {
    /// Row recorded before `turn/start`; the RPC has not confirmed yet.
    Starting,
    /// `turn/start` accepted by the app-server.
    Running,
    /// Finished cleanly (terminal).
    Completed,
    /// Finished with an error (terminal).
    Failed,
    /// Interrupted via `/stop` and recovered (terminal).
    Interrupted,
    /// The `turn/start` outcome is unknown (connection lost mid-call);
    /// never blindly resent. Recovery resolves this to a terminal state.
    Uncertain,
}

impl TurnState {
    /// Stable database representation.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Interrupted => "interrupted",
            Self::Uncertain => "uncertain",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        match text {
            "starting" => Some(Self::Starting),
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "interrupted" => Some(Self::Interrupted),
            "uncertain" => Some(Self::Uncertain),
            _ => None,
        }
    }
}

/// One row of the `scopes` table.
#[derive(Clone, PartialEq, Eq)]
pub struct ScopeRow {
    /// Scope key (`im:<chat_id>` / `im:<chat_id>:thread:<thread_id>`).
    pub scope_key: String,
    /// Canonical workspace path for the scope.
    pub cwd: PathBuf,
    /// Policy fingerprint active when the row was written.
    pub policy_fingerprint: String,
    /// Last update, milliseconds since the Unix epoch.
    pub updated_ms: i64,
}

impl std::fmt::Debug for ScopeRow {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ScopeRow")
            .field("scope_key", &self.scope_key)
            .field("cwd_len", &self.cwd.to_string_lossy().len())
            .field("policy_fingerprint", &self.policy_fingerprint)
            .field("updated_ms", &self.updated_ms)
            .finish_non_exhaustive()
    }
}

/// One row of the `threads` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadRow {
    /// Owning scope key.
    pub scope_key: String,
    /// Codex app-server thread ID.
    pub codex_thread_id: String,
    /// Lifecycle status.
    pub status: ThreadStatus,
    /// Creation time, milliseconds since the Unix epoch.
    pub created_ms: i64,
    /// Archive time, milliseconds since the Unix epoch.
    pub archived_ms: Option<i64>,
}

/// One row of the `turns` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnRow {
    /// Row ID.
    pub id: i64,
    /// Owning scope key.
    pub scope_key: String,
    /// Bridge-generated idempotency ID sent as `client_user_message_id`.
    pub client_message_id: String,
    /// Codex thread the turn ran on, once known.
    pub codex_thread_id: Option<String>,
    /// Codex turn ID, once `turn/start` returned it.
    pub codex_turn_id: Option<String>,
    /// Lifecycle state.
    pub state: TurnState,
    /// Whether the `turn/start` outcome is unknown.
    pub uncertain: bool,
    /// Creation time, milliseconds since the Unix epoch.
    pub created_ms: i64,
    /// Last update, milliseconds since the Unix epoch.
    pub updated_ms: i64,
}

/// Fields needed to record a new turn row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTurnRow {
    /// Owning scope key.
    pub scope_key: String,
    /// Bridge-generated idempotency ID (unique).
    pub client_message_id: String,
    /// Codex thread the turn will run on, when already known.
    pub codex_thread_id: Option<String>,
    /// Initial state (normally [`TurnState::Starting`]).
    pub state: TurnState,
}

impl StoreHandle {
    /// Inserts or updates the scope row (cwd + policy fingerprint).
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or SQLite fails.
    pub async fn upsert_scope(
        &self,
        scope: &ScopeKey,
        cwd: &Path,
        fingerprint: &str,
    ) -> Result<(), StoreError> {
        let scope_key = scope.to_string();
        let cwd = cwd
            .to_str()
            .ok_or(StoreError::InvalidPath {
                context: "persisting a non-UTF-8 workspace path",
            })?
            .to_owned();
        let fingerprint = fingerprint.to_owned();
        let request_size = request_bytes(&[&scope_key, &cwd, &fingerprint]);
        self.run_sized(request_size, move |connection| {
            connection
                .execute(
                    "INSERT INTO scopes (scope_key, cwd, policy_fingerprint, updated_ms)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT (scope_key) DO UPDATE SET
                         cwd = excluded.cwd,
                         policy_fingerprint = excluded.policy_fingerprint,
                         updated_ms = excluded.updated_ms",
                    params![scope_key, cwd, fingerprint, now_ms()],
                )
                .map_err(|error| sqlite_error("upserting a scope", &error))?;
            Ok(())
        })
        .await
    }

    /// Reads the scope row, when present.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or SQLite fails.
    pub async fn scope_row(&self, scope: &ScopeKey) -> Result<Option<ScopeRow>, StoreError> {
        let scope_key = scope.to_string();
        let request_size = request_bytes(&[&scope_key]);
        self.run_sized(request_size, move |connection| {
            let row = connection.query_row(
                "SELECT scope_key, cwd, policy_fingerprint, updated_ms
                 FROM scopes WHERE scope_key = ?1",
                params![scope_key],
                |row| {
                    Ok(ScopeRow {
                        scope_key: row.get(0)?,
                        cwd: PathBuf::from(row.get::<_, String>(1)?),
                        policy_fingerprint: row.get(2)?,
                        updated_ms: row.get(3)?,
                    })
                },
            );
            query_optional(row, "reading a scope row")
        })
        .await
    }

    /// Records a new active thread mapping for the scope.
    ///
    /// # Errors
    ///
    /// Returns an error when the scope already has an active thread or the
    /// mapping already exists.
    pub async fn record_active_thread(
        &self,
        scope: &ScopeKey,
        codex_thread_id: &str,
    ) -> Result<(), StoreError> {
        let scope_key = scope.to_string();
        let codex_thread_id = codex_thread_id.to_owned();
        let request_size = request_bytes(&[&scope_key, &codex_thread_id]);
        self.run_sized(request_size, move |connection| {
            connection
                .execute(
                    "INSERT INTO threads (scope_key, codex_thread_id, status, created_ms)
                     VALUES (?1, ?2, 'active', ?3)",
                    params![scope_key, codex_thread_id, now_ms()],
                )
                .map_err(|error| sqlite_error("recording an active thread", &error))?;
            Ok(())
        })
        .await
    }

    /// Reads the scope's active thread mapping, when present.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or SQLite fails.
    pub async fn active_thread(&self, scope: &ScopeKey) -> Result<Option<ThreadRow>, StoreError> {
        let scope_key = scope.to_string();
        let request_size = request_bytes(&[&scope_key]);
        self.run_sized(request_size, move |connection| {
            let row = connection.query_row(
                "SELECT scope_key, codex_thread_id, status, created_ms, archived_ms
                 FROM threads WHERE scope_key = ?1 AND status = 'active'",
                params![scope_key],
                read_thread_row,
            );
            query_optional(row, "reading the active thread")
        })
        .await
    }

    /// Archives the scope's active thread, returning the archived row.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or SQLite fails.
    pub async fn archive_active_thread(
        &self,
        scope: &ScopeKey,
    ) -> Result<Option<ThreadRow>, StoreError> {
        let scope_key = scope.to_string();
        let request_size = request_bytes(&[&scope_key]);
        self.run_sized(request_size, move |connection| {
            let now = now_ms();
            let active = connection.query_row(
                "SELECT scope_key, codex_thread_id, status, created_ms, archived_ms
                 FROM threads WHERE scope_key = ?1 AND status = 'active'",
                params![scope_key],
                read_thread_row,
            );
            let Some(mut row) = query_optional(active, "reading the active thread to archive")?
            else {
                return Ok(None);
            };
            connection
                .execute(
                    "UPDATE threads SET status = 'archived', archived_ms = ?3
                     WHERE scope_key = ?1 AND codex_thread_id = ?2 AND status = 'active'",
                    params![row.scope_key, row.codex_thread_id, now],
                )
                .map_err(|error| sqlite_error("archiving the active thread", &error))?;
            row.status = ThreadStatus::Archived;
            row.archived_ms = Some(now);
            Ok(Some(row))
        })
        .await
    }

    /// Records a new turn row, returning its row ID.
    ///
    /// # Errors
    ///
    /// Returns an error when `client_message_id` already exists or SQLite
    /// fails.
    pub async fn record_turn(&self, row: NewTurnRow) -> Result<i64, StoreError> {
        if row.state != TurnState::Starting {
            return Err(StoreError::InvalidTransition {
                context: "creating a turn outside the starting state",
            });
        }
        let request_size = request_bytes(&[
            &row.scope_key,
            &row.client_message_id,
            row.codex_thread_id.as_deref().unwrap_or_default(),
        ]);
        self.run_sized(request_size, move |connection| {
            let now = now_ms();
            let transaction = connection
                .transaction()
                .map_err(|error| sqlite_error("starting a turn transaction", &error))?;
            let (count, bytes) = recovery_usage(&transaction, None)?;
            let row_bytes = turn_bytes(
                &row.scope_key,
                &row.client_message_id,
                row.codex_thread_id.as_deref(),
                None,
            );
            ensure_recovery_capacity(count, bytes, row_bytes, "recording a live turn")?;
            transaction
                .execute(
                    "INSERT INTO turns
                     (scope_key, client_message_id, codex_thread_id, state, uncertain, created_ms, updated_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)",
                    params![
                        row.scope_key,
                        row.client_message_id,
                        row.codex_thread_id,
                        TurnState::Starting.as_str(),
                        false,
                        now,
                    ],
                )
                .map_err(|error| sqlite_error("recording a turn", &error))?;
            let id = transaction.last_insert_rowid();
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing a turn transaction", &error))?;
            Ok(id)
        })
        .await
    }

    /// Transitions one turn along the legal state machine, optionally
    /// recording the Codex turn ID.
    ///
    /// Legal transitions: `starting → running|failed|interrupted|uncertain`,
    /// `running → completed|failed|interrupted|uncertain`, and
    /// `uncertain → completed|failed|interrupted` (recovery resolution).
    /// Terminal states are final.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`] for an unknown row and
    /// [`StoreError::InvalidTransition`] for any other transition.
    pub async fn set_turn_state(
        &self,
        id: i64,
        state: TurnState,
        codex_turn_id: Option<&str>,
    ) -> Result<(), StoreError> {
        let codex_turn_id = codex_turn_id.map(str::to_owned);
        let request_size = request_bytes(&[codex_turn_id.as_deref().unwrap_or_default()]);
        self.run_sized(request_size, move |connection| {
            let (current, scope_key, client_message_id, current_thread_id, current_turn_id): (
                String,
                String,
                String,
                Option<String>,
                Option<String>,
            ) = connection
                .query_row(
                    "SELECT state, scope_key, client_message_id, codex_thread_id, codex_turn_id
                     FROM turns WHERE id = ?1",
                    params![id],
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
                .map_err(|error| match error {
                    rusqlite::Error::QueryReturnedNoRows => StoreError::NotFound {
                        context: "transitioning an unknown turn",
                    },
                    other => sqlite_error("reading a turn state", &other),
                })?;
            let from = TurnState::parse(&current).ok_or(StoreError::Sqlite {
                context: "decoding a turn state",
                code: None,
            })?;
            if !legal_turn_transition(from, state) {
                return Err(StoreError::InvalidTransition {
                    context: "transitioning a turn",
                });
            }
            let transaction = connection
                .transaction()
                .map_err(|error| sqlite_error("starting a turn transition", &error))?;
            if matches!(state, TurnState::Running | TurnState::Uncertain) {
                let (count, bytes) = recovery_usage(&transaction, Some(id))?;
                let resulting_turn_id = codex_turn_id.as_deref().or(current_turn_id.as_deref());
                let row_bytes = turn_bytes(
                    &scope_key,
                    &client_message_id,
                    current_thread_id.as_deref(),
                    resulting_turn_id,
                );
                ensure_recovery_capacity(count, bytes, row_bytes, "transitioning a live turn")?;
            }
            transaction
                .execute(
                    "UPDATE turns SET state = ?2, uncertain = ?3, updated_ms = ?4,
                         codex_turn_id = COALESCE(?5, codex_turn_id)
                     WHERE id = ?1",
                    params![
                        id,
                        state.as_str(),
                        state == TurnState::Uncertain,
                        now_ms(),
                        codex_turn_id
                    ],
                )
                .map_err(|error| sqlite_error("transitioning a turn", &error))?;
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing a turn transition", &error))?;
            Ok(())
        })
        .await
    }

    /// Lists all bounded live turns (`starting`, `running`, and `uncertain`)
    /// requiring crash recovery. The stored live-turn invariant caps both
    /// returned row count and identifier bytes; corrupt legacy data that
    /// exceeds either cap fails closed with [`StoreError::CapacityExceeded`].
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or SQLite fails.
    pub async fn uncertain_turns(&self) -> Result<Vec<TurnRow>, StoreError> {
        self.run(|connection| {
            let (count, bytes) = recovery_usage(connection, None)?;
            if count > STORE_RECOVERY_TURN_MAX_ROWS || bytes > STORE_RECOVERY_TURN_MAX_BYTES {
                return Err(StoreError::CapacityExceeded {
                    context: "reading live turn recovery rows",
                });
            }
            let mut statement = connection
                .prepare(
                    "SELECT id, scope_key, client_message_id, codex_thread_id, codex_turn_id,
                            state, uncertain, created_ms, updated_ms
                     FROM turns
                     WHERE state IN ('starting', 'running', 'uncertain')
                     ORDER BY id LIMIT ?1",
                )
                .map_err(|error| sqlite_error("listing uncertain turns", &error))?;
            let rows = statement
                .query_map(
                    params![i64::try_from(STORE_RECOVERY_TURN_MAX_ROWS).unwrap_or(i64::MAX)],
                    read_turn_row,
                )
                .map_err(|error| sqlite_error("listing uncertain turns", &error))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| sqlite_error("listing uncertain turns", &error))?;
            Ok(rows)
        })
        .await
    }

    /// Reads one turn row by ID.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or SQLite fails.
    pub async fn turn_row(&self, id: i64) -> Result<Option<TurnRow>, StoreError> {
        self.run(move |connection| {
            let row = connection.query_row(
                "SELECT id, scope_key, client_message_id, codex_thread_id, codex_turn_id,
                        state, uncertain, created_ms, updated_ms
                 FROM turns WHERE id = ?1",
                params![id],
                read_turn_row,
            );
            query_optional(row, "reading a turn row")
        })
        .await
    }
}

fn recovery_usage(
    connection: &rusqlite::Connection,
    exclude_id: Option<i64>,
) -> Result<(usize, usize), StoreError> {
    let (count, bytes): (i64, i64) = connection
        .query_row(
            "SELECT COUNT(*), COALESCE(SUM(
                 LENGTH(CAST(scope_key AS BLOB)) + LENGTH(CAST(client_message_id AS BLOB)) +
                 COALESCE(LENGTH(CAST(codex_thread_id AS BLOB)), 0) +
                 COALESCE(LENGTH(CAST(codex_turn_id AS BLOB)), 0)
             ), 0)
             FROM turns
             WHERE state IN ('starting', 'running', 'uncertain')
               AND (?1 IS NULL OR id != ?1)",
            params![exclude_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| sqlite_error("reading live turn recovery usage", &error))?;
    Ok((
        usize::try_from(count).unwrap_or(usize::MAX),
        usize::try_from(bytes).unwrap_or(usize::MAX),
    ))
}

fn turn_bytes(
    scope_key: &str,
    client_message_id: &str,
    codex_thread_id: Option<&str>,
    codex_turn_id: Option<&str>,
) -> usize {
    request_bytes(&[
        scope_key,
        client_message_id,
        codex_thread_id.unwrap_or_default(),
        codex_turn_id.unwrap_or_default(),
    ])
}

fn ensure_recovery_capacity(
    count: usize,
    bytes: usize,
    additional_bytes: usize,
    context: &'static str,
) -> Result<(), StoreError> {
    if count >= STORE_RECOVERY_TURN_MAX_ROWS
        || bytes.saturating_add(additional_bytes) > STORE_RECOVERY_TURN_MAX_BYTES
    {
        return Err(StoreError::CapacityExceeded { context });
    }
    Ok(())
}

fn read_thread_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ThreadRow> {
    let status: String = row.get(2)?;
    Ok(ThreadRow {
        scope_key: row.get(0)?,
        codex_thread_id: row.get(1)?,
        status: ThreadStatus::parse(&status).ok_or_else(|| {
            rusqlite::Error::InvalidColumnType(2, "status".to_owned(), rusqlite::types::Type::Text)
        })?,
        created_ms: row.get(3)?,
        archived_ms: row.get(4)?,
    })
}

fn read_turn_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TurnRow> {
    let state: String = row.get(5)?;
    Ok(TurnRow {
        id: row.get(0)?,
        scope_key: row.get(1)?,
        client_message_id: row.get(2)?,
        codex_thread_id: row.get(3)?,
        codex_turn_id: row.get(4)?,
        state: TurnState::parse(&state).ok_or_else(|| {
            rusqlite::Error::InvalidColumnType(5, "state".to_owned(), rusqlite::types::Type::Text)
        })?,
        uncertain: row.get::<_, i64>(6)? != 0,
        created_ms: row.get(7)?,
        updated_ms: row.get(8)?,
    })
}

fn legal_turn_transition(from: TurnState, to: TurnState) -> bool {
    use TurnState::{Completed, Failed, Interrupted, Running, Starting, Uncertain};
    match from {
        Starting => matches!(to, Running | Failed | Interrupted | Uncertain),
        Running => matches!(to, Completed | Failed | Interrupted | Uncertain),
        Uncertain => matches!(to, Completed | Failed | Interrupted),
        Completed | Failed | Interrupted => false,
    }
}
