//! `user_version`-driven schema migrations for the bridge store.
//!
//! Each migration runs in its own transaction inside the single writer task
//! before any request is served. Milestone-4 tables (`approvals`,
//! `callback_nonces`, `workspace_aliases`) are deliberately absent.

/// One `user_version` migration step.
pub struct Migration {
    /// The `user_version` value once this migration has been applied.
    pub version: u32,
    /// Human-readable migration name used in logs and errors.
    pub name: &'static str,
    /// DDL executed atomically in one transaction.
    pub sql: &'static str,
}

/// All migrations in ascending `user_version` order.
pub const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "initial bridge schema",
    sql: "
CREATE TABLE inbound_events (
    tenant TEXT NOT NULL,
    event_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('received', 'accepted', 'completed', 'rejected')),
    first_seen_ms INTEGER NOT NULL,
    updated_ms INTEGER NOT NULL,
    rejection_reason TEXT,
    PRIMARY KEY (tenant, event_id)
);
CREATE INDEX inbound_events_message_id ON inbound_events (message_id);

CREATE TABLE scopes (
    scope_key TEXT PRIMARY KEY,
    cwd TEXT NOT NULL,
    policy_fingerprint TEXT NOT NULL,
    updated_ms INTEGER NOT NULL
);

CREATE TABLE threads (
    scope_key TEXT NOT NULL,
    codex_thread_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'archived')),
    created_ms INTEGER NOT NULL,
    archived_ms INTEGER,
    PRIMARY KEY (scope_key, codex_thread_id)
);
CREATE UNIQUE INDEX threads_one_active_per_scope
    ON threads (scope_key) WHERE status = 'active';

CREATE TABLE turns (
    id INTEGER PRIMARY KEY,
    scope_key TEXT NOT NULL,
    client_message_id TEXT NOT NULL UNIQUE,
    codex_thread_id TEXT,
    codex_turn_id TEXT,
    state TEXT NOT NULL CHECK (state IN ('starting', 'running', 'completed', 'failed', 'interrupted', 'uncertain')),
    uncertain INTEGER NOT NULL DEFAULT 0,
    created_ms INTEGER NOT NULL,
    updated_ms INTEGER NOT NULL
);
CREATE INDEX turns_scope ON turns (scope_key);

CREATE TABLE outbox (
    id INTEGER PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    scope_key TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_bytes INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'sending', 'sent', 'failed', 'uncertain_delivery')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_retry_ms INTEGER NOT NULL DEFAULT 0,
    receipt_message_id TEXT,
    created_ms INTEGER NOT NULL,
    updated_ms INTEGER NOT NULL
);
CREATE INDEX outbox_pending_retry ON outbox (state, next_retry_ms);

CREATE TABLE attachments (
    sha256 TEXT PRIMARY KEY,
    bytes INTEGER NOT NULL,
    kind TEXT NOT NULL,
    created_ms INTEGER NOT NULL,
    last_used_ms INTEGER NOT NULL
);

CREATE TABLE attachment_leases (
    sha256 TEXT NOT NULL,
    turn_row_id INTEGER NOT NULL,
    created_ms INTEGER NOT NULL,
    PRIMARY KEY (sha256, turn_row_id),
    FOREIGN KEY (sha256) REFERENCES attachments (sha256) ON DELETE CASCADE,
    FOREIGN KEY (turn_row_id) REFERENCES turns (id) ON DELETE CASCADE
);
",
}];
