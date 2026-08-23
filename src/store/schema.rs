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
pub const MIGRATIONS: &[Migration] = &[
    Migration {
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
    },
    Migration {
        version: 2,
        name: "replayable inbound inbox",
        sql: "
ALTER TABLE inbound_events ADD COLUMN payload_version INTEGER;
ALTER TABLE inbound_events ADD COLUMN payload_blob BLOB;
ALTER TABLE inbound_events ADD COLUMN payload_bytes INTEGER NOT NULL DEFAULT 0
    CHECK (payload_bytes >= 0);
ALTER TABLE inbound_events ADD COLUMN turn_row_id INTEGER
    REFERENCES turns(id) ON DELETE RESTRICT;
ALTER TABLE turns ADD COLUMN inbound_count INTEGER NOT NULL DEFAULT 0
    CHECK (inbound_count >= 0);

CREATE INDEX inbound_events_tenant_message_state
    ON inbound_events (tenant, message_id, state);
CREATE INDEX inbound_events_turn_row ON inbound_events (turn_row_id);
CREATE INDEX inbound_events_terminal_sweep
    ON inbound_events (state, updated_ms, tenant, event_id);

CREATE TRIGGER inbound_events_v2_shape_insert
BEFORE INSERT ON inbound_events
WHEN
    (NEW.state = 'received' AND (
        NEW.turn_row_id IS NOT NULL OR NEW.payload_version IS NOT 1 OR
        NEW.payload_blob IS NULL OR
        NEW.payload_bytes IS NOT length(NEW.payload_blob)
    ))
 OR (NEW.state = 'accepted' AND (
        NEW.turn_row_id IS NULL OR NEW.payload_version IS NOT 1 OR
        NEW.payload_blob IS NULL OR
        NEW.payload_bytes IS NOT length(NEW.payload_blob) OR NOT
        EXISTS (
            SELECT 1 FROM turns
            WHERE id = NEW.turn_row_id AND scope_key = NEW.scope_key
        )
    ))
 OR (NEW.state IN ('completed', 'rejected') AND (
        NEW.payload_version IS NOT NULL OR NEW.payload_blob IS NOT NULL OR
        NEW.payload_bytes IS NOT 0 OR (
            NEW.turn_row_id IS NOT NULL AND NOT EXISTS (
              SELECT 1 FROM turns
              WHERE id = NEW.turn_row_id AND scope_key = NEW.scope_key
                AND (state IN ('completed', 'failed', 'interrupted')
                     OR (state = 'uncertain' AND uncertain = 0))
            )
        )
    ))
BEGIN
    SELECT RAISE(ABORT, 'invalid inbound v2 shape');
END;

CREATE TRIGGER inbound_events_v2_shape_update
BEFORE UPDATE ON inbound_events
WHEN
    (NEW.state = 'received' AND (
        NEW.turn_row_id IS NOT NULL OR NEW.payload_version IS NOT 1 OR
        NEW.payload_blob IS NULL OR
        NEW.payload_bytes IS NOT length(NEW.payload_blob)
    ))
 OR (NEW.state = 'accepted' AND (
        NEW.turn_row_id IS NULL OR NEW.payload_version IS NOT 1 OR
        NEW.payload_blob IS NULL OR
        NEW.payload_bytes IS NOT length(NEW.payload_blob) OR NOT
        EXISTS (
            SELECT 1 FROM turns
            WHERE id = NEW.turn_row_id AND scope_key = NEW.scope_key
        )
    ))
 OR (NEW.state IN ('completed', 'rejected') AND (
        NEW.payload_version IS NOT NULL OR NEW.payload_blob IS NOT NULL OR
        NEW.payload_bytes IS NOT 0 OR (
            NEW.turn_row_id IS NOT NULL AND NOT EXISTS (
              SELECT 1 FROM turns
              WHERE id = NEW.turn_row_id AND scope_key = NEW.scope_key
                AND (state IN ('completed', 'failed', 'interrupted')
                     OR (state = 'uncertain' AND uncertain = 0))
            )
        )
    ))
BEGIN
    SELECT RAISE(ABORT, 'invalid inbound v2 shape');
END;
",
    },
    Migration {
        version: 3,
        name: "attachment scan cursor",
        sql: "
CREATE TABLE attachment_scan_cursor (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    entry_name TEXT NOT NULL
);
",
    },
    Migration {
        version: 4,
        name: "remove obsolete attachment scan cursor",
        sql: "DROP TABLE attachment_scan_cursor;",
    },
    Migration {
        version: 5,
        name: "track bridge context tools on threads",
        sql: "
ALTER TABLE threads ADD COLUMN context_tools_version INTEGER NOT NULL DEFAULT 0
    CHECK (context_tools_version >= 0);
",
    },
    Migration {
        version: 6,
        name: "external Codex reconciliation epochs",
        sql: "
CREATE TABLE external_endpoint_epochs (
    endpoint_label TEXT PRIMARY KEY,
    current_epoch INTEGER NOT NULL CHECK (current_epoch > 0),
    state TEXT NOT NULL CHECK (state IN ('connecting', 'reconciling', 'ready', 'unavailable', 'stopped')),
    updated_ms INTEGER NOT NULL
);

CREATE TABLE external_managed_threads (
    endpoint_label TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    epoch INTEGER NOT NULL CHECK (epoch >= 0),
    state TEXT NOT NULL CHECK (state IN ('unavailable', 'reconciling', 'ready', 'uncertain')),
    reason TEXT CHECK (reason IS NULL OR reason IN (
        'bridge_restart', 'socket_disconnect', 'request_timeout', 'buffer_overflow',
        'page_limit', 'server_restart', 'protocol_violation', 'conflicting_terminal'
    )),
    updated_ms INTEGER NOT NULL,
    PRIMARY KEY (endpoint_label, thread_id),
    FOREIGN KEY (endpoint_label) REFERENCES external_endpoint_epochs(endpoint_label)
        ON DELETE CASCADE
);

CREATE TABLE external_turn_terminals (
    endpoint_label TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('completed', 'failed', 'interrupted')),
    observed_epoch INTEGER NOT NULL CHECK (observed_epoch > 0),
    PRIMARY KEY (endpoint_label, thread_id, turn_id),
    FOREIGN KEY (endpoint_label, thread_id)
        REFERENCES external_managed_threads(endpoint_label, thread_id) ON DELETE CASCADE
);

CREATE TABLE external_item_terminals (
    endpoint_label TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    item_id TEXT NOT NULL,
    observed_epoch INTEGER NOT NULL CHECK (observed_epoch > 0),
    PRIMARY KEY (endpoint_label, thread_id, turn_id, item_id),
    FOREIGN KEY (endpoint_label, thread_id)
        REFERENCES external_managed_threads(endpoint_label, thread_id) ON DELETE CASCADE
);

CREATE INDEX external_managed_threads_state
    ON external_managed_threads(endpoint_label, state, thread_id);
",
    },
    Migration {
        version: 7,
        name: "external Codex write and approval fences",
        sql: "
CREATE TABLE external_write_fences (
    endpoint_label TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    epoch INTEGER NOT NULL CHECK (epoch > 0),
    state TEXT NOT NULL CHECK (state IN ('open', 'active', 'uncertain')),
    active_intent_id TEXT,
    approval_actor TEXT NOT NULL,
    updated_ms INTEGER NOT NULL,
    PRIMARY KEY (endpoint_label, thread_id),
    FOREIGN KEY (endpoint_label, thread_id)
        REFERENCES external_managed_threads(endpoint_label, thread_id) ON DELETE CASCADE,
    CHECK ((state = 'active') = (active_intent_id IS NOT NULL))
);

CREATE TABLE external_mutation_intents (
    endpoint_label TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    intent_id TEXT NOT NULL,
    epoch INTEGER NOT NULL CHECK (epoch > 0),
    kind TEXT NOT NULL CHECK (kind IN (
        'turn_start', 'turn_steer', 'turn_interrupt', 'queue_add', 'queue_start'
    )),
    expected_turn_id TEXT,
    client_message_id TEXT,
    source_actor TEXT NOT NULL,
    client_actor TEXT NOT NULL,
    approval_actor TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN (
        'prepared', 'sent', 'applied', 'rejected', 'uncertain'
    )),
    result_id TEXT,
    created_ms INTEGER NOT NULL,
    updated_ms INTEGER NOT NULL,
    PRIMARY KEY (endpoint_label, thread_id, intent_id),
    FOREIGN KEY (endpoint_label, thread_id)
        REFERENCES external_managed_threads(endpoint_label, thread_id) ON DELETE CASCADE
);
CREATE INDEX external_mutation_intents_state
    ON external_mutation_intents(endpoint_label, thread_id, state, updated_ms);

CREATE TABLE external_approval_claims (
    endpoint_label TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    approval_id TEXT NOT NULL,
    request_key TEXT NOT NULL,
    epoch INTEGER NOT NULL CHECK (epoch > 0),
    turn_id TEXT NOT NULL,
    item_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('command', 'file_change', 'permissions')),
    source_actor TEXT NOT NULL,
    client_actor TEXT NOT NULL,
    approval_actor TEXT NOT NULL,
    recipient_actor TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN (
        'received', 'claimed', 'responding', 'resolved', 'denied', 'uncertain'
    )),
    deadline_ms INTEGER NOT NULL,
    created_ms INTEGER NOT NULL,
    updated_ms INTEGER NOT NULL,
    PRIMARY KEY (endpoint_label, thread_id, approval_id),
    UNIQUE (endpoint_label, epoch, request_key),
    FOREIGN KEY (endpoint_label, thread_id)
        REFERENCES external_managed_threads(endpoint_label, thread_id) ON DELETE CASCADE
);
CREATE INDEX external_approval_claims_state
    ON external_approval_claims(endpoint_label, thread_id, state, deadline_ms);
",
    },
];
