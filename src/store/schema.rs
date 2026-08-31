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
        name: "tokenize attachment lease acquisitions",
        sql: "
ALTER TABLE attachment_leases RENAME TO attachment_leases_v1;

CREATE TABLE attachment_leases (
    lease_token TEXT PRIMARY KEY CHECK (
        length(lease_token) BETWEEN 1 AND 64
    ),
    sha256 TEXT NOT NULL,
    turn_row_id INTEGER NOT NULL,
    created_ms INTEGER NOT NULL,
    FOREIGN KEY (sha256) REFERENCES attachments (sha256) ON DELETE CASCADE,
    FOREIGN KEY (turn_row_id) REFERENCES turns (id) ON DELETE CASCADE
);
CREATE INDEX attachment_leases_sha256 ON attachment_leases (sha256);
CREATE INDEX attachment_leases_turn ON attachment_leases (turn_row_id);

INSERT INTO attachment_leases (lease_token, sha256, turn_row_id, created_ms)
SELECT printf('legacy-%016x', rowid), sha256, turn_row_id, created_ms
FROM attachment_leases_v1;

DROP TABLE attachment_leases_v1;
",
    },
    Migration {
        version: 7,
        name: "fence versioned Markdown outbox payloads",
        // No table shape changes are needed: outbox payloads are deliberately
        // opaque JSON. Advancing `user_version` is nevertheless required so a
        // v1-only binary refuses to open a database after this binary may have
        // persisted payload v2 rows it cannot understand.
        sql: "SELECT 1;",
    },
    Migration {
        version: 8,
        name: "remove durable media capabilities and transcripts",
        // The data rewrite is implemented by the writer immediately before
        // this marker migration because it must decode and validate the
        // versioned application payload rather than mutate JSON in SQL.
        sql: "SELECT 1;",
    },
    Migration {
        version: 9,
        name: "external Codex reconciliation epochs",
        sql: "
CREATE TABLE IF NOT EXISTS external_endpoint_epochs (
    endpoint_label TEXT PRIMARY KEY,
    current_epoch INTEGER NOT NULL CHECK (current_epoch > 0),
    state TEXT NOT NULL CHECK (state IN ('connecting', 'reconciling', 'ready', 'unavailable', 'stopped')),
    updated_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS external_managed_threads (
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

CREATE TABLE IF NOT EXISTS external_turn_terminals (
    endpoint_label TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('completed', 'failed', 'interrupted')),
    observed_epoch INTEGER NOT NULL CHECK (observed_epoch > 0),
    PRIMARY KEY (endpoint_label, thread_id, turn_id),
    FOREIGN KEY (endpoint_label, thread_id)
        REFERENCES external_managed_threads(endpoint_label, thread_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS external_item_terminals (
    endpoint_label TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    item_id TEXT NOT NULL,
    observed_epoch INTEGER NOT NULL CHECK (observed_epoch > 0),
    PRIMARY KEY (endpoint_label, thread_id, turn_id, item_id),
    FOREIGN KEY (endpoint_label, thread_id)
        REFERENCES external_managed_threads(endpoint_label, thread_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS external_managed_threads_state
    ON external_managed_threads(endpoint_label, state, thread_id);
",
    },
    Migration {
        version: 10,
        name: "external Codex write and approval fences",
        sql: "
CREATE TABLE IF NOT EXISTS external_write_fences (
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

CREATE TABLE IF NOT EXISTS external_mutation_intents (
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
CREATE INDEX IF NOT EXISTS external_mutation_intents_state
    ON external_mutation_intents(endpoint_label, thread_id, state, updated_ms);

CREATE TABLE IF NOT EXISTS external_approval_claims (
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
CREATE INDEX IF NOT EXISTS external_approval_claims_state
    ON external_approval_claims(endpoint_label, thread_id, state, deadline_ms);
",
    },
    Migration {
        version: 11,
        name: "durable persisted thread adoption saga",
        sql: "
-- Versions through v10 allowed the same active Codex thread ID to be mapped by
-- multiple scopes. There is no durable ownership fact that can select one of
-- those scopes safely. Archive every active row in each ambiguous set, keeping
-- all history while ensuring that no scope inherits write authority merely by
-- row order. The migration transaction makes this rewrite and the unique index
-- atomic. Active rows normally have no archived_ms, so created_ms is the stable
-- deterministic fallback for historical stores.
UPDATE threads
SET status = 'archived', archived_ms = COALESCE(archived_ms, created_ms)
WHERE status = 'active' AND codex_thread_id IN (
    SELECT codex_thread_id
    FROM threads
    WHERE status = 'active'
    GROUP BY codex_thread_id
    HAVING COUNT(*) > 1
);

ALTER TABLE threads ADD COLUMN origin TEXT NOT NULL DEFAULT 'bridge_created'
    CHECK (origin IN ('bridge_created', 'externally_adopted'));
ALTER TABLE threads ADD COLUMN adoption_generation INTEGER
    CHECK (adoption_generation IS NULL OR adoption_generation > 0);

CREATE UNIQUE INDEX threads_one_active_thread_id
    ON threads (codex_thread_id) WHERE status = 'active';

CREATE TABLE thread_adoption_sagas (
    scope_key TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    codex_thread_id TEXT NOT NULL CHECK (length(codex_thread_id) > 0),
    state TEXT NOT NULL CHECK (state IN (
        'acquiring', 'owned', 'releasing', 'recovery_required',
        'release_failed', 'terminal'
    )),
    outcome TEXT CHECK (outcome IS NULL OR outcome IN (
        'acquisition_failed', 'released'
    )),
    created_ms INTEGER NOT NULL,
    updated_ms INTEGER NOT NULL,
    PRIMARY KEY (scope_key),
    FOREIGN KEY (scope_key) REFERENCES scopes(scope_key) ON DELETE RESTRICT,
    CHECK (
        (state = 'terminal' AND outcome IS NOT NULL)
        OR (state != 'terminal' AND outcome IS NULL)
    )
);

CREATE UNIQUE INDEX thread_adoption_one_live_per_thread
    ON thread_adoption_sagas (codex_thread_id) WHERE state != 'terminal';
CREATE INDEX thread_adoption_recovery_scan
    ON thread_adoption_sagas (state, updated_ms, scope_key, generation);

CREATE TRIGGER threads_adoption_shape_insert
BEFORE INSERT ON threads
WHEN
    (NEW.origin = 'bridge_created' AND NEW.adoption_generation IS NOT NULL)
 OR (NEW.origin = 'externally_adopted' AND NEW.adoption_generation IS NULL)
BEGIN
    SELECT RAISE(ABORT, 'invalid thread adoption shape');
END;

CREATE TRIGGER threads_adoption_shape_update
BEFORE UPDATE OF origin, adoption_generation ON threads
WHEN
    (NEW.origin = 'bridge_created' AND NEW.adoption_generation IS NOT NULL)
 OR (NEW.origin = 'externally_adopted' AND NEW.adoption_generation IS NULL)
BEGIN
    SELECT RAISE(ABORT, 'invalid thread adoption shape');
END;
",
    },
    Migration {
        version: 12,
        name: "durable inbound reply effect marker",
        sql: "
-- The marker duplicates bounded tenant/event identity bytes. Guard the exact
-- post-migration logical collection before changing the table so an otherwise
-- valid older store cannot advance user_version and only then fail recovery.
-- These four limits mirror STORE_INBOUND_{MAX,RECEIVED_MAX}_{ROWS,BYTES}.
-- The guard table and every later v12 change share the migration transaction;
-- a CHECK failure therefore preserves both the old schema and user_version.
CREATE TABLE inbound_reply_effect_v12_guard (
    valid INTEGER NOT NULL CHECK (valid = 1)
);
INSERT INTO inbound_reply_effect_v12_guard (valid)
SELECT CASE WHEN
    COUNT(*) <= 65536
    AND COALESCE(SUM(
        LENGTH(CAST(tenant AS BLOB)) + LENGTH(CAST(event_id AS BLOB)) +
        LENGTH(CAST(message_id AS BLOB)) + LENGTH(CAST(scope_key AS BLOB)) +
        COALESCE(LENGTH(CAST(rejection_reason AS BLOB)), 0) + payload_bytes +
        CASE WHEN state = 'rejected' AND turn_row_id IS NULL
                       AND length(CAST(tenant AS BLOB)) = 64
                       AND tenant NOT GLOB '*[^0-9a-f]*'
                       AND rejection_reason IN (
                           'overloaded', 'not_owner', 'not_sender', 'not_group',
                           'missing_mention', 'owner_command_required', 'policy',
                           'stale', 'internal'
                       )
             THEN LENGTH(CAST(
                 'inbound:v1:' || tenant || ':' || event_id ||
                 ':notice:' || rejection_reason AS BLOB
             ))
             ELSE 0
        END
    ), 0) <= 67108864
    AND COALESCE(SUM(CASE WHEN state = 'received' THEN 1 ELSE 0 END), 0) <= 256
    AND COALESCE(SUM(
        CASE WHEN state = 'received' THEN payload_bytes ELSE 0 END
    ), 0) <= 8388608
THEN 1 ELSE 0 END
FROM inbound_events;
DROP TABLE inbound_reply_effect_v12_guard;

ALTER TABLE inbound_events ADD COLUMN reply_outbox_key TEXT;

-- Versions through v11 already atomically enqueued rejection notices using a
-- global event-id key. Some of those outbox rows may have reached their
-- retention horizon before this migration, so row existence cannot be used as
-- the proof. Mark every classified legacy rejection with its new deterministic
-- tenant-scoped identity so the bounded inbound/outbox retention contract has
-- a durable effect witness for the remainder of that event's dedup window.
-- Turn-linked rejections are excluded: their outbound effect belongs to turn
-- finalization, not to one inbound notice. Pre-namespace legacy tenants and
-- open legacy reasons are also left marker-less so their historically legal
-- terminal rows remain recoverable. A current-namespace, classified no-turn
-- rejection is inherently ambiguous because the old schema also exposed a
-- bare rejection API. The production path was atomic, so conservatively
-- recording that class avoids a duplicate send at the cost of not backfilling
-- a hypothetical bare legacy rejection.
UPDATE inbound_events
SET reply_outbox_key =
    'inbound:v1:' || tenant || ':' || event_id || ':notice:' || rejection_reason
WHERE state = 'rejected' AND turn_row_id IS NULL
  AND length(CAST(tenant AS BLOB)) = 64
  AND tenant NOT GLOB '*[^0-9a-f]*'
  AND rejection_reason IN (
      'overloaded', 'not_owner', 'not_sender', 'not_group', 'missing_mention',
      'owner_command_required', 'policy', 'stale', 'internal'
  );

-- 4308 is the maximum byte length derived from a 64-hex current tenant plus
-- the bounded event-id and rejection-reason fields. These triggers protect future writes;
-- startup recovery independently validates the same identity so dropped or
-- bypassed triggers still fail closed.
CREATE TRIGGER inbound_events_v12_reply_effect_insert
BEFORE INSERT ON inbound_events
WHEN NEW.reply_outbox_key IS NOT NULL AND NOT (
    length(CAST(NEW.reply_outbox_key AS BLOB)) BETWEEN 1 AND 4308
    AND length(CAST(NEW.tenant AS BLOB)) = 64
    AND NEW.tenant NOT GLOB '*[^0-9a-f]*'
    AND NEW.turn_row_id IS NULL
    AND (
        (NEW.state = 'completed' AND NEW.rejection_reason IS NULL
         AND NEW.reply_outbox_key =
             'inbound:v1:' || NEW.tenant || ':' || NEW.event_id || ':control')
        OR
        (NEW.state = 'rejected' AND NEW.rejection_reason IS NOT NULL
         AND NEW.rejection_reason IN (
             'overloaded', 'not_owner', 'not_sender', 'not_group',
             'missing_mention', 'owner_command_required', 'policy',
             'stale', 'internal'
         )
         AND NEW.reply_outbox_key =
             'inbound:v1:' || NEW.tenant || ':' || NEW.event_id ||
             ':notice:' || NEW.rejection_reason)
    )
)
BEGIN
    SELECT RAISE(ABORT, 'invalid inbound reply effect marker');
END;

CREATE TRIGGER inbound_events_v12_reply_effect_update
BEFORE UPDATE ON inbound_events
WHEN
    (OLD.reply_outbox_key IS NOT NULL
     AND NEW.reply_outbox_key IS NOT OLD.reply_outbox_key)
 OR (NEW.reply_outbox_key IS NOT NULL AND NOT (
        length(CAST(NEW.reply_outbox_key AS BLOB)) BETWEEN 1 AND 4308
        AND length(CAST(NEW.tenant AS BLOB)) = 64
        AND NEW.tenant NOT GLOB '*[^0-9a-f]*'
        AND NEW.turn_row_id IS NULL
        AND (
            (NEW.state = 'completed' AND NEW.rejection_reason IS NULL
             AND NEW.reply_outbox_key =
                 'inbound:v1:' || NEW.tenant || ':' || NEW.event_id || ':control')
            OR
            (NEW.state = 'rejected' AND NEW.rejection_reason IS NOT NULL
             AND NEW.rejection_reason IN (
                 'overloaded', 'not_owner', 'not_sender', 'not_group',
                 'missing_mention', 'owner_command_required', 'policy',
                 'stale', 'internal'
             )
             AND NEW.reply_outbox_key =
                 'inbound:v1:' || NEW.tenant || ':' || NEW.event_id ||
                 ':notice:' || NEW.rejection_reason)
        )
    )
)
BEGIN
    SELECT RAISE(ABORT, 'invalid inbound reply effect marker');
END;
",
    },
];
