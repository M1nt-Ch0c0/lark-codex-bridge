# Reliable Bridge Runtime Implementation Plan

> **For agentic workers:** implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking; check a step only after its verification commands pass.

**Goal:** Turn the completed Lark transport and Codex app-server foundation into a durable bridge runtime: a single-writer SQLite store (WAL, foreign keys, migrations) with inbound event dedup (`received`/`accepted`/`completed`/`rejected`), one actor per scope with 600 ms debounce and a global active-turn semaphore, persistent scope→thread mapping with resume and uncertain-turn recovery, a durable outbox with idempotency keys and explicit `uncertain_delivery`, a progress/final reply projector honoring the design's hard reply contracts, a bounded content-addressed attachment cache with leases and GC, first-stage commands (`/new` `/stop` `/status` `/cd` `/help`), and owner-only access with safe workspace defaults — all verified by fault-injection tests and an opt-in real Lark + Codex smoke test.

**Architecture:** A `store` module owns SQLite through one dedicated blocking writer task; every other component talks to it through an async `StoreHandle`. The `runtime` module hosts the `Router` (scope resolution, actor lifecycle, global turn semaphore) and one `ScopeActor` per scope, serializing user messages and commands through a single bounded mailbox per scope. `ScopeActor` drives `codex::client::AppServerClient` (`start_thread`/`resume_thread`/`start_turn`/`interrupt_turn`/`subscribe`) and projects events through `render::ReplyProjector` into the `outbox`, whose pump performs Lark sends via `lark::api::LarkApi` and records receipts. `runtime::attachments` downloads resources through `LarkApi::download_message_resource` into a content-addressed cache whose leases are tracked in SQLite. `config` loads the bridge config with owner-only defaults; `app` assembles everything behind a new `run` CLI subcommand. Approvals, team mode, and service managers are explicitly out of scope (next milestone).

**Tech Stack:** Rust 2024, Tokio, rusqlite (bundled, single blocking writer, no ORM), sha2, serde/toml, secrecy, tracing, thiserror/anyhow, assert_cmd, tempfile — on top of the existing `codex` and `lark` modules.

**Behavioral source of truth:** user-visible semantics extracted (not copied) from the reference checkout at `/home/wcy/.lark-channel-workspaces/codex/default/feishu-claude-code-bridge` — specifically `src/commands/index.ts` (`/new` archives the active session and keeps the workspace, `/cd` validates an absolute path and resets the session, `/stop` interrupts the active run and answers passively when idle, `/status` renders a secret-free summary, `/help` lists available commands; admin gating for cwd-changing commands), `src/bot/scope.ts` (chat vs topic-thread scope routing), and the reply-contract fixes described in design spec §9. Claude support, Web UI, meeting features, and multi-provider adapters are not carried over.

## Global Constraints

- Use Rust edition 2024 with `rust-version = "1.85"`; CI tests stable Rust on Linux, macOS, and Windows. `unsafe_code` stays forbidden; keep clippy `pedantic` clean with `-D warnings`.
- SQLite runs in WAL mode with `PRAGMA foreign_keys = ON`, `PRAGMA busy_timeout`, and a `user_version`-driven migration table. Exactly one blocking writer task owns the `rusqlite::Connection`; all writes go through it. Reads either go through the same writer task (first version preference — keeps one code path) or through a separate read-only connection opened with the same pragmas; never open a second read-write connection (design §8, §15).
- Every long-lived queue, cache, pending map, mailbox, dedup table sweep buffer, outbox batch, and attachment cache gets both a count and a byte limit, defined in `src/limits.rs` next to the existing constants (handoff §2 rule 5).
- The Codex concurrency/resource invariants from handoff §4.2 must not regress: this milestone consumes `AppServerClient` and `AppServerSupervisor` through their existing public APIs only; it does not reach into `rpc`/`transport` internals, does not add a second stdin writer, and does not bypass the thread-scoped mailbox or byte permits.
- Uncertain non-idempotent writes are explicit: a Lark send whose outcome is unknown is recorded as `uncertain_delivery` and never blindly retried as if it had failed; a `turn/start` interrupted by a connection loss marks the turn `uncertain` and is never blindly resent (handoff §2 rule 6, design §9, §13.1).
- `Debug`, tracing output, and error messages must never contain user prompt text, message/card content, tool output, attachment bytes or user file names, the App Secret, tenant tokens, or absolute local paths outside the configured workspace root; log only IDs (`event_id`, `message_id`, `scope_hash`, `thread_id`, `turn_id`), sizes, counts, and classified error kinds (handoff §4.2 rule 12, design §14).
- First-stage access model: only configured owner `open_id`s may use the bridge at all; group messages must directly @ the bot; the default sandbox is `workspace-write` with the network setting from explicit config; `/cd` is owner-only; workspaces under `/`, the home root, system directories, desktop/download roots, and temp roots are rejected (design §11, §12).
- Reuse existing interfaces: `lark::bridge::LarkBridge::start_with` → `(TransportHandle, mpsc::Receiver<QueuedInboundEvent>)`, `lark::normalize::{InboundEvent, ScopeKey}`, `lark::api::LarkApi`, `codex::supervisor::{AppServerSupervisor, SupervisorHandle}`, `codex::client::{AppServerClient, ThreadSubscription, TurnOutcome, AppServerEvent}`. Extend them only where a listed task says so.
- Pin exact dependency versions in `Cargo.toml` and keep `--locked` green.
- Follow the user's efficient-development preference: write boundary tests and fixtures, then implement the complete slice; do not require a separate failing-test commit for every private helper.
- Commit after each numbered task whose checks pass; the main agent reviews and pushes.

---

## File Map

- `Cargo.toml`: add `rusqlite` (bundled) and `sha2`; promote nothing else.
- `src/lib.rs`: export `app`, `config`, `outbox`, `render`, `runtime`, `store`.
- `src/limits.rs`: all new bounded capacities, byte budgets, TTLs, debounce, and retry constants for this milestone.
- `src/config.rs`: bridge config schema, TOML load, owner list, workspace policy, safe defaults, validation.
- `src/store/mod.rs`: `StoreHandle`, `StoreError`, open/init with pragmas and migrations.
- `src/store/schema.rs`: migration list (`user_version` steps) and table DDL.
- `src/store/writer.rs`: single blocking writer task and bounded command channel.
- `src/store/dedup.rs`: inbound event registration and state transitions.
- `src/store/sessions.rs`: scopes/threads/turns rows and queries.
- `src/store/outbox.rs`: outbox enqueue/claim/complete/fail queries.
- `src/runtime/mod.rs`: stable exports.
- `src/runtime/policy.rs`: owner gate, group-mention gate, workspace validator, policy fingerprint.
- `src/runtime/router.rs`: scope → actor lifecycle, global active-turn semaphore.
- `src/runtime/scope.rs`: `ScopeActor` state machine, debounce, batching, turn lifecycle.
- `src/runtime/commands.rs`: `/new` `/stop` `/status` `/cd` `/help` parsing and handlers.
- `src/runtime/attachments.rs`: bounded download, content-addressed cache, lease, GC.
- `src/outbox/mod.rs`: outbox pump, idempotency keys, retry schedule, receipts, `uncertain_delivery`.
- `src/render/mod.rs`: `ReplyProjector`, progress/final text projection, email audit masking.
- `src/app.rs`: assembly, startup order, signal handling, graceful shutdown.
- `src/cli.rs`: `run` subcommand.
- `src/lark/bridge.rs`: extend with a pre-enqueue durability hook (Task 3 only).
- `tests/store.rs`: pragmas, migrations, single-writer behavior, dedup transitions, TTL.
- `tests/runtime_policy.rs`: config defaults, owner/mention gates, workspace rejection set.
- `tests/runtime_scope.rs`: actor batching, semaphore, thread mapping, commands, recovery — against the existing fake process factory.
- `tests/runtime_recovery.rs`: fault injection (child exit mid-turn, bridge restart, duplicate redelivery).
- `tests/outbox.rs`: idempotent retry, receipt, uncertain delivery, ordering.
- `tests/reply_projector.rs`: the five hard reply contracts plus masking.
- `tests/attachments.rs`: bounds, hashing, lease/GC, failure cleanup.
- `tests/runtime_smoke.rs`: ignored, opt-in real Lark + Codex end-to-end test (`LARK_E2E=1` + `CODEX_E2E=1`).
- `tests/fixtures/runtime/*.toml|*.json`: config and projector fixtures.

## Task 1: SQLite Store — WAL, Migrations, Single-Writer Task

**Files:**

- Modify: `Cargo.toml`
- Create: `src/store/mod.rs`
- Create: `src/store/schema.rs`
- Create: `src/store/writer.rs`
- Create: `src/store/dedup.rs`
- Create: `src/store/sessions.rs`
- Create: `src/store/outbox.rs`
- Create: `tests/store.rs`
- Modify: `src/lib.rs`
- Modify: `src/limits.rs`

**Interfaces:**

- Produces: `StoreHandle`, `StoreError`, `InboundEventState`, `DedupOutcome`, `ScopeRow`, `ThreadRow`, `TurnRow`, `OutboxRow`, `Migration`.
- Consumes: `rusqlite`, `lark::normalize::{InboundEvent, ScopeKey}`.

- [x] **Step 1: Add the milestone dependency set**

Append to `[dependencies]` with exact pins (verify the lockfile resolves each; bump to the newest compatible patch if the pin fails to resolve and record the chosen version in the commit message):

```toml
rusqlite = { version = "0.40.2", features = ["bundled"] }
sha2 = "0.11.0"
```

`bundled` keeps CI hermetic on all three platforms; no ORM, no `sqlx`/`diesel` (design §15). No async SQLite wrapper crate: the writer task is a plain `std::thread` (or `tokio::task::spawn_blocking` loop) owned by this module.

- [x] **Step 2: Implement schema, migrations, and the single-writer store**

```rust
pub struct StoreHandle { /* bounded mpsc to the writer task */ }

#[derive(Debug, thiserror::Error)]
pub enum StoreError { /* Io, Sqlite classified, QueueFull, Closed, Migration } */

pub struct Migration { pub version: u32, pub name: &'static str, pub sql: &'static str }

impl StoreHandle {
    pub async fn open(path: &Path) -> Result<Self, StoreError>;       // file-backed
    pub async fn open_in_memory() -> Result<Self, StoreError>;        // tests
    pub async fn shutdown(self) -> Result<(), StoreError>;
}
```

`open` applies, inside the writer task before serving requests: `PRAGMA journal_mode = WAL`, `PRAGMA foreign_keys = ON`, `PRAGMA busy_timeout = STORE_BUSY_TIMEOUT` (e.g. 5 s), `PRAGMA synchronous = NORMAL`, then runs pending migrations in ascending `user_version` order, each in its own transaction. Migration DDL (design §8, minus the milestone-4 tables):

- `inbound_events(tenant, event_id, message_id, scope_key, state, first_seen_ms, updated_ms, rejection_reason, PRIMARY KEY(tenant, event_id))` with `state ∈ {received, accepted, completed, rejected}` and an index on `(message_id)`;
- `scopes(scope_key PRIMARY KEY, cwd, policy_fingerprint, updated_ms)`;
- `threads(scope_key, codex_thread_id, status ∈ {active, archived}, created_ms, archived_ms, PRIMARY KEY(scope_key, codex_thread_id))`;
- `turns(id INTEGER PRIMARY KEY, scope_key, client_message_id UNIQUE, codex_thread_id, codex_turn_id, state ∈ {starting, running, completed, failed, interrupted, uncertain}, uncertain INTEGER, created_ms, updated_ms)`;
- `outbox(id INTEGER PRIMARY KEY, idempotency_key UNIQUE, scope_key, kind, payload_json, payload_bytes, state ∈ {pending, sending, sent, failed, uncertain_delivery}, attempts, next_retry_ms, receipt_message_id, created_ms, updated_ms)`;
- `attachments(sha256 PRIMARY KEY, bytes, kind, created_ms, last_used_ms)` and `attachment_leases(sha256, turn_row_id, created_ms, PRIMARY KEY(sha256, turn_row_id), FOREIGN KEY(sha256) REFERENCES attachments(sha256))`.

All writer requests travel one bounded channel (`STORE_WRITER_CAPACITY` count; oversized payloads rejected before enqueue) and are answered by oneshot; the task processes them sequentially so every transaction has exactly one author. `payload_bytes` mirrors the serialized size so byte budgets can be enforced in queries without re-parsing.

- [x] **Step 3: Implement the typed store queries used by later tasks**

Keep SQL inside `src/store/*`; callers see async methods only:

```rust
// dedup.rs
// Task 3 supersedes this initial marker-only outcome/schema and the unbounded
// sweep signature with a replayable payload, tenant namespace, atomic
// inbound→turn APIs, and a bounded sweep.
pub enum DedupOutcome { New, Duplicate { state: InboundEventState } }
impl StoreHandle {
    pub async fn register_inbound(&self, tenant: &str, event: &InboundEvent) -> Result<DedupOutcome, StoreError>;
    pub async fn transition_inbound(&self, tenant: &str, event_id: &str, to: InboundEventState, reason: Option<&str>) -> Result<(), StoreError>;
    pub async fn sweep_inbound(&self, older_than_ms: i64) -> Result<u64, StoreError>; // TTL pruning
}

// sessions.rs
impl StoreHandle {
    pub async fn upsert_scope(&self, scope: &ScopeKey, cwd: &Path, fingerprint: &str) -> Result<(), StoreError>;
    pub async fn scope_row(&self, scope: &ScopeKey) -> Result<Option<ScopeRow>, StoreError>;
    pub async fn active_thread(&self, scope: &ScopeKey) -> Result<Option<ThreadRow>, StoreError>;
    pub async fn archive_active_thread(&self, scope: &ScopeKey) -> Result<Option<ThreadRow>, StoreError>;
    pub async fn record_turn(&self, row: NewTurnRow) -> Result<i64, StoreError>;
    pub async fn set_turn_state(&self, id: i64, state: TurnState, codex_turn_id: Option<&str>) -> Result<(), StoreError>;
    pub async fn uncertain_turns(&self) -> Result<Vec<TurnRow>, StoreError>;
}

// outbox.rs
impl StoreHandle {
    pub async fn enqueue_outbox(&self, row: NewOutboxRow) -> Result<OutboxEnqueue, StoreError>; // idempotency_key dedup
    pub async fn claim_outbox_batch(&self, now_ms: i64, limit: u32) -> Result<Vec<OutboxRow>, StoreError>; // atomic pending→sending
    pub async fn complete_outbox(&self, id: i64, receipt_message_id: &str) -> Result<(), StoreError>;
    pub async fn fail_outbox(&self, id: i64, attempts: u32, next_retry_ms: i64, uncertain: bool) -> Result<(), StoreError>;
    pub async fn outbox_depth(&self) -> Result<OutboxDepth, StoreError>; // count + bytes for /status
}
```

Legal inbound transitions are `received → accepted → completed|rejected` (plus `received → rejected`); anything else is a `StoreError` so a duplicate redelivery in TTL can never restart Codex. This is the historical Task 1 baseline: Task 3 removes the generic public `received → accepted` operation and permits that edge only inside its atomic starting-turn + inbound-association transaction. Turn and outbox state machines are enforced the same way.

- [x] **Step 4: Test the store**

Cover: pragmas actually applied (`PRAGMA journal_mode` returns `wal`, foreign keys enforced by a failing insert); migrations apply once and survive reopen (`user_version` persisted); concurrent writers serialize (N tasks × M writes, final count exact); channel bound rejection; dedup register/duplicate/same-message-different-event; illegal transition rejection; TTL sweep; outbox idempotent enqueue (same `idempotency_key` returns the existing row), claim is atomic under concurrent claimers, complete records the receipt; foreign-key cascade behavior for attachment leases; reopen persistence for every table.

- [x] **Step 5: Verify and publish the task**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --test store --locked
cargo test --all-targets --locked
cargo +1.85.0 check --all-targets --all-features --locked
cargo build --release --locked
```

Commit `feat: add SQLite store with WAL, migrations, and single-writer task`.

## Task 2: Bridge Config and Access Policy

**Files:**

- Create: `src/config.rs`
- Create: `src/runtime/mod.rs`
- Create: `src/runtime/policy.rs`
- Create: `tests/runtime_policy.rs`
- Create: `tests/fixtures/runtime/config_minimal.toml`
- Create: `tests/fixtures/runtime/config_full.toml`
- Modify: `src/lib.rs`
- Modify: `src/limits.rs`

**Interfaces:**

- Produces: `BridgeConfig`, `WorkspacePolicy`, `AccessPolicy`, `PolicyFingerprint`, `AccessDecision`.
- Consumes: `toml`, `secrecy`, `lark::normalize::InboundEvent`.

- [x] **Step 1: Implement the bridge config schema with safe defaults**

```rust
pub struct BridgeConfig {
    pub owners: Vec<String>,                 // open_ids; empty = refuse all inbound (fail-closed)
    pub default_workspace: Option<PathBuf>,
    pub workspace: WorkspacePolicy,
    pub concurrency: ConcurrencyConfig,      // active-turn permits, scope caps
    pub codex: CodexSection,                 // binary, optional codex_home, model override
    pub paths: PathsSection,                 // db path, attachment cache dir
}

pub struct WorkspacePolicy {
    pub allow_roots: Vec<PathBuf>,           // workspace must resolve under one of these
    pub network_access: bool,                // default false
}
```

Load from an explicit TOML path (`run --config`), else the platform config dir. Every field has a conservative default; unknown keys are rejected. `BridgeConfig::validate` canonicalizes `allow_roots`, requires at least one owner (otherwise startup fails with an actionable error rather than silently allowing nobody — the distinction is logged), and requires `default_workspace` to pass the same validation as `/cd`. Config `Debug` shows owner count and paths, never owner IDs beyond a trailing-6-char fragment (matching the reference's `sender.slice(-6)` logging habit) and never any secret.

- [x] **Step 2: Implement the access policy and workspace validator**

```rust
pub enum AccessDecision { Allow, DenyNotOwner, DenyMissingMention, DenyWorkspace { reason: &'static str } }

pub struct PolicyFingerprint(String); // stable hash of canonical cwd + sandbox mode + approval policy + network flag

impl AccessPolicy {
    pub fn decide(&self, event: &InboundEvent) -> AccessDecision;   // owner-only + group-mention gate
    pub fn validate_workspace(&self, path: &Path) -> Result<PathBuf, WorkspaceRejection>; // canonicalize + reject set
    pub fn fingerprint(&self, cwd: &Path) -> PolicyFingerprint;
}
```

Semantics (design §11, reference `commands/index.ts` gates): sender must be in `owners`; group and topic messages must have `mentions_bot == true` (p2p exempt); the workspace validator rejects `/`, the user's home root itself (home subdirectories are fine), system roots (`/etc`, `/usr`, `/bin`, `/System`, `C:\Windows`, …), temp roots, and desktop/download roots, then requires the canonical path to sit under an `allow_roots` entry. `PolicyFingerprint` is a truncated SHA-256 over the canonical cwd, sandbox mode, approval policy, and network flag — any change means the old Codex thread is not reused (design §8).

- [x] **Step 3: Test config and policy**

Fixtures cover: minimal config fills safe defaults; full config round-trips; unknown key rejection; missing owner fails validation; p2p owner allowed; p2p non-owner denied; group without @ denied even for the owner; topic message with @ allowed; each workspace rejection class; fingerprint changes on cwd/sandbox/network change and is stable otherwise; `Debug` redaction (no full owner IDs, no secrets).

- [x] **Step 4: Verify and publish the task**

Run the Task 1 gate set with `cargo test --test runtime_policy --locked`. Commit `feat: add bridge config and owner-only access policy`.

## Task 3: Durable Inbound Inbox and Dedup at the Receipt Boundary

**Files:**

- Modify: `src/lark/bridge.rs`
- Create: `src/runtime/intake.rs`
- Modify: `src/runtime/mod.rs`
- Modify: `src/store/dedup.rs`
- Modify: `src/store/schema.rs`
- Modify: `src/store/mod.rs`
- Modify: `src/store/sessions.rs`
- Modify: `src/lark/normalize.rs`
- Modify: `src/lark/credentials.rs`
- Modify: `src/limits.rs`
- Modify: `tests/store.rs`
- Create: `tests/runtime_intake.rs`
- Modify: `tests/lark_bridge.rs`
- Create: `tests/bridgews/mod.rs`

**Interfaces:**

- Consumes: `StoreHandle`, `LarkBridge::start_with`, normalized `InboundEvent`.
- Produces: `DurableIntake`, `TenantNamespace`, `IntakeRuntime`, `IntakeHook`, replay/claim outcome types; extended `LarkBridge`.

- [x] **Step 1: Upgrade inbound registration into a bounded replayable inbox**

Add migration 2 rather than changing migration 1. Use nullable
`payload_version INTEGER`, nullable `payload_blob BLOB`,
`payload_bytes INTEGER NOT NULL DEFAULT 0 CHECK(payload_bytes >= 0)`, and
nullable `turn_row_id INTEGER REFERENCES turns(id) ON DELETE RESTRICT` on
`inbound_events`; add `inbound_count INTEGER NOT NULL DEFAULT 0
CHECK(inbound_count >= 0)` to `turns`. Add non-unique tenant/message/state,
inbound→turn, and deterministic terminal-sweep indexes.
Do not add a partial unique same-message index: v1 permits conflicting rows and
the migration must remain atomic and reopenable. A v2 `accepted` row must have
a turn association; `received` must not. Migration-2 insert/update triggers
enforce the payload **column shape** of future writes: received has no turn and
version 1/non-null blob/exact `payload_bytes == length(payload_blob)`; accepted
has the same column shape and a same-scope turn; terminal has no payload and,
when associated, points to a terminal/resolved turn. SQLite triggers do not
claim to validate strict JSON semantics, duplicate/extra fields, enums, or
scope logic; typed write/read plus startup scans own those checks. The triggers
deliberately do not retroactively rewrite legacy rows; preparation validates
those. Terminal
payload clearing is logical
SQLite clearing (`version/blob = NULL`, bytes = 0), not a claim of physical
secure erasure from WAL/freelist.

Milestone 2 never used the v1 table as the production receipt boundary, so v1
tenant strings are development data and cannot be guessed/mapped to the new
namespace. Legacy terminal rows remain readable/sweepable under their old
tenant string. `DurableIntake::prepare` scans all namespaces for legacy
`received` **or** `accepted` rows lacking a valid payload/association and fails
closed. If a tenant/message has more than one non-`rejected` canonical
candidate, registration/recovery returns static `CorruptData`; never choose one
with `LIMIT 1`. A future real alias migration must take an explicit old→new
mapping in one transaction rather than guessing `feishu`/`lark`.

Persist a private `#[serde(deny_unknown_fields)]` v1 DTO (including private
closed wire enums), never public serde derives on `InboundEvent`. Decode checks
version, valid/strict JSON, declared versus actual blob length, row/DTO IDs and
scope, scope/chat/thread consistency, the closed resource kind/open message
type representation, and every logical field cap. Add explicit constants for
ID bytes (4 KiB), serialized scope (12
KiB), message type (256 B), text (1 MiB), resources (64), one resource key (4
KiB), aggregate resource keys (256 KiB), and serialized payload (2 MiB); raise
the single writer request cap to 3 MiB so a maximum valid DTO plus indexed
columns is representable while
the existing 8 MiB writer-byte budget still limits concurrency. Unknown
version, invalid/duplicate/extra fields, inconsistency, or forged length is a
content-free error.

`register_inbound` uses this transaction order:

1. validate only the fixed tenant namespace and the incoming event/message ID
   presence and byte caps;
2. exact `(tenant,event_id)` lookup — `received` strictly decodes and returns
   the stored canonical payload, while accepted/terminal states return only
   canonical ID/state without validating the untrusted redelivery body;
3. query **all** same `(tenant,message_id)` non-rejected candidates — exactly
   one is handled as above, more than one is corrupt, and all-rejected/none
   continues (exact rejected still wins because exact lookup came first);
4. only now logically validate and serialize the incoming DTO;
5. check total row/logical-byte and `received` row/payload-byte capacity;
6. insert `received` and commit.

No duplicate inserts an alias row or refreshes `updated_ms`. Use distinct
constants: total 65,536 rows/64 MiB variable retained bytes, `received` 256 rows/8
MiB payload bytes, and the per-field/payload limits above. The total accounting
is deliberately the sum of variable tenant/event/message/scope/reason/payload
bytes (fixed state/timestamp/foreign-key scalars are covered by the row cap);
every transition recomputes the resulting value, including a new rejection
reason. Moving to accepted releases only the `received` quota, and terminal
clearing releases payload
bytes but retains marker row/key bytes. Startup recovery first checks count,
declared/actual bytes, single-row caps, legacy corruption, and aggregate quota
inside one writer job, then materializes exactly the complete current-tenant
`received` set ordered by `(first_seen_ms,event_id)`; accepted rows are only
integrity-checked/associated for later Task 8 reconciliation and never enter the
ordinary business queue. On any error recovery returns no partial vector. The
`retained_bytes` used by runtime is always the exact persisted blob length.

`TenantNamespace` is an opaque 32-byte SHA-256 value stored as fixed 64-char
hex. It hashes a domain-separated, length-framed stable tenant-brand tag and
app ID; it never owns/prints credentials, app ID, app secret, or the broad
brand alone. Two apps sharing one database therefore cannot collide.

Add purpose-specific atomic APIs; do **not** expose a plain
`received → accepted` claim that could create an orphan accepted row:

```rust
pub struct InboundKey { /* tenant namespace + canonical event ID */ }

pub struct ClaimedInbound { /* canonical key + decoded persisted event + retained bytes */ }
pub struct SkippedInbound { /* canonical key + observed state + associated turn, if any */ }

pub enum BeginTurnOutcome {
    Started { turn_row_id: i64, claimed: Vec<ClaimedInbound>, skipped: Vec<SkippedInbound> },
    NoReceived { skipped: Vec<SkippedInbound> },
}

pub enum ResolveTurnOutcome {
    Resolved { inbound_rows: usize },
    AlreadyResolved { inbound_rows: usize },
}

pub async fn begin_turn_and_claim_inbound(
    &self,
    turn: NewTurnRow,
    events: &[InboundKey],
) -> Result<BeginTurnOutcome, StoreError>;
pub async fn reject_received(
    &self,
    key: &InboundKey,
    reason: InboundRejectionKind,
) -> Result<InboundDisposition, StoreError>;
pub async fn reject_received_and_enqueue_notice(
    &self,
    key: &InboundKey,
    reason: InboundRejectionKind,
    notice: NewOutboxRow,
) -> Result<InboundDisposition, StoreError>;
pub async fn recover_received(
    &self,
    tenant: &TenantNamespace,
) -> Result<Vec<RetainedInbound>, StoreError>;
pub async fn resolve_turn_and_finish_inbound_batch(
    &self,
    turn_row_id: i64,
    turn: TurnResolution,
    inbound: InboundTerminal,
) -> Result<ResolveTurnOutcome, StoreError>;
```

Delete the old public `transition_inbound` API. Internal helpers cannot write
accepted; that state is reachable only through `begin_turn_and_claim_inbound`.
The begin API validates a bounded, unique, same-scope key set (at most 64 keys
and 256 KiB aggregate key bytes) and reuses `record_turn`'s complete invariants:
initial state is starting, client message ID uniqueness, request byte budget,
and live recovery turn count+byte capacity. Task 4's batch cap must be no larger.
In one transaction it partitions current `received` rows from already claimed
rows, inserts one `starting` turn only when at least one remains, conditionally
updates precisely that subset to `accepted` with the new turn ID, and returns
the exact canonical claimed events for input assembly. Skippable rows are only
accepted/completed/rejected and carry their key/state/turn; unknown rows, scope
or turn mismatch, corruption, and duplicate input keys roll back everything.
`claimed + skipped == unique input`, and all input/output vectors are checked
against the count+byte bounds before materialization. This partitioning
prevents one concurrent duplicate from stranding unrelated received rows in
the same debounce batch.

For turns with `inbound_count > 0`, the existing public `set_turn_state` may
only perform the non-terminal `starting → running` update (including the Codex
turn ID). Any completed/failed/interrupted/uncertain resolution must use the
combined API; legacy/test turns with `inbound_count == 0` retain Task 1's state
API. This prevents another public path from recreating a split terminal state.

`reject_received` is an idempotent CAS with a closed static reason enum. Only a
currently received row is changed, in one transaction, to rejected with reason
and `updated_ms`, version/blob cleared and bytes zero; accepted is returned as
already claimed and never modified. Same-reason rejected repeats are
idempotent; completed or conflicting terminal state is a typed disposition.

User-visible policy/stale/overload paths use
`reject_received_and_enqueue_notice(key, reason, notice: NewOutboxRow)` instead
of the bare CAS. In one transaction it first verifies the row is still received,
validates/enqueues the deterministic notice under the existing outbox
count+byte/idempotency rules, then rejects and clears payload. If the row was
concurrently accepted, it writes no incorrect notice and returns
`AlreadyClaimed`; if outbox capacity/persistence fails, the entire transaction
rolls back so the received event remains replayable. Because intake may already
have acknowledged durable receipt, the router first schedules a bounded local
retry; Task 8's cancellation-aware periodic Received rescan is the durable
fallback if that retry cannot fit or the process dies. The bare
`reject_received` is reserved for silent/internal
rejections whose contract explicitly requires no user notice.

`resolve_turn_and_finish_inbound_batch` replaces separate turn/inbound final
writes. `TurnResolution` deterministically maps completed to inbound completed,
and failed/interrupted/uncertain to inbound rejected with a closed reason; the
caller cannot request a conflicting pair. In one transaction the API validates
the legal turn resolution, verifies an unresolved turn has exactly
`inbound_count` accepted rows, updates the turn and all linked inbound rows,
and clears payloads. `inbound_count` is the immutable historical claim count,
not a remaining-marker count. A resolved turn may have 0..=`inbound_count`
matching terminal markers because TTL sweeping can delete them in batches; it
must have no accepted marker, and a same-resolution repeat returns
`AlreadyResolved` even after all markers are swept. Missing turn, zero historical
links for a runtime turn, unresolved count mismatch, or conflicting state fails
closed. Projected final/failure/uncertainty outbox rows are persisted first with
deterministic keys; a crash between outbox enqueue and this atomic resolve is
safe to repeat. Thus no public future write can produce either an orphan
accepted row or a terminal-turn/accepted-inbound split.

For new runtime turns, `uncertain + terminal inbound + durable deterministic
notice` is a resolved historical uncertainty: it no longer occupies live turn
recovery count/bytes and is never automatically resumed. `starting`/`running`
are unresolved recovery work. Legacy Task-1 rows with `inbound_count == 0` keep
the old uncertain recovery semantics. A later manual uncertain→failed/
interrupted resolution still uses the combined idempotent API and remains valid
after marker sweeping.

Because the database now contains prompt/resource metadata, a newly created
Unix database is mode `0600`; an existing main file is tightened through its
already-open file handle before SQLite opens/migrates, and any created WAL/SHM
sidecars are verified/tightened before requests are served and verified again
after reopen/recreation paths. Reject non-regular files. Debug/errors never
expose payload, sender, resource keys, app ID, or a
dynamic serde/SQLite message.

- [x] **Step 2: Extend bridge wiring with durable registration and startup replay**

`runtime::intake::DurableIntake::prepare(store, &credentials)` borrows the
credentials only long enough to derive the namespace/binding and returns a
non-`Clone`, by-value `IntakeRuntime` containing a bounded startup recovery set
and a hook. The hook captures only `StoreHandle + TenantNamespace`, never the
credentials or secret. Put the transport seam types in `lark::bridge` and the
store-backed implementation in `runtime::intake`:

```rust
pub struct RetainedInbound {
    event: Box<InboundEvent>,
    retained_bytes: usize,
}

pub type IntakeHook = Arc<
    dyn Fn(Box<InboundEvent>)
        -> BoxFuture<'static, Result<IntakeVerdict, LarkError>>
        + Send
        + Sync,
>;

pub enum IntakeVerdict {
    Enqueue(RetainedInbound),
    DropDuplicate,
}

impl IntakeRuntime {
    pub fn try_from_parts(
        credentials: &LarkCredentials,
        recovery: Vec<RetainedInbound>,
        hook: IntakeHook,
    ) -> Result<Self, LarkError>;
}
```

`try_from_parts` is the narrow injection seam used by deterministic bridge
tests (and custom durable intake implementations); it derives the binding
without retaining credentials, validates the store-level recovery count/byte
ceiling, and still defers the caller's smaller channel bounds to startup.
Production `prepare` uses the same constructor. Fields stay private, there are
no getters that can replay the set, and `start_with_runtime` consumes it exactly
once.

`LarkBridge::start_with_runtime(endpoints, creds, config, intake)` consumes the
runtime, recomputes and verifies its credential binding, validates non-zero
channel limits and checks both count capacity and byte budget against Tokio's
`Semaphore::MAX_PERMITS`, and uses one channel/semaphore for
startup and live traffic. Before starting the WebSocket transport, checked-add
the complete recovery count/bytes and every `usize → u32`; reject overflow.
Preload the complete set without an await: for each item use
`try_reserve_owned`, then `try_acquire_many_owned`, then `OwnedPermit::send`.
Any theoretical partial failure drops the local channel and all permits and
returns no running transport. Bot-info HTTP may precede this, but the WebSocket
must not start before preload completes. Existing `start`/`start_with` behavior
stays unchanged and continues to budget legacy events by raw payload bytes;
runtime events use exact persisted payload bytes, with docs updated accordingly.

The live handler order is exactly `normalize → await hook → try-reserve count →
try-acquire bytes → send → Ok`; nothing is reserved before the hook and there
is no await after it returns `Enqueue`. `New` and `ReplayReceived` enqueue the
canonical persisted event; accepted/terminal duplicates take `DropDuplicate`
without touching queue capacity, so they still ack 200 when the queue is full.
Store failure, channel fullness, or byte exhaustion fail the handler/startup.
A live delivery receives 200 only after both SQLite commit and bounded enqueue.
If the outer handler timeout cancels while a queued writer job later commits,
or commit succeeds but permit/enqueue fails, the transport sends 500 and
redelivery/restart replays the `received` row. If handler success is followed by
a socket failure while sending the receipt, the platform receives no receipt
(not a 500) and may redeliver; the same canonical replay semantics apply.
Receipt loss or concurrent startup/live delivery may enqueue duplicates, but
Task 4's combined turn-intent-and-claim transaction is the single
business-execution gate.

Map store errors to static Lark classifications: transient writer/SQLite/I/O to
retryable, capacity/payload limits to exhausted, and corrupt/legacy/binding/
version/invariant/startup failures to `ProtocolViolation`. Never interpolate
the original error or log `?event`,
payload JSON, sender ID, text, resource keys, app ID, or dynamic SQLite/serde
messages. Tighten `InboundEvent`, `ResourceDesc`, `Normalizer`, credentials,
queue/outcome/runtime Debug to counts/lengths/states and short non-sensitive
fingerprints only. Receipt still does not mean "Codex finished"—business
failure is reported through the durable outbox.

- [x] **Step 3: Add bounded terminal sweeping**

Change sweeping to `sweep_inbound(older_than_ms, max_rows)`, clamp it to
`DEDUP_SWEEP_BATCH`, and delete at most that many old
`completed`/`rejected` rows in deterministic order. `received`/`accepted` and
new terminal rows are never swept. Sweeping never mutates historical
`turns.inbound_count`; resolved-turn validation accepts any remaining matching
marker count from zero through that historical value. Define `DEDUP_TTL` (7 days),
`DEDUP_SWEEP_INTERVAL` (1 hour), and the batch bound here; Task 8 owns the one
cancellation-aware periodic runner and invokes a first pass at startup. Also
define a clamped `INBOUND_REPLAY_INTERVAL` (30 seconds) for Task 8's bounded
current-tenant Received rescan; each scan is all-or-nothing and reuses
`recover_received`'s count+byte bounds.

- [x] **Step 4: Test crash windows, dedup semantics, and bounds**

Store tests create a real user-version-1 database and cover migration 1→2,
rollback/reopen, all four legacy states, global fail-closed for legacy
received/accepted, conflicting canonical candidates, strict payload round-trip,
unknown version, invalid/extra/duplicate fields, row/DTO mismatch, forged
length, and every logical/serialized/total/live count+byte exact/max+1 edge.
They cover transactional concurrent exact/same-message registration, exact
duplicates in all four states, exact-rejected precedence, same-message behavior
including all-rejected, canonical content/ID replay, duplicate at capacity,
terminal logical payload clearing, duplicate TTL not refreshing, recovery
tenant isolation/order/all-or-nothing bounds, and batched terminal-only sweep.
Atomic-turn tests prove duplicate key rejection, mixed-batch partitioning,
whole-transaction rollback on unknown/mismatched rows, one winner under
concurrency, all claimed rows share one turn, no accepted row lacks a turn,
the begin path preserves every existing `record_turn` capacity/state invariant,
atomic/idempotent turn+inbound resolution, trigger enforcement, and
crash-visible turn association. The state-matrix tests require: received+NULL
replays; received+turn is corrupt; accepted+NULL is corrupt; accepted linked to
starting/running/uncertain enters recovery; accepted linked to a resolved turn
is repairable only through the deterministic reconciliation workflow; terminal
inbound linked to a live turn is corrupt; terminal+NULL or matching resolved
turn is valid. Also resolve a multi-message turn, sweep its terminal markers in
multiple batches, reopen between batches, and prove integrity validation plus
`AlreadyResolved` still succeed. Prove a resolved uncertain runtime turn no
longer consumes live recovery quota. Prove atomic reject+notice rolls back both
on outbox capacity failure, writes neither a wrong notice nor rejection after a
concurrent claim, and returns idempotently on retry. Unix verifies a
new and pre-existing main DB plus any WAL/SHM sidecars have mode `0600`.

Extract the bounded reusable WS harness to `tests/bridgews/mod.rs` (replace its
current unbounded test channel, and bound connection concurrency instead of
unrestricted per-connection task spawning). Against it plus a file store, a
barrier proves no receipt/business queue item before hook resolution; first
delivery persists
then enqueues and returns 200; accepted/terminal duplicate returns 200 even
when full; received duplicate replays old canonical ID/content. With a test
channel configured below the store live cap, registration can commit and then
return 500 on count/byte fullness; redelivery after release succeeds. When the
store live cap is hit first, it returns 500 without a new row. Cover outer
hook timeout after commit, receipt loss/concurrent duplicate queueing followed
by one atomic turn claim, 200 then restart preload before any WS connection
(bot-info HTTP may already have run), credential mismatch, zero/oversized
limits, count/byte startup overflow with no partial receiver/connection,
ignored/card bypass, and permit release for every
drop/error/receiver-close path. Keep the legacy no-hook raw-byte tests. Debug/
error sentinels prove text, sender, resource key, app ID, secret, and payload
JSON never appear. Assert `IntakeHook: Send + Sync` and `IntakeRuntime: Send`;
ownership tests use the consuming API, while review verifies that runtime does
not implement Clone. Keep all test queues/fixtures count+byte bounded.

- [x] **Step 5: Verify and publish the task**

Run the Task 1 gate set plus `cargo test --test runtime_intake --locked`,
`cargo test --test lark_bridge --locked`, and `cargo test --test store --locked`.
Commit `feat: persist a replayable inbound inbox before Lark receipts`.

## Task 4: Scope Runtime — Router, Scope Actor, Global Turn Semaphore

**Files:**

- Create: `src/runtime/router.rs`
- Create: `src/runtime/scope.rs`
- Create: `tests/runtime_scope.rs`
- Modify: `src/runtime/mod.rs`
- Modify: `src/limits.rs`

**Interfaces:**

- Consumes: `StoreHandle`, `AccessPolicy`, `SupervisorHandle`, `AppServerClient` (`start_thread`, `resume_thread`, `start_turn`, `interrupt_turn`, `subscribe`, `release_thread`), `QueuedInboundEvent`.
- Produces: `Router`, `RouterHandle`, `ScopeActor`, `ScopeState`, `TurnRequest`, `ScopeSnapshot`.

- [ ] **Step 1: Implement the router with bounded actor registry and the global semaphore**

```rust
pub struct RouterHandle { /* bounded command channel */ }

impl RouterHandle {
    pub async fn route(&self, event: QueuedInboundEvent) -> Result<(), RouteError>;
    pub fn snapshot(&self) -> RouterSnapshot;               // scope count, queue depths, active turns
    pub async fn shutdown(self) -> Result<(), RouteError>;
}
```

The router owns `ScopeKey → mpsc::Sender<ScopeCommand>` (`MAX_SCOPE_ACTORS`, LRU-ish eviction only for actors that are `Idle` with empty mailboxes — never evict a busy scope), one `Arc<Semaphore>` with `ACTIVE_TURN_PERMITS` (default 4) shared by all actors, and the store/policy handles. Non-owner/non-mention, stale, and overload events use `reject_received_and_enqueue_notice`, so the deterministic user notice and terminal CAS are one transaction. Every scope mailbox has `SCOPE_MAILBOX_CAPACITY` count and `SCOPE_MAILBOX_BYTE_BUDGET` bytes (permits ride the queued item, matching the existing pattern); a full mailbox uses the same atomic API with the static `busy` reason (design §13.3). If the transaction cannot persist, the row remains received; retry it through a bounded router retry lane, falling back to Task 8's periodic durable Received rescan. Duplicate queue items keep their canonical `InboundKey`; actors coalesce duplicate keys before debounce, but correctness relies on the store transaction rather than memory dedup.

- [ ] **Step 2: Implement the scope actor state machine**

```rust
pub enum ScopeState {
    Idle,
    Debouncing,
    WaitingPermit,
    StartingTurn,
    Running { turn_row_id: i64 },
    Finalizing { turn_row_id: i64 },
    Failed { kind: ScopeFailureKind },
}
```

Exactly the design §7 machine (`Idle → Debouncing → WaitingPermit → StartingTurn → Running → Finalizing → Idle`, `Failed` reachable from `StartingTurn`/`Running`). Behavior:

- On a user message while `Idle`, start a `SCOPE_DEBOUNCE_WINDOW` (600 ms) timer; messages arriving during the window accumulate into one batch (count `TURN_BATCH_MAX_MESSAGES`, bytes `TURN_BATCH_BYTE_BUDGET`).
- After `WaitingPermit` acquires a semaphore permit, re-check message age (`TURN_MESSAGE_MAX_AGE`, stale → reject with notice), access policy, and the stored scope row's cwd/fingerprint before any RPC (design §7).
- `StartingTurn`: ensure a thread — reuse `active_thread` when the scope row's fingerprint still matches (`client.resume_thread(ThreadResumeParams::new(thread_id))`), else `client.start_thread(ThreadStartParams { cwd, sandbox, approval_policy, .. })` and persist the mapping. Re-canonicalize/re-authorize cwd immediately adjacent to every cwd-bearing RPC. Generate the client message UUID, then call `begin_turn_and_claim_inbound`: in one transaction it records the `starting` turn and attaches/accepts only the still-received subset. Assemble Codex input only from the returned claimed keys; if none remain, create no turn/RPC. Finally call `client.start_turn(TurnStartParams::new(thread_id, inputs))` with that persisted UUID. A crash before the transaction leaves rows received/replayable; a crash after it leaves every accepted row attached to the recoverable starting turn; a crash mid-RPC is uncertain and never blindly resent.
- While `Running`, further user messages go to the next-batch buffer (never `turn/steer` in this milestone); `/stop` is the only thing that touches the live turn.
- `Finalizing` waits for the authoritative `TurnOutcome` from the `ThreadSubscription` (never trust deltas for completion), hands the projector its result (Task 6) so any deterministic outbox rows are durable first, and calls `resolve_turn_and_finish_inbound_batch` to atomically resolve the turn plus its linked inbound set and clear payloads. It then releases the permit and drains the next batch or goes `Idle`. User-visible policy/age/overload paths use `reject_received_and_enqueue_notice`; the bare reject API is only for explicitly silent internal cases. No generic accepted or separate turn/inbound terminal transition is used.
- Interrupt (`/stop`) calls `client.interrupt_turn` and then waits for `turn/completed` or `SCOPE_INTERRUPT_RECOVERY_TIMEOUT`; a new turn for the same scope cannot start while the old one is still active (design §7).
- Supervisor transitions: `SupervisorHandle::changed()` delivering a non-`Ready` state durably enqueues the deterministic uncertainty notice, then atomically resolves the in-flight turn and inbound set as uncertain (the epoch died mid-flight); actors pause turn starts until `Ready` returns. Startup `thread/resume` remains lazy per scope rather than a storm, but a scope-specific recovery barrier always reconciles that scope's old work before it can start any newly received turn (design §13.1).

- [ ] **Step 3: Test the actor against the fake app-server**

Reuse `AppServerSupervisor::start_with_factory` with a scripted fake process factory: extract the private `FakeFactory`/`FakeControl` harness from `tests/supervisor.rs` into a shared integration-test helper module (e.g. `tests/fakecodex/mod.rs`) as part of this task, without changing `tests/supervisor.rs`'s scenarios. Cover: two messages inside 600 ms land in one turn (fake sees one `turn/start` with combined input); duplicate queue copies and concurrent redelivery yield one claim/turn; a mixed batch omits an already-claimed key without stranding received siblings; crash injection before the atomic begin leaves received/replayable while injection after it leaves accepted rows linked to one starting turn; a message during `Running` produces a second turn only after the first completes; semaphore saturation queues a second scope's turn and starts it when the first finishes; permit re-check rejects a message that aged out; thread reuse on matching fingerprint vs `start_thread` on fingerprint change; `release_thread` on scope eviction/shutdown so routing state cannot leak; interrupt → waits for `turn/completed` before accepting new work; supervisor `Backoff` first durably enqueues the uncertainty notice, then atomically resolves the turn as `uncertain` plus its inbound set, and blocks new turns until `Ready`; mailbox overflow rejects with a busy notice; every accepted row has `turn_row_id`, and terminal/rejected paths leave none permanently orphaned.

- [ ] **Step 4: Verify and publish the task**

Run the Task 1 gate set plus `cargo test --test runtime_scope --locked`, and re-run `cargo test --test supervisor --locked` to prove no foundation regression. Commit `feat: add scope router, scope actors, and global turn semaphore`.

## Task 5: First-Stage Commands — `/new` `/stop` `/status` `/cd` `/help`

**Files:**

- Create: `src/runtime/commands.rs`
- Modify: `src/runtime/scope.rs`
- Modify: `src/runtime/router.rs`
- Modify: `tests/runtime_scope.rs`
- Modify: `src/limits.rs`

**Interfaces:**

- Consumes: `ScopeActor` mailbox, `StoreHandle` sessions queries, `AccessPolicy`, `LarkApi` (via outbox).
- Produces: `BridgeCommand`, `CommandOutcome`.

- [ ] **Step 1: Parse and route commands through the scope mailbox**

```rust
pub enum BridgeCommand { New, Stop, Status, Cd { path: PathBuf }, Help }
```

Text starting with `/` is parsed before batching; a recognized command becomes a `ScopeCommand::Command` item in the same mailbox as user messages, so commands and messages are totally ordered per scope (design §12). Unrecognized `/`-prefixed text is not a command and flows to Codex as a normal message with no silent dropping. Commands never silently discard queued messages: each handler's reply states what happened to pending work.

- [ ] **Step 2: Implement the five handlers with reference-matching behavior**

- `/new`: archive the scope's active thread (`archive_active_thread`), keep the workspace, drop the pending next-batch buffer (reported in the reply), interrupt a running turn first; reply confirms "new session started" (and "interrupted the running task" when applicable) — matching the reference `handleNew`, minus the `chat` subcommand and Codex-history resume candidates (milestone 4).
- `/stop`: interrupt the active turn; with no active turn answer passively ("no running task") — the reference sends no reply in the p2p case, but the bridge answers with a short status line so the behavior is testable end-to-end; record the deviation in module docs.
- `/status`: reply with connection state (Lark transport state, supervisor state/epoch), scope key, cwd, thread id (or none), turn state, queue depths (mailbox, next-batch, outbox count+bytes), and the effective permission summary — never secrets, owner IDs, or message content (design §12, reference `handleStatus`).
- `/cd <path>`: owner-only (enforced again at the handler, not just at intake); validate through `AccessPolicy::validate_workspace`; on success archive the active thread, update the scope row (cwd + new fingerprint), interrupt a running turn, and reply with the new canonical cwd — matching the reference `handleCd` including its "session reset" notice.
- `/help`: reply listing the five commands with one-line usage, driven by a single table so `/status` and `/help` cannot drift apart.

All replies go through the outbox (Task 6 can land after this task; until then a minimal direct-send shim behind the same `enqueue_outbox` call shape is acceptable only if this task is implemented after Task 6 — implementers: prefer landing Task 6 first if it avoids the shim).

- [ ] **Step 3: Test command semantics**

Extend `tests/runtime_scope.rs`: `/new` during a run interrupts then archives (fake observes `turn/interrupt`, store shows `archived`); `/new` keeps cwd; `/stop` with no turn gets the passive reply; `/status` output contains scope/cwd/thread/queue fields and provably no owner ID or secret (assert on captured reply text); `/cd` to a rejected path keeps the old workspace and reports why; `/cd` by a non-owner is denied; `/cd` archives the thread and changes the fingerprint (next message starts a fresh thread); `/help` lists exactly the five commands; an unknown `/frobnicate` reaches the fake as user input; a command queued behind a running turn executes after it finishes (mailbox ordering).

- [ ] **Step 4: Verify and publish the task**

Run the Task 1 gate set plus `cargo test --test runtime_scope --locked`. Commit `feat: add first-stage bridge commands`.

## Task 6: Reply Projector and Durable Outbox

**Files:**

- Create: `src/render/mod.rs`
- Create: `src/outbox/mod.rs`
- Create: `tests/reply_projector.rs`
- Create: `tests/outbox.rs`
- Create: `tests/fixtures/runtime/turn_*.json` (projector fixtures: final-only, clean-empty, streamed+final, stream-failure, late-final)
- Modify: `src/lib.rs`
- Modify: `src/limits.rs`

**Interfaces:**

- Consumes: `AppServerEvent`/`TurnOutcome` (via `ThreadSubscription`), `ThreadItem` (`AgentMessage { phase }`, `Reasoning`, `CommandExecution`, `FileChange`), `LarkApi::{reply_text, reply_text_in_thread, send_text, update_card}`, `StoreHandle` outbox queries.
- Produces: `ReplyProjector`, `ProjectedReply`, `OutboxPump`, `OutboxHandle`.

- [ ] **Step 1: Implement the reply projector and its hard contracts**

```rust
pub struct ReplyProjector { /* per-turn projection state */ }

pub enum ProjectorOutput {
    ProgressUpsert { text: String },        // create-or-update the progress message
    Final { text: String },                 // the standalone final answer
    Nothing,                                // clean-empty turn: send nothing
}
```

Feed the projector `AppServerEvent`s for the scope's thread; it maintains reasoning/command/file-change/agent-message sections and the streamed agent text. Enforce design §9 as tests, not comments:

1. The final answer (last agent message with `MessagePhase::FinalAnswer`, or the trailing agent message at `TurnCompleted` when no phase marker exists) is a standalone message, never mixed into the progress view.
2. A final-only turn creates no progress message at all.
3. A clean-empty turn (no visible output, empty final) creates, sends, and recalls nothing.
4. A progress-send failure never swallows the final answer — the final is enqueued regardless.
5. Text already streamed into the progress view is not re-sent when the turn ends without an independent final (choose: no second message, and the progress message is finalized in place; document the choice).
6. A final reply counts as delivered only after Lark returns a non-empty `message_id` (stored as the outbox receipt).

Progress updates are rate-limited (`REPLY_UPDATE_MIN_INTERVAL` e.g. 1.5 s and `REPLY_UPDATE_MIN_CHARS` e.g. 200 since the last upsert) and length-capped (`REPLY_MESSAGE_MAX_CHARS`, chunked or truncated with an explicit marker). All agent-generated outbound strings pass the email audit mask (`user@example.com` → `user[at]example.com`) with package names, versions, and `@mention` markers provably untouched (design §9).

- [ ] **Step 2: Implement the durable outbox pump**

```rust
pub struct OutboxHandle { /* enqueue + depth queries via StoreHandle */ }

impl OutboxPump {
    pub fn spawn(store: StoreHandle, api: LarkApi, transport_state: watch::Receiver<TransportState>) -> OutboxHandle;
}
```

Enqueue first, send second (design §9): projector and commands only ever write outbox rows. Idempotency keys are deterministic — `<turn_row_id>:final`, `<turn_row_id>:progress`, `<event_id>:cmd:<name>`, `<turn_row_id>:busy` — so a restart re-sends the pending row instead of creating a second business record. The pump claims batches (`claim_outbox_batch`, `OUTBOX_CLAIM_BATCH`), sends via `LarkApi` (reply to the originating `message_id`, `reply_text_in_thread` inside topic scopes, `update_card` for progress upserts once a `receipt_message_id` exists), and on success stores the receipt; on failure it classifies: `Retryable` → exponential backoff with attempt cap (`OUTBOX_MAX_ATTEMPTS`), `PermanentAuth` → `failed` with the row kept for diagnostics. When the API call's outcome is unknowable (timeout/disconnect after the request may have been sent) the row becomes `uncertain_delivery` and is never auto-resent as if it had failed — it surfaces in `/status` and logs until resolved manually (handoff §2 rule 6). While the Lark transport is disconnected the pump keeps projecting into SQLite and sends in original order after reconnect (design §13.2). Outbox growth is bounded by `OUTBOX_MAX_ROWS`/`OUTBOX_MAX_BYTES`; enqueue beyond the bound fails the producer (which turns the turn into a `failed` reply attempt, never unbounded memory).

- [ ] **Step 3: Test the projector contracts and the outbox**

Projector fixtures drive scripted `AppServerEvent` sequences through the projector (no Codex needed): assert the exact `ProjectorOutput` sequence for each contract case above, update rate-limiting, length capping, and the masking table. Outbox tests use the Lark HTTP stub from `tests/lark_api.rs`: idempotent enqueue under restart; claim ordering by `created_ms`; success stores receipt and transitions `sent`; retryable failure backs off and eventually succeeds; timeout-after-possible-send becomes `uncertain_delivery` and is excluded from automatic retry; permanent failure exhausts attempts; progress upsert uses `update_card` after the first receipt exists; depth limits reject overflow enqueues.

- [ ] **Step 4: Verify and publish the task**

Run the Task 1 gate set plus `cargo test --test reply_projector --locked` and `cargo test --test outbox --locked`. Commit `feat: add reply projector and durable outbox`.

## Task 7: Bounded Attachment Cache with Leases and GC

**Files:**

- Create: `src/runtime/attachments.rs`
- Create: `tests/attachments.rs`
- Modify: `src/runtime/scope.rs` (turn input assembly)
- Modify: `src/limits.rs`

**Interfaces:**

- Consumes: `LarkApi::download_message_resource`, `ResourceDesc`, `StoreHandle` attachment queries, `sha2`.
- Produces: `AttachmentCache`, `AttachmentLease`, `CachedAttachment`.

- [ ] **Step 1: Implement the content-addressed cache**

```rust
pub struct AttachmentCache { dir: PathBuf, store: StoreHandle, limits: AttachmentLimits }

pub struct CachedAttachment { pub sha256: String, pub path: PathBuf, pub kind: ResourceKind, pub bytes: u64 }

impl AttachmentCache {
    pub async fn fetch(&self, message_id: &str, desc: &ResourceDesc, turn_row_id: i64) -> Result<CachedAttachment, AttachError>;
    pub async fn release_turn(&self, turn_row_id: i64) -> Result<(), AttachError>;
    pub async fn gc(&self) -> Result<GcStats, AttachError>;
}
```

Design §10 exactly: validate declared kind and platform-reported size before downloading; stream the download (already capped mid-body at `LARK_MAX_RESOURCE_BYTES` by `LarkApi`) while computing SHA-256, enforcing per-item (`ATTACHMENT_MAX_BYTES`, reuse `LARK_MAX_RESOURCE_BYTES`) and per-batch total (`ATTACHMENT_TURN_TOTAL_BYTES`) caps; write to a random temp name (UUID) in the cache dir, `fsync`, then atomic rename to `<sha256>`; upsert the `attachments` row with a per-turn lease row in the same transaction. Concurrent fetches of the same hash share one content record and independent lease rows — one turn's cleanup can never delete another turn's file. Failure paths always remove the temp file. `fetch` on a hash already present only re-validates size and adds the lease (no re-download).

- [ ] **Step 2: Wire attachments into turn input and implement GC**

Scope actor turn assembly maps each fetched attachment to Codex input: images → `UserInput::LocalImage { path, detail: None }` with the canonical cache path (never the user file name); files → the cache path embedded in the structured user text context (design §10). The lease lives until the turn reaches a terminal state (`release_turn` in `Finalizing`), and the sweeper (`ATTACHMENT_GC_INTERVAL`) deletes content rows with zero leases whose `last_used_ms` is older than `ATTACHMENT_GC_AGE`, always bounded by the cache's total byte cap (`ATTACHMENT_CACHE_TOTAL_BYTES`, LRU eviction by `last_used_ms` among unleased entries first). GC never deletes a leased object, and it reconciles on-disk files with the `attachments` table at startup (orphan temp files deleted, missing files dropped from the table).

- [ ] **Step 3: Test the cache**

Against the Lark HTTP stub: kind/size pre-validation rejects before any I/O; SHA-256 matches content; oversize stream aborts and leaves no file; temp file removed on mid-download failure; same-hash concurrency yields one file and two leases; turn release + GC removes the file while a second turn's lease protects it; cache byte cap evicts the oldest unleased entry; startup reconciliation clears orphan temps and stale rows; `Debug` of every public type shows hash/kind/size only.

- [ ] **Step 4: Verify and publish the task**

Run the Task 1 gate set plus `cargo test --test attachments --locked` and `cargo test --test runtime_scope --locked`. Commit `feat: add bounded content-addressed attachment cache`.

## Task 8: App Assembly, `run` Command, Fault Recovery, and Real Smoke

**Files:**

- Create: `src/app.rs`
- Create: `tests/runtime_recovery.rs`
- Create: `tests/runtime_smoke.rs`
- Modify: `src/cli.rs`
- Modify: `src/runtime/router.rs`
- Modify: `README.md`
- Modify: `src/limits.rs`

**Interfaces:**

- Consumes: every module above plus `AppServerSupervisor`, `LarkBridge::start_with_runtime`.
- Produces: `app::run(BridgeConfig) -> Result<()>`, CLI `run`, gated real smoke.

- [ ] **Step 1: Assemble the application and the `run` subcommand**

Startup order is fail-closed: load+validate config → open store/migrate →
`DurableIntake::prepare` performs the global legacy/current-tenant inbox
integrity scan and builds the complete received recovery set → validate every
accepted↔turn↔scope association and build a bounded reconciliation worklist →
start `AppServerSupervisor` → start `LarkBridge` with that already-validated
runtime (it preloads received before opening WebSocket) → attach the receiver
and spawn outbox pump, router, sweeper/GC tasks → install signal handlers. No
WebSocket may connect before the pure-store checks complete. A failure after
the supervisor starts but before the app is fully assembled must shut the
supervisor down within its deadline. Shutdown (SIGINT/SIGTERM) reverses the
order: stop intake (drop the transport), drain or deadline-bound in-flight turns
(`APP_SHUTDOWN_GRACE`), flush the outbox pump's claimed batch back to `pending`,
shut down the supervisor (no orphan app-server), close the store writer.
`lark-codex-bridge run --config <path>` runs this; it prints one sanitized
startup line (tenant, scope caps, db path) and then JSON `tracing` with the
design §14 field set (`profile`, `scope_hash`, `thread_id`, `turn_id`,
`message_id`, `connection_epoch`, phase, elapsed, error class).

- [ ] **Step 2: Implement restart and uncertainty recovery**

Before transport startup, the runtime validates the complete v2 state matrix:
received rows have no turn, and terminal rows never point to a live turn.
Normally accepted rows have one same-scope unresolved turn. The sole repairable
exception is an accepted row linked to a terminal/resolved turn from an older
or partially upgraded writer; it enters the deterministic reconciliation
worklist rather than failing before that worklist can be built. An unresolved starting/running
runtime turn has exactly `inbound_count` accepted rows. A resolved runtime turn
has no accepted rows and may retain 0..=`inbound_count` matching terminal
markers after partial/full TTL sweep. It validates that every v2 `accepted`
inbound row has a valid `turn_row_id`, every association points to the same
scope, and the distinct turn/count+byte recovery worklist is bounded; an
orphan/mismatch/overflow fails closed. Accepted rows linked to a turn that is
already terminal are also put on the reconciliation worklist, so a crash
between turn terminalization/outbox projection/inbound finalization cannot
leave them permanently accepted. A resolved uncertain turn on this worklist may
only be idempotently terminalized/cleaned up; it is never resumed or re-executed.
`turns` rows in `starting`/`running` are by
definition uncertain (the process died); any inconsistent legacy `uncertain`
turn that still owns accepted inbound is recovery work too. The bounded
worklist initializes a per-scope recovery barrier in the router: new events for
that scope may queue within its count+byte mailbox, but cannot acquire an active
turn permit or issue any RPC until old work is reconciled. For each, resume the mapped thread
lazily on the scope's next activity and use the `resume_thread` response's
`thread.turns` (the typed client has no `thread/read` yet —
`ThreadResumeResult` already embeds `Thread { turns: Vec<Turn> }`, whose
`Turn.status` distinguishes `completed`/`interrupted`/`failed`/`inProgress`) to
classify the old turn. A terminal Codex turn is projected idempotently from its
returned items, then `resolve_turn_and_finish_inbound_batch` atomically resolves
its store turn and linked inbound batch.

Still `inProgress` or unknown (including a crash after durable begin but before
the RPC) becomes `uncertain`; atomically resolve the turn/inbound set only after
a deterministic, durable user-notice outbox row exists, and ask the user to
re-issue the request,
never blindly resend `turn/start` (design §13.1). Pending `outbox` rows resume
delivery automatically; `uncertain_delivery` rows stay parked and visible in
`/status`. `sending` rows from the dead process return to `pending` exactly once
(claim recovery), protected by the idempotency key. A cancellation-aware runner
also rescans the current tenant's bounded Received set at startup and a clamped
interval, trying to re-offer rows left durable by transient router/store/outbox
failures; duplicate offers are harmless because the atomic begin/reject gates
remain authoritative.

- [ ] **Step 3: Fault-injection integration tests**

`tests/runtime_recovery.rs` drives the whole runtime minus real Lark (in-process WS server + HTTP stub) and minus real Codex (scripted fake factory):

1. **Child exit mid-turn**: fake app-server dies during `Running` → supervisor restarts → turn row is `uncertain`, user gets an explicit notice, no duplicate `turn/start`, next user message resumes the thread on the new epoch.
2. **Bridge restart mid-turn**: kill the runtime after `turn/start` succeeded; restart against the same DB and a fake that reports the old turn `completed` on resume → the final answer is delivered exactly once (one outbox row, one receipt).
3. **Duplicate redelivery**: same `event_id` delivered twice across a restart → one Codex turn, two `{code: 200}` receipts.
4. **Send uncertainty**: Lark stub accepts a reply then drops the connection before responding → row becomes `uncertain_delivery`, is not auto-resent, and appears in `/status`.
5. **Interrupt then resume**: `/stop` mid-turn → `turn/interrupt` observed → actor waits for `turn/completed` before the next turn starts; queued messages run as a fresh turn on the same thread.
6. **Lark disconnect during a turn**: transport down → events still project to the outbox → reconnect → replies arrive in order.

- [ ] **Step 4: Add the opt-in real Lark + Codex smoke test**

`tests/runtime_smoke.rs` is `#[ignore = "requires real Feishu/Lark app credentials and a real Codex login"]` and requires both `LARK_E2E=1` and `CODEX_E2E=1`; without them it prints the skip reason and exits successfully — a skip is explicitly not evidence. When enabled it requires `LARK_E2E_APP_ID`, `LARK_E2E_APP_SECRET`, `LARK_E2E_TENANT`, `LARK_E2E_CHAT_ID`, and `LARK_E2E_OWNER_OPEN_ID` (the sender the config must list as owner), then: writes a temp config (temp DB, temp cache dir, the test chat's owner), starts `app::run` in-process, uses a second `LarkApi` client to send `runtime-smoke <unix-ts> ping` (p2p chat or @bot in the test group), waits up to 300 s for a bot reply containing the token, sends `/status` and asserts the reply carries scope/thread fields, sends `/new` and asserts the confirmation, then asserts the DB shows the completed turn, exactly one final outbox row with a receipt, and no `uncertain` rows; shutdown leaves no orphan app-server (exact `ps` parent/PID check as in milestone 2) and no leftover temp files.

- [ ] **Step 5: Verify and publish the milestone**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-targets --locked
cargo +1.85.0 check --all-targets --all-features --locked
cargo build --release --locked
git diff --check
cargo test --test runtime_recovery --locked -- --nocapture
LARK_E2E=1 CODEX_E2E=1 LARK_E2E_APP_ID=… LARK_E2E_APP_SECRET=… LARK_E2E_TENANT=feishu \
  LARK_E2E_CHAT_ID=… LARK_E2E_OWNER_OPEN_ID=… \
  cargo test --test runtime_smoke --locked -- --ignored --nocapture
```

Commit `feat: assemble bridge runtime with recovery and real smoke`.

## Milestone Completion Evidence

The milestone is complete only when:

1. All task commits exist on public `main` and CI is green on Linux quality, Rust 1.85, macOS, and Windows (`gh run list --branch main --limit 5` / `gh run view <id>` per handoff §7.2).
2. The full local gate passes: `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features --locked -- -D warnings`, `cargo test --all-targets --locked`, `cargo +1.85.0 check --all-targets --all-features --locked`, `cargo build --release --locked`, `git diff --check`.
3. The fault-recovery suite (`cargo test --test runtime_recovery --locked -- --nocapture`) passes with its six scenarios observable in the output: child-exit-mid-turn uncertainty, bridge-restart exactly-once final delivery, cross-restart dedup, uncertain send not auto-resent, interrupt-then-resume ordering, and Lark-reconnect ordered delivery.
4. The gated real smoke (`LARK_E2E=1 CODEX_E2E=1` plus the five env vars) proves inbound message → real Codex turn → delivered final reply → `/status` → `/new` in one run against a real tenant, with the DB inspected afterwards (one completed turn, one final outbox row with receipt, zero uncertain rows). Running the suite without credentials and observing the skip message does not count, exactly as in the previous milestone.
5. After the smoke, `ps` inspection shows no orphan `codex app-server --listen stdio` child and no orphan bridge tasks, and a redaction sweep (test-level assertions plus `git grep` review) confirms no user text, prompt, tool output, attachment content, owner open_id in full, App Secret, or tenant token appears in logs, `Debug` output, errors, or the database's non-payload columns.
6. Unit/integration coverage proves: WAL + foreign keys + single-writer serialization; migration idempotence across reopen; dedup TTL semantics and illegal-transition rejection; config safe defaults and workspace rejection set; owner-only and group-mention gates; 600 ms batching, next-turn buffering, semaphore queuing, permit re-checks; thread resume vs fingerprint-change restart; the five reply hard contracts plus masking; outbox idempotency, backoff, receipt, and `uncertain_delivery`; attachment bounds, hashing, lease protection, and GC.

## Subsequent Milestone Plans

After this milestone, create and execute:

1. `2026-08-12-core-parity-platform.md`: `/ws` `/resume` `/timeout` `/doctor` `/reconnect`, `/invite` `/remove` and admins, approval cards with HMAC + one-time nonces (`approvals` and `callback_nonces` tables are intentionally deferred to that milestone), profile/`lark-cli` isolation, service managers, document comments, team mode (after security review), kill -9/network/slow-consumer fault injection, 24 h soak, same-host benchmarks against the reference Node implementation, and install/ops/release documentation.
