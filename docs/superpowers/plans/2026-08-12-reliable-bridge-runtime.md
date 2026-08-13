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

- [ ] **Step 1: Add the milestone dependency set**

Append to `[dependencies]` with exact pins (verify the lockfile resolves each; bump to the newest compatible patch if the pin fails to resolve and record the chosen version in the commit message):

```toml
rusqlite = { version = "0.40.2", features = ["bundled"] }
sha2 = "0.11.0"
```

`bundled` keeps CI hermetic on all three platforms; no ORM, no `sqlx`/`diesel` (design §15). No async SQLite wrapper crate: the writer task is a plain `std::thread` (or `tokio::task::spawn_blocking` loop) owned by this module.

- [ ] **Step 2: Implement schema, migrations, and the single-writer store**

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

- [ ] **Step 3: Implement the typed store queries used by later tasks**

Keep SQL inside `src/store/*`; callers see async methods only:

```rust
// dedup.rs
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

Legal inbound transitions are `received → accepted → completed|rejected` (plus `received → rejected`); anything else is a `StoreError` so a duplicate redelivery in TTL can never restart Codex. Turn and outbox state machines are enforced the same way.

- [ ] **Step 4: Test the store**

Cover: pragmas actually applied (`PRAGMA journal_mode` returns `wal`, foreign keys enforced by a failing insert); migrations apply once and survive reopen (`user_version` persisted); concurrent writers serialize (N tasks × M writes, final count exact); channel bound rejection; dedup register/duplicate/same-message-different-event; illegal transition rejection; TTL sweep; outbox idempotent enqueue (same `idempotency_key` returns the existing row), claim is atomic under concurrent claimers, complete records the receipt; foreign-key cascade behavior for attachment leases; reopen persistence for every table.

- [ ] **Step 5: Verify and publish the task**

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

- [ ] **Step 1: Implement the bridge config schema with safe defaults**

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

- [ ] **Step 2: Implement the access policy and workspace validator**

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

- [ ] **Step 3: Test config and policy**

Fixtures cover: minimal config fills safe defaults; full config round-trips; unknown key rejection; missing owner fails validation; p2p owner allowed; p2p non-owner denied; group without @ denied even for the owner; topic message with @ allowed; each workspace rejection class; fingerprint changes on cwd/sandbox/network change and is stable otherwise; `Debug` redaction (no full owner IDs, no secrets).

- [ ] **Step 4: Verify and publish the task**

Run the Task 1 gate set with `cargo test --test runtime_policy --locked`. Commit `feat: add bridge config and owner-only access policy`.

## Task 3: Durable Inbound Dedup at the Receipt Boundary

**Files:**

- Modify: `src/lark/bridge.rs`
- Create: `src/runtime/intake.rs`
- Modify: `tests/store.rs` (or create `tests/runtime_intake.rs`)
- Modify: `src/limits.rs`

**Interfaces:**

- Consumes: `StoreHandle::register_inbound/transition_inbound`, `LarkBridge::start_with`.
- Produces: `IntakeHook` wiring; extended `LarkBridge`.

- [ ] **Step 1: Extend the bridge wiring with a pre-enqueue durability hook**

`LarkBridge` gains an optional hook executed inside the transport handler before the event enters the bounded channel, so the `{code: 200}` frame receipt means "durably registered in SQLite", not "parked in memory" (design §5.3):

```rust
pub type IntakeHook = Arc<dyn Fn(InboundEvent) -> BoxFuture<'static, Result<IntakeVerdict, LarkError>> + Send + Sync>;
pub enum IntakeVerdict { Accept, DropDuplicate }
```

`LarkBridge::start_with_runtime(endpoints, creds, config, hook)` (name may settle during implementation) installs the hook; a `DropDuplicate` verdict acks `{code: 200}` without enqueuing (the platform's redelivery is legitimately absorbed). The existing `start`/`start_with` keep their exact current behavior so milestone-2 tests stay green; the hook only runs for `NormalizeOutcome::Event`, and hook failure propagates as handler failure → `{code: 500}` receipt → platform retry (safe: registration is idempotent).

- [ ] **Step 2: Implement the intake path**

`runtime::intake::durable_hook(store, tenant)` returns the hook: `register_inbound` inside one store transaction; `DedupOutcome::New` → `Accept` (state stays `received` until the scope actor accepts the work, Task 4), `Duplicate { state }` → `DropDuplicate` with a structured log (`event_id`, prior state only). A periodic sweeper (`DEDUP_TTL`, default 7 days; sweep interval `DEDUP_SWEEP_INTERVAL`, default 1 h) prunes terminal rows so the table stays bounded; the sweep budget (`DEDUP_SWEEP_BATCH`) caps rows per pass. Document that the receipt still does not mean "Codex finished" — business failure surfaces as a Lark reply (design §5.3).

- [ ] **Step 3: Test dedup at the boundary**

Against an in-process WS server (reuse the `tests/lark_bridge.rs` harness) plus a file-backed store: first delivery accepts and enqueues; redelivery of the same `event_id` within TTL acks 200 without a second enqueue and without touching Codex; same `message_id` under a new `event_id` (platform repost) is deduplicated via the message-id index per the documented rule chosen in Step 2 (pick one rule — dedup on `(tenant, event_id)` primary and additionally absorb `(tenant, message_id)` duplicates seen in a `received/accepted/completed` state — and test it); store failure → 500 receipt → redelivery later succeeds; duplicates survive a bridge restart (reopen the same DB); sweep prunes only terminal rows past TTL.

- [ ] **Step 4: Verify and publish the task**

Run the Task 1 gate set plus `cargo test --test runtime_intake --locked` and `cargo test --test lark_bridge --locked`. Commit `feat: persist inbound dedup before Lark frame receipts`.

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

The router owns `ScopeKey → mpsc::Sender<ScopeCommand>` (`MAX_SCOPE_ACTORS`, LRU-ish eviction only for actors that are `Idle` with empty mailboxes — never evict a busy scope), one `Arc<Semaphore>` with `ACTIVE_TURN_PERMITS` (default 4) shared by all actors, and the store/policy handles. Non-owner or non-mention events are already filtered by `AccessPolicy` here and marked `rejected` in the dedup table. Every scope mailbox has `SCOPE_MAILBOX_CAPACITY` count and `SCOPE_MAILBOX_BYTE_BUDGET` bytes (permits ride the queued item, matching the existing pattern); a full mailbox transitions the event to `rejected` with reason `busy` and enqueues a user-visible busy notice through the outbox (design §13.3).

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
- `StartingTurn`: ensure a thread — reuse `active_thread` when the scope row's fingerprint still matches (`client.resume_thread(ThreadResumeParams::new(thread_id))`), else `client.start_thread(ThreadStartParams { cwd, sandbox, approval_policy, .. })` and persist the mapping; then `client.start_turn(TurnStartParams::new(thread_id, inputs))` with `client_user_message_id` set to a bridge-generated UUID stored in the `turns` row before the RPC so a crash mid-call leaves an `uncertain` row instead of a silent gap.
- While `Running`, further user messages go to the next-batch buffer (never `turn/steer` in this milestone); `/stop` is the only thing that touches the live turn.
- `Finalizing` waits for the authoritative `TurnOutcome` from the `ThreadSubscription` (never trust deltas for completion), hands the projector its result (Task 6), transitions the dedup rows of the batch to `completed`, releases the permit, then drains the next batch or goes `Idle`.
- Interrupt (`/stop`) calls `client.interrupt_turn` and then waits for `turn/completed` or `SCOPE_INTERRUPT_RECOVERY_TIMEOUT`; a new turn for the same scope cannot start while the old one is still active (design §7).
- Supervisor transitions: `SupervisorHandle::changed()` delivering a non-`Ready` state fails in-flight turns as `uncertain` (the epoch died mid-flight), and actors pause turn starts until `Ready` returns; `thread/resume` happens lazily per scope on the next turn, not in a startup storm (design §13.1).

- [ ] **Step 3: Test the actor against the fake app-server**

Reuse `AppServerSupervisor::start_with_factory` with a scripted fake process factory: extract the private `FakeFactory`/`FakeControl` harness from `tests/supervisor.rs` into a shared integration-test helper module (e.g. `tests/fakecodex/mod.rs`) as part of this task, without changing `tests/supervisor.rs`'s scenarios. Cover: two messages inside 600 ms land in one turn (fake sees one `turn/start` with combined input); a message during `Running` produces a second turn only after the first completes; semaphore saturation queues a second scope's turn and starts it when the first finishes; permit re-check rejects a message that aged out; thread reuse on matching fingerprint vs `start_thread` on fingerprint change; `release_thread` on scope eviction/shutdown so routing state cannot leak; interrupt → waits for `turn/completed` before accepting new work; supervisor `Backoff` marks the in-flight turn `uncertain` and blocks new turns until `Ready`; mailbox overflow rejects with a busy notice; dedup rows end in `completed`/`rejected`, never stuck `accepted`.

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

Startup order: load+validate config → open store (migrations) → start `AppServerSupervisor` → start `LarkBridge` with the durable intake hook → spawn outbox pump, router, sweeper/GC tasks → install signal handlers. Shutdown (SIGINT/SIGTERM) reverses it: stop intake (drop the transport), drain or deadline-bound in-flight turns (`APP_SHUTDOWN_GRACE`), flush the outbox pump's claimed batch back to `pending`, shut down the supervisor (no orphan app-server), close the store writer. `lark-codex-bridge run --config <path>` runs this; it prints one sanitized startup line (tenant, scope caps, db path) and then JSON `tracing` with the design §14 field set (`profile`, `scope_hash`, `thread_id`, `turn_id`, `message_id`, `connection_epoch`, phase, elapsed, error class).

- [ ] **Step 2: Implement restart and uncertainty recovery**

On startup the runtime reconciles: `turns` rows in `starting`/`running` are by definition uncertain (the process died); for each, resume the mapped thread lazily on the scope's next activity and use the `resume_thread` response's `thread.turns` (the typed client has no `thread/read` yet — `ThreadResumeResult` already embeds `Thread { turns: Vec<Turn> }`, whose `Turn.status` distinguishes `completed`/`interrupted`/`failed`/`inProgress`) to classify the old turn: terminal → mark it accordingly and replay nothing; still `inProgress` or unknown → mark `uncertain` and reply to the user asking them to re-issue the request, never blindly resend `turn/start` (design §13.1). Pending `outbox` rows resume delivery automatically; `uncertain_delivery` rows stay parked and visible in `/status`. `sending` rows from the dead process return to `pending` exactly once (claim recovery), protected by the idempotency key.

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
