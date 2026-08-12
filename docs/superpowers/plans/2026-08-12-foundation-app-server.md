# Foundation and Codex App Server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a runnable Rust binary that owns one long-lived `codex app-server`, completes initialize/thread/turn/interrupt flows, streams authoritative events, and passes a real Codex smoke test.

**Architecture:** A process adapter supplies bounded JSONL transport to a single RPC actor. The actor correlates client responses, routes server requests and notifications, and exposes a typed `AppServerClient`; a supervisor adds version gating, connection epochs, restart state, and graceful shutdown.

**Tech Stack:** Rust 2024, Tokio, Serde JSON, Clap, tracing, UUID, SemVer, thiserror/anyhow, assert_cmd, tempfile.

## Global Constraints

- Use Rust edition 2024 with `rust-version = "1.85"`; CI tests stable Rust on Linux, macOS, and Windows.
- Support `codex-cli >=0.146.0,<0.147.0` in this milestone and reject other versions with an actionable error.
- Start `codex app-server --listen stdio://`; never invoke `codex exec`.
- Do not enable `initialize.capabilities.experimentalApi`.
- Bound every channel and JSONL line; use a 32 MiB line limit and capacities defined in `src/limits.rs`.
- Treat `item/completed` and `turn/completed` as authoritative; unknown methods and fields must not crash the client.
- Do not log prompts, tool output, authorization values, App Secret, access tokens, or raw protocol lines.
- Do not add Claude, Web UI, meeting, Feishu, SQLite, or daemon dependencies in this milestone.
- Follow the user's efficient-development preference: write boundary tests and fixtures, then implement the complete slice; do not require a separate failing-test commit for every private helper.
- Commit and push after each numbered task whose checks pass.

---

## File Map

- `Cargo.toml`: package metadata, dependency versions, binary/library targets, lint policy.
- `rust-toolchain.toml`: stable toolchain with rustfmt and clippy.
- `.gitignore`: Cargo output, local state, generated schemas, logs, editor files.
- `.github/workflows/ci.yml`: format, clippy, test, and build matrix.
- `src/main.rs`: process entry point and exit reporting.
- `src/lib.rs`: public module boundary used by integration tests and future bridge runtime.
- `src/cli.rs`: Clap command definitions and `codex probe` execution.
- `src/limits.rs`: all bounded-capacity and timeout constants.
- `src/codex/protocol.rs`: JSON-RPC-like envelopes, IDs, parser, encoder, open enums.
- `src/codex/types.rs`: typed stable initialize/thread/turn DTOs.
- `src/codex/process.rs`: Codex version probe and child process ownership.
- `src/codex/transport.rs`: bounded stdin/stdout/stderr tasks over generic async streams.
- `src/codex/rpc.rs`: connection epoch, request correlation, priorities, deadlines, server requests.
- `src/codex/client.rs`: typed initialization, thread, turn, interrupt, event subscription.
- `src/codex/supervisor.rs`: state machine, restart/backoff, and graceful shutdown.
- `src/codex/mod.rs`: stable exports for the rest of the bridge.
- `tests/cli.rs`: binary help/version/probe error contracts.
- `tests/protocol_fixtures.rs`: fixtures generated from Codex 0.146.0 shapes.
- `tests/rpc_duplex.rs`: deterministic in-memory bidirectional RPC tests.
- `tests/supervisor.rs`: fake process factory restart and epoch tests.
- `tests/codex_smoke.rs`: ignored, opt-in real binary thread/turn test.
- `tests/fixtures/codex/*.json`: scrubbed initialize/thread/turn/item samples.

## Task 1: Rust Project, CLI Shell, and CI

**Files:**

- Create: `Cargo.toml`
- Create: `rust-toolchain.toml`
- Create: `.gitignore`
- Create: `.github/workflows/ci.yml`
- Create: `src/lib.rs`
- Create: `src/main.rs`
- Create: `src/cli.rs`
- Create: `src/limits.rs`
- Create: `tests/cli.rs`

**Interfaces:**

- Produces: `cli::run() -> anyhow::Result<()>` and `cli::Cli` for the binary.
- Produces: `limits::{MAX_JSONL_LINE_BYTES, RPC_HIGH_CAPACITY, RPC_NORMAL_CAPACITY, EVENT_CAPACITY}`.

- [x] **Step 1: Create package metadata and exact dependency set**

Use package name `lark-codex-bridge`, version `0.1.0-alpha.1`, edition `2024`, and MSRV `1.85`. Pin compatible dependency families to these current versions:

```toml
[dependencies]
anyhow = "1.0.104"
bytes = "1.12.1"
clap = { version = "4.6.6", features = ["derive"] }
futures-util = "0.3.34"
semver = "1.0.28"
serde = { version = "1.0.229", features = ["derive"] }
serde_json = "1.0.151"
thiserror = "2.0.20"
tokio = { version = "1.53.1", features = ["macros", "process", "rt-multi-thread", "signal", "sync", "time", "io-util"] }
tokio-util = { version = "0.7.19", features = ["codec", "rt"] }
tracing = "0.1.44"
tracing-subscriber = { version = "0.3.23", features = ["env-filter", "fmt", "json"] }
uuid = { version = "1.24.0", features = ["v4", "serde"] }

[dev-dependencies]
assert_cmd = "2.2.2"
predicates = "3.1.4"
tempfile = "3.27.0"
```

- [x] **Step 2: Add the CLI shell and bounded constants**

Define these initial commands; `codex probe` is implemented in Task 6:

```rust
#[derive(clap::Parser)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(clap::Subcommand)]
pub enum Command {
    Codex { #[command(subcommand)] command: CodexCommand },
}

#[derive(clap::Subcommand)]
pub enum CodexCommand {
    Probe { #[arg(long, default_value = "codex")] binary: PathBuf },
}
```

Set `MAX_JSONL_LINE_BYTES = 32 * 1024 * 1024`, high/normal RPC capacities to `64/256`, event capacity to `1024`, initialize timeout to 10 seconds, control RPC timeout to 30 seconds, and interrupt timeout to 10 seconds.

- [x] **Step 3: Add CI and CLI contract tests**

CI runs `cargo fmt --check`, `cargo clippy --all-targets --all-features -- -D warnings`, `cargo test --all-targets`, and `cargo build --release` on Linux; macOS and Windows run `cargo test --all-targets` and `cargo build`. `tests/cli.rs` verifies `--help`, `--version`, and a missing `codex` binary returns a non-zero code without a panic backtrace.

- [x] **Step 4: Verify and publish the task**

Run:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
cargo build --release
```

Commit `chore: bootstrap Rust project` and push `main`.

## Task 2: Versioned Protocol Envelopes and Stable DTOs

**Files:**

- Create: `src/codex/mod.rs`
- Create: `src/codex/protocol.rs`
- Create: `src/codex/types.rs`
- Create: `tests/protocol_fixtures.rs`
- Create: `tests/fixtures/codex/initialize_response.json`
- Create: `tests/fixtures/codex/thread_start_response.json`
- Create: `tests/fixtures/codex/turn_started.json`
- Create: `tests/fixtures/codex/agent_delta.json`
- Create: `tests/fixtures/codex/item_completed.json`
- Create: `tests/fixtures/codex/turn_completed.json`

**Interfaces:**

- Produces: `RequestId`, `InboundMessage`, `OutboundMessage`, `decode_line`, and `encode_line`.
- Produces: `InitializeParams`, `ThreadStartParams`, `ThreadResumeParams`, `TurnStartParams`, `TurnInterruptParams`, their result types, `UserInput`, `ThreadItem`, and `TurnStatus`.

- [x] **Step 1: Implement open wire envelopes**

Use an untagged opaque ID and explicit classification so interleaved notifications cannot be mistaken for the next response:

```rust
#[derive(Clone, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum RequestId { String(String), Integer(i64) }

pub enum InboundMessage {
    Response { id: RequestId, result: Value },
    ErrorResponse { id: RequestId, error: RpcErrorObject },
    Request { id: RequestId, method: String, params: Value },
    Notification { method: String, params: Value },
}

pub fn decode_line(line: &[u8]) -> Result<InboundMessage, ProtocolError>;
pub fn encode_line(message: &OutboundMessage) -> Result<Vec<u8>, ProtocolError>;
```

Reject missing/ambiguous envelope fields and lines over the global limit. Accept additive unknown fields. Encoding appends exactly one newline and omits the `jsonrpc` member.

- [x] **Step 2: Implement only the stable typed DTO subset**

Represent changing item payloads as a tagged `ThreadItem` with known variants plus `Unknown { item_type, raw }`. Use exact wire names through Serde. `SandboxMode` serializes `read-only`, `workspace-write`, or `danger-full-access`; turn sandbox policy uses `readOnly`, `workspaceWrite`, or `dangerFullAccess`.

- [x] **Step 3: Add scrubbed 0.146.0 fixtures and focused tests**

Cover string/integer IDs, interleaved notification/response classification, unknown item preservation, error response decoding, newline encoding, oversized line rejection, kebab/camel sandbox serialization, and authoritative terminal DTOs.

- [x] **Step 4: Verify and publish the task**

Run:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --test protocol_fixtures
cargo test --all-targets
cargo build --release
```

Commit `feat: model Codex app-server protocol` and push `main`.

## Task 3: Process Adapter and Bounded JSONL Transport

**Files:**

- Create: `src/codex/process.rs`
- Create: `src/codex/transport.rs`
- Create: `tests/transport.rs`
- Modify: `src/codex/mod.rs`

**Interfaces:**

- Consumes: `protocol::{InboundMessage, OutboundMessage, decode_line, encode_line}`.
- Produces: `CodexProcessConfig`, `CodexProcess`, `ProcessExit`, `TransportHandle`, `TransportEvent`, and `spawn_stream_transport`.

- [x] **Step 1: Implement version probing and owned child startup**

```rust
pub struct CodexProcessConfig {
    pub binary: PathBuf,
    pub codex_home: Option<PathBuf>,
}

pub async fn probe_version(config: &CodexProcessConfig) -> Result<Version, ProcessError>;
pub async fn spawn_app_server(config: &CodexProcessConfig) -> Result<CodexProcess, ProcessError>;

impl CodexProcess {
    pub fn version(&self) -> &Version;
    pub fn take_stdio(&mut self) -> Result<(ChildStdout, ChildStdin, ChildStderr), ProcessError>;
    pub async fn wait(&mut self) -> Result<ProcessExit, ProcessError>;
    pub async fn terminate(&mut self, grace: Duration) -> Result<ProcessExit, ProcessError>;
}
```

Parse only output matching `codex-cli X.Y.Z`, enforce `>=0.146.0,<0.147.0`, start arguments `app-server --listen stdio://`, pipe all stdio, set `kill_on_drop(true)`, and never interpolate the binary through a shell. Log only binary path, PID, version, and exit status.

- [x] **Step 2: Implement generic bounded reader/writer tasks**

```rust
pub fn spawn_stream_transport<R, W, E>(
    stdout: R,
    stdin: W,
    stderr: E,
    cancellation: CancellationToken,
) -> TransportHandle
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
    E: AsyncRead + Unpin + Send + 'static;
```

The reader uses `LinesCodec::new_with_max_length`; the writer owns stdin and flushes each message. Stderr is line-limited and emits redacted metadata rather than protocol events. `TransportHandle` exposes separate bounded high/normal senders and one event receiver. High-priority messages always drain before normal messages without starving normal traffic indefinitely.

- [x] **Step 3: Test framing, pressure, cancellation, and EOF**

Use `tokio::io::duplex` to verify partial reads, several messages in one read, line overflow, malformed JSON isolation, high priority ordering, closed stdin, cancellation, stdout EOF, and stderr not entering the protocol channel.

- [x] **Step 4: Verify and publish the task**

Run:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --test transport
cargo test --all-targets
cargo build --release
```

Commit `feat: add bounded app-server transport` and push `main`.

## Task 4: RPC Actor and Initialization Handshake

**Files:**

- Create: `src/codex/rpc.rs`
- Create: `tests/rpc_duplex.rs`
- Modify: `src/codex/mod.rs`

**Interfaces:**

- Consumes: `TransportHandle`, wire envelopes, timeout constants.
- Produces: `RpcHandle`, `RpcEvent`, `ServerRequest`, `ConnectionEpoch`, and `initialize_connection`.

- [ ] **Step 1: Implement one RPC owner task**

```rust
pub struct RpcHandle {
    high_tx: mpsc::Sender<RpcCommand>,
    normal_tx: mpsc::Sender<RpcCommand>,
    epoch: ConnectionEpoch,
    initialized: Arc<AtomicBool>,
}

impl RpcHandle {
    pub async fn request<P, R>(&self, method: &'static str, params: &P, timeout: Duration) -> Result<R, RpcError>
    where P: Serialize + ?Sized, R: DeserializeOwned;
    pub async fn notify<P: Serialize + ?Sized>(&self, method: &'static str, params: &P) -> Result<(), RpcError>;
    pub async fn respond<R: Serialize + ?Sized>(&self, id: RequestId, result: &R) -> Result<(), RpcError>;
    pub async fn respond_error(&self, id: RequestId, code: i64, message: &str) -> Result<(), RpcError>;
    pub fn epoch(&self) -> ConnectionEpoch;
}
```

Generate IDs as `c:<epoch>:<monotonic-u64>`. Maintain one pending map with method/deadline/oneshot. On EOF fail every pending call with `ConnectionLost(epoch)`. Route notifications and server requests through an event channel. Late/unknown IDs increment drift and are ignored.

- [ ] **Step 2: Implement the exact handshake**

`initialize_connection` sends one `initialize` request with client name `lark_codex_bridge`, title `Lark Codex Bridge`, package version, and no experimental capability. Only after a successful response does it send `initialized {}`. A second call on the same connection returns `AlreadyInitialized` locally.

- [ ] **Step 3: Add deterministic duplex RPC tests**

Verify notification-before-response, concurrent out-of-order responses, timeout cleanup, error responses, opaque server integer IDs, response priority, EOF failure fanout, old-epoch late response isolation, and initialize/initialized order.

- [ ] **Step 4: Verify and publish the task**

Run:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --test rpc_duplex
cargo test --all-targets
cargo build --release
```

Commit `feat: add app-server RPC broker` and push `main`.

## Task 5: Typed Thread, Turn, Event, and Interrupt Client

**Files:**

- Create: `src/codex/client.rs`
- Create: `tests/client_flow.rs`
- Modify: `src/codex/mod.rs`

**Interfaces:**

- Consumes: initialized `RpcHandle` and stable DTOs.
- Produces: `AppServerClient`, `ThreadId`, `TurnId`, `ThreadSubscription`, `AppServerEvent`, and `TurnOutcome`.

- [ ] **Step 1: Implement thread-scoped event routing**

```rust
impl AppServerClient {
    pub async fn start_thread(&self, params: ThreadStartParams) -> Result<Thread, ClientError>;
    pub async fn resume_thread(&self, params: ThreadResumeParams) -> Result<Thread, ClientError>;
    pub async fn start_turn(&self, params: TurnStartParams) -> Result<Turn, ClientError>;
    pub async fn interrupt_turn(&self, thread_id: &ThreadId, turn_id: &TurnId) -> Result<(), ClientError>;
    pub async fn subscribe(&self, thread_id: ThreadId) -> Result<ThreadSubscription, ClientError>;
}
```

Create a bounded mailbox per subscribed thread. Route global warnings separately. Coalesce agent and command deltas by item when a mailbox nears capacity; never drop responses, server requests, `item/completed`, or `turn/completed`. Merge `turn/start` response and `turn/started` idempotently by turn ID.

- [ ] **Step 2: Implement authoritative turn projection**

`ThreadSubscription` yields raw typed events and maintains last completed item state. `TurnOutcome` contains status, optional error, completed items, and token usage; it becomes available only after `turn/completed`. An interrupted turn is a successful terminal outcome with status `Interrupted`, not a transport error.

- [ ] **Step 3: Test full fake-server flows**

Cover new thread, resumed thread, delta before item start, duplicate item terminal, turn response/notification inversion, final agent message, failed turn, interrupt acknowledgement followed by interrupted terminal, unknown notification, and mailbox pressure preserving terminal events.

- [ ] **Step 4: Verify and publish the task**

Run:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --test client_flow
cargo test --all-targets
cargo build --release
```

Commit `feat: drive Codex threads and turns` and push `main`.

## Task 6: Supervisor, Probe Command, and Real Codex Smoke

**Files:**

- Create: `src/codex/supervisor.rs`
- Create: `tests/supervisor.rs`
- Create: `tests/codex_smoke.rs`
- Modify: `src/cli.rs`
- Modify: `README.md`
- Modify: `src/codex/mod.rs`

**Interfaces:**

- Consumes: process adapter, transport, RPC handshake, typed client.
- Produces: `AppServerSupervisor`, `SupervisorHandle`, `SupervisorState`, and a functional `codex probe` command.

- [ ] **Step 1: Implement supervisor state and restart policy**

```rust
pub enum SupervisorState {
    Starting { epoch: u64 },
    Ready { epoch: u64, version: Version },
    Backoff { epoch: u64, attempt: u32, delay: Duration },
    Degraded { reason: String },
    Stopped,
}

impl AppServerSupervisor {
    pub async fn start(config: CodexProcessConfig) -> Result<SupervisorHandle, SupervisorError>;
}
```

The supervisor owns the child and cancellation token. Unexpected exit fails the epoch, increments it, then retries with jittered delays based on 0.5, 1, 2, 4, 8, 16, and 30 seconds. Version/auth/config permanent errors enter `Degraded`. Shutdown cancels tasks, closes stdin, waits 5 seconds, then kills the child and waits for exit.

- [ ] **Step 2: Implement `codex probe`**

The command prints one JSON object containing supported version, initialize user agent, platform family/OS, and epoch. It must not print Codex home, account identity, tokens, environment, or raw responses. It exits non-zero for missing binary, unsupported version, handshake timeout, or early child exit.

- [ ] **Step 3: Add fake-factory supervisor tests**

Inject a `ProcessFactory` trait so tests deterministically verify state ordering, exponential cap, epoch increments, pending request failure, permanent version failure without retry, graceful shutdown, and force kill after timeout.

- [ ] **Step 4: Add the opt-in real smoke test**

Mark `tests/codex_smoke.rs` with `#[ignore = "requires an authenticated Codex account"]`. The test requires `CODEX_E2E=1`; otherwise it exits successfully after printing a skip reason. When enabled, it starts the installed `codex`, initializes, creates an ephemeral read-only thread in a temporary cwd, starts a turn with `Reply with exactly: pong`, waits up to 180 seconds, asserts a completed agent message contains `pong`, and shuts down without an orphan child. Authentication failures fail with an actionable login diagnostic so the milestone cannot claim a false positive.

- [ ] **Step 5: Verify and publish the milestone**

Run:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
CODEX_E2E=1 cargo test --test codex_smoke -- --ignored --nocapture
cargo run -- codex probe
cargo build --release
```

Commit `feat: supervise Codex app-server` and push `main`.

## Milestone Completion Evidence

The milestone is complete only when:

1. All six task commits exist on public `main` and CI is green.
2. `cargo run -- codex probe` reports a ready 0.146.x app-server without leaking sensitive fields.
3. The real smoke test proves initialize → thread/start → turn/start → item/turn terminal over one child process.
4. The supervisor tests prove bounded transport, correlation, epoch isolation, restart, and shutdown behavior.
5. `ps` inspection after all tests shows no orphan `codex app-server` child.

## Subsequent Milestone Plans

After this milestone, create and execute these separate plans against the stable interfaces above:

1. `2026-08-12-native-lark-transport.md`: registration, tenant token, protobuf WebSocket, normalization, REST send/download, reconnect, and Lark international domain.
2. `2026-08-12-reliable-bridge-runtime.md`: SQLite migrations, inbound dedup, scope actors, concurrency, workspaces, reliable outbox, reply projector, attachments, and first-stage commands.
3. `2026-08-12-core-parity-platform.md`: approvals, resume/history, access administration, profile/lark-cli isolation, service managers, document comments, team mode, fault injection, benchmark, and release documentation.
