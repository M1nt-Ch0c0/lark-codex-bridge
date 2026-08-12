# Native Lark Transport and OpenAPI Implementation Plan

> **For agentic workers:** implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking; check a step only after its verification commands pass.

**Goal:** Stand up a native Rust Feishu/Lark client that onboards app credentials (PersonalAgent QR registration or an existing App ID/Secret), holds one long-lived WebSocket with protobuf `Frame` decoding, bounded fragment reassembly, ping/pong, receipt-after-handling, and jittered reconnect with permanent-auth degradation, plus a tenant-token-cached OpenAPI client and a stable `InboundEvent` normalization layer — all verified by an opt-in real Lark smoke test.

**Architecture:** A `lark::config`/`lark::credentials` layer owns tenant domains and secrets. A `lark::token` cache and a `lark::api` client share one `reqwest` (rustls) HTTP core with typed error classification. A `lark::frame` prost codec and `lark::fragments` reassembler feed a `lark::transport` actor that owns bootstrap, ping, receipt, and reconnect state. `lark::normalize` converts raw event payloads into the stable `InboundEvent` consumed by the next milestone's scope runtime. Durable dedup, SQLite state, and outbound delivery are explicitly out of scope here; the transport's handler interface is shaped so milestone 3 can plug in durable receipt semantics without changing the wire path.

**Tech Stack:** Rust 2024, Tokio, prost (pure derive, no protoc), tokio-tungstenite (rustls), reqwest (rustls, json, multipart), url, base64, flate2, secrecy, Serde JSON, Clap, tracing, thiserror/anyhow, assert_cmd, tempfile.

**Protocol source of truth:** semantics extracted (not copied) from the reference checkout at `/home/wcy/.lark-channel-workspaces/codex/default/feishu-claude-code-bridge` — specifically the bundled `node_modules/.pnpm/@larksuiteoapi+node-sdk@1.67.0/node_modules/@larksuiteoapi/node-sdk/lib/index.js` (WSClient, pbbp2 Frame, registration device flow, token cache), `node_modules/.pnpm/@larksuite+channel@0.4.0/node_modules/@larksuite/channel/dist/index.mjs` (normalization, API surface), `src/utils/feishu-auth.ts` (tenant domains, credential validation), and `src/bot/thread-id.ts` / `src/bot/chat-mode-cache.ts` (topic thread backfill, chat mode caching). See design spec §5 and §15.

## Global Constraints

- Use Rust edition 2024 with `rust-version = "1.85"`; CI tests stable Rust on Linux, macOS, and Windows. `unsafe_code` stays forbidden; keep clippy `pedantic` clean with `-D warnings`.
- Only Feishu (`https://open.feishu.cn`, `https://accounts.feishu.cn`) and Lark international (`https://open.larksuite.com`, `https://accounts.larksuite.com`) are supported tenants; never hardcode one domain without the tenant switch.
- Every long-lived queue, cache, pending map, mailbox, fragment buffer, and download buffer gets both a count and a byte limit, defined in `src/limits.rs` next to the existing constants (handoff §2 rule 5).
- The fragment cache enforces four simultaneous bounds: total buffered bytes, per-message bytes, fragment count, and a 5-second TTL. Duplicate fragments and out-of-range `seq`/`sum` are rejected and logged as protocol anomalies (design §5.1). Note the reference SDK's cache uses a 10 s TTL and no byte bounds — the design deliberately tightens this; do not copy the reference behavior.
- Reuse the supervisor's deterministic jittered backoff (bases 0.5/1/2/4/8/16/30 s, `base * [0.75, 1.25]`, capped at 30 s) for WebSocket reconnects instead of adding `rand`; extract the helper to a shared location if it is currently private.
- `Debug`, tracing output, and error messages must never contain the App Secret, tenant access token, message/card content, or raw frame payloads; log only IDs (`message_id`, `trace_id`, `chat_id`, `event_id`), sizes, status codes, and classified error kinds (handoff §4.2 rule 12 extended to Lark secrets).
- A frame receipt with `code: 200` is sent only after the inbound handler has completed successfully; handler failure sends `code: 500` (design §5.1 step 5). This milestone's handler is in-memory normalization plus a bounded event channel; the handler is an async closure returning `Result` so milestone 3 can swap in SQLite-persisted receipt without touching the transport.
- No SQLite, scope actor, outbox, render, or approval code in this milestone. No Claude, Web UI, or meeting functionality. Do not depend on `@larksuite/*` at runtime; the reference tree is read-only protocol documentation.
- Pin exact dependency versions in `Cargo.toml` and keep `--locked` green; the manifest is the lockfile's source of truth for review.
- Follow the user's efficient-development preference: write boundary tests and fixtures, then implement the complete slice; do not require a separate failing-test commit for every private helper.
- Commit after each numbered task whose checks pass; the main agent reviews and pushes.

---

## File Map

- `Cargo.toml`: add `prost`, `tokio-tungstenite`, `reqwest`, `url`, `base64`, `flate2`, `secrecy`.
- `src/lib.rs`: export the new `lark` module.
- `src/limits.rs`: all new bounded capacities, byte budgets, TTLs, and timeouts for this milestone.
- `src/lark/mod.rs`: stable exports for the rest of the bridge.
- `src/lark/config.rs`: tenant brand, domain resolution, connect defaults.
- `src/lark/error.rs`: `LarkError` taxonomy (`PermanentAuth`, `Retryable`, `ProtocolViolation`, `Exhausted`) shared by token/API/transport.
- `src/lark/credentials.rs`: App ID/Secret storage, atomic write, `0600` permissions, env overrides for tests.
- `src/lark/http.rs`: shared `reqwest` client, per-tenant base URL, bounded response bodies, classified errors.
- `src/lark/token.rs`: tenant access token cache with early refresh and failure classification.
- `src/lark/register.rs`: PersonalAgent device-flow registration and existing-app onboarding.
- `src/lark/frame.rs`: pbbp2 `Header`/`Frame` prost messages, header accessors, frame/header enums.
- `src/lark/fragments.rs`: bounded `message_id`/`seq`/`sum` reassembly with TTL.
- `src/lark/transport.rs`: endpoint bootstrap, WebSocket actor, ping/pong, receipt, reconnect state machine.
- `src/lark/api.rs`: tenant-token-aware OpenAPI client (messages, cards, images, files, bot info, chat info, message get).
- `src/lark/normalize.rs`: raw `im.message.receive_v1` → stable `InboundEvent`, scope keys, mention/topic/quote handling, bounded backfill.
- `src/cli.rs`: `lark auth check|register`, `lark probe` subcommands.
- `tests/lark_token.rs`: token cache and bot-info flows against a local stub HTTP server.
- `tests/lark_register.rs`: registration begin/poll flows against a local stub (no real scan).
- `tests/lark_frame.rs`: codec goldens, reassembly bounds and anomalies.
- `tests/lark_transport.rs`: in-process WebSocket server driving bootstrap/ping/receipt/reconnect/degraded flows.
- `tests/lark_api.rs`: OpenAPI request shapes, auth headers, and bounded downloads against a local stub.
- `tests/lark_normalize.rs`: normalization fixtures for p2p, group @, topic, quote, and backfill degradation.
- `tests/lark_smoke.rs`: ignored, opt-in real Lark end-to-end test (`LARK_E2E=1`).
- `tests/fixtures/lark/*.json`: scrubbed event/message/chat/token samples.

## Task 1: Tenant Configuration, Credentials, HTTP Core, and Tenant Token Cache

**Files:**

- Modify: `Cargo.toml`
- Create: `src/lark/mod.rs`
- Create: `src/lark/config.rs`
- Create: `src/lark/error.rs`
- Create: `src/lark/credentials.rs`
- Create: `src/lark/http.rs`
- Create: `src/lark/token.rs`
- Create: `tests/lark_token.rs`
- Modify: `src/lib.rs`
- Modify: `src/limits.rs`

**Interfaces:**

- Produces: `TenantBrand`, `LarkEndpoints`, `LarkError`, `LarkCredentials`, `CredentialStore`, `LarkHttp`, `TenantTokenProvider`.
- Consumes: nothing outside `limits` and the new dependencies.

- [x] **Step 1: Add the milestone dependency set**

Append to `[dependencies]` with exact pins (verify the lockfile resolves each):

```toml
prost = { version = "0.14.4", default-features = false, features = ["derive", "std"] }
tokio-tungstenite = { version = "0.30.0", features = ["rustls-tls-native-roots"] }
reqwest = { version = "0.13.4", default-features = false, features = ["rustls-tls", "json", "multipart"] }
url = "2.5.8"
base64 = "0.23.1"
flate2 = "1.1.9"
secrecy = { version = "0.10.3", features = ["serde"] }
```

No `native-tls`/OpenSSL anywhere; TLS is rustls only (design §15).

- [x] **Step 2: Implement tenant configuration and credential storage**

```rust
pub enum TenantBrand { Feishu, Lark }

pub struct LarkEndpoints {
    pub open_base: Url,     // https://open.feishu.cn | https://open.larksuite.com
    pub accounts_base: Url, // https://accounts.feishu.cn | https://accounts.larksuite.com
}

pub struct LarkCredentials {
    pub app_id: String,
    pub app_secret: SecretString,
    pub tenant: TenantBrand,
}

pub trait CredentialStore {
    fn load(&self) -> Result<Option<LarkCredentials>, LarkError>;
    fn save(&self, creds: &LarkCredentials) -> Result<(), LarkError>;
}
```

The file store writes atomically (temp file + rename) with `0600` permissions and redacts the secret in every `Debug`/`Display` path. Environment variables `LARK_APP_ID`, `LARK_APP_SECRET`, `LARK_TENANT` override the file so tests and the smoke gate never touch real state. `LarkError` classifies into `PermanentAuth` (bad credentials, forbidden), `Retryable` (network, timeout, 5xx, rate limit), `ProtocolViolation` (malformed frame/event), and `Exhausted` (bounds hit).

- [x] **Step 3: Implement the shared HTTP core and tenant token cache**

```rust
impl LarkHttp {
    pub fn new(endpoints: LarkEndpoints) -> Result<Self, LarkError>;
    pub async fn post_json<P: Serialize, R: DeserializeOwned>(&self, path: &str, body: &P) -> Result<R, LarkError>;
    pub async fn get_json<R: DeserializeOwned>(&self, path: &str, bearer: Option<&SecretString>) -> Result<R, LarkError>;
}

impl TenantTokenProvider {
    pub fn new(http: LarkHttp, creds: LarkCredentials) -> Self;
    pub async fn token(&self) -> Result<SecretString, LarkError>;
}
```

`TenantTokenProvider` calls `POST /open-apis/auth/v3/tenant_access_token/internal` with `{app_id, app_secret}`, caches `{token, expire_at}` behind a mutex with a single-flight refresh, and refreshes `TOKEN_REFRESH_SKEW` (3 minutes, matching the reference's early-expiry margin) before `expire_at`. A `code != 0` response classifies 403/auth failures as `PermanentAuth` and everything else as `Retryable`; the cached token is never logged. Response bodies are capped at `LARK_MAX_HTTP_BODY_BYTES` (e.g. 4 MiB) before JSON parsing. Add `LARK_HTTP_TIMEOUT = 15 s` (the reference bootstrap uses a 15 s request timeout) and token/cache constants to `src/limits.rs`.

- [x] **Step 4: Test token cache and error classification against a stub server**

Hand-roll a minimal HTTP/1.1 stub on `tokio::net::TcpListener` (no new dev-dependencies): canned status/JSON per request path, request capture for assertion. Cover first fetch, cache hit without a second request, refresh inside the skew window, single-flight under concurrent callers, `PermanentAuth` on invalid credentials, `Retryable` on 500 and on connect failure, body cap rejection, and `Debug` output containing no secret material.

- [x] **Step 5: Verify and publish the task**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --test lark_token --locked
cargo test --all-targets --locked
cargo +1.85.0 check --all-targets --all-features --locked
cargo build --release --locked
```

Commit `feat: add Lark tenant config, credentials, and token cache`.

## Task 2: PersonalAgent QR Registration and App Onboarding

**Files:**

- Create: `src/lark/register.rs`
- Create: `tests/lark_register.rs`
- Modify: `src/cli.rs`
- Modify: `src/lark/mod.rs`
- Modify: `src/limits.rs`

**Interfaces:**

- Consumes: `LarkHttp`, `LarkCredentials`, `CredentialStore`, `TenantTokenProvider`.
- Produces: `RegistrationFlow`, `RegistrationOutcome`, and CLI `lark auth check` / `lark auth register`.

- [x] **Step 1: Implement the registration device flow**

```rust
pub struct RegistrationFlow { /* endpoints, state */ }

pub struct QrChallenge {
    pub url: String,       // verification_uri_complete + tracking params
    pub expires_in: u64,   // seconds, server default 600
    pub interval: u64,     // poll seconds, server default 5
}

pub enum RegistrationOutcome {
    Credentials { creds: LarkCredentials, bot_hint: Option<String> },
    Pending,
    SlowDown { new_interval: u64 },
}

impl RegistrationFlow {
    pub async fn begin(&self) -> Result<QrChallenge, LarkError>;
    pub async fn poll_once(&mut self) -> Result<RegistrationOutcome, LarkError>;
}
```

Protocol (from the reference SDK's `registerApp` in the bundled node-sdk): `POST {accounts_base}/oauth/v1/app/registration`, form-encoded, `action=begin&archetype=PersonalAgent&auth_method=client_secret&request_user_info=open_id`; the response carries `device_code`, `verification_uri_complete`, `expires_in`, `interval`. The QR URL appends `from=sdk`, `source=lark-codex-bridge`, `tp=sdk`, and optionally `addons` (JSON → gzip → base64url, `+`→`-`, `/`→`_`, strip `=`). Polling posts `action=poll&device_code=…`; `authorization_pending` → `Pending`, `slow_down` → increase interval by 5 s, `access_denied`/`expired_token` → terminal error, success returns `client_id`/`client_secret` plus `user_info.tenant_brand`. When `tenant_brand == "lark"`, switch the accounts base URL to `accounts.larksuite.com` exactly once, mirroring the reference. Enforce a registration deadline (`LARK_REGISTER_TIMEOUT`, default 20 minutes — matches the reference QR session TTL).

- [x] **Step 2: Add CLI onboarding for both paths**

`lark auth register` runs the device flow: prints the QR URL (and, when a terminal renderer is trivially available later, the QR itself — plain URL output is sufficient in this milestone), polls with server-directed intervals, validates the returned credentials through `TenantTokenProvider` + `GET /open-apis/bot/v3/info`, then saves them through `CredentialStore`. `lark auth register --app-id <id> --app-secret <secret> --tenant <feishu|lark>` validates and saves existing credentials instead. `lark auth check` loads stored credentials, exchanges a tenant token, fetches bot info, and prints one sanitized JSON object (tenant, bot name, bot open_id) — never the secret or token.

- [x] **Step 3: Test the flow against the stub server**

Extend the Task 1 stub to serve `/oauth/v1/app/registration`. Cover begin response parsing, QR URL query assembly including gzip+base64url `addons`, pending → slow_down(interval grows) → success sequencing, `access_denied` terminal failure, one-time Lark domain switch on `tenant_brand: "lark"`, deadline expiry, and existing-app validation rejecting bad credentials with an actionable error.

- [x] **Step 4: Verify and publish the task**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --test lark_register --locked
cargo test --all-targets --locked
cargo +1.85.0 check --all-targets --all-features --locked
cargo build --release --locked
```

Commit `feat: add PersonalAgent QR registration and app onboarding`.

## Task 3: Protobuf Frame Codec and Bounded Fragment Reassembly

**Files:**

- Create: `src/lark/frame.rs`
- Create: `src/lark/fragments.rs`
- Create: `tests/lark_frame.rs`
- Create: `tests/fixtures/lark/frame_data_fragment.json` (documented header/payload shapes, scrubbed)
- Modify: `src/lark/mod.rs`
- Modify: `src/limits.rs`

**Interfaces:**

- Produces: `Frame`, `Header`, `FrameMethod`, `MessageType`, `FrameHeaders`, `Reassembler`, `Reassembly`, `ReassemblyError`.
- Consumes: `prost`, `bytes`, `LarkError`.

- [x] **Step 1: Implement the pbbp2 wire messages with pure prost derives**

No `build.rs` and no `protoc` requirement; derive directly in Rust source with explicit field numbers extracted from the reference SDK's generated encoder:

```rust
#[derive(Clone, PartialEq, prost::Message)]
pub struct Header {
    #[prost(string, tag = "1")] pub key: String,
    #[prost(string, tag = "2")] pub value: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct Frame {
    #[prost(uint64, tag = "1")] pub seq_id: u64,      // wire: SeqID
    #[prost(uint64, tag = "2")] pub log_id: u64,      // wire: LogID
    #[prost(int32, tag = "3")]  pub service: i32,
    #[prost(int32, tag = "4")]  pub method: i32,      // 0 = control, 1 = data
    #[prost(message, repeated, tag = "5")] pub headers: Vec<Header>,
    #[prost(string, optional, tag = "6")] pub payload_encoding: Option<String>,
    #[prost(string, optional, tag = "7")] pub payload_type: Option<String>,
    #[prost(bytes, optional, tag = "8")]  pub payload: Option<Bytes>,
    #[prost(string, optional, tag = "9")] pub log_id_new: Option<String>, // wire: LogIDNew
}
```

Custom `Debug` for `Frame` redacts `payload` (logs length only). Provide `FrameHeaders` helpers for `type`, `message_id`, `sum`, `seq`, `trace_id`, `biz_rt`, `handshake-status`, `handshake-msg`, `handshake-autherrcode`, and the `MessageType` enum (`event`, `card`, `ping`, `pong`).

- [x] **Step 2: Implement bounded fragment reassembly**

```rust
pub struct Reassembly {
    pub message_id: String,
    pub trace_id: Option<String>,
    pub payload: Bytes, // complete UTF-8 JSON body
}

pub enum ReassemblyError { Duplicate, OutOfRange, OverBytes, TooManyFragments, Expired }

impl Reassembler {
    pub fn ingest(&mut self, headers: &FrameHeaders, payload: Bytes, now: Instant)
        -> Result<Option<Reassembly>, ReassemblyError>;
}
```

Semantics from the reference `DataCache` plus the design's hardening: single-fragment frames (`sum == 1`) pass through directly; multi-fragment frames allocate a per-message slot vector keyed by `message_id`, indexed by `seq`, completing when all `sum` slots are filled. Enforce simultaneously: `LARK_FRAGMENT_TOTAL_BYTES` (e.g. 8 MiB), `LARK_FRAGMENT_MESSAGE_BYTES` (e.g. 1 MiB), `LARK_FRAGMENT_MAX_COUNT` per message and in flight, and a 5-second TTL per message swept on ingest (not by a background timer). Duplicate `seq`, `seq >= sum`, `sum == 0`, and conflicting `sum` for an in-flight `message_id` are rejected and recorded as protocol anomalies with IDs only.

- [x] **Step 3: Add codec goldens and reassembly boundary tests**

Goldens: hand-computed byte vectors for a ping frame (control, `service=<id>`, `type=ping`, SeqID/LogID 0) and a single-fragment event frame, plus round-trips preserving unknown/optional fields. Reassembly: out-of-order arrival, duplicate fragment, `seq` out of range, sum mismatch, per-message byte overflow, total byte overflow, fragment count overflow, TTL expiry mid-sequence, and `Debug` redaction of payload bytes.

- [x] **Step 4: Verify and publish the task**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --test lark_frame --locked
cargo test --all-targets --locked
cargo +1.85.0 check --all-targets --all-features --locked
cargo build --release --locked
```

Commit `feat: add Lark protobuf frame codec and bounded reassembly`.

## Task 4: WebSocket Endpoint Bootstrap, Transport Actor, and Reconnect

**Files:**

- Create: `src/lark/transport.rs`
- Create: `tests/lark_transport.rs`
- Modify: `src/lark/mod.rs`
- Modify: `src/limits.rs`

**Interfaces:**

- Consumes: `LarkHttp`, `LarkCredentials`, `Frame`, `Reassembler`, supervisor backoff helper.
- Produces: `LarkTransport`, `TransportHandle`, `TransportEvent`, `TransportState`, `InboundFrameHandler`.

- [ ] **Step 1: Implement endpoint bootstrap with classified failures**

```rust
pub struct WsEndpoint {
    pub url: Url,        // contains device_id and service_id query params
    pub service_id: i32,
    pub ping_interval: Duration,
    pub reconnect_count: i64,     // < 0 means unlimited
    pub reconnect_interval: Duration,
    pub reconnect_nonce: Duration,
}

impl LarkTransport {
    pub async fn pull_endpoint(http: &LarkHttp, creds: &LarkCredentials) -> Result<WsEndpoint, LarkError>;
}
```

`POST {open_base}/callback/ws/endpoint` with JSON body `{AppID, AppSecret}` and a `locale: zh` header, 15 s timeout. Response `{code, msg, data: {URL, ClientConfig: {PingInterval, ReconnectCount, ReconnectInterval, ReconnectNonce}}}`; ClientConfig values are seconds. `device_id`/`service_id` are parsed from the returned `URL` query string (the reference extracts them exactly this way). Code classification: `0` ok; `1` (system busy), `1000040343` (internal error), transport errors, timeouts, and HTTP 5xx → `Retryable`; `403` (forbidden), `514` (auth failed), and `1000040350` (exceed connection limit) → `PermanentAuth`/`Exhausted` (non-retryable, surfaces `Degraded`). Note: the reference treats every code except `1000040343` as non-retryable; the design's classification above is a deliberate deviation toward retrying transient server errors — record it in the module docs.

- [ ] **Step 2: Implement the WebSocket actor with ping/pong and liveness**

```rust
pub enum TransportState {
    Connecting { attempt: u32 },
    Connected,
    Backoff { attempt: u32, delay: Duration },
    Degraded { reason: String },
    Stopped,
}

pub enum TransportEvent {
    State(TransportState),
    Message { headers: FrameHeaders, payload: Bytes }, // complete, reassembled
    Anomaly { kind: &'static str, message_id: Option<String> },
}
```

One actor task owns the `tokio-tungstenite` binary WebSocket. On open it starts a ping loop sending a control frame (`service = service_id`, `method = control`, headers `[{type: ping}]`, SeqID/LogID 0) every server-supplied `PingInterval`; any inbound frame cancels the liveness watchdog, and a ping unanswered within `LARK_PONG_TIMEOUT` terminates the socket to trigger reconnect. Inbound control frames with `type: pong` parse the JSON payload `{PingInterval, ReconnectCount, ReconnectInterval, ReconnectNonce}` and update the live config, exactly like the reference. Data frames route through the `Reassembler`; completed `event`/`card` payloads go to the handler; anomalies surface as `TransportEvent::Anomaly` and are never fatal to the connection.

- [ ] **Step 3: Implement receipt-after-handling and reconnect policy**

The handler is `Arc<dyn Fn(FrameHeaders, Bytes) -> Future<Output = Result<Option<Value>, LarkError>> + Send + Sync>`. Only after the handler returns `Ok` does the actor encode and send the receipt frame: the original frame's headers plus `biz_rt`, with payload JSON `{code: 200}` and optional `data` = base64(JSON of the handler's return value); handler failure sends `{code: 500}`. A receipt send failure on a closing socket is logged, not retried. Reconnect: on socket close/error or a retryable bootstrap failure, wait using the supervisor's jittered exponential backoff (0.5–30 s) seeded by attempt, honoring the server-supplied `ReconnectNonce` as the initial delay and `ReconnectCount >= 0` as an attempt cap; `PermanentAuth`/`Exhausted` from bootstrap or a `handshake-autherrcode` header enters `Degraded` without further retries. Shutdown closes the socket and joins the actor with a bounded grace.

- [ ] **Step 4: Test against an in-process WebSocket server**

Use `tokio_tungstenite::accept_async` on a `tokio::net::TcpListener` pair (no new dev-dependencies) plus the HTTP stub for bootstrap. Cover bootstrap parsing including query extraction, classified permanent vs retryable bootstrap codes, ping frame bytes on the wire, pong-driven config update, liveness timeout triggering reconnect, control/data dispatch, single- and multi-fragment events delivered in order, receipt `{code:200}` only after handler success and `{code:500}` on failure, receipt `data` base64 round-trip, anomaly surfacing without disconnect, backoff delay sequence, attempt cap, `Degraded` on 403/514 with no retry, and clean shutdown without orphan tasks.

- [ ] **Step 5: Verify and publish the task**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --test lark_transport --locked
cargo test --all-targets --locked
cargo +1.85.0 check --all-targets --all-features --locked
cargo build --release --locked
```

Commit `feat: add Lark WebSocket transport with reconnect and receipts`.

## Task 5: OpenAPI Client for Messages, Cards, Images, and Files

**Files:**

- Create: `src/lark/api.rs`
- Create: `tests/lark_api.rs`
- Create: `tests/fixtures/lark/message_get_response.json`
- Create: `tests/fixtures/lark/chat_get_response.json`
- Modify: `src/lark/mod.rs`
- Modify: `src/limits.rs`

**Interfaces:**

- Consumes: `LarkHttp`, `TenantTokenProvider`.
- Produces: `LarkApi`, `MessageRef`, `ChatMode`, `ResourceData`, and typed request/response DTOs.

- [ ] **Step 1: Implement message and card sending**

```rust
impl LarkApi {
    pub async fn send_text(&self, chat_id: &str, text: &str) -> Result<MessageRef, LarkError>;
    pub async fn reply_text(&self, message_id: &str, text: &str) -> Result<MessageRef, LarkError>;
    pub async fn send_card(&self, chat_id: &str, card: Value) -> Result<MessageRef, LarkError>;
    pub async fn reply_card(&self, message_id: &str, card: Value) -> Result<MessageRef, LarkError>;
    pub async fn update_card(&self, message_id: &str, card: Value) -> Result<(), LarkError>;
    pub async fn reply_text_in_thread(&self, message_id: &str, text: &str) -> Result<MessageRef, LarkError>;
}
```

Wire paths (from the bundled node-sdk codegen): `POST /open-apis/im/v1/messages?receive_id_type=chat_id` for sends, `POST /open-apis/im/v1/messages/{message_id}/reply` for replies (`reply_in_thread: true` for topic replies), `PATCH /open-apis/im/v1/messages/{message_id}` for card updates. Every call attaches `Authorization: Bearer <tenant token>` from `TenantTokenProvider`, classifies `code != 0` into `PermanentAuth` (99991663-class token errors trigger exactly one forced token refresh retry) vs `Retryable`, and caps outbound text/card bodies at `LARK_MAX_SEND_BODY_BYTES`. Message content is never logged.

- [ ] **Step 2: Implement message get, chat info, and bounded resource download**

```rust
pub enum ChatMode { P2p, Group, Topic }

impl LarkApi {
    pub async fn get_message(&self, message_id: &str) -> Result<RawMessage, LarkError>; // keeps thread_id even when events omit it
    pub async fn get_chat_mode(&self, chat_id: &str) -> Result<ChatMode, LarkError>;
    pub async fn download_message_resource(&self, message_id: &str, file_key: &str, kind: ResourceKind)
        -> Result<ResourceData, LarkError>;
    pub async fn upload_image(&self, bytes: Bytes) -> Result<String /* image_key */, LarkError>;
    pub async fn upload_file(&self, name: &str, bytes: Bytes) -> Result<String /* file_key */, LarkError>;
    pub async fn bot_info(&self) -> Result<BotInfo, LarkError>;
}
```

Paths: `GET /open-apis/im/v1/messages/{message_id}`, `GET /open-apis/im/v1/chats/{chat_id}` (`chat_mode` field maps `p2p`/`group`/`topic`), `GET /open-apis/im/v1/messages/{message_id}/resources/{file_key}?type=image|file`, `POST /open-apis/im/v1/images` and `POST /open-apis/im/v1/files` (multipart), `GET /open-apis/bot/v3/info`. Downloads stream through a `LARK_MAX_RESOURCE_BYTES` (e.g. 20 MiB) hard cap that aborts mid-body rather than buffering unbounded data; uploads likewise refuse oversize inputs before sending.

- [ ] **Step 3: Test request shapes, auth, and bounds against the stub**

Assert exact method/path/query/header/body for every call, including `receive_id_type`, `reply_in_thread`, multipart field names, and the bearer header. Cover token-error → single forced refresh → retry once, `PermanentAuth` propagation, body cap abort on an oversize download stream, oversize upload rejection before I/O, and `code != 0` classification fixtures.

- [ ] **Step 4: Verify and publish the task**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --test lark_api --locked
cargo test --all-targets --locked
cargo +1.85.0 check --all-targets --all-features --locked
cargo build --release --locked
```

Commit `feat: add Lark OpenAPI client for messages, cards, and media`.

## Task 6: Stable InboundEvent Normalization

**Files:**

- Create: `src/lark/normalize.rs`
- Create: `tests/lark_normalize.rs`
- Create: `tests/fixtures/lark/event_p2p_text.json`
- Create: `tests/fixtures/lark/event_group_mention.json`
- Create: `tests/fixtures/lark/event_topic_reply.json`
- Create: `tests/fixtures/lark/event_quote.json`
- Modify: `src/lark/mod.rs`
- Modify: `src/limits.rs`

**Interfaces:**

- Consumes: `LarkApi` (backfill), bot open_id.
- Produces: `InboundEvent`, `ScopeKey`, `Normalizer`, `NormalizeOutcome`.

- [ ] **Step 1: Define the stable inbound model and scope rules**

```rust
pub struct InboundEvent {
    pub event_id: String,
    pub message_id: String,
    pub chat_id: String,
    pub sender_id: String,            // open_id
    pub chat_type: ChatMode,
    pub thread_id: Option<String>,
    pub root_id: Option<String>,
    pub reply_to_message_id: Option<String>, // event parent_id / quoted message
    pub text: String,                  // mentions stripped
    pub mentions_bot: bool,
    pub mention_all: bool,
    pub resources: Vec<ResourceDesc>,  // image/file keys + types, not bytes
    pub message_type: String,          // raw type, open string
    pub create_time_ms: i64,
    pub scope: ScopeKey,
}

pub enum ScopeKey { Chat(String), Thread(String, String) } // renders im:<chat_id> | im:<chat_id>:thread:<thread_id>
```

Scope rules per design §5.2: p2p and plain group messages → `im:<chat_id>`; topic messages with a `thread_id` → `im:<chat_id>:thread:<thread_id>`; document comments (`doc:<file_token>`) are out of scope for this milestone.

- [ ] **Step 2: Implement normalization with mention, topic, and quote handling**

Parse `im.message.receive_v1` payloads (`header.event_id`, `event.sender.sender_id.open_id`, `event.message.{message_id, chat_id, chat_type, message_type, content, mentions, root_id, parent_id, thread_id, create_time}`). Detect bot mentions via the `mentions` array against the bot open_id and `<at user_id="all">` for mention-all; strip mention tags from the text. Maintain a bounded `chat_id → ChatMode` cache (count + TTL) that falls back to `Group` on lookup failure and is invalidated when a message carries a `thread_id` that contradicts a cached non-topic entry, matching the reference `ChatModeCache`. When a topic-group event lacks `thread_id`, backfill once via `LarkApi::get_message` (whose raw item keeps `thread_id` even when the event dropped it, per the reference `thread-id.ts`); on backfill failure fall back to chat-level scope and record the degradation reason. For quoted messages, resolve `parent_id`/quote content only within a bounded single fetch — no recursive history walks in this milestone; full history backfill belongs to the scope-runtime milestone.

- [ ] **Step 3: Add normalization fixture tests**

Fixtures cover p2p text, group message with bot @ (text stripped, `mentions_bot`), group without @, topic reply with `thread_id`, topic event missing `thread_id` recovered by stubbed backfill, backfill failure degradation, quoted message linkage, image/file message resource descriptors, unknown message types preserved via the open `message_type` string, and scope-key rendering for both forms.

- [ ] **Step 4: Verify and publish the task**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --test lark_normalize --locked
cargo test --all-targets --locked
cargo +1.85.0 check --all-targets --all-features --locked
cargo build --release --locked
```

Commit `feat: normalize Lark events into stable inbound model`.

## Task 7: Wiring, `lark probe`, and Opt-In Real Lark Smoke

**Files:**

- Create: `tests/lark_smoke.rs`
- Modify: `src/cli.rs`
- Modify: `src/lark/mod.rs`
- Modify: `README.md`

**Interfaces:**

- Consumes: every `lark` module above.
- Produces: `lark probe` command and a gated real end-to-end smoke test.

- [ ] **Step 1: Wire the transport to the normalizer behind a bounded event channel**

`LarkBridge::start(creds) -> (TransportHandle, mpsc::Receiver<InboundEvent>)`: the transport handler normalizes each completed event payload and pushes `InboundEvent` into a channel bounded by `LARK_INBOUND_EVENT_CAPACITY` count and `LARK_INBOUND_EVENT_BYTE_BUDGET` bytes (permits held until dequeue, matching the existing transport/RPC permit pattern). A full channel fails the handler so the receipt honestly reports `{code: 500}` instead of silently dropping. Card-action payloads are acknowledged with `{code: 200, data}` and logged as unsupported for this milestone rather than routed.

- [ ] **Step 2: Implement `lark probe`**

Loads credentials, exchanges a tenant token, fetches bot info, pulls a WS endpoint, opens the socket, waits for the first ping/pong round trip (bounded by `PROBE_TIMEOUT`), then closes. Prints one sanitized JSON object: tenant, bot name, bot open_id, endpoint reachability, negotiated `PingInterval`, and elapsed milliseconds. Never prints secrets, tokens, or the full endpoint URL (log only host). Exits non-zero with an actionable diagnostic for missing credentials, `PermanentAuth`, and timeout.

- [ ] **Step 3: Add the opt-in real Lark smoke test**

`tests/lark_smoke.rs` is `#[ignore = "requires real Feishu/Lark app credentials"]` and additionally requires `LARK_E2E=1`; without it the test prints the skip reason and exits successfully — a skipped run is explicitly not evidence. When enabled it requires `LARK_E2E_APP_ID`, `LARK_E2E_APP_SECRET`, `LARK_E2E_TENANT` (`feishu|lark`), and `LARK_E2E_CHAT_ID` (a chat where the app bot is a member), then: exchanges a tenant token, sends `bridge-smoke <unix-ts>` via `LarkApi::send_text`, starts the transport, waits up to 180 s for its own message event to round-trip through `InboundEvent`, asserts scope/chat/text metadata, replies `pong` to that message via `reply_text`, and shuts down with no orphan tasks. A skip, an assertion failure, or missing credentials can never be reported as a pass in milestone evidence.

- [ ] **Step 4: Verify and publish the milestone**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-targets --locked
cargo +1.85.0 check --all-targets --all-features --locked
cargo build --release --locked
git diff --check
LARK_E2E=1 LARK_E2E_APP_ID=… LARK_E2E_APP_SECRET=… LARK_E2E_TENANT=feishu LARK_E2E_CHAT_ID=… \
  cargo test --test lark_smoke --locked -- --ignored --nocapture
cargo run --locked -- lark probe
```

Commit `feat: wire Lark transport, probe, and real smoke`.

## Milestone Completion Evidence

The milestone is complete only when:

1. All seven task commits exist on public `main` and CI is green on Linux quality, Rust 1.85, macOS, and Windows.
2. `cargo run --locked -- lark probe` reports a reachable tenant, bot identity, endpoint, and ping/pong negotiation without leaking secrets, tokens, or message content.
3. The gated real smoke test (`LARK_E2E=1` plus app credentials and a test chat) proves send → WebSocket receive → normalized `InboundEvent` → reply in one run, and its recorded output is attached to the handoff. Running the suite without credentials and observing the skip message does not count.
4. Unit and integration tests prove: token cache early refresh and failure classification; registration begin/poll/domain-switch; frame codec goldens; fragment bounds (total bytes, per-message bytes, count, 5 s TTL) and duplicate/out-of-range rejection; ping/pong config updates and liveness reconnect; receipt `{code:200}` only after handler success; backoff sequence, attempt cap, and `Degraded` on permanent auth errors; OpenAPI request shapes and bounded downloads; normalization for p2p, group @, topic, quote, and backfill degradation.
5. After the smoke run, `ps` inspection shows no orphan bridge or WebSocket tasks, and a redaction sweep (`git grep`-level review plus test assertions) confirms no App Secret, tenant token, or message content appears in logs, `Debug` output, or errors.

## Subsequent Milestone Plans

After this milestone, create and execute these separate plans against the stable interfaces above:

1. `2026-08-12-reliable-bridge-runtime.md`: SQLite migrations, durable inbound dedup and receipt semantics (plugging into this milestone's handler seam), scope actors, concurrency, workspaces, reliable outbox, reply projector, attachments, and first-stage commands.
2. `2026-08-12-core-parity-platform.md`: approvals, resume/history, access administration, profile/lark-cli isolation, service managers, document comments, team mode, fault injection, benchmark, and release documentation.
