# RFC: connect to a shared external Codex app-server

- Status: research complete; implementation not started
- Date: 2026-08-23
- Issue: [#8](https://github.com/M1nt-Ch0c0/lark-codex-bridge/issues/8)
- Local subject: `codex-cli 0.149.0` on Darwin arm64
- Decision: conditional go for bounded follow-up work; no production external-write support yet

## Decision summary

A committed, bounded real-server probe (E17) shows only that two read-only raw
clients can initialize against one explicitly started Codex app-server WebSocket
listener, each perform a one-row `thread/list`, disconnect, and leave the server
available for an exact health check and a fresh client. E1-E16 separately record
sanitized, uncommitted investigation observations about authentication,
same-server resume and events, steering, interrupt, queueing, reconnect, restart,
and writer conflicts. Those observations are not executable reproductions in
this repository and are not acceptance evidence for those capabilities.

This narrow result establishes a research direction, not production support or
shared-write compatibility. Current
[OpenAI Docs for Codex App Server](https://developers.openai.com/codex/app-server)
explicitly call the WebSocket transport experimental and unsupported for
production. The protocol has no atomic "start only if idle" precondition, no
server capability manifest, no notification replay cursor, and no documented
multi-client approval election. The uncommitted Local-0.149 observations also
recorded that
`clientUserMessageId` is not an idempotency key and that a concurrent
`turn/start` can ambiguously return the already-active turn id. Arbitrary
simultaneous writers therefore remain unsupported.

The implementation direction is:

1. Preserve today's explicitly selected `spawned_stdio` behavior.
2. Add a separately selected `external_endpoint` backend only after the bounded
   follow-ups at the end of this RFC pass.
3. Initially permit only an authenticated, explicitly configured WebSocket
   endpoint and an exact promoted Codex wire version.
4. Keep all mutations fenced while connection state or prior delivery is
   uncertain. Never replay an uncertain `thread/start`, `turn/start`,
   `turn/steer`, or queue mutation.
5. Treat a shared server as one server process with multiple clients. Two
   app-server processes pointed at one profile are not a sharing mechanism.
6. Keep Desktop endpoint reuse `unknown / unsupported`. Do not discover private
   sockets or infer that a Desktop process permits third-party clients.

If the follow-up approval-routing and write-coordination gates cannot be proven,
the safe fallback remains Issue #4's explicit sequential handoff after the prior
owner exits. There is no automatic takeover.

## Acceptance traceability

| Issue #8 criterion | RFC evidence |
| --- | --- |
| Spawned versus external components, lifecycle, and state | Component boundary and both lifecycle state machines |
| Endpoint, auth, and initialization matrix | Transport, authentication, and capability matrices |
| Two-client reproducibility | Committed E17 reproduces initialize, one-row `thread/list`, disconnect, exact health, and a fresh client only; E3-E5 are uncommitted observations |
| Discovery, resume, subscription, and active state | Design plus uncommitted E4-E5/E8/E16 observations; no committed resume, active-state, or event reproduction |
| Start, steer, interrupt, and queue conflicts | Design plus uncommitted E6-E13 observations; no committed mutation or queue reproduction |
| One approval handler, timeout, disconnect, and reassignment | Design only; no local or committed approval-routing reproduction |
| Reconnect, epoch, compensation, deduplication, and uncertainty | Design plus uncommitted E8-E9 observations; E17 opens fresh connections but does not reproduce reconnect, resume, replay, or recovery |
| Version/auth/capability fail-closed behavior | Proposed configuration and promotion gates; committed E17 verifies exact version/profile but does not exercise authentication |
| External process survival | Committed E17 verifies black-box survival after read-only probe clients; structural and bridge-integration proofs remain required |
| Desktop support status | Client support matrix: unknown / unsupported |
| Bounded implementation split | Five core issues plus one Unix-transport RFC, all required to be created or linked before #8 closes |

## Evidence and authority

This RFC deliberately separates four kinds of statement:

- **Official-current** means the current OpenAI Docs page linked above. That
  page is rolling documentation and itself labels WebSocket experimental.
- **Local-0.149 observation (E1-E16)** means a sanitized, bounded investigation
  note from the exact local `codex-cli 0.149.0` binary. The raw harnesses and
  payloads are not committed, so these rows are not repository-reproducible
  acceptance evidence and must not be generalized to another version.
- **Committed Local-0.149 reproduction (E17)** means the sole checked-in
  real-server procedure in this RFC. It covers only exact identity, read-only
  initialize/list, observed disconnect, exact health, fresh-client reuse, and
  the negative code-1006 close result described below.
- **Design** means a proposed bridge rule or implementation gate.

An earlier official-only search during this investigation did not find an
app-server page. A fresh check on 2026-08-23 found the current OpenAI Docs page.
The earlier search result is not evidence that documentation does not exist.
Conversely, the current rolling page does not turn the exact 0.149.0 observations
below into stable protocol guarantees.

The repository's generated 0.149.0 schema and contract are reproducible local
artifacts, not official documentation:

- [`protocol/codex/schemas/0.149.0/manifest.json`](../protocol/codex/schemas/0.149.0/manifest.json)
- [`protocol/codex/contracts/0.149.0.json`](../protocol/codex/contracts/0.149.0.json)
- [`protocol/codex/reports/0.146.0-to-0.149.0.md`](../protocol/codex/reports/0.146.0-to-0.149.0.md)

At this commit, 0.146.0 is the only promoted production adapter. Version 0.149.0
is a candidate blocked by reviewed breaking changes. Nothing in this RFC bypasses
that compatibility boundary.

## Scope and non-goals

This RFC covers an external app-server connection, transport ownership, recovery,
thread coordination, and approval responsibility. It does not implement the
backend.

It does not:

- run two independent app-server processes against one active thread;
- add cross-process leader election or automatic writer takeover;
- terminate, restart, signal, or reconfigure an external app-server;
- expose an app-server directly to the public internet;
- auto-discover Desktop, IDE, daemon, or user-profile sockets;
- make an unauthenticated non-loopback WebSocket a supported configuration;
- change Lark authorization, workspace policy, media handling, or message policy;
- replace Issue #4's explicit sequential handoff;
- treat a notification stream as a durable event log.

## What exists today

The current `AppServerSupervisor` is intentionally a process owner:

1. It probes the configured binary with `codex --version`.
2. It spawns `codex app-server --listen stdio://`.
3. It owns the child, stdin, stdout, stderr, PID, wait, and termination paths.
4. It binds one `ConnectionEpoch` to the JSONL transport, RPC broker, and typed
   client.
5. On child exit it tears down the epoch and restarts with bounded backoff.
6. On bridge shutdown it closes stdin, waits a grace period, kills if necessary,
   and reaps the child.

That ownership is correct for spawned stdio. It is forbidden in external mode.
The existing RPC layer already has useful invariants to preserve: exact-epoch
request correlation, bounded count and byte queues, high-priority reverse
responses, redacted protocol failures, and no retry of uncertain non-idempotent
turn creation.

## Proposed component boundary

Mode selection must be a tagged, exhaustive configuration decision. There is no
`auto` mode and no fallback from an unavailable external endpoint to spawning a
second server.

| Component | Spawned stdio | External endpoint |
| --- | --- | --- |
| `BackendSelector` | Validates spawned fields | Validates endpoint, TLS, auth, exact version, and capability profile |
| `SpawnedProcessOwner` | Owns `Command`, child, PID, wait, stderr, and terminate | Type is absent |
| `ExternalConnector` | Type is absent | Owns DNS/socket/TLS/WebSocket connection only |
| `EpochSession` | Owns transport, RPC, typed client, and client-generated epoch | Same |
| `VersionGate` | Probes the owned binary before spawn | Verifies exact reviewed version asserted by initialize and capability canaries |
| `RecoveryCoordinator` | Restarts a failed owned child | Reconnects only; never starts or stops a server |
| `ThreadCoordinator` | Serializes bridge turns | Also fences against externally visible active/queued/uncertain state |
| `ApprovalCoordinator` | One local control stream | One explicitly configured handler for bridge-managed turns |
| Shutdown | Close RPC/stdio, terminate and reap child | Close RPC/WebSocket only |

The shared portion begins at a transport-neutral `EpochSession`. The two
lifecycle implementations must not share a trait that exposes `pid`, `wait`,
`kill`, `terminate`, or `ProcessFactory` to the external branch.

An implementation should make the non-ownership property structural:

```text
SpawnedBackend
  -> SpawnedProcessOwner
  -> StreamTransport(stdout, stdin, redacted stderr)
  -> EpochSession

ExternalBackend
  -> ExternalConnector(endpoint, bearer, TLS policy)
  -> WebSocketTransport(socket)
  -> EpochSession
```

`ExternalConnector` returns a socket-bearing connection, not a process-like
object. Dropping it sends or attempts a bounded WebSocket close and drops the
socket. There is no callback into an operator-owned launcher.

## Proposed configuration

The following is illustrative and not accepted by the current parser:

```toml
[codex]
model = "gpt-5.6-terra"
sandbox = "workspace-write"
approval_policy = "on-request"

[codex.backend]
mode = "spawned_stdio"
binary = "codex"
codex_home = "/absolute/private/profile"
```

```toml
[codex]
model = "gpt-5.6-terra"
sandbox = "workspace-write"
approval_policy = "on-request"

[codex.backend]
mode = "external_endpoint"
endpoint = "wss://codex.example.invalid/app-server"
expected_codex_version = "0.149.0"
capability_profile = "observe_shared"
auth_token_file = "/absolute/private/app-server.bearer"
approval_handler = "external"
```

Rules:

- `spawned_stdio` rejects endpoint and bearer fields.
- `external_endpoint` rejects binary and `codex_home` fields.
- Unknown fields are errors.
- `expected_codex_version` is one exact semantic version, never a range.
- The expected version must have a promoted `WireAdapter`; a candidate schema is
  insufficient.
- Endpoint userinfo, query, fragment, embedded credentials, and overlong values
  are rejected.
- Plain `ws://` is limited to a literal loopback address. A hostname that happens
  to resolve to loopback is not enough. SSH forwarding should terminate on a
  literal loopback listener.
- Any non-loopback TCP endpoint requires `wss://`, normal certificate and hostname
  verification, and explicit bearer authentication. "Skip verification" is not
  a supported switch.
- External mode requires authentication even on loopback. An explicit
  read-only research probe may waive it only under an isolated-profile gate; the
  production connector may not.
- `auth_token_file` is an absolute, privately permissioned regular file. Inline
  tokens and URI credentials are forbidden. An environment-variable-name source
  may be added for managed deployments, but the value itself never enters TOML.
- A signed-bearer client receives an already issued bearer token. It must not be
  given the server's signing secret.
- Token files are read under a small byte limit into a secret wrapper. Rotation
  is an explicit drain-and-reconnect operation; logs expose only that a source is
  configured.
- Changing backend mode requires restart. An external outage never silently
  selects `spawned_stdio`.

The endpoint must receive a private, stable configuration identity derived from
canonical non-secret configuration. Runtime logs should use a short opaque
endpoint label, not the URI, host, token path, token, thread ids, prompt text,
approval details, or initialize `codexHome`.

## Endpoint and authentication support

### Transport matrix

| Form | Official-current | Local evidence boundary | Bridge decision |
| --- | --- | --- | --- |
| `stdio://` listener | JSONL, default | Existing bridge implementation and tests, independent of E1-E17 | Supported only as an owned spawned child |
| `ws://127.0.0.1:PORT` listener | Experimental; one RPC per text frame | Committed E17 covers unauthenticated read-only loopback only; other multi-client behavior is uncommitted | Eligible for gated local research; production connector still requires auth |
| Non-loopback `ws://` listener | Docs say auth should be configured and note a rollout period that may allow no auth | Sanitized uncommitted E1 observation only | Bridge rejects remote plaintext regardless of server behavior |
| Direct `wss://` listener | Not listed as a listener form | Sanitized uncommitted 0.149 rejection observation only | Unsupported; put a `ws://` listener behind a separately owned TLS terminator |
| `wss://` remote client | CLI remote form is documented | Help-surface observation only; no committed real connection | Eligible only after normal TLS verification and bearer-auth reproduction |
| `unix://` listener/client | Documented as WebSocket over HTTP Upgrade | Sanitized uncommitted, negative, incomplete E14 observation only | Unsupported and rejected until a dedicated adapter is committed and reproduced |
| `off` | Disables local transport | Help-surface observation only | Not a connectable endpoint |

The current OpenAI Docs statement about unauthenticated non-loopback rollout and
the 0.149 refusal are intentionally both recorded. Security policy follows the
stricter behavior and does not depend on either one remaining true.

### Authentication matrix

| Server configuration | Local evidence boundary | Client requirement | Decision |
| --- | --- | --- | --- |
| No auth, loopback WS | Committed E17 only | None at handshake | Research probe only; reject in production external mode |
| No auth, non-loopback WS | Sanitized uncommitted E1 observation only | N/A | Unsupported |
| Capability token file | Help-surface observation only | Raw token as HTTP `Authorization: Bearer` | Candidate only after issue-local reproduction; token file preferred |
| Capability token SHA-256 | Sanitized uncommitted E2 observation only | Client retains raw high-entropy token; server stores verifier | Candidate only after issue-local reproduction |
| Signed bearer | Help-surface observation only | Client presents a valid issued token | Candidate only after expiry/rotation reproduction |
| Credential in endpoint/CLI argument | Not needed | Leaks through config/process inspection | Forbidden |

Official-current places WebSocket authentication at connection setup, and the
sanitized uncommitted E2 observation recorded HTTP 401 before JSON-RPC
`initialize`. The bridge design therefore treats HTTP 401/403 as a permanent
configuration state until an operator reloads credentials. It is not an
exponential-retry loop; E2 itself is not a committed auth reproduction.

### Initialization and capability matrix

Every new connection must:

1. Complete socket/TLS/WebSocket authentication.
2. Send exactly one `initialize` with static bridge name, display title, bridge
   version, and an explicit set of client-declared capabilities.
3. Validate the response using the exact promoted generated schema.
4. Extract exactly one reviewed Codex version token from `userAgent` and require
   equality with `expected_codex_version`.
5. Drop, rather than log, `codexHome` and other environment-bearing values.
6. Send `initialized`.
7. Run the available non-mutating capability canaries before entering
   reconciliation. Methods that require a known thread are gated on their first
   use for each thread.

`initialize.params.capabilities` declares client behavior; it is not a server
capability manifest. Initialize success therefore cannot prove that every needed
method or notification exists.

| Profile | Required surface | Startup evidence | Failure behavior |
| --- | --- | --- | --- |
| `observe_shared` | initialize, `thread/list`, `thread/read` | Exact wire decode, a bounded list canary, and a read canary before adopting a known thread | Fail closed |
| `resume_shared` | Observe profile plus `thread/resume`, `thread/unsubscribe`, thread status and turn/item terminal notifications | Versioned conformance test and successful first resume for each adopted thread | Fail closed |
| `mutate_shared` | Resume profile plus `turn/start`, exact-id `turn/steer`, `turn/interrupt`, all configured approval request/response shapes | Exact promoted contract, fake-server suite, and gated real smoke | Fail closed; not currently eligible |
| `queue_shared` | Mutate profile plus queue add/list/start and `thread/queue/changed` | Exact-version local schema and canary; queue is not relied on as an official-stable API | Disable queue or fail startup when explicitly required |

An unknown method, unknown required notification, schema drift, missing exact
version, or mismatched version moves the backend to `Degraded`. No best-effort
generic JSON path is allowed.

## Lifecycle state machines

### Spawned stdio

```text
Disabled
  -> ValidateConfig
  -> ProbeBinaryVersion
  -> SpawnChild
  -> Initialize
  -> Ready(epoch)
  -> [child exit] Backoff -> SpawnChild
  -> [shutdown] CloseEpoch -> TerminateChild -> Reap -> Stopped

Any permanent config/version/initialize rejection -> Degraded
```

This is the current ownership model and remains the default.

### External endpoint

```text
Disabled
  -> ValidateConfig
  -> Connect
  -> Authenticate/TLS
  -> InitializeAndVersionGate
  -> CapabilityCanaries
  -> ReconcileAndResubscribe(epoch)
  -> Ready(epoch)

Ready -- socket loss --> FenceUncertain(epoch) -> Backoff -> Connect
Ready -- shutdown ----> CloseEpochSocketOnly -> Stopped

Auth/version/capability/config failure -> Degraded
Repeated transient unavailability ------> Unavailable (all writes fenced)
```

There are deliberately no `SpawnChild`, `WaitChild`, `TerminateChild`, or
`ReapChild` transitions in external mode. A health probe proves listener
liveness, not authentication, version, initialization, or thread consistency.

Each successful initialized connection receives a client-generated, monotonically
advanced transport epoch. The app-server does not expose a stable server-instance
epoch in the reviewed surface. The bridge epoch therefore fences local RPC ids,
reverse requests, subscriptions, partial deltas, and pending work; it must never
be presented as a server generation number.

## Thread discovery and subscription semantics

Official-current descriptions and the sanitized uncommitted E4-E16 observations
assign distinct meanings to these operations:

- `thread/list` discovers persisted threads and is paginated. It does not
  subscribe.
- `thread/read` returns authoritative persisted data and runtime status without
  loading or subscribing.
- `thread/resume` loads or reuses the thread and subscribes that connection to
  later thread, turn, and item events.
- `thread/unsubscribe` removes that connection's subscription. It is not a writer
  handoff primitive.

Sanitized, uncommitted Local-0.149 observations:

- A second client could resume a thread created by the first client on the same
  app-server.
- The resuming client saw subsequent state but did not receive a historical
  `turn/started` event.
- Disconnecting lost notifications; reconnect did not replay them.
- Reconnect plus `thread/resume` and `thread/read` recovered persisted turns.
- Restarting the same app-server with the same isolated profile recovered
  persisted turns.
- `thread/unsubscribe` returned `unsubscribed` but did not release an active
  writer during the bounded experiment. Only exit of the owning app-server did.
- Two separate app-server processes using the same profile produced a conflict
  classified with code `-32600`; the raw message included
  `thread <id> already has an active writer`. The bridge must redact the id.

Current OpenAI Docs say a last-subscriber unsubscribe can lead to unload after a
30-minute no-subscriber inactivity grace. That newer rolling behavior does not
make unsubscribe a safe handoff for 0.149, and a 30-minute eventual unload would
not be an atomic writer transfer anyway.

## Reconnect, compensation, and deduplication

### Epoch loss

When a socket is lost, the bridge must atomically:

1. Remove the current client from the supervisor handle.
2. Fail all pending RPCs with the old transport epoch.
3. Reject all late responses and reverse-request answers from that epoch.
4. Persist an `epoch_lost` marker for every managed thread with an in-flight
   mutation or approval.
5. Discard partial presentation deltas. Completed items and turns remain the only
   durable semantic boundary.
6. Fence every new mutation until reconciliation succeeds.

### Resubscribe without creating a gap

For each bounded set of managed threads after reconnect:

1. Start a bounded per-thread reconciliation mailbox.
2. Call `thread/resume` so subsequent notifications reach the new connection.
3. Buffer those notifications without presenting partial deltas as authoritative.
4. Call `thread/read` with turns included.
5. If the exact promoted version supports and requires it, page
   `thread/turns/list` and `thread/items/list` under explicit page/count/byte
   limits.
6. Apply the snapshot, then fold buffered terminal events using stable ids and a
   monotonic status lattice.
7. If a mailbox overflows, pagination is unsupported, a page bound is exceeded,
   or state contradicts the pending intent, mark the thread `Uncertain` and keep
   writes disabled.

Resume-before-read narrows the gap but cannot create a true server snapshot
barrier because notifications have no replay cursor or global sequence. Stable
domain ids and terminal snapshots, not arrival time across connections, are the
authority.

### Deduplication keys

Within and across transport epochs:

- turn terminal state: `(thread_id, turn_id)`;
- item terminal state: `(thread_id, turn_id, item_id)`;
- server reverse request: `(transport_epoch, request_id)` plus thread/turn/item
  binding from the decoded request;
- local inbound intent: the existing durable Lark event identity;
- presentation delta: epoch-local only and disposable;
- notification without a stable domain id: never assumed deduplicable across an
  epoch boundary.

Status may move from unknown/in-progress to exactly one terminal observation.
A conflicting terminal observation is protocol drift and degrades the thread.
Do not hash prompt, command, tool arguments, approval text, or transcript into
logs as a substitute dedup key.

### Delivery certainty

| Observation | Classification | Automatic action |
| --- | --- | --- |
| Local validation/serialization failed before enqueue | Definitely not applied | Correct locally; a new explicit attempt is allowed |
| Server returned official overload `-32001` | Definitely rejected by ingress for that request | Retry only operations whose method policy allows it, after backoff and fresh state |
| Server returned a method-specific rejection and post-read proves unchanged state | Definitely not applied | Surface rejection; do not parse a generic code alone |
| Response success and required terminal/postcondition is observed | Applied | Commit the durable intent |
| Socket loss or timeout after a mutation may have been written | Uncertain | Reconcile; never automatically replay the mutation |
| Response shape is malformed after the request was written | Uncertain | Fault epoch, reconcile, no replay |
| Reconciliation cannot fit configured bounds | Uncertain | Fence thread and require operator action |

JSON-RPC code `-32600` is overloaded in the local evidence: it appeared for a
stale steering target and for an active-writer conflict. Code alone must not drive
retry or delivery classification. Raw server messages can contain identifiers and
must not be logged.

`clientUserMessageId` is correlation metadata only. Local 0.149 accepted the same
value twice and created a second turn. It is not an idempotency key. It can help an
operator correlate a recovered turn with a durable intent, but absence or
uniqueness never authorizes automatic replay.

## Turn and queue coordination

External shared mode retains one in-process actor per Lark scope and adds one
thread mutation lock. It still cannot atomically exclude an unrelated CLI or
Desktop client. `mutate_shared` therefore requires an operator contract that the
bridge is the sole writer for its managed threads. Observer clients may connect;
uncoordinated writers make the thread unsupported.

### Admission by observed thread state

| Authoritative state | Ordinary new Lark message | Explicit steer | Interrupt | App-server queue start |
| --- | --- | --- | --- | --- |
| Idle, no queued item, no pending intent | `turn/start` | Reject: no active target | Reject/no-op | Reject: empty |
| Idle with app-server queue | Keep durable local input behind existing order; use queue policy | Reject | Reject/no-op | Allowed only by an elected writer after fresh queue read |
| Active bridge-owned turn | Keep input durably pending or explicitly `thread/queue/add`; never call `turn/start` | Allowed only with exact current turn id and explicit user intent | Allowed only for an authorized actor and exact turn id | Reject while active |
| Active foreign/unknown-owner turn | Keep pending or reject with a busy notice | Reject | Reject | Reject |
| Reconnecting/reconciling | Keep pending; do not send | Reject | Defer | Reject |
| Prior mutation uncertain | Fence all following writes | Reject | Reconcile exact target first | Reject |
| Protocol drift/capability missing | Reject and degrade | Reject | Reject | Reject |

### Method conflict and replay rules

| Method | Precondition | Sanitized, uncommitted Local-0.149 note | Uncertain outcome rule |
| --- | --- | --- | --- |
| `thread/start` | Explicit new-thread intent | Non-idempotent by nature | Never replay after possible write |
| `turn/start` | Fresh read says idle; no local pending mutation; elected writer | Calls while active/concurrent could return success with the **same active turn id**, so success alone is ambiguous | Persist intent before send; never replay after possible write; require a new turn id and reconciled state before commit |
| `turn/steer` | Explicit steer command, same authorized scope, known active bridge turn, exact `expectedTurnId` | Exact id succeeded; stale id returned `-32600` | Never replay after possible write; on explicit rejection, refresh state before allowing a new user action |
| `turn/interrupt` | Authorized exact active turn id | Success returned `{}` and later terminal status was interrupted | A timeout is uncertain; after read proves the same exact turn remains active, a separately authorized retry may be safe, but it is not blind replay |
| `thread/queue/add` | Explicit queue policy, exact queue capability, durable local intent | Add worked | Treat as non-idempotent; never replay after possible write |
| `thread/queue/start` | Fresh read says idle and fresh queue read identifies work | While active it rejected and retained the queued item | Never replay after possible write; reconcile both turns and queue |

The default for another ordinary user message during an active turn is not
`turn/steer`. Steering changes the active model interaction and requires explicit
intent, an exact target, and authorization. The bridge may keep the inbound event
in its existing durable local queue until the turn completes; use app-server queue
methods only when the exact version/profile enables them.

E13 records a sanitized, uncommitted observation that the official
`codex queue --remote ws://... --thread ...` command succeeded against the local
shared endpoint. This repository does not yet reproduce that CLI participation
or cross-client serialization. A CLI queue or TUI writer can still race bridge
preconditions.

## Single approval handler

The official protocol describes reverse approval requests and
`serverRequest/resolved`, but it does not document how one request is selected or
broadcast among multiple connected clients. The 0.149 research did not establish
a safe cross-client transfer mechanism. Consequently, arbitrary multi-client
approval handling is a blocker for `mutate_shared`, not something to guess around.

### Static election

The first implementation uses explicit configuration, not cross-process leader
election:

- `approval_handler = "bridge"`: this bridge connection is the only permitted
  initiator and approval handler for bridge-managed turns. Other clients must be
  observers or use an approval policy that cannot delegate approvals to them.
- `approval_handler = "external"`: the bridge is read-only for that thread and
  never starts work that could require a reverse approval.
- More than one bridge process configured as handler for one endpoint is
  unsupported and fails the deployment precondition.

Within one bridge, exactly one control-stream consumer owns reverse requests for
the current transport epoch. It elects exactly one Lark recipient: the authorized
originator of the turn when eligible, otherwise one configured owner. It must not
broadcast actionable cards to multiple users.

### Request state machine

```text
Received(epoch, request_id, thread_id, turn_id)
  -> ValidateEpochAndManagedTurn
  -> PersistPendingAndClaimSingleHandler
  -> PresentToOneAuthorizedPrincipal
  -> RespondOnSameConnection
  -> Await serverRequest/resolved
  -> PersistResolved

Invalid epoch/binding/handler -> fail closed and degrade the thread
```

The response lease is connection-scoped. A replacement connection must never
answer an old request id.

### Timeout and failure behavior

- Let the UI deadline be the smaller of the configured five-minute ceiling and
  `autoResolutionMs` minus a five-second response margin when that field exists.
- If the remaining server window is no larger than the response margin, resolve
  immediately with the most restrictive valid denial.
- On timeout, deny or cancel; never auto-accept. Permission requests receive an
  empty granted subset.
- Persist only safe audit fields: endpoint label, transport epoch, request kind,
  thread/turn/item binding, selected handler role, actor identity under existing
  store protections, decision category, and timestamps. Normal logs contain only
  redacted categories.
- A duplicate UI action is rejected by a durable compare-and-set from `pending`
  to one decision.
- `serverRequest/resolved` is the completion signal. Sending a response alone is
  not proof that the server applied it.

### Disconnect and reassignment

An in-flight reverse request cannot safely move to another connection with the
reviewed protocol. On disconnect:

1. Mark it `approval_uncertain` and remove the actionable UI.
2. Do not answer its request id on the new epoch.
3. Reconnect, resume, and reconcile the turn/item state.
4. Elect the configured handler for **future** requests only.
5. If the old request did not resolve into authoritative terminal state, keep the
   thread fenced and require an operator to interrupt or inspect it.

Planned reassignment requires a drain: no active turn, no pending reverse request,
an observed idle snapshot, close of the old client connection, configuration
change, then initialization of the new handler. This is reassignment between
requests, not takeover of one live approval.

## Backpressure, timeouts, and limits

The implementation follow-ups must commit limits rather than inherit WebSocket or
server defaults implicitly.

| Resource | Proposed initial limit |
| --- | --- |
| Endpoint text | 2 KiB |
| Bearer token file | 8 KiB |
| WebSocket text frame / decoded RPC | 32 MiB, matching current JSONL maximum |
| Binary WebSocket frame | Reject |
| Outbound transport queue | Existing 64 high / 256 normal count classes and 64 MiB byte budget |
| Reliable reverse/event queue | Existing count and byte budgets; overflow faults the epoch |
| Connect/TLS/WebSocket deadline | 15 seconds |
| Initialize deadline | Existing 10 seconds |
| Control RPC deadline | Existing 30 seconds |
| Interrupt RPC deadline | Existing 10 seconds |
| Reconnect | Jittered 0.5, 1, 2, 4, 8, 16, then 30 seconds maximum |
| Managed subscriptions per endpoint | 64 until capacity testing justifies more |
| Reconciliation buffer per thread | Existing 256-event / 4 MiB mailbox bounds |
| Reconciliation pages | 32 pages and 3,200 turns/items total per thread |
| Reconciliation materialized bytes | 32 MiB per thread and 64 MiB per endpoint pass |
| Approval UI deadline | Five minutes maximum, reduced by server auto-resolution margin |
| Unavailable threshold | Publish unavailable after ten failures or five minutes; continue sparse health attempts with writes fenced |

The exact constants belong in the implementation issue and tests. Exceeding a
bound is an explicit overloaded/uncertain result, never truncation that is later
treated as complete state.

Official-current documents WebSocket ingress overload as `-32001` and asks
clients to use exponential backoff with jitter. That instruction does not grant
permission to replay an uncertain mutation. High-priority approval responses and
interrupts must retain priority over normal requests, with a burst limit so normal
traffic cannot starve forever.

## Reproducibility record

All recorded protocol identifiers below are symbolic. No endpoint, token,
credential, profile path, prompt, command, approval payload, real thread id, or
turn id belongs in evidence output.

### Environment and common setup

| Item | Value |
| --- | --- |
| Date | 2026-08-23 |
| OS/architecture | Darwin arm64 |
| Codex | Exact `codex-cli 0.149.0` |
| Listener | Explicit loopback `ws://127.0.0.1:<ephemeral-port>` unless stated otherwise |
| Profile | Isolated temporary `CODEX_HOME`; the restart case reused only that isolated profile |
| Clients | Raw WebSocket clients A/B with one RPC per text frame; official CLI where named |
| Initialization | Both clients sent `initialize`, validated a response, then sent `initialized` |
| Logging | Result categories and booleans only; protocol payloads discarded |
| Bounds | Recorded experiments were deadline-bounded; E17 additionally caps each text message at 256 KiB, aggregate inbound text at 1 MiB, messages at 64 per client, JSON at depth 64 / 8,192 nodes, and user-agent text at 1 KiB |

The exact CLI surface can be checked without contacting a server:

```bash
codex --version
codex app-server --help
codex queue --help
codex app-server generate-json-schema --experimental --out <temporary-directory>
```

The sanitized, uncommitted help-surface check advertised listener forms
`stdio://`, `unix://`,
`unix://PATH`, `ws://IP:PORT`, and `off`; remote clients advertised `ws://`,
`wss://`, and Unix forms. It also advertised capability-token and signed-bearer
server flags. Direct `wss://` listener input was rejected.

### Experiment outcomes

| ID | Procedure | Sanitized outcome | Evidence status / authority |
| --- | --- | --- | --- |
| E1 | Start non-loopback WS without auth | Startup refused and required capability or signed bearer auth | Sanitized, uncommitted Local-0.149 observation |
| E2 | Configure capability-token SHA-256; connect without and with bearer | Missing bearer returned HTTP 401; matching bearer completed the upgrade | Sanitized, uncommitted Local-0.149 observation; not a committed auth test |
| E3 | Initialize raw clients A and B concurrently | Both initialized | Sanitized, uncommitted Local-0.149 observation |
| E4 | A starts thread; B discovers and resumes it | Same-server multi-client resume succeeded | Sanitized, uncommitted Local-0.149 observation; not a committed resume test |
| E5 | A starts work; B resumes/reads, then observes later events | Active state was visible; B saw subsequent state but no historical start event | Sanitized, uncommitted Local-0.149 observation; not a committed active/event test |
| E6 | Steer with exact then stale expected turn id | Exact target succeeded; stale target returned `-32600` | Sanitized, uncommitted Local-0.149 observation; not a committed steer test |
| E7 | Interrupt exact active turn | Request succeeded and the turn reached interrupted terminal state | Sanitized, uncommitted Local-0.149 observation; not a committed interrupt test |
| E8 | Disconnect/reconnect and resume/read | No notification replay; persisted turns recovered through authoritative reads | Sanitized, uncommitted Local-0.149 observation; not a committed reconnect test |
| E9 | Restart server with same isolated profile | Persisted turns recovered; client synthesized a new transport epoch | Sanitized, uncommitted Local-0.149 observation; not a committed restart test |
| E10 | Reuse `clientUserMessageId` for a second start | A second turn was created; field is not idempotent | Sanitized, uncommitted Local-0.149 observation; not a committed idempotency test |
| E11 | Submit active/concurrent `turn/start` | Calls could succeed with the same active turn id; result is unsafe/ambiguous | Sanitized, uncommitted Local-0.149 observation; not a committed race test |
| E12 | Queue add, then queue start while active | Add worked; start rejected and retained the queued item | Sanitized, uncommitted Local-0.149 observation; not a committed queue test |
| E13 | Run official `codex queue --remote` on the shared endpoint | CLI used the same endpoint successfully | Sanitized, uncommitted Local-0.149 observation; not a committed CLI/queue test |
| E14 | Try raw JSON, HTTP Upgrade, ws+unix, and proxy approaches to advertised Unix transport | No usable response within the bounded attempts | Sanitized, uncommitted Local-0.149 observation; negative, incomplete, and not a Unix reproduction |
| E15 | Resume one profile thread from two separate server processes | Second server returned active-writer conflict under generic `-32600` | Sanitized, uncommitted Local-0.149 observation; not a committed writer-conflict test |
| E16 | Unsubscribe the subscribed client, then retry writer acquisition elsewhere | Reported unsubscribed; writer remained until owning server exited | Sanitized, uncommitted Local-0.149 observation; not a committed handoff test |
| E17 | Verify exact server version/profile; connect two read-only clients; initialize and perform one-row `thread/list`; require an observed peer disconnect for both; require exact HTTP 200 health; repeat with a third client | Identity matched; all three clients completed the bounded read-only flow; health remained exact-200; external server remained alive; Node observed 1006/unclean rather than a server close reply | Only committed Local-0.149 real-server reproduction; fake-peer tests cover probe fail-closed behavior but are not additional real-server evidence |

E14 does not prove Unix transport is broken. It proves only that the attempted
client approaches are insufficient, so the bridge must reject Unix instead of
guessing a framing or URL convention.

E1-E16 are recorded, sanitized local observations from the investigation. They
are not represented as committed executable tests by this RFC. In particular,
this repository does not yet reproduce authentication, thread resume, active
state, event delivery, steering, interrupt, queue participation, approval
routing, or Unix transport against a real server. E17 is the only committed
real-server reproduction; its fake-peer suite checks the probe's negative
configuration, protocol, and lifecycle behavior, not the missing real-server
capabilities. That distinction keeps the evidence claim narrower than the
exploratory work.

### Committed read-only lifecycle probe

[`tools/codex_shared_server_probe.mjs`](../tools/codex_shared_server_probe.mjs)
has no runtime dependency installation and performs only initialize plus a
one-row `thread/list`. It requires Node 26's built-in WebSocket client, a literal
loopback endpoint, one exact semantic version, and an absolute canonical private
profile containing the probe marker. Every initialize response must report that
same canonical `codexHome` and exactly one matching version token in
`userAgent`; an empty initialize result cannot pass. It emits only booleans or a
fixed `configuration`, `protocol`, or `health` failure stage.

The probe rejects binary frames, oversized or over-budget input, excessive JSON
depth/work, unknown or duplicate response ids, and more than 64 inbound messages
per client. A client-initiated close succeeds only after a peer disconnect event
arrives before the deadline; a peer that ignores close fails. Clean code-1000
handshakes and the current 0.149.0 code-1006/unclean observation are reported as
different booleans rather than conflated. The latter is a negative interoperability
finding, not an acceptable production close contract. The health request disables
redirects and accepts status 200 exactly. The no-dependency fake WebSocket peer
suite exercises these failure paths in CI, including unsafe endpoint forms,
noncanonical or symlinked profiles and markers, private Unix permissions,
identity mismatch, and health redirects:

```bash
node --test tools/tests/test_codex_shared_server_probe.mjs
```

In one shell, start an isolated server and keep that shell as its owner:

```bash
set -eu
task_probe_home="$(mktemp -d)"
task_probe_home="$(cd "$task_probe_home" && pwd -P)"
cleanup_issue8_probe() {
  if [ -n "${task_probe_home:-}" ] &&
     [ "$task_probe_home" != / ] &&
     [ -f "$task_probe_home/.lark-codex-bridge-issue8-isolated-v1" ]; then
    rm -rf -- "$task_probe_home"
  fi
}
trap cleanup_issue8_probe EXIT
trap 'exit 130' HUP INT TERM
umask 077
printf '%s\n' 'lark-codex-bridge issue8 isolated profile v1' \
  > "$task_probe_home/.lark-codex-bridge-issue8-isolated-v1"
chmod 700 "$task_probe_home"
chmod 600 "$task_probe_home/.lark-codex-bridge-issue8-isolated-v1"
test "$(codex --version)" = 'codex-cli 0.149.0'
printf 'Copy this isolated profile path into the second shell: %s\n' \
  "$task_probe_home"
CODEX_HOME="$task_probe_home" \
  codex app-server --listen ws://127.0.0.1:45152
```

In another shell, replace the placeholder with the path printed by the first:

```bash
CODEX_SHARED_PROBE_ENDPOINT=ws://127.0.0.1:45152 \
CODEX_SHARED_PROBE_EXPECTED_VERSION=0.149.0 \
CODEX_SHARED_PROBE_EXPECTED_HOME=/absolute/path/printed-by-first-shell \
node tools/codex_shared_server_probe.mjs
```

Expected sanitized result:

```json
{"ok":true,"exactVersionVerified":true,"isolatedProfileVerified":true,"twoClientsInitializedAndDisconnected":true,"twoClientCloseHandshakesClean":false,"healthAfterClientDisconnect":true,"freshClientInitializedAndDisconnected":true,"freshClientCloseHandshakeClean":false}
```

Here `"ok":true` means only that the bounded read-only E17 procedure reached
each required checkpoint and emitted a complete sanitized observation. The two
explicit `false` close fields preserve the code-1006/unclean result. `ok` does
not mean a clean WebSocket close, authenticated transport, production
compatibility, or support for resume, active state, event delivery, steering,
interrupt, queueing, approval routing, or Unix transport.

The operator-owned server remained alive after all probe clients closed. The
probe cannot send a process signal because it receives no PID or process handle.
This is read-only lifecycle evidence, not the production external connector or
an authentication test. After recording the result, press Ctrl-C in the first
shell. That stops the exact operator-owned server; the EXIT trap then removes
only the freshly created, marker-bearing profile.

## Proving external process non-ownership

The future implementation is acceptable only if all of these are true:

1. The external backend type graph contains no process factory, command, child,
   PID, wait, kill, or terminate capability.
2. External shutdown performs a bounded RPC/WebSocket close only.
3. A fake external-server integration test records every accepted socket and any
   process-control callback; bridge shutdown and drop close only the socket and
   never invoke process control.
4. A real gated smoke starts the server in the test harness, connects the bridge,
   shuts down and crashes the bridge client, then proves `/healthz` and a fresh
   initialized connection still work with the same server.
5. Server crash is initiated only by the harness/operator. The bridge reconnects
   after the harness restarts it; the bridge never restarts it itself.
6. Configuration tests prove external mode cannot deserialize spawned-only
   fields and cannot fall back to spawned mode.

E17 is committed evidence only for the bounded black-box read-only survival
property above. It does not exercise the bridge backend, authentication, clean
close, mutations, or recovery. The structural and integration proofs remain
mandatory follow-up gates because no production external backend exists in this
RFC commit.

## Client support matrix

| Peer/topology | Documentation | Local evidence | Status |
| --- | --- | --- | --- |
| Bridge-owned spawned app-server over stdio | Current bridge architecture; stdio documented | Existing bridge tests and smoke, independent of E1-E17 | Supported |
| Explicit independent app-server plus read-only raw client | WebSocket documented as experimental/unsupported for production | Committed E17: exact identity, initialize/list, disconnect, health, and fresh-client reuse only; close was 1006/unclean | Research viable; production compatibility not established |
| Same-server resume, active/event observation, and reconnect | Protocol surface described by rolling docs | Sanitized uncommitted E4-E5/E8-E9 observations only | Not repository-reproduced; unsupported |
| Official CLI/TUI on explicit remote endpoint | `--remote` documented | Sanitized uncommitted E13 observation only | Not repository-reproduced; experimental/unsupported here |
| CLI and bridge as simultaneous arbitrary writers | No atomic writer arbitration documented | Sanitized uncommitted E10-E13 observations only | Unsupported |
| Two independent app-server processes on one profile/thread | Not a documented sharing topology | Sanitized uncommitted E15-E16 observations only | Unsupported |
| Authenticated external endpoint | Authentication is documented | Sanitized uncommitted E1-E2 observations only; E17 is unauthenticated loopback | Not repository-reproduced; unsupported |
| Shared approvals | No multi-client approval election documented | Design only; no E1-E17 approval reproduction | Unsupported |
| Unix-socket shared endpoint | Official-current documents it | Sanitized uncommitted, negative, incomplete E14 observation only | Fail closed / unsupported pending RFC and reproduction |
| Direct WSS listener | Not documented as listener | Sanitized uncommitted 0.149 rejection observation only | Unsupported; use an externally owned TLS terminator design |
| Desktop-owned endpoint reuse | No documented stable endpoint/discovery or third-party attachment contract found | Not tested | Unknown / unsupported |
| Codex VS Code extension's internal server reuse | Docs say app-server powers rich clients, but do not expose a reuse contract | Not tested | Unknown / unsupported |

The bridge must never scan Desktop files, process arguments, ports, sockets, or
profiles in an attempt to turn `unknown` into discovery.

## Security and privacy invariants

- Authentication, TLS, exact version, and required capability checks happen
  before a thread id is accepted or any mutation is enabled.
- Endpoint and secret configuration have custom redacted `Debug` output.
- Bearers, signing secrets, authorization headers, endpoint query strings,
  initialize `codexHome`, prompts, tool arguments, commands, approval details,
  thread ids, turn ids, item ids, and raw JSON-RPC errors never enter ordinary
  errors or tracing.
- WebSocket binary frames, oversized text frames, malformed JSON, unknown
  required messages, and duplicate response ids fault the epoch.
- A server response is decoded and size-checked before typed state changes.
- Reverse responses are accepted only for the exact connection epoch and live
  request lease.
- Source authorization remains the Lark tenant/sender/group policy. Sharing a
  Codex endpoint does not grant another Codex client authority to impersonate a
  Lark source or approve on its behalf.
- Audit records distinguish Lark source identity, static initialize client
  identity, transport epoch, and approval actor. They do not claim that
  `clientInfo` authenticates a remote client; it is metadata.
- A malformed, missing, expired, or rejected credential is a safe outage, not a
  request to downgrade transport security.

## Promotion and release gates

External mode stays compile-time or configuration gated and documented as
experimental until every applicable gate passes:

- the exact Codex version is promoted through the schema compatibility policy;
- authenticated loopback WS and verified WSS client tests pass on Linux, macOS,
  and Windows;
- max-frame, binary-frame, fragmentation, queue saturation, overload, timeout,
  and unknown-message tests pass;
- fake-server lifecycle tests prove no external process ownership;
- reconnect/resubscribe/snapshot-buffer ordering and overflow tests pass;
- mutation uncertainty tests prove no replay after write/timeout/disconnect;
- two-client races prove the admission matrix, including same-turn-id start;
- approval routing is reproduced for two clients or external writes remain
  disabled;
- credential redaction and rotation tests pass;
- each issue-local real smoke required by follow-ups 2-5 fails when its explicit
  endpoint/auth/version configuration is missing; a skip is not evidence;
- Desktop remains disabled unless OpenAI documents and the project reproduces a
  supported attachment contract.

Because official-current calls WebSocket unsupported for production, passing
project tests can justify an opt-in experimental feature, not an unqualified
production-support claim.

## Bounded implementation follow-ups

Before Issue #8 can close, maintainers must create or link one bounded GitHub
issue for each of the six work packages below and record those links in this
section. An equivalent split is acceptable only if it preserves every scope and
dependency. A required real-server smoke is part of the definition of done for
items 2-5; it must not be postponed into a standalone catch-all issue or left as
untracked prose.

1. **P2: promote the exact Codex protocol contract required by shared endpoint
   mode**
   - Tracking issue: **required before #8 closure**.
   - Resolve the 0.149 compatibility blockers or select a later exact binary.
   - Add queue, unsubscribe, steering, status, and every approval shape used by
     external mode to versioned contracts.
   - Exclude transport and runtime behavior.

2. **P2: add explicit Codex backend configuration, endpoint security, and an
   authenticated connection gate**
   - Tracking issue: **required before #8 closure**.
   - Add the tagged modes, URL policy, exact-version field, secret sources,
     redacted debug/errors, TLS/auth handshake canary, and no-fallback tests.
   - Definition of done includes a gated real exact-binary smoke proving the
     explicit endpoint/auth/version gate. Missing configuration, invalid
     credentials, or a skipped smoke is a hard failure, not acceptance.
   - Exclude general RPC transport, reconnect, and mutation behavior.

3. **P2: implement a bounded authenticated WebSocket app-server transport with
   no process ownership**
   - Tracking issue: **required before #8 closure**.
   - Add text-frame RPC transport, TLS/auth handshake, close semantics, limits,
     priority queues, fake server, and the lifecycle non-ownership proof.
   - Definition of done includes a gated real exact-binary bridge smoke with two
     read-only clients, bridge shutdown and crash, exact health, a fresh client,
     external-server survival, and truthful clean/unclean close reporting.
     Missing gate configuration or skip is failure.
   - Exclude reconnect and mutations.

4. **P2: persist external transport epochs and reconcile subscriptions after
   reconnect**
   - Tracking issue: **required before #8 closure**.
   - Add epoch fencing, resume-before-read buffering, bounded pagination,
     terminal deduplication, unavailable state, and restart tests.
   - Definition of done includes a gated real exact-binary smoke that forces a
     bounded disconnect/reconnect and operator-owned server restart, then proves
     resume/read reconciliation without replaying uncertain work. Missing gate
     configuration or skip is failure.
   - Exclude new mutation policy.

5. **P2: coordinate shared-server mutations and route approvals through one
   fail-closed handler**
   - Tracking issue: **required before #8 closure**.
   - Add the durable intent state machine, per-thread write fence, exact target
     rules, queue policy, uncertainty handling, adversarial two-client tests,
     static approval-handler election, single-recipient UI, durable claim,
     deadlines, denial, resolution, disconnect fencing, and drained
     reassignment.
   - Definition of done includes a gated real exact-binary authenticated smoke
     for two-client start races, exact-id steer/interrupt, queue behavior, and
     the configured approval route. Missing gate configuration or skip is
     failure.
   - If routing cannot be proven, permanently gate external writes and document
     read-only support.

6. **P3 RFC: determine the Codex app-server Unix-socket WebSocket contract**
   - Tracking issue: **required before #8 closure**.
   - Produce a sanitized, committed, platform-specific, deadline-bounded raw
     handshake reproduction, peer/filesystem permission policy, and
     cross-platform support decision before any implementation issue is opened.
   - Do not silently alias Unix to JSONL or TCP.

## Final recommendation

Create or link the five core issues and the Unix RFC before closing #8. The only
committed real-server reproduction today is E17's bounded, unauthenticated,
read-only initialize/list/disconnect/health flow, with an explicitly unclean
code-1006 close. Authentication, resume, active state, event delivery, steering,
interrupt, queueing, approval routing, and Unix transport remain uncommitted
observations or design and require the issue-local real smokes above.

Proceed first with the exact contract, then the configuration/security and
bounded transport issues for an opt-in experimental read-only backend. Proceed
to shared mutations only after reconnect, two-client race, and single approval
handler gates are complete. Do not ship arbitrary simultaneous writers, Unix,
Desktop reuse, direct public listeners, or live approval reassignment on the
present evidence.
