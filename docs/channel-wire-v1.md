# Channel boundary and Node wire v1

The Rust core owns authorization, normalization, durable inbox/outbox state,
backpressure, media-cache policy, and Codex routing. Provider adapters expose
only four stable capabilities:

- an inbound source with connection state;
- single-attempt outbound delivery with `retryable`, `uncertain`, or
  `definitive` failure;
- bounded chat/message queries;
- controlled resolution of an opaque message/resource handle into bounded
  bytes.

No Node SDK type crosses this boundary. The first sidecar stage replaces only
the inbound WebSocket and connection state; native Rust OpenAPI remains the
query, media, and outbound implementation.

## Process and framing

The sidecar is a single local process tree supervised by Rust. The Node leader
is placed in a dedicated POSIX process group or a Windows Job object; Rust
terminates that entire boundary on bootstrap failure, protocol failure,
timeout, crash, stdout EOF, shutdown expiry, cancellation, and handle drop.
The direct child is reaped within a fixed bound. Stdin and stdout carry
UTF-8 NDJSON version 1 (`v: 1`, `protocol: "lark-channel"`), with one JSON
object per line and a 1 MiB hard frame limit. Stderr is log-only. Rust drains
it in fixed-size chunks, discards the remainder of an oversized or unterminated
record, and keeps draining later records. It records only byte counts and
static classifications, never content. The child logger similarly discards
SDK-provided messages and writes static classifications only.

Startup is:

1. Node → Rust `hello`, with protocol, version, maximum frame size, and
   `connection_state`, `durable_event_ack`, `inbound_events`, and
   `graceful_shutdown` capabilities.
2. Rust → Node correlated `configure`, containing credentials over stdin plus
   negotiated frame, in-flight-event, and ack-timeout bounds.
3. Node → Rust correlated successful `response` after `WSClient.start` accepts
   the dispatcher. This is protocol configuration acceptance, not connection
   readiness. SDK state callbacks are held until that response is queued.
4. Node → Rust `state: "connected"` from `onReady` (or the SDK's authoritative
   status snapshot). `NodeSidecar::start` succeeds only after this frame. A
   terminal `failed`, process exit, stdout EOF, or 30-second connection deadline
   before it is returned makes the one bootstrap attempt fail.

Credentials are never argv or environment values. Rust starts the child with
a cleared environment. They are redacted from `Debug`, errors, and tracing.

Steady-state frames are:

- Node → Rust `state` (`connecting`, `connected`, `reconnecting`, `backoff`,
  `failed`, or `stopped`);
- Node → Rust `event` with a bounded correlation `id` and the raw Lark event
  envelope in `payload`;
- Rust → Node `event_ack` with the same id and `ok: true`, or `ok: false` plus
  a stable error code;
- either direction `error` for a correlated unknown message;
- Rust → Node `shutdown`, answered by a correlated `response` before exit.

IDs are at most 128 ASCII letters, digits, `_`, `-`, or `.`. Unknown versions,
malformed frames, unknown correlations, overlong frames, and unsolicited
responses fail the process session. A well-formed unknown message receives
`unknown_message`. Rust bounds the event queue and stdin-write queue; queue
saturation returns a negative ack. Node independently bounds pending event
acks. Rust rejects reuse of an event ID while its durable decision is queued or
running; IDs are released on completion and every process-epoch termination,
so concurrent decisions may safely complete in reverse order. Every wait has a
timeout. Closing protocol stdout while leaving the child alive is a crash, not
an unbounded wait.

## Durable upstream receipt

`@larksuiteoapi/node-sdk` 1.72.0's low-level `WSClient.handleEventData` awaits
`EventDispatcher.invoke`. It sends a success receipt only when that promise
resolves, and sends code 500 when it throws. The sidecar subclasses the public
dispatcher entrypoint for `im.message.receive_v1` so it can preserve the raw,
unflattened event envelope (the stock dispatcher flattens `header` and `event`)
while retaining the SDK's awaited receipt path. That handler:

1. emits a correlated `event` to Rust;
2. waits for the matching `event_ack`;
3. returns only on `ok: true`;
4. throws on negative ack, timeout, local capacity exhaustion, or shutdown.

Rust issues `ok: true` only after normalization, SQLite inbound registration,
and bounded queue reservation. Thus persistence failure, timeout, or
backpressure cannot be reported upstream as success. This behavior is covered
by the fake-sidecar integration test; it is not inferred from connection state.
The offline `sdk-contract-check.cjs` additionally executes 1.72.0's compiled
`WSClient.handleEventData`: no receipt appears while the dispatcher is blocked,
resolution produces code 200, and rejection produces code 500.

## Packaging and rollout

The sidecar pins `@larksuiteoapi/node-sdk` exactly to 1.72.0 in both
`package.json` and `package-lock.json`. Installation is a build/deploy step:

```bash
npm ci --ignore-scripts --prefix sidecar
npm run check --prefix sidecar
```

The Rust runtime never runs npm or downloads dependencies. `native` remains
the default. `node-sidecar` is opt-in and may be configured to fall back to
native when the initial executable, protocol, configuration, or SDK connection
attempt fails.
"Initial" includes reaching the first authoritative SDK `connected` state:
mere configure success never suppresses fallback. Once the first process has
connected, later crashes stay on the explicitly selected sidecar and are
supervised with a fresh handshake on every process epoch; the bridge never
switches live sources mid-run. Restart delay escalates through the existing
bounded jittered schedule (30-second cap) and resets only after one process
epoch remains continuously connected for 30 seconds, preventing
connected-then-crash loops from staying at the minimum delay.
