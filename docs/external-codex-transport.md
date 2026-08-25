# Bounded external Codex WebSocket transport

Issue [#29](https://github.com/M1nt-Ch0c0/lark-codex-bridge/issues/29) adds the
long-running transport immediately above the authenticated endpoint gate. It
supports the exact-version `observe_shared` surface. Issue #30 extends the same
socket-only owner with the promoted read/resume operations needed by
`resume_shared`; the durable coordinator is documented in
[external-codex-reconciliation.md](external-codex-reconciliation.md). The
ordinary bridge `run` path remains mutation-driven and therefore continues to
reject external mode. The separately configured #31 write surface is documented
in [external-codex-write-coordination.md](external-codex-write-coordination.md).

## Ownership boundary

`ExternalReadOnlyConnection` contains one admitted WebSocket, its bounded RPC
owner, an exact `WireAdapter`, a client-generated connection epoch, and a safe
gate report. Its type graph has no process factory, command, child, PID, wait,
kill, terminate, restart, or server configuration capability.

- Orderly shutdown sends a normal WebSocket close, waits for the peer only for
  the configured grace, and returns a content-free `WebSocketCloseReport`.
- Abrupt abort drops the connection task and socket without a close handshake.
- Neither path can signal or reconfigure the operator-owned server.
- A peer close frame is distinguished from EOF/reset/timeout. Handshake
  completion and the optional numeric close code are recorded; remote close
  reason text is discarded.

The exact 0.149 server currently does not answer the client's close with a
clean handshake in the bounded local smoke. That remains an explicit
`handshake = Incomplete` observation, not a passing clean-close claim.

## Protocol and resource policy

The endpoint gate performs authentication, typed initialize, exact version
validation, `initialized`, and a one-row typed `thread/list` canary on the same
socket before the long-running owner is constructed. A load-balanced or newly
opened socket cannot inherit another connection's gate result.

The transport accepts one JSON-RPC object per assembled text message. It:

- rejects binary/raw frames, malformed JSON, oversized messages, excessive JSON
  structure, stale/unknown/duplicate responses, server requests, and unexpected
  notifications; `observe_shared` accepts no notifications, while
  `resume_shared` accepts only exact promoted status/terminal lifecycle traffic;
- bounds WebSocket frame/message size, outbound count and byte queues, inbound
  retained bytes, RPC pending requests, notification/reliable queues, write
  deadlines, request deadlines, incomplete-fragment shutdown, and close grace;
- retains separate high and normal queues with an eight-message high-priority
  burst limit, so control traffic can overtake backlog without starving normal
  traffic;
- exposes typed `thread/list` for `observe_shared`; `resume_shared` additionally
  exposes typed `thread/resume`, `thread/read`, `thread/turns/list`, and
  `thread/items/list`. It exposes no start, steer, interrupt, queue, approval,
  generic mutation, process, or reconnect API.

Endpoint URLs, hosts, bearer values, token paths, authorization headers, raw
payloads, server error text/data, close reasons, `codexHome`, and thread records
are absent from transport errors and `Debug`. Only the opaque endpoint label,
exact version/profile, epoch, close-handshake classification, and optional
numeric close code are safe observations.

## Verification

The fake authenticated server suite covers socket-only ownership, orderly and
abrupt close behavior, server survival and fresh reuse, binary/malformed/unknown
traffic, duplicate and stale response IDs, complete fragmentation, unfinished
fragment shutdown bounds, typed reads, and secret redaction:

```bash
cargo test --locked --test external_transport
```

The real smoke starts only the explicitly supplied exact binary, opens two
authenticated read-only bridge clients concurrently, performs bounded list
reads, orderly-shuts one client, abruptly aborts the other, requires exact HTTP
200 health, opens and initializes a fresh third client, rechecks health, and
only then asks the smoke harness to stop its own child:

```bash
CODEX_EXTERNAL_TRANSPORT_E2E=1 \
CODEX_EXTERNAL_TRANSPORT_BINARY=/absolute/path/to/codex \
CODEX_EXTERNAL_TRANSPORT_EXPECTED_VERSION=0.149.0 \
cargo test --locked --test external_transport_smoke \
  real_exact_binary_transport_preserves_external_server_across_two_clients_and_fresh_reuse \
  -- --ignored --exact --nocapture
```

The ignored marker keeps ordinary builds independent of an installed Codex
binary. It is not acceptance evidence. The exact invocation treats every
missing/empty variable, skipped selection, version mismatch, failed client,
non-200 health result, or unexpected server exit as a test failure. CI installs
the official exact 0.149 package and invokes the smoke by exact name on Linux,
macOS, and Windows.
