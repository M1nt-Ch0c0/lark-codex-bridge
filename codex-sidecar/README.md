# Codex protocol sidecar

This package is the version-specific Codex app-server adapter. Rust remains
authoritative for durable state, authorization, epoch fencing, retry and
uncertain-result policy, rendering, and operator-visible lifecycle state. The
sidecar owns one upstream app-server process for one process epoch and never
reconnects, retries, or replays a request.

## Packaging

`@openai/codex` is pinned exactly to `0.151.0` in both `package.json` and
`package-lock.json`. Installation is a build/deployment step, never a runtime
operation:

```bash
npm ci --ignore-scripts --prefix codex-sidecar
npm run verify --prefix codex-sidecar
```

CI performs that pinned install on Linux, macOS, and Windows and runs a real
`hello` / `configure` / `initialize` / `thread/list` / `sidecar/shutdown`
lifecycle with `codexBinary: null` and no override arguments. The smoke therefore
uses the lockfile-installed `@openai/codex` 0.151.0 rather than a fixture or an
ambient Codex executable.

Each matrix lane then creates a platform-and-architecture-specific artifact
directory containing the sidecar sources, production `node_modules`, the
matching native Codex package, and a SHA-256 inventory. The build step exports
the manifest digest as trusted workflow state. CI uploads that directory with
hidden entries included, downloads it independently, and uses the verifier from
the checked-out source (never code from the artifact) to check the manifest
digest and every inventory entry before repeating the same real lifecycle. The
downloaded artifact does not contain CI verifier code. The smoke invokes Node
directly, removes npm and other tools from the child `PATH`, uses a fresh
credential-free `CODEX_HOME`, and performs no install or package download.
Because GitHub directory artifacts normalize file modes, the trusted verifier
restores only the executable bits recorded in the authenticated manifest, and
only after every path, size, and digest check succeeds.
Release packaging must ship the corresponding tested directory contents; a
source-only `codex-sidecar/` directory is not a deployable artifact.

When `codexBinary` is `null`, the sidecar runs the `codex.js` entrypoint from
that pinned package. An explicit binary or wrapper can be supplied by Rust;
`codexArguments` is limited to eight non-empty, non-NUL arguments of at most
1024 UTF-8 bytes each. Arguments precede `--version` or `app-server --listen
stdio://`. They are configuration only: credentials and user/message content
must never be placed there.

## Bootstrap wire v1

Stdin/stdout use UTF-8 NDJSON. Every record, including the final record before
EOF, must end in LF; an unterminated tail fails closed. Stdout is
protocol-only. The sidecar's first frame is exactly:

```json
{"protocol":"codex-sidecar-wire","v":1,"type":"hello","maxFrameBytes":33554432,"capabilities":["bounded-ndjson","correlated-requests","correlated-server-requests","epoch-on-restart","no-mutation-replay","priority-control-lane","stable-domain-jsonrpc"]}
```

Rust answers with one bounded configuration frame:

```json
{"v":1,"type":"configure","id":"configure-1","codexBinary":"codex","codexHome":null,"codexArguments":[],"maxFrameBytes":33554432,"maxPending":448}
```

Unknown fields, malformed values, arguments outside their count/byte bounds,
frame limits above 32 MiB, or `maxPending` above 448 fail closed. The 448
correlation slots cover 320 normal-priority and 64 reserved high-priority
outbound requests plus 64 independently retained upstream reverse requests. The sidecar
executes the binary directly without a shell, requires version output to be
exactly `codex-cli 0.149.0` or `codex-cli 0.151.0` plus an optional final line
ending, selects the corresponding adapter module, and starts exactly one
`app-server --listen stdio://` child. Readiness is returned as:

```json
{"v":1,"type":"response","id":"configure-1","ok":true,"data":{"upstreamVersion":"0.151.0","adapterVersion":"0.151.0","capabilities":["bounded-ndjson","correlated-requests","correlated-server-requests","epoch-on-restart","no-mutation-replay","priority-control-lane","stable-domain-jsonrpc"]}}
```

Bootstrap failure uses `ok:false` and a static string error code. Paths,
upstream stderr, provider error messages, payloads, and credentials are never
included.

After readiness, both local and upstream streams are JSON-RPC-shaped NDJSON.
Local payloads use the stable bridge domain shape; raw provider payloads are
accepted only by the version-specific adapter. Unreviewed upstream
notifications are filtered inside Node, including their method names. Unknown
reverse requests are rejected upstream without crossing the boundary.

## Correlation and mutation safety

- Local and upstream request IDs are translated into disjoint, epoch-randomized
  correlations. The pending maps share the negotiated count bound.
- Reuse of an active/recent ID, an unknown response, or a late duplicate
  response terminates the epoch.
- An ordinary local-to-upstream request has a 30-second completion deadline;
  expiry terminates the epoch because the mutation result may be uncertain.
  Upstream reverse requests instead have a 180-second deadline, covering the
  bridge's 15-second Lark fetch, 30-second ffmpeg, and 60-second ASR envelope
  plus queueing margin. Reverse expiry returns a static request-specific
  `-32022` error upstream without
  killing the epoch. Its resolution ID remains mapped to the local ID; exactly
  one racing late Rust response is discarded, while a duplicate late response
  still fails closed.
- Reverse requests receive separate local IDs; their later
  `serverRequest/resolved.requestId` is mapped back to that same local ID. Both
  always use the control write lane. Interrupts, approval responses,
  terminal/error notifications, and shutdown also use that lane. Eight
  consecutive control frames are followed by an available normal frame,
  preventing starvation.
- Capacity rejection before a request is queued returns a static JSON-RPC error
  and is definitely not applied. Once a local-to-upstream request is queued,
  its timeout, EOF, signal, write ambiguity, child exit, or protocol failure
  closes the epoch; the sidecar never resends it. The reverse-request timeout
  is the request-scoped exception described above. Rust therefore retains the
  authoritative uncertain-mutation decision.

Local stdin EOF, `SIGINT`, `SIGTERM`, or the local `sidecar/shutdown` request
closes upstream stdin, waits a fixed five-second grace, force-kills if needed,
and drains/tears down pipes within the same bound. Upstream stdout EOF or child
exit is always terminal. There is no restart loop in this package. Rust owns
the enclosing POSIX process group or Windows Job object and supplies the final
process-tree cleanup guarantee.

## Adapter policy

`adapters/0.149.0.cjs` and `adapters/0.151.0.cjs` are independently reviewed.
Adding an upstream release requires a new adapter module and Node contract
tests, but no new Rust version DTO or wire module. Both adapters expose only
the promoted thread/read/start/resume, turn/start/steer/interrupt, queue,
notification, dynamic-tool, approval, and reconciliation surface.

Every promoted request, response, notification, and reverse request has an
explicit field projector. Unknown members inside local adapter-domain params
and results are rejected so caller intent is never silently ignored; upstream
unknown fields are stripped. Thread,
Turn, TurnError, ThreadItem, tool, and approval objects are recursively
allowlisted instead of relying on Rust's flatten/raw compatibility fields.
For 0.151.0, `functionCallOutput` is reduced to `{type,id}`, provider
TurnError messages are static, additional/misalignment details are removed,
rate-limit details become a content-free upstream/capacity/retryable
classification, and `writeStdin` command approval requests are rejected
because that operation is not in the stable domain. The four additive
MCP/realtime notification methods are also forced into the 0.151.0 initialize
opt-out list and remain filtered locally as defense in depth.

The unit test suite uses a fake Codex process and covers both supported versions,
bootstrap negotiation, ordinary requests, notifications, reverse requests,
priority writes, pending saturation, exact version rejection, malformed and
oversized frames, correlation reuse, late responses, upstream EOF, signals,
bounded shutdown, and the one-process/no-replay invariant.

Separately, `npm run ci:smoke` is the non-fixture acceptance smoke: it requires
the real lockfile-installed package and exercises the complete local lifecycle
without credentials or a model request. `npm run ci:artifact` is reserved for
CI/release assembly and requires an absolute, nonexistent output path in
`CODEX_SIDECAR_ARTIFACT_DIR`. In GitHub Actions it also publishes the manifest
SHA-256 through `GITHUB_OUTPUT` so the checked-out verifier has an independent
integrity root after download.
