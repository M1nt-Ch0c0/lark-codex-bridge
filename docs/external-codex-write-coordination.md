# External Codex shared-write coordination

Issue [#31](https://github.com/M1nt-Ch0c0/lark-codex-bridge/issues/31)
adds a fail-closed coordinator for mutations and approvals on an
operator-owned Codex app-server. This module is not connected to the ordinary
`run` path. Selecting external mode never falls back to a spawned stdio child,
and a caller without an explicit write profile and coordinator configuration
remains read-only.

## Capability and admission matrix

| Profile | Admitted surface |
| --- | --- |
| `observe_shared` | Bounded list/read only |
| `resume_shared` | Read-only resume and epoch reconciliation |
| `mutate_shared` | Exact owned-turn steer/interrupt and the static approval route; no new turn without a queue-empty proof |
| `queue_shared` | Reconciliation plus start, exact steer/interrupt, queue add/list/start, and approvals |

Every command first acquires a per-thread SQLite fence under the current
connection epoch. `turn/start` requires an idle authoritative thread snapshot
and an empty authoritative queue. Steer and interrupt require the exact active
turn to be owned by the same authorized Lark source, bridge client actor, and
approval actor. Queue add targets that exact owned active turn; queue start
requires an idle thread and the exact durable queued-submission ID. Foreign,
stale, duplicate, mismatched, or unprovable targets are rejected.

## Durable intent and uncertainty

Mutation intents transition through `prepared`, `sent`, and exactly one of
`applied`, `rejected`, or `uncertain`. A duplicate intent never sends again.
Disconnect or timeout after `sent`, a response that cannot be correlated to
the exact client message/turn/queue ID, and protocol drift create durable
uncertainty. That thread remains fenced across later epochs; the bridge never
guesses whether the write happened and never replays it.

The shared notification allowlist is exact and schema-decoded. It includes the
reviewed lifecycle stream needed when another client is attached, queue and
request-resolution notifications, thread settings, and bounded endpoint rate
limits. Unknown methods, malformed known methods, a changed approval reviewer,
duplicate approval delivery, or a mismatched resolution fail closed.

## One approval handler and recipient

Each endpoint has one statically configured bridge approval actor and one
owner-authorized Lark recipient. The ordinary Lark source actor, bridge client
actor, approval actor, and recipient remain distinct durable fields. An
allowed sender can initiate ordinary work but cannot answer an approval unless
the same event also passes the owner-only recipient policy.

Command, file-change, and permissions approval requests retain the original
epoch-bound JSON-RPC response token. Delivery is a single-recipient compare-and-
swap claim. Deadlines are bounded by both local policy and the remote automatic
resolution deadline; timeout or prompt-channel overflow sends a typed default
denial (empty permissions for a permissions request). Completion is not final
until the matching `serverRequest/resolved` notification is durably recorded.
Disconnect while a claim is unresolved makes it uncertain instead of
reassigning or answering twice.

`ExternalWriteCoordinator::reassign_approval_actor` serializes a handler
change with the command stream. It succeeds only when all mutation fences,
owned turns, and approval claims on the endpoint are fully drained. Success
atomically updates the durable fences, orderly closes the old coordinator, and
requires a new coordinator configured with the new actor. A busy or uncertain
endpoint cannot be reassigned.

## Verification

Deterministic SQLite and two-WebSocket fake-server tests cover simultaneous
commands, every mutation type, exact-ID conflicts, disconnect after send,
timeout, no replay, one-recipient approval claims, unauthorized and duplicate
answers, all deadline defaults, protocol mismatch, and drained reassignment:

```bash
cargo test --locked --test external_write_store --test external_write --test runtime_policy
```

The hard-gated smoke starts only the explicitly supplied native Codex 0.149.0
binary with bearer authentication and an isolated local Responses API. It
proves start/steer/interrupt/queue behavior, one approval response, orderly
socket-only shutdown while the server stays healthy, reconnect, and a real
two-client start race. An uncorrelatable race may surface as `Uncertain` when
the competing write disrupts preflight, or `Ambiguous` after the bridge sends
its mutation; either result must become durable uncertainty, with one actual
turn and no model-work replay:

```bash
CODEX_EXTERNAL_WRITE_E2E=1 \
CODEX_EXTERNAL_WRITE_BINARY=/absolute/path/to/native/codex \
CODEX_EXTERNAL_WRITE_EXPECTED_VERSION=0.149.0 \
cargo test --locked --test external_write_smoke \
  real_exact_binary_coordinates_two_clients_queue_exact_ids_and_one_approval_route \
  -- --ignored --exact --nocapture
```

The ignored marker is not acceptance evidence. The exact invocation fails on
every missing or empty gate variable, non-native binary, version/auth mismatch,
skipped selection, replay, approval-routing failure, or non-200 health check.
CI installs exact official 0.149.0 and runs this command on Linux, macOS, and
Windows.
