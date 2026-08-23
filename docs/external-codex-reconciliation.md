# External Codex epoch reconciliation

Issue [#30](https://github.com/M1nt-Ch0c0/lark-codex-bridge/issues/30)
adds durable, read-only recovery for operator-owned Codex app-servers. It does
not connect external mode to the mutation-driven `run` path and does not admit
new work; shared-write admission remains a separate #31 policy boundary.

## Ownership and recovery sequence

`ExternalRecoveryCoordinator` owns one authenticated WebSocket epoch at a time
and never owns a process, PID, command, restart, signal, or server configuration.
Every connection attempt first reserves a monotonically increasing SQLite
epoch. Older responses and notifications then fail the same durable epoch
fence and cannot change current state.

For each explicitly managed, non-uncertain thread, an epoch performs this
order:

1. mark the thread `reconciling` under the current epoch fence;
2. call `thread/resume` with `excludeTurns: true` before any authoritative read;
3. call `thread/read` with `includeTurns: false`;
4. page `thread/turns/list`, then page `thread/items/list` per observed turn;
5. fold buffered terminal notifications and snapshot terminals by stable turn
   and item IDs;
6. atomically persist the terminal projection and mark the thread `ready`.

The exact 0.149.0 paginated item API is store-dependent. Threads intended for
this recovery profile must use paginated history; a server rejection fails
closed and never falls back to an unbounded embedded-history read.

## Bounds and uncertainty

The coordinator manages at most 64 threads per endpoint. A reconciliation uses
pages of 100, at most 32 turn pages and 32 total item pages per thread, at most
3,200 retained turn/item entries, 32 MiB of snapshot bytes per thread, and
64 MiB per endpoint epoch. Notifications are buffered at the existing bounded
thread-event capacity and mailbox byte budget. Reconnect backoff starts at
500 ms and caps at 30 seconds.

Socket disconnect, request timeout, bridge restart, operator-announced server
restart, page exhaustion, buffer overflow, protocol violation, and conflicting
terminal status have explicit persisted reasons. Disconnect-class failures
make the endpoint and its active threads `unavailable` until a new epoch
reconciles them. A per-thread overflow, page limit, or conflicting terminal
makes that thread durably `uncertain`; later epochs leave it inert rather than
inventing completion.

The bridge never replays an uncertain `thread/start`, `turn/start`, steer,
queue, interrupt, or approval request. The production recovery module has no
API for those methods. An operator-controlled server restart is announced with
`note_operator_server_restart`; that call records unavailability and fences the
socket, but does not stop or start the server process.

## Verification

Deterministic fake-server and SQLite tests cover disconnect at every recovery
request boundary, durable epoch advancement, stale-event fencing, stable-ID
deduplication, persistent conflict uncertainty, notification overflow, and
pagination limits:

```bash
cargo test --locked --test external_recovery --test external_reconciliation_store
```

The real smoke starts only the explicitly supplied exact binary, lets a
separate operator harness create one paginated thread and one turn, adopts that
thread, and blocks that turn against an isolated local Responses endpoint. The
harness does not restart the server until `turn/interrupt` has produced both its
correlated response and matching `turn/completed`, the interrupted status is
readable through `thread/turns/list`, and the model HTTP connection is released.
It then forces a bridge socket reconnect, proves the old TCP listener is gone,
starts and health-checks the replacement operator-owned server, and proves that
resume/read reconciliation retained exactly one thread and turn. Coordinator
shutdown must leave the server healthy; only the harness stops its child:

```bash
CODEX_EXTERNAL_RECONCILIATION_E2E=1 \
CODEX_EXTERNAL_RECONCILIATION_BINARY=/absolute/path/to/native/codex \
CODEX_EXTERNAL_RECONCILIATION_EXPECTED_VERSION=0.149.0 \
cargo test --locked --test external_recovery_smoke \
  real_exact_binary_reconciles_across_socket_and_operator_server_restarts_without_write_replay \
  -- --ignored --exact --nocapture
```

The ignored marker keeps ordinary builds independent of an installed Codex
binary and is not acceptance evidence. The exact invocation fails on every
missing or empty gate variable, skipped selection, version mismatch, recovery
failure, replayed work, unexpected server exit, or non-200 health result. CI
installs official exact 0.149.0 and invokes this test by exact name on Linux,
macOS, and Windows. The binary variable must point to the platform's native
Codex executable inside the npm package, not its `.bin` Node/command launcher:
killing only that launcher can orphan the real server and invalidate restart
evidence.
