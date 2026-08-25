# Codex Unix-socket WebSocket contract RFC

Status: transport contract verified; future implementation is viable only under the fail-closed
Linux/macOS policy below. This RFC and its reproduction do not add a Unix backend to the bridge,
and no implementation issue was opened before reaching this decision.

## Decision

Exact `codex-cli 0.149.0` accepts an explicit absolute
`unix:///private/path/app.sock` listener and speaks RFC 6455 HTTP/WebSocket framing over that Unix
stream. It is not JSONL-over-Unix, stdio, or TCP. A future bridge connector is viable on Linux and
macOS when it can enforce the filesystem and peer-identity rules in this RFC.

The current bridge continues to accept only `ws://` and `wss://` external endpoints. It must reject
`unix://` rather than aliasing it to the spawned stdio adapter or to a loopback TCP listener. This
RFC authorizes neither runtime wiring nor a weaker fallback.

Project support is deliberately narrower than platforms that may expose `AF_UNIX`:

| Platform | Project decision | Exact evidence |
| --- | --- | --- |
| Linux (`ubuntu-latest`) | viable for a future guarded connector | CI runs the native 0.149.0 reproduction |
| macOS (`macos-latest`, plus local arm64) | viable for a future guarded connector | CI and the local native 0.149.0 reproduction |
| Windows | unsupported | no Unix runtime gate; portable compilation must remain green |
| Other Unix families | unsupported until separately reproduced | no inference from `AF_UNIX` availability |

The tested version is exact 0.149.0. A later Codex version is unsupported until this reproduction
and the repository's exact schema policy are both rerun and reviewed.

## Committed reproduction

[`tests/codex_unix_socket_smoke.rs`](../tests/codex_unix_socket_smoke.rs) is a raw, bounded,
path-free reproduction. It uses a short owner-only directory under `/tmp`, starts the explicitly
selected native binary with `--listen unix://ABSOLUTE_PATH`, opens a `UnixStream`, and writes this
HTTP request directly:

```http
GET / HTTP/1.1
Host: localhost
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==
Sec-WebSocket-Version: 13
```

Acceptance requires exact HTTP `101 Switching Protocols`, `Upgrade`, `Connection`, and the RFC
6455 `Sec-WebSocket-Accept` derived from the fixed public key. A second Unix connection writes a
complete JSONL `initialize` record and half-closes its write side. Exact 0.149.0 closes it without
emitting stream bytes. These positive and negative observations distinguish the endpoint from
JSONL or a raw byte stream.

Every process start, collision rejection, connect, write, read, and shutdown has a fixed deadline.
HTTP evidence is capped at 4 KiB. Child stdout/stderr are discarded; output contains only platform,
architecture, exact version, boolean policy results, mode `0600`, framing labels, and whether
graceful shutdown removed the socket or left a verified stale inode that the next server replaced.
It never prints the temporary path, inode, UID/GID/PID values, Codex home, environment, RPC
contents, or account data.

Run it only by its exact name:

```bash
CODEX_UNIX_WS_E2E=1 \
CODEX_UNIX_WS_BINARY=/absolute/path/to/native/codex \
CODEX_UNIX_WS_EXPECTED_VERSION=0.149.0 \
cargo test --locked --test codex_unix_socket_smoke \
  real_exact_binary_exposes_websocket_framing_and_safe_unix_socket_boundaries \
  -- --ignored --exact --nocapture
```

The ignored marker keeps ordinary builds independent of an installed Codex binary. It is not a
pass. Linux and macOS CI install the official exact package and invoke the test explicitly. A
missing/empty gate, launcher script in place of the native executable, version mismatch, absent
socket support, filtered test, timeout, or skipped invocation is a hard failure in that gate.

## Filesystem policy

A future implementation and its operator-owned launcher must enforce all of these rules:

- Use an absolute, explicitly configured socket path inside a dedicated directory owned by the
  service UID and mode `0700`. Every operator-controlled path component must be non-symlink and
  owner-only; a public parent such as `/tmp` is acceptable only when it contains that private
  directory.
- Before connecting, use `lstat`, require a socket rather than a symlink or regular file, require
  the expected owner UID, and require exact mode `0600`. Group is recorded for diagnostics but is
  not an authority: group and other bits must remain zero, and deployments must not widen them.
- After connecting, repeat `lstat` and require the same device/inode pair. Then obtain kernel peer
  credentials and require the expected service UID and the known app-server PID. Peer GID is
  observed but cannot replace the UID/PID checks.
- The bridge client never creates, replaces, chmods, chowns, or unlinks the listener. Listener and
  directory lifecycle belong to the operator-owned Codex process/launcher.

The reproduction verifies that exact 0.149.0 creates a socket owned like its private parent with
mode `0600`. It also verifies the connected peer UID and PID, revalidates the inode after connect,
and records that peer GID was available without exposing its value.

## Collision, stale-socket, cleanup, and race policy

The exact reproduction covers four distinct path states:

- a pre-existing regular file is rejected and its bytes remain unchanged;
- a pre-existing symbolic link is rejected and neither link nor target is modified;
- a second server is rejected while a live listener owns the path;
- a dead/stale socket inode is replaced when created as a fixture, after an abnormal app-server
  kill, and when a graceful termination leaves the inode behind.

Exact 0.149.0 does not promise that graceful `SIGTERM` removes the owned socket: repeated local
runs observed both removal and a dead socket inode. The reproduction accepts the latter only after
the child is reaped, the inode is unchanged, and a bounded connect proves that no listener remains;
it then requires a fresh exact server to replace that inode and complete another raw Upgrade.
Abnormal death can likewise leave a stale socket. This observed replacement is acceptable only
inside the private operator-owned directory and does not authorize the bridge client to perform
cleanup. Regular files, links, unexpected owners/modes, a live peer, changed inode, missing peer
credentials, or a peer UID/PID mismatch are terminal policy failures—not stale-socket candidates.

The private directory, pre/post-connect inode comparison, and kernel peer credentials close the
path-substitution window that pathname checks alone leave open. Any future connector that cannot
perform all three checks must keep Unix transport disabled.

## Remaining implementation boundary

The transport contract is viable, but the bridge has no Unix connector today. A future change
would need a distinct configured endpoint type, a WebSocket client handshake over `UnixStream`,
the complete identity/race policy above, bounded close semantics, and the same exact-version
capability gate used by TCP WebSocket mode. Until that work is explicitly scoped, `unix://` remains
rejected and must never fall back to JSONL, stdio, or TCP.
