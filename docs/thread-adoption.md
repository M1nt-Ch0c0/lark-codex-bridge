# Persisted-thread adoption capability

## Current decision

Persisted-thread discovery, adoption, and release are disabled with the stable
classification `unavailable_no_reliable_writer_release`.

This is a safety decision, not an inference from `thread/list`, `thread/read`,
turn status, or timestamps. Those read-only observations cannot prove that a
Desktop, CLI, or another app-server has relinquished its single-writer claim.
The bridge therefore does not expose a candidate page, call `thread/resume`,
create or replace a scope mapping, retry a conflict, kill another client, or
modify the external thread lifecycle.

Run the path-free diagnostic without starting Codex or reading a profile:

```console
$ lark-codex-bridge codex adoption-status
{"available":false,"classification":"unavailable_no_reliable_writer_release",...}
```

The Feishu command grammar reserves three explicit controls:

- `/threads [cursor]` for a bounded, paginated candidate page;
- `/adopt <selector> --handoff-complete` for explicit selection and handoff
  acknowledgement;
- `/release` for a local release that must never delete, archive, rename, or
  otherwise change an external thread globally.

The runtime slash-command handler is not wired in this alpha. More importantly,
the dependency-free `ThreadAdoptionGate` rejects all three operations before a
handler can receive an app-server client, store handle, workspace, scope, or
thread ID. The reserved cursor and selector inputs are byte-bounded and their
`Debug` representations disclose lengths only. Candidate result count and wire
budgets are reserved in `src/limits.rs`; no result is emitted while disabled.

## Negative interoperability observation

On 2026-08-23, a two-process experiment used exact `codex-cli 0.149.0` and an
isolated temporary profile:

1. app-server A resumed a persisted thread and acquired its writer;
2. A completed a turn and sent `thread/unsubscribe`;
3. while A remained alive, app-server B still could not resume the thread
   because Codex reported that the thread already had an active writer;
4. only exiting app-server A released the writer so B could resume it.

This proves that unsubscribe is not a writer-release primitive for that exact
version. The bridge's `AppServerClient::release_thread` is also only a local
route/projection release and makes no remote ownership claim. There is no
positive sequential-handoff end-to-end result to report.

Because the supported version set has no typed, verified remote release
operation, the bridge applies the conservative decision to the whole feature.
An active-writer conflict remains a refusal, never a takeover signal.

## Conditions for enabling

Enabling the gate requires all of the following in a reviewed versioned
contract and real cross-platform evidence:

1. authoritative acquisition and release operations with stable success,
   conflict, missing, archived, and uncertain classifications;
2. a sequential handoff proving other client release, bridge adoption, history
   continuation, bridge release, and another client reacquisition;
3. owner-only command authorization and serialization with the scope actor,
   including refusal during starting, running, or uncertain turns;
4. same-profile and revalidated workspace checks immediately before acquisition;
5. cross-scope uniqueness plus an atomic mapping that distinguishes
   `bridge_created` from `externally_adopted`;
6. bounded and redacted candidate projection whose idle status is explicitly
   labelled ownership-unverified;
7. restart, crash, disappearance, and failed-resume recovery rules that never
   leave a half-written mapping or silently create a replacement thread.
8. Issue #3 context handles remain bound to their original Feishu scope and
   authoritative Codex thread/turn across adoption and release; adoption must
   never broaden the messages, media, or context that a handle is allowed to
   read.

Connecting multiple clients to one shared app-server endpoint is a different
ownership model and remains the subject of Issue #8.
