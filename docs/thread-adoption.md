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

## Negative interoperability observations

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

On 2026-08-31, the same bounded ownership check was repeated with exact
`codex-cli 0.151.0`, an isolated synthetic profile, and a local scripted model
provider. Both the stable and experimental generated method sets exposed
`thread/unsubscribe` as the only method matching release, unsubscribe, close,
or writer ownership. App-server A created one completed persisted turn and held
the writer. App-server B received the redacted active-writer conflict before A
unsubscribed, immediately afterward, and again after a five-second bound. Only
after A exited cleanly could B resume the same thread and read its completed
history. The temporary profile was then removed. No existing profile,
credential, thread identifier, message, path, or model output was used as
evidence.

The [rolling App Server documentation](https://developers.openai.com/codex/app-server)
describes unsubscribe as removing that connection's subscription; a thread
with no subscribers and no activity may be unloaded after a 30-minute grace.
Eventual inactivity-based unload is neither an atomic writer transfer nor a
release acknowledgement, so it cannot make a bounded sequential handoff safe.

Because the exact 0.149.0 and 0.151.0 supported versions have no typed, verified remote release
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
