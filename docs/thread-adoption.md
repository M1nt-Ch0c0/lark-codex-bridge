# Persisted-thread adoption

## Availability and safety invariant

Persisted-thread adoption is available on **Linux and macOS only**, for the two
local, process-owning backends:

- `spawned_stdio`;
- `protocol_sidecar`, when the bridge owns both the sidecar and its Codex child.

It remains unavailable for `external_endpoint`. A socket-only client cannot
authoritatively terminate and reap the remote app-server process, so that
shared ownership model remains the subject of
[Issue #8](https://github.com/M1nt-Ch0c0/lark-codex-bridge/issues/8).

It also fails closed on Windows. `process-wrap` still places ordinary bridge
children in a Job object, but waiting for its child wrapper does not prove the
Job reached `ACTIVE_PROCESS_ZERO`; that wait is therefore not accepted as an
Issue #4 writer-release authority. Both the public capability gate and the
dedicated production launcher enforce this platform boundary. This does not
disable either managed backend for ordinary, non-adoption bridge traffic.

The invariant is:

> one externally adopted persisted thread has exactly one dedicated,
> non-restarting bridge-owned app-server process group; successful
> `thread/resume` acquires its writer and confirmed process-group absence
> releases it.

The managed app-server/sidecar contract forbids daemonizing or escaping the
owned process group (for example with `setsid`). The release proof covers that
owned group; it does not claim authority over a deliberately escaped process.

`thread/unsubscribe`, closing a local subscription, dropping a route, an idle
runtime status, and an old timestamp are not writer-release evidence. The
bridge never retries a writer conflict as a takeover, never kills another
client, and never creates a replacement thread to hide a failed adoption.

The path-free diagnostic does not start Codex or read any profile:

```console
$ lark-codex-bridge codex adoption-status
{"available":true,"classification":"available_dedicated_process_ownership","releaseAuthority":"dedicated_process_tree_reap","managedBackends":["spawned_stdio","protocol_sidecar"],"supportedPlatforms":["linux","macos"],"externalEndpoint":{"available":false,"classification":"unavailable_shared_external_endpoint",...},...}
```

On Windows the same static command reports `available: false`,
`classification: "unavailable_platform_process_tree_proof"`, and
`releaseAuthority: null` without starting Codex or loading a profile.

## Owner commands

The explicit control grammar is:

- `/threads [cursor]` — list one bounded page of candidates;
- `/adopt <selector> --handoff-complete` — acknowledge that every other
  Desktop, CLI, or app-server has been closed, then try to acquire that exact
  thread;
- `/release` — release the bridge-owned process domain without deleting,
  archiving, renaming, or forking the persisted thread.

Only an authorized owner can invoke these controls. They enter the same scope
actor mailbox as ordinary messages, so adoption and release cannot overtake a
starting, running, or uncertain turn. Recognized control text is never sent to
Codex as a prompt. A malformed recognized control receives a bounded static
reply.

Discovery is preflight, not acquisition. `thread/list` requests newest updated
non-archived records in pages of at most 20. The cursor is limited to 512 bytes,
the selector to 128 bytes, and the serialized result to 16 KiB. Ephemeral,
non-idle, denied-workspace, already mapped, and already reserved records are
omitted. A result exposes only a bounded title, a one-way workspace alias, a
sanitized source, update time, selector, and
`ownership: "unverified"`; it does not expose preview/history or a raw path.

## Dedicated ownership flow

Bridge-created threads continue to use the ordinary supervised singleton. The
singleton may perform bounded candidate discovery because listing and reading
do not acquire the writer. It is never used to resume or write an externally
adopted thread.

An adoption performs these steps in the scope actor's serialized control turn:

1. authorize the owner and require `--handoff-complete`;
2. require a managed stdio or managed sidecar backend;
3. require the scope to have no starting, running, or uncertain turn;
4. reserve the exact selector in the durable adoption saga without changing
   the current scope mapping;
5. start one non-restarting app-server ownership domain from the configured
   backend and compare its initialized profile identity with the discovery
   client;
6. require a bounded active-discovery proof for the exact selector, then
   freshly `thread/read` the exact target and recheck persisted state,
   selector, workspace policy, and bridge-wide uniqueness; `thread/read`
   status alone is not treated as archive visibility;
7. call the version-gated typed `thread/resume` exactly once;
8. only after resume succeeds, atomically store the canonical workspace,
   policy fingerprint, `origin = externally_adopted`, and adoption generation;
9. route later messages only through that exact dedicated client.

An active-writer conflict is a typed terminal acquisition refusal. The bridge
reaps only the process tree it just created, terminalizes the failed
reservation after confirmed cleanup, leaves the old mapping intact, and tells
the owner to close the other client. It does not retry, kill, or write a partial
mapping. Missing, archived, workspace-denied, profile-mismatched, and generic
resume failures follow the same no-mapping rule. If cleanup cannot be proved,
the saga is fenced as `recovery_required` instead of being declared free.

For an `externally_adopted` mapping, absence of the matching live ownership
domain is an error. Message routing must not fall back to the singleton,
`thread/start`, or an implicit resume.

Release of a committed adoption is also serialized in the scope actor:

1. persist `releasing` while the mapping remains active;
2. remove the domain from message routing;
3. terminate the dedicated owned process group and prove the group absent;
4. only after confirmed reap, remove the external mapping and terminalize the
   generation as `released`.

A startup crash can instead leave a pre-commit generation fenced while the
scope still has its prior `bridge_created` mapping, or no mapping at all.
Explicit `/release` recovery probes only the saga's exact thread and
generation, confirms the recovery owner reaped, and terminalizes that saga as
`acquisition_failed`. No external mapping was ever committed in this branch,
so recovery removes no mapping and leaves any prior bridge-created mapping
active. The typed release receipt and owner reply distinguish this cleanup
from `released`. Replaying `/release` after reply-delivery interruption is
idempotent only for that exact terminal saga together with either no active
mapping or a distinct generation-free `bridge_created` mapping.

Failed cleanup retains both the mapping and a `release_failed` fence. Release
never calls archive/delete/rename/fork and never treats unsubscribe as writer
release.

On Linux and macOS, the owned wrapper first signals and waits for its process
group. The bridge then uses a side-effect-free signal-0 group probe under the
same absolute force-cleanup deadline. Only `ESRCH` confirms that the group is
empty; success and `EPERM` mean it still exists, while every other OS error
fails closed. Sidecar bootstrap failures use the same proof when a leader PID
is available. A missing PID is never reported as confirmed cleanup. An
unconfirmed bootstrap cleanup remains a typed supervisor failure, so a pending
adoption generation is fenced instead of terminalized as freely retryable.

## Durable state and recovery matrix

| Durable state or event | Scope mapping | Writer assumption | Required action |
| --- | --- | --- | --- |
| `acquiring` before resume | unchanged | not acquired | finish validation and one resume attempt |
| conflict, missing, archived, or rejected resume with confirmed cleanup | unchanged | not owned by bridge | terminal `acquisition_failed`; owner may correct the cause and issue a new explicit command |
| resume succeeded, commit pending | unchanged until the atomic commit | bridge process may own writer | commit immediately; on any failure reap, otherwise fence uncertainty |
| pre-commit `recovery_required` + confirmed recovery reap | unchanged; prior `bridge_created` mapping remains active if present | recovery owner was reaped; no external mapping was committed | terminal `acquisition_failed`; report uncommitted acquisition cleanup, not mapping removal; exact replay is idempotent |
| `owned` | active `externally_adopted` generation | dedicated domain is sole bridge writer | route only through that domain |
| `releasing` | retained and unroutable | ownership may still exist | finish process-tree termination and reap |
| confirmed reap of committed adoption | removed | released | terminal `released`; another client may resume |
| `release_failed` | retained and unroutable | uncertain | explicit recovery/release retry; never route or auto-unmap |
| unexpected domain exit | retained and fenced | release is not inferred from an observation alone | `recovery_required`; require an explicit recovery decision |
| bridge shutdown while owned | retained and fenced before shutdown | bridge then reaps its owned tree | keep `recovery_required`; do not silently convert shutdown into `/release` intent |
| bridge startup | all non-terminal generations fenced before message routing | unknown | reconcile the exact generation; never start a replacement thread |
| recovery resume reports active writer | retained and fenced | another owner may exist | fail closed; never terminate that owner |

Generation compare-and-swap checks reject stale callbacks. A global uniqueness
constraint prevents two Feishu scopes from reserving or mapping the same Codex
thread. `bridge_created` rows have no adoption generation;
`externally_adopted` rows must have one.

The v11 migration applies that uniqueness rule fail closed. If a pre-v11 store
contains the same active Codex thread ID in more than one scope, every active
row in that ambiguous set is archived atomically before the unique index is
created. The migration preserves the rows as history but selects no owner and
creates no adoption saga; restoring access therefore requires a new explicit
`/threads` selection after the operator has verified the intended handoff.

## Context and shared-endpoint boundaries

Adoption changes the Codex history routed for future messages; it does not
broaden Feishu authorization. Every Issue #3 `bridge_context` handle remains
bound to the Feishu scope and authoritative turn that created it. A persisted
thread's historical tool metadata is not authority to read old messages,
media, quotes, or another scope. If a dedicated adopted connection cannot
install or validate the current scope-bound dynamic tools, the request fails
closed.

This feature is sequential handoff only. It does not connect to another
operator's live app-server, inject into an active Desktop/CLI session, or permit
two app-servers to write concurrently. Those are Issue #8 concerns, and
`external_endpoint` therefore reports `unavailable_shared_external_endpoint`.

## Real sequential-handoff evidence

There are two deliberately separate evidence layers. The Node harness proves
upstream persisted-thread interoperability and the archived-target contract; it
does **not** instantiate the bridge Router and is not production Router evidence.
The ignored Rust smoke exercises the production Router, Supervisor, Store, and
durable outbox boundary.

### Upstream interoperability and archived-target audit

The upstream interoperability harness is
[`scripts/thread-adoption-handoff-e2e.cjs`](../scripts/thread-adoption-handoff-e2e.cjs).
It creates a random temporary `CODEX_HOME`, replaces `HOME` with another
temporary directory, binds a loopback-only scripted Responses provider, sends
no credentials to the child, applies strict RPC/turn/process timeouts, discards
bounded child stderr, and removes the temporary tree before printing. Its
output schema contains no thread ID, path, prompt, marker, provider body, or
model output.

Run its offline contract self-test in ordinary CI:

```console
$ node scripts/thread-adoption-handoff-e2e.cjs --self-test
{"schema":"lark-codex-bridge/thread-adoption-handoff-e2e/v1","result":"self-test-pass","redactedOutputContract":true}
```

Run the real upstream POSIX audit only with an exact reviewed binary:

```console
$ node scripts/thread-adoption-handoff-e2e.cjs \
    --binary /absolute/path/to/codex \
    --expected-version 0.149.0
```

The real mode rejects Windows for the same product boundary: neither the Node
harness nor the production launcher has an `ACTIVE_PROCESS_ZERO` proof. A Job
object remains useful containment for ordinary bridge children, but its child
wrapper wait is not falsely reported as real sequential-handoff or release
evidence.

### Production Router A→B→C smoke

[`tests/thread_adoption_handoff_smoke.rs`](../tests/thread_adoption_handoff_smoke.rs)
is the production-path evidence. Independent raw stdio owner A creates the
persisted thread and first marker, then its POSIX process group is explicitly
reaped. Owner B is the public production `Supervisor` + `Router` + file-backed
`Store` + `OutboxReplySink`: an authenticated Feishu owner runs `/threads`,
copies the exact rendered `/adopt … --handoff-complete`, sends the second marker,
and runs `/release`. The smoke checks the exact thread ID, external origin and
generation, provider-visible prior history, the pre-release mapping, and the
terminal `released` saga after the durable cleanup reply. Independent raw owner
C then resumes and reads the same two-turn history while the Router and shared
supervisor are still alive, proving Router shutdown did not make the target
available.

The test is ignored and fail-closed unless every explicit gate is present. For
Codex 0.149.0, `CODEX_THREAD_ADOPTION_ROUTER_BACKEND=spawned-stdio` exercises the
native production backend. For Codex 0.151.0, the only accepted mapping is
`CODEX_THREAD_ADOPTION_ROUTER_BACKEND=protocol-sidecar`, together with absolute
`CODEX_THREAD_ADOPTION_ROUTER_NODE_BINARY` and
`CODEX_THREAD_ADOPTION_ROUTER_SIDECAR_ENTRYPOINT` paths. Both use the exact
native binary for independent owners A and C and an isolated `CODEX_HOME` with a
loopback Responses provider. The fixed invocation tail is:

```console
$ CODEX_THREAD_ADOPTION_ROUTER_E2E=1 \
    CODEX_THREAD_ADOPTION_ROUTER_BINARY=/absolute/path/to/codex \
    CODEX_THREAD_ADOPTION_ROUTER_EXPECTED_VERSION=0.149.0 \
    CODEX_THREAD_ADOPTION_ROUTER_BACKEND=spawned-stdio \
    cargo test --locked --test thread_adoption_handoff_smoke \
      real_exact_binary_routes_sequential_adoption_without_replacement \
      -- --ignored --exact --nocapture
```

The `thread-adoption-handoff` CI matrix resolves native executables from exact
official npm installs and runs both evidence layers on Ubuntu and macOS. Its
four required cells are Codex 0.149.0 through production spawned stdio and Codex
0.151.0 through the production protocol sidecar, on each OS. The Node steps are
labelled upstream interoperability/archived-resume audit; only the Rust step is
labelled production Router sequential-handoff smoke. Windows does not run this
process-group evidence.

On 2026-08-31 the exact installed `codex-cli 0.149.0` passed on Darwin arm64:

```json
{"schema":"lark-codex-bridge/thread-adoption-handoff-e2e/v1","result":"pass","codexVersion":"0.149.0","platform":"darwin-arm64","transport":"managed_stdio","isolatedProfile":true,"localScriptedProvider":true,"explicitSequentialOwners":3,"completedTurns":2,"successfulHandoffs":2,"preResumeReadStatusAtOwnerB":"notLoaded","historyVisibleAfterFirstHandoff":true,"historyVisibleAfterSecondHandoff":true,"processTreesReaped":3,"providerObservedPriorHistoryOnContinuation":true,"temporaryDataRemoved":true}
```

This historical JSON is the direct Node interoperability sequence: owner A
created a persisted thread and one marker turn; A's owned process group was
confirmed absent; independent owner B resumed the same opaque ID, read A's
history, and appended a second marker turn; B's owned group was confirmed
absent; independent owner C resumed and read both completed turns, then exited
and was reaped. The provider also observed prior history in B's continuation request.
It complements, but does not replace, the production Router smoke above.

The same exact binary also passed the archived-target audit:

```console
$ node scripts/thread-adoption-handoff-e2e.cjs \
    --audit-archived-resume \
    --binary /absolute/path/to/codex \
    --expected-version 0.149.0
```

```json
{"schema":"lark-codex-bridge/thread-adoption-handoff-e2e/v1","result":"pass","audit":"archived_resume","codexVersion":"0.149.0","platform":"darwin-arm64","transport":"managed_stdio","isolatedProfile":true,"activeExactIdSearchMatchedBeforeArchive":false,"activeCwdFilterMatchedBeforeArchive":true,"activeExactIdAndCwdMatchedBeforeArchive":false,"activeExactIdSearchMatchedAfterArchive":false,"archivedExactIdSearchMatchedAfterArchive":false,"activeCwdFilterMatchedAfterArchive":false,"archivedCwdFilterMatchedAfterArchive":true,"archivedExactIdAndCwdMatchedAfterArchive":false,"archiveConfirmedBeforeRead":true,"archivedReadSucceeded":true,"archivedReadReportedArchived":false,"archivedReadStatus":"notLoaded","activeAfterRead":false,"archivedAfterRead":true,"archivedResumeRefused":true,"activeAfterResume":false,"archivedAfterResume":true,"processTreesReaped":3,"temporaryDataRemoved":true}
```

This audit proves three distinct 0.149.0 contract facts without exposing the
target ID or cwd. First, `searchTerm` does not find an exact thread ID in
either the active or archived list; the schema defines it as an extracted-title
substring filter. Second, exact `cwd` narrows the active and archived lists to
the workspace but does not identify one target. Third, both a normal persisted
target and an archived target can be reported as `notLoaded` by a fresh
owner's `thread/read`, while the archived target's `thread/resume` is refused
and leaves it archived. Consequently, neither `notLoaded`, `searchTerm`, nor a
cwd-only list is accepted as an exact active-target proof. The archived audit
is a required command in every POSIX version/platform CI cell.

No exact 0.151.0 result is claimed here. The locally available 0.151 binary was
a prerelease and was deliberately rejected as evidence; fetching the exact npm
artifact did not complete, so the committed positive result is limited to the
machine/version above. The same command can add platform/version evidence when
an exact reviewed binary is available.

The rolling
[OpenAI App Server documentation](https://developers.openai.com/codex/app-server)
defines `thread/resume` as reopening an existing thread so later `turn/start`
calls append to it, and `thread/read` as reading stored history without
resuming. Repository evidence, rather than an inference from those read-only
methods, establishes the process-exit handoff boundary used here.
