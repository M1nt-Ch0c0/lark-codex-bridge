# ADR: Codex protocol sidecar wire v1

- Status: Implemented contract
- Date: 2026-08-31
- Tracking issue: #52
- Protocol name: `codex-sidecar-wire`
- Protocol version: `1`
- Exact upstream adapters: `0.149.0`, `0.151.0`

## Purpose and source of truth

The bridge can run Codex through a supervised Node sidecar selected by
`[codex.backend].mode = "protocol_sidecar"`. The sidecar owns exact upstream
version selection and a promoted-method boundary. Rust continues to own the
durable and policy-bearing bridge runtime.

This document records the wire that is implemented now. The bootstrap structs
and validation in `src/codex/sidecar.rs` are the source of truth. In particular,
v1 does **not** put a boot ID, Rust supervisor epoch, schema hash, sequence
number, domain prefix, or nested configuration object on the wire. Those would
be protocol changes rather than implied fields.

## Ownership

| Concern | Rust bridge | Codex sidecar |
| --- | --- | --- |
| Durable inbox/outbox, receipts, checkpoints, reconciliation | Owns | Does not persist |
| Authorization, workspace policy, routing, rendering | Owns | Does not decide |
| Rust RPC epoch and mutation retry policy | Owns | Does not receive the epoch |
| Sidecar/Codex process-tree supervision | Owns outer boundary | Starts and terminates its direct Codex child |
| Exact Codex version probe and adapter selection | Consumes result | Owns |
| Local/upstream correlation remapping | Consumes local IDs | Owns volatile maps |
| Promoted-method allowlist and provider error redaction | Consumes stable result | Owns |

The sidecar keeps only bounded, process-local correlation and queue state. A
sidecar restart discards all of it. It does not write bridge state or approval
decisions to disk.

## Topology

```text
Rust supervisor
  `-- Node sidecar
        `-- codex app-server --listen stdio://
```

Rust launches the sidecar with piped stdin/stdout/stderr. On POSIX it wraps the
sidecar as the leader of an owned process group. On Windows it places the
sidecar in an owned Job object. The sidecar starts Codex directly with
`shell: false`; the Codex child remains inside the outer ownership boundary.

Sidecar stdout is NDJSON protocol traffic. Sidecar stderr contains only a
static classification such as `codex_sidecar_failure code=request_timeout`.
Codex stderr is drained and discarded rather than forwarded.

## Bootstrap

Bootstrap is a three-frame exchange:

1. sidecar to Rust: `hello`;
2. Rust to sidecar: `configure`;
3. sidecar to Rust: correlated `response`.

These three frames are bootstrap objects, not data-plane RPC messages. Each is
one UTF-8 JSON object followed by `\n`. Rust rejects unknown fields in `hello`,
the configure response, and its nested `data` object.

The whole Rust bootstrap operation is bounded by 15 seconds. That bound covers
reading `hello`, writing `configure`, the sidecar's exact version probe and
Codex child spawn, and reading the configure response. After bootstrap, the
supervisor separately performs the normal app-server `initialize` handshake
before publishing `Ready`.

### Hello

The exact v1 hello shape is:

```json
{
  "protocol": "codex-sidecar-wire",
  "v": 1,
  "type": "hello",
  "maxFrameBytes": 33554432,
  "capabilities": [
    "bounded-ndjson",
    "correlated-requests",
    "correlated-server-requests",
    "epoch-on-restart",
    "no-mutation-replay",
    "priority-control-lane",
    "stable-domain-jsonrpc"
  ]
}
```

Rust requires all fields above, exact protocol/version/type values, an exact
`maxFrameBytes` match, and exact set equality for the seven capabilities.
Missing, additional, or duplicate capability entries fail closed. Capability
order is not significant.

There is no `bootId`, `protocolVersions`, adapter list, schema hash,
`maxPending`, or nested `limits` object in `hello`.

### Configure

Rust writes this flat object:

```json
{
  "v": 1,
  "type": "configure",
  "id": "configure-4e0a7d7e-49a5-4fbd-bbed-d307c99813ac",
  "codexBinary": null,
  "codexHome": null,
  "codexArguments": [],
  "maxFrameBytes": 33554432,
  "maxPending": 448
}
```

`id` is `configure-` plus a UUID and is used only to correlate this bootstrap
response. `codexBinary: null` selects the sidecar package's exactly pinned
`@openai/codex` 0.151.0; an explicit reviewed 0.149.0/0.151.0 override is
serialized as a path/string.
`codexHome` is either a configured path or JSON `null`.
`codexArguments` contains at most eight non-empty strings, each at most 1,024
bytes and without NUL. The arguments are intended for a reviewed wrapper or
test fixture and are not a secret transport.

The configure object has no `protocol`, epoch, capabilities, shutdown deadline,
adapter selector, or nested `codex`/`limits` objects. In the production config
path, frame and pending limits use the compiled defaults and are not
operator-tunable.

### Configure response

For a successful exact `0.151.0` selection the shape is:

```json
{
  "v": 1,
  "type": "response",
  "id": "configure-4e0a7d7e-49a5-4fbd-bbed-d307c99813ac",
  "ok": true,
  "data": {
    "upstreamVersion": "0.151.0",
    "adapterVersion": "0.151.0",
    "capabilities": [
      "bounded-ndjson",
      "correlated-requests",
      "correlated-server-requests",
      "epoch-on-restart",
      "no-mutation-replay",
      "priority-control-lane",
      "stable-domain-jsonrpc"
    ]
  }
}
```

Rust accepts success only when:

- `v`, `type`, and `id` match the configure request;
- `ok` is `true`, `data` is present, and `error` is absent;
- `adapterVersion` exactly equals `upstreamVersion`;
- the capability list has exact set equality with the same seven v1 entries;
- the upstream version is exact stable semver without prerelease/build data;
- the version is exactly `0.149.0` or `0.151.0`.

A bootstrap failure may use the same outer shape with `ok: false` and one
closed, static `error` code. Rust accepts only the reviewed configuration,
package, exact-version, probe, and spawn classifications; an unknown code is a
protocol violation. Resource-pressure spawn errors, probe I/O, and probe
timeout use distinct retryable codes and enter supervisor backoff. Invalid
configuration, a missing pinned artifact, deterministic launch/probe failure,
oversized or unsupported version output, malformed protocol, and local codec
failure are permanent. Raw OS, version-probe, provider, and path text is never
exposed.

The response means that the exact adapter was selected and one Codex child was
spawned. It is not the supervisor's `Ready` state. Rust still sends
`initialize`, validates the stable response, sends `initialized`, and only then
publishes `Ready`.

## Exact adapters

The sidecar accepts only version output matching one of:

```text
codex-cli 0.149.0
codex-cli 0.151.0
```

The version probe has its own five-second bound and 4,096-byte output bound.
Ranges, prereleases, build metadata, `0.150.0`, and newer unreviewed versions
fail closed. The two exact releases have separate checked-in adapter modules.

The current v1 adapters are promoted-method allowlists with recursive,
field-by-field projection into the Rust stable domain. They do not rely on
Rust's permissive flatten/raw compatibility fields to strip provider data.
They do not advertise or validate a schema hash, and they do not send an
adapter manifest during hello. Adding those features would require code and a
documented wire revision.

Rust uses `WireAdapter::SidecarV1`, so it does not compile a Codex `0.151.0`
wire module. Rust serializes its stable domain types on the local side and
deserializes promoted results/notifications back into those types.

## Data plane

After bootstrap, Rust hands the same stdio pipes to the existing bounded stream
transport and RPC broker. The envelope is the Codex app-server JSONL RPC subset:

```json
{"id":1,"method":"thread/list","params":{"limit":1}}
{"id":1,"result":{"data":[]}}
{"method":"thread/status/changed","params":{"threadId":"...","status":{}}}
```

The current envelope omits a `jsonrpc: "2.0"` member. Requests contain `id`,
`method`, and optional object/null `params`; notifications omit `id`; responses
contain `id` and exactly one of `result` or `error`. There is no `{epoch, body}`
wrapper, sequence field, or `domain/` method prefix. An error object requires a
safe-integer `code` and string `message`; its provider `message` and any `data`
are never forwarded across the stable boundary. Additional envelope members
are not part of v1 and are not preserved.

### Field notation and projection rules

The tables below are the stable data-plane contract inside that envelope.
`field!` means required, `field?` means optional, `T[]` is an array, `T/null`
accepts JSON null, `uint` is a non-negative safe integer, and `int` is any safe
integer. A field that is optional but not marked nullable must be omitted rather
than set to null. `JSON` means any defined JSON value; `Object` means a plain
JSON object. Enum alternatives are separated with `/`.

All local request param objects are strict allowlists: an unknown field, wrong
type, missing required field, or unknown enum value fails with an adapter
contract error before the request is written upstream. This strictness also
applies recursively to `UserInput`, `DynamicToolSpec`, `ApprovalPolicy`, and the
`turn/start` `SandboxPolicy`. The explicit `JSON` and `Object` fields below are
the only opaque extension points; their members are intentionally not
projected.

Upstream results, notifications, and reverse-request params are projected
field by field: every named field is type checked, required fields must be
present, and unknown fields are stripped at each forward-compatible projector
level before Rust can see them. `ApprovalPolicy`, `TextElement`/`ByteRange`,
and amendment payloads instead reject unknown members at their exact security
boundary; explicit `JSON` fields remain opaque. The normalization exceptions
are called out below.

Reverse-request result objects sent by Rust are strict at the top level.
`DynamicOutput` and amendment payloads are recursively strict. A returned
`PermissionProfile` is strict at the profile, `fileSystem`, and `network`
object levels; entry/path objects are projected and have unknown siblings
stripped. A `CommandDecision` wrapper selects its one recognized amendment
member and strips sibling fields. These rules do not relax the envelope,
correlation, method-name, frame, queue, or timeout bounds in this ADR.

### Shared stable objects

These definitions are referenced by all method tables. Fields not listed here
do not cross the stable-domain boundary.

| Type | Stable fields |
| --- | --- |
| `ClientInfo` | `name!: string`, `version!: string`, `title?: string/null` |
| `ClientCapabilities` | `experimentalApi?: bool`, `mcpServerOpenaiFormElicitation?: bool`, `optOutNotificationMethods?: string[]/null` |
| `ApprovalPolicy` | String enum `never/on-request/untrusted`; or `{granular!: {mcp_elicitations!: bool, rules!: bool, sandbox_approval!: bool, request_permissions?: bool, skill_approval?: bool}}`. Both object levels are strict. |
| `ApprovalReviewer` | Enum `auto_review/guardian_subagent/user` |
| `SandboxMode` | Enum `read-only/workspace-write/danger-full-access` |
| `SandboxPolicy` | Discriminated object: `readOnly {type!, networkAccess?: bool}`; `workspaceWrite {type!, writableRoots?: string[], networkAccess?: bool, excludeSlashTmp?: bool, excludeTmpdirEnvVar?: bool}`; `dangerFullAccess {type!}`; `externalSandbox {type!, networkAccess?: restricted/enabled}`. |
| `UserInput` | `text {type!, text!, text_elements?: TextElement[]}`; `image {type!, url!, detail?: auto/low/high/original/null}`; `localImage {type!, path!, detail?: auto/low/high/original/null}`; `audio {type!, url!}`; `localAudio {type!, path!}`; `skill` or `mention {type!, name!, path!}`. `TextElement` is `{byteRange!: {start!: uint, end!: uint}, placeholder?: string/null}`. |
| `DynamicToolSpec` | `function {type!, name!, description!, inputSchema!: JSON, deferLoading?: bool}`; or `namespace {type!, name!, description!, tools!: FunctionSpec[]}`. |
| `AdditionalContext` | Record values are `{kind!: application/untrusted, value!: string}`. |
| `CollaborationMode` | `{mode!: default/plan, settings!: {model!: string, developer_instructions?: string/null, reasoning_effort?: non-empty string/null}}` |
| `ThreadSettings` | `{approvalPolicy!: ApprovalPolicy, approvalsReviewer!: ApprovalReviewer, collaborationMode!: CollaborationMode, cwd!: string, model!: string, modelProvider!: string, sandboxPolicy!: SandboxPolicy}` |
| `PermissionProfile` | `{fileSystem?: {entries?: {access!: deny/read/write, path!: FileSystemPath}[]/null, globScanMaxDepth?: uint/null, read?: string[]/null, write?: string[]/null}/null, network?: {enabled?: bool/null}/null}`. `FileSystemPath` is `{type: path, path!: string}`, `{type: glob_pattern, pattern!: string}`, or `{type: special, value!: {kind!: root/minimal/project_roots/tmpdir/slash_tmp, subpath?: string/null}}`. |
| `ThreadStatus` | `{type: notLoaded/idle/systemError}`; or `{type: active, activeFlags: (waitingOnApproval/waitingOnUserInput)[]}`. Unknown status types normalize to `{type: "unsupported"}` and unknown active flags are dropped. |
| `ThreadSource` | String `appServer/cli/exec/unknown/vscode`; `{custom: string}`; or `{subAgent: compact/memory_consolidation/review/other}`; `{subAgent: {thread_spawn: {depth!: int, parent_thread_id!: string, agent_nickname?: string/null, agent_path?: string/null, agent_role?: string/null}}}`; `{subAgent: {other: string}}`. Other values normalize to `unknown` or `other`. |
| `Thread` | Required: `id`, `sessionId`, `preview`, `modelProvider`, `cliVersion`, `cwd` strings; `createdAt`, `updatedAt` ints; `status: ThreadStatus`; `ephemeral: bool`; `turns: Turn[]`; `source: ThreadSource`. Optional: `name`, `path`, `forkedFromId`, `parentThreadId` as `string/null`. |
| `Turn` | `{id!: string, items!: ThreadItem[], status!: completed/interrupted/failed/inProgress, startedAt?: int/null, completedAt?: int/null, durationMs?: int/null, error?: TurnError/null, itemsView?: notLoaded/summary/full}` |
| `TurnError` | `{message!: "upstream turn failed", codexErrorInfo?: StableError/null}`. Provider message text is validated then replaced. Rate-limit errors normalize to `{source: "upstream", category: "capacity", retryable: true}`. Reviewed tags are `badRequest/contextWindowExceeded/cyberPolicy/internalServerError/misalignmentPolicyViolation/other/sandboxError/serverOverloaded/sessionBudgetExceeded/threadRollbackFailed/unauthorized/usageLimitExceeded/httpConnectionFailed/responseStreamConnectionFailed/responseStreamDisconnected/responseTooManyFailedAttempts/activeTurnNotSteerable`; only the tag and an optional non-negative `httpStatusCode` survive. Unknown tags normalize to `other`. |
| `QueuedSubmission` | `{id!: string, clientUserMessageId!: string, input!: UserInput[]}` |
| `ThreadStartResult` | Required: `thread!: Thread`, `approvalPolicy!: ApprovalPolicy`, `approvalsReviewer!: ApprovalReviewer`, `cwd!: string`, `model!: string`, `modelProvider!: string`, `sandbox!: SandboxPolicy`. Optional: `instructionSources?: string[]`, `reasoningEffort?: string/null`, `serviceTier?: string/null`. |
| `TokenUsageBreakdown` | Required ints: `inputTokens`, `cachedInputTokens`, `outputTokens`, `reasoningOutputTokens`, `totalTokens`; optional `cacheWriteInputTokens?: int`. |
| `CommandAction` | `read {type!, command!, name!, path!}`; `listFiles {type!, command!, path?: string/null}`; `search {type!, command!, path?: string/null, query?: string/null}`; `unknown {type!, command!}`. An unreviewed type normalizes to `{type: "unknown", command: "unsupported"}`. |
| `CommandDecision` | Enum `accept/acceptForSession/decline/cancel`; `{acceptWithExecpolicyAmendment: {execpolicy_amendment!: string[]}}`; or `{applyNetworkPolicyAmendment: {network_policy_amendment!: {action!: allow/deny, host!: string}}}`. Amendment objects are strict. |
| `DynamicOutput` | `{type!: inputText, text!: string}`; `{type!: inputImage, imageUrl!: string}`; or `{type!: inputAudio, audioUrl!: string}`. |

`ThreadItem` is a discriminated object with required `type` and `id`. Its
reviewed variants and remaining fields are:

| `ThreadItem.type` | Stable fields besides `type` and `id` |
| --- | --- |
| `userMessage` | `content!: UserInput[]`, `clientId?: string/null` |
| `agentMessage` | `text!: string`, `phase?: commentary/final_answer/null`, `memoryCitation?: {threadIds!: string[], entries!: {path!: string, lineStart!: uint, lineEnd!: uint, note!: string}[]}/null` |
| `plan` | `text!: string` |
| `reasoning` | `summary?: string[]`, `content?: string[]` |
| `hookPrompt` | `fragments!: {hookRunId!: string, text!: string}[]` |
| `commandExecution` | Required `command!: string`, `commandActions!: CommandAction[]`, `cwd!: string`, `status!: completed/declined/failed/inProgress`. Optional `aggregatedOutput?: string/null`, `durationMs?: int/null`, `exitCode?: int/null`, `processId?: string/null`, `pluginId?: string/null`, `scriptPath?: string/null`, `source?: agent/unifiedExecInteraction/unifiedExecStartup/userShell`. |
| `fileChange` | `status!: completed/declined/failed/inProgress`, `changes!: {path!: string, kind!: PatchKind, diff!: string}[]`. `PatchKind` is `add/delete {type!}` or `update {type!, move_path?: string/null}`. |
| `mcpToolCall` | Required `server!: string`, `tool!: string`, `arguments!: JSON`, `status!: completed/failed/inProgress`. Optional `durationMs?: int/null`, `readOnlyHint?: bool/null`, `error?: {message!: string}/null`, `result?: {content!: McpContent[]}/null`, `appContext?: {connectorId!: string, actionName?: string/null, appName?: string/null, linkId?: string/null, resourceUri?: string/null}/null`, `mcpAppResourceUri?: string/null`, `pluginId?: string/null`. |
| `dynamicToolCall` | Required `tool!: string`, `arguments!: JSON`, `status!: completed/failed/inProgress`. Optional `namespace?: string/null`, `contentItems?: DynamicOutput[]/null`, `durationMs?: int/null`, `success?: bool/null`. |
| `collabAgentToolCall` | Required `tool!: closeAgent/resumeAgent/sendInput/spawnAgent/wait`, `status!: completed/failed/inProgress`, `senderThreadId!: string`, `receiverThreadIds!: string[]`, `agentsStates!: Record<{status!: completed/errored/interrupted/notFound/pendingInit/running/shutdown, message?: string/null}>`. Optional `model?: string/null`, `prompt?: string/null`, `reasoningEffort?: non-empty string/null`. |
| `subAgentActivity` | `agentPath!: string`, `agentThreadId!: string`, `kind!: interacted/interrupted/started` |
| `webSearch` | `query!: string` |
| `imageView` | `path!: string` |
| `sleep` | `durationMs!: uint` |
| `imageGeneration` | `result!: string`, `status!: string`, optional `revisedPrompt?: string/null`, `savedPath?: string/null`, `transparentBackground?: bool/null` |
| `enteredReviewMode`, `exitedReviewMode` | `review!: string` |
| `contextCompaction` | No additional fields. |
| `functionCallOutput` | No additional fields in 0.151.0; 0.149.0 normalizes it to `unsupported`. |
| `unsupported` normalization | Unreviewed item types, or reviewed command/tool/collaboration types with an unreviewed status/tool, become `{type: "unsupported", id!: string, reviewedKind!: string}`. |

`McpContent` variants are:

- `text {type!, text!}`;
- `image {type!, data!, mimeType!}` or `audio {type!, data!, mimeType!}`;
- `resource_link {type!, name!, uri!, title?: string, description?: string, mimeType?: string, size?: uint}`;
- `resource {type!, resource!: {uri!: string, mimeType?: string, text?: string, blob?: string}}`.

An unreviewed MCP content type becomes `{type: "unsupported"}`.

### Request and result fields

The complete promoted Rust-to-Codex request/result surface is:

| Method | Stable `params` | Stable successful `result` |
| --- | --- | --- |
| `initialize` | `clientInfo!: ClientInfo`; `capabilities?: ClientCapabilities/null` | Required strings `codexHome`, `platformFamily`, `platformOs`, `userAgent`. |
| `thread/start` | All optional: `sandbox?: SandboxMode`, `approvalPolicy?: ApprovalPolicy`, `approvalsReviewer?: ApprovalReviewer`, `baseInstructions?: string`, `config?: Object`, `cwd?: string`, `developerInstructions?: string`, `dynamicTools?: DynamicToolSpec[]`, `serviceTier?: string`, `serviceName?: string`, `ephemeral?: bool`, `personality?: none/friendly/pragmatic`, `sessionStartSource?: clear/startup`, `threadSource?: string`, `model?: string`, `modelProvider?: string`, `projectId?: string`. Missing/null params normalize to `{}`. | `ThreadStartResult` |
| `thread/list` | All optional: `cursor?: string`, `limit?: uint`, `sortKey?: created_at/updated_at/recency_at/section_position`, `sortDirection?: asc/desc`, `modelProviders?: string[]`, `sourceKinds?: (cli/vscode/exec/appServer/subAgent/subAgentReview/subAgentCompact/subAgentThreadSpawn/subAgentOther/unknown)[]`, `cwd?: string/string[]`, `archived?: bool`, `projectId?: string`, `sectionId?: string`, `searchTerm?: string`, `useStateDbOnly?: bool`. Missing/null params normalize to `{}`. | `{data!: Thread[], nextCursor?: string/null, backwardsCursor?: string/null}` |
| `thread/read` | `threadId!: string`, `includeTurns?: bool` | `{thread!: Thread}` |
| `thread/resume` | `threadId!: string`; optional `excludeTurns?: bool`, `approvalPolicy?: ApprovalPolicy`, `approvalsReviewer?: ApprovalReviewer`, `baseInstructions?: string`, `config?: Object`, `cwd?: string`, `developerInstructions?: string`, `sandbox?: SandboxMode`, `personality?: none/friendly/pragmatic`, `model?: string`, `modelProvider?: string`, `serviceTier?: string` | `ThreadStartResult` |
| `thread/unsubscribe` | `threadId!: string` | `{status!: notLoaded/notSubscribed/unsubscribed}` |
| `thread/turns/list` | `threadId!: string`; optional `cursor?: string`, `limit?: uint`, `sortDirection?: asc/desc`, `itemsView?: notLoaded/summary/full` | `{data!: Turn[], nextCursor?: string/null, backwardsCursor?: string/null}` |
| `thread/items/list` | `threadId!: string`; optional `turnId?: string`, `cursor?: string`, `limit?: uint`, `sortDirection?: asc/desc` | `{data!: {item!: ThreadItem, turnId!: string}[], nextCursor?: string/null, backwardsCursor?: string/null}` |
| `thread/queue/add` | `threadId!: string`, `clientUserMessageId!: string`, `input!: UserInput[]` | `{queuedSubmission!: QueuedSubmission}` |
| `thread/queue/list` | `threadId!: string`; optional `cursor?: string`, `limit?: uint` | `{data!: QueuedSubmission[], nextCursor?: string/null}` |
| `thread/queue/start` | `threadId!: string`, `queuedSubmissionId!: string` | `{turn!: Turn}` |
| `turn/start` | `threadId!: string`, `input!: UserInput[]`; optional `sandboxPolicy?: SandboxPolicy`, `approvalPolicy?: ApprovalPolicy`, `approvalsReviewer?: ApprovalReviewer`, `clientUserMessageId?: string`, `summary?: auto/concise/detailed/none`, `cwd?: string`, `effort?: non-empty string`, `personality?: none/friendly/pragmatic`, `model?: string`, `serviceTier?: string`, `outputSchema?: JSON` | `{turn!: Turn}` |
| `turn/steer` | `threadId!: string`, `expectedTurnId!: string`, `input!: UserInput[]`; optional `additionalContext?: AdditionalContext`, `clientUserMessageId?: string`, `responsesapiClientMetadata?: Record<string>` | `{turnId!: string}` |
| `turn/interrupt` | `threadId!: string`, `turnId!: string` | `{}`; any upstream result fields are stripped. |

The sole local notification is `initialized`; its params may be absent, null,
or `{}`. A present object is strict and must be empty.

### Notification fields

The complete promoted Codex-to-Rust notification surface is:

| Method | Stable `params` |
| --- | --- |
| `account/rateLimits/updated` | `{rateLimits!: RateLimitSnapshot}`. `RateLimitSnapshot` has optional nullable `credits`, `individualLimit`, `limitId`, `limitName`, `planType`, `primary`, `rateLimitReachedType`, `secondary`, `spendControlReached`. `credits` is `{hasCredits!: bool, unlimited!: bool, balance?: string/null}`. `individualLimit` is `{limit!: string, remainingPercent!: int, resetsAt!: int, used!: string}`. `primary`/`secondary` are `{usedPercent!: int, resetsAt?: int/null, windowDurationMins?: int/null}`. `planType` enum: `free/go/plus/pro/prolite/team/self_serve_business_prolite/self_serve_business_usage_based/business/ent26/enterprise_cbp_automation/enterprise_cbp_usage_based/enterprise/edu/edu_plus/edu_pro/unknown`. `rateLimitReachedType` enum: `rate_limit_reached/workspace_owner_credits_depleted/workspace_member_credits_depleted/workspace_owner_usage_limit_reached/workspace_member_usage_limit_reached`. |
| `remoteControl/status/changed` | Required `installationId!: string`, `serverName!: string`, `status!: disabled/connecting/connected/errored`; optional `environmentId?: string/null`. |
| `thread/goal/cleared` | `{threadId!: string}` |
| `thread/settings/updated` | `{threadId!: string, threadSettings!: ThreadSettings}` |
| `thread/started` | `{thread!: Thread}` |
| `thread/status/changed` | `{threadId!: string, status!: ThreadStatus}` |
| `thread/queue/changed` | `{threadId!: string}` |
| `turn/started` | `{threadId!: string, turn!: Turn}` |
| `turn/completed` | `{threadId!: string, turn!: Turn}` |
| `item/started` | `{threadId!: string, turnId!: string, startedAtMs!: int, item!: ThreadItem}` |
| `item/agentMessage/delta` | `{threadId!: string, turnId!: string, itemId!: string, delta!: string}` |
| `item/commandExecution/outputDelta` | `{threadId!: string, turnId!: string, itemId!: string, delta!: string}` |
| `item/completed` | `{threadId!: string, turnId!: string, completedAtMs!: int, item!: ThreadItem}` |
| `thread/tokenUsage/updated` | `{threadId!: string, turnId!: string, tokenUsage!: {total!: TokenUsageBreakdown, last!: TokenUsageBreakdown, modelContextWindow?: int/null}}` |
| `serverRequest/resolved` | `{threadId!: string, requestId!: JSON}` |
| `error` | `{threadId!: string, turnId!: string, error!: TurnError, willRetry!: bool}` |

### Reverse request and response fields

The complete promoted Codex-to-Rust correlated request surface is:

| Method | Stable request `params` | Stable Rust success `result` |
| --- | --- | --- |
| `item/tool/call` | Required `threadId!: string`, `turnId!: string`, `callId!: string`, `tool!: string`, `arguments!: JSON`; optional `namespace?: string/null`. | Strict `{contentItems!: DynamicOutput[], success!: bool}` |
| `item/commandExecution/requestApproval` | Required `threadId!: string`, `turnId!: string`, `itemId!: string`, `startedAtMs!: int`. Optional `kind?: command`, `approvalId?: string/null`, `command?: string/null`, `cwd?: string/null`, `reason?: string/null`, `environmentId?: string/null`, `autoResolutionMs?: uint`, `availableDecisions?: CommandDecision[]/null`, `commandActions?: CommandAction[]/null`, `proposedExecpolicyAmendment?: string[]/null`, `proposedNetworkPolicyAmendments?: {action!: allow/deny, host!: string}[]/null`, `networkApprovalContext?: {host!: string, protocol!: http/https/socks5Tcp/socks5Udp}/null`, `additionalPermissions?: PermissionProfile/null`. A missing `kind` is treated as `command`; `writeStdin` and other kinds are not promoted. | Strict `{decision!: CommandDecision}` |
| `item/fileChange/requestApproval` | Required `threadId!: string`, `turnId!: string`, `itemId!: string`, `startedAtMs!: int`; optional `grantRoot?: string/null`, `reason?: string/null`, `autoResolutionMs?: uint`. | Strict `{decision!: accept/acceptForSession/decline/cancel}` |
| `item/permissions/requestApproval` | Required `threadId!: string`, `turnId!: string`, `itemId!: string`, `startedAtMs!: int`, `cwd!: string`, `permissions!: PermissionProfile`; optional `reason?: string/null`, `environmentId?: string/null`, `autoResolutionMs?: uint`. | Strict `{permissions!: PermissionProfile, scope?: turn/session, strictAutoReview?: bool/null}`; returned profile, `fileSystem`, and `network` object levels are strict, while entry/path objects are projected. |

For any reverse request, Rust may instead return an error with integer `code`;
the sidecar discards its message/data and sends the static message
`bridge rejected server request` upstream. Unsupported or filtered reverse
request methods receive `-32601` upstream.

### Promoted requests

The current adapters admit these Rust-to-Codex request names:

- `initialize`;
- `thread/start`, `thread/list`, `thread/read`, `thread/resume`;
- `thread/unsubscribe`, `thread/turns/list`, `thread/items/list`;
- `thread/queue/add`, `thread/queue/list`, `thread/queue/start`;
- `turn/start`, `turn/steer`, `turn/interrupt`.

The only admitted Rust-to-Codex notification is `initialized`.

### Promoted notifications and server requests

The current adapters admit these Codex-to-Rust notifications:

- `account/rateLimits/updated`, `remoteControl/status/changed`;
- `thread/goal/cleared`, `thread/settings/updated`;
- `thread/started`, `thread/status/changed`, `thread/queue/changed`;
- `turn/started`, `turn/completed`;
- `item/started`, `item/agentMessage/delta`;
- `item/commandExecution/outputDelta`, `item/completed`;
- `thread/tokenUsage/updated`, `serverRequest/resolved`, `error`.

They admit these Codex-to-Rust server requests:

- `item/tool/call`;
- `item/commandExecution/requestApproval`;
- `item/fileChange/requestApproval`;
- `item/permissions/requestApproval`.

An unsupported local request receives `-32601`. An unsupported upstream server
request is rejected upstream with `-32601`. For an unreviewed upstream
notification, the sidecar filters both its method name and params before they
can reach Rust. The 0.151.0 adapter also adds
`mcpServer/event/stream/notification`, `thread/realtime/item/completed`,
`thread/realtime/item/started`, and `thread/realtime/item/transcript/delta` to
`initialize.capabilities.optOutNotificationMethods`; local filtering remains
the defense-in-depth boundary.

## Bounds, correlation, and priority

| Bound | Current v1 value |
| --- | ---: |
| Frame before newline | 33,554,432 bytes |
| Configure `maxPending` | 448 |
| Rust bootstrap timeout | 15 seconds |
| Sidecar version probe timeout | 5 seconds |
| Local-to-upstream request timeout | 30 seconds |
| Upstream reverse-request timeout | 180 seconds |
| Correlation ID | 128 UTF-8 bytes |
| Method name | 256 UTF-8 bytes |
| JSON nesting | 128 object/array levels |
| JSON structural tokens | 65,536 per frame |
| Frame fragments | 4,096 chunks |
| Derived write queue | 512 frames, 64 MiB |
| Consecutive control burst | 8 frames |
| Configured shutdown grace | 5 seconds |

The sidecar accepts a safe integer correlation ID or a non-empty string made
only of ASCII letters, digits, `_`, `.`, `:`, and `-`. It replaces each Rust
request ID with `bridge:<random-nonce>:<counter>` before the upstream write and
restores the Rust ID on response. It similarly replaces an upstream
server-request ID with `server:<random-nonce>:<counter>` and restores the
upstream ID when Rust responds. Active and bounded retired-ID sets reject
reuse, late responses, and unknown correlations.

`maxPending` is sent as the scalar value `448`; it is not negotiated through a
limits object. The shared bound covers the Rust broker's 384 simultaneous
outgoing correlations plus 64 reverse-request slots, so a saturated local lane
cannot crowd an approval or tool request out of its reviewed capacity.
Capacity is checked before enqueueing a new correlated request. Local request
saturation returns `-32020`. Upstream server-request saturation returns
`-32021` to Codex. A write-queue saturation before the upstream write is also
returned locally as `-32020`.

A local-to-upstream request that reaches its 30-second deadline terminates the
epoch because its mutation outcome may be uncertain. A reverse request instead
has a 180-second handler envelope. On expiry the sidecar returns static error
`-32022` upstream, frees the slot, retains the reviewed resolution-ID mapping,
and keeps the epoch healthy. Exactly one Rust response racing that timeout is
dropped; a second response for the retired correlation still fails closed.

The sidecar has `control` and `normal` FIFO write lanes. `turn/interrupt`, all
server requests and their responses, and selected terminal/status notifications
use the control lane. The queue serves at most eight consecutive control frames
while normal work is waiting, then one normal frame.

The sidecar applies its own JSON/frame guards, and Rust applies the existing
transport/RPC retained-memory and structural guards after the sidecar. Passing
the 32 MiB line limit alone does not guarantee acceptance of a structurally
pathological payload.

## Errors and outcome semantics

The sidecar deliberately reduces error content:

- an upstream request error preserves only its integer code and replaces the
  message with `upstream request failed`; upstream error data is not forwarded;
- an error response from Rust to an upstream server request preserves only the
  integer code and uses `bridge rejected server request`;
- an expired reverse request returns only code `-32022` and the static message
  `bridge server request timed out`;
- process stderr contains a static `codex_sidecar_failure code=<class>` line;
- malformed frames, unknown/duplicate-late correlation, local request timeout,
  stdout EOF, and child exit terminate the current sidecar session. The single
  reverse-timeout race described above is the only late-response exception.

V1 does not put `determinacy`, `retryable`, `fatal`, or an epoch in an RPC error
object. The Rust RPC/client and durable write layers remain responsible for
distinguishing a pre-write rejection from an uncertain post-write connection
loss. Documentation and callers must not infer a richer sidecar error contract.

## No replay and Rust epochs

The sidecar sends an admitted local request upstream once. It has no reconnect
loop, mutation journal, or replay cache. On local request timeout, pipe loss,
Codex exit, or sidecar failure, its correlation maps are discarded and the
process exits. It does not resend a pending mutation into a replacement Codex
process. Reverse-request timeout is request-scoped as documented above and also
never causes a replay.

The Rust supervisor assigns a fresh in-memory `ConnectionEpoch` to each new
sidecar/Codex pair. Rust request tokens and pending work are scoped to that
epoch, and a failed epoch is cancelled before the supervisor starts a
replacement. The literal capability name `epoch-on-restart` describes this
combined lifecycle. The epoch is not serialized in `hello`, `configure`, the
configure response, or data-plane frames.

Because v1 has no wire-level applied/uncertain receipt, a lost response after a
mutation write remains subject to the existing conservative Rust no-retry and
reconciliation rules. Restart never authorizes the sidecar to replay it.

## Shutdown and process-tree cleanup

The normal Rust supervisor path cancels the RPC epoch and closes the stream;
the sidecar treats local stdin EOF as graceful shutdown. It finishes its
already-queued upstream writes, closes Codex stdin, waits within its configured
five-second child grace, then kills the child if necessary. The sidecar also
accepts a correlated `sidecar/shutdown` request and replies with `{}` before
starting the same cleanup, but the Rust supervisor does not depend on that
request for correctness.

Rust then waits for the sidecar leader within the effective five-second process
grace. If needed it targets the whole POSIX process group or Windows Job,
performs another bounded wait, and reaps the leader. The shutdown path contains
multiple bounded phases; five seconds is the configured per-process grace, not
a promise that every supervisor shutdown completes within five wall-clock
seconds. Process-tree ownership begins immediately after spawn: a bootstrap
guard targets the full group/Job even if the supervisor cancels the factory
future before configure completes.

If the Codex child exits unexpectedly, the sidecar session fails. Rust clears
the old client, terminates the owned tree, applies bounded supervisor backoff,
and only then starts a new epoch. It never leaves a knowingly live descendant
before spawning the replacement. A kill, wrapper wait, or bootstrap-cleanup
failure enters a static terminal degraded state and fences replacement until
the bridge is restarted; it is never treated as permission to overlap epochs.

## Redaction and launch data

Rust clears the sidecar environment and restores only `NO_COLOR`, `PATH`, and
the minimum required Windows process variables. The Node entrypoint is the one
sidecar argument. Configured Codex path, optional home, and wrapper arguments
are sent in the configure frame rather than the sidecar argv. Rust `Debug`
output replaces every configured path and argument value with a marker or
count.

The sidecar gives Codex a reviewed environment allowlist and may set
`CODEX_HOME` only for that child. Provider stderr, raw errors, request bodies,
prompts, tool arguments, credentials, and configured paths are not copied to
ordinary logs or sidecar stderr.

The operator-visible probe reports only the exact supported version,
initialize user agent and platform fields, Rust epoch, backend label, wire
protocol/version, and the seven capability names.

## Configuration and probe

The backend is explicit and fixed for the process lifetime:

```toml
[codex.backend]
mode = "protocol_sidecar"
node_binary = "node"
sidecar_entrypoint = "/opt/lark-codex-bridge/codex-sidecar/index.cjs"
# codex_binary = "/absolute/path/to/exact/codex" # optional override
# codex_home = "/absolute/private/codex-home"
# codex_arguments = []
```

There is no live fallback from `protocol_sidecar` to `spawned_stdio` or
`external_endpoint`. Changing mode requires a full bridge restart.

Probe the same configured components before selecting the backend:

```bash
lark-codex-bridge codex sidecar-probe \
  --entrypoint /opt/lark-codex-bridge/codex-sidecar/index.cjs
```

Use `--node-binary`, optional `--codex-binary`, `--codex-home`, and repeated
`--codex-argument` flags when the deployment config uses their non-default
equivalents. Omitting `--codex-binary` tests the package-lock-pinned release.
The probe starts the
supervisor, completes bootstrap plus Codex initialize, prints one sanitized JSON
object, and shuts down the whole owned process tree.

## Current limits and non-goals

The following are deliberately not claims of v1:

- no `bootId`, adapter manifest, schema hash, or protocol-version array;
- no epoch or sequence number on the local wire;
- no nested configure object or per-lane limit negotiation;
- no `domain/...` renaming or generic `{epoch, body}` payload wrapper;
- no sidecar-provided determinacy/retry error object;
- no arbitrary Codex-version compatibility or nearest-version matching;
- no sidecar-owned durable state, authorization, rendering, or reconciliation;
- no live backend fallback;
- no shared remote sidecar or multi-bridge multiplexing;
- no runtime package download.

Future work may add stronger generated adapter codecs or new negotiated fields,
but must not be described as part of v1 until the code and conformance tests
implement the same exact contract.

## Verification

The checked-in suites exercise the bootstrap, exact capability matching,
0.149.0/0.151.0 adapter selection, malformed/oversize input, correlation reuse,
capacity rejection, priority writes, no mutation replay, shutdown, and
process-tree cleanup:

```bash
npm run verify --prefix codex-sidecar
cargo test --locked codex::sidecar
cargo test --locked --test codex_sidecar
```

The operator probe above is the supported local installation check. A skipped
or fake-only test does not prove that a particular installed Codex binary can
start and initialize through the sidecar.
