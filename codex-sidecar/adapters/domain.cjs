"use strict";

const { SidecarError, isPlainObject } = require("../wire.cjs");

function contract(label) {
  throw new SidecarError("adapter_contract", `${label} is outside the stable domain contract`);
}

function object(value, label, emptyWhenMissing = false) {
  if (emptyWhenMissing && (value === undefined || value === null)) {
    return {};
  }
  if (!isPlainObject(value)) {
    contract(label);
  }
  return value;
}

function projectObject(
  value,
  label,
  fields,
  required = [],
  { strict = false, emptyWhenMissing = false } = {},
) {
  const source = object(value, label, emptyWhenMissing);
  if (strict) {
    for (const key of Object.keys(source)) {
      if (!Object.hasOwn(fields, key)) {
        contract(label);
      }
    }
  }
  for (const key of required) {
    if (!Object.hasOwn(source, key)) {
      contract(label);
    }
  }
  const projected = {};
  for (const [key, mapper] of Object.entries(fields)) {
    if (Object.hasOwn(source, key)) {
      projected[key] = mapper(source[key], `${label}.${key}`);
    }
  }
  return projected;
}

function json(value, label) {
  if (value === undefined) {
    contract(label);
  }
  return value;
}

function string(value, label) {
  if (typeof value !== "string") {
    contract(label);
  }
  return value;
}

function nonemptyString(value, label) {
  const selected = string(value, label);
  if (selected.length === 0) {
    contract(label);
  }
  return selected;
}

function boolean(value, label) {
  if (typeof value !== "boolean") {
    contract(label);
  }
  return value;
}

function integer(value, label) {
  if (!Number.isSafeInteger(value)) {
    contract(label);
  }
  return value;
}

function unsigned(value, label) {
  const selected = integer(value, label);
  if (selected < 0) {
    contract(label);
  }
  return selected;
}

function nullable(mapper) {
  return (value, label) => (value === null ? null : mapper(value, label));
}

function arrayOf(mapper) {
  return (value, label) => {
    if (!Array.isArray(value)) {
      contract(label);
    }
    return value.map((entry, index) => mapper(entry, `${label}[${index}]`));
  };
}

function enumOf(values) {
  const allowed = new Set(values);
  return (value, label) => {
    const selected = string(value, label);
    if (!allowed.has(selected)) {
      contract(label);
    }
    return selected;
  };
}

function recordOf(mapper) {
  return (value, label) => {
    const source = object(value, label);
    const projected = {};
    for (const [key, entry] of Object.entries(source)) {
      projected[key] = mapper(entry, `${label}.${key}`);
    }
    return projected;
  };
}

const approvalReviewer = enumOf(["auto_review", "guardian_subagent", "user"]);
const personality = enumOf(["none", "friendly", "pragmatic"]);
const sandboxMode = enumOf(["read-only", "workspace-write", "danger-full-access"]);
const sortDirection = enumOf(["asc", "desc"]);
const threadSortKey = enumOf([
  "created_at",
  "updated_at",
  "recency_at",
  "section_position",
]);
const threadSourceKind = enumOf([
  "cli",
  "vscode",
  "exec",
  "appServer",
  "subAgent",
  "subAgentReview",
  "subAgentCompact",
  "subAgentThreadSpawn",
  "subAgentOther",
  "unknown",
]);
const summaryMode = enumOf(["auto", "concise", "detailed", "none"]);
const imageDetail = enumOf(["auto", "low", "high", "original"]);
const turnItemsView = enumOf(["notLoaded", "summary", "full"]);
const collaborationModeKind = enumOf(["default", "plan"]);
const planType = enumOf([
  "free",
  "go",
  "plus",
  "pro",
  "prolite",
  "team",
  "self_serve_business_prolite",
  "self_serve_business_usage_based",
  "business",
  "ent26",
  "enterprise_cbp_automation",
  "enterprise_cbp_usage_based",
  "enterprise",
  "edu",
  "edu_plus",
  "edu_pro",
  "unknown",
]);
const rateLimitReachedType = enumOf([
  "rate_limit_reached",
  "workspace_owner_credits_depleted",
  "workspace_member_credits_depleted",
  "workspace_owner_usage_limit_reached",
  "workspace_member_usage_limit_reached",
]);
const remoteControlStatus = enumOf(["disabled", "connecting", "connected", "errored"]);

function projectApprovalPolicy(value, label) {
  if (typeof value === "string") {
    return enumOf(["never", "on-request", "untrusted"])(value, label);
  }
  return projectObject(
    value,
    label,
    {
      granular: (entry, childLabel) =>
        projectObject(
          entry,
          childLabel,
          {
            mcp_elicitations: boolean,
            rules: boolean,
            sandbox_approval: boolean,
            request_permissions: boolean,
            skill_approval: boolean,
          },
          ["mcp_elicitations", "rules", "sandbox_approval"],
          { strict: true },
        ),
    },
    ["granular"],
    { strict: true },
  );
}

function projectCollaborationMode(value, label) {
  return projectObject(
    value,
    label,
    {
      mode: collaborationModeKind,
      settings: (entry, childLabel) =>
        projectObject(
          entry,
          childLabel,
          {
            developer_instructions: nullable(string),
            model: string,
            reasoning_effort: nullable(nonemptyString),
          },
          ["model"],
        ),
    },
    ["mode", "settings"],
  );
}

function projectCreditsSnapshot(value, label) {
  return projectObject(
    value,
    label,
    {
      balance: nullable(string),
      hasCredits: boolean,
      unlimited: boolean,
    },
    ["hasCredits", "unlimited"],
  );
}

function projectSpendControlLimit(value, label) {
  return projectObject(
    value,
    label,
    {
      limit: string,
      remainingPercent: integer,
      resetsAt: integer,
      used: string,
    },
    ["limit", "remainingPercent", "resetsAt", "used"],
  );
}

function projectRateLimitWindow(value, label) {
  return projectObject(
    value,
    label,
    {
      resetsAt: nullable(integer),
      usedPercent: integer,
      windowDurationMins: nullable(integer),
    },
    ["usedPercent"],
  );
}

function projectRateLimitSnapshot(value, label) {
  return projectObject(value, label, {
    credits: nullable(projectCreditsSnapshot),
    individualLimit: nullable(projectSpendControlLimit),
    limitId: nullable(string),
    limitName: nullable(string),
    planType: nullable(planType),
    primary: nullable(projectRateLimitWindow),
    rateLimitReachedType: nullable(rateLimitReachedType),
    secondary: nullable(projectRateLimitWindow),
    spendControlReached: nullable(boolean),
  });
}

function projectThreadSettings(value, label) {
  return projectObject(
    value,
    label,
    {
      approvalPolicy: projectApprovalPolicy,
      approvalsReviewer: approvalReviewer,
      collaborationMode: projectCollaborationMode,
      cwd: string,
      model: string,
      modelProvider: string,
      sandboxPolicy: projectTurnSandboxPolicy,
    },
    [
      "approvalPolicy",
      "approvalsReviewer",
      "collaborationMode",
      "cwd",
      "model",
      "modelProvider",
      "sandboxPolicy",
    ],
  );
}

function projectByteRange(value, label) {
  return projectObject(
    value,
    label,
    { start: unsigned, end: unsigned },
    ["start", "end"],
    { strict: true },
  );
}

function projectTextElement(value, label) {
  return projectObject(
    value,
    label,
    { byteRange: projectByteRange, placeholder: nullable(string) },
    ["byteRange"],
    { strict: true },
  );
}

function projectUserInput(value, label, strict = true) {
  const source = object(value, label);
  const type = string(source.type, `${label}.type`);
  const options = { strict };
  switch (type) {
    case "text":
      return projectObject(
        source,
        label,
        { type: enumOf(["text"]), text: string, text_elements: arrayOf(projectTextElement) },
        ["type", "text"],
        options,
      );
    case "image":
      return projectObject(
        source,
        label,
        { type: enumOf(["image"]), url: string, detail: nullable(imageDetail) },
        ["type", "url"],
        options,
      );
    case "localImage":
      return projectObject(
        source,
        label,
        { type: enumOf(["localImage"]), path: string, detail: nullable(imageDetail) },
        ["type", "path"],
        options,
      );
    case "audio":
      return projectObject(
        source,
        label,
        { type: enumOf(["audio"]), url: string },
        ["type", "url"],
        options,
      );
    case "localAudio":
      return projectObject(
        source,
        label,
        { type: enumOf(["localAudio"]), path: string },
        ["type", "path"],
        options,
      );
    case "skill":
    case "mention":
      return projectObject(
        source,
        label,
        { type: enumOf([type]), name: string, path: string },
        ["type", "name", "path"],
        options,
      );
    default:
      contract(label);
  }
}

function projectDynamicToolFunction(value, label, strict = true) {
  return projectObject(
    value,
    label,
    {
      type: enumOf(["function"]),
      name: string,
      description: string,
      inputSchema: json,
      deferLoading: boolean,
    },
    ["type", "name", "description", "inputSchema"],
    { strict },
  );
}

function projectDynamicToolSpec(value, label, strict = true) {
  const source = object(value, label);
  if (source.type === "function") {
    return projectDynamicToolFunction(source, label, strict);
  }
  if (source.type === "namespace") {
    return projectObject(
      source,
      label,
      {
        type: enumOf(["namespace"]),
        name: string,
        description: string,
        tools: arrayOf((entry, childLabel) => projectDynamicToolFunction(entry, childLabel, strict)),
      },
      ["type", "name", "description", "tools"],
      { strict },
    );
  }
  contract(label);
}

function projectTurnSandboxPolicy(value, label, strict = false) {
  const source = object(value, label);
  switch (source.type) {
    case "readOnly":
      return projectObject(
        source,
        label,
        { type: enumOf(["readOnly"]), networkAccess: boolean },
        ["type"],
        { strict },
      );
    case "workspaceWrite":
      return projectObject(
        source,
        label,
        {
          type: enumOf(["workspaceWrite"]),
          writableRoots: arrayOf(string),
          networkAccess: boolean,
          excludeSlashTmp: boolean,
          excludeTmpdirEnvVar: boolean,
        },
        ["type"],
        { strict },
      );
    case "dangerFullAccess":
      return projectObject(
        source,
        label,
        { type: enumOf(["dangerFullAccess"]) },
        ["type"],
        { strict },
      );
    case "externalSandbox":
      return projectObject(
        source,
        label,
        { type: enumOf(["externalSandbox"]), networkAccess: enumOf(["restricted", "enabled"]) },
        ["type"],
        { strict },
      );
    default:
      contract(label);
  }
}

function projectAdditionalContextEntry(value, label) {
  return projectObject(
    value,
    label,
    { kind: enumOf(["application", "untrusted"]), value: string },
    ["kind", "value"],
    { strict: true },
  );
}

function projectFileSystemPath(value, label) {
  const source = object(value, label);
  switch (source.type) {
    case "path":
      return projectObject(source, label, { type: enumOf(["path"]), path: string }, ["type", "path"]);
    case "glob_pattern":
      return projectObject(
        source,
        label,
        { type: enumOf(["glob_pattern"]), pattern: string },
        ["type", "pattern"],
      );
    case "special":
      return projectObject(
        source,
        label,
        {
          type: enumOf(["special"]),
          value: (entry, childLabel) =>
            projectObject(
              entry,
              childLabel,
              { kind: enumOf(["root", "minimal", "project_roots", "tmpdir", "slash_tmp"]), subpath: nullable(string) },
              ["kind"],
            ),
        },
        ["type", "value"],
      );
    default:
      contract(label);
  }
}

function projectPermissionProfile(value, label, strict = false) {
  return projectObject(
    value,
    label,
    {
      fileSystem: nullable((entry, childLabel) =>
        projectObject(
          entry,
          childLabel,
          {
            entries: nullable(
              arrayOf((item, itemLabel) =>
                projectObject(
                  item,
                  itemLabel,
                  { access: enumOf(["deny", "read", "write"]), path: projectFileSystemPath },
                  ["access", "path"],
                ),
              ),
            ),
            globScanMaxDepth: nullable(unsigned),
            read: nullable(arrayOf(string)),
            write: nullable(arrayOf(string)),
          },
          [],
          { strict },
        )),
      network: nullable((entry, childLabel) =>
        projectObject(entry, childLabel, { enabled: nullable(boolean) }, [], { strict })),
    },
    [],
    { strict },
  );
}

function projectThreadStatus(value, label) {
  const source = object(value, label);
  if (["notLoaded", "idle", "systemError"].includes(source.type)) {
    return { type: source.type };
  }
  if (source.type === "active") {
    const activeFlags = Array.isArray(source.activeFlags)
      ? source.activeFlags.filter((entry) =>
          ["waitingOnApproval", "waitingOnUserInput"].includes(entry),
        )
      : [];
    return { type: "active", activeFlags };
  }
  return { type: "unsupported" };
}

function projectSubAgentSource(value, label) {
  if (typeof value === "string") {
    return ["compact", "memory_consolidation", "review"].includes(value) ? value : "other";
  }
  const source = object(value, label);
  if (Object.hasOwn(source, "thread_spawn")) {
    return {
      thread_spawn: projectObject(
        source.thread_spawn,
        `${label}.thread_spawn`,
        {
          depth: integer,
          parent_thread_id: string,
          agent_nickname: nullable(string),
          agent_path: nullable(string),
          agent_role: nullable(string),
        },
        ["depth", "parent_thread_id"],
      ),
    };
  }
  if (Object.hasOwn(source, "other")) {
    return { other: string(source.other, `${label}.other`) };
  }
  return { other: "unsupported" };
}

function projectThreadSource(value, label) {
  if (typeof value === "string") {
    return ["appServer", "cli", "exec", "unknown", "vscode"].includes(value)
      ? value
      : "unknown";
  }
  const source = object(value, label);
  if (Object.hasOwn(source, "custom")) {
    return { custom: string(source.custom, `${label}.custom`) };
  }
  if (Object.hasOwn(source, "subAgent")) {
    return { subAgent: projectSubAgentSource(source.subAgent, `${label}.subAgent`) };
  }
  return "unknown";
}

function projectMemoryCitation(value, label) {
  return projectObject(
    value,
    label,
    {
      threadIds: arrayOf(string),
      entries: arrayOf((entry, childLabel) =>
        projectObject(
          entry,
          childLabel,
          { path: string, lineStart: unsigned, lineEnd: unsigned, note: string },
          ["path", "lineStart", "lineEnd", "note"],
        )),
    },
    ["threadIds", "entries"],
  );
}

function unsupportedItem(source, label) {
  return {
    type: "unsupported",
    id: string(source.id, `${label}.id`),
    reviewedKind: string(source.type, `${label}.type`),
  };
}

function projectCommandAction(value, label) {
  const source = object(value, label);
  switch (source.type) {
    case "read":
      return projectObject(source, label, { type: enumOf(["read"]), command: string, name: string, path: string }, ["type", "command", "name", "path"]);
    case "listFiles":
      return projectObject(source, label, { type: enumOf(["listFiles"]), command: string, path: nullable(string) }, ["type", "command"]);
    case "search":
      return projectObject(source, label, { type: enumOf(["search"]), command: string, path: nullable(string), query: nullable(string) }, ["type", "command"]);
    case "unknown":
      return projectObject(source, label, { type: enumOf(["unknown"]), command: string }, ["type", "command"]);
    default:
      return { type: "unknown", command: "unsupported" };
  }
}

function projectPatchKind(value, label) {
  const source = object(value, label);
  if (source.type === "add" || source.type === "delete") {
    return { type: source.type };
  }
  if (source.type === "update") {
    const projected = { type: "update" };
    if (Object.hasOwn(source, "move_path")) {
      projected.move_path = nullable(string)(source.move_path, `${label}.move_path`);
    }
    return projected;
  }
  contract(label);
}

function projectMcpContent(value, label) {
  const source = object(value, label);
  const type = string(source.type, `${label}.type`);
  switch (type) {
    case "text":
      return projectObject(source, label, { type: enumOf(["text"]), text: string }, ["type", "text"]);
    case "image":
      return projectObject(source, label, { type: enumOf(["image"]), data: string, mimeType: string }, ["type", "data", "mimeType"]);
    case "audio":
      return projectObject(source, label, { type: enumOf(["audio"]), data: string, mimeType: string }, ["type", "data", "mimeType"]);
    case "resource_link":
      return projectObject(
        source,
        label,
        { type: enumOf(["resource_link"]), name: string, title: string, uri: string, description: string, mimeType: string, size: unsigned },
        ["type", "name", "uri"],
      );
    case "resource":
      return projectObject(
        source,
        label,
        {
          type: enumOf(["resource"]),
          resource: (entry, childLabel) =>
            projectObject(entry, childLabel, { uri: string, mimeType: string, text: string, blob: string }, ["uri"]),
        },
        ["type", "resource"],
      );
    default:
      return { type: "unsupported" };
  }
}

function projectDynamicOutput(value, label, strict = false) {
  const source = object(value, label);
  switch (source.type) {
    case "inputText":
      return projectObject(source, label, { type: enumOf(["inputText"]), text: string }, ["type", "text"], { strict });
    case "inputImage":
      return projectObject(source, label, { type: enumOf(["inputImage"]), imageUrl: string }, ["type", "imageUrl"], { strict });
    case "inputAudio":
      return projectObject(source, label, { type: enumOf(["inputAudio"]), audioUrl: string }, ["type", "audioUrl"], { strict });
    default:
      contract(label);
  }
}

const collabTools = new Set(["closeAgent", "resumeAgent", "sendInput", "spawnAgent", "wait"]);
const collabStatuses = new Set(["completed", "failed", "inProgress"]);
const agentStatuses = new Set([
  "completed",
  "errored",
  "interrupted",
  "notFound",
  "pendingInit",
  "running",
  "shutdown",
]);

function projectThreadItem(value, label, allowFunctionCallOutput) {
  const source = object(value, label);
  const type = string(source.type, `${label}.type`);
  switch (type) {
    case "userMessage":
      return projectObject(
        source,
        label,
        { type: enumOf([type]), id: string, content: arrayOf((entry, childLabel) => projectUserInput(entry, childLabel, false)), clientId: nullable(string) },
        ["type", "id", "content"],
      );
    case "agentMessage":
      return projectObject(
        source,
        label,
        { type: enumOf([type]), id: string, text: string, phase: nullable(enumOf(["commentary", "final_answer"])), memoryCitation: nullable(projectMemoryCitation) },
        ["type", "id", "text"],
      );
    case "plan":
      return projectObject(source, label, { type: enumOf([type]), id: string, text: string }, ["type", "id", "text"]);
    case "reasoning":
      return projectObject(source, label, { type: enumOf([type]), id: string, summary: arrayOf(string), content: arrayOf(string) }, ["type", "id"]);
    case "hookPrompt":
      return projectObject(
        source,
        label,
        { type: enumOf([type]), id: string, fragments: arrayOf((entry, childLabel) => projectObject(entry, childLabel, { hookRunId: string, text: string }, ["hookRunId", "text"])) },
        ["type", "id", "fragments"],
      );
    case "commandExecution": {
      const statuses = ["completed", "declined", "failed", "inProgress"];
      if (!statuses.includes(source.status)) {
        return unsupportedItem(source, label);
      }
      return projectObject(
        source,
        label,
        {
          type: enumOf([type]), id: string, command: string, commandActions: arrayOf(projectCommandAction), cwd: string,
          status: enumOf(statuses), aggregatedOutput: nullable(string), durationMs: nullable(integer), exitCode: nullable(integer),
          processId: nullable(string), pluginId: nullable(string), scriptPath: nullable(string),
          source: enumOf(["agent", "unifiedExecInteraction", "unifiedExecStartup", "userShell"]),
        },
        ["type", "id", "command", "commandActions", "cwd", "status"],
      );
    }
    case "fileChange":
      if (!["completed", "declined", "failed", "inProgress"].includes(source.status)) {
        return unsupportedItem(source, label);
      }
      return projectObject(
        source,
        label,
        {
          type: enumOf([type]), id: string, status: enumOf(["completed", "declined", "failed", "inProgress"]),
          changes: arrayOf((entry, childLabel) => projectObject(entry, childLabel, { path: string, kind: projectPatchKind, diff: string }, ["path", "kind", "diff"])),
        },
        ["type", "id", "status", "changes"],
      );
    case "mcpToolCall":
      if (!["completed", "failed", "inProgress"].includes(source.status)) {
        return unsupportedItem(source, label);
      }
      return projectObject(
        source,
        label,
        {
          type: enumOf([type]), id: string, server: string, tool: string, arguments: json,
          status: enumOf(["completed", "failed", "inProgress"]), durationMs: nullable(integer), readOnlyHint: nullable(boolean),
          error: nullable((entry, childLabel) => projectObject(entry, childLabel, { message: string }, ["message"])),
          result: nullable((entry, childLabel) => projectObject(entry, childLabel, { content: arrayOf(projectMcpContent) }, ["content"])),
          appContext: nullable((entry, childLabel) => projectObject(entry, childLabel, { connectorId: string, actionName: nullable(string), appName: nullable(string), linkId: nullable(string), resourceUri: nullable(string) }, ["connectorId"])),
          mcpAppResourceUri: nullable(string), pluginId: nullable(string),
        },
        ["type", "id", "server", "tool", "arguments", "status"],
      );
    case "dynamicToolCall":
      if (!["completed", "failed", "inProgress"].includes(source.status)) {
        return unsupportedItem(source, label);
      }
      return projectObject(
        source,
        label,
        {
          type: enumOf([type]), id: string, tool: string, namespace: nullable(string), arguments: json,
          status: enumOf(["completed", "failed", "inProgress"]), contentItems: nullable(arrayOf(projectDynamicOutput)),
          durationMs: nullable(integer), success: nullable(boolean),
        },
        ["type", "id", "tool", "arguments", "status"],
      );
    case "collabAgentToolCall": {
      if (!collabTools.has(source.tool) || !collabStatuses.has(source.status)) {
        return unsupportedItem(source, label);
      }
      const states = object(source.agentsStates, `${label}.agentsStates`);
      if (Object.values(states).some((entry) => !isPlainObject(entry) || !agentStatuses.has(entry.status))) {
        return unsupportedItem(source, label);
      }
      return projectObject(
        source,
        label,
        {
          type: enumOf([type]), id: string, tool: enumOf([...collabTools]), status: enumOf([...collabStatuses]),
          senderThreadId: string, receiverThreadIds: arrayOf(string),
          agentsStates: recordOf((entry, childLabel) => projectObject(entry, childLabel, { status: enumOf([...agentStatuses]), message: nullable(string) }, ["status"])),
          model: nullable(string), prompt: nullable(string), reasoningEffort: nullable(nonemptyString),
        },
        ["type", "id", "tool", "status", "senderThreadId", "receiverThreadIds", "agentsStates"],
      );
    }
    case "subAgentActivity":
      if (!["interacted", "interrupted", "started"].includes(source.kind)) {
        return unsupportedItem(source, label);
      }
      return projectObject(
        source,
        label,
        { type: enumOf([type]), id: string, agentPath: string, agentThreadId: string, kind: enumOf(["interacted", "interrupted", "started"]) },
        ["type", "id", "agentPath", "agentThreadId", "kind"],
      );
    case "webSearch":
      return projectObject(source, label, { type: enumOf([type]), id: string, query: string }, ["type", "id", "query"]);
    case "imageView":
      return projectObject(source, label, { type: enumOf([type]), id: string, path: string }, ["type", "id", "path"]);
    case "sleep":
      return projectObject(source, label, { type: enumOf([type]), id: string, durationMs: unsigned }, ["type", "id", "durationMs"]);
    case "imageGeneration":
      return projectObject(
        source,
        label,
        { type: enumOf([type]), id: string, result: string, status: string, revisedPrompt: nullable(string), savedPath: nullable(string), transparentBackground: nullable(boolean) },
        ["type", "id", "result", "status"],
      );
    case "enteredReviewMode":
    case "exitedReviewMode":
      return projectObject(source, label, { type: enumOf([type]), id: string, review: string }, ["type", "id", "review"]);
    case "contextCompaction":
      return projectObject(source, label, { type: enumOf([type]), id: string }, ["type", "id"]);
    case "functionCallOutput":
      if (allowFunctionCallOutput) {
        return { type, id: string(source.id, `${label}.id`) };
      }
      return unsupportedItem(source, label);
    default:
      return unsupportedItem(source, label);
  }
}

const stableErrorCodes = new Set([
  "badRequest", "contextWindowExceeded", "cyberPolicy", "internalServerError",
  "misalignmentPolicyViolation", "other", "sandboxError", "serverOverloaded",
  "sessionBudgetExceeded", "threadRollbackFailed", "unauthorized", "usageLimitExceeded",
  "httpConnectionFailed", "responseStreamConnectionFailed", "responseStreamDisconnected",
  "responseTooManyFailedAttempts", "activeTurnNotSteerable",
]);

function projectCodexErrorInfo(value, label) {
  if (value === null) {
    return null;
  }
  const rateLimited =
    value === "rateLimitExceeded" ||
    (isPlainObject(value) &&
      (value.type === "rateLimitExceeded" || Object.hasOwn(value, "rateLimitExceeded")));
  if (rateLimited) {
    return { source: "upstream", category: "capacity", retryable: true };
  }
  if (typeof value === "string") {
    return stableErrorCodes.has(value) ? value : "other";
  }
  const source = object(value, label);
  if (typeof source.type === "string") {
    return { type: stableErrorCodes.has(source.type) ? source.type : "other" };
  }
  for (const type of stableErrorCodes) {
    if (Object.hasOwn(source, type)) {
      const details = isPlainObject(source[type]) ? source[type] : {};
      const projected = {};
      if (Number.isSafeInteger(details.httpStatusCode) && details.httpStatusCode >= 0) {
        projected.httpStatusCode = details.httpStatusCode;
      }
      return { [type]: projected };
    }
  }
  return { type: "other" };
}

function projectTurnError(value, label) {
  return projectObject(
    value,
    label,
    {
      // Provider error text and additionalDetails may contain prompts, paths,
      // or credentials. Preserve only a content-free stable classification.
      message: (entry, childLabel) => {
        string(entry, childLabel);
        return "upstream turn failed";
      },
      codexErrorInfo: projectCodexErrorInfo,
    },
    ["message"],
  );
}

function projectTurn(value, label, allowFunctionCallOutput) {
  return projectObject(
    value,
    label,
    {
      id: string,
      items: arrayOf((entry, childLabel) => projectThreadItem(entry, childLabel, allowFunctionCallOutput)),
      status: enumOf(["completed", "interrupted", "failed", "inProgress"]),
      startedAt: nullable(integer), completedAt: nullable(integer), durationMs: nullable(integer),
      error: nullable(projectTurnError), itemsView: turnItemsView,
    },
    ["id", "items", "status"],
  );
}

function projectThread(value, label, allowFunctionCallOutput) {
  return projectObject(
    value,
    label,
    {
      id: string, sessionId: string, preview: string, modelProvider: string,
      createdAt: integer, updatedAt: integer, status: projectThreadStatus, ephemeral: boolean,
      turns: arrayOf((entry, childLabel) => projectTurn(entry, childLabel, allowFunctionCallOutput)),
      source: projectThreadSource, cliVersion: string, cwd: string,
      name: nullable(string), path: nullable(string), forkedFromId: nullable(string), parentThreadId: nullable(string),
    },
    ["id", "sessionId", "preview", "modelProvider", "createdAt", "updatedAt", "status", "ephemeral", "turns", "source", "cliVersion", "cwd"],
  );
}

function projectThreadStartResult(value, label, allowFunctionCallOutput) {
  return projectObject(
    value,
    label,
    {
      thread: (entry, childLabel) => projectThread(entry, childLabel, allowFunctionCallOutput),
      approvalPolicy: projectApprovalPolicy, approvalsReviewer: approvalReviewer, cwd: string,
      model: string, modelProvider: string, sandbox: projectTurnSandboxPolicy,
      instructionSources: arrayOf(string), reasoningEffort: nullable(string), serviceTier: nullable(string),
    },
    ["thread", "approvalPolicy", "approvalsReviewer", "cwd", "model", "modelProvider", "sandbox"],
  );
}

function projectQueuedSubmission(value, label) {
  return projectObject(
    value,
    label,
    { id: string, clientUserMessageId: string, input: arrayOf((entry, childLabel) => projectUserInput(entry, childLabel, false)) },
    ["id", "clientUserMessageId", "input"],
  );
}

function projectTokenUsageBreakdown(value, label) {
  return projectObject(
    value,
    label,
    { inputTokens: integer, cachedInputTokens: integer, cacheWriteInputTokens: integer, outputTokens: integer, reasoningOutputTokens: integer, totalTokens: integer },
    ["inputTokens", "cachedInputTokens", "outputTokens", "reasoningOutputTokens", "totalTokens"],
  );
}

function projectCommandDecision(value, label) {
  if (typeof value === "string") {
    return enumOf(["accept", "acceptForSession", "decline", "cancel"])(value, label);
  }
  const source = object(value, label);
  if (Object.hasOwn(source, "acceptWithExecpolicyAmendment")) {
    return {
      acceptWithExecpolicyAmendment: projectObject(
        source.acceptWithExecpolicyAmendment,
        `${label}.acceptWithExecpolicyAmendment`,
        { execpolicy_amendment: arrayOf(string) },
        ["execpolicy_amendment"],
        { strict: true },
      ),
    };
  }
  if (Object.hasOwn(source, "applyNetworkPolicyAmendment")) {
    return {
      applyNetworkPolicyAmendment: projectObject(
        source.applyNetworkPolicyAmendment,
        `${label}.applyNetworkPolicyAmendment`,
        {
          network_policy_amendment: (entry, childLabel) =>
            projectObject(entry, childLabel, { action: enumOf(["allow", "deny"]), host: string }, ["action", "host"], { strict: true }),
        },
        ["network_policy_amendment"],
        { strict: true },
      ),
    };
  }
  contract(label);
}

function projectCommandApproval(value, label) {
  const source = object(value, label);
  const kind = Object.hasOwn(source, "kind") ? string(source.kind, `${label}.kind`) : "command";
  if (kind === "writeStdin") {
    return null;
  }
  if (kind !== "command") {
    return null;
  }
  return projectObject(
    source,
    label,
    {
      kind: () => "command",
      threadId: string, turnId: string, itemId: string, startedAtMs: integer,
      approvalId: nullable(string), command: nullable(string), cwd: nullable(string), reason: nullable(string), environmentId: nullable(string),
      autoResolutionMs: unsigned,
      availableDecisions: nullable(arrayOf(projectCommandDecision)),
      commandActions: nullable(arrayOf(projectCommandAction)),
      proposedExecpolicyAmendment: nullable(arrayOf(string)),
      proposedNetworkPolicyAmendments: nullable(arrayOf((entry, childLabel) => projectObject(entry, childLabel, { action: enumOf(["allow", "deny"]), host: string }, ["action", "host"]))),
      networkApprovalContext: nullable((entry, childLabel) => projectObject(entry, childLabel, { host: string, protocol: enumOf(["http", "https", "socks5Tcp", "socks5Udp"]) }, ["host", "protocol"])),
      additionalPermissions: nullable(projectPermissionProfile),
    },
    ["threadId", "turnId", "itemId", "startedAtMs"],
  );
}

function createStableDomain(options) {
  const allowFunctionCallOutput = options.allowFunctionCallOutput === true;
  const item = (entry, label) => projectThreadItem(entry, label, allowFunctionCallOutput);
  const turn = (entry, label) => projectTurn(entry, label, allowFunctionCallOutput);
  const thread = (entry, label) => projectThread(entry, label, allowFunctionCallOutput);
  const params = (value, label) => object(value, label, true);

  return Object.freeze({
    initializeRequest(value) {
      return projectObject(
        params(value, "initialize params"),
        "initialize params",
        {
          clientInfo: (entry, label) => projectObject(entry, label, { name: string, version: string, title: nullable(string) }, ["name", "version"], { strict: true }),
          capabilities: nullable((entry, label) => projectObject(entry, label, { experimentalApi: boolean, mcpServerOpenaiFormElicitation: boolean, optOutNotificationMethods: nullable(arrayOf(string)) }, [], { strict: true })),
        },
        ["clientInfo"],
        { strict: true },
      );
    },
    threadStartRequest(value) {
      return projectObject(
        params(value, "thread/start params"),
        "thread/start params",
        {
          sandbox: sandboxMode, approvalPolicy: projectApprovalPolicy, approvalsReviewer: approvalReviewer,
          baseInstructions: string, config: (entry, label) => object(entry, label), cwd: string,
          developerInstructions: string, dynamicTools: arrayOf(projectDynamicToolSpec), serviceTier: string,
          serviceName: string, ephemeral: boolean, personality, sessionStartSource: enumOf(["clear", "startup"]),
          threadSource: string, model: string, modelProvider: string, projectId: string,
        },
        [],
        { strict: true },
      );
    },
    threadListRequest(value) {
      return projectObject(
        params(value, "thread/list params"),
        "thread/list params",
        {
          cursor: string, limit: unsigned, sortKey: threadSortKey, sortDirection,
          modelProviders: arrayOf(string), sourceKinds: arrayOf(threadSourceKind),
          cwd: (entry, label) => typeof entry === "string" ? entry : arrayOf(string)(entry, label),
          archived: boolean, projectId: string, sectionId: string, searchTerm: string, useStateDbOnly: boolean,
        },
        [],
        { strict: true },
      );
    },
    threadReadRequest(value) {
      return projectObject(value, "thread/read params", { threadId: string, includeTurns: boolean }, ["threadId"], { strict: true });
    },
    threadResumeRequest(value) {
      return projectObject(
        value,
        "thread/resume params",
        {
          threadId: string, excludeTurns: boolean, approvalPolicy: projectApprovalPolicy, approvalsReviewer: approvalReviewer,
          baseInstructions: string, config: (entry, label) => object(entry, label), cwd: string, developerInstructions: string,
          sandbox: sandboxMode, personality, model: string, modelProvider: string, serviceTier: string,
        },
        ["threadId"],
        { strict: true },
      );
    },
    threadUnsubscribeRequest(value) {
      return projectObject(value, "thread/unsubscribe params", { threadId: string }, ["threadId"], { strict: true });
    },
    threadTurnsListRequest(value) {
      return projectObject(value, "thread/turns/list params", { threadId: string, cursor: string, limit: unsigned, sortDirection, itemsView: turnItemsView }, ["threadId"], { strict: true });
    },
    threadItemsListRequest(value) {
      return projectObject(value, "thread/items/list params", { threadId: string, turnId: string, cursor: string, limit: unsigned, sortDirection }, ["threadId"], { strict: true });
    },
    threadQueueAddRequest(value) {
      return projectObject(value, "thread/queue/add params", { threadId: string, clientUserMessageId: string, input: arrayOf(projectUserInput) }, ["threadId", "clientUserMessageId", "input"], { strict: true });
    },
    threadQueueListRequest(value) {
      return projectObject(value, "thread/queue/list params", { threadId: string, cursor: string, limit: unsigned }, ["threadId"], { strict: true });
    },
    threadQueueStartRequest(value) {
      return projectObject(value, "thread/queue/start params", { threadId: string, queuedSubmissionId: string }, ["threadId"], { strict: true });
    },
    turnStartRequest(value) {
      return projectObject(
        value,
        "turn/start params",
        {
          threadId: string, input: arrayOf(projectUserInput), sandboxPolicy: (entry, label) => projectTurnSandboxPolicy(entry, label, true),
          approvalPolicy: projectApprovalPolicy, approvalsReviewer: approvalReviewer, clientUserMessageId: string,
          summary: summaryMode, cwd: string, effort: nonemptyString, personality, model: string, serviceTier: string, outputSchema: json,
        },
        ["threadId", "input"],
        { strict: true },
      );
    },
    turnSteerRequest(value) {
      return projectObject(
        value,
        "turn/steer params",
        {
          threadId: string, expectedTurnId: string, input: arrayOf(projectUserInput),
          additionalContext: recordOf(projectAdditionalContextEntry), clientUserMessageId: string,
          responsesapiClientMetadata: recordOf(string),
        },
        ["threadId", "expectedTurnId", "input"],
        { strict: true },
      );
    },
    turnInterruptRequest(value) {
      return projectObject(value, "turn/interrupt params", { threadId: string, turnId: string }, ["threadId", "turnId"], { strict: true });
    },
    initializedNotification(value) {
      if (value === undefined || value === null) {
        return undefined;
      }
      return projectObject(value, "initialized params", {}, [], { strict: true });
    },

    initializeResponse(value) {
      return projectObject(value, "initialize response", { codexHome: string, platformFamily: string, platformOs: string, userAgent: string }, ["codexHome", "platformFamily", "platformOs", "userAgent"]);
    },
    threadStartResponse(value) { return projectThreadStartResult(value, "thread/start response", allowFunctionCallOutput); },
    threadListResponse(value) {
      return projectObject(value, "thread/list response", { data: arrayOf(thread), nextCursor: nullable(string), backwardsCursor: nullable(string) }, ["data"]);
    },
    threadReadResponse(value) { return projectObject(value, "thread/read response", { thread }, ["thread"]); },
    threadResumeResponse(value) { return projectThreadStartResult(value, "thread/resume response", allowFunctionCallOutput); },
    threadUnsubscribeResponse(value) { return projectObject(value, "thread/unsubscribe response", { status: enumOf(["notLoaded", "notSubscribed", "unsubscribed"]) }, ["status"]); },
    threadTurnsListResponse(value) { return projectObject(value, "thread/turns/list response", { data: arrayOf(turn), nextCursor: nullable(string), backwardsCursor: nullable(string) }, ["data"]); },
    threadItemsListResponse(value) {
      return projectObject(value, "thread/items/list response", { data: arrayOf((entry, label) => projectObject(entry, label, { item, turnId: string }, ["item", "turnId"])), nextCursor: nullable(string), backwardsCursor: nullable(string) }, ["data"]);
    },
    threadQueueAddResponse(value) { return projectObject(value, "thread/queue/add response", { queuedSubmission: projectQueuedSubmission }, ["queuedSubmission"]); },
    threadQueueListResponse(value) { return projectObject(value, "thread/queue/list response", { data: arrayOf(projectQueuedSubmission), nextCursor: nullable(string) }, ["data"]); },
    threadQueueStartResponse(value) { return projectObject(value, "thread/queue/start response", { turn }, ["turn"]); },
    turnStartResponse(value) { return projectObject(value, "turn/start response", { turn }, ["turn"]); },
    turnSteerResponse(value) { return projectObject(value, "turn/steer response", { turnId: string }, ["turnId"]); },
    turnInterruptResponse(value) { return projectObject(value, "turn/interrupt response", {}, [], { strict: false }); },

    accountRateLimitsUpdatedNotification(value) {
      return projectObject(
        value,
        "account/rateLimits/updated notification",
        { rateLimits: projectRateLimitSnapshot },
        ["rateLimits"],
      );
    },
    remoteControlStatusChangedNotification(value) {
      return projectObject(
        value,
        "remoteControl/status/changed notification",
        {
          environmentId: nullable(string),
          installationId: string,
          serverName: string,
          status: remoteControlStatus,
        },
        ["installationId", "serverName", "status"],
      );
    },
    threadGoalClearedNotification(value) {
      return projectObject(
        value,
        "thread/goal/cleared notification",
        { threadId: string },
        ["threadId"],
      );
    },
    threadSettingsUpdatedNotification(value) {
      return projectObject(
        value,
        "thread/settings/updated notification",
        { threadId: string, threadSettings: projectThreadSettings },
        ["threadId", "threadSettings"],
      );
    },
    threadStartedNotification(value) { return projectObject(value, "thread/started notification", { thread }, ["thread"]); },
    threadStatusChangedNotification(value) { return projectObject(value, "thread/status/changed notification", { threadId: string, status: projectThreadStatus }, ["threadId", "status"]); },
    threadQueueChangedNotification(value) { return projectObject(value, "thread/queue/changed notification", { threadId: string }, ["threadId"]); },
    turnStartedNotification(value) { return projectObject(value, "turn/started notification", { threadId: string, turn }, ["threadId", "turn"]); },
    itemStartedNotification(value) { return projectObject(value, "item/started notification", { threadId: string, turnId: string, startedAtMs: integer, item }, ["threadId", "turnId", "startedAtMs", "item"]); },
    agentMessageDeltaNotification(value) { return projectObject(value, "item/agentMessage/delta notification", { threadId: string, turnId: string, itemId: string, delta: string }, ["threadId", "turnId", "itemId", "delta"]); },
    commandOutputDeltaNotification(value) { return projectObject(value, "item/commandExecution/outputDelta notification", { threadId: string, turnId: string, itemId: string, delta: string }, ["threadId", "turnId", "itemId", "delta"]); },
    itemCompletedNotification(value) { return projectObject(value, "item/completed notification", { threadId: string, turnId: string, completedAtMs: integer, item }, ["threadId", "turnId", "completedAtMs", "item"]); },
    tokenUsageNotification(value) {
      return projectObject(value, "thread/tokenUsage/updated notification", { threadId: string, turnId: string, tokenUsage: (entry, label) => projectObject(entry, label, { total: projectTokenUsageBreakdown, last: projectTokenUsageBreakdown, modelContextWindow: nullable(integer) }, ["total", "last"]) }, ["threadId", "turnId", "tokenUsage"]);
    },
    serverRequestResolvedNotification(value) { return projectObject(value, "serverRequest/resolved notification", { threadId: string, requestId: json }, ["threadId", "requestId"]); },
    errorNotification(value) { return projectObject(value, "error notification", { threadId: string, turnId: string, error: projectTurnError, willRetry: boolean }, ["threadId", "turnId", "error", "willRetry"]); },
    turnCompletedNotification(value) { return projectObject(value, "turn/completed notification", { threadId: string, turn }, ["threadId", "turn"]); },

    dynamicToolRequest(value) { return projectObject(value, "item/tool/call params", { threadId: string, turnId: string, callId: string, namespace: nullable(string), tool: string, arguments: json }, ["threadId", "turnId", "callId", "tool", "arguments"]); },
    commandApprovalRequest(value) { return projectCommandApproval(value, "item/commandExecution/requestApproval params"); },
    fileApprovalRequest(value) { return projectObject(value, "item/fileChange/requestApproval params", { threadId: string, turnId: string, itemId: string, startedAtMs: integer, grantRoot: nullable(string), reason: nullable(string), autoResolutionMs: unsigned }, ["threadId", "turnId", "itemId", "startedAtMs"]); },
    permissionsApprovalRequest(value) { return projectObject(value, "item/permissions/requestApproval params", { threadId: string, turnId: string, itemId: string, startedAtMs: integer, cwd: string, permissions: projectPermissionProfile, reason: nullable(string), environmentId: nullable(string), autoResolutionMs: unsigned }, ["threadId", "turnId", "itemId", "startedAtMs", "cwd", "permissions"]); },

    dynamicToolResponse(value) { return projectObject(value, "item/tool/call response", { contentItems: arrayOf((entry, label) => projectDynamicOutput(entry, label, true)), success: boolean }, ["contentItems", "success"], { strict: true }); },
    commandApprovalResponse(value) { return projectObject(value, "item/commandExecution/requestApproval response", { decision: projectCommandDecision }, ["decision"], { strict: true }); },
    fileApprovalResponse(value) { return projectObject(value, "item/fileChange/requestApproval response", { decision: enumOf(["accept", "acceptForSession", "decline", "cancel"]) }, ["decision"], { strict: true }); },
    permissionsApprovalResponse(value) { return projectObject(value, "item/permissions/requestApproval response", { permissions: (entry, label) => projectPermissionProfile(entry, label, true), scope: enumOf(["turn", "session"]), strictAutoReview: nullable(boolean) }, ["permissions"], { strict: true }); },
  });
}

module.exports = { createStableDomain };
