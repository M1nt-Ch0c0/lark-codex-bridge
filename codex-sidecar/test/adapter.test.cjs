"use strict";

const assert = require("node:assert/strict");
const test = require("node:test");

const { adapterForVersion } = require("../adapters/index.cjs");
const { SidecarError } = require("../wire.cjs");

function turnWith(items, additions = {}) {
  return {
    id: "turn-1",
    items,
    status: "inProgress",
    startedAt: 100,
    completedAt: null,
    durationMs: null,
    error: null,
    ...additions,
  };
}

function threadWith(items, additions = {}) {
  return {
    id: "thread-1",
    sessionId: "session-1",
    preview: "",
    modelProvider: "openai",
    createdAt: 99,
    updatedAt: 100,
    status: { type: "active", activeFlags: ["waitingOnApproval", "futureFlag"] },
    ephemeral: false,
    turns: [turnWith(items)],
    source: "appServer",
    cliVersion: "0.151.0",
    cwd: "/workspace",
    ...additions,
  };
}

function isContractError(error) {
  return error instanceof SidecarError && error.code === "adapter_contract";
}

for (const version of ["0.149.0", "0.151.0"]) {
  test(`${version} rejects unpromoted local fields and enum drift`, () => {
    const adapter = adapterForVersion(version);
    for (const [method, params] of [
      ["thread/list", { isPinned: true }],
      ["thread/list", { sortKey: "future_sort" }],
      ["thread/list", { sourceKinds: ["futureSource"] }],
      ["thread/start", { approvalPolicy: "future-policy" }],
      ["thread/start", { approvalsReviewer: "future-reviewer" }],
      ["thread/start", { sessionStartSource: "future-source" }],
      ["thread/resume", { threadId: "thread-1", history: [] }],
      ["thread/turns/list", { threadId: "thread-1", sortDirection: "future" }],
      ["thread/items/list", { threadId: "thread-1", sortDirection: "future" }],
      ["turn/start", { threadId: "thread-1", input: [], effort: "" }],
      ["turn/start", { threadId: "thread-1", input: [], summary: "future" }],
      ["turn/start", { threadId: "thread-1", input: [], turnTrigger: "provider-only" }],
      ["turn/start", { threadId: "thread-1", input: [], toolOutput: { secret: true } }],
    ]) {
      assert.throws(() => adapter.toUpstreamRequest(method, params), isContractError);
    }
  });

  test(`${version} projects localAudio in outgoing input and incoming history`, () => {
    const adapter = adapterForVersion(version);
    const localAudio = { type: "localAudio", path: "/workspace/input.wav" };
    const outgoing = adapter.toUpstreamRequest("turn/start", {
      threadId: "thread-1",
      input: [localAudio],
    });
    assert.deepEqual(outgoing.params.input, [localAudio]);

    const incoming = adapter.fromUpstreamResponse("thread/read", {
      thread: threadWith([
        {
          type: "userMessage",
          id: "user-message-1",
          content: [{ ...localAudio, providerSecret: "must-not-cross" }],
        },
      ]),
    });
    assert.deepEqual(incoming.thread.turns[0].items[0].content, [localAudio]);
  });
}

const PROMOTED_REQUEST_METHODS = [
  "initialize",
  "thread/items/list",
  "thread/list",
  "thread/queue/add",
  "thread/queue/list",
  "thread/queue/start",
  "thread/read",
  "thread/resume",
  "thread/start",
  "thread/turns/list",
  "thread/unsubscribe",
  "turn/interrupt",
  "turn/start",
  "turn/steer",
];
const PROMOTED_NOTIFICATION_METHODS = [
  "account/rateLimits/updated",
  "error",
  "item/agentMessage/delta",
  "item/commandExecution/outputDelta",
  "item/completed",
  "item/started",
  "remoteControl/status/changed",
  "serverRequest/resolved",
  "thread/goal/cleared",
  "thread/queue/changed",
  "thread/settings/updated",
  "thread/started",
  "thread/status/changed",
  "thread/tokenUsage/updated",
  "turn/completed",
  "turn/started",
];
const PROMOTED_SERVER_REQUEST_METHODS = [
  "item/commandExecution/requestApproval",
  "item/fileChange/requestApproval",
  "item/permissions/requestApproval",
  "item/tool/call",
];

for (const version of ["0.149.0", "0.151.0"]) {
  test(`${version} declares the complete reviewed bridge surface`, () => {
    const adapter = adapterForVersion(version);
    assert.deepEqual(adapter.requestMethods, PROMOTED_REQUEST_METHODS);
    assert.deepEqual(adapter.notificationMethods, PROMOTED_NOTIFICATION_METHODS);
    assert.deepEqual(adapter.serverRequestMethods, PROMOTED_SERVER_REQUEST_METHODS);
    assert.deepEqual(adapter.localNotificationMethods, ["initialized"]);
  });

  test(`${version} recursively projects shared reconciliation notifications`, () => {
    const adapter = adapterForVersion(version);
    const rateLimits = adapter.fromUpstreamNotification("account/rateLimits/updated", {
      rateLimits: {
        credits: {
          balance: "12.50",
          hasCredits: true,
          unlimited: false,
          providerSecret: "must-not-cross",
        },
        individualLimit: {
          limit: "100.00",
          remainingPercent: 75,
          resetsAt: 1_786_478_400,
          used: "25.00",
          providerSecret: "must-not-cross",
        },
        limitId: "limit-1",
        limitName: "primary",
        planType: "plus",
        primary: {
          usedPercent: 25,
          resetsAt: 1_786_478_400,
          windowDurationMins: 300,
          providerSecret: "must-not-cross",
        },
        rateLimitReachedType: null,
        secondary: null,
        spendControlReached: false,
        providerSecret: "must-not-cross",
      },
      providerEnvelope: "must-not-cross",
    });
    assert.deepEqual(rateLimits.params, {
      rateLimits: {
        credits: { balance: "12.50", hasCredits: true, unlimited: false },
        individualLimit: {
          limit: "100.00",
          remainingPercent: 75,
          resetsAt: 1_786_478_400,
          used: "25.00",
        },
        limitId: "limit-1",
        limitName: "primary",
        planType: "plus",
        primary: {
          resetsAt: 1_786_478_400,
          usedPercent: 25,
          windowDurationMins: 300,
        },
        rateLimitReachedType: null,
        secondary: null,
        spendControlReached: false,
      },
    });

    assert.deepEqual(
      adapter.fromUpstreamNotification("remoteControl/status/changed", {
        environmentId: null,
        installationId: "installation-1",
        serverName: "remote-control",
        status: "connected",
        providerSecret: "must-not-cross",
      }).params,
      {
        environmentId: null,
        installationId: "installation-1",
        serverName: "remote-control",
        status: "connected",
      },
    );
    assert.deepEqual(
      adapter.fromUpstreamNotification("thread/goal/cleared", {
        threadId: "thread-1",
        providerSecret: "must-not-cross",
      }).params,
      { threadId: "thread-1" },
    );

    const settings = adapter.fromUpstreamNotification("thread/settings/updated", {
      threadId: "thread-1",
      threadSettings: {
        activePermissionProfile: { id: "provider-only" },
        approvalPolicy: "on-request",
        approvalsReviewer: "user",
        collaborationMode: {
          mode: "default",
          settings: {
            developer_instructions: null,
            model: "gpt-test",
            reasoning_effort: "high",
            providerSecret: "must-not-cross",
          },
          providerSecret: "must-not-cross",
        },
        cwd: "/workspace",
        effort: "high",
        model: "gpt-test",
        modelProvider: "openai",
        multiAgentMode: "explicitRequestOnly",
        personality: "pragmatic",
        sandboxPolicy: {
          type: "workspaceWrite",
          writableRoots: ["/workspace"],
          networkAccess: false,
          excludeSlashTmp: false,
          excludeTmpdirEnvVar: false,
          providerSecret: "must-not-cross",
        },
        serviceTier: null,
        summary: "auto",
      },
      providerEnvelope: "must-not-cross",
    });
    assert.deepEqual(settings.params, {
      threadId: "thread-1",
      threadSettings: {
        approvalPolicy: "on-request",
        approvalsReviewer: "user",
        collaborationMode: {
          mode: "default",
          settings: {
            developer_instructions: null,
            model: "gpt-test",
            reasoning_effort: "high",
          },
        },
        cwd: "/workspace",
        model: "gpt-test",
        modelProvider: "openai",
        sandboxPolicy: {
          type: "workspaceWrite",
          writableRoots: ["/workspace"],
          networkAccess: false,
          excludeSlashTmp: false,
          excludeTmpdirEnvVar: false,
        },
      },
    });

    assert.throws(
      () =>
        adapter.fromUpstreamNotification("remoteControl/status/changed", {
          installationId: "installation-1",
          serverName: "remote-control",
          status: "future-status",
        }),
      isContractError,
    );
    assert.throws(
      () =>
        adapter.fromUpstreamNotification("thread/settings/updated", {
          threadId: "thread-1",
          threadSettings: {
            approvalPolicy: "on-request",
            approvalsReviewer: "future-reviewer",
            collaborationMode: { mode: "default", settings: { model: "gpt-test" } },
            cwd: "/workspace",
            model: "gpt-test",
            modelProvider: "openai",
            sandboxPolicy: { type: "readOnly" },
          },
        }),
      isContractError,
    );
  });
}

test("incoming Thread, Turn, TurnError, and known item objects are recursively allowlisted", () => {
  const adapter = adapterForVersion("0.151.0");
  const result = adapter.fromUpstreamResponse("thread/read", {
    thread: threadWith(
      [
        {
          type: "agentMessage",
          id: "item-1",
          text: "safe",
          phase: "final_answer",
          providerSecret: "must-not-cross",
        },
      ],
      {
        projectId: "provider-only",
        futureThreadField: { secret: "must-not-cross" },
      },
    ),
    providerEnvelope: "must-not-cross",
  });
  assert.equal(result.thread.projectId, undefined);
  assert.equal(result.thread.futureThreadField, undefined);
  assert.equal(result.providerEnvelope, undefined);
  assert.equal(result.thread.turns[0].items[0].providerSecret, undefined);
  assert.deepEqual(result.thread.status, {
    type: "active",
    activeFlags: ["waitingOnApproval"],
  });

  const failed = adapter.fromUpstreamNotification("turn/completed", {
    threadId: "thread-1",
    turn: turnWith([], {
      status: "failed",
      futureTurnField: "must-not-cross",
      error: {
        message: "safe classification message",
        additionalDetails: "reviewed details",
        misalignment: { prompt: "must-not-cross" },
        futureErrorField: "must-not-cross",
      },
    }),
  });
  assert.deepEqual(failed.params.turn.error, {
    message: "upstream turn failed",
  });
  assert.equal(failed.params.turn.futureTurnField, undefined);
});

test("0.151.0 projects functionCallOutput everywhere to only type and id", () => {
  const adapter = adapterForVersion("0.151.0");
  const raw = {
    type: "functionCallOutput",
    id: "function-output-1",
    name: "private_function",
    namespace: "private_namespace",
    output: { secret: "must-not-cross" },
  };
  const read = adapter.fromUpstreamResponse("thread/read", {
    thread: threadWith([raw]),
  });
  assert.deepEqual(read.thread.turns[0].items[0], {
    type: "functionCallOutput",
    id: "function-output-1",
  });
  const started = adapter.fromUpstreamNotification("item/started", {
    threadId: "thread-1",
    turnId: "turn-1",
    startedAtMs: 100_000,
    item: raw,
  });
  assert.deepEqual(started.params.item, {
    type: "functionCallOutput",
    id: "function-output-1",
  });
});

test("unsupported 0.149 items and expanded collab enums retain no raw payload", () => {
  const older = adapterForVersion("0.149.0");
  const functionOutput = older.fromUpstreamResponse("turn/start", {
    turn: turnWith([
      {
        type: "functionCallOutput",
        id: "future-1",
        output: { secret: "must-not-cross" },
      },
    ]),
  });
  assert.deepEqual(functionOutput.turn.items[0], {
    type: "unsupported",
    id: "future-1",
    reviewedKind: "functionCallOutput",
  });

  const newer = adapterForVersion("0.151.0");
  const collab = newer.fromUpstreamResponse("turn/start", {
    turn: turnWith([
      {
        type: "collabAgentToolCall",
        id: "collab-1",
        tool: "newCollabOperation",
        status: "inProgress",
        senderThreadId: "sender",
        receiverThreadIds: [],
        agentsStates: {},
        prompt: "must-not-cross",
      },
    ]),
  });
  assert.deepEqual(collab.turn.items[0], {
    type: "unsupported",
    id: "collab-1",
    reviewedKind: "collabAgentToolCall",
  });
});

test("0.151 rate-limit errors map to a content-free stable capacity classification", () => {
  const adapter = adapterForVersion("0.151.0");
  const mapped = adapter.fromUpstreamNotification("error", {
    threadId: "thread-1",
    turnId: "turn-1",
    error: {
      message: "rate limited",
      codexErrorInfo: {
        type: "rateLimitExceeded",
        privateProviderDetails: "must-not-cross",
      },
      misalignment: { privatePrompt: "must-not-cross" },
    },
    willRetry: true,
  });
  assert.deepEqual(mapped.params.error, {
    message: "upstream turn failed",
    codexErrorInfo: {
      source: "upstream",
      category: "capacity",
      retryable: true,
    },
  });
  assert.equal(JSON.stringify(mapped).includes("rateLimitExceeded"), false);
  assert.equal(JSON.stringify(mapped).includes("privateProviderDetails"), false);
});

test("command approvals default to command, strip details, and reject writeStdin", () => {
  const adapter = adapterForVersion("0.151.0");
  const base = {
    threadId: "thread-1",
    turnId: "turn-1",
    itemId: "item-1",
    startedAtMs: 100_000,
    approvalId: "approval-1",
    command: "pwd",
    reason: "reviewed",
    providerSecret: "must-not-cross",
  };
  const command = adapter.fromUpstreamServerRequest(
    "item/commandExecution/requestApproval",
    base,
  );
  assert.equal(command.method, "item/commandExecution/requestApproval");
  assert.equal(command.params.approvalId, "approval-1");
  assert.equal(command.params.command, "pwd");
  assert.equal(command.params.providerSecret, undefined);
  assert.equal(
    adapter.fromUpstreamServerRequest("item/commandExecution/requestApproval", {
      ...base,
      kind: "writeStdin",
      stdin: "must-not-cross",
    }),
    null,
  );
});

for (const version of ["0.149.0", "0.151.0"]) {
  test(`${version} maps every promoted reverse request and response`, () => {
    const adapter = adapterForVersion(version);
    const dynamic = adapter.fromUpstreamServerRequest("item/tool/call", {
      threadId: "thread-1",
      turnId: "turn-1",
      callId: "call-1",
      namespace: null,
      tool: "bridge_tool",
      arguments: { reviewed: true },
      providerSecret: "must-not-cross",
    });
    assert.deepEqual(dynamic, {
      method: "item/tool/call",
      params: {
        threadId: "thread-1",
        turnId: "turn-1",
        callId: "call-1",
        namespace: null,
        tool: "bridge_tool",
        arguments: { reviewed: true },
      },
    });

    const file = adapter.fromUpstreamServerRequest("item/fileChange/requestApproval", {
      threadId: "thread-1",
      turnId: "turn-1",
      itemId: "item-file",
      startedAtMs: 1_786_478_401_000,
      grantRoot: "/workspace",
      reason: "reviewed",
      autoResolutionMs: 30_000,
      providerSecret: "must-not-cross",
    });
    assert.equal(file.params.providerSecret, undefined);
    assert.equal(file.params.grantRoot, "/workspace");

    const permissions = adapter.fromUpstreamServerRequest(
      "item/permissions/requestApproval",
      {
        threadId: "thread-1",
        turnId: "turn-1",
        itemId: "item-permissions",
        startedAtMs: 1_786_478_401_000,
        cwd: "/workspace",
        permissions: {
          fileSystem: null,
          network: { enabled: true, providerSecret: "must-not-cross" },
          providerSecret: "must-not-cross",
        },
        reason: null,
        environmentId: null,
        autoResolutionMs: 30_000,
        providerSecret: "must-not-cross",
      },
    );
    assert.deepEqual(permissions.params.permissions, {
      fileSystem: null,
      network: { enabled: true },
    });
    assert.equal(permissions.params.providerSecret, undefined);

    assert.deepEqual(
      adapter.toUpstreamServerResponse(
        "item/tool/call",
        {
          contentItems: [{ type: "inputText", text: "safe" }],
          success: true,
        },
        undefined,
      ),
      {
        result: {
          contentItems: [{ type: "inputText", text: "safe" }],
          success: true,
        },
      },
    );
    assert.deepEqual(
      adapter.toUpstreamServerResponse(
        "item/commandExecution/requestApproval",
        { decision: "decline" },
        undefined,
      ),
      { result: { decision: "decline" } },
    );
    assert.deepEqual(
      adapter.toUpstreamServerResponse(
        "item/fileChange/requestApproval",
        { decision: "acceptForSession" },
        undefined,
      ),
      { result: { decision: "acceptForSession" } },
    );
    assert.deepEqual(
      adapter.toUpstreamServerResponse(
        "item/permissions/requestApproval",
        {
          permissions: { network: { enabled: true } },
          scope: "turn",
          strictAutoReview: null,
        },
        undefined,
      ),
      {
        result: {
          permissions: { network: { enabled: true } },
          scope: "turn",
          strictAutoReview: null,
        },
      },
    );
  });
}

test("unreviewed provider notification methods are filtered rather than renamed", () => {
  const adapter = adapterForVersion("0.151.0");
  for (const method of [
    "mcpServer/event/stream/notification",
    "thread/realtime/item/completed",
    "thread/realtime/item/started",
    "thread/realtime/item/transcript/delta",
  ]) {
    assert.equal(adapter.fromUpstreamNotification(method, { secret: "must-not-cross" }), null);
  }
});

test("0.151 initialize opts out of every additive unreviewed notification", () => {
  const adapter = adapterForVersion("0.151.0");
  const mapped = adapter.toUpstreamRequest("initialize", {
    clientInfo: { name: "bridge", version: "test" },
    capabilities: { optOutNotificationMethods: ["existing/method"] },
  });
  assert.deepEqual(mapped.params.capabilities.optOutNotificationMethods, [
    "existing/method",
    "mcpServer/event/stream/notification",
    "thread/realtime/item/completed",
    "thread/realtime/item/started",
    "thread/realtime/item/transcript/delta",
  ]);
});

test("reverse responses are strict stable-domain objects", () => {
  const adapter = adapterForVersion("0.151.0");
  assert.throws(
    () =>
      adapter.toUpstreamServerResponse(
        "item/tool/call",
        {
          contentItems: [{ type: "inputText", text: "safe", providerSecret: true }],
          success: true,
        },
        undefined,
      ),
    isContractError,
  );
});
