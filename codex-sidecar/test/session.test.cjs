"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");

const {
  configureSidecar,
  readMarker,
  send,
  temporaryMarker,
  waitForExit,
} = require("./helpers.cjs");

const SIDECAR_ROOT = path.resolve(__dirname, "..");

async function cleanupRunning(running) {
  if (running.child.exitCode === null && running.child.signalCode === null) {
    if (!running.child.stdin.destroyed && running.child.stdin.writable) {
      running.child.stdin.end();
    }
    try {
      await waitForExit(running.child, 1_000);
    } catch {
      running.child.kill("SIGKILL");
      await waitForExit(running.child, 1_000).catch(() => {});
    }
  }
  fs.rmSync(running.directory, { recursive: true, force: true });
}

async function waitForMarker(running, predicate, timeoutMs = 5_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (readMarker(running.marker).some(predicate)) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error("timed out waiting for marker evidence");
}

async function receiveUntil(inbox, predicate, maximum = 12) {
  const observed = [];
  for (let index = 0; index < maximum; index += 1) {
    const value = await inbox.nextJson();
    observed.push(value);
    if (predicate(value)) {
      return { value, observed };
    }
  }
  throw new Error("expected protocol frame was not observed");
}

function expectedTurn(status = "inProgress") {
  return {
    id: "turn-fake",
    items: [],
    status,
    startedAt: 1_786_478_401,
    completedAt: status === "completed" ? 1_786_478_402 : null,
    durationMs: status === "completed" ? 1_000 : null,
    error: null,
  };
}

function expectedThread(version) {
  return {
    id: "thread-fake",
    sessionId: "session-fake",
    preview: "",
    modelProvider: "openai",
    createdAt: 1_786_478_400,
    updatedAt: 1_786_478_400,
    status: { type: "idle" },
    ephemeral: true,
    turns: [],
    source: "appServer",
    cliVersion: version,
    cwd: SIDECAR_ROOT,
  };
}

function expectedReverseRequests() {
  return new Map([
    [
      "item/tool/call",
      {
        threadId: "thread-fake",
        turnId: "turn-fake",
        callId: "call-fake",
        namespace: "bridge_context",
        tool: "resolve",
        arguments: { id: "context-fake" },
      },
    ],
    [
      "item/commandExecution/requestApproval",
      {
        kind: "command",
        threadId: "thread-fake",
        turnId: "turn-fake",
        itemId: "command-fake",
        startedAtMs: 1_786_478_401_000,
        command: "pwd",
        cwd: SIDECAR_ROOT,
        reason: "reviewed command",
      },
    ],
    [
      "item/fileChange/requestApproval",
      {
        threadId: "thread-fake",
        turnId: "turn-fake",
        itemId: "file-fake",
        startedAtMs: 1_786_478_401_001,
        grantRoot: SIDECAR_ROOT,
        reason: "reviewed file change",
        autoResolutionMs: 30_000,
      },
    ],
    [
      "item/permissions/requestApproval",
      {
        threadId: "thread-fake",
        turnId: "turn-fake",
        itemId: "permissions-fake",
        startedAtMs: 1_786_478_401_002,
        cwd: SIDECAR_ROOT,
        permissions: { fileSystem: null, network: { enabled: true } },
        reason: null,
        environmentId: null,
        autoResolutionMs: 30_000,
      },
    ],
  ]);
}

const REVERSE_RESPONSES = new Map([
  [
    "item/tool/call",
    { contentItems: [{ type: "inputText", text: "resolved" }], success: true },
  ],
  ["item/commandExecution/requestApproval", { decision: "decline" }],
  ["item/fileChange/requestApproval", { decision: "acceptForSession" }],
  [
    "item/permissions/requestApproval",
    {
      permissions: { network: { enabled: true } },
      scope: "turn",
      strictAutoReview: null,
    },
  ],
]);

function expectedNotifications(version) {
  const item = {
    type: "agentMessage",
    id: "item-fake",
    text: "stable notification",
    phase: "commentary",
  };
  return new Map([
    [
      "account/rateLimits/updated",
      {
        rateLimits: {
          credits: { balance: "12.50", hasCredits: true, unlimited: false },
          planType: "plus",
          primary: {
            resetsAt: 1_786_478_400,
            usedPercent: 25,
            windowDurationMins: 300,
          },
          secondary: null,
          spendControlReached: false,
        },
      },
    ],
    [
      "remoteControl/status/changed",
      {
        environmentId: null,
        installationId: "installation-fake",
        serverName: "remote-fake",
        status: "connected",
      },
    ],
    ["thread/goal/cleared", { threadId: "thread-fake" }],
    [
      "thread/settings/updated",
      {
        threadId: "thread-fake",
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
          cwd: SIDECAR_ROOT,
          model: "gpt-test",
          modelProvider: "openai",
          sandboxPolicy: {
            type: "workspaceWrite",
            writableRoots: [SIDECAR_ROOT],
            networkAccess: false,
          },
        },
      },
    ],
    ["thread/started", { thread: expectedThread(version) }],
    [
      "thread/status/changed",
      {
        threadId: "thread-fake",
        status: { type: "active", activeFlags: ["waitingOnApproval"] },
      },
    ],
    ["thread/queue/changed", { threadId: "thread-fake" }],
    ["turn/started", { threadId: "thread-fake", turn: expectedTurn() }],
    [
      "turn/completed",
      { threadId: "thread-fake", turn: expectedTurn("completed") },
    ],
    [
      "item/started",
      {
        threadId: "thread-fake",
        turnId: "turn-fake",
        startedAtMs: 1_786_478_401_000,
        item,
      },
    ],
    [
      "item/agentMessage/delta",
      {
        threadId: "thread-fake",
        turnId: "turn-fake",
        itemId: "item-fake",
        delta: "agent delta",
      },
    ],
    [
      "item/commandExecution/outputDelta",
      {
        threadId: "thread-fake",
        turnId: "turn-fake",
        itemId: "command-fake",
        delta: "command delta",
      },
    ],
    [
      "item/completed",
      {
        threadId: "thread-fake",
        turnId: "turn-fake",
        completedAtMs: 1_786_478_402_000,
        item,
      },
    ],
    [
      "thread/tokenUsage/updated",
      {
        threadId: "thread-fake",
        turnId: "turn-fake",
        tokenUsage: {
          total: {
            inputTokens: 10,
            cachedInputTokens: 2,
            cacheWriteInputTokens: 1,
            outputTokens: 4,
            reasoningOutputTokens: 1,
            totalTokens: 14,
          },
          last: {
            inputTokens: 5,
            cachedInputTokens: 1,
            outputTokens: 2,
            reasoningOutputTokens: 1,
            totalTokens: 7,
          },
          modelContextWindow: 128_000,
        },
      },
    ],
    [
      "error",
      {
        threadId: "thread-fake",
        turnId: "turn-fake",
        error: { message: "upstream turn failed", codexErrorInfo: "other" },
        willRetry: false,
      },
    ],
  ]);
}

async function initialize(running) {
  send(running.child, {
    id: "initialize-matrix",
    method: "initialize",
    params: { clientInfo: { name: "bridge", version: "test" } },
  });
  const response = await receiveUntil(
    running.stdout,
    (value) => value.id === "initialize-matrix",
  );
  assert.equal(response.value.result.userAgent.startsWith("fake-codex/"), true);
  send(running.child, { method: "initialized" });
}

async function shutdown(running, id) {
  send(running.child, { id, method: "sidecar/shutdown" });
  const response = await receiveUntil(running.stdout, (value) => value.id === id, 32);
  assert.deepEqual(response.value, { id, result: {} });
  assert.equal((await waitForExit(running.child)).code, 0);
}

async function expectSidecarFailure(running, code) {
  assert.equal(await running.stderr.next(), `codex_sidecar_failure code=${code}`);
  assert.equal((await waitForExit(running.child)).code, 1);
}

for (const version of ["0.149.0", "0.151.0"]) {
  test(`stable RPC, notifications, and reverse requests work through ${version}`, async (t) => {
    const running = await configureSidecar({ version });
    t.after(() => cleanupRunning(running));
    assert.equal(running.hello.maxFrameBytes, 33_554_432);
    assert.equal(running.response.ok, true);
    assert.equal(running.response.data.upstreamVersion, version);
    assert.equal(running.response.data.adapterVersion, version);
    assert.ok(running.response.data.capabilities.includes("stable-domain-jsonrpc"));

    send(running.child, {
      id: "initialize-local",
      method: "initialize",
      params: { clientInfo: { name: "bridge", version: "test" } },
    });
    const initialized = await receiveUntil(
      running.stdout,
      (value) => value.id === "initialize-local",
    );
    assert.equal(initialized.value.result.userAgent, `fake-codex/${version}`);
    send(running.child, { method: "initialized" });

    send(running.child, {
      id: "turn-local",
      method: "turn/start",
      params: { threadId: "thread-fake", input: [{ type: "text", text: "hello" }] },
    });
    const approval = await receiveUntil(
      running.stdout,
      (value) => value.method === "item/commandExecution/requestApproval",
    );
    assert.match(approval.value.id, /^server:[a-f0-9]+:\d+$/u);
    assert.notEqual(approval.value.id, "approval-1");
    send(running.child, { id: approval.value.id, result: { decision: "decline" } });
    const resolved = await receiveUntil(
      running.stdout,
      (value) => value.method === "serverRequest/resolved",
    );
    assert.equal(resolved.value.params.threadId, "thread-fake");
    assert.equal(resolved.value.params.requestId, approval.value.id);

    running.child.stdin.end();
    const exit = await waitForExit(running.child);
    assert.equal(exit.code, 0);
    const evidence = readMarker(running.marker);
    assert.equal(evidence.filter((entry) => entry.event === "start").length, 1);
    assert.ok(
      evidence.some(
        (entry) =>
          entry.event === "request" &&
          entry.method === "initialize" &&
          /^bridge:[a-f0-9]+:\d+$/u.test(entry.id),
      ),
    );
    assert.ok(
      evidence.some(
        (entry) =>
          entry.event === "server-response" && entry.id === "approval-1" && entry.ok === true,
      ),
    );
  });

  test(`every promoted request round-trips through the ${version} process boundary`, async (t) => {
    const running = await configureSidecar({ version, mode: "parity" });
    t.after(() => cleanupRunning(running));
    const exchanges = [
      ["initialize", { clientInfo: { name: "bridge", version: "test" } }],
      ["thread/start", {}],
      ["thread/list", {}],
      ["thread/read", { threadId: "thread-fake", includeTurns: true }],
      ["thread/resume", { threadId: "thread-fake" }],
      ["thread/unsubscribe", { threadId: "thread-fake" }],
      ["thread/turns/list", { threadId: "thread-fake" }],
      ["thread/items/list", { threadId: "thread-fake" }],
      [
        "thread/queue/add",
        {
          threadId: "thread-fake",
          clientUserMessageId: "client-fake",
          input: [{ type: "text", text: "queued" }],
        },
      ],
      ["thread/queue/list", { threadId: "thread-fake" }],
      [
        "thread/queue/start",
        { threadId: "thread-fake", queuedSubmissionId: "queued-fake" },
      ],
      [
        "turn/start",
        { threadId: "thread-fake", input: [{ type: "text", text: "hello" }] },
      ],
      [
        "turn/steer",
        {
          threadId: "thread-fake",
          expectedTurnId: "turn-fake",
          input: [{ type: "text", text: "steer" }],
        },
      ],
      ["turn/interrupt", { threadId: "thread-fake", turnId: "turn-fake" }],
    ];

    for (const [index, [method, params]] of exchanges.entries()) {
      const id = `parity-${index}`;
      send(running.child, { id, method, params });
      const response = await receiveUntil(running.stdout, (value) => value.id === id);
      assert.equal(Object.hasOwn(response.value, "result"), true, method);
      if (method === "initialize") {
        send(running.child, { method: "initialized" });
      }
    }

    send(running.child, { id: "shutdown-parity", method: "sidecar/shutdown", params: {} });
    const shutdown = await receiveUntil(
      running.stdout,
      (value) => value.id === "shutdown-parity",
    );
    assert.deepEqual(shutdown.value, { id: "shutdown-parity", result: {} });
    assert.equal((await waitForExit(running.child)).code, 0);
    assert.deepEqual(
      readMarker(running.marker)
        .filter((entry) => entry.event === "request")
        .map((entry) => entry.method),
      exchanges.map(([method]) => method),
    );
  });

  test(`every promoted reverse request round-trips through the ${version} process boundary`, async (t) => {
    const running = await configureSidecar({ version, mode: "reverse-parity" });
    t.after(() => cleanupRunning(running));
    await initialize(running);

    const expected = expectedReverseRequests();
    const localIds = new Map();
    const resolvedIds = new Set();
    while (localIds.size < expected.size || resolvedIds.size < expected.size) {
      const frame = await running.stdout.nextJson();
      if (Object.hasOwn(frame, "id") && Object.hasOwn(frame, "method")) {
        assert.equal(expected.has(frame.method), true, frame.method);
        assert.deepEqual(frame.params, expected.get(frame.method), frame.method);
        assert.match(frame.id, /^server:[a-f0-9]+:\d+$/u);
        assert.equal(localIds.has(frame.method), false, frame.method);
        assert.equal([...localIds.values()].includes(frame.id), false, frame.id);
        assert.notEqual(frame.id, `reverse-${frame.method}`);
        localIds.set(frame.method, frame.id);
        send(running.child, {
          id: frame.id,
          result: REVERSE_RESPONSES.get(frame.method),
        });
      } else if (frame.method === "serverRequest/resolved") {
        assert.equal([...localIds.values()].includes(frame.params.requestId), true);
        assert.equal(resolvedIds.has(frame.params.requestId), false);
        assert.deepEqual(frame.params, {
          threadId: "thread-fake",
          requestId: frame.params.requestId,
        });
        resolvedIds.add(frame.params.requestId);
      } else {
        assert.fail(`unexpected reverse parity frame: ${JSON.stringify(frame)}`);
      }
    }

    await shutdown(running, `shutdown-reverse-${version}`);
    const serverResponses = readMarker(running.marker).filter(
      (entry) => entry.event === "server-response" && entry.method !== null,
    );
    assert.deepEqual(
      serverResponses.map((entry) => entry.id).sort(),
      ["reverse-command", "reverse-file", "reverse-permissions", "reverse-tool"],
    );
    for (const response of serverResponses) {
      assert.equal(response.ok, true);
      assert.deepEqual(response.result, REVERSE_RESPONSES.get(response.method));
    }
  });

  test(`every promoted notification projects through the ${version} process boundary`, async (t) => {
    const running = await configureSidecar({ version, mode: "notification-parity" });
    t.after(() => cleanupRunning(running));
    await initialize(running);

    const expected = expectedNotifications(version);
    const observed = new Map();
    let approvalId = null;
    while (observed.size < expected.size || approvalId === null) {
      const frame = await running.stdout.nextJson();
      if (
        frame.method === "item/commandExecution/requestApproval" &&
        Object.hasOwn(frame, "id")
      ) {
        assert.equal(approvalId, null);
        approvalId = frame.id;
        assert.match(approvalId, /^server:[a-f0-9]+:\d+$/u);
        assert.deepEqual(frame.params, {
          threadId: "thread-fake",
          turnId: "turn-fake",
          itemId: "notification-approval",
          startedAtMs: 1_786_478_401_003,
          reason: "notification correlation",
        });
        send(running.child, { id: approvalId, result: { decision: "decline" } });
        continue;
      }
      assert.equal(expected.has(frame.method), true, frame.method);
      assert.equal(observed.has(frame.method), false, frame.method);
      assert.deepEqual(frame.params, expected.get(frame.method), frame.method);
      assert.equal(JSON.stringify(frame).includes("must-not-cross"), false, frame.method);
      observed.set(frame.method, frame.params);
    }

    const resolved = await receiveUntil(
      running.stdout,
      (frame) => frame.method === "serverRequest/resolved",
      32,
    );
    assert.deepEqual(resolved.value.params, {
      threadId: "thread-fake",
      requestId: approvalId,
    });
    assert.equal(JSON.stringify(resolved.value).includes("must-not-cross"), false);

    await shutdown(running, `shutdown-notifications-${version}`);
    assert.deepEqual(
      readMarker(running.marker)
        .filter((entry) => entry.event === "notification")
        .map((entry) => entry.method),
      [...expected.keys()],
    );
    const resolution = readMarker(running.marker).find(
      (entry) => entry.event === "server-response" && entry.id === "notification-resolution",
    );
    assert.equal(resolution.method, "item/commandExecution/requestApproval");
    assert.deepEqual(resolution.result, { decision: "decline" });
  });
}

test("pending capacity fails before write and never replays a mutation", async (t) => {
  const running = await configureSidecar({ mode: "hold", maxPending: 1 });
  t.after(() => cleanupRunning(running));
  assert.equal(running.response.ok, true);
  send(running.child, { id: "first", method: "turn/start", params: { threadId: "thread", input: [] } });
  await waitForMarker(
    running,
    (entry) => entry.event === "request" && entry.method === "turn/start",
  );
  send(running.child, { id: "second", method: "turn/start", params: { threadId: "thread", input: [] } });
  const rejected = await receiveUntil(running.stdout, (value) => value.id === "second");
  assert.equal(rejected.value.error.code, -32020);

  running.child.stdin.end();
  const exit = await waitForExit(running.child);
  assert.equal(exit.code, 0);
  const requests = readMarker(running.marker).filter((entry) => entry.event === "request");
  assert.equal(requests.filter((entry) => entry.method === "turn/start").length, 1);
  assert.equal(readMarker(running.marker).filter((entry) => entry.event === "start").length, 1);
});

test("pending capacity is shared by outbound and reverse correlations", async (t) => {
  const running = await configureSidecar({ mode: "hold-with-approval", maxPending: 2 });
  t.after(() => cleanupRunning(running));
  send(running.child, {
    id: "outbound-held",
    method: "turn/start",
    params: { threadId: "thread", input: [] },
  });
  const approval = await receiveUntil(
    running.stdout,
    (value) => value.method === "item/commandExecution/requestApproval",
  );

  send(running.child, {
    id: "outbound-rejected",
    method: "turn/start",
    params: { threadId: "thread", input: [] },
  });
  const rejected = await receiveUntil(
    running.stdout,
    (value) => value.id === "outbound-rejected",
  );
  assert.equal(rejected.value.error.code, -32020);

  send(running.child, { id: approval.value.id, result: { decision: "decline" } });
  const resolved = await receiveUntil(
    running.stdout,
    (value) => value.method === "serverRequest/resolved",
  );
  assert.equal(resolved.value.params.requestId, approval.value.id);

  running.child.stdin.end();
  assert.equal((await waitForExit(running.child)).code, 0);
  const requests = readMarker(running.marker).filter(
    (entry) => entry.event === "request" && entry.method === "turn/start",
  );
  assert.equal(requests.length, 1);
});

test("explicit sidecar shutdown is correlated and bounded", async (t) => {
  const running = await configureSidecar();
  t.after(() => cleanupRunning(running));
  send(running.child, { id: "shutdown-local", method: "sidecar/shutdown", params: {} });
  const response = await running.stdout.nextJson();
  assert.deepEqual(response, { id: "shutdown-local", result: {} });
  const exit = await waitForExit(running.child);
  assert.equal(exit.code, 0);
  assert.equal(readMarker(running.marker).filter((entry) => entry.event === "start").length, 1);
});

test("sidecar shutdown rejects unknown params before acknowledging", async (t) => {
  const running = await configureSidecar();
  t.after(() => cleanupRunning(running));
  send(running.child, {
    id: "shutdown-invalid-params",
    method: "sidecar/shutdown",
    params: { futureField: true },
  });
  await expectSidecarFailure(running, "invalid_params");
});

test("sidecar shutdown rejects an active local correlation", async (t) => {
  const running = await configureSidecar({ mode: "hold" });
  t.after(() => cleanupRunning(running));
  send(running.child, {
    id: "shutdown-active",
    method: "thread/list",
    params: {},
  });
  send(running.child, { id: "shutdown-active", method: "sidecar/shutdown" });
  await expectSidecarFailure(running, "correlation_reuse");
});

test("sidecar shutdown rejects a retired local correlation", async (t) => {
  const running = await configureSidecar();
  t.after(() => cleanupRunning(running));
  send(running.child, { id: "shutdown-retired", method: "future/write", params: {} });
  assert.deepEqual(await running.stdout.nextJson(), {
    id: "shutdown-retired",
    error: { code: -32601, message: "method is not promoted by the active adapter" },
  });
  send(running.child, { id: "shutdown-retired", method: "sidecar/shutdown", params: null });
  await expectSidecarFailure(running, "correlation_reuse");
});

test("sidecar shutdown rejects an active server-local correlation", async (t) => {
  const running = await configureSidecar({ mode: "reverse-parity" });
  t.after(() => cleanupRunning(running));
  await initialize(running);
  const request = await running.stdout.nextJson();
  assert.equal(Object.hasOwn(request, "id"), true);
  assert.equal(Object.hasOwn(request, "method"), true);
  send(running.child, { id: request.id, method: "sidecar/shutdown", params: {} });
  await expectSidecarFailure(running, "correlation_reuse");
});

test("sidecar shutdown rejects a retired server-local correlation", async (t) => {
  const running = await configureSidecar({ mode: "reverse-parity" });
  t.after(() => cleanupRunning(running));
  await initialize(running);
  const request = await running.stdout.nextJson();
  assert.equal(Object.hasOwn(request, "id"), true);
  assert.equal(Object.hasOwn(request, "method"), true);
  send(running.child, {
    id: request.id,
    result: REVERSE_RESPONSES.get(request.method),
  });
  const resolved = await receiveUntil(
    running.stdout,
    (frame) =>
      frame.method === "serverRequest/resolved" && frame.params.requestId === request.id,
    16,
  );
  assert.equal(resolved.value.params.requestId, request.id);

  send(running.child, { id: request.id, method: "sidecar/shutdown", params: {} });
  await expectSidecarFailure(running, "correlation_reuse");
});

test("SIGTERM performs bounded cleanup without starting a replacement", { skip: process.platform === "win32" }, async (t) => {
  const temporary = temporaryMarker();
  t.after(() => fs.rmSync(temporary.directory, { recursive: true, force: true }));
  const running = await configureSidecar({ temporary, mode: "hold" });
  send(running.child, { id: "uncertain", method: "turn/start", params: { threadId: "thread", input: [] } });
  await new Promise((resolve) => setTimeout(resolve, 50));
  running.child.kill("SIGTERM");
  const exit = await waitForExit(running.child);
  assert.equal(exit.code, 0);
  const evidence = readMarker(running.marker);
  assert.equal(evidence.filter((entry) => entry.event === "start").length, 1);
  assert.ok(evidence.filter((entry) => entry.event === "request").length <= 1);
});
