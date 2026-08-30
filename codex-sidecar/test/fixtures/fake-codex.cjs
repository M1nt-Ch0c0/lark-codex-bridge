#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const readline = require("node:readline");

function option(name, fallback) {
  const prefix = `--fake-${name}=`;
  const found = process.argv.find((argument) => argument.startsWith(prefix));
  return found === undefined ? fallback : found.slice(prefix.length);
}

const version = option("version", "0.151.0");
const mode = option("mode", "normal");
const marker = option("marker", "");

function mark(event, details = {}) {
  if (marker.length > 0) {
    fs.appendFileSync(marker, `${JSON.stringify({ event, ...details })}\n`, "utf8");
  }
}

if (process.argv.includes("--version")) {
  if (mode === "malformed-version") {
    process.stdout.write(` codex-cli ${version}\n`);
  } else {
    process.stdout.write(`codex-cli ${version}\n`);
  }
  process.exitCode = 0;
  return;
}

if (!process.argv.includes("app-server")) {
  process.exitCode = 2;
  return;
}

mark("start", { version, mode });

if (mode === "eof") {
  setImmediate(() => process.exit(0));
} else {
  const lines = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
  let approvalSent = false;
  const pendingReverseMethods = new Map();

  function fakeTurn(status = "inProgress") {
    return {
      id: "turn-fake",
      status,
      items: [],
      startedAt: 1_786_478_401,
      completedAt: status === "completed" ? 1_786_478_402 : null,
      durationMs: status === "completed" ? 1_000 : null,
      error: null,
    };
  }

  function fakeThread() {
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
      cwd: process.cwd(),
      projectId: null,
    };
  }

  function fakeThreadSession() {
    return {
      thread: fakeThread(),
      approvalPolicy: "on-request",
      approvalsReviewer: "user",
      cwd: process.cwd(),
      model: "gpt-test",
      modelProvider: "openai",
      sandbox: { type: "workspaceWrite", writableRoots: [process.cwd()] },
      instructionSources: [],
      reasoningEffort: null,
      serviceTier: null,
    };
  }

  function fakeQueuedSubmission() {
    return {
      id: "queued-fake",
      clientUserMessageId: "client-fake",
      input: [{ type: "text", text: "queued" }],
    };
  }

  function send(value) {
    process.stdout.write(`${JSON.stringify(value)}\n`);
  }

  function sendNotification(method, params) {
    mark("notification", { method });
    send({ method, params });
  }

  function sendReverseRequest(id, method, params) {
    pendingReverseMethods.set(id, method);
    mark("server-request", { id, method });
    send({ id, method, params });
  }

  function sendReverseParity() {
    sendReverseRequest("reverse-tool", "item/tool/call", {
      threadId: "thread-fake",
      turnId: "turn-fake",
      callId: "call-fake",
      namespace: "bridge_context",
      tool: "resolve",
      arguments: { id: "context-fake" },
      providerSecret: "must-not-cross",
    });
    sendReverseRequest("reverse-command", "item/commandExecution/requestApproval", {
      kind: "command",
      threadId: "thread-fake",
      turnId: "turn-fake",
      itemId: "command-fake",
      startedAtMs: 1_786_478_401_000,
      command: "pwd",
      cwd: process.cwd(),
      reason: "reviewed command",
      providerSecret: "must-not-cross",
    });
    sendReverseRequest("reverse-file", "item/fileChange/requestApproval", {
      threadId: "thread-fake",
      turnId: "turn-fake",
      itemId: "file-fake",
      startedAtMs: 1_786_478_401_001,
      grantRoot: process.cwd(),
      reason: "reviewed file change",
      autoResolutionMs: 30_000,
      providerSecret: "must-not-cross",
    });
    sendReverseRequest("reverse-permissions", "item/permissions/requestApproval", {
      threadId: "thread-fake",
      turnId: "turn-fake",
      itemId: "permissions-fake",
      startedAtMs: 1_786_478_401_002,
      cwd: process.cwd(),
      permissions: {
        fileSystem: null,
        network: { enabled: true, providerSecret: "must-not-cross" },
        providerSecret: "must-not-cross",
      },
      reason: null,
      environmentId: null,
      autoResolutionMs: 30_000,
      providerSecret: "must-not-cross",
    });
  }

  function sendNotificationParity() {
    const item = {
      type: "agentMessage",
      id: "item-fake",
      text: "stable notification",
      phase: "commentary",
      providerSecret: "must-not-cross",
    };
    sendNotification("account/rateLimits/updated", {
      rateLimits: {
        credits: {
          balance: "12.50",
          hasCredits: true,
          unlimited: false,
          providerSecret: "must-not-cross",
        },
        planType: "plus",
        primary: {
          usedPercent: 25,
          resetsAt: 1_786_478_400,
          windowDurationMins: 300,
          providerSecret: "must-not-cross",
        },
        secondary: null,
        spendControlReached: false,
        providerSecret: "must-not-cross",
      },
      providerSecret: "must-not-cross",
    });
    sendNotification("remoteControl/status/changed", {
      environmentId: null,
      installationId: "installation-fake",
      serverName: "remote-fake",
      status: "connected",
      providerSecret: "must-not-cross",
    });
    sendNotification("thread/goal/cleared", {
      threadId: "thread-fake",
      providerSecret: "must-not-cross",
    });
    sendNotification("thread/settings/updated", {
      threadId: "thread-fake",
      threadSettings: {
        approvalPolicy: "on-request",
        approvalsReviewer: "user",
        collaborationMode: {
          mode: "default",
          settings: {
            model: "gpt-test",
            developer_instructions: null,
            reasoning_effort: "high",
            providerSecret: "must-not-cross",
          },
          providerSecret: "must-not-cross",
        },
        cwd: process.cwd(),
        model: "gpt-test",
        modelProvider: "openai",
        sandboxPolicy: {
          type: "workspaceWrite",
          writableRoots: [process.cwd()],
          networkAccess: false,
          providerSecret: "must-not-cross",
        },
        activePermissionProfile: { providerSecret: "must-not-cross" },
      },
      providerSecret: "must-not-cross",
    });
    sendNotification("thread/started", {
      thread: { ...fakeThread(), providerSecret: "must-not-cross" },
      providerSecret: "must-not-cross",
    });
    sendNotification("thread/status/changed", {
      threadId: "thread-fake",
      status: {
        type: "active",
        activeFlags: ["waitingOnApproval", "futureFlag"],
        providerSecret: "must-not-cross",
      },
      providerSecret: "must-not-cross",
    });
    sendNotification("thread/queue/changed", {
      threadId: "thread-fake",
      providerSecret: "must-not-cross",
    });
    sendNotification("turn/started", {
      threadId: "thread-fake",
      turn: { ...fakeTurn(), providerSecret: "must-not-cross" },
      providerSecret: "must-not-cross",
    });
    sendNotification("turn/completed", {
      threadId: "thread-fake",
      turn: { ...fakeTurn("completed"), providerSecret: "must-not-cross" },
      providerSecret: "must-not-cross",
    });
    sendNotification("item/started", {
      threadId: "thread-fake",
      turnId: "turn-fake",
      startedAtMs: 1_786_478_401_000,
      item,
      providerSecret: "must-not-cross",
    });
    sendNotification("item/agentMessage/delta", {
      threadId: "thread-fake",
      turnId: "turn-fake",
      itemId: "item-fake",
      delta: "agent delta",
      providerSecret: "must-not-cross",
    });
    sendNotification("item/commandExecution/outputDelta", {
      threadId: "thread-fake",
      turnId: "turn-fake",
      itemId: "command-fake",
      delta: "command delta",
      providerSecret: "must-not-cross",
    });
    sendNotification("item/completed", {
      threadId: "thread-fake",
      turnId: "turn-fake",
      completedAtMs: 1_786_478_402_000,
      item,
      providerSecret: "must-not-cross",
    });
    sendNotification("thread/tokenUsage/updated", {
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
          providerSecret: "must-not-cross",
        },
        last: {
          inputTokens: 5,
          cachedInputTokens: 1,
          outputTokens: 2,
          reasoningOutputTokens: 1,
          totalTokens: 7,
          providerSecret: "must-not-cross",
        },
        modelContextWindow: 128_000,
        providerSecret: "must-not-cross",
      },
      providerSecret: "must-not-cross",
    });
    sendNotification("error", {
      threadId: "thread-fake",
      turnId: "turn-fake",
      error: {
        message: "provider detail must not cross",
        codexErrorInfo: "other",
        additionalDetails: "must-not-cross",
      },
      willRetry: false,
      providerSecret: "must-not-cross",
    });
    sendReverseRequest("notification-resolution", "item/commandExecution/requestApproval", {
      threadId: "thread-fake",
      turnId: "turn-fake",
      itemId: "notification-approval",
      startedAtMs: 1_786_478_401_003,
      reason: "notification correlation",
    });
  }

  lines.on("line", (line) => {
    const message = JSON.parse(line);
    if (Object.hasOwn(message, "method") && Object.hasOwn(message, "id")) {
      mark("request", { method: message.method, id: message.id });
      if (mode === "hold") {
        return;
      }
      if (mode === "hold-with-approval") {
        if (message.method === "turn/start" && !approvalSent) {
          approvalSent = true;
          send({
            id: "approval-held",
            method: "item/commandExecution/requestApproval",
            params: {
              threadId: "thread-fake",
              turnId: "turn-fake",
              itemId: "item-held",
              startedAtMs: 1_786_478_401_000,
            },
          });
        }
        return;
      }
      let result;
      switch (message.method) {
        case "initialize":
          result = {
            codexHome: process.env.CODEX_HOME || process.cwd(),
            userAgent: `fake-codex/${version}`,
            platformFamily: "test",
            platformOs: process.platform,
          };
          break;
        case "thread/start":
        case "thread/resume":
          result = fakeThreadSession();
          break;
        case "thread/list":
          result = { data: [fakeThread()], nextCursor: null, backwardsCursor: null };
          break;
        case "thread/read":
          result = { thread: fakeThread() };
          break;
        case "thread/unsubscribe":
          result = { status: "unsubscribed" };
          break;
        case "thread/turns/list":
          result = { data: [fakeTurn()], nextCursor: null, backwardsCursor: null };
          break;
        case "thread/items/list":
          result = { data: [], nextCursor: null, backwardsCursor: null };
          break;
        case "thread/queue/add":
          result = { queuedSubmission: fakeQueuedSubmission() };
          break;
        case "thread/queue/list":
          result = { data: [fakeQueuedSubmission()], nextCursor: null };
          break;
        case "thread/queue/start":
          result = { turn: fakeTurn() };
          break;
        case "turn/start":
          result = { turn: fakeTurn() };
          break;
        case "turn/interrupt":
          result = {};
          break;
        case "turn/steer":
          result = { turnId: "turn-fake" };
          break;
        default:
          result = {};
          break;
      }
      send({ id: message.id, result });
      if (mode === "duplicate-response") {
        send({ id: message.id, result });
      }
      if (message.method === "turn/start" && mode !== "parity") {
        send({
          method: "turn/started",
          params: {
            threadId: "thread-fake",
            turn: result.turn,
          },
        });
        if (!approvalSent) {
          approvalSent = true;
          send({
            id: "approval-1",
            method: "item/commandExecution/requestApproval",
            params: {
              threadId: "thread-fake",
              turnId: "turn-fake",
              itemId: "item-fake",
              startedAtMs: 1_786_478_401_000,
              reason: "test",
            },
          });
          if (mode === "duplicate-server-id") {
            send({
              id: "approval-1",
              method: "item/fileChange/requestApproval",
              params: {
                threadId: "thread-fake",
                turnId: "turn-fake",
                itemId: "item-file",
                startedAtMs: 1_786_478_401_000,
                reason: "test",
              },
            });
          }
        }
      }
      return;
    }

    if (Object.hasOwn(message, "id") && (Object.hasOwn(message, "result") || Object.hasOwn(message, "error"))) {
      const method = pendingReverseMethods.get(String(message.id)) ?? null;
      pendingReverseMethods.delete(String(message.id));
      mark("server-response", {
        id: message.id,
        method,
        ok: Object.hasOwn(message, "result"),
        result: message.result,
      });
      send({
        method: "serverRequest/resolved",
        params: {
          requestId: String(message.id),
          threadId: "thread-fake",
          turnId: "turn-fake",
        },
      });
      return;
    }

    if (message.method === "initialized") {
      mark("initialized");
      if (mode === "reverse-parity") {
        sendReverseParity();
      } else if (mode === "notification-parity") {
        sendNotificationParity();
      }
    }
  });

  lines.on("close", () => {
    mark("stdin-eof");
    process.exitCode = 0;
  });
}

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.once(signal, () => {
    mark("signal", { signal });
    process.exit(0);
  });
}
