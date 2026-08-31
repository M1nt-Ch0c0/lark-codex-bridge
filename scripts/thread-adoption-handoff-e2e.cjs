#!/usr/bin/env node
"use strict";

// Real, opt-in sequential-handoff proof for Codex persisted threads.
//
// The harness deliberately owns an isolated CODEX_HOME, an isolated HOME, a
// loopback-only scripted Responses API, and every app-server process it starts.
// Its only output is a fixed-shape summary: thread IDs, paths, prompts, model
// output, provider requests, and child stderr never cross the harness boundary.

const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const fs = require("node:fs");
const http = require("node:http");
const os = require("node:os");
const path = require("node:path");
const { spawn } = require("node:child_process");

const SCHEMA = "lark-codex-bridge/thread-adoption-handoff-e2e/v1";
const DEFAULT_EXPECTED_VERSION = "0.149.0";
const RPC_TIMEOUT_MS = 15_000;
const TURN_TIMEOUT_MS = 30_000;
const PROCESS_EXIT_TIMEOUT_MS = 5_000;
const PROCESS_TREE_TIMEOUT_MS = 5_000;
const VERSION_TIMEOUT_MS = 5_000;
const HTTP_BODY_MAX_BYTES = 2 * 1024 * 1024;
const CHILD_STREAM_MAX_BYTES = 16 * 1024 * 1024;
const CHILD_LINE_MAX_BYTES = 2 * 1024 * 1024;
const MAX_NOTIFICATION_BACKLOG = 512;

class HarnessError extends Error {
  constructor(code) {
    super(code);
    this.name = "HarnessError";
    this.code = code;
  }
}

function requireCondition(condition, code) {
  if (!condition) {
    throw new HarnessError(code);
  }
}

function boundedPromise(promise, timeoutMs, code) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_, reject) => {
      timer = setTimeout(() => reject(new HarnessError(code)), timeoutMs);
      timer.unref?.();
    }),
  ]).finally(() => clearTimeout(timer));
}

function parseArguments(argv) {
  const parsed = {
    binary: process.env.CODEX_ADOPTION_HANDOFF_BINARY || "codex",
    expectedVersion:
      process.env.CODEX_ADOPTION_HANDOFF_EXPECTED_VERSION ||
      DEFAULT_EXPECTED_VERSION,
    archivedAudit: false,
    selfTest: false,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (argument === "--self-test") {
      parsed.selfTest = true;
    } else if (argument === "--audit-archived-resume") {
      parsed.archivedAudit = true;
    } else if (argument === "--binary") {
      index += 1;
      requireCondition(index < argv.length, "invalid_arguments");
      parsed.binary = argv[index];
    } else if (argument === "--expected-version") {
      index += 1;
      requireCondition(index < argv.length, "invalid_arguments");
      parsed.expectedVersion = argv[index];
    } else {
      throw new HarnessError("invalid_arguments");
    }
  }
  requireCondition(parsed.binary.length > 0, "invalid_arguments");
  requireCondition(
    !(parsed.selfTest && parsed.archivedAudit),
    "invalid_arguments",
  );
  requireCondition(
    /^\d+\.\d+\.\d+$/u.test(parsed.expectedVersion),
    "invalid_expected_version",
  );
  return parsed;
}

function marker(label) {
  return `${label}_${crypto.randomBytes(18).toString("hex")}`;
}

function containsMarker(value, expected) {
  if (typeof value === "string") {
    return value.includes(expected);
  }
  if (Array.isArray(value)) {
    return value.some((entry) => containsMarker(entry, expected));
  }
  if (value !== null && typeof value === "object") {
    return Object.values(value).some((entry) => containsMarker(entry, expected));
  }
  return false;
}

function containsObjectKey(value, expected) {
  if (Array.isArray(value)) {
    return value.some((entry) => containsObjectKey(entry, expected));
  }
  if (value !== null && typeof value === "object") {
    return (
      Object.hasOwn(value, expected) ||
      Object.values(value).some((entry) => containsObjectKey(entry, expected))
    );
  }
  return false;
}

function threadListContains(result, threadId) {
  const data = result?.data;
  requireCondition(Array.isArray(data), "thread_list_shape_invalid");
  return data.some((thread) => thread?.id === threadId);
}

function safeThreadStatus(readResult) {
  const status = readResult?.thread?.status?.type;
  return ["active", "idle", "notLoaded", "systemError"].includes(status)
    ? status
    : "unknown";
}

async function auditRequest(owner, method, params, failureCode) {
  try {
    return await owner.request(method, params);
  } catch (error) {
    if (error instanceof HarnessError && error.code === "app_server_rpc_error") {
      throw new HarnessError(failureCode);
    }
    throw error;
  }
}

function completedTurns(readResult) {
  const turns = readResult?.thread?.turns;
  if (!Array.isArray(turns)) {
    throw new HarnessError("history_shape_invalid");
  }
  return turns.filter((turn) => turn?.status === "completed").length;
}

function isolatedEnvironment(codexHome, isolatedHome) {
  const environment = {
    CODEX_HOME: codexHome,
    HOME: isolatedHome,
    NO_PROXY: "127.0.0.1,localhost",
    no_proxy: "127.0.0.1,localhost",
    RUST_BACKTRACE: "0",
  };
  for (const name of [
    "PATH",
    "SHELL",
    "LANG",
    "LC_ALL",
    "TMPDIR",
    "TEMP",
    "TMP",
    "SystemRoot",
    "WINDIR",
    "ComSpec",
    "PATHEXT",
  ]) {
    if (typeof process.env[name] === "string") {
      environment[name] = process.env[name];
    }
  }
  return environment;
}

function spawnResult(command, args, options, timeoutMs) {
  return new Promise((resolve, reject) => {
    let stdout = Buffer.alloc(0);
    let stderrBytes = 0;
    let settled = false;
    let child;
    try {
      child = spawn(command, args, {
        ...options,
        stdio: ["ignore", "pipe", "pipe"],
      });
    } catch {
      reject(new HarnessError("binary_unavailable"));
      return;
    }
    const timer = setTimeout(() => {
      if (!settled) {
        settled = true;
        child.kill("SIGKILL");
        reject(new HarnessError("version_probe_timeout"));
      }
    }, timeoutMs);
    child.on("error", () => {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        reject(new HarnessError("binary_unavailable"));
      }
    });
    child.stdout.on("data", (chunk) => {
      if (stdout.length + chunk.length > 4096) {
        if (!settled) {
          settled = true;
          clearTimeout(timer);
          child.kill("SIGKILL");
          reject(new HarnessError("version_probe_output_too_large"));
        }
        return;
      }
      stdout = Buffer.concat([stdout, chunk]);
    });
    child.stderr.on("data", (chunk) => {
      stderrBytes += chunk.length;
      if (stderrBytes > 4096 && !settled) {
        settled = true;
        clearTimeout(timer);
        child.kill("SIGKILL");
        reject(new HarnessError("version_probe_output_too_large"));
      }
    });
    child.on("close", (code, signal) => {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        resolve({ code, signal, stdout: stdout.toString("utf8").trim() });
      }
    });
  });
}

async function probeVersion(binary, environment, expectedVersion) {
  const result = await spawnResult(
    binary,
    ["--version"],
    { env: environment },
    VERSION_TIMEOUT_MS,
  );
  requireCondition(result.code === 0 && result.signal === null, "version_probe_failed");
  const match = /^codex-cli (\d+\.\d+\.\d+)$/u.exec(result.stdout);
  requireCondition(match !== null, "version_probe_shape_invalid");
  requireCondition(match[1] === expectedVersion, "version_mismatch");
  return match[1];
}

function responseSse(responseId, messageId, text) {
  const events = [
    { type: "response.created", response: { id: responseId } },
    {
      type: "response.output_item.done",
      item: {
        type: "message",
        role: "assistant",
        id: messageId,
        content: [{ type: "output_text", text }],
      },
    },
    {
      type: "response.completed",
      response: {
        id: responseId,
        usage: {
          input_tokens: 0,
          input_tokens_details: null,
          output_tokens: 0,
          output_tokens_details: null,
          total_tokens: 0,
        },
      },
    },
  ];
  return events
    .map((event) => `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`)
    .join("");
}

class ScriptedProvider {
  constructor(inputMarkers, outputMarkers) {
    this.inputMarkers = inputMarkers;
    this.outputMarkers = outputMarkers;
    this.requests = 0;
    this.currentInputObserved = [false, false];
    this.priorHistoryObservedOnSecondTurn = false;
    this.failure = null;
    this.server = null;
    this.baseUrl = null;
  }

  async start() {
    this.server = http.createServer((request, response) => {
      this.#serve(request, response).catch(() => {
        this.failure = "provider_protocol_failure";
        if (!response.headersSent) {
          response.writeHead(500, { Connection: "close" });
        }
        response.end();
      });
    });
    this.server.requestTimeout = RPC_TIMEOUT_MS;
    this.server.headersTimeout = RPC_TIMEOUT_MS;
    this.server.keepAliveTimeout = 1_000;
    await boundedPromise(
      new Promise((resolve, reject) => {
        this.server.once("error", () => reject(new HarnessError("provider_bind_failed")));
        this.server.listen(0, "127.0.0.1", resolve);
      }),
      RPC_TIMEOUT_MS,
      "provider_bind_timeout",
    );
    const address = this.server.address();
    requireCondition(
      address !== null && typeof address === "object",
      "provider_address_invalid",
    );
    this.baseUrl = `http://127.0.0.1:${address.port}/v1`;
  }

  async #serve(request, response) {
    const remote = request.socket.remoteAddress;
    if (
      request.method !== "POST" ||
      request.url !== "/v1/responses" ||
      (remote !== "127.0.0.1" && remote !== "::ffff:127.0.0.1")
    ) {
      this.failure = "provider_request_rejected";
      response.writeHead(404, { Connection: "close" });
      response.end();
      return;
    }
    const chunks = [];
    let bytes = 0;
    for await (const chunk of request) {
      bytes += chunk.length;
      if (bytes > HTTP_BODY_MAX_BYTES) {
        this.failure = "provider_request_too_large";
        response.writeHead(413, { Connection: "close" });
        response.end();
        request.destroy();
        return;
      }
      chunks.push(chunk);
    }
    const requestIndex = this.requests;
    this.requests += 1;
    if (requestIndex >= this.outputMarkers.length) {
      this.failure = "unexpected_provider_request";
      response.writeHead(409, { Connection: "close" });
      response.end();
      return;
    }
    const body = Buffer.concat(chunks).toString("utf8");
    this.currentInputObserved[requestIndex] = body.includes(
      this.inputMarkers[requestIndex],
    );
    if (requestIndex === 1) {
      this.priorHistoryObservedOnSecondTurn =
        body.includes(this.inputMarkers[0]) || body.includes(this.outputMarkers[0]);
    }
    if (!this.currentInputObserved[requestIndex]) {
      this.failure = "current_turn_missing_from_provider_request";
      response.writeHead(400, { Connection: "close" });
      response.end();
      return;
    }
    const bodySse = responseSse(
      `resp-handoff-${requestIndex + 1}`,
      `msg-handoff-${requestIndex + 1}`,
      this.outputMarkers[requestIndex],
    );
    response.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Content-Length": Buffer.byteLength(bodySse),
      Connection: "close",
    });
    response.end(bodySse);
  }

  assertComplete(expectedRequests = 2) {
    requireCondition(this.failure === null, this.failure || "provider_failed");
    requireCondition(
      this.requests === expectedRequests,
      "provider_request_count_mismatch",
    );
    requireCondition(
      this.currentInputObserved.slice(0, expectedRequests).every(Boolean),
      "provider_current_input_not_observed",
    );
  }

  async stop() {
    if (this.server === null) {
      return;
    }
    this.server.closeAllConnections?.();
    await boundedPromise(
      new Promise((resolve) => this.server.close(resolve)),
      PROCESS_EXIT_TIMEOUT_MS,
      "provider_shutdown_timeout",
    );
    this.server = null;
  }
}

function processGroupExists(groupId) {
  if (process.platform === "win32") {
    return false;
  }
  try {
    process.kill(-groupId, 0);
    return true;
  } catch (error) {
    if (error?.code === "ESRCH") {
      return false;
    }
    return true;
  }
}

async function waitForProcessGroupReap(groupId) {
  if (process.platform === "win32") {
    return;
  }
  const deadline = Date.now() + PROCESS_TREE_TIMEOUT_MS;
  while (processGroupExists(groupId)) {
    if (Date.now() >= deadline) {
      throw new HarnessError("process_tree_not_reaped");
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
}

class AppServerOwner {
  constructor(binary, environment, ownerNumber) {
    this.binary = binary;
    this.environment = environment;
    this.ownerNumber = ownerNumber;
    this.child = null;
    this.pid = null;
    this.nextId = 1;
    this.pending = new Map();
    this.notifications = [];
    this.notificationWaiters = new Set();
    this.stdoutBuffer = "";
    this.stdoutBytes = 0;
    this.stderrBytes = 0;
    this.fault = null;
    this.stopping = false;
    this.exitPromise = null;
  }

  async start() {
    try {
      this.child = spawn(this.binary, ["app-server", "--listen", "stdio://"], {
        env: this.environment,
        detached: process.platform !== "win32",
        stdio: ["pipe", "pipe", "pipe"],
        windowsHide: true,
      });
    } catch {
      throw new HarnessError("app_server_spawn_failed");
    }
    requireCondition(this.child.pid !== undefined, "app_server_pid_missing");
    this.pid = this.child.pid;
    this.exitPromise = new Promise((resolve) => {
      this.child.once("close", (code, signal) => resolve({ code, signal }));
    });
    this.child.once("error", () => this.#fail("app_server_spawn_failed"));
    this.child.stdout.setEncoding("utf8");
    this.child.stdout.on("data", (chunk) => this.#consumeStdout(chunk));
    this.child.stderr.on("data", (chunk) => {
      this.stderrBytes += chunk.length;
      if (this.stderrBytes > CHILD_STREAM_MAX_BYTES) {
        this.#fail("app_server_stderr_too_large");
      }
    });
    this.child.once("exit", () => {
      if (!this.stopping) {
        this.#fail("app_server_exited_early");
      }
    });
    const initialized = await this.request("initialize", {
      clientInfo: {
        name: `thread-adoption-handoff-owner-${this.ownerNumber}`,
        title: "Thread adoption handoff evidence",
        version: "1.0.0",
      },
      capabilities: { experimentalApi: false },
    });
    requireCondition(
      typeof initialized?.userAgent === "string",
      "initialize_shape_invalid",
    );
    this.notify("initialized", {});
  }

  #consumeStdout(chunk) {
    this.stdoutBytes += Buffer.byteLength(chunk);
    if (this.stdoutBytes > CHILD_STREAM_MAX_BYTES) {
      this.#fail("app_server_stdout_too_large");
      return;
    }
    this.stdoutBuffer += chunk;
    if (Buffer.byteLength(this.stdoutBuffer) > CHILD_LINE_MAX_BYTES) {
      this.#fail("app_server_line_too_large");
      return;
    }
    for (;;) {
      const newline = this.stdoutBuffer.indexOf("\n");
      if (newline < 0) {
        return;
      }
      const line = this.stdoutBuffer.slice(0, newline).replace(/\r$/u, "");
      this.stdoutBuffer = this.stdoutBuffer.slice(newline + 1);
      if (line.length === 0) {
        continue;
      }
      let value;
      try {
        value = JSON.parse(line);
      } catch {
        this.#fail("app_server_non_json_output");
        return;
      }
      this.#route(value);
    }
  }

  #route(value) {
    if (value === null || typeof value !== "object" || Array.isArray(value)) {
      this.#fail("app_server_message_shape_invalid");
      return;
    }
    if (Object.hasOwn(value, "id") && !Object.hasOwn(value, "method")) {
      const pending = this.pending.get(value.id);
      if (pending === undefined) {
        this.#fail("app_server_unknown_response");
        return;
      }
      this.pending.delete(value.id);
      clearTimeout(pending.timer);
      if (Object.hasOwn(value, "error")) {
        pending.reject(new HarnessError("app_server_rpc_error"));
      } else if (Object.hasOwn(value, "result")) {
        pending.resolve(value.result);
      } else {
        pending.reject(new HarnessError("app_server_response_shape_invalid"));
      }
      return;
    }
    if (typeof value.method !== "string") {
      this.#fail("app_server_message_shape_invalid");
      return;
    }
    if (Object.hasOwn(value, "id")) {
      this.#write({
        id: value.id,
        error: { code: -32601, message: "unsupported harness reverse request" },
      });
      return;
    }
    this.notifications.push(value);
    if (this.notifications.length > MAX_NOTIFICATION_BACKLOG) {
      this.notifications.shift();
    }
    for (const waiter of [...this.notificationWaiters]) {
      if (waiter.predicate(value)) {
        this.notificationWaiters.delete(waiter);
        clearTimeout(waiter.timer);
        waiter.resolve(value);
      }
    }
  }

  #write(value) {
    if (this.child === null || this.child.stdin.destroyed) {
      throw new HarnessError("app_server_stdin_closed");
    }
    this.child.stdin.write(`${JSON.stringify(value)}\n`, (error) => {
      if (error && !this.stopping) {
        this.#fail("app_server_write_failed");
      }
    });
  }

  #fail(code) {
    if (this.fault === null) {
      this.fault = new HarnessError(code);
    }
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(this.fault);
    }
    this.pending.clear();
    for (const waiter of this.notificationWaiters) {
      clearTimeout(waiter.timer);
      waiter.reject(this.fault);
    }
    this.notificationWaiters.clear();
    if (this.child !== null && !this.stopping) {
      this.#signalTree("SIGKILL");
    }
  }

  #signalTree(signal) {
    if (this.child === null || this.pid === null) {
      return;
    }
    try {
      if (process.platform === "win32") {
        this.child.kill(signal);
      } else {
        process.kill(-this.pid, signal);
      }
    } catch (error) {
      if (error?.code !== "ESRCH") {
        this.child.kill(signal);
      }
    }
  }

  notify(method, params) {
    this.#write({ method, params });
  }

  request(method, params) {
    if (this.fault !== null) {
      return Promise.reject(this.fault);
    }
    const id = this.nextId;
    this.nextId += 1;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new HarnessError("app_server_rpc_timeout"));
      }, RPC_TIMEOUT_MS);
      this.pending.set(id, { resolve, reject, timer });
      try {
        this.#write({ id, method, params });
      } catch (error) {
        clearTimeout(timer);
        this.pending.delete(id);
        reject(error);
      }
    });
  }

  waitForNotification(predicate) {
    const existingIndex = this.notifications.findIndex(predicate);
    if (existingIndex >= 0) {
      const [existing] = this.notifications.splice(existingIndex, 1);
      return Promise.resolve(existing);
    }
    return new Promise((resolve, reject) => {
      const waiter = { predicate, resolve, reject, timer: null };
      waiter.timer = setTimeout(() => {
        this.notificationWaiters.delete(waiter);
        reject(new HarnessError("turn_completion_timeout"));
      }, TURN_TIMEOUT_MS);
      this.notificationWaiters.add(waiter);
    });
  }

  async startTurn(threadId, text) {
    const response = await this.request("turn/start", {
      threadId,
      input: [{ type: "text", text, textElements: [] }],
      approvalPolicy: "never",
    });
    const turnId = response?.turn?.id;
    requireCondition(typeof turnId === "string", "turn_start_shape_invalid");
    const terminal = await this.waitForNotification(
      (notification) =>
        notification.method === "turn/completed" &&
        notification.params?.threadId === threadId &&
        notification.params?.turn?.id === turnId,
    );
    requireCondition(
      terminal.params?.turn?.status === "completed",
      "turn_not_completed",
    );
  }

  async stop() {
    if (this.child === null || this.exitPromise === null || this.pid === null) {
      return false;
    }
    this.stopping = true;
    if (!this.child.stdin.destroyed) {
      this.child.stdin.end();
    }
    let exit;
    try {
      exit = await boundedPromise(
        this.exitPromise,
        PROCESS_EXIT_TIMEOUT_MS,
        "app_server_graceful_exit_timeout",
      );
    } catch {
      this.#signalTree("SIGTERM");
      try {
        exit = await boundedPromise(
          this.exitPromise,
          PROCESS_EXIT_TIMEOUT_MS,
          "app_server_terminate_timeout",
        );
      } catch {
        this.#signalTree("SIGKILL");
        exit = await boundedPromise(
          this.exitPromise,
          PROCESS_EXIT_TIMEOUT_MS,
          "app_server_kill_timeout",
        );
      }
    }
    requireCondition(
      exit !== undefined && (exit.code !== null || exit.signal !== null),
      "app_server_exit_invalid",
    );
    await waitForProcessGroupReap(this.pid);
    this.child = null;
    return true;
  }
}

function writeConfig(codexHome, baseUrl) {
  const config = `model = "gpt-5.4"\nmodel_provider = "adoption-handoff"\n\n[analytics]\nenabled = false\n\n[model_providers.adoption-handoff]\nname = "Thread adoption handoff"\nbase_url = "${baseUrl}"\nwire_api = "responses"\nrequest_max_retries = 0\nstream_max_retries = 0\nrequires_openai_auth = false\n`;
  fs.writeFileSync(path.join(codexHome, "config.toml"), config, {
    encoding: "utf8",
    mode: 0o600,
    flag: "wx",
  });
}

function threadSettings(workspace) {
  return {
    cwd: workspace,
    model: "gpt-5.4",
    modelProvider: "adoption-handoff",
    approvalPolicy: "never",
    sandbox: "read-only",
  };
}

async function runHandoff(options) {
  let scratch = null;
  let provider = null;
  const owners = [];
  let successfulSummary = null;
  let removed = false;
  try {
    requireCondition(
      process.platform !== "win32",
      "unsupported_process_tree_platform",
    );
    scratch = fs.mkdtempSync(path.join(os.tmpdir(), "lark-codex-handoff-"));
    const codexHome = path.join(scratch, "codex-home");
    const isolatedHome = path.join(scratch, "os-home");
    const workspace = path.join(scratch, "workspace");
    fs.mkdirSync(codexHome, { mode: 0o700 });
    fs.mkdirSync(isolatedHome, { mode: 0o700 });
    fs.mkdirSync(workspace, { mode: 0o700 });

    const inputMarkers = [marker("owner_a_input"), marker("owner_b_input")];
    const outputMarkers = [marker("owner_a_output"), marker("owner_b_output")];
    provider = new ScriptedProvider(inputMarkers, outputMarkers);
    await provider.start();
    writeConfig(codexHome, provider.baseUrl);
    const environment = isolatedEnvironment(codexHome, isolatedHome);
    const codexVersion = await probeVersion(
      options.binary,
      environment,
      options.expectedVersion,
    );
    const settings = threadSettings(workspace);

    const ownerA = new AppServerOwner(options.binary, environment, 1);
    owners.push(ownerA);
    await ownerA.start();
    const started = await ownerA.request("thread/start", {
      ...settings,
      ephemeral: false,
    });
    const threadId = started?.thread?.id;
    requireCondition(typeof threadId === "string", "thread_start_shape_invalid");
    await ownerA.startTurn(threadId, inputMarkers[0]);
    const historyA = await ownerA.request("thread/read", {
      threadId,
      includeTurns: true,
    });
    requireCondition(
      containsMarker(historyA, inputMarkers[0]) &&
        containsMarker(historyA, outputMarkers[0]) &&
        completedTurns(historyA) >= 1,
      "owner_a_history_missing",
    );
    requireCondition(await ownerA.stop(), "owner_a_not_reaped");

    const ownerB = new AppServerOwner(options.binary, environment, 2);
    owners.push(ownerB);
    await ownerB.start();
    const historyBeforeB = await ownerB.request("thread/read", {
      threadId,
      includeTurns: true,
    });
    requireCondition(
      containsMarker(historyBeforeB, inputMarkers[0]) &&
        containsMarker(historyBeforeB, outputMarkers[0]) &&
        completedTurns(historyBeforeB) >= 1,
      "owner_b_prior_history_missing",
    );
    const preResumeReadStatusAtOwnerB = safeThreadStatus(historyBeforeB);
    const resumedB = await ownerB.request("thread/resume", {
      threadId,
      ...settings,
    });
    requireCondition(resumedB?.thread?.id === threadId, "owner_b_resume_mismatch");
    await ownerB.startTurn(threadId, inputMarkers[1]);
    const historyB = await ownerB.request("thread/read", {
      threadId,
      includeTurns: true,
    });
    requireCondition(
      [...inputMarkers, ...outputMarkers].every((value) =>
        containsMarker(historyB, value),
      ) && completedTurns(historyB) >= 2,
      "owner_b_continuation_missing",
    );
    requireCondition(await ownerB.stop(), "owner_b_not_reaped");

    const ownerC = new AppServerOwner(options.binary, environment, 3);
    owners.push(ownerC);
    await ownerC.start();
    const resumedC = await ownerC.request("thread/resume", {
      threadId,
      ...settings,
    });
    requireCondition(resumedC?.thread?.id === threadId, "owner_c_resume_mismatch");
    const historyC = await ownerC.request("thread/read", {
      threadId,
      includeTurns: true,
    });
    requireCondition(
      [...inputMarkers, ...outputMarkers].every((value) =>
        containsMarker(historyC, value),
      ) && completedTurns(historyC) >= 2,
      "owner_c_history_missing",
    );
    requireCondition(await ownerC.stop(), "owner_c_not_reaped");

    provider.assertComplete();
    requireCondition(
      provider.priorHistoryObservedOnSecondTurn,
      "provider_prior_history_not_observed",
    );
    successfulSummary = {
      schema: SCHEMA,
      result: "pass",
      codexVersion,
      platform: `${process.platform}-${process.arch}`,
      transport: "managed_stdio",
      isolatedProfile: true,
      localScriptedProvider: true,
      explicitSequentialOwners: 3,
      completedTurns: 2,
      successfulHandoffs: 2,
      preResumeReadStatusAtOwnerB,
      historyVisibleAfterFirstHandoff: true,
      historyVisibleAfterSecondHandoff: true,
      processTreesReaped: 3,
      providerObservedPriorHistoryOnContinuation:
        provider.priorHistoryObservedOnSecondTurn,
      temporaryDataRemoved: false,
    };
  } finally {
    for (const owner of owners.reverse()) {
      try {
        await owner.stop();
      } catch {
        // Preserve the primary static failure classification. The exact temp
        // directory is still removed below and is never printed.
      }
    }
    if (provider !== null) {
      try {
        await provider.stop();
      } catch {
        // Preserve the primary static failure classification.
      }
    }
    if (scratch !== null) {
      fs.rmSync(scratch, { recursive: true, force: true, maxRetries: 3 });
      removed = !fs.existsSync(scratch);
    }
  }
  requireCondition(removed, "temporary_data_cleanup_failed");
  successfulSummary.temporaryDataRemoved = true;
  return successfulSummary;
}

async function runArchivedResumeAudit(options) {
  let scratch = null;
  let provider = null;
  const owners = [];
  let successfulSummary = null;
  let removed = false;
  try {
    requireCondition(
      process.platform !== "win32",
      "unsupported_process_tree_platform",
    );
    scratch = fs.mkdtempSync(path.join(os.tmpdir(), "lark-codex-archive-audit-"));
    const codexHome = path.join(scratch, "codex-home");
    const isolatedHome = path.join(scratch, "os-home");
    const workspace = path.join(scratch, "workspace");
    fs.mkdirSync(codexHome, { mode: 0o700 });
    fs.mkdirSync(isolatedHome, { mode: 0o700 });
    fs.mkdirSync(workspace, { mode: 0o700 });
    const seedInput = marker("archive_seed_input");
    const seedOutput = marker("archive_seed_output");
    provider = new ScriptedProvider([seedInput], [seedOutput]);
    await provider.start();
    writeConfig(codexHome, provider.baseUrl);
    const environment = isolatedEnvironment(codexHome, isolatedHome);
    const codexVersion = await probeVersion(
      options.binary,
      environment,
      options.expectedVersion,
    );
    const settings = threadSettings(workspace);

    const creator = new AppServerOwner(options.binary, environment, 1);
    owners.push(creator);
    await creator.start();
    const started = await creator.request("thread/start", {
      ...settings,
      ephemeral: false,
    });
    const threadId = started?.thread?.id;
    requireCondition(typeof threadId === "string", "thread_start_shape_invalid");
    await creator.startTurn(threadId, seedInput);
    provider.assertComplete(1);
    requireCondition(await creator.stop(), "archive_creator_not_reaped");

    const archiver = new AppServerOwner(options.binary, environment, 2);
    owners.push(archiver);
    await archiver.start();
    const activeBeforeArchive = await auditRequest(
      archiver,
      "thread/list",
      { archived: false, limit: 100 },
      "active_list_before_archive_rejected",
    );
    const archivedBeforeArchive = await auditRequest(
      archiver,
      "thread/list",
      { archived: true, limit: 100 },
      "archived_list_before_archive_rejected",
    );
    requireCondition(
      threadListContains(activeBeforeArchive, threadId) &&
        !threadListContains(archivedBeforeArchive, threadId),
      "active_thread_not_confirmed",
    );
    const activeExactIdSearchBeforeArchive = await auditRequest(
      archiver,
      "thread/list",
      { archived: false, searchTerm: threadId, limit: 20 },
      "active_exact_id_search_before_archive_rejected",
    );
    const activeCwdBeforeArchive = await auditRequest(
      archiver,
      "thread/list",
      { archived: false, cwd: workspace, limit: 20 },
      "active_cwd_filter_before_archive_rejected",
    );
    const activeExactIdAndCwdBeforeArchive = await auditRequest(
      archiver,
      "thread/list",
      {
        archived: false,
        searchTerm: threadId,
        cwd: workspace,
        limit: 20,
      },
      "active_exact_id_cwd_filter_before_archive_rejected",
    );
    await auditRequest(
      archiver,
      "thread/archive",
      { threadId },
      "archive_request_rejected",
    );
    const activeBeforeRead = await auditRequest(
      archiver,
      "thread/list",
      { archived: false, limit: 100 },
      "active_list_before_read_rejected",
    );
    const archivedBeforeRead = await auditRequest(
      archiver,
      "thread/list",
      { archived: true, limit: 100 },
      "archived_list_before_read_rejected",
    );
    requireCondition(
      !threadListContains(activeBeforeRead, threadId) &&
        threadListContains(archivedBeforeRead, threadId),
      "archive_not_confirmed",
    );
    const activeExactIdSearchAfterArchive = await auditRequest(
      archiver,
      "thread/list",
      { archived: false, searchTerm: threadId, limit: 20 },
      "active_exact_id_search_after_archive_rejected",
    );
    const archivedExactIdSearchAfterArchive = await auditRequest(
      archiver,
      "thread/list",
      { archived: true, searchTerm: threadId, limit: 20 },
      "archived_exact_id_search_after_archive_rejected",
    );
    const activeCwdAfterArchive = await auditRequest(
      archiver,
      "thread/list",
      { archived: false, cwd: workspace, limit: 20 },
      "active_cwd_filter_after_archive_rejected",
    );
    const archivedCwdAfterArchive = await auditRequest(
      archiver,
      "thread/list",
      { archived: true, cwd: workspace, limit: 20 },
      "archived_cwd_filter_after_archive_rejected",
    );
    const archivedExactIdAndCwdAfterArchive = await auditRequest(
      archiver,
      "thread/list",
      {
        archived: true,
        searchTerm: threadId,
        cwd: workspace,
        limit: 20,
      },
      "archived_exact_id_cwd_filter_after_archive_rejected",
    );
    requireCondition(await archiver.stop(), "archive_archiver_not_reaped");

    const reader = new AppServerOwner(options.binary, environment, 3);
    owners.push(reader);
    await reader.start();
    let readSucceeded = false;
    let readReportedArchived = false;
    let readStatus = "unavailable";
    try {
      const read = await reader.request("thread/read", {
        threadId,
        includeTurns: true,
      });
      readSucceeded = true;
      readReportedArchived = containsObjectKey(read, "archived");
      readStatus = safeThreadStatus(read);
    } catch (error) {
      requireCondition(
        error instanceof HarnessError && error.code === "app_server_rpc_error",
        "archived_read_unexpected_failure",
      );
    }
    const activeAfterRead = await auditRequest(
      reader,
      "thread/list",
      { archived: false, limit: 100 },
      "active_list_after_read_rejected",
    );
    const archivedAfterRead = await auditRequest(
      reader,
      "thread/list",
      { archived: true, limit: 100 },
      "archived_list_after_read_rejected",
    );

    let resumeSucceeded = false;
    try {
      const resumed = await reader.request("thread/resume", {
        threadId,
        ...settings,
      });
      requireCondition(
        resumed?.thread?.id === threadId,
        "archived_resume_shape_invalid",
      );
      resumeSucceeded = true;
    } catch (error) {
      requireCondition(
        error instanceof HarnessError && error.code === "app_server_rpc_error",
        "archived_resume_unexpected_failure",
      );
    }
    const activeAfterResume = await auditRequest(
      reader,
      "thread/list",
      { archived: false, limit: 100 },
      "active_list_after_resume_rejected",
    );
    const archivedAfterResume = await auditRequest(
      reader,
      "thread/list",
      { archived: true, limit: 100 },
      "archived_list_after_resume_rejected",
    );
    requireCondition(await reader.stop(), "archive_reader_not_reaped");
    const activeAfterResumeObserved = threadListContains(activeAfterResume, threadId);
    const archivedAfterResumeObserved = threadListContains(
      archivedAfterResume,
      threadId,
    );
    requireCondition(
      !resumeSucceeded &&
        !activeAfterResumeObserved &&
        archivedAfterResumeObserved,
      "archived_resume_not_refused",
    );

    successfulSummary = {
      schema: SCHEMA,
      result: "pass",
      audit: "archived_resume",
      codexVersion,
      platform: `${process.platform}-${process.arch}`,
      transport: "managed_stdio",
      isolatedProfile: true,
      activeExactIdSearchMatchedBeforeArchive: threadListContains(
        activeExactIdSearchBeforeArchive,
        threadId,
      ),
      activeCwdFilterMatchedBeforeArchive: threadListContains(
        activeCwdBeforeArchive,
        threadId,
      ),
      activeExactIdAndCwdMatchedBeforeArchive: threadListContains(
        activeExactIdAndCwdBeforeArchive,
        threadId,
      ),
      activeExactIdSearchMatchedAfterArchive: threadListContains(
        activeExactIdSearchAfterArchive,
        threadId,
      ),
      archivedExactIdSearchMatchedAfterArchive: threadListContains(
        archivedExactIdSearchAfterArchive,
        threadId,
      ),
      activeCwdFilterMatchedAfterArchive: threadListContains(
        activeCwdAfterArchive,
        threadId,
      ),
      archivedCwdFilterMatchedAfterArchive: threadListContains(
        archivedCwdAfterArchive,
        threadId,
      ),
      archivedExactIdAndCwdMatchedAfterArchive: threadListContains(
        archivedExactIdAndCwdAfterArchive,
        threadId,
      ),
      archiveConfirmedBeforeRead: true,
      archivedReadSucceeded: readSucceeded,
      archivedReadReportedArchived: readReportedArchived,
      archivedReadStatus: readStatus,
      activeAfterRead: threadListContains(activeAfterRead, threadId),
      archivedAfterRead: threadListContains(archivedAfterRead, threadId),
      archivedResumeRefused: true,
      activeAfterResume: activeAfterResumeObserved,
      archivedAfterResume: archivedAfterResumeObserved,
      processTreesReaped: 3,
      temporaryDataRemoved: false,
    };
  } finally {
    for (const owner of owners.reverse()) {
      try {
        await owner.stop();
      } catch {
        // Preserve the primary static failure classification.
      }
    }
    if (provider !== null) {
      try {
        await provider.stop();
      } catch {
        // Preserve the primary static failure classification.
      }
    }
    if (scratch !== null) {
      fs.rmSync(scratch, { recursive: true, force: true, maxRetries: 3 });
      removed = !fs.existsSync(scratch);
    }
  }
  requireCondition(removed, "temporary_data_cleanup_failed");
  successfulSummary.temporaryDataRemoved = true;
  return successfulSummary;
}

function runSelfTest() {
  const secret = marker("secret");
  assert.equal(containsMarker({ nested: ["prefix", secret] }, secret), true);
  assert.equal(containsMarker({ nested: ["prefix"] }, secret), false);
  assert.equal(completedTurns({ thread: { turns: [{ status: "completed" }] } }), 1);
  const summary = {
    schema: SCHEMA,
    result: "self-test-pass",
    redactedOutputContract: true,
  };
  const encoded = JSON.stringify(summary);
  assert.equal(encoded.includes(secret), false);
  return summary;
}

async function main() {
  let options;
  try {
    options = parseArguments(process.argv.slice(2));
    const summary = options.selfTest
      ? runSelfTest()
      : options.archivedAudit
        ? await runArchivedResumeAudit(options)
        : await runHandoff(options);
    process.stdout.write(`${JSON.stringify(summary)}\n`);
  } catch (error) {
    const code = error instanceof HarnessError ? error.code : "unexpected_failure";
    process.stderr.write(
      `${JSON.stringify({ schema: SCHEMA, result: "fail", classification: code })}\n`,
    );
    process.exitCode = 1;
  }
}

void main();
