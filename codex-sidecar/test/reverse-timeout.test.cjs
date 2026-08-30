"use strict";

const assert = require("node:assert/strict");
const { EventEmitter } = require("node:events");
const { PassThrough, Writable } = require("node:stream");
const test = require("node:test");

const { adapterForVersion } = require("../adapters/index.cjs");
const {
  LOCAL_REQUEST_TIMEOUT_MS,
  ProtocolSession,
  SERVER_REQUEST_TIMEOUT_MS,
} = require("../session.cjs");
const { boundedLines } = require("../wire.cjs");
const { LineInbox } = require("./helpers.cjs");

class SessionChild extends EventEmitter {
  constructor(options = {}) {
    super();
    this.stdin = options.stdin ?? new PassThrough();
    this.stdout = new PassThrough();
    this.stderr = new PassThrough();
    this.exitCode = null;
    this.signalCode = null;
    this.exiting = false;
    this.stdinFinished = false;
    this.killCalls = [];
    this.stdin.once("finish", () => {
      this.stdinFinished = true;
      if (options.exitOnStdinFinish !== false) {
        setImmediate(() => this.finish(0, null));
      }
    });
  }

  finish(code, signal) {
    if (this.exiting) {
      return;
    }
    this.exiting = true;
    this.exitCode = code;
    this.signalCode = signal;
    this.stdout.end();
    this.stderr.end();
    this.emit("exit", code, signal);
  }

  kill(signal = "SIGTERM") {
    this.killCalls.push(signal);
    setImmediate(() => this.finish(null, signal));
    return true;
  }
}

function send(stream, value) {
  stream.write(`${JSON.stringify(value)}\n`);
}

test("production reverse-request timeout covers the full handler envelope", () => {
  assert.equal(LOCAL_REQUEST_TIMEOUT_MS, 30_000);
  assert.ok(SERVER_REQUEST_TIMEOUT_MS > 105_000);
});

test("shutdown grace expiry force-kills and closes every upstream pipe", async (t) => {
  const child = new SessionChild({ exitOnStdinFinish: false });
  const localInput = new PassThrough();
  const localOutput = new PassThrough();
  const localInbox = new LineInbox(localOutput);
  t.after(() => {
    localInbox.close();
    localInput.destroy();
    localOutput.destroy();
    child.stdin.destroy();
    child.stdout.destroy();
    child.stderr.destroy();
  });

  const configuration = {
    maxFrameBytes: 4_096,
    maxPending: 4,
    maxWriteQueueFrames: 16,
    maxWriteQueueBytes: 65_536,
    shutdownGraceMs: 30,
  };
  const session = new ProtocolSession({
    configuration,
    adapter: adapterForVersion("0.151.0"),
    child,
    localInput,
    localLines: boundedLines(localInput, () => configuration.maxFrameBytes),
    localOutput,
  });
  const startedAt = Date.now();
  const running = session.run();

  send(localInput, { id: "shutdown-local", method: "sidecar/shutdown", params: {} });
  assert.deepEqual(await localInbox.nextJson(), { id: "shutdown-local", result: {} });
  const outcome = await running;
  const elapsedMs = Date.now() - startedAt;

  assert.deepEqual(outcome, { kind: "graceful" });
  assert.equal(child.stdinFinished, true);
  assert.deepEqual(child.killCalls, ["SIGKILL"]);
  assert.ok(elapsedMs >= 25, `shutdown elapsed ${elapsedMs}ms before grace`);
  assert.ok(elapsedMs < 500, `shutdown exceeded bound: ${elapsedMs}ms`);
  assert.equal(child.stdin.destroyed, true);
  assert.equal(child.stdout.destroyed, true);
  assert.equal(child.stderr.destroyed, true);
});

test("session write-queue saturation rejects before upstream write and clears correlation", async (t) => {
  const writes = [];
  const callbacks = [];
  const stalledInput = new Writable({
    write(chunk, _encoding, callback) {
      writes.push(JSON.parse(chunk.toString("utf8")));
      callbacks.push(callback);
    },
  });
  const child = new SessionChild({
    stdin: stalledInput,
    exitOnStdinFinish: false,
  });
  const localInput = new PassThrough();
  const localOutput = new PassThrough();
  const localInbox = new LineInbox(localOutput);
  t.after(() => {
    localInbox.close();
    localInput.destroy();
    localOutput.destroy();
    child.stdin.destroy();
    child.stdout.destroy();
    child.stderr.destroy();
  });

  const configuration = {
    maxFrameBytes: 4_096,
    maxPending: 4,
    maxWriteQueueFrames: 1,
    maxWriteQueueBytes: 65_536,
    shutdownGraceMs: 30,
  };
  const session = new ProtocolSession({
    configuration,
    adapter: adapterForVersion("0.151.0"),
    child,
    localInput,
    localLines: boundedLines(localInput, () => configuration.maxFrameBytes),
    localOutput,
    localRequestTimeoutMs: 1_000,
  });
  const running = session.run();

  send(localInput, { id: "active", method: "thread/list", params: {} });
  send(localInput, { id: "rejected", method: "thread/list", params: {} });
  assert.deepEqual(await localInbox.nextJson(), {
    id: "rejected",
    error: { code: -32020, message: "sidecar write capacity is exhausted" },
  });
  assert.equal(writes.length, 1);
  assert.equal(writes[0].method, "thread/list");
  assert.equal(session.pendingRequestsByUpstream.size, 1);
  assert.equal(session.activeLocalRequestIds.size, 1);

  callbacks.shift()();
  await new Promise((resolve) => setImmediate(resolve));
  send(localInput, { id: "shutdown-local", method: "sidecar/shutdown", params: {} });
  assert.deepEqual(await localInbox.nextJson(), { id: "shutdown-local", result: {} });
  assert.deepEqual(await running, { kind: "graceful" });
  assert.equal(writes.length, 1);
  assert.deepEqual(child.killCalls, ["SIGKILL"]);
});

test("reverse timeout is request-scoped, keeps health RPC alive, and fences duplicate late responses", async (t) => {
  const child = new SessionChild();
  const localInput = new PassThrough();
  const localOutput = new PassThrough();
  const localInbox = new LineInbox(localOutput);
  const upstreamInbox = new LineInbox(child.stdin);
  t.after(() => {
    localInbox.close();
    upstreamInbox.close();
    localInput.destroy();
    localOutput.destroy();
    child.stdin.destroy();
    child.stdout.destroy();
    child.stderr.destroy();
  });

  const configuration = {
    maxFrameBytes: 4_096,
    maxPending: 4,
    maxWriteQueueFrames: 16,
    maxWriteQueueBytes: 65_536,
    shutdownGraceMs: 250,
  };
  const session = new ProtocolSession({
    configuration,
    adapter: adapterForVersion("0.151.0"),
    child,
    localInput,
    localLines: boundedLines(localInput, () => configuration.maxFrameBytes),
    localOutput,
    serverRequestTimeoutMs: 25,
  });
  const running = session.run();

  send(child.stdout, {
    id: "approval-timeout",
    method: "item/commandExecution/requestApproval",
    params: {
      threadId: "thread-1",
      turnId: "turn-1",
      itemId: "item-1",
      startedAtMs: 100,
      command: "pwd",
    },
  });
  const approval = await localInbox.nextJson();
  assert.match(approval.id, /^server:[a-f0-9]+:\d+$/u);

  const timedOut = await upstreamInbox.nextJson();
  assert.deepEqual(timedOut, {
    id: "approval-timeout",
    error: { code: -32022, message: "bridge server request timed out" },
  });

  send(child.stdout, {
    method: "serverRequest/resolved",
    params: { threadId: "thread-1", requestId: "approval-timeout" },
  });
  const resolved = await localInbox.nextJson();
  assert.equal(resolved.method, "serverRequest/resolved");
  assert.equal(resolved.params.requestId, approval.id);

  // Exactly one Rust response that raced the timeout is discarded. The next
  // frame observed upstream must therefore be the independent health request.
  send(localInput, { id: approval.id, result: { decision: "decline" } });
  send(localInput, { id: "health-local", method: "thread/list", params: {} });
  const healthUpstream = await upstreamInbox.nextJson();
  assert.equal(healthUpstream.method, "thread/list");
  send(child.stdout, {
    id: healthUpstream.id,
    result: { data: [], nextCursor: null, backwardsCursor: null },
  });
  assert.deepEqual(await localInbox.nextJson(), {
    id: "health-local",
    result: { data: [], nextCursor: null, backwardsCursor: null },
  });

  send(localInput, { id: approval.id, result: { decision: "decline" } });
  const outcome = await running;
  assert.equal(outcome.kind, "fatal");
  assert.equal(outcome.error.code, "late_response");
});
