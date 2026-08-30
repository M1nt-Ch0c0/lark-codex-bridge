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
const { terminateChild } = require("../upstream.cjs");
const { boundedLines, correlationKey } = require("../wire.cjs");
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
  assert.ok(elapsedMs < 5_000, `shutdown exceeded bound: ${elapsedMs}ms`);
  assert.equal(child.stdin.destroyed, true);
  assert.equal(child.stdout.destroyed, true);
  assert.equal(child.stderr.destroyed, true);
});

test("terminateChild releases wait listeners when a child never exits", async (t) => {
  const child = new SessionChild({ exitOnStdinFinish: false });
  child.kill = (signal = "SIGTERM") => {
    child.killCalls.push(signal);
    return true;
  };
  t.after(() => {
    child.stdin.destroy();
    child.stdout.destroy();
    child.stderr.destroy();
  });

  await terminateChild(child, 10);

  assert.deepEqual(child.killCalls, ["SIGKILL"]);
  assert.equal(child.listenerCount("exit"), 0);
  assert.equal(child.listenerCount("error"), 0);
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

test("upstream consumption applies bounded admission backpressure", async (t) => {
  const writes = [];
  const callbacks = [];
  const stalledOutput = new Writable({
    write(chunk, _encoding, callback) {
      writes.push(JSON.parse(chunk.toString("utf8")));
      callbacks.push(callback);
    },
  });
  const child = new SessionChild();
  const localInput = new PassThrough();
  const upstreamInbox = new LineInbox(child.stdin);
  t.after(() => {
    upstreamInbox.close();
    localInput.destroy();
    stalledOutput.destroy();
    child.stdin.destroy();
    child.stdout.destroy();
    child.stderr.destroy();
  });

  const configuration = {
    maxFrameBytes: 4_096,
    maxPending: 8,
    maxWriteQueueFrames: 2,
    maxWriteQueueBytes: 65_536,
    shutdownGraceMs: 250,
  };
  const session = new ProtocolSession({
    configuration,
    adapter: adapterForVersion("0.151.0"),
    child,
    localInput,
    localLines: boundedLines(localInput, () => configuration.maxFrameBytes),
    localOutput: stalledOutput,
  });
  const running = session.run();

  const upstream = [];
  for (const id of ["first", "second", "third", "fourth"]) {
    send(localInput, { id, method: "thread/list", params: {} });
    upstream.push(await upstreamInbox.nextJson());
  }
  const result = { data: [], nextCursor: null, backwardsCursor: null };
  for (const request of upstream) {
    send(child.stdout, { id: request.id, result });
  }
  for (
    let attempts = 0;
    attempts < 100 && session.toLocal.capacityWaiters.length === 0;
    attempts += 1
  ) {
    await new Promise((resolve) => setImmediate(resolve));
  }

  assert.deepEqual(writes, [{ id: "first", result }]);
  assert.equal(session.pendingRequestsByUpstream.size, 1);
  assert.equal(session.toLocal.queuedFrames, 2);
  assert.equal(session.toLocal.capacityWaiters.length, 1);
  assert.deepEqual(child.killCalls, []);

  callbacks.shift()();
  for (
    let attempts = 0;
    attempts < 100 && session.pendingRequestsByUpstream.size !== 0;
    attempts += 1
  ) {
    await new Promise((resolve) => setImmediate(resolve));
  }
  assert.equal(writes.length, 2);
  assert.deepEqual(writes[1], { id: "second", result });
  assert.equal(session.pendingRequestsByUpstream.size, 0);
  assert.equal(session.toLocal.queuedFrames, 2);
  assert.equal(session.toLocal.capacityWaiters.length, 1);

  for (const expectedId of ["third", "fourth"]) {
    callbacks.shift()();
    for (let attempts = 0; attempts < 100 && writes.at(-1).id !== expectedId; attempts += 1) {
      await new Promise((resolve) => setImmediate(resolve));
    }
    assert.equal(writes.at(-1).id, expectedId);
  }
  callbacks.shift()();

  localInput.end();
  assert.deepEqual(await running, { kind: "graceful" });
});

test("admission backpressure preserves control-response priority", async (t) => {
  const writes = [];
  const callbacks = [];
  const stalledOutput = new Writable({
    write(chunk, _encoding, callback) {
      writes.push(JSON.parse(chunk.toString("utf8")));
      callbacks.push(callback);
    },
  });
  const child = new SessionChild();
  const localInput = new PassThrough();
  const upstreamInbox = new LineInbox(child.stdin);
  t.after(() => {
    upstreamInbox.close();
    localInput.destroy();
    stalledOutput.destroy();
    child.stdin.destroy();
    child.stdout.destroy();
    child.stderr.destroy();
  });

  const configuration = {
    maxFrameBytes: 4_096,
    maxPending: 8,
    maxWriteQueueFrames: 4,
    maxWriteQueueBytes: 65_536,
    shutdownGraceMs: 250,
  };
  const session = new ProtocolSession({
    configuration,
    adapter: adapterForVersion("0.151.0"),
    child,
    localInput,
    localLines: boundedLines(localInput, () => configuration.maxFrameBytes),
    localOutput: stalledOutput,
  });
  const running = session.run();

  send(localInput, { id: "first", method: "thread/list", params: {} });
  send(localInput, { id: "second", method: "thread/list", params: {} });
  send(localInput, {
    id: "control",
    method: "turn/interrupt",
    params: { threadId: "thread-1", turnId: "turn-1" },
  });
  send(localInput, { id: "fourth", method: "thread/list", params: {} });

  const upstream = [];
  for (let index = 0; index < 4; index += 1) {
    upstream.push(await upstreamInbox.nextJson());
  }
  const upstreamByLocal = new Map();
  for (const pending of session.pendingRequestsByUpstream.values()) {
    const request = upstream.find(
      (candidate) => correlationKey(candidate.id) === pending.upstreamKey,
    );
    upstreamByLocal.set(pending.localId, request);
  }
  const listResult = { data: [], nextCursor: null, backwardsCursor: null };
  for (const id of ["first", "second", "control", "fourth"]) {
    send(child.stdout, {
      id: upstreamByLocal.get(id).id,
      result: id === "control" ? {} : listResult,
    });
  }
  for (
    let attempts = 0;
    attempts < 100 && session.pendingRequestsByUpstream.size !== 0;
    attempts += 1
  ) {
    await new Promise((resolve) => setImmediate(resolve));
  }

  assert.deepEqual(writes.map((frame) => frame.id), ["first"]);
  assert.equal(session.pendingRequestsByUpstream.size, 0);
  assert.equal(session.toLocal.queuedFrames, 4);
  assert.equal(session.toLocal.capacityWaiters.length, 0);

  for (const expectedId of ["control", "second", "fourth"]) {
    callbacks.shift()();
    for (let attempts = 0; attempts < 100 && writes.at(-1).id !== expectedId; attempts += 1) {
      await new Promise((resolve) => setImmediate(resolve));
    }
    assert.equal(writes.at(-1).id, expectedId);
  }
  callbacks.shift()();

  localInput.end();
  assert.deepEqual(await running, { kind: "graceful" });
});

test("reverse-request timeout starts only after physical local output", async (t) => {
  const localWrites = [];
  const localCallbacks = [];
  const stalledOutput = new Writable({
    write(chunk, _encoding, callback) {
      localWrites.push(JSON.parse(chunk.toString("utf8")));
      localCallbacks.push(callback);
    },
  });
  const upstreamWrites = [];
  const upstreamInput = new Writable({
    write(chunk, _encoding, callback) {
      upstreamWrites.push(JSON.parse(chunk.toString("utf8")));
      callback();
    },
  });
  const child = new SessionChild({ stdin: upstreamInput });
  const localInput = new PassThrough();
  t.after(() => {
    localInput.destroy();
    stalledOutput.destroy();
    child.stdin.destroy();
    child.stdout.destroy();
    child.stderr.destroy();
  });

  const configuration = {
    maxFrameBytes: 4_096,
    maxPending: 4,
    maxWriteQueueFrames: 2,
    maxWriteQueueBytes: 65_536,
    shutdownGraceMs: 250,
  };
  const session = new ProtocolSession({
    configuration,
    adapter: adapterForVersion("0.151.0"),
    child,
    localInput,
    localLines: boundedLines(localInput, () => configuration.maxFrameBytes),
    localOutput: stalledOutput,
    serverRequestTimeoutMs: 25,
  });
  const running = session.run();

  send(localInput, { id: "fills-output", method: "future/write", params: {} });
  for (let attempts = 0; attempts < 100 && localWrites.length === 0; attempts += 1) {
    await new Promise((resolve) => setImmediate(resolve));
  }
  assert.equal(localWrites[0].id, "fills-output");

  send(child.stdout, {
    id: "approval-delayed",
    method: "item/commandExecution/requestApproval",
    params: {
      threadId: "thread-1",
      turnId: "turn-1",
      itemId: "item-1",
      startedAtMs: 100,
      command: "pwd",
    },
  });
  for (
    let attempts = 0;
    attempts < 100 && session.toLocal.capacityWaiters.length === 0;
    attempts += 1
  ) {
    await new Promise((resolve) => setImmediate(resolve));
  }
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.equal(session.pendingServerByLocal.size, 1);
  assert.equal(session.toLocal.capacityWaiters.length, 0);
  assert.equal(session.toLocal.queuedFrames, 2);
  assert.deepEqual(upstreamWrites, []);
  assert.equal(localWrites.length, 1);

  localCallbacks.shift()();
  for (let attempts = 0; attempts < 100 && localWrites.length < 2; attempts += 1) {
    await new Promise((resolve) => setImmediate(resolve));
  }
  const approval = localWrites[1];
  assert.equal(approval.method, "item/commandExecution/requestApproval");
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.equal(session.pendingServerByLocal.size, 1);
  assert.deepEqual(upstreamWrites, []);

  send(localInput, { id: approval.id, result: { decision: "decline" } });
  for (let attempts = 0; attempts < 100 && upstreamWrites.length === 0; attempts += 1) {
    await new Promise((resolve) => setImmediate(resolve));
  }
  assert.deepEqual(upstreamWrites, [
    { id: "approval-delayed", result: { decision: "decline" } },
  ]);

  localCallbacks.shift()();
  localInput.end();
  assert.deepEqual(await running, { kind: "graceful" });
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
