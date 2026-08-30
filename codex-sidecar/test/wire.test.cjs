"use strict";

const assert = require("node:assert/strict");
const { Readable, Writable } = require("node:stream");
const test = require("node:test");

const { adapterForVersion, SUPPORTED_UPSTREAM_VERSIONS } = require("../adapters/index.cjs");
const { PriorityWriteQueue } = require("../priority-write-queue.cjs");
const {
  HARD_MAX_FRAME_BYTES,
  HELLO_CAPABILITIES,
  MAX_JSON_NESTING,
  MAX_JSON_STRUCTURAL_TOKENS,
  SidecarError,
  boundedLines,
  classifyRpcFrame,
  helloFrame,
  parseJsonLine,
  validateConfigureFrame,
} = require("../wire.cjs");

test("wire v1 hello advertises fixed bounds and lifecycle capabilities", () => {
  const hello = helloFrame();
  assert.equal(hello.protocol, "codex-sidecar-wire");
  assert.equal(hello.v, 1);
  assert.equal(hello.type, "hello");
  assert.equal(hello.maxFrameBytes, 33_554_432);
  assert.deepEqual(hello.capabilities, [...HELLO_CAPABILITIES]);
  assert.ok(hello.capabilities.includes("no-mutation-replay"));
  assert.ok(hello.capabilities.includes("priority-control-lane"));
});

test("configure validation is fail closed and bounds wrapper arguments", () => {
  const frame = {
    v: 1,
    type: "configure",
    id: "configure-1",
    codexBinary: null,
    codexHome: null,
    codexArguments: ["wrapper.cjs", "--static-mode"],
    maxFrameBytes: HARD_MAX_FRAME_BYTES,
    maxPending: 448,
  };
  const valid = validateConfigureFrame(frame);
  assert.equal(valid.maxFrameBytes, HARD_MAX_FRAME_BYTES);
  assert.equal(valid.maxPending, 448);
  assert.equal(valid.maxWriteQueueFrames, 512);

  assert.throws(
    () => validateConfigureFrame({ ...frame, unknownField: true }),
    (error) => error instanceof SidecarError && error.code === "invalid_configuration",
  );
  for (const required of [
    "v",
    "type",
    "id",
    "codexBinary",
    "codexHome",
    "maxFrameBytes",
    "maxPending",
  ]) {
    const missing = { ...frame };
    delete missing[required];
    assert.throws(
      () => validateConfigureFrame(missing),
      (error) => error instanceof SidecarError && error.code === "invalid_configuration",
    );
  }
  assert.throws(
    () => validateConfigureFrame({ ...frame, codexArguments: null }),
    (error) => error instanceof SidecarError && error.code === "invalid_configuration",
  );
  assert.deepEqual(
    validateConfigureFrame({ ...frame, codexArguments: [""] }).codexArguments,
    [""],
  );
  assert.throws(
    () => validateConfigureFrame({ ...frame, maxPending: 449 }),
    (error) => error instanceof SidecarError && error.code === "invalid_configuration",
  );
  assert.throws(
    () =>
      validateConfigureFrame({
        ...frame,
        codexArguments: Array.from({ length: 9 }, (_, index) => `arg-${index}`),
      }),
    (error) => error instanceof SidecarError && error.code === "invalid_configuration",
  );
});

test("bounded NDJSON accepts fragmented records and rejects oversized unterminated input", async () => {
  const stream = Readable.from([Buffer.from('{"id":"one",'), Buffer.from('"result":{}}\n')]);
  const records = [];
  for await (const line of boundedLines(stream, () => 128)) {
    records.push(parseJsonLine(line));
  }
  assert.deepEqual(records, [{ id: "one", result: {} }]);

  const oversized = Readable.from([Buffer.alloc(129, 0x61)]);
  await assert.rejects(
    async () => {
      for await (const _line of boundedLines(oversized, () => 128)) {
        // No complete line is expected.
      }
    },
    (error) => error instanceof SidecarError && error.code === "frame_too_large",
  );

  const unterminated = Readable.from([Buffer.from('{"id":"tail","result":{}}')]);
  await assert.rejects(
    async () => {
      for await (const _line of boundedLines(unterminated, () => 128)) {
        // Strict NDJSON requires a final LF delimiter.
      }
    },
    (error) => error instanceof SidecarError && error.code === "unterminated_frame",
  );

  const overFragmented = Readable.from(
    Array.from({ length: 4_097 }, () => Buffer.from("a")),
  );
  await assert.rejects(
    async () => {
      for await (const _line of boundedLines(overFragmented, () => 8_192)) {
        // Wire v1 explicitly caps one frame at 4,096 input chunks.
      }
    },
    (error) => error instanceof SidecarError && error.code === "frame_fragmented",
  );
});

test("JSON preflight limits stay at Rust wire parity", () => {
  assert.equal(MAX_JSON_NESTING, 128);
  assert.equal(MAX_JSON_STRUCTURAL_TOKENS, 65_536);
  assert.throws(
    () => parseJsonLine(Buffer.from(`${"[".repeat(129)}0${"]".repeat(129)}`)),
    (error) => error instanceof SidecarError && error.code === "json_nesting",
  );
  assert.throws(
    () => parseJsonLine(Buffer.from(`[${"0,".repeat(65_536)}0]`)),
    (error) => error instanceof SidecarError && error.code === "json_structure",
  );
});

test("RPC envelopes reject ambiguous fields and correlation shapes", () => {
  assert.deepEqual(classifyRpcFrame({ id: "request-1", method: "thread/list", params: {} }), {
    kind: "request",
    id: "request-1",
    method: "thread/list",
    params: {},
  });
  assert.throws(
    () => classifyRpcFrame({ id: "x", method: "thread/list", result: {} }),
    (error) => error instanceof SidecarError && error.code === "invalid_rpc",
  );
  assert.throws(
    () => classifyRpcFrame({ id: "contains space", result: {} }),
    (error) => error instanceof SidecarError && error.code === "invalid_correlation",
  );
});

test("0.149.0 and 0.151.0 use distinct reviewed adapter modules", () => {
  assert.deepEqual(SUPPORTED_UPSTREAM_VERSIONS, ["0.149.0", "0.151.0"]);
  const older = adapterForVersion("0.149.0");
  const newer = adapterForVersion("0.151.0");
  assert.notEqual(older, newer);
  assert.equal(older.adapterVersion, "0.149.0");
  assert.equal(newer.adapterVersion, "0.151.0");
  assert.equal(adapterForVersion("0.150.0"), null);
  assert.equal(
    newer.fromUpstreamNotification("future/unreviewed", { secret: "must-not-cross" }),
    null,
  );
  assert.throws(() => newer.toUpstreamRequest("future/write", {}), /not promoted/u);
});

test("control writes overtake queued normal writes without starving normal traffic", async () => {
  const observed = [];
  const callbacks = [];
  const sink = new Writable({
    write(chunk, _encoding, callback) {
      observed.push(JSON.parse(chunk.toString("utf8")));
      callbacks.push(callback);
    },
  });
  const queue = new PriorityWriteQueue(sink, {
    maxFrameBytes: 4_096,
    maxFrames: 16,
    maxBytes: 65_536,
  });

  const first = queue.enqueue({ ordinal: "normal-active" });
  const second = queue.enqueue({ ordinal: "normal-queued" });
  const control = queue.enqueue({ ordinal: "control" }, "control");
  assert.deepEqual(observed.map((value) => value.ordinal), ["normal-active"]);

  callbacks.shift()();
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(observed.map((value) => value.ordinal), ["normal-active", "control"]);
  callbacks.shift()();
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(observed.map((value) => value.ordinal), [
    "normal-active",
    "control",
    "normal-queued",
  ]);
  callbacks.shift()();
  await Promise.all([first, second, control]);
  await queue.waitIdle();
});

test("abort rejects the in-flight write and idle waiters before its callback returns", async () => {
  const callbacks = [];
  const sink = new Writable({
    write(_chunk, _encoding, callback) {
      callbacks.push(callback);
    },
  });
  const queue = new PriorityWriteQueue(sink, {
    maxFrameBytes: 4_096,
    maxFrames: 1,
    maxBytes: 65_536,
  });

  const active = queue.enqueue({ ordinal: "active" });
  const waiting = queue.enqueueWithBackpressure({ ordinal: "waiting" });
  const idle = queue.waitIdle();
  const failure = new SidecarError("write_aborted", "protocol write queue was aborted");
  const waitingRejected = assert.rejects(waiting, (error) => error === failure);
  const idleRejected = assert.rejects(idle, (error) => error === failure);
  queue.abort(failure);

  await assert.rejects(active, (error) => error === failure);
  await waitingRejected;
  await idleRejected;
  await assert.rejects(queue.waitIdle(), (error) => error === failure);

  callbacks.shift()();
  await new Promise((resolve) => setImmediate(resolve));
  await assert.rejects(active, (error) => error === failure);
});

test("backpressure waits for bounded write capacity without awaiting physical output", async () => {
  const observed = [];
  const callbacks = [];
  const sink = new Writable({
    write(chunk, _encoding, callback) {
      observed.push(JSON.parse(chunk.toString("utf8")));
      callbacks.push(callback);
    },
  });
  const queue = new PriorityWriteQueue(sink, {
    maxFrameBytes: 4_096,
    maxFrames: 1,
    maxBytes: 65_536,
  });

  const active = queue.enqueue({ ordinal: "active" });
  const admission = queue.enqueueWithBackpressure({ ordinal: "waiting" });
  let idleSettled = false;
  const idle = queue.waitIdle().then(() => {
    idleSettled = true;
  });
  assert.deepEqual(observed, [{ ordinal: "active" }]);
  assert.equal(queue.capacityWaiters.length, 1);

  callbacks.shift()();
  await active;
  await admission;
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(observed, [{ ordinal: "active" }, { ordinal: "waiting" }]);
  assert.equal(idleSettled, false);

  callbacks.shift()();
  await idle;
  await queue.end();
});

test("a capacity-blocked control write overtakes queued normal output", async () => {
  const observed = [];
  const callbacks = [];
  const sink = new Writable({
    write(chunk, _encoding, callback) {
      observed.push(JSON.parse(chunk.toString("utf8")));
      callbacks.push(callback);
    },
  });
  const queue = new PriorityWriteQueue(sink, {
    maxFrameBytes: 4_096,
    maxFrames: 2,
    maxBytes: 65_536,
  });

  const active = queue.enqueue({ ordinal: "normal-active" });
  const normal = queue.enqueue({ ordinal: "normal-queued" });
  const controlAdmission = queue.enqueueWithBackpressure(
    { ordinal: "control-waiting" },
    "control",
  );
  assert.equal(queue.capacityWaiters.length, 1);

  callbacks.shift()();
  await active;
  await controlAdmission;
  assert.deepEqual(observed.map((frame) => frame.ordinal), [
    "normal-active",
    "control-waiting",
  ]);

  callbacks.shift()();
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(observed.map((frame) => frame.ordinal), [
    "normal-active",
    "control-waiting",
    "normal-queued",
  ]);
  callbacks.shift()();
  await normal;
  await queue.waitIdle();
  await queue.end();
});

test("backpressure retains at most one encoded overflow frame", async () => {
  const callbacks = [];
  const sink = new Writable({
    write(_chunk, _encoding, callback) {
      callbacks.push(callback);
    },
  });
  const queue = new PriorityWriteQueue(sink, {
    maxFrameBytes: 4_096,
    maxFrames: 1,
    maxBytes: 65_536,
  });

  const active = queue.enqueue({ ordinal: "active" });
  const waiting = queue.enqueueWithBackpressure({ ordinal: "waiting" });
  await assert.rejects(
    queue.enqueueWithBackpressure({ ordinal: "must-not-be-retained" }, "control"),
    (error) => error instanceof SidecarError && error.code === "write_queue_full",
  );
  assert.equal(queue.capacityWaiters.length, 1);

  callbacks.shift()();
  await active;
  const admission = await waiting;
  callbacks.shift()();
  await admission.completion;
  await queue.waitIdle();
  await queue.end();
});

test("stream write errors are contained, redacted, and reported exactly once", async () => {
  const reported = [];
  const sink = new Writable({
    write(_chunk, _encoding, callback) {
      callback(new Error("raw-provider-path-secret"));
    },
  });
  const queue = new PriorityWriteQueue(sink, {
    maxFrameBytes: 4_096,
    maxFrames: 4,
    maxBytes: 65_536,
    onError(error) {
      reported.push(error);
    },
  });

  await assert.rejects(
    queue.enqueue({ ordinal: "fails" }),
    (error) =>
      error instanceof SidecarError &&
      error.code === "write_failed" &&
      !String(error).includes("raw-provider-path-secret"),
  );
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(reported.length, 1);
  assert.equal(reported[0].code, "write_failed");
  assert.equal(String(reported[0]).includes("raw-provider-path-secret"), false);
});

test("end closes admission before waiting for an in-flight write", async () => {
  const callbacks = [];
  const sink = new Writable({
    write(_chunk, _encoding, callback) {
      callbacks.push(callback);
    },
  });
  const queue = new PriorityWriteQueue(sink, {
    maxFrameBytes: 4_096,
    maxFrames: 4,
    maxBytes: 65_536,
  });

  const active = queue.enqueue({ ordinal: "active" });
  const ending = queue.end();
  assert.throws(
    () => queue.enqueue({ ordinal: "must-not-enter" }),
    (error) => error instanceof SidecarError && error.code === "write_closed",
  );
  callbacks.shift()();
  await active;
  await ending;
  await queue.end();
});

test("abort immediately settles a stream finalization that never returns", async () => {
  let finish;
  const sink = new Writable({
    write(_chunk, _encoding, callback) {
      callback();
    },
    final(callback) {
      finish = callback;
    },
  });
  const queue = new PriorityWriteQueue(sink, {
    maxFrameBytes: 4_096,
    maxFrames: 4,
    maxBytes: 65_536,
  });

  const ending = queue.end();
  for (let attempts = 0; attempts < 100 && finish === undefined; attempts += 1) {
    await new Promise((resolve) => setImmediate(resolve));
  }
  assert.equal(typeof finish, "function");
  const failure = new SidecarError("write_aborted", "protocol write queue was aborted");
  const rejected = assert.rejects(ending, (error) => error === failure);
  queue.abort(failure);
  await rejected;
  finish();
  await new Promise((resolve) => setImmediate(resolve));
});

test("an unexpected stream close settles every queue waiter", async () => {
  const callbacks = [];
  const reported = [];
  const sink = new Writable({
    write(_chunk, _encoding, callback) {
      callbacks.push(callback);
    },
  });
  const queue = new PriorityWriteQueue(sink, {
    maxFrameBytes: 4_096,
    maxFrames: 1,
    maxBytes: 65_536,
    onError(error) {
      reported.push(error);
    },
  });

  const active = queue.enqueue({ ordinal: "active" });
  const waiting = queue.enqueueWithBackpressure({ ordinal: "waiting" });
  const idle = queue.waitIdle();
  const isWriteFailure = (error) =>
    error instanceof SidecarError && error.code === "write_failed";
  const activeRejected = assert.rejects(active, isWriteFailure);
  const waitingRejected = assert.rejects(waiting, isWriteFailure);
  const idleRejected = assert.rejects(idle, isWriteFailure);

  sink.destroy();
  await Promise.all([activeRejected, waitingRejected, idleRejected]);
  assert.equal(reported.length, 1);
  callbacks.shift()();
  await new Promise((resolve) => setImmediate(resolve));
});

test("an idle queue can release a stream to its next owner", async () => {
  const reported = [];
  const sink = new Writable({
    write(_chunk, _encoding, callback) {
      callback();
    },
  });
  const options = {
    maxFrameBytes: 4_096,
    maxFrames: 4,
    maxBytes: 65_536,
    onError(error) {
      reported.push(error);
    },
  };

  const bootstrap = new PriorityWriteQueue(sink, options);
  await bootstrap.enqueue({ phase: "bootstrap" }, "control");
  await bootstrap.waitIdle();
  bootstrap.release();
  assert.throws(
    () => bootstrap.enqueue({ phase: "must-not-return" }),
    (error) => error instanceof SidecarError && error.code === "write_closed",
  );

  const session = new PriorityWriteQueue(sink, options);
  await session.enqueue({ phase: "session" });
  await session.end();
  assert.deepEqual(reported, []);
});

test("a late auto-destroy error after finish remains contained", async () => {
  const reported = [];
  const sink = new Writable({
    write(_chunk, _encoding, callback) {
      callback();
    },
    final(callback) {
      callback();
    },
    destroy(_error, callback) {
      setImmediate(() => callback(new Error("late-provider-secret")));
    },
  });
  const queue = new PriorityWriteQueue(sink, {
    maxFrameBytes: 4_096,
    maxFrames: 4,
    maxBytes: 65_536,
    onError(error) {
      reported.push(error);
    },
  });

  await queue.end();
  for (let attempts = 0; attempts < 100 && reported.length === 0; attempts += 1) {
    await new Promise((resolve) => setImmediate(resolve));
  }
  assert.equal(reported.length, 1);
  assert.equal(reported[0].code, "write_failed");
  assert.equal(String(reported[0]).includes("late-provider-secret"), false);
});

for (const [label, limits] of [
  ["frame", { maxFrames: 1, maxBytes: 65_536 }],
  [
    "byte",
    {
      maxFrames: 4,
      maxBytes: Buffer.byteLength(`${JSON.stringify({ ordinal: "active" })}\n`),
    },
  ],
]) {
  test(`${label} saturation rejects before a second protocol write`, async () => {
    const observed = [];
    const callbacks = [];
    const sink = new Writable({
      write(chunk, _encoding, callback) {
        observed.push(JSON.parse(chunk.toString("utf8")));
        callbacks.push(callback);
      },
    });
    const queue = new PriorityWriteQueue(sink, {
      maxFrameBytes: 4_096,
      ...limits,
    });

    const active = queue.enqueue({ ordinal: "active" });
    assert.throws(
      () => queue.enqueue({ ordinal: "must-not-write" }),
      (error) => error instanceof SidecarError && error.code === "write_queue_full",
    );
    assert.deepEqual(observed, [{ ordinal: "active" }]);

    callbacks.shift()();
    await active;
    await queue.end();
    assert.deepEqual(observed, [{ ordinal: "active" }]);
  });
}
