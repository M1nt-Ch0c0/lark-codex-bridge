"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const test = require("node:test");

const { spawnFailureCode } = require("../upstream.cjs");

const {
  configureSidecar,
  readMarker,
  send,
  spawnSidecar,
  waitForExit,
} = require("./helpers.cjs");

test("spawn classification separates resource pressure from deterministic launch failures", () => {
  for (const code of ["EAGAIN", "EMFILE", "ENFILE", "ENOMEM"]) {
    assert.equal(
      spawnFailureCode({ code }, "version_probe"),
      "version_probe_spawn_unavailable",
    );
  }
  assert.equal(spawnFailureCode({ code: "ENOENT" }, "version_probe"), "version_probe_spawn_failed");
  assert.equal(spawnFailureCode({ code: "EACCES" }, "upstream"), "upstream_spawn_failed");
});

test("version probing accepts only exact 0.149.0 and 0.151.0 output", async (t) => {
  for (const options of [
    { version: "0.150.0", mode: "normal", expected: "unsupported_upstream_version" },
    { version: "0.151.0", mode: "malformed-version", expected: "unsupported_upstream_version" },
  ]) {
    const running = await configureSidecar(options);
    t.after(() => fs.rmSync(running.directory, { recursive: true, force: true }));
    assert.equal(running.response.ok, false);
    assert.equal(running.response.error, options.expected);
    const exit = await waitForExit(running.child);
    assert.equal(exit.code, 1);
    assert.match(await running.stderr.next(), new RegExp(`code=${options.expected}$`, "u"));
  }
});

test("active local correlation reuse fails the epoch and does not replay", async (t) => {
  const running = await configureSidecar({ mode: "hold" });
  t.after(() => fs.rmSync(running.directory, { recursive: true, force: true }));
  send(running.child, { id: "reused", method: "turn/start", params: { threadId: "thread", input: [] } });
  await new Promise((resolve) => setTimeout(resolve, 75));
  send(running.child, { id: "reused", method: "turn/start", params: { threadId: "thread", input: [] } });
  const exit = await waitForExit(running.child);
  assert.equal(exit.code, 1);
  assert.match(await running.stderr.next(), /code=correlation_reuse$/u);
  const evidence = readMarker(running.marker);
  assert.equal(evidence.filter((entry) => entry.event === "start").length, 1);
  assert.ok(evidence.filter((entry) => entry.event === "request").length <= 1);
});

test("late duplicate upstream response is fenced and terminates the epoch", async (t) => {
  const running = await configureSidecar({ mode: "duplicate-response" });
  t.after(() => fs.rmSync(running.directory, { recursive: true, force: true }));
  send(running.child, { id: "list-local", method: "thread/list", params: {} });
  const exit = await waitForExit(running.child);
  assert.equal(exit.code, 1);
  assert.match(await running.stderr.next(), /code=late_response$/u);
  assert.equal(readMarker(running.marker).filter((entry) => entry.event === "start").length, 1);
});

test("duplicate active reverse-request IDs are fenced", async (t) => {
  const running = await configureSidecar({ mode: "duplicate-server-id" });
  t.after(() => fs.rmSync(running.directory, { recursive: true, force: true }));
  send(running.child, { id: "turn-local", method: "turn/start", params: { threadId: "thread", input: [] } });
  const exit = await waitForExit(running.child);
  assert.equal(exit.code, 1);
  assert.match(await running.stderr.next(), /code=correlation_reuse$/u);
  assert.equal(readMarker(running.marker).filter((entry) => entry.event === "start").length, 1);
});

test("upstream stdout EOF is terminal and never starts a replacement", async (t) => {
  const running = await configureSidecar({ mode: "eof" });
  t.after(() => fs.rmSync(running.directory, { recursive: true, force: true }));
  assert.equal(running.response.ok, true);
  const exit = await waitForExit(running.child);
  assert.equal(exit.code, 1);
  assert.match(await running.stderr.next(), /code=upstream_/u);
  assert.equal(readMarker(running.marker).filter((entry) => entry.event === "start").length, 1);
});

test("oversized and malformed local frames fail closed", async (t) => {
  const oversized = await configureSidecar({ maxFrameBytes: 4_096 });
  t.after(() => fs.rmSync(oversized.directory, { recursive: true, force: true }));
  oversized.child.stdin.write(`${"x".repeat(4_097)}\n`);
  assert.equal((await waitForExit(oversized.child)).code, 1);
  assert.match(await oversized.stderr.next(), /code=frame_too_large$/u);

  const malformed = await configureSidecar();
  t.after(() => fs.rmSync(malformed.directory, { recursive: true, force: true }));
  malformed.child.stdin.write("{not-json}\n");
  assert.equal((await waitForExit(malformed.child)).code, 1);
  assert.match(await malformed.stderr.next(), /code=invalid_json$/u);
});

test("incompatible configure fails before any upstream process is started", async () => {
  const running = spawnSidecar();
  const hello = await running.stdout.nextJson();
  assert.equal(hello.type, "hello");
  running.child.stdin.write(
    `${JSON.stringify({
      v: 1,
      type: "configure",
      id: "configure-bad",
      codexBinary: null,
      codexHome: null,
      maxFrameBytes: 33_554_432,
      maxPending: 448,
      unknownField: true,
    })}\n`,
  );
  const response = await running.stdout.nextJson();
  assert.equal(response.ok, false);
  assert.equal(response.error, "invalid_configuration");
  assert.equal((await waitForExit(running.child)).code, 1);
});
