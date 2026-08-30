#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const ROOT = path.resolve(__dirname, "..");
const tests = fs
  .readdirSync(path.join(ROOT, "test"))
  .filter((entry) => entry.endsWith(".test.cjs"))
  .sort()
  .map((entry) => path.join("test", entry));

if (tests.length === 0) {
  process.stderr.write("codex_sidecar_test_failure code=no_tests_found\n");
  process.exitCode = 1;
} else {
  const result = spawnSync(
    process.execPath,
    ["--test", "--test-concurrency=1", ...tests],
    { cwd: ROOT, stdio: "inherit", windowsHide: true },
  );
  if (result.error !== undefined) {
    process.stderr.write("codex_sidecar_test_failure code=test_runner_spawn_failed\n");
    process.exitCode = 1;
  } else if (result.signal !== null) {
    process.stderr.write("codex_sidecar_test_failure code=test_runner_signaled\n");
    process.exitCode = 1;
  } else {
    process.exitCode = result.status ?? 1;
  }
}
