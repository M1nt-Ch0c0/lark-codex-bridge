#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const readline = require("node:readline");
const { spawn } = require("node:child_process");

const capabilities = [
  "bounded-ndjson",
  "correlated-requests",
  "correlated-server-requests",
  "epoch-on-restart",
  "no-mutation-replay",
  "priority-control-lane",
  "stable-domain-jsonrpc",
];

process.stdout.write(`${JSON.stringify({
  protocol: "codex-sidecar-wire",
  v: 1,
  type: "hello",
  maxFrameBytes: 33_554_432,
  capabilities,
})}\n`);

const lines = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
lines.once("line", (line) => {
  const configure = JSON.parse(line);
  // The test supplies its marker path through this otherwise opaque bootstrap
  // value so the fixture still works under Rust's env_clear process policy.
  const marker = configure.codexBinary;
  const token = `bridge-sidecar-bootstrap-descendant:${marker}`;
  const descendant = spawn(
    process.execPath,
    ["-e", "setInterval(() => {}, 60_000)", token],
    { detached: false, stdio: "ignore" },
  );
  descendant.unref();
  fs.appendFileSync(
    marker,
    `${JSON.stringify({ event: "descendant", pid: descendant.pid, token })}\n`,
    "utf8",
  );
  // Deliberately never write the configure response. Aborting the Rust spawn
  // future must still terminate the entire process group immediately.
});

setInterval(() => {}, 60_000);
