#!/usr/bin/env node
"use strict";

const fs = require("node:fs");
const net = require("node:net");
const readline = require("node:readline");
const { spawn } = require("node:child_process");
const { randomUUID } = require("node:crypto");

function option(name, fallback = "") {
  const prefix = `--fake-${name}=`;
  const value = process.argv.find((argument) => argument.startsWith(prefix));
  return value === undefined ? fallback : value.slice(prefix.length);
}

const version = option("version", "0.151.0");
const mode = option("mode", "normal");
const marker = option("marker");
const mutationCrashMarker = `${marker}.mutation-crashed`;

function mark(event, details = {}) {
  if (marker.length === 0) {
    return;
  }
  fs.appendFileSync(marker, `${JSON.stringify({ event, ...details })}\n`, "utf8");
}

if (process.argv.includes("--version")) {
  process.stdout.write(`codex-cli ${version}\n`);
  process.exitCode = 0;
  return;
}

if (!process.argv.includes("app-server")) {
  process.exitCode = 2;
  return;
}

mark("start", { version, pid: process.pid });

function spawnDescendant() {
  const descendantToken = `bridge-sidecar-descendant:${process.pid}:${randomUUID()}`;
  const descendant = spawn(
    process.execPath,
    [
      "-e",
      [
        'const net = require("node:net");',
        "const server = net.createServer((socket) => socket.destroy());",
        'server.listen(0, "127.0.0.1", () => {',
        "  const address = server.address();",
        '  process.stdout.write(`${address.port}\\n`);',
        "});",
      ].join("\n"),
      descendantToken,
    ],
    { detached: false, stdio: ["ignore", "pipe", "ignore"] },
  );
  const ready = readline.createInterface({ input: descendant.stdout, crlfDelay: Infinity });
  ready.once("line", (line) => {
    const port = Number.parseInt(line, 10);
    if (Number.isSafeInteger(port) && port > 0 && port <= 65_535) {
      mark("descendant", { pid: descendant.pid, token: descendantToken, port });
    }
    ready.close();
    descendant.stdout.destroy();
  });
  descendant.unref();
}

function previousDescendantPort() {
  if (marker.length === 0) {
    return null;
  }
  try {
    const events = fs
      .readFileSync(marker, "utf8")
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line));
    const descendant = events.findLast((event) => event.event === "descendant");
    return Number.isSafeInteger(descendant?.port) ? descendant.port : null;
  } catch {
    return null;
  }
}

function endpointIsAlive(port) {
  return new Promise((resolve) => {
    const socket = net.createConnection({ host: "127.0.0.1", port });
    const timer = setTimeout(() => {
      socket.destroy();
      resolve(false);
    }, 250);
    const finish = (alive) => {
      clearTimeout(timer);
      socket.destroy();
      resolve(alive);
    };
    socket.once("connect", () => finish(true));
    socket.once("error", () => finish(false));
  });
}

const crashWithDescendant = mode === "crash-once-with-descendant";
const replacementAfterDescendantCrash =
  crashWithDescendant && fs.existsSync(mutationCrashMarker);
if (mode === "leave-descendant" || (crashWithDescendant && !replacementAfterDescendantCrash)) {
  spawnDescendant();
}

const lines = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
if (replacementAfterDescendantCrash) {
  lines.pause();
  const port = previousDescendantPort();
  const check = port === null ? Promise.resolve(null) : endpointIsAlive(port);
  check.then((alive) => {
    mark("replacement-descendant-check", { observed: port !== null, alive });
    lines.resume();
  });
}

function send(value) {
  process.stdout.write(`${JSON.stringify(value)}\n`);
}

function stableThread() {
  return {
    id: "thread-fake",
    sessionId: "thread-fake",
    preview: "",
    modelProvider: "openai",
    createdAt: 1_786_478_400,
    updatedAt: 1_786_478_400,
    status: { type: "idle" },
    ephemeral: false,
    turns: [],
    source: "appServer",
    cliVersion: version,
    cwd: process.cwd(),
  };
}

function crashFirstMutation() {
  if (marker.length === 0) {
    return true;
  }
  try {
    const descriptor = fs.openSync(mutationCrashMarker, "wx");
    fs.closeSync(descriptor);
    return true;
  } catch (error) {
    if (error && error.code === "EEXIST") {
      return false;
    }
    throw error;
  }
}

lines.on("line", (line) => {
  let message;
  try {
    message = JSON.parse(line);
  } catch {
    mark("invalid-json");
    process.exit(3);
    return;
  }

  if (Object.hasOwn(message, "method") && Object.hasOwn(message, "id")) {
    mark("request", { method: message.method });
    if (
      (mode === "crash-once-mutation" || mode === "crash-once-with-descendant") &&
      message.method === "thread/start" &&
      crashFirstMutation()
    ) {
      mark("crash-before-response", { method: message.method });
      process.exit(23);
      return;
    }

    let result;
    switch (message.method) {
      case "initialize":
        result = {
          codexHome: process.cwd(),
          platformFamily: "test",
          platformOs: process.platform,
          userAgent: `fake-codex/${version}`,
        };
        break;
      case "thread/list":
        result = { data: [], nextCursor: null, backwardsCursor: null };
        break;
      case "thread/start":
        result = {
          thread: stableThread(),
          approvalPolicy: "on-request",
          approvalsReviewer: "user",
          cwd: process.cwd(),
          model: "gpt-test",
          modelProvider: "openai",
          sandbox: {
            type: "workspaceWrite",
            writableRoots: [process.cwd()],
            networkAccess: false,
            excludeTmpdirEnvVar: false,
            excludeSlashTmp: false,
          },
          instructionSources: [],
        };
        break;
      default:
        result = {};
        break;
    }
    send({ id: message.id, result });
    return;
  }

  if (message.method === "initialized") {
    mark("initialized");
  }
});

lines.on("close", () => {
  mark("stdin-eof");
  process.exitCode = 0;
});

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.once(signal, () => {
    mark("signal", { signal });
    process.exit(0);
  });
}
