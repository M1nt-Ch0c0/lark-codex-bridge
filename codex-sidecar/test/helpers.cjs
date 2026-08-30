"use strict";

const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const readline = require("node:readline");
const { spawn } = require("node:child_process");

const ROOT = path.resolve(__dirname, "..");
const ENTRYPOINT = path.join(ROOT, "index.cjs");
const FIXTURE = path.join(__dirname, "fixtures", "fake-codex.cjs");

class LineInbox {
  constructor(stream) {
    this.lines = [];
    this.waiters = [];
    this.closed = false;
    this.reader = readline.createInterface({ input: stream, crlfDelay: Infinity });
    this.reader.on("line", (line) => {
      const waiter = this.waiters.shift();
      if (waiter === undefined) {
        this.lines.push(line);
      } else {
        waiter.resolve(line);
      }
    });
    this.reader.on("close", () => {
      this.closed = true;
      for (const waiter of this.waiters.splice(0)) {
        waiter.reject(new Error("line stream closed"));
      }
    });
  }

  async next(timeoutMs = 5_000) {
    if (this.lines.length > 0) {
      return this.lines.shift();
    }
    if (this.closed) {
      throw new Error("line stream closed");
    }
    return new Promise((resolve, reject) => {
      const waiter = { resolve, reject };
      this.waiters.push(waiter);
      const timer = setTimeout(() => {
        const index = this.waiters.indexOf(waiter);
        if (index >= 0) {
          this.waiters.splice(index, 1);
        }
        reject(new Error("timed out waiting for protocol line"));
      }, timeoutMs);
      waiter.resolve = (line) => {
        clearTimeout(timer);
        resolve(line);
      };
      waiter.reject = (error) => {
        clearTimeout(timer);
        reject(error);
      };
    });
  }

  async nextJson(timeoutMs = 5_000) {
    return JSON.parse(await this.next(timeoutMs));
  }

  close() {
    this.reader.close();
  }
}

function temporaryMarker() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "codex-sidecar-test-"));
  return { directory, marker: path.join(directory, "events.ndjson") };
}

function readMarker(marker) {
  if (!fs.existsSync(marker)) {
    return [];
  }
  return fs
    .readFileSync(marker, "utf8")
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

function spawnSidecar() {
  const child = spawn(process.execPath, [ENTRYPOINT], {
    cwd: ROOT,
    stdio: ["pipe", "pipe", "pipe"],
    env: { PATH: process.env.PATH ?? "", SystemRoot: process.env.SystemRoot ?? "" },
    windowsHide: true,
  });
  const stdout = new LineInbox(child.stdout);
  const stderr = new LineInbox(child.stderr);
  return { child, stdout, stderr };
}

async function configureSidecar(options = {}) {
  const running = spawnSidecar();
  const hello = await running.stdout.nextJson();
  const temporary = options.temporary ?? temporaryMarker();
  const version = options.version ?? "0.151.0";
  const mode = options.mode ?? "normal";
  const configure = {
    v: 1,
    type: "configure",
    id: "configure-1",
    codexBinary: process.execPath,
    codexHome: null,
    codexArguments: [
      FIXTURE,
      `--fake-version=${version}`,
      `--fake-mode=${mode}`,
      `--fake-marker=${temporary.marker}`,
    ],
    maxFrameBytes: options.maxFrameBytes ?? 33_554_432,
    maxPending: options.maxPending ?? 448,
  };
  running.child.stdin.write(`${JSON.stringify(configure)}\n`);
  const response = await running.stdout.nextJson();
  return { ...running, hello, response, ...temporary };
}

async function waitForExit(child, timeoutMs = 5_000) {
  if (child.exitCode !== null || child.signalCode !== null) {
    return { code: child.exitCode, signal: child.signalCode };
  }
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("timed out waiting for child exit")), timeoutMs);
    child.once("exit", (code, signal) => {
      clearTimeout(timer);
      resolve({ code, signal });
    });
  });
}

function send(child, value) {
  child.stdin.write(`${JSON.stringify(value)}\n`);
}

module.exports = {
  ENTRYPOINT,
  FIXTURE,
  LineInbox,
  configureSidecar,
  readMarker,
  send,
  spawnSidecar,
  temporaryMarker,
  waitForExit,
};
