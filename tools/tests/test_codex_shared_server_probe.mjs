import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { chmod, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";

const PROBE = new URL("../codex_shared_server_probe.mjs", import.meta.url);
const MARKER = ".lark-codex-bridge-issue8-isolated-v1";
const MARKER_CONTENT = "lark-codex-bridge issue8 isolated profile v1\n";
const EXPECTED_VERSION = "0.149.0";
const MAX_CAPTURE_BYTES = 4_096;

function serverFrame(opcode, payload = Buffer.alloc(0)) {
  const data = Buffer.isBuffer(payload) ? payload : Buffer.from(payload, "utf8");
  let header;
  if (data.length <= 125) {
    header = Buffer.from([0x80 | opcode, data.length]);
  } else if (data.length <= 65_535) {
    header = Buffer.alloc(4);
    header[0] = 0x80 | opcode;
    header[1] = 126;
    header.writeUInt16BE(data.length, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x80 | opcode;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(data.length), 2);
  }
  return Buffer.concat([header, data]);
}

function textFrame(value) {
  return serverFrame(0x1, JSON.stringify(value));
}

function consumeClientFrames(state, chunk, onFrame) {
  state.buffer = Buffer.concat([state.buffer, chunk]);
  while (state.buffer.length >= 2) {
    const first = state.buffer[0];
    const second = state.buffer[1];
    const final = (first & 0x80) !== 0;
    const masked = (second & 0x80) !== 0;
    const opcode = first & 0x0f;
    let payloadLength = second & 0x7f;
    let offset = 2;

    if (!final || !masked) {
      throw new Error("invalid client frame");
    }
    if (payloadLength === 126) {
      if (state.buffer.length < 4) return;
      payloadLength = state.buffer.readUInt16BE(2);
      offset = 4;
    } else if (payloadLength === 127) {
      if (state.buffer.length < 10) return;
      const wideLength = state.buffer.readBigUInt64BE(2);
      if (wideLength > BigInt(Number.MAX_SAFE_INTEGER)) {
        throw new Error("oversized client frame");
      }
      payloadLength = Number(wideLength);
      offset = 10;
    }

    const frameLength = offset + 4 + payloadLength;
    if (state.buffer.length < frameLength) return;
    const mask = state.buffer.subarray(offset, offset + 4);
    const payload = Buffer.from(
      state.buffer.subarray(offset + 4, frameLength),
    );
    for (let index = 0; index < payload.length; index += 1) {
      payload[index] ^= mask[index % 4];
    }
    state.buffer = state.buffer.subarray(frameLength);
    onFrame(opcode, payload);
  }
}

class FakeCodexServer {
  constructor(scenario, expectedProfile) {
    this.scenario = scenario;
    this.expectedProfile = expectedProfile;
    this.sockets = new Set();
    this.server = createServer((request, response) => {
      if (request.url !== "/healthz") {
        response.writeHead(404).end();
        return;
      }
      if (scenario === "health-redirect") {
        response.writeHead(302, { location: "/healthz" }).end();
      } else if (scenario === "health-no-content") {
        response.writeHead(204).end();
      } else {
        response.writeHead(200, { "content-type": "text/plain" }).end("ok");
      }
    });
    this.server.on("upgrade", (request, socket) => {
      const key = request.headers["sec-websocket-key"];
      if (typeof key !== "string") {
        socket.destroy();
        return;
      }
      const accept = createHash("sha1")
        .update(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`)
        .digest("base64");
      socket.write(
        "HTTP/1.1 101 Switching Protocols\r\n" +
          "Upgrade: websocket\r\n" +
          "Connection: Upgrade\r\n" +
          `Sec-WebSocket-Accept: ${accept}\r\n\r\n`,
      );
      socket.setNoDelay(true);
      this.sockets.add(socket);
      const state = { buffer: Buffer.alloc(0) };
      socket.on("data", (chunk) => {
        try {
          consumeClientFrames(state, chunk, (opcode, payload) =>
            this.onFrame(socket, opcode, payload),
          );
        } catch {
          socket.destroy();
        }
      });
      socket.on("close", () => this.sockets.delete(socket));
      socket.on("error", () => this.sockets.delete(socket));
    });
  }

  initializeResult() {
    if (this.scenario === "empty-initialize") return {};
    return {
      userAgent:
        this.scenario === "wrong-version"
          ? "codex_cli_rs/0.148.0 (fake)"
          : `codex_cli_rs/${EXPECTED_VERSION} (fake)`,
      codexHome:
        this.scenario === "wrong-home"
          ? `${this.expectedProfile}-different`
          : this.expectedProfile,
      platformFamily: "unix",
      platformOs: "fake",
    };
  }

  onFrame(socket, opcode, payload) {
    if (opcode === 0x8) {
      if (this.scenario === "abrupt-close") {
        socket.destroy();
      } else if (this.scenario !== "ignore-close") {
        const code = Buffer.alloc(2);
        code.writeUInt16BE(1000);
        socket.write(serverFrame(0x8, code), () => socket.end());
      }
      return;
    }
    if (opcode === 0x9) {
      socket.write(serverFrame(0x0a, payload));
      return;
    }
    if (opcode !== 0x1) {
      socket.destroy();
      return;
    }

    let request;
    try {
      request = JSON.parse(payload.toString("utf8"));
    } catch {
      socket.destroy();
      return;
    }

    if (request.method === "initialize" && request.id === 1) {
      const response = { id: 1, result: this.initializeResult() };
      if (this.scenario === "unknown-id") {
        socket.write(textFrame({ id: 3, result: {} }));
      } else if (this.scenario === "result-and-error") {
        socket.write(
          textFrame({ ...response, error: { code: -1, message: "fake" } }),
        );
      } else if (this.scenario === "deep-json") {
        let nested = null;
        for (let depth = 0; depth < 70; depth += 1) nested = { next: nested };
        socket.write(textFrame({ ...response, extra: nested }));
      } else if (this.scenario === "json-work") {
        socket.write(
          textFrame({ ...response, extra: Array(8_200).fill(null) }),
        );
      } else if (this.scenario === "binary") {
        socket.write(serverFrame(0x2, JSON.stringify(response)));
      } else if (this.scenario === "oversized") {
        socket.write(serverFrame(0x1, "x".repeat(256 * 1_024 + 1)));
      } else if (this.scenario === "duplicate-id") {
        socket.write(Buffer.concat([textFrame(response), textFrame(response)]));
      } else {
        socket.write(textFrame(response));
      }
      return;
    }

    if (request.method === "thread/list" && request.id === 2) {
      if (this.scenario === "notification-flood") {
        const notifications = [];
        for (let index = 0; index < 65; index += 1) {
          notifications.push(
            textFrame({ method: "probe/noop", params: { index } }),
          );
        }
        socket.write(Buffer.concat(notifications));
      } else if (this.scenario === "aggregate-overflow") {
        const notifications = [];
        for (let index = 0; index < 5; index += 1) {
          notifications.push(
            textFrame({
              method: "probe/noop",
              params: { index, padding: "a".repeat(220 * 1_024) },
            }),
          );
        }
        socket.write(Buffer.concat(notifications));
      } else if (this.scenario === "list-over-limit") {
        socket.write(textFrame({ id: 2, result: { data: [{}, {}] } }));
      } else {
        socket.write(textFrame({ id: 2, result: { data: [] } }));
      }
    }
  }

  async listen() {
    await new Promise((resolve, reject) => {
      this.server.once("error", reject);
      this.server.listen(0, "127.0.0.1", resolve);
    });
    const address = this.server.address();
    assert(address && typeof address !== "string");
    return `ws://127.0.0.1:${address.port}/`;
  }

  async close() {
    for (const socket of this.sockets) socket.destroy();
    await new Promise((resolve, reject) => {
      this.server.close((error) => (error ? reject(error) : resolve()));
    });
  }
}

async function isolatedProfile(withMarker = true) {
  const created = await mkdtemp(path.join(tmpdir(), "issue8-probe-test-"));
  const canonical = await realpath(created);
  await chmod(canonical, 0o700);
  if (withMarker) {
    const marker = path.join(canonical, MARKER);
    await writeFile(marker, MARKER_CONTENT, { mode: 0o600 });
    await chmod(marker, 0o600);
  }
  return { created, canonical };
}

async function runProbe(endpoint, profile, overrides = {}) {
  const environment = {
    ...process.env,
    CODEX_SHARED_PROBE_ENDPOINT: endpoint,
    CODEX_SHARED_PROBE_EXPECTED_VERSION: EXPECTED_VERSION,
    CODEX_SHARED_PROBE_EXPECTED_HOME: profile,
    CODEX_SHARED_PROBE_TEST_ONLY_TIMEOUT_MS: "150",
    ...overrides,
  };
  const child = spawn(process.execPath, [PROBE.pathname], {
    env: environment,
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  let captureExceeded = false;
  const capture = (current, chunk) => {
    const next = current + chunk.toString("utf8");
    if (Buffer.byteLength(next, "utf8") > MAX_CAPTURE_BYTES) {
      captureExceeded = true;
      child.kill("SIGKILL");
    }
    return next;
  };
  child.stdout.on("data", (chunk) => {
    stdout = capture(stdout, chunk);
  });
  child.stderr.on("data", (chunk) => {
    stderr = capture(stderr, chunk);
  });

  const result = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new Error("probe test timed out"));
    }, 4_000);
    child.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.once("close", (code, signal) => {
      clearTimeout(timer);
      resolve({ code, signal });
    });
  });
  assert.equal(captureExceeded, false);
  assert.equal(result.signal, null);
  assert.equal(stderr, "");
  const lines = stdout.trim().split("\n");
  assert.equal(lines.length, 1);
  return { code: result.code, output: JSON.parse(lines[0]) };
}

async function scenario(name, expected) {
  const profile = await isolatedProfile();
  const server = new FakeCodexServer(name, profile.canonical);
  try {
    const endpoint = await server.listen();
    const result = await runProbe(endpoint, profile.canonical);
    assert.deepEqual(result, expected);
  } finally {
    await server.close();
    await rm(profile.created, { recursive: true, force: true });
  }
}

const protocolFailure = { code: 1, output: { ok: false, stage: "protocol" } };
const healthFailure = { code: 1, output: { ok: false, stage: "health" } };

test("accepts exact identity, clean closes, exact health, and reconnect", async () => {
  await scenario("success", {
    code: 0,
    output: {
      ok: true,
      exactVersionVerified: true,
      isolatedProfileVerified: true,
      twoClientsInitializedAndDisconnected: true,
      twoClientCloseHandshakesClean: true,
      healthAfterClientDisconnect: true,
      freshClientInitializedAndDisconnected: true,
      freshClientCloseHandshakeClean: true,
    },
  });
});

test("reports a prompt abrupt peer close without calling it clean", async () => {
  await scenario("abrupt-close", {
    code: 0,
    output: {
      ok: true,
      exactVersionVerified: true,
      isolatedProfileVerified: true,
      twoClientsInitializedAndDisconnected: true,
      twoClientCloseHandshakesClean: false,
      healthAfterClientDisconnect: true,
      freshClientInitializedAndDisconnected: true,
      freshClientCloseHandshakeClean: false,
    },
  });
});

for (const name of [
  "ignore-close",
  "empty-initialize",
  "wrong-version",
  "wrong-home",
  "binary",
  "oversized",
  "aggregate-overflow",
  "deep-json",
  "json-work",
  "unknown-id",
  "result-and-error",
  "duplicate-id",
  "notification-flood",
  "list-over-limit",
]) {
  test(`fails closed for ${name}`, async () => {
    await scenario(name, protocolFailure);
  });
}

for (const name of ["health-redirect", "health-no-content"]) {
  test(`requires exact HTTP 200 for ${name}`, async () => {
    await scenario(name, healthFailure);
  });
}

test("the old isolation acknowledgement cannot replace profile proof", async () => {
  const profile = await isolatedProfile(false);
  const server = new FakeCodexServer("success", profile.canonical);
  try {
    const endpoint = await server.listen();
    const result = await runProbe(endpoint, profile.canonical, {
      CODEX_SHARED_PROBE_ISOLATED: "1",
    });
    assert.deepEqual(result, {
      code: 1,
      output: { ok: false, stage: "configuration" },
    });
  } finally {
    await server.close();
    await rm(profile.created, { recursive: true, force: true });
  }
});
