#!/usr/bin/env node

// Read-only research probe for Issue #8. This intentionally supports only an
// explicitly acknowledged, unauthenticated loopback listener backed by an
// isolated Codex profile. Production external-endpoint auth is not implemented
// here. The probe never prints endpoint, profile, thread, response, or error
// contents.

import { constants as fsConstants } from "node:fs";
import { lstat, open, realpath, stat } from "node:fs/promises";
import path from "node:path";

const PROFILE_MARKER = ".lark-codex-bridge-issue8-isolated-v1";
const PROFILE_MARKER_CONTENT =
  "lark-codex-bridge issue8 isolated profile v1\n";
const MAX_ENDPOINT_BYTES = 2_048;
const MAX_PROFILE_BYTES = 4_096;
const MAX_RPC_MESSAGE_BYTES = 256 * 1_024;
const MAX_RPC_TOTAL_BYTES = 1_024 * 1_024;
const MAX_RPC_MESSAGES = 64;
const MAX_JSON_DEPTH = 64;
const MAX_JSON_NODES = 8_192;
const MAX_USER_AGENT_BYTES = 1_024;
const DEFAULT_TIMEOUT_MS = 5_000;

function report(value, exitCode) {
  process.exitCode = exitCode;
  process.stdout.write(`${JSON.stringify(value)}\n`, () => {
    if (exitCode !== 0) {
      // A failed WebSocket peer may otherwise keep its socket alive. Every
      // resource here belongs to this short-lived probe process.
      process.exit(exitCode);
    }
  });
}

function reportFailure(stage) {
  report({ ok: false, stage }, 1);
}

function utf8Length(value) {
  return Buffer.byteLength(value, "utf8");
}

function plainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function validateJsonBudget(root) {
  const pending = [{ value: root, depth: 1 }];
  let nodes = 0;

  while (pending.length > 0) {
    const { value, depth } = pending.pop();
    nodes += 1;
    if (nodes > MAX_JSON_NODES || depth > MAX_JSON_DEPTH) {
      return false;
    }

    if (value !== null && typeof value === "object") {
      const children = Array.isArray(value) ? value : Object.values(value);
      for (const child of children) {
        pending.push({ value: child, depth: depth + 1 });
      }
    }
  }

  return true;
}

function parseTimeout() {
  const configured = process.env.CODEX_SHARED_PROBE_TEST_ONLY_TIMEOUT_MS;
  if (configured === undefined) {
    return DEFAULT_TIMEOUT_MS;
  }
  if (!/^[0-9]{3,4}$/.test(configured)) {
    return null;
  }
  const timeout = Number(configured);
  return timeout >= 100 && timeout <= DEFAULT_TIMEOUT_MS ? timeout : null;
}

function validateEndpoint(value) {
  if (
    typeof value !== "string" ||
    utf8Length(value) > MAX_ENDPOINT_BYTES ||
    !/^ws:\/\/(?:127\.0\.0\.1|\[::1\])(?::[0-9]{1,5})?\/?$/.test(value)
  ) {
    return null;
  }

  let parsed;
  try {
    parsed = new URL(value);
  } catch {
    return null;
  }

  const port = Number(parsed.port);
  const clean =
    parsed.protocol === "ws:" &&
    (parsed.hostname === "127.0.0.1" || parsed.hostname === "[::1]") &&
    parsed.username === "" &&
    parsed.password === "" &&
    parsed.search === "" &&
    parsed.hash === "" &&
    parsed.port !== "" &&
    Number.isInteger(port) &&
    port >= 1 &&
    port <= 65_535 &&
    parsed.pathname === "/";
  return clean ? parsed : null;
}

async function validateProfile(value) {
  if (
    typeof value !== "string" ||
    !path.isAbsolute(value) ||
    utf8Length(value) > MAX_PROFILE_BYTES ||
    value !== path.normalize(value)
  ) {
    return null;
  }

  const [entry, canonical] = await Promise.all([lstat(value), realpath(value)]);
  if (!entry.isDirectory() || entry.isSymbolicLink() || canonical !== value) {
    return null;
  }

  const directory = await stat(value);
  if (process.platform !== "win32") {
    const currentUid = typeof process.getuid === "function" ? process.getuid() : null;
    if (
      (directory.mode & 0o777) !== 0o700 ||
      (currentUid !== null && directory.uid !== currentUid)
    ) {
      return null;
    }
  }

  const markerPath = path.join(value, PROFILE_MARKER);
  const markerEntry = await lstat(markerPath);
  if (!markerEntry.isFile() || markerEntry.isSymbolicLink()) return null;

  const markerHandle = await open(
    markerPath,
    fsConstants.O_RDONLY | (fsConstants.O_NOFOLLOW ?? 0),
  );
  try {
    const marker = await markerHandle.stat();
    if (
      !marker.isFile() ||
      marker.size !== utf8Length(PROFILE_MARKER_CONTENT) ||
      (process.platform !== "win32" && (marker.mode & 0o077) !== 0)
    ) {
      return null;
    }
    const markerContent = await markerHandle.readFile("utf8");
    return markerContent === PROFILE_MARKER_CONTENT ? canonical : null;
  } finally {
    await markerHandle.close();
  }
}

async function configuration() {
  const endpoint = validateEndpoint(process.env.CODEX_SHARED_PROBE_ENDPOINT);
  const expectedVersion = process.env.CODEX_SHARED_PROBE_EXPECTED_VERSION;
  const timeoutMs = parseTimeout();
  if (
    !endpoint ||
    typeof expectedVersion !== "string" ||
    utf8Length(expectedVersion) > 32 ||
    !/^(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$/.test(
      expectedVersion,
    ) ||
    timeoutMs === null
  ) {
    return null;
  }

  let expectedProfile;
  try {
    expectedProfile = await validateProfile(
      process.env.CODEX_SHARED_PROBE_EXPECTED_HOME,
    );
  } catch {
    return null;
  }
  if (!expectedProfile) {
    return null;
  }

  return { endpoint, expectedVersion, expectedProfile, timeoutMs };
}

function exactServerVersion(userAgent) {
  if (
    typeof userAgent !== "string" ||
    utf8Length(userAgent) > MAX_USER_AGENT_BYTES
  ) {
    return null;
  }
  const versions = [
    ...userAgent.matchAll(/\/(\d+\.\d+\.\d+)(?=\s|\()/g),
  ];
  return versions.length === 1 ? versions[0][1] : null;
}

async function reportedProfileMatches(reported, expected) {
  if (
    typeof reported !== "string" ||
    !path.isAbsolute(reported) ||
    utf8Length(reported) > MAX_PROFILE_BYTES ||
    reported !== path.normalize(reported)
  ) {
    return false;
  }
  try {
    const [entry, canonical] = await Promise.all([
      lstat(reported),
      realpath(reported),
    ]);
    return (
      entry.isDirectory() &&
      !entry.isSymbolicLink() &&
      canonical === reported &&
      canonical === expected
    );
  } catch {
    return false;
  }
}

function connectListAndClose(config, label) {
  return new Promise((resolve, reject) => {
    let socket;
    let settled = false;
    let phase = "connecting";
    let timer;
    let messages = 0;
    let totalBytes = 0;
    const seenResponseIds = new Set();

    function armTimer() {
      clearTimeout(timer);
      timer = setTimeout(() => finish(new Error("timeout")), config.timeoutMs);
    }

    function finish(error, result) {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      if (error) {
        try {
          socket?.close();
        } catch {
          // The process exits after emitting a fixed, redacted failure.
        }
        reject(error);
      } else {
        resolve(result);
      }
    }

    function send(value) {
      try {
        socket.send(JSON.stringify(value));
      } catch {
        finish(new Error("send"));
      }
    }

    try {
      socket = new WebSocket(config.endpoint.href);
      socket.binaryType = "arraybuffer";
    } catch {
      finish(new Error("socket"));
      return;
    }
    armTimer();

    socket.addEventListener("open", () => {
      if (settled || phase !== "connecting") {
        finish(new Error("state"));
        return;
      }
      phase = "initialize";
      armTimer();
      send({
        method: "initialize",
        id: 1,
        params: {
          clientInfo: {
            name: `lark_codex_bridge_issue8_probe_${label}`,
            title: "Issue 8 read-only probe",
            version: "0.0.0",
          },
        },
      });
    });

    socket.addEventListener("message", async (event) => {
      if (settled || phase === "closing" || typeof event.data !== "string") {
        finish(new Error("frame"));
        return;
      }

      messages += 1;
      const messageBytes = utf8Length(event.data);
      totalBytes += messageBytes;
      if (
        messages > MAX_RPC_MESSAGES ||
        messageBytes > MAX_RPC_MESSAGE_BYTES ||
        totalBytes > MAX_RPC_TOTAL_BYTES
      ) {
        finish(new Error("budget"));
        return;
      }

      let message;
      try {
        message = JSON.parse(event.data);
      } catch {
        finish(new Error("decode"));
        return;
      }
      if (!plainObject(message) || !validateJsonBudget(message)) {
        finish(new Error("shape"));
        return;
      }

      if (!Object.hasOwn(message, "id")) {
        if (
          typeof message.method !== "string" ||
          utf8Length(message.method) > 256
        ) {
          finish(new Error("notification"));
        }
        return;
      }

      const id = message.id;
      if (
        !Number.isSafeInteger(id) ||
        (id !== 1 && id !== 2) ||
        seenResponseIds.has(id) ||
        Object.hasOwn(message, "error") ||
        Object.hasOwn(message, "method") ||
        !Object.hasOwn(message, "result")
      ) {
        finish(new Error("correlation"));
        return;
      }
      seenResponseIds.add(id);

      if (id === 1) {
        if (phase !== "initialize" || !plainObject(message.result)) {
          finish(new Error("initialize"));
          return;
        }
        const version = exactServerVersion(message.result.userAgent);
        if (
          version !== config.expectedVersion ||
          !(await reportedProfileMatches(
            message.result.codexHome,
            config.expectedProfile,
          ))
        ) {
          finish(new Error("identity"));
          return;
        }

        phase = "list";
        armTimer();
        send({ method: "initialized", params: {} });
        send({ method: "thread/list", id: 2, params: { limit: 1 } });
        return;
      }

      if (
        phase !== "list" ||
        !plainObject(message.result) ||
        !Array.isArray(message.result.data) ||
        message.result.data.length > 1
      ) {
        finish(new Error("list"));
        return;
      }

      phase = "closing";
      armTimer();
      try {
        socket.close(1000, "probe-complete");
      } catch {
        finish(new Error("close"));
      }
    });

    socket.addEventListener("error", () => {
      // Codex 0.149.0 promptly drops the TCP connection after a client close,
      // which Node reports as an error followed by close code 1006. Keep the
      // close deadline armed so a peer that ignores close still fails.
      if (phase !== "closing") {
        finish(new Error("socket"));
      }
    });
    socket.addEventListener("close", (event) => {
      if (settled || phase !== "closing") {
        finish(new Error("closed"));
        return;
      }
      const cleanHandshake = event.code === 1000 && event.wasClean;
      const observedCodexAbruptClose = event.code === 1006 && !event.wasClean;
      if (!cleanHandshake && !observedCodexAbruptClose) {
        finish(new Error("close-code"));
        return;
      }
      finish(null, cleanHandshake);
    });
  });
}

async function exactHealth(config) {
  const healthUrl = new URL("/healthz", config.endpoint);
  healthUrl.protocol = "http:";
  const response = await fetch(healthUrl, {
    redirect: "error",
    signal: AbortSignal.timeout(config.timeoutMs),
  });
  try {
    return response.status === 200;
  } finally {
    await response.body?.cancel();
  }
}

const config = await configuration();
if (!config) {
  reportFailure("configuration");
} else {
  let firstCloseHandshakes;
  try {
    firstCloseHandshakes = await Promise.all([
      connectListAndClose(config, "a"),
      connectListAndClose(config, "b"),
    ]);
  } catch {
    reportFailure("protocol");
  }

  if (process.exitCode === undefined) {
    let healthy = false;
    try {
      healthy = await exactHealth(config);
    } catch {
      // The fixed health-stage result deliberately omits response details.
    }
    if (!healthy) {
      reportFailure("health");
    }
  }

  let freshCloseHandshake;
  if (process.exitCode === undefined) {
    try {
      freshCloseHandshake = await connectListAndClose(config, "c");
    } catch {
      reportFailure("protocol");
    }
  }

  if (process.exitCode === undefined) {
    report(
      {
        ok: true,
        exactVersionVerified: true,
        isolatedProfileVerified: true,
        twoClientsInitializedAndDisconnected: true,
        twoClientCloseHandshakesClean: firstCloseHandshakes.every(Boolean),
        healthAfterClientDisconnect: true,
        freshClientInitializedAndDisconnected: true,
        freshClientCloseHandshakeClean: freshCloseHandshake,
      },
      0,
    );
  }
}
