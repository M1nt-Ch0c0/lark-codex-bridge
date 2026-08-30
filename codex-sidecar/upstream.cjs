"use strict";

const { spawn } = require("node:child_process");
const { once } = require("node:events");

const { adapterForVersion, SUPPORTED_UPSTREAM_VERSIONS } = require("./adapters/index.cjs");
const { SidecarError } = require("./wire.cjs");

const VERSION_PROBE_TIMEOUT_MS = 5_000;
const VERSION_OUTPUT_LIMIT = 4_096;
const TRANSIENT_SPAWN_CODES = new Set(["EAGAIN", "EMFILE", "ENFILE", "ENOMEM"]);

function delay(milliseconds) {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, milliseconds);
    timer.unref();
  });
}

function shutdownError() {
  return new SidecarError("shutdown_requested", "sidecar shutdown was requested");
}

function parseVersionOutput(output) {
  return (
    SUPPORTED_UPSTREAM_VERSIONS.find(
      (version) =>
        output === `codex-cli ${version}` ||
        output === `codex-cli ${version}\n` ||
        output === `codex-cli ${version}\r\n`,
    ) ?? null
  );
}

async function raceWithAbort(promise, signal) {
  if (signal === undefined) {
    return { aborted: false, value: await promise };
  }
  if (signal.aborted) {
    return { aborted: true, value: undefined };
  }
  let onAbort;
  const aborted = new Promise((resolve) => {
    onAbort = () => resolve({ aborted: true, value: undefined });
    signal.addEventListener("abort", onAbort, { once: true });
  });
  try {
    return await Promise.race([
      promise.then((value) => ({ aborted: false, value })),
      aborted,
    ]);
  } finally {
    signal.removeEventListener("abort", onAbort);
  }
}

async function waitUntilOrTimeout(promise, milliseconds, timeoutValue) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((resolve) => {
        // Cleanup must keep an otherwise idle process alive until the hard
        // bound, while a prompt exit must cancel the leftover timer.
        timer = setTimeout(() => resolve(timeoutValue), milliseconds);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function safeEnvironment(codexHome) {
  const allowed = [
    "PATH",
    "Path",
    "SystemRoot",
    "WINDIR",
    "PATHEXT",
    "COMSPEC",
    "TMPDIR",
    "TMP",
    "TEMP",
    "HOME",
    "USERPROFILE",
    "LOCALAPPDATA",
    "APPDATA",
    "XDG_CONFIG_HOME",
    "LANG",
    "LC_ALL",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
  ];
  const environment = {};
  for (const name of allowed) {
    if (typeof process.env[name] === "string") {
      environment[name] = process.env[name];
    }
  }
  if (codexHome !== null) {
    environment.CODEX_HOME = codexHome;
  }
  return environment;
}

function pinnedCodexCommand() {
  let script;
  try {
    script = require.resolve("@openai/codex/bin/codex.js");
  } catch {
    throw new SidecarError("pinned_codex_missing", "pinned Codex package is unavailable");
  }
  return { command: process.execPath, prefixArguments: [script] };
}

function configuredCommand(configuration) {
  if (configuration.codexBinary === null) {
    const pinned = pinnedCodexCommand();
    return {
      command: pinned.command,
      prefixArguments: [...pinned.prefixArguments, ...configuration.codexArguments],
    };
  }
  return {
    command: configuration.codexBinary,
    prefixArguments: [...configuration.codexArguments],
  };
}

function spawnFailureCode(error, operation) {
  const unavailable =
    error !== null &&
    typeof error === "object" &&
    typeof error.code === "string" &&
    TRANSIENT_SPAWN_CODES.has(error.code);
  return `${operation}_spawn_${unavailable ? "unavailable" : "failed"}`;
}

function spawnSafe(command, arguments_, options, operation) {
  try {
    return spawn(command, arguments_, {
      ...options,
      shell: false,
      windowsHide: true,
    });
  } catch (error) {
    throw new SidecarError(
      spawnFailureCode(error, operation),
      "unable to start configured Codex binary",
    );
  }
}

function collectBounded(stream, maximum, onLimit) {
  return new Promise((resolve, reject) => {
    const pieces = [];
    let length = 0;
    stream.on("data", (raw) => {
      const chunk = Buffer.isBuffer(raw) ? raw : Buffer.from(raw);
      if (length + chunk.length > maximum) {
        onLimit();
        reject(new SidecarError("version_output_too_large", "Codex version output is too large"));
        return;
      }
      length += chunk.length;
      pieces.push(chunk);
    });
    stream.once("end", () => resolve(Buffer.concat(pieces, length)));
    stream.once("error", () =>
      reject(new SidecarError("version_probe_io", "unable to read Codex version output")),
    );
  });
}

async function terminateChild(child, graceMs) {
  if (child.exitCode !== null || child.signalCode !== null) {
    closeChildPipes(child);
    return;
  }
  if (child.stdin && !child.stdin.destroyed) {
    child.stdin.end();
  }
  const exitWait = new AbortController();
  const exited = once(child, "exit", { signal: exitWait.signal }).then(() => true, () => true);
  try {
    if (await waitUntilOrTimeout(exited, graceMs, false)) {
      closeChildPipes(child);
      return;
    }
    try {
      child.kill("SIGKILL");
    } catch {
      // The outer Rust process-group/Job owner remains authoritative.
    }
    await waitUntilOrTimeout(exited, Math.min(graceMs, 1_000), false);
    closeChildPipes(child);
  } finally {
    // A child that ignores SIGKILL must not retain the waiter's exit/error
    // listeners after the hard cleanup bound expires.
    exitWait.abort();
  }
}

function closeChildPipes(child) {
  for (const stream of [child.stdin, child.stdout, child.stderr]) {
    if (stream && !stream.destroyed) {
      stream.destroy();
    }
  }
}

async function probeVersion(configuration, signal) {
  if (signal?.aborted) {
    throw shutdownError();
  }
  const selected = configuredCommand(configuration);
  const child = spawnSafe(
    selected.command,
    [...selected.prefixArguments, "--version"],
    {
      env: safeEnvironment(configuration.codexHome),
      stdio: ["ignore", "pipe", "pipe"],
    },
    "version_probe",
  );
  let killedForLimit = false;
  const killForLimit = () => {
    if (!killedForLimit) {
      killedForLimit = true;
      try {
        child.kill("SIGKILL");
      } catch {
        // Spawn/error classification below remains content-free.
      }
    }
  };
  const stdout = collectBounded(child.stdout, VERSION_OUTPUT_LIMIT, killForLimit);
  const stderr = collectBounded(child.stderr, VERSION_OUTPUT_LIMIT, killForLimit);
  const exit = new Promise((resolve, reject) => {
    child.once("error", (error) =>
      reject(
        new SidecarError(
          spawnFailureCode(error, "version_probe"),
          "unable to run Codex version probe",
        ),
      ),
    );
    child.once("exit", (code, signal) => resolve({ code, signal }));
  });

  const probe = await raceWithAbort(
    Promise.race([
      Promise.all([stdout, stderr, exit]),
      delay(VERSION_PROBE_TIMEOUT_MS).then(() => null),
    ]),
    signal,
  );
  if (probe.aborted || signal?.aborted) {
    await terminateChild(child, 100);
    throw shutdownError();
  }
  const timed = probe.value;
  if (timed === null) {
    await terminateChild(child, 100);
    throw new SidecarError("version_probe_timeout", "Codex version probe timed out");
  }
  const [stdoutBytes, , status] = timed;
  if (status.code !== 0 || status.signal !== null) {
    throw new SidecarError("version_probe_failed", "Codex version probe failed");
  }
  const output = stdoutBytes.toString("utf8");
  const version = parseVersionOutput(output);
  if (version === null) {
    throw new SidecarError(
      "unsupported_upstream_version",
      `Codex version is unsupported; expected ${SUPPORTED_UPSTREAM_VERSIONS.join(" or ")}`,
    );
  }
  const adapter = adapterForVersion(version);
  if (adapter === null) {
    throw new SidecarError("unsupported_upstream_version", "Codex version has no adapter");
  }
  return { version, adapter, command: selected };
}

async function startAppServer(configuration, probed, signal) {
  if (signal?.aborted) {
    throw shutdownError();
  }
  const child = spawnSafe(
    probed.command.command,
    [...probed.command.prefixArguments, "app-server", "--listen", "stdio://"],
    {
      env: safeEnvironment(configuration.codexHome),
      stdio: ["pipe", "pipe", "pipe"],
    },
    "upstream",
  );
  const startup = await raceWithAbort(
    Promise.race([
      once(child, "spawn").then(() => ({ ok: true })),
      once(child, "error").then(([error]) => ({ ok: false, error })),
    ]),
    signal,
  );
  if (startup.aborted || signal?.aborted) {
    await terminateChild(child, 100);
    throw shutdownError();
  }
  const spawned = startup.value;
  if (!spawned.ok) {
    throw new SidecarError(
      spawnFailureCode(spawned.error, "upstream"),
      "unable to start Codex app-server",
    );
  }
  return child;
}

async function drainStderr(stream) {
  try {
    for await (const _chunk of stream) {
      // Deliberately discard provider stderr. Content, paths, and credentials
      // never enter sidecar errors or logs.
    }
  } catch {
    // The stdout/exit lifecycle is authoritative and remains observable.
  }
}

module.exports = {
  VERSION_OUTPUT_LIMIT,
  drainStderr,
  parseVersionOutput,
  pinnedCodexCommand,
  probeVersion,
  safeEnvironment,
  spawnFailureCode,
  startAppServer,
  terminateChild,
};
