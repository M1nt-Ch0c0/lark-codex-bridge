#!/usr/bin/env node
"use strict";

const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const readline = require("node:readline");
const { spawn } = require("node:child_process");

const EXPECTED_CODEX_VERSION = "0.151.0";
const EXPECTED_PROTOCOL = "codex-sidecar-wire";
const EXPECTED_WIRE_VERSION = 1;
const SOURCE_ROOT = fs.realpathSync(path.resolve(__dirname, ".."));
const RUNTIME_ROOT_OVERRIDE = process.env.CODEX_SIDECAR_RUNTIME_ROOT;
let ROOT =
  typeof RUNTIME_ROOT_OVERRIDE === "string"
    ? path.resolve(RUNTIME_ROOT_OVERRIDE)
    : SOURCE_ROOT;
const TIMEOUT_MS = 30_000;
const DISABLED_NETWORK_ENDPOINT = "http://127.0.0.1:9/disabled";

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function sha256(file) {
  const hash = crypto.createHash("sha256");
  hash.update(fs.readFileSync(file));
  return hash.digest("hex");
}

function pathIsWithin(root, candidate) {
  const relative = path.relative(root, candidate);
  return (
    relative === "" ||
    (!path.isAbsolute(relative) &&
      relative !== ".." &&
      !relative.startsWith(`..${path.sep}`))
  );
}

function canonicalExistingRoot(candidate) {
  return fs.realpathSync(path.resolve(candidate));
}

function artifactPaths(directory, relative = "") {
  const paths = [];
  for (const name of fs.readdirSync(path.join(directory, relative)).sort()) {
    const childRelative = path.join(relative, name);
    if (childRelative === "artifact-manifest.json") {
      continue;
    }
    const child = path.join(directory, childRelative);
    if (fs.lstatSync(child).isDirectory()) {
      paths.push(...artifactPaths(directory, childRelative));
    } else {
      paths.push(childRelative.split(path.sep).join("/"));
    }
  }
  return paths;
}

function verifyPinnedRuntime() {
  const packageJson = readJson(path.join(ROOT, "package.json"));
  const lock = readJson(path.join(ROOT, "package-lock.json"));
  const installed = readJson(
    path.join(ROOT, "node_modules", "@openai", "codex", "package.json"),
  );
  const nativePackage = `codex-${process.platform}-${process.arch}`;
  const installedNative = readJson(
    path.join(ROOT, "node_modules", "@openai", nativePackage, "package.json"),
  );
  assert.equal(packageJson.dependencies?.["@openai/codex"], EXPECTED_CODEX_VERSION);
  assert.equal(
    lock.packages?.[""]?.dependencies?.["@openai/codex"],
    EXPECTED_CODEX_VERSION,
  );
  assert.equal(
    lock.packages?.["node_modules/@openai/codex"]?.version,
    EXPECTED_CODEX_VERSION,
  );
  assert.equal(installed.version, EXPECTED_CODEX_VERSION);
  assert.equal(
    installedNative.version,
    `${EXPECTED_CODEX_VERSION}-${process.platform}-${process.arch}`,
  );
  const nodeModulesRoot = canonicalExistingRoot(path.join(ROOT, "node_modules"));
  const codexEntrypoint = fs.realpathSync(
    require.resolve("@openai/codex/bin/codex.js", { paths: [ROOT] }),
  );
  assert.ok(pathIsWithin(nodeModulesRoot, codexEntrypoint));
}

function verifyArtifactManifest() {
  const manifestFile = path.join(ROOT, "artifact-manifest.json");
  if (process.env.CODEX_SIDECAR_REQUIRE_ARTIFACT !== "1") {
    return;
  }
  assert.equal(
    typeof RUNTIME_ROOT_OVERRIDE === "string" &&
      path.isAbsolute(RUNTIME_ROOT_OVERRIDE),
    true,
    "the downloaded runtime root must be explicit and absolute",
  );
  assert.notEqual(ROOT, SOURCE_ROOT, "the downloaded runtime must be independent");
  const expectedManifestSha256 = process.env.CODEX_SIDECAR_EXPECTED_MANIFEST_SHA256;
  assert.match(
    expectedManifestSha256 ?? "",
    /^[a-f0-9]{64}$/u,
    "a trusted manifest SHA-256 is required",
  );
  assert.ok(fs.existsSync(manifestFile), "unpacked artifact manifest is required");
  assert.equal(
    sha256(manifestFile),
    expectedManifestSha256,
    "downloaded manifest must match the trusted build output",
  );
  assert.ok(
    !fs.existsSync(path.join(ROOT, "ci")),
    "the runtime artifact must not contain CI verifier code",
  );
  assert.ok(
    !fs.existsSync(path.join(ROOT, "test", "fixtures", "fake-codex.cjs")),
    "the runtime artifact must not contain the fake test upstream",
  );
  const manifest = readJson(manifestFile);
  assert.equal(manifest.schema, "lark-codex-bridge/codex-sidecar-artifact/v1");
  assert.equal(manifest.codexVersion, EXPECTED_CODEX_VERSION);
  assert.equal(manifest.platform, process.platform);
  assert.equal(manifest.arch, process.arch);
  assert.ok(Array.isArray(manifest.files) && manifest.files.length > 0);
  const listedPaths = manifest.files.map((entry) => entry.path).sort();
  assert.equal(new Set(listedPaths).size, listedPaths.length);
  assert.deepEqual(artifactPaths(ROOT).sort(), listedPaths);
  for (const entry of manifest.files) {
    const file = path.resolve(ROOT, entry.path);
    assert.ok(
      file !== ROOT && pathIsWithin(ROOT, file),
      "artifact inventory paths must remain below the artifact root",
    );
    if (Object.hasOwn(entry, "symlink")) {
      assert.equal(fs.lstatSync(file).isSymbolicLink(), true);
      assert.equal(fs.readlinkSync(file), entry.symlink);
      const target = path.resolve(path.dirname(file), entry.symlink);
      assert.ok(
        pathIsWithin(ROOT, target),
        "artifact symlinks must remain below the artifact root",
      );
    } else {
      assert.equal(fs.lstatSync(file).isFile(), true);
      assert.equal(fs.lstatSync(file).size, entry.size);
      assert.equal(sha256(file), entry.sha256);
      assert.equal(typeof entry.executable, "boolean");
    }
  }
  if (process.platform !== "win32") {
    for (const entry of manifest.files) {
      if (!Object.hasOwn(entry, "symlink")) {
        fs.chmodSync(path.resolve(ROOT, entry.path), entry.executable ? 0o755 : 0o644);
      }
    }
  }
}

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
        waiter.reject(new Error("protocol stream closed"));
      }
    });
  }

  async nextJson(timeoutMs = TIMEOUT_MS) {
    let line;
    if (this.lines.length > 0) {
      line = this.lines.shift();
    } else {
      assert.equal(this.closed, false, "protocol stream closed");
      line = await new Promise((resolve, reject) => {
        const waiter = { resolve, reject };
        this.waiters.push(waiter);
        const timer = setTimeout(() => {
          const index = this.waiters.indexOf(waiter);
          if (index >= 0) {
            this.waiters.splice(index, 1);
          }
          reject(new Error("timed out waiting for protocol frame"));
        }, timeoutMs);
        waiter.resolve = (value) => {
          clearTimeout(timer);
          resolve(value);
        };
        waiter.reject = (error) => {
          clearTimeout(timer);
          reject(error);
        };
      });
    }
    return JSON.parse(line);
  }
}

async function receive(inbox, predicate) {
  for (let count = 0; count < 64; count += 1) {
    const frame = await inbox.nextJson();
    if (predicate(frame)) {
      return frame;
    }
  }
  throw new Error("expected protocol frame was not observed");
}

function send(child, frame) {
  child.stdin.write(`${JSON.stringify(frame)}\n`);
}

function waitForExit(child) {
  if (child.exitCode !== null || child.signalCode !== null) {
    return Promise.resolve({ code: child.exitCode, signal: child.signalCode });
  }
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error("timed out waiting for sidecar shutdown")),
      TIMEOUT_MS,
    );
    child.once("exit", (code, signal) => {
      clearTimeout(timer);
      resolve({ code, signal });
    });
  });
}

function inheritedValue(environment, name) {
  const expected = name.toUpperCase();
  for (const [key, value] of Object.entries(environment)) {
    if (key.toUpperCase() === expected && typeof value === "string") {
      return value;
    }
  }
  return undefined;
}

function isolatedEnvironment(root, inherited = process.env, platform = process.platform) {
  const emptyBin = path.join(root, "empty-bin");
  const temporary = path.join(root, "tmp");
  const localAppData = path.join(root, "local-app-data");
  const appData = path.join(root, "app-data");
  const xdgConfig = path.join(root, "xdg-config");
  const xdgCache = path.join(root, "xdg-cache");
  const xdgData = path.join(root, "xdg-data");
  const xdgState = path.join(root, "xdg-state");
  const corepackHome = path.join(root, "corepack-home");
  const npmCache = path.join(root, "npm-cache");
  for (const directory of [
    emptyBin,
    temporary,
    localAppData,
    appData,
    xdgConfig,
    xdgCache,
    xdgData,
    xdgState,
    corepackHome,
    npmCache,
  ]) {
    fs.mkdirSync(directory, { recursive: true });
  }

  // Start from an allowlist rather than copying the runner environment. In
  // particular, NODE_OPTIONS/NODE_PATH/NODE_EXTRA_CA_CERTS, dynamic-loader
  // injection, inherited credentials, and host proxy configuration never
  // reach either the downloaded Node runtime or its pinned Codex child.
  // The offline package-manager settings below are defense in depth against
  // downloads; they deliberately make no claim to be an OS-level socket sandbox.
  const environment = {
    PATH: emptyBin,
    HOME: root,
    USER: "codex-sidecar-ci",
    LOGNAME: "codex-sidecar-ci",
    USERNAME: "codex-sidecar-ci",
    USERPROFILE: root,
    LOCALAPPDATA: localAppData,
    APPDATA: appData,
    XDG_CONFIG_HOME: xdgConfig,
    XDG_CACHE_HOME: xdgCache,
    XDG_DATA_HOME: xdgData,
    XDG_STATE_HOME: xdgState,
    TMPDIR: temporary,
    TMP: temporary,
    TEMP: temporary,
    NO_COLOR: "1",
    COREPACK_ENABLE_DOWNLOAD_PROMPT: "0",
    COREPACK_ENABLE_NETWORK: "0",
    COREPACK_HOME: corepackHome,
    npm_config_offline: "true",
    npm_config_ignore_scripts: "true",
    npm_config_cache: npmCache,
    npm_config_registry: DISABLED_NETWORK_ENDPOINT,
    npm_config_proxy: DISABLED_NETWORK_ENDPOINT,
    npm_config_https_proxy: DISABLED_NETWORK_ENDPOINT,
    npm_config_fetch_retries: "0",
    npm_config_fetch_timeout: "1000",
    HTTP_PROXY: DISABLED_NETWORK_ENDPOINT,
    HTTPS_PROXY: DISABLED_NETWORK_ENDPOINT,
    ALL_PROXY: DISABLED_NETWORK_ENDPOINT,
    NO_PROXY: "",
    http_proxy: DISABLED_NETWORK_ENDPOINT,
    https_proxy: DISABLED_NETWORK_ENDPOINT,
    all_proxy: DISABLED_NETWORK_ENDPOINT,
    no_proxy: "",
  };
  for (const name of ["LANG", "LC_ALL", "LC_CTYPE"]) {
    const value = inheritedValue(inherited, name);
    if (value !== undefined) {
      environment[name] = value;
    }
  }
  if (platform === "win32") {
    const systemRoot =
      inheritedValue(inherited, "SystemRoot") ?? inheritedValue(inherited, "WINDIR");
    if (systemRoot !== undefined && path.win32.isAbsolute(systemRoot)) {
      environment.SystemRoot = systemRoot;
      environment.WINDIR = systemRoot;
      environment.ComSpec = path.win32.join(systemRoot, "System32", "cmd.exe");
      environment.PATHEXT = ".COM;.EXE;.BAT;.CMD";
    }
  }
  return environment;
}

function environmentSelfTest() {
  const temporary = fs.mkdtempSync(path.join(os.tmpdir(), "codex-sidecar-env-test-"));
  try {
    const environment = isolatedEnvironment(temporary, {
      PATH: "/host/bin",
      LANG: "C.UTF-8",
      CODEX_HOME: "/credential-home",
      OPENAI_API_KEY: "must-not-cross",
      NODE_OPTIONS: "--require=/tmp/must-not-cross.cjs",
      NODE_PATH: "/tmp/must-not-cross",
      NODE_EXTRA_CA_CERTS: "/tmp/must-not-cross.pem",
      NODE_DEBUG: "must-not-cross",
      LD_PRELOAD: "/tmp/must-not-cross.so",
      LD_LIBRARY_PATH: "/tmp/must-not-cross",
      DYLD_INSERT_LIBRARIES: "/tmp/must-not-cross.dylib",
      DYLD_LIBRARY_PATH: "/tmp/must-not-cross",
      BASH_ENV: "/tmp/must-not-cross.sh",
      ENV: "/tmp/must-not-cross.sh",
      PYTHONPATH: "/tmp/must-not-cross",
      RUBYOPT: "-rmust-not-cross",
      PERL5OPT: "-Mmust-not-cross",
      JAVA_TOOL_OPTIONS: "-agentpath:/tmp/must-not-cross",
      HTTP_PROXY: "http://host-proxy.invalid",
      npm_config_registry: "https://registry.npmjs.org",
    });
    for (const forbidden of [
      "CODEX_HOME",
      "OPENAI_API_KEY",
      "NODE_OPTIONS",
      "NODE_PATH",
      "NODE_EXTRA_CA_CERTS",
      "LD_PRELOAD",
      "DYLD_INSERT_LIBRARIES",
      "BASH_ENV",
      "ENV",
      "PYTHONPATH",
      "RUBYOPT",
      "PERL5OPT",
      "JAVA_TOOL_OPTIONS",
    ]) {
      assert.equal(inheritedValue(environment, forbidden), undefined);
    }
    assert.equal(
      Object.keys(environment).some((key) => /^(?:NODE_|LD_|DYLD_)/iu.test(key)),
      false,
    );
    assert.equal(environment.LANG, "C.UTF-8");
    assert.equal(environment.HTTP_PROXY, DISABLED_NETWORK_ENDPOINT);
    assert.equal(environment.npm_config_registry, DISABLED_NETWORK_ENDPOINT);
    assert.equal(environment.npm_config_offline, "true");
    assert.notEqual(environment.PATH, "/host/bin");
    assert.ok(fs.statSync(environment.PATH).isDirectory());

    const windowsRoot = path.join(temporary, "windows");
    fs.mkdirSync(windowsRoot);
    const windowsEnvironment = isolatedEnvironment(
      windowsRoot,
      {
        SystemRoot: "C:\\Windows",
        NODE_OPTIONS: "--require=C:\\must-not-cross.cjs",
      },
      "win32",
    );
    assert.equal(windowsEnvironment.SystemRoot, "C:\\Windows");
    assert.equal(windowsEnvironment.WINDIR, "C:\\Windows");
    assert.equal(windowsEnvironment.ComSpec, "C:\\Windows\\System32\\cmd.exe");
    assert.equal(windowsEnvironment.PATHEXT, ".COM;.EXE;.BAT;.CMD");
    assert.equal(windowsEnvironment.NODE_OPTIONS, undefined);

    const runtimeRoot = path.join(temporary, "runtime-root");
    const runtimeAlias = path.join(temporary, "runtime-alias");
    fs.mkdirSync(runtimeRoot);
    fs.symlinkSync(
      runtimeRoot,
      runtimeAlias,
      process.platform === "win32" ? "junction" : "dir",
    );
    assert.equal(canonicalExistingRoot(runtimeAlias), fs.realpathSync(runtimeRoot));
    const canonicalRuntimeRoot = fs.realpathSync(runtimeRoot);
    assert.equal(
      pathIsWithin(canonicalExistingRoot(runtimeAlias), path.join(canonicalRuntimeRoot, "x")),
      true,
    );
    assert.equal(
      pathIsWithin(canonicalRuntimeRoot, `${canonicalRuntimeRoot}-sibling`),
      false,
    );
    process.stdout.write("codex_sidecar_isolated_environment_self_test_ok\n");
  } finally {
    fs.rmSync(temporary, { recursive: true, force: true });
  }
}

async function main() {
  ROOT = canonicalExistingRoot(ROOT);
  verifyArtifactManifest();
  verifyPinnedRuntime();

  const temporary = fs.mkdtempSync(path.join(os.tmpdir(), "codex-sidecar-real-smoke-"));
  const codexHome = path.join(temporary, "codex-home");
  fs.mkdirSync(codexHome);
  const child = spawn(process.execPath, [path.join(ROOT, "index.cjs")], {
    cwd: ROOT,
    env: isolatedEnvironment(temporary),
    stdio: ["pipe", "pipe", "pipe"],
    windowsHide: true,
  });
  const stdout = new LineInbox(child.stdout);
  const stderr = [];
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk) => stderr.push(chunk));

  try {
    const hello = await stdout.nextJson();
    assert.equal(hello.protocol, EXPECTED_PROTOCOL);
    assert.equal(hello.v, EXPECTED_WIRE_VERSION);
    assert.equal(hello.type, "hello");
    assert.ok(hello.capabilities.includes("stable-domain-jsonrpc"));

    send(child, {
      v: EXPECTED_WIRE_VERSION,
      type: "configure",
      id: "configure-real-artifact-smoke",
      codexBinary: null,
      codexHome,
      codexArguments: [],
      maxFrameBytes: 33_554_432,
      maxPending: 448,
    });
    const configured = await receive(
      stdout,
      (frame) => frame.id === "configure-real-artifact-smoke",
    );
    assert.equal(configured.ok, true);
    assert.equal(configured.data.upstreamVersion, EXPECTED_CODEX_VERSION);
    assert.equal(configured.data.adapterVersion, EXPECTED_CODEX_VERSION);

    send(child, {
      id: "initialize-real-artifact-smoke",
      method: "initialize",
      params: {
        clientInfo: {
          name: "lark-codex-bridge-ci",
          version: "0.1.0",
        },
      },
    });
    const initialized = await receive(
      stdout,
      (frame) => frame.id === "initialize-real-artifact-smoke",
    );
    assert.ok(!Object.hasOwn(initialized, "error"));
    assert.equal(typeof initialized.result.userAgent, "string");
    send(child, { method: "initialized" });

    send(child, {
      id: "thread-list-real-artifact-smoke",
      method: "thread/list",
      params: { limit: 1 },
    });
    const listed = await receive(
      stdout,
      (frame) => frame.id === "thread-list-real-artifact-smoke",
    );
    assert.ok(!Object.hasOwn(listed, "error"));
    assert.ok(Array.isArray(listed.result.data));

    send(child, {
      id: "shutdown-real-artifact-smoke",
      method: "sidecar/shutdown",
      params: {},
    });
    const shutdown = await receive(
      stdout,
      (frame) => frame.id === "shutdown-real-artifact-smoke",
    );
    assert.deepEqual(shutdown.result, {});
    const exit = await waitForExit(child);
    assert.deepEqual(exit, { code: 0, signal: null });
    assert.deepEqual(stderr, []);
    process.stdout.write(
      `codex_sidecar_real_smoke_ok version=${EXPECTED_CODEX_VERSION} source=pinned-package\n`,
    );
  } finally {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill("SIGKILL");
    }
    fs.rmSync(temporary, { recursive: true, force: true });
  }
}

if (require.main === module) {
  if (process.argv.includes("--self-test")) {
    try {
      environmentSelfTest();
    } catch {
      process.stderr.write("codex_sidecar_real_smoke_failure code=environment_self_test_failed\n");
      process.exitCode = 1;
    }
  } else {
    main().catch(() => {
      process.stderr.write("codex_sidecar_real_smoke_failure code=smoke_failed\n");
      process.exitCode = 1;
    });
  }
}

module.exports = {
  canonicalExistingRoot,
  inheritedValue,
  isolatedEnvironment,
  pathIsWithin,
};
