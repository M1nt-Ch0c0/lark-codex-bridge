#!/usr/bin/env node
"use strict";

const assert = require("node:assert/strict");
const crypto = require("node:crypto");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const EXPECTED_CODEX_VERSION = "0.151.0";
const ROOT = path.resolve(__dirname, "..");
const REAL_ROOT = fs.realpathSync(ROOT);
const RUNTIME_ENTRIES = [
  "README.md",
  "adapters",
  "index.cjs",
  "node_modules",
  "package-lock.json",
  "package.json",
  "priority-write-queue.cjs",
  "session.cjs",
  "upstream.cjs",
  "wire.cjs",
];

class ArtifactBuildFailure extends Error {
  constructor(code) {
    super(code);
    this.name = "ArtifactBuildFailure";
    this.code = code;
  }
}

function fail(code) {
  throw new ArtifactBuildFailure(code);
}

function reportFailure(error) {
  const code = error instanceof ArtifactBuildFailure ? error.code : "build_failed";
  process.stderr.write(`codex_sidecar_artifact_failure code=${code}\n`);
  process.exitCode = 1;
}

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function assertPinnedGraph() {
  const packageJson = readJson(path.join(ROOT, "package.json"));
  const lock = readJson(path.join(ROOT, "package-lock.json"));
  const installed = readJson(
    path.join(ROOT, "node_modules", "@openai", "codex", "package.json"),
  );
  const nativePackage = `codex-${process.platform}-${process.arch}`;
  const installedNative = readJson(
    path.join(ROOT, "node_modules", "@openai", nativePackage, "package.json"),
  );

  if (
    packageJson.dependencies?.["@openai/codex"] !== EXPECTED_CODEX_VERSION ||
    lock.packages?.[""]?.dependencies?.["@openai/codex"] !== EXPECTED_CODEX_VERSION ||
    lock.packages?.["node_modules/@openai/codex"]?.version !== EXPECTED_CODEX_VERSION ||
    installed.version !== EXPECTED_CODEX_VERSION ||
    installedNative.version !== `${EXPECTED_CODEX_VERSION}-${process.platform}-${process.arch}`
  ) {
    fail("unpinned_dependency_graph");
  }
}

function digestFile(file) {
  const hash = crypto.createHash("sha256");
  hash.update(fs.readFileSync(file));
  return hash.digest("hex");
}

function inventory(directory, relative = "") {
  const entries = [];
  const current = path.join(directory, relative);
  for (const name of fs.readdirSync(current).sort()) {
    const childRelative = path.join(relative, name);
    const child = path.join(directory, childRelative);
    const metadata = fs.lstatSync(child);
    const portablePath = childRelative.split(path.sep).join("/");
    if (metadata.isDirectory()) {
      entries.push(...inventory(directory, childRelative));
    } else if (metadata.isSymbolicLink()) {
      const target = fs.readlinkSync(child);
      const resolvedTarget = path.resolve(path.dirname(child), target);
      const relativeTarget = path.relative(directory, resolvedTarget);
      if (
        path.isAbsolute(relativeTarget) ||
        relativeTarget === ".." ||
        relativeTarget.startsWith(`..${path.sep}`)
      ) {
        fail("artifact_symlink_escape");
      }
      entries.push({ path: portablePath, symlink: target });
    } else if (metadata.isFile()) {
      entries.push({
        path: portablePath,
        sha256: digestFile(child),
        size: metadata.size,
        executable: (metadata.mode & 0o111) !== 0,
      });
    } else {
      fail("unsupported_artifact_entry");
    }
  }
  return entries;
}

function validateSourceSymlinks(candidate) {
  const metadata = fs.lstatSync(candidate);
  if (metadata.isSymbolicLink()) {
    const resolved = fs.realpathSync(candidate);
    if (fs.statSync(candidate).isDirectory()) {
      fail("source_directory_symlink");
    }
    if (!pathIsWithin(REAL_ROOT, resolved)) {
      fail("source_symlink_escape");
    }
    return;
  }
  if (metadata.isDirectory()) {
    for (const name of fs.readdirSync(candidate)) {
      validateSourceSymlinks(path.join(candidate, name));
    }
  }
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

function pathEntryExists(candidate) {
  try {
    fs.lstatSync(candidate);
    return true;
  } catch (error) {
    if (error && (error.code === "ENOENT" || error.code === "ENOTDIR")) {
      return false;
    }
    throw error;
  }
}

// Resolve a path that does not exist yet by canonicalizing its nearest existing
// ancestor, then appending the still-missing suffix. This catches an existing
// symlink or Windows junction in any parent component before mkdir follows it.
function prospectiveRealPath(candidate) {
  let current = path.resolve(candidate);
  const missing = [];
  while (!pathEntryExists(current)) {
    const parent = path.dirname(current);
    if (parent === current) {
      fail("stage_parent_unavailable");
    }
    missing.unshift(path.basename(current));
    current = parent;
  }
  return path.resolve(fs.realpathSync(current), ...missing);
}

function requireAbsoluteOutsideSource(candidate, label, sourceRoot = REAL_ROOT) {
  if (typeof candidate !== "string" || !path.isAbsolute(candidate)) {
    fail(`${label}_not_absolute`);
  }
  const resolved = path.resolve(candidate);
  if (pathEntryExists(resolved)) {
    fail(`${label}_already_exists`);
  }
  const canonicalSource = fs.realpathSync(sourceRoot);
  if (pathIsWithin(canonicalSource, prospectiveRealPath(resolved))) {
    fail(`${label}_inside_source`);
  }
  return resolved;
}

function verifyCreatedOutsideSource(candidate, label, sourceRoot = REAL_ROOT) {
  const canonicalSource = fs.realpathSync(sourceRoot);
  const canonicalCandidate = fs.realpathSync(candidate);
  if (pathIsWithin(canonicalSource, canonicalCandidate)) {
    fail(`${label}_inside_source`);
  }
  return canonicalCandidate;
}

function main() {
  assertPinnedGraph();

  const stageRoot = requireAbsoluteOutsideSource(
    process.env.CODEX_SIDECAR_ARTIFACT_DIR,
    "stage",
  );
  const artifactRoot = path.join(stageRoot, "codex-sidecar");
  fs.mkdirSync(artifactRoot, { recursive: true });
  const canonicalStageRoot = verifyCreatedOutsideSource(stageRoot, "stage");
  const canonicalArtifactRoot = verifyCreatedOutsideSource(artifactRoot, "artifact");
  if (
    !pathIsWithin(canonicalStageRoot, canonicalArtifactRoot) ||
    path.dirname(canonicalArtifactRoot) !== canonicalStageRoot
  ) {
    fail("artifact_outside_stage");
  }

  for (const entry of RUNTIME_ENTRIES) {
    const source = path.join(ROOT, entry);
    const destination = path.join(artifactRoot, entry);
    if (!fs.existsSync(source)) {
      fail("runtime_entry_missing");
    }
    validateSourceSymlinks(source);
    fs.cpSync(source, destination, {
      recursive: true,
      force: false,
      errorOnExist: true,
      preserveTimestamps: true,
      dereference: true,
    });
  }

  const manifest = {
    schema: "lark-codex-bridge/codex-sidecar-artifact/v1",
    codexVersion: EXPECTED_CODEX_VERSION,
    platform: process.platform,
    arch: process.arch,
    files: inventory(artifactRoot),
  };
  const manifestFile = path.join(artifactRoot, "artifact-manifest.json");
  fs.writeFileSync(
    manifestFile,
    `${JSON.stringify(manifest, null, 2)}\n`,
    { encoding: "utf8", flag: "wx" },
  );

  const manifestSha256 = digestFile(manifestFile);
  if (typeof process.env.GITHUB_OUTPUT === "string") {
    fs.appendFileSync(
      process.env.GITHUB_OUTPUT,
      `manifest_sha256=${manifestSha256}\n`,
      "utf8",
    );
  }
  process.stdout.write(
    `codex_sidecar_artifact_ok version=${EXPECTED_CODEX_VERSION} platform=${process.platform} arch=${process.arch} manifest_sha256=${manifestSha256}\n`,
  );
}

function selfTest() {
  const temporary = fs.mkdtempSync(path.join(os.tmpdir(), "codex-sidecar-artifact-path-test-"));
  try {
    const source = path.join(temporary, "source");
    const outside = path.join(temporary, "outside");
    fs.mkdirSync(source);
    fs.mkdirSync(outside);

    const redirect = path.join(temporary, "redirect");
    fs.symlinkSync(source, redirect, process.platform === "win32" ? "junction" : "dir");
    assert.throws(
      () => requireAbsoluteOutsideSource(path.join(redirect, "stage"), "stage", source),
      (error) => error instanceof ArtifactBuildFailure && error.code === "stage_inside_source",
    );

    const safeStage = path.join(outside, "stage");
    assert.equal(
      requireAbsoluteOutsideSource(safeStage, "stage", source),
      path.resolve(safeStage),
    );
    fs.mkdirSync(safeStage);
    assert.equal(verifyCreatedOutsideSource(safeStage, "stage", source), fs.realpathSync(safeStage));

    const swapped = path.join(temporary, "swapped");
    fs.symlinkSync(source, swapped, process.platform === "win32" ? "junction" : "dir");
    assert.throws(
      () => verifyCreatedOutsideSource(swapped, "stage", source),
      (error) => error instanceof ArtifactBuildFailure && error.code === "stage_inside_source",
    );
    process.stdout.write("codex_sidecar_artifact_path_self_test_ok\n");
  } finally {
    fs.rmSync(temporary, { recursive: true, force: true });
  }
}

if (require.main === module) {
  try {
    if (process.argv.includes("--self-test")) {
      selfTest();
    } else {
      main();
    }
  } catch (error) {
    reportFailure(error);
  }
}

module.exports = {
  ArtifactBuildFailure,
  pathIsWithin,
  prospectiveRealPath,
  requireAbsoluteOutsideSource,
  verifyCreatedOutsideSource,
};
