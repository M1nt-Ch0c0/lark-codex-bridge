#!/usr/bin/env python3
"""Deterministic Codex app-server schema maintenance.

This tool is deliberately outside Cargo's build graph.  `sync` is the only
command that executes Codex; `verify`, `diff`, and `contract` operate entirely
on committed artifacts and are safe in an offline build/test environment.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, NoReturn


GENERATOR_NAME = "lark-codex-bridge/codex-schema"
GENERATOR_VERSION = "1.1.0"
MANIFEST_FORMAT_VERSION = 1
SCHEMA_BUNDLE_FORMAT_VERSION = 1
CONTRACT_FORMAT_VERSION = 1
AUDIT_FORMAT_VERSION = 1
HISTORY_FORMAT_VERSION = 1
ESTABLISHED_BASELINE_VERSION = "0.146.0"
ESTABLISHED_BASELINE_SCHEMA_SHA256 = "8f949f41d0de731f26d264db686a90469a817837f83050c47487045745a3b3a6"
MAX_CAPTURE_BYTES = 64 * 1024
MAX_ARTIFACT_BYTES = 64 * 1024 * 1024
SCHEMA_VALIDATION_RECURSION_LIMIT = 512
VERSION_TIMEOUT_SECONDS = 10
GENERATION_TIMEOUT_SECONDS = 120
VERSION_RE = re.compile(rb"codex-cli (0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:\r?\n)?\Z")

REPO_ROOT = Path(__file__).resolve().parent.parent
PROTOCOL_ROOT = REPO_ROOT / "protocol" / "codex"
SELECTION_PATH = PROTOCOL_ROOT / "selection.json"
POLICY_PATH = PROTOCOL_ROOT / "support-policy.json"
SCHEMAS_ROOT = PROTOCOL_ROOT / "schemas"
CONTRACTS_ROOT = PROTOCOL_ROOT / "contracts"
REPORTS_ROOT = PROTOCOL_ROOT / "reports"
WIRE_ROOT = REPO_ROOT / "src" / "codex" / "wire"
WIRE_TEMPLATE_PATH = REPO_ROOT / "tools" / "codex-wire-template.rs"
HISTORY_PATH = PROTOCOL_ROOT / "support-history.json"


class SchemaToolError(Exception):
    """A sanitized maintenance failure; it never contains wire payloads."""


@dataclass(frozen=True)
class SelectionRoot:
    name: str
    path: str


@dataclass(frozen=True)
class Selection:
    protocol_family: str
    generator_arguments: tuple[str, ...]
    roots: tuple[SelectionRoot, ...]
    notification_catalog: str


@dataclass(frozen=True)
class BoundedProcessResult:
    returncode: int
    stdout: bytes
    stderr: bytes
    overflowed: bool
    timed_out: bool


def _signal_process_group(process: subprocess.Popen[bytes], signal_number: int) -> None:
    try:
        if os.name == "posix":
            os.killpg(process.pid, signal_number)
        else:
            if signal_number == signal.SIGTERM:
                process.terminate()
            else:
                process.kill()
    except OSError:
        pass


def _stop_process(process: subprocess.Popen[bytes]) -> None:
    _signal_process_group(process, signal.SIGTERM)
    if process.poll() is None:
        try:
            process.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            _signal_process_group(process, signal.SIGKILL)
    if process.poll() is None:
        try:
            process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            pass


def run_bounded(command: list[str], *, timeout: float) -> BoundedProcessResult:
    """Run a command while concurrently draining and bounding both output pipes."""
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=os.name == "posix",
        )
    except OSError as error:
        raise SchemaToolError("external command could not start") from error
    assert process.stdout is not None and process.stderr is not None
    overflow = threading.Event()
    reader_failed = threading.Event()
    captures = [bytearray(), bytearray()]

    def drain(pipe: Any, capture: bytearray) -> None:
        try:
            while chunk := pipe.read(8192):
                remaining = MAX_CAPTURE_BYTES - len(capture)
                if remaining > 0:
                    capture.extend(chunk[:remaining])
                if len(chunk) > remaining:
                    overflow.set()
        except OSError:
            reader_failed.set()
        finally:
            pipe.close()

    readers = [
        threading.Thread(target=drain, args=(process.stdout, captures[0]), daemon=True),
        threading.Thread(target=drain, args=(process.stderr, captures[1]), daemon=True),
    ]
    for reader in readers:
        reader.start()
    deadline = time.monotonic() + timeout
    timed_out = False
    inherited_pipe = False
    try:
        while process.poll() is None:
            if overflow.is_set():
                _stop_process(process)
                break
            if reader_failed.is_set():
                _stop_process(process)
                break
            if time.monotonic() >= deadline:
                timed_out = True
                _stop_process(process)
                break
            time.sleep(0.01)
    finally:
        for reader in readers:
            reader.join(timeout=0.25)
        if any(reader.is_alive() for reader in readers):
            # A descendant inherited stdout/stderr after the direct child
            # exited. Kill the isolated group so EOF and cleanup are bounded.
            inherited_pipe = True
            _signal_process_group(process, signal.SIGKILL)
            for reader in readers:
                reader.join(timeout=1)
        if any(reader.is_alive() for reader in readers):
            raise SchemaToolError("external command pipe cleanup did not complete")
        if process.poll() is None:
            _stop_process(process)
    if reader_failed.is_set():
        raise SchemaToolError("external command pipe read failed")
    if inherited_pipe and not (timed_out or overflow.is_set()):
        raise SchemaToolError("external command descendants retained output pipes")
    return BoundedProcessResult(
        process.returncode if process.returncode is not None else -1,
        bytes(captures[0]),
        bytes(captures[1]),
        overflow.is_set(),
        timed_out,
    )


def fail(message: str) -> NoReturn:
    raise SchemaToolError(message)


def load_json(path: Path, *, maximum: int = MAX_ARTIFACT_BYTES) -> Any:
    try:
        size = path.stat().st_size
    except OSError as error:
        raise SchemaToolError(f"required artifact is unavailable: {safe_relative(path)}") from error
    if size > maximum:
        fail(f"JSON artifact exceeds the {maximum}-byte limit: {safe_relative(path)}")
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(
                handle,
                parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
                object_pairs_hook=unique_json_object,
            )
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError, RecursionError) as error:
        raise SchemaToolError(f"invalid JSON artifact: {safe_relative(path)}") from error


def unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


def safe_relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT).as_posix()
    except (OSError, ValueError):
        return path.name


def canonical_bytes(value: Any) -> bytes:
    try:
        encoded = json.dumps(value, allow_nan=False, ensure_ascii=False, indent=2, sort_keys=True)
    except (TypeError, ValueError, RecursionError) as error:
        raise SchemaToolError("value cannot be encoded as canonical JSON") from error
    return (encoded + "\n").encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def read_selection() -> Selection:
    raw = load_json(SELECTION_PATH)
    if not isinstance(raw, dict) or raw.get("formatVersion") != 1:
        fail("unsupported Codex schema selection format")
    protocol = raw.get("protocolFamily")
    arguments = raw.get("generatorArguments")
    roots = raw.get("roots")
    catalog = raw.get("notificationCatalog")
    if not isinstance(protocol, str) or not protocol:
        fail("schema selection has no protocol family")
    if not isinstance(arguments, list) or not all(isinstance(value, str) for value in arguments):
        fail("schema selection has invalid generator arguments")
    if arguments.count("<temporary-directory>") != 1:
        fail("schema selection must contain one temporary output placeholder")
    if not isinstance(roots, list) or not roots:
        fail("schema selection has no roots")
    selected: list[SelectionRoot] = []
    names: set[str] = set()
    paths: set[str] = set()
    for raw_root in roots:
        if not isinstance(raw_root, dict):
            fail("schema selection contains an invalid root")
        name = raw_root.get("name")
        relative = raw_root.get("path")
        if not isinstance(name, str) or not isinstance(relative, str):
            fail("schema selection contains an invalid root")
        candidate = Path(relative)
        if candidate.is_absolute() or ".." in candidate.parts or candidate.suffix != ".json":
            fail(f"schema selection contains an unsafe path for root {name}")
        if name in names or relative in paths:
            fail(f"schema selection contains a duplicate root: {name}")
        names.add(name)
        paths.add(relative)
        selected.append(SelectionRoot(name, relative))
    if not isinstance(catalog, str) or Path(catalog).name != catalog:
        fail("schema selection has an invalid notification catalog")
    return Selection(protocol, tuple(arguments), tuple(selected), catalog)


def read_policy() -> dict[str, Any]:
    raw = load_json(POLICY_PATH)
    if not isinstance(raw, dict) or raw.get("formatVersion") != 1:
        fail("unsupported Codex support policy format")
    required = (
        "protocolFamily",
        "selectedWireVersion",
        "supportedVersions",
        "candidateVersions",
    )
    if any(key not in raw for key in required):
        fail("Codex support policy is incomplete")
    for key in ("supportedVersions", "candidateVersions"):
        versions = raw[key]
        if not isinstance(versions, list) or not all(is_version(value) for value in versions):
            fail(f"Codex support policy has invalid {key}")
        if versions != sorted(set(versions), key=version_key):
            fail(f"Codex support policy {key} must be sorted and unique")
    if raw["selectedWireVersion"] not in raw["supportedVersions"]:
        fail("selected wire version is not supported")
    if set(raw["supportedVersions"]) & set(raw["candidateVersions"]):
        fail("supported and candidate Codex versions overlap")
    return raw


def read_history(path: Path = HISTORY_PATH) -> dict[str, Any]:
    raw = load_json(path)
    if not isinstance(raw, dict) or raw.get("formatVersion") != HISTORY_FORMAT_VERSION:
        fail("unsupported Codex support history format")
    if raw.get("establishedBaselineVersion") != ESTABLISHED_BASELINE_VERSION:
        fail("Codex support history changed the established baseline")
    releases = raw.get("releases")
    if not isinstance(releases, list) or not releases:
        fail("Codex support history has no releases")
    seen: set[str] = set()
    for release in releases:
        if not isinstance(release, dict) or release.get("decision") != "supported":
            fail("Codex support history contains an invalid decision")
        version = release.get("version")
        if not is_version(version) or version in seen:
            fail("Codex support history contains an invalid version")
        seen.add(version)
        for key in ("schemaSha256", "contractSha256", "rustWireSha256"):
            if not isinstance(release.get(key), str) or re.fullmatch(r"[0-9a-f]{64}", release[key]) is None:
                fail(f"Codex support history has an invalid {key}")
    baseline = next(
        (release for release in releases if release["version"] == ESTABLISHED_BASELINE_VERSION),
        None,
    )
    if baseline is None or baseline["schemaSha256"] != ESTABLISHED_BASELINE_SCHEMA_SHA256:
        fail("Codex support history no longer contains the pinned baseline schema")
    return raw


def verify_history_append_only(previous_path: Path) -> None:
    previous = read_history(previous_path)
    current = read_history()
    if previous.get("protocolFamily") != current.get("protocolFamily"):
        fail("Codex support history protocol family changed")
    old_releases = previous["releases"]
    if current["releases"][: len(old_releases)] != old_releases:
        fail("Codex support history is not append-only")


def is_version(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)", value) is not None


def version_key(value: str) -> tuple[int, int, int]:
    return tuple(int(part) for part in value.split("."))  # type: ignore[return-value]


def rust_version_module(version: str) -> str:
    if not is_version(version):
        fail("Codex version is not a stable X.Y.Z value")
    return "v" + version.replace(".", "_")


def probe_version(binary: Path) -> str:
    try:
        result = run_bounded([os.fspath(binary), "--version"], timeout=VERSION_TIMEOUT_SECONDS)
    except SchemaToolError as error:
        raise SchemaToolError("Codex version probe could not complete") from error
    if result.timed_out:
        fail("Codex version probe timed out")
    if result.overflowed:
        fail("Codex version output exceeded the bounded capture limit")
    if result.returncode != 0:
        fail(f"Codex version probe exited unsuccessfully (code {result.returncode})")
    if result.stderr:
        fail("Codex version probe wrote unexpected stderr output")
    match = VERSION_RE.fullmatch(result.stdout)
    if match is None:
        fail("Codex version output must exactly match `codex-cli X.Y.Z`")
    return ".".join(part.decode("ascii") for part in match.groups())


def generate_schema_directory(binary: Path, selection: Selection) -> tuple[str, Path, tempfile.TemporaryDirectory[str]]:
    version = probe_version(binary)
    temporary = tempfile.TemporaryDirectory(prefix="lark-codex-schema-")
    output = Path(temporary.name) / "export"
    arguments = [str(output) if value == "<temporary-directory>" else value for value in selection.generator_arguments]
    try:
        result = run_bounded(
            [os.fspath(binary), *arguments], timeout=GENERATION_TIMEOUT_SECONDS
        )
    except SchemaToolError as error:
        temporary.cleanup()
        raise SchemaToolError(f"Codex {version} schema export could not complete") from error
    if result.timed_out:
        temporary.cleanup()
        fail(f"Codex {version} schema export timed out")
    if result.overflowed:
        temporary.cleanup()
        fail(f"Codex {version} schema export exceeded the bounded diagnostic limit")
    if result.returncode != 0:
        temporary.cleanup()
        fail(f"Codex {version} schema export failed (code {result.returncode})")
    if not output.is_dir():
        temporary.cleanup()
        fail(f"Codex {version} schema export produced no output directory")
    return version, output, temporary


def normalize_schema(value: Any, parent_key: str | None = None) -> Any:
    if isinstance(value, dict):
        return {key: normalize_schema(value[key], key) for key in sorted(value)}
    if isinstance(value, list):
        normalized = [normalize_schema(item) for item in value]
        if parent_key in {"required", "enum", "type"}:
            normalized.sort(key=lambda item: json.dumps(item, ensure_ascii=False, sort_keys=True))
        return normalized
    return value


def notification_methods(schema: Any) -> list[str]:
    if not isinstance(schema, dict):
        fail("notification catalog is not a JSON Schema object")
    methods: set[str] = set()
    variants = schema.get("oneOf", [])
    if not isinstance(variants, list):
        fail("notification catalog has no oneOf variants")
    for variant in variants:
        try:
            values = variant["properties"]["method"]["enum"]
        except (KeyError, TypeError):
            continue
        if isinstance(values, list):
            methods.update(value for value in values if isinstance(value, str))
    if not methods:
        fail("notification catalog contains no methods")
    return sorted(methods)


def make_bundle(export: Path, selection: Selection) -> dict[str, Any]:
    roots: dict[str, Any] = {}
    for root in selection.roots:
        source = export / root.path
        if not source.is_file():
            fail(f"Codex schema export omitted selected root {root.name}")
        roots[root.name] = normalize_schema(load_json(source))
    catalog_path = export / selection.notification_catalog
    if not catalog_path.is_file():
        fail("Codex schema export omitted the notification catalog")
    catalog = normalize_schema(load_json(catalog_path))
    return {
        "formatVersion": SCHEMA_BUNDLE_FORMAT_VERSION,
        "notificationMethods": notification_methods(catalog),
        "roots": roots,
    }


def camel_to_snake(value: str) -> str:
    first = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", value)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", first).replace("-", "_").lower()


def generated_value_fields(properties: dict[str, Any], required: set[str], excluded: set[str]) -> str:
    lines: list[str] = []
    for wire_name in sorted(set(properties) - excluded):
        rust_name = camel_to_snake(wire_name)
        if not re.fullmatch(r"[a-z_][a-z0-9_]*", rust_name):
            fail("selected schema contains a field the Rust generator cannot name safely")
        if wire_name in required:
            lines.extend(
                [
                    f'    #[serde(rename = "{wire_name}")]',
                    f"    pub {rust_name}: Value,",
                ]
            )
        else:
            lines.extend(
                [
                    f'    #[serde(default, rename = "{wire_name}", skip_serializing_if = "Option::is_none")]',
                    f"    pub {rust_name}: Option<Value>,",
                ]
            )
    return "\n".join(lines)


def render_wire(version: str, protocol_family: str, schema_sha: str, bundle: dict[str, Any]) -> bytes:
    try:
        template = WIRE_TEMPLATE_PATH.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise SchemaToolError("wire template is unavailable") from error
    roots = bundle["roots"]
    thread_response = roots["thread.start.response"]
    thread_schema = thread_response["definitions"]["Thread"]
    thread_properties = thread_schema.get("properties", {})
    thread_required = set(thread_schema.get("required", []))
    if not isinstance(thread_properties, dict):
        fail("selected Thread schema has no object properties")
    base_thread_fields = {
        "id",
        "sessionId",
        "preview",
        "modelProvider",
        "createdAt",
        "updatedAt",
        "status",
        "ephemeral",
        "turns",
        "source",
        "cliVersion",
        "cwd",
        "name",
        "path",
        "forkedFromId",
        "parentThreadId",
    }
    thread_fields = generated_value_fields(thread_properties, thread_required, base_thread_fields)

    list_schema = roots["thread.list.params"]
    list_properties = list_schema.get("properties", {})
    if not isinstance(list_properties, dict):
        fail("selected ThreadListParams schema has no object properties")
    base_list_fields = {
        "cursor",
        "limit",
        "sortKey",
        "sortDirection",
        "modelProviders",
        "sourceKinds",
        "cwd",
        "archived",
        "searchTerm",
        "useStateDbOnly",
    }
    list_fields = generated_value_fields(list_properties, set(list_schema.get("required", [])), base_list_fields)

    replacements = {
        "@GENERATOR_VERSION@": GENERATOR_VERSION,
        "@CODEX_VERSION@": version,
        "@PROTOCOL_FAMILY@": protocol_family,
        "@SCHEMA_SHA256@": schema_sha,
        "@THREAD_VERSION_FIELDS@": thread_fields,
        "@THREAD_LIST_VERSION_FIELDS@": list_fields,
    }
    rendered = template
    for marker, value in replacements.items():
        rendered = rendered.replace(marker, value)
    if re.search(r"@[A-Z][A-Z0-9_]+@", rendered):
        fail("wire template contains an unresolved generator marker")
    return rendered.encode("utf-8")


def template_sha256() -> str:
    try:
        return sha256_bytes(WIRE_TEMPLATE_PATH.read_bytes())
    except OSError as error:
        raise SchemaToolError("wire template is unavailable") from error


def manifest_for(
    version: str,
    selection: Selection,
    schema_bytes: bytes,
    wire_bytes: bytes,
    audit_bytes: bytes,
    policy: dict[str, Any],
) -> dict[str, Any]:
    module = rust_version_module(version)
    if version in policy["supportedVersions"]:
        lifecycle = "supported"
    elif version in policy["candidateVersions"]:
        lifecycle = "candidate"
    else:
        lifecycle = "unclassified"
    return {
        "formatVersion": MANIFEST_FORMAT_VERSION,
        "codexVersion": version,
        "protocolFamily": selection.protocol_family,
        "schemaSha256": sha256_bytes(schema_bytes),
        "generator": {
            "name": GENERATOR_NAME,
            "version": GENERATOR_VERSION,
            "templateSha256": template_sha256(),
        },
        "generationArguments": list(selection.generator_arguments),
        "lifecycle": lifecycle,
        "selectedRoots": [root.name for root in selection.roots],
        "artifacts": {
            "incomingAudit": f"protocol/codex/schemas/{version}/incoming-audit.json",
            "normalizedSchema": f"protocol/codex/schemas/{version}/selected.schema.json",
            "rustWire": f"src/codex/wire/{module}.rs",
        },
        "artifactSha256": {
            "incomingAudit": sha256_bytes(audit_bytes),
            "normalizedSchema": sha256_bytes(schema_bytes),
            "rustWire": sha256_bytes(wire_bytes),
        },
    }


def is_incoming_root(name: str) -> bool:
    return (
        name.endswith(".response")
        or name.startswith("notification.")
        or (name.startswith("server_request.") and name.endswith(".params"))
    )


def incoming_audit(version: str, bundle: dict[str, Any]) -> dict[str, Any]:
    entries: dict[tuple[str, str], dict[str, Any]] = {}

    def walk(value: Any, path: str) -> None:
        if isinstance(value, dict):
            logical_path = path.split("/definitions/", 1)[-1]
            if logical_path != path:
                logical_path = "definitions/" + logical_path
            for kind in ("enum", "oneOf", "anyOf"):
                if isinstance(value.get(kind), list):
                    open_name = next(
                        (
                            name
                            for name in ("TurnStatus", "MessagePhase")
                            if f"definitions/{name}" in logical_path
                        ),
                        None,
                    )
                    thread_item = logical_path == "definitions/ThreadItem" and kind in {
                        "oneOf",
                        "anyOf",
                    }
                    if open_name is not None:
                        handling = "open-string-fallback"
                        evidence = "unknown_generated_enum_values_fail_soft_at_the_stable_boundary"
                    elif thread_item:
                        handling = "open-tagged-fallback"
                        evidence = "unknown_thread_items_preserve_the_complete_raw_payload"
                    else:
                        handling = "promotion-blocking"
                        evidence = "incoming_closed_union_additions_are_breaking"
                    key = (logical_path, kind)
                    entries[key] = {
                        "schemaPath": logical_path,
                        "construct": kind,
                        "handling": handling,
                        "evidence": evidence,
                    }
            for key, child in value.items():
                if key not in {"description", "title", "default", "examples"}:
                    walk(child, f"{path}/{key}")
        elif isinstance(value, list):
            for index, child in enumerate(value):
                walk(child, f"{path}/{index}")

    for name, schema in bundle["roots"].items():
        if is_incoming_root(name):
            walk(schema, f"roots/{name}")
    ordered = sorted(entries.values(), key=lambda item: (item["schemaPath"], item["construct"]))
    return {
        "formatVersion": AUDIT_FORMAT_VERSION,
        "codexVersion": version,
        "incomingRoots": sorted(name for name in bundle["roots"] if is_incoming_root(name)),
        "constructs": ordered,
    }


def render_wire_mod(policy: dict[str, Any], versions: Iterable[str]) -> bytes:
    modules = sorted(set(versions), key=version_key)
    lines = [
        "// @generated by tools/codex_schema.py; DO NOT EDIT.",
        "//! Versioned Codex app-server wire DTOs. Stable domain types live in `types`.",
        "",
    ]
    for version in modules:
        lines.extend(["#[rustfmt::skip]", f"pub mod {rust_version_module(version)};"])
    lines.extend(
        [
            "",
            "/// Exact versions whose schema and contracts have passed review.",
        ]
    )
    quoted_versions = ", ".join(f'"{version}"' for version in policy["supportedVersions"])
    version_patterns = " | ".join(
        f"({major}, {minor}, {patch})"
        for major, minor, patch in (version_key(version) for version in policy["supportedVersions"])
    )
    lines.extend(
        [
            f"pub const SUPPORTED_CODEX_VERSIONS: &[&str] = &[{quoted_versions}];",
            "",
            "/// Returns true only for an exact, reviewed schema/contract version.",
            "#[must_use]",
            "pub fn is_supported_codex_version(version: &semver::Version) -> bool {",
            f"    matches!((version.major, version.minor, version.patch), {version_patterns})",
            "        && version.pre.is_empty()",
            "        && version.build.is_empty()",
            "}",
            "",
        ]
    )
    return "\n".join(lines).encode("utf-8")


def existing_wire_versions(extra: str | None = None) -> list[str]:
    versions: set[str] = set()
    if SCHEMAS_ROOT.is_dir():
        for child in SCHEMAS_ROOT.iterdir():
            if child.is_dir() and is_version(child.name) and (child / "manifest.json").is_file():
                versions.add(child.name)
    if extra is not None:
        versions.add(extra)
    return sorted(versions, key=version_key)


def sync(binary: Path, *, check: bool) -> str:
    selection = read_selection()
    policy = read_policy()
    version, export, temporary = generate_schema_directory(binary, selection)
    try:
        bundle = make_bundle(export, selection)
        schema_bytes = canonical_bytes(bundle)
        schema_sha = sha256_bytes(schema_bytes)
        wire_bytes = render_wire(version, selection.protocol_family, schema_sha, bundle)
        audit_bytes = canonical_bytes(incoming_audit(version, bundle))
        manifest = manifest_for(version, selection, schema_bytes, wire_bytes, audit_bytes, policy)
        manifest_bytes = canonical_bytes(manifest)
        module = rust_version_module(version)
        targets = {
            SCHEMAS_ROOT / version / "incoming-audit.json": audit_bytes,
            SCHEMAS_ROOT / version / "selected.schema.json": schema_bytes,
            SCHEMAS_ROOT / version / "manifest.json": manifest_bytes,
            WIRE_ROOT / f"{module}.rs": wire_bytes,
            WIRE_ROOT / "mod.rs": render_wire_mod(policy, existing_wire_versions(version)),
        }
        if check:
            mismatches = []
            for path, expected in targets.items():
                try:
                    actual = path.read_bytes()
                except OSError:
                    actual = b""
                if actual != expected:
                    mismatches.append(safe_relative(path))
            if mismatches:
                fail("schema sync is not reproducible for: " + ", ".join(mismatches))
        else:
            for path, content in targets.items():
                atomic_write(path, content)
        return version
    finally:
        temporary.cleanup()


def load_bundle(version: str) -> dict[str, Any]:
    if not is_version(version):
        fail("Codex version is not a stable X.Y.Z value")
    bundle = load_json(SCHEMAS_ROOT / version / "selected.schema.json")
    if not isinstance(bundle, dict) or bundle.get("formatVersion") != SCHEMA_BUNDLE_FORMAT_VERSION:
        fail(f"Codex {version} has an unsupported normalized schema format")
    if not isinstance(bundle.get("roots"), dict) or not isinstance(bundle.get("notificationMethods"), list):
        fail(f"Codex {version} has an incomplete normalized schema")
    return bundle


def change(classification: str, kind: str, path: str, **details: Any) -> dict[str, Any]:
    result = {"classification": classification, "kind": kind, "path": path}
    result.update(details)
    return result


def schema_types(schema: dict[str, Any]) -> set[str] | None:
    raw = schema.get("type")
    if isinstance(raw, str):
        return {raw}
    if isinstance(raw, list) and all(isinstance(value, str) for value in raw):
        return set(raw)
    return None


def type_atoms(types: set[str]) -> set[str]:
    atoms: set[str] = set()
    for value in types:
        if value == "number":
            atoms.update(("integer", "non_integer_number"))
        else:
            atoms.add(value)
    return atoms


def semantic_json_key(value: Any) -> str:
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return f"{type(value).__name__}:{json.dumps(value, sort_keys=True)}"
    if isinstance(value, (int, float)):
        return f"number:{float(value):.17g}"
    if isinstance(value, list):
        return "list:[" + ",".join(semantic_json_key(item) for item in value) + "]"
    if isinstance(value, dict):
        return "object:{" + ",".join(
            f"{json.dumps(key)}:{semantic_json_key(value[key])}" for key in sorted(value)
        ) + "}"
    return canonical_bytes(value).decode("utf-8")


def finite_values(schema: dict[str, Any]) -> list[Any] | None:
    if "const" in schema:
        return [schema["const"]]
    values = schema.get("enum")
    return values if isinstance(values, list) else None


def classify_bound(
    before: dict[str, Any],
    after: dict[str, Any],
    path: str,
    changes: list[dict[str, Any]],
    key: str,
    *,
    minimum: bool,
) -> None:
    old = before.get(key)
    new = after.get(key)
    old_number = isinstance(old, (int, float)) and not isinstance(old, bool)
    new_number = isinstance(new, (int, float)) and not isinstance(new, bool)
    if not old_number and not new_number:
        return
    if not old_number:
        changes.append(change("breaking", f"{camel_to_snake(key)}_added", path, value=new))
    elif not new_number:
        changes.append(change("additive", f"{camel_to_snake(key)}_removed", path, value=old))
    elif old != new:
        narrows = new > old if minimum else new < old
        changes.append(
            change(
                "breaking" if narrows else "additive",
                f"{camel_to_snake(key)}_{'narrowed' if narrows else 'widened'}",
                path,
                before=old,
                after=new,
            )
        )


def branch_key(schema: Any) -> tuple[str, str] | None:
    if not isinstance(schema, dict):
        return None
    if isinstance(schema.get("$ref"), str):
        return ("ref", schema["$ref"])
    properties = schema.get("properties")
    required = schema.get("required", [])
    if isinstance(properties, dict) and isinstance(required, list):
        for name in sorted(properties):
            child = properties[name]
            if name not in required or not isinstance(child, dict):
                continue
            values = finite_values(child)
            if values is not None and len(values) == 1:
                return (f"tag:{name}", semantic_json_key(values[0]))
    types = schema_types(schema)
    if types is not None and len(type_atoms(types)) == 1:
        return ("type", next(iter(type_atoms(types))))
    return None


def branches_provably_disjoint(left: Any, right: Any) -> bool:
    if not isinstance(left, dict) or not isinstance(right, dict):
        return False
    left_types = schema_types(left)
    right_types = schema_types(right)
    if left_types is not None and right_types is not None:
        if type_atoms(left_types).isdisjoint(type_atoms(right_types)):
            return True
    left_key = branch_key(left)
    right_key = branch_key(right)
    return (
        left_key is not None
        and right_key is not None
        and left_key[0].startswith("tag:")
        and left_key[0] == right_key[0]
        and left_key[1] != right_key[1]
    )


def open_incoming_fallback(path: str, *, tagged: bool = False) -> str | None:
    if tagged and path.endswith("/definitions/ThreadItem"):
        return "unknown_thread_items_preserve_the_complete_raw_payload"
    if any(f"/definitions/{name}" in path for name in ("TurnStatus", "MessagePhase")):
        return "unknown_generated_enum_values_fail_soft_at_the_stable_boundary"
    return None


def compare_combinator(
    before: dict[str, Any],
    after: dict[str, Any],
    path: str,
    changes: list[dict[str, Any]],
    combinator: str,
    *,
    incoming: bool,
) -> None:
    old = before.get(combinator)
    new = after.get(combinator)
    if not isinstance(old, list) and not isinstance(new, list):
        return
    snake = camel_to_snake(combinator)
    if not isinstance(old, list):
        changes.append(change("breaking", f"{snake}_constraint_added", path))
        return
    if not isinstance(new, list):
        changes.append(change("additive", f"{snake}_constraint_removed", path))
        return

    old_unmatched = set(range(len(old)))
    new_unmatched = set(range(len(new)))
    pairs: list[tuple[int, int]] = []
    new_fingerprints: dict[bytes, list[int]] = {}
    for index, variant in enumerate(new):
        new_fingerprints.setdefault(canonical_bytes(variant), []).append(index)
    for old_index, variant in enumerate(old):
        candidates = new_fingerprints.get(canonical_bytes(variant), [])
        candidate = next((index for index in candidates if index in new_unmatched), None)
        if candidate is not None:
            old_unmatched.remove(old_index)
            new_unmatched.remove(candidate)

    for old_index in list(old_unmatched):
        key = branch_key(old[old_index])
        candidates = [index for index in new_unmatched if branch_key(new[index]) == key]
        if key is not None and len(candidates) == 1:
            new_index = candidates[0]
            old_unmatched.remove(old_index)
            new_unmatched.remove(new_index)
            pairs.append((old_index, new_index))
    # Preserve a modified branch at the same position. This is what prevents an
    # optional field edit inside oneOf from being double-counted as remove+add.
    for index in sorted(old_unmatched & new_unmatched):
        old_unmatched.remove(index)
        new_unmatched.remove(index)
        pairs.append((index, index))
    for old_index, new_index in pairs:
        if isinstance(old[old_index], dict) and isinstance(new[new_index], dict):
            compare_named_schemas(
                old[old_index],
                new[new_index],
                f"{path}/{combinator}/{new_index}",
                changes,
                incoming=incoming,
            )
        elif old[old_index] != new[new_index]:
            changes.append(
                change("breaking", f"{snake}_boolean_variant_changed", f"{path}/{combinator}/{new_index}")
            )

    if old_unmatched:
        classification = "additive" if combinator == "allOf" else "breaking"
        changes.append(
            change(classification, f"{snake}_variants_removed", path, count=len(old_unmatched))
        )
    if new_unmatched:
        fallback: str | None = None
        if combinator == "allOf":
            classification = "breaking"
            kind = "all_of_variants_added"
        elif combinator == "anyOf":
            fallback = open_incoming_fallback(path, tagged=True) if incoming else None
            classification = "breaking" if incoming and fallback is None else "additive"
            kind = "incoming_closed_union_variants_added" if classification == "breaking" else "any_of_variants_added"
        else:
            additions_disjoint = all(
                all(branches_provably_disjoint(new[index], existing) for existing in old)
                for index in new_unmatched
            )
            fallback = open_incoming_fallback(path, tagged=True) if incoming else None
            classification = "additive" if additions_disjoint and (not incoming or fallback) else "breaking"
            kind = (
                "one_of_variants_added"
                if classification == "additive"
                else "one_of_variant_added_unproven_or_closed"
            )
        details: dict[str, Any] = {"count": len(new_unmatched)}
        if classification == "additive" and incoming and fallback is not None:
            details["fallbackEvidence"] = fallback
        changes.append(change(classification, kind, path, **details))


def compare_named_schemas(
    before: dict[str, Any],
    after: dict[str, Any],
    path: str,
    changes: list[dict[str, Any]],
    *,
    incoming: bool = False,
) -> None:
    first_change = len(changes)
    before_types = schema_types(before)
    after_types = schema_types(after)
    if before_types is not None and after_types is not None:
        before_atoms = type_atoms(before_types)
        after_atoms = type_atoms(after_types)
        removed_types = sorted(before_atoms - after_atoms)
        added_types = sorted(after_atoms - before_atoms)
        if removed_types:
            changes.append(change("breaking", "type_narrowed_or_changed", path, removedTypes=removed_types, addedTypes=added_types))
        elif added_types:
            changes.append(change("additive", "type_widened", path, addedTypes=added_types))
    elif before_types is None and after_types is not None:
        changes.append(change("breaking", "type_narrowed", path, addedConstraint=sorted(after_types)))
    elif before_types is not None and after_types is None:
        changes.append(change("additive", "type_constraint_removed", path))

    before_values_raw = finite_values(before)
    after_values_raw = finite_values(after)
    if before_values_raw is not None and after_values_raw is not None:
        before_values = {semantic_json_key(value): value for value in before_values_raw}
        after_values = {semantic_json_key(value): value for value in after_values_raw}
        removed = [before_values[key] for key in sorted(before_values.keys() - after_values.keys())]
        added = [after_values[key] for key in sorted(after_values.keys() - before_values.keys())]
        if removed:
            changes.append(change("breaking", "finite_values_removed", path, values=removed))
        if added:
            fallback = open_incoming_fallback(path) if incoming else None
            classification = "breaking" if incoming and fallback is None else "additive"
            details = {"values": added}
            if fallback is not None:
                details["fallbackEvidence"] = fallback
            changes.append(change(classification, "incoming_closed_values_added" if classification == "breaking" else "finite_values_added", path, **details))
    elif before_values_raw is None and after_values_raw is not None:
        changes.append(change("breaking", "finite_constraint_added", path))
    elif before_values_raw is not None and after_values_raw is None:
        changes.append(change("additive", "finite_constraint_removed", path))

    before_ref = before.get("$ref")
    after_ref = after.get("$ref")
    if isinstance(before_ref, str) and isinstance(after_ref, str) and before_ref != after_ref:
        changes.append(change("breaking", "reference_changed", path))

    old_additional = before.get("additionalProperties", True)
    new_additional = after.get("additionalProperties", True)
    if old_additional != new_additional:
        if old_additional is True:
            changes.append(change("breaking", "additional_properties_narrowed", path))
        elif new_additional is True:
            changes.append(change("additive", "additional_properties_widened", path))
        elif old_additional is False:
            changes.append(change("additive", "additional_properties_widened", path))
        elif new_additional is False:
            changes.append(change("breaking", "additional_properties_narrowed", path))
        elif isinstance(old_additional, dict) and isinstance(new_additional, dict):
            compare_named_schemas(old_additional, new_additional, f"{path}/additionalProperties", changes, incoming=incoming)

    for key in ("minimum", "exclusiveMinimum", "minLength", "minItems", "minProperties"):
        classify_bound(before, after, path, changes, key, minimum=True)
    for key in ("maximum", "exclusiveMaximum", "maxLength", "maxItems", "maxProperties"):
        classify_bound(before, after, path, changes, key, minimum=False)
    for key in ("pattern", "format"):
        old = before.get(key)
        new = after.get(key)
        if old != new:
            if old is None:
                changes.append(change("breaking", f"{key}_constraint_added", path))
            elif new is None:
                changes.append(change("additive", f"{key}_constraint_removed", path))
            else:
                changes.append(change("breaking", f"{key}_constraint_changed", path))
    old_multiple = before.get("multipleOf")
    new_multiple = after.get("multipleOf")
    if old_multiple != new_multiple:
        if old_multiple is None:
            changes.append(change("breaking", "multiple_of_added", path))
        elif new_multiple is None:
            changes.append(change("additive", "multiple_of_removed", path))
        elif isinstance(old_multiple, (int, float)) and isinstance(new_multiple, (int, float)):
            ratio = new_multiple / old_multiple if old_multiple else None
            inverse = old_multiple / new_multiple if new_multiple else None
            widened = inverse is not None and float(inverse).is_integer()
            narrowed = ratio is not None and float(ratio).is_integer()
            changes.append(change("additive" if widened and not narrowed else "breaking", "multiple_of_changed", path))
        else:
            changes.append(change("breaking", "multiple_of_changed", path))
    old_unique = before.get("uniqueItems", False) is True
    new_unique = after.get("uniqueItems", False) is True
    if old_unique != new_unique:
        changes.append(change("breaking" if new_unique else "additive", "unique_items_enabled" if new_unique else "unique_items_disabled", path))

    before_props = before.get("properties", {})
    after_props = after.get("properties", {})
    if isinstance(before_props, dict) and isinstance(after_props, dict):
        before_required = set(before.get("required", []))
        after_required = set(after.get("required", []))
        for name in sorted(before_props.keys() - after_props.keys()):
            changes.append(change("breaking", "property_removed", f"{path}/properties/{name}"))
        for name in sorted(after_props.keys() - before_props.keys()):
            classification = "breaking" if name in after_required else "additive"
            kind = "required_property_added" if name in after_required else "optional_property_added"
            changes.append(change(classification, kind, f"{path}/properties/{name}"))
        for name in sorted(before_props.keys() & after_props.keys()):
            before_child = before_props[name]
            after_child = after_props[name]
            if isinstance(before_child, dict) and isinstance(after_child, dict):
                compare_named_schemas(before_child, after_child, f"{path}/properties/{name}", changes, incoming=incoming)
        newly_declared = after_props.keys() - before_props.keys()
        for name in sorted(after_required - before_required):
            if name not in newly_declared:
                changes.append(change("breaking", "property_became_required", f"{path}/properties/{name}"))
        for name in sorted(before_required - after_required):
            changes.append(change("additive", "property_became_optional", f"{path}/properties/{name}"))

    before_defs = before.get("definitions", {})
    after_defs = after.get("definitions", {})
    if isinstance(before_defs, dict) and isinstance(after_defs, dict):
        for name in sorted(before_defs.keys() - after_defs.keys()):
            changes.append(change("breaking", "definition_removed", f"{path}/definitions/{name}"))
        for name in sorted(after_defs.keys() - before_defs.keys()):
            changes.append(change("additive", "definition_added", f"{path}/definitions/{name}"))
        for name in sorted(before_defs.keys() & after_defs.keys()):
            before_child = before_defs[name]
            after_child = after_defs[name]
            if isinstance(before_child, dict) and isinstance(after_child, dict):
                compare_named_schemas(before_child, after_child, f"{path}/definitions/{name}", changes, incoming=incoming)

    before_items = before.get("items")
    after_items = after.get("items")
    if isinstance(before_items, dict) and isinstance(after_items, dict):
        compare_named_schemas(before_items, after_items, f"{path}/items", changes, incoming=incoming)
    elif before_items is None and isinstance(after_items, (dict, bool)):
        changes.append(change("breaking", "items_constraint_added", f"{path}/items"))
    elif isinstance(before_items, (dict, bool)) and after_items is None:
        changes.append(change("additive", "items_constraint_removed", f"{path}/items"))

    for combinator in ("anyOf", "oneOf", "allOf"):
        compare_combinator(before, after, path, changes, combinator, incoming=incoming)

    for key in ("not", "if", "then", "else"):
        if before.get(key) != after.get(key):
            classification = "additive" if key == "not" and key in before and key not in after else "breaking"
            changes.append(change(classification, f"{key}_constraint_changed", path))

    annotations = {"$schema", "$id", "title", "description", "default", "examples", "deprecated", "readOnly", "writeOnly"}
    handled = annotations | {
        "$ref", "type", "enum", "const", "properties", "required", "definitions",
        "additionalProperties", "items", "anyOf", "oneOf", "allOf", "not", "if", "then", "else",
        "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf",
        "minLength", "maxLength", "pattern", "format", "minItems", "maxItems", "uniqueItems",
        "minProperties", "maxProperties",
    }
    for key in sorted((set(before) | set(after)) - handled):
        if before.get(key) != after.get(key):
            changes.append(change("breaking", "unknown_constraint_changed", f"{path}/{key}", keyword=key))

    if incoming:
        for item in changes[first_change:]:
            if item["classification"] != "additive":
                continue
            evidenced_fallback = isinstance(item.get("fallbackEvidence"), str)
            if item["kind"] in {"optional_property_added", "definition_added"} or evidenced_fallback:
                continue
            item["classification"] = "breaking"
            item["incomingDirection"] = "conservative_consumer_boundary"


def compatibility_report(baseline: str, candidate: str) -> dict[str, Any]:
    before = load_bundle(baseline)
    after = load_bundle(candidate)
    changes: list[dict[str, Any]] = []
    before_roots = before["roots"]
    after_roots = after["roots"]
    for name in sorted(before_roots.keys() - after_roots.keys()):
        changes.append(change("breaking", "selected_root_removed", f"roots/{name}"))
    for name in sorted(after_roots.keys() - before_roots.keys()):
        changes.append(change("additive", "selected_root_added", f"roots/{name}"))
    for name in sorted(before_roots.keys() & after_roots.keys()):
        compare_named_schemas(
            before_roots[name],
            after_roots[name],
            f"roots/{name}",
            changes,
            incoming=is_incoming_root(name),
        )

    before_notifications = set(before["notificationMethods"])
    after_notifications = set(after["notificationMethods"])
    for method in sorted(before_notifications - after_notifications):
        changes.append(change("breaking", "notification_removed", f"notifications/{method}"))
    for method in sorted(after_notifications - before_notifications):
        changes.append(change("additive", "notification_added", f"notifications/{method}"))

    before_audit = incoming_audit(baseline, before)
    after_audit = incoming_audit(candidate, after)
    before_constructs = {
        (entry["schemaPath"], entry["construct"]) for entry in before_audit["constructs"]
    }
    for entry in after_audit["constructs"]:
        key = (entry["schemaPath"], entry["construct"])
        if key in before_constructs:
            continue
        classification = "breaking" if entry["handling"] == "promotion-blocking" else "additive"
        changes.append(
            change(
                classification,
                "incoming_construct_added",
                entry["schemaPath"],
                construct=entry["construct"],
                handling=entry["handling"],
                fallbackEvidence=entry["evidence"],
            )
        )

    unique: dict[bytes, dict[str, Any]] = {}
    for item in changes:
        dedupe = dict(item)
        if "/definitions/" in dedupe["path"]:
            dedupe["path"] = "definitions/" + dedupe["path"].split("/definitions/", 1)[1]
        unique.setdefault(canonical_bytes(dedupe), item)
    ordered = sorted(unique.values(), key=lambda item: (item["classification"], item["kind"], item["path"], canonical_bytes(item)))
    breaking = sum(item["classification"] == "breaking" for item in ordered)
    additive = len(ordered) - breaking
    return {
        "formatVersion": 1,
        "baselineVersion": baseline,
        "candidateVersion": candidate,
        "protocolFamily": read_selection().protocol_family,
        "compatible": breaking == 0,
        "summary": {"additive": additive, "breaking": breaking, "total": len(ordered)},
        "changes": ordered,
    }


def report_markdown(report: dict[str, Any]) -> bytes:
    state = "PASS (additive only)" if report["compatible"] else "BLOCKED (breaking changes found)"
    lines = [
        f"# Codex schema upgrade: {report['baselineVersion']} → {report['candidateVersion']}",
        "",
        f"Gate: **{state}**",
        "",
        f"- Additive changes: {report['summary']['additive']}",
        f"- Breaking changes: {report['summary']['breaking']}",
        f"- Total classified changes: {report['summary']['total']}",
        "",
        "| Classification | Kind | Schema path |",
        "| --- | --- | --- |",
    ]
    limit = 250
    for item in report["changes"][:limit]:
        path = str(item["path"]).replace("|", "\\|")
        lines.append(f"| {item['classification']} | `{item['kind']}` | `{path}` |")
    omitted = len(report["changes"]) - limit
    if omitted > 0:
        lines.extend(["", f"The table omits {omitted} additional machine-readable entries; see the JSON report."])
    lines.extend(
        [
            "",
            "Incoming enum/union additions are additive only when the machine-readable audit names tested fallback evidence; closed constructs block promotion.",
            "Promotion still requires contract fixtures, append-only support history, and explicit adapter review.",
            "",
        ]
    )
    return "\n".join(lines).encode("utf-8")


def write_report(baseline: str, candidate: str, json_path: Path | None, markdown_path: Path | None) -> dict[str, Any]:
    report = compatibility_report(baseline, candidate)
    if json_path is not None:
        atomic_write(json_path, canonical_bytes(report))
    if markdown_path is not None:
        atomic_write(markdown_path, report_markdown(report))
    return report


class ValidationFailure(Exception):
    pass


def instance_type_matches(instance: Any, expected: str) -> bool:
    if expected == "null":
        return instance is None
    if expected == "boolean":
        return isinstance(instance, bool)
    if expected == "integer":
        return isinstance(instance, int) and not isinstance(instance, bool)
    if expected == "number":
        return isinstance(instance, (int, float)) and not isinstance(instance, bool)
    if expected == "string":
        return isinstance(instance, str)
    if expected == "array":
        return isinstance(instance, list)
    if expected == "object":
        return isinstance(instance, dict)
    return False


def resolve_pointer(root: dict[str, Any], reference: str) -> Any:
    if not reference.startswith("#/"):
        raise ValidationFailure("external references are not permitted")
    current: Any = root
    for encoded in reference[2:].split("/"):
        key = encoded.replace("~1", "/").replace("~0", "~")
        if not isinstance(current, dict) or key not in current:
            raise ValidationFailure("schema reference is unresolved")
        current = current[key]
    if not isinstance(current, (dict, bool)):
        raise ValidationFailure("schema reference is not a schema")
    return current


def validate_instance(instance: Any, schema: Any, root: dict[str, Any], *, depth: int = 0) -> None:
    if depth > SCHEMA_VALIDATION_RECURSION_LIMIT:
        raise ValidationFailure("schema validation nesting limit exceeded")
    if schema is True:
        return
    if schema is False or not isinstance(schema, dict):
        raise ValidationFailure("boolean schema constraint failed")
    reference = schema.get("$ref")
    if isinstance(reference, str):
        validate_instance(instance, resolve_pointer(root, reference), root, depth=depth + 1)
        return
    if "const" in schema and semantic_json_key(instance) != semantic_json_key(schema["const"]):
        raise ValidationFailure("constant constraint failed")
    if isinstance(schema.get("enum"), list) and semantic_json_key(instance) not in {
        semantic_json_key(value) for value in schema["enum"]
    }:
        raise ValidationFailure("enum constraint failed")
    raw_type = schema.get("type")
    allowed_types = [raw_type] if isinstance(raw_type, str) else raw_type
    if isinstance(allowed_types, list) and allowed_types:
        if not any(isinstance(value, str) and instance_type_matches(instance, value) for value in allowed_types):
            raise ValidationFailure("type constraint failed")

    for combinator in ("allOf",):
        variants = schema.get(combinator)
        if isinstance(variants, list):
            for variant in variants:
                if isinstance(variant, (dict, bool)):
                    validate_instance(instance, variant, root, depth=depth + 1)
    for combinator, exact in (("anyOf", False), ("oneOf", True)):
        variants = schema.get(combinator)
        if isinstance(variants, list):
            matches = 0
            for variant in variants:
                if not isinstance(variant, (dict, bool)):
                    continue
                try:
                    validate_instance(instance, variant, root, depth=depth + 1)
                    matches += 1
                except ValidationFailure:
                    pass
            if matches == 0 or (exact and matches != 1):
                raise ValidationFailure(f"{combinator} constraint failed")
    negative = schema.get("not")
    if isinstance(negative, (dict, bool)):
        try:
            validate_instance(instance, negative, root, depth=depth + 1)
        except ValidationFailure:
            pass
        else:
            raise ValidationFailure("not constraint failed")
    condition = schema.get("if")
    if isinstance(condition, (dict, bool)):
        try:
            validate_instance(instance, condition, root, depth=depth + 1)
            branch = schema.get("then")
        except ValidationFailure:
            branch = schema.get("else")
        if isinstance(branch, (dict, bool)):
            validate_instance(instance, branch, root, depth=depth + 1)

    if isinstance(instance, dict):
        if isinstance(schema.get("minProperties"), int) and len(instance) < schema["minProperties"]:
            raise ValidationFailure("minimum object property count failed")
        if isinstance(schema.get("maxProperties"), int) and len(instance) > schema["maxProperties"]:
            raise ValidationFailure("maximum object property count failed")
        required = schema.get("required", [])
        if isinstance(required, list):
            for key in required:
                if isinstance(key, str) and key not in instance:
                    raise ValidationFailure("required property is absent")
        properties = schema.get("properties", {})
        if isinstance(properties, dict):
            for key, value in instance.items():
                child = properties.get(key)
                if isinstance(child, (dict, bool)):
                    validate_instance(value, child, root, depth=depth + 1)
                    continue
                matched_pattern = False
                patterns = schema.get("patternProperties", {})
                if isinstance(patterns, dict):
                    for pattern, pattern_schema in patterns.items():
                        if re.search(pattern, key) and isinstance(pattern_schema, (dict, bool)):
                            matched_pattern = True
                            validate_instance(value, pattern_schema, root, depth=depth + 1)
                if matched_pattern:
                    continue
                additional = schema.get("additionalProperties", True)
                if additional is False:
                    raise ValidationFailure("additional property is forbidden")
                if isinstance(additional, dict):
                    validate_instance(value, additional, root, depth=depth + 1)
        dependencies = schema.get("dependencies", {})
        if isinstance(dependencies, dict):
            for key, dependency in dependencies.items():
                if key not in instance:
                    continue
                if isinstance(dependency, list) and any(item not in instance for item in dependency):
                    raise ValidationFailure("property dependency failed")
                if isinstance(dependency, (dict, bool)):
                    validate_instance(instance, dependency, root, depth=depth + 1)
    if isinstance(instance, list):
        if isinstance(schema.get("minItems"), int) and len(instance) < schema["minItems"]:
            raise ValidationFailure("minimum array length failed")
        if isinstance(schema.get("maxItems"), int) and len(instance) > schema["maxItems"]:
            raise ValidationFailure("maximum array length failed")
        if schema.get("uniqueItems") is True:
            keys = [semantic_json_key(item) for item in instance]
            if len(keys) != len(set(keys)):
                raise ValidationFailure("unique array item constraint failed")
        items = schema.get("items")
        if isinstance(items, (dict, bool)):
            for item in instance:
                validate_instance(item, items, root, depth=depth + 1)
        elif isinstance(items, list):
            for index, item in enumerate(instance[: len(items)]):
                validate_instance(item, items[index], root, depth=depth + 1)
            if len(instance) > len(items):
                additional_items = schema.get("additionalItems", True)
                if additional_items is False:
                    raise ValidationFailure("additional array item is forbidden")
                if isinstance(additional_items, dict):
                    for item in instance[len(items) :]:
                        validate_instance(item, additional_items, root, depth=depth + 1)
        contains = schema.get("contains")
        if isinstance(contains, (dict, bool)):
            if not any(instance_valid(item, contains, root, depth + 1) for item in instance):
                raise ValidationFailure("array contains constraint failed")
    if isinstance(instance, str):
        if isinstance(schema.get("minLength"), int) and len(instance) < schema["minLength"]:
            raise ValidationFailure("minimum string length failed")
        if isinstance(schema.get("maxLength"), int) and len(instance) > schema["maxLength"]:
            raise ValidationFailure("maximum string length failed")
        pattern = schema.get("pattern")
        if isinstance(pattern, str):
            try:
                matched = re.search(pattern, instance)
            except re.error as error:
                raise ValidationFailure("schema pattern is invalid") from error
            if matched is None:
                raise ValidationFailure("string pattern constraint failed")
    if isinstance(instance, (int, float)) and not isinstance(instance, bool):
        if isinstance(schema.get("minimum"), (int, float)) and instance < schema["minimum"]:
            raise ValidationFailure("minimum numeric constraint failed")
        if isinstance(schema.get("maximum"), (int, float)) and instance > schema["maximum"]:
            raise ValidationFailure("maximum numeric constraint failed")
        if isinstance(schema.get("exclusiveMinimum"), (int, float)) and instance <= schema["exclusiveMinimum"]:
            raise ValidationFailure("exclusive minimum numeric constraint failed")
        if isinstance(schema.get("exclusiveMaximum"), (int, float)) and instance >= schema["exclusiveMaximum"]:
            raise ValidationFailure("exclusive maximum numeric constraint failed")
        multiple = schema.get("multipleOf")
        if isinstance(multiple, (int, float)) and multiple > 0:
            quotient = instance / multiple
            if abs(quotient - round(quotient)) > 1e-12:
                raise ValidationFailure("multiple-of numeric constraint failed")
        numeric_ranges = {
            "int32": (-(2**31), 2**31 - 1),
            "uint16": (0, 2**16 - 1),
            "uint32": (0, 2**32 - 1),
            "int64": (-(2**63), 2**63 - 1),
            "uint64": (0, 2**64 - 1),
            "uint": (0, 2**64 - 1),
        }
        bounds = numeric_ranges.get(schema.get("format"))
        if bounds is not None and not bounds[0] <= instance <= bounds[1]:
            raise ValidationFailure("formatted integer range failed")


def instance_valid(instance: Any, schema: Any, root: dict[str, Any], depth: int) -> bool:
    try:
        validate_instance(instance, schema, root, depth=depth)
        return True
    except ValidationFailure:
        return False


METHOD_ROOTS = {
    "initialize": ("initialize.params", "initialize.response"),
    "thread/start": ("thread.start.params", "thread.start.response"),
    "thread/list": ("thread.list.params", "thread.list.response"),
    "thread/read": ("thread.read.params", "thread.read.response"),
    "thread/resume": ("thread.resume.params", "thread.resume.response"),
    "turn/start": ("turn.start.params", "turn.start.response"),
    "turn/interrupt": ("turn.interrupt.params", "turn.interrupt.response"),
}

NOTIFICATION_ROOTS = {
    "thread/started": "notification.thread.started",
    "turn/started": "notification.turn.started",
    "item/started": "notification.item.started",
    "item/agentMessage/delta": "notification.item.agent_message.delta",
    "item/commandExecution/outputDelta": "notification.item.command_execution.output_delta",
    "item/completed": "notification.item.completed",
    "thread/tokenUsage/updated": "notification.thread.token_usage.updated",
    "error": "notification.error",
    "turn/completed": "notification.turn.completed",
}

REVERSE_REQUEST_ROOTS = {
    "item/tool/call": ("server_request.dynamic_tool_call.params", "server_request.dynamic_tool_call.response")
}

def validate_contract(version: str) -> None:
    bundle = load_bundle(version)
    roots = bundle["roots"]
    path = CONTRACTS_ROOT / f"{version}.json"
    contract = load_json(path)
    if not isinstance(contract, dict) or contract.get("formatVersion") != CONTRACT_FORMAT_VERSION:
        fail(f"Codex {version} contract has an unsupported format")
    if contract.get("codexVersion") != version:
        fail(f"Codex {version} contract records the wrong version")
    if contract.get("protocolFamily") != read_selection().protocol_family:
        fail(f"Codex {version} contract records the wrong protocol family")

    exchanges = contract.get("exchanges")
    if not isinstance(exchanges, list):
        fail(f"Codex {version} contract has no method exchanges")
    seen_methods: set[str] = set()
    for exchange in exchanges:
        if not isinstance(exchange, dict) or not isinstance(exchange.get("method"), str):
            fail(f"Codex {version} contract contains an invalid exchange")
        method = exchange["method"]
        if method not in METHOD_ROOTS or method in seen_methods:
            fail(f"Codex {version} contract contains an unexpected or duplicate method")
        seen_methods.add(method)
        params_root, result_root = METHOD_ROOTS[method]
        try:
            validate_instance(exchange.get("params"), roots[params_root], roots[params_root])
            validate_instance(exchange.get("result"), roots[result_root], roots[result_root])
        except (KeyError, ValidationFailure) as error:
            raise SchemaToolError(f"Codex {version} contract violates the selected schema for {method}") from error
    if seen_methods != set(METHOD_ROOTS):
        fail(f"Codex {version} contract does not cover every selected method")

    notifications = contract.get("notifications")
    if not isinstance(notifications, list):
        fail(f"Codex {version} contract has no notifications")
    seen_notifications: set[str] = set()
    for notification in notifications:
        if not isinstance(notification, dict) or not isinstance(notification.get("method"), str):
            fail(f"Codex {version} contract contains an invalid notification")
        method = notification["method"]
        if method not in NOTIFICATION_ROOTS or method in seen_notifications:
            fail(f"Codex {version} contract contains an unexpected or duplicate notification")
        seen_notifications.add(method)
        params = notification.get("params")
        root_name = NOTIFICATION_ROOTS[method]
        try:
            validate_instance(params, roots[root_name], roots[root_name])
        except (KeyError, ValidationFailure) as error:
            raise SchemaToolError(f"Codex {version} contract violates the selected schema for {method}") from error
    if seen_notifications != set(NOTIFICATION_ROOTS):
        fail(f"Codex {version} contract does not cover every consumed notification")
    normal_order = contract.get("normalNotificationOrder")
    if not isinstance(normal_order, list) or not all(isinstance(method, str) for method in normal_order):
        fail(f"Codex {version} contract has an invalid notification-order fixture")

    reverse_requests = contract.get("reverseRequests")
    if not isinstance(reverse_requests, list) or len(reverse_requests) != len(REVERSE_REQUEST_ROOTS):
        fail(f"Codex {version} contract has invalid reverse-request coverage")
    seen_reverse: set[str] = set()
    for request in reverse_requests:
        if not isinstance(request, dict) or request.get("method") not in REVERSE_REQUEST_ROOTS:
            fail(f"Codex {version} contract has an unexpected reverse request")
        method = request["method"]
        if method in seen_reverse:
            fail(f"Codex {version} contract has a duplicate reverse request")
        seen_reverse.add(method)
        params_root, result_root = REVERSE_REQUEST_ROOTS[method]
        try:
            validate_instance(request.get("params"), roots[params_root], roots[params_root])
            validate_instance(request.get("result"), roots[result_root], roots[result_root])
        except (KeyError, ValidationFailure) as error:
            raise SchemaToolError(f"Codex {version} contract violates the selected schema for {method}") from error

    failures = contract.get("failureCases")
    if not isinstance(failures, list) or not failures:
        fail(f"Codex {version} contract has no failure classification cases")
    observed_failure_sources: set[str] = set()
    for case in failures:
        if not isinstance(case, dict):
            fail(f"Codex {version} contract contains an invalid failure case")
        source = case.get("source")
        expected = case.get("expected")
        if not isinstance(source, str) or not isinstance(expected, str) or source in observed_failure_sources:
            fail(f"Codex {version} contract contains an invalid failure classification")
        observed_failure_sources.add(source)


def verify_manifest(version: str, selection: Selection, policy: dict[str, Any]) -> None:
    manifest_path = SCHEMAS_ROOT / version / "manifest.json"
    schema_path = SCHEMAS_ROOT / version / "selected.schema.json"
    wire_path = WIRE_ROOT / f"{rust_version_module(version)}.rs"
    audit_path = SCHEMAS_ROOT / version / "incoming-audit.json"
    try:
        manifest_bytes = manifest_path.read_bytes()
        schema_bytes = schema_path.read_bytes()
        wire_bytes = wire_path.read_bytes()
        audit_bytes = audit_path.read_bytes()
    except OSError as error:
        raise SchemaToolError(f"Codex {version} is missing a generated artifact") from error
    bundle = load_bundle(version)
    expected_schema = canonical_bytes(bundle)
    if schema_bytes != expected_schema:
        fail(f"Codex {version} normalized schema is not canonical")
    if list(bundle["roots"].keys()) != sorted(root.name for root in selection.roots):
        fail(f"Codex {version} selected roots are stale")
    expected_wire = render_wire(
        version, selection.protocol_family, sha256_bytes(expected_schema), bundle
    )
    if wire_bytes != expected_wire:
        fail(f"Codex {version} generated Rust does not match the normalized schema")
    expected_audit = canonical_bytes(incoming_audit(version, bundle))
    if audit_bytes != expected_audit:
        fail(f"Codex {version} incoming enum/union audit is stale")
    expected_manifest = canonical_bytes(
        manifest_for(version, selection, expected_schema, expected_wire, expected_audit, policy)
    )
    if manifest_bytes != expected_manifest:
        fail(f"Codex {version} manifest is stale")


def verify_all() -> None:
    selection = read_selection()
    policy = read_policy()
    history = read_history()
    if policy["protocolFamily"] != selection.protocol_family:
        fail("support policy and schema selection protocol families differ")
    if history.get("protocolFamily") != selection.protocol_family:
        fail("support history and schema selection protocol families differ")
    historical_versions = [release["version"] for release in history["releases"]]
    if policy["supportedVersions"] != sorted(historical_versions, key=version_key):
        fail("support policy must retain every version in append-only support history")
    versions = sorted(
        set(policy["supportedVersions"] + policy["candidateVersions"] + historical_versions),
        key=version_key,
    )
    for version in versions:
        verify_manifest(version, selection, policy)
        validate_contract(version)

    expected_mod = render_wire_mod(policy, versions)
    try:
        actual_mod = (WIRE_ROOT / "mod.rs").read_bytes()
    except OSError as error:
        raise SchemaToolError("generated wire module registry is unavailable") from error
    if actual_mod != expected_mod:
        fail("generated wire module registry is stale")

    for release in history["releases"]:
        version = release["version"]
        try:
            schema = (SCHEMAS_ROOT / version / "selected.schema.json").read_bytes()
            contract_bytes = (CONTRACTS_ROOT / f"{version}.json").read_bytes()
            wire = (WIRE_ROOT / f"{rust_version_module(version)}.rs").read_bytes()
        except OSError as error:
            raise SchemaToolError(f"Codex {version} support history artifact is unavailable") from error
        if release["schemaSha256"] != sha256_bytes(schema):
            fail(f"Codex {version} support history schema hash is stale")
        if release["contractSha256"] != sha256_bytes(contract_bytes):
            fail(f"Codex {version} support history contract hash is stale")
        if release["rustWireSha256"] != sha256_bytes(wire):
            fail(f"Codex {version} support history Rust hash is stale")

    baseline = ESTABLISHED_BASELINE_VERSION
    for candidate in policy["candidateVersions"]:
        for supported in historical_versions:
            expected_report = compatibility_report(supported, candidate)
            json_path = REPORTS_ROOT / f"{supported}-to-{candidate}.json"
            markdown_path = REPORTS_ROOT / f"{supported}-to-{candidate}.md"
            try:
                actual_json = json_path.read_bytes()
                actual_markdown = markdown_path.read_bytes()
            except OSError as error:
                raise SchemaToolError(f"Codex {candidate} compatibility report is unavailable") from error
            if actual_json != canonical_bytes(expected_report) or actual_markdown != report_markdown(expected_report):
                fail(f"Codex {candidate} compatibility report is stale")

    for supported in policy["supportedVersions"]:
        if supported == baseline:
            continue
        report = compatibility_report(baseline, supported)
        if not report["compatible"]:
            fail(f"Codex {supported} cannot be supported because its schema diff is breaking")


def default_report_paths(baseline: str, candidate: str) -> tuple[Path, Path]:
    stem = f"{baseline}-to-{candidate}"
    return REPORTS_ROOT / f"{stem}.json", REPORTS_ROOT / f"{stem}.md"


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    sync_parser = subparsers.add_parser("sync", help="export and normalize an exact Codex binary schema")
    sync_parser.add_argument("--binary", required=True, type=Path)
    sync_parser.add_argument("--check", action="store_true", help="compare with committed output without writing")

    diff_parser = subparsers.add_parser("diff", help="classify a committed candidate schema")
    diff_parser.add_argument("--baseline", required=True)
    diff_parser.add_argument("--candidate", required=True)
    diff_parser.add_argument("--json", type=Path)
    diff_parser.add_argument("--markdown", type=Path)
    diff_parser.add_argument("--write-defaults", action="store_true")
    diff_parser.add_argument("--allow-breaking", action="store_true")

    contract_parser = subparsers.add_parser("contract", help="validate committed fixtures against selected schemas")
    contract_parser.add_argument("--version", action="append", dest="versions")

    history_parser = subparsers.add_parser(
        "verify-history", help="verify the support ledger only appends to a trusted prior copy"
    )
    history_parser.add_argument("--previous", required=True, type=Path)

    subparsers.add_parser("verify", help="offline verification of manifests, contracts, reports, and promotion gates")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.command == "sync":
        version = sync(args.binary, check=args.check)
        print(json.dumps({"codexVersion": version, "status": "verified" if args.check else "synced"}, sort_keys=True))
        return 0
    if args.command == "diff":
        json_path = args.json
        markdown_path = args.markdown
        if args.write_defaults:
            json_path, markdown_path = default_report_paths(args.baseline, args.candidate)
        report = write_report(args.baseline, args.candidate, json_path, markdown_path)
        print(json.dumps({"compatible": report["compatible"], **report["summary"]}, sort_keys=True))
        return 0 if report["compatible"] or args.allow_breaking else 2
    if args.command == "contract":
        policy = read_policy()
        versions = args.versions or policy["supportedVersions"] + policy["candidateVersions"]
        for version in versions:
            validate_contract(version)
        print(json.dumps({"contracts": sorted(set(versions), key=version_key), "status": "valid"}, sort_keys=True))
        return 0
    if args.command == "verify-history":
        verify_history_append_only(args.previous)
        print(json.dumps({"history": "append-only", "status": "valid"}, sort_keys=True))
        return 0
    if args.command == "verify":
        verify_all()
        policy = read_policy()
        print(
            json.dumps(
                {
                    "candidates": policy["candidateVersions"],
                    "selectedWireVersion": policy["selectedWireVersion"],
                    "status": "valid",
                    "supportedVersions": policy["supportedVersions"],
                },
                sort_keys=True,
            )
        )
        return 0
    fail("unknown command")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SchemaToolError as error:
        print(f"codex-schema: {error}", file=sys.stderr)
        raise SystemExit(1) from None
