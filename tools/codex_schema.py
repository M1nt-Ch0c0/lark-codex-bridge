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
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, NoReturn


GENERATOR_NAME = "lark-codex-bridge/codex-schema"
GENERATOR_VERSION = "1.0.0"
MANIFEST_FORMAT_VERSION = 1
SCHEMA_BUNDLE_FORMAT_VERSION = 1
CONTRACT_FORMAT_VERSION = 1
MAX_CAPTURE_BYTES = 64 * 1024
MAX_JSONL_LINE_BYTES = 32 * 1024 * 1024
MAX_JSON_NESTING = 128
MAX_JSON_STRUCTURAL_TOKENS = 64 * 1024
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


def fail(message: str) -> NoReturn:
    raise SchemaToolError(message)


def load_json(path: Path, *, maximum: int = MAX_JSONL_LINE_BYTES) -> Any:
    try:
        size = path.stat().st_size
    except OSError as error:
        raise SchemaToolError(f"required artifact is unavailable: {safe_relative(path)}") from error
    if size > maximum:
        fail(f"JSON artifact exceeds the {maximum}-byte limit: {safe_relative(path)}")
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise SchemaToolError(f"invalid JSON artifact: {safe_relative(path)}") from error


def safe_relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT).as_posix()
    except (OSError, ValueError):
        return path.name


def canonical_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


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
        "compatibilityBaselineVersion",
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
    if not is_version(raw["compatibilityBaselineVersion"]):
        fail("compatibility baseline is not an exact Codex version")
    if set(raw["supportedVersions"]) & set(raw["candidateVersions"]):
        fail("supported and candidate Codex versions overlap")
    return raw


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
        result = subprocess.run(
            [os.fspath(binary), "--version"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=VERSION_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise SchemaToolError("Codex version probe could not complete") from error
    if len(result.stdout) > MAX_CAPTURE_BYTES or len(result.stderr) > MAX_CAPTURE_BYTES:
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
        result = subprocess.run(
            [os.fspath(binary), *arguments],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=GENERATION_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        temporary.cleanup()
        raise SchemaToolError(f"Codex {version} schema export could not complete") from error
    if len(result.stdout) > MAX_CAPTURE_BYTES or len(result.stderr) > MAX_CAPTURE_BYTES:
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
            "normalizedSchema": f"protocol/codex/schemas/{version}/selected.schema.json",
            "rustWire": f"src/codex/wire/{module}.rs",
        },
        "artifactSha256": {
            "normalizedSchema": sha256_bytes(schema_bytes),
            "rustWire": sha256_bytes(wire_bytes),
        },
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
        manifest = manifest_for(version, selection, schema_bytes, wire_bytes, policy)
        manifest_bytes = canonical_bytes(manifest)
        module = rust_version_module(version)
        targets = {
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


def compare_named_schemas(before: dict[str, Any], after: dict[str, Any], path: str, changes: list[dict[str, Any]]) -> None:
    before_types = schema_types(before)
    after_types = schema_types(after)
    if before_types is not None and after_types is not None:
        removed_types = sorted(before_types - after_types)
        added_types = sorted(after_types - before_types)
        if removed_types:
            changes.append(change("breaking", "type_narrowed", path, removedTypes=removed_types, addedTypes=added_types))
        elif added_types:
            changes.append(change("additive", "type_widened", path, addedTypes=added_types))
    elif before_types is None and after_types is not None:
        changes.append(change("breaking", "type_narrowed", path, addedConstraint=sorted(after_types)))

    before_enum = before.get("enum")
    after_enum = after.get("enum")
    if isinstance(before_enum, list) and isinstance(after_enum, list):
        before_values = {json.dumps(value, sort_keys=True): value for value in before_enum}
        after_values = {json.dumps(value, sort_keys=True): value for value in after_enum}
        removed = [before_values[key] for key in sorted(before_values.keys() - after_values.keys())]
        added = [after_values[key] for key in sorted(after_values.keys() - before_values.keys())]
        if removed:
            changes.append(change("breaking", "enum_values_removed", path, values=removed))
        if added:
            changes.append(change("additive", "enum_values_added", path, values=added, requiresUnknownFallback=True))

    before_ref = before.get("$ref")
    after_ref = after.get("$ref")
    if isinstance(before_ref, str) and isinstance(after_ref, str) and before_ref != after_ref:
        changes.append(change("breaking", "reference_changed", path))

    if before.get("additionalProperties", True) is not False and after.get("additionalProperties", True) is False:
        changes.append(change("breaking", "additional_properties_closed", path))

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
                compare_named_schemas(before_child, after_child, f"{path}/properties/{name}", changes)
        for name in sorted((after_required - before_required) & before_props.keys()):
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
                compare_named_schemas(before_child, after_child, f"{path}/definitions/{name}", changes)

    before_items = before.get("items")
    after_items = after.get("items")
    if isinstance(before_items, dict) and isinstance(after_items, dict):
        compare_named_schemas(before_items, after_items, f"{path}/items", changes)

    for combinator in ("anyOf", "oneOf", "allOf"):
        before_variants = before.get(combinator)
        after_variants = after.get(combinator)
        if isinstance(before_variants, list) and isinstance(after_variants, list):
            before_fingerprints = {sha256_bytes(canonical_bytes(value)) for value in before_variants}
            after_fingerprints = {sha256_bytes(canonical_bytes(value)) for value in after_variants}
            if before_fingerprints - after_fingerprints:
                changes.append(change("breaking", f"{camel_to_snake(combinator)}_variant_removed_or_changed", path))
            if after_fingerprints - before_fingerprints:
                changes.append(change("additive", f"{camel_to_snake(combinator)}_variant_added_or_changed", path))


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
        compare_named_schemas(before_roots[name], after_roots[name], f"roots/{name}", changes)

    before_notifications = set(before["notificationMethods"])
    after_notifications = set(after["notificationMethods"])
    for method in sorted(before_notifications - after_notifications):
        changes.append(change("breaking", "notification_removed", f"notifications/{method}"))
    for method in sorted(after_notifications - before_notifications):
        changes.append(change("additive", "notification_added", f"notifications/{method}"))

    unique = {canonical_bytes(item): item for item in changes}
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
            "Enum additions are additive only because every selected wire enum is decoded through an unknown-value fallback.",
            "Promotion still requires contract fixtures and explicit support-policy review.",
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
    return True


def resolve_pointer(root: dict[str, Any], reference: str) -> dict[str, Any]:
    if not reference.startswith("#/"):
        raise ValidationFailure("external references are not permitted")
    current: Any = root
    for encoded in reference[2:].split("/"):
        key = encoded.replace("~1", "/").replace("~0", "~")
        if not isinstance(current, dict) or key not in current:
            raise ValidationFailure("schema reference is unresolved")
        current = current[key]
    if not isinstance(current, dict):
        raise ValidationFailure("schema reference is not an object")
    return current


def validate_instance(instance: Any, schema: Any, root: dict[str, Any], *, depth: int = 0) -> None:
    if depth > MAX_JSON_NESTING:
        raise ValidationFailure("schema validation nesting limit exceeded")
    if schema is True:
        return
    if schema is False or not isinstance(schema, dict):
        raise ValidationFailure("boolean schema constraint failed")
    reference = schema.get("$ref")
    if isinstance(reference, str):
        validate_instance(instance, resolve_pointer(root, reference), root, depth=depth + 1)
        return
    if "const" in schema and instance != schema["const"]:
        raise ValidationFailure("constant constraint failed")
    if isinstance(schema.get("enum"), list) and instance not in schema["enum"]:
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
                if isinstance(variant, dict):
                    validate_instance(instance, variant, root, depth=depth + 1)
    for combinator, exact in (("anyOf", False), ("oneOf", True)):
        variants = schema.get(combinator)
        if isinstance(variants, list):
            matches = 0
            for variant in variants:
                if not isinstance(variant, dict):
                    continue
                try:
                    validate_instance(instance, variant, root, depth=depth + 1)
                    matches += 1
                except ValidationFailure:
                    pass
            if matches == 0 or (exact and matches != 1):
                raise ValidationFailure(f"{combinator} constraint failed")
    negative = schema.get("not")
    if isinstance(negative, dict):
        try:
            validate_instance(instance, negative, root, depth=depth + 1)
        except ValidationFailure:
            pass
        else:
            raise ValidationFailure("not constraint failed")

    if isinstance(instance, dict):
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
                elif schema.get("additionalProperties") is False:
                    raise ValidationFailure("additional property is forbidden")
    if isinstance(instance, list) and isinstance(schema.get("items"), dict):
        for item in instance:
            validate_instance(item, schema["items"], root, depth=depth + 1)
    if isinstance(instance, str):
        if isinstance(schema.get("minLength"), int) and len(instance) < schema["minLength"]:
            raise ValidationFailure("minimum string length failed")
        if isinstance(schema.get("maxLength"), int) and len(instance) > schema["maxLength"]:
            raise ValidationFailure("maximum string length failed")
    if isinstance(instance, (int, float)) and not isinstance(instance, bool):
        if isinstance(schema.get("minimum"), (int, float)) and instance < schema["minimum"]:
            raise ValidationFailure("minimum numeric constraint failed")
        if isinstance(schema.get("maximum"), (int, float)) and instance > schema["maximum"]:
            raise ValidationFailure("maximum numeric constraint failed")


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

NORMAL_NOTIFICATION_ORDER = [
    "thread/started",
    "turn/started",
    "item/started",
    "item/agentMessage/delta",
    "item/commandExecution/outputDelta",
    "item/completed",
    "thread/tokenUsage/updated",
    "turn/completed",
]

FAILURE_CLASSES = {
    "serialize": "definitely_not_applied",
    "payload_too_large": "definitely_not_applied",
    "request_id_exhausted": "definitely_not_applied",
    "server_error": "definitely_not_applied",
    "timeout": "uncertain",
    "connection_lost": "uncertain",
    "confirmed_untracked": "uncertain",
}


def structural_weight(value: Any, depth: int = 0) -> tuple[int, int]:
    maximum_depth = depth
    tokens = 0
    if isinstance(value, dict):
        tokens += 1 + len(value)
        for child in value.values():
            child_depth, child_tokens = structural_weight(child, depth + 1)
            maximum_depth = max(maximum_depth, child_depth)
            tokens += child_tokens
    elif isinstance(value, list):
        tokens += 1 + max(0, len(value) - 1)
        for child in value:
            child_depth, child_tokens = structural_weight(child, depth + 1)
            maximum_depth = max(maximum_depth, child_depth)
            tokens += child_tokens
    return maximum_depth, tokens


def validate_wire_record(record: Any, label: str) -> None:
    encoded = json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(encoded) > MAX_JSONL_LINE_BYTES:
        fail(f"contract record exceeds the wire byte limit: {label}")
    depth, tokens = structural_weight(record)
    if depth > MAX_JSON_NESTING:
        fail(f"contract record exceeds the nesting limit: {label}")
    if tokens > MAX_JSON_STRUCTURAL_TOKENS:
        fail(f"contract record exceeds the structural-token limit: {label}")


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
        validate_wire_record(exchange.get("params"), f"{method} params")
        validate_wire_record(exchange.get("result"), f"{method} result")
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
        validate_wire_record(params, f"{method} notification")
        root_name = NOTIFICATION_ROOTS[method]
        try:
            validate_instance(params, roots[root_name], roots[root_name])
        except (KeyError, ValidationFailure) as error:
            raise SchemaToolError(f"Codex {version} contract violates the selected schema for {method}") from error
    if seen_notifications != set(NOTIFICATION_ROOTS):
        fail(f"Codex {version} contract does not cover every consumed notification")
    if contract.get("normalNotificationOrder") != NORMAL_NOTIFICATION_ORDER:
        fail(f"Codex {version} contract has an invalid notification order")

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
        if source not in FAILURE_CLASSES or FAILURE_CLASSES[source] != expected:
            fail(f"Codex {version} contract contains an invalid failure classification")
        observed_failure_sources.add(source)
    if observed_failure_sources != set(FAILURE_CLASSES):
        fail(f"Codex {version} contract does not cover every failure classification")


def verify_manifest(version: str, selection: Selection, policy: dict[str, Any]) -> None:
    manifest_path = SCHEMAS_ROOT / version / "manifest.json"
    manifest = load_json(manifest_path)
    schema_path = SCHEMAS_ROOT / version / "selected.schema.json"
    wire_path = WIRE_ROOT / f"{rust_version_module(version)}.rs"
    try:
        schema_bytes = schema_path.read_bytes()
        wire_bytes = wire_path.read_bytes()
    except OSError as error:
        raise SchemaToolError(f"Codex {version} is missing a generated artifact") from error
    expected_lifecycle = (
        "supported" if version in policy["supportedVersions"] else "candidate" if version in policy["candidateVersions"] else "unclassified"
    )
    required_values = {
        "formatVersion": MANIFEST_FORMAT_VERSION,
        "codexVersion": version,
        "protocolFamily": selection.protocol_family,
        "schemaSha256": sha256_bytes(schema_bytes),
        "generationArguments": list(selection.generator_arguments),
        "lifecycle": expected_lifecycle,
        "selectedRoots": [root.name for root in selection.roots],
    }
    if not isinstance(manifest, dict) or any(manifest.get(key) != value for key, value in required_values.items()):
        fail(f"Codex {version} manifest metadata is stale")
    expected_generator = {
        "name": GENERATOR_NAME,
        "version": GENERATOR_VERSION,
        "templateSha256": template_sha256(),
    }
    if manifest.get("generator") != expected_generator:
        fail(f"Codex {version} manifest generator metadata is stale")
    expected_artifacts = {
        "normalizedSchema": f"protocol/codex/schemas/{version}/selected.schema.json",
        "rustWire": f"src/codex/wire/{rust_version_module(version)}.rs",
    }
    if manifest.get("artifacts") != expected_artifacts:
        fail(f"Codex {version} manifest artifact paths are stale")
    expected_hashes = {"normalizedSchema": sha256_bytes(schema_bytes), "rustWire": sha256_bytes(wire_bytes)}
    if manifest.get("artifactSha256") != expected_hashes:
        fail(f"Codex {version} manifest artifact hashes are stale")
    bundle = load_bundle(version)
    if list(bundle["roots"].keys()) != sorted(root.name for root in selection.roots):
        fail(f"Codex {version} selected roots are stale")


def verify_all() -> None:
    selection = read_selection()
    policy = read_policy()
    if policy["protocolFamily"] != selection.protocol_family:
        fail("support policy and schema selection protocol families differ")
    versions = sorted(
        set(policy["supportedVersions"] + policy["candidateVersions"] + [policy["compatibilityBaselineVersion"]]),
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

    baseline = policy["compatibilityBaselineVersion"]
    for candidate in policy["candidateVersions"]:
        expected_report = compatibility_report(baseline, candidate)
        json_path = REPORTS_ROOT / f"{baseline}-to-{candidate}.json"
        markdown_path = REPORTS_ROOT / f"{baseline}-to-{candidate}.md"
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
