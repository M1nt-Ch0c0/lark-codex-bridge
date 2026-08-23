# Codex app-server schema maintenance

The bridge supports only exact Codex versions whose generated schema, wire DTOs,
compatibility mapping, and contract fixture have been reviewed. The authoritative
policy is [`protocol/codex/support-policy.json`](../protocol/codex/support-policy.json).
At present 0.146.0 is supported; 0.149.0 is a recorded candidate and is deliberately
blocked because its compatibility report contains breaking changes.

Normal Cargo builds are offline with respect to Codex. They compile committed Rust
wire DTOs and never install, locate, or execute a `codex` binary. Only the explicit
maintenance command below executes Codex.

## Sync an exact binary

Pass the binary itself, not a package name or version range:

```bash
python3 tools/codex_schema.py sync --binary /absolute/path/to/codex
python3 tools/codex_schema.py sync --binary /absolute/path/to/codex --check
```

`sync` requires stdout from `codex --version` to match `codex-cli X.Y.Z` exactly,
runs `app-server generate-json-schema --experimental`, selects only bridge-owned
roots and their transitive definitions, canonicalizes JSON, and writes:

- `protocol/codex/schemas/X.Y.Z/selected.schema.json`;
- a manifest containing exact Codex version, protocol family, schema SHA-256,
  generator version/template hash, selected roots, and artifact hashes;
- `src/codex/wire/vX_Y_Z.rs`, isolated from handwritten `src/codex/types.rs`;
- the generated wire registry.

The manifest contains no timestamp, host path, or binary path. Re-running `--check`
with the same binary must be byte-for-byte identical.

## Review a candidate

Add the exact version to `candidateVersions`, sync it, add a versioned contract
fixture, then generate the review report:

```bash
python3 tools/codex_schema.py diff \
  --baseline 0.146.0 --candidate X.Y.Z --write-defaults --allow-breaking
python3 tools/codex_schema.py contract --version X.Y.Z
python3 tools/codex_schema.py verify
```

The JSON report is the machine-readable source of truth. It separates optional
additions, notification additions, enum additions and widening from removals,
new required fields, type narrowing, enum removals, and closed objects. Enum
additions are accepted only because generated strings cross a handwritten compat
mapper into stable open enums with an `Unknown(String)` fallback.

Promotion requires moving the version to `supportedVersions`, updating the selected
compat boundary when needed, and passing offline `verify`. A breaking report or any
missing/invalid contract makes that check fail. Do not edit generated Rust or schema
artifacts by hand, do not rewrite `compatibilityBaselineVersion` to bypass a report,
and do not put schema export in `build.rs`.

Contract fixtures cover initialize, thread start/list/read/resume, turn
start/interrupt, the dynamic-tool reverse request, all notifications consumed by
the bridge, normal notification order, and retry/uncertain failure classification.
Every fixture record is checked against the same byte, nesting, and structural-token
limits as app-server JSONL. Tool errors identify only a contract/root label and never
echo fixture or remote payload text.

The scheduled `Codex Schema Upgrade Report` workflow discovers a newer npm release,
generates the normalized candidate report as a workflow artifact, and opens an
idempotent review issue. It never edits the supported range. Human contract review
is therefore mandatory before a version can be promoted.
