# Codex app-server schema maintenance

The bridge supports only exact Codex versions whose generated schema, wire DTOs,
compatibility mapping, and contract fixture have been reviewed. The authoritative
policy is [`protocol/codex/support-policy.json`](../protocol/codex/support-policy.json).
The established baseline and every promoted version are also pinned in the
append-only [`protocol/codex/support-history.json`](../protocol/codex/support-history.json).
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
- `incoming-audit.json`, inventorying every incoming enum/union as either an
  evidenced open fallback or a promotion-blocking closed construct;
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

The JSON report is the machine-readable source of truth. Its conservative
comparison covers type relationships (including integer as a subset of number),
finite enum/const sets, object/array/string/numeric constraints, and JSON Schema
combinators. An incoming enum or union addition is breaking unless the generated
audit points to a tested open fallback. Changes the comparator cannot prove safe
are blocking, not silently additive.

Promotion requires adding an append-only support-history record, moving the version
to `supportedVersions`, adding its explicit `WireAdapter` branch, and passing offline
`verify`. CI compares the history to the trusted pull-request base so deleting or
rewriting an earlier supported release cannot make a candidate trust itself.
`verify` re-renders Rust and the incoming audit from canonical schema bytes, compares
them byte-for-byte, and validates the complete manifest. Do not edit generated Rust
or schema artifacts by hand, and do not put schema export in `build.rs`.

Contract fixtures cover initialize, thread start/list/read/resume, turn
start/interrupt, the dynamic-tool reverse request, all notifications consumed by
the bridge, normal notification order, and retry/uncertain failure classification.
Rust tests drive those records through the production wire adapter, JSONL
encoder/decoder bounds, notification mapping, and actual retry classifier; the
Python validator checks the records against their selected schemas. Tool errors
identify only a contract/root label and never echo fixture or remote payload text.

At runtime the supervisor probes the exact binary version before initialization and
selects only a promoted adapter. Every outgoing request and reverse response is
serialized through that generated version, while every response, consumed
notification, and reverse request is decoded through it before reaching stable
domain types. Candidate 0.149.0 therefore cannot enter production paths.

The scheduled `Codex Schema Upgrade Report` workflow discovers a newer npm release,
generates the normalized candidate report as a workflow artifact, and opens an
idempotent review issue. It never edits the supported range. Human contract review
is therefore mandatory before a version can be promoted.
