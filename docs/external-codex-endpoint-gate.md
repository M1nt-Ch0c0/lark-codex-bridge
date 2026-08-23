# External Codex endpoint admission gate

Issue [#28](https://github.com/M1nt-Ch0c0/lark-codex-bridge/issues/28) adds the
fail-closed configuration and admission boundary required before an external
Codex app-server can be used. It does **not** add the long-running WebSocket RPC
transport, reconnect, or external mutation support; those remain in #29-#31.
Consequently, selecting `external_endpoint` in the normal `run` path never
falls back to a spawned child and currently stops before a runtime is created.

## Tagged backend configuration

Spawned stdio remains the generated/default mode and owns its child process:

```toml
[codex.backend]
mode = "spawned_stdio"
binary = "codex"
# codex_home = "/absolute/private/profile"
```

External mode owns only a connection attempt. Its bearer is referenced through
a file and is never embedded in TOML or the endpoint:

```toml
[codex.backend]
mode = "external_endpoint"
endpoint = "wss://codex.example.invalid/app-server"
expected_codex_version = "0.149.0"
capability_profile = "observe_shared"

[codex.backend.authentication]
source = "bearer_token_file"
path = "/absolute/private/app-server.bearer"
```

The tagged enum rejects spawn-only fields in external mode and external-only
fields in spawned mode during deserialization. Unknown fields are errors. There
is no `auto` mode and no external-to-spawn fallback.

Only `observe_shared` is admitted by this bounded change. Later profiles remain
disabled until their transport and mutation/reconciliation issues land.

## Endpoint and credential policy

- `ws://` requires a literal loopback IPv4 or IPv6 address. A hostname that
  resolves to loopback is not accepted.
- Non-loopback endpoints require `wss://` with the platform trust store,
  certificate validation, and hostname validation. There is no insecure TLS
  switch.
- Userinfo, credentials, query strings, fragments, control characters, port
  zero, and overlong endpoints are rejected.
- Every endpoint, including loopback, requires a bearer-token file. It must be
  an absolute regular non-symlink path. Unix files must deny all group/other
  permissions. Tokens are bounded, valid UTF-8 header material with at least 32
  bytes; one trailing newline is accepted.
- Endpoint URLs, hosts, paths, authorization headers, bearer values, raw RPC
  payloads, initialize `codexHome`, and thread IDs do not appear in ordinary
  errors or `Debug`. A stable `ext-…` hash label identifies non-secret endpoint
  configuration.

Credential rotation is explicit: atomically replace the private token file,
drain the old connection, and start a new admission check/reconnect. The gate
loads the file immediately before every new connection. It never retries with
an old token, another source, weaker authentication, plaintext remote transport,
or spawned stdio.

## Admission sequence

One bounded `ExternalEndpointGate::check` performs:

1. URL, exact-version, promoted-profile, and secret-source validation;
2. WebSocket HTTP Upgrade with `Authorization: Bearer …`;
3. one typed `initialize` using static bridge metadata and explicit
   capabilities;
4. exact version extraction from the typed `userAgent` response;
5. `initialized`;
6. a typed, one-row `thread/list` read-only canary.

Frames, message count, aggregate bytes, JSON structure, and the whole operation
are bounded. Startup notifications are decoded and discarded while awaiting the
two correlated responses. Server requests, wrong response IDs, binary frames,
raw/malformed envelopes, rejected authentication, a mismatched version, or a
missing/malformed canary fail closed. Returned thread records are validated and
dropped; the report contains no thread identifier and enables no mutation.

## Verification

The ordinary suite runs fake-server coverage for authentication rejection,
rotation, cross-mode deserialization, unsafe URLs, exact version, missing
capability, protocol failures, credential-file policy, redaction, and no spawn
fallback:

```bash
cargo test --locked --test external_endpoint_gate
```

The real smoke starts the explicitly supplied exact binary as a separately
owned authenticated listener, proves that a wrong bearer is rejected, proves
that an unpromoted expected version is rejected, and then passes the real
authentication/initialize/version/list gate. It must be invoked by exact name:

```bash
CODEX_EXTERNAL_GATE_E2E=1 \
CODEX_EXTERNAL_GATE_BINARY=/absolute/path/to/codex \
CODEX_EXTERNAL_GATE_EXPECTED_VERSION=0.149.0 \
cargo test --locked --test external_endpoint_smoke \
  real_exact_binary_enforces_external_auth_version_and_capability_gate \
  -- --ignored --exact --nocapture
```

The ignored marker keeps ordinary builds independent of an installed binary;
it is not acceptance evidence. Once invoked, a missing/empty gate variable,
missing binary, invalid credential behavior, skipped test selection, or any
failed gate is a hard nonzero test failure. CI installs the exact official
0.149.0 package and executes this ignored test by exact name on Linux, macOS,
and Windows, so the required smoke is not skipped by the acceptance workflow.
