# Official SDK inbound sidecar

This directory is a deployable Node component, not a runtime installer. Run
`npm ci --ignore-scripts` during build/deployment and ship `node_modules` with
the checked-in `index.cjs`, `package.json`, and lockfile. The Rust process never
invokes npm.

`index.cjs` intentionally uses the SDK's low-level `WSClient` and a narrow
`EventDispatcher.invoke` subclass that preserves the unflattened raw envelope.
Rust remains authoritative for normalization, policy,
durable intake, and upstream acknowledgement. Stdout is reserved for the
versioned NDJSON protocol documented in
[`docs/channel-wire-v1.md`](../docs/channel-wire-v1.md); all logs go to stderr
as static classifications.
