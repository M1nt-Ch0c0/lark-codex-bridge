//! Versioned NDJSON contract shared with the Node sidecar.

/// Stable protocol name.
pub const PROTOCOL: &str = "lark-channel";
/// Only wire version currently accepted.
pub const VERSION: u16 = 1;
/// Capabilities required from an inbound sidecar.
pub const REQUIRED_CAPABILITIES: &[&str] = &[
    "connection_state",
    "durable_event_ack",
    "inbound_events",
    "graceful_shutdown",
];
