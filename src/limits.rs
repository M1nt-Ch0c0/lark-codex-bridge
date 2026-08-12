use std::time::Duration;

pub const MAX_JSONL_LINE_BYTES: usize = 32 * 1024 * 1024;
/// Conservative structural guard before allocating a `serde_json::Value`.
pub const MAX_JSON_STRUCTURAL_TOKENS: usize = 64 * 1024;
pub const MAX_JSON_NESTING: usize = 128;
/// Wire bytes accepted for a generic value before `serde_json::to_value`.
/// This intentionally leaves a wide margin for compact JSON expanding into a
/// tree of `Value` nodes; it is not allocator accounting.
pub const MAX_OUTBOUND_VALUE_WIRE_BYTES: usize = MAX_JSONL_LINE_BYTES / 32;
pub const RPC_HIGH_CAPACITY: usize = 64;
pub const RPC_NORMAL_CAPACITY: usize = 256;
pub const RPC_INFLIGHT_CAPACITY: usize = RPC_HIGH_CAPACITY + RPC_NORMAL_CAPACITY;
pub const RPC_TOTAL_PENDING_CAPACITY: usize = RPC_INFLIGHT_CAPACITY + RPC_HIGH_CAPACITY;
pub const RPC_SERVER_REQUEST_CAPACITY: usize = RPC_HIGH_CAPACITY;
pub const EVENT_CAPACITY: usize = 1024;
pub const RPC_RELIABLE_EVENT_CAPACITY: usize = RPC_SERVER_REQUEST_CAPACITY * 2;
pub const THREAD_EVENT_CAPACITY: usize = 256;
pub const THREAD_TERMINAL_CAPACITY: usize = 64;
pub const CLIENT_COMMAND_CAPACITY: usize = 256;
pub const CLIENT_CONTROL_CAPACITY: usize = 64;
pub const CLIENT_CONTROL_BYTE_BUDGET: usize = 8 * 1024 * 1024;
pub const CLIENT_CONTROL_EVENT_BYTE_LIMIT: usize = 4 * 1024 * 1024;
pub const CLIENT_PROJECTION_CAPACITY: usize = 64;
pub const CLIENT_SUBSCRIBER_CAPACITY: usize = 64;
pub const THREAD_SUBSCRIBER_CAPACITY: usize = 4;
pub const THREAD_MAILBOX_BYTE_BUDGET: usize = 4 * 1024 * 1024;
pub const THREAD_DELTA_BYTE_LIMIT: usize = 512 * 1024;
pub const THREAD_OUTCOME_CAPACITY: usize = 64;
pub const THREAD_PROJECTION_BYTE_BUDGET: usize = 4 * 1024 * 1024;
pub const ROUTING_ID_BYTE_LIMIT: usize = 1024;
pub const MAX_STDERR_LINE_BYTES: usize = 64 * 1024;
pub const MAX_VERSION_OUTPUT_BYTES: usize = 64 * 1024;
pub const HIGH_PRIORITY_BURST: usize = 8;
pub const TRANSPORT_BYTE_BUDGET: usize = 64 * 1024 * 1024;
pub const TRANSPORT_HIGH_BYTE_BUDGET: usize = TRANSPORT_BYTE_BUDGET;
pub const RPC_BYTE_BUDGET: usize = 64 * 1024 * 1024;
pub const RPC_HIGH_BYTE_BUDGET: usize = RPC_BYTE_BUDGET;
pub const RPC_RELIABLE_EVENT_BYTE_BUDGET: usize = RPC_BYTE_BUDGET;

/// Hard cap on a single Lark HTTP response body, enforced before JSON
/// parsing and mid-stream for close-delimited bodies.
pub const LARK_MAX_HTTP_BODY_BYTES: usize = 4 * 1024 * 1024;
/// Refresh the tenant access token this long before its server-declared
/// expiry (matches the reference SDK's early-expiry margin).
pub const TOKEN_REFRESH_SKEW: Duration = Duration::from_secs(3 * 60);

pub const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(10);
pub const SUPERVISOR_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(60);
pub const CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(30);
pub const INTERRUPT_TIMEOUT: Duration = Duration::from_secs(10);
pub const VERSION_PROBE_TIMEOUT: Duration = Duration::from_secs(10);
/// Shared timeout for every Lark HTTP request (matches the reference
/// bootstrap request timeout).
pub const LARK_HTTP_TIMEOUT: Duration = Duration::from_secs(15);
/// Overall deadline for one QR registration session (matches the reference
/// QR session TTL).
pub const LARK_REGISTER_TIMEOUT: Duration = Duration::from_secs(20 * 60);

/// Total bytes buffered across all in-flight fragmented Lark messages.
pub const LARK_FRAGMENT_TOTAL_BYTES: usize = 8 * 1024 * 1024;
/// Maximum reassembled payload size of a single Lark message; also the WebSocket
/// binary message cap, since one fragment can never legally exceed it.
pub const LARK_FRAGMENT_MESSAGE_BYTES: usize = 1024 * 1024;
/// Maximum fragment count of one Lark message; `sum` headers above this are
/// rejected instead of allocating a huge slot vector.
pub const LARK_FRAGMENT_MESSAGE_MAX_FRAGMENTS: usize = 64;
/// Maximum distinct fragmented Lark messages buffered concurrently.
pub const LARK_FRAGMENT_MAX_IN_FLIGHT: usize = 64;
/// Time-to-live of a partially reassembled Lark message, swept on ingest
/// (deliberately tighter than the reference SDK's 10 s interval sweep).
pub const LARK_FRAGMENT_TTL: Duration = Duration::from_secs(5);

/// After the transport sends a ping, any inbound frame within this window
/// proves liveness; otherwise the socket is dropped to trigger a reconnect.
pub const LARK_PONG_TIMEOUT: Duration = Duration::from_secs(10);
/// Fallback ping interval when the server bootstrap does not provide a
/// positive `PingInterval`.
pub const LARK_DEFAULT_PING_INTERVAL: Duration = Duration::from_secs(30);
/// Timeout for one WebSocket connect/handshake attempt.
pub const LARK_WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// Bounded grace for the Lark WebSocket actor to close the socket on shutdown.
pub const LARK_TRANSPORT_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
/// Count bound of the transport observation channel (state/messages/anomalies).
pub const LARK_TRANSPORT_EVENT_CAPACITY: usize = 64;
/// Byte budget for message payloads parked in the transport observation
/// channel; permits are held until the receiver dequeues.
pub const LARK_TRANSPORT_EVENT_BYTE_BUDGET: usize = 8 * 1024 * 1024;
