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

pub const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(10);
pub const SUPERVISOR_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(60);
pub const CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(30);
pub const INTERRUPT_TIMEOUT: Duration = Duration::from_secs(10);
pub const VERSION_PROBE_TIMEOUT: Duration = Duration::from_secs(10);
