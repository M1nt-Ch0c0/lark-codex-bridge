use std::time::Duration;

pub const MAX_JSONL_LINE_BYTES: usize = 32 * 1024 * 1024;
pub const RPC_HIGH_CAPACITY: usize = 64;
pub const RPC_NORMAL_CAPACITY: usize = 256;
pub const RPC_INFLIGHT_CAPACITY: usize = RPC_HIGH_CAPACITY + RPC_NORMAL_CAPACITY;
pub const RPC_SERVER_REQUEST_CAPACITY: usize = RPC_HIGH_CAPACITY;
pub const EVENT_CAPACITY: usize = 1024;
pub const MAX_STDERR_LINE_BYTES: usize = 64 * 1024;
pub const MAX_VERSION_OUTPUT_BYTES: usize = 64 * 1024;
pub const HIGH_PRIORITY_BURST: usize = 8;
pub const TRANSPORT_BYTE_BUDGET: usize = 64 * 1024 * 1024;
pub const RPC_BYTE_BUDGET: usize = 64 * 1024 * 1024;

pub const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(10);
pub const CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(30);
pub const INTERRUPT_TIMEOUT: Duration = Duration::from_secs(10);
pub const VERSION_PROBE_TIMEOUT: Duration = Duration::from_secs(10);
