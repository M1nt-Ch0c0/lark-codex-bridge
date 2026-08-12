use std::time::Duration;

pub const MAX_JSONL_LINE_BYTES: usize = 32 * 1024 * 1024;
pub const RPC_HIGH_CAPACITY: usize = 64;
pub const RPC_NORMAL_CAPACITY: usize = 256;
pub const EVENT_CAPACITY: usize = 1024;

pub const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(10);
pub const CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(30);
pub const INTERRUPT_TIMEOUT: Duration = Duration::from_secs(10);
