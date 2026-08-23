//! Durable outbound queue: versioned payloads, the bounded send pump, and the
//! [`DurableReplySink`](crate::runtime::scope::DurableReplySink) adapter.
//!
//! [`StoreHandle`](crate::store::StoreHandle) owns the rows and the atomic
//! state machine; this module owns the private `payload` codec, `pump` send
//! loop, and `sink` runtime boundary.

#![allow(clippy::doc_markdown)]

mod payload;
mod pump;
mod sink;

pub use payload::{OUTBOX_PAYLOAD_VERSION, OutboxError, OutboxOperation};
pub use pump::{DeliveryClass, OutboxHandle, OutboxPump, OutboxPumpConfig, classify_delivery};
pub use sink::OutboxReplySink;
