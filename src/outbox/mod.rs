//! Durable outbound queue: versioned payloads, the bounded send pump, and the
//! [`DurableReplySink`](crate::runtime::scope::DurableReplySink) adapter.
//!
//! The store layer owns the rows and the atomic state machine; this module owns
//! the `payload` codec, the `pump` send loop, and the `sink` runtime boundary.

#![allow(clippy::doc_markdown)]

mod payload;
mod pump;
mod sink;

pub use payload::{OUTBOX_PAYLOAD_VERSION, OutboxError, OutboxOperation};
pub use pump::{
    AppliedCertainty, DeliveryClass, DeliveryDecision, OutboxHandle, OutboxPump, OutboxPumpConfig,
    Retryability, classify_delivery, delivery_decision,
};
pub use sink::OutboxReplySink;
