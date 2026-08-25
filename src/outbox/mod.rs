//! Durable outbound queue: versioned payloads, the bounded send pump, and the
//! [`OutboxReplySink`] adapter.
//!
//! The store (see [`crate::store`]) owns the rows and the atomic
//! state machine; this module owns the codec (`payload`), the send loop
//! (`pump`), and the runtime boundary (`sink`).

#![allow(clippy::doc_markdown)]

mod payload;
pub(crate) mod pump;
mod sink;

pub use payload::{OUTBOX_PAYLOAD_VERSION, OutboxError, OutboxOperation};
pub use pump::{
    AppliedCertainty, DeliveryClass, DeliveryDecision, OutboxHandle, OutboxPump, OutboxPumpConfig,
    Retryability, classify_delivery, delivery_decision,
};
pub use sink::OutboxReplySink;
