//! Bounded `message_id`/`seq`/`sum` fragment reassembly.
//!
//! Semantics follow the reference SDK's `DataCache` (per-message slot vector
//! keyed by `message_id`, indexed by `seq`, complete when all `sum` slots are
//! filled) with the design's deliberate hardening on top — the reference uses
//! a 10 s timer sweep and no byte bounds, and this implementation does **not**
//! copy that:
//!
//! - single-fragment frames (`sum == 1` or no `sum` header) pass through
//!   without caching;
//! - four simultaneous bounds: [`LARK_FRAGMENT_TOTAL_BYTES`] across all
//!   in-flight messages, [`LARK_FRAGMENT_MESSAGE_BYTES`] per message,
//!   [`LARK_FRAGMENT_MESSAGE_MAX_FRAGMENTS`] per message (also capping `sum`)
//!   plus [`LARK_FRAGMENT_MAX_IN_FLIGHT`] concurrent messages, and a
//!   [`LARK_FRAGMENT_TTL`] (5 s) per message swept on ingest (no background
//!   timer);
//! - duplicate `seq`, missing/`seq >= sum`, `sum == 0`, and a conflicting
//!   `sum` for an in-flight `message_id` are rejected and logged as protocol
//!   anomalies with IDs only — never payload bytes.

use std::collections::HashMap;
use std::fmt;
use std::time::Instant;

use bytes::Bytes;

use super::frame::{FrameHeaders, header_key};
use crate::limits::{
    LARK_FRAGMENT_MAX_IN_FLIGHT, LARK_FRAGMENT_MESSAGE_BYTES, LARK_FRAGMENT_MESSAGE_MAX_FRAGMENTS,
    LARK_FRAGMENT_TOTAL_BYTES, LARK_FRAGMENT_TTL,
};

/// One completely reassembled message payload.
///
/// `Debug` prints the payload length only, never the bytes.
#[derive(Clone, PartialEq, Eq)]
pub struct Reassembly {
    /// The message identifier shared by all fragments.
    pub message_id: String,
    /// The server trace identifier, when present.
    pub trace_id: Option<String>,
    /// The complete payload (UTF-8 JSON for event/card frames).
    pub payload: Bytes,
}

impl fmt::Debug for Reassembly {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Reassembly")
            .field("message_id", &self.message_id)
            .field("trace_id", &self.trace_id)
            .field("payload_len", &self.payload.len())
            .finish()
    }
}

/// Why a fragment was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReassemblyError {
    /// A fragment with an already-buffered `seq` arrived again.
    Duplicate,
    /// `sum == 0`, `seq >= sum`, missing `seq`/`message_id`, or a conflicting
    /// `sum` for an in-flight message.
    OutOfRange,
    /// A per-message or total byte bound would be exceeded.
    OverBytes,
    /// A fragment-count or in-flight-message bound would be exceeded.
    TooManyFragments,
    /// The message this fragment continues had already expired.
    Expired,
}

impl ReassemblyError {
    /// Returns the stable anomaly kind string used in logs and events.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Duplicate => "duplicate-fragment",
            Self::OutOfRange => "fragment-out-of-range",
            Self::OverBytes => "fragment-over-bytes",
            Self::TooManyFragments => "too-many-fragments",
            Self::Expired => "fragment-expired",
        }
    }
}

impl fmt::Display for ReassemblyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::error::Error for ReassemblyError {}

/// Parses the `sum` header: absent means a single-fragment message; present
/// but unparsable is a protocol anomaly.
fn parse_sum(headers: &FrameHeaders) -> Result<u32, ReassemblyError> {
    match headers.get(header_key::SUM) {
        None => Ok(1),
        Some(raw) => raw.parse::<u32>().map_err(|_| ReassemblyError::OutOfRange),
    }
}

struct Entry {
    sum: u32,
    trace_id: Option<String>,
    slots: Vec<Option<Bytes>>,
    filled: u32,
    bytes: usize,
    created: Instant,
}

/// Bounded fragment reassembler; one instance per WebSocket session.
///
/// In-flight fragments die with the connection, so the transport creates a
/// fresh reassembler per session. `Debug` is manual: buffered fragment bytes
/// are message content and are never printed (counts/bytes only).
#[derive(Default)]
pub struct Reassembler {
    entries: HashMap<String, Entry>,
    total_bytes: usize,
}

impl fmt::Debug for Reassembler {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Reassembler")
            .field("in_flight", &self.entries.len())
            .field("total_bytes", &self.total_bytes)
            .finish()
    }
}

impl Reassembler {
    /// Creates an empty reassembler.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of in-flight (partially buffered) messages.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.entries.len()
    }

    /// Total bytes currently buffered across all in-flight messages.
    #[must_use]
    pub fn buffered_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Ingests one fragment.
    ///
    /// Returns `Ok(Some)` exactly once per message, when the final missing
    /// fragment arrives; `Ok(None)` while the message is still incomplete.
    ///
    /// # Errors
    ///
    /// Returns a [`ReassemblyError`] for protocol anomalies (duplicate,
    /// out-of-range, expired) and for bound violations; every rejection is
    /// logged with the `message_id`/`trace_id` only.
    ///
    /// # Panics
    ///
    /// Never in practice; the two `expect`s document internal invariants (the
    /// completed entry exists, and `filled == sum` implies every slot is set).
    pub fn ingest(
        &mut self,
        headers: &FrameHeaders,
        payload: Bytes,
        now: Instant,
    ) -> Result<Option<Reassembly>, ReassemblyError> {
        let message_id = headers.message_id().unwrap_or_default();
        let trace_id = headers.trace_id().map(str::to_owned);
        let reject = |error: ReassemblyError| {
            tracing::warn!(
                message_id,
                trace_id = trace_id.as_deref().unwrap_or(""),
                kind = error.as_str(),
                "lark fragment rejected"
            );
            error
        };

        // Sweep expired entries before any validation or early return so the
        // TTL is enforced on every ingest, not only on well-formed fragments.
        // Remember whether this message's own entry expired so a late
        // continuation reports `Expired` instead of starting over mid-sequence.
        let entry_expired = self
            .entries
            .get(message_id)
            .is_some_and(|entry| now.duration_since(entry.created) > LARK_FRAGMENT_TTL);
        self.sweep(now);

        // A missing `sum` header means a single fragment; a present but
        // unparsable one is a protocol anomaly, not a passthrough.
        let sum = match parse_sum(headers) {
            Ok(sum) => sum,
            Err(error) => return Err(reject(error)),
        };

        if payload.len() > LARK_FRAGMENT_MESSAGE_BYTES {
            return Err(reject(ReassemblyError::OverBytes));
        }
        if sum == 1 {
            // Single-fragment fast path: no caching, no count/in-flight bounds.
            return Ok(Some(Reassembly {
                message_id: message_id.to_owned(),
                trace_id,
                payload,
            }));
        }
        if sum == 0 || sum as usize > LARK_FRAGMENT_MESSAGE_MAX_FRAGMENTS {
            return Err(reject(if sum == 0 {
                ReassemblyError::OutOfRange
            } else {
                ReassemblyError::TooManyFragments
            }));
        }
        if message_id.is_empty() {
            return Err(reject(ReassemblyError::OutOfRange));
        }
        let Some(seq) = headers.seq() else {
            return Err(reject(ReassemblyError::OutOfRange));
        };
        if seq >= sum {
            return Err(reject(ReassemblyError::OutOfRange));
        }
        if entry_expired {
            return Err(reject(ReassemblyError::Expired));
        }

        let len = payload.len();
        if self.total_bytes + len > LARK_FRAGMENT_TOTAL_BYTES {
            return Err(reject(ReassemblyError::OverBytes));
        }
        let entry = if let Some(entry) = self.entries.get_mut(message_id) {
            if entry.sum != sum {
                return Err(reject(ReassemblyError::OutOfRange));
            }
            entry
        } else {
            if self.entries.len() >= LARK_FRAGMENT_MAX_IN_FLIGHT {
                return Err(reject(ReassemblyError::TooManyFragments));
            }
            self.entries.entry(message_id.to_owned()).or_insert(Entry {
                sum,
                trace_id: trace_id.clone(),
                slots: vec![None; sum as usize],
                filled: 0,
                bytes: 0,
                created: now,
            })
        };
        let slot = &mut entry.slots[seq as usize];
        if slot.is_some() {
            return Err(reject(ReassemblyError::Duplicate));
        }
        if entry.bytes + len > LARK_FRAGMENT_MESSAGE_BYTES {
            return Err(reject(ReassemblyError::OverBytes));
        }
        *slot = Some(payload);
        entry.filled += 1;
        entry.bytes += len;
        self.total_bytes += len;

        if entry.filled < entry.sum {
            return Ok(None);
        }
        let entry = self
            .remove_entry(message_id)
            .expect("completed entry was just present");
        let mut payload = Vec::with_capacity(entry.bytes);
        for slot in entry.slots {
            let fragment = slot.expect("all slots filled when filled == sum");
            payload.extend_from_slice(&fragment);
        }
        Ok(Some(Reassembly {
            message_id: message_id.to_owned(),
            trace_id: entry.trace_id,
            payload: Bytes::from(payload),
        }))
    }

    /// Drops every entry whose TTL has lapsed; called on each ingest so no
    /// background timer is needed.
    fn sweep(&mut self, now: Instant) {
        let expired: Vec<String> = self
            .entries
            .iter()
            .filter(|(_, entry)| now.duration_since(entry.created) > LARK_FRAGMENT_TTL)
            .map(|(message_id, entry)| {
                tracing::warn!(
                    message_id = message_id.as_str(),
                    trace_id = entry.trace_id.as_deref().unwrap_or(""),
                    kind = ReassemblyError::Expired.as_str(),
                    "lark fragment message expired"
                );
                message_id.clone()
            })
            .collect();
        for message_id in expired {
            self.remove_entry(&message_id);
        }
    }

    fn remove_entry(&mut self, message_id: &str) -> Option<Entry> {
        let entry = self.entries.remove(message_id)?;
        self.total_bytes -= entry.bytes;
        Some(entry)
    }
}
