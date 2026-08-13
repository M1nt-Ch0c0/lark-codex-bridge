//! Versioned, strict outbox payloads.
//!
//! The store persists an opaque JSON string; this module is its single codec.
//! Every payload carries an explicit `version`, rejects unknown fields, is
//! size-capped before it enters the store, and never exposes message text in
//! `Debug` output or error values.

#![allow(clippy::doc_markdown)]

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::limits::STORE_OUTBOX_PAYLOAD_MAX_BYTES;

/// Current payload version. A stored payload with any other version is
/// rejected as undeliverable rather than being guessed at.
pub const OUTBOX_PAYLOAD_VERSION: u32 = 1;

/// One durable outbound operation.
#[derive(Clone, PartialEq, Eq)]
pub enum OutboxOperation {
    /// Reply to a message with text. `thread_id` selects the in-thread reply
    /// path for topic scopes; `None` is a plain reply.
    ReplyText {
        /// Target parent message ID.
        message_id: String,
        /// Topic thread ID when the reply belongs to a thread.
        thread_id: Option<String>,
        /// Already-masked reply text.
        text: String,
    },
}

impl fmt::Debug for OutboxOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReplyText {
                message_id,
                thread_id,
                text,
            } => formatter
                .debug_struct("ReplyText")
                .field("message_id_len", &message_id.len())
                .field("in_thread", &thread_id.is_some())
                .field("text_chars", &text.chars().count())
                .finish_non_exhaustive(),
        }
    }
}

/// Content-free codec failures. No variant carries dynamic payload text or a
/// raw field value, so decode errors are safe to log verbatim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboxError {
    /// Serialization failed while encoding an outbound operation.
    Serialize,
    /// The stored JSON could not be decoded as a strict payload.
    Deserialize,
    /// The payload declares an unknown version.
    UnsupportedVersion {
        /// The stored version number (a small integer, safe to report).
        version: u32,
    },
    /// The payload declares an operation this build does not know.
    UnknownOperation,
    /// The payload violates a closed invariant.
    Invalid {
        /// Static description of the violated invariant.
        context: &'static str,
    },
    /// The serialized payload exceeds the store's byte cap.
    PayloadTooLarge {
        /// The configured byte limit.
        limit: usize,
    },
}

impl fmt::Display for OutboxError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialize => write!(formatter, "encoding an outbox payload failed"),
            Self::Deserialize => write!(formatter, "outbox payload is not a strict payload"),
            Self::UnsupportedVersion { version } => {
                write!(formatter, "outbox payload version {version} is unsupported")
            }
            Self::UnknownOperation => write!(formatter, "outbox payload operation is unknown"),
            Self::Invalid { context } => write!(formatter, "outbox payload is invalid: {context}"),
            Self::PayloadTooLarge { limit } => {
                write!(formatter, "outbox payload exceeds the {limit}-byte cap")
            }
        }
    }
}

impl std::error::Error for OutboxError {}

impl OutboxOperation {
    /// Encodes this operation as a versioned, strict JSON payload, enforcing
    /// the byte cap before the string is returned.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::Serialize`] if the operation cannot be encoded,
    /// or [`OutboxError::PayloadTooLarge`] when the result exceeds the cap.
    pub fn encode(&self) -> Result<String, OutboxError> {
        let dto = PayloadDto::from(self);
        let json = serde_json::to_string(&dto).map_err(|_| OutboxError::Serialize)?;
        if json.len() > STORE_OUTBOX_PAYLOAD_MAX_BYTES {
            return Err(OutboxError::PayloadTooLarge {
                limit: STORE_OUTBOX_PAYLOAD_MAX_BYTES,
            });
        }
        Ok(json)
    }

    /// Decodes a stored payload, rejecting unknown fields, an unknown
    /// version, an unknown operation, and oversized input.
    ///
    /// # Errors
    ///
    /// Returns [`OutboxError::PayloadTooLarge`], [`OutboxError::Deserialize`],
    /// [`OutboxError::UnsupportedVersion`], [`OutboxError::UnknownOperation`],
    /// or [`OutboxError::Invalid`] for the corresponding failure.
    pub fn decode(json: &str) -> Result<Self, OutboxError> {
        if json.len() > STORE_OUTBOX_PAYLOAD_MAX_BYTES {
            return Err(OutboxError::PayloadTooLarge {
                limit: STORE_OUTBOX_PAYLOAD_MAX_BYTES,
            });
        }
        let dto: PayloadDto = serde_json::from_str(json).map_err(|_| OutboxError::Deserialize)?;
        if dto.version != OUTBOX_PAYLOAD_VERSION {
            return Err(OutboxError::UnsupportedVersion {
                version: dto.version,
            });
        }
        match dto.op.as_str() {
            "reply_text" => {
                if dto.message_id.is_empty() {
                    return Err(OutboxError::Invalid {
                        context: "empty reply message_id",
                    });
                }
                if dto.text.is_empty() {
                    return Err(OutboxError::Invalid {
                        context: "empty reply text",
                    });
                }
                if dto.thread_id.as_deref().is_some_and(str::is_empty) {
                    return Err(OutboxError::Invalid {
                        context: "empty reply thread_id",
                    });
                }
                Ok(Self::ReplyText {
                    message_id: dto.message_id,
                    thread_id: dto.thread_id,
                    text: dto.text,
                })
            }
            _ => Err(OutboxError::UnknownOperation),
        }
    }
}

/// Strict wire shape. `deny_unknown_fields` makes any forward-incompatible
/// field a hard decode failure instead of a silent drop.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PayloadDto {
    version: u32,
    op: String,
    message_id: String,
    #[serde(default)]
    thread_id: Option<String>,
    text: String,
}

impl From<&OutboxOperation> for PayloadDto {
    fn from(operation: &OutboxOperation) -> Self {
        match operation {
            OutboxOperation::ReplyText {
                message_id,
                thread_id,
                text,
            } => Self {
                version: OUTBOX_PAYLOAD_VERSION,
                op: "reply_text".to_owned(),
                message_id: message_id.clone(),
                thread_id: thread_id.clone(),
                text: text.clone(),
            },
        }
    }
}
