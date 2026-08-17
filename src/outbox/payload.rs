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
    /// Creates the first visible progress card for a turn.
    ReplyProgressCard {
        /// Target parent message ID.
        message_id: String,
        /// Topic thread ID when the reply belongs to a thread.
        thread_id: Option<String>,
        /// Already-masked cumulative progress text.
        text: String,
    },
    /// Updates the first progress card in place.
    UpdateProgressCard {
        /// Idempotency key of the first progress-card outbox row.
        anchor_key: String,
        /// Already-masked cumulative progress text.
        text: String,
    },
    /// Finalizes a progress card in place, with a standalone fallback target
    /// when the initial card definitively failed before delivery.
    FinalizeProgressCard {
        /// Idempotency key of the first progress-card outbox row.
        anchor_key: String,
        /// Original parent message used only after definitive anchor failure.
        message_id: String,
        /// Topic thread for the definitive-failure fallback.
        thread_id: Option<String>,
        /// Complete, already-masked fallback answer.
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
            Self::ReplyProgressCard {
                message_id,
                thread_id,
                text,
            } => formatter
                .debug_struct("ReplyProgressCard")
                .field("message_id_len", &message_id.len())
                .field("in_thread", &thread_id.is_some())
                .field("text_chars", &text.chars().count())
                .finish_non_exhaustive(),
            Self::UpdateProgressCard { anchor_key, text } => formatter
                .debug_struct("UpdateProgressCard")
                .field("anchor_key_len", &anchor_key.len())
                .field("text_chars", &text.chars().count())
                .finish_non_exhaustive(),
            Self::FinalizeProgressCard {
                anchor_key,
                message_id,
                thread_id,
                text,
            } => formatter
                .debug_struct("FinalizeProgressCard")
                .field("anchor_key_len", &anchor_key.len())
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
            "reply_text" | "reply_progress_card" => {
                let Some(message_id) = dto.message_id else {
                    return Err(OutboxError::Invalid {
                        context: "empty reply message_id",
                    });
                };
                if message_id.is_empty() || dto.anchor_key.is_some() {
                    return Err(OutboxError::Invalid {
                        context: "invalid reply target",
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
                if dto.op == "reply_text" {
                    Ok(Self::ReplyText {
                        message_id,
                        thread_id: dto.thread_id,
                        text: dto.text,
                    })
                } else {
                    Ok(Self::ReplyProgressCard {
                        message_id,
                        thread_id: dto.thread_id,
                        text: dto.text,
                    })
                }
            }
            "update_progress_card" => {
                let Some(anchor_key) = dto.anchor_key else {
                    return Err(OutboxError::Invalid {
                        context: "empty progress anchor_key",
                    });
                };
                if anchor_key.is_empty()
                    || dto.message_id.is_some()
                    || dto.thread_id.is_some()
                    || dto.text.is_empty()
                {
                    return Err(OutboxError::Invalid {
                        context: "invalid progress update",
                    });
                }
                Ok(Self::UpdateProgressCard {
                    anchor_key,
                    text: dto.text,
                })
            }
            "finalize_progress_card" => {
                let (Some(anchor_key), Some(message_id)) = (dto.anchor_key, dto.message_id) else {
                    return Err(OutboxError::Invalid {
                        context: "missing progress finalization target",
                    });
                };
                if anchor_key.is_empty() || message_id.is_empty() || dto.text.is_empty() {
                    return Err(OutboxError::Invalid {
                        context: "invalid progress finalization",
                    });
                }
                if dto.thread_id.as_deref().is_some_and(str::is_empty) {
                    return Err(OutboxError::Invalid {
                        context: "empty reply thread_id",
                    });
                }
                Ok(Self::FinalizeProgressCard {
                    anchor_key,
                    message_id,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    message_id: Option<String>,
    #[serde(default)]
    thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    anchor_key: Option<String>,
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
                message_id: Some(message_id.clone()),
                thread_id: thread_id.clone(),
                anchor_key: None,
                text: text.clone(),
            },
            OutboxOperation::ReplyProgressCard {
                message_id,
                thread_id,
                text,
            } => Self {
                version: OUTBOX_PAYLOAD_VERSION,
                op: "reply_progress_card".to_owned(),
                message_id: Some(message_id.clone()),
                thread_id: thread_id.clone(),
                anchor_key: None,
                text: text.clone(),
            },
            OutboxOperation::UpdateProgressCard { anchor_key, text } => Self {
                version: OUTBOX_PAYLOAD_VERSION,
                op: "update_progress_card".to_owned(),
                message_id: None,
                thread_id: None,
                anchor_key: Some(anchor_key.clone()),
                text: text.clone(),
            },
            OutboxOperation::FinalizeProgressCard {
                anchor_key,
                message_id,
                thread_id,
                text,
            } => Self {
                version: OUTBOX_PAYLOAD_VERSION,
                op: "finalize_progress_card".to_owned(),
                message_id: Some(message_id.clone()),
                thread_id: thread_id.clone(),
                anchor_key: Some(anchor_key.clone()),
                text: text.clone(),
            },
        }
    }
}
