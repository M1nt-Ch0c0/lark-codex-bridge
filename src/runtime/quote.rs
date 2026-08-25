//! Authorized, one-hop resolution of directly quoted Lark messages.
//!
//! The resolver accepts only a parent ID copied from a trusted inbound event.
//! Callers are responsible for applying sender/group policy before invoking
//! it. Implementations never recurse through the fetched item's own
//! `parent_id` or `root_id`.

use std::fmt;

use futures_util::future::{BoxFuture, FutureExt};

use crate::lark::api::{ChatMode, LarkApi};
use crate::lark::error::LarkError;
use crate::lark::normalize::{MessagePart, normalize_message_parts};
use crate::limits::{
    ATTACHMENT_RESOURCE_KEY_MAX_BYTES, QUOTE_CONTENT_MAX_BYTES, QUOTE_MAX_PARTS,
    STORE_INBOUND_ID_MAX_BYTES, STORE_INBOUND_MESSAGE_TYPE_MAX_BYTES,
};
use crate::runtime::context::{DraftPart, QuoteDraft, QuoteStatus, draft_part_from_inbound};
use crate::runtime::policy::AccessPolicy;

/// Trusted identifiers required for one direct-parent lookup.
#[derive(Clone)]
pub struct QuoteRequest {
    /// Parent ID copied from the current inbound receive event.
    pub parent_message_id: String,
    /// Chat containing the authorized trigger; fetched parents must match it.
    pub chat_id: String,
}

impl fmt::Debug for QuoteRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuoteRequest")
            .field("parent_message_id_len", &self.parent_message_id.len())
            .field("chat_id_len", &self.chat_id.len())
            .finish_non_exhaustive()
    }
}

/// Fakeable boundary used by the scope runtime after access policy succeeds.
pub trait QuoteResolver: Send + Sync {
    /// Resolves exactly the requested direct parent into a safe snapshot draft.
    fn resolve(&self, request: QuoteRequest) -> BoxFuture<'static, QuoteDraft>;
}

/// Production resolver backed by the tenant-bound Lark `OpenAPI` client.
#[derive(Clone)]
pub struct LarkQuoteResolver {
    api: LarkApi,
    policy: AccessPolicy,
}

impl LarkQuoteResolver {
    /// Creates the tenant-bound one-hop resolver.
    #[must_use]
    pub fn new(api: LarkApi, policy: AccessPolicy) -> Self {
        Self { api, policy }
    }
}

impl fmt::Debug for LarkQuoteResolver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LarkQuoteResolver")
            .finish_non_exhaustive()
    }
}

impl QuoteResolver for LarkQuoteResolver {
    fn resolve(&self, request: QuoteRequest) -> BoxFuture<'static, QuoteDraft> {
        let api = self.api.clone();
        let policy = self.policy.clone();
        async move {
            let parent_id = request.parent_message_id;
            let raw = match api.get_message(&parent_id).await {
                Ok(raw) => raw,
                Err(error) => return degraded(parent_id, status_for_error(&error)),
            };
            // Bind the lookup result to both trusted request identifiers. A
            // mismatched response is unavailable rather than exposing fields
            // from another conversation.
            if raw.message_id != parent_id || raw.chat_id != request.chat_id {
                return degraded(parent_id, QuoteStatus::Unauthorized);
            }
            if raw.deleted {
                return degraded(parent_id, QuoteStatus::Deleted);
            }
            let chat_type = match raw.chat_type.as_str() {
                "p2p" => ChatMode::P2p,
                "group" if raw.thread_id.is_some() => ChatMode::Topic,
                "group" => ChatMode::Group,
                _ => return degraded(parent_id, QuoteStatus::Unauthorized),
            };
            let Some(sender_id) = raw.sender_id.as_deref() else {
                return degraded(parent_id, QuoteStatus::Unauthorized);
            };
            if sender_id.is_empty() || sender_id.len() > STORE_INBOUND_ID_MAX_BYTES {
                return degraded(parent_id, QuoteStatus::Unauthorized);
            }
            let sender_is_human = raw.sender_type.as_deref() == Some("user");
            if !policy.allows_quoted_parent(sender_id, sender_is_human, &raw.chat_id, chat_type) {
                return degraded(parent_id, QuoteStatus::Unauthorized);
            }
            if raw.message_type.is_empty()
                || raw.message_type.len() > STORE_INBOUND_MESSAGE_TYPE_MAX_BYTES
            {
                return degraded(parent_id, QuoteStatus::Oversize);
            }
            let Some(content) = raw.content else {
                return degraded_with_type(parent_id, raw.message_type, QuoteStatus::Unavailable);
            };
            if content.len() > QUOTE_CONTENT_MAX_BYTES {
                return degraded_with_type(parent_id, raw.message_type, QuoteStatus::Oversize);
            }
            let Ok(parts) = normalize_message_parts(&raw.message_type, &content) else {
                return degraded_with_type(parent_id, raw.message_type, QuoteStatus::Unavailable);
            };
            if parts.len() > QUOTE_MAX_PARTS || parts.iter().any(part_descriptor_oversize) {
                return degraded_with_type(parent_id, raw.message_type, QuoteStatus::Oversize);
            }
            let status = if parts.iter().all(|part| {
                matches!(
                    part,
                    MessagePart::Unsupported { .. } | MessagePart::Card { .. }
                )
            }) {
                QuoteStatus::Unsupported
            } else {
                QuoteStatus::Available
            };
            QuoteDraft {
                message_id: parent_id,
                message_type: Some(raw.message_type),
                status,
                parts: parts.iter().map(draft_part_from_inbound).collect(),
            }
        }
        .boxed()
    }
}

fn part_descriptor_oversize(part: &MessagePart) -> bool {
    match part {
        MessagePart::Image(media)
        | MessagePart::File(media)
        | MessagePart::Sticker(media)
        | MessagePart::Audio(media)
        | MessagePart::Video(media) => [media.key.as_deref(), media.thumbnail_key.as_deref()]
            .into_iter()
            .flatten()
            .any(|key| key.len() > ATTACHMENT_RESOURCE_KEY_MAX_BYTES),
        MessagePart::Text { .. }
        | MessagePart::Forward { .. }
        | MessagePart::Card { .. }
        | MessagePart::Unsupported { .. } => false,
    }
}

fn status_for_error(error: &LarkError) -> QuoteStatus {
    match error {
        LarkError::Exhausted { .. } => QuoteStatus::Oversize,
        // Documented message-get/reply codes: bot not in chat, bot disabled,
        // missing permission, and invisible message all fail closed as an
        // authorization boundary. A recalled parent has a stable deleted
        // degradation. Unknown envelope failures remain unavailable.
        LarkError::PermanentAuth { .. }
        | LarkError::Retryable {
            code: Some(230_002 | 230_006 | 230_027 | 230_050),
            ..
        } => QuoteStatus::Unauthorized,
        LarkError::Retryable {
            code: Some(230_011),
            ..
        }
        | LarkError::ProtocolViolation {
            code: Some(404), ..
        } => QuoteStatus::Deleted,
        LarkError::Retryable { .. }
        | LarkError::ProtocolViolation { .. }
        | LarkError::InvalidRequest { .. } => QuoteStatus::Unavailable,
    }
}

fn degraded(message_id: String, status: QuoteStatus) -> QuoteDraft {
    QuoteDraft {
        message_id,
        message_type: None,
        status,
        parts: Vec::new(),
    }
}

fn degraded_with_type(message_id: String, message_type: String, status: QuoteStatus) -> QuoteDraft {
    QuoteDraft {
        message_id,
        message_type: Some(message_type),
        status,
        parts: Vec::<DraftPart>::new(),
    }
}
