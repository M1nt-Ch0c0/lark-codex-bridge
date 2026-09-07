//! Authorized, one-hop resolution of directly quoted Lark messages.
//!
//! The resolver accepts only a parent ID copied from a trusted inbound event.
//! Callers are responsible for applying sender/group policy before invoking
//! it. Implementations never recurse through the fetched item's own
//! `parent_id` or `root_id`.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex};

use futures_util::future::{BoxFuture, FutureExt};
use serde_json::Value;

use crate::lark::api::{ChatMode, LarkApi, RawMessage};
use crate::lark::error::LarkError;
use crate::lark::normalize::{MessagePart, normalize_message_parts};
use crate::limits::{
    ATTACHMENT_RESOURCE_KEY_MAX_BYTES, FORWARD_EXPAND_MAX_DEPTH, FORWARD_EXPAND_MAX_ITEMS,
    QUOTE_CONTENT_MAX_BYTES, QUOTE_MAX_PARTS, STORE_INBOUND_ID_MAX_BYTES,
    STORE_INBOUND_MESSAGE_TYPE_MAX_BYTES, TOPIC_CONTEXT_MAX_MESSAGES,
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
    /// Chat mode of the authorized trigger. The message-get item often omits
    /// `chat_type`; the resolver then inherits this instead of failing closed.
    pub chat_mode: ChatMode,
}

impl fmt::Debug for QuoteRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuoteRequest")
            .field("parent_message_id_len", &self.parent_message_id.len())
            .field("chat_id_len", &self.chat_id.len())
            .field("chat_mode", &self.chat_mode)
            .finish_non_exhaustive()
    }
}

/// Trusted identifiers for a first-engagement topic history fetch.
#[derive(Clone)]
pub struct TopicContextRequest {
    pub thread_id: String,
    pub chat_id: String,
    pub exclude_ids: HashSet<String>,
    pub max_messages: usize,
}

impl fmt::Debug for TopicContextRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TopicContextRequest")
            .field("thread_id_len", &self.thread_id.len())
            .field("chat_id_len", &self.chat_id.len())
            .field("exclude_count", &self.exclude_ids.len())
            .field("max_messages", &self.max_messages)
            .finish_non_exhaustive()
    }
}

/// Fakeable boundary used by the scope runtime after access policy succeeds.
pub trait QuoteResolver: Send + Sync {
    /// Resolves exactly the requested direct parent into a safe snapshot draft.
    fn resolve(&self, request: QuoteRequest) -> BoxFuture<'static, QuoteDraft>;

    /// Best-effort display name. Tests and fakes return `None`.
    fn display_name(&self, _open_id: &str) -> BoxFuture<'static, Option<String>> {
        async { None }.boxed()
    }

    /// First-engagement topic history. Default is empty so unit fakes stay small.
    fn topic_context(&self, _request: TopicContextRequest) -> BoxFuture<'static, Vec<QuoteDraft>> {
        async { Vec::new() }.boxed()
    }
}

/// Production resolver backed by the tenant-bound Lark `OpenAPI` client.
#[derive(Clone)]
pub struct LarkQuoteResolver {
    api: LarkApi,
    names: Arc<Mutex<HashMap<String, Option<String>>>>,
}

impl LarkQuoteResolver {
    /// Creates the tenant-bound one-hop resolver.
    ///
    /// `policy` is accepted for call-site stability. Quoted parents are
    /// conversation context for an already-authorized trigger, so the
    /// resolver does not re-apply sender or group allowlists.
    #[must_use]
    pub fn new(api: LarkApi, _policy: AccessPolicy) -> Self {
        Self {
            api,
            names: Arc::new(Mutex::new(HashMap::new())),
        }
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
        let names = Arc::clone(&self.names);
        async move {
            let parent_id = request.parent_message_id;
            let items = match api.get_message_items(&parent_id).await {
                Ok(items) if !items.is_empty() => items,
                Ok(_) => return degraded(parent_id, QuoteStatus::Unavailable),
                Err(error) => return degraded(parent_id, status_for_error(&error)),
            };
            let raw = items[0].clone();
            if raw.message_id != parent_id || raw.chat_id != request.chat_id {
                return degraded(parent_id, QuoteStatus::Unauthorized);
            }
            if raw.deleted {
                return degraded(parent_id, QuoteStatus::Deleted);
            }
            if resolve_parent_chat_mode(&raw, request.chat_mode).is_none() {
                return degraded(parent_id, QuoteStatus::Unauthorized);
            }
            let mut draft = match draft_from_raw(&raw) {
                Some(draft) => draft,
                None => return degraded(parent_id, QuoteStatus::Unavailable),
            };
            expand_forwards(&api, &names, &mut draft, &items, 0).await;
            draft.sender_name = lookup_name(&api, &names, draft.sender_id.as_deref()).await;
            draft
        }
        .boxed()
    }

    fn display_name(&self, open_id: &str) -> BoxFuture<'static, Option<String>> {
        let api = self.api.clone();
        let names = Arc::clone(&self.names);
        let open_id = open_id.to_owned();
        async move { lookup_name(&api, &names, Some(&open_id)).await }.boxed()
    }

    fn topic_context(&self, request: TopicContextRequest) -> BoxFuture<'static, Vec<QuoteDraft>> {
        let api = self.api.clone();
        let names = Arc::clone(&self.names);
        async move { fetch_topic_context(&api, &names, request).await }.boxed()
    }
}

/// Resolves the policy chat mode for a fetched parent.
///
/// Feishu's `GET /im/v1/messages/:message_id` item commonly omits `chat_type`.
/// In that case the already-authorized trigger chat mode is inherited. A
/// present wire value must stay in the same p2p/group family as the trigger.
fn resolve_parent_chat_mode(raw: &RawMessage, requested: ChatMode) -> Option<ChatMode> {
    match raw.chat_type.as_str() {
        "" => Some(requested),
        "p2p" if requested == ChatMode::P2p => Some(ChatMode::P2p),
        "group" if requested != ChatMode::P2p => Some(requested),
        _ => None,
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
        // `check_code` now classifies those business codes as protocol
        // violations; match the numeric code on both variants so the quote
        // contract survives that taxonomy change.
        LarkError::PermanentAuth { .. } => QuoteStatus::Unauthorized,
        LarkError::Retryable { code, .. } | LarkError::ProtocolViolation { code, .. } => match code
        {
            Some(230_002 | 230_006 | 230_027 | 230_050) => QuoteStatus::Unauthorized,
            Some(230_011 | 404) => QuoteStatus::Deleted,
            _ => QuoteStatus::Unavailable,
        },
        LarkError::InvalidRequest { .. } => QuoteStatus::Unavailable,
    }
}

fn degraded(message_id: String, status: QuoteStatus) -> QuoteDraft {
    QuoteDraft {
        message_id,
        message_type: None,
        sender_id: None,
        sender_name: None,
        status,
        parts: Vec::new(),
    }
}

fn draft_from_raw(raw: &RawMessage) -> Option<QuoteDraft> {
    if raw.deleted {
        return Some(degraded(raw.message_id.clone(), QuoteStatus::Deleted));
    }
    let sender_id = raw.sender_id.as_deref().and_then(|sender_id| {
        if sender_id.is_empty() || sender_id.len() > STORE_INBOUND_ID_MAX_BYTES {
            None
        } else {
            Some(sender_id.to_owned())
        }
    });
    if raw.message_type.is_empty() || raw.message_type.len() > STORE_INBOUND_MESSAGE_TYPE_MAX_BYTES
    {
        return Some(degraded_with_type(
            raw.message_id.clone(),
            raw.message_type.clone(),
            QuoteStatus::Oversize,
        ));
    }
    let Some(content) = raw.content.as_ref() else {
        return Some(degraded_with_type(
            raw.message_id.clone(),
            raw.message_type.clone(),
            QuoteStatus::Unavailable,
        ));
    };
    if content.len() > QUOTE_CONTENT_MAX_BYTES {
        return Some(degraded_with_type(
            raw.message_id.clone(),
            raw.message_type.clone(),
            QuoteStatus::Oversize,
        ));
    }
    let Ok(parts) = normalize_message_parts(&raw.message_type, content) else {
        return Some(degraded_with_type(
            raw.message_id.clone(),
            raw.message_type.clone(),
            QuoteStatus::Unavailable,
        ));
    };
    if parts.len() > QUOTE_MAX_PARTS || parts.iter().any(part_descriptor_oversize) {
        return Some(degraded_with_type(
            raw.message_id.clone(),
            raw.message_type.clone(),
            QuoteStatus::Oversize,
        ));
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
    Some(QuoteDraft {
        message_id: raw.message_id.clone(),
        message_type: Some(raw.message_type.clone()),
        sender_id,
        sender_name: None,
        status,
        parts: parts.iter().map(draft_part_from_inbound).collect(),
    })
}

async fn expand_forwards(
    api: &LarkApi,
    names: &Mutex<HashMap<String, Option<String>>>,
    draft: &mut QuoteDraft,
    already_fetched: &[RawMessage],
    depth: usize,
) {
    if depth >= FORWARD_EXPAND_MAX_DEPTH {
        return;
    }
    let mut expanded = Vec::new();
    for part in &draft.parts {
        match part {
            DraftPart::Forward {
                message_id: Some(message_id),
                ..
            } => {
                let children = if already_fetched.len() > 1
                    && already_fetched[0].message_id == draft.message_id
                {
                    already_fetched[1..].to_vec()
                } else {
                    api.get_message_items(message_id).await.unwrap_or_default()
                };
                if children.is_empty() {
                    expanded.push(DraftPart::Text("[forwarded messages]".to_owned()));
                    continue;
                }
                let mut lines = Vec::new();
                for child in children.iter().take(FORWARD_EXPAND_MAX_ITEMS) {
                    let Some(mut child_draft) = draft_from_raw(child) else {
                        continue;
                    };
                    Box::pin(expand_forwards(
                        api,
                        names,
                        &mut child_draft,
                        &[],
                        depth + 1,
                    ))
                    .await;
                    child_draft.sender_name =
                        lookup_name(api, names, child_draft.sender_id.as_deref()).await;
                    let sender = child_draft
                        .sender_name
                        .as_deref()
                        .or(child_draft.sender_id.as_deref())
                        .unwrap_or("unknown");
                    let body = flatten_draft_text(&child_draft);
                    if !body.is_empty() {
                        lines.push(format!("{sender}: {body}"));
                    }
                }
                if lines.is_empty() {
                    expanded.push(DraftPart::Text("[forwarded messages]".to_owned()));
                } else {
                    expanded.push(DraftPart::Text(format!(
                        "<forwarded_messages>\n{}\n</forwarded_messages>",
                        lines.join("\n")
                    )));
                }
            }
            DraftPart::Card(value) => {
                if let Some(text) = expand_card_text(value) {
                    expanded.push(DraftPart::Text(text));
                } else {
                    expanded.push(part.clone());
                }
            }
            other => expanded.push(other.clone()),
        }
    }
    draft.parts = expanded;
}

fn flatten_draft_text(draft: &QuoteDraft) -> String {
    let mut texts = Vec::new();
    for part in &draft.parts {
        match part {
            DraftPart::Text(text) if !text.is_empty() => texts.push(text.clone()),
            DraftPart::Media { kind, metadata, .. } => {
                let name = metadata.name.as_deref().unwrap_or("attachment");
                texts.push(format!("[{kind:?}: {name}]"));
            }
            DraftPart::Card(_) => texts.push("[interactive card]".to_owned()),
            DraftPart::Forward { .. } => texts.push("[forwarded messages]".to_owned()),
            DraftPart::Unsupported { message_type, .. } => {
                texts.push(format!("[unsupported:{message_type}]"));
            }
            DraftPart::Text(_) => {}
        }
    }
    texts.join("\n")
}

fn expand_card_text(value: &Value) -> Option<String> {
    let mut texts = Vec::new();
    collect_card_text(value, &mut texts, 4096);
    if texts.is_empty() {
        None
    } else {
        Some(texts.join("\n"))
    }
}

fn collect_card_text(value: &Value, texts: &mut Vec<String>, budget: usize) {
    let used: usize = texts.iter().map(String::len).sum();
    if used >= budget {
        return;
    }
    match value {
        Value::Object(map) => {
            let tag = map.get("tag").and_then(Value::as_str).unwrap_or("");
            if matches!(tag, "plain_text" | "lark_md" | "markdown") {
                if let Some(content) = map.get("content").and_then(Value::as_str) {
                    if !content.is_empty() {
                        texts.push(content.chars().take(budget.saturating_sub(used)).collect());
                    }
                }
            }
            for child in map.values() {
                collect_card_text(child, texts, budget);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_card_text(item, texts, budget);
            }
        }
        _ => {}
    }
}

async fn lookup_name(
    api: &LarkApi,
    cache: &Mutex<HashMap<String, Option<String>>>,
    open_id: Option<&str>,
) -> Option<String> {
    let open_id = open_id.filter(|id| !id.is_empty() && id.len() <= STORE_INBOUND_ID_MAX_BYTES)?;
    if let Some(cached) = cache
        .lock()
        .ok()
        .and_then(|guard| guard.get(open_id).cloned())
    {
        return cached;
    }
    let name = api.get_user_name(open_id).await;
    if let Ok(mut guard) = cache.lock() {
        if guard.len() < 256 {
            guard.insert(open_id.to_owned(), name.clone());
        }
    }
    name
}

async fn fetch_topic_context(
    api: &LarkApi,
    names: &Mutex<HashMap<String, Option<String>>>,
    request: TopicContextRequest,
) -> Vec<QuoteDraft> {
    let max = request.max_messages.min(TOPIC_CONTEXT_MAX_MESSAGES).max(1);
    let mut newest = Vec::new();
    let mut page_token = None;
    for _ in 0..4 {
        let page = match api
            .list_thread_messages(
                &request.thread_id,
                &request.chat_id,
                page_token.as_deref(),
                50,
            )
            .await
        {
            Ok(page) => page,
            Err(_) => break,
        };
        for raw in page.items {
            if raw.deleted
                || request.exclude_ids.contains(&raw.message_id)
                || raw.chat_id != request.chat_id
            {
                continue;
            }
            newest.push(raw);
            if newest.len() >= max {
                break;
            }
        }
        page_token = page.page_token;
        if page_token.is_none() || newest.len() >= max {
            break;
        }
    }
    newest.reverse();
    let mut drafts = Vec::new();
    for raw in newest {
        let Some(mut draft) = draft_from_raw(&raw) else {
            continue;
        };
        expand_forwards(api, names, &mut draft, &[], 0).await;
        draft.sender_name = lookup_name(api, names, draft.sender_id.as_deref()).await;
        drafts.push(draft);
    }
    drafts
}

fn degraded_with_type(message_id: String, message_type: String, status: QuoteStatus) -> QuoteDraft {
    QuoteDraft {
        message_id,
        message_type: Some(message_type),
        sender_id: None,
        sender_name: None,
        status,
        parts: Vec::<DraftPart>::new(),
    }
}
