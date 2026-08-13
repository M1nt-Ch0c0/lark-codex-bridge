//! Normalization of raw `im.message.receive_v1` event payloads into the
//! stable [`InboundEvent`] model consumed by the scope-runtime milestone.
//!
//! Scope rules (design §5.2): p2p and plain group messages scope to
//! `im:<chat_id>`; topic messages with a `thread_id` scope to
//! `im:<chat_id>:thread:<thread_id>`.
//!
//! The chat mode (`p2p`/`group`/`topic`) is not on the event, so a bounded
//! `chat_id → ChatMode` cache ([`LARK_CHAT_MODE_CACHE_CAPACITY`] entries,
//! [`LARK_CHAT_MODE_CACHE_TTL`] TTL, [`LARK_CHAT_MODE_CACHE_KEY_BYTES`] key
//! cap) avoids one `GET /im/v1/chats/{id}` per message, matching the
//! reference `ChatModeCache`: lookup failures fall back to `Group` without
//! poisoning the cache, and a message-level `thread_id` contradicting a
//! cached non-topic entry invalidates it and re-probes once. Topic-group
//! events missing `thread_id` are backfilled once via
//! [`LarkApi::get_message`] (the raw item keeps `thread_id` even when the
//! event dropped it, per the reference `thread-id.ts`); a failed or empty
//! backfill degrades to chat-level scope and records the reason in the
//! outcome's `degradation` field. Quoted messages keep only the event's own
//! `parent_id`/`root_id` linkage — no recursive history fetch in this
//! milestone.
//!
//! Redaction: `Debug` implementations and degradation records carry IDs,
//! types, and lengths only — never message text, mention names, or file
//! names.

use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;
use std::time::Instant;

use serde::Deserialize;

use super::api::{ChatMode, LarkApi, ResourceKind};
use super::error::{LarkError, LarkErrorKind};
use crate::limits::{
    LARK_CHAT_MODE_CACHE_CAPACITY, LARK_CHAT_MODE_CACHE_KEY_BYTES, LARK_CHAT_MODE_CACHE_TTL,
    LARK_MAX_EVENT_PAYLOAD_BYTES,
};

/// Stable inbound message model handed to the scope runtime.
///
/// `text` never appears in `Debug` output; only its length is reported.
#[derive(Clone, PartialEq, Eq)]
pub struct InboundEvent {
    /// `header.event_id` of the receive event.
    pub event_id: String,
    /// `message_id` (`om_…`) of the message.
    pub message_id: String,
    /// Owning `chat_id` (`oc_…`).
    pub chat_id: String,
    /// Sender `open_id` (`ou_…`).
    pub sender_id: String,
    /// Resolved conversation mode of the chat.
    pub chat_type: ChatMode,
    /// Topic `thread_id` (`omt_…`) when the message belongs to a thread.
    pub thread_id: Option<String>,
    /// Reply-chain root `message_id`, when the message is a reply.
    pub root_id: Option<String>,
    /// Event `parent_id` / quoted message linkage (single hop, no history
    /// walk).
    pub reply_to_message_id: Option<String>,
    /// Message text with every `<at …>…</at>` mention tag stripped. Empty
    /// for non-text message types.
    pub text: String,
    /// Whether the `mentions` array contains the bot's `open_id`.
    pub mentions_bot: bool,
    /// Whether the message mentions everyone (`<at user_id="all">`).
    pub mention_all: bool,
    /// Image/file descriptors (keys and kinds), never bytes.
    pub resources: Vec<ResourceDesc>,
    /// Raw wire `message_type`, kept as an open string so unknown types
    /// survive normalization.
    pub message_type: String,
    /// `create_time` of the message in milliseconds since the Unix epoch.
    pub create_time_ms: i64,
    /// Scope the scope-runtime routes this event to.
    pub scope: ScopeKey,
}

impl fmt::Debug for InboundEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InboundEvent")
            .field("event_id", &self.event_id)
            .field("message_id", &self.message_id)
            .field("chat_id", &self.chat_id)
            .field("chat_type", &self.chat_type)
            .field("thread_id", &self.thread_id)
            .field("root_id", &self.root_id)
            .field("reply_to_message_id", &self.reply_to_message_id)
            .field("text_len", &self.text.len())
            .field("mentions_bot", &self.mentions_bot)
            .field("mention_all", &self.mention_all)
            .field("resource_count", &self.resources.len())
            .field(
                "resource_key_bytes",
                &self
                    .resources
                    .iter()
                    .map(|resource| resource.key.len())
                    .sum::<usize>(),
            )
            .field("message_type", &self.message_type)
            .field("create_time_ms", &self.create_time_ms)
            .field("scope", &self.scope)
            .finish()
    }
}

/// Routing scope of one inbound event.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScopeKey {
    /// Whole-chat scope (`im:<chat_id>`).
    Chat(String),
    /// Topic-thread scope (`im:<chat_id>:thread:<thread_id>`).
    Thread(String, String),
}

impl fmt::Display for ScopeKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Chat(chat_id) => write!(formatter, "im:{chat_id}"),
            Self::Thread(chat_id, thread_id) => {
                write!(formatter, "im:{chat_id}:thread:{thread_id}")
            }
        }
    }
}

/// Descriptor of a message resource (image/file): key and kind, never the
/// bytes and never the user-chosen file name.
#[derive(Clone, PartialEq, Eq)]
pub struct ResourceDesc {
    /// Resource kind (`image`/`file`).
    pub kind: ResourceKind,
    /// Server-side resource key (`image_key`/`file_key`).
    pub key: String,
}

impl fmt::Debug for ResourceDesc {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResourceDesc")
            .field("kind", &self.kind)
            .field("key_len", &self.key.len())
            .finish()
    }
}

/// Conservative degradation applied while normalizing one event. Recorded as
/// structured data (never message content) so operators can see when topic
/// routing fell back to chat-level scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Degradation {
    /// Chat-mode lookup failed; the message was treated as a plain group
    /// message (the reference cache's conservative default).
    ChatModeLookupFailed,
    /// A topic-group event lacked `thread_id` and the single backfill fetch
    /// failed; scoped to the chat instead of the thread.
    ThreadBackfillFailed {
        /// Retry classification of the backfill error.
        kind: LarkErrorKind,
    },
    /// The backfill fetch succeeded but the raw item carried no `thread_id`
    /// either; scoped to the chat.
    ThreadBackfillMissing,
}

/// Result of normalizing one raw event payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NormalizeOutcome {
    /// The payload was a message event and normalized successfully. The
    /// event is boxed to keep the outcome small on every path.
    Event {
        /// The normalized event.
        event: Box<InboundEvent>,
        /// Conservative degradation applied along the way, if any.
        degradation: Option<Degradation>,
    },
    /// The payload was a well-formed event of a type this normalizer does
    /// not handle (not `im.message.receive_v1`).
    Ignored {
        /// Static reason the event was ignored.
        reason: &'static str,
    },
}

/// Normalizes raw event payloads into [`InboundEvent`]s.
///
/// Holds the bot `open_id` (mention detection), a [`LarkApi`] (chat-mode
/// resolution and one-shot thread backfill), and a bounded chat-mode cache.
/// The normalizer owns no unbounded state.
pub struct Normalizer {
    api: LarkApi,
    bot_open_id: String,
    chat_modes: Mutex<HashMap<String, ChatModeEntry>>,
}

struct ChatModeEntry {
    mode: ChatMode,
    fetched_at: Instant,
}

impl Normalizer {
    /// Creates a normalizer for one tenant/bot identity.
    #[must_use]
    pub fn new(api: LarkApi, bot_open_id: impl Into<String>) -> Self {
        Self {
            api,
            bot_open_id: bot_open_id.into(),
            chat_modes: Mutex::new(HashMap::new()),
        }
    }

    /// Normalizes one raw event payload using the current time for cache
    /// bookkeeping.
    ///
    /// # Errors
    ///
    /// Returns [`LarkError::Exhausted`] for oversize payloads and
    /// [`LarkError::ProtocolViolation`] for malformed or incomplete event
    /// envelopes.
    pub async fn normalize(&self, payload: &[u8]) -> Result<NormalizeOutcome, LarkError> {
        self.normalize_at(payload, Instant::now()).await
    }

    /// Normalizes one raw event payload, using `now` for cache TTL
    /// bookkeeping (test hook).
    ///
    /// # Errors
    ///
    /// Same contract as [`Normalizer::normalize`].
    pub async fn normalize_at(
        &self,
        payload: &[u8],
        now: Instant,
    ) -> Result<NormalizeOutcome, LarkError> {
        let Some(parsed) = self.parse_event(payload)? else {
            return Ok(NormalizeOutcome::Ignored {
                reason: "not an im.message.receive_v1 event",
            });
        };
        let (chat_mode, scope, degradation) = self.resolve_scope(&parsed, now).await;
        // The event's thread id mirrors the final scope, so a backfilled id
        // is visible on the event exactly like an event-carried one.
        let thread_id = match &scope {
            ScopeKey::Chat(_) => None,
            ScopeKey::Thread(_, thread_id) => Some(thread_id.clone()),
        };
        Ok(NormalizeOutcome::Event {
            event: Box::new(InboundEvent {
                event_id: parsed.event_id,
                message_id: parsed.message_id,
                chat_id: parsed.chat_id,
                sender_id: parsed.sender_open_id,
                chat_type: chat_mode,
                thread_id,
                root_id: parsed.root_id,
                reply_to_message_id: parsed.parent_id,
                text: parsed.text,
                mentions_bot: parsed.mentions_bot,
                mention_all: parsed.mention_all,
                resources: parsed.resources,
                message_type: parsed.message_type,
                create_time_ms: parsed.create_time_ms,
                scope,
            }),
            degradation,
        })
    }

    /// Number of entries currently held in the chat-mode cache (bounded by
    /// [`LARK_CHAT_MODE_CACHE_CAPACITY`]); exposed for tests and metrics.
    ///
    /// # Panics
    ///
    /// Panics if the cache lock is poisoned (a previous holder panicked).
    #[must_use]
    pub fn cached_chat_mode_count(&self) -> usize {
        self.chat_modes.lock().expect("chat mode cache lock").len()
    }

    /// Parses and validates the envelope; `Ok(None)` means a well-formed
    /// event of an unhandled type.
    fn parse_event(&self, payload: &[u8]) -> Result<Option<ParsedEvent>, LarkError> {
        if payload.len() > LARK_MAX_EVENT_PAYLOAD_BYTES {
            return Err(LarkError::exhausted(
                "inbound event payload exceeds the byte cap",
                LARK_MAX_EVENT_PAYLOAD_BYTES as u64,
            ));
        }
        let envelope: EventEnvelope = serde_json::from_slice(payload)
            .map_err(|_| LarkError::protocol("event payload is not valid JSON"))?;
        let header = envelope
            .header
            .ok_or_else(|| LarkError::protocol("event payload missing the header"))?;
        if header.event_type.as_deref() != Some("im.message.receive_v1") {
            return Ok(None);
        }
        let event_id = required(header.event_id, "event header missing event_id")?;
        let event = envelope
            .event
            .ok_or_else(|| LarkError::protocol("event payload missing the event object"))?;
        let sender_open_id = event
            .sender
            .and_then(|sender| sender.sender_id)
            .and_then(|id| id.open_id)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| LarkError::protocol("event sender missing open_id"))?;
        let message = event
            .message
            .ok_or_else(|| LarkError::protocol("event missing the message object"))?;

        let message_type = required(message.message_type, "message missing message_type")?;
        let content = required(message.content, "message missing content")?;
        let create_time_ms = required(message.create_time, "message missing create_time")?
            .parse::<i64>()
            .map_err(|_| LarkError::protocol("message create_time is not a millisecond integer"))?;
        let mentions = message.mentions.unwrap_or_default();
        let mentions_bot = mentions.iter().any(|mention| {
            mention.id.as_ref().and_then(|id| id.open_id.as_deref())
                == Some(self.bot_open_id.as_str())
        });
        let mentions_all_in_array = mentions
            .iter()
            .any(|mention| mention.id.as_ref().and_then(|id| id.user_id.as_deref()) == Some("all"));
        let (text, mentions_all_in_text) = extract_text(&message_type, &content)?;

        Ok(Some(ParsedEvent {
            event_id,
            message_id: required(message.message_id, "message missing message_id")?,
            chat_id: required(message.chat_id, "message missing chat_id")?,
            chat_type_wire: required(message.chat_type, "message missing chat_type")?,
            message_type: message_type.clone(),
            create_time_ms,
            sender_open_id,
            thread_id: non_empty(message.thread_id),
            root_id: non_empty(message.root_id),
            parent_id: non_empty(message.parent_id),
            text,
            mentions_bot,
            mention_all: mentions_all_in_array || mentions_all_in_text,
            resources: extract_resources(&message_type, &content)?,
        }))
    }

    /// Resolves the chat mode and routing scope, applying the bounded cache,
    /// the message-level `thread_id` override, and one-shot thread backfill.
    async fn resolve_scope(
        &self,
        parsed: &ParsedEvent,
        now: Instant,
    ) -> (ChatMode, ScopeKey, Option<Degradation>) {
        let mut degradation = None;
        let mut chat_mode = if parsed.chat_type_wire == "p2p" {
            ChatMode::P2p
        } else {
            let (mode, mode_degradation) = self.resolve_chat_mode(&parsed.chat_id, now).await;
            degradation = mode_degradation;
            mode
        };

        let scope = if chat_mode == ChatMode::P2p {
            ScopeKey::Chat(parsed.chat_id.clone())
        } else if let Some(thread_id) = parsed.thread_id.clone() {
            // A message-level thread_id is authoritative (reference
            // ChatModeCache): if the resolved mode contradicts it, drop the
            // stale entry and re-probe once so the cache converges.
            if chat_mode != ChatMode::Topic {
                self.invalidate_chat_mode(&parsed.chat_id);
                let (reprobed, reprobe_degradation) =
                    self.resolve_chat_mode(&parsed.chat_id, now).await;
                chat_mode = reprobed;
                if degradation.is_none() {
                    degradation = reprobe_degradation;
                }
            }
            ScopeKey::Thread(parsed.chat_id.clone(), thread_id)
        } else if chat_mode == ChatMode::Topic {
            // Topic-group event without a thread_id: backfill once via the
            // raw message item, which keeps thread_id even when the event
            // dropped it (reference thread-id.ts).
            match self.api.get_message(&parsed.message_id).await {
                Ok(raw) => {
                    if let Some(backfilled) = non_empty(raw.thread_id) {
                        ScopeKey::Thread(parsed.chat_id.clone(), backfilled)
                    } else {
                        degradation = Some(Degradation::ThreadBackfillMissing);
                        ScopeKey::Chat(parsed.chat_id.clone())
                    }
                }
                Err(error) => {
                    degradation = Some(Degradation::ThreadBackfillFailed { kind: error.kind() });
                    ScopeKey::Chat(parsed.chat_id.clone())
                }
            }
        } else {
            ScopeKey::Chat(parsed.chat_id.clone())
        };
        (chat_mode, scope, degradation)
    }

    /// Resolves the chat mode, serving from the bounded cache when fresh and
    /// falling back to `Group` (uncached) on lookup failure.
    async fn resolve_chat_mode(
        &self,
        chat_id: &str,
        now: Instant,
    ) -> (ChatMode, Option<Degradation>) {
        if let Some(mode) = self.cached_chat_mode(chat_id, now) {
            return (mode, None);
        }
        match self.api.get_chat_mode(chat_id).await {
            Ok(mode) => {
                self.store_chat_mode(chat_id, mode, now);
                (mode, None)
            }
            Err(_) => (ChatMode::Group, Some(Degradation::ChatModeLookupFailed)),
        }
    }

    fn cached_chat_mode(&self, chat_id: &str, now: Instant) -> Option<ChatMode> {
        let cache = self.chat_modes.lock().expect("chat mode cache lock");
        cache.get(chat_id).and_then(|entry| {
            if now.duration_since(entry.fetched_at) < LARK_CHAT_MODE_CACHE_TTL {
                Some(entry.mode)
            } else {
                None
            }
        })
    }

    fn store_chat_mode(&self, chat_id: &str, mode: ChatMode, now: Instant) {
        if chat_id.len() > LARK_CHAT_MODE_CACHE_KEY_BYTES {
            return;
        }
        let mut cache = self.chat_modes.lock().expect("chat mode cache lock");
        cache.retain(|_, entry| now.duration_since(entry.fetched_at) < LARK_CHAT_MODE_CACHE_TTL);
        if !cache.contains_key(chat_id) && cache.len() >= LARK_CHAT_MODE_CACHE_CAPACITY {
            // Bounded overflow: evict the oldest entry.
            if let Some(oldest) = cache
                .iter()
                .max_by_key(|(_, entry)| now.duration_since(entry.fetched_at))
                .map(|(key, _)| key.clone())
            {
                cache.remove(&oldest);
            }
        }
        cache.insert(
            chat_id.to_owned(),
            ChatModeEntry {
                mode,
                fetched_at: now,
            },
        );
    }

    fn invalidate_chat_mode(&self, chat_id: &str) {
        self.chat_modes
            .lock()
            .expect("chat mode cache lock")
            .remove(chat_id);
    }
}

impl fmt::Debug for Normalizer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Normalizer")
            .field("bot_open_id_len", &self.bot_open_id.len())
            .field("cached_chat_modes", &self.cached_chat_mode_count())
            .finish_non_exhaustive()
    }
}

/// Fully parsed, validated event fields ready for scope resolution.
struct ParsedEvent {
    event_id: String,
    message_id: String,
    chat_id: String,
    chat_type_wire: String,
    message_type: String,
    create_time_ms: i64,
    sender_open_id: String,
    thread_id: Option<String>,
    root_id: Option<String>,
    parent_id: Option<String>,
    text: String,
    mentions_bot: bool,
    mention_all: bool,
    resources: Vec<ResourceDesc>,
}

fn required(value: Option<String>, context: &'static str) -> Result<String, LarkError> {
    value
        .filter(|value| !value.is_empty())
        .ok_or_else(|| LarkError::protocol(context))
}

fn non_empty(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.is_empty())
}

#[derive(Deserialize)]
struct TextContent {
    text: Option<String>,
}

#[derive(Deserialize)]
struct ImageContent {
    image_key: Option<String>,
}

#[derive(Deserialize)]
struct FileContent {
    file_key: Option<String>,
}

/// Extracts the text body of a `text` message with every `<at …>…</at>`
/// mention tag stripped; also reports whether the raw text mentioned
/// `@all`. Non-text types yield an empty string.
fn extract_text(message_type: &str, content: &str) -> Result<(String, bool), LarkError> {
    if message_type != "text" {
        return Ok((String::new(), false));
    }
    let parsed: TextContent = serde_json::from_str(content)
        .map_err(|_| LarkError::protocol("text message content is not valid JSON"))?;
    let raw = parsed.text.unwrap_or_default();
    let mentions_all = raw.contains("<at user_id=\"all\">");
    Ok((strip_mention_tags(&raw), mentions_all))
}

/// Removes every `<at …>…</at>` span and trims surrounding whitespace.
/// Unterminated tags are left untouched (fail-open on display, never on
/// routing).
fn strip_mention_tags(text: &str) -> String {
    let mut stripped = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find("<at ") {
        stripped.push_str(&rest[..start]);
        if let Some(end) = rest[start..].find("</at>") {
            rest = &rest[start + end + "</at>".len()..];
        } else {
            stripped.push_str(&rest[start..]);
            rest = "";
            break;
        }
    }
    stripped.push_str(rest);
    stripped.trim().to_owned()
}

/// Extracts image/file descriptors from known resource message types.
/// Unknown types keep no descriptors but survive via the open
/// `message_type` string.
fn extract_resources(message_type: &str, content: &str) -> Result<Vec<ResourceDesc>, LarkError> {
    match message_type {
        "image" => {
            let parsed: ImageContent = serde_json::from_str(content)
                .map_err(|_| LarkError::protocol("image message content is not valid JSON"))?;
            Ok(resource_desc(parsed.image_key, ResourceKind::Image))
        }
        "file" => {
            let parsed: FileContent = serde_json::from_str(content)
                .map_err(|_| LarkError::protocol("file message content is not valid JSON"))?;
            Ok(resource_desc(parsed.file_key, ResourceKind::File))
        }
        _ => Ok(Vec::new()),
    }
}

fn resource_desc(key: Option<String>, kind: ResourceKind) -> Vec<ResourceDesc> {
    key.filter(|key| !key.is_empty())
        .map(|key| vec![ResourceDesc { kind, key }])
        .unwrap_or_default()
}

#[derive(Deserialize)]
struct EventEnvelope {
    header: Option<EventHeader>,
    event: Option<EventBody>,
}

#[derive(Deserialize)]
struct EventHeader {
    event_id: Option<String>,
    event_type: Option<String>,
}

#[derive(Deserialize)]
struct EventBody {
    sender: Option<EventSender>,
    message: Option<EventMessage>,
}

#[derive(Deserialize)]
struct EventSender {
    sender_id: Option<EventSenderId>,
}

#[derive(Deserialize)]
struct EventSenderId {
    open_id: Option<String>,
}

#[derive(Deserialize)]
struct EventMessage {
    message_id: Option<String>,
    chat_id: Option<String>,
    chat_type: Option<String>,
    message_type: Option<String>,
    content: Option<String>,
    create_time: Option<String>,
    root_id: Option<String>,
    parent_id: Option<String>,
    thread_id: Option<String>,
    mentions: Option<Vec<EventMention>>,
}

#[derive(Deserialize)]
struct EventMention {
    id: Option<EventMentionId>,
}

#[derive(Deserialize)]
struct EventMentionId {
    open_id: Option<String>,
    user_id: Option<String>,
}
