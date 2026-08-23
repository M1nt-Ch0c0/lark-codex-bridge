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

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::api::{ChatMode, LarkApi, ResourceKind};
use super::error::{LarkError, LarkErrorKind};
use crate::limits::{
    ATTACHMENT_FILE_NAME_MAX_BYTES, ATTACHMENT_MIME_MAX_BYTES, LARK_CHAT_MODE_CACHE_CAPACITY,
    LARK_CHAT_MODE_CACHE_KEY_BYTES, LARK_CHAT_MODE_CACHE_TTL, LARK_MAX_EVENT_PAYLOAD_BYTES,
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
    /// Whether the wire sender is an ordinary human user (`sender_type` is
    /// `"user"`). Bot/app/system/anonymous senders are never eligible for
    /// sender/group allowlists.
    pub sender_is_human: bool,
    /// Structured mention identities from the event. Display names and IDs
    /// are retained for an authorized context resolver but never printed by
    /// `Debug`.
    pub mentions: Vec<MentionIdentity>,
    /// Typed message content. Every known rich-message kind is represented,
    /// including an explicit unsupported/unavailable status when its content
    /// cannot be exposed safely.
    pub parts: Vec<MessagePart>,
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
            .field("event_id", &ShortId(&self.event_id))
            .field("message_id", &ShortId(&self.message_id))
            .field("chat_id", &ShortId(&self.chat_id))
            .field("sender_id_len", &self.sender_id.len())
            .field("chat_type", &self.chat_type)
            .field("thread_id", &self.thread_id.as_deref().map(ShortId))
            .field("root_id", &self.root_id.as_deref().map(ShortId))
            .field(
                "reply_to_message_id",
                &self.reply_to_message_id.as_deref().map(ShortId),
            )
            .field("text_len", &self.text.len())
            .field("mentions_bot", &self.mentions_bot)
            .field("mention_all", &self.mention_all)
            .field("sender_is_human", &self.sender_is_human)
            .field("mention_count", &self.mentions.len())
            .field("part_count", &self.parts.len())
            .field("resource_count", &self.resources.len())
            .field(
                "resource_key_bytes",
                &self
                    .resources
                    .iter()
                    .map(|resource| resource.key.len())
                    .sum::<usize>(),
            )
            .field("message_type_len", &self.message_type.len())
            .field("create_time_ms", &self.create_time_ms)
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

/// One identity from the message's `mentions` array.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MentionIdentity {
    /// Placeholder key used by some Feishu message bodies (for example
    /// `@_user_1`).
    pub key: Option<String>,
    /// Mentioned user's tenant-local `open_id`, when supplied.
    pub open_id: Option<String>,
    /// Mentioned user's `user_id`, including the sentinel `all`.
    pub user_id: Option<String>,
    /// Mentioned user's cross-app `union_id`, when supplied.
    pub union_id: Option<String>,
    /// Display name supplied by Feishu, when present.
    pub name: Option<String>,
}

impl fmt::Debug for MentionIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MentionIdentity")
            .field("key_len", &self.key.as_deref().map_or(0, str::len))
            .field("open_id_len", &self.open_id.as_deref().map_or(0, str::len))
            .field("user_id_len", &self.user_id.as_deref().map_or(0, str::len))
            .field(
                "union_id_len",
                &self.union_id.as_deref().map_or(0, str::len),
            )
            .field("name_len", &self.name.as_deref().map_or(0, str::len))
            .finish_non_exhaustive()
    }
}

/// Whether a typed part can be resolved by the bridge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartStatus {
    /// The descriptor contains enough information for the bridge to expose or
    /// retrieve the part.
    Available,
    /// The message kind is recognized but deliberately not exposed yet.
    Unsupported,
    /// The kind is supported, but this event omitted the required handle.
    Unavailable,
}

/// Safe metadata accompanying a media descriptor. String values are bounded
/// and validated before retention and are redacted from `Debug`.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaMetadata {
    /// User-facing file name, only when it is a safe basename.
    pub file_name: Option<String>,
    /// MIME type, only when it contains bounded printable ASCII.
    pub mime_type: Option<String>,
    /// Resource size in bytes, when supplied as a non-negative integer.
    pub size_bytes: Option<u64>,
    /// Media duration in milliseconds, when supplied as a non-negative
    /// integer.
    pub duration_ms: Option<u64>,
}

impl fmt::Debug for MediaMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MediaMetadata")
            .field(
                "file_name_len",
                &self.file_name.as_deref().map_or(0, str::len),
            )
            .field(
                "mime_type_len",
                &self.mime_type.as_deref().map_or(0, str::len),
            )
            .field("size_bytes", &self.size_bytes)
            .field("duration_ms", &self.duration_ms)
            .finish_non_exhaustive()
    }
}

/// Descriptor shared by image, file, sticker, audio, and video parts.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MediaPart {
    /// Server-side resource handle. Images use `image_key`; all other media
    /// use `file_key`.
    pub key: Option<String>,
    /// Optional video thumbnail `image_key`.
    pub thumbnail_key: Option<String>,
    /// Safe metadata supplied by the event.
    pub metadata: MediaMetadata,
    /// Availability of the part.
    pub status: PartStatus,
}

impl fmt::Debug for MediaPart {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MediaPart")
            .field("key_len", &self.key.as_deref().map_or(0, str::len))
            .field(
                "thumbnail_key_len",
                &self.thumbnail_key.as_deref().map_or(0, str::len),
            )
            .field("metadata", &self.metadata)
            .field("status", &self.status)
            .finish_non_exhaustive()
    }
}

/// Typed content of one inbound message.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum MessagePart {
    /// Plain text after mention tags were stripped.
    Text { text: String },
    /// Image descriptor.
    Image(MediaPart),
    /// File descriptor.
    File(MediaPart),
    /// Sticker descriptor.
    Sticker(MediaPart),
    /// Audio descriptor.
    Audio(MediaPart),
    /// Video descriptor (`media` on current Feishu wire payloads).
    Video(MediaPart),
    /// Merge-forward or forward message reference.
    Forward {
        message_id: Option<String>,
        status: PartStatus,
    },
    /// Interactive/card content. Raw card JSON is not retained.
    Card { status: PartStatus },
    /// An open wire type unknown to this bridge version.
    Unsupported {
        message_type: String,
        status: PartStatus,
    },
}

impl fmt::Debug for MessagePart {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text { text } => formatter
                .debug_struct("Text")
                .field("text_len", &text.len())
                .finish(),
            Self::Image(media) => formatter.debug_tuple("Image").field(media).finish(),
            Self::File(media) => formatter.debug_tuple("File").field(media).finish(),
            Self::Sticker(media) => formatter.debug_tuple("Sticker").field(media).finish(),
            Self::Audio(media) => formatter.debug_tuple("Audio").field(media).finish(),
            Self::Video(media) => formatter.debug_tuple("Video").field(media).finish(),
            Self::Forward { message_id, status } => formatter
                .debug_struct("Forward")
                .field("message_id_len", &message_id.as_deref().map_or(0, str::len))
                .field("status", status)
                .finish(),
            Self::Card { status } => formatter
                .debug_struct("Card")
                .field("status", status)
                .finish(),
            Self::Unsupported {
                message_type,
                status,
            } => formatter
                .debug_struct("Unsupported")
                .field("message_type_len", &message_type.len())
                .field("status", status)
                .finish(),
        }
    }
}

/// Routing scope of one inbound event.
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum ScopeKey {
    /// Whole-chat scope (`im:<chat_id>`).
    Chat(String),
    /// Topic-thread scope (`im:<chat_id>:thread:<thread_id>`).
    Thread(String, String),
}

impl fmt::Debug for ScopeKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Chat(chat_id) => formatter
                .debug_struct("Chat")
                .field("chat_id", &ShortId(chat_id))
                .finish(),
            Self::Thread(chat_id, thread_id) => formatter
                .debug_struct("Thread")
                .field("chat_id", &ShortId(chat_id))
                .field("thread_id", &ShortId(thread_id))
                .finish(),
        }
    }
}

pub(crate) struct ShortId<'a>(pub(crate) &'a str);

impl fmt::Debug for ShortId<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let digest = Sha256::digest(self.0.as_bytes());
        write!(
            formatter,
            "id(len={},sha256={:02x}{:02x}{:02x}{:02x})",
            self.0.len(),
            digest[0],
            digest[1],
            digest[2],
            digest[3]
        )
    }
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
                sender_is_human: parsed.sender_is_human,
                mentions: parsed.mentions,
                parts: parsed.parts,
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
        // Fail closed: only the explicit `"user"` sender type is a human.
        // Bot/app/system/anonymous senders (or a missing type) are never
        // eligible for sender/group allowlists.
        let sender_is_human = event
            .sender
            .as_ref()
            .and_then(|sender| sender.sender_type.as_deref())
            == Some("user");
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
        let raw_mentions = message.mentions.unwrap_or_default();
        let mentions_bot = raw_mentions.iter().any(|mention| {
            mention.id.as_ref().and_then(|id| id.open_id.as_deref())
                == Some(self.bot_open_id.as_str())
        });
        let mentions_all_in_array = raw_mentions
            .iter()
            .any(|mention| mention.id.as_ref().and_then(|id| id.user_id.as_deref()) == Some("all"));
        let mentions = raw_mentions.into_iter().map(mention_identity).collect();
        let extracted = extract_message_content(&message_type, &content)?;

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
            text: extracted.text,
            mentions_bot,
            mention_all: mentions_all_in_array || extracted.mentions_all,
            sender_is_human,
            mentions,
            parts: extracted.parts,
            resources: extracted.resources,
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
    sender_is_human: bool,
    mentions: Vec<MentionIdentity>,
    parts: Vec<MessagePart>,
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

struct ExtractedContent {
    text: String,
    mentions_all: bool,
    resources: Vec<ResourceDesc>,
    parts: Vec<MessagePart>,
}

/// Extracts legacy text/resource fields and the richer typed representation
/// in one parse. Unknown wire kinds survive as an explicit unsupported part;
/// their opaque content is never retained.
fn extract_message_content(
    message_type: &str,
    content: &str,
) -> Result<ExtractedContent, LarkError> {
    let known = matches!(
        message_type,
        "text"
            | "image"
            | "file"
            | "sticker"
            | "audio"
            | "video"
            | "media"
            | "interactive"
            | "card"
            | "merge_forward"
            | "forward"
    );
    if !known {
        return Ok(ExtractedContent {
            text: String::new(),
            mentions_all: false,
            resources: Vec::new(),
            parts: vec![MessagePart::Unsupported {
                message_type: message_type.to_owned(),
                status: PartStatus::Unsupported,
            }],
        });
    }
    let value: Value = serde_json::from_str(content)
        .map_err(|_| LarkError::protocol("message content is not valid JSON"))?;
    match message_type {
        "text" => {
            let raw = value
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let mentions_all = raw.contains("<at user_id=\"all\">");
            let text = strip_mention_tags(raw);
            Ok(ExtractedContent {
                parts: vec![MessagePart::Text { text: text.clone() }],
                text,
                mentions_all,
                resources: Vec::new(),
            })
        }
        "image" => {
            let key = content_string(&value, "image_key");
            let media = media_part(key.clone(), None, &value);
            Ok(ExtractedContent {
                text: String::new(),
                mentions_all: false,
                resources: resource_desc(key, ResourceKind::Image),
                parts: vec![MessagePart::Image(media)],
            })
        }
        "file" => {
            let key = content_string(&value, "file_key");
            let media = media_part(key.clone(), None, &value);
            Ok(ExtractedContent {
                text: String::new(),
                mentions_all: false,
                resources: resource_desc(key, ResourceKind::File),
                parts: vec![MessagePart::File(media)],
            })
        }
        "sticker" => Ok(rich_media_content(&value, MessagePart::Sticker)),
        "audio" => Ok(rich_media_content(&value, MessagePart::Audio)),
        "video" | "media" => {
            let key = content_string(&value, "file_key");
            let thumbnail_key = content_string(&value, "image_key");
            Ok(ExtractedContent {
                text: String::new(),
                mentions_all: false,
                resources: Vec::new(),
                parts: vec![MessagePart::Video(media_part(key, thumbnail_key, &value))],
            })
        }
        "interactive" | "card" => Ok(ExtractedContent {
            text: String::new(),
            mentions_all: false,
            resources: Vec::new(),
            parts: vec![MessagePart::Card {
                status: PartStatus::Unsupported,
            }],
        }),
        "merge_forward" | "forward" => {
            let message_id = content_string(&value, "message_id");
            let status = if message_id.is_some() {
                PartStatus::Available
            } else {
                PartStatus::Unavailable
            };
            Ok(ExtractedContent {
                text: String::new(),
                mentions_all: false,
                resources: Vec::new(),
                parts: vec![MessagePart::Forward { message_id, status }],
            })
        }
        _ => unreachable!("known message type handled above"),
    }
}

fn rich_media_content(value: &Value, wrap: fn(MediaPart) -> MessagePart) -> ExtractedContent {
    let key = content_string(value, "file_key");
    ExtractedContent {
        text: String::new(),
        mentions_all: false,
        resources: Vec::new(),
        parts: vec![wrap(media_part(key, None, value))],
    }
}

fn media_part(key: Option<String>, thumbnail_key: Option<String>, value: &Value) -> MediaPart {
    let status = if key.is_some() {
        PartStatus::Available
    } else {
        PartStatus::Unavailable
    };
    MediaPart {
        key,
        thumbnail_key,
        metadata: MediaMetadata {
            file_name: content_string(value, "file_name")
                .or_else(|| content_string(value, "name"))
                .filter(|name| safe_file_name(name)),
            mime_type: content_string(value, "mime_type")
                .or_else(|| content_string(value, "mime"))
                .filter(|mime| safe_mime(mime)),
            size_bytes: content_u64(value, &["file_size", "size"]),
            duration_ms: content_u64(value, &["duration_ms", "duration"]),
        },
        status,
    }
}

fn content_string(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn content_u64(value: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter().find_map(|key| {
        value.get(*key).and_then(|value| {
            value
                .as_u64()
                .or_else(|| value.as_str().and_then(|value| value.parse::<u64>().ok()))
        })
    })
}

fn safe_file_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= ATTACHMENT_FILE_NAME_MAX_BYTES
        && !matches!(name, "." | "..")
        && !name
            .bytes()
            .any(|byte| byte.is_ascii_control() || matches!(byte, b'/' | b'\\'))
}

fn safe_mime(mime: &str) -> bool {
    !mime.is_empty()
        && mime.len() <= ATTACHMENT_MIME_MAX_BYTES
        && mime.bytes().all(|byte| (0x20..=0x7e).contains(&byte))
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
    sender_type: Option<String>,
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
    key: Option<String>,
    id: Option<EventMentionId>,
    name: Option<String>,
}

#[derive(Default, Deserialize)]
#[allow(
    clippy::struct_field_names,
    reason = "the field names mirror the external event schema"
)]
struct EventMentionId {
    open_id: Option<String>,
    user_id: Option<String>,
    union_id: Option<String>,
}

fn mention_identity(mention: EventMention) -> MentionIdentity {
    let id = mention.id.unwrap_or_default();
    MentionIdentity {
        key: non_empty(mention.key),
        open_id: non_empty(id.open_id),
        user_id: non_empty(id.user_id),
        union_id: non_empty(id.union_id),
        name: non_empty(mention.name),
    }
}
