//! Bounded, in-memory capabilities for resolving one inbound Lark message.
//!
//! Context and media IDs are bearer capabilities.  Neither ID contains a Lark
//! resource key, and both are useful only while the owning Codex turn is
//! active.  Downloading is deliberately outside this module: callers pass an
//! [`AuthorizedResource`] to the attachment cache after authorization.

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::lark::{
    api::{ChatMode, ResourceKind},
    normalize::{
        InboundEvent, LiveTranscriptHandoff, MediaMetadata as InboundMediaMetadata, MediaPart,
        MessagePart, PartStatus, ResourceDesc, TranscriptFailure,
    },
};
use crate::limits::{
    ATTACHMENT_MAX_BYTES, ATTACHMENT_MAX_PER_MESSAGE, ATTACHMENT_TURN_TOTAL_BYTES,
};

const DEFAULT_CONTEXT_TTL: Duration = Duration::from_secs(10 * 60);
const DEFAULT_MAX_CONTEXTS: usize = 4_096;
const DEFAULT_MAX_PARTS: usize = 128;

/// Limits applied by [`ContextRegistry`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContextRegistryConfig {
    /// Lifetime of a capability, including time spent pending activation.
    pub ttl: Duration,
    /// Maximum number of live and retained revoked contexts.
    pub max_contexts: usize,
    /// Maximum number of typed parts accepted for a single message.
    pub max_parts_per_context: usize,
    /// Maximum attempted media reads charged to one turn.
    pub max_media_reads_per_turn: usize,
    /// Aggregate media bytes charged to one turn.
    pub max_media_read_bytes_per_turn: u64,
    /// Pessimistic reservation for a not-yet-materialized media handle.
    pub max_media_read_bytes_per_item: u64,
}

impl Default for ContextRegistryConfig {
    fn default() -> Self {
        Self {
            ttl: DEFAULT_CONTEXT_TTL,
            max_contexts: DEFAULT_MAX_CONTEXTS,
            max_parts_per_context: DEFAULT_MAX_PARTS,
            max_media_reads_per_turn: ATTACHMENT_MAX_PER_MESSAGE,
            max_media_read_bytes_per_turn: ATTACHMENT_TURN_TOTAL_BYTES,
            max_media_read_bytes_per_item: u64::try_from(ATTACHMENT_MAX_BYTES).unwrap_or(u64::MAX),
        }
    }
}

/// Opaque capability placed in the prompt's `bridge_context` object.
#[derive(Clone, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ContextId(String);

impl ContextId {
    /// Parses an untrusted context ID supplied by a tool caller.
    #[must_use]
    pub fn from_external(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the wire representation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ContextId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ContextId([REDACTED])")
    }
}

/// Opaque handle for one downloadable resource inside a context.
#[derive(Clone, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MediaHandle(String);

impl MediaHandle {
    /// Parses an untrusted media handle supplied by a tool caller.
    #[must_use]
    pub fn from_external(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the wire representation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for MediaHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MediaHandle([REDACTED])")
    }
}

/// Stable binding known before `turn/start` returns.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingBinding {
    /// Codex thread selected for the local turn.
    pub codex_thread_id: String,
    /// Primary key of the bridge's local turn row.
    pub local_turn_row_id: i64,
}

/// Full caller identity required after activation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveBinding {
    /// Codex thread making the tool request.
    pub codex_thread_id: String,
    /// Primary key of the bridge's local turn row.
    pub local_turn_row_id: i64,
    /// Codex turn making the tool request.
    pub codex_turn_id: String,
}

/// Lifecycle visible in registry metrics and registration results.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextLifecycle {
    /// Registered before `turn/start` has returned a Codex turn ID.
    Pending,
    /// Bound to a concrete Codex turn and usable by tools.
    Active,
    /// Permanently unavailable; its resource grants have been erased.
    Revoked,
}

/// Why an active capability was revoked.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RevocationReason {
    /// Turn reached a successful terminal state.
    Completed,
    /// Turn failed.
    Failed,
    /// Turn was cancelled or interrupted.
    Cancelled,
    /// Capability TTL elapsed.
    Expired,
    /// Context was superseded by another registration.
    Replaced,
    /// Bridge is shutting down.
    Shutdown,
}

/// Error code safe to expose in an MCP or dynamic-tool response.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextErrorCode {
    /// Malformed configuration or invalid lifecycle request.
    InvalidRequest,
    /// No such opaque capability exists.
    NotFound,
    /// Capability exists but is pending, expired, or revoked.
    Unavailable,
    /// Capability belongs to a different thread or turn.
    Forbidden,
    /// The requested message part has no supported resolver.
    Unsupported,
    /// The bounded registry has no safe eviction candidate.
    CapacityExceeded,
}

/// Structured context failure with no sensitive identifiers in its message.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextError {
    /// Machine-readable category.
    pub code: ContextErrorCode,
    /// Stable, operator-readable detail.
    pub message: &'static str,
    /// Whether retrying during the same turn can succeed.
    pub retryable: bool,
}

impl fmt::Display for ContextError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for ContextError {}

/// Normalized sender fields safe for model consumption.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SenderSnapshot {
    /// Sender's Lark `open_id`.
    pub open_id: String,
    /// Open wire sender type, normally `user`.
    pub sender_type: String,
}

/// Conversation kind, independent of the Lark transport enum.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChatKind {
    /// Direct chat.
    P2p,
    /// Ordinary group.
    Group,
    /// Topic group.
    Topic,
}

/// Chat fields attached to the inbound message.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatSnapshot {
    /// Lark chat ID.
    pub chat_id: String,
    /// Resolved chat kind.
    pub kind: ChatKind,
}

/// Kind of an explicit mention in the inbound message.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MentionKind {
    /// A user mention.
    User,
    /// The receiving bot was mentioned.
    Bot,
    /// `@all`.
    Everyone,
}

/// Structured mention. Fields absent from the normalized event remain `None`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MentionSnapshot {
    /// Mention category.
    pub kind: MentionKind,
    /// Mentioned user's `open_id`, if normalization retained it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub open_id: Option<String>,
    /// Display name, if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
}

/// Topic/reply-root metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadSnapshot {
    /// Topic `thread_id`, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    /// Reply-chain root message ID, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_message_id: Option<String>,
}

/// Stable outcome of resolving one directly quoted message.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QuoteStatus {
    /// The direct parent was fetched and normalized.
    Available,
    /// Lark reports that the direct parent was deleted or no longer exists.
    Deleted,
    /// The app is not authorized to read the direct parent.
    Unauthorized,
    /// The parent body or descriptor metadata exceeds a local bound.
    Oversize,
    /// The parent message kind is not supported by this bridge version.
    Unsupported,
    /// The parent could not be resolved at this time.
    Unavailable,
}

/// Immediate quoted/replied-to message snapshot.
#[derive(Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuoteSnapshot {
    /// Immediate parent message ID.
    pub message_id: String,
    /// Open Lark wire type, omitted when the parent could not be read.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_type: Option<String>,
    /// Stable resolution/degradation state.
    pub status: QuoteStatus,
    /// Sanitized parent parts. Resource keys are replaced with opaque handles.
    pub parts: Vec<TypedPart>,
}

impl fmt::Debug for QuoteSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuoteSnapshot")
            .field("message_id_len", &self.message_id.len())
            .field(
                "message_type_len",
                &self.message_type.as_deref().map_or(0, str::len),
            )
            .field("status", &self.status)
            .field("part_count", &self.parts.len())
            .finish_non_exhaustive()
    }
}

/// Input form of a quote before opaque resource handles are minted.
#[derive(Clone, PartialEq)]
pub struct QuoteDraft {
    /// Immediate parent message ID from the trusted receive event.
    pub message_id: String,
    /// Open Lark wire type, when known.
    pub message_type: Option<String>,
    /// Stable resolution/degradation state.
    pub status: QuoteStatus,
    /// Sanitized parent parts, containing resource descriptors only while the
    /// context is pending registration.
    pub parts: Vec<DraftPart>,
}

impl fmt::Debug for QuoteDraft {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuoteDraft")
            .field("message_id_len", &self.message_id.len())
            .field(
                "message_type_len",
                &self.message_type.as_deref().map_or(0, str::len),
            )
            .field("status", &self.status)
            .field("part_count", &self.parts.len())
            .finish_non_exhaustive()
    }
}

impl QuoteDraft {
    /// Builds the stable unresolved form used when no resolver is installed.
    #[must_use]
    pub fn unavailable(message_id: String) -> Self {
        Self {
            message_id,
            message_type: None,
            status: QuoteStatus::Unavailable,
            parts: Vec::new(),
        }
    }
}

/// Semantic media kind exposed to the model.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaKind {
    /// Still image.
    Image,
    /// Generic file.
    File,
    /// Sticker image.
    Sticker,
    /// Audio attachment.
    Audio,
    /// Video attachment.
    Video,
}

/// Optional, non-authoritative metadata for one media part.
#[derive(Clone, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaMetadata {
    /// Safe display name selected by the bridge.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Declared MIME type, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    /// Declared byte length, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// Duration for audio/video, if known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
}

impl fmt::Debug for MediaMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MediaMetadata")
            .field("name_len", &self.name.as_deref().map_or(0, str::len))
            .field(
                "mime_type_len",
                &self.mime_type.as_deref().map_or(0, str::len),
            )
            .field("size_bytes", &self.size_bytes)
            .field("duration_ms", &self.duration_ms)
            .finish_non_exhaustive()
    }
}

/// Typed part returned by context resolution. Resource keys never appear here.
#[derive(Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TypedPart {
    /// Plain normalized text.
    Text {
        /// Text content.
        text: String,
    },
    /// Downloadable media represented by an opaque handle.
    Media {
        /// Semantic type.
        kind: MediaKind,
        /// Turn-scoped opaque handle.
        handle: MediaHandle,
        /// Optional video-thumbnail handle, governed by the same capability.
        #[serde(skip_serializing_if = "Option::is_none")]
        thumbnail_handle: Option<MediaHandle>,
        /// Optional metadata.
        #[serde(flatten)]
        metadata: MediaMetadata,
    },
    /// Structured interactive card content.
    Card {
        /// Sanitized card JSON.
        content: Value,
    },
    /// Merge-forward reference retained without recursively fetching history.
    Forward {
        /// Referenced message ID, when supplied by Lark.
        #[serde(skip_serializing_if = "Option::is_none")]
        message_id: Option<String>,
        /// Whether the reference is usable by this bridge version.
        status: PartAvailability,
    },
    /// A preserved part for which no resolver is implemented.
    Unsupported {
        /// Open Lark wire type.
        message_type: String,
        /// Stable explanation suitable for a tool result.
        reason: String,
    },
}

impl fmt::Debug for TypedPart {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text { text } => formatter
                .debug_struct("Text")
                .field("text_len", &text.len())
                .finish(),
            Self::Media {
                kind,
                handle,
                thumbnail_handle,
                metadata,
            } => formatter
                .debug_struct("Media")
                .field("kind", kind)
                .field("handle", handle)
                .field("thumbnail_handle", thumbnail_handle)
                .field("metadata", metadata)
                .finish(),
            Self::Card { .. } => formatter.write_str("Card([REDACTED])"),
            Self::Forward { message_id, status } => formatter
                .debug_struct("Forward")
                .field("message_id_len", &message_id.as_deref().map_or(0, str::len))
                .field("status", status)
                .finish(),
            Self::Unsupported {
                message_type,
                reason,
            } => formatter
                .debug_struct("Unsupported")
                .field("message_type_len", &message_type.len())
                .field("reason", reason)
                .finish(),
        }
    }
}

/// Input part accepted during registration, before opaque handles are minted.
#[derive(Clone, PartialEq)]
pub enum DraftPart {
    /// Plain normalized text.
    Text(String),
    /// Downloadable media. `resource.key` is retained only inside the registry.
    Media {
        /// Semantic type.
        kind: MediaKind,
        /// Lark descriptor authorized after binding checks.
        resource: ResourceDesc,
        /// Optional video thumbnail descriptor.
        thumbnail: Option<ResourceDesc>,
        /// Non-authoritative display metadata.
        metadata: MediaMetadata,
        /// Non-content rejection classification for a supplied transcript.
        transcript_failure: Option<TranscriptFailure>,
    },
    /// Structured interactive card content.
    Card(Value),
    /// Merge-forward reference.
    Forward {
        /// Referenced message ID, when supplied by Lark.
        message_id: Option<String>,
        /// Availability reported by normalization.
        status: PartAvailability,
    },
    /// Explicit unsupported part retained in the snapshot.
    Unsupported {
        /// Open Lark wire type.
        message_type: String,
        /// Stable explanation.
        reason: String,
    },
}

impl fmt::Debug for DraftPart {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text(text) => formatter
                .debug_struct("Text")
                .field("chars", &text.chars().count())
                .finish_non_exhaustive(),
            Self::Media {
                kind,
                thumbnail,
                metadata,
                transcript_failure,
                ..
            } => formatter
                .debug_struct("Media")
                .field("kind", kind)
                .field("has_thumbnail", &thumbnail.is_some())
                .field("metadata", metadata)
                .field("transcript_failure", transcript_failure)
                .finish_non_exhaustive(),
            Self::Card(_) => formatter.write_str("Card([REDACTED])"),
            Self::Forward { status, .. } => formatter
                .debug_struct("Forward")
                .field("status", status)
                .finish_non_exhaustive(),
            Self::Unsupported { message_type, .. } => formatter
                .debug_struct("Unsupported")
                .field("message_type_len", &message_type.len())
                .finish_non_exhaustive(),
        }
    }
}

/// Availability of a typed part that does not carry a downloadable handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PartAvailability {
    /// Part contains the required descriptor.
    Available,
    /// Part kind is recognized but deliberately unsupported.
    Unsupported,
    /// Part should be supported, but required wire data was absent.
    Unavailable,
}

/// Complete immutable message draft registered before `turn/start`.
#[derive(Clone, Debug, PartialEq)]
pub struct ContextDraft {
    /// Lark receive-event ID.
    pub event_id: String,
    /// Lark message ID.
    pub message_id: String,
    /// Sender metadata.
    pub sender: SenderSnapshot,
    /// Chat metadata.
    pub chat: ChatSnapshot,
    /// Explicit mentions.
    pub mentions: Vec<MentionSnapshot>,
    /// Topic and reply-root metadata.
    pub thread: ThreadSnapshot,
    /// Immediate quoted/replied-to message, if any.
    pub quote: Option<QuoteDraft>,
    /// Open Lark wire message type.
    pub message_type: String,
    /// Lark create time in milliseconds since the Unix epoch.
    pub create_time_ms: i64,
    /// Typed content parts.
    pub parts: Vec<DraftPart>,
}

impl ContextDraft {
    /// Builds the best available draft from today's normalized inbound model.
    ///
    /// Later normalization stages may enrich `mentions`, media metadata, cards,
    /// audio, or video before registration.
    #[must_use]
    pub fn from_inbound(event: &InboundEvent) -> Self {
        let mut mentions = event
            .mentions
            .iter()
            .map(|mention| MentionSnapshot {
                kind: if mention.user_id.as_deref() == Some("all") {
                    MentionKind::Everyone
                } else {
                    MentionKind::User
                },
                open_id: mention.open_id.clone(),
                display_name: mention.name.clone(),
            })
            .collect::<Vec<_>>();
        if event.mentions_bot {
            mentions.push(MentionSnapshot {
                kind: MentionKind::Bot,
                open_id: None,
                display_name: None,
            });
        }
        if event.mention_all
            && !mentions
                .iter()
                .any(|mention| mention.kind == MentionKind::Everyone)
        {
            mentions.push(MentionSnapshot {
                kind: MentionKind::Everyone,
                open_id: None,
                display_name: None,
            });
        }

        let mut parts = event
            .parts
            .iter()
            .map(draft_part_from_inbound)
            .collect::<Vec<_>>();
        // Compatibility for persisted payloads created before typed parts
        // existed. New normalized events always take the branch above.
        if parts.is_empty() {
            if !event.text.is_empty() || event.message_type == "text" {
                parts.push(DraftPart::Text(event.text.clone()));
            }
            parts.extend(event.resources.iter().map(|resource| DraftPart::Media {
                kind: match resource.kind {
                    ResourceKind::Image => MediaKind::Image,
                    ResourceKind::File => MediaKind::File,
                },
                resource: resource.clone(),
                thumbnail: None,
                metadata: MediaMetadata::default(),
                transcript_failure: None,
            }));
        }
        if parts.is_empty() {
            parts.push(DraftPart::Unsupported {
                message_type: event.message_type.clone(),
                reason: "message type has no registered resolver".to_owned(),
            });
        }

        Self {
            event_id: event.event_id.clone(),
            message_id: event.message_id.clone(),
            sender: SenderSnapshot {
                open_id: event.sender_id.clone(),
                sender_type: "user".to_owned(),
            },
            chat: ChatSnapshot {
                chat_id: event.chat_id.clone(),
                kind: match event.chat_type {
                    ChatMode::P2p => ChatKind::P2p,
                    ChatMode::Group => ChatKind::Group,
                    ChatMode::Topic => ChatKind::Topic,
                },
            },
            mentions,
            thread: ThreadSnapshot {
                thread_id: event.thread_id.clone(),
                root_message_id: event.root_id.clone(),
            },
            quote: event
                .reply_to_message_id
                .as_ref()
                .map(|message_id| QuoteDraft::unavailable(message_id.clone())),
            message_type: event.message_type.clone(),
            create_time_ms: event.create_time_ms,
            parts,
        }
    }
}

pub(crate) fn draft_part_from_inbound(part: &MessagePart) -> DraftPart {
    match part {
        MessagePart::Text { text } => DraftPart::Text(text.clone()),
        MessagePart::Image(media) => draft_media_part(MediaKind::Image, ResourceKind::Image, media),
        MessagePart::File(media) => draft_media_part(MediaKind::File, ResourceKind::File, media),
        MessagePart::Sticker(media) => {
            draft_media_part(MediaKind::Sticker, ResourceKind::File, media)
        }
        MessagePart::Audio(media) => draft_media_part(MediaKind::Audio, ResourceKind::File, media),
        MessagePart::Video(media) => draft_media_part(MediaKind::Video, ResourceKind::File, media),
        MessagePart::Forward { message_id, status } => DraftPart::Forward {
            message_id: message_id.clone(),
            status: part_availability(*status),
        },
        MessagePart::Card { status } => DraftPart::Unsupported {
            message_type: "card".to_owned(),
            reason: part_status_reason(*status).to_owned(),
        },
        MessagePart::Unsupported {
            message_type,
            status,
        } => DraftPart::Unsupported {
            message_type: message_type.clone(),
            reason: part_status_reason(*status).to_owned(),
        },
    }
}

fn draft_media_part(kind: MediaKind, resource_kind: ResourceKind, media: &MediaPart) -> DraftPart {
    let Some(key) = media
        .key
        .as_ref()
        .filter(|_| media.status == PartStatus::Available)
    else {
        return DraftPart::Unsupported {
            message_type: media_kind_str(kind).to_owned(),
            reason: part_status_reason(media.status).to_owned(),
        };
    };
    DraftPart::Media {
        kind,
        resource: ResourceDesc {
            kind: resource_kind,
            key: key.clone(),
        },
        thumbnail: media.thumbnail_key.as_ref().map(|key| ResourceDesc {
            kind: ResourceKind::Image,
            key: key.clone(),
        }),
        metadata: media_metadata(&media.metadata),
        transcript_failure: media.metadata.transcript_failure,
    }
}

fn media_metadata(metadata: &InboundMediaMetadata) -> MediaMetadata {
    MediaMetadata {
        name: metadata.file_name.clone(),
        mime_type: metadata.mime_type.clone(),
        size_bytes: metadata.size_bytes,
        duration_ms: metadata.duration_ms,
    }
}

const fn part_availability(status: PartStatus) -> PartAvailability {
    match status {
        PartStatus::Available => PartAvailability::Available,
        PartStatus::Unsupported => PartAvailability::Unsupported,
        PartStatus::Unavailable => PartAvailability::Unavailable,
    }
}

const fn part_status_reason(status: PartStatus) -> &'static str {
    match status {
        PartStatus::Available => "message part has no registered resolver",
        PartStatus::Unsupported => "message part is not supported",
        PartStatus::Unavailable => "message part descriptor is unavailable",
    }
}

const fn media_kind_str(kind: MediaKind) -> &'static str {
    match kind {
        MediaKind::Image => "image",
        MediaKind::File => "file",
        MediaKind::Sticker => "sticker",
        MediaKind::Audio => "audio",
        MediaKind::Video => "video",
    }
}

/// Immutable message data returned to an authorized tool caller.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextSnapshot {
    /// Lark receive-event ID.
    pub event_id: String,
    /// Lark message ID.
    pub message_id: String,
    /// Sender metadata.
    pub sender: SenderSnapshot,
    /// Chat metadata.
    pub chat: ChatSnapshot,
    /// Explicit mentions.
    pub mentions: Vec<MentionSnapshot>,
    /// Topic and reply-root metadata.
    pub thread: ThreadSnapshot,
    /// Immediate quoted/replied-to message, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quote: Option<QuoteSnapshot>,
    /// Open Lark wire message type.
    pub message_type: String,
    /// Lark create time in milliseconds since the Unix epoch.
    pub create_time_ms: i64,
    /// Typed content parts.
    pub parts: Vec<TypedPart>,
}

/// Result of pending registration. Safe to serialize into a prompt reference.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisteredContext {
    /// Opaque context capability.
    pub context_id: ContextId,
    /// Current lifecycle state.
    pub lifecycle: ContextLifecycle,
}

/// A descriptor returned only after context, thread, turn, and handle checks.
pub struct AuthorizedResource {
    /// Lark message that owns the resource.
    pub message_id: String,
    /// Semantic media type.
    pub media_kind: MediaKind,
    /// Local lease owner passed to `AttachmentCache::fetch`.
    pub local_turn_row_id: i64,
    /// Key and endpoint kind consumed by `AttachmentCache`.
    pub resource: ResourceDesc,
    /// Inbound recognition text that can skip the sidecar.
    pub transcript: Option<String>,
    /// Why a supplied transcript was rejected before content retention.
    pub transcript_failure: Option<TranscriptFailure>,
    /// Declared duration, used to refuse over-long audio before download.
    pub duration_ms: Option<u64>,
    /// Cancellation tied to the exact context/turn capability. This is kept
    /// crate-private so callers cannot mint or replace lifecycle authority.
    pub(crate) cancellation: CancellationToken,
    read_charge: Option<ReadCharge>,
    response_operation: ResponseOperation,
}

impl AuthorizedResource {
    /// Whether the owning turn has already revoked this resource grant.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Replaces this attempt's pessimistic byte reservation with the exact
    /// materialized size. Failed attempts deliberately retain their charge so
    /// retries cannot bypass the turn budget.
    pub(crate) fn settle_read(&mut self, actual_bytes: u64) {
        if let Some(charge) = self.read_charge.as_mut() {
            charge.settle(actual_bytes);
        }
    }

    /// Establishes the response-vs-revocation linearization point. A `true`
    /// result authorizes one response while interrupt acknowledgement waits for
    /// this operation to be dropped; `false` forbids returning media content.
    pub(crate) fn commit_response(&self) -> bool {
        self.response_operation.commit()
    }
}

impl fmt::Debug for AuthorizedResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizedResource")
            .field("message_id_len", &self.message_id.len())
            .field("media_kind", &self.media_kind)
            .field("local_turn_row_id", &self.local_turn_row_id)
            .field("resource", &self.resource)
            .field("has_live_transcript", &self.transcript.is_some())
            .field("transcript_failure", &self.transcript_failure)
            .field("duration_ms", &self.duration_ms)
            .finish_non_exhaustive()
    }
}

/// Counts useful for bounded-registry telemetry.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ContextRegistryStats {
    /// All retained entries, including revoked tombstones.
    pub total: usize,
    /// Pending entries.
    pub pending: usize,
    /// Active entries.
    pub active: usize,
    /// Revoked entries.
    pub revoked: usize,
    /// Download grants still resident in active or pending entries.
    pub media_grants: usize,
}

#[derive(Clone)]
struct ResourceGrant {
    message_id: String,
    kind: MediaKind,
    resource: ResourceDesc,
    transcript: Option<String>,
    transcript_failure: Option<TranscriptFailure>,
    duration_ms: Option<u64>,
}

struct TurnReadMeter {
    reads: usize,
    charged_bytes: u64,
    observed_bytes: HashMap<MediaHandle, u64>,
    max_reads: usize,
    max_bytes: u64,
}

impl TurnReadMeter {
    fn new(config: ContextRegistryConfig) -> Self {
        Self {
            reads: 0,
            charged_bytes: 0,
            observed_bytes: HashMap::new(),
            max_reads: config.max_media_reads_per_turn,
            max_bytes: config.max_media_read_bytes_per_turn,
        }
    }

    fn reserve(
        &mut self,
        handle: &MediaHandle,
        max_unobserved_bytes: u64,
    ) -> Result<u64, ContextError> {
        let reserved_bytes = self
            .observed_bytes
            .get(handle)
            .copied()
            .unwrap_or(max_unobserved_bytes);
        if self.reads >= self.max_reads
            || self.charged_bytes.saturating_add(reserved_bytes) > self.max_bytes
        {
            return Err(error(
                ContextErrorCode::CapacityExceeded,
                "turn media read budget is exhausted",
                false,
            ));
        }
        self.reads = self.reads.saturating_add(1);
        self.charged_bytes = self.charged_bytes.saturating_add(reserved_bytes);
        Ok(reserved_bytes)
    }
}

struct ReadCharge {
    meter: Arc<Mutex<TurnReadMeter>>,
    handle: MediaHandle,
    reserved_bytes: u64,
    settled: bool,
}

impl ReadCharge {
    fn settle(&mut self, actual_bytes: u64) {
        if self.settled {
            return;
        }
        let mut meter = self
            .meter
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        meter.charged_bytes = meter
            .charged_bytes
            .saturating_sub(self.reserved_bytes)
            .saturating_add(actual_bytes);
        meter
            .observed_bytes
            .insert(self.handle.clone(), actual_bytes);
        self.settled = true;
    }
}

enum EntryState {
    Pending,
    Active { codex_turn_id: String },
    Revoked { reason: RevocationReason },
}

struct ContextEntry {
    pending_binding: PendingBinding,
    state: EntryState,
    expires_at: Instant,
    snapshot: ContextSnapshot,
    grants: HashMap<MediaHandle, ResourceGrant>,
    read_meter: Arc<Mutex<TurnReadMeter>>,
    cancellation: CancellationToken,
    response_gate: Arc<ResponseGate>,
}

#[derive(Default)]
struct ResponseGateState {
    revoked: bool,
    active: usize,
}

#[derive(Default)]
struct ResponseGate {
    state: Mutex<ResponseGateState>,
    idle: Notify,
}

impl ResponseGate {
    fn acquire(self: &Arc<Self>) -> Option<ResponseOperation> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.revoked {
            return None;
        }
        state.active = state.active.saturating_add(1);
        Some(ResponseOperation {
            gate: Arc::clone(self),
        })
    }

    fn revoke(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.revoked = true;
        if state.active == 0 {
            self.idle.notify_waiters();
        }
    }

    async fn wait_idle(&self) {
        loop {
            let notified = self.idle.notified();
            if self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .active
                == 0
            {
                return;
            }
            notified.await;
        }
    }
}

struct ResponseOperation {
    gate: Arc<ResponseGate>,
}

impl ResponseOperation {
    fn commit(&self) -> bool {
        let state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.revoked {
            return false;
        }
        true
    }
}

impl Drop for ResponseOperation {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active = state.active.saturating_sub(1);
        if state.active == 0 {
            self.gate.idle.notify_waiters();
        }
    }
}

#[derive(Default)]
struct RegistryState {
    entries: HashMap<ContextId, ContextEntry>,
}

/// Cloneable, concurrency-safe in-memory context capability registry.
#[derive(Clone)]
pub struct ContextRegistry {
    config: ContextRegistryConfig,
    state: Arc<Mutex<RegistryState>>,
}

impl fmt::Debug for ContextRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContextRegistry")
            .field("config", &self.config)
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

impl ContextRegistry {
    /// Creates an empty bounded registry.
    ///
    /// # Errors
    ///
    /// Returns `invalid_request` when any bound or the TTL is zero.
    pub fn new(config: ContextRegistryConfig) -> Result<Self, ContextError> {
        if config.ttl.is_zero()
            || config.max_contexts == 0
            || config.max_parts_per_context == 0
            || config.max_media_reads_per_turn == 0
            || config.max_media_read_bytes_per_turn == 0
            || config.max_media_read_bytes_per_item == 0
            || config.max_media_read_bytes_per_item > config.max_media_read_bytes_per_turn
        {
            return Err(error(
                ContextErrorCode::InvalidRequest,
                "context registry limits must be non-zero",
                false,
            ));
        }
        Ok(Self {
            config,
            state: Arc::new(Mutex::new(RegistryState::default())),
        })
    }

    /// Registers one message as a pending context and mints all opaque handles.
    ///
    /// # Errors
    ///
    /// Returns `capacity_exceeded` when no expired/revoked entry can be
    /// evicted, or `invalid_request` when the message has too many parts.
    pub fn register_pending(
        &self,
        binding: PendingBinding,
        draft: ContextDraft,
    ) -> Result<RegisteredContext, ContextError> {
        self.register_pending_with_transcripts(binding, draft, LiveTranscriptHandoff::empty())
    }

    pub(crate) fn register_pending_with_transcripts(
        &self,
        binding: PendingBinding,
        draft: ContextDraft,
        mut live_transcripts: LiveTranscriptHandoff,
    ) -> Result<RegisteredContext, ContextError> {
        let quote_parts = draft.quote.as_ref().map_or(0, |quote| quote.parts.len());
        if draft.parts.len().saturating_add(quote_parts) > self.config.max_parts_per_context {
            return Err(error(
                ContextErrorCode::InvalidRequest,
                "context contains too many typed parts",
                false,
            ));
        }

        let now = Instant::now();
        let expires_at = now.checked_add(self.config.ttl).ok_or_else(|| {
            error(
                ContextErrorCode::InvalidRequest,
                "context registry TTL is too large",
                false,
            )
        })?;
        let mut state = self.lock();
        make_capacity(&mut state, self.config.max_contexts, now)?;
        let read_meter = state
            .entries
            .values()
            .find(|entry| entry.pending_binding == binding)
            .map_or_else(
                || Arc::new(Mutex::new(TurnReadMeter::new(self.config))),
                |entry| Arc::clone(&entry.read_meter),
            );
        let context_id = unique_context_id(&state);
        let mut grants = HashMap::new();
        let message_id = draft.message_id.clone();
        let parts = draft
            .parts
            .into_iter()
            .enumerate()
            .map(|(part_index, part)| {
                materialize_part(
                    part_index,
                    part,
                    &message_id,
                    &mut live_transcripts,
                    &mut grants,
                )
            })
            .collect();
        let quote = draft.quote.map(|quote| {
            let quote_message_id = quote.message_id.clone();
            let mut quote_transcripts = LiveTranscriptHandoff::empty();
            let parts = quote
                .parts
                .into_iter()
                .enumerate()
                .map(|(part_index, part)| {
                    let part = prepare_live_quote_part(part);
                    materialize_part(
                        part_index,
                        part,
                        &quote_message_id,
                        &mut quote_transcripts,
                        &mut grants,
                    )
                })
                .collect();
            QuoteSnapshot {
                message_id: quote.message_id,
                message_type: quote.message_type,
                status: quote.status,
                parts,
            }
        });
        let snapshot = ContextSnapshot {
            event_id: draft.event_id,
            message_id: draft.message_id,
            sender: draft.sender,
            chat: draft.chat,
            mentions: draft.mentions,
            thread: draft.thread,
            quote,
            message_type: draft.message_type,
            create_time_ms: draft.create_time_ms,
            parts,
        };
        state.entries.insert(
            context_id.clone(),
            ContextEntry {
                pending_binding: binding,
                state: EntryState::Pending,
                expires_at,
                snapshot,
                grants,
                read_meter,
                cancellation: CancellationToken::new(),
                response_gate: Arc::new(ResponseGate::default()),
            },
        );
        Ok(RegisteredContext {
            context_id,
            lifecycle: ContextLifecycle::Pending,
        })
    }

    /// Binds a pending context to the concrete Codex turn returned by
    /// `turn/start`. Repeating the identical activation is idempotent.
    ///
    /// # Errors
    ///
    /// Returns `forbidden` for a different thread/local row and `unavailable`
    /// for an expired or revoked context.
    pub fn activate(
        &self,
        context_id: &ContextId,
        pending: &PendingBinding,
        codex_turn_id: impl Into<String>,
    ) -> Result<(), ContextError> {
        let codex_turn_id = codex_turn_id.into();
        if codex_turn_id.is_empty() {
            return Err(error(
                ContextErrorCode::InvalidRequest,
                "Codex turn ID must not be empty",
                false,
            ));
        }
        let now = Instant::now();
        let mut state = self.lock();
        let entry = state.entries.get_mut(context_id).ok_or_else(not_found)?;
        expire(entry, now);
        if &entry.pending_binding != pending {
            return Err(forbidden());
        }
        match &entry.state {
            EntryState::Pending => {
                entry.state = EntryState::Active { codex_turn_id };
                Ok(())
            }
            EntryState::Active {
                codex_turn_id: active,
            } if active == &codex_turn_id => Ok(()),
            EntryState::Active { .. } => Err(forbidden()),
            EntryState::Revoked { reason } => Err(unavailable(*reason)),
        }
    }

    /// Resolves the immutable message snapshot for the exact active turn.
    ///
    /// # Errors
    ///
    /// Pending, expired, and revoked contexts return `unavailable`; a binding
    /// mismatch returns `forbidden` without identifying the expected binding.
    pub fn resolve(
        &self,
        context_id: &ContextId,
        caller: &ActiveBinding,
    ) -> Result<ContextSnapshot, ContextError> {
        let now = Instant::now();
        let mut state = self.lock();
        let entry = state.entries.get_mut(context_id).ok_or_else(not_found)?;
        authorize_entry(entry, caller, now)?;
        Ok(entry.snapshot.clone())
    }

    /// Resolves a context from a Codex tool request, whose trusted envelope
    /// carries only `threadId` and `turnId`.
    ///
    /// A tool request can race ahead of the `turn/start` response. If this
    /// context is still pending and the thread matches, this method atomically
    /// binds it to the supplied turn ID. The local turn row never enters tool
    /// arguments; it remains part of the registry entry's binding.
    ///
    /// # Errors
    ///
    /// Returns `invalid_request` for empty IDs, `forbidden` for another Codex
    /// thread/turn, and `unavailable` after revocation or expiry.
    pub fn resolve_for_tool(
        &self,
        context_id: &ContextId,
        codex_thread_id: &str,
        codex_turn_id: &str,
    ) -> Result<ContextSnapshot, ContextError> {
        validate_tool_binding(codex_thread_id, codex_turn_id)?;
        let now = Instant::now();
        let mut state = self.lock();
        let entry = state.entries.get_mut(context_id).ok_or_else(not_found)?;
        authorize_entry_for_tool(entry, codex_thread_id, codex_turn_id, now)?;
        Ok(entry.snapshot.clone())
    }

    /// Authorizes a media handle and returns the descriptor needed by the
    /// attachment cache. This method performs no network or filesystem I/O.
    ///
    /// # Errors
    ///
    /// Returns `forbidden` for a handle from another context, `unavailable`
    /// outside the active turn, and `unsupported` for a non-media/unknown part.
    pub fn authorize_media(
        &self,
        context_id: &ContextId,
        handle: &MediaHandle,
        caller: &ActiveBinding,
    ) -> Result<AuthorizedResource, ContextError> {
        let now = Instant::now();
        let mut state = self.lock();
        let entry = state.entries.get_mut(context_id).ok_or_else(not_found)?;
        authorize_entry(entry, caller, now)?;
        authorized_resource(entry, handle, self.config.max_media_read_bytes_per_item)
    }

    /// Authorizes media from the trusted `threadId`/`turnId` tool envelope.
    /// `max_materialized_bytes` must be the same single-object cap enforced by
    /// the downstream attachment cache. Like [`Self::resolve_for_tool`], this
    /// safely handles a tool call that races ahead of explicit activation.
    ///
    /// # Errors
    ///
    /// Returns the same structured binding/lifecycle errors as
    /// [`Self::resolve_for_tool`], or `unsupported` for an unknown handle.
    pub fn authorize_media_for_tool(
        &self,
        context_id: &ContextId,
        handle: &MediaHandle,
        codex_thread_id: &str,
        codex_turn_id: &str,
        max_materialized_bytes: u64,
    ) -> Result<AuthorizedResource, ContextError> {
        validate_tool_binding(codex_thread_id, codex_turn_id)?;
        let now = Instant::now();
        let mut state = self.lock();
        let entry = state.entries.get_mut(context_id).ok_or_else(not_found)?;
        authorize_entry_for_tool(entry, codex_thread_id, codex_turn_id, now)?;
        authorized_resource(entry, handle, max_materialized_bytes)
    }

    /// Revokes a single context and erases all underlying resource grants.
    /// Repeating a revocation is idempotent.
    ///
    /// # Errors
    ///
    /// Returns `forbidden` when the pending binding does not match.
    pub fn revoke(
        &self,
        context_id: &ContextId,
        binding: &PendingBinding,
        reason: RevocationReason,
    ) -> Result<(), ContextError> {
        let mut state = self.lock();
        let entry = state.entries.get_mut(context_id).ok_or_else(not_found)?;
        if &entry.pending_binding != binding {
            return Err(forbidden());
        }
        revoke_entry(entry, reason);
        Ok(())
    }

    /// Revokes every context belonging to one local turn. Returns the number
    /// of contexts transitioned during this call.
    #[must_use]
    pub fn revoke_turn(&self, binding: &PendingBinding, reason: RevocationReason) -> usize {
        let mut state = self.lock();
        let mut revoked = 0;
        for entry in state.entries.values_mut() {
            if &entry.pending_binding == binding
                && !matches!(entry.state, EntryState::Revoked { .. })
            {
                revoke_entry(entry, reason);
                revoked += 1;
            }
        }
        revoked
    }

    /// Revokes a turn and waits until every media operation that linearized
    /// before revocation has sent (or abandoned) its response. This is used by
    /// interrupt handling so an acknowledged interrupt cannot be followed by
    /// transcript/media content from the cancelled turn.
    pub async fn revoke_turn_and_wait(
        &self,
        binding: &PendingBinding,
        reason: RevocationReason,
    ) -> usize {
        let (revoked, gates) = {
            let mut state = self.lock();
            let mut revoked = 0;
            let mut gates = Vec::new();
            for entry in state.entries.values_mut() {
                if &entry.pending_binding == binding
                    && !matches!(entry.state, EntryState::Revoked { .. })
                {
                    let gate = Arc::clone(&entry.response_gate);
                    revoke_entry(entry, reason);
                    gates.push(gate);
                    revoked += 1;
                }
            }
            (revoked, gates)
        };
        for gate in gates {
            gate.wait_idle().await;
        }
        revoked
    }

    /// Returns bounded aggregate counts without exposing capability IDs.
    #[must_use]
    pub fn stats(&self) -> ContextRegistryStats {
        let mut state = self.lock();
        let now = Instant::now();
        for entry in state.entries.values_mut() {
            expire(entry, now);
        }
        let mut counts = ContextRegistryStats {
            total: state.entries.len(),
            ..ContextRegistryStats::default()
        };
        for entry in state.entries.values() {
            match entry.state {
                EntryState::Pending => counts.pending += 1,
                EntryState::Active { .. } => counts.active += 1,
                EntryState::Revoked { .. } => counts.revoked += 1,
            }
            counts.media_grants += entry.grants.len();
        }
        counts
    }

    fn lock(&self) -> MutexGuard<'_, RegistryState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Default for ContextRegistry {
    fn default() -> Self {
        Self::new(ContextRegistryConfig::default())
            .expect("default context registry limits are non-zero")
    }
}

fn prepare_live_quote_part(mut part: DraftPart) -> DraftPart {
    let DraftPart::Media {
        kind,
        transcript_failure,
        ..
    } = &mut part
    else {
        return part;
    };
    if *kind == MediaKind::Audio && *transcript_failure == Some(TranscriptFailure::NotRetained) {
        // A quote is resolved live, after authorization and only when the
        // instruction triggers it. Its fetched recognition text is never
        // retained or trusted as the transcript, so byte-backed sidecar ASR
        // remains available. Durable inbound recovery still preserves the
        // fail-closed `NotRetained` behavior in the ordinary part path.
        *transcript_failure = None;
    }
    part
}

fn materialize_part(
    part_index: usize,
    part: DraftPart,
    message_id: &str,
    live_transcripts: &mut LiveTranscriptHandoff,
    grants: &mut HashMap<MediaHandle, ResourceGrant>,
) -> TypedPart {
    match part {
        DraftPart::Text(text) => TypedPart::Text { text },
        DraftPart::Media {
            kind,
            resource,
            thumbnail,
            metadata,
            transcript_failure,
        } => {
            let transcript = (kind == MediaKind::Audio)
                .then(|| live_transcripts.take_for_part(part_index))
                .flatten();
            let transcript_failure = if transcript.is_some() {
                None
            } else {
                transcript_failure
            };
            let handle = unique_media_handle(grants);
            grants.insert(
                handle.clone(),
                ResourceGrant {
                    message_id: message_id.to_owned(),
                    kind,
                    resource,
                    transcript,
                    transcript_failure,
                    duration_ms: metadata.duration_ms,
                },
            );
            let thumbnail_handle = thumbnail.map(|resource| {
                let thumbnail_handle = unique_media_handle(grants);
                grants.insert(
                    thumbnail_handle.clone(),
                    ResourceGrant {
                        message_id: message_id.to_owned(),
                        kind: MediaKind::Image,
                        resource,
                        transcript: None,
                        transcript_failure: None,
                        duration_ms: None,
                    },
                );
                thumbnail_handle
            });
            TypedPart::Media {
                kind,
                handle,
                thumbnail_handle,
                metadata,
            }
        }
        DraftPart::Card(content) => TypedPart::Card { content },
        DraftPart::Forward { message_id, status } => TypedPart::Forward { message_id, status },
        DraftPart::Unsupported {
            message_type,
            reason,
        } => TypedPart::Unsupported {
            message_type,
            reason,
        },
    }
}

fn unique_context_id(state: &RegistryState) -> ContextId {
    loop {
        let candidate = ContextId(format!("bctx_{}", Uuid::new_v4().simple()));
        if !state.entries.contains_key(&candidate) {
            return candidate;
        }
    }
}

fn unique_media_handle(grants: &HashMap<MediaHandle, ResourceGrant>) -> MediaHandle {
    loop {
        let candidate = MediaHandle(format!("bmedia_{}", Uuid::new_v4().simple()));
        if !grants.contains_key(&candidate) {
            return candidate;
        }
    }
}

fn make_capacity(
    state: &mut RegistryState,
    max_contexts: usize,
    now: Instant,
) -> Result<(), ContextError> {
    if state.entries.len() < max_contexts {
        return Ok(());
    }
    for entry in state.entries.values_mut() {
        expire(entry, now);
    }
    state
        .entries
        .retain(|_, entry| !matches!(entry.state, EntryState::Revoked { .. }));
    if state.entries.len() >= max_contexts {
        return Err(error(
            ContextErrorCode::CapacityExceeded,
            "context registry is at capacity",
            true,
        ));
    }
    Ok(())
}

fn authorize_entry(
    entry: &mut ContextEntry,
    caller: &ActiveBinding,
    now: Instant,
) -> Result<(), ContextError> {
    expire(entry, now);
    if entry.pending_binding.codex_thread_id != caller.codex_thread_id
        || entry.pending_binding.local_turn_row_id != caller.local_turn_row_id
    {
        return Err(forbidden());
    }
    match &entry.state {
        EntryState::Pending => Err(error(
            ContextErrorCode::Unavailable,
            "context is pending turn activation",
            true,
        )),
        EntryState::Active { codex_turn_id } if codex_turn_id == &caller.codex_turn_id => Ok(()),
        EntryState::Active { .. } => Err(forbidden()),
        EntryState::Revoked { reason } => Err(unavailable(*reason)),
    }
}

fn authorize_entry_for_tool(
    entry: &mut ContextEntry,
    codex_thread_id: &str,
    codex_turn_id: &str,
    now: Instant,
) -> Result<(), ContextError> {
    expire(entry, now);
    if entry.pending_binding.codex_thread_id != codex_thread_id {
        return Err(forbidden());
    }
    match &entry.state {
        EntryState::Pending => {
            entry.state = EntryState::Active {
                codex_turn_id: codex_turn_id.to_owned(),
            };
            Ok(())
        }
        EntryState::Active {
            codex_turn_id: active,
        } if active == codex_turn_id => Ok(()),
        EntryState::Active { .. } => Err(forbidden()),
        EntryState::Revoked { reason } => Err(unavailable(*reason)),
    }
}

fn authorized_resource(
    entry: &ContextEntry,
    handle: &MediaHandle,
    max_unobserved_bytes: u64,
) -> Result<AuthorizedResource, ContextError> {
    let grant = entry.grants.get(handle).ok_or_else(|| {
        error(
            ContextErrorCode::Unsupported,
            "media handle is not resolvable in this context",
            false,
        )
    })?;
    let response_operation = entry.response_gate.acquire().ok_or_else(|| {
        error(
            ContextErrorCode::Unavailable,
            "context capability was revoked",
            false,
        )
    })?;
    let reserved_bytes = entry
        .read_meter
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .reserve(handle, max_unobserved_bytes)?;
    Ok(AuthorizedResource {
        message_id: grant.message_id.clone(),
        media_kind: grant.kind,
        local_turn_row_id: entry.pending_binding.local_turn_row_id,
        resource: grant.resource.clone(),
        transcript: grant.transcript.clone(),
        transcript_failure: grant.transcript_failure,
        duration_ms: grant.duration_ms,
        cancellation: entry.cancellation.clone(),
        read_charge: Some(ReadCharge {
            meter: Arc::clone(&entry.read_meter),
            handle: handle.clone(),
            reserved_bytes,
            settled: false,
        }),
        response_operation,
    })
}

fn validate_tool_binding(codex_thread_id: &str, codex_turn_id: &str) -> Result<(), ContextError> {
    if codex_thread_id.is_empty() || codex_turn_id.is_empty() {
        return Err(error(
            ContextErrorCode::InvalidRequest,
            "Codex thread and turn IDs must not be empty",
            false,
        ));
    }
    Ok(())
}

fn expire(entry: &mut ContextEntry, now: Instant) {
    if now >= entry.expires_at && !matches!(entry.state, EntryState::Revoked { .. }) {
        revoke_entry(entry, RevocationReason::Expired);
    }
}

fn revoke_entry(entry: &mut ContextEntry, reason: RevocationReason) {
    if matches!(entry.state, EntryState::Revoked { .. }) {
        return;
    }
    entry.cancellation.cancel();
    entry.response_gate.revoke();
    entry.state = EntryState::Revoked { reason };
    entry.grants.clear();
}

fn unavailable(reason: RevocationReason) -> ContextError {
    let message = match reason {
        RevocationReason::Expired => "context capability has expired",
        RevocationReason::Completed => "context is unavailable after turn completion",
        RevocationReason::Failed => "context is unavailable after turn failure",
        RevocationReason::Cancelled => "context is unavailable after turn cancellation",
        RevocationReason::Replaced => "context capability was replaced",
        RevocationReason::Shutdown => "context is unavailable after bridge shutdown",
    };
    error(ContextErrorCode::Unavailable, message, false)
}

fn not_found() -> ContextError {
    error(
        ContextErrorCode::NotFound,
        "context capability was not found",
        false,
    )
}

fn forbidden() -> ContextError {
    error(
        ContextErrorCode::Forbidden,
        "context capability does not belong to this Codex turn",
        false,
    )
}

const fn error(code: ContextErrorCode, message: &'static str, retryable: bool) -> ContextError {
    ContextError {
        code,
        message,
        retryable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn revocation_forbids_an_uncommitted_media_response() {
        let gate = Arc::new(ResponseGate::default());
        let operation = gate.acquire().expect("active response operation");

        gate.revoke();
        assert!(
            !operation.commit(),
            "a response cannot commit after revocation linearizes"
        );

        let waiting_gate = Arc::clone(&gate);
        let waiter = tokio::spawn(async move { waiting_gate.wait_idle().await });
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "interrupt acknowledgement waits for the cancelled response"
        );
        drop(operation);
        timeout(Duration::from_secs(1), waiter)
            .await
            .expect("response barrier becomes idle")
            .expect("waiter joins");
    }

    #[tokio::test]
    async fn committed_media_response_finishes_before_revocation_acknowledgement() {
        let gate = Arc::new(ResponseGate::default());
        let operation = gate.acquire().expect("active response operation");
        assert!(operation.commit(), "response linearizes before revocation");

        gate.revoke();
        let waiting_gate = Arc::clone(&gate);
        let waiter = tokio::spawn(async move { waiting_gate.wait_idle().await });
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "revocation acknowledgement cannot pass a committed response"
        );
        drop(operation);
        timeout(Duration::from_secs(1), waiter)
            .await
            .expect("committed response drains")
            .expect("waiter joins");
    }
}
