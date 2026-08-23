//! Provider-neutral channel capabilities used by the Rust core.
//!
//! The bridge deliberately keeps vendor SDK values outside these traits.  A
//! transport may be the native Rust WebSocket or a supervised sidecar, while
//! query, media, and outbound implementations can move independently without
//! changing durable inbox/outbox or scope semantics.

use std::fmt;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use serde_json::Value;
use tokio::sync::watch;

use crate::lark::bridge::QueuedInboundEvent;

pub mod native;
pub mod sidecar;
pub mod wire;

/// Lifecycle state shared by every inbound implementation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionState {
    /// A bootstrap + connect attempt is in progress.
    Connecting {
        /// One-based attempt number within the current outage.
        attempt: u32,
    },
    /// Inbound delivery is live.
    Connected,
    /// The source is waiting before retrying.
    Backoff {
        /// Consecutive failures in the current outage.
        attempt: u32,
        /// Exact bounded delay before the next attempt.
        delay: Duration,
    },
    /// A non-retryable failure stopped the source.
    Degraded {
        /// Static classification, never provider text or credentials.
        reason: String,
    },
    /// The source shut down cleanly.
    Stopped,
}

/// Running inbound source owned by application assembly.
pub trait InboundSource: Send {
    /// Subscribes to lifecycle changes.
    fn subscribe_state(&self) -> watch::Receiver<ConnectionState>;

    /// Stops the source and joins all owned work.
    fn shutdown(self: Box<Self>) -> BoxFuture<'static, ()>;
}

/// A running source plus its bounded, already-durable event stream.
pub struct InboundRuntime {
    /// Source lifecycle/supervision handle.
    pub source: Box<dyn InboundSource>,
    /// Canonical events whose receipt boundary has already completed.
    pub events: tokio::sync::mpsc::Receiver<QueuedInboundEvent>,
}

impl fmt::Debug for InboundRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InboundRuntime")
            .finish_non_exhaustive()
    }
}

/// Coarse, provider-independent failure kind for query and media operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelErrorKind {
    /// Authentication or authorization cannot succeed without intervention.
    PermanentAuth,
    /// A bounded retry can succeed.
    Retryable,
    /// The provider or sidecar violated the capability contract.
    Protocol,
    /// A configured count, byte, or time bound was exceeded.
    Exhausted,
}

/// Content-free query/media failure.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ChannelError {
    kind: ChannelErrorKind,
    context: &'static str,
}

impl ChannelError {
    /// Constructs a classified error with a static, non-sensitive context.
    #[must_use]
    pub const fn new(kind: ChannelErrorKind, context: &'static str) -> Self {
        Self { kind, context }
    }

    /// Returns the stable failure kind.
    #[must_use]
    pub const fn kind(self) -> ChannelErrorKind {
        self.kind
    }
}

impl fmt::Display for ChannelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "channel {:?} failure while {}",
            self.kind, self.context
        )
    }
}

impl fmt::Debug for ChannelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl std::error::Error for ChannelError {}

/// Provider-neutral conversation mode needed by scope normalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConversationMode {
    /// Direct conversation.
    P2p,
    /// Plain group conversation.
    Group,
    /// Topic/threaded group conversation.
    Topic,
}

/// Minimal message fields needed for reply-chain and topic resolution.
#[derive(Clone, PartialEq, Eq)]
pub struct MessageSnapshot {
    /// Provider message identifier.
    pub message_id: String,
    /// Owning conversation identifier.
    pub chat_id: String,
    /// Open provider chat-type spelling.
    pub chat_type: String,
    /// Open provider message-type spelling.
    pub message_type: String,
    /// Reply root, when present.
    pub root_id: Option<String>,
    /// Immediate parent, when present.
    pub parent_id: Option<String>,
    /// Topic identifier, when present.
    pub thread_id: Option<String>,
}

impl fmt::Debug for MessageSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MessageSnapshot")
            .field("message_id_len", &self.message_id.len())
            .field("chat_id_len", &self.chat_id.len())
            .field("chat_type_len", &self.chat_type.len())
            .field("message_type_len", &self.message_type.len())
            .field("has_root", &self.root_id.is_some())
            .field("has_parent", &self.parent_id.is_some())
            .field("has_thread", &self.thread_id.is_some())
            .finish()
    }
}

/// Required provider reads. Implementations must validate identifiers and
/// return bounded values.
pub trait ChatMessageQuery: Send + Sync {
    /// Resolves one message without retaining its content body.
    fn message(
        &self,
        message_id: String,
    ) -> BoxFuture<'static, Result<MessageSnapshot, ChannelError>>;

    /// Resolves the conversation mode for one chat.
    fn conversation_mode(
        &self,
        chat_id: String,
    ) -> BoxFuture<'static, Result<ConversationMode, ChannelError>>;
}

/// Provider-neutral resource selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaKind {
    /// Image resource.
    Image,
    /// File-backed resource (file/audio/video/sticker payload).
    File,
}

impl MediaKind {
    pub(crate) const fn as_provider_str(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::File => "file",
        }
    }
}

/// Controlled media request. Identifiers are opaque provider handles, never
/// local paths or URLs.
pub struct MediaRequest {
    /// Owning message identifier.
    pub message_id: String,
    /// Opaque resource key.
    pub resource_key: String,
    /// Provider-neutral resource kind.
    pub kind: MediaKind,
}

impl fmt::Debug for MediaRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MediaRequest")
            .field("message_id_len", &self.message_id.len())
            .field("resource_key_len", &self.resource_key.len())
            .field("kind", &self.kind)
            .finish()
    }
}

/// Capability used by the attachment cache to resolve an authorized opaque
/// handle into bounded bytes.
pub trait ControlledMediaResolver: Send + Sync {
    /// Resolves one resource. Implementations must stop reading at their
    /// configured byte cap.
    fn resolve(&self, request: MediaRequest) -> BoxFuture<'static, Result<Bytes, ChannelError>>;
}

/// One provider-neutral outbound operation.
pub enum OutboundRequest {
    /// Reply with plain text.
    ReplyText {
        /// Parent message.
        message_id: String,
        /// Whether to keep the reply inside its topic.
        in_thread: bool,
        /// Reply content (redacted from `Debug`).
        text: String,
    },
    /// Reply with an interactive card.
    ReplyCard {
        /// Parent message.
        message_id: String,
        /// Whether to keep the reply inside its topic.
        in_thread: bool,
        /// Card JSON (redacted from `Debug`).
        card: Value,
    },
    /// Update an existing interactive card.
    UpdateCard {
        /// Existing message identifier.
        message_id: String,
        /// Card JSON (redacted from `Debug`).
        card: Value,
    },
}

impl fmt::Debug for OutboundRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReplyText {
                message_id,
                in_thread,
                text,
            } => formatter
                .debug_struct("ReplyText")
                .field("message_id_len", &message_id.len())
                .field("in_thread", in_thread)
                .field("text_len", &text.len())
                .finish(),
            Self::ReplyCard {
                message_id,
                in_thread,
                card: _,
            } => formatter
                .debug_struct("ReplyCard")
                .field("message_id_len", &message_id.len())
                .field("in_thread", in_thread)
                .finish(),
            Self::UpdateCard {
                message_id,
                card: _,
            } => formatter
                .debug_struct("UpdateCard")
                .field("message_id_len", &message_id.len())
                .finish(),
        }
    }
}

/// Successful provider receipt.
#[derive(Clone, PartialEq, Eq)]
pub struct DeliveryReceipt {
    /// Server-assigned message id. For updates this is the updated id.
    pub message_id: String,
}

impl fmt::Debug for DeliveryReceipt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryReceipt")
            .field("message_id_len", &self.message_id.len())
            .finish()
    }
}

/// Three-way failure classification required by the durable outbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryFailureClass {
    /// The provider definitively rejected the request and retrying may work.
    Retryable,
    /// The request may have reached the provider; automatic retry is unsafe.
    Uncertain,
    /// The request definitively cannot succeed without changing input/config.
    Definitive,
}

/// Content-free outbound failure.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct DeliveryError {
    class: DeliveryFailureClass,
    context: &'static str,
}

impl DeliveryError {
    /// Creates a classified outbound failure.
    #[must_use]
    pub const fn new(class: DeliveryFailureClass, context: &'static str) -> Self {
        Self { class, context }
    }

    /// Returns the receipt classification.
    #[must_use]
    pub const fn class(self) -> DeliveryFailureClass {
        self.class
    }
}

impl fmt::Display for DeliveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "channel delivery {:?} while {}",
            self.class, self.context
        )
    }
}

impl fmt::Debug for DeliveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl std::error::Error for DeliveryError {}

/// Outbound provider capability. Durable ownership stays in Rust; this trait
/// performs exactly one network attempt and returns an explicit receipt class.
pub trait OutboundDelivery: Send + Sync {
    /// Executes one already-durable operation.
    fn deliver(
        &self,
        request: OutboundRequest,
    ) -> BoxFuture<'static, Result<DeliveryReceipt, DeliveryError>>;
}
