//! Native Rust adapters for the provider-neutral channel capabilities.

use std::sync::Arc;

use futures_util::FutureExt;

use super::{
    ChannelError, ChannelErrorKind, ChatMessageQuery, ControlledMediaResolver, ConversationMode,
    DeliveryError, DeliveryFailureClass, DeliveryReceipt, InboundSource, MediaKind, MediaRequest,
    MessageSnapshot, OutboundDelivery, OutboundRequest,
};
use crate::lark::api::{ChatMode, LarkApi, ResourceKind};
use crate::lark::error::LarkError;
use crate::lark::transport::TransportHandle;
use crate::outbox::pump::{is_documented_transient, is_http_server_error};

/// Native `OpenAPI` implementation of query, media, and outbound capabilities.
#[derive(Clone)]
pub struct NativeChannel {
    api: LarkApi,
}

impl NativeChannel {
    /// Wraps a tenant-bound native API client.
    #[must_use]
    pub fn new(api: LarkApi) -> Self {
        Self { api }
    }
}

impl std::fmt::Debug for NativeChannel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeChannel")
            .finish_non_exhaustive()
    }
}

impl ChatMessageQuery for NativeChannel {
    fn message(
        &self,
        message_id: String,
    ) -> futures_util::future::BoxFuture<'static, Result<MessageSnapshot, ChannelError>> {
        let api = self.api.clone();
        async move {
            let message = api
                .get_message(&message_id)
                .await
                .map_err(|error| channel_error(&error, "querying a message"))?;
            Ok(MessageSnapshot {
                message_id: message.message_id,
                chat_id: message.chat_id,
                chat_type: message.chat_type,
                message_type: message.message_type,
                root_id: message.root_id,
                parent_id: message.parent_id,
                thread_id: message.thread_id,
            })
        }
        .boxed()
    }

    fn conversation_mode(
        &self,
        chat_id: String,
    ) -> futures_util::future::BoxFuture<'static, Result<ConversationMode, ChannelError>> {
        let api = self.api.clone();
        async move {
            api.get_chat_mode(&chat_id)
                .await
                .map(|mode| match mode {
                    ChatMode::P2p => ConversationMode::P2p,
                    ChatMode::Group => ConversationMode::Group,
                    ChatMode::Topic => ConversationMode::Topic,
                })
                .map_err(|error| channel_error(&error, "querying a chat"))
        }
        .boxed()
    }
}

impl ControlledMediaResolver for NativeChannel {
    fn resolve(
        &self,
        request: MediaRequest,
    ) -> futures_util::future::BoxFuture<'static, Result<bytes::Bytes, ChannelError>> {
        let api = self.api.clone();
        async move {
            api.download_message_resource(
                &request.message_id,
                &request.resource_key,
                match request.kind {
                    MediaKind::Image => ResourceKind::Image,
                    MediaKind::File => ResourceKind::File,
                },
            )
            .await
            .map(|resource| resource.bytes)
            .map_err(|error| channel_error(&error, "resolving controlled media"))
        }
        .boxed()
    }
}

impl OutboundDelivery for NativeChannel {
    fn deliver(
        &self,
        request: OutboundRequest,
    ) -> futures_util::future::BoxFuture<'static, Result<DeliveryReceipt, DeliveryError>> {
        let api = self.api.clone();
        async move {
            let message_id = match request {
                OutboundRequest::ReplyText {
                    message_id,
                    in_thread: true,
                    text,
                } => api
                    .reply_text_in_thread(&message_id, &text)
                    .await
                    .map(|receipt| receipt.message_id),
                OutboundRequest::ReplyText {
                    message_id,
                    in_thread: false,
                    text,
                } => api
                    .reply_text(&message_id, &text)
                    .await
                    .map(|receipt| receipt.message_id),
                OutboundRequest::ReplyCard {
                    message_id,
                    in_thread: true,
                    card,
                } => api
                    .reply_card_in_thread(&message_id, card)
                    .await
                    .map(|receipt| receipt.message_id),
                OutboundRequest::ReplyCard {
                    message_id,
                    in_thread: false,
                    card,
                } => api
                    .reply_card(&message_id, card)
                    .await
                    .map(|receipt| receipt.message_id),
                OutboundRequest::ReplyMarkdownPost {
                    message_id,
                    in_thread: true,
                    markdown,
                } => api
                    .reply_post_markdown_in_thread(&message_id, &markdown)
                    .await
                    .map(|receipt| receipt.message_id),
                OutboundRequest::ReplyMarkdownPost {
                    message_id,
                    in_thread: false,
                    markdown,
                } => api
                    .reply_post_markdown(&message_id, &markdown)
                    .await
                    .map(|receipt| receipt.message_id),
                OutboundRequest::UpdateCard { message_id, card } => api
                    .update_card(&message_id, card)
                    .await
                    .map(|()| message_id),
            }
            .map_err(|error| delivery_error(&error))?;
            Ok(DeliveryReceipt { message_id })
        }
        .boxed()
    }
}

impl OutboundDelivery for LarkApi {
    fn deliver(
        &self,
        request: OutboundRequest,
    ) -> futures_util::future::BoxFuture<'static, Result<DeliveryReceipt, DeliveryError>> {
        NativeChannel::new(self.clone()).deliver(request)
    }
}

fn channel_error(error: &LarkError, context: &'static str) -> ChannelError {
    let kind = match error {
        LarkError::PermanentAuth { .. } => ChannelErrorKind::PermanentAuth,
        LarkError::Retryable { .. } => ChannelErrorKind::Retryable,
        LarkError::InvalidRequest { .. } | LarkError::ProtocolViolation { .. } => {
            ChannelErrorKind::Protocol
        }
        LarkError::Exhausted { .. } => ChannelErrorKind::Exhausted,
    };
    ChannelError::new(kind, context)
}

fn delivery_error(error: &LarkError) -> DeliveryError {
    const CONTEXT: &str = "delivering an outbound operation";
    match error {
        LarkError::InvalidRequest { .. }
        | LarkError::PermanentAuth { .. }
        | LarkError::Exhausted { .. } => {
            DeliveryError::new(DeliveryFailureClass::Definitive, CONTEXT)
        }
        LarkError::Retryable {
            code: Some(code), ..
        }
        | LarkError::ProtocolViolation {
            code: Some(code), ..
        } => {
            // Mirrors `delivery_decision` in the outbox pump: an explicit 5xx
            // cannot prove a POST was not applied, so it stays uncertain, but
            // an idempotent PATCH may retry it with the same body.
            if is_http_server_error(*code) {
                DeliveryError::new(DeliveryFailureClass::Uncertain, CONTEXT)
                    .with_patch_retryable(true)
            } else if is_documented_transient(*code) {
                DeliveryError::new(DeliveryFailureClass::Retryable, CONTEXT)
                    .with_patch_retryable(true)
            } else {
                DeliveryError::new(DeliveryFailureClass::Definitive, CONTEXT)
            }
        }
        LarkError::Retryable { code: None, .. }
        | LarkError::ProtocolViolation { code: None, .. } => {
            DeliveryError::new(DeliveryFailureClass::Uncertain, CONTEXT)
        }
    }
}

/// Inbound-source adapter retaining the native transport as the fallback.
pub struct NativeInboundSource {
    handle: TransportHandle,
}

impl NativeInboundSource {
    /// Wraps a running native transport.
    #[must_use]
    pub fn new(handle: TransportHandle) -> Self {
        Self { handle }
    }
}

impl InboundSource for NativeInboundSource {
    fn subscribe_state(&self) -> tokio::sync::watch::Receiver<super::ConnectionState> {
        self.handle.subscribe_state()
    }

    fn shutdown(self: Box<Self>) -> futures_util::future::BoxFuture<'static, ()> {
        async move { self.handle.shutdown().await }.boxed()
    }
}

/// Shared native capability bundle used by application assembly.
#[must_use]
pub fn shared(api: LarkApi) -> Arc<NativeChannel> {
    Arc::new(NativeChannel::new(api))
}
