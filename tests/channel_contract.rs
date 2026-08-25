//! Provider-neutral boundary tests independent of native Lark clients.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::{FutureExt, future::BoxFuture};
use serde_json::json;
use tokio::sync::watch;

use lark_codex_bridge::channel::{
    ChannelError, ChatMessageQuery, ConnectionState, ControlledMediaResolver, ConversationMode,
    DeliveryError, DeliveryFailureClass, DeliveryReceipt, MediaKind, MediaRequest, MessageSnapshot,
    OutboundDelivery, OutboundRequest,
};
use lark_codex_bridge::lark::normalize::{Normalizer, ScopeKey};
use lark_codex_bridge::outbox::{OutboxOperation, OutboxPump, OutboxPumpConfig};
use lark_codex_bridge::runtime::attachments::{ChannelResourceDownloader, ResourceDownloader};
use lark_codex_bridge::store::{NewOutboxRow, OutboxEnqueue, OutboxState, StoreHandle};

struct FakeQuery;

impl ChatMessageQuery for FakeQuery {
    fn message(
        &self,
        message_id: String,
    ) -> BoxFuture<'static, Result<MessageSnapshot, ChannelError>> {
        async move {
            Ok(MessageSnapshot {
                message_id,
                chat_id: "oc_query".to_owned(),
                chat_type: "group".to_owned(),
                message_type: "text".to_owned(),
                root_id: None,
                parent_id: None,
                thread_id: Some("omt_query".to_owned()),
            })
        }
        .boxed()
    }

    fn conversation_mode(
        &self,
        _chat_id: String,
    ) -> BoxFuture<'static, Result<ConversationMode, ChannelError>> {
        async { Ok(ConversationMode::Topic) }.boxed()
    }
}

#[tokio::test]
async fn normalizer_uses_only_the_stable_query_capability() {
    let normalizer = Normalizer::with_query(Arc::new(FakeQuery), "ou_bot");
    let payload = serde_json::to_vec(&json!({
        "schema": "2.0",
        "header": {
            "event_id": "evt_query_boundary",
            "event_type": "im.message.receive_v1"
        },
        "event": {
            "sender": {
                "sender_id": { "open_id": "ou_sender" },
                "sender_type": "user"
            },
            "message": {
                "message_id": "om_query",
                "chat_id": "oc_query",
                "chat_type": "group",
                "message_type": "text",
                "content": "{\"text\":\"hello\"}",
                "create_time": "1",
                "thread_id": "omt_query"
            }
        }
    }))
    .expect("fixture JSON");

    let outcome = normalizer.normalize(&payload).await.expect("normalize");
    let lark_codex_bridge::lark::normalize::NormalizeOutcome::Event { event, .. } = outcome else {
        panic!("expected message event");
    };
    assert_eq!(event.chat_type, ConversationMode::Topic);
    assert_eq!(
        event.scope,
        ScopeKey::Thread("oc_query".to_owned(), "omt_query".to_owned())
    );
}

struct FakeMedia {
    seen: Arc<Mutex<Vec<MediaRequest>>>,
}

impl ControlledMediaResolver for FakeMedia {
    fn resolve(&self, request: MediaRequest) -> BoxFuture<'static, Result<Bytes, ChannelError>> {
        self.seen.lock().expect("media requests").push(request);
        async { Ok(Bytes::from_static(b"bounded")) }.boxed()
    }
}

#[tokio::test]
async fn attachment_adapter_resolves_opaque_media_without_a_native_api_type() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let resolver: Arc<dyn ControlledMediaResolver> = Arc::new(FakeMedia {
        seen: Arc::clone(&seen),
    });
    let downloader = ChannelResourceDownloader::new(resolver);
    let bytes = downloader
        .download("om_media", "file_key", MediaKind::File)
        .await
        .expect("controlled media");
    assert_eq!(bytes, Bytes::from_static(b"bounded"));
    let requests = seen.lock().expect("media requests");
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].kind, MediaKind::File);
    assert_eq!(requests[0].message_id, "om_media");
    assert_eq!(requests[0].resource_key, "file_key");
}

struct FakeDelivery(DeliveryFailureClass);

impl OutboundDelivery for FakeDelivery {
    fn deliver(
        &self,
        _request: OutboundRequest,
    ) -> BoxFuture<'static, Result<DeliveryReceipt, DeliveryError>> {
        let class = self.0;
        async move { Err(DeliveryError::new(class, "fake provider delivery")) }.boxed()
    }
}

async fn enqueue_reply(store: &StoreHandle, key: &str) -> i64 {
    let payload_json = OutboxOperation::ReplyText {
        message_id: "om_parent".to_owned(),
        thread_id: None,
        text: "content stays redacted".to_owned(),
    }
    .encode()
    .expect("encode outbox operation");
    match store
        .enqueue_outbox(NewOutboxRow {
            idempotency_key: key.to_owned(),
            scope_key: "im:oc_contract".to_owned(),
            kind: "final".to_owned(),
            payload_json,
            next_retry_ms: 0,
        })
        .await
        .expect("enqueue outbox")
    {
        OutboxEnqueue::New(row) | OutboxEnqueue::Duplicate(row) => row.id,
    }
}

async fn wait_state(store: &StoreHandle, id: i64, expected: OutboxState) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let row = store
            .outbox_row(id)
            .await
            .expect("outbox read")
            .expect("outbox row");
        let attempt_recorded = expected != OutboxState::Pending || row.attempts >= 1;
        if row.state == expected && attempt_recorded {
            return;
        }
        assert!(Instant::now() < deadline, "outbox classification timeout");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn durable_outbox_honors_all_provider_neutral_delivery_classes() {
    for (index, class, expected) in [
        (1, DeliveryFailureClass::Retryable, OutboxState::Pending),
        (
            2,
            DeliveryFailureClass::Uncertain,
            OutboxState::UncertainDelivery,
        ),
        (3, DeliveryFailureClass::Definitive, OutboxState::Failed),
    ] {
        let store = StoreHandle::open_in_memory().await.expect("store");
        let id = enqueue_reply(&store, &format!("classification-{index}")).await;
        let (_, state) = watch::channel(ConnectionState::Connected);
        let pump = OutboxPump::spawn(
            store.clone(),
            FakeDelivery(class),
            state,
            OutboxPumpConfig {
                retry_base: Duration::from_secs(60),
                retry_max: Duration::from_secs(60),
                poll_interval: Duration::from_millis(10),
                claim_batch: 1,
            },
        );
        wait_state(&store, id, expected).await;
        pump.shutdown().await;
        store.shutdown().await.expect("store shutdown");
    }
}

#[test]
fn channel_debug_views_redact_content_and_opaque_handles() {
    let outbound = OutboundRequest::ReplyText {
        message_id: "om_secret_identifier".to_owned(),
        in_thread: false,
        text: "secret message content".to_owned(),
    };
    let media = MediaRequest {
        message_id: "om_secret_identifier".to_owned(),
        resource_key: "secret_resource_key".to_owned(),
        kind: MediaKind::Image,
    };
    let rendered = format!("{outbound:?} {media:?}");
    assert!(!rendered.contains("secret message content"));
    assert!(!rendered.contains("om_secret_identifier"));
    assert!(!rendered.contains("secret_resource_key"));
}
