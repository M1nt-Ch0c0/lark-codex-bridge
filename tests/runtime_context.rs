use std::{sync::Arc, time::Duration};

use lark_codex_bridge::{
    lark::{
        api::{ChatMode, ResourceKind},
        normalize::{
            InboundEvent, MediaMetadata as InboundMediaMetadata, MediaPart, MentionIdentity,
            MessagePart, PartStatus, ResourceDesc, ScopeKey,
        },
    },
    runtime::context::{
        ActiveBinding, ChatKind, ChatSnapshot, ContextDraft, ContextErrorCode, ContextRegistry,
        ContextRegistryConfig, DraftPart, MediaKind, MediaMetadata, PendingBinding, QuoteDraft,
        QuoteStatus, RevocationReason, SenderSnapshot, ThreadSnapshot, TypedPart,
    },
};
use tokio::sync::Barrier;

fn config(max_contexts: usize, ttl: Duration) -> ContextRegistryConfig {
    ContextRegistryConfig {
        ttl,
        max_contexts,
        max_parts_per_context: 8,
    }
}

fn pending(row: i64) -> PendingBinding {
    PendingBinding {
        codex_thread_id: "thread-a".to_owned(),
        local_turn_row_id: row,
    }
}

fn active(row: i64) -> ActiveBinding {
    ActiveBinding {
        codex_thread_id: "thread-a".to_owned(),
        local_turn_row_id: row,
        codex_turn_id: "turn-a".to_owned(),
    }
}

fn draft(message_id: &str) -> ContextDraft {
    ContextDraft {
        event_id: format!("event-{message_id}"),
        message_id: message_id.to_owned(),
        sender: SenderSnapshot {
            open_id: "ou_sender".to_owned(),
            sender_type: "user".to_owned(),
        },
        chat: ChatSnapshot {
            chat_id: "oc_chat".to_owned(),
            kind: ChatKind::Topic,
        },
        mentions: Vec::new(),
        thread: ThreadSnapshot {
            thread_id: Some("omt_topic".to_owned()),
            root_message_id: Some("om_root".to_owned()),
        },
        quote: None,
        message_type: "image".to_owned(),
        create_time_ms: 123,
        parts: vec![
            DraftPart::Text("look".to_owned()),
            DraftPart::Media {
                kind: MediaKind::Image,
                resource: ResourceDesc {
                    kind: ResourceKind::Image,
                    key: "img_secret_key".to_owned(),
                },
                thumbnail: None,
                metadata: MediaMetadata {
                    mime_type: Some("image/png".to_owned()),
                    ..MediaMetadata::default()
                },
            },
        ],
    }
}

#[test]
fn pending_activation_resolve_and_revoke_form_one_way_lifecycle() {
    let registry = ContextRegistry::new(config(4, Duration::from_secs(60))).expect("registry");
    let binding = pending(7);
    let registered = registry
        .register_pending(binding.clone(), draft("om_one"))
        .expect("register");

    let error = registry
        .resolve(&registered.context_id, &active(7))
        .expect_err("pending contexts are unavailable");
    assert_eq!(error.code, ContextErrorCode::Unavailable);
    assert!(error.retryable);

    registry
        .activate(&registered.context_id, &binding, "turn-a")
        .expect("activate");
    registry
        .activate(&registered.context_id, &binding, "turn-a")
        .expect("identical activation is idempotent");
    let snapshot = registry
        .resolve(&registered.context_id, &active(7))
        .expect("resolve");
    assert_eq!(snapshot.sender.open_id, "ou_sender");
    assert_eq!(snapshot.chat.chat_id, "oc_chat");
    assert_eq!(snapshot.thread.thread_id.as_deref(), Some("omt_topic"));

    registry
        .revoke(
            &registered.context_id,
            &binding,
            RevocationReason::Completed,
        )
        .expect("revoke");
    let error = registry
        .resolve(&registered.context_id, &active(7))
        .expect_err("revocation is terminal");
    assert_eq!(error.code, ContextErrorCode::Unavailable);
    assert!(!error.retryable);
}

#[test]
fn media_key_is_hidden_and_handle_is_bound_to_exact_context_and_turn() {
    let registry = ContextRegistry::new(config(4, Duration::from_secs(60))).expect("registry");
    let first = registry
        .register_pending(pending(1), draft("om_one"))
        .expect("first");
    let second = registry
        .register_pending(pending(2), draft("om_two"))
        .expect("second");
    registry
        .activate(&first.context_id, &pending(1), "turn-a")
        .expect("activate first");
    registry
        .activate(&second.context_id, &pending(2), "turn-a")
        .expect("activate second");

    let snapshot = registry
        .resolve(&first.context_id, &active(1))
        .expect("snapshot");
    let serialized = serde_json::to_string(&snapshot).expect("serialize");
    assert!(!serialized.contains("img_secret_key"));
    let TypedPart::Media { handle, .. } = &snapshot.parts[1] else {
        panic!("media part")
    };
    assert!(handle.as_str().starts_with("bmedia_"));

    let resource = registry
        .authorize_media(&first.context_id, handle, &active(1))
        .expect("authorized");
    assert_eq!(resource.message_id, "om_one");
    assert_eq!(resource.local_turn_row_id, 1);
    assert_eq!(resource.resource.key, "img_secret_key");

    let error = registry
        .authorize_media(&second.context_id, handle, &active(2))
        .expect_err("handle is scoped to its context");
    assert_eq!(error.code, ContextErrorCode::Unsupported);

    let mut wrong_turn = active(1);
    wrong_turn.codex_turn_id = "turn-other".to_owned();
    let error = registry
        .authorize_media(&first.context_id, handle, &wrong_turn)
        .expect_err("handle is scoped to its turn");
    assert_eq!(error.code, ContextErrorCode::Forbidden);

    assert!(!resource.is_cancelled());
    assert_eq!(
        registry.revoke_turn(&pending(1), RevocationReason::Cancelled),
        1
    );
    assert!(
        resource.is_cancelled(),
        "revocation must cancel already-authorized media work"
    );
}

#[test]
fn quoted_media_handle_reads_from_the_parent_and_debug_redacts_keys_and_text() {
    let registry = ContextRegistry::new(config(4, Duration::from_secs(60))).expect("registry");
    let mut context = draft("om_trigger");
    context.parts = vec![DraftPart::Text("trigger secret text".to_owned())];
    context.quote = Some(QuoteDraft {
        message_id: "om_parent".to_owned(),
        message_type: Some("image".to_owned()),
        status: QuoteStatus::Available,
        parts: vec![DraftPart::Media {
            kind: MediaKind::Image,
            resource: ResourceDesc {
                kind: ResourceKind::Image,
                key: "quoted_secret_key".to_owned(),
            },
            thumbnail: None,
            metadata: MediaMetadata::default(),
        }],
    });
    let debug = format!("{context:?}");
    assert!(!debug.contains("trigger secret text"));
    assert!(!debug.contains("quoted_secret_key"));

    let registered = registry
        .register_pending(pending(41), context)
        .expect("register quoted context");
    registry
        .activate(&registered.context_id, &pending(41), "turn-a")
        .expect("activate quoted context");
    let snapshot = registry
        .resolve(&registered.context_id, &active(41))
        .expect("resolve quoted context");
    assert_eq!(
        snapshot.quote.as_ref().expect("quote").status,
        QuoteStatus::Available
    );
    let TypedPart::Media { handle, .. } = &snapshot.quote.as_ref().expect("quote").parts[0] else {
        panic!("quoted media handle")
    };
    let json = serde_json::to_string(&snapshot).expect("serialize");
    assert!(!json.contains("quoted_secret_key"));
    let authorized = registry
        .authorize_media(&registered.context_id, handle, &active(41))
        .expect("authorize quoted media");
    assert_eq!(authorized.message_id, "om_parent");
    assert_eq!(authorized.resource.key, "quoted_secret_key");
}

#[test]
fn tool_envelope_can_atomically_activate_a_pending_context() {
    let registry = ContextRegistry::new(config(4, Duration::from_secs(60))).expect("registry");
    let binding = pending(23);
    let registered = registry
        .register_pending(binding.clone(), draft("om_race"))
        .expect("register");

    let snapshot = registry
        .resolve_for_tool(&registered.context_id, "thread-a", "turn-raced")
        .expect("tool request activates pending context");
    assert_eq!(snapshot.message_id, "om_race");
    registry
        .activate(&registered.context_id, &binding, "turn-raced")
        .expect("later explicit activation is idempotent");

    let error = registry
        .resolve_for_tool(&registered.context_id, "thread-a", "turn-other")
        .expect_err("second turn cannot reuse capability");
    assert_eq!(error.code, ContextErrorCode::Forbidden);
}

#[test]
fn inbound_rich_parts_become_opaque_typed_context_parts() {
    let event = InboundEvent {
        event_id: "event-rich".to_owned(),
        message_id: "om_rich".to_owned(),
        chat_id: "oc_rich".to_owned(),
        sender_id: "ou_sender".to_owned(),
        chat_type: ChatMode::Group,
        thread_id: None,
        root_id: None,
        reply_to_message_id: None,
        text: String::new(),
        mentions_bot: false,
        mention_all: false,
        sender_is_human: true,
        mentions: vec![MentionIdentity {
            key: Some("@_user_1".to_owned()),
            open_id: Some("ou_mentioned".to_owned()),
            user_id: None,
            union_id: None,
            name: Some("Mentioned".to_owned()),
        }],
        parts: vec![
            MessagePart::Video(MediaPart {
                key: Some("file_video_secret".to_owned()),
                thumbnail_key: Some("image_thumbnail_secret".to_owned()),
                metadata: InboundMediaMetadata {
                    file_name: Some("clip.mp4".to_owned()),
                    mime_type: Some("video/mp4".to_owned()),
                    size_bytes: Some(10),
                    duration_ms: Some(20),
                    transcript: None,
                },
                status: PartStatus::Available,
            }),
            MessagePart::Card {
                status: PartStatus::Unsupported,
            },
        ],
        resources: Vec::new(),
        message_type: "video".to_owned(),
        create_time_ms: 456,
        scope: ScopeKey::Chat("oc_rich".to_owned()),
    };
    let registry = ContextRegistry::new(config(2, Duration::from_secs(60))).expect("registry");
    let registered = registry
        .register_pending(pending(31), ContextDraft::from_inbound(&event))
        .expect("register rich event");
    let snapshot = registry
        .resolve_for_tool(&registered.context_id, "thread-a", "turn-rich")
        .expect("resolve rich event");
    assert_eq!(
        snapshot.mentions[0].open_id.as_deref(),
        Some("ou_mentioned")
    );
    let TypedPart::Media {
        handle,
        thumbnail_handle: Some(thumbnail),
        ..
    } = &snapshot.parts[0]
    else {
        panic!("video with thumbnail")
    };
    let video = registry
        .authorize_media_for_tool(&registered.context_id, handle, "thread-a", "turn-rich")
        .expect("video grant");
    assert_eq!(video.media_kind, MediaKind::Video);
    let thumbnail = registry
        .authorize_media_for_tool(&registered.context_id, thumbnail, "thread-a", "turn-rich")
        .expect("thumbnail grant");
    assert_eq!(thumbnail.media_kind, MediaKind::Image);
    assert!(matches!(snapshot.parts[1], TypedPart::Unsupported { .. }));

    let json = serde_json::to_string(&snapshot).expect("serialize");
    assert!(!json.contains("file_video_secret"));
    assert!(!json.contains("image_thumbnail_secret"));
}

#[tokio::test]
async fn ttl_and_capacity_are_enforced() {
    let registry = ContextRegistry::new(config(1, Duration::from_millis(20))).expect("registry");
    let first = registry
        .register_pending(pending(1), draft("om_one"))
        .expect("first");
    let error = registry
        .register_pending(pending(2), draft("om_two"))
        .expect_err("active bounds reject unsafe eviction");
    assert_eq!(error.code, ContextErrorCode::CapacityExceeded);

    tokio::time::sleep(Duration::from_millis(40)).await;
    let error = registry
        .resolve(&first.context_id, &active(1))
        .expect_err("expired");
    assert_eq!(error.code, ContextErrorCode::Unavailable);

    registry
        .register_pending(pending(2), draft("om_two"))
        .expect("expired tombstone is an eviction candidate");
    assert_eq!(registry.stats().total, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_resolution_is_safe_and_revocation_wins_terminally() {
    let registry =
        Arc::new(ContextRegistry::new(config(4, Duration::from_secs(60))).expect("registry"));
    let binding = pending(11);
    let registered = registry
        .register_pending(binding.clone(), draft("om_concurrent"))
        .expect("register");
    registry
        .activate(&registered.context_id, &binding, "turn-a")
        .expect("activate");

    let barrier = Arc::new(Barrier::new(17));
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let registry = Arc::clone(&registry);
        let barrier = Arc::clone(&barrier);
        let context_id = registered.context_id.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            registry.resolve(&context_id, &active(11))
        }));
    }
    barrier.wait().await;
    for task in tasks {
        let snapshot = task.await.expect("join").expect("concurrent resolve");
        assert_eq!(snapshot.message_id, "om_concurrent");
    }

    assert_eq!(
        registry.revoke_turn(&binding, RevocationReason::Cancelled),
        1
    );
    assert_eq!(
        registry.revoke_turn(&binding, RevocationReason::Cancelled),
        0
    );
    let error = registry
        .resolve(&registered.context_id, &active(11))
        .expect_err("revoked");
    assert_eq!(error.code, ContextErrorCode::Unavailable);
    assert_eq!(registry.stats().media_grants, 0);
}
