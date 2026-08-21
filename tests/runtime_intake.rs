use std::sync::Arc;

use futures_util::FutureExt;
use lark_codex_bridge::lark::api::ChatMode;
use lark_codex_bridge::lark::bridge::{IntakeHook, IntakeVerdict};
use lark_codex_bridge::lark::config::TenantBrand;
use lark_codex_bridge::lark::credentials::LarkCredentials;
use lark_codex_bridge::lark::normalize::{InboundEvent, ScopeKey};
use lark_codex_bridge::runtime::intake::{DurableIntake, IntakeRuntime};
use lark_codex_bridge::store::StoreHandle;
use secrecy::SecretString;

fn credentials(app_id: &str, secret: &str) -> LarkCredentials {
    LarkCredentials::new(
        app_id.to_owned(),
        SecretString::from(secret.to_owned()),
        TenantBrand::Feishu,
    )
}

fn event() -> InboundEvent {
    InboundEvent {
        event_id: "event-runtime".to_owned(),
        message_id: "message-runtime".to_owned(),
        chat_id: "chat-runtime".to_owned(),
        sender_id: "sender-runtime-sentinel".to_owned(),
        chat_type: ChatMode::Group,
        thread_id: None,
        root_id: None,
        reply_to_message_id: None,
        text: "runtime-text-sentinel".to_owned(),
        mentions_bot: true,
        mention_all: false,
        sender_is_human: true,
        mentions: Vec::new(),
        parts: Vec::new(),
        resources: Vec::new(),
        message_type: "text".to_owned(),
        create_time_ms: 1,
        scope: ScopeKey::Chat("chat-runtime".to_owned()),
    }
}

fn assert_send<T: Send>() {}
fn assert_send_sync<T: Send + Sync>() {}

#[tokio::test]
async fn prepare_captures_only_store_namespace_and_complete_recovery() {
    assert_send::<IntakeRuntime>();
    assert_send_sync::<IntakeHook>();
    let store = StoreHandle::open_in_memory().await.expect("open");
    let creds = credentials("cli_runtime_prepare", "secret-runtime-sentinel");
    let namespace = lark_codex_bridge::runtime::intake::TenantNamespace::from_credentials(&creds);
    store
        .register_inbound(&namespace, &event())
        .await
        .expect("register");
    let runtime = DurableIntake::prepare(store.clone(), &creds)
        .await
        .expect("prepare");
    let debug = format!("{runtime:?}");
    assert!(debug.contains("recovery_count: 1"));
    assert!(!debug.contains("cli_runtime_prepare"));
    assert!(!debug.contains("secret-runtime-sentinel"));
    assert!(!debug.contains("runtime-text-sentinel"));
    assert!(!debug.contains("sender-runtime-sentinel"));
    store.shutdown().await.expect("shutdown");
}

#[test]
fn injection_seam_is_bound_and_debug_is_redacted() {
    let hook: IntakeHook =
        Arc::new(|_event| async move { Ok(IntakeVerdict::DropDuplicate) }.boxed());
    let creds = credentials("cli_runtime_seam", "secret-seam-sentinel");
    let runtime = IntakeRuntime::try_from_parts(&creds, Vec::new(), hook).expect("runtime");
    let debug = format!("{runtime:?}");
    assert!(debug.contains("recovery_count: 0"));
    assert!(!debug.contains("cli_runtime_seam"));
    assert!(!debug.contains("secret-seam-sentinel"));
}
