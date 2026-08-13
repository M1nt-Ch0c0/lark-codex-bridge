mod fakecodex;

use std::sync::{Arc, Mutex};

use futures_util::{FutureExt, future::BoxFuture};
use lark_codex_bridge::codex::process::{CodexProcessConfig, ProcessError};
use lark_codex_bridge::codex::supervisor::AppServerSupervisor;
use lark_codex_bridge::config::{BridgeConfig, WorkspacePolicy};
use lark_codex_bridge::lark::api::ChatMode;
use lark_codex_bridge::lark::bridge::QueuedInboundEvent;
use lark_codex_bridge::lark::config::TenantBrand;
use lark_codex_bridge::lark::credentials::LarkCredentials;
use lark_codex_bridge::lark::normalize::{InboundEvent, ScopeKey};
use lark_codex_bridge::runtime::intake::TenantNamespace;
use lark_codex_bridge::runtime::policy::AccessPolicy;
use lark_codex_bridge::runtime::router::{Router, RouterSettings};
use lark_codex_bridge::runtime::scope::{DurableReplySink, ReplySinkError, TurnFinalization};
use lark_codex_bridge::store::{
    DedupOutcome, InboundEventState, InboundRejectionKind, NewOutboxRow, StoreHandle,
};
use secrecy::SecretString;
use semver::Version;
use tokio::sync::Semaphore;

use fakecodex::{FakeFactory, FakeOutcome, test_settings};

#[derive(Default)]
struct RecordingSink {
    rejections: Mutex<Vec<InboundRejectionKind>>,
}

impl DurableReplySink for RecordingSink {
    fn rejection_notice(
        &self,
        event: &InboundEvent,
        reason: InboundRejectionKind,
    ) -> Result<NewOutboxRow, ReplySinkError> {
        self.rejections.lock().expect("rejection lock").push(reason);
        Ok(NewOutboxRow {
            idempotency_key: format!("{}:rejection", event.event_id),
            scope_key: event.scope.to_string(),
            kind: "notice".to_owned(),
            payload_json: "{\"text\":\"rejected\"}".to_owned(),
            next_retry_ms: 0,
        })
    }

    fn finalize(&self, _turn: TurnFinalization) -> BoxFuture<'static, Result<(), ReplySinkError>> {
        async { Ok(()) }.boxed()
    }
}

fn credentials() -> LarkCredentials {
    LarkCredentials::new(
        "cli_runtime_scope".to_owned(),
        SecretString::from("scope-secret".to_owned()),
        TenantBrand::Feishu,
    )
}

fn event(event_id: &str, sender_id: &str) -> InboundEvent {
    InboundEvent {
        event_id: event_id.to_owned(),
        message_id: format!("message-{event_id}"),
        chat_id: "chat-runtime-scope".to_owned(),
        sender_id: sender_id.to_owned(),
        chat_type: ChatMode::P2p,
        thread_id: None,
        root_id: None,
        reply_to_message_id: None,
        text: "hello".to_owned(),
        mentions_bot: false,
        mention_all: false,
        resources: Vec::new(),
        message_type: "text".to_owned(),
        create_time_ms: 1,
        scope: ScopeKey::Chat("chat-runtime-scope".to_owned()),
    }
}

fn validated_config() -> BridgeConfig {
    let workspace = std::env::current_dir().expect("current workspace");
    let mut config = BridgeConfig {
        owners: vec!["owner-runtime-scope".to_owned()],
        default_workspace: Some(workspace.clone()),
        workspace: WorkspacePolicy {
            allow_roots: vec![workspace],
            ..WorkspacePolicy::default()
        },
        ..BridgeConfig::default()
    };
    config.validate().expect("valid runtime config");
    config
}

async fn degraded_supervisor() -> lark_codex_bridge::codex::supervisor::SupervisorHandle {
    AppServerSupervisor::start_with_factory(
        CodexProcessConfig::default(),
        Arc::new(FakeFactory::new([FakeOutcome::Error(
            ProcessError::UnsupportedVersion {
                found: Version::new(0, 145, 0),
            },
        )])),
        test_settings(),
    )
    .await
    .expect("supervisor task")
}

async fn queued_registered(
    store: &StoreHandle,
    namespace: &TenantNamespace,
    event: InboundEvent,
) -> QueuedInboundEvent {
    let retained = match store
        .register_inbound(namespace, &event)
        .await
        .expect("register")
    {
        DedupOutcome::New(retained) => retained,
        other => panic!("expected new retained event, got {other:?}"),
    };
    let bytes = retained.retained_bytes();
    let permit = Arc::new(Semaphore::new(bytes))
        .acquire_many_owned(u32::try_from(bytes).expect("retained bytes fit"))
        .await
        .expect("byte permit");
    QueuedInboundEvent {
        event: *retained.into_event(),
        permit,
    }
}

#[tokio::test]
async fn router_rejects_non_owner_with_one_atomic_durable_notice() {
    let config = validated_config();
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let credentials = credentials();
    let namespace = TenantNamespace::from_credentials(&credentials);
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = Arc::new(RecordingSink::default());
    let supervisor = degraded_supervisor().await;
    let router = Router::start(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        supervisor,
        sink.clone(),
    )
    .await
    .expect("router");
    let inbound = event("event-policy", "intruder-runtime-scope");
    router
        .route(queued_registered(&store, &namespace, inbound).await)
        .await
        .expect("route rejection");

    assert_eq!(
        store
            .inbound_state(&namespace, "event-policy")
            .await
            .expect("state"),
        Some(InboundEventState::Rejected)
    );
    assert_eq!(store.outbox_depth().await.expect("outbox").pending, 1);
    assert_eq!(
        *sink.rejections.lock().expect("rejection lock"),
        vec![InboundRejectionKind::Policy]
    );
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}
