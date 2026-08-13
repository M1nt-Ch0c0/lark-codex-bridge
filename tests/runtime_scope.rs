mod fakecodex;

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
use serde_json::{Value, json};
use tokio::sync::Semaphore;
use tokio::time::timeout;

use fakecodex::{FakeFactory, FakeOutcome, test_settings};

#[derive(Default)]
struct RecordingSink {
    rejections: Mutex<Vec<InboundRejectionKind>>,
    finalizations: Mutex<Vec<(i64, lark_codex_bridge::store::TurnResolution)>>,
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

    fn finalize(&self, turn: TurnFinalization) -> BoxFuture<'static, Result<(), ReplySinkError>> {
        self.finalizations
            .lock()
            .expect("finalization lock")
            .push((turn.turn_row_id, turn.resolution));
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
        create_time_ms: now_ms(),
        scope: ScopeKey::Chat("chat-runtime-scope".to_owned()),
    }
}

fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
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

async fn ready_supervisor() -> (
    lark_codex_bridge::codex::supervisor::SupervisorHandle,
    fakecodex::FakeControl,
) {
    let (outcome, control) = FakeFactory::ready();
    let supervisor = AppServerSupervisor::start_with_factory(
        CodexProcessConfig::default(),
        Arc::new(FakeFactory::new([outcome])),
        test_settings(),
    )
    .await
    .expect("ready supervisor");
    (supervisor, control)
}

fn thread(thread_id: &str, cwd: &std::path::Path) -> Value {
    json!({
        "id": thread_id,
        "sessionId": thread_id,
        "preview": "",
        "modelProvider": "openai",
        "createdAt": 1_786_478_400_i64,
        "updatedAt": 1_786_478_400_i64,
        "status": {"type": "idle"},
        "ephemeral": false,
        "turns": [],
        "source": "appServer",
        "cliVersion": "0.146.0",
        "cwd": cwd
    })
}

fn thread_result(thread_id: &str, cwd: &std::path::Path) -> Value {
    json!({
        "thread": thread(thread_id, cwd),
        "model": "gpt-5.6",
        "modelProvider": "openai",
        "cwd": cwd,
        "approvalPolicy": "on-request",
        "approvalsReviewer": "user",
        "sandbox": {
            "type": "workspaceWrite",
            "writableRoots": [cwd],
            "networkAccess": false,
            "excludeTmpdirEnvVar": false,
            "excludeSlashTmp": false
        }
    })
}

fn turn(turn_id: &str, status: &str) -> Value {
    json!({
        "id": turn_id,
        "items": [],
        "status": status,
        "startedAt": 1_786_478_401_i64,
        "completedAt": if status == "inProgress" { Value::Null } else { json!(1_786_478_402_i64) },
        "durationMs": if status == "inProgress" { Value::Null } else { json!(1_500_i64) },
        "error": null
    })
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

#[tokio::test]
async fn allowed_event_claims_one_turn_and_uses_the_exact_client_message_id() {
    let config = validated_config();
    let workspace = config
        .default_workspace
        .clone()
        .expect("validated default workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let credentials = credentials();
    let namespace = TenantNamespace::from_credentials(&credentials);
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = Arc::new(RecordingSink::default());
    let (supervisor, control) = ready_supervisor().await;
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
    let inbound = event("event-allowed", "owner-runtime-scope");
    router
        .route(queued_registered(&store, &namespace, inbound).await)
        .await
        .expect("route to actor");

    let start_thread = control.next_request().await;
    assert_eq!(start_thread["method"], "thread/start");
    assert_eq!(start_thread["params"]["cwd"], json!(workspace));
    control
        .respond(&start_thread, thread_result("thread-runtime", &workspace))
        .await;

    let start_turn = control.next_request().await;
    assert_eq!(start_turn["method"], "turn/start");
    assert_eq!(start_turn["params"]["threadId"], "thread-runtime");
    assert_eq!(start_turn["params"]["input"][0]["text"], "hello");
    let client_message_id = start_turn["params"]["clientUserMessageId"]
        .as_str()
        .expect("client message id");
    assert!(!client_message_id.is_empty());
    control
        .respond(
            &start_turn,
            json!({"turn": turn("turn-runtime", "inProgress")}),
        )
        .await;
    control
        .send_json(json!({
            "method": "turn/completed",
            "params": {
                "threadId": "thread-runtime",
                "turn": turn("turn-runtime", "completed")
            }
        }))
        .await;

    timeout(Duration::from_secs(2), async {
        loop {
            if store
                .inbound_state(&namespace, "event-allowed")
                .await
                .expect("inbound state")
                == Some(InboundEventState::Completed)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actor completes the turn");
    assert!(
        store
            .uncertain_turns()
            .await
            .expect("live turns")
            .is_empty()
    );
    assert_eq!(sink.finalizations.lock().expect("finalizations").len(), 1);
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}
