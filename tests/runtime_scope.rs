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
    TurnResolution,
};
use secrecy::SecretString;
use semver::Version;
use serde_json::{Value, json};
use tokio::sync::Semaphore;
use tokio::time::{sleep, timeout};

use fakecodex::{FakeFactory, FakeOutcome, test_settings};

#[derive(Default)]
struct RecordingSink {
    rejections: Mutex<Vec<InboundRejectionKind>>,
    finalizations: Mutex<Vec<(i64, lark_codex_bridge::store::TurnResolution, usize)>>,
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
        self.finalizations.lock().expect("finalization lock").push((
            turn.turn_row_id,
            turn.resolution,
            turn.sources.len(),
        ));
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
        DedupOutcome::New(retained) | DedupOutcome::ReplayReceived(retained) => retained,
        duplicate @ DedupOutcome::Duplicate { .. } => {
            panic!("expected retained event, got {duplicate:?}")
        }
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

async fn wait_for_inbound_states(
    store: &StoreHandle,
    namespace: &TenantNamespace,
    event_ids: &[&str],
    expected: InboundEventState,
) {
    timeout(Duration::from_secs(2), async {
        loop {
            let mut ready = true;
            for event_id in event_ids {
                ready &= store
                    .inbound_state(namespace, event_id)
                    .await
                    .expect("inbound state")
                    == Some(expected);
            }
            if ready {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("inbound events reach the expected state");
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
async fn debounce_batch_claims_one_turn_and_uses_the_exact_client_message_id() {
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
        .route(queued_registered(&store, &namespace, inbound.clone()).await)
        .await
        .expect("route to actor");
    router
        .route(queued_registered(&store, &namespace, inbound).await)
        .await
        .expect("route exact replay to actor");
    let second = event("event-allowed-second", "owner-runtime-scope");
    router
        .route(queued_registered(&store, &namespace, second).await)
        .await
        .expect("route second debounce item");

    let start_thread = control.next_request().await;
    assert_eq!(start_thread["method"], "thread/start");
    assert_eq!(start_thread["params"]["cwd"], json!(workspace));
    control
        .respond(&start_thread, thread_result("thread-runtime", &workspace))
        .await;

    let start_turn = control.next_request().await;
    assert_eq!(start_turn["method"], "turn/start");
    assert_eq!(start_turn["params"]["threadId"], "thread-runtime");
    assert_eq!(
        start_turn["params"]["input"]
            .as_array()
            .expect("input array")
            .len(),
        2
    );
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

    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-allowed", "event-allowed-second"],
        InboundEventState::Completed,
    )
    .await;
    assert!(
        store
            .uncertain_turns()
            .await
            .expect("live turns")
            .is_empty()
    );
    let turn_row_id = {
        let finalizations = sink.finalizations.lock().expect("finalizations");
        assert_eq!(finalizations.len(), 1);
        assert_eq!(finalizations[0].2, 2);
        finalizations[0].0
    };
    let row = store
        .turn_row(turn_row_id)
        .await
        .expect("turn row")
        .expect("persisted turn");
    assert_eq!(row.client_message_id, client_message_id);
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn permit_recheck_atomically_rejects_a_stale_event_before_any_rpc() {
    let config = validated_config();
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
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
    let mut inbound = event("event-stale", "owner-runtime-scope");
    inbound.create_time_ms =
        now_ms() - i64::try_from(Duration::from_secs(16 * 60).as_millis()).expect("age fits");
    router
        .route(queued_registered(&store, &namespace, inbound).await)
        .await
        .expect("route stale event");

    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-stale"],
        InboundEventState::Rejected,
    )
    .await;
    assert_eq!(store.outbox_depth().await.expect("outbox").pending, 1);
    assert_eq!(
        *sink.rejections.lock().expect("rejections"),
        vec![InboundRejectionKind::Stale]
    );
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn shutdown_cancels_an_actor_waiting_for_a_supervisor_client() {
    let config = validated_config();
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let supervisor = degraded_supervisor().await;
    let router = Router::start(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        supervisor,
        Arc::new(RecordingSink::default()),
    )
    .await
    .expect("router");
    router
        .route(
            queued_registered(
                &store,
                &namespace,
                event("event-shutdown", "owner-runtime-scope"),
            )
            .await,
        )
        .await
        .expect("route to waiting actor");
    sleep(Duration::from_millis(700)).await;

    timeout(Duration::from_millis(500), router.shutdown())
        .await
        .expect("router shutdown deadline")
        .expect("router shutdown");
    assert_eq!(
        store
            .inbound_state(&namespace, "event-shutdown")
            .await
            .expect("inbound state"),
        Some(InboundEventState::Received)
    );
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn shutdown_durably_resolves_a_running_turn_as_uncertain() {
    let config = validated_config();
    let workspace = config.default_workspace.clone().expect("workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
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
    router
        .route(
            queued_registered(
                &store,
                &namespace,
                event("event-running-shutdown", "owner-runtime-scope"),
            )
            .await,
        )
        .await
        .expect("route event");
    let start_thread = control.next_request().await;
    control
        .respond(
            &start_thread,
            thread_result("thread-running-shutdown", &workspace),
        )
        .await;
    let start_turn = control.next_request().await;
    control
        .respond(
            &start_turn,
            json!({"turn": turn("turn-running-shutdown", "inProgress")}),
        )
        .await;

    timeout(Duration::from_millis(500), router.shutdown())
        .await
        .expect("router shutdown deadline")
        .expect("router shutdown");
    assert_eq!(
        store
            .inbound_state(&namespace, "event-running-shutdown")
            .await
            .expect("inbound state"),
        Some(InboundEventState::Rejected)
    );
    {
        let finalizations = sink.finalizations.lock().expect("finalizations");
        assert_eq!(finalizations.len(), 1);
        assert_eq!(finalizations[0].1, TurnResolution::Uncertain);
    }
    assert!(
        store
            .uncertain_turns()
            .await
            .expect("live turns")
            .is_empty()
    );
    store.shutdown().await.expect("store shutdown");
}
