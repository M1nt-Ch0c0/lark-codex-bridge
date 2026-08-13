mod fakecodex;

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
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
use lark_codex_bridge::limits::{ROUTER_ACTIVE_TURN_HARD_LIMIT, ROUTER_SCOPE_ACTOR_HARD_LIMIT};
use lark_codex_bridge::runtime::intake::TenantNamespace;
use lark_codex_bridge::runtime::policy::AccessPolicy;
use lark_codex_bridge::runtime::router::{RouteError, Router, RouterSettings};
use lark_codex_bridge::runtime::scope::{
    DurableReplySink, InterruptOutcome, ReplySinkError, TurnFinalization,
};
use lark_codex_bridge::store::{
    DedupOutcome, InboundEventState, InboundRejectionKind, NewOutboxRow, StoreHandle,
    TurnResolution,
};
use secrecy::SecretString;
use semver::Version;
use serde_json::{Value, json};
use tokio::sync::{Notify, Semaphore};
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

#[derive(Default)]
struct UnavailableSink {
    attempts: AtomicUsize,
    attempted: Notify,
}

impl UnavailableSink {
    async fn wait_for_attempt(&self) {
        timeout(Duration::from_secs(2), async {
            while self.attempts.load(Ordering::SeqCst) == 0 {
                self.attempted.notified().await;
            }
        })
        .await
        .expect("finalization attempt");
    }
}

impl DurableReplySink for UnavailableSink {
    fn rejection_notice(
        &self,
        event: &InboundEvent,
        _reason: InboundRejectionKind,
    ) -> Result<NewOutboxRow, ReplySinkError> {
        Ok(NewOutboxRow {
            idempotency_key: format!("{}:rejection", event.event_id),
            scope_key: event.scope.to_string(),
            kind: "notice".to_owned(),
            payload_json: "{\"text\":\"rejected\"}".to_owned(),
            next_retry_ms: 0,
        })
    }

    fn finalize(&self, _turn: TurnFinalization) -> BoxFuture<'static, Result<(), ReplySinkError>> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        self.attempted.notify_waiters();
        async { Err(ReplySinkError::Unavailable) }.boxed()
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
    event_in_chat(event_id, sender_id, "chat-runtime-scope")
}

fn event_in_chat(event_id: &str, sender_id: &str, chat_id: &str) -> InboundEvent {
    InboundEvent {
        event_id: event_id.to_owned(),
        message_id: format!("message-{event_id}"),
        chat_id: chat_id.to_owned(),
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
        scope: ScopeKey::Chat(chat_id.to_owned()),
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

async fn assert_invalid_router_settings(config: &BridgeConfig) {
    let policy = AccessPolicy::from_config(config).expect("policy");
    let settings = RouterSettings::from_config(config);
    let store = StoreHandle::open_in_memory().await.expect("store");
    let result = Router::start(
        store.clone(),
        TenantNamespace::from_credentials(&credentials()),
        policy,
        settings,
        degraded_supervisor().await,
        Arc::new(RecordingSink::default()),
    )
    .await;
    assert!(matches!(result, Err(RouteError::InvalidSettings)));
    store.shutdown().await.expect("store shutdown");
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

async fn respond_turn_started(control: &fakecodex::FakeControl, request: &Value, turn_id: &str) {
    control
        .respond(request, json!({"turn": turn(turn_id, "inProgress")}))
        .await;
}

async fn send_turn_completed(
    control: &fakecodex::FakeControl,
    thread_id: &str,
    turn_id: &str,
    status: &str,
) {
    control
        .send_json(json!({
            "method": "turn/completed",
            "params": {
                "threadId": thread_id,
                "turn": turn(turn_id, status)
            }
        }))
        .await;
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

async fn wait_for_running_turn(store: &StoreHandle, codex_turn_id: &str) {
    timeout(Duration::from_secs(2), async {
        loop {
            let turns = store.uncertain_turns().await.expect("live turns");
            if turns.iter().any(|turn| {
                turn.state == lark_codex_bridge::store::TurnState::Running
                    && turn.codex_turn_id.as_deref() == Some(codex_turn_id)
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("turn becomes active");
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
async fn missing_default_workspace_is_a_durable_policy_rejection() {
    let mut config = validated_config();
    config.default_workspace = None;
    config.validate().expect("optional workspace remains valid");
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
    router
        .route(
            queued_registered(
                &store,
                &namespace,
                event("event-no-workspace", "owner-runtime-scope"),
            )
            .await,
        )
        .await
        .expect("route event");

    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-no-workspace"],
        InboundEventState::Rejected,
    )
    .await;
    assert_eq!(
        *sink.rejections.lock().expect("rejections"),
        vec![InboundRejectionKind::Policy]
    );
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn router_rejects_zero_and_hard_cap_runtime_settings() {
    let mut zero = validated_config();
    zero.concurrency.active_turn_permits = 0;
    assert_invalid_router_settings(&zero).await;

    let mut active_over = validated_config();
    active_over.concurrency.active_turn_permits = ROUTER_ACTIVE_TURN_HARD_LIMIT + 1;
    assert_invalid_router_settings(&active_over).await;

    let mut actor_zero = validated_config();
    actor_zero.concurrency.max_scope_actors = 0;
    assert_invalid_router_settings(&actor_zero).await;

    let mut actor_over = validated_config();
    actor_over.concurrency.max_scope_actors = ROUTER_SCOPE_ACTOR_HARD_LIMIT + 1;
    assert_invalid_router_settings(&actor_over).await;
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

#[tokio::test]
async fn message_while_running_waits_then_resumes_the_same_thread() {
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
                event("event-first-turn", "owner-runtime-scope"),
            )
            .await,
        )
        .await
        .expect("route first turn");
    let start_thread = control.next_request().await;
    assert_eq!(start_thread["method"], "thread/start");
    control
        .respond(&start_thread, thread_result("thread-reused", &workspace))
        .await;
    let first_turn = control.next_request().await;
    assert_eq!(first_turn["method"], "turn/start");
    control
        .respond(
            &first_turn,
            json!({"turn": turn("turn-first", "inProgress")}),
        )
        .await;

    router
        .route(
            queued_registered(
                &store,
                &namespace,
                event("event-second-turn", "owner-runtime-scope"),
            )
            .await,
        )
        .await
        .expect("queue second turn");
    control
        .expect_no_request_for(Duration::from_millis(100))
        .await;
    control
        .send_json(json!({
            "method": "turn/completed",
            "params": {
                "threadId": "thread-reused",
                "turn": turn("turn-first", "completed")
            }
        }))
        .await;

    let resume_thread = control.next_request().await;
    assert_eq!(resume_thread["method"], "thread/resume");
    assert_eq!(resume_thread["params"]["threadId"], "thread-reused");
    control
        .respond(&resume_thread, thread_result("thread-reused", &workspace))
        .await;
    let second_turn = control.next_request().await;
    assert_eq!(second_turn["method"], "turn/start");
    assert_eq!(second_turn["params"]["threadId"], "thread-reused");
    control
        .respond(
            &second_turn,
            json!({"turn": turn("turn-second", "inProgress")}),
        )
        .await;
    control
        .send_json(json!({
            "method": "turn/completed",
            "params": {
                "threadId": "thread-reused",
                "turn": turn("turn-second", "completed")
            }
        }))
        .await;

    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-first-turn", "event-second-turn"],
        InboundEventState::Completed,
    )
    .await;
    assert_eq!(sink.finalizations.lock().expect("finalizations").len(), 2);
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn shutdown_cancels_unavailable_finalization_without_clearing_accepted_payload() {
    let config = validated_config();
    let workspace = config.default_workspace.clone().expect("workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = Arc::new(UnavailableSink::default());
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
                event("event-finalization-unavailable", "owner-runtime-scope"),
            )
            .await,
        )
        .await
        .expect("route event");
    let start_thread = control.next_request().await;
    control
        .respond(
            &start_thread,
            thread_result("thread-finalization-unavailable", &workspace),
        )
        .await;
    let start_turn = control.next_request().await;
    control
        .respond(
            &start_turn,
            json!({"turn": turn("turn-finalization-unavailable", "inProgress")}),
        )
        .await;
    control
        .send_json(json!({
            "method": "turn/completed",
            "params": {
                "threadId": "thread-finalization-unavailable",
                "turn": turn("turn-finalization-unavailable", "completed")
            }
        }))
        .await;
    sink.wait_for_attempt().await;

    timeout(Duration::from_millis(500), router.shutdown())
        .await
        .expect("shutdown cancels the finalization retry")
        .expect("router shutdown");
    assert_eq!(
        store
            .inbound_state(&namespace, "event-finalization-unavailable")
            .await
            .expect("inbound state"),
        Some(InboundEventState::Accepted)
    );
    let live_turns = store.uncertain_turns().await.expect("live turns");
    assert_eq!(live_turns.len(), 1);
    assert_eq!(
        live_turns[0].state,
        lark_codex_bridge::store::TurnState::Running
    );
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn global_turn_permit_queues_a_second_scope_and_snapshot_tracks_usage() {
    let mut config = validated_config();
    config.concurrency.active_turn_permits = 1;
    config.validate().expect("single active turn is valid");
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
        sink,
    )
    .await
    .expect("router");

    router
        .route(
            queued_registered(
                &store,
                &namespace,
                event_in_chat("event-scope-one", "owner-runtime-scope", "chat-one"),
            )
            .await,
        )
        .await
        .expect("route first scope");
    let first_thread = control.next_request().await;
    assert_eq!(first_thread["method"], "thread/start");
    control
        .respond(&first_thread, thread_result("thread-one", &workspace))
        .await;
    let first_turn = control.next_request().await;
    assert_eq!(first_turn["method"], "turn/start");
    respond_turn_started(&control, &first_turn, "turn-one").await;
    assert_eq!(router.snapshot().active_turns, 1);

    router
        .route(
            queued_registered(
                &store,
                &namespace,
                event_in_chat("event-scope-two", "owner-runtime-scope", "chat-two"),
            )
            .await,
        )
        .await
        .expect("route second scope");
    control
        .expect_no_request_for(Duration::from_millis(700))
        .await;

    send_turn_completed(&control, "thread-one", "turn-one", "completed").await;
    let second_thread = control.next_request().await;
    assert_eq!(second_thread["method"], "thread/start");
    control
        .respond(&second_thread, thread_result("thread-two", &workspace))
        .await;
    let second_turn = control.next_request().await;
    assert_eq!(second_turn["method"], "turn/start");
    respond_turn_started(&control, &second_turn, "turn-two").await;
    send_turn_completed(&control, "thread-two", "turn-two", "completed").await;
    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-scope-one", "event-scope-two"],
        InboundEventState::Completed,
    )
    .await;
    timeout(Duration::from_secs(2), async {
        while router.snapshot().active_turns != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("active permit is released");
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn debounce_batch_cap_defers_excess_messages_to_the_next_turn() {
    let config = validated_config();
    let workspace = config.default_workspace.clone().expect("workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = Arc::new(RecordingSink::default());
    let mut queued = Vec::new();
    for index in 0..=lark_codex_bridge::limits::TURN_BATCH_MAX_MESSAGES {
        queued.push(
            queued_registered(
                &store,
                &namespace,
                event(
                    format!("event-batch-cap-{index}").as_str(),
                    "owner-runtime-scope",
                ),
            )
            .await,
        );
    }
    let (supervisor, control) = ready_supervisor().await;
    let router = Router::start(
        store.clone(),
        namespace,
        policy,
        settings,
        supervisor,
        sink.clone(),
    )
    .await
    .expect("router");
    for event in queued {
        router.route(event).await.expect("route bounded batch item");
        tokio::task::yield_now().await;
    }

    let first_thread = control.next_request().await;
    assert_eq!(first_thread["method"], "thread/start");
    control
        .respond(&first_thread, thread_result("thread-batch-cap", &workspace))
        .await;
    let first_turn = control.next_request().await;
    assert_eq!(first_turn["method"], "turn/start");
    assert_eq!(
        first_turn["params"]["input"]
            .as_array()
            .expect("first turn inputs")
            .len(),
        lark_codex_bridge::limits::TURN_BATCH_MAX_MESSAGES
    );
    respond_turn_started(&control, &first_turn, "turn-batch-cap-one").await;
    send_turn_completed(
        &control,
        "thread-batch-cap",
        "turn-batch-cap-one",
        "completed",
    )
    .await;

    let resume = control.next_request().await;
    assert_eq!(resume["method"], "thread/resume");
    control
        .respond(&resume, thread_result("thread-batch-cap", &workspace))
        .await;
    let second_turn = control.next_request().await;
    assert_eq!(second_turn["method"], "turn/start");
    assert_eq!(
        second_turn["params"]["input"]
            .as_array()
            .expect("second turn inputs")
            .len(),
        1
    );
    respond_turn_started(&control, &second_turn, "turn-batch-cap-two").await;
    send_turn_completed(
        &control,
        "thread-batch-cap",
        "turn-batch-cap-two",
        "completed",
    )
    .await;
    timeout(Duration::from_secs(2), async {
        while sink.finalizations.lock().expect("finalizations").len() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both turns finalize");
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn oversized_single_message_is_durably_rejected_without_any_rpc() {
    let config = validated_config();
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
    let mut oversized = event("event-oversized-turn-input", "owner-runtime-scope");
    oversized.text = "x".repeat(lark_codex_bridge::limits::TURN_BATCH_TEXT_BYTE_BUDGET + 1);
    router
        .route(queued_registered(&store, &namespace, oversized).await)
        .await
        .expect("route oversized event");

    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-oversized-turn-input"],
        InboundEventState::Rejected,
    )
    .await;
    control
        .expect_no_request_for(Duration::from_millis(100))
        .await;
    assert_eq!(
        *sink.rejections.lock().expect("rejections"),
        vec![InboundRejectionKind::Overloaded]
    );
    assert_eq!(store.outbox_depth().await.expect("outbox").pending, 1);
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn policy_fingerprint_change_archives_the_old_thread_before_starting_a_new_one() {
    let config = validated_config();
    let workspace = config.default_workspace.clone().expect("workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let inbound = event("event-policy-fingerprint-change", "owner-runtime-scope");
    store
        .upsert_scope(&inbound.scope, &workspace, "stale-policy-fingerprint")
        .await
        .expect("seed stale scope policy");
    store
        .record_active_thread(&inbound.scope, "thread-old-policy")
        .await
        .expect("seed old active thread");
    let sink = Arc::new(RecordingSink::default());
    let (supervisor, control) = ready_supervisor().await;
    let router = Router::start(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        supervisor,
        sink,
    )
    .await
    .expect("router");
    router
        .route(queued_registered(&store, &namespace, inbound.clone()).await)
        .await
        .expect("route event");

    let new_thread = control.next_request().await;
    assert_eq!(new_thread["method"], "thread/start");
    control
        .respond(&new_thread, thread_result("thread-new-policy", &workspace))
        .await;
    let start_turn = control.next_request().await;
    assert_eq!(start_turn["method"], "turn/start");
    assert_eq!(start_turn["params"]["threadId"], "thread-new-policy");
    control
        .respond(
            &start_turn,
            json!({"turn": turn("turn-new-policy", "inProgress")}),
        )
        .await;
    control
        .send_json(json!({
            "method": "turn/completed",
            "params": {
                "threadId": "thread-new-policy",
                "turn": turn("turn-new-policy", "completed")
            }
        }))
        .await;
    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-policy-fingerprint-change"],
        InboundEventState::Completed,
    )
    .await;
    let current_scope = store
        .scope_row(&inbound.scope)
        .await
        .expect("scope row")
        .expect("scope remains persisted");
    assert_ne!(current_scope.policy_fingerprint, "stale-policy-fingerprint");
    assert_eq!(
        store
            .active_thread(&inbound.scope)
            .await
            .expect("active thread")
            .expect("new active thread")
            .codex_thread_id,
        "thread-new-policy"
    );
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn interrupt_waits_for_terminal_completion_before_the_next_turn() {
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
        sink,
    )
    .await
    .expect("router");
    let first = event("event-interrupt-first", "owner-runtime-scope");
    let scope = first.scope.clone();
    router
        .route(queued_registered(&store, &namespace, first).await)
        .await
        .expect("route first turn");
    let start_thread = control.next_request().await;
    control
        .respond(&start_thread, thread_result("thread-interrupt", &workspace))
        .await;
    let first_turn = control.next_request().await;
    respond_turn_started(&control, &first_turn, "turn-interrupt-first").await;
    wait_for_running_turn(&store, "turn-interrupt-first").await;

    let (interrupt, ()) = tokio::join!(router.interrupt(&scope), async {
        let request = control.next_request().await;
        assert_eq!(request["method"], "turn/interrupt");
        assert_eq!(request["params"]["threadId"], "thread-interrupt");
        assert_eq!(request["params"]["turnId"], "turn-interrupt-first");
        control.respond(&request, json!({})).await;
    });
    assert_eq!(
        interrupt.expect("interrupt request"),
        InterruptOutcome::Requested
    );

    router
        .route(
            queued_registered(
                &store,
                &namespace,
                event("event-interrupt-second", "owner-runtime-scope"),
            )
            .await,
        )
        .await
        .expect("queue second turn");
    control
        .expect_no_request_for(Duration::from_millis(100))
        .await;
    send_turn_completed(
        &control,
        "thread-interrupt",
        "turn-interrupt-first",
        "interrupted",
    )
    .await;

    let resume = control.next_request().await;
    assert_eq!(resume["method"], "thread/resume");
    control
        .respond(&resume, thread_result("thread-interrupt", &workspace))
        .await;
    let second_turn = control.next_request().await;
    assert_eq!(second_turn["method"], "turn/start");
    respond_turn_started(&control, &second_turn, "turn-interrupt-second").await;
    send_turn_completed(
        &control,
        "thread-interrupt",
        "turn-interrupt-second",
        "completed",
    )
    .await;
    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-interrupt-first"],
        InboundEventState::Rejected,
    )
    .await;
    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-interrupt-second"],
        InboundEventState::Completed,
    )
    .await;
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}
