mod fakecodex;

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use fs2::FileExt;
use futures_util::{FutureExt, future::BoxFuture};
use lark_codex_bridge::codex::process::{CodexProcessConfig, ProcessError};
use lark_codex_bridge::codex::supervisor::AppServerSupervisor;
use lark_codex_bridge::config::{AsrSection, BridgeConfig, WorkspacePolicy};
use lark_codex_bridge::lark::api::{ChatMode, ResourceKind};
use lark_codex_bridge::lark::bridge::QueuedInboundEvent;
use lark_codex_bridge::lark::config::TenantBrand;
use lark_codex_bridge::lark::credentials::LarkCredentials;
use lark_codex_bridge::lark::normalize::{
    InboundEvent, MediaMetadata, MediaPart, MentionIdentity, MessagePart, PartStatus, ResourceDesc,
    ScopeKey,
};
use lark_codex_bridge::limits::{
    ROUTER_ACTIVE_TURN_HARD_LIMIT, ROUTER_COMMAND_BYTE_BUDGET, ROUTER_RETRY_CAPACITY,
    ROUTER_SCOPE_ACTOR_HARD_LIMIT, SCOPE_MAILBOX_CAPACITY,
};
use lark_codex_bridge::runtime::attachments::{
    AttachError, AttachmentCache, AttachmentLimits, ResourceDownloader,
};
use lark_codex_bridge::runtime::context::{
    ContextRegistry, ContextRegistryConfig, DraftPart, MediaKind,
    MediaMetadata as ContextMediaMetadata, QuoteDraft, QuoteStatus,
};
use lark_codex_bridge::runtime::intake::TenantNamespace;
use lark_codex_bridge::runtime::policy::AccessPolicy;
use lark_codex_bridge::runtime::quote::{QuoteRequest, QuoteResolver};
use lark_codex_bridge::runtime::router::{RouteError, Router, RouterSettings};
use lark_codex_bridge::runtime::scope::{
    DurableReplySink, InterruptOutcome, ReplySinkError, ScopeState, TurnFinalization, TurnProgress,
};
use lark_codex_bridge::store::{
    BeginTurnOutcome, DedupOutcome, InboundEventState, InboundKey, InboundRejectionKind,
    InboundTerminal, NewOutboxRow, NewTurnRow, StoreHandle, TurnResolution, TurnState,
};
use secrecy::SecretString;
use semver::Version;
use serde_json::{Value, json};
use tempfile::tempdir;
use tokio::sync::{Notify, Semaphore};
use tokio::time::{sleep, timeout};

use fakecodex::{FakeFactory, FakeOutcome, test_settings};

// Process-heavy ASR cases share the host process table and system temp root.
// Serializing only those cases keeps their short fake-RPC deadlines
// deterministic while the rest of this large integration suite stays parallel.
static ASR_SUBPROCESS_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn lock_asr_process_tests() -> std::fs::File {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(std::env::temp_dir().join("lark-codex-bridge-asr-process-tests.lock"))
        .expect("open shared ASR process-test lock");
    file.lock_exclusive()
        .expect("lock shared ASR process-test file");
    file
}

#[derive(Default)]
struct RecordingSink {
    rejections: Mutex<Vec<InboundRejectionKind>>,
    progress: Mutex<Vec<(i64, u32, usize)>>,
    finalizations: Mutex<Vec<(i64, lark_codex_bridge::store::TurnResolution, usize)>>,
}

struct StaticAttachmentDownloader;

#[derive(Default)]
struct RecordingAttachmentDownloader {
    calls: Arc<Mutex<Vec<(String, String)>>>,
}

#[derive(Default)]
struct MeteredAttachmentDownloader {
    calls: Arc<Mutex<Vec<(String, String)>>>,
}

#[derive(Default)]
struct RecordingQuoteResolver {
    calls: Arc<Mutex<Vec<(String, String)>>>,
}

#[derive(Default)]
struct RecordingAudioQuoteResolver {
    calls: Arc<Mutex<Vec<(String, String)>>>,
}

struct PendingAttachmentDownloader {
    started: Arc<AtomicUsize>,
    started_notify: Arc<Notify>,
}

struct RemovingWorkspaceDownloader {
    workspace: std::path::PathBuf,
}

#[derive(Default)]
struct YieldingRecordingSink {
    finalizations: Arc<Mutex<Vec<(i64, TurnResolution)>>>,
}

impl ResourceDownloader for StaticAttachmentDownloader {
    fn download(
        &self,
        _message_id: &str,
        key: &str,
        _kind: ResourceKind,
    ) -> BoxFuture<'static, Result<Bytes, AttachError>> {
        let bytes = match key {
            "img_key" => Bytes::from_static(b"fake-image-bytes"),
            "file_key" => Bytes::from_static(b"fake-file-bytes"),
            "aud_key" => Bytes::from_static(b"fake-opus-bytes"),
            _ => Bytes::from_static(b"fallback-attachment"),
        };
        async move { Ok(bytes) }.boxed()
    }
}

impl ResourceDownloader for RecordingAttachmentDownloader {
    fn download(
        &self,
        message_id: &str,
        key: &str,
        _kind: ResourceKind,
    ) -> BoxFuture<'static, Result<Bytes, AttachError>> {
        self.calls
            .lock()
            .expect("download calls")
            .push((message_id.to_owned(), key.to_owned()));
        async { Ok(Bytes::from_static(b"quoted-or-pending-image")) }.boxed()
    }
}

impl ResourceDownloader for MeteredAttachmentDownloader {
    fn download(
        &self,
        message_id: &str,
        key: &str,
        _kind: ResourceKind,
    ) -> BoxFuture<'static, Result<Bytes, AttachError>> {
        self.calls
            .lock()
            .expect("download calls")
            .push((message_id.to_owned(), key.to_owned()));
        let bytes = match key {
            "meter_key_0" => Bytes::from_static(b"0000"),
            "meter_key_1" => Bytes::from_static(b"1111"),
            _ => Bytes::from_static(b"2222"),
        };
        async move { Ok(bytes) }.boxed()
    }
}

impl QuoteResolver for RecordingQuoteResolver {
    fn resolve(&self, request: QuoteRequest) -> BoxFuture<'static, QuoteDraft> {
        self.calls
            .lock()
            .expect("quote calls")
            .push((request.parent_message_id.clone(), request.chat_id));
        async move {
            QuoteDraft {
                message_id: request.parent_message_id,
                message_type: Some("image".to_owned()),
                status: QuoteStatus::Available,
                parts: vec![DraftPart::Media {
                    kind: MediaKind::Image,
                    resource: ResourceDesc {
                        kind: ResourceKind::Image,
                        key: "quoted_key".to_owned(),
                    },
                    thumbnail: None,
                    metadata: ContextMediaMetadata::default(),
                }],
            }
        }
        .boxed()
    }
}

impl QuoteResolver for RecordingAudioQuoteResolver {
    fn resolve(&self, request: QuoteRequest) -> BoxFuture<'static, QuoteDraft> {
        self.calls
            .lock()
            .expect("quote calls")
            .push((request.parent_message_id.clone(), request.chat_id));
        async move {
            QuoteDraft {
                message_id: request.parent_message_id,
                message_type: Some("audio".to_owned()),
                status: QuoteStatus::Available,
                parts: vec![DraftPart::Media {
                    kind: MediaKind::Audio,
                    resource: ResourceDesc {
                        kind: ResourceKind::File,
                        key: "quoted_audio_key".to_owned(),
                    },
                    thumbnail: None,
                    metadata: ContextMediaMetadata {
                        duration_ms: Some(800),
                        ..ContextMediaMetadata::default()
                    },
                }],
            }
        }
        .boxed()
    }
}

impl ResourceDownloader for PendingAttachmentDownloader {
    fn download(
        &self,
        _message_id: &str,
        _key: &str,
        _kind: ResourceKind,
    ) -> BoxFuture<'static, Result<Bytes, AttachError>> {
        let started = Arc::clone(&self.started);
        let started_notify = Arc::clone(&self.started_notify);
        async move {
            started.fetch_add(1, Ordering::SeqCst);
            started_notify.notify_waiters();
            std::future::pending::<Result<Bytes, AttachError>>().await
        }
        .boxed()
    }
}

impl ResourceDownloader for RemovingWorkspaceDownloader {
    fn download(
        &self,
        _message_id: &str,
        _key: &str,
        _kind: ResourceKind,
    ) -> BoxFuture<'static, Result<Bytes, AttachError>> {
        let workspace = self.workspace.clone();
        async move {
            std::fs::remove_dir(&workspace).expect("remove empty workspace during download");
            Ok(Bytes::from_static(b"workspace-race-attachment"))
        }
        .boxed()
    }
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

    fn progress(&self, progress: TurnProgress) -> BoxFuture<'static, Result<(), ReplySinkError>> {
        self.progress.lock().expect("progress lock").push((
            progress.turn_row_id,
            progress.sequence,
            progress.text.chars().count(),
        ));
        async { Ok(()) }.boxed()
    }
}

impl DurableReplySink for YieldingRecordingSink {
    fn rejection_notice(
        &self,
        event: &InboundEvent,
        _reason: InboundRejectionKind,
    ) -> Result<NewOutboxRow, ReplySinkError> {
        Ok(NewOutboxRow {
            idempotency_key: format!("{}:yielding-rejection", event.event_id),
            scope_key: event.scope.to_string(),
            kind: "notice".to_owned(),
            payload_json: "{\"text\":\"rejected\"}".to_owned(),
            next_retry_ms: 0,
        })
    }

    fn finalize(&self, turn: TurnFinalization) -> BoxFuture<'static, Result<(), ReplySinkError>> {
        let finalizations = Arc::clone(&self.finalizations);
        async move {
            tokio::task::yield_now().await;
            finalizations
                .lock()
                .expect("yielding finalization lock")
                .push((turn.turn_row_id, turn.resolution));
            Ok(())
        }
        .boxed()
    }
}

#[derive(Default)]
struct UnavailableSink {
    attempts: AtomicUsize,
    attempted: Notify,
}

#[derive(Default)]
struct RetryOnceRejectionSink {
    attempts: AtomicUsize,
}

#[derive(Default)]
struct UnavailableRejectionSink;

impl DurableReplySink for UnavailableRejectionSink {
    fn rejection_notice(
        &self,
        _event: &InboundEvent,
        _reason: InboundRejectionKind,
    ) -> Result<NewOutboxRow, ReplySinkError> {
        Err(ReplySinkError::Unavailable)
    }

    fn finalize(&self, _turn: TurnFinalization) -> BoxFuture<'static, Result<(), ReplySinkError>> {
        async { Ok(()) }.boxed()
    }
}

impl DurableReplySink for RetryOnceRejectionSink {
    fn rejection_notice(
        &self,
        event: &InboundEvent,
        reason: InboundRejectionKind,
    ) -> Result<NewOutboxRow, ReplySinkError> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err(ReplySinkError::Unavailable);
        }
        Ok(NewOutboxRow {
            idempotency_key: format!("{}:retry:{reason:?}", event.event_id),
            scope_key: event.scope.to_string(),
            kind: "notice".to_owned(),
            payload_json: "{\"text\":\"rejected after retry\"}".to_owned(),
            next_retry_ms: 0,
        })
    }

    fn finalize(&self, _turn: TurnFinalization) -> BoxFuture<'static, Result<(), ReplySinkError>> {
        async { Ok(()) }.boxed()
    }
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
        sender_is_human: true,
        mentions: Vec::new(),
        parts: Vec::new(),
        resources: Vec::new(),
        message_type: "text".to_owned(),
        create_time_ms: now_ms(),
        scope: ScopeKey::Chat(chat_id.to_owned()),
    }
}

fn image_event(event_id: &str, key: &str) -> InboundEvent {
    let mut inbound = event(event_id, "owner-runtime-scope");
    inbound.text.clear();
    "image".clone_into(&mut inbound.message_type);
    inbound.parts = vec![MessagePart::Image(MediaPart {
        key: Some(key.to_owned()),
        thumbnail_key: None,
        metadata: MediaMetadata::default(),
        status: PartStatus::Available,
    })];
    inbound.resources = vec![ResourceDesc {
        kind: ResourceKind::Image,
        key: key.to_owned(),
    }];
    inbound
}

fn context_reference(input: &Value) -> Value {
    let reference = input["text"].as_str().expect("context input text");
    let payload = reference
        .strip_prefix("<bridge_context>")
        .and_then(|value| value.strip_suffix("</bridge_context>"))
        .expect("context envelope");
    serde_json::from_str(payload).expect("context reference JSON")
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

#[derive(Clone, Copy, Debug)]
enum DebounceInvalidator {
    ExplicitQuote,
    ResetCommand,
    ControlInterrupt,
}

#[allow(clippy::too_many_lines)]
async fn assert_debounce_invalidates_reserved_media(mode: DebounceInvalidator) {
    let config = validated_config();
    let workspace = config.default_workspace.clone().expect("workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config).with_test_timings(
        Duration::from_millis(300),
        Duration::from_secs(60),
        Duration::from_millis(10),
    );
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let temp = tempdir().expect("tempdir");
    let cache = Arc::new(
        AttachmentCache::open(
            &temp.path().join("debounce-invalidation-cache"),
            store.clone(),
            Arc::new(StaticAttachmentDownloader),
            AttachmentLimits::default(),
        )
        .expect("cache"),
    );
    let contexts = Arc::new(ContextRegistry::default());
    let resolver = Arc::new(RecordingQuoteResolver::default());
    let quote_calls = Arc::clone(&resolver.calls);
    let (supervisor, control) = ready_supervisor().await;
    let router = Router::start_with_contexts_and_quotes(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        supervisor,
        Arc::new(RecordingSink::default()),
        Arc::clone(&cache),
        contexts,
        resolver,
    )
    .await
    .expect("router");
    let scope = ScopeKey::Chat("chat-runtime-scope".to_owned());
    router
        .route(
            queued_registered(
                &store,
                &namespace,
                image_event("debounce-reserved-media", "debounce_reserved_key"),
            )
            .await,
        )
        .await
        .expect("stage media");
    wait_for_inbound_states(
        &store,
        &namespace,
        &["debounce-reserved-media"],
        InboundEventState::Completed,
    )
    .await;
    router
        .route(
            queued_registered(
                &store,
                &namespace,
                event("debounce-reserving-trigger", "owner-runtime-scope"),
            )
            .await,
        )
        .await
        .expect("route reserving trigger");
    timeout(Duration::from_secs(2), async {
        loop {
            if router
                .scope_snapshot(&scope)
                .await
                .expect("snapshot")
                .is_some_and(|snapshot| {
                    snapshot.state == ScopeState::Debouncing && snapshot.pending_media == 0
                })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("ordinary text holds the reservation during debounce");

    let invalidator_id = match mode {
        DebounceInvalidator::ExplicitQuote => {
            let mut explicit = event("debounce-explicit-quote", "owner-runtime-scope");
            "prefer this explicit quote".clone_into(&mut explicit.text);
            explicit.reply_to_message_id = Some("om_parent".to_owned());
            router
                .route(queued_registered(&store, &namespace, explicit).await)
                .await
                .expect("route explicit quote");
            Some("debounce-explicit-quote")
        }
        DebounceInvalidator::ResetCommand => {
            let mut reset = event("debounce-reset-command", "owner-runtime-scope");
            "/new".clone_into(&mut reset.text);
            router
                .route(queued_registered(&store, &namespace, reset).await)
                .await
                .expect("route reset");
            Some("debounce-reset-command")
        }
        DebounceInvalidator::ControlInterrupt => {
            assert_eq!(
                router.interrupt(&scope).await.expect("interrupt"),
                InterruptOutcome::NoActiveTurn
            );
            None
        }
    };

    let start_thread = control.next_request().await;
    control
        .respond(
            &start_thread,
            thread_result("thread-debounce-invalidation", &workspace),
        )
        .await;
    let start_turn = control.next_request().await;
    let context_references = start_turn["params"]["input"]
        .as_array()
        .expect("inputs")
        .iter()
        .filter_map(|input| input["text"].as_str())
        .filter_map(|text| {
            text.strip_prefix("<bridge_context>")
                .and_then(|value| value.strip_suffix("</bridge_context>"))
        })
        .map(|reference| serde_json::from_str::<Value>(reference).expect("context JSON"))
        .collect::<Vec<_>>();
    assert!(
        context_references
            .iter()
            .all(|reference| reference["wake"] != "pending_media"),
        "{mode:?} must invalidate the reservation already held by the batch"
    );
    assert_eq!(
        quote_calls.lock().expect("quote calls").len(),
        usize::from(matches!(mode, DebounceInvalidator::ExplicitQuote))
    );
    respond_turn_started(&control, &start_turn, "turn-debounce-invalidation").await;
    send_turn_completed(
        &control,
        "thread-debounce-invalidation",
        "turn-debounce-invalidation",
        "completed",
    )
    .await;
    let mut completed = vec!["debounce-reserving-trigger"];
    if let Some(invalidator_id) = invalidator_id {
        completed.push(invalidator_id);
    }
    wait_for_inbound_states(&store, &namespace, &completed, InboundEventState::Completed).await;
    assert_eq!(
        router
            .scope_snapshot(&scope)
            .await
            .expect("snapshot")
            .expect("actor")
            .pending_media,
        0
    );
    router.shutdown().await.expect("shutdown");
    drop(cache);
    store.shutdown().await.expect("store shutdown");
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

async fn restarting_supervisor() -> (
    lark_codex_bridge::codex::supervisor::SupervisorHandle,
    fakecodex::FakeControl,
    fakecodex::FakeControl,
) {
    let (first, first_control) = FakeFactory::ready();
    let (second, second_control) = FakeFactory::ready();
    let supervisor = AppServerSupervisor::start_with_factory(
        CodexProcessConfig::default(),
        Arc::new(FakeFactory::new([first, second])),
        test_settings(),
    )
    .await
    .expect("restarting supervisor");
    (supervisor, first_control, second_control)
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

async fn queued_synthetic(event: InboundEvent, retained_bytes: usize) -> QueuedInboundEvent {
    let permit = Arc::new(Semaphore::new(retained_bytes))
        .acquire_many_owned(u32::try_from(retained_bytes).expect("retained bytes fit"))
        .await
        .expect("synthetic retained permit");
    QueuedInboundEvent { event, permit }
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
        vec![InboundRejectionKind::NotOwner]
    );
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn router_retries_a_transient_rejection_projection_without_losing_the_received_row() {
    let config = validated_config();
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = Arc::new(RetryOnceRejectionSink::default());
    let router = Router::start(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        degraded_supervisor().await,
        sink.clone(),
    )
    .await
    .expect("router");

    router
        .route(
            queued_registered(
                &store,
                &namespace,
                event("event-rejection-retry", "intruder-runtime-scope"),
            )
            .await,
        )
        .await
        .expect("bounded retry lane accepts transient projection failure");
    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-rejection-retry"],
        InboundEventState::Rejected,
    )
    .await;
    assert_eq!(sink.attempts.load(Ordering::SeqCst), 2);
    assert_eq!(store.outbox_depth().await.expect("outbox").pending, 1);

    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn router_retry_lane_enforces_its_count_bound() {
    let config = validated_config();
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let router = Router::start(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        degraded_supervisor().await,
        Arc::new(UnavailableRejectionSink),
    )
    .await
    .expect("router");

    let retry_event = event("event-retry-capacity", "intruder-runtime-scope");
    router
        .route(queued_registered(&store, &namespace, retry_event.clone()).await)
        .await
        .expect("first bounded retry slot");
    for _ in 1..ROUTER_RETRY_CAPACITY {
        router
            .route(queued_synthetic(retry_event.clone(), 1).await)
            .await
            .expect("bounded retry slot");
    }
    assert!(matches!(
        router
            .route(queued_synthetic(retry_event.clone(), 1).await)
            .await,
        Err(RouteError::ReplySink)
    ));
    assert_eq!(router.snapshot().queued_commands, ROUTER_RETRY_CAPACITY);
    assert_eq!(
        store
            .inbound_state(&namespace, &retry_event.event_id)
            .await
            .expect("overflow state"),
        Some(InboundEventState::Received)
    );

    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn router_retry_lane_enforces_its_aggregate_byte_bound() {
    let config = validated_config();
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let router = Router::start(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        degraded_supervisor().await,
        Arc::new(UnavailableRejectionSink),
    )
    .await
    .expect("router");
    let retained_bytes = ROUTER_COMMAND_BYTE_BUDGET / 2 + 1;
    let mut first = queued_registered(
        &store,
        &namespace,
        event("event-retry-bytes-first", "intruder-runtime-scope"),
    )
    .await;
    first.permit = Arc::new(Semaphore::new(retained_bytes))
        .acquire_many_owned(u32::try_from(retained_bytes).expect("retained bytes fit"))
        .await
        .expect("first synthetic retained permit");
    router.route(first).await.expect("first retry fits");

    let overflow_id = "event-retry-bytes-overflow";
    let mut overflow = queued_registered(
        &store,
        &namespace,
        event(overflow_id, "intruder-runtime-scope"),
    )
    .await;
    overflow.permit = Arc::new(Semaphore::new(retained_bytes))
        .acquire_many_owned(u32::try_from(retained_bytes).expect("retained bytes fit"))
        .await
        .expect("overflow synthetic retained permit");
    assert!(matches!(
        router.route(overflow).await,
        Err(RouteError::ReplySink)
    ));
    assert_eq!(
        store
            .inbound_state(&namespace, overflow_id)
            .await
            .expect("overflow state"),
        Some(InboundEventState::Received)
    );

    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[allow(clippy::too_many_lines)]
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
    let progress_text = "p".repeat(200);
    control
        .send_json(json!({
            "method": "item/completed",
            "params": {
                "threadId": "thread-runtime",
                "turnId": "turn-runtime",
                "completedAtMs": 1_786_478_402_500_i64,
                "item": {
                    "id": "item-progress",
                    "type": "agentMessage",
                    "text": progress_text,
                    "phase": "commentary",
                    "memoryCitation": null
                }
            }
        }))
        .await;
    let mut completed_turn = turn("turn-runtime", "completed");
    completed_turn["items"] = json!([{
        "id": "item-final",
        "type": "agentMessage",
        "text": "done",
        "phase": "final_answer",
        "memoryCitation": null
    }]);
    control
        .send_json(json!({
            "method": "turn/completed",
            "params": {
                "threadId": "thread-runtime",
                "turn": completed_turn
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
    assert_eq!(
        *sink.progress.lock().expect("progress"),
        vec![(turn_row_id, 0, 200)],
        "the running actor must durably project the commentary event"
    );
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
#[allow(clippy::too_many_lines)]
async fn attachment_cache_inputs_are_leased_for_the_turn_and_released_at_completion() {
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
    let temp = tempdir().expect("tempdir");
    let cache_root = temp.path().join("attachments");
    let cache = Arc::new(
        AttachmentCache::open(
            &cache_root,
            store.clone(),
            Arc::new(StaticAttachmentDownloader),
            AttachmentLimits::default(),
        )
        .expect("attachment cache"),
    );
    let canonical_cache_root = std::fs::canonicalize(&cache_root).expect("canonical cache root");
    let (supervisor, control) = ready_supervisor().await;
    let router = Router::start_with_attachments(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        supervisor,
        sink.clone(),
        Arc::clone(&cache),
    )
    .await
    .expect("router");

    let mut inbound = event("event-attachments", "owner-runtime-scope");
    inbound.resources = vec![
        ResourceDesc {
            kind: ResourceKind::Image,
            key: "img_key".to_owned(),
        },
        ResourceDesc {
            kind: ResourceKind::File,
            key: "file_key".to_owned(),
        },
    ];
    router
        .route(queued_registered(&store, &namespace, inbound).await)
        .await
        .expect("route attachment event");

    let start_thread = control.next_request().await;
    assert_eq!(start_thread["method"], "thread/start");
    control
        .respond(
            &start_thread,
            thread_result("thread-attachments", &workspace),
        )
        .await;
    let start_turn = control.next_request().await;
    assert_eq!(start_turn["method"], "turn/start");
    let inputs = start_turn["params"]["input"]
        .as_array()
        .expect("turn inputs");
    assert_eq!(inputs.len(), 3);
    assert_eq!(inputs[0]["type"], "text");
    assert_eq!(inputs[0]["text"], "hello");
    assert_eq!(inputs[1]["type"], "localImage");
    assert_eq!(inputs[2]["type"], "text");
    let file_context: Value =
        serde_json::from_str(inputs[2]["text"].as_str().expect("structured file context"))
            .expect("file context JSON");
    assert_eq!(file_context["attachment"]["kind"], "file");
    assert_eq!(file_context["attachment"]["name"], "attachment-2");
    let image_path = std::path::PathBuf::from(inputs[1]["path"].as_str().expect("image path"));
    let file_path = std::path::PathBuf::from(
        file_context["attachment"]["path"]
            .as_str()
            .expect("file path"),
    );
    for path in [&image_path, &file_path] {
        assert!(path.starts_with(&canonical_cache_root));
        assert!(path.is_file());
    }

    let rows = store.list_attachments().await.expect("attachment rows");
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert_eq!(
            store
                .attachment_leases(&row.sha256)
                .await
                .expect("attachment leases")
                .len(),
            1
        );
    }

    respond_turn_started(&control, &start_turn, "turn-attachments").await;
    send_turn_completed(
        &control,
        "thread-attachments",
        "turn-attachments",
        "completed",
    )
    .await;
    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-attachments"],
        InboundEventState::Completed,
    )
    .await;
    timeout(Duration::from_secs(2), async {
        loop {
            let mut leased = false;
            for row in &rows {
                leased |= !store
                    .attachment_leases(&row.sha256)
                    .await
                    .expect("attachment leases")
                    .is_empty();
            }
            if !leased {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("leases released after completion");

    router.shutdown().await.expect("shutdown");
    drop(cache);
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn lazy_context_resolves_metadata_and_fetches_media_only_on_tool_call() {
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
    let temp = tempdir().expect("tempdir");
    let cache_root = temp.path().join("lazy-context-attachments");
    let cache = Arc::new(
        AttachmentCache::open(
            &cache_root,
            store.clone(),
            Arc::new(StaticAttachmentDownloader),
            AttachmentLimits::default(),
        )
        .expect("attachment cache"),
    );
    let contexts = Arc::new(ContextRegistry::default());
    let (supervisor, control) = ready_supervisor().await;
    let router = Router::start_with_contexts(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        supervisor,
        sink,
        Arc::clone(&cache),
        contexts,
    )
    .await
    .expect("router");

    let mut inbound = event("event-lazy-context", "owner-runtime-scope");
    inbound.parts = vec![
        MessagePart::Text {
            text: "hello".to_owned(),
        },
        MessagePart::Image(MediaPart {
            key: Some("img_key".to_owned()),
            thumbnail_key: None,
            metadata: MediaMetadata::default(),
            status: PartStatus::Available,
        }),
    ];
    inbound.resources = vec![ResourceDesc {
        kind: ResourceKind::Image,
        key: "img_key".to_owned(),
    }];
    router
        .route(queued_registered(&store, &namespace, inbound).await)
        .await
        .expect("route context event");

    let start_thread = control.next_request().await;
    assert_eq!(start_thread["method"], "thread/start");
    assert_eq!(
        start_thread["params"]["dynamicTools"]
            .as_array()
            .expect("dynamic tool declarations")
            .len(),
        2
    );
    control
        .respond(
            &start_thread,
            thread_result("thread-lazy-context", &workspace),
        )
        .await;

    let start_turn = control.next_request().await;
    let inputs = start_turn["params"]["input"]
        .as_array()
        .expect("turn input array");
    assert_eq!(inputs.len(), 2, "media must not be downloaded eagerly");
    assert_eq!(inputs[0]["text"], "hello");
    let reference = inputs[1]["text"].as_str().expect("context reference");
    let payload = reference
        .strip_prefix("<bridge_context>")
        .and_then(|value| value.strip_suffix("</bridge_context>"))
        .expect("compact context envelope");
    let reference: Value = serde_json::from_str(payload).expect("context JSON");
    let context_id = reference["id"].as_str().expect("opaque context id");
    assert_eq!(reference["wake"], "message");
    assert_eq!(reference["mentioned_self"], false);
    assert!(!reference.to_string().contains("img_key"));

    respond_turn_started(&control, &start_turn, "turn-lazy-context").await;
    control
        .send_json(json!({
            "id": "server-context-resolve",
            "method": "item/tool/call",
            "params": {
                "threadId": "thread-lazy-context",
                "turnId": "turn-lazy-context",
                "callId": "call-context-resolve",
                "namespace": "bridge_context",
                "tool": "resolve",
                "arguments": {"id": context_id}
            }
        }))
        .await;
    let context_response = control.next_request().await;
    assert_eq!(context_response["id"], "server-context-resolve");
    assert_eq!(context_response["result"]["success"], true);
    let context_text = context_response["result"]["contentItems"][0]["text"]
        .as_str()
        .expect("context result text");
    let context_value: Value = serde_json::from_str(context_text).expect("context result JSON");
    assert_eq!(context_value["sender"]["openId"], "owner-runtime-scope");
    assert!(!context_text.contains("img_key"));
    let media_handle = context_value["parts"][1]["handle"]
        .as_str()
        .expect("opaque media handle");

    control
        .send_json(json!({
            "id": "server-media-read",
            "method": "item/tool/call",
            "params": {
                "threadId": "thread-lazy-context",
                "turnId": "turn-lazy-context",
                "callId": "call-media-read",
                "namespace": "bridge_media",
                "tool": "read",
                "arguments": {"context_id": context_id, "handle": media_handle}
            }
        }))
        .await;
    let media_response = control.next_request().await;
    assert_eq!(media_response["id"], "server-media-read");
    assert_eq!(media_response["result"]["success"], true);
    let media_text = media_response["result"]["contentItems"][0]["text"]
        .as_str()
        .expect("media result text");
    let media_value: Value = serde_json::from_str(media_text).expect("media result JSON");
    let media_path = std::path::Path::new(
        media_value["media"]["path"]
            .as_str()
            .expect("cached media path"),
    );
    assert!(media_path.is_file());
    assert_eq!(media_value["media"]["bytes"], 16);

    send_turn_completed(
        &control,
        "thread-lazy-context",
        "turn-lazy-context",
        "completed",
    )
    .await;
    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-lazy-context"],
        InboundEventState::Completed,
    )
    .await;
    router.shutdown().await.expect("shutdown");
    drop(cache);
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn bridge_media_read_meters_distinct_and_repeated_handles_before_cache_or_lease() {
    let config = validated_config();
    let workspace = config.default_workspace.clone().expect("workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let temp = tempdir().expect("tempdir");
    let downloader = Arc::new(MeteredAttachmentDownloader::default());
    let calls = Arc::clone(&downloader.calls);
    let cache = Arc::new(
        AttachmentCache::open(
            &temp.path().join("metered-media"),
            store.clone(),
            downloader,
            AttachmentLimits {
                max_attachment_bytes: 8,
                ..AttachmentLimits::default()
            },
        )
        .expect("cache"),
    );
    let contexts = Arc::new(
        ContextRegistry::new(ContextRegistryConfig {
            max_media_reads_per_turn: 5,
            max_media_read_bytes_per_turn: 13,
            // Deliberately smaller than the cache cap: the production tool
            // path must reserve the downstream cache maximum, not trust this
            // direct-authorization fallback.
            max_media_read_bytes_per_item: 1,
            ..ContextRegistryConfig::default()
        })
        .expect("metered registry"),
    );
    let (supervisor, control) = ready_supervisor().await;
    let router = Router::start_with_contexts(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        supervisor,
        Arc::new(RecordingSink::default()),
        Arc::clone(&cache),
        contexts,
    )
    .await
    .expect("router");

    let mut inbound = event("event-metered-media", "owner-runtime-scope");
    inbound.parts = ["meter_key_0", "meter_key_1", "meter_key_2"]
        .into_iter()
        .map(|key| {
            MessagePart::Image(MediaPart {
                key: Some(key.to_owned()),
                thumbnail_key: None,
                metadata: MediaMetadata::default(),
                status: PartStatus::Available,
            })
        })
        .collect();
    router
        .route(queued_registered(&store, &namespace, inbound).await)
        .await
        .expect("route media event");
    let start_thread = control.next_request().await;
    control
        .respond(
            &start_thread,
            thread_result("thread-metered-media", &workspace),
        )
        .await;
    let start_turn = control.next_request().await;
    let reference = start_turn["params"]["input"]
        .as_array()
        .expect("inputs")
        .iter()
        .filter_map(|input| input["text"].as_str())
        .find_map(|text| {
            text.strip_prefix("<bridge_context>")
                .and_then(|value| value.strip_suffix("</bridge_context>"))
        })
        .expect("context reference");
    let reference: Value = serde_json::from_str(reference).expect("context reference JSON");
    let context_id = reference["id"].as_str().expect("context id").to_owned();
    respond_turn_started(&control, &start_turn, "turn-metered-media").await;
    control
        .send_json(json!({
            "id": "resolve-metered-media",
            "method": "item/tool/call",
            "params": {
                "threadId": "thread-metered-media",
                "turnId": "turn-metered-media",
                "callId": "resolve-metered-media",
                "namespace": "bridge_context",
                "tool": "resolve",
                "arguments": {"id": context_id}
            }
        }))
        .await;
    let resolved = control.next_request().await;
    let body: Value = serde_json::from_str(
        resolved["result"]["contentItems"][0]["text"]
            .as_str()
            .expect("resolved context"),
    )
    .expect("resolved JSON");
    let handles = body["parts"]
        .as_array()
        .expect("parts")
        .iter()
        .map(|part| part["handle"].as_str().expect("media handle").to_owned())
        .collect::<Vec<_>>();
    assert_eq!(handles.len(), 3);

    for (request_id, handle) in [
        ("read-meter-0", handles[0].as_str()),
        ("read-meter-1", handles[1].as_str()),
        ("read-meter-1-repeat", handles[1].as_str()),
    ] {
        let response = read_media(
            &control,
            "thread-metered-media",
            "turn-metered-media",
            request_id,
            &context_id,
            handle,
        )
        .await;
        assert_eq!(response["result"]["success"], true, "{request_id}");
    }
    assert_eq!(
        calls.lock().expect("calls").len(),
        3,
        "the repeated handle is independently materialized and charged"
    );

    for (request_id, handle) in [
        ("read-meter-distinct-denied", handles[2].as_str()),
        ("read-meter-repeat-denied", handles[0].as_str()),
    ] {
        let response = read_media(
            &control,
            "thread-metered-media",
            "turn-metered-media",
            request_id,
            &context_id,
            handle,
        )
        .await;
        assert_eq!(response["result"]["success"], false, "{request_id}");
        let error: Value = serde_json::from_str(
            response["result"]["contentItems"][0]["text"]
                .as_str()
                .expect("error body"),
        )
        .expect("error JSON");
        assert_eq!(error["error"]["code"], "capacity_exceeded");
    }
    assert_eq!(
        calls.lock().expect("calls").len(),
        3,
        "over-budget reads fail before downloader I/O"
    );
    let attachments = store.list_attachments().await.expect("attachments");
    assert_eq!(attachments.len(), 2, "denied handle creates no cache row");
    for attachment in &attachments {
        assert_eq!(
            store
                .attachment_leases(&attachment.sha256)
                .await
                .expect("leases")
                .len(),
            1
        );
    }

    send_turn_completed(
        &control,
        "thread-metered-media",
        "turn-metered-media",
        "completed",
    )
    .await;
    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-metered-media"],
        InboundEventState::Completed,
    )
    .await;
    router.shutdown().await.expect("shutdown");
    drop(cache);
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn p2p_media_stages_without_a_turn_and_next_text_consumes_it_once_lazily() {
    let config = validated_config();
    let workspace = config.default_workspace.clone().expect("workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let temp = tempdir().expect("tempdir");
    let downloader = Arc::new(RecordingAttachmentDownloader::default());
    let calls = Arc::clone(&downloader.calls);
    let cache = Arc::new(
        AttachmentCache::open(
            &temp.path().join("pending-media-cache"),
            store.clone(),
            downloader,
            AttachmentLimits::default(),
        )
        .expect("cache"),
    );
    let contexts = Arc::new(ContextRegistry::default());
    let (supervisor, control) = ready_supervisor().await;
    let router = Router::start_with_contexts(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        supervisor,
        Arc::new(RecordingSink::default()),
        Arc::clone(&cache),
        contexts,
    )
    .await
    .expect("router");

    for (event_id, key) in [
        ("pending-one", "pending_key_one"),
        ("pending-two", "pending_key_two"),
    ] {
        router
            .route(queued_registered(&store, &namespace, image_event(event_id, key)).await)
            .await
            .expect("stage image");
    }
    wait_for_inbound_states(
        &store,
        &namespace,
        &["pending-one", "pending-two"],
        InboundEventState::Completed,
    )
    .await;
    timeout(Duration::from_secs(2), async {
        loop {
            if router
                .scope_snapshot(&ScopeKey::Chat("chat-runtime-scope".to_owned()))
                .await
                .expect("snapshot")
                .is_some_and(|snapshot| snapshot.pending_media == 2)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("two pending descriptors");
    assert!(calls.lock().expect("download calls").is_empty());
    assert_eq!(router.snapshot().active_turns, 0);

    let trigger = event("pending-trigger", "owner-runtime-scope");
    router
        .route(queued_registered(&store, &namespace, trigger).await)
        .await
        .expect("route trigger");
    let start_thread = control.next_request().await;
    control
        .respond(
            &start_thread,
            thread_result("thread-pending-media", &workspace),
        )
        .await;
    let start_turn = control.next_request().await;
    let inputs = start_turn["params"]["input"].as_array().expect("inputs");
    assert_eq!(inputs.len(), 4, "one text plus three context references");
    assert_eq!(inputs[0]["text"], "hello");
    let first_pending = context_reference(&inputs[1]);
    assert_eq!(first_pending["wake"], "pending_media");
    assert_eq!(context_reference(&inputs[2])["wake"], "pending_media");
    assert_eq!(context_reference(&inputs[3])["wake"], "message");
    let prompt = serde_json::to_string(inputs).expect("serialize prompt");
    assert!(!prompt.contains("pending_key_one"));
    assert!(!prompt.contains("pending_key_two"));
    assert!(calls.lock().expect("download calls").is_empty());

    respond_turn_started(&control, &start_turn, "turn-pending-media").await;
    let context_id = first_pending["id"].as_str().expect("context ID");
    control
        .send_json(json!({
            "id": "resolve-pending-media",
            "method": "item/tool/call",
            "params": {
                "threadId": "thread-pending-media",
                "turnId": "turn-pending-media",
                "callId": "call-resolve-pending-media",
                "namespace": "bridge_context",
                "tool": "resolve",
                "arguments": {"id": context_id}
            }
        }))
        .await;
    let response = control.next_request().await;
    let body: Value = serde_json::from_str(
        response["result"]["contentItems"][0]["text"]
            .as_str()
            .expect("resolve text"),
    )
    .expect("resolve JSON");
    let handle = body["parts"][0]["handle"].as_str().expect("media handle");
    assert!(!body.to_string().contains("pending_key_one"));
    let response = read_media(
        &control,
        "thread-pending-media",
        "turn-pending-media",
        "read-pending-media",
        context_id,
        handle,
    )
    .await;
    assert_eq!(response["result"]["success"], true);
    assert_eq!(
        *calls.lock().expect("download calls"),
        vec![(
            "message-pending-one".to_owned(),
            "pending_key_one".to_owned()
        )]
    );

    send_turn_completed(
        &control,
        "thread-pending-media",
        "turn-pending-media",
        "completed",
    )
    .await;
    wait_for_inbound_states(
        &store,
        &namespace,
        &["pending-trigger"],
        InboundEventState::Completed,
    )
    .await;
    let snapshot = router
        .scope_snapshot(&ScopeKey::Chat("chat-runtime-scope".to_owned()))
        .await
        .expect("snapshot")
        .expect("actor");
    assert_eq!(snapshot.pending_media, 0, "pending media is single-use");
    router.shutdown().await.expect("shutdown");
    drop(cache);
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn pending_media_is_restored_when_the_consuming_turn_is_not_started() {
    let config = validated_config();
    let workspace = config.default_workspace.clone().expect("workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let temp = tempdir().expect("tempdir");
    let cache = Arc::new(
        AttachmentCache::open(
            &temp.path().join("pending-retry-cache"),
            store.clone(),
            Arc::new(RecordingAttachmentDownloader::default()),
            AttachmentLimits::default(),
        )
        .expect("cache"),
    );
    let (supervisor, control) = ready_supervisor().await;
    let router = Router::start_with_contexts(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        supervisor,
        Arc::new(RecordingSink::default()),
        cache,
        Arc::new(ContextRegistry::default()),
    )
    .await
    .expect("router");

    router
        .route(
            queued_registered(
                &store,
                &namespace,
                image_event("pending-retry", "pending_retry_key"),
            )
            .await,
        )
        .await
        .expect("stage image");
    wait_for_inbound_states(
        &store,
        &namespace,
        &["pending-retry"],
        InboundEventState::Completed,
    )
    .await;

    router
        .route(
            queued_registered(
                &store,
                &namespace,
                event("pending-failed-trigger", "owner-runtime-scope"),
            )
            .await,
        )
        .await
        .expect("route failed trigger");
    let start_thread = control.next_request().await;
    control
        .respond(
            &start_thread,
            thread_result("thread-pending-retry", &workspace),
        )
        .await;
    let failed_start = control.next_request().await;
    assert_eq!(failed_start["method"], "turn/start");
    control
        .send_json(json!({
            "id": failed_start["id"],
            "error": {"code": -32602, "message": "deliberate start rejection"}
        }))
        .await;
    wait_for_inbound_states(
        &store,
        &namespace,
        &["pending-failed-trigger"],
        InboundEventState::Rejected,
    )
    .await;
    timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = router
                .scope_snapshot(&ScopeKey::Chat("chat-runtime-scope".to_owned()))
                .await
                .expect("snapshot")
                .expect("actor");
            if snapshot.pending_media == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a definitely rejected start restores staged media");

    router
        .route(
            queued_registered(
                &store,
                &namespace,
                event("pending-retry-trigger", "owner-runtime-scope"),
            )
            .await,
        )
        .await
        .expect("route retry trigger");
    let resume = control.next_request().await;
    assert_eq!(resume["method"], "thread/resume");
    control
        .respond(&resume, thread_result("thread-pending-retry", &workspace))
        .await;
    let start_turn = control.next_request().await;
    let inputs = start_turn["params"]["input"].as_array().expect("inputs");
    assert_eq!(
        inputs.len(),
        3,
        "text, restored pending context, trigger context"
    );
    assert_eq!(context_reference(&inputs[1])["wake"], "pending_media");
    respond_turn_started(&control, &start_turn, "turn-pending-retry").await;
    send_turn_completed(
        &control,
        "thread-pending-retry",
        "turn-pending-retry",
        "completed",
    )
    .await;
    wait_for_inbound_states(
        &store,
        &namespace,
        &["pending-retry-trigger"],
        InboundEventState::Completed,
    )
    .await;
    assert_eq!(
        router
            .scope_snapshot(&ScopeKey::Chat("chat-runtime-scope".to_owned()))
            .await
            .expect("snapshot")
            .expect("actor")
            .pending_media,
        0
    );
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn uncertain_turn_start_commits_pending_media_instead_of_restoring_it() {
    let config = validated_config();
    let workspace = config.default_workspace.clone().expect("workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = Arc::new(RecordingSink::default());
    let (supervisor, first_control, second_control) = restarting_supervisor().await;
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
                image_event("pending-uncertain", "pending_uncertain_key"),
            )
            .await,
        )
        .await
        .expect("stage image");
    wait_for_inbound_states(
        &store,
        &namespace,
        &["pending-uncertain"],
        InboundEventState::Completed,
    )
    .await;
    router
        .route(
            queued_registered(
                &store,
                &namespace,
                event("pending-uncertain-trigger", "owner-runtime-scope"),
            )
            .await,
        )
        .await
        .expect("route uncertain trigger");
    let start_thread = first_control.next_request().await;
    first_control
        .respond(
            &start_thread,
            thread_result("thread-pending-uncertain", &workspace),
        )
        .await;
    let start_turn = first_control.next_request().await;
    assert_eq!(start_turn["method"], "turn/start");
    first_control.unexpected_exit();
    wait_for_inbound_states(
        &store,
        &namespace,
        &["pending-uncertain-trigger"],
        InboundEventState::Rejected,
    )
    .await;
    assert_eq!(
        sink.finalizations.lock().expect("finalizations")[0].1,
        TurnResolution::Uncertain
    );
    let scope = ScopeKey::Chat("chat-runtime-scope".to_owned());
    assert_eq!(
        router
            .scope_snapshot(&scope)
            .await
            .expect("snapshot")
            .expect("actor")
            .pending_media,
        0,
        "an ambiguously applied association is never replayed"
    );
    second_control
        .expect_no_request_for(Duration::from_millis(100))
        .await;
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn p2p_audio_triggers_alone_without_consuming_pending_images() {
    let config = validated_config();
    let workspace = config.default_workspace.clone().expect("workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let temp = tempdir().expect("tempdir");
    let cache = Arc::new(
        AttachmentCache::open(
            &temp.path().join("audio-does-not-consume"),
            store.clone(),
            Arc::new(StaticAttachmentDownloader),
            AttachmentLimits::default(),
        )
        .expect("cache"),
    );
    let (supervisor, control) = ready_supervisor().await;
    let router = Router::start_with_contexts(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        supervisor,
        Arc::new(RecordingSink::default()),
        Arc::clone(&cache),
        Arc::new(ContextRegistry::default()),
    )
    .await
    .expect("router");
    router
        .route(
            queued_registered(
                &store,
                &namespace,
                image_event("audio-pending-image", "img_key"),
            )
            .await,
        )
        .await
        .expect("stage image");
    wait_for_inbound_states(
        &store,
        &namespace,
        &["audio-pending-image"],
        InboundEventState::Completed,
    )
    .await;

    let mut audio = event("audio-direct-trigger", "owner-runtime-scope");
    audio.text.clear();
    audio.message_type = "audio".to_owned();
    audio.parts = vec![audio_part(Some("audio is complete input"))];
    router
        .route(queued_registered(&store, &namespace, audio).await)
        .await
        .expect("route audio");
    let start_thread = control.next_request().await;
    control
        .respond(
            &start_thread,
            thread_result("thread-audio-pending", &workspace),
        )
        .await;
    let start_audio = control.next_request().await;
    let audio_inputs = start_audio["params"]["input"]
        .as_array()
        .expect("audio inputs");
    assert_eq!(audio_inputs.len(), 2, "audio has only its own context");
    assert_eq!(context_reference(&audio_inputs[1])["wake"], "message");
    respond_turn_started(&control, &start_audio, "turn-audio-pending").await;
    let snapshot = router
        .scope_snapshot(&ScopeKey::Chat("chat-runtime-scope".to_owned()))
        .await
        .expect("snapshot")
        .expect("actor");
    assert_eq!(snapshot.pending_media, 1);
    send_turn_completed(
        &control,
        "thread-audio-pending",
        "turn-audio-pending",
        "completed",
    )
    .await;
    wait_for_inbound_states(
        &store,
        &namespace,
        &["audio-direct-trigger"],
        InboundEventState::Completed,
    )
    .await;

    router
        .route(
            queued_registered(
                &store,
                &namespace,
                event("audio-followup-text", "owner-runtime-scope"),
            )
            .await,
        )
        .await
        .expect("route followup");
    let resume = control.next_request().await;
    assert_eq!(resume["method"], "thread/resume");
    control
        .respond(&resume, thread_result("thread-audio-pending", &workspace))
        .await;
    let start_followup = control.next_request().await;
    let inputs = start_followup["params"]["input"]
        .as_array()
        .expect("followup inputs");
    assert_eq!(
        inputs.len(),
        3,
        "text consumes the image plus its own context"
    );
    assert_eq!(context_reference(&inputs[1])["wake"], "pending_media");
    respond_turn_started(&control, &start_followup, "turn-audio-followup").await;
    send_turn_completed(
        &control,
        "thread-audio-pending",
        "turn-audio-followup",
        "completed",
    )
    .await;
    wait_for_inbound_states(
        &store,
        &namespace,
        &["audio-followup-text"],
        InboundEventState::Completed,
    )
    .await;
    router.shutdown().await.expect("shutdown");
    drop(cache);
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn group_media_flood_is_durably_ignored_without_actor_context_cache_or_asr_work() {
    let mut config = validated_config();
    config.allowed_groups.push("chat-group-media".to_owned());
    config.validate().expect("group policy");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let temp = tempdir().expect("tempdir");
    let downloader = Arc::new(RecordingAttachmentDownloader::default());
    let download_calls = Arc::clone(&downloader.calls);
    let cache = Arc::new(
        AttachmentCache::open(
            &temp.path().join("ignored-group-media"),
            store.clone(),
            downloader,
            AttachmentLimits::default(),
        )
        .expect("cache"),
    );
    let contexts = Arc::new(ContextRegistry::default());
    let router = Router::start_with_contexts(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        degraded_supervisor().await,
        Arc::new(RecordingSink::default()),
        Arc::clone(&cache),
        Arc::clone(&contexts),
    )
    .await
    .expect("router");

    let mut ids = Vec::new();
    for index in 0..100 {
        let event_id = format!("group-media-{index}");
        let mut inbound = if index % 4 == 3 {
            let mut audio = event_in_chat(&event_id, "owner-runtime-scope", "chat-group-media");
            audio.text.clear();
            audio.message_type = "audio".to_owned();
            audio.parts = vec![audio_part(None)];
            audio
        } else {
            let mut image = image_event(&event_id, &format!("group_key_{index}"));
            image.chat_id = "chat-group-media".to_owned();
            image.scope = ScopeKey::Chat("chat-group-media".to_owned());
            image
        };
        inbound.chat_type = ChatMode::Group;
        inbound.mentions_bot = false;
        router
            .route(queued_registered(&store, &namespace, inbound).await)
            .await
            .expect("ignore group media");
        ids.push(event_id);
    }
    for event_id in &ids {
        assert_eq!(
            store
                .inbound_state(&namespace, event_id)
                .await
                .expect("inbound state"),
            Some(InboundEventState::Completed)
        );
    }
    assert_eq!(router.snapshot().scope_count, 0);
    assert!(download_calls.lock().expect("download calls").is_empty());
    assert_eq!(contexts.stats().total, 0);
    assert!(
        store
            .list_attachments()
            .await
            .expect("attachment rows")
            .is_empty()
    );
    router.shutdown().await.expect("shutdown");
    drop(cache);
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn pending_media_count_ttl_and_interrupt_bounds_clear_metadata() {
    let config = validated_config();
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config).with_test_pending_media_limits(
        Duration::from_millis(40),
        2,
        16 * 1024,
    );
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let router = Router::start(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        degraded_supervisor().await,
        Arc::new(RecordingSink::default()),
    )
    .await
    .expect("router");
    for index in 0..3 {
        let event_id = format!("bounded-pending-{index}");
        router
            .route(
                queued_registered(
                    &store,
                    &namespace,
                    image_event(&event_id, &format!("bounded_key_{index}")),
                )
                .await,
            )
            .await
            .expect("stage bounded media");
    }
    wait_for_inbound_states(
        &store,
        &namespace,
        &[
            "bounded-pending-0",
            "bounded-pending-1",
            "bounded-pending-2",
        ],
        InboundEventState::Completed,
    )
    .await;
    let scope = ScopeKey::Chat("chat-runtime-scope".to_owned());
    timeout(Duration::from_secs(2), async {
        loop {
            if router
                .scope_snapshot(&scope)
                .await
                .expect("snapshot")
                .is_some_and(|snapshot| snapshot.pending_media == 2)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("count bound evicts oldest");
    assert_eq!(
        router.interrupt(&scope).await.expect("interrupt"),
        InterruptOutcome::NoActiveTurn
    );
    let cleared = router
        .scope_snapshot(&scope)
        .await
        .expect("snapshot")
        .expect("actor");
    assert_eq!((cleared.pending_media, cleared.pending_media_bytes), (0, 0));

    router
        .route(
            queued_registered(
                &store,
                &namespace,
                image_event("bounded-pending-expiry", "bounded_expiry_key"),
            )
            .await,
        )
        .await
        .expect("stage expiring media");
    wait_for_inbound_states(
        &store,
        &namespace,
        &["bounded-pending-expiry"],
        InboundEventState::Completed,
    )
    .await;
    sleep(Duration::from_millis(100)).await;
    let expired = router
        .scope_snapshot(&scope)
        .await
        .expect("snapshot")
        .expect("actor");
    assert_eq!((expired.pending_media, expired.pending_media_bytes), (0, 0));

    router
        .route(
            queued_registered(
                &store,
                &namespace,
                image_event("bounded-pending-explicit", "bounded_explicit_key"),
            )
            .await,
        )
        .await
        .expect("stage before explicit quote");
    wait_for_inbound_states(
        &store,
        &namespace,
        &["bounded-pending-explicit"],
        InboundEventState::Completed,
    )
    .await;
    let mut explicit = event("bounded-explicit-trigger", "owner-runtime-scope");
    explicit.reply_to_message_id = Some("om_explicit_parent".to_owned());
    router
        .route(queued_registered(&store, &namespace, explicit).await)
        .await
        .expect("route explicit quote");
    timeout(Duration::from_secs(2), async {
        loop {
            if router
                .scope_snapshot(&scope)
                .await
                .expect("snapshot")
                .is_some_and(|snapshot| snapshot.pending_media == 0)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("explicit quote wins and clears pending media");
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn debounce_quote_reset_and_control_interrupt_invalidate_held_pending_reservations() {
    for mode in [
        DebounceInvalidator::ExplicitQuote,
        DebounceInvalidator::ResetCommand,
        DebounceInvalidator::ControlInterrupt,
    ] {
        assert_debounce_invalidates_reserved_media(mode).await;
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn pending_media_ttl_remains_truthful_while_an_unrelated_turn_is_active() {
    let config = validated_config();
    let workspace = config.default_workspace.clone().expect("workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config)
        .with_test_timings(
            Duration::from_millis(300),
            Duration::from_secs(60),
            Duration::from_millis(10),
        )
        .with_test_pending_media_limits(Duration::from_millis(400), 2, 16 * 1024);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let (supervisor, control) = ready_supervisor().await;
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
    let scope = ScopeKey::Chat("chat-runtime-scope".to_owned());

    router
        .route(
            queued_registered(
                &store,
                &namespace,
                event("active-ttl-trigger", "owner-runtime-scope"),
            )
            .await,
        )
        .await
        .expect("route trigger");
    timeout(Duration::from_secs(2), async {
        loop {
            if router
                .scope_snapshot(&scope)
                .await
                .expect("snapshot")
                .is_some_and(|snapshot| snapshot.state == ScopeState::Debouncing)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("trigger enters debounce");
    router
        .route(
            queued_registered(
                &store,
                &namespace,
                image_event("active-ttl-media", "active_ttl_key"),
            )
            .await,
        )
        .await
        .expect("stage during debounce");
    wait_for_inbound_states(
        &store,
        &namespace,
        &["active-ttl-media"],
        InboundEventState::Completed,
    )
    .await;
    let start_thread = control.next_request().await;
    control
        .respond(
            &start_thread,
            thread_result("thread-active-ttl", &workspace),
        )
        .await;
    let start_turn = control.next_request().await;
    respond_turn_started(&control, &start_turn, "turn-active-ttl").await;
    assert_eq!(
        router
            .scope_snapshot(&scope)
            .await
            .expect("snapshot")
            .expect("actor")
            .pending_media,
        1
    );

    sleep(Duration::from_millis(450)).await;
    let expired = router
        .scope_snapshot(&scope)
        .await
        .expect("snapshot")
        .expect("actor");
    assert_eq!((expired.pending_media, expired.pending_media_bytes), (0, 0));
    assert!(matches!(expired.state, ScopeState::Running { .. }));

    send_turn_completed(
        &control,
        "thread-active-ttl",
        "turn-active-ttl",
        "completed",
    )
    .await;
    wait_for_inbound_states(
        &store,
        &namespace,
        &["active-ttl-trigger"],
        InboundEventState::Completed,
    )
    .await;
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn pending_media_metadata_byte_bound_drops_an_oversize_descriptor() {
    let config = validated_config();
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config).with_test_pending_media_limits(
        Duration::from_secs(60),
        2,
        128,
    );
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let router = Router::start(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        degraded_supervisor().await,
        Arc::new(RecordingSink::default()),
    )
    .await
    .expect("router");
    router
        .route(
            queued_registered(
                &store,
                &namespace,
                image_event("pending-metadata-oversize", &"k".repeat(256)),
            )
            .await,
        )
        .await
        .expect("settle oversize descriptor");
    let mut oversized_mention = image_event("pending-mention-oversize", "short_key");
    oversized_mention.mentions = vec![MentionIdentity {
        key: Some("@_user_1".to_owned()),
        open_id: Some("ou_mentioned".to_owned()),
        user_id: Some("user_mentioned".to_owned()),
        union_id: Some("on_mentioned".to_owned()),
        name: Some("m".repeat(256)),
    }];
    router
        .route(queued_registered(&store, &namespace, oversized_mention).await)
        .await
        .expect("settle oversize retained mention");
    wait_for_inbound_states(
        &store,
        &namespace,
        &["pending-metadata-oversize", "pending-mention-oversize"],
        InboundEventState::Completed,
    )
    .await;
    sleep(Duration::from_millis(50)).await;
    let snapshot = router
        .scope_snapshot(&ScopeKey::Chat("chat-runtime-scope".to_owned()))
        .await
        .expect("snapshot")
        .expect("actor");
    assert_eq!(
        (snapshot.pending_media, snapshot.pending_media_bytes),
        (0, 0)
    );
    assert_eq!(router.snapshot().active_turns, 0);
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn authorized_group_quote_resolves_one_hop_and_reads_parent_media_lazily() {
    let mut config = validated_config();
    config.allowed_groups.push("chat-quoted-group".to_owned());
    config.validate().expect("group policy");
    let workspace = config.default_workspace.clone().expect("workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let temp = tempdir().expect("tempdir");
    let downloader = Arc::new(RecordingAttachmentDownloader::default());
    let download_calls = Arc::clone(&downloader.calls);
    let cache = Arc::new(
        AttachmentCache::open(
            &temp.path().join("quoted-group-cache"),
            store.clone(),
            downloader,
            AttachmentLimits::default(),
        )
        .expect("cache"),
    );
    let resolver = Arc::new(RecordingQuoteResolver::default());
    let quote_calls = Arc::clone(&resolver.calls);
    let (supervisor, control) = ready_supervisor().await;
    let router = Router::start_with_contexts_and_quotes(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        supervisor,
        Arc::new(RecordingSink::default()),
        Arc::clone(&cache),
        Arc::new(ContextRegistry::default()),
        resolver,
    )
    .await
    .expect("router");

    let mut unauthorized = event_in_chat(
        "quote-unauthorized",
        "intruder-runtime-scope",
        "chat-not-allowed",
    );
    unauthorized.chat_type = ChatMode::Group;
    unauthorized.mentions_bot = true;
    unauthorized.reply_to_message_id = Some("om_forbidden_parent".to_owned());
    router
        .route(queued_registered(&store, &namespace, unauthorized).await)
        .await
        .expect("durable policy rejection");
    assert!(quote_calls.lock().expect("quote calls").is_empty());

    let mut inbound = event_in_chat(
        "quote-authorized",
        "owner-runtime-scope",
        "chat-quoted-group",
    );
    inbound.chat_type = ChatMode::Group;
    inbound.mentions_bot = true;
    inbound.reply_to_message_id = Some("om_parent".to_owned());
    inbound.text = "analyze the quoted image".to_owned();
    inbound.parts = vec![MessagePart::Text {
        text: inbound.text.clone(),
    }];
    router
        .route(queued_registered(&store, &namespace, inbound).await)
        .await
        .expect("route authorized quote");
    let start_thread = control.next_request().await;
    control
        .respond(
            &start_thread,
            thread_result("thread-quoted-group", &workspace),
        )
        .await;
    let start_turn = control.next_request().await;
    let inputs = start_turn["params"]["input"].as_array().expect("inputs");
    assert_eq!(inputs.len(), 2);
    assert_eq!(inputs[0]["text"], "analyze the quoted image");
    assert!(
        !serde_json::to_string(inputs)
            .expect("prompt")
            .contains("quoted_key")
    );
    assert!(download_calls.lock().expect("download calls").is_empty());
    assert_eq!(
        *quote_calls.lock().expect("quote calls"),
        vec![("om_parent".to_owned(), "chat-quoted-group".to_owned())]
    );

    let context_id = context_reference(&inputs[1])["id"]
        .as_str()
        .expect("context ID")
        .to_owned();
    respond_turn_started(&control, &start_turn, "turn-quoted-group").await;
    control
        .send_json(json!({
            "id": "resolve-quoted-group",
            "method": "item/tool/call",
            "params": {
                "threadId": "thread-quoted-group",
                "turnId": "turn-quoted-group",
                "callId": "call-resolve-quoted-group",
                "namespace": "bridge_context",
                "tool": "resolve",
                "arguments": {"id": context_id}
            }
        }))
        .await;
    let response = control.next_request().await;
    let context_text = response["result"]["contentItems"][0]["text"]
        .as_str()
        .expect("context text");
    assert!(!context_text.contains("quoted_key"));
    let context: Value = serde_json::from_str(context_text).expect("context JSON");
    assert_eq!(context["quote"]["messageId"], "om_parent");
    assert_eq!(context["quote"]["messageType"], "image");
    assert_eq!(context["quote"]["status"], "available");
    let handle = context["quote"]["parts"][0]["handle"]
        .as_str()
        .expect("quoted handle");
    let response = read_media(
        &control,
        "thread-quoted-group",
        "turn-quoted-group",
        "read-quoted-group",
        &context_id,
        handle,
    )
    .await;
    assert_eq!(response["result"]["success"], true);
    assert_eq!(
        *download_calls.lock().expect("download calls"),
        vec![("om_parent".to_owned(), "quoted_key".to_owned())]
    );
    send_turn_completed(
        &control,
        "thread-quoted-group",
        "turn-quoted-group",
        "completed",
    )
    .await;
    wait_for_inbound_states(
        &store,
        &namespace,
        &["quote-authorized"],
        InboundEventState::Completed,
    )
    .await;
    router.shutdown().await.expect("shutdown");
    drop(cache);
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn authorized_group_audio_quote_runs_lazy_asr_inside_the_instruction_turn() {
    let _asr_subprocess_guard = ASR_SUBPROCESS_TEST_LOCK.lock().await;
    let _asr_process_lock = lock_asr_process_tests();
    let mut config = validated_config();
    config.allowed_groups.push("chat-quoted-audio".to_owned());
    let temp = tempdir().expect("tempdir");
    let marker = temp.path().join("quoted-asr-invoked.txt");
    let asr = stub_program(
        temp.path(),
        "quoted-asr",
        r#"printf 'invoked\n' >> "$1"; printf 'QUOTED AUDIO TRANSCRIPT\n'"#,
        "@echo off\r\necho invoked>>\"%~1\"\r\necho QUOTED AUDIO TRANSCRIPT\r\n",
    );
    config.asr = asr_config(
        asr,
        ffmpeg_stub(temp.path()),
        vec![marker.to_string_lossy().into_owned()],
    );
    config.validate().expect("group audio policy");
    let workspace = config.default_workspace.clone().expect("workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let downloader = Arc::new(RecordingAttachmentDownloader::default());
    let download_calls = Arc::clone(&downloader.calls);
    let cache = Arc::new(
        AttachmentCache::open(
            &temp.path().join("quoted-audio-cache"),
            store.clone(),
            downloader,
            AttachmentLimits::default(),
        )
        .expect("cache"),
    );
    let resolver = Arc::new(RecordingAudioQuoteResolver::default());
    let quote_calls = Arc::clone(&resolver.calls);
    let (supervisor, control) = ready_supervisor().await;
    let router = Router::start_with_contexts_and_quotes(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        supervisor,
        Arc::new(RecordingSink::default()),
        Arc::clone(&cache),
        Arc::new(ContextRegistry::default()),
        resolver,
    )
    .await
    .expect("router");

    let mut inbound = event_in_chat(
        "quote-authorized-audio",
        "owner-runtime-scope",
        "chat-quoted-audio",
    );
    inbound.chat_type = ChatMode::Group;
    inbound.mentions_bot = true;
    inbound.reply_to_message_id = Some("om_audio_parent".to_owned());
    inbound.text = "summarize this voice note".to_owned();
    inbound.parts = vec![MessagePart::Text {
        text: inbound.text.clone(),
    }];
    router
        .route(queued_registered(&store, &namespace, inbound).await)
        .await
        .expect("route quote");
    let start_thread = control.next_request().await;
    control
        .respond(
            &start_thread,
            thread_result("thread-quoted-audio", &workspace),
        )
        .await;
    let start_turn = control.next_request().await;
    let inputs = start_turn["params"]["input"].as_array().expect("inputs");
    assert_eq!(inputs.len(), 2, "instruction and quote form one turn");
    assert_eq!(inputs[0]["text"], "summarize this voice note");
    assert!(
        !serde_json::to_string(inputs)
            .expect("prompt")
            .contains("quoted_audio_key")
    );
    assert!(download_calls.lock().expect("downloads").is_empty());
    assert!(!marker.exists());
    assert_eq!(
        *quote_calls.lock().expect("quote calls"),
        vec![("om_audio_parent".to_owned(), "chat-quoted-audio".to_owned())]
    );

    let context_id = context_reference(&inputs[1])["id"]
        .as_str()
        .expect("context ID")
        .to_owned();
    respond_turn_started(&control, &start_turn, "turn-quoted-audio").await;
    control
        .send_json(json!({
            "id": "resolve-quoted-audio",
            "method": "item/tool/call",
            "params": {
                "threadId": "thread-quoted-audio",
                "turnId": "turn-quoted-audio",
                "callId": "call-resolve-quoted-audio",
                "namespace": "bridge_context",
                "tool": "resolve",
                "arguments": {"id": context_id}
            }
        }))
        .await;
    let context_response = control.next_request().await;
    let context_text = context_response["result"]["contentItems"][0]["text"]
        .as_str()
        .expect("context text");
    assert!(!context_text.contains("quoted_audio_key"));
    assert!(!context_text.contains("QUOTED AUDIO TRANSCRIPT"));
    let context: Value = serde_json::from_str(context_text).expect("context JSON");
    assert_eq!(context["quote"]["status"], "available");
    assert_eq!(context["quote"]["parts"][0]["kind"], "audio");
    let handle = context["quote"]["parts"][0]["handle"]
        .as_str()
        .expect("audio handle");

    let response = read_media(
        &control,
        "thread-quoted-audio",
        "turn-quoted-audio",
        "read-quoted-audio",
        &context_id,
        handle,
    )
    .await;
    assert_eq!(response["result"]["success"], true);
    let text = response["result"]["contentItems"][0]["text"]
        .as_str()
        .expect("media response");
    let media: Value = serde_json::from_str(text).expect("media JSON");
    assert_eq!(media["media"]["transcript"], "QUOTED AUDIO TRANSCRIPT");
    assert_eq!(media["media"]["source"], "sidecar");
    assert_eq!(
        *download_calls.lock().expect("downloads"),
        vec![("om_audio_parent".to_owned(), "quoted_audio_key".to_owned())]
    );
    assert!(marker.exists());

    send_turn_completed(
        &control,
        "thread-quoted-audio",
        "turn-quoted-audio",
        "completed",
    )
    .await;
    wait_for_inbound_states(
        &store,
        &namespace,
        &["quote-authorized-audio"],
        InboundEventState::Completed,
    )
    .await;
    router.shutdown().await.expect("shutdown");
    drop(cache);
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn attachment_limit_failure_is_durably_failed_before_codex_turn_start() {
    let config = validated_config();
    let workspace = config
        .default_workspace
        .clone()
        .expect("validated default workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = Arc::new(RecordingSink::default());
    let temp = tempdir().expect("tempdir");
    let cache = Arc::new(
        AttachmentCache::open(
            &temp.path().join("attachments"),
            store.clone(),
            Arc::new(StaticAttachmentDownloader),
            AttachmentLimits {
                max_attachments_per_message: 1,
                ..AttachmentLimits::default()
            },
        )
        .expect("attachment cache"),
    );
    let (supervisor, control) = ready_supervisor().await;
    let router = Router::start_with_attachments(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        supervisor,
        sink.clone(),
        Arc::clone(&cache),
    )
    .await
    .expect("router");

    let mut inbound = event("event-attachment-limit", "owner-runtime-scope");
    inbound.resources = vec![
        ResourceDesc {
            kind: ResourceKind::Image,
            key: "img_key".to_owned(),
        },
        ResourceDesc {
            kind: ResourceKind::File,
            key: "file_key".to_owned(),
        },
    ];
    router
        .route(queued_registered(&store, &namespace, inbound).await)
        .await
        .expect("route attachment event");
    let start_thread = control.next_request().await;
    assert_eq!(start_thread["method"], "thread/start");
    control
        .respond(
            &start_thread,
            thread_result("thread-attachment-limit", &workspace),
        )
        .await;

    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-attachment-limit"],
        InboundEventState::Rejected,
    )
    .await;
    control
        .expect_no_request_for(Duration::from_millis(100))
        .await;
    assert!(
        store
            .list_attachments()
            .await
            .expect("attachments")
            .is_empty()
    );
    assert!(
        store
            .uncertain_turns()
            .await
            .expect("live turns")
            .is_empty()
    );
    {
        let finalizations = sink.finalizations.lock().expect("finalizations");
        assert_eq!(finalizations.len(), 1);
        assert_eq!(finalizations[0].1, TurnResolution::Failed);
    }

    router.shutdown().await.expect("shutdown");
    drop(cache);
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn workspace_revalidation_failure_finalizes_the_turn_and_releases_attachments() {
    let repository = std::env::current_dir().expect("repository");
    let workspace_parent = tempfile::Builder::new()
        .prefix("workspace-race-")
        .tempdir_in(&repository)
        .expect("workspace parent");
    let workspace = workspace_parent.path().join("workspace");
    std::fs::create_dir(&workspace).expect("workspace");
    let mut config = BridgeConfig {
        owners: vec!["owner-runtime-scope".to_owned()],
        default_workspace: Some(workspace.clone()),
        workspace: WorkspacePolicy {
            allow_roots: vec![repository],
            ..WorkspacePolicy::default()
        },
        ..BridgeConfig::default()
    };
    config.validate().expect("valid workspace race config");
    let workspace = config
        .default_workspace
        .clone()
        .expect("canonical workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = Arc::new(RecordingSink::default());
    let cache_temp = tempdir().expect("cache tempdir");
    let cache = Arc::new(
        AttachmentCache::open(
            &cache_temp.path().join("attachments"),
            store.clone(),
            Arc::new(RemovingWorkspaceDownloader {
                workspace: workspace.clone(),
            }),
            AttachmentLimits::default(),
        )
        .expect("attachment cache"),
    );
    let (supervisor, control) = ready_supervisor().await;
    let router = Router::start_with_attachments(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        supervisor,
        sink.clone(),
        Arc::clone(&cache),
    )
    .await
    .expect("router");
    let mut inbound = event("event-workspace-race", "owner-runtime-scope");
    inbound.resources.push(ResourceDesc {
        kind: ResourceKind::File,
        key: "file_key".to_owned(),
    });
    router
        .route(queued_registered(&store, &namespace, inbound).await)
        .await
        .expect("route attachment event");
    let start_thread = control.next_request().await;
    control
        .respond(
            &start_thread,
            thread_result("thread-workspace-race", &workspace),
        )
        .await;

    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-workspace-race"],
        InboundEventState::Rejected,
    )
    .await;
    control
        .expect_no_request_for(Duration::from_millis(100))
        .await;
    let turn_row_id = sink.finalizations.lock().expect("finalizations")[0].0;
    assert_eq!(
        store
            .turn_row(turn_row_id)
            .await
            .expect("turn row")
            .expect("turn")
            .state,
        TurnState::Failed
    );
    let attachment_rows = store.list_attachments().await.expect("attachments");
    assert_eq!(attachment_rows.len(), 1);
    for row in attachment_rows {
        assert!(
            store
                .attachment_leases(&row.sha256)
                .await
                .expect("attachment leases")
                .is_empty()
        );
    }
    router.shutdown().await.expect("shutdown");
    drop(cache);
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn shutdown_cancels_attachment_download_without_waiting_for_later_resources() {
    let config = validated_config();
    let workspace = config.default_workspace.clone().expect("workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = Arc::new(RecordingSink::default());
    let started = Arc::new(AtomicUsize::new(0));
    let started_notify = Arc::new(Notify::new());
    let cache_temp = tempdir().expect("cache tempdir");
    let cache = Arc::new(
        AttachmentCache::open(
            &cache_temp.path().join("attachments"),
            store.clone(),
            Arc::new(PendingAttachmentDownloader {
                started: Arc::clone(&started),
                started_notify: Arc::clone(&started_notify),
            }),
            AttachmentLimits::default(),
        )
        .expect("attachment cache"),
    );
    let (supervisor, control) = ready_supervisor().await;
    let router = Router::start_with_attachments(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        supervisor,
        sink.clone(),
        Arc::clone(&cache),
    )
    .await
    .expect("router");
    let mut inbound = event("event-download-shutdown", "owner-runtime-scope");
    inbound.resources = (0..8)
        .map(|index| ResourceDesc {
            kind: ResourceKind::File,
            key: format!("file_key_{index}"),
        })
        .collect();
    router
        .route(queued_registered(&store, &namespace, inbound).await)
        .await
        .expect("route attachment event");
    let start_thread = control.next_request().await;
    control
        .respond(
            &start_thread,
            thread_result("thread-download-shutdown", &workspace),
        )
        .await;
    timeout(Duration::from_secs(2), async {
        while started.load(Ordering::SeqCst) == 0 {
            started_notify.notified().await;
        }
    })
    .await
    .expect("first attachment download started");

    timeout(Duration::from_millis(500), router.shutdown())
        .await
        .expect("download-aware shutdown deadline")
        .expect("router shutdown");
    assert_eq!(started.load(Ordering::SeqCst), 1);
    assert_eq!(
        store
            .inbound_state(&namespace, "event-download-shutdown")
            .await
            .expect("inbound state"),
        Some(InboundEventState::Rejected)
    );
    assert!(
        store
            .list_attachments()
            .await
            .expect("attachments")
            .is_empty()
    );
    assert!(
        store
            .uncertain_turns()
            .await
            .expect("live turns")
            .is_empty()
    );
    assert_eq!(
        sink.finalizations.lock().expect("finalizations")[0].1,
        TurnResolution::Failed
    );
    drop(cache);
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn supervisor_epoch_loss_releases_uncertain_attachment_leases_in_process() {
    let config = validated_config();
    let workspace = config.default_workspace.clone().expect("workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = Arc::new(RecordingSink::default());
    let cache_temp = tempdir().expect("cache tempdir");
    let cache = Arc::new(
        AttachmentCache::open(
            &cache_temp.path().join("attachments"),
            store.clone(),
            Arc::new(StaticAttachmentDownloader),
            AttachmentLimits::default(),
        )
        .expect("attachment cache"),
    );
    let (supervisor, first_control, _second_control) = restarting_supervisor().await;
    let router = Router::start_with_attachments(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        supervisor,
        sink,
        Arc::clone(&cache),
    )
    .await
    .expect("router");
    let mut inbound = event("event-attachment-epoch-loss", "owner-runtime-scope");
    inbound.resources.push(ResourceDesc {
        kind: ResourceKind::Image,
        key: "img_key".to_owned(),
    });
    router
        .route(queued_registered(&store, &namespace, inbound).await)
        .await
        .expect("route attachment event");
    let start_thread = first_control.next_request().await;
    first_control
        .respond(
            &start_thread,
            thread_result("thread-attachment-epoch-loss", &workspace),
        )
        .await;
    let start_turn = first_control.next_request().await;
    respond_turn_started(&first_control, &start_turn, "turn-attachment-epoch-loss").await;
    wait_for_running_turn(&store, "turn-attachment-epoch-loss").await;
    let rows = store.list_attachments().await.expect("attachments");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        store
            .attachment_leases(&rows[0].sha256)
            .await
            .expect("attachment leases")
            .len(),
        1
    );

    first_control.unexpected_exit();
    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-attachment-epoch-loss"],
        InboundEventState::Rejected,
    )
    .await;
    timeout(Duration::from_secs(2), async {
        while !store
            .attachment_leases(&rows[0].sha256)
            .await
            .expect("attachment leases")
            .is_empty()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("old epoch attachment leases released");
    router.shutdown().await.expect("shutdown");
    drop(cache);
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn shutdown_reconciles_uncertain_attachment_leases_after_the_process_stops() {
    let config = validated_config();
    let workspace = config.default_workspace.clone().expect("workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let cache_temp = tempdir().expect("cache tempdir");
    let cache = Arc::new(
        AttachmentCache::open(
            &cache_temp.path().join("attachments"),
            store.clone(),
            Arc::new(StaticAttachmentDownloader),
            AttachmentLimits::default(),
        )
        .expect("attachment cache"),
    );
    let (supervisor, control) = ready_supervisor().await;
    let router = Router::start_with_attachments(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        supervisor,
        Arc::new(YieldingRecordingSink::default()),
        Arc::clone(&cache),
    )
    .await
    .expect("router");
    let mut inbound = event("event-attachment-shutdown", "owner-runtime-scope");
    inbound.resources.push(ResourceDesc {
        kind: ResourceKind::File,
        key: "file_key".to_owned(),
    });
    router
        .route(queued_registered(&store, &namespace, inbound).await)
        .await
        .expect("route attachment event");
    let start_thread = control.next_request().await;
    control
        .respond(
            &start_thread,
            thread_result("thread-attachment-shutdown", &workspace),
        )
        .await;
    let start_turn = control.next_request().await;
    respond_turn_started(&control, &start_turn, "turn-attachment-shutdown").await;
    wait_for_running_turn(&store, "turn-attachment-shutdown").await;
    let rows = store.list_attachments().await.expect("attachments");
    assert_eq!(rows.len(), 1);

    router.shutdown().await.expect("shutdown");
    assert_eq!(
        store
            .inbound_state(&namespace, "event-attachment-shutdown")
            .await
            .expect("inbound state"),
        Some(InboundEventState::Rejected)
    );
    assert!(
        store
            .attachment_leases(&rows[0].sha256)
            .await
            .expect("attachment leases")
            .is_empty()
    );
    drop(cache);
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
async fn shutdown_uses_a_fresh_deadline_for_an_async_uncertain_finalization() {
    let config = validated_config();
    let workspace = config.default_workspace.clone().expect("workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = Arc::new(YieldingRecordingSink::default());
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
                event("event-async-running-shutdown", "owner-runtime-scope"),
            )
            .await,
        )
        .await
        .expect("route event");
    let start_thread = control.next_request().await;
    control
        .respond(
            &start_thread,
            thread_result("thread-async-running-shutdown", &workspace),
        )
        .await;
    let start_turn = control.next_request().await;
    respond_turn_started(&control, &start_turn, "turn-async-running-shutdown").await;
    wait_for_running_turn(&store, "turn-async-running-shutdown").await;

    timeout(Duration::from_millis(500), router.shutdown())
        .await
        .expect("router shutdown deadline")
        .expect("router shutdown");
    assert_eq!(
        store
            .inbound_state(&namespace, "event-async-running-shutdown")
            .await
            .expect("inbound state"),
        Some(InboundEventState::Rejected)
    );
    {
        let finalizations = sink.finalizations.lock().expect("yielding finalizations");
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
    let settings = RouterSettings::from_config(&config)
        .with_test_shutdown_cleanup_timeout(Duration::from_millis(100));
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

#[tokio::test]
async fn supervisor_epoch_loss_finalizes_uncertain_before_queued_work_resumes() {
    let config = validated_config();
    let workspace = config.default_workspace.clone().expect("workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = Arc::new(RecordingSink::default());
    let (supervisor, first_control, second_control) = restarting_supervisor().await;
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
                event("event-epoch-first", "owner-runtime-scope"),
            )
            .await,
        )
        .await
        .expect("route first turn");
    let start_thread = first_control.next_request().await;
    first_control
        .respond(&start_thread, thread_result("thread-epoch", &workspace))
        .await;
    let first_turn = first_control.next_request().await;
    respond_turn_started(&first_control, &first_turn, "turn-epoch-first").await;
    wait_for_running_turn(&store, "turn-epoch-first").await;
    router
        .route(
            queued_registered(
                &store,
                &namespace,
                event("event-epoch-second", "owner-runtime-scope"),
            )
            .await,
        )
        .await
        .expect("queue second turn");

    first_control.unexpected_exit();
    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-epoch-first"],
        InboundEventState::Rejected,
    )
    .await;
    {
        let finalizations = sink.finalizations.lock().expect("finalizations");
        assert_eq!(finalizations.len(), 1);
        assert_eq!(finalizations[0].1, TurnResolution::Uncertain);
    }

    let resume = second_control.next_request().await;
    assert_eq!(resume["method"], "thread/resume");
    second_control
        .respond(&resume, thread_result("thread-epoch", &workspace))
        .await;
    let second_turn = second_control.next_request().await;
    assert_eq!(second_turn["method"], "turn/start");
    respond_turn_started(&second_control, &second_turn, "turn-epoch-second").await;
    send_turn_completed(
        &second_control,
        "thread-epoch",
        "turn-epoch-second",
        "completed",
    )
    .await;
    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-epoch-second"],
        InboundEventState::Completed,
    )
    .await;
    assert_eq!(sink.finalizations.lock().expect("finalizations").len(), 2);
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn supervisor_epoch_loss_is_observed_while_the_scope_mailbox_is_full() {
    let config = validated_config();
    let workspace = config.default_workspace.clone().expect("workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = Arc::new(RecordingSink::default());
    let (supervisor, first_control, _second_control) = restarting_supervisor().await;
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
                event("event-full-epoch-running", "owner-runtime-scope"),
            )
            .await,
        )
        .await
        .expect("route running turn");
    let start_thread = first_control.next_request().await;
    first_control
        .respond(
            &start_thread,
            thread_result("thread-full-epoch", &workspace),
        )
        .await;
    let start_turn = first_control.next_request().await;
    respond_turn_started(&first_control, &start_turn, "turn-full-epoch").await;
    wait_for_running_turn(&store, "turn-full-epoch").await;

    for index in 0..SCOPE_MAILBOX_CAPACITY {
        router
            .route(
                queued_registered(
                    &store,
                    &namespace,
                    event(
                        format!("event-full-epoch-pending-{index}").as_str(),
                        "owner-runtime-scope",
                    ),
                )
                .await,
            )
            .await
            .expect("fill scope mailbox");
    }

    first_control.unexpected_exit();
    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-full-epoch-running"],
        InboundEventState::Rejected,
    )
    .await;
    assert_eq!(
        sink.finalizations.lock().expect("finalizations")[0].1,
        TurnResolution::Uncertain
    );
    assert_eq!(
        store
            .inbound_state(
                &namespace,
                format!("event-full-epoch-pending-{}", SCOPE_MAILBOX_CAPACITY - 1).as_str(),
            )
            .await
            .expect("queued inbound state"),
        Some(InboundEventState::Received)
    );

    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn mixed_batch_omits_an_already_claimed_key_without_stranding_its_sibling() {
    let config = validated_config();
    let workspace = config.default_workspace.clone().expect("workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let first = event("event-mixed-claimed", "owner-runtime-scope");
    let second = event("event-mixed-received", "owner-runtime-scope");
    let first_queued = queued_registered(&store, &namespace, first.clone()).await;
    let second_queued = queued_registered(&store, &namespace, second).await;
    let seeded = store
        .begin_turn_and_claim_inbound(
            NewTurnRow {
                scope_key: first.scope.to_string(),
                client_message_id: "seeded-mixed-claim".to_owned(),
                codex_thread_id: Some("thread-seeded-mixed".to_owned()),
                state: TurnState::Starting,
            },
            &[InboundKey::new(
                namespace.clone(),
                "event-mixed-claimed".to_owned(),
            )],
        )
        .await
        .expect("seed one claimed key");
    let BeginTurnOutcome::Started {
        turn_row_id: seeded_turn,
        ..
    } = seeded
    else {
        panic!("seeded claim must create a turn");
    };
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
    router.route(first_queued).await.expect("route claimed key");
    router
        .route(second_queued)
        .await
        .expect("route received sibling");

    let start_thread = control.next_request().await;
    control
        .respond(&start_thread, thread_result("thread-mixed", &workspace))
        .await;
    let start_turn = control.next_request().await;
    assert_eq!(
        start_turn["params"]["input"]
            .as_array()
            .expect("input array")
            .len(),
        1
    );
    respond_turn_started(&control, &start_turn, "turn-mixed").await;
    send_turn_completed(&control, "thread-mixed", "turn-mixed", "completed").await;
    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-mixed-received"],
        InboundEventState::Completed,
    )
    .await;
    assert_eq!(
        store
            .inbound_state(&namespace, "event-mixed-claimed")
            .await
            .expect("claimed state"),
        Some(InboundEventState::Accepted)
    );
    store
        .resolve_turn_and_finish_inbound_batch(
            seeded_turn,
            TurnResolution::Uncertain,
            InboundTerminal::Rejected,
        )
        .await
        .expect("clean up seeded recovery turn");
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn full_scope_mailbox_atomically_rejects_the_overflow_with_a_busy_notice() {
    let config = validated_config();
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = Arc::new(RecordingSink::default());
    let router = Router::start(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        degraded_supervisor().await,
        sink.clone(),
    )
    .await
    .expect("router");
    router
        .route(
            queued_registered(
                &store,
                &namespace,
                event("event-mailbox-blocker", "owner-runtime-scope"),
            )
            .await,
        )
        .await
        .expect("route blocker");
    sleep(Duration::from_millis(700)).await;
    for index in 0..SCOPE_MAILBOX_CAPACITY {
        router
            .route(
                queued_registered(
                    &store,
                    &namespace,
                    event(
                        format!("event-mailbox-pending-{index}").as_str(),
                        "owner-runtime-scope",
                    ),
                )
                .await,
            )
            .await
            .expect("fill bounded mailbox");
    }
    router
        .route(
            queued_registered(
                &store,
                &namespace,
                event("event-mailbox-overflow", "owner-runtime-scope"),
            )
            .await,
        )
        .await
        .expect("overflow becomes a durable rejection");
    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-mailbox-overflow"],
        InboundEventState::Rejected,
    )
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
async fn router_command_byte_budget_refuses_an_oversized_retained_item() {
    let config = validated_config();
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let router = Router::start(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        degraded_supervisor().await,
        Arc::new(RecordingSink::default()),
    )
    .await
    .expect("router");
    let mut queued = queued_registered(
        &store,
        &namespace,
        event("event-router-byte-overflow", "owner-runtime-scope"),
    )
    .await;
    let retained_bytes = ROUTER_COMMAND_BYTE_BUDGET + 1;
    queued.permit = Arc::new(Semaphore::new(retained_bytes))
        .acquire_many_owned(u32::try_from(retained_bytes).expect("retained bytes fit"))
        .await
        .expect("oversized synthetic retained permit");

    assert!(matches!(
        router.route(queued).await,
        Err(RouteError::Capacity)
    ));
    assert_eq!(
        store
            .inbound_state(&namespace, "event-router-byte-overflow")
            .await
            .expect("inbound state"),
        Some(InboundEventState::Received)
    );

    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn connection_loss_after_turn_start_write_is_uncertain_and_never_auto_resent() {
    let config = validated_config();
    let workspace = config.default_workspace.clone().expect("workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = Arc::new(RecordingSink::default());
    let (supervisor, first_control, second_control) = restarting_supervisor().await;
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
                event("event-start-uncertain", "owner-runtime-scope"),
            )
            .await,
        )
        .await
        .expect("route event");
    let start_thread = first_control.next_request().await;
    first_control
        .respond(
            &start_thread,
            thread_result("thread-start-uncertain", &workspace),
        )
        .await;
    let start_turn = first_control.next_request().await;
    assert_eq!(start_turn["method"], "turn/start");
    first_control.unexpected_exit();

    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-start-uncertain"],
        InboundEventState::Rejected,
    )
    .await;
    assert_eq!(
        sink.finalizations.lock().expect("finalizations")[0].1,
        TurnResolution::Uncertain
    );
    assert!(
        store
            .uncertain_turns()
            .await
            .expect("live turns")
            .is_empty()
    );
    second_control
        .expect_no_request_for(Duration::from_millis(100))
        .await;
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn busy_actor_registry_rejects_a_new_scope_without_evicting_live_work() {
    let mut config = validated_config();
    config.concurrency.max_scope_actors = 1;
    config.validate().expect("single actor is valid");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let sink = Arc::new(RecordingSink::default());
    let router = Router::start(
        store.clone(),
        namespace.clone(),
        policy,
        settings,
        degraded_supervisor().await,
        sink.clone(),
    )
    .await
    .expect("router");
    router
        .route(
            queued_registered(
                &store,
                &namespace,
                event_in_chat("event-actor-live", "owner-runtime-scope", "chat-live"),
            )
            .await,
        )
        .await
        .expect("route live actor");
    sleep(Duration::from_millis(700)).await;
    router
        .route(
            queued_registered(
                &store,
                &namespace,
                event_in_chat("event-actor-overflow", "owner-runtime-scope", "chat-new"),
            )
            .await,
        )
        .await
        .expect("new scope becomes a durable overload rejection");
    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-actor-overflow"],
        InboundEventState::Rejected,
    )
    .await;
    assert_eq!(router.snapshot().scope_count, 1);
    assert_eq!(
        *sink.rejections.lock().expect("rejections"),
        vec![InboundRejectionKind::Overloaded]
    );
    assert_eq!(
        store
            .inbound_state(&namespace, "event-actor-live")
            .await
            .expect("live state"),
        Some(InboundEventState::Received)
    );
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn scope_snapshot_reports_only_structural_state_and_mailbox_depth() {
    let config = validated_config();
    let workspace = config.default_workspace.clone().expect("workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let (supervisor, control) = ready_supervisor().await;
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
    let first = event("event-snapshot-first", "owner-runtime-scope");
    let scope = first.scope.clone();
    assert_eq!(router.scope_snapshot(&scope).await.expect("snapshot"), None);
    router
        .route(queued_registered(&store, &namespace, first).await)
        .await
        .expect("route first turn");
    let start_thread = control.next_request().await;
    control
        .respond(&start_thread, thread_result("thread-snapshot", &workspace))
        .await;
    let start_turn = control.next_request().await;
    respond_turn_started(&control, &start_turn, "turn-snapshot").await;
    wait_for_running_turn(&store, "turn-snapshot").await;
    router
        .route(
            queued_registered(
                &store,
                &namespace,
                event("event-snapshot-queued", "owner-runtime-scope"),
            )
            .await,
        )
        .await
        .expect("queue next turn");
    let snapshot = router
        .scope_snapshot(&scope)
        .await
        .expect("snapshot")
        .expect("scope exists");
    assert!(matches!(
        snapshot.state,
        lark_codex_bridge::runtime::scope::ScopeState::Running { .. }
    ));
    assert_eq!(snapshot.queued_messages, 1);
    let debug = format!("{snapshot:?}");
    assert!(!debug.contains("owner-runtime-scope"));
    assert!(!debug.contains("event-snapshot"));
    assert!(!debug.contains(workspace.to_string_lossy().as_ref()));

    send_turn_completed(&control, "thread-snapshot", "turn-snapshot", "completed").await;
    let resume = control.next_request().await;
    control
        .respond(&resume, thread_result("thread-snapshot", &workspace))
        .await;
    let queued_turn = control.next_request().await;
    respond_turn_started(&control, &queued_turn, "turn-snapshot-queued").await;
    send_turn_completed(
        &control,
        "thread-snapshot",
        "turn-snapshot-queued",
        "completed",
    )
    .await;
    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-snapshot-first", "event-snapshot-queued"],
        InboundEventState::Completed,
    )
    .await;
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn scope_mailbox_byte_budget_rejects_before_the_durable_inbox_limit() {
    const LARGE_TEXT_BYTES: usize = 700 * 1024;

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
    let mut blocker = event("event-byte-blocker", "owner-runtime-scope");
    blocker.text = "b".repeat(LARGE_TEXT_BYTES);
    router
        .route(queued_registered(&store, &namespace, blocker).await)
        .await
        .expect("route byte blocker");
    let start_thread = control.next_request().await;
    control
        .respond(
            &start_thread,
            thread_result("thread-byte-budget", &workspace),
        )
        .await;
    let start_turn = control.next_request().await;
    respond_turn_started(&control, &start_turn, "turn-byte-budget").await;
    wait_for_running_turn(&store, "turn-byte-budget").await;

    let mut rejected = None;
    for index in 0..16 {
        let event_id = format!("event-byte-pending-{index}");
        let mut pending = event(event_id.as_str(), "owner-runtime-scope");
        pending.text = "p".repeat(LARGE_TEXT_BYTES);
        router
            .route(queued_registered(&store, &namespace, pending).await)
            .await
            .expect("route or durably reject byte pressure");
        if store
            .inbound_state(&namespace, event_id.as_str())
            .await
            .expect("inbound state")
            == Some(InboundEventState::Rejected)
        {
            rejected = Some(event_id);
            break;
        }
    }
    assert!(
        rejected.is_some(),
        "actor byte budget must reject bounded input"
    );
    assert_eq!(
        sink.rejections.lock().expect("rejections").as_slice(),
        &[InboundRejectionKind::Overloaded]
    );
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn idle_actor_eviction_releases_completed_thread_routes() {
    let mut config = validated_config();
    config.concurrency.max_scope_actors = 1;
    config.validate().expect("single actor is valid");
    let workspace = config.default_workspace.clone().expect("workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config).with_test_timings(
        Duration::from_millis(1),
        Duration::from_secs(60),
        Duration::from_millis(1),
    );
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let (supervisor, control) = ready_supervisor().await;
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

    for index in 0..=lark_codex_bridge::limits::CLIENT_PROJECTION_CAPACITY {
        let event_id = format!("event-evict-{index}");
        let chat_id = format!("chat-evict-{index}");
        let thread_id = format!("thread-evict-{index}");
        let turn_id = format!("turn-evict-{index}");
        router
            .route(
                queued_registered(
                    &store,
                    &namespace,
                    event_in_chat(event_id.as_str(), "owner-runtime-scope", chat_id.as_str()),
                )
                .await,
            )
            .await
            .expect("route next scope");
        let start_thread = control.next_request().await;
        assert_eq!(start_thread["method"], "thread/start");
        control
            .respond(&start_thread, thread_result(thread_id.as_str(), &workspace))
            .await;
        let start_turn = control.next_request().await;
        respond_turn_started(&control, &start_turn, turn_id.as_str()).await;
        send_turn_completed(&control, thread_id.as_str(), turn_id.as_str(), "completed").await;
        wait_for_inbound_states(
            &store,
            &namespace,
            &[event_id.as_str()],
            InboundEventState::Completed,
        )
        .await;
    }
    assert_eq!(router.snapshot().scope_count, 1);
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn connection_loss_before_atomic_begin_leaves_the_event_received_for_replay() {
    let config = validated_config();
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config).with_test_timings(
        Duration::from_millis(1),
        Duration::from_secs(60),
        Duration::from_millis(1),
    );
    let namespace = TenantNamespace::from_credentials(&credentials());
    let store = StoreHandle::open_in_memory().await.expect("store");
    let (supervisor, first_control, second_control) = restarting_supervisor().await;
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
                event("event-before-begin-crash", "owner-runtime-scope"),
            )
            .await,
        )
        .await
        .expect("route event");
    let start_thread = first_control.next_request().await;
    assert_eq!(start_thread["method"], "thread/start");
    first_control.unexpected_exit();
    timeout(Duration::from_secs(2), async {
        loop {
            if router
                .scope_snapshot(&ScopeKey::Chat("chat-runtime-scope".to_owned()))
                .await
                .expect("snapshot")
                .is_some_and(|snapshot| {
                    matches!(
                        snapshot.state,
                        lark_codex_bridge::runtime::scope::ScopeState::Failed { .. }
                    )
                })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actor records the pre-begin failure");
    assert_eq!(
        store
            .inbound_state(&namespace, "event-before-begin-crash")
            .await
            .expect("inbound state"),
        Some(InboundEventState::Received)
    );
    assert!(
        store
            .uncertain_turns()
            .await
            .expect("live turns")
            .is_empty()
    );
    second_control
        .expect_no_request_for(Duration::from_millis(100))
        .await;
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

fn stub_program(
    dir: &std::path::Path,
    name: &str,
    unix: &str,
    #[allow(unused_variables)] windows: &str,
) -> std::path::PathBuf {
    #[cfg(windows)]
    {
        let path = dir.join(format!("{name}.cmd"));
        std::fs::write(&path, windows).expect("write windows stub");
        path
    }
    #[cfg(not(windows))]
    {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join(name);
        std::fs::write(&path, format!("#!/bin/sh\n{unix}\n")).expect("write unix stub");
        std::fs::set_permissions(&path, PermissionsExt::from_mode(0o755)).expect("chmod stub");
        path
    }
}

fn ffmpeg_stub(dir: &std::path::Path) -> std::path::PathBuf {
    let fixture = dir.join("decoded-audio.wav");
    let samples = 160_u32;
    let data_bytes = samples * 2;
    let mut wav = Vec::with_capacity(44 + data_bytes as usize);
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&(36_u32 + data_bytes).to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16_u32.to_le_bytes());
    wav.extend_from_slice(&1_u16.to_le_bytes());
    wav.extend_from_slice(&1_u16.to_le_bytes());
    wav.extend_from_slice(&16_000_u32.to_le_bytes());
    wav.extend_from_slice(&32_000_u32.to_le_bytes());
    wav.extend_from_slice(&2_u16.to_le_bytes());
    wav.extend_from_slice(&16_u16.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&data_bytes.to_le_bytes());
    wav.resize(44 + data_bytes as usize, 0);
    std::fs::write(&fixture, wav).expect("write decoded WAV fixture");
    let source = fixture.to_string_lossy();
    stub_program(
        dir,
        "ffmpeg",
        &format!(
            r#"out=""; for arg in "$@"; do out=$arg; done; cp "{}" "$out""#,
            source.replace('"', r#"\""#)
        ),
        &format!(
            "@echo off\r\n:loop\r\nif \"%~2\"==\"\" (\r\ncopy /y \"{source}\" \"%~1\" >nul\r\nexit /b %errorlevel%\r\n)\r\nshift\r\ngoto loop\r\n"
        ),
    )
}

fn asr_config(
    command: std::path::PathBuf,
    ffmpeg: std::path::PathBuf,
    args: Vec<String>,
) -> AsrSection {
    AsrSection {
        command: Some(command),
        args,
        ffmpeg,
        ..AsrSection::default()
    }
}

fn audio_part(transcript: Option<&str>) -> MessagePart {
    MessagePart::Audio(MediaPart {
        key: Some("aud_key".to_owned()),
        thumbnail_key: None,
        metadata: MediaMetadata {
            duration_ms: Some(800),
            transcript: transcript.map(ToOwned::to_owned),
            ..MediaMetadata::default()
        },
        status: PartStatus::Available,
    })
}

async fn start_audio_router(
    config: BridgeConfig,
    store: StoreHandle,
    cache: Arc<AttachmentCache>,
) -> (
    lark_codex_bridge::runtime::router::RouterHandle,
    fakecodex::FakeControl,
    TenantNamespace,
    std::path::PathBuf,
) {
    let workspace = config
        .default_workspace
        .clone()
        .expect("validated default workspace");
    let policy = AccessPolicy::from_config(&config).expect("policy");
    let settings = RouterSettings::from_config(&config);
    let namespace = TenantNamespace::from_credentials(&credentials());
    let sink = Arc::new(RecordingSink::default());
    let contexts = Arc::new(ContextRegistry::default());
    let (supervisor, control) = ready_supervisor().await;
    let router = Router::start_with_contexts(
        store,
        namespace.clone(),
        policy,
        settings,
        supervisor,
        sink,
        cache,
        contexts,
    )
    .await
    .expect("router");
    (router, control, namespace, workspace)
}

#[allow(clippy::too_many_arguments)]
async fn route_audio_event(
    router: &lark_codex_bridge::runtime::router::RouterHandle,
    store: &StoreHandle,
    namespace: &TenantNamespace,
    control: &fakecodex::FakeControl,
    workspace: &std::path::Path,
    event_id: &str,
    inbound: InboundEvent,
    thread_id: &str,
    turn_id: &str,
) -> (String, String, Value) {
    router
        .route(queued_registered(store, namespace, inbound).await)
        .await
        .expect("route audio event");
    let start_thread = control.next_request().await;
    assert_eq!(start_thread["method"], "thread/start");
    control
        .respond(&start_thread, thread_result(thread_id, workspace))
        .await;
    let start_turn = control.next_request().await;
    let inputs = start_turn["params"]["input"]
        .as_array()
        .expect("turn input array");
    assert!(
        inputs
            .iter()
            .all(|input| input["type"] != "localAudio" && input["type"] != "audio"),
        "audio must not be sent as Codex user input"
    );
    let reference = inputs
        .iter()
        .filter_map(|input| input["text"].as_str())
        .find_map(|text| {
            text.strip_prefix("<bridge_context>")
                .and_then(|value| value.strip_suffix("</bridge_context>"))
        })
        .expect("compact context envelope");
    let reference: Value = serde_json::from_str(reference).expect("context JSON");
    let context_id = reference["id"]
        .as_str()
        .expect("opaque context id")
        .to_owned();
    respond_turn_started(control, &start_turn, turn_id).await;
    control
        .send_json(json!({
            "id": format!("server-context-{event_id}"),
            "method": "item/tool/call",
            "params": {
                "threadId": thread_id,
                "turnId": turn_id,
                "callId": format!("call-context-{event_id}"),
                "namespace": "bridge_context",
                "tool": "resolve",
                "arguments": {"id": context_id}
            }
        }))
        .await;
    let context_response = control.next_request().await;
    assert_eq!(context_response["result"]["success"], true);
    let context_text = context_response["result"]["contentItems"][0]["text"]
        .as_str()
        .expect("context result text");
    let context_value: Value = serde_json::from_str(context_text).expect("context result JSON");
    let handle = context_value["parts"]
        .as_array()
        .expect("parts")
        .iter()
        .find(|part| part["kind"] == "audio")
        .and_then(|part| part["handle"].as_str())
        .expect("audio handle")
        .to_owned();
    (context_id, handle, start_turn)
}

async fn read_media(
    control: &fakecodex::FakeControl,
    thread_id: &str,
    turn_id: &str,
    request_id: &str,
    context_id: &str,
    handle: &str,
) -> Value {
    control
        .send_json(json!({
            "id": request_id,
            "method": "item/tool/call",
            "params": {
                "threadId": thread_id,
                "turnId": turn_id,
                "callId": request_id,
                "namespace": "bridge_media",
                "tool": "read",
                "arguments": {"context_id": context_id, "handle": handle}
            }
        }))
        .await;
    // Local process startup is intentionally tested through the real Tokio
    // process boundary and can be slower under a fully parallel CI suite.
    control.next_request_within(Duration::from_secs(10)).await
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn audio_media_read_uses_stub_sidecar_and_leaves_image_path_reads_unchanged() {
    let _asr_subprocess_guard = ASR_SUBPROCESS_TEST_LOCK.lock().await;
    let _asr_process_lock = lock_asr_process_tests();
    let mut config = validated_config();
    let temp = tempdir().expect("tempdir");
    let marker = temp.path().join("asr-invoked.txt");
    let asr = stub_program(
        temp.path(),
        "asr",
        r#"printf 'invoked\n' >> "$1"; printf 'KNOWN TRANSCRIPT\n'"#,
        "@echo off\r\necho invoked>>\"%~1\"\r\necho KNOWN TRANSCRIPT\r\n",
    );
    config.asr = asr_config(
        asr,
        ffmpeg_stub(temp.path()),
        vec![marker.to_string_lossy().into_owned()],
    );
    let store = StoreHandle::open_in_memory().await.expect("store");
    let cache = Arc::new(
        AttachmentCache::open(
            &temp.path().join("attachments-live"),
            store.clone(),
            Arc::new(StaticAttachmentDownloader),
            AttachmentLimits::default(),
        )
        .expect("attachment cache"),
    );
    let (router, control, namespace, workspace) =
        start_audio_router(config, store.clone(), Arc::clone(&cache)).await;

    let mut inbound = event("event-audio-sidecar", "owner-runtime-scope");
    inbound.message_type = "audio".to_owned();
    inbound.text = "hello".to_owned();
    inbound.parts = vec![
        MessagePart::Text {
            text: "hello".to_owned(),
        },
        MessagePart::Image(MediaPart {
            key: Some("img_key".to_owned()),
            thumbnail_key: None,
            metadata: MediaMetadata::default(),
            status: PartStatus::Available,
        }),
        audio_part(None),
    ];

    router
        .route(queued_registered(&store, &namespace, inbound).await)
        .await
        .expect("route");
    let start_thread = control.next_request().await;
    control
        .respond(
            &start_thread,
            thread_result("thread-audio-sidecar", &workspace),
        )
        .await;
    let start_turn = control.next_request().await;
    let inputs = start_turn["params"]["input"]
        .as_array()
        .expect("turn inputs");
    assert_eq!(inputs.len(), 2, "media must not be downloaded eagerly");
    assert!(
        inputs
            .iter()
            .all(|input| input["type"] != "localAudio" && input["type"] != "localImage"),
        "audio and images stay lazy"
    );
    let reference = inputs[1]["text"].as_str().expect("context reference");
    let payload = reference
        .strip_prefix("<bridge_context>")
        .and_then(|value| value.strip_suffix("</bridge_context>"))
        .expect("compact context envelope");
    let reference: Value = serde_json::from_str(payload).expect("context JSON");
    let context_id = reference["id"].as_str().expect("opaque context id");
    respond_turn_started(&control, &start_turn, "turn-audio-sidecar").await;

    control
        .send_json(json!({
            "id": "server-context-audio",
            "method": "item/tool/call",
            "params": {
                "threadId": "thread-audio-sidecar",
                "turnId": "turn-audio-sidecar",
                "callId": "call-context-audio",
                "namespace": "bridge_context",
                "tool": "resolve",
                "arguments": {"id": context_id}
            }
        }))
        .await;
    let context_response = control.next_request().await;
    let context_text = context_response["result"]["contentItems"][0]["text"]
        .as_str()
        .expect("context result text");
    let context_value: Value = serde_json::from_str(context_text).expect("context result JSON");
    let image_handle = context_value["parts"][1]["handle"]
        .as_str()
        .expect("image handle");
    let audio_handle = context_value["parts"][2]["handle"]
        .as_str()
        .expect("audio handle");

    let image_response = read_media(
        &control,
        "thread-audio-sidecar",
        "turn-audio-sidecar",
        "server-image-read",
        context_id,
        image_handle,
    )
    .await;
    assert_eq!(image_response["result"]["success"], true);
    let image_text = image_response["result"]["contentItems"][0]["text"]
        .as_str()
        .expect("image result");
    let image_value: Value = serde_json::from_str(image_text).expect("image JSON");
    assert!(
        std::path::Path::new(image_value["media"]["path"].as_str().expect("image path")).is_file()
    );
    assert_eq!(image_value["media"]["bytes"], 16);
    assert!(image_value["media"]["transcript"].is_null());

    let audio_response = read_media(
        &control,
        "thread-audio-sidecar",
        "turn-audio-sidecar",
        "server-audio-read",
        context_id,
        audio_handle,
    )
    .await;
    assert_eq!(audio_response["result"]["success"], true);
    let audio_text = audio_response["result"]["contentItems"][0]["text"]
        .as_str()
        .expect("audio result");
    let audio_value: Value = serde_json::from_str(audio_text).expect("audio JSON");
    assert_eq!(audio_value["media"]["transcript"], "KNOWN TRANSCRIPT");
    assert_eq!(audio_value["media"]["source"], "sidecar");
    assert!(audio_value["media"]["path"].is_null());
    assert!(
        std::fs::read_to_string(&marker)
            .expect("marker")
            .contains("invoked")
    );

    send_turn_completed(
        &control,
        "thread-audio-sidecar",
        "turn-audio-sidecar",
        "completed",
    )
    .await;
    router.shutdown().await.expect("shutdown");
    drop(cache);
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn inbound_audio_transcript_skips_sidecar() {
    let mut config = validated_config();
    let temp = tempdir().expect("tempdir");
    let marker = temp.path().join("must-not-run.txt");
    let exploding = stub_program(
        temp.path(),
        "asr-explode",
        r#"printf 'invoked\n' >> "$1"; exit 99"#,
        "@echo off\r\necho invoked>>\"%~1\"\r\nexit /b 99\r\n",
    );
    config.asr = asr_config(
        exploding,
        ffmpeg_stub(temp.path()),
        vec![marker.to_string_lossy().into_owned()],
    );
    let store = StoreHandle::open_in_memory().await.expect("store");
    let cache = Arc::new(
        AttachmentCache::open(
            &temp.path().join("attachments"),
            store.clone(),
            Arc::new(StaticAttachmentDownloader),
            AttachmentLimits::default(),
        )
        .expect("attachment cache"),
    );
    let (router, control, namespace, workspace) =
        start_audio_router(config, store.clone(), cache).await;
    let mut inbound = event("event-audio-inbound", "owner-runtime-scope");
    inbound.message_type = "audio".to_owned();
    inbound.text.clear();
    inbound.parts = vec![audio_part(Some("please review the patch"))];
    let (context_id, handle, _) = route_audio_event(
        &router,
        &store,
        &namespace,
        &control,
        &workspace,
        "inbound",
        inbound,
        "thread-audio-inbound",
        "turn-audio-inbound",
    )
    .await;
    let response = read_media(
        &control,
        "thread-audio-inbound",
        "turn-audio-inbound",
        "server-audio-inbound",
        &context_id,
        &handle,
    )
    .await;
    assert_eq!(response["result"]["success"], true);
    let body: Value = serde_json::from_str(
        response["result"]["contentItems"][0]["text"]
            .as_str()
            .expect("text"),
    )
    .expect("json");
    assert_eq!(body["media"]["transcript"], "please review the patch");
    assert_eq!(body["media"]["source"], "inbound");
    assert!(!marker.exists(), "sidecar must not run for inbound text");
    send_turn_completed(
        &control,
        "thread-audio-inbound",
        "turn-audio-inbound",
        "completed",
    )
    .await;
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn configured_inbound_transcript_limit_is_enforced_only_at_media_read() {
    let mut config = validated_config();
    let temp = tempdir().expect("tempdir");
    let marker = temp.path().join("must-not-run-over-limit.txt");
    let exploding = stub_program(
        temp.path(),
        "asr-over-limit",
        r#"printf 'invoked\n' >> "$1"; exit 99"#,
        "@echo off\r\necho invoked>>\"%~1\"\r\nexit /b 99\r\n",
    );
    config.asr = asr_config(
        exploding,
        ffmpeg_stub(temp.path()),
        vec![marker.to_string_lossy().into_owned()],
    );
    config.asr.max_transcript_bytes = 4;
    let store = StoreHandle::open_in_memory().await.expect("store");
    let download_count = Arc::new(AtomicUsize::new(0));
    let cache = Arc::new(
        AttachmentCache::open(
            &temp.path().join("attachments-over-limit"),
            store.clone(),
            Arc::new(PendingAttachmentDownloader {
                started: Arc::clone(&download_count),
                started_notify: Arc::new(Notify::new()),
            }),
            AttachmentLimits::default(),
        )
        .expect("attachment cache"),
    );
    let (router, control, namespace, workspace) =
        start_audio_router(config, store.clone(), cache).await;
    let mut inbound = event("event-audio-over-limit", "owner-runtime-scope");
    inbound.message_type = "audio".to_owned();
    inbound.text.clear();
    inbound.parts = vec![audio_part(Some("private transcript"))];
    let (context_id, handle, start_turn) = route_audio_event(
        &router,
        &store,
        &namespace,
        &control,
        &workspace,
        "over-limit",
        inbound,
        "thread-audio-over-limit",
        "turn-audio-over-limit",
    )
    .await;
    assert!(
        !start_turn.to_string().contains("private transcript"),
        "recognition text must remain behind the turn-scoped media capability"
    );
    let response = read_media(
        &control,
        "thread-audio-over-limit",
        "turn-audio-over-limit",
        "server-audio-over-limit",
        &context_id,
        &handle,
    )
    .await;
    assert_eq!(response["result"]["success"], false);
    let body: Value = serde_json::from_str(
        response["result"]["contentItems"][0]["text"]
            .as_str()
            .expect("tool response text"),
    )
    .expect("tool response JSON");
    assert_eq!(body["error"]["code"], "transcript_too_large");
    assert_eq!(download_count.load(Ordering::SeqCst), 0);
    assert!(!marker.exists(), "over-limit inbound text must not run ASR");
    send_turn_completed(
        &control,
        "thread-audio-over-limit",
        "turn-audio-over-limit",
        "completed",
    )
    .await;
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn interrupt_cancels_an_active_audio_download_and_returns_a_tool_result() {
    let mut config = validated_config();
    let temp = tempdir().expect("tempdir");
    config.asr = asr_config(
        stub_program(
            temp.path(),
            "asr-unused-on-cancel",
            "printf unexpected",
            "@echo off\r\necho unexpected\r\n",
        ),
        ffmpeg_stub(temp.path()),
        Vec::new(),
    );
    let store = StoreHandle::open_in_memory().await.expect("store");
    let started = Arc::new(AtomicUsize::new(0));
    let started_notify = Arc::new(Notify::new());
    let cache = Arc::new(
        AttachmentCache::open(
            &temp.path().join("attachments-cancelled-download"),
            store.clone(),
            Arc::new(PendingAttachmentDownloader {
                started: Arc::clone(&started),
                started_notify: Arc::clone(&started_notify),
            }),
            AttachmentLimits::default(),
        )
        .expect("attachment cache"),
    );
    let (router, control, namespace, workspace) =
        start_audio_router(config, store.clone(), cache).await;
    let mut inbound = event("event-audio-cancel-download", "owner-runtime-scope");
    inbound.message_type = "audio".to_owned();
    inbound.text.clear();
    inbound.parts = vec![audio_part(None)];
    let scope = inbound.scope.clone();
    let (context_id, handle, _) = route_audio_event(
        &router,
        &store,
        &namespace,
        &control,
        &workspace,
        "cancel-download",
        inbound,
        "thread-audio-cancel-download",
        "turn-audio-cancel-download",
    )
    .await;
    control
        .send_json(json!({
            "id": "server-audio-cancel-download",
            "method": "item/tool/call",
            "params": {
                "threadId": "thread-audio-cancel-download",
                "turnId": "turn-audio-cancel-download",
                "callId": "server-audio-cancel-download",
                "namespace": "bridge_media",
                "tool": "read",
                "arguments": {"context_id": context_id, "handle": handle}
            }
        }))
        .await;
    timeout(Duration::from_secs(2), async {
        while started.load(Ordering::SeqCst) == 0 {
            started_notify.notified().await;
        }
    })
    .await
    .expect("audio download starts");

    let (interrupt, ()) = tokio::join!(router.interrupt(&scope), async {
        let request = control.next_request().await;
        assert_eq!(request["method"], "turn/interrupt");
        control.respond(&request, json!({})).await;
    });
    assert_eq!(
        interrupt.expect("interrupt request"),
        InterruptOutcome::Requested
    );
    let response = timeout(Duration::from_secs(2), control.next_request())
        .await
        .expect("cancelled media tool responds");
    assert_eq!(response["id"], "server-audio-cancel-download");
    assert_eq!(response["result"]["success"], false);
    let body: Value = serde_json::from_str(
        response["result"]["contentItems"][0]["text"]
            .as_str()
            .expect("tool response text"),
    )
    .expect("tool response JSON");
    assert_eq!(body["error"]["code"], "cancelled");
    assert!(
        store
            .list_attachments()
            .await
            .expect("attachments")
            .is_empty()
    );
    send_turn_completed(
        &control,
        "thread-audio-cancel-download",
        "turn-audio-cancel-download",
        "interrupted",
    )
    .await;
    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-audio-cancel-download"],
        InboundEventState::Rejected,
    )
    .await;
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn interrupt_cancels_an_active_sidecar_and_releases_its_exact_lease() {
    let _asr_subprocess_guard = ASR_SUBPROCESS_TEST_LOCK.lock().await;
    let _asr_process_lock = lock_asr_process_tests();
    let mut config = validated_config();
    let temp = tempdir().expect("tempdir");
    let marker = temp.path().join("active-sidecar.txt");
    config.asr = asr_config(
        stub_program(
            temp.path(),
            "asr-block-until-cancelled",
            r#"printf '%s' "$$" > "$1"; exec sleep 60"#,
            "@echo off\r\necho invoked>\"%~1\"\r\n:loop\r\ngoto loop\r\n",
        ),
        ffmpeg_stub(temp.path()),
        vec![marker.to_string_lossy().into_owned()],
    );
    let store = StoreHandle::open_in_memory().await.expect("store");
    let cache = Arc::new(
        AttachmentCache::open(
            &temp.path().join("attachments-cancelled-sidecar"),
            store.clone(),
            Arc::new(StaticAttachmentDownloader),
            AttachmentLimits::default(),
        )
        .expect("attachment cache"),
    );
    let (router, control, namespace, workspace) =
        start_audio_router(config, store.clone(), Arc::clone(&cache)).await;
    let mut inbound = event("event-audio-cancel-sidecar", "owner-runtime-scope");
    inbound.message_type = "audio".to_owned();
    inbound.text.clear();
    inbound.parts = vec![audio_part(None)];
    let scope = inbound.scope.clone();
    let (context_id, handle, _) = route_audio_event(
        &router,
        &store,
        &namespace,
        &control,
        &workspace,
        "cancel-sidecar",
        inbound,
        "thread-audio-cancel-sidecar",
        "turn-audio-cancel-sidecar",
    )
    .await;
    control
        .send_json(json!({
            "id": "server-audio-cancel-sidecar",
            "method": "item/tool/call",
            "params": {
                "threadId": "thread-audio-cancel-sidecar",
                "turnId": "turn-audio-cancel-sidecar",
                "callId": "server-audio-cancel-sidecar",
                "namespace": "bridge_media",
                "tool": "read",
                "arguments": {"context_id": context_id, "handle": handle}
            }
        }))
        .await;
    timeout(Duration::from_secs(3), async {
        while !marker.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("sidecar starts");
    let rows = store.list_attachments().await.expect("attachments");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        store
            .attachment_leases(&rows[0].sha256)
            .await
            .expect("active ASR lease")
            .len(),
        1
    );

    let (interrupt, ()) = tokio::join!(router.interrupt(&scope), async {
        let request = control.next_request().await;
        assert_eq!(request["method"], "turn/interrupt");
        control.respond(&request, json!({})).await;
    });
    assert_eq!(
        interrupt.expect("interrupt request"),
        InterruptOutcome::Requested
    );
    let response = timeout(Duration::from_secs(2), control.next_request())
        .await
        .expect("cancelled ASR tool responds");
    assert_eq!(response["id"], "server-audio-cancel-sidecar");
    assert_eq!(response["result"]["success"], false);
    let body: Value = serde_json::from_str(
        response["result"]["contentItems"][0]["text"]
            .as_str()
            .expect("tool response text"),
    )
    .expect("tool response JSON");
    assert_eq!(body["error"]["code"], "cancelled");
    timeout(Duration::from_secs(2), async {
        while !store
            .attachment_leases(&rows[0].sha256)
            .await
            .expect("cancelled ASR leases")
            .is_empty()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled ASR releases its exact lease");
    #[cfg(unix)]
    {
        let pid = std::fs::read_to_string(&marker)
            .expect("sidecar pid marker")
            .parse::<u32>()
            .expect("sidecar pid");
        timeout(Duration::from_secs(2), async {
            while std::process::Command::new("kill")
                .args(["-0", &pid.to_string()])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .is_ok_and(|status| status.success())
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("exec-ed sidecar process exits on cancellation");
    }
    send_turn_completed(
        &control,
        "thread-audio-cancel-sidecar",
        "turn-audio-cancel-sidecar",
        "interrupted",
    )
    .await;
    wait_for_inbound_states(
        &store,
        &namespace,
        &["event-audio-cancel-sidecar"],
        InboundEventState::Rejected,
    )
    .await;
    router.shutdown().await.expect("shutdown");
    drop(cache);
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn missing_sidecar_returns_structured_audio_error() {
    let config = validated_config();
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let download_count = Arc::new(AtomicUsize::new(0));
    let cache = Arc::new(
        AttachmentCache::open(
            &temp.path().join("attachments"),
            store.clone(),
            Arc::new(PendingAttachmentDownloader {
                started: Arc::clone(&download_count),
                started_notify: Arc::new(Notify::new()),
            }),
            AttachmentLimits::default(),
        )
        .expect("attachment cache"),
    );
    let (router, control, namespace, workspace) =
        start_audio_router(config, store.clone(), cache).await;
    let mut inbound = event("event-audio-missing", "owner-runtime-scope");
    inbound.message_type = "audio".to_owned();
    inbound.text.clear();
    inbound.parts = vec![audio_part(None)];
    let (context_id, handle, _) = route_audio_event(
        &router,
        &store,
        &namespace,
        &control,
        &workspace,
        "missing",
        inbound,
        "thread-audio-missing",
        "turn-audio-missing",
    )
    .await;
    let response = read_media(
        &control,
        "thread-audio-missing",
        "turn-audio-missing",
        "server-audio-missing",
        &context_id,
        &handle,
    )
    .await;
    assert_eq!(response["result"]["success"], false);
    let body: Value = serde_json::from_str(
        response["result"]["contentItems"][0]["text"]
            .as_str()
            .expect("text"),
    )
    .expect("json");
    assert_eq!(body["error"]["code"], "sidecar_missing");
    assert_eq!(
        download_count.load(Ordering::SeqCst),
        0,
        "missing sidecar must fail before media download"
    );
    send_turn_completed(
        &control,
        "thread-audio-missing",
        "turn-audio-missing",
        "completed",
    )
    .await;
    router.shutdown().await.expect("shutdown");
    store.shutdown().await.expect("store shutdown");
}

#[tokio::test]
async fn empty_and_failing_sidecar_return_structured_audio_errors() {
    let _asr_subprocess_guard = ASR_SUBPROCESS_TEST_LOCK.lock().await;
    let _asr_process_lock = lock_asr_process_tests();
    for (name, unix, windows, code) in [
        (
            "empty",
            "exit 0",
            "@echo off\r\nexit /b 0\r\n",
            "empty_transcript",
        ),
        (
            "failing",
            "exit 2",
            "@echo off\r\nexit /b 2\r\n",
            "sidecar_failed",
        ),
    ] {
        let mut config = validated_config();
        let temp = tempdir().expect("tempdir");
        config.asr = asr_config(
            stub_program(temp.path(), name, unix, windows),
            ffmpeg_stub(temp.path()),
            Vec::new(),
        );
        let store = StoreHandle::open_in_memory().await.expect("store");
        let cache = Arc::new(
            AttachmentCache::open(
                &temp.path().join("attachments"),
                store.clone(),
                Arc::new(StaticAttachmentDownloader),
                AttachmentLimits::default(),
            )
            .expect("attachment cache"),
        );
        let (router, control, namespace, workspace) =
            start_audio_router(config, store.clone(), cache).await;
        let event_id = format!("event-audio-{name}");
        let thread_id = format!("thread-audio-{name}");
        let turn_id = format!("turn-audio-{name}");
        let mut inbound = event(&event_id, "owner-runtime-scope");
        inbound.message_type = "audio".to_owned();
        inbound.text.clear();
        inbound.parts = vec![audio_part(None)];
        let (context_id, handle, _) = route_audio_event(
            &router, &store, &namespace, &control, &workspace, name, inbound, &thread_id, &turn_id,
        )
        .await;
        let response = read_media(
            &control,
            &thread_id,
            &turn_id,
            &format!("server-audio-{name}"),
            &context_id,
            &handle,
        )
        .await;
        assert_eq!(response["result"]["success"], false);
        let body: Value = serde_json::from_str(
            response["result"]["contentItems"][0]["text"]
                .as_str()
                .expect("text"),
        )
        .expect("json");
        assert_eq!(body["error"]["code"], code);
        send_turn_completed(&control, &thread_id, &turn_id, "completed").await;
        router.shutdown().await.expect("shutdown");
        store.shutdown().await.expect("store shutdown");
    }
}
