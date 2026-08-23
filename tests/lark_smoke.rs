//! Opt-in end-to-end smoke against the real Feishu/Lark `OpenAPI` and
//! WebSocket transport.
//!
//! Runs only with `--ignored` and `LARK_E2E=1`; without the environment gate
//! it reports a skip reason and exits successfully — a skipped run is
//! explicitly not milestone evidence. When enabled it requires
//! `LARK_E2E_APP_ID`, `LARK_E2E_APP_SECRET`, `LARK_E2E_TENANT`
//! (`feishu|lark`), and `LARK_E2E_CHAT_ID` (a chat where the app bot is a
//! member), then proves send → WebSocket receive → normalized `InboundEvent`
//! → reply in one run. It never fakes a pass: any failure, including missing
//! credentials, fails the test with an actionable diagnostic.

mod fakecodex;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use futures_util::future::BoxFuture;
use lark_codex_bridge::codex::process::{CodexProcessConfig, ProcessError};
use lark_codex_bridge::codex::supervisor::AppServerSupervisor;
use lark_codex_bridge::config::{BridgeConfig, WorkspacePolicy};
use lark_codex_bridge::lark::api::LarkApi;
use lark_codex_bridge::lark::bridge::LarkBridge;
use lark_codex_bridge::lark::config::{LarkEndpoints, TenantBrand};
use lark_codex_bridge::lark::credentials::LarkCredentials;
use lark_codex_bridge::lark::http::LarkHttp;
use lark_codex_bridge::lark::normalize::{InboundEvent, ScopeKey};
use lark_codex_bridge::lark::token::TenantTokenProvider;
use lark_codex_bridge::runtime::attachments::{
    AttachmentCache, AttachmentLimits, LarkResourceDownloader,
};
use lark_codex_bridge::runtime::context::{
    ContextDraft, ContextRegistry, DraftPart, MediaHandle, PendingBinding, RevocationReason,
    TypedPart,
};
use lark_codex_bridge::runtime::intake::TenantNamespace;
use lark_codex_bridge::runtime::policy::{AccessDecision, AccessPolicy};
use lark_codex_bridge::runtime::quote::{LarkQuoteResolver, QuoteRequest, QuoteResolver};
use lark_codex_bridge::runtime::router::{Router, RouterSettings};
use lark_codex_bridge::runtime::scope::{DurableReplySink, ReplySinkError, TurnFinalization};
use lark_codex_bridge::store::{
    DedupOutcome, InboundEventState, InboundRejectionKind, NewOutboxRow, NewTurnRow, StoreHandle,
    TurnState,
};
use secrecy::SecretString;
use semver::Version;
use tempfile::tempdir;
use tokio::time::timeout;

use fakecodex::{FakeFactory, FakeOutcome};

const ROUND_TRIP_TIMEOUT: Duration = Duration::from_secs(180);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

struct SmokeSink;

impl DurableReplySink for SmokeSink {
    fn rejection_notice(
        &self,
        event: &InboundEvent,
        _reason: InboundRejectionKind,
    ) -> Result<NewOutboxRow, ReplySinkError> {
        Ok(NewOutboxRow {
            idempotency_key: format!("{}:mobile-smoke-rejection", event.event_id),
            scope_key: event.scope.to_string(),
            kind: "notice".to_owned(),
            payload_json: "{\"text\":\"mobile smoke rejected\"}".to_owned(),
            next_retry_ms: 0,
        })
    }

    fn finalize(&self, _turn: TurnFinalization) -> BoxFuture<'static, Result<(), ReplySinkError>> {
        Box::pin(async { Ok(()) })
    }
}

async fn degraded_supervisor() -> lark_codex_bridge::codex::supervisor::SupervisorHandle {
    AppServerSupervisor::start_with_factory(
        CodexProcessConfig::default(),
        Arc::new(FakeFactory::new([FakeOutcome::Error(
            ProcessError::UnsupportedVersion {
                found: Version::new(0, 145, 0),
            },
        )])),
        lark_codex_bridge::codex::supervisor::SupervisorSettings::default(),
    )
    .await
    .expect("degraded smoke supervisor")
}

#[tokio::test]
#[ignore = "requires real Feishu/Lark app credentials"]
async fn real_lark_round_trips_a_smoke_message() {
    if std::env::var("LARK_E2E").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping real Lark smoke: re-run with LARK_E2E=1 plus LARK_E2E_APP_ID, \
             LARK_E2E_APP_SECRET, LARK_E2E_TENANT (feishu|lark), and LARK_E2E_CHAT_ID"
        );
        return;
    }
    run_smoke().await.expect("real Lark smoke");
}

#[tokio::test]
#[ignore = "requires real Feishu/Lark credentials and a mobile group-chat action"]
async fn real_mobile_group_quote_resolves_direct_media_parent() {
    if std::env::var("LARK_MEDIA_E2E").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping mobile quote smoke: re-run with LARK_MEDIA_E2E=1 plus the normal \
             LARK_E2E credentials and LARK_MEDIA_E2E_GROUP_CHAT_ID"
        );
        return;
    }
    run_mobile_quote_smoke()
        .await
        .expect("real mobile group quote smoke");
}

fn required_env(name: &str) -> String {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => value,
        _ => panic!(
            "{name} is required for the real Lark smoke; set LARK_E2E_APP_ID, \
             LARK_E2E_APP_SECRET, LARK_E2E_TENANT (feishu|lark), and LARK_E2E_CHAT_ID"
        ),
    }
}

async fn run_smoke() -> Result<()> {
    let app_id = required_env("LARK_E2E_APP_ID");
    let app_secret = required_env("LARK_E2E_APP_SECRET");
    let tenant: TenantBrand = required_env("LARK_E2E_TENANT")
        .parse()
        .map_err(|_| anyhow!("LARK_E2E_TENANT must be feishu or lark"))?;
    let chat_id = required_env("LARK_E2E_CHAT_ID");

    let creds = LarkCredentials::new(app_id, SecretString::from(app_secret), tenant);
    let http = LarkHttp::new(LarkEndpoints::for_tenant(tenant))
        .context("unable to build the Lark HTTP client")?;
    let api = LarkApi::new(http.clone(), TenantTokenProvider::new(http, creds.clone()));

    let unix_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before the Unix epoch")?
        .as_secs();
    let text = format!("bridge-smoke {unix_ts}");
    let sent = api
        .send_text(&chat_id, &text)
        .await
        .context("unable to send the smoke message; verify the credentials and that the bot is a member of LARK_E2E_CHAT_ID")?;

    let (handle, mut events) = LarkBridge::start(creds)
        .await
        .context("unable to start the Lark bridge")?;
    let outcome = wait_for_own_message(&mut events, &sent.message_id).await;
    // Always stop the transport first so no WebSocket actor outlives the test.
    handle.shutdown().await;
    let event = outcome?;

    assert_eq!(event.chat_id, chat_id, "smoke event chat_id");
    assert_eq!(event.text, text, "smoke event text");
    match &event.scope {
        ScopeKey::Chat(scope_chat) => assert_eq!(scope_chat, &chat_id, "smoke event scope"),
        ScopeKey::Thread(scope_chat, _) => {
            assert_eq!(scope_chat, &chat_id, "smoke event scope");
        }
    }

    api.reply_text(&event.message_id, "pong")
        .await
        .context("unable to reply `pong` to the smoke message")?;

    // No orphan tasks: once the transport actor stops, the event channel must
    // drain and close.
    let drained = timeout(DRAIN_TIMEOUT, async {
        while events.recv().await.is_some() {}
    })
    .await;
    assert!(
        drained.is_ok(),
        "inbound event channel did not close after transport shutdown"
    );
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn run_mobile_quote_smoke() -> Result<()> {
    let app_id = required_env("LARK_E2E_APP_ID");
    let app_secret = required_env("LARK_E2E_APP_SECRET");
    let tenant: TenantBrand = required_env("LARK_E2E_TENANT")
        .parse()
        .map_err(|_| anyhow!("LARK_E2E_TENANT must be feishu or lark"))?;
    let chat_id = required_env("LARK_MEDIA_E2E_GROUP_CHAT_ID");
    let creds = LarkCredentials::new(app_id, SecretString::from(app_secret), tenant);
    let http = LarkHttp::new(LarkEndpoints::for_tenant(tenant))
        .context("unable to build the Lark HTTP client")?;
    let api = LarkApi::new(http.clone(), TenantTokenProvider::new(http, creds.clone()));
    let (handle, mut events) = LarkBridge::start(creds.clone())
        .await
        .context("unable to start the Lark bridge")?;
    let marker = format!(
        "bridge-media-smoke-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock before Unix epoch")?
            .as_secs()
    );
    eprintln!(
        "Mobile action required in group {chat_id}: send one standalone image/video/file/audio \
         without mentioning the bot, then reply directly to that exact message with \
         `@bot {marker}`. Do not reply through a forwarded/history card."
    );
    let outcome = timeout(ROUND_TRIP_TIMEOUT, async {
        let mut standalone = None;
        loop {
            let queued = events
                .recv()
                .await
                .context("inbound stream closed before the mobile quote arrived")?;
            let event = &queued.event;
            if event.chat_id == chat_id
                && !event.mentions_bot
                && matches!(
                    event.message_type.as_str(),
                    "image" | "video" | "media" | "file" | "audio"
                )
                && standalone.is_none()
            {
                standalone = Some(queued);
                continue;
            }
            if event.chat_id == chat_id && event.mentions_bot && event.text.contains(&marker) {
                let standalone = standalone.context(
                    "mobile quote arrived before a captured unmentioned standalone media event",
                )?;
                return Ok::<_, anyhow::Error>((standalone, queued));
            }
        }
    })
    .await
    .context("timed out waiting for the mobile @bot quote action")?;
    handle.shutdown().await;
    let (standalone, trigger) = outcome?;
    let event = &trigger.event;
    let parent_id = event
        .reply_to_message_id
        .clone()
        .context("mobile quote event did not carry parent_id")?;
    if standalone.event.message_id != parent_id {
        return Err(anyhow!(
            "mobile trigger did not quote the captured standalone media message"
        ));
    }

    let workspace = std::env::current_dir().context("current workspace")?;
    let mut config = BridgeConfig {
        owners: vec![event.sender_id.clone()],
        allowed_groups: vec![chat_id.clone()],
        default_workspace: Some(workspace.clone()),
        workspace: WorkspacePolicy {
            allow_roots: vec![workspace],
            ..WorkspacePolicy::default()
        },
        ..BridgeConfig::default()
    };
    config.validate().context("mobile smoke policy config")?;
    let policy = AccessPolicy::from_config(&config).context("mobile smoke policy")?;
    if policy.decide(event) != AccessDecision::Allow {
        return Err(anyhow!(
            "mobile @bot trigger did not pass sender/group/mention policy"
        ));
    }

    let namespace = TenantNamespace::from_credentials(&creds);
    let store = StoreHandle::open_in_memory().await.context("smoke store")?;
    match store
        .register_inbound(&namespace, &standalone.event)
        .await
        .context("register standalone media")?
    {
        DedupOutcome::New(_) => {}
        _ => return Err(anyhow!("standalone media was not a new durable row")),
    }
    let temp = tempdir().context("smoke cache tempdir")?;
    let cache = Arc::new(
        AttachmentCache::open(
            &temp.path().join("mobile-media-cache"),
            store.clone(),
            Arc::new(LarkResourceDownloader::new(api.clone())),
            AttachmentLimits::default(),
        )
        .context("smoke attachment cache")?,
    );
    let contexts = Arc::new(ContextRegistry::default());
    let router = Router::start_with_contexts(
        store.clone(),
        namespace.clone(),
        policy.clone(),
        RouterSettings::from_config(&config),
        degraded_supervisor().await,
        Arc::new(SmokeSink),
        Arc::clone(&cache),
        Arc::clone(&contexts),
    )
    .await
    .context("smoke router")?;
    let standalone_scope = standalone.event.scope.clone();
    let standalone_event_id = standalone.event.event_id.clone();
    router
        .route(standalone)
        .await
        .context("route standalone group media")?;
    timeout(Duration::from_secs(5), async {
        loop {
            if store
                .inbound_state(&namespace, &standalone_event_id)
                .await
                .ok()
                .flatten()
                == Some(InboundEventState::Completed)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("standalone group media did not settle without a turn")?;
    if router
        .scope_snapshot(&standalone_scope)
        .await
        .context("standalone scope snapshot")?
        .is_some()
        || contexts.stats().total != 0
        || !store
            .list_attachments()
            .await
            .context("standalone attachment rows")?
            .is_empty()
    {
        return Err(anyhow!(
            "standalone unmentioned group media created actor/context/cache work"
        ));
    }

    let quote = LarkQuoteResolver::new(api, policy)
        .resolve(QuoteRequest {
            parent_message_id: parent_id,
            chat_id: chat_id.clone(),
        })
        .await;
    if quote.status != lark_codex_bridge::runtime::context::QuoteStatus::Available {
        return Err(anyhow!(
            "authorized direct parent did not resolve as available media"
        ));
    }
    let resource_key = quote
        .parts
        .iter()
        .find_map(|part| match part {
            DraftPart::Media { resource, .. } => Some(resource.key.clone()),
            _ => None,
        })
        .context("resolved direct parent carried no readable media")?;
    let mut draft = ContextDraft::from_inbound(event);
    draft.quote = Some(quote);
    let turn_row_id = store
        .record_turn(NewTurnRow {
            scope_key: event.scope.to_string(),
            client_message_id: "mobile-smoke-local-turn".to_owned(),
            codex_thread_id: Some("mobile-smoke-thread".to_owned()),
            state: TurnState::Starting,
        })
        .await
        .context("record smoke turn")?;
    store
        .set_turn_state(
            turn_row_id,
            TurnState::Running,
            Some("mobile-smoke-codex-turn"),
        )
        .await
        .context("activate smoke turn row")?;
    let binding = PendingBinding {
        codex_thread_id: "mobile-smoke-thread".to_owned(),
        local_turn_row_id: turn_row_id,
    };
    let registered = contexts
        .register_pending(binding.clone(), draft)
        .context("register smoke context")?;
    let snapshot = contexts
        .resolve_for_tool(
            &registered.context_id,
            "mobile-smoke-thread",
            "mobile-smoke-codex-turn",
        )
        .context("resolve opaque smoke context")?;
    let serialized = serde_json::to_string(&snapshot).context("serialize smoke context")?;
    if serialized.contains(&resource_key) {
        return Err(anyhow!(
            "bridge_context.resolve exposed a plaintext Lark resource key"
        ));
    }
    let handle = snapshot
        .quote
        .as_ref()
        .into_iter()
        .flat_map(|quote| &quote.parts)
        .find_map(|part| match part {
            TypedPart::Media { handle, .. } => Some(handle.clone()),
            _ => None,
        })
        .context("opaque quote handle missing")?;
    if handle == MediaHandle::from_external(resource_key.clone()) {
        return Err(anyhow!("opaque handle reused the Lark resource key"));
    }
    let authorized = contexts
        .authorize_media_for_tool(
            &registered.context_id,
            &handle,
            "mobile-smoke-thread",
            "mobile-smoke-codex-turn",
            u64::try_from(cache.limits().max_attachment_bytes).unwrap_or(u64::MAX),
        )
        .context("authorize opaque smoke media handle")?;
    let cached = cache
        .fetch(
            &authorized.message_id,
            &authorized.resource,
            authorized.local_turn_row_id,
        )
        .await
        .context("read quoted media through the bounded cache")?;
    if cached.bytes == 0 || !cached.path.is_file() {
        return Err(anyhow!("quoted media read produced no bounded cache file"));
    }
    cache
        .release_turn(turn_row_id)
        .await
        .context("release smoke media lease")?;
    assert_eq!(
        contexts.revoke_turn(&binding, RevocationReason::Completed),
        1
    );
    store
        .set_turn_state(turn_row_id, TurnState::Completed, None)
        .await
        .context("complete smoke turn")?;
    router.shutdown().await.context("shutdown smoke router")?;
    drop(cache);
    store.shutdown().await.context("shutdown smoke store")?;
    Ok(())
}

async fn wait_for_own_message(
    events: &mut tokio::sync::mpsc::Receiver<lark_codex_bridge::lark::bridge::QueuedInboundEvent>,
    message_id: &str,
) -> Result<InboundEvent> {
    timeout(ROUND_TRIP_TIMEOUT, async {
        loop {
            let queued = events
                .recv()
                .await
                .context("inbound event channel closed before the smoke message arrived")?;
            let event = queued.into_event();
            if event.message_id == message_id {
                return Ok(event);
            }
        }
    })
    .await
    .context("timed out waiting for the smoke message to round-trip through the transport")?
}
