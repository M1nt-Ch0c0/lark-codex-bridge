//! Opt-in end-to-end smoke against the real Feishu/Lark `OpenAPI` and
//! WebSocket transport.
//!
//! Runs only with `--ignored` and `LARK_E2E=1`; without the environment gate
//! it reports a skip reason and exits successfully — a skipped run is
//! explicitly not milestone evidence. It never fakes a pass: any failure,
//! including missing credentials, fails the test with an actionable
//! diagnostic.
//!
//! # Operator-assisted design
//!
//! A bot does not receive its own `OpenAPI`-sent message back as an
//! `im.message.receive_v1` event, so the original "send then wait for the
//! echo" round trip can never complete (confirmed against the real tenant). A
//! group message event also only fires when the bot is `@`-mentioned. This
//! smoke is therefore honest operator-assisted: it starts the bridge, waits
//! for the transport to reach `Connected`, sends a beacon message to the
//! target chat so the operator can find the right conversation, prints a
//! unique nonce, and asks a human to send that nonce from a *human* account
//! into the target chat (`@`-mentioning the bot only when the chat is a
//! group). It then verifies the normalized event carries the right `chat_id`
//! and contains the nonce, replies `pong` over `OpenAPI`, and shuts the
//! transport down on every path. Because it requires a human, it is
//! `#[ignore]`d and gated on `LARK_E2E=1`; it is never a CI test.

mod bridgews;
mod fakecodex;
mod larkstub;

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use futures_util::future::BoxFuture;
use lark_codex_bridge::codex::process::{CodexProcessConfig, ProcessError};
use lark_codex_bridge::codex::supervisor::AppServerSupervisor;
use lark_codex_bridge::config::{BridgeConfig, WorkspacePolicy};
use lark_codex_bridge::lark::api::LarkApi;
use lark_codex_bridge::lark::bridge::{LarkBridge, QueuedInboundEvent};
use lark_codex_bridge::lark::config::{LarkEndpoints, TenantBrand};
use lark_codex_bridge::lark::credentials::{LarkCredentials, load_credentials};
use lark_codex_bridge::lark::http::LarkHttp;
use lark_codex_bridge::lark::normalize::{InboundEvent, ScopeKey};
use lark_codex_bridge::lark::token::TenantTokenProvider;
use lark_codex_bridge::lark::transport::{
    InboundFrameHandler, LarkTransport, TransportEvent, TransportHandle, TransportState,
};
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
use tokio::sync::mpsc;
use tokio::time::timeout;
use url::Url;

use fakecodex::{FakeFactory, FakeOutcome};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const OPERATOR_TIMEOUT: Duration = Duration::from_secs(300);
const ROUND_TRIP_TIMEOUT: Duration = Duration::from_secs(180);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
/// Fixed beacon text sent to the target chat so the operator can locate the
/// right conversation. Deliberately nonce-free so a hypothetical self-send
/// echo of the beacon can never satisfy the nonce match.
const BEACON_TEXT: &str = "lark-codex-bridge smoke beacon (ignore)";

#[tokio::test]
#[ignore = "requires real Feishu/Lark app credentials and a human operator"]
async fn real_lark_round_trips_an_operator_message() {
    if std::env::var("LARK_E2E").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping real Lark smoke: re-run with LARK_E2E=1 plus LARK_E2E_CHAT_ID and stored \
             credentials (LARK_APP_ID/LARK_APP_SECRET/LARK_TENANT or \
             ~/.config/lark-codex-bridge/credentials.toml)"
        );
        return;
    }
    run_smoke().await.expect("real Lark smoke");
}

fn required_env(name: &str) -> String {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => value,
        _ => panic!("{name} is required for the real Lark smoke"),
    }
}

async fn run_smoke() -> Result<()> {
    let chat_id = required_env("LARK_E2E_CHAT_ID");
    let creds = load_credentials()
        .context(
            "unable to load Lark credentials; set LARK_APP_ID, LARK_APP_SECRET, and LARK_TENANT \
             or register credentials with `lark auth register`",
        )?
        .ok_or_else(|| anyhow!("no Lark credentials found; run `lark auth register` first"))?;
    let tenant = creds.tenant;
    let http = LarkHttp::new(LarkEndpoints::for_tenant(tenant))
        .context("unable to build the Lark HTTP client")?;
    let api = LarkApi::new(http.clone(), TenantTokenProvider::new(http, creds.clone()));

    // Start the bridge FIRST so the transport is subscribed and Connected
    // before anything is sent; waiting for the echo of a pre-send would race.
    let (mut handle, mut events) = LarkBridge::start(creds)
        .await
        .context("unable to start the Lark bridge")?;

    let outcome = timeout(
        OPERATOR_TIMEOUT,
        run_operator_round_trip(&api, &mut handle, &mut events, &chat_id),
    )
    .await;

    // Always stop the transport first so no WebSocket actor outlives the test,
    // even when the round trip below failed or timed out.
    handle.shutdown().await;
    let (event, nonce) = match outcome {
        Ok(Ok(pair)) => pair,
        Ok(Err(error)) => return Err(error),
        Err(_) => {
            return Err(anyhow!(
                "timed out after {}s waiting for the operator message to round-trip",
                OPERATOR_TIMEOUT.as_secs()
            ));
        }
    };

    assert_eq!(event.chat_id, chat_id, "smoke event chat_id");
    assert!(
        text_contains_nonce(&event.text, &nonce),
        "smoke event text must contain the nonce (text_len={}, nonce_len={})",
        event.text.len(),
        nonce.len()
    );
    match &event.scope {
        ScopeKey::Chat(scope_chat) => assert_eq!(scope_chat, &chat_id, "smoke event scope"),
        ScopeKey::Thread(scope_chat, _) => {
            assert_eq!(scope_chat, &chat_id, "smoke event scope");
        }
    }

    let reply = api
        .reply_text(&event.message_id, "pong")
        .await
        .context("unable to reply `pong` to the smoke message")?;
    println!(
        "replied `pong` to the smoke message (reply message_id {})",
        reply.message_id
    );

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

/// Connects, sends a beacon to the target chat so the operator can find the
/// right conversation, prints the operator prompt, and waits for the operator's
/// nonce to round-trip. Returns the matched event and the nonce so the caller
/// can validate them after shutdown.
async fn run_operator_round_trip(
    api: &LarkApi,
    handle: &mut TransportHandle,
    events: &mut mpsc::Receiver<QueuedInboundEvent>,
    chat_id: &str,
) -> Result<(InboundEvent, String)> {
    wait_for_connected(handle, CONNECT_TIMEOUT).await?;
    let nonce = generate_nonce();
    // Beacon BEFORE the prompt: the operator opens the conversation that just
    // received this message and sends the nonce there, removing any ambiguity
    // about which chat is the target.
    let beacon = api.send_text(chat_id, BEACON_TEXT).await.context(
        "unable to send the smoke beacon; verify the credentials and that the bot is a member \
             of LARK_E2E_CHAT_ID",
    )?;
    println!(
        "beacon sent to the target chat (beacon message_id {})",
        beacon.message_id
    );
    println!(
        "\n===== Lark operator-assisted smoke =====\n\
         The bot just sent a beacon message to the target chat. Open the\n\
         conversation where the bot (TEST) posted that new message, then send\n\
         the following nonce from a HUMAN account in THAT conversation:\n\
         \n    {nonce}\n\
         \n  - If the target chat is a p2p (single) chat with the bot: send the\n\
         nonce as-is.\n\
         - If the target chat is a group: send the nonce AND @-mention the bot\n\
         (TEST) in the SAME message (nonce and mention order is free).\n\
         \nTarget chat_id: {chat_id}\n\
         Waiting up to {}s for the round trip…\n\
         =========================================\n",
        OPERATOR_TIMEOUT.as_secs(),
    );
    let event = await_operator_nonce(handle, events, chat_id, &nonce).await?;
    Ok((event, nonce))
}

/// Waits for the transport to publish [`TransportState::Connected`] within a
/// bounded deadline, failing fast on `Degraded`/`Stopped`.
async fn wait_for_connected(handle: &TransportHandle, deadline: Duration) -> Result<()> {
    let mut state = handle.subscribe_state();
    timeout(deadline, async {
        loop {
            match (*state.borrow_and_update()).clone() {
                TransportState::Connected => return Ok(()),
                TransportState::Degraded { reason } => {
                    return Err(anyhow!("transport degraded before Connected: {reason}"));
                }
                TransportState::Stopped => {
                    return Err(anyhow!("transport stopped before Connected"));
                }
                TransportState::Connecting { .. } | TransportState::Backoff { .. } => {}
            }
            state
                .changed()
                .await
                .map_err(|_| anyhow!("transport stopped before Connected"))?;
        }
    })
    .await
    .context("timed out waiting for the transport to reach Connected")?
}

/// Waits for the operator's nonce to round-trip through the transport and the
/// normalizer, observing the raw stream for diagnostics and failing fast if the
/// transport degrades or stops.
async fn await_operator_nonce(
    handle: &mut TransportHandle,
    events: &mut mpsc::Receiver<QueuedInboundEvent>,
    chat_id: &str,
    nonce: &str,
) -> Result<InboundEvent> {
    loop {
        tokio::select! {
            raw = handle.next_event() => {
                match raw {
                    None => return Err(anyhow!("transport observation channel closed before the smoke message arrived")),
                    Some(TransportEvent::Message { headers, payload, .. }) => {
                        eprintln!(
                            "raw inbound: message_id={} type={:?} payload_len={}",
                            headers.message_id().unwrap_or(""),
                            headers.ty(),
                            payload.len(),
                        );
                    }
                    Some(TransportEvent::State(TransportState::Degraded { reason })) => {
                        return Err(anyhow!("transport degraded while waiting for the smoke message: {reason}"));
                    }
                    Some(TransportEvent::State(TransportState::Stopped)) => {
                        return Err(anyhow!("transport stopped while waiting for the smoke message"));
                    }
                    Some(TransportEvent::State(_) | TransportEvent::Anomaly { .. }) => {}
                }
            }
            queued = events.recv() => {
                let Some(queued) = queued else {
                    return Err(anyhow!("inbound event channel closed before the smoke message arrived"));
                };
                let event = queued.into_event();
                // Opt-in, human-assisted smoke: print the full chat_id for
                // every event. chat_id is a chat identifier, not a secret (the
                // target is already documented), and it is what distinguishes
                // the target chat from a same-length wrong chat.
                eprintln!(
                    "normalized inbound: message_id={} chat_id={} text_len={} type={}",
                    event.message_id, event.chat_id, event.text.len(), event.message_type,
                );
                // Echo the received text (escaped) only for the target chat so
                // a mistyped or client-transformed nonce is visible instead of
                // an anonymous length mismatch; every other chat stays
                // text-hidden.
                if event.chat_id == chat_id {
                    println!(
                        "operator message text (target chat): message_id={} text={:?}",
                        event.message_id, event.text
                    );
                }
                if event.chat_id == chat_id && text_contains_nonce(&event.text, nonce) {
                    println!("nonce matched: message_id={}", event.message_id);
                    return Ok(event);
                }
            }
        }
    }
}

/// A unique, non-secret nonce for a single operator round trip.
///
/// The nonce is deliberately short (`smoke-` + 8 lowercase hex = 14 chars) so
/// a human can hand-type it without transcription errors. Its 32 bits of
/// entropy are ample for an opt-in smoke whose acceptance also requires an
/// exact `chat_id` match against the target chat, so a coincidental collision
/// with an unrelated message is negligible. `RandomState` seeds from OS
/// entropy where available.
fn generate_nonce() -> String {
    let entropy = RandomState::new().build_hasher().finish();
    format!("smoke-{:08x}", entropy & 0xffff_ffff)
}

/// Reports whether the normalized `text` carries the operator's nonce.
///
/// Real receive events leave a `@_user_N` mention placeholder inline in the
/// normalized text (a genuine `@`-mention is not an `<at>` tag), and the
/// operator may place the nonce before or after the mention. A substring match
/// is therefore the honest criterion: the nonce is unique and non-secret, so
/// any text containing it is the operator's message regardless of mention
/// position, count, or surrounding whitespace. Exact equality would reject
/// every mentioned message, and stripping the placeholder would hard-code
/// Feishu's mention grammar into the test.
fn text_contains_nonce(text: &str, nonce: &str) -> bool {
    text.contains(nonce)
}

mod nonce_matching {
    use super::text_contains_nonce;

    const NONCE: &str = "smoke-deadbeef";

    #[test]
    fn matches_a_pure_nonce() {
        assert!(text_contains_nonce(NONCE, NONCE));
    }

    #[test]
    fn matches_a_nonce_at_the_start_before_a_mention() {
        let text = format!("{NONCE} @_user_1");
        assert!(text_contains_nonce(&text, NONCE));
    }

    #[test]
    fn matches_a_nonce_at_the_end_after_a_mention() {
        let text = format!("@_user_1 {NONCE}");
        assert!(text_contains_nonce(&text, NONCE));
    }

    #[test]
    fn matches_a_nonce_in_the_middle() {
        let text = format!("please verify {NONCE} thanks");
        assert!(text_contains_nonce(&text, NONCE));
    }

    #[test]
    fn matches_with_multiple_mentions() {
        let text = format!("@_user_1 @_user_2 {NONCE} @_user_3");
        assert!(text_contains_nonce(&text, NONCE));
    }

    #[test]
    fn rejects_unrelated_text() {
        assert!(!text_contains_nonce("@_user_1 nothing to see here", NONCE));
    }

    #[test]
    fn rejects_a_partial_nonce_and_empty_text() {
        assert!(!text_contains_nonce("smoke-deadbe", NONCE));
        assert!(!text_contains_nonce("", NONCE));
    }
}

// ---------------------------------------------------------------------------
// Credential-free deterministic regressions for the connection-ordering fix.
// ---------------------------------------------------------------------------

fn test_credentials() -> LarkCredentials {
    LarkCredentials::new(
        "cli_test1234567890".to_owned(),
        SecretString::from("test-secret"),
        TenantBrand::Feishu,
    )
}

fn endpoints_for(stub: &larkstub::StubServer) -> LarkEndpoints {
    let base = Url::parse(&stub.url()).expect("stub url");
    LarkEndpoints {
        open_base: base.clone(),
        accounts_base: base,
    }
}

fn endpoint_body(ws_addr: SocketAddr) -> String {
    format!(
        r#"{{"code":0,"msg":"ok","data":{{"URL":"ws://{ws_addr}/ws?device_id=dev-1&service_id=7","ClientConfig":{{"PingInterval":60,"ReconnectCount":-1,"ReconnectInterval":2,"ReconnectNonce":0}}}}}}"#
    )
}

fn ok_handler() -> InboundFrameHandler {
    Arc::new(|_headers, _payload| Box::pin(async move { Ok(None) }))
}

#[tokio::test]
async fn wait_for_connected_resolves_after_the_websocket_handshake() {
    let mut ws = bridgews::TestWsServer::start().await;
    let stub = larkstub::StubServer::start(Arc::new(move |_| {
        larkstub::StubResponse::json(200, &endpoint_body(ws.addr))
    }))
    .await;
    let http = LarkHttp::new(endpoints_for(&stub)).expect("http client");
    let handle = LarkTransport::start(http, test_credentials(), ok_handler());

    // Drive the WebSocket handshake; the actor publishes Connected only after
    // it completes, so the wait below must resolve once this returns. Keep the
    // accepted connection alive so the transport stays Connected until shutdown.
    let _conn = ws.accept().await;
    wait_for_connected(&handle, Duration::from_secs(5))
        .await
        .expect("transport reaches Connected after the handshake");

    let started = Instant::now();
    handle.shutdown().await;
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "shutdown is bounded"
    );
}

#[tokio::test]
async fn wait_for_connected_is_bounded_when_the_endpoint_is_unreachable() {
    // Reserve a port and release it: the bootstrap succeeds but the WebSocket
    // connect is refused, so the transport never reaches Connected.
    let dead = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve dead address");
    let dead_addr = dead.local_addr().expect("dead address");
    drop(dead);

    let stub = larkstub::StubServer::start(Arc::new(move |_| {
        larkstub::StubResponse::json(200, &endpoint_body(dead_addr))
    }))
    .await;
    let http = LarkHttp::new(endpoints_for(&stub)).expect("http client");
    let handle = LarkTransport::start(http, test_credentials(), ok_handler());

    let err = wait_for_connected(&handle, Duration::from_millis(500)).await;
    assert!(err.is_err(), "wait_for_connected must respect its deadline");

    let started = Instant::now();
    handle.shutdown().await;
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "shutdown leaves no orphan WebSocket task"
    );
}

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
