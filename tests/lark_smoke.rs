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
//! for the transport to reach `Connected`, prints a unique nonce, and asks a
//! human to send that nonce from a *human* account into the target chat
//! (`@`-mentioning the bot only when the chat is a group). It then verifies
//! the normalized event carries the right `chat_id` and contains the nonce,
//! replies `pong` over `OpenAPI`, and shuts the transport down on every path.
//! Because it requires a human, it is `#[ignore]`d and gated on `LARK_E2E=1`;
//! it is never a CI test.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
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
use secrecy::SecretString;
use tokio::sync::mpsc;
use tokio::time::timeout;
use url::Url;

mod bridgews;
mod larkstub;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const OPERATOR_TIMEOUT: Duration = Duration::from_secs(300);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

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
        _ => panic!("{name} is required for the real Lark smoke; set LARK_E2E_CHAT_ID"),
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
        run_operator_round_trip(&mut handle, &mut events, &chat_id),
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

/// Connects, prints the operator prompt, and waits for the operator's nonce to
/// round-trip. Returns the matched event and the nonce so the caller can
/// validate them after shutdown.
async fn run_operator_round_trip(
    handle: &mut TransportHandle,
    events: &mut mpsc::Receiver<QueuedInboundEvent>,
    chat_id: &str,
) -> Result<(InboundEvent, String)> {
    wait_for_connected(handle, CONNECT_TIMEOUT).await?;
    let nonce = generate_nonce();
    println!(
        "\n===== Lark operator-assisted smoke =====\n\
         Send the following nonce from a HUMAN account:\n\
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
                eprintln!(
                    "normalized inbound: message_id={} chat_id_len={} text_len={} type={}",
                    event.message_id,
                    event.chat_id.len(),
                    event.text.len(),
                    event.message_type,
                );
                // This is an opt-in, human-assisted smoke against a chat the
                // operator controls. When the event is from the target chat,
                // echo the received text (escaped) so a nonce that was
                // mistyped or transformed by the client is visible instead of
                // surfacing as an anonymous length mismatch. Every other chat
                // stays length-only.
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
fn generate_nonce() -> String {
    let epoch_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    // `RandomState` seeds from OS entropy where available; the epoch seconds
    // keep the nonce unique even where the OS source is unavailable.
    let entropy = RandomState::new().build_hasher().finish();
    format!("lark-smoke-{epoch_secs:08x}-{entropy:08x}")
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

    const NONCE: &str = "lark-smoke-00000001-deadbeef";

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
        assert!(!text_contains_nonce("lark-smoke-00000001", NONCE));
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
