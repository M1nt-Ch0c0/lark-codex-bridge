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

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use lark_codex_bridge::lark::api::LarkApi;
use lark_codex_bridge::lark::bridge::LarkBridge;
use lark_codex_bridge::lark::config::{LarkEndpoints, TenantBrand};
use lark_codex_bridge::lark::credentials::LarkCredentials;
use lark_codex_bridge::lark::http::LarkHttp;
use lark_codex_bridge::lark::normalize::{InboundEvent, ScopeKey};
use lark_codex_bridge::lark::token::TenantTokenProvider;
use secrecy::SecretString;
use tokio::time::timeout;

const ROUND_TRIP_TIMEOUT: Duration = Duration::from_secs(180);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

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
