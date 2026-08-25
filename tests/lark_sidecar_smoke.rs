//! Explicitly gated real Feishu/Lark sidecar smoke.
//!
//! A passing run requires a fresh user message while the test is connected;
//! merely leaving this ignored or observing a skip is not acceptance evidence.

use std::env;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use secrecy::SecretString;

use lark_codex_bridge::channel::ChatMessageQuery;
use lark_codex_bridge::channel::ConnectionState;
use lark_codex_bridge::channel::native::NativeChannel;
use lark_codex_bridge::channel::sidecar::{NodeSidecar, NodeSidecarConfig};
use lark_codex_bridge::lark::api::LarkApi;
use lark_codex_bridge::lark::bridge::{BridgeConfig, LarkBridge};
use lark_codex_bridge::lark::config::{LarkEndpoints, TenantBrand};
use lark_codex_bridge::lark::credentials::LarkCredentials;
use lark_codex_bridge::lark::http::LarkHttp;
use lark_codex_bridge::lark::normalize::Normalizer;
use lark_codex_bridge::lark::token::TenantTokenProvider;
use lark_codex_bridge::runtime::intake::{DurableIntake, TenantNamespace};
use lark_codex_bridge::store::{InboundEventState, StoreHandle};

#[tokio::test]
#[ignore = "requires LARK_SIDECAR_E2E=1, real credentials, npm ci, and a fresh message"]
async fn official_sdk_event_reaches_durable_rust_intake_before_ack() {
    if env::var("LARK_SIDECAR_E2E").as_deref() != Ok("1") {
        eprintln!("SKIP: set LARK_SIDECAR_E2E=1; a skip is not acceptance evidence");
        return;
    }
    let app_id = env::var("LARK_SIDECAR_E2E_APP_ID").expect("sidecar smoke app id");
    let app_secret = env::var("LARK_SIDECAR_E2E_APP_SECRET").expect("sidecar smoke app secret");
    let tenant = env::var("LARK_SIDECAR_E2E_TENANT")
        .expect("sidecar smoke tenant")
        .parse::<TenantBrand>()
        .expect("feishu or lark tenant");
    let credentials = LarkCredentials::new(app_id, SecretString::from(app_secret), tenant);
    let http = LarkHttp::new(LarkEndpoints::for_tenant(tenant)).expect("official endpoints");
    let tokens = TenantTokenProvider::new(http, credentials.clone());
    let api = LarkApi::new(
        LarkHttp::new(LarkEndpoints::for_tenant(tenant)).expect("official endpoints"),
        tokens,
    );
    let bot_open_id = api
        .bot_info()
        .await
        .expect("bot identity")
        .open_id
        .filter(|value| !value.is_empty())
        .expect("bot open id");
    let native = Arc::new(NativeChannel::new(api));
    let query: Arc<dyn ChatMessageQuery> = native;
    let normalizer = Arc::new(Normalizer::with_query(query, bot_open_id));
    let store = StoreHandle::open_in_memory()
        .await
        .expect("in-memory store");
    let intake = DurableIntake::prepare(store.clone(), &credentials)
        .await
        .expect("durable intake");
    let (handler, mut events) =
        LarkBridge::prepare_durable(&credentials, BridgeConfig::default(), intake, normalizer)
            .expect("durable pipeline");
    let sidecar = NodeSidecar::start(
        NodeSidecarConfig {
            entrypoint: Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("sidecar")
                .join("index.cjs"),
            ..NodeSidecarConfig::default()
        },
        credentials.clone(),
        handler,
    )
    .await
    .expect("official SDK sidecar startup");
    let mut state = sidecar.subscribe_state();
    tokio::time::timeout(Duration::from_secs(30), async {
        while !matches!(*state.borrow(), ConnectionState::Connected) {
            state.changed().await.expect("sidecar state");
        }
    })
    .await
    .expect("official SDK connected");

    eprintln!("Send a fresh private-chat text message to the bot within 120 seconds.");
    let queued = tokio::time::timeout(Duration::from_secs(120), events.recv())
        .await
        .expect("fresh real event timeout")
        .expect("durable event stream");
    let namespace = TenantNamespace::from_credentials(&credentials);
    assert_eq!(
        store
            .inbound_state(&namespace, &queued.event.event_id)
            .await
            .expect("stored state"),
        Some(InboundEventState::Received),
        "the Node handler may succeed only after the durable row exists"
    );

    sidecar.shutdown().await;
    drop(queued);
    store.shutdown().await.expect("store shutdown");
}
