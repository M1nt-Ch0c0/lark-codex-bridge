//! Opt-in real-client acceptance for standalone Markdown posts.
//!
//! This smoke sends a real `post/tag=md` reply, prints its Lark message ID,
//! then waits for fresh desktop and mobile screenshots plus a small manual
//! attestation containing that exact ID. A skipped run is not evidence.

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, anyhow, ensure};
use lark_codex_bridge::lark::api::LarkApi;
use lark_codex_bridge::lark::config::{LarkEndpoints, TenantBrand};
use lark_codex_bridge::lark::credentials::LarkCredentials;
use lark_codex_bridge::lark::http::LarkHttp;
use lark_codex_bridge::lark::token::TenantTokenProvider;
use lark_codex_bridge::render::render_lark_markdown;
use secrecy::SecretString;
use tokio::time::{Instant, sleep};

const DEFAULT_EVIDENCE_TIMEOUT: Duration = Duration::from_secs(300);
const POLL_INTERVAL: Duration = Duration::from_secs(2);

#[tokio::test]
#[ignore = "requires real Lark credentials plus desktop/mobile manual evidence"]
async fn real_lark_markdown_post_has_desktop_and_mobile_evidence() {
    if std::env::var("LARK_MARKDOWN_E2E").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping real Markdown-post smoke: set LARK_MARKDOWN_E2E=1 and the documented credentials/evidence paths"
        );
        return;
    }
    run_smoke().await.expect("real Lark Markdown-post smoke");
}

async fn run_smoke() -> Result<()> {
    let app_id = required_env("LARK_E2E_APP_ID");
    let app_secret = required_env("LARK_E2E_APP_SECRET");
    let tenant: TenantBrand = required_env("LARK_E2E_TENANT")
        .parse()
        .map_err(|_| anyhow!("LARK_E2E_TENANT must be feishu or lark"))?;
    let parent_message_id = required_env("LARK_MARKDOWN_E2E_PARENT_MESSAGE_ID");
    let desktop = PathBuf::from(required_env("LARK_MARKDOWN_E2E_DESKTOP_SCREENSHOT"));
    let mobile = PathBuf::from(required_env("LARK_MARKDOWN_E2E_MOBILE_SCREENSHOT"));
    let attestation = PathBuf::from(required_env("LARK_MARKDOWN_E2E_ATTESTATION"));

    let http = LarkHttp::new(LarkEndpoints::for_tenant(tenant))
        .context("unable to build the Lark HTTP client")?;
    let credentials = LarkCredentials::new(app_id, SecretString::from(app_secret), tenant);
    let api = LarkApi::new(http.clone(), TenantTokenProvider::new(http, credentials));

    let markdown = render_lark_markdown(concat!(
        "# Lark Markdown acceptance\n\n",
        "Paragraph with **bold**, *italic*, ~~deleted~~, `inline code`, and ",
        "[a link](https://example.com).\n\n",
        "- unordered\n1. ordered\n> quote\n\n",
        "```rust\nfn main() { println!(\"desktop + mobile\"); }\n```\n\n",
        "| table | fallback |\n| --- | --- |\n| must | be fenced |\n\n",
        "- [x] task syntax degrades\n<div>HTML controls are removed</div>",
    ));
    let evidence_not_before = SystemTime::now()
        .checked_sub(Duration::from_secs(2))
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let sent = api
        .reply_post_markdown(&parent_message_id, &markdown)
        .await
        .context("unable to send the real Markdown post reply")?;

    eprintln!(
        "sent Markdown acceptance reply {}; capture it on desktop and mobile, then write the documented JSON attestation",
        sent.message_id
    );
    let timeout = evidence_timeout();
    let deadline = Instant::now() + timeout;
    loop {
        if evidence_ready(
            &desktop,
            &mobile,
            &attestation,
            &sent.message_id,
            evidence_not_before,
        )? {
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "timed out after {}s waiting for fresh desktop/mobile screenshots and attestation for {}",
            timeout.as_secs(),
            sent.message_id
        );
        sleep(POLL_INTERVAL).await;
    }
}

fn evidence_ready(
    desktop: &Path,
    mobile: &Path,
    attestation: &Path,
    message_id: &str,
    not_before: SystemTime,
) -> Result<bool> {
    if !fresh_image(desktop, not_before)? || !fresh_image(mobile, not_before)? {
        return Ok(false);
    }
    let Ok(metadata) = attestation.metadata() else {
        return Ok(false);
    };
    if metadata.len() == 0
        || metadata.len() > 16 * 1024
        || metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH) < not_before
    {
        return Ok(false);
    }
    let raw = std::fs::read_to_string(attestation).context("reading the attestation file")?;
    let value: serde_json::Value =
        serde_json::from_str(&raw).context("the attestation must be valid JSON")?;
    Ok(value["message_id"] == message_id
        && value["desktop"] == "pass"
        && value["mobile"] == "pass"
        && value["table"] == "fenced")
}

fn fresh_image(path: &Path, not_before: SystemTime) -> Result<bool> {
    let Ok(metadata) = path.metadata() else {
        return Ok(false);
    };
    if metadata.len() < 100 || metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH) < not_before {
        return Ok(false);
    }
    let mut magic = [0_u8; 12];
    let read = File::open(path)
        .with_context(|| format!("opening screenshot {}", path.display()))?
        .read(&mut magic)
        .with_context(|| format!("reading screenshot {}", path.display()))?;
    let png = read >= 8 && magic[..8] == [137, 80, 78, 71, 13, 10, 26, 10];
    let jpeg = read >= 3 && magic[..3] == [0xff, 0xd8, 0xff];
    let webp = read >= 12 && &magic[..4] == b"RIFF" && &magic[8..12] == b"WEBP";
    Ok(png || jpeg || webp)
}

fn evidence_timeout() -> Duration {
    std::env::var("LARK_MARKDOWN_E2E_EVIDENCE_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map_or(DEFAULT_EVIDENCE_TIMEOUT, |seconds| {
            Duration::from_secs(seconds.clamp(30, 900))
        })
}

fn required_env(name: &str) -> String {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => value,
        _ => panic!("{name} is required when LARK_MARKDOWN_E2E=1"),
    }
}
