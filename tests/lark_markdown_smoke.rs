//! Opt-in real-client acceptance for standalone Markdown posts.
//!
//! The ignored test is deliberately fail-closed: invoking it without the
//! explicit gate or complete evidence configuration is a failure, not a skip.

use std::fmt::Write as _;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, anyhow, ensure};
use ed25519_dalek::{Signature, VerifyingKey};
use image::{GenericImageView, ImageFormat};
use lark_codex_bridge::lark::api::LarkApi;
use lark_codex_bridge::lark::config::{LarkEndpoints, TenantBrand};
use lark_codex_bridge::lark::credentials::LarkCredentials;
use lark_codex_bridge::lark::http::LarkHttp;
use lark_codex_bridge::lark::token::TenantTokenProvider;
use lark_codex_bridge::render::render_lark_markdown;
use secrecy::SecretString;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::time::{Instant, sleep};
use uuid::Uuid;

const DEFAULT_EVIDENCE_TIMEOUT: Duration = Duration::from_secs(300);
const POLL_INTERVAL: Duration = Duration::from_secs(2);
const MAX_SCREENSHOT_BYTES: u64 = 16 * 1024 * 1024;
const MAX_SCREENSHOT_PIXELS: u64 = 50_000_000;
const MAX_ATTESTATION_BYTES: u64 = 16 * 1024;

struct TrustedReviewAnchor {
    reviewer: &'static str,
    public_key: [u8; 32],
}

// Trust for the real smoke is repository state, never runtime configuration.
// Add an independently obtained reviewer identity and Ed25519 public key here
// only through a reviewed commit. No legitimate independent anchor is
// currently available, so real desktop/mobile evidence intentionally remains
// unavailable and the smoke fails before reading credentials or sending data.
const TRUSTED_REVIEW_ANCHORS: &[TrustedReviewAnchor] = &[];

#[tokio::test]
#[ignore = "requires the explicit real-Lark gate and fresh desktop/mobile evidence"]
async fn real_lark_markdown_post_has_desktop_and_mobile_evidence() {
    require_gate(std::env::var("LARK_MARKDOWN_E2E").ok().as_deref())
        .expect("the real Markdown smoke gate is mandatory");
    run_smoke().await.expect("real Lark Markdown-post smoke");
}

fn require_gate(value: Option<&str>) -> Result<()> {
    ensure!(
        value == Some("1"),
        "set LARK_MARKDOWN_E2E=1 and every documented credential/evidence variable"
    );
    Ok(())
}

async fn run_smoke() -> Result<()> {
    ensure_trusted_review_anchors_configured()?;
    let reviewer = required_env("LARK_MARKDOWN_E2E_REVIEWER")?;
    ensure!(
        reviewer.len() <= 200 && reviewer.chars().all(|character| !character.is_control()),
        "LARK_MARKDOWN_E2E_REVIEWER must be at most 200 bytes without control characters"
    );
    let review_public_key = trusted_review_anchor(&reviewer)?;
    let review_public_key_sha256 = sha256(review_public_key.as_bytes());

    let app_id = required_env("LARK_E2E_APP_ID")?;
    let app_secret = required_env("LARK_E2E_APP_SECRET")?;
    let tenant: TenantBrand = required_env("LARK_E2E_TENANT")?
        .parse()
        .map_err(|_| anyhow!("LARK_E2E_TENANT must be feishu or lark"))?;
    let parent_message_id = required_env("LARK_MARKDOWN_E2E_PARENT_MESSAGE_ID")?;
    let desktop = PathBuf::from(required_env("LARK_MARKDOWN_E2E_DESKTOP_SCREENSHOT")?);
    let mobile = PathBuf::from(required_env("LARK_MARKDOWN_E2E_MOBILE_SCREENSHOT")?);
    let attestation = PathBuf::from(required_env("LARK_MARKDOWN_E2E_ATTESTATION")?);

    let before = EvidenceBefore {
        desktop: snapshot(&desktop, MAX_SCREENSHOT_BYTES)?,
        mobile: snapshot(&mobile, MAX_SCREENSHOT_BYTES)?,
        attestation: snapshot(&attestation, MAX_ATTESTATION_BYTES)?,
    };
    let nonce = Uuid::new_v4().to_string();
    let (markdown, body_sha256, marker_sha256) = acceptance_markdown(&nonce);

    let http = LarkHttp::new(LarkEndpoints::for_tenant(tenant))
        .context("unable to build the Lark HTTP client")?;
    let credentials = LarkCredentials::new(app_id, SecretString::from(app_secret), tenant);
    let api = LarkApi::new(http.clone(), TenantTokenProvider::new(http, credentials));
    let markdown_sha256 = sha256(markdown.as_bytes());
    let sent = api
        .reply_post_markdown(&parent_message_id, &markdown)
        .await
        .context("unable to send the real Markdown post reply")?;
    let send_completed_at = SystemTime::now();

    eprintln!(
        "sent Markdown acceptance reply {}; visible marker nonce {}; body sha256 {}; payload sha256 {}; independent reviewer {}; review public-key sha256 {}; capture distinct desktop/mobile screenshots and obtain the reviewer's Ed25519-signed attestation",
        sent.message_id, nonce, body_sha256, markdown_sha256, reviewer, review_public_key_sha256,
    );
    let timeout = evidence_timeout();
    let deadline = Instant::now() + timeout;
    let expected = ExpectedAttestation {
        nonce: &nonce,
        message_id: &sent.message_id,
        body_sha256: &body_sha256,
        markdown_sha256: &markdown_sha256,
        marker_sha256: &marker_sha256,
        reviewer: &reviewer,
        review_public_key: &review_public_key,
        review_public_key_sha256: &review_public_key_sha256,
    };
    let mut announced_evidence = None;
    loop {
        if let Some((desktop_evidence, mobile_evidence)) = fresh_distinct_images(
            EvidencePaths {
                desktop: &desktop,
                mobile: &mobile,
                attestation: &attestation,
            },
            &before,
            send_completed_at,
        )? {
            let binding = evidence_binding(&expected, &desktop_evidence, &mobile_evidence);
            if announced_evidence.as_deref() != Some(binding.as_str()) {
                eprintln!(
                    "fresh evidence: marker_sha256={}; desktop file_sha256={} pixel_sha256={} dimensions={}x{}; mobile file_sha256={} pixel_sha256={} dimensions={}x{}; evidence_sha256={}",
                    marker_sha256,
                    desktop_evidence.file_sha256,
                    desktop_evidence.pixel_sha256,
                    desktop_evidence.width,
                    desktop_evidence.height,
                    mobile_evidence.file_sha256,
                    mobile_evidence.pixel_sha256,
                    mobile_evidence.width,
                    mobile_evidence.height,
                    binding,
                );
                announced_evidence = Some(binding);
            }
        }
        if evidence_ready(
            EvidencePaths {
                desktop: &desktop,
                mobile: &mobile,
                attestation: &attestation,
            },
            &before,
            expected,
            send_completed_at,
        )? {
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "timed out after {}s waiting for changed, decodable, distinct desktop/mobile evidence bound to the sent message",
            timeout.as_secs()
        );
        sleep(POLL_INTERVAL).await;
    }
}

fn acceptance_markdown(nonce: &str) -> (String, String, String) {
    let body = render_lark_markdown(concat!(
        "# Lark Markdown acceptance\n\n",
        "Paragraph with **bold**, *italic*, ~~deleted~~, `inline code`, and ",
        "[a link](https://example.com).\n\n",
        "- unordered\n1. ordered\n> quote\n\n",
        "```rust\nfn main() { println!(\"desktop + mobile\"); }\n```\n\n",
        "| table | fallback |\n| --- | --- |\n| must | be fenced |\n\n",
        "- [x] task syntax degrades\n<div>HTML controls are removed</div>",
    ));
    let body_sha256 = sha256(body.as_bytes());
    let marker = format!("Lark Markdown evidence marker: nonce={nonce}; body-sha256={body_sha256}");
    let marker_sha256 = sha256(marker.as_bytes());
    let markdown = format!("{body}\n\n**{marker}**");
    (markdown, body_sha256, marker_sha256)
}

#[derive(Clone, Copy)]
struct EvidencePaths<'a> {
    desktop: &'a Path,
    mobile: &'a Path,
    attestation: &'a Path,
}

struct EvidenceBefore {
    desktop: Option<String>,
    mobile: Option<String>,
    attestation: Option<String>,
}

#[derive(Clone, Copy)]
struct ExpectedAttestation<'a> {
    nonce: &'a str,
    message_id: &'a str,
    body_sha256: &'a str,
    markdown_sha256: &'a str,
    marker_sha256: &'a str,
    reviewer: &'a str,
    review_public_key: &'a VerifyingKey,
    review_public_key_sha256: &'a str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SmokeAttestation {
    version: u32,
    nonce: String,
    message_id: String,
    body_sha256: String,
    markdown_sha256: String,
    marker: MarkerAttestation,
    desktop: ScreenshotAttestation,
    mobile: ScreenshotAttestation,
    review: ReviewAttestation,
    evidence_sha256: String,
    table: TableVerdict,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewAttestation {
    reviewer: String,
    public_key_sha256: String,
    signature_ed25519: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MarkerAttestation {
    verdict: MarkerVerdict,
    sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScreenshotAttestation {
    verdict: PassVerdict,
    file_sha256: String,
    pixel_sha256: String,
    width: u32,
    height: u32,
}

#[derive(Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum PassVerdict {
    Pass,
}

#[derive(Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum MarkerVerdict {
    VisibleInBoth,
}

#[derive(Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TableVerdict {
    Fenced,
}

fn evidence_ready(
    paths: EvidencePaths<'_>,
    before: &EvidenceBefore,
    expected: ExpectedAttestation<'_>,
    not_before: SystemTime,
) -> Result<bool> {
    let Some((desktop, mobile)) = fresh_distinct_images(paths, before, not_before)? else {
        return Ok(false);
    };
    let Some(raw) = changed_file(
        paths.attestation,
        MAX_ATTESTATION_BYTES,
        before.attestation.as_deref(),
        not_before,
    )?
    else {
        return Ok(false);
    };
    let Ok(attestation) = serde_json::from_slice::<SmokeAttestation>(&raw) else {
        return Ok(false);
    };
    let binding = evidence_binding(&expected, &desktop, &mobile);
    let signature_valid = review_signature_is_valid(
        &attestation.review.signature_ed25519,
        &expected,
        &desktop,
        &mobile,
    );
    Ok(attestation.version == 3
        && attestation.nonce == expected.nonce
        && attestation.message_id == expected.message_id
        && attestation.body_sha256 == expected.body_sha256
        && attestation.markdown_sha256 == expected.markdown_sha256
        && attestation.marker.verdict == MarkerVerdict::VisibleInBoth
        && attestation.marker.sha256 == expected.marker_sha256
        && attestation.desktop.verdict == PassVerdict::Pass
        && attestation.mobile.verdict == PassVerdict::Pass
        && attestation.desktop.file_sha256 == desktop.file_sha256
        && attestation.desktop.pixel_sha256 == desktop.pixel_sha256
        && attestation.desktop.width == desktop.width
        && attestation.desktop.height == desktop.height
        && attestation.mobile.file_sha256 == mobile.file_sha256
        && attestation.mobile.pixel_sha256 == mobile.pixel_sha256
        && attestation.mobile.width == mobile.width
        && attestation.mobile.height == mobile.height
        && attestation.review.reviewer == expected.reviewer
        && attestation.review.public_key_sha256 == expected.review_public_key_sha256
        && attestation.evidence_sha256 == binding
        && attestation.table == TableVerdict::Fenced
        && signature_valid)
}

fn fresh_distinct_images(
    paths: EvidencePaths<'_>,
    before: &EvidenceBefore,
    not_before: SystemTime,
) -> Result<Option<(ImageEvidence, ImageEvidence)>> {
    let Some(desktop) =
        changed_decodable_image(paths.desktop, before.desktop.as_deref(), not_before)?
    else {
        return Ok(None);
    };
    let Some(mobile) = changed_decodable_image(paths.mobile, before.mobile.as_deref(), not_before)?
    else {
        return Ok(None);
    };
    if desktop.pixel_sha256 == mobile.pixel_sha256 {
        return Ok(None);
    }
    Ok(Some((desktop, mobile)))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ImageEvidence {
    file_sha256: String,
    pixel_sha256: String,
    width: u32,
    height: u32,
}

fn changed_decodable_image(
    path: &Path,
    before_sha256: Option<&str>,
    not_before: SystemTime,
) -> Result<Option<ImageEvidence>> {
    let Some(bytes) = changed_file(path, MAX_SCREENSHOT_BYTES, before_sha256, not_before)? else {
        return Ok(None);
    };
    let Ok(reader) = image::ImageReader::new(std::io::Cursor::new(&bytes)).with_guessed_format()
    else {
        return Ok(None);
    };
    if !matches!(
        reader.format(),
        Some(ImageFormat::Png | ImageFormat::Jpeg | ImageFormat::WebP)
    ) {
        return Ok(None);
    }
    let Ok((width, height)) = reader.into_dimensions() else {
        return Ok(None);
    };
    let pixels = u64::from(width).saturating_mul(u64::from(height));
    if width == 0 || height == 0 || pixels > MAX_SCREENSHOT_PIXELS {
        return Ok(None);
    }
    let Ok(reader) = image::ImageReader::new(std::io::Cursor::new(&bytes)).with_guessed_format()
    else {
        return Ok(None);
    };
    let Ok(image) = reader.decode() else {
        return Ok(None);
    };
    if image.dimensions() != (width, height) {
        return Ok(None);
    }
    let rgba = image.to_rgba8();
    let mut canonical = Vec::with_capacity(rgba.as_raw().len().saturating_add(8));
    canonical.extend_from_slice(&width.to_be_bytes());
    canonical.extend_from_slice(&height.to_be_bytes());
    canonical.extend_from_slice(rgba.as_raw());
    Ok(Some(ImageEvidence {
        file_sha256: sha256(&bytes),
        pixel_sha256: sha256(&canonical),
        width,
        height,
    }))
}

fn evidence_binding(
    expected: &ExpectedAttestation<'_>,
    desktop: &ImageEvidence,
    mobile: &ImageEvidence,
) -> String {
    let mut canonical = String::from("lark-markdown-evidence-v3\n");
    binding_field(&mut canonical, "nonce", expected.nonce);
    binding_field(&mut canonical, "message_id", expected.message_id);
    binding_field(&mut canonical, "body_sha256", expected.body_sha256);
    binding_field(&mut canonical, "markdown_sha256", expected.markdown_sha256);
    binding_field(&mut canonical, "marker_verdict", "visible_in_both");
    binding_field(&mut canonical, "marker_sha256", expected.marker_sha256);
    binding_field(&mut canonical, "desktop_verdict", "pass");
    binding_field(&mut canonical, "desktop_file_sha256", &desktop.file_sha256);
    binding_field(
        &mut canonical,
        "desktop_pixel_sha256",
        &desktop.pixel_sha256,
    );
    binding_field(&mut canonical, "desktop_width", &desktop.width.to_string());
    binding_field(
        &mut canonical,
        "desktop_height",
        &desktop.height.to_string(),
    );
    binding_field(&mut canonical, "mobile_verdict", "pass");
    binding_field(&mut canonical, "mobile_file_sha256", &mobile.file_sha256);
    binding_field(&mut canonical, "mobile_pixel_sha256", &mobile.pixel_sha256);
    binding_field(&mut canonical, "mobile_width", &mobile.width.to_string());
    binding_field(&mut canonical, "mobile_height", &mobile.height.to_string());
    binding_field(&mut canonical, "table_verdict", "fenced");
    binding_field(&mut canonical, "reviewer", expected.reviewer);
    binding_field(
        &mut canonical,
        "review_public_key_sha256",
        expected.review_public_key_sha256,
    );
    sha256(canonical.as_bytes())
}

fn binding_field(canonical: &mut String, name: &str, value: &str) {
    write!(canonical, "{name}:{}:", value.len()).expect("writing to a string cannot fail");
    canonical.push_str(value);
    canonical.push('\n');
}

fn review_signature_message(binding: &str) -> String {
    format!("lark-markdown-independent-review-signature-v3\n{binding}\n")
}

fn review_signature_is_valid(
    encoded_signature: &str,
    expected: &ExpectedAttestation<'_>,
    desktop: &ImageEvidence,
    mobile: &ImageEvidence,
) -> bool {
    let Some(signature_bytes) = decode_fixed_hex::<64>(encoded_signature) else {
        return false;
    };
    let signature = Signature::from_bytes(&signature_bytes);
    let binding = evidence_binding(expected, desktop, mobile);
    expected
        .review_public_key
        .verify_strict(review_signature_message(&binding).as_bytes(), &signature)
        .is_ok()
}

fn ensure_trusted_review_anchors_configured() -> Result<()> {
    ensure!(
        !TRUSTED_REVIEW_ANCHORS.is_empty(),
        "no trusted review anchor configured; obtain an independent reviewer's Ed25519 public key out-of-band and add its identity/key to TRUSTED_REVIEW_ANCHORS through a reviewed repository commit before running this smoke"
    );
    Ok(())
}

fn trusted_review_anchor(reviewer: &str) -> Result<VerifyingKey> {
    ensure_trusted_review_anchors_configured()?;
    let anchor = TRUSTED_REVIEW_ANCHORS
        .iter()
        .find(|anchor| anchor.reviewer == reviewer)
        .ok_or_else(|| {
            anyhow!(
                "LARK_MARKDOWN_E2E_REVIEWER does not select a repository-pinned trusted review anchor"
            )
        })?;
    VerifyingKey::from_bytes(&anchor.public_key).map_err(|_| {
        anyhow!(
            "the repository-pinned Ed25519 key for reviewer {reviewer:?} is invalid; correct it through a reviewed commit"
        )
    })
}

fn decode_fixed_hex<const N: usize>(encoded: &str) -> Option<[u8; N]> {
    let bytes = encoded.as_bytes();
    if bytes.len() != N.saturating_mul(2) {
        return None;
    }
    let mut decoded = [0_u8; N];
    for (index, output) in decoded.iter_mut().enumerate() {
        let high = hex_nibble(bytes[index * 2])?;
        let low = hex_nibble(bytes[index * 2 + 1])?;
        *output = (high << 4) | low;
    }
    Some(decoded)
}

const fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn changed_file(
    path: &Path,
    limit: u64,
    before_sha256: Option<&str>,
    not_before: SystemTime,
) -> Result<Option<Vec<u8>>> {
    let Ok(metadata) = path.metadata() else {
        return Ok(None);
    };
    if !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > limit
        || metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH) < not_before
    {
        return Ok(None);
    }
    let bytes = read_bounded(path, limit)?;
    let digest = sha256(&bytes);
    if before_sha256 == Some(digest.as_str()) {
        return Ok(None);
    }
    Ok(Some(bytes))
}

fn snapshot(path: &Path, limit: u64) -> Result<Option<String>> {
    let Ok(metadata) = path.metadata() else {
        return Ok(None);
    };
    ensure!(metadata.is_file(), "an evidence path is not a regular file");
    ensure!(
        metadata.len() <= limit,
        "pre-send evidence exceeds its byte cap"
    );
    Ok(Some(sha256(&read_bounded(path, limit)?)))
}

fn read_bounded(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)
        .context("opening an evidence file")?
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)
        .context("reading an evidence file")?;
    ensure!(
        u64::try_from(bytes.len()).unwrap_or(u64::MAX) <= limit,
        "evidence changed beyond its byte cap while being read"
    );
    Ok(bytes)
}

fn sha256(bytes: &[u8]) -> String {
    hex_digest(Sha256::digest(bytes))
}

fn hex_digest(digest: impl AsRef<[u8]>) -> String {
    let digest = digest.as_ref();
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to a string cannot fail");
    }
    encoded
}

fn evidence_timeout() -> Duration {
    std::env::var("LARK_MARKDOWN_E2E_EVIDENCE_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map_or(DEFAULT_EVIDENCE_TIMEOUT, |seconds| {
            Duration::from_secs(seconds.clamp(30, 900))
        })
}

fn required_env(name: &str) -> Result<String> {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(value),
        _ => Err(anyhow!("{name} is required when LARK_MARKDOWN_E2E=1")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};
    use tempfile::tempdir;

    #[test]
    fn explicit_gate_is_fail_closed() {
        assert!(require_gate(None).is_err());
        assert!(require_gate(Some("0")).is_err());
        assert!(require_gate(Some("1")).is_ok());
    }

    #[test]
    fn arbitrary_runtime_key_cannot_resolve_a_production_trust_anchor() {
        let operator_key = SigningKey::from_bytes(&[11; 32]);
        let former_runtime_key_value = hex_digest(operator_key.verifying_key().as_bytes());
        assert_eq!(former_runtime_key_value.len(), 64);

        // There is deliberately no public-key argument to the production
        // resolver: an environment value generated by the screenshot
        // operator cannot introduce trust for unrelated images or verdicts.
        let error = trusted_review_anchor("operator-selected-reviewer")
            .expect_err("the production allowlist is intentionally empty");
        assert!(
            error
                .to_string()
                .contains("no trusted review anchor configured"),
            "unexpected error: {error:#}"
        );
        assert!(TRUSTED_REVIEW_ANCHORS.is_empty());
    }

    #[test]
    fn sent_fixture_visibly_binds_nonce_and_body_digest() {
        let nonce = "run-specific-nonce";
        let (markdown, body_sha256, marker_sha256) = acceptance_markdown(nonce);
        let marker =
            format!("Lark Markdown evidence marker: nonce={nonce}; body-sha256={body_sha256}");
        assert!(markdown.ends_with(&format!("**{marker}**")));
        assert_eq!(marker_sha256, sha256(marker.as_bytes()));
        assert!(markdown.contains("```text\n| table | fallback |"));
    }

    #[test]
    fn low_level_evidence_binding_rejects_unsigned_and_mismatched_fixtures() {
        let temp = tempdir().expect("tempdir");
        let desktop = temp.path().join("desktop.png");
        let mobile = temp.path().join("mobile.png");
        let attestation = temp.path().join("attestation.json");
        image::DynamicImage::new_rgb8(32, 24)
            .save(&desktop)
            .expect("desktop PNG");
        let mut mobile_image = image::RgbImage::new(32, 24);
        mobile_image.put_pixel(0, 0, image::Rgb([255, 0, 0]));
        image::DynamicImage::ImageRgb8(mobile_image)
            .save(&mobile)
            .expect("mobile PNG");
        // This fixture key exercises only the lower-level signature binding.
        // It is not present in the production trust-anchor allowlist.
        let signing_key = SigningKey::from_bytes(&[7; 32]);
        let review_public_key = signing_key.verifying_key();
        let review_public_key_sha256 = sha256(review_public_key.as_bytes());
        let expected = ExpectedAttestation {
            nonce: "nonce",
            message_id: "om_message",
            body_sha256: "body-hash",
            markdown_sha256: "markdown-hash",
            marker_sha256: "marker-hash",
            reviewer: "independent-reviewer",
            review_public_key: &review_public_key,
            review_public_key_sha256: &review_public_key_sha256,
        };
        let desktop_evidence = image_evidence(&desktop);
        let mobile_evidence = image_evidence(&mobile);

        // These are just two unrelated generated images. Every self-filled
        // semantic field is correct, but without the independently held
        // review key the evidence must remain rejected.
        write_attestation(
            &attestation,
            expected,
            expected.marker_sha256,
            &desktop_evidence,
            &mobile_evidence,
            None,
        );
        let before = EvidenceBefore {
            desktop: None,
            mobile: None,
            attestation: None,
        };
        assert!(
            !evidence_ready(
                EvidencePaths {
                    desktop: &desktop,
                    mobile: &mobile,
                    attestation: &attestation,
                },
                &before,
                expected,
                SystemTime::UNIX_EPOCH,
            )
            .expect("unsigned self-attestation")
        );

        let unrelated_signing_key = SigningKey::from_bytes(&[8; 32]);
        write_attestation(
            &attestation,
            expected,
            expected.marker_sha256,
            &desktop_evidence,
            &mobile_evidence,
            Some(&unrelated_signing_key),
        );
        assert!(
            !evidence_ready(
                EvidencePaths {
                    desktop: &desktop,
                    mobile: &mobile,
                    attestation: &attestation,
                },
                &before,
                expected,
                SystemTime::UNIX_EPOCH,
            )
            .expect("signature by an untrusted key")
        );

        write_attestation(
            &attestation,
            expected,
            expected.marker_sha256,
            &desktop_evidence,
            &mobile_evidence,
            Some(&signing_key),
        );
        assert!(
            evidence_ready(
                EvidencePaths {
                    desktop: &desktop,
                    mobile: &mobile,
                    attestation: &attestation,
                },
                &before,
                expected,
                SystemTime::UNIX_EPOCH,
            )
            .expect("independently signed evidence")
        );

        assert!(
            !evidence_ready(
                EvidencePaths {
                    desktop: &desktop,
                    mobile: &mobile,
                    attestation: &attestation,
                },
                &before,
                expected,
                SystemTime::now() + Duration::from_secs(60),
            )
            .expect("stale evidence")
        );

        write_attestation(
            &attestation,
            expected,
            "unrelated-marker-hash",
            &desktop_evidence,
            &mobile_evidence,
            Some(&signing_key),
        );
        assert!(
            !evidence_ready(
                EvidencePaths {
                    desktop: &desktop,
                    mobile: &mobile,
                    attestation: &attestation,
                },
                &before,
                expected,
                SystemTime::UNIX_EPOCH,
            )
            .expect("unrelated marker")
        );

        // Encode the desktop's identical opaque pixels through a different
        // PNG color type. File hashes differ, but canonical RGBA pixels do
        // not, so this cannot masquerade as independent mobile evidence.
        let rgba = image::RgbaImage::from_pixel(32, 24, image::Rgba([0, 0, 0, 255]));
        image::DynamicImage::ImageRgba8(rgba)
            .save(&mobile)
            .expect("re-encoded mobile PNG");
        let reencoded = image_evidence(&mobile);
        assert_ne!(desktop_evidence.file_sha256, reencoded.file_sha256);
        assert_eq!(desktop_evidence.pixel_sha256, reencoded.pixel_sha256);
        write_attestation(
            &attestation,
            expected,
            expected.marker_sha256,
            &desktop_evidence,
            &reencoded,
            Some(&signing_key),
        );
        assert!(
            !evidence_ready(
                EvidencePaths {
                    desktop: &desktop,
                    mobile: &mobile,
                    attestation: &attestation,
                },
                &before,
                expected,
                SystemTime::UNIX_EPOCH,
            )
            .expect("same decoded pixels")
        );

        let unchanged = EvidenceBefore {
            desktop: Some(desktop_evidence.file_sha256.clone()),
            mobile: None,
            attestation: None,
        };
        assert!(
            !evidence_ready(
                EvidencePaths {
                    desktop: &desktop,
                    mobile: &mobile,
                    attestation: &attestation,
                },
                &unchanged,
                expected,
                SystemTime::UNIX_EPOCH,
            )
            .expect("pre-send snapshot")
        );
    }

    fn image_evidence(path: &Path) -> ImageEvidence {
        changed_decodable_image(path, None, SystemTime::UNIX_EPOCH)
            .expect("decode image")
            .expect("image evidence")
    }

    fn write_attestation(
        path: &Path,
        expected: ExpectedAttestation<'_>,
        marker_sha256: &str,
        desktop: &ImageEvidence,
        mobile: &ImageEvidence,
        signing_key: Option<&SigningKey>,
    ) {
        let binding = evidence_binding(&expected, desktop, mobile);
        let signature = signing_key.map_or_else(
            || "00".repeat(64),
            |key| {
                hex_digest(
                    key.sign(review_signature_message(&binding).as_bytes())
                        .to_bytes(),
                )
            },
        );
        std::fs::write(
            path,
            serde_json::to_vec(&serde_json::json!({
                "version": 3,
                "nonce": expected.nonce,
                "message_id": expected.message_id,
                "body_sha256": expected.body_sha256,
                "markdown_sha256": expected.markdown_sha256,
                "marker": {
                    "verdict": "visible_in_both",
                    "sha256": marker_sha256,
                },
                "desktop": {
                    "verdict": "pass",
                    "file_sha256": desktop.file_sha256,
                    "pixel_sha256": desktop.pixel_sha256,
                    "width": desktop.width,
                    "height": desktop.height,
                },
                "mobile": {
                    "verdict": "pass",
                    "file_sha256": mobile.file_sha256,
                    "pixel_sha256": mobile.pixel_sha256,
                    "width": mobile.width,
                    "height": mobile.height,
                },
                "review": {
                    "reviewer": expected.reviewer,
                    "public_key_sha256": expected.review_public_key_sha256,
                    "signature_ed25519": signature,
                },
                "evidence_sha256": binding,
                "table": "fenced",
            }))
            .expect("attestation JSON"),
        )
        .expect("attestation file");
    }
}
