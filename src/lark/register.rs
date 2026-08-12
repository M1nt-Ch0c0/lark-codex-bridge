//! `PersonalAgent` QR registration device flow and existing-app onboarding.
//!
//! Protocol semantics mirror the reference SDK's `registerApp`: begin against
//! the tenant's accounts host, show the QR URL, then poll with
//! server-directed intervals. A `user_info.tenant_brand == "lark"` response
//! switches the accounts base to `accounts.larksuite.com` exactly once.

use std::io::Write;
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use flate2::Compression;
use flate2::write::GzEncoder;
use secrecy::SecretString;
use serde_json::Value;
use url::Url;

use super::api::{BotInfo, LarkApi};
use super::config::{LarkEndpoints, TenantBrand};
use super::credentials::LarkCredentials;
use super::error::LarkError;
use super::http::LarkHttp;
use super::token::TenantTokenProvider;
use crate::limits::LARK_REGISTER_TIMEOUT;

const REGISTRATION_PATH: &str = "/oauth/v1/app/registration";
const DEFAULT_EXPIRES_IN_SECS: u64 = 600;
const DEFAULT_INTERVAL_SECS: u64 = 5;
/// Server-directed poll intervals are clamped into this range so a hostile or
/// broken accounts host cannot turn polling into a busy loop or a stall.
const MIN_INTERVAL_SECS: u64 = 1;
const MAX_INTERVAL_SECS: u64 = 60;
const SLOW_DOWN_STEP: Duration = Duration::from_secs(5);

/// QR challenge returned by [`RegistrationFlow::begin`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QrChallenge {
    /// `verification_uri_complete` plus the SDK tracking parameters.
    pub url: String,
    /// Seconds the challenge stays valid; server default 600.
    pub expires_in: u64,
    /// Initial poll interval in seconds; server default 5.
    pub interval: u64,
}

/// One step of the registration poll loop.
#[derive(Debug)]
pub enum RegistrationOutcome {
    /// The user completed authorization; credentials are ready to validate.
    Credentials {
        /// The freshly issued app credentials.
        creds: LarkCredentials,
        /// The authorizing user's `open_id`, when the server provided one.
        bot_hint: Option<String>,
    },
    /// Authorization is still pending (or the accounts domain just switched);
    /// poll again after the current interval.
    Pending,
    /// The server asked to slow down; poll again after the grown interval.
    SlowDown {
        /// The new poll interval in seconds.
        new_interval: u64,
    },
}

/// `PersonalAgent` device-flow registration.
///
/// One flow instance is single-use: [`RegistrationFlow::begin`] starts the
/// session and [`RegistrationFlow::poll_once`] advances it. Polling cadence
/// is owned by the caller via [`RegistrationFlow::interval`]; the flow itself
/// never sleeps.
pub struct RegistrationFlow {
    http: LarkHttp,
    accounts_base: Url,
    lark_accounts_base: Url,
    domain_switched: bool,
    addons: Option<Value>,
    timeout: Duration,
    device_code: Option<String>,
    interval: Duration,
    deadline: Option<Instant>,
}

impl RegistrationFlow {
    /// Starts a flow against the tenant's accounts base, with the official
    /// `accounts.larksuite.com` as the one-time Lark switch target and the
    /// milestone's default registration deadline.
    #[must_use]
    pub fn new(http: LarkHttp, addons: Option<Value>) -> Self {
        Self::with_parts(
            http,
            LarkEndpoints::for_tenant(TenantBrand::Lark).accounts_base,
            addons,
            LARK_REGISTER_TIMEOUT,
        )
    }

    /// Starts a flow with an explicit Lark switch target and deadline
    /// (tests point both bases at local stubs).
    #[must_use]
    pub fn with_parts(
        http: LarkHttp,
        lark_accounts_base: Url,
        addons: Option<Value>,
        timeout: Duration,
    ) -> Self {
        let accounts_base = http.endpoints().accounts_base.clone();
        Self {
            http,
            accounts_base,
            lark_accounts_base,
            domain_switched: false,
            addons,
            timeout,
            device_code: None,
            interval: Duration::from_secs(DEFAULT_INTERVAL_SECS),
            deadline: None,
        }
    }

    /// Returns the current server-directed poll interval.
    #[must_use]
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Begins the device flow and returns the QR challenge to display.
    ///
    /// # Errors
    ///
    /// Returns a classified error on transport failure or a malformed begin
    /// response.
    pub async fn begin(&mut self) -> Result<QrChallenge, LarkError> {
        let response = self
            .post(&[
                ("action", "begin"),
                ("archetype", "PersonalAgent"),
                ("auth_method", "client_secret"),
                ("request_user_info", "open_id"),
            ])
            .await?;
        let device_code = response
            .get("device_code")
            .and_then(Value::as_str)
            .filter(|code| !code.is_empty())
            .ok_or_else(|| LarkError::protocol("registration begin missing device_code"))?;
        let verification_uri_complete = response
            .get("verification_uri_complete")
            .and_then(Value::as_str)
            .filter(|uri| !uri.is_empty())
            .ok_or_else(|| {
                LarkError::protocol("registration begin missing verification_uri_complete")
            })?;
        let expires_in = response
            .get("expires_in")
            .and_then(Value::as_u64)
            .unwrap_or(DEFAULT_EXPIRES_IN_SECS);
        // Clamp the server-directed poll interval: a zero or tiny interval
        // would busy-poll the accounts host until the deadline.
        let interval = response
            .get("interval")
            .and_then(Value::as_u64)
            .unwrap_or(DEFAULT_INTERVAL_SECS)
            .clamp(MIN_INTERVAL_SECS, MAX_INTERVAL_SECS);

        let url = self.qr_url(verification_uri_complete)?;
        self.device_code = Some(device_code.to_owned());
        self.interval = Duration::from_secs(interval);
        self.deadline = Some(Instant::now() + self.timeout);
        Ok(QrChallenge {
            url,
            expires_in,
            interval,
        })
    }

    /// Polls the device flow once.
    ///
    /// # Errors
    ///
    /// Returns `PermanentAuth` on `access_denied`, `Exhausted` on
    /// `expired_token` or deadline expiry, `Retryable` on transient failures,
    /// and `ProtocolViolation` on malformed or unknown responses.
    pub async fn poll_once(&mut self) -> Result<RegistrationOutcome, LarkError> {
        let device_code = self
            .device_code
            .clone()
            .ok_or_else(|| LarkError::protocol("registration poll before begin"))?;
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(LarkError::exhausted(
                "registration deadline",
                self.timeout.as_secs(),
            ));
        }
        let response = self
            .post(&[("action", "poll"), ("device_code", &device_code)])
            .await?;

        let tenant_brand = response
            .get("user_info")
            .and_then(|info| info.get("tenant_brand"))
            .and_then(Value::as_str);
        // Mirror the reference: a Lark-brand answer redirects polling to the
        // Lark accounts host exactly once, before any success handling.
        if tenant_brand == Some("lark") && !self.domain_switched {
            self.domain_switched = true;
            self.accounts_base = self.lark_accounts_base.clone();
            return Ok(RegistrationOutcome::Pending);
        }

        let client_id = response
            .get("client_id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty());
        let client_secret = response
            .get("client_secret")
            .and_then(Value::as_str)
            .filter(|secret| !secret.is_empty());
        if let (Some(app_id), Some(app_secret)) = (client_id, client_secret) {
            let tenant = if tenant_brand == Some("lark") {
                TenantBrand::Lark
            } else {
                TenantBrand::Feishu
            };
            let bot_hint = response
                .get("user_info")
                .and_then(|info| info.get("open_id"))
                .and_then(Value::as_str)
                .map(str::to_owned);
            return Ok(RegistrationOutcome::Credentials {
                creds: LarkCredentials::new(
                    app_id.to_owned(),
                    SecretString::from(app_secret),
                    tenant,
                ),
                bot_hint,
            });
        }

        match response.get("error").and_then(Value::as_str) {
            None | Some("authorization_pending") => Ok(RegistrationOutcome::Pending),
            Some("slow_down") => {
                self.interval += SLOW_DOWN_STEP;
                Ok(RegistrationOutcome::SlowDown {
                    new_interval: self.interval.as_secs(),
                })
            }
            Some("access_denied") => Err(LarkError::permanent_auth(
                "device flow authorization was denied",
            )),
            Some("expired_token") => Err(LarkError::exhausted(
                "device flow session lifetime",
                self.timeout.as_secs(),
            )),
            Some(_) => Err(LarkError::protocol(
                "device flow returned an unknown error code",
            )),
        }
    }

    async fn post(&self, form: &[(&str, &str)]) -> Result<Value, LarkError> {
        let url = self
            .accounts_base
            .join(REGISTRATION_PATH)
            .map_err(|_| LarkError::protocol("invalid accounts base URL"))?;
        self.http.post_form_at(url, form).await
    }

    fn qr_url(&self, verification_uri_complete: &str) -> Result<String, LarkError> {
        let mut url = Url::parse(verification_uri_complete)
            .map_err(|_| LarkError::protocol("invalid verification_uri_complete"))?;
        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("from", "sdk");
            pairs.append_pair("source", "lark-codex-bridge");
            pairs.append_pair("tp", "sdk");
            if let Some(addons) = &self.addons {
                pairs.append_pair("addons", &encode_addons(addons)?);
            }
        }
        Ok(url.into())
    }
}

/// Encodes registration addons for the QR URL: JSON → gzip → base64url with
/// `+`→`-`, `/`→`_`, and `=` padding stripped, matching the reference SDK's
/// `encodeAddons` pipeline.
///
/// # Errors
///
/// Returns an error when the addons fail to serialize or compress.
pub fn encode_addons(addons: &Value) -> Result<String, LarkError> {
    let json = serde_json::to_vec(addons)
        .map_err(|_| LarkError::protocol("serializing registration addons"))?;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(&json)
        .map_err(|_| LarkError::protocol("compressing registration addons"))?;
    let gzip = encoder
        .finish()
        .map_err(|_| LarkError::protocol("compressing registration addons"))?;
    Ok(URL_SAFE_NO_PAD.encode(gzip))
}

/// Validates existing app credentials by exchanging a tenant token and
/// fetching the bot identity.
///
/// # Errors
///
/// Returns `PermanentAuth` for rejected credentials and `Retryable` for
/// transient failures.
pub async fn validate_credentials(
    http: LarkHttp,
    creds: LarkCredentials,
) -> Result<BotInfo, LarkError> {
    let tokens = TenantTokenProvider::new(http.clone(), creds);
    LarkApi::new(http, tokens).bot_info().await
}
