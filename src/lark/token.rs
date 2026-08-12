//! Tenant access token cache with early refresh and single-flight fetches.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use super::credentials::LarkCredentials;
use super::error::{LarkError, check_code};
use super::http::LarkHttp;
use crate::limits::TOKEN_REFRESH_SKEW;

const TOKEN_PATH: &str = "/open-apis/auth/v3/tenant_access_token/internal";

/// Caches the tenant access token for one app.
///
/// The cache holds exactly one `{token, refresh_after}` entry, so it is
/// bounded by construction. Fetches are single-flight: the async mutex is
/// held across the network request so concurrent callers share one exchange.
/// The cached token is never logged.
#[derive(Clone)]
pub struct TenantTokenProvider {
    inner: Arc<TokenInner>,
}

struct TokenInner {
    http: LarkHttp,
    creds: LarkCredentials,
    state: Mutex<TokenState>,
}

#[derive(Default)]
struct TokenState {
    cached: Option<CachedToken>,
}

#[derive(Clone)]
struct CachedToken {
    token: SecretString,
    refresh_after: Instant,
}

impl TenantTokenProvider {
    /// Creates a provider over the shared HTTP core and app credentials.
    #[must_use]
    pub fn new(http: LarkHttp, creds: LarkCredentials) -> Self {
        Self {
            inner: Arc::new(TokenInner {
                http,
                creds,
                state: Mutex::new(TokenState::default()),
            }),
        }
    }

    /// Returns a valid tenant access token, refreshing it when the cached
    /// token is within [`TOKEN_REFRESH_SKEW`] of expiry.
    ///
    /// # Errors
    ///
    /// Returns `PermanentAuth` for rejected credentials, `Retryable` for
    /// transient failures, and `ProtocolViolation` for malformed responses.
    pub async fn token(&self) -> Result<SecretString, LarkError> {
        let mut state = self.inner.state.lock().await;
        if let Some(cached) = &state.cached {
            if Instant::now() < cached.refresh_after {
                return Ok(cached.token.clone());
            }
        }
        let fresh = self.fetch_token().await?;
        let token = fresh.token.clone();
        state.cached = Some(fresh);
        Ok(token)
    }

    /// Returns a freshly exchanged tenant access token, ignoring and
    /// replacing the cached entry.
    ///
    /// Used by the `OpenAPI` client when a call fails with a token-invalid
    /// error: the cached token is provably stale, so exactly one forced
    /// refresh is followed by exactly one retry of the failed call.
    ///
    /// # Errors
    ///
    /// Returns `PermanentAuth` for rejected credentials, `Retryable` for
    /// transient failures, and `ProtocolViolation` for malformed responses.
    pub async fn force_refresh(&self) -> Result<SecretString, LarkError> {
        let mut state = self.inner.state.lock().await;
        let fresh = self.fetch_token().await?;
        let token = fresh.token.clone();
        state.cached = Some(fresh);
        Ok(token)
    }

    async fn fetch_token(&self) -> Result<CachedToken, LarkError> {
        #[derive(Serialize)]
        struct TokenRequest<'a> {
            app_id: &'a str,
            app_secret: &'a str,
        }
        #[derive(Deserialize)]
        struct TokenResponse {
            code: i64,
            tenant_access_token: Option<String>,
            expire: Option<i64>,
        }

        let request = TokenRequest {
            app_id: &self.inner.creds.app_id,
            app_secret: self.inner.creds.app_secret.expose_secret(),
        };
        let response: TokenResponse = self.inner.http.post_json(TOKEN_PATH, &request).await?;
        check_code(response.code, "exchanging the tenant access token")?;
        let token = response
            .tenant_access_token
            .filter(|token| !token.is_empty())
            .ok_or_else(|| LarkError::protocol("token response missing tenant_access_token"))?;
        let expire = response
            .expire
            .ok_or_else(|| LarkError::protocol("token response missing expire"))?;
        let validity = u64::try_from(expire)
            .map(Duration::from_secs)
            .map_err(|_| LarkError::protocol("token response has a negative expire"))?;
        // A validity shorter than the skew leaves the token immediately
        // stale, so the next call refetches instead of serving an expired one.
        let refresh_after = Instant::now()
            .checked_add(validity.saturating_sub(TOKEN_REFRESH_SKEW))
            .ok_or_else(|| LarkError::protocol("token expire out of range"))?;
        Ok(CachedToken {
            token: SecretString::from(token),
            refresh_after,
        })
    }
}

impl fmt::Debug for TenantTokenProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TenantTokenProvider")
            .field("app_id", &self.inner.creds.app_id)
            .field("tenant", &self.inner.creds.tenant)
            .finish_non_exhaustive()
    }
}
