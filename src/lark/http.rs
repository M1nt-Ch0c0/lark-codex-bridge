//! Shared rustls-only HTTP core with bounded response bodies and classified
//! errors.

use std::fmt;

use bytes::Bytes;
use reqwest::{Client, RequestBuilder, StatusCode};
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::config::LarkEndpoints;
use super::error::LarkError;
use crate::limits::{LARK_HTTP_TIMEOUT, LARK_MAX_HTTP_BODY_BYTES};

/// Shared HTTP client bound to one tenant's endpoints.
///
/// Response bodies are capped at [`LARK_MAX_HTTP_BODY_BYTES`] before JSON
/// parsing; TLS is rustls only.
#[derive(Clone)]
pub struct LarkHttp {
    client: Client,
    endpoints: LarkEndpoints,
}

impl LarkHttp {
    /// Builds the shared client with the milestone's 15 second timeout.
    ///
    /// # Errors
    ///
    /// Returns an error when the TLS-backed client cannot be initialized.
    pub fn new(endpoints: LarkEndpoints) -> Result<Self, LarkError> {
        let client = Client::builder()
            .timeout(LARK_HTTP_TIMEOUT)
            .user_agent(concat!("lark-codex-bridge/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|_| LarkError::retryable("initializing the HTTP client"))?;
        Ok(Self { client, endpoints })
    }

    /// Returns the tenant endpoints this client is bound to.
    #[must_use]
    pub fn endpoints(&self) -> &LarkEndpoints {
        &self.endpoints
    }

    /// POSTs a JSON body to `{open_base}{path}` and parses the JSON response.
    ///
    /// # Errors
    ///
    /// Returns a classified error on transport failure, non-success status,
    /// oversize body, or malformed JSON.
    pub async fn post_json<P, R>(&self, path: &str, body: &P) -> Result<R, LarkError>
    where
        P: Serialize + Sync,
        R: DeserializeOwned,
    {
        let url = self.endpoints.open_url(path)?;
        let request = self.client.post(url).json(body);
        let (status, bytes) = self
            .execute(request, "POSTing an OpenAPI JSON request")
            .await?;
        ensure_success(status, "POSTing an OpenAPI JSON request")?;
        parse_json(&bytes, "parsing an OpenAPI JSON response")
    }

    /// GETs `{open_base}{path}` with an optional bearer token and parses the
    /// JSON response.
    ///
    /// # Errors
    ///
    /// Returns a classified error on transport failure, non-success status,
    /// oversize body, or malformed JSON.
    pub async fn get_json<R>(
        &self,
        path: &str,
        bearer: Option<&SecretString>,
    ) -> Result<R, LarkError>
    where
        R: DeserializeOwned,
    {
        let url = self.endpoints.open_url(path)?;
        let mut request = self.client.get(url);
        if let Some(token) = bearer {
            request = request.bearer_auth(token.expose_secret());
        }
        let (status, bytes) = self
            .execute(request, "GETting an OpenAPI JSON request")
            .await?;
        ensure_success(status, "GETting an OpenAPI JSON request")?;
        parse_json(&bytes, "parsing an OpenAPI JSON response")
    }

    /// POSTs a form to `{accounts_base}{path}` for the registration device
    /// flow, which returns protocol errors as 4xx responses with a JSON body
    /// (RFC 8628). Any status below 500 therefore still parses as JSON; 5xx is
    /// classified retryable.
    ///
    /// # Errors
    ///
    /// Returns a classified error on transport failure, 5xx status, oversize
    /// body, or malformed JSON.
    pub async fn post_accounts_form(
        &self,
        path: &str,
        form: &[(&str, &str)],
    ) -> Result<serde_json::Value, LarkError> {
        let url = self.endpoints.accounts_url(path)?;
        self.post_form_at(url, form).await
    }

    /// Same as [`LarkHttp::post_accounts_form`] but against an explicit base
    /// URL, so the registration flow can switch accounts domains mid-flight.
    pub(crate) async fn post_form_at(
        &self,
        url: url::Url,
        form: &[(&str, &str)],
    ) -> Result<serde_json::Value, LarkError> {
        let request = self.client.post(url).form(form);
        let context = "POSTing an accounts form request";
        let (status, bytes) = self.execute(request, context).await?;
        if status.is_server_error() {
            return Err(LarkError::Retryable {
                context,
                code: Some(i64::from(status.as_u16())),
            });
        }
        parse_json(&bytes, "parsing an accounts form response")
    }

    async fn execute(
        &self,
        request: RequestBuilder,
        context: &'static str,
    ) -> Result<(StatusCode, Bytes), LarkError> {
        let mut response = request
            .send()
            .await
            .map_err(|_| LarkError::retryable(context))?;
        let status = response.status();
        if let Some(length) = response.content_length() {
            if length > LARK_MAX_HTTP_BODY_BYTES as u64 {
                return Err(LarkError::exhausted(
                    context,
                    LARK_MAX_HTTP_BODY_BYTES as u64,
                ));
            }
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| LarkError::retryable(context))?
        {
            if body.len() + chunk.len() > LARK_MAX_HTTP_BODY_BYTES {
                return Err(LarkError::exhausted(
                    context,
                    LARK_MAX_HTTP_BODY_BYTES as u64,
                ));
            }
            body.extend_from_slice(&chunk);
        }
        Ok((status, Bytes::from(body)))
    }
}

impl fmt::Debug for LarkHttp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LarkHttp")
            .field("open_base", &self.endpoints.open_base.as_str())
            .field("accounts_base", &self.endpoints.accounts_base.as_str())
            .finish_non_exhaustive()
    }
}

fn ensure_success(status: StatusCode, context: &'static str) -> Result<(), LarkError> {
    if status.is_success() {
        return Ok(());
    }
    let code = Some(i64::from(status.as_u16()));
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        return Err(LarkError::PermanentAuth { context, code });
    }
    if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        return Err(LarkError::Retryable { context, code });
    }
    // Any other 4xx means this client sent a request the server rejects;
    // retrying the identical request cannot succeed.
    Err(LarkError::ProtocolViolation { context })
}

fn parse_json<R: DeserializeOwned>(body: &[u8], context: &'static str) -> Result<R, LarkError> {
    serde_json::from_slice(body).map_err(|_| LarkError::protocol(context))
}
