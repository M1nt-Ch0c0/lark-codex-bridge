//! Shared rustls-only HTTP core with bounded response bodies and classified
//! errors.

use std::fmt;

use bytes::Bytes;
use reqwest::{Client, RequestBuilder, StatusCode};
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::config::LarkEndpoints;
use super::error::{LarkError, check_code};
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
            // Never follow redirects: a 307/308 would re-post credential-bearing
            // bodies to the redirect target, and these endpoints never redirect.
            .redirect(reqwest::redirect::Policy::none())
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
        self.post_json_with_headers(path, body, &[]).await
    }

    /// POSTs a JSON body with extra headers (e.g. the `locale` header the
    /// WebSocket endpoint bootstrap requires). Used by the transport; the
    /// caller classifies the response body itself.
    ///
    /// # Errors
    ///
    /// Returns a classified error on transport failure, non-success status,
    /// oversize body, or malformed JSON.
    pub(crate) async fn post_json_with_headers<P, R>(
        &self,
        path: &str,
        body: &P,
        headers: &[(&str, &str)],
    ) -> Result<R, LarkError>
    where
        P: Serialize + Sync,
        R: DeserializeOwned,
    {
        let url = self.endpoints.open_url(path)?;
        let mut request = self.client.post(url).json(body);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
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

    /// POSTs a JSON body with a tenant-token bearer header and parses the
    /// JSON response. The token is attached as a header and never logged.
    ///
    /// # Errors
    ///
    /// Returns a classified error on transport failure, non-success status,
    /// oversize body, or malformed JSON.
    pub(crate) async fn post_json_bearer<P, R>(
        &self,
        path: &str,
        body: &P,
        bearer: &SecretString,
    ) -> Result<R, LarkError>
    where
        P: Serialize + Sync,
        R: DeserializeOwned,
    {
        let url = self.endpoints.open_url(path)?;
        let request = self
            .client
            .post(url)
            .json(body)
            .bearer_auth(bearer.expose_secret());
        let (status, bytes) = self
            .execute(request, "POSTing an OpenAPI JSON request")
            .await
            .map_err(post_write_failure)?;
        ensure_openapi_success(status, &bytes, "POSTing an OpenAPI JSON request")?;
        parse_json(&bytes, "parsing an OpenAPI JSON response")
    }

    /// Sends a `PATCH` JSON body with a tenant-token bearer header and parses
    /// the JSON response. Used by card updates.
    ///
    /// # Errors
    ///
    /// Returns a classified error on transport failure, non-success status,
    /// oversize body, or malformed JSON.
    pub(crate) async fn patch_json_bearer<P, R>(
        &self,
        path: &str,
        body: &P,
        bearer: &SecretString,
    ) -> Result<R, LarkError>
    where
        P: Serialize + Sync,
        R: DeserializeOwned,
    {
        let url = self.endpoints.open_url(path)?;
        let request = self
            .client
            .patch(url)
            .json(body)
            .bearer_auth(bearer.expose_secret());
        let (status, bytes) = self
            .execute(request, "PATCHing an OpenAPI JSON request")
            .await
            .map_err(post_write_failure)?;
        ensure_openapi_success(status, &bytes, "PATCHing an OpenAPI JSON request")?;
        parse_json(&bytes, "parsing an OpenAPI JSON response")
    }

    /// POSTs a multipart form with a tenant-token bearer header and parses
    /// the JSON response. Used by image/file uploads.
    ///
    /// # Errors
    ///
    /// Returns a classified error on transport failure, non-success status,
    /// oversize body, or malformed JSON.
    pub(crate) async fn post_multipart_bearer<R>(
        &self,
        path: &str,
        form: reqwest::multipart::Form,
        bearer: &SecretString,
    ) -> Result<R, LarkError>
    where
        R: DeserializeOwned,
    {
        let url = self.endpoints.open_url(path)?;
        let request = self
            .client
            .post(url)
            .multipart(form)
            .bearer_auth(bearer.expose_secret());
        let (status, bytes) = self
            .execute(request, "POSTing an OpenAPI multipart request")
            .await?;
        ensure_success(status, "POSTing an OpenAPI multipart request")?;
        parse_json(&bytes, "parsing an OpenAPI JSON response")
    }

    /// GETs `{open_base}{path}` with a tenant-token bearer header, streaming
    /// the body into memory with a hard `limit` byte cap. The stream is
    /// aborted mid-body once the cap is exceeded rather than buffering an
    /// unbounded response.
    ///
    /// # Errors
    ///
    /// Returns a classified error on transport failure, non-success status,
    /// or an oversize body.
    pub(crate) async fn get_bytes_bearer(
        &self,
        path: &str,
        bearer: &SecretString,
        limit: usize,
    ) -> Result<Bytes, LarkError> {
        let url = self.endpoints.open_url(path)?;
        let request = self.client.get(url).bearer_auth(bearer.expose_secret());
        let context = "GETting an OpenAPI binary resource";
        let mut response = request
            .send()
            .await
            .map_err(|_| LarkError::retryable(context))?;
        let status = response.status();
        ensure_success(status, context)?;
        let is_json = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/json"));
        if let Some(length) = response.content_length() {
            if length > limit as u64 {
                return Err(LarkError::exhausted(context, limit as u64));
            }
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| LarkError::retryable(context))?
        {
            if body.len() + chunk.len() > limit {
                return Err(LarkError::exhausted(context, limit as u64));
            }
            body.extend_from_slice(&chunk);
        }
        if is_json {
            // A JSON content type on a binary resource endpoint means the
            // server returned an error envelope instead of resource bytes;
            // classify its code so token failures still trigger the caller's
            // single forced refresh.
            let envelope: serde_json::Value = serde_json::from_slice(&body)
                .map_err(|_| LarkError::protocol("parsing a resource error envelope"))?;
            let code = envelope
                .get("code")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| LarkError::protocol("resource envelope missing code"))?;
            check_code(code, context)?;
            return Err(LarkError::protocol(
                "resource endpoint returned a JSON envelope",
            ));
        }
        Ok(Bytes::from(body))
    }

    /// POSTs a form to `{accounts_base}{path}` for the registration device
    /// flow, which returns protocol errors as 4xx responses with a JSON body
    /// (RFC 8628). Other 4xx statuses still parse as JSON, while 401/403 are
    /// classified permanent and 429/5xx retryable.
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
        let code = Some(i64::from(status.as_u16()));
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            return Err(LarkError::PermanentAuth { context, code });
        }
        if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
            return Err(LarkError::Retryable { context, code });
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
    // Any other 4xx is a definitive rejection: the server responded and
    // nothing was sent. Keep the `ProtocolViolation` kind (so the transport
    // still treats an explicit bootstrap rejection as fatal) but carry the
    // HTTP status as a `code` so the delivery classifier can tell a rejected
    // (safe-to-retry, bounded) send from an unparsable response.
    Err(LarkError::ProtocolViolation { context, code })
}

/// Lark sometimes returns a structured `OpenAPI` error envelope with HTTP 400,
/// including the documented `99991400` application-frequency limit. Preserve
/// that more specific business code before falling back to HTTP status
/// classification. A contradictory `code: 0` never turns a non-success HTTP
/// response into success.
fn ensure_openapi_success(
    status: StatusCode,
    body: &[u8],
    context: &'static str,
) -> Result<(), LarkError> {
    if status.is_success() {
        return Ok(());
    }
    // HTTP transport-level throttling and server failures remain transient
    // even when a proxy or gateway happens to attach an unrelated JSON code.
    if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        return ensure_success(status, context);
    }
    if let Some(code) = serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|value| value.get("code").and_then(serde_json::Value::as_i64))
        .filter(|code| *code != 0)
    {
        return check_code(code, context);
    }
    ensure_success(status, context)
}

/// Converts a bounded-response failure after a mutating request was started
/// into an uncertain outcome. The peer may already have applied the write even
/// though its response body could not be retained. Transport failures already
/// use a no-code `Retryable` and therefore remain uncertain as well.
fn post_write_failure(error: LarkError) -> LarkError {
    match error {
        LarkError::Exhausted { .. } => {
            LarkError::protocol("mutating OpenAPI response exceeded the response byte cap")
        }
        other => other,
    }
}

fn parse_json<R: DeserializeOwned>(body: &[u8], context: &'static str) -> Result<R, LarkError> {
    serde_json::from_slice(body).map_err(|_| LarkError::protocol(context))
}
