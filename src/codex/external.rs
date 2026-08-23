//! Fail-closed configuration and one-shot admission gate for external app-server endpoints.
//!
//! It authenticates one bounded connection, validates initialize metadata against an exact
//! promoted wire adapter, and runs the read-only `thread/list` capability canary. No thread
//! identifier or raw RPC value leaves the gate. The separately owned long-running socket transport
//! can take over only this same admitted connection, so it cannot bypass the per-connection gate.

use std::{
    fmt,
    fs::{self, OpenOptions},
    io::Read,
    net::IpAddr,
    path::{Component, Path, PathBuf},
};

use futures_util::{SinkExt, StreamExt};
use secrecy::{ExposeSecret, SecretString};
use semver::Version;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{net::TcpStream, time::timeout};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{
        Error as WebSocketError, Message,
        client::IntoClientRequest,
        http::{HeaderValue, StatusCode, header::AUTHORIZATION},
        protocol::WebSocketConfig,
    },
};
use url::{Host, Url};

use crate::{
    codex::{
        compat::{SharedWireProfile, WireAdapter},
        process::CodexProcessConfig,
        protocol::{InboundMessage, OutboundMessage, RequestId, decode_line, encode_line},
        types::{ClientInfo, InitializeCapabilities, InitializeParams, ThreadListParams},
    },
    limits::{
        EXTERNAL_GATE_MAX_MESSAGES, EXTERNAL_GATE_MESSAGE_BYTES, EXTERNAL_GATE_TIMEOUT,
        EXTERNAL_GATE_TOTAL_BYTES, EXTERNAL_WS_CLOSE_TIMEOUT, EXTERNAL_WS_MESSAGE_BYTES,
        MAX_EXTERNAL_AUTH_TOKEN_BYTES, MAX_EXTERNAL_ENDPOINT_BYTES, MAX_EXTERNAL_SECRET_PATH_BYTES,
    },
};

/// Exhaustive backend choice. Serde's internal tag and unknown-field rejection make fields from
/// one mode invalid in the other mode instead of permitting inference or fallback.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum CodexBackendConfig {
    SpawnedStdio {
        #[serde(default = "default_codex_binary")]
        binary: PathBuf,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        codex_home: Option<PathBuf>,
    },
    ExternalEndpoint {
        endpoint: String,
        expected_codex_version: String,
        capability_profile: ExternalCapabilityProfile,
        authentication: ExternalAuthentication,
    },
}

fn default_codex_binary() -> PathBuf {
    PathBuf::from("codex")
}

impl Default for CodexBackendConfig {
    fn default() -> Self {
        Self::SpawnedStdio {
            binary: default_codex_binary(),
            codex_home: None,
        }
    }
}

impl fmt::Debug for CodexBackendConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SpawnedStdio { codex_home, .. } => formatter
                .debug_struct("SpawnedStdio")
                .field("binary", &"[configured]")
                .field("codex_home", &codex_home.as_ref().map(|_| "[configured]"))
                .finish(),
            Self::ExternalEndpoint {
                expected_codex_version,
                capability_profile,
                authentication,
                ..
            } => formatter
                .debug_struct("ExternalEndpoint")
                .field("endpoint", &"[redacted]")
                .field("expected_codex_version", expected_codex_version)
                .field("capability_profile", capability_profile)
                .field("authentication", authentication)
                .finish(),
        }
    }
}

impl CodexBackendConfig {
    /// Returns a process configuration only for the explicitly tagged process-owning mode.
    #[must_use]
    pub fn spawned_process_config(&self) -> Option<CodexProcessConfig> {
        match self {
            Self::SpawnedStdio { binary, codex_home } => Some(CodexProcessConfig {
                binary: binary.clone(),
                codex_home: codex_home.clone(),
            }),
            Self::ExternalEndpoint { .. } => None,
        }
    }

    /// Produces a one-shot external admission gate only for the explicitly tagged external mode.
    ///
    /// # Errors
    ///
    /// Returns a static error classification for unsafe endpoint, version, profile, or credential
    /// source configuration.
    pub fn external_gate(&self) -> Result<Option<ExternalEndpointGate>, ExternalGateError> {
        match self {
            Self::SpawnedStdio { .. } => Ok(None),
            Self::ExternalEndpoint {
                endpoint,
                expected_codex_version,
                capability_profile,
                authentication,
            } => ExternalEndpointGate::new(ExternalEndpointConfig {
                endpoint: endpoint.clone(),
                expected_codex_version: expected_codex_version.clone(),
                capability_profile: *capability_profile,
                authentication: authentication.clone(),
            })
            .map(Some),
        }
    }

    /// Validates mode-local configuration without reading a secret or opening a socket.
    ///
    /// # Errors
    ///
    /// Returns a static error classification for invalid external configuration.
    pub fn validate(&self) -> Result<(), ExternalGateError> {
        match self {
            Self::SpawnedStdio { .. } => Ok(()),
            Self::ExternalEndpoint { .. } => self.external_gate().map(|_| ()),
        }
    }
}

/// Explicitly promoted external shared-endpoint capability profiles.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExternalCapabilityProfile {
    ObserveShared,
    ResumeShared,
    MutateShared,
    QueueShared,
}

impl ExternalCapabilityProfile {
    const fn shared_wire_profile(self) -> SharedWireProfile {
        match self {
            Self::ObserveShared => SharedWireProfile::ObserveShared,
            Self::ResumeShared => SharedWireProfile::ResumeShared,
            Self::MutateShared => SharedWireProfile::MutateShared,
            Self::QueueShared => SharedWireProfile::QueueShared,
        }
    }
}

/// External credentials are referenced, never embedded in TOML or an endpoint.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExternalAuthentication {
    BearerTokenFile { path: PathBuf },
}

impl fmt::Debug for ExternalAuthentication {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BearerTokenFile { .. } => formatter
                .debug_struct("BearerTokenFile")
                .field("path", &"[redacted]")
                .finish(),
        }
    }
}

/// External endpoint fields copied out of the tagged bridge configuration.
#[derive(Clone, Eq, PartialEq)]
pub struct ExternalEndpointConfig {
    pub endpoint: String,
    pub expected_codex_version: String,
    pub capability_profile: ExternalCapabilityProfile,
    pub authentication: ExternalAuthentication,
}

impl fmt::Debug for ExternalEndpointConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExternalEndpointConfig")
            .field("endpoint", &"[redacted]")
            .field("expected_codex_version", &self.expected_codex_version)
            .field("capability_profile", &self.capability_profile)
            .field("authentication", &self.authentication)
            .finish()
    }
}

/// Opaque, collision-resistant identifier for non-secret endpoint configuration.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct EndpointLabel(String);

impl EndpointLabel {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for EndpointLabel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl fmt::Display for EndpointLabel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Safe admission result. It intentionally contains no endpoint, path, headers, payloads, or IDs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalGateReport {
    pub endpoint_label: EndpointLabel,
    pub codex_version: Version,
    pub capability_profile: ExternalCapabilityProfile,
}

/// Static, redacted external admission failures.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ExternalGateError {
    #[error("external Codex endpoint configuration is invalid or unsafe")]
    InvalidEndpoint,
    #[error("external Codex expected version must be one exact promoted version")]
    InvalidExpectedVersion,
    #[error("external Codex capability profile is not promoted for the exact version")]
    UnsupportedCapabilityProfile,
    #[error("external Codex credential source is invalid")]
    InvalidCredentialSource,
    #[error("external Codex credential is unavailable or invalid")]
    CredentialUnavailable,
    #[error("external Codex endpoint rejected authentication")]
    AuthenticationRejected,
    #[error("external Codex endpoint connection failed")]
    ConnectionFailed,
    #[error("external Codex endpoint gate timed out")]
    Timeout,
    #[error("external Codex endpoint violated the promoted protocol contract")]
    ProtocolViolation,
    #[error("external Codex endpoint initialize metadata did not match the exact version")]
    VersionMismatch,
    #[error("external Codex endpoint is missing the required read-only capability")]
    MissingCapability,
}

/// Reusable one-shot gate. Credentials are loaded afresh for every check, so rotation is an
/// explicit new connection rather than an in-place header mutation or an authentication fallback.
#[derive(Clone)]
pub struct ExternalEndpointGate {
    endpoint: Url,
    expected_version: Version,
    wire: WireAdapter,
    capability_profile: ExternalCapabilityProfile,
    authentication: ExternalAuthentication,
    endpoint_label: EndpointLabel,
}

pub(crate) type ExternalSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub(crate) struct AdmittedExternalSocket {
    pub socket: ExternalSocket,
    pub report: ExternalGateReport,
    pub wire: WireAdapter,
}

impl fmt::Debug for ExternalEndpointGate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExternalEndpointGate")
            .field("endpoint", &"[redacted]")
            .field("endpoint_label", &self.endpoint_label)
            .field("expected_version", &self.expected_version)
            .field("wire_version", &self.wire.codex_version())
            .field("capability_profile", &self.capability_profile)
            .field("authentication", &self.authentication)
            .finish()
    }
}

impl ExternalEndpointGate {
    /// Validates endpoint, exact-version, profile, and secret-source policy without opening I/O.
    ///
    /// # Errors
    ///
    /// Returns a static failure classification and never echoes configuration values.
    pub fn new(config: ExternalEndpointConfig) -> Result<Self, ExternalGateError> {
        let endpoint = validate_endpoint(&config.endpoint)?;
        let expected_version = parse_exact_version(&config.expected_codex_version)?;
        let wire = WireAdapter::for_version(&expected_version)
            .ok_or(ExternalGateError::InvalidExpectedVersion)?;
        if !wire.supports_shared_profile(config.capability_profile.shared_wire_profile()) {
            return Err(ExternalGateError::UnsupportedCapabilityProfile);
        }
        validate_authentication_source(&config.authentication)?;
        let endpoint_label =
            endpoint_label(&endpoint, &expected_version, config.capability_profile);
        Ok(Self {
            endpoint,
            expected_version,
            wire,
            capability_profile: config.capability_profile,
            authentication: config.authentication,
            endpoint_label,
        })
    }

    #[must_use]
    pub fn endpoint_label(&self) -> &EndpointLabel {
        &self.endpoint_label
    }

    #[must_use]
    pub const fn capability_profile(&self) -> ExternalCapabilityProfile {
        self.capability_profile
    }

    /// Authenticates and runs initialize, exact-version, and read-only capability gates.
    ///
    /// Credentials are read immediately before the new connection. The result never exposes a
    /// thread ID even if the one-row canary response contains one.
    ///
    /// # Errors
    ///
    /// Fails closed with a static, redacted classification for every configuration, credential,
    /// connection, authentication, version, or protocol failure.
    pub async fn check(&self) -> Result<ExternalGateReport, ExternalGateError> {
        let admitted = self.admit_socket().await?;
        let mut socket = admitted.socket;
        let _ = timeout(EXTERNAL_WS_CLOSE_TIMEOUT, socket.close(None)).await;
        Ok(admitted.report)
    }

    pub(crate) async fn admit_socket(&self) -> Result<AdmittedExternalSocket, ExternalGateError> {
        let token = load_authentication(&self.authentication)?;
        let socket = timeout(EXTERNAL_GATE_TIMEOUT, self.admit_inner(&token))
            .await
            .map_err(|_| ExternalGateError::Timeout)??;
        Ok(AdmittedExternalSocket {
            socket,
            report: ExternalGateReport {
                endpoint_label: self.endpoint_label.clone(),
                codex_version: self.expected_version.clone(),
                capability_profile: self.capability_profile,
            },
            wire: self.wire,
        })
    }

    async fn admit_inner(&self, token: &SecretString) -> Result<ExternalSocket, ExternalGateError> {
        let mut request = self
            .endpoint
            .as_str()
            .into_client_request()
            .map_err(|_| ExternalGateError::InvalidEndpoint)?;
        let authorization_secret = SecretString::from(format!("Bearer {}", token.expose_secret()));
        let mut authorization = HeaderValue::from_str(authorization_secret.expose_secret())
            .map_err(|_| ExternalGateError::CredentialUnavailable)?;
        authorization.set_sensitive(true);
        request.headers_mut().insert(AUTHORIZATION, authorization);

        let ws_config = WebSocketConfig::default()
            .read_buffer_size(16 * 1024)
            .write_buffer_size(16 * 1024)
            .max_write_buffer_size(EXTERNAL_WS_MESSAGE_BYTES.saturating_mul(2))
            .max_message_size(Some(EXTERNAL_WS_MESSAGE_BYTES))
            .max_frame_size(Some(EXTERNAL_WS_MESSAGE_BYTES));
        let (mut socket, _) = connect_async_with_config(request, Some(ws_config), true)
            .await
            .map_err(classify_connect_error)?;

        let mut client_info =
            ClientInfo::new("lark_codex_bridge_external_gate", env!("CARGO_PKG_VERSION"));
        client_info.title = Some("Lark Codex Bridge external endpoint gate".to_owned());
        let mut initialize = InitializeParams::new(client_info);
        initialize.capabilities = Some(InitializeCapabilities {
            experimental_api: (self.capability_profile != ExternalCapabilityProfile::ObserveShared)
                .then_some(true),
            ..InitializeCapabilities::default()
        });
        let initialize = self
            .wire
            .initialize_params(&initialize)
            .map_err(|_| ExternalGateError::ProtocolViolation)?;
        send_message(
            &mut socket,
            OutboundMessage::Request {
                id: RequestId::Integer(1),
                method: "initialize".to_owned(),
                params: Some(initialize),
            },
        )
        .await?;
        let initialize = receive_response(&mut socket, 1, GateStage::Initialize).await?;
        let initialize = self
            .wire
            .initialize_response(initialize)
            .map_err(|_| ExternalGateError::ProtocolViolation)?;
        let found_version = exact_user_agent_version(&initialize.user_agent)
            .ok_or(ExternalGateError::VersionMismatch)?;
        if found_version != self.expected_version {
            return Err(ExternalGateError::VersionMismatch);
        }
        drop(initialize.codex_home);

        send_message(
            &mut socket,
            OutboundMessage::Notification {
                method: "initialized".to_owned(),
                params: None,
            },
        )
        .await?;

        let list = ThreadListParams {
            limit: Some(1),
            ..ThreadListParams::default()
        };
        let list = self
            .wire
            .thread_list_params(&list)
            .map_err(|_| ExternalGateError::UnsupportedCapabilityProfile)?;
        send_message(
            &mut socket,
            OutboundMessage::Request {
                id: RequestId::Integer(2),
                method: "thread/list".to_owned(),
                params: Some(list),
            },
        )
        .await?;
        let list = receive_response(&mut socket, 2, GateStage::Capability).await?;
        let list = self
            .wire
            .thread_list_response(list)
            .map_err(|_| ExternalGateError::MissingCapability)?;
        if list.data.len() > 1 {
            return Err(ExternalGateError::MissingCapability);
        }
        drop(list);

        Ok(socket)
    }
}

#[derive(Clone, Copy)]
enum GateStage {
    Initialize,
    Capability,
}

async fn send_message<S>(
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
    message: OutboundMessage,
) -> Result<(), ExternalGateError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut encoded = encode_line(&message).map_err(|_| ExternalGateError::ProtocolViolation)?;
    let Some(b'\n') = encoded.pop() else {
        return Err(ExternalGateError::ProtocolViolation);
    };
    let text = String::from_utf8(encoded).map_err(|_| ExternalGateError::ProtocolViolation)?;
    socket
        .send(Message::Text(text.into()))
        .await
        .map_err(|_| ExternalGateError::ConnectionFailed)
}

async fn receive_response<S>(
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
    expected_id: i64,
    stage: GateStage,
) -> Result<serde_json::Value, ExternalGateError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut total_bytes = 0_usize;
    for _ in 0..EXTERNAL_GATE_MAX_MESSAGES {
        let Some(frame) = socket.next().await else {
            return Err(ExternalGateError::ConnectionFailed);
        };
        let frame = frame.map_err(|_| ExternalGateError::ConnectionFailed)?;
        let text = match frame {
            Message::Text(text) => text,
            Message::Ping(_) | Message::Pong(_) => continue,
            _ => return Err(ExternalGateError::ProtocolViolation),
        };
        total_bytes = total_bytes.saturating_add(text.len());
        if text.len() > EXTERNAL_GATE_MESSAGE_BYTES || total_bytes > EXTERNAL_GATE_TOTAL_BYTES {
            return Err(ExternalGateError::ProtocolViolation);
        }
        match decode_line(text.as_bytes()).map_err(|_| ExternalGateError::ProtocolViolation)? {
            InboundMessage::Response {
                id: RequestId::Integer(id),
                result,
            } if id == expected_id => return Ok(result),
            InboundMessage::ErrorResponse {
                id: RequestId::Integer(id),
                ..
            } if id == expected_id => {
                return Err(match stage {
                    GateStage::Initialize => ExternalGateError::ProtocolViolation,
                    GateStage::Capability => ExternalGateError::MissingCapability,
                });
            }
            // Notifications emitted during startup are decoded under strict structural budgets and
            // then dropped. The one-shot gate does not project them, accept their identifiers, or
            // infer capabilities from them.
            InboundMessage::Notification { .. } => {}
            _ => return Err(ExternalGateError::ProtocolViolation),
        }
    }
    Err(ExternalGateError::ProtocolViolation)
}

fn classify_connect_error(error: WebSocketError) -> ExternalGateError {
    match error {
        WebSocketError::Http(response)
            if matches!(
                response.status(),
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
            ) =>
        {
            ExternalGateError::AuthenticationRejected
        }
        _ => ExternalGateError::ConnectionFailed,
    }
}

fn validate_endpoint(raw: &str) -> Result<Url, ExternalGateError> {
    if raw.is_empty()
        || raw.len() > MAX_EXTERNAL_ENDPOINT_BYTES
        || raw.trim() != raw
        || raw.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(ExternalGateError::InvalidEndpoint);
    }
    let endpoint = Url::parse(raw).map_err(|_| ExternalGateError::InvalidEndpoint)?;
    if endpoint.username() != ""
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
        || endpoint.host().is_none()
        || endpoint.port() == Some(0)
    {
        return Err(ExternalGateError::InvalidEndpoint);
    }
    match endpoint.scheme() {
        "wss" => {}
        "ws" if literal_loopback(&endpoint) => {}
        _ => return Err(ExternalGateError::InvalidEndpoint),
    }
    Ok(endpoint)
}

fn literal_loopback(endpoint: &Url) -> bool {
    match endpoint.host() {
        Some(Host::Ipv4(address)) => IpAddr::V4(address).is_loopback(),
        Some(Host::Ipv6(address)) => IpAddr::V6(address).is_loopback(),
        Some(Host::Domain(_)) | None => false,
    }
}

fn parse_exact_version(raw: &str) -> Result<Version, ExternalGateError> {
    if raw.is_empty() || raw.len() > 32 || raw.trim() != raw {
        return Err(ExternalGateError::InvalidExpectedVersion);
    }
    let version = Version::parse(raw).map_err(|_| ExternalGateError::InvalidExpectedVersion)?;
    if !version.pre.is_empty() || !version.build.is_empty() || version.to_string() != raw {
        return Err(ExternalGateError::InvalidExpectedVersion);
    }
    Ok(version)
}

fn validate_authentication_source(
    authentication: &ExternalAuthentication,
) -> Result<(), ExternalGateError> {
    match authentication {
        ExternalAuthentication::BearerTokenFile { path } => {
            if !valid_secret_path(path) {
                return Err(ExternalGateError::InvalidCredentialSource);
            }
            Ok(())
        }
    }
}

fn valid_secret_path(path: &Path) -> bool {
    path.is_absolute()
        && path.as_os_str().as_encoded_bytes().len() <= MAX_EXTERNAL_SECRET_PATH_BYTES
        && !path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
}

fn load_authentication(
    authentication: &ExternalAuthentication,
) -> Result<SecretString, ExternalGateError> {
    match authentication {
        ExternalAuthentication::BearerTokenFile { path } => load_token_file(path),
    }
}

fn load_token_file(path: &Path) -> Result<SecretString, ExternalGateError> {
    validate_authentication_source(&ExternalAuthentication::BearerTokenFile {
        path: path.to_path_buf(),
    })?;
    let before =
        fs::symlink_metadata(path).map_err(|_| ExternalGateError::CredentialUnavailable)?;
    if !before.file_type().is_file() || before.file_type().is_symlink() {
        return Err(ExternalGateError::CredentialUnavailable);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

        if before.mode() & 0o077 != 0 {
            return Err(ExternalGateError::CredentialUnavailable);
        }
        let mut options = OpenOptions::new();
        options.read(true).custom_flags(libc::O_NOFOLLOW);
        let mut file = options
            .open(path)
            .map_err(|_| ExternalGateError::CredentialUnavailable)?;
        let after = file
            .metadata()
            .map_err(|_| ExternalGateError::CredentialUnavailable)?;
        if !after.is_file() || before.dev() != after.dev() || before.ino() != after.ino() {
            return Err(ExternalGateError::CredentialUnavailable);
        }
        read_token(&mut file)
    }
    #[cfg(not(unix))]
    {
        let mut file = OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|_| ExternalGateError::CredentialUnavailable)?;
        let after = file
            .metadata()
            .map_err(|_| ExternalGateError::CredentialUnavailable)?;
        if !after.is_file() || before.len() != after.len() {
            return Err(ExternalGateError::CredentialUnavailable);
        }
        read_token(&mut file)
    }
}

fn read_token(file: &mut fs::File) -> Result<SecretString, ExternalGateError> {
    let mut bytes = Vec::new();
    file.take(u64::try_from(MAX_EXTERNAL_AUTH_TOKEN_BYTES + 1).unwrap_or(u64::MAX))
        .read_to_end(&mut bytes)
        .map_err(|_| ExternalGateError::CredentialUnavailable)?;
    if bytes.len() > MAX_EXTERNAL_AUTH_TOKEN_BYTES {
        return Err(ExternalGateError::CredentialUnavailable);
    }
    if bytes.ends_with(b"\r\n") {
        bytes.truncate(bytes.len() - 2);
    } else if bytes.ends_with(b"\n") {
        bytes.pop();
    }
    if bytes.len() < 32
        || bytes
            .iter()
            .any(|byte| !byte.is_ascii_graphic() || byte.is_ascii_whitespace())
    {
        return Err(ExternalGateError::CredentialUnavailable);
    }
    let token = String::from_utf8(bytes).map_err(|_| ExternalGateError::CredentialUnavailable)?;
    Ok(SecretString::from(token))
}

fn exact_user_agent_version(user_agent: &str) -> Option<Version> {
    if user_agent.len() > 1024 {
        return None;
    }
    let bytes = user_agent.as_bytes();
    let mut found = None;
    for slash in bytes
        .iter()
        .enumerate()
        .filter_map(|(index, byte)| (*byte == b'/').then_some(index))
    {
        let tail = user_agent.get(slash + 1..)?;
        let token_len = tail
            .bytes()
            .take_while(|byte| byte.is_ascii_digit() || *byte == b'.')
            .count();
        if token_len == 0 {
            continue;
        }
        let token = tail.get(..token_len)?;
        if tail
            .as_bytes()
            .get(token_len)
            .is_some_and(|byte| !byte.is_ascii_whitespace() && *byte != b'(')
        {
            continue;
        }
        let Ok(version) = Version::parse(token) else {
            continue;
        };
        if !version.pre.is_empty() || !version.build.is_empty() || version.to_string() != token {
            continue;
        }
        if found.replace(version).is_some() {
            return None;
        }
    }
    found
}

fn endpoint_label(
    endpoint: &Url,
    expected_version: &Version,
    profile: ExternalCapabilityProfile,
) -> EndpointLabel {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut hasher = Sha256::new();
    hasher.update(endpoint.as_str().as_bytes());
    hasher.update([0]);
    hasher.update(expected_version.to_string().as_bytes());
    hasher.update([0]);
    hasher.update(match profile {
        ExternalCapabilityProfile::ObserveShared => b"observe_shared".as_slice(),
        ExternalCapabilityProfile::ResumeShared => b"resume_shared".as_slice(),
        ExternalCapabilityProfile::MutateShared => b"mutate_shared".as_slice(),
        ExternalCapabilityProfile::QueueShared => b"queue_shared".as_slice(),
    });
    let digest = hasher.finalize();
    let mut encoded = String::with_capacity(68);
    encoded.push_str("ext-");
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    EndpointLabel(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_extraction_requires_one_exact_token() {
        assert_eq!(
            exact_user_agent_version("codex_cli_rs/0.149.0 (test)"),
            Some(Version::new(0, 149, 0))
        );
        assert!(exact_user_agent_version("codex_cli_rs/0.149.0 other/1.2.3").is_none());
        assert!(exact_user_agent_version("codex_cli_rs/0.149").is_none());
        assert!(exact_user_agent_version("codex_cli_rs/0.149.0suffix").is_none());
    }

    #[test]
    fn plaintext_policy_requires_a_literal_loopback_address() {
        assert!(validate_endpoint("ws://127.0.0.1:1234/app-server").is_ok());
        assert!(validate_endpoint("ws://[::1]:1234/app-server").is_ok());
        assert!(validate_endpoint("ws://localhost:1234/app-server").is_err());
        assert!(validate_endpoint("ws://192.0.2.10:1234/app-server").is_err());
        assert!(validate_endpoint("wss://codex.example.invalid/app-server").is_ok());
    }

    #[test]
    fn endpoint_label_retains_the_complete_sha256_identity() {
        let endpoint =
            validate_endpoint("ws://127.0.0.1:1234/app-server").expect("literal loopback endpoint");
        let label = endpoint_label(
            &endpoint,
            &Version::new(0, 149, 0),
            ExternalCapabilityProfile::ObserveShared,
        );

        assert_eq!(
            label.as_str(),
            "ext-51b10b2f97227b2b887fad3d07115e67f9e2c59449c0d10593dada049b87fa07"
        );
        assert_eq!(label.as_str().len(), 68);
    }
}
