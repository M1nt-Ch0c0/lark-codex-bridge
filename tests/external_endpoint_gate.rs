use std::{path::Path, sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use lark_codex_bridge::{
    codex::external::{
        CodexBackendConfig, ExternalAuthentication, ExternalCapabilityProfile,
        ExternalEndpointConfig, ExternalEndpointGate, ExternalGateError,
    },
    config::BridgeConfig,
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        Message,
        handshake::server::{ErrorResponse, Request, Response},
        http::StatusCode,
    },
};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);
const TOKEN_ONE: &str = "token-one-0123456789abcdef0123456789abcdef";
const TOKEN_TWO: &str = "token-two-0123456789abcdef0123456789abcdef";
const RAW_SENTINEL: &str = "RAW_RPC_SECRET_SENTINEL";

#[derive(Clone, Copy)]
enum Behavior {
    Success,
    WrongVersion,
    MissingCapability,
    RawInitializeError,
}

struct FakeServer {
    endpoint: String,
    task: JoinHandle<()>,
}

impl FakeServer {
    #[allow(clippy::result_large_err)] // Tungstenite fixes the handshake callback's error type.
    async fn start(expected_tokens: Vec<&str>, behavior: Behavior) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("fake listener binds");
        let address = listener.local_addr().expect("fake listener address");
        let tokens = expected_tokens
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        let task = tokio::spawn(async move {
            for expected_token in tokens {
                let (stream, _) = listener.accept().await.expect("fake connection accepts");
                let expected = Arc::new(format!("Bearer {expected_token}"));
                let observed = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let callback_expected = Arc::clone(&expected);
                let callback_observed = Arc::clone(&observed);
                let accepted =
                    accept_hdr_async(stream, move |request: &Request, response: Response| {
                        let matches = request
                            .headers()
                            .get("authorization")
                            .and_then(|value| value.to_str().ok())
                            .is_some_and(|value| value == callback_expected.as_str());
                        callback_observed.store(matches, std::sync::atomic::Ordering::Release);
                        if matches {
                            Ok(response)
                        } else {
                            let mut rejected = ErrorResponse::new(Some(RAW_SENTINEL.to_owned()));
                            *rejected.status_mut() = StatusCode::UNAUTHORIZED;
                            Err(rejected)
                        }
                    })
                    .await;
                if !observed.load(std::sync::atomic::Ordering::Acquire) {
                    assert!(accepted.is_err(), "bad auth must reject the upgrade");
                    continue;
                }
                let mut socket = accepted.expect("matching auth upgrades");
                serve_gate(&mut socket, behavior).await;
            }
        });
        Self {
            endpoint: format!("ws://{address}/app-server"),
            task,
        }
    }

    async fn finish(self) {
        timeout(TEST_TIMEOUT, self.task)
            .await
            .expect("fake server finishes before timeout")
            .expect("fake server task succeeds");
    }
}

async fn serve_gate(
    socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    behavior: Behavior,
) {
    let initialize = recv_json(socket).await;
    assert_eq!(initialize["id"], 1);
    assert_eq!(initialize["method"], "initialize");
    assert_eq!(
        initialize["params"]["clientInfo"]["name"],
        "lark_codex_bridge_external_gate"
    );
    assert_eq!(
        initialize["params"]["clientInfo"]["title"],
        "Lark Codex Bridge external endpoint gate"
    );
    assert!(initialize["params"].get("capabilities").is_some());

    if matches!(behavior, Behavior::RawInitializeError) {
        send_json(
            socket,
            json!({
                "id": 1,
                "error": {"code": -32000, "message": RAW_SENTINEL, "data": RAW_SENTINEL}
            }),
        )
        .await;
        return;
    }

    let user_agent = if matches!(behavior, Behavior::WrongVersion) {
        "codex_cli_rs/0.148.0 (fake)"
    } else {
        "codex_cli_rs/0.149.0 (fake)"
    };
    send_json(
        socket,
        json!({
            "id": 1,
            "result": {
                "codexHome": absolute_fake_home(),
                "platformFamily": "test",
                "platformOs": "test",
                "userAgent": user_agent
            }
        }),
    )
    .await;
    if matches!(behavior, Behavior::WrongVersion) {
        return;
    }

    let initialized = recv_json(socket).await;
    assert_eq!(initialized, json!({"method": "initialized"}));
    let list = recv_json(socket).await;
    assert_eq!(list["id"], 2);
    assert_eq!(list["method"], "thread/list");
    assert_eq!(list["params"]["limit"], 1);
    if matches!(behavior, Behavior::MissingCapability) {
        send_json(
            socket,
            json!({
                "id": 2,
                "error": {"code": -32601, "message": RAW_SENTINEL}
            }),
        )
        .await;
    } else {
        send_json(socket, json!({"id": 2, "result": {"data": []}})).await;
    }
}

fn absolute_fake_home() -> String {
    std::env::temp_dir()
        .join("external-gate-fake-codex-home")
        .display()
        .to_string()
}

async fn recv_json(
    socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
) -> Value {
    let message = timeout(TEST_TIMEOUT, socket.next())
        .await
        .expect("client message arrives before timeout")
        .expect("client socket stays open")
        .expect("client frame is valid");
    let Message::Text(text) = message else {
        panic!("gate sends only text protocol frames");
    };
    serde_json::from_str(&text).expect("gate text is JSON")
}

async fn send_json(
    socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    value: Value,
) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .expect("fake response sends");
}

fn write_token(path: &Path, token: &str) {
    std::fs::write(path, format!("{token}\n")).expect("token file writes");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = std::fs::metadata(path)
            .expect("token metadata reads")
            .permissions();
        permissions.set_mode(0o600);
        std::fs::set_permissions(path, permissions).expect("token permissions set");
    }
}

fn gate(endpoint: &str, token_path: &Path) -> ExternalEndpointGate {
    ExternalEndpointGate::new(ExternalEndpointConfig {
        endpoint: endpoint.to_owned(),
        expected_codex_version: "0.149.0".to_owned(),
        capability_profile: ExternalCapabilityProfile::ObserveShared,
        authentication: ExternalAuthentication::BearerTokenFile {
            path: token_path.to_path_buf(),
        },
    })
    .expect("test gate config is valid")
}

#[test]
fn tagged_backend_modes_reject_cross_mode_fields_during_deserialization() {
    let spawned_with_endpoint = r#"
owners = ["ou_owner"]

[codex.backend]
mode = "spawned_stdio"
endpoint = "wss://endpoint.invalid/app-server"
"#;
    let external_with_binary = r#"
owners = ["ou_owner"]

[codex.backend]
mode = "external_endpoint"
endpoint = "wss://endpoint.invalid/app-server"
expected_codex_version = "0.149.0"
capability_profile = "observe_shared"
binary = "codex"

[codex.backend.authentication]
source = "bearer_token_file"
path = "/private/token"
"#;
    assert!(toml::from_str::<BridgeConfig>(spawned_with_endpoint).is_err());
    assert!(toml::from_str::<BridgeConfig>(external_with_binary).is_err());
}

#[test]
fn endpoint_policy_is_exact_and_external_mode_has_no_process_fallback() {
    let backend = CodexBackendConfig::ExternalEndpoint {
        endpoint: "ws://localhost:8123/app-server".to_owned(),
        expected_codex_version: "0.149.0".to_owned(),
        capability_profile: ExternalCapabilityProfile::ObserveShared,
        authentication: ExternalAuthentication::BearerTokenFile {
            path: std::env::temp_dir().join("external-token"),
        },
    };
    assert!(backend.spawned_process_config().is_none());
    assert!(matches!(
        backend.external_gate(),
        Err(ExternalGateError::InvalidEndpoint)
    ));
    assert!(backend.spawned_process_config().is_none());

    for endpoint in [
        "ws://192.0.2.10:8123/app-server",
        "ws://127.0.0.1:8123/app-server?token=secret",
        "ws://user:secret@127.0.0.1:8123/app-server",
        "unix:///private/socket",
    ] {
        let configured = ExternalEndpointConfig {
            endpoint: endpoint.to_owned(),
            expected_codex_version: "0.149.0".to_owned(),
            capability_profile: ExternalCapabilityProfile::ObserveShared,
            authentication: ExternalAuthentication::BearerTokenFile {
                path: std::env::temp_dir().join("external-token"),
            },
        };
        assert!(matches!(
            ExternalEndpointGate::new(configured),
            Err(ExternalGateError::InvalidEndpoint)
        ));
    }
}

#[test]
fn exact_write_profiles_are_explicit_and_version_gated() {
    let token_path = std::env::temp_dir().join("external-write-profile-token");
    let mut labels = Vec::new();
    for profile in [
        ExternalCapabilityProfile::MutateShared,
        ExternalCapabilityProfile::QueueShared,
    ] {
        let gate = ExternalEndpointGate::new(ExternalEndpointConfig {
            endpoint: "ws://127.0.0.1:8123/app-server".to_owned(),
            expected_codex_version: "0.149.0".to_owned(),
            capability_profile: profile,
            authentication: ExternalAuthentication::BearerTokenFile {
                path: token_path.clone(),
            },
        })
        .expect("exact write profile is promoted");
        labels.push(gate.endpoint_label().as_str().to_owned());
    }
    assert_ne!(labels[0], labels[1]);

    assert!(matches!(
        ExternalEndpointGate::new(ExternalEndpointConfig {
            endpoint: "ws://127.0.0.1:8123/app-server".to_owned(),
            expected_codex_version: "0.146.0".to_owned(),
            capability_profile: ExternalCapabilityProfile::QueueShared,
            authentication: ExternalAuthentication::BearerTokenFile { path: token_path },
        }),
        Err(ExternalGateError::UnsupportedCapabilityProfile)
    ));
}

#[test]
fn unvalidated_external_configuration_debug_is_already_redacted() {
    let endpoint = "wss://user:ENDPOINT_SECRET@host.invalid/private?bearer=QUERY_SECRET";
    let token_path = Path::new("/private/CREDENTIAL_PATH_SECRET");
    let config = ExternalEndpointConfig {
        endpoint: endpoint.to_owned(),
        expected_codex_version: "0.149.0".to_owned(),
        capability_profile: ExternalCapabilityProfile::ObserveShared,
        authentication: ExternalAuthentication::BearerTokenFile {
            path: token_path.to_path_buf(),
        },
    };
    let backend = CodexBackendConfig::ExternalEndpoint {
        endpoint: endpoint.to_owned(),
        expected_codex_version: "0.149.0".to_owned(),
        capability_profile: ExternalCapabilityProfile::ObserveShared,
        authentication: ExternalAuthentication::BearerTokenFile {
            path: token_path.to_path_buf(),
        },
    };
    let rendered = format!("{config:?} {backend:?}");
    for sentinel in [
        endpoint,
        "ENDPOINT_SECRET",
        "QUERY_SECRET",
        "CREDENTIAL_PATH_SECRET",
    ] {
        assert!(!rendered.contains(sentinel));
    }
}

#[tokio::test]
async fn authenticated_initialize_version_and_capability_gate_succeeds() {
    let directory = tempfile::tempdir().expect("temporary credential directory");
    let token_path = directory.path().join("bearer");
    write_token(&token_path, TOKEN_ONE);
    let server = FakeServer::start(vec![TOKEN_ONE], Behavior::Success).await;
    let endpoint = server.endpoint.clone();
    let report = gate(&endpoint, &token_path)
        .check()
        .await
        .expect("all gates pass");

    assert_eq!(report.codex_version.to_string(), "0.149.0");
    assert_eq!(
        report.capability_profile,
        ExternalCapabilityProfile::ObserveShared
    );
    assert!(report.endpoint_label.as_str().starts_with("ext-"));
    assert!(!format!("{report:?}").contains(&endpoint));
    server.finish().await;
}

#[tokio::test]
async fn authentication_rejection_is_permanent_and_redacted() {
    let directory = tempfile::tempdir().expect("temporary credential directory");
    let token_path = directory.path().join("credential-path-sentinel");
    write_token(&token_path, TOKEN_TWO);
    let server = FakeServer::start(vec![TOKEN_ONE], Behavior::Success).await;
    let endpoint = server.endpoint.clone();
    let configured = gate(&endpoint, &token_path);

    let error = configured
        .check()
        .await
        .expect_err("invalid bearer is rejected");
    assert_eq!(error, ExternalGateError::AuthenticationRejected);
    let rendered = format!("{configured:?} {error:?} {error}");
    for secret in [
        endpoint.as_str(),
        token_path.to_string_lossy().as_ref(),
        TOKEN_ONE,
        TOKEN_TWO,
        RAW_SENTINEL,
    ] {
        assert!(!rendered.contains(secret));
    }
    server.finish().await;
}

#[tokio::test]
async fn missing_credential_fails_before_any_endpoint_connection() {
    let directory = tempfile::tempdir().expect("temporary credential directory");
    let missing = directory.path().join("missing-bearer");
    let server = FakeServer::start(vec![], Behavior::Success).await;
    let error = gate(&server.endpoint, &missing)
        .check()
        .await
        .expect_err("missing credential fails closed");
    assert_eq!(error, ExternalGateError::CredentialUnavailable);
    server.finish().await;
}

#[tokio::test]
async fn token_rotation_is_an_explicit_new_connection_and_never_falls_back() {
    let directory = tempfile::tempdir().expect("temporary credential directory");
    let token_path = directory.path().join("bearer");
    write_token(&token_path, TOKEN_ONE);
    let server = FakeServer::start(vec![TOKEN_ONE, TOKEN_TWO], Behavior::Success).await;
    let configured = gate(&server.endpoint, &token_path);

    configured.check().await.expect("initial credential passes");
    write_token(&token_path, TOKEN_TWO);
    configured.check().await.expect("rotated credential passes");
    server.finish().await;
}

#[tokio::test]
async fn version_and_capability_failures_are_closed_and_redacted() {
    for (behavior, expected) in [
        (Behavior::WrongVersion, ExternalGateError::VersionMismatch),
        (
            Behavior::MissingCapability,
            ExternalGateError::MissingCapability,
        ),
        (
            Behavior::RawInitializeError,
            ExternalGateError::ProtocolViolation,
        ),
    ] {
        let directory = tempfile::tempdir().expect("temporary credential directory");
        let token_path = directory.path().join("bearer");
        write_token(&token_path, TOKEN_ONE);
        let server = FakeServer::start(vec![TOKEN_ONE], behavior).await;
        let configured = gate(&server.endpoint, &token_path);
        let error = configured.check().await.expect_err("gate must fail closed");
        assert_eq!(error, expected);
        let rendered = format!("{configured:?} {error:?} {error}");
        assert!(!rendered.contains(&server.endpoint));
        assert!(!rendered.contains(TOKEN_ONE));
        assert!(!rendered.contains(RAW_SENTINEL));
        server.finish().await;
    }
}

#[cfg(unix)]
#[tokio::test]
async fn credential_source_rejects_symlinks_and_group_readable_files() {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let directory = tempfile::tempdir().expect("temporary credential directory");
    let token_path = directory.path().join("bearer");
    write_token(&token_path, TOKEN_ONE);
    let symlink_path = directory.path().join("bearer-link");
    symlink(&token_path, &symlink_path).expect("credential symlink creates");
    let server = FakeServer::start(vec![], Behavior::Success).await;
    let error = gate(&server.endpoint, &symlink_path)
        .check()
        .await
        .expect_err("credential symlink fails closed");
    assert_eq!(error, ExternalGateError::CredentialUnavailable);
    server.finish().await;

    let mut permissions = std::fs::metadata(&token_path)
        .expect("token metadata reads")
        .permissions();
    permissions.set_mode(0o640);
    std::fs::set_permissions(&token_path, permissions).expect("token permissions set");
    let server = FakeServer::start(vec![], Behavior::Success).await;
    let error = gate(&server.endpoint, &token_path)
        .check()
        .await
        .expect_err("group-readable credential fails closed");
    assert_eq!(error, ExternalGateError::CredentialUnavailable);
    server.finish().await;
}
