use std::{fmt, path::PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand, ValueEnum};
use secrecy::SecretString;
use serde::Serialize;
use tokio::time::{sleep, timeout};

use crate::{
    codex::{
        process::CodexProcessConfig,
        supervisor::{AppServerSupervisor, SupervisorHandle, SupervisorState},
    },
    lark::{
        api::{BotInfo, LarkApi},
        config::{LarkEndpoints, TenantBrand},
        credentials::{CredentialStore, FileCredentialStore, LarkCredentials, load_credentials},
        error::LarkError,
        http::LarkHttp,
        register::{RegistrationFlow, RegistrationOutcome, validate_credentials},
        token::TenantTokenProvider,
        transport::LarkTransport,
    },
    limits::PROBE_TIMEOUT,
};

#[derive(Debug, Parser)]
#[command(name = "lark-codex-bridge", version, about)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Inspect the local Codex installation.
    Codex {
        #[command(subcommand)]
        command: CodexCommand,
    },
    /// Onboard and inspect Feishu/Lark app credentials.
    Lark {
        #[command(subcommand)]
        command: LarkCommand,
    },
}

#[derive(Subcommand)]
pub enum CodexCommand {
    /// Spawn the app-server, run the initialize handshake, and print a
    /// sanitized JSON summary of the supported installation.
    Probe {
        #[arg(long, default_value = "codex")]
        binary: PathBuf,
    },
}

impl fmt::Debug for CodexCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Probe { binary } => formatter
                .debug_struct("Probe")
                .field("binary_bytes", &binary.as_os_str().len())
                .finish(),
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum LarkCommand {
    /// Manage Feishu/Lark app credentials.
    Auth {
        #[command(subcommand)]
        command: LarkAuthCommand,
    },
    /// Exchange a tenant token, fetch bot info, pull the WebSocket endpoint,
    /// and verify one ping/pong round trip; prints a sanitized JSON summary.
    Probe,
}

#[derive(Subcommand)]
pub enum LarkAuthCommand {
    /// Register a new `PersonalAgent` app via the QR device flow, or validate
    /// and store existing app credentials.
    Register {
        /// Existing app ID; requires --tenant, plus --app-secret or the
        /// `LARK_APP_SECRET` environment variable.
        #[arg(long)]
        app_id: Option<String>,
        /// Existing app secret; visible in the process list and shell history,
        /// so prefer the `LARK_APP_SECRET` environment variable instead.
        #[arg(long)]
        app_secret: Option<String>,
        /// Tenant of the existing app; requires --app-id and --app-secret.
        #[arg(long)]
        tenant: Option<TenantArg>,
    },
    /// Validate stored credentials and print a sanitized identity summary.
    Check,
}

impl fmt::Debug for LarkAuthCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Register {
                app_id,
                app_secret,
                tenant,
            } => formatter
                .debug_struct("Register")
                .field("app_id_configured", &app_id.is_some())
                .field("app_secret_configured", &app_secret.is_some())
                .field("tenant", tenant)
                .finish(),
            Self::Check => formatter.write_str("Check"),
        }
    }
}

/// CLI spelling of the tenant brand.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum TenantArg {
    /// Feishu (`feishu.cn`).
    Feishu,
    /// Lark international (`larksuite.com`).
    Lark,
}

impl From<TenantArg> for TenantBrand {
    fn from(arg: TenantArg) -> Self {
        match arg {
            TenantArg::Feishu => Self::Feishu,
            TenantArg::Lark => Self::Lark,
        }
    }
}

/// Parses the process arguments and executes the selected command.
///
/// # Errors
///
/// Returns an error when the selected command cannot complete successfully.
pub async fn run() -> Result<()> {
    run_with(Cli::parse()).await
}

/// Executes an already parsed command.
///
/// # Errors
///
/// Returns an error when the selected command cannot complete successfully.
pub async fn run_with(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Codex {
            command: CodexCommand::Probe { binary },
        } => probe_codex(binary).await,
        Command::Lark {
            command:
                LarkCommand::Auth {
                    command:
                        LarkAuthCommand::Register {
                            app_id,
                            app_secret,
                            tenant,
                        },
                },
        } => lark_auth_register(app_id, app_secret, tenant).await,
        Command::Lark {
            command:
                LarkCommand::Auth {
                    command: LarkAuthCommand::Check,
                },
        } => lark_auth_check().await,
        Command::Lark {
            command: LarkCommand::Probe,
        } => lark_probe().await,
    }
}

/// The only fields `codex probe` may ever print: no Codex home, account
/// identity, tokens, environment, or raw responses.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProbeReport {
    supported_version: String,
    initialize_user_agent: String,
    platform_family: String,
    platform_os: String,
    epoch: u64,
}

async fn probe_codex(binary: PathBuf) -> Result<()> {
    let config = CodexProcessConfig {
        binary,
        codex_home: None,
    };
    let mut handle = AppServerSupervisor::start(config)
        .await
        .context("unable to start the Codex supervisor")?;

    let probe = timeout(PROBE_TIMEOUT, wait_for_probe(&mut handle)).await;
    // Always stop the supervisor first so no app-server child outlives the probe.
    handle
        .shutdown()
        .await
        .context("unable to stop the Codex supervisor")?;
    let state = probe.context("Codex probe timed out waiting for the app-server handshake")??;

    match state {
        SupervisorState::Ready {
            epoch,
            version,
            peer,
        } => {
            let report = ProbeReport {
                supported_version: version.to_string(),
                initialize_user_agent: peer.user_agent,
                platform_family: peer.platform_family,
                platform_os: peer.platform_os,
                epoch,
            };
            let line = serde_json::to_string(&report).context("unable to encode probe report")?;
            println!("{line}");
            Ok(())
        }
        SupervisorState::Degraded { reason } => bail!("{reason}"),
        _ => bail!("Codex app-server stopped before completing the probe"),
    }
}

async fn wait_for_probe(handle: &mut SupervisorHandle) -> Result<SupervisorState> {
    loop {
        let state = handle
            .changed()
            .await
            .context("Codex supervisor stopped during the probe")?;
        match state {
            SupervisorState::Ready { .. } | SupervisorState::Degraded { .. } => return Ok(state),
            _ => {}
        }
    }
}

/// The only fields `lark auth` commands may ever print: no app secret, no
/// tenant token, no raw responses.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LarkAuthReport {
    tenant: String,
    bot_name: Option<String>,
    bot_open_id: Option<String>,
}

async fn lark_auth_register(
    app_id: Option<String>,
    app_secret: Option<String>,
    tenant: Option<TenantArg>,
) -> Result<()> {
    let creds = match (app_id, app_secret, tenant) {
        (Some(app_id), Some(app_secret), Some(tenant)) => {
            LarkCredentials::new(app_id, SecretString::from(app_secret), tenant.into())
        }
        (Some(app_id), None, Some(tenant)) => {
            // Avoid putting the secret on the command line: read it from the
            // environment when only --app-id/--tenant are given.
            let secret = std::env::var("LARK_APP_SECRET").ok().and_then(|value| {
                (!value.is_empty()).then_some(value)
            }).ok_or_else(|| {
                anyhow!("--app-id/--tenant given without --app-secret; set LARK_APP_SECRET to supply the secret without exposing it in the process list")
            })?;
            LarkCredentials::new(app_id, SecretString::from(secret), tenant.into())
        }
        (None, None, None) => run_device_flow().await?,
        _ => bail!(
            "--app-id and --tenant must be given together, optionally with --app-secret, or none at all"
        ),
    };
    let info = validate_new_credentials(&creds).await?;
    let store = FileCredentialStore::at_default()
        .context("unable to locate the credentials file directory")?;
    store
        .save(&creds)
        .map_err(|error| anyhow!("unable to store the credentials: {error}"))?;
    print_auth_report(creds.tenant, &info)
}

async fn lark_auth_check() -> Result<()> {
    let creds = load_credentials()
        .context("unable to load stored credentials")?
        .ok_or_else(|| {
            anyhow!(
                "no Lark credentials found; run `lark auth register` or set LARK_APP_ID, LARK_APP_SECRET, and LARK_TENANT"
            )
        })?;
    let info = validate_new_credentials(&creds).await?;
    print_auth_report(creds.tenant, &info)
}

async fn validate_new_credentials(creds: &LarkCredentials) -> Result<BotInfo> {
    let http = LarkHttp::new(LarkEndpoints::for_tenant(creds.tenant))
        .context("unable to build the Lark HTTP client")?;
    validate_credentials(http, creds.clone())
        .await
        .map_err(|error| {
            anyhow!(
                "unable to validate the app credentials: {error}; verify the app ID, app secret, and tenant"
            )
        })
}

async fn run_device_flow() -> Result<LarkCredentials> {
    // Begin always targets the Feishu accounts host; the flow itself switches
    // to the Lark accounts host when the authorizing tenant is Lark-branded.
    let http = LarkHttp::new(LarkEndpoints::for_tenant(TenantBrand::Feishu))
        .context("unable to build the Lark HTTP client")?;
    let mut flow = RegistrationFlow::new(http, None);
    let challenge = flow
        .begin()
        .await
        .context("unable to start app registration")?;
    eprintln!("Open this URL in a browser to authorize the bridge app:");
    eprintln!("{}", challenge.url);
    loop {
        sleep(flow.interval()).await;
        match flow.poll_once().await {
            Ok(RegistrationOutcome::Pending) => {}
            Ok(RegistrationOutcome::SlowDown { new_interval }) => {
                eprintln!("registration server asked to slow down; polling every {new_interval}s");
            }
            Ok(RegistrationOutcome::Credentials { creds, .. }) => return Ok(creds),
            Err(error) => return Err(error).context("app registration failed"),
        }
    }
}

fn print_auth_report(tenant: TenantBrand, info: &BotInfo) -> Result<()> {
    let report = LarkAuthReport {
        tenant: tenant.to_string(),
        bot_name: info.app_name.clone(),
        bot_open_id: info.open_id.clone(),
    };
    let line = serde_json::to_string(&report).context("unable to encode auth report")?;
    println!("{line}");
    Ok(())
}

/// The only fields `lark probe` may ever print: no app secret, no tenant
/// token, and never the full endpoint URL (host only).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LarkProbeReport {
    tenant: String,
    bot_name: Option<String>,
    bot_open_id: Option<String>,
    endpoint_host: String,
    endpoint_reachable: bool,
    ping_interval_secs: u64,
    elapsed_ms: u64,
}

async fn lark_probe() -> Result<()> {
    let creds = load_credentials()
        .context("unable to load stored credentials")?
        .ok_or_else(|| {
            anyhow!(
                "no Lark credentials found; run `lark auth register` or set LARK_APP_ID, LARK_APP_SECRET, and LARK_TENANT"
            )
        })?;
    let http = LarkHttp::new(LarkEndpoints::for_tenant(creds.tenant))
        .context("unable to build the Lark HTTP client")?;
    let tokens = TenantTokenProvider::new(http.clone(), creds.clone());
    // Exchange the token explicitly first so a permanent auth failure gets an
    // actionable diagnostic instead of a generic probe error.
    tokens.token().await.map_err(|error| match error {
        LarkError::PermanentAuth { .. } => anyhow!(
            "Lark authentication failed permanently; verify the app ID, app secret, and tenant (or re-run `lark auth register`)"
        ),
        other => anyhow!("unable to exchange a Lark tenant token: {other}"),
    })?;
    let api = LarkApi::new(http.clone(), tokens);
    let info = api
        .bot_info()
        .await
        .map_err(|error| anyhow!("unable to fetch the Lark bot info: {error}"))?;
    let outcome = LarkTransport::probe(&http, &creds).await.map_err(|error| match error {
        LarkError::PermanentAuth { code, .. } => anyhow!(
            "Lark WebSocket endpoint rejected the credentials permanently (code {code:?}); verify the app ID, app secret, and tenant"
        ),
        other => anyhow!(
            "unable to complete the Lark WebSocket probe: {other}; check network reachability of the tenant endpoints and retry"
        ),
    })?;
    let report = LarkProbeReport {
        tenant: creds.tenant.to_string(),
        bot_name: info.app_name,
        bot_open_id: info.open_id,
        endpoint_host: outcome.endpoint_host,
        endpoint_reachable: true,
        ping_interval_secs: outcome.ping_interval.as_secs(),
        elapsed_ms: u64::try_from(outcome.elapsed.as_millis()).unwrap_or(u64::MAX),
    };
    let line = serde_json::to_string(&report).context("unable to encode probe report")?;
    println!("{line}");
    Ok(())
}
