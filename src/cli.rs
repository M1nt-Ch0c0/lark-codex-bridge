use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use serde::Serialize;
use tokio::time::timeout;

use crate::{
    codex::{
        process::CodexProcessConfig,
        supervisor::{AppServerSupervisor, SupervisorHandle, SupervisorState},
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
}

#[derive(Debug, Subcommand)]
pub enum CodexCommand {
    /// Spawn the app-server, run the initialize handshake, and print a
    /// sanitized JSON summary of the supported installation.
    Probe {
        #[arg(long, default_value = "codex")]
        binary: PathBuf,
    },
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
