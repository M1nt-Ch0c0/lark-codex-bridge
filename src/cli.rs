use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use tokio::process::Command as TokioCommand;

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
    /// Check that the configured Codex binary can be executed.
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

async fn probe_codex(binary: PathBuf) -> Result<()> {
    let output = TokioCommand::new(&binary)
        .arg("--version")
        .output()
        .await
        .with_context(|| format!("unable to run Codex binary {}", binary.display()))?;

    if !output.status.success() {
        bail!("Codex version probe failed with status {}", output.status);
    }

    let version = String::from_utf8(output.stdout).context("Codex version output is not UTF-8")?;
    println!("{}", version.trim());
    Ok(())
}
