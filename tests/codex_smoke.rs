//! Opt-in end-to-end smoke against the real installed Codex app-server.
//!
//! Runs only with `--ignored` and `CODEX_E2E=1`; without the environment gate
//! it reports a skip reason and exits successfully. It never fakes a pass: any
//! failure, including missing authentication, fails the test with an actionable
//! diagnostic.

use std::{path::Path, time::Duration};

use anyhow::{Context, Result, bail};
use lark_codex_bridge::codex::{
    client::{AppServerEvent, ThreadId, TurnId, TurnOutcome},
    process::CodexProcessConfig,
    supervisor::{AppServerSupervisor, SupervisorHandle, SupervisorState},
    types::{
        ApprovalPolicy, SandboxMode, ThreadItem, ThreadStartParams, TurnStartParams, TurnStatus,
        UserInput,
    },
};
use tokio::time::timeout;

const READY_TIMEOUT: Duration = Duration::from_secs(60);
const TURN_TIMEOUT: Duration = Duration::from_secs(180);

#[tokio::test]
#[ignore = "requires an authenticated Codex account"]
async fn real_codex_replies_pong_over_one_supervised_child() {
    if std::env::var("CODEX_E2E").ok().as_deref() != Some("1") {
        eprintln!(
            "skipping real Codex smoke: re-run with CODEX_E2E=1 and an authenticated `codex login`"
        );
        return;
    }

    let workspace = tempfile::tempdir().expect("temporary workspace");
    let mut handle = AppServerSupervisor::start(CodexProcessConfig::default())
        .await
        .expect("supervisor starts");
    let outcome = run_smoke(&mut handle, workspace.path()).await;
    handle
        .shutdown()
        .await
        .expect("supervisor shutdown must reap the app-server child");
    outcome.expect("real Codex smoke");
}

async fn run_smoke(handle: &mut SupervisorHandle, cwd: &Path) -> Result<()> {
    let client = loop {
        let state = timeout(READY_TIMEOUT, handle.changed())
            .await
            .context("timed out waiting for the app-server to become ready")?
            .context("supervisor stopped before becoming ready")?;
        match state {
            SupervisorState::Ready { .. } => {
                break handle.client().context("ready state without a client")?;
            }
            SupervisorState::Degraded { reason } => {
                bail!(
                    "app-server is degraded: {reason}; if authentication is missing, \
                     run `codex login` and retry with CODEX_E2E=1"
                );
            }
            _ => {}
        }
    };

    let thread = client
        .start_thread(ThreadStartParams {
            sandbox: Some(SandboxMode::ReadOnly),
            approval_policy: Some(ApprovalPolicy::Named("never".to_owned())),
            cwd: Some(cwd.to_path_buf()),
            ephemeral: Some(true),
            ..ThreadStartParams::default()
        })
        .await
        .map_err(|error| anyhow::anyhow!("{error}; {AUTH_HINT}"))?;

    let thread_id = ThreadId::from(thread.id.clone());
    let mut subscription = client
        .subscribe(thread_id.clone())
        .await
        .context("unable to subscribe to the smoke thread")?;
    let turn = client
        .start_turn(TurnStartParams::new(
            thread.id.clone(),
            vec![UserInput::Text {
                text: "Reply with exactly: pong".to_owned(),
                text_elements: Vec::new(),
            }],
        ))
        .await
        .map_err(|error| anyhow::anyhow!("{error}; {AUTH_HINT}"))?;
    let turn_id = TurnId::from(turn.id.clone());

    let outcome = timeout(TURN_TIMEOUT, wait_for_turn(&mut subscription, &turn_id))
        .await
        .context("timed out waiting for the authoritative turn completion")??;

    match outcome.status {
        TurnStatus::Completed => {}
        ref status => {
            bail!(
                "turn ended with status {status:?} instead of completed; \
                 if authentication is missing, run `codex login` and retry with CODEX_E2E=1"
            );
        }
    }
    let replied = outcome.completed_items.iter().any(|item| {
        matches!(item, ThreadItem::AgentMessage { text, .. } if text.to_lowercase().contains("pong"))
    });
    if !replied {
        bail!("no completed agent message contained `pong`");
    }

    drop(subscription);
    client
        .release_thread(&thread_id)
        .await
        .context("unable to release the smoke thread")?;
    Ok(())
}

const AUTH_HINT: &str =
    "the installed Codex must be authenticated; run `codex login` and retry with CODEX_E2E=1";

async fn wait_for_turn(
    subscription: &mut lark_codex_bridge::codex::client::ThreadSubscription,
    turn_id: &TurnId,
) -> Result<TurnOutcome> {
    loop {
        match subscription.recv().await {
            Some(AppServerEvent::TurnCompleted(outcome)) if outcome.turn_id == *turn_id => {
                return Ok(outcome);
            }
            Some(AppServerEvent::ConnectionClosed { .. }) | None => {
                bail!("connection closed before the turn completed; {AUTH_HINT}");
            }
            Some(_) => {}
        }
    }
}
