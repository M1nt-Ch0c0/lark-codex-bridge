mod fakecodex;

use std::{sync::Arc, time::Duration};

use lark_codex_bridge::codex::{
    process::{CodexProcessConfig, ProcessError},
    supervisor::{AppServerSupervisor, SupervisorError, SupervisorSettings, SupervisorState},
    types::ThreadStartParams,
};
use semver::Version;

use fakecodex::{FakeFactory, FakeOutcome, next_state, test_settings};

#[tokio::test]
async fn restart_increments_epoch_and_invalidates_the_previous_client() {
    let (first, first_control) = FakeFactory::ready();
    let (second, _second_control) = FakeFactory::ready();
    let factory = Arc::new(FakeFactory::new([first, second]));
    let mut handle = AppServerSupervisor::start_with_factory(
        CodexProcessConfig::default(),
        factory.clone(),
        test_settings(),
    )
    .await
    .expect("supervisor starts");

    assert!(matches!(
        next_state(&mut handle).await,
        SupervisorState::Ready { epoch: 1, .. }
    ));
    let stale = handle.client().expect("ready client");
    assert_eq!(stale.epoch().get(), 1);
    first_control.unexpected_exit();

    assert!(matches!(
        next_state(&mut handle).await,
        SupervisorState::Backoff { epoch: 2, attempt: 1, delay } if delay.is_zero()
    ));
    assert!(matches!(
        next_state(&mut handle).await,
        SupervisorState::Starting { epoch: 2 }
    ));
    assert!(matches!(
        next_state(&mut handle).await,
        SupervisorState::Ready { epoch: 2, .. }
    ));
    assert!(
        stale
            .start_thread(ThreadStartParams::default())
            .await
            .is_err(),
        "old client must fail after its epoch exits"
    );
    assert_eq!(
        handle.client().expect("replacement client").epoch().get(),
        2
    );
    assert_eq!(factory.spawn_count(), 2);
    handle.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn permanent_version_failure_degrades_without_retrying() {
    let factory = Arc::new(FakeFactory::new([FakeOutcome::Error(
        ProcessError::UnsupportedVersion {
            found: Version::new(0, 145, 0),
        },
    )]));
    let mut handle = AppServerSupervisor::start_with_factory(
        CodexProcessConfig::default(),
        factory.clone(),
        test_settings(),
    )
    .await
    .expect("supervisor task starts");

    assert!(matches!(
        next_state(&mut handle).await,
        SupervisorState::Degraded { .. }
    ));
    assert_eq!(factory.spawn_count(), 1);
    assert!(matches!(handle.client(), Err(SupervisorError::NotReady)));
    handle.shutdown().await.expect("shutdown");
}

#[test]
fn retry_schedule_is_capped_and_jittered_deterministically() {
    let delays = (1..=8)
        .map(|attempt| AppServerSupervisor::retry_delay(7, attempt))
        .collect::<Vec<_>>();
    assert_eq!(delays.len(), 8);
    assert!(delays[0] >= Duration::from_millis(375));
    assert!(delays[0] <= Duration::from_millis(625));
    assert!(delays[5] <= Duration::from_secs(20));
    assert!(delays[6] <= Duration::from_secs(30));
    assert!(delays[7] <= Duration::from_secs(30));
    assert_ne!(delays[0], AppServerSupervisor::retry_delay(8, 1));
}

#[tokio::test]
async fn shutdown_uses_the_configured_grace_period_before_process_termination() {
    let (outcome, control) = FakeFactory::ready();
    let factory = Arc::new(FakeFactory::new([outcome]));
    let mut handle = AppServerSupervisor::start_with_factory(
        CodexProcessConfig::default(),
        factory,
        SupervisorSettings::new(Duration::from_millis(7), |_, _| Duration::ZERO),
    )
    .await
    .expect("supervisor starts");
    assert!(matches!(
        next_state(&mut handle).await,
        SupervisorState::Ready { .. }
    ));
    handle.shutdown().await.expect("shutdown");
    assert_eq!(control.terminate_calls(), vec![Duration::from_millis(7)]);
}
