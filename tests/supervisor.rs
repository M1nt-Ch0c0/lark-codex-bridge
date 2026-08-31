mod fakecodex;

use std::{io, sync::Arc, time::Duration};

use lark_codex_bridge::codex::{
    process::{CodexProcessConfig, ProcessError},
    supervisor::{
        AppServerSupervisor, OneShotSupervisorHandle, SupervisorError, SupervisorSettings,
        SupervisorState,
    },
    types::ThreadStartParams,
    wire::SUPPORTED_CODEX_VERSIONS,
};
use semver::Version;

use fakecodex::{FakeFactory, FakeOutcome, next_state, test_settings};

async fn next_one_shot_state(handle: &mut OneShotSupervisorHandle) -> SupervisorState {
    tokio::time::timeout(Duration::from_secs(3), handle.changed())
        .await
        .expect("one-shot state transition timeout")
        .expect("one-shot state watch should stay available")
}

struct PanickingFactory;

impl lark_codex_bridge::codex::supervisor::ProcessFactory for PanickingFactory {
    fn spawn<'a>(
        &'a self,
        _config: &'a CodexProcessConfig,
    ) -> lark_codex_bridge::codex::supervisor::SpawnFuture<'a> {
        panic!("injected process factory panic before its first result")
    }
}

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
    let shared_profile = handle
        .profile_identity()
        .expect("ready shared profile identity");
    assert_eq!(format!("{shared_profile:?}"), "ProfileIdentity([REDACTED])");
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
async fn cleanup_failure_fences_replacement_epoch() {
    let (first, first_control) = FakeFactory::ready_with_terminate_error(io::ErrorKind::TimedOut);
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
    first_control.unexpected_exit();

    let SupervisorState::Degraded { reason } = next_state(&mut handle).await else {
        panic!("failed process cleanup must fence replacement");
    };
    assert_eq!(
        reason,
        "Codex process cleanup failed; replacement is fenced until bridge restart"
    );
    assert_eq!(first_control.terminate_calls(), vec![Duration::ZERO]);
    tokio::task::yield_now().await;
    assert_eq!(factory.spawn_count(), 1);
    assert!(matches!(handle.client(), Err(SupervisorError::NotReady)));
    assert_eq!(
        handle.shutdown().await,
        Err(SupervisorError::CleanupFailed),
        "a fenced cleanup failure must remain visible to the owner"
    );
}

#[tokio::test]
async fn one_shot_unexpected_exit_never_restarts_and_confirms_cleanup() {
    let (first, first_control) = FakeFactory::ready();
    let (second, _second_control) = FakeFactory::ready();
    let factory = Arc::new(FakeFactory::new([first, second]));
    let mut handle = AppServerSupervisor::start_once_with_factory(
        CodexProcessConfig::default(),
        factory.clone(),
        test_settings(),
    )
    .await
    .expect("one-shot supervisor starts");

    assert!(matches!(
        next_one_shot_state(&mut handle).await,
        SupervisorState::Ready { epoch: 1, .. }
    ));
    assert_eq!(
        handle
            .client()
            .expect("initialized one-shot client")
            .epoch()
            .get(),
        1
    );
    assert_eq!(
        format!(
            "{:?}",
            handle
                .profile_identity()
                .expect("ready one-shot profile identity")
        ),
        "ProfileIdentity([REDACTED])"
    );

    first_control.unexpected_exit();
    assert!(matches!(
        next_one_shot_state(&mut handle).await,
        SupervisorState::Stopped
    ));
    assert_eq!(
        factory.spawn_count(),
        1,
        "one-shot ownership must not restart"
    );
    assert_eq!(first_control.terminate_calls(), vec![Duration::ZERO]);
    assert!(matches!(handle.client(), Err(SupervisorError::NotReady)));
    assert!(matches!(
        handle.profile_identity(),
        Err(SupervisorError::NotReady)
    ));
    handle
        .shutdown()
        .await
        .expect("completed cleanup remains confirmed to the owner");
}

#[tokio::test]
async fn unexpected_leader_exit_terminates_before_client_shutdown() {
    let (outcome, control) = FakeFactory::ready();
    let factory = Arc::new(FakeFactory::new([outcome]));
    let mut handle = AppServerSupervisor::start_once_with_factory(
        CodexProcessConfig::default(),
        factory,
        SupervisorSettings::new(Duration::from_millis(50), |_, _| Duration::ZERO),
    )
    .await
    .expect("one-shot supervisor starts");
    assert!(matches!(
        next_one_shot_state(&mut handle).await,
        SupervisorState::Ready { epoch: 1, .. }
    ));

    control.unexpected_leader_exit();
    assert!(matches!(
        next_one_shot_state(&mut handle).await,
        SupervisorState::Stopped
    ));
    assert_eq!(control.terminate_calls(), vec![Duration::ZERO]);
    assert_eq!(
        control.lifecycle_events(),
        vec!["terminate", "client_shutdown"],
        "a reaped leader's numeric process-group identity must be settled before transport teardown"
    );
    handle.shutdown().await.expect("cleanup remains confirmed");
}

#[tokio::test]
async fn one_shot_consuming_shutdown_waits_for_confirmed_cleanup() {
    let (outcome, control) = FakeFactory::ready();
    let factory = Arc::new(FakeFactory::new([outcome]));
    let mut handle = AppServerSupervisor::start_once_with_factory(
        CodexProcessConfig::default(),
        factory,
        SupervisorSettings::new(Duration::from_millis(11), |_, _| Duration::ZERO),
    )
    .await
    .expect("one-shot supervisor starts");
    assert!(matches!(
        next_one_shot_state(&mut handle).await,
        SupervisorState::Ready { epoch: 1, .. }
    ));

    handle
        .shutdown()
        .await
        .expect("process abstraction confirms termination and reap");
    assert_eq!(control.terminate_calls(), vec![Duration::from_millis(11)]);
}

#[tokio::test]
async fn one_shot_consuming_shutdown_reports_unconfirmed_cleanup() {
    let (outcome, control) = FakeFactory::ready_with_terminate_error(io::ErrorKind::TimedOut);
    let factory = Arc::new(FakeFactory::new([outcome]));
    let mut handle = AppServerSupervisor::start_once_with_factory(
        CodexProcessConfig::default(),
        factory.clone(),
        SupervisorSettings::new(Duration::from_millis(13), |_, _| Duration::ZERO),
    )
    .await
    .expect("one-shot supervisor starts");
    assert!(matches!(
        next_one_shot_state(&mut handle).await,
        SupervisorState::Ready { epoch: 1, .. }
    ));

    assert_eq!(handle.shutdown().await, Err(SupervisorError::CleanupFailed));
    assert_eq!(factory.spawn_count(), 1);
    assert_eq!(control.terminate_calls(), vec![Duration::from_millis(13)]);
}

#[tokio::test]
async fn one_shot_process_tree_cleanup_uncertainty_survives_consuming_shutdown() {
    let factory = Arc::new(FakeFactory::new([FakeOutcome::Error(
        ProcessError::ProcessTreeCleanupUnconfirmed,
    )]));
    let handle = AppServerSupervisor::start_once_with_factory(
        CodexProcessConfig::default(),
        factory.clone(),
        test_settings(),
    )
    .await
    .expect("the owner must receive the degraded one-shot handle");

    assert!(matches!(
        handle.state(),
        SupervisorState::Degraded { ref reason }
            if reason == "Codex process cleanup failed; replacement is fenced until bridge restart"
    ));
    assert_eq!(handle.shutdown().await, Err(SupervisorError::CleanupFailed));
    assert_eq!(factory.spawn_count(), 1);
}

#[tokio::test]
async fn cancelling_public_startup_stops_the_detached_supervisor_task() {
    let (first, first_gate) =
        FakeFactory::gated_error(ProcessError::Wait(io::Error::from(io::ErrorKind::TimedOut)));
    let (second, second_gate) =
        FakeFactory::gated_error(ProcessError::Wait(io::Error::from(io::ErrorKind::TimedOut)));
    let factory = Arc::new(FakeFactory::new([first, second]));
    let startup = tokio::spawn({
        let factory = Arc::clone(&factory);
        async move {
            AppServerSupervisor::start_with_factory(
                CodexProcessConfig::default(),
                factory,
                test_settings(),
            )
            .await
        }
    });

    first_gate.wait_started().await;
    startup.abort();
    assert!(
        matches!(startup.await, Err(error) if error.is_cancelled()),
        "the public startup future must be cancelled before it returns a handle"
    );
    first_gate.release();
    assert!(
        tokio::time::timeout(Duration::from_millis(250), second_gate.wait_started())
            .await
            .is_err(),
        "startup cancellation must not leave a detached retry loop"
    );
    assert_eq!(factory.spawn_count(), 1);
}

#[tokio::test]
async fn cancelling_public_startup_reaps_a_process_returned_by_inflight_spawn() {
    let (outcome, control, gate) = FakeFactory::gated_ready();
    let factory = Arc::new(FakeFactory::new([outcome]));
    let settings = test_settings();
    let expected_grace = settings.shutdown_grace();
    let startup = tokio::spawn({
        let factory = Arc::clone(&factory);
        async move {
            AppServerSupervisor::start_with_factory(
                CodexProcessConfig::default(),
                factory,
                settings,
            )
            .await
        }
    });

    gate.wait_started().await;
    startup.abort();
    assert!(
        matches!(startup.await, Err(error) if error.is_cancelled()),
        "the public startup future must be cancelled before it returns a handle"
    );
    gate.release();

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if control.terminate_calls() == vec![expected_grace] {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the detached supervisor must reap the process after spawn completes");
    assert_eq!(factory.spawn_count(), 1);
}

#[tokio::test]
async fn startup_reports_task_failure_if_the_supervisor_exits_before_its_first_state() {
    let result = tokio::time::timeout(
        Duration::from_millis(250),
        AppServerSupervisor::start_with_factory(
            CodexProcessConfig::default(),
            Arc::new(PanickingFactory),
            test_settings(),
        ),
    )
    .await
    .expect("startup must not hang after the supervisor task fails");

    assert!(matches!(result, Err(SupervisorError::TaskFailed)));
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

    let state = next_state(&mut handle).await;
    let SupervisorState::Degraded { reason } = state else {
        panic!("permanent version failure must degrade");
    };
    assert_eq!(
        reason,
        format!(
            "Codex 0.145.0 is unsupported; expected an exact reviewed version ({})",
            SUPPORTED_CODEX_VERSIONS.join(", ")
        )
    );
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

#[tokio::test]
async fn shutdown_propagates_process_cleanup_failure() {
    let (outcome, control) = FakeFactory::ready_with_terminate_error(io::ErrorKind::TimedOut);
    let factory = Arc::new(FakeFactory::new([outcome]));
    let mut handle = AppServerSupervisor::start_with_factory(
        CodexProcessConfig::default(),
        factory,
        SupervisorSettings::new(Duration::from_millis(9), |_, _| Duration::ZERO),
    )
    .await
    .expect("supervisor starts");
    assert!(matches!(
        next_state(&mut handle).await,
        SupervisorState::Ready { .. }
    ));

    assert_eq!(handle.shutdown().await, Err(SupervisorError::CleanupFailed));
    assert_eq!(control.terminate_calls(), vec![Duration::from_millis(9)]);
}
