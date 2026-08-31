#![allow(clippy::similar_names, clippy::too_many_lines)]

use std::path::{Path, PathBuf};

use lark_codex_bridge::lark::normalize::ScopeKey;
use lark_codex_bridge::store::{
    NewTurnRow, StoreError, StoreHandle, ThreadAdoptionOutcome, ThreadAdoptionReleaseResult,
    ThreadAdoptionSaga, ThreadAdoptionState, ThreadOrigin, TurnState,
};
use tempfile::tempdir;

async fn seed_scope(store: &StoreHandle, name: &str, cwd: &Path, fingerprint: &str) -> ScopeKey {
    let scope = ScopeKey::Chat(name.to_owned());
    store
        .upsert_scope(&scope, cwd, fingerprint)
        .await
        .expect("seed scope");
    scope
}

async fn make_live_turn(
    store: &StoreHandle,
    scope: &ScopeKey,
    label: &str,
    state: TurnState,
) -> i64 {
    let id = store
        .record_turn(NewTurnRow {
            scope_key: scope.to_string(),
            client_message_id: label.to_owned(),
            codex_thread_id: None,
            state: TurnState::Starting,
        })
        .await
        .expect("record starting turn");
    if state != TurnState::Starting {
        store
            .set_turn_state(id, state, None)
            .await
            .expect("advance live turn");
    }
    id
}

async fn finish_live_turn(store: &StoreHandle, id: i64) {
    store
        .set_turn_state(id, TurnState::Failed, None)
        .await
        .expect("finish live turn");
}

async fn commit(store: &StoreHandle, saga: &ThreadAdoptionSaga, cwd: &Path, fingerprint: &str) {
    store
        .commit_thread_adoption(saga, cwd, fingerprint)
        .await
        .expect("commit adoption");
}

#[tokio::test]
async fn migration_v11_defaults_bridge_threads_and_filters_bound_targets() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    assert_eq!(store.pragmas().await.expect("pragmas").user_version, 12);
    let scope = seed_scope(&store, "migration", Path::new("/old-workspace"), "old-fp").await;

    assert!(
        store
            .thread_adoption_target_available("bridge-thread")
            .await
            .expect("unbound target")
    );
    assert!(
        !store
            .thread_adoption_target_available("")
            .await
            .expect("empty target")
    );
    store
        .record_active_thread(&scope, "bridge-thread")
        .await
        .expect("bridge mapping");
    let row = store
        .active_thread(&scope)
        .await
        .expect("read mapping")
        .expect("active mapping");
    assert_eq!(row.origin, ThreadOrigin::BridgeCreated);
    assert_eq!(row.adoption_generation, None);
    assert!(
        !store
            .thread_adoption_target_available("bridge-thread")
            .await
            .expect("bound target")
    );

    store
        .archive_active_thread(&scope)
        .await
        .expect("archive bridge mapping")
        .expect("archived row");
    assert!(
        store
            .thread_adoption_target_available("bridge-thread")
            .await
            .expect("archived target")
    );
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn reserve_then_commit_atomically_switches_mapping_workspace_and_origin() {
    let temp = tempdir().expect("tempdir");
    let old_cwd = temp.path().join("old");
    let adopted_cwd = temp.path().join("adopted");
    let store = StoreHandle::open_in_memory().await.expect("open");
    let scope = seed_scope(&store, "commit", &old_cwd, "old-fp").await;
    store
        .record_active_thread_with_context_tools(&scope, "bridge-thread", 7)
        .await
        .expect("old mapping");

    let reservation = store
        .reserve_thread_adoption(&scope, "persisted-thread")
        .await
        .expect("reserve");
    assert_eq!(reservation.state, ThreadAdoptionState::Acquiring);
    assert!(
        !store
            .thread_adoption_target_available("persisted-thread")
            .await
            .expect("reserved target")
    );
    let before = store
        .active_thread(&scope)
        .await
        .expect("old mapping")
        .expect("old active");
    assert_eq!(before.codex_thread_id, "bridge-thread");
    assert_eq!(before.origin, ThreadOrigin::BridgeCreated);

    let adopted = store
        .commit_thread_adoption(&reservation, &adopted_cwd, "adopted-fp")
        .await
        .expect("commit");
    assert_eq!(adopted.codex_thread_id, "persisted-thread");
    assert_eq!(adopted.origin, ThreadOrigin::ExternallyAdopted);
    assert_eq!(adopted.adoption_generation, Some(reservation.generation));
    assert_eq!(
        store
            .active_thread_adoption(&scope)
            .await
            .expect("saga")
            .expect("live saga")
            .state,
        ThreadAdoptionState::Owned
    );
    let updated_scope = store
        .scope_row(&scope)
        .await
        .expect("scope")
        .expect("scope row");
    assert_eq!(updated_scope.cwd, adopted_cwd);
    assert_eq!(updated_scope.policy_fingerprint, "adopted-fp");
    assert!(matches!(
        store.archive_active_thread(&scope).await,
        Err(StoreError::InvalidTransition { .. })
    ));
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn commit_conflict_rolls_back_scope_mapping_and_saga() {
    let temp = tempdir().expect("tempdir");
    let old_cwd = temp.path().join("old");
    let new_cwd = temp.path().join("new");
    let other_cwd = temp.path().join("other");
    let store = StoreHandle::open_in_memory().await.expect("open");
    let scope = seed_scope(&store, "rollback", &old_cwd, "old-fp").await;
    let other = seed_scope(&store, "rollback-other", &other_cwd, "other-fp").await;
    store
        .record_active_thread(&scope, "old-thread")
        .await
        .expect("old mapping");
    let reservation = store
        .reserve_thread_adoption(&scope, "raced-thread")
        .await
        .expect("reserve");

    // Simulate the target becoming active after read-only discovery and
    // reservation but before the authoritative resume is committed.
    store
        .record_active_thread(&other, "raced-thread")
        .await
        .expect("racing mapping");
    assert!(matches!(
        store
            .commit_thread_adoption(&reservation, &new_cwd, "new-fp")
            .await,
        Err(StoreError::InvalidTransition { .. })
    ));

    let scope_row = store
        .scope_row(&scope)
        .await
        .expect("scope")
        .expect("scope row");
    assert_eq!(scope_row.cwd, old_cwd);
    assert_eq!(scope_row.policy_fingerprint, "old-fp");
    assert_eq!(
        store
            .active_thread(&scope)
            .await
            .expect("mapping")
            .expect("old active")
            .codex_thread_id,
        "old-thread"
    );
    assert_eq!(
        store
            .thread_adoption_saga(&scope)
            .await
            .expect("saga")
            .expect("reservation")
            .state,
        ThreadAdoptionState::Acquiring
    );
    assert_eq!(
        store
            .active_thread(&other)
            .await
            .expect("other mapping")
            .expect("other active")
            .codex_thread_id,
        "raced-thread"
    );

    store
        .archive_active_thread(&other)
        .await
        .expect("clear race")
        .expect("other archived");
    commit(&store, &reservation, &new_cwd, "new-fp").await;
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn reservations_are_bridge_wide_terminal_and_generational() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    let first = seed_scope(&store, "first", Path::new("/first"), "first-fp").await;
    let second = seed_scope(&store, "second", Path::new("/second"), "second-fp").await;
    let first_generation = store
        .reserve_thread_adoption(&first, "shared-target")
        .await
        .expect("first reserve");
    assert!(matches!(
        store
            .reserve_thread_adoption(&second, "shared-target")
            .await,
        Err(StoreError::InvalidTransition { .. })
    ));
    let terminal = store
        .finish_thread_adoption_acquisition_failure(&first_generation)
        .await
        .expect("terminalize first");
    assert_eq!(terminal.state, ThreadAdoptionState::Terminal);
    assert_eq!(
        terminal.outcome,
        Some(ThreadAdoptionOutcome::AcquisitionFailed)
    );
    assert!(
        store
            .thread_adoption_target_available("shared-target")
            .await
            .expect("terminal target")
    );

    let second_generation = store
        .reserve_thread_adoption(&second, "shared-target")
        .await
        .expect("second reserve");
    store
        .finish_thread_adoption_acquisition_failure(&second_generation)
        .await
        .expect("terminalize second");
    let next = store
        .reserve_thread_adoption(&first, "next-target")
        .await
        .expect("next generation");
    assert_eq!(next.generation, first_generation.generation + 1);
    assert!(matches!(
        store
            .finish_thread_adoption_acquisition_failure(&first_generation)
            .await,
        Err(StoreError::InvalidTransition { .. })
    ));
    store
        .finish_thread_adoption_acquisition_failure(&next)
        .await
        .expect("finish next generation");

    store
        .record_active_thread(&first, "globally-unique")
        .await
        .expect("first active mapping");
    assert!(matches!(
        store.record_active_thread(&second, "globally-unique").await,
        Err(StoreError::Sqlite { .. })
    ));
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn every_live_turn_state_blocks_reserve_commit_begin_and_finish() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    for (index, live_state) in [
        TurnState::Starting,
        TurnState::Running,
        TurnState::Uncertain,
    ]
    .into_iter()
    .enumerate()
    {
        let cwd = PathBuf::from(format!("/live-{index}"));
        let scope = seed_scope(&store, &format!("live-{index}"), &cwd, "fp").await;
        let turn =
            make_live_turn(&store, &scope, &format!("reserve-live-{index}"), live_state).await;
        assert!(matches!(
            store
                .reserve_thread_adoption(&scope, &format!("reserve-target-{index}"))
                .await,
            Err(StoreError::InvalidTransition { .. })
        ));
        finish_live_turn(&store, turn).await;

        let saga = store
            .reserve_thread_adoption(&scope, &format!("owned-target-{index}"))
            .await
            .expect("reserve after terminal turn");
        let turn =
            make_live_turn(&store, &scope, &format!("commit-live-{index}"), live_state).await;
        assert!(matches!(
            store.commit_thread_adoption(&saga, &cwd, "fp").await,
            Err(StoreError::InvalidTransition { .. })
        ));
        finish_live_turn(&store, turn).await;
        commit(&store, &saga, &cwd, "fp").await;

        let turn = make_live_turn(&store, &scope, &format!("begin-live-{index}"), live_state).await;
        assert!(matches!(
            store.begin_thread_adoption_release(&saga).await,
            Err(StoreError::InvalidTransition { .. })
        ));
        finish_live_turn(&store, turn).await;
        let releasing = store
            .begin_thread_adoption_release(&saga)
            .await
            .expect("begin after terminal turn");

        let turn =
            make_live_turn(&store, &scope, &format!("finish-live-{index}"), live_state).await;
        assert!(matches!(
            store
                .finish_thread_adoption_release(&releasing, ThreadAdoptionReleaseResult::Released,)
                .await,
            Err(StoreError::InvalidTransition { .. })
        ));
        assert!(
            store
                .active_thread(&scope)
                .await
                .expect("mapping")
                .is_some()
        );
        finish_live_turn(&store, turn).await;
        store
            .finish_thread_adoption_release(&releasing, ThreadAdoptionReleaseResult::Released)
            .await
            .expect("finish after terminal turn");
    }
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn failed_release_keeps_mapping_fenced_until_confirmed_retry() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    let scope = seed_scope(&store, "release", Path::new("/release"), "fp").await;
    let saga = store
        .reserve_thread_adoption(&scope, "release-target")
        .await
        .expect("reserve");
    commit(&store, &saga, Path::new("/release"), "fp").await;
    let releasing = store
        .begin_thread_adoption_release(&saga)
        .await
        .expect("begin release");
    assert!(
        store
            .active_thread(&scope)
            .await
            .expect("mapping")
            .is_some()
    );
    let failed = store
        .finish_thread_adoption_release(&releasing, ThreadAdoptionReleaseResult::Failed)
        .await
        .expect("failed release");
    assert_eq!(failed.state, ThreadAdoptionState::ReleaseFailed);
    assert!(
        store
            .active_thread(&scope)
            .await
            .expect("mapping")
            .is_some()
    );
    assert!(
        !store
            .thread_adoption_target_available("release-target")
            .await
            .expect("failed target")
    );

    let retry = store
        .begin_thread_adoption_release(&failed)
        .await
        .expect("retry release");
    let released = store
        .finish_thread_adoption_release(&retry, ThreadAdoptionReleaseResult::Released)
        .await
        .expect("confirmed release");
    assert_eq!(released.state, ThreadAdoptionState::Terminal);
    assert_eq!(released.outcome, Some(ThreadAdoptionOutcome::Released));
    assert!(
        store
            .active_thread(&scope)
            .await
            .expect("mapping")
            .is_none()
    );
    assert!(
        store
            .thread_adoption_target_available("release-target")
            .await
            .expect("released target")
    );
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn startup_fence_marks_every_live_saga_without_dropping_mappings() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    let acquiring_scope =
        seed_scope(&store, "fence-acquiring", Path::new("/acquiring"), "fp").await;
    let owned_scope = seed_scope(&store, "fence-owned", Path::new("/owned"), "fp").await;
    let releasing_scope =
        seed_scope(&store, "fence-releasing", Path::new("/releasing"), "fp").await;
    let failed_scope = seed_scope(&store, "fence-failed", Path::new("/failed"), "fp").await;

    let acquiring = store
        .reserve_thread_adoption(&acquiring_scope, "acquiring-target")
        .await
        .expect("acquiring");
    let owned = store
        .reserve_thread_adoption(&owned_scope, "owned-target")
        .await
        .expect("owned reserve");
    commit(&store, &owned, Path::new("/owned"), "fp").await;
    let releasing = store
        .reserve_thread_adoption(&releasing_scope, "releasing-target")
        .await
        .expect("releasing reserve");
    commit(&store, &releasing, Path::new("/releasing"), "fp").await;
    store
        .begin_thread_adoption_release(&releasing)
        .await
        .expect("releasing state");
    let failed = store
        .reserve_thread_adoption(&failed_scope, "failed-target")
        .await
        .expect("failed reserve");
    commit(&store, &failed, Path::new("/failed"), "fp").await;
    let failed_releasing = store
        .begin_thread_adoption_release(&failed)
        .await
        .expect("failed begin");
    store
        .finish_thread_adoption_release(&failed_releasing, ThreadAdoptionReleaseResult::Failed)
        .await
        .expect("failed state");

    assert_eq!(
        store
            .fence_thread_adoptions_on_startup()
            .await
            .expect("startup fence"),
        4
    );
    assert_eq!(
        store
            .fence_thread_adoptions_on_startup()
            .await
            .expect("idempotent startup fence"),
        0
    );
    for scope in [
        &acquiring_scope,
        &owned_scope,
        &releasing_scope,
        &failed_scope,
    ] {
        assert_eq!(
            store
                .active_thread_adoption(scope)
                .await
                .expect("live saga")
                .expect("recovery saga")
                .state,
            ThreadAdoptionState::RecoveryRequired
        );
    }
    assert!(
        store
            .active_thread(&acquiring_scope)
            .await
            .expect("acquiring mapping")
            .is_none()
    );
    for scope in [&owned_scope, &releasing_scope, &failed_scope] {
        assert!(
            store
                .active_thread(scope)
                .await
                .expect("retained mapping")
                .is_some()
        );
    }

    let fenced_acquiring = store
        .active_thread_adoption(&acquiring_scope)
        .await
        .expect("acquiring saga")
        .expect("acquiring recovery");
    store
        .finish_thread_adoption_acquisition_failure(&fenced_acquiring)
        .await
        .expect("finish fenced acquisition");
    let fenced_owned = store
        .active_thread_adoption(&owned_scope)
        .await
        .expect("owned saga")
        .expect("owned recovery");
    store
        .begin_thread_adoption_release(&fenced_owned)
        .await
        .expect("recover owned release");

    let individually_fenced = store
        .fence_thread_adoption(&acquiring)
        .await
        .expect_err("terminal stale generation cannot be fenced");
    assert!(matches!(
        individually_fenced,
        StoreError::InvalidTransition { .. }
    ));
    store.shutdown().await.expect("shutdown");
}
