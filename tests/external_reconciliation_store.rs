use lark_codex_bridge::limits::EXTERNAL_MANAGED_THREAD_CAPACITY;
use lark_codex_bridge::store::{
    ExternalApplyOutcome, ExternalEndpointState, ExternalFenceOutcome, ExternalItemTerminal,
    ExternalTerminalStatus, ExternalThreadState, ExternalTurnTerminal, ExternalUncertaintyReason,
    StoreError, StoreHandle,
};
use tempfile::tempdir;

fn turn(id: &str, status: ExternalTerminalStatus) -> ExternalTurnTerminal {
    ExternalTurnTerminal {
        turn_id: id.to_owned(),
        status,
    }
}

fn item(turn_id: &str, item_id: &str) -> ExternalItemTerminal {
    ExternalItemTerminal {
        turn_id: turn_id.to_owned(),
        item_id: item_id.to_owned(),
    }
}

#[tokio::test]
async fn epoch_fence_deduplicates_terminals_and_rejects_stale_updates() {
    let store = StoreHandle::open_in_memory().await.expect("store");
    let first = store
        .reserve_external_epoch("ext-test", ExternalUncertaintyReason::BridgeRestart)
        .await
        .expect("first epoch");
    assert_eq!(first.epoch, 1);
    store
        .register_external_thread("ext-test", "thread-a")
        .await
        .expect("register");
    assert_eq!(
        store
            .begin_external_reconciliation("ext-test", "thread-a", first.epoch)
            .await
            .expect("begin"),
        ExternalFenceOutcome::Current
    );
    assert_eq!(
        store
            .apply_external_reconciliation(
                "ext-test",
                "thread-a",
                first.epoch,
                vec![turn("turn-a", ExternalTerminalStatus::Completed)],
                vec![item("turn-a", "item-a")],
            )
            .await
            .expect("apply"),
        ExternalApplyOutcome::Applied {
            inserted_turns: 1,
            inserted_items: 1,
        }
    );
    assert_eq!(
        store
            .record_external_terminal(
                "ext-test",
                "thread-a",
                first.epoch,
                Some(turn("turn-a", ExternalTerminalStatus::Completed)),
                Some(item("turn-a", "item-a")),
            )
            .await
            .expect("deduplicate"),
        ExternalApplyOutcome::Applied {
            inserted_turns: 0,
            inserted_items: 0,
        }
    );

    let second = store
        .reserve_external_epoch("ext-test", ExternalUncertaintyReason::SocketDisconnect)
        .await
        .expect("second epoch");
    assert_eq!(second.epoch, 2);
    assert_eq!(
        store
            .record_external_terminal(
                "ext-test",
                "thread-a",
                first.epoch,
                Some(turn("stale", ExternalTerminalStatus::Failed)),
                None,
            )
            .await
            .expect("stale"),
        ExternalApplyOutcome::Stale
    );
    let snapshot = store
        .external_thread_snapshot("ext-test", "thread-a")
        .await
        .expect("snapshot")
        .expect("managed");
    assert_eq!(snapshot.epoch, second.epoch);
    assert_eq!(snapshot.state, ExternalThreadState::Unavailable);
    assert_eq!(
        snapshot.reason,
        Some(ExternalUncertaintyReason::SocketDisconnect)
    );
    assert_eq!(snapshot.terminal_turns.len(), 1);
    assert_eq!(snapshot.terminal_items.len(), 1);
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn conflicting_terminal_is_durable_uncertainty_across_bridge_restart() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("store.sqlite");
    let store = StoreHandle::open(&path).await.expect("store");
    let epoch = store
        .reserve_external_epoch("ext-test", ExternalUncertaintyReason::BridgeRestart)
        .await
        .expect("epoch")
        .epoch;
    store
        .register_external_thread("ext-test", "thread-a")
        .await
        .expect("register");
    store
        .begin_external_reconciliation("ext-test", "thread-a", epoch)
        .await
        .expect("begin");
    store
        .apply_external_reconciliation(
            "ext-test",
            "thread-a",
            epoch,
            vec![turn("turn-a", ExternalTerminalStatus::Completed)],
            vec![],
        )
        .await
        .expect("apply");
    assert_eq!(
        store
            .record_external_terminal(
                "ext-test",
                "thread-a",
                epoch,
                Some(turn("turn-a", ExternalTerminalStatus::Failed)),
                None,
            )
            .await
            .expect("conflict"),
        ExternalApplyOutcome::ConflictingTerminal
    );
    store.shutdown().await.expect("shutdown");

    let store = StoreHandle::open(&path).await.expect("reopen");
    let next = store
        .reserve_external_epoch("ext-test", ExternalUncertaintyReason::BridgeRestart)
        .await
        .expect("new epoch");
    assert_eq!(next.epoch, 2);
    let snapshot = store
        .external_thread_snapshot("ext-test", "thread-a")
        .await
        .expect("snapshot")
        .expect("managed");
    assert_eq!(
        snapshot.epoch, 1,
        "uncertain epoch remains the evidence fence"
    );
    assert_eq!(snapshot.state, ExternalThreadState::Uncertain);
    assert_eq!(
        snapshot.reason,
        Some(ExternalUncertaintyReason::ConflictingTerminal)
    );
    assert_eq!(
        store
            .begin_external_reconciliation("ext-test", "thread-a", next.epoch)
            .await
            .expect("fenced begin"),
        ExternalFenceOutcome::Stale,
        "reconnect never silently clears uncertainty"
    );
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn managed_thread_collection_is_bounded_and_endpoint_state_is_fenced() {
    let store = StoreHandle::open_in_memory().await.expect("store");
    let epoch = store
        .reserve_external_epoch("ext-test", ExternalUncertaintyReason::BridgeRestart)
        .await
        .expect("epoch")
        .epoch;
    for index in 0..EXTERNAL_MANAGED_THREAD_CAPACITY {
        store
            .register_external_thread("ext-test", &format!("thread-{index:02}"))
            .await
            .expect("within bound");
    }
    assert!(matches!(
        store
            .register_external_thread("ext-test", "one-too-many")
            .await,
        Err(StoreError::CapacityExceeded { .. })
    ));
    assert_eq!(
        store
            .set_external_endpoint_state("ext-test", epoch + 1, ExternalEndpointState::Ready)
            .await
            .expect("stale state"),
        ExternalFenceOutcome::Stale
    );
    assert_eq!(
        store
            .set_external_endpoint_state("ext-test", epoch, ExternalEndpointState::Reconciling)
            .await
            .expect("current state"),
        ExternalFenceOutcome::Current
    );
    assert_eq!(
        store
            .external_endpoint_epoch("ext-test")
            .await
            .expect("read")
            .expect("endpoint")
            .state,
        ExternalEndpointState::Reconciling
    );
    assert_eq!(
        store
            .external_managed_threads("ext-test")
            .await
            .expect("list")
            .len(),
        EXTERNAL_MANAGED_THREAD_CAPACITY
    );
    store.shutdown().await.expect("shutdown");
}
