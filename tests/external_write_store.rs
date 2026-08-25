use lark_codex_bridge::store::{
    ExternalApplyOutcome, ExternalApprovalClaimOutcome, ExternalApprovalKind,
    ExternalApprovalReassignmentOutcome, ExternalApprovalReceiveOutcome,
    ExternalApprovalResolution, ExternalApprovalState, ExternalEndpointState, ExternalMutationKind,
    ExternalMutationResolution, ExternalMutationState, ExternalPrepareOutcome,
    ExternalTerminalStatus, ExternalTransitionOutcome, ExternalTurnTerminal,
    ExternalUncertaintyReason, NewExternalApprovalClaim, NewExternalMutationIntent, StoreHandle,
};

async fn ready_store() -> (StoreHandle, u64) {
    let store = StoreHandle::open_in_memory().await.expect("store");
    let epoch = store
        .reserve_external_epoch("ext-write", ExternalUncertaintyReason::BridgeRestart)
        .await
        .expect("epoch")
        .epoch;
    store
        .register_external_thread("ext-write", "thread-a")
        .await
        .expect("register");
    store
        .begin_external_reconciliation("ext-write", "thread-a", epoch)
        .await
        .expect("begin");
    assert!(matches!(
        store
            .apply_external_reconciliation("ext-write", "thread-a", epoch, vec![], vec![])
            .await
            .expect("reconcile"),
        ExternalApplyOutcome::Applied { .. }
    ));
    store
        .set_external_endpoint_state("ext-write", epoch, ExternalEndpointState::Ready)
        .await
        .expect("ready");
    (store, epoch)
}

fn intent(
    id: &str,
    epoch: u64,
    kind: ExternalMutationKind,
    expected_turn_id: Option<&str>,
    client_message_id: Option<&str>,
) -> NewExternalMutationIntent {
    NewExternalMutationIntent {
        endpoint_label: "ext-write".to_owned(),
        thread_id: "thread-a".to_owned(),
        intent_id: id.to_owned(),
        epoch,
        kind,
        expected_turn_id: expected_turn_id.map(str::to_owned),
        client_message_id: client_message_id.map(str::to_owned),
        source_actor: "lark-source-a".to_owned(),
        client_actor: "bridge-client-a".to_owned(),
        approval_actor: "bridge-approval-a".to_owned(),
    }
}

async fn applied_start(store: &StoreHandle, epoch: u64, id: &str, turn_id: &str) {
    assert_eq!(
        store
            .prepare_external_mutation(intent(
                id,
                epoch,
                ExternalMutationKind::TurnStart,
                None,
                Some("message-a"),
            ))
            .await
            .expect("prepare"),
        ExternalPrepareOutcome::Prepared
    );
    assert_eq!(
        store
            .mark_external_mutation_sent("ext-write", "thread-a", id, epoch)
            .await
            .expect("sent"),
        ExternalTransitionOutcome::Applied
    );
    assert_eq!(
        store
            .resolve_external_mutation(
                "ext-write",
                "thread-a",
                id,
                epoch,
                ExternalMutationResolution::Applied {
                    result_id: Some(turn_id),
                },
            )
            .await
            .expect("applied"),
        ExternalTransitionOutcome::Applied
    );
}

#[tokio::test]
async fn simultaneous_mutations_have_one_durable_winner_and_duplicates_never_resend() {
    let (store, epoch) = ready_store().await;
    let first = intent(
        "intent-a",
        epoch,
        ExternalMutationKind::TurnStart,
        None,
        Some("message-a"),
    );
    let second = intent(
        "intent-b",
        epoch,
        ExternalMutationKind::TurnStart,
        None,
        Some("message-b"),
    );
    let (left, right) = tokio::join!(
        store.prepare_external_mutation(first.clone()),
        store.prepare_external_mutation(second)
    );
    let outcomes = [left.expect("left"), right.expect("right")];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == ExternalPrepareOutcome::Prepared)
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == ExternalPrepareOutcome::Busy)
            .count(),
        1
    );
    let winner = if outcomes[0] == ExternalPrepareOutcome::Prepared {
        "intent-a"
    } else {
        "intent-b"
    };
    let duplicate = if winner == "intent-a" {
        first
    } else {
        intent(
            "intent-b",
            epoch,
            ExternalMutationKind::TurnStart,
            None,
            Some("message-b"),
        )
    };
    assert_eq!(
        store
            .prepare_external_mutation(duplicate)
            .await
            .expect("duplicate"),
        ExternalPrepareOutcome::Duplicate(ExternalMutationState::Prepared)
    );
    store
        .resolve_external_mutation(
            "ext-write",
            "thread-a",
            winner,
            epoch,
            ExternalMutationResolution::Rejected,
        )
        .await
        .expect("release");
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn sent_disconnect_is_durable_uncertainty_and_never_reopens_on_restart() {
    let (store, epoch) = ready_store().await;
    let sent = intent(
        "intent-sent",
        epoch,
        ExternalMutationKind::TurnInterrupt,
        Some("turn-a"),
        None,
    );
    assert_eq!(
        store
            .prepare_external_mutation(sent)
            .await
            .expect("prepare"),
        ExternalPrepareOutcome::Prepared
    );
    store
        .mark_external_mutation_sent("ext-write", "thread-a", "intent-sent", epoch)
        .await
        .expect("sent");
    let next = store
        .reserve_external_epoch("ext-write", ExternalUncertaintyReason::SocketDisconnect)
        .await
        .expect("restart")
        .epoch;
    let persisted = store
        .external_mutation_intent("ext-write", "thread-a", "intent-sent")
        .await
        .expect("read")
        .expect("intent");
    assert_eq!(persisted.state, ExternalMutationState::Uncertain);
    store
        .begin_external_reconciliation("ext-write", "thread-a", next)
        .await
        .expect("begin");
    store
        .apply_external_reconciliation("ext-write", "thread-a", next, vec![], vec![])
        .await
        .expect("reconcile");
    store
        .set_external_endpoint_state("ext-write", next, ExternalEndpointState::Ready)
        .await
        .expect("ready");
    assert_eq!(
        store
            .prepare_external_mutation(intent(
                "intent-after-restart",
                next,
                ExternalMutationKind::TurnStart,
                None,
                Some("message-next"),
            ))
            .await
            .expect("fenced"),
        ExternalPrepareOutcome::Uncertain
    );
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn approval_claims_are_single_recipient_epoch_bound_and_resolve_once() {
    let (store, epoch) = ready_store().await;
    applied_start(&store, epoch, "intent-owner", "turn-owned").await;
    let claim = NewExternalApprovalClaim {
        endpoint_label: "ext-write".to_owned(),
        thread_id: "thread-a".to_owned(),
        approval_id: "approval-a".to_owned(),
        request_key: "request-hash-a".to_owned(),
        epoch,
        turn_id: "turn-owned".to_owned(),
        item_id: "item-a".to_owned(),
        kind: ExternalApprovalKind::Command,
        client_actor: "bridge-client-a".to_owned(),
        approval_actor: "bridge-approval-a".to_owned(),
        recipient_actor: "lark-owner-a".to_owned(),
        deadline_ms: i64::MAX - 1,
    };
    assert_eq!(
        store
            .receive_external_approval(claim.clone())
            .await
            .expect("receive"),
        ExternalApprovalReceiveOutcome::Received
    );
    assert!(matches!(
        store
            .receive_external_approval(claim)
            .await
            .expect("duplicate"),
        ExternalApprovalReceiveOutcome::Duplicate {
            state: ExternalApprovalState::Received,
            ..
        }
    ));
    assert_eq!(
        store
            .claim_external_approval(
                "ext-write",
                "thread-a",
                "approval-a",
                "not-the-recipient",
                epoch,
            )
            .await
            .expect("unauthorized"),
        ExternalApprovalClaimOutcome::Unauthorized
    );
    assert_eq!(
        store
            .claim_external_approval("ext-write", "thread-a", "approval-a", "lark-owner-a", epoch,)
            .await
            .expect("claim"),
        ExternalApprovalClaimOutcome::Claimed
    );
    assert_eq!(
        store
            .claim_external_approval("ext-write", "thread-a", "approval-a", "lark-owner-a", epoch,)
            .await
            .expect("duplicate claim"),
        ExternalApprovalClaimOutcome::Duplicate
    );
    store
        .resolve_external_approval(
            "ext-write",
            "thread-a",
            "approval-a",
            epoch,
            ExternalApprovalResolution::Responding,
        )
        .await
        .expect("responding");
    assert_eq!(
        store
            .resolve_external_approval_request("ext-write", "request-hash-a", epoch)
            .await
            .expect("resolved notification"),
        ExternalTransitionOutcome::Applied
    );
    assert_eq!(
        store
            .external_approval_claim("ext-write", "thread-a", "approval-a")
            .await
            .expect("read")
            .expect("claim")
            .state,
        ExternalApprovalState::Resolved
    );
    assert_eq!(
        store
            .claim_external_approval("ext-write", "thread-a", "approval-a", "lark-owner-a", epoch,)
            .await
            .expect("late duplicate"),
        ExternalApprovalClaimOutcome::Stale
    );
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn approval_reassignment_requires_terminal_turns_and_a_fully_drained_handler() {
    let (store, epoch) = ready_store().await;
    applied_start(&store, epoch, "intent-owner", "turn-owned").await;
    assert_eq!(
        store
            .reassign_external_approval_actor(
                "ext-write",
                epoch,
                "bridge-approval-a",
                "bridge-approval-b",
            )
            .await
            .expect("busy"),
        ExternalApprovalReassignmentOutcome::NotDrained
    );
    store
        .record_external_terminal(
            "ext-write",
            "thread-a",
            epoch,
            Some(ExternalTurnTerminal {
                turn_id: "turn-owned".to_owned(),
                status: ExternalTerminalStatus::Interrupted,
            }),
            None,
        )
        .await
        .expect("terminal");
    assert_eq!(
        store
            .reassign_external_approval_actor(
                "ext-write",
                epoch,
                "bridge-approval-a",
                "bridge-approval-b",
            )
            .await
            .expect("reassign"),
        ExternalApprovalReassignmentOutcome::Reassigned
    );
    let next = intent(
        "intent-next",
        epoch,
        ExternalMutationKind::TurnStart,
        None,
        Some("message-next"),
    );
    assert_eq!(
        store
            .prepare_external_mutation(next)
            .await
            .expect("old actor rejected"),
        ExternalPrepareOutcome::ApprovalHandlerMismatch
    );
    store.shutdown().await.expect("shutdown");
}
