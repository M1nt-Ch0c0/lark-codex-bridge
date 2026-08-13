use std::collections::HashSet;
use std::future::Future;
use std::task::{Context, Poll, Waker};

use lark_codex_bridge::lark::api::ChatMode;
use lark_codex_bridge::lark::config::TenantBrand;
use lark_codex_bridge::lark::credentials::LarkCredentials;
use lark_codex_bridge::lark::normalize::{InboundEvent, ScopeKey};
use lark_codex_bridge::limits::{
    STORE_OUTBOX_CLAIM_MAX_BYTES, STORE_OUTBOX_MAX_QUEUED_BYTES, STORE_OUTBOX_PAYLOAD_MAX_BYTES,
    STORE_RECOVERY_TURN_MAX_BYTES, STORE_RECOVERY_TURN_MAX_ROWS, STORE_REQUEST_MAX_BYTES,
    STORE_WRITER_BYTE_BUDGET, STORE_WRITER_CAPACITY,
};
use lark_codex_bridge::runtime::intake::DurableIntake;
use lark_codex_bridge::runtime::intake::TenantNamespace;
use lark_codex_bridge::store::{
    BeginTurnOutcome, DedupOutcome, InboundDisposition, InboundEventState, InboundKey,
    InboundRejectionKind, InboundTerminal, NewOutboxRow, NewTurnRow, OutboxEnqueue, OutboxState,
    ResolveTurnOutcome, ScopeRow, StoreError, StoreHandle, ThreadRow, ThreadStatus, TurnResolution,
    TurnRow, TurnState,
};
use secrecy::SecretString;
use tempfile::tempdir;

fn event(event_id: &str, message_id: &str) -> InboundEvent {
    InboundEvent {
        event_id: event_id.to_owned(),
        message_id: message_id.to_owned(),
        chat_id: "oc_test".to_owned(),
        sender_id: "ou_test".to_owned(),
        chat_type: ChatMode::Group,
        thread_id: None,
        root_id: None,
        reply_to_message_id: None,
        text: "not persisted".to_owned(),
        mentions_bot: true,
        mention_all: false,
        resources: Vec::new(),
        message_type: "text".to_owned(),
        create_time_ms: 1,
        scope: ScopeKey::Chat("oc_test".to_owned()),
    }
}

fn outbox(key: &str, payload: &str) -> NewOutboxRow {
    NewOutboxRow {
        idempotency_key: key.to_owned(),
        scope_key: "im:oc_test".to_owned(),
        kind: "final".to_owned(),
        payload_json: payload.to_owned(),
        next_retry_ms: 0,
    }
}

fn turn(message_id: &str, state: TurnState) -> NewTurnRow {
    NewTurnRow {
        scope_key: "im:oc_test".to_owned(),
        client_message_id: message_id.to_owned(),
        codex_thread_id: Some("thread-1".to_owned()),
        state,
    }
}

fn tenant_namespace(app_id: &str) -> TenantNamespace {
    TenantNamespace::from_credentials(&credentials_for(app_id))
}

fn credentials_for(app_id: &str) -> LarkCredentials {
    LarkCredentials::new(
        app_id.to_owned(),
        SecretString::from("test-secret".to_owned()),
        TenantBrand::Feishu,
    )
}

async fn seed_accepted_file_store(
    path: &std::path::Path,
    app_id: &str,
) -> (LarkCredentials, TenantNamespace, i64) {
    let credentials = credentials_for(app_id);
    let tenant = TenantNamespace::from_credentials(&credentials);
    let store = StoreHandle::open(path).await.expect("open seed store");
    store
        .register_inbound(&tenant, &event("event-forged", "message-forged"))
        .await
        .expect("register seed");
    let begun = store
        .begin_turn_and_claim_inbound(
            turn("original-forged-turn", TurnState::Starting),
            &[InboundKey::new(tenant.clone(), "event-forged".to_owned())],
        )
        .await
        .expect("claim seed");
    let BeginTurnOutcome::Started { turn_row_id, .. } = begun else {
        panic!("seed claim starts a turn")
    };
    store.shutdown().await.expect("shutdown seed store");
    (credentials, tenant, turn_row_id)
}

async fn assert_forged_store_fails_recovery_and_skip(
    path: &std::path::Path,
    credentials: &LarkCredentials,
    tenant: &TenantNamespace,
    event_id: &str,
) {
    let store = StoreHandle::open(path).await.expect("reopen forged store");
    assert!(
        DurableIntake::prepare(store.clone(), credentials)
            .await
            .is_err(),
        "startup preparation must reject the forged state"
    );
    assert!(matches!(
        store.recover_received(tenant).await,
        Err(StoreError::CorruptData { .. } | StoreError::CapacityExceeded { .. })
    ));
    let skip = store
        .begin_turn_and_claim_inbound(
            turn("forged-skip-must-not-create", TurnState::Starting),
            &[InboundKey::new(tenant.clone(), event_id.to_owned())],
        )
        .await;
    assert!(
        matches!(
            skip,
            Err(StoreError::CorruptData { .. }
                | StoreError::CapacityExceeded { .. }
                | StoreError::PayloadTooLarge { .. })
        ),
        "begin skip must reject the forged state, got {skip:?}"
    );
    store.shutdown().await.expect("shutdown forged store");
    let connection = rusqlite::Connection::open(path).expect("inspect forged store");
    let created: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM turns WHERE client_message_id = 'forged-skip-must-not-create'",
            [],
            |row| row.get(0),
        )
        .expect("count forged skip turns");
    assert_eq!(created, 0, "failed skip validation cannot create a turn");
}

fn forge_turn_association(connection: &rusqlite::Connection, turn_row_id: i64, association: &str) {
    match association {
        "accepted-resolved" => connection
            .execute(
                "UPDATE turns SET state = 'failed', uncertain = 0 WHERE id = ?1",
                [turn_row_id],
            )
            .expect("forge accepted resolved"),
        "accepted-cross-scope" => connection
            .execute(
                "UPDATE turns SET scope_key = 'im:other-scope' WHERE id = ?1",
                [turn_row_id],
            )
            .expect("forge accepted scope"),
        "accepted-missing-turn" => connection
            .execute("DELETE FROM turns WHERE id = ?1", [turn_row_id])
            .expect("forge missing turn"),
        "unresolved-count-mismatch" => connection
            .execute(
                "UPDATE turns SET inbound_count = 2 WHERE id = ?1",
                [turn_row_id],
            )
            .expect("forge unresolved marker count"),
        "terminal-live" => connection
            .execute(
                "UPDATE inbound_events
                 SET state = 'rejected', rejection_reason = 'turn_failed',
                     payload_version = NULL, payload_blob = NULL, payload_bytes = 0
                 WHERE event_id = 'event-forged'",
                [],
            )
            .expect("forge terminal linked live"),
        "terminal-cross-scope" | "terminal-wrong-outcome" | "resolved-marker-overflow" => {
            connection
                .execute(
                    "UPDATE turns SET state = 'completed', uncertain = 0 WHERE id = ?1",
                    [turn_row_id],
                )
                .expect("forge completed turn");
            connection
                .execute(
                    "UPDATE inbound_events
                     SET state = 'completed', rejection_reason = NULL,
                         payload_version = NULL, payload_blob = NULL, payload_bytes = 0
                     WHERE event_id = 'event-forged'",
                    [],
                )
                .expect("forge completed marker");
            match association {
                "terminal-cross-scope" => connection
                    .execute(
                        "UPDATE turns SET scope_key = 'im:other-scope' WHERE id = ?1",
                        [turn_row_id],
                    )
                    .expect("forge terminal scope"),
                "terminal-wrong-outcome" => connection
                    .execute(
                        "UPDATE inbound_events
                         SET state = 'rejected', rejection_reason = 'turn_failed'
                         WHERE event_id = 'event-forged'",
                        [],
                    )
                    .expect("forge wrong terminal outcome"),
                "resolved-marker-overflow" => connection
                    .execute(
                        "INSERT INTO inbound_events
                         (tenant, event_id, message_id, scope_key, state,
                          first_seen_ms, updated_ms, rejection_reason,
                          payload_version, payload_blob, payload_bytes, turn_row_id)
                         SELECT tenant, 'event-forged-extra', 'message-forged-extra', scope_key,
                                'completed', 1, 1, NULL, NULL, NULL, 0, ?1
                         FROM inbound_events WHERE event_id = 'event-forged'",
                        [turn_row_id],
                    )
                    .expect("forge excess resolved marker"),
                _ => unreachable!(),
            };
            1
        }
        _ => unreachable!(),
    };
}

#[tokio::test]
async fn migration_two_persists_strict_inbound_payload_columns() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("store.sqlite");
    let store = StoreHandle::open(&path).await.expect("open");
    assert_eq!(store.pragmas().await.expect("pragmas").user_version, 2);
    store.shutdown().await.expect("shutdown");

    let connection = rusqlite::Connection::open(&path).expect("inspect");
    let inbound_columns = connection
        .prepare("PRAGMA table_info(inbound_events)")
        .expect("prepare inbound columns")
        .query_map([], |row| row.get::<_, String>(1))
        .expect("query inbound columns")
        .collect::<Result<HashSet<_>, _>>()
        .expect("decode inbound columns");
    for name in [
        "payload_version",
        "payload_blob",
        "payload_bytes",
        "turn_row_id",
    ] {
        assert!(inbound_columns.contains(name), "missing {name}");
    }
    let turn_columns = connection
        .prepare("PRAGMA table_info(turns)")
        .expect("prepare turn columns")
        .query_map([], |row| row.get::<_, String>(1))
        .expect("query turn columns")
        .collect::<Result<HashSet<_>, _>>()
        .expect("decode turn columns");
    assert!(turn_columns.contains("inbound_count"));
}

#[tokio::test]
async fn registration_replays_canonical_payload_and_atomic_turn_resolution() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    let tenant = tenant_namespace("cli_store_test");
    let canonical = event("event-canonical", "message-canonical");
    let first = store
        .register_inbound(&tenant, &canonical)
        .await
        .expect("register canonical");
    assert!(matches!(first, DedupOutcome::New(_)));

    let mut redelivery = event("event-alias", "message-canonical");
    redelivery.text = "untrusted-redelivery-body".repeat(48_000);
    let replay = store
        .register_inbound(&tenant, &redelivery)
        .await
        .expect("replay canonical");
    let DedupOutcome::ReplayReceived(retained) = replay else {
        panic!("expected canonical replay")
    };
    assert_eq!(retained.event().event_id, canonical.event_id);
    assert_eq!(retained.event().text, canonical.text);
    assert!(retained.retained_bytes() > canonical.text.len());

    let key = InboundKey::new(tenant.clone(), canonical.event_id.clone());
    let started = store
        .begin_turn_and_claim_inbound(turn("atomic-turn", TurnState::Starting), &[key])
        .await
        .expect("begin and claim");
    let BeginTurnOutcome::Started {
        turn_row_id,
        claimed,
        skipped,
    } = started
    else {
        panic!("received row must start a turn")
    };
    assert_eq!(claimed.len(), 1);
    assert!(skipped.is_empty());
    assert_eq!(
        store
            .turn_row(turn_row_id)
            .await
            .expect("turn row")
            .expect("turn row")
            .inbound_count,
        1
    );
    store
        .set_turn_state(turn_row_id, TurnState::Running, Some("codex-turn-atomic"))
        .await
        .expect("runtime turn may enter running");

    let resolved = store
        .resolve_turn_and_finish_inbound_batch(
            turn_row_id,
            TurnResolution::Completed,
            InboundTerminal::Completed,
        )
        .await
        .expect("atomic resolve");
    assert_eq!(resolved, ResolveTurnOutcome::Resolved { inbound_rows: 1 });
    assert_eq!(
        store
            .resolve_turn_and_finish_inbound_batch(
                turn_row_id,
                TurnResolution::Completed,
                InboundTerminal::Completed,
            )
            .await
            .expect("idempotent resolve"),
        ResolveTurnOutcome::AlreadyResolved { inbound_rows: 1 }
    );
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn file_store_applies_pragmas_and_persists_every_typed_table() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("store.sqlite");
    let scope = ScopeKey::Chat("oc_test".to_owned());
    let tenant = tenant_namespace("cli_persistence_test");
    let store = StoreHandle::open(&path).await.expect("open");
    let pragmas = store.pragmas().await.expect("pragmas");
    assert_eq!(pragmas.journal_mode, "wal");
    assert!(pragmas.foreign_keys);
    assert_eq!(pragmas.busy_timeout_ms, 5_000);
    assert_eq!(pragmas.synchronous, 1);
    assert_eq!(pragmas.user_version, 2);
    store
        .upsert_scope(&scope, temp.path(), "fp")
        .await
        .expect("scope");
    store
        .record_active_thread(&scope, "thread-1")
        .await
        .expect("thread");
    let turn_id = store
        .record_turn(turn("turn-1", TurnState::Starting))
        .await
        .expect("turn");
    store
        .put_attachment("hash-1", 3, "file")
        .await
        .expect("attachment");
    store
        .add_attachment_lease("hash-1", turn_id)
        .await
        .expect("lease");
    assert!(matches!(
        store
            .register_inbound(&tenant, &event("event-1", "message-1"))
            .await
            .expect("inbound"),
        DedupOutcome::New(_)
    ));
    let queued = store
        .enqueue_outbox(outbox("outbox-1", "{}"))
        .await
        .expect("outbox");
    assert!(matches!(queued, OutboxEnqueue::New(_)));
    store.shutdown().await.expect("shutdown");

    let store = StoreHandle::open(&path).await.expect("reopen");
    assert_eq!(store.pragmas().await.expect("pragmas").user_version, 2);
    assert!(store.scope_row(&scope).await.expect("scope").is_some());
    assert_eq!(
        store
            .active_thread(&scope)
            .await
            .expect("thread")
            .expect("active")
            .codex_thread_id,
        "thread-1"
    );
    assert!(store.turn_row(turn_id).await.expect("turn").is_some());
    assert!(
        store
            .attachment_row("hash-1")
            .await
            .expect("attachment")
            .is_some()
    );
    assert_eq!(
        store
            .attachment_leases("hash-1")
            .await
            .expect("leases")
            .len(),
        1
    );
    assert_eq!(
        store
            .inbound_state(&tenant, "event-1")
            .await
            .expect("inbound"),
        Some(InboundEventState::Received)
    );
    assert!(store.outbox_row(1).await.expect("outbox").is_some());
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn rejects_future_schema_versions_without_mutating_the_database() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("future.sqlite");
    {
        let connection = rusqlite::Connection::open(&path).expect("seed");
        connection
            .pragma_update(None, "user_version", 3_u32)
            .expect("version");
    }
    assert!(matches!(
        StoreHandle::open(&path).await,
        Err(StoreError::Migration { .. })
    ));
    let connection = rusqlite::Connection::open(&path).expect("inspect");
    let version: u32 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .expect("version");
    assert_eq!(version, 3);
}

#[tokio::test]
async fn file_store_rejects_a_second_live_open_until_the_writer_exits() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("store.sqlite");
    let first = StoreHandle::open(&path).await.expect("first open");
    let clone = first.clone();
    drop(clone);
    assert!(matches!(
        StoreHandle::open(&path).await,
        Err(StoreError::AlreadyOpen)
    ));
    first.shutdown().await.expect("shutdown");
    StoreHandle::open(&path)
        .await
        .expect("reservation releases with writer lifecycle")
        .shutdown()
        .await
        .expect("shutdown");
}

#[cfg(unix)]
#[tokio::test]
async fn file_store_rejects_symlink_and_hard_link_aliases() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("store.sqlite");
    let link = temp.path().join("store-link.sqlite");
    let hard_link = temp.path().join("store-hard-link.sqlite");
    let first = StoreHandle::open(&path).await.expect("open");
    std::os::unix::fs::symlink(&path, &link).expect("symlink");
    std::fs::hard_link(&path, &hard_link).expect("hard link");
    assert!(matches!(
        StoreHandle::open(&link).await,
        Err(StoreError::AlreadyOpen)
    ));
    assert!(matches!(
        StoreHandle::open(&hard_link).await,
        Err(StoreError::AlreadyOpen)
    ));
    first.shutdown().await.expect("shutdown");
}

#[cfg(unix)]
#[tokio::test]
async fn concurrent_dangling_symlink_and_target_allow_one_open() {
    let temp = tempdir().expect("tempdir");
    let target = temp.path().join("target.sqlite");
    let alias = temp.path().join("alias.sqlite");
    std::os::unix::fs::symlink(&target, &alias).expect("dangling symlink");
    let (left, right) = tokio::join!(StoreHandle::open(&target), StoreHandle::open(&alias));
    let successful = [left, right]
        .into_iter()
        .filter_map(Result::ok)
        .collect::<Vec<_>>();
    assert_eq!(successful.len(), 1);
    successful
        .into_iter()
        .next()
        .expect("one")
        .shutdown()
        .await
        .expect("shutdown");
}

#[tokio::test]
async fn dedup_transitions_and_ttl_are_fail_closed() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    let tenant = tenant_namespace("cli_dedup_test");
    let first = event("event-1", "message-1");
    assert!(matches!(
        store.register_inbound(&tenant, &first).await.expect("new"),
        DedupOutcome::New(_)
    ));
    assert!(matches!(
        store
            .register_inbound(&tenant, &first)
            .await
            .expect("duplicate"),
        DedupOutcome::ReplayReceived(_)
    ));
    assert!(matches!(
        store
            .register_inbound(&tenant, &event("event-2", "message-1"))
            .await
            .expect("same-message replay"),
        DedupOutcome::ReplayReceived(_)
    ));
    let started = store
        .begin_turn_and_claim_inbound(
            turn("dedup-turn", TurnState::Starting),
            &[InboundKey::new(tenant.clone(), "event-1".to_owned())],
        )
        .await
        .expect("claim");
    let BeginTurnOutcome::Started { turn_row_id, .. } = started else {
        panic!("received row starts a turn")
    };
    store
        .set_turn_state(turn_row_id, TurnState::Running, Some("codex-dedup"))
        .await
        .expect("running");
    store
        .resolve_turn_and_finish_inbound_batch(
            turn_row_id,
            TurnResolution::Completed,
            InboundTerminal::Completed,
        )
        .await
        .expect("complete");
    assert_eq!(
        store
            .sweep_inbound(i64::MAX, u32::MAX)
            .await
            .expect("sweep"),
        1
    );
    assert_eq!(
        store.inbound_state(&tenant, "event-2").await.expect("live"),
        None
    );
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn only_starting_turns_can_be_created_and_turn_transitions_are_checked() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    assert!(matches!(
        store
            .record_turn(turn("invalid", TurnState::Completed))
            .await,
        Err(StoreError::InvalidTransition { .. })
    ));
    let id = store
        .record_turn(turn("valid", TurnState::Starting))
        .await
        .expect("starting");
    store
        .set_turn_state(id, TurnState::Running, Some("codex-turn"))
        .await
        .expect("running");
    store
        .set_turn_state(id, TurnState::Uncertain, None)
        .await
        .expect("uncertain");
    assert_eq!(store.uncertain_turns().await.expect("uncertain").len(), 1);
    store
        .set_turn_state(id, TurnState::Completed, None)
        .await
        .expect("resolved");
    assert!(store.uncertain_turns().await.expect("resolved").is_empty());
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn live_turn_recovery_has_transactional_count_and_byte_bounds() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    for number in 0..STORE_RECOVERY_TURN_MAX_ROWS {
        store
            .record_turn(turn(&format!("turn-{number}"), TurnState::Starting))
            .await
            .expect("fits count cap");
    }
    assert!(matches!(
        store
            .record_turn(turn("overflow", TurnState::Starting))
            .await,
        Err(StoreError::CapacityExceeded { .. })
    ));
    let recovery = store.uncertain_turns().await.expect("bounded recovery");
    assert_eq!(recovery.len(), STORE_RECOVERY_TURN_MAX_ROWS);
    let recovery_bytes: usize = recovery
        .iter()
        .map(|row| {
            row.scope_key.len()
                + row.client_message_id.len()
                + row.codex_thread_id.as_deref().unwrap_or_default().len()
                + row.codex_turn_id.as_deref().unwrap_or_default().len()
        })
        .sum();
    assert!(recovery_bytes <= STORE_RECOVERY_TURN_MAX_BYTES);
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn live_turn_recovery_rejects_byte_overflow_before_count_overflow() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    let identifier = "x".repeat(400 * 1024);
    store
        .record_turn(turn(&format!("{identifier}-one"), TurnState::Starting))
        .await
        .expect("first fits");
    store
        .record_turn(turn(&format!("{identifier}-two"), TurnState::Starting))
        .await
        .expect("second fits");
    assert!(matches!(
        store
            .record_turn(turn(&format!("{identifier}-three"), TurnState::Starting))
            .await,
        Err(StoreError::CapacityExceeded { .. })
    ));
    assert!(
        store.uncertain_turns().await.expect("bounded rows").len() < STORE_RECOVERY_TURN_MAX_ROWS
    );
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn outbox_is_idempotent_claimed_once_and_has_explicit_crash_recovery() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    let original = match store
        .enqueue_outbox(outbox("key", "first"))
        .await
        .expect("enqueue")
    {
        OutboxEnqueue::New(row) => row,
        OutboxEnqueue::Duplicate(_) => panic!("new"),
    };
    let duplicate = store
        .enqueue_outbox(outbox("key", "different-content"))
        .await
        .expect("duplicate");
    assert!(matches!(duplicate, OutboxEnqueue::Duplicate(ref row) if row.payload_json == "first"));
    let (left, right) = tokio::join!(
        store.claim_outbox_batch(i64::MAX, 1),
        store.claim_outbox_batch(i64::MAX, 1)
    );
    let claimed: Vec<_> = left
        .expect("left")
        .into_iter()
        .chain(right.expect("right"))
        .collect();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, original.id);
    assert_eq!(claimed[0].state, OutboxState::Sending);
    assert_eq!(store.recover_sending_outbox().await.expect("recover"), 1);
    assert_eq!(
        store
            .outbox_row(original.id)
            .await
            .expect("row")
            .expect("row")
            .state,
        OutboxState::UncertainDelivery
    );
    assert!(
        store
            .claim_outbox_batch(i64::MAX, 1)
            .await
            .expect("none")
            .is_empty()
    );
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn outbox_attempts_never_decrease_and_receipts_require_sending() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    let row = match store
        .enqueue_outbox(outbox("key", "body"))
        .await
        .expect("enqueue")
    {
        OutboxEnqueue::New(row) => row,
        OutboxEnqueue::Duplicate(_) => panic!("new"),
    };
    assert!(matches!(
        store.complete_outbox(row.id, "receipt").await,
        Err(StoreError::InvalidTransition { .. })
    ));
    store.claim_outbox_batch(i64::MAX, 1).await.expect("claim");
    store
        .fail_outbox(row.id, 1, 0, false)
        .await
        .expect("first failure");
    assert_eq!(
        store
            .outbox_row(row.id)
            .await
            .expect("row")
            .expect("row")
            .attempts,
        1
    );
    store.claim_outbox_batch(i64::MAX, 1).await.expect("claim");
    assert!(matches!(
        store.fail_outbox(row.id, 1, 0, false).await,
        Err(StoreError::InvalidTransition { .. })
    ));
    assert!(matches!(
        store.fail_outbox(row.id, 0, 0, false).await,
        Err(StoreError::InvalidTransition { .. })
    ));
    assert!(matches!(
        store.complete_outbox(row.id, "receipt").await,
        Ok(())
    ));
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn attachment_lease_foreign_key_cascades_follow_both_parents() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("store.sqlite");
    let store = StoreHandle::open(&path).await.expect("open");
    let first_turn = store
        .record_turn(turn("first", TurnState::Starting))
        .await
        .expect("turn");
    let second_turn = store
        .record_turn(turn("second", TurnState::Starting))
        .await
        .expect("turn");
    store
        .put_attachment("attachment-parent", 1, "file")
        .await
        .expect("attachment");
    store
        .put_attachment("turn-parent", 1, "file")
        .await
        .expect("attachment");
    store
        .add_attachment_lease("attachment-parent", first_turn)
        .await
        .expect("lease");
    store
        .add_attachment_lease("turn-parent", second_turn)
        .await
        .expect("lease");
    store.shutdown().await.expect("shutdown");
    let connection = rusqlite::Connection::open(&path).expect("connection");
    connection
        .execute_batch("PRAGMA foreign_keys = ON")
        .expect("foreign keys");
    connection
        .execute(
            "DELETE FROM attachments WHERE sha256 = ?1",
            ["attachment-parent"],
        )
        .expect("attachment cascade");
    connection
        .execute("DELETE FROM turns WHERE id = ?1", [second_turn])
        .expect("turn cascade");
    drop(connection);
    let store = StoreHandle::open(&path).await.expect("reopen");
    assert!(
        store
            .attachment_leases("attachment-parent")
            .await
            .expect("leases")
            .is_empty()
    );
    assert!(
        store
            .attachment_leases("turn-parent")
            .await
            .expect("leases")
            .is_empty()
    );
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn file_backed_sending_rows_become_uncertain_after_reopen_recovery() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("store.sqlite");
    let store = StoreHandle::open(&path).await.expect("open");
    let row = match store
        .enqueue_outbox(outbox("key", "body"))
        .await
        .expect("enqueue")
    {
        OutboxEnqueue::New(row) => row,
        OutboxEnqueue::Duplicate(_) => panic!("new"),
    };
    store.claim_outbox_batch(i64::MAX, 1).await.expect("claim");
    store.shutdown().await.expect("shutdown");
    let store = StoreHandle::open(&path).await.expect("reopen");
    assert_eq!(store.recover_sending_outbox().await.expect("recover"), 1);
    assert_eq!(
        store
            .outbox_row(row.id)
            .await
            .expect("row")
            .expect("row")
            .state,
        OutboxState::UncertainDelivery
    );
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn failed_migration_rolls_back_and_remains_reopenable() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("migration.sqlite");
    let connection = rusqlite::Connection::open(&path).expect("seed");
    connection
        .execute_batch("CREATE TABLE inbound_events (bad INTEGER); PRAGMA user_version = 0;")
        .expect("conflict");
    drop(connection);
    assert!(matches!(
        StoreHandle::open(&path).await,
        Err(StoreError::Migration { version: 1, .. })
    ));
    let connection = rusqlite::Connection::open(&path).expect("inspect");
    let version: u32 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .expect("version");
    assert_eq!(version, 0);
    let scopes: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'scopes'",
            [],
            |row| row.get(0),
        )
        .expect("schema");
    assert_eq!(scopes, 0);
}

#[tokio::test]
async fn attachment_leases_require_both_parents_and_protect_gc_deletion() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    assert!(store.add_attachment_lease("missing", 1).await.is_err());
    store
        .put_attachment("hash", 1, "file")
        .await
        .expect("attachment");
    assert!(store.add_attachment_lease("hash", 99).await.is_err());
    let turn_id = store
        .record_turn(turn("turn", TurnState::Starting))
        .await
        .expect("turn");
    store
        .add_attachment_lease("hash", turn_id)
        .await
        .expect("lease");
    assert!(!store.delete_attachment("hash").await.expect("protected"));
    store
        .set_turn_state(turn_id, TurnState::Failed, None)
        .await
        .expect("terminal");
    store
        .release_turn_attachment_leases(turn_id)
        .await
        .expect("release");
    assert!(store.delete_attachment("hash").await.expect("deleted"));
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn debug_never_contains_outbox_payload() {
    let row = outbox("key", "prompt-sentinel-must-not-leak");
    let debug = format!("{row:?}");
    assert!(!debug.contains("prompt-sentinel-must-not-leak"));
}

#[tokio::test]
async fn concurrent_typed_writes_are_serialized() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    let mut tasks = Vec::new();
    for number in 0..32 {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            store
                .enqueue_outbox(outbox(&format!("key-{number}"), "x"))
                .await
        }));
    }
    for task in tasks {
        task.await.expect("join").expect("write");
    }
    let depth = store.outbox_depth().await.expect("depth");
    assert_eq!(depth.pending, 32);
    assert_eq!(depth.queued_bytes, 32);
    let claimed = store.claim_outbox_batch(i64::MAX, 64).await.expect("claim");
    assert_eq!(claimed.len(), 32);
    assert_eq!(
        claimed
            .iter()
            .map(|row| row.id)
            .collect::<HashSet<_>>()
            .len(),
        32
    );
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn writer_channel_rejects_overflow_and_oversized_typed_inputs() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("store.sqlite");
    let store = StoreHandle::open(&path).await.expect("open");
    let oversized = "x".repeat(STORE_REQUEST_MAX_BYTES);
    assert!(matches!(
        store.enqueue_outbox(outbox(&oversized, "")).await,
        Err(StoreError::PayloadTooLarge { .. })
    ));

    let lock = rusqlite::Connection::open(&path).expect("lock connection");
    lock.execute_batch("PRAGMA busy_timeout = 5000; BEGIN IMMEDIATE")
        .expect("write lock");
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut queued = Vec::new();
    let mut queue_full = false;
    for number in 0..=(STORE_WRITER_CAPACITY + 1) {
        let mut future = Box::pin(store.enqueue_outbox(outbox(&format!("queued-{number}"), "x")));
        match future.as_mut().poll(&mut context) {
            Poll::Pending => queued.push(future),
            Poll::Ready(Err(StoreError::QueueFull)) => {
                queue_full = true;
                break;
            }
            Poll::Ready(other) => panic!("unexpected writer saturation result: {other:?}"),
        }
    }
    assert!(
        queue_full,
        "manually polled typed futures fill the writer channel"
    );
    lock.execute_batch("ROLLBACK").expect("release lock");
    drop(queued);
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn writer_byte_budget_and_cancelled_callers_release_permits_after_dequeue() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("store.sqlite");
    let store = StoreHandle::open(&path).await.expect("open");
    let lock = rusqlite::Connection::open(&path).expect("lock connection");
    lock.execute_batch("PRAGMA busy_timeout = 5000; BEGIN IMMEDIATE")
        .expect("write lock");
    let key_bytes = 256 * 1024;
    let key = "k".repeat(key_bytes);
    let request_bytes = key.len() + "im:oc_test".len() + "final".len() + 1;
    let permits = STORE_WRITER_BYTE_BUDGET / request_bytes;
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut cancelled = Box::pin(store.enqueue_outbox(outbox(&key, "x")));
    assert!(matches!(
        cancelled.as_mut().poll(&mut context),
        Poll::Pending
    ));
    drop(cancelled);
    let mut queued = Vec::new();
    for _ in 1..permits {
        let mut future = Box::pin(store.enqueue_outbox(outbox(&key, "x")));
        assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
        queued.push(future);
    }
    let mut overflow = Box::pin(store.enqueue_outbox(outbox(&key, "x")));
    assert!(matches!(
        overflow.as_mut().poll(&mut context),
        Poll::Ready(Err(StoreError::QueueFull))
    ));
    drop(overflow);
    lock.execute_batch("ROLLBACK").expect("release lock");
    drop(queued);
    store
        .enqueue_outbox(outbox("after-cancel", "x"))
        .await
        .expect("dequeued cancelled work released its permit");
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn archive_returns_the_thread_changed_by_this_call() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    let scope = ScopeKey::Chat("oc_test".to_owned());
    store
        .record_active_thread(&scope, "first")
        .await
        .expect("first");
    assert_eq!(
        store
            .archive_active_thread(&scope)
            .await
            .expect("archive")
            .expect("row")
            .codex_thread_id,
        "first"
    );
    store
        .record_active_thread(&scope, "second")
        .await
        .expect("second");
    assert_eq!(
        store
            .archive_active_thread(&scope)
            .await
            .expect("archive")
            .expect("row")
            .codex_thread_id,
        "second"
    );
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn scope_paths_are_redacted_and_non_utf8_paths_are_refused() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    let scope = ScopeKey::Chat("oc_scope".to_owned());
    store
        .upsert_scope(
            &scope,
            std::path::Path::new("/workspace/secret-project"),
            "fp",
        )
        .await
        .expect("scope");
    let debug = format!(
        "{:?}",
        store.scope_row(&scope).await.expect("row").expect("row")
    );
    assert!(!debug.contains("secret-project"));
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;

        let invalid = std::path::PathBuf::from(std::ffi::OsString::from_vec(vec![0xff]));
        assert!(matches!(
            store.upsert_scope(&scope, &invalid, "fp").await,
            Err(StoreError::InvalidPath { .. })
        ));
    }
    store.shutdown().await.expect("shutdown");
}

#[test]
fn durable_session_rows_redact_scope_and_workspace_values_from_debug() {
    let scope_key = "im:oc_sensitive:thread:omt_sensitive".to_owned();
    let thread_id = "codex-thread-sensitive".to_owned();
    let scope = ScopeRow {
        scope_key: scope_key.clone(),
        cwd: std::path::PathBuf::from("/workspace/secret-project"),
        policy_fingerprint: "secret-policy-fingerprint".to_owned(),
        updated_ms: 1,
    };
    let thread = ThreadRow {
        scope_key: scope_key.clone(),
        codex_thread_id: thread_id.clone(),
        status: ThreadStatus::Active,
        created_ms: 2,
        archived_ms: None,
    };
    let turn = TurnRow {
        id: 3,
        scope_key: scope_key.clone(),
        client_message_id: "client-message-sensitive".to_owned(),
        codex_thread_id: Some(thread_id.clone()),
        codex_turn_id: Some("codex-turn-sensitive".to_owned()),
        state: TurnState::Running,
        uncertain: false,
        created_ms: 4,
        updated_ms: 5,
        inbound_count: 1,
    };
    let new_turn = NewTurnRow {
        scope_key,
        client_message_id: "new-client-message-sensitive".to_owned(),
        codex_thread_id: Some(thread_id),
        state: TurnState::Starting,
    };

    for debug in [
        format!("{scope:?}"),
        format!("{thread:?}"),
        format!("{turn:?}"),
        format!("{new_turn:?}"),
    ] {
        assert!(
            !debug.contains("sensitive"),
            "leaked session value: {debug}"
        );
        assert!(!debug.contains("secret-project"), "leaked path: {debug}");
    }
}

#[tokio::test]
async fn outbox_enforces_aggregate_bytes_and_claim_batch_bytes() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    let payload = "x".repeat(STORE_OUTBOX_PAYLOAD_MAX_BYTES);
    let rows = STORE_OUTBOX_MAX_QUEUED_BYTES
        / u64::try_from(STORE_OUTBOX_PAYLOAD_MAX_BYTES).expect("payload bytes");
    for index in 0..rows {
        store
            .enqueue_outbox(outbox(&format!("key-{index}"), &payload))
            .await
            .expect("fits exact queue byte capacity");
    }
    assert!(matches!(
        store.enqueue_outbox(outbox("overflow", &payload)).await,
        Err(StoreError::CapacityExceeded { .. })
    ));
    let claimed = store
        .claim_outbox_batch(i64::MAX, u32::MAX)
        .await
        .expect("claim");
    let claimed_bytes: u64 = claimed.iter().map(|row| row.payload_bytes).sum();
    assert!(claimed_bytes <= u64::try_from(STORE_OUTBOX_CLAIM_MAX_BYTES).expect("claim bytes"));
    assert_eq!(
        claimed.len(),
        STORE_OUTBOX_CLAIM_MAX_BYTES / STORE_OUTBOX_PAYLOAD_MAX_BYTES
    );
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn outbox_debug_redacts_payload_routing_and_idempotency_values() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    let row = NewOutboxRow {
        idempotency_key: "sensitive-idempotency-key".to_owned(),
        scope_key: "im:oc_sensitive:thread:omt_sensitive".to_owned(),
        kind: "notice".to_owned(),
        payload_json: "{\"text\":\"sensitive-payload\"}".to_owned(),
        next_retry_ms: 0,
    };
    let new_debug = format!("{row:?}");
    assert!(!new_debug.contains("sensitive"));
    let persisted = store.enqueue_outbox(row).await.expect("enqueue");
    let persisted_debug = format!("{persisted:?}");
    assert!(!persisted_debug.contains("sensitive"));
    assert!(persisted_debug.contains("payload_bytes"));
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn recovery_is_strict_all_or_nothing_and_tenant_isolated() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("store.sqlite");
    let tenant = tenant_namespace("cli_recovery_test");
    let other = tenant_namespace("cli_recovery_other");
    let store = StoreHandle::open(&path).await.expect("open");
    store
        .register_inbound(&tenant, &event("event-b", "message-b"))
        .await
        .expect("b");
    store
        .register_inbound(&tenant, &event("event-a", "message-a"))
        .await
        .expect("a");
    store
        .register_inbound(&other, &event("event-other", "message-other"))
        .await
        .expect("other");
    store.shutdown().await.expect("shutdown before seed");
    let connection = rusqlite::Connection::open(&path).expect("seed ordering");
    connection
        .execute(
            "UPDATE inbound_events SET first_seen_ms = 10 WHERE event_id = 'event-b'",
            [],
        )
        .expect("seed first");
    connection
        .execute(
            "UPDATE inbound_events SET first_seen_ms = 20 WHERE event_id = 'event-a'",
            [],
        )
        .expect("seed second");
    drop(connection);
    let store = StoreHandle::open(&path).await.expect("reopen ordered");
    let recovered = store.recover_received(&tenant).await.expect("recover");
    assert_eq!(recovered.len(), 2);
    assert_eq!(recovered[0].event().event_id, "event-b");
    assert_eq!(recovered[1].event().event_id, "event-a");
    store.shutdown().await.expect("shutdown");

    let connection = rusqlite::Connection::open(&path).expect("mutate");
    let (row_id, payload): (i64, Vec<u8>) = connection
        .query_row(
            "SELECT rowid, payload_blob FROM inbound_events WHERE event_id = 'event-a'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("payload");
    let mut value: serde_json::Value = serde_json::from_slice(&payload).expect("json");
    value
        .as_object_mut()
        .expect("object")
        .insert("unexpected".to_owned(), serde_json::Value::Bool(true));
    let forged = serde_json::to_vec(&value).expect("encode");
    connection
        .execute(
            "UPDATE inbound_events SET payload_blob = ?2, payload_bytes = ?3 WHERE rowid = ?1",
            rusqlite::params![row_id, forged, i64::try_from(forged.len()).expect("length")],
        )
        .expect("forge strict payload");
    drop(connection);

    let store = StoreHandle::open(&path).await.expect("reopen");
    assert!(matches!(
        store.recover_received(&tenant).await,
        Err(StoreError::CorruptData { .. })
    ));
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn startup_and_begin_skip_reject_terminal_payload_and_field_corruption() {
    let temp = tempdir().expect("tempdir");
    for (index, field) in ["payload", "event_id", "message_id", "scope", "reason"]
        .into_iter()
        .enumerate()
    {
        let path = temp.path().join(format!("terminal-{field}.sqlite"));
        let (credentials, tenant, turn_row_id) =
            seed_accepted_file_store(&path, &format!("cli_terminal_shape_{index}")).await;
        let connection = rusqlite::Connection::open(&path).expect("open raw terminal store");
        connection
            .execute_batch(
                "PRAGMA foreign_keys = OFF;
                 DROP TRIGGER inbound_events_v2_shape_insert;
                 DROP TRIGGER inbound_events_v2_shape_update;",
            )
            .expect("disable terminal guards");
        connection
            .execute(
                "UPDATE turns SET state = 'failed', uncertain = 0 WHERE id = ?1",
                [turn_row_id],
            )
            .expect("forge resolved turn");
        connection
            .execute(
                "UPDATE inbound_events
                 SET state = 'rejected', rejection_reason = 'turn_failed',
                     payload_version = NULL, payload_blob = NULL, payload_bytes = 0
                 WHERE event_id = 'event-forged'",
                [],
            )
            .expect("forge valid terminal baseline");
        let forged_event_id = "x".repeat(4 * 1024 + 1);
        match field {
            "payload" => connection
                .execute(
                    "UPDATE inbound_events
                     SET payload_version = 1, payload_blob = X'78', payload_bytes = 1
                     WHERE event_id = 'event-forged'",
                    [],
                )
                .expect("forge terminal payload"),
            "event_id" => connection
                .execute(
                    "UPDATE inbound_events SET event_id = ?1 WHERE event_id = 'event-forged'",
                    [&forged_event_id],
                )
                .expect("forge event id"),
            "message_id" => connection
                .execute(
                    "UPDATE inbound_events SET message_id = ?1 WHERE event_id = 'event-forged'",
                    ["x".repeat(4 * 1024 + 1)],
                )
                .expect("forge message id"),
            "scope" => connection
                .execute(
                    "UPDATE inbound_events SET scope_key = ?1 WHERE event_id = 'event-forged'",
                    ["x".repeat(12 * 1024 + 1)],
                )
                .expect("forge scope"),
            "reason" => connection
                .execute(
                    "UPDATE inbound_events SET rejection_reason = ?1
                     WHERE event_id = 'event-forged'",
                    ["x".repeat(129)],
                )
                .expect("forge reason"),
            _ => unreachable!(),
        };
        drop(connection);
        let event_id = if field == "event_id" {
            forged_event_id.as_str()
        } else {
            "event-forged"
        };
        assert_forged_store_fails_recovery_and_skip(&path, &credentials, &tenant, event_id).await;
    }
}

#[tokio::test]
async fn startup_and_begin_skip_reject_forged_turn_associations() {
    let temp = tempdir().expect("tempdir");
    for (index, association) in [
        "accepted-resolved",
        "accepted-cross-scope",
        "accepted-missing-turn",
        "unresolved-count-mismatch",
        "terminal-live",
        "terminal-cross-scope",
        "terminal-wrong-outcome",
        "resolved-marker-overflow",
    ]
    .into_iter()
    .enumerate()
    {
        let path = temp
            .path()
            .join(format!("association-{association}.sqlite"));
        let (credentials, tenant, turn_row_id) =
            seed_accepted_file_store(&path, &format!("cli_association_{index}")).await;
        let connection = rusqlite::Connection::open(&path).expect("open raw association store");
        connection
            .execute_batch(
                "PRAGMA foreign_keys = OFF;
                 DROP TRIGGER inbound_events_v2_shape_insert;
                 DROP TRIGGER inbound_events_v2_shape_update;",
            )
            .expect("disable association guards");
        forge_turn_association(&connection, turn_row_id, association);
        drop(connection);
        assert_forged_store_fails_recovery_and_skip(&path, &credentials, &tenant, "event-forged")
            .await;
    }
}

#[tokio::test]
async fn legal_legacy_v1_terminal_rows_migrate_prepare_and_sweep() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("legacy-terminal.sqlite");
    let connection = rusqlite::Connection::open(&path).expect("open legacy store");
    connection
        .execute_batch(lark_codex_bridge::store::schema::MIGRATIONS[0].sql)
        .expect("apply migration one");
    connection
        .execute_batch(
            "INSERT INTO inbound_events
             (tenant,event_id,message_id,scope_key,state,first_seen_ms,updated_ms,rejection_reason)
             VALUES
             ('legacy-tenant','legacy-completed','legacy-message-1','im:legacy','completed',1,1,NULL),
             ('legacy-tenant','legacy-rejected','legacy-message-2','im:legacy','rejected',1,1,'legacy_reason');
             PRAGMA user_version = 1;",
        )
        .expect("seed legacy terminals");
    drop(connection);

    let credentials = credentials_for("cli_legacy_terminal_prepare");
    let store = StoreHandle::open(&path)
        .await
        .expect("migrate legacy store");
    let _runtime = DurableIntake::prepare(store.clone(), &credentials)
        .await
        .expect("legacy terminal rows do not block startup");
    assert_eq!(
        store
            .sweep_inbound(i64::MAX, 2)
            .await
            .expect("sweep legacy"),
        2
    );
    store.shutdown().await.expect("shutdown legacy store");
}

#[tokio::test]
async fn atomic_rejection_notice_validates_scope_and_duplicate_identity() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    let tenant = tenant_namespace("cli_notice_identity");

    let success = event("event-notice-success", "message-notice-success");
    store
        .register_inbound(&tenant, &success)
        .await
        .expect("register success");
    let success_key = InboundKey::new(tenant.clone(), success.event_id.clone());
    let success_notice = outbox("notice-success", "notice-body");
    assert_eq!(
        store
            .reject_received_and_enqueue_notice(
                &success_key,
                InboundRejectionKind::Policy,
                success_notice.clone(),
            )
            .await
            .expect("atomic success"),
        InboundDisposition::Rejected
    );
    assert_eq!(
        store
            .reject_received_and_enqueue_notice(
                &success_key,
                InboundRejectionKind::Policy,
                success_notice,
            )
            .await
            .expect("identical retry"),
        InboundDisposition::AlreadyRejected
    );
    assert_eq!(store.outbox_depth().await.expect("depth").pending, 1);

    let mismatch = event("event-notice-mismatch", "message-notice-mismatch");
    store
        .register_inbound(&tenant, &mismatch)
        .await
        .expect("register mismatch");
    let mismatch_key = InboundKey::new(tenant.clone(), mismatch.event_id.clone());
    let mut wrong_scope = outbox("notice-wrong-scope", "body");
    wrong_scope.scope_key = "im:another-chat".to_owned();
    assert!(matches!(
        store
            .reject_received_and_enqueue_notice(
                &mismatch_key,
                InboundRejectionKind::Policy,
                wrong_scope,
            )
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    assert_eq!(
        store
            .inbound_state(&tenant, &mismatch.event_id)
            .await
            .expect("mismatch state"),
        Some(InboundEventState::Received)
    );

    let conflict = event("event-notice-conflict", "message-notice-conflict");
    store
        .register_inbound(&tenant, &conflict)
        .await
        .expect("register conflict");
    store
        .enqueue_outbox(outbox("notice-conflict", "original-body"))
        .await
        .expect("seed conflicting idempotency key");
    assert!(matches!(
        store
            .reject_received_and_enqueue_notice(
                &InboundKey::new(tenant.clone(), conflict.event_id.clone()),
                InboundRejectionKind::Policy,
                outbox("notice-conflict", "different-body"),
            )
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    assert_eq!(
        store
            .inbound_state(&tenant, &conflict.event_id)
            .await
            .expect("conflict state"),
        Some(InboundEventState::Received)
    );
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn atomic_rejection_backfills_and_validates_a_bare_rejection_notice() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    let tenant = tenant_namespace("cli_notice_backfill");
    let inbound = event("event-notice-backfill", "message-notice-backfill");
    store
        .register_inbound(&tenant, &inbound)
        .await
        .expect("register");
    let key = InboundKey::new(tenant, inbound.event_id);
    assert_eq!(
        store
            .reject_received(&key, InboundRejectionKind::Policy)
            .await
            .expect("bare rejection"),
        InboundDisposition::Rejected
    );
    assert_eq!(store.outbox_depth().await.expect("empty depth").pending, 0);

    let notice = outbox("notice-backfill", "original-body");
    for _ in 0..2 {
        assert_eq!(
            store
                .reject_received_and_enqueue_notice(
                    &key,
                    InboundRejectionKind::Policy,
                    notice.clone(),
                )
                .await
                .expect("backfill or identical retry"),
            InboundDisposition::AlreadyRejected
        );
        assert_eq!(store.outbox_depth().await.expect("depth").pending, 1);
    }

    let mut wrong_body = notice.clone();
    wrong_body.payload_json = "changed-body".to_owned();
    assert!(matches!(
        store
            .reject_received_and_enqueue_notice(&key, InboundRejectionKind::Policy, wrong_body,)
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    let mut wrong_scope = notice.clone();
    wrong_scope.scope_key = "im:wrong-scope".to_owned();
    assert!(matches!(
        store
            .reject_received_and_enqueue_notice(&key, InboundRejectionKind::Policy, wrong_scope,)
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    let claimed = store
        .claim_outbox_batch(i64::MAX, 2)
        .await
        .expect("claim original notice");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].scope_key, notice.scope_key);
    assert_eq!(claimed[0].payload_json, notice.payload_json);
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn atomic_rejection_notice_rolls_back_at_real_outbox_byte_capacity() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    let payload = "x".repeat(STORE_OUTBOX_PAYLOAD_MAX_BYTES);
    let rows = STORE_OUTBOX_MAX_QUEUED_BYTES
        / u64::try_from(STORE_OUTBOX_PAYLOAD_MAX_BYTES).expect("payload bytes");
    for index in 0..rows {
        store
            .enqueue_outbox(outbox(&format!("capacity-{index}"), &payload))
            .await
            .expect("fill byte capacity");
    }
    let tenant = tenant_namespace("cli_notice_capacity");
    let inbound = event("event-notice-capacity", "message-notice-capacity");
    store
        .register_inbound(&tenant, &inbound)
        .await
        .expect("register");
    assert!(matches!(
        store
            .reject_received_and_enqueue_notice(
                &InboundKey::new(tenant.clone(), inbound.event_id.clone()),
                InboundRejectionKind::Overloaded,
                outbox("capacity-overflow-notice", "x"),
            )
            .await,
        Err(StoreError::CapacityExceeded { .. })
    ));
    assert_eq!(
        store
            .inbound_state(&tenant, &inbound.event_id)
            .await
            .expect("rollback state"),
        Some(InboundEventState::Received)
    );
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn atomic_rejection_notice_and_turn_claim_have_one_consistent_winner() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    let tenant = tenant_namespace("cli_notice_claim_race");
    let inbound = event("event-notice-race", "message-notice-race");
    store
        .register_inbound(&tenant, &inbound)
        .await
        .expect("register");
    let key = InboundKey::new(tenant, inbound.event_id);
    let claim_keys = [key.clone()];
    let (rejected, claimed) = tokio::join!(
        store.reject_received_and_enqueue_notice(
            &key,
            InboundRejectionKind::Policy,
            outbox("race-notice", "body"),
        ),
        store.begin_turn_and_claim_inbound(turn("race-turn", TurnState::Starting), &claim_keys)
    );
    match (
        rejected.expect("rejection result"),
        claimed.expect("claim result"),
    ) {
        (InboundDisposition::Rejected, BeginTurnOutcome::NoReceived { skipped }) => {
            assert_eq!(skipped.len(), 1);
            assert_eq!(store.outbox_depth().await.expect("depth").pending, 1);
        }
        (InboundDisposition::AlreadyClaimed { .. }, BeginTurnOutcome::Started { claimed, .. }) => {
            assert_eq!(claimed.len(), 1);
            assert_eq!(store.outbox_depth().await.expect("depth").pending, 0);
        }
        other => panic!("inconsistent race outcome: {other:?}"),
    }
    store.shutdown().await.expect("shutdown");
}

// Darwin rejects this invalid UTF-8 filename before SQLite or the store can
// observe it. Other Unix platforms exercise the byte-preserving sidecar path.
#[cfg(all(unix, not(target_vendor = "apple")))]
#[tokio::test]
async fn database_sidecars_are_retightened_for_non_utf8_paths() {
    use std::os::unix::ffi::OsStringExt;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let temp = tempdir().expect("tempdir");
    let mut name = b"private-".to_vec();
    name.push(0xff);
    name.extend_from_slice(b".sqlite");
    let path = temp.path().join(std::ffi::OsString::from_vec(name));
    let store = StoreHandle::open(&path).await.expect("open non-UTF-8");
    let mut wal = path.as_os_str().to_os_string();
    wal.push("-wal");
    let mut shm = path.as_os_str().to_os_string();
    shm.push("-shm");
    for sidecar in [&wal, &shm] {
        let sidecar = std::path::Path::new(sidecar);
        assert!(sidecar.exists(), "SQLite created the sidecar");
        std::fs::set_permissions(sidecar, std::fs::Permissions::from_mode(0o666))
            .expect("loosen sidecar");
    }
    store.pragmas().await.expect("request retightens sidecars");
    for sidecar in [&wal, &shm] {
        assert_eq!(
            std::fs::metadata(std::path::Path::new(sidecar))
                .expect("metadata")
                .mode()
                & 0o777,
            0o600
        );
    }
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn reject_notice_rolls_back_and_rejection_is_idempotent() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    let tenant = tenant_namespace("cli_reject_test");
    let inbound = event("event-reject", "message-reject");
    store
        .register_inbound(&tenant, &inbound)
        .await
        .expect("register");
    let key = InboundKey::new(tenant.clone(), inbound.event_id.clone());
    let mut notice = outbox("reject-notice", "");
    notice.payload_json = "x".repeat(STORE_OUTBOX_PAYLOAD_MAX_BYTES + 1);
    assert!(matches!(
        store
            .reject_received_and_enqueue_notice(&key, InboundRejectionKind::Overloaded, notice,)
            .await,
        Err(StoreError::PayloadTooLarge { .. })
    ));
    assert_eq!(
        store.recover_received(&tenant).await.expect("replay").len(),
        1
    );
    assert_eq!(
        store
            .reject_received(&key, InboundRejectionKind::Overloaded)
            .await
            .expect("reject"),
        InboundDisposition::Rejected
    );
    assert_eq!(
        store
            .reject_received(&key, InboundRejectionKind::Overloaded)
            .await
            .expect("idempotent"),
        InboundDisposition::AlreadyRejected
    );
    assert!(
        store
            .recover_received(&tenant)
            .await
            .expect("none")
            .is_empty()
    );
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn runtime_turn_terminalization_is_combined_and_survives_marker_sweep() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    let tenant = tenant_namespace("cli_sweep_test");
    let inbound = event("event-sweep", "message-sweep");
    store
        .register_inbound(&tenant, &inbound)
        .await
        .expect("register");
    let started = store
        .begin_turn_and_claim_inbound(
            turn("sweep-turn", TurnState::Starting),
            &[InboundKey::new(tenant, inbound.event_id)],
        )
        .await
        .expect("begin");
    let BeginTurnOutcome::Started { turn_row_id, .. } = started else {
        panic!("started")
    };
    store
        .set_turn_state(turn_row_id, TurnState::Running, Some("codex-sweep"))
        .await
        .expect("running");
    assert!(matches!(
        store
            .set_turn_state(turn_row_id, TurnState::Completed, None)
            .await,
        Err(StoreError::InvalidTransition { .. })
    ));
    store
        .resolve_turn_and_finish_inbound_batch(
            turn_row_id,
            TurnResolution::Completed,
            InboundTerminal::Completed,
        )
        .await
        .expect("resolve");
    assert_eq!(store.sweep_inbound(i64::MAX, 1).await.expect("sweep"), 1);
    assert_eq!(
        store
            .resolve_turn_and_finish_inbound_batch(
                turn_row_id,
                TurnResolution::Completed,
                InboundTerminal::Completed,
            )
            .await
            .expect("already resolved after sweep"),
        ResolveTurnOutcome::AlreadyResolved { inbound_rows: 1 }
    );
    assert_eq!(
        store
            .turn_row(turn_row_id)
            .await
            .expect("turn")
            .expect("turn")
            .inbound_count,
        1
    );
    store.shutdown().await.expect("shutdown");
}

#[cfg(unix)]
#[tokio::test]
async fn file_store_tightens_existing_database_permissions() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("private.sqlite");
    std::fs::write(&path, []).expect("seed file");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o666)).expect("loosen");
    let store = StoreHandle::open(&path).await.expect("open");
    assert_eq!(
        std::fs::metadata(&path).expect("metadata").mode() & 0o777,
        0o600
    );
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn migration_two_triggers_reject_null_payload_shape_on_insert_and_update() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("trigger.sqlite");
    StoreHandle::open(&path)
        .await
        .expect("migrate")
        .shutdown()
        .await
        .expect("shutdown");
    let connection = rusqlite::Connection::open(&path).expect("open raw");
    connection
        .execute_batch("PRAGMA foreign_keys = ON")
        .expect("foreign keys");
    assert!(
        connection
            .execute(
                "INSERT INTO inbound_events
                 (tenant,event_id,message_id,scope_key,state,first_seen_ms,updated_ms,
                  payload_version,payload_blob,payload_bytes,turn_row_id)
                 VALUES ('tenant','null-insert','message','im:chat','received',1,1,
                         NULL,X'78',1,NULL)",
                [],
            )
            .is_err(),
        "received insert with SQL NULL payload columns must be rejected"
    );

    let tenant = tenant_namespace("cli_trigger_test");
    drop(connection);
    let store = StoreHandle::open(&path).await.expect("typed reopen");
    store
        .register_inbound(&tenant, &event("valid-trigger", "valid-message"))
        .await
        .expect("valid row");
    store.shutdown().await.expect("shutdown");
    let connection = rusqlite::Connection::open(&path).expect("raw reopen");
    assert!(
        connection
            .execute(
                "UPDATE inbound_events
                 SET payload_version = NULL
                 WHERE event_id = 'valid-trigger'",
                [],
            )
            .is_err(),
        "received update with SQL NULL payload columns must be rejected"
    );
}

#[tokio::test]
async fn resolved_uncertain_runtime_turn_is_not_live_and_can_be_refined() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    let tenant = tenant_namespace("cli_uncertain_test");
    let inbound = event("event-uncertain", "message-uncertain");
    store
        .register_inbound(&tenant, &inbound)
        .await
        .expect("register");
    let begun = store
        .begin_turn_and_claim_inbound(
            turn("uncertain-runtime", TurnState::Starting),
            &[InboundKey::new(tenant, inbound.event_id)],
        )
        .await
        .expect("begin");
    let BeginTurnOutcome::Started { turn_row_id, .. } = begun else {
        panic!("started")
    };
    store
        .set_turn_state(turn_row_id, TurnState::Running, Some("codex-uncertain"))
        .await
        .expect("running");
    store
        .resolve_turn_and_finish_inbound_batch(
            turn_row_id,
            TurnResolution::Uncertain,
            InboundTerminal::Rejected,
        )
        .await
        .expect("resolve uncertain");
    assert!(
        store
            .uncertain_turns()
            .await
            .expect("live recovery")
            .is_empty(),
        "resolved uncertainty must not consume live recovery capacity"
    );
    assert_eq!(
        store
            .resolve_turn_and_finish_inbound_batch(
                turn_row_id,
                TurnResolution::Failed,
                InboundTerminal::Rejected,
            )
            .await
            .expect("manual refinement"),
        ResolveTurnOutcome::Resolved { inbound_rows: 1 }
    );
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn debug_redacts_inbound_sender_resource_keys_and_content() {
    use lark_codex_bridge::lark::api::ResourceKind;
    use lark_codex_bridge::lark::bridge::QueuedInboundEvent;
    use lark_codex_bridge::lark::normalize::ResourceDesc;
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    let mut sensitive = event("event-debug", "message-debug");
    sensitive.sender_id = "sender-sentinel".to_owned();
    sensitive.event_id = "event-id-sentinel".to_owned();
    sensitive.message_id = "message-id-sentinel".to_owned();
    sensitive.chat_id = "chat-id-sentinel".to_owned();
    sensitive.thread_id = Some("thread-id-sentinel".to_owned());
    sensitive.root_id = Some("root-id-sentinel".to_owned());
    sensitive.reply_to_message_id = Some("reply-id-sentinel".to_owned());
    sensitive.chat_type = ChatMode::Topic;
    sensitive.scope = ScopeKey::Thread(
        "chat-id-sentinel".to_owned(),
        "thread-id-sentinel".to_owned(),
    );
    sensitive.text = "text-sentinel".to_owned();
    sensitive.resources.push(ResourceDesc {
        kind: ResourceKind::File,
        key: "resource-key-sentinel".to_owned(),
    });
    sensitive.message_type = "message-type-sentinel".to_owned();
    let permit = Arc::new(Semaphore::new(1))
        .try_acquire_owned()
        .expect("permit");
    let queued = QueuedInboundEvent {
        event: sensitive.clone(),
        permit,
    };
    let key = InboundKey::new(
        tenant_namespace("cli_debug_key"),
        "event-id-sentinel".to_owned(),
    );
    for debug in [
        format!("{sensitive:?}"),
        format!("{queued:?}"),
        format!("{key:?}"),
    ] {
        for sentinel in [
            "sender-sentinel",
            "text-sentinel",
            "resource-key-sentinel",
            "message-type-sentinel",
            "event-id-sentinel",
            "message-id-sentinel",
            "chat-id-sentinel",
            "thread-id-sentinel",
            "root-id-sentinel",
            "reply-id-sentinel",
        ] {
            assert!(
                !debug.contains(sentinel),
                "debug leaked {sentinel}: {debug}"
            );
        }
    }
}

#[tokio::test]
async fn combined_resolve_rejects_extra_linked_markers_and_scope_mismatch() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("resolve-integrity.sqlite");
    let tenant = tenant_namespace("cli_resolve_integrity");
    let store = StoreHandle::open(&path).await.expect("open");
    store
        .register_inbound(&tenant, &event("event-claimed", "message-claimed"))
        .await
        .expect("claimed row");
    store
        .register_inbound(&tenant, &event("event-extra", "message-extra"))
        .await
        .expect("extra row");
    let begun = store
        .begin_turn_and_claim_inbound(
            turn("integrity-turn", TurnState::Starting),
            &[InboundKey::new(tenant.clone(), "event-claimed".to_owned())],
        )
        .await
        .expect("begin");
    let BeginTurnOutcome::Started { turn_row_id, .. } = begun else {
        panic!("started")
    };
    store.shutdown().await.expect("shutdown");

    let connection = rusqlite::Connection::open(&path).expect("corrupt raw");
    connection
        .execute_batch(
            "DROP TRIGGER inbound_events_v2_shape_update;
             PRAGMA foreign_keys = ON;",
        )
        .expect("disable shape trigger");
    connection
        .execute(
            "UPDATE inbound_events
             SET state = 'completed', turn_row_id = ?1,
                 payload_version = NULL, payload_blob = NULL, payload_bytes = 0
             WHERE event_id = 'event-extra'",
            [turn_row_id],
        )
        .expect("forge extra terminal marker");
    connection
        .execute(
            "UPDATE turns SET scope_key = 'im:different-scope' WHERE id = ?1",
            [turn_row_id],
        )
        .expect("forge scope mismatch");
    drop(connection);

    let store = StoreHandle::open(&path).await.expect("reopen");
    store
        .set_turn_state(turn_row_id, TurnState::Running, Some("codex-integrity"))
        .await
        .expect("running");
    assert!(matches!(
        store
            .resolve_turn_and_finish_inbound_batch(
                turn_row_id,
                TurnResolution::Completed,
                InboundTerminal::Completed,
            )
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    store.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn inbound_writer_permits_account_for_the_captured_event_and_payload() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("inbound-writer-budget.sqlite");
    let store = StoreHandle::open(&path).await.expect("open");
    let tenant = tenant_namespace("cli_inbound_writer_budget");
    let mut large = event("event-large", "message-large");
    large.text = "x".repeat(256 * 1024);

    let lock = rusqlite::Connection::open(&path).expect("lock connection");
    lock.execute_batch("PRAGMA busy_timeout = 5000; BEGIN IMMEDIATE")
        .expect("write lock");
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut pending = Vec::new();
    let mut byte_budget_full = false;
    for _ in 0..64 {
        let mut future = Box::pin(store.register_inbound(&tenant, &large));
        match future.as_mut().poll(&mut context) {
            Poll::Pending => pending.push(future),
            Poll::Ready(Err(StoreError::QueueFull)) => {
                byte_budget_full = true;
                break;
            }
            Poll::Ready(other) => panic!("unexpected inbound budget result: {other:?}"),
        }
    }
    assert!(
        byte_budget_full,
        "captured normalized events and their serialized payloads consume writer permits"
    );
    lock.execute_batch("ROLLBACK").expect("release lock");
    drop(pending);
    store.shutdown().await.expect("shutdown");
}
