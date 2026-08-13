use std::collections::HashSet;
use std::future::Future;
use std::task::{Context, Poll, Waker};

use lark_codex_bridge::lark::api::ChatMode;
use lark_codex_bridge::lark::normalize::{InboundEvent, ScopeKey};
use lark_codex_bridge::limits::{
    STORE_OUTBOX_CLAIM_MAX_BYTES, STORE_OUTBOX_MAX_QUEUED_BYTES, STORE_OUTBOX_PAYLOAD_MAX_BYTES,
    STORE_RECOVERY_TURN_MAX_BYTES, STORE_RECOVERY_TURN_MAX_ROWS, STORE_REQUEST_MAX_BYTES,
    STORE_WRITER_BYTE_BUDGET, STORE_WRITER_CAPACITY,
};
use lark_codex_bridge::store::{
    DedupOutcome, InboundEventState, NewOutboxRow, NewTurnRow, OutboxEnqueue, OutboxState,
    StoreError, StoreHandle, TurnState,
};
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

#[tokio::test]
async fn file_store_applies_pragmas_and_persists_every_typed_table() {
    let temp = tempdir().expect("tempdir");
    let path = temp.path().join("store.sqlite");
    let scope = ScopeKey::Chat("oc_test".to_owned());
    let store = StoreHandle::open(&path).await.expect("open");
    let pragmas = store.pragmas().await.expect("pragmas");
    assert_eq!(pragmas.journal_mode, "wal");
    assert!(pragmas.foreign_keys);
    assert_eq!(pragmas.busy_timeout_ms, 5_000);
    assert_eq!(pragmas.synchronous, 1);
    assert_eq!(pragmas.user_version, 1);
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
    assert_eq!(
        store
            .register_inbound("tenant", &event("event-1", "message-1"))
            .await
            .expect("inbound"),
        DedupOutcome::New
    );
    let queued = store
        .enqueue_outbox(outbox("outbox-1", "{}"))
        .await
        .expect("outbox");
    assert!(matches!(queued, OutboxEnqueue::New(_)));
    store.shutdown().await.expect("shutdown");

    let store = StoreHandle::open(&path).await.expect("reopen");
    assert_eq!(store.pragmas().await.expect("pragmas").user_version, 1);
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
            .inbound_state("tenant", "event-1")
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
            .pragma_update(None, "user_version", 2_u32)
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
    assert_eq!(version, 2);
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

#[tokio::test]
async fn dedup_transitions_and_ttl_are_fail_closed() {
    let store = StoreHandle::open_in_memory().await.expect("open");
    let first = event("event-1", "message-1");
    assert_eq!(
        store.register_inbound("tenant", &first).await.expect("new"),
        DedupOutcome::New
    );
    assert_eq!(
        store
            .register_inbound("tenant", &first)
            .await
            .expect("duplicate"),
        DedupOutcome::Duplicate {
            state: InboundEventState::Received
        }
    );
    assert_eq!(
        store
            .register_inbound("tenant", &event("event-2", "message-1"))
            .await
            .expect("new event"),
        DedupOutcome::New
    );
    assert!(matches!(
        store
            .transition_inbound("tenant", "event-1", InboundEventState::Completed, None)
            .await,
        Err(StoreError::InvalidTransition { .. })
    ));
    store
        .transition_inbound("tenant", "event-1", InboundEventState::Accepted, None)
        .await
        .expect("accept");
    store
        .transition_inbound("tenant", "event-1", InboundEventState::Completed, None)
        .await
        .expect("complete");
    assert_eq!(store.sweep_inbound(i64::MAX).await.expect("sweep"), 1);
    assert_eq!(
        store
            .inbound_state("tenant", "event-2")
            .await
            .expect("live"),
        Some(InboundEventState::Received)
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
