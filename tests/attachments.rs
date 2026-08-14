//! Deterministic, offline tests for the content-addressed attachment cache:
//! safe writes, leases, GC, and startup reconciliation.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures_util::future::BoxFuture;
use lark_codex_bridge::lark::api::ResourceKind;
use lark_codex_bridge::lark::normalize::ResourceDesc;
use lark_codex_bridge::runtime::attachments::{
    AttachError, AttachmentCache, AttachmentLimits, CachedAttachment, DownloadKind,
    ResourceDownloader,
};
use lark_codex_bridge::store::{NewTurnRow, StoreHandle, TurnState};
use sha2::{Digest, Sha256};
use tempfile::tempdir;

/// Deterministic in-memory downloader keyed by resource key.
#[derive(Clone)]
struct MapDownloader {
    responses: HashMap<String, Result<Bytes, AttachError>>,
}

impl ResourceDownloader for MapDownloader {
    fn download(
        &self,
        _message_id: &str,
        key: &str,
        _kind: ResourceKind,
    ) -> BoxFuture<'static, Result<Bytes, AttachError>> {
        let result = self
            .responses
            .get(key)
            .cloned()
            .unwrap_or_else(|| Ok(Bytes::from_static(b"fallback")));
        Box::pin(async move { result })
    }
}

fn downloader(entries: &[(&str, &[u8])]) -> Arc<dyn ResourceDownloader> {
    let responses = entries
        .iter()
        .map(|(key, bytes)| ((*key).to_owned(), Ok(Bytes::copy_from_slice(bytes))))
        .collect();
    Arc::new(MapDownloader { responses })
}

fn downloader_with_error(key: &str, error: AttachError) -> Arc<dyn ResourceDownloader> {
    let mut responses = HashMap::new();
    responses.insert(key.to_owned(), Err(error));
    Arc::new(MapDownloader { responses })
}

fn desc(key: &str, kind: ResourceKind) -> ResourceDesc {
    ResourceDesc {
        kind,
        key: key.to_owned(),
    }
}

fn sha_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(64);
    for byte in &digest {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to a string cannot fail");
    }
    encoded
}

async fn record_turn(store: &StoreHandle, client_message_id: &str) -> i64 {
    store
        .record_turn(NewTurnRow {
            scope_key: "im:oc_test".to_owned(),
            client_message_id: client_message_id.to_owned(),
            codex_thread_id: Some("thread-1".to_owned()),
            state: TurnState::Starting,
        })
        .await
        .expect("turn")
}

fn cache(
    dir: &Path,
    store: StoreHandle,
    downloader: Arc<dyn ResourceDownloader>,
    limits: AttachmentLimits,
) -> AttachmentCache {
    AttachmentCache::open(dir, store, downloader, limits).expect("cache")
}

fn file_names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .expect("read dir")
        .filter_map(std::result::Result::ok)
        .filter(|entry| entry.file_type().is_ok_and(|t| t.is_file()))
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

fn now_ms() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis());
    i64::try_from(millis).unwrap_or(i64::MAX)
}

#[test]
fn public_types_are_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<AttachmentCache>();
    assert_send_sync::<CachedAttachment>();
}

#[tokio::test]
async fn fetch_writes_content_addressed_file_and_no_temp() {
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let dl = downloader(&[("k", b"hello-content")]);
    let cache = cache(temp.path(), store.clone(), dl, AttachmentLimits::default());
    let turn_id = record_turn(&store, "turn-1").await;

    let cached = cache
        .fetch("om_test", &desc("k", ResourceKind::File), turn_id)
        .await
        .expect("fetch");

    let sha = sha_hex(b"hello-content");
    assert_eq!(cached.sha256, sha);
    assert_eq!(cached.bytes, 13);
    assert_eq!(cached.kind, ResourceKind::File);
    assert_eq!(cached.path, temp.path().join(&sha));
    assert_eq!(std::fs::read(&cached.path).expect("read"), b"hello-content");
    // Exactly one content file, no temp files remain.
    assert_eq!(file_names(temp.path()), vec![sha.clone()]);
    // Store has one row and one lease.
    assert_eq!(store.list_attachments().await.expect("list").len(), 1);
    assert_eq!(
        store.attachment_leases(&sha).await.expect("leases").len(),
        1
    );
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn same_content_reuses_one_file_with_two_leases() {
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let dl = downloader(&[("k", b"shared")]);
    let cache = cache(temp.path(), store.clone(), dl, AttachmentLimits::default());
    let first = record_turn(&store, "turn-1").await;
    let second = record_turn(&store, "turn-2").await;

    cache
        .fetch("om_test", &desc("k", ResourceKind::File), first)
        .await
        .expect("fetch 1");
    cache
        .fetch("om_test", &desc("k", ResourceKind::File), second)
        .await
        .expect("fetch 2");

    let sha = sha_hex(b"shared");
    assert_eq!(file_names(temp.path()), vec![sha.clone()]);
    assert_eq!(store.list_attachments().await.expect("list").len(), 1);
    assert_eq!(
        store.attachment_leases(&sha).await.expect("leases").len(),
        2
    );
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn concurrent_same_hash_writes_share_one_intact_file() {
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let dl = downloader(&[("k", b"concurrent-content")]);
    let limits = AttachmentLimits::default();
    let cache = Arc::new(cache(temp.path(), store.clone(), dl, limits));

    let mut turns = Vec::new();
    for index in 0..8 {
        turns.push(record_turn(&store, &format!("turn-{index}")).await);
    }
    let mut handles = Vec::new();
    for turn_id in turns {
        let cache = Arc::clone(&cache);
        let resource = desc("k", ResourceKind::File);
        handles.push(tokio::spawn(async move {
            cache
                .fetch("om_test", &resource, turn_id)
                .await
                .expect("fetch");
        }));
    }
    for handle in handles {
        handle.await.expect("join");
    }

    let sha = sha_hex(b"concurrent-content");
    assert_eq!(file_names(temp.path()), vec![sha.clone()]);
    assert_eq!(
        std::fs::read(temp.path().join(&sha)).expect("read"),
        b"concurrent-content"
    );
    assert_eq!(store.list_attachments().await.expect("list").len(), 1);
    assert_eq!(
        store.attachment_leases(&sha).await.expect("leases").len(),
        8
    );
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn oversize_download_is_rejected_before_any_disk_write() {
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let dl = downloader(&[("k", b"0123456789abcdef")]); // 16 bytes
    let limits = AttachmentLimits {
        max_attachment_bytes: 8,
        ..AttachmentLimits::default()
    };
    let cache = cache(temp.path(), store.clone(), dl, limits);
    let turn_id = record_turn(&store, "turn-1").await;

    let result = cache
        .fetch("om_test", &desc("k", ResourceKind::File), turn_id)
        .await;
    assert!(matches!(
        result,
        Err(AttachError::TooLarge { limit: 8, .. })
    ));
    // Nothing reached disk: no content file, no temp file, no store row.
    assert!(file_names(temp.path()).is_empty());
    assert!(store.list_attachments().await.expect("list").is_empty());
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn download_failure_leaves_no_file_or_row() {
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let dl = downloader_with_error(
        "k",
        AttachError::Download {
            kind: DownloadKind::Retryable,
        },
    );
    let cache = cache(temp.path(), store.clone(), dl, AttachmentLimits::default());
    let turn_id = record_turn(&store, "turn-1").await;

    let result = cache
        .fetch("om_test", &desc("k", ResourceKind::File), turn_id)
        .await;
    assert!(matches!(
        result,
        Err(AttachError::Download {
            kind: DownloadKind::Retryable
        })
    ));
    assert!(file_names(temp.path()).is_empty());
    assert!(store.list_attachments().await.expect("list").is_empty());
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn lease_protects_file_from_gc_until_released() {
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let dl = downloader(&[("k", b"leased")]);
    let limits = AttachmentLimits {
        gc_age: Duration::ZERO,
        max_cache_files: 0,
        max_cache_bytes: 0,
        ..AttachmentLimits::default()
    };
    let cache = cache(temp.path(), store.clone(), dl, limits);
    let turn_id = record_turn(&store, "turn-1").await;
    let sha = sha_hex(b"leased");
    cache
        .fetch("om_test", &desc("k", ResourceKind::File), turn_id)
        .await
        .expect("fetch");

    // Leased: GC must not delete the file.
    let stats = cache.gc().await.expect("gc");
    assert_eq!(stats.evicted, 0);
    assert!(stats.skipped_leased >= 1);
    assert!(temp.path().join(&sha).exists());

    // Released: GC evicts.
    let released = cache.release_turn(turn_id).await.expect("release");
    assert_eq!(released, 1);
    let stats = cache.gc().await.expect("gc");
    assert_eq!(stats.evicted, 1);
    assert!(!temp.path().join(&sha).exists());
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn gc_evicts_oldest_unleased_first_by_last_used() {
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let dl = downloader(&[("a", b"aaaa"), ("b", b"bbbb")]);
    let limits = AttachmentLimits {
        gc_age: Duration::from_secs(3600),
        max_cache_files: 1,
        max_cache_bytes: u64::MAX,
        ..AttachmentLimits::default()
    };
    let cache = cache(temp.path(), store.clone(), dl, limits);
    let turn_id = record_turn(&store, "turn-1").await;
    cache
        .fetch("om_test", &desc("a", ResourceKind::File), turn_id)
        .await
        .expect("fetch a");
    cache
        .fetch("om_test", &desc("b", ResourceKind::File), turn_id)
        .await
        .expect("fetch b");

    let sha_a = sha_hex(b"aaaa");
    let sha_b = sha_hex(b"bbbb");
    let now = now_ms();
    // "a" is older than "b"; both are far below the 1-hour age threshold.
    store
        .set_attachment_last_used(&sha_a, now - 2000)
        .await
        .expect("backdate a");
    store
        .set_attachment_last_used(&sha_b, now - 1000)
        .await
        .expect("backdate b");
    cache.release_turn(turn_id).await.expect("release");

    let stats = cache.gc().await.expect("gc");
    assert_eq!(stats.evicted, 1);
    assert!(!temp.path().join(&sha_a).exists());
    assert!(temp.path().join(&sha_b).exists());
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn reconcile_removes_orphan_temp_and_orphan_content_files() {
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let dl = downloader(&[]);
    let cache = cache(temp.path(), store.clone(), dl, AttachmentLimits::default());

    // Orphan temp file (unrecognized prefix).
    std::fs::write(temp.path().join(".tmp-deadbeef"), b"partial").expect("write temp");
    // Orphan content file (valid sha name, no store row).
    let orphan_sha = sha_hex(b"orphan-content");
    std::fs::write(temp.path().join(&orphan_sha), b"orphan-content").expect("write orphan");

    let stats = cache.reconcile().await.expect("reconcile");
    assert_eq!(stats.temp_files, 1);
    assert_eq!(stats.orphan_files, 1);
    assert!(file_names(temp.path()).is_empty());
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn reconcile_drops_missing_file_rows() {
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let dl = downloader(&[]);
    let cache = cache(temp.path(), store.clone(), dl, AttachmentLimits::default());

    // A store row with no backing file.
    let sha = sha_hex(b"never-written");
    store
        .put_attachment(&sha, 13, "file")
        .await
        .expect("put attachment");

    let stats = cache.reconcile().await.expect("reconcile");
    assert_eq!(stats.dropped_rows, 1);
    assert!(store.list_attachments().await.expect("list").is_empty());
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn reconcile_detects_size_mismatch() {
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let dl = downloader(&[]);
    let cache = cache(temp.path(), store.clone(), dl, AttachmentLimits::default());

    let sha = sha_hex(b"content");
    std::fs::write(temp.path().join(&sha), b"content").expect("write file");
    // Store claims a different size.
    store
        .put_attachment(&sha, 99, "file")
        .await
        .expect("put attachment");

    let stats = cache.reconcile().await.expect("reconcile");
    assert_eq!(stats.corrupt_files, 1);
    assert_eq!(stats.dropped_rows, 1);
    assert!(file_names(temp.path()).is_empty());
    assert!(store.list_attachments().await.expect("list").is_empty());
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn reconcile_cleans_stale_leases() {
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let dl = downloader(&[("k", b"stale")]);
    let cache = cache(temp.path(), store.clone(), dl, AttachmentLimits::default());
    let turn_id = record_turn(&store, "turn-1").await;
    cache
        .fetch("om_test", &desc("k", ResourceKind::File), turn_id)
        .await
        .expect("fetch");
    // Terminal turn without an explicit release leaves a stale lease.
    store
        .set_turn_state(turn_id, TurnState::Failed, None)
        .await
        .expect("terminal");

    let stats = cache.reconcile().await.expect("reconcile");
    assert_eq!(stats.stale_leases, 1);
    assert!(
        store
            .attachment_leases(&sha_hex(b"stale"))
            .await
            .expect("leases")
            .is_empty()
    );
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn dirty_store_row_cannot_delete_outside_cache_root() {
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let dl = downloader(&[]);
    let cache = cache(temp.path(), store.clone(), dl, AttachmentLimits::default());

    // A victim outside the cache root that must survive reconciliation.
    let victim = temp
        .path()
        .parent()
        .expect("parent")
        .join("victim-must-survive");
    std::fs::write(&victim, b"do not delete").expect("write victim");
    // A dirty store row whose sha looks like a path escape.
    store
        .put_attachment("../victim-must-survive", 13, "file")
        .await
        .expect("put attachment");

    let stats = cache.reconcile().await.expect("reconcile");
    assert_eq!(stats.dropped_rows, 1);
    assert!(store.list_attachments().await.expect("list").is_empty());
    assert!(victim.exists(), "victim file must survive");
    std::fs::remove_file(&victim).expect("cleanup victim");
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn hash_mismatch_is_repaired_at_reuse() {
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let dl = downloader(&[("k", b"correct-content")]);
    let cache = cache(temp.path(), store.clone(), dl, AttachmentLimits::default());
    let turn_id = record_turn(&store, "turn-1").await;
    let sha = sha_hex(b"correct-content");
    cache
        .fetch("om_test", &desc("k", ResourceKind::File), turn_id)
        .await
        .expect("fetch");

    // Corrupt the file in place with same-length different bytes.
    std::fs::write(temp.path().join(&sha), b"corrupt!content").expect("corrupt");

    // A second fetch re-verifies and repairs from the fresh download.
    cache
        .fetch("om_test", &desc("k", ResourceKind::File), turn_id)
        .await
        .expect("refetch");
    assert_eq!(
        std::fs::read(temp.path().join(&sha)).expect("read"),
        b"correct-content"
    );
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn reconcile_is_idempotent() {
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let dl = downloader(&[]);
    let cache = cache(temp.path(), store.clone(), dl, AttachmentLimits::default());
    std::fs::write(temp.path().join(".tmp-1"), b"x").expect("temp");

    let first = cache.reconcile().await.expect("reconcile");
    assert_eq!(first.temp_files, 1);
    let second = cache.reconcile().await.expect("reconcile");
    assert_eq!(second.temp_files, 0);
    assert_eq!(second.orphan_files, 0);
    assert_eq!(second.dropped_rows, 0);
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn reconcile_truncated_scan_preserves_valid_rows_and_leases() {
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let dl = downloader(&[("k", b"survivor")]);
    let limits = AttachmentLimits {
        reconcile_batch: 0,
        ..AttachmentLimits::default()
    };
    let cache = cache(temp.path(), store.clone(), dl, limits);
    let turn_id = record_turn(&store, "turn-1").await;
    let sha = sha_hex(b"survivor");
    cache
        .fetch("om_test", &desc("k", ResourceKind::File), turn_id)
        .await
        .expect("fetch");

    // A zero batch forces a truncated scan: the directory entry is never
    // scanned, so it must not be mistaken for a missing file. The store row
    // and its lease must both survive (the lease cascades away if the row is
    // force-deleted).
    let stats = cache.reconcile().await.expect("reconcile");
    assert_eq!(stats.dropped_rows, 0, "truncated scan must not drop rows");

    let rows = store.list_attachments().await.expect("list");
    assert_eq!(rows.len(), 1, "valid store row survives");
    assert_eq!(rows[0].sha256, sha);
    assert_eq!(
        store.attachment_leases(&sha).await.expect("leases").len(),
        1,
        "valid lease survives"
    );
    assert!(temp.path().join(&sha).exists(), "valid file survives");
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn reconcile_converges_orphans_across_repeated_truncated_passes() {
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let dl = downloader(&[("k", b"keep")]);
    let limits = AttachmentLimits {
        reconcile_batch: 2,
        ..AttachmentLimits::default()
    };
    let cache = cache(temp.path(), store.clone(), dl, limits);
    let turn_id = record_turn(&store, "turn-1").await;
    let sha = sha_hex(b"keep");
    cache
        .fetch("om_test", &desc("k", ResourceKind::File), turn_id)
        .await
        .expect("fetch");

    // More directory entries than one pass can scan: a valid file plus five
    // orphan temp files. Reconcile must converge to only the valid file, and
    // must never drop the valid row/lease even while scans are truncated.
    for index in 0..5 {
        std::fs::write(temp.path().join(format!(".tmp-{index}")), b"x").expect("temp");
    }

    let mut temp_removed = 0_u64;
    for _ in 0..10 {
        let stats = cache.reconcile().await.expect("reconcile");
        temp_removed = temp_removed.saturating_add(stats.temp_files);
        if file_names(temp.path()) == vec![sha.clone()] {
            break;
        }
    }

    assert_eq!(file_names(temp.path()), vec![sha.clone()]);
    assert_eq!(temp_removed, 5, "every orphan temp file is removed");
    let rows = store.list_attachments().await.expect("list");
    assert_eq!(rows.len(), 1, "valid row survives every pass");
    assert_eq!(rows[0].sha256, sha);
    assert_eq!(
        store.attachment_leases(&sha).await.expect("leases").len(),
        1,
        "valid lease survives every pass"
    );
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn fetch_rejects_oversized_resource_key_before_download() {
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let bad_key = "x".repeat(AttachmentLimits::default().max_resource_key_bytes + 1);
    // The downloader maps the bad key to a Download error, so surfacing
    // `InvalidResourceKey` proves the key was rejected *before* any download.
    let dl = downloader_with_error(
        &bad_key,
        AttachError::Download {
            kind: DownloadKind::Retryable,
        },
    );
    let cache = cache(temp.path(), store.clone(), dl, AttachmentLimits::default());
    let turn_id = record_turn(&store, "turn-1").await;

    let result = cache
        .fetch("om_test", &desc(&bad_key, ResourceKind::File), turn_id)
        .await;
    assert!(matches!(
        result,
        Err(AttachError::InvalidResourceKey { .. })
    ));
    // Nothing reached disk and no row/lease was produced.
    assert!(file_names(temp.path()).is_empty());
    assert!(store.list_attachments().await.expect("list").is_empty());
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn put_attachment_and_lease_is_atomic() {
    let store = StoreHandle::open_in_memory().await.expect("store");

    // A nonexistent turn must fail the whole operation and leave no row (the
    // lease FK aborts the transaction, rolling the attachment row back too).
    let missing_turn = store
        .put_attachment_and_lease(&sha_hex(b"x"), 1, "file", 99)
        .await;
    assert!(missing_turn.is_err(), "missing turn must fail");
    assert!(
        store.list_attachments().await.expect("list").is_empty(),
        "no attachment row may survive a failed lease"
    );

    // With a real turn, the row and lease commit together: GC's
    // `delete_attachment` can never observe an unleased window, so it refuses
    // to delete the row while the lease exists.
    let turn_id = record_turn(&store, "turn-atomic").await;
    let sha = sha_hex(b"atomic-content");
    store
        .put_attachment_and_lease(&sha, 15, "file", turn_id)
        .await
        .expect("put + lease");
    assert_eq!(store.list_attachments().await.expect("list").len(), 1);
    assert_eq!(
        store.attachment_leases(&sha).await.expect("leases").len(),
        1
    );
    assert!(!store.delete_attachment(&sha).await.expect("protected"));
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn fetch_with_missing_turn_leaves_no_row_or_lease() {
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let dl = downloader(&[("k", b"orphan-on-failure")]);
    let cache = cache(temp.path(), store.clone(), dl, AttachmentLimits::default());

    let result = cache
        .fetch("om_test", &desc("k", ResourceKind::File), 99)
        .await;
    assert!(result.is_err(), "fetch with a missing turn must fail");

    // The installed content file is an orphan (design ordering: file before
    // row), but there must be no dangling row or lease left behind.
    assert!(store.list_attachments().await.expect("list").is_empty());
    assert_eq!(
        file_names(temp.path()).len(),
        1,
        "the content file becomes an orphan, reconciled later"
    );
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn restart_reconciliation_heals_every_dirty_state() {
    let temp = tempdir().expect("tempdir");
    let cache_dir = temp.path().join("cache");
    let db_path = temp.path().join("store.sqlite");

    // First life: fetch one attachment leased by a live turn.
    {
        let store = StoreHandle::open(&db_path).await.expect("open");
        let dl = downloader(&[("k", b"survives-restart")]);
        let cache = cache(&cache_dir, store.clone(), dl, AttachmentLimits::default());
        let turn_id = record_turn(&store, "turn-1").await;
        cache
            .fetch("om_test", &desc("k", ResourceKind::File), turn_id)
            .await
            .expect("fetch");
        // Turn ends but the process "crashes" before releasing.
        store
            .set_turn_state(turn_id, TurnState::Failed, None)
            .await
            .expect("terminal");
        let _ = store.shutdown().await;
    }

    // Plant additional dirty states while "offline".
    std::fs::write(cache_dir.join(".tmp-orphan"), b"t").expect("temp");
    let orphan_sha = sha_hex(b"orphan-file");
    std::fs::write(cache_dir.join(&orphan_sha), b"orphan-file").expect("orphan");
    let missing_sha = sha_hex(b"missing-file");
    {
        let store = StoreHandle::open(&db_path).await.expect("reopen for setup");
        store
            .put_attachment(&missing_sha, 12, "file")
            .await
            .expect("missing row");
        let _ = store.shutdown().await;
    }

    // Second life: reconcile at startup.
    let store = StoreHandle::open(&db_path).await.expect("reopen");
    let dl = downloader(&[("k", b"survives-restart")]);
    let cache = cache(&cache_dir, store.clone(), dl, AttachmentLimits::default());
    let stats = cache.reconcile().await.expect("reconcile");

    assert_eq!(stats.temp_files, 1);
    assert_eq!(stats.orphan_files, 1);
    assert_eq!(stats.dropped_rows, 1, "missing-file row must be dropped");
    assert_eq!(stats.stale_leases, 1, "completed turn lease is stale");

    // The surviving attachment (now unleased) remains until GC evicts it.
    let sha = sha_hex(b"survives-restart");
    assert!(cache_dir.join(&sha).exists());
    let rows = store.list_attachments().await.expect("list");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].sha256, sha);
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn limits_reject_unsafe_names_keys_and_mime() {
    let limits = AttachmentLimits::default();

    // Resource key length and charset.
    assert!(limits.check_resource_key("ok-key").is_ok());
    assert!(matches!(
        limits.check_resource_key("bad/key"),
        Err(AttachError::InvalidResourceKey { .. })
    ));
    assert!(matches!(
        limits.check_resource_key(""),
        Err(AttachError::InvalidResourceKey { .. })
    ));
    assert!(matches!(
        limits.check_resource_key(&"x".repeat(limits.max_resource_key_bytes + 1)),
        Err(AttachError::InvalidResourceKey { .. })
    ));

    // Batch count.
    let too_many: Vec<ResourceDesc> = (0..=limits.max_attachments_per_message)
        .map(|index| desc(&format!("k{index}"), ResourceKind::File))
        .collect();
    assert!(matches!(
        limits.check_resource_batch(&too_many),
        Err(AttachError::TooManyResources { .. })
    ));

    // Display file names (metadata only, never a path).
    assert!(limits.check_file_name("report.pdf").is_ok());
    assert!(matches!(
        limits.check_file_name("../evil"),
        Err(AttachError::InvalidFileName { .. })
    ));
    assert!(matches!(
        limits.check_file_name("a/b"),
        Err(AttachError::InvalidFileName { .. })
    ));
    assert!(matches!(
        limits.check_file_name("a\\b"),
        Err(AttachError::InvalidFileName { .. })
    ));
    assert!(matches!(
        limits.check_file_name(".."),
        Err(AttachError::InvalidFileName { .. })
    ));

    // MIME.
    assert!(limits.check_mime("image/png").is_ok());
    assert!(matches!(
        limits.check_mime("image\npng"),
        Err(AttachError::InvalidMime { .. })
    ));

    // Turn total.
    assert!(limits.check_turn_total(limits.max_turn_total_bytes).is_ok());
    assert!(matches!(
        limits.check_turn_total(limits.max_turn_total_bytes + 1),
        Err(AttachError::TurnTotalExceeded { .. })
    ));
}

#[test]
fn debug_output_never_leaks_content_or_paths() {
    let cached = CachedAttachment {
        sha256: sha_hex(b"secret-bytes"),
        path: Path::new("/home/secret/cache").join(sha_hex(b"secret-bytes")),
        kind: ResourceKind::File,
        bytes: 12,
    };
    let debug = format!("{cached:?}");
    assert!(!debug.contains("secret-bytes"));
    assert!(!debug.contains("/home/secret"));
    assert!(debug.contains("CachedAttachment"));

    let error = AttachError::HashMismatch {
        context: "cached file content",
    };
    assert!(format!("{error:?}").contains("HashMismatch"));
}
