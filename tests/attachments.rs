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
use lark_codex_bridge::limits::{ATTACHMENT_CACHE_MARKER, ATTACHMENT_INSTANCE_LOCK};
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

/// A downloader that parks inside `download` until released, so tests can hold
/// a fetch in the pre-lock download phase and interleave it with GC.
struct GateDownloader {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    bytes: Bytes,
}

impl ResourceDownloader for GateDownloader {
    fn download(
        &self,
        _message_id: &str,
        _key: &str,
        _kind: ResourceKind,
    ) -> BoxFuture<'static, Result<Bytes, AttachError>> {
        let entered = Arc::clone(&self.entered);
        let release = Arc::clone(&self.release);
        let bytes = self.bytes.clone();
        Box::pin(async move {
            entered.notify_one();
            release.notified().await;
            Ok(bytes)
        })
    }
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
        .filter(|name| name != ATTACHMENT_CACHE_MARKER && name != ATTACHMENT_INSTANCE_LOCK)
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
    let canonical_temp = std::fs::canonicalize(temp.path()).expect("canonical tempdir");
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
    assert_eq!(cached.path, canonical_temp.join(&sha));
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
async fn gc_batch_bounds_rows_inspected_even_when_every_row_is_leased() {
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let limits = AttachmentLimits {
        gc_age: Duration::ZERO,
        max_cache_files: 0,
        max_cache_bytes: 0,
        gc_batch: 2,
        ..AttachmentLimits::default()
    };
    let cache = cache(
        temp.path(),
        store.clone(),
        downloader(&[("a", b"one"), ("b", b"two"), ("c", b"three")]),
        limits,
    );

    for (index, key) in ["a", "b", "c"].into_iter().enumerate() {
        let turn = record_turn(&store, &format!("leased-{index}")).await;
        cache
            .fetch("om_test", &desc(key, ResourceKind::File), turn)
            .await
            .expect("fetch");
    }

    let stats = cache.gc().await.expect("gc");
    assert_eq!(stats.inspected, 2, "the pass must stop at its row budget");
    assert_eq!(stats.skipped_leased, 2);
    assert_eq!(stats.evicted, 0);
    assert_eq!(store.list_attachments().await.expect("list").len(), 3);
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
async fn reconcile_streams_bounded_deletions_and_preserves_valid_entries() {
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let dl = downloader(&[("k", b"survivor")]);
    let batch = 8_usize;
    let limits = AttachmentLimits {
        reconcile_batch: batch,
        ..AttachmentLimits::default()
    };
    let cache = cache(temp.path(), store.clone(), dl, limits);
    let turn_id = record_turn(&store, "turn-1").await;
    let sha = sha_hex(b"survivor");
    cache
        .fetch("om_test", &desc("k", ResourceKind::File), turn_id)
        .await
        .expect("fetch");

    // Scatter more than one batch of orphans across the directory: a mix of
    // orphan temp files and orphan content files (valid hash names with no
    // store row), so the streaming pass must delete across the whole name
    // space rather than a sorted head.
    let total = batch * 3;
    for index in 0..total {
        if index % 2 == 0 {
            std::fs::write(temp.path().join(format!(".tmp-{index}")), b"x").expect("temp");
        } else {
            let orphan = sha_hex(format!("orphan-{index}").as_bytes());
            std::fs::write(temp.path().join(&orphan), format!("orphan-{index}")).expect("orphan");
        }
    }

    // Behavioral convergence: each pass may delete at most `batch` files, and
    // enough passes clear every orphan while the valid file/row/lease survive
    // untouched. No heap-size assertion is made: the O(1)-memory property is
    // structural (the scan never materializes a directory vector) and a
    // byte-count assertion would be allocator- and platform-dependent.
    let batch_cap = u64::try_from(batch).unwrap_or(u64::MAX);
    let total_count = u64::try_from(total).unwrap_or(u64::MAX);
    let mut removed = 0_u64;
    let mut max_deletions = 0_u64;
    for _ in 0..(total + 2) {
        let stats = cache.reconcile().await.expect("reconcile");
        assert!(stats.scanned_entries <= batch_cap);
        let deletions = stats.temp_files + stats.orphan_files + stats.corrupt_files;
        assert!(
            deletions <= batch_cap,
            "a pass deleted {deletions} files, above the batch cap {batch}"
        );
        max_deletions = max_deletions.max(deletions);
        removed = removed.saturating_add(deletions);
        if file_names(temp.path()) == vec![sha.clone()] {
            break;
        }
    }

    assert_eq!(
        file_names(temp.path()),
        vec![sha.clone()],
        "orphans cleared"
    );
    assert_eq!(removed, total_count, "every orphan removed exactly once");
    assert!(max_deletions <= batch_cap, "per-pass deletion cap held");
    let rows = store.list_attachments().await.expect("list");
    assert_eq!(rows.len(), 1, "valid row survives every pass");
    assert_eq!(rows[0].sha256, sha);
    assert_eq!(
        store.attachment_leases(&sha).await.expect("leases").len(),
        1,
        "valid lease survives every pass"
    );
    assert!(temp.path().join(&sha).is_file(), "valid file survives");
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
        lease_was_inserted: true,
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

// --- B1: dedicated-directory marker -------------------------------------------------

#[tokio::test]
async fn open_refuses_non_empty_directory_without_marker_and_preserves_files() {
    let temp = tempdir().expect("tempdir");
    let root = temp.path().join("cache");
    std::fs::create_dir_all(&root).expect("create dir");
    // A stray file the cache must never delete.
    std::fs::write(root.join("user-data.txt"), b"precious").expect("write stray");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let dl = downloader(&[]);

    let result = AttachmentCache::open(&root, store.clone(), dl, AttachmentLimits::default());
    assert!(
        matches!(result, Err(AttachError::InvalidPath { .. })),
        "a non-empty unmarked directory must be refused fail-closed"
    );
    // The stray file is untouched and no marker was created.
    assert_eq!(
        std::fs::read(root.join("user-data.txt")).expect("read"),
        b"precious"
    );
    assert!(!root.join(ATTACHMENT_CACHE_MARKER).exists());
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn open_creates_marker_in_empty_directory_and_reopens() {
    let temp = tempdir().expect("tempdir");
    let root = temp.path().join("cache");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let dl = downloader(&[]);

    let cache = AttachmentCache::open(&root, store.clone(), dl, AttachmentLimits::default())
        .expect("first open");
    let marker = root.join(ATTACHMENT_CACHE_MARKER);
    assert!(marker.is_file(), "first open must write the marker");

    // Reopen the same (now non-empty, marked) directory: must succeed.
    drop(cache);
    let _reopened = AttachmentCache::open(
        &root,
        store.clone(),
        downloader(&[]),
        AttachmentLimits::default(),
    )
    .expect("reopen of a valid marked cache");
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn open_refuses_directory_with_wrong_marker_contents() {
    let temp = tempdir().expect("tempdir");
    let root = temp.path().join("cache");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let _ = AttachmentCache::open(
        &root,
        store.clone(),
        downloader(&[]),
        AttachmentLimits::default(),
    )
    .expect("first open writes the marker");
    // Corrupt the marker in place.
    std::fs::write(root.join(ATTACHMENT_CACHE_MARKER), b"not the magic").expect("corrupt marker");

    let result = AttachmentCache::open(
        &root,
        store.clone(),
        downloader(&[]),
        AttachmentLimits::default(),
    );
    assert!(
        matches!(result, Err(AttachError::InvalidPath { .. })),
        "a wrong marker must be refused fail-closed"
    );
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn open_refuses_a_non_directory_root() {
    let temp = tempdir().expect("tempdir");
    let root = temp.path().join("cache-file");
    std::fs::write(&root, b"i am a file").expect("write file");
    let store = StoreHandle::open_in_memory().await.expect("store");

    let result = AttachmentCache::open(
        &root,
        store.clone(),
        downloader(&[]),
        AttachmentLimits::default(),
    );
    assert!(matches!(result, Err(AttachError::InvalidPath { .. })));
    let _ = store.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn open_refuses_a_symlink_root() {
    let temp = tempdir().expect("tempdir");
    let target = temp.path().join("real-cache");
    std::fs::create_dir_all(&target).expect("create target");
    let link = temp.path().join("cache-link");
    std::os::unix::fs::symlink(&target, &link).expect("symlink");
    let store = StoreHandle::open_in_memory().await.expect("store");

    let result = AttachmentCache::open(
        &link,
        store.clone(),
        downloader(&[]),
        AttachmentLimits::default(),
    );
    assert!(matches!(result, Err(AttachError::InvalidPath { .. })));
    let _ = store.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn open_refusal_does_not_chmod_the_directory() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempdir().expect("tempdir");
    let root = temp.path().join("cache");
    std::fs::create_dir_all(&root).expect("create dir");
    std::fs::write(root.join("user-data.txt"), b"precious").expect("write stray");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    let store = StoreHandle::open_in_memory().await.expect("store");

    // A non-empty directory without a valid marker is refused fail-closed. The
    // refusal must happen before any chmod, so the directory's permissions are
    // left exactly as configured.
    let result = AttachmentCache::open(
        &root,
        store.clone(),
        downloader(&[]),
        AttachmentLimits::default(),
    );
    assert!(matches!(result, Err(AttachError::InvalidPath { .. })));

    let mode = std::fs::metadata(&root)
        .expect("metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(
        mode, 0o755,
        "a refused directory's permissions must be untouched"
    );
    assert_eq!(
        std::fs::read(root.join("user-data.txt")).expect("read"),
        b"precious"
    );
    assert!(
        !root.join(ATTACHMENT_CACHE_MARKER).exists(),
        "no marker may be written on refusal"
    );
    let _ = store.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn open_tightens_permissions_only_after_marker_validation() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempdir().expect("tempdir");
    let root = temp.path().join("cache");
    std::fs::create_dir_all(&root).expect("create dir");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).expect("chmod");
    let store = StoreHandle::open_in_memory().await.expect("store");

    // An empty directory passes marker validation (marker is written), and
    // only then is its mode tightened to owner-only.
    let cache = AttachmentCache::open(
        &root,
        store.clone(),
        downloader(&[]),
        AttachmentLimits::default(),
    )
    .expect("open");
    let mode = std::fs::metadata(&root)
        .expect("metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o700, "a validated directory is tightened to 0700");
    drop(cache);
    let _ = store.shutdown().await;
}

// --- B2: GC vs. fetch serialization ------------------------------------------------

#[tokio::test]
async fn fetch_download_is_outside_the_gc_lock() {
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let dl: Arc<dyn ResourceDownloader> = Arc::new(GateDownloader {
        entered: Arc::clone(&entered),
        release: Arc::clone(&release),
        bytes: Bytes::from_static(b"gated"),
    });
    let cache = Arc::new(cache(
        temp.path(),
        store.clone(),
        dl,
        AttachmentLimits::default(),
    ));
    let turn = record_turn(&store, "gate-turn").await;

    let fetch = tokio::spawn({
        let cache = Arc::clone(&cache);
        async move {
            cache
                .fetch("om_test", &desc("k", ResourceKind::File), turn)
                .await
        }
    });

    // Park the fetch inside `download` (before it takes the per-cache lock).
    entered.notified().await;
    // GC must complete while the fetch is still downloading: if the lock were
    // held across the download, this would deadlock.
    let gc = tokio::time::timeout(std::time::Duration::from_secs(5), cache.gc()).await;
    assert!(gc.is_ok(), "gc must not wait on a fetch still downloading");

    release.notify_one();
    let fetched = fetch.await.expect("join").expect("fetch");
    assert_eq!(fetched.bytes, 5);
    assert!(temp.path().join(sha_hex(b"gated")).is_file());
    let _ = store.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_fetch_gc_never_leaves_a_leased_row_without_file() {
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let dl = downloader(&[("k", b"race-content")]);
    let limits = AttachmentLimits {
        gc_age: Duration::ZERO,
        max_cache_files: 0,
        max_cache_bytes: 0,
        gc_batch: 64,
        ..AttachmentLimits::default()
    };
    let cache = Arc::new(cache(temp.path(), store.clone(), dl, limits));

    for round in 0..60 {
        // Seed an unleased victim so GC has something to evict, then free the
        // live-turn slot (Starting -> Failed keeps the row but no lease).
        let seed = record_turn(&store, &format!("seed-{round}")).await;
        cache
            .fetch("om_test", &desc("k", ResourceKind::File), seed)
            .await
            .expect("seed fetch");
        cache.release_turn(seed).await.expect("release seed");
        store
            .set_turn_state(seed, TurnState::Failed, None)
            .await
            .expect("terminalize seed");

        let mut fetch_tasks = Vec::new();
        for index in 0..4 {
            let cache = Arc::clone(&cache);
            let store = store.clone();
            fetch_tasks.push(tokio::spawn(async move {
                let turn = record_turn(&store, &format!("f-{round}-{index}")).await;
                cache
                    .fetch("om_test", &desc("k", ResourceKind::File), turn)
                    .await
                    .expect("fetch");
                // Free the live-turn slot while keeping the lease so the
                // invariant check still sees a leased row.
                store
                    .set_turn_state(turn, TurnState::Failed, None)
                    .await
                    .expect("terminalize fetch");
                turn
            }));
        }
        let mut gc_tasks = Vec::new();
        for _ in 0..4 {
            let cache = Arc::clone(&cache);
            gc_tasks.push(tokio::spawn(async move {
                let _ = cache.gc().await;
            }));
        }

        let mut fetch_turns = Vec::new();
        for task in fetch_tasks {
            fetch_turns.push(task.await.expect("join"));
        }
        for task in gc_tasks {
            task.await.expect("join");
        }

        // Invariant: any row still holding a lease must have its file on disk.
        let rows = store.list_attachments().await.expect("list");
        for row in rows {
            let leased = !store
                .attachment_leases(&row.sha256)
                .await
                .expect("leases")
                .is_empty();
            if leased {
                assert!(
                    temp.path().join(&row.sha256).is_file(),
                    "leased row lost its file after round {round}: {row:?}"
                );
            }
        }

        // Release this round's leases so the next round seeds a fresh
        // unleased victim again.
        for turn in fetch_turns {
            cache.release_turn(turn).await.expect("release fetch lease");
        }
    }
    let _ = store.shutdown().await;
}

// --- B4: rename durability ---------------------------------------------------------

#[cfg(unix)]
#[tokio::test]
async fn install_syncs_the_parent_directory_on_unix() {
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let cache = cache(
        temp.path(),
        store.clone(),
        downloader(&[("k", b"sync-me")]),
        AttachmentLimits::default(),
    );
    let turn = record_turn(&store, "dir-sync").await;
    // The install path (temp write -> rename -> parent-dir fsync) must run
    // cleanly on a real directory. The `fsync` syscall itself is not
    // observable from here, so this asserts the code path executes without
    // error; power-loss durability is not directly assertable.
    cache
        .fetch("om_test", &desc("k", ResourceKind::File), turn)
        .await
        .expect("fetch");
    assert!(temp.path().join(sha_hex(b"sync-me")).is_file());
    let _ = store.shutdown().await;
}

// --- B1b: reconcile batch clamp + off-worker scan ----------------------------------

#[test]
fn reconcile_batch_zero_is_clamped_to_at_least_one() {
    let limits = AttachmentLimits {
        reconcile_batch: 0,
        ..AttachmentLimits::default()
    }
    .clamped();
    assert!(
        limits.reconcile_batch >= 1,
        "a zero reconcile_batch must be clamped to a positive minimum"
    );
    assert!(
        limits.reconcile_batch <= lark_codex_bridge::limits::ATTACHMENT_RECONCILE_BATCH,
        "reconcile_batch must not exceed the global scan bound"
    );
}

#[tokio::test]
async fn reconcile_batch_zero_still_converges() {
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let dl = downloader(&[]);
    let limits = AttachmentLimits {
        reconcile_batch: 0,
        ..AttachmentLimits::default()
    };
    let cache = cache(temp.path(), store.clone(), dl, limits);

    // Three orphan files must still be fully cleaned even though the caller
    // configured a zero batch: `open` clamps it to >= 1, so each pass deletes
    // at least one file and reconciliation converges monotonically.
    for index in 0..3 {
        std::fs::write(temp.path().join(format!(".tmp-{index}")), b"x").expect("temp");
    }
    for _ in 0..8 {
        let stats = cache.reconcile().await.expect("reconcile");
        assert!(stats.scanned_entries <= 1);
        assert!(
            stats.temp_files <= 1,
            "a clamped batch must delete at most one file per pass"
        );
        if file_names(temp.path()).is_empty() {
            break;
        }
    }
    assert!(file_names(temp.path()).is_empty(), "orphans cleared");
    let _ = store.shutdown().await;
}

// --- B2b: cross-instance fail-closed lock ------------------------------------------

#[tokio::test]
async fn second_open_in_the_same_directory_fails_closed() {
    let temp = tempdir().expect("tempdir");
    let root = temp.path().join("cache");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let dl = downloader(&[]);

    let _first = AttachmentCache::open(
        &root,
        store.clone(),
        dl.clone(),
        AttachmentLimits::default(),
    )
    .expect("first open");
    assert!(
        root.join(ATTACHMENT_INSTANCE_LOCK).is_file(),
        "first open must create the instance lock"
    );

    let second = AttachmentCache::open(&root, store.clone(), dl, AttachmentLimits::default());
    assert!(
        matches!(second, Err(AttachError::InvalidPath { .. })),
        "a live second instance must be refused fail-closed"
    );
    let _ = store.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn open_refuses_a_symlink_instance_lock_without_chmodding_its_target() {
    use std::os::unix::fs::{PermissionsExt, symlink};

    let temp = tempdir().expect("tempdir");
    let root = temp.path().join("cache");
    let outside = temp.path().join("outside-lock-target");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let first = AttachmentCache::open(
        &root,
        store.clone(),
        downloader(&[]),
        AttachmentLimits::default(),
    )
    .expect("seed dedicated cache");
    drop(first);
    std::fs::remove_file(root.join(ATTACHMENT_INSTANCE_LOCK)).expect("remove seed lock");
    std::fs::write(&outside, b"outside").expect("outside file");
    std::fs::set_permissions(&outside, std::fs::Permissions::from_mode(0o640))
        .expect("outside permissions");
    symlink(&outside, root.join(ATTACHMENT_INSTANCE_LOCK)).expect("symlink lock");

    let reopened = AttachmentCache::open(
        &root,
        store.clone(),
        downloader(&[]),
        AttachmentLimits::default(),
    );
    assert!(
        matches!(reopened, Err(AttachError::InvalidPath { .. })),
        "a symlink lock path must be refused fail-closed"
    );
    let mode = std::fs::metadata(&outside)
        .expect("outside metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o640, "the symlink target must never be chmod'd");
    let _ = store.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn open_refuses_a_hard_link_instance_lock_without_chmodding_its_alias() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempdir().expect("tempdir");
    let root = temp.path().join("cache");
    let outside = temp.path().join("outside-lock-alias");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let first = AttachmentCache::open(
        &root,
        store.clone(),
        downloader(&[]),
        AttachmentLimits::default(),
    )
    .expect("seed dedicated cache");
    drop(first);
    std::fs::remove_file(root.join(ATTACHMENT_INSTANCE_LOCK)).expect("remove seed lock");
    std::fs::write(&outside, b"outside").expect("outside file");
    std::fs::set_permissions(&outside, std::fs::Permissions::from_mode(0o640))
        .expect("outside permissions");
    std::fs::hard_link(&outside, root.join(ATTACHMENT_INSTANCE_LOCK)).expect("hard-link lock");

    let reopened = AttachmentCache::open(
        &root,
        store.clone(),
        downloader(&[]),
        AttachmentLimits::default(),
    );
    assert!(
        matches!(reopened, Err(AttachError::InvalidPath { .. })),
        "a multiply-linked lock inode must be refused fail-closed"
    );
    let mode = std::fs::metadata(&outside)
        .expect("outside metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o640, "the hard-link alias must never be chmod'd");
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn dropping_the_file_lock_lets_a_later_instance_reopen() {
    let temp = tempdir().expect("tempdir");
    let root = temp.path().join("cache");
    let store = StoreHandle::open_in_memory().await.expect("store");

    let first = AttachmentCache::open(
        &root,
        store.clone(),
        downloader(&[]),
        AttachmentLimits::default(),
    )
    .expect("first open");
    let lock = root.join(ATTACHMENT_INSTANCE_LOCK);
    assert!(lock.is_file());
    drop(first);
    assert!(
        lock.is_file(),
        "the stable lock path remains while the kernel lock is released"
    );

    let _second = AttachmentCache::open(
        &root,
        store.clone(),
        downloader(&[]),
        AttachmentLimits::default(),
    )
    .expect("reopen after lock release");
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn an_existing_unlocked_lock_file_can_be_reused() {
    let temp = tempdir().expect("tempdir");
    let root = temp.path().join("cache");
    let store = StoreHandle::open_in_memory().await.expect("store");

    // First life creates the stable lock path, then exits. The next owner must
    // lock the same inode without deleting or replacing it.
    {
        let _first = AttachmentCache::open(
            &root,
            store.clone(),
            downloader(&[]),
            AttachmentLimits::default(),
        )
        .expect("first open");
    }
    let lock = root.join(ATTACHMENT_INSTANCE_LOCK);
    assert!(lock.is_file(), "the stable lock file remains after drop");

    let _reopened = AttachmentCache::open(
        &root,
        store.clone(),
        downloader(&[]),
        AttachmentLimits::default(),
    )
    .expect("the unlocked stable file can be acquired");
    assert!(lock.is_file());
    let _ = store.shutdown().await;
}

#[tokio::test]
async fn reconcile_never_deletes_the_instance_lock_or_marker() {
    let temp = tempdir().expect("tempdir");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let cache = cache(
        temp.path(),
        store.clone(),
        downloader(&[]),
        AttachmentLimits::default(),
    );

    let _ = cache.reconcile().await.expect("reconcile");
    assert!(
        temp.path().join(ATTACHMENT_INSTANCE_LOCK).is_file(),
        "the instance lock must survive reconciliation"
    );
    assert!(
        temp.path().join(ATTACHMENT_CACHE_MARKER).is_file(),
        "the marker must survive reconciliation"
    );
    let _ = store.shutdown().await;
}

// --- B3b: fail-closed chmod + owner-only marker/lock ---------------------------------

#[cfg(unix)]
#[tokio::test]
async fn marker_and_lock_files_are_owner_only() {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempdir().expect("tempdir");
    let root = temp.path().join("cache");
    let store = StoreHandle::open_in_memory().await.expect("store");
    let _cache = cache(
        &root,
        store.clone(),
        downloader(&[]),
        AttachmentLimits::default(),
    );

    let marker_mode = std::fs::metadata(root.join(ATTACHMENT_CACHE_MARKER))
        .expect("marker metadata")
        .permissions()
        .mode()
        & 0o777;
    let lock_mode = std::fs::metadata(root.join(ATTACHMENT_INSTANCE_LOCK))
        .expect("lock metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(marker_mode, 0o600, "the marker must be owner-only (0600)");
    assert_eq!(
        lock_mode, 0o600,
        "the instance lock must be owner-only (0600)"
    );
    let _ = store.shutdown().await;
}
