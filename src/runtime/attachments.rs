//! Content-addressed attachment cache with leases, GC, and startup
//! reconciliation (design §10, Task 7 core).
//!
//! Files are stored under a cache root named by their SHA-256 hex digest; the
//! user-visible file name never influences the on-disk path (design §10). A
//! download is hashed while being written to a same-directory temp file, then
//! `fsync`ed and atomically renamed into place, so the final path only ever
//! holds a complete object. Store rows and leases reuse the existing
//! [`StoreHandle`] attachment protocol; nothing here re-derives a second one.
//!
//! Ordering invariant: content is installed on disk *before* its store row and
//! lease are committed, and GC drops a store row *before* removing its file.
//! A crash therefore leaves an orphan file (reconciled at the next startup)
//! rather than a dangling store row that promises a missing file.
//!
//! Redaction: no `Debug`, tracing, or error carries attachment bytes, message
//! text, user file names, or absolute paths outside the cache root.
//!
//! Single-instance constraint: the cache root and its backing store are
//! private to one bridge instance; sharing one cache root across processes is
//! unsupported. Within one process the per-cache [`tokio::sync::Mutex`] and
//! the single-writer WAL `SQLite` store serialize `fetch`/`gc`/`reconcile`, so a
//! valid lease can never point at a missing file. `gc`'s "delete row then
//! delete file" window cannot be closed across processes without a
//! cross-process lock, which is deliberately not introduced (no `flock`, no
//! Unix-only primitives); `gc` re-checks the row before unlinking as best
//! effort only, which narrows but does not close that window.
//!
//! Reconciliation is a single streaming `read_dir` pass: directory entries are
//! never materialized into a sorted vector, so scan memory is O(1) in the
//! directory size. Destructive cleanup (orphan temp/content files and
//! size-mismatched files) is capped at [`AttachmentLimits::reconcile_batch`]
//! deletions per pass; the scan itself is never truncated, so each pass walks
//! the whole directory and orphan cleanup converges because the entry count
//! strictly decreases by at most `reconcile_batch` files per pass.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures_util::future::BoxFuture;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::lark::api::{LarkApi, ResourceKind};
use crate::lark::error::LarkError;
use crate::lark::normalize::ResourceDesc;
use crate::limits::{
    ATTACHMENT_CACHE_MARKER, ATTACHMENT_CACHE_MAX_BYTES, ATTACHMENT_CACHE_MAX_FILES,
    ATTACHMENT_FILE_NAME_MAX_BYTES, ATTACHMENT_GC_AGE, ATTACHMENT_GC_BATCH, ATTACHMENT_MAX_BYTES,
    ATTACHMENT_MAX_PER_MESSAGE, ATTACHMENT_MIME_MAX_BYTES, ATTACHMENT_RECONCILE_BATCH,
    ATTACHMENT_RESOURCE_KEY_MAX_BYTES, ATTACHMENT_TEMP_PREFIX, ATTACHMENT_TURN_TOTAL_BYTES,
};
use crate::store::{StoreError, StoreHandle};

/// Fixed marker-file contents proving a directory is a dedicated attachment
/// cache. Any other contents are rejected fail-closed at [`AttachmentCache::open`].
const CACHE_MARKER_CONTENTS: &[u8] = b"lark-codex-bridge attachment cache v1\n";

/// Bounded cache configuration. The [`Default`] values mirror the explicit
/// constants in [`crate::limits`]; tests may shrink them to exercise GC and
/// reconciliation deterministically.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttachmentLimits {
    /// Per-item hard cap before anything is written to disk.
    pub max_attachment_bytes: usize,
    /// Distinct resources accepted for one message/turn.
    pub max_attachments_per_message: usize,
    /// Aggregate bytes leased by one turn.
    pub max_turn_total_bytes: u64,
    /// Content files retained on disk.
    pub max_cache_files: usize,
    /// Total content bytes retained on disk.
    pub max_cache_bytes: u64,
    /// Display file name bytes retained as metadata only.
    pub max_file_name_bytes: usize,
    /// MIME type string bytes.
    pub max_mime_bytes: usize,
    /// Resource key bytes.
    pub max_resource_key_bytes: usize,
    /// Unleased attachments older than this become GC victims.
    pub gc_age: Duration,
    /// Victims examined and evicted by one GC pass.
    pub gc_batch: usize,
    /// Files deleted by one reconciliation pass (the destructive-cleanup cap;
    /// the scan itself always walks the whole directory).
    pub reconcile_batch: usize,
}

impl Default for AttachmentLimits {
    fn default() -> Self {
        Self {
            max_attachment_bytes: ATTACHMENT_MAX_BYTES,
            max_attachments_per_message: ATTACHMENT_MAX_PER_MESSAGE,
            max_turn_total_bytes: ATTACHMENT_TURN_TOTAL_BYTES,
            max_cache_files: ATTACHMENT_CACHE_MAX_FILES,
            max_cache_bytes: ATTACHMENT_CACHE_MAX_BYTES,
            max_file_name_bytes: ATTACHMENT_FILE_NAME_MAX_BYTES,
            max_mime_bytes: ATTACHMENT_MIME_MAX_BYTES,
            max_resource_key_bytes: ATTACHMENT_RESOURCE_KEY_MAX_BYTES,
            gc_age: ATTACHMENT_GC_AGE,
            gc_batch: ATTACHMENT_GC_BATCH,
            reconcile_batch: ATTACHMENT_RECONCILE_BATCH,
        }
    }
}

impl AttachmentLimits {
    /// Validates one resource key (length plus a URL-safe character set), so a
    /// hostile key can never influence a path or request target.
    ///
    /// # Errors
    ///
    /// Returns [`AttachError::InvalidResourceKey`] when the key is empty, too
    /// long, or contains unsafe characters.
    pub fn check_resource_key(&self, key: &str) -> Result<(), AttachError> {
        if key.is_empty() || key.len() > self.max_resource_key_bytes {
            return Err(AttachError::InvalidResourceKey {
                context: "checking a resource key",
            });
        }
        if !key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err(AttachError::InvalidResourceKey {
                context: "checking a resource key",
            });
        }
        Ok(())
    }

    /// Validates a bounded batch of resource descriptors (count and per-key
    /// safety). The scope actor uses this before issuing any download.
    ///
    /// # Errors
    ///
    /// Returns [`AttachError::TooManyResources`] for an oversized batch and
    /// [`AttachError::InvalidResourceKey`] for an unsafe key.
    pub fn check_resource_batch(&self, descs: &[ResourceDesc]) -> Result<(), AttachError> {
        if descs.len() > self.max_attachments_per_message {
            return Err(AttachError::TooManyResources {
                context: "checking a resource batch",
                limit: self.max_attachments_per_message,
            });
        }
        for desc in descs {
            self.check_resource_key(&desc.key)?;
        }
        Ok(())
    }

    /// Validates a downloaded byte length before any write to disk.
    ///
    /// # Errors
    ///
    /// Returns [`AttachError::TooLarge`] for an oversize object.
    pub fn check_attachment_bytes(&self, len: usize) -> Result<(), AttachError> {
        if len > self.max_attachment_bytes {
            return Err(AttachError::TooLarge {
                context: "downloading an attachment",
                limit: u64::try_from(self.max_attachment_bytes).unwrap_or(u64::MAX),
            });
        }
        Ok(())
    }

    /// Validates a running per-turn byte total.
    ///
    /// # Errors
    ///
    /// Returns [`AttachError::TurnTotalExceeded`] when `total` exceeds the
    /// per-turn budget.
    pub fn check_turn_total(&self, total: u64) -> Result<(), AttachError> {
        if total > self.max_turn_total_bytes {
            return Err(AttachError::TurnTotalExceeded {
                limit: self.max_turn_total_bytes,
            });
        }
        Ok(())
    }

    /// Validates a display file name used only as metadata (never a disk-path
    /// component): bounded length, no separators, no `..`, no control bytes.
    ///
    /// # Errors
    ///
    /// Returns [`AttachError::InvalidFileName`] for an unsafe name.
    pub fn check_file_name(&self, name: &str) -> Result<(), AttachError> {
        if name.is_empty() || name.len() > self.max_file_name_bytes {
            return Err(AttachError::InvalidFileName {
                context: "checking a display file name",
            });
        }
        if name == "." || name == ".." {
            return Err(AttachError::InvalidFileName {
                context: "checking a display file name",
            });
        }
        if name
            .bytes()
            .any(|byte| byte == b'/' || byte == b'\\' || byte == 0 || byte < 0x20 || byte == 0x7f)
        {
            return Err(AttachError::InvalidFileName {
                context: "checking a display file name",
            });
        }
        Ok(())
    }

    /// Validates a MIME type string: bounded length and printable ASCII only.
    ///
    /// # Errors
    ///
    /// Returns [`AttachError::InvalidMime`] for an unsafe MIME string.
    pub fn check_mime(&self, mime: &str) -> Result<(), AttachError> {
        if mime.is_empty() || mime.len() > self.max_mime_bytes {
            return Err(AttachError::InvalidMime {
                context: "checking a MIME type",
            });
        }
        if !mime.bytes().all(|byte| (0x20..=0x7e).contains(&byte)) {
            return Err(AttachError::InvalidMime {
                context: "checking a MIME type",
            });
        }
        Ok(())
    }
}

/// Coarse classification of a download failure, kept content-free.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadKind {
    /// Credentials are invalid; retrying will not help.
    PermanentAuth,
    /// Network, timeout, rate limit, or transient server failure.
    Retryable,
    /// The peer violated the expected protocol shape.
    Protocol,
    /// A configured count/byte bound was hit.
    Exhausted,
}

/// Attachment cache failures. Every variant carries static contexts, limits,
/// or classified kinds only — never bytes, message text, file names, or
/// absolute paths.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AttachError {
    /// The downloader failed.
    #[error("attachment download failed ({kind:?})")]
    Download {
        /// Coarse retry classification.
        kind: DownloadKind,
    },
    /// An object exceeded its per-item byte budget before touching disk.
    #[error("attachment exceeds the {limit}-byte limit while {context}")]
    TooLarge {
        /// Static description of the bounded operation.
        context: &'static str,
        /// The configured byte limit.
        limit: u64,
    },
    /// A resource batch exceeded its count budget.
    #[error("too many resources while {context} (limit {limit})")]
    TooManyResources {
        /// Static description of the bounded operation.
        context: &'static str,
        /// The configured count limit.
        limit: usize,
    },
    /// A turn exceeded its aggregate byte budget.
    #[error("attachment turn total exceeds the {limit}-byte limit")]
    TurnTotalExceeded {
        /// The configured byte limit.
        limit: u64,
    },
    /// A resource key was empty, oversized, or unsafe.
    #[error("attachment resource key is invalid while {context}")]
    InvalidResourceKey {
        /// Static description of the rejected key.
        context: &'static str,
    },
    /// A display file name was unsafe.
    #[error("attachment file name is invalid while {context}")]
    InvalidFileName {
        /// Static description of the rejected name.
        context: &'static str,
    },
    /// A MIME string was unsafe.
    #[error("attachment MIME type is invalid while {context}")]
    InvalidMime {
        /// Static description of the rejected MIME string.
        context: &'static str,
    },
    /// A bounded cache collection reached its limit.
    #[error("attachment cache capacity is exhausted while {context}")]
    CapacityExceeded {
        /// Static description of the bounded collection.
        context: &'static str,
    },
    /// Cached content does not match its expected hash or size.
    #[error("attachment content does not match its hash while {context}")]
    HashMismatch {
        /// Static description of the failed check.
        context: &'static str,
    },
    /// A deletion target was not a direct child of the cache root.
    #[error("attachment path is outside the cache root while {context}")]
    InvalidPath {
        /// Static description of the rejected path operation.
        context: &'static str,
    },
    /// A local filesystem operation failed.
    #[error("attachment I/O failure while {context}")]
    Io {
        /// Static description of the failed operation.
        context: &'static str,
    },
    /// The durable store rejected an operation.
    #[error("attachment store failure while {context}: {source}")]
    Store {
        /// Static description of the failed operation.
        context: &'static str,
        /// Classified store failure.
        #[source]
        source: StoreError,
    },
}

/// Downloads one message resource into memory. Implementations must return
/// already-bounded bytes; the cache re-checks the size before any write.
pub trait ResourceDownloader: Send + Sync {
    /// Fetches the raw resource bytes for `key` of `kind`.
    fn download(
        &self,
        message_id: &str,
        key: &str,
        kind: ResourceKind,
    ) -> BoxFuture<'static, Result<Bytes, AttachError>>;
}

/// [`ResourceDownloader`] adapter over the real [`LarkApi`] download path.
/// Wiring this into scope/turn input is Task 8 integration and intentionally
/// out of scope here.
pub struct LarkResourceDownloader {
    api: LarkApi,
}

impl LarkResourceDownloader {
    /// Creates an adapter over one tenant's API client.
    #[must_use]
    pub fn new(api: LarkApi) -> Self {
        Self { api }
    }
}

impl ResourceDownloader for LarkResourceDownloader {
    fn download(
        &self,
        message_id: &str,
        key: &str,
        kind: ResourceKind,
    ) -> BoxFuture<'static, Result<Bytes, AttachError>> {
        let api = self.api.clone();
        let message_id = message_id.to_owned();
        let key = key.to_owned();
        Box::pin(async move {
            api.download_message_resource(&message_id, &key, kind)
                .await
                .map(|data| data.bytes)
                .map_err(map_lark_error)
        })
    }
}

impl fmt::Debug for LarkResourceDownloader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LarkResourceDownloader")
            .finish_non_exhaustive()
    }
}

// `map_err` requires the adapter to accept the error by value even though the
// body only reads its classification.
#[allow(clippy::needless_pass_by_value)]
fn map_lark_error(error: LarkError) -> AttachError {
    let kind = match error {
        LarkError::PermanentAuth { .. } => DownloadKind::PermanentAuth,
        LarkError::Retryable { .. } => DownloadKind::Retryable,
        LarkError::ProtocolViolation { .. } => DownloadKind::Protocol,
        LarkError::Exhausted { .. } => DownloadKind::Exhausted,
    };
    AttachError::Download { kind }
}

/// One cached attachment handed back to the caller. `Debug` shows hash, kind,
/// and size only — never the on-disk path or content.
#[derive(Clone, PartialEq, Eq)]
pub struct CachedAttachment {
    /// Content hash (hex SHA-256).
    pub sha256: String,
    /// Canonical cache path (a direct child of the cache root).
    pub path: PathBuf,
    /// Resource kind.
    pub kind: ResourceKind,
    /// Object size in bytes.
    pub bytes: u64,
}

impl fmt::Debug for CachedAttachment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CachedAttachment")
            .field("sha256", &self.sha256)
            .field("kind", &self.kind)
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

/// Outcome of one GC pass: counts and bytes only.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct GcStats {
    /// Rows examined.
    pub inspected: u64,
    /// Unleased victims evicted.
    pub evicted: u64,
    /// Victims skipped because they held a lease.
    pub skipped_leased: u64,
    /// Content bytes freed.
    pub freed_bytes: u64,
}

/// Outcome of one reconciliation pass: counts and bytes only.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReconcileStats {
    /// Orphan temp files removed.
    pub temp_files: u64,
    /// Files removed because the store had no matching row.
    pub orphan_files: u64,
    /// Files removed because their size did not match their row.
    pub corrupt_files: u64,
    /// Store rows dropped (missing file, corrupt file, or malformed hash).
    pub dropped_rows: u64,
    /// Stale leases removed.
    pub stale_leases: u64,
    /// Directories skipped (never recursed into).
    pub skipped_dirs: u64,
    /// Directory entries that could not be inspected.
    pub errors: u64,
    /// Over-capacity cleanup performed at the end of reconciliation.
    pub gc: GcStats,
}

/// Content-addressed attachment cache rooted at one directory, backed by the
/// durable store, and fed by a downloader.
pub struct AttachmentCache {
    root: PathBuf,
    store: StoreHandle,
    downloader: Arc<dyn ResourceDownloader>,
    limits: AttachmentLimits,
    /// Serializes the destructive file/row pairs of `fetch` (install, commit,
    /// re-verify), `gc` (delete row, delete file), and `reconcile` against one
    /// another so a valid lease can never point at a missing file (B2).
    lock: tokio::sync::Mutex<()>,
}

impl fmt::Debug for AttachmentCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AttachmentCache")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl AttachmentCache {
    /// Creates (if needed) and canonicalizes the cache directory, then builds
    /// the cache. The directory must be a dedicated attachment cache: on first
    /// open an empty directory gets a valid [`ATTACHMENT_CACHE_MARKER`] marker
    /// written to prove ownership, and a non-empty directory is accepted only
    /// when its marker validates (fail-closed, so a misconfigured root such as
    /// `$HOME` is refused rather than scanned). Only after the marker
    /// validates is the directory tightened to owner-only permissions (0700)
    /// on Unix — a refused directory is never chmod'd — and a chmod failure is
    /// a non-fatal warning, never an error.
    ///
    /// Single-instance constraint: the cache root and store are private to one
    /// bridge instance; sharing one root across processes is unsupported (see
    /// the module docs).
    ///
    /// # Errors
    ///
    /// Returns an error when the directory cannot be created, canonicalized,
    /// is not a directory, is a symlink, or fails marker validation.
    pub fn open(
        root: &Path,
        store: StoreHandle,
        downloader: Arc<dyn ResourceDownloader>,
        limits: AttachmentLimits,
    ) -> Result<Self, AttachError> {
        ensure_cache_directory(root)?;
        let root = std::fs::canonicalize(root).map_err(|_| AttachError::Io {
            context: "resolving the cache directory",
        })?;
        validate_cache_marker(&root)?;
        // Only a validated dedicated directory may have its mode tightened; a
        // misconfigured root must be refused without mutating its permissions.
        tighten_permissions(&root);
        Ok(Self {
            root,
            store,
            downloader,
            limits,
            lock: tokio::sync::Mutex::new(()),
        })
    }

    /// Fetches one resource into the cache and leases it for `turn_row_id`.
    ///
    /// The resource key is validated, the download is size-checked before any
    /// write, content is hashed while being installed through a temp file plus
    /// atomic rename, and the store row and lease are recorded only after the
    /// file exists. A file already present for the same hash is re-verified
    /// and reused rather than rewritten.
    ///
    /// Enforcement boundary: `fetch` handles exactly one resource, so the
    /// individually-checkable bounds are the resource key length/safety
    /// ([`AttachmentLimits::check_resource_key`]) and the single-object byte
    /// cap ([`AttachmentLimits::check_attachment_bytes`]). The per-message
    /// count ([`AttachmentLimits::check_resource_batch`]) and the per-turn
    /// byte total ([`AttachmentLimits::check_turn_total`]) are turn-assembly
    /// responsibilities (plan Task 8 / B8), not fetch's; the display file-name
    /// and MIME checkers ([`AttachmentLimits::check_file_name`] and
    /// [`AttachmentLimits::check_mime`]) apply to metadata that
    /// [`ResourceDesc`] does not carry (only `kind` + `key`), so they remain
    /// public for the scope-actor wiring point that does carry that metadata.
    ///
    /// The install/commit/re-verify sequence runs under the per-cache lock so
    /// a concurrent same-process `gc`/`reconcile` cannot delete the file
    /// between the write and the row/lease commit. This is in-process
    /// serialization only; the cache root is single-instance (see the module
    /// docs).
    ///
    /// # Errors
    ///
    /// Returns a classified [`AttachError`] on validation, download, I/O, or
    /// store failure. Oversize content never reaches disk.
    pub async fn fetch(
        &self,
        message_id: &str,
        desc: &ResourceDesc,
        turn_row_id: i64,
    ) -> Result<CachedAttachment, AttachError> {
        self.limits.check_resource_key(&desc.key)?;
        let bytes = self
            .downloader
            .download(message_id, &desc.key, desc.kind)
            .await?;
        self.limits.check_attachment_bytes(bytes.len())?;
        let sha = sha256_hex(bytes.as_ref());
        let final_path = self.root.join(&sha);
        let size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        // Install + commit + re-verify run under the per-cache lock so GC's
        // "delete row then delete file" pair can never interleave between the
        // file write and the row/lease commit, which would otherwise leave a
        // valid lease pointing at a file GC then removed (B2). The network
        // download stays outside the lock.
        let _guard = self.lock.lock().await;
        self.install_file(&sha, bytes.as_ref())?;
        // Row and lease commit in one transaction (design §10, Task 7 Step 1),
        // so GC can never observe an unleased row and evict it mid-fetch.
        self.store
            .put_attachment_and_lease(&sha, size, resource_kind_str(desc.kind), turn_row_id)
            .await
            .map_err(|error| store_err("recording and leasing an attachment", error))?;
        // Close the file race: a concurrent reconcile may have removed the
        // still-rowless file as an orphan before the transaction committed, so
        // re-establish it now that the row+lease exist (bytes are in hand).
        if verify_existing(&final_path, &sha, bytes.len()).is_err() {
            self.install_file(&sha, bytes.as_ref())?;
        }
        Ok(CachedAttachment {
            sha256: sha,
            path: final_path,
            kind: desc.kind,
            bytes: size,
        })
    }

    /// Releases every attachment lease held by a turn, returning the number
    /// released. Called from turn finalization.
    ///
    /// # Errors
    ///
    /// Returns a classified store failure.
    pub async fn release_turn(&self, turn_row_id: i64) -> Result<u64, AttachError> {
        self.store
            .release_turn_attachment_leases(turn_row_id)
            .await
            .map_err(|error| store_err("releasing turn attachment leases", error))
    }

    /// Evicts unleased attachments, oldest (`last_used_ms`) first, until the
    /// cache is within its file/byte caps and no aged-out unleased entries
    /// remain. Leased rows are never deleted. Work is bounded by
    /// [`AttachmentLimits::gc_batch`]. Runs under the per-cache lock, which
    /// serializes it against same-process `fetch`/`reconcile`; the cache root
    /// is single-instance, so there is no cross-process guarantee (see the
    /// module docs).
    ///
    /// # Errors
    ///
    /// Returns a classified store or I/O failure.
    pub async fn gc(&self) -> Result<GcStats, AttachError> {
        let _guard = self.lock.lock().await;
        self.gc_inner().await
    }

    /// Eviction pass without the per-cache lock; callers hold it (so
    /// `reconcile` can run GC inside its own critical section without
    /// re-acquiring the lock and deadlocking).
    async fn gc_inner(&self) -> Result<GcStats, AttachError> {
        let rows = self
            .store
            .list_attachments()
            .await
            .map_err(|error| store_err("listing attachments for gc", error))?;
        let mut stats = GcStats::default();
        let mut files = u64::try_from(rows.len()).unwrap_or(u64::MAX);
        let mut total_bytes = rows
            .iter()
            .fold(0_u64, |acc, row| acc.saturating_add(row.bytes));
        let age_ms = i64::try_from(self.limits.gc_age.as_millis()).unwrap_or(i64::MAX);
        let batch = u64::try_from(self.limits.gc_batch).unwrap_or(u64::MAX);
        let max_files = u64::try_from(self.limits.max_cache_files).unwrap_or(u64::MAX);
        let now = now_ms();

        for row in &rows {
            if stats.evicted >= batch {
                break;
            }
            stats.inspected = stats.inspected.saturating_add(1);
            let aged = now.saturating_sub(row.last_used_ms) >= age_ms;
            let over = files > max_files || total_bytes > self.limits.max_cache_bytes;
            if !aged && !over {
                continue;
            }
            let deleted = self
                .store
                .delete_attachment(&row.sha256)
                .await
                .map_err(|error| store_err("deleting an attachment during gc", error))?;
            if !deleted {
                stats.skipped_leased = stats.skipped_leased.saturating_add(1);
                continue;
            }
            // Only after the row was atomically confirmed unleased may the
            // file go; a crash here leaves an orphan, never a dangling row.
            if is_valid_sha256_name(&row.sha256) {
                // Defense in depth against sharing one cache root across
                // processes (unsupported, see module docs): the row was
                // deleted above, but another instance could re-install the
                // same hash between that delete and this unlink. Re-checking
                // narrows that window without closing it, so it is best
                // effort, not a correctness guarantee.
                if self
                    .store
                    .attachment_row(&row.sha256)
                    .await
                    .map_err(|error| store_err("re-checking an attachment before unlink", error))?
                    .is_some()
                {
                    continue;
                }
                let _ = self.remove_file(
                    &self.root.join(&row.sha256),
                    "removing an evicted cached file",
                )?;
            }
            files = files.saturating_sub(1);
            total_bytes = total_bytes.saturating_sub(row.bytes);
            stats.evicted = stats.evicted.saturating_add(1);
            stats.freed_bytes = stats.freed_bytes.saturating_add(row.bytes);
        }
        Ok(stats)
    }

    /// Reconciles the cache directory with the store at startup. Handles
    /// residual temp files, files without a store row, store rows without a
    /// file, size-mismatched files, stale leases, and over-capacity caches.
    /// The directory scan is one complete streaming pass (never truncated);
    /// destructive file cleanup is capped at
    /// [`AttachmentLimits::reconcile_batch`] deletions per pass, so repeated
    /// passes converge. Bounded, idempotent, and repeatable: a single bad
    /// entry is skipped, never fatal.
    ///
    /// # Errors
    ///
    /// Returns a classified store or I/O failure. Per-entry inspection
    /// failures are recorded in [`ReconcileStats::errors`] instead of aborting.
    pub async fn reconcile(&self) -> Result<ReconcileStats, AttachError> {
        let _guard = self.lock.lock().await;
        self.reconcile_locked().await
    }

    /// Reconciliation pass without the per-cache lock; callers hold it.
    async fn reconcile_locked(&self) -> Result<ReconcileStats, AttachError> {
        let mut stats = ReconcileStats::default();
        let rows = self
            .store
            .list_attachments()
            .await
            .map_err(|error| store_err("listing attachments for reconciliation", error))?;

        let mut rows_by_sha: HashMap<String, u64> = HashMap::new();
        let mut drop_rows: HashSet<String> = HashSet::new();
        for row in &rows {
            if is_valid_sha256_name(&row.sha256) {
                rows_by_sha.insert(row.sha256.clone(), row.bytes);
            } else {
                drop_rows.insert(row.sha256.clone());
            }
        }

        self.scan_entries(&rows_by_sha, &mut drop_rows, &mut stats)?;

        for sha in &drop_rows {
            if self
                .store
                .delete_attachment_force(sha)
                .await
                .map_err(|error| store_err("dropping a dangling attachment row", error))?
            {
                stats.dropped_rows = stats.dropped_rows.saturating_add(1);
            }
        }
        // Missing-file detection is per-row and independent of the directory
        // scan: a store row whose backing file is not a regular file on disk
        // is dangling and dropped (leases cascade). The scan is one complete
        // pass (never truncated), so there is no "unscanned" tail to confuse
        // with "missing"; this loop is therefore always complete.
        for sha in rows_by_sha.keys() {
            if drop_rows.contains(sha) {
                continue;
            }
            if !regular_file_exists(&self.root.join(sha))
                && self
                    .store
                    .delete_attachment_force(sha)
                    .await
                    .map_err(|error| store_err("dropping a missing attachment row", error))?
            {
                stats.dropped_rows = stats.dropped_rows.saturating_add(1);
            }
        }

        stats.stale_leases = self
            .store
            .delete_stale_attachment_leases()
            .await
            .map_err(|error| store_err("deleting stale attachment leases", error))?;
        stats.gc = self.gc_inner().await?;
        Ok(stats)
    }

    /// Streams one complete `read_dir` pass over the cache root, deleting
    /// orphan temp files, orphan/unrecognized content files, and
    /// size-mismatched files. Entries are inspected one at a time and never
    /// materialized into a sorted vector, so memory is O(1) in the directory
    /// size (the `rows_by_sha`/`drop_rows` working set is bounded by the
    /// store's attachment-row cap, not by directory size). Destructive cleanup
    /// is capped at [`AttachmentLimits::reconcile_batch`] deletions per pass;
    /// the scan itself is never truncated, so each pass walks the whole
    /// directory and orphan cleanup converges because the entry count strictly
    /// decreases by at most `reconcile_batch` files per pass.
    fn scan_entries(
        &self,
        rows_by_sha: &HashMap<String, u64>,
        drop_rows: &mut HashSet<String>,
        stats: &mut ReconcileStats,
    ) -> Result<(), AttachError> {
        let dir = std::fs::read_dir(&self.root).map_err(|_| AttachError::Io {
            context: "reading the cache directory",
        })?;
        let batch = self.limits.reconcile_batch;
        let mut deletions = 0_usize;

        for entry in dir {
            let Ok(entry) = entry else {
                stats.errors = stats.errors.saturating_add(1);
                continue;
            };
            let path = entry.path();
            let file_name = entry.file_name();
            if file_name.to_string_lossy() == ATTACHMENT_CACHE_MARKER {
                continue;
            }
            let file_type = if let Ok(metadata) = std::fs::symlink_metadata(&path) {
                metadata.file_type()
            } else {
                stats.errors = stats.errors.saturating_add(1);
                continue;
            };
            if file_type.is_dir() {
                stats.skipped_dirs = stats.skipped_dirs.saturating_add(1);
                continue;
            }
            if !file_type.is_file() {
                // Symlinks and special files are never followed or deleted.
                continue;
            }
            // A regular file that is a deletion candidate: apply the per-pass
            // deletion cap. Candidates beyond the cap are left for the next
            // pass (the scan restarts from the directory head, so they are
            // not lost).
            if deletions >= batch {
                continue;
            }
            let Some(name) = file_name.to_str() else {
                if self.remove_file(&path, "removing an unrecognized cache file")? {
                    stats.orphan_files = stats.orphan_files.saturating_add(1);
                    deletions = deletions.saturating_add(1);
                }
                continue;
            };
            if name.starts_with(ATTACHMENT_TEMP_PREFIX) {
                if self.remove_file(&path, "removing an orphan temp file")? {
                    stats.temp_files = stats.temp_files.saturating_add(1);
                    deletions = deletions.saturating_add(1);
                }
                continue;
            }
            if is_valid_sha256_name(name) {
                if let Some(&expected) = rows_by_sha.get(name) {
                    let actual = std::fs::metadata(&path).map_or(u64::MAX, |m| m.len());
                    if actual != expected {
                        if self.remove_file(&path, "removing a corrupt cached file")? {
                            stats.corrupt_files = stats.corrupt_files.saturating_add(1);
                            deletions = deletions.saturating_add(1);
                        }
                        drop_rows.insert(name.to_owned());
                    }
                } else if self.remove_file(&path, "removing an orphan cached file")? {
                    stats.orphan_files = stats.orphan_files.saturating_add(1);
                    deletions = deletions.saturating_add(1);
                }
                continue;
            }
            if self.remove_file(&path, "removing an unrecognized cache file")? {
                stats.orphan_files = stats.orphan_files.saturating_add(1);
                deletions = deletions.saturating_add(1);
            }
        }
        Ok(())
    }

    /// Writes `bytes` to a same-directory temp file, `fsync`s, and atomically
    /// renames it to `<sha>`. If the target already holds matching content it
    /// is reused; a corrupt target is replaced. Same-hash concurrent writers
    /// produce identical bytes, so the winner is always correct.
    fn install_file(&self, sha: &str, bytes: &[u8]) -> Result<(), AttachError> {
        let final_path = self.root.join(sha);
        for _ in 0..2 {
            let temp = self.write_temp(bytes)?;
            if final_path.exists() {
                if verify_existing(&final_path, sha, bytes.len()).is_ok() {
                    return Ok(());
                }
                self.remove_file(&final_path, "removing a corrupt cached file")?;
            }
            match std::fs::rename(&temp.path, &final_path) {
                Ok(()) => {
                    // Make the rename durable before the row is committed:
                    // without syncing the parent directory, a power loss could
                    // commit the store row while losing the rename (B4).
                    sync_parent_dir(&self.root)?;
                    return Ok(());
                }
                Err(_) if final_path.exists() => {
                    if verify_existing(&final_path, sha, bytes.len()).is_ok() {
                        return Ok(());
                    }
                    self.remove_file(&final_path, "removing a corrupt cached file")?;
                }
                Err(_) => {
                    return Err(AttachError::Io {
                        context: "renaming a temp file into the cache",
                    });
                }
            }
        }
        Err(AttachError::Io {
            context: "installing a cached file after retries",
        })
    }

    /// Writes bytes to a freshly created temp file and `fsync`s it. The
    /// returned guard removes the file on any error path.
    fn write_temp(&self, bytes: &[u8]) -> Result<TempFile, AttachError> {
        let path = self
            .root
            .join(format!("{ATTACHMENT_TEMP_PREFIX}{}", Uuid::new_v4()));
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|_| AttachError::Io {
                context: "creating a temp file",
            })?;
        let guard = TempFile { path };
        let write = (|| -> Result<(), AttachError> {
            file.write_all(bytes).map_err(|_| AttachError::Io {
                context: "writing a temp file",
            })?;
            file.flush().map_err(|_| AttachError::Io {
                context: "flushing a temp file",
            })?;
            file.sync_all().map_err(|_| AttachError::Io {
                context: "syncing a temp file",
            })?;
            Ok(())
        })();
        write?;
        Ok(guard)
    }

    /// Removes one file, verifying it is a direct child of the cache root
    /// first so a dirty store row can never direct a deletion outside it.
    fn remove_file(&self, path: &Path, context: &'static str) -> Result<bool, AttachError> {
        if path.parent() != Some(self.root.as_path()) {
            return Err(AttachError::InvalidPath {
                context: "deleting outside the cache root",
            });
        }
        match std::fs::remove_file(path) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(_) => Err(AttachError::Io { context }),
        }
    }
}

/// Ensures `root` exists as a real (non-symlink) directory, creating it when
/// absent. A symlink or a non-directory is refused fail-closed.
fn ensure_cache_directory(root: &Path) -> Result<(), AttachError> {
    match std::fs::symlink_metadata(root) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                return Err(AttachError::InvalidPath {
                    context: "cache root must not be a symlink",
                });
            }
            if !metadata.is_dir() {
                return Err(AttachError::InvalidPath {
                    context: "cache root is not a directory",
                });
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(root).map_err(|_| AttachError::Io {
                context: "creating the cache directory",
            })?;
        }
        Err(_) => {
            return Err(AttachError::Io {
                context: "reading the cache directory",
            });
        }
    }
    Ok(())
}

/// Tightens a validated cache directory to owner-only permissions (0700) on
/// Unix. Failure is a non-fatal `tracing::warn` — never an error, never a
/// panic — because the directory already passed marker validation and the
/// cache must still open. No-op on non-Unix platforms.
fn tighten_permissions(root: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(error) = std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)) {
            tracing::warn!(
                error = %error,
                "failed to tighten attachment cache directory permissions to 0700"
            );
        }
    }
    #[cfg(not(unix))]
    let _ = root;
}

/// Validates the dedicated-directory marker, writing one into a fresh empty
/// directory. A non-empty directory without a valid marker is refused
/// fail-closed without creating or deleting anything.
fn validate_cache_marker(root: &Path) -> Result<(), AttachError> {
    let marker = root.join(ATTACHMENT_CACHE_MARKER);
    let mut entries = std::fs::read_dir(root).map_err(|_| AttachError::Io {
        context: "reading the cache directory",
    })?;
    if entries.next().is_none() {
        return write_marker(&marker);
    }
    // Non-empty directory: the marker must already exist and validate. A
    // missing or wrong marker is a misconfigured directory, never an I/O
    // error, so the caller gets a clear fail-closed `InvalidPath`.
    if !marker.exists() {
        return Err(AttachError::InvalidPath {
            context: "cache directory is not a dedicated attachment cache",
        });
    }
    if verify_marker(&marker)? {
        Ok(())
    } else {
        Err(AttachError::InvalidPath {
            context: "cache directory is not a dedicated attachment cache",
        })
    }
}

/// Reads and validates the marker file contents. `false` means the contents
/// are present but wrong; an unreadable marker surfaces as an I/O error.
fn verify_marker(path: &Path) -> Result<bool, AttachError> {
    let contents = std::fs::read(path).map_err(|_| AttachError::Io {
        context: "reading the cache marker",
    })?;
    Ok(contents == CACHE_MARKER_CONTENTS)
}

/// Writes the marker file (0600 on Unix) without ever overwriting an existing
/// one; a concurrent first-open that lost the `create_new` race re-validates.
fn write_marker(path: &Path) -> Result<(), AttachError> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(path) {
        Ok(mut file) => {
            file.write_all(CACHE_MARKER_CONTENTS)
                .map_err(|_| AttachError::Io {
                    context: "writing the cache marker",
                })?;
            file.sync_all().map_err(|_| AttachError::Io {
                context: "syncing the cache marker",
            })?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if verify_marker(path)? {
                Ok(())
            } else {
                Err(AttachError::InvalidPath {
                    context: "cache directory is not a dedicated attachment cache",
                })
            }
        }
        Err(_) => Err(AttachError::Io {
            context: "creating the cache marker",
        }),
    }
}

/// Best-effort durability of a directory entry after a rename. On Unix this
/// `fsync`s the parent directory so the rename survives a power loss; Windows
/// has no portable directory-handle sync, so it is a no-op there.
fn sync_parent_dir(root: &Path) -> Result<(), AttachError> {
    #[cfg(unix)]
    {
        let dir = std::fs::File::open(root).map_err(|_| AttachError::Io {
            context: "opening the cache directory for sync",
        })?;
        dir.sync_all().map_err(|_| AttachError::Io {
            context: "syncing the cache directory",
        })?;
    }
    #[cfg(not(unix))]
    let _ = root;
    Ok(())
}

/// Whether a non-symlink regular file exists at `path` (matches the scanner's
/// `file_type().is_file()` semantics, so symlinks never count as present).
fn regular_file_exists(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_file())
}

/// RAII removal of a temp file on any error path.
struct TempFile {
    path: PathBuf,
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn store_err(context: &'static str, source: StoreError) -> AttachError {
    AttachError::Store { context, source }
}

/// Streams a bounded read of `path`, verifying both length and SHA-256
/// against the expected values.
fn verify_existing(
    path: &Path,
    expected_sha: &str,
    expected_len: usize,
) -> Result<(), AttachError> {
    let mut file = std::fs::File::open(path).map_err(|_| AttachError::Io {
        context: "opening a cached file for verification",
    })?;
    let metadata = file.metadata().map_err(|_| AttachError::Io {
        context: "reading a cached file",
    })?;
    let actual_len = usize::try_from(metadata.len()).unwrap_or(usize::MAX);
    if actual_len != expected_len {
        return Err(AttachError::HashMismatch {
            context: "cached file size",
        });
    }
    let mut hasher = Sha256::new();
    let mut total = 0_usize;
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|_| AttachError::Io {
            context: "reading a cached file",
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        total = total.saturating_add(read);
        if total > expected_len {
            return Err(AttachError::HashMismatch {
                context: "cached file grew during verification",
            });
        }
    }
    if total != expected_len {
        return Err(AttachError::HashMismatch {
            context: "cached file size",
        });
    }
    let digest = hasher.finalize();
    if sha256_hex(digest.as_slice()) != expected_sha {
        return Err(AttachError::HashMismatch {
            context: "cached file content",
        });
    }
    Ok(())
}

fn resource_kind_str(kind: ResourceKind) -> &'static str {
    match kind {
        ResourceKind::Image => "image",
        ResourceKind::File => "file",
    }
}

fn sha256_hex(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    let mut encoded = String::with_capacity(64);
    for byte in &digest {
        use fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to a string cannot fail");
    }
    encoded
}

/// A valid content file name is exactly 64 lowercase hex digits (SHA-256).
fn is_valid_sha256_name(name: &str) -> bool {
    name.len() == 64
        && name
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn now_ms() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis());
    i64::try_from(millis).unwrap_or(i64::MAX)
}
