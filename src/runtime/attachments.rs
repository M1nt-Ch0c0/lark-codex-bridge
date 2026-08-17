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
//! Single-instance enforcement: the cache root and its backing store are
//! private to one bridge instance. [`AttachmentCache::open`] acquires a
//! non-blocking OS-level exclusive advisory lock on
//! [`ATTACHMENT_INSTANCE_LOCK`]; the kernel releases it on clean exit and on
//! crash, with no stale-file deletion protocol. Within one process the
//! per-cache [`tokio::sync::Mutex`] and the single-writer WAL `SQLite` store
//! serialize `fetch`/`gc`/`reconcile`, so a valid lease cannot point at a
//! missing file. Blocking mutations carry an owned mutex guard into Tokio's
//! blocking pool; cancelling the caller therefore cannot release the cache
//! lock while a detached filesystem operation is still running.
//!
//! Reconciliation keeps a resumable `ReadDir` iterator per cache and consumes
//! at most [`AttachmentLimits::reconcile_batch`] entries per call. Directory
//! inspection and candidate application run on the blocking pool; candidates
//! are re-verified while the in-process cache lock is held. Repeated calls
//! advance to EOF, restart a fresh cycle, and converge without an unbounded
//! directory walk or candidate vector.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures_util::future::BoxFuture;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::lark::api::{LarkApi, ResourceKind};
use crate::lark::error::LarkError;
use crate::lark::normalize::ResourceDesc;
use crate::limits::{
    ATTACHMENT_CACHE_MARKER, ATTACHMENT_CACHE_MAX_BYTES, ATTACHMENT_CACHE_MAX_FILES,
    ATTACHMENT_FILE_NAME_MAX_BYTES, ATTACHMENT_GC_AGE, ATTACHMENT_GC_BATCH,
    ATTACHMENT_INSTANCE_LOCK, ATTACHMENT_MAX_BYTES, ATTACHMENT_MAX_PER_MESSAGE,
    ATTACHMENT_MIME_MAX_BYTES, ATTACHMENT_RECONCILE_BATCH, ATTACHMENT_RESOURCE_KEY_MAX_BYTES,
    ATTACHMENT_TEMP_PREFIX, ATTACHMENT_TURN_TOTAL_BYTES,
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
    /// Directory entries inspected (and maximum candidates deleted) by one
    /// reconciliation pass.
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
    /// Returns a copy with cleanup batches clamped to their global hard bounds
    /// and a positive minimum, so caller-provided values cannot create an
    /// unbounded pass or a non-converging zero-sized pass.
    #[must_use]
    pub fn clamped(self) -> Self {
        Self {
            gc_batch: self.gc_batch.clamp(1, ATTACHMENT_GC_BATCH),
            reconcile_batch: self.reconcile_batch.clamp(1, ATTACHMENT_RECONCILE_BATCH),
            ..self
        }
    }

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
    /// The caller cancelled before the attachment became usable.
    #[error("attachment fetch was cancelled while {context}")]
    Cancelled {
        /// Static description of the interrupted phase.
        context: &'static str,
    },
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
    /// Directory entries consumed from the resumable iterator this pass.
    pub scanned_entries: u64,
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
    lock: Arc<tokio::sync::Mutex<()>>,
    /// Serializes calls that advance the resumable directory iterator.
    reconcile_lock: tokio::sync::Mutex<()>,
    /// Resumable directory iterator. It is moved into the blocking worker for
    /// one bounded batch, then returned here for the next call.
    scan: std::sync::Mutex<Option<std::fs::ReadDir>>,
    /// RAII owner of the OS-released cross-instance file lock.
    instance_lock: Arc<InstanceLockGuard>,
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
    /// written to prove ownership (0600 on Unix), and a non-empty directory is
    /// accepted only when its marker validates (fail-closed, so a misconfigured
    /// root such as `$HOME` is refused rather than scanned). Only after the
    /// marker validates is the directory tightened to owner-only permissions
    /// (0700) on Unix — a refused directory is never chmod'd — and a chmod
    /// failure is fail-closed: a validated dedicated directory that cannot be
    /// tightened is refused rather than left readable with plaintext cache
    /// content on disk.
    ///
    /// Single-instance enforcement: after marker validation and permission
    /// tightening, the bridge opens [`ATTACHMENT_INSTANCE_LOCK`] as 0600 and
    /// acquires a non-blocking exclusive advisory lock. A live second bridge
    /// is refused; the kernel releases the lock on normal exit or crash.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory cannot be created, canonicalized,
    /// is not a directory, is a symlink, fails marker validation, cannot be
    /// tightened to 0700, or is already owned by a live instance.
    pub fn open(
        root: &Path,
        store: StoreHandle,
        downloader: Arc<dyn ResourceDownloader>,
        limits: AttachmentLimits,
    ) -> Result<Self, AttachError> {
        let limits = limits.clamped();
        ensure_cache_directory(root)?;
        let root = std::fs::canonicalize(root).map_err(|_| AttachError::Io {
            context: "resolving the cache directory",
        })?;
        validate_cache_marker(&root)?;
        // Only a validated dedicated directory may have its mode tightened; a
        // misconfigured root must be refused without mutating its permissions.
        tighten_permissions(&root)?;
        let instance_lock = acquire_instance_lock(&root)?;
        Ok(Self {
            root,
            store,
            downloader,
            limits,
            lock: Arc::new(tokio::sync::Mutex::new(())),
            reconcile_lock: tokio::sync::Mutex::new(()),
            scan: std::sync::Mutex::new(None),
            instance_lock: Arc::new(instance_lock),
        })
    }

    /// Returns the validated hard limits used by this cache. Turn assembly
    /// uses the same values for per-message counts and aggregate turn bytes,
    /// avoiding a second configuration source at the routing boundary.
    #[must_use]
    pub fn limits(&self) -> AttachmentLimits {
        self.limits
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
        self.fetch_inner(message_id, desc, turn_row_id, None).await
    }

    /// Fetches one resource while observing actor shutdown without abandoning
    /// a partially committed lease. Network work is cancelled immediately;
    /// once local mutation begins, the bounded phase is allowed to settle so
    /// the caller can reliably release any resulting turn lease.
    pub(crate) async fn fetch_cancellable(
        &self,
        message_id: &str,
        desc: &ResourceDesc,
        turn_row_id: i64,
        shutdown: &CancellationToken,
    ) -> Result<CachedAttachment, AttachError> {
        self.fetch_inner(message_id, desc, turn_row_id, Some(shutdown))
            .await
    }

    async fn fetch_inner(
        &self,
        message_id: &str,
        desc: &ResourceDesc,
        turn_row_id: i64,
        shutdown: Option<&CancellationToken>,
    ) -> Result<CachedAttachment, AttachError> {
        self.limits.check_resource_key(&desc.key)?;
        let download = self.downloader.download(message_id, &desc.key, desc.kind);
        let bytes = if let Some(shutdown) = shutdown {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    return Err(AttachError::Cancelled {
                        context: "downloading an attachment",
                    });
                }
                result = download => result?,
            }
        } else {
            download.await?
        };
        self.limits.check_attachment_bytes(bytes.len())?;
        let hash_bytes = bytes.clone();
        let sha = tokio::task::spawn_blocking(move || sha256_hex(hash_bytes.as_ref()))
            .await
            .map_err(|_| AttachError::Io {
                context: "hashing a downloaded attachment",
            })?;
        if shutdown.is_some_and(CancellationToken::is_cancelled) {
            return Err(AttachError::Cancelled {
                context: "hashing a downloaded attachment",
            });
        }
        let final_path = self.root.join(&sha);
        let size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        // Install + commit + re-verify run under the per-cache lock so GC's
        // "delete row then delete file" pair can never interleave between the
        // file write and the row/lease commit, which would otherwise leave a
        // valid lease pointing at a file GC then removed (B2). The network
        // download stays outside the lock.
        let mut guard = Some(Arc::clone(&self.lock).lock_owned().await);
        if shutdown.is_some_and(CancellationToken::is_cancelled) {
            return Err(AttachError::Cancelled {
                context: "waiting to install an attachment",
            });
        }
        let root = self.root.clone();
        let install_sha = sha.clone();
        let install_bytes = bytes.clone();
        run_locked_blocking(
            &mut guard,
            Arc::clone(&self.instance_lock),
            "installing a downloaded attachment",
            move || install_file(&root, &install_sha, install_bytes.as_ref()),
        )
        .await?;
        if shutdown.is_some_and(CancellationToken::is_cancelled) {
            return Err(AttachError::Cancelled {
                context: "installing a downloaded attachment",
            });
        }
        // Row and lease commit in one transaction (design §10, Task 7 Step 1),
        // so GC can never observe an unleased row and evict it mid-fetch.
        self.store
            .put_attachment_and_lease(&sha, size, resource_kind_str(desc.kind), turn_row_id)
            .await
            .map_err(|error| store_err("recording and leasing an attachment", error))?;
        // Close the file race: a concurrent reconcile may have removed the
        // still-rowless file as an orphan before the transaction committed, so
        // re-establish it now that the row+lease exist (bytes are in hand).
        let root = self.root.clone();
        let verify_sha = sha.clone();
        let verify_bytes = bytes.clone();
        run_locked_blocking(
            &mut guard,
            Arc::clone(&self.instance_lock),
            "verifying an installed attachment",
            move || {
                let path = root.join(&verify_sha);
                if verify_existing(&path, &verify_sha, verify_bytes.len()).is_err() {
                    install_file(&root, &verify_sha, verify_bytes.as_ref())?;
                }
                Ok::<(), AttachError>(())
            },
        )
        .await?;
        if shutdown.is_some_and(CancellationToken::is_cancelled) {
            return Err(AttachError::Cancelled {
                context: "verifying an installed attachment",
            });
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
    /// serializes it against same-process `fetch`/`reconcile`; the cache root's
    /// advisory lock excludes a second cooperating bridge process.
    ///
    /// # Errors
    ///
    /// Returns a classified store or I/O failure.
    pub async fn gc(&self) -> Result<GcStats, AttachError> {
        let mut guard = Some(Arc::clone(&self.lock).lock_owned().await);
        self.gc_inner(&mut guard).await
    }

    /// Eviction pass without the per-cache lock; callers hold it (so
    /// `reconcile` can run GC inside its own critical section without
    /// re-acquiring the lock and deadlocking).
    async fn gc_inner(
        &self,
        guard: &mut Option<tokio::sync::OwnedMutexGuard<()>>,
    ) -> Result<GcStats, AttachError> {
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
            if stats.inspected >= batch {
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
                // The row is rechecked after deletion before unlinking. The
                // OS-level instance lock excludes another bridge process and
                // the in-process mutex excludes fetch/reconcile here.
                if self
                    .store
                    .attachment_row(&row.sha256)
                    .await
                    .map_err(|error| store_err("re-checking an attachment before unlink", error))?
                    .is_some()
                {
                    continue;
                }
                let root = self.root.clone();
                let sha = row.sha256.clone();
                run_locked_blocking(
                    guard,
                    Arc::clone(&self.instance_lock),
                    "removing an evicted cached file",
                    move || {
                        remove_direct_child(
                            &root,
                            &root.join(sha),
                            "removing an evicted cached file",
                        )
                        .map(|_| ())
                    },
                )
                .await?;
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
    ///
    /// A resumable `ReadDir` iterator consumes at most
    /// [`AttachmentLimits::reconcile_batch`] entries per call on the blocking
    /// pool. Candidate rechecks, metadata calls, and file removal also run on
    /// that pool while the cache mutation lock is held. Repeated calls advance
    /// to EOF and start a fresh scan cycle; one bad entry is counted and
    /// skipped, never fatal.
    ///
    /// # Errors
    ///
    /// Returns a classified store or I/O failure. Per-entry inspection
    /// failures are recorded in [`ReconcileStats::errors`] instead of aborting.
    pub async fn reconcile(&self) -> Result<ReconcileStats, AttachError> {
        let _reconcile_guard = self.reconcile_lock.lock().await;
        // Phase A — snapshot valid rows to seed the scan's classification.
        // This is a pure store read (serialized by the store's own writer) and
        // only a hint: every destructive decision is re-verified against a
        // fresh read under the lock in phase C, so a `fetch`/`gc` that commits
        // or removes rows while the scan runs cannot be harmed.
        let rows = self
            .store
            .list_attachments()
            .await
            .map_err(|error| store_err("listing attachments for reconciliation", error))?;
        let mut snapshot: HashMap<String, u64> = HashMap::new();
        for row in &rows {
            if is_valid_sha256_name(&row.sha256) {
                snapshot.insert(row.sha256.clone(), row.bytes);
            }
        }

        // Phase B — take the resumable iterator out of its mutex, advance one
        // strictly bounded batch off-worker, and put the returned iterator
        // back before applying candidates.
        let iterator = self
            .scan
            .lock()
            .map_err(|_| AttachError::Io {
                context: "locking the reconciliation scan state",
            })?
            .take();
        let root = self.root.clone();
        let batch = self.limits.reconcile_batch;
        let scan = tokio::task::spawn_blocking(move || {
            scan_directory_batch(&root, &snapshot, batch, iterator)
        })
        .await
        .map_err(|_| AttachError::Io {
            context: "scanning the cache directory",
        })??;
        *self.scan.lock().map_err(|_| AttachError::Io {
            context: "locking the reconciliation scan state",
        })? = scan.next;

        // Phase C — apply the candidates under the per-cache lock, re-verified
        // against fresh store rows.
        let mut guard = Some(Arc::clone(&self.lock).lock_owned().await);
        self.reconcile_locked(scan.candidates, &mut guard).await
    }

    /// Destructive half of reconciliation; the caller holds the per-cache
    /// lock. Every candidate from the off-lock scan is re-verified against a
    /// fresh row snapshot before being unlinked, and row drops / stale-lease
    /// cleanup / GC run exactly as before under the same lock.
    async fn reconcile_locked(
        &self,
        candidates: ScanCandidates,
        guard: &mut Option<tokio::sync::OwnedMutexGuard<()>>,
    ) -> Result<ReconcileStats, AttachError> {
        let rows = self
            .store
            .list_attachments()
            .await
            .map_err(|error| store_err("listing attachments for reconciliation", error))?;

        let mut rows_by_sha: HashMap<String, u64> = HashMap::new();
        let mut malformed_rows: HashSet<String> = HashSet::new();
        for row in &rows {
            if is_valid_sha256_name(&row.sha256) {
                rows_by_sha.insert(row.sha256.clone(), row.bytes);
            } else {
                malformed_rows.insert(row.sha256.clone());
            }
        }

        let batch = self.limits.reconcile_batch;
        let root = self.root.clone();
        let applied = run_locked_blocking(
            guard,
            Arc::clone(&self.instance_lock),
            "applying reconciliation candidates",
            move || apply_scan_candidates(&root, candidates, &rows_by_sha, malformed_rows, batch),
        )
        .await?;
        let mut stats = applied.stats;
        for sha in &applied.drop_rows {
            let dropped = self
                .store
                .delete_attachment_force(sha)
                .await
                .map_err(|error| store_err("dropping a dangling attachment row", error))?;
            if dropped {
                stats.dropped_rows = stats.dropped_rows.saturating_add(1);
            }
            if applied.delete_after_drop.contains(sha)
                && self
                    .store
                    .attachment_row(sha)
                    .await
                    .map_err(|error| {
                        store_err("re-checking a corrupt attachment before unlink", error)
                    })?
                    .is_none()
            {
                let root = self.root.clone();
                let sha = sha.clone();
                let removed = run_locked_blocking(
                    guard,
                    Arc::clone(&self.instance_lock),
                    "removing a corrupt cached file",
                    move || {
                        remove_direct_child(
                            &root,
                            &root.join(sha),
                            "removing a corrupt cached file",
                        )
                    },
                )
                .await?;
                if removed {
                    stats.corrupt_files = stats.corrupt_files.saturating_add(1);
                }
            }
        }
        stats.stale_leases = self
            .store
            .delete_stale_attachment_leases()
            .await
            .map_err(|error| store_err("deleting stale attachment leases", error))?;
        stats.gc = self.gc_inner(guard).await?;
        Ok(stats)
    }
}

/// Runs one filesystem mutation on Tokio's blocking pool without opening a
/// cancellation window in the cache critical section. The owned mutex guard
/// and an owner of the OS instance lock travel into the blocking closure and
/// are returned only after the mutation completes. If the awaiting future is
/// cancelled, the detached blocking job still owns both locks until it exits,
/// so neither a same-process cache operation nor a newly opened bridge
/// instance can interleave with it.
async fn run_locked_blocking<T, F>(
    guard: &mut Option<tokio::sync::OwnedMutexGuard<()>>,
    instance_lock: Arc<InstanceLockGuard>,
    context: &'static str,
    task: F,
) -> Result<T, AttachError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, AttachError> + Send + 'static,
{
    let owned = guard.take().ok_or(AttachError::Io {
        context: "holding the attachment cache mutation lock",
    })?;
    let (owned, instance_lock, result) =
        tokio::task::spawn_blocking(move || (owned, instance_lock, task()))
            .await
            .map_err(|_| AttachError::Io { context })?;
    *guard = Some(owned);
    drop(instance_lock);
    result
}

/// Writes `bytes` to a same-directory temp file, `fsync`s, and atomically
/// renames it to `<sha>`. All callers execute this helper on Tokio's blocking
/// pool so hashing, write, flush, `fsync`, metadata checks, and rename never
/// occupy an async runtime worker.
fn install_file(root: &Path, sha: &str, bytes: &[u8]) -> Result<(), AttachError> {
    let final_path = root.join(sha);
    for _ in 0..2 {
        let temp = write_temp(root, bytes)?;
        if final_path.exists() {
            if verify_existing(&final_path, sha, bytes.len()).is_ok() {
                return Ok(());
            }
            remove_direct_child(root, &final_path, "removing a corrupt cached file")?;
        }
        match std::fs::rename(&temp.path, &final_path) {
            Ok(()) => {
                // Make the rename durable before the row is committed: without
                // syncing the parent directory, a power loss could commit the
                // store row while losing the rename (B4).
                sync_parent_dir(root)?;
                return Ok(());
            }
            Err(_) if final_path.exists() => {
                if verify_existing(&final_path, sha, bytes.len()).is_ok() {
                    return Ok(());
                }
                remove_direct_child(root, &final_path, "removing a corrupt cached file")?;
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

/// Writes bytes to a freshly created temp file (0600 on Unix, so the content
/// file it is renamed into is owner-only) and `fsync`s it. The returned guard
/// removes the file on any error path.
fn write_temp(root: &Path, bytes: &[u8]) -> Result<TempFile, AttachError> {
    let path = root.join(format!("{ATTACHMENT_TEMP_PREFIX}{}", Uuid::new_v4()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&path).map_err(|_| AttachError::Io {
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

/// Deletion candidates produced by one read-only directory scan, bounded by
/// the per-pass deletion cap. The scan performs no deletion and no store I/O;
/// [`AttachmentCache::reconcile_locked`] applies each candidate under the
/// per-cache lock after re-verifying it against the *current* store rows, so a
/// scan that runs outside the async lock cannot race a concurrent `fetch`
/// (which installs under that same lock) into deleting an in-flight file.
#[derive(Default)]
struct ScanCandidates {
    /// Directory entries consumed this pass, including marker/lock entries.
    scanned_entries: u64,
    /// Orphan `.tmp-*` files (paths, whose names may be non-UTF-8).
    temp: Vec<PathBuf>,
    /// Orphan content files (valid SHA-256 names with no snapshot row).
    orphan: Vec<String>,
    /// Size-mismatched content files (valid SHA-256 names).
    corrupt: Vec<String>,
    /// Unrecognized regular files (paths; names may be non-UTF-8).
    unrecognized: Vec<PathBuf>,
    /// Directory entries that could not be inspected.
    errors: u64,
    /// Directories skipped (never recursed into).
    skipped_dirs: u64,
}

struct ScanBatch {
    candidates: ScanCandidates,
    next: Option<std::fs::ReadDir>,
}

struct AppliedCandidates {
    stats: ReconcileStats,
    drop_rows: HashSet<String>,
    /// Corrupt content files whose store row must be dropped before unlink.
    /// Keeping the delete on the safe side of that ordering means cancellation
    /// can leave only an orphan file, never a row/lease pointing at no file.
    delete_after_drop: HashSet<String>,
}

/// Advances a resumable directory iterator by at most `batch` entries. The
/// iterator is returned for the next call, or cleared once EOF is observed so
/// a subsequent reconciliation starts a fresh cycle.
fn scan_directory_batch(
    root: &Path,
    rows_by_sha: &HashMap<String, u64>,
    batch: usize,
    iterator: Option<std::fs::ReadDir>,
) -> Result<ScanBatch, AttachError> {
    let mut dir = match iterator {
        Some(dir) => dir,
        None => std::fs::read_dir(root).map_err(|_| AttachError::Io {
            context: "reading the cache directory",
        })?,
    };
    let mut candidates = ScanCandidates::default();
    let mut exhausted = false;

    for _ in 0..batch {
        let Some(entry) = dir.next() else {
            exhausted = true;
            break;
        };
        candidates.scanned_entries = candidates.scanned_entries.saturating_add(1);
        let Ok(entry) = entry else {
            candidates.errors = candidates.errors.saturating_add(1);
            continue;
        };
        let path = entry.path();
        let file_name = entry.file_name();
        let lossy = file_name.to_string_lossy();
        if lossy == ATTACHMENT_CACHE_MARKER || lossy == ATTACHMENT_INSTANCE_LOCK {
            continue;
        }
        let file_type = if let Ok(metadata) = std::fs::symlink_metadata(&path) {
            metadata.file_type()
        } else {
            candidates.errors = candidates.errors.saturating_add(1);
            continue;
        };
        if file_type.is_dir() {
            candidates.skipped_dirs = candidates.skipped_dirs.saturating_add(1);
            continue;
        }
        if !file_type.is_file() {
            // Symlinks and special files are never followed or deleted.
            continue;
        }
        let Some(name) = file_name.to_str() else {
            candidates.unrecognized.push(path);
            continue;
        };
        if name.starts_with(ATTACHMENT_TEMP_PREFIX) {
            candidates.temp.push(path);
            continue;
        }
        if is_valid_sha256_name(name) {
            if let Some(&expected) = rows_by_sha.get(name) {
                let actual = std::fs::metadata(&path).map_or(u64::MAX, |m| m.len());
                if actual != expected {
                    candidates.corrupt.push(name.to_owned());
                }
            } else {
                candidates.orphan.push(name.to_owned());
            }
            continue;
        }
        candidates.unrecognized.push(path);
    }
    Ok(ScanBatch {
        candidates,
        next: (!exhausted).then_some(dir),
    })
}

/// Re-verifies and applies one bounded candidate batch on a blocking worker.
/// The caller holds the in-process cache mutation lock, and the advisory file
/// lock excludes other bridge processes using the same root.
fn apply_scan_candidates(
    root: &Path,
    candidates: ScanCandidates,
    rows_by_sha: &HashMap<String, u64>,
    mut drop_rows: HashSet<String>,
    batch: usize,
) -> Result<AppliedCandidates, AttachError> {
    let mut stats = ReconcileStats {
        scanned_entries: candidates.scanned_entries,
        errors: candidates.errors,
        skipped_dirs: candidates.skipped_dirs,
        ..ReconcileStats::default()
    };
    let mut deletions = 0_usize;
    let mut delete_after_drop = HashSet::new();

    for path in candidates.temp {
        if deletions >= batch {
            break;
        }
        if remove_direct_child(root, &path, "removing an orphan temp file")? {
            stats.temp_files = stats.temp_files.saturating_add(1);
            deletions = deletions.saturating_add(1);
        }
    }
    for sha in candidates.orphan {
        if deletions >= batch {
            break;
        }
        if rows_by_sha.contains_key(&sha) {
            continue;
        }
        if remove_direct_child(root, &root.join(&sha), "removing an orphan cached file")? {
            stats.orphan_files = stats.orphan_files.saturating_add(1);
            deletions = deletions.saturating_add(1);
        }
    }
    for sha in candidates.corrupt {
        if deletions >= batch {
            break;
        }
        let Some(&expected) = rows_by_sha.get(&sha) else {
            continue;
        };
        let path = root.join(&sha);
        let actual = std::fs::metadata(&path).map_or(u64::MAX, |metadata| metadata.len());
        if actual == expected {
            continue;
        }
        delete_after_drop.insert(sha.clone());
        drop_rows.insert(sha);
        deletions = deletions.saturating_add(1);
    }
    for path in candidates.unrecognized {
        if deletions >= batch {
            break;
        }
        if remove_direct_child(root, &path, "removing an unrecognized cache file")? {
            stats.orphan_files = stats.orphan_files.saturating_add(1);
            deletions = deletions.saturating_add(1);
        }
    }

    for sha in rows_by_sha.keys() {
        if !drop_rows.contains(sha) && !regular_file_exists(&root.join(sha)) {
            drop_rows.insert(sha.clone());
        }
    }
    Ok(AppliedCandidates {
        stats,
        drop_rows,
        delete_after_drop,
    })
}

fn remove_direct_child(
    root: &Path,
    path: &Path,
    context: &'static str,
) -> Result<bool, AttachError> {
    if path.parent() != Some(root) {
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

/// RAII owner of the cross-instance advisory lock. Dropping the file releases
/// the kernel lock on both normal exit and unwinding; the stable lock path is
/// deliberately retained and never deleted by an owner-blind guard.
struct InstanceLockGuard {
    _file: std::fs::File,
}

/// Opens the stable lock file and acquires a non-blocking OS-level exclusive
/// advisory lock. Contention is a clean fail-closed second-instance error;
/// other open/permission/locking failures are I/O errors. No stale-time test
/// or unlink race is involved.
fn acquire_instance_lock(root: &Path) -> Result<InstanceLockGuard, AttachError> {
    let path = root.join(ATTACHMENT_INSTANCE_LOCK);
    validate_instance_lock_path(&path)?;
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(&path).map_err(|_| AttachError::Io {
        context: "opening the instance lock",
    })?;
    validate_opened_instance_lock(&path, &file)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|_| AttachError::Io {
                context: "tightening instance lock permissions",
            })?;
    }
    fs2::FileExt::try_lock_exclusive(&file).map_err(|error| {
        if error.kind() == fs2::lock_contended_error().kind() {
            AttachError::InvalidPath {
                context: "another bridge instance is using this cache",
            }
        } else {
            AttachError::Io {
                context: "locking the attachment cache",
            }
        }
    })?;
    Ok(InstanceLockGuard { _file: file })
}

/// Refuses an existing lock path unless it is a direct regular file with no
/// detectable hard-link alias. This check happens before opening so no chmod
/// or lock operation can touch an obvious path redirection.
fn validate_instance_lock_path(path: &Path) -> Result<(), AttachError> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => validate_lock_metadata(&metadata),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(AttachError::Io {
            context: "inspecting the instance lock path",
        }),
    }
}

/// Re-validates the path and opened handle before changing permissions. On
/// Unix, `O_NOFOLLOW` closes the symlink race and device/inode equality closes
/// replacement races between the pre-open check and `open`; a link count other
/// than one rejects a hard-link alias.
fn validate_opened_instance_lock(path: &Path, file: &std::fs::File) -> Result<(), AttachError> {
    let path_metadata = std::fs::symlink_metadata(path).map_err(|_| AttachError::Io {
        context: "re-checking the instance lock path",
    })?;
    validate_lock_metadata(&path_metadata)?;
    let file_metadata = file.metadata().map_err(|_| AttachError::Io {
        context: "inspecting the opened instance lock",
    })?;
    validate_lock_metadata(&file_metadata)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if path_metadata.dev() != file_metadata.dev() || path_metadata.ino() != file_metadata.ino()
        {
            return Err(AttachError::InvalidPath {
                context: "instance lock path changed while opening",
            });
        }
    }
    Ok(())
}

fn validate_lock_metadata(metadata: &std::fs::Metadata) -> Result<(), AttachError> {
    if !metadata.file_type().is_file() {
        return Err(AttachError::InvalidPath {
            context: "instance lock must be a regular file",
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(AttachError::InvalidPath {
                context: "instance lock must not have hard-link aliases",
            });
        }
    }
    Ok(())
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
/// Unix. Fail-closed: a directory that already passed marker validation but
/// cannot be tightened is refused rather than left world-readable with
/// plaintext cache content on disk. No-op on non-Unix platforms (which have
/// no portable owner-only mode primitive here).
fn tighten_permissions(root: &Path) -> Result<(), AttachError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o700)).map_err(|_| {
            AttachError::Io {
                context: "tightening cache directory permissions to 0700",
            }
        })
    }
    #[cfg(not(unix))]
    {
        let _ = root;
        Ok(())
    }
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    struct EmptyDownloader;

    impl ResourceDownloader for EmptyDownloader {
        fn download(
            &self,
            _message_id: &str,
            _key: &str,
            _kind: ResourceKind,
        ) -> BoxFuture<'static, Result<Bytes, AttachError>> {
            Box::pin(async { Ok(Bytes::new()) })
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_last_owner_retains_both_locks_until_blocking_mutation_finishes() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = StoreHandle::open_in_memory().await.expect("store");
        let cache = AttachmentCache::open(
            temp.path(),
            store.clone(),
            Arc::new(EmptyDownloader),
            AttachmentLimits::default(),
        )
        .expect("cache");
        let lock = Arc::clone(&cache.lock);
        let mut guard = Some(Arc::clone(&lock).lock_owned().await);
        let instance_lock = Arc::clone(&cache.instance_lock);
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();

        let task = tokio::spawn(async move {
            run_locked_blocking(
                &mut guard,
                instance_lock,
                "testing cancellation shielding",
                move || {
                    let _ = entered_tx.send(());
                    release_rx.recv().map_err(|_| AttachError::Io {
                        context: "waiting to release the blocking test mutation",
                    })?;
                    let _ = finished_tx.send(());
                    Ok(())
                },
            )
            .await
        });
        entered_rx.await.expect("blocking mutation entered");
        task.abort();
        let join = task.await.expect_err("outer waiter must be cancelled");
        assert!(join.is_cancelled());
        drop(cache);

        assert!(
            tokio::time::timeout(Duration::from_millis(50), Arc::clone(&lock).lock_owned())
                .await
                .is_err(),
            "the detached blocking mutation must retain the cache lock"
        );
        let second = AttachmentCache::open(
            temp.path(),
            store.clone(),
            Arc::new(EmptyDownloader),
            AttachmentLimits::default(),
        );
        assert!(
            matches!(second, Err(AttachError::InvalidPath { .. })),
            "the detached mutation must retain the OS instance lock after the last cache owner drops"
        );

        release_tx.send(()).expect("release blocking mutation");
        finished_rx.await.expect("blocking mutation finished");
        let reacquired_guard =
            tokio::time::timeout(Duration::from_secs(1), Arc::clone(&lock).lock_owned())
                .await
                .expect("lock released after blocking mutation");
        drop(reacquired_guard);

        let mut reopened = None;
        for _ in 0..100 {
            match AttachmentCache::open(
                temp.path(),
                store.clone(),
                Arc::new(EmptyDownloader),
                AttachmentLimits::default(),
            ) {
                Ok(cache) => {
                    reopened = Some(cache);
                    break;
                }
                Err(AttachError::InvalidPath { .. }) => tokio::task::yield_now().await,
                Err(error) => panic!("unexpected reopen error: {error:?}"),
            }
        }
        assert!(reopened.is_some(), "cache reopens after detached job exits");
        drop(reopened);
        let _ = store.shutdown().await;
    }

    /// The chmod failure path is hard to reach through `AttachmentCache::open`
    /// (the directory must first pass marker validation before the chmod, and
    /// a validated directory we own almost never fails `set_permissions`), so
    /// it is asserted at the unit boundary: `tighten_permissions` must return
    /// `Err` rather than swallow the failure. The success path (0700) is
    /// asserted in the integration test `open_tightens_permissions_only_after_marker_validation`.
    #[test]
    fn tighten_permissions_failure_is_fail_closed() {
        let missing = Path::new("/definitely/not/a/real/attachment-cache-dir");
        let result = tighten_permissions(missing);
        assert!(
            matches!(result, Err(AttachError::Io { .. })),
            "a chmod failure must be an error, not a warning: {result:?}"
        );
    }
}
