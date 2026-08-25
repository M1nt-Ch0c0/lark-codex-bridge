#![allow(clippy::doc_markdown)]

//! Durable SQLite store behind a single-writer task.
//!
//! The store owns one `rusqlite::Connection` on a dedicated blocking thread
//! (`writer`); every query — reads included — travels one bounded command
//! channel and is answered by oneshot, so there is a single code path and
//! exactly one author for every transaction (plan decision 5, design §8).
//! The database runs in WAL mode with `foreign_keys = ON`,
//! `synchronous = NORMAL`, and a bounded `busy_timeout`; schema changes are
//! `user_version` migrations ([`schema`]).
//!
//! Typed query groups live in `dedup` (inbound event registration and
//! state machine), `sessions` (scopes/threads/turns), `outbox` (durable
//! outbound queue), and `attachments` (content-addressed cache rows and
//! leases).
//!
//! Redaction: errors and `Debug` output carry static contexts, states, IDs,
//! and sizes only — never message text, payload bodies, or secrets.

mod attachments;
mod dedup;
mod outbox;
pub mod schema;
mod sessions;
mod writer;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::Connection;
use tokio::sync::{Semaphore, mpsc, oneshot};

use crate::limits::{STORE_REQUEST_MAX_BYTES, STORE_WRITER_BYTE_BUDGET};

pub use attachments::{AttachmentLeaseRow, AttachmentRow};
pub use dedup::{
    BeginTurnOutcome, ClaimedInbound, DedupOutcome, InboundDisposition, InboundEventState,
    InboundKey, InboundRejectionKind, InboundTerminal, ResolveTurnOutcome, SkippedInbound,
    TurnResolution,
};
pub use outbox::{NewOutboxRow, OutboxDepth, OutboxEnqueue, OutboxRow, OutboxState};
pub use sessions::{NewTurnRow, ScopeRow, ThreadRow, ThreadStatus, TurnRow, TurnState};
use writer::StoreRequest;

/// Store failures with classified, content-free contexts.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    /// Local filesystem failure (directory creation, database file).
    #[error("store I/O failure while {context}")]
    Io {
        /// Static description of the operation that failed.
        context: &'static str,
    },
    /// SQLite rejected an operation. Server engine messages are discarded;
    /// only the extended result code is kept.
    #[error("SQLite failure while {context} (code {code:?})")]
    Sqlite {
        /// Static description of the operation that failed.
        context: &'static str,
        /// SQLite extended result code, when available.
        code: Option<i64>,
    },
    /// The bounded writer channel is full; the caller must back off.
    #[error("store writer queue is full")]
    QueueFull,
    /// The writer task has stopped (shutdown completed or it panicked).
    #[error("store is closed")]
    Closed,
    /// Another live writer already owns this file-backed database in-process.
    #[error("store is already open in this process")]
    AlreadyOpen,
    /// A `user_version` migration failed to apply.
    #[error("store migration {version} ({name}) failed")]
    Migration {
        /// Failing migration version.
        version: u32,
        /// Failing migration name.
        name: &'static str,
    },
    /// The addressed row does not exist.
    #[error("store row not found while {context}")]
    NotFound {
        /// Static description of the lookup.
        context: &'static str,
    },
    /// A row state machine rejected the requested transition.
    #[error("illegal state transition while {context}")]
    InvalidTransition {
        /// Static description of the transition.
        context: &'static str,
    },
    /// A payload exceeded its byte budget before enqueue.
    #[error("store payload exceeds the {limit}-byte limit while {context}")]
    PayloadTooLarge {
        /// Static description of the payload.
        context: &'static str,
        /// The configured byte limit.
        limit: u64,
    },
    /// A durable collection reached its count or byte limit.
    #[error("store capacity is exhausted while {context}")]
    CapacityExceeded {
        /// Static description of the bounded collection.
        context: &'static str,
    },
    /// Persisted data violates a closed schema or cross-row invariant.
    #[error("store data is corrupt while {context}")]
    CorruptData {
        /// Static description of the failed integrity check.
        context: &'static str,
    },
    /// A path cannot be represented losslessly by this schema.
    #[error("store path is invalid while {context}")]
    InvalidPath {
        /// Static description of the rejected path operation.
        context: &'static str,
    },
}

/// Effective pragma state of the store connection, for diagnostics (`doctor`)
/// and tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorePragmas {
    /// `PRAGMA journal_mode` (`wal` for file-backed stores, `memory` for
    /// in-memory ones).
    pub journal_mode: String,
    /// `PRAGMA foreign_keys`.
    pub foreign_keys: bool,
    /// `PRAGMA busy_timeout` in milliseconds.
    pub busy_timeout_ms: i64,
    /// `PRAGMA synchronous` (`1` = NORMAL).
    pub synchronous: i64,
    /// Current schema version (`PRAGMA user_version`).
    pub user_version: u32,
}

/// Async handle to the single-writer store. Cheap to clone; every clone
/// shares the same bounded channel and writer task.
#[derive(Clone)]
pub struct StoreHandle {
    sender: mpsc::Sender<StoreRequest>,
    join: Arc<Mutex<Option<JoinHandle<()>>>>,
    byte_budget: Arc<Semaphore>,
}

impl StoreHandle {
    /// Opens a file-backed store (creating parent directories), applies
    /// pragmas, and runs pending migrations before serving requests.
    ///
    /// # Errors
    ///
    /// Returns an error when the database cannot be opened or migrated.
    pub async fn open(path: &Path) -> Result<Self, StoreError> {
        let (database_path, reservation) = prepare_and_reserve_file_store(path)?;
        Ok(Self::from_parts(
            writer::spawn(
                writer::StoreLocation::File(database_path),
                Some(reservation),
            )
            .await?,
        ))
    }

    /// Opens a private in-memory store (tests).
    ///
    /// # Errors
    ///
    /// Returns an error when the database cannot be initialized.
    pub async fn open_in_memory() -> Result<Self, StoreError> {
        Ok(Self::from_parts(
            writer::spawn(writer::StoreLocation::InMemory, None).await?,
        ))
    }

    fn from_parts(parts: writer::WriterParts) -> Self {
        Self {
            sender: parts.sender,
            join: Arc::new(Mutex::new(Some(parts.join))),
            byte_budget: Arc::new(Semaphore::new(STORE_WRITER_BYTE_BUDGET)),
        }
    }

    /// Stops the writer after every queued request has been processed and
    /// waits for the thread to exit.
    ///
    /// Requests sent by other clones after this point fail with
    /// [`StoreError::Closed`].
    ///
    /// # Errors
    ///
    /// Returns an error when the writer thread panicked.
    pub async fn shutdown(self) -> Result<(), StoreError> {
        let _ = self.sender.send(StoreRequest::Shutdown).await;
        let join = self.join.lock().ok().and_then(|mut guard| guard.take());
        if let Some(join) = join {
            tokio::task::spawn_blocking(move || join.join())
                .await
                .map_err(|_| StoreError::Closed)?
                .map_err(|_| StoreError::Closed)?;
        }
        Ok(())
    }

    /// Runs one unit of work on the writer thread.
    ///
    /// Oversized payloads must be rejected by the typed wrappers *before*
    /// this point; here only the channel bound applies.
    pub(crate) async fn run<T, F>(&self, job: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, StoreError> + Send + 'static,
    {
        self.run_sized(0, job).await
    }

    /// Runs a writer request after reserving its captured input bytes until
    /// the writer dequeues it. Typed APIs calculate this before enqueueing.
    pub(crate) async fn run_sized<T, F>(
        &self,
        request_bytes: usize,
        job: F,
    ) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, StoreError> + Send + 'static,
    {
        if request_bytes > STORE_REQUEST_MAX_BYTES || request_bytes > STORE_WRITER_BYTE_BUDGET {
            return Err(StoreError::PayloadTooLarge {
                context: "queueing a store writer request",
                limit: u64::try_from(STORE_REQUEST_MAX_BYTES).unwrap_or(u64::MAX),
            });
        }
        let permits = u32::try_from(request_bytes).unwrap_or(u32::MAX);
        let permit = self
            .byte_budget
            .clone()
            .try_acquire_many_owned(permits)
            .map_err(|_| StoreError::QueueFull)?;
        let (respond, wait) = oneshot::channel();
        let respond = Arc::new(Mutex::new(Some(respond)));
        let execute_respond = Arc::clone(&respond);
        let execute = Box::new(move |connection: &mut Connection| {
            let _permit = permit;
            let result = job(connection);
            Box::new(move |validation: Result<(), StoreError>| {
                let final_result = match validation {
                    Ok(()) => result,
                    Err(error) => Err(error),
                };
                if let Some(respond) = execute_respond
                    .lock()
                    .ok()
                    .and_then(|mut respond| respond.take())
                {
                    let _ = respond.send(final_result);
                }
            }) as writer::StoreCompletion
        });
        let reject = Box::new(move |error: StoreError| {
            if let Some(respond) = respond.lock().ok().and_then(|mut respond| respond.take()) {
                let _ = respond.send(Err(error));
            }
        });
        self.sender
            .try_send(StoreRequest::Job { execute, reject })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => StoreError::QueueFull,
                mpsc::error::TrySendError::Closed(_) => StoreError::Closed,
            })?;
        wait.await.map_err(|_| StoreError::Closed)?
    }

    /// Reads the effective pragma state of the store connection.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task is unavailable.
    pub async fn pragmas(&self) -> Result<StorePragmas, StoreError> {
        self.run(|connection| {
            let journal_mode: String = connection
                .pragma_query_value(None, "journal_mode", |row| row.get(0))
                .map_err(|error| sqlite_error("reading store pragmas", &error))?;
            let foreign_keys: i64 = connection
                .pragma_query_value(None, "foreign_keys", |row| row.get(0))
                .map_err(|error| sqlite_error("reading store pragmas", &error))?;
            let busy_timeout_ms: i64 = connection
                .pragma_query_value(None, "busy_timeout", |row| row.get(0))
                .map_err(|error| sqlite_error("reading store pragmas", &error))?;
            let synchronous: i64 = connection
                .pragma_query_value(None, "synchronous", |row| row.get(0))
                .map_err(|error| sqlite_error("reading store pragmas", &error))?;
            let user_version: u32 = connection
                .pragma_query_value(None, "user_version", |row| row.get(0))
                .map_err(|error| sqlite_error("reading store pragmas", &error))?;
            Ok(StorePragmas {
                journal_mode,
                foreign_keys: foreign_keys != 0,
                busy_timeout_ms,
                synchronous,
                user_version,
            })
        })
        .await
    }
}

fn prepare_and_reserve_file_store(path: &Path) -> Result<(PathBuf, FileReservation), StoreError> {
    let database_path = prepare_database_file(path)?;
    let reservation = FileReservation::reserve(&database_path)?;
    Ok((database_path, reservation))
}

static LIVE_FILE_STORES: OnceLock<Mutex<HashSet<FileIdentity>>> = OnceLock::new();

#[derive(Clone, Hash, PartialEq, Eq)]
enum FileIdentity {
    #[cfg(unix)]
    Unix { device: u64, inode: u64 },
    #[cfg(not(unix))]
    Canonical(PathBuf),
}

/// Process-local reservation held by the writer thread for its full lifetime.
pub(crate) struct FileReservation {
    key: FileIdentity,
}

impl FileReservation {
    fn reserve(path: &Path) -> Result<Self, StoreError> {
        let key = file_identity(path)?;
        let stores = LIVE_FILE_STORES.get_or_init(|| Mutex::new(HashSet::new()));
        let mut stores = stores.lock().map_err(|_| StoreError::Closed)?;
        if !stores.insert(key.clone()) {
            return Err(StoreError::AlreadyOpen);
        }
        Ok(Self { key })
    }
}

impl Drop for FileReservation {
    fn drop(&mut self) {
        if let Some(stores) = LIVE_FILE_STORES.get() {
            if let Ok(mut stores) = stores.lock() {
                stores.remove(&self.key);
            }
        }
    }
}

fn file_identity(path: &Path) -> Result<FileIdentity, StoreError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|_| StoreError::Io {
                context: "resolving the database path",
            })?
            .join(path)
    };
    let canonical = std::fs::canonicalize(&absolute).map_err(|_| StoreError::Io {
        context: "resolving the database path",
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        let metadata = std::fs::metadata(canonical).map_err(|_| StoreError::Io {
            context: "resolving the database path",
        })?;
        Ok(FileIdentity::Unix {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
    #[cfg(not(unix))]
    {
        Ok(FileIdentity::Canonical(canonical))
    }
}

fn prepare_database_file(path: &Path) -> Result<PathBuf, StoreError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|_| StoreError::Io {
                context: "resolving the database path",
            })?
            .join(path)
    };
    let parent = absolute.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent).map_err(|_| StoreError::Io {
        context: "creating the database directory",
    })?;
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(&absolute).map_err(|_| StoreError::Io {
        context: "creating the database file",
    })?;
    let metadata = file.metadata().map_err(|_| StoreError::Io {
        context: "validating the database file",
    })?;
    if !metadata.is_file() {
        return Err(StoreError::InvalidPath {
            context: "opening a non-regular database file",
        });
    }
    tighten_open_file(&file, "tightening the database file")?;
    std::fs::canonicalize(absolute).map_err(|_| StoreError::Io {
        context: "resolving the database path",
    })
}

#[cfg(unix)]
fn tighten_open_file(file: &std::fs::File, context: &'static str) -> Result<(), StoreError> {
    use std::os::unix::fs::PermissionsExt;

    file.set_permissions(std::fs::Permissions::from_mode(0o600))
        .map_err(|_| StoreError::Io { context })
}

#[cfg(not(unix))]
fn tighten_open_file(_file: &std::fs::File, _context: &'static str) -> Result<(), StoreError> {
    Ok(())
}

pub(crate) fn tighten_database_sidecars(path: &Path) -> Result<(), StoreError> {
    for suffix in ["-wal", "-shm"] {
        let mut sidecar = path.as_os_str().to_os_string();
        sidecar.push(suffix);
        let sidecar = PathBuf::from(sidecar);
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true);
        let file = match options.open(&sidecar) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => {
                return Err(StoreError::Io {
                    context: "opening a database sidecar",
                });
            }
        };
        let metadata = file.metadata().map_err(|_| StoreError::Io {
            context: "validating a database sidecar",
        })?;
        if !metadata.is_file() {
            return Err(StoreError::InvalidPath {
                context: "opening a non-regular database sidecar",
            });
        }
        tighten_open_file(&file, "tightening a database sidecar")?;
    }
    Ok(())
}

/// Adds captured strings' UTF-8 byte lengths without overflowing.
pub(crate) fn request_bytes(values: &[&str]) -> usize {
    values
        .iter()
        .fold(0_usize, |total, value| total.saturating_add(value.len()))
}

/// Current wall-clock time in milliseconds since the Unix epoch.
pub(crate) fn now_ms() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis());
    i64::try_from(millis).unwrap_or(i64::MAX)
}

/// Maps a `rusqlite` failure to a classified, message-free [`StoreError`].
pub(crate) fn sqlite_error(context: &'static str, error: &rusqlite::Error) -> StoreError {
    let code = match error {
        rusqlite::Error::SqliteFailure(inner, _) => Some(i64::from(inner.extended_code)),
        _ => None,
    };
    StoreError::Sqlite { context, code }
}

/// Reads one row with the given query, returning `None` on `QueryReturnedNoRows`.
pub(crate) fn query_optional<T>(
    result: Result<T, rusqlite::Error>,
    context: &'static str,
) -> Result<Option<T>, StoreError> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(error) => Err(sqlite_error(context, &error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn file_preparation_reserves_actual_identity_before_writer_spawn() {
        let temp = tempdir().expect("tempdir");
        let target = temp.path().join("target.sqlite");
        assert!(!target.exists());

        let (database_path, reservation) =
            prepare_and_reserve_file_store(&target).expect("prepare target");
        assert_eq!(
            database_path,
            std::fs::canonicalize(&target).expect("canonical target")
        );
        assert_eq!(
            std::fs::metadata(&target).expect("target metadata").len(),
            0
        );
        assert!(matches!(
            prepare_and_reserve_file_store(&target),
            Err(StoreError::AlreadyOpen)
        ));
        assert_eq!(
            std::fs::metadata(&target).expect("target metadata").len(),
            0
        );

        drop(reservation);
    }

    #[cfg(unix)]
    #[test]
    fn dangling_symlink_alias_is_rejected_before_writer_spawn() {
        let temp = tempdir().expect("tempdir");
        let target = temp.path().join("target.sqlite");
        let alias = temp.path().join("alias.sqlite");
        std::os::unix::fs::symlink(&target, &alias).expect("dangling symlink");
        assert!(!target.exists());

        let (_database_path, reservation) =
            prepare_and_reserve_file_store(&target).expect("prepare target");
        assert_eq!(
            std::fs::metadata(&target).expect("target metadata").len(),
            0
        );
        assert!(matches!(
            prepare_and_reserve_file_store(&alias),
            Err(StoreError::AlreadyOpen)
        ));
        assert_eq!(
            std::fs::metadata(&target).expect("target metadata").len(),
            0
        );

        drop(reservation);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn post_job_sidecar_validation_precedes_the_typed_response() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("post-job.sqlite");
        let store = StoreHandle::open(&path).await.expect("open");
        let mut shm = path.as_os_str().to_os_string();
        shm.push("-shm");
        let shm = PathBuf::from(shm);
        assert!(shm.exists(), "SQLite created shm");
        let sabotage = shm.clone();
        let result = store
            .run(move |_connection| {
                std::fs::remove_file(&sabotage).map_err(|_| StoreError::Io {
                    context: "removing the sidecar in the validation test",
                })?;
                std::fs::create_dir(&sabotage).map_err(|_| StoreError::Io {
                    context: "replacing the sidecar in the validation test",
                })?;
                Ok(())
            })
            .await;
        assert!(
            matches!(
                result,
                Err(StoreError::InvalidPath { .. } | StoreError::Io { .. })
            ),
            "post-job validation error must replace typed success: {result:?}"
        );
        let _ = store.shutdown().await;
    }
}
