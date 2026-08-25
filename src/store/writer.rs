#![allow(clippy::doc_markdown)]

//! The single blocking writer task that owns the SQLite connection.
//!
//! Exactly one `std::thread` opens the database, applies the pragmas, runs
//! pending migrations, and then serves every store request (reads included)
//! sequentially from one bounded channel, so every transaction has exactly
//! one author and no second read-write connection ever exists (design §8).

use std::path::PathBuf;
use std::thread::JoinHandle;

use rusqlite::Connection;
use tokio::sync::{mpsc, oneshot};

use super::schema::MIGRATIONS;
use super::{FileReservation, StoreError, sqlite_error, tighten_database_sidecars};
use crate::limits::{STORE_ATTACHMENT_LEASE_MAX_ROWS, STORE_BUSY_TIMEOUT, STORE_WRITER_CAPACITY};

/// One request toward the writer task.
pub(crate) enum StoreRequest {
    /// A unit of work executed against the connection. The closure carries
    /// a delayed completion so sidecar validation can precede the reply.
    Job {
        /// Executes the typed operation and returns a validation-gated reply.
        execute: Box<dyn FnOnce(&mut Connection) -> StoreCompletion + Send>,
        /// Replies with a pre-execution validation error without running it.
        reject: Box<dyn FnOnce(StoreError) + Send>,
    },
    /// Graceful stop: the writer exits after every request queued ahead of
    /// this one has been processed.
    Shutdown,
}

/// Completes one typed request only after post-job validation has run.
pub(crate) type StoreCompletion = Box<dyn FnOnce(Result<(), StoreError>) + Send>;

/// Handle pieces returned by [`spawn`]: the command channel and the writer
/// thread's join handle.
pub(crate) struct WriterParts {
    /// Bounded command channel to the writer task.
    pub sender: mpsc::Sender<StoreRequest>,
    /// Join handle of the writer thread.
    pub join: JoinHandle<()>,
}

/// Location of the database the writer opens.
pub(crate) enum StoreLocation {
    /// File-backed database at this path (parent directories are created).
    File(PathBuf),
    /// Private in-memory database (tests).
    InMemory,
}

/// Spawns the writer thread and waits for it to finish opening the database,
/// applying pragmas, and running migrations.
///
/// # Errors
///
/// Returns an error when the database cannot be opened or a migration fails.
#[allow(clippy::needless_pass_by_value)]
pub(crate) async fn spawn(
    location: StoreLocation,
    reservation: Option<FileReservation>,
) -> Result<WriterParts, StoreError> {
    let (sender, receiver) = mpsc::channel(STORE_WRITER_CAPACITY);
    let (init, initialized) = oneshot::channel();
    let join = std::thread::spawn(move || writer_main(location, receiver, init, reservation));
    initialized.await.map_err(|_| StoreError::Closed)??;
    Ok(WriterParts { sender, join })
}

#[allow(clippy::needless_pass_by_value)]
fn writer_main(
    location: StoreLocation,
    mut receiver: mpsc::Receiver<StoreRequest>,
    init: oneshot::Sender<Result<(), StoreError>>,
    _reservation: Option<FileReservation>,
) {
    let mut connection = match open_and_migrate(&location) {
        Ok(connection) => connection,
        Err(error) => {
            let _ = init.send(Err(error));
            return;
        }
    };
    let _ = init.send(Ok(()));
    while let Some(request) = receiver.blocking_recv() {
        match request {
            StoreRequest::Job { execute, reject } => {
                let prevalidation = match &location {
                    StoreLocation::File(path) => tighten_database_sidecars(path),
                    StoreLocation::InMemory => Ok(()),
                };
                if let Err(error) = prevalidation {
                    reject(error);
                    break;
                }
                let completion = execute(&mut connection);
                let validation = match &location {
                    StoreLocation::File(path) => tighten_database_sidecars(path),
                    StoreLocation::InMemory => Ok(()),
                };
                let failed = validation.is_err();
                completion(validation);
                if failed {
                    break;
                }
            }
            StoreRequest::Shutdown => break,
        }
    }
}

fn open_and_migrate(location: &StoreLocation) -> Result<Connection, StoreError> {
    let mut connection = match location {
        StoreLocation::File(path) => {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent).map_err(|_| StoreError::Io {
                        context: "creating the database directory",
                    })?;
                }
            }
            Connection::open(path).map_err(|error| sqlite_error("opening the database", &error))?
        }
        StoreLocation::InMemory => Connection::open_in_memory()
            .map_err(|error| sqlite_error("opening the in-memory database", &error))?,
    };
    apply_pragmas(&connection)?;
    migrate(&mut connection)?;
    if let StoreLocation::File(path) = location {
        tighten_database_sidecars(path)?;
    }
    Ok(connection)
}

fn apply_pragmas(connection: &Connection) -> Result<(), StoreError> {
    connection
        .execute_batch(
            "PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON; PRAGMA synchronous = NORMAL;
             PRAGMA secure_delete = ON;",
        )
        .map_err(|error| sqlite_error("applying store pragmas", &error))?;
    let busy_timeout_ms = i64::try_from(STORE_BUSY_TIMEOUT.as_millis()).unwrap_or(i64::MAX);
    connection
        .pragma_update(None, "busy_timeout", busy_timeout_ms)
        .map_err(|error| sqlite_error("applying store pragmas", &error))
}

fn migrate(connection: &mut Connection) -> Result<(), StoreError> {
    migrate_through(connection, MIGRATIONS)
}

fn migrate_through(
    connection: &mut Connection,
    migrations: &[super::schema::Migration],
) -> Result<(), StoreError> {
    validate_migrations(migrations)?;
    let current: u32 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(|error| sqlite_error("reading the schema version", &error))?;
    let latest = migrations.last().map_or(0, |migration| migration.version);
    if current > latest {
        return Err(StoreError::Migration {
            version: current,
            name: "database schema is newer than this binary",
        });
    }
    for migration in migrations
        .iter()
        .filter(|migration| migration.version > current)
    {
        if migration.name == "remove durable media capabilities and transcripts" {
            super::dedup::scrub_persisted_inbound_secrets(connection).map_err(
                |error| match error {
                    StoreError::Sqlite { .. } => StoreError::Migration {
                        version: migration.version,
                        name: migration.name,
                    },
                    other => other,
                },
            )?;
            // Updating a row is insufficient: old bytes can remain in free
            // pages or WAL frames. Compact before advancing `user_version`;
            // a failure therefore retries this cleanup on the next open.
            connection
                .execute_batch(
                    "PRAGMA wal_checkpoint(TRUNCATE);
                     VACUUM;
                     PRAGMA wal_checkpoint(TRUNCATE);",
                )
                .map_err(|_| StoreError::Migration {
                    version: migration.version,
                    name: migration.name,
                })?;
        }
        let apply = |connection: &mut Connection| -> Result<(), StoreError> {
            let transaction = connection
                .transaction()
                .map_err(|error| sqlite_error("starting a migration transaction", &error))?;
            prepare_migration(&transaction, migration.name)?;
            transaction
                .execute_batch(migration.sql)
                .map_err(|error| sqlite_error("applying a migration", &error))?;
            // `user_version` is a database header value; setting it inside
            // the transaction commits atomically with the DDL.
            transaction
                .pragma_update(None, "user_version", migration.version)
                .map_err(|error| sqlite_error("recording the schema version", &error))?;
            transaction
                .commit()
                .map_err(|error| sqlite_error("committing a migration", &error))
        };
        apply(connection).map_err(|error| match error {
            StoreError::Sqlite { .. } => StoreError::Migration {
                version: migration.version,
                name: migration.name,
            },
            other => other,
        })?;
    }
    Ok(())
}

fn prepare_migration(
    transaction: &rusqlite::Transaction<'_>,
    migration_name: &str,
) -> Result<(), StoreError> {
    if migration_name != "tokenize attachment lease acquisitions" {
        return Ok(());
    }
    transaction
        .execute(
            "DELETE FROM attachment_leases WHERE turn_row_id IN (
                 SELECT id FROM turns
                 WHERE state IN ('completed', 'failed', 'interrupted')
                    OR (state = 'uncertain' AND uncertain = 0)
             )",
            [],
        )
        .map_err(|error| sqlite_error("cleaning stale leases before migration", &error))?;
    let lease_count: i64 = transaction
        .query_row("SELECT COUNT(*) FROM attachment_leases", [], |row| {
            row.get(0)
        })
        .map_err(|error| sqlite_error("checking lease migration capacity", &error))?;
    if u64::try_from(lease_count).unwrap_or(u64::MAX) > STORE_ATTACHMENT_LEASE_MAX_ROWS {
        return Err(StoreError::CapacityExceeded {
            context: "migrating attachment leases",
        });
    }
    Ok(())
}

fn validate_migrations(migrations: &[super::schema::Migration]) -> Result<(), StoreError> {
    for (index, migration) in migrations.iter().enumerate() {
        let expected = u32::try_from(index + 1).unwrap_or(u32::MAX);
        if migration.version != expected {
            return Err(StoreError::Migration {
                version: migration.version,
                name: "migration versions must be contiguous",
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn full_channel_maps_to_queue_full() {
        let (sender, _receiver) = mpsc::channel::<StoreRequest>(1);
        sender
            .try_send(StoreRequest::Shutdown)
            .expect("first request fits");
        let error = sender
            .try_send(StoreRequest::Shutdown)
            .expect_err("bounded channel rejects overflow");
        assert!(matches!(error, mpsc::error::TrySendError::Full(_)));
    }

    #[test]
    fn file_upgrade_sets_v2_outbox_fence_and_v1_binary_refuses_downgrade() {
        let temp = tempdir().expect("tempdir");
        let path = temp.path().join("upgrade.sqlite");
        {
            let mut connection = Connection::open(&path).expect("open legacy file");
            apply_pragmas(&connection).expect("legacy pragmas");
            migrate_through(&mut connection, &MIGRATIONS[..5]).expect("seed schema v5");
            connection
                .execute(
                    "INSERT INTO outbox
                     (idempotency_key, scope_key, kind, payload_json, payload_bytes,
                      state, attempts, next_retry_ms, created_ms, updated_ms)
                     VALUES ('legacy', 'im:scope', 'final', ?1, length(?1),
                             'pending', 0, 0, 1, 1)",
                    [r#"{"version":1,"op":"reply_text","message_id":"om_old","text":"**literal**"}"#],
                )
                .expect("seed legacy payload");
        }

        {
            let mut connection = Connection::open(&path).expect("reopen for upgrade");
            migrate(&mut connection).expect("upgrade to schema v9");
            let version: u32 = connection
                .pragma_query_value(None, "user_version", |row| row.get(0))
                .expect("read upgraded version");
            assert_eq!(version, 9);
            let count: u32 = connection
                .query_row(
                    "SELECT COUNT(*) FROM outbox WHERE idempotency_key = 'legacy'",
                    [],
                    |row| row.get(0),
                )
                .expect("legacy row survives upgrade");
            assert_eq!(count, 1);
        }

        let mut legacy = Connection::open(&path).expect("legacy reopen");
        assert!(matches!(
            migrate_through(&mut legacy, &MIGRATIONS[..5]),
            Err(StoreError::Migration { version: 9, .. })
        ));
        let version: u32 = legacy
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("downgrade fence stays intact");
        assert_eq!(version, 9);
    }
}
