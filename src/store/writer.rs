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
use super::{FileReservation, StoreError, sqlite_error};
use crate::limits::{STORE_BUSY_TIMEOUT, STORE_WRITER_CAPACITY};

/// One request toward the writer task.
pub(crate) enum StoreRequest {
    /// A unit of work executed against the connection. The closure carries
    /// its own reply oneshot, so responses stay typed at the call site.
    Job(Box<dyn FnOnce(&mut Connection) + Send>),
    /// Graceful stop: the writer exits after every request queued ahead of
    /// this one has been processed.
    Shutdown,
}

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
        Ok(connection) => {
            let _ = init.send(Ok(()));
            connection
        }
        Err(error) => {
            let _ = init.send(Err(error));
            return;
        }
    };
    while let Some(request) = receiver.blocking_recv() {
        match request {
            StoreRequest::Job(job) => job(&mut connection),
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
    Ok(connection)
}

fn apply_pragmas(connection: &Connection) -> Result<(), StoreError> {
    connection
        .execute_batch(
            "PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON; PRAGMA synchronous = NORMAL;",
        )
        .map_err(|error| sqlite_error("applying store pragmas", &error))?;
    let busy_timeout_ms = i64::try_from(STORE_BUSY_TIMEOUT.as_millis()).unwrap_or(i64::MAX);
    connection
        .pragma_update(None, "busy_timeout", busy_timeout_ms)
        .map_err(|error| sqlite_error("applying store pragmas", &error))
}

fn migrate(connection: &mut Connection) -> Result<(), StoreError> {
    validate_migrations()?;
    let current: u32 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(|error| sqlite_error("reading the schema version", &error))?;
    let latest = MIGRATIONS.last().map_or(0, |migration| migration.version);
    if current > latest {
        return Err(StoreError::Migration {
            version: current,
            name: "database schema is newer than this binary",
        });
    }
    for migration in MIGRATIONS
        .iter()
        .filter(|migration| migration.version > current)
    {
        let apply = |connection: &mut Connection| -> Result<(), StoreError> {
            let transaction = connection
                .transaction()
                .map_err(|error| sqlite_error("starting a migration transaction", &error))?;
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

fn validate_migrations() -> Result<(), StoreError> {
    for (index, migration) in MIGRATIONS.iter().enumerate() {
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
}
