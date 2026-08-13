#![allow(clippy::doc_markdown)]

//! Content-addressed attachment rows and per-turn leases (design §10).
//!
//! The cache itself lives on disk (Task 7); these rows track hashes, sizes,
//! and which turn still uses each object so GC never deletes a leased
//! attachment. Leases cascade when their attachment row is deleted.

use rusqlite::params;

use super::{StoreError, StoreHandle, now_ms, query_optional, request_bytes, sqlite_error};
use crate::limits::{STORE_ATTACHMENT_MAX_BYTES, STORE_ATTACHMENT_MAX_ROWS};

/// One row of the `attachments` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentRow {
    /// Content hash (hex SHA-256).
    pub sha256: String,
    /// Object size in bytes.
    pub bytes: u64,
    /// Resource kind (`image`/`file`).
    pub kind: String,
    /// Creation time, milliseconds since the Unix epoch.
    pub created_ms: i64,
    /// Last use, milliseconds since the Unix epoch.
    pub last_used_ms: i64,
}

/// One row of the `attachment_leases` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentLeaseRow {
    /// Content hash of the leased attachment.
    pub sha256: String,
    /// Turn row holding the lease.
    pub turn_row_id: i64,
    /// Lease creation time, milliseconds since the Unix epoch.
    pub created_ms: i64,
}

impl StoreHandle {
    /// Inserts or refreshes an attachment row (`last_used_ms` bumps on
    /// conflict so concurrent same-hash turns share one record).
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or SQLite fails.
    pub async fn put_attachment(
        &self,
        sha256: &str,
        bytes: u64,
        kind: &str,
    ) -> Result<(), StoreError> {
        let sha256 = sha256.to_owned();
        let kind = kind.to_owned();
        let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
        let request_size = request_bytes(&[&sha256, &kind]);
        self.run_sized(request_size, move |connection| {
            let exists: bool = connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM attachments WHERE sha256 = ?1)",
                    params![sha256],
                    |row| row.get(0),
                )
                .map_err(|error| sqlite_error("checking an attachment", &error))?;
            if !exists {
                let (count, stored_bytes): (i64, i64) = connection
                    .query_row(
                        "SELECT COUNT(*), COALESCE(SUM(bytes), 0) FROM attachments",
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(|error| sqlite_error("checking attachment capacity", &error))?;
                if u64::try_from(count).unwrap_or(u64::MAX) >= STORE_ATTACHMENT_MAX_ROWS
                    || u64::try_from(stored_bytes)
                        .unwrap_or(u64::MAX)
                        .saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX))
                        > STORE_ATTACHMENT_MAX_BYTES
                {
                    return Err(StoreError::CapacityExceeded {
                        context: "recording an attachment",
                    });
                }
            }
            connection
                .execute(
                    "INSERT INTO attachments (sha256, bytes, kind, created_ms, last_used_ms)
                     VALUES (?1, ?2, ?3, ?4, ?4)
                     ON CONFLICT (sha256) DO UPDATE SET last_used_ms = excluded.last_used_ms",
                    params![sha256, bytes, kind, now_ms()],
                )
                .map_err(|error| sqlite_error("recording an attachment", &error))?;
            Ok(())
        })
        .await
    }

    /// Reads one attachment row by hash.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or SQLite fails.
    pub async fn attachment_row(&self, sha256: &str) -> Result<Option<AttachmentRow>, StoreError> {
        let sha256 = sha256.to_owned();
        let request_size = request_bytes(&[&sha256]);
        self.run_sized(request_size, move |connection| {
            let row = connection.query_row(
                "SELECT sha256, bytes, kind, created_ms, last_used_ms
                 FROM attachments WHERE sha256 = ?1",
                params![sha256],
                |row| {
                    Ok(AttachmentRow {
                        sha256: row.get(0)?,
                        bytes: u64::try_from(row.get::<_, i64>(1)?).unwrap_or(0),
                        kind: row.get(2)?,
                        created_ms: row.get(3)?,
                        last_used_ms: row.get(4)?,
                    })
                },
            );
            query_optional(row, "reading an attachment row")
        })
        .await
    }

    /// Leases an attachment for one turn (idempotent per pair). Both foreign
    /// keys require the attachment and the turn row to exist.
    ///
    /// # Errors
    ///
    /// Returns an error when the attachment does not exist or SQLite fails.
    pub async fn add_attachment_lease(
        &self,
        sha256: &str,
        turn_row_id: i64,
    ) -> Result<(), StoreError> {
        let sha256 = sha256.to_owned();
        let request_size = request_bytes(&[&sha256]);
        self.run_sized(request_size, move |connection| {
            connection
                .execute(
                    "INSERT OR IGNORE INTO attachment_leases (sha256, turn_row_id, created_ms)
                     VALUES (?1, ?2, ?3)",
                    params![sha256, turn_row_id, now_ms()],
                )
                .map_err(|error| sqlite_error("adding an attachment lease", &error))?;
            Ok(())
        })
        .await
    }

    /// Lists all leases of one attachment.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or SQLite fails.
    pub async fn attachment_leases(
        &self,
        sha256: &str,
    ) -> Result<Vec<AttachmentLeaseRow>, StoreError> {
        let sha256 = sha256.to_owned();
        let request_size = request_bytes(&[&sha256]);
        self.run_sized(request_size, move |connection| {
            let mut statement = connection
                .prepare(
                    "SELECT sha256, turn_row_id, created_ms
                     FROM attachment_leases WHERE sha256 = ?1 ORDER BY turn_row_id",
                )
                .map_err(|error| sqlite_error("listing attachment leases", &error))?;
            let rows = statement
                .query_map(params![sha256], |row| {
                    Ok(AttachmentLeaseRow {
                        sha256: row.get(0)?,
                        turn_row_id: row.get(1)?,
                        created_ms: row.get(2)?,
                    })
                })
                .map_err(|error| sqlite_error("listing attachment leases", &error))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| sqlite_error("listing attachment leases", &error))?;
            Ok(rows)
        })
        .await
    }

    /// Deletes an unleased attachment row, returning whether a row was
    /// deleted. The lease check and deletion share the writer transaction.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or SQLite fails.
    pub async fn delete_attachment(&self, sha256: &str) -> Result<bool, StoreError> {
        let sha256 = sha256.to_owned();
        let request_size = request_bytes(&[&sha256]);
        self.run_sized(request_size, move |connection| {
            let deleted = connection
                .execute(
                    "DELETE FROM attachments WHERE sha256 = ?1
                     AND NOT EXISTS (
                         SELECT 1 FROM attachment_leases WHERE sha256 = ?1
                     )",
                    params![sha256],
                )
                .map_err(|error| sqlite_error("deleting an attachment", &error))?;
            Ok(deleted == 1)
        })
        .await
    }

    /// Releases every attachment lease held by a completed turn.
    ///
    /// # Errors
    ///
    /// Returns an error when the writer task or `SQLite` fails.
    pub async fn release_turn_attachment_leases(
        &self,
        turn_row_id: i64,
    ) -> Result<u64, StoreError> {
        self.run(move |connection| {
            let removed = connection
                .execute(
                    "DELETE FROM attachment_leases WHERE turn_row_id = ?1",
                    params![turn_row_id],
                )
                .map_err(|error| sqlite_error("releasing attachment leases", &error))?;
            Ok(u64::try_from(removed).unwrap_or(u64::MAX))
        })
        .await
    }
}
