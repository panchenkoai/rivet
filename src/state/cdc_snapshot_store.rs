use crate::error::Result;

use super::{StateConn, StateStore};

impl StateStore {
    /// Record that `cdc.initial: snapshot`'s backfill for `(export_name,
    /// table_name)` completed, on run `run_id`. Idempotent (upsert on the
    /// `(export_name, table_name)` key), so a retried run never double-inserts.
    ///
    /// This is the durable, cleanup-proof twin of the GCS `snapshot/_SUCCESS`
    /// marker: once here, `cleanup_source: true` may wipe the bucket without the
    /// next run mistaking the table for un-snapshotted and re-snapshotting it.
    pub fn mark_snapshot_done(
        &self,
        export_name: &str,
        table_name: &str,
        run_id: &str,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        match &self.conn {
            StateConn::Sqlite(c) => {
                c.execute(
                    "INSERT OR REPLACE INTO cdc_snapshot
                       (export_name, table_name, run_id, completed_at)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![export_name, table_name, run_id, now],
                )?;
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                c.execute(
                    "INSERT INTO cdc_snapshot
                       (export_name, table_name, run_id, completed_at)
                     VALUES ($1, $2, $3, $4)
                     ON CONFLICT (export_name, table_name) DO UPDATE SET
                         run_id       = excluded.run_id,
                         completed_at = excluded.completed_at",
                    &[&export_name, &table_name, &run_id, &now],
                )?;
            }
        }
        Ok(())
    }

    /// Whether `(export_name, table_name)`'s initial snapshot has completed per
    /// the state DB — the authoritative, GCS-independent signal.
    pub fn snapshot_done(&self, export_name: &str, table_name: &str) -> Result<bool> {
        match &self.conn {
            StateConn::Sqlite(c) => {
                let n: i64 = c.query_row(
                    "SELECT COUNT(*) FROM cdc_snapshot
                     WHERE export_name = ?1 AND table_name = ?2",
                    rusqlite::params![export_name, table_name],
                    |r| r.get(0),
                )?;
                Ok(n > 0)
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                let row = c.query_one(
                    "SELECT COUNT(*) FROM cdc_snapshot
                     WHERE export_name = $1 AND table_name = $2",
                    &[&export_name, &table_name],
                )?;
                Ok(row.get::<_, i64>(0) > 0)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_done_is_false_until_marked_then_true() {
        let s = StateStore::open_in_memory().unwrap();
        assert!(!s.snapshot_done("customers", "customers").unwrap());
        s.mark_snapshot_done("customers", "customers", "run_1")
            .unwrap();
        assert!(s.snapshot_done("customers", "customers").unwrap());
        // Scoped per (export, table): a sibling table is still un-snapshotted.
        assert!(!s.snapshot_done("customers", "orders").unwrap());
    }

    #[test]
    fn mark_snapshot_done_is_idempotent() {
        let s = StateStore::open_in_memory().unwrap();
        s.mark_snapshot_done("e", "t", "run_1").unwrap();
        s.mark_snapshot_done("e", "t", "run_2").unwrap(); // replay/re-record
        assert!(s.snapshot_done("e", "t").unwrap());
    }
}
