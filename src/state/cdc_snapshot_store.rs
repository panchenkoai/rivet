use crate::error::Result;

use super::StateStore;

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
        prefix: &str,
        run_id: &str,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        // Keyed by the baseline's DESTINATION too: two configs sharing a state DB
        // and an export name write two rows, not one.
        self.execute(
            "INSERT INTO cdc_snapshot (export_name, table_name, prefix, run_id, completed_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (export_name, table_name, prefix) DO UPDATE SET
                 run_id       = excluded.run_id,
                 completed_at = excluded.completed_at",
            &[
                export_name.into(),
                table_name.into(),
                prefix.into(),
                run_id.into(),
                now.into(),
            ],
        )?;
        Ok(())
    }

    /// Whether `(export_name, table_name)`'s baseline under `prefix` has completed
    /// per the state DB — the authoritative, GCS-independent signal. A row written
    /// before v29 carries no prefix and counts for every prefix.
    pub fn snapshot_done(&self, export_name: &str, table_name: &str, prefix: &str) -> Result<bool> {
        Ok(self
            .query_opt(
                "SELECT COUNT(*) FROM cdc_snapshot
                 WHERE export_name = ?1 AND table_name = ?2 AND (prefix = ?3 OR prefix = '')",
                &[export_name.into(), table_name.into(), prefix.into()],
                |r| r.i64(0),
            )?
            .unwrap_or(0)
            > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const P: &str = "b/cdc/customers/snapshot";

    #[test]
    fn snapshot_done_is_false_until_marked_then_true() {
        let s = StateStore::open_in_memory().unwrap();
        assert!(!s.snapshot_done("customers", "customers", P).unwrap());
        s.mark_snapshot_done("customers", "customers", P, "run_1")
            .unwrap();
        assert!(s.snapshot_done("customers", "customers", P).unwrap());
        // Scoped per (export, table): a sibling table is still un-snapshotted.
        assert!(!s.snapshot_done("customers", "orders", P).unwrap());
    }

    #[test]
    fn mark_snapshot_done_is_idempotent() {
        let s = StateStore::open_in_memory().unwrap();
        s.mark_snapshot_done("e", "t", P, "run_1").unwrap();
        s.mark_snapshot_done("e", "t", P, "run_2").unwrap(); // replay/re-record
        assert!(s.snapshot_done("e", "t", P).unwrap());
    }

    /// Two configs on one state DB, both exporting `users` into different
    /// destinations: A's finished baseline must not make B skip its own.
    #[test]
    fn a_same_named_export_on_another_prefix_has_its_own_baseline() {
        let s = StateStore::open_in_memory().unwrap();
        s.mark_snapshot_done("users", "users", "b/pa/users/snapshot", "a1")
            .unwrap();
        assert!(
            !s.snapshot_done("users", "users", "b/pb/users/snapshot")
                .unwrap()
        );
        s.mark_snapshot_done("users", "users", "b/pb/users/snapshot", "b1")
            .unwrap();
        assert!(
            s.snapshot_done("users", "users", "b/pa/users/snapshot")
                .unwrap()
        );
        assert!(
            s.snapshot_done("users", "users", "b/pb/users/snapshot")
                .unwrap()
        );
    }

    /// A row recorded before v29 has no prefix: it stays "done" wherever asked,
    /// so an upgrade never re-baselines a stream that already has one.
    #[test]
    fn a_legacy_row_without_a_prefix_counts_for_every_prefix() {
        let s = StateStore::open_in_memory().unwrap();
        s.mark_snapshot_done("orders", "orders", "", "old").unwrap();
        assert!(
            s.snapshot_done("orders", "orders", "b/anything/snapshot")
                .unwrap()
        );
    }
}
