use crate::error::Result;

use super::{ProgressKey, StateStore};

/// Whether `table` is none of the `captured` tables (a bare name may be the qualified one).
pub(crate) fn joins_capture(captured: &[String], table: &str) -> bool {
    captured
        .iter()
        .all(|c| super::cursor::streams_differ(c, table))
}

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

    /// The tables the CDC stream of `key` captured into `destination` on its last run; `None` when no run recorded them there.
    pub fn captured_tables(
        &self,
        key: &ProgressKey,
        destination: &str,
    ) -> Result<Option<Vec<String>>> {
        let row = self.query_opt(
            "SELECT stream, destination FROM export_state WHERE export_name = ?1 AND prefix = ?2",
            &[key.export_name.as_str().into(), key.source.as_str().into()],
            |r| (r.opt_text(0), r.opt_text(1)),
        )?;
        Ok(match row {
            Some((Some(stream), Some(dest))) if dest == destination => {
                serde_json::from_str(&stream).ok()
            }
            _ => None,
        })
    }

    /// Record the tables of `key` as what its CDC stream captures into `destination` from this run on.
    pub fn record_captured_tables(&self, key: &ProgressKey, destination: &str) -> Result<()> {
        self.execute(
            "INSERT INTO export_state (export_name, prefix, stream, destination)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(export_name, prefix) DO UPDATE SET
                stream = excluded.stream,
                destination = excluded.destination",
            &[
                key.export_name.as_str().into(),
                key.source.as_str().into(),
                key.stream.as_str().into(),
                destination.into(),
            ],
        )?;
        Ok(())
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

    fn names(t: &[&str]) -> Vec<String> {
        t.iter().map(|t| t.to_string()).collect()
    }

    /// The captured set is what the last run recorded, for its own source and destination only.
    #[test]
    fn a_stream_remembers_the_tables_of_its_last_run_per_source_and_destination() {
        let s = StateStore::open_in_memory().unwrap();
        let key = |tables: &[&str]| ProgressKey::cdc("e", "pg://h/db", &names(tables));
        assert_eq!(s.captured_tables(&key(&["a"]), "b/out").unwrap(), None);
        s.record_captured_tables(&key(&["b", "a,x"]), "b/out")
            .unwrap();
        assert_eq!(
            s.captured_tables(&key(&["a"]), "b/out").unwrap(),
            Some(names(&["a,x", "b"]))
        );
        assert_eq!(s.captured_tables(&key(&["a"]), "b/other").unwrap(), None);
        let elsewhere = ProgressKey::cdc("e", "pg://h/other", &names(&["a"]));
        assert_eq!(s.captured_tables(&elsewhere, "b/out").unwrap(), None);
        s.record_captured_tables(&key(&["b"]), "b/out").unwrap();
        assert_eq!(
            s.captured_tables(&key(&["a"]), "b/out").unwrap(),
            Some(names(&["b"]))
        );
    }

    /// A row a batch run of the same name wrote holds a relation, not a table set: nothing is known.
    #[test]
    fn a_stream_recorded_by_a_batch_run_is_not_a_captured_set() {
        let s = StateStore::open_in_memory().unwrap();
        let key = ProgressKey::cdc("e", "pg://h/db", &names(&["a"]));
        s.record_captured_tables(&key, "b/out").unwrap();
        s.execute("UPDATE export_state SET stream = 'public.a'", &[])
            .unwrap();
        assert_eq!(s.captured_tables(&key, "b/out").unwrap(), None);
    }

    #[test]
    fn a_table_joins_a_capture_that_names_it_in_no_spelling() {
        let captured = names(&["public.a", "b"]);
        assert!(!joins_capture(&captured, "a"));
        assert!(!joins_capture(&captured, "public.b"));
        assert!(!joins_capture(&captured, "b"));
        assert!(joins_capture(&captured, "c"));
        assert!(joins_capture(&captured, "xa"));
        assert!(joins_capture(&[], "a"));
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
