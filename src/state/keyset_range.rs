//! Parallel-keyset crash-recovery ranges (`keyset_range`), feat/parallel-keyset
//! iteration 2.
//!
//! A parallel keyset run partitions the key into N stable ROW-percentile ranges
//! sampled once at open. Recovery is **per-range** (coarse): the boundaries are
//! persisted here at open (keyed by run_id) so a resume reloads the SAME ranges
//! rather than re-sampling a possibly-changed table (which would move the
//! boundaries and leave a gap). Each worker flips only its OWN `(export_name,
//! range_index)` row to `done=1` at completion — in the SAME transaction that
//! records its parts to `file_log` — so the checkpoint is atomic: a crash before
//! that commit leaves the range `done=0` (re-read from `lo` on resume, its parts
//! overwritten by stable run_id-based names), a crash after it leaves the range
//! `done=1` (skipped on resume, its parts rehydrated from `file_log`).

use crate::error::Result;

use super::{StateRef, StateStore, open_connection, pg_sql};

/// One persisted range of a parallel keyset run.
#[derive(Debug, Clone)]
pub struct KeysetRangeRow {
    pub range_index: i64,
    /// Exclusive lower bound (`None` = -∞, the first range).
    pub lo: Option<String>,
    /// Inclusive upper bound (`None` = +∞, the last range).
    pub hi: Option<String>,
    pub done: bool,
}

/// A part a worker committed for its range — the `file_log` payload written
/// atomically with the range's `done` flip.
pub struct KeysetRangePart {
    pub file_name: String,
    pub rows: i64,
    pub bytes: i64,
}

impl StateStore {
    /// Persist the sampled ranges of a FRESH parallel keyset run (all `done=0`),
    /// replacing any prior set for this export. Called once at open, before the
    /// workers spawn; the boundaries are then immutable for the run's lifetime.
    pub fn persist_keyset_ranges(
        &self,
        export_name: &str,
        run_id: &str,
        ranges: &[(Option<String>, Option<String>)],
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.execute(
            "DELETE FROM keyset_range WHERE export_name = ?1",
            &[export_name.into()],
        )?;
        for (idx, (lo, hi)) in ranges.iter().enumerate() {
            self.execute(
                "INSERT INTO keyset_range \
                 (export_name, run_id, range_index, lo, hi, done, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6)",
                &[
                    export_name.into(),
                    run_id.into(),
                    (idx as i64).into(),
                    lo.clone().into(),
                    hi.clone().into(),
                    now.as_str().into(),
                ],
            )?;
        }
        Ok(())
    }

    /// Load the persisted ranges for a resuming run, ordered by `range_index`.
    /// Filtered by `run_id` so a stale set from a superseded run is ignored.
    pub fn load_keyset_ranges(
        &self,
        export_name: &str,
        run_id: &str,
    ) -> Result<Vec<KeysetRangeRow>> {
        let sql = "SELECT range_index, lo, hi, done FROM keyset_range \
                   WHERE export_name = ?1 AND run_id = ?2 ORDER BY range_index";
        self.query(sql, &[export_name.into(), run_id.into()], |r| {
            KeysetRangeRow {
                range_index: r.i64(0),
                lo: r.opt_text(1),
                hi: r.opt_text(2),
                done: r.i64(3) != 0,
            }
        })
    }

    /// Clear an export's persisted ranges. Called post-finalize (via
    /// `finalize_keyset_anchor`) once the complete manifest is written — a no-op
    /// for any export that never ran parallel keyset.
    pub fn clear_keyset_ranges(&self, export_name: &str) -> Result<()> {
        self.execute(
            "DELETE FROM keyset_range WHERE export_name = ?1",
            &[export_name.into()],
        )?;
        Ok(())
    }

    /// Atomically COMMIT a completed range from a worker thread: record every part
    /// to `file_log` AND flip the range's `done=1`, in ONE transaction, over a
    /// fresh connection (the live `StateStore` is not `Sync`, so workers reconnect
    /// per the ADR-0011 `*_at_ref` pattern). The transaction is the checkpoint
    /// boundary — a crash before it commits leaves the range `done=0` with no
    /// `file_log` rows (re-read on resume), after it leaves both durable (skipped +
    /// rehydrated on resume). Only the range's OWN row is touched, so concurrent
    /// workers never contend.
    pub fn commit_keyset_range_at_ref(
        state_ref: &StateRef,
        run_id: &str,
        export_name: &str,
        range_index: i64,
        parts: &[KeysetRangePart],
        format: &str,
        compression: Option<&str>,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let file_sql = "INSERT INTO file_log \
             (run_id, export_name, file_name, row_count, bytes, format, compression, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)";
        // Scope the `done` flip by run_id (the table's PK is (export_name,
        // range_index) — run_id is NOT in it), so a run can never flip a row
        // belonging to ANOTHER run's recovery set left behind by a crash (H1).
        let done_sql = "UPDATE keyset_range SET done = 1, updated_at = ?1 \
             WHERE export_name = ?2 AND range_index = ?3 AND run_id = ?4";
        match state_ref {
            StateRef::Sqlite(db_path) => {
                let mut conn = open_connection(db_path)?;
                let tx = conn.transaction()?;
                for p in parts {
                    tx.execute(
                        file_sql,
                        rusqlite::params![
                            run_id,
                            export_name,
                            p.file_name,
                            p.rows,
                            p.bytes,
                            format,
                            compression,
                            now
                        ],
                    )?;
                }
                tx.execute(
                    done_sql,
                    rusqlite::params![now, export_name, range_index, run_id],
                )?;
                tx.commit()?;
            }
            StateRef::Postgres(url) => {
                let mut client = super::connect_pg(url)?;
                let mut tx = client.transaction()?;
                for p in parts {
                    tx.execute(
                        &pg_sql(file_sql),
                        &[
                            &run_id,
                            &export_name,
                            &p.file_name,
                            &p.rows,
                            &p.bytes,
                            &format,
                            &compression,
                            &now,
                        ],
                    )?;
                }
                tx.execute(
                    &pg_sql(done_sql),
                    &[&now, &export_name, &range_index, &run_id],
                )?;
                tx.commit()?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> StateStore {
        StateStore::open_in_memory().expect("in-memory store")
    }

    #[test]
    fn persist_then_load_round_trips_ranges_all_not_done() {
        let s = store();
        let ranges = vec![
            (None, Some("k0500".to_string())),
            (Some("k0500".to_string()), Some("k1000".to_string())),
            (Some("k1000".to_string()), None),
        ];
        s.persist_keyset_ranges("exp", "run-1", &ranges).unwrap();
        let loaded = s.load_keyset_ranges("exp", "run-1").unwrap();
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0].lo, None);
        assert_eq!(loaded[0].hi.as_deref(), Some("k0500"));
        assert_eq!(loaded[2].hi, None);
        assert!(loaded.iter().all(|r| !r.done), "fresh ranges are not done");
    }

    #[test]
    fn load_with_a_different_run_id_returns_nothing() {
        // A stale set from a superseded run must not leak into a fresh run's load.
        let s = store();
        s.persist_keyset_ranges("exp", "run-1", &[(None, None)])
            .unwrap();
        assert!(s.load_keyset_ranges("exp", "run-2").unwrap().is_empty());
    }

    #[test]
    fn commit_range_marks_only_its_own_row_done_and_records_parts() {
        // `_at_ref` reconnects (workers are not `Sync`), so an in-memory store won't
        // do: `open_connection(":memory:")` is a DISTINCT empty DB. Use a file store.
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("rivet.yaml");
        std::fs::write(&cfg, "# test").unwrap();
        let s = StateStore::open(cfg.to_str().unwrap()).unwrap();
        let ranges = vec![
            (None, Some("k5".to_string())),
            (Some("k5".to_string()), None),
        ];
        s.persist_keyset_ranges("exp", "run-1", &ranges).unwrap();

        StateStore::commit_keyset_range_at_ref(
            s.state_ref(),
            "run-1",
            "exp",
            1,
            &[KeysetRangePart {
                file_name: "exp_run-1_pk_w1_0.parquet".to_string(),
                rows: 42,
                bytes: 100,
            }],
            "parquet",
            Some("zstd"),
        )
        .unwrap();

        let loaded = s.load_keyset_ranges("exp", "run-1").unwrap();
        assert!(!loaded[0].done, "range 0 untouched");
        assert!(loaded[1].done, "range 1 committed → done");
        // The part landed in file_log under the run_id (rehydration source).
        let files = s.list_files_for_run("run-1").unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].file_name, "exp_run-1_pk_w1_0.parquet");
        assert_eq!(files[0].row_count, 42);
    }

    #[test]
    fn clear_removes_all_ranges_for_the_export() {
        let s = store();
        s.persist_keyset_ranges("exp", "run-1", &[(None, None)])
            .unwrap();
        s.clear_keyset_ranges("exp").unwrap();
        assert!(s.load_keyset_ranges("exp", "run-1").unwrap().is_empty());
    }
}
