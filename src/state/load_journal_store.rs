use std::collections::HashSet;

use crate::error::Result;

use super::{StateConn, StateStore};

/// One `rivet load` invocation's ledger record: the `load_run` audit row plus
/// the extraction `source_run_ids` it consumed (written into `loaded_source_run`
/// so a later load skips them). `status` ∈ `success` | `failed`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadRecord {
    pub load_id: String,
    pub export_name: String,
    /// Fully-qualified target the rows landed in (`proj.ds.table` /
    /// `db.schema.table`) — the ledger key, so two configs targeting different
    /// datasets never share a skip set.
    pub target_table: String,
    pub warehouse: String,
    pub mode: String,
    pub source_run_ids: Vec<String>,
    pub rows_loaded: i64,
    pub status: String,
    pub finished_at: String,
    /// WHICH SOURCE these rows came from (`engine:schema.table`), so a later load
    /// of the same target from a DIFFERENT database is refusable. Empty when the
    /// manifests did not say — treated as unknown, never as a mismatch.
    pub source_ident: String,
}

impl StateStore {
    /// Log one load into `load_run` and — only for a **successful** load — mark
    /// each consumed extraction run in `loaded_source_run` (the skip set). A
    /// FAILED load still records its audit row with the attempted run_ids but
    /// marks none loaded, so the next load RETRIES those runs instead of
    /// skipping their never-loaded data forever (silent loss). Idempotent:
    /// `load_id` and `(target_table, source_run_id)` upsert, so a retried load
    /// never double-inserts.
    pub fn store_load(&self, rec: &LoadRecord) -> Result<()> {
        let run_ids_json = serde_json::to_string(&rec.source_run_ids)?;
        // A run is "loaded" only when its load succeeded; a failed load must
        // leave its runs retryable, never mark them loaded (else their data is
        // skipped on every subsequent load).
        let mark_loaded = rec.status == "success";
        match &self.conn {
            StateConn::Sqlite(c) => {
                // One transaction: the audit row + the skip-set rows commit
                // together, so a crash mid-write never leaves a `success` load_run
                // with a PARTIAL loaded_source_run (which would re-select the
                // unmarked runs next load).
                let tx = c.unchecked_transaction()?;
                tx.execute(
                    "INSERT OR REPLACE INTO load_run
                       (load_id, export_name, target_table, warehouse, mode,
                        source_run_ids, rows_loaded, status, finished_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    rusqlite::params![
                        rec.load_id,
                        rec.export_name,
                        rec.target_table,
                        rec.warehouse,
                        rec.mode,
                        run_ids_json,
                        rec.rows_loaded,
                        rec.status,
                        rec.finished_at,
                    ],
                )?;
                if mark_loaded {
                    for rid in &rec.source_run_ids {
                        tx.execute(
                            "INSERT OR REPLACE INTO loaded_source_run
                               (target_table, source_run_id, load_id, loaded_at, source_ident)
                             VALUES (?1, ?2, ?3, ?4, ?5)",
                            rusqlite::params![
                                rec.target_table,
                                rid,
                                rec.load_id,
                                rec.finished_at,
                                rec.source_ident,
                            ],
                        )?;
                    }
                }
                tx.commit()?;
            }
            StateConn::Postgres(client) => {
                // One transaction (mirrors the SQLite arm): the audit row and the
                // skip-set rows commit atomically.
                let mut c = client.borrow_mut();
                let mut tx = c.transaction()?;
                tx.execute(
                    "INSERT INTO load_run
                       (load_id, export_name, target_table, warehouse, mode,
                        source_run_ids, rows_loaded, status, finished_at)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                     ON CONFLICT (load_id) DO UPDATE SET
                         export_name    = excluded.export_name,
                         target_table   = excluded.target_table,
                         warehouse      = excluded.warehouse,
                         mode           = excluded.mode,
                         source_run_ids = excluded.source_run_ids,
                         rows_loaded    = excluded.rows_loaded,
                         status         = excluded.status,
                         finished_at    = excluded.finished_at",
                    &[
                        &rec.load_id,
                        &rec.export_name,
                        &rec.target_table,
                        &rec.warehouse,
                        &rec.mode,
                        &run_ids_json,
                        &rec.rows_loaded,
                        &rec.status,
                        &rec.finished_at,
                    ],
                )?;
                if mark_loaded {
                    for rid in &rec.source_run_ids {
                        tx.execute(
                            "INSERT INTO loaded_source_run
                               (target_table, source_run_id, load_id, loaded_at, source_ident)
                             VALUES ($1, $2, $3, $4, $5)
                             ON CONFLICT (target_table, source_run_id) DO UPDATE SET
                                 load_id      = excluded.load_id,
                                 loaded_at    = excluded.loaded_at,
                                 source_ident = excluded.source_ident",
                            &[
                                &rec.target_table,
                                rid,
                                &rec.load_id,
                                &rec.finished_at,
                                &rec.source_ident,
                            ],
                        )?;
                    }
                }
                tx.commit()?;
            }
        }
        Ok(())
    }

    /// The extraction run_ids already loaded into `target_table` — the skip set
    /// an incremental load filters its candidate manifests against.
    /// The DISTINCT non-empty source identities a target table has been loaded
    /// from. More than one means two different databases wrote the same
    /// warehouse table — the second silently replaced the first.
    pub fn loaded_source_idents(&self, target_table: &str) -> Result<Vec<String>> {
        let mut out = self.query(
            "SELECT DISTINCT source_ident FROM loaded_source_run \
             WHERE target_table = ?1 AND source_ident IS NOT NULL AND source_ident <> ''",
            &[target_table.into()],
            |r| r.text(0),
        )?;
        out.sort();
        out.dedup();
        Ok(out)
    }

    pub fn loaded_source_run_ids(&self, target_table: &str) -> Result<HashSet<String>> {
        Ok(self
            .query(
                "SELECT source_run_id FROM loaded_source_run WHERE target_table = ?1",
                &[target_table.into()],
                |r| r.text(0),
            )?
            .into_iter()
            .collect())
    }

    /// Recent `load_run` rows, newest first; optionally filtered to one target.
    pub fn recent_loads(
        &self,
        target_table: Option<&str>,
        limit: usize,
    ) -> Result<Vec<LoadRecord>> {
        const COLS: &str = "load_id, export_name, target_table, warehouse, mode, \
                            source_run_ids, rows_loaded, status, finished_at";
        let build = |r: &dyn super::row::StateRow| LoadRecord {
            // Reading back a historical row: the identity is not part of the
            // load_run projection, and no caller of this reader needs it.
            source_ident: String::new(),
            load_id: r.text(0),
            export_name: r.text(1),
            target_table: r.text(2),
            warehouse: r.text(3),
            mode: r.text(4),
            source_run_ids: serde_json::from_str(&r.text(5)).unwrap_or_default(),
            rows_loaded: r.i64(6),
            status: r.text(7),
            finished_at: r.text(8),
        };
        match target_table {
            Some(t) => self.query(
                &format!(
                    "SELECT {COLS} FROM load_run WHERE target_table = ?1 \
                     ORDER BY finished_at DESC LIMIT ?2"
                ),
                &[t.into(), (limit as i64).into()],
                build,
            ),
            None => self.query(
                &format!("SELECT {COLS} FROM load_run ORDER BY finished_at DESC LIMIT ?1"),
                &[(limit as i64).into()],
                build,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(load_id: &str, target: &str, runs: &[&str], rows: i64, status: &str) -> LoadRecord {
        LoadRecord {
            source_ident: String::new(),
            load_id: load_id.into(),
            export_name: "customers".into(),
            target_table: target.into(),
            warehouse: "bigquery".into(),
            mode: "cdc".into(),
            source_run_ids: runs.iter().map(|s| s.to_string()).collect(),
            rows_loaded: rows,
            status: status.into(),
            finished_at: format!("2026-01-01T00:00:0{}Z", load_id.len() % 10),
        }
    }

    /// The source-ownership guard reads `loaded_source_idents`; this asserts the
    /// value it reads is the value `store_load` WROTE, on the default backend.
    ///
    /// It was not: the SQLite arm inserted four columns and omitted
    /// `source_ident` entirely (the Postgres arm wrote all five), so
    /// `loaded_source_idents` — which filters `IS NOT NULL AND <> ''` — always
    /// came back empty, and the guard that refuses "two sources into one
    /// warehouse table" never fired on the backend almost everyone runs. It also
    /// explains an asymmetry in the release gate: its BigQuery stage loads three
    /// engines into one table and PASSED on the SQLite pass while failing on the
    /// Postgres one.
    ///
    /// Nothing caught it because the shared `rec()` fixture sets
    /// `source_ident: String::new()` — the empty string the reader filters out.
    /// A field can only be tested by a fixture that crosses its threshold, and a
    /// value that decides something must be observed AT the boundary, produced by
    /// the real producer rather than handed over by the test.
    #[test]
    fn a_load_records_the_source_identity_the_ownership_guard_reads_back() {
        let s = StateStore::open_in_memory().unwrap();
        let mut r = rec("L1", "p.d.orders", &["r1"], 100, "success");
        r.source_ident = "postgres:public.orders".into();
        s.store_load(&r).unwrap();

        assert_eq!(
            s.loaded_source_idents("p.d.orders").unwrap(),
            vec!["postgres:public.orders".to_string()],
            "the guard cannot refuse a second source if the first one's identity was never stored"
        );

        // A SECOND source into the same table is what the guard exists to catch,
        // so both identities must be visible to it — one row is not enough to
        // prove the reader distinguishes them.
        let mut r2 = rec("L2", "p.d.orders", &["r2"], 50, "success");
        r2.source_ident = "mysql:rivet.orders".into();
        s.store_load(&r2).unwrap();
        let mut got = s.loaded_source_idents("p.d.orders").unwrap();
        got.sort();
        assert_eq!(
            got,
            vec![
                "mysql:rivet.orders".to_string(),
                "postgres:public.orders".to_string()
            ],
            "two sources under one target table must both be recorded — that pair IS the refusal"
        );

        // An empty ident stays filtered: a stateless/legacy load must not be
        // mistaken for a competing source.
        let mut r3 = rec("L3", "p.d.other", &["r3"], 10, "success");
        r3.source_ident = String::new();
        s.store_load(&r3).unwrap();
        assert!(
            s.loaded_source_idents("p.d.other").unwrap().is_empty(),
            "an unidentified load must not manufacture a phantom owner"
        );
    }

    #[test]
    fn store_load_records_run_and_marks_source_runs() {
        let s = StateStore::open_in_memory().unwrap();
        s.store_load(&rec("L1", "p.d.customers", &["r1", "r2"], 100, "success"))
            .unwrap();

        let loaded = s.loaded_source_run_ids("p.d.customers").unwrap();
        assert_eq!(loaded, HashSet::from(["r1".to_string(), "r2".to_string()]));
        // A different target has its own (empty) skip set.
        assert!(s.loaded_source_run_ids("p.d.other").unwrap().is_empty());

        let loads = s.recent_loads(Some("p.d.customers"), 10).unwrap();
        assert_eq!(loads.len(), 1);
        assert_eq!(loads[0].rows_loaded, 100);
        assert_eq!(loads[0].source_run_ids, vec!["r1", "r2"]);
    }

    #[test]
    fn failed_load_leaves_its_source_runs_retryable() {
        let s = StateStore::open_in_memory().unwrap();
        // A failed load records its audit row (with the attempted runs) but must
        // NOT mark them loaded — else the next load skips their never-loaded data.
        s.store_load(&rec("L1", "p.d.t", &["r1", "r2"], 0, "failed"))
            .unwrap();
        assert!(
            s.loaded_source_run_ids("p.d.t").unwrap().is_empty(),
            "a failed load must leave its runs retryable, never mark them loaded"
        );
        let loads = s.recent_loads(Some("p.d.t"), 10).unwrap();
        assert_eq!(loads.len(), 1);
        assert_eq!(loads[0].status, "failed");
        assert_eq!(
            loads[0].source_run_ids,
            vec!["r1", "r2"],
            "the audit row still records what the failed load attempted"
        );

        // A later SUCCESS over the same runs marks them loaded (the retry landed).
        s.store_load(&rec("L2", "p.d.t", &["r1", "r2"], 100, "success"))
            .unwrap();
        assert_eq!(
            s.loaded_source_run_ids("p.d.t").unwrap(),
            HashSet::from(["r1".to_string(), "r2".to_string()])
        );
    }

    #[test]
    fn store_load_is_idempotent_on_load_id_and_source_run() {
        let s = StateStore::open_in_memory().unwrap();
        let r = rec("L1", "p.d.t", &["r1"], 10, "success");
        s.store_load(&r).unwrap();
        s.store_load(&r).unwrap(); // replay
        assert_eq!(s.recent_loads(None, 10).unwrap().len(), 1);
        assert_eq!(s.loaded_source_run_ids("p.d.t").unwrap().len(), 1);
    }

    #[test]
    fn recent_loads_filters_by_target_and_orders_newest_first() {
        let s = StateStore::open_in_memory().unwrap();
        s.store_load(&rec("La", "p.d.a", &["r1"], 1, "success"))
            .unwrap();
        s.store_load(&rec("Lbb", "p.d.b", &["r2"], 2, "success"))
            .unwrap();
        s.store_load(&rec("Lccc", "p.d.b", &["r3"], 3, "failed"))
            .unwrap();

        let all = s.recent_loads(None, 10).unwrap();
        assert_eq!(all.len(), 3);
        // finished_at is keyed off load_id length in the fixture (3 > 2 > …).
        assert_eq!(all[0].load_id, "Lccc");

        let only_b = s.recent_loads(Some("p.d.b"), 10).unwrap();
        assert_eq!(only_b.len(), 2);
        assert!(only_b.iter().all(|l| l.target_table == "p.d.b"));
    }
}
