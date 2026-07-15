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
}

impl StateStore {
    /// Log one load into `load_run` and mark each consumed extraction run in
    /// `loaded_source_run`. Idempotent: `load_id` and `(target_table,
    /// source_run_id)` upsert, so a retried load never double-inserts.
    pub fn store_load(&self, rec: &LoadRecord) -> Result<()> {
        let run_ids_json = serde_json::to_string(&rec.source_run_ids)?;
        match &self.conn {
            StateConn::Sqlite(c) => {
                c.execute(
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
                for rid in &rec.source_run_ids {
                    c.execute(
                        "INSERT OR REPLACE INTO loaded_source_run
                           (target_table, source_run_id, load_id, loaded_at)
                         VALUES (?1, ?2, ?3, ?4)",
                        rusqlite::params![rec.target_table, rid, rec.load_id, rec.finished_at],
                    )?;
                }
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                c.execute(
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
                for rid in &rec.source_run_ids {
                    c.execute(
                        "INSERT INTO loaded_source_run
                           (target_table, source_run_id, load_id, loaded_at)
                         VALUES ($1, $2, $3, $4)
                         ON CONFLICT (target_table, source_run_id) DO UPDATE SET
                             load_id   = excluded.load_id,
                             loaded_at = excluded.loaded_at",
                        &[&rec.target_table, rid, &rec.load_id, &rec.finished_at],
                    )?;
                }
            }
        }
        Ok(())
    }

    /// The extraction run_ids already loaded into `target_table` — the skip set
    /// an incremental load filters its candidate manifests against.
    pub fn loaded_source_run_ids(&self, target_table: &str) -> Result<HashSet<String>> {
        match &self.conn {
            StateConn::Sqlite(c) => {
                let mut stmt = c.prepare(
                    "SELECT source_run_id FROM loaded_source_run WHERE target_table = ?1",
                )?;
                let rows =
                    stmt.query_map(rusqlite::params![target_table], |r| r.get::<_, String>(0))?;
                let mut out = HashSet::new();
                for r in rows {
                    out.insert(r?);
                }
                Ok(out)
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                let rows = c.query(
                    "SELECT source_run_id FROM loaded_source_run WHERE target_table = $1",
                    &[&target_table],
                )?;
                Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
            }
        }
    }

    /// Recent `load_run` rows, newest first; optionally filtered to one target.
    pub fn recent_loads(
        &self,
        target_table: Option<&str>,
        limit: usize,
    ) -> Result<Vec<LoadRecord>> {
        const COLS: &str = "load_id, export_name, target_table, warehouse, mode, \
                            source_run_ids, rows_loaded, status, finished_at";
        // (load_id, export, target, warehouse, mode, run_ids_json, rows, status, finished_at)
        type Raw = (
            String,
            String,
            String,
            String,
            String,
            String,
            i64,
            String,
            String,
        );
        let build = |r: Raw| LoadRecord {
            load_id: r.0,
            export_name: r.1,
            target_table: r.2,
            warehouse: r.3,
            mode: r.4,
            source_run_ids: serde_json::from_str(&r.5).unwrap_or_default(),
            rows_loaded: r.6,
            status: r.7,
            finished_at: r.8,
        };
        match &self.conn {
            StateConn::Sqlite(c) => {
                let map = |row: &rusqlite::Row| -> rusqlite::Result<Raw> {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                    ))
                };
                let mut out = Vec::new();
                match target_table {
                    Some(t) => {
                        let mut stmt = c.prepare(&format!(
                            "SELECT {COLS} FROM load_run WHERE target_table = ?1 \
                             ORDER BY finished_at DESC LIMIT ?2"
                        ))?;
                        for r in stmt.query_map(rusqlite::params![t, limit as i64], map)? {
                            out.push(build(r?));
                        }
                    }
                    None => {
                        let mut stmt = c.prepare(&format!(
                            "SELECT {COLS} FROM load_run ORDER BY finished_at DESC LIMIT ?1"
                        ))?;
                        for r in stmt.query_map(rusqlite::params![limit as i64], map)? {
                            out.push(build(r?));
                        }
                    }
                }
                Ok(out)
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                let rows = match target_table {
                    Some(t) => c.query(
                        &format!(
                            "SELECT {COLS} FROM load_run WHERE target_table = $1 \
                             ORDER BY finished_at DESC LIMIT {limit}"
                        ),
                        &[&t],
                    )?,
                    None => c.query(
                        &format!(
                            "SELECT {COLS} FROM load_run ORDER BY finished_at DESC LIMIT {limit}"
                        ),
                        &[],
                    )?,
                };
                Ok(rows
                    .iter()
                    .map(|row| {
                        build((
                            row.get(0),
                            row.get(1),
                            row.get(2),
                            row.get(3),
                            row.get(4),
                            row.get(5),
                            row.get(6),
                            row.get(7),
                            row.get(8),
                        ))
                    })
                    .collect())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(load_id: &str, target: &str, runs: &[&str], rows: i64, status: &str) -> LoadRecord {
        LoadRecord {
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
