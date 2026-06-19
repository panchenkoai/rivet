//! Shared reader for rivet's SQLite state DB — the "metabase" (`.rivet_state.db`).
//!
//! The path convention (state DB sits next to the config, or next to the plan
//! for `apply`) and the `export_metrics` / `export_harm` schema were previously
//! re-opened ad hoc in 10+ live tests, each hardcoding the filename and column
//! names. This centralises that knowledge: one `StateDb`, one place to update
//! when the schema moves, and one row read per run instead of N per-column
//! `Connection::open` round-trips.

#![allow(dead_code)]

use rusqlite::Connection;

/// One `export_metrics` row, read in a single query. Only the columns the
/// live-metrics tests assert on are surfaced; extend as callers need more.
#[derive(Debug)]
pub struct MetricsRow {
    pub run_id: String,
    pub status: String,
    pub total_rows: Option<i64>,
    pub source_type: Option<String>,
    pub destination_type: Option<String>,
    pub rivet_version: Option<String>,
    pub batch_size: Option<i64>,
    pub chunk_size: Option<i64>,
    pub parallel: Option<i64>,
    pub files_committed: Option<i64>,
    pub longest_chunk_ms: Option<i64>,
    pub pg_temp_bytes_delta: Option<i64>,
    pub source_count: Option<i64>,
    pub reconciled: Option<bool>,
    pub validated: Option<bool>,
    pub schema_fingerprint: Option<String>,
    pub quality_passed: Option<bool>,
    pub batch_size_memory_mb: Option<i64>,
    pub skip_reason: Option<String>,
}

/// A handle to one run's state DB. Open once, query many.
pub struct StateDb {
    conn: Connection,
}

impl StateDb {
    /// Open the state DB that rivet writes next to a config file (the `run`
    /// path) — and, when the original config dir still exists, the `apply` path
    /// too (`apply_cmd` opens state next to the original config).
    pub fn next_to_config(cfg: &std::path::Path) -> Self {
        let db = cfg
            .parent()
            .expect("config path has a parent dir")
            .join(".rivet_state.db");
        let conn =
            Connection::open(&db).unwrap_or_else(|e| panic!("open state db {}: {e}", db.display()));
        Self { conn }
    }

    /// `run_id` of the most recent `export_metrics` row for `export`.
    pub fn latest_run_id(&self, export: &str) -> String {
        self.conn
            .query_row(
                "SELECT run_id FROM export_metrics \
                 WHERE export_name = ?1 ORDER BY id DESC LIMIT 1",
                [export],
                |r| r.get::<_, Option<String>>(0),
            )
            .expect("an export_metrics row must exist after the run")
            .expect("export_metrics.run_id must be set by the run path")
    }

    /// The full `export_metrics` row for `run_id`, read in one query.
    pub fn metrics_row(&self, run_id: &str) -> MetricsRow {
        self.conn
            .query_row(
                "SELECT run_id, status, total_rows, source_type, destination_type, \
                        rivet_version, batch_size, chunk_size, parallel, files_committed, \
                        longest_chunk_ms, pg_temp_bytes_delta, source_count, reconciled, validated, \
                        schema_fingerprint, quality_passed, batch_size_memory_mb, skip_reason \
                 FROM export_metrics WHERE run_id = ?1",
                [run_id],
                |r| {
                    Ok(MetricsRow {
                        run_id: r.get(0)?,
                        status: r.get(1)?,
                        total_rows: r.get(2)?,
                        source_type: r.get(3)?,
                        destination_type: r.get(4)?,
                        rivet_version: r.get(5)?,
                        batch_size: r.get(6)?,
                        chunk_size: r.get(7)?,
                        parallel: r.get(8)?,
                        files_committed: r.get(9)?,
                        longest_chunk_ms: r.get(10)?,
                        pg_temp_bytes_delta: r.get(11)?,
                        source_count: r.get(12)?,
                        reconciled: r.get(13)?,
                        validated: r.get(14)?,
                        schema_fingerprint: r.get(15)?,
                        quality_passed: r.get(16)?,
                        batch_size_memory_mb: r.get(17)?,
                        skip_reason: r.get(18)?,
                    })
                },
            )
            .unwrap_or_else(|e| panic!("read export_metrics row for run {run_id}: {e}"))
    }

    /// `(metric, delta)` rows from `export_harm` for `run_id`, sorted by metric.
    pub fn harm_rows(&self, run_id: &str) -> Vec<(String, i64)> {
        let mut stmt = self
            .conn
            .prepare("SELECT metric, delta FROM export_harm WHERE run_id = ?1 ORDER BY metric")
            .expect("prepare export_harm read");
        let rows = stmt
            .query_map([run_id], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
            })
            .expect("query export_harm");
        rows.filter_map(|r| r.ok()).collect()
    }
}
