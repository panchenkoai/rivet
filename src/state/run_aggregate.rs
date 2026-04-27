//! Aggregate run summary store — one row per `rivet run` invocation.
//!
//! Per-export rows stay in `export_metrics`.  This table answers:
//! - "what happened in the last N scheduled runs as a whole?"
//! - "which run produced the most data / the most failures?"
//!
//! `details_json` carries the per-export breakdown so callers do not have to
//! join on `run_at` ranges to reconstruct the run.  This is intentional: the
//! aggregate row is observational, not a source of truth — `export_metrics`
//! remains the canonical per-export record.
use crate::error::Result;

use super::StateStore;

/// One aggregated `rivet run`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RunAggregate {
    /// Unique id assigned by the pipeline (`agg_<utc_ts>`).
    pub run_aggregate_id: String,
    pub started_at: String,
    pub finished_at: String,
    pub duration_ms: i64,
    pub config_path: Option<String>,
    /// `sequential` | `parallel-threads` | `parallel-processes`.
    pub parallel_mode: String,
    pub total_exports: usize,
    pub success_count: usize,
    pub failed_count: usize,
    pub skipped_count: usize,
    pub total_rows: i64,
    pub total_files: i64,
    pub total_bytes: u64,
    pub per_export: Vec<RunAggregateEntry>,
}

/// Per-export row inside an aggregate.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RunAggregateEntry {
    pub export_name: String,
    pub status: String,
    pub run_id: String,
    pub rows: i64,
    pub files: i64,
    pub bytes: u64,
    pub duration_ms: i64,
    pub mode: String,
    pub error_message: Option<String>,
}

impl StateStore {
    /// Persist an aggregate.  `per_export` is serialized as a JSON array into
    /// `details_json`.
    pub fn record_run_aggregate(&self, agg: &RunAggregate) -> Result<()> {
        let details = serde_json::to_string(&agg.per_export)
            .map_err(|e| anyhow::anyhow!("run_aggregate: serialize details_json: {:#}", e))?;
        self.conn.execute(
            "INSERT INTO run_aggregate (
                run_aggregate_id, started_at, finished_at, duration_ms,
                config_path, parallel_mode,
                total_exports, success_count, failed_count, skipped_count,
                total_rows, total_files, total_bytes, details_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            rusqlite::params![
                agg.run_aggregate_id,
                agg.started_at,
                agg.finished_at,
                agg.duration_ms,
                agg.config_path,
                agg.parallel_mode,
                agg.total_exports as i64,
                agg.success_count as i64,
                agg.failed_count as i64,
                agg.skipped_count as i64,
                agg.total_rows,
                agg.total_files,
                agg.total_bytes as i64,
                details,
            ],
        )?;
        Ok(())
    }

    /// Most-recent aggregates first.
    #[allow(dead_code)] // surfaced via `rivet metrics runs` in a follow-up
    pub fn get_recent_run_aggregates(&self, limit: usize) -> Result<Vec<RunAggregate>> {
        let mut stmt = self.conn.prepare(
            "SELECT run_aggregate_id, started_at, finished_at, duration_ms,
                    config_path, parallel_mode,
                    total_exports, success_count, failed_count, skipped_count,
                    total_rows, total_files, total_bytes, details_json
             FROM run_aggregate
             ORDER BY finished_at DESC
             LIMIT ?1",
        )?;
        let rows = stmt.query_map([limit as i64], |row| {
            let details_json: String = row.get(13)?;
            let per_export: Vec<RunAggregateEntry> =
                serde_json::from_str(&details_json).unwrap_or_default();
            Ok(RunAggregate {
                run_aggregate_id: row.get(0)?,
                started_at: row.get(1)?,
                finished_at: row.get(2)?,
                duration_ms: row.get(3)?,
                config_path: row.get(4)?,
                parallel_mode: row.get(5)?,
                total_exports: row.get::<_, i64>(6)? as usize,
                success_count: row.get::<_, i64>(7)? as usize,
                failed_count: row.get::<_, i64>(8)? as usize,
                skipped_count: row.get::<_, i64>(9)? as usize,
                total_rows: row.get(10)?,
                total_files: row.get(11)?,
                total_bytes: row.get::<_, i64>(12)? as u64,
                per_export,
            })
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(id: &str) -> RunAggregate {
        RunAggregate {
            run_aggregate_id: id.into(),
            started_at: "2026-04-27T10:00:00Z".into(),
            finished_at: "2026-04-27T10:11:30Z".into(),
            duration_ms: 690_000,
            config_path: Some("pilot.yaml".into()),
            parallel_mode: "sequential".into(),
            total_exports: 2,
            success_count: 1,
            failed_count: 1,
            skipped_count: 0,
            total_rows: 1_500_000,
            total_files: 12,
            total_bytes: 750 * 1024 * 1024,
            per_export: vec![
                RunAggregateEntry {
                    export_name: "orders".into(),
                    status: "success".into(),
                    run_id: "orders_20260427T100000".into(),
                    rows: 1_000_000,
                    files: 10,
                    bytes: 600 * 1024 * 1024,
                    duration_ms: 600_000,
                    mode: "chunked".into(),
                    error_message: None,
                },
                RunAggregateEntry {
                    export_name: "users".into(),
                    status: "failed".into(),
                    run_id: "users_20260427T100000".into(),
                    rows: 500_000,
                    files: 2,
                    bytes: 150 * 1024 * 1024,
                    duration_ms: 80_000,
                    mode: "full".into(),
                    error_message: Some("connection reset".into()),
                },
            ],
        }
    }

    #[test]
    fn record_and_query_round_trip() {
        let s = StateStore::open_in_memory().unwrap();
        s.record_run_aggregate(&sample("agg_001")).unwrap();
        s.record_run_aggregate(&sample("agg_002")).unwrap();

        let rows = s.get_recent_run_aggregates(10).unwrap();
        assert_eq!(rows.len(), 2);
        // ORDER BY finished_at DESC — both share the same finished_at, so we
        // only assert the set rather than the order.
        let ids: Vec<_> = rows.iter().map(|r| r.run_aggregate_id.as_str()).collect();
        assert!(ids.contains(&"agg_001"));
        assert!(ids.contains(&"agg_002"));

        let r = rows
            .iter()
            .find(|r| r.run_aggregate_id == "agg_001")
            .unwrap();
        assert_eq!(r.total_exports, 2);
        assert_eq!(r.success_count, 1);
        assert_eq!(r.failed_count, 1);
        assert_eq!(r.total_rows, 1_500_000);
        assert_eq!(r.per_export.len(), 2);
        assert_eq!(r.per_export[0].export_name, "orders");
        assert_eq!(
            r.per_export[1].error_message.as_deref(),
            Some("connection reset")
        );
    }

    #[test]
    fn limit_is_respected() {
        let s = StateStore::open_in_memory().unwrap();
        for i in 0..5 {
            let mut a = sample(&format!("agg_{i:03}"));
            a.finished_at = format!("2026-04-27T10:{:02}:00Z", i);
            s.record_run_aggregate(&a).unwrap();
        }
        let rows = s.get_recent_run_aggregates(3).unwrap();
        assert_eq!(rows.len(), 3);
        // Most-recent first: 004, 003, 002.
        assert_eq!(rows[0].run_aggregate_id, "agg_004");
        assert_eq!(rows[1].run_aggregate_id, "agg_003");
        assert_eq!(rows[2].run_aggregate_id, "agg_002");
    }

    #[test]
    fn empty_per_export_is_allowed() {
        let s = StateStore::open_in_memory().unwrap();
        let mut a = sample("agg_empty");
        a.per_export.clear();
        a.total_exports = 0;
        a.success_count = 0;
        a.failed_count = 0;
        s.record_run_aggregate(&a).unwrap();

        let rows = s.get_recent_run_aggregates(10).unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].per_export.is_empty());
    }
}
