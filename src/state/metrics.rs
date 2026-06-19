use crate::error::Result;

use super::{StateConn, StateStore, pg_sql};

/// One row from `export_metrics`.
#[derive(Debug)]
#[allow(dead_code)]
pub struct ExportMetric {
    pub export_name: String,
    pub run_id: Option<String>,
    pub run_at: String,
    pub duration_ms: i64,
    pub total_rows: i64,
    pub peak_rss_mb: Option<i64>,
    pub status: String,
    pub error_message: Option<String>,
    pub tuning_profile: Option<String>,
    pub format: Option<String>,
    pub mode: Option<String>,
    pub files_produced: i64,
    pub bytes_written: i64,
    pub retries: i64,
    pub validated: Option<bool>,
    pub schema_changed: Option<bool>,
}

/// Every column written to one `export_metrics` row.
///
/// Bundles the original 15 metrics with the v9 additions (source harm,
/// completeness, memory, config dimensions) so a new metric is a struct field +
/// a column, not another positional argument to a 30-arg function. `Default`
/// lets a call site fill only the signals it actually has; the run path builds
/// the whole thing via `pipeline::job::build_metric_row`.
#[derive(Debug, Default, Clone)]
pub struct MetricRow {
    pub export_name: String,
    pub run_id: String,
    pub duration_ms: i64,
    pub total_rows: i64,
    pub peak_rss_mb: Option<i64>,
    pub status: String,
    pub error_message: Option<String>,
    pub tuning_profile: Option<String>,
    pub format: Option<String>,
    pub mode: Option<String>,
    pub files_produced: i64,
    pub bytes_written: i64,
    pub retries: i64,
    pub validated: Option<bool>,
    pub schema_changed: Option<bool>,
    // ── v9: post-pilot analysis signals ──
    pub files_committed: i64,
    pub reconciled: Option<bool>,
    pub source_count: Option<i64>,
    pub quality_passed: Option<bool>,
    pub pg_temp_bytes_delta: Option<i64>,
    pub batch_size: i64,
    pub batch_size_memory_mb: Option<i64>,
    pub skip_reason: Option<String>,
    pub schema_fingerprint: Option<String>,
    pub chunk_size: Option<i64>,
    pub parallel: Option<i64>,
    pub source_type: Option<String>,
    pub destination_type: Option<String>,
    pub rivet_version: Option<String>,
    // ── v10: timing ──
    pub longest_chunk_ms: Option<i64>,
}

/// Metrics store — reads and writes `export_metrics`.
///
/// Invariant I4 (Metric After Verdict) governs when `record_metric` is called:
/// only after the terminal run outcome is determined.
impl StateStore {
    /// Back-compat shim: the original 15-field metric. Fills the v9 columns with
    /// defaults (NULL) and delegates to [`record_metric_full`]. The production
    /// run/apply path now builds a full [`MetricRow`]; this shim remains for the
    /// unit + integration tests that only assert the core signals.
    ///
    /// `#[allow(dead_code)]`: the only non-test caller migrated to
    /// `record_metric_full`, and the bin/lib dead-code pass can't see the uses
    /// in `tests/*` (same reason `RunSummary::stub_for_testing` carries it).
    #[allow(clippy::too_many_arguments, dead_code)]
    pub fn record_metric(
        &self,
        export_name: &str,
        run_id: &str,
        duration_ms: i64,
        total_rows: i64,
        peak_rss_mb: Option<i64>,
        status: &str,
        error_message: Option<&str>,
        tuning_profile: Option<&str>,
        format: Option<&str>,
        mode: Option<&str>,
        files_produced: i64,
        bytes_written: i64,
        retries: i64,
        validated: Option<bool>,
        schema_changed: Option<bool>,
    ) -> Result<()> {
        self.record_metric_full(&MetricRow {
            export_name: export_name.to_string(),
            run_id: run_id.to_string(),
            duration_ms,
            total_rows,
            peak_rss_mb,
            status: status.to_string(),
            error_message: error_message.map(str::to_string),
            tuning_profile: tuning_profile.map(str::to_string),
            format: format.map(str::to_string),
            mode: mode.map(str::to_string),
            files_produced,
            bytes_written,
            retries,
            validated,
            schema_changed,
            ..Default::default()
        })
    }

    /// Insert one fully-populated `export_metrics` row (all v1 + v9 columns).
    /// The production run/apply path builds the complete [`MetricRow`]; the
    /// 15-field [`record_metric`] shim covers callers with only the core signals.
    pub fn record_metric_full(&self, m: &MetricRow) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let sql = "INSERT INTO export_metrics (
             export_name, run_id, run_at, duration_ms, total_rows, peak_rss_mb,
             status, error_message, tuning_profile, format, mode,
             files_produced, bytes_written, retries, validated, schema_changed,
             files_committed, reconciled, source_count, quality_passed, pg_temp_bytes_delta,
             batch_size, batch_size_memory_mb, skip_reason, schema_fingerprint,
             chunk_size, parallel, source_type, destination_type, rivet_version,
             longest_chunk_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16,
             ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31)";
        match &self.conn {
            StateConn::Sqlite(c) => {
                c.execute(
                    sql,
                    rusqlite::params![
                        m.export_name,
                        m.run_id,
                        now,
                        m.duration_ms,
                        m.total_rows,
                        m.peak_rss_mb,
                        m.status,
                        m.error_message,
                        m.tuning_profile,
                        m.format,
                        m.mode,
                        m.files_produced,
                        m.bytes_written,
                        m.retries,
                        m.validated,
                        m.schema_changed,
                        m.files_committed,
                        m.reconciled,
                        m.source_count,
                        m.quality_passed,
                        m.pg_temp_bytes_delta,
                        m.batch_size,
                        m.batch_size_memory_mb,
                        m.skip_reason,
                        m.schema_fingerprint,
                        m.chunk_size,
                        m.parallel,
                        m.source_type,
                        m.destination_type,
                        m.rivet_version,
                        m.longest_chunk_ms
                    ],
                )?;
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                c.execute(
                    &pg_sql(sql),
                    &[
                        &m.export_name,
                        &m.run_id,
                        &now,
                        &m.duration_ms,
                        &m.total_rows,
                        &m.peak_rss_mb,
                        &m.status,
                        &m.error_message,
                        &m.tuning_profile,
                        &m.format,
                        &m.mode,
                        &m.files_produced,
                        &m.bytes_written,
                        &m.retries,
                        &m.validated,
                        &m.schema_changed,
                        &m.files_committed,
                        &m.reconciled,
                        &m.source_count,
                        &m.quality_passed,
                        &m.pg_temp_bytes_delta,
                        &m.batch_size,
                        &m.batch_size_memory_mb,
                        &m.skip_reason,
                        &m.schema_fingerprint,
                        &m.chunk_size,
                        &m.parallel,
                        &m.source_type,
                        &m.destination_type,
                        &m.rivet_version,
                        &m.longest_chunk_ms,
                    ],
                )?;
            }
        }
        Ok(())
    }

    /// Test-only raw scalar read of a v9 metric column the typed `get_metrics`
    /// path intentionally doesn't surface — lets tests pin that the wide INSERT
    /// mapped each field to the right column (catches a positional param swap).
    #[cfg(test)]
    pub(crate) fn metric_scalar_i64(&self, run_id: &str, column: &str) -> Option<i64> {
        match &self.conn {
            StateConn::Sqlite(c) => c
                .query_row(
                    &format!("SELECT {column} FROM export_metrics WHERE run_id = ?1"),
                    [run_id],
                    |r| r.get::<_, Option<i64>>(0),
                )
                .ok()
                .flatten(),
            _ => None,
        }
    }

    pub fn get_metrics(
        &self,
        export_name: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ExportMetric>> {
        let cols = "export_name, run_id, run_at, duration_ms, total_rows, peak_rss_mb,
                    status, error_message, tuning_profile, format, mode,
                    files_produced, bytes_written, retries, validated, schema_changed";

        let limit_i64 = limit as i64;
        match &self.conn {
            StateConn::Sqlite(c) => {
                let (sql, params): (&str, Vec<Box<dyn rusqlite::types::ToSql>>) = if let Some(
                    name,
                ) = export_name
                {
                    (
                        "SELECT export_name, run_id, run_at, duration_ms, total_rows, peak_rss_mb, \
                             status, error_message, tuning_profile, format, mode, \
                             files_produced, bytes_written, retries, validated, schema_changed \
                             FROM export_metrics WHERE export_name = ?1 ORDER BY id DESC LIMIT ?2",
                        vec![Box::new(name.to_string()), Box::new(limit_i64)],
                    )
                } else {
                    (
                        "SELECT export_name, run_id, run_at, duration_ms, total_rows, peak_rss_mb, \
                             status, error_message, tuning_profile, format, mode, \
                             files_produced, bytes_written, retries, validated, schema_changed \
                             FROM export_metrics ORDER BY id DESC LIMIT ?1",
                        vec![Box::new(limit_i64)],
                    )
                };
                let mut stmt = c.prepare(sql)?;
                let params_refs: Vec<&dyn rusqlite::types::ToSql> =
                    params.iter().map(|p| p.as_ref()).collect();
                let rows = stmt.query_map(params_refs.as_slice(), |row| {
                    Ok(ExportMetric {
                        export_name: row.get(0)?,
                        run_id: row.get(1)?,
                        run_at: row.get(2)?,
                        duration_ms: row.get(3)?,
                        total_rows: row.get(4)?,
                        peak_rss_mb: row.get(5)?,
                        status: row.get(6)?,
                        error_message: row.get(7)?,
                        tuning_profile: row.get(8)?,
                        format: row.get(9)?,
                        mode: row.get(10)?,
                        files_produced: row.get::<_, Option<i64>>(11)?.unwrap_or(0),
                        bytes_written: row.get::<_, Option<i64>>(12)?.unwrap_or(0),
                        retries: row.get::<_, Option<i64>>(13)?.unwrap_or(0),
                        validated: row.get(14)?,
                        schema_changed: row.get(15)?,
                    })
                })?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(Into::into)
            }
            StateConn::Postgres(client) => {
                // Single borrow for the duration of this call; safe because all Postgres
                // operations in StateStore are sequential (no re-entrant borrows).
                let mut c = client.borrow_mut();
                let rows = if let Some(name) = export_name {
                    c.query(
                        &format!("SELECT {} FROM export_metrics WHERE export_name = $1 ORDER BY id DESC LIMIT $2", cols),
                        &[&name, &limit_i64],
                    )?
                } else {
                    c.query(
                        &format!(
                            "SELECT {} FROM export_metrics ORDER BY id DESC LIMIT $1",
                            cols
                        ),
                        &[&limit_i64],
                    )?
                };
                Ok(rows
                    .iter()
                    .map(|row| ExportMetric {
                        export_name: row.get(0),
                        run_id: row.get(1),
                        run_at: row.get(2),
                        duration_ms: row.get(3),
                        total_rows: row.get(4),
                        peak_rss_mb: row.get(5),
                        status: row.get(6),
                        error_message: row.get(7),
                        tuning_profile: row.get(8),
                        format: row.get(9),
                        mode: row.get(10),
                        files_produced: row.get::<_, Option<i64>>(11).unwrap_or(0),
                        bytes_written: row.get::<_, Option<i64>>(12).unwrap_or(0),
                        retries: row.get::<_, Option<i64>>(13).unwrap_or(0),
                        validated: row.get(14),
                        schema_changed: row.get(15),
                    })
                    .collect())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> StateStore {
        StateStore::open_in_memory().expect("in-memory store")
    }

    #[test]
    fn record_and_query_metrics() {
        let s = store();
        s.record_metric(
            "orders",
            "run_001",
            1200,
            50000,
            Some(142),
            "success",
            None,
            Some("safe"),
            Some("parquet"),
            Some("full"),
            1,
            4096,
            0,
            Some(true),
            Some(false),
        )
        .unwrap();
        s.record_metric(
            "orders",
            "run_002",
            300,
            0,
            Some(30),
            "failed",
            Some("timeout"),
            Some("safe"),
            Some("parquet"),
            Some("full"),
            0,
            0,
            2,
            None,
            None,
        )
        .unwrap();

        let metrics = s.get_metrics(Some("orders"), 10).unwrap();
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0].status, "failed");
        assert_eq!(metrics[0].run_id.as_deref(), Some("run_002"));
        assert_eq!(metrics[0].retries, 2);
        assert_eq!(metrics[1].total_rows, 50000);
        assert_eq!(metrics[1].run_id.as_deref(), Some("run_001"));
        assert_eq!(metrics[1].files_produced, 1);
        assert_eq!(metrics[1].bytes_written, 4096);
        assert_eq!(metrics[1].validated, Some(true));
        assert_eq!(metrics[1].schema_changed, Some(false));
    }

    #[test]
    fn record_metric_full_persists_v9_columns_in_order() {
        let s = store();
        s.record_metric_full(&MetricRow {
            export_name: "orders".into(),
            run_id: "r1".into(),
            duration_ms: 1200,
            total_rows: 50_000,
            status: "success".into(),
            // v9 signals, chosen as distinct values so a positional param swap
            // in the 30-column INSERT shows up as a wrong-column read below.
            files_committed: 11,
            source_count: Some(50_000),
            pg_temp_bytes_delta: Some(1_048_576),
            batch_size: 32_000,
            chunk_size: Some(100_000),
            parallel: Some(4),
            longest_chunk_ms: Some(1_839),
            ..Default::default()
        })
        .unwrap();

        // Core read path is unchanged.
        let got = s.get_metrics(Some("orders"), 1).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].total_rows, 50_000);
        assert_eq!(got[0].run_id.as_deref(), Some("r1"));

        // v9 columns round-trip to the right column (chunk_size=100000 vs
        // parallel=4 pinned separately so a swap of the two Option<i64> params
        // can't pass).
        assert_eq!(s.metric_scalar_i64("r1", "files_committed"), Some(11));
        assert_eq!(s.metric_scalar_i64("r1", "source_count"), Some(50_000));
        assert_eq!(
            s.metric_scalar_i64("r1", "pg_temp_bytes_delta"),
            Some(1_048_576)
        );
        assert_eq!(s.metric_scalar_i64("r1", "batch_size"), Some(32_000));
        assert_eq!(s.metric_scalar_i64("r1", "chunk_size"), Some(100_000));
        assert_eq!(s.metric_scalar_i64("r1", "parallel"), Some(4));
        assert_eq!(s.metric_scalar_i64("r1", "longest_chunk_ms"), Some(1_839));
    }

    #[test]
    fn query_metrics_all_exports() {
        let s = store();
        s.record_metric(
            "orders", "r1", 100, 1000, None, "success", None, None, None, None, 1, 500, 0, None,
            None,
        )
        .unwrap();
        s.record_metric(
            "users", "r2", 200, 2000, None, "success", None, None, None, None, 1, 800, 0, None,
            None,
        )
        .unwrap();

        let metrics = s.get_metrics(None, 10).unwrap();
        assert_eq!(metrics.len(), 2);
    }

    #[test]
    fn metrics_limit_works() {
        let s = store();
        for i in 0..10 {
            s.record_metric(
                "t",
                &format!("r{}", i),
                i * 100,
                i,
                None,
                "success",
                None,
                None,
                None,
                None,
                0,
                0,
                0,
                None,
                None,
            )
            .unwrap();
        }
        let metrics = s.get_metrics(Some("t"), 3).unwrap();
        assert_eq!(metrics.len(), 3);
    }
}
