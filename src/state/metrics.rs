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
    // ── v12: chunking diagnostics — which key was chunked (the resolved strategy
    // is already the `mode` column; whether the key is a PK is a follow-up that
    // needs a run-time probe, so it is not derived-from-strategy here) ──
    pub chunk_key: Option<String>,
    // ── v18: failure forensics — populated so a `status='failed'` row reconstructs
    // the failure without the source DB (export_schema is success-only). See
    // `pipeline::job::build_metric_row` for the per-field write points. ──
    /// Stable error CLASS (`keyset_unreadable_key` | `statement_timeout` |
    /// `parallel_checkpoint` | `schema_drift` | …) classified from `error_message`,
    /// so failures group/trend without `LIKE '%…%'`.
    pub error_class: Option<String>,
    /// The key RANGE the run covered/died in (from the summary cursor bounds).
    pub cursor_min: Option<String>,
    pub cursor_max: Option<String>,
    /// The keyset/chunk key's SHAPE as JSON (`{key, db_type, signed, is_primary_key,
    /// …}`) — the answer to "was the key indexed / unsigned" without the source.
    pub key_descriptor_json: Option<String>,
    /// The raw value that could not be read/advanced (e.g. a u64 past i64::MAX) —
    /// turns "unsupported type" into a concrete number.
    pub offending_value: Option<String>,
    /// Source SERVER context as JSON (`{version, max_execution_time, sql_mode,
    /// time_zone, …}`) — the ERROR 3024 timeout is unexplainable without the limit.
    pub server_context_json: Option<String>,
}

/// Metrics store — reads and writes `export_metrics`.
///
/// Invariant I4 (Metric After Verdict) governs when `record_metric` is called:
/// only after the terminal run outcome is determined.
/// What a run has made durable so far — the payload of an in-flight
/// [`StateStore::record_metric_progress`].
///
/// A struct rather than three positional `i64`s: `(rows, files, bytes)` at a call
/// site is three numbers of the same type in an order nothing checks.
#[derive(Debug, Clone, Copy, Default)]
pub struct RunProgress {
    pub total_rows: i64,
    pub files: i64,
    pub bytes: i64,
}

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

    /// The `running` aggregate for a run in flight — written at the same durable
    /// seam as the parts, so the ledger describes a run WHILE it happens.
    ///
    /// `export_metrics` used to appear only at finalize, which meant a run that
    /// died mid-stream had its parts in `file_log` (once the CDC sink started
    /// recording them) and no aggregate anywhere: the database knew the files
    /// existed and could not say what run they amounted to. This closes that —
    /// the row exists from the first durable roll and carries the totals so far.
    ///
    /// ONE row per run, always: this UPDATEs the run's `running` row and inserts
    /// only when there is none, and [`record_metric_full`] clears it before
    /// writing the terminal row. Anything that sums `total_rows` across runs
    /// would otherwise double-count a run for every roll it survived — a far
    /// worse failure than the gap being closed.
    /// Drop a run's in-flight `running` aggregate. Only ever called by
    /// [`record_metric_full`], immediately before the terminal row replaces it.
    fn clear_running_metric(&self, run_id: &str) -> Result<()> {
        let sql = "DELETE FROM export_metrics WHERE run_id = ?1 AND status = 'running'";
        match &self.conn {
            StateConn::Sqlite(c) => {
                c.execute(sql, rusqlite::params![run_id])?;
            }
            StateConn::Postgres(client) => {
                client.borrow_mut().execute(&pg_sql(sql), &[&run_id])?;
            }
        }
        Ok(())
    }

    pub fn record_metric_progress(
        &self,
        export_name: &str,
        run_id: &str,
        mode: &str,
        format: &str,
        p: RunProgress,
    ) -> Result<()> {
        let RunProgress {
            total_rows,
            files,
            bytes,
        } = p;
        let now = chrono::Utc::now().to_rfc3339();
        let upd = "UPDATE export_metrics SET total_rows = ?1, files_produced = ?2, \
                   files_committed = ?3, bytes_written = ?4, run_at = ?5 \
                   WHERE run_id = ?6 AND status = 'running'";
        let ins = "INSERT INTO export_metrics (export_name, run_id, run_at, duration_ms, \
                   total_rows, status, mode, format, files_produced, files_committed, \
                   bytes_written, retries) \
                   VALUES (?1, ?2, ?3, 0, ?4, 'running', ?5, ?6, ?7, ?8, ?9, 0)";
        match &self.conn {
            StateConn::Sqlite(c) => {
                let n = c.execute(
                    upd,
                    rusqlite::params![total_rows, files, files, bytes, now, run_id],
                )?;
                if n == 0 {
                    c.execute(
                        ins,
                        rusqlite::params![
                            export_name,
                            run_id,
                            now,
                            total_rows,
                            mode,
                            format,
                            files,
                            files,
                            bytes
                        ],
                    )?;
                }
            }
            StateConn::Postgres(client) => {
                let mut c = client.borrow_mut();
                let n = c.execute(
                    &pg_sql(upd),
                    &[&total_rows, &files, &files, &bytes, &now, &run_id],
                )?;
                if n == 0 {
                    c.execute(
                        &pg_sql(ins),
                        &[
                            &export_name,
                            &run_id,
                            &now,
                            &total_rows,
                            &mode,
                            &format,
                            &files,
                            &files,
                            &bytes,
                        ],
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Insert one fully-populated `export_metrics` row (all v1 + v9 columns).
    /// The production run/apply path builds the complete [`MetricRow`]; the
    /// 15-field [`record_metric`] shim covers callers with only the core signals.
    ///
    /// Clears this run's in-flight `running` row first, so a run leaves exactly
    /// ONE row at rest — see [`record_metric_progress`] for why that matters more
    /// than keeping the progress history.
    pub fn record_metric_full(&self, m: &MetricRow) -> Result<()> {
        self.clear_running_metric(&m.run_id)?;
        let now = chrono::Utc::now().to_rfc3339();
        let sql = "INSERT INTO export_metrics (
             export_name, run_id, run_at, duration_ms, total_rows, peak_rss_mb,
             status, error_message, tuning_profile, format, mode,
             files_produced, bytes_written, retries, validated, schema_changed,
             files_committed, reconciled, source_count, quality_passed, pg_temp_bytes_delta,
             batch_size, batch_size_memory_mb, skip_reason, schema_fingerprint,
             chunk_size, parallel, source_type, destination_type, rivet_version,
             longest_chunk_ms, chunk_key,
             error_class, cursor_min, cursor_max, key_descriptor_json, offending_value,
             server_context_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16,
             ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31,
             ?32, ?33, ?34, ?35, ?36, ?37, ?38)";
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
                        m.longest_chunk_ms,
                        m.chunk_key,
                        m.error_class,
                        m.cursor_min,
                        m.cursor_max,
                        m.key_descriptor_json,
                        m.offending_value,
                        m.server_context_json
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
                        &m.chunk_key,
                        &m.error_class,
                        &m.cursor_min,
                        &m.cursor_max,
                        &m.key_descriptor_json,
                        &m.offending_value,
                        &m.server_context_json,
                    ],
                )?;
            }
        }
        Ok(())
    }

    /// Record per-run source-harm deltas (Tier 2): one row per counter into
    /// `export_harm`, keyed on `run_id`. Best-effort observability — the caller
    /// logs and ignores any error; a missing harm row never affects the run
    /// verdict. No-op for an empty delta set (e.g. a non-Postgres source whose
    /// probe was skipped, or a counter set that didn't move).
    pub fn record_harm(
        &self,
        run_id: &str,
        export_name: &str,
        deltas: &[(String, i64)],
    ) -> Result<()> {
        if deltas.is_empty() {
            return Ok(());
        }
        let now = chrono::Utc::now().to_rfc3339();
        let sql = "INSERT INTO export_harm (run_id, export_name, metric, delta, recorded_at) \
                   VALUES (?1, ?2, ?3, ?4, ?5)";
        for (metric, delta) in deltas {
            self.execute(
                sql,
                &[
                    run_id.into(),
                    export_name.into(),
                    metric.as_str().into(),
                    (*delta).into(),
                    now.as_str().into(),
                ],
            )?;
        }
        Ok(())
    }

    /// Test-only read of the `export_harm` rows for a run, `(metric, delta)`
    /// sorted by metric — lets tests trace the harm signal through the table.
    #[cfg(test)]
    pub(crate) fn harm_rows_for_test(&self, run_id: &str) -> Vec<(String, i64)> {
        match &self.conn {
            StateConn::Sqlite(c) => {
                let mut stmt = c
                    .prepare(
                        "SELECT metric, delta FROM export_harm WHERE run_id = ?1 ORDER BY metric",
                    )
                    .expect("prepare export_harm read");
                let rows = stmt
                    .query_map([run_id], |r| {
                        Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
                    })
                    .expect("query export_harm");
                rows.filter_map(|r| r.ok()).collect()
            }
            _ => Vec::new(),
        }
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
        // One projection, written once for both backends (was duplicated per arm).
        let extract = |r: &dyn super::row::StateRow| ExportMetric {
            export_name: r.text(0),
            run_id: r.opt_text(1),
            run_at: r.text(2),
            duration_ms: r.i64(3),
            total_rows: r.i64(4),
            peak_rss_mb: r.opt_i64(5),
            status: r.text(6),
            error_message: r.opt_text(7),
            tuning_profile: r.opt_text(8),
            format: r.opt_text(9),
            mode: r.opt_text(10),
            files_produced: r.opt_i64(11).unwrap_or(0),
            bytes_written: r.opt_i64(12).unwrap_or(0),
            retries: r.opt_i64(13).unwrap_or(0),
            validated: r.opt_bool(14),
            schema_changed: r.opt_bool(15),
        };
        let cols = "export_name, run_id, run_at, duration_ms, total_rows, peak_rss_mb, \
                    status, error_message, tuning_profile, format, mode, \
                    files_produced, bytes_written, retries, validated, schema_changed";
        match export_name {
            Some(name) => self.query(
                &format!(
                    "SELECT {cols} FROM export_metrics WHERE export_name = ?1 ORDER BY id DESC LIMIT ?2"
                ),
                &[name.into(), (limit as i64).into()],
                extract,
            ),
            None => self.query(
                &format!("SELECT {cols} FROM export_metrics ORDER BY id DESC LIMIT ?1"),
                &[(limit as i64).into()],
                extract,
            ),
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
    fn record_harm_round_trips_per_counter_rows() {
        // Traces the Tier 2 signal through SQLite: v11 migration creates
        // export_harm, record_harm writes one row per counter, the read returns
        // them. (The live source-probe that produces these deltas needs a real
        // DB; this validates the storage path it lands in.)
        let s = store();
        let deltas = vec![
            ("pg_tup_returned".to_string(), 1_000_000),
            ("pg_blks_read".to_string(), 2_048),
            ("pg_temp_files".to_string(), 3),
        ];
        s.record_harm("run-h", "content_items", &deltas).unwrap();

        // One row per counter, keyed on run_id (read sorted by metric).
        assert_eq!(
            s.harm_rows_for_test("run-h"),
            vec![
                ("pg_blks_read".to_string(), 2_048),
                ("pg_temp_files".to_string(), 3),
                ("pg_tup_returned".to_string(), 1_000_000),
            ]
        );

        // Empty delta set (probe skipped / counters unmoved) → no rows, no error.
        s.record_harm("run-empty", "x", &[]).unwrap();
        assert!(s.harm_rows_for_test("run-empty").is_empty());
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
