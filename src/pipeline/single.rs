//! **Layer: Execution** (with bounded persistence writes)
//!
//! Runs a single-query export for Snapshot, Incremental, and TimeWindow strategies.
//! After the write completes, this module performs three bounded persistence writes
//! that are invariant-ordered (see ADR-0001): manifest entry, cursor advance,
//! schema snapshot.  These are post-execution state updates, not runtime decisions.

use std::time::Duration;

use super::RunSummary;
use super::chunked::{run_chunked_sequential, run_chunked_sequential_checkpoint};
use super::journal::RunEvent;
use super::retry::classify_error;
use super::sink::{CompletedPart, ExportSink};
use super::validate::validate_output;
use crate::config::SchemaDriftPolicy;
use crate::error::Result;
use crate::plan::{ExtractionStrategy, ResolvedRunPlan};
use crate::source::{self, Source};
use crate::state::StateStore;
use crate::{destination, format};

pub(crate) fn run_with_reconnect(
    state: &StateStore,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    config_path: &str,
) -> Result<()> {
    let mut last_err: Option<anyhow::Error> = None;

    for attempt in 0..=plan.tuning.max_retries {
        if attempt > 0 {
            summary.retries = attempt;
            let (_, needs_reconnect, extra_delay) = last_err
                .as_ref()
                .map(classify_error)
                .unwrap_or((false, false, 0));
            let backoff = plan.tuning.retry_backoff_ms * 2u64.pow(attempt - 1) + extra_delay;
            log::warn!(
                "export '{}': retry {}/{} in {}ms{}({})",
                plan.export_name,
                attempt,
                plan.tuning.max_retries,
                backoff,
                if needs_reconnect {
                    " [reconnecting] "
                } else {
                    " "
                },
                last_err
                    .as_ref()
                    .map(|e: &anyhow::Error| format!("{:#}", e))
                    .unwrap_or_default(),
            );
            summary.journal.record(RunEvent::RetryAttempted {
                attempt,
                reason: last_err
                    .as_ref()
                    .map(|e| format!("{:#}", e))
                    .unwrap_or_default(),
                backoff_ms: backoff,
            });
            std::thread::sleep(Duration::from_millis(backoff));
        }

        let mut src = match source::create_source(&plan.source) {
            Ok(s) => s,
            Err(e) => {
                let (transient, _, _) = classify_error(&e);
                if attempt < plan.tuning.max_retries && transient {
                    log::warn!(
                        "export '{}': connection failed, will retry: {:#}",
                        plan.export_name,
                        e
                    );
                    last_err = Some(e);
                    continue;
                }
                return Err(e);
            }
        };

        match run_export(&mut *src, state, plan, summary, config_path) {
            Ok(()) => return Ok(()),
            Err(e) => {
                let (transient, _, _) = classify_error(&e);
                if attempt < plan.tuning.max_retries && transient {
                    // Guard: if a file was already committed to the destination, retrying
                    // from the same cursor would re-read the same rows and produce duplicates.
                    if summary.files_committed > 0 {
                        return Err(anyhow::anyhow!(
                            "export '{}': transient error after {} file(s) written to destination \
                             — cannot safely retry (would duplicate rows). \
                             Run `rivet reconcile` to verify state. Original error: {:#}",
                            plan.export_name,
                            summary.files_committed,
                            e
                        ));
                    }
                    last_err = Some(e);
                    continue;
                }
                return Err(e);
            }
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("export failed after retries")))
}

pub(crate) fn run_export(
    src: &mut dyn Source,
    state: &StateStore,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    config_path: &str,
) -> Result<()> {
    // Chunked strategies own their own execution path.
    if matches!(plan.strategy, ExtractionStrategy::Chunked(_)) {
        if plan.strategy.is_resumable() {
            return run_chunked_sequential_checkpoint(
                src,
                state,
                plan,
                summary,
                config_path,
                super::chunked::ChunkSource::Detect,
            );
        } else {
            return run_chunked_sequential(
                src,
                plan,
                summary,
                Some(state),
                super::chunked::ChunkSource::Detect,
            );
        }
    }

    // All non-chunked strategies: ask the strategy for its query and cursor needs.
    let cursor_state = if plan.strategy.needs_cursor_state() {
        Some(state.get(&plan.export_name)?)
    } else {
        None
    };
    let query = plan
        .strategy
        .resolve_query(&plan.base_query, plan.source.source_type)
        .expect("non-chunked strategy must return a resolved query");

    run_single_export(
        src,
        &query,
        cursor_state.as_ref(),
        plan,
        Some(state),
        summary,
    )
}

pub(super) fn run_single_export(
    src: &mut dyn Source,
    query: &str,
    cursor: Option<&crate::types::CursorState>,
    plan: &ResolvedRunPlan,
    state: Option<&StateStore>,
    summary: &mut RunSummary,
) -> Result<()> {
    let mut sink = ExportSink::new(plan)?;

    src.export(
        query,
        plan.strategy.incremental_plan(),
        cursor,
        &plan.tuning,
        &plan.column_overrides,
        &mut sink,
    )?;

    // Test fault-point #1: after the source stream was fully read but
    // before the writer is finalised.  Exercised by QA backlog Task 1.1.
    crate::test_hook::maybe_panic_at("after_source_read");

    if let Some(w) = sink.writer.take() {
        w.finish()?;
    }

    summary.total_rows += sink.total_rows as i64;
    log::info!(
        "export '{}': {} rows written",
        plan.export_name,
        sink.total_rows
    );

    if sink.total_rows == 0 {
        if plan.skip_empty {
            summary.status = "skipped".into();
            log::info!(
                "export '{}': skipped (0 rows, skip_empty=true)",
                plan.export_name
            );
        } else {
            log::info!("export '{}': no data to export", plan.export_name);
        }
        return Ok(());
    }

    let quality_issues = sink.run_quality_checks();
    if !quality_issues.is_empty() {
        for issue in &quality_issues {
            let level = match issue.severity {
                crate::quality::Severity::Fail => "FAIL",
                crate::quality::Severity::Warn => "WARN",
            };
            log::warn!("quality {}: {}", level, issue.message);
            summary.journal.record(RunEvent::QualityIssue {
                severity: level.to_string(),
                message: issue.message.clone(),
            });
        }
        if quality_issues
            .iter()
            .any(|i| i.severity == crate::quality::Severity::Fail)
        {
            summary.quality_passed = Some(false);
            anyhow::bail!("export '{}': quality checks failed", plan.export_name);
        }
    }
    if plan.quality.is_some() {
        summary.quality_passed = Some(true);
    }

    if sink.part_rows > 0 {
        sink.completed_parts.push(CompletedPart {
            tmp: std::mem::replace(&mut sink.tmp, tempfile::NamedTempFile::new()?),
            rows: sink.part_rows,
        });
    }

    let fmt = format::create_format(plan.format, plan.compression, plan.compression_level);
    let ext = fmt.file_extension();
    let dest = destination::create_destination(&plan.destination)?;

    // ADR-0004: log backend capabilities; warn when non-retry-safe destination is configured with retries.
    destination::log_capabilities(&plan.export_name, dest.as_ref(), plan.tuning.max_retries);

    let has_parts = sink.completed_parts.len() > 1;
    let ts = chrono::Utc::now().format("%Y%m%d_%H%M%S");

    for (part_idx, part) in sink.completed_parts.iter().enumerate() {
        if plan.validate {
            validate_output(part.tmp.path(), plan.format, part.rows)?;
            summary.validated = Some(true);
            summary
                .journal
                .record(RunEvent::ValidationResult { passed: true });
        }

        let file_bytes = std::fs::metadata(part.tmp.path())
            .map(|m| m.len())
            .unwrap_or(0);
        summary.bytes_written += file_bytes;
        summary.files_produced += 1;

        let file_name = if has_parts {
            format!("{}_{}_part{}.{}", plan.export_name, ts, part_idx, ext)
        } else {
            format!("{}_{}.{}", plan.export_name, ts, ext)
        };
        dest.write(part.tmp.path(), &file_name)?;
        summary.files_committed += 1;

        // Test fault-point #2: dest.write() just succeeded, but the manifest
        // has NOT been updated yet.  Reproduces the ADR-0001 I2→I3 crash
        // window (file at destination, no record in manifest, cursor not
        // advanced).  QA backlog Task 1.1.
        crate::test_hook::maybe_panic_at("after_file_write");

        // ADR-0001 I2–I4 / ADR-0004: state writes happen only after destination.write()
        // returns Ok(()), which for all current backends is the commit boundary.
        summary.journal.record(RunEvent::FileWritten {
            file_name: file_name.clone(),
            rows: part.rows as i64,
            bytes: file_bytes,
            part_index: part_idx,
        });

        if let Some(st) = state
            && let Err(e) = st.record_file(
                &summary.run_id,
                &plan.export_name,
                &file_name,
                part.rows as i64,
                file_bytes as i64,
                plan.format.label(),
                Some(plan.compression.label()),
            )
        {
            log::warn!(
                "export '{}': manifest write failed for '{}' (file was produced): {:#}",
                plan.export_name,
                file_name,
                e
            );
        }

        // Test fault-point #3: manifest has just been recorded, but the
        // cursor has NOT been advanced yet.  QA backlog Task 1.1.
        crate::test_hook::maybe_panic_at("after_manifest_update");
    }

    if let (Some(last_val), Some(st)) = (&sink.last_cursor_value, state) {
        st.update(&plan.export_name, last_val)?;

        // Test fault-point #4: cursor advanced, but the final run metric
        // (record_metric at the outer pipeline loop) has NOT been recorded.
        // QA backlog Task 1.1.
        crate::test_hook::maybe_panic_at("after_cursor_commit");

        log::info!(
            "export '{}': cursor updated to '{}'",
            plan.export_name,
            last_val
        );
        // Epic G: record committed boundary for progression reporting.
        if let Err(e) =
            st.record_committed_incremental(&plan.export_name, last_val, &summary.run_id)
        {
            log::warn!(
                "export '{}': committed boundary update failed: {:#}",
                plan.export_name,
                e
            );
        }
    }

    if let (Some(schema), Some(st)) = (&sink.dest_schema, state) {
        let columns: Vec<crate::state::SchemaColumn> = schema
            .fields()
            .iter()
            .map(|f| crate::state::SchemaColumn {
                name: f.name().clone(),
                data_type: format!("{:?}", f.data_type()),
            })
            .collect();

        match st.detect_schema_change(&plan.export_name, &columns) {
            Ok(Some(change)) => {
                summary.schema_changed = Some(true);
                summary.journal.record(RunEvent::SchemaChanged {
                    added: change.added.clone(),
                    removed: change.removed.clone(),
                    type_changed: change.type_changed.clone(),
                });

                match plan.schema_drift_policy {
                    SchemaDriftPolicy::Continue => {
                        if let Err(e) = st.store_schema(&plan.export_name, &columns) {
                            log::warn!(
                                "export '{}': schema store update failed: {:#}",
                                plan.export_name,
                                e
                            );
                        }
                    }
                    SchemaDriftPolicy::Warn => {
                        log::warn!("export '{}': schema changed!", plan.export_name);
                        if !change.added.is_empty() {
                            log::warn!("  added: {}", change.added.join(", "));
                        }
                        if !change.removed.is_empty() {
                            log::warn!("  removed: {}", change.removed.join(", "));
                        }
                        for (col, old, new) in &change.type_changed {
                            log::warn!("  type changed: {} ({} → {})", col, old, new);
                        }
                        if let Err(e) = st.store_schema(&plan.export_name, &columns) {
                            log::warn!(
                                "export '{}': schema store update failed: {:#}",
                                plan.export_name,
                                e
                            );
                        }
                    }
                    SchemaDriftPolicy::Fail => {
                        log::error!(
                            "export '{}': schema drift detected — aborting (on_schema_drift: fail)",
                            plan.export_name
                        );
                        if !change.added.is_empty() {
                            log::error!("  added: {}", change.added.join(", "));
                        }
                        if !change.removed.is_empty() {
                            log::error!("  removed: {}", change.removed.join(", "));
                        }
                        for (col, old, new) in &change.type_changed {
                            log::error!("  type changed: {} ({} → {})", col, old, new);
                        }
                        return Err(anyhow::anyhow!(
                            "schema drift detected for export '{}': \
                             {} column(s) added, {} removed, {} retyped — \
                             set `on_schema_drift: warn` to accept, or fix the schema mismatch",
                            plan.export_name,
                            change.added.len(),
                            change.removed.len(),
                            change.type_changed.len()
                        ));
                    }
                }
            }
            Ok(None) => {
                summary.schema_changed = Some(false);
            }
            Err(e) => log::warn!("schema tracking error: {:#}", e),
        }
    }

    // Epic 8: data shape drift — warn when string/binary columns grow beyond threshold.
    if plan.shape_drift_warn_factor > 0.0
        && !sink.column_max_bytes.is_empty()
        && let Some(st) = state
    {
        match st.detect_shape_drift(
            &plan.export_name,
            &sink.column_max_bytes,
            plan.shape_drift_warn_factor,
        ) {
            Ok(warnings) => {
                for w in &warnings {
                    log::warn!(
                        "export '{}': shape drift in column '{}' — \
                         max byte length grew {:.1}× ({} → {} bytes); \
                         set `shape_drift_warn_factor` to a higher value to suppress",
                        plan.export_name,
                        w.column,
                        w.growth_factor,
                        w.stored_max_bytes,
                        w.current_max_bytes,
                    );
                    summary.journal.record(RunEvent::Warning {
                        context: format!("shape_drift:{}", w.column),
                        message: format!(
                            "column '{}' max byte length grew {:.1}× ({} → {} bytes)",
                            w.column, w.growth_factor, w.stored_max_bytes, w.current_max_bytes
                        ),
                    });
                }
            }
            Err(e) => log::warn!(
                "export '{}': shape tracking error: {:#}",
                plan.export_name,
                e
            ),
        }
    }

    log::info!("export '{}' completed successfully", plan.export_name);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    use crate::config::{
        CompressionType, DestinationConfig, DestinationType, FormatType, MetaColumns, SourceConfig,
        SourceType,
    };
    use crate::plan::ExtractionStrategy;
    use crate::tuning::SourceTuning;

    // ── mock sources ─────────────────────────────────────────────────────────

    /// Source that emits no schema and no batches → sink sees 0 rows.
    struct EmptySource;

    impl crate::source::Source for EmptySource {
        fn export(
            &mut self,
            _query: &str,
            _incremental: Option<&crate::plan::IncrementalCursorPlan>,
            _cursor: Option<&crate::types::CursorState>,
            _tuning: &SourceTuning,
            _column_overrides: &crate::types::ColumnOverrides,
            _sink: &mut dyn crate::source::BatchSink,
        ) -> crate::error::Result<()> {
            Ok(())
        }
        fn query_scalar(&mut self, _sql: &str) -> crate::error::Result<Option<String>> {
            Ok(None)
        }
        fn type_mappings(
            &mut self,
            _query: &str,
            _overrides: &crate::types::ColumnOverrides,
        ) -> crate::error::Result<Vec<crate::types::TypeMapping>> {
            Ok(vec![])
        }
    }

    /// Source that emits `n` rows with a single `id: Int64` column.
    struct RowSource(usize);

    impl crate::source::Source for RowSource {
        fn export(
            &mut self,
            _query: &str,
            _incremental: Option<&crate::plan::IncrementalCursorPlan>,
            _cursor: Option<&crate::types::CursorState>,
            _tuning: &SourceTuning,
            _column_overrides: &crate::types::ColumnOverrides,
            sink: &mut dyn crate::source::BatchSink,
        ) -> crate::error::Result<()> {
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            sink.on_schema(Arc::clone(&schema))?;
            if self.0 > 0 {
                let ids: Vec<i64> = (0..self.0 as i64).collect();
                let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids))])
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                sink.on_batch(&batch)?;
            }
            Ok(())
        }
        fn query_scalar(&mut self, _sql: &str) -> crate::error::Result<Option<String>> {
            Ok(None)
        }
        fn type_mappings(
            &mut self,
            _query: &str,
            _overrides: &crate::types::ColumnOverrides,
        ) -> crate::error::Result<Vec<crate::types::TypeMapping>> {
            Ok(vec![])
        }
    }

    // ── minimal plan builder ─────────────────────────────────────────────────

    fn minimal_plan() -> ResolvedRunPlan {
        ResolvedRunPlan {
            export_name: "test_export".into(),
            base_query: "SELECT 1".into(),
            strategy: ExtractionStrategy::Snapshot,
            format: FormatType::Parquet,
            compression: CompressionType::None,
            compression_level: None,
            max_file_size_bytes: None,
            skip_empty: false,
            meta_columns: MetaColumns::default(),
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                bucket: None,
                prefix: None,
                path: Some("/tmp".into()),
                region: None,
                endpoint: None,
                credentials_file: None,
                access_key_env: None,
                secret_key_env: None,
                aws_profile: None,
                allow_anonymous: false,
            },
            quality: None,
            tuning: SourceTuning::from_config(None),
            tuning_profile_label: "balanced".into(),
            validate: false,
            reconcile: false,
            resume: false,
            source: SourceConfig {
                source_type: SourceType::Postgres,
                url: Some("postgresql://localhost/test".into()),
                url_env: None,
                url_file: None,
                host: None,
                port: None,
                user: None,
                password: None,
                password_env: None,
                database: None,
                tuning: None,
                tls: None,
            },
            column_overrides: Default::default(),
            schema_drift_policy: Default::default(),
            shape_drift_warn_factor: 0.0,
        }
    }

    fn run(
        src: &mut dyn crate::source::Source,
        plan: &ResolvedRunPlan,
    ) -> (crate::error::Result<()>, RunSummary) {
        let mut summary = RunSummary::new(plan);
        let result = run_single_export(src, "SELECT 1", None, plan, None, &mut summary);
        (result, summary)
    }

    // ── zero-rows paths ───────────────────────────────────────────────────────

    /// When there are 0 rows and skip_empty is false, run succeeds and status stays "running".
    #[test]
    fn zero_rows_no_skip_empty_succeeds() {
        let plan = minimal_plan();
        let (result, summary) = run(&mut EmptySource, &plan);
        result.expect("0 rows without skip_empty should succeed");
        assert_eq!(summary.total_rows, 0);
        assert_ne!(summary.status, "skipped");
    }

    /// When skip_empty is true and source emits 0 rows, status is "skipped".
    #[test]
    fn zero_rows_with_skip_empty_sets_status_skipped() {
        let mut plan = minimal_plan();
        plan.skip_empty = true;
        let (result, summary) = run(&mut EmptySource, &plan);
        result.expect("skip_empty with 0 rows should succeed");
        assert_eq!(summary.status, "skipped");
        assert_eq!(summary.total_rows, 0);
    }

    /// When there are rows but skip_empty is true, skip_empty must NOT apply.
    /// Verified without running the full pipeline: the early-return path only
    /// triggers when `sink.total_rows == 0`, so any non-zero row count bypasses it.
    /// (We only test the 0-row skip_empty path; the non-zero path needs a live dest.)
    #[test]
    fn skip_empty_semantics_zero_rows_only() {
        // Confirmed by the skip_empty contract: `if sink.total_rows == 0 && skip_empty`.
        // Non-zero rows cannot set status="skipped" — that branch is gated on total_rows==0.
        // This test exists to document the invariant; the live behaviour is covered by
        // live_harness_canary integration tests.
        let mut plan_skip = minimal_plan();
        plan_skip.skip_empty = true;
        let (result_skip, summary_skip) = run(&mut EmptySource, &plan_skip);
        result_skip.expect("skip_empty+0 rows must succeed");
        assert_eq!(summary_skip.status, "skipped");

        let mut plan_no_skip = minimal_plan();
        plan_no_skip.skip_empty = false;
        let (result_no_skip, summary_no_skip) = run(&mut EmptySource, &plan_no_skip);
        result_no_skip.expect("no skip_empty+0 rows must succeed");
        assert_ne!(summary_no_skip.status, "skipped");
    }

    // ── quality gate ──────────────────────────────────────────────────────────

    /// When quality.row_count_min is set and actual rows fall short, the run fails.
    #[test]
    fn quality_row_count_min_fail_aborts_run() {
        use crate::config::QualityConfig;
        let mut plan = minimal_plan();
        plan.quality = Some(QualityConfig {
            row_count_min: Some(100), // require 100 rows minimum
            row_count_max: None,
            null_ratio_max: Default::default(),
            unique_columns: vec![],
        });
        // Source only emits 3 rows → quality gate should fire Severity::Fail
        let (result, summary) = run(&mut RowSource(3), &plan);
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("quality"),
            "expected quality error: {err}"
        );
        assert_eq!(summary.quality_passed, Some(false));
    }

    /// When quality.row_count_max is set and actual rows exceed it, the run fails.
    #[test]
    fn quality_row_count_max_fail_aborts_run() {
        use crate::config::QualityConfig;
        let mut plan = minimal_plan();
        plan.quality = Some(QualityConfig {
            row_count_min: None,
            row_count_max: Some(2), // max 2 rows
            null_ratio_max: Default::default(),
            unique_columns: vec![],
        });
        // Source emits 10 rows → exceeds max
        let (result, summary) = run(&mut RowSource(10), &plan);
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("quality"),
            "expected quality error: {err}"
        );
        assert_eq!(summary.quality_passed, Some(false));
    }
}
