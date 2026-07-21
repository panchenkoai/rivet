//! **Layer: Execution** (with bounded persistence writes)
//!
//! Runs a single-query export for Snapshot, Incremental, and TimeWindow strategies.
//! After the write completes, this module performs three bounded persistence writes
//! that are invariant-ordered (see ADR-0001): manifest entry, cursor advance,
//! schema snapshot.  These are post-execution state updates, not runtime decisions.

use std::time::Duration;

use super::RunSummary;
use super::chunked::{run_chunked_sequential, run_chunked_sequential_checkpoint};
use super::retry::{RetryClass, classify_error};
use super::sink::{CompletedPart, ExportSink};
use super::validate::validate_output;
use crate::error::{DataIntegrityError, Result};
use crate::journal::RunEvent;
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
            let class = last_err
                .as_ref()
                .map(classify_error)
                .unwrap_or(RetryClass::Permanent);
            let backoff =
                plan.tuning.retry_backoff_ms * 2u64.pow(attempt - 1) + class.extra_delay_ms();
            log::warn!(
                "export '{}': retry {}/{} in {}ms{}({})",
                plan.export_name,
                attempt,
                plan.tuning.max_retries,
                backoff,
                if class.needs_reconnect() {
                    " [reconnecting] "
                } else {
                    " "
                },
                last_err
                    .as_ref()
                    .map(crate::redact::redact_error)
                    .unwrap_or_default(),
            );
            summary.journal.record(RunEvent::RetryAttempted {
                attempt,
                reason: last_err
                    .as_ref()
                    .map(crate::redact::redact_error)
                    .unwrap_or_default(),
                backoff_ms: backoff,
            });
            std::thread::sleep(Duration::from_millis(backoff));
        }

        let mut src = match source::create_source(&plan.source) {
            Ok(s) => s,
            Err(e) => {
                // A closed port / unroutable host / DNS failure will never come
                // up on its own — retrying just spams ~14s of escalating backoff
                // before the same error. Fail fast with a doctor pointer on the
                // first attempt. (Connection RESET / EOF / cold-start "starting
                // up" can recover, so those still flow into the retry below.)
                if attempt == 0 && is_port_closed(&e) {
                    return Err(e.context(format!(
                        "cannot reach the source — run `rivet doctor -c {config_path}` to diagnose (is the host/port right and the server up?)"
                    )));
                }
                if attempt < plan.tuning.max_retries && classify_error(&e).is_transient() {
                    log::warn!(
                        "export '{}': connection failed, will retry: {}",
                        plan.export_name,
                        crate::redact::redact_error(&e)
                    );
                    last_err = Some(e);
                    continue;
                }
                // Final, non-transient connect failure (e.g. auth): bridge the
                // same category + remediation hint `rivet doctor` already gives,
                // so a run-time failure isn't a dead end. `.context()` (not
                // message replacement) keeps the typed cause + the substrings
                // classify_exit / retry classification depend on.
                return Err(attach_connect_hint(e, &plan.source));
            }
        };

        match run_export(&mut *src, state, plan, summary, config_path) {
            Ok(()) => return Ok(()),
            Err(e) => match decide_export_retry(
                attempt,
                plan.tuning.max_retries,
                summary.files_committed,
                &plan.export_name,
                &e,
            ) {
                ExportRetry::Retry => {
                    last_err = Some(e);
                    continue;
                }
                ExportRetry::BailDuplicateGuard(err) => return Err(err),
                ExportRetry::BailOriginal => return Err(e),
            },
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("export failed after retries")))
}

/// Outcome of the retry decision in `run_with_reconnect`.
///
/// Lifted out of the loop body so the invariant — "do not retry after a file
/// was committed; that would duplicate rows" — is independently unit-testable.
#[derive(Debug)]
enum ExportRetry {
    /// Caller should consume the error, sleep the backoff, and retry.
    Retry,
    /// Caller must propagate this synthesised error: the original failure was
    /// transient, but at least one file was already committed to the destination
    /// so retrying from the same cursor would re-read those rows.
    BailDuplicateGuard(anyhow::Error),
    /// Caller must propagate the *original* error (either permanent, or we have
    /// exhausted the retry budget).
    BailOriginal,
}

fn decide_export_retry(
    attempt: u32,
    max_retries: u32,
    files_committed: usize,
    export_name: &str,
    err: &anyhow::Error,
) -> ExportRetry {
    if attempt >= max_retries || !classify_error(err).is_transient() {
        return ExportRetry::BailOriginal;
    }
    if files_committed > 0 {
        // Data-integrity stop (exit 3): rows already landed and a same-cursor
        // retry would duplicate them. Typed marker so a scheduler does not treat
        // the underlying *transient* cause as safe-to-retry. Message verbatim.
        return ExportRetry::BailDuplicateGuard(
            DataIntegrityError::new(format!(
                "export '{}': transient error after {} file(s) written to destination \
                 — cannot safely retry (would duplicate rows). \
                 Run `rivet reconcile` to verify state. Original error: {:#}",
                export_name, files_committed, err
            ))
            .into(),
        );
    }
    ExportRetry::Retry
}

pub(crate) fn run_export(
    src: &mut dyn Source,
    state: &StateStore,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    config_path: &str,
) -> Result<()> {
    // Keyset (seek) pagination owns its own sequential paging loop (OPT-4).
    if let ExtractionStrategy::Keyset(kp) = &plan.strategy {
        // A MongoDB keyset export with `parallel: N` fans N disjoint `_id`-range
        // workers (each keyset-paging its slice); every other keyset stays the
        // single sequential seek loop.
        if plan.source.source_type == crate::config::SourceType::Mongo && kp.parallel.max(1) > 1 {
            return super::mongo_parallel::run_mongo_parallel(plan, summary, state, kp);
        }
        return super::keyset::run_keyset(src, plan, summary, Some(state));
    }

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
    // Resolve catalog hints from the unwrapped base. For Snapshot/Incremental
    // `query == base_query` (the incremental predicate is applied inside the
    // driver); for TimeWindow `resolve_query` wraps the base in a
    // `SELECT * FROM (base) WHERE <ts> BETWEEN …` subquery that hides the source
    // table, so the base is needed to resolve PG NUMERIC. Passing it through
    // `wrapped` is correct for both (no-op when query == base).
    let request = source::ExportRequest::wrapped(
        query,
        &plan.base_query,
        &plan.tuning,
        &plan.column_overrides,
    )
    .with_incremental(plan.strategy.incremental_plan())
    .with_cursor(cursor);

    // Pipelined path (on by default; disable with `RIVET_PIPELINE_WRITES=0`):
    // the source thread only fetches + converts; a worker thread owns the
    // ExportSink and does the parquet encode/compress so DB I/O overlaps with
    // compression CPU. The worker error (if any) is recovered by `finish()` and
    // takes precedence over the source-side error.
    let mut sink = if super::sink::PipelinedSink::enabled() {
        let mut psink = super::sink::PipelinedSink::spawn(plan)?;
        let export_result = src.export(&request, &mut psink);
        let sink = psink.finish()?;
        export_result?;
        sink
    } else {
        let mut sink = ExportSink::new(plan)?;
        src.export(&request, &mut sink)?;
        sink
    };

    // Test fault-point #1: after the source stream was fully read but
    // before the writer is finalised.  Exercised by QA backlog Task 1.1.
    crate::test_hook::maybe_panic_at("after_source_read");
    // Deterministic "alive mid-export" window for the OPT-6 signal-reaping test.
    crate::test_hook::maybe_block_at("after_source_read");

    if let Some(w) = sink.writer.take() {
        w.finish()?;
    }

    summary.total_rows += sink.total_rows as i64;
    log::info!(
        "export '{}': {} rows written",
        plan.export_name,
        sink.total_rows
    );

    // Run quality checks BEFORE the empty-export early return so that
    // row_count_min fires even when the source returned zero rows.
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
        let fails: Vec<&str> = quality_issues
            .iter()
            .filter(|i| i.severity == crate::quality::Severity::Fail)
            .map(|i| i.message.as_str())
            .collect();
        if !fails.is_empty() {
            summary.quality_passed = Some(false);
            // Surface *which* checks failed (they're already computed +
            // warn-logged above) via the shared failure contract in
            // `crate::quality` so single and chunked modes can't drift. Tagged
            // as a data-integrity failure (exit 3) so a scheduler stops rather
            // than retries — the message text is unchanged.
            return Err(DataIntegrityError::new(crate::quality::failure_message(
                &plan.export_name,
                None,
                &fails,
            ))
            .into());
        }
    }
    if plan.quality.is_some() {
        summary.quality_passed = Some(true);
    }

    if sink.total_rows == 0 {
        if plan.skip_empty {
            summary.status = "skipped".into();
            // Attach a short, mode-specific reason so the operator can see
            // *why* nothing was written, not just `status: skipped` with
            // an empty surrounding. Incremental no-op is the common case
            // (no rows past the recorded cursor); other modes get a
            // generic 0-rows note.
            summary.skip_reason = Some(match plan.strategy.cursor_column() {
                Some(col) => format!("no new rows since cursor '{col}'"),
                None => "source returned 0 rows".into(),
            });
            log::info!(
                "export '{}': skipped (0 rows, skip_empty=true)",
                plan.export_name
            );
        } else {
            log::info!("export '{}': no data to export", plan.export_name);
        }
        return Ok(());
    }

    if sink.part_rows > 0 {
        sink.completed_parts.push(CompletedPart {
            tmp: std::mem::replace(&mut sink.tmp, tempfile::NamedTempFile::new()?),
            rows: sink.part_rows,
        });
    }

    let fmt = format::create_format(plan.format, plan.compression, plan.compression_level, None);
    let ext = fmt.file_extension();
    let dest = destination::create_destination(&plan.destination)?;
    // Finding #44, early check: fail cleanly before the first part if this
    // prefix already belongs to a CDC export (cross-shape manifests clobber
    // each other; the write-seam guard is the backstop).
    crate::manifest::guard_manifest_mode(dest.as_ref(), "batch")?;

    // ADR-0004: log backend capabilities; warn when non-retry-safe destination is configured with retries.
    destination::log_capabilities(
        &plan.export_name,
        dest.as_ref(),
        plan.destination.destination_type,
        plan.tuning.max_retries,
    );

    let has_parts = sink.completed_parts.len() > 1;
    // Millisecond precision (matches keyset.rs / mongo_parallel.rs / cdc sink):
    // two runs into the same prefix within the same SECOND must not produce
    // identical part names, or the later run silently clobbers the earlier's file
    // (LocalDestination idempotent_overwrite) — a real incremental-delta loss.
    let ts = chrono::Utc::now().format("%Y%m%d_%H%M%S_%3f");

    for (part_idx, part) in sink.completed_parts.iter().enumerate() {
        if plan.validate {
            validate_output(part.tmp.path(), plan.format, part.rows)?;
            summary.validated = Some(true);
            summary
                .journal
                .record(RunEvent::ValidationResult { passed: true });
        }

        let file_name = if has_parts {
            format!("{}_{}_part{}.{}", plan.export_name, ts, part_idx, ext)
        } else {
            format!("{}_{}.{}", plan.export_name, ts, ext)
        };

        // ADR-0001 I1→I2→I7 + the I2/I3 fault windows + the manifest/journal/
        // counters all live in `commit::{write_part_file,record_part}` now (one
        // home for the ordering that used to be copied across runners).
        let rec = super::commit::write_part_file(
            dest.as_ref(),
            part.tmp.path(),
            part.rows as i64,
            file_name,
        )?;
        super::commit::record_part(
            plan,
            summary,
            state,
            &rec,
            super::commit::PartKind::File {
                part_index: part_idx,
            },
        );
    }

    // Round-2 audit #12: record the incremental cursor RANGE on the summary but do
    // NOT advance the state cursor here. Under ADR-0001 the DESTINATION is the
    // loader's source of truth, so the cursor advance must happen AFTER the
    // destination manifest is durable (`finalize_manifest`) — otherwise a crash in
    // the advance→manifest window leaves the cursor past parts the manifest never
    // recorded, which the manifest-authoritative `rivet load` then silently drops
    // (an export→load at-least-once break). The caller commits the cursor via
    // `commit_incremental_cursor` once the manifest is written; the
    // `after_cursor_commit` fault hook fires there.
    if let (Some(last_val), Some(st)) = (&sink.last_cursor_value, state) {
        // Capture the value this run resumed FROM (the low) — the deferred commit
        // overwrites the stored cursor with the new high; the RANGE (low..high)
        // ships to the manifest for warehouse-side continuity.
        let prior_low = st
            .get(&plan.export_name)
            .ok()
            .and_then(|e| e.last_cursor_value.clone());
        summary.cursor_column = plan
            .strategy
            .incremental_plan()
            .map(|p| p.column_for_storage_extract().to_string());
        summary.cursor_low = prior_low;
        summary.cursor_high = Some(last_val.clone());
    }

    // ADR-0012 M3: pin the dest schema fingerprint on the summary so
    // `finalize_manifest` does not have to round-trip through the state
    // store (which is only populated by the drift-detect path below, and
    // not at all in chunked mode).
    if let Some(schema) = sink.dest_schema.as_deref() {
        super::manifest_writer::record_run_schema_fingerprint(summary, schema);
    }

    if let (Some(schema), Some(st)) = (&sink.dest_schema, state) {
        // Single mode: drift from the sink's resolved (data-derived) schema,
        // post-write. Chunked runs the same facade pre-chunk via
        // `check_from_type_mappings` (ADR-0021).
        super::schema_drift::check_from_sink_schema(
            st,
            &plan.export_name,
            schema,
            plan.schema_drift_policy,
            summary,
        )?;
    }

    // Form B: harvest the per-column value checksums the sink accumulated into the
    // summary, so the manifest records them. `validate` re-reads the parts to verify
    // the Arrow→Parquet encode + post-write fault the in-process Form A cannot see.
    if !sink.column_checksums.is_empty() {
        summary.column_checksums = sink
            .column_checksums
            .iter()
            .map(|(name, sum)| crate::manifest::ColumnChecksum {
                name: name.clone(),
                checksum: sum.to_string(),
            })
            .collect();
        summary.checksum_key_column = sink.checksum_key_col.and(sink.cursor_column.clone());
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

/// Round-2 audit #12: advance the incremental cursor AFTER the destination
/// manifest is durable. `run_single_export` records the cursor range on the
/// summary but leaves the state cursor untouched; the caller invokes this once
/// `finalize_manifest` has written the manifest, so a crash never leaves the
/// cursor advanced past parts the loader-authoritative manifest doesn't record.
/// No-op unless an incremental cursor is pending (`summary.cursor_high`). The
/// `after_cursor_commit` fault hook fires inside `RunStore::commit()` here.
pub(super) fn commit_incremental_cursor(
    state: &StateStore,
    plan: &ResolvedRunPlan,
    summary: &RunSummary,
) -> Result<()> {
    if let Some(cursor) = summary.cursor_high.clone() {
        super::run_store::RunStore::finalize(state, plan, summary)
            .with_cursor(cursor.clone())
            .with_progression(super::run_store::Progression::Incremental { last_value: cursor })
            .commit()?;
    }
    Ok(())
}

/// True for connect errors that will NEVER recover on their own — nothing is
/// listening, the host is unroutable, or DNS can't resolve it. Deliberately
/// EXCLUDES connection-reset / broken-pipe / EOF (a transient mid-handshake
/// blip that does retry) and is never hit by cold-start "starting up", which is
/// a post-connect SQLSTATE, not a connect failure.
/// Bridge `rivet doctor`'s source-error categorizer + remediation hint to the
/// run-time connect seam, so a failed `rivet run` (auth, TLS, …) carries the same
/// guidance instead of a bare driver error. `.context()` (not message
/// replacement) keeps the original error — and the substrings classify_exit /
/// retry classification rely on — intact underneath.
// pub(crate) so the chunked/parallel runners attach the same actionable connect
// hint (doctor pointer + auth/TLS category) their first `create_source` — the
// single-export path had it, the parallel-chunked path (the default for large
// tables) leaked the raw driver error. Audit finding.
pub(crate) fn attach_connect_hint(
    e: anyhow::Error,
    source: &crate::config::SourceConfig,
) -> anyhow::Error {
    let category = crate::preflight::categorize_source_error(&e);
    match crate::preflight::source_error_hint(category, &e, &source.source_type) {
        Some(hint) => e.context(format!("{category} — {hint}")),
        None => e,
    }
}

fn is_port_closed(e: &anyhow::Error) -> bool {
    let m = format!("{e:#}").to_ascii_lowercase();
    m.contains("connection refused")
        || m.contains("no route to host")
        || m.contains("network unreachable")
        || m.contains("name or service not known") // Linux DNS failure
        || m.contains("nodename nor servname") // macOS DNS failure
        || m.contains("failed to lookup address") // hickory/reqwest DNS failure
        || m.contains("could not translate host name") // libpq DNS failure
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
            _request: &crate::source::ExportRequest<'_>,
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
            _request: &crate::source::ExportRequest<'_>,
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
                path: Some("/tmp".into()),
                ..Default::default()
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
                environment: None,
                tuning: None,
                tls: None,
                mongo: None,
            },
            column_overrides: Default::default(),
            verify: crate::config::VerifyMode::Size,
            schema_drift_policy: Default::default(),
            shape_drift_warn_factor: 0.0,
            parquet: None,
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
            unique_max_entries: None,
        });
        // Source only emits 3 rows → quality gate should fire Severity::Fail
        let (result, summary) = run(&mut RowSource(3), &plan);
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("quality"),
            "expected quality error: {err}"
        );
        // The quality bail carries the DataIntegrityError marker → exit class 3
        // (STOP), and the operator message is unchanged (asserted above).
        assert!(
            err.downcast_ref::<DataIntegrityError>().is_some(),
            "quality-gate failure must be a typed data-integrity error"
        );
        assert_eq!(crate::error::classify_exit(&err), 3);
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
            unique_max_entries: None,
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

    // ── decide_export_retry: retry/reconnect decision matrix ────────────────
    //
    // These tests exhaustively cover the four-way decision that gates the
    // retry loop in `run_with_reconnect`. The function is pure (no I/O, no
    // state) which lets every combination of (attempt, max, files_committed,
    // error class) be enumerated without touching docker.

    fn transient_err() -> anyhow::Error {
        // String form matches the network branch in classify_error.
        anyhow::anyhow!("connection reset by peer")
    }

    fn permanent_err() -> anyhow::Error {
        // String form matches the auth branch in classify_error.
        anyhow::anyhow!("permission denied for table users")
    }

    #[test]
    fn decide_retry_transient_with_budget_and_no_files_retries() {
        let d = decide_export_retry(0, 3, 0, "orders", &transient_err());
        assert!(matches!(d, ExportRetry::Retry), "got: {d:?}");
    }

    #[test]
    fn decide_retry_transient_after_files_committed_is_duplicate_guard() {
        // After a file has been written, retrying from the same cursor would
        // re-export those rows. The guard is the highest-value invariant in
        // the retry loop — losing it would silently corrupt a downstream
        // warehouse. Test it explicitly.
        let err = transient_err();
        let d = decide_export_retry(1, 3, 2, "orders", &err);
        match d {
            ExportRetry::BailDuplicateGuard(synth) => {
                let s = format!("{:#}", synth);
                assert!(
                    s.contains("2 file(s) written"),
                    "must surface how many files were committed: {s}"
                );
                assert!(
                    s.contains("would duplicate rows"),
                    "must explain the invariant being protected: {s}"
                );
                assert!(s.contains("rivet reconcile"), "must hint at recovery: {s}");
                assert!(
                    s.contains("connection reset"),
                    "must chain the original error: {s}"
                );
            }
            other => panic!("expected BailDuplicateGuard, got {other:?}"),
        }
    }

    #[test]
    fn duplicate_guard_is_typed_data_integrity_exit_3() {
        // The duplicate-guard bail must carry the DataIntegrityError marker so
        // `classify_exit` returns 3 (STOP, do not blindly retry) — even though
        // the *underlying* cause was transient. A scheduler must not treat a
        // possibly-duplicated dataset as safe-to-retry.
        let err = transient_err();
        let d = decide_export_retry(1, 3, 2, "orders", &err);
        let synth = match d {
            ExportRetry::BailDuplicateGuard(e) => e,
            other => panic!("expected BailDuplicateGuard, got {other:?}"),
        };
        assert!(
            synth.downcast_ref::<DataIntegrityError>().is_some(),
            "duplicate-guard bail must carry the DataIntegrityError marker"
        );
        assert_eq!(crate::error::classify_exit(&synth), 3);
    }

    #[test]
    fn decide_retry_transient_after_budget_exhausted_bails_original() {
        // attempt == max_retries → no more attempts allowed.
        let d = decide_export_retry(3, 3, 0, "orders", &transient_err());
        assert!(matches!(d, ExportRetry::BailOriginal), "got: {d:?}");
    }

    #[test]
    fn decide_retry_permanent_error_bails_immediately_regardless_of_budget() {
        // Permanent errors short-circuit even when retry budget remains.
        let d = decide_export_retry(0, 3, 0, "orders", &permanent_err());
        assert!(matches!(d, ExportRetry::BailOriginal), "got: {d:?}");
    }

    #[test]
    fn decide_retry_permanent_error_after_files_does_not_synthesise_duplicate_guard() {
        // The duplicate-guard message is reserved for *transient* errors —
        // a permanent failure should propagate as-is so the operator sees the
        // real cause, not a wrapped recovery hint.
        let d = decide_export_retry(0, 3, 5, "orders", &permanent_err());
        assert!(matches!(d, ExportRetry::BailOriginal), "got: {d:?}");
    }

    #[test]
    fn decide_retry_zero_max_retries_never_retries() {
        // `max_retries = 0` is the "fail fast" config. attempt(0) >= max(0)
        // must bail immediately even on a transient error.
        let d = decide_export_retry(0, 0, 0, "orders", &transient_err());
        assert!(matches!(d, ExportRetry::BailOriginal), "got: {d:?}");
    }

    #[test]
    fn is_port_closed_fast_bails_dead_endpoints_not_transient_blips() {
        // Hard "nothing will ever answer here" signals → fast-bail.
        assert!(is_port_closed(&anyhow::anyhow!(
            "error connecting to server: Connection refused (os error 61)"
        )));
        assert!(is_port_closed(&anyhow::anyhow!(
            "No route to host (os error 65)"
        )));
        assert!(is_port_closed(&anyhow::anyhow!(
            "dns error: failed to lookup address information: Name or service not known"
        )));
        // Transient / recoverable signals → must NOT fast-bail (they retry).
        assert!(!is_port_closed(&anyhow::anyhow!(
            "connection reset by peer (os error 54)"
        )));
        assert!(!is_port_closed(&anyhow::anyhow!("broken pipe")));
        // Cold-start: a post-connect SQLSTATE, must keep retrying.
        assert!(!is_port_closed(&anyhow::anyhow!(
            "db error: FATAL: the database system is starting up"
        )));
    }
}
