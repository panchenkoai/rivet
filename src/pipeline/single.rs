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
use super::sink::{CompletedPart, ExportSink, extract_last_cursor_value};
use super::validate::validate_output;
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
        &mut sink,
    )?;

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

    // ADR-0004: inspect backend capabilities at runtime.
    // Logs commit protocol and warns when a non-retry-safe destination is used with retries.
    {
        let caps = dest.capabilities();
        log::debug!(
            "export '{}': destination commit_protocol={:?} idempotent={} retry_safe={} partial_risk={}",
            plan.export_name,
            caps.commit_protocol,
            caps.idempotent_overwrite,
            caps.retry_safe,
            caps.partial_write_risk,
        );
        if !caps.retry_safe && summary.retries > 0 {
            log::warn!(
                "export '{}': destination is not retry-safe ({} retries used); \
                 partial artifacts may exist at destination — manual cleanup may be needed",
                plan.export_name,
                summary.retries,
            );
        }
    }

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
    }

    if let Some(cursor_col) = plan.strategy.cursor_extract_column()
        && let (Some(batch), Some(schema), Some(st)) = (&sink.last_batch, &sink.schema, state)
        && let Some(last_val) = extract_last_cursor_value(batch, cursor_col, schema)
    {
        st.update(&plan.export_name, &last_val)?;
        log::info!(
            "export '{}': cursor updated to '{}'",
            plan.export_name,
            last_val
        );
        // Epic G: record committed boundary for progression reporting.
        if let Err(e) =
            st.record_committed_incremental(&plan.export_name, &last_val, &summary.run_id)
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
                log::warn!("export '{}': schema changed!", plan.export_name);
                if !change.added.is_empty() {
                    log::warn!("  added columns: {}", change.added.join(", "));
                }
                if !change.removed.is_empty() {
                    log::warn!("  removed columns: {}", change.removed.join(", "));
                }
                for (col, old, new) in &change.type_changed {
                    log::warn!("  type changed: {} ({} -> {})", col, old, new);
                }
                summary.journal.record(RunEvent::SchemaChanged {
                    added: change.added.clone(),
                    removed: change.removed.clone(),
                    type_changed: change.type_changed.clone(),
                });
            }
            Ok(None) => {
                summary.schema_changed = Some(false);
            }
            Err(e) => log::warn!("schema tracking error: {:#}", e),
        }
    }

    log::info!("export '{}' completed successfully", plan.export_name);
    Ok(())
}
