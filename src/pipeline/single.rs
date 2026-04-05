use std::path::Path;
use std::time::Duration;

use super::RunSummary;
use super::chunked::{run_chunked_sequential, run_chunked_sequential_checkpoint};
use super::retry::classify_error;
use super::sink::{CompletedPart, ExportSink, extract_last_cursor_value};
use super::validate::validate_output;
use crate::config::{ExportConfig, ExportMode, SourceConfig, TimeColumnType};
use crate::error::Result;
use crate::source::{self, Source};
use crate::state::StateStore;
use crate::tuning::SourceTuning;
use crate::{destination, format};

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_with_reconnect(
    source_config: &SourceConfig,
    state: &StateStore,
    export: &ExportConfig,
    tuning: &SourceTuning,
    config_dir: &Path,
    validate: bool,
    summary: &mut RunSummary,
    params: Option<&std::collections::HashMap<String, String>>,
    resume: bool,
    config_path: &str,
) -> Result<()> {
    let mut last_err: Option<anyhow::Error> = None;

    for attempt in 0..=tuning.max_retries {
        if attempt > 0 {
            summary.retries = attempt;
            let (_, needs_reconnect, extra_delay) = last_err
                .as_ref()
                .map(classify_error)
                .unwrap_or((false, false, 0));
            let backoff = tuning.retry_backoff_ms * 2u64.pow(attempt - 1) + extra_delay;
            log::warn!(
                "export '{}': retry {}/{} in {}ms{}({})",
                export.name,
                attempt,
                tuning.max_retries,
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
            std::thread::sleep(Duration::from_millis(backoff));
        }

        let mut src = match source::create_source(source_config) {
            Ok(s) => s,
            Err(e) => {
                let (transient, _, _) = classify_error(&e);
                if attempt < tuning.max_retries && transient {
                    log::warn!(
                        "export '{}': connection failed, will retry: {:#}",
                        export.name,
                        e
                    );
                    last_err = Some(e);
                    continue;
                }
                return Err(e);
            }
        };

        match run_export(
            &mut *src,
            source_config,
            state,
            export,
            tuning,
            config_dir,
            validate,
            summary,
            params,
            resume,
            config_path,
        ) {
            Ok(()) => return Ok(()),
            Err(e) => {
                let (transient, _, _) = classify_error(&e);
                if attempt < tuning.max_retries && transient {
                    last_err = Some(e);
                    continue;
                }
                return Err(e);
            }
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("export failed after retries")))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run_export(
    src: &mut dyn Source,
    source_config: &SourceConfig,
    state: &StateStore,
    export: &ExportConfig,
    tuning: &SourceTuning,
    config_dir: &Path,
    validate: bool,
    summary: &mut RunSummary,
    params: Option<&std::collections::HashMap<String, String>>,
    resume: bool,
    config_path: &str,
) -> Result<()> {
    let base_query = export.resolve_query(config_dir, params)?;

    match export.mode {
        ExportMode::Full => {
            run_single_export(
                src,
                &base_query,
                None,
                None,
                export,
                tuning,
                validate,
                Some(state),
                summary,
            )?;
        }
        ExportMode::Incremental => {
            let cursor_state = state.get(&export.name)?;
            let cursor_col = export.cursor_column.as_deref();
            run_single_export(
                src,
                &base_query,
                cursor_col,
                Some(&cursor_state),
                export,
                tuning,
                validate,
                Some(state),
                summary,
            )?;
        }
        ExportMode::Chunked => {
            if export.chunk_checkpoint {
                run_chunked_sequential_checkpoint(
                    src,
                    source_config,
                    state,
                    &base_query,
                    export,
                    tuning,
                    validate,
                    summary,
                    Some(state),
                    resume,
                    config_path,
                )?;
            } else {
                run_chunked_sequential(
                    src,
                    &base_query,
                    export,
                    tuning,
                    validate,
                    summary,
                    Some(state),
                )?;
            }
        }
        ExportMode::TimeWindow => {
            let windowed_query = build_time_window_query(
                &base_query,
                export
                    .time_column
                    .as_deref()
                    .expect("time_column required for TimeWindow mode"),
                export.time_column_type,
                export
                    .days_window
                    .expect("days_window required for TimeWindow mode"),
            );
            run_single_export(
                src,
                &windowed_query,
                None,
                None,
                export,
                tuning,
                validate,
                Some(state),
                summary,
            )?;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn run_single_export(
    src: &mut dyn Source,
    query: &str,
    cursor_column: Option<&str>,
    cursor: Option<&crate::types::CursorState>,
    export: &ExportConfig,
    tuning: &SourceTuning,
    validate: bool,
    state: Option<&StateStore>,
    summary: &mut RunSummary,
) -> Result<()> {
    let mut sink = ExportSink::new(export)?;

    src.export(query, cursor_column, cursor, tuning, &mut sink)?;

    if let Some(w) = sink.writer.take() {
        w.finish()?;
    }

    summary.total_rows += sink.total_rows as i64;
    log::info!("export '{}': {} rows written", export.name, sink.total_rows);

    if sink.total_rows == 0 {
        if export.skip_empty {
            summary.status = "skipped".into();
            log::info!(
                "export '{}': skipped (0 rows, skip_empty=true)",
                export.name
            );
        } else {
            log::info!("export '{}': no data to export", export.name);
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
        }
        if quality_issues
            .iter()
            .any(|i| i.severity == crate::quality::Severity::Fail)
        {
            summary.quality_passed = Some(false);
            anyhow::bail!("export '{}': quality checks failed", export.name);
        }
    }
    if export.quality.is_some() {
        summary.quality_passed = Some(true);
    }

    if sink.part_rows > 0 {
        sink.completed_parts.push(CompletedPart {
            tmp: std::mem::replace(&mut sink.tmp, tempfile::NamedTempFile::new()?),
            rows: sink.part_rows,
        });
    }

    let fmt = format::create_format(export.format, export.compression, export.compression_level);
    let ext = fmt.file_extension();
    let dest = destination::create_destination(&export.destination)?;
    let has_parts = sink.completed_parts.len() > 1;
    let ts = chrono::Utc::now().format("%Y%m%d_%H%M%S");

    for (part_idx, part) in sink.completed_parts.iter().enumerate() {
        if validate {
            validate_output(part.tmp.path(), export.format, part.rows)?;
            summary.validated = Some(true);
        }

        let file_bytes = std::fs::metadata(part.tmp.path())
            .map(|m| m.len())
            .unwrap_or(0);
        summary.bytes_written += file_bytes;
        summary.files_produced += 1;

        let file_name = if has_parts {
            format!("{}_{}_part{}.{}", export.name, ts, part_idx, ext)
        } else {
            format!("{}_{}.{}", export.name, ts, ext)
        };
        dest.write(part.tmp.path(), &file_name)?;

        if let Some(st) = state {
            let _ = st.record_file(
                &summary.run_id,
                &export.name,
                &file_name,
                part.rows as i64,
                file_bytes as i64,
                &format!("{:?}", export.format).to_lowercase(),
                Some(&format!("{:?}", export.compression).to_lowercase()),
            );
        }
    }

    if export.mode == ExportMode::Incremental
        && let (Some(cursor_col), Some(batch), Some(schema), Some(st)) =
            (&export.cursor_column, &sink.last_batch, &sink.schema, state)
        && let Some(last_val) = extract_last_cursor_value(batch, cursor_col, schema)
    {
        st.update(&export.name, &last_val)?;
        log::info!("export '{}': cursor updated to '{}'", export.name, last_val);
    }

    if let (Some(schema), Some(st)) = (&sink.schema, state) {
        let columns: Vec<crate::state::SchemaColumn> = schema
            .fields()
            .iter()
            .map(|f| crate::state::SchemaColumn {
                name: f.name().clone(),
                data_type: format!("{:?}", f.data_type()),
            })
            .collect();

        match st.detect_schema_change(&export.name, &columns) {
            Ok(Some(change)) => {
                summary.schema_changed = Some(true);
                log::warn!("export '{}': schema changed!", export.name);
                if !change.added.is_empty() {
                    log::warn!("  added columns: {}", change.added.join(", "));
                }
                if !change.removed.is_empty() {
                    log::warn!("  removed columns: {}", change.removed.join(", "));
                }
                for (col, old, new) in &change.type_changed {
                    log::warn!("  type changed: {} ({} -> {})", col, old, new);
                }
            }
            Ok(None) => {
                summary.schema_changed = Some(false);
            }
            Err(e) => log::warn!("schema tracking error: {:#}", e),
        }
    }

    log::info!("export '{}' completed successfully", export.name);
    Ok(())
}

pub fn build_time_window_query(
    base_query: &str,
    time_column: &str,
    time_type: TimeColumnType,
    days_window: u32,
) -> String {
    let now = chrono::Utc::now();
    let window_start = now - chrono::Duration::days(days_window as i64);
    let truncated = window_start
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .expect("midnight is always valid");

    let condition = match time_type {
        TimeColumnType::Timestamp => {
            format!(
                "{} >= '{}'",
                time_column,
                truncated.format("%Y-%m-%d %H:%M:%S")
            )
        }
        TimeColumnType::Unix => {
            format!("{} >= {}", time_column, truncated.and_utc().timestamp())
        }
    };

    format!(
        "SELECT * FROM ({base}) AS _rivet WHERE {cond}",
        base = base_query,
        cond = condition,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_time_window_timestamp() {
        let q = build_time_window_query(
            "SELECT * FROM events",
            "created_at",
            TimeColumnType::Timestamp,
            7,
        );
        assert!(q.contains("created_at >= '"), "got: {}", q);
        assert!(q.contains("_rivet WHERE"));
    }

    #[test]
    fn test_build_time_window_unix() {
        let q = build_time_window_query("SELECT * FROM events", "ts", TimeColumnType::Unix, 30);
        assert!(q.contains("ts >= "), "got: {}", q);
        assert!(!q.contains("'"), "unix should not have quotes, got: {}", q);
    }
}
