//! **Layer: Execution** — keyset (seek) pagination runner (OPT-4).
//!
//! The source-safe shape for tables without a single-integer PK. Pages the
//! table by one index-backed, NOT NULL, unique key:
//!
//! ```sql
//! SELECT * FROM (<base>) AS _rivet [WHERE <key> > <last>] ORDER BY <key> LIMIT <n>
//! ```
//!
//! Each page is a bounded, index-driven range scan (never a filesort — the key
//! is index-backed by construction, see [`crate::plan::build`]) and becomes one
//! output part file. This bounds both peak RSS (`<= chunk_size` rows in flight)
//! and longest-query time (one `LIMIT` seek), unlike a `mode: full` snapshot
//! which holds a single unbounded `SELECT` open with no MySQL server cursor.
//!
//! Reuses the incremental machinery: the driver builds the page via
//! [`crate::source::query::build_keyset_query`] with the same injection-safe
//! value handling as incremental, and [`ExportSink`] tracks the per-page max
//! key in `last_cursor_value` (its `cursor_extract_column` resolves to the
//! keyset key), which the loop reads to advance to the next page.

use super::manifest_writer;
use super::{RunSummary, sink::ExportSink, validate::validate_output};
use crate::config::IncrementalCursorMode;
use crate::error::Result;
use crate::journal::RunEvent;
use crate::plan::{ExtractionStrategy, IncrementalCursorPlan, KeysetPlan, ResolvedRunPlan};
use crate::source::{self, Source};
use crate::state::StateStore;
use crate::types::CursorState;
use crate::{destination, format};

fn keyset_plan(plan: &ResolvedRunPlan) -> &KeysetPlan {
    match &plan.strategy {
        ExtractionStrategy::Keyset(kp) => kp,
        _ => unreachable!("keyset runner called with non-keyset plan"),
    }
}

pub(crate) fn run_keyset(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    state: Option<&StateStore>,
) -> Result<()> {
    let kp = keyset_plan(plan);
    // The key drives both the WHERE/ORDER BY (built in the driver) and the
    // sink's per-page max-key extraction (via `cursor_extract_column`).
    let key_plan = IncrementalCursorPlan {
        primary_column: kp.key_column.clone(),
        fallback_column: None,
        mode: IncrementalCursorMode::SingleColumn,
    };

    log::info!(
        "export '{}': keyset (seek) pagination on '{}', page size {}",
        plan.export_name,
        kp.key_column,
        kp.chunk_size
    );

    let mut last: Option<String> = None;
    let mut pages: usize = 0;

    loop {
        let cursor = last.as_ref().map(|v| CursorState {
            export_name: plan.export_name.clone(),
            last_cursor_value: Some(v.clone()),
            last_run_at: None,
        });

        let mut sink = ExportSink::new(plan)?;
        src.export(
            &source::ExportRequest {
                query: &plan.base_query,
                incremental: Some(&key_plan),
                cursor: cursor.as_ref(),
                tuning: &plan.tuning,
                column_overrides: &plan.column_overrides,
                page_limit: Some(kp.chunk_size),
            },
            &mut sink,
        )?;
        if let Some(w) = sink.writer.take() {
            w.finish()?;
        }
        // ADR-0012 M3: capture the dest schema fingerprint from the first
        // non-empty page; idempotent run-wide.
        if let Some(s) = sink.dest_schema.as_deref() {
            manifest_writer::record_run_schema_fingerprint(summary, s);
        }

        let rows = sink.total_rows;
        if rows == 0 {
            break; // empty page (table exhausted, or an exact-multiple last page)
        }
        summary.total_rows += rows as i64;

        if plan.validate {
            validate_output(sink.tmp.path(), plan.format, rows)?;
            summary.validated = Some(true);
        }
        let file_bytes = std::fs::metadata(sink.tmp.path())
            .map(|m| m.len())
            .unwrap_or(0);
        summary.bytes_written += file_bytes;
        summary.files_produced += 1;

        let fmt =
            format::create_format(plan.format, plan.compression, plan.compression_level, None);
        let file_name = format!(
            "{}_{}_keyset{}.{}",
            plan.export_name,
            chrono::Utc::now().format("%Y%m%d_%H%M%S"),
            pages,
            fmt.file_extension()
        );
        let dest = destination::create_destination(&plan.destination)?;
        dest.write(sink.tmp.path(), &file_name)?;

        // ADR-0012 M1: record the committed part for the manifest.
        manifest_writer::record_committed_part(
            summary,
            file_name.clone(),
            rows as i64,
            file_bytes,
            sink.tmp.path(),
        );

        if let Some(st) = state
            && let Err(e) = st.record_file(
                &summary.run_id,
                &plan.export_name,
                &file_name,
                rows as i64,
                file_bytes as i64,
                plan.format.label(),
                Some(plan.compression.label()),
            )
        {
            log::warn!(
                "export '{}': file_log write failed for keyset page '{}' (file was produced): {:#}",
                plan.export_name,
                file_name,
                e
            );
        }

        summary.journal.record(RunEvent::ChunkCompleted {
            chunk_index: pages as i64,
            rows: rows as i64,
            file_name: Some(file_name),
        });
        log::info!(
            "export '{}': keyset page {} — {} rows",
            plan.export_name,
            pages,
            rows
        );
        pages += 1;

        // A short page means the index range is exhausted — stop without an
        // extra empty round-trip.
        if rows < kp.chunk_size {
            break;
        }
        // Advance. The sink extracts the page's max key; if it could not (NULL
        // or an unsupported Arrow type), we must NOT loop on the same bound —
        // that would re-read the same page forever.
        match sink.last_cursor_value.clone() {
            Some(v) => last = Some(v),
            None => anyhow::bail!(
                "export '{}': keyset could not read the '{}' value from the last row of page {} \
                 (NULL or unsupported type) — cannot advance safely. The key must be NOT NULL and \
                 one of: integer, float, string, timestamp, date.",
                plan.export_name,
                kp.key_column,
                pages - 1
            ),
        }
    }

    log::info!(
        "export '{}': keyset complete — {} page(s), {} rows",
        plan.export_name,
        pages,
        summary.total_rows
    );
    Ok(())
}
