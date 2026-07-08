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
use super::{RunSummary, sink::ExportSink};
use crate::config::IncrementalCursorMode;
use crate::error::Result;
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

    // RESUME (opt-in via `kp.checkpoint`): continue from the last DURABLY
    // committed key so a crashed run picks up where it left off instead of
    // re-reading the whole collection. Reuses the incremental `export_state`
    // store. Default OFF keeps `mode: full` re-reading the whole key range every
    // run — resume is only correct when the caller wants it (a plain re-run of a
    // full export must NOT silently skip already-exported rows). Lossless for
    // keys whose output-column string uniquely and order-preservingly identifies
    // the source key (integers, strings, timestamps, UUIDs, ObjectId hex).
    let mut last: Option<String> = if kp.checkpoint {
        state
            .and_then(|s| s.get(&plan.export_name).ok())
            .and_then(|cs| cs.last_cursor_value)
    } else {
        None
    };
    let mut pages: usize = 0;

    loop {
        let cursor = last.as_ref().map(|v| CursorState {
            export_name: plan.export_name.clone(),
            last_cursor_value: Some(v.clone()),
            last_run_at: None,
        });

        let mut sink = ExportSink::new(plan)?;
        src.export(
            // `query` is the unwrapped base; the driver wraps it with the keyset
            // predicate internally, so the catalog parser still sees the source
            // table and hints resolve from `query` (`unwrapped`).
            &source::ExportRequest::unwrapped(
                &plan.base_query,
                &plan.tuning,
                &plan.column_overrides,
            )
            .with_incremental(Some(&key_plan))
            .with_cursor(cursor.as_ref())
            .with_page_limit(kp.chunk_size),
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

        let fmt =
            format::create_format(plan.format, plan.compression, plan.compression_level, None);
        let base = format!(
            "{}_{}_keyset{}.{}",
            plan.export_name,
            chrono::Utc::now().format("%Y%m%d_%H%M%S"),
            pages,
            fmt.file_extension()
        );
        let dest = destination::create_destination(&plan.destination)?;
        // Finding #44, early check: fail cleanly before the first part if this
        // prefix already belongs to a CDC export (cross-shape manifests clobber
        // each other; the write-seam guard is the backstop).
        crate::manifest::guard_manifest_mode(dest.as_ref(), "batch")?;
        // Shared commit path (I1→I2→I7 + counters + journal + fault hooks).
        // write_sink_parts drains every part the sink produced — the final
        // temp file plus anything maybe_split rotated at max_file_size — so
        // rotation cannot drop data.
        let recs = super::commit::write_sink_parts(
            dest.as_ref(),
            &mut sink,
            plan.validate.then_some(plan.format),
            |idx, count| super::commit::part_indexed_name(&base, idx, count),
        )?;
        if plan.validate {
            summary.validated = Some(true);
        }
        for rec in &recs {
            super::commit::record_part(
                plan,
                summary,
                state,
                rec,
                super::commit::PartKind::Page {
                    page_index: pages as i64,
                },
            );
        }
        // Persist the page's max key AFTER its parts are durably committed, so a
        // resume continues from committed data (peek→flush→ack ordering). Cost of
        // the crash window between the commit and this line is at-least-once: the
        // last page is re-read (a downstream dedup / reconcile absorbs it), never
        // lost. Only when resume is opted in, and only when we read the key.
        if kp.checkpoint
            && let (Some(st), Some(v)) = (state, &sink.last_cursor_value)
        {
            st.update(&plan.export_name, v)?;
        }
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
                 one of: integer, float, string, timestamp, date, uuid.",
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
