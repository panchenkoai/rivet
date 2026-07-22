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

/// One keyset page produced by [`read_keyset_page`]: the parts written to the
/// destination, the row count, the dest schema (for the run fingerprint), and
/// the typed high-water cursor to advance from. The two runners
/// ([`run_keyset`] sequential, `mongo_parallel::range_worker` parallel) share
/// the page READ; they differ only in WHEN the parts commit, which stays each
/// caller's business.
pub(crate) struct KeysetPage {
    pub(crate) parts: Vec<super::commit::PartRecord>,
    pub(crate) rows: usize,
    pub(crate) schema: Option<arrow::datatypes::Schema>,
    pub(crate) next_cursor: Option<String>,
}

/// Read ONE seek page: `find`-and-seek from `cursor` (or the range floor), write
/// its parts to `dest` named by `part_base`, and report the page + the typed
/// high-water cursor. Returns `None` when the page is empty (range exhausted).
///
/// Paging control stays with the caller via the returned `rows`/`next_cursor`:
/// a page shorter than `page_size` is the last one; a full page whose
/// `next_cursor` is `None` cannot advance (the caller must bail rather than
/// re-read the same bound forever).
pub(crate) fn read_keyset_page(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    key_plan: &IncrementalCursorPlan,
    page_size: usize,
    cursor: Option<&str>,
    dest: &dyn destination::Destination,
    part_base: &str,
) -> Result<Option<KeysetPage>> {
    let cursor_state = cursor.map(|v| CursorState {
        export_name: plan.export_name.clone(),
        last_cursor_value: Some(v.to_string()),
        last_run_at: None,
    });
    let mut sink = ExportSink::new(plan)?;
    src.export(
        // `query` is the unwrapped base; the driver wraps it with the keyset
        // predicate internally, so the catalog parser still sees the source
        // table and hints resolve from `query` (`unwrapped`).
        &source::ExportRequest::unwrapped(&plan.base_query, &plan.tuning, &plan.column_overrides)
            .with_incremental(Some(key_plan))
            .with_cursor(cursor_state.as_ref())
            .with_page_limit(page_size),
        &mut sink,
    )?;
    if let Some(w) = sink.writer.take() {
        w.finish()?;
    }
    let rows = sink.total_rows;
    if rows == 0 {
        return Ok(None); // range exhausted, or an exact-multiple last page
    }
    let schema = sink.dest_schema.as_deref().cloned();
    // Shared commit path (I1→I2→I7 + counters + journal + fault hooks).
    // write_sink_parts drains every part the sink produced — the final temp file
    // plus anything maybe_split rotated at max_file_size — so rotation can't drop.
    let parts = super::commit::write_sink_parts(
        dest,
        &mut sink,
        plan.validate.then_some(plan.format),
        |idx, count| super::commit::part_indexed_name(part_base, idx, count),
    )?;
    Ok(Some(KeysetPage {
        parts,
        rows,
        schema,
        // The source's own lossless token (Mongo BSON `_id`) when it reported
        // one, else the column-extracted string (every SQL engine).
        next_cursor: sink.effective_cursor(),
    }))
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
    // full export must NOT silently skip already-exported rows).
    let mut last: Option<String> = if kp.checkpoint {
        state
            .and_then(|s| s.get(&plan.export_name).ok())
            .and_then(|cs| cs.last_cursor_value)
    } else {
        None
    };

    // Round-5 (keyset checkpoint-resume manifest completeness — the sibling of the
    // chunked fix): a crash mid-keyset leaves pages durably committed (parquet +
    // file_log) with NO destination manifest; on resume the page loop continues from
    // the cursor and skips them, so finalize would write a manifest of ONLY this
    // run's pages, orphaning the pre-crash pages from the manifest-authoritative
    // loader. export_state now persists the in-progress run_id: REUSE it across
    // resumes so every page lives under ONE run_id in file_log, and reconstruct the
    // already-committed pages into this run's manifest. Cleared by the caller once
    // finalize writes the complete manifest.
    if kp.checkpoint
        && let Some(st) = state
    {
        match st.get_resume_run_id(&plan.export_name)? {
            Some(rid) => {
                summary.run_id = rid.clone();
                super::chunked::rehydrate_manifest_parts_from_file_log(st, &rid, summary)?;
            }
            None => st.set_resume_run_id(&plan.export_name, &summary.run_id)?,
        }
    }

    let mut pages: usize = 0;
    // Captured once from the first non-empty page for the post-run on_schema_drift
    // gate: keyset owns its runner (run_single_export early-returns here), so the
    // drift check single mode applies must be applied here too.
    let mut drift_schema: Option<arrow::datatypes::Schema> = None;

    // Destination + manifest-mode guard (Finding #44) + run-unique part stamp are
    // fixed for the whole run — hoisted out of the page loop. Millisecond stamp:
    // two runs into the same prefix must not clobber (run-unique part-name rule).
    let dest = destination::create_destination(&plan.destination)?;
    crate::manifest::guard_manifest_mode(dest.as_ref(), "batch")?;
    let ext = format::create_format(plan.format, plan.compression, plan.compression_level, None)
        .file_extension()
        .to_string();
    let stamp = chrono::Utc::now().format("%Y%m%d_%H%M%S_%3f").to_string();

    loop {
        let base = format!("{}_{}_keyset{}.{}", plan.export_name, stamp, pages, ext);
        let Some(page) = read_keyset_page(
            src,
            plan,
            &key_plan,
            kp.chunk_size,
            last.as_deref(),
            dest.as_ref(),
            &base,
        )?
        else {
            break;
        };

        // ADR-0012 M3: capture the dest schema fingerprint from the first
        // non-empty page; idempotent run-wide.
        if let Some(sc) = &page.schema {
            manifest_writer::record_run_schema_fingerprint(summary, sc);
            if drift_schema.is_none() {
                drift_schema = Some(sc.clone());
            }
        }
        summary.total_rows += page.rows as i64;
        if plan.validate {
            summary.validated = Some(true);
        }
        for rec in &page.parts {
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
        // Persist the high-water mark AFTER the parts are durably committed, so a
        // resume continues from committed data (peek→flush→ack). The crash window
        // between the commit and this line is at-least-once: the last page is
        // re-read (downstream dedup / reconcile absorbs it), never lost.
        if kp.checkpoint
            && let (Some(st), Some(v)) = (state, page.next_cursor.as_ref())
        {
            st.update(&plan.export_name, v)?;
        }
        // Fault point: page durably committed (parts + file_log + cursor advanced),
        // NO destination manifest yet — a crash here must be resume-recoverable
        // MANIFEST-DRIVEN (round-5): the resume rehydrates this page from file_log.
        crate::test_hook::maybe_panic_at(&format!("after_keyset_page:{pages}"));
        log::info!(
            "export '{}': keyset page {} — {} rows",
            plan.export_name,
            pages,
            page.rows
        );
        pages += 1;

        // A short page means the index range is exhausted — stop without an
        // extra empty round-trip.
        if page.rows < kp.chunk_size {
            break;
        }
        // Advance to the page's max key; if it could not be read (NULL or an
        // unsupported type), we must NOT loop on the same bound — that would
        // re-read the same page forever.
        match page.next_cursor {
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

    // on_schema_drift gate — run_single_export applies this, but keyset returns
    // through its own runner and never reached it, so an opted-in
    // `on_schema_drift: fail` silently returned exit 0 on a drifted schema for the
    // headline large-table path. Mirror single mode: compare the run's resolved
    // schema against the stored fingerprint once, post-run.
    if let (Some(sc), Some(st)) = (&drift_schema, state) {
        super::schema_drift::check_from_sink_schema(
            st,
            &plan.export_name,
            sc,
            plan.schema_drift_policy,
            summary,
        )?;
    }
    Ok(())
}
