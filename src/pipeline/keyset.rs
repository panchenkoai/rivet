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
    /// This page's sink's per-column Form B value checksums — XOR-combined
    /// run-wide by `run_keyset` so the finalize manifest records Form B (previously
    /// dropped here, making `rivet validate`'s re-read a no-op on keyset exports).
    pub(crate) column_checksums: std::collections::BTreeMap<String, u64>,
    /// The key-column name the checksums are keyed on (constant across pages).
    pub(crate) checksum_key_column: Option<String>,
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
    read_keyset_page_bounded(
        src, plan, key_plan, page_size, cursor, None, dest, part_base,
    )
}

/// [`read_keyset_page`] with an optional INCLUSIVE upper bound on the key — one
/// parallel keyset worker's `(cursor, upper]` range (feat/parallel-keyset). The
/// page becomes `WHERE key > cursor AND key <= upper ORDER BY key LIMIT n`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn read_keyset_page_bounded(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    key_plan: &IncrementalCursorPlan,
    page_size: usize,
    cursor: Option<&str>,
    upper: Option<&str>,
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
            .with_upper_bound(upper)
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
    let checksum_key_column = sink.checksum_key_col.and(sink.cursor_column.clone());
    Ok(Some(KeysetPage {
        parts,
        rows,
        schema,
        // The source's own lossless token (Mongo BSON `_id`) when it reported
        // one, else the column-extracted string (every SQL engine).
        next_cursor: sink.effective_cursor(),
        column_checksums: std::mem::take(&mut sink.column_checksums),
        checksum_key_column,
    }))
}

/// The 0-indexed ROW offset of the i-th of `parts` percentile boundaries over `total`
/// rows (i in 1..parts). Extracted pure so the boundary arithmetic is unit-mutation-
/// covered — a `*`/`/` slip here silently unbalances the ROW-percentile ranges.
fn percentile_offset(total: i64, i: usize, parts: usize) -> i64 {
    total * i as i64 / parts as i64
}

/// The run's `cursor_high` (forensics v18): the max key of the HIGHEST-index range that
/// produced data. Ranges partition the key ascending, so the last POPULATED range holds
/// the run's top key — walk from the top, skip empty ranges, take the first. Extracted
/// pure from the post-join merge so this fold is unit-mutation-covered (a dropped
/// `.rev()` would silently report the LOWEST range's max instead of the highest).
fn highest_range_max(range_maxes: Vec<Option<String>>) -> Option<String> {
    range_maxes.into_iter().rev().flatten().next()
}

/// "The single row at offset `off`" clause (after the `ORDER BY`), per dialect.
fn nth_row_clause(st: crate::config::SourceType, off: i64) -> String {
    use crate::config::SourceType::*;
    match st {
        Postgres | Mysql => format!("LIMIT 1 OFFSET {off}"),
        Mssql => format!("OFFSET {off} ROWS FETCH NEXT 1 ROWS ONLY"),
        Mongo => unreachable!("parallel keyset sampling is a SQL path; Mongo uses $sample"),
    }
}

/// Sample N−1 ROW-percentile boundaries of the keyset key: the key values at row
/// offsets `total*i/N`. A prototype uses `OFFSET` (an index-only skip, cheap to
/// ~10M rows; production would SAMPLE beyond that — dev/parallel_keyset/results.md).
/// Row-count parity is STRUCTURAL: the resulting half-open intervals partition the
/// key, so the union of ranges reads every row exactly once regardless of the
/// sample's balance. Fewer boundaries than requested (a repeated value at two
/// percentiles) just yields fewer, larger ranges — never a gap or an overlap.
fn sample_key_boundaries(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    key: &str,
    parts: usize,
    floor: Option<&str>,
    ceil: Option<&str>,
) -> Result<Vec<String>> {
    let st = plan.source.source_type;
    let base = &plan.base_query;
    let k = crate::sql::quote_ident(st, key);
    // Incremental (iteration 3): sample percentiles of only the NEW rows,
    // `(floor, ceil]`, as inline per-dialect literals (never a bind param).
    let mut preds: Vec<String> = Vec::new();
    if let Some(lo) = floor {
        preds.push(format!(
            "{k} > {}",
            crate::source::query::inline_literal(st, lo)
        ));
    }
    if let Some(hi) = ceil {
        preds.push(format!(
            "{k} <= {}",
            crate::source::query::inline_literal(st, hi)
        ));
    }
    let where_clause = if preds.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", preds.join(" AND "))
    };
    let total: i64 = src
        .query_scalar(&format!(
            "SELECT COUNT(*) FROM ({base}) AS _rivet_pk_cnt {where_clause}"
        ))?
        .as_deref()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0);
    if total <= 1 {
        return Ok(vec![]);
    }
    let mut bounds: Vec<String> = Vec::with_capacity(parts.saturating_sub(1));
    for i in 1..parts {
        let off = percentile_offset(total, i, parts);
        let nth = nth_row_clause(st, off);
        let sql =
            format!("SELECT {k} FROM ({base}) AS _rivet_pk {where_clause} ORDER BY {k} {nth}");
        if let Some(v) = src.query_scalar(&sql)?
            && bounds.last().map(String::as_str) != Some(v.as_str())
        {
            bounds.push(v);
        }
    }
    Ok(bounds)
}

/// Sanitize a run_id into a filename-safe token so it can key part names.
fn sanitize_run_id(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Sample the N ROW-percentile ranges for a FRESH parallel keyset run:
/// `(range_index, lo_exclusive, hi_inclusive, done=false)`. The N−1 boundaries
/// partition the key into half-open intervals whose union is the whole key space.
#[allow(clippy::type_complexity)]
fn sample_parallel_ranges(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    key: &str,
    parallel: usize,
    floor: Option<&str>,
    ceil: Option<&str>,
) -> Result<Vec<(usize, Option<String>, Option<String>, bool)>> {
    let bounds = sample_key_boundaries(src, plan, key, parallel, floor, ceil)?;
    // The first range's floor + the last range's ceiling come from the incremental
    // bounds (both None for a full pass): the first range seeks past `floor`, the
    // last stops at `ceil` so a row arriving DURING the run is deferred, not
    // double-counted (which keeps the anchor advance exact).
    let mut ranges = Vec::with_capacity(bounds.len() + 1);
    let mut prev: Option<String> = floor.map(str::to_string);
    for (i, b) in bounds.iter().enumerate() {
        ranges.push((i, prev.clone(), Some(b.clone()), false));
        prev = Some(b.clone());
    }
    let last = ranges.len();
    ranges.push((last, prev, ceil.map(str::to_string), false));
    Ok(ranges)
}

/// Parallel keyset (feat/parallel-keyset). N ROW-percentile-range workers seek
/// concurrently in a `std::thread::scope`; each owns its source connection and
/// runs the standard bounded seek loop, writing run-unique parts to the SHARED
/// destination. Rows / parts / Form-B checksums / the run schema fingerprint are
/// merged into `summary` after the join, through the same commit seam the
/// sequential runner uses. Row-count parity is structural (the ranges partition
/// the key); the live test asserts the union reads every row once.
///
/// With `chunk_checkpoint` (iteration 2) it does PER-RANGE crash-recovery: the
/// boundaries are sampled once and PERSISTED (`keyset_range`, keyed by run_id) so
/// a resume reloads the SAME ranges rather than re-sampling a possibly-changed
/// table. Each worker, at completion, atomically records its parts to `file_log`
/// AND flips its range `done=1`. A resume skips `done` ranges (rehydrating their
/// parts from `file_log`) and re-runs the rest from their `lo` — the run_id-based
/// part names make the re-run OVERWRITE the crashed range's partial parts rather
/// than accumulate duplicates. Without `chunk_checkpoint`, a fresh full pass.
fn run_keyset_parallel(
    src: &mut dyn Source,
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    key_plan: IncrementalCursorPlan,
    parallel: usize,
    state: Option<&StateStore>,
) -> Result<()> {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicI64, Ordering};

    let kp = keyset_plan(plan);
    let key = kp.key_column.clone();
    let page_size = kp.chunk_size;
    let checkpoint = kp.checkpoint;

    // Resume detection (checkpoint only): a surviving resume_run_id means a prior
    // parallel run of this export crashed. Reuse its run_id (so every worker's
    // file_log lives under ONE run_id, rehydratable) and RELOAD its persisted
    // ranges — re-sampling a changed table would move the boundaries and leave a
    // gap. A fresh run samples the ranges, persists them, and sets the anchor.
    let resume_run_id: Option<String> = if checkpoint {
        state
            .and_then(|s| s.get_resume_run_id(&plan.export_name).ok())
            .flatten()
    } else {
        None
    };

    // Incremental (iteration 3): a FRESH run seeks past the persisted anchor
    // (`floor`) up to the source max AT OPEN (`ceil`) — bounding the last range at
    // `ceil` defers a row arriving mid-run to the next run, so the anchor advance is
    // exact. A RESUME reloads its ranges (floor/ceil already baked in), so it must
    // NOT recompute. `key_advances` is numeric-aware so "no new rows" is not a
    // lexical "1000" < "999" mistake.
    let incremental = kp.incremental;
    let (floor, ceil): (Option<String>, Option<String>) = if incremental && resume_run_id.is_none()
    {
        let anchor = state
            .and_then(|s| s.get(&plan.export_name).ok())
            .and_then(|c| c.last_cursor_value);
        let key_q = crate::sql::quote_ident(plan.source.source_type, &key);
        let cur_max = src.query_scalar(&format!(
            "SELECT MAX({key_q}) FROM ({}) AS _rivet_pk_max",
            plan.base_query
        ))?;
        let advances = match (&anchor, &cur_max) {
            (_, None) => false,                       // empty source
            (None, Some(_)) => true,                  // no prior anchor → all rows new
            (Some(a), Some(c)) => key_advances(a, c), // c strictly past a
        };
        if !advances {
            log::info!(
                "export '{}': parallel keyset incremental — no new rows past the anchor, nothing to export",
                plan.export_name
            );
            return Ok(());
        }
        (anchor, cur_max)
    } else {
        (None, None)
    };
    let (floor_r, ceil_r) = (floor.as_deref(), ceil.as_deref());

    // ranges: (range_index, lo_exclusive, hi_inclusive, already_done)
    let ranges: Vec<(usize, Option<String>, Option<String>, bool)> = match (&resume_run_id, state) {
        (Some(rid), Some(st)) => {
            summary.run_id = rid.clone();
            summary.resumed = true;
            let rows = st.load_keyset_ranges(&plan.export_name, rid)?;
            if rows.is_empty() {
                // Anchor set but no persisted ranges (a crash between set_resume_
                // run_id and persist_keyset_ranges — nothing committed): re-sample
                // + persist under this run_id and start over. No skip.
                let fresh = sample_parallel_ranges(src, plan, &key, parallel, floor_r, ceil_r)?;
                st.persist_keyset_ranges(&plan.export_name, rid, &lo_hi_pairs(&fresh))?;
                fresh
            } else {
                rows.into_iter()
                    .map(|r| (r.range_index as usize, r.lo, r.hi, r.done))
                    .collect()
            }
        }
        (None, Some(st)) if checkpoint => {
            // Fresh checkpoint run: sample, persist the boundaries (all done=0),
            // THEN set the anchor. If a crash lands before the anchor, the next run
            // sees no resume_run_id and does a fresh full pass (persist replaces the
            // orphaned rows) — safe, never a skip.
            let fresh = sample_parallel_ranges(src, plan, &key, parallel, floor_r, ceil_r)?;
            st.persist_keyset_ranges(&plan.export_name, &summary.run_id, &lo_hi_pairs(&fresh))?;
            st.set_resume_run_id(&plan.export_name, &summary.run_id)?;
            fresh
        }
        _ => sample_parallel_ranges(src, plan, &key, parallel, floor_r, ceil_r)?,
    };

    // The anchor advance for incremental = the last range's ceiling (the source max
    // pinned at open). None for a full pass (last range's hi is None → no advance).
    let anchor_ceiling: Option<String> = ranges.last().and_then(|(_, _, hi, _)| hi.clone());
    // The first range's floor = the anchor this run continued PAST. Recovered from
    // the ranges (not the local `floor`, which is None on a resume — the incremental
    // bound block is skipped there) so a RESUMED incremental run reports the accurate
    // manifest cursor range `(floor, ceil]`, not `(None, ceil]` (M6, #72 contract).
    let anchor_floor: Option<String> = ranges.first().and_then(|(_, lo, _, _)| lo.clone());

    let total_ranges = ranges.len();
    let pending: Vec<(usize, Option<String>, Option<String>)> = ranges
        .into_iter()
        .filter(|(_, _, _, done)| !done)
        .map(|(idx, lo, hi, _)| (idx, lo, hi))
        .collect();

    // Fan-out collapse: `parallel: N` was requested but the sampler produced ONE
    // range — the headline speed-up is silently absent. The usual cause is a key
    // type the boundary probe cannot render (e.g. a source that returns the key as
    // an unhandled type from query_scalar). warn, not info, so it is visible.
    if parallel > 1 && total_ranges == 1 {
        log::warn!(
            "export '{}': parallel keyset requested {} workers but sampled 0 boundaries — \
             running as a SINGLE worker. The key may be a type the boundary probe cannot \
             read; data is complete but the parallel speed-up is absent.",
            plan.export_name,
            parallel
        );
    }

    log::info!(
        "export '{}': parallel keyset — {} range(s), {} to run{}, page size {}",
        plan.export_name,
        total_ranges,
        pending.len(),
        if resume_run_id.is_some() {
            " (resume)"
        } else {
            ""
        },
        page_size
    );

    let dest = std::sync::Arc::new(destination::create_destination(&plan.destination)?);
    crate::manifest::guard_manifest_mode(&**dest, "batch")?;

    let ext = format::create_format(plan.format, plan.compression, plan.compression_level, None)
        .file_extension()
        .to_string();
    // Part names key off the run_id, not a wall-clock stamp: unique per fresh run
    // AND stable across a resume, so a re-run range's parts OVERWRITE its crashed
    // partial parts (idempotent) instead of accumulating duplicates.
    let run_tag = sanitize_run_id(&summary.run_id);
    let run_id = summary.run_id.clone();
    // Workers commit to keyset_range + file_log ONLY on a checkpoint run — a
    // non-checkpoint run persists no ranges (the `_ =>` sample arm), so letting its
    // workers run the `done=1` UPDATE would flip a LEFTOVER checkpoint set's rows
    // under a foreign run_id (H1 silent-loss). Gating state_ref on `checkpoint`
    // matches the "checkpoint runs only" contract the worker commit documents.
    let state_ref = if checkpoint {
        state.map(|s| s.state_ref().clone())
    } else {
        None
    };
    let fmt_label = plan.format.label();
    let cmp_label = plan.compression.label();

    let rows = AtomicI64::new(0);
    let parts_mx: Mutex<Vec<super::commit::PartRecord>> = Mutex::new(Vec::new());
    #[allow(clippy::type_complexity)]
    let checksums_mx: Mutex<Vec<(std::collections::BTreeMap<String, u64>, Option<String>)>> =
        Mutex::new(Vec::new());
    let fingerprint: std::sync::OnceLock<arrow::datatypes::Schema> = std::sync::OnceLock::new();
    // Per-range high-water key, indexed by range_index (done ranges stay None —
    // they are not re-run). cursor_high = the highest populated range's max; on a
    // RESUME this reflects the RE-RUN ranges only (a range already `done` pre-crash
    // is skipped), which is acceptable — parallel keyset is a full snapshot, not an
    // incremental anchor, so its cursor range is descriptive, not a resume floor.
    let range_max: Mutex<Vec<Option<String>>> = Mutex::new(vec![None; total_ranges]);
    let errors: Mutex<Vec<String>> = Mutex::new(Vec::new());

    std::thread::scope(|scope| {
        for (ridx, lo, hi) in pending.iter().cloned() {
            let dest = std::sync::Arc::clone(&dest);
            let (plan_r, key_plan_r, ext_r, tag_r, key_r) =
                (plan, &key_plan, &ext, run_tag.as_str(), key.as_str());
            let (rows_r, parts_r, checks_r, fp_r, rmax_r, errs_r) = (
                &rows,
                &parts_mx,
                &checksums_mx,
                &fingerprint,
                &range_max,
                &errors,
            );
            let (sref_r, rid_r, fmt_r, cmp_r) = (&state_ref, run_id.as_str(), fmt_label, cmp_label);
            scope.spawn(move || {
                let mut wsrc = match source::create_source(&plan_r.source) {
                    Ok(s) => s,
                    Err(e) => {
                        errs_r
                            .lock()
                            .unwrap()
                            .push(format!("range {ridx}: connect: {e:#}"));
                        return;
                    }
                };
                let mut cursor = lo;
                let mut pages = 0usize;
                let mut rmax: Option<String> = None;
                // Parts this range committed — recorded to file_log atomically with
                // its `done` flip at completion (checkpoint only).
                let mut range_parts: Vec<crate::state::KeysetRangePart> = Vec::new();
                let mut local_parts: Vec<super::commit::PartRecord> = Vec::new();
                let mut local_checks: Vec<(
                    std::collections::BTreeMap<String, u64>,
                    Option<String>,
                )> = Vec::new();
                loop {
                    // Test-only: simulate a per-worker SQL error mid-range (Err path,
                    // not a crash). The worker records it + returns; the post-join check
                    // bails, so the run fails cleanly with no _SUCCESS / finalized manifest.
                    if let Err(e) = crate::test_hook::maybe_error_at_index(
                        "keyset_parallel_worker",
                        ridx as i64,
                    ) {
                        errs_r.lock().unwrap().push(format!("range {ridx}: {e}"));
                        return;
                    }
                    let base = format!(
                        "{}_{}_pk_w{}_{}.{}",
                        plan_r.export_name, tag_r, ridx, pages, ext_r
                    );
                    let page = match read_keyset_page_bounded(
                        &mut *wsrc,
                        plan_r,
                        key_plan_r,
                        page_size,
                        cursor.as_deref(),
                        hi.as_deref(),
                        &**dest,
                        &base,
                    ) {
                        Ok(p) => p,
                        Err(e) => {
                            errs_r
                                .lock()
                                .unwrap()
                                .push(format!("range {ridx}: page {pages}: {e:#}"));
                            return;
                        }
                    };
                    let Some(page) = page else { break };
                    rows_r.fetch_add(page.rows as i64, Ordering::Relaxed);
                    if let Some(sc) = &page.schema {
                        let _ = fp_r.set(sc.clone());
                    }
                    rmax = page.next_cursor.clone().or(rmax);
                    for p in &page.parts {
                        range_parts.push(crate::state::KeysetRangePart {
                            file_name: p.file_name.clone(),
                            rows: p.rows,
                            bytes: p.bytes as i64,
                        });
                    }
                    local_parts.extend(page.parts);
                    local_checks.push((page.column_checksums, page.checksum_key_column));
                    let last_page = page.rows < page_size;
                    if !last_page {
                        match page.next_cursor {
                            Some(v) => cursor = Some(v),
                            None => {
                                errs_r.lock().unwrap().push(format!(
                                    "range {ridx}: could not advance the '{key_r}' cursor at page \
                                     {pages} (NULL or unsupported type)"
                                ));
                                return;
                            }
                        }
                    }
                    pages += 1;
                    if last_page {
                        break;
                    }
                }
                // Atomic checkpoint: the range's parts → file_log AND `done=1` in one
                // transaction (checkpoint runs only). A crash before this leaves the
                // range `done=0` with no file_log rows — re-read on resume.
                if let Some(sref) = sref_r
                    && let Err(e) = crate::state::StateStore::commit_keyset_range_at_ref(
                        sref,
                        rid_r,
                        &plan_r.export_name,
                        ridx as i64,
                        &range_parts,
                        fmt_r,
                        Some(cmp_r),
                    )
                {
                    errs_r
                        .lock()
                        .unwrap()
                        .push(format!("range {ridx}: checkpoint commit: {e:#}"));
                    return;
                }
                // Crash simulation: this range is now durably `done` in the state DB,
                // but the run has NOT finalized — a resume must skip it (rehydrate its
                // parts) and re-run only the ranges that never reached here.
                crate::test_hook::maybe_exit_at_index(
                    "keyset_parallel_range_committed",
                    ridx as i64,
                );
                // Publish to the shared merge state ONLY after the checkpoint commits,
                // so a failed commit does not leave half-merged summary state.
                rmax_r.lock().unwrap()[ridx] = rmax;
                parts_r.lock().unwrap().extend(local_parts);
                checks_r.lock().unwrap().extend(local_checks);
            });
        }
    });

    let errs = errors.into_inner().unwrap();

    // Record what the SUCCESSFUL workers already made durable — BEFORE deciding
    // whether to bail. `summary.files_committed` is the retry guard's only input
    // (`decide_export_retry` -> `BailDuplicateGuard`, pipeline/single.rs), and
    // `record_part` is the sole production site that raises it. Bailing above
    // this loop left the guard reading ZERO while ranges were already on disk,
    // so a TRANSIENT worker failure retried the whole export over durable parts.
    //
    // Measured before the fix: worker 2 failed transiently, 4 parts were on
    // disk, rivet retried twice ("retry 1/2", "retry 2/2"). Stable run_id part
    // names usually make attempt N+1 overwrite attempt N, which is why this hid
    // since 0.23.0 — but when a range's output SHRINKS between attempts (rows
    // deleted concurrently, or a part-count drop) the extra part of the earlier
    // attempt survives as an orphan: a 2000-row fixture with a mid-backoff
    // DELETE produced 751 rows / 750 distinct — id 501 duplicated, sitting in
    // both `..._w0_1.parq` (attempt 1, unoverwritten) and `..._w1_0.parq`.
    //
    // Recording on the failure path is also the truthful thing: the run
    // finalizes a Failed manifest, and listing the durable debris is what makes
    // it discoverable to validate/gc instead of unreferenced.
    // Records through the commit seam (populates summary.manifest_parts +
    // counters + journal). On a CHECKPOINT run the workers ALREADY wrote
    // file_log atomically with their `done` flip, so pass None to avoid a
    // duplicate write. On a NON-checkpoint run the workers write no file_log
    // (their commit_keyset_range_at_ref is gated on state_ref = checkpoint-only),
    // so the merge must write it — matching the sequential keyset path.
    let file_log_state = if checkpoint { None } else { state };
    let parts = parts_mx.into_inner().unwrap();
    for (idx, rec) in parts.iter().enumerate() {
        super::commit::record_part(
            plan,
            summary,
            file_log_state,
            rec,
            super::commit::PartKind::Page {
                page_index: idx as i64,
            },
        );
    }
    summary.total_rows += rows.into_inner();

    if !errs.is_empty() {
        anyhow::bail!(
            "export '{}': parallel keyset failed on {} range(s): {}",
            plan.export_name,
            errs.len(),
            errs.join("; ")
        );
    }

    // Merge into the summary through the shared seams (identical to the sequential
    // runner's per-page path, folded run-wide).
    if plan.validate {
        summary.validated = Some(true);
    }
    if let Some(sc) = fingerprint.get() {
        manifest_writer::record_run_schema_fingerprint(summary, sc);
    }
    // cursor_high = the highest populated range's max (forensics v18); see range_max.
    summary.cursor_high = highest_range_max(range_max.into_inner().unwrap());
    summary.cursor_low = None; // a fresh parallel run seeks from the floor of each range

    // Resume completeness: reconstruct the parts of the ranges that completed in a
    // PRIOR (crashed) run — they were not re-run, so they are absent from
    // `parts_mx`; file_log (under the reused run_id) is their record. rehydrate
    // dedupes against the parts just recorded, so a fresh run (all ranges re-run
    // this pass) is a no-op here.
    if let Some(st) = state
        && checkpoint
    {
        super::chunked::rehydrate_manifest_parts_from_file_log(st, &run_id, summary)?;
    }
    // Form B: XOR-combine every worker's per-page column checksums run-wide.
    let mut acc: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    let mut ck_key: Option<String> = None;
    for (m, k) in checksums_mx.into_inner().unwrap() {
        super::commit::accumulate_column_checksums(&mut acc, &m);
        if ck_key.is_none() {
            ck_key = k;
        }
    }
    super::commit::harvest_column_checksums(summary, acc, ck_key);

    log::info!(
        "export '{}': parallel keyset complete — {} range(s), {} parts, {} rows",
        plan.export_name,
        total_ranges,
        parts.len(),
        summary.total_rows
    );

    // on_schema_drift gate — the SAME post-run check the sequential runner applies
    // (mirror single mode). Without this, `on_schema_drift: fail` would silently
    // return exit 0 on a drifted schema on the parallel path — the runner-bypass
    // class. The workers converge on ONE run schema (fingerprint), so the check is
    // run-wide, not per-worker.
    if let (Some(sc), Some(st)) = (fingerprint.get(), state) {
        super::schema_drift::check_from_sink_schema(
            st,
            &plan.export_name,
            sc,
            plan.schema_drift_policy,
            summary,
        )?;
    }

    // Incremental (iteration 3): on CLEAN SUCCESS advance the persisted anchor to
    // this run's ceiling (the source max pinned at open), so the next run seeks
    // strictly past it. Also pin the manifest cursor range to the ACCURATE
    // `[floor, ceil]` — vs `worker_max` which a resume under-reports (done ranges
    // are skipped, so their max is not re-observed). This is the incremental anchor
    // that iteration 2's cursor_high caveat deferred.
    if incremental && let Some(hi) = &anchor_ceiling {
        summary.cursor_high = Some(hi.clone());
        summary.cursor_low = anchor_floor.clone();
        if let Some(st) = state {
            st.update(&plan.export_name, hi)?;
        }
    }
    Ok(())
}

/// True when `candidate` advances strictly past `anchor` under cursor ordering —
/// numeric-aware (i128 then f64, exact past f64's 2^53 mantissa) with a byte-wise
/// string fallback for UUIDs / RFC3339 timestamps. Mirrors `progression::
/// cursor_advances`; used to decide whether an incremental parallel run has any
/// new rows past the anchor (a lexical compare would misread "1000" < "999").
fn key_advances(anchor: &str, candidate: &str) -> bool {
    if let (Ok(a), Ok(b)) = (anchor.parse::<i128>(), candidate.parse::<i128>()) {
        return b > a;
    }
    if let (Ok(a), Ok(b)) = (anchor.parse::<f64>(), candidate.parse::<f64>())
        && let Some(ord) = b.partial_cmp(&a)
    {
        return ord.is_gt();
    }
    candidate > anchor
}

/// The `(lo, hi)` pairs of a sampled range list, for `persist_keyset_ranges`.
fn lo_hi_pairs(
    ranges: &[(usize, Option<String>, Option<String>, bool)],
) -> Vec<(Option<String>, Option<String>)> {
    ranges
        .iter()
        .map(|(_, lo, hi, _)| (lo.clone(), hi.clone()))
        .collect()
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

    // Parallel keyset (feat/parallel-keyset): N ROW-percentile-range workers seek
    // concurrently. `chunk_checkpoint` → per-range crash-recovery (iteration 2);
    // `keyset_incremental` → seek past the persisted anchor + advance it at success
    // (iteration 3); neither → a fresh full pass (iteration 1).
    if kp.parallel > 1 {
        return run_keyset_parallel(src, plan, summary, key_plan, kp.parallel, state);
    }

    log::info!(
        "export '{}': keyset (seek) pagination on '{}', page size {}",
        plan.export_name,
        kp.key_column,
        kp.chunk_size
    );

    // CRASH-RECOVERY vs INCREMENTAL — two distinct reasons to continue from the
    // last committed key, kept SEPARATE so a clean re-run of a mutable table can
    // never silently skip already-exported rows:
    //
    //   * Crash-recovery (`chunk_checkpoint`): the prior run died mid-stream, so
    //     its in-progress run_id (set at open below, cleared at finalize) is still
    //     present. Continuing from its last committed key picks up exactly where it
    //     stopped — resuming already-committed data can never skip a row.
    //   * Incremental (`keyset_incremental`): an append-only opt-in — a CLEAN
    //     re-run pulls only keys past the high-water mark.
    //
    // A clean re-run (prior run finished → run_id cleared) WITHOUT the incremental
    // opt-in loads no cursor and re-reads the whole range (full/snapshot
    // semantics). This is the crash-recovery ⇄ incremental split (ADR: keyset
    // checkpoint no longer implies incremental-by-key).
    let resume_run_id: Option<String> = if kp.checkpoint {
        match state {
            Some(st) => st.get_resume_run_id(&plan.export_name)?,
            None => None,
        }
    } else {
        None
    };
    let recovering_crash = resume_run_id.is_some();
    // Surface the recovery in the run's own metrics/log line: a resume-hit is the
    // tell that the prior run crashed (a flaky-link diagnosis signal).
    summary.resumed = recovering_crash;

    let mut last: Option<String> = if kp.checkpoint && (recovering_crash || kp.incremental) {
        state
            .and_then(|s| s.get(&plan.export_name).ok())
            .and_then(|cs| cs.last_cursor_value)
    } else {
        None
    };
    // Forensics (v18): a resume's lower bound is the checkpoint it continues from
    // (None on a fresh run — keyset seeks forward from the start). cursor_high (the
    // max reached) is set at the loop exits below.
    summary.cursor_low = last.clone();

    // Round-5 (keyset checkpoint-resume manifest completeness — the sibling of the
    // chunked fix): a crash mid-keyset leaves pages durably committed (parquet +
    // file_log) with NO destination manifest; on resume the page loop continues from
    // the cursor and skips them, so finalize would write a manifest of ONLY this
    // run's pages, orphaning the pre-crash pages from the manifest-authoritative
    // loader. export_state persists the in-progress run_id: REUSE it across resumes
    // so every page lives under ONE run_id in file_log, and reconstruct the
    // already-committed pages into this run's manifest. A first/clean run has no
    // in-progress run_id → record a fresh one. Cleared by the caller once finalize
    // writes the complete manifest.
    if kp.checkpoint
        && let Some(st) = state
    {
        match &resume_run_id {
            Some(rid) => {
                summary.run_id = rid.clone();
                super::chunked::rehydrate_manifest_parts_from_file_log(st, rid, summary)?;
            }
            None => {
                // FRESH run. For crash-recovery-only (non-incremental) keyset, null
                // the persisted high-water mark FIRST: it may still hold a prior
                // COMPLETED run's final key, and if this fresh run crashes before
                // its first page commits, the recovery run would load that stale
                // key as this run's resume point and skip the entire table. Tying
                // the cursor to this run makes a pre-first-commit crash re-read from
                // the start. Incremental deliberately keeps it (that IS the point).
                if !kp.incremental {
                    st.clear_cursor_value(&plan.export_name)?;
                }
                st.set_resume_run_id(&plan.export_name, &summary.run_id)?;
            }
        }
    }

    // Fault point: a fresh run has opened (resume_run_id set, cursor cleared for
    // non-incremental) but committed NO page yet. A crash here must, on the next
    // run, re-read from the START — never resume from a prior completed run's
    // stale high-water mark (the silent whole-table skip the cursor clear fixes).
    crate::test_hook::maybe_panic_at("keyset_after_open_before_first_page");

    let mut pages: usize = 0;
    // Captured once from the first non-empty page for the post-run on_schema_drift
    // gate: keyset owns its runner (run_single_export early-returns here), so the
    // drift check single mode applies must be applied here too.
    let mut drift_schema: Option<arrow::datatypes::Schema> = None;
    // Form B: XOR-combine each page's per-column checksums run-wide (order-
    // independent), then harvest once after the loop — so the finalize manifest
    // records Form B and `rivet validate` can re-verify keyset exports too.
    let mut checksums_acc: std::collections::BTreeMap<String, u64> =
        std::collections::BTreeMap::new();
    let mut checksum_key_column: Option<String> = None;

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
            // No further rows (the seek past the last full page came back empty):
            // the last advanced key is the run's high-water. This is the OTHER exit
            // from the short-page break below — a table whose size is an exact
            // multiple of chunk_size leaves via here, so cursor_max must be set on
            // both paths or an exact-fit keyset records no max.
            summary.cursor_high = last.clone();
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
        // Form B: fold this page's checksums into the run-wide XOR accumulator.
        super::commit::accumulate_column_checksums(&mut checksums_acc, &page.column_checksums);
        if checksum_key_column.is_none() {
            checksum_key_column = page.checksum_key_column.clone();
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
            // Forensics (v18): the final page's max key is the run's true high-water.
            // Record it BEFORE breaking — the loop stops without advancing `last`, so
            // a short tail page (e.g. the 3 u64 ids above i64::MAX) is captured yet
            // would otherwise be invisible in cursor_max. `.or(last)` covers an EMPTY
            // final page, whose max is the previous full page's key.
            summary.cursor_high = page.next_cursor.clone().or_else(|| last.clone());
            break;
        }
        // Advance to the page's max key; if it could not be read (NULL or an
        // unsupported type), we must NOT loop on the same bound — that would
        // re-read the same page forever.
        match page.next_cursor {
            Some(v) => last = Some(v),
            None => {
                // Failure forensics (v18): stamp the LAST key we did read — the
                // boundary just before the unadvanceable row. With `cursor_high`
                // (the table's max key) this brackets the value that broke
                // advancing (e.g. a u64 in the zone above i64::MAX), so a failed
                // `export_metrics` row explains itself without the source.
                summary.offending_value = last.clone();
                summary.cursor_high = last.clone();
                anyhow::bail!(
                    "export '{}': keyset could not read the '{}' value from the last row of page {} \
                     (NULL or unsupported type) — cannot advance safely (last readable key: {}). \
                     The key must be NOT NULL and one of: integer, float, string, timestamp, date, uuid.",
                    plan.export_name,
                    kp.key_column,
                    pages - 1,
                    last.as_deref().unwrap_or("<none>"),
                );
            }
        }
    }

    // Form B: record the run-wide XOR-combined checksums so finalize writes them.
    super::commit::harvest_column_checksums(summary, checksums_acc, checksum_key_column);

    // DATA COMPLETE: the page loop exhausted the key range, so there is no
    // uncommitted work left to resume — for a NON-INCREMENTAL run, clear the
    // in-progress run_id NOW, BEFORE the post-data gates (schema-drift below, and
    // the quality gate in job.rs). A gate that fails AFTER all data is durable must
    // not leave a resume anchor, or the operator's intended full re-run would be
    // treated as a crash-recovery and continue from the high-water mark, silently
    // skipping rows updated since (the crash-recovery/incremental split's raison
    // d'être).
    //
    // INCREMENTAL is gated OUT (`!kp.incremental`, mirroring the fresh-run
    // clear_cursor_value above): its next run continues from the high-water mark
    // regardless of the anchor, so clearing it yields NO benefit — and clearing it
    // HERE, before finalize_manifest writes the destination manifest, would strand
    // this run's committed pages. A crash in the [clear → finalize] window would
    // then leave a run whose parquet is on the destination + file_log but referenced
    // by NO manifest: the next incremental run reads only keys past the high-water
    // mark (0 new rows) and never rehydrates those parts, so the manifest-
    // authoritative loader silently drops them. The anchor must survive until
    // finalize for the incremental path (job.rs clears it AFTER the manifest write).
    if kp.checkpoint
        && !kp.incremental
        && let Some(st) = state
    {
        st.clear_resume_run_id(&plan.export_name)?;
    }

    // Fault point: data is fully committed (and, for a non-incremental run, the
    // resume anchor is cleared), but a post-data gate / late failure has not yet
    // run. For non-incremental a crash here must leave NO anchor (next run is a
    // fresh full pass); for incremental the anchor must SURVIVE (next run rehydrates
    // the committed pages rather than orphaning them).
    crate::test_hook::maybe_panic_at("keyset_after_data_complete");

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SourceType;

    // ── highest_range_max: cursor_high = the top populated range's max ────────
    #[test]
    fn highest_range_max_takes_the_top_populated_range() {
        let s = |x: &str| Some(x.to_string());
        // ascending ranges, all populated → the last one's max.
        assert_eq!(highest_range_max(vec![s("k1"), s("k2"), s("k3")]), s("k3"));
        // the top range is EMPTY → fall back to the highest populated below it.
        assert_eq!(highest_range_max(vec![s("k1"), s("k2"), None]), s("k2"));
        // a GAP: range 1 empty, range 2 populated → range 2 wins, NOT range 0. This is
        // the case that pins `.rev()` — without it the fold would return "lo".
        assert_eq!(highest_range_max(vec![s("lo"), None, s("hi")]), s("hi"));
        // all empty → None.
        assert_eq!(highest_range_max(vec![None, None]), None);
        assert_eq!(highest_range_max(vec![]), None);
    }

    // ── percentile_offset: the ROW-percentile boundary arithmetic ────────────
    #[test]
    fn percentile_offset_partitions_evenly() {
        // 4 workers over 1000 rows → boundaries at 250 / 500 / 750.
        assert_eq!(percentile_offset(1000, 1, 4), 250);
        assert_eq!(percentile_offset(1000, 2, 4), 500);
        assert_eq!(percentile_offset(1000, 3, 4), 750);
        // A `*`→`/` slip is invisible at i=1 (1000*1/4 == 1000/1/4) but not at i=2,
        // and `/`→`*`/`%` and `*`→`+` all diverge at i=1 — both cases pinned above.
        assert_eq!(percentile_offset(999, 1, 3), 333);
        assert_eq!(percentile_offset(999, 2, 3), 666);
    }

    // ── nth_row_clause: per-dialect single-row-at-offset clause ───────────────
    #[test]
    fn nth_row_clause_is_per_dialect() {
        assert_eq!(
            nth_row_clause(SourceType::Postgres, 250),
            "LIMIT 1 OFFSET 250"
        );
        assert_eq!(nth_row_clause(SourceType::Mysql, 250), "LIMIT 1 OFFSET 250");
        assert_eq!(
            nth_row_clause(SourceType::Mssql, 250),
            "OFFSET 250 ROWS FETCH NEXT 1 ROWS ONLY"
        );
    }

    // ── sanitize_run_id: filename-safe token for part names ──────────────────
    #[test]
    fn sanitize_run_id_keeps_safe_chars_and_replaces_the_rest() {
        // alnum, '-', '_' survive; everything else becomes '_'.
        assert_eq!(sanitize_run_id("run-2026_01A9"), "run-2026_01A9");
        assert_eq!(sanitize_run_id("a/b c:d.e"), "a_b_c_d_e");
        assert_eq!(sanitize_run_id("../etc"), "___etc");
        // A `||`→`&&` slip in the keep-predicate would drop alnum too — pinned by the
        // all-safe case round-tripping unchanged.
        assert_eq!(sanitize_run_id("ABCabc012"), "ABCabc012");
    }

    // ── key_advances: numeric-aware strictly-past-anchor compare ─────────────
    #[test]
    fn key_advances_is_numeric_not_lexical() {
        // Numeric: "1000" advances past "999" (a lexical compare would say no).
        assert!(key_advances("999", "1000"));
        assert!(!key_advances("1000", "999"));
        assert!(!key_advances("5", "5")); // equal is NOT an advance (strict >)
        // Unsigned above i64::MAX still compares as i128.
        assert!(key_advances("18446744073709551614", "18446744073709551615"));
        // Float fallback.
        assert!(key_advances("1.5", "2.0"));
        assert!(!key_advances("2.0", "1.5"));
        // String fallback (UUID / RFC3339): byte-wise.
        assert!(key_advances("2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"));
        assert!(!key_advances(
            "2026-01-02T00:00:00Z",
            "2026-01-01T00:00:00Z"
        ));
    }

    // ── lo_hi_pairs: project (lo, hi) out of a sampled range list ────────────
    #[test]
    fn lo_hi_pairs_projects_the_bounds() {
        let ranges = vec![
            (0usize, None, Some("k0500".to_string()), false),
            (
                1,
                Some("k0500".to_string()),
                Some("k1000".to_string()),
                false,
            ),
            (2, Some("k1000".to_string()), None, false),
        ];
        assert_eq!(
            lo_hi_pairs(&ranges),
            vec![
                (None, Some("k0500".to_string())),
                (Some("k0500".to_string()), Some("k1000".to_string())),
                (Some("k1000".to_string()), None),
            ]
        );
    }
}
