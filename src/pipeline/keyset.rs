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

use super::{RunSummary, sink::ExportSink};
use crate::config::IncrementalCursorMode;
use crate::destination;
use crate::error::Result;
use crate::plan::{ExtractionStrategy, IncrementalCursorPlan, KeysetPlan, ResolvedRunPlan};
use crate::source::{self, Source};
use crate::state::StateStore;
use crate::types::CursorState;

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
    /// First observed key of this page (the run floor when it is page 1 of
    /// range 0) — recorded so cursor_min lands in the metrics (#151).
    pub(crate) first_cursor: Option<String>,
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
        first_cursor: sink.first_cursor_value.clone(),
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
pub(crate) fn sample_key_boundaries(
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

/// The run-unique tag used INSIDE a keyset part name — the sanitized run_id
/// with a redundant leading `<export>_` stripped.
///
/// The production run_id is `<export>_<ms-stamp>` (job.rs), and the part-name
/// format already prepends `<export>_`, so using the raw run_id produced
/// `<export>_<export>_<stamp>_pk_w...` — the export name TWICE (field-run
/// observation). The chunked and mongo-parallel siblings key their middle
/// segment off a fresh stamp, not the run_id, so they never doubled; keyset was
/// the odd one out. Stripping only a PRESENT `<export>_` prefix keeps run-
/// uniqueness (the ms stamp survives) and leaves a bare/custom run_id (e.g. a
/// synthetic `run-1`) untouched.
fn run_scoped_tag(run_id: &str, export_name: &str) -> String {
    let tag = sanitize_run_id(run_id);
    let prefix = format!("{}_", sanitize_run_id(export_name));
    tag.strip_prefix(&prefix).unwrap_or(&tag).to_string()
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
    Ok(partition_ranges(&bounds, floor, ceil))
}

/// Pure partitioning half of [`sample_parallel_ranges`] (#161): fold N−1 sampled
/// boundaries into N half-open `(lo_exclusive, hi_inclusive]` ranges whose union
/// is exactly the `(floor, ceil]` key space — gap-free and overlap-free BY
/// CONSTRUCTION (each range's `lo` IS the previous range's `hi`), which the
/// property test asserts rather than trusts. The first range's floor + the last
/// range's ceiling come from the incremental bounds (both None for a full pass):
/// the first range seeks past `floor`, the last stops at `ceil` so a row
/// arriving DURING the run is deferred, not double-counted (which keeps the
/// anchor advance exact).
#[allow(clippy::type_complexity)]
fn partition_ranges(
    bounds: &[String],
    floor: Option<&str>,
    ceil: Option<&str>,
) -> Vec<(usize, Option<String>, Option<String>, bool)> {
    let mut ranges = Vec::with_capacity(bounds.len() + 1);
    let mut prev: Option<String> = floor.map(str::to_string);
    for (i, b) in bounds.iter().enumerate() {
        ranges.push((i, prev.clone(), Some(b.clone()), false));
        prev = Some(b.clone());
    }
    let last = ranges.len();
    ranges.push((last, prev, ceil.map(str::to_string), false));
    ranges
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
/// A stable, filename-safe tag for a keyset page's SEEK cursor — the sequential-checkpoint part
/// name keys off it so a resume re-reading from the SAME seek OVERWRITES its rehydrated part
/// (idempotent) instead of writing a differently-named duplicate. `None` (the first page's seek)
/// → "start". A key VALUE → a FNV-1a hash (stable across rivet versions, unlike std's SipHash, so
/// a resume — possibly a newer binary — reproduces the same tag for the same seek).
fn seek_tag(seek: Option<&str>) -> String {
    match seek {
        None => "start".to_string(),
        Some(v) => {
            let mut h: u64 = 0xcbf29ce484222325;
            for b in v.as_bytes() {
                h ^= u64::from(*b);
                h = h.wrapping_mul(0x0000_0100_0000_01b3);
            }
            format!("{h:016x}")
        }
    }
}

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

    let (dest, ext) = super::frame::RunnerFrame::open_shared(plan)?;
    // Part names key off the run_id, not a wall-clock stamp: unique per fresh run
    // AND stable across a resume, so a re-run range's parts OVERWRITE its crashed
    // partial parts (idempotent) instead of accumulating duplicates.
    let run_tag = run_scoped_tag(&summary.run_id, &plan.export_name);
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
    // ADR-0029: both accumulators carry the RANGE index — the commit unit this
    // runner publishes checksums at. Parts are published per PAGE (durability
    // must reflect what is on disk, #200-1) while checksums are published per
    // committed RANGE, so the range is the only id the two can agree on, and
    // the seam needs them keyed alike to compute Form-B coverage.
    #[allow(clippy::type_complexity)]
    let parts_mx: Mutex<Vec<(usize, super::commit::PartRecord)>> = Mutex::new(Vec::new());
    #[allow(clippy::type_complexity)]
    let checksums_mx: Mutex<
        Vec<(
            usize,
            std::collections::BTreeMap<String, u64>,
            Option<String>,
        )>,
    > = Mutex::new(Vec::new());
    let fingerprint: std::sync::OnceLock<arrow::datatypes::Schema> = std::sync::OnceLock::new();
    // Per-range high-water key, indexed by range_index (done ranges stay None —
    // they are not re-run). cursor_high = the highest populated range's max; on a
    // RESUME this reflects the RE-RUN ranges only (a range already `done` pre-crash
    // is skipped), which is acceptable — parallel keyset is a full snapshot, not an
    // incremental anchor, so its cursor range is descriptive, not a resume floor.
    let range_max: Mutex<Vec<Option<String>>> = Mutex::new(vec![None; total_ranges]);
    let range_first: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);
    let errors: Mutex<Vec<String>> = Mutex::new(Vec::new());

    // #152: the OPT-2 concurrency governor, previously chunked-only. Each worker
    // acquires a permit PER PAGE (the guard releases at each iteration's end, on
    // every path), so shrinking the ceiling sheds workers at page granularity —
    // the same shape the range-chunked runner uses at chunk granularity. `finished`
    // counts workers that have exited (success OR error) so the governor's exit
    // predicate can't be stranded by a failing worker.
    let semaphore = crate::resource::Semaphore::new(parallel.max(1));
    let finished = std::sync::atomic::AtomicUsize::new(0);
    // OPT-2 adaptive concurrency governor — the SHARED seam (identical wiring in the chunked
    // runner; #152). arm → spawn_into (in the scope) → drain_into (post-scope, before any bail).
    let governor = crate::pipeline::governor::GovernorHarness::arm(plan, parallel);

    std::thread::scope(|scope| {
        // Governor thread (shared seam): resizes the permit semaphore within [floor, ceiling],
        // self-terminating once every worker has FINISHED (success OR failure).
        governor.spawn_into(
            scope,
            &semaphore,
            &finished,
            pending.len(),
            &plan.export_name,
        );

        for (ridx, lo, hi) in pending.iter().cloned() {
            let dest = std::sync::Arc::clone(&dest);
            let (plan_r, key_plan_r, ext_r, tag_r, key_r) =
                (plan, &key_plan, &ext, run_tag.as_str(), key.as_str());
            let rfirst_r = &range_first;
            let (rows_r, parts_r, checks_r, fp_r, rmax_r, errs_r) = (
                &rows,
                &parts_mx,
                &checksums_mx,
                &fingerprint,
                &range_max,
                &errors,
            );
            let (sref_r, rid_r, fmt_r, cmp_r) = (&state_ref, run_id.as_str(), fmt_label, cmp_label);
            let sem_r = &semaphore;
            let fin_r = &finished;
            scope.spawn(move || {
                // Count this worker as finished on EVERY exit path (success or
                // error) so the governor's exit predicate can't be stranded.
                struct FinishGuard<'a>(&'a std::sync::atomic::AtomicUsize);
                impl Drop for FinishGuard<'_> {
                    fn drop(&mut self) {
                        self.0.fetch_add(1, Ordering::Relaxed);
                    }
                }
                let _finish = FinishGuard(fin_r);
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
                let mut local_checks: Vec<(
                    std::collections::BTreeMap<String, u64>,
                    Option<String>,
                )> = Vec::new();
                loop {
                    // #152: one permit per page (guard releases at the end of
                    // THIS iteration on every path), so the governor sheds
                    // workers at page granularity when it shrinks the ceiling.
                    struct PermitGuard<'a>(&'a crate::resource::Semaphore);
                    impl Drop for PermitGuard<'_> {
                        fn drop(&mut self) {
                            self.0.release();
                        }
                    }
                    sem_r.acquire();
                    let _permit = PermitGuard(sem_r);
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
                    // Test-only: a MID-RANGE error — fires only once this range has
                    // ALREADY made page(s) durable (`pages > 0`), unlike the hook
                    // above which fires at the range's first page (range writes
                    // nothing). This is the fixture #200-1 needs: pre-failure pages
                    // are on disk but the range never commits, so they must still
                    // reach `files_committed` (published per-page below), not be
                    // dropped with the uncommitted range.
                    if pages > 0
                        && let Err(e) = crate::test_hook::maybe_error_at_index(
                            "keyset_parallel_worker_midrange",
                            ridx as i64,
                        )
                    {
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
                    if ridx == 0 && rfirst_r.lock().unwrap().is_none() {
                        // Range 0 is the LOWEST range: its first key is the
                        // run's observed floor (#151).
                        *rfirst_r.lock().unwrap() = page.first_cursor.clone();
                    }
                    // Publish the parts THIS page just wrote to the destination to
                    // the shared count IMMEDIATELY — the parquet is durable the
                    // moment `read_keyset_page_bounded` returns, before the range's
                    // checkpoint commit below. Deferring this to range-completion
                    // (the old `local_parts` at line 741) dropped every page a
                    // FAILED range had already made durable from `files_committed`,
                    // handing `decide_export_retry` a short count — the same
                    // durable-parts blind spot as the worker-level bail, one level
                    // deeper (page granularity within a range, #200-1). Cursor
                    // (`rmax`) and checksums stay commit-gated below — those feed
                    // the SUMMARY and must reflect only committed data — but the
                    // durability count must reflect what is physically on disk.
                    parts_r
                        .lock()
                        .unwrap()
                        .extend(page.parts.into_iter().map(|p| (ridx, p)));
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
                // Project the in-flight `running` aggregate from file_log, so the
                // part-landed / aggregate-projected pair travels together on this
                // runner like the other three (roast 2026-08-09, #173: keyset was
                // the one runner whose mid-run metrics row lagged its parts).
                // Best-effort observability over a fresh at-ref connection — the
                // projection is race-safe by construction (INSERT ON CONFLICT +
                // recompute-UPDATE) and never gates the checkpoint above.
                if let Some(sref) = sref_r
                    && let Err(e) = crate::state::StateStore::open_at_ref(sref).and_then(|st| {
                        st.project_running_aggregate(
                            rid_r,
                            &plan_r.export_name,
                            plan_r.strategy.mode_label(),
                            plan_r.format.label(),
                        )
                    })
                {
                    log::warn!(
                        "export '{}': running-aggregate projection failed for range {ridx} \
                         (checkpoint is durable; metrics row will catch up at finalize): {e:#}",
                        plan_r.export_name
                    );
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
                // Parts were published per-page above (they are durable pre-commit);
                // only the cursor and checksums — SUMMARY state — publish here, gated
                // on the checkpoint commit so a failed commit leaves no half-merged
                // summary (the parts count is intentionally NOT gated: it must show
                // on-disk debris even for a range that failed to commit).
                checks_r
                    .lock()
                    .unwrap()
                    .extend(local_checks.into_iter().map(|(m, k)| (ridx, m, k)));
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
    for (idx, (ridx, rec)) in parts.iter().enumerate() {
        super::commit::record_part(
            plan,
            summary,
            file_log_state,
            rec,
            super::commit::PartKind::Page {
                page_index: idx as i64,
                // The PARALLEL keyset runner resumes via per-range done flags + stable run_id part
                // names (immune to the sequential cursor window), so it does not use the v25
                // cursor-atomic reconcile — None.
                cursor_high: None,
            },
            // ADR-0029: the JOURNAL id stays the drain index (unchanged on-disk
            // shape) while the COVERAGE unit is the range that committed — or
            // failed to. A range that never committed published no checksums,
            // so its pages are recorded-but-uncovered and the seam suppresses
            // Form B rather than publish a record covering a strict subset of
            // this manifest. That suppression is the whole reason the naive
            // "move the feed above the bail" fix was rejected.
            super::commit::UnitId::Range(*ridx as i64),
        );
    }
    summary.total_rows += rows.into_inner();

    // #152: drain the governor's decisions into the journal BEFORE the error bail — the failure
    // path is EXACTLY where the back-off forensics matter (was the source under pressure when it
    // failed?). The shared drain_into (which poison-recovers, unlike the old into_inner().unwrap()
    // here) makes this identical to the chunked runner — the drift the copy re-introduced twice.
    governor.drain_into(summary);

    // ADR-0029 (the reported defect): the schema is an OBSERVATION — the workers
    // converged on ONE run schema and it describes what they READ, with no
    // coverage obligation — so feed it ABOVE the bail. This runner has no direct
    // `summary.schema_fingerprint` assignment at all, so the ledger is its only
    // path: fed below the bail, a FAILED parallel-keyset run pinned the stale
    // open-time baseline onto a Failed manifest listing parts whose parquet
    // carries the observed schema. The seam pins the fingerprint on BOTH paths
    // and runs the drift gate only on success.
    if let Some(sc) = fingerprint.get() {
        summary.ledger.note_schema(sc);
    }

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
    // cursor_high = the highest populated range's max (forensics v18); see range_max.
    summary.cursor_high = highest_range_max(range_max.into_inner().unwrap());
    // #151: the observed floor = range 0's first key (the lowest range);
    // the incremental block below overwrites this with the anchor floor.
    summary.cursor_low = range_first.into_inner().unwrap();

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
    // ADR-0028/0029: feed every committed range's Form-B checksums to the ledger
    // under the SAME `UnitId::Range` its parts were recorded with (commit-gated —
    // a failed range published none, and the seam then sees the shortfall itself
    // instead of being told about it). The seam harvests once.
    for (ridx, m, k) in checksums_mx.into_inner().unwrap() {
        summary
            .ledger
            .contribute_checksums(super::commit::UnitId::Range(ridx as i64), &m, k);
    }

    log::info!(
        "export '{}': parallel keyset complete — {} range(s), {} parts, {} rows",
        plan.export_name,
        total_ranges,
        parts.len(),
        summary.total_rows
    );

    // Incremental (iteration 3): on CLEAN SUCCESS advance the persisted anchor to
    // this run's ceiling (the source max pinned at open), so the next run seeks
    // strictly past it. Also pin the manifest cursor range to the ACCURATE
    // `[floor, ceil]` — vs `worker_max` which a resume under-reports (done ranges
    // are skipped, so their max is not re-observed). This is the incremental anchor
    // that iteration 2's cursor_high caveat deferred.
    if incremental && let Some(hi) = &anchor_ceiling {
        // Set the pending cursor range ONLY — do NOT advance the persisted
        // cursor here. `run_export_job` calls `commit_incremental_cursor` AFTER
        // `finalize_manifest` and ONLY when there is no manifest gap, exactly as
        // single mode defers it (single.rs commit_incremental_cursor). Advancing
        // eagerly inside the runner (before the manifest is durable) meant a
        // crash between this point and the manifest left the persisted cursor at
        // `hi` with no manifest referencing the just-written rows — the next run
        // seeks past them and they are lost (bug hunt 2026-08-09, HIGH: the
        // audit-#12 advance-after-durable invariant, which single upholds and
        // parallel keyset bypassed).
        summary.cursor_high = Some(hi.clone());
        summary.cursor_low = anchor_floor.clone();
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
                // v25 cursor-atomic reconcile: the export_state cursor can LAG the committed parts
                // — a crash in the after_manifest_update window advanced file_log (with the page's
                // cursor_high) but NOT the export_state cursor, so resuming from the latter
                // re-reads an already-committed page and DUPLICATES it (measured 300/1000; and its
                // multi-part-rotation variant). Each committed part carries its page high-water key
                // in the SAME file_log row, so resume from the LAST committed cursor_high instead —
                // the loop then never re-reads a committed page. It is >= the persisted cursor by
                // construction (written before/with the cursor advance), so this only ever moves
                // `last` FORWARD, never skipping uncommitted rows.
                if let Some(hw) = st.last_committed_cursor_high(rid)? {
                    last = Some(hw);
                    summary.cursor_low = last.clone();
                }
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

    // Destination + manifest-mode guard (Finding #44) fixed for the whole run — hoisted out of
    // the page loop. Part names key off the SANITIZED RUN_ID (stable across a resume, unique per
    // fresh run — the run-unique part-name rule), NOT a per-invocation wall-clock stamp: a
    // wall-clock stamp gave every resume a NEW name, so a crash in the after_manifest_update
    // window (file_log written, cursor NOT advanced) left the pre-crash page REHYDRATED while the
    // re-read wrote a differently-named copy — a durable MANIFEST duplicate (measured 300/1000
    // rows on a live mysql keyset resume; convergence round-1 HIGH). Matches the parallel path.
    let frame = super::frame::RunnerFrame::open(plan)?;
    let (dest, ext) = (frame.dest, frame.ext);
    let run_tag = run_scoped_tag(&summary.run_id, &plan.export_name);

    loop {
        // Name the part by the SEEK cursor (`last`), not the per-invocation page counter: a
        // re-read from the SAME seek (the un-advanced-cursor crash window) reproduces the SAME
        // page and OVERWRITES its rehydrated part idempotently; a re-read from an ADVANCED cursor
        // (a crash AFTER the cursor moved) has a different seek → a different name → both parts
        // are kept. The seek is known BEFORE the read, so the name is stable at write time. The
        // single-cursor analog of the parallel path's per-range `pk_w{range_index}` identity.
        let base = format!(
            "{}_{}_keyset_{}.{}",
            plan.export_name,
            run_tag,
            seek_tag(last.as_deref()),
            ext
        );
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

        // #151: the run's observed FLOOR — first page's first key. With
        // cursor_min+max in the metrics, key density is derivable from the
        // state DB alone (the keyset-vs-range input, no source round-trip).
        if summary.cursor_low.is_none() {
            summary.cursor_low = page.first_cursor.clone();
        }

        // ADR-0028: feed the run ledger from this page — the seam
        // (`finalize::finalize_export`) pins the fingerprint, runs the drift
        // gate and harvests Form B once, at the dispatcher. No application here.
        if let Some(sc) = &page.schema {
            summary.ledger.note_schema(sc);
        }
        // ADR-0029: the sequential runner's commit unit is the PAGE — this feed
        // and the `record_part` calls below are the same loop iteration over the
        // same page, so the two sets agree by construction.
        summary.ledger.contribute_checksums(
            super::commit::UnitId::Page(pages as i64),
            &page.column_checksums,
            page.checksum_key_column.clone(),
        );
        if plan.validate {
            summary.validated = Some(true);
        }
        // Record the parts FIRST, tracking whether EVERY part deduped. With v25 the cursor
        // reconcile (above) means a committed page is never re-read, so a dedup normally fires only
        // in the mid-page-crash fallback (below); the counter must still not double-count a deduped
        // re-read (rehydration already counted that page), or total_rows diverges from
        // sum(manifest_parts) and trips the coherence invariant — so total_rows is added only when
        // a page has a genuinely-new part.
        let mut any_new_part = page.parts.is_empty();
        let n_parts = page.parts.len();
        for (pi, rec) in page.parts.iter().enumerate() {
            // v25: stamp the page's high-water key ONLY on the LAST part's file_log row — the
            // point at which the WHOLE page is committed. On resume, `last` reconciles to the max
            // committed `cursor_high`, so a page that fully committed is skipped (never re-read →
            // no dup, the measured single-part fix). A crash MID-page (only earlier parts written,
            // last part absent → no cursor_high for this page) does NOT advance the reconcile, so
            // the page re-reads and its already-committed parts are handled by the run-id part
            // naming — a recoverable dup, never LOSS. Stamping every part with the page's eventual
            // high-water would falsely mark a mid-page crash "done" and DROP its uncommitted parts
            // (there is no per-part key to reconcile against — KeysetPage carries only next_cursor).
            let is_last = pi + 1 == n_parts;
            let deduped = super::commit::record_part(
                plan,
                summary,
                state,
                rec,
                super::commit::PartKind::Page {
                    page_index: pages as i64,
                    cursor_high: if is_last {
                        page.next_cursor.clone()
                    } else {
                        None
                    },
                },
                super::commit::UnitId::Page(pages as i64),
            );
            any_new_part |= !deduped;
        }
        if any_new_part {
            summary.total_rows += page.rows as i64;
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

    // DATA COMPLETE: the page loop exhausted the key range, so there is no
    // uncommitted work left to resume — for a NON-INCREMENTAL run, clear the
    // in-progress run_id NOW, BEFORE the post-data gates (schema-drift at the finalize_export seam, and
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

    // ADR-0028: the on_schema_drift gate, Form-B harvest and fingerprint pin are
    // applied by the ONE seam (`finalize::finalize_export`, at the dispatcher)
    // from the ledger this loop fed — the runner-bypass class this runner
    // twice re-introduced by hand-mirroring single mode is structurally gone.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SourceType;

    // ── seek_tag: the sequential-checkpoint part-name identity ────────────────
    const FNV_ID_000300: &str = "c4c7be0f3cc9638a"; // FNV-1a of "id-000300", pinned

    #[test]
    fn seek_tag_is_deterministic_and_distinguishes_seeks() {
        // The dedup invariant the after_manifest_update fix rests on: the SAME seek must yield the
        // SAME tag (so a re-read overwrites its rehydrated part), DIFFERENT seeks different tags
        // (so an advanced-cursor re-read is kept alongside). Deterministic across calls/versions
        // (FNV-1a), so a resume — possibly a newer binary — reproduces the crash run's names.
        assert_eq!(seek_tag(None), "start");
        assert_eq!(seek_tag(Some("id-000300")), seek_tag(Some("id-000300")));
        assert_ne!(seek_tag(Some("id-000300")), seek_tag(Some("id-000600")));
        assert_ne!(seek_tag(Some("id-000300")), seek_tag(None));
        // Pinned literal (FNV-1a of "id-000300") — if the hash constants ever change, a
        // mid-recovery upgrade would stop overwriting the rehydrated part and re-introduce the
        // dup; this catches that regression.
        assert_eq!(seek_tag(Some("id-000300")), FNV_ID_000300);
        // 16 lowercase hex chars, always.
        let t = seek_tag(Some("anything"));
        assert_eq!(t.len(), 16);
        assert!(
            t.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

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
    fn run_scoped_tag_strips_a_redundant_export_prefix_but_keeps_a_bare_run_id() {
        // Production run_id is `<export>_<stamp>`; the part-name format prepends
        // `<export>_`, so the raw run_id doubled the export name in the file
        // (field-run: `aa_bonus_conversions_usd_aa_bonus_conversions_usd_...`).
        assert_eq!(
            run_scoped_tag(
                "aa_bonus_conversions_usd_20260820T104554088",
                "aa_bonus_conversions_usd"
            ),
            "20260820T104554088",
            "the leading <export>_ must be stripped so the part name is not <export>_<export>_<stamp>"
        );
        // A bare / custom run_id (no export prefix) is left untouched — the
        // synthetic keyset_range fixtures rely on `exp_run-1_pk_w1_0`.
        assert_eq!(run_scoped_tag("run-1", "exp"), "run-1");
        // Uniqueness survives: two runs -> two stamps -> two tags.
        assert_ne!(
            run_scoped_tag("e_20260820T104554088", "e"),
            run_scoped_tag("e_20260820T104554090", "e")
        );
    }

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

    /// #161: the gap-free / overlap-free coverage property of the pure
    /// partitioning fold, asserted rather than trusted — for ANY boundary set
    /// and any floor/ceil, consecutive ranges CHAIN (each lo == previous hi),
    /// the first lo == floor, the last hi == ceil, indices are dense.
    #[test]
    fn partition_ranges_cover_the_key_space_without_gaps_or_overlap() {
        use proptest::prelude::*;
        proptest!(|(
            mut bounds in proptest::collection::vec("[0-9a-f]{1,8}", 0..12),
            floor in proptest::option::of("[0-9a-f]{1,8}"),
            ceil in proptest::option::of("[0-9a-f]{1,8}"),
        )| {
            bounds.sort();
            bounds.dedup();
            let ranges = partition_ranges(&bounds, floor.as_deref(), ceil.as_deref());
            // N boundaries -> N+1 ranges, densely indexed.
            prop_assert_eq!(ranges.len(), bounds.len() + 1);
            for (i, r) in ranges.iter().enumerate() {
                prop_assert_eq!(r.0, i, "dense range indices");
                prop_assert!(!r.3, "ranges start not-done");
            }
            // Chain: first lo == floor, each next lo == previous hi, last hi == ceil.
            prop_assert_eq!(ranges[0].1.as_deref(), floor.as_deref());
            for w in ranges.windows(2) {
                prop_assert_eq!(w[1].1.as_deref(), w[0].2.as_deref(), "no gap, no overlap");
            }
            prop_assert_eq!(ranges[ranges.len() - 1].2.as_deref(), ceil.as_deref());
        });
    }
}
