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

/// Rehydrate a keyset resume's prior committed pages WITH a destination probe —
/// and REFUSE to finalize over a hole. Round-5 lifecycle HIGH: keyset has no M8
/// (no per-part Rewrite machinery), so a committed page deleted from the
/// destination between attempts (a gc pass after `state finish-run`, a
/// foreign-host gc) was re-declared sight-unseen — Success + `_SUCCESS` naming
/// deleted parquet, the page's rows silently absent. Chunked re-exports the
/// missing chunk; keyset cannot re-read one page (the cursor has moved), so the
/// honest outcome is a LOUD bail naming the remedy. A listing failure degrades
/// to the unverified declare, said out loud (a resume must not die on a blip).
fn rehydrate_keyset_pages_probed(
    st: &crate::state::StateStore,
    run_id: &str,
    plan: &ResolvedRunPlan,
    summary: &mut crate::pipeline::RunSummary,
) -> anyhow::Result<()> {
    let probe: Option<std::collections::HashSet<String>> =
        match crate::destination::create_destination(&plan.destination) {
            Ok(dest) if dest.capabilities().commit_protocol.leaves_objects_at_rest() => {
                match dest.list_prefix("") {
                    Ok(listing) => Some(
                        listing
                            .iter()
                            .map(|m| m.key.rsplit('/').next().unwrap_or(&m.key).to_string())
                            .collect(),
                    ),
                    Err(e) => {
                        log::warn!(
                            "keyset resume: cannot list the destination to verify prior \
                             pages ({e:#}) — declaring from the state DB unverified"
                        );
                        None
                    }
                }
            }
            _ => None, // streaming destination, or unopenable: nothing to probe
        };
    let (_, missing) =
        super::chunked::rehydrate_manifest_parts_probed(st, run_id, summary, probe.as_ref())?;
    if !missing.is_empty() {
        let names: Vec<&str> = missing.iter().map(|(_, n)| n.as_str()).collect();
        // The remedy is `state reset`, NOT `state reset-chunks`: keyset resume
        // keys on `export_state.resume_run_id` + `keyset_range`, which
        // reset-chunks does not touch (it clears chunk_task/chunk_run — tables
        // keyset never writes). Round-6 live-proved the wrong hint strands the
        // operator in a refusal loop; `state reset` drops the export_state row
        // (cursor included, which incremental-keyset needs cleared too) and the
        // next run does a fresh full pass.
        anyhow::bail!(
            "keyset resume: {} committed page part(s) are GONE from the destination \
             ({}) — a gc/cleanup pass deleted them between attempts. Refusing to \
             finalize a Success manifest naming deleted files (their rows would be \
             silently absent). Reset this export's keyset state (`rivet state \
             reset -c <config> --export {}`) so the whole range is re-read, \
             or point the export at a fresh prefix.",
            names.len(),
            names.join(", "),
            plan.export_name
        );
    }
    Ok(())
}

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
    /// What this page's sink SAW: dest schema (run fingerprint) + column max bytes.
    pub(crate) observed: super::commit::Observations,
    pub(crate) next_cursor: Option<String>,
    /// First observed key of this page (the run floor when it is page 1 of
    /// range 0) — recorded so cursor_min lands in the metrics (#151).
    pub(crate) first_cursor: Option<String>,
    /// This page's sink's Form-B value checksums and their key column.
    pub(crate) checksums: super::commit::UnitChecksums,
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
        cursor_column: None,
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
    sink.finish_writer()?;
    let rows = sink.total_rows;
    if rows == 0 {
        return Ok(None); // range exhausted, or an exact-multiple last page
    }
    let observed = sink.take_observations();
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
        observed,
        // The source's own lossless token (Mongo BSON `_id`) when it reported
        // one, else the column-extracted string (every SQL engine).
        next_cursor: sink.effective_cursor(),
        first_cursor: sink.first_cursor_value.clone(),
        checksums: sink.take_checksums(),
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
        let anchor = match state {
            Some(s) => s.get_owned(&plan.export_name, &key)?.last_cursor_value,
            None => None,
        };
        let key_q = crate::sql::quote_ident(plan.source.source_type, &key);
        let cur_max = src.query_scalar(&format!(
            "SELECT MAX({key_q}) FROM ({}) AS _rivet_pk_max",
            plan.base_query
        ))?;
        if nothing_past_anchor(anchor.as_deref(), cur_max.as_deref()) {
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
            let rows = st.load_keyset_ranges(&plan.export_name, rid, &key)?;
            if rows.is_empty() {
                // Anchor set but no persisted ranges (a crash between set_resume_
                // run_id and persist_keyset_ranges — nothing committed), or ranges
                // sampled on ANOTHER key column (the recipe changed): re-sample +
                // persist under this run_id and start over. No skip.
                let fresh = sample_parallel_ranges(src, plan, &key, parallel, floor_r, ceil_r)?;
                st.persist_keyset_ranges(&plan.export_name, rid, &key, &lo_hi_pairs(&fresh))?;
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
            st.persist_keyset_ranges(
                &plan.export_name,
                &summary.run_id,
                &key,
                &lo_hi_pairs(&fresh),
            )?;
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
    if fan_out_collapsed(parallel, total_ranges) {
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

    // ADR-0029: parts are published per PAGE (durability must reflect what is on
    // disk, #200-1) and checksums per committed RANGE — both under the range's
    // `UnitId`, so the seam can compute Form-B coverage.
    let fan = super::fan_in::FanIn::default();
    // Per-range high-water key, indexed by range_index (done ranges stay None —
    // they are not re-run). cursor_high = the highest populated range's max; on a
    // RESUME this reflects the RE-RUN ranges only (a range already `done` pre-crash
    // is skipped), which is acceptable — parallel keyset is a full snapshot, not an
    // incremental anchor, so its cursor range is descriptive, not a resume floor.
    let range_max: Mutex<Vec<Option<String>>> = Mutex::new(vec![None; total_ranges]);
    let range_first: Mutex<Option<String>> = Mutex::new(None);

    // #152: one permit PER PAGE, so shrinking the ceiling sheds workers at page
    // granularity — the same shape the range-chunked runner uses at chunk granularity.
    let semaphore = crate::resource::Semaphore::new(parallel.max(1));
    let governor = crate::pipeline::governor::GovernorHarness::arm(plan, parallel);

    std::thread::scope(|scope| {
        governor.spawn_into(
            scope,
            &semaphore,
            fan.finished(),
            pending.len(),
            &plan.export_name,
        );

        for (ridx, lo, hi) in pending.iter().cloned() {
            let dest = std::sync::Arc::clone(&dest);
            let (plan_r, key_plan_r, ext_r, tag_r, key_r) =
                (plan, &key_plan, &ext, run_tag.as_str(), key.as_str());
            let (fan_r, rfirst_r, rmax_r) = (&fan, &range_first, &range_max);
            let (sref_r, rid_r, fmt_r, cmp_r) = (&state_ref, run_id.as_str(), fmt_label, cmp_label);
            let sem_r = &semaphore;
            let unit = super::commit::UnitId::Range(ridx as i64);
            fan.spawn(scope, format!("range {ridx}"), move || {
                let mut wsrc = source::create_source(&plan_r.source)
                    .map_err(|e| anyhow::anyhow!("connect: {e:#}"))?;
                let mut cursor = lo;
                let mut pages = 0usize;
                let mut rmax: Option<String> = None;
                // Parts this range committed — recorded to file_log atomically with
                // its `done` flip at completion (checkpoint only).
                let mut range_parts: Vec<crate::state::KeysetRangePart> = Vec::new();
                let mut local_checks: Vec<super::commit::UnitChecksums> = Vec::new();
                loop {
                    let _permit = crate::pipeline::governor::TaskPermit::acquire(sem_r);
                    // Test-only: a per-worker SQL error at the range's first page
                    // (Err path, not a crash) — the run fails after the drain.
                    crate::test_hook::maybe_error_at_index("keyset_parallel_worker", ridx as i64)
                        .map_err(|e| anyhow::anyhow!("{e}"))?;
                    // Test-only: a MID-RANGE error — fires only once this range has
                    // ALREADY made page(s) durable (`pages > 0`). The fixture #200-1
                    // needs: pre-failure pages are on disk but the range never
                    // commits, so they must still reach `files_committed`.
                    if pages > 0 {
                        crate::test_hook::maybe_error_at_index(
                            "keyset_parallel_worker_midrange",
                            ridx as i64,
                        )
                        .map_err(|e| anyhow::anyhow!("{e}"))?;
                    }
                    let base = format!(
                        "{}_{}_pk_w{}_{}.{}",
                        plan_r.export_name, tag_r, ridx, pages, ext_r
                    );
                    let page = read_keyset_page_bounded(
                        &mut *wsrc,
                        plan_r,
                        key_plan_r,
                        page_size,
                        cursor.as_deref(),
                        hi.as_deref(),
                        &**dest,
                        &base,
                    )
                    .map_err(|e| anyhow::anyhow!("page {pages}: {e:#}"))?;
                    let Some(page) = page else { break };
                    fan_r.observe(page.observed);
                    rmax = page.next_cursor.clone().or(rmax);
                    for p in &page.parts {
                        range_parts.push(crate::state::KeysetRangePart {
                            file_name: p.file_name.clone(),
                            rows: p.rows,
                            bytes: p.bytes as i64,
                        });
                    }
                    if ridx == 0 {
                        // Range 0 is the LOWEST range: its first key is the
                        // run's observed floor (#151).
                        let mut first = rfirst_r.lock().unwrap_or_else(|e| e.into_inner());
                        if first.is_none() {
                            *first = page.first_cursor.clone();
                        }
                    }
                    // The parquet is durable the moment `read_keyset_page_bounded`
                    // returns — publish its parts now, before the range commits, so
                    // a range that later fails still counts them (#200-1). Cursor
                    // and checksums stay commit-gated below.
                    for p in page.parts {
                        fan_r.part(unit, p);
                    }
                    local_checks.push(page.checksums);
                    let last_page = is_last_page(page.rows, page_size);
                    if !last_page {
                        cursor = Some(page.next_cursor.ok_or_else(|| {
                            anyhow::anyhow!(
                                "could not advance the '{key_r}' cursor at page {pages} \
                                 (NULL or unsupported type)"
                            )
                        })?);
                    }
                    pages += 1;
                    if last_page {
                        break;
                    }
                }
                // Atomic checkpoint: the range's parts → file_log AND `done=1` in one
                // transaction (checkpoint runs only). A crash before this leaves the
                // range `done=0` with no file_log rows — re-read on resume.
                if let Some(sref) = sref_r {
                    crate::state::StateStore::commit_keyset_range_at_ref(
                        sref,
                        rid_r,
                        &plan_r.export_name,
                        ridx as i64,
                        &range_parts,
                        fmt_r,
                        Some(cmp_r),
                    )
                    .map_err(|e| anyhow::anyhow!("checkpoint commit: {e:#}"))?;
                }
                // Project the in-flight `running` aggregate from file_log (#173):
                // best-effort observability, never gates the checkpoint above.
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
                // Cursor and checksums publish only after the checkpoint commits.
                rmax_r.lock().unwrap_or_else(|e| e.into_inner())[ridx] = rmax;
                for c in local_checks {
                    fan_r.contribute(unit, c);
                }
                Ok(())
            });
        }
    });

    // On a CHECKPOINT run the workers already wrote file_log atomically with their
    // `done` flip, so the drain writes none; a non-checkpoint run persists no
    // ranges, so the drain writes it — matching the sequential keyset path.
    // The JOURNAL id stays the drain index (unchanged on-disk shape) while the
    // COVERAGE unit is the range; a range that never committed published no
    // checksums, so its pages stay uncovered and the seam suppresses Form B.
    let file_log_state = if checkpoint { None } else { state };
    fan.finish(
        plan,
        summary,
        file_log_state,
        Some(governor),
        |idx, _| super::commit::PartKind::Page {
            page_index: idx as i64,
            // Parallel keyset resumes via per-range done flags + stable run_id part
            // names, not the sequential cursor reconcile — None.
            cursor_high: None,
        },
        |errs| {
            anyhow::anyhow!(
                "export '{}': parallel keyset failed on {} range(s): {}",
                plan.export_name,
                errs.len(),
                errs.join("; ")
            )
        },
    )?;

    // Merge into the summary through the shared seams (identical to the sequential
    // runner's per-page path, folded run-wide).
    if plan.validate {
        summary.validated = Some(true);
    }
    // cursor_high = the highest populated range's max (forensics v18); see range_max.
    summary.cursor_high =
        highest_range_max(range_max.into_inner().unwrap_or_else(|e| e.into_inner()));
    // #151: the observed floor = range 0's first key (the lowest range);
    // the incremental block below overwrites this with the anchor floor.
    summary.cursor_low = range_first.into_inner().unwrap_or_else(|e| e.into_inner());

    // Resume completeness: reconstruct the parts of the ranges that completed in a
    // PRIOR (crashed) run — they were not re-run, so they are absent from
    // `parts_mx`; file_log (under the reused run_id) is their record. rehydrate
    // dedupes against the parts just recorded, so a fresh run (all ranges re-run
    // this pass) is a no-op here.
    if let Some(st) = state
        && checkpoint
    {
        rehydrate_keyset_pages_probed(st, &run_id, plan, summary)?;
    }

    log::info!(
        "export '{}': parallel keyset complete — {} range(s), {} parts, {} rows",
        plan.export_name,
        total_ranges,
        summary.manifest_parts.len(),
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

/// Is there nothing past the incremental anchor: an empty source, or a source
/// max that does not advance it? No prior anchor means every row is new.
fn nothing_past_anchor(anchor: Option<&str>, cur_max: Option<&str>) -> bool {
    match (anchor, cur_max) {
        (_, None) => true,
        (None, Some(_)) => false,
        (Some(a), Some(c)) => !key_advances(a, c),
    }
}

/// Did the sampler collapse a requested parallel fan-out to a single range?
fn fan_out_collapsed(parallel: usize, total_ranges: usize) -> bool {
    parallel > 1 && total_ranges == 1
}

/// A short page means the key range is exhausted.
fn is_last_page(rows: usize, page_size: usize) -> bool {
    rows < page_size
}

/// Does a sequential keyset run seek from the persisted cursor (crash recovery,
/// or `keyset_incremental`) rather than from the start of the key space?
fn seeks_from_persisted_cursor(
    checkpoint: bool,
    recovering_crash: bool,
    incremental: bool,
) -> bool {
    checkpoint && (recovering_crash || incremental)
}

/// Is the resume anchor released as soon as the data is complete? Only for a
/// crash-recovery-only run: an incremental run keeps it until the manifest is
/// written, or a crash in between would orphan the committed pages.
fn releases_anchor_at_data_complete(checkpoint: bool, incremental: bool) -> bool {
    checkpoint && !incremental
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
        settle: None,
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

    let mut last: Option<String> =
        if seeks_from_persisted_cursor(kp.checkpoint, recovering_crash, kp.incremental) {
            match state {
                Some(s) => {
                    s.get_owned(&plan.export_name, &kp.key_column)?
                        .last_cursor_value
                }
                None => None,
            }
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
                rehydrate_keyset_pages_probed(st, rid, plan, summary)?;
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
        let Some(mut page) = read_keyset_page(
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
        summary.ledger.observe(std::mem::take(&mut page.observed));
        // ADR-0029: the sequential runner's commit unit is the PAGE — this feed
        // and the `record_part` calls below are the same loop iteration over the
        // same page, so the two sets agree by construction.
        summary.ledger.contribute(
            super::commit::UnitId::Page(pages as i64),
            std::mem::take(&mut page.checksums),
        );
        if plan.validate {
            summary.validated = Some(true);
        }
        // Record the parts FIRST, tracking whether EVERY part deduped. With v25 the cursor
        // reconcile (above) means a committed page is never re-read, so a dedup normally fires only
        // in the mid-page-crash fallback (below); `record_part` counts each part's rows once, so a
        // deduped re-read of a rehydrated part adds nothing.
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
            super::commit::record_part(
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
        }
        // Persist the high-water mark AFTER the parts are durably committed, so a
        // resume continues from committed data (peek→flush→ack). The crash window
        // between the commit and this line is at-least-once: the last page is
        // re-read (downstream dedup / reconcile absorbs it), never lost.
        if kp.checkpoint
            && let (Some(st), Some(v)) = (state, page.next_cursor.as_ref())
        {
            st.update_with_column(&plan.export_name, v, &kp.key_column)?;
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
        if is_last_page(page.rows, kp.chunk_size) {
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
    if releases_anchor_at_data_complete(kp.checkpoint, kp.incremental)
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

    #[test]
    fn nothing_past_anchor_covers_empty_first_and_stale_sources() {
        assert!(nothing_past_anchor(Some("5"), None), "empty source");
        assert!(nothing_past_anchor(None, None), "empty source, no anchor");
        assert!(
            !nothing_past_anchor(None, Some("1")),
            "no anchor: every row is new"
        );
        assert!(
            !nothing_past_anchor(Some("999"), Some("1000")),
            "numeric, not lexical"
        );
        assert!(
            nothing_past_anchor(Some("1000"), Some("1000")),
            "max == anchor"
        );
    }

    #[test]
    fn fan_out_collapses_only_when_parallel_was_asked_for() {
        assert!(fan_out_collapsed(4, 1));
        assert!(!fan_out_collapsed(1, 1), "sequential was asked for");
        assert!(!fan_out_collapsed(4, 2));
    }

    #[test]
    fn a_short_page_is_the_last() {
        assert!(is_last_page(2, 3));
        assert!(!is_last_page(3, 3), "a full page may have a successor");
    }

    #[test]
    fn keyset_seeks_from_the_cursor_only_for_recovery_or_incremental() {
        assert!(
            seeks_from_persisted_cursor(true, true, false),
            "crash recovery"
        );
        assert!(
            seeks_from_persisted_cursor(true, false, true),
            "keyset_incremental"
        );
        assert!(
            !seeks_from_persisted_cursor(true, false, false),
            "clean re-run: full pass"
        );
        assert!(
            !seeks_from_persisted_cursor(false, true, true),
            "no checkpoint, no cursor"
        );
    }

    #[test]
    fn only_a_crash_recovery_run_releases_its_anchor_at_data_complete() {
        assert!(releases_anchor_at_data_complete(true, false));
        assert!(
            !releases_anchor_at_data_complete(true, true),
            "incremental keeps it to finalize"
        );
        assert!(
            !releases_anchor_at_data_complete(false, false),
            "no checkpoint, no anchor"
        );
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
