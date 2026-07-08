//! **Layer: Execution** — parallel `_id`-range reader for MongoDB (OPT-4++).
//!
//! `parallel: N` on a keyset (`source.mongo.page_size`) Mongo export fans the
//! collection into `N` disjoint `_id` ranges and reads them concurrently. Each
//! worker keyset-pages its own slice (`find({_id: {$gte: lo, $lt: hi}})`), so
//! the union is the whole collection with **no overlap and no gap**: `_id` is
//! immutable, so a document never migrates between ranges mid-read (the
//! miss/dup hazard that rules out range-splitting a mutable chunk key does not
//! apply here). Range boundaries come from a cheap `$sample` (a random cursor,
//! not a collection scan — see [`MongoSource::sample_id_ranges`]).
//!
//! Safe on a **quiescent** collection. It is NOT point-in-time consistent under
//! concurrent writes at scale (the snapshot window can't cover a multi-minute
//! parallel scan) — that is CDC's job, not this reader's.
//!
//! Fan-in mirrors the SQL `run_chunked_parallel` shape without re-plumbing its
//! `i64`-typed range machinery: each worker writes its OWN part files
//! (run-unique + worker-unique names) and returns its `PartRecord`s; the main
//! thread drains them through the shared `commit::record_part` so the
//! I2→I7→counters/journal ordering stays single-threaded and race-free.

use super::{RunSummary, commit, sink::ExportSink};
use crate::config::IncrementalCursorMode;
use crate::error::Result;
use crate::plan::{IncrementalCursorPlan, KeysetPlan, ResolvedRunPlan};
use crate::source::mongo::MongoSource;
use crate::source::{self, Source};
use crate::state::StateStore;
use crate::types::CursorState;
use crate::{destination, format};

pub(crate) fn run_mongo_parallel(
    plan: &ResolvedRunPlan,
    summary: &mut RunSummary,
    state: &StateStore,
    kp: &KeysetPlan,
) -> Result<()> {
    let parallel = kp.parallel.max(1);
    let url = plan.source.resolve_url()?;
    let collection = crate::sql::strip_select_star_from(&plan.base_query).ok_or_else(|| {
        anyhow::anyhow!(
            "export '{}': parallel `_id`-range reads need a `table:` shortcut (a bare collection), \
             not a hand-written `query:`.",
            plan.export_name
        )
    })?;

    // Boundaries: a short-lived probe connection computes the ranges, then drops
    // before the workers open theirs (mirrors the chunked Detect preamble).
    let ranges = {
        let probe =
            MongoSource::connect(&url, plan.source.tls.as_ref(), plan.source.mongo.as_ref())?;
        probe.sample_id_ranges(collection, parallel)?
    };
    log::info!(
        "export '{}': parallel `_id`-range read — {} range(s), page size {}",
        plan.export_name,
        ranges.len(),
        kp.chunk_size
    );

    // One run-unique stamp shared by every worker; the worker index + page index
    // make each part name unique WITHIN the run, the stamp unique ACROSS runs
    // (millisecond precision — two runs into the same prefix must not clobber,
    // per the run-unique part-name rule).
    let stamp = chrono::Utc::now().format("%Y%m%d_%H%M%S_%3f").to_string();

    let key_plan = IncrementalCursorPlan {
        primary_column: kp.key_column.clone(),
        fallback_column: None,
        mode: IncrementalCursorMode::SingleColumn,
    };

    // Fan out: each worker reads its disjoint slice, writes its parts, returns
    // (rows, PartRecords). Errors surface per worker and fail the whole run.
    let results: Vec<Result<WorkerOutput>> = std::thread::scope(|s| {
        let handles: Vec<_> = ranges
            .iter()
            .enumerate()
            .map(|(w, &(lo, hi))| {
                let url = &url;
                let key_plan = &key_plan;
                let stamp = &stamp;
                s.spawn(move || range_worker(url, plan, key_plan, kp, stamp, w, lo, hi))
            })
            .collect();
        handles
            .into_iter()
            .map(|h| {
                h.join()
                    .unwrap_or_else(|_| Err(anyhow::anyhow!("mongo parallel worker panicked")))
            })
            .collect()
    });

    // Drain on the main thread: sum rows + record every part through the shared
    // commit path (single-threaded → the counter/journal ordering is race-free).
    for (w, res) in results.into_iter().enumerate() {
        let out = res?;
        summary.total_rows += out.rows;
        if let Some(sc) = &out.schema {
            super::manifest_writer::record_run_schema_fingerprint(summary, sc);
        }
        if plan.validate && out.rows > 0 {
            summary.validated = Some(true);
        }
        for rec in &out.parts {
            commit::record_part(
                plan,
                summary,
                Some(state),
                rec,
                commit::PartKind::Chunk {
                    chunk_index: w as i64,
                },
            );
        }
    }

    log::info!(
        "export '{}': parallel complete — {} range(s), {} rows",
        plan.export_name,
        ranges.len(),
        summary.total_rows
    );
    Ok(())
}

struct WorkerOutput {
    rows: i64,
    parts: Vec<commit::PartRecord>,
    schema: Option<arrow::datatypes::Schema>,
}

#[allow(clippy::too_many_arguments)]
fn range_worker(
    url: &str,
    plan: &ResolvedRunPlan,
    key_plan: &IncrementalCursorPlan,
    kp: &KeysetPlan,
    stamp: &str,
    worker: usize,
    lo: mongodb::bson::oid::ObjectId,
    hi: mongodb::bson::oid::ObjectId,
) -> Result<WorkerOutput> {
    let mut src = MongoSource::connect(url, plan.source.tls.as_ref(), plan.source.mongo.as_ref())?
        .with_id_range(lo, hi);
    let dest = destination::create_destination(&plan.destination)?;
    crate::manifest::guard_manifest_mode(dest.as_ref(), "batch")?;

    let mut out = WorkerOutput {
        rows: 0,
        parts: Vec::new(),
        schema: None,
    };
    let mut last: Option<String> = None;
    let mut page = 0usize;

    loop {
        let cursor = last.as_ref().map(|v| CursorState {
            export_name: plan.export_name.clone(),
            last_cursor_value: Some(v.clone()),
            last_run_at: None,
        });
        let mut sink = ExportSink::new(plan)?;
        src.export(
            &source::ExportRequest::unwrapped(
                &plan.base_query,
                &plan.tuning,
                &plan.column_overrides,
            )
            .with_incremental(Some(key_plan))
            .with_cursor(cursor.as_ref())
            .with_page_limit(kp.chunk_size),
            &mut sink,
        )?;
        if let Some(w) = sink.writer.take() {
            w.finish()?;
        }
        if out.schema.is_none()
            && let Some(sc) = sink.dest_schema.as_deref()
        {
            out.schema = Some(sc.clone());
        }

        let rows = sink.total_rows;
        if rows == 0 {
            break;
        }
        out.rows += rows as i64;

        let fmt =
            format::create_format(plan.format, plan.compression, plan.compression_level, None);
        // run-unique (stamp) + worker-unique (w{worker}) + page-unique.
        let base = format!(
            "{}_{}_w{}_keyset{}.{}",
            plan.export_name,
            stamp,
            worker,
            page,
            fmt.file_extension()
        );
        let recs = commit::write_sink_parts(
            dest.as_ref(),
            &mut sink,
            plan.validate.then_some(plan.format),
            |idx, count| commit::part_indexed_name(&base, idx, count),
        )?;
        out.parts.extend(recs);
        page += 1;

        if rows < kp.chunk_size {
            break;
        }
        match sink.last_cursor_value.clone() {
            Some(v) => last = Some(v),
            None => anyhow::bail!(
                "export '{}': parallel worker {} could not read the '{}' value to advance keyset \
                 (NULL or unsupported type).",
                plan.export_name,
                worker,
                kp.key_column
            ),
        }
    }
    Ok(out)
}
