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

use super::{RunSummary, commit};
use crate::config::IncrementalCursorMode;
use crate::error::Result;
use crate::plan::{IncrementalCursorPlan, KeysetPlan, ResolvedRunPlan};
use crate::source::mongo::MongoSource;
use crate::state::StateStore;
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
            .map(|(w, range)| {
                let url = &url;
                let key_plan = &key_plan;
                let stamp = &stamp;
                // Bson bounds aren't Copy — clone the slice into the worker.
                let (lo, hi) = (range.0.clone(), range.1.clone());
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
    let mut drift_schema: Option<arrow::datatypes::Schema> = None;
    // Form B: fold each worker's XOR-combined checksums run-wide (order-independent
    // across workers), then harvest once — so a parallel-Mongo manifest records
    // Form B like every other runner.
    let mut checksums_acc: std::collections::BTreeMap<String, u64> =
        std::collections::BTreeMap::new();
    let mut checksum_key_column: Option<String> = None;
    for (w, res) in results.into_iter().enumerate() {
        let out = res?;
        summary.total_rows += out.rows;
        if let Some(sc) = &out.schema {
            super::manifest_writer::record_run_schema_fingerprint(summary, sc);
            if drift_schema.is_none() {
                drift_schema = Some(sc.clone());
            }
        }
        super::commit::accumulate_column_checksums(&mut checksums_acc, &out.column_checksums);
        if checksum_key_column.is_none() {
            checksum_key_column = out.checksum_key_column;
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
    super::commit::harvest_column_checksums(summary, checksums_acc, checksum_key_column);

    log::info!(
        "export '{}': parallel complete — {} range(s), {} rows",
        plan.export_name,
        ranges.len(),
        summary.total_rows
    );

    // on_schema_drift gate — mirror single/keyset: this runner also bypasses
    // run_single_export, so without this an opted-in `on_schema_drift: fail`
    // returned exit 0 on a drifted schema for a parallel Mongo export.
    if let Some(sc) = &drift_schema {
        super::schema_drift::check_from_sink_schema(
            state,
            &plan.export_name,
            sc,
            plan.schema_drift_policy,
            summary,
        )?;
    }
    Ok(())
}

struct WorkerOutput {
    rows: i64,
    parts: Vec<commit::PartRecord>,
    schema: Option<arrow::datatypes::Schema>,
    /// This worker's XOR-combined per-column Form B checksums (main thread folds
    /// them run-wide across workers so the finalize manifest records Form B).
    column_checksums: std::collections::BTreeMap<String, u64>,
    checksum_key_column: Option<String>,
}

#[allow(clippy::too_many_arguments)]
fn range_worker(
    url: &str,
    plan: &ResolvedRunPlan,
    key_plan: &IncrementalCursorPlan,
    kp: &KeysetPlan,
    stamp: &str,
    worker: usize,
    lo: mongodb::bson::Bson,
    hi: mongodb::bson::Bson,
) -> Result<WorkerOutput> {
    let mut src = MongoSource::connect(url, plan.source.tls.as_ref(), plan.source.mongo.as_ref())?
        .with_id_range(lo, hi);
    let dest = destination::create_destination(&plan.destination)?;
    crate::manifest::guard_manifest_mode(dest.as_ref(), "batch")?;
    let ext = format::create_format(plan.format, plan.compression, plan.compression_level, None)
        .file_extension()
        .to_string();

    let mut out = WorkerOutput {
        rows: 0,
        parts: Vec::new(),
        schema: None,
        column_checksums: std::collections::BTreeMap::new(),
        checksum_key_column: None,
    };
    let mut last: Option<String> = None;
    let mut page = 0usize;

    loop {
        // run-unique (stamp) + worker-unique (w{worker}) + page-unique.
        let base = format!(
            "{}_{}_w{}_keyset{}.{}",
            plan.export_name, stamp, worker, page, ext
        );
        // Deferred commit: the worker collects its parts (the main thread drains
        // them through `record_part`), unlike the sequential runner which commits
        // each page as it arrives — the one axis the two callers differ on.
        let Some(p) = super::keyset::read_keyset_page(
            &mut src,
            plan,
            key_plan,
            kp.chunk_size,
            last.as_deref(),
            dest.as_ref(),
            &base,
        )?
        else {
            break;
        };
        out.rows += p.rows as i64;
        if out.schema.is_none() {
            out.schema = p.schema;
        }
        out.parts.extend(p.parts);
        super::commit::accumulate_column_checksums(&mut out.column_checksums, &p.column_checksums);
        if out.checksum_key_column.is_none() {
            out.checksum_key_column = p.checksum_key_column;
        }
        page += 1;

        if p.rows < kp.chunk_size {
            break;
        }
        match p.next_cursor {
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
