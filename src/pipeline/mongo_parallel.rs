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
        settle: None,
    };

    // Fan out: each worker reads its disjoint slice, writes its parts, returns
    // (rows, PartRecords). Errors surface per worker and fail the whole run.
    // ONE guarded open at run start (the cross-shape manifest GET), shared into
    // every worker — N workers used to open GUARDED frames concurrently: N remote
    // GETs for an answer that cannot change mid-run (roast 2026-08-09, #173; the
    // chunked sibling was fixed the same way).
    let (shared_dest, shared_ext) = super::frame::RunnerFrame::open_shared(plan)?;
    // No permits and no governor: one unthrottled thread per `_id` range. A worker
    // publishes each page the moment it is durable, so a failed — or panicking —
    // worker still hands back what it wrote.
    let fan = super::fan_in::FanIn::default();
    std::thread::scope(|s| {
        for (w, range) in ranges.iter().enumerate() {
            let (url, key_plan, stamp, ext, fan_r) = (&url, &key_plan, &stamp, &shared_ext, &fan);
            // Bson bounds aren't Copy — clone the slice into the worker.
            let (lo, hi) = (range.0.clone(), range.1.clone());
            let dest = std::sync::Arc::clone(&shared_dest);
            fan.spawn(s, format!("worker {w}"), move || {
                range_worker_pages(url, plan, key_plan, kp, stamp, w, lo, hi, dest, ext, fan_r)
            });
        }
    });
    // ADR-0029: the worker range is this runner's commit unit; a worker contributes
    // the checksums of every page it wrote, failed or not, so its parts stay covered.
    let drained = fan.finish(
        plan,
        summary,
        Some(state),
        None,
        |_, unit| match unit {
            commit::UnitId::Chunk(chunk_index) => commit::PartKind::Chunk { chunk_index },
            other => unreachable!("mongo parts are recorded under a chunk unit, not {other:?}"),
        },
        |errs| {
            anyhow::anyhow!(
                "export '{}': parallel mongo failed on {} range(s): {}",
                plan.export_name,
                errs.len(),
                errs.join("; ")
            )
        },
    );
    if plan.validate {
        summary.validated = Some(true);
    }
    drained?;

    log::info!(
        "export '{}': parallel complete — {} range(s), {} rows",
        plan.export_name,
        ranges.len(),
        summary.total_rows
    );

    // ADR-0028: fingerprint/drift/Form-B application lives in the ONE seam
    // (`finalize::finalize_export`), fed from the ledger above.
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn range_worker_pages(
    url: &str,
    plan: &ResolvedRunPlan,
    key_plan: &IncrementalCursorPlan,
    kp: &KeysetPlan,
    stamp: &str,
    worker: usize,
    lo: mongodb::bson::Bson,
    hi: mongodb::bson::Bson,
    dest: std::sync::Arc<Box<dyn crate::destination::Destination>>,
    ext: &str,
    fan: &super::fan_in::FanIn,
) -> Result<()> {
    let unit = commit::UnitId::Chunk(worker as i64);
    let mut src = MongoSource::connect(url, plan.source.tls.as_ref(), plan.source.mongo.as_ref())?
        .with_id_range(lo, hi);

    let mut last: Option<String> = None;
    let mut page = 0usize;

    loop {
        // Test-only: a per-worker error mid-range (Err path, not a crash). A crash
        // cannot reach the drain at all, so it is blind to whether the drain
        // records durable parts before bailing — the same reason the chunked
        // runners needed RIVET_TEST_ERROR_AT rather than RIVET_TEST_PANIC_AT.
        if let Err(e) =
            crate::test_hook::maybe_error_at_index("mongo_parallel_worker", worker as i64)
        {
            anyhow::bail!("range {worker}: {e}");
        }
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
            &**dest,
            &base,
        )?
        else {
            break;
        };
        fan.observe(p.observed);
        for part in p.parts {
            fan.part(unit, part);
        }
        fan.contribute(unit, p.checksums);
        page += 1;

        if p.rows < kp.chunk_size {
            break;
        }
        match p.next_cursor {
            Some(v) => last = Some(v),
            None => anyhow::bail!(
                // last-good key carried in the message: a worker has no &mut summary
                // (its failure is collected by the FanIn), so the forensic value
                // rides error_message — which error_class reads as keyset_unreadable_key.
                "export '{}': parallel worker {} could not read the '{}' value to advance keyset \
                 (NULL or unsupported type) — last readable key: {}.",
                plan.export_name,
                worker,
                kp.key_column,
                last.as_deref().unwrap_or("<none>"),
            ),
        }
    }
    Ok(())
}
