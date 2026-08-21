//! Range sub-export synthesis for `apply --pool --split` (#167).
//!
//! The pool floors at `max(longest_export, total/M)`. When one export dominates
//! (a versioned table at 51 min on the field's 154-export refresh), that longest
//! IS the floor — extra slots buy nothing until the giant is itself divisible.
//! The pure planner ([`crate::pipeline::pool::advise_split`]) decides WHETHER and
//! into HOW MANY; this module realizes it: the one dominating [`ExportConfig`]
//! becomes N range sub-exports over its key span, each a first-class scheduler
//! unit the pool places on a separate slot with its own write leg — so N of them
//! run concurrently and the single-export floor breaks.
//!
//! Merge-back (the design decision, #167): the N units share ONE destination
//! prefix and all fold to the parent FAMILY ([`ExportConfig::family`] via the
//! [`SplitSynth`] marker), so the load view recombines them as one logical table
//! exactly as it reads any single export's prefix today. Their PART names carry
//! the unit name (`<name>#i`), so they never collide within the prefix — the same
//! run-unique-part-name property the sink already relies on, one axis wider.
//!
//! Scope v1: full/chunked/keyset only. An incremental split would need each
//! sub-range to carry its own cursor (non-trivial); [`splittable_key`] refuses it
//! so a split is never silently lossy.

use std::path::Path;

use crate::config::{Config, ExportConfig, ExportMode, SourceType, SplitSynth};
use crate::error::Result;

/// The N half-open key windows `(lo, hi]` that partition the whole key span,
/// given `n - 1` interior boundary values (ascending). The union is the entire
/// key space: the first window has NO floor (`lo = None`, so a key below the
/// probed min still lands) and the last NO ceil (`hi = None`, so a key above the
/// probed max still lands) — defer-nothing, drop-nothing, the same convention
/// `keyset::partition_ranges` uses. Pure.
fn windows(bounds: &[String]) -> Vec<(Option<String>, Option<String>)> {
    let mut out = Vec::with_capacity(bounds.len() + 1);
    let mut prev: Option<String> = None;
    for b in bounds {
        out.push((prev.clone(), Some(b.clone())));
        prev = Some(b.clone());
    }
    out.push((prev, None));
    out
}

/// Whether a source can be RANGE-SPLIT into `WHERE key > lo AND key <= hi` sub-exports.
/// MongoDB cannot: it has no inline SQL range literal ([`crate::source::query::inline_literal`]
/// is `unreachable!()` for it — a key window is expressed through the driver, not a textual
/// predicate), so a split unit would PANIC at plan build. `--pool --split` therefore leaves a
/// Mongo giant WHOLE (its own keyset/parallel path fans out differently) rather than crash.
pub(crate) fn source_type_supports_split(st: SourceType) -> bool {
    !matches!(st, SourceType::Mongo)
}

/// Whether an export may be split into range sub-exports, and on which key. A
/// split is only correct when the export (1) has a single ordered key to
/// partition over, (2) re-reads the whole range each run (NOT incremental — its
/// cursor is a single per-export value; NOT CDC — a stream), AND (3) runs in a
/// CHECKPOINT-RESUMABLE mode, so `--pool --split --resume` can resume a crashed
/// unit from its own checkpoint (#167) rather than re-run it fresh and duplicate
/// its partial parts. The two resumable shapes are `chunk_by_key` (keyset,
/// resumable via its range checkpoint in any mode) and `chunk_column` in
/// `mode: chunked` (range chunking, resumable via the chunk checkpoint).
///
/// A plain `mode: full` export (chunk_column ignored) is NOT split — it has no
/// per-unit checkpoint, so a crashed unit could not resume without duplicating.
/// Returns the key column, or `None` (leave the export whole).
pub(crate) fn splittable_key(export: &ExportConfig) -> Option<String> {
    if export.mode == ExportMode::Cdc {
        return None; // a stream, not a range scan
    }
    if export.cursor_column.is_some() {
        return None; // incremental: a single per-export cursor, not per-range
    }
    if export.keyset_incremental {
        // Append-only incremental-by-key: on a clean re-run each unit would continue
        // from ITS OWN window's high-water mark, but the partition is re-derived every
        // run — so the per-unit water marks and the fresh windows disagree and rows are
        // silently dropped/duplicated across runs. This is the "incremental split needs
        // a per-range cursor" case scope v1 refuses; keep the export WHOLE (its own
        // keyset_incremental high-water is correct only unsplit).
        return None;
    }
    if let Some(key) = &export.chunk_by_key {
        return Some(key.clone()); // keyset (non-incremental) — resumable in any mode
    }
    if export.mode == ExportMode::Chunked
        && let Some(col) = &export.chunk_column
    {
        return Some(col.clone()); // range chunking — resumable via chunk checkpoint
    }
    None
}

/// Realize a split: the dominating `base` export becomes `bounds.len() + 1` range
/// sub-exports named `<base>#0..#N-1`, each bounded to its key window and folding
/// to the parent family. `bounds` are the `N - 1` interior key boundaries
/// (ascending) from the pool's min/max probe. Every other field is inherited by
/// clone, so each unit runs the base's mode/format/parallelism over its slice.
///
/// Pure and total: it does not probe the source (the caller supplies `bounds`),
/// so it is unit-testable without a database. Returns the base unchanged in a
/// one-element vec if `bounds` is empty (nothing to split into).
pub(crate) fn synthesize(
    base: &ExportConfig,
    key_column: &str,
    bounds: &[String],
) -> Vec<ExportConfig> {
    let wins = windows(bounds);
    if wins.len() < 2 {
        return vec![base.clone()];
    }
    let parent = base.family();
    wins.into_iter()
        .enumerate()
        .map(|(i, (lo, hi))| {
            let mut e = base.clone();
            e.name = format!("{}#{i}", base.name);
            // Crash-recovery ON so `--pool --split --resume` resumes a crashed unit from its
            // own checkpoint (#167 per-unit resume): the runner reuses the in-progress run_id
            // for MANIFEST IDENTITY and REHYDRATES the file_log-committed parts into this run's
            // manifest (rehydrate_manifest_parts_from_file_log) — it does NOT overwrite them in
            // place (part names carry a per-invocation wall-clock stamp / random nonce, not the
            // run_id, so a resume writes DIFFERENTLY-named parts). A part written but not yet
            // file_log-committed before the crash is therefore neither rehydrated nor overwritten
            // — it lingers as an unmanifested cloud orphan that `gc_orphans` reclaims (a delete
            // IS eventually needed on cloud). No dup/loss: the loader is manifest-authoritative,
            // so the orphan is ignored and the resume re-reads that page. Completed units are
            // skipped by the pool; never-started units run fresh. Crash-recovery only (never the
            // append-only keyset_incremental), so a clean re-run does a full pass over the window.
            e.chunk_checkpoint = true;
            e.split = Some(SplitSynth {
                parent: parent.clone(),
                key_column: key_column.to_string(),
                lo,
                hi,
            });
            e
        })
        .collect()
}

/// Probe the giant's key span and realize the split into `n` range sub-exports.
///
/// The RUNTIME half of the synthesis: it opens a source connection to sample the
/// `n - 1` ROW-percentile key boundaries (`keyset::sample_key_boundaries`, an
/// index-only OFFSET skip — the same sampler the parallel-keyset runner uses),
/// then hands them to the pure [`synthesize`]. Bounded to full/chunked/keyset via
/// [`splittable_key`]; returns `None` (leave the giant whole) when it is not
/// splittable or the probe finds too few distinct keys to partition — a
/// degenerate split is never worse than not splitting.
///
/// Errors from the probe are RETURNED, not swallowed: `--split` is an explicit
/// opt-in, so a probe that cannot run is a loud failure the operator asked for,
/// not a silent fall-back to the un-split giant (which would quietly deliver the
/// makespan they were trying to avoid).
pub(crate) fn probe_and_synthesize(
    config: &Config,
    giant: &ExportConfig,
    config_dir: &Path,
    n: usize,
) -> Result<Option<Vec<ExportConfig>>> {
    // MongoDB has no inline SQL range literal, so a range-split unit would panic at plan
    // build (inline_literal is unreachable for Mongo). Leave a Mongo giant whole — fail
    // fast BEFORE the boundary probe, never crash. Every SQL engine range-splits normally.
    if !source_type_supports_split(config.source.source_type) {
        return Ok(None);
    }
    let Some(key) = splittable_key(giant) else {
        return Ok(None);
    };
    if n < 2 {
        return Ok(None);
    }
    // Build the giant's OWN plan (base_query = the whole table; no split marker
    // yet) so the boundary sample runs over exactly the rows the sub-exports will
    // cover. `resume=false`: the probe is read-only.
    let plan = crate::plan::build_plan(config, giant, config_dir, false, false, false, None)?;
    let mut src = crate::source::create_source(&plan.source)?;
    // NULL-keyed guard for a RANGE split (chunk_column). Each unit's window is
    // `key > lo AND key <= hi`, which excludes NULL (SQL 3-valued logic), and the
    // per-unit `bail_if_null_keyed` is BLIND on a split unit because it probes the
    // already-windowed subquery — so NULL-keyed rows would be SILENTLY DROPPED while
    // every unit reports success. The un-split chunked path bails loudly (and
    // chunk_dense covers NULLs); the split cannot, so refuse it here against the WHOLE
    // table's key domain, before any unit runs. Keyset (chunk_by_key) keys are
    // planner-enforced NOT NULL — exempt; only chunk_column/dense range keys can be null.
    if giant.chunk_by_key.is_none() && giant.chunk_column.is_some() {
        let probe =
            crate::sql::null_key_probe_sql(config.source.source_type, &key, &plan.base_query);
        if src.as_mut().query_scalar(&probe)?.is_some() {
            anyhow::bail!(
                "export '{}': `--pool --split` over chunk_column '{}' would SILENTLY DROP \
                 NULL-keyed rows — each range sub-export's window excludes NULL, and unlike the \
                 un-split path (which bails or, with chunk_dense, covers them) the split has no \
                 way to carry them. Fix one of: use a NOT NULL column; add `WHERE {} IS NOT NULL` \
                 to drop them explicitly; or run this export with `mode: full` (unsplit).",
                giant.name,
                key,
                key
            );
        }
    }
    let bounds = super::keyset::sample_key_boundaries(src.as_mut(), &plan, &key, n, None, None)?;
    if bounds.is_empty() {
        // Too few distinct keys to partition (a tiny or single-valued key) —
        // splitting would make one real unit plus empties. Leave it whole.
        return Ok(None);
    }
    Ok(Some(synthesize(giant, &key, &bounds)))
}

/// The set of split-unit export names that ALREADY completed into the shared
/// prefix — a unit is complete iff it left a run-unique manifest COPY with status
/// `Success` (the durable, mode-agnostic, cloud-safe completion signal; a crashed
/// unit never finalizes one). `--pool --split --resume` reads this ONCE per giant
/// and skips the named units, re-running only the rest (#167 per-unit resume).
///
/// `dest_config` is the giant's destination; it is expanded with the FAMILY (the
/// same `{export}`→family resolution `build_plan` does for a split unit) so it
/// points at the ONE shared prefix. Best-effort: any listing/read/parse failure
/// yields an empty set (the unit is treated as incomplete and re-run — safe,
/// never a skip that could drop data). Read-only.
/// Read every run-unique unit manifest under the split prefix — the shared listing
/// scaffolding both [`completed_units_in_prefix`] and [`reconstruct_units_from_prefix`]
/// need (expand the destination for today's family, list the prefix, parse each
/// `manifest-<run_id>.json`). A dest/list/read/parse failure yields an empty vec; both
/// callers read "nothing found" as "no prior split". Read-only.
fn read_unit_manifests(
    dest_config: &crate::config::DestinationConfig,
    family: &str,
) -> Vec<crate::manifest::RunManifest> {
    use crate::manifest::{RunManifest, is_run_unique_manifest_name};
    let ctx = crate::destination::placeholder::PlaceholderContext::for_today(family);
    let expanded = crate::destination::placeholder::expand_destination(dest_config.clone(), &ctx);
    let Ok(dest) = crate::destination::create_destination(&expanded) else {
        return Vec::new();
    };
    let Ok(listing) = dest.list_prefix("") else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for meta in listing {
        let base = meta.key.rsplit('/').next().unwrap_or("");
        if !is_run_unique_manifest_name(base) {
            continue;
        }
        if let Ok(bytes) = dest.read(&meta.key)
            && let Ok(m) = serde_json::from_slice::<RunManifest>(&bytes)
        {
            out.push(m);
        }
    }
    out
}

/// The Success unit names under the split prefix — the per-unit resume skip set. KNOWN LIMIT
/// (post-0.24.3 review, PLAUSIBLE-HIGH): this is GENERATION-BLIND — it collects Success across
/// EVERY manifest copy in the prefix, so if an OLDER split generation (a prior full run into a
/// fixed, non-{run_id} prefix) completed the same ordinal, its Success masks a CURRENT-generation
/// unit that crashed, and the skip drops it. The silent-loss chain requires a full split export
/// run REPEATEDLY into one accumulating prefix — an atypical pattern — AND is caught at LOAD by
/// the tiling backstop `manifest::census::ensure_single_generation` (a mixed-generation
/// selection whose windows do not tile is REFUSED loudly, not silently gapped/duplicated). The precise run-side
/// fix is generation-scoping (a generation id on `SplitWindow`, or matching the reconstructed
/// window per ordinal before skipping) — tracked; the load guard makes the interim non-silent.
pub(crate) fn completed_units_in_prefix(
    dest_config: &crate::config::DestinationConfig,
    family: &str,
) -> std::collections::BTreeSet<String> {
    use crate::manifest::ManifestStatus;
    read_unit_manifests(dest_config, family)
        .into_iter()
        .filter(|m| matches!(m.status, ManifestStatus::Success))
        .map(|m| m.export_name)
        .collect()
}

/// Reconstruct the split into the EXACT partition a PRIOR run used, from the windows its
/// units persisted in the shared prefix — the fix for finding 2. `--split --resume` must NOT
/// re-sample (`sample_key_boundaries` is offset/percentile-based, so a source that grew
/// between crash and resume yields DIFFERENT boundaries, and skipping completed units by
/// ordinal name then covers a different key range than was exported → silent gap). Instead we
/// read every COMPLETED unit's manifest (a Success/terminal manifest carries its window; a
/// HARD-crashed unit leaves none) and rebuild the boundary list POSITIONALLY by ordinal
/// (`{giant}#i`), never by value-sorting the strings (a numeric key would sort lexically
/// wrong). Each boundary `i` is `unit#i.hi` = `unit#(i+1).lo`, so it survives ONE unit
/// crashing (recovered from a neighbor). Two ADJACENT crashes lose the boundary between them;
/// the fill logic below reconstructs the FULL ordinal partition anyway (an unrecoverable
/// interior boundary becomes an empty window, never a truncation) so a surviving completed
/// unit above the gap is neither re-covered (dup) nor abandoned to a re-sample (gap). Returns
/// `None` (caller re-samples: a genuine first run) only when NO unit windows are found at all.
/// Read-only.
pub(crate) fn reconstruct_units_from_prefix(
    dest_config: &crate::config::DestinationConfig,
    family: &str,
    giant: &ExportConfig,
) -> Option<Vec<ExportConfig>> {
    // A resume must not RESURRECT a split the fresh path would now refuse. `probe_and_synthesize`
    // gates new splits through `splittable_key` (e.g. it refuses `keyset_incremental`), but
    // reconstruct rebuilds from on-disk unit manifests and would bypass that gate — so a config
    // that turned unsafe-to-split since the prior run (a flipped `keyset_incremental`, an added
    // `cursor_column`) would resume its old split and silently drop/duplicate. Honour the same
    // gate here: not splittable → leave the giant whole (probe agrees, so no split runs).
    splittable_key(giant)?;
    let prefix = format!("{}#", giant.name); // this giant's unit names: "{giant}#<ordinal>"
    let mut key: Option<String> = None;
    // boundary position -> value. Position i is the boundary between unit#i and unit#(i+1),
    // i.e. unit#i.hi and unit#(i+1).lo — collected from BOTH so a single crashed unit's
    // boundaries survive via its neighbours.
    let mut bmap: std::collections::BTreeMap<usize, String> = std::collections::BTreeMap::new();
    // Which ordinals left a manifest — a unit that completed (or at least window-committed). Used
    // to decide whether the reconstructed OPEN TAIL is a genuine completed tail or a WIDENED one
    // absorbing a trailing crash (below).
    let mut manifested: std::collections::HashSet<usize> = std::collections::HashSet::new();
    for m in read_unit_manifests(dest_config, family) {
        let (Some(w), Some(ord)) = (
            m.split_window.as_ref(),
            m.export_name
                .strip_prefix(&prefix)
                .and_then(|s| s.parse::<usize>().ok()),
        ) else {
            continue; // not a unit of THIS giant, or not a split manifest
        };
        manifested.insert(ord);
        key.get_or_insert_with(|| w.key_column.clone());
        if let Some(h) = &w.hi {
            bmap.insert(ord, h.clone()); // boundary[ord]
        }
        if let Some(l) = &w.lo
            && ord > 0
        {
            bmap.insert(ord - 1, l.clone()); // boundary[ord-1]
        }
    }
    let key = key?;
    // Rebuild the FULL boundary vector [0..=max_pos], preserving the ORIGINAL unit
    // COUNT and ordinals. A run of ADJACENT crashes can leave an interior boundary
    // position that NEITHER a hi nor a lo could supply — a HARD crash (SIGKILL / OOM /
    // tunnel drop) writes no window-bearing manifest at all (the running marker carries
    // `split_window: None` and is cloud-only), so two adjacent hard-crashed units lose
    // the boundary between them. We FILL such a hole rather than truncate:
    //   * forward-fill carries the last known boundary down into the hole (the
    //     intervening unit becomes an empty `(b, b]` window — harmless, exports 0 rows);
    //   * a LEADING hole (position 0 unrecoverable) is then back-filled from the first
    //     known boundary.
    // This keeps every COMPLETED unit at its exact original ordinal+window so the
    // name-based skip still lines up, and covers each gap exactly once. The earlier
    // "take contiguously from 0, stop at the first gap" was wrong TWO ways, both proven
    // by the adversarial pre-merge review: (1) a leading gap left `boundaries` empty →
    // `None` → the caller re-samples → the exact finding-2 silent GAP; (2) an interior
    // gap DISCARDED every recoverable higher boundary → a coarser open-ended tail unit
    // RE-COVERED a surviving completed unit above the gap → DUPLICATE rows (distinct
    // part names, so no overwrite; both manifest-declared → double-counted at load).
    // (A trailing adjacent crash lowers `max_pos` naturally, so the last unit is an
    // open `(b, None]` that absorbs the crashed tail — no completed unit sits above it,
    // so that coarsening is genuinely loss- AND dup-free.)
    let &max_pos = bmap.keys().next_back()?; // no interior boundaries → None (first run)
    // Boundary positions NO surviving unit could supply — the adjacent-crash holes the fill
    // synthesizes. A unit whose window TOUCHES a filled position has a window that DIFFERS from
    // the one its own pre-crash per-unit checkpoint was taken over; resuming that stale
    // checkpoint against the changed window silently DUPLICATES or LOSES rows (chunked bailed
    // loud on the plan-fingerprint mismatch, keyset had no guard at all — post-0.24.3 review
    // HIGH). Such units must run FRESH; recorded here, enforced after `synthesize` below.
    let filled: std::collections::HashSet<usize> =
        (0..=max_pos).filter(|i| !bmap.contains_key(i)).collect();
    let mut boundaries: Vec<Option<String>> =
        (0..=max_pos).map(|i| bmap.get(&i).cloned()).collect();
    let mut last: Option<String> = None;
    for slot in boundaries.iter_mut() {
        match slot.as_ref() {
            Some(v) => last = Some(v.clone()),
            None => slot.clone_from(&last),
        }
    }
    let mut next: Option<String> = None;
    for slot in boundaries.iter_mut().rev() {
        match slot.as_ref() {
            Some(v) => next = Some(v.clone()),
            None => slot.clone_from(&next),
        }
    }
    // After both fill passes every slot is Some: `max_pos` came from a real bmap key, so
    // at least one slot is Some; forward-fill covers every slot after the first Some and
    // back-fill every slot before it. So `flatten()` yields exactly `max_pos + 1` — the
    // invariant is documented, not a live branch (the fill makes a partial set impossible).
    let boundaries: Vec<String> = boundaries.into_iter().flatten().collect();
    debug_assert_eq!(boundaries.len(), max_pos + 1);

    let mut units = synthesize(giant, &key, &boundaries);
    // Unit i's window is `(boundaries[i-1], boundaries[i]]` (open at the ends). If EITHER of its
    // two boundary positions was FILLED, the window is synthetic — its stale per-unit checkpoint
    // no longer matches, so force a FRESH run (chunk_checkpoint off → the keyset/chunked resume
    // path reads no checkpoint and re-runs the whole window; the pre-crash orphan parts are
    // unmanifested and gc'd, so no duplicate/loss and no loud bail). A unit both of whose
    // boundaries are REAL (a single-crash exact recovery, or a completed unit that will be
    // skipped anyway) keeps crash-recovery and resumes normally.
    for (i, unit) in units.iter_mut().enumerate() {
        if (i > 0 && filled.contains(&(i - 1))) || filled.contains(&i) {
            unit.chunk_checkpoint = false;
        }
    }
    // The OPEN-ENDED tail unit `(b_max, None]` is the ONE unit whose window can have WIDENED
    // undetectably: a TRAILING adjacent crash (the top K units hard-crash with no manifest)
    // lowers `max_pos`, so the reconstructed tail absorbs the crashed units' key range while
    // keeping its name — but its two boundary positions are BOTH real (its lo is a surviving
    // bmap key; its open top is not a `filled` hole), so the loop above never marks it fresh.
    // On the PARALLEL keyset runner that is a silent loss: the tail reloads its OLD persisted
    // ranges, and the DONE `hi=None` range (physically capped at the old ceil by run-1's
    // base_query) is skipped from `pending` and merely rehydrated — so the widened region
    // `(old_ceil, max]` is scanned by nobody (post-0.24.3 convergence HIGH; a prior window-
    // fingerprint guard was reverted because the persisted outer bounds are always (None,None),
    // making runtime detection impossible). The tail's ordinal is `max_pos + 1`; if that ordinal
    // left NO manifest it either crashed or absorbed a trailing crash — its window may have
    // widened, so run it FRESH (re-sample over the CURRENT window; the pre-crash orphan parts are
    // unmanifested + gc'd, no dup/loss). If the tail ordinal DID leave a manifest it is a genuine
    // completed/exact tail (skipped by the pool, or resumed at its true window) and keeps its
    // checkpoint — so an interior single-crash resume is untouched. Cost: a tail re-read only on a
    // tail-crash resume.
    if !manifested.contains(&(max_pos + 1))
        && let Some(tail) = units.last_mut()
    {
        tail.chunk_checkpoint = false;
    }
    Some(units)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::sample_export;

    #[test]
    fn windows_partition_the_key_space_gap_free_and_overlap_free() {
        let w = windows(&["10".into(), "20".into(), "30".into()]);
        assert_eq!(
            w,
            vec![
                (None, Some("10".into())),
                (Some("10".into()), Some("20".into())),
                (Some("20".into()), Some("30".into())),
                (Some("30".into()), None),
            ],
            "4 windows for 3 boundaries: first no-floor, last no-ceil, each lo == prev hi"
        );
    }

    #[test]
    fn synthesize_makes_n_units_that_fold_to_the_parent_family() {
        let base = {
            let mut e = sample_export("daily");
            e.mode = ExportMode::Chunked;
            e.chunk_by_key = Some("id".into());
            e
        };
        let units = synthesize(&base, "id", &["1000".into(), "2000".into()]);
        assert_eq!(units.len(), 3, "2 boundaries → 3 range units");
        assert_eq!(
            units.iter().map(|u| u.name.clone()).collect::<Vec<_>>(),
            vec!["daily#0", "daily#1", "daily#2"]
        );
        // Every unit folds to the ONE family so the load view merges them.
        for u in &units {
            assert_eq!(u.family(), "daily", "unit {} must fold to family", u.name);
        }
        // The windows tile the key space: first no-floor, last no-ceil, adjacent
        // lo == prev hi.
        let ranges: Vec<_> = units
            .iter()
            .map(|u| {
                let s = u.split.as_ref().unwrap();
                (s.lo.clone(), s.hi.clone())
            })
            .collect();
        assert_eq!(ranges[0], (None, Some("1000".into())));
        assert_eq!(ranges[1], (Some("1000".into()), Some("2000".into())));
        assert_eq!(ranges[2], (Some("2000".into()), None));
    }

    #[test]
    fn synthesize_with_no_boundaries_leaves_the_export_whole() {
        let base = sample_export("solo");
        let units = synthesize(&base, "id", &[]);
        assert_eq!(units.len(), 1, "no boundaries → no split");
        assert_eq!(units[0].name, "solo");
        assert!(
            units[0].split.is_none(),
            "an unsplit export carries no marker"
        );
    }

    #[test]
    fn incremental_and_cdc_exports_are_not_splittable() {
        // chunked over a key → splittable
        let mut chunked = sample_export("t");
        chunked.mode = ExportMode::Chunked;
        chunked.chunk_by_key = Some("id".into());
        assert_eq!(splittable_key(&chunked).as_deref(), Some("id"));

        // incremental (cursor set) → NOT splittable (single per-export cursor)
        let mut incr = chunked.clone();
        incr.cursor_column = Some("updated_at".into());
        assert!(
            splittable_key(&incr).is_none(),
            "an incremental export must not split — the cursor is per-export, not per-range"
        );

        // CDC → NOT splittable (a stream, not a range scan)
        let mut cdc = sample_export("c");
        cdc.mode = ExportMode::Cdc;
        cdc.chunk_by_key = Some("id".into());
        assert!(
            splittable_key(&cdc).is_none(),
            "CDC is a stream, not a range"
        );

        // no key at all → nothing to partition over
        let mut nokey = sample_export("n");
        nokey.mode = ExportMode::Chunked;
        assert!(splittable_key(&nokey).is_none());

        // append-only incremental-by-key (keyset_incremental) → NOT splittable: a split
        // re-derives the partition every run, so per-unit high-water marks and fresh
        // windows disagree and rows are silently dropped/duplicated across re-runs. Scope
        // v1 refuses an incremental split; the unsplit keyset_incremental is correct.
        let mut ks_incr = chunked.clone();
        ks_incr.keyset_incremental = true;
        assert!(
            splittable_key(&ks_incr).is_none(),
            "an append-only keyset_incremental export must not split — the per-unit \
             high-water mark disagrees with a re-derived partition (silent drop/dup)"
        );
    }

    // Build a minimal SUCCESS unit manifest carrying a split window, and write it as a
    // run-unique copy into `dir` — exactly what a completed split unit leaves on the
    // destination. Fields irrelevant to reconstruction get harmless defaults.
    fn write_unit_manifest(
        dir: &std::path::Path,
        unit_name: &str,
        run_id: &str,
        lo: Option<&str>,
        hi: Option<&str>,
    ) {
        use crate::manifest::{
            MANIFEST_VERSION, ManifestDestination, ManifestSource, ManifestStatus, RunManifest,
            SplitWindow, run_unique_manifest_name,
        };
        let m = RunManifest {
            split_window: Some(SplitWindow {
                key_column: "id".into(),
                lo: lo.map(String::from),
                hi: hi.map(String::from),
            }),
            checksum_render: None,
            row_hash: None,
            mode: "batch".into(),
            manifest_version: MANIFEST_VERSION,
            run_id: run_id.into(),
            export_name: unit_name.into(),
            export_family: "daily".into(),
            started_at: "t".into(),
            finished_at: "t".into(),
            status: ManifestStatus::Success,
            source: ManifestSource {
                engine: "pg".into(),
                schema: None,
                table: None,
                extraction: None,
            },
            destination: ManifestDestination {
                kind: "local".into(),
                uri: "file:///x".into(),
            },
            format: "parquet".into(),
            compression: "zstd".into(),
            schema_fingerprint: "xxh3:0".into(),
            row_count: 1,
            part_count: 1,
            parts: vec![],
            column_checksums: None,
            checksum_key_column: None,
        };
        let name = run_unique_manifest_name(run_id);
        std::fs::write(dir.join(name), serde_json::to_vec(&m).unwrap()).unwrap();
    }

    #[test]
    fn reconstruct_recovers_the_exact_partition_including_a_crashed_units_window() {
        // A 4-way split over `id` with boundaries 250/500/750. Unit #2 CRASHED mid-run —
        // it left NO manifest. The other three persisted their windows. On `--resume`,
        // reconstruct must rebuild the EXACT original 4-way partition — the whole point of
        // finding 2 — because re-sampling `sample_key_boundaries` against a source that
        // GREW between crash and resume would shift the boundaries and the ordinal-name
        // skip would then cover the wrong ranges, silently dropping the crashed unit's
        // original key range. Critically, unit #2's window (500, 750] survives via its
        // NEIGHBOURS: #1.hi=500 gives boundary[1], #3.lo=750 gives boundary[2].
        let dir = tempfile::tempdir().unwrap();
        let mut giant = sample_export("daily");
        giant.mode = ExportMode::Chunked;
        giant.chunk_by_key = Some("id".into());
        giant.destination.path = Some(dir.path().to_string_lossy().into_owned());

        write_unit_manifest(dir.path(), "daily#0", "r0", None, Some("250"));
        write_unit_manifest(dir.path(), "daily#1", "r1", Some("250"), Some("500"));
        // unit #2 crashed → NO manifest written
        write_unit_manifest(dir.path(), "daily#3", "r3", Some("750"), None);

        let units = reconstruct_units_from_prefix(&giant.destination, "daily", &giant)
            .expect("reconstruct must recover the partition from the persisted windows");

        // The EXACT original partition, gap-free and overlap-free — NOT a re-sample of the
        // grown source. Unit #2's (500, 750] is present despite #2 leaving no manifest.
        assert_eq!(
            wins_of(&units),
            vec![
                (None, Some("250".into())),
                (Some("250".into()), Some("500".into())),
                (Some("500".into()), Some("750".into())),
                (Some("750".into()), None),
            ],
            "reconstruct must rebuild the original 4-way partition (incl. the crashed \
             unit's window recovered from its neighbours), not a re-sampled one"
        );
        // Names must line up with ordinals so the pool's completed-unit skip stays correct.
        let names: Vec<&str> = units.iter().map(|u| u.name.as_str()).collect();
        assert_eq!(names, vec!["daily#0", "daily#1", "daily#2", "daily#3"]);
    }

    // Helper: the (lo, hi) window of each reconstructed unit, in ordinal order.
    fn wins_of(units: &[ExportConfig]) -> Vec<(Option<String>, Option<String>)> {
        units
            .iter()
            .map(|u| {
                let s = u
                    .split
                    .as_ref()
                    .expect("reconstructed unit carries a split window");
                (s.lo.clone(), s.hi.clone())
            })
            .collect()
    }

    #[test]
    fn reconstruct_refuses_to_resurrect_a_now_unsplittable_export_on_resume() {
        // A prior run split this export and left unit manifests; the config has since flipped
        // `keyset_incremental: true` (append-only), which `splittable_key` now refuses. Resume
        // must NOT rebuild the old split from the on-disk manifests (that would bypass the
        // fresh-path guard and drop/duplicate) — reconstruct returns None, so the giant runs
        // whole (probe_and_synthesize also returns None for it).
        let dir = tempfile::tempdir().unwrap();
        let mut giant = sample_export("daily");
        giant.mode = ExportMode::Chunked;
        giant.chunk_by_key = Some("id".into());
        giant.destination.path = Some(dir.path().to_string_lossy().into_owned());
        // On-disk split units from the prior (splittable) run.
        write_unit_manifest(dir.path(), "daily#0", "r0", None, Some("500"));
        write_unit_manifest(dir.path(), "daily#1", "r1", Some("500"), None);
        // The config is now keyset_incremental → not splittable.
        giant.keyset_incremental = true;
        assert!(
            reconstruct_units_from_prefix(&giant.destination, "daily", &giant).is_none(),
            "a resume must not resurrect a split for a config that is no longer splittable"
        );
    }

    #[test]
    fn reconstruct_runs_the_open_tail_fresh_after_a_trailing_adjacent_crash() {
        // #0, #1 completed; the top TWO units #2, #3 both HARD-crash (no manifest). max_pos drops
        // to 1, so reconstruct's tail #2 WIDENS from its original (500,750] to the open (500,None],
        // absorbing crashed #3's key range. Its boundaries are both "real" (lo=500 is a bmap key,
        // the open top is not a `filled` hole), so the interior fresh-marking never fires. On the
        // PARALLEL keyset runner that widened tail would reload its OLD ranges and SKIP the done
        // hi=None range (rehydrated only to run-1's ceil) — silently dropping (750, max]
        // (post-0.24.3 convergence HIGH). The open tail must therefore run FRESH. RED against the
        // pre-fix reconstruct (tail kept chunk_checkpoint=true).
        let dir = tempfile::tempdir().unwrap();
        let mut giant = sample_export("daily");
        giant.mode = ExportMode::Chunked;
        giant.chunk_by_key = Some("id".into());
        giant.destination.path = Some(dir.path().to_string_lossy().into_owned());

        // #0, #1 completed; #2, #3 crashed (no manifest) — a trailing double-crash.
        write_unit_manifest(dir.path(), "daily#0", "r0", None, Some("250"));
        write_unit_manifest(dir.path(), "daily#1", "r1", Some("250"), Some("500"));

        let units = reconstruct_units_from_prefix(&giant.destination, "daily", &giant)
            .expect("reconstruct");
        assert_eq!(
            wins_of(&units),
            vec![
                (None, Some("250".into())),               // #0 exact (completed, skipped)
                (Some("250".into()), Some("500".into())), // #1 exact (completed, skipped)
                (Some("500".into()), None),               // #2 WIDENED tail — must run fresh
            ],
            "a trailing double-crash rebuilds a 3-unit partition with an open widened tail"
        );
        assert_eq!(
            units.iter().map(|u| u.chunk_checkpoint).collect::<Vec<_>>(),
            vec![true, true, false],
            "the open tail must run FRESH (chunk_checkpoint=false) so the widened region is \
             re-sampled, not resumed from stale ranges that skip the done hi=None range"
        );
    }

    #[test]
    fn reconstruct_covers_a_leading_adjacent_crash_instead_of_re_sampling() {
        // Two ADJACENT LOW-end units (#0, #1) HARD-crash — no manifest at all (a
        // SIGKILL/OOM leaves none). #2 and #3 completed. Boundary position 0 is then
        // unrecoverable (needs #0.hi or #1.lo). The OLD "contiguous-from-0" loop found
        // nothing at position 0, returned None, and the caller RE-SAMPLED the (grown)
        // source → the exact finding-2 silent gap. Reconstruct must instead back-fill the
        // leading hole and rebuild the FULL 4-unit partition, so the completed #2/#3 keep
        // their exact windows and #0 covers the whole crashed leading range.
        let dir = tempfile::tempdir().unwrap();
        let mut giant = sample_export("daily");
        giant.mode = ExportMode::Chunked;
        giant.chunk_by_key = Some("id".into());
        giant.destination.path = Some(dir.path().to_string_lossy().into_owned());

        // #0, #1 crashed (no manifest); #2, #3 completed.
        write_unit_manifest(dir.path(), "daily#2", "r2", Some("500"), Some("750"));
        write_unit_manifest(dir.path(), "daily#3", "r3", Some("750"), None);

        let units = reconstruct_units_from_prefix(&giant.destination, "daily", &giant)
            .expect("a leading adjacent crash must NOT collapse to None (that re-samples)");
        assert_eq!(
            wins_of(&units),
            vec![
                (None, Some("500".into())), // #0 covers the crashed (None,250]+(250,500]
                (Some("500".into()), Some("500".into())), // #1 empty (interior hole filled)
                (Some("500".into()), Some("750".into())), // #2 exact (completed, skipped)
                (Some("750".into()), None), // #3 exact (completed, skipped)
            ],
            "leading hole must be back-filled into a full 4-unit partition, not re-sampled"
        );
    }

    #[test]
    fn reconstruct_fills_an_interior_adjacent_crash_without_overlapping_a_survivor() {
        // #0 and #3 completed; #1 and #2 both HARD-crash (no manifest). The interior
        // boundary between #1 and #2 is unrecoverable. The OLD loop stopped at that gap,
        // DISCARDED the recoverable higher boundary (750), and synthesized a coarse
        // open-ended #1=(250,None] that RE-COVERED the completed #3=(750,None] → duplicate
        // rows at load. Reconstruct must keep the full 4-unit partition: the crashed span
        // becomes a bounded (250,750] unit (plus an empty filler) that ends exactly where
        // the surviving #3 begins — no overlap.
        let dir = tempfile::tempdir().unwrap();
        let mut giant = sample_export("daily");
        giant.mode = ExportMode::Chunked;
        giant.chunk_by_key = Some("id".into());
        giant.destination.path = Some(dir.path().to_string_lossy().into_owned());

        // #0, #3 completed; #1, #2 crashed (no manifest).
        write_unit_manifest(dir.path(), "daily#0", "r0", None, Some("250"));
        write_unit_manifest(dir.path(), "daily#3", "r3", Some("750"), None);

        let units = reconstruct_units_from_prefix(&giant.destination, "daily", &giant)
            .expect("reconstruct");
        assert_eq!(
            wins_of(&units),
            vec![
                (None, Some("250".into())),               // #0 exact (completed, skipped)
                (Some("250".into()), Some("250".into())), // #1 empty (interior hole filled)
                (Some("250".into()), Some("750".into())), // #2 covers the crashed span, BOUNDED
                (Some("750".into()), None),               // #3 exact (completed, skipped)
            ],
            "the re-run unit must end at 750 (where completed #3 begins) — never an \
             open-ended window that re-covers #3 and duplicates its rows"
        );
        // The load-bearing invariant: no reconstructed unit's window overlaps the
        // surviving completed #3 = (750, None].
        let overlaps_survivor = units.iter().any(|u| {
            let s = u.split.as_ref().unwrap();
            // a unit overlaps (750, +inf) iff its hi is None (open to +inf) AND it is not #3 itself
            s.hi.is_none() && s.lo.as_deref() != Some("750")
        });
        assert!(
            !overlaps_survivor,
            "no re-run unit may be open-ended below the surviving #3 boundary (would dup)"
        );

        // HIGH (post-0.24.3 review): a unit whose window was SYNTHESIZED by the fill must run
        // FRESH (chunk_checkpoint off), never resume its stale pre-crash checkpoint against the
        // changed window (keyset would silently dup/lose; chunked would loudly bail). #1 (empty,
        // touches the filled boundary 1) and #2 (merged, also touches it) → fresh; #0 and #3
        // (both boundaries real) keep crash-recovery.
        let ck: Vec<bool> = units.iter().map(|u| u.chunk_checkpoint).collect();
        assert_eq!(
            ck,
            vec![true, false, false, true],
            "fill-synthesized units (#1, #2) must run fresh; exact units (#0, #3) keep checkpoint"
        );
    }

    #[test]
    fn reconstruct_keeps_checkpoint_for_an_exactly_recovered_single_crash() {
        // A SINGLE crash (#2), both neighbours present → every boundary is real, no fill → the
        // crashed unit's window is EXACTLY its original, so its checkpoint is valid and it must
        // resume normally (chunk_checkpoint stays on for every unit).
        let dir = tempfile::tempdir().unwrap();
        let mut giant = sample_export("daily");
        giant.mode = ExportMode::Chunked;
        giant.chunk_by_key = Some("id".into());
        giant.destination.path = Some(dir.path().to_string_lossy().into_owned());
        write_unit_manifest(dir.path(), "daily#0", "r0", None, Some("250"));
        write_unit_manifest(dir.path(), "daily#1", "r1", Some("250"), Some("500"));
        // #2 crashed (no manifest) — recovered exactly from #1.hi (500) and #3.lo (750).
        write_unit_manifest(dir.path(), "daily#3", "r3", Some("750"), None);

        let units = reconstruct_units_from_prefix(&giant.destination, "daily", &giant)
            .expect("reconstruct");
        assert!(
            units.iter().all(|u| u.chunk_checkpoint),
            "an exactly-recovered single-crash resume must keep crash-recovery on every unit"
        );
    }

    #[test]
    fn mongo_source_is_not_range_split_capable() {
        // inline_literal is unreachable!() for Mongo, so a range split would PANIC at plan
        // build — probe_and_synthesize must leave a Mongo giant whole. Every SQL engine splits.
        assert!(
            !source_type_supports_split(SourceType::Mongo),
            "Mongo has no inline SQL range literal — range-splitting it would panic"
        );
        assert!(source_type_supports_split(SourceType::Postgres));
        assert!(source_type_supports_split(SourceType::Mysql));
        assert!(source_type_supports_split(SourceType::Mssql));
    }
}
