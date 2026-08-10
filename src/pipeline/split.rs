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

use crate::config::{Config, ExportConfig, ExportMode, SplitSynth};
use crate::error::Result;

/// The N half-open key windows `(lo, hi]` that partition the whole key span,
/// given `n - 1` interior boundary values (ascending). The union is the entire
/// key space: the first window has NO floor (`lo = None`, so a key below the
/// probed min still lands) and the last NO ceil (`hi = None`, so a key above the
/// probed max still lands) — defer-nothing, drop-nothing, the same convention
/// `keyset::partition_ranges` uses. Pure.
#[allow(dead_code)]
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

/// Whether an export may be split into range sub-exports. A split is only correct
/// when (1) it has a single ordered key to partition over — `chunk_by_key` or
/// `chunk_column` — and (2) its mode re-reads the whole range on each run
/// (full/chunked/keyset), NOT incremental (which would need a per-sub-range
/// cursor, out of v1) and NOT CDC (a stream, not a range scan). Returns the key
/// column to split over, or `None` (leave the export whole).
pub(crate) fn splittable_key(export: &ExportConfig) -> Option<String> {
    // CDC is a stream, not a range scan — never splittable. A whole-table
    // full/chunked/keyset scan is exactly what a range partition covers
    // losslessly; incremental is excluded just below (its cursor is per-export).
    if export.mode == ExportMode::Cdc {
        return None;
    }
    if export.cursor_column.is_some() {
        // An incremental export (cursor set) is excluded even in chunked mode —
        // the cursor high-water is a single per-export value, not per-range.
        return None;
    }
    export
        .chunk_by_key
        .clone()
        .or_else(|| export.chunk_column.clone())
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
    let bounds = super::keyset::sample_key_boundaries(src.as_mut(), &plan, &key, n, None, None)?;
    if bounds.is_empty() {
        // Too few distinct keys to partition (a tiny or single-valued key) —
        // splitting would make one real unit plus empties. Leave it whole.
        return Ok(None);
    }
    Ok(Some(synthesize(giant, &key, &bounds)))
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
    }
}
