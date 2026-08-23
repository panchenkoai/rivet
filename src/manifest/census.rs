//! **Layer: Trust contract**
//!
//! ONE census over the manifests under a destination prefix: given the
//! `(key, manifest)` pairs a reader listed, it answers *which runs live here and
//! how do they relate* — run enumeration + run-id dedupe, family membership,
//! split-unit identity, supersession/liveness, generation coherence, and the
//! per-family claimed-part set.
//!
//! Two verifiers ask those questions over the SAME artifact set — `--validate`
//! (`pipeline::validate_manifest`, "is every declared part present and is
//! anything here unaccounted for") and `rivet load`
//! (`load::reconcile`, "which runs may I load, and what may I delete"). They used
//! to each define the relations for themselves, in two dialects: validate keyed a
//! split unit off `split_window`, reconcile off the `{family}#i` export-NAME
//! pattern; each dialect was blind exactly where the other could see, and every
//! split bug of the 0.24.x pool/split hardening had to be diagnosed against both.
//! The relations live here now; the two verifiers keep only their distinct
//! question, and the I/O that feeds them stays where it is (validate reads the
//! sidecars through a `Destination`, reconcile through the GCS store).
//!
//! Pure and borrow-only: a census is a view over the caller's slice — no I/O, no
//! clones, so both callers can build one per pass without paying for it.

use super::{
    ManifestStatus, PartStatus, RunManifest, identity_family, is_run_unique_manifest_name,
};
use anyhow::Result;

/// One run in the census: the manifest, the key it was read from, and the two
/// derived identities every relation below is phrased in — its FAMILY and, when
/// it is a `--pool --split` unit, its unit index.
#[derive(Debug)]
pub struct CensusRun<'a> {
    /// Position in the slice the census was built from. Lets a caller that owns
    /// that slice materialise a selection without cloning the manifests.
    pub index: usize,
    /// Storage key the manifest was read from (the canonical `manifest.json`
    /// pointer or an immutable `manifest-<run_id>.json` copy).
    pub key: &'a str,
    pub manifest: &'a RunManifest,
    family: &'a str,
    unit: Option<&'a str>,
}

impl<'a> CensusRun<'a> {
    /// The family this run belongs to — [`identity_family`] resolved ONCE, over
    /// the whole census, so a legacy (family-less) manifest joins the recorded
    /// family of a manifest sharing its `export_name` instead of standing alone.
    pub fn family(&self) -> &'a str {
        self.family
    }

    /// The `{family}#i` unit index when the export NAME carries one.
    pub fn unit_index(&self) -> Option<&'a str> {
        self.unit
    }

    /// Is this run a `--pool --split` UNIT of its family?
    ///
    /// The two signals are reconciled here, once, because each is blind where the
    /// other sees:
    /// - `split_window` is the precise mark a UNIT's terminal manifest carries —
    ///   but a unit's `running` MARKER carries `split_window: None` (the terminal
    ///   overwrites it), so supersession, which must read markers, cannot use it;
    /// - the `{family}#i` export NAME is all a marker carries — but it is a
    ///   naming convention, so a unit whose terminal manifest lost its name shape
    ///   would fall out of validate's presence check.
    ///
    /// Either signal is sufficient. This is where a stamped split-generation id
    /// lands when it exists (the tracked follow-up
    /// [`ensure_single_generation`] documents), replacing both.
    pub fn is_split_unit(&self) -> bool {
        self.unit.is_some() || self.manifest.split_window.is_some()
    }
}

/// The relations over one prefix's manifests. Build with [`ManifestCensus::new`];
/// every query is a pure read over the borrowed slice.
#[derive(Debug)]
pub struct ManifestCensus<'a> {
    runs: Vec<CensusRun<'a>>,
}

impl<'a> ManifestCensus<'a> {
    /// Census the `(key, manifest)` pairs a reader listed under one prefix.
    ///
    /// Run enumeration applies the run-id dedupe ([`dedupe_by_run_id`]) so the
    /// canonical pointer and its immutable copy never count as two runs; family
    /// and unit index are resolved once per run.
    pub fn new(keyed: &'a [(String, RunManifest)]) -> Self {
        let runs = keep_one_per_run(keyed)
            .into_iter()
            .enumerate()
            .filter(|(_, keep)| *keep)
            .map(|(index, _)| {
                let (key, manifest) = &keyed[index];
                let family = identity_family(manifest, keyed);
                CensusRun {
                    index,
                    key: key.as_str(),
                    manifest,
                    family,
                    unit: unit_index(manifest, family),
                }
            })
            .collect();
        Self { runs }
    }

    /// Every run under the prefix, in listing order.
    pub fn runs(&self) -> &[CensusRun<'a>] {
        &self.runs
    }

    /// A `running` manifest is SUPERSEDED when a NEWER run of the SAME family
    /// exists (a higher `started_at`) — it crashed and its successor already
    /// re-ran, so it no longer protects anything. The ONE clock-free staleness
    /// predicate, shared by [`Self::active_running`] (spare the non-superseded)
    /// and `gc_orphans`'s marker-GC sweep (delete the superseded). The ledger
    /// enforces the same rule in SQL — that copy cannot share this Rust.
    ///
    /// FAMILY, not name: a CDC run's `running` marker carries the EXPORT name
    /// while the drain's terminal manifest carries the TABLE string, so with
    /// `name:` ≠ `table:` a name-only compare never matched — a crashed marker
    /// stayed "active" forever and gc over-deferred cleanup permanently.
    ///
    /// BUT a `--pool --split` fans ONE family into N units (`{family}#0..#N-1`)
    /// that run CONCURRENTLY under that shared family. A later-STARTED sibling
    /// that finishes first must NOT make an earlier, still-live sibling look
    /// superseded — that would let a cross-host `gc_orphans` (whose only liveness
    /// signal is [`Self::active_running`]) delete the live sibling's in-flight
    /// parts (post-0.24.3 convergence HIGH). A crash SUCCESSOR of a unit shares
    /// its export NAME (`orders#0` re-run), so it still supersedes correctly;
    /// only a DIFFERENT sibling (`orders#1` vs `orders#0`) is excluded.
    pub fn superseded(&self, run: &CensusRun<'_>) -> bool {
        self.runs.iter().any(|o| {
            o.family == run.family()
                && o.manifest.started_at > run.manifest.started_at
                && !split_siblings(o, run)
        })
    }

    /// Is a LIVE run's `running` MARKER manifest present under the prefix — the
    /// bucket-side projection of the run-status ledger, for a cross-boundary load
    /// (Airflow / a foreign-host `rivet load`) that cannot read the extract's
    /// state DB? True iff some `running` manifest is not [`Self::superseded`].
    pub fn active_running(&self) -> bool {
        self.runs
            .iter()
            .any(|r| r.manifest.status == ManifestStatus::Running && !self.superseded(r))
    }

    /// True if `run` (a Failed/Interrupted manifest) is superseded by a LIVE run
    /// of the SAME family — a non-superseded `Running` marker with a newer
    /// `started_at`. That live run is a chunk-checkpoint resume which ADOPTS the
    /// crashed run's run_id and REUSES its committed parts, so those parts are
    /// being adopted, NOT dead debris — gc must SPARE them until the resume
    /// finalizes. When no same-family run is live, the parts stay terminal
    /// (deleted even while an unrelated run is active), so genuine crash debris is
    /// never over-deferred.
    pub fn adopted_by_active_running(&self, run: &CensusRun<'_>) -> bool {
        self.runs.iter().any(|o| {
            o.manifest.status == ManifestStatus::Running
                && o.family == run.family()
                && o.manifest.started_at > run.manifest.started_at
                && !self.superseded(o)
        })
    }

    /// The LATEST run per split unit — the snapshot generation a Full (replace)
    /// load materialises.
    ///
    /// Grouped by export NAME, then the newest per group by `finished_at`. A
    /// `--pool --split` snapshot is N units `{family}#0..#N-1`, each its OWN
    /// run_id/manifest finishing at a slightly different instant; a `max_by` over
    /// the WHOLE set picks exactly ONE — silently dropping the other N-1 units on
    /// a Full load. A non-split export keeps ONE export_name across repeated runs
    /// → a single group → the single latest snapshot, unchanged replace semantics.
    /// Ordered by unit name (BTreeMap) so the selection is deterministic.
    ///
    /// Coherence of the selection is a separate question — see
    /// [`ensure_single_generation`].
    pub fn latest_generation(&self) -> Vec<&CensusRun<'a>> {
        let mut latest: std::collections::BTreeMap<&str, &CensusRun<'a>> =
            std::collections::BTreeMap::new();
        for run in &self.runs {
            let name = run.manifest.export_name.as_str();
            let newer = match latest.get(name) {
                None => true,
                Some(prev) => finished_after(&run.manifest.finished_at, &prev.manifest.finished_at),
            };
            if newer {
                latest.insert(name, run);
            }
        }
        latest.into_values().collect()
    }

    /// The `--pool --split` UNITS of `family` (see [`CensusRun::is_split_unit`]).
    ///
    /// A plain export's family is its own name, so its HISTORICAL repeated-run
    /// copies share the family too — but they are SUPERSEDED snapshots, not
    /// co-current units, and a presence check over them would false-fail on a
    /// legitimately cleaned old part. An empty family cannot disambiguate
    /// anything, so it selects nothing.
    pub fn split_units(&self, family: &str) -> Vec<&CensusRun<'a>> {
        if family.is_empty() {
            return Vec::new();
        }
        self.runs
            .iter()
            .filter(|r| r.family == family && r.is_split_unit())
            .collect()
    }

    /// The set of destination keys CLAIMED by the runs of `family` — every
    /// `Committed` part they declare, resolved against `manifest_dir`.
    ///
    /// Covers BOTH split units AND a plain export's superseded historical / CDC-
    /// soak copies, so neither reads as untracked surplus (noise that also MASKS
    /// a real orphan). A FOREIGN family's parts are never claimed, so
    /// cross-contamination under a mistakenly-shared prefix still surfaces. An
    /// empty family claims nothing — a legacy prefix is single-run, and folding by
    /// empty family could hide cross-contamination.
    pub fn claimed_parts(
        &self,
        family: &str,
        manifest_dir: &str,
    ) -> std::collections::BTreeSet<String> {
        let mut out = std::collections::BTreeSet::new();
        if family.is_empty() {
            return out;
        }
        for run in self.runs.iter().filter(|r| r.family == family) {
            for p in &run.manifest.parts {
                if p.status == PartStatus::Committed {
                    out.insert(super::join_key(manifest_dir, &p.path));
                }
            }
        }
        out
    }
}

/// True if `a` and `b` are DIFFERENT split units of the same family (`{family}#i`
/// vs `{family}#j`, i≠j) — concurrent siblings, not a crash + successor.
fn split_siblings(a: &CensusRun<'_>, b: &CensusRun<'_>) -> bool {
    a.family() == b.family()
        && a.manifest.export_name != b.manifest.export_name
        && a.is_split_unit()
        && b.is_split_unit()
}

/// `"orders#0"` under family `"orders"` → `Some("0")`; a non-split name → `None`.
/// A family-less manifest has no prefix to strip, so it is never a unit.
fn unit_index<'a>(m: &'a RunManifest, family: &str) -> Option<&'a str> {
    if family.is_empty() {
        return None;
    }
    m.export_name
        .strip_prefix(family)
        .and_then(|s| s.strip_prefix('#'))
        .filter(|s| !s.is_empty() && s.chars().all(|c| c.is_ascii_digit()))
}

/// True if `a`'s finished-at instant is strictly after `b`'s (RFC3339; falls back
/// to a lexical compare only if either fails to parse). Parses as an INSTANT — a
/// lexical byte compare mis-picks on mixed RFC3339 precision (`…00.5Z` sorts
/// before `…00Z`) — and never panics on a malformed manifest.
fn finished_after(a: &str, b: &str) -> bool {
    match (
        chrono::DateTime::parse_from_rfc3339(a).ok(),
        chrono::DateTime::parse_from_rfc3339(b).ok(),
    ) {
        (Some(x), Some(y)) => x > y,
        _ => a > b,
    }
}

/// Per-entry keep mask for the run-id dedupe — the rule [`dedupe_by_run_id`] and
/// [`ManifestCensus::new`] share so a census and the vector it was built from
/// enumerate the SAME runs.
fn keep_one_per_run(keyed: &[(String, RunManifest)]) -> Vec<bool> {
    let copy_run_ids: std::collections::HashSet<&str> = keyed
        .iter()
        .filter(|(k, _)| is_copy_key(k))
        .map(|(_, m)| m.run_id.as_str())
        .collect();
    keyed
        .iter()
        .map(|(k, m)| is_copy_key(k) || !copy_run_ids.contains(m.run_id.as_str()))
        .collect()
}

/// A listed key whose final segment is a per-run manifest COPY
/// (`manifest-<run_id>.json`) rather than the canonical pointer.
fn is_copy_key(key: &str) -> bool {
    is_run_unique_manifest_name(key.rsplit('/').next().unwrap_or(""))
}

/// One manifest per RUN: the canonical `manifest.json` is a last-writer-wins
/// POINTER to the latest run, so when its run_id is also present as an immutable
/// `manifest-<run_id>.json` copy, keep the copy and drop the pointer (the
/// double-count guard). A run_id present ONLY under the canonical name — a legacy
/// run predating the run-unique copies — survives, so an upgraded prefix still
/// counts it (#173).
pub fn dedupe_by_run_id(keyed: Vec<(String, RunManifest)>) -> Vec<(String, RunManifest)> {
    let keep = keep_one_per_run(&keyed);
    keyed
        .into_iter()
        .zip(keep)
        .filter_map(|(entry, keep)| keep.then_some(entry))
        .collect()
}

/// A Full selection must be ONE coherent split generation (or a single plain
/// snapshot). A `--pool --split` prefix accumulates a manifest copy per unit per
/// run (different run_ids → different part names → nothing overwrites), and there
/// is no generation id, so if a later run split into a DIFFERENT unit count — or
/// the export toggled unsplit↔split — [`ManifestCensus::latest_generation`]'s
/// group-by-unit selection would mix TWO generations: their windows overlap and a
/// Full (replace) load would DUPLICATE rows (the count gate can't catch it —
/// `expected_rows` inflates with the parts). Every coherent split generation has
/// EXACTLY one bottom unit (`lo == None`) and one top (`hi == None`) and no
/// plain/split mix; violate that and we cannot tell which parts form the current
/// snapshot, so REFUSE loudly rather than silently duplicate. (Proper fix: stamp a
/// split-generation id on the units — tracked follow-up; it lands beside
/// [`CensusRun::is_split_unit`], the other half of this question.)
///
/// Keyed off `split_window` rather than [`CensusRun::is_split_unit`] because
/// coherence needs the WINDOW itself (the boundaries must tile), not merely
/// unit-hood: a windowless unit is a `running` MARKER, which a Full selection has
/// already filtered out (only `Success` runs are loadable).
pub fn ensure_single_generation(selected: &[&CensusRun<'_>]) -> Result<()> {
    let split: Vec<&RunManifest> = selected
        .iter()
        .map(|r| r.manifest)
        .filter(|m| m.split_window.is_some())
        .collect();
    if split.is_empty() {
        return Ok(()); // a plain single-snapshot selection is always coherent
    }
    let plain = selected.len() - split.len();
    let bottoms = split
        .iter()
        .filter(|m| m.split_window.as_ref().unwrap().lo.is_none())
        .count();
    let tops = split
        .iter()
        .filter(|m| m.split_window.as_ref().unwrap().hi.is_none())
        .count();
    if plain > 0 || bottoms > 1 || tops > 1 {
        anyhow::bail!(
            "Full load over a --pool --split prefix selected parts from MORE THAN ONE split \
             generation ({} split units incl. {bottoms} bottom + {tops} top window(s){}); \
             loading their union would DUPLICATE rows. The prefix holds a prior split run's \
             leftover parts (a later run split into a different unit count, or the export \
             toggled split↔unsplit). Load into a FRESH destination prefix (or clear the stale \
             generation) and re-run.",
            split.len(),
            if plain > 0 {
                format!(", plus {plain} non-split snapshot(s)")
            } else {
                String::new()
            }
        );
    }
    // bottoms==1, tops==1, plain==0 is NECESSARY but NOT sufficient. When two generations
    // split into the SAME unit COUNT but at DIFFERENT boundaries and an INTERIOR unit is
    // substituted from the older generation (its newer sibling crashed, so latest_generation
    // falls back to the stale Success), the selection still has exactly one bottom + one top —
    // yet the windows no longer tile: a GAP loses rows and an OVERLAP duplicates them, and the
    // count gate can't see it (expected_rows inflates with the parts). Post-0.24.3 review HIGH.
    //
    // A coherent split tiles the key space: every INTERIOR boundary appears once as some
    // unit's `hi` and once as the NEXT unit's `lo`. So the multiset of non-None `lo` bounds
    // must equal the multiset of non-None `hi` bounds. Compare them sorted — order-free, no
    // numeric parse of opaque key text (a gap leaves a `hi` with no matching `lo`; an overlap
    // leaves a `lo` with no matching `hi`).
    let mut los: Vec<&String> = split
        .iter()
        .filter_map(|m| m.split_window.as_ref().unwrap().lo.as_ref())
        .collect();
    let mut his: Vec<&String> = split
        .iter()
        .filter_map(|m| m.split_window.as_ref().unwrap().hi.as_ref())
        .collect();
    los.sort();
    his.sort();
    if los != his {
        anyhow::bail!(
            "Full load over a --pool --split prefix selected an INCOHERENT set of split windows \
             — the interior boundaries do not tile the key space, so a later run re-sampled to \
             different boundaries and an older unit was substituted for a crashed one. Their \
             union LOSES the gap rows and DUPLICATES the overlap rows (the count gate can't see \
             it). Interior lower bounds {los:?} != upper bounds {his:?}. Load into a FRESH \
             destination prefix (or clear the stale generation) and re-run."
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{
        MANIFEST_VERSION, ManifestDestination, ManifestPart, ManifestSource, SplitWindow,
    };

    /// A minimal Success manifest — the census reads only identity, timing,
    /// status, window and parts, so everything else is inert filler.
    fn m(run: &str, export_name: &str, family: &str) -> RunManifest {
        RunManifest {
            manifest_version: MANIFEST_VERSION,
            run_id: run.into(),
            export_name: export_name.into(),
            export_family: family.into(),
            started_at: "2026-08-21T00:00:00Z".into(),
            finished_at: "2026-08-21T00:01:00Z".into(),
            status: ManifestStatus::Success,
            source: ManifestSource {
                engine: "postgres".into(),
                schema: Some("public".into()),
                table: Some("orders".into()),
                extraction: None,
            },
            destination: ManifestDestination {
                kind: "local".into(),
                uri: "file:///tmp/out".into(),
            },
            format: "parquet".into(),
            compression: "zstd".into(),
            schema_fingerprint: "xxh3:0123456789abcdef".into(),
            row_count: 0,
            part_count: 0,
            parts: Vec::new(),
            column_checksums: None,
            checksum_key_column: None,
            checksum_render: None,
            row_hash: None,
            mode: "batch".into(),
            split_window: None,
        }
    }

    fn part(path: &str, status: PartStatus) -> ManifestPart {
        ManifestPart {
            part_id: 0,
            path: path.into(),
            rows: 10,
            size_bytes: 100,
            content_fingerprint: "xxh3:0123456789abcdef".into(),
            content_md5: String::new(),
            status,
        }
    }

    fn window(lo: Option<&str>, hi: Option<&str>) -> SplitWindow {
        SplitWindow {
            key_column: "id".into(),
            lo: lo.map(str::to_string),
            hi: hi.map(str::to_string),
        }
    }

    fn keyed(run: &str, m: RunManifest) -> (String, RunManifest) {
        (format!("base/manifest-{run}.json"), m)
    }

    // ── run enumeration ──────────────────────────────────────────────────────

    /// The canonical pointer and the copy of the SAME run are ONE run; a run
    /// recorded only under the canonical name (a legacy prefix that later gained
    /// copies) still counts.
    #[test]
    fn census_counts_one_run_per_run_id_keeping_a_legacy_canonical_only_run() {
        let keyed = vec![
            (
                "base/manifest.json".to_string(),
                m("r2", "orders", "orders"),
            ),
            (
                "base/manifest-r2.json".to_string(),
                m("r2", "orders", "orders"),
            ),
            (
                "base/old/manifest.json".to_string(),
                m("r1", "orders", "orders"),
            ),
        ];
        let census = ManifestCensus::new(&keyed);
        let keys: Vec<&str> = census.runs().iter().map(|r| r.key).collect();
        assert_eq!(
            keys,
            vec!["base/manifest-r2.json", "base/old/manifest.json"],
            "the pointer loses to its own copy; the legacy canonical-only run survives"
        );
        assert_eq!(
            census.runs()[1].index,
            2,
            "index points back into the caller's slice so a selection needs no clone"
        );
    }

    // ── family + split-unit identity ─────────────────────────────────────────

    /// A legacy (family-less) manifest joins the recorded family of a manifest
    /// sharing its export NAME — the upgrade story `identity_family` owns,
    /// resolved once for every consumer of the census.
    #[test]
    fn a_legacy_manifest_takes_the_recorded_family_of_the_same_export_name() {
        let keyed = vec![
            keyed("r1", m("r1", "orders", "")),
            keyed("r2", m("r2", "orders", "orders_cdc")),
        ];
        let census = ManifestCensus::new(&keyed);
        assert_eq!(census.runs()[0].family(), "orders_cdc");
    }

    /// EITHER signal makes a split unit: the terminal manifest's `split_window`
    /// (whose name shape a future scheme could change) OR the `{family}#i` export
    /// name (all a `running` MARKER carries, since the terminal overwrites its
    /// window). A plain export's repeated run is neither.
    #[test]
    fn split_unit_identity_reconciles_the_window_and_the_name_pattern() {
        let mut windowed = m("r1", "daily-unit", "daily");
        windowed.split_window = Some(window(None, Some("1000")));
        let mut marker = m("r2", "daily#1", "daily");
        marker.status = ManifestStatus::Running;
        let keyed = vec![
            keyed("r1", windowed),
            keyed("r2", marker),
            keyed("r3", m("r3", "daily", "daily")), // plain repeated run
        ];
        let census = ManifestCensus::new(&keyed);
        let units: Vec<&str> = census
            .split_units("daily")
            .iter()
            .map(|r| r.manifest.export_name.as_str())
            .collect();
        assert_eq!(
            units,
            vec!["daily-unit", "daily#1"],
            "a windowed terminal AND a `#i`-named marker are units; a plain run is not"
        );
        assert!(
            census.split_units("").is_empty(),
            "an empty family disambiguates nothing"
        );
    }

    // ── supersession / liveness ──────────────────────────────────────────────

    /// A later-STARTED split sibling that finished first must not mask a live
    /// sibling — concurrent units of one family, not a crash + successor.
    #[test]
    fn a_split_sibling_does_not_supersede_a_live_sibling_but_its_own_rerun_does() {
        let live = {
            let mut x = m("r0", "orders#0", "orders");
            x.status = ManifestStatus::Running;
            x.started_at = "2026-08-21T00:00:00Z".into();
            x
        };
        let sibling = {
            let mut x = m("r1", "orders#1", "orders");
            x.started_at = "2026-08-21T00:00:30Z".into();
            x
        };
        let keyed = vec![keyed("r0", live), keyed("r1", sibling)];
        let census = ManifestCensus::new(&keyed);
        assert!(!census.superseded(&census.runs()[0]));
        assert!(census.active_running(), "the live unit is still writing");

        // A crash SUCCESSOR of the SAME unit shares its export name → supersedes.
        let mut rerun = m("r2", "orders#0", "orders");
        rerun.started_at = "2026-08-21T00:01:00Z".into();
        let keyed = vec![
            keyed.into_iter().next().unwrap(),
            ("base/manifest-r2.json".to_string(), rerun),
        ];
        let census = ManifestCensus::new(&keyed);
        assert!(census.superseded(&census.runs()[0]));
        assert!(!census.active_running());
    }

    /// A LIVE same-family run is adopting a crashed run's committed parts
    /// (chunk-checkpoint resume reuses the run_id), so gc must spare them; with no
    /// live run they are terminal debris.
    #[test]
    fn a_terminal_run_is_adopted_only_while_a_same_family_run_is_live() {
        let mut crashed = m("r1", "orders", "orders");
        crashed.status = ManifestStatus::Interrupted;
        crashed.started_at = "2026-08-21T00:00:00Z".into();
        let mut live = m("r2", "orders", "orders");
        live.status = ManifestStatus::Running;
        live.started_at = "2026-08-21T00:05:00Z".into();

        let alone = vec![keyed("r1", crashed.clone())];
        let census = ManifestCensus::new(&alone);
        assert!(!census.adopted_by_active_running(&census.runs()[0]));

        let resuming = vec![keyed("r1", crashed), keyed("r2", live)];
        let census = ManifestCensus::new(&resuming);
        assert!(census.adopted_by_active_running(&census.runs()[0]));
    }

    // ── generation selection + coherence ─────────────────────────────────────

    /// The latest run PER UNIT — a split snapshot's N units all belong to the
    /// selection, and their coherence check passes when the windows tile.
    #[test]
    fn latest_generation_takes_the_newest_run_of_every_unit() {
        let unit = |run: &str, name: &str, finished: &str, lo, hi| {
            let mut x = m(run, name, "daily");
            x.finished_at = finished.into();
            x.split_window = Some(window(lo, hi));
            keyed(run, x)
        };
        let keyed = vec![
            unit("r1", "daily#0", "2026-08-21T00:01:00Z", None, Some("500")),
            unit("r2", "daily#0", "2026-08-21T00:02:00Z", None, Some("500")),
            unit("r3", "daily#1", "2026-08-21T00:01:30Z", Some("500"), None),
        ];
        let census = ManifestCensus::new(&keyed);
        let sel = census.latest_generation();
        let runs: Vec<&str> = sel.iter().map(|r| r.manifest.run_id.as_str()).collect();
        assert_eq!(
            runs,
            vec!["r2", "r3"],
            "newest per unit, ordered by unit name"
        );
        ensure_single_generation(&sel).expect("the two windows tile the key space");
    }

    /// Two generations mixed in one selection — the union would DUPLICATE rows, so
    /// the census refuses rather than let the count gate miss it.
    #[test]
    fn ensure_single_generation_refuses_a_mixed_and_a_non_tiling_selection() {
        let unit = |run: &str, name: &str, lo, hi| {
            let mut x = m(run, name, "daily");
            x.split_window = Some(window(lo, hi));
            keyed(run, x)
        };
        // Two bottoms: a 2-unit generation plus a stale 3-unit one.
        let mixed = vec![
            unit("r1", "daily#0", None, Some("500")),
            unit("r2", "daily#1", Some("500"), None),
            unit("r3", "daily#2", None, Some("300")),
        ];
        let census = ManifestCensus::new(&mixed);
        let sel = census.latest_generation();
        let err = ensure_single_generation(&sel).unwrap_err().to_string();
        assert!(err.contains("MORE THAN ONE split generation"), "{err}");

        // One bottom + one top, but the interior boundaries do not meet.
        let holed = vec![
            unit("r1", "daily#0", None, Some("500")),
            unit("r2", "daily#1", Some("700"), None),
        ];
        let census = ManifestCensus::new(&holed);
        let sel = census.latest_generation();
        let err = ensure_single_generation(&sel).unwrap_err().to_string();
        assert!(err.contains("INCOHERENT set of split windows"), "{err}");

        // A plain single snapshot is always coherent.
        let plain = vec![keyed("r1", m("r1", "orders", "orders"))];
        let census = ManifestCensus::new(&plain);
        ensure_single_generation(&census.latest_generation()).unwrap();
    }

    // ── claimed parts ────────────────────────────────────────────────────────

    /// Every SAME-FAMILY run's committed parts are claimed (a split unit AND a
    /// plain export's superseded historical copy), never a foreign family's, never
    /// a quarantined part, and nothing at all under an empty family.
    #[test]
    fn claimed_parts_covers_the_family_only_and_committed_only() {
        let with_part = |run: &str, name: &str, fam: &str, path: &str, status| {
            let mut x = m(run, name, fam);
            x.parts = vec![part(path, status)];
            keyed(run, x)
        };
        let keyed = vec![
            with_part(
                "r1",
                "daily#0",
                "daily",
                "daily#0_p.parquet",
                PartStatus::Committed,
            ),
            with_part(
                "r2",
                "daily",
                "daily",
                "daily_old.parquet",
                PartStatus::Committed,
            ),
            with_part(
                "r3",
                "daily#1",
                "daily",
                "daily#1_q.parquet",
                PartStatus::Quarantined,
            ),
            with_part(
                "r4",
                "other",
                "other",
                "other_p.parquet",
                PartStatus::Committed,
            ),
            // A manifest with NO identity at all (no family, no name) resolves to the
            // empty family. It must never fold with anything — an empty family groups
            // unrelated legacy runs together, which is how cross-contamination hides.
            {
                let (k, mut nameless) =
                    with_part("r5", "", "", "nameless_p.parquet", PartStatus::Committed);
                nameless.split_window = Some(window(None, None));
                (k, nameless)
            },
        ];
        let census = ManifestCensus::new(&keyed);
        let claimed = census.claimed_parts("daily", "sub");
        assert_eq!(
            claimed,
            ["sub/daily#0_p.parquet", "sub/daily_old.parquet"]
                .into_iter()
                .map(String::from)
                .collect(),
            "same-family committed parts only, resolved against the manifest dir: {claimed:?}"
        );
        assert!(
            census.claimed_parts("", "sub").is_empty(),
            "an empty family claims nothing — not even a part declared by a manifest that \
             resolves to it"
        );
        assert!(
            census.split_units("").is_empty(),
            "…and nothing is a split UNIT of the empty family, window or not"
        );
    }
}
