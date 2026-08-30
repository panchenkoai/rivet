//! End-to-end load integrity — reconcile **source → file → warehouse** row
//! counts before (and after) a load.
//!
//! The OSS engine records, in each run's `manifest.json`, two of the three legs
//! of the chain: how many rows the source held at extraction time
//! ([`ExtractionMetadata::source_row_count`](crate::manifest::ExtractionMetadata),
//! when cheaply probed) and how many rows were actually written to files
//! ([`RunManifest::row_count`]). The warehouse leg — how many rows the load
//! landed — is the loaders' post-load `COUNT(*)`, enforced by their
//! `expected_rows` gate.
//!
//! Until now that gate was dead in the production path: nothing read the
//! manifest, so `rivet load` trusted whatever Parquet happened to sit under
//! the prefix ("file in a bucket"). This module closes the loop:
//!
//! 1. read every `manifest.json` under the export's GCS prefix,
//! 2. **refuse** to load unless each is a self-consistent `Success` run whose
//!    source count (when known) matches what it extracted, and
//! 3. return the summed, authoritative `file_rows` the loader's count gate then
//!    checks against the warehouse.
//!
//! It is the value the OSS core deliberately stops short of — file-level
//! integrity is free (`rivet validate`); reconciling that the rows *arrived in
//! the warehouse* is the paid last mile.

use crate::destination::gcs::GcsStore;
use crate::manifest::census::{ManifestCensus, dedupe_by_run_id, ensure_single_generation};
use crate::manifest::{MANIFEST_FILENAME, ManifestStatus, RunManifest};
use crate::pipeline::validate_manifest::MANIFEST_MAX_BYTES;
use anyhow::{Context, Result, bail};

/// The reconciled row-count chain for one export's load, derived from the run
/// manifests under its GCS prefix. `file_rows` is what the warehouse must end
/// up holding; the loader's `expected_rows` gate enforces `warehouse == file`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadIntegrity {
    /// Rows the source held at extraction time, summed over the manifests that
    /// probed it. `None` when *no* contributing manifest carried a
    /// `source_row_count` — the source→file leg is then unverifiable from the
    /// manifest alone (e.g. a full snapshot that did not count the source).
    pub source_rows: Option<u64>,
    /// Rows written to files — the sum of the trustworthy manifests'
    /// `row_count`. This is the authoritative expected warehouse row count.
    pub file_rows: u64,
    /// How many run manifests contributed to the totals.
    pub manifests: usize,
}

impl LoadIntegrity {
    /// A one-line human summary of the chain, e.g.
    /// `source 1000 → files 1000 → (warehouse pending)`. The warehouse leg is
    /// filled in by the caller once the load's `COUNT(*)` is known.
    pub fn chain_prefix(&self) -> String {
        let src = self
            .source_rows
            .map_or_else(|| "?".to_string(), |n| n.to_string());
        format!("source {src} → files {}", self.file_rows)
    }
}

/// Fetch and parse every `manifest.json` under `gcs_prefix` (recursive), keeping
/// each manifest's bucket-relative storage key — needed to resolve a manifest's
/// (relative) part paths back to full object keys for per-run loading (see
/// [`select_load_uris`]).
///
/// A rivet export writes one manifest per `run_id`; a prefix that has
/// accumulated several incremental / CDC runs holds several. Transport is the
/// native opendal client the extraction destination already uses (`store`), so
/// auth is the export's own GCS credentials and this is offline-testable over a
/// filesystem-backed store.
///
/// `pub` (a public-API root the lib keeps alive) even though its only caller is
/// the binary-only `cli::dispatch`; `#[allow(private_interfaces)]` because the
/// injected `GcsStore` is deliberately an internal (`pub(crate)`) type — the
/// `destination` module stays crate-private. Same rationale as the `preflight`
/// module note in `lib.rs`.
#[allow(private_interfaces)]
pub fn fetch_manifests_keyed(
    store: &GcsStore,
    gcs_prefix: &str,
) -> Result<Vec<(String, RunManifest)>> {
    let (_, base) = crate::load::split_gs_uri(gcs_prefix)?;
    let keys = list_manifest_keys(store, base)?;
    keys.into_iter()
        .map(|key| {
            // Round-5 (CWE-400): cap the manifest body before reading it whole into
            // memory — a planted multi-GB manifest.json under the load prefix would
            // otherwise OOM the loader. Mirrors the V21 cap the export-side manifest
            // readers already enforce; this was the 4th, uncapped, read path.
            let sz = store.stat_size(&key)?;
            if sz > MANIFEST_MAX_BYTES {
                bail!(
                    "manifest {key} is {sz} bytes, over the {MANIFEST_MAX_BYTES}-byte cap — \
                     refusing to read a possibly-hostile manifest into memory (CWE-400)"
                );
            }
            let bytes = store.read(&key)?;
            let m = serde_json::from_slice::<RunManifest>(&bytes)
                .with_context(|| format!("parsing manifest {key}"))?;
            Ok((key, m))
        })
        .collect::<Result<Vec<_>>>()
        .map(dedupe_by_run_id)
}

/// Full `gs://` URIs of the parquet to load for `new` (the not-yet-loaded run
/// manifests), preferring each manifest's own parts over a blanket listing.
/// See [`select_load_keys`] for the selection rule.
#[allow(private_interfaces)]
pub fn select_load_uris(
    store: &GcsStore,
    gcs_prefix: &str,
    new: &[(String, RunManifest)],
) -> Result<Vec<String>> {
    let (bucket, base) = crate::load::split_gs_uri(gcs_prefix)?;
    let all_parquet: Vec<String> = store
        .list_files(base)?
        .into_iter()
        .filter(|k| k.ends_with(".parquet"))
        .collect();
    Ok(select_load_keys(new, &all_parquet)
        .into_iter()
        .map(|k| format!("gs://{bucket}/{k}"))
        .collect())
}

/// The bucket-relative part keys a manifest declares, each resolved against the
/// manifest's own directory: `<dir(manifest_key)>/<part.path>`. Shared by
/// [`select_load_keys`] (which intersects them with what's present) and
/// [`gc_orphans`] (which treats them as the keep-set), so the two can't drift on
/// how a manifest maps to its files.
fn resolve_parts<'a>(
    manifest_key: &'a str,
    m: &'a RunManifest,
) -> impl Iterator<Item = String> + 'a {
    let dir = manifest_key.rsplit_once('/').map(|(d, _)| d).unwrap_or("");
    m.parts.iter().map(move |p| {
        if dir.is_empty() {
            p.path.clone()
        } else {
            format!("{dir}/{}", p.path)
        }
    })
}

/// Pure selection: which bucket-relative parquet keys to load for the given
/// (not-yet-loaded) run manifests.
///
/// Prefers each manifest's own parts (via [`resolve_parts`]) intersected with
/// `all_parquet` — so a load pulls exactly the new runs' files, not every object
/// under the prefix (the key to incremental loads once `cleanup_source` no
/// longer wipes the bucket). Falls back to the whole `all_parquet` listing when
/// ANY new manifest resolves to no present part (legacy/part-less manifests
/// still load, at the cost of not pruning); the row-count gate then still guards
/// correctness.
pub fn select_load_keys(new: &[(String, RunManifest)], all_parquet: &[String]) -> Vec<String> {
    use std::collections::BTreeSet;
    let present: std::collections::HashSet<&str> = all_parquet.iter().map(String::as_str).collect();
    let mut selected: BTreeSet<String> = BTreeSet::new();
    for (key, m) in new {
        let mut resolved_any = false;
        for full in resolve_parts(key, m) {
            if present.contains(full.as_str()) {
                selected.insert(full);
                resolved_any = true;
            }
        }
        // A manifest that DECLARES parts but resolves none is genuinely
        // unresolvable — bail to the full listing rather than risk a partial
        // selection. A manifest declaring NO parts is a different thing: the run
        // legitimately produced nothing (a CDC cycle with no changes, the anchor
        // cycle of `initial: snapshot`), and treating that as unresolvable
        // dragged EVERY parquet under the prefix into the load while
        // `expected_delta` still summed the manifests to zero — so the load
        // appended rows nobody accounted for and rivet's own count gate fired:
        // "appended 2 rows, expected 0 from the run manifests". Surfaced by the
        // scenario-artifact matrix on SQL Server, where the async capture Agent
        // makes an empty first cycle common; the mechanism is engine-independent.
        if !resolved_any && !m.parts.is_empty() {
            return all_parquet.to_vec();
        }
    }
    selected.into_iter().collect()
}

/// Delete every `.parquet` under `gcs_prefix` that no **`Success`** manifest
/// references — crash leftovers from an interrupted extract that accumulate.
/// Keeps every manifest, `_SUCCESS`, and every manifested part (including a
/// `snapshot/` sub-prefix's, since `keyed` is fetched recursively). Strictly
/// gentler than `cleanup_source`, which wipes the whole prefix.
///
/// A candidate falls into three classes:
/// - referenced by a **`Success`** manifest → KEEP (live data).
/// - referenced by a **`Failed`/`Interrupted`** manifest → DELETE unconditionally.
///   A run that WROTE a manifest is terminal — no live extract is still streaming
///   it — so its leftovers are unambiguous crash debris.
/// - referenced by **NO manifest at all** → AMBIGUOUS: a crash-BEFORE-manifest
///   orphan (delete) OR a concurrent extract's committed-but-not-yet-manifested
///   part (must NOT delete). `active` decides — the caller's answer to "is a run
///   currently WRITING this prefix?", read from the central run-status ledger
///   (`StateStore::has_active_run_on_prefix`): authoritative and CLOCK-FREE.
///   `active` → spare every unmanifested part; `!active` → no run is live, so an
///   unmanifested part is dead crash debris → delete.
///
/// The ledger read is the SEAM. A co-located / shared-Postgres load gets a
/// precise `active`; a stateless load has NO ledger to ask (`ledger_says_active
/// (None)` is false — "the manifest signal decides alone") and a foreign-host
/// load reads a fresh empty DB (`Some(Ok(false))`) — for BOTH, the bucket's
/// `running`-marker projection is the ONLY spare signal, which is why marker
/// supersession is success-only (round-4: this sentence used to claim they
/// "pass `active = true`", and a reader citing it would conclude the marker
/// rule cannot matter — it is all that stands between gc and a live run).
/// Returns `(removed, bytes)`.
#[allow(private_interfaces)]
pub fn gc_orphans(
    store: &GcsStore,
    gcs_prefix: &str,
    keyed: &[(String, RunManifest)],
    active: bool,
    // `running` MARKERS whose run_id the LEDGER says is TERMINAL — dead crash
    // markers even though no Success ever superseded them (an abandoned
    // export's marker can never be superseded; `state finish-run` and the
    // split ceased-ordinal stamp both produce exactly this state — round-5,
    // three agents independently). The caller computes this from the state
    // store; a stateless load passes empty and keeps the conservative
    // supersession-only sweep.
    dead_marker_run_ids: &std::collections::HashSet<String>,
) -> Result<(usize, u64)> {
    let (_bucket, base) = crate::load::split_gs_uri(gcs_prefix)?;
    // Success parts → keep. Failed/Interrupted parts → terminal ONLY once a
    // newer same-family Success supersedes their run (until then a checkpoint
    // resume may still adopt them); then deletable regardless of `active`. A
    // part in NEITHER set has no manifest at all — the ambiguous case `active`
    // gates.
    let census = ManifestCensus::new(keyed);
    let mut keep: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut terminal: std::collections::HashSet<String> = std::collections::HashSet::new();
    for run in census.runs() {
        let (key, m) = (run.key, run.manifest);
        match m.status {
            ManifestStatus::Success => keep.extend(resolve_parts(key, m)),
            // A `running` manifest is a LIVE run's marker (schema-less, no parts).
            // Its parts (if any) must NOT be treated as terminal debris — its
            // activeness is what makes `active` true (see the callsite), which
            // spares the run's unmanifested in-flight parts below.
            ManifestStatus::Running => {}
            ManifestStatus::Failed | ManifestStatus::Interrupted => {
                // A Failed/Interrupted run's parts are USUALLY dead crash debris — but NOT
                // always. A chunk-checkpoint resume ADOPTS the crashed run's run_id and
                // REUSES its already-committed parts (rehydrate_manifest_parts_from_file_log
                // references them into the resumed run's manifest rather than re-exporting),
                // so while a LIVE run of the SAME export is in flight — a newer, non-superseded
                // `Running` marker — those "terminal" parts may be getting adopted. Deleting
                // them mid-resume loses the completed chunks and the resume finalizes Success
                // referencing parts that are gone (post-0.24.3 review HIGH). Spare exactly those
                // (KEEP); everything else stays terminal debris, deletable even while an
                // UNRELATED export's run is active (no over-defer of genuine crash leftovers).
                // …and the live-resume spare has an idle sibling (round-4 HIGH):
                // BETWEEN attempts of a checkpoint-resumable export there is no
                // Running marker at all, yet the next `--resume` will rehydrate
                // these very parts into its manifest WITHOUT re-probing the
                // destination (keyset has no M8 preamble) — deleting them here
                // makes that resume finalize Success + `_SUCCESS` naming parquet
                // that no longer exists. So a Failed run's parts stay spared
                // until the run is SUPERSEDED (a newer same-family SUCCESS — the
                // resume completed; whatever it adopted is now declared under
                // the Success arm above, and the remainder is genuine debris).
                // Cost of the spare: a NEVER-succeeding export (nightly
                // crash-loop) accumulates every attempt's partial parquet until
                // its first Success ever lands — unbounded in that shape, and
                // accepted: the safe direction, with `cleanup_source` as the
                // deliberate-delete escape (round-5 corrected an earlier
                // "bounded" claim here).
                if census.adopted_by_active_running(run) || !census.superseded(run) {
                    keep.extend(resolve_parts(key, m));
                } else {
                    terminal.extend(resolve_parts(key, m));
                }
            }
        }
    }
    let mut removed = 0usize;
    let mut removed_bytes = 0u64;
    for key in store.list_files(base)? {
        if !key.ends_with(".parquet") || keep.contains(&key) {
            continue;
        }
        // A live run on this prefix + a part with NO manifest → maybe its
        // in-flight write → spare. A terminal-manifested part is deleted even
        // while a (different) run is active.
        if active && !terminal.contains(&key) {
            log::warn!(
                "gc_orphans: sparing unmanifested `{key}` — a run is active on this prefix \
                 (run-status ledger); it is GC'd once no run is active or it gets a manifest"
            );
            continue;
        }
        removed_bytes += store.stat_size(&key).unwrap_or(0);
        store.remove(&key)?;
        removed += 1;
    }
    // Second pass: a SUPERSEDED `running` MARKER manifest (its run was overtaken by
    // a newer run of the same export) is a dead crash marker — never the live
    // signal — so its lingering `.json` is safe to remove. gc otherwise never
    // touches it (the parquet pass above only lists `.parquet`), so a hard-crashed
    // run's marker would accumulate forever. A NON-superseded running manifest is
    // the ACTIVE signal and MUST survive — deletion here is gated on SUPERSESSION,
    // never on `active`.
    for run in census.runs() {
        let dead_by_ledger = dead_marker_run_ids.contains(&run.manifest.run_id);
        if run.manifest.status == ManifestStatus::Running
            && (census.superseded(run) || dead_by_ledger)
        {
            removed_bytes += store.stat_size(run.key).unwrap_or(0);
            store.remove(run.key)?;
            removed += 1;
        }
    }
    Ok((removed, removed_bytes))
}

/// Is a LIVE run's `running` MARKER manifest present under the prefix — the
/// bucket-side projection of the run-status ledger, for a cross-boundary load
/// (Airflow / a foreign-host `rivet load`) that cannot read the extract's state
/// DB? `gc_orphans`'s `active` is `ledger_active OR this`, so the two signals are
/// belt-and-suspenders: the ledger is precise co-located, the marker covers
/// cross-host. The supersession rule it reads is the census's
/// ([`ManifestCensus::active_running`]) — the same one gc's marker sweep deletes
/// by, so the two can never disagree about which marker is live.
pub fn has_active_running_manifest(keyed: &[(String, RunManifest)]) -> bool {
    ManifestCensus::new(keyed).active_running()
}

/// Full/chunked loads care only about the LATEST snapshot: from `keyed` (all run
/// manifests under the prefix) pick the newest run of each split unit
/// ([`ManifestCensus::latest_generation`], which owns the grouping rule). Full
/// loads OVERWRITE, so exactly ONE generation may be loaded (loading every
/// accumulated run would duplicate rows). Deliberately **not** ledger-gated: full
/// re-materializes the latest snapshot on *every* load, so a re-load self-heals a
/// drifted target and stays resilient to hidden in-place source updates. An empty
/// input (no staged run — e.g. the staging was cleaned) yields an empty
/// selection, so the caller no-ops WITHOUT truncating the target.
pub fn latest_full(keyed: Vec<(String, RunManifest)>) -> Vec<(String, RunManifest)> {
    let picked: Vec<usize> = ManifestCensus::new(&keyed)
        .latest_generation()
        .iter()
        .map(|r| r.index)
        .collect();
    take_in_order(keyed, &picked)
}

/// Move the entries at `picked` out of `keyed`, in `picked`'s order — the census
/// selects by INDEX into the caller's slice, so materialising it costs no clone.
fn take_in_order(
    keyed: Vec<(String, RunManifest)>,
    picked: &[usize],
) -> Vec<(String, RunManifest)> {
    let mut slots: Vec<Option<(String, RunManifest)>> = keyed.into_iter().map(Some).collect();
    picked
        .iter()
        .filter_map(|&i| slots.get_mut(i).and_then(Option::take))
        .collect()
}

/// Which run manifests to load for `mode`, given the ledger's already-`loaded`
/// run_ids. The single mode→selection decision, pure and testable so the load's
/// central invariant isn't buried in dispatch I/O:
/// - `Full` → the single LATEST run ([`latest_full`]) — ledger-INDEPENDENT, so a
///   re-load re-materializes the snapshot (self-heal) and, crucially, the
///   STATELESS path (empty `loaded`) still picks the latest instead of blanket-
///   loading every accumulated snapshot (the duplicate-rows bug).
/// - `Incremental`/`Cdc` → every run not yet in `loaded` (append). Stateless →
///   `loaded` empty → load all; at-least-once, absorbed by the dedup view.
pub fn select_runs(
    keyed: Vec<(String, RunManifest)>,
    loaded: &std::collections::HashSet<String>,
    mode: crate::load::plan::LoadMode,
) -> Result<Vec<(String, RunManifest)>> {
    // Only a `Success` run is loadable, so drop every other status BEFORE
    // selection. `Running` is a live run's in-flight marker (no committed
    // parts); `Failed`/`Interrupted` is an ABORTED run whose parts are partial —
    // its data comes back complete under a NEW run_id when the export is
    // retried, and `gc_orphans` already classifies those parts as terminal
    // debris to delete.
    //
    // Dropping only `Running` here was a permanent stall: `reconcile` bails on
    // the first non-Success manifest it is handed, and nothing ever removes a
    // Failed one (gc deletes `.parquet` and superseded Running markers, never a
    // terminal manifest; `cleanup_source` runs only after a load that can no
    // longer succeed). So ONE aborted run — a source drop mid-extract, a query
    // error, a destination write failure — made every LATER successful run
    // unloadable forever, with an error telling the operator to reconfigure
    // prefixes, which is not the cause. Released since 0.20.0.
    //
    // Safe by construction for CDC: the drain writes `Success` unconditionally
    // (source/cdc/sink.rs), so an acked part is never inside a Failed manifest.
    // `Failed` originates only on the batch path (pipeline/finalize.rs).
    let keyed: Vec<(String, RunManifest)> = keyed
        .into_iter()
        .filter(|(_, m)| m.status == ManifestStatus::Success)
        .collect();
    match mode {
        crate::load::plan::LoadMode::Full => {
            let sel = latest_full(keyed);
            // ...and that selection must be ONE coherent split generation — the
            // other half of the census's split classification, so the grouping
            // above and the coherence check here read the SAME unit identity.
            ensure_single_generation(&ManifestCensus::new(&sel).runs().iter().collect::<Vec<_>>())?;
            Ok(sel)
        }
        _ => Ok(keyed
            .into_iter()
            .filter(|(_, m)| !loaded.contains(&m.run_id))
            .collect()),
    }
}

/// Bucket-relative keys of every run manifest under `base` (recursive).
fn list_manifest_keys(store: &GcsStore, base: &str) -> Result<Vec<String>> {
    let all: Vec<String> = store
        .list_files(base)?
        .into_iter()
        .filter(|k| is_manifest_key(k))
        .collect();
    // Return EVERY manifest key — the canonical `manifest.json` pointer AND the
    // immutable `manifest-<run_id>.json` copies. The double-count guard is a
    // RUN-ID dedupe in `fetch_manifests_keyed` (post-parse), not a name filter
    // here: the old "copies win when any exist" rule silently DROPPED a legacy
    // run whose only record is the canonical name on a prefix that later gained
    // run-unique copies — an upgrade-compat under-count (roast 2026-08-09, #173).
    Ok(all)
}

/// A listed key is a run manifest iff its final path segment is the canonical
/// [`MANIFEST_FILENAME`] (`manifest.json`) or a run-unique copy
/// (`manifest-<run_id>.json`) — so a data file merely *named* like it (an
/// unlikely `…/x_manifest.json`) is not mistaken for one.
fn is_manifest_key(key: &str) -> bool {
    let base = key.rsplit('/').next().unwrap_or("");
    base == MANIFEST_FILENAME || is_run_unique_manifest(base)
}

/// A per-run manifest copy: `manifest-<token>.json` (the sidecar the OSS sink
/// writes alongside the canonical pointer so cross-run reconcile can sum it).
fn is_run_unique_manifest(base: &str) -> bool {
    // The sidecar naming scheme lives once, in `manifest.rs` (the writer's home).
    crate::manifest::is_run_unique_manifest_name(base)
}

/// Refuse a load whose prefix holds MORE THAN ONE export's manifests. The load
/// sums EVERY manifest under `gcs_prefix` (fetched recursively) and cleanup
/// removes the prefix recursively, so a shared base prefix — two `partition_by`
/// exports whose prefix-before-`{partition}` coincides — would silently cross-
/// contaminate the reconciled row count AND delete the sibling export's un-loaded
/// parts. The `LoadPlan` carries no source `export_name` to disambiguate which
/// export the operator meant, so fail LOUDLY rather than load-and-delete the
/// wrong data. Legacy manifests with no recorded `export_name` (pre-0.x) are
/// tolerated — an empty name matches any single named export, so a prefix that
/// mixes old and new runs of the SAME export still loads.
///
/// The CDC `initial: snapshot` leg is NOT a sibling: `cdc_job` synthesizes it
/// as `{export}__snapshot_{table}` and points it at the SAME prefix by design —
/// the baseline and the drain are one table's rows, summed and cleaned
/// together. The guard therefore compares export FAMILIES, folding a
/// snapshot-leg name to its parent (see [`crate::manifest::snapshot_family`]).
pub fn ensure_single_export(keyed: &[(String, RunManifest)]) -> Result<()> {
    let mut names: std::collections::BTreeSet<&str> = keyed
        .iter()
        .map(|(_, m)| crate::manifest::identity_family(m, keyed))
        .filter(|n| !n.is_empty())
        .collect();
    if names.len() > 1 {
        let listed: Vec<&str> = std::mem::take(&mut names).into_iter().collect();
        bail!(
            "the load prefix holds manifests from {} distinct exports ({}) — the load sums every \
             manifest under the prefix and cleanup wipes it recursively, so loading a shared \
             prefix would cross-contaminate the row count and could delete a sibling export's \
             parts. Give each export a DISTINCT destination prefix (or scope the load prefix to a \
             single export) and re-run.",
            listed.len(),
            listed.join(", ")
        );
    }
    ensure_single_source(keyed)
}

/// One family is not one EXPORT when two different databases are called the same
/// thing.
///
/// The family is the export NAME, so two configs that both call their export
/// `orders` — because both databases have an `orders` table — are one family to
/// every check above. Measured end to end: two sources wrote 600 rows into one
/// GCS prefix under one name, and `rivet load` driven by the POSTGRES config put
/// the MYSQL rows in the warehouse (`n=300, lo=10001`), reported `integrity ✓`,
/// and exited 0. The postgres rows reached the warehouse from neither command.
/// The second load then replaced the first's table with the same 300. Half the
/// data absent, both commands green.
///
/// The manifest has always recorded WHICH SOURCE it came from, so the identity
/// this guard needs is already on disk — including in artifacts written before
/// this check existed, which is why the source is read rather than folded into
/// `export_family` (that would only protect prefixes written after the change).
///
/// A single source is the norm and stays silent. Two are refused, because the
/// alternatives both lose: loading one of them silently drops the other, and
/// loading their union would double-count the ordinary case of repeated runs.
fn ensure_single_source(keyed: &[(String, RunManifest)]) -> Result<()> {
    // The ENGINE alone is already an identity — a batch manifest records
    // `table: null` (the export's table lives in the plan, not here), so a
    // filter that required a table dropped both sides and the guard stayed
    // silent on the very case it was written for. Only a manifest with no engine
    // at all has nothing to compare.
    //
    // …and that same `table: null` is why a coarser identity must not count as
    // a DIFFERENT one. The CDC `initial: snapshot` leg is a synthesized BATCH
    // export writing into the drain's own prefix by design, so it records
    // `table: null` → `mysql`, while the drain records the table →
    // `mysql:cashback_versions`. Comparing the two strings refused the
    // ordinary snapshot → drain → load flow at the load step, on a prefix
    // holding one table from one database (reproduced on the CDC pilot against
    // 0.24.3: 5 drain manifests + 2 snapshot-leg manifests). `mysql` there
    // means "this engine, table unrecorded" — an UNKNOWN, and an unknown is
    // not evidence of a second source. Refuse only on evidence: a different
    // engine, or two different NAMED tables. (The same blind spot as the
    // export-family fold in #138, in the identity this guard added.)
    let sources: std::collections::BTreeSet<String> = keyed
        .iter()
        .map(|(_, m)| crate::manifest::identity_source(m))
        .filter(|s| !s.is_empty())
        .filter(|s| {
            // Drop an identity that is the BARE ENGINE (no table recorded — the
            // synthesized batch leg's `table: null`, #144) when a fuller
            // identity for that engine is present: it is the same source with
            // less detail. Only the colon-FREE engine folds — a `:`-bearing
            // identity is a concrete engine:table and must never be treated as
            // a coarser prefix of another, or two DIFFERENT tables/collections
            // whose names share a `:` boundary (a Mongo collection literally
            // named `orders:archive` vs `orders`) would silently fold into one
            // source and the two-source guard would wrongly pass (bug hunt
            // 2026-08-08).
            if s.contains(':') {
                return true; // concrete engine:table — never a coarsening
            }
            !keyed.iter().any(|(_, other)| {
                let o = crate::manifest::identity_source(other);
                o.len() > s.len() && o.starts_with(s.as_str()) && o.as_bytes()[s.len()] == b':'
            })
        })
        .collect();
    if sources.len() > 1 {
        let listed: Vec<&str> = sources.iter().map(String::as_str).collect();
        bail!(
            "the load prefix holds manifests from {} DIFFERENT SOURCES ({}) under one export \
             name. The warehouse table is named from the source table, so loading this prefix \
             puts one source's rows in the table and leaves the other's unloaded — and a second \
             load REPLACES the first. Both commands report success while half the data never \
             arrives. Give each source its own destination prefix and its own export name, or \
             load them into separate target tables.",
            listed.len(),
            listed.join(", ")
        );
    }
    Ok(())
}

/// Reconcile a run's manifests into the authoritative expected warehouse row
/// count, refusing to load anything that is not provably complete.
///
/// Every manifest must be a **`Success`** run (a `Failed` / `Interrupted`
/// manifest describes a partial, untrustworthy export) and **self-consistent**
/// (`row_count` == sum of committed parts — a mismatch is a writer bug). When a
/// manifest recorded a `source_row_count`, the **source→file** leg must
/// reconcile too: `source_row_count == row_count`, or the extract silently
/// dropped rows. `allow_source_drift` downgrades only that last check to a
/// warning (e.g. an incremental cursor window whose source moved under it);
/// the completeness and self-consistency gates never yield.
///
/// Returns [`LoadIntegrity`] with the summed `file_rows` the loader's
/// `expected_rows` gate then checks against the warehouse's `COUNT(*)`.
pub fn reconcile(manifests: &[RunManifest], allow_source_drift: bool) -> Result<LoadIntegrity> {
    if manifests.is_empty() {
        bail!(
            "no `{MANIFEST_FILENAME}` found under the export prefix — refusing to load \
             unverified files. A rivet export writes a manifest on success; its absence \
             means the run never completed (or points at the wrong prefix)."
        );
    }

    let mut file_rows: u64 = 0;
    let mut source_rows: u64 = 0;
    let mut any_source = false;

    for m in manifests {
        // Completeness: only a `Success` run may be loaded.
        if m.status != ManifestStatus::Success {
            bail!(
                "manifest for run `{}` (export `{}`) is {:?}, not Success — refusing to load a \
                 partial export",
                m.run_id,
                m.export_name,
                m.status
            );
        }
        // Self-consistency: the recorded aggregates must match the committed
        // parts (a divergence is a writer bug, per OSS `validate_self_consistency`).
        m.validate_self_consistency().map_err(|e| {
            anyhow::anyhow!(
                "manifest for run `{}` (export `{}`) is internally inconsistent: {e} — refusing \
                 to load",
                m.run_id,
                m.export_name
            )
        })?;

        let rows = u64::try_from(m.row_count).with_context(|| {
            format!(
                "manifest for run `{}` has a negative row_count ({})",
                m.run_id, m.row_count
            )
        })?;
        file_rows += rows;

        // Source→file: reconcile the extract against the source when the run
        // probed it. `None` = not probed (unverifiable, not a failure).
        if let Some(src) = m
            .source
            .extraction
            .as_ref()
            .and_then(|x| x.source_row_count)
        {
            let src = u64::try_from(src).with_context(|| {
                format!(
                    "manifest for run `{}` has a negative source_row_count ({src})",
                    m.run_id
                )
            })?;
            any_source = true;
            source_rows += src;
            if src != rows {
                if allow_source_drift {
                    eprintln!(
                        "warning: source→file drift for run `{}` (export `{}`): source had {src} \
                         rows, extracted {rows} (--allow-source-drift)",
                        m.run_id, m.export_name
                    );
                } else {
                    bail!(
                        "source→file mismatch for run `{}` (export `{}`): source had {src} rows \
                         but {rows} were extracted — the extract dropped {} row(s). Investigate \
                         before loading, or pass --allow-source-drift to override.",
                        m.run_id,
                        m.export_name,
                        src.abs_diff(rows)
                    );
                }
            }
        }
    }

    Ok(LoadIntegrity {
        source_rows: any_source.then_some(source_rows),
        file_rows,
        manifests: manifests.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{
        ExtractionMetadata, ManifestDestination, ManifestPart, ManifestSource, PartStatus,
    };

    /// A minimal `Success` manifest with one committed part of `rows` rows and,
    /// optionally, a probed `source_row_count`.
    fn manifest(run: &str, rows: i64, source: Option<i64>) -> RunManifest {
        RunManifest {
            split_window: None,
            checksum_render: None,
            row_hash: None,
            manifest_version: crate::manifest::MANIFEST_VERSION,
            run_id: run.into(),
            export_name: "orders".into(),
            export_family: String::new(),
            mode: "batch".into(),
            started_at: "t".into(),
            finished_at: "t".into(),
            status: ManifestStatus::Success,
            source: ManifestSource {
                engine: "pg".into(),
                schema: None,
                table: None,
                extraction: source.map(|n| ExtractionMetadata {
                    strategy: "full".into(),
                    cursor_column: None,
                    cursor_type: None,
                    cursor_low: None,
                    cursor_high: None,
                    source_row_count: Some(n),
                }),
            },
            destination: ManifestDestination {
                kind: "gcs".into(),
                uri: "gs://b/p".into(),
            },
            format: "parquet".into(),
            compression: "zstd".into(),
            schema_fingerprint: "xxh3:0".into(),
            row_count: rows,
            part_count: 1,
            parts: vec![ManifestPart {
                part_id: 0,
                path: "part-000000.parquet".into(),
                rows,
                size_bytes: 1,
                content_fingerprint: "xxh3:0".into(),
                content_md5: String::new(),
                status: PartStatus::Committed,
            }],
            column_checksums: None,
            checksum_key_column: None,
        }
    }

    #[test]
    fn sums_file_and_source_rows_across_manifests() {
        let ms = vec![manifest("r1", 100, Some(100)), manifest("r2", 40, Some(40))];
        let got = reconcile(&ms, false).unwrap();
        assert_eq!(got.file_rows, 140);
        assert_eq!(got.source_rows, Some(140));
        assert_eq!(got.manifests, 2);
    }

    #[test]
    fn ensure_single_export_refuses_a_prefix_shared_by_two_exports() {
        let keyed = |m: RunManifest| ("gs://b/p/manifest-x.json".to_string(), m);
        let orders = keyed(manifest("r1", 10, None)); // export_name "orders"
        let mut cust_m = manifest("r2", 10, None);
        cust_m.export_name = "customers".into();
        // Two DISTINCT exports under one load prefix → loud refusal (else the load
        // sums both and cleanup deletes the sibling's parts).
        assert!(ensure_single_export(&[orders.clone(), keyed(cust_m)]).is_err());
        // Many runs of ONE export → fine.
        assert!(ensure_single_export(&[orders.clone(), keyed(manifest("r3", 5, None))]).is_ok());
        // A legacy (no export_name) run mixed with the SAME named export → still
        // one logical export, tolerated (an upgrade mid-prefix must not brick load).
        let mut legacy = manifest("r0", 3, None);
        legacy.export_name = String::new();
        assert!(ensure_single_export(&[orders, keyed(legacy)]).is_ok());
    }

    /// Two DIFFERENT databases, ONE export name, one prefix — refuse.
    ///
    /// The family is the export name, so this arrangement is invisible to every
    /// other check: an operator with two databases that both have `orders`, two
    /// configs that both call the export `orders`, and one bucket prefix. What
    /// actually happened, measured end to end before this guard: 600 rows on the
    /// prefix, `rivet load` driven by the POSTGRES config loaded the MYSQL rows
    /// (`n=300, lo=10001`), said `integrity ✓`, exited 0 — and the second load
    /// replaced that table with the same 300. Half the data in the warehouse
    /// from neither command, both green.
    ///
    /// The fixture varies ONLY the source. Same family, same name, same shape —
    /// otherwise the older family guard would catch it and this one would never
    /// be reached, which is the shape of a test that grades a check it does not
    /// exercise.
    #[test]
    fn ensure_single_export_refuses_two_different_sources_under_one_name() {
        let keyed = |m: RunManifest| ("gs://b/p/manifest-x.json".to_string(), m);
        let mut pg = manifest("r1", 300, None);
        pg.source.engine = "postgres".into();
        pg.source.table = Some("orders".into());
        let mut my = manifest("r2", 300, None);
        my.source.engine = "mysql".into();
        my.source.table = Some("orders".into());
        assert_eq!(
            pg.export_name, my.export_name,
            "the fixture must share the name"
        );

        let err = ensure_single_export(&[keyed(pg.clone()), keyed(my.clone())])
            .expect_err("two sources under one name must be refused, not silently half-loaded");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("DIFFERENT SOURCES") && msg.contains("postgres") && msg.contains("mysql"),
            "the refusal must NAME both sources — an operator cannot act on `something is \
             ambiguous`: {msg}"
        );

        // The ordinary case stays silent: many runs of ONE export from ONE source.
        let mut again = manifest("r3", 300, None);
        again.source.engine = "postgres".into();
        again.source.table = Some("orders".into());
        assert!(
            ensure_single_export(&[keyed(pg), keyed(again)]).is_ok(),
            "repeated runs of one export must not be refused — that is the normal load"
        );
    }

    /// A `:` in a collection/table name must not fold two DIFFERENT sources
    /// into one (bug hunt 2026-08-08). Mongo collection `orders:archive` →
    /// identity `mongo:orders:archive`; collection `orders` → `mongo:orders`.
    /// The bare-engine fold used to treat `mongo:orders` as a coarsening of
    /// `mongo:orders:archive` (both at a `:` boundary) and pass — two distinct
    /// collections under one export name loading as one source.
    #[test]
    fn ensure_single_source_does_not_fold_two_colon_named_collections() {
        let keyed = |m: RunManifest| ("gs://b/p/manifest-x.json".to_string(), m);
        let mut a = manifest("r1", 10, None);
        a.source.engine = "mongo".into();
        a.source.table = Some("orders".into());
        let mut b = manifest("r2", 10, None);
        b.source.engine = "mongo".into();
        b.source.table = Some("orders:archive".into());
        b.export_name = a.export_name.clone();
        assert!(
            ensure_single_export(&[keyed(a.clone()), keyed(b.clone())]).is_err(),
            "two collections whose names share a `:` boundary are TWO sources"
        );
        // …and the bare-engine fold it must NOT have broken still works:
        // engine `mongo` (no table) folds into `mongo:orders`.
        let mut bare = manifest("r3", 10, None);
        bare.source.engine = "mongo".into();
        bare.source.table = None;
        bare.export_name = a.export_name.clone();
        assert!(
            ensure_single_export(&[keyed(a), keyed(bare)]).is_ok(),
            "a bare engine is still the same source, less detail"
        );
    }

    /// Regression (0.24.3, found on the CDC pilot): the SOURCE guard had the
    /// same blind spot the export guard had — and refused the same flow, one
    /// check later.
    ///
    /// The `initial: snapshot` leg is a synthesized BATCH export, and a batch
    /// manifest records `table: null` (the table lives in the plan), so its
    /// identity is the bare engine `mysql` while the drain's is
    /// `mysql:orders`. Two strings, one source. Measured on a real prefix: 5
    /// drain manifests + 2 snapshot-leg manifests ⇒ "manifests from 2
    /// DIFFERENT SOURCES (mysql, mysql:cashback_versions)", and the pilot's
    /// documented snapshot → drain → load flow could not load.
    ///
    /// A coarser identity is an UNKNOWN, not a second source. Refuse on
    /// evidence — a different engine, or two different NAMED tables.
    #[test]
    fn ensure_single_source_admits_the_batch_legs_unrecorded_table() {
        let keyed = |m: RunManifest| ("gs://b/p/manifest-x.json".to_string(), m);
        let mut drain = manifest("r1", 10, None);
        drain.source.engine = "mysql".into();
        drain.source.table = Some("orders".into());
        // The snapshot leg: same engine, table unrecorded (batch manifest).
        let mut snap = manifest("r2", 10, None);
        snap.source.engine = "mysql".into();
        snap.source.table = None;
        snap.export_name = drain.export_name.clone();
        assert!(
            ensure_single_export(&[keyed(drain.clone()), keyed(snap.clone())]).is_ok(),
            "the batch leg's unrecorded table is the SAME source, less detail"
        );
        // Order must not matter.
        assert!(ensure_single_export(&[keyed(snap.clone()), keyed(drain.clone())]).is_ok());

        // What the guard was written for still fires: a different ENGINE…
        let mut pg = manifest("r3", 10, None);
        pg.source.engine = "postgres".into();
        pg.source.table = Some("orders".into());
        pg.export_name = drain.export_name.clone();
        assert!(
            ensure_single_export(&[keyed(drain.clone()), keyed(pg)]).is_err(),
            "two engines under one name is still two sources"
        );
        // …and two different NAMED tables of one engine.
        let mut other_tbl = manifest("r4", 10, None);
        other_tbl.source.engine = "mysql".into();
        other_tbl.source.table = Some("customers".into());
        other_tbl.export_name = drain.export_name.clone();
        assert!(
            ensure_single_export(&[keyed(drain), keyed(other_tbl)]).is_err(),
            "two named tables under one name is still two sources"
        );
        // A bare engine that is a prefix of NOTHING present is still its own
        // identity — two unrecorded-table manifests from different engines
        // must not silently pass.
        let mut bare_my = manifest("r5", 10, None);
        bare_my.source.engine = "mysql".into();
        bare_my.source.table = None;
        let mut bare_pg = manifest("r6", 10, None);
        bare_pg.source.engine = "postgres".into();
        bare_pg.source.table = None;
        bare_pg.export_name = bare_my.export_name.clone();
        assert!(ensure_single_export(&[keyed(bare_my), keyed(bare_pg)]).is_err());
    }

    /// Regression: the CDC `initial: snapshot` leg shares the drain's prefix BY
    /// DESIGN (`cdc_job` synthesizes `{export}__snapshot_{table}` pointed at the
    /// same destination). 0.24.0's cross-run manifest summing put both names in
    /// front of this guard for the first time and the ordinary snapshot → drain
    /// → `rivet load` flow refused itself. The pair is ONE export family.
    #[test]
    fn ensure_single_export_admits_the_cdc_snapshot_leg_of_the_same_export() {
        let keyed = |m: RunManifest| ("gs://b/p/manifest-x.json".to_string(), m);
        let drain = keyed(manifest("r1", 10, None)); // export_name "orders"
        let mut snap = manifest("r2", 10, None);
        snap.export_name = "orders__snapshot_orders".into();
        // Snapshot baseline + drain of the SAME export → one family, loads.
        assert!(ensure_single_export(&[drain.clone(), keyed(snap)]).is_ok());
        // A snapshot leg of a DIFFERENT export is still a sibling → refuse.
        let mut foreign = manifest("r3", 10, None);
        foreign.export_name = "customers__snapshot_customers".into();
        assert!(ensure_single_export(&[drain, keyed(foreign)]).is_err());
    }

    /// The documented `initial: snapshot` config — `name: orders_cdc`,
    /// `table: orders` (docs/reference/cdc.md) — could NOT be loaded even after
    /// the 0.24.1 family fold: the drain writes `export_name` from the TABLE
    /// string while the snapshot leg writes `{export.name}__snapshot_{table}`,
    /// so the substring fold produced families `orders` and `orders_cdc` — two,
    /// and the guard refused rivet's own output with advice to split a prefix
    /// rivet shares by design. The RECORDED family closes it: both legs stamp
    /// the parent export name at write time.
    #[test]
    fn recorded_family_reconciles_the_documented_name_ne_table_config() {
        let keyed = |m: RunManifest| ("gs://b/p/manifest-x.json".to_string(), m);
        // drain: export_name is the TABLE ("orders"), family records the export.
        let mut drain = manifest("r1", 10, None);
        drain.export_name = "orders".into();
        drain.export_family = "orders_cdc".into();
        // snapshot leg: synthesized name, same recorded family.
        let mut snap = manifest("r2", 10, None);
        snap.export_name = "orders_cdc__snapshot_orders".into();
        snap.export_family = "orders_cdc".into();
        assert!(
            ensure_single_export(&[keyed(drain), keyed(snap)]).is_ok(),
            "one export whose name differs from its table must reconcile as ONE family"
        );
    }

    /// The inverse hole the substring fold had: an export the user literally
    /// NAMED with the infix. Before the family was recorded, `daily__snapshot_v2`
    /// folded to `daily` and silently DISARMED the shared-prefix guard — a real
    /// sibling export could sum into the load and be wiped by cleanup.
    ///
    /// This test derives both families from `plan::build`, the code that
    /// actually decides them, instead of hand-setting `export_family`. The
    /// earlier version typed the expected value in — and passed against a
    /// product that still mis-folded, because the batch writer folded the name
    /// at write time. A fixture that states the answer cannot grade it.
    #[test]
    fn a_user_export_named_like_a_snapshot_leg_no_longer_disarms_the_guard() {
        // Families as the PRODUCT computes them for two sibling batch exports.
        let daily = crate::config::sample_export("daily");
        let mut tricky = crate::config::sample_export("daily__snapshot_v2");
        tricky.snapshot_parent = None; // a user name, not a synthesized leg
        // Through the PRODUCT's own resolver — `plan::build` records exactly
        // this into every manifest. A closure re-implementing the rule here is
        // what let the previous version of this test survive the fold mutant.
        assert_eq!(daily.family(), "daily");
        assert_eq!(
            tricky.family(),
            "daily__snapshot_v2",
            "a user export's name is its own family — folding it merges two exports"
        );

        let keyed = |m: RunManifest| ("gs://b/p/manifest-x.json".to_string(), m);
        let mut a = manifest("r1", 10, None);
        a.export_name = daily.name.clone();
        a.export_family = daily.family();
        let mut b = manifest("r2", 10, None);
        b.export_name = tricky.name.clone();
        b.export_family = tricky.family();
        assert!(
            ensure_single_export(&[keyed(a), keyed(b)]).is_err(),
            "two distinct exports must refuse, even when one's NAME contains the infix"
        );
    }

    /// The synthesized leg is the ONE case where the infix name does mean a
    /// family: `cdc_job` sets `snapshot_parent`, so the leg and its drain share
    /// a family even though their names differ.
    #[test]
    fn the_synthesized_snapshot_leg_takes_its_parents_family() {
        let export = crate::config::sample_export("orders_cdc");
        let leg =
            crate::pipeline::cdc_job::synth_snapshot_export_for_test(&export, "orders", "orders");
        assert_eq!(
            leg.snapshot_parent.as_deref(),
            Some("orders_cdc"),
            "the leg must RECORD its parent, not leave it to be re-derived"
        );
        assert!(
            leg.name.contains(crate::manifest::SNAPSHOT_LEG_INFIX),
            "sanity: the leg is the infix-named export"
        );
    }

    /// UPGRADE COMPAT. `export_family` did not exist before this change, so a
    /// prefix holds BOTH kinds for as long as the old manifests live: a legacy
    /// CDC drain manifest carries `export_name` = the TABLE and no family (folds
    /// to "orders"), while a post-upgrade drain records family = the EXPORT
    /// ("orders_cdc"). Grouping naively then reports TWO families and the load
    /// refuses rivet\'s own prefix — a regression the field itself would have
    /// introduced, hitting every scheduled drain+load cadence with `name:` ≠
    /// `table:` at its first post-upgrade load.
    ///
    /// A legacy manifest joins the family of a recorded manifest that shares its
    /// `export_name` — precise, not permissive: the legacy drain's name IS the
    /// recorded drain\'s name.
    #[test]
    fn a_legacy_manifest_joins_the_recorded_family_of_the_same_export() {
        let keyed = |m: RunManifest| ("gs://b/p/manifest-x.json".to_string(), m);
        let mut legacy = manifest("r1", 10, None);
        legacy.export_name = "orders".into(); // the TABLE, as the old sink wrote
        legacy.export_family = String::new(); // pre-upgrade: field absent
        let mut fresh = manifest("r2", 10, None);
        fresh.export_name = "orders".into();
        fresh.export_family = "orders_cdc".into();
        assert!(
            ensure_single_export(&[keyed(legacy), keyed(fresh)]).is_ok(),
            "a pre-upgrade manifest beside its post-upgrade successor is ONE export"
        );
    }

    /// The other half: tolerance must not blind the guard. A legacy manifest of a
    /// genuinely DIFFERENT export shares no name with the recorded one, so it
    /// stays its own family and the prefix is still refused.
    #[test]
    fn a_legacy_manifest_of_another_export_is_still_refused() {
        let keyed = |m: RunManifest| ("gs://b/p/manifest-x.json".to_string(), m);
        let mut legacy = manifest("r1", 10, None);
        legacy.export_name = "customers".into();
        legacy.export_family = String::new();
        let mut fresh = manifest("r2", 10, None);
        fresh.export_name = "orders".into();
        fresh.export_family = "orders_cdc".into();
        assert!(
            ensure_single_export(&[keyed(legacy), keyed(fresh)]).is_err(),
            "a legacy manifest of a DIFFERENT export must still be refused"
        );
    }

    /// DOCUMENTS the contract a snapshot leg\'s `running` marker must satisfy:
    /// it carries the leg\'s FAMILY (the parent export), never its decorated
    /// `{parent}__snapshot_{table}` name. A marker carrying the name would
    /// belong to a family no other manifest under the prefix shares, so
    /// `ensure_single_export` refuses the load — and since nothing can supersede
    /// that marker, the prefix stays bricked until a human deletes the object.
    /// The family is read back VERBATIM when recorded (`resolved_family`), so
    /// the substring fold cannot rescue a wrong value: recording the name is
    /// strictly worse than recording nothing.
    ///
    /// SCOPE, stated because a green test here is easy to over-read: this
    /// exercises the READ side only. It builds the marker by hand and does NOT
    /// go through `write_running_manifest`, so it stays green against a writer
    /// that records the wrong family — verified by mutating the writer back and
    /// watching this pass. The writer is cloud-only (it returns early for a
    /// local destination, `pipeline/finalize.rs`), so no unit test can observe
    /// it; the real closure is to route that hand-rolled `RunManifest` literal
    /// through `ManifestBuilder`, which REQUIRES the family and would make the
    /// two writers-for-one-run structurally unable to disagree.
    #[test]
    fn a_snapshot_legs_running_marker_documents_the_family_contract() {
        let keyed = |m: RunManifest| ("gs://b/p/manifest-x.json".to_string(), m);
        let mut drain = manifest("r1", 10, None);
        drain.export_name = "orders".into();
        drain.export_family = "sales".into();
        let mut marker = manifest("r2", 0, None);
        marker.export_name = "sales__snapshot_orders".into();
        marker.export_family = "sales".into(); // the family, as the writer records
        marker.status = ManifestStatus::Running;
        assert!(
            ensure_single_export(&[keyed(drain.clone()), keyed(marker.clone())]).is_ok(),
            "a leg\'s marker belongs to its parent\'s family"
        );
        // And the failure this guards: the same marker carrying the NAME.
        let mut wrong = marker;
        wrong.export_family = "sales__snapshot_orders".into();
        assert!(
            ensure_single_export(&[keyed(drain), keyed(wrong)]).is_err(),
            "recording the decorated name reads as a second export — the bricked prefix"
        );
    }

    /// ONE aborted run must not brick the prefix. `reconcile` bails on the first
    /// non-Success manifest it is handed, and nothing ever deletes a Failed one —
    /// gc removes `.parquet` and superseded Running markers, never a terminal
    /// manifest. So before this, run #7 failing made runs #8, #9, #10 unloadable
    /// FOREVER, with an error blaming a shared prefix instead of naming the
    /// cause; recovery was a human deleting one object from the bucket.
    ///
    /// The Failed run's own parts are deliberately NOT loaded: they are a
    /// partial export whose data returns complete under a new run_id, and
    /// `gc_orphans` already classifies them as terminal debris. Safe for CDC by
    /// construction — the drain writes Success unconditionally, so an acked part
    /// is never inside a Failed manifest.
    #[test]
    fn a_failed_manifest_does_not_block_the_successful_runs() {
        let keyed = |m: RunManifest| ("gs://b/p/manifest-x.json".to_string(), m);
        let mut failed = manifest("r7", 5, None);
        failed.status = ManifestStatus::Failed;
        let ok8 = manifest("r8", 10, None);
        let ok9 = manifest("r9", 10, None);
        let sel = select_runs(
            vec![keyed(failed), keyed(ok8), keyed(ok9)],
            &std::collections::HashSet::new(),
            crate::load::plan::LoadMode::Cdc,
        )
        .unwrap();
        let ids: Vec<&str> = sel.iter().map(|(_, m)| m.run_id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["r8", "r9"],
            "the successful runs must still load; the aborted run is skipped, not fatal"
        );
        // …and what survives must pass reconcile, which is where the old bail hit.
        let manifests: Vec<RunManifest> = sel.into_iter().map(|(_, m)| m).collect();
        assert!(
            reconcile(&manifests, false).is_ok(),
            "reconcile must not see the aborted run at all"
        );
    }

    /// A run that captured NOTHING must not turn the selection into a
    /// full-prefix load.
    ///
    /// `select_load_keys` bails to `all_parquet` when a manifest "can't be
    /// resolved to files" — a guard against a PARTIAL selection. But a manifest
    /// with zero parts resolves to nothing for the ordinary reason that the run
    /// legitimately produced nothing (a CDC cycle with no changes; the anchor
    /// cycle of `initial: snapshot`), and it then dragged EVERY parquet under the
    /// prefix into the load while `expected_delta` still summed the manifests to
    /// 0 — so the load appended rows nobody accounted for and rivet's own count
    /// gate fired: "appended 2 rows, expected 0 from the run manifests".
    ///
    /// Found by the scenario-artifact matrix on SQL Server, where the async
    /// capture Agent makes an empty first cycle common — but the mechanism is
    /// engine-independent: any zero-part manifest does it.
    #[test]
    fn a_zero_part_manifest_does_not_drag_the_whole_prefix_into_the_load() {
        // The shared `manifest()` helper always declares ONE part, so it cannot
        // express this case — an earlier version of this test used it and passed
        // against the unfixed product, because a declared-but-absent part IS the
        // unresolvable case the fallback is for. A zero-capture run is different:
        // it declares nothing.
        let mut empty = manifest("r_anchor", 0, None);
        empty.parts = Vec::new();
        empty.part_count = 0;
        empty.row_count = 0;
        let all = vec!["base/a.parquet".to_string(), "base/b.parquet".to_string()];
        let picked = select_load_keys(
            &[("gs://b/base/manifest-r_anchor.json".to_string(), empty)],
            &all,
        );
        assert!(
            picked.is_empty(),
            "a run with no parts contributes no files; it must not fall back to the \
             whole prefix (got {picked:?}), or the load appends rows the manifests \
             never accounted for"
        );
    }

    #[test]
    fn source_rows_is_none_when_no_manifest_probed_the_source() {
        let ms = vec![manifest("r1", 100, None), manifest("r2", 40, None)];
        let got = reconcile(&ms, false).unwrap();
        assert_eq!(got.file_rows, 140);
        assert_eq!(
            got.source_rows, None,
            "unprobed source is unknown, not zero"
        );
    }

    #[test]
    fn source_rows_present_even_if_only_some_manifests_probed() {
        let ms = vec![manifest("r1", 100, Some(100)), manifest("r2", 40, None)];
        let got = reconcile(&ms, false).unwrap();
        assert_eq!(got.source_rows, Some(100));
    }

    #[test]
    fn empty_manifests_refuses_to_load() {
        let err = reconcile(&[], false).unwrap_err().to_string();
        assert!(err.contains("refusing to load"), "{err}");
    }

    #[test]
    fn non_success_manifest_refuses_to_load() {
        let mut m = manifest("r1", 100, Some(100));
        m.status = ManifestStatus::Interrupted;
        let err = reconcile(&[m], false).unwrap_err().to_string();
        assert!(err.contains("not Success"), "{err}");
    }

    #[test]
    fn self_inconsistent_manifest_refuses_to_load() {
        // row_count claims 100 but the only committed part holds 100 → make the
        // aggregate lie by bumping the recorded row_count only.
        let mut m = manifest("r1", 100, Some(100));
        m.row_count = 999; // no longer equals committed parts' sum (100)
        let err = reconcile(&[m], false).unwrap_err().to_string();
        assert!(err.contains("inconsistent"), "{err}");
    }

    #[test]
    fn source_file_mismatch_hard_fails_by_default() {
        // Source had 120, only 100 extracted → 20 rows silently dropped.
        let m = manifest("r1", 100, Some(120));
        let err = reconcile(&[m], false).unwrap_err().to_string();
        assert!(err.contains("source→file mismatch"), "{err}");
        assert!(err.contains("dropped 20"), "{err}");
    }

    #[test]
    fn source_file_mismatch_is_allowed_under_the_override() {
        let m = manifest("r1", 100, Some(120));
        let got = reconcile(&[m], true).expect("--allow-source-drift proceeds");
        assert_eq!(got.file_rows, 100);
        assert_eq!(
            got.source_rows,
            Some(120),
            "the probed source count is still surfaced"
        );
    }

    /// A `(manifest_key, manifest)` pair with a single part named `part`.
    fn keyed(key: &str, run: &str, part: &str) -> (String, RunManifest) {
        let mut m = manifest(run, 10, Some(10));
        m.parts[0].path = part.into();
        (key.to_string(), m)
    }

    #[test]
    fn select_load_keys_picks_only_the_new_runs_parts() {
        // Two runs' files sit under the prefix; only r2 is "new".
        let all = vec![
            "base/r1-000.parquet".to_string(),
            "base/r2-000.parquet".to_string(),
        ];
        let new = vec![keyed("base/manifest-r2.json", "r2", "r2-000.parquet")];
        assert_eq!(
            select_load_keys(&new, &all),
            vec!["base/r2-000.parquet".to_string()],
            "loads r2's part only — not r1's already-loaded file"
        );
    }

    #[test]
    fn select_load_uris_lists_and_re_prefixes_selected_keys_with_the_source_bucket() {
        // select_load_keys (the pure selection) is unit-tested throughout; this
        // pins the store WRAPPER around it — list `.parquet` under the base, then
        // reconstruct each `gs://<bucket>/<key>` the loader COPYs from. A mangled
        // wrapper (empty/garbage URIs) would send the warehouse load at nothing,
        // or the wrong object — invisible to the pure-selection tests.
        let (store, _g) = fs_store(&[
            ("base/r1-000.parquet", b"a".to_vec()),
            ("base/manifest-r1.json", b"{}".to_vec()), // not .parquet — must be skipped
        ]);
        let new = vec![keyed("base/manifest-r1.json", "r1", "r1-000.parquet")];
        assert_eq!(
            select_load_uris(&store, "gs://my-bucket/base", &new).unwrap(),
            vec!["gs://my-bucket/base/r1-000.parquet".to_string()],
            "the selected key, re-prefixed with the source bucket — never the manifest"
        );
    }

    #[test]
    fn select_load_keys_resolves_a_snapshot_subprefix_manifest() {
        // A snapshot manifest lives under `base/snapshot/`; its part is relative
        // to that dir. Resolution must reconstruct the full key.
        let all = vec!["base/snapshot/snap-000.parquet".to_string()];
        let new = vec![keyed(
            "base/snapshot/manifest-r1.json",
            "r1",
            "snap-000.parquet",
        )];
        assert_eq!(
            select_load_keys(&new, &all),
            vec!["base/snapshot/snap-000.parquet".to_string()]
        );
    }

    #[test]
    fn select_load_keys_falls_back_to_full_listing_when_a_manifest_has_no_present_part() {
        // A manifest whose part isn't in the listing (legacy / renamed) → don't
        // risk a partial selection; load everything under the prefix.
        let all = vec!["base/a.parquet".to_string(), "base/b.parquet".to_string()];
        let new = vec![keyed("base/manifest-r1.json", "r1", "missing.parquet")];
        assert_eq!(
            select_load_keys(&new, &all),
            all,
            "unresolvable part → blanket fallback"
        );
    }

    #[test]
    fn select_load_keys_empty_new_set_selects_nothing_never_the_full_listing() {
        // When every run is already in the ledger the "new" set is empty. That
        // must resolve to ZERO uris — NOT the blanket fallback, which would
        // re-load the whole prefix on every up-to-date run (double-load). The
        // fallback fires only for an unresolvable NON-empty manifest.
        let all = vec![
            "base/r1-000.parquet".to_string(),
            "base/r2-000.parquet".to_string(),
        ];
        assert!(
            select_load_keys(&[], &all).is_empty(),
            "no new runs ⇒ load nothing, not everything"
        );
    }

    #[test]
    fn gc_orphans_removes_unmanifested_parquet_only() {
        let (store, _g) = fs_store(&[
            ("base/r1-000.parquet", b"aa".to_vec()),   // manifested
            ("base/orphan.parquet", b"junk".to_vec()), // crash leftover — no manifest
            ("base/manifest.json", b"{}".to_vec()),    // kept — not a .parquet
            ("base/_SUCCESS", b"".to_vec()),           // kept — not a .parquet
        ]);
        let keyed = vec![keyed("base/manifest-r1.json", "r1", "r1-000.parquet")];
        // No run is active on this prefix → the unmanifested part is dead debris.
        let (removed, bytes) =
            gc_orphans(&store, "gs://b/base", &keyed, false, &Default::default()).unwrap();
        assert_eq!(removed, 1, "only the unmanifested part is removed");
        assert_eq!(bytes, 4, "'junk' is 4 bytes");
        let mut left = store.list_files("base").unwrap();
        left.sort();
        assert_eq!(
            left,
            vec![
                "base/_SUCCESS".to_string(),
                "base/manifest.json".to_string(),
                "base/r1-000.parquet".to_string(),
            ],
            "the manifested part, the manifest, and _SUCCESS all survive"
        );
    }

    #[test]
    fn gc_orphans_of_an_all_manifested_prefix_removes_nothing() {
        let (store, _g) = fs_store(&[("base/r1-000.parquet", b"a".to_vec())]);
        let keyed = vec![keyed("base/manifest-r1.json", "r1", "r1-000.parquet")];
        assert_eq!(
            gc_orphans(&store, "gs://b/base", &keyed, false, &Default::default())
                .unwrap()
                .0,
            0
        );
    }

    #[test]
    fn gc_orphans_keeps_a_snapshot_subprefix_manifested_part() {
        let (store, _g) = fs_store(&[
            ("base/snapshot/s-000.parquet", b"a".to_vec()), // manifested under snapshot/
            ("base/orphan.parquet", b"x".to_vec()),         // top-level orphan
        ]);
        let keyed = vec![keyed("base/snapshot/manifest-s.json", "s", "s-000.parquet")];
        let (removed, _) =
            gc_orphans(&store, "gs://b/base", &keyed, false, &Default::default()).unwrap();
        assert_eq!(removed, 1, "the top-level orphan goes");
        assert_eq!(
            store.list_files("base/snapshot").unwrap(),
            vec!["base/snapshot/s-000.parquet".to_string()],
            "the snapshot-subprefix manifested part is kept"
        );
    }

    #[test]
    fn gc_orphans_deletes_a_superseded_terminal_runs_parts_even_while_a_run_is_active() {
        // A Failed run whose export has SINCE PASSED (newer same-family Success)
        // is terminal crash debris — deleted regardless of `active`, since no
        // resume will ever adopt it again. Passing active=true proves the
        // `active` gate applies only to UNmanifested parts. (Round-4: without
        // the Success successor these parts are SPARED — the next `--resume`
        // rehydrates them destination-blind; see the sibling test below.)
        let (store, _g) = fs_store(&[("base/f-000.parquet", b"x".to_vec())]);
        let mut kv = keyed("base/manifest-f.json", "f", "f-000.parquet");
        kv.1.status = ManifestStatus::Failed;
        kv.1.started_at = "2026-01-01T00:00:00Z".into();
        let mut ok = manifest("f2", 10, Some(10)); // Success, same export family
        ok.started_at = "2026-01-02T00:00:00Z".into();
        let ok = ("base/manifest-f2.json".to_string(), ok);
        assert_eq!(
            gc_orphans(&store, "gs://b/base", &[kv, ok], true, &Default::default())
                .unwrap()
                .0,
            1
        );
    }

    /// ROUND-4 HIGH (keyset × gc): between two attempts of a checkpoint-resumable
    /// export there is NO live run — the crashed attempt's Failed manifest names
    /// its committed pages, and the next `--resume` rehydrates exactly those
    /// parts from the state DB without probing the destination. gc firing in that
    /// window used to delete them (the old unconditional terminal arm), so the
    /// resume finalized Success + `_SUCCESS` naming parquet that no longer
    /// exists — rows silently absent while every count reads clean. RED against
    /// `terminal.extend` without the supersession gate.
    #[test]
    fn gc_orphans_spares_a_failed_runs_parts_until_a_success_supersedes_it() {
        let (store, _g) = fs_store(&[("base/page0.parquet", b"x".to_vec())]);
        let mut failed = keyed("base/manifest-r0.json", "r0", "page0.parquet");
        failed.1.status = ManifestStatus::Failed;
        failed.1.started_at = "2026-01-01T00:00:00Z".into();
        // No successor at all — the between-attempts window. active=false: no
        // ledger row, no marker; pre-fix this deleted the page.
        let (removed, _) =
            gc_orphans(&store, "gs://b/base", &[failed], false, &Default::default()).unwrap();
        assert_eq!(
            removed, 0,
            "a Failed run's committed pages are the next resume's payload, not debris"
        );
        assert!(
            store
                .list_files("base")
                .unwrap()
                .iter()
                .any(|k| k.ends_with("page0.parquet")),
            "the committed page must survive the between-attempts gc"
        );
    }

    #[test]
    fn gc_orphans_spares_a_terminal_runs_parts_that_an_active_resume_is_reusing() {
        // Post-0.24.3 review HIGH: a chunk-checkpoint resume ADOPTS a crashed run's run_id
        // and REUSES its already-committed parts (rehydrate_manifest_parts_from_file_log),
        // so a Failed manifest's parts are NOT dead debris while a LIVE run of the SAME export
        // is in flight (a newer, non-superseded Running marker). Deleting them mid-resume loses
        // the completed chunks. gc must SPARE them. RED against the pre-fix unconditional
        // terminal delete (which returned removed=1 even with the live resume present).
        let (store, _g) = fs_store(&[("base/chunk0.parquet", b"x".to_vec())]);
        // r0: the crashed run, Failed, referencing the completed chunk (both default export "orders").
        let mut failed = keyed("base/manifest-r0.json", "r0", "chunk0.parquet");
        failed.1.status = ManifestStatus::Failed;
        failed.1.started_at = "2026-01-01T00:00:00Z".into();
        // r1: the live resume of the SAME export — a newer Running marker adopting r0's parts.
        let live = running("r1", "2026-01-02T00:00:00Z");
        let (removed, _) = gc_orphans(
            &store,
            "gs://b/base",
            &[failed, live],
            true,
            &Default::default(),
        )
        .unwrap();
        assert_eq!(
            removed, 0,
            "a terminal run's parts that an active same-export resume is reusing must be spared"
        );
        assert!(
            store
                .list_files("base")
                .unwrap()
                .iter()
                .any(|k| k.ends_with("chunk0.parquet")),
            "the reused chunk part must survive gc while the resume is live"
        );
    }

    #[test]
    fn gc_orphans_spares_an_unmanifested_part_while_a_run_is_active() {
        // The concurrent-extract guard. A part with NO manifest, while a run is
        // ACTIVE on this prefix (the run-status ledger says so), is probably that
        // run's committed-but-not-yet-manifested write — deleting it would be
        // silent data loss on the live extract. RED against a mutant that ignores
        // `active` (deletes every non-Success-manifested `.parquet`).
        let (store, _g) = fs_store(&[
            ("base/r1-000.parquet", b"aa".to_vec()),  // manifested (kept)
            ("base/inflight.parquet", b"x".to_vec()), // unmanifested, a live run's part
        ]);
        let keyed = vec![keyed("base/manifest-r1.json", "r1", "r1-000.parquet")];
        let (removed, _) =
            gc_orphans(&store, "gs://b/base", &keyed, true, &Default::default()).unwrap();
        assert_eq!(
            removed, 0,
            "an unmanifested part is spared while a run is active"
        );
        assert!(
            store
                .list_files("base")
                .unwrap()
                .iter()
                .any(|k| k.ends_with("inflight.parquet")),
            "the live run's in-flight part must survive gc_orphans"
        );
    }

    #[test]
    fn gc_orphans_collects_an_unmanifested_part_when_no_run_is_active() {
        // The other half: with NO run active, an unmanifested part is a dead
        // crash orphan and IS collected — the gate DEFERS cleanup, never abandons.
        let (store, _g) = fs_store(&[
            ("base/r1-000.parquet", b"aa".to_vec()),
            ("base/dead-orphan.parquet", b"x".to_vec()),
        ]);
        let keyed = vec![keyed("base/manifest-r1.json", "r1", "r1-000.parquet")];
        assert_eq!(
            gc_orphans(&store, "gs://b/base", &keyed, false, &Default::default())
                .unwrap()
                .0,
            1,
            "with no active run, an unmanifested orphan is collected"
        );
    }

    /// A `running` MARKER manifest (schema-less, no parts) started at `started_at`.
    fn running(run: &str, started_at: &str) -> (String, RunManifest) {
        let mut m = manifest(run, 0, None);
        m.status = ManifestStatus::Running;
        m.started_at = started_at.into();
        m.finished_at = String::new();
        m.parts.clear();
        (format!("base/manifest-{run}.json"), m)
    }

    #[test]
    fn has_active_running_manifest_true_for_a_lone_running_marker() {
        assert!(has_active_running_manifest(&[running(
            "r1",
            "2026-01-01T00:00:00Z"
        )]));
    }

    #[test]
    fn a_later_started_split_sibling_finishing_first_does_not_mask_a_live_sibling() {
        // Post-0.24.3 convergence HIGH: `--pool --split` fans `orders` into siblings that run
        // CONCURRENTLY under the shared family "orders". orders#2 started LAST (T2) but its window
        // is smallest so it finishes FIRST (Success); orders#0/#1 are still Running with in-flight
        // parts. Supersession keyed on (family, started_at) alone would mark #0/#1 superseded by
        // #2 → has_active_running_manifest=false → a cross-host gc_orphans deletes #0/#1's live
        // parts. The sibling exclusion must keep the two live markers ACTIVE. RED against the
        // pre-fix predicate (which returned false here).
        let unit = |name: &str, status, started: &str| {
            let mut m = manifest(name, 0, None);
            m.export_name = name.into();
            m.export_family = "orders".into();
            m.status = status;
            m.started_at = started.into();
            m.finished_at = String::new();
            m.parts.clear();
            (format!("base/manifest-{name}.json"), m)
        };
        let keyed = vec![
            unit("orders#0", ManifestStatus::Running, "2026-01-01T00:00:00Z"),
            unit("orders#1", ManifestStatus::Running, "2026-01-01T00:00:01Z"),
            unit("orders#2", ManifestStatus::Success, "2026-01-01T00:00:02Z"),
        ];
        assert!(
            has_active_running_manifest(&keyed),
            "two still-Running split siblings must count as active even when a later-started \
             sibling has already finished — else a cross-host gc deletes their in-flight parts"
        );
        // A merely-RUNNING re-run of the same unit does NOT supersede (ledger
        // parity, round-4): the successor cannot prove the older writer died,
        // and it keeps the prefix active by itself — nothing is lost by waiting.
        let crashed = unit("orders#0", ManifestStatus::Running, "2026-01-01T00:00:00Z");
        let successor = unit("orders#0", ManifestStatus::Running, "2026-01-02T00:00:00Z");
        let rerun = [crashed.clone(), successor];
        let census = ManifestCensus::new(&rerun);
        assert!(
            !census.superseded(&census.runs()[0]),
            "a running successor proves nothing about the older marker"
        );
        assert!(census.active_running());
        // Its SUCCESS does: a finished full pass retires the crashed marker.
        let done = unit("orders#0", ManifestStatus::Success, "2026-01-02T00:00:00Z");
        let rerun = [crashed, done];
        let census = ManifestCensus::new(&rerun);
        assert!(
            census.superseded(&census.runs()[0]),
            "a SUCCESS re-run of the SAME unit (orders#0) supersedes its crashed marker"
        );
    }

    #[test]
    fn has_active_running_manifest_false_when_none_is_running() {
        // A Success manifest is not a live-run marker.
        assert!(!has_active_running_manifest(&[keyed(
            "k",
            "r1",
            "p.parquet"
        )]));
    }

    #[test]
    fn has_active_running_manifest_false_for_a_superseded_running_marker() {
        // r1 crashed leaving a `running` marker; r2 (newer started_at, SAME
        // export) already ran → r1 is stale and must NOT count as active. Same
        // clock-free supersession the ledger uses.
        let r1 = running("r1", "2026-01-01T00:00:00Z");
        let mut r2 = manifest("r2", 10, Some(10)); // Success, same export ("orders")
        r2.started_at = "2026-01-02T00:00:00Z".into();
        assert!(!has_active_running_manifest(&[
            r1,
            ("base/manifest-r2.json".into(), r2)
        ]));
    }

    #[test]
    fn select_runs_drops_a_running_manifest() {
        // A `running` (in-flight) manifest must NEVER be selected for load — else
        // reconcile would refuse the whole load on a non-Success run. Incremental
        // mode would otherwise include it (not yet loaded).
        let run_marker = running("r_running", "2026-01-02T00:00:00Z");
        let ok = keyed("base/manifest-r_ok.json", "r_ok", "r_ok-000.parquet"); // Success
        let sel = select_runs(
            vec![run_marker, ok],
            &std::collections::HashSet::new(),
            crate::load::plan::LoadMode::Incremental,
        )
        .unwrap();
        assert_eq!(sel.len(), 1, "only the Success run is selected");
        assert_eq!(sel[0].1.run_id, "r_ok");
        assert!(sel.iter().all(|(_, m)| m.status != ManifestStatus::Running));
    }

    #[test]
    fn gc_orphans_removes_a_superseded_running_marker_but_spares_a_live_one() {
        // r1 crashed leaving a `running` marker; r2 (newer, same export) RAN TO
        // SUCCESS. The DEAD superseded marker must be GC'd (else it accumulates
        // forever — gc otherwise only lists `.parquet`). Success-only (round-4
        // ledger parity): when the successor is merely another RUNNING marker —
        // plain overlap, either could be live — NEITHER marker is swept, because
        // deleting a live run's marker mid-flight destroys the one signal that
        // protects its unmanifested parts from the next cross-host gc.
        let (store, _g) = fs_store(&[
            ("base/manifest-r1.json", b"{}".to_vec()), // superseded running marker
            ("base/manifest-r2.json", b"{}".to_vec()), // the successor's manifest
        ]);
        let r1 = running("r1", "2026-01-01T00:00:01Z");
        let mut ok = manifest("r2", 10, Some(10)); // Success, same export
        ok.started_at = "2026-01-01T00:00:02Z".into();
        let r2 = ("base/manifest-r2.json".to_string(), ok);
        let (removed, _) =
            gc_orphans(&store, "gs://b/base", &[r1, r2], true, &Default::default()).unwrap();
        assert_eq!(removed, 1, "only the superseded running marker is removed");
        let left = store.list_files("base").unwrap();
        assert!(
            !left.iter().any(|k| k.ends_with("manifest-r1.json")),
            "the superseded (dead) running marker is deleted"
        );
        assert!(
            left.iter().any(|k| k.ends_with("manifest-r2.json")),
            "the successor's manifest survives"
        );

        // Overlap: two running markers, no Success — both survive the sweep.
        let (store, _g) = fs_store(&[
            ("base/manifest-r1.json", b"{}".to_vec()),
            ("base/manifest-r2.json", b"{}".to_vec()),
        ]);
        let r1 = running("r1", "2026-01-01T00:00:01Z");
        let r2 = running("r2", "2026-01-01T00:00:02Z");
        let (removed, _) =
            gc_orphans(&store, "gs://b/base", &[r1, r2], true, &Default::default()).unwrap();
        assert_eq!(
            removed, 0,
            "an overlapping running successor sweeps NOTHING"
        );
        assert_eq!(store.list_files("base").unwrap().len(), 2);
    }

    /// ROUND-5 (three agents): a `running` marker whose run_id the LEDGER says
    /// is TERMINAL is a dead crash marker even though no Success ever
    /// superseded it — an abandoned export's marker CANNOT be superseded, so
    /// without this arm `state finish-run` unblocked the ledger half while the
    /// marker kept cleanup refusing forever (and the CLI's success message
    /// lied). The arm retires exactly the ledger-terminal markers; an id the
    /// ledger still calls running (or an empty set — the stateless caller)
    /// keeps the conservative supersession-only sweep. RED against removing
    /// `|| dead_by_ledger`.
    #[test]
    fn the_sweep_retires_a_running_marker_whose_ledger_row_is_terminal() {
        let (store, _g) = fs_store(&[
            ("base/manifest-r1.json", b"{}".to_vec()),
            ("base/manifest-r2.json", b"{}".to_vec()),
        ]);
        let r1 = running("r1", "2026-01-01T00:00:01Z");
        let r2 = running("r2", "2026-01-01T00:00:02Z");
        let dead = std::collections::HashSet::from(["r1".to_string()]);
        let (removed, _) = gc_orphans(&store, "gs://b/base", &[r1, r2], true, &dead).unwrap();
        assert_eq!(removed, 1, "only the ledger-terminal marker is swept");
        let left = store.list_files("base").unwrap();
        assert!(!left.iter().any(|k| k.ends_with("manifest-r1.json")));
        assert!(
            left.iter().any(|k| k.ends_with("manifest-r2.json")),
            "a marker the ledger still calls running survives"
        );
    }

    fn keyed_at(run: &str, finished_at: &str) -> (String, RunManifest) {
        let mut m = manifest(run, 100, None);
        m.finished_at = finished_at.into();
        (format!("base/manifest-{run}.json"), m)
    }

    #[test]
    fn latest_full_picks_the_newest_snapshot_not_all() {
        // Full loads OVERWRITE, so only the LATEST snapshot may be loaded —
        // selecting all accumulated runs would load duplicate snapshots.
        let keyed = vec![
            keyed_at("r1", "2026-01-01T00:00:00Z"),
            keyed_at("r3", "2026-01-03T00:00:00Z"),
            keyed_at("r2", "2026-01-02T00:00:00Z"),
        ];
        let sel = latest_full(keyed);
        assert_eq!(sel.len(), 1, "exactly one snapshot, never all");
        assert_eq!(sel[0].1.run_id, "r3", "the newest by finished_at");
    }

    #[test]
    fn latest_full_over_a_split_family_selects_every_unit_not_just_the_last() {
        // A `--pool --split` snapshot: 3 units {family}#0..#2, each its own run_id and a
        // slightly different finished_at. Full load must select ALL THREE (the whole
        // snapshot), not just whichever unit finished last — the old max_by-over-all
        // dropped 2 of 3 units silently.
        let unit = |name: &str, run: &str, at: &str| {
            let mut m = manifest(run, 100, None);
            m.export_name = name.into();
            m.export_family = "orders".into();
            m.finished_at = at.into();
            (format!("orders/manifest-{run}.json"), m)
        };
        let keyed = vec![
            unit("orders#0", "r0", "2026-01-01T00:00:01Z"),
            unit("orders#1", "r1", "2026-01-01T00:00:02Z"),
            unit("orders#2", "r2", "2026-01-01T00:00:03Z"),
        ];
        let mut sel = latest_full(keyed);
        sel.sort_by(|a, b| a.1.export_name.cmp(&b.1.export_name));
        let names: Vec<&str> = sel.iter().map(|(_, m)| m.export_name.as_str()).collect();
        assert_eq!(
            names,
            vec!["orders#0", "orders#1", "orders#2"],
            "a Full load over a split prefix must select EVERY unit — one per {{family}}#i — \
             not just the unit that finished last"
        );
    }

    #[test]
    fn latest_full_over_a_split_family_takes_the_newest_run_per_unit() {
        // Repeated Full runs into a split prefix: each unit has TWO runs. Per unit, only
        // the newest is selected (replace semantics), and all units are present.
        let unit = |name: &str, run: &str, at: &str| {
            let mut m = manifest(run, 100, None);
            m.export_name = name.into();
            m.export_family = "orders".into();
            m.finished_at = at.into();
            (format!("orders/manifest-{run}.json"), m)
        };
        let keyed = vec![
            unit("orders#0", "r0a", "2026-01-01T00:00:00Z"),
            unit("orders#0", "r0b", "2026-01-02T00:00:00Z"), // newer #0
            unit("orders#1", "r1a", "2026-01-01T00:00:00Z"),
            unit("orders#1", "r1b", "2026-01-02T00:00:00Z"), // newer #1
        ];
        let mut sel = latest_full(keyed);
        sel.sort_by(|a, b| a.1.export_name.cmp(&b.1.export_name));
        let picked: Vec<(&str, &str)> = sel
            .iter()
            .map(|(_, m)| (m.export_name.as_str(), m.run_id.as_str()))
            .collect();
        assert_eq!(
            picked,
            vec![("orders#0", "r0b"), ("orders#1", "r1b")],
            "each unit contributes its NEWEST run; no unit is dropped and no stale run loaded"
        );
    }

    #[test]
    fn select_runs_full_refuses_a_mixed_generation_split_prefix() {
        // Two split generations accumulated under one prefix: gen A split into 3
        // (orders#0..#2), a later clean run gen B into 2 (orders#0..#1). Nothing deletes gen
        // A's copies, so latest_full's group-by-name picks B#0, B#1, and STALE A#2 — TWO top
        // (hi=None) units → overlapping coverage → a Full (replace) load would DUPLICATE the
        // (700, ∞) rows. select_runs must REFUSE, not silently duplicate.
        let unit = |name: &str, run: &str, lo: Option<&str>, hi: Option<&str>, at: &str| {
            let mut m = manifest(run, 100, None);
            m.export_name = name.into();
            m.export_family = "orders".into();
            m.finished_at = at.into();
            m.split_window = Some(crate::manifest::SplitWindow {
                key_column: "id".into(),
                lo: lo.map(String::from),
                hi: hi.map(String::from),
            });
            (format!("orders/manifest-{run}.json"), m)
        };
        let keyed = vec![
            unit("orders#0", "b0", None, Some("500"), "2026-01-02T00:00:00Z"),
            unit("orders#1", "b1", Some("500"), None, "2026-01-02T00:00:01Z"),
            unit("orders#2", "a2", Some("700"), None, "2026-01-01T00:00:00Z"), // stale gen A top
        ];
        let err = select_runs(
            keyed,
            &std::collections::HashSet::new(),
            crate::load::plan::LoadMode::Full,
        )
        .expect_err("a mixed-generation split prefix must be refused, not silently loaded");
        assert!(
            err.to_string().contains("more than one split generation")
                || err.to_string().contains("DUPLICATE"),
            "error must name the multi-generation duplication hazard: {err}"
        );
    }

    #[test]
    fn select_runs_full_refuses_an_equal_count_split_generation_with_an_interior_hole() {
        // The subtler mixed-generation case the bottom/top COUNT guard misses (post-0.24.3
        // review HIGH). Gen A and gen B both split into 4, so there is exactly ONE bottom
        // (lo=None) and ONE top (hi=None) — bottoms==1/tops==1/plain==0 all pass. But gen B
        // re-sampled to DIFFERENT boundaries (200/400/600 vs A's 250/500/750) and its interior
        // unit #2 crashed, so latest_full substitutes STALE gen A #2 (window (500,750]). The
        // four selected windows no longer tile: (400,500] is in NO unit (LOST) and (600,750] is
        // in BOTH A#2 and B#3 (DUPLICATED). Every count/sum still passes. Must REFUSE.
        let unit = |name: &str, run: &str, lo: Option<&str>, hi: Option<&str>, at: &str| {
            let mut m = manifest(run, 100, None);
            m.export_name = name.into();
            m.export_family = "orders".into();
            m.finished_at = at.into();
            m.split_window = Some(crate::manifest::SplitWindow {
                key_column: "id".into(),
                lo: lo.map(String::from),
                hi: hi.map(String::from),
            });
            (format!("orders/manifest-{run}.json"), m)
        };
        let keyed = vec![
            unit("orders#0", "b0", None, Some("200"), "2026-01-02T00:00:00Z"),
            unit(
                "orders#1",
                "b1",
                Some("200"),
                Some("400"),
                "2026-01-02T00:00:01Z",
            ),
            // gen B #2 crashed → stale gen A #2 (500,750] substituted (older finished_at):
            unit(
                "orders#2",
                "a2",
                Some("500"),
                Some("750"),
                "2026-01-01T00:00:00Z",
            ),
            unit("orders#3", "b3", Some("600"), None, "2026-01-02T00:00:03Z"),
        ];
        let err = select_runs(
            keyed,
            &std::collections::HashSet::new(),
            crate::load::plan::LoadMode::Full,
        )
        .expect_err(
            "an equal-count split generation with a gap+overlap must be refused, not loaded",
        );
        assert!(
            err.to_string().contains("tile") || err.to_string().contains("INCOHERENT"),
            "error must name the non-tiling (gap+overlap) hazard: {err}"
        );
    }

    #[test]
    fn select_runs_full_accepts_one_coherent_split_generation() {
        // A single clean split generation (one bottom lo=None, one top hi=None) loads all its
        // units — the headline fix — without tripping the guard.
        let unit = |name: &str, run: &str, lo: Option<&str>, hi: Option<&str>| {
            let mut m = manifest(run, 100, None);
            m.export_name = name.into();
            m.export_family = "orders".into();
            m.split_window = Some(crate::manifest::SplitWindow {
                key_column: "id".into(),
                lo: lo.map(String::from),
                hi: hi.map(String::from),
            });
            (format!("orders/manifest-{run}.json"), m)
        };
        let keyed = vec![
            unit("orders#0", "r0", None, Some("500")),
            unit("orders#1", "r1", Some("500"), None),
        ];
        let sel = select_runs(
            keyed,
            &std::collections::HashSet::new(),
            crate::load::plan::LoadMode::Full,
        )
        .unwrap();
        assert_eq!(
            sel.len(),
            2,
            "both units of one coherent generation must load"
        );
    }

    #[test]
    fn latest_full_re_materializes_even_when_the_latest_is_already_loaded() {
        // Full is NOT ledger-skipped: a re-load re-OVERWRITEs from the latest
        // snapshot, self-healing a drifted target and staying resilient to hidden
        // in-place updates. The ledger must NOT suppress the re-materialization —
        // full has to stay full (the old `latest_unloaded_full` skip was the bug).
        let keyed = vec![
            keyed_at("r1", "2026-01-01T00:00:00Z"),
            keyed_at("r2", "2026-01-02T00:00:00Z"),
        ];
        // r2 is "already loaded" in the caller's ledger, yet full still selects it.
        let sel = latest_full(keyed);
        assert_eq!(sel.len(), 1);
        assert_eq!(sel[0].1.run_id, "r2", "always the latest, loaded or not");
    }

    #[test]
    fn latest_full_of_no_staged_runs_is_empty_so_the_caller_no_ops_without_truncating() {
        // Empty staging (e.g. cleaned, no fresh extract) → empty selection → the
        // caller returns None and never truncates the target to empty.
        assert!(latest_full(Vec::new()).is_empty());
    }

    #[test]
    fn latest_full_orders_by_parsed_instant_not_lexical_bytes() {
        // `…00.500Z` is LATER than `…00Z` but sorts EARLIER lexically ('.' < 'Z').
        // Parsing to an instant picks the truly-newest run, not the lexical max.
        let keyed = vec![
            keyed_at("older", "2026-01-01T00:00:00Z"),
            keyed_at("newer", "2026-01-01T00:00:00.500Z"),
        ];
        let sel = latest_full(keyed);
        assert_eq!(sel.len(), 1);
        assert_eq!(
            sel[0].1.run_id, "newer",
            "the fractional-second run is the newer instant"
        );
    }

    #[test]
    fn select_runs_full_picks_the_latest_even_when_loaded_and_even_when_stateless() {
        use crate::load::plan::LoadMode;
        use std::collections::HashSet;
        let keyed = vec![
            keyed_at("r1", "2026-01-01T00:00:00Z"),
            keyed_at("r2", "2026-01-02T00:00:00Z"),
        ];
        // Stateful, r2 already loaded → still selects r2 (re-materialize/self-heal).
        let loaded = HashSet::from(["r2".to_string()]);
        let sel = select_runs(keyed.clone(), &loaded, LoadMode::Full).unwrap();
        assert_eq!(sel.len(), 1);
        assert_eq!(sel[0].1.run_id, "r2", "Full picks latest, loaded or not");
        // STATELESS (empty loaded) → the latest, NEVER a blanket load of both
        // snapshots (the duplicate-rows bug this fix closes).
        let sel = select_runs(keyed, &HashSet::new(), LoadMode::Full).unwrap();
        assert_eq!(sel.len(), 1, "stateless Full is not a blanket load");
        assert_eq!(sel[0].1.run_id, "r2");
    }

    #[test]
    fn select_runs_append_modes_filter_loaded_and_load_all_when_stateless() {
        use crate::load::plan::LoadMode;
        use std::collections::HashSet;
        let keyed = vec![
            keyed_at("r1", "2026-01-01T00:00:00Z"),
            keyed_at("r2", "2026-01-02T00:00:00Z"),
        ];
        let loaded = HashSet::from(["r1".to_string()]);
        for mode in [LoadMode::Incremental, LoadMode::Cdc] {
            let sel = select_runs(keyed.clone(), &loaded, mode).unwrap();
            assert_eq!(sel.len(), 1, "{mode:?}: only the unloaded run");
            assert_eq!(sel[0].1.run_id, "r2");
            // Stateless → empty loaded → load all (at-least-once, dedup absorbs).
            assert_eq!(
                select_runs(keyed.clone(), &HashSet::new(), mode)
                    .unwrap()
                    .len(),
                2,
                "{mode:?}: stateless loads every run"
            );
        }
    }

    #[test]
    fn is_run_unique_manifest_needs_both_prefix_and_json() {
        assert!(is_run_unique_manifest("manifest-20260101T000000.json"));
        assert!(!is_run_unique_manifest("manifest.json")); // no `-` after manifest
        assert!(!is_run_unique_manifest("manifest-abc.txt")); // not .json
        assert!(!is_run_unique_manifest("data.json")); // wrong prefix
    }

    #[test]
    fn chain_prefix_renders_source_and_files() {
        let known = LoadIntegrity {
            source_rows: Some(100),
            file_rows: 100,
            manifests: 1,
        };
        assert_eq!(known.chain_prefix(), "source 100 → files 100");
        let unknown = LoadIntegrity {
            source_rows: None,
            file_rows: 40,
            manifests: 1,
        };
        assert_eq!(unknown.chain_prefix(), "source ? → files 40");
    }

    #[test]
    fn is_manifest_key_matches_only_the_final_segment() {
        assert!(is_manifest_key("gs://b/p/manifest.json"));
        assert!(is_manifest_key("manifest.json"));
        assert!(!is_manifest_key("gs://b/p/part-0.parquet"));
        assert!(!is_manifest_key("gs://b/p/x_manifest.json"));
    }

    // ---- offline (filesystem-backed store) transport tests ----

    /// Build an fs-backed [`GcsStore`] over a fresh tempdir seeded with
    /// `(bucket-relative-key, bytes)` objects. Returns the store and the guard —
    /// hold the `TempDir` for the store's lifetime.
    fn fs_store(files: &[(&str, Vec<u8>)]) -> (GcsStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        for (rel, bytes) in files {
            let p = dir.path().join(rel);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(p, bytes).unwrap();
        }
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        (store, dir)
    }

    fn manifest_bytes(run: &str, rows: i64, source: Option<i64>) -> Vec<u8> {
        serde_json::to_vec(&manifest(run, rows, source)).unwrap()
    }

    #[test]
    fn dedupe_drops_the_pointer_but_keeps_a_legacy_canonical_only_run() {
        // The double-count guard moved from the KEY level to the RUN-ID level
        // (#173): the canonical pointer is dropped when its run_id also exists
        // as an immutable copy — but a LEGACY run whose only record IS the
        // canonical name (pre-copy prefix that later gained copies) must
        // survive. The old "copies win when any exist" key filter silently
        // dropped it (upgrade-compat under-count); RED against that filter.
        //
        // Case 1: pointer's run_id (r2) is covered by a copy → pointer dropped.
        let keyed = vec![
            (
                "base/manifest.json".to_string(),
                serde_json::from_slice::<RunManifest>(&manifest_bytes("r2", 40, Some(40))).unwrap(),
            ),
            (
                "base/manifest-r1.json".to_string(),
                serde_json::from_slice::<RunManifest>(&manifest_bytes("r1", 100, Some(100)))
                    .unwrap(),
            ),
            (
                "base/manifest-r2.json".to_string(),
                serde_json::from_slice::<RunManifest>(&manifest_bytes("r2", 40, Some(40))).unwrap(),
            ),
        ];
        let mut ids: Vec<String> = dedupe_by_run_id(keyed)
            .into_iter()
            .map(|(_, m)| m.run_id)
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["r1".to_string(), "r2".to_string()]);

        // Case 2: the canonical names a LEGACY run (r0) no copy covers → kept.
        let keyed = vec![
            (
                "base/manifest.json".to_string(),
                serde_json::from_slice::<RunManifest>(&manifest_bytes("r0", 7, Some(7))).unwrap(),
            ),
            (
                "base/manifest-r1.json".to_string(),
                serde_json::from_slice::<RunManifest>(&manifest_bytes("r1", 100, Some(100)))
                    .unwrap(),
            ),
        ];
        let mut ids: Vec<String> = dedupe_by_run_id(keyed)
            .into_iter()
            .map(|(_, m)| m.run_id)
            .collect();
        ids.sort();
        assert_eq!(
            ids,
            vec!["r0".to_string(), "r1".to_string()],
            "a legacy canonical-only run must not be silently dropped"
        );
    }

    #[test]
    fn list_manifest_keys_falls_back_to_the_canonical_name_for_a_single_run() {
        // A single batch run (or a legacy prefix) has only the canonical name —
        // with no per-run copy, it must still be found.
        let (store, _g) = fs_store(&[("base/manifest.json", b"{}".to_vec())]);
        assert_eq!(
            list_manifest_keys(&store, "base").unwrap(),
            vec!["base/manifest.json".to_string()]
        );
    }

    #[test]
    fn fetch_manifests_keyed_reads_and_parses_every_run_copy_under_the_prefix() {
        // Two runs' copies plus a canonical pointer to the latest: fetch returns
        // exactly the two run copies, parsed — so reconcile sums 100 + 40 = 140
        // without double-counting the pointer.
        let (store, _g) = fs_store(&[
            ("base/manifest.json", manifest_bytes("r2", 40, Some(40))),
            (
                "base/manifest-r1.json",
                manifest_bytes("r1", 100, Some(100)),
            ),
            ("base/manifest-r2.json", manifest_bytes("r2", 40, Some(40))),
        ]);
        let manifests: Vec<_> = fetch_manifests_keyed(&store, "gs://my-bucket/base")
            .unwrap()
            .into_iter()
            .map(|(_, m)| m)
            .collect();
        assert_eq!(manifests.len(), 2);
        let integrity = reconcile(&manifests, false).unwrap();
        assert_eq!(integrity.file_rows, 140);
        assert_eq!(integrity.manifests, 2);
    }

    #[test]
    fn fetch_manifests_keyed_names_the_key_when_a_manifest_is_unparseable() {
        let (store, _g) = fs_store(&[("base/manifest.json", b"{ not json".to_vec())]);
        let err = fetch_manifests_keyed(&store, "gs://my-bucket/base")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("parsing manifest") && err.contains("base/manifest.json"),
            "error should name the offending key: {err}"
        );
    }

    // ── fake-gcs-server: the storage contract over the REAL opendal-GCS transport
    //
    // Every test above runs the storage-contract fns over an Fs-backed GcsStore —
    // proving the LOGIC, but not that opendal's GCS client speaks the same
    // list/read/remove semantics a real bucket answers with. This cell closes that
    // gap against fsouza/fake-gcs-server (the GCS JSON API emulator the extract
    // side already uses), so a GCS-only regression — listing pagination, prefix
    // handling, `remove_all` recursion — fails HERE, not in production. The
    // warehouse half (bq/snow COPY) can't be emulated; it stays the live BigQuery
    // matrix (`smoke_batch_mysql` / `StagingWiped`). This owns the BUCKET half.
    const FAKE_GCS_ENDPOINT: &str = "http://127.0.0.1:4443";
    const FAKE_GCS_BUCKET: &str = "rivet-load-emulator";

    /// A real GCS-transport [`GcsStore`] against the local emulator, scoped to
    /// `FAKE_GCS_BUCKET`. Ensures the bucket first via the emulator's JSON API
    /// (fake-gcs does not auto-create on write) — an idempotent POST, so a re-run
    /// needs no teardown. curl, not a new HTTP dep: this is a dev-only cell.
    fn fake_gcs_store() -> GcsStore {
        let created = std::process::Command::new("curl")
            .args([
                "-s",
                "-X",
                "POST",
                &format!("{FAKE_GCS_ENDPOINT}/storage/v1/b?project=rivet-test"),
                "-H",
                "Content-Type: application/json",
                "-d",
                &format!("{{\"name\":\"{FAKE_GCS_BUCKET}\"}}"),
            ])
            .output();
        assert!(
            created.is_ok_and(|o| o.status.success()),
            "could not reach fake-gcs to create the bucket — is `docker compose up -d fake-gcs` running on :4443?"
        );
        let cfg = crate::config::DestinationConfig {
            destination_type: crate::config::DestinationType::Gcs,
            bucket: Some(FAKE_GCS_BUCKET.into()),
            endpoint: Some(FAKE_GCS_ENDPOINT.into()),
            allow_anonymous: true,
            ..Default::default()
        };
        GcsStore::new(&cfg).expect("build GcsStore against fake-gcs")
    }

    /// Per-object drain of `prefix` — list + single `remove` each. fake-gcs
    /// implements single-object DELETE but NOT the GCS batch-delete endpoint that
    /// opendal's `remove_all` issues for 2+ objects (that 400s with `deleted: 0`),
    /// so the emulator can't run the `cleanup_source` batch wipe. That wipe
    /// (`delete_under` → `remove_all`) is verified against a REAL bucket by the
    /// live BigQuery `StagingWiped` matrix, and over the Fs seam above; here we
    /// drain per-object for idempotent setup/teardown.
    fn drain(store: &GcsStore, prefix: &str) {
        for key in store.list_files(prefix).unwrap() {
            store.remove(&key).unwrap();
        }
    }

    #[test]
    #[ignore = "emulator: needs `docker compose up -d fake-gcs` (fsouza/fake-gcs-server :4443)"]
    fn storage_contract_over_fake_gcs() {
        let store = fake_gcs_store();
        // Test-owned prefix, drained first so a re-run starts clean even though the
        // emulator bucket persists (no cross-run bleed).
        let prefix = "load-contract/orders";
        drain(&store, prefix);
        let gs = format!("gs://{FAKE_GCS_BUCKET}/{prefix}");

        // Seed one Success run (its manifest + committed part) plus a crash orphan
        // — an unmanifested `.parquet` an interrupted extract would leave behind.
        store
            .put(
                &format!("{prefix}/manifest-r1.json"),
                &manifest_bytes("r1", 100, Some(100)),
            )
            .unwrap();
        store
            .put(&format!("{prefix}/part-000000.parquet"), b"rows-of-r1")
            .unwrap();
        store
            .put(&format!("{prefix}/orphan.parquet"), b"crash-leftover")
            .unwrap();

        // 1. fetch + parse the manifest back over real GCS list+read.
        let keyed = fetch_manifests_keyed(&store, &gs).unwrap();
        assert_eq!(keyed.len(), 1, "the run's manifest, read back over GCS");

        // 2. reconcile → file_rows, the value the warehouse COUNT(*) gate enforces.
        let manifests: Vec<_> = keyed.iter().map(|(_, m)| m.clone()).collect();
        assert_eq!(
            reconcile(&manifests, false).unwrap().file_rows,
            100,
            "file_rows drives the count-gate; a bad GCS read would corrupt it"
        );

        // 3. select_load_uris resolves the MANIFESTED part only — never the orphan.
        assert_eq!(
            select_load_uris(&store, &gs, &keyed).unwrap(),
            vec![format!(
                "gs://{FAKE_GCS_BUCKET}/{prefix}/part-000000.parquet"
            )],
            "load pulls the manifested part, not the unmanifested crash orphan"
        );

        // 4. gc_orphans deletes the orphan over real GCS — a REAL single-object
        // DELETE against the emulator — keeping the manifested part + manifest.
        let (removed, _bytes) =
            gc_orphans(&store, &gs, &keyed, false, &Default::default()).unwrap();
        assert_eq!(
            removed, 1,
            "exactly the orphan parquet is GC'd over real GCS"
        );
        let mut left = store.list_files(prefix).unwrap();
        left.sort();
        assert_eq!(
            left,
            vec![
                format!("{prefix}/manifest-r1.json"),
                format!("{prefix}/part-000000.parquet"),
            ],
            "the manifested part + its manifest survive the orphan GC"
        );

        // cleanup_source's batch wipe (`remove_all`) is NOT emulatable here — see
        // `drain`. Teardown per-object; the empty listing confirms the deletes
        // landed over real GCS transport.
        drain(&store, prefix);
        assert!(
            store.list_files(prefix).unwrap().is_empty(),
            "teardown left the prefix clean over real GCS"
        );
    }
}
