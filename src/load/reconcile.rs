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
use crate::manifest::{MANIFEST_FILENAME, ManifestStatus, RunManifest};
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

/// Fetch and parse every `manifest.json` under `gcs_prefix` (recursive).
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
pub fn fetch_manifests(store: &GcsStore, gcs_prefix: &str) -> Result<Vec<RunManifest>> {
    Ok(fetch_manifests_keyed(store, gcs_prefix)?
        .into_iter()
        .map(|(_, m)| m)
        .collect())
}

/// Like [`fetch_manifests`] but keeps each manifest's bucket-relative storage
/// key — needed to resolve a manifest's (relative) part paths back to full
/// object keys for per-run incremental loading (see [`select_load_uris`]).
#[allow(private_interfaces)]
pub fn fetch_manifests_keyed(
    store: &GcsStore,
    gcs_prefix: &str,
) -> Result<Vec<(String, RunManifest)>> {
    let (_, base) = crate::load::split_gs_uri(gcs_prefix)?;
    let keys = list_manifest_keys(store, base)?;
    keys.into_iter()
        .map(|key| {
            let bytes = store.read(&key)?;
            let m = serde_json::from_slice::<RunManifest>(&bytes)
                .with_context(|| format!("parsing manifest {key}"))?;
            Ok((key, m))
        })
        .collect()
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

/// Pure selection: which bucket-relative parquet keys to load for the given
/// (not-yet-loaded) run manifests.
///
/// Prefers each manifest's own parts, resolved as `<dir(manifest_key)>/<part>`
/// and intersected with `all_parquet` — so a load pulls exactly the new runs'
/// files, not every object under the prefix (the key to incremental loads once
/// `cleanup_source` no longer wipes the bucket). Falls back to the whole
/// `all_parquet` listing when ANY new manifest resolves to no present part
/// (legacy/part-less manifests still load, at the cost of not pruning); the
/// row-count gate then still guards correctness.
pub fn select_load_keys(new: &[(String, RunManifest)], all_parquet: &[String]) -> Vec<String> {
    use std::collections::BTreeSet;
    let present: std::collections::HashSet<&str> = all_parquet.iter().map(String::as_str).collect();
    let mut selected: BTreeSet<String> = BTreeSet::new();
    for (key, m) in new {
        let dir = key.rsplit_once('/').map(|(d, _)| d).unwrap_or("");
        let mut resolved_any = false;
        for p in &m.parts {
            let full = if dir.is_empty() {
                p.path.clone()
            } else {
                format!("{dir}/{}", p.path)
            };
            if present.contains(full.as_str()) {
                selected.insert(full);
                resolved_any = true;
            }
        }
        if !resolved_any {
            // Can't resolve this run to files — don't risk a partial selection.
            return all_parquet.to_vec();
        }
    }
    selected.into_iter().collect()
}

/// Full/chunked loads care only about the LATEST snapshot: from `keyed` (all run
/// manifests under the prefix), pick the newest by `finished_at` — unless its
/// `run_id` is already in `loaded`, in which case the target is current and this
/// returns empty (a skip). Returns a 0-or-1 element `Vec` so [`prepare_load`]
/// treats it uniformly with the append modes' "all new runs".
/// Full loads OVERWRITE, so exactly ONE snapshot may be loaded — the LATEST by
/// `finished_at` (loading every accumulated run would duplicate rows). It is
/// deliberately **not** ledger-gated: full re-materializes the latest snapshot
/// on *every* load, so a re-load self-heals a drifted target and full stays
/// resilient to hidden in-place source updates — its whole guarantee. An empty
/// input (no staged run — e.g. the staging was cleaned with no fresh extract)
/// yields an empty selection, so the caller no-ops WITHOUT truncating the target.
pub fn latest_full(keyed: Vec<(String, RunManifest)>) -> Vec<(String, RunManifest)> {
    keyed
        .into_iter()
        .max_by(|a, b| a.1.finished_at.cmp(&b.1.finished_at))
        .into_iter()
        .collect()
}

/// Bucket-relative keys of every run manifest under `base` (recursive).
fn list_manifest_keys(store: &GcsStore, base: &str) -> Result<Vec<String>> {
    let all: Vec<String> = store
        .list_files(base)?
        .into_iter()
        .filter(|k| is_manifest_key(k))
        .collect();
    // A run into a shared prefix leaves BOTH the canonical `manifest.json`
    // (last-writer-wins — a pointer to the LATEST run) and an immutable
    // `manifest-<run_id>.json` copy (one per run). Sum the per-run copies so a
    // prefix that accumulated several CDC/incremental cycles counts EVERY run;
    // counting the canonical pointer too would double-count the latest. Fall
    // back to the canonical name only when no per-run copy exists (a single
    // batch run, or a legacy prefix predating the run-unique copy).
    let run_unique: Vec<String> = all
        .iter()
        .filter(|k| is_run_unique_manifest(k.rsplit('/').next().unwrap_or("")))
        .cloned()
        .collect();
    Ok(if run_unique.is_empty() {
        all
    } else {
        run_unique
    })
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
    base.starts_with("manifest-") && base.ends_with(".json")
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
            manifest_version: crate::manifest::MANIFEST_VERSION,
            run_id: run.into(),
            export_name: "orders".into(),
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
    fn list_manifest_keys_prefers_run_unique_copies_over_the_canonical_pointer() {
        // A shared prefix that accumulated two runs holds the last-writer-wins
        // `manifest.json` pointer AND one immutable per-run copy each. Summing
        // must count the two run copies, never the pointer (double-count guard).
        let (store, _g) = fs_store(&[
            ("base/manifest.json", b"{}".to_vec()),
            ("base/manifest-r1.json", b"{}".to_vec()),
            ("base/manifest-r2.json", b"{}".to_vec()),
            ("base/part-0.parquet", b"x".to_vec()), // data file: not a manifest
        ]);
        let mut keys = list_manifest_keys(&store, "base").unwrap();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                "base/manifest-r1.json".to_string(),
                "base/manifest-r2.json".to_string(),
            ]
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
    fn fetch_manifests_reads_and_parses_every_run_copy_under_the_prefix() {
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
        let manifests = fetch_manifests(&store, "gs://my-bucket/base").unwrap();
        assert_eq!(manifests.len(), 2);
        let integrity = reconcile(&manifests, false).unwrap();
        assert_eq!(integrity.file_rows, 140);
        assert_eq!(integrity.manifests, 2);
    }

    #[test]
    fn fetch_manifests_names_the_key_when_a_manifest_is_unparseable() {
        let (store, _g) = fs_store(&[("base/manifest.json", b"{ not json".to_vec())]);
        let err = fetch_manifests(&store, "gs://my-bucket/base")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("parsing manifest") && err.contains("base/manifest.json"),
            "error should name the offending key: {err}"
        );
    }
}
