//! ORACLE SENSITIVITY LEDGER — rig-mutant pairs in executable form.
//!
//! The harness audit (2026-08-29) asked "who tests the tests" and answered it
//! twice in one day: `duckdb_declared_dir_id_set` shipped parsing the wrong
//! JSON shape and `unwrap_or_default()`ed the mismatch into a silent empty
//! set (arrow saw 30 ids, it saw 0), and all four declared-parts resolvers
//! counted a failed manifest's parts as delivered data. Both were oracle bugs
//! — invisible to every product-side check, because the oracle IS the check.
//!
//! Each test here is a PAIR made permanent: a hand-built BROKEN WORLD (the
//! product-RED analog — a failed manifest, an orphan part, a corrupt file, a
//! type-hostile column) fed to one oracle helper, asserting the helper either
//! reports the truth or panics LOUDLY. A pair goes RED when the helper's
//! guard is reverted (the snapshot-restore mutant protocol: copy the file
//! aside, revert the guard, watch the pair fail, restore + touch) — which is
//! exactly how three of them were proven before landing.
//!
//! File-only pairs are NOT `#[ignore]`d: they run in the offline battery on
//! every PR (the rig-golden pattern — live_suite target, no DB). Pairs that
//! need the `rivet-duckdb` / `fake-gcs` containers are `#[ignore]`d and run
//! nightly (the blanket job's `--run-ignored ignored-only` picks them up).
//!
//! What this ledger CANNOT see, said plainly: an oracle and a broken world
//! that agree on a wrong spec (the manifest grammar itself drifting), and
//! oracles whose subject needs a live engine (`row_census` legs) — those are
//! graded by the release-oracle harness and the live suites respectively.

use std::path::Path;
use std::sync::Arc;

use crate::common::*;

/// An `id: Int64` parquet part with the given ids — the minimal world cell.
fn write_ids_parquet(path: &Path, ids: &[i64]) {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow::array::Int64Array::from(ids.to_vec()))],
    )
    .unwrap();
    let file = std::fs::File::create(path).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// A CDC part carrying every column `read_cdc_changes` reads EXCEPT `__op` —
/// the world where exactly one column is missing.
///
/// The first cut wrote an `id`-only part, which panicked on `v` long before
/// `__op` was ever looked up; `should_panic(expected = "column present")` is a
/// SUBSTRING and caught `"v column present"` just as happily. Measured by a
/// critic: with the `__op` panic replaced by a silent `"MUTANT"` fallback, all
/// twelve pairs stayed green — the ledger missed an oracle inventing an op for
/// every row. The fixture must therefore be complete but for the one column
/// under test, and the expectation must NAME that column.
fn write_cdc_part_without_op(path: &Path, ids: &[i64]) {
    let n = ids.len();
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("__seq", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("__pos", arrow::datatypes::DataType::Utf8, false),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow::array::Int64Array::from(ids.to_vec())),
            Arc::new(arrow::array::Int64Array::from(vec![7i64; n])),
            Arc::new(arrow::array::Int64Array::from(
                (0..n as i64).collect::<Vec<_>>(),
            )),
            Arc::new(arrow::array::StringArray::from(vec!["0/1"; n])),
        ],
    )
    .unwrap();
    let file = std::fs::File::create(path).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// Same shape but `id` is TEXT — the type-hostile world for the numeric leg.
fn write_text_id_parquet(path: &Path, ids: &[&str]) {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Utf8, false),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow::array::StringArray::from(ids.to_vec()))],
    )
    .unwrap();
    let file = std::fs::File::create(path).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// A minimal manifest body: top-level `status`, parts with per-part statuses.
fn manifest_body(status: &str, parts: &[(&str, &str)]) -> String {
    let parts_json: Vec<String> = parts
        .iter()
        .enumerate()
        .map(|(i, (name, part_status))| {
            format!(
                r#"{{"part_id":{i},"path":"{name}","rows":1,"size_bytes":1,"status":"{part_status}"}}"#
            )
        })
        .collect();
    format!(
        r#"{{"manifest_version":1,"run_id":"r","export_name":"e","status":"{status}","parts":[{}]}}"#,
        parts_json.join(",")
    )
}

// ── declared_parquet_parts: the shared file-side resolver ────────────────────

/// PAIR (live-proven origin): a crashed keyset run leaves a `failed` manifest
/// whose part names a refused resume re-creates on disk — a status-blind union
/// counted 500 undelivered rows as declared. Mutant: remove the success-only
/// filter in `declared_parquet_parts` → this goes RED (proven pre-landing).
#[test]
fn oracle_declared_parts_skips_a_failed_manifests_parts() {
    let d = tempfile::tempdir().unwrap();
    write_ids_parquet(&d.path().join("pa.parquet"), &[1]);
    write_ids_parquet(&d.path().join("pb.parquet"), &[2]);
    std::fs::write(
        d.path().join("manifest-runA.json"),
        manifest_body("failed", &[("pa.parquet", "committed")]),
    )
    .unwrap();
    std::fs::write(
        d.path().join("manifest-runB.json"),
        manifest_body("success", &[("pb.parquet", "committed")]),
    )
    .unwrap();
    let declared = declared_parquet_parts(d.path());
    assert_eq!(
        declared,
        vec![d.path().join("pb.parquet")],
        "a failed manifest's parts are gc candidates, not delivered data"
    );
}

/// PAIR: a part the manifest lists but does not mark committed is not
/// delivered. The non-committed value is `quarantined` — the ONLY other
/// `PartStatus` the product has (src/manifest.rs); an earlier cut invented
/// `"in-flight"`, a string no writer emits, which made the pair grade a
/// fictional grammar. Mutant: drop the per-part status filter → RED.
#[test]
fn oracle_declared_parts_skips_an_uncommitted_part() {
    let d = tempfile::tempdir().unwrap();
    write_ids_parquet(&d.path().join("pa.parquet"), &[1]);
    write_ids_parquet(&d.path().join("pb.parquet"), &[2]);
    std::fs::write(
        d.path().join("manifest-run.json"),
        manifest_body(
            "success",
            &[("pa.parquet", "committed"), ("pb.parquet", "quarantined")],
        ),
    )
    .unwrap();
    assert_eq!(
        declared_parquet_parts(d.path()),
        vec![d.path().join("pa.parquet")]
    );
}

/// PAIR (the gc_orphans class): an on-disk parquet no manifest names is a
/// retry's abandoned attempt, not delivered data — the glob-vs-declared
/// difference IS the claim on every crash suite.
///
/// HONEST about its strength: the mutant it names ("fall back to a directory
/// glob") is a change nobody has made, so today this pair kills nothing — it
/// is a guard against a FUTURE rewrite of the resolver, not evidence about the
/// present one. Said here rather than left to read like a proof.
#[test]
fn oracle_declared_parts_ignores_an_orphan_part_on_disk() {
    let d = tempfile::tempdir().unwrap();
    write_ids_parquet(&d.path().join("pa.parquet"), &[1]);
    write_ids_parquet(&d.path().join("orphan.parquet"), &[99]);
    std::fs::write(
        d.path().join("manifest-run.json"),
        manifest_body("success", &[("pa.parquet", "committed")]),
    )
    .unwrap();
    assert_eq!(
        declared_parquet_parts(d.path()),
        vec![d.path().join("pa.parquet")]
    );
}

/// DOCUMENTS the missing-file leg: a declared part that is GONE from disk is
/// excluded rather than panicking — the resolver answers "what is reachable",
/// and the keyset resume REFUSAL (product side) is what makes gone-ness loud.
#[test]
fn oracle_declared_parts_documents_excluding_a_declared_but_missing_file() {
    let d = tempfile::tempdir().unwrap();
    write_ids_parquet(&d.path().join("pa.parquet"), &[1]);
    std::fs::write(
        d.path().join("manifest-run.json"),
        manifest_body(
            "success",
            &[("pa.parquet", "committed"), ("gone.parquet", "committed")],
        ),
    )
    .unwrap();
    assert_eq!(
        declared_parquet_parts(d.path()),
        vec![d.path().join("pa.parquet")]
    );
}

/// PAIR (the sidecar-clobber class): the canonical `manifest.json` is
/// last-writer-wins, so it is read ONLY when no run-unique copy exists —
/// otherwise N runs into one prefix would under-declare to the last run's
/// parts. Mutant: always include the canonical → RED (pc double-counted /
/// pa lost depending on direction).
#[test]
fn oracle_declared_parts_prefers_copies_over_the_canonical() {
    let d = tempfile::tempdir().unwrap();
    write_ids_parquet(&d.path().join("pa.parquet"), &[1]);
    write_ids_parquet(&d.path().join("pc.parquet"), &[3]);
    // Canonical alone → its parts are the answer.
    std::fs::write(
        d.path().join("manifest.json"),
        manifest_body("success", &[("pc.parquet", "committed")]),
    )
    .unwrap();
    assert_eq!(
        declared_parquet_parts(d.path()),
        vec![d.path().join("pc.parquet")]
    );
    // A run-unique copy appears → copies win, canonical is ignored.
    std::fs::write(
        d.path().join("manifest-runA.json"),
        manifest_body("success", &[("pa.parquet", "committed")]),
    )
    .unwrap();
    assert_eq!(
        declared_parquet_parts(d.path()),
        vec![d.path().join("pa.parquet")]
    );
}

// ── read_cdc_changes: the shared-codec CDC reader ────────────────────────────

/// PAIR: a declared part MISSING the __op column must panic loudly — a reader
/// that silently yields nothing would turn every zero-expectation assert
/// vacuous. Mutant: replace the column panic with a skip → RED.
#[test]
#[should_panic(expected = "__op column present")]
fn oracle_read_cdc_changes_panics_on_a_missing_op_column() {
    let d = tempfile::tempdir().unwrap();
    write_cdc_part_without_op(&d.path().join("pa.parquet"), &[1]);
    std::fs::write(
        d.path().join("manifest-run.json"),
        manifest_body("success", &[("pa.parquet", "committed")]),
    )
    .unwrap();
    let _ = read_cdc_changes(d.path());
}

// ── Rig::read_declared_parts: the rig-side resolver twin ─────────────────────

/// PAIR: the rig resolver carries the same success-only rule as the shared
/// one — two resolvers that disagree about a bug are worse than either
/// answer. Mutant: remove the rig-side manifest-status filter → RED.
#[test]
fn oracle_rig_read_declared_parts_skips_a_failed_manifests_parts() {
    let d = tempfile::tempdir().unwrap();
    write_ids_parquet(&d.path().join("pa.parquet"), &[1]);
    write_ids_parquet(&d.path().join("pb.parquet"), &[2]);
    std::fs::write(
        d.path().join("manifest-runA.json"),
        manifest_body("failed", &[("pa.parquet", "committed")]),
    )
    .unwrap();
    std::fs::write(
        d.path().join("manifest-runB.json"),
        manifest_body("success", &[("pb.parquet", "committed")]),
    )
    .unwrap();
    let rig = Rig::pg_batch("t").dest_path(d.path().to_path_buf());
    let rows: usize = rig.read_declared_parts().iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        rows, 1,
        "only the success manifest's single-row part is delivered"
    );
}

// ── duckdb legs: the independent-codec resolvers (container required) ────────

/// PAIR twin of the failed-manifest world through the DuckDB leg end-to-end
/// (staging + container read). Mutant: success-only revert → RED.
#[test]
#[ignore = "live: requires docker compose duckdb"]
fn oracle_duckdb_declared_id_set_sees_only_success_manifest_parts() {
    let d = tempfile::tempdir().unwrap();
    write_ids_parquet(&d.path().join("pa.parquet"), &[1]);
    write_ids_parquet(&d.path().join("pb.parquet"), &[2]);
    std::fs::write(
        d.path().join("manifest-runA.json"),
        manifest_body("failed", &[("pa.parquet", "committed")]),
    )
    .unwrap();
    std::fs::write(
        d.path().join("manifest-runB.json"),
        manifest_body("success", &[("pb.parquet", "committed")]),
    )
    .unwrap();
    let ids = duckdb_declared_dir_id_set(d.path());
    assert_eq!(ids.into_iter().collect::<Vec<_>>(), vec![2]);
}

/// PAIR (the exact 2026-08-29 regression, pinned): a TEXT `id` column must
/// PANIC, never silently become an empty set — the first cut of this helper
/// `unwrap_or_default()`ed a shape mismatch into "zero rows" and a
/// completeness oracle that invents zero is the absence-is-not-success class.
/// Mutant: swap the panic back to a filter_map → RED (should_panic fails).
#[test]
#[ignore = "live: requires docker compose duckdb"]
#[should_panic(expected = "unparseable id cell")]
fn oracle_duckdb_declared_id_set_panics_on_a_text_id_column() {
    let d = tempfile::tempdir().unwrap();
    write_text_id_parquet(&d.path().join("pa.parquet"), &["not-a-number"]);
    std::fs::write(
        d.path().join("manifest-run.json"),
        manifest_body("success", &[("pa.parquet", "committed")]),
    )
    .unwrap();
    let _ = duckdb_declared_dir_id_set(d.path());
}

/// PAIR: a dir with NO manifests stages nothing, and a scalar over nothing
/// must panic (the read_parquet glob matches no files), never answer 0 — the
/// vacuous-[] class one leg over from the cloud refusal.
#[test]
#[ignore = "live: requires docker compose duckdb"]
// The expectation names DuckDB's OWN message about the empty glob, not the
// exec wrapper's "python exec failed": that wrapper string is produced by ANY
// non-zero docker exec, so a critic proved both this pair and the corrupt-part
// twin below pass green with the duckdb container ABSENT — the
// fixture-fails-for-a-second-reason class, inside the ledger built to refuse it.
#[should_panic(expected = "No files found")]
fn oracle_duckdb_declared_scalar_panics_on_a_manifestless_dir() {
    let d = tempfile::tempdir().unwrap();
    write_ids_parquet(&d.path().join("orphan.parquet"), &[1]);
    let _ = duckdb_declared_dir_scalar(d.path(), "count(*)");
}

/// PAIR (the truncated-part class): a DECLARED part whose bytes are garbage
/// must error loudly through the DuckDB leg — a resolver that silently skips
/// a corrupt part under-counts, which is the silent arm the audit flagged on
/// the python resolver.
#[test]
#[ignore = "live: requires docker compose duckdb"]
// Same reason as the twin above: name the PARQUET decode failure, not the exec
// wrapper — otherwise "the container is down" grades as "the oracle refused".
#[should_panic(expected = "Invalid Input Error")]
fn oracle_duckdb_declared_scalar_errors_loudly_on_a_corrupt_declared_part() {
    let d = tempfile::tempdir().unwrap();
    write_ids_parquet(&d.path().join("pa.parquet"), &[1]);
    std::fs::write(d.path().join("pbad.parquet"), b"NOTPARQUET").unwrap();
    std::fs::write(
        d.path().join("manifest-run.json"),
        manifest_body(
            "success",
            &[("pa.parquet", "committed"), ("pbad.parquet", "committed")],
        ),
    )
    .unwrap();
    let _ = duckdb_declared_dir_scalar(d.path(), "count(*)");
}

// ── fake-gcs leg ─────────────────────────────────────────────────────────────

/// PAIR (the wrong-prefix class, measured 2026-08-29): a prefix that matches
/// NO objects at all answered an honest-but-wrong 0 — on the GCS all-features
/// test the probed prefix was missing the export-name segment and the oracle
/// graded the fixture, not the capture. An empty LISTING is a harness bug
/// (wrong prefix / wrong bucket), never evidence of zero rows.
#[test]
#[ignore = "live: requires docker compose fake-gcs"]
#[should_panic(expected = "matched no objects")]
fn oracle_fake_gcs_total_rows_panics_on_an_unmatched_prefix() {
    ensure_gcs_bucket("rivet-qa-oracle-sens");
    let _ = fake_gcs_parquet_total_rows("rivet-qa-oracle-sens", "no_such_prefix_ever");
}
