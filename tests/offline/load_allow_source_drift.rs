//! `load.allow_source_drift` — a run whose source count differs from what it extracted is
//! refused at load time; the flag downgrades that to a warning. Graded over a SECOND run,
//! since a load sums every unloaded run: one clean run and one drifted run.

use rivet::load::reconcile::reconcile;
use rivet::manifest::{
    ExtractionMetadata, MANIFEST_VERSION, ManifestDestination, ManifestPart, ManifestSource,
    ManifestStatus, PartStatus, RunManifest,
};

/// A `Success` manifest with one committed part of `rows` rows and a probed source count.
fn run_manifest(run_id: &str, rows: i64, source_rows: i64) -> RunManifest {
    RunManifest {
        split_window: None,
        checksum_render: None,
        row_hash: None,
        manifest_version: MANIFEST_VERSION,
        run_id: run_id.into(),
        export_name: "orders".into(),
        export_family: String::new(),
        mode: "batch".into(),
        started_at: "2026-09-13T00:00:00Z".into(),
        finished_at: "2026-09-13T00:01:00Z".into(),
        status: ManifestStatus::Success,
        source: ManifestSource {
            engine: "postgres".into(),
            schema: Some("public".into()),
            table: Some("orders".into()),
            extraction: Some(ExtractionMetadata {
                strategy: "full".into(),
                cursor_column: None,
                cursor_type: None,
                cursor_low: None,
                cursor_high: None,
                source_row_count: Some(source_rows),
            }),
        },
        destination: ManifestDestination {
            kind: "gcs".into(),
            uri: "gs://b/orders".into(),
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
fn a_drifted_second_run_is_refused_unless_allow_source_drift_is_set() {
    let clean = run_manifest("run-1", 100, 100);
    let drifted = run_manifest("run-2", 40, 42);

    let err = reconcile(&[clean.clone(), drifted.clone()], false)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("run `run-2`"),
        "the refusal names the drifted run: {err}"
    );
    assert!(err.contains("dropped 2 row(s)"), "{err}");
    assert!(
        err.contains("allow-source-drift"),
        "and the way past it: {err}"
    );

    let got =
        reconcile(&[clean, drifted], true).expect("allow_source_drift downgrades to a warning");
    assert_eq!(
        got.file_rows, 140,
        "the file side is what the load gates on"
    );
    assert_eq!(got.source_rows, Some(142));
    assert_eq!(got.manifests, 2);
}
