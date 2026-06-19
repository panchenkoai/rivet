//! Live regression for audit L14: `rivet validate` must not label an advisory
//! (non-fatal) entry as a "failure:" while exiting 0.
//!
//! An untracked surplus object (`UntrackedObject`) is advisory — it never flips
//! `passed` and never changes the exit code (cleanup is `--resume`'s job, M9).
//! The pretty renderer used to print it under the same "failure:" label as a
//! hard missing-part failure, so an operator saw "failure: …" next to a clean
//! exit 0 — a self-contradiction. The fix relabels advisory entries "warning:".
//!
//! No source is dialed by `rivet validate`, so this builds a local dataset
//! in-process (manifest + _SUCCESS + a real part) and drives the binary; no
//! database is required.

use std::path::Path;

use rivet::config::{DestinationConfig, DestinationType};
use rivet::manifest::{
    MANIFEST_VERSION, ManifestDestination, ManifestPart, ManifestSource, ManifestStatus,
    PartStatus, RunManifest,
};
use rivet::pipeline::write_manifest;

const RIVET_BIN: &str = env!("CARGO_BIN_EXE_rivet");

/// A success manifest declaring exactly one committed part (`part-000001.parquet`,
/// 4 bytes) — matches the bytes written below so the part verifies and the
/// verdict passes (the surplus is then the only advisory entry).
fn one_part_manifest() -> RunManifest {
    RunManifest {
        manifest_version: MANIFEST_VERSION,
        run_id: "r-l14".into(),
        export_name: "orders".into(),
        started_at: "2026-06-09T12:00:00Z".into(),
        finished_at: "2026-06-09T12:01:00Z".into(),
        status: ManifestStatus::Success,
        source: ManifestSource {
            engine: "postgres".into(),
            schema: Some("public".into()),
            table: Some("orders".into()),
        },
        destination: ManifestDestination {
            kind: "local".into(),
            uri: "file:///tmp/out".into(),
        },
        format: "parquet".into(),
        compression: "zstd".into(),
        schema_fingerprint: "xxh3:0123456789abcdef".into(),
        row_count: 1,
        part_count: 1,
        parts: vec![ManifestPart {
            part_id: 1,
            path: "part-000001.parquet".into(),
            rows: 1,
            size_bytes: 4,
            content_fingerprint: "xxh3:1111111111111111".into(),
            content_md5: String::new(),
            status: PartStatus::Committed,
        }],
    }
}

/// Land manifest.json + _SUCCESS + the declared part at `prefix`.
fn stage_dataset(prefix: &Path, m: &RunManifest) {
    std::fs::create_dir_all(prefix).unwrap();
    std::fs::write(prefix.join("part-000001.parquet"), b"AAAA").unwrap();
    let dest = rivet::destination_for_tests::create_destination(&DestinationConfig {
        destination_type: DestinationType::Local,
        path: Some(prefix.to_string_lossy().into_owned()),
        ..Default::default()
    })
    .unwrap();
    write_manifest(&*dest, m).unwrap();
}

fn write_cfg(dir: &Path, prefix: &Path) -> std::path::PathBuf {
    let cfg = dir.join("rivet.yaml");
    let yaml = format!(
        "source:\n  type: postgres\n  url: postgresql://nobody@localhost/nope\nexports:\n  - name: orders\n    query: \"SELECT 1\"\n    mode: full\n    format: parquet\n    destination:\n      type: local\n      path: \"{}\"\n",
        prefix.to_string_lossy()
    );
    std::fs::write(&cfg, yaml).unwrap();
    cfg
}

#[test]
fn validate_pretty_labels_untracked_surplus_as_warning_not_failure() {
    let dir = tempfile::tempdir().unwrap();
    let prefix = dir.path().join("out");
    stage_dataset(&prefix, &one_part_manifest());
    // A surplus object the manifest does not reference → advisory UntrackedObject.
    std::fs::write(prefix.join("rogue.parquet"), b"XX").unwrap();
    let cfg = write_cfg(dir.path(), &prefix);

    let out = std::process::Command::new(RIVET_BIN)
        .args([
            "validate",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            "orders",
        ])
        .output()
        .expect("spawn rivet validate");

    // Advisory surplus does not change the exit code: clean exit 0.
    assert!(
        out.status.success(),
        "untracked surplus is advisory → exit 0; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let stdout = String::from_utf8_lossy(&out.stdout);
    // The surplus line is present and labelled "warning:", not "failure:".
    assert!(
        stdout.contains("warning:") && stdout.contains("rogue.parquet"),
        "untracked surplus must render under a 'warning:' label; got:\n{stdout}"
    );
    // And it must NOT appear under a "failure:" label (the L14 contradiction).
    let surplus_under_failure = stdout
        .lines()
        .any(|l| l.contains("failure:") && l.contains("untracked"));
    assert!(
        !surplus_under_failure,
        "untracked surplus must NOT render under 'failure:' beside exit 0; got:\n{stdout}"
    );
}
