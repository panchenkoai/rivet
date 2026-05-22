//! Regression tests for `rivet validate` historical-prefix flags
//! (`--date`, `--run-id`, `--prefix`) introduced in v0.7.2.
//!
//! Anchor scenario (roadmap §0.7.2 P0.1):
//!
//! ```text
//! run happened yesterday
//! validate runs today
//! validate still checks the correct physical prefix when given --date
//! ```
//!
//! The historical flags only affect destination-prefix resolution.  Source
//! connectivity is never opened by `rivet validate`, so these tests use a
//! placeholder source URL that nobody dials.

#![allow(clippy::needless_borrow)]

use std::path::Path;

use chrono::{Duration, NaiveDate, Utc};
use rivet::config::{DestinationConfig, DestinationType};
use rivet::manifest::{
    MANIFEST_VERSION, ManifestDestination, ManifestPart, ManifestSource, ManifestStatus,
    RunManifest,
};
use rivet::pipeline::{ValidateOutputFormat, ValidateTarget, run_validate_command, write_manifest};

// ── Fixtures ──────────────────────────────────────────────────────────────────

/// Build a minimal manifest with no committed parts.  The verifier's
/// per-part head-check pass becomes a no-op so the test focuses on
/// "did we find manifest.json at the right prefix" — which is what the
/// historical-prefix flags affect.
fn empty_success_manifest(run_id: &str, export_name: &str) -> RunManifest {
    RunManifest {
        manifest_version: MANIFEST_VERSION,
        run_id: run_id.into(),
        export_name: export_name.into(),
        started_at: "2026-05-20T12:00:00Z".into(),
        finished_at: "2026-05-20T12:14:33Z".into(),
        status: ManifestStatus::Success,
        source: ManifestSource {
            engine: "postgres".into(),
            schema: Some("public".into()),
            table: Some(export_name.into()),
        },
        destination: ManifestDestination {
            kind: "local".into(),
            uri: "file:///tmp/out/".into(),
        },
        format: "parquet".into(),
        compression: "zstd".into(),
        schema_fingerprint: "xxh3:0123456789abcdef".into(),
        row_count: 0,
        part_count: 0,
        parts: Vec::<ManifestPart>::new(),
    }
}

/// Land a `manifest.json` + `_SUCCESS` at the given on-disk prefix via the
/// public destination + manifest writer surface.  Mirrors the path the
/// `rivet run` end-of-run writer would have taken on `prefix_dir`.
fn land_manifest(prefix_dir: &Path, run_id: &str, export_name: &str) {
    std::fs::create_dir_all(prefix_dir).expect("create prefix dir");
    let dest_cfg = DestinationConfig {
        destination_type: DestinationType::Local,
        path: Some(prefix_dir.to_string_lossy().into_owned()),
        ..Default::default()
    };
    let dest = rivet::destination_for_tests::create_destination(&dest_cfg)
        .expect("create local destination");
    let m = empty_success_manifest(run_id, export_name);
    write_manifest(&*dest, &m).expect("write manifest");
}

/// Write a minimal YAML config that points at `<base>/{date}/{export}` as
/// the local destination.  `rivet validate` resolves `{date}` against the
/// `ValidateTarget` we pass.
fn write_config(base: &Path, export_name: &str) -> std::path::PathBuf {
    let cfg = base.join("rivet.yaml");
    let path_template = format!("{}/{{date}}/{{export}}", base.to_string_lossy());
    let yaml = format!(
        "source:\n  type: postgres\n  url: postgresql://nobody@localhost/nope\nexports:\n  - name: {export_name}\n    query: \"SELECT 1\"\n    mode: full\n    format: parquet\n    destination:\n      type: local\n      path: \"{path_template}\"\n"
    );
    std::fs::write(&cfg, yaml).expect("write rivet.yaml");
    cfg
}

fn yesterday_utc() -> NaiveDate {
    (Utc::now() - Duration::days(1)).date_naive()
}

fn read_json(path: &Path) -> serde_json::Value {
    let raw = std::fs::read_to_string(path).expect("read json report");
    serde_json::from_str(&raw).expect("parse json report")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[test]
fn validate_today_misses_yesterdays_prefix() {
    // Baseline: without --date, validate looks at today's resolved prefix.
    // Yesterday's manifest is invisible — `manifest_found` is false.  This
    // is the very gap --date / --run-id are designed to close, so the
    // baseline must be a hard "no manifest" result; a future change that
    // accidentally points validate at "any matching prefix" would flip
    // this test red.
    let dir = tempfile::tempdir().unwrap();
    let yesterday = yesterday_utc();
    let yesterday_str = yesterday.format("%Y-%m-%d").to_string();
    let prefix = dir.path().join(&yesterday_str).join("orders");
    land_manifest(&prefix, "r-historical-1", "orders");

    let cfg = write_config(dir.path(), "orders");
    let out = dir.path().join("validate_today.json");

    run_validate_command(
        cfg.to_str().unwrap(),
        Some("orders"),
        ValidateOutputFormat::Json(Some(out.to_string_lossy().into_owned())),
        ValidateTarget::default(),
    )
    .expect("validate without --date returns Ok when no manifest is found");

    let json = read_json(&out);
    let export = &json["exports"][0];
    assert_eq!(export["export_name"], "orders");
    assert_eq!(
        export["verification"]["manifest_found"], false,
        "today's prefix should have no manifest"
    );
    let resolved = export["resolved_prefix"].as_str().unwrap();
    let today_str = Utc::now().date_naive().format("%Y-%m-%d").to_string();
    assert!(
        resolved.contains(&today_str),
        "resolved_prefix should mention today's date ({today_str}), got: {resolved}"
    );
}

#[test]
fn validate_with_date_finds_yesterdays_manifest() {
    // Regression scenario from the v0.7.2 roadmap:
    //   "run happened yesterday, validate runs today,
    //    validate still checks the correct physical prefix"
    let dir = tempfile::tempdir().unwrap();
    let yesterday = yesterday_utc();
    let yesterday_str = yesterday.format("%Y-%m-%d").to_string();
    let prefix = dir.path().join(&yesterday_str).join("orders");
    land_manifest(&prefix, "r-historical-2", "orders");

    let cfg = write_config(dir.path(), "orders");
    let out = dir.path().join("validate_yesterday.json");

    run_validate_command(
        cfg.to_str().unwrap(),
        Some("orders"),
        ValidateOutputFormat::Json(Some(out.to_string_lossy().into_owned())),
        ValidateTarget {
            date: Some(yesterday),
            ..Default::default()
        },
    )
    .expect("validate --date yesterday must succeed");

    let json = read_json(&out);
    let export = &json["exports"][0];
    assert_eq!(export["verification"]["manifest_found"], true);
    assert_eq!(export["verification"]["passed"], true);
    let resolved = export["resolved_prefix"].as_str().unwrap();
    assert!(
        resolved.contains(&yesterday_str),
        "resolved_prefix should mention yesterday ({yesterday_str}), got: {resolved}"
    );
}

#[test]
fn validate_with_prefix_override_bypasses_placeholder_resolution() {
    // `--prefix` is the escape hatch: data lives at a literal location
    // that no longer matches the config template (relocated, renamed,
    // template changed since landing).  Validate should head-check
    // exactly that prefix, ignoring `{date}` etc.
    let dir = tempfile::tempdir().unwrap();
    let custom = dir.path().join("relocated").join("orders");
    land_manifest(&custom, "r-historical-3", "orders");

    let cfg = write_config(dir.path(), "orders");
    let out = dir.path().join("validate_prefix.json");

    run_validate_command(
        cfg.to_str().unwrap(),
        Some("orders"),
        ValidateOutputFormat::Json(Some(out.to_string_lossy().into_owned())),
        ValidateTarget {
            prefix_override: Some(custom.to_string_lossy().into_owned()),
            ..Default::default()
        },
    )
    .expect("validate --prefix must succeed when manifest is at that prefix");

    let json = read_json(&out);
    let export = &json["exports"][0];
    assert_eq!(export["verification"]["manifest_found"], true);
    let resolved = export["resolved_prefix"].as_str().unwrap();
    assert_eq!(
        resolved,
        custom.to_string_lossy(),
        "resolved_prefix must match the --prefix override verbatim"
    );
}

#[test]
fn validate_prefix_requires_single_export() {
    // Refuse to apply one literal prefix across multiple exports — that
    // would silently point every verification at the same bytes.  The
    // operator must scope with `--export <name>`.
    let dir = tempfile::tempdir().unwrap();
    let cfg = dir.path().join("rivet.yaml");
    let path_template = format!("{}/{{date}}/{{export}}", dir.path().to_string_lossy());
    let yaml = format!(
        "source:\n  type: postgres\n  url: postgresql://nobody@localhost/nope\nexports:\n  - name: orders\n    query: \"SELECT 1\"\n    mode: full\n    format: parquet\n    destination:\n      type: local\n      path: \"{path_template}\"\n  - name: users\n    query: \"SELECT 1\"\n    mode: full\n    format: parquet\n    destination:\n      type: local\n      path: \"{path_template}\"\n"
    );
    std::fs::write(&cfg, yaml).unwrap();

    let err = run_validate_command(
        cfg.to_str().unwrap(),
        None,
        ValidateOutputFormat::Json(None),
        ValidateTarget {
            prefix_override: Some("/nowhere".into()),
            ..Default::default()
        },
    )
    .expect_err("multi-export + --prefix must be rejected");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("--prefix requires --export"),
        "error must explain the scoping rule, got: {msg}",
    );
}
