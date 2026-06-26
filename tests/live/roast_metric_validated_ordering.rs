//! ROAST-RED live-metric-ordering: `record_metric` persists `validated`
//! BEFORE `finalize_validate_manifest` can downgrade it.
//!
//! In `run_export_job` (src/pipeline/job.rs:383) the metrics row is written
//! with `summary.validated` as it stands after the per-file row check — but
//! the manifest-aware `--validate` pass (`finalize_validate_manifest`,
//! src/pipeline/finalize.rs:276-278) runs LATER (job.rs:415) and may flip
//! `summary.validated` from `Some(true)` to `Some(false)`.  The run report
//! (`summary.json`), the console summary, and the notification all carry the
//! downgraded verdict; `export_metrics` (what `rivet metrics` reads) is stuck
//! at `validated = pass` forever.  Same ordering bug exists at the apply-path
//! call site (job.rs:510 vs job.rs:537).
//!
//! ## Deterministic staging — no tampering, no fault hooks
//!
//! `verify: content` (ADR-0013, review D) makes parts that are only
//! size-verified a FATAL manifest-verification failure
//! (`Failure::ContentVerificationUnmet` — fatal per `Failure::is_fatal`,
//! enforced via `ManifestVerification::enforce_content_policy`).  A local
//! destination never exposes a store checksum (`src/destination/local.rs`:
//! `content_md5: None` — "local FS exposes no checksum in metadata"), so every
//! part degrades to size-only (`PartPresence::Present { md5_verified: false }`
//! in `manifest_reconcile.rs`).  Therefore a clean PG → local-parquet run with
//! `--validate` and `verify: content`:
//!
//!   1. per-file row check passes → `summary.validated = Some(true)`
//!   2. `record_metric` persists `validated = true`        ← too early (BUG)
//!   3. `finalize_manifest` writes `manifest.json` + `_SUCCESS`
//!   4. `finalize_validate_manifest` → fatal `ContentVerificationUnmet`
//!      → `summary.validated = Some(false)`
//!   5. `finalize_run_report` writes `summary.json` with
//!      `validation.passed = false`
//!
//! The metrics row and the run's own final report now disagree, permanently.

use crate::common::*;

/// Latest `export_metrics` row for the export: `(run_id, validated)`.
/// Mirrors `latest_metric_validated` in tests/live_chunked_recovery.rs, but
/// also pulls `run_id` so the test can open exactly this run's report.
fn latest_metric_row(cfg: &std::path::Path, export: &str) -> (String, Option<bool>) {
    let db = cfg.parent().unwrap().join(".rivet_state.db");
    let conn = rusqlite::Connection::open(db).expect("open state db");
    conn.query_row(
        "SELECT run_id, validated FROM export_metrics \
         WHERE export_name = ?1 ORDER BY id DESC LIMIT 1",
        [export],
        |r| Ok((r.get::<_, Option<String>>(0)?, r.get::<_, Option<bool>>(1)?)),
    )
    .map(|(run_id, validated)| {
        (
            run_id.expect("export_metrics.run_id must be set by record_metric"),
            validated,
        )
    })
    .expect("an export_metrics row must exist after the run")
}

// ROAST-RED live-metric-ordering: record_metric (job.rs:383) persists
// `validated` before finalize_validate_manifest (job.rs:415 →
// finalize.rs:276-278) can downgrade it, so `rivet metrics` permanently shows
// validated=pass for a run whose own summary.json says validation FAILED.
// Asserts CORRECT behavior; expected to FAIL until the fix lands.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn roast_metric_validated_matches_final_summary_verdict() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(50);
    let export = unique_name("roast_metric_order");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    // `verify: content` + local destination (no store checksum) is the
    // deterministic stage: per-file row validation passes mid-run, then the
    // end-of-run manifest pass fails fatally (ContentVerificationUnmet).
    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export}
    query: "SELECT id, name FROM {table_name}"
    mode: full
    format: parquet
    verify: content
    destination: {{type: local, path: {dir}}}
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    // The manifest-verification failure is non-fatal by design (ADR-0001 §I7):
    // the run itself still exits 0.
    let run = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export,
            "--validate",
        ])
        .output()
        .expect("spawn rivet");
    assert!(
        run.status.success(),
        "run must succeed (manifest verification failures are non-fatal); stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );

    let (run_id, metric_validated) = latest_metric_row(&cfg, &export);

    // ── Staging precondition (passes both before and after the fix) ────────
    //
    // The run's own final report must say validation FAILED — `verify:
    // content` on a checksum-less local destination guarantees the fatal
    // ContentVerificationUnmet downgrade.  If THIS fires, the staging broke
    // (e.g. local destinations started exposing checksums), not the ordering.
    let report_path = cfg
        .parent()
        .unwrap()
        .join(".rivet")
        .join("runs")
        .join(&run_id)
        .join("summary.json");
    let report: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(&report_path)
            .unwrap_or_else(|e| panic!("read run report {}: {e}", report_path.display())),
    )
    .expect("summary.json must be valid JSON");
    let report_passed = report["validation"]["passed"]
        .as_bool()
        .expect("summary.json validation.passed must be present for a --validate run");
    assert!(
        !report_passed,
        "staging precondition broke: summary.json says validation passed, but \
         `verify: content` against a local destination (no store checksum) must \
         fail manifest verification with ContentVerificationUnmet"
    );

    // ── The ordering bug ────────────────────────────────────────────────────
    //
    // CORRECT behavior: the persisted metric carries the run's FINAL verdict —
    // the same one summary.json, the console card, and the notification show.
    // Current code wrote `validated = true` at job.rs:383, before
    // finalize_validate_manifest downgraded it, so this assert fails with
    // metric_validated = Some(true).
    assert_eq!(
        metric_validated,
        Some(report_passed),
        "export_metrics.validated says {:?} but the run's final summary.json says \
         validation passed={report_passed} — record_metric persisted `validated` \
         BEFORE finalize_validate_manifest downgraded it (`rivet metrics` will \
         show validated=pass forever for a run that FAILED validation)",
        metric_validated
    );
}
