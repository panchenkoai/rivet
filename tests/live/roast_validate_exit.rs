//! RED test for the `rivet validate` exit gate (cluster: live-validate-exit).
//!
//! `rivet validate`'s doc contract (src/pipeline/validate_cmd.rs header)
//! promises a non-zero exit on **any explicit failure**.  But the exit gate
//! is `manifest_found && !passed`, and `verify_at_destination` reports a
//! manifest read I/O error as `{manifest_found: false, passed: false,
//! failures: [ManifestReadError]}` — so an UNREADABLE manifest exits 0 and a
//! CI gate `rivet validate && deploy` sails past a dead destination.
//!
//! Scenario: successful PG → local export (manifest.json written), then the
//! manifest is made unreadable (`chmod 000`), then `rivet validate` runs.
//! Correct behavior: exit code != 0.  Current behavior: exit code 0.

use std::os::unix::fs::PermissionsExt;

use crate::common::*;

/// Restore the original permission bits on drop so cleanup (tempdir removal,
/// post-mortem inspection) works even when an assertion panics mid-test.
struct RestorePerms {
    path: std::path::PathBuf,
    mode: u32,
}

impl Drop for RestorePerms {
    fn drop(&mut self) {
        let _ = std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(self.mode));
    }
}

// ROAST-RED live-validate-exit: `rivet validate` exits 0 when manifest.json
// exists but cannot be read (ManifestReadError), because the exit gate is
// `manifest_found && !passed` and the read-error verdict carries
// `manifest_found: false`.
// Asserts CORRECT behavior; expected to FAIL until the fix lands.
#[test]
#[ignore = "live: postgres"]
fn roast_validate_exits_nonzero_when_manifest_unreadable() {
    require_alive(LiveService::Postgres);

    // ── 1. Small successful export to a local destination ────────────────
    // `rivet run` writes manifest.json (+ _SUCCESS) at end-of-run for every
    // non-streaming destination (finalize_manifest), no extra config needed.
    let table = seed_pg_numeric_table(10);
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let export_name = unique_name("roast_validate");
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {export_name}
    query: "SELECT id, name, amount, created_at FROM {table_name} ORDER BY id"
    mode: full
    format: parquet
    compression: zstd
    destination:
      type: local
      path: {out_path}
"#,
        table_name = table.name(),
        out_path = out_dir.path().display(),
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    let run_out = run_rivet_export(&cfg_path, &export_name);
    assert!(
        run_out.status.success(),
        "setup: rivet run failed (exit {:?}); stderr:\n{}",
        run_out.status.code(),
        String::from_utf8_lossy(&run_out.stderr),
    );

    let manifest_path = out_dir.path().join("manifest.json");
    assert!(
        manifest_path.exists(),
        "setup: rivet run must have written manifest.json at {}",
        manifest_path.display(),
    );

    // ── 2. Baseline: validate on the intact dataset exits 0 ──────────────
    // Pins that any non-zero exit below comes from the degraded manifest,
    // not from an unrelated config/destination problem.
    let healthy = run_rivet(&[
        "validate",
        "--config",
        cfg_path.to_str().unwrap(),
        "--export",
        &export_name,
    ]);
    assert!(
        healthy.status.success(),
        "setup: validate on the intact dataset must exit 0 (exit {:?}); stderr:\n{}",
        healthy.status.code(),
        String::from_utf8_lossy(&healthy.stderr),
    );

    // ── 3. Degrade: manifest present but unreadable ───────────────────────
    // head() (fs::metadata) still succeeds, read() (fs::read) hits EACCES —
    // exactly the ManifestReadError branch in verify_at_destination.
    let orig_mode = std::fs::metadata(&manifest_path)
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    let _restore = RestorePerms {
        path: manifest_path.clone(),
        mode: orig_mode,
    };
    std::fs::set_permissions(&manifest_path, std::fs::Permissions::from_mode(0o000))
        .expect("chmod manifest.json to 000");

    if std::fs::read(&manifest_path).is_ok() {
        // Running as root: mode 000 does not block reads, so the degraded
        // state cannot be staged.  Skip rather than report a false verdict.
        eprintln!(
            "skipping roast_validate_exits_nonzero_when_manifest_unreadable: \
             euid 0 ignores file modes"
        );
        return;
    }

    // ── 4. The gate under test ────────────────────────────────────────────
    let degraded = run_rivet(&[
        "validate",
        "--config",
        cfg_path.to_str().unwrap(),
        "--export",
        &export_name,
    ]);
    assert!(
        !degraded.status.success(),
        "rivet validate exited {:?} although manifest.json was unreadable \
         (ManifestReadError is an explicit failure): a CI gate \
         `rivet validate && deploy` would sail past a dead destination. \
         The exit-code policy must count read-error verdicts \
         (has_failures), not only `manifest_found && !passed`.\n\
         stdout:\n{}\nstderr:\n{}",
        degraded.status.code(),
        String::from_utf8_lossy(&degraded.stdout),
        String::from_utf8_lossy(&degraded.stderr),
    );
}
