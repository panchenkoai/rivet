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
    let export_name = unique_name("roast_validate");
    let rig = Rig::pg_batch(&export_name)
        .query(&format!(
            "SELECT id, name, amount, created_at FROM {} ORDER BY id",
            table.name()
        ))
        .export_line("compression: zstd")
        .dest_path(out_dir.path().to_path_buf());

    let run_out = rig.run_args(&["--export", &export_name]);
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
    let healthy = rig.cli(&["validate", "--export", &export_name]);
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
        skip_live(
            "skipping roast_validate_exits_nonzero_when_manifest_unreadable: \
             euid 0 ignores file modes",
        );
        return;
    }

    // ── 4. The gate under test ────────────────────────────────────────────
    let degraded = rig.cli(&["validate", "--export", &export_name]);
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

/// A CSV part that lost a row (same byte size, so size/presence still pass) fails `--depth full`.
#[test]
#[ignore = "live: postgres"]
fn validate_full_depth_fails_a_csv_part_that_lost_a_row() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(10);
    let out_dir = tempfile::tempdir().unwrap();
    let export_name = unique_name("csv_rowloss");
    let rig = Rig::pg_batch(&export_name)
        .query(&format!(
            "SELECT id, name FROM {} ORDER BY id",
            table.name()
        ))
        .with_format("csv")
        .dest_path(out_dir.path().to_path_buf());
    let run_out = rig.run_args(&["--export", &export_name]);
    assert!(
        run_out.status.success(),
        "setup: rivet run failed; stderr:\n{}",
        String::from_utf8_lossy(&run_out.stderr)
    );

    let healthy = rig.cli(&["validate", "--export", &export_name, "--depth", "full"]);
    let healthy_out = String::from_utf8_lossy(&healthy.stdout);
    assert!(
        healthy.status.success(),
        "intact CSV must validate:\n{healthy_out}"
    );
    assert!(
        healthy_out.contains(
            "warning:   [RIVET_VERIFY_VALUE_CHECK_NOT_AVAILABLE] cell values were not re-read: \
             csv parts carry no value checksum (only parquet does); each part's row count was \
             re-counted instead"
        ),
        "full depth must say the CSV value check did not run:\n{healthy_out}"
    );

    let part = std::fs::read_dir(out_dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|x| x == "csv"))
        .expect("a csv part");
    let body = std::fs::read_to_string(&part).unwrap();
    let damaged = body.replacen("\n9,row_9", " 9,row_9", 1);
    assert_ne!(
        body, damaged,
        "fixture: the last row must be present to merge"
    );
    assert_eq!(body.len(), damaged.len());
    std::fs::write(&part, damaged).unwrap();

    let degraded = rig.cli(&["validate", "--export", &export_name, "--depth", "full"]);
    let out = String::from_utf8_lossy(&degraded.stdout);
    assert!(
        !degraded.status.success(),
        "a CSV part missing a row must not pass:\n{out}"
    );
    assert!(
        out.contains("[RIVET_VERIFY_PART_ROW_COUNT]")
            && out.contains("declares 10 rows but holds 9 CSV records after the header"),
        "{out}"
    );
}

/// A prefix whose last run failed (manifest status `failed`) does not validate.
#[test]
#[ignore = "live: postgres"]
fn validate_fails_a_prefix_whose_last_run_failed() {
    require_alive(LiveService::Postgres);
    let out_dir = tempfile::tempdir().unwrap();
    let export_name = unique_name("failed_run");
    let rig = Rig::pg_batch(&export_name)
        .query("SELECT id FROM rivet_no_such_table_r3")
        .dest_path(out_dir.path().to_path_buf());
    let run_out = rig.run_args(&["--export", &export_name]);
    assert!(!run_out.status.success(), "setup: the run must fail");
    let manifest = std::fs::read_to_string(out_dir.path().join("manifest.json"))
        .expect("setup: a failed run writes its manifest");
    assert!(manifest.contains("\"status\": \"failed\""), "{manifest}");

    let v = rig.cli(&["validate", "--export", &export_name]);
    let out = String::from_utf8_lossy(&v.stdout);
    assert_eq!(
        v.status.code(),
        Some(1),
        "a failed run must not validate:\n{out}"
    );
    assert!(out.contains("status:    FAILED"), "{out}");
    assert!(
        out.contains(
            "failure:   [RIVET_VERIFY_RUN_NOT_SUCCESSFUL] the manifest records its last run as \
             failed, not success: this prefix does not hold a completed export. Re-run the export."
        ),
        "{out}"
    );
}
