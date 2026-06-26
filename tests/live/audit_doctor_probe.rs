//! Live audit test for FINDING #26 (cluster: doctor-probe-cleanup).
//!
//! `rivet doctor` write-probes the destination to verify write access by
//! dropping a `.rivet_doctor_probe` object at the destination prefix
//! (`check_destination_auth` in `src/preflight/doctor.rs`). It then only
//! removes the *local* temp file it copied from — the probe object left at
//! the destination (local dir / S3 / GCS) is never deleted. The result is a
//! stray 2-byte `.rivet_doctor_probe` file polluting the export prefix.
//!
//! Correct behavior: doctor must clean up the destination-side probe it
//! created, leaving the prefix exactly as it found it.
//!
//! This file exercises a local destination (enough to prove the root cause;
//! S3/GCS share the same `check_destination_auth` code path). It drives the
//! real `rivet` binary against a live, reachable Postgres source so that
//! `doctor` proceeds to the destination write-probe and exits 0.

use crate::common::*;

// AUDIT-RED doctor-probe-cleanup: `rivet doctor` write-probes the destination with a
// `.rivet_doctor_probe` object but only deletes the local temp, leaving the probe at
// the destination prefix. Asserts CORRECT behavior; expected to FAIL until fixed.
#[test]
#[ignore = "live: postgres"]
fn audit_doctor_removes_destination_probe_after_local_write_check() {
    require_alive(LiveService::Postgres);

    // Reachable Postgres source so source auth passes and doctor reaches the
    // destination write-probe loop.
    let table = seed_pg_numeric_table(5);

    // A fresh, empty destination directory: anything found here afterward was
    // put there by doctor.
    let dest = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: {name}
    query: "SELECT id, name FROM {name}"
    mode: full
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        name = table.name(),
        out = dest.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let result = std::process::Command::new(RIVET_BIN)
        .args(["doctor", "-c", cfg.to_str().unwrap()])
        .output()
        .expect("spawn rivet doctor");

    // Doctor must pass (source reachable, destination writable) — this is the
    // happy path where the probe write succeeds and therefore must be cleaned.
    assert!(
        result.status.success(),
        "doctor must exit 0 when source is reachable and dest is writable; stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );

    // Capture the full destination listing for a useful failure message.
    let probe = ".rivet_doctor_probe";
    let entries: Vec<String> = std::fs::read_dir(dest.path())
        .expect("read destination dir after doctor")
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();

    let probe_path = dest.path().join(probe);
    assert!(
        !probe_path.exists(),
        "doctor left its write-probe `{probe}` at the destination prefix {} \
         (it only removes the local temp, never the destination object); \
         dir listing after doctor: {entries:?}",
        dest.path().display()
    );

    // Strengthen the assertion: the destination must be exactly as doctor
    // found it — empty. Any stray sidecar (e.g. a `.rivet_doctor_probe.tmp`
    // staging file from a partial cleanup) is also pollution.
    assert!(
        entries.is_empty(),
        "doctor must leave the destination prefix {} exactly as it found it (empty); \
         found stray entries: {entries:?}",
        dest.path().display()
    );
}
