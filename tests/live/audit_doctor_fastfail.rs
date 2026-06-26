//! Live audit test for L20 (cluster: cloud-fastfail).
//!
//! `rivet doctor` write-probes each export destination to verify connectivity
//! and write access (`check_destination_auth` in `src/preflight/doctor.rs`).
//! Before the fix the probe inherited the export-path `RetryLayer` (five
//! attempts, escalating backoff up to ten seconds), so a probe against an
//! unreachable cloud endpoint blocked doctor for roughly ten seconds with
//! OpenDAL transient-retry WARN lines before the `[FAIL]`. A preflight
//! connectivity probe must fail fast.
//!
//! Correct behavior: the destination probe is built with retries disabled
//! (`create_destination_for_probe`, max_times of zero), so a closed loopback
//! port surfaces immediately. This drives the real `rivet` binary against an
//! S3 endpoint on a closed loopback port (`http://127.0.0.1:9`) and asserts
//! doctor returns non-zero in well under the old retry storm.
//!
//! No docker required: the source uses an unset `url_env` (so the source
//! check fails fast, offline, via `resolve_url`) and the destination is a
//! closed loopback port (no MinIO / fake-gcs needed).

use crate::common::*;

use std::time::{Duration, Instant};

// AUDIT cloud-fastfail (L20): doctor's destination probe must not inherit the
// export's 5-attempt escalating-backoff RetryLayer. Against a closed cloud
// endpoint the probe must fail fast (single attempt), not after ~10s of
// retries. Asserts CORRECT behavior; RED before the no-retry probe seam.
#[test]
#[ignore = "live: none"]
fn audit_doctor_fastfails_against_unreachable_cloud_endpoint() {
    // Port 9 (discard) is closed on loopback in this environment → an
    // immediate connection-refused, with no docker dependency.
    let s3_endpoint = "http://127.0.0.1:9";

    // Unset source env var: doctor's source check fails fast (offline) via
    // `resolve_url`, then continues to the destination probe loop, which is
    // the behavior under test. A unique name keeps this race-free under
    // `--test-threads=N`.
    let unset_url_env = unique_name("RIVET_FASTFAIL_UNSET_URL_ENV").to_uppercase();
    // Defensive: ensure it really is unset (a stray export must not pre-pass
    // the source check and change which branches doctor walks).
    // SAFETY: test-only; the var is uniquely named so no other thread touches it.
    unsafe { std::env::remove_var(&unset_url_env) };

    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url_env: {url_env}

exports:
  - name: fastfail
    query: "SELECT 1"
    format: parquet
    destination:
      type: s3
      bucket: rivet-fastfail-probe
      endpoint: "{endpoint}"
      region: us-east-1
      access_key_env: RIVET_FASTFAIL_AK
      secret_key_env: RIVET_FASTFAIL_SK
"#,
        url_env = unset_url_env,
        endpoint = s3_endpoint,
    );
    let cfg = write_config(&cfg_dir, &yaml);

    // Provide dummy S3 creds so the probe reaches the *network* (a missing
    // access_key_env would short-circuit in build_operator with an env error
    // and never exercise the RetryLayer — the very thing under test).
    // SAFETY: test-only; uniquely scoped, removed below.
    unsafe {
        std::env::set_var("RIVET_FASTFAIL_AK", "AKIAFASTFAILTEST");
        std::env::set_var("RIVET_FASTFAIL_SK", "fastfail-secret");
    }

    let start = Instant::now();
    let result = std::process::Command::new(RIVET_BIN)
        .args(["doctor", "-c", cfg.to_str().unwrap()])
        .output()
        .expect("spawn rivet doctor");
    let elapsed = start.elapsed();

    unsafe {
        std::env::remove_var("RIVET_FASTFAIL_AK");
        std::env::remove_var("RIVET_FASTFAIL_SK");
    }

    let stdout = String::from_utf8_lossy(&result.stdout);
    let stderr = String::from_utf8_lossy(&result.stderr);

    // Doctor must exit non-zero: the source check fails (unset env) and the
    // destination probe fails (closed port) — either alone is enough.
    assert!(
        !result.status.success(),
        "doctor must exit non-zero when the destination is unreachable; \
         stdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // FAIL FAST: the old behavior was ~10s of 5-attempt escalating backoff.
    // A single-attempt probe against a closed loopback port returns in
    // milliseconds. 4s is a generous ceiling that still proves the retry
    // storm is gone (binary spawn overhead is well under a second).
    assert!(
        elapsed < Duration::from_secs(4),
        "doctor took {elapsed:?} probing an unreachable cloud endpoint — the \
         preflight probe must FAIL FAST (no inherited 5-attempt RetryLayer); \
         stdout:\n{stdout}\nstderr:\n{stderr}"
    );
}
