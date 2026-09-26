//! Gate findings: an enforced-TLS connect against a server that cannot satisfy it is a
//! CONFIG error — permanent (never exit 2), fast, and reported as TLS first.

use std::time::{Duration, Instant};

use crate::common::*;

/// Run the rig and return (exit code, stderr, wall time).
fn run_timed(rig: &Rig) -> (Option<i32>, String, Duration) {
    let t = Instant::now();
    let out = rig.run_args(&[]);
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
        t.elapsed(),
    )
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn mongo_verify_full_against_a_plaintext_server_is_permanent_not_retried() {
    require_alive(LiveService::Mongo);
    let rig = Rig::mongo_batch(&unique_name("tls_probe"))
        .source_url("mongodb://127.0.0.1:27017/rivet")
        .source_line("tls: { mode: verify-full }");
    let (code, stderr, took) = run_timed(&rig);
    assert!(
        code.is_some_and(|c| c != 0 && c != 2),
        "a TLS handshake failure is a config error: exit must be non-zero and NOT the \
         retryable 2, got {code:?}; stderr:\n{stderr}"
    );
    assert!(
        stderr.contains("TLS handshake with 127.0.0.1:27017 failed"),
        "the TLS cause must be named; stderr:\n{stderr}"
    );
    assert!(
        !stderr.contains("will retry") && !stderr.contains("no answer from"),
        "no retry and no host/firewall hint for a TLS failure; stderr:\n{stderr}"
    );
    // Each connect pays the driver's 30 s server-selection timeout and a run opens three
    // (harm probe, open forensics, the export): ~90 s without retries, ~194 s with them.
    assert!(
        took < Duration::from_secs(120),
        "a permanent failure must not ride out the retry budget: took {took:?}"
    );
}

/// The last `Error:` line rivet prints for a failed run.
fn error_line(stderr: &str) -> String {
    stderr
        .lines()
        .rev()
        .find(|l| l.starts_with("Error: "))
        .unwrap_or_else(|| panic!("no `Error:` line; stderr:\n{stderr}"))
        .to_string()
}

#[test]
#[ignore = "live: requires docker compose up -d postgres-cdc"]
fn pg_cdc_tls_failure_is_not_buried_behind_the_setup_hint() {
    let rig = Rig::pg_cdc("public.tls_probe", &unique_name("tls_slot"))
        .source_line("tls: { mode: verify-full }");
    let (code, stderr, _) = run_timed(&rig);
    assert_ne!(code, Some(0), "stderr:\n{stderr}");
    assert_eq!(
        error_line(&stderr),
        format!(
            "Error: {TLS_VERDICT}: error performing TLS handshake: server does not support TLS"
        )
    );
}

const TLS_VERDICT: &str = "TLS handshake failed — the server does not speak TLS or its \
     certificate is not trusted: set `tls.ca_file` for a private CA, or `tls.mode: disable` if \
     the server has no TLS (trusted networks only); retrying will not help";

#[test]
#[ignore = "live: requires docker compose up -d mysql-cdc"]
fn mysql_cdc_tls_failure_is_not_buried_behind_the_setup_hint() {
    let rig = Rig::mysql_cdc("tls_probe").source_line("tls: { mode: verify-full }");
    let (code, stderr, _) = run_timed(&rig);
    assert_ne!(code, Some(0), "stderr:\n{stderr}");
    // The stand's MySQL speaks TLS with a self-signed certificate, so verify-full fails on
    // the certificate; the TLS backend's own reason after `TlsError {` differs by platform.
    let line = error_line(&stderr);
    assert!(
        line.starts_with(&format!("Error: {TLS_VERDICT}: TlsError {{ ")),
        "{line}"
    );
}
