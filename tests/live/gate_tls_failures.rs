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
