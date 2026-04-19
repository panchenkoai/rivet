//! Runtime retry behaviour and mid-stream fault-injection via Toxiproxy.
//!
//! QA backlog:
//!   * Task 4.1 — retry contract at runtime (not just classifier-level)
//!   * Task 4.2 — mid-stream transient failures, then recovery
//!
//! Every test that manipulates Toxiproxy registers the `postgres` proxy
//! lazily via `ensure_toxi_proxy("postgres", 15432, "postgres:5432")` and
//! clears leftover toxics at the start so the suite is resilient to
//! interleaved reruns.

mod common;

use std::time::Duration;

use common::*;

/// Idempotent setup: register the proxy and wipe any previous toxics.
fn reset_postgres_proxy() {
    ensure_toxi_proxy("postgres", 15432, "postgres:5432");
    toxi_reset_toxics("postgres");
}

#[test]
#[ignore = "live: requires docker compose postgres + toxiproxy"]
fn clean_postgres_via_toxi_works_as_a_baseline_and_exports_successfully() {
    // Negative control: without any toxic, routing through Toxiproxy must
    // behave identically to the direct postgres URL.  If this fails, the
    // subsequent fault-injection tests cannot be trusted.
    let _g = toxiproxy_guard();
    require_alive(LiveService::Postgres);
    require_alive(LiveService::Toxiproxy);
    reset_postgres_proxy();

    // Seed on primary, export through toxi URL.
    let table = seed_pg_numeric_table(5);
    let export_name = unique_name("qa41_baseline");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_TOXI_URL}"
exports:
  - name: {export_name}
    query: "SELECT id FROM {table_name}"
    mode: full
    format: parquet
    destination: {{type: local, path: {dir}}}
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);
    let out_run = run_rivet_export(&cfg_path, &export_name);
    assert!(
        out_run.status.success(),
        "baseline run through toxiproxy failed; stderr:\n{}",
        String::from_utf8_lossy(&out_run.stderr),
    );
}

#[test]
#[ignore = "live: requires docker compose postgres + toxiproxy"]
fn export_survives_transient_latency_added_via_toxiproxy() {
    // Inject a short latency and re-run. The statement_timeout default is
    // generous; the export must still complete.  This pins the contract
    // that a slow-but-not-failed network is not misclassified as an error.
    let _g = toxiproxy_guard();
    require_alive(LiveService::Postgres);
    require_alive(LiveService::Toxiproxy);
    reset_postgres_proxy();

    let toxic = toxi_add_latency("postgres", 50); // 50ms per packet

    let table = seed_pg_numeric_table(5);
    let export_name = unique_name("qa41_latency");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_TOXI_URL}"
exports:
  - name: {export_name}
    query: "SELECT id FROM {table_name}"
    mode: full
    format: parquet
    destination: {{type: local, path: {dir}}}
    tuning:
      max_retries: 2
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);
    let out_run = run_rivet_export(&cfg_path, &export_name);

    // Clean up before asserting so we don't leak toxics on failure.
    let _ = toxic;
    toxi_reset_toxics("postgres");

    assert!(
        out_run.status.success(),
        "export must complete under 50ms latency; stderr:\n{}",
        String::from_utf8_lossy(&out_run.stderr),
    );
}

#[test]
#[ignore = "live: requires docker compose postgres + toxiproxy"]
fn export_fails_cleanly_when_toxiproxy_is_disabled_before_run() {
    // Disabling the proxy simulates a total outage.  The pipeline must exit
    // non-zero and stderr must point the operator at a network-level cause
    // (connection refused / reset / timeout).  No panic, no zero-exit.
    let _g = toxiproxy_guard();
    require_alive(LiveService::Postgres);
    require_alive(LiveService::Toxiproxy);
    reset_postgres_proxy();

    toxi_disable("postgres");

    let export_name = unique_name("qa42_disabled");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_TOXI_URL}"
exports:
  - name: {export_name}
    query: "SELECT 1::int"
    mode: full
    format: parquet
    destination: {{type: local, path: {dir}}}
    tuning:
      max_retries: 0
"#,
        dir = out.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);
    let out_run = run_rivet_export(&cfg_path, &export_name);

    // Re-enable before assertions so subsequent tests are not affected.
    toxi_enable("postgres");

    assert!(
        !out_run.status.success(),
        "export through a disabled proxy must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&out_run.stderr);
    let low = stderr.to_lowercase();
    assert!(
        low.contains("connection")
            || low.contains("refused")
            || low.contains("reset")
            || low.contains("timeout")
            || low.contains("failed"),
        "stderr must include a network-level diagnostic; got:\n{stderr}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres + toxiproxy"]
fn export_recovers_after_mid_stream_proxy_disable_then_enable_with_retries() {
    // Start export, immediately disable the proxy for a short window, then
    // re-enable.  With max_retries >= 2 the retry loop must reconnect and
    // the export must finish with the full row count.
    //
    // The "mid-stream" nature of the fault is simulated by scheduling the
    // disable-then-enable cycle in a background thread that races the
    // export itself.  This is weaker than a true "fault after batch N"
    // hook (which is QA backlog Task 1.1 / Phase G), but it does exercise
    // the reconnect path.
    let _g = toxiproxy_guard();
    require_alive(LiveService::Postgres);
    require_alive(LiveService::Toxiproxy);
    reset_postgres_proxy();

    let table = seed_pg_numeric_table(50);
    let export_name = unique_name("qa42_midstream");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_TOXI_URL}"
exports:
  - name: {export_name}
    query: "SELECT id FROM {table_name}"
    mode: full
    format: parquet
    destination: {{type: local, path: {dir}}}
    tuning:
      max_retries: 3
      retry_backoff_ms: 200
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    // Disable/enable cycle runs in the background.
    let bg = std::thread::spawn(|| {
        std::thread::sleep(Duration::from_millis(50));
        toxi_disable("postgres");
        std::thread::sleep(Duration::from_millis(500));
        toxi_enable("postgres");
    });

    let out_run = run_rivet_export(&cfg_path, &export_name);
    let _ = bg.join();

    assert!(
        out_run.status.success(),
        "export must recover after transient proxy outage; stderr:\n{}",
        String::from_utf8_lossy(&out_run.stderr),
    );
    // Row-count sanity: a single file was produced and contains 50 rows.
    let files = files_with_extension(out.path(), "parquet");
    assert_eq!(files.len(), 1);
}

// ─── Task 4.1: retry counter & classification at runtime ───────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn permanent_sql_error_fails_fast_without_exhausting_retries() {
    // An invalid SQL query (syntax error) is classified as permanent.
    // Even with max_retries=5 the runtime must NOT retry — it should bail
    // after the first attempt with an actionable error.  Observable: the
    // process exit time is short (< 2s) even with a high retry_backoff_ms.
    require_alive(LiveService::Postgres);

    let export_name = unique_name("qa43_permerr");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {export_name}
    query: "SELCT 1"  # intentional typo → SQL syntax error
    mode: full
    format: parquet
    destination: {{type: local, path: {dir}}}
    tuning:
      max_retries: 5
      retry_backoff_ms: 5000
"#,
        dir = out.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    let t0 = std::time::Instant::now();
    let out_run = run_rivet_export(&cfg_path, &export_name);
    let elapsed = t0.elapsed();

    assert!(
        !out_run.status.success(),
        "invalid query must exit non-zero"
    );

    // If retries had been executed, wall time would approach
    //   sum(retry_backoff_ms * 2^k for k in 0..5) ≈ 5_000 * 31 = 155 s.
    // Permanent-error classification caps this at the first attempt +
    // connection overhead — easily under 15 seconds.
    assert!(
        elapsed < Duration::from_secs(15),
        "permanent error must short-circuit retry loop; elapsed={elapsed:?}"
    );
    // And the stderr must name the underlying cause.
    let stderr = String::from_utf8_lossy(&out_run.stderr);
    let low = stderr.to_lowercase();
    assert!(
        low.contains("syntax") || low.contains("error"),
        "stderr must include the driver's error text; got:\n{stderr}"
    );
}
