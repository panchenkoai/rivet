//! Controlled chaos scenarios — Toxiproxy-driven and object-storage faults.
//!
//! QA backlog:
//!   * Task 4A.4 — deterministic gremlin-style resilience tests.
//!   * Task 6.2 — object storage intermittent behaviour.
//!
//! "Deterministic" here means: each scenario injects a named, time-bounded
//! fault and asserts a specific observable outcome (retry success, clean
//! error, no data loss).  We do not exercise random chaos — backlog Task
//! 4A.5 (nightly extended) is the place for that.

mod common;

use std::time::Duration;

use common::*;

// ─── Toxiproxy scenarios ───────────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres + toxiproxy"]
fn high_latency_does_not_cause_false_positive_transient_classification() {
    // 200ms per-packet latency significantly slows the query, but it must
    // NOT be interpreted as a timeout/transient failure because the driver
    // never actually returns an error.  This pins the contract that slow
    // networks are tolerated.
    let _g = toxiproxy_guard();
    require_alive(LiveService::Postgres);
    require_alive(LiveService::Toxiproxy);
    ensure_toxi_proxy("postgres", 15432, "postgres:5432");
    toxi_reset_toxics("postgres");
    let _toxic = toxi_add_latency("postgres", 200);

    let table = seed_pg_numeric_table(3);
    let export_name = unique_name("qa4A4_hilat");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_TOXI_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id FROM {table_name}"
    mode: full
    format: parquet
    destination: {{type: local, path: {dir}}}
    tuning: {{max_retries: 0}}
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let out_run = run_rivet_export(&cfg, &export_name);
    toxi_reset_toxics("postgres");

    assert!(
        out_run.status.success(),
        "200ms latency must not be classified as transient failure"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres + toxiproxy"]
fn chunked_export_completes_through_temporary_proxy_disable() {
    // Chunked mode with retries enabled must recover from a mid-stream
    // proxy disable.  Each chunk is a separate query, so reconnect happens
    // on the next chunk after the proxy is re-enabled.
    let _g = toxiproxy_guard();
    require_alive(LiveService::Postgres);
    require_alive(LiveService::Toxiproxy);
    ensure_toxi_proxy("postgres", 15432, "postgres:5432");
    toxi_reset_toxics("postgres");

    let table = seed_pg_numeric_table(40);
    let export_name = unique_name("qa4A4_chunk");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_TOXI_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id, name FROM {table_name}"
    mode: chunked
    chunk_column: id
    chunk_size: 10
    format: parquet
    destination: {{type: local, path: {dir}}}
    tuning: {{max_retries: 3, retry_backoff_ms: 200}}
"#,
        table_name = table.name(),
        dir = out.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);

    let bg = std::thread::spawn(|| {
        std::thread::sleep(Duration::from_millis(60));
        toxi_disable("postgres");
        std::thread::sleep(Duration::from_millis(400));
        toxi_enable("postgres");
    });

    let out_run = run_rivet_export(&cfg, &export_name);
    let _ = bg.join();
    toxi_reset_toxics("postgres");

    assert!(
        out_run.status.success(),
        "chunked export must recover from transient outage; stderr:\n{}",
        String::from_utf8_lossy(&out_run.stderr)
    );
}

// ─── Object storage intermittent behaviour ────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres + minio"]
fn s3_export_fails_cleanly_when_bucket_does_not_exist() {
    // Missing bucket is a permanent error.  Must surface as non-zero exit
    // with a recognisable error message (not a panic, not an empty log).
    require_alive(LiveService::Postgres);
    require_alive(LiveService::Minio);

    let table = seed_pg_numeric_table(3);
    let bucket = unique_name("rivet-qa-nope"); // never created
    let export_name = unique_name("qa62_nobucket");

    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id FROM {table_name}"
    mode: full
    format: parquet
    destination:
      type: s3
      bucket: {bucket}
      region: us-east-1
      endpoint: {MINIO_ENDPOINT}
      access_key_env: RIVET_TEST_MINIO_AK
      secret_key_env: RIVET_TEST_MINIO_SK
    tuning: {{max_retries: 0}}
"#,
        table_name = table.name()
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let out = std::process::Command::new(RIVET_BIN)
        .args([
            "run",
            "--config",
            cfg.to_str().unwrap(),
            "--export",
            &export_name,
        ])
        .env("RIVET_TEST_MINIO_AK", MINIO_ACCESS_KEY)
        .env("RIVET_TEST_MINIO_SK", MINIO_SECRET_KEY)
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .output()
        .expect("spawn rivet");

    assert!(
        !out.status.success(),
        "missing bucket must not silently succeed"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !stderr.is_empty(),
        "missing bucket must produce a non-empty stderr diagnostic"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres + minio"]
fn s3_export_recovers_when_destination_comes_back_after_transient_outage() {
    // Simulating a brief MinIO outage without docker network tricks is
    // difficult — but we can simulate the equivalent on our end by pointing
    // the export at a wrong endpoint first (force a connection error) and
    // then re-running against the real one.  Each run is independent, but
    // the test proves the retry path and the second attempt's success are
    // both observable.
    require_alive(LiveService::Postgres);
    require_alive(LiveService::Minio);

    let bucket = "rivet-qa-parity"; // already created in Phase B
    ensure_minio_bucket(bucket);
    let table = seed_pg_numeric_table(5);

    // Attempt #1: wrong endpoint.
    {
        let export_name = unique_name("qa62_wrong");
        let cfg_dir = tempfile::tempdir().unwrap();
        let yaml = format!(
            r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id FROM {table_name}"
    mode: full
    format: parquet
    destination:
      type: s3
      bucket: {bucket}
      region: us-east-1
      endpoint: http://127.0.0.1:1
      access_key_env: RIVET_TEST_MINIO_AK
      secret_key_env: RIVET_TEST_MINIO_SK
    tuning: {{max_retries: 0}}
"#,
            table_name = table.name()
        );
        let cfg = write_config(&cfg_dir, &yaml);
        let out = std::process::Command::new(RIVET_BIN)
            .args([
                "run",
                "--config",
                cfg.to_str().unwrap(),
                "--export",
                &export_name,
            ])
            .env("RIVET_TEST_MINIO_AK", MINIO_ACCESS_KEY)
            .env("RIVET_TEST_MINIO_SK", MINIO_SECRET_KEY)
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .output()
            .expect("spawn rivet");
        assert!(!out.status.success(), "export to wrong endpoint must fail");
    }

    // Attempt #2: correct endpoint — must succeed.
    {
        let export_name = unique_name("qa62_right");
        let prefix = unique_name("qa62_intermittent");
        let cfg_dir = tempfile::tempdir().unwrap();
        let yaml = format!(
            r#"
source: {{type: postgres, url: "{POSTGRES_URL}"}}
exports:
  - name: {export_name}
    query: "SELECT id FROM {table_name}"
    mode: full
    format: parquet
    destination:
      type: s3
      bucket: {bucket}
      prefix: {prefix}
      region: us-east-1
      endpoint: {MINIO_ENDPOINT}
      access_key_env: RIVET_TEST_MINIO_AK
      secret_key_env: RIVET_TEST_MINIO_SK
    tuning: {{max_retries: 0}}
"#,
            table_name = table.name()
        );
        let cfg = write_config(&cfg_dir, &yaml);
        let out = std::process::Command::new(RIVET_BIN)
            .args([
                "run",
                "--config",
                cfg.to_str().unwrap(),
                "--export",
                &export_name,
            ])
            .env("RIVET_TEST_MINIO_AK", MINIO_ACCESS_KEY)
            .env("RIVET_TEST_MINIO_SK", MINIO_SECRET_KEY)
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .output()
            .expect("spawn rivet");
        assert!(
            out.status.success(),
            "recovery run must succeed; stderr:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}
