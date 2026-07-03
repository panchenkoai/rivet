//! Runtime retry behaviour and mid-stream fault-injection via Toxiproxy —
//! MySQL twin of `tests/live_retry_and_faults.rs`.
//!
//! `RetryClass` and the retry backoff loop live in `pipeline::retry` and
//! `pipeline::single` respectively; both are driver-agnostic. This file
//! mirrors the PG retry matrix against MySQL so a regression of the
//! transient/permanent classifier on the MySQL error surface is caught
//! independently.
//!
//! ## Test symmetry with `live_retry_and_faults.rs`
//!
//! | PG test                                                                  | MySQL twin (this file)                                                             |
//! |--------------------------------------------------------------------------|------------------------------------------------------------------------------------|
//! | `clean_postgres_via_toxi_works_as_a_baseline_and_exports_successfully`   | `mysql_clean_via_toxi_works_as_a_baseline_and_exports_successfully`                |
//! | `export_survives_transient_latency_added_via_toxiproxy`                  | `mysql_export_survives_transient_latency_added_via_toxiproxy`                      |
//! | `export_fails_cleanly_when_toxiproxy_is_disabled_before_run`             | `mysql_export_fails_cleanly_when_toxiproxy_is_disabled_before_run`                 |
//! | `export_recovers_after_mid_stream_proxy_disable_then_enable_with_retries`| `mysql_export_recovers_after_mid_stream_proxy_disable_then_enable_with_retries`    |
//! | `permanent_sql_error_fails_fast_without_exhausting_retries`              | `mysql_permanent_sql_error_fails_fast_without_exhausting_retries`                  |

use std::time::Duration;

use crate::common::*;

/// Idempotent setup: register the `mysql` toxi proxy (listen on 13306 →
/// `mysql:3306`) and wipe any leftover toxics.
fn reset_mysql_proxy() {
    ensure_toxi_proxy("mysql", 13306, "mysql:3306");
    toxi_reset_toxics("mysql");
}

#[test]
#[ignore = "live: requires docker compose mysql + toxiproxy"]
fn mysql_clean_via_toxi_works_as_a_baseline_and_exports_successfully() {
    let _g = toxiproxy_guard();
    require_alive(LiveService::Mysql);
    require_alive(LiveService::Toxiproxy);
    reset_mysql_proxy();

    let table = seed_mysql_numeric_table(5);
    let export_name = unique_name("qa41my_baseline");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = Rig::mysql_batch(&export_name)
        .source_url(MYSQL_TOXI_URL)
        .query(&format!(
            r#"SELECT id FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("full")
        .dest_path(out.path().to_path_buf())
        .yaml();
    let cfg_path = write_config(&cfg_dir, &yaml);
    let out_run = run_rivet_export(&cfg_path, &export_name);
    assert!(
        out_run.status.success(),
        "baseline mysql run through toxiproxy failed; stderr:\n{}",
        String::from_utf8_lossy(&out_run.stderr),
    );
}

#[test]
#[ignore = "live: requires docker compose mysql + toxiproxy"]
fn mysql_export_survives_transient_latency_added_via_toxiproxy() {
    let _g = toxiproxy_guard();
    require_alive(LiveService::Mysql);
    require_alive(LiveService::Toxiproxy);
    reset_mysql_proxy();

    let toxic = toxi_add_latency("mysql", 50);

    let table = seed_mysql_numeric_table(5);
    let export_name = unique_name("qa41my_latency");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = Rig::mysql_batch(&export_name)
        .source_url(MYSQL_TOXI_URL)
        .query(&format!(
            r#"SELECT id FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("full")
        .export_line("tuning:")
        .export_line("  max_retries: 2")
        .dest_path(out.path().to_path_buf())
        .yaml();
    let cfg_path = write_config(&cfg_dir, &yaml);
    let out_run = run_rivet_export(&cfg_path, &export_name);

    let _ = toxic;
    toxi_reset_toxics("mysql");

    assert!(
        out_run.status.success(),
        "mysql export must complete under 50ms latency; stderr:\n{}",
        String::from_utf8_lossy(&out_run.stderr),
    );
}

#[test]
#[ignore = "live: requires docker compose mysql + toxiproxy"]
fn mysql_export_fails_cleanly_when_toxiproxy_is_disabled_before_run() {
    let _g = toxiproxy_guard();
    require_alive(LiveService::Mysql);
    require_alive(LiveService::Toxiproxy);
    reset_mysql_proxy();

    toxi_disable("mysql");

    let export_name = unique_name("qa42my_disabled");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = Rig::mysql_batch(&export_name)
        .source_url(MYSQL_TOXI_URL)
        .query("SELECT 1 AS n")
        .mode("full")
        .export_line("tuning:")
        .export_line("  max_retries: 0")
        .dest_path(out.path().to_path_buf())
        .yaml();
    let cfg_path = write_config(&cfg_dir, &yaml);
    let out_run = run_rivet_export(&cfg_path, &export_name);

    toxi_enable("mysql");

    assert!(
        !out_run.status.success(),
        "mysql export through a disabled proxy must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&out_run.stderr);
    let low = stderr.to_lowercase();
    assert!(
        low.contains("connection")
            || low.contains("refused")
            || low.contains("reset")
            || low.contains("timeout")
            || low.contains("failed")
            || low.contains("broken"),
        "stderr must include a network-level diagnostic; got:\n{stderr}"
    );
}

#[test]
#[ignore = "live: requires docker compose mysql + toxiproxy"]
fn mysql_export_recovers_after_mid_stream_proxy_disable_then_enable_with_retries() {
    let _g = toxiproxy_guard();
    require_alive(LiveService::Mysql);
    require_alive(LiveService::Toxiproxy);
    reset_mysql_proxy();

    let table = seed_mysql_numeric_table(50);
    let export_name = unique_name("qa42my_midstream");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = Rig::mysql_batch(&export_name)
        .source_url(MYSQL_TOXI_URL)
        .query(&format!(
            r#"SELECT id FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("full")
        .export_line("tuning:")
        .export_line("  max_retries: 3")
        .export_line("  retry_backoff_ms: 200")
        .dest_path(out.path().to_path_buf())
        .yaml();
    let cfg_path = write_config(&cfg_dir, &yaml);

    let bg = std::thread::spawn(|| {
        std::thread::sleep(Duration::from_millis(50));
        toxi_disable("mysql");
        std::thread::sleep(Duration::from_millis(500));
        toxi_enable("mysql");
    });

    let out_run = run_rivet_export(&cfg_path, &export_name);
    let _ = bg.join();

    assert!(
        out_run.status.success(),
        "mysql export must recover after transient proxy outage; stderr:\n{}",
        String::from_utf8_lossy(&out_run.stderr),
    );
    let files = files_with_extension(out.path(), "parquet");
    assert_eq!(files.len(), 1);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_permanent_sql_error_fails_fast_without_exhausting_retries() {
    require_alive(LiveService::Mysql);

    let export_name = unique_name("qa43my_permerr");
    let out = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = Rig::mysql_batch(&export_name)
        // intentional typo → a permanent SQL syntax error
        .query("SELCT 1")
        .mode("full")
        .export_line("tuning:")
        .export_line("  max_retries: 5")
        .export_line("  retry_backoff_ms: 5000")
        .dest_path(out.path().to_path_buf())
        .yaml();
    let cfg_path = write_config(&cfg_dir, &yaml);

    let t0 = std::time::Instant::now();
    let out_run = run_rivet_export(&cfg_path, &export_name);
    let elapsed = t0.elapsed();

    assert!(
        !out_run.status.success(),
        "invalid mysql query must exit non-zero"
    );

    // With 5 retries × 5s base backoff, full retry chain would approach
    // 5_000 * 31 ≈ 155 s. Permanent-error classification must short-circuit.
    assert!(
        elapsed < Duration::from_secs(15),
        "permanent error must short-circuit retry loop; elapsed={elapsed:?}"
    );
    let stderr = String::from_utf8_lossy(&out_run.stderr);
    let low = stderr.to_lowercase();
    assert!(
        low.contains("syntax") || low.contains("error") || low.contains("sql"),
        "stderr must include the driver's error text; got:\n{stderr}"
    );
}
