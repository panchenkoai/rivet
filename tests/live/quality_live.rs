//! Live E2E tests for quality checks.
//!
//! Roadmap Phase 1 §3.4 — Tests for Quality Uniqueness.
//!
//! All tests require the docker-compose Postgres stack.
//! Run with: `cargo test --test quality_live -- --include-ignored`
//!
//! ## What these tests verify
//!
//! - Unique column on a clean dataset: export succeeds with no quality issues.
//! - Unique column on a dataset with duplicates: export fails (Severity::Fail).
//! - `row_count_min` gate: export fails when source has too few rows.
//! - `row_count_max` gate: export fails when source has too many rows.
//! - `unique_columns` without `unique_max_entries`: plan validation warning
//!   appears in stderr (tested with RUST_LOG=warn).
//! - `null_ratio_max` gate: export fails when null ratio exceeds threshold.

use crate::common::*;

fn cfg(yaml: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let d = tempfile::tempdir().unwrap();
    let p = write_config(&d, yaml);
    (d, p)
}

// ─── §3.4 — unique_columns ───────────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn unique_column_on_clean_data_passes() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(20);
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("ql_uniq_ok");

    let yaml = Rig::pg_batch(&export_name)
        .query(&format!(
            r#"SELECT id, name, amount FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("full")
        .export_line(r#"columns: { amount: "decimal(12,2)" }"#)
        .export_line("quality:")
        .export_line("  unique_columns: [id]")
        .export_line("  unique_max_entries: 1000")
        .dest_path(out.path().to_path_buf())
        .yaml();
    let (_cfgdir, cfgpath) = cfg(&yaml);

    let result = run_rivet_export(&cfgpath, &export_name);
    assert!(
        result.status.success(),
        "unique check on clean data must pass; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn unique_column_on_duplicate_data_fails_export() {
    // Insert deliberate duplicates in the id column by UNION ALL.
    // The query wraps the table twice so id values appear twice.
    // rivet should detect duplicates and exit non-zero.
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(10);
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("ql_uniq_dup");

    let yaml = Rig::pg_batch(&export_name)
        .query(&format!(
            "SELECT id FROM {t} UNION ALL SELECT id FROM {t}",
            t = table.name()
        ))
        .mode("full")
        .export_line("quality:")
        .export_line("  unique_columns: [id]")
        .dest_path(out.path().to_path_buf())
        .yaml();
    let (_cfgdir, cfgpath) = cfg(&yaml);

    let result = run_rivet_export(&cfgpath, &export_name);
    assert!(
        !result.status.success(),
        "unique check on duplicate data must fail the export"
    );
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("duplicate") || stderr.contains("quality"),
        "error message must mention duplicates or quality; stderr:\n{stderr}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn unique_columns_without_cap_emits_plan_warning() {
    // When unique_columns is configured without unique_max_entries, the plan
    // validator emits a quality-unique-no-cap Warning via log::warn!.
    // Verified with RUST_LOG=warn so the warning appears in stderr.
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(10);
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("ql_uniq_no_cap");

    let yaml = Rig::pg_batch(&export_name)
        .query(&format!(
            r#"SELECT id FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("full")
        .export_line("quality:")
        .export_line("  unique_columns: [id]")
        .dest_path(out.path().to_path_buf())
        .yaml();
    let (_cfgdir, cfgpath) = cfg(&yaml);

    let result = run_rivet_with_warn_log(&[
        "run",
        "--config",
        cfgpath.to_str().unwrap(),
        "--export",
        &export_name,
    ]);
    // Export must still succeed (Warning does not block execution).
    assert!(
        result.status.success(),
        "quality-unique-no-cap is a Warning, must not block export; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("quality-unique-no-cap") || stderr.contains("unique_max_entries"),
        "plan validation must emit quality-unique-no-cap warning; stderr:\n{stderr}"
    );
}

// ─── row_count gates ─────────────────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn row_count_min_gate_fails_when_below_threshold() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(5); // only 5 rows
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("ql_rowmin");

    let yaml = Rig::pg_batch(&export_name)
        .query(&format!(
            r#"SELECT id FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("full")
        .export_line("quality:")
        .export_line("  row_count_min: 100")
        .dest_path(out.path().to_path_buf())
        .yaml();
    let (_cfgdir, cfgpath) = cfg(&yaml);

    let result = run_rivet_export(&cfgpath, &export_name);
    assert!(
        !result.status.success(),
        "row_count_min=100 on 5-row source must fail the export"
    );
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("row_count") || stderr.contains("quality") || stderr.contains("minimum"),
        "error must mention row count; stderr:\n{stderr}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn row_count_max_gate_fails_when_above_threshold() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(50); // 50 rows
    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("ql_rowmax");

    let yaml = Rig::pg_batch(&export_name)
        .query(&format!(
            r#"SELECT id FROM {table_name}"#,
            table_name = table.name()
        ))
        .mode("full")
        .export_line("quality:")
        .export_line("  row_count_max: 10")
        .dest_path(out.path().to_path_buf())
        .yaml();
    let (_cfgdir, cfgpath) = cfg(&yaml);

    let result = run_rivet_export(&cfgpath, &export_name);
    assert!(
        !result.status.success(),
        "row_count_max=10 on 50-row source must fail the export"
    );
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("row_count") || stderr.contains("quality") || stderr.contains("maximum"),
        "error must mention row count; stderr:\n{stderr}"
    );
}

// ─── null_ratio_max gate ─────────────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn null_ratio_max_gate_fails_when_exceeded() {
    // Seed a table with a nullable column where most rows are NULL.
    require_alive(LiveService::Postgres);

    let table_name = unique_name("ql_null");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, val TEXT);
         INSERT INTO {table_name} (id, val)
         SELECT g, CASE WHEN g <= 2 THEN 'x' ELSE NULL END
         FROM generate_series(1, 10) g;"
    ))
    .unwrap();

    struct Cleanup(String);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            if let Ok(mut c) = postgres::Client::connect(POSTGRES_URL, postgres::NoTls) {
                let _ = c.execute(&format!("DROP TABLE IF EXISTS {}", self.0), &[]);
            }
        }
    }
    let _guard = Cleanup(table_name.clone());

    let out = tempfile::tempdir().unwrap();
    let export_name = unique_name("ql_null_exp");
    let yaml = Rig::pg_batch(&export_name)
        .query(&format!(r#"SELECT id, val FROM {table_name}"#))
        .mode("full")
        .export_line("quality:")
        .export_line("  null_ratio_max:")
        .export_line("    val: 0.1")
        .dest_path(out.path().to_path_buf())
        .yaml();
    let (_cfgdir, cfgpath) = cfg(&yaml);

    let result = run_rivet_export(&cfgpath, &export_name);
    assert!(
        !result.status.success(),
        "null_ratio_max=0.1 with 80% nulls must fail the export"
    );
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("null") || stderr.contains("quality"),
        "error must mention null ratio or quality; stderr:\n{stderr}"
    );
}
