//! Pre-flight must catch a query against a non-existent table/column at `check`
//! time (non-zero exit + an actionable message) instead of passing with exit 0
//! and only failing at run time. SQLSTATE class 42 (PG) / error 1146/1054
//! (MySQL) / 208/207 (MSSQL) is permanent and author-fixable; operational errors
//! stay fail-soft (covered by the existing valid-config live tests).
//!
//! Run: `docker compose up -d postgres mysql mssql && \
//!       cargo test --test preflight_missing_table -- --ignored`

use crate::common::*;

fn check_rejects_missing_table(source_yaml: &str, what: &str) {
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"{source_yaml}
exports:
  - name: probe
    query: "SELECT * FROM definitely_not_a_real_table_xyzzy"
    mode: full
    format: csv
    destination:
      type: local
      path: {dir}
"#,
        dir = cfg_dir.path().display()
    );
    let cfg = write_config(&cfg_dir, &yaml);
    let out = std::process::Command::new(RIVET_BIN)
        .args(["check", "-c", cfg.to_str().unwrap()])
        .output()
        .expect("spawn rivet check");
    assert!(
        !out.status.success(),
        "{what}: `rivet check` must FAIL (non-zero) on a non-existent table, not pass to run time. \
         exit={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        combined.to_lowercase().contains("preflight"),
        "{what}: failure must name the preflight schema check; got:\n{combined}"
    );
}

#[test]
#[ignore = "live: postgres"]
fn pg_check_rejects_missing_table() {
    require_alive(LiveService::Postgres);
    check_rejects_missing_table(
        &format!("source: {{type: postgres, url: \"{POSTGRES_URL}\"}}"),
        "postgres",
    );
}

#[test]
#[ignore = "live: mysql"]
fn mysql_check_rejects_missing_table() {
    require_alive(LiveService::Mysql);
    check_rejects_missing_table(
        &format!("source: {{type: mysql, url: \"{MYSQL_URL}\"}}"),
        "mysql",
    );
}

#[test]
#[ignore = "live: mssql"]
fn mssql_check_rejects_missing_table() {
    require_alive(LiveService::Mssql);
    check_rejects_missing_table(
        &format!("source: {{type: mssql, url: \"{MSSQL_URL}\"}}"),
        "mssql",
    );
}
