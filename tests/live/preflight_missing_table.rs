//! Pre-flight must catch a query against a non-existent table/column at `check`
//! time (non-zero exit + an actionable message) instead of passing with exit 0
//! and only failing at run time. SQLSTATE class 42 (PG) / error 1146/1054
//! (MySQL) / 208/207 (MSSQL) is permanent and author-fixable; operational errors
//! stay fail-soft (covered by the existing valid-config live tests).
//!
//! Run: `docker compose up -d postgres mysql mssql && \
//!       cargo test --test preflight_missing_table -- --ignored`

use crate::common::*;

fn check_rejects_missing_table(base: Rig, what: &str) {
    let rig = base
        .export_named("probe")
        .query("SELECT * FROM definitely_not_a_real_table_xyzzy")
        .with_format("csv");
    let out = rig.cli(&["check"]);
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
    check_rejects_missing_table(Rig::pg_batch("probe"), "postgres");
}

#[test]
#[ignore = "live: mysql"]
fn mysql_check_rejects_missing_table() {
    require_alive(LiveService::Mysql);
    check_rejects_missing_table(Rig::mysql_batch("probe"), "mysql");
}

#[test]
#[ignore = "live: mssql"]
fn mssql_check_rejects_missing_table() {
    require_alive(LiveService::Mssql);
    check_rejects_missing_table(Rig::mssql_batch("probe"), "mssql");
}
