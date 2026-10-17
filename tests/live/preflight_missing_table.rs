//! Pre-flight must catch a query against a non-existent table at `check` time with the
//! refusal every path shares, `RIVET_SOURCE_TABLE_NOT_FOUND` (exit 1), instead of passing
//! with exit 0 and only failing at run time. SQLSTATE 42P01 (PG) / error 1146 (MySQL) /
//! 208 (MSSQL) / ORA-00942 (Oracle) is permanent and author-fixable; operational errors
//! stay fail-soft (covered by the existing valid-config live tests).
//!
//! Run: `docker compose up -d postgres mysql mssql oracle && \
//!       cargo test --test preflight_missing_table -- --ignored`

use crate::common::*;

fn check_rejects_missing_table(base: Rig) {
    let rig = base
        .export_named("probe")
        .query("SELECT * FROM definitely_not_a_real_table_xyzzy")
        .with_format("csv");
    let out = rig.cli(&["check"]);
    assert_refused(&out, Refused::by_code("RIVET_SOURCE_TABLE_NOT_FOUND", 1));
}

#[test]
#[ignore = "live: postgres"]
fn pg_check_rejects_missing_table() {
    require_alive(LiveService::Postgres);
    check_rejects_missing_table(Rig::pg_batch("probe"));
}

#[test]
#[ignore = "live: mysql"]
fn mysql_check_rejects_missing_table() {
    require_alive(LiveService::Mysql);
    check_rejects_missing_table(Rig::mysql_batch("probe"));
}

#[test]
#[ignore = "live: mssql"]
fn mssql_check_rejects_missing_table() {
    require_alive(LiveService::Mssql);
    check_rejects_missing_table(Rig::mssql_batch("probe"));
}

#[test]
#[ignore = "live: oracle"]
fn oracle_check_rejects_missing_table() {
    require_alive(LiveService::Oracle);
    check_rejects_missing_table(Rig::oracle_batch("probe"));
}
