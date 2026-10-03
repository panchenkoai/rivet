//! SQL Server incremental reads against a transaction that commits mid-scan.

use crate::common::*;
use std::collections::BTreeSet;

const SI_DB: &str = "rivet_si";

/// A database with ALLOW_SNAPSHOT_ISOLATION on, created on first use.
fn snapshot_db() {
    mssql_exec(&format!(
        "IF DB_ID('{SI_DB}') IS NULL CREATE DATABASE {SI_DB};\nGO\n\
         ALTER DATABASE {SI_DB} SET ALLOW_SNAPSHOT_ISOLATION ON;"
    ));
    assert_eq!(
        mssql_query_i64(&format!(
            "SELECT CAST(snapshot_isolation_state AS INT) FROM sys.databases WHERE name = '{SI_DB}'"
        )),
        1,
        "fixture: {SI_DB} must have ALLOW_SNAPSHOT_ISOLATION ON"
    );
}

/// T-SQL that loops until `cond` holds, or THROWs `what` after `secs` (needs `DECLARE @deadline DATETIME2`).
fn server_wait(cond: &str, secs: u32, what: &str) -> String {
    format!(
        "SET @deadline = DATEADD(SECOND, {secs}, SYSDATETIME());\n\
         WHILE NOT ({cond})\nBEGIN\n\
         IF SYSDATETIME() > @deadline THROW 50000, N'{} within {secs} s', 1;\n\
         WAITFOR DELAY '00:00:00.010';\nEND;\n",
        what.replace('\'', "''")
    )
}

/// Poll (NOLOCK) until `sql` counts a row, panicking with the elapsed time after 60 s.
fn wait_visible(sql: &str, what: &str) {
    let t0 = std::time::Instant::now();
    while mssql_query_i64(sql) == 0 {
        assert!(
            t0.elapsed().as_secs() < 60,
            "fixture: {what} not visible after {:?}",
            t0.elapsed()
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

/// Run 2 overlaps a transaction that inserts id 100, then updates id 1 to v2 once the server shows run 2's scan.
fn run_across_a_mid_scan_commit(db: &str) -> (Rig, MssqlTable, String) {
    let table = format!("{db}.dbo.{}", unique_name("inc_iso"));
    mssql_exec(&format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, version BIGINT NOT NULL, updated_at DATETIME2(6) NOT NULL);\n\
         INSERT INTO {table} VALUES (1,1,'2026-01-01'),(2,1,'2026-01-01');"
    ));
    let guard = MssqlTable::adopt(table.clone());
    let rig = Rig::mssql_batch("inc_iso")
        .source_url(&MSSQL_URL.replace("/rivet", &format!("/{db}")))
        .query(&format!("SELECT id, version, updated_at FROM {table}"))
        .mode("incremental")
        .export_line("cursor_column: updated_at")
        .duckdb_oracle()
        .no_oracle("a writer commits mid-run by design, and the query export names no key to fold versions by");
    rig.run_ok();

    // Run 2's scan text, split so this batch's own text never contains it.
    let scan = format!("N'{table}) AS _rivet WHERE [upd' + N'ated_at] >'");
    let started = "EXISTS (SELECT 1 FROM sys.dm_exec_requests r CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t \
         WHERE r.session_id <> @@SPID AND CHARINDEX(@scan, t.text) > 0) \
         OR EXISTS (SELECT 1 FROM sys.dm_exec_query_stats q CROSS APPLY sys.dm_exec_sql_text(q.sql_handle) t \
         WHERE q.last_execution_time >= @t0 AND CHARINDEX(@scan, t.text) > 0)";
    let tx = format!(
        "SET XACT_ABORT ON; DECLARE @deadline DATETIME2; DECLARE @scan NVARCHAR(400) = {scan};\n\
         BEGIN TRAN; INSERT INTO {table} VALUES (100,1,'2026-01-02');\n\
         DECLARE @t0 DATETIME2 = DATEADD(SECOND, -1, SYSDATETIME());\n{}\
         UPDATE {table} SET version = 2, updated_at = '2026-01-02' WHERE id = 1;\nCOMMIT;",
        server_wait(
            started,
            60,
            "fixture: run 2's scan never reached the server"
        )
    );
    let writer = std::thread::spawn(move || mssql_exec_once(&tx));
    wait_visible(
        &format!("SELECT COUNT(*) FROM {table} WITH (NOLOCK) WHERE id = 100"),
        "the writer's uncommitted id 100",
    );
    let said = rig.run_ok_capture();
    if let Err(e) = writer.join().expect("writer thread") {
        panic!("the writer did not commit after run 2's scan started: {e}");
    }
    rig.run_ok();
    (rig, guard, said)
}

/// `id:version` pairs the source holds now.
fn source_state(table: &str) -> BTreeSet<String> {
    mssql_query_strings(&format!("SELECT CONCAT(id, ':', version) FROM {table}"))
        .into_iter()
        .collect()
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_incremental_never_skips_an_update_committed_mid_scan() {
    snapshot_db();
    let (rig, guard, said) = run_across_a_mid_scan_commit(SI_DB);
    let source = source_state(guard.name());
    assert_eq!(
        source,
        BTreeSet::from(["1:2".into(), "2:1".into(), "100:1".into()])
    );
    let exported = duckdb_declared_distinct_set(
        rig.oracle_dir(),
        "CAST(id AS VARCHAR) || ':' || CAST(version AS VARCHAR)",
    );
    let missing: Vec<_> = source.difference(&exported).collect();
    assert!(
        missing.is_empty(),
        "the source's final rows {missing:?} never reached the declared parts {exported:?}"
    );
    assert!(
        !said.contains("READ_COMMITTED_SNAPSHOT"),
        "no locking warning on a snapshot database: {said}"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_incremental_on_a_locking_database_warns_with_the_fix() {
    assert_eq!(
        mssql_query_i64(
            "SELECT CAST(snapshot_isolation_state AS INT) + CAST(is_read_committed_snapshot_on AS INT) \
             FROM sys.databases WHERE name = 'rivet'"
        ),
        0,
        "fixture: the shared rivet database must run locking READ COMMITTED"
    );
    let (_rig, _guard, said) = run_across_a_mid_scan_commit("rivet");
    assert!(
        said.contains(
            "mssql: database 'rivet' has neither READ_COMMITTED_SNAPSHOT nor ALLOW_SNAPSHOT_ISOLATION on"
        ) && said.contains("`ALTER DATABASE [rivet] SET ALLOW_SNAPSHOT_ISOLATION ON`"),
        "{said}"
    );
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_lock_timeout_fails_a_blocked_read_fast() {
    let table = format!("rivet.dbo.{}", unique_name("lock_to"));
    mssql_exec(&format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, v BIGINT NOT NULL);\nINSERT INTO {table} VALUES (1,1);"
    ));
    let _guard = MssqlTable::adopt(table.clone());
    let rig = Rig::mssql_batch("lock_to")
        .query(&format!("SELECT id, v FROM {table}"))
        .source_line("tuning:")
        .source_line("  lock_timeout_s: 1");
    let blocked = "EXISTS (SELECT 1 FROM sys.dm_exec_requests WHERE blocking_session_id = @@SPID)";
    let tx = format!(
        "SET XACT_ABORT ON; DECLARE @deadline DATETIME2;\n\
         BEGIN TRAN; UPDATE {table} SET v = 2 WHERE id = 1;\n{}{}COMMIT;",
        server_wait(
            blocked,
            60,
            "fixture: rivet's read never blocked on the held row lock"
        ),
        server_wait(
            &format!("NOT {blocked}"),
            5,
            "rivet's read stayed blocked over 5 s on a 1 s lock_timeout"
        )
    );
    let writer = std::thread::spawn(move || mssql_exec_once(&tx));
    wait_visible(
        &format!("SELECT COUNT(*) FROM {table} WITH (NOLOCK) WHERE v = 2"),
        "the writer's row lock",
    );
    let said = rig.run_expect_fail();
    let held = writer.join().expect("writer thread");
    assert!(
        said.contains("Lock request time out period exceeded"),
        "{said}"
    );
    held.unwrap_or_else(|e| panic!("{e}"));
}
