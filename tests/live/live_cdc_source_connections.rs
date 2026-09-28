//! What a run — a bounded CDC run, and a batch run — costs the source in CONNECTIONS, read from the server's own
//! cumulative counter around one steady-state run (checkpoint present, one change).
//!
//! Every connection is a server session to set up — on PostgreSQL a backend that warms its
//! relcache from `pg_class`/`pg_attribute`, which is exactly the catalog churn the perf gate
//! caught. Measured before the fix: PostgreSQL 10, MySQL 12, SQL Server 6 per run, most of
//! them probes of facts another connection of the same run already held. The ceilings below
//! are what the run needs: one metadata connection plus the change stream, on every engine.
//!
//! `live+exclusive`: the counters are server-wide, so a concurrent test's sessions would count.
//! Each stand's healthcheck also opens a session every 5-10 s; it only ever ADDS, so each
//! test takes the minimum over three runs.

use crate::common::*;

/// True (and a skip is recorded) unless the stand is ours alone for this test.
fn not_exclusive() -> bool {
    let shared = std::env::var("RIVET_TEST_EXCLUSIVE").is_err();
    if shared {
        skip_live(
            "counts server-wide connections — run with RIVET_TEST_EXCLUSIVE=1 and --test-threads=1",
        );
    }
    shared
}

/// The fewest connections any of three runs opened: `opened` measures one run.
fn fewest(mut opened: impl FnMut() -> i64) -> i64 {
    (0..3).map(|_| opened()).min().unwrap()
}

/// New sessions the server has seen, read on a connection the probe already holds.
fn pg_sessions(c: &mut postgres::Client) -> i64 {
    c.query_one(
        "SELECT sessions FROM pg_stat_database WHERE datname = current_database()",
        &[],
    )
    .unwrap()
    .get(0)
}

#[test]
#[ignore = "live+exclusive: requires postgres-cdc; counts server-wide sessions"]
fn a_postgres_cdc_run_opens_at_most_two_source_connections() {
    if not_exclusive() {
        return;
    }
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls).unwrap();
    let tbl = unique_name("rivet_cdc_conns");
    let slot = unique_name("rivet_conns_slot");
    c.batch_execute(&format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT); ALTER TABLE {tbl} REPLICA IDENTITY FULL"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    let rig = Rig::pg_cdc(&tbl, &slot);
    rig.run_ok();
    c.execute(&format!("INSERT INTO {tbl} VALUES (1, 10)"), &[])
        .unwrap();
    let before = pg_sessions(&mut c);
    let out = rig.run_and_read();
    let first = pg_sessions(&mut c) - before;
    let opened = first.min(fewest(|| {
        let before = pg_sessions(&mut c);
        rig.run_ok();
        pg_sessions(&mut c) - before
    }));
    c.execute("SELECT pg_drop_replication_slot($1)", &[&slot])
        .ok();
    assert_eq!(
        out.iter().map(|b| b.num_rows()).sum::<usize>(),
        1,
        "fixture: the run captured the change"
    );
    assert!(
        opened <= 2,
        "a bounded CDC run opened {opened} PostgreSQL sessions; it needs two (metadata + slot)"
    );
}

#[test]
#[ignore = "live+exclusive: requires mysql-cdc; counts server-wide connections"]
fn a_mysql_cdc_run_opens_at_most_two_source_connections() {
    if not_exclusive() {
        return;
    }
    use mysql::prelude::Queryable;
    let mut c = mysql::Conn::new(mysql::Opts::from_url(MYSQL_CDC_URL).unwrap()).unwrap();
    let connections = |c: &mut mysql::Conn| -> i64 {
        let row: (String, String) = c
            .query_first("SHOW GLOBAL STATUS LIKE 'Connections'")
            .unwrap()
            .unwrap();
        row.1.parse().unwrap()
    };
    let tbl = unique_name("rivet_cdc_conns");
    c.query_drop(format!("CREATE TABLE {tbl} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let _tbl = MysqlCdcTable(tbl.clone());
    let rig = Rig::mysql_cdc(&tbl);
    rig.run_ok();
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, 10)"))
        .unwrap();
    let before = connections(&mut c);
    let out = rig.run_and_read();
    let first = connections(&mut c) - before;
    let opened = first.min(fewest(|| {
        let before = connections(&mut c);
        rig.run_ok();
        connections(&mut c) - before
    }));
    assert_eq!(
        out.iter().map(|b| b.num_rows()).sum::<usize>(),
        1,
        "fixture: the run captured the change"
    );
    assert!(
        opened <= 2,
        "a bounded CDC run opened {opened} MySQL connections; it needs metadata + binlog"
    );
}

#[test]
#[ignore = "live+exclusive: requires mssql-cdc with SQL Server Agent; counts server-wide logins"]
fn a_sql_server_cdc_run_opens_at_most_two_source_connections() {
    if not_exclusive() {
        return;
    }
    let _serial = cross_process_serial("mssql_cdc");
    let logins = || {
        mssql_cdc_query_i64(
            "SELECT CAST(cntr_value AS INT) FROM sys.dm_os_performance_counters \
             WHERE counter_name = 'Logins/sec'",
        )
    };
    let table = unique_name("rivet_cdc_conns");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table}(id INT PRIMARY KEY, v INT)"
    ));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    let rig = Rig::mssql_cdc(&table, &ci);
    rig.run_ok();
    mssql_cdc_exec(&format!("INSERT INTO dbo.{table} VALUES (1, 10)"));
    wait_for_capture(&ci, 1);
    // Each probe logs in once itself: measure that, then subtract it.
    let probe = {
        let a = logins();
        logins() - a
    };
    let before = logins();
    let out = rig.run_and_read();
    let first = logins() - before - probe;
    let opened = first.min(fewest(|| {
        let before = logins();
        rig.run_ok();
        logins() - before - probe
    }));
    assert_eq!(
        out.iter().map(|b| b.num_rows()).sum::<usize>(),
        1,
        "fixture: the run captured the change"
    );
    assert!(
        opened <= 2,
        "a bounded CDC run opened {opened} SQL Server logins; it needs metadata + change table"
    );
}

/// The keyset export `rivet init --mode chunked` writes: its planner probe must reuse the metadata connection.
const KEYSET: &[&str] = &["chunk_by_key: id", "chunk_checkpoint: true"];

/// Connections one steady-state BATCH run opens, per engine: the fewest of three runs.
/// `lines` restages the export as `chunked` with those lines (e.g. keyset); empty keeps `full`.
fn batch_run_opens(engine: SqlEngine, lines: &[&str]) -> i64 {
    let (t, _guard) = engine.create("rivet_batch_conns", "id INT PRIMARY KEY, v INT");
    engine.exec(&format!("INSERT INTO {t} VALUES (1, 10), (2, 20)"));
    let rig = if lines.is_empty() {
        engine.rig(&t)
    } else {
        engine.rig(&t).restage("chunked", lines)
    };
    rig.run_ok();
    match engine {
        SqlEngine::Pg => {
            let mut c = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).unwrap();
            fewest(|| {
                let before = pg_sessions(&mut c);
                rig.run_ok();
                std::thread::sleep(std::time::Duration::from_millis(1100));
                pg_sessions(&mut c) - before
            })
        }
        SqlEngine::Mysql => {
            use mysql::prelude::Queryable;
            let mut c = mysql::Conn::new(mysql::Opts::from_url(MYSQL_URL).unwrap()).unwrap();
            let mut connections = move || -> i64 {
                let row: (String, String) = c
                    .query_first("SHOW GLOBAL STATUS LIKE 'Connections'")
                    .unwrap()
                    .unwrap();
                row.1.parse().unwrap()
            };
            fewest(|| {
                let before = connections();
                rig.run_ok();
                connections() - before
            })
        }
        SqlEngine::Mssql => {
            let logins = || {
                mssql_query_bigints(
                    "SELECT CAST(cntr_value AS BIGINT) FROM sys.dm_os_performance_counters \
                     WHERE counter_name = 'Logins/sec'",
                    1,
                )[0]
            };
            // Each probe logs in once itself: measure that, then subtract it.
            let probe = {
                let a = logins();
                logins() - a
            };
            fewest(|| {
                let before = logins();
                rig.run_ok();
                logins() - before - probe
            })
        }
    }
}

#[test]
#[ignore = "live+exclusive: requires postgres; counts server-wide sessions"]
fn a_postgres_batch_run_opens_at_most_two_source_connections() {
    if not_exclusive() {
        return;
    }
    for lines in [&[][..], KEYSET] {
        let opened = batch_run_opens(SqlEngine::Pg, lines);
        assert!(
            opened <= 2,
            "a batch run {lines:?} opened {opened} PostgreSQL sessions; it needs two (metadata + data)"
        );
    }
}

#[test]
#[ignore = "live+exclusive: requires mysql; counts server-wide connections"]
fn a_mysql_batch_run_opens_at_most_two_source_connections() {
    if not_exclusive() {
        return;
    }
    for lines in [&[][..], KEYSET] {
        let opened = batch_run_opens(SqlEngine::Mysql, lines);
        assert!(
            opened <= 2,
            "a batch run {lines:?} opened {opened} MySQL connections; it needs two (metadata + data)"
        );
    }
}

#[test]
#[ignore = "live+exclusive: requires mssql; counts server-wide logins"]
fn a_sql_server_batch_run_opens_at_most_two_source_connections() {
    if not_exclusive() {
        return;
    }
    for lines in [&[][..], KEYSET] {
        let opened = batch_run_opens(SqlEngine::Mssql, lines);
        assert!(
            opened <= 2,
            "a batch run {lines:?} opened {opened} SQL Server logins; it needs two (metadata + data)"
        );
    }
}
