//! What a bounded CDC run costs the source in CONNECTIONS, read from the server's own
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
