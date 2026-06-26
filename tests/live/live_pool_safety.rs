//! Pool safety tests — verifies that session state is not leaked to the
//! connection pool after successful or failed exports.
//!
//! ## Postgres tests (pgBouncer transaction mode)
//!
//! Require pgBouncer running with pool_size=1 so the same physical Postgres
//! connection is always reused — makes assertions deterministic.
//!
//! ```text
//! docker compose --profile pool up -d pgbouncer
//! cargo test --test live_suite -- --include-ignored
//! ```
//!
//! ## MySQL tests
//!
//! Use a single-connection pool (PoolConstraints::new(1,1)) so the same
//! physical MySQL connection is reused after export.
//!
//! ```text
//! docker compose up -d mysql
//! cargo test --test live_suite -- --include-ignored
//! ```
//!
//! ## MySQL / ProxySQL tests
//!
//! Cover the MySQL proxy-detection + cleanup-through-a-proxy paths.  ProxySQL
//! lives in the `pool` docker-compose profile alongside pgBouncer:
//!
//! ```text
//! docker compose --profile pool up -d proxysql
//! cargo test --test live_suite mysql_proxysql -- --include-ignored
//! ```
//!
//! What they verify:
//! - `detect_mysql_proxy_kind()` correctly classifies the connection as
//!   `MysqlProxyKind::ProxySql` (banner or `@@proxy_version` signal).
//! - Session-state cleanup still runs to completion when the export traffic
//!   is routed through ProxySQL (no silent fallback past the proxy).

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use postgres::{Client as PgClient, NoTls};

use crate::common::*;
use rivet::error::Result;
use rivet::source::mssql::{MssqlProxyKind, MssqlSource};
use rivet::source::mysql::{MysqlProxyKind, MysqlSource};
use rivet::source::postgres::PostgresSource;
use rivet::source::{BatchSink, ExportRequest, Source};
use rivet::tuning::{SourceTuning, TuningConfig};
use rivet::types::ColumnOverrides;

// ─── Sinks ─────────────────────────────────────────────────────────────────

struct NullSink;
impl BatchSink for NullSink {
    fn on_schema(&mut self, _: SchemaRef) -> Result<()> {
        Ok(())
    }
    fn on_batch(&mut self, _: &RecordBatch) -> Result<()> {
        Ok(())
    }
}

/// Errors on the first batch — simulates a mid-stream failure so we can
/// verify the connection is left in a clean state after ROLLBACK.
struct FailOnFirstBatch;
impl BatchSink for FailOnFirstBatch {
    fn on_schema(&mut self, _: SchemaRef) -> Result<()> {
        Ok(())
    }
    fn on_batch(&mut self, _: &RecordBatch) -> Result<()> {
        Err(anyhow::anyhow!("injected sink failure"))
    }
}

/// Panics on the first batch — simulates a *non-Result* failure (e.g. an
/// arithmetic overflow inside a sink) that bypasses `?`-based error handling
/// entirely.  This is the G1 fault model from the DBA audit: only the RAII
/// guard's `Drop` can release the cursor + ROLLBACK the txn here.
struct PanicOnFirstBatch;
impl BatchSink for PanicOnFirstBatch {
    fn on_schema(&mut self, _: SchemaRef) -> Result<()> {
        Ok(())
    }
    fn on_batch(&mut self, _: &RecordBatch) -> Result<()> {
        panic!("injected sink panic — must unwind through pg_run_export");
    }
}

// ─── Helpers ───────────────────────────────────────────────────────────────

fn tuning_300s() -> SourceTuning {
    let cfg = TuningConfig {
        statement_timeout_s: Some(300),
        ..Default::default()
    };
    SourceTuning::from_config(Some(&cfg))
}

fn pg_show_statement_timeout(client: &mut PgClient) -> String {
    client
        .query_one("SHOW statement_timeout", &[])
        .expect("SHOW statement_timeout")
        .get::<_, String>(0)
}

// ─── Postgres / pgBouncer tests ────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn pg_statement_timeout_not_leaked_after_successful_export() {
    require_alive(LiveService::Postgres);
    require_alive(LiveService::PgBouncer);

    let mut source = PostgresSource::connect(PGBOUNCER_URL).unwrap();
    source
        .export(
            &ExportRequest {
                query: "SELECT 1 AS n",
                catalog_hint_query: None,
                incremental: None,
                cursor: None,
                tuning: &tuning_300s(),
                column_overrides: &ColumnOverrides::default(),
                page_limit: None,
            },
            &mut NullSink,
        )
        .expect("export should succeed");

    // pool_size=1 → same physical connection; SET LOCAL must have been reset at COMMIT.
    let mut check = PgClient::connect(PGBOUNCER_URL, NoTls).unwrap();
    let timeout = pg_show_statement_timeout(&mut check);
    assert_eq!(
        timeout, "0",
        "statement_timeout leaked to pool after successful export: got {timeout:?}"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d pgbouncer (transaction mode, pool_size=1)"]
fn pg_connection_usable_and_clean_after_failed_export() {
    require_alive(LiveService::Postgres);
    require_alive(LiveService::PgBouncer);

    let mut source = PostgresSource::connect(PGBOUNCER_URL).unwrap();
    let result = source.export(
        &ExportRequest {
            query: "SELECT generate_series(1, 1000) AS n",
            catalog_hint_query: None,
            incremental: None,
            cursor: None,
            tuning: &tuning_300s(),
            column_overrides: &ColumnOverrides::default(),
            page_limit: None,
        },
        &mut FailOnFirstBatch,
    );
    assert!(result.is_err(), "export should have failed via sink error");

    // pool_size=1 → same physical connection after ROLLBACK.
    let mut check = PgClient::connect(PGBOUNCER_URL, NoTls).unwrap();

    // Must be able to run queries — no open transaction blocking the connection.
    let alive: i32 = check
        .query_one("SELECT 42 AS alive", &[])
        .expect("connection stuck after failed export — ROLLBACK was not issued")
        .get(0);
    assert_eq!(alive, 42);

    // statement_timeout must be back to the server default.
    let timeout = pg_show_statement_timeout(&mut check);
    assert_eq!(
        timeout, "0",
        "statement_timeout leaked after failed export: got {timeout:?}"
    );
}

/// **G1 / PgTxnGuard regression test.** Exports against a plain Postgres
/// connection with a sink that panics on the first batch. The panic unwinds
/// through `pg_run_export`; only the RAII `PgTxnGuard::drop` can issue
/// ROLLBACK before the txn leaks into the connection's session state.
///
/// We then reuse the same `PostgresSource` for a follow-up query — if the
/// guard had not rolled back, Postgres would reject the next statement with
/// `current transaction is aborted` or block on an open BEGIN.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn pg_panic_in_sink_releases_cursor_and_aborts_txn() {
    use std::panic::AssertUnwindSafe;

    require_alive(LiveService::Postgres);

    let mut source = PostgresSource::connect(POSTGRES_URL).unwrap();

    let panicked = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = source.export(
            &ExportRequest {
                query: "SELECT generate_series(1, 100) AS n",
                catalog_hint_query: None,
                incremental: None,
                cursor: None,
                tuning: &tuning_300s(),
                column_overrides: &ColumnOverrides::default(),
                page_limit: None,
            },
            &mut PanicOnFirstBatch,
        );
    }));
    assert!(
        panicked.is_err(),
        "sink panic must have unwound through pg_run_export"
    );

    // If PgTxnGuard::drop fired ROLLBACK, the same source's underlying client
    // is back in autocommit and this scalar query succeeds. If the guard is
    // absent the connection is stuck in an open or aborted txn.
    let v = source
        .query_scalar("SELECT 1::text")
        .expect("connection is unusable — PgTxnGuard did not roll back on panic");
    assert_eq!(v.as_deref(), Some("1"));
}

// ─── MySQL tests ───────────────────────────────────────────────────────────

fn single_conn_pool() -> mysql::Pool {
    use mysql::{Opts, OptsBuilder, PoolConstraints, PoolOpts};
    let opts = Opts::from(
        OptsBuilder::from_opts(Opts::from_url(MYSQL_URL).unwrap()).pool_opts(
            PoolOpts::default()
                .with_constraints(PoolConstraints::new(1, 1).expect("valid pool constraints")),
        ),
    );
    mysql::Pool::new(opts).expect("create single-conn mysql pool")
}

fn mysql_session_time_zone(pool: &mysql::Pool) -> String {
    use mysql::prelude::*;
    let mut conn = pool.get_conn().unwrap();
    conn.query_first::<String, _>("SELECT @@session.time_zone")
        .unwrap()
        .unwrap()
}

fn mysql_session_max_execution_time(pool: &mysql::Pool) -> u64 {
    use mysql::prelude::*;
    let mut conn = pool.get_conn().unwrap();
    conn.query_first::<u64, _>("SELECT @@session.max_execution_time")
        .unwrap()
        .unwrap()
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn mysql_session_vars_clean_after_successful_export() {
    require_alive(LiveService::Mysql);

    let pool = single_conn_pool();
    let mut source = MysqlSource::from_pool(pool.clone());

    source
        .export(
            &ExportRequest {
                query: "SELECT 1 AS n",
                catalog_hint_query: None,
                incremental: None,
                cursor: None,
                tuning: &tuning_300s(),
                column_overrides: &ColumnOverrides::default(),
                page_limit: None,
            },
            &mut NullSink,
        )
        .expect("export should succeed");

    // pool_size=1 → same physical connection; cleanup must have run.
    let tz = mysql_session_time_zone(&pool);
    assert_ne!(
        tz, "+00:00",
        "time_zone leaked to pool after successful export: got {tz:?}"
    );

    let timeout = mysql_session_max_execution_time(&pool);
    assert_eq!(
        timeout, 0,
        "max_execution_time leaked to pool after successful export: got {timeout}"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn mysql_session_vars_clean_after_failed_export() {
    require_alive(LiveService::Mysql);

    let pool = single_conn_pool();
    let mut source = MysqlSource::from_pool(pool.clone());

    let result = source.export(
        &ExportRequest {
            query: "SELECT 1 AS n UNION ALL SELECT 2",
            catalog_hint_query: None,
            incremental: None,
            cursor: None,
            tuning: &tuning_300s(),
            column_overrides: &ColumnOverrides::default(),
            page_limit: None,
        },
        &mut FailOnFirstBatch,
    );
    assert!(result.is_err(), "export should have failed via sink error");

    // pool_size=1 → same physical connection; cleanup must have run despite error.
    let tz = mysql_session_time_zone(&pool);
    assert_ne!(
        tz, "+00:00",
        "time_zone leaked to pool after failed export: got {tz:?}"
    );

    let timeout = mysql_session_max_execution_time(&pool);
    assert_eq!(
        timeout, 0,
        "max_execution_time leaked to pool after failed export: got {timeout}"
    );
}

// ─── MySQL / ProxySQL tests ────────────────────────────────────────────────
//
// All ProxySQL tests gate on both the `mysql` service (the backend) and the
// `proxysql` service from the `pool` profile.  The `MysqlProxyKind::ProxySql`
// classification is the primary contract here; cleanup-through-the-proxy is
// the regression test for the existing session-state guarantee.

/// Returns a `Pool` configured exactly like the production `MysqlSource`'s
/// `connect()` path but pointed at ProxySQL.  Used by the proxy-detection
/// tests so we exercise the *same* signal collection code that runs in
/// production, not a one-off ad-hoc pool.
fn proxysql_default_pool() -> mysql::Pool {
    use mysql::{Opts, OptsBuilder, PoolConstraints, PoolOpts};
    // PoolConstraints::new(1,1) means: even if the ProxySQL multiplexes
    // backend connections, our *client-side* pool keeps a single conn open
    // so post-export inspection sees the same client→ProxySQL session.
    let opts = Opts::from(
        OptsBuilder::from_opts(Opts::from_url(PROXYSQL_URL).unwrap()).pool_opts(
            PoolOpts::default()
                .with_constraints(PoolConstraints::new(1, 1).expect("valid pool constraints")),
        ),
    );
    mysql::Pool::new(opts).expect("create single-conn proxysql client pool")
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d proxysql"]
fn mysql_proxysql_connection_classified_as_proxysql() {
    // The decisive test: `MysqlSource::connect()` returns a source whose
    // `proxy_kind()` is `ProxySql`.  This fails loudly if either
    //   (a) the classifier regresses (banner/proxy_version signals broken), or
    //   (b) the ProxySQL config we ship in `dev/proxysql/proxysql.cnf` stops
    //       advertising the ProxySQL banner.
    require_alive(LiveService::Mysql);
    require_alive(LiveService::ProxySql);

    let source = MysqlSource::connect(PROXYSQL_URL).expect("connect through ProxySQL");
    assert_eq!(
        source.proxy_kind(),
        MysqlProxyKind::ProxySql,
        "ProxySQL connection must be classified as ProxySql; got {:?}",
        source.proxy_kind()
    );
    assert!(
        source.proxy_kind().is_proxy(),
        "is_proxy() must be true for a ProxySQL connection"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mysql (direct backend, no ProxySQL)"]
fn mysql_direct_connection_classified_as_direct() {
    // Pair test for `mysql_proxysql_connection_classified_as_proxysql`: a
    // direct MySQL connection must NOT be misclassified as a proxy.  This is
    // the false-positive guard for the new CONNECTION_ID() check —
    // single-conn pools to a direct backend always reuse the same physical
    // backend, so `cid1 == cid2` and the classifier returns `Direct`.
    require_alive(LiveService::Mysql);

    let source = MysqlSource::connect(MYSQL_URL).expect("connect directly to MySQL");
    assert_eq!(
        source.proxy_kind(),
        MysqlProxyKind::Direct,
        "direct MySQL connection must be classified as Direct; got {:?}",
        source.proxy_kind()
    );
    assert!(
        !source.proxy_kind().is_proxy(),
        "is_proxy() must be false for a direct connection"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mssql (direct backend, no proxy)"]
fn mssql_direct_connection_classified_as_direct() {
    // SQL Server analogue of `mysql_direct_connection_classified_as_direct`: a
    // direct connection to the dev SQL Server container must NOT be
    // misclassified. `@@SPID` is stable across queries on one connection, and
    // the dev image reports an on-prem EngineEdition (Developer = 3), so the
    // classifier returns `Direct`. No open-source SQL Server statement-level
    // multiplexer exists to exercise the `Multiplexed` path live; that variant
    // (and `AzureGateway`) is covered by the pure-classifier unit tests in
    // `src/source/mssql/proxy.rs`.
    require_alive(LiveService::Mssql);

    let source =
        MssqlSource::connect_with_tls(MSSQL_URL, None).expect("connect directly to SQL Server");
    assert_eq!(
        source.proxy_kind(),
        MssqlProxyKind::Direct,
        "direct SQL Server connection must be classified as Direct; got {:?}",
        source.proxy_kind()
    );
    assert!(
        !source.proxy_kind().is_proxy(),
        "is_proxy() must be false for a direct connection"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d proxysql"]
fn mysql_proxysql_session_vars_clean_after_successful_export() {
    // Mirror of `mysql_session_vars_clean_after_successful_export` but
    // through ProxySQL.  Verifies the cleanup code path executes regardless
    // of whether the export traffic is direct or proxied — there must be no
    // silent fallback that skips cleanup on a proxied connection.
    require_alive(LiveService::Mysql);
    require_alive(LiveService::ProxySql);

    let pool = proxysql_default_pool();
    let mut source = MysqlSource::from_pool(pool.clone());
    assert_eq!(
        source.proxy_kind(),
        MysqlProxyKind::ProxySql,
        "pool→ProxySQL must classify as ProxySql"
    );

    source
        .export(
            &ExportRequest {
                query: "SELECT 1 AS n",
                catalog_hint_query: None,
                incremental: None,
                cursor: None,
                tuning: &tuning_300s(),
                column_overrides: &ColumnOverrides::default(),
                page_limit: None,
            },
            &mut NullSink,
        )
        .expect("export through ProxySQL should succeed");

    // ProxySQL with transaction_persistent=1 keeps the same backend conn
    // throughout a transaction, so the post-export check sees the same
    // session state the export left behind.  Cleanup must have run.
    let tz = mysql_session_time_zone(&pool);
    assert_ne!(
        tz, "+00:00",
        "time_zone leaked through ProxySQL after successful export: got {tz:?}"
    );

    let timeout = mysql_session_max_execution_time(&pool);
    assert_eq!(
        timeout, 0,
        "max_execution_time leaked through ProxySQL after successful export: got {timeout}"
    );
}

#[test]
#[ignore = "live: requires docker compose --profile pool up -d proxysql"]
fn mysql_proxysql_session_vars_clean_after_failed_export() {
    // Failure-path analog of the above: cleanup must run on the error path
    // too when the connection is proxied.
    require_alive(LiveService::Mysql);
    require_alive(LiveService::ProxySql);

    let pool = proxysql_default_pool();
    let mut source = MysqlSource::from_pool(pool.clone());
    assert_eq!(source.proxy_kind(), MysqlProxyKind::ProxySql);

    let result = source.export(
        &ExportRequest {
            query: "SELECT 1 AS n UNION ALL SELECT 2",
            catalog_hint_query: None,
            incremental: None,
            cursor: None,
            tuning: &tuning_300s(),
            column_overrides: &ColumnOverrides::default(),
            page_limit: None,
        },
        &mut FailOnFirstBatch,
    );
    assert!(
        result.is_err(),
        "export should have failed via injected sink error"
    );

    let tz = mysql_session_time_zone(&pool);
    assert_ne!(
        tz, "+00:00",
        "time_zone leaked through ProxySQL after failed export: got {tz:?}"
    );

    let timeout = mysql_session_max_execution_time(&pool);
    assert_eq!(
        timeout, 0,
        "max_execution_time leaked through ProxySQL after failed export: got {timeout}"
    );
}
