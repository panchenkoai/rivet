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
//! cargo test --test live_pool_safety -- --include-ignored
//! ```
//!
//! ## MySQL tests
//!
//! Use a single-connection pool (PoolConstraints::new(1,1)) so the same
//! physical MySQL connection is reused after export.
//!
//! ```text
//! docker compose up -d mysql
//! cargo test --test live_pool_safety -- --include-ignored
//! ```

mod common;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use postgres::{Client as PgClient, NoTls};

use common::*;
use rivet::error::Result;
use rivet::source::mysql::MysqlSource;
use rivet::source::postgres::PostgresSource;
use rivet::source::{BatchSink, Source};
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

// ─── Helpers ───────────────────────────────────────────────────────────────

fn tuning_300s() -> SourceTuning {
    let mut cfg = TuningConfig::default();
    cfg.statement_timeout_s = Some(300);
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
            "SELECT 1 AS n",
            None,
            None,
            &tuning_300s(),
            &ColumnOverrides::default(),
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
        "SELECT generate_series(1, 1000) AS n",
        None,
        None,
        &tuning_300s(),
        &ColumnOverrides::default(),
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
            "SELECT 1 AS n",
            None,
            None,
            &tuning_300s(),
            &ColumnOverrides::default(),
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
        "SELECT 1 AS n UNION ALL SELECT 2",
        None,
        None,
        &tuning_300s(),
        &ColumnOverrides::default(),
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
