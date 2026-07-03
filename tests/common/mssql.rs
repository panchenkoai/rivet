//! MSSQL test helpers: a tiny tiberius-backed executor for fixture setup.
//!
//! Mirrors [`super::mysql`] / [`super::pg`] but the SQL Server driver is async,
//! so each call spins a current-thread runtime and `block_on`s — fixture setup
//! is infrequent, so a per-call runtime is fine. Connection parameters match
//! the `mssql` service in `docker-compose.yaml`.

#![allow(dead_code)]

use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use super::unique_name;

/// Connect to a SQL Server instance on `port` — `:1433` is the shared `mssql`
/// service, `:1434` is the CDC-configured `mssql-cdc` (cdc profile).
async fn connect_at(port: u16) -> Client<Compat<TcpStream>> {
    let mut config = Config::new();
    config.host("127.0.0.1");
    config.port(port);
    config.database("rivet");
    config.authentication(AuthMethod::sql_server("sa", "Rivet_Passw0rd!"));
    config.encryption(EncryptionLevel::Required);
    config.trust_cert();
    let tcp = TcpStream::connect(config.get_addr())
        .await
        .expect("mssql: tcp connect (is the service up?)");
    tcp.set_nodelay(true).ok();
    Client::connect(config, tcp.compat_write())
        .await
        .expect("mssql: login")
}

/// Like `exec_at`, but tolerates server errors — for Agent job control, whose
/// errors (22022 "already running"/"not running") are raised by the Agent
/// process outside any T-SQL TRY/CATCH reach.
fn try_exec_at(port: u16, sql: &str) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("mssql: tokio runtime");
    rt.block_on(async {
        let mut client = connect_at(port).await;
        for batch in split_go(sql) {
            if batch.trim().is_empty() {
                continue;
            }
            if let Ok(r) = client.simple_query(batch.as_str()).await {
                let _ = r.into_results().await;
            }
        }
    });
}

fn exec_at(port: u16, sql: &str) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("mssql: tokio runtime");
    rt.block_on(async {
        let mut client = connect_at(port).await;
        for batch in split_go(sql) {
            if batch.trim().is_empty() {
                continue;
            }
            client
                .simple_query(batch.as_str())
                .await
                .expect("mssql: exec batch")
                .into_results()
                .await
                .expect("mssql: drain batch");
        }
    });
}

fn query_i64_at(port: u16, sql: &str) -> i64 {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("mssql: tokio runtime");
    rt.block_on(async {
        let mut client = connect_at(port).await;
        let row = client
            .simple_query(sql)
            .await
            .expect("mssql: query")
            .into_row()
            .await
            .expect("mssql: row")
            .expect("mssql: at least one row");
        i64::from(row.get::<i32, _>(0).unwrap_or(0))
    })
}

/// Run T-SQL against the shared `mssql` (`:1433`). `GO`-delimited batches run in
/// order; statements within a batch run together. Panics on error (test setup).
pub fn mssql_exec(sql: &str) {
    exec_at(1433, sql)
}

/// As [`mssql_exec`], but against the CDC `mssql-cdc` instance (`:1434`).
pub fn mssql_cdc_exec(sql: &str) {
    exec_at(1434, sql)
}

/// Error-tolerant twin of [`mssql_cdc_exec`] for Agent job control.
pub fn mssql_cdc_try_exec(sql: &str) {
    try_exec_at(1434, sql)
}

/// Scalar query → first column of the first row as `i64`, against the shared
/// `mssql` (`:1433`).
pub fn mssql_query_i64(sql: &str) -> i64 {
    query_i64_at(1433, sql)
}

/// As [`mssql_query_i64`], but against `mssql-cdc` (`:1434`) — e.g. polling a CDC
/// change table's row count while the capture job catches up.
pub fn mssql_cdc_query_i64(sql: &str) -> i64 {
    query_i64_at(1434, sql)
}

/// Run a query whose `cols` columns are all `BIGINT` and return the first row as
/// `i64`s — for multi-aggregate fingerprints (each aggregate `CAST(... AS BIGINT)`
/// in SQL, since [`mssql_query_i64`] only reads a single `INT` column). Shared
/// `mssql` (`:1433`).
pub fn mssql_query_bigints(sql: &str, cols: usize) -> Vec<i64> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("mssql: tokio runtime");
    rt.block_on(async {
        let mut client = connect_at(1433).await;
        let row = client
            .simple_query(sql)
            .await
            .expect("mssql: query")
            .into_row()
            .await
            .expect("mssql: row")
            .expect("mssql: at least one row");
        (0..cols)
            .map(|i| row.get::<i64, _>(i).unwrap_or(0))
            .collect()
    })
}

/// Idempotent table drop for RAII cleanup guards (shared `mssql`).
pub fn mssql_drop_table(name: &str) {
    mssql_exec(&format!(
        "IF OBJECT_ID('{name}','U') IS NOT NULL DROP TABLE {name}"
    ));
}

/// Idempotent table drop against `mssql-cdc` (`:1434`).
pub fn mssql_cdc_drop_table(name: &str) {
    mssql_cdc_exec(&format!(
        "IF OBJECT_ID('{name}','U') IS NOT NULL DROP TABLE {name}"
    ));
}

/// A seeded SQL Server table that drops itself on `Drop` (RAII) — the SQL
/// Server twin of [`super::mysql::MysqlTable`].
pub struct MssqlTable {
    name: String,
}

impl MssqlTable {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Wrap an already-created table (custom schema) in the RAII drop guard.
    pub fn adopt(name: String) -> Self {
        MssqlTable { name }
    }
}

impl Drop for MssqlTable {
    fn drop(&mut self) {
        mssql_drop_table(&self.name);
    }
}

/// Seed a `(id BIGINT PK, name NVARCHAR(100), amount DECIMAL(12,2),
/// created_at DATETIME2)` SQL Server table with `row_count` rows — the SQL
/// Server twin of [`super::mysql::seed_mysql_numeric_table`]. Rows are
/// `id`-ordered `0..row_count` with `amount = id * 1.5` and a descending
/// `created_at`, matching the MySQL/PG seeders so the same export queries and
/// row-count assertions hold across engines.
pub fn seed_mssql_numeric_table(row_count: i64) -> MssqlTable {
    let name = unique_name("rivet_qa_tbl");
    mssql_drop_table(&name);
    mssql_exec(&format!(
        "CREATE TABLE {name} (
            id BIGINT PRIMARY KEY,
            name NVARCHAR(100) NOT NULL,
            amount DECIMAL(12,2) NOT NULL,
            created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );"
    ));
    if row_count > 0 {
        // T-SQL caps a multi-row VALUES clause at 1000 rows per INSERT — chunk.
        let mut start = 0;
        while start < row_count {
            let end = (start + 1000).min(row_count);
            let mut sql = format!("INSERT INTO {name} (id, name, amount, created_at) VALUES ");
            for i in start..end {
                if i > start {
                    sql.push_str(", ");
                }
                sql.push_str(&format!(
                    "({i}, 'row_{i}', {:.2}, DATEADD(SECOND, -{}, SYSUTCDATETIME()))",
                    (i as f64) * 1.5,
                    row_count - i
                ));
            }
            mssql_exec(&sql);
            start = end;
        }
    }
    MssqlTable { name }
}

/// Split a script on lines that are exactly `GO` (the sqlcmd batch separator,
/// not a T-SQL keyword) so each batch can be submitted independently.
fn split_go(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for line in sql.lines() {
        if line.trim().eq_ignore_ascii_case("GO") {
            out.push(std::mem::take(&mut cur));
        } else {
            cur.push_str(line);
            cur.push('\n');
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur);
    }
    out
}
