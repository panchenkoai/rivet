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
async fn try_connect_at(port: u16) -> Result<Client<Compat<TcpStream>>, String> {
    let mut config = Config::new();
    config.host("127.0.0.1");
    config.port(port);
    config.database("rivet");
    config.authentication(AuthMethod::sql_server("sa", "Rivet_Passw0rd!"));
    config.encryption(EncryptionLevel::Required);
    config.trust_cert();
    let tcp = TcpStream::connect(config.get_addr())
        .await
        .map_err(|e| format!("mssql: tcp connect (is the service up?): {e}"))?;
    tcp.set_nodelay(true).ok();
    Client::connect(config, tcp.compat_write())
        .await
        .map_err(|e| format!("mssql: login: {e}"))
}

async fn connect_at(port: u16) -> Client<Compat<TcpStream>> {
    match try_connect_at(port).await {
        Ok(c) => c,
        Err(e) => panic!("{e}"),
    }
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

/// Fully error-tolerant executor: returns `true` only if the connection AND
/// every batch succeeded, `false` on ANY failure (connect, login, a batch
/// error). Unlike [`try_exec_at`] it also absorbs the CONNECT half.
///
/// For BACKGROUND LOAD threads, which are not fixture setup: they run hundreds
/// of fresh logins under load they create themselves, so a single transient
/// connect failure or deadlock-victim error must not abort the thread and turn
/// the test's verdict into `writer thread` — that verdict skips every real
/// assertion AND the scratch-table cleanup. The returned bool is what keeps the
/// tolerance honest: the caller counts SUCCESSES, so a writer that absorbs
/// every statement is still detectable (a silent writer is exactly the inert
/// fixture the activation guards exist to catch).
fn soft_exec_at(port: u16, sql: &str) -> bool {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("mssql: tokio runtime");
    rt.block_on(async {
        let Ok(mut client) = try_connect_at(port).await else {
            return false;
        };
        for batch in split_go(sql) {
            if batch.trim().is_empty() {
                continue;
            }
            let Ok(stream) = client.simple_query(batch.as_str()).await else {
                return false;
            };
            if stream.into_results().await.is_err() {
                return false;
            }
        }
        true
    })
}

fn exec_at(port: u16, sql: &str) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("mssql: tokio runtime");
    // Bounded retry: under the full E2E matrix the SQL Server container drops
    // connections / times out queries mid-batch (a transient the loaded runner
    // hits — two setup flakes: wait_for_capture, then this exec). These are
    // test-SETUP batches (seed / CDC-enable); seeds are DROP-first idempotent,
    // and a DETERMINISTIC error (bad SQL, permission) still surfaces after the
    // last attempt rather than being masked. Re-establishes the connection
    // each try. r7/r8: the mssql-under-load class.
    rt.block_on(async {
        let mut last_err: Option<String> = None;
        for attempt in 0..3 {
            match run_batch(port, sql).await {
                Ok(()) => return,
                Err(e) => {
                    last_err = Some(e);
                    tokio::time::sleep(std::time::Duration::from_millis(500 * (attempt + 1))).await;
                }
            }
        }
        panic!(
            "mssql: exec batch failed after 3 attempts: {}",
            last_err.unwrap()
        );
    });
}

/// One connect + batch execution, fallible (so [`exec_at`] can retry a
/// transient transport failure under load).
async fn run_batch(port: u16, sql: &str) -> Result<(), String> {
    let mut client = try_connect_at(port).await.map_err(|e| e.to_string())?;
    for batch in split_go(sql) {
        if batch.trim().is_empty() {
            continue;
        }
        client
            .simple_query(batch.as_str())
            .await
            .map_err(|e| format!("exec: {e}"))?
            .into_results()
            .await
            .map_err(|e| format!("drain: {e}"))?;
    }
    Ok(())
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
        // COUNT(*) is int, but SUM/MIN/MAX over a BIGINT column come back as
        // bigint — accept either width.
        match row.try_get::<i64, _>(0) {
            Ok(Some(v)) => v,
            _ => i64::from(row.get::<i32, _>(0).unwrap_or(0)),
        }
    })
}

/// Run T-SQL against the shared `mssql` (`:1433`). `GO`-delimited batches run in
/// order; statements within a batch run together. Panics on error (test setup).
pub fn mssql_exec(sql: &str) {
    exec_at(1433, sql)
}

/// Error-tolerant twin of [`mssql_exec`] for BACKGROUND LOAD threads against
/// the shared `mssql` (`:1433`) — `true` when the statement really ran. See
/// [`soft_exec_at`] for why a load loop must not use the panicking executor.
pub fn mssql_try_exec(sql: &str) -> bool {
    soft_exec_at(1433, sql)
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

/// The governor instance's twins (`mssql-governor`, :1435). Kept beside their
/// `:1433` siblings so a reader sees the two servers are DIFFERENT servers, not
/// two spellings of one — the whole point of the split is that
/// `Log Flush Waits/sec (_Total)` is server-wide.
pub fn mssql_governor_exec(sql: &str) {
    exec_at(1435, sql)
}

pub fn mssql_governor_try_exec(sql: &str) -> bool {
    soft_exec_at(1435, sql)
}

pub fn mssql_governor_query_i64(sql: &str) -> i64 {
    query_i64_at(1435, sql)
}

/// Block until the governor instance's transaction log has stopped accumulating
/// flush waits, so an "idle source" assertion grades the RUN rather than the
/// fixture's own seed still draining.
///
/// Despite the name, `Log Flush Waits/sec` is a CUMULATIVE counter — two equal
/// consecutive reads mean no wait was recorded in between, which is exactly the
/// condition the no-shed canaries assert holds during their runs. Seeding 40k
/// rows in 1000-row batches leaves the log draining for as long as the host's
/// disk needs: under a second on a laptop, but long enough on CI's disk that the
/// governor (correctly) shed for pressure the test itself had created. Giving
/// the shared instance its own container fixed the SIBLING half of that; this
/// fixes the half the fixture creates for itself.
///
/// Panics rather than proceeding when the log never settles: a bounded wait that
/// gives up quietly is a `sleep` wearing a loop, and would hand the caller the
/// same unestablished precondition it was called to establish.
pub fn wait_for_quiet_mssql_governor_log() {
    const FLUSH_WAITS: &str = "SELECT cntr_value FROM sys.dm_os_performance_counters \
                               WHERE counter_name LIKE 'Log Flush Waits%' \
                               AND instance_name = '_Total'";
    /// Equal reads required in a row (i.e. two consecutive quiet intervals).
    const SETTLED_READS: usize = 3;
    const INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);
    const MAX_ATTEMPTS: usize = 60; // ~30s ceiling

    let started = std::time::Instant::now();
    let mut last = mssql_governor_query_i64(FLUSH_WAITS);
    let mut equal_runs = 1usize;
    for _ in 0..MAX_ATTEMPTS {
        std::thread::sleep(INTERVAL);
        let now = mssql_governor_query_i64(FLUSH_WAITS);
        if now == last {
            equal_runs += 1;
            if equal_runs >= SETTLED_READS {
                // Report the wait: on a fast disk the log is already quiet and
                // this returns at the floor, which is exactly why the CI-only
                // failure was invisible locally. Printing it means a future
                // reader can tell a firing wait from an inert one.
                eprintln!(
                    "governor log settled after {:.1}s (flush waits steady at {last})",
                    started.elapsed().as_secs_f64()
                );
                return;
            }
        } else {
            equal_runs = 1;
        }
        last = now;
    }
    panic!(
        "the governor instance's log never went quiet within {}s (Log Flush Waits still \
         advancing, last={last}) — an idle-source assertion taken now would grade the \
         fixture's own seed, not the run",
        (MAX_ATTEMPTS as u64 * INTERVAL.as_millis() as u64) / 1000
    );
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
    /// The instance the table LIVES on — Drop targets this port, not a
    /// hardcoded :1433 (r4 bughunt: governor-instance fixtures — a 20k-row
    /// seed + a multi-hundred-MB VARBINARY scratch per canary run — were
    /// "dropped" on the wrong server and accumulated unboundedly).
    port: u16,
}

impl MssqlTable {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Wrap an already-created table (custom schema) in the RAII drop guard.
    pub fn adopt(name: String) -> Self {
        Self::adopt_at(1433, name)
    }

    /// Adopt a table on a NON-primary instance (mssql-governor :1435).
    pub fn adopt_at(port: u16, name: String) -> Self {
        MssqlTable { name, port }
    }
}

impl Drop for MssqlTable {
    fn drop(&mut self) {
        // Best-effort, deliberately NOT [`mssql_drop_table`]: this Drop runs
        // while the test may already be UNWINDING from a failed assertion, and
        // a panicking executor there aborts the process — replacing a readable
        // assertion message with a SIGABRT. Cleanup is not the verdict.
        let name = &self.name;
        soft_exec_at(
            self.port,
            &format!("IF OBJECT_ID('{name}','U') IS NOT NULL DROP TABLE {name}"),
        );
    }
}

/// Seed a `(id BIGINT PK, name NVARCHAR(100), amount DECIMAL(12,2),
/// created_at DATETIME2)` SQL Server table with `row_count` rows — the SQL
/// Server twin of [`super::mysql::seed_mysql_numeric_table`]. Rows are
/// `id`-ordered `0..row_count` with `amount = id * 1.5` and a descending
/// `created_at`, matching the MySQL/PG seeders so the same export queries and
/// row-count assertions hold across engines.
pub fn seed_mssql_numeric_table(row_count: i64) -> MssqlTable {
    seed_mssql_numeric_table_at(1433, row_count)
}

/// Seed the same fixture on the GOVERNOR instance (`mssql-governor`, :1435).
///
/// The concurrency-governor canaries need a SQL Server nobody else is writing
/// to: the signal they assert on is `Log Flush Waits/sec` with the `_Total`
/// instance, which is server-wide. See `env::MSSQL_GOVERNOR_URL`.
pub fn seed_mssql_governor_numeric_table(row_count: i64) -> MssqlTable {
    seed_mssql_numeric_table_at(1435, row_count)
}

fn seed_mssql_numeric_table_at(port: u16, row_count: i64) -> MssqlTable {
    let name = unique_name("rivet_qa_tbl");
    exec_at(
        port,
        &format!("IF OBJECT_ID('{name}','U') IS NOT NULL DROP TABLE {name}"),
    );
    exec_at(
        port,
        &format!(
            "CREATE TABLE {name} (
            id BIGINT PRIMARY KEY,
            name NVARCHAR(100) NOT NULL,
            amount DECIMAL(12,2) NOT NULL,
            created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );"
        ),
    );
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
            exec_at(port, &sql);
            start = end;
        }
    }
    MssqlTable::adopt_at(port, name)
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

/// Every row's first column as a `String`, against the CDC `mssql-cdc` (`:1434`).
///
/// For DERIVING an enumeration from the catalog rather than re-typing it in a
/// test — a hand-written column list grades only what its author remembered, and
/// silently stops covering a column the fixture gains later.
pub fn mssql_cdc_query_strings(sql: &str) -> Vec<String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("mssql: tokio runtime");
    rt.block_on(async {
        let mut client = connect_at(1434).await;
        let rows = client
            .simple_query(sql)
            .await
            .expect("mssql: query")
            .into_first_result()
            .await
            .expect("mssql: rows");
        rows.iter()
            .filter_map(|r| r.get::<&str, _>(0).map(String::from))
            .collect()
    })
}

/// Seed ONE transaction spanning `ids` on SQL Server — a scenario, not a `format!`
/// in a test body.
///
/// A single set-based `INSERT … SELECT`, which is ONE statement and therefore one
/// transaction: every row lands in a single `__$start_lsn` group, which is exactly
/// what an oversized-transaction fixture needs. N separate one-row inserts are N
/// groups and never reach a per-transaction cap however many of them there are.
///
/// Shaped for the DuckDB census and [`crate::common::cdc_id_ops`] — `(id, v)`
/// integers plus a wide `pad`, so a row cap and a BYTE cap are both reachable and
/// the source table's row count equals the delivered change count.
pub fn mssql_seed_one_transaction(table: &str, ids: std::ops::RangeInclusive<usize>) {
    mssql_seed_one_transaction_wide(table, ids, 200);
}

/// [`mssql_seed_one_transaction`] with the `pad` width chosen — the HEAVY-row
/// fixture, which crosses the BYTE cap where a narrow one only crosses the row cap.
///
/// `REPLICATE` past 8000 needs an explicit `VARCHAR(MAX)` operand, or it silently
/// truncates at 8000 — a wide fixture that quietly stops being wide would report a
/// byte cap that was never crossed.
pub fn mssql_seed_one_transaction_wide(
    table: &str,
    ids: std::ops::RangeInclusive<usize>,
    pad: usize,
) {
    let (lo, hi) = (*ids.start(), *ids.end());
    let n = hi.saturating_sub(lo) + 1;
    mssql_cdc_exec(&format!(
        "INSERT INTO dbo.{table} (id, v, pad) \
         SELECT {lo} + q.n - 1, {lo} + q.n - 1, REPLICATE(CAST('x' AS VARCHAR(MAX)), {pad}) \
         FROM ( \
             SELECT TOP ({n}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n \
             FROM sys.all_objects a CROSS JOIN sys.all_objects b \
         ) q"
    ));
}

// ─── CDC scenarios, shared by the live suite and the soak stand ──────────────
//
// Private to `live_cdc_mssql.rs` until the soak stand needed them. A scenario that
// two suites need is harness, not test body — the same rule that put the transaction
// seeds here.

/// Enable CDC on the database (idempotent) + the table, creating capture instance
/// `ci`. The capture job (SQL Server Agent) then populates `cdc.<ci>_CT`.
pub fn enable_cdc(table: &str, ci: &str) {
    mssql_cdc_exec(
        "IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE name='rivet' AND is_cdc_enabled=1) \
         EXEC sys.sp_cdc_enable_db;",
    );
    mssql_cdc_exec(&format!(
        "EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'{table}', \
         @role_name=NULL, @capture_instance=N'{ci}';"
    ));
}

/// Block until the capture job has copied at least `want` rows into the change
/// table — the job runs asynchronously, so the test must wait for it.
pub fn wait_for_capture(ci: &str, want: i64) {
    // 60s ceiling: the SQL Server Agent capture job is asynchronous and its
    // scan interval stretches under a loaded E2E runner — a 30s bound flaked
    // one test at ~456s suite wall-clock (r6 CI). Doubling the ceiling matches
    // the async reality; it does NOT mask a bug (a real drop still times out).
    for _ in 0..120 {
        if mssql_cdc_query_i64(&format!("SELECT COUNT(*) FROM cdc.{ci}_CT")) >= want {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    panic!("capture job did not populate cdc.{ci}_CT to {want} rows in 60s");
}
