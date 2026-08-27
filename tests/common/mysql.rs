//! MySQL test helpers: connection, RAII table guard, canonical seeder.
//!
//! Schemas mirror the Postgres helpers in [`super::pg`] so cross-dialect parity
//! tests can assert byte-equivalent exports.

#![allow(dead_code)]

use super::env::MYSQL_URL;
use super::unique_name;

/// Open a fresh MySQL connection to the primary instance.
pub fn mysql_connect() -> mysql::PooledConn {
    let pool = mysql::Pool::new(MYSQL_URL).expect("mysql pool");
    pool.get_conn().expect("connect to mysql")
}

/// RAII handle that drops the MySQL table on test exit.
pub struct MysqlTable {
    name: String,
}

impl MysqlTable {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Wrap an already-created table (custom schema) in the RAII drop guard, so
    /// tests needing a non-canonical shape don't roll their own `DropMysqlTable`.
    pub fn adopt(name: String) -> Self {
        MysqlTable { name }
    }
}

impl Drop for MysqlTable {
    fn drop(&mut self) {
        use mysql::prelude::Queryable;
        if let Ok(pool) = mysql::Pool::new(MYSQL_URL)
            && let Ok(mut c) = pool.get_conn()
        {
            let _ = c.query_drop(format!("DROP TABLE IF EXISTS {}", self.name));
        }
    }
}

/// MySQL analogue of `seed_pg_numeric_table` — same logical schema so parity
/// tests can assert identical exports across both dialects.
pub fn seed_mysql_numeric_table(row_count: i64) -> MysqlTable {
    use mysql::prelude::Queryable;

    let name = unique_name("rivet_qa_tbl");
    let mut c = mysql_connect();
    c.query_drop(format!(
        "CREATE TABLE {name} (
            id BIGINT PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            amount DECIMAL(12,2) NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB;"
    ))
    .expect("create mysql test table");

    if row_count > 0 {
        let mut sql = format!("INSERT INTO {name} (id, name, amount, created_at) VALUES ");
        for i in 0..row_count {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!(
                "({i}, 'row_{i}', {:.2}, DATE_SUB(NOW(), INTERVAL {} SECOND))",
                (i as f64) * 1.5,
                row_count - i
            ));
        }
        c.query_drop(sql).expect("seed mysql rows");
    }
    MysqlTable { name }
}

/// RAII drop-guard for a table on the mysql-cdc instance. (Was five identical
/// local structs across the live CDC files.)
pub struct MysqlCdcTable(pub String);
impl Drop for MysqlCdcTable {
    fn drop(&mut self) {
        use mysql::prelude::Queryable as _;
        if let Ok(pool) = mysql::Pool::new(super::env::MYSQL_CDC_URL)
            && let Ok(mut c) = pool.get_conn()
        {
            let _ = c.query_drop(format!("DROP TABLE IF EXISTS {}", self.0));
        }
    }
}

/// RAII cross-process lock serializing tests that either FLIP server-wide
/// MySQL tmp-table globals (`internal_tmp_mem_storage_engine`,
/// `tmp_table_size`) or measure WALL-CLOCK ratios against the shared MySQL
/// server. Cargo runs integration binaries in parallel, so a flipped global
/// can slow an unrelated test's queries into flakiness, and a heavy sibling
/// can skew an A/B timing bound (bughunt 2026-08-13). Same advisory
/// `flock(2)` shape as `toxiproxy_guard` — the kernel releases it on drop,
/// panic included.
pub struct MysqlGlobalsGuard {
    _file: std::fs::File,
}

pub fn mysql_globals_guard() -> MysqlGlobalsGuard {
    use std::os::unix::io::AsRawFd;
    let path = std::env::temp_dir().join("rivet_qa_mysql_globals.lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .unwrap_or_else(|e| panic!("open mysql globals lock {}: {e}", path.display()));
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    if rc != 0 {
        panic!(
            "flock(LOCK_EX) on {} failed: {}",
            path.display(),
            std::io::Error::last_os_error()
        );
    }
    MysqlGlobalsGuard { _file: file }
}

/// RAII guard for a MySQL GLOBAL variable — restores the PREVIOUS value on scope
/// exit, not a hard-coded default.
///
/// The sibling of [`crate::common::pg::PgSetting`], and the reason it captures the
/// prior value rather than resetting: `binlog_row_image` and `binlog_row_metadata`
/// are exactly the settings a stand may already have tuned away from the server
/// default, and a guard that "restores" a guess leaves the stand drifted in a way
/// the next test blames on the product.
///
/// No restart: every variable this is used for is dynamic.
pub struct MysqlGlobal {
    name: String,
    prev: String,
}

impl MysqlGlobal {
    pub fn set(name: &str, value: &str) -> Self {
        use mysql::prelude::Queryable;
        let mut c = mysql_connect();
        let prev: String = c
            .query_first::<String, _>(format!("SELECT @@GLOBAL.{name}"))
            .unwrap_or_else(|e| panic!("read @@GLOBAL.{name}: {e}"))
            .unwrap_or_else(|| panic!("@@GLOBAL.{name} does not exist"));
        c.query_drop(format!("SET GLOBAL {name} = '{value}'"))
            .unwrap_or_else(|e| panic!("SET GLOBAL {name} = {value}: {e}"));
        Self {
            name: name.to_string(),
            prev,
        }
    }

    /// What the server reports NOW — assert the flip landed before concluding
    /// anything from a probe that depends on it.
    pub fn current(name: &str) -> String {
        use mysql::prelude::Queryable;
        mysql_connect()
            .query_first::<String, _>(format!("SELECT @@GLOBAL.{name}"))
            .ok()
            .flatten()
            .unwrap_or_default()
    }
}

impl Drop for MysqlGlobal {
    fn drop(&mut self) {
        use mysql::prelude::Queryable;
        if let Ok(mut c) = std::panic::catch_unwind(mysql_connect) {
            let _ = c.query_drop(format!("SET GLOBAL {} = '{}'", self.name, self.prev));
        }
    }
}

/// Seed ONE transaction spanning `ids` — the fixture an oversized-transaction test
/// needs, and a scenario rather than a `format!` in a test body.
///
/// A single `START TRANSACTION … COMMIT`, because the subject is a transaction
/// LARGER than the in-memory cap: N separate one-row transactions never reach the
/// cap however many of them there are.
///
/// Shaped for [`crate::common::cdc_id_ops`] and for the DuckDB census — `(id, v)`
/// integers plus a wide `pad`, so a row cap and a BYTE cap are both reachable, and
/// the source table's row count equals the delivered change count on an
/// insert-only fixture.
pub fn seed_one_transaction(
    c: &mut mysql::PooledConn,
    table: &str,
    ids: std::ops::RangeInclusive<usize>,
) {
    use mysql::prelude::Queryable;
    c.query_drop("START TRANSACTION").expect("begin");
    for i in ids {
        c.query_drop(format!(
            "INSERT INTO {table} VALUES ({i}, {i}, REPEAT('x', 200))"
        ))
        .expect("insert");
    }
    c.query_drop("COMMIT").expect("commit");
}
