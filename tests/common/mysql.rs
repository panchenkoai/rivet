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
