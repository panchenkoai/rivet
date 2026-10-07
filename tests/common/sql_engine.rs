//! One handle over MySQL, PostgreSQL, SQL Server and Oracle for live scenarios run on each.

#![allow(dead_code)]

use super::{
    LiveService, MSSQL_URL, MYSQL_URL, MssqlTable, MysqlTable, POSTGRES_URL, PgTable, Rig,
    mssql_exec, mssql_query_strings, mysql_connect, mysql_root_connect, pg_connect, read_all_parts,
    require_alive, unique_name,
};
#[cfg(feature = "oracle")]
use super::{ORACLE_URL, OracleTable, ora_exec, ora_text_rows};
use ::mysql::prelude::Queryable;
use arrow::array::{Array, Int32Array, Int64Array};
use std::path::Path;

#[derive(Clone, Copy, Debug)]
pub enum SqlEngine {
    Mysql,
    Pg,
    Mssql,
    #[cfg(feature = "oracle")]
    Oracle,
}

impl SqlEngine {
    /// Skip unless this engine's live service is up.
    pub fn alive(self) {
        require_alive(match self {
            SqlEngine::Mysql => LiveService::Mysql,
            SqlEngine::Pg => LiveService::Postgres,
            SqlEngine::Mssql => LiveService::Mssql,
            #[cfg(feature = "oracle")]
            SqlEngine::Oracle => LiveService::Oracle,
        });
    }

    /// Whether the engine's catalog holds unquoted names in upper case (Oracle).
    pub fn folds_upper(self) -> bool {
        #[cfg(feature = "oracle")]
        if let SqlEngine::Oracle = self {
            return true;
        }
        false
    }

    /// The batch stand's source URL.
    pub fn url(self) -> &'static str {
        match self {
            SqlEngine::Mysql => MYSQL_URL,
            SqlEngine::Pg => POSTGRES_URL,
            SqlEngine::Mssql => MSSQL_URL,
            #[cfg(feature = "oracle")]
            SqlEngine::Oracle => ORACLE_URL,
        }
    }

    /// Run setup SQL, panicking on error.
    pub fn exec(self, sql: &str) {
        match self {
            SqlEngine::Mysql => mysql_connect().query_drop(sql).expect("mysql exec"),
            SqlEngine::Pg => pg_connect().batch_execute(sql).expect("pg exec"),
            SqlEngine::Mssql => mssql_exec(sql),
            #[cfg(feature = "oracle")]
            SqlEngine::Oracle => ora_exec(sql),
        }
    }

    /// `name` as this engine's DDL/SQL must spell it to keep it lower-case (Oracle folds bare names up).
    pub fn col(self, name: &str) -> String {
        match self {
            #[cfg(feature = "oracle")]
            SqlEngine::Oracle => format!("\"{name}\""),
            _ => name.to_string(),
        }
    }

    /// The 64-bit integer column type.
    pub fn int64(self) -> &'static str {
        match self {
            #[cfg(feature = "oracle")]
            SqlEngine::Oracle => "NUMBER(19)",
            _ => "BIGINT",
        }
    }

    /// The `(id, v)` integer pairs of `table` (columns made with [`SqlEngine::col`]), ordered by id.
    pub fn id_v_pairs(self, table: &str) -> Vec<(i64, i64)> {
        let text = |rows: Vec<(String, String)>| -> Vec<(i64, i64)> {
            rows.into_iter()
                .map(|(a, b)| (a.parse().expect("id"), b.parse().expect("v")))
                .collect()
        };
        match self {
            SqlEngine::Mysql => mysql_connect()
                .query(format!("SELECT id, v FROM {table} ORDER BY id"))
                .expect("mysql read"),
            SqlEngine::Pg => pg_connect()
                .query(&format!("SELECT id, v FROM {table} ORDER BY id"), &[])
                .expect("pg read")
                .iter()
                .map(|r| (r.get(0), r.get(1)))
                .collect(),
            SqlEngine::Mssql => text(
                mssql_query_strings(&format!(
                    "SELECT CONCAT(id, CHAR(9), v) FROM {table} ORDER BY id"
                ))
                .into_iter()
                .map(|l| {
                    let (a, b) = l.split_once('\t').expect("two columns");
                    (a.to_string(), b.to_string())
                })
                .collect(),
            ),
            #[cfg(feature = "oracle")]
            SqlEngine::Oracle => text(
                ora_text_rows(&format!(
                    "SELECT TO_CHAR(\"id\"), TO_CHAR(\"v\") FROM {table} ORDER BY \"id\""
                ))
                .into_iter()
                .map(|r| (r[0].clone().expect("id"), r[1].clone().expect("v")))
                .collect(),
            ),
        }
    }

    /// SQL for UTC now minus `minutes`.
    pub fn ago(self, minutes: i64) -> String {
        match self {
            SqlEngine::Mysql => format!("UTC_TIMESTAMP(6) - INTERVAL {minutes} MINUTE"),
            #[cfg(feature = "oracle")]
            SqlEngine::Oracle => {
                format!("SYS_EXTRACT_UTC(SYSTIMESTAMP) - NUMTODSINTERVAL({minutes}, 'MINUTE')")
            }
            SqlEngine::Pg => {
                format!("(now() AT TIME ZONE 'UTC') - INTERVAL '{minutes} minutes'")
            }
            SqlEngine::Mssql => format!("DATEADD(MINUTE, -{minutes}, SYSUTCDATETIME())"),
        }
    }

    /// The column definitions of [`SqlEngine::table`].
    fn standard_columns(self) -> String {
        let ts = match self {
            SqlEngine::Mysql => "DATETIME(6)",
            SqlEngine::Pg => "TIMESTAMP",
            SqlEngine::Mssql => "DATETIME2(6)",
            #[cfg(feature = "oracle")]
            SqlEngine::Oracle => "TIMESTAMP(6)",
        };
        let i = self.int64();
        format!(
            "id {i} PRIMARY KEY, ext_id {i} NOT NULL UNIQUE, \
             server_time {ts} NOT NULL, updated_at {ts} NULL, time_spent INT NULL"
        )
    }

    /// A fresh `(id, ext_id, server_time, updated_at, time_spent)` table and its drop guard.
    pub fn table(self, prefix: &str) -> (String, Box<dyn std::any::Any>) {
        self.create(prefix, &self.standard_columns())
    }

    /// A second database on this engine's stand server (another source key), or `None` where the stand has one (Oracle: one service).
    pub fn second_database(self, tag: &str) -> Option<SecondDatabase> {
        #[cfg(feature = "oracle")]
        if let SqlEngine::Oracle = self {
            return None;
        }
        let name = unique_name(tag);
        match self {
            SqlEngine::Mysql => mysql_root_connect()
                .query_drop(format!(
                    "CREATE DATABASE {name}; GRANT ALL ON {name}.* TO 'rivet'@'%'"
                ))
                .expect("mysql create database as root"),
            _ => self.exec(&format!("CREATE DATABASE {name}")),
        }
        Some(SecondDatabase { engine: self, name })
    }

    /// A fresh table with the given column definitions and its drop guard.
    pub fn create(self, prefix: &str, columns: &str) -> (String, Box<dyn std::any::Any>) {
        #[cfg(feature = "oracle")]
        if let SqlEngine::Oracle = self {
            let t = OracleTable::create(prefix, columns);
            return (t.name().to_string(), Box::new(t));
        }
        let name = unique_name(prefix);
        self.exec(&format!("CREATE TABLE {name} ({columns})"));
        let guard: Box<dyn std::any::Any> = match self {
            SqlEngine::Mysql => Box::new(MysqlTable::adopt(name.clone())),
            SqlEngine::Pg => Box::new(PgTable::adopt(name.clone())),
            SqlEngine::Mssql => Box::new(MssqlTable::adopt(name.clone())),
            #[cfg(feature = "oracle")]
            SqlEngine::Oracle => unreachable!("created above"),
        };
        (name, guard)
    }

    /// The INSERT of `ids` with `ext_id = id * 10`, `server_time` `minutes_ago` and `time_spent`.
    fn insert_sql(
        self,
        table: &str,
        ids: std::ops::RangeInclusive<i64>,
        minutes_ago: i64,
        spent: Option<i32>,
    ) -> String {
        let spent = spent.map_or("NULL".to_string(), |v| v.to_string());
        let rows: Vec<String> = ids
            .map(|i| format!("({i}, {}, {}, {spent})", i * 10, self.ago(minutes_ago)))
            .collect();
        format!(
            "INSERT INTO {table} (id, ext_id, server_time, time_spent) VALUES {}",
            rows.join(", ")
        )
    }

    /// Insert `ids` with `ext_id = id * 10`, `server_time` `minutes_ago` and `time_spent`.
    pub fn insert(
        self,
        table: &str,
        ids: std::ops::RangeInclusive<i64>,
        minutes_ago: i64,
        spent: Option<i32>,
    ) {
        self.exec(&self.insert_sql(table, ids, minutes_ago, spent));
    }

    /// A batch rig for this engine.
    pub fn rig(self, export: &str) -> Rig {
        match self {
            SqlEngine::Mysql => Rig::mysql_batch(export),
            SqlEngine::Pg => Rig::pg_batch(export),
            SqlEngine::Mssql => Rig::mssql_batch(export),
            #[cfg(feature = "oracle")]
            SqlEngine::Oracle => Rig::oracle_batch(export),
        }
    }
}

/// A scratch database beside the stand's own, dropped with this guard.
pub struct SecondDatabase {
    engine: SqlEngine,
    pub name: String,
}

impl SecondDatabase {
    /// The source URL of this database: the stand URL with another database path.
    pub fn url(&self) -> String {
        let (server, _) = self.engine.url().rsplit_once('/').expect("a database path");
        format!("{server}/{}", self.name)
    }

    /// Run setup SQL where `table` resolves inside this database.
    fn exec(&self, sql: &str) {
        match self.engine {
            SqlEngine::Pg => postgres::Client::connect(&self.url(), postgres::NoTls)
                .expect("connect to the second database")
                .batch_execute(sql)
                .expect("pg exec"),
            _ => self.engine.exec(sql),
        }
    }

    /// `table` as setup SQL names it: bare on its own connection (PostgreSQL), qualified elsewhere.
    fn qualified(&self, table: &str) -> String {
        match self.engine {
            SqlEngine::Pg => table.to_string(),
            SqlEngine::Mssql => format!("{}.dbo.{table}", self.name),
            _ => format!("{}.{table}", self.name),
        }
    }

    /// Create `table` here with the columns of [`SqlEngine::table`] and insert `ids`.
    pub fn table_with(&self, table: &str, ids: std::ops::RangeInclusive<i64>) {
        let (e, t) = (self.engine, self.qualified(table));
        self.exec(&format!("CREATE TABLE {t} ({})", e.standard_columns()));
        self.exec(&e.insert_sql(&t, ids, 180, Some(10)));
    }
}

impl Drop for SecondDatabase {
    fn drop(&mut self) {
        let drop = match self.engine {
            SqlEngine::Pg => format!("DROP DATABASE IF EXISTS {} WITH (FORCE)", self.name),
            SqlEngine::Mssql => format!(
                "ALTER DATABASE {0} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE {0}",
                self.name
            ),
            _ => format!("DROP DATABASE IF EXISTS {}", self.name),
        };
        let run = || match self.engine {
            SqlEngine::Mysql => mysql_root_connect().query_drop(&drop).expect("mysql drop"),
            _ => self.engine.exec(&drop),
        };
        if std::panic::catch_unwind(run).is_err() {
            eprintln!("could not drop scratch database {}", self.name);
        }
    }
}

/// Sorted `(id, time_spent)` of every part under `out`.
pub fn read_id_spent(out: &Path) -> Vec<(i64, Option<i32>)> {
    let mut rows = Vec::new();
    for b in read_all_parts(out) {
        let id = b
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let spent = b
            .column_by_name("time_spent")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            rows.push((id.value(i), (!spent.is_null(i)).then(|| spent.value(i))));
        }
    }
    rows.sort();
    rows
}

/// Sorted ids of every part under `out`.
pub fn read_ids(out: &Path) -> Vec<i64> {
    read_id_spent(out).into_iter().map(|r| r.0).collect()
}
