//! One handle over MySQL, PostgreSQL, SQL Server and Oracle for live scenarios run on each.

#![allow(dead_code)]

use super::{
    LiveService, MSSQL_URL, MYSQL_URL, MssqlTable, MysqlTable, POSTGRES_URL, PgTable, Rig,
    mssql_exec, mssql_query_strings, mysql_connect, pg_connect, read_all_parts, require_alive,
    unique_name,
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

    /// A fresh `(id, ext_id, server_time, updated_at, time_spent)` table and its drop guard.
    pub fn table(self, prefix: &str) -> (String, Box<dyn std::any::Any>) {
        let ts = match self {
            SqlEngine::Mysql => "DATETIME(6)",
            SqlEngine::Pg => "TIMESTAMP",
            SqlEngine::Mssql => "DATETIME2(6)",
            #[cfg(feature = "oracle")]
            SqlEngine::Oracle => "TIMESTAMP(6)",
        };
        let i = self.int64();
        self.create(
            prefix,
            &format!(
                "id {i} PRIMARY KEY, ext_id {i} NOT NULL UNIQUE, \
                 server_time {ts} NOT NULL, updated_at {ts} NULL, time_spent INT NULL"
            ),
        )
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

    /// Insert `ids` with `ext_id = id * 10`, `server_time` `minutes_ago` and `time_spent`.
    pub fn insert(
        self,
        table: &str,
        ids: std::ops::RangeInclusive<i64>,
        minutes_ago: i64,
        spent: Option<i32>,
    ) {
        let spent = spent.map_or("NULL".to_string(), |v| v.to_string());
        let rows: Vec<String> = ids
            .map(|i| format!("({i}, {}, {}, {spent})", i * 10, self.ago(minutes_ago)))
            .collect();
        self.exec(&format!(
            "INSERT INTO {table} (id, ext_id, server_time, time_spent) VALUES {}",
            rows.join(", ")
        ));
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
