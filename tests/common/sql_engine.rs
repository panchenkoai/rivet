//! One handle over MySQL, PostgreSQL and SQL Server for live scenarios run on all three.

#![allow(dead_code)]

use super::{
    LiveService, MssqlTable, MysqlTable, PgTable, Rig, mssql_exec, mysql_connect, pg_connect,
    read_all_parts, require_alive, unique_name,
};
use ::mysql::prelude::Queryable;
use arrow::array::{Array, Int32Array, Int64Array};
use std::path::Path;

#[derive(Clone, Copy, Debug)]
pub enum SqlEngine {
    Mysql,
    Pg,
    Mssql,
}

impl SqlEngine {
    /// Skip unless this engine's live service is up.
    pub fn alive(self) {
        require_alive(match self {
            SqlEngine::Mysql => LiveService::Mysql,
            SqlEngine::Pg => LiveService::Postgres,
            SqlEngine::Mssql => LiveService::Mssql,
        });
    }

    /// Run setup SQL, panicking on error.
    pub fn exec(self, sql: &str) {
        match self {
            SqlEngine::Mysql => mysql_connect().query_drop(sql).expect("mysql exec"),
            SqlEngine::Pg => pg_connect().batch_execute(sql).expect("pg exec"),
            SqlEngine::Mssql => mssql_exec(sql),
        }
    }

    /// SQL for UTC now minus `minutes`.
    pub fn ago(self, minutes: i64) -> String {
        match self {
            SqlEngine::Mysql => format!("UTC_TIMESTAMP(6) - INTERVAL {minutes} MINUTE"),
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
        };
        self.create(
            prefix,
            &format!(
                "id BIGINT PRIMARY KEY, ext_id BIGINT NOT NULL UNIQUE, \
                 server_time {ts} NOT NULL, updated_at {ts} NULL, time_spent INT NULL"
            ),
        )
    }

    /// A fresh table with the given column definitions and its drop guard.
    pub fn create(self, prefix: &str, columns: &str) -> (String, Box<dyn std::any::Any>) {
        let name = unique_name(prefix);
        self.exec(&format!("CREATE TABLE {name} ({columns})"));
        let guard: Box<dyn std::any::Any> = match self {
            SqlEngine::Mysql => Box::new(MysqlTable::adopt(name.clone())),
            SqlEngine::Pg => Box::new(PgTable::adopt(name.clone())),
            SqlEngine::Mssql => Box::new(MssqlTable::adopt(name.clone())),
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
