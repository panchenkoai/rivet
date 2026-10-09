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

    /// The port a source URL of this engine means when it names none.
    pub fn default_port(self) -> u16 {
        match self {
            SqlEngine::Mysql => 3306,
            SqlEngine::Pg => 5432,
            SqlEngine::Mssql => 1433,
            #[cfg(feature = "oracle")]
            SqlEngine::Oracle => 1521,
        }
    }

    /// The widest integer column type range chunking accepts as its key (Oracle: `NUMBER(18)`).
    fn range_int(self) -> &'static str {
        match self {
            #[cfg(feature = "oracle")]
            SqlEngine::Oracle => "NUMBER(18)",
            _ => self.int64(),
        }
    }

    /// The column definitions of [`SqlEngine::table`], with integer columns of type `i`.
    fn standard_columns(self, i: &str) -> String {
        let ts = match self {
            SqlEngine::Mysql => "DATETIME(6)",
            SqlEngine::Pg => "TIMESTAMP",
            SqlEngine::Mssql => "DATETIME2(6)",
            #[cfg(feature = "oracle")]
            SqlEngine::Oracle => "TIMESTAMP(6)",
        };
        format!(
            "id {i} PRIMARY KEY, ext_id {i} NOT NULL UNIQUE, \
             server_time {ts} NOT NULL, updated_at {ts} NULL, time_spent INT NULL"
        )
    }

    /// A fresh `(id, ext_id, server_time, updated_at, time_spent)` table and its drop guard.
    pub fn table(self, prefix: &str) -> (String, Box<dyn std::any::Any>) {
        self.create(prefix, &self.standard_columns(self.int64()))
    }

    /// [`SqlEngine::table`] with a key range chunking accepts on every engine.
    pub fn range_table(self, prefix: &str) -> (String, Box<dyn std::any::Any>) {
        self.create(prefix, &self.standard_columns(self.range_int()))
    }

    /// Create `table` again as [`SqlEngine::range_table`] made it, after a cell dropped it (the first guard still drops it).
    pub fn range_table_again(self, table: &str) {
        self.exec(&format!(
            "CREATE TABLE {table} ({})",
            self.standard_columns(self.range_int())
        ));
    }

    /// A second database of this engine (another source key): a scratch one on the stand server, or the stand's second Oracle instance (`oracle-latin1`).
    pub fn second_database(self, tag: &str) -> SecondDatabase {
        #[cfg(feature = "oracle")]
        if let SqlEngine::Oracle = self {
            return SecondDatabase {
                engine: self,
                name: String::new(),
                tables: Default::default(),
            };
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
        SecondDatabase {
            engine: self,
            name,
            tables: Default::default(),
        }
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

    /// Make the catalog's row estimate of `table` its real row count: the engine's statistics command (SQL Server counts rows without one).
    pub fn refresh_row_estimate(self, table: &str) {
        match self {
            SqlEngine::Mysql => self.exec(&format!("ANALYZE TABLE {table}")),
            SqlEngine::Pg => self.exec(&format!("ANALYZE {table}")),
            SqlEngine::Mssql => {}
            #[cfg(feature = "oracle")]
            SqlEngine::Oracle => self.exec(&format!(
                "BEGIN DBMS_STATS.GATHER_TABLE_STATS(USER, '{table}'); END;"
            )),
        }
    }

    /// `rig` restaged to `mode` with `lines`, each key column spelled as this engine's catalog holds it (Oracle: upper case).
    pub fn staged(self, rig: Rig, mode: &str, lines: &[&str]) -> Rig {
        const KEYS: &[&str] = &[
            "chunk_by_key",
            "chunk_column",
            "cursor_column",
            "time_column",
            "cursor_fallback_column",
        ];
        let lines: Vec<String> = lines
            .iter()
            .map(|l| match l.split_once(": ") {
                Some((k, v)) if self.folds_upper() && KEYS.contains(&k) => {
                    format!("{k}: {}", v.to_uppercase())
                }
                _ => l.to_string(),
            })
            .collect();
        let lines: Vec<&str> = lines.iter().map(String::as_str).collect();
        rig.restage(mode, &lines)
    }

    /// A login of this engine that may only read `table`, dropped with the guard; its sessions can be killed and its SELECT revoked and granted back. Oracle: the stand's own user, its sessions told apart by the statement that names `table`.
    pub fn reader(self, table: &str) -> Reader {
        let name = unique_name("sab_reader");
        let (name, url) = match self {
            SqlEngine::Pg => {
                self.exec(&format!("CREATE ROLE {name} LOGIN PASSWORD 'rivet'"));
                let url = POSTGRES_URL.replace("rivet:rivet@", &format!("{name}:rivet@"));
                (name, url)
            }
            SqlEngine::Mysql => {
                mysql_root_connect()
                    .query_drop(format!("CREATE USER '{name}'@'%' IDENTIFIED BY 'rivet'"))
                    .expect("mysql create user as root");
                let url = MYSQL_URL.replace("rivet:rivet@", &format!("{name}:rivet@"));
                (name, url)
            }
            SqlEngine::Mssql => {
                self.exec(&format!(
                    "CREATE LOGIN {name} WITH PASSWORD = 'Rivet_Passw0rd!', CHECK_POLICY = OFF; \
                     CREATE USER {name} FOR LOGIN {name}"
                ));
                let url = MSSQL_URL.replace("sa:", &format!("{name}:"));
                (name, url)
            }
            #[cfg(feature = "oracle")]
            SqlEngine::Oracle => ("RIVET".to_string(), ORACLE_URL.to_string()),
        };
        let reader = Reader {
            engine: self,
            name,
            table: table.to_string(),
            url,
        };
        if !self.folds_upper() {
            reader.grant();
        }
        reader
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

/// Why a kill that hit `killed` sessions of `login` is not a source session taken away, else `None`.
pub(crate) fn not_killed(login: &str, killed: usize) -> Option<String> {
    (killed == 0)
        .then(|| format!("sabotage: no session of {login} appeared to kill: the run was not met"))
}

#[test]
fn a_kill_that_hit_no_session_took_nothing_away() {
    assert_eq!(not_killed("t", 1), None);
    assert_eq!(
        not_killed("t", 0).as_deref(),
        Some("sabotage: no session of t appeared to kill: the run was not met")
    );
}

/// A login that may only read one table (see [`SqlEngine::reader`]).
pub struct Reader {
    engine: SqlEngine,
    name: String,
    table: String,
    url: String,
}

impl Reader {
    /// The source URL that logs in as this reader.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// `GRANT SELECT ... TO` or `REVOKE SELECT ... FROM` this reader on its table, as the engine's administrator.
    fn select(&self, verb: &str, prep: &str) {
        let (t, n) = (&self.table, &self.name);
        match self.engine {
            SqlEngine::Pg => self
                .engine
                .exec(&format!("{verb} SELECT ON {t} {prep} {n}")),
            SqlEngine::Mysql => mysql_root_connect()
                .query_drop(format!("{verb} SELECT ON rivet.{t} {prep} '{n}'@'%'"))
                .expect("mysql grant as root"),
            SqlEngine::Mssql => self
                .engine
                .exec(&format!("{verb} SELECT ON dbo.{t} {prep} {n}")),
            #[cfg(feature = "oracle")]
            SqlEngine::Oracle => panic!(
                "{verb} SELECT {prep} {n} on {t}: the Oracle reader is the stand's own user (takeaway_select_revoked is a gap on Oracle)"
            ),
        }
    }

    /// The kill handles of this login's sessions.
    fn sessions(&self) -> Vec<String> {
        let n = &self.name;
        match self.engine {
            SqlEngine::Pg => pg_connect()
                .query(
                    "SELECT pid::text FROM pg_stat_activity WHERE usename = $1 ORDER BY 1",
                    &[n],
                )
                .expect("pg_stat_activity")
                .iter()
                .map(|r| r.get(0))
                .collect(),
            SqlEngine::Mysql => mysql_root_connect()
                .query::<u64, _>(format!(
                    "SELECT id FROM information_schema.processlist WHERE user = '{n}' ORDER BY id"
                ))
                .expect("processlist")
                .into_iter()
                .map(|id| id.to_string())
                .collect(),
            SqlEngine::Mssql => mssql_query_strings(&format!(
                "SELECT CAST(session_id AS VARCHAR(12)) FROM sys.dm_exec_sessions \
                 WHERE login_name = '{n}' ORDER BY 1"
            )),
            #[cfg(feature = "oracle")]
            SqlEngine::Oracle => {
                let sql = format!(
                    "SELECT TO_CHAR(s.sid) || ',' || TO_CHAR(s.serial#) FROM v$session s \
                     JOIN v$sql q ON q.sql_id = NVL(s.sql_id, s.prev_sql_id) \
                     WHERE s.username = '{n}' AND q.sql_text LIKE '%{}%' \
                     AND q.sql_text NOT LIKE '%v$session%' AND q.rows_processed > 0 ORDER BY 1",
                    self.table
                );
                match super::ora_system_conn().query(&sql, &[]) {
                    Ok(rows) => rows
                        .filter_map(|r| r.ok()?.get::<Option<String>>(0).ok()?)
                        .collect(),
                    Err(_) => Vec::new(),
                }
            }
        }
    }

    /// Kill one session on the server by its handle; a session already gone is not an error.
    fn kill_session(&self, handle: &str) {
        match self.engine {
            SqlEngine::Pg => {
                let pid: i32 = handle.parse().expect("a backend pid");
                let _ = pg_connect().query("SELECT pg_terminate_backend($1)", &[&pid]);
            }
            SqlEngine::Mysql => {
                let _ = mysql_root_connect().query_drop(format!("KILL {handle}"));
            }
            SqlEngine::Mssql => {
                let _ = super::mssql_exec_once(&format!("KILL {handle}"));
            }
            #[cfg(feature = "oracle")]
            SqlEngine::Oracle => {
                let kill = format!("ALTER SYSTEM KILL SESSION '{handle}' IMMEDIATE");
                let _ = super::ora_system_conn().execute(&kill, &[]);
            }
        }
    }

    /// Kill on the server every session of this login once the same ones have been there for 300 ms; panics when none appears in 60 s. Returns how many were killed.
    pub fn kill_sessions(&self) -> usize {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        let mut seen: Option<(Vec<String>, std::time::Instant)> = None;
        let mut killed = 0;
        while killed == 0 && std::time::Instant::now() < deadline {
            let now = self.sessions();
            match &seen {
                Some((prev, since)) if !now.is_empty() && *prev == now => {
                    if since.elapsed() >= std::time::Duration::from_millis(300) {
                        now.iter().for_each(|s| self.kill_session(s));
                        killed = now.len();
                    }
                }
                _ if now.is_empty() => seen = None,
                _ => seen = Some((now, std::time::Instant::now())),
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        if let Some(why) = not_killed(&self.name, killed) {
            panic!("{why}");
        }
        killed
    }

    /// Take this reader's SELECT on its table away.
    pub fn revoke(&self) {
        self.select("REVOKE", "FROM");
    }

    /// Give this reader's SELECT on its table back.
    pub fn grant(&self) {
        self.select("GRANT", "TO");
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        let n = &self.name;
        let run = || match self.engine {
            SqlEngine::Pg => self.engine.exec(&format!(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '{n}'; \
                 DROP OWNED BY {n}; DROP ROLE {n}"
            )),
            SqlEngine::Mysql => mysql_root_connect()
                .query_drop(format!("DROP USER IF EXISTS '{n}'@'%'"))
                .expect("mysql drop user"),
            SqlEngine::Mssql => {
                for spid in mssql_query_strings(&format!(
                    "SELECT CAST(session_id AS VARCHAR(12)) FROM sys.dm_exec_sessions WHERE login_name = '{n}'"
                )) {
                    let _ = super::mssql_exec_once(&format!("KILL {spid}"));
                }
                self.engine.exec(&format!("DROP USER {n}; DROP LOGIN {n}"))
            }
            #[cfg(feature = "oracle")]
            SqlEngine::Oracle => {}
        };
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(run)).is_err() {
            eprintln!("could not drop the reader {n}");
        }
    }
}

/// A scratch database beside the stand's own, dropped with this guard.
pub struct SecondDatabase {
    engine: SqlEngine,
    pub name: String,
    /// Tables to drop one by one where the database itself outlives the guard (Oracle).
    tables: std::cell::RefCell<Vec<String>>,
}

impl SecondDatabase {
    /// The source URL of this database: the stand URL with another database path.
    pub fn url(&self) -> String {
        #[cfg(feature = "oracle")]
        if let SqlEngine::Oracle = self.engine {
            return super::oracle_latin1_url();
        }
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
            #[cfg(feature = "oracle")]
            SqlEngine::Oracle => super::ora_exec_on(&self.url(), sql),
            _ => self.engine.exec(sql),
        }
    }

    /// `table` as setup SQL names it: bare on its own connection (PostgreSQL), qualified elsewhere.
    fn qualified(&self, table: &str) -> String {
        match self.engine {
            SqlEngine::Pg => table.to_string(),
            #[cfg(feature = "oracle")]
            SqlEngine::Oracle => table.to_string(),
            SqlEngine::Mssql => format!("{}.dbo.{table}", self.name),
            _ => format!("{}.{table}", self.name),
        }
    }

    /// Create `table` here with the columns of [`SqlEngine::table`] and insert `ids`.
    pub fn table_with(&self, table: &str, ids: std::ops::RangeInclusive<i64>) {
        let (e, t) = (self.engine, self.qualified(table));
        self.exec(&format!(
            "CREATE TABLE {t} ({})",
            e.standard_columns(e.range_int())
        ));
        self.tables.borrow_mut().push(t.clone());
        self.exec(&e.insert_sql(&t, ids, 180, Some(10)));
    }
}

impl Drop for SecondDatabase {
    fn drop(&mut self) {
        #[cfg(feature = "oracle")]
        if let SqlEngine::Oracle = self.engine {
            for t in self.tables.borrow().iter() {
                let url = self.url();
                let drop = format!("DROP TABLE {t} PURGE");
                if std::panic::catch_unwind(|| super::ora_exec_on(&url, &drop)).is_err() {
                    eprintln!("could not drop {t} in the second Oracle database");
                }
            }
            return;
        }
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

/// Sorted `(id, time_spent)` of every part under `out`; the column names match in either case (Oracle's are upper-case).
pub fn read_id_spent(out: &Path) -> Vec<(i64, Option<i32>)> {
    let mut rows = Vec::new();
    for b in read_all_parts(out) {
        let col = |name: &str| {
            let schema = b.schema();
            let i = schema
                .fields()
                .iter()
                .position(|f| f.name().eq_ignore_ascii_case(name))
                .unwrap_or_else(|| panic!("no `{name}` column in {:?}", schema.fields()));
            b.column(i).clone()
        };
        let (id, spent) = (col("id"), col("time_spent"));
        let id = id.as_any().downcast_ref::<Int64Array>().unwrap();
        let spent = spent.as_any().downcast_ref::<Int32Array>().unwrap();
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

/// Sorted `id` values of `batches`.
pub fn ids_of(batches: &[arrow::record_batch::RecordBatch]) -> Vec<i64> {
    let mut ids: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            let col = b.column_by_name("id").expect("an id column");
            let col = col
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("a BIGINT id");
            col.values().to_vec()
        })
        .collect();
    ids.sort();
    ids
}
