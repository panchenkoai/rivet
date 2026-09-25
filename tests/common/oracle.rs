//! Oracle test helpers: a pinned connection to the stand's `rivet` user, DDL/DML
//! execution, server-side renderings as text (the independent oracle — the
//! database formats its own values), and a self-dropping table guard.

use super::env::ORACLE_URL;

fn ora<T>(r: Result<T, oracledb::Error>, what: &str) -> T {
    r.unwrap_or_else(|e| panic!("oracle {what}: {e:?}"))
}

/// A connection as the stand's `rivet` user, session pinned to UTC.
pub fn ora_conn() -> oracledb::Connection {
    let rest = ORACLE_URL.strip_prefix("oracle://").unwrap();
    let (cred, target) = rest.split_once('@').unwrap();
    let (user, pass) = cred.split_once(':').unwrap();
    let cfg = ora(
        oracledb::Config::default()
            .set_credentials(user, pass)
            .set_connect_string(target),
        "config",
    );
    let _ = rustls::crypto::ring::default_provider().install_default();
    let conn = ora(oracledb::connect(cfg), "connect");
    ora(
        conn.execute("ALTER SESSION SET TIME_ZONE = '+00:00'", &[]),
        "pin tz",
    );
    conn
}

/// A connection as the stand's `SYSTEM` user (password `rivet`), for grants and session kills.
pub fn ora_system_conn() -> oracledb::Connection {
    let target = ORACLE_URL.split_once('@').unwrap().1;
    let cfg = ora(
        oracledb::Config::default()
            .set_credentials("system", "rivet")
            .set_connect_string(target),
        "config",
    );
    let _ = rustls::crypto::ring::default_provider().install_default();
    ora(oracledb::connect(cfg), "connect as system")
}

/// Run one statement as `SYSTEM`.
pub fn ora_system_exec(sql: &str) {
    ora(ora_system_conn().execute(sql, &[]), sql);
}

/// Run one statement (DDL or DML) and commit.
pub fn ora_exec(sql: &str) {
    let conn = ora_conn();
    ora(conn.execute(sql, &[]), sql);
    ora(conn.commit(), "commit");
}

/// Every row of a query whose columns are all character types, as text.
pub fn ora_text_rows(sql: &str) -> Vec<Vec<Option<String>>> {
    let conn = ora_conn();
    let cursor = ora(conn.query(sql, &[]), sql);
    let n = cursor.columns().len();
    cursor
        .map(|row| {
            let row = ora(row, "row");
            (0..n)
                .map(|i| ora(row.get::<Option<String>>(i), "cell"))
                .collect()
        })
        .collect()
}

/// A table named `<prefix>_<unique>` (upper-case, as Oracle stores it), dropped on scope exit.
pub struct OracleTable(String);

impl OracleTable {
    /// Create `name` with the given column list.
    pub fn create(prefix: &str, columns: &str) -> Self {
        let name = super::unique_name(prefix).to_uppercase();
        ora_exec(&format!("CREATE TABLE {name} ({columns})"));
        Self(name)
    }

    pub fn name(&self) -> &str {
        &self.0
    }
}

impl Drop for OracleTable {
    fn drop(&mut self) {
        if let Ok(conn) = std::panic::catch_unwind(ora_conn) {
            let _ = conn.execute(&format!("DROP TABLE {} PURGE", self.0), &[]);
        }
    }
}

/// `ID NUMBER PRIMARY KEY, NAME VARCHAR2, AMOUNT NUMBER(12,2)` with `rows` rows (ids 1..=rows).
pub fn seed_oracle_numeric_table(rows: i64) -> OracleTable {
    let t = OracleTable::create(
        "ora_num",
        "id NUMBER PRIMARY KEY, name VARCHAR2(40) NOT NULL, amount NUMBER(12,2)",
    );
    ora_exec(&format!(
        "INSERT INTO {} SELECT LEVEL, 'name_' || LEVEL, LEVEL * 1.25 FROM dual CONNECT BY LEVEL <= {rows}",
        t.name()
    ));
    t
}
