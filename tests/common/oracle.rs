//! Oracle test helpers: a pinned connection to the stand's `rivet` user, DDL/DML
//! execution, server-side renderings as text (the independent oracle — the
//! database formats its own values), and a self-dropping table guard.

use super::env::ORACLE_URL;

fn ora<T>(r: Result<T, oracledb::Error>, what: &str) -> T {
    r.unwrap_or_else(|e| panic!("oracle {what}: {e:?}"))
}

/// A connection as the stand's `rivet` user, session pinned to UTC.
pub fn ora_conn() -> oracledb::Connection {
    ora_conn_to(ORACLE_URL)
}

/// A connection for `url` (`oracle://user:pass@host:port/service`), session pinned to UTC.
pub fn ora_conn_to(url: &str) -> oracledb::Connection {
    let rest = url.strip_prefix("oracle://").unwrap();
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
    ora_exec_on(ORACLE_URL, sql);
}

/// Run one statement (DDL or DML) against `url` and commit.
pub fn ora_exec_on(url: &str, sql: &str) {
    let conn = ora_conn_to(url);
    ora(conn.execute(sql, &[]), sql);
    ora(conn.commit(), "commit");
}

/// Every row of a query whose columns are all character types, as text.
pub fn ora_text_rows(sql: &str) -> Vec<Vec<Option<String>>> {
    ora_text_rows_on(ORACLE_URL, sql)
}

/// [`ora_text_rows`] against `url`.
pub fn ora_text_rows_on(url: &str, sql: &str) -> Vec<Vec<Option<String>>> {
    let conn = ora_conn_to(url);
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
pub struct OracleTable(String, String);

impl OracleTable {
    /// Create `name` with the given column list.
    pub fn create(prefix: &str, columns: &str) -> Self {
        Self::create_on(ORACLE_URL, prefix, columns)
    }

    /// [`OracleTable::create`] in the database `url` names.
    pub fn create_on(url: &str, prefix: &str, columns: &str) -> Self {
        let name = super::unique_name(prefix).to_uppercase();
        ora_exec_on(url, &format!("CREATE TABLE {name} ({columns})"));
        Self(name, url.to_string())
    }

    /// Create a table whose catalog name is exactly `name` (quoted, so a mixed case survives); `name()` returns it quoted.
    pub fn create_exact(name: &str, columns: &str) -> Self {
        let quoted = format!("\"{name}\"");
        ora_exec(&format!("CREATE TABLE {quoted} ({columns})"));
        Self(quoted, ORACLE_URL.to_string())
    }

    pub fn name(&self) -> &str {
        &self.0
    }
}

impl Drop for OracleTable {
    fn drop(&mut self) {
        let url = self.1.clone();
        if let Ok(conn) = std::panic::catch_unwind(|| ora_conn_to(&url)) {
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

/// A throwaway login with SELECT on `RIVET.<table>` whose every session runs `logon_body` (an AFTER LOGON trigger); trigger and user dropped on scope exit.
pub struct OracleLogonUser(String);

impl OracleLogonUser {
    const PASSWORD: &'static str = "Odd_passw0rd1";

    pub fn create(prefix: &str, table: &str, logon_body: &str) -> Self {
        let name = super::unique_name(prefix).to_uppercase();
        ora_system_exec(&format!(
            "CREATE USER {name} IDENTIFIED BY \"{}\"",
            Self::PASSWORD
        ));
        let user = Self(name);
        ora_system_exec(&format!("GRANT CREATE SESSION TO {}", user.0));
        ora_system_exec(&format!("GRANT SELECT ON RIVET.{table} TO {}", user.0));
        ora_system_exec(&format!(
            "CREATE OR REPLACE TRIGGER SYSTEM.{0}_LOGON AFTER LOGON ON {0}.SCHEMA \
             BEGIN {logon_body} END;",
            user.0
        ));
        user
    }

    pub fn url(&self) -> String {
        format!(
            "oracle://{}:{}@127.0.0.1:1521/FREEPDB1",
            self.0,
            Self::PASSWORD
        )
    }
}

impl Drop for OracleLogonUser {
    fn drop(&mut self) {
        let _ = std::panic::catch_unwind(|| {
            ora_system_exec(&format!("DROP TRIGGER SYSTEM.{}_LOGON", self.0))
        });
        let _ =
            std::panic::catch_unwind(|| ora_system_exec(&format!("DROP USER {} CASCADE", self.0)));
    }
}
