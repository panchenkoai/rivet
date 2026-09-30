//! Session-state battery: the same exports run under a NON-default session — a Postgres
//! role in Asia/Tokyo with `DateStyle = German, DMY`, and an SQL Server login whose
//! language reads dates day-first. The dev stand is UTC/ISO/us_english everywhere, so a
//! value rendered to text and re-injected as a literal is only graded here.

use crate::common::*;

const PW: &str = "Rivet_Passw0rd!";

/// A Postgres login role whose session defaults are all non-default; dropped on `Drop`.
struct OddPgRole(String);

impl OddPgRole {
    fn create(tables: &[&str]) -> Self {
        let role = unique_name("rivet_odd");
        let grants: String = tables
            .iter()
            .map(|t| format!("GRANT SELECT ON {t} TO {role};"))
            .collect();
        pg_connect()
            .batch_execute(&format!(
                "DROP ROLE IF EXISTS {role};
                 CREATE ROLE {role} LOGIN PASSWORD '{PW}';
                 ALTER ROLE {role} SET timezone = 'Asia/Tokyo';
                 ALTER ROLE {role} SET datestyle = 'German, DMY';
                 ALTER ROLE {role} SET intervalstyle = 'sql_standard';
                 ALTER ROLE {role} SET bytea_output = 'escape';
                 {grants}"
            ))
            .expect("create odd role");
        Self(role)
    }

    fn url(&self) -> String {
        format!("postgresql://{}:{PW}@127.0.0.1:5432/rivet", self.0)
    }
}

impl Drop for OddPgRole {
    fn drop(&mut self) {
        let _ = pg_connect().batch_execute(&format!(
            "DROP OWNED BY {0}; DROP ROLE IF EXISTS {0};",
            self.0
        ));
    }
}

/// An SQL Server login whose language is British (DATEFORMAT dmy); dropped on `Drop`.
struct DayFirstLogin(String);

impl DayFirstLogin {
    fn create() -> Self {
        let login = unique_name("rivet_dmy");
        let me = Self(login.clone());
        me.drop_principal();
        mssql_exec(&format!(
            "CREATE LOGIN {login} WITH PASSWORD = '{PW}', CHECK_POLICY = OFF, DEFAULT_LANGUAGE = British; \
             CREATE USER {login} FOR LOGIN {login}; ALTER ROLE db_datareader ADD MEMBER {login};"
        ));
        me
    }

    fn url(&self) -> String {
        format!("sqlserver://{}:{PW}@127.0.0.1:1433/rivet", self.0)
    }

    fn drop_principal(&self) {
        mssql_exec(&format!(
            "IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name = '{0}') DROP USER {0}; \
             IF EXISTS (SELECT 1 FROM sys.server_principals WHERE name = '{0}') DROP LOGIN {0};",
            self.0
        ));
    }
}

impl Drop for DayFirstLogin {
    fn drop(&mut self) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.drop_principal()));
    }
}

/// `(count(*), count(DISTINCT id))` over the parts the destination manifest declares.
fn declared_rows(rig: &Rig) -> (i64, i64) {
    (
        duckdb_declared_dir_scalar(&rig.out_dir(), "count(*)"),
        duckdb_declared_dir_scalar(&rig.out_dir(), "count(DISTINCT id)"),
    )
}

/// Seed `public.<name>` with `n` rows whose `ts` (timestamptz) steps a minute from a
/// half-second instant, so the fraction and the zone both matter.
fn pg_ts_table(prefix: &str, n: i64) -> (String, PgTable) {
    let t = unique_name(prefix);
    pg_connect()
        .batch_execute(&format!(
            "CREATE TABLE public.{t} (id BIGINT PRIMARY KEY, ts TIMESTAMPTZ NOT NULL, d DATE NOT NULL);
             INSERT INTO public.{t} SELECT g, TIMESTAMPTZ '2024-01-01 00:00:00.5+00' + g * INTERVAL '1 minute',
               DATE '2024-01-01' + (g % 40) FROM generate_series(1, {n}) g;"
        ))
        .expect("seed");
    (t.clone(), PgTable::adopt(format!("public.{t}")))
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn pg_check_reports_a_timestamptz_cursor_range_in_iso_utc_for_any_session() {
    require_alive(LiveService::Postgres);
    let (t, _g) = pg_ts_table("ss_check", 10);
    let role = OddPgRole::create(&[&format!("public.{t}")]);
    let rig = Rig::pg_batch(&format!("public.{t}"))
        .source_url(&role.url())
        .mode("incremental")
        .export_line("cursor_column: ts");
    let out = rig.cli(&["check"]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        said.contains("Cursor range: 2024-01-01 00:01:00.5+00 .. 2024-01-01 00:10:00.5+00"),
        "the range is the instant in ISO UTC, not the role's German/Tokyo rendering:\n{said}"
    );
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn pg_incremental_on_a_timestamptz_cursor_reads_each_row_once_across_runs_in_an_odd_session() {
    require_alive(LiveService::Postgres);
    let (t, _g) = pg_ts_table("ss_inc", 300);
    let role = OddPgRole::create(&[&format!("public.{t}")]);
    let rig = Rig::pg_batch(&format!("public.{t}"))
        .source_url(&role.url())
        .mode("incremental")
        .export_line("cursor_column: ts");
    rig.run_ok();
    assert_eq!(declared_rows(&rig), (300, 300), "run 1");
    pg_connect()
        .batch_execute(&format!(
            "INSERT INTO public.{t} SELECT g, TIMESTAMPTZ '2024-01-01 00:00:00.5+00' + g * INTERVAL '1 minute',
               DATE '2024-01-01' FROM generate_series(301, 450) g;"
        ))
        .expect("grow");
    rig.run_ok();
    assert_eq!(declared_rows(&rig), (450, 450), "both runs: every row once");
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn pg_date_window_chunks_read_each_row_once_in_an_odd_session() {
    require_alive(LiveService::Postgres);
    let (t, _g) = pg_ts_table("ss_days", 400);
    let role = OddPgRole::create(&[&format!("public.{t}")]);
    let rig = Rig::pg_batch(&format!("public.{t}"))
        .source_url(&role.url())
        .mode("chunked")
        .export_line("chunk_column: d")
        .export_line("chunk_by_days: 7");
    rig.run_ok();
    assert_eq!(declared_rows(&rig), (400, 400));
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn pg_parallel_keyset_incremental_on_a_timestamptz_key_reads_each_row_once_in_an_odd_session() {
    require_alive(LiveService::Postgres);
    let (t, _g) = pg_ts_table("ss_ks", 1000);
    pg_connect()
        .batch_execute(&format!(
            "ALTER TABLE public.{t} ADD CONSTRAINT {t}_ts UNIQUE (ts)"
        ))
        .expect("unique ts");
    let role = OddPgRole::create(&[&format!("public.{t}")]);
    let rig = Rig::pg_batch(&format!("public.{t}"))
        .source_url(&role.url())
        .mode("chunked")
        .export_line("chunk_by_key: ts")
        .export_line("parallel: 4")
        .export_line("chunk_checkpoint: true")
        .export_line("keyset_incremental: true")
        .export_line("chunk_size: 100");
    rig.run_ok();
    pg_connect()
        .batch_execute(&format!(
            "INSERT INTO public.{t} SELECT g, TIMESTAMPTZ '2024-01-01 00:00:00.5+00' + g * INTERVAL '1 minute',
               DATE '2024-01-01' FROM generate_series(1001, 1500) g;"
        ))
        .expect("grow");
    rig.run_ok();
    assert_eq!(declared_rows(&rig), (1500, 1500));
}

/// Seed `dbo.<name>` with `n` rows whose legacy `DATETIME` `ts` walks days 1..28 of
/// successive months, so a day-first parse of any `yyyy-mm-dd` literal moves or fails.
fn mssql_dt_table(prefix: &str, n: i64) -> (String, MssqlTable) {
    let t = unique_name(prefix);
    let values = (1..=n)
        .map(|i| {
            format!(
                "({i}, DATEADD(MINUTE, {i}, CAST('2024-{:02}-{:02}T10:00:00.997' AS DATETIME)))",
                (i % 12) + 1,
                (i % 28) + 1
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    mssql_exec(&format!(
        "IF OBJECT_ID('dbo.{t}') IS NOT NULL DROP TABLE dbo.{t}; \
         CREATE TABLE dbo.{t} (id INT NOT NULL PRIMARY KEY, ts DATETIME NOT NULL); \
         INSERT INTO dbo.{t} VALUES {values}"
    ));
    (t.clone(), MssqlTable::adopt(t))
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_incremental_on_a_datetime_cursor_reads_each_row_once_for_a_day_first_login() {
    let (t, _g) = mssql_dt_table("ss_minc", 200);
    let login = DayFirstLogin::create();
    let rig = Rig::mssql_batch(&t)
        .no_oracle("legacy DATETIME is 1/300 s: rivet rounds it to the nearest microsecond, the DuckDB scanner truncates, and neither equals the source")
        .source_url(&login.url())
        .mode("incremental")
        .export_line("cursor_column: ts");
    rig.run_ok();
    assert_eq!(declared_rows(&rig), (200, 200), "run 1");
    mssql_exec(&format!(
        "INSERT INTO dbo.{t} VALUES (201, '2025-01-13T10:00:00.997'), (202, '2025-02-14T10:00:00.003')"
    ));
    rig.run_ok();
    assert_eq!(declared_rows(&rig), (202, 202), "both runs: every row once");
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_date_window_chunks_read_each_row_once_for_a_day_first_login() {
    let (t, _g) = mssql_dt_table("ss_mdays", 300);
    let login = DayFirstLogin::create();
    let rig = Rig::mssql_batch(&t)
        .no_oracle("legacy DATETIME is 1/300 s: rivet rounds it to the nearest microsecond, the DuckDB scanner truncates, and neither equals the source")
        .source_url(&login.url())
        .mode("chunked")
        .export_line("chunk_column: ts")
        .export_line("chunk_by_days: 20");
    rig.run_ok();
    assert_eq!(declared_rows(&rig), (300, 300));
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn pg_exported_instants_and_dates_equal_the_source_in_an_odd_session() {
    require_alive(LiveService::Postgres);
    let (t, _g) = pg_ts_table("ss_vals", 200);
    let role = OddPgRole::create(&[&format!("public.{t}")]);
    let rig = Rig::pg_batch(&format!("public.{t}"))
        .source_url(&role.url())
        .mode("full");
    rig.run_ok();
    let src = pg_connect()
        .query_one(
            &format!(
                "SELECT sum((extract(epoch FROM ts) * 1000)::bigint)::bigint, \
                 sum(d - DATE '1970-01-01')::bigint FROM public.{t}"
            ),
            &[],
        )
        .expect("source sums");
    let (ts_ms, days): (i64, i64) = (src.get(0), src.get(1));
    assert_eq!(
        (
            duckdb_declared_dir_scalar(&rig.out_dir(), "sum(epoch_ms(ts))::BIGINT"),
            duckdb_declared_dir_scalar(&rig.out_dir(), "sum(d - DATE '1970-01-01')::BIGINT"),
        ),
        (ts_ms, days),
        "every instant and every date is the source's, whatever the session zone"
    );
}
