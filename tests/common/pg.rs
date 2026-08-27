//! Postgres test helpers: connection, RAII table guard, canonical seeders.

#![allow(dead_code)]

use postgres::{Client as PgClient, NoTls};

use super::env::POSTGRES_URL;
use super::unique_name;

/// Open a fresh Postgres connection to the primary instance.  Panics on
/// failure with the driver's message — callers should call `require_alive`
/// first to get an actionable error if the stack is down.
pub fn pg_connect() -> PgClient {
    PgClient::connect(POSTGRES_URL, NoTls).expect("connect to postgres")
}

/// RAII handle that drops the table on test exit (panic-safe via `Drop`).
/// Without this, a test that seeds `orders_xyz` and then fails leaves the
/// table behind, polluting the next run.
pub struct PgTable {
    name: String,
    /// The instance the table LIVES on — Drop must connect here, not to a
    /// hardcoded primary (r4 bughunt: CDC-instance tables were never dropped).
    url: String,
}

impl PgTable {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Wrap an already-created table (custom schema, seeded by the test itself)
    /// in the RAII drop guard, so tests that need a non-canonical shape don't
    /// roll their own `DropPgTable`. The table is dropped on guard drop.
    pub fn adopt(name: String) -> Self {
        Self::adopt_on(POSTGRES_URL, name)
    }

    /// Adopt a table living on a NON-primary instance (pg-cdc :5434). The Drop
    /// guard connects to the STORED url — it hardcoded :5432 for years, so
    /// every CDC-instance table was "dropped" on the wrong server where IF
    /// EXISTS made it a silent no-op and the isolation instance accumulated
    /// fixtures unboundedly (r4 bughunt; the Slot guard always did this right).
    pub fn adopt_on(url: &str, name: String) -> Self {
        PgTable {
            name,
            url: url.to_string(),
        }
    }
}

impl Drop for PgTable {
    fn drop(&mut self) {
        // Best-effort cleanup: if the drop fails we've already caused more
        // damage than this can unwind.  Do NOT panic from Drop.
        if let Ok(mut c) = PgClient::connect(&self.url, NoTls) {
            let _ = c.execute(&format!("DROP TABLE IF EXISTS {}", self.name), &[]);
        }
    }
}

/// Create a uniquely-named Postgres table populated with `row_count` rows of
/// the canonical `(id BIGINT, name TEXT, amount NUMERIC, created_at TIMESTAMPTZ)`
/// shape used throughout the live-test suite.  Returns a `PgTable` guard
/// plus the table name so the caller can inject it into a rivet YAML config.
pub fn seed_pg_numeric_table(row_count: i64) -> PgTable {
    let name = unique_name("rivet_qa_tbl");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {name} (
            id BIGINT PRIMARY KEY,
            name TEXT NOT NULL,
            amount NUMERIC(12,2) NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );"
    ))
    .expect("create test table");

    // Insert in a single VALUES statement — fast enough for row counts up to
    // a few thousand, which is all live tests need.
    if row_count > 0 {
        let mut sql = format!("INSERT INTO {name} (id, name, amount, created_at) VALUES ");
        for i in 0..row_count {
            if i > 0 {
                sql.push_str(", ");
            }
            // created_at spaced one second apart so cursor-based pagination
            // has something deterministic to walk.
            sql.push_str(&format!(
                "({i}, 'row_{i}', {:.2}, now() - ({} || ' seconds')::interval)",
                (i as f64) * 1.5,
                row_count - i
            ));
        }
        c.batch_execute(&sql).expect("seed rows");
    }

    PgTable::adopt(name)
}

/// Seed a wide-text Postgres table: `(id BIGINT, payload TEXT, updated_at TIMESTAMPTZ)`
/// where every row contains `payload_len` repetitions of 'x'.  Useful for triggering the
/// batch memory cap — with 2000 rows and 600-char payloads the Arrow StringArray
/// buffer is ~1.2 MB, reliably exceeding a `max_batch_memory_mb: 1` cap.
///
/// `payload_len = 0` uses the default of 600 characters.
pub fn seed_pg_wide_table(row_count: i64, payload_len: usize) -> PgTable {
    let payload_len = if payload_len == 0 { 600 } else { payload_len };
    let name = unique_name("rivet_wide_tbl");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {name} (
            id BIGINT PRIMARY KEY,
            payload TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );"
    ))
    .expect("create wide table");

    if row_count > 0 {
        c.batch_execute(&format!(
            "INSERT INTO {name} (id, payload, updated_at)
             SELECT g, repeat('x', {payload_len}),
                    now() - (interval '1 second') * ({row_count} + 1 - g)
             FROM generate_series(1, {row_count}) g;"
        ))
        .expect("seed wide table rows");
    }

    PgTable::adopt(name)
}

/// RAII guard for a logical replication slot — drops it on scope exit so an
/// aborted test never leaks a slot into `max_replication_slots`.
pub struct Slot(pub String);
impl Drop for Slot {
    fn drop(&mut self) {
        let url = std::env::var("POSTGRES_CDC_URL")
            .unwrap_or_else(|_| "postgresql://rivet:rivet@127.0.0.1:5434/rivet".to_string());
        if let Ok(mut c) = postgres::Client::connect(&url, postgres::NoTls) {
            let _ = c.execute("SELECT pg_drop_replication_slot($1)", &[&self.0]);
        }
    }
}

/// RAII guard for a SERVER-LEVEL PostgreSQL setting — reverts it on scope exit.
///
/// The STATE axis is the most productive one this repo has (the `timestamptz`
/// rendering corruption, `datestyle='German, DMY'` nulling every timestamp,
/// `bytea_output='escape'` corrupting every bytea, and PostgreSQL's two-phase
/// immunity were all settled by flipping one server setting), and until now it was
/// also the most expensive: every probe hand-rolled `ALTER SYSTEM SET`, a container
/// restart, the probe, and a revert — with the revert conditional on the test
/// actually reaching the end.
///
/// That conditional revert is the real hazard, not the typing. A hand-drifted
/// container is how this repo produced two false "verified" claims in one day, so
/// the revert belongs in `Drop` where a panic cannot skip it.
///
/// Two flavours because PostgreSQL has two kinds of setting: most are reloadable
/// (`pg_reload_conf`), while `PGC_POSTMASTER` ones — `max_prepared_transactions`,
/// `wal_level`, `max_replication_slots` — need the server restarted.
pub struct PgSetting {
    name: String,
    container: Option<&'static str>,
}

impl PgSetting {
    /// A reloadable setting: `ALTER SYSTEM SET` + `pg_reload_conf()`.
    pub fn set(name: &str, value: &str) -> Self {
        Self::apply(name, value);
        reload();
        Self {
            name: name.to_string(),
            container: None,
        }
    }

    /// A `PGC_POSTMASTER` setting: `ALTER SYSTEM SET` + a container restart, then
    /// waits for the server to accept connections again. `container` is the docker
    /// name (`rivet-postgres-cdc-1`).
    ///
    /// EXCLUSIVE. The restart kills every open connection to that stand, and
    /// `cargo test` runs tests in parallel — MEASURED: one restart-using test took
    /// an unrelated bounded-drain test down with it, which reads as a product
    /// failure and is a fixture breaking its neighbours. A caller must gate itself
    /// (an env var plus `--test-threads=1`) or not use this variant. `set` is free
    /// of the problem entirely, so prefer it whenever the setting is reloadable.
    pub fn set_with_restart(name: &str, value: &str, container: &'static str) -> Self {
        Self::apply(name, value);
        restart(container);
        Self {
            name: name.to_string(),
            container: Some(container),
        }
    }

    /// What the server reports for this setting RIGHT NOW — so a test can assert
    /// the flip took effect before drawing any conclusion from it. A probe that
    /// silently ran at the default is the "absence is not success" shape.
    pub fn current(name: &str) -> String {
        let mut c = connect();
        c.query_one(&format!("SHOW {name}"), &[])
            .map(|r| r.get::<_, String>(0))
            .unwrap_or_default()
    }

    fn apply(name: &str, value: &str) {
        let mut c = connect();
        c.batch_execute(&format!("ALTER SYSTEM SET {name} = '{value}'"))
            .unwrap_or_else(|e| panic!("ALTER SYSTEM SET {name}: {e}"));
    }
}

impl Drop for PgSetting {
    fn drop(&mut self) {
        if let Ok(mut c) = postgres::Client::connect(&url(), postgres::NoTls) {
            let _ = c.batch_execute(&format!("ALTER SYSTEM RESET {}", self.name));
        }
        match self.container {
            Some(name) => restart(name),
            None => reload(),
        }
    }
}

fn url() -> String {
    std::env::var("POSTGRES_CDC_URL")
        .unwrap_or_else(|_| "postgresql://rivet:rivet@127.0.0.1:5434/rivet".to_string())
}

fn connect() -> postgres::Client {
    postgres::Client::connect(&url(), postgres::NoTls).expect("connect postgres-cdc")
}

fn reload() {
    if let Ok(mut c) = postgres::Client::connect(&url(), postgres::NoTls) {
        let _ = c.batch_execute("SELECT pg_reload_conf()");
    }
}

/// Restart the container and WAIT for the server to answer — returning before it
/// is up turns every later step into a connection error that reads like a product
/// failure.
fn restart(container: &str) {
    let _ = std::process::Command::new("docker")
        .args(["restart", container])
        .output();
    for _ in 0..60 {
        if postgres::Client::connect(&url(), postgres::NoTls).is_ok() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    panic!("postgres container `{container}` did not accept connections after a restart");
}
