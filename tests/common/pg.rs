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
}

impl PgTable {
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Drop for PgTable {
    fn drop(&mut self) {
        // Best-effort cleanup: if the drop fails we've already caused more
        // damage than this can unwind.  Do NOT panic from Drop.
        if let Ok(mut c) = PgClient::connect(POSTGRES_URL, NoTls) {
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

    PgTable { name }
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

    PgTable { name }
}
