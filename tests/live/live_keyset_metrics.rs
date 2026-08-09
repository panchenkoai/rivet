//! #151: keyset runs must record the same metric FIELDS chunked runs do —
//! `parallel`, `cursor_min`/`cursor_max`, and the v23 `bytes_read` — asserted
//! ON the run's own metrics row (never re-derived in the test). The field DB
//! had `parallel` NULL on every keyset run (4 worker ranges demonstrably ran)
//! and cursor bounds NULL on 0 of 51: the runner-bypass class, in metrics.

use crate::common::*;

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn keyset_run_records_parallel_cursor_bounds_and_bytes_read() {
    require_alive(LiveService::Postgres);
    let table = unique_name("km_pg");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {table};
         CREATE TABLE {table} (id BIGINT PRIMARY KEY, payload INT NOT NULL);
         INSERT INTO {table} SELECT g, g FROM generate_series(1, 2000) g;"
    ))
    .unwrap();
    let rig = Rig::pg_batch(&format!("public.{table}"))
        .mode("chunked")
        .export_line("chunk_by_key: id")
        .export_line("parallel: 2")
        .export_line("chunk_size: 400");
    rig.run_ok();

    // THE metrics row — the artifact the product wrote, not a re-derivation.
    let db = rig.config_path().parent().unwrap().join(".rivet_state.db");
    let conn = rusqlite::Connection::open(&db).unwrap();
    let (parallel, cur_min, cur_max, bytes_read): (
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<i64>,
    ) = conn
        .query_row(
            "SELECT parallel, cursor_min, cursor_max, bytes_read FROM export_metrics \
             WHERE status='success' ORDER BY id DESC LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap();
    assert_eq!(parallel, Some(2), "#151-1: keyset must record its fan-out");
    assert_eq!(
        cur_min.as_deref(),
        Some("1"),
        "#151-2: the observed floor (density derivable from the DB alone)"
    );
    assert_eq!(cur_max.as_deref(), Some("2000"), "#151-2: the high-water");
    assert!(
        bytes_read.unwrap_or(0) > 0,
        "#151-3: the read leg's cost must be recorded"
    );
    let _ = c.execute(&format!("DROP TABLE IF EXISTS {table}"), &[]);
}
