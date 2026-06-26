//! OLTP load tests — verifies that Rivet exports correctly under concurrent
//! INSERT/UPDATE activity on the same table.
//!
//! These tests start a background writer thread that continuously inserts rows
//! while the main thread drives a Rivet export.  The goal is to confirm:
//!   - The export completes successfully under write pressure.
//!   - Row counts are internally consistent (no partial reads, no crashes).
//!   - Adaptive batch sizing is exercised when `tuning.adaptive = true`.
//!
//! ```text
//! docker compose up -d postgres mysql
//! cargo test --test live_suite -- --include-ignored
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::common::*;
use rivet::error::Result;
use rivet::source::mysql::MysqlSource;
use rivet::source::postgres::PostgresSource;
use rivet::source::{BatchSink, ExportRequest, Source};
use rivet::tuning::{SourceTuning, TuningConfig};
use rivet::types::{ColumnOverrides, RivetType};

/// Column overrides matching the `seed_pg_numeric_table` schema: `amount`
/// is `NUMERIC(12,2)` but `SELECT *` wrapped in a subquery loses precision
/// metadata, so Rivet rejects it. Providing the explicit type unblocks the
/// export under test (the test itself is about retry/streaming behaviour,
/// not about Rivet's NUMERIC-precision inference).
fn amount_override() -> ColumnOverrides {
    let mut o = ColumnOverrides::new();
    o.insert(
        "amount".to_string(),
        RivetType::Decimal {
            precision: 12,
            scale: 2,
        },
    );
    o
}

// ─── Counting sink ─────────────────────────────────────────────────────────

struct CountingSink {
    pub total_rows: usize,
    pub batch_count: usize,
}

impl CountingSink {
    fn new() -> Self {
        Self {
            total_rows: 0,
            batch_count: 0,
        }
    }
}

impl BatchSink for CountingSink {
    fn on_schema(&mut self, _: SchemaRef) -> Result<()> {
        Ok(())
    }
    fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.batch_count += 1;
        self.total_rows += batch.num_rows();
        Ok(())
    }
}

// ─── Tuning helpers ────────────────────────────────────────────────────────

fn tuning_balanced() -> SourceTuning {
    SourceTuning::from_config(None)
}

fn tuning_adaptive() -> SourceTuning {
    let cfg = TuningConfig {
        adaptive: Some(true),
        batch_size: Some(500),
        ..Default::default()
    };
    SourceTuning::from_config(Some(&cfg))
}

// ─── Postgres OLTP load tests ──────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn pg_export_survives_concurrent_inserts() {
    require_alive(LiveService::Postgres);

    // Seed a table with a modest initial row count.
    let tbl = seed_pg_numeric_table(5_000);
    let tname = tbl.name().to_string();

    // Background writer: keep inserting rows until the stop flag is set.
    let stop = Arc::new(AtomicBool::new(false));
    let stop_writer = Arc::clone(&stop);
    let tname_writer = tname.clone();
    let writer = std::thread::spawn(move || {
        use postgres::{Client, NoTls};
        let mut client = Client::connect(POSTGRES_URL, NoTls).expect("writer connect");
        let mut id: i64 = 1_000_000;
        while !stop_writer.load(Ordering::Relaxed) {
            let _ = client.execute(
                &format!(
                    "INSERT INTO {tname_writer} (id, name, amount, created_at)
                     VALUES ($1, 'oltp_row', 1.00, now())
                     ON CONFLICT (id) DO NOTHING"
                ),
                &[&id],
            );
            id += 1;
            std::thread::sleep(Duration::from_millis(2));
        }
    });

    // Export while the writer is running.
    let mut source = PostgresSource::connect(POSTGRES_URL).unwrap();
    let mut sink = CountingSink::new();
    let result = source.export(
        &ExportRequest {
            query: &format!("SELECT * FROM {tname}"),
            catalog_hint_query: None,
            incremental: None,
            cursor: None,
            tuning: &tuning_balanced(),
            column_overrides: &amount_override(),
            page_limit: None,
        },
        &mut sink,
    );

    stop.store(true, Ordering::Relaxed);
    writer.join().expect("writer thread panicked");

    result.expect("export should succeed under concurrent inserts");
    assert!(
        sink.total_rows >= 5_000,
        "expected at least 5000 rows, got {}",
        sink.total_rows
    );
    assert!(sink.batch_count >= 1, "expected at least one batch");
}

#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn pg_export_adaptive_under_write_pressure() {
    require_alive(LiveService::Postgres);

    // Larger table so we hit the adaptive sample interval (every 10 batches).
    let tbl = seed_pg_numeric_table(20_000);
    let tname = tbl.name().to_string();

    let stop = Arc::new(AtomicBool::new(false));
    let stop_writer = Arc::clone(&stop);
    let tname_writer = tname.clone();
    let writer = std::thread::spawn(move || {
        use postgres::{Client, NoTls};
        let mut client = Client::connect(POSTGRES_URL, NoTls).expect("writer connect");
        let mut id: i64 = 2_000_000;
        while !stop_writer.load(Ordering::Relaxed) {
            let _ = client.execute(
                &format!(
                    "INSERT INTO {tname_writer} (id, name, amount, created_at)
                     VALUES ($1, 'oltp_row', 2.00, now())
                     ON CONFLICT (id) DO NOTHING"
                ),
                &[&id],
            );
            id += 1;
            // High-frequency writes to maximise checkpoint pressure.
            std::thread::sleep(Duration::from_millis(1));
        }
    });

    let mut source = PostgresSource::connect(POSTGRES_URL).unwrap();
    let mut sink = CountingSink::new();
    let result = source.export(
        &ExportRequest {
            query: &format!("SELECT * FROM {tname}"),
            catalog_hint_query: None,
            incremental: None,
            cursor: None,
            tuning: &tuning_adaptive(),
            column_overrides: &amount_override(),
            page_limit: None,
        },
        &mut sink,
    );

    stop.store(true, Ordering::Relaxed);
    writer.join().expect("writer thread panicked");

    result.expect("adaptive export should succeed under write pressure");
    assert!(sink.total_rows >= 20_000);
    // Adaptive mode uses batch_size=500, so at least 40 batches expected.
    assert!(
        sink.batch_count >= 40,
        "expected ≥40 batches, got {}",
        sink.batch_count
    );
}

// ─── MySQL OLTP load tests ─────────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn mysql_export_survives_concurrent_inserts() {
    require_alive(LiveService::Mysql);

    let tbl = seed_mysql_numeric_table(5_000);
    let tname = tbl.name().to_string();

    let stop = Arc::new(AtomicBool::new(false));
    let stop_writer = Arc::clone(&stop);
    let tname_writer = tname.clone();
    let writer = std::thread::spawn(move || {
        use mysql::prelude::*;
        let pool = mysql::Pool::new(MYSQL_URL).expect("writer pool");
        let mut conn = pool.get_conn().expect("writer conn");
        let mut id: i64 = 1_000_000;
        while !stop_writer.load(Ordering::Relaxed) {
            let _ = conn.exec_drop(
                format!(
                    "INSERT IGNORE INTO {tname_writer} (id, name, amount, created_at)
                     VALUES (?, 'oltp_row', 1.00, NOW())"
                ),
                (id,),
            );
            id += 1;
            std::thread::sleep(Duration::from_millis(2));
        }
    });

    let mut source = MysqlSource::connect(MYSQL_URL).unwrap();
    let mut sink = CountingSink::new();
    let result = source.export(
        &ExportRequest {
            query: &format!("SELECT id, name, created_at FROM {tname}"),
            catalog_hint_query: None,
            incremental: None,
            cursor: None,
            tuning: &tuning_balanced(),
            column_overrides: &ColumnOverrides::default(),
            page_limit: None,
        },
        &mut sink,
    );

    stop.store(true, Ordering::Relaxed);
    writer.join().expect("writer thread panicked");

    result.expect("mysql export should succeed under concurrent inserts");
    assert!(
        sink.total_rows >= 5_000,
        "expected at least 5000 rows, got {}",
        sink.total_rows
    );
}

#[test]
#[ignore = "live: requires docker compose up -d mysql"]
fn mysql_export_adaptive_under_write_pressure() {
    require_alive(LiveService::Mysql);

    let tbl = seed_mysql_numeric_table(20_000);
    let tname = tbl.name().to_string();

    let stop = Arc::new(AtomicBool::new(false));
    let stop_writer = Arc::clone(&stop);
    let tname_writer = tname.clone();
    let writer = std::thread::spawn(move || {
        use mysql::prelude::*;
        let pool = mysql::Pool::new(MYSQL_URL).expect("writer pool");
        let mut conn = pool.get_conn().expect("writer conn");
        let mut id: i64 = 2_000_000;
        while !stop_writer.load(Ordering::Relaxed) {
            let _ = conn.exec_drop(
                format!(
                    "INSERT IGNORE INTO {tname_writer} (id, name, amount, created_at)
                     VALUES (?, 'oltp_row', 2.00, NOW())"
                ),
                (id,),
            );
            id += 1;
            std::thread::sleep(Duration::from_millis(1));
        }
    });

    let mut source = MysqlSource::connect(MYSQL_URL).unwrap();
    let mut sink = CountingSink::new();
    let result = source.export(
        &ExportRequest {
            query: &format!("SELECT id, name, created_at FROM {tname}"),
            catalog_hint_query: None,
            incremental: None,
            cursor: None,
            tuning: &tuning_adaptive(),
            column_overrides: &ColumnOverrides::default(),
            page_limit: None,
        },
        &mut sink,
    );

    stop.store(true, Ordering::Relaxed);
    writer.join().expect("writer thread panicked");

    result.expect("adaptive mysql export should succeed under write pressure");
    assert!(sink.total_rows >= 20_000);
    assert!(
        sink.batch_count >= 40,
        "expected ≥40 batches, got {}",
        sink.batch_count
    );
}
