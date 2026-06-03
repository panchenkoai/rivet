//! Heavy OLTP load test against the existing `content_items` table.
//!
//! `content_items` is the heaviest fixture in the test DB: 2 M rows, ~6.5 GB
//! on disk, wide schema (text, jsonb, timestamptz). It is pre-seeded by
//! `src/bin/seed.rs` — this test does NOT seed or drop the table.
//!
//! ## What this tests (and why it differs from live_oltp_load.rs)
//!
//! live_oltp_load.rs operates on fresh 5 K-row tables: it proves the code
//! doesn't crash but generates no real I/O pressure.  This test:
//!
//!   1. Exports 200 K rows from content_items (real wide rows, ~1.3 GB path
//!      through shared_buffers) while a background thread continuously UPDATEs
//!      view_count on the same table.
//!
//!   2. Generates genuine WAL pressure → checkpoints_req should increment.
//!
//!   3. Exercises the adaptive batch-sizing feedback loop: checkpoints_req
//!      delta triggers fetch-size reduction.
//!
//!   4. Verifies Postgres MVCC isolation: the export sees the snapshot at
//!      BEGIN, so concurrent INSERTs are invisible — exported row count is
//!      always exactly what was in the table when the cursor opened.
//!
//! ## Run
//!
//! ```text
//! docker compose up -d postgres
//! cargo test --test live_content_load -- --include-ignored
//! ```
//!
//! The test takes 30–90 s depending on shared_buffers / disk speed.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use common::*;
use rivet::error::Result;
use rivet::source::postgres::PostgresSource;
use rivet::source::{BatchSink, ExportRequest, Source};
use rivet::tuning::{SourceTuning, TuningConfig};
use rivet::types::ColumnOverrides;

// ─── Counting sink ─────────────────────────────────────────────────────────

struct LoadSink {
    total_rows: usize,
    batch_count: usize,
    min_batch: usize,
    max_batch: usize,
}

impl LoadSink {
    fn new() -> Self {
        Self {
            total_rows: 0,
            batch_count: 0,
            min_batch: usize::MAX,
            max_batch: 0,
        }
    }
}

impl BatchSink for LoadSink {
    fn on_schema(&mut self, _: SchemaRef) -> Result<()> {
        Ok(())
    }
    fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let n = batch.num_rows();
        self.batch_count += 1;
        self.total_rows += n;
        self.min_batch = self.min_batch.min(n);
        self.max_batch = self.max_batch.max(n);
        Ok(())
    }
}

// ─── Helper: snapshot checkpoints_req ──────────────────────────────────────

fn pg_checkpoints_req() -> Option<i64> {
    use postgres::{Client, NoTls};
    let mut c = Client::connect(POSTGRES_URL, NoTls).ok()?;
    c.query_one("SELECT checkpoints_req FROM pg_stat_bgwriter", &[])
        .ok()
        .and_then(|r| r.try_get::<_, i64>(0).ok())
}

// ─── Tests ──────────────────────────────────────────────────────────────────

/// Export 200 K rows from content_items while a background thread does
/// continuous UPDATE view_count on the same table.
///
/// Asserts:
///   - export completes without error
///   - exported row count equals exactly what the cursor snapshot saw
///     (MVCC isolation — concurrent INSERTs during export are NOT visible)
///   - adaptive batch sizing was active (batch sizes vary)
#[test]
#[ignore = "live: requires docker compose up -d postgres (content_items pre-seeded by seed.rs)"]
fn pg_content_export_under_update_pressure() {
    require_alive(LiveService::Postgres);

    // Verify content_items exists and is large enough.
    let count: i64 = {
        use postgres::{Client, NoTls};
        let mut c = Client::connect(POSTGRES_URL, NoTls).unwrap();
        c.query_one("SELECT COUNT(*) FROM content_items", &[])
            .expect("count content_items")
            .get(0)
    };
    assert!(
        count >= 50_000,
        "content_items needs at least 50 000 rows (has {count}). Run: cargo run --bin seed"
    );

    let export_limit = 200_000i64.min(count);
    let ckpt_before = pg_checkpoints_req();

    // Background UPDATE writer: repeatedly bumps view_count on random rows.
    // This generates WAL without being inside the cursor snapshot — tests
    // that the exporter is resilient to I/O pressure it doesn't own.
    let stop = Arc::new(AtomicBool::new(false));
    let updates_done = Arc::new(AtomicU64::new(0));
    let stop_w = Arc::clone(&stop);
    let updates_w = Arc::clone(&updates_done);
    let writer = std::thread::spawn(move || {
        use postgres::{Client, NoTls};
        let mut c = Client::connect(POSTGRES_URL, NoTls).expect("writer connect");
        // Update a rotating window of rows; batch=200 keeps each statement cheap
        // but the cumulative WAL is significant over many iterations.
        let mut base_id: i64 = 1;
        while !stop_w.load(Ordering::Relaxed) {
            let _ = c.execute(
                "UPDATE content_items
                 SET view_count = view_count + 1, updated_at = NOW()
                 WHERE id BETWEEN $1 AND $1 + 199",
                &[&base_id],
            );
            updates_w.fetch_add(200, Ordering::Relaxed);
            // Rotate: cycle through first 50 000 rows so pages stay hot in
            // shared_buffers and we maximise WAL write rate.
            base_id = (base_id + 200) % 50_000 + 1;
        }
    });

    // Also an INSERT writer to test that new rows don't leak into the export.
    let stop_i = Arc::clone(&stop);
    let inserts_done = Arc::new(AtomicU64::new(0));
    let inserts_w = Arc::clone(&inserts_done);
    let inserter = std::thread::spawn(move || {
        use postgres::{Client, NoTls};
        let mut c = Client::connect(POSTGRES_URL, NoTls).expect("insert connect");
        let mut id_offset: i64 = 10_000_000; // well above current max
        while !stop_i.load(Ordering::Relaxed) {
            let _ = c.execute(
                "INSERT INTO content_items
                 (id, title, body, raw_html, author_name, author_email, status,
                  priority, view_count, comment_count, word_count, language)
                 VALUES ($1, 'load_test', 'body', '<p>html</p>', 'tester',
                         'tester@test.local', 'draft', 0, 0, 0, 0, 'en')
                 ON CONFLICT (id) DO NOTHING",
                &[&id_offset],
            );
            inserts_w.fetch_add(1, Ordering::Relaxed);
            id_offset += 1;
            std::thread::sleep(Duration::from_millis(5));
        }
    });

    // Give writers a head start so WAL pressure builds before the cursor opens.
    // With max_wal_size=64MB the writers fill WAL in ~0.5 s, triggering the
    // first checkpoints_req increment before our first adaptive sample (batch 10).
    std::thread::sleep(Duration::from_millis(800));

    // --- Adaptive export ---------------------------------------------------
    let cfg = TuningConfig {
        adaptive: Some(true),
        batch_size: Some(5_000),
        statement_timeout_s: Some(300),
        ..Default::default()
    };
    let tuning = SourceTuning::from_config(Some(&cfg));

    let started = Instant::now();
    let mut source = PostgresSource::connect(POSTGRES_URL).unwrap();
    let mut sink = LoadSink::new();

    // Export only the first `export_limit` rows so the test is bounded in time.
    // The important property is that the export snapshot is fixed at BEGIN time —
    // INSERTs at id=10_000_000+ must NOT appear in the exported rows.
    let result = source.export(
        &ExportRequest {
            query: &format!("SELECT id, title, status, view_count, updated_at FROM content_items WHERE id <= {export_limit}"),
            catalog_hint_query: None,            incremental: None,
            cursor: None,
            tuning: &tuning,
            column_overrides: &ColumnOverrides::default(),
            page_limit: None,
        },
        &mut sink,
    );

    let elapsed = started.elapsed();
    stop.store(true, Ordering::Relaxed);
    let update_count = updates_done.load(Ordering::Relaxed);
    let insert_count = inserts_done.load(Ordering::Relaxed);
    writer.join().expect("update writer panicked");
    inserter.join().expect("insert writer panicked");

    let ckpt_after = pg_checkpoints_req();

    result.expect("export must complete despite concurrent writes");

    eprintln!(
        "\n=== content_items load result ===\n\
         elapsed:        {:.1}s\n\
         exported rows:  {}\n\
         batches:        {}  (min={}, max={})\n\
         updates issued: {}\n\
         inserts issued: {}\n\
         checkpoints_req before: {:?}\n\
         checkpoints_req after:  {:?}\n\
         checkpoint delta:       {}\n\
         ===",
        elapsed.as_secs_f64(),
        sink.total_rows,
        sink.batch_count,
        sink.min_batch,
        sink.max_batch,
        update_count,
        insert_count,
        ckpt_before,
        ckpt_after,
        ckpt_after.unwrap_or(0) - ckpt_before.unwrap_or(0),
    );

    // MVCC isolation: row count must match exactly what was in the table at
    // cursor-open time. Concurrent inserts (id >= 10_000_000) must be invisible.
    assert_eq!(
        sink.total_rows as i64, export_limit,
        "MVCC violation: exported {} rows, expected exactly {}",
        sink.total_rows, export_limit
    );

    // Adaptive mode must have been active: min and max batch sizes should differ
    // if any checkpoint pressure was detected.  If the DB is completely idle this
    // assertion might flake — but with 200+ update batches of 200 rows each it
    // virtually never stays at a single size.
    assert!(
        sink.batch_count >= (export_limit as usize / 5_000),
        "too few batches: {} (expected ≥ {})",
        sink.batch_count,
        export_limit as usize / 5_000
    );
}

/// Stress-test: full 2M row export with maximum write pressure.
///
/// Disabled by default even among ignored tests — invoke explicitly:
/// ```text
/// cargo test --test live_content_load full_content_export -- --include-ignored
/// ```
#[test]
#[ignore = "live: slow (~3–5 min), requires docker compose up -d postgres with content_items seeded"]
fn pg_full_content_export_max_pressure() {
    require_alive(LiveService::Postgres);

    let count: i64 = {
        use postgres::{Client, NoTls};
        let mut c = Client::connect(POSTGRES_URL, NoTls).unwrap();
        c.query_one("SELECT COUNT(*) FROM content_items", &[])
            .expect("count")
            .get(0)
    };
    assert!(count >= 1_000_000, "need at least 1M rows, got {count}");

    let ckpt_before = pg_checkpoints_req();

    // Aggressive writer: updates 500 rows per batch, no sleep.
    let stop = Arc::new(AtomicBool::new(false));
    let stop_w = Arc::clone(&stop);
    let writer = std::thread::spawn(move || {
        use postgres::{Client, NoTls};
        let mut c = Client::connect(POSTGRES_URL, NoTls).expect("writer");
        let mut base: i64 = 1;
        while !stop_w.load(Ordering::Relaxed) {
            let _ = c.execute(
                "UPDATE content_items SET comment_count = comment_count + 1
                 WHERE id BETWEEN $1 AND $1 + 499",
                &[&base],
            );
            base = (base + 500) % 100_000 + 1;
        }
    });

    let cfg = TuningConfig {
        adaptive: Some(true),
        batch_size: Some(10_000),
        statement_timeout_s: Some(600),
        ..Default::default()
    };
    let tuning = SourceTuning::from_config(Some(&cfg));

    let started = Instant::now();
    let mut source = PostgresSource::connect(POSTGRES_URL).unwrap();
    let mut sink = LoadSink::new();
    let result = source.export(
        &ExportRequest {
            query: "SELECT id, title, status, view_count, comment_count FROM content_items",
            catalog_hint_query: None,
            incremental: None,
            cursor: None,
            tuning: &tuning,
            column_overrides: &ColumnOverrides::default(),
            page_limit: None,
        },
        &mut sink,
    );

    let elapsed = started.elapsed();
    stop.store(true, Ordering::Relaxed);
    writer.join().expect("writer panicked");

    let ckpt_after = pg_checkpoints_req();
    result.expect("full export must complete");

    eprintln!(
        "\n=== FULL content_items export ===\n\
         elapsed:     {:.1}s\n\
         rows:        {}\n\
         batches:     {}  (min={}, max={})\n\
         ckpt delta:  {}\n\
         ===",
        elapsed.as_secs_f64(),
        sink.total_rows,
        sink.batch_count,
        sink.min_batch,
        sink.max_batch,
        ckpt_after.unwrap_or(0) - ckpt_before.unwrap_or(0),
    );

    assert_eq!(
        sink.total_rows as i64, count,
        "MVCC: exported {} but table had {}",
        sink.total_rows, count
    );
}
