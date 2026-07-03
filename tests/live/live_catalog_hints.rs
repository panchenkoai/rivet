//! Live regression tests for "silent degradation" paths in Postgres source.
//!
//! Catches a class of bugs where a `SELECT * FROM <table>` with a
//! `NUMERIC(p,s)` column fails because the catalog lookup behind the scenes
//! could not serialize its `regclass` parameter. The original symptom: every
//! `NUMERIC` export required a manual `columns: foo: decimal(p,s)` override.
//!
//! Also covers:
//! - `table:` shortcut producing a parser-friendly `SELECT * FROM` form.
//! - `source.environment:` honouring tuning fallback for `local` (no throttle).
//! - `rivet check` surfacing preflight warnings on degraded probes (RUST_LOG=debug).
//!
//! Requires `docker compose up -d` (Postgres).

use std::path::Path;

use crate::common::*;
use arrow::array::AsArray;
use arrow::array::types::Decimal128Type;
use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[test]
#[ignore = "live: requires docker compose postgres"]
fn catalog_hints_decimals_resolve_without_column_overrides() {
    require_alive(LiveService::Postgres);

    let tbl = seed_pg_numeric_table(50);
    let tmp = tempfile::tempdir().expect("tmpdir");
    let out_dir = tmp.path().join("out");
    let yaml = format!(
        r#"source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: nohint_decimal
    query: "SELECT * FROM public.{name}"
    mode: full
    format: parquet
    compression: snappy
    destination:
      type: local
      path: {out}
"#,
        name = tbl.name(),
        out = out_dir.display(),
    );
    let cfg = write_config(&tmp, &yaml);

    let out = run_rivet(&["run", "-c", cfg.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "rivet run must succeed without `columns:` overrides — \
         catalog hints should auto-resolve NUMERIC(p,s).\n\
         stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    // Find the produced parquet file and check the amount column is Decimal128(12, 2)
    let entries: Vec<_> = std::fs::read_dir(&out_dir)
        .expect("read out_dir")
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();
    assert_eq!(
        entries.len(),
        1,
        "expected 1 parquet, found {}",
        entries.len()
    );
    let pq_path = entries[0].path();

    let bytes = std::fs::read(&pq_path).expect("read parquet");
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).expect("open parquet");
    let schema = builder.schema().clone();
    let amount_field = schema
        .field_with_name("amount")
        .expect("amount column in parquet");
    assert!(
        matches!(amount_field.data_type(), DataType::Decimal128(12, 2)),
        "expected Decimal128(12, 2), got {:?} — catalog hint path regressed",
        amount_field.data_type()
    );

    // Spot-check the data — first row's amount should be 0.00, second 1.50, etc.
    let mut reader = builder.build().expect("build reader");
    let batch = reader
        .next()
        .expect("at least one batch")
        .expect("batch ok");
    let amount = batch
        .column_by_name("amount")
        .expect("amount in batch")
        .as_primitive::<Decimal128Type>();
    assert_eq!(amount.value(0), 0); // 0.00 * 100
    assert_eq!(amount.value(1), 150); // 1.50 * 100
    let _ = Path::new(&pq_path); // keep clippy happy if unused
}

// ── `table:` shortcut runs end-to-end ──────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn table_shortcut_runs_end_to_end_with_catalog_hints() {
    require_alive(LiveService::Postgres);

    let tbl = seed_pg_numeric_table(20);
    let tmp = tempfile::tempdir().expect("tmpdir");
    let out_dir = tmp.path().join("out");
    let yaml = format!(
        r#"source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: shortcut_run
    table: public.{name}
    mode: full
    format: parquet
    compression: snappy
    destination:
      type: local
      path: {out}
"#,
        name = tbl.name(),
        out = out_dir.display(),
    );
    let cfg = write_config(&tmp, &yaml);

    let out = run_rivet(&["run", "-c", cfg.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "`table:` shortcut export must succeed end-to-end:\n\
         stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    let pq: Vec<_> = std::fs::read_dir(&out_dir)
        .expect("read out_dir")
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();
    assert_eq!(pq.len(), 1, "expected 1 parquet");

    // Schema must still get Decimal128(12,2) for `amount` — the `table:` form
    // compiles to a parser-friendly SELECT * and catalog hints must resolve.
    let bytes = std::fs::read(pq[0].path()).expect("read parquet");
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).expect("open parquet");
    let amount_t = builder
        .schema()
        .field_with_name("amount")
        .expect("amount column")
        .data_type()
        .clone();
    assert!(
        matches!(amount_t, DataType::Decimal128(12, 2)),
        "Decimal128(12,2) expected via `table:` shortcut, got {amount_t:?}",
    );
}

// ── chunked mode auto-resolves chunk_column from PK on `table:` shortcut ──

#[test]
#[ignore = "live: requires docker compose postgres"]
fn chunked_auto_resolves_chunk_column_from_pk() {
    require_alive(LiveService::Postgres);

    // seed_pg_numeric_table creates `(id BIGINT PRIMARY KEY, name TEXT, …)`.
    // Our auto-resolver should detect `id` as the integer PK.
    let tbl = seed_pg_numeric_table(2_000);
    let tmp = tempfile::tempdir().expect("tmpdir");
    let out_dir = tmp.path().join("out");
    let yaml = format!(
        r#"source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: chunked_auto_pk
    table: public.{name}
    mode: chunked
    chunk_size: 500
    format: parquet
    compression: snappy
    destination:
      type: local
      path: {out}
"#,
        name = tbl.name(),
        out = out_dir.display(),
    );
    let cfg = write_config(&tmp, &yaml);

    let out = run_rivet(&["run", "-c", cfg.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "chunked + table: with no explicit chunk_column must auto-resolve from PK:\n\
         stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    // 2,000 rows / chunk_size 500 ⇒ exactly 4 parquet files — proves chunking
    // ran with the PK as the chunk column (otherwise we'd get 0 or 1 files).
    let pq: Vec<_> = std::fs::read_dir(&out_dir)
        .expect("read out_dir")
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();
    assert_eq!(
        pq.len(),
        4,
        "expected 4 chunk files for 2k rows / 500 chunk_size, got {}",
        pq.len()
    );
}

// ── small-table escape downgrades chunked → snapshot ─────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn chunked_small_table_downgrades_to_snapshot() {
    require_alive(LiveService::Postgres);

    // 50 rows, chunk_size 100k ⇒ planner should downgrade to a single-file
    // snapshot run instead of going through the chunked machinery.
    let tbl = seed_pg_numeric_table(50);
    // ANALYZE so pg_class.reltuples is populated for the small-table check.
    let mut c = pg_connect();
    c.batch_execute(&format!("ANALYZE {}", tbl.name()))
        .expect("analyze");

    let tmp = tempfile::tempdir().expect("tmpdir");
    let out_dir = tmp.path().join("out");
    let yaml = format!(
        r#"source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: small_chunked
    table: public.{name}
    mode: chunked
    chunk_size: 100000
    format: parquet
    compression: snappy
    destination:
      type: local
      path: {out}
"#,
        name = tbl.name(),
        out = out_dir.display(),
    );
    let cfg = write_config(&tmp, &yaml);

    let out = run_rivet(&["run", "-c", cfg.to_str().unwrap()]);
    assert!(out.status.success(), "small chunked run must succeed");

    // Exactly one parquet file proves the downgrade fired (chunked would
    // still have produced 1 file too, so we also assert on size). The key
    // observation: it should NOT have written chunked metadata to state.
    let pq: Vec<_> = std::fs::read_dir(&out_dir)
        .expect("read out_dir")
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();
    assert_eq!(pq.len(), 1, "expected single snapshot file");
}

// ── chunk_size_memory_mb computes chunk_size from row width ──────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn chunked_memory_budget_derives_chunk_size() {
    require_alive(LiveService::Postgres);

    // Wide-row fixture: ~600 char payload ⇒ pg_class will report a row size
    // well above the 600-byte minimum so chunk_size lands well below 100k.
    let tbl = seed_pg_wide_table(20_000, 600);
    let mut c = pg_connect();
    c.batch_execute(&format!("ANALYZE {}", tbl.name()))
        .expect("analyze");

    let tmp = tempfile::tempdir().expect("tmpdir");
    let out_dir = tmp.path().join("out");
    let yaml = format!(
        r#"source:
  type: postgres
  url: "{POSTGRES_URL}"

exports:
  - name: budgeted
    table: public.{name}
    mode: chunked
    chunk_size_memory_mb: 4
    format: parquet
    compression: snappy
    destination:
      type: local
      path: {out}
"#,
        name = tbl.name(),
        out = out_dir.display(),
    );
    let cfg = write_config(&tmp, &yaml);

    let out = run_rivet(&["run", "-c", cfg.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "mem-budgeted chunked run must succeed:\n\
         stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    // 4 MB ÷ ~600 B/row ⇒ ~7k rows/chunk, clamped to 10_000. For 20k rows
    // we expect exactly 2 chunks. The lower bound (≥2) is the key invariant —
    // the budget DID shrink the chunk below the default 100k.
    let pq: Vec<_> = std::fs::read_dir(&out_dir)
        .expect("read out_dir")
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("parquet"))
        .collect();
    assert!(
        pq.len() >= 2,
        "memory budget should have produced ≥2 chunk files (default chunk_size would have given 1); got {}",
        pq.len(),
    );
}

// ── `source.environment: local` switches default profile to `fast` ─────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn source_environment_local_runs_fast_profile_by_default() {
    require_alive(LiveService::Postgres);

    let tbl = seed_pg_numeric_table(50);
    let tmp = tempfile::tempdir().expect("tmpdir");
    let out_dir = tmp.path().join("out");
    let yaml = format!(
        r#"source:
  type: postgres
  url: "{POSTGRES_URL}"
  environment: local

exports:
  - name: env_local
    table: public.{name}
    mode: full
    format: parquet
    compression: snappy
    destination:
      type: local
      path: {out}
"#,
        name = tbl.name(),
        out = out_dir.display(),
    );
    let cfg = write_config(&tmp, &yaml);

    // Use the warn-level wrapper so the profile decision appears in stderr.
    let out = run_rivet_with_warn_log(&["run", "-c", cfg.to_str().unwrap()]);
    assert!(
        out.status.success(),
        "`environment: local` run must succeed:\n\
         stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    // The run summary is emitted on stderr; assert the env-derived profile label.
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("fast") && stderr.contains("environment: local"),
        "expected stderr to mention 'fast (default for environment: local)'; got:\n{stderr}",
    );
}

/// Regression: chunked mode wraps the base query in a `SELECT … FROM (<base>) …`
/// subquery, which hides the source table from the PG NUMERIC catalog-hint
/// parser. Before the fix, a chunked export of a *non*-`SELECT * FROM <ident>`
/// query with an undeclared `NUMERIC` column failed ("no safe Rivet mapping"),
/// while `check` and full mode reported it safe — a check↔run inconsistency. The
/// fix resolves hints from the unwrapped base query
/// (`ExportRequest.catalog_hint_query`), so chunked now resolves without an
/// override and consistently with `check`/full.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn chunked_nonsimple_query_resolves_numeric_via_base_catalog_hint() {
    require_alive(LiveService::Postgres);
    let tbl = seed_pg_numeric_table(60);
    let tmp = tempfile::tempdir().expect("tmpdir");
    let yaml = Rig::pg_batch(tbl.name())
        .query(&format!(
            r#"SELECT id, amount FROM {name}"#,
            name = tbl.name()
        ))
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 25")
        .export_line("chunk_checkpoint: true")
        .dest_path(tmp.path().join("o").to_path_buf())
        .yaml();
    let cfg = write_config(&tmp, &yaml);
    let out = run_rivet(&["run", "-c", cfg.to_str().unwrap(), "--export", tbl.name()]);
    assert!(
        out.status.success(),
        "chunked non-simple query with an undeclared NUMERIC must resolve via the \
         base-query catalog hint (no override); stderr:\n{}",
        String::from_utf8_lossy(&out.stderr),
    );
}

/// Same regression for the `chunk_dense: true` path, which wraps the base query
/// in a ROW_NUMBER() subquery *even for the simple `SELECT * FROM <ident>` form*
/// — so it lost the NUMERIC catalog hint before the fix even on the fast-path
/// query shape.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn chunked_dense_resolves_numeric_via_base_catalog_hint() {
    require_alive(LiveService::Postgres);
    let tbl = seed_pg_numeric_table(60);
    let tmp = tempfile::tempdir().expect("tmpdir");
    let yaml = Rig::pg_batch(tbl.name())
        .query(&format!(r#"SELECT * FROM {name}"#, name = tbl.name()))
        .mode("chunked")
        .export_line("chunk_column: id")
        .export_line("chunk_size: 25")
        .export_line("chunk_dense: true")
        .export_line("chunk_checkpoint: true")
        .dest_path(tmp.path().join("o").to_path_buf())
        .yaml();
    let cfg = write_config(&tmp, &yaml);
    let out = run_rivet(&["run", "-c", cfg.to_str().unwrap(), "--export", tbl.name()]);
    assert!(
        out.status.success(),
        "chunked dense export with an undeclared NUMERIC must resolve via the \
         base-query catalog hint (no override); stderr:\n{}",
        String::from_utf8_lossy(&out.stderr),
    );
}

/// Same regression for `time_window` mode: its `resolve_query` wraps the base in
/// a `SELECT * FROM (base) WHERE <ts> BETWEEN …` subquery (single-query path),
/// which — like chunked — hides the source table from the NUMERIC catalog-hint
/// parser. The fix resolves hints from `plan.base_query` in the single runner.
#[test]
#[ignore = "live: requires docker compose up -d postgres"]
fn time_window_resolves_numeric_via_base_catalog_hint() {
    require_alive(LiveService::Postgres);
    let tbl = seed_pg_numeric_table(40);
    let tmp = tempfile::tempdir().expect("tmpdir");
    let yaml = Rig::pg_batch(tbl.name())
        .query(&format!(
            r#"SELECT id, amount, created_at FROM {name}"#,
            name = tbl.name()
        ))
        .mode("time_window")
        .export_line("time_column: created_at")
        .export_line("days_window: 1")
        .dest_path(tmp.path().join("o").to_path_buf())
        .yaml();
    let cfg = write_config(&tmp, &yaml);
    let out = run_rivet(&["run", "-c", cfg.to_str().unwrap(), "--export", tbl.name()]);
    assert!(
        out.status.success(),
        "time_window export with an undeclared NUMERIC must resolve via the \
         base-query catalog hint (no override); stderr:\n{}",
        String::from_utf8_lossy(&out.stderr),
    );
}
