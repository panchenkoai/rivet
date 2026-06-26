//! End-to-end Parquet round-trip tests against live Postgres.
//!
//! QA backlog Task 2.2.  The existing `tests/format_golden.rs` covers writer
//! correctness in isolation; this file goes one level up and exercises the
//! full pipeline:
//!
//!   Postgres → rivet (run) → Parquet on disk → Parquet reader → assertions
//!
//! Acceptance criteria (from backlog):
//!   - Round-trip output preserves expected schema.
//!   - No row loss.
//!   - Null handling matches source expectations.

use crate::common::*;
use arrow::array::{Array, AsArray, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// Run `rivet` against a seeded Postgres table and return the path of the
/// single Parquet file it produced.  Helper keeps each test focused on the
/// *assertion*, not the setup mechanics.
fn export_to_parquet(query: &str, out_dir: &std::path::Path) -> std::path::PathBuf {
    let export_name = unique_name("qa22");
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {export_name}
    query: "{query}"
    mode: full
    format: parquet
    compression: zstd
    columns:
      amount: "decimal(12,2)"
    destination:
      type: local
      path: {}
"#,
        out_dir.display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    let out = run_rivet_export(&cfg_path, &export_name);
    assert!(
        out.status.success(),
        "rivet exited {}; stderr:\n{}\nstdout:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr),
        String::from_utf8_lossy(&out.stdout),
    );

    let files = files_with_extension(out_dir, "parquet");
    assert_eq!(
        files.len(),
        1,
        "expected exactly one .parquet file, got {files:?}"
    );
    files.into_iter().next().unwrap()
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn full_export_round_trips_row_count_and_column_order() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(50);
    let out_dir = tempfile::tempdir().unwrap();

    let query = format!(
        "SELECT id, name, amount, created_at FROM {} ORDER BY id",
        table.name()
    );
    let parquet_path = export_to_parquet(&query, out_dir.path());

    let bytes = std::fs::read(&parquet_path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
    let schema = builder.schema().clone();
    let reader = builder.build().unwrap();

    // Column order and names must match the SELECT list exactly.
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        vec!["id", "name", "amount", "created_at"],
        "column order/names must round-trip through rivet verbatim"
    );

    // No row loss.
    let total: usize = reader.map(|b| b.unwrap().num_rows()).sum();
    assert_eq!(total, 50, "row count must survive full export");
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn full_export_preserves_string_and_null_distinction() {
    require_alive(LiveService::Postgres);

    // Build a purpose-specific table so we can seed NULLs and empty strings.
    let name = unique_name("qa22_nulls");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {name} (
            id BIGINT PRIMARY KEY,
            label TEXT  -- nullable
        );
        INSERT INTO {name} (id, label) VALUES
            (1, 'alice'),
            (2, ''),      -- empty string, distinct from NULL
            (3, NULL),
            (4, 'ελλάδα 🚀');"
    ))
    .unwrap();
    // RAII cleanup via inline drop at end of test (no PgTable since we built
    // the table manually).
    struct Cleanup(String);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            if let Ok(mut c) = postgres::Client::connect(POSTGRES_URL, postgres::NoTls) {
                let _ = c.execute(&format!("DROP TABLE IF EXISTS {}", self.0), &[]);
            }
        }
    }
    let _guard = Cleanup(name.clone());

    let out_dir = tempfile::tempdir().unwrap();
    let query = format!("SELECT id, label FROM {name} ORDER BY id");
    let parquet_path = export_to_parquet(&query, out_dir.path());

    let bytes = std::fs::read(&parquet_path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
    let mut reader = builder.build().unwrap();

    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 4);

    let col = batch.column_by_name("label").expect("label column");
    let strings = col
        .as_any()
        .downcast_ref::<StringArray>()
        .or_else(|| col.as_string_opt::<i32>())
        .expect("label must decode as utf8");
    assert_eq!(strings.value(0), "alice");
    assert!(
        !strings.is_null(1),
        "row 2: empty string must NOT be read back as NULL"
    );
    assert_eq!(strings.value(1), "");
    assert!(strings.is_null(2), "row 3: explicit NULL must stay NULL");
    assert_eq!(
        strings.value(3),
        "ελλάδα 🚀",
        "unicode payload must round-trip byte-for-byte"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn full_export_zero_row_table_succeeds_and_writes_no_file() {
    // Documented contract (pipeline/single.rs:177): when the source yields
    // zero rows, rivet exits 0 and does NOT create an output file — the sink
    // writer is never finalized.  `skip_empty` only toggles the summary
    // status between `"success"` (false) and `"skipped"` (true), it does
    // not control file materialisation.
    //
    // Test both branches.
    require_alive(LiveService::Postgres);

    for skip_empty in [false, true] {
        let table = seed_pg_numeric_table(0);
        let out_dir = tempfile::tempdir().unwrap();
        let export_name = unique_name("qa22_zero");
        let cfg_dir = tempfile::tempdir().unwrap();
        let yaml = format!(
            r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {export_name}
    query: "SELECT id, name FROM {table_name}"
    mode: full
    format: parquet
    compression: zstd
    skip_empty: {skip_empty}
    destination:
      type: local
      path: {dir}
"#,
            table_name = table.name(),
            dir = out_dir.path().display()
        );
        let cfg_path = write_config(&cfg_dir, &yaml);
        let out = run_rivet_export(&cfg_path, &export_name);
        assert!(
            out.status.success(),
            "rivet skip_empty={skip_empty} must exit 0 even for empty source; stderr:\n{}",
            String::from_utf8_lossy(&out.stderr),
        );
        let files = files_with_extension(out_dir.path(), "parquet");
        assert!(
            files.is_empty(),
            "empty source must produce zero output files regardless of skip_empty \
             (contract: pipeline/single.rs:177); skip_empty={skip_empty}, got: {files:?}"
        );
    }
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn full_export_with_validate_flag_matches_exported_row_count() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_numeric_table(13);
    let out_dir = tempfile::tempdir().unwrap();
    let export_name = unique_name("qa22_val");
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {export_name}
    query: "SELECT id, name, amount FROM {table_name}"
    mode: full
    format: parquet
    columns:
      amount: "decimal(12,2)"
    destination:
      type: local
      path: {dir}
"#,
        table_name = table.name(),
        dir = out_dir.path().display(),
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    // Add --validate so rivet opens the produced Parquet and recounts rows.
    let out = run_rivet(&[
        "run",
        "--config",
        cfg_path.to_str().unwrap(),
        "--export",
        &export_name,
        "--validate",
    ]);
    assert!(
        out.status.success(),
        "rivet --validate exited {}; stderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr),
    );

    let files = files_with_extension(out_dir.path(), "parquet");
    assert_eq!(files.len(), 1);
    let bytes = std::fs::read(&files[0]).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
    let total: usize = builder
        .build()
        .unwrap()
        .map(|b| b.unwrap().num_rows())
        .sum();
    assert_eq!(total, 13, "--validate must not alter row count");
}

/// Invariant audit gap #4: no successful run without final summary.
///
/// After a successful run, the per-run report artifacts
/// `<config_dir>/.rivet/runs/<run_id>/{summary.json,summary.md}` must
/// exist on disk. ADR-0001 I8 (Finalize Order) places the run-report
/// write last in the finalize sequence, but `finalize_run_report`
/// treats a write failure as non-fatal — the run keeps its success exit
/// code, and the operator loses observability silently. The runtime
/// invariant being pinned here is the existence of those files as a
/// consequence of `status == "success"`: any reordering of finalize
/// hooks that bypasses the report write would land in this test as a
/// missing-file assertion failure.
///
/// Complements the gap #2 / #3 unit gates: those pin the in-memory
/// shape of the `RunSummary`; this one pins the on-disk artifact.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn successful_run_writes_summary_artifacts_under_dot_rivet() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(10);
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let export_name = unique_name("gap4_summary_artifacts");
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {export_name}
    query: "SELECT id, name FROM {table_name}"
    mode: full
    format: parquet
    destination:
      type: local
      path: {dir}
"#,
        table_name = table.name(),
        dir = out_dir.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);
    let out = run_rivet_export(&cfg_path, &export_name);
    assert!(
        out.status.success(),
        "rivet must exit zero; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let runs_dir = cfg_dir.path().join(".rivet").join("runs");
    assert!(
        runs_dir.is_dir(),
        "I8: the run-report dir {runs_dir:?} must exist after a successful run"
    );

    let run_dirs: Vec<_> = std::fs::read_dir(&runs_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .collect();
    assert!(
        !run_dirs.is_empty(),
        "I8: at least one run subdirectory must exist under {runs_dir:?}"
    );

    for entry in &run_dirs {
        let dir = entry.path();
        let json_path = dir.join("summary.json");
        let md_path = dir.join("summary.md");
        assert!(
            json_path.is_file(),
            "I8 / gap #4: summary.json missing for run dir {dir:?}"
        );
        assert!(
            md_path.is_file(),
            "I8 / gap #4: summary.md missing for run dir {dir:?}"
        );

        // Sanity-check that the persisted summary reflects a successful run —
        // catches a regression where the JSON is written but the status field
        // got dropped or mis-serialized.
        let parsed: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&json_path).unwrap())
                .unwrap_or_else(|e| panic!("summary.json at {json_path:?} must parse: {e}"));
        assert_eq!(
            parsed.get("status").and_then(|v| v.as_str()),
            Some("success"),
            "summary.json at {json_path:?} must report status=success for a successful run"
        );
    }
}
