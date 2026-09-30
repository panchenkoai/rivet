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
    let rig = Rig::pg_batch(&export_name)
        .query(query)
        .export_line("compression: zstd")
        .export_line("columns:")
        .export_line("  amount: \"decimal(12,2)\"")
        .dest_path(out_dir.to_path_buf());

    let out = rig.run_args(&["--export", &export_name]);
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
        let rig = Rig::pg_batch(&export_name)
            .query(&format!("SELECT id, name FROM {}", table.name()))
            .export_line("compression: zstd")
            .export_line(&format!("skip_empty: {skip_empty}"))
            .dest_path(out_dir.path().to_path_buf());
        let out = rig.run_args(&["--export", &export_name]);
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
    let rig = Rig::pg_batch(&export_name)
        .query(&format!("SELECT id, name, amount FROM {}", table.name()))
        .export_line("columns:")
        .export_line("  amount: \"decimal(12,2)\"")
        .dest_path(out_dir.path().to_path_buf());

    // Add --validate so rivet opens the produced Parquet and recounts rows.
    let out = rig.run_args(&["--export", &export_name, "--validate"]);
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
    let export_name = unique_name("gap4_summary_artifacts");
    let rig = Rig::pg_batch(&export_name)
        .query(&format!("SELECT id, name FROM {}", table.name()))
        .dest_path(out_dir.path().to_path_buf());
    let cfg_path = rig.config_path();
    let out = rig.run_args(&["--export", &export_name]);
    assert!(
        out.status.success(),
        "rivet must exit zero; stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );

    let runs_dir = cfg_path.parent().unwrap().join(".rivet").join("runs");
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

/// Seed `(id int8, <cols>)` on the primary and return its drop guard.
fn seed_pg_override_table(cols: &str, values: &str) -> PgTable {
    let name = unique_name("pg_ovr");
    pg_connect()
        .batch_execute(&format!(
            "CREATE TABLE {name} (id int8 PRIMARY KEY, {cols}); INSERT INTO {name} VALUES {values};"
        ))
        .expect("seed override table");
    PgTable::adopt(name)
}

/// Run an export under one `columns:` override and return (exit code, combined output).
fn run_pg_override(table: &PgTable, override_line: &str) -> (Option<i32>, String) {
    let out = Rig::pg_batch(table.name())
        .export_line("columns:")
        .export_line(override_line)
        .run();
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.code(), said)
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn an_int2_and_int4_column_widened_by_an_int8_override_round_trip_exactly() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_override_table(
        "a int2 NOT NULL, b int4 NOT NULL, c int2 NOT NULL, d int4 NOT NULL",
        "(1, -32768, -2147483648, 7, 70000), (2, 32767, 2147483647, -7, -70000), (3, -1, 0, 32767, 5)",
    );
    let rig = Rig::pg_batch(table.name())
        .export_line("columns:")
        .export_line("  a: int8")
        .export_line("  b: int8");
    rig.run_ok();
    let out = rig.out_dir();
    let mut src = pg_connect();
    for col in ["a", "b"] {
        assert_eq!(
            parquet_column_type(&out, col),
            arrow::datatypes::DataType::Int64
        );
        let mut want: Vec<i64> = src
            .query(&format!("SELECT {col}::int8 FROM {}", table.name()), &[])
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect();
        let mut got = duckdb_dir_parquet_i64(&out, col);
        want.sort();
        got.sort();
        assert_eq!(got, want, "column {col} must round-trip the source values");
    }
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn an_int2_override_on_an_int8_value_that_does_not_fit_is_refused_by_name() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_override_table("v int8", "(1, 7), (2, 40000)");
    let (code, said) = run_pg_override(&table, "  v: int2");
    assert_ne!(code, Some(101), "rivet panicked:\n{said}");
    assert_ne!(code, Some(0), "an overflowing narrowing must fail:\n{said}");
    assert!(
        said.contains("holds 40000, which does not fit the int2"),
        "the refusal must name the value and the declared width:\n{said}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_bool_override_on_an_integer_column_is_refused_by_name() {
    require_alive(LiveService::Postgres);
    let table = seed_pg_override_table("v int4", "(1, 1)");
    let (code, said) = run_pg_override(&table, "  v: bool");
    assert_ne!(code, Some(101), "rivet panicked:\n{said}");
    assert_ne!(code, Some(0), "a bool override on int must fail:\n{said}");
    assert!(
        said.contains("is declared bool by a `columns:` override but PostgreSQL sends it as int4"),
        "the refusal must name the override and the wire type:\n{said}"
    );
}

const SQLASCII_URL: &str = "postgresql://rivet:rivet@127.0.0.1:5432/rivet_sqlascii";

/// Invalid UTF-8 from a SQL_ASCII database (query-side client_encoding switch) is refused by name, not a panic.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_text_value_that_is_not_utf8_is_refused_by_name_not_a_panic() {
    require_alive(LiveService::Postgres);
    // A concurrent creator loses with duplicate_database; the connect below is the real check.
    let _ = pg_connect().batch_execute(
        "CREATE DATABASE rivet_sqlascii ENCODING 'SQL_ASCII' LC_COLLATE 'C' LC_CTYPE 'C' \
         TEMPLATE template0",
    );
    let t = unique_name("pg_sqlascii");
    postgres::Client::connect(SQLASCII_URL, postgres::NoTls)
        .expect("connect to rivet_sqlascii")
        .batch_execute(&format!(
            r"CREATE TABLE {t} (id int8 PRIMARY KEY, note text);
              INSERT INTO {t} VALUES (1, 'ok'), (2, E'caf\351');"
        ))
        .expect("seed SQL_ASCII table");
    let table = PgTable::adopt_on(SQLASCII_URL, t);
    let t = table.name();
    let out = Rig::pg_batch(t)
        .source_url(SQLASCII_URL)
        .query(&format!(
            "SELECT * FROM {t} WHERE set_config('client_encoding', 'SQL_ASCII', false) IS NOT NULL"
        ))
        .run();
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_ne!(out.status.code(), Some(101), "rivet panicked:\n{said}");
    assert_ne!(
        out.status.code(),
        Some(0),
        "invalid UTF-8 must fail the export:\n{said}"
    );
    assert!(
        said.contains("column `note` (text) holds a value that is not valid UTF-8"),
        "the refusal must name the column and the cause:\n{said}"
    );
}
