//! ROAST-RED live-pg-json-fidelity: PG json/jsonb cells are decoded via a
//! `serde_json::Value` round-trip (`src/source/postgres/arrow_convert.rs`,
//! `Type::JSON | Type::JSONB` arm of `build_pg_text_array`), which destroys
//! byte fidelity:
//!
//!   (a) non-integer JSON numbers pass through `f64`, so values with more
//!       than ~17 significant digits are silently altered;
//!   (b) for PG `json` (not jsonb) the column stores the ORIGINAL TEXT,
//!       whitespace included, but re-serialization normalizes it away.
//!
//! Key order is NOT tested here — `preserve_order` is active crate-wide, so
//! order already survives. These tests assert the CORRECT behavior (byte /
//! digit fidelity) and are expected to FAIL until the fix lands.

use crate::common::*;
use arrow::array::{Array, StringArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use postgres::NoTls;

/// The exact source text seeded into the PG `json` column: a number with 23
/// significant digits (unrepresentable in f64) and a DOUBLE space before "x".
/// PG `json` stores this verbatim; a fidelity-preserving export must too.
const ORIGINAL_JSON_TEXT: &str = r#"{"amount": 0.12345678901234567890123, "note":  "x"}"#;

/// Digit run that survives PG `jsonb`'s numeric storage losslessly but not a
/// round-trip through `f64` (which yields 0.12345678901234568).
const HIGH_PRECISION_DIGITS: &str = "0.12345678901234567890123";

/// RAII drop guard — same shape as the other live test binaries (each test
/// binary defines its own; `tests/common` deliberately exposes only the
/// canonical seeders).
struct PgCleanup(String);

impl Drop for PgCleanup {
    fn drop(&mut self) {
        if let Ok(mut c) = postgres::Client::connect(POSTGRES_URL, NoTls) {
            let _ = c.execute(&format!("DROP TABLE IF EXISTS {}", self.0), &[]);
        }
    }
}

/// Export `SELECT id, doc FROM <table> ORDER BY id` to a local Parquet file
/// and return the single string cell of the `doc` column.
fn export_doc_column_cell(table_name: &str, prefix: &str) -> String {
    let export_name = unique_name(prefix);
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {export_name}
    query: "SELECT id, doc FROM {table_name} ORDER BY id"
    mode: full
    format: parquet
    compression: zstd
    destination:
      type: local
      path: {out_dir}
"#,
        out_dir = out_dir.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    let out = run_rivet_export(&cfg_path, &export_name);
    assert!(
        out.status.success(),
        "rivet exited {}; stderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stderr),
    );

    let files = files_with_extension(out_dir.path(), "parquet");
    assert_eq!(
        files.len(),
        1,
        "expected exactly one .parquet file, got {files:?}"
    );

    let bytes = std::fs::read(&files[0]).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
    let mut reader = builder.build().unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1, "single seeded row must round-trip");

    let doc = batch
        .column_by_name("doc")
        .expect("doc column present in exported parquet")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("doc must decode as utf8");
    assert!(!doc.is_null(0), "doc cell must not be NULL");
    doc.value(0).to_owned()
}

// ROAST-RED live-pg-json-fidelity: PG `json` column text is re-serialized via
// serde_json::Value, losing whitespace bytes and >17-sig-digit number precision.
// Asserts CORRECT behavior; expected to FAIL until the fix lands.
#[test]
#[ignore = "live: postgres"]
fn roast_pg_json_column_preserves_source_bytes_through_parquet() {
    require_alive(LiveService::Postgres);

    let table_name = unique_name("roast_json_bytes");
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, doc JSON); \
         INSERT INTO {table_name} (id, doc) VALUES (1, '{ORIGINAL_JSON_TEXT}');"
    ))
    .unwrap();
    let _guard = PgCleanup(table_name.clone());

    // Precondition: PG `json` (unlike jsonb) stores the input text verbatim,
    // so any divergence in the export is rivet's doing, not Postgres's.
    let echoed: String = c
        .query_one(&format!("SELECT doc::text FROM {table_name}"), &[])
        .unwrap()
        .get(0);
    assert_eq!(
        echoed, ORIGINAL_JSON_TEXT,
        "precondition: PG json column must hold the source text verbatim"
    );

    let exported = export_doc_column_cell(&table_name, "roast_json_run");
    assert_eq!(
        exported, ORIGINAL_JSON_TEXT,
        "PG `json` column must round-trip byte-for-byte (whitespace + full \
         number precision); rivet's serde_json::Value re-serialization \
         produced {exported:?} instead"
    );
}

// ROAST-RED live-pg-json-fidelity: jsonb numbers are decoded through f64, so
// PG's losslessly-stored 23-significant-digit numeric is silently rounded.
// Asserts CORRECT behavior; expected to FAIL until the fix lands.
#[test]
#[ignore = "live: postgres"]
fn roast_pg_jsonb_high_precision_number_survives_export() {
    require_alive(LiveService::Postgres);

    let table_name = unique_name("roast_jsonb_num");
    let doc_literal = format!(r#"{{"amount": {HIGH_PRECISION_DIGITS}}}"#);
    let mut c = pg_connect();
    c.batch_execute(&format!(
        "CREATE TABLE {table_name} (id BIGINT PRIMARY KEY, doc JSONB); \
         INSERT INTO {table_name} (id, doc) VALUES (1, '{doc_literal}');"
    ))
    .unwrap();
    let _guard = PgCleanup(table_name.clone());

    // Precondition: jsonb stores numbers as PG `numeric`, so the full digit
    // run survives inside Postgres itself.
    let echoed: String = c
        .query_one(&format!("SELECT doc::text FROM {table_name}"), &[])
        .unwrap()
        .get(0);
    assert!(
        echoed.contains(HIGH_PRECISION_DIGITS),
        "precondition: jsonb must keep the numeric digits losslessly, got {echoed:?}"
    );

    let exported = export_doc_column_cell(&table_name, "roast_jsonb_run");
    assert!(
        exported.contains(HIGH_PRECISION_DIGITS),
        "jsonb numeric precision lost: exported cell is {exported:?}, which no \
         longer contains the source digits {HIGH_PRECISION_DIGITS} (rounded \
         through f64 by the serde_json::Value decode path)"
    );
}
