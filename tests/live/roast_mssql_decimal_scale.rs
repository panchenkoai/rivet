//! ROAST-RED live-mssql-scale-freeze: SQL Server decimal scale must not freeze
//! at 0 when the first streamed batch is all NULL.
//!
//! tiberius drops a decimal column's declared precision/scale, so the MSSQL
//! source defaults `Decimaln`/`Numericn` to `Decimal{38,0}` and recovers the
//! real scale via `decimal_scale_from_rows` over the FIRST emitted batch only
//! (the `PROBE_BATCH_SIZE = 500` probe). When those rows are all NULL the
//! schema freezes at scale 0 and every later non-null value goes through
//! `rescale_i128`'s silent integer division: 123.45 exports as 123 — cents
//! gone, no error (`src/source/mssql/arrow_convert.rs`, `mod.rs`).
//!
//! This test seeds 600 leading-NULL rows (> the 500-row probe) followed by one
//! `123.45` and asserts the exported Parquet still carries exactly 123.45.

use crate::common::*;
use arrow::array::types::Decimal128Type;
use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{DataType, SchemaRef};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn read_parquet_batches(path: &std::path::Path) -> (SchemaRef, Vec<RecordBatch>) {
    let bytes = std::fs::read(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes)).unwrap();
    let schema = builder.schema().clone();
    let batches: Vec<RecordBatch> = builder.build().unwrap().map(|b| b.unwrap()).collect();
    (schema, batches)
}

/// RAII drop guard for the ad-hoc fixture table (same pattern as
/// `tests/live_mssql_resume.rs`'s `MssqlCursorCleanup`).
struct MssqlCleanup(String);
impl Drop for MssqlCleanup {
    fn drop(&mut self) {
        mssql_drop_table(&self.0);
    }
}

// ROAST-RED live-mssql-scale-freeze: MSSQL Decimaln/Numericn defaults to
// Decimal{38,0}; the real scale is inferred from the FIRST emitted batch only
// (~500 probe rows). If that batch is all NULL the schema freezes at scale 0
// and later non-null values hit rescale_i128's silent integer division —
// DECIMAL(10,2) 123.45 exports as 123.
// Asserts CORRECT behavior; expected to FAIL until the fix lands.
#[test]
#[ignore = "live: requires docker compose mssql"]
fn roast_mssql_decimal_scale_survives_all_null_first_batch() {
    require_alive(LiveService::Mssql);

    // 600 leading NULLs > PROBE_BATCH_SIZE (500): the schema-freezing first
    // batch contains no non-null decimal to infer the scale from, regardless
    // of the configured batch_size (the probe is min(batch_size, 500)).
    let table_name = unique_name("roast_dec_freeze");
    mssql_drop_table(&table_name);
    mssql_exec(&format!(
        "CREATE TABLE {table_name} (
            id INT IDENTITY(1,1) PRIMARY KEY,
            amount DECIMAL(10,2) NULL
        );"
    ));
    let _guard = MssqlCleanup(table_name.clone());
    // 600 NULL rows in one INSERT (T-SQL caps a multi-row VALUES at 1000).
    let mut sql = format!("INSERT INTO {table_name} (amount) VALUES ");
    for i in 0..600 {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str("(NULL)");
    }
    mssql_exec(&sql);
    mssql_exec(&format!(
        "INSERT INTO {table_name} (amount) VALUES (123.45);"
    ));

    let export_name = unique_name("roast_dec_freeze_run");
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    // No `columns:` override for `amount` — the bug lives in the autodetect
    // (scale-inference) path; an override would mask it.
    let yaml = format!(
        r#"
source:
  type: mssql
  url: "{MSSQL_URL}"
  tls:
    accept_invalid_certs: true
exports:
  - name: {export_name}
    query: "SELECT id, amount FROM {table_name} ORDER BY id"
    mode: full
    format: parquet
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
        "mssql decimal export must succeed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    let files = files_with_extension(out_dir.path(), "parquet");
    assert_eq!(files.len(), 1, "full export must emit one parquet");
    let (schema, batches) = read_parquet_batches(&files[0]);

    let scale = match schema.field_with_name("amount").unwrap().data_type() {
        DataType::Decimal128(_, s) => *s,
        dt => panic!("amount DECIMAL(10,2) must stay Decimal128, got {dt:?}"),
    };

    // Exactly one non-null amount was seeded; locate it across all batches.
    let mut non_null: Option<(i128, String)> = None;
    let mut total_rows = 0usize;
    for b in &batches {
        let arr = b
            .column_by_name("amount")
            .unwrap()
            .as_primitive::<Decimal128Type>();
        total_rows += arr.len();
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            assert!(
                non_null.is_none(),
                "seeded exactly one non-null amount; found a second"
            );
            non_null = Some((arr.value(i), arr.value_as_string(i)));
        }
    }
    assert_eq!(total_rows, 601, "600 NULL rows + 1 valued row must export");
    let (unscaled, rendered) =
        non_null.expect("the single non-null amount row must survive export");

    // Exact rational equality: unscaled × 10⁻ˢᶜᵃˡᵉ must equal 123.45
    // (= 12345 × 10⁻²), i.e. unscaled·10² == 12345·10^scale. Checked
    // arithmetic so a pathological scale reads as "not equal" rather than a
    // panic-by-overflow.
    let lhs = unscaled.checked_mul(100);
    let rhs = u32::try_from(scale)
        .ok()
        .and_then(|s| 10i128.checked_pow(s))
        .and_then(|f| 12345i128.checked_mul(f));
    assert!(
        lhs.is_some() && lhs == rhs,
        "DECIMAL(10,2) value 123.45 must survive an all-NULL first batch; \
         parquet holds {rendered} (unscaled {unscaled} at Decimal128 scale {scale}) — \
         the schema froze at the placeholder scale 0 inferred from the all-NULL \
         500-row probe batch and rescale_i128 silently truncated the cents"
    );
}

// A `datetimeoffset` column made the whole batch export ERROR — arrow_convert read
// it as a NaiveDateTime, which is the wrong type ("cannot interpret DateTimeOffset
// ... as NaiveDateTime"). Asserts CORRECT behavior: the export succeeds and the
// column keeps its UTC instant, tz-typed. RED before the FixedOffset fallback.
#[test]
#[ignore = "live: requires docker compose mssql"]
fn mssql_batch_datetimeoffset_exports_the_utc_instant() {
    require_alive(LiveService::Mssql);
    let table_name = unique_name("dto_batch");
    mssql_drop_table(&table_name);
    mssql_exec(&format!(
        "CREATE TABLE {table_name} (id INT PRIMARY KEY, dto DATETIMEOFFSET);"
    ));
    let _guard = MssqlCleanup(table_name.clone());
    // 10:00 at +05:30 is 04:30:00 UTC — the instant that must survive.
    mssql_exec(&format!(
        "INSERT INTO {table_name} VALUES (1, '2026-06-23 10:00:00 +05:30');"
    ));

    let export_name = unique_name("dto_batch_run");
    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: mssql
  url: "{MSSQL_URL}"
  tls:
    accept_invalid_certs: true
exports:
  - name: {export_name}
    query: "SELECT id, dto FROM {table_name}"
    mode: full
    format: parquet
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
        "datetimeoffset export must not fail:\nstderr:\n{}",
        String::from_utf8_lossy(&out.stderr),
    );

    let files = files_with_extension(out_dir.path(), "parquet");
    let (schema, batches) = read_parquet_batches(&files[0]);
    let dt = schema.field_with_name("dto").unwrap().data_type();
    assert!(
        matches!(dt, DataType::Timestamp(_, Some(_))),
        "datetimeoffset stays tz-typed, got {dt:?}"
    );
    let arr = batches[0]
        .column_by_name("dto")
        .unwrap()
        .as_primitive::<arrow::array::types::TimestampMicrosecondType>();
    let instant = arr
        .value_as_datetime(0)
        .expect("a non-null instant")
        .to_string();
    assert!(
        instant.starts_with("2026-06-23 04:30:00"),
        "the UTC instant must be 04:30 (10:00 +05:30), got {instant}"
    );
}
