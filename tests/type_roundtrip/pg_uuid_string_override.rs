//! Regression: a PostgreSQL `uuid` column with a `columns: { uid: string }`
//! override must materialize as canonical hyphenated Utf8 text, not stay
//! `FixedSizeBinary(16)` (which would disagree with the resolved schema and
//! make `rows_to_record_batch_typed` bail).
//!
//! Pins the target-aware UUID arm in [src/source/postgres/arrow_convert.rs]:
//! the value dispatch follows the resolved `target_type` (like the NUMERIC
//! arm), so retyping `uuid -> string` renders text instead of bytes. Before
//! the fix the override produced a FixedSizeBinary array against a Utf8 field
//! and failed at batch construction.

use crate::common::*;

use super::helpers::{PgCleanup, read_parquet_batches, setup_pg_matrix_table};

use arrow::array::{Array, StringArray};
use arrow::datatypes::DataType;

#[test]
#[ignore = "live: requires docker compose postgres"]
fn pg_uuid_with_string_override_renders_canonical_text() {
    require_alive(LiveService::Postgres);

    let table_name = unique_name("type_rt_pg_uuidstr");
    let enum_type = setup_pg_matrix_table(&table_name);
    let _guard = PgCleanup {
        table: table_name.clone(),
        enum_type,
    };

    let out_dir = tempfile::tempdir().unwrap();
    let export_name = unique_name("type_rt_pg_uuidstr");
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {export_name}
    query: >-
      SELECT id, uid FROM {table_name} ORDER BY id
    mode: full
    format: parquet
    columns:
      uid: string
    destination:
      type: local
      path: {path}
"#,
        path = out_dir.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);

    let out = run_rivet_export(&cfg_path, &export_name);
    assert!(
        out.status.success(),
        "rivet failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let files = files_with_extension(out_dir.path(), "parquet");
    assert_eq!(files.len(), 1);
    let (schema, batches) = read_parquet_batches(&files[0]);
    assert_eq!(
        *schema.field_with_name("uid").unwrap().data_type(),
        DataType::Utf8,
        "uid: string override must materialize as Utf8, not FixedSizeBinary(16)"
    );

    // Canonical seed values for `uid` (from
    // tests/type_roundtrip/fixtures/postgres_seed.sql), in id order.
    let expected = [
        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380011",
        "b0eebc99-9c0b-4ef8-bb6d-6bb9bd380022",
        "c0eebc99-9c0b-4ef8-bb6d-6bb9bd380033",
        "d0eebc99-9c0b-4ef8-bb6d-6bb9bd380044",
    ];

    let mut got: Vec<String> = Vec::new();
    for batch in &batches {
        let col = batch.column_by_name("uid").expect("uid column");
        let arr = col
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("StringArray (uid rendered as text)");
        for i in 0..arr.len() {
            assert!(!arr.is_null(i), "row {i}: uid must not be NULL");
            got.push(arr.value(i).to_string());
        }
    }
    assert_eq!(got, expected.to_vec());
}
