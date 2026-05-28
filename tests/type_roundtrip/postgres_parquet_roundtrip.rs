//! PostgreSQL → Parquet type matrix (live).

use crate::common::*;

use super::helpers::{
    PgCleanup, assert_pg_doc_parquet_schema, assert_pg_doc_parquet_values, read_parquet_batches,
    run_pg_matrix_export, setup_pg_matrix_table,
};

#[test]
#[ignore = "live: requires docker compose postgres"]
fn postgres_type_matrix_parquet_roundtrip() {
    require_alive(LiveService::Postgres);

    let table_name = unique_name("type_rt_pg");
    let enum_type = setup_pg_matrix_table(&table_name);
    let _guard = PgCleanup {
        table: table_name.clone(),
        enum_type,
    };

    let out_dir = tempfile::tempdir().unwrap();
    run_pg_matrix_export(&table_name, "parquet", out_dir.path());

    let files = files_with_extension(out_dir.path(), "parquet");
    assert_eq!(files.len(), 1, "single parquet part");
    let (schema, batches) = read_parquet_batches(&files[0]);
    assert_pg_doc_parquet_schema(&schema);
    assert_pg_doc_parquet_values(&batches);
}
