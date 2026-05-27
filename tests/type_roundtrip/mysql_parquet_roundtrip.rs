//! MySQL → Parquet type matrix (live).

use crate::common::*;

use super::helpers::{
    MysqlCleanup, assert_mysql_doc_parquet_schema, assert_mysql_doc_parquet_values,
    read_parquet_batches, run_mysql_matrix_export, setup_mysql_matrix_table,
};

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_type_matrix_parquet_roundtrip() {
    require_alive(LiveService::Mysql);

    let table_name = unique_name("type_rt_my");
    setup_mysql_matrix_table(&table_name);
    let _guard = MysqlCleanup(table_name.clone());

    let out_dir = tempfile::tempdir().unwrap();
    run_mysql_matrix_export(&table_name, "parquet", out_dir.path());

    let files = files_with_extension(out_dir.path(), "parquet");
    assert_eq!(files.len(), 1);
    let (schema, batches) = read_parquet_batches(&files[0]);
    assert_mysql_doc_parquet_schema(&schema);
    assert_mysql_doc_parquet_values(&batches);
}
