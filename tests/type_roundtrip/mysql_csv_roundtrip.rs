//! MySQL → CSV serialization contract (live).

use crate::common::*;

use super::helpers::{MATRIX_COLUMN_OVERRIDES_YAML, MysqlCleanup, setup_mysql_matrix_table};

#[test]
#[ignore = "live: requires docker compose mysql"]
fn mysql_type_matrix_csv_decimal_text_preserved() {
    require_alive(LiveService::Mysql);

    let table_name = unique_name("type_rt_my_csv");
    setup_mysql_matrix_table(&table_name);
    let _guard = MysqlCleanup(table_name.clone());

    let out_dir = tempfile::tempdir().unwrap();
    let export_name = unique_name("type_rt_my_csv_run");
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: mysql
  url: "{MYSQL_URL}"
exports:
  - name: {export_name}
    query: "SELECT id, amount, fee, price, c_int_u FROM {table_name} ORDER BY id"
    mode: full
    format: csv
{MATRIX_COLUMN_OVERRIDES_YAML}
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
        "mysql csv export: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let files = files_with_extension(out_dir.path(), "csv");
    let content = std::fs::read_to_string(&files[0]).unwrap();
    assert!(content.contains("1234.56"));
    assert!(content.contains("4294967295"));
    assert!(content.contains("999999999999.99"));
    assert!(!content.contains("e+"));
}
