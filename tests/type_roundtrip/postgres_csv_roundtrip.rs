//! PostgreSQL → CSV serialization contract (live).

use crate::common::*;

use super::helpers::{MATRIX_COLUMN_OVERRIDES_YAML, PgCleanup, setup_pg_matrix_table};

#[test]
#[ignore = "live: requires docker compose postgres"]
fn postgres_type_matrix_csv_decimal_and_row_count() {
    require_alive(LiveService::Postgres);

    let table_name = unique_name("type_rt_pg_csv");
    let enum_type = setup_pg_matrix_table(&table_name);
    let _guard = PgCleanup {
        table: table_name.clone(),
        enum_type,
    };

    let out_dir = tempfile::tempdir().unwrap();
    let export_name = unique_name("type_rt_pg_csv_run");
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {export_name}
    query: "SELECT id, amount, fee, price, label FROM {table_name} ORDER BY id"
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
        "postgres csv export: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let files = files_with_extension(out_dir.path(), "csv");
    let content = std::fs::read_to_string(&files[0]).unwrap();
    let lines: Vec<_> = content.lines().collect();
    assert_eq!(lines.len(), 5, "header + 4 rows");
    assert!(
        content.contains("0.10") || content.contains(",0.1,"),
        "amount decimal: {content}"
    );
    assert!(content.contains("1234.56"), "price decimal(10,2)");
    assert!(content.contains("999999999999.99"));
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn postgres_type_matrix_csv_unicode_and_newlines_escaped() {
    require_alive(LiveService::Postgres);

    let table_name = unique_name("type_rt_pg_csv_esc");
    let enum_type = setup_pg_matrix_table(&table_name);
    let _guard = PgCleanup {
        table: table_name.clone(),
        enum_type,
    };

    let out_dir = tempfile::tempdir().unwrap();
    let export_name = unique_name("type_rt_pg_csv_esc_run");
    let cfg_dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "{POSTGRES_URL}"
exports:
  - name: {export_name}
    query: "SELECT id, note_nullable, large_text FROM {table_name} WHERE id IN (1, 4) ORDER BY id"
    mode: full
    format: csv
    destination:
      type: local
      path: {out_dir}
"#,
        out_dir = out_dir.path().display()
    );
    let cfg_path = write_config(&cfg_dir, &yaml);
    let out = run_rivet_export(&cfg_path, &export_name);
    assert!(out.status.success());

    let files = files_with_extension(out_dir.path(), "csv");
    let content = std::fs::read_to_string(&files[0]).unwrap();
    let mut reader = csv::Reader::from_reader(content.as_bytes());
    let rows: Vec<csv::StringRecord> = reader.records().map(|r| r.unwrap()).collect();
    assert_eq!(rows.len(), 2);
    assert!(rows[0].get(1).unwrap().contains('\n'));
    assert!(rows[1].get(1).unwrap().contains("日本語"));
    assert!(rows[0].get(2).unwrap().len() >= 5000);
}
