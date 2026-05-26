//! Full-config parse goldens (migrated from former tests/v2_golden.rs).

use super::*;

#[test]
fn chunked_config_full_parse_goldens() {
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: orders
    query: "SELECT * FROM orders"
    mode: chunked
    chunk_column: id
    chunk_size: 50000
    parallel: 4
    format: parquet
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
    let e = &cfg.exports[0];
    assert_eq!(e.mode, ExportMode::Chunked);
    assert_eq!(e.chunk_column.as_deref(), Some("id"));
    assert_eq!(e.chunk_size, 50000);
    assert_eq!(e.parallel, 4);
}

#[test]
fn time_window_config_full_parse_goldens() {
    let cfg = Config::from_yaml(
        r#"
source:
  type: mysql
  url: "mysql://localhost/test"
exports:
  - name: events
    query: "SELECT * FROM events"
    mode: time_window
    time_column: created_at
    time_column_type: unix
    days_window: 30
    format: csv
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
    let e = &cfg.exports[0];
    assert_eq!(e.mode, ExportMode::TimeWindow);
    assert_eq!(e.time_column.as_deref(), Some("created_at"));
    assert_eq!(e.time_column_type, TimeColumnType::Unix);
    assert_eq!(e.days_window, Some(30));
}

#[test]
fn query_file_config_parses_goldens() {
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: custom
    query_file: sql/custom.sql
    format: parquet
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
    assert!(cfg.exports[0].query.is_none());
    assert_eq!(cfg.exports[0].query_file.as_deref(), Some("sql/custom.sql"));
}
