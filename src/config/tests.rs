use super::*;

const MINIMAL_YAML: &str = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: users
    query: "SELECT * FROM users"
    format: csv
    destination:
      type: local
      path: ./out
"#;

#[test]
fn parse_minimal_config() {
    let cfg = Config::from_yaml(MINIMAL_YAML).unwrap();
    assert_eq!(cfg.source.source_type, SourceType::Postgres);
    assert_eq!(cfg.exports.len(), 1);
    assert_eq!(cfg.exports[0].mode, ExportMode::Full);
    assert!(!cfg.parallel_exports);
    assert!(!cfg.parallel_export_processes);
}

#[test]
fn parallel_exports_parses() {
    let yaml = r#"
parallel_exports: true
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: a
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
  - name: b
    query: "SELECT 2"
    format: csv
    destination:
      type: local
      path: ./out
"#;
    let cfg = Config::from_yaml(yaml).unwrap();
    assert!(cfg.parallel_exports);
}

#[test]
fn parallel_export_processes_parses() {
    let yaml = r#"
parallel_export_processes: true
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: a
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
  - name: b
    query: "SELECT 2"
    format: csv
    destination:
      type: local
      path: ./out
"#;
    let cfg = Config::from_yaml(yaml).unwrap();
    assert!(cfg.parallel_export_processes);
}

#[test]
fn parse_config_with_tuning() {
    let yaml = r#"
source:
  type: mysql
  url: "mysql://localhost/test"
  tuning:
    profile: safe
    batch_size: 3000
exports:
  - name: orders
    query: "SELECT * FROM orders"
    format: parquet
    destination:
      type: s3
      bucket: my-bucket
"#;
    let cfg = Config::from_yaml(yaml).unwrap();
    let tuning = cfg.source.tuning.as_ref().unwrap();
    assert_eq!(tuning.profile, Some(crate::tuning::TuningProfile::Safe));
    assert_eq!(tuning.batch_size, Some(3000));
}

#[test]
fn parse_export_level_tuning_merges_with_source() {
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
  tuning:
    profile: fast
    batch_size: 5000
exports:
  - name: a
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
    tuning:
      profile: balanced
"#;
    let cfg = Config::from_yaml(yaml).unwrap();
    let merged = crate::tuning::merge_tuning_config(
        cfg.source.tuning.as_ref(),
        cfg.exports[0].tuning.as_ref(),
    )
    .expect("merged");
    assert_eq!(merged.profile, Some(crate::tuning::TuningProfile::Balanced));
    assert_eq!(merged.batch_size, Some(5000));
}

#[test]
fn effective_tuning_rejects_cross_layer_batch_conflict() {
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
  tuning:
    batch_size: 1000
exports:
  - name: a
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
    tuning:
      batch_size_memory_mb: 128
"#;
    let err = Config::from_yaml(yaml).unwrap_err();
    assert!(
        err.to_string().contains("effective tuning"),
        "expected effective tuning error, got: {err}"
    );
}

#[test]
fn incremental_without_cursor_column_is_rejected() {
    let yaml = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: bad
    query: "SELECT * FROM t"
    mode: incremental
    format: csv
    destination:
      type: local
      path: ./out
"#;
    let err = Config::from_yaml(yaml).unwrap_err();
    assert!(err.to_string().contains("cursor_column"));
}

#[test]
fn incremental_with_cursor_column_is_accepted() {
    Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: good
    query: "SELECT * FROM t"
    mode: incremental
    cursor_column: updated_at
    format: csv
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
}

#[test]
fn default_export_mode_is_full() {
    let cfg = Config::from_yaml(MINIMAL_YAML).unwrap();
    assert_eq!(cfg.exports[0].mode, ExportMode::Full);
}

#[test]
fn chunked_mode_requires_chunk_column() {
    let err = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: bad
    query: "SELECT * FROM t"
    mode: chunked
    format: csv
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("chunk_column"));
}

#[test]
fn chunked_mode_parses() {
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
    assert_eq!(cfg.exports[0].mode, ExportMode::Chunked);
    assert_eq!(cfg.exports[0].chunk_column.as_deref(), Some("id"));
    assert_eq!(cfg.exports[0].chunk_size, 50000);
    assert_eq!(cfg.exports[0].parallel, 4);
}

#[test]
fn chunk_dense_parses() {
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: orders
    query: "SELECT id, payload FROM orders_sparse"
    mode: chunked
    chunk_column: id
    chunk_dense: true
    chunk_size: 10000
    format: csv
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
    assert!(cfg.exports[0].chunk_dense);
}

#[test]
fn chunk_dense_rejected_without_chunked_mode() {
    let err = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: bad
    query: "SELECT * FROM t"
    mode: full
    chunk_dense: true
    format: csv
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("chunk_dense"));
}

#[test]
fn time_window_requires_time_column_and_days() {
    let err = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: bad
    query: "SELECT * FROM t"
    mode: time_window
    format: csv
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("time_column"));
}

#[test]
fn time_window_parses() {
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: events
    query: "SELECT * FROM events"
    mode: time_window
    time_column: created_at
    time_column_type: timestamp
    days_window: 7
    format: csv
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
    assert_eq!(cfg.exports[0].mode, ExportMode::TimeWindow);
    assert_eq!(cfg.exports[0].time_column.as_deref(), Some("created_at"));
    assert_eq!(cfg.exports[0].days_window, Some(7));
}

#[test]
fn query_and_query_file_both_set_rejected() {
    let err = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: bad
    query: "SELECT 1"
    query_file: sql/test.sql
    format: csv
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("query"));
}

#[test]
fn neither_query_nor_query_file_rejected() {
    let err = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: bad
    format: csv
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("query"));
}

#[test]
fn url_env_parses() {
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  url_env: DATABASE_URL
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
    assert!(cfg.source.url.is_none());
    assert_eq!(cfg.source.url_env.as_deref(), Some("DATABASE_URL"));
}

#[test]
fn url_and_url_env_both_set_rejected() {
    let err = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
  url_env: DATABASE_URL
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("exactly one"));
}

#[test]
fn no_url_at_all_rejected() {
    let err = Config::from_yaml(
        r#"
source:
  type: postgres
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("must specify"));
}

#[test]
fn resolve_url_from_env() {
    unsafe {
        std::env::set_var("RIVET_TEST_DB_URL", "postgresql://test:test@localhost/db");
    }
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  url_env: RIVET_TEST_DB_URL
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
    let url = cfg.source.resolve_url().unwrap();
    assert_eq!(url, "postgresql://test:test@localhost/db");
    unsafe {
        std::env::remove_var("RIVET_TEST_DB_URL");
    }
}

#[test]
fn env_var_substitution_in_url() {
    unsafe {
        std::env::set_var("RIVET_TEST_PASS", "s3cret");
    }
    let resolved = resolve_env_vars("postgresql://user:${RIVET_TEST_PASS}@localhost/db");
    assert_eq!(resolved, "postgresql://user:s3cret@localhost/db");
    unsafe {
        std::env::remove_var("RIVET_TEST_PASS");
    }
}

#[test]
fn param_substitution_overrides_env() {
    unsafe {
        std::env::set_var("RIVET_TEST_PARAM", "from_env");
    }
    let mut params = std::collections::HashMap::new();
    params.insert("RIVET_TEST_PARAM".into(), "from_param".into());
    let resolved = resolve_vars("val=${RIVET_TEST_PARAM}", Some(&params));
    assert_eq!(resolved, "val=from_param");
    unsafe {
        std::env::remove_var("RIVET_TEST_PARAM");
    }
}

#[test]
fn param_substitution_falls_back_to_env() {
    unsafe {
        std::env::set_var("RIVET_TEST_FALLBACK", "env_val");
    }
    let params = std::collections::HashMap::new();
    let resolved = resolve_vars("v=${RIVET_TEST_FALLBACK}", Some(&params));
    assert_eq!(resolved, "v=env_val");
    unsafe {
        std::env::remove_var("RIVET_TEST_FALLBACK");
    }
}

#[test]
fn env_var_substitution_missing_var() {
    unsafe {
        std::env::remove_var("RIVET_NONEXISTENT_VAR");
    }
    let resolved = resolve_env_vars("prefix_${RIVET_NONEXISTENT_VAR}_suffix");
    assert_eq!(resolved, "prefix__suffix");
}

#[test]
fn credentials_file_parses() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let yaml = format!(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    destination:
      type: gcs
      bucket: my-bucket
      credentials_file: "{}"
"#,
        tmp.path().display()
    );
    let cfg = Config::from_yaml(&yaml).unwrap();
    assert!(cfg.exports[0].destination.credentials_file.is_some());
}

#[test]
fn s3_access_key_env_parses() {
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    destination:
      type: s3
      bucket: my-bucket
      region: us-east-1
      access_key_env: MY_AWS_KEY
      secret_key_env: MY_AWS_SECRET
"#,
    )
    .unwrap();
    assert_eq!(
        cfg.exports[0].destination.access_key_env.as_deref(),
        Some("MY_AWS_KEY")
    );
    assert_eq!(
        cfg.exports[0].destination.secret_key_env.as_deref(),
        Some("MY_AWS_SECRET")
    );
}

#[test]
fn s3_only_one_of_access_key_env_rejected() {
    let err = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    destination:
      type: s3
      bucket: my-bucket
      region: us-east-1
      access_key_env: MY_AWS_KEY
"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("access_key_env"));
    assert!(err.to_string().contains("secret_key_env"));
}

#[test]
fn gcs_allow_anonymous_parses() {
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    destination:
      type: gcs
      bucket: b
      endpoint: http://127.0.0.1:4443
      allow_anonymous: true
"#,
    )
    .unwrap();
    assert!(cfg.exports[0].destination.allow_anonymous);
}

#[test]
fn gcs_allow_anonymous_with_credentials_file_rejected() {
    let err = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    destination:
      type: gcs
      bucket: b
      allow_anonymous: true
      credentials_file: /x/sa.json
"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("allow_anonymous"));
}

#[test]
fn structured_pg_credentials_build_url() {
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  host: db.example.com
  port: 5433
  user: admin
  password: s3cret
  database: mydb
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
    let url = cfg.source.resolve_url().unwrap();
    assert_eq!(url, "postgresql://admin:s3cret@db.example.com:5433/mydb");
}

#[test]
fn structured_mysql_credentials_default_port() {
    let cfg = Config::from_yaml(
        r#"
source:
  type: mysql
  host: localhost
  user: root
  database: app
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
    let url = cfg.source.resolve_url().unwrap();
    assert_eq!(url, "mysql://root@localhost:3306/app");
}

#[test]
fn structured_with_password_env() {
    unsafe {
        std::env::set_var("RIVET_TEST_DBPASS", "topSecret");
    }
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  host: localhost
  user: rivet
  password_env: RIVET_TEST_DBPASS
  database: rivet
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
    let url = cfg.source.resolve_url().unwrap();
    assert_eq!(url, "postgresql://rivet:topSecret@localhost:5432/rivet");
    unsafe {
        std::env::remove_var("RIVET_TEST_DBPASS");
    }
}

#[test]
fn structured_missing_host_rejected() {
    let err = Config::from_yaml(
        r#"
source:
  type: postgres
  user: rivet
  database: rivet
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("host"));
}

#[test]
fn structured_missing_user_rejected() {
    let err = Config::from_yaml(
        r#"
source:
  type: postgres
  host: localhost
  database: rivet
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("user"));
}

#[test]
fn structured_missing_database_rejected() {
    let err = Config::from_yaml(
        r#"
source:
  type: postgres
  host: localhost
  user: rivet
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("database"));
}

#[test]
fn structured_and_url_both_rejected() {
    let err = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
  host: localhost
  user: rivet
  database: rivet
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("not both"));
}

#[test]
fn gcs_credentials_file_missing_rejected() {
    let err = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    destination:
      type: gcs
      bucket: b
      credentials_file: /nonexistent/path/sa.json
"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("does not exist"), "got: {}", err);
}

#[test]
fn structured_password_and_password_env_both_rejected() {
    let err = Config::from_yaml(
        r#"
source:
  type: postgres
  host: localhost
  user: rivet
  password: abc
  password_env: MY_PASS
  database: rivet
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("password"));
}

#[test]
fn compression_defaults_to_zstd() {
    let cfg = Config::from_yaml(MINIMAL_YAML).unwrap();
    assert_eq!(cfg.exports[0].compression, CompressionType::Zstd);
    assert!(cfg.exports[0].compression_level.is_none());
}

#[test]
fn compression_snappy_parses() {
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    compression: snappy
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
    assert_eq!(cfg.exports[0].compression, CompressionType::Snappy);
}

#[test]
fn compression_zstd_with_level() {
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    compression: zstd
    compression_level: 9
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
    assert_eq!(cfg.exports[0].compression, CompressionType::Zstd);
    assert_eq!(cfg.exports[0].compression_level, Some(9));
}

#[test]
fn compression_gzip_with_level() {
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    compression: gzip
    compression_level: 6
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
    assert_eq!(cfg.exports[0].compression, CompressionType::Gzip);
    assert_eq!(cfg.exports[0].compression_level, Some(6));
}

#[test]
fn compression_none_parses() {
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    compression: none
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
    assert_eq!(cfg.exports[0].compression, CompressionType::None);
}

#[test]
fn compression_level_rejected_for_snappy() {
    let err = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    compression: snappy
    compression_level: 5
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("compression_level"),
        "got: {}",
        err
    );
}

#[test]
fn zstd_compression_level_out_of_range_rejected() {
    let err = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    compression: zstd
    compression_level: 30
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("1..22"), "got: {}", err);
}

#[test]
fn gzip_compression_level_out_of_range_rejected() {
    let err = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    compression: gzip
    compression_level: 15
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("0..10"), "got: {}", err);
}

#[test]
fn skip_empty_defaults_to_false() {
    let cfg = Config::from_yaml(MINIMAL_YAML).unwrap();
    assert!(!cfg.exports[0].skip_empty);
}

#[test]
fn skip_empty_true_parses() {
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    skip_empty: true
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
    assert!(cfg.exports[0].skip_empty);
}

#[test]
fn meta_columns_defaults_to_disabled() {
    let cfg = Config::from_yaml(MINIMAL_YAML).unwrap();
    assert!(!cfg.exports[0].meta_columns.exported_at);
    assert!(!cfg.exports[0].meta_columns.row_hash);
}

#[test]
fn meta_columns_both_enabled() {
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    meta_columns:
      exported_at: true
      row_hash: true
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
    assert!(cfg.exports[0].meta_columns.exported_at);
    assert!(cfg.exports[0].meta_columns.row_hash);
}

#[test]
fn stdout_destination_parses() {
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: stdout
"#,
    )
    .unwrap();
    assert_eq!(
        cfg.exports[0].destination.destination_type,
        DestinationType::Stdout
    );
}

#[test]
fn meta_columns_partial() {
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    meta_columns:
      row_hash: true
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
    assert!(!cfg.exports[0].meta_columns.exported_at);
    assert!(cfg.exports[0].meta_columns.row_hash);
}

#[test]
fn quality_config_parses() {
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    quality:
      row_count_min: 100
      row_count_max: 1000000
      null_ratio_max:
        email: 0.05
      unique_columns: [id]
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
    let q = cfg.exports[0].quality.as_ref().unwrap();
    assert_eq!(q.row_count_min, Some(100));
    assert_eq!(q.row_count_max, Some(1_000_000));
    assert_eq!(q.null_ratio_max.get("email"), Some(&0.05));
    assert_eq!(q.unique_columns, vec!["id".to_string()]);
}

#[test]
fn max_file_size_parses() {
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    max_file_size: 512MB
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
    assert_eq!(
        cfg.exports[0].max_file_size_bytes(),
        Some(512 * 1024 * 1024)
    );
}

#[test]
fn parse_file_size_variants() {
    assert_eq!(parse_file_size("1GB").unwrap(), 1024 * 1024 * 1024);
    assert_eq!(parse_file_size("512MB").unwrap(), 512 * 1024 * 1024);
    assert_eq!(parse_file_size("100KB").unwrap(), 100 * 1024);
    assert_eq!(parse_file_size("1024B").unwrap(), 1024);
    assert_eq!(parse_file_size("1024").unwrap(), 1024);
}

#[test]
fn notification_config_parses() {
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
notifications:
  slack:
    webhook_url_env: SLACK_WEBHOOK
    on: [failure, schema_change]
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
    let n = cfg.notifications.as_ref().unwrap();
    let s = n.slack.as_ref().unwrap();
    assert_eq!(s.webhook_url_env.as_deref(), Some("SLACK_WEBHOOK"));
    assert_eq!(s.on.len(), 2);
}

#[test]
fn batch_size_memory_mb_parses() {
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
  tuning:
    batch_size_memory_mb: 256
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
    let t = cfg.source.tuning.as_ref().unwrap();
    assert_eq!(t.batch_size_memory_mb, Some(256));
}

#[test]
fn batch_size_and_memory_mb_mutually_exclusive() {
    let err = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
  tuning:
    batch_size: 5000
    batch_size_memory_mb: 256
exports:
  - name: t
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap_err();
    assert!(err.to_string().contains("mutually exclusive"), "got: {err}");
}

#[test]
fn chunk_checkpoint_fields_parse() {
    let cfg = Config::from_yaml(
        r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    mode: chunked
    chunk_column: id
    chunk_checkpoint: true
    chunk_max_attempts: 7
    format: csv
    destination:
      type: local
      path: ./out
"#,
    )
    .unwrap();
    assert!(cfg.exports[0].chunk_checkpoint);
    assert_eq!(cfg.exports[0].chunk_max_attempts, Some(7));
}
