//! Every config refusal carries its registered `RIVET_CONFIG_*` code, read back through the public path.

use super::*;
use crate::error::{classify_exit, error_code};

/// A config with one parquet export under `export_extra`, a postgres URL source plus `source_extra`.
fn yaml(source_extra: &str, export_extra: &str) -> String {
    format!(
        "source:\n  type: postgres\n  url: \"postgresql://localhost/test\"\n{source_extra}\
         exports:\n  - name: users\n    format: parquet\n    destination:\n      type: local\n      path: ./out\n{export_extra}"
    )
}

/// The code and exit class `Config::from_yaml` refuses `text` with.
fn refused(text: &str) -> (Option<&'static str>, i32, String) {
    let err = Config::from_yaml(text).expect_err("the config must be refused");
    (error_code(&err), classify_exit(&err), format!("{err:#}"))
}

fn assert_refused(text: &str, code: crate::error::Code, needle: &str) {
    let (got, exit, msg) = refused(text);
    assert_eq!(got, Some(code.id), "{msg}");
    assert_eq!(exit, 1, "a config refusal exits 1: {msg}");
    assert!(
        msg.contains(needle),
        "message kept its text `{needle}`: {msg}"
    );
}

const QUERY: &str = "    query: \"SELECT * FROM users\"\n";

#[test]
fn a_missing_config_file_is_coded_file_unreadable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("absent.yaml");
    let err = Config::load(path.to_str().unwrap()).unwrap_err();
    assert_eq!(
        error_code(&err),
        Some(crate::error::codes::CONFIG_FILE_UNREADABLE.id)
    );
    assert_eq!(classify_exit(&err), 1, "a missing file is not retried");
    assert!(format!("{err:#}").contains("not found"));
}

#[test]
fn a_typo_a_misplaced_key_and_broken_yaml_are_coded_yaml_invalid() {
    let code = crate::error::codes::CONFIG_YAML_INVALID;
    assert_refused(
        &yaml("", "    qeury: \"SELECT 1\"\n"),
        code,
        "unknown field `qeury`",
    );
    assert_refused(
        &yaml("  batch_size: 10\n", QUERY),
        code,
        "belong under 'source.tuning:'",
    );
    assert_refused("source: [\nexports: x\n", code, "");
}

#[test]
fn a_half_structured_source_is_coded_source_connection_invalid() {
    let code = crate::error::codes::CONFIG_SOURCE_CONNECTION_INVALID;
    let text = format!(
        "source:\n  type: postgres\n  user: u\n  database: d\n\
         exports:\n  - name: users\n    format: parquet\n{QUERY}    destination:\n      type: local\n      path: ./out\n"
    );
    assert_refused(&text, code, "structured config is missing 'host'");
    assert_refused(
        &yaml("  url_env: SOME_URL\n", QUERY),
        code,
        "specify exactly one of 'url', 'url_env', or 'url_file'",
    );
}

#[test]
fn an_unset_url_env_and_an_unresolved_placeholder_are_coded_secret_unset() {
    let code = crate::error::codes::CONFIG_SECRET_UNSET;
    let text = format!(
        "source:\n  type: postgres\n  url_env: RIVET_TEST_CODES_SURELY_UNSET_URL\n\
         exports:\n  - name: users\n    format: parquet\n{QUERY}    destination:\n      type: local\n      path: ./out\n"
    );
    let cfg = Config::from_yaml(&text).expect("an unset url_env is read at connect time");
    let err = cfg.source.resolve_url().unwrap_err();
    assert_eq!(error_code(&err), Some(code.id));
    assert_eq!(classify_exit(&err), 1, "an unset variable is not retried");

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rivet.yaml");
    std::fs::write(
        &path,
        yaml(
            "",
            "    query: \"SELECT ${RIVET_TEST_CODES_SURELY_UNSET_P}\"\n",
        ),
    )
    .unwrap();
    let err = Config::load(path.to_str().unwrap()).unwrap_err();
    assert_eq!(error_code(&err), Some(code.id));
    assert!(format!("{err:#}").contains("is unresolved"));
}

#[test]
fn an_export_without_one_source_selector_is_coded_export_source_invalid() {
    let code = crate::error::codes::CONFIG_EXPORT_SOURCE_INVALID;
    assert_refused(
        &yaml("", ""),
        code,
        "specify exactly one of 'query', 'query_file', 'table', or 'tables'",
    );
    assert_refused(
        &yaml("", "    query_file: /etc/q.sql\n"),
        code,
        "query_file must be a relative path",
    );
    let cfg = Config::from_yaml(&yaml("", "    table: \"a.b.c\"\n")).unwrap();
    let err = cfg.exports[0]
        .resolve_query(std::path::Path::new("."), None)
        .unwrap_err();
    assert_eq!(error_code(&err), Some(code.id));
    assert!(format!("{err:#}").contains("'table' must be `<name>` or `<schema>.<name>`"));
}

#[test]
fn a_mode_without_its_setting_is_coded_mode_setting_invalid() {
    let code = crate::error::codes::CONFIG_MODE_SETTING_INVALID;
    assert_refused(
        &yaml("", &format!("{QUERY}    mode: incremental\n")),
        code,
        "incremental mode requires cursor_column",
    );
    assert_refused(
        &yaml("", &format!("{QUERY}    chunk_column: id\n")),
        code,
        "`chunk_column` requires `mode: chunked`",
    );
}

/// The same config with the local destination swapped for `dest`.
fn with_destination(dest: &str) -> String {
    yaml("", QUERY).replace("      type: local\n      path: ./out\n", dest)
}

#[test]
fn a_half_configured_cloud_credential_is_coded_destination_auth_invalid() {
    assert_refused(
        &with_destination("      type: s3\n      bucket: b\n      access_key_env: AK\n"),
        crate::error::codes::CONFIG_DESTINATION_AUTH_INVALID,
        "S3 requires both access_key_env and secret_key_env",
    );
}

#[test]
fn a_remote_custom_endpoint_is_coded_destination_endpoint_unsafe() {
    assert_refused(
        &with_destination(
            "      type: s3\n      bucket: b\n      endpoint: https://collector.example.com\n",
        ),
        crate::error::codes::CONFIG_DESTINATION_ENDPOINT_UNSAFE,
        "points at a non-loopback host",
    );
}

#[test]
fn a_missing_credentials_file_is_coded_secret_unset() {
    assert_refused(
        &with_destination(
            "      type: gcs\n      bucket: b\n      credentials_file: /nonexistent/rivet-codes-key.json\n",
        ),
        crate::error::codes::CONFIG_SECRET_UNSET,
        "does not exist",
    );
}

#[test]
fn an_out_of_range_value_is_coded_value_out_of_range() {
    let code = crate::error::codes::CONFIG_VALUE_OUT_OF_RANGE;
    assert_refused(
        &yaml("", &format!("{QUERY}    compression_level: 30\n")),
        code,
        "zstd compression_level must be 1..22",
    );
    assert_refused(
        &yaml("", &format!("{QUERY}    max_file_size: lots\n")),
        code,
        "invalid max_file_size 'lots'",
    );
    assert_refused(
        &yaml(
            "",
            &format!(
                "{QUERY}    mode: incremental\n    cursor_column: ts\n    settle:\n      after: 0s\n"
            ),
        ),
        code,
        "is zero",
    );
}

#[test]
fn a_malformed_tables_list_is_coded_cdc_shape_invalid() {
    let code = crate::error::codes::CONFIG_CDC_SHAPE_INVALID;
    let cdc = |tables: &str| {
        yaml(
            "",
            &format!("    mode: cdc\n    tables: {tables}\n    cdc:\n      slot: s\n"),
        )
    };
    assert_refused(&cdc("[]"), code, "`tables:` must list at least one table");
    assert_refused(&cdc("[a, a]"), code, "duplicate table 'a'");
}

#[test]
fn csv_with_a_codec_is_coded_csv_compression_unsupported() {
    let text = yaml("", &format!("{QUERY}    compression: gzip\n"))
        .replace("format: parquet", "format: csv");
    assert_refused(
        &text,
        crate::error::codes::CONFIG_CSV_COMPRESSION_UNSUPPORTED,
        "CSV output does not support compression: gzip",
    );
}

#[test]
fn verify_full_with_a_danger_knob_is_coded_tls_verification_downgrade() {
    assert_refused(
        &yaml(
            "  tls:\n    mode: verify-full\n    accept_invalid_certs: true\n",
            QUERY,
        ),
        crate::error::codes::CONFIG_TLS_VERIFICATION_DOWNGRADE,
        "disables certificate verification",
    );
}

#[test]
fn an_export_load_override_without_a_load_block_is_coded_load_block_invalid() {
    assert_refused(
        &yaml(
            "",
            &format!("{QUERY}    load:\n      cleanup_source: true\n"),
        ),
        crate::error::codes::CONFIG_LOAD_BLOCK_INVALID,
        "has a `load:` override but the config has no top-level `load:`",
    );
}

#[test]
fn a_traversing_name_or_prefix_is_coded_path_unsafe() {
    let code = crate::error::codes::CONFIG_PATH_UNSAFE;
    assert_refused(
        &yaml("", QUERY).replace("name: users", "name: \"../escape\""),
        code,
        "is not filename-safe",
    );
    assert_refused(
        &with_destination("      type: local\n      path: ./out/../../x\n"),
        code,
        "contains a `..` segment",
    );
}

#[test]
fn an_empty_row_hash_list_is_coded_row_hash_invalid() {
    assert_refused(
        &yaml(
            "",
            &format!("{QUERY}    meta_columns:\n      row_hash: []\n"),
        ),
        crate::error::codes::CONFIG_ROW_HASH_INVALID,
        "meta_columns.row_hash is an empty list",
    );
}

#[test]
fn a_qualified_override_naming_an_uncaptured_table_is_coded_column_override_table_unknown() {
    let code = crate::error::codes::CONFIG_COLUMN_OVERRIDE_TABLE_UNKNOWN;
    assert_refused(
        &yaml(
            "",
            &format!("{QUERY}    columns: {{ \"users.v\": \"int8\" }}\n"),
        ),
        code,
        "needs a table-shaped export",
    );
    assert_refused(
        &yaml(
            "",
            "    table: users\n    columns: { \"ghost.v\": \"int8\" }\n",
        ),
        code,
        "names table 'ghost'",
    );
}
