//! Live E2E tests for `rivet init` flag variants not covered by `live_init.rs`.
//!
//! | ID | Flag | Test |
//! |---|---|---|
//! | IE1 | `--source-env ENV` | `init_source_env_reads_url_from_env_var` |
//! | IE2 | `--source-file PATH` | `init_source_file_reads_url_from_file` |
//! | IE3 | `--schema public` | `init_schema_flag_filters_to_schema` |
//! | IE4 | `--discover` | `init_discover_flag_emits_json_artifact` |

use crate::common::*;

// ─── IE1: --source-env reads the DB URL from an environment variable ───────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn init_source_env_reads_url_from_env_var() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(5);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "init",
            "--source-env",
            "RIVET_TEST_DB_URL",
            "--table",
            table.name(),
        ])
        .env("RIVET_TEST_DB_URL", POSTGRES_URL)
        .output()
        .expect("spawn rivet init --source-env");

    assert!(
        result.status.success(),
        "init --source-env must exit 0 when env var is set; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let yaml = String::from_utf8_lossy(&result.stdout);
    assert!(
        yaml.contains(table.name()),
        "emitted YAML must contain the seeded table name; got:\n{yaml}"
    );
    // With --source-env the scaffold embeds url_env reference, not plaintext URL.
    assert!(
        yaml.contains("url_env"),
        "scaffold must use url_env when built from --source-env; got:\n{yaml}"
    );
}

// ─── IE2: --source-file reads the DB URL from a file ──────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn init_source_file_reads_url_from_file() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(5);
    let url_file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(url_file.path(), POSTGRES_URL).expect("write url file");

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "init",
            "--source-file",
            url_file.path().to_str().unwrap(),
            "--table",
            table.name(),
        ])
        .output()
        .expect("spawn rivet init --source-file");

    assert!(
        result.status.success(),
        "init --source-file must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let yaml = String::from_utf8_lossy(&result.stdout);
    assert!(
        yaml.contains(table.name()),
        "emitted YAML must contain the seeded table name; got:\n{yaml}"
    );
    assert!(
        yaml.contains("type: postgres"),
        "emitted YAML must have type: postgres; got:\n{yaml}"
    );
}

// ─── IE3: --schema filters schema-wide discovery to the named schema ──────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn init_schema_flag_filters_to_schema() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(5);

    // --schema public is the default but passing it explicitly must still work.
    let result = std::process::Command::new(RIVET_BIN)
        .args(["init", "--source", POSTGRES_URL, "--schema", "public"])
        .output()
        .expect("spawn rivet init --schema public");

    assert!(
        result.status.success(),
        "init --schema public must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let yaml = String::from_utf8_lossy(&result.stdout);
    assert!(
        yaml.contains("exports:"),
        "schema-scoped init must produce exports section; got:\n{yaml}"
    );
    assert!(
        yaml.contains(table.name()),
        "seeded table must appear in the scaffold; got:\n{yaml}"
    );
}

// ─── IE4: --discover emits a machine-readable JSON discovery artifact ──────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn init_discover_flag_emits_json_artifact() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(5);

    let result = std::process::Command::new(RIVET_BIN)
        .args(["init", "--source", POSTGRES_URL, "--discover"])
        .output()
        .expect("spawn rivet init --discover");

    assert!(
        result.status.success(),
        "init --discover must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    let json: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("--discover output must be valid JSON");

    assert_eq!(
        json["source_type"].as_str().unwrap_or(""),
        "postgres",
        "discovery JSON must have source_type = 'postgres'"
    );
    assert!(
        json["tables"].is_array(),
        "discovery JSON must have a 'tables' array"
    );

    // Seeded table must appear in the tables list.
    let tables = json["tables"].as_array().unwrap();
    let found = tables
        .iter()
        .any(|t| t["table"].as_str().unwrap_or("") == table.name());
    assert!(
        found,
        "seeded table '{}' must appear in discovery JSON tables; got: {:?}",
        table.name(),
        tables
            .iter()
            .filter_map(|t| t["table"].as_str())
            .collect::<Vec<_>>()
    );

    // Each table entry must have suggested_mode and cursor_candidates.
    let entry = tables
        .iter()
        .find(|t| t["table"].as_str().unwrap_or("") == table.name())
        .unwrap();
    assert!(
        entry["suggested_mode"].is_string(),
        "each table must have suggested_mode; got:\n{entry}"
    );
    assert!(
        entry["cursor_candidates"].is_array(),
        "each table must have cursor_candidates array; got:\n{entry}"
    );
}

// ─── IE5: --source-env with unset var → non-zero exit ─────────────────────────

#[test]
#[ignore = "live: requires docker compose (any)"]
fn init_source_env_unset_exits_nonzero() {
    require_alive(LiveService::Postgres);

    let result = std::process::Command::new(RIVET_BIN)
        .args(["init", "--source-env", "RIVET_DEFINITELY_NOT_SET_XYZ"])
        .output()
        .expect("spawn rivet init --source-env unset");

    assert!(
        !result.status.success(),
        "init --source-env with unset var must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("RIVET_DEFINITELY_NOT_SET_XYZ")
            || stderr.contains("not set")
            || stderr.contains("Error"),
        "error must mention the missing env var; got:\n{stderr}"
    );
}
