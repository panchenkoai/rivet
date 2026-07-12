//! Live Parquet validation via Snowflake (ADR-0014 §4 + §6 — Snowflake resolver row).
//!
//! The CI guardian for `ExportTarget::Snowflake` in `src/types/target.rs`. The
//! unit tests there assert the *shape* of the autoload/cast claims; this harness
//! asserts they are *true* against a real Snowflake — the same role
//! [`bigquery_load`] plays for BigQuery. The Parquet is the contract; Snowflake
//! is the oracle.
//!
//! It reproduces the documented load flow (memory: snowflake-autoload-quirks):
//!   FILE FORMAT … BINARY_AS_TEXT=FALSE → PUT → INFER_SCHEMA template → COPY
//! then checks two things the resolver promises:
//!
//! * **autoload degradations** — `INFER_SCHEMA` lands JSON→TEXT, UUID→BINARY,
//!   naive timestamp→NUMBER (µs), TIME→NUMBER (µs of day), bytea→BINARY,
//!   decimal→NUMBER, tz timestamp→TIMESTAMP_NTZ, list→ARRAY. These are exactly
//!   the `autoload_type` values `snowflake::native` emits.
//! * **recovery casts** — the `cast_sql` expressions (`PARSE_JSON`,
//!   `HEX_ENCODE`+`REGEXP`, `TO_TIMESTAMP_NTZ`, `TIME_FROM_PARTS`) recover the
//!   original values from the degraded staging table.
//!
//! Gating: requires `SNOWFLAKE_TEST_CONNECTION` env + the `snow` CLI on PATH.
//! Tests skip cleanly otherwise so CI without Snowflake credentials stays green.
//!
//! Configurable via env:
//!   SNOWFLAKE_TEST_CONNECTION   — required, the `snow` connection name (e.g. `rivet`)
//!   SNOWFLAKE_TEST_PRIVATE_KEY  — optional, absolute path to the key-pair .p8
//!                                 (the connection's `private_key_file` may carry a
//!                                 literal `~` that `snow` will not expand)
//!   SNOWFLAKE_TEST_DATABASE     — optional, overrides the connection default
//!   SNOWFLAKE_TEST_SCHEMA       — optional, overrides the connection default

use crate::common::*;

use super::helpers::{PgCleanup, run_pg_matrix_export, setup_pg_matrix_table};

use std::path::Path;
use std::process::Command;

// ─── Gating + helpers ──────────────────────────────────────────────────────

struct SfConfig {
    connection: String,
    private_key: Option<String>,
    database: Option<String>,
    schema: Option<String>,
}

/// Read Snowflake configuration from env. Returns `None` if the connection name
/// is missing or `snow` is not on PATH — tests skip rather than fail.
fn sf_config() -> Option<SfConfig> {
    let connection = std::env::var("SNOWFLAKE_TEST_CONNECTION").ok()?;
    if Command::new("snow").arg("--version").output().is_err() {
        eprintln!("snowflake_load: skipping — `snow` CLI not on PATH");
        return None;
    }
    Some(SfConfig {
        connection,
        private_key: std::env::var("SNOWFLAKE_TEST_PRIVATE_KEY").ok(),
        database: std::env::var("SNOWFLAKE_TEST_DATABASE").ok(),
        schema: std::env::var("SNOWFLAKE_TEST_SCHEMA").ok(),
    })
}

impl SfConfig {
    /// A `snow sql` command pre-loaded with connection / key / db / schema and
    /// JSON output. Callers append `-q <sql>` (or `-f <file>`).
    fn snow(&self) -> Command {
        let mut c = Command::new("snow");
        c.arg("sql")
            .arg("-c")
            .arg(&self.connection)
            .arg("--format")
            .arg("json");
        if let Some(pk) = &self.private_key {
            c.arg("--private-key-file").arg(pk);
        }
        if let Some(db) = &self.database {
            c.arg("--database").arg(db);
        }
        if let Some(s) = &self.schema {
            c.arg("--schema").arg(s);
        }
        c
    }

    /// Run one statement, asserting success, returning the JSON rows
    /// (`--format json` yields an array of row objects keyed by column name —
    /// unquoted aliases fold to UPPERCASE keys).
    fn run_sql(&self, sql: &str) -> serde_json::Value {
        let out = self
            .snow()
            .arg("-q")
            .arg(sql)
            .output()
            .expect("`snow sql` must run");
        assert!(
            out.status.success(),
            "snow sql failed:\nsql: {sql}\nstderr: {}\nstdout: {}",
            String::from_utf8_lossy(&out.stderr),
            String::from_utf8_lossy(&out.stdout),
        );
        // `snow` prints a JSON array; an empty result set may print nothing.
        let stdout = String::from_utf8_lossy(&out.stdout);
        let trimmed = stdout.trim();
        if trimmed.is_empty() {
            return serde_json::Value::Array(vec![]);
        }
        serde_json::from_str(trimmed)
            .unwrap_or_else(|e| panic!("snow sql output is not JSON ({e}):\n{trimmed}"))
    }

    /// PUT a local Parquet file onto an internal stage (no compression — the
    /// file is already zstd Parquet; `OVERWRITE` so reruns are idempotent).
    fn put(&self, parquet: &Path, stage: &str) {
        let abs = parquet.canonicalize().expect("parquet path canonicalizes");
        let sql = format!(
            "PUT file://{} @{stage} AUTO_COMPRESS=FALSE OVERWRITE=TRUE",
            abs.display()
        );
        self.run_sql(&sql);
    }
}

/// Drop the Snowflake objects a test created, even on panic.
struct SfObjectsGuard<'a> {
    cfg: &'a SfConfig,
    stage: String,
    file_format: String,
    staging: String,
}

impl Drop for SfObjectsGuard<'_> {
    fn drop(&mut self) {
        // Best-effort cleanup; ignore output. Separate statements because
        // `snow sql -q` runs one at a time most predictably.
        let _ = self
            .cfg
            .snow()
            .arg("-q")
            .arg(format!("DROP TABLE IF EXISTS {}", self.staging))
            .output();
        let _ = self
            .cfg
            .snow()
            .arg("-q")
            .arg(format!("DROP STAGE IF EXISTS {}", self.stage))
            .output();
        let _ = self
            .cfg
            .snow()
            .arg("-q")
            .arg(format!("DROP FILE FORMAT IF EXISTS {}", self.file_format))
            .output();
    }
}

/// Look up one column's `INFER_SCHEMA` TYPE (lowercase column names) from the
/// already-loaded rows, panicking with context if absent.
fn infer_type<'a>(rows: &'a serde_json::Value, col: &str) -> &'a str {
    rows.as_array()
        .expect("INFER_SCHEMA returns an array")
        .iter()
        .find(|r| r["COLUMN_NAME"].as_str() == Some(col))
        .unwrap_or_else(|| panic!("INFER_SCHEMA has no column `{col}`; saw: {rows}"))["TYPE"]
        .as_str()
        .expect("INFER_SCHEMA TYPE is a string")
}

// ─── PostgreSQL matrix → Parquet → Snowflake ───────────────────────────────

#[test]
#[ignore = "live: requires SNOWFLAKE_TEST_CONNECTION env + snow CLI"]
fn snowflake_validates_postgres_type_matrix_parquet() {
    require_alive(LiveService::Postgres);
    let Some(cfg) = sf_config() else {
        eprintln!("snowflake_load: skipping (SNOWFLAKE_TEST_CONNECTION not set)");
        return;
    };

    // 1) PG matrix → Parquet.
    let pg_table = unique_name("sf_pg");
    let enum_type = setup_pg_matrix_table(&pg_table);
    let _pg_guard = PgCleanup {
        table: pg_table.clone(),
        enum_type,
    };
    let out_dir = tempfile::tempdir().unwrap();
    run_pg_matrix_export(&pg_table, "parquet", out_dir.path());
    let parquet = files_with_extension(out_dir.path(), "parquet")
        .into_iter()
        .next()
        .expect("one parquet part");

    // 2) Stage + file format + PUT + INFER_SCHEMA template + COPY — the
    //    documented load flow the recovery SQL preamble describes.
    let tag = unique_name("sfpg");
    let stage = format!("rivet_stage_{tag}");
    let file_format = format!("rivet_pq_{tag}");
    let staging = format!("rivet_stg_{tag}");
    let _objs = SfObjectsGuard {
        cfg: &cfg,
        stage: stage.clone(),
        file_format: file_format.clone(),
        staging: staging.clone(),
    };

    cfg.run_sql(&format!(
        "CREATE OR REPLACE FILE FORMAT {file_format} TYPE=PARQUET BINARY_AS_TEXT=FALSE"
    ));
    cfg.run_sql(&format!(
        "CREATE OR REPLACE STAGE {stage} FILE_FORMAT={file_format}"
    ));
    cfg.run_sql("ALTER SESSION SET TIMEZONE='UTC'");
    cfg.put(&parquet, &stage);

    // 3) Autoload schema — the columns ADR-0014 cares about. These are exactly
    //    the `autoload_type` values `snowflake::native` claims.
    let inferred = cfg.run_sql(&format!(
        "SELECT COLUMN_NAME, TYPE FROM TABLE(INFER_SCHEMA(\
         LOCATION=>'@{stage}', FILE_FORMAT=>'{file_format}'))"
    ));
    let starts = |col: &str, want: &str| {
        let got = infer_type(&inferred, col);
        assert!(
            got.starts_with(want),
            "Snowflake autoload type for `{col}`: expected {want}*, got {got}",
        );
    };
    starts("amount", "NUMBER");
    starts("fee", "NUMBER");
    starts("price", "NUMBER");
    starts("c_bool", "BOOLEAN");
    starts("c_date", "DATE");
    starts("label", "TEXT");
    starts("attrs", "TEXT"); // JSON degrades to TEXT
    starts("attrs_json", "TEXT");
    starts("uid", "BINARY"); // UUID degrades to 16-byte BINARY
    starts("raw_bytes", "BINARY");
    starts("c_time", "NUMBER"); // TIME degrades to µs-of-day NUMBER
    starts("created_at", "NUMBER"); // naive timestamp degrades to µs NUMBER
    starts("created_at_tz", "TIMESTAMP_NTZ"); // tz timestamp lands NTZ holding the UTC instant
    starts("tags", "VARIANT"); // list degrades to VARIANT (the JSON array), not native ARRAY
    starts("nums", "VARIANT");

    // 4) Materialise the staging table and recover native values via the exact
    //    `cast_sql` expressions the resolver emits (INFER_SCHEMA names are
    //    lowercase + case-sensitive, so every ref is double-quoted).
    cfg.run_sql(&format!(
        "CREATE OR REPLACE TABLE {staging} USING TEMPLATE (\
         SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM TABLE(INFER_SCHEMA(\
         LOCATION=>'@{stage}', FILE_FORMAT=>'{file_format}')))"
    ));
    cfg.run_sql(&format!(
        "COPY INTO {staging} FROM @{stage} FILE_FORMAT=(FORMAT_NAME='{file_format}') \
         MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE"
    ));

    let recovered = cfg.run_sql(&format!(
        r#"SELECT
             PARSE_JSON("attrs"):"tier"::STRING AS TIER,
             REGEXP_REPLACE(LOWER(HEX_ENCODE("uid")),'^(.{{8}})(.{{4}})(.{{4}})(.{{4}})(.{{12}})$','\\1-\\2-\\3-\\4-\\5') AS UID_TEXT,
             TO_CHAR(TO_TIMESTAMP_NTZ("created_at", 6),'YYYY-MM-DD HH24:MI:SS.FF6') AS NAIVE_TS,
             TO_CHAR(TIME_FROM_PARTS(0,0,FLOOR("c_time"/1000000),MOD("c_time",1000000)*1000),'HH24:MI:SS.FF6') AS C_TIME,
             ARRAY_SIZE("tags"::ARRAY) AS TAGS_LEN,
             GET("tags"::ARRAY, 0)::STRING AS TAGS_0,
             ARRAY_SIZE("nums"::ARRAY) AS NUMS_LEN
           FROM {staging} WHERE "id" = 1"#
    ));
    let r = &recovered[0];
    assert_eq!(r["TIER"], "gold", "PARSE_JSON recovers native VARIANT");
    assert_eq!(
        r["UID_TEXT"], "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380011",
        "HEX_ENCODE + REGEXP recovers canonical UUID text"
    );
    assert_eq!(
        r["NAIVE_TS"], "2035-08-07 09:08:07.987654",
        "TO_TIMESTAMP_NTZ recovers the naive instant from µs NUMBER"
    );
    assert_eq!(
        r["C_TIME"], "09:08:07.987654",
        "TIME_FROM_PARTS recovers TIME from µs-of-day NUMBER"
    );
    // `::ARRAY` recovers the native ARRAY from the autoloaded VARIANT.
    assert_eq!(r["TAGS_LEN"], 2, "tags recovers to a 2-element ARRAY");
    assert_eq!(r["TAGS_0"], "alpha", "tags[0] element survives");
    assert_eq!(r["NUMS_LEN"], 3, "nums recovers to a 3-element ARRAY");
}

// ─── MySQL UInt64 overflow → Snowflake (the warehouse-matrix live gap) ──────

#[test]
#[ignore = "live: requires SNOWFLAKE_TEST_CONNECTION env + snow CLI + docker mysql"]
fn snowflake_validates_mysql_uint64_overflow_survives_via_decimal_override() {
    use mysql::prelude::Queryable;
    require_alive(LiveService::Mysql);
    let Some(cfg) = sf_config() else {
        eprintln!("snowflake_load: skipping (SNOWFLAKE_TEST_CONNECTION not set)");
        return;
    };

    // The exact gap the warehouse matrix flagged: UInt64 > INT64_MAX had zero LIVE
    // Snowflake proof (PostgreSQL has no unsigned-64 type; only MySQL produces it,
    // and there was no snowflake_validates_mysql test). u64::MAX overflows a Parquet
    // INT64 read, so it MUST ride as decimal(20,0) — the documented override — and
    // land as Snowflake NUMBER with the value intact, never a silent overflow.
    const U64_MAX: &str = "18446744073709551615";
    let tbl = unique_name("sf_u64");
    let mut c = mysql_connect();
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id INT PRIMARY KEY, c_bigint_u BIGINT UNSIGNED NOT NULL)"
    ))
    .unwrap();
    let _tbl = MysqlTable::adopt(tbl.clone());
    c.query_drop(format!("INSERT INTO {tbl} VALUES (1, {U64_MAX})"))
        .unwrap();

    // Export with the decimal(20,0) override → Parquet DECIMAL(20,0).
    let out_dir = tempfile::tempdir().unwrap();
    let d = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"source: {{ type: mysql, url: "{MYSQL_URL}" }}
exports:
  - name: {tbl}
    query: "SELECT id, c_bigint_u FROM {tbl}"
    mode: full
    format: parquet
    columns: {{ c_bigint_u: "decimal(20,0)" }}
    destination: {{ type: local, path: "{out}" }}
"#,
        out = out_dir.path().display(),
    );
    run_rivet_ok(&write_config(&d, &yaml));
    let parquet = files_with_extension(out_dir.path(), "parquet")
        .into_iter()
        .next()
        .expect("one parquet part");

    let tag = unique_name("sfu64");
    let stage = format!("rivet_stage_{tag}");
    let file_format = format!("rivet_pq_{tag}");
    let staging = format!("rivet_stg_{tag}");
    let _objs = SfObjectsGuard {
        cfg: &cfg,
        stage: stage.clone(),
        file_format: file_format.clone(),
        staging: staging.clone(),
    };
    cfg.run_sql(&format!(
        "CREATE OR REPLACE FILE FORMAT {file_format} TYPE=PARQUET BINARY_AS_TEXT=FALSE"
    ));
    cfg.run_sql(&format!(
        "CREATE OR REPLACE STAGE {stage} FILE_FORMAT={file_format}"
    ));
    cfg.put(&parquet, &stage);

    // Autoload: decimal(20,0) lands as NUMBER — not a truncated/overflowed INT.
    let inferred = cfg.run_sql(&format!(
        "SELECT COLUMN_NAME, TYPE FROM TABLE(INFER_SCHEMA(\
         LOCATION=>'@{stage}', FILE_FORMAT=>'{file_format}'))"
    ));
    assert!(
        infer_type(&inferred, "c_bigint_u").starts_with("NUMBER"),
        "u64 via decimal(20,0) must autoload as Snowflake NUMBER, got {}",
        infer_type(&inferred, "c_bigint_u")
    );

    // Load + read back — the whole point: u64::MAX survives, no overflow.
    cfg.run_sql(&format!(
        "CREATE OR REPLACE TABLE {staging} USING TEMPLATE (\
         SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM TABLE(INFER_SCHEMA(\
         LOCATION=>'@{stage}', FILE_FORMAT=>'{file_format}')))"
    ));
    cfg.run_sql(&format!(
        "COPY INTO {staging} FROM @{stage} FILE_FORMAT=(FORMAT_NAME='{file_format}') \
         MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE"
    ));
    let rows = cfg.run_sql(&format!(
        r#"SELECT "c_bigint_u"::VARCHAR AS V FROM {staging}"#
    ));
    let got = rows
        .as_array()
        .and_then(|a| a.first())
        .and_then(|r| r["V"].as_str())
        .unwrap_or("<none>");
    assert_eq!(
        got, U64_MAX,
        "u64::MAX must survive the round-trip into Snowflake NUMBER, not overflow"
    );
}
