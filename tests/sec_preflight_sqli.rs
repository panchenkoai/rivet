//! SEC-RED live tests for cluster `preflight-sqli` (V0 Postgres, V1 MySQL).
//!
//! Vulnerability: the preflight cursor/chunk range probe interpolates the
//! config-author-controlled `chunk_column` / `cursor_column` RAW into a
//! `SELECT min(COL), max(COL) FROM …` query with no `quote_ident`:
//!
//! - Postgres — `src/preflight/postgres.rs:242` (`get_cursor_range_pg`):
//!   `format!("SELECT min({cursor_col})::text, max({cursor_col})::text FROM {tbl}")`
//! - MySQL — `src/preflight/mysql.rs:137`:
//!   `format!("SELECT CAST(min({col}) AS CHAR), CAST(max({col}) AS CHAR) FROM ({base}) AS _rivet")`
//!
//! The MSSQL sibling (`src/preflight/mssql.rs:137`) is already fixed — it passes
//! `crate::sql::quote_ident(SourceType::Mssql, col)` as the expression, so ANY
//! `chunk_column` value is treated as a single identifier. That is the SECURE
//! reference these tests assert PG/MySQL must match.
//!
//! Threat model: an untrusted config author (or a config from an untrusted
//! source / templating layer) supplies a crafted `chunk_column`. When an
//! operator runs `rivet check` (or `rivet plan`) the payload is executed
//! verbatim against their live database.
//!
//! ── How the test distinguishes RAW interpolation from quote_ident ──────────
//!
//! The probe folds the column into `SELECT min(COL)…, max(COL)… FROM orders`.
//! The `Cursor range:` line (`src/preflight/mod.rs::print_diagnostic`) prints
//! ONLY when both `cursor_min` and `cursor_max` come back `Some`.
//!
//!   * SAFE (quote_ident): the whole payload becomes ONE quoted identifier
//!     (`"id)::text, max(id"` / `` `id) AS CHAR), CAST(min(id` ``). No such
//!     column exists on `orders`, so the probe errors, returns `(None, None)`,
//!     and the diagnostic prints NO `Cursor range:` line. (`check` stays rc=0:
//!     preflight is non-fatal.)
//!   * VULNERABLE (raw): the payload closes the `min(`/`max(` call and re-opens
//!     a fresh aggregate so the query parses as several valid text columns.
//!     The probe SUCCEEDS, the first two columns come back `Some`, and the
//!     diagnostic prints a `Cursor range:` line — proof the injected SQL ran.
//!
//! So: presence of a `Cursor range:` line for an obviously-not-a-column payload
//! is the injection signal. These tests assert it is ABSENT (the SECURE
//! outcome) and therefore FAIL against the current raw-interpolation code.
//!
//! Observed RED behavior (docker postgres / mysql, seeded `orders`, PK `id`):
//!   PG   payload `id)::text, max(id`            → prints `Cursor range: … .. …`
//!   MySQL payload `id) AS CHAR), CAST(min(id`   → prints `Cursor range: … .. …`
//! Both should print NO cursor-range line once the column is quoted.

mod common;

use common::*;

/// Seeded benchmark table the live stack ships with: `orders`, 2500 rows,
/// PK `id`. Same table `audit_preflight_table.rs` keys on.
const SEEDED_TABLE: &str = "orders";

/// Captured output of a `rivet check` run. Exit code is NOT asserted: the
/// SECURE and VULNERABLE paths both exit 0 (preflight is non-fatal), so the
/// injection signal is in stdout, not the status.
struct CheckOutput {
    stdout: String,
    stderr: String,
}

/// Run `rivet check --config <cfg> --export <name>` against a live DB and
/// capture stdout/stderr. Does not assert exit status — see `CheckOutput`.
fn run_check(config_path: &std::path::Path, export_name: &str) -> CheckOutput {
    let out = std::process::Command::new(RIVET_BIN)
        .args([
            "check",
            "--config",
            config_path.to_str().unwrap(),
            "--export",
            export_name,
        ])
        .output()
        .expect("spawn rivet check");
    CheckOutput {
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

/// Build a chunked `rivet.yaml` whose `chunk_column` is the injection payload.
/// `source_block` selects the engine; `payload` is emitted as a double-quoted
/// YAML scalar so its parens/commas/colons survive parsing intact.
fn config_with_chunk_column(
    out_dir: &std::path::Path,
    name: &str,
    source_block: &str,
    payload: &str,
) -> String {
    // Defensive: our payloads contain no `"`, but escape anyway so a future
    // payload edit can't silently break the YAML.
    let yaml_payload = payload.replace('\\', "\\\\").replace('"', "\\\"");
    format!(
        r#"
source:
{source_block}

exports:
  - name: {name}
    table: {SEEDED_TABLE}
    mode: chunked
    chunk_column: "{yaml_payload}"
    chunk_size: 500
    format: parquet
    destination:
      type: local
      path: {out}
"#,
        out = out_dir.display()
    )
}

// ── V0 (Postgres) ──────────────────────────────────────────────────────────

// SEC-RED V0: preflight raw-interpolates chunk_column into the PG range probe (src/preflight/postgres.rs:242) — SQL injection. Asserts SECURE behavior (column treated as one identifier, no injected range); expected to FAIL until quote_ident is applied.
#[test]
#[ignore = "live: postgres"]
fn sec_pg_chunk_column_quoted_not_injected() {
    require_alive(LiveService::Postgres);

    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    // Raw template: `SELECT min(COL)::text, max(COL)::text FROM orders`.
    // With this payload the raw form becomes
    //   SELECT min(id)::text, max(id)::text, max(id)::text, max(id)::text FROM orders
    // — four valid TEXT columns; the probe reads cols 0/1 and prints a range.
    // Quoted, the whole thing is one identifier `"id)::text, max(id"` that does
    // not exist on `orders`, so the probe fails and prints no range line.
    let payload = "id)::text, max(id";
    let name = unique_name("sec_pg_sqli");
    let source_block = format!("  type: postgres\n  url: \"{POSTGRES_URL}\"");
    let yaml = config_with_chunk_column(out_dir.path(), &name, &source_block, payload);
    let cfg = write_config(&cfg_dir, &yaml);

    let out = run_check(&cfg, &name);

    // SECURE: a column literally named `id)::text, max(id` does not exist, so a
    // quote_ident'd probe finds nothing and the diagnostic prints NO cursor
    // range. Today the raw interpolation makes the payload parse as valid SQL,
    // the probe succeeds, and a `Cursor range:` line appears — that line is the
    // injection signal.
    assert!(
        !out.stdout.contains("Cursor range:"),
        "SQL injection via chunk_column: the payload {payload:?} must be treated \
         as a single (non-existent) identifier, so NO 'Cursor range:' line should \
         appear. Its presence proves the injected aggregate executed.\n\
         stdout:\n{}\nstderr:\n{}",
        out.stdout,
        out.stderr,
    );
}

// ── V1 (MySQL) ───────────────────────────────────────────────────────────────

// SEC-RED V1: preflight raw-interpolates chunk_column into the MySQL range probe (src/preflight/mysql.rs:137) — SQL injection. Asserts SECURE behavior (column treated as one identifier, no injected range); expected to FAIL until quote_ident is applied.
#[test]
#[ignore = "live: mysql"]
fn sec_mysql_chunk_column_quoted_not_injected() {
    require_alive(LiveService::Mysql);

    let out_dir = tempfile::tempdir().unwrap();
    let cfg_dir = tempfile::tempdir().unwrap();

    // Raw template (no strip-star on MySQL; always a derived table):
    //   SELECT CAST(min(COL) AS CHAR), CAST(max(COL) AS CHAR) FROM (SELECT * FROM orders) AS _rivet
    // With this payload the raw form becomes
    //   SELECT CAST(min(id) AS CHAR), CAST(min(id) AS CHAR), CAST(max(id) AS CHAR),
    //          CAST(min(id) AS CHAR) FROM (SELECT * FROM orders) AS _rivet
    // — four valid CHAR columns; the probe reads cols 0/1 and prints a range.
    // Backtick-quoted, the whole thing is one identifier that does not exist.
    let payload = "id) AS CHAR), CAST(min(id";
    let name = unique_name("sec_mysql_sqli");
    let source_block = format!("  type: mysql\n  url: \"{MYSQL_URL}\"");
    let yaml = config_with_chunk_column(out_dir.path(), &name, &source_block, payload);
    let cfg = write_config(&cfg_dir, &yaml);

    let out = run_check(&cfg, &name);

    // SECURE: a column literally named `id) AS CHAR), CAST(min(id` does not
    // exist, so a quote_ident'd probe finds nothing and the diagnostic prints
    // NO cursor range. Today the raw interpolation makes the payload parse as
    // valid SQL, the probe succeeds, and a `Cursor range:` line appears — that
    // line is the injection signal.
    assert!(
        !out.stdout.contains("Cursor range:"),
        "SQL injection via chunk_column: the payload {payload:?} must be treated \
         as a single (non-existent) identifier, so NO 'Cursor range:' line should \
         appear. Its presence proves the injected aggregate executed.\n\
         stdout:\n{}\nstderr:\n{}",
        out.stdout,
        out.stderr,
    );
}
