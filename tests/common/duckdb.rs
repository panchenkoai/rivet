//! DuckDB validator helpers (ADR-0014 §4).
//!
//! Tests reach DuckDB through the long-running `rivet-duckdb` container
//! (see `docker-compose.yaml`). The container has the `duckdb` python module
//! installed; we shell into it with `docker exec` and read JSON back.
//!
//! Path / workdir helpers (`live_container_path`, `live_shared_workdir`)
//! live in [`super::env`] because they are shared with the ClickHouse
//! validator — both containers see the same bind mount under `/work`.
//!
//! Why python and not the `duckdb` CLI: the CLI ships a custom REPL output
//! format that's annoying to parse, while `duckdb` + `json.dumps(...)` lets
//! us round-trip arbitrary types (decimals, dates, blobs, lists) through one
//! canonical stable shape.

#![allow(dead_code)]

use std::collections::HashMap;
use std::process::Command;

use super::env::DUCKDB_CONTAINER;

/// Re-exported under the historical `duckdb_*` names so existing call sites
/// keep working. The implementations live in [`super::env`] because both
/// the DuckDB and ClickHouse helpers point at the same bind mount.
pub use super::env::live_container_path as duckdb_container_path;
pub use super::env::live_shared_workdir as duckdb_shared_workdir;

/// Run a single DuckDB query inside the container and return its rows as JSON
/// (a `Vec` of objects keyed by column name). The DuckDB driver is created
/// fresh per call — fine for tests, not fine for hot paths.
///
/// All values are stringified via `str(...)` in Python so callers don't have
/// to worry about JSON-unsafe DuckDB types (e.g. `decimal.Decimal`, `bytes`,
/// `datetime`). Strings round-trip; numbers come back as quoted strings —
/// callers parse what they need.
pub fn duckdb_run_sql_json(sql: &str) -> serde_json::Value {
    // The python snippet:
    //   - opens an in-memory duckdb connection
    //   - executes the SQL
    //   - collects (description, rows) and emits {columns:[...], rows:[[...]]}
    //   - every cell is `str(v)` so json.dumps never trips on Decimal etc.
    let py = format!(
        r#"
import duckdb, json, sys
con = duckdb.connect()
cur = con.execute({sql_repr})
cols = [d[0] for d in cur.description] if cur.description else []
out_rows = []
for row in cur.fetchall():
    out_rows.append([None if v is None else str(v) for v in row])
sys.stdout.write(json.dumps({{"columns": cols, "rows": out_rows}}))
"#,
        sql_repr = python_repr(sql),
    );
    let out = duckdb_run_python(&py);
    serde_json::from_str(&out)
        .unwrap_or_else(|e| panic!("parse duckdb json: {e}\nraw stdout: {out}"))
}

/// Run an arbitrary python snippet inside the `rivet-duckdb` container and
/// return its stdout. The snippet has `duckdb`, `pyarrow`, and `pytz`
/// available — see `docker-compose.yaml` for the install list. Use this for
/// pyarrow-driven assertions (`pyarrow.parquet`) that go beyond what plain
/// SQL through DuckDB can express (e.g. field metadata, column statistics).
pub fn duckdb_run_python(py: &str) -> String {
    let output = Command::new("docker")
        .args(["exec", "-i", DUCKDB_CONTAINER, "python", "-c", py])
        .output()
        .expect("spawn docker exec rivet-duckdb python");
    if !output.status.success() {
        panic!(
            "python exec failed (status {:?}):\nSCRIPT:\n{py}\nSTDERR:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    String::from_utf8_lossy(&output.stdout).to_string()
}

/// Like [`duckdb_run_python`] but parses stdout as JSON.
pub fn duckdb_run_python_json(py: &str) -> serde_json::Value {
    let out = duckdb_run_python(py);
    serde_json::from_str(out.trim())
        .unwrap_or_else(|e| panic!("parse python json: {e}\nstdout: {out}"))
}

/// Turn a DuckDB `DESCRIBE …` result (as returned by [`duckdb_run_sql_json`])
/// into a `column_name → column_type` map. Both `*_load.rs` validators use
/// this shape to compare the autoload schema against an expected set.
pub fn duckdb_parse_describe(described: &serde_json::Value) -> HashMap<String, String> {
    described["rows"]
        .as_array()
        .expect("DESCRIBE result has a `rows` array")
        .iter()
        .map(|r| {
            let a = r.as_array().expect("DESCRIBE row is an array");
            (
                a[0].as_str().expect("name col is a string").to_string(),
                a[1].as_str().expect("type col is a string").to_string(),
            )
        })
        .collect()
}

/// Python `repr(...)` of a string — safer than wrapping in our own quotes,
/// because DuckDB SQL often contains quotes, newlines, and backslashes.
fn python_repr(s: &str) -> String {
    let escaped: String = s
        .chars()
        .flat_map(|c| match c {
            '\\' => "\\\\".chars().collect::<Vec<_>>(),
            '\'' => "\\'".chars().collect(),
            '\n' => "\\n".chars().collect(),
            '\r' => "\\r".chars().collect(),
            '\t' => "\\t".chars().collect(),
            c if (c as u32) < 0x20 => format!("\\x{:02x}", c as u32).chars().collect(),
            c => vec![c],
        })
        .collect();
    format!("'{escaped}'")
}
