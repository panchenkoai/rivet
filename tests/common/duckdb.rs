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

// ── row/value oracles ────────────────────────────────────────────────────────
//
// Why these exist, and what they are NOT for.
//
// `common::parquet::{parquet_rows, total_parquet_rows}` decode with the SAME
// `parquet` crate rivet ENCODES with. That is independent of rivet's COUNTERS —
// which was the point when they were written — but not of rivet's CODEC: a fault
// in the shared encode/decode path cancels out and the assertion still passes.
// For a completeness or value claim, DuckDB is the reader that does not share
// that failure mode.
//
// Deliberately NOT extended to file COUNTS or file HASHES. Counting entries is
// `std::fs::read_dir` and hashing bytes is a digest over the file — neither goes
// through rivet's codec, so routing them through DuckDB would buy nothing and
// cost a container round-trip. Independence is only worth paying for where a
// DECODER sits between the bytes and the claim.
//
// The files must live under the shared bind mount, so the test's output dir has
// to come from `duckdb_shared_workdir` rather than a bare `tempfile::tempdir()`.

/// Total rows across every `.parquet` under `container_dir`, read by DuckDB.
///
/// `container_dir` is the second element of [`duckdb_shared_workdir`].
pub fn duckdb_parquet_rows(container_dir: &str) -> i64 {
    duckdb_scalar_or_empty(container_dir, "count(*)", "*").unwrap_or(0)
}

/// The python that resolves a destination's MANIFEST-DECLARED parts, shared by
/// every `*_declared` helper below.
///
/// A `**/*.parquet` glob answers "what does the destination HOLD"; every
/// consumer — `rivet load`, `rivet validate`, reconcile — reads what the run
/// DECLARED. On a crash/resume cell that difference IS the test: a crashed run
/// leaves durable parts no manifest names, and a glob counts them as delivered.
///
/// Measured 2026-08-23 on a 5-part keyset export of 1000 rows: with one part
/// dropped from the manifests and its FILE left on disk, the glob read 1000/1000
/// and PASSED while the declared list read 800 and diverged from the source.
///
/// Mirrors `dev/release_oracle/scenarios.py::_manifest_declared_parts` — the same
/// union over the immutable `manifest-*.json` copies, falling back to the
/// canonical `manifest.json` only when no copy exists (the sink writes copies for
/// exactly the repeated-run case where the canonical one is last-writer-wins).
///
/// An empty result is an OUTCOME ("nothing was declared"), never a reason to fall
/// back to the glob — a fallback would restore the blindness this removes.
const DECLARED_PARTS_PY: &str = r#"
import json, glob as _g, os
def _declared(root):
    copies = sorted(_g.glob(root + "/**/manifest-*.json", recursive=True))
    if not copies:
        c = os.path.join(root, "manifest.json")
        copies = [c] if os.path.isfile(c) else []
    out = set()
    for d in copies:
        try:
            art = json.load(open(d))
        except Exception:
            continue
        for f in art.get("parts") or []:
            if isinstance(f, dict) and f.get("status") not in (None, "committed"):
                continue
            name = (f.get("path") or f.get("name")) if isinstance(f, dict) else f
            if not name:
                continue
            cand = name if os.path.isabs(name) else os.path.join(os.path.dirname(d), name)
            if os.path.isfile(cand):
                out.add(cand)
    return sorted(out)
"#;

/// One aggregate over only the parts the manifest DECLARES, or `None` when it
/// declares nothing readable. The manifest-scoped twin of
/// [`duckdb_scalar_or_empty`] — see [`DECLARED_PARTS_PY`] for why both exist.
fn duckdb_declared_scalar_or_empty(container_dir: &str, agg: &str, col: &str) -> Option<i64> {
    let py = format!(
        r#"{decl}
import duckdb, json, sys
files = _declared({dir_repr})
if not files:
    sys.stdout.write("null")
else:
    con = duckdb.connect()
    v = con.execute("SELECT {agg} FROM read_parquet(" + repr(files) + ")").fetchone()[0]
    sys.stdout.write(json.dumps(int(v)))
"#,
        decl = DECLARED_PARTS_PY,
        dir_repr = python_repr(container_dir),
        agg = agg.replace("{col}", col),
    );
    let out = duckdb_run_python(&py);
    serde_json::from_str::<Option<i64>>(out.trim())
        .unwrap_or_else(|e| panic!("duckdb declared scalar not an int/null: {e}; raw {out}"))
}

/// [`duckdb_distinct_set`] over the manifest-DECLARED parts.
///
/// The set answers "WHICH", which is what a crash/resume cell needs — and it must
/// come from what the run declared, not from what the directory holds, because a
/// crashed run leaves durable parts no manifest names.
pub fn duckdb_declared_distinct_set(
    container_dir: &str,
    column: &str,
) -> std::collections::BTreeSet<String> {
    let py = format!(
        r#"{decl}
import duckdb, json, sys
files = _declared({dir_repr})
if not files:
    sys.stdout.write(json.dumps([]))
else:
    con = duckdb.connect()
    rows = con.execute(
        "SELECT DISTINCT CAST({column} AS VARCHAR) AS v FROM read_parquet(" + repr(files) +
        ") WHERE {column} IS NOT NULL").fetchall()
    sys.stdout.write(json.dumps([str(r[0]) for r in rows]))
"#,
        decl = DECLARED_PARTS_PY,
        dir_repr = python_repr(container_dir),
        column = column,
    );
    let out = duckdb_run_python(&py);
    serde_json::from_str::<Vec<String>>(out.trim())
        .unwrap_or_else(|e| panic!("duckdb declared distinct set: {e}; raw {out}"))
        .into_iter()
        .collect()
}

/// Row count over the manifest-DECLARED parts — the declared twin of
/// [`duckdb_parquet_rows`].
pub fn duckdb_declared_rows(container_dir: &str) -> i64 {
    duckdb_declared_scalar_or_empty(container_dir, "count(*)", "*").unwrap_or(0)
}

/// Exact row count AND exact distinct count over the manifest-DECLARED parts.
///
/// The manifest-scoped twin of [`duckdb_assert_rows_and_distinct`]. Use this on
/// any cell where a run may have left parts no manifest names — every crash,
/// resume, or repeated-run case — and the glob version only where the test is
/// deliberately asking what the destination HOLDS (orphan/GC cells).
pub fn duckdb_declared_assert_rows_and_distinct(
    container_dir: &str,
    column: &str,
    expect_rows: i64,
    expect_distinct: i64,
    what: &str,
) {
    let rows = duckdb_declared_scalar_or_empty(container_dir, "count(*)", column).unwrap_or(0);
    let distinct = duckdb_declared_scalar_or_empty(
        container_dir,
        &format!("count(DISTINCT {column})"),
        column,
    )
    .unwrap_or(0);
    assert_eq!(
        (rows, distinct),
        (expect_rows, expect_distinct),
        "{what}: the MANIFEST-DECLARED parts under {container_dir} hold {rows} rows / \
         {distinct} distinct {column}, expected {expect_rows}/{expect_distinct}. \
         Declared, not globbed: a crashed run leaves durable parts no manifest names, \
         and no consumer will ever read those"
    );
}

/// [`duckdb_declared_assert_rows_and_distinct`] for the common "no loss AND no
/// duplication" claim, where both numbers are the same.
pub fn duckdb_declared_assert_complete(
    container_dir: &str,
    column: &str,
    expected: i64,
    what: &str,
) {
    duckdb_declared_assert_rows_and_distinct(container_dir, column, expected, expected, what);
}

/// One aggregate over every parquet under `container_dir`, or `None` when the
/// directory holds NO parquet at all.
///
/// An empty destination is a legitimate expected value — "resume must capture
/// only new changes" asserts exactly zero — but `read_parquet` on a glob that
/// matches nothing is an ERROR, so the first version of these helpers panicked
/// on the one answer some tests are looking for. The emptiness is resolved in
/// python, where the exception is catchable, rather than by asking the caller to
/// know in advance whether any file exists.
fn duckdb_scalar_or_empty(container_dir: &str, agg: &str, col: &str) -> Option<i64> {
    let py = format!(
        r#"
import duckdb, json, sys, glob
files = glob.glob({dir_repr} + "/**/*.parquet", recursive=True)
if not files:
    sys.stdout.write("null")
else:
    con = duckdb.connect()
    v = con.execute("SELECT {agg} FROM read_parquet('" + {dir_repr} + "/**/*.parquet')").fetchone()[0]
    sys.stdout.write(json.dumps(int(v)))
"#,
        dir_repr = python_repr(container_dir),
        agg = agg.replace("{col}", col),
    );
    let out = duckdb_run_python(&py);
    serde_json::from_str::<Option<i64>>(out.trim())
        .unwrap_or_else(|e| panic!("duckdb scalar not an int/null: {e}; raw {out}"))
}

/// Distinct values of `column` across every `.parquet` under `container_dir`.
///
/// The count that separates "no rows lost" from "no rows lost AND none
/// duplicated" — a retry or a resume can satisfy the first and break the second.
pub fn duckdb_parquet_distinct(container_dir: &str, column: &str) -> i64 {
    duckdb_scalar_or_empty(container_dir, &format!("count(DISTINCT {column})"), column).unwrap_or(0)
}

/// Assert total and distinct in one call — the shape a completeness claim needs.
///
/// Equal totals with a lower distinct count is duplication; a lower total is
/// loss. Reporting both at once names which one happened instead of leaving the
/// reader to guess from a single number.
pub fn duckdb_assert_complete(container_dir: &str, column: &str, expected: i64, what: &str) {
    duckdb_assert_rows_and_distinct(container_dir, column, expected, expected, what);
}

/// The general primitive: exact row count AND exact distinct count.
///
/// The named claims above are the two common cases; a test whose expectation is
/// neither — two full snapshots into one prefix, say, where 200 rows over 100
/// distinct keys is CORRECT — states both numbers here rather than bending one of
/// the named helpers into a shape it does not mean.
pub fn duckdb_assert_rows_and_distinct(
    container_dir: &str,
    column: &str,
    rows: i64,
    distinct: i64,
    what: &str,
) {
    let got_rows = duckdb_parquet_rows(container_dir);
    let got_distinct = duckdb_parquet_distinct(container_dir, column);
    assert_eq!(
        (got_rows, got_distinct),
        (rows, distinct),
        "{what}: expected {rows} rows over {distinct} distinct `{column}`, DuckDB \
         read {got_rows} rows / {got_distinct} distinct"
    );
}

/// The AT-LEAST-ONCE shape: every key present, duplicates permitted.
///
/// Distinct must equal `expected` exactly (a missing key is loss) while the total
/// may exceed it (a crash-orphaned page re-exported on recovery is correct
/// behaviour, not a defect). Deliberately NOT [`duckdb_assert_complete`], which
/// demands total == distinct and would fail a legitimate recovery — the two
/// claims are different and collapsing them would make one of them a lie.
pub fn duckdb_assert_at_least_once(container_dir: &str, column: &str, expected: i64, what: &str) {
    let total = duckdb_parquet_rows(container_dir);
    let distinct = duckdb_parquet_distinct(container_dir, column);
    assert_eq!(
        distinct, expected,
        "{what}: every `{column}` must survive — DuckDB read {distinct} distinct of \
         {expected} expected ({total} rows total). A missing key is loss."
    );
    assert!(
        total >= expected,
        "{what}: {total} rows for {expected} distinct `{column}` — fewer rows than \
         keys is impossible unless the read itself is wrong"
    );
}

/// Distinct values of `column` as a SET, read by DuckDB.
///
/// The counting helpers above answer "how many"; a set answers "which", which is
/// what a CDC test needs — membership of a specific `_id`, or the exact symmetric
/// difference against the source. Values come back stringified (the same
/// convention as [`duckdb_run_sql_json`]), so an integer `_id` reads as "42".
pub fn duckdb_distinct_set(
    container_dir: &str,
    column: &str,
) -> std::collections::BTreeSet<String> {
    if duckdb_scalar_or_empty(container_dir, "count(*)", "*").is_none() {
        return std::collections::BTreeSet::new(); // no parquet at all
    }
    let v = duckdb_run_sql_json(&format!(
        "SELECT DISTINCT CAST({column} AS VARCHAR) AS v \
         FROM read_parquet('{container_dir}/**/*.parquet') WHERE {column} IS NOT NULL"
    ));
    v["rows"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .filter_map(|r| r[0].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// [`duckdb_distinct_set`] for an INTEGER key column.
///
/// The stringified form is the right default (a Mongo `_id` may be an ObjectId or
/// a string), but a SQL test comparing against `(0..10).collect()` wants the
/// numbers — converting at every call site would put a parse in the assertion,
/// which is where a silent `unwrap_or(0)` would hide a real decode failure.
pub fn duckdb_distinct_i64_set(
    container_dir: &str,
    column: &str,
) -> std::collections::BTreeSet<i64> {
    duckdb_distinct_set(container_dir, column)
        .into_iter()
        .map(|v| {
            v.parse().unwrap_or_else(|e| {
                panic!("`{column}` is not an integer in the destination: {v:?} ({e})")
            })
        })
        .collect()
}
