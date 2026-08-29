//! Shared Parquet read-back primitives for destination-verifying tests.
//!
//! The data-correctness audit's core lesson: a completeness test must
//! INDEPENDENTLY re-read the destination, not trust rivet's own counters. Every
//! such test was rolling its own `ParquetRecordBatchReaderBuilder` boilerplate
//! (~20 files). These three primitives centralise the reader; tests compose the
//! dir-level aggregation they need locally (a one-liner over
//! [`files_with_extension`]), so the duplicated *reader* is gone without forcing
//! one rigid dir-helper shape on every caller.

#![allow(dead_code)]

use std::collections::{BTreeSet, HashMap};
use std::path::Path;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::files_with_extension;

/// Stage a host directory of Parquet under the shared bind mount and hand back
/// its in-container path, so a value claim can be read by DuckDB instead of by
/// the same `parquet` crate rivet wrote it with.
///
/// The bridge exists because of a NAMESPACE boundary, not a data one. Tests
/// write to their own `tempfile::tempdir()` — a real directory the host reads
/// fine — but the validator containers have exactly ONE path mounted
/// (`tests/.live-tmp` → `/work`), so from inside them that tempdir does not
/// exist. The failure then surfaces as `0 rows` or "no files match", which
/// reads as data loss and costs an hour before anyone suspects the mount. (It
/// cost exactly that on 2026-08-05, when three tests run from a secondary git
/// worktree failed and a merge was suspected first.)
///
/// Copying rather than moving: the caller still owns its directory and may
/// assert on it afterwards. Test outputs here are kilobytes, and one DuckDB
/// round trip is ~38 ms, so the copy is not the cost that matters.
fn stage_for_duckdb(dir: &Path) -> String {
    // ONE stable directory per THREAD, contents cleared — never a fresh
    // directory per call.
    //
    // The first version minted `stage_<pid>_<seq>` on every call: 193 new
    // directories in a single parallel run, on a gRPC-FUSE bind mount whose
    // documented failure mode (see `live_shared_workdir`) is that a container
    // which has resolved a path keeps the OLD inode after a recreate and sees
    // an empty directory forever. It showed up exactly as that comment
    // predicts — green alone, one failure in the parallel suite, reading zero
    // rows from a directory that was full.
    //
    // A per-thread name is stable across the thousands of calls one test binary
    // makes, so the container resolves each path once and keeps seeing it.
    let tid = format!("{:?}", std::thread::current().id());
    // PID + thread id: ThreadId is a PER-PROCESS counter, and the canonical
    // runner (`make test-live` = nextest) runs every test in its OWN process
    // with an identical startup sequence — so every concurrent test got the
    // SAME small N and they cleared/graded each other's staged files (r3
    // bughunt; the identical class rig::unique_name already fixed for oracle
    // labels). The per-thread inode-stability property the comment above
    // demands is preserved: within one process the label is still stable.
    let label = format!(
        "stage_p{}_t{}",
        std::process::id(),
        tid.chars()
            .filter(|c| c.is_ascii_digit())
            .collect::<String>()
    );
    let (host, container) = super::live_shared_workdir(&label);
    // Both output formats rivet ships. Staging only parquet made every TEXT
    // output unreadable through this seam — a CSV destination staged to an empty
    // directory and DuckDB failed with "no files match", which reads as a broken
    // test rather than a helper that cannot see the format under test.
    for ext in ["parquet", "csv"] {
        for f in files_with_extension(dir, ext) {
            let name = f.file_name().expect("output file has a name");
            std::fs::copy(&f, host.join(name))
                .unwrap_or_else(|e| panic!("stage {} for duckdb: {e}", f.display()));
        }
    }
    container
}

/// Stage only the parts the MANIFEST declares, for a reader inside the container.
///
/// The sibling of [`stage_for_duckdb`], which globs. A glob answers "what does the
/// destination hold"; every consumer — `rivet load`, `rivet validate`, reconcile —
/// reads what the run DECLARED, and a crashed run leaves parts no manifest names.
/// A row-level comparison against the source has to use the declared set or it
/// grades orphans as delivered.
fn stage_declared_for_duckdb(dir: &Path) -> String {
    let label = format!(
        "duckdb_declared_{}_{:?}",
        std::process::id(),
        std::thread::current().id()
    );
    let (host, container) = super::live_shared_workdir(&label);
    for f in std::fs::read_dir(&host).into_iter().flatten().flatten() {
        let _ = std::fs::remove_file(f.path());
    }
    for f in declared_parquet_parts(dir) {
        let name = f.file_name().expect("part has a name");
        std::fs::copy(&f, host.join(name))
            .unwrap_or_else(|e| panic!("stage declared {} for duckdb: {e}", f.display()));
    }
    container
}

/// Rows the two sides do not agree on, at TUPLE granularity, with the source
/// ATTACHed into the same DuckDB session.
///
/// Every source-vs-destination oracle in this repo before this one was a per-column
/// AGGREGATE — `live_differential`'s fingerprint is `count, sum(id), sum(n),
/// sum(big), sum(length(text)), count(DISTINCT id), count(DISTINCT text)`, and the
/// source-parity sweep compares `count/count(col)/count(distinct col)` plus sums.
/// Swapping the values of one column between two rows preserves EVERY one of those.
/// MEASURED on 1000 rows: all seven fingerprint fields identical, all four sweep
/// profiles identical, and a row-level `EXCEPT` finds the two rows in 4 ms.
///
/// So this closes a class nothing else here can see: row misassociation, a
/// cross-row shuffle, a swap between two columns of the same profile — and, in
/// practice more often, any type the aggregate fixtures do not carry (the
/// differential's fixture is `BIGINT, INT, BIGINT, TEXT`; no decimal, timestamp,
/// json, bytea or array is in it at all).
///
/// The reader shares no code with either side: DuckDB decodes the Parquet AND
/// pulls the source through its own postgres/mysql scanner. The `ATTACH` pattern is
/// already used one directory over — `dev/pytools/state_parity_duckdb.py` attaches
/// SQLite and Postgres in one session, for the same reason: a digest computed with
/// each engine's own string concatenation disagreed across engines whose data was
/// byte-identical.
///
/// Both sides are cast to VARCHAR by the CALLER's projection, deliberately: a
/// difference in RENDERING (1.10 vs 1.1) would otherwise read as a difference in
/// VALUE. Pass explicit casts for any type whose two renderings differ, and keep the
/// projections textually identical so a mismatch means the data differs.
///
/// `attach_type` is a CORE scanner (`postgres`, `mysql`, `sqlite`). SQL Server and
/// MongoDB are reachable too — this docstring claimed for a while that DuckDB "ships
/// no SQL Server or MongoDB one", which was wrong: both exist as COMMUNITY
/// extensions and simply publish no build below DuckDB 1.5.0 (measured: 404 for
/// every version through 1.4.1, 200 from 1.5.0). The stand's pin moved to that
/// floor. Reach them through [`OracleEngine`], which also carries the container-side
/// hostnames and the fact that Mongo has no `ATTACH` at all — its extension exposes
/// `mongo_scan(uri, db, collection)` instead.
pub struct RowDiff {
    /// In the SOURCE and not at the destination — a loss.
    pub missing: Vec<String>,
    /// At the destination and not in the source — a duplicate, a stale row, or a
    /// value written wrong (which shows up on BOTH sides, once each way).
    pub extra: Vec<String>,
}

impl RowDiff {
    pub fn is_empty(&self) -> bool {
        self.missing.is_empty() && self.extra.is_empty()
    }
}

pub fn duckdb_row_diff_vs_source(
    dir: &Path,
    attach_dsn: &str,
    attach_type: &str,
    source_projection: &str,
    dest_projection: &str,
) -> RowDiff {
    let staged = stage_declared_for_duckdb(dir);
    let sql = format!(
        "INSTALL {attach_type}; LOAD {attach_type}; \
         ATTACH '{attach_dsn}' AS src (TYPE {attach_type}, READ_ONLY); \
         CREATE OR REPLACE VIEW s AS {source_projection}; \
         CREATE OR REPLACE VIEW d AS SELECT * FROM ({dest_projection}); \
         SELECT 'missing' AS side, * FROM (SELECT * FROM s EXCEPT ALL SELECT * FROM d) \
         UNION ALL \
         SELECT 'extra', * FROM (SELECT * FROM d EXCEPT ALL SELECT * FROM s) \
         LIMIT 50"
    )
    .replace("__STAGED__", &staged);
    let v = super::duckdb_run_sql_json(&sql);
    let mut out = RowDiff {
        missing: Vec::new(),
        extra: Vec::new(),
    };
    for row in v["rows"].as_array().into_iter().flatten() {
        let cells: Vec<String> = row
            .as_array()
            .into_iter()
            .flatten()
            .skip(1)
            .map(|c| c.as_str().unwrap_or("<null>").to_string())
            .collect();
        let line = cells.join(" | ");
        match row[0].as_str().unwrap_or("") {
            "missing" => out.missing.push(line),
            _ => out.extra.push(line),
        }
    }
    out
}

/// Row count of every `.parquet` under `dir`, read by DuckDB.
///
/// The independent-decoder twin of [`total_parquet_rows`]. rivet writes Parquet
/// with the `parquet` crate and the old helper read it back with the SAME
/// crate, so a symmetric write/read fault is invisible by construction — the
/// class that let a whole column render as empty for four months. DuckDB shares
/// no code with the writer.
pub fn duckdb_total_parquet_rows(dir: &Path) -> usize {
    if files_with_extension(dir, "parquet").is_empty() {
        return 0; // an empty destination is a legitimate expected value
    }
    let c = stage_for_duckdb(dir);
    super::duckdb_parquet_rows(&c) as usize
}

/// One scalar aggregate over every parquet under `dir`, read by DuckDB.
///
/// `expr` is the SELECT list (e.g. `count(*) - count("uid")`) and `filter` an
/// optional WHERE (e.g. `__op = 'insert'` — a CDC part holds one row per CHANGE,
/// so only the insert images correspond to source rows).
///
/// The seam a per-column NULL/distinct profile needs. rivet's own decoder is the
/// thing under suspicion in that class — a lenient per-cell fallback that nulls
/// what it cannot parse — so the count has to come from a reader that shares no
/// code with the writer. Measured cost of not having one: 100% of a uuid column
/// rendered NULL through PostgreSQL CDC while every count and sum check passed,
/// found by a human eye on a real bucket.
pub fn duckdb_dir_scalar(dir: &Path, expr: &str, filter: Option<&str>) -> i64 {
    assert!(
        !files_with_extension(dir, "parquet").is_empty(),
        "duckdb_dir_scalar on a directory with no parquet — an empty destination \
         cannot answer a per-column question, and reading 0 as the answer is how \
         this class hides"
    );
    let c = stage_for_duckdb(dir);
    let where_ = filter.map(|f| format!(" WHERE {f}")).unwrap_or_default();
    let sql = format!("SELECT {expr} FROM read_parquet('{c}/**/*.parquet'){where_}");
    // `duckdb_run_sql_json` emits {columns:[..], rows:[[..]]} with every cell
    // stringified, so the scalar is rows[0][0] parsed — not an object field.
    let v = super::duckdb_run_sql_json(&sql);
    v["rows"]
        .get(0)
        .and_then(|r| r.get(0))
        .and_then(|x| x.as_str())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or_else(|| panic!("duckdb returned no scalar for `{sql}`: {v}"))
}

/// Every `id` value under `dir`, WITH multiplicity, read by DuckDB.
///
/// Multiplicity is the point: a completeness claim needs to tell LOSS (fewer
/// values) from DUPLICATION (same count, fewer distinct), and a set cannot.
pub fn duckdb_dir_parquet_ids(dir: &Path) -> Vec<i64> {
    duckdb_dir_parquet_i64(dir, "id")
}

/// The distinct `id` values under `dir`, read by DuckDB.
/// [`duckdb_dir_parquet_id_set`], but over the MANIFEST-DECLARED parts only —
/// the loader's view. A glob counts orphan parts no manifest names; on a crash
/// suite that difference IS the claim (harness audit: the gremlin final union
/// rode the glob and its own sibling file documents the glob as MASKING the
/// ack→manifest window loss).
pub fn duckdb_declared_dir_id_set(dir: &Path) -> BTreeSet<i64> {
    let staged = stage_declared_for_duckdb(dir);
    let sql = format!("SELECT DISTINCT id FROM read_parquet('{staged}/**/*.parquet') ORDER BY id");
    // `duckdb_run_sql_json` emits {columns:[..], rows:[[..]]} with every cell
    // STRINGIFIED (same shape `duckdb_dir_scalar` documents). The first cut of
    // this helper parsed the top level as an array-of-objects and
    // `unwrap_or_default()`ed the mismatch into an EMPTY SET — a completeness
    // oracle that silently answers "zero rows" on its own parse bug is the
    // absence-is-not-success class wearing a DuckDB badge (caught live,
    // 2026-08-29: arrow saw 30 ids, this saw 0). Parse the real shape; PANIC
    // on anything else.
    let v = super::duckdb_run_sql_json(&sql);
    let rows = v["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("duckdb returned no rows array for `{sql}`: {v}"));
    rows.iter()
        .map(|r| {
            r.get(0)
                .and_then(|x| x.as_str())
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or_else(|| panic!("unparseable id cell in `{sql}`: {r}"))
        })
        .collect()
}

/// A scalar over the MANIFEST-DECLARED parts only (the loader's view) — the
/// generic sibling of [`duckdb_declared_dir_id_set`] for tables whose key is
/// not an `id` int. Same why: a glob counts orphan parts of FAILED attempts
/// (a refused resume exports ranges before refusing at finalize), and those
/// are the `gc_orphans` case, not delivered data.
pub fn duckdb_declared_dir_scalar(dir: &Path, expr: &str) -> i64 {
    let staged = stage_declared_for_duckdb(dir);
    let sql = format!("SELECT {expr} FROM read_parquet('{staged}/**/*.parquet')");
    let v = super::duckdb_run_sql_json(&sql);
    v["rows"]
        .get(0)
        .and_then(|r| r.get(0))
        .and_then(|x| x.as_str())
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or_else(|| panic!("duckdb returned no scalar for `{sql}`: {v}"))
}

pub fn duckdb_dir_parquet_id_set(dir: &Path) -> BTreeSet<i64> {
    duckdb_dir_parquet_ids(dir).into_iter().collect()
}

/// Every value of an integer column under `dir`, in file/row order, by DuckDB.
pub fn duckdb_dir_parquet_i64(dir: &Path, col: &str) -> Vec<i64> {
    if files_with_extension(dir, "parquet").is_empty() {
        return Vec::new();
    }
    let c = stage_for_duckdb(dir);
    let v = super::duckdb_run_sql_json(&format!(
        "SELECT CAST({col} AS BIGINT) FROM read_parquet('{c}/**/*.parquet')"
    ));
    v["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("duckdb returned no rows array for {col}"))
        .iter()
        .map(|r| {
            r[0].as_str()
                .unwrap_or_else(|| panic!("{col} cell is not a string: {r:?}"))
                .parse::<i64>()
                .unwrap_or_else(|e| panic!("{col} does not parse as i64: {e}"))
        })
        .collect()
}

/// The distinct values of a text column under `dir`, read by DuckDB.
pub fn duckdb_dir_parquet_distinct_strings(dir: &Path, col: &str) -> BTreeSet<String> {
    if files_with_extension(dir, "parquet").is_empty() {
        return BTreeSet::new();
    }
    let c = stage_for_duckdb(dir);
    let v = super::duckdb_run_sql_json(&format!(
        "SELECT DISTINCT CAST({col} AS VARCHAR) FROM read_parquet('{c}/**/*.parquet') \
         WHERE {col} IS NOT NULL"
    ));
    v["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("duckdb returned no rows array for {col}"))
        .iter()
        .map(|r| r[0].as_str().unwrap_or_default().to_string())
        .collect()
}

/// Does any part under `dir` carry `col`? Answered from DuckDB's own schema
/// read, so a column rivet believes it wrote is confirmed by a foreign decoder.
pub fn duckdb_dir_parquet_has_column(dir: &Path, col: &str) -> bool {
    if files_with_extension(dir, "parquet").is_empty() {
        return false;
    }
    let c = stage_for_duckdb(dir);
    let described = super::duckdb_run_sql_json(&format!(
        "DESCRIBE SELECT * FROM read_parquet('{c}/**/*.parquet')"
    ));
    super::duckdb_parse_describe(&described).contains_key(col)
}

/// Row count of a single `.parquet` file.
pub fn parquet_rows(path: &Path) -> usize {
    let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    parquet_rows_from_bytes(bytes)
}

/// Row count of an in-memory Parquet body (e.g. an object downloaded from S3 /
/// GCS / Azure).
pub fn parquet_rows_from_bytes(bytes: Vec<u8>) -> usize {
    ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .expect("open parquet bytes")
        .build()
        .unwrap()
        .map(|b| b.unwrap().num_rows())
        .sum()
}

/// The `i64` `id` column values of a single `.parquet` file, WITH multiplicity
/// (so duplicates are observable). Accepts `Int64` (the canonical `BIGINT id`
/// seeders) or `Int32` (an `INT id` source column) — panics on anything else.
pub fn parquet_ids(path: &Path) -> Vec<i64> {
    use arrow::array::{Array, AsArray};
    let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .unwrap_or_else(|e| panic!("open {}: {e}", path.display()))
        .build()
        .unwrap();
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let col = batch.column_by_name("id").expect("id column present");
        match col.data_type() {
            arrow::datatypes::DataType::Int64 => {
                let a = col.as_primitive::<arrow::datatypes::Int64Type>();
                for i in 0..a.len() {
                    if !a.is_null(i) {
                        out.push(a.value(i));
                    }
                }
            }
            arrow::datatypes::DataType::Int32 => {
                let a = col.as_primitive::<arrow::datatypes::Int32Type>();
                for i in 0..a.len() {
                    if !a.is_null(i) {
                        out.push(i64::from(a.value(i)));
                    }
                }
            }
            other => panic!("id column: expected Int32/Int64, got {other:?}"),
        }
    }
    out
}

// ── Dir-level convenience (the two shapes most tests want) ───────────────────

/// Total rows across every `.parquet` directly under `dir`.
pub fn total_parquet_rows(dir: &Path) -> usize {
    files_with_extension(dir, "parquet")
        .iter()
        .map(|p| parquet_rows(p))
        .sum()
}

/// All `id` values across every `.parquet` under `dir`, WITH multiplicity.
pub fn dir_parquet_ids(dir: &Path) -> Vec<i64> {
    files_with_extension(dir, "parquet")
        .iter()
        .flat_map(|p| parquet_ids(p))
        .collect()
}

/// Distinct `id` set across every `.parquet` under `dir`.
pub fn dir_parquet_id_set(dir: &Path) -> BTreeSet<i64> {
    dir_parquet_ids(dir).into_iter().collect()
}

// ── CDC `__seq` conformance helpers (used by every engine's live CDC test) ───

fn reader(path: &Path) -> parquet::arrow::arrow_reader::ParquetRecordBatchReader {
    let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .unwrap_or_else(|e| panic!("open {}: {e}", path.display()))
        .build()
        .unwrap()
}

/// The `i64` values of column `col` across every `.parquet` under `dir`, in row
/// order (e.g. `__seq`, or a `BIGINT` data column).
pub fn dir_parquet_i64(dir: &Path, col: &str) -> Vec<i64> {
    use arrow::array::{Array, AsArray};
    let mut out = Vec::new();
    for path in files_with_extension(dir, "parquet") {
        for batch in reader(&path) {
            let batch = batch.unwrap();
            let c = batch
                .column_by_name(col)
                .unwrap_or_else(|| panic!("column {col} present"));
            let a = c
                .as_primitive_opt::<arrow::datatypes::Int64Type>()
                .unwrap_or_else(|| panic!("{col} decodes as Int64"));
            (0..a.len()).for_each(|i| out.push(a.value(i)));
        }
    }
    out
}

/// Distinct string values of `col` across every `.parquet` under `dir`.
pub fn dir_parquet_distinct_strings(dir: &Path, col: &str) -> BTreeSet<String> {
    use arrow::array::{Array, AsArray};
    let mut out = BTreeSet::new();
    for path in files_with_extension(dir, "parquet") {
        for batch in reader(&path) {
            let batch = batch.unwrap();
            let c = batch
                .column_by_name(col)
                .unwrap_or_else(|| panic!("column {col} present"));
            let a = c
                .as_string_opt::<i32>()
                .unwrap_or_else(|| panic!("{col} is Utf8"));
            (0..a.len()).for_each(|i| {
                out.insert(a.value(i).to_string());
            });
        }
    }
    out
}

/// Sum `row_count` across the run-unique manifest COPIES (`manifest-<run>.json`)
/// in `dir` — the dest sidecars a cross-run consumer (the Pro loader's reconcile,
/// a warehouse autoloader) reads. Unlike a parquet re-read, this goes **RED when
/// a run's manifest CLOBBERS**: the payload survives (run-unique parts) but its
/// manifest entry is lost, so pre-fix a shared prefix held ONE clobbered
/// `manifest.json` and the sum was the LAST run's rows, not every run's. This is
/// the oracle a run-uniqueness / accumulation claim must use — the `file_log`
/// state-DB ledger accumulates regardless of the sidecar clobber, and a parquet
/// re-read cannot see it. Ignores the canonical `manifest.json` pointer (it
/// duplicates the latest copy; counting it would double the latest run).
pub fn dir_manifest_copy_total_rows(dir: &Path) -> i64 {
    let mut total = 0;
    for path in files_with_extension(dir, "json") {
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        if !(name.starts_with("manifest-") && name.ends_with(".json")) {
            continue; // the bare `manifest.json` pointer, or a foreign file
        }
        let bytes = std::fs::read(&path).expect("read manifest copy");
        let v: serde_json::Value =
            serde_json::from_slice(&bytes).expect("parse manifest copy JSON");
        total += v
            .get("row_count")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
    }
    total
}

/// The set of `id` values DECLARED by the run-unique manifest copies under `dir`
/// — the orphan-immune twin of [`duckdb_dir_parquet_id_set`]. A raw-disk id-set
/// counts every `.parquet` present, INCLUDING crash orphans a manifest never
/// referenced; this reads only the parts the manifest copies actually declare, so
/// it answers "what would `rivet load` (manifest-authoritative) deliver". The
/// oracle a split-RESUME completeness claim needs: a re-sampled resume that shifts
/// the partition drops the crashed unit's ORIGINAL key range from the manifest
/// while the raw disk still shows the orphaned pre-crash parts (false-clean).
///
/// Reads by BASENAME to match [`stage_for_duckdb`], which flattens all parquet
/// into one staging dir by file name; part names are run-unique so basenames do
/// not collide across units.
pub fn dir_manifest_copy_id_set(dir: &Path) -> BTreeSet<i64> {
    let mut declared: BTreeSet<String> = BTreeSet::new();
    for path in files_with_extension(dir, "json") {
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        if !(name.starts_with("manifest-") && name.ends_with(".json")) {
            continue; // the bare `manifest.json` pointer, or a foreign file
        }
        let bytes = std::fs::read(&path).expect("read manifest copy");
        let v: serde_json::Value =
            serde_json::from_slice(&bytes).expect("parse manifest copy JSON");
        for part in v
            .get("parts")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
        {
            if let Some(p) = part.get("path").and_then(serde_json::Value::as_str) {
                let base = p.rsplit('/').next().unwrap_or(p);
                declared.insert(base.to_string());
            }
        }
    }
    if declared.is_empty() {
        return BTreeSet::new();
    }
    let c = stage_for_duckdb(dir);
    let list = declared
        .iter()
        .map(|b| format!("'{c}/{b}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let v = super::duckdb_run_sql_json(&format!(
        "SELECT DISTINCT CAST(id AS BIGINT) FROM read_parquet([{list}])"
    ));
    v["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("duckdb returned no rows array for manifest id-set"))
        .iter()
        .map(|r| {
            r[0].as_str()
                .expect("id cell is not a string")
                .parse::<i64>()
                .expect("id does not parse as i64")
        })
        .collect()
}

/// Count the DISTINCT run-unique manifest copies (`manifest-<run_id>.json`) in
/// `dir` — one per run_id. Two runs into the same prefix that each rotate their
/// run_id leave two files; a run that reuses (freezes) a prior run_id overwrites
/// the same filename, leaving one. The oracle for "the run_id rotates across
/// repeated runs" (a frozen run_id collapses the sidecars and a run_id-deduping
/// loader then skips later deltas). Ignores the canonical `manifest.json` pointer.
pub fn dir_manifest_copy_count(dir: &Path) -> usize {
    files_with_extension(dir, "json")
        .into_iter()
        .filter(|p| {
            let name = p.file_name().unwrap_or_default().to_string_lossy();
            name.starts_with("manifest-") && name.ends_with(".json")
        })
        .count()
}

/// True if any `.parquet` under `dir` carries a column named `col`.
pub fn dir_parquet_has_column(dir: &Path, col: &str) -> bool {
    files_with_extension(dir, "parquet").iter().any(|p| {
        let bytes = std::fs::read(p).unwrap();
        ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .unwrap()
            .schema()
            .fields()
            .iter()
            .any(|f| f.name() == col)
    })
}

/// Assert the intra-transaction `__seq` conformance case: `out` holds exactly
/// `n` updates of ONE PK done in a SINGLE transaction, so they share one
/// `__pos`, `__seq` is the distinct order `0..n-1`, and the greatest-`__seq`
/// change carries the committed value (`counter = n`). An `ORDER BY (__pos,
/// __seq)` dedup therefore returns the last write, not an arbitrary row. Shared
/// so every engine's CDC test asserts the fix identically.
pub fn assert_intra_transaction_seq(out: &Path, n: i64) {
    assert!(
        dir_parquet_has_column(out, "__seq"),
        "CDC output must carry the __seq column"
    );
    let seqs = dir_parquet_i64(out, "__seq");
    let counters = dir_parquet_i64(out, "counter");
    assert_eq!(seqs.len() as i64, n, "all {n} updates must be captured");
    assert_eq!(
        dir_parquet_distinct_strings(out, "__pos").len(),
        1,
        "all changes of one transaction share a single __pos — the tie __seq breaks"
    );
    let mut sorted = seqs.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len() as i64, n, "__seq must be distinct per change");
    assert_eq!(
        (*sorted.first().unwrap(), *sorted.last().unwrap()),
        (0, n - 1)
    );
    let latest = seqs
        .iter()
        .enumerate()
        .max_by_key(|(_, s)| **s)
        .map(|(i, _)| counters[i])
        .unwrap();
    assert_eq!(latest, n, "the max-__seq change must carry counter = n");
}

// ── CDC current-state SUM reconciliation (source == deduped target) ──────────
//
// The strong end-to-end oracle: apply a workload of many transactions (some
// updating one PK several times in ONE transaction) and assert the source's
// final `SUM(v)` equals the target's, where the target is deduped STRICTLY by
// `(__pos, __seq)` — with row order discarded, exactly as an unordered warehouse
// table forces. Fails without `__seq` (the intra-transaction `__pos` tie makes
// the dedup pick an arbitrary version, skewing the sum); passes with it.

/// A captured change: the columns the reconciliation needs (`id BIGINT, v
/// BIGINT` + the CDC meta). `v` on a delete is the (irrelevant) before-image.
pub struct CdcChange {
    pub id: i64,
    pub v: i64,
    pub op: String,
    pub pos: String,
    pub seq: i64,
}

/// Source engine — selects how `__pos` parses into a monotonic position.
#[derive(Clone, Copy)]
pub enum CdcEngine {
    MySql,
    Postgres,
    SqlServer,
}

/// The parts the manifest(s) under `dir` DECLARE as committed — what a consumer
/// (`rivet load`, `rivet validate`, reconcile) will actually read.
///
/// The union across the immutable `manifest-<run_id>.json` copies, falling back to
/// the canonical `manifest.json` only when no copy exists — the sink writes copies
/// for exactly the repeated-run case where the canonical one is last-writer-wins.
///
/// This is NOT the same set as every `.parquet` under `dir`, and the difference is
/// the point: a crashed run leaves durable parts no manifest names, so a directory
/// scan reports rows that no consumer will ever see. The CDC evidence audit graded
/// three of four engines `glob only` on that read-back; this is what closes it.
///
/// A part listed with a non-`committed` status is skipped — in-flight is not
/// delivered.
pub fn declared_parquet_parts(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut copies: Vec<std::path::PathBuf> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.starts_with("manifest-") && n.ends_with(".json"))
                })
                .collect()
        })
        .unwrap_or_default();
    copies.sort();
    if copies.is_empty() && dir.join("manifest.json").is_file() {
        copies.push(dir.join("manifest.json"));
    }
    let mut declared = Vec::new();
    for m in &copies {
        let Ok(text) = std::fs::read_to_string(m) else {
            continue;
        };
        let Ok(doc) = serde_json::from_str::<serde_json::Value>(&text) else {
            continue;
        };
        // SUCCESS manifests only — the loader's rule (a part in a Failed/
        // Interrupted manifest is a gc DELETE candidate, not delivered data).
        // Live-proven 2026-08-29: a crashed keyset run leaves a `failed`
        // manifest copy whose part NAMES a refused resume then re-creates
        // under the same run stamp, so without this filter the union counted
        // 500 undelivered rows as declared (2500 vs the true 2000).
        let ok = doc
            .get("status")
            .and_then(|s| s.as_str())
            .is_none_or(|s| s.eq_ignore_ascii_case("success"));
        if !ok {
            continue;
        }
        for part in doc
            .get("parts")
            .and_then(|p| p.as_array())
            .map(Vec::as_slice)
            .unwrap_or(&[])
        {
            let committed = part
                .get("status")
                .and_then(|s| s.as_str())
                .is_none_or(|s| s == "committed");
            if !committed {
                continue;
            }
            if let Some(name) = part.get("path").and_then(|p| p.as_str()) {
                let cand = dir.join(name);
                if cand.is_file() {
                    declared.push(cand);
                }
            }
        }
    }
    declared.sort();
    declared.dedup();
    declared
}

/// Read every CDC change (`id, v, __op, __pos, __seq`) from the `.parquet`
/// parts under `dir`.
pub fn read_cdc_changes(dir: &Path) -> Vec<CdcChange> {
    use arrow::array::{Array, AsArray};
    let mut out = Vec::new();
    // DECLARED, not globbed. A crashed run leaves durable parts no manifest names,
    // and every consumer reads what the run declared — so a directory scan here
    // reports rows nobody will ever see, which is exactly the defect the crash and
    // resume cells using this reader exist to catch. See `declared_parquet_parts`.
    for path in declared_parquet_parts(dir) {
        for batch in reader(&path) {
            let b = batch.unwrap();
            // Int32 OR Int64 → i64: an `INT` source column lands as Int32 in
            // the CDC parquet; the original Int64-only downcast panicked on it.
            let col_i64 = |name: &str| -> Vec<i64> {
                let col = b
                    .column_by_name(name)
                    .unwrap_or_else(|| panic!("{name} column present"));
                match col.data_type() {
                    arrow::datatypes::DataType::Int64 => col
                        .as_primitive::<arrow::datatypes::Int64Type>()
                        .values()
                        .to_vec(),
                    arrow::datatypes::DataType::Int32 => col
                        .as_primitive::<arrow::datatypes::Int32Type>()
                        .values()
                        .iter()
                        .map(|v| i64::from(*v))
                        .collect(),
                    other => panic!("{name}: expected Int32/Int64, got {other:?}"),
                }
            };
            let col_str = |name: &str| {
                b.column_by_name(name)
                    .unwrap_or_else(|| panic!("{name} column present"))
                    .as_string::<i32>()
                    .clone()
            };
            let (id, v, seq) = (col_i64("id"), col_i64("v"), col_i64("__seq"));
            let (op, pos) = (col_str("__op"), col_str("__pos"));
            for r in 0..b.num_rows() {
                out.push(CdcChange {
                    id: id[r],
                    v: v[r],
                    op: op.value(r).to_string(),
                    pos: pos.value(r).to_string(),
                    seq: seq[r],
                });
            }
        }
    }
    out
}

/// Sorted `(id, op)` pairs from the CDC parquet under `dir` — the independent
/// re-read a capture-count assert needs. `manifest_rows(out) == 2` is rivet's
/// OWN summary and cannot tell "the right 2 changes" from "the wrong 2"; this
/// reads the payload itself (matrix audit: self-oracle class).
pub fn cdc_id_ops(dir: &Path) -> Vec<(i64, String)> {
    let mut v: Vec<(i64, String)> = read_cdc_changes(dir)
        .into_iter()
        .map(|c| (c.id, c.op))
        .collect();
    v.sort();
    v
}

/// `__pos` → a single monotonic `u128`, per engine (`__seq` is the lower-order
/// tiebreak, applied separately). MySQL `{file,pos}`; PostgreSQL `{lsn:"hi/lo"}`
/// (hex halves); SQL Server `{lsn:"<fixed-width hex>"}`.
fn pos_u128(engine: CdcEngine, pos: &str) -> u128 {
    let j: serde_json::Value =
        serde_json::from_str(pos).unwrap_or_else(|_| panic!("__pos is not JSON: {pos}"));
    match engine {
        CdcEngine::MySql => {
            let file = j["file"].as_str().expect("__pos.file");
            let file_num: u128 = file
                .rsplit('.')
                .next()
                .and_then(|s| s.parse().ok())
                .expect("binlog file number");
            let p = j["pos"].as_u64().expect("__pos.pos") as u128;
            (file_num << 64) | p
        }
        CdcEngine::Postgres => {
            let lsn = j["lsn"].as_str().expect("__pos.lsn");
            let (hi, lo) = lsn.split_once('/').expect("lsn hi/lo");
            let hi = u128::from_str_radix(hi, 16).expect("lsn hi hex");
            let lo = u128::from_str_radix(lo, 16).expect("lsn lo hex");
            (hi << 32) | lo
        }
        CdcEngine::SqlServer => {
            let lsn = j["lsn"].as_str().expect("__pos.lsn");
            u128::from_str_radix(lsn, 16).expect("lsn hex")
        }
    }
}

/// Dedup the change log to current state by `(__pos, __seq)` — row order
/// DISCARDED (the log is scrambled first, as an unordered warehouse table
/// leaves it) — and `SUM(v)` over the surviving (non-deleted) rows.
pub fn deduped_current_sum(mut changes: Vec<CdcChange>, engine: CdcEngine) -> i64 {
    // Deterministic scramble: only (__pos, __seq) may decide the latest row.
    use std::hash::{Hash, Hasher};
    changes.sort_by_key(|c| {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        (c.id, c.seq, &c.pos).hash(&mut h);
        h.finish()
    });
    let mut best: HashMap<i64, ((u128, i64), i64, bool)> = HashMap::new();
    for c in changes {
        let key = (pos_u128(engine, &c.pos), c.seq);
        let is_delete = c.op == "delete";
        match best.get(&c.id) {
            Some((k, _, _)) if *k >= key => {}
            _ => {
                best.insert(c.id, (key, c.v, is_delete));
            }
        }
    }
    best.values()
        .filter(|(_, _, del)| !del)
        .map(|(_, v, _)| *v)
        .sum()
}

/// Count of `(transaction, PK)` pairs touched more than once — i.e. a PK updated
/// several times in one transaction. A reconciliation workload MUST produce some,
/// else the intra-transaction case is untested and the sum passes vacuously.
pub fn intra_txn_multi_change_count(changes: &[CdcChange]) -> usize {
    let mut per: HashMap<(&str, i64), usize> = HashMap::new();
    for c in changes {
        *per.entry((c.pos.as_str(), c.id)).or_default() += 1;
    }
    per.values().filter(|&&n| n > 1).count()
}

/// A deterministic reconciliation workload as transactions of engine-agnostic
/// SQL statements (`id BIGINT, v BIGINT`). EVERY transaction updates one PK 2–4
/// times (`v = v + delta`), so the committed `v` differs from every intermediate
/// version — a dedup that ordered by `__pos` alone would skew `SUM(v)`. Mixed
/// with fresh inserts and deletes. The caller wraps each inner `Vec` in its
/// engine's `BEGIN`/`COMMIT`.
pub fn cdc_sum_workload(tbl: &str) -> Vec<Vec<String>> {
    const K: i64 = 25; // shared key range 1..=K
    const M: i64 = 200; // transactions
    let mut txns: Vec<Vec<String>> = Vec::new();
    // Seed the shared keys with v = 0.
    txns.push(
        (1..=K)
            .map(|k| format!("INSERT INTO {tbl} (id, v) VALUES ({k}, 0)"))
            .collect(),
    );
    let mut rng: u64 = 0x1234_5678_9abc_def0;
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let mut extra_key = K;
    for t in 0..M {
        let mut stmts = Vec::new();
        let key = (next() % K as u64) as i64 + 1;
        let r = 2 + next() % 3; // 2..=4 updates to the SAME key in this txn
        for _ in 0..r {
            let delta = (next() % 100) as i64 + 1;
            stmts.push(format!("UPDATE {tbl} SET v = v + {delta} WHERE id = {key}"));
        }
        if t % 4 == 0 {
            extra_key += 1;
            let val = (next() % 500) as i64;
            stmts.push(format!(
                "INSERT INTO {tbl} (id, v) VALUES ({extra_key}, {val})"
            ));
        }
        if t % 5 == 0 {
            let dk = (next() % K as u64) as i64 + 1;
            stmts.push(format!("DELETE FROM {tbl} WHERE id = {dk}"));
        }
        txns.push(stmts);
    }
    txns
}

// ── Mongo CDC dedup oracle (distinct __pos per event; __seq unused) ──────────
//
// Unlike the SQL engines, a MongoDB change stream gives every event a DISTINCT,
// order-preserving resume token (`__pos`) — even within ONE transaction — so the
// total order is `__pos` ALONE (`__seq` is always 0). The `document` blob is the
// image; the dedup keeps the last per `_id` by `__pos` and reads `field` from it.

pub struct MongoCdcChange {
    pub id: String,
    pub document: String,
    pub op: String,
    pub pos: String,
    pub seq: i64,
}

/// Read every Mongo CDC change (`_id, document, __op, __pos, __seq`) from the
/// `.parquet` parts under `dir`.
pub fn read_mongo_cdc_changes(dir: &Path) -> Vec<MongoCdcChange> {
    use arrow::array::{Array, AsArray};
    let mut out = Vec::new();
    // DECLARED, not globbed — same reason as `read_cdc_changes`: this is Mongo's
    // read-back on every crash and resume cell, and a crashed run's orphan parts
    // are exactly what a directory scan would count as delivered.
    for path in declared_parquet_parts(dir) {
        for batch in reader(&path) {
            let b = batch.unwrap();
            let s = |n: &str| {
                b.column_by_name(n)
                    .unwrap_or_else(|| panic!("{n} column present"))
                    .as_string::<i32>()
                    .clone()
            };
            let (id, document, op, pos) = (s("_id"), s("document"), s("__op"), s("__pos"));
            let seq = b
                .column_by_name("__seq")
                .expect("__seq column present")
                .as_primitive::<arrow::datatypes::Int64Type>()
                .clone();
            for r in 0..b.num_rows() {
                out.push(MongoCdcChange {
                    id: id.value(r).to_string(),
                    document: if document.is_null(r) {
                        String::new()
                    } else {
                        document.value(r).to_string()
                    },
                    op: op.value(r).to_string(),
                    pos: pos.value(r).to_string(),
                    seq: seq.value(r),
                });
            }
        }
    }
    out
}

/// Dedup the Mongo CDC log to the current `_id → document.field` state, ordering
/// STRICTLY by `(__pos, __seq)` (row order discarded, as an unordered warehouse
/// table forces) with deletes removed. Integer `_id` only. The oracle a captured
/// change log must reproduce exactly.
pub fn mongo_deduped_field(
    mut changes: Vec<MongoCdcChange>,
    field: &str,
) -> std::collections::BTreeMap<i64, String> {
    changes.sort_by(|a, b| (&a.pos, a.seq).cmp(&(&b.pos, b.seq)));
    let mut state: std::collections::BTreeMap<i64, String> = std::collections::BTreeMap::new();
    for c in &changes {
        let id: i64 = match c.id.parse() {
            Ok(i) => i,
            Err(_) => continue,
        };
        if c.op == "delete" {
            state.remove(&id);
            continue;
        }
        let doc: serde_json::Value =
            serde_json::from_str(&c.document).unwrap_or(serde_json::Value::Null);
        if let Some(v) = doc.get(field) {
            let vs = match v {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            state.insert(id, vs);
        }
    }
    state
}

/// Every `id` under `dir` read from CSV by DuckDB — the CSV twin of
/// [`duckdb_dir_parquet_id_set`], for the paths that write text.
///
/// A separate reader on purpose: `read_parquet` cannot open a `.csv`, so a test
/// that meant to grade the CSV writer and used the parquet helper would panic on
/// "no parquet here" rather than quietly grade nothing — but it also could not
/// grade anything. This is the reader that can.
pub fn duckdb_dir_csv_id_set(dir: &Path) -> BTreeSet<i64> {
    assert!(
        !files_with_extension(dir, "csv").is_empty(),
        "duckdb_dir_csv_id_set on a directory with no .csv — reading an empty set as \
         the answer is how a wrong-format wiring hides"
    );
    let c = stage_for_duckdb(dir);
    let sql = format!("SELECT id FROM read_csv_auto('{c}/**/*.csv', header=true)");
    let v = super::duckdb_run_sql_json(&sql);
    v["rows"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .filter_map(|r| r.get(0)?.as_str()?.parse::<i64>().ok())
                .collect()
        })
        .unwrap_or_default()
}
