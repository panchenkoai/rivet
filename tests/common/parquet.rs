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

use std::collections::BTreeSet;
use std::path::Path;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::files_with_extension;

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
/// (so duplicates are observable). Panics if there is no `id` column or it does
/// not decode as `Int64` — the canonical seeders all use a `BIGINT id`.
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
        let a = col
            .as_primitive_opt::<arrow::datatypes::Int64Type>()
            .expect("id column decodes as Int64");
        for i in 0..a.len() {
            if !a.is_null(i) {
                out.push(a.value(i));
            }
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
