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
