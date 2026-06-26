//! Schema-based memory estimation.
//!
//! Pure functions that convert an Arrow schema into:
//! - a per-row byte estimate (`estimate_row_bytes`)
//! - a `batch_size` count from a target memory budget in MB (`compute_batch_size_from_memory`)
//!
//! No DB connection required; used during plan resolution and as a fall-back
//! when a fetch loop hasn't observed real row sizes yet.

use arrow::datatypes::{DataType, SchemaRef};

/// Estimate average row size in bytes from an Arrow schema.
pub fn estimate_row_bytes(schema: &SchemaRef) -> usize {
    const STRING_ESTIMATE: usize = 256;
    let mut total: usize = 0;
    for field in schema.fields() {
        total += match field.data_type() {
            DataType::Boolean | DataType::Int8 | DataType::UInt8 => 1,
            DataType::Int16 | DataType::UInt16 => 2,
            DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Date32 => 4,
            DataType::Int64
            | DataType::UInt64
            | DataType::Float64
            | DataType::Date64
            | DataType::Timestamp(_, _)
            | DataType::Time64(_)
            | DataType::Duration(_) => 8,
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => 16,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
                STRING_ESTIMATE
            }
            _ => 64,
        };
        total += 1; // validity bitmap overhead (rounded up)
    }
    total.max(1)
}

/// Compute batch_size from a memory target in MB and estimated row size.
///
/// The 150k upper clamp bounds the *raw-row accumulator* an engine holds
/// alongside the Arrow batch (mysql/tiberius `Vec<Row>`): for narrow rows that
/// raw buffer is several× the compact Arrow form, so it — not the MB target —
/// drives peak RSS. 150k still gives ~15× fewer pipeline flushes than the old
/// static 10k (most of the throughput win) at a fraction of the peak RSS that
/// a 500k cap incurred on narrow tables.
pub fn compute_batch_size_from_memory(memory_mb: usize, schema: &SchemaRef) -> usize {
    let row_bytes = estimate_row_bytes(schema);
    let target = memory_mb * 1024 * 1024 / row_bytes;
    target.clamp(1_000, 150_000)
}

/// Default RSS budget (MB) the scaffold sizes `parallel:` against, and the
/// threshold `preflight::check` uses to decide whether an unindexed large-table
/// scan is UNSAFE (over budget) or merely DEGRADED (fits) — one budget, shared by
/// the scaffold and the check.
pub(crate) const DEFAULT_MEM_BUDGET_MB: u64 = 2048;

/// Per-worker peak RSS (MB) under the default *adaptive* batching, fitted to the
/// sweep in `docs/bench/reports/REPORT_full_vs_parallel.md`. Anchored on measured
/// points — ~19 MB/worker at ~40 B/row (narrow), ~105 MB at ~4 KB/row (wide) —
/// and clamped to a ceiling of ≈ 2× the adaptive batch target. The driver is
/// **row width × in-flight batch, not chunk_size** (chunk_size only sets file
/// count). An explicit large `tuning.batch_size` overrides adaptive batching and
/// raises this beyond the model.
///
/// This is the *catalog-width* estimate (pre-schema `avg_row_bytes`), shared by
/// `init` (scaffold) and `preflight::check`. Its schema-resolved sibling is
/// [`compute_batch_size_from_memory`] above; the ceiling is single-sourced from
/// [`crate::source::batch_controller::DEFAULT_BATCH_TARGET_MB`] so the two can't
/// silently drift on the batch target. The slope/floor remain empirical (the
/// bench sweep, not derivable from the schema-bytes model).
pub(crate) fn per_worker_rss_mb(avg_row_bytes: i64) -> u64 {
    const FLOOR_MB: u64 = 18;
    // ~2× the adaptive batch target (Arrow builders + parquet row-group + zstd
    // hold roughly twice the raw in-flight batch). Linked to the source of truth
    // so it tracks the batch target instead of drifting.
    const CEIL_MB: u64 = 2 * crate::source::batch_controller::DEFAULT_BATCH_TARGET_MB as u64;
    let b = avg_row_bytes.max(0) as u64;
    (FLOOR_MB + b * 87 / 4096).clamp(FLOOR_MB, CEIL_MB)
}

/// Predicted peak process RSS (MB) for a chunked export with `parallel` workers.
/// `peak ≈ 16 (process base) + parallel × per_worker_rss_mb(width)`. Linear in
/// `parallel`; slightly *over*-estimates past ~4 workers (allocator reuse) — the
/// safe direction for a budget. Validated against the sweep (par 4 wide: est 436
/// vs measured 444 MB; par 8 narrow: est 166 vs 169 MB).
pub(crate) fn estimate_peak_rss_mb(parallel: usize, avg_row_bytes: i64) -> u64 {
    const PROCESS_BASE_MB: u64 = 16;
    PROCESS_BASE_MB + parallel as u64 * per_worker_rss_mb(avg_row_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn estimate_row_bytes_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let est = estimate_row_bytes(&schema);
        // Int64=8+1, Utf8=256+1 = 266
        assert_eq!(est, 266);
    }

    #[test]
    fn compute_batch_size_clamped() {
        // 1 tiny column -> huge batch, clamped to 150_000
        let schema = Arc::new(Schema::new(vec![Field::new(
            "flag",
            DataType::Boolean,
            false,
        )]));
        assert_eq!(compute_batch_size_from_memory(256, &schema), 150_000);

        // 100 large string columns -> small batch, clamped to 1_000
        let fields: Vec<Field> = (0..100)
            .map(|i| Field::new(format!("c{i}"), DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        assert_eq!(compute_batch_size_from_memory(1, &schema), 1_000);
    }
}
