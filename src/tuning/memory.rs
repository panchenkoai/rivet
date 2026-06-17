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
