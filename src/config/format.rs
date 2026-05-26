//! Output format & compression: parquet/csv selector, codecs, parquet tuning.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FormatType {
    Parquet,
    Csv,
}

impl FormatType {
    /// Stable lowercase string label for persistence and display.
    /// Prefer this over `format!("{:?}", self).to_lowercase()` — `Debug` output
    /// is not a stable format contract.
    pub fn label(self) -> &'static str {
        match self {
            FormatType::Parquet => "parquet",
            FormatType::Csv => "csv",
        }
    }
}

#[derive(Debug, Default, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CompressionType {
    #[default]
    Zstd,
    Snappy,
    Gzip,
    Lz4,
    None,
}

impl CompressionType {
    /// Stable lowercase string label for persistence and display.
    pub fn label(self) -> &'static str {
        match self {
            CompressionType::Zstd => "zstd",
            CompressionType::Snappy => "snappy",
            CompressionType::Gzip => "gzip",
            CompressionType::Lz4 => "lz4",
            CompressionType::None => "none",
        }
    }
}

/// Parquet row group tuning strategy.
///
/// Controls how many rows Rivet places in each Parquet row group. Row group size
/// affects memory usage during write, compression ratio, and downstream read
/// performance (predicate pushdown, column skipping).
///
/// ```yaml
/// exports:
///   - name: events
///     parquet:
///       row_group_strategy: auto          # compute from schema + target_row_group_mb
///       target_row_group_mb: 128          # default target; auto + fixed_memory only
///       max_row_group_mb: 256             # optional upper bound (all strategies)
///       # row_group_strategy: fixed_rows  # exact row count
///       # row_group_rows: 500000          # used with fixed_rows
///       # row_group_strategy: fixed_memory  # same math as auto, made explicit
/// ```
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RowGroupStrategy {
    /// Compute rows-per-group from schema column types and `target_row_group_mb`.
    /// For narrow tables this produces large groups (efficient). For wide tables
    /// it reduces group size to stay within the memory target.
    #[default]
    Auto,
    /// Use `row_group_rows` as a literal row count. Ignores memory targets.
    FixedRows,
    /// Identical math to `auto`, but the strategy label is explicit in logs.
    FixedMemory,
}

/// Parquet-specific tuning for row group sizing.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct ParquetConfig {
    /// How to determine the row group size. Default: `auto`.
    pub row_group_strategy: Option<RowGroupStrategy>,
    /// Exact number of rows per group (`fixed_rows` only).
    pub row_group_rows: Option<usize>,
    /// Target Arrow buffer memory per row group in MB (`auto` and `fixed_memory`). Default: 128.
    pub target_row_group_mb: Option<usize>,
    /// Hard upper bound on row group memory in MB. When set, further reduces computed row count.
    pub max_row_group_mb: Option<usize>,
}

impl ParquetConfig {
    pub const DEFAULT_TARGET_ROW_GROUP_MB: usize = 128;

    /// Compute the effective rows-per-group from schema column types.
    ///
    /// Returns `None` for `fixed_rows` when `row_group_rows` is not set (caller
    /// falls back to the parquet library default of 1,048,576 rows).
    pub fn effective_row_group_rows(&self, schema: &arrow::datatypes::SchemaRef) -> Option<usize> {
        let strategy = self.row_group_strategy.unwrap_or_default();
        match strategy {
            RowGroupStrategy::FixedRows => self.row_group_rows,
            RowGroupStrategy::Auto | RowGroupStrategy::FixedMemory => {
                let target_mb = self
                    .target_row_group_mb
                    .unwrap_or(Self::DEFAULT_TARGET_ROW_GROUP_MB);
                let row_bytes = crate::tuning::estimate_row_bytes(schema).max(1);
                let rows = (target_mb * 1024 * 1024) / row_bytes;
                // Clamp to a safe range: at least 1 000 rows, at most 10 M rows.
                let rows = rows.clamp(1_000, 10_000_000);
                // Apply optional max_row_group_mb cap.
                let rows = if let Some(max_mb) = self.max_row_group_mb {
                    let max_rows = ((max_mb * 1024 * 1024) / row_bytes).max(1_000);
                    rows.min(max_rows)
                } else {
                    rows
                };
                Some(rows)
            }
        }
    }
}

/// High-level compression preset. Maps to a `(CompressionType, level)` pair.
///
/// ```yaml
/// exports:
///   - name: events
///     compression_profile: fast   # snappy — fastest, larger files
///     # compression_profile: balanced  # zstd level 3 — default for production
///     # compression_profile: compact   # zstd level 9 — smallest files, more CPU
///     # compression_profile: none      # no compression
/// ```
///
/// When set, takes precedence over `compression` and `compression_level`.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CompressionProfile {
    None,
    Fast,
    Balanced,
    Compact,
}

impl CompressionProfile {
    #[allow(dead_code)]
    pub fn label(self) -> &'static str {
        match self {
            CompressionProfile::None => "none",
            CompressionProfile::Fast => "fast",
            CompressionProfile::Balanced => "balanced",
            CompressionProfile::Compact => "compact",
        }
    }

    pub fn to_codec(self) -> (CompressionType, Option<u32>) {
        match self {
            CompressionProfile::None => (CompressionType::None, None),
            CompressionProfile::Fast => (CompressionType::Snappy, None),
            CompressionProfile::Balanced => (CompressionType::Zstd, Some(3)),
            CompressionProfile::Compact => (CompressionType::Zstd, Some(9)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Label methods stability ──────────────────────────────────────────────

    #[test]
    fn format_type_labels_stable() {
        assert_eq!(FormatType::Parquet.label(), "parquet");
        assert_eq!(FormatType::Csv.label(), "csv");
    }

    #[test]
    fn compression_type_labels_stable() {
        assert_eq!(CompressionType::Zstd.label(), "zstd");
        assert_eq!(CompressionType::Snappy.label(), "snappy");
        assert_eq!(CompressionType::Gzip.label(), "gzip");
        assert_eq!(CompressionType::Lz4.label(), "lz4");
        assert_eq!(CompressionType::None.label(), "none");
    }

    // ── ParquetConfig::effective_row_group_rows ─────────────────────────────

    fn narrow_schema() -> arrow::datatypes::SchemaRef {
        use arrow::datatypes::{DataType, Field, Schema};
        std::sync::Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("created_at", DataType::Int64, false),
        ]))
    }

    fn wide_schema() -> arrow::datatypes::SchemaRef {
        use arrow::datatypes::{DataType, Field, Schema};
        let fields: Vec<Field> = (0..50)
            .map(|i| Field::new(format!("col{i}"), DataType::Utf8, true))
            .collect();
        std::sync::Arc::new(Schema::new(fields))
    }

    #[test]
    fn parquet_config_fixed_rows_returns_explicit_count() {
        let pc = ParquetConfig {
            row_group_strategy: Some(RowGroupStrategy::FixedRows),
            row_group_rows: Some(250_000),
            ..Default::default()
        };
        assert_eq!(pc.effective_row_group_rows(&narrow_schema()), Some(250_000));
    }

    #[test]
    fn parquet_config_fixed_rows_without_row_group_rows_returns_none() {
        let pc = ParquetConfig {
            row_group_strategy: Some(RowGroupStrategy::FixedRows),
            row_group_rows: None,
            ..Default::default()
        };
        assert_eq!(pc.effective_row_group_rows(&narrow_schema()), None);
    }

    #[test]
    fn parquet_config_auto_narrow_table_produces_large_groups() {
        let pc = ParquetConfig {
            row_group_strategy: Some(RowGroupStrategy::Auto),
            target_row_group_mb: Some(128),
            ..Default::default()
        };
        let rows = pc.effective_row_group_rows(&narrow_schema()).unwrap();
        assert!(
            rows >= 1_000_000,
            "narrow table should get large groups, got {rows}"
        );
    }

    #[test]
    fn parquet_config_auto_wide_table_produces_smaller_groups() {
        let pc = ParquetConfig {
            row_group_strategy: Some(RowGroupStrategy::Auto),
            target_row_group_mb: Some(128),
            ..Default::default()
        };
        let rows = pc.effective_row_group_rows(&wide_schema()).unwrap();
        assert!(
            rows < 100_000,
            "wide table should get smaller groups, got {rows}"
        );
        assert!(rows >= 1_000, "should be at least the minimum, got {rows}");
    }

    #[test]
    fn parquet_config_max_row_group_mb_caps_result() {
        let pc = ParquetConfig {
            row_group_strategy: Some(RowGroupStrategy::Auto),
            target_row_group_mb: Some(128),
            max_row_group_mb: Some(1),
            ..Default::default()
        };
        let rows = pc.effective_row_group_rows(&narrow_schema()).unwrap();
        assert!(
            rows <= 100_000,
            "max_row_group_mb should cap rows, got {rows}"
        );
    }

    #[test]
    fn parquet_config_deserializes_from_yaml() {
        let yaml = "row_group_strategy: auto\ntarget_row_group_mb: 64\n";
        let pc: ParquetConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(pc.row_group_strategy, Some(RowGroupStrategy::Auto));
        assert_eq!(pc.target_row_group_mb, Some(64));
    }

    #[test]
    fn parquet_config_fixed_memory_same_math_as_auto() {
        let auto_pc = ParquetConfig {
            row_group_strategy: Some(RowGroupStrategy::Auto),
            target_row_group_mb: Some(64),
            ..Default::default()
        };
        let fixed_mem_pc = ParquetConfig {
            row_group_strategy: Some(RowGroupStrategy::FixedMemory),
            target_row_group_mb: Some(64),
            ..Default::default()
        };
        assert_eq!(
            auto_pc.effective_row_group_rows(&narrow_schema()),
            fixed_mem_pc.effective_row_group_rows(&narrow_schema()),
            "FixedMemory and Auto must produce identical row counts for the same target"
        );
        assert_eq!(
            auto_pc.effective_row_group_rows(&wide_schema()),
            fixed_mem_pc.effective_row_group_rows(&wide_schema()),
        );
    }

    #[test]
    fn parquet_config_auto_without_target_uses_default_128mb() {
        let pc = ParquetConfig {
            row_group_strategy: Some(RowGroupStrategy::Auto),
            target_row_group_mb: None,
            ..Default::default()
        };
        let rows = pc.effective_row_group_rows(&narrow_schema()).unwrap();
        assert!(
            rows >= 1_000_000,
            "default 128 MB target should give large groups for narrow table; got {rows}"
        );
    }

    #[test]
    fn parquet_config_no_block_gives_none_for_row_group_rows() {
        let pc = ParquetConfig::default();
        let rows = pc.effective_row_group_rows(&narrow_schema());
        assert!(
            rows.is_some(),
            "default ParquetConfig (strategy: None) must return Some, got None"
        );
    }

    #[test]
    fn parquet_config_small_target_clamps_to_minimum_1000_rows() {
        let pc = ParquetConfig {
            row_group_strategy: Some(RowGroupStrategy::Auto),
            target_row_group_mb: Some(1),
            ..Default::default()
        };
        let rows = pc.effective_row_group_rows(&wide_schema()).unwrap();
        assert!(
            rows >= 1_000,
            "must not go below minimum 1 000 rows; got {rows}"
        );
    }
}
