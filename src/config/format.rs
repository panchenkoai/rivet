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

/// Whether `format` actually encodes `compression` on write.
///
/// Only Parquet has a compression encoder (see [`crate::format::parquet`]); the
/// CSV writer ([`crate::format::csv`]) ignores the codec entirely. Accepting a
/// non-`None` codec for CSV would be a silent no-op — the file stays
/// uncompressed while the run manifest records the codec — so config validation
/// rejects that combination up-front (Finding #10). `None` is always supported
/// (it means "do not compress").
pub fn compression_supported(format: FormatType, compression: CompressionType) -> bool {
    match format {
        FormatType::Parquet => true,
        FormatType::Csv => matches!(compression, CompressionType::None),
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

    /// Parse a [`label`](Self::label)-shaped string back to a codec.
    ///
    /// Used by config validation to evaluate an *explicitly-written* codec
    /// against [`compression_supported`] without re-deriving the serde mapping.
    /// Returns `None` for any unrecognised string (serde rejects those during
    /// the real parse anyway).
    pub fn from_label(s: &str) -> Option<Self> {
        match s {
            "zstd" => Some(CompressionType::Zstd),
            "snappy" => Some(CompressionType::Snappy),
            "gzip" => Some(CompressionType::Gzip),
            "lz4" => Some(CompressionType::Lz4),
            "none" => Some(CompressionType::None),
            _ => None,
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
            // Clamp to >= 1 like the Auto/FixedMemory arms below: a row group cannot
            // hold 0 rows, so `row_group_rows: 0` would panic parquet-rs's
            // set_max_row_group_row_count(0) mid-export (config passed, run crashed).
            // Any positive count the user pins still stands (this arm is deliberately
            // unclamped-above so an exact large/small group size is honoured).
            RowGroupStrategy::FixedRows => self.row_group_rows.map(|n| n.max(1)),
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

/// L24: when a `compression_profile` is set *and* the user also wrote an
/// explicit codec the profile silently discards, return a one-line warning
/// naming both — otherwise `None`.
///
/// Pure (no logging) so it can be unit-tested; the caller emits the message.
/// `explicit_compression` is `Some` only when the user actually wrote a
/// `compression:` codec — a `#[serde(default)]` Zstd cannot be distinguished
/// from an omitted field, so the caller passes `None` for the defaulted case.
pub fn compression_profile_override_warning(
    profile: CompressionProfile,
    explicit_compression: Option<CompressionType>,
    explicit_level: Option<u32>,
) -> Option<String> {
    let (codec, _) = profile.to_codec();
    if let Some(c) = explicit_compression
        && c != codec
    {
        return Some(format!(
            "compression_profile '{}' overrides explicit compression '{}' (using '{}')",
            profile.label(),
            c.label(),
            codec.label(),
        ));
    }
    if explicit_level.is_some() {
        return Some(format!(
            "compression_profile '{}' overrides explicit compression_level (the profile sets its own level)",
            profile.label(),
        ));
    }
    None
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

    // ── L24: compression_profile override warning ───────────────────────────

    #[test]
    fn profile_override_warns_on_conflicting_explicit_codec() {
        // `compression_profile: fast` (snappy) with explicit `compression: gzip`
        // must warn, naming both codecs and the winner.
        let msg = compression_profile_override_warning(
            CompressionProfile::Fast,
            Some(CompressionType::Gzip),
            None,
        )
        .expect("conflicting explicit codec must warn");
        assert!(msg.contains("fast"), "got: {msg}");
        assert!(msg.contains("gzip"), "got: {msg}");
        assert!(msg.contains("snappy"), "winner codec named, got: {msg}");
    }

    #[test]
    fn profile_override_warns_on_explicit_level() {
        let msg = compression_profile_override_warning(CompressionProfile::Balanced, None, Some(7))
            .expect("explicit compression_level under a profile must warn");
        assert!(msg.contains("compression_level"), "got: {msg}");
    }

    #[test]
    fn profile_override_silent_when_codec_matches_profile() {
        // Profile `fast` resolves to snappy; an explicit `snappy` is not a
        // conflict, so no warning.
        assert!(
            compression_profile_override_warning(
                CompressionProfile::Fast,
                Some(CompressionType::Snappy),
                None,
            )
            .is_none()
        );
    }

    #[test]
    fn profile_override_silent_when_nothing_explicit() {
        assert!(
            compression_profile_override_warning(CompressionProfile::Compact, None, None).is_none()
        );
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
    fn parquet_config_fixed_rows_zero_is_clamped_to_one_not_a_panic() {
        // RED before the .max(1) clamp: `row_group_rows: 0` returned Some(0), which
        // panics parquet-rs's set_max_row_group_row_count(0) mid-export (config
        // passed `check`, run crashed). A row group needs >= 1 row.
        let pc = ParquetConfig {
            row_group_strategy: Some(RowGroupStrategy::FixedRows),
            row_group_rows: Some(0),
            ..Default::default()
        };
        assert_eq!(
            pc.effective_row_group_rows(&narrow_schema()),
            Some(1),
            "row_group_rows: 0 must clamp to 1, never reach the parquet writer as 0"
        );
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
