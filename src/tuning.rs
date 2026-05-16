use arrow::datatypes::{DataType, SchemaRef};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceTuning {
    pub batch_size: usize,
    pub batch_size_memory_mb: Option<usize>,
    pub throttle_ms: u64,
    pub statement_timeout_s: u64,
    pub max_retries: u32,
    pub retry_backoff_ms: u64,
    pub lock_timeout_s: u64,
    /// RSS limit in MB before chunk processing throttles. `0` = no limit (disabled).
    pub memory_threshold_mb: usize,
    /// Hard cap on a single Arrow batch in MB. `None` = no cap.
    pub max_batch_memory_mb: Option<usize>,
    pub on_batch_memory_exceeded: BatchMemoryPolicy,
    /// When true, Rivet samples DB pressure metrics every ADAPTIVE_SAMPLE_INTERVAL
    /// batches and shrinks/restores the fetch size in response. Default: false.
    pub adaptive: bool,
    configured_profile: TuningProfile,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TuningProfile {
    Fast,
    Balanced,
    Safe,
}

/// Action taken when a single Arrow batch exceeds `max_batch_memory_mb`.
#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum BatchMemoryPolicy {
    /// Log a warning and continue. (default)
    #[default]
    Warn,
    /// Return an error — the export fails immediately.
    Fail,
    /// Split the oversized batch in half recursively until each sub-batch fits,
    /// then process them individually. Transparent to the rest of the pipeline.
    AutoShrink,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct TuningConfig {
    pub profile: Option<TuningProfile>,
    pub batch_size: Option<usize>,
    /// Target memory per batch in MB. Mutually exclusive with batch_size.
    pub batch_size_memory_mb: Option<usize>,
    pub throttle_ms: Option<u64>,
    pub statement_timeout_s: Option<u64>,
    pub max_retries: Option<u32>,
    pub retry_backoff_ms: Option<u64>,
    pub lock_timeout_s: Option<u64>,
    pub memory_threshold_mb: Option<usize>,
    /// Hard cap on Arrow batch memory in MB. When a batch exceeds this limit,
    /// `on_batch_memory_exceeded` determines the response.
    pub max_batch_memory_mb: Option<usize>,
    /// Policy applied when a batch exceeds `max_batch_memory_mb`. Default: `warn`.
    pub on_batch_memory_exceeded: Option<BatchMemoryPolicy>,
    /// Enable real-time batch size adaptation based on DB pressure metrics.
    /// Postgres: samples `pg_stat_bgwriter`. MySQL: samples `Innodb_log_waits`.
    pub adaptive: Option<bool>,
}

/// Layer `export` on top of `source`: each field uses export when set, otherwise source.
/// `None` only when both inputs are `None`.
pub fn merge_tuning_config(
    source: Option<&TuningConfig>,
    export: Option<&TuningConfig>,
) -> Option<TuningConfig> {
    match (source, export) {
        (None, None) => None,
        (Some(s), None) => Some(s.clone()),
        (None, Some(e)) => Some(e.clone()),
        (Some(s), Some(e)) => Some(TuningConfig {
            profile: e.profile.or(s.profile),
            batch_size: e.batch_size.or(s.batch_size),
            batch_size_memory_mb: e.batch_size_memory_mb.or(s.batch_size_memory_mb),
            throttle_ms: e.throttle_ms.or(s.throttle_ms),
            statement_timeout_s: e.statement_timeout_s.or(s.statement_timeout_s),
            max_retries: e.max_retries.or(s.max_retries),
            retry_backoff_ms: e.retry_backoff_ms.or(s.retry_backoff_ms),
            lock_timeout_s: e.lock_timeout_s.or(s.lock_timeout_s),
            memory_threshold_mb: e.memory_threshold_mb.or(s.memory_threshold_mb),
            max_batch_memory_mb: e.max_batch_memory_mb.or(s.max_batch_memory_mb),
            on_batch_memory_exceeded: e.on_batch_memory_exceeded.or(s.on_batch_memory_exceeded),
            adaptive: e.adaptive.or(s.adaptive),
        }),
    }
}

impl SourceTuning {
    pub fn from_config(config: Option<&TuningConfig>) -> Self {
        let profile = config
            .and_then(|c| c.profile)
            .unwrap_or(TuningProfile::Balanced);

        let mut tuning = Self::from_profile(profile);
        tuning.configured_profile = profile;

        if let Some(cfg) = config {
            if let Some(v) = cfg.batch_size {
                tuning.batch_size = v;
            }
            tuning.batch_size_memory_mb = cfg.batch_size_memory_mb;
            if let Some(v) = cfg.throttle_ms {
                tuning.throttle_ms = v;
            }
            if let Some(v) = cfg.statement_timeout_s {
                tuning.statement_timeout_s = v;
            }
            if let Some(v) = cfg.max_retries {
                tuning.max_retries = v;
            }
            if let Some(v) = cfg.retry_backoff_ms {
                tuning.retry_backoff_ms = v;
            }
            if let Some(v) = cfg.lock_timeout_s {
                tuning.lock_timeout_s = v;
            }
            if let Some(v) = cfg.memory_threshold_mb {
                tuning.memory_threshold_mb = v;
            }
            tuning.max_batch_memory_mb = cfg.max_batch_memory_mb;
            if let Some(v) = cfg.on_batch_memory_exceeded {
                tuning.on_batch_memory_exceeded = v;
            }
            if let Some(v) = cfg.adaptive {
                tuning.adaptive = v;
            }
        }

        tuning
    }

    fn from_profile(profile: TuningProfile) -> Self {
        match profile {
            TuningProfile::Fast => Self {
                batch_size: 50_000,
                batch_size_memory_mb: None,
                throttle_ms: 0,
                statement_timeout_s: 0,
                max_retries: 1,
                retry_backoff_ms: 1_000,
                lock_timeout_s: 0,
                memory_threshold_mb: 0,
                max_batch_memory_mb: None,
                on_batch_memory_exceeded: BatchMemoryPolicy::Warn,
                adaptive: false,
                configured_profile: TuningProfile::Fast,
            },
            TuningProfile::Balanced => Self {
                batch_size: 10_000,
                batch_size_memory_mb: None,
                throttle_ms: 50,
                statement_timeout_s: 300,
                max_retries: 3,
                retry_backoff_ms: 2_000,
                lock_timeout_s: 30,
                memory_threshold_mb: 4_096,
                max_batch_memory_mb: None,
                on_batch_memory_exceeded: BatchMemoryPolicy::Warn,
                adaptive: false,
                configured_profile: TuningProfile::Balanced,
            },
            TuningProfile::Safe => Self {
                batch_size: 2_000,
                batch_size_memory_mb: None,
                throttle_ms: 500,
                statement_timeout_s: 120,
                max_retries: 5,
                retry_backoff_ms: 5_000,
                lock_timeout_s: 10,
                memory_threshold_mb: 2_048,
                max_batch_memory_mb: None,
                on_batch_memory_exceeded: BatchMemoryPolicy::Warn,
                adaptive: false,
                configured_profile: TuningProfile::Safe,
            },
        }
    }

    pub fn profile_name(&self) -> &'static str {
        match self.configured_profile {
            TuningProfile::Fast => "fast",
            TuningProfile::Balanced => "balanced",
            TuningProfile::Safe => "safe",
        }
    }
}

impl std::fmt::Display for SourceTuning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "profile={}, batch_size={}, throttle={}ms, timeout={}s, retries={}, lock_timeout={}s",
            self.profile_name(),
            self.batch_size,
            self.throttle_ms,
            self.statement_timeout_s,
            self.max_retries,
            self.lock_timeout_s,
        )
    }
}

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
pub fn compute_batch_size_from_memory(memory_mb: usize, schema: &SchemaRef) -> usize {
    let row_bytes = estimate_row_bytes(schema);
    let target = memory_mb * 1024 * 1024 / row_bytes;
    target.clamp(1_000, 500_000)
}

impl SourceTuning {
    /// If `batch_size_memory_mb` is set, compute and return an adjusted batch_size
    /// from the schema; otherwise return the configured `batch_size`.
    pub fn effective_batch_size(&self, schema: Option<&SchemaRef>) -> usize {
        if let (Some(mem_mb), Some(schema)) = (self.batch_size_memory_mb, schema) {
            let computed = compute_batch_size_from_memory(mem_mb, schema);
            log::info!(
                "batch_size_memory_mb={}: estimated row ~{}B, computed batch_size={}",
                mem_mb,
                estimate_row_bytes(schema),
                computed
            );
            computed
        } else {
            self.batch_size
        }
    }

    /// Return the actual Arrow memory footprint of a batch in bytes.
    ///
    /// Sums `get_array_memory_size()` across all columns — includes buffers for
    /// validity bitmaps, offsets, and value data. Does not include Arrow struct
    /// overhead (~few hundred bytes) which is negligible at batch scale.
    pub fn batch_memory_bytes(batch: &arrow::record_batch::RecordBatch) -> usize {
        batch
            .columns()
            .iter()
            .map(|col| col.get_array_memory_size())
            .sum()
    }

    /// Produce a `ResourceSummary` from the resolved tuning settings.
    ///
    /// The summary requires no database connection. It reports two batch-memory
    /// bounds based on narrow-table (~200 B/row) and wide-table (~10 KB/row)
    /// heuristics. A `wide_table_risk` flag is set when the upper bound exceeds
    /// 128 MB per batch.
    pub fn resource_summary(&self) -> ResourceSummary {
        const NARROW_BYTES: f64 = 200.0;
        const WIDE_BYTES: f64 = 10_240.0;
        let batch = self.batch_size as f64;
        let batch_narrow_mb = batch * NARROW_BYTES / (1024.0 * 1024.0);
        let batch_wide_mb = batch * WIDE_BYTES / (1024.0 * 1024.0);
        ResourceSummary {
            profile: self.profile_name().to_string(),
            batch_size: self.batch_size,
            batch_size_memory_mb: self.batch_size_memory_mb,
            memory_threshold_mb: self.memory_threshold_mb,
            throttle_ms: self.throttle_ms,
            batch_narrow_mb,
            batch_wide_mb,
            wide_table_risk: batch_wide_mb > 128.0,
        }
    }
}

/// Number of batches between adaptive pressure samples.
pub const ADAPTIVE_SAMPLE_INTERVAL: usize = 10;
/// Hard floor for the adaptive fetch size — the loop never shrinks below this.
pub const ADAPTIVE_MIN_BATCH: usize = 500;

/// Decide the next adaptive fetch size from current pressure state.
///
/// - Under pressure: shrink to 75 %, but never below [`ADAPTIVE_MIN_BATCH`].
/// - Otherwise: grow to 125 %, but never above the schema-chosen `base` ceiling
///   (so we recover toward the initial fetch size without overshooting it).
///
/// Pure function — exported so adaptive batch-sizing can be unit-tested without
/// a live database. Both `PostgresSource` and `MysqlSource` call this.
pub fn next_adaptive_batch_size(current: usize, base: usize, under_pressure: bool) -> usize {
    if under_pressure {
        (current * 3 / 4).max(ADAPTIVE_MIN_BATCH)
    } else {
        (current * 5 / 4).min(base)
    }
}

/// Resource estimate computed from tuning settings alone (no DB connection required).
///
/// `batch_narrow_mb` and `batch_wide_mb` bracket the expected per-batch memory:
/// - narrow table: ~200 B/row (int-heavy, no text blobs)
/// - wide table  : ~10 KB/row (many text/JSON/binary columns)
///
/// Use `wide_table_risk` to decide whether to recommend `adaptive_batch` or a
/// lower `batch_size`.
#[derive(Debug, Clone)]
pub struct ResourceSummary {
    #[allow(dead_code)]
    pub profile: String,
    pub batch_size: usize,
    pub batch_size_memory_mb: Option<usize>,
    pub memory_threshold_mb: usize,
    pub throttle_ms: u64,
    pub batch_narrow_mb: f64,
    pub batch_wide_mb: f64,
    pub wide_table_risk: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_with_profile(profile: TuningProfile) -> TuningConfig {
        TuningConfig {
            profile: Some(profile),
            ..Default::default()
        }
    }

    #[test]
    fn default_config_uses_balanced_profile() {
        let t = SourceTuning::from_config(None);
        assert_eq!(t.batch_size, 10_000);
        assert_eq!(t.throttle_ms, 50);
        assert_eq!(t.statement_timeout_s, 300);
        assert_eq!(t.max_retries, 3);
        assert_eq!(t.retry_backoff_ms, 2_000);
        assert_eq!(t.lock_timeout_s, 30);
    }

    #[test]
    fn fast_profile_favors_throughput() {
        let t = SourceTuning::from_config(Some(&cfg_with_profile(TuningProfile::Fast)));
        assert_eq!(t.batch_size, 50_000);
        assert_eq!(t.throttle_ms, 0);
        assert_eq!(t.statement_timeout_s, 0);
        assert_eq!(t.max_retries, 1);
    }

    #[test]
    fn safe_profile_limits_impact() {
        let t = SourceTuning::from_config(Some(&cfg_with_profile(TuningProfile::Safe)));
        assert_eq!(t.batch_size, 2_000);
        assert_eq!(t.throttle_ms, 500);
        assert_eq!(t.statement_timeout_s, 120);
        assert_eq!(t.max_retries, 5);
        assert_eq!(t.retry_backoff_ms, 5_000);
        assert_eq!(t.lock_timeout_s, 10);
    }

    #[test]
    fn explicit_fields_override_profile_defaults() {
        let cfg = TuningConfig {
            profile: Some(TuningProfile::Safe),
            batch_size: Some(3_000),
            throttle_ms: Some(250),
            ..Default::default()
        };
        let t = SourceTuning::from_config(Some(&cfg));
        assert_eq!(t.batch_size, 3_000, "explicit batch_size should win");
        assert_eq!(t.throttle_ms, 250, "explicit throttle_ms should win");
        assert_eq!(
            t.statement_timeout_s, 120,
            "non-overridden field stays at safe default"
        );
        assert_eq!(
            t.max_retries, 5,
            "non-overridden field stays at safe default"
        );
    }

    #[test]
    fn profile_name_fast() {
        let t = SourceTuning::from_config(Some(&cfg_with_profile(TuningProfile::Fast)));
        assert_eq!(t.profile_name(), "fast");
    }

    #[test]
    fn profile_name_balanced() {
        let t = SourceTuning::from_config(None);
        assert_eq!(t.profile_name(), "balanced");
    }

    #[test]
    fn profile_name_safe() {
        let t = SourceTuning::from_config(Some(&cfg_with_profile(TuningProfile::Safe)));
        assert_eq!(t.profile_name(), "safe");
    }

    #[test]
    fn display_contains_all_fields() {
        let t = SourceTuning::from_config(None);
        let s = t.to_string();
        assert!(s.contains("profile=balanced"), "missing profile in: {s}");
        assert!(s.contains("batch_size=10000"), "missing batch_size in: {s}");
        assert!(s.contains("throttle=50ms"), "missing throttle in: {s}");
        assert!(s.contains("timeout=300s"), "missing timeout in: {s}");
        assert!(s.contains("retries=3"), "missing retries in: {s}");
        assert!(
            s.contains("lock_timeout=30s"),
            "missing lock_timeout in: {s}"
        );
    }

    #[test]
    fn estimate_row_bytes_basic() {
        use arrow::datatypes::{Field, Schema};
        use std::sync::Arc;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", arrow::datatypes::DataType::Int64, false),
            Field::new("name", arrow::datatypes::DataType::Utf8, true),
        ]));
        let est = estimate_row_bytes(&schema);
        // Int64=8+1, Utf8=256+1 = 266
        assert_eq!(est, 266);
    }

    #[test]
    fn compute_batch_size_clamped() {
        use arrow::datatypes::{Field, Schema};
        use std::sync::Arc;
        // 1 tiny column -> huge batch, clamped to 500_000
        let schema = Arc::new(Schema::new(vec![Field::new(
            "flag",
            arrow::datatypes::DataType::Boolean,
            false,
        )]));
        assert_eq!(compute_batch_size_from_memory(256, &schema), 500_000);

        // 100 large string columns -> small batch, clamped to 1_000
        let fields: Vec<Field> = (0..100)
            .map(|i| Field::new(format!("c{i}"), arrow::datatypes::DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        assert_eq!(compute_batch_size_from_memory(1, &schema), 1_000);
    }

    #[test]
    fn merge_tuning_export_overrides_source_fields() {
        let source = TuningConfig {
            profile: Some(TuningProfile::Fast),
            batch_size: Some(1_000),
            throttle_ms: Some(0),
            ..Default::default()
        };
        let export = TuningConfig {
            profile: Some(TuningProfile::Safe),
            batch_size: None,
            ..Default::default()
        };
        let m = merge_tuning_config(Some(&source), Some(&export)).expect("merged");
        assert_eq!(m.profile, Some(TuningProfile::Safe));
        assert_eq!(
            m.batch_size,
            Some(1_000),
            "export omitted batch_size -> keep source"
        );
        assert_eq!(m.throttle_ms, Some(0));
    }

    #[test]
    fn merge_tuning_export_only() {
        let e = cfg_with_profile(TuningProfile::Fast);
        let m = merge_tuning_config(None, Some(&e)).expect("merged");
        assert_eq!(m.profile, Some(TuningProfile::Fast));
    }

    #[test]
    fn effective_batch_size_without_memory() {
        let t = SourceTuning::from_config(None);
        assert_eq!(t.effective_batch_size(None), 10_000);
    }

    #[test]
    fn effective_batch_size_with_memory() {
        use arrow::datatypes::{Field, Schema};
        use std::sync::Arc;
        let cfg = TuningConfig {
            batch_size_memory_mb: Some(256),
            ..Default::default()
        };
        let t = SourceTuning::from_config(Some(&cfg));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", arrow::datatypes::DataType::Int64, false),
            Field::new("name", arrow::datatypes::DataType::Utf8, true),
        ]));
        let bs = t.effective_batch_size(Some(&schema));
        assert!((1_000..=500_000).contains(&bs), "got {bs}");
        // 256MB / 266B ≈ 1_009_022, clamped to 500_000
        assert_eq!(bs, 500_000);
    }

    #[test]
    fn resource_summary_balanced_profile() {
        let t = SourceTuning::from_config(None);
        let r = t.resource_summary();
        assert_eq!(r.profile, "balanced");
        assert_eq!(r.batch_size, 10_000);
        assert!(r.batch_size_memory_mb.is_none());
        assert_eq!(r.memory_threshold_mb, 4_096);
        assert_eq!(r.throttle_ms, 50);
        // narrow: 10_000 × 200 B = ~1.9 MB
        assert!(
            r.batch_narrow_mb < 5.0,
            "narrow too high: {}",
            r.batch_narrow_mb
        );
        // wide: 10_000 × 10 KB = ~95 MB — no risk (< 128 MB)
        assert!(
            !r.wide_table_risk,
            "balanced 10k should not trigger wide_table_risk"
        );
    }

    #[test]
    fn resource_summary_fast_profile_triggers_wide_table_risk() {
        let t = SourceTuning::from_config(Some(&TuningConfig {
            profile: Some(TuningProfile::Fast),
            ..Default::default()
        }));
        let r = t.resource_summary();
        assert_eq!(r.batch_size, 50_000);
        // wide: 50_000 × 10 KB = ~476 MB → high risk
        assert!(r.wide_table_risk, "fast 50k should trigger wide_table_risk");
    }

    #[test]
    fn resource_summary_with_adaptive_batch() {
        let cfg = TuningConfig {
            batch_size_memory_mb: Some(64),
            ..Default::default()
        };
        let t = SourceTuning::from_config(Some(&cfg));
        let r = t.resource_summary();
        assert_eq!(r.batch_size_memory_mb, Some(64));
    }

    // ── next_adaptive_batch_size ────────────────────────────────────────────

    #[test]
    fn adaptive_shrinks_by_25_percent_under_pressure() {
        assert_eq!(next_adaptive_batch_size(10_000, 10_000, true), 7_500);
        assert_eq!(next_adaptive_batch_size(8_000, 10_000, true), 6_000);
    }

    #[test]
    fn adaptive_grows_by_25_percent_when_idle() {
        // 4_000 × 5/4 = 5_000; well under base ceiling.
        assert_eq!(next_adaptive_batch_size(4_000, 10_000, false), 5_000);
    }

    #[test]
    fn adaptive_recovery_caps_at_base_ceiling() {
        // 9_000 × 5/4 = 11_250, but base is 10_000 — must clamp.
        assert_eq!(next_adaptive_batch_size(9_000, 10_000, false), 10_000);
        // Already at base: stays there.
        assert_eq!(next_adaptive_batch_size(10_000, 10_000, false), 10_000);
    }

    #[test]
    fn adaptive_shrink_respects_min_floor() {
        // 600 × 3/4 = 450, but ADAPTIVE_MIN_BATCH = 500 — must clamp up.
        assert_eq!(
            next_adaptive_batch_size(600, 10_000, true),
            ADAPTIVE_MIN_BATCH
        );
        // Already at floor: stays at floor.
        assert_eq!(
            next_adaptive_batch_size(ADAPTIVE_MIN_BATCH, 10_000, true),
            ADAPTIVE_MIN_BATCH
        );
    }

    #[test]
    fn adaptive_pressure_path_ignores_base_uses_only_floor() {
        // Pressure path never consults base: shrink is computed from current,
        // then clamped only to ADAPTIVE_MIN_BATCH. A pathologically low base
        // does not artificially pin us lower than the floor.
        // current=ADAPTIVE_MIN_BATCH already at floor; 500*3/4=375 → max(375,500)=500.
        assert_eq!(
            next_adaptive_batch_size(ADAPTIVE_MIN_BATCH, 100, true),
            ADAPTIVE_MIN_BATCH
        );
    }

    #[test]
    fn adaptive_steady_state_oscillation_stays_bounded() {
        // Simulate 50 sample cycles under sustained pressure, then sustained recovery.
        // Verifies: the loop never wanders below floor or above base, and converges.
        let base = 5_000;
        let mut s = base;
        for _ in 0..50 {
            s = next_adaptive_batch_size(s, base, true);
        }
        assert_eq!(
            s, ADAPTIVE_MIN_BATCH,
            "sustained pressure must converge to floor"
        );
        for _ in 0..50 {
            s = next_adaptive_batch_size(s, base, false);
        }
        assert_eq!(s, base, "sustained recovery must converge to base ceiling");
    }
}
