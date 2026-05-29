//! Tuning profiles and the resolved `SourceTuning` config.
//!
//! Three baked-in profiles (`Fast`, `Balanced`, `Safe`) ship per-field defaults;
//! a YAML `TuningConfig` can override individual fields and the chosen profile.
//! `from_config_with_default_profile` is the production entry point and is
//! wired in [`crate::plan::build`] to honour `source.environment:` —
//! `Local` → `Fast`, `Replica`/`Production` → `Balanced`.

use arrow::datatypes::SchemaRef;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::memory::{compute_batch_size_from_memory, estimate_row_bytes};

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
    /// When true, Rivet samples DB pressure metrics every
    /// [`super::ADAPTIVE_SAMPLE_INTERVAL`] batches and shrinks/restores the
    /// fetch size in response. Default: false.
    pub adaptive: bool,
    /// Floor for the OPT-2 concurrency governor: the lowest worker/connection
    /// count it will back parallelism down to under source pressure. `None`
    /// ⇒ 1. The ceiling is the export's configured `parallel`. Only consulted
    /// when `adaptive` is on and `parallel > 1`.
    pub min_parallel: Option<usize>,
    /// Hard ceiling on a single cell/value in MB (OPT-1 memory hardening): a
    /// variable-length value (text/JSON/blob) larger than this aborts the run
    /// with `RIVET_VALUE_TOO_LARGE` instead of risking OOM, since the
    /// average-based batch cap can't bound one giant cell. `Some(0)` / `None`
    /// disable the guard. Default: 256 MiB.
    pub max_value_mb: Option<usize>,
    configured_profile: TuningProfile,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TuningProfile {
    Fast,
    Balanced,
    Safe,
}

/// Action taken when a single Arrow batch exceeds `max_batch_memory_mb`.
#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq, Default)]
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

#[derive(Debug, Deserialize, Serialize, JsonSchema, Default, Clone)]
#[serde(deny_unknown_fields)]
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
    /// Also arms the OPT-2 concurrency governor when `parallel > 1`.
    pub adaptive: Option<bool>,
    /// Floor for the concurrency governor (lowest parallelism under pressure).
    /// Default 1. Ceiling is the export's `parallel`.
    pub min_parallel: Option<usize>,
    /// Hard per-value size ceiling in MB. A single text/JSON/blob cell larger
    /// than this aborts the run with `RIVET_VALUE_TOO_LARGE`. `0` disables the
    /// guard. Default: 256.
    pub max_value_mb: Option<usize>,
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
            min_parallel: e.min_parallel.or(s.min_parallel),
            max_value_mb: e.max_value_mb.or(s.max_value_mb),
        }),
    }
}

impl SourceTuning {
    /// Build tuning with the legacy `Balanced` fallback. Public for downstream
    /// callers and tests; production resolution in [`crate::plan::build`] uses
    /// [`Self::from_config_with_default_profile`] so that `source.environment:`
    /// can pick the right default.
    #[allow(dead_code)]
    pub fn from_config(config: Option<&TuningConfig>) -> Self {
        Self::from_config_with_default_profile(config, TuningProfile::Balanced)
    }

    /// Like [`Self::from_config`] but lets the caller override the fallback
    /// profile used when `config.profile` is unset.
    pub fn from_config_with_default_profile(
        config: Option<&TuningConfig>,
        fallback_profile: TuningProfile,
    ) -> Self {
        let profile = config.and_then(|c| c.profile).unwrap_or(fallback_profile);

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
            if cfg.min_parallel.is_some() {
                tuning.min_parallel = cfg.min_parallel;
            }
            if cfg.max_value_mb.is_some() {
                tuning.max_value_mb = cfg.max_value_mb;
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
                min_parallel: None,
                max_value_mb: Some(256),
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
                min_parallel: None,
                max_value_mb: Some(256),
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
                min_parallel: None,
                max_value_mb: Some(256),
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
    fn fallback_profile_used_when_no_profile_in_config() {
        let t = SourceTuning::from_config_with_default_profile(None, TuningProfile::Fast);
        assert_eq!(t.batch_size, 50_000);
        assert_eq!(t.throttle_ms, 0, "fallback to Fast must zero the throttle");
        assert_eq!(t.profile_name(), "fast");

        let t = SourceTuning::from_config_with_default_profile(None, TuningProfile::Safe);
        assert_eq!(t.throttle_ms, 500);
        assert_eq!(t.profile_name(), "safe");
    }

    #[test]
    fn explicit_profile_wins_over_fallback() {
        let cfg = cfg_with_profile(TuningProfile::Balanced);
        let t = SourceTuning::from_config_with_default_profile(Some(&cfg), TuningProfile::Fast);
        assert_eq!(
            t.throttle_ms, 50,
            "explicit balanced profile must keep its throttle"
        );
        assert_eq!(t.profile_name(), "balanced");
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
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;
        let cfg = TuningConfig {
            batch_size_memory_mb: Some(256),
            ..Default::default()
        };
        let t = SourceTuning::from_config(Some(&cfg));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
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
}
