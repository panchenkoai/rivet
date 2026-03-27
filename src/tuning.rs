use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct SourceTuning {
    pub batch_size: usize,
    pub throttle_ms: u64,
    pub statement_timeout_s: u64,
    pub max_retries: u32,
    pub retry_backoff_ms: u64,
    pub lock_timeout_s: u64,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TuningProfile {
    Fast,
    Balanced,
    Safe,
}

#[derive(Debug, Deserialize, Default)]
pub struct TuningConfig {
    pub profile: Option<TuningProfile>,
    pub batch_size: Option<usize>,
    pub throttle_ms: Option<u64>,
    pub statement_timeout_s: Option<u64>,
    pub max_retries: Option<u32>,
    pub retry_backoff_ms: Option<u64>,
    pub lock_timeout_s: Option<u64>,
}

impl SourceTuning {
    pub fn from_config(config: Option<&TuningConfig>) -> Self {
        let profile = config
            .and_then(|c| c.profile)
            .unwrap_or(TuningProfile::Balanced);

        let mut tuning = Self::from_profile(profile);

        if let Some(cfg) = config {
            if let Some(v) = cfg.batch_size { tuning.batch_size = v; }
            if let Some(v) = cfg.throttle_ms { tuning.throttle_ms = v; }
            if let Some(v) = cfg.statement_timeout_s { tuning.statement_timeout_s = v; }
            if let Some(v) = cfg.max_retries { tuning.max_retries = v; }
            if let Some(v) = cfg.retry_backoff_ms { tuning.retry_backoff_ms = v; }
            if let Some(v) = cfg.lock_timeout_s { tuning.lock_timeout_s = v; }
        }

        tuning
    }

    fn from_profile(profile: TuningProfile) -> Self {
        match profile {
            TuningProfile::Fast => Self {
                batch_size: 50_000,
                throttle_ms: 0,
                statement_timeout_s: 0,
                max_retries: 1,
                retry_backoff_ms: 1_000,
                lock_timeout_s: 0,
            },
            TuningProfile::Balanced => Self {
                batch_size: 10_000,
                throttle_ms: 50,
                statement_timeout_s: 300,
                max_retries: 3,
                retry_backoff_ms: 2_000,
                lock_timeout_s: 30,
            },
            TuningProfile::Safe => Self {
                batch_size: 2_000,
                throttle_ms: 500,
                statement_timeout_s: 120,
                max_retries: 5,
                retry_backoff_ms: 5_000,
                lock_timeout_s: 10,
            },
        }
    }

    pub fn profile_name(&self) -> &'static str {
        if self.throttle_ms == 0 && self.batch_size >= 50_000 {
            "fast"
        } else if self.throttle_ms >= 500 || self.batch_size <= 2_000 {
            "safe"
        } else {
            "balanced"
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

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_with_profile(profile: TuningProfile) -> TuningConfig {
        TuningConfig { profile: Some(profile), ..Default::default() }
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
        assert_eq!(t.statement_timeout_s, 120, "non-overridden field stays at safe default");
        assert_eq!(t.max_retries, 5, "non-overridden field stays at safe default");
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
        assert!(s.contains("lock_timeout=30s"), "missing lock_timeout in: {s}");
    }
}
