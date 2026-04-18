//! Historical refinement for prioritization scoring (Epic I).
//!
//! Summarizes recent `export_metrics` rows into a small, deterministic snapshot
//! that the scoring engine can fold in. Bounded contribution by design: history
//! is a signal, not an oracle (ADR-0006 principle 3).
//!
//! Internal consumer (binary) only; silence library-only dead_code checks.
#![allow(dead_code)]

use serde::{Deserialize, Serialize};

use crate::state::ExportMetric;

/// Aggregated signals from recent runs of one export.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct HistorySnapshot {
    pub sample_size: usize,
    pub success_rate: f64,
    pub retry_rate: f64,
    pub avg_duration_ms: i64,
    pub last_status: Option<LastStatus>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LastStatus {
    Succeeded,
    Skipped,
    Failed,
}

impl HistorySnapshot {
    /// Build a snapshot from the most recent `metrics` (caller decides the window; up to ~20 is typical).
    ///
    /// `metrics` is expected in descending `run_at` order — same shape as
    /// [`crate::state::StateStore::get_metrics`] returns.
    pub fn summarize(metrics: &[ExportMetric]) -> Self {
        if metrics.is_empty() {
            return Self::empty();
        }
        let n = metrics.len();
        let successes = metrics
            .iter()
            .filter(|m| matches!(m.status.as_str(), "completed" | "skipped"))
            .count();
        let total_retries: i64 = metrics.iter().map(|m| m.retries).sum();
        let avg_duration_ms = metrics.iter().map(|m| m.duration_ms).sum::<i64>() / n as i64;
        let last_status = match metrics[0].status.as_str() {
            "completed" => Some(LastStatus::Succeeded),
            "skipped" => Some(LastStatus::Skipped),
            "failed" | "error" => Some(LastStatus::Failed),
            _ => None,
        };
        Self {
            sample_size: n,
            success_rate: successes as f64 / n as f64,
            retry_rate: total_retries as f64 / n as f64,
            avg_duration_ms,
            last_status,
        }
    }

    pub fn empty() -> Self {
        Self {
            sample_size: 0,
            success_rate: 0.0,
            retry_rate: 0.0,
            avg_duration_ms: 0,
            last_status: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.sample_size == 0
    }

    pub fn last_run_failed(&self) -> bool {
        matches!(self.last_status, Some(LastStatus::Failed))
    }

    /// Advisory threshold: > 0.3 means "one retry per three runs on average".
    pub fn retry_rate_is_high(&self) -> bool {
        self.sample_size >= 3 && self.retry_rate > 0.3
    }

    /// Advisory threshold: ≥ 5 minutes average.
    pub fn is_slow_historically(&self) -> bool {
        self.sample_size >= 3 && self.avg_duration_ms >= 5 * 60 * 1000
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metric(status: &str, duration_ms: i64, retries: i64) -> ExportMetric {
        ExportMetric {
            export_name: "orders".into(),
            run_id: Some("r".into()),
            run_at: "2026-04-18T00:00:00Z".into(),
            duration_ms,
            total_rows: 0,
            peak_rss_mb: None,
            status: status.into(),
            error_message: None,
            tuning_profile: None,
            format: None,
            mode: None,
            files_produced: 0,
            bytes_written: 0,
            retries,
            validated: None,
            schema_changed: None,
        }
    }

    #[test]
    fn empty_history_has_empty_snapshot() {
        let h = HistorySnapshot::summarize(&[]);
        assert!(h.is_empty());
        assert!(!h.last_run_failed());
        assert!(!h.retry_rate_is_high());
        assert!(!h.is_slow_historically());
    }

    #[test]
    fn success_rate_reflects_completed_and_skipped() {
        let ms = vec![
            metric("completed", 1000, 0),
            metric("skipped", 100, 0),
            metric("failed", 5000, 3),
        ];
        let h = HistorySnapshot::summarize(&ms);
        assert_eq!(h.sample_size, 3);
        assert!((h.success_rate - 2.0 / 3.0).abs() < 1e-9);
        assert_eq!(h.last_status, Some(LastStatus::Succeeded));
    }

    #[test]
    fn retry_rate_high_triggers_above_threshold() {
        let ms = vec![
            metric("completed", 100, 2),
            metric("completed", 100, 2),
            metric("completed", 100, 2),
        ];
        let h = HistorySnapshot::summarize(&ms);
        assert!(h.retry_rate_is_high());
    }

    #[test]
    fn retry_rate_high_needs_minimum_sample_size() {
        let ms = vec![metric("completed", 100, 5)];
        let h = HistorySnapshot::summarize(&ms);
        assert!(!h.retry_rate_is_high(), "single-run histories are ignored");
    }

    #[test]
    fn slow_history_requires_sustained_duration() {
        let ms = vec![
            metric("completed", 10 * 60_000, 0),
            metric("completed", 10 * 60_000, 0),
            metric("completed", 10 * 60_000, 0),
        ];
        let h = HistorySnapshot::summarize(&ms);
        assert!(h.is_slow_historically());
    }

    #[test]
    fn last_run_failed_surfaces_recent_failure() {
        let ms = vec![metric("failed", 0, 3), metric("completed", 1000, 0)];
        let h = HistorySnapshot::summarize(&ms);
        assert!(h.last_run_failed());
    }
}
