//! Partition/window reconciliation report types (Epic F).
//!
//! Produced by `rivet reconcile`; serializable so external tooling can pipe the
//! output into repair scripts or dashboards. Consistent with ADR-0006's advisory
//! policy: reports surface mismatches and candidate partitions — they do not
//! trigger re-exports on their own.
//!
//! Consumer lives in the `rivet` binary (`pipeline::reconcile_cmd`); silence
//! library-only dead_code analysis here.
#![allow(dead_code)]

use serde::{Deserialize, Serialize};

/// What kind of partition a row in the report describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionKind {
    /// A numeric chunk range `[start, end]` from chunked mode.
    Chunk,
    /// A single day window from `time_window` mode (reserved; not populated in v1).
    TimeWindow,
}

/// Outcome of comparing per-partition source and exported row counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionStatus {
    /// `source_count == exported_count` — partition matches.
    Match,
    /// Counts differ — partition is a repair candidate.
    Mismatch,
    /// Either `source_count` or `exported_count` is not available (e.g. a
    /// `chunk_task` that never completed); partition is a repair candidate.
    Unknown,
}

/// Per-partition reconciliation result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionResult {
    pub kind: PartitionKind,
    /// Stable human-readable identifier (e.g. `"chunk 3 [1000..2000]"`).
    pub identifier: String,
    pub source_count: Option<i64>,
    pub exported_count: Option<i64>,
    pub status: PartitionStatus,
    /// Non-empty when `status != Match`; short operator-facing explanation.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub note: String,
}

impl PartitionResult {
    /// Classify `(source, exported)` into a status + note.
    pub fn classify(
        kind: PartitionKind,
        identifier: String,
        source_count: Option<i64>,
        exported_count: Option<i64>,
    ) -> Self {
        let (status, note) = match (source_count, exported_count) {
            (Some(s), Some(e)) if s == e => (PartitionStatus::Match, String::new()),
            (Some(s), Some(e)) => (
                PartitionStatus::Mismatch,
                format!("source={s}, exported={e}, diff={}", s - e),
            ),
            (None, Some(_)) => (
                PartitionStatus::Unknown,
                "source count unavailable".to_string(),
            ),
            (Some(_), None) => (
                PartitionStatus::Unknown,
                "exported count unavailable (partition never completed?)".to_string(),
            ),
            (None, None) => (PartitionStatus::Unknown, "no counts available".to_string()),
        };
        Self {
            kind,
            identifier,
            source_count,
            exported_count,
            status,
            note,
        }
    }
}

/// Full reconciliation report for one export.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconcileReport {
    pub rivet_version: String,
    pub export_name: String,
    pub run_id: String,
    pub strategy: String,
    pub partitions: Vec<PartitionResult>,
    pub summary: ReconcileSummary,
}

/// Aggregated counts over all partitions in a report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconcileSummary {
    pub total_partitions: usize,
    pub matches: usize,
    pub mismatches: usize,
    pub unknown: usize,
    pub total_source_rows: i64,
    pub total_exported_rows: i64,
}

impl ReconcileReport {
    pub fn new(
        export_name: String,
        run_id: String,
        strategy: String,
        partitions: Vec<PartitionResult>,
    ) -> Self {
        let summary = ReconcileSummary::from_partitions(&partitions);
        Self {
            rivet_version: env!("CARGO_PKG_VERSION").to_string(),
            export_name,
            run_id,
            strategy,
            partitions,
            summary,
        }
    }

    /// Partitions that need repair (mismatch or unknown), in original order.
    pub fn repair_candidates(&self) -> Vec<&PartitionResult> {
        self.partitions
            .iter()
            .filter(|p| !matches!(p.status, PartitionStatus::Match))
            .collect()
    }

    pub fn to_json_pretty(&self) -> crate::error::Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }
}

impl ReconcileSummary {
    fn from_partitions(partitions: &[PartitionResult]) -> Self {
        let mut matches = 0;
        let mut mismatches = 0;
        let mut unknown = 0;
        let mut total_source_rows: i64 = 0;
        let mut total_exported_rows: i64 = 0;
        for p in partitions {
            match p.status {
                PartitionStatus::Match => matches += 1,
                PartitionStatus::Mismatch => mismatches += 1,
                PartitionStatus::Unknown => unknown += 1,
            }
            if let Some(s) = p.source_count {
                total_source_rows += s;
            }
            if let Some(e) = p.exported_count {
                total_exported_rows += e;
            }
        }
        Self {
            total_partitions: partitions.len(),
            matches,
            mismatches,
            unknown,
            total_source_rows,
            total_exported_rows,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(id: &str, s: Option<i64>, e: Option<i64>) -> PartitionResult {
        PartitionResult::classify(PartitionKind::Chunk, id.into(), s, e)
    }

    #[test]
    fn classify_match_when_counts_equal() {
        let p = chunk("c0", Some(100), Some(100));
        assert_eq!(p.status, PartitionStatus::Match);
        assert!(p.note.is_empty());
    }

    #[test]
    fn classify_mismatch_surfaces_diff() {
        let p = chunk("c0", Some(100), Some(90));
        assert_eq!(p.status, PartitionStatus::Mismatch);
        assert!(p.note.contains("diff=10"));
    }

    #[test]
    fn classify_unknown_when_exported_missing() {
        let p = chunk("c0", Some(50), None);
        assert_eq!(p.status, PartitionStatus::Unknown);
        assert!(p.note.to_lowercase().contains("exported"));
    }

    #[test]
    fn report_summary_counts_and_repair_candidates() {
        let partitions = vec![
            chunk("c0", Some(10), Some(10)),
            chunk("c1", Some(20), Some(15)),
            chunk("c2", Some(30), None),
        ];
        let r = ReconcileReport::new("orders".into(), "run1".into(), "chunked".into(), partitions);
        assert_eq!(r.summary.total_partitions, 3);
        assert_eq!(r.summary.matches, 1);
        assert_eq!(r.summary.mismatches, 1);
        assert_eq!(r.summary.unknown, 1);
        assert_eq!(r.summary.total_source_rows, 60);
        assert_eq!(r.summary.total_exported_rows, 25);
        let repair: Vec<_> = r
            .repair_candidates()
            .iter()
            .map(|p| p.identifier.as_str())
            .collect();
        assert_eq!(repair, vec!["c1", "c2"]);
    }

    #[test]
    fn json_round_trip_report() {
        let p = vec![chunk("c0", Some(10), Some(10))];
        let r = ReconcileReport::new("x".into(), "run".into(), "chunked".into(), p);
        let j = r.to_json_pretty().unwrap();
        let back: ReconcileReport = serde_json::from_str(&j).unwrap();
        assert_eq!(back, r);
    }
}
