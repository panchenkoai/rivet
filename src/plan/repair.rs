//! Targeted repair types (Epic H).
//!
//! Consumes a [`ReconcileReport`] (Epic F) and produces a `RepairPlan` —
//! the minimal set of partitions to re-export without rerunning the whole
//! export. Both plan and outcome (`RepairReport`) are JSON-serializable so
//! external tooling can review / approve before execution (ADR-0005-style
//! plan/apply separation).
//!
//! Execution lives in `pipeline::repair_cmd`; this module is data + rules only.
//!
//! Consumer lives in the `rivet` binary; silence library-only dead_code checks.
#![allow(dead_code)]

use serde::{Deserialize, Serialize};

use super::reconcile::{PartitionStatus, ReconcileReport};

/// A single chunk to re-export.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairAction {
    pub chunk_index: i64,
    pub start_key: String,
    pub end_key: String,
    /// Human-readable reason, copied from the source reconcile partition note.
    pub reason: String,
}

impl RepairAction {
    /// Parse `chunk_index` and `[start..end]` from a partition identifier
    /// like `"chunk 3 [1000..2000]"`. Returns `None` if the identifier is
    /// not in the canonical shape (e.g. unparseable chunk keys).
    pub fn from_identifier(identifier: &str, reason: String) -> Option<Self> {
        let rest = identifier.strip_prefix("chunk ")?;
        let (idx_str, bracket) = rest.split_once(' ')?;
        let chunk_index = idx_str.parse::<i64>().ok()?;
        let inside = bracket.strip_prefix('[')?.strip_suffix(']')?;
        let (start, end) = inside.split_once("..")?;
        Some(Self {
            chunk_index,
            start_key: start.to_string(),
            end_key: end.to_string(),
            reason,
        })
    }
}

/// Repair plan derived from a reconcile report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairPlan {
    pub rivet_version: String,
    pub export_name: String,
    /// Reconcile run that produced the mismatch set.
    pub reconcile_run_id: String,
    pub strategy: String,
    pub actions: Vec<RepairAction>,
    /// Reasons for skipped partitions (e.g. unparseable identifiers).
    pub skipped: Vec<String>,
}

impl RepairPlan {
    pub fn from_reconcile(report: &ReconcileReport) -> Self {
        let mut actions = Vec::new();
        let mut skipped = Vec::new();
        for p in &report.partitions {
            if matches!(p.status, PartitionStatus::Match) {
                continue;
            }
            let reason = if p.note.is_empty() {
                format!("{:?}", p.status).to_lowercase()
            } else {
                p.note.clone()
            };
            match RepairAction::from_identifier(&p.identifier, reason) {
                Some(a) => actions.push(a),
                None => skipped.push(format!(
                    "cannot parse partition identifier '{}' ({:?})",
                    p.identifier, p.status
                )),
            }
        }
        Self {
            rivet_version: env!("CARGO_PKG_VERSION").to_string(),
            export_name: report.export_name.clone(),
            reconcile_run_id: report.run_id.clone(),
            strategy: report.strategy.clone(),
            actions,
            skipped,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }

    pub fn to_json_pretty(&self) -> crate::error::Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }
}

/// Per-action outcome after `rivet repair` executes the plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RepairOutcome {
    Executed { rows_written: i64 },
    Skipped { reason: String },
    Failed { error: String },
}

/// Execution report for a `rivet repair --execute` run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairReport {
    pub plan: RepairPlan,
    pub repair_run_id: String,
    pub results: Vec<(RepairAction, RepairOutcome)>,
    pub summary: RepairSummary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairSummary {
    pub planned: usize,
    pub executed: usize,
    pub skipped: usize,
    pub failed: usize,
    pub rows_written: i64,
}

impl RepairReport {
    pub fn new(
        plan: RepairPlan,
        repair_run_id: String,
        results: Vec<(RepairAction, RepairOutcome)>,
    ) -> Self {
        let mut executed = 0;
        let mut skipped = 0;
        let mut failed = 0;
        let mut rows_written: i64 = 0;
        for (_, out) in &results {
            match out {
                RepairOutcome::Executed { rows_written: n } => {
                    executed += 1;
                    rows_written += *n;
                }
                RepairOutcome::Skipped { .. } => skipped += 1,
                RepairOutcome::Failed { .. } => failed += 1,
            }
        }
        let summary = RepairSummary {
            planned: plan.actions.len(),
            executed,
            skipped,
            failed,
            rows_written,
        };
        Self {
            plan,
            repair_run_id,
            results,
            summary,
        }
    }

    pub fn to_json_pretty(&self) -> crate::error::Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::reconcile::{PartitionKind, PartitionResult, ReconcileReport};

    fn report(partitions: Vec<PartitionResult>) -> ReconcileReport {
        ReconcileReport::new(
            "orders".into(),
            "rec-run-1".into(),
            "chunked".into(),
            partitions,
        )
    }

    fn chunk(id: &str, s: Option<i64>, e: Option<i64>) -> PartitionResult {
        PartitionResult::classify(PartitionKind::Chunk, id.into(), s, e)
    }

    #[test]
    fn identifier_parsing_standard_shape() {
        let a = RepairAction::from_identifier("chunk 3 [1000..2000]", "diff=5".into()).unwrap();
        assert_eq!(a.chunk_index, 3);
        assert_eq!(a.start_key, "1000");
        assert_eq!(a.end_key, "2000");
    }

    #[test]
    fn identifier_parsing_rejects_noncanonical() {
        assert!(RepairAction::from_identifier("chunk 3 [alpha..omega]", "".into()).is_some());
        assert!(RepairAction::from_identifier("day 2024-01-01", "".into()).is_none());
        assert!(RepairAction::from_identifier("chunk 3", "".into()).is_none());
    }

    #[test]
    fn repair_plan_derives_actions_from_mismatches_only() {
        let r = report(vec![
            chunk("chunk 0 [1..100]", Some(100), Some(100)), // match
            chunk("chunk 1 [101..200]", Some(100), Some(90)), // mismatch
            chunk("chunk 2 [201..300]", Some(80), None),     // unknown
        ]);
        let p = RepairPlan::from_reconcile(&r);
        let indexes: Vec<_> = p.actions.iter().map(|a| a.chunk_index).collect();
        assert_eq!(indexes, vec![1, 2]);
        assert!(p.skipped.is_empty());
        assert!(!p.is_empty());
    }

    #[test]
    fn repair_plan_skips_unparseable_identifiers() {
        let mut partition = chunk("chunk 0 [alpha..beta]", None, Some(5));
        // status Unknown path already — still unparseable i64 keys but the identifier is still in canonical form.
        // Force a weird identifier for this test:
        partition.identifier = "day 2024-01-01".into();
        let r = report(vec![partition]);
        let p = RepairPlan::from_reconcile(&r);
        assert!(p.actions.is_empty());
        assert_eq!(p.skipped.len(), 1);
        assert!(p.skipped[0].contains("day 2024-01-01"));
    }

    #[test]
    fn repair_plan_is_empty_when_all_match() {
        let r = report(vec![chunk("chunk 0 [1..100]", Some(10), Some(10))]);
        let p = RepairPlan::from_reconcile(&r);
        assert!(p.is_empty());
    }

    #[test]
    fn repair_report_summary_counts() {
        let plan = RepairPlan {
            rivet_version: "0.0.0".into(),
            export_name: "x".into(),
            reconcile_run_id: "r".into(),
            strategy: "chunked".into(),
            actions: vec![
                RepairAction::from_identifier("chunk 0 [1..10]", "r".into()).unwrap(),
                RepairAction::from_identifier("chunk 1 [11..20]", "r".into()).unwrap(),
                RepairAction::from_identifier("chunk 2 [21..30]", "r".into()).unwrap(),
            ],
            skipped: vec![],
        };
        let results = vec![
            (
                plan.actions[0].clone(),
                RepairOutcome::Executed { rows_written: 10 },
            ),
            (
                plan.actions[1].clone(),
                RepairOutcome::Skipped {
                    reason: "n/a".into(),
                },
            ),
            (
                plan.actions[2].clone(),
                RepairOutcome::Failed {
                    error: "boom".into(),
                },
            ),
        ];
        let rep = RepairReport::new(plan, "repair-1".into(), results);
        assert_eq!(rep.summary.planned, 3);
        assert_eq!(rep.summary.executed, 1);
        assert_eq!(rep.summary.skipped, 1);
        assert_eq!(rep.summary.failed, 1);
        assert_eq!(rep.summary.rows_written, 10);
    }
}
