//! **Layer: Observability**
//!
//! `RunSummary` is the single observability artifact for a pipeline run.
//! It accumulates operational data during execution and is consumed by:
//! - the end-of-run terminal output (`print`)
//! - the metrics store (`state::record_metric`)
//! - the notification system (`notify::maybe_send`)
//!
//! `RunSummary` is written to by execution modules (row counts, byte counts, retries)
//! but it makes no execution decisions itself — it is a pure data accumulator.
//!
//! It embeds a `RunJournal` so that all pipeline modules — which already hold
//! `&mut RunSummary` — can record structured events via `summary.journal.record()`
//! without any signature changes.  In a future epic the relationship will invert:
//! `RunSummary` will be derived from `RunJournal`.

use super::format_bytes;
use super::journal::{PlanSnapshot, RunEvent, RunJournal};
use crate::plan::ResolvedRunPlan;

/// Accumulates operational data during a pipeline run for summary and metrics.
///
/// The embedded `journal` is the structured event log for this run.  Use
/// `summary.journal.record(event)` at any call site that already holds
/// `&mut RunSummary`.
#[derive(Debug, Clone)]
pub struct RunSummary {
    pub run_id: String,
    pub export_name: String,
    pub status: String,
    pub total_rows: i64,
    pub files_produced: usize,
    pub bytes_written: u64,
    pub duration_ms: i64,
    pub peak_rss_mb: i64,
    pub retries: u32,
    pub validated: Option<bool>,
    pub schema_changed: Option<bool>,
    pub quality_passed: Option<bool>,
    pub error_message: Option<String>,
    /// `profile` from YAML, or `balanced (default)` if omitted.
    pub tuning_profile: String,
    /// Configured `batch_size` from YAML/profile (FETCH cap before `batch_size_memory_mb` override).
    pub batch_size: usize,
    /// When set, actual FETCH size is derived from schema (see logs).
    pub batch_size_memory_mb: Option<usize>,
    pub format: String,
    pub mode: String,
    pub compression: String,
    /// Source COUNT(*) result for reconciliation (None = not requested or not applicable).
    pub source_count: Option<i64>,
    /// Whether reconciliation passed (Some(true) = match, Some(false) = mismatch, None = skipped).
    pub reconciled: Option<bool>,
    /// Structured event log for this run.  Answers the four DoD observability questions.
    pub journal: RunJournal,
}

impl RunSummary {
    pub(super) fn new(plan: &ResolvedRunPlan) -> Self {
        let run_id = format!(
            "{}_{}",
            plan.export_name,
            chrono::Utc::now().format("%Y%m%dT%H%M%S%.3f"),
        );
        let mut journal = RunJournal::new(&run_id, &plan.export_name);
        journal.record(RunEvent::PlanResolved(PlanSnapshot::from(plan)));
        Self {
            run_id,
            export_name: plan.export_name.clone(),
            status: "running".into(),
            total_rows: 0,
            files_produced: 0,
            bytes_written: 0,
            duration_ms: 0,
            peak_rss_mb: 0,
            retries: 0,
            validated: None,
            schema_changed: None,
            quality_passed: None,
            error_message: None,
            tuning_profile: plan.tuning_profile_label.clone(),
            batch_size: plan.tuning.batch_size,
            batch_size_memory_mb: plan.tuning.batch_size_memory_mb,
            format: plan.format.label().to_string(),
            mode: plan.strategy.mode_label().to_string(),
            compression: plan.compression.label().to_string(),
            source_count: None,
            reconciled: None,
            journal,
        }
    }

    pub(super) fn print(&self) {
        eprintln!();
        eprintln!("── {} ──", self.export_name);
        eprintln!("  run_id:      {}", self.run_id);
        eprintln!("  status:      {}", self.status);
        if let Some(mem) = self.batch_size_memory_mb {
            eprintln!(
                "  tuning:      profile={}, batch_size={} (batch_size_memory_mb={}MiB → effective FETCH in logs)",
                self.tuning_profile, self.batch_size, mem
            );
        } else {
            eprintln!(
                "  tuning:      profile={}, batch_size={}",
                self.tuning_profile, self.batch_size
            );
        }
        eprintln!("  rows:        {}", self.total_rows);
        eprintln!("  files:       {}", self.files_produced);
        if self.bytes_written > 0 {
            eprintln!("  bytes:       {}", format_bytes(self.bytes_written));
        }
        let dur = if self.duration_ms >= 1000 {
            format!("{:.1}s", self.duration_ms as f64 / 1000.0)
        } else {
            format!("{}ms", self.duration_ms)
        };
        eprintln!("  duration:    {}", dur);
        if self.peak_rss_mb > 0 {
            eprintln!("  peak RSS:    {}MB (sampled during run)", self.peak_rss_mb);
        }
        if self.format == "parquet" && self.compression != "zstd" {
            eprintln!("  compression: {}", self.compression);
        }
        if self.retries > 0 {
            eprintln!("  retries:     {}", self.retries);
        }
        if let Some(v) = self.validated {
            eprintln!("  validated:   {}", if v { "pass" } else { "FAIL" });
        }
        if let Some(sc) = self.schema_changed {
            eprintln!(
                "  schema:      {}",
                if sc { "CHANGED" } else { "unchanged" }
            );
        }
        if let Some(q) = self.quality_passed {
            eprintln!("  quality:     {}", if q { "pass" } else { "FAIL" });
        }
        if let Some(reconciled) = self.reconciled {
            let src = self
                .source_count
                .map(|c| c.to_string())
                .unwrap_or("?".into());
            if reconciled {
                eprintln!("  reconcile:   MATCH ({}/{})", self.total_rows, src);
            } else {
                eprintln!(
                    "  reconcile:   MISMATCH (exported {} vs source {})",
                    self.total_rows, src
                );
            }
        }
        if let Some(err) = &self.error_message {
            eprintln!("  error:       {}", err);
        }
    }
}
