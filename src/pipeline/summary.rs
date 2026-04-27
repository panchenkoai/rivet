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
use super::ipc::{self, ChildEvent};
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

        if ipc::ipc_events_enabled() {
            ipc::emit(&ChildEvent::Started {
                export_name: plan.export_name.clone(),
                run_id: run_id.clone(),
                mode: plan.strategy.mode_label().to_string(),
                tuning_profile: plan.tuning_profile_label.clone(),
                batch_size: plan.tuning.batch_size,
            });
        }

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
        // Child mode: emit a `Finished` IPC event instead of drawing the
        // stderr block.  The parent will render the final card itself.
        if ipc::ipc_events_enabled() {
            ipc::emit(&ChildEvent::Finished {
                export_name: self.export_name.clone(),
                run_id: self.run_id.clone(),
                status: self.status.clone(),
                total_rows: self.total_rows,
                files_produced: self.files_produced as u64,
                bytes_written: self.bytes_written,
                duration_ms: self.duration_ms,
                peak_rss_mb: self.peak_rss_mb,
                error_message: self.error_message.clone(),
            });
            return;
        }

        use std::io::Write;
        // Render the whole block into a single buffer so the call site emits
        // one `write_all` to stderr.  Without this, parallel exports could
        // interleave individual lines from different `RunSummary::print()`
        // calls — visible as garbled blocks in `--parallel-exports` runs.
        let block = self.render();
        let stderr = std::io::stderr();
        let mut handle = stderr.lock();
        let _ = handle.write_all(block.as_bytes());
        let _ = handle.flush();
    }

    /// Build the block as a string.  Public to the module so tests can assert
    /// formatting without capturing stderr.
    fn render(&self) -> String {
        // Adaptive layout: collect (label, value) pairs that actually apply to
        // this run, then pad labels to the longest one so columns line up
        // *within* the block.  Header is a fixed width so consecutive blocks
        // look uniform regardless of which optional fields are present.
        let mut rows: Vec<(&'static str, String)> = Vec::with_capacity(16);
        rows.push(("run_id", self.run_id.clone()));
        rows.push(("status", self.status.clone()));

        let tuning_value = match self.batch_size_memory_mb {
            Some(mem) => format!(
                "profile={}, batch_size={} (batch_size_memory_mb={}MiB → effective FETCH in logs)",
                self.tuning_profile,
                fmt_thousands(self.batch_size as i64),
                mem
            ),
            None => format!(
                "profile={}, batch_size={}",
                self.tuning_profile,
                fmt_thousands(self.batch_size as i64)
            ),
        };
        rows.push(("tuning", tuning_value));

        rows.push(("rows", fmt_thousands(self.total_rows)));
        rows.push(("files", fmt_thousands(self.files_produced as i64)));
        if self.bytes_written > 0 {
            rows.push(("bytes", format_bytes(self.bytes_written)));
        }
        rows.push(("duration", fmt_duration_ms(self.duration_ms)));

        if self.peak_rss_mb > 0 {
            rows.push((
                "peak RSS",
                format!("{} MB (sampled during run)", fmt_thousands(self.peak_rss_mb)),
            ));
        }
        if self.format == "parquet" && self.compression != "zstd" {
            rows.push(("compression", self.compression.clone()));
        }
        if self.retries > 0 {
            rows.push(("retries", self.retries.to_string()));
        }
        if let Some(v) = self.validated {
            rows.push(("validated", if v { "pass".into() } else { "FAIL".into() }));
        }
        if let Some(sc) = self.schema_changed {
            rows.push((
                "schema",
                if sc { "CHANGED".into() } else { "unchanged".into() },
            ));
        }
        if let Some(q) = self.quality_passed {
            rows.push(("quality", if q { "pass".into() } else { "FAIL".into() }));
        }
        if let Some(reconciled) = self.reconciled {
            let src = self
                .source_count
                .map(fmt_thousands)
                .unwrap_or_else(|| "?".into());
            let exported = fmt_thousands(self.total_rows);
            let value = if reconciled {
                format!("MATCH ({exported}/{src})")
            } else {
                format!("MISMATCH (exported {exported} vs source {src})")
            };
            rows.push(("reconcile", value));
        }
        if let Some(err) = &self.error_message {
            rows.push(("error", err.clone()));
        }

        format_block(&self.export_name, &rows)
    }
}

/// Render a `── name ─────…─` header plus one indented `label:  value` line
/// per row, all joined into a single string ending with `\n`.
fn format_block(name: &str, rows: &[(&str, String)]) -> String {
    const HEADER_WIDTH: usize = 60;
    let label_w = rows.iter().map(|(l, _)| l.len()).max().unwrap_or(0);

    let prefix = format!("── {} ", name);
    let prefix_chars = prefix.chars().count();
    let dashes = HEADER_WIDTH.saturating_sub(prefix_chars);
    let mut out = String::with_capacity(HEADER_WIDTH * (rows.len() + 3));
    out.push('\n');
    out.push_str(&prefix);
    for _ in 0..dashes {
        out.push('─');
    }
    out.push('\n');
    for (label, value) in rows {
        // `label_w + 1` so the colon stays attached to the label and the
        // value column starts uniformly two spaces after it.
        out.push_str(&format!(
            "  {:<width$}  {}\n",
            format!("{label}:"),
            value,
            width = label_w + 1
        ));
    }
    out
}

fn fmt_duration_ms(ms: i64) -> String {
    if ms < 1000 {
        return format!("{}ms", ms);
    }
    let total_secs = ms / 1000;
    let h = total_secs / 3600;
    let m = (total_secs % 3600) / 60;
    let s_frac = (ms % 60_000) as f64 / 1000.0;
    if h > 0 {
        format!("{}h {:02}m {:04.1}s", h, m, s_frac)
    } else if m > 0 {
        format!("{}m {:04.1}s", m, s_frac)
    } else {
        format!("{:.1}s", ms as f64 / 1000.0)
    }
}

/// Format integers with a comma every three digits.  Negative values keep
/// their sign.  Used for rows / files / batch_size so large numbers stay
/// readable: `39_990_376` → `39,990,376`.
fn fmt_thousands(n: i64) -> String {
    let abs = n.unsigned_abs();
    let s = abs.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3 + 1);
    if n < 0 {
        out.push('-');
    }
    for (i, b) in bytes.iter().enumerate() {
        let from_end = bytes.len() - i;
        if i > 0 && from_end.is_multiple_of(3) {
            out.push(',');
        }
        out.push(*b as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fmt_thousands_handles_small_and_large() {
        assert_eq!(fmt_thousands(0), "0");
        assert_eq!(fmt_thousands(7), "7");
        assert_eq!(fmt_thousands(999), "999");
        assert_eq!(fmt_thousands(1_000), "1,000");
        assert_eq!(fmt_thousands(1_000_908), "1,000,908");
        assert_eq!(fmt_thousands(39_990_376), "39,990,376");
        assert_eq!(fmt_thousands(-1_234), "-1,234");
        assert_eq!(fmt_thousands(i64::MAX), "9,223,372,036,854,775,807");
    }

    #[test]
    fn fmt_duration_picks_unit() {
        assert_eq!(fmt_duration_ms(0), "0ms");
        assert_eq!(fmt_duration_ms(800), "800ms");
        assert_eq!(fmt_duration_ms(1_500), "1.5s");
        assert_eq!(fmt_duration_ms(68_400), "1m 08.4s");
        assert_eq!(fmt_duration_ms(3_725_300), "1h 02m 05.3s");
    }

    #[test]
    fn format_block_pads_labels_uniformly() {
        let rows = vec![
            ("run_id", "abc".to_string()),
            ("rows", "42".to_string()),
            ("compression", "zstd".to_string()),
        ];
        let out = format_block("orders", &rows);

        // Each value column starts at the same character position.
        let lines: Vec<&str> = out.lines().filter(|l| l.contains(':')).collect();
        assert_eq!(lines.len(), 3);
        let value_starts: Vec<usize> = lines
            .iter()
            .map(|l| l.find(':').unwrap() + l[l.find(':').unwrap()..].find(' ').unwrap())
            .collect();
        // The value (after `label:` plus padding plus two spaces) starts at the
        // same column for every row.  We verify by checking all lines have the
        // value substring at the same byte offset.
        let value_col = lines[0].rfind("abc").unwrap();
        assert_eq!(lines[1].rfind("42").unwrap(), value_col);
        assert_eq!(lines[2].rfind("zstd").unwrap(), value_col);
        // Sanity: silence unused.
        let _ = value_starts;
    }

    #[test]
    fn format_block_header_has_consistent_width() {
        let block_a = format_block("a", &[("rows", "1".into())]);
        let block_b = format_block("orders_table_xyz", &[("rows", "1".into())]);
        let header_a = block_a.lines().nth(1).unwrap();
        let header_b = block_b.lines().nth(1).unwrap();
        assert_eq!(
            header_a.chars().count(),
            header_b.chars().count(),
            "headers must be the same width regardless of name length: {:?} vs {:?}",
            header_a,
            header_b
        );
    }

    #[test]
    fn render_produces_a_single_string_with_trailing_newline() {
        use crate::plan::{
            CompressionType, DestinationConfig, DestinationType, ExtractionStrategy, FormatType,
            MetaColumns, ResolvedRunPlan,
        };
        use crate::tuning::SourceTuning;
        let plan = ResolvedRunPlan {
            export_name: "orders".into(),
            base_query: "SELECT 1".into(),
            strategy: ExtractionStrategy::Snapshot,
            format: FormatType::Parquet,
            compression: CompressionType::default(),
            compression_level: None,
            max_file_size_bytes: None,
            skip_empty: false,
            meta_columns: MetaColumns::default(),
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                bucket: None,
                prefix: None,
                path: Some("./out".into()),
                region: None,
                endpoint: None,
                credentials_file: None,
                access_key_env: None,
                secret_key_env: None,
                aws_profile: None,
                allow_anonymous: false,
            },
            quality: None,
            tuning: SourceTuning::from_config(None),
            tuning_profile_label: "balanced (default)".into(),
            validate: false,
            reconcile: false,
            resume: false,
            source: crate::config::SourceConfig {
                source_type: crate::config::SourceType::Postgres,
                url: Some("postgresql://localhost/test".into()),
                url_env: None,
                url_file: None,
                host: None,
                port: None,
                user: None,
                password: None,
                password_env: None,
                database: None,
                tuning: None,
                tls: None,
            },
        };
        let mut s = RunSummary::new(&plan);
        s.status = "success".into();
        s.total_rows = 1_000_908;
        s.files_produced = 11;
        s.bytes_written = 32 * 1024 * 1024 + 400 * 1024;
        s.duration_ms = 68_400;
        s.peak_rss_mb = 884;

        let block = s.render();
        assert!(block.starts_with('\n'), "block should start with a blank line");
        assert!(block.ends_with('\n'), "block should end with a newline");
        assert!(block.contains("── orders "));
        assert!(
            block.contains("1,000,908"),
            "rows should be formatted with thousands separator: {}",
            block
        );
        assert!(block.contains("1m 08.4s"), "duration formatting: {}", block);
        // No raw progress-bar bleed: header dashes still present, no carriage
        // returns or escape sequences.
        assert!(!block.contains('\r'));
    }
}
