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

use super::ipc::{self, ChildEvent};
use super::{format_bytes, multi_export_mode, strip_chunked_recovery_hint};
use crate::journal::{PlanSnapshot, RunEvent, RunJournal};
use crate::manifest::ManifestPart;
use crate::plan::ResolvedRunPlan;

/// Build a `PlanSnapshot` from a `ResolvedRunPlan`.
///
/// Lives here rather than on `journal` itself so that the journal module
/// stays free of plan/pipeline dependencies (avoids the state→pipeline cycle
/// we used to have via `state::journal_store`).
fn plan_snapshot_from(plan: &ResolvedRunPlan) -> PlanSnapshot {
    PlanSnapshot {
        export_name: plan.export_name.clone(),
        base_query: plan.base_query.clone(),
        strategy: plan.strategy.mode_label().to_string(),
        format: plan.format.label().to_string(),
        compression: plan.compression.label().to_string(),
        destination_type: plan.destination.destination_type.label().to_string(),
        tuning_profile: plan.tuning_profile_label.clone(),
        batch_size: plan.tuning.batch_size,
        validate: plan.validate,
        reconcile: plan.reconcile,
        resume: plan.resume,
    }
}

/// Context recorded when a run was launched via `rivet apply` rather than
/// `rivet run`.  Provides the audit trail for **F5**: which plan artifact
/// was applied, whether `--force` was passed, and which preflight checks
/// `--force` actually bypassed.
///
/// `None` on `RunSummary` means the run came from `rivet run` (no plan
/// artifact was applied).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ApplyContext {
    /// `plan_id` from the applied `PlanArtifact`.
    pub plan_id: String,
    /// Was `--force` passed to `rivet apply`?
    pub forced: bool,
    /// Names of preflight checks that `--force` actually overrode for this
    /// run.  Possible values: `"staleness"`, `"cursor_drift"`.  Empty when
    /// `forced` is true but no check actually had to be bypassed (the plan
    /// was fresh and the cursor matched).
    pub force_bypassed: Vec<String>,
}

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
    /// Incremented after each successful `dest.write()`. Non-zero means a previous
    /// attempt already committed data — retrying from the same cursor would duplicate rows.
    pub files_committed: usize,
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
    /// Postgres `pg_stat_database.temp_bytes` delta around the run. `None` for
    /// non-Postgres sources or when the snapshot probe failed (no admin perms
    /// not required — the view is readable by any role). When set and large,
    /// indicates cursor / sort spill to `pgsql_tmp/` — the safe action is to
    /// shrink `tuning.batch_size` or set `tuning.batch_size_memory_mb` below
    /// PG's `work_mem`.
    pub pg_temp_bytes_delta: Option<i64>,
    /// Human-readable parenthetical attached to `status: skipped` so the
    /// operator knows *why* there was nothing to export this run (e.g.
    /// `"no new rows since cursor 'updated_at'"`). Always `None` when
    /// `status != "skipped"`. Surfaced in the console summary card as
    /// `status: skipped (<reason>)`.
    pub skip_reason: Option<String>,
    /// Source COUNT(*) result for reconciliation (None = not requested or not applicable).
    pub source_count: Option<i64>,
    /// Whether reconciliation passed (Some(true) = match, Some(false) = mismatch, None = skipped).
    pub reconciled: Option<bool>,
    /// Committed parts accumulated during the run, in commit order.  Populated by
    /// `pipeline::manifest_writer::record_committed_part` at each `dest.write`
    /// site (ADR-0012 M1 — Parts Before Manifest).  Drained at finalize into a
    /// `RunManifest` by [`crate::pipeline::manifest_writer::write_manifest`].
    pub manifest_parts: Vec<ManifestPart>,
    /// xxh3 fingerprint of the dest-facing column schema for this run, in the
    /// canonical `xxh3:<16-hex>` form produced by [`crate::state::schema_fingerprint`].
    ///
    /// Recorded by [`crate::pipeline::manifest_writer::record_run_schema_fingerprint`]
    /// the first time the sink has resolved a schema (i.e. on the first batch
    /// of any chunk).  Idempotent within a run — the schema is identical across
    /// chunks, so later writes are no-ops.
    ///
    /// `finalize_manifest` reads this directly so the manifest's
    /// `schema_fingerprint` no longer depends on the per-export schema row
    /// happening to land in `state` before the manifest write.  The state
    /// lookup remains a fallback for resume scenarios where the summary was
    /// reconstructed without ever seeing a live schema.
    pub schema_fingerprint: Option<String>,
    /// Result of the manifest-aware `--validate` pass (ADR-0012 M5/M6,
    /// ADR-0013).  Populated by `pipeline::job::finalize_validate_manifest`
    /// after `finalize_manifest` succeeds; `None` when the run targeted a
    /// streaming destination, when `--validate` was not requested, or when
    /// the run failed before any manifest could be written.
    pub manifest_verification: Option<crate::pipeline::ManifestVerification>,
    /// Apply-time context (plan_id, --force usage, bypassed checks).
    /// `None` when the run came from `rivet run` rather than `rivet apply`.
    /// See [`ApplyContext`] and finding **F5** of the 0.7.5 audit.
    pub apply_context: Option<ApplyContext>,
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
        journal.record(RunEvent::PlanResolved(plan_snapshot_from(plan)));

        ipc::emit_event(&ChildEvent::Started {
            export_name: plan.export_name.clone(),
            run_id: run_id.clone(),
            mode: plan.strategy.mode_label().to_string(),
            tuning_profile: plan.tuning_profile_label.clone(),
            batch_size: plan.tuning.batch_size,
        });

        Self {
            run_id,
            export_name: plan.export_name.clone(),
            status: "running".into(),
            total_rows: 0,
            files_produced: 0,
            bytes_written: 0,
            files_committed: 0,
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
            pg_temp_bytes_delta: None,
            skip_reason: None,
            source_count: None,
            reconciled: None,
            manifest_parts: Vec::new(),
            schema_fingerprint: None,
            manifest_verification: None,
            apply_context: None,
            journal,
        }
    }

    /// One canonical builder for tests across the crate + integration suite.
    ///
    /// Every field is filled with a sensible default; callers tweak only what
    /// the test cares about via the chainable setters below.  This replaces
    /// the seven copies of `stub_summary` / `fresh_summary` / `make_summary`
    /// / `dummy_summary` / `empty_summary` that used to live in `notify.rs`,
    /// `report.rs`, `chunked/{exec,mod}.rs`, `manifest_writer.rs`, and the
    /// two integration-test files.  When `RunSummary` gains a field, it is
    /// updated here once instead of across nine sites.
    ///
    /// Available outside `pipeline` (`pub` not `pub(crate)`) so integration
    /// tests in `tests/` can use it via `RunSummary::stub_for_testing(...)`.
    /// The `_for_testing` suffix is the convention from elsewhere in the
    /// codebase (`destination_for_tests`, etc.) — production code should
    /// never call it.
    ///
    /// `#[allow(dead_code)]` on each helper because the bin target's
    /// dead-code analysis doesn't see uses from integration tests in `tests/`.
    /// The lib's unit tests + the integration suite together exercise every
    /// helper; the attribute is a no-op for them.
    #[doc(hidden)]
    #[allow(dead_code)]
    pub fn stub_for_testing(run_id: impl Into<String>, export_name: impl Into<String>) -> Self {
        let run_id = run_id.into();
        let export_name = export_name.into();
        let journal = RunJournal::new(&run_id, &export_name);
        Self {
            run_id,
            export_name,
            status: "running".into(),
            total_rows: 0,
            files_produced: 0,
            bytes_written: 0,
            files_committed: 0,
            duration_ms: 0,
            peak_rss_mb: 0,
            retries: 0,
            validated: None,
            schema_changed: None,
            quality_passed: None,
            error_message: None,
            tuning_profile: "balanced".into(),
            batch_size: 1000,
            batch_size_memory_mb: None,
            format: "parquet".into(),
            mode: "snapshot".into(),
            compression: "zstd".into(),
            pg_temp_bytes_delta: None,
            skip_reason: None,
            source_count: None,
            reconciled: None,
            manifest_parts: Vec::new(),
            schema_fingerprint: None,
            manifest_verification: None,
            apply_context: None,
            journal,
        }
    }

    /// Test-only chainable setter for the run's status field.
    ///
    /// Used to build success/failed/running variants without re-listing every
    /// field.  Keeps the journal in sync: terminal statuses get a matching
    /// `RunCompleted` event recorded so consumers reading
    /// `journal.final_outcome()` see the right shape.
    #[doc(hidden)]
    #[allow(dead_code)]
    pub fn with_status(mut self, status: impl Into<String>) -> Self {
        let s = status.into();
        if (s == "success" || s == "failed") && self.journal.final_outcome().is_none() {
            self.journal.record(RunEvent::RunCompleted {
                status: s.clone(),
                error_message: self.error_message.clone(),
                duration_ms: self.duration_ms,
            });
        }
        self.status = s;
        self
    }

    /// Test-only setter — record `files_committed` so resume-hint logic
    /// (`pipeline::report`) can detect the "failed run with committed files"
    /// path that produces a resume command.
    #[doc(hidden)]
    #[allow(dead_code)]
    pub fn with_files_committed(mut self, n: usize) -> Self {
        self.files_committed = n;
        self
    }

    /// Test-only setter — replace the recorded manifest parts (and adjust
    /// `total_rows` / `bytes_written` / `files_produced` to keep them
    /// consistent with the parts list, the way real production code does).
    #[doc(hidden)]
    #[allow(dead_code)]
    pub fn with_manifest_parts(mut self, parts: Vec<crate::manifest::ManifestPart>) -> Self {
        self.total_rows = parts.iter().map(|p| p.rows).sum();
        self.bytes_written = parts.iter().map(|p| p.size_bytes).sum();
        self.files_produced = parts.len();
        self.files_committed = parts.len();
        self.manifest_parts = parts;
        self
    }

    /// Test-only setter — error_message + (optionally) status.  Common
    /// shape for the "failed-run" fixtures that populated 4+ existing
    /// stubs.
    #[doc(hidden)]
    #[allow(dead_code)]
    pub fn with_error(mut self, msg: impl Into<String>) -> Self {
        self.error_message = Some(msg.into());
        self
    }

    /// Test-only setter — record a PlanResolved event in the journal so
    /// downstream observability paths (`journal.plan_snapshot()`,
    /// `RunReport::from_summary` plan_origin lookup) see the same shape
    /// they would on a real run.
    #[doc(hidden)]
    #[allow(dead_code)]
    pub fn with_plan_snapshot(mut self, snap: PlanSnapshot) -> Self {
        self.journal.record(RunEvent::PlanResolved(snap));
        self
    }

    pub(super) fn print(&self) {
        // Capturing mode (IPC child or in-process channel): emit a
        // `Finished` event and let the unified UI thread render the card.
        // No stderr block here — the renderer owns the screen.
        if ipc::capturing_events() {
            ipc::emit_event(&ChildEvent::Finished {
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

        self.print_stderr_block();
    }

    /// Write the per-export summary block to stderr, bypassing IPC capture.
    /// Used after the in-process card UI finishes on single-export sequential
    /// runs so operators still get the detailed `── name ──` block below the
    /// live card line.
    pub(super) fn print_stderr_block(&self) {
        let block = if multi_export_mode() {
            self.render_compact()
        } else {
            // Render the whole block into a single buffer so the call site
            // emits one `write_all` to stderr.  Without this, parallel
            // exports could interleave individual lines from different
            // `RunSummary::print()` calls — visible as garbled blocks in
            // `--parallel-exports` runs.
            self.render().trim_end_matches('\n').to_string()
        };

        use std::io::Write;
        let mut buf = block;
        buf.push('\n');
        let stderr = std::io::stderr();
        let mut handle = stderr.lock();
        let _ = handle.write_all(buf.as_bytes());
        let _ = handle.flush();
    }

    /// Compact one-line summary used when several exports run in the same
    /// invocation.  Mirrors the parent_ui card line so `--parallel-exports`
    /// (threads), sequential, and `--parallel-export-processes` (processes)
    /// produce visually consistent per-export rows.
    fn render_compact(&self) -> String {
        const NAME_COL: usize = 22;
        const MODE_COL: usize = 8;
        let icon = match self.status.as_str() {
            "success" => "✓",
            "failed" => "✗",
            _ => "•",
        };
        let body = if self.status == "failed" {
            let err = self
                .error_message
                .as_deref()
                .unwrap_or("(no error message recorded)");
            let (cause, _) = strip_chunked_recovery_hint(err);
            // Collapse multi-line / extremely long errors so the compact
            // line stays one row tall.  Full payload lives in the stderr
            // log above the run summary.
            compact_error(cause)
        } else {
            let rss = if self.peak_rss_mb > 0 {
                format!("  RSS {} MB", fmt_thousands(self.peak_rss_mb))
            } else {
                String::new()
            };
            format!(
                "{} rows  {} files  {}  {}{}",
                fmt_thousands(self.total_rows),
                fmt_thousands(self.files_produced as i64),
                format_bytes(self.bytes_written),
                fmt_duration_ms(self.duration_ms),
                rss
            )
        };
        format!(
            "{} {:<name$}  {:<mode$}  {}",
            icon,
            self.export_name,
            self.mode,
            body,
            name = NAME_COL,
            mode = MODE_COL,
        )
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
        let status_value = match (&self.status, &self.skip_reason) {
            (s, Some(reason)) if s == "skipped" => format!("{s} ({reason})"),
            (s, _) => s.clone(),
        };
        rows.push(("status", status_value));

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
        // On a 0-new incremental run, `0 rows  0 files` alone hides *why*
        // nothing moved. Surface the cursor position as its own line so the
        // operator sees the incremental boundary held — not just an empty run.
        // The cursor *value* lives in the runner and isn't plumbed onto the
        // summary yet, so this reports the column-level position derived from
        // `skip_reason` (the value is a follow-up once it reaches here).
        if let Some(pos) = incremental_position_line(self.skip_reason.as_deref()) {
            rows.push(("cursor", pos));
        } else if let Some(window) = time_window_skip_line(&self.mode, self.skip_reason.as_deref())
        {
            // A time_window run that returned 0 rows reports the generic
            // `"source returned 0 rows"` skip (`cursor_column()` is `None` for
            // this strategy), so the incremental branch above never fires. Add
            // an explicit `window:` line so the operator can tell an *empty
            // window* apart from a *wrong column / window* — otherwise the
            // summary is indistinguishable from any other empty run. The window
            // column / days / computed bound are not plumbed onto `RunSummary`,
            // so this is the strategy-level signal reachable here (the concrete
            // bound is a follow-up once the runner records it on the summary).
            rows.push(("window", window));
        }
        if self.bytes_written > 0 {
            rows.push(("bytes", format_bytes(self.bytes_written)));
        }
        rows.push(("duration", fmt_duration_ms(self.duration_ms)));

        if self.peak_rss_mb > 0 {
            rows.push((
                "peak RSS",
                format!(
                    "{} MB (sampled during run)",
                    fmt_thousands(self.peak_rss_mb)
                ),
            ));
        }
        if let Some(temp) = self.pg_temp_bytes_delta {
            // Skip when the cluster reported no delta — only chatter when there
            // was actual spill. > 100 MB is annotated with a tuning hint;
            // smaller numbers are reported as plain info.
            if temp > 0 {
                let temp_mb = temp as f64 / (1024.0 * 1024.0);
                let label = if temp > 100 * 1024 * 1024 {
                    format!(
                        "{:.1} MB ⚠ shrink tuning.batch_size or set batch_size_memory_mb",
                        temp_mb
                    )
                } else {
                    format!("{:.1} MB", temp_mb)
                };
                rows.push(("pg temp spill", label));
            }
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
                if sc {
                    "CHANGED".into()
                } else {
                    "unchanged".into()
                },
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
            // Preserve the error's own line structure: the detailed block has
            // room for it and `format_block` now indents continuation lines
            // under the value column. Flattening to `"; "`-joined text (the
            // compact one-liner's job) made multi-line errors — e.g. a quality
            // failure's `failed:\n  - <check>\n  Fix …` — hard to read here.
            rows.push(("error", err.trim_end().to_string()));
        }

        format_block(&self.export_name, &rows)
    }

    /// Sanity-check the post-run summary ↔ manifest_parts coherence. Used as
    /// a `debug_assert!`-style runtime gate from `finalize_manifest` so any
    /// future runner that bumps `bytes_written` / `files_committed` /
    /// `files_produced` without going through `pipeline::commit::record_part`
    /// is caught the moment it finishes a real export. Compiled out in
    /// release builds via the `cfg!(debug_assertions)` guard at the call site.
    ///
    /// **Resume-safe inequalities only**: on resume, `manifest_parts` carries
    /// prior runs' parts via `chunked::resume_m8` while `bytes_written` /
    /// `files_committed` reflect only the current invocation — so strict
    /// equality is wrong across resume boundaries. Strict equality on the
    /// non-resume path is pinned by `pipeline::commit::tests`.
    ///
    /// Returns `Ok(())` when the summary satisfies the invariants, else an
    /// `Err(String)` naming which one was violated and by how much.
    pub fn check_post_run_invariants(&self) -> Result<(), String> {
        let parts_bytes: u64 = self.manifest_parts.iter().map(|p| p.size_bytes).sum();

        if self.files_committed > self.manifest_parts.len() {
            return Err(format!(
                "summary.files_committed ({}) > manifest_parts.len() ({}) — \
                 a runner bumped files_committed without commit::record_part",
                self.files_committed,
                self.manifest_parts.len()
            ));
        }
        if self.files_produced > self.manifest_parts.len() {
            return Err(format!(
                "summary.files_produced ({}) > manifest_parts.len() ({}) — \
                 a runner bumped files_produced without commit::record_part",
                self.files_produced,
                self.manifest_parts.len()
            ));
        }
        if self.bytes_written > parts_bytes {
            return Err(format!(
                "summary.bytes_written ({}) > sum(manifest_parts.size_bytes) ({}) — \
                 a runner bumped bytes_written without commit::record_part",
                self.bytes_written, parts_bytes
            ));
        }
        if self.status == "success" && self.files_committed > 0 && self.manifest_parts.is_empty() {
            return Err(format!(
                "success run with files_committed={} has empty manifest_parts — \
                 cloud manifest (ADR-0012 M1) would ship with no part list \
                 (this is the gap parallel_checkpoint had before commit e9b0796)",
                self.files_committed
            ));
        }
        // Invariant audit gap #1, weak form: a successful run that produced
        // rows for THIS invocation must have committed at least one file.
        // The strict form ("rows_written <= rows_read") would require a
        // separate source-side row counter we do not track, and concurrent
        // INSERTs on the source (live_oltp_load) make a source_count
        // comparison brittle. This weak form catches a fabrication shape:
        // total_rows accumulated but nothing reached the destination — a
        // runner that fetched and silently dropped rows produces exactly
        // this signature. Resume-safe: total_rows reflects only this
        // invocation, so a resume with no work to do legitimately ends at
        // total_rows=0 / files_committed=0 and the guard does not fire.
        if self.status == "success" && self.total_rows > 0 && self.files_committed == 0 {
            return Err(format!(
                "summary.total_rows={} but files_committed=0 — rows extracted from \
                 source but no files committed (no output reached the destination)",
                self.total_rows
            ));
        }
        Ok(())
    }
}

/// Reduce a possibly-multi-line execution error to a single-line, bounded-
/// length cause suitable for the per-export summary block and the compact
/// one-liner.  Keeps the user-actionable bit and drops noisy diagnostic
/// payloads (long URLs, query strings, repeated chunk errors).
///
/// Recognised shapes:
/// - `parallel checkpoint worker errors:\nchunk N: <msg>\nchunk M: <msg>` →
///   `parallel checkpoint workers failed: K chunk(s) (chunk N: <truncated>)`.
///   The full per-chunk detail is already in stderr logs.
/// - Generic multi-line: newlines are replaced with `; ` and the result is
///   clamped to 240 characters with an ellipsis.
fn compact_error(raw: &str) -> String {
    const MAX_CHARS: usize = 240;
    if let Some(summary) = summarize_parallel_chunk_errors(raw) {
        return clamp_chars(&summary, MAX_CHARS);
    }
    let collapsed: String = raw
        .lines()
        .map(str::trim_end)
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("; ");
    clamp_chars(&collapsed, MAX_CHARS)
}

/// Derive the summary block's `cursor:` line from a `skip_reason`.
///
/// `skip_reason` for an incremental no-op is `"no new rows since cursor
/// '<col>'"` (set by the runner); we lift the column out and report the
/// position as held. Returns `None` for the non-cursor `"source returned 0
/// rows"` skip and for `None` (a run that actually produced rows). The cursor
/// *value* isn't carried on the summary yet, so this is column-level only.
fn incremental_position_line(skip_reason: Option<&str>) -> Option<String> {
    let col = skip_reason?
        .strip_prefix("no new rows since cursor '")?
        .strip_suffix('\'')?;
    Some(format!("'{col}' unchanged (no new rows this run)"))
}

/// Derive the summary block's `window:` line for a time_window run that
/// returned nothing.
///
/// A `TimeWindow` strategy has no cursor column, so a 0-row run reports the
/// generic `"source returned 0 rows"` skip — the `incremental_position_line`
/// branch never fires and, without this line, an empty time window looks
/// identical to any other empty export. Surfacing it lets the operator tell an
/// *empty window* (data simply outside the rolling range) from a *misconfigured
/// window* (wrong `time_column` / `days_window`).
///
/// Keyed on `mode == "timewindow"` (set from `ExtractionStrategy::mode_label`)
/// plus a set skip reason, so it only fires on a skipped time_window run and
/// never on incremental/snapshot/chunked/keyset. The window column, days, and
/// computed lower bound are not carried on `RunSummary`, so this reports the
/// strategy-level fact and where to look — the concrete bound is a follow-up
/// once the runner records it onto the summary.
fn time_window_skip_line(mode: &str, skip_reason: Option<&str>) -> Option<String> {
    skip_reason?;
    if mode != "timewindow" {
        return None;
    }
    Some("rolling time window matched no rows — check `time_column`/`days_window`".to_string())
}

fn summarize_parallel_chunk_errors(raw: &str) -> Option<String> {
    let header_pos = raw.find("parallel checkpoint worker errors:")?;
    let prefix = raw[..header_pos].trim_end_matches(": ").trim_end();
    let tail = &raw[header_pos + "parallel checkpoint worker errors:".len()..];

    let chunk_lines: Vec<&str> = tail
        .lines()
        .map(str::trim)
        .filter(|l| l.starts_with("chunk "))
        .collect();
    if chunk_lines.is_empty() {
        return None;
    }
    let first_chunk_full = chunk_lines[0];
    // Truncate the example chunk message; the URL/payload is in stderr logs.
    let first_chunk_short = clamp_chars(first_chunk_full, 140);
    let prefix = if prefix.is_empty() {
        String::new()
    } else {
        format!("{}: ", prefix)
    };
    Some(format!(
        "{}parallel checkpoint workers failed: {} chunk(s) ({}); see stderr for full payloads",
        prefix,
        chunk_lines.len(),
        first_chunk_short
    ))
}

fn clamp_chars(s: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }
    if s.chars().count() <= max_chars {
        return s.to_string();
    }
    let keep = max_chars.saturating_sub(1);
    let mut out: String = s.chars().take(keep).collect();
    out.push('…');
    out
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
    // Continuation lines of a multi-line value (e.g. the multi-line `error`
    // row) are indented to align under the value column, so block-shaped
    // messages stay readable instead of being flattened onto one line.
    let value_indent = " ".repeat(2 + (label_w + 1) + 2);
    for (label, value) in rows {
        // `label_w + 1` so the colon stays attached to the label and the
        // value column starts uniformly two spaces after it.
        let mut lines = value.split('\n');
        let first = lines.next().unwrap_or("");
        out.push_str(&format!(
            "  {:<width$}  {}\n",
            format!("{label}:"),
            first,
            width = label_w + 1
        ));
        for cont in lines {
            out.push_str(&value_indent);
            out.push_str(cont);
            out.push('\n');
        }
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
                path: Some("./out".into()),
                ..Default::default()
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
                environment: None,
                tuning: None,
                tls: None,
            },
            column_overrides: Default::default(),
            verify: crate::config::VerifyMode::Size,
            schema_drift_policy: Default::default(),
            shape_drift_warn_factor: 2.0,
            parquet: None,
        };
        let mut s = RunSummary::new(&plan);
        s.status = "success".into();
        s.total_rows = 1_000_908;
        s.files_produced = 11;
        s.bytes_written = 32 * 1024 * 1024 + 400 * 1024;
        s.duration_ms = 68_400;
        s.peak_rss_mb = 884;

        let block = s.render();
        assert!(
            block.starts_with('\n'),
            "block should start with a blank line"
        );
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

        // Compact one-liner used in multi-export runs.
        let line = s.render_compact();
        assert!(line.starts_with("✓ "), "success icon present: {:?}", line);
        assert!(line.contains("orders"), "export name present: {:?}", line);
        assert!(line.contains("1,000,908 rows"), "rows present: {:?}", line);
        assert!(line.contains("32.4 MB"), "bytes present: {:?}", line);
        assert!(line.contains("1m 08.4s"), "duration present: {:?}", line);
        assert!(line.contains("RSS 884 MB"), "rss present: {:?}", line);
        assert!(!line.contains('\n'), "single line: {:?}", line);
    }

    #[test]
    fn compact_error_summarises_parallel_chunk_errors() {
        let raw = "export 'page_views': parallel checkpoint worker errors:\n\
                   chunk 4: Unexpected (temporary) at write, context: { url: https://storage.googleapis.com/rivet_data_test/exports%2Fpage_views%2Fpage_views_20260430_202442_chunk4.parquet?partNumber=1&uploadId=ABPnzm7RqplA, called: http_util::Client::send } => send http request, source: error sending request: client error (SendRequest): dispatch task is gone\n\
                   chunk 5: Unexpected (temporary) at write, context: { url: https://storage.googleapis.com/rivet_data_test/exports%2Fpage_views%2Fpage_views_20260430_202443_chunk5.parquet?partNumber=1&uploadId=ABPnzm6q, called: http_util::Client::send } => send http request, source: dispatch task is gone";
        let out = compact_error(raw);
        assert!(
            out.contains("2 chunk(s)"),
            "should report number of failed chunks: {:?}",
            out
        );
        assert!(
            out.starts_with("export 'page_views': parallel checkpoint workers failed:"),
            "should keep export prefix and use compact phrasing: {:?}",
            out
        );
        assert!(
            out.contains("chunk 4:"),
            "should include the first chunk as an example: {:?}",
            out
        );
        assert!(!out.contains('\n'), "single line output: {:?}", out);
        assert!(
            out.chars().count() <= 240,
            "must be clamped to <=240 chars, got {}: {:?}",
            out.chars().count(),
            out
        );
    }

    #[test]
    fn compact_error_collapses_generic_multiline() {
        let raw = "first line of trouble\nsecond line with detail\n\nthird line\n";
        let out = compact_error(raw);
        assert_eq!(
            out, "first line of trouble; second line with detail; third line",
            "newlines should collapse to '; ' and blanks dropped"
        );
    }

    #[test]
    fn compact_error_clamps_excessively_long_lines() {
        let raw = "x".repeat(1_000);
        let out = compact_error(&raw);
        assert_eq!(out.chars().count(), 240);
        assert!(out.ends_with('…'));
    }

    #[test]
    fn render_compact_strips_chunked_recovery_hint_for_failed() {
        use crate::plan::{
            CompressionType, DestinationConfig, DestinationType, ExtractionStrategy, FormatType,
            MetaColumns, ResolvedRunPlan,
        };
        use crate::tuning::SourceTuning;
        let plan = ResolvedRunPlan {
            export_name: "events".into(),
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
                path: Some("./out".into()),
                ..Default::default()
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
                environment: None,
                tuning: None,
                tls: None,
            },
            column_overrides: Default::default(),
            verify: crate::config::VerifyMode::Size,
            schema_drift_policy: Default::default(),
            shape_drift_warn_factor: 2.0,
            parquet: None,
        };
        let mut s = RunSummary::new(&plan);
        s.status = "failed".into();
        s.error_message = Some(
            "export 'events': --resume but no in-progress chunk checkpoint; \
             run without --resume first or `rivet state reset-chunks --config x.yaml --export events`"
                .to_string(),
        );

        let line = s.render_compact();
        assert!(line.starts_with("✗ "), "failure icon: {:?}", line);
        assert!(line.contains("events"), "name present: {:?}", line);
        assert!(
            line.contains("--resume but no in-progress chunk checkpoint"),
            "cause kept: {:?}",
            line
        );
        assert!(
            !line.contains("rivet state reset-chunks"),
            "recovery hint should be stripped from per-export line: {:?}",
            line
        );
        assert!(!line.contains('\n'), "single line: {:?}", line);
    }

    fn plan_for(export_name: &str) -> crate::plan::ResolvedRunPlan {
        use crate::plan::{
            CompressionType, DestinationConfig, DestinationType, ExtractionStrategy, FormatType,
            MetaColumns, ResolvedRunPlan,
        };
        use crate::tuning::SourceTuning;
        ResolvedRunPlan {
            export_name: export_name.into(),
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
                path: Some("./out".into()),
                ..Default::default()
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
                environment: None,
                tuning: None,
                tls: None,
            },
            column_overrides: Default::default(),
            verify: crate::config::VerifyMode::Size,
            schema_drift_policy: Default::default(),
            shape_drift_warn_factor: 2.0,
            parquet: None,
        }
    }

    #[test]
    fn render_preserves_multiline_error_block() {
        // L19: a multi-line error (a quality failure here) must stay multi-line
        // in the detailed single-export block — not collapsed to `"; "`-joined
        // text the way the compact one-liner does.
        let mut s = RunSummary::new(&plan_for("orders"));
        s.status = "failed".into();
        s.error_message = Some(
            "export 'orders': 1 quality check(s) failed:\n  \
             - row_count 10 below minimum 999999\n  \
             Fix the source data, or adjust the thresholds under `quality:` in your config."
                .to_string(),
        );

        let block = s.render();
        // The collapsed form joined lines with `"; "` — assert that flattening
        // is gone and the original newline structure survives.
        assert!(
            !block.contains("failed:;"),
            "error must not be '; '-flattened in the detailed block: {block}"
        );
        assert!(
            block.contains("- row_count 10 below minimum 999999"),
            "failing check line present: {block}"
        );
        // Each part of the multi-line error lands on its own line.
        let err_lines: Vec<&str> = block
            .lines()
            .filter(|l| {
                l.contains("quality check(s) failed")
                    || l.contains("row_count 10 below minimum")
                    || l.contains("Fix the source data")
            })
            .collect();
        assert_eq!(
            err_lines.len(),
            3,
            "all three error lines should render on separate lines: {block}"
        );
        // Continuation lines are indented under the value column, not at col 0.
        for l in &err_lines {
            assert!(l.starts_with(' '), "error line should be indented: {l:?}");
        }
    }

    #[test]
    fn render_surfaces_cursor_position_on_zero_new_incremental() {
        // L27: a 0-new incremental run shows `0 rows  0 files`; without a
        // cursor line the operator can't tell the boundary held. Assert the
        // dedicated `cursor:` line appears, derived from `skip_reason`.
        let mut s = RunSummary::new(&plan_for("orders"));
        s.status = "skipped".into();
        s.skip_reason = Some("no new rows since cursor 'updated_at'".into());

        let block = s.render();
        let cursor_line = block
            .lines()
            .find(|l| l.trim_start().starts_with("cursor:"))
            .unwrap_or_else(|| panic!("expected a cursor: line in block: {block}"));
        assert!(
            cursor_line.contains("'updated_at'"),
            "cursor line names the column: {cursor_line:?}"
        );
        assert!(
            cursor_line.contains("unchanged"),
            "cursor line reports the position held: {cursor_line:?}"
        );
    }

    #[test]
    fn incremental_position_line_only_for_cursor_skips() {
        // The non-cursor 0-row skip and the no-skip case produce no cursor line.
        assert_eq!(
            incremental_position_line(Some("no new rows since cursor 'ts'")),
            Some("'ts' unchanged (no new rows this run)".into())
        );
        assert_eq!(
            incremental_position_line(Some("source returned 0 rows")),
            None
        );
        assert_eq!(incremental_position_line(None), None);
    }

    #[test]
    fn render_surfaces_window_position_on_zero_row_time_window() {
        // L27 (time_window arm): a 0-row time_window run reports the generic
        // `"source returned 0 rows"` skip (the strategy has no cursor column),
        // so the `cursor:` branch never fires. Without a `window:` line the
        // operator can't tell an empty window from a wrong column/window —
        // assert the dedicated `window:` line appears for this mode.
        let mut s = RunSummary::new(&plan_for("events"));
        s.status = "skipped".into();
        s.mode = "timewindow".into();
        s.skip_reason = Some("source returned 0 rows".into());

        let block = s.render();
        let window_line = block
            .lines()
            .find(|l| l.trim_start().starts_with("window:"))
            .unwrap_or_else(|| panic!("expected a window: line in block: {block}"));
        assert!(
            window_line.contains("matched no rows"),
            "window line reports the empty window: {window_line:?}"
        );
        assert!(
            window_line.contains("time_column") && window_line.contains("days_window"),
            "window line points at the window config to check: {window_line:?}"
        );
        // The generic 0-row skip must not also produce a `cursor:` line.
        assert!(
            !block.lines().any(|l| l.trim_start().starts_with("cursor:")),
            "no cursor line for a non-cursor strategy: {block}"
        );
    }

    #[test]
    fn time_window_skip_line_only_for_skipped_time_window() {
        // Fires only when the run skipped AND the strategy is time_window.
        assert_eq!(
            time_window_skip_line("timewindow", Some("source returned 0 rows")),
            Some("rolling time window matched no rows — check `time_column`/`days_window`".into())
        );
        // Wrong mode → no window line (incremental/snapshot handle their own).
        assert_eq!(
            time_window_skip_line("incremental", Some("source returned 0 rows")),
            None
        );
        assert_eq!(
            time_window_skip_line("full", Some("source returned 0 rows")),
            None
        );
        // A time_window run that produced rows (no skip) gets no window line.
        assert_eq!(time_window_skip_line("timewindow", None), None);
    }
}
