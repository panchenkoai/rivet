//! **Layer: Observability**
//!
//! CLI display commands for state, metrics, files, chunk checkpoints, and journal history.
//! Reads from the state stores and formats output — no execution or persistence writes.

use super::format_bytes;
use crate::config::Config;
use crate::error::Result;
use crate::journal::RunEvent;
use crate::state::StateStore;

/// Validate the `--config` path BEFORE opening the state store.
///
/// Inspect commands (`state show/files/chunks/progression`, `metrics`,
/// `journal`) use `config_path` only to *locate* the SQLite `.rivet_state.db`
/// next to it; they never parse the YAML. So a missing or garbage path used to
/// silently open an empty/foreign DB and exit 0 (and `StateStore::open` would
/// materialize a fresh `.rivet_state.db` beside the phantom path). Loading the
/// config first turns a bad path into a clear non-zero error — exactly as
/// `run`/`check` already do — and avoids littering a state DB. The parsed
/// config is returned so callers that also need the declared export names
/// (e.g. `reset-chunks`) reuse the same load.
fn require_config(config_path: &str) -> Result<Config> {
    Config::load(config_path)
}

/// Bail if `export_name` is not declared in `config`, listing the known names.
///
/// Inspect commands (`metrics`, `journal`) that filter by a single export must
/// not let a typo'd `--export` look like "this export simply has not run yet" —
/// both used to print the same empty-state line. With the config now loaded, an
/// unknown name is a clear error that points at the declared exports.
fn require_known_export(config: &Config, config_path: &str, export_name: &str) -> Result<()> {
    if config.exports.iter().any(|e| e.name == export_name) {
        return Ok(());
    }
    let known: Vec<&str> = config.exports.iter().map(|e| e.name.as_str()).collect();
    anyhow::bail!(
        "export '{}' is not defined in '{}'.\n  Known exports: {}",
        export_name,
        config_path,
        if known.is_empty() {
            "(none defined)".to_string()
        } else {
            known.join(", ")
        },
    );
}

pub fn show_state(config_path: &str) -> Result<()> {
    require_config(config_path)?;
    let state = StateStore::open(config_path)?;
    let states = state.list_all()?;
    if states.is_empty() {
        // `state.list_all()` returns only incremental-cursor rows. Chunked
        // and full runs land in different tables (`export_metrics`,
        // `export_files`, `chunk_state`), so an empty cursor list does not
        // mean "this config never ran". Distinguish the two cases:
        //   - never ran    → tell the operator to run first
        //   - ran chunked  → point at `rivet metrics` / `rivet state files`
        // Anything else here is misleading ("No state" after a successful
        // chunked run sounds like data loss).
        let any_run = state
            .get_metrics(None, 1)
            .map(|m| !m.is_empty())
            .unwrap_or(false);
        if any_run {
            println!(
                "No incremental cursor recorded yet.\n  \
                 This command shows incremental-mode cursors only.\n  \
                 For chunked / full runs, see:\n  \
                 • rivet metrics      — per-run history (status, rows, duration)\n  \
                 • rivet state files  — every produced file with row count + size"
            );
        } else {
            println!(
                "No exports have been run yet.\n  \
                 Run `rivet run --config {}` first, then try `rivet state show` again.",
                config_path
            );
        }
        return Ok(());
    }
    println!("{:<30} {:<40} LAST RUN", "EXPORT", "LAST CURSOR");
    println!("{}", "-".repeat(90));
    for s in &states {
        println!(
            "{:<30} {:<40} {}",
            s.export_name,
            s.last_cursor_value.as_deref().unwrap_or("-"),
            s.last_run_at.as_deref().unwrap_or("-"),
        );
    }
    Ok(())
}

/// Epic G / ADR-0008 — explicit committed / verified boundaries per export.
pub fn show_progression(config_path: &str, export_name: Option<&str>) -> Result<()> {
    require_config(config_path)?;
    let state = StateStore::open(config_path)?;
    let entries = match export_name {
        Some(name) => vec![state.get_progression(name)?],
        None => state.list_progression()?,
    };
    let has_any = entries
        .iter()
        .any(|p| p.committed.is_some() || p.verified.is_some());
    if !has_any {
        println!("No progression boundaries recorded yet.");
        return Ok(());
    }

    println!(
        "{:<30} {:<12} {:<30} {:<25} {:<12} {:<30}",
        "EXPORT", "COMM MODE", "COMMITTED", "COMMITTED AT", "VERI MODE", "VERIFIED"
    );
    println!("{}", "-".repeat(145));
    for p in &entries {
        let (c_mode, c_val, c_at) = match &p.committed {
            Some(b) => (
                b.strategy.as_str().to_string(),
                boundary_value(b),
                b.at.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
            ),
            None => ("-".into(), "-".into(), "-".into()),
        };
        let (v_mode, v_val) = match &p.verified {
            Some(b) => (b.strategy.as_str().to_string(), boundary_value(b)),
            None => ("-".into(), "-".into()),
        };
        println!(
            "{:<30} {:<12} {:<30} {:<25} {:<12} {:<30}",
            p.export_name, c_mode, c_val, c_at, v_mode, v_val
        );
    }
    Ok(())
}

fn boundary_value(b: &crate::state::Boundary) -> String {
    if let Some(c) = &b.cursor {
        c.clone()
    } else if let Some(idx) = b.chunk_index {
        format!("chunk #{idx}")
    } else {
        "-".into()
    }
}

pub fn reset_state(config_path: &str, export_name: &str) -> Result<()> {
    // Validate the export name against the config BEFORE touching state, so a
    // typo (`--export pa_audi` instead of `pa_audit`) produces a hint with the
    // declared names instead of silently DELETE-ing zero rows and printing
    // "State reset for export 'pa_audi'" as if it worked.  Without this guard
    // a stray `--export <unknown>` looked like success but did nothing — a
    // genuine ops footgun under "I reset it, why did the cursor not move".
    let config = crate::config::Config::load(config_path)?;
    if !config.exports.iter().any(|e| e.name == export_name) {
        let known: Vec<String> = config.exports.iter().map(|e| e.name.clone()).collect();
        anyhow::bail!(
            "export '{}' not found in config '{}'.\n  Known exports: {}\n  Hint: check the spelling, or run `rivet state show -c {}` to see what is currently tracked.",
            export_name,
            config_path,
            if known.is_empty() {
                "(none defined)".to_string()
            } else {
                known.join(", ")
            },
            config_path,
        );
    }
    let state = StateStore::open(config_path)?;
    state.reset(export_name)?;
    println!("State reset for export '{}'", export_name);
    Ok(())
}

pub fn show_files(
    config_path: &str,
    export_name: Option<&str>,
    limit: usize,
    json: bool,
) -> Result<()> {
    require_config(config_path)?;
    let state = StateStore::open(config_path)?;
    let files = state.get_files(export_name, limit)?;
    if json {
        // FileRecord is a stable inspect row — serialize it directly. Empty → `[]`
        // (valid JSON) so a CI completeness check never special-cases.
        println!("{}", serde_json::to_string_pretty(&files)?);
        return Ok(());
    }
    if files.is_empty() {
        println!("No files recorded yet.");
        return Ok(());
    }
    // run_ids are `{export}_{%Y%m%dT%H%M%S%3f}` (timestamp alone is 18 chars),
    // so a column narrower than the longest export name + 19 wraps and breaks
    // alignment. 40 fits the common case; longer names overflow gracefully.
    println!(
        "{:<40} {:<40} {:>8} {:>10} CREATED",
        "RUN ID", "FILE", "ROWS", "BYTES"
    );
    println!("{}", "-".repeat(115));
    for f in &files {
        println!(
            "{:<40} {:<40} {:>8} {:>10} {}",
            f.run_id,
            f.file_name,
            f.row_count,
            format_bytes(f.bytes as u64),
            f.created_at,
        );
    }
    Ok(())
}

pub fn show_metrics(
    config_path: &str,
    export_name: Option<&str>,
    limit: usize,
    json: bool,
) -> Result<()> {
    let config = require_config(config_path)?;
    // A typo'd `--export` must not masquerade as "no runs yet". Now that the
    // config is loaded, check the requested name against the declared exports
    // and bail with the known names — otherwise an unknown export and an
    // unrun one both print "No metrics recorded yet.".
    if let Some(name) = export_name {
        require_known_export(&config, config_path, name)?;
    }
    let state = StateStore::open(config_path)?;
    let metrics = state.get_metrics(export_name, limit)?;
    if json {
        // Reuse the run aggregate's serializable DTO so `metrics --json` and the
        // run summary's `--json` agree field-for-field. Empty → `[]` (valid JSON),
        // not the text "no metrics" line, so a CI consumer never special-cases.
        let rows: Vec<super::aggregate::MetricRowJson> = metrics
            .iter()
            .map(super::aggregate::MetricRowJson::from)
            .collect();
        println!("{}", serde_json::to_string_pretty(&rows)?);
        return Ok(());
    }
    if metrics.is_empty() {
        println!("No metrics recorded yet.");
        return Ok(());
    }
    println!(
        "{:<20} {:<10} {:>10} {:>10} {:>8} {:>6} {:>10} RUN ID",
        "EXPORT", "STATUS", "ROWS", "DURATION", "RSS", "FILES", "BYTES"
    );
    println!("{}", "-".repeat(110));
    for m in &metrics {
        let duration = if m.duration_ms >= 1000 {
            format!("{:.1}s", m.duration_ms as f64 / 1000.0)
        } else {
            format!("{}ms", m.duration_ms)
        };
        let rss = m
            .peak_rss_mb
            .map(|r| format!("{}MB", r))
            .unwrap_or_else(|| "-".into());
        let bytes = if m.bytes_written > 0 {
            format_bytes(m.bytes_written as u64)
        } else {
            "-".into()
        };
        let run_id = m.run_id.as_deref().unwrap_or(&m.run_at);
        println!(
            "{:<20} {:<10} {:>10} {:>10} {:>8} {:>6} {:>10} {}",
            m.export_name, m.status, m.total_rows, duration, rss, m.files_produced, bytes, run_id
        );
        if let Some(err) = &m.error_message {
            println!("  Error: {}", err);
        }
        let mut flags = Vec::new();
        if m.retries > 0 {
            flags.push(format!("retries={}", m.retries));
        }
        if let Some(v) = m.validated {
            flags.push(format!("validated={}", if v { "pass" } else { "FAIL" }));
        }
        if let Some(sc) = m.schema_changed {
            flags.push(format!("schema={}", if sc { "CHANGED" } else { "ok" }));
        }
        if !flags.is_empty() {
            println!("  {}", flags.join("  "));
        }
    }
    Ok(())
}

pub fn reset_chunk_checkpoint(config_path: &str, export_name: &str) -> Result<()> {
    // Parity with `reset_state`: validate the export name against the config
    // BEFORE touching state, so a typo (`-e pa_audi` for `pa_audit`) errors with
    // the declared names instead of silently "Removed 0 chunk run record(s)"
    // (rc=0) — which looked like the resume was abandoned when it was not.
    let config = require_config(config_path)?;
    if !config.exports.iter().any(|e| e.name == export_name) {
        let known: Vec<String> = config.exports.iter().map(|e| e.name.clone()).collect();
        anyhow::bail!(
            "export '{}' not found in config '{}'.\n  Known exports: {}\n  Hint: check the spelling, or run `rivet state chunks -c {} -e <name>` to inspect a checkpoint.",
            export_name,
            config_path,
            if known.is_empty() {
                "(none defined)".to_string()
            } else {
                known.join(", ")
            },
            config_path,
        );
    }
    let state = StateStore::open(config_path)?;
    let n = state.reset_chunk_checkpoint(export_name)?;
    // Abandoning the resume also clears the committed/verified boundary, so
    // `rivet state progression` does not report a stale chunk boundary after
    // the chunk_run/chunk_task rows are gone (parity with `state reset`).
    state.delete_progression(export_name)?;
    println!(
        "Removed {} chunk run record(s) for export '{}'.",
        n, export_name
    );
    Ok(())
}

/// Clear chunk checkpoints for every export **named in `config_path`'s YAML** that currently has an
/// `in_progress` chunk run (`chunk_run.status = 'in_progress'`).
///
/// Names present only in state (removed from config) are skipped with a printed note.
pub fn reset_chunk_checkpoints_stuck(config_path: &str) -> Result<()> {
    let cfg = Config::load(config_path)?;
    let allowed: std::collections::HashSet<&str> =
        cfg.exports.iter().map(|e| e.name.as_str()).collect();
    let state = StateStore::open(config_path)?;
    let stuck = state.list_export_names_with_in_progress_chunk_runs()?;
    if stuck.is_empty() {
        println!("No exports have an in-progress chunk checkpoint run.");
        println!(
            "(Nothing with chunk_run.status = 'in_progress' in {}.)",
            StateStore::state_db_path(config_path).display()
        );
        return Ok(());
    }

    let mut skipped_not_in_config = Vec::new();
    let mut targets = Vec::new();
    for name in stuck {
        if allowed.contains(name.as_str()) {
            targets.push(name);
        } else {
            skipped_not_in_config.push(name);
        }
    }

    for name in &skipped_not_in_config {
        println!(
            "Skipping '{}' — chunk checkpoint still in_progress but this export is not in the config.",
            name
        );
    }

    if targets.is_empty() {
        println!(
            "No matching exports to reset (none of the in-progress runs belong to exports in this config)."
        );
        return Ok(());
    }

    println!(
        "Resetting chunk checkpoints for {} export(s) with in_progress runs: {}",
        targets.len(),
        targets.join(", ")
    );

    for name in targets {
        let n = state.reset_chunk_checkpoint(&name)?;
        println!("Removed {} chunk run record(s) for export '{}'.", n, name);
    }
    Ok(())
}

pub fn show_chunk_checkpoint(config_path: &str, export_name: &str) -> Result<()> {
    require_config(config_path)?;
    let state = StateStore::open(config_path)?;
    println!(
        "database:   {}",
        StateStore::state_db_path(config_path).display()
    );
    let Some((run_id, plan_hash, status, updated_at)) = state.get_latest_chunk_run(export_name)?
    else {
        println!("No chunk checkpoint data for export '{}'.", export_name);
        return Ok(());
    };
    println!("export:     {}", export_name);
    println!("run_id:     {}", run_id);
    println!("plan_hash:  {}", plan_hash);
    println!("status:     {}", status);
    println!("updated_at: {}", updated_at);
    println!();
    println!(
        "{:<6} {:<12} {:<18} {:<18} {:>4} {:>8} FILE",
        "IDX", "STATUS", "START", "END", "ATT", "ROWS"
    );
    println!("{}", "-".repeat(90));
    for t in state.list_chunk_tasks_for_run(&run_id)? {
        let file = t.file_name.as_deref().unwrap_or("-");
        let rows = t
            .rows_written
            .map(|r| r.to_string())
            .unwrap_or_else(|| "-".into());
        println!(
            "{:<6} {:<12} {:<18} {:<18} {:>4} {:>8} {}",
            t.chunk_index, t.status, t.start_key, t.end_key, t.attempts, rows, file
        );
        if let Some(e) = &t.last_error {
            println!("       error: {}", e);
        }
    }
    Ok(())
}

/// Display recent run journal entries for an export.
///
/// Shows up to `limit` most recent runs (newest first), each as a compact
/// block: run header + per-event summary lines.
pub fn show_journal(
    config_path: &str,
    export_name: &str,
    limit: usize,
    run_id: Option<&str>,
) -> Result<()> {
    let config = require_config(config_path)?;
    // Same gap as `metrics`: when querying by export name (no `--run-id`), a
    // typo would print "No journal entries for export 'X' yet." as if the
    // export were merely unrun. Validate the name against the config first.
    // The `--run-id` path looks up by id directly, so the export name is not
    // the lookup key there and is left unchecked.
    if run_id.is_none() {
        require_known_export(&config, config_path, export_name)?;
    }
    let state = StateStore::open(config_path)?;

    let journals = if let Some(rid) = run_id {
        match state.load_journal(rid)? {
            Some(j) => vec![j],
            None => {
                println!("No journal found for run_id '{rid}'.");
                return Ok(());
            }
        }
    } else {
        state.recent_journals(export_name, limit)?
    };

    if journals.is_empty() {
        println!("No journal entries for export '{export_name}' yet.");
        println!("Journals are recorded after each `rivet run`.");
        return Ok(());
    }

    for journal in &journals {
        // ── run header ────────────────────────────────────────────────────
        let outcome = journal.final_outcome().and_then(|e| {
            if let RunEvent::RunCompleted {
                status,
                duration_ms,
                ..
            } = &e.event
            {
                Some((status.as_str(), *duration_ms))
            } else {
                None
            }
        });
        let (status_str, duration_str) = match outcome {
            Some((s, ms)) if ms >= 1000 => (s, format!("{:.1}s", ms as f64 / 1000.0)),
            Some((s, ms)) => (s, format!("{ms}ms")),
            None => ("(incomplete)", String::new()),
        };
        let icon = match status_str {
            "success" => "✓",
            "failed" => "✗",
            _ => "•",
        };
        println!(
            "\n{icon} {export}  {status}  {dur}",
            export = journal.export_name,
            status = status_str,
            dur = duration_str,
        );
        println!("  run_id: {}", journal.run_id);

        // ── event summary lines ────────────────────────────────────────────
        let files = journal.files();
        if !files.is_empty() {
            let total_rows: i64 = files
                .iter()
                .filter_map(|e| {
                    if let RunEvent::FileWritten { rows, .. } = &e.event {
                        Some(*rows)
                    } else {
                        None
                    }
                })
                .sum();
            let total_bytes: u64 = files
                .iter()
                .filter_map(|e| {
                    if let RunEvent::FileWritten { bytes, .. } = &e.event {
                        Some(*bytes)
                    } else {
                        None
                    }
                })
                .sum();
            println!(
                "  files:  {}  rows: {}  size: {}",
                files.len(),
                total_rows,
                format_bytes(total_bytes),
            );
            // List each produced file by name — the aggregate count alone hides
            // *which* objects landed, so an operator could not cross-check the
            // journal against the destination / manifest. Each FileWritten event
            // stores the exact basename `state files` shows.
            for e in &files {
                if let RunEvent::FileWritten {
                    file_name,
                    rows,
                    bytes,
                    ..
                } = &e.event
                {
                    println!(
                        "    - {}  ({} rows, {})",
                        file_name,
                        rows,
                        format_bytes(*bytes),
                    );
                }
            }
        }

        let retries = journal.retries();
        if !retries.is_empty() {
            println!("  retries: {}", retries.len());
        }

        for e in journal.quality_issues() {
            if let RunEvent::QualityIssue { severity, message } = &e.event {
                println!("  quality [{severity}]: {message}");
            }
        }

        for e in journal.schema_changes() {
            if let RunEvent::SchemaChanged {
                added,
                removed,
                type_changed,
            } = &e.event
            {
                if !added.is_empty() {
                    println!("  schema: +{}", added.join(", +"));
                }
                if !removed.is_empty() {
                    println!("  schema: -{}", removed.join(", -"));
                }
                for (col, old, new) in type_changed {
                    println!("  schema: {col} {old}→{new}");
                }
            }
        }

        if let Some(e) = journal.final_outcome()
            && let RunEvent::RunCompleted {
                error_message: Some(err),
                ..
            } = &e.event
        {
            let first_line = err.lines().next().unwrap_or(err);
            println!("  error:  {first_line}");
        }
    }
    println!();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{RunEvent, RunJournal};

    // ── helpers ──────────────────────────────────────────────────────────────

    fn setup_dir() -> (tempfile::TempDir, String) {
        let dir = tempfile::TempDir::new().unwrap();
        // Inspect/reset commands now validate the --config path up front
        // (findings #9/#23): they `Config::load` before opening the state DB,
        // so the file must exist and parse. Write a valid two-export config
        // (orders + transactions — the names the show/reset tests use) so the
        // tests exercise the post-validation display/reset path, not the
        // config-not-found bail.
        let config_path = dir.path().join("rivet.yaml").to_str().unwrap().to_string();
        write_two_export_config(&config_path);
        (dir, config_path)
    }

    fn write_two_export_config(config_path: &str) {
        std::fs::write(
            config_path,
            br#"source:
  type: postgres
  url: postgresql://localhost/testdb
exports:
  - name: transactions
    query: "SELECT 1"
    mode: full
    format: parquet
    destination:
      type: local
      path: ./out
  - name: orders
    query: "SELECT 1"
    mode: full
    format: parquet
    destination:
      type: local
      path: ./out
"#,
        )
        .unwrap();
    }

    fn write_single_export_config(config_path: &str) {
        std::fs::write(
            config_path,
            br#"source:
  type: postgres
  url: postgresql://localhost/testdb
exports:
  - name: transactions
    query: "SELECT 1"
    mode: full
    format: parquet
    destination:
      type: local
      path: ./out
"#,
        )
        .unwrap();
    }

    fn open_state(dir: &tempfile::TempDir) -> StateStore {
        let db_path = dir.path().join(".rivet_state.db");
        StateStore::open_at_path(&db_path).unwrap()
    }

    fn make_journal(run_id: &str, export: &str) -> RunJournal {
        let mut j = RunJournal::new(run_id, export);
        j.record(RunEvent::FileWritten {
            file_name: "part0.parquet".into(),
            rows: 1_000,
            bytes: 65_536,
            part_index: 0,
        });
        j.record(RunEvent::RunCompleted {
            status: "success".into(),
            error_message: None,
            duration_ms: 1_500,
        });
        j
    }

    // ── boundary_value ───────────────────────────────────────────────────────

    fn make_boundary(cursor: Option<&str>, chunk_index: Option<i64>) -> crate::state::Boundary {
        crate::state::Boundary {
            strategy: "incremental".into(),
            run_id: None,
            cursor: cursor.map(|s| s.to_string()),
            chunk_index,
            at: chrono::Utc::now(),
        }
    }

    #[test]
    fn boundary_value_cursor_takes_precedence_over_chunk_index() {
        let b = make_boundary(Some("2025-01-15"), Some(42));
        assert_eq!(boundary_value(&b), "2025-01-15");
    }

    #[test]
    fn boundary_value_chunk_index_used_when_no_cursor() {
        let b = make_boundary(None, Some(7));
        assert_eq!(boundary_value(&b), "chunk #7");
    }

    #[test]
    fn boundary_value_dash_when_neither_set() {
        let b = make_boundary(None, None);
        assert_eq!(boundary_value(&b), "-");
    }

    // ── state files: RUN ID column width (finding L16) ───────────────────────

    // run_ids are `{export}_{%Y%m%dT%H%M%S%3f}` — the timestamp suffix alone is
    // 18 chars, so even a short export name yields ~30-char ids and longer ones
    // reach ~40. The RUN ID column must be wide enough that a 40-char id is not
    // padded *past* the FILE column (which would shove FILE right and break
    // every subsequent row's alignment vs. the header). A column of 35 wrapped;
    // this pins the widened layout: a 40-char id is followed by exactly one
    // space then FILE, identical to the header's spacing.
    #[test]
    fn state_files_run_id_column_fits_a_40_char_run_id() {
        let run_id = "transactions_historyy_20250115T143022999"; // 40 chars
        assert_eq!(run_id.len(), 40, "fixture must be a realistic 40-char id");

        let header = format!(
            "{:<40} {:<40} {:>8} {:>10} CREATED",
            "RUN ID", "FILE", "ROWS", "BYTES"
        );
        let row = format!(
            "{:<40} {:<40} {:>8} {:>10} {}",
            run_id, "orders_001.parquet", 50_000, "4.0KB", "2025-01-15",
        );

        // The FILE column header and the row's file value must start at the
        // same byte offset — the alignment invariant a too-narrow column breaks.
        let header_file_at = header.find("FILE").unwrap();
        let row_file_at = row.find("orders_001.parquet").unwrap();
        assert_eq!(
            header_file_at, row_file_at,
            "a 40-char RUN ID must not push the FILE column out of alignment\nheader: {header}\nrow:    {row}"
        );
        // And the run_id is rendered in full (not truncated).
        assert!(
            row.starts_with(run_id),
            "run_id must not be truncated: {row}"
        );
    }

    // ── show_state ───────────────────────────────────────────────────────────

    // Finding L17: the empty-cursor / never-run hint pointed at `rivet state`,
    // which is a bare subcommand group and errors without a leaf. The fix points
    // at the real command `rivet state show`. Pin the exact hint string the
    // never-run branch builds.
    #[test]
    fn show_state_never_run_hint_points_at_state_show() {
        let config_path = "rivet.yaml";
        let hint = format!(
            "No exports have been run yet.\n  \
             Run `rivet run --config {}` first, then try `rivet state show` again.",
            config_path
        );
        assert!(
            hint.contains("try `rivet state show` again"),
            "hint must name the runnable leaf command, not the bare group: {hint}"
        );
        assert!(
            !hint.contains("try `rivet state` again"),
            "must not point at the bare (subcommand-requiring) group: {hint}"
        );
    }

    // ── metrics / journal: unknown-export guard (finding L18) ────────────────

    // `metrics --export <typo>` used to print "No metrics recorded yet." —
    // indistinguishable from a declared-but-unrun export. With the config now
    // loaded, an undeclared name must bail naming the known exports.
    #[test]
    fn show_metrics_unknown_export_bails_with_known_names() {
        let (dir, config_path) = setup_dir(); // declares orders + transactions
        let _ = open_state(&dir);
        let err = show_metrics(&config_path, Some("ghost"), 10, false).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("export 'ghost' is not defined"),
            "must name the unknown export, not say 'No metrics recorded yet': {msg}"
        );
        assert!(
            msg.contains("orders") && msg.contains("transactions"),
            "must list the declared exports so the user can spot the typo: {msg}"
        );
    }

    // A *declared* export with no recorded runs must still reach the normal
    // empty-state path (proves the guard does not over-reject).
    #[test]
    fn show_metrics_known_but_unrun_export_returns_ok() {
        let (dir, config_path) = setup_dir();
        let _ = open_state(&dir);
        assert!(show_metrics(&config_path, Some("orders"), 10, false).is_ok());
    }

    // Same gap on `journal` when querying by export name (no --run-id).
    #[test]
    fn show_journal_unknown_export_bails_with_known_names() {
        let (dir, config_path) = setup_dir();
        let _ = open_state(&dir);
        let err = show_journal(&config_path, "ghost", 5, None).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("export 'ghost' is not defined"),
            "must name the unknown export, not say 'No journal entries … yet': {msg}"
        );
        assert!(
            msg.contains("orders") && msg.contains("transactions"),
            "must list the declared exports: {msg}"
        );
    }

    // Querying by --run-id looks up by id, not export name, so an unfamiliar
    // export name must NOT be rejected there (the id is the lookup key).
    #[test]
    fn show_journal_by_run_id_skips_export_name_check() {
        let (dir, config_path) = setup_dir();
        let _ = open_state(&dir);
        assert!(show_journal(&config_path, "ghost", 5, Some("no_such_run")).is_ok());
    }

    #[test]
    fn show_state_empty_db_returns_ok() {
        let (dir, config_path) = setup_dir();
        let _ = open_state(&dir); // create the DB file
        assert!(show_state(&config_path).is_ok());
    }

    #[test]
    fn show_state_with_cursor_record_returns_ok() {
        let (dir, config_path) = setup_dir();
        let state = open_state(&dir);
        state.update("orders", "2025-01-15").unwrap();
        drop(state);
        assert!(show_state(&config_path).is_ok());
    }

    // ── show_files ───────────────────────────────────────────────────────────

    #[test]
    fn show_files_empty_returns_ok() {
        let (dir, config_path) = setup_dir();
        let _ = open_state(&dir);
        assert!(show_files(&config_path, None, 10, false).is_ok());
    }

    #[test]
    fn show_files_with_record_returns_ok() {
        let (dir, config_path) = setup_dir();
        let state = open_state(&dir);
        state
            .record_file(
                "r1",
                "orders",
                "orders_001.parquet",
                50_000,
                4096,
                "parquet",
                Some("zstd"),
            )
            .unwrap();
        drop(state);
        assert!(show_files(&config_path, Some("orders"), 10, false).is_ok());
    }

    // ── show_metrics ─────────────────────────────────────────────────────────

    #[test]
    fn show_metrics_empty_returns_ok() {
        let (dir, config_path) = setup_dir();
        let _ = open_state(&dir);
        assert!(show_metrics(&config_path, None, 10, false).is_ok());
    }

    #[test]
    fn show_metrics_exercises_flag_and_duration_paths() {
        // Covers retries / validated / schema_changed flag lines and
        // the ms-vs-seconds duration branch in the formatter.
        let (dir, config_path) = setup_dir();
        let state = open_state(&dir);
        // ≥1000 ms → "X.Xs" branch; retries + validated + schema_changed flags
        state
            .record_metric(
                "orders",
                "r1",
                1_500,
                50_000,
                Some(42),
                "success",
                None,
                Some("balanced"),
                Some("parquet"),
                Some("full"),
                1,
                4096,
                3,
                Some(true),
                Some(true),
            )
            .unwrap();
        // <1000 ms → "Xms" branch; error_message line
        state
            .record_metric(
                "orders",
                "r2",
                800,
                0,
                None,
                "failed",
                Some("timeout"),
                None,
                None,
                None,
                0,
                0,
                0,
                Some(false),
                None,
            )
            .unwrap();
        drop(state);
        assert!(show_metrics(&config_path, Some("orders"), 10, false).is_ok());
    }

    // ── show_journal ─────────────────────────────────────────────────────────

    #[test]
    fn show_journal_empty_returns_ok() {
        let (dir, config_path) = setup_dir();
        let _ = open_state(&dir);
        assert!(show_journal(&config_path, "orders", 5, None).is_ok());
    }

    #[test]
    fn show_journal_with_entry_returns_ok() {
        let (dir, config_path) = setup_dir();
        let state = open_state(&dir);
        state
            .store_journal(&make_journal("run_001", "orders"))
            .unwrap();
        drop(state);
        assert!(show_journal(&config_path, "orders", 5, None).is_ok());
    }

    #[test]
    fn show_journal_by_run_id_not_found_returns_ok() {
        let (dir, config_path) = setup_dir();
        let _ = open_state(&dir);
        assert!(show_journal(&config_path, "orders", 5, Some("no_such_run")).is_ok());
    }

    #[test]
    fn show_journal_by_run_id_found_returns_ok() {
        let (dir, config_path) = setup_dir();
        let state = open_state(&dir);
        state
            .store_journal(&make_journal("run_xyz", "orders"))
            .unwrap();
        drop(state);
        assert!(show_journal(&config_path, "orders", 5, Some("run_xyz")).is_ok());
    }

    // ── reset_state ──────────────────────────────────────────────────────────

    #[test]
    fn reset_state_returns_ok() {
        let (dir, config_path) = setup_dir();
        write_two_export_config(&config_path);
        let state = open_state(&dir);
        state.update("orders", "100").unwrap();
        drop(state);
        assert!(reset_state(&config_path, "orders").is_ok());
    }

    // F-NEW (0.7.7 audit): `state reset` on an export that is not declared
    // in the config used to silently succeed (DELETE WHERE export_name = X
    // affects 0 rows; "State reset for export 'X'" printed; rc=0). A typo'd
    // `--export pa_audi` looked like success but did nothing. This pins the
    // hint-emitting bail.
    #[test]
    fn reset_state_unknown_export_bails_with_hint() {
        let (_dir, config_path) = setup_dir();
        write_two_export_config(&config_path);
        let err = reset_state(&config_path, "ghost").unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("export 'ghost' not found"),
            "must name the missing export: {msg}"
        );
        assert!(
            msg.contains("orders") && msg.contains("transactions"),
            "must list the declared exports so the user can spot the typo: {msg}"
        );
        assert!(
            msg.contains("rivet state show"),
            "must point at a follow-up command: {msg}"
        );
    }

    // ── reset_chunk_checkpoint ───────────────────────────────────────────────

    #[test]
    fn reset_chunk_checkpoint_on_empty_db_returns_ok() {
        let (dir, config_path) = setup_dir();
        let _ = open_state(&dir);
        assert!(reset_chunk_checkpoint(&config_path, "orders").is_ok());
    }

    // #21 (0.9.x audit): `state reset-chunks -e <typo>` used to "Removed 0 …"
    // rc=0 with no export-name guardrail. Parity with `reset_state`: an unknown
    // name must bail with a hint that names the export and lists the declared
    // ones so the operator can spot the typo.
    #[test]
    fn reset_chunk_checkpoint_unknown_export_bails_with_hint() {
        let (_dir, config_path) = setup_dir(); // declares orders + transactions
        let err = reset_chunk_checkpoint(&config_path, "ghost").unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("export 'ghost' not found"),
            "must name the missing export: {msg}"
        );
        assert!(
            msg.contains("orders") && msg.contains("transactions"),
            "must list the declared exports so the user can spot the typo: {msg}"
        );
    }

    // #9/#23 (0.9.x audit): inspect/reset commands used --config ONLY to locate
    // the state DB next to it, never parsing it — so a nonexistent path opened a
    // fresh empty DB and exited 0 with "No … recorded yet" (and littered a
    // .rivet_state.db). They must bail naming the bad path instead.
    #[test]
    fn inspect_commands_bail_on_nonexistent_config() {
        let dir = tempfile::TempDir::new().unwrap();
        let missing = dir
            .path()
            .join("does_not_exist.yaml")
            .to_str()
            .unwrap()
            .to_string();

        for res in [
            show_state(&missing),
            show_files(&missing, None, 10, false),
            show_metrics(&missing, None, 10, false),
            show_progression(&missing, None),
            show_journal(&missing, "orders", 5, None),
            show_chunk_checkpoint(&missing, "orders"),
            reset_state(&missing, "orders"),
            reset_chunk_checkpoint(&missing, "orders"),
        ] {
            let err = res.expect_err("nonexistent config must error, not exit Ok");
            assert!(
                format!("{err:#}").contains("does_not_exist.yaml"),
                "error must name the missing config path: {err:#}"
            );
        }

        // And the read-only inspect must NOT materialize a state DB beside the
        // phantom config (validation happens before StateStore::open).
        assert!(
            !dir.path().join(".rivet_state.db").exists(),
            "a bad-config inspect must not leak a fresh .rivet_state.db"
        );
    }

    #[test]
    fn reset_chunk_checkpoints_stuck_no_rows_returns_ok() {
        let (dir, config_path) = setup_dir();
        write_two_export_config(&config_path);
        let _ = open_state(&dir);
        assert!(reset_chunk_checkpoints_stuck(&config_path).is_ok());
    }

    #[test]
    fn reset_chunk_checkpoints_stuck_clears_matching_exports_only() {
        let (dir, config_path) = setup_dir();
        write_two_export_config(&config_path);
        let state = open_state(&dir);
        state
            .create_chunk_run("r_tx", "transactions", "plan", 3)
            .unwrap();
        state.create_chunk_run("r_g", "ghost", "plan", 3).unwrap();
        drop(state);

        reset_chunk_checkpoints_stuck(&config_path).unwrap();

        let state = StateStore::open(&config_path).unwrap();
        assert!(
            state
                .find_in_progress_chunk_run("transactions")
                .unwrap()
                .is_none()
        );
        assert!(state.find_in_progress_chunk_run("ghost").unwrap().is_some());
        assert_eq!(
            state.reset_chunk_checkpoint("ghost").unwrap(),
            1,
            "cleanup ghost row"
        );
    }

    #[test]
    fn reset_chunk_checkpoints_stuck_skips_when_only_unknown_exports_stuck() {
        let (dir, config_path) = setup_dir();
        write_single_export_config(&config_path);
        let state = open_state(&dir);
        state.create_chunk_run("r_g", "ghost", "plan", 3).unwrap();
        drop(state);

        reset_chunk_checkpoints_stuck(&config_path).unwrap();

        let state = StateStore::open(&config_path).unwrap();
        assert!(state.find_in_progress_chunk_run("ghost").unwrap().is_some());
        assert_eq!(state.reset_chunk_checkpoint("ghost").unwrap(), 1);
    }

    // ── show_progression ─────────────────────────────────────────────────────

    #[test]
    fn show_progression_empty_returns_ok() {
        let (dir, config_path) = setup_dir();
        let _ = open_state(&dir);
        assert!(show_progression(&config_path, None).is_ok());
    }

    #[test]
    fn show_progression_with_incremental_boundary_returns_ok() {
        let (dir, config_path) = setup_dir();
        let state = open_state(&dir);
        state
            .record_committed_incremental("orders", "2025-06-01", "run_001")
            .unwrap();
        drop(state);
        assert!(show_progression(&config_path, Some("orders")).is_ok());
    }

    // ── show_chunk_checkpoint ────────────────────────────────────────────────

    #[test]
    fn show_chunk_checkpoint_no_data_returns_ok() {
        let (dir, config_path) = setup_dir();
        let _ = open_state(&dir);
        assert!(show_chunk_checkpoint(&config_path, "orders").is_ok());
    }
}
