//! **Layer: Observability**
//!
//! CLI display commands for state, metrics, files, chunk checkpoints, and journal history.
//! Reads from the state stores and formats output — no execution or persistence writes.

use super::format_bytes;
use crate::error::Result;
use crate::pipeline::journal::RunEvent;
use crate::state::StateStore;

pub fn show_state(config_path: &str) -> Result<()> {
    let state = StateStore::open(config_path)?;
    let states = state.list_all()?;
    if states.is_empty() {
        println!("No export state recorded yet.");
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
    let state = StateStore::open(config_path)?;
    state.reset(export_name)?;
    println!("State reset for export '{}'", export_name);
    Ok(())
}

pub fn show_files(config_path: &str, export_name: Option<&str>, limit: usize) -> Result<()> {
    let state = StateStore::open(config_path)?;
    let files = state.get_files(export_name, limit)?;
    if files.is_empty() {
        println!("No files recorded yet.");
        return Ok(());
    }
    println!(
        "{:<35} {:<40} {:>8} {:>10} CREATED",
        "RUN ID", "FILE", "ROWS", "BYTES"
    );
    println!("{}", "-".repeat(110));
    for f in &files {
        println!(
            "{:<35} {:<40} {:>8} {:>10} {}",
            f.run_id,
            f.file_name,
            f.row_count,
            format_bytes(f.bytes as u64),
            f.created_at,
        );
    }
    Ok(())
}

pub fn show_metrics(config_path: &str, export_name: Option<&str>, limit: usize) -> Result<()> {
    let state = StateStore::open(config_path)?;
    let metrics = state.get_metrics(export_name, limit)?;
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
    let state = StateStore::open(config_path)?;
    let n = state.reset_chunk_checkpoint(export_name)?;
    println!(
        "Removed {} chunk run record(s) for export '{}'.",
        n, export_name
    );
    Ok(())
}

pub fn show_chunk_checkpoint(config_path: &str, export_name: &str) -> Result<()> {
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
    use crate::pipeline::journal::{RunEvent, RunJournal};

    // ── helpers ──────────────────────────────────────────────────────────────

    fn setup_dir() -> (tempfile::TempDir, String) {
        let dir = tempfile::TempDir::new().unwrap();
        // config file itself does not need to exist; StateStore::open uses its parent dir
        let config_path = dir.path().join("rivet.yaml").to_str().unwrap().to_string();
        (dir, config_path)
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

    // ── show_state ───────────────────────────────────────────────────────────

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
        assert!(show_files(&config_path, None, 10).is_ok());
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
        assert!(show_files(&config_path, Some("orders"), 10).is_ok());
    }

    // ── show_metrics ─────────────────────────────────────────────────────────

    #[test]
    fn show_metrics_empty_returns_ok() {
        let (dir, config_path) = setup_dir();
        let _ = open_state(&dir);
        assert!(show_metrics(&config_path, None, 10).is_ok());
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
        assert!(show_metrics(&config_path, Some("orders"), 10).is_ok());
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
        let state = open_state(&dir);
        state.update("orders", "100").unwrap();
        drop(state);
        assert!(reset_state(&config_path, "orders").is_ok());
    }

    // ── reset_chunk_checkpoint ───────────────────────────────────────────────

    #[test]
    fn reset_chunk_checkpoint_on_empty_db_returns_ok() {
        let (dir, config_path) = setup_dir();
        let _ = open_state(&dir);
        assert!(reset_chunk_checkpoint(&config_path, "orders").is_ok());
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
