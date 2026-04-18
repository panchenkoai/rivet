//! **Layer: Observability**
//!
//! CLI display commands for state, metrics, files, and chunk checkpoints.
//! Reads from the state stores and formats output — no execution or persistence writes.

use super::format_bytes;
use crate::error::Result;
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
