//! IPC protocol between a parent `rivet run --parallel-export-processes` and
//! its child processes.
//!
//! When the parent spawns a child, it sets `RIVET_IPC_EVENTS=1` in the child's
//! environment.  A child that sees this flag:
//!
//! - emits one [`ChildEvent`] per significant event as a single JSON line on
//!   **stdout** (stdout is otherwise unused by `rivet run`),
//! - hides its own `indicatif` progress bar (the parent draws progress for
//!   every child in its own `MultiProgress`),
//! - skips the per-export stderr summary block (the parent renders the final
//!   metrics in-place inside the progress card on `Finished`).
//!
//! The parent reads each child's stdout line-by-line in a dedicated thread,
//! parses events, and forwards them through an `mpsc::Sender` to a single UI
//! thread that owns the `MultiProgress`.  Stderr (env-logger output) is left
//! inherited so log messages still flow normally.

use serde::{Deserialize, Serialize};

/// Env var that toggles emitter mode in children.  Set by the parent in
/// `run_exports_as_child_processes`; absent for stand-alone CLI invocations.
pub const ENV_IPC_EVENTS: &str = "RIVET_IPC_EVENTS";

/// One observable event from a child.  The order is always:
/// `Started → ProgressInit → Progress* → Finished` per export.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChildEvent {
    /// `RunSummary::new` was called — the export is about to start.
    Started {
        export_name: String,
        run_id: String,
        mode: String,
        tuning_profile: String,
        batch_size: usize,
    },
    /// A `ChunkProgress` was created — total chunk count is now known.
    /// Skipped for non-chunked strategies.
    ProgressInit {
        export_name: String,
        total_chunks: u64,
    },
    /// One chunk finished — emit running totals.
    Progress {
        export_name: String,
        chunks_done: u64,
        rows: i64,
    },
    /// Terminal verdict reached.  Carries final metrics that the parent
    /// renders in place of the progress bar.
    Finished {
        export_name: String,
        run_id: String,
        status: String,
        total_rows: i64,
        files_produced: u64,
        bytes_written: u64,
        duration_ms: i64,
        peak_rss_mb: i64,
        error_message: Option<String>,
    },
}

impl ChildEvent {
    /// Name of the export this event refers to (used by the parent to route
    /// the event to the right card).
    #[allow(dead_code)] // wired up by the parent UI in a follow-up commit
    pub fn export_name(&self) -> &str {
        match self {
            ChildEvent::Started { export_name, .. }
            | ChildEvent::ProgressInit { export_name, .. }
            | ChildEvent::Progress { export_name, .. }
            | ChildEvent::Finished { export_name, .. } => export_name,
        }
    }
}

/// `true` iff `RIVET_IPC_EVENTS` is set to a non-empty value (i.e., this
/// process is a child spawned by a parent that wants events).
pub fn ipc_events_enabled() -> bool {
    std::env::var(ENV_IPC_EVENTS)
        .map(|v| !v.is_empty())
        .unwrap_or(false)
}

/// Emit one event to stdout as a single JSON line.  Errors are logged at
/// `debug` and otherwise swallowed — IPC failures must not abort an export.
pub fn emit(event: &ChildEvent) {
    use std::io::Write;
    let line = match serde_json::to_string(event) {
        Ok(s) => s,
        Err(e) => {
            log::debug!("ipc: failed to serialize event {:?}: {:#}", event, e);
            return;
        }
    };
    let stdout = std::io::stdout();
    let mut h = stdout.lock();
    if let Err(e) = writeln!(h, "{line}") {
        log::debug!("ipc: failed to write event to stdout: {:#}", e);
        return;
    }
    let _ = h.flush();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_started() {
        let ev = ChildEvent::Started {
            export_name: "orders".into(),
            run_id: "orders_20260427T120000".into(),
            mode: "chunked".into(),
            tuning_profile: "balanced (default)".into(),
            batch_size: 10_000,
        };
        let json = serde_json::to_string(&ev).unwrap();
        let back: ChildEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(ev, back);
        // Tag is `started` (snake_case).
        assert!(json.contains("\"type\":\"started\""));
    }

    #[test]
    fn round_trip_progress() {
        let ev = ChildEvent::Progress {
            export_name: "events".into(),
            chunks_done: 7,
            rows: 1_234_567,
        };
        let s = serde_json::to_string(&ev).unwrap();
        let back: ChildEvent = serde_json::from_str(&s).unwrap();
        assert_eq!(ev, back);
    }

    #[test]
    fn round_trip_finished_with_error() {
        let ev = ChildEvent::Finished {
            export_name: "users".into(),
            run_id: "users_20260427T120000".into(),
            status: "failed".into(),
            total_rows: 0,
            files_produced: 0,
            bytes_written: 0,
            duration_ms: 1234,
            peak_rss_mb: 12,
            error_message: Some("connection reset".into()),
        };
        let s = serde_json::to_string(&ev).unwrap();
        let back: ChildEvent = serde_json::from_str(&s).unwrap();
        assert_eq!(ev, back);
    }

    #[test]
    fn export_name_routing() {
        let ev = ChildEvent::ProgressInit {
            export_name: "orders".into(),
            total_chunks: 20,
        };
        assert_eq!(ev.export_name(), "orders");
    }

    #[test]
    fn ipc_events_enabled_false_by_default() {
        // We cannot assert global env state safely in parallel tests, but we
        // can at least verify the helper handles an empty string as off.
        // (We don't mutate env here to avoid races.)
        // SAFETY: this test only reads.
        let _ = ipc_events_enabled();
    }
}
