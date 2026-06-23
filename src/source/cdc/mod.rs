//! Canonical change-data-capture types + driver, shared across engines.
//!
//! Each engine's reader (`source::<engine>::cdc`) is an **adapter** that yields
//! these canonical types; the driver [`run`] — and the future Parquet/CSV sink —
//! is written once against the [`ChangeStream`] seam, not per engine. Three
//! adapters (MySQL binlog, PG logical slot, SQL Server change-table poll) make
//! the seam real.
//!
//! `#![allow(dead_code)]`: the consumer is `cli::dispatch` (the `rivet cdc`
//! command), which lives only in the binary crate; the library crate also
//! compiles `source` for the integration tests but has no CDC consumer of its
//! own. Same pattern as `source::mysql::cdc`.
#![allow(dead_code)]

pub(crate) mod sink;
pub(crate) mod value;

use std::path::{Path, PathBuf};

use serde_json::Value as Json;

use crate::error::Result;
use value::RivetValue;

/// Canonical DML kind. Engine framing — PostgreSQL `BEGIN`/`COMMIT` markers, the
/// SQL Server update before/after split — is normalised away by each adapter; a
/// row change is exactly one of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChangeOp {
    Insert,
    Update,
    Delete,
}

impl ChangeOp {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            ChangeOp::Insert => "insert",
            ChangeOp::Update => "update",
            ChangeOp::Delete => "delete",
        }
    }
}

/// An opaque, engine-shaped resume position — MySQL `{file, pos}`, a PostgreSQL
/// LSN, a SQL Server LSN. Persisted verbatim as the checkpoint; each engine
/// interprets its own shape when resuming. Compared only for equality.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Position(pub(crate) Json);

impl Position {
    /// Load a persisted checkpoint, or `None` on first run (absent).
    pub(crate) fn load(path: &Path) -> Result<Option<Self>> {
        match std::fs::read_to_string(path) {
            Ok(s) => Ok(Some(Position(serde_json::from_str(&s)?))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Persist atomically (temp file + rename) so a crash never leaves a torn
    /// checkpoint that would resume from a corrupt position.
    pub(crate) fn save(&self, path: &Path) -> Result<()> {
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, serde_json::to_vec(&self.0)?)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }
}

/// One canonical row-level change.
#[derive(Debug, Clone)]
pub(crate) struct ChangeEvent {
    pub(crate) op: ChangeOp,
    pub(crate) schema: String,
    pub(crate) table: String,
    /// Pre-image — present for `Update`/`Delete` when the engine carries it.
    pub(crate) before: Option<Vec<RivetValue>>,
    /// Post-image — present for `Insert`/`Update` when the engine carries it.
    pub(crate) after: Option<Vec<RivetValue>>,
    /// Resume position after this change.
    pub(crate) position: Position,
    /// `true` if this is the last change in its source transaction — the only
    /// point it is safe to advance the checkpoint (transaction-atomic resume) and
    /// to roll an output file (never split a transaction across files). MySQL sets
    /// it at the XID/commit marker; the poll-based PG / SQL Server adapters only
    /// ever read already-committed data, so every change is a commit boundary.
    pub(crate) committed: bool,
}

/// The seam every engine reader satisfies: a blocking pull of canonical changes.
///
/// `None` ⇒ no more changes available now. MySQL blocks until one arrives, so it
/// only ends when the connection closes; the poll-based PostgreSQL / SQL Server
/// adapters return `None` once their current backlog drains (a continuous daemon
/// wraps the driver in an outer poll loop).
pub(crate) trait ChangeStream {
    fn next_change(&mut self) -> Option<Result<ChangeEvent>>;
}

/// `rivet cdc` driver. Streams canonical changes from any engine adapter,
/// emitting one NDJSON object per change to stdout and persisting the resume
/// position after each (when `checkpoint` is set). Stops at end of stream,
/// `max_events`, or interruption.
///
/// (Candidate 3 will branch the output here onto the Parquet/CSV sink; today it
/// is NDJSON only.)
pub(crate) fn run(
    stream: &mut dyn ChangeStream,
    checkpoint: Option<PathBuf>,
    tables: Vec<String>,
    max_events: Option<usize>,
) -> Result<()> {
    let mut emitted = 0usize;
    while let Some(ev) = stream.next_change() {
        let ev = ev?;
        if !tables.is_empty() && !tables.iter().any(|t| t == &ev.table) {
            continue;
        }
        let to_json = |img: &Option<Vec<RivetValue>>| {
            img.as_ref()
                .map(|vs| vs.iter().map(RivetValue::to_json).collect::<Vec<_>>())
        };
        let line = serde_json::json!({
            "op": ev.op.as_str(),
            "schema": ev.schema,
            "table": ev.table,
            "before": to_json(&ev.before),
            "after": to_json(&ev.after),
            "pos": ev.position.0,
        });
        println!("{line}");
        // Checkpoint only at a transaction boundary — resuming mid-transaction
        // would replay a partial transaction downstream.
        if ev.committed
            && let Some(p) = &checkpoint
        {
            ev.position.save(p)?;
        }
        emitted += 1;
        if max_events.is_some_and(|m| emitted >= m) {
            break;
        }
    }
    Ok(())
}
