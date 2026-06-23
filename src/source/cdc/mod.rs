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
pub(crate) mod validate;
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

    /// Acknowledge that every change up to and including `position` is **durably
    /// persisted** at the destination. Engines that consume-on-read (PostgreSQL:
    /// reading a logical slot advances it) defer the actual consume to here — so a
    /// crash between reading and a durable write re-reads the un-acked changes
    /// (at-least-once). MySQL (binlog) and SQL Server (change tables) retain on the
    /// server independently of reads, so this is a no-op for them — the resume
    /// checkpoint alone makes them at-least-once.
    fn ack(&mut self, _position: &Position) -> Result<()> {
        Ok(())
    }
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

/// Connection + resume parameters for `rivet cdc`, across engines — the CDC
/// sibling of [`crate::source::create_source`]'s `SourceConfig`.
pub(crate) struct CdcConfig {
    pub url: String,
    /// MySQL replica id.
    pub server_id: u32,
    /// PostgreSQL logical slot name.
    pub slot: String,
    /// SQL Server capture instance (required for `sqlserver://`).
    pub capture_instance: Option<String>,
    /// MySQL checkpoint file (PG resumes via the slot; SQL Server via its LSN).
    pub checkpoint: Option<PathBuf>,
    /// Catch up to the source's current end and exit, instead of streaming
    /// indefinitely. For MySQL this sets `BINLOG_DUMP_NON_BLOCK`; PostgreSQL /
    /// SQL Server already drain their backlog and exit, so it is a no-op there.
    pub until_current: bool,
    /// Transport security, applied by every adapter through the same
    /// `require_tls_or_loopback` gate the batch path uses (refuse remote
    /// plaintext / unauthenticated TLS). `None` ⇒ loopback-only (the CLI default).
    pub tls: Option<crate::config::TlsConfig>,
}

/// Construct the right [`ChangeStream`] adapter for the source URL's scheme —
/// dispatching by engine exactly as [`crate::source::create_source`] does for the
/// batch path.
pub(crate) fn create_change_stream(cfg: &CdcConfig) -> Result<Box<dyn ChangeStream>> {
    let url = cfg.url.as_str();
    let tls = cfg.tls.as_ref();
    if url.starts_with("mysql://") {
        Ok(Box::new(
            crate::source::mysql::cdc::MysqlChangeStream::open_or_resume(
                url,
                cfg.server_id,
                cfg.checkpoint.as_deref(),
                cfg.until_current,
                tls,
            )?,
        ))
    } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        Ok(Box::new(
            crate::source::postgres::cdc::PgChangeStream::open(url, &cfg.slot, tls)?,
        ))
    } else if url.starts_with("sqlserver://") || url.starts_with("mssql://") {
        let ci = cfg.capture_instance.as_deref().ok_or_else(|| {
            anyhow::anyhow!("sqlserver cdc requires --capture-instance (e.g. dbo_orders)")
        })?;
        Ok(Box::new(
            crate::source::mssql::cdc::MssqlChangeStream::from_url(url, ci, tls)?,
        ))
    } else {
        anyhow::bail!(
            "rivet cdc: unsupported source url — expected mysql:// / postgresql:// / sqlserver://"
        )
    }
}

/// Resolve a CDC table's column type mappings from the source — the **same**
/// `RivetType` → Arrow pipeline the batch export uses — so the typed file sink
/// writes identical columns (logical types `json`/`uuid`/…, real int widths, …)
/// via [`crate::types::build_arrow_field`]. `tls` is threaded through the same
/// gate as the streaming connection.
pub(crate) fn resolve_cdc_columns(
    url: &str,
    table: &str,
    tls: Option<&crate::config::TlsConfig>,
) -> Result<Vec<crate::types::TypeMapping>> {
    use crate::source::Source;
    // The table name is interpolated into `SELECT * FROM {table}` for the schema
    // probe, so validate it to a plain `[schema.]table` identifier — no quote,
    // paren, semicolon, or space can break out (mirrors the capture-instance check).
    if table.is_empty()
        || !table
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    {
        anyhow::bail!(
            "rivet cdc table must be a plain [schema.]table identifier (got {table:?}); \
             refusing to interpolate it into SQL"
        );
    }
    let mut src: Box<dyn Source> = if url.starts_with("mysql://") {
        Box::new(crate::source::mysql::MysqlSource::connect_with_tls(
            url, tls,
        )?)
    } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        Box::new(crate::source::postgres::PostgresSource::connect_with_tls(
            url, tls,
        )?)
    } else {
        Box::new(crate::source::mssql::MssqlSource::connect_with_tls(
            url, tls,
        )?)
    };
    src.type_mappings(
        &format!("SELECT * FROM {table}"),
        &crate::types::ColumnOverrides::new(),
    )
}
