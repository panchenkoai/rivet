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

impl ChangeEvent {
    /// Rough in-memory footprint of this buffered change — drives the sink's
    /// memory-budget rollover (`rollover_memory_mb`). The before/after value
    /// images dominate; schema/table names + a small fixed overhead are added.
    pub(crate) fn estimated_bytes(&self) -> usize {
        let img = |v: &Option<Vec<RivetValue>>| {
            v.as_ref()
                .map_or(0, |vs| vs.iter().map(RivetValue::estimated_bytes).sum())
        };
        self.schema.len() + self.table.len() + img(&self.before) + img(&self.after) + 32
    }
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

/// The CDC engine, resolved ONCE from the source URL's scheme. Every
/// downstream dispatch matches on this enum — never on the URL string — so
/// adding engine #4 is one variant plus compiler-led match arms, and a
/// mistyped scheme fails in exactly one place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CdcEngine {
    Mysql,
    Postgres,
    Mssql,
}

impl CdcEngine {
    pub(crate) fn from_url(url: &str) -> Result<Self> {
        if url.starts_with("mysql://") {
            Ok(Self::Mysql)
        } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            Ok(Self::Postgres)
        } else if url.starts_with("sqlserver://") || url.starts_with("mssql://") {
            Ok(Self::Mssql)
        } else {
            anyhow::bail!(
                "rivet cdc: unsupported source url — expected mysql:// / postgresql:// / sqlserver://"
            )
        }
    }

    /// Stable lowercase label for metrics / run records / hints.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Mysql => "mysql",
            Self::Postgres => "postgres",
            Self::Mssql => "mssql",
        }
    }
}

/// Setup/permission hints appended to a CDC start-up error — so a missing grant
/// surfaces the fix, not just a raw driver error. Phrased "if this is a
/// permissions/setup error" because the same call can fail for other reasons.
pub(crate) const MYSQL_CDC_HINT: &str = "if this is a permissions/setup error: MySQL CDC needs binlog_format=ROW plus a REPLICATION SLAVE + REPLICATION CLIENT grant (and SELECT on the table) — see the 'MySQL — the binlog grants' section of docs/reference/cdc.md";
pub(crate) const PG_CDC_HINT: &str = "if this is a permissions/setup error: PostgreSQL CDC needs wal_level=logical and a role with the REPLICATION attribute — see the 'PostgreSQL — the logical slot' section of docs/reference/cdc.md";
pub(crate) const MSSQL_CDC_HINT: &str = "if this is a permissions/setup error: SQL Server CDC must be enabled on the table (sys.sp_cdc_enable_table) with SQL Server Agent running, and the reader needs SELECT on the cdc schema — see the 'SQL Server — CDC change tables' section of docs/reference/cdc.md";

/// Construct the right [`ChangeStream`] adapter for the source URL's scheme —
/// dispatching by engine exactly as [`crate::source::create_source`] does for the
/// batch path.
pub(crate) fn create_change_stream(cfg: &CdcConfig) -> Result<Box<dyn ChangeStream>> {
    use anyhow::Context;
    let url = cfg.url.as_str();
    let tls = cfg.tls.as_ref();
    match CdcEngine::from_url(url)? {
        CdcEngine::Mysql => Ok(Box::new(
            crate::source::mysql::cdc::MysqlChangeStream::open_or_resume(
                url,
                cfg.server_id,
                cfg.checkpoint.as_deref(),
                cfg.until_current,
                tls,
            )
            .context(MYSQL_CDC_HINT)?,
        )),
        CdcEngine::Postgres => {
            // A persisted checkpoint proves a prior run happened — if the slot is
            // then MISSING, it was dropped/invalidated and silently recreating it
            // at the current position would skip everything since (a silent gap).
            let resume_expected = cfg
                .checkpoint
                .as_deref()
                .and_then(|p| Position::load(p).ok().flatten())
                .is_some();
            Ok(Box::new(
                crate::source::postgres::cdc::PgChangeStream::open(
                    url,
                    &cfg.slot,
                    resume_expected,
                    tls,
                )
                .context(PG_CDC_HINT)?,
            ))
        }
        CdcEngine::Mssql => {
            let ci = cfg.capture_instance.as_deref().ok_or_else(|| {
                anyhow::anyhow!("sqlserver cdc requires --capture-instance (e.g. dbo_orders)")
            })?;
            // Resume from the checkpoint's LSN if one was persisted (SQL Server has no
            // server-side cursor — the from-LSN is what makes it at-least-once instead
            // of re-reading the whole change table each run).
            let from_lsn = cfg
                .checkpoint
                .as_deref()
                .and_then(|p| Position::load(p).ok().flatten())
                .and_then(|pos| {
                    pos.0
                        .get("lsn")
                        .and_then(|v| v.as_str())
                        .map(str::to_string)
                });
            Ok(Box::new(
                crate::source::mssql::cdc::MssqlChangeStream::from_url(url, ci, from_lsn, tls)
                    .context(MSSQL_CDC_HINT)?,
            ))
        }
    }
}

/// Resolve a CDC table's column type mappings from the source — the **same**
/// `RivetType` → Arrow pipeline the batch export uses — so the typed file sink
/// writes identical columns (logical types `json`/`uuid`/…, real int widths, …)
/// via [`crate::types::build_arrow_field`]. `tls` is threaded through the same
/// gate as the streaming connection. `overrides` are the export's `columns:`
/// declarations — the SAME override surface the batch export honours (e.g.
/// `bigu: decimal(20,0)` for a BigQuery-bound unsigned bigint).
pub(crate) fn resolve_cdc_columns(
    url: &str,
    table: &str,
    tls: Option<&crate::config::TlsConfig>,
    overrides: &crate::types::ColumnOverrides,
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
    let engine = CdcEngine::from_url(url)?;
    let mut src: Box<dyn Source> = match engine {
        CdcEngine::Mysql => Box::new(crate::source::mysql::MysqlSource::connect_with_tls(
            url, tls,
        )?),
        CdcEngine::Postgres => Box::new(crate::source::postgres::PostgresSource::connect_with_tls(
            url, tls,
        )?),
        CdcEngine::Mssql => Box::new(crate::source::mssql::MssqlSource::connect_with_tls(
            url, tls,
        )?),
    };
    let mut mappings = src.type_mappings(&format!("SELECT * FROM {table}"), overrides)?;
    // MySQL: the wire metadata the probe sees has no widths or labels for
    // BIT/BINARY/ENUM/SET ("bit", "binary", "enum"), but the binlog cell fixes
    // (`value::mysql_cell_fix`) need them — BINARY's pad width and ENUM's label
    // list in particular. Enrich `source_native_type` with the full
    // `information_schema.COLUMN_TYPE` ("bit(8)", "binary(4)",
    // "enum('a','b','c')"); CDC-only, so the batch path's contract-pinned
    // native names stay untouched.
    if engine == CdcEngine::Mysql {
        use mysql::prelude::Queryable;
        let pool = crate::source::mysql::connect_pool(url, tls)?;
        let mut conn = pool.get_conn()?;
        let bare = table.rsplit('.').next().unwrap_or(table);
        let full: Vec<(String, String)> = conn.exec(
            "SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
            (bare,),
        )?;
        for m in &mut mappings {
            if let Some((_, ct)) = full.iter().find(|(n, _)| *n == m.column_name) {
                m.source_native_type = ct.clone();
            }
        }
    }
    Ok(mappings)
}

/// One table's destination wiring for a capture — see [`CdcCapture::outputs`].
pub(crate) struct CaptureOutput<'a> {
    pub table: String,
    pub dest: &'a dyn crate::destination::Destination,
    pub dest_uri: String,
    /// The export's `columns:` type overrides for THIS table — already
    /// narrowed by `types::overrides_for_table` (bare keys apply everywhere;
    /// `"table.column"` keys target one table and win over bare).
    pub overrides: crate::types::ColumnOverrides,
}

/// Everything needed to capture a change stream to typed files, assembled once —
/// the source/output differ between the `rivet cdc` CLI and a `mode: cdc` run, but
/// the capture itself (open the stream, resolve the schemas, drive the file sink)
/// is identical. Both entry points fill this in and call [`run_capture`].
/// `outputs` carries one entry per captured table: several tables ride ONE stream
/// (one slot / one binlog connection) and one checkpoint.
pub(crate) struct CdcCapture<'a> {
    pub cdc_cfg: CdcConfig,
    pub outputs: Vec<CaptureOutput<'a>>,
    pub format: crate::config::FormatType,
    pub max_events: Option<usize>,
    pub rollover: usize,
    pub rollover_memory_bytes: Option<usize>,
    /// RFC3339 stamps the caller owns (`Utc::now()` is theirs to call).
    pub run_id: String,
    pub started_at: String,
}

/// Open the change stream (with the engine's permission/TLS gate), resolve each
/// table's typed schema, and drive the commit-seam file sink — the single place
/// the typed CDC capture is assembled. Returns one `RunManifest` per output, in
/// `outputs` order.
pub(crate) fn run_capture(cap: CdcCapture<'_>) -> Result<Vec<crate::manifest::RunManifest>> {
    let url = cap.cdc_cfg.url.clone();
    let tls = cap.cdc_cfg.tls.clone();
    let checkpoint = cap.cdc_cfg.checkpoint.clone();
    let mut stream = create_change_stream(&cap.cdc_cfg)?;
    let engine = CdcEngine::from_url(&url)?;
    let mut outputs = Vec::with_capacity(cap.outputs.len());
    for o in cap.outputs {
        let columns = resolve_cdc_columns(&url, &o.table, tls.as_ref(), &o.overrides)?;
        outputs.push(sink::TableOutput {
            table: o.table,
            columns,
            dest: o.dest,
            dest_uri: o.dest_uri,
        });
    }
    let sink_cfg = sink::SinkConfig {
        outputs,
        engine,
        format: cap.format,
        checkpoint,
        max_events: cap.max_events,
        rollover: cap.rollover,
        rollover_memory_bytes: cap.rollover_memory_bytes,
        started_at: cap.started_at,
        run_id: cap.run_id,
    };
    sink::run_to_files(stream.as_mut(), sink_cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_cdc_columns_rejects_a_non_identifier_table() {
        // The table is interpolated into `SELECT * FROM {table}` for the schema
        // probe — a name carrying a quote / paren / semicolon / space must be
        // refused *before* any connection, so this needs no live database.
        for bad in ["orders; DROP TABLE x", "orders WHERE 1=1", "a b", "o'r", ""] {
            let err = resolve_cdc_columns(
                "mysql://u:p@127.0.0.1:3306/db",
                bad,
                None,
                &crate::types::ColumnOverrides::new(),
            )
            .expect_err(&format!("{bad:?} must be rejected"));
            assert!(
                err.to_string().contains("plain [schema.]table identifier"),
                "{bad:?} → {err}"
            );
        }
    }
}
