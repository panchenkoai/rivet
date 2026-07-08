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
    /// checkpoint that would resume from a corrupt position. Creates the
    /// parent directory: `rivet init --mode cdc` scaffolds
    /// `checkpoint: ./cdc/<table>.ckpt`, and the first client-flow rehearsal
    /// (finding #43) died on the missing `./cdc/` with an ENOENT dressed in
    /// the grants hint — a quickstart-blocking wall for every fresh user.
    pub(crate) fn save(&self, path: &Path) -> Result<()> {
        use anyhow::Context as _;
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating checkpoint directory '{}'", parent.display()))?;
        }
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, serde_json::to_vec(&self.0)?)
            .with_context(|| format!("writing checkpoint '{}'", path.display()))?;
        std::fs::rename(&tmp, path)
            .with_context(|| format!("committing checkpoint '{}'", path.display()))?;
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
    /// Column NAMES of this event's image, when the engine carries them
    /// (PostgreSQL wire text names every column; SQL Server change-table rows
    /// are name-addressable). With names present the sink maps the image BY
    /// NAME into the resolved schema — the positional-mapping corruption
    /// class (findings #37/#41/#42: mid-window DDL shifts, non-first PK
    /// deletes) is unrepresentable. `None` ⇒ positional full row (MySQL
    /// binlog carries no names; its arity guard stays load-bearing).
    pub(crate) image_names: Option<std::sync::Arc<[String]>>,
    /// Ordinal of this change **within its source transaction** (0-based),
    /// stamped by [`TxnSeq`] as the stream is consumed. `position` alone is the
    /// commit position — every change in one transaction shares it — so ordering
    /// a current-state dedup by `position` picks an arbitrary row when a PK is
    /// touched more than once per transaction. `(position, seq)` is the total
    /// order; being log-derived it is identical on an at-least-once re-emit.
    pub(crate) seq: u64,
}

/// Stamps each change with its intra-transaction ordinal ([`ChangeEvent::seq`]).
/// The ordinal resets whenever the change's `position` (the commit position)
/// changes — every change in one transaction shares that position, so this is
/// the reliable transaction boundary on ALL engines. (`committed` cannot serve:
/// the poll-based PostgreSQL / SQL Server adapters read already-committed data
/// and mark EVERY change `committed`, which would reset the ordinal every row.)
/// Being derived from `position` + log order, the ordinal is reproduced exactly
/// on an at-least-once replay.
#[derive(Default)]
pub(crate) struct TxnSeq {
    counter: u64,
    prev: Option<Position>,
}

impl TxnSeq {
    /// Ordinal for a change at commit `position`: 0 when `position` differs from
    /// the previous change (a new transaction), else one more than the last.
    pub(crate) fn next(&mut self, position: &Position) -> u64 {
        if self.prev.as_ref() == Some(position) {
            self.counter += 1;
        } else {
            self.counter = 0;
            self.prev = Some(position.clone());
        }
        self.counter
    }

    pub(crate) fn stamp(&mut self, ev: &mut ChangeEvent) {
        ev.seq = self.next(&ev.position);
    }
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
    let mut txn_seq = TxnSeq::default();
    while let Some(ev) = stream.next_change() {
        let mut ev = ev?;
        txn_seq.stamp(&mut ev);
        // Checkpoint at every commit boundary BEFORE the table filter — the
        // resume position is a stream property; a transaction whose last
        // event lands on an unlisted table must still advance it (mirrors
        // the file sink).
        if ev.committed
            && let Some(p) = &checkpoint
        {
            ev.position.save(p)?;
        }
        if !tables.is_empty()
            && !tables
                .iter()
                .any(|t| sink::table_matches(t, &ev.schema, &ev.table))
        {
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
            "seq": ev.seq,
        });
        println!("{line}");
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
    /// MongoDB-only: render the `document` blob as canonical (type-tagged)
    /// extended JSON — the `source.mongo.json: canonical` mode. Matches the batch
    /// reader so a CDC stream and a full export produce identical `document`
    /// text. Config-driven only; the `rivet cdc` CLI defaults to relaxed.
    pub mongo_canonical: bool,
}

/// How many changes a poll adapter pulls per `peek` — the drain's memory bound.
/// It is NOT a free number: on PostgreSQL the peek is non-consuming and pages
/// forward only when the sink acks (advances the slot) at a `rollover` boundary,
/// so a peek SMALLER than the part rollover starves — the second peek re-reads
/// the same changes, trips `exhausted`, and drops the rest of the backlog. This
/// enum makes that unrepresentable: the acking sink builds [`PeekBound::Sized`]
/// **from its own rollover** (so peek == rollover, always ≥), and the NDJSON
/// driver — which never acks — is [`PeekBound::Unbounded`] (one peek drains
/// everything; the LSN-frontier check ends the stream). There is no way to hand
/// the stream a bare "peek 500" that undershoots the rollover.
#[derive(Debug, Clone, Copy)]
pub(crate) enum PeekBound {
    /// Peek at most this many changes per batch — the sink passes its part
    /// `rollover`, so it is ≥ rollover by construction.
    Sized(usize),
    /// One peek pulls the whole backlog (the non-acking NDJSON path).
    Unbounded,
}

impl PeekBound {
    /// Resolve to a positive `upto_nchanges`-style row cap the adapters clamp to
    /// their SQL arg width. `Unbounded` ⇒ the i32 ceiling (effectively "all").
    pub(crate) fn rows_capped(self) -> usize {
        match self {
            PeekBound::Sized(n) => n.clamp(1, i32::MAX as usize),
            PeekBound::Unbounded => i32::MAX as usize,
        }
    }
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
    Mongo,
}

impl CdcEngine {
    pub(crate) fn from_url(url: &str) -> Result<Self> {
        if url.starts_with("mysql://") {
            Ok(Self::Mysql)
        } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            Ok(Self::Postgres)
        } else if url.starts_with("sqlserver://") || url.starts_with("mssql://") {
            Ok(Self::Mssql)
        } else if url.starts_with("mongodb://") || url.starts_with("mongodb+srv://") {
            Ok(Self::Mongo)
        } else {
            anyhow::bail!(
                "rivet cdc: unsupported source url — expected mysql:// / postgresql:// / sqlserver:// / mongodb://"
            )
        }
    }

    /// Stable lowercase label for metrics / run records / hints.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Mysql => "mysql",
            Self::Postgres => "postgres",
            Self::Mssql => "mssql",
            Self::Mongo => "mongo",
        }
    }

    /// Ensure the resume anchor EXISTS — `initial: snapshot` step ① and the
    /// single entry point for anchor creation (idempotent: a present anchor is
    /// never moved). The per-engine anchor models (see CLAUDE.md):
    /// PG pins server-side at slot creation; MySQL has NO server-side anchor —
    /// the checkpoint is pinned at first open; MSSQL floors at
    /// `fn_cdc_get_min_lsn` without one (over-reads, never skips).
    /// `resume_expected` = prior-run evidence exists — a missing server-side
    /// anchor then fails LOUDLY instead of silently re-anchoring at "current".
    pub(crate) fn ensure_anchor(
        self,
        url: &str,
        slot: &str,
        checkpoint: Option<&std::path::Path>,
        tls: Option<&crate::config::TlsConfig>,
        resume_expected: bool,
    ) -> Result<()> {
        match self {
            Self::Postgres => {
                // Slot creation IS the anchor; open() creates it only on a
                // genuine FIRST run (resume_expected=false).
                // Anchor-only open: it creates the slot and is dropped without
                // reading, so the peek bound is irrelevant.
                drop(crate::source::postgres::cdc::PgChangeStream::open(
                    url,
                    slot,
                    resume_expected,
                    tls,
                    PeekBound::Unbounded,
                )?);
                Ok(())
            }
            Self::Mysql | Self::Mssql | Self::Mongo => {
                let ckpt = checkpoint.ok_or_else(|| {
                    anyhow::anyhow!(
                        "{} cdc: an anchor needs cdc.checkpoint (no server-side anchor exists)",
                        self.label()
                    )
                })?;
                if Position::load(ckpt)?.is_some() {
                    return Ok(()); // anchored already — never move it
                }
                if resume_expected {
                    // Prior-run evidence (a completed snapshot marker) with a
                    // MISSING checkpoint: pinning "current" would silently skip
                    // everything since the loss — and on MSSQL would actively
                    // destroy the min-LSN over-read floor. Fail loudly.
                    anyhow::bail!(
                        "{} cdc: checkpoint '{}' is missing but prior-run evidence exists — \
                         re-snapshot (delete the snapshot/_SUCCESS markers) to accept a new \
                         anchor, or restore the checkpoint file",
                        self.label(),
                        ckpt.display()
                    );
                }
                match self {
                    Self::Mysql => {
                        crate::source::mysql::cdc::MysqlChangeStream::pin_checkpoint_at_current(
                            url, ckpt, tls,
                        )
                    }
                    Self::Mssql => {
                        crate::source::mssql::cdc::pin_checkpoint_at_max_lsn(url, ckpt, tls)
                    }
                    _ => crate::source::mongo::cdc::pin_checkpoint_at_current(url, tls, ckpt),
                }
            }
        }
    }
}

/// Setup/permission hints appended to a CDC start-up error — so a missing grant
/// surfaces the fix, not just a raw driver error. Phrased "if this is a
/// permissions/setup error" because the same call can fail for other reasons.
pub(crate) const MYSQL_CDC_HINT: &str = "if this is a permissions/setup error: MySQL CDC needs binlog_format=ROW plus a REPLICATION SLAVE + REPLICATION CLIENT grant (and SELECT on the table) — see the 'MySQL — the binlog grants' section of docs/reference/cdc.md";
pub(crate) const PG_CDC_HINT: &str = "if this is a permissions/setup error: PostgreSQL CDC needs wal_level=logical and a role with the REPLICATION attribute — see the 'PostgreSQL — the logical slot' section of docs/reference/cdc.md";
pub(crate) const MSSQL_CDC_HINT: &str = "if this is a permissions/setup error: SQL Server CDC must be enabled on the table (sys.sp_cdc_enable_table) with SQL Server Agent running, and the reader needs SELECT on the cdc schema — see the 'SQL Server — CDC change tables' section of docs/reference/cdc.md";
pub(crate) const MONGO_CDC_HINT: &str = "if this is a setup error: MongoDB change streams require a replica set (a single-node replica set is fine) — a standalone mongod cannot watch(); the reader needs a role that can run changeStream (readAnyDatabase / read on the db) — see the 'MongoDB — change streams' section of docs/reference/cdc.md";

/// Construct the right [`ChangeStream`] adapter for the source URL's scheme —
/// dispatching by engine exactly as [`crate::source::create_source`] does for the
/// batch path.
pub(crate) fn create_change_stream(
    cfg: &CdcConfig,
    peek: PeekBound,
) -> Result<Box<dyn ChangeStream>> {
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
                    peek,
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
                crate::source::mssql::cdc::MssqlChangeStream::from_url(
                    url, ci, from_lsn, tls, peek,
                )
                .context(MSSQL_CDC_HINT)?,
            ))
        }
        CdcEngine::Mongo => Ok(Box::new(
            // Whole-database change stream; resumes from the persisted token when
            // one exists. `document` JSON fidelity follows `source.mongo.json`
            // (canonical vs relaxed), so CDC and batch render it identically.
            crate::source::mongo::cdc::MongoChangeStream::open(
                url,
                tls,
                cfg.checkpoint.as_deref(),
                cfg.mongo_canonical,
            )
            .context(MONGO_CDC_HINT)?,
        )),
    }
}

/// Resolve CDC tables' column type mappings from the source — the **same**
/// `RivetType` → Arrow pipeline the batch export uses — so the typed file sink
/// writes identical columns (logical types `json`/`uuid`/…, real int widths, …)
/// via [`crate::types::build_arrow_field`]. Session-based: ONE source
/// connection (plus, for MySQL, one enrichment connection) serves every table
/// of a multi-table export — the per-table constructor cost was 2 connections
/// per table per run.
pub(crate) struct CdcSchemaResolver {
    src: Box<dyn crate::source::Source>,
    /// MySQL-only: one connection for the `information_schema.COLUMN_TYPE`
    /// enrichment (wire metadata has no widths/labels for BIT/BINARY/ENUM/SET).
    enrich: Option<mysql::PooledConn>,
}

impl CdcSchemaResolver {
    pub(crate) fn connect(url: &str, tls: Option<&crate::config::TlsConfig>) -> Result<Self> {
        let engine = CdcEngine::from_url(url)?;
        let src: Box<dyn crate::source::Source> = match engine {
            CdcEngine::Mysql => Box::new(crate::source::mysql::MysqlSource::connect_with_tls(
                url, tls,
            )?),
            CdcEngine::Postgres => Box::new(
                crate::source::postgres::PostgresSource::connect_with_tls(url, tls)?,
            ),
            CdcEngine::Mssql => Box::new(crate::source::mssql::MssqlSource::connect_with_tls(
                url, tls,
            )?),
            // The JSON-blob model has a fixed 2-column schema (`_id`, `document`),
            // resolved by `MongoSource::type_mappings` — same as the batch path.
            CdcEngine::Mongo => {
                Box::new(crate::source::mongo::MongoSource::connect(url, tls, None)?)
            }
        };
        let enrich = match engine {
            CdcEngine::Mysql => Some(crate::source::mysql::connect_pool(url, tls)?.get_conn()?),
            _ => None,
        };
        Ok(Self { src, enrich })
    }

    /// One table's mappings. `overrides` are the export's `columns:`
    /// declarations for THIS table (already narrowed by
    /// `types::overrides_for_table`) — the same override surface batch honours.
    pub(crate) fn resolve(
        &mut self,
        table: &str,
        overrides: &crate::types::ColumnOverrides,
    ) -> Result<Vec<crate::types::TypeMapping>> {
        validate_table_ident(table)?;
        let mut mappings = self
            .src
            .type_mappings(&format!("SELECT * FROM {table}"), overrides)?;
        // MySQL: enrich `source_native_type` with the full
        // `information_schema.COLUMN_TYPE` ("bit(8)", "binary(4)",
        // "enum('a','b','c')") — the binlog cell fixes need widths + labels the
        // wire metadata lacks. CDC-only; batch's contract-pinned native names
        // stay untouched.
        if let Some(conn) = self.enrich.as_mut() {
            use mysql::prelude::Queryable;
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
}

/// Single-table convenience over [`CdcSchemaResolver`] (CLI path + tests).
pub(crate) fn resolve_cdc_columns(
    url: &str,
    table: &str,
    tls: Option<&crate::config::TlsConfig>,
    overrides: &crate::types::ColumnOverrides,
) -> Result<Vec<crate::types::TypeMapping>> {
    // Validate BEFORE connecting, so a hostile table name needs no database.
    validate_table_ident(table)?;
    CdcSchemaResolver::connect(url, tls)?.resolve(table, overrides)
}

/// The table name is interpolated into `SELECT * FROM {table}` for the schema
/// probe — refuse anything but a plain `[schema.]table` identifier (no quote,
/// paren, semicolon, or space can break out).
fn validate_table_ident(table: &str) -> Result<()> {
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
    Ok(())
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
    // Derive the peek bound from the ONE rollover the sink also uses — so the
    // PG peek is always ≥ the part rollover (never starves). The single source
    // of truth for both is `cap.rollover`.
    let mut stream = create_change_stream(&cap.cdc_cfg, PeekBound::Sized(cap.rollover))?;
    // Fault point: stream (and any server-side anchor) opened, nothing read.
    crate::test_hook::maybe_panic_at("cdc_after_open");
    let engine = CdcEngine::from_url(&url)?;
    let mut outputs = Vec::with_capacity(cap.outputs.len());
    // ONE resolver session serves every table (was: 2 fresh connections per
    // table per run — the multi-table per-cycle cost the roast flagged).
    crate::test_hook::maybe_panic_at("cdc_before_resolve");
    let mut resolver = CdcSchemaResolver::connect(&url, tls.as_ref())?;
    for o in cap.outputs {
        let columns = resolver.resolve(&o.table, &o.overrides)?;
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
    /// Finding #43: `rivet init --mode cdc` scaffolds
    /// `checkpoint: ./cdc/<table>.ckpt`; the first save must create the
    /// parent, or every fresh quickstart dies on ENOENT dressed in the
    /// grants hint.
    #[test]
    fn checkpoint_save_creates_missing_parent_directories() {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("cdc").join("nested").join("orders.ckpt");
        let pos = Position(serde_json::json!({"file": "binlog.000001", "pos": 4}));
        pos.save(&path).expect("save must create parents");
        let loaded = Position::load(&path).unwrap().expect("roundtrip");
        assert_eq!(loaded.0["pos"], 4);
        // And the error context names the path, not the grants, when the
        // parent CANNOT be created (a file where the dir should be).
        let blocker = d.path().join("blocked");
        std::fs::write(&blocker, b"file").unwrap();
        let bad = blocker.join("x.ckpt");
        let err = pos.save(&bad).unwrap_err().to_string();
        assert!(
            err.contains("checkpoint directory"),
            "the failure must name the real cause: {err}"
        );
    }

    use super::*;

    // Ultrareview bug_001: the loud-fail-on-missing-anchor promise held only
    // for PostgreSQL. On MySQL/MSSQL a deleted checkpoint with prior-run
    // evidence behind it silently re-pinned at "current" (and on MSSQL that
    // pin actively destroys the min-LSN over-read floor). The bail must fire
    // BEFORE any connection — so this needs no live database.
    #[test]
    fn ensure_anchor_missing_checkpoint_with_evidence_fails_loudly() {
        let d = tempfile::tempdir().unwrap();
        let missing = d.path().join("nonexistent.ckpt");
        for engine in [CdcEngine::Mysql, CdcEngine::Mssql] {
            let err = engine
                .ensure_anchor(
                    "mysql://u:p@127.0.0.1:1/db",
                    "unused",
                    Some(&missing),
                    None,
                    true, // resume evidence exists
                )
                .expect_err("missing checkpoint + evidence must bail, not re-pin");
            let msg = err.to_string();
            assert!(
                msg.contains("prior-run evidence"),
                "{engine:?}: must explain the evidence: {msg}"
            );
        }
    }

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

    #[test]
    fn txn_seq_ordinals_reset_when_commit_position_changes() {
        // `position` (commit-scoped) alone ties every change in a transaction;
        // `__seq` restores intra-transaction order and RESETS when the commit
        // position changes — the reliable txn boundary on every engine (PG/MSSQL
        // mark every change `committed`, so `committed` can't be it).
        let mut ts = TxnSeq::default();
        let pa = Position(serde_json::json!({ "lsn": "A" })); // transaction A
        let pb = Position(serde_json::json!({ "lsn": "B" })); // transaction B
        // A = 3 changes, B = 2 changes.
        let seqs: Vec<u64> = [&pa, &pa, &pa, &pb, &pb]
            .iter()
            .map(|p| ts.next(p))
            .collect();
        assert_eq!(seqs, vec![0, 1, 2, 0, 1]);

        // Same position again after B still counts up within B.
        assert_eq!(ts.next(&pb), 2);
        // A new position resets.
        assert_eq!(ts.next(&Position(serde_json::json!({ "lsn": "C" }))), 0);
    }
}
