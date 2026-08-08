//! SQL Server CDC adapter — `cdc.*` change-table poll → canonical
//! [`cdc::ChangeEvent`]. The structural outlier: no client-streamable log over
//! TDS, so this polls the change tables the server's capture Agent extracted, by
//! LSN window — plain T-SQL over `tiberius` (no CDC-specific crate exists or is
//! needed).
//!
//! `next_change` polls the change function in bounded LSN batches into a buffer
//! and drains it (memory is O(batch), not O(total window)); a
//! continuous daemon wraps [`crate::source::cdc::run`] in an outer poll loop. The
//! runtime + connection are held by the stream (paid once, not per poll).
//!
//! Captured source columns are read generically from each change row
//! (`Row::cells()`) into typed `RivetValue`s — ints/bool/float/string/binary,
//! numeric → exact decimal text, temporal via tiberius+chrono structural
//! `try_get` (no manual DateTime2-increment math). Mirrors `mssql::arrow_convert`.
//!
//! Prereqs (heaviest of the three): CDC enabled, **SQL Server Agent running**,
//! supported edition (not Express). A stalled Agent freezes the change tables AND
//! pins log truncation — a real reader must detect a non-advancing max LSN.
//!
//! `#![allow(dead_code)]`: consumed by `cli::dispatch` (binary crate); the lib
//! crate compiles `source` for tests but has no CDC consumer of its own.
#![allow(dead_code)]

use std::collections::VecDeque;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use serde_json::json;
use tiberius::{AuthMethod, Client, ColumnData, Config, EncryptionLevel, Row};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::config::TlsConfig;
use crate::error::Result;
use crate::source::cdc::value::RivetValue;
use crate::source::cdc::{ChangeEvent, ChangeOp, ChangeStream, DrainMode, Position};
use crate::source::require_tls_or_loopback;

/// Does the capture instance carry every column of its source table?
///
/// `sp_cdc_enable_table @captured_column_list` lets an operator capture a SUBSET,
/// and rivet had NO check for it — `grep -c captured_column_list src/` returned 0
/// while MySQL was being taught to refuse a partial binlog image and PostgreSQL to
/// warn about key-only deletes. SQL Server was the one engine where a partial
/// image is CONFIGURABLE and unchecked.
///
/// `cdc.captured_columns` is the catalog answer, joined to `sys.columns` for what
/// the table actually has. Catalogs, not names.
///
/// Best-effort: a reader without rights on the `cdc` schema answers `Whole` rather
/// than blocking a capture that is otherwise fine.
pub(crate) fn row_image(
    url: &str,
    tls: Option<&TlsConfig>,
    tables: &[String],
    capture_instance: Option<&str>,
) -> crate::source::cdc::RowImage {
    use crate::source::cdc::RowImage;

    if tables.is_empty() {
        return RowImage::Whole;
    }
    let Ok(mut src) = crate::source::mssql::MssqlSource::connect_with_tls(url, tls) else {
        return RowImage::Whole;
    };
    let bare: Vec<String> = tables
        .iter()
        .map(|t| t.rsplit('.').next().unwrap_or(t).to_string())
        .collect();
    let list = bare
        .iter()
        .map(|t| format!("'{}'", t.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(",");
    // Scope by the CAPTURE INSTANCE rivet will actually read, not by table name.
    //
    // Two defects lived in the name-only form, both found by an adversarial pass
    // over this branch and both invisible in the single-instance case that every
    // manual check uses:
    //
    // 1. `cdc.change_tables` holds ONE ROW PER CAPTURE INSTANCE, and the join to
    //    `cdc.captured_columns` is on the CHANGE table (`ct.object_id`) while the
    //    GROUP BY is on the SOURCE table. With two instances — which is exactly
    //    the documented schema-change workflow (create a second, cut over, drop
    //    the old) — their captured columns are SUMMED against a denominator
    //    counted once, so any pair reaching the source column count masks the
    //    partial one. Reading the partial instance then writes NULL for every
    //    omitted column with `status: success`: the harm this gate exists to
    //    refuse. Per-instance grouping alone would NOT do — mid-cutover the old
    //    instance is legitimately short, so grading every instance turns the
    //    drift-recovery path into a false refusal. The instance rivet READS is
    //    the only one whose answer means anything.
    //
    // 2. `WHERE t.name IN (…)` had no schema predicate (the schema is stripped by
    //    `rsplit('.')` above), so `archive.orders` — a different table that merely
    //    shares a name — could make a healthy `dbo.orders` capture hard-fail with
    //    a message naming a table the operator did not configure.
    //
    // The instance predicate closes both: it names one change table, which names
    // one source object_id, in one schema. `open` already validates the instance
    // to `[A-Za-z0-9_]` (see below), so it is safe to inline.
    let sql = match capture_instance {
        Some(ci) => format!(
            "SELECT t.name + ':' + CAST(COUNT(cc.column_name) AS varchar(12)) + '/' + \
                    CAST((SELECT COUNT(*) FROM sys.columns sc WHERE sc.object_id = t.object_id) \
                         AS varchar(12)) \
             FROM cdc.change_tables ct \
             JOIN sys.tables t ON t.object_id = ct.source_object_id \
             JOIN cdc.captured_columns cc ON cc.object_id = ct.object_id \
             WHERE ct.capture_instance = '{ci}' \
             GROUP BY t.name, t.object_id",
            ci = ci.replace('\'', "''"),
        ),
        // No instance configured (not reachable for a real MSSQL CDC export —
        // `open` requires one — but the seam allows it): fall back to the name
        // match, still scoped to ONE instance per group so the sum cannot mask.
        None => format!(
            "SELECT t.name + ':' + CAST(COUNT(cc.column_name) AS varchar(12)) + '/' + \
                    CAST((SELECT COUNT(*) FROM sys.columns sc WHERE sc.object_id = t.object_id) \
                         AS varchar(12)) \
             FROM cdc.change_tables ct \
             JOIN sys.tables t ON t.object_id = ct.source_object_id \
             JOIN cdc.captured_columns cc ON cc.object_id = ct.object_id \
             WHERE t.name IN ({list}) \
             GROUP BY t.name, t.object_id, ct.capture_instance"
        ),
    };
    let Ok(rows) = src.query_single_column(&sql) else {
        return RowImage::Whole;
    };
    let short: Vec<String> = rows
        .iter()
        .filter_map(|r| {
            let (name, counts) = r.split_once(':')?;
            let (got, all) = counts.split_once('/')?;
            (got.trim().parse::<i64>().ok()? < all.trim().parse::<i64>().ok()?)
                .then(|| format!("{name} ({got} of {all} columns)"))
        })
        .collect();
    if short.is_empty() {
        return RowImage::Whole;
    }
    RowImage::Partial {
        why: format!(
            "the capture instance(s) for {} were enabled with a PARTIAL @captured_column_list, so \
             every change event is missing the columns that were left out — the rows would be \
             recorded as NULL rather than as absent. Re-enable the table with the full column \
             list (omit @captured_column_list) and re-run",
            short.join(", ")
        ),
    }
}

/// Build one poll's T-SQL. `bound` pins `@max` to the open-time ceiling
/// (`0x{hex}`, a bounded `until_current` run) instead of re-reading
/// `sys.fn_cdc_get_max_lsn()` per poll (the daemon's chase-the-head mode) — the
/// termination contract of [`MssqlChangeStream::bound`], pure so the bounded
/// shape is asserted without a server.
///
/// TWO WAYS a resume position can sit below `fn_cdc_get_min_lsn`, and only one is
/// data loss. `cdc.change_tables.start_lsn` is where the capture instance BEGAN
/// and never moves; `fn_cdc_get_min_lsn` is the current low watermark, which the
/// cleanup job raises. So:
///
///   * `@from < @start` — the position PREDATES the instance. There is no history
///     before its creation to have lost, so floor to `@min` and over-read, which
///     is SQL Server's anchor model to begin with.
///   * `@start <= @from < @min` — the position was inside the instance's lifetime
///     and the cleanup job removed it. That IS loss, and the THROW is right.
///
/// Without the first case the anchor wedged a brand-new export permanently, and
/// KNOWN GAP, measured and deliberately left in place — see the decision below.
///
/// The floor `IF @from < @start SET @from = @min` rescues a brand-new export
/// whose anchor had been pinned at the DATABASE-wide
/// `sys.fn_cdc_get_max_lsn()`, a position a freshly-enabled instance's own
/// `start_lsn` can legally exceed. It rescued that case and destroyed this one.
/// Measured on a real SQL Server: the cleanup job ADVANCES
/// `cdc.change_tables.start_lsn` and keeps it equal to `fn_cdc_get_min_lsn`
/// (before: both `0x…71F80036`, 5 rows; after a forced cleanup: both
/// `0x…7C980005`, 3 rows). So `@start` and `@min` are one value, `@from < @start`
/// fires exactly when `@from < @min`, and it always ran FIRST — the interval the
/// THROW guards (`@start <= @from < @min`) is empty, the THROW is unreachable,
/// and a position the cleanup job purged past was silently floored to `@min`,
/// skipping every change in between. That is the thing its own message promises
/// not to do.
///
/// Half of it is fixed at the cause: `pin_checkpoint_at_instance_start` now
/// anchors at the INSTANCE's watermark (its `start_lsn` while the capture job has
/// not published a `min_lsn` yet) instead of the database max, so a FRESH export
/// no longer lands below its own instance and no longer needs rescuing.
///
/// The floor stays because a checkpoint written by an EARLIER rivet was pinned at
/// the database max and can still sit below `@min` — and from LSNs alone the two
/// causes are indistinguishable once `start_lsn` moves: "the instance is newer
/// than this position" (floor is right, THROW would wedge the export) and "the
/// cleanup job purged past this position" (THROW is right, floor silently skips).
///
/// The discriminator does not exist on the server; it exists in rivet. A
/// checkpoint is either a PIN (written by `ensure_anchor` before anything was
/// captured) or a RESUME position (written after a flush). Recording which — an
/// optional field, so old files still load — lets the poll floor a pin and throw
/// on a resume. What legacy checkpoints without the field should default to is a
/// real decision with a cost either way (a false alarm that wedges an export, or
/// a silent skip), which is why this is documented rather than guessed.
/// What one poll needs to know. A parameter object rather than five positional
/// arguments, because four of them are `&str`/`Option`/`bool` in a row and the
/// call site said nothing: `fill_sql(ci, expr, 500, None, false)` gives a reader
/// no way to see that the last value decides whether a purged resume position is
/// refused or silently skipped.
struct Poll<'a> {
    /// The capture instance whose change function is read.
    ci: &'a str,
    /// SQL expression yielding `@from` — the position to read AFTER.
    from_expr: &'a str,
    /// Row cap for one batch (`TOP (n)`).
    batch: i64,
    /// `Some(hex)` pins `@max` at the open-time LSN (a bounded drain); `None`
    /// re-reads `fn_cdc_get_max_lsn()` every poll (the daemon).
    bound: Option<&'a str>,
    /// True when `from_expr` came from a PIN rather than a flush. Only a pin may
    /// be floored up to the instance's start.
    from_is_pin: bool,
}

/// Move the read cursor to `to` — which CONSUMES the pin.
///
/// A pin is a guess written by `ensure_anchor` before anything was read, and
/// `fill_sql` may floor it to the capture instance's start: nothing was captured,
/// so nothing can be lost. Once a poll has consumed real changes, `from_lsn` is a
/// position we REACHED, and flooring that is precisely the silent skip the
/// retention THROW exists to prevent.
///
/// The flag used to be set once in the constructor and never cleared, so from the
/// SECOND poll onward a mid-run retention purge was floored instead of thrown.
/// Note the shape of that bug: `fill_sql` was correct on every input it was given
/// — its own unit test feeds `from_is_pin` by hand and passes either way. The
/// defect lived in the SUPPLIER. Hence this is a function both production and the
/// test call, rather than an assignment only a live SQL Server could reach.
fn advance_cursor(from_lsn: &mut Option<String>, from_is_pin: &mut bool, to: String) {
    *from_lsn = Some(to);
    *from_is_pin = false;
}

fn fill_sql(p: Poll<'_>) -> String {
    let Poll {
        ci,
        from_expr,
        batch,
        bound,
        from_is_pin,
    } = p;
    let max_expr = match bound {
        Some(hex) => format!("0x{hex}"),
        None => "sys.fn_cdc_get_max_lsn()".to_string(),
    };
    // The floor exists ONLY for an anchor: a pinned position can legitimately sit
    // below a capture instance that was enabled after it, and throwing there
    // wedges a brand-new export with a diagnosis ("the cleanup job removed it")
    // that is wrong about the cause and unfixable by the remedy it names. A
    // RESUME position below `@min` is the opposite: changes we had reached are
    // gone, and flooring silently skips them. Legacy checkpoints carry no marker
    // and are treated as resume positions — the loud direction, since a false
    // alarm is recoverable and a silent skip is not.
    let floor = if from_is_pin {
        format!(
            "DECLARE @start binary(10) = (SELECT start_lsn FROM cdc.change_tables \
                 WHERE capture_instance = '{ci}'); \
             IF @from IS NOT NULL AND @start IS NOT NULL AND @from < @start SET @from = @min;"
        )
    } else {
        String::new()
    };
    format!(
        "DECLARE @from binary(10) = {from_expr}; \
         DECLARE @min binary(10) = sys.fn_cdc_get_min_lsn('{ci}'); \
         DECLARE @max binary(10) = {max_expr}; \
         {floor} \
         IF @from IS NOT NULL AND @min IS NOT NULL AND @from < @min \
            THROW 51000, 'rivet cdc: the resume position is older than the SQL Server \
CDC change-table retention (the cleanup job removed it). Resuming would silently skip changes \
— re-snapshot the table (mode: full) and restart CDC from a fresh checkpoint.', 1; \
         DECLARE @to binary(10) = NULL; \
         IF @from IS NOT NULL AND @max IS NOT NULL AND @from <= @max \
            SELECT @to = MAX(s) FROM (SELECT TOP ({batch}) __$start_lsn AS s \
                FROM cdc.fn_cdc_get_all_changes_{ci}(@from, @max, N'all') \
                ORDER BY __$start_lsn) q; \
         IF @to IS NOT NULL \
            SELECT * FROM cdc.fn_cdc_get_all_changes_{ci}(@from, @to, N'all') \
            ORDER BY __$start_lsn, __$seqval;"
    )
}

/// Connection parameters for a SQL Server CDC poll stream.
/// Where a poll resumes from, and WHAT that position is.
///
/// One named value rather than two positional arguments, because the pair is a
/// `String` and a `bool` whose meaning is invisible at the call site — and the
/// bool decides whether a position the cleanup job purged past is REFUSED or
/// silently skipped.
pub(crate) struct Resume {
    /// Hex LSN to read after, or `None` for "from the change table's min".
    pub from_lsn: Option<String>,
    /// True when that LSN is an ANCHOR (nothing was captured from it) rather
    /// than a flushed resume position.
    pub from_is_pin: bool,
}

pub(crate) struct MssqlCdcConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
    /// CDC capture instance, e.g. `dbo_orders`. Validated as an identifier
    /// because it is interpolated into the change-function name (which can't be a
    /// bind parameter).
    pub capture_instance: String,
    /// Resume LSN — the hex of the last durably-written `__$start_lsn`. The poll
    /// reads changes *after* it (`fn_cdc_increment_lsn`); `None` ⇒ from the change
    /// table's min LSN (first run). This is what makes SQL Server CDC at-least-once
    /// rather than re-reading the whole retained change table every run.
    pub from_lsn: Option<String>,
    /// True when `from_lsn` came from a PIN (an anchor written before anything
    /// was captured) rather than from a flush. Only a pin may be floored up to
    /// the instance's start; a resume position below `@min` is retention loss and
    /// must THROW. See `fill_sql`.
    pub from_is_pin: bool,
}

/// Polls a CDC change table and yields canonical changes.
pub(crate) struct MssqlChangeStream {
    rt: tokio::runtime::Runtime,
    client: Client<Compat<TcpStream>>,
    capture_instance: String,
    schema: String,
    table: String,
    /// Internal read cursor — advanced per bounded batch so the next poll reads
    /// the following window. NOT the durable resume position: that is the sink's
    /// checkpoint (advanced only after a durable part), so a crash re-reads from
    /// there (at-least-once) regardless of how far this cursor has moved.
    from_lsn: Option<String>,
    /// See `MssqlCdcConfig::from_is_pin`.
    from_is_pin: bool,
    pending: VecDeque<ChangeEvent>,
    /// Max changes to pull per poll — bounds drain memory to O(batch) instead of
    /// O(total change-table window). See [`crate::source::cdc::PeekBound`].
    batch_limit: i64,
    /// A poll that returns no rows has drained the window up to the current max
    /// LSN — the stream ends (the next scheduler run resumes from the checkpoint).
    exhausted: bool,
    /// Open-time max-LSN ceiling (bare hex) for a bounded run: every poll's
    /// `@max` pins here instead of re-reading `fn_cdc_get_max_lsn()`, so the
    /// window cannot recede under sustained writes; `None` (daemon) keeps the
    /// chase-the-head behaviour. The contract lives on [`DrainMode`].
    bound: Option<String>,
}

impl MssqlChangeStream {
    /// Connect and bind to a capture instance. Holds the runtime + connection for
    /// the life of the stream (folds the per-poll runtime/connect smell away).
    ///
    /// A [`DrainMode::BoundedAtOpen`] run snapshots `fn_cdc_get_max_lsn()` once
    /// and pins every poll's `@max` to it — see [`Self::bound`].
    pub(crate) fn open(
        cfg: &MssqlCdcConfig,
        tls: Option<&TlsConfig>,
        peek: crate::source::cdc::PeekBound,
        mode: DrainMode,
        // The `table:` values the config asked for, checked against the catalog
        // identity this stream will actually emit. Empty ⇒ no check (the
        // `rivet cdc` CLI path supplies none).
        configured_tables: &[String],
    ) -> Result<Self> {
        if !cfg
            .capture_instance
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            anyhow::bail!(
                "invalid CDC capture instance name: {:?}",
                cfg.capture_instance
            );
        }
        // The resume LSN is inlined into `0x{hex}` (binary(10) can't be bound), so
        // validate it to even-length hex — no SQL can break out.
        if let Some(lsn) = &cfg.from_lsn
            && (lsn.is_empty() || lsn.len() % 2 != 0 || !lsn.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            anyhow::bail!("mssql cdc: malformed resume LSN {lsn:?}");
        }
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let mut client = rt.block_on(connect(cfg, tls))?;

        // Resolve the REAL schema/table from cdc.change_tables metadata. The
        // previous `<schema>_<table>` name heuristic silently mis-tagged every
        // event when the capture instance was named after an underscored table
        // (`product_catalog` → schema "product", table "catalog"), so the
        // sink's table routing dropped 100% of its changes while the run still
        // reported success. The name is a label; the metadata is the truth.
        // Fall back to the heuristic only if the metadata row is unreadable.
        let meta: Option<(String, String)> = rt.block_on(async {
            let row = client
                .query(
                    "SELECT OBJECT_SCHEMA_NAME(source_object_id), \
                            OBJECT_NAME(source_object_id) \
                     FROM cdc.change_tables WHERE capture_instance = @P1",
                    &[&cfg.capture_instance.as_str()],
                )
                .await
                .ok()?
                .into_row()
                .await
                .ok()??;
            let s: Option<&str> = row.get(0);
            let t: Option<&str> = row.get(1);
            Some((s?.to_string(), t?.to_string()))
        });
        let (schema, table) = meta.unwrap_or_else(|| {
            cfg.capture_instance
                .split_once('_')
                .map(|(s, t)| (s.to_string(), t.to_string()))
                .unwrap_or_else(|| (String::new(), cfg.capture_instance.clone()))
        });

        // The config's name is a LABEL; this identity is the truth — and until
        // now nothing compared them. The sink routes with
        // `cdc::sink::table_matches`, a byte-exact comparison, while SQL Server's
        // default collation is case-INSENSITIVE: `table: dbo.orders` against a
        // catalog `dbo.Orders` resolves its schema perfectly (a full, correct
        // column list) and then matches ZERO events.
        //
        // That is not merely a delay. The commit boundary is recorded BEFORE the
        // routing filter and the end-of-pass roll fires on `unacked_commit`
        // alone, so the checkpoint advances over events that were never
        // captured. Measured: 6 change rows, 0 captured, exit 0 — and re-running
        // with the case FIXED against the same checkpoint recovered NOTHING,
        // while the same run with the checkpoint deleted recovered all 5 events.
        // The rows never left the change table; only the advanced LSN skipped
        // them.
        //
        // Checked with the SAME predicate the sink routes by, deliberately: a
        // check that asks a different question is not a check.
        if !configured_tables.is_empty()
            && !configured_tables
                .iter()
                .any(|c| crate::source::cdc::sink::table_matches(c, &schema, &table))
        {
            anyhow::bail!(
                "sqlserver cdc: capture instance '{}' emits changes for `{}.{}` (from \
                 cdc.change_tables), but no configured table matches it — the config asks for \
                 {:?}. Routing is byte-exact while SQL Server's collation is not, so every \
                 event would be dropped AND the checkpoint would advance past it, losing the \
                 changes for good. Set `table:` to `{}.{}` exactly as the catalog spells it.",
                cfg.capture_instance,
                schema,
                table,
                configured_tables,
                schema,
                table
            );
        }

        // Bounded run: snapshot the ceiling once, at open. A NULL max LSN (CDC
        // not enabled yet) keeps `bound = None` so the first poll surfaces the
        // same loud setup error the daemon path does — never a silent empty run.
        let bound = if mode.is_bounded() {
            let max: Option<String> = rt.block_on(async {
                Ok::<_, anyhow::Error>(
                    client
                        .query(
                            "SELECT CONVERT(varchar(24), sys.fn_cdc_get_max_lsn(), 1)",
                            &[],
                        )
                        .await?
                        .into_row()
                        .await?
                        .and_then(|r| r.get::<&str, _>(0).map(|s| s.to_string())),
                )
            })?;
            let max = max.map(|s| s.trim_start_matches("0x").to_string());
            // The value is inlined into `0x{hex}` in every poll — hold it to the
            // same charset gate as the resume LSN, even though the server made it.
            if let Some(hex) = &max
                && (hex.is_empty()
                    || hex.len() % 2 != 0
                    || !hex.bytes().all(|b| b.is_ascii_hexdigit()))
            {
                anyhow::bail!("mssql cdc: malformed open-time max LSN {hex:?}");
            }
            max
        } else {
            None
        };
        Ok(Self {
            rt,
            client,
            capture_instance: cfg.capture_instance.clone(),
            schema,
            table,
            from_lsn: cfg.from_lsn.clone(),
            from_is_pin: cfg.from_is_pin,
            pending: VecDeque::new(),
            batch_limit: peek.rows_capped() as i64,
            exhausted: false,
            bound,
        })
    }

    /// Open from a `sqlserver://user:pass@host:port/db` URL + a capture instance
    /// (the factory path).
    pub(crate) fn from_url(
        url: &str,
        capture_instance: &str,
        resume: Resume,
        tls: Option<&TlsConfig>,
        peek: crate::source::cdc::PeekBound,
        mode: DrainMode,
        // Passed through to `open`, which cross-checks it against the catalog.
        configured_tables: &[String],
    ) -> Result<Self> {
        // Refuse remote plaintext / unauthenticated TLS before any dial (the gate
        // the batch MssqlSource uses).
        require_tls_or_loopback(url, tls)?;
        let p = crate::source::mssql::parse_mssql_url(url)?;
        Self::open(
            &MssqlCdcConfig {
                host: p.host,
                port: p.port,
                database: p.database,
                user: p.user,
                password: p.password,
                capture_instance: capture_instance.to_string(),
                from_lsn: resume.from_lsn,
                from_is_pin: resume.from_is_pin,
            },
            tls,
            peek,
            mode,
            configured_tables,
        )
    }

    /// Poll ONE **bounded batch** of the change table into `pending`. `@to` is the
    /// `__$start_lsn` of the `batch_limit`-th change, so the window `[@from, @to]`
    /// returns only **whole transactions** (`fn_cdc_get_all_changes` never splits a
    /// `__$start_lsn` group) — memory is O(batch), never O(total window). The
    /// internal cursor then advances to `@to`; the next poll continues past it. A
    /// poll that returns no rows has drained the window up to the current max LSN.
    fn fill(&mut self) -> Result<()> {
        // Only ever called from `next_change` under `!self.exhausted`, so no
        // early-return guard here (matches the PostgreSQL adapter).
        let ci = self.capture_instance.clone();
        // Resume window: read changes *after* the last cursor LSN
        // (`fn_cdc_increment_lsn`); on the first poll (no cursor) start at the
        // change table's min LSN. If the cursor has fallen BELOW the min LSN the
        // cleanup job removed the changes — THROW (forcing a re-snapshot) rather
        // than silently skip. `@to` bounds the batch at a real transaction
        // boundary (the batch_limit-th change's start LSN); NULL LSNs / nothing
        // new leave `@to` NULL and the final SELECT returns zero rows.
        let from_expr = match &self.from_lsn {
            Some(hex) => format!("sys.fn_cdc_increment_lsn(0x{hex})"),
            None => format!("sys.fn_cdc_get_min_lsn('{ci}')"),
        };
        let sql = fill_sql(Poll {
            ci: &ci,
            from_expr: &from_expr,
            batch: self.batch_limit,
            bound: self.bound.as_deref(),
            from_is_pin: self.from_is_pin,
        });
        // The most common SQL Server gotcha — "Invalid object name
        // cdc.fn_cdc_get_all_changes_…" — surfaces here, at the first poll, not at
        // connect. Append the setup hint so the missing CDC enable is obvious.
        let rows = {
            let Self { rt, client, .. } = self;
            rt.block_on(async { client.simple_query(sql).await?.into_first_result().await })
                .map_err(|e| anyhow::Error::new(e).context(crate::source::cdc::MSSQL_CDC_HINT))?
        };
        // Rows are ordered ascending by start LSN, so the last one's `__$start_lsn`
        // is `@to` — the cursor advances there regardless of each row's op.
        let mut max_lsn: Option<String> = None;
        // Collect this batch's events with their start LSN, then mark ONLY the
        // last row of each `__$start_lsn` group (a source transaction) as the
        // commit boundary — the sink rolls only on a committed event, so without
        // this a transaction larger than `rollover` rolls + checkpoints MID-
        // transaction, and a crash before the tail flushes loses it (resume reads
        // strictly after the checkpoint LSN, skipping the rest of the same-LSN
        // group). Mirrors PostgreSQL's per-transaction commit marking.
        let mut batch: Vec<(String, ChangeEvent)> = Vec::new();
        // Round-2 audit #9: running byte footprint — the row cap is a poor bound
        // when cells are large.
        let mut batch_bytes = 0usize;
        for r in &rows {
            let mut op_code = 0i32;
            let mut lsn = String::new();
            let mut values: Vec<RivetValue> = Vec::new();
            // Captured column NAMES ride along — the sink then maps this
            // image by name, making the positional-corruption class
            // (findings #37/#41) unrepresentable on SQL Server too.
            let mut names: Vec<String> = Vec::new();
            for (idx, (col, data)) in r.cells().enumerate() {
                match col.name() {
                    "__$operation" => {
                        if let ColumnData::I32(Some(v)) = data {
                            op_code = *v;
                        }
                    }
                    "__$start_lsn" => {
                        if let ColumnData::Binary(Some(b)) = data {
                            lsn = hex(b);
                        }
                    }
                    n if n.starts_with("__$") => {} // skip other metadata
                    n => {
                        names.push(n.to_string());
                        values.push(cell_to_rivet(r, idx, data));
                    }
                }
            }
            if !lsn.is_empty() {
                max_lsn = Some(lsn.clone());
            }
            let Some(op) = map_op(op_code) else { continue };
            // after-image for insert/update; the key (before-image) for delete
            let (before, after) = match op {
                ChangeOp::Delete => (Some(values), None),
                _ => (None, Some(values)),
            };
            let ev = ChangeEvent {
                op,
                schema: self.schema.clone(),
                table: self.table.clone(),
                before,
                after,
                position: Position(json!({ "lsn": lsn })),
                // Overridden below — the last row of each start-LSN group is
                // the commit boundary.
                committed: false,
                image_names: Some(std::sync::Arc::from(names)),
                seq: 0, // stamped by TxnSeq as the stream is consumed
                poison: None,
            };
            batch_bytes = batch_bytes.saturating_add(ev.estimated_bytes());
            batch.push((lsn.clone(), ev));
            // Memory backstop (matching MySQL's MAX_TX_ROWS): a transaction is
            // buffered whole (never split across parts), and a single
            // `__$start_lsn` group can be arbitrarily large. Bail loudly rather
            // than OOM. `@to` bounds the batch at a group boundary, so a group
            // never straddles two polls — the whole group lands in one `batch`.
            let cap = crate::source::cdc::max_tx_rows();
            if batch.len() > cap {
                anyhow::bail!(
                    "mssql cdc: a single transaction has more than {cap} change rows — \
                     it must be buffered whole (a transaction is never split across parts), so \
                     this would exhaust memory. Split the source transaction, or raise the cap \
                     only if a transaction this large is genuinely expected."
                );
            }
            // Round-2 audit #9: byte backstop — a few large-cell rows stay under
            // the row cap yet exhaust memory.
            let byte_cap = crate::source::cdc::max_tx_bytes();
            if batch_bytes > byte_cap {
                anyhow::bail!(
                    "mssql cdc: a single transaction buffered more than {byte_cap} bytes \
                     (large cells) before its commit — it must be buffered whole, so this would \
                     exhaust memory. Split the source transaction, or raise RIVET_CDC_MAX_TX_BYTES \
                     only if a transaction this large is expected."
                );
            }
        }
        // Mark the commit boundary: a row is the last of its transaction when it
        // is the final row of the batch OR the next row has a different start LSN.
        let n = batch.len();
        for i in 0..n {
            let is_boundary = i + 1 == n || batch[i].0 != batch[i + 1].0;
            batch[i].1.committed = is_boundary;
        }
        for (_, ev) in batch {
            self.pending.push_back(ev);
        }
        match max_lsn {
            // Advance the internal cursor to @to; the next poll reads past it.
            Some(l) => advance_cursor(&mut self.from_lsn, &mut self.from_is_pin, l),
            // No rows ⇒ the window is drained up to the current max LSN. The
            // position did not move, so a pin is still a pin.
            None => self.exhausted = true,
        }
        Ok(())
    }
}

impl ChangeStream for MssqlChangeStream {
    fn next_change(&mut self) -> Option<Result<ChangeEvent>> {
        // Refill a bounded batch whenever the buffer drains, advancing the cursor
        // each time, until a poll returns nothing (window drained to the max LSN).
        while self.pending.is_empty() && !self.exhausted {
            if let Err(e) = self.fill() {
                return Some(Err(e));
            }
        }
        self.pending.pop_front().map(Ok)
    }
}

/// `__$operation` → canonical op. 1=delete, 2=insert, 4=update-after; 3 (update
/// before-image) is skipped — under `N'all'` an update yields only op 4.
fn map_op(code: i32) -> Option<ChangeOp> {
    match code {
        1 => Some(ChangeOp::Delete),
        2 => Some(ChangeOp::Insert),
        4 => Some(ChangeOp::Update),
        _ => None,
    }
}

/// Map a captured source cell to a typed [`RivetValue`]. Temporals use
/// tiberius+chrono's structural `try_get` (no manual DateTime2-increment math);
/// numeric carries its exact unscaled value → decimal text → `Decimal128` at the
/// sink. Mirrors `mssql::arrow_convert`'s per-`ColumnData` handling.
/// The arms that depend ONLY on the cell's own data — no `Row`, no column index.
///
/// Split out so they can be tested at all. `cell_to_rivet` takes a `&Row`, a
/// tiberius type with no public constructor, so for as long as every arm lived
/// inside it NONE of them had a unit test: deleting any of these eleven arms
/// survived the whole lib cycle (eleven standing entries in the mutants
/// baseline). The temporal arms genuinely need the `Row` — tiberius decodes them
/// through `try_get` rather than from `ColumnData` — so they stay behind, and
/// `None` here means exactly "not a data-only arm, ask the row".
fn cell_from_data(data: &ColumnData<'_>) -> Option<RivetValue> {
    Some(match data {
        ColumnData::Bit(Some(b)) => RivetValue::Bool(*b),
        ColumnData::U8(Some(v)) => RivetValue::Int(*v as i64),
        ColumnData::I16(Some(v)) => RivetValue::Int(*v as i64),
        ColumnData::I32(Some(v)) => RivetValue::Int(*v as i64),
        ColumnData::I64(Some(v)) => RivetValue::Int(*v),
        ColumnData::F32(Some(v)) => RivetValue::Float(*v as f64),
        ColumnData::F64(Some(v)) => RivetValue::Float(*v),
        ColumnData::String(Some(s)) => RivetValue::Bytes(s.as_bytes().to_vec()),
        // uniqueidentifier resolves to a UUID column (FixedSizeBinary(16)), so carry
        // the 16 canonical bytes — NOT the 36-char string, which won't fit the
        // fixed-size builder and silently becomes NULL. Mirrors mssql::arrow_convert.
        ColumnData::Guid(Some(g)) => RivetValue::Bytes(g.as_bytes().to_vec()),
        ColumnData::Binary(Some(b)) => RivetValue::Bytes(b.to_vec()),
        ColumnData::Numeric(Some(n)) => {
            RivetValue::Bytes(numeric_to_decimal_string(n.value(), n.scale()).into_bytes())
        }
        _ => return None,
    })
}

/// Microseconds since midnight, truncating sub-microsecond nanos.
///
/// Pulled out of the `Time` arm because it is the only ARITHMETIC in this
/// mapper, and arithmetic is where an operator swap hides: `*`, `+` and `/` here
/// carried six standing baseline entries with nothing able to tell them apart.
fn naive_time_to_micros(t: NaiveTime) -> i64 {
    t.num_seconds_from_midnight() as i64 * 1_000_000 + t.nanosecond() as i64 / 1000
}

fn cell_to_rivet(row: &Row, idx: usize, data: &ColumnData<'_>) -> RivetValue {
    if let Some(v) = cell_from_data(data) {
        return v;
    }
    match data {
        // datetimeoffset is tz-aware — `try_get::<NaiveDateTime>` is the *wrong* type
        // and returns None (silent data loss). Read it as FixedOffset and carry its UTC
        // instant; the resolved column is a tz-aware Timestamp, so the sink writes it
        // identically to the batch export (parity) with the zone preserved.
        ColumnData::DateTimeOffset(_) => row
            .try_get::<chrono::DateTime<chrono::FixedOffset>, _>(idx)
            .ok()
            .flatten()
            .map_or(RivetValue::Null, |dt| RivetValue::DateTime(dt.naive_utc())),
        ColumnData::DateTime(_) | ColumnData::DateTime2(_) | ColumnData::SmallDateTime(_) => row
            .try_get::<NaiveDateTime, _>(idx)
            .ok()
            .flatten()
            .map_or(RivetValue::Null, RivetValue::DateTime),
        ColumnData::Date(_) => row
            .try_get::<NaiveDate, _>(idx)
            .ok()
            .flatten()
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .map_or(RivetValue::Null, RivetValue::DateTime),
        ColumnData::Time(_) => row
            .try_get::<NaiveTime, _>(idx)
            .ok()
            .flatten()
            .map_or(RivetValue::Null, |t| {
                RivetValue::TimeMicros(naive_time_to_micros(t))
            }),
        // every None (NULL) variant + anything unhandled
        _ => RivetValue::Null,
    }
}

/// Render a tiberius `Numeric` (unscaled `value` + `scale`) to exact decimal text.
fn numeric_to_decimal_string(value: i128, scale: u8) -> String {
    let scale = scale as usize;
    if scale == 0 {
        return value.to_string();
    }
    let neg = value < 0;
    let digits = value.unsigned_abs().to_string();
    let digits = if digits.len() <= scale {
        format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits)
    } else {
        digits
    };
    let (int_part, frac) = digits.split_at(digits.len() - scale);
    format!("{}{}.{}", if neg { "-" } else { "" }, int_part, frac)
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

async fn connect(
    cfg: &MssqlCdcConfig,
    tls: Option<&TlsConfig>,
) -> Result<Client<Compat<TcpStream>>> {
    let mut config = Config::new();
    config.host(&cfg.host);
    config.port(cfg.port);
    config.database(&cfg.database);
    config.authentication(AuthMethod::sql_server(&cfg.user, &cfg.password));
    config.encryption(EncryptionLevel::Required);
    // Gate trust_cert exactly as the batch MssqlSource does: verify the chain by
    // default (no trust_cert); trust the named CA when given; accept-any only for
    // an explicit disable / accept-invalid, or for loopback (None — the
    // require_tls_or_loopback gate already ensured a remote host carries a tls block).
    match tls {
        Some(c) if crate::source::mssql::mssql_trusts_cert_without_verify(c) => config.trust_cert(),
        Some(c) => {
            if let Some(ca) = &c.ca_file {
                config.trust_cert_ca(ca);
            }
        }
        None => config.trust_cert(),
    }
    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    Ok(Client::connect(config, tcp.compat_write()).await?)
}

/// Persist the database's CURRENT max LSN to `ckpt` — the anchor for
/// `cdc.initial: snapshot`, taken BEFORE the snapshot read so the change
/// stream overlaps the snapshot instead of gapping it. Fails loudly when CDC
/// is not enabled on the database (no max LSN exists to anchor at).
pub(crate) fn pin_checkpoint_at_max_lsn(
    url: &str,
    ckpt: &std::path::Path,
    tls: Option<&TlsConfig>,
) -> Result<()> {
    let mut src = crate::source::mssql::MssqlSource::connect_with_tls(url, tls)?;
    let probe = src.cdc_health(None)?;
    let Some(max) = probe_max_lsn(&probe) else {
        anyhow::bail!(
            "mssql cdc initial snapshot: sys.fn_cdc_get_max_lsn() is NULL — enable CDC first \
             (EXEC sys.sp_cdc_enable_db) so the anchor exists before the snapshot"
        );
    };
    // `pinned` marks this as an ANCHOR — a position nothing was ever captured
    // from — as opposed to a resume position written after a flush. The poll
    // needs the difference: `@from < @min` means "the instance begins after this
    // position", and only rivet knows whether that is because the instance is
    // newer than an anchor (harmless, floor to @min) or because the cleanup job
    // purged past a position we had actually reached (loss, and the THROW must
    // fire). From LSNs alone the two are indistinguishable, because the cleanup
    // job ADVANCES `cdc.change_tables.start_lsn` and keeps it equal to
    // `fn_cdc_get_min_lsn` — measured on a real server: before a forced cleanup
    // both read 0x…71F80036 with 5 change rows, after it both read 0x…7C980005
    // with 3.
    Position(serde_json::json!({ "lsn": max, "pinned": true })).save(ckpt)
}

/// The probe's max LSN as the bare hex the checkpoint stores (strip `0x`).
fn probe_max_lsn(probe: &crate::source::mssql::MssqlCdcProbe) -> Option<String> {
    if !probe.cdc_enabled {
        return None;
    }
    probe.max_lsn_hex.as_deref().map(|s| {
        s.trim_start_matches("0x")
            .trim_start_matches("0X")
            .to_string()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every data-only `ColumnData` arm, with an INDEPENDENT expectation.
    ///
    /// `cell_to_rivet` takes a `&Row` — no public constructor — so until the
    /// data-only arms were split into `cell_from_data` not one of them could be
    /// reached from a unit test, and deleting ANY of the eleven survived the whole
    /// lib cycle. The expected side here is a hand-written literal, never a value
    /// recomputed with the mapping logic it grades.
    ///
    /// Fixtures are chosen so a wrong arm cannot coincide with a right one: the
    /// integer widths use DIFFERENT magnitudes (a shared `1` would make U8/I16/
    /// I32/I64 indistinguishable), and the floats are values whose f32 and f64
    /// renderings differ.
    #[test]
    fn every_data_only_column_data_arm_maps_to_its_documented_value() {
        use std::borrow::Cow;

        let uuid = tiberius::Uuid::from_bytes([
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10,
        ]);
        let cases: Vec<(&str, ColumnData<'_>, RivetValue)> = vec![
            (
                "bit_true",
                ColumnData::Bit(Some(true)),
                RivetValue::Bool(true),
            ),
            (
                "bit_false",
                ColumnData::Bit(Some(false)),
                RivetValue::Bool(false),
            ),
            ("u8", ColumnData::U8(Some(200)), RivetValue::Int(200)),
            (
                "i16",
                ColumnData::I16(Some(-30_000)),
                RivetValue::Int(-30_000),
            ),
            (
                "i32",
                ColumnData::I32(Some(2_000_000_000)),
                RivetValue::Int(2_000_000_000),
            ),
            (
                "i64",
                ColumnData::I64(Some(9_000_000_000_000_000_000)),
                RivetValue::Int(9_000_000_000_000_000_000),
            ),
            ("f32", ColumnData::F32(Some(0.5)), RivetValue::Float(0.5)),
            (
                "f64",
                ColumnData::F64(Some(-1.25)),
                RivetValue::Float(-1.25),
            ),
            (
                "string",
                ColumnData::String(Some(Cow::Borrowed("héllo"))),
                RivetValue::Bytes("héllo".as_bytes().to_vec()),
            ),
            // uniqueidentifier must carry the 16 CANONICAL BYTES, not the 36-char
            // text — the fixed-size builder nulls anything that is not 16 bytes,
            // which is the silent-loss shape this repo has already been bitten by.
            (
                "guid",
                ColumnData::Guid(Some(uuid)),
                RivetValue::Bytes(vec![
                    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
                    0x0e, 0x0f, 0x10,
                ]),
            ),
            (
                "binary",
                ColumnData::Binary(Some(Cow::Borrowed(&[0xde, 0xad, 0xbe, 0xef]))),
                RivetValue::Bytes(vec![0xde, 0xad, 0xbe, 0xef]),
            ),
            (
                "numeric",
                ColumnData::Numeric(Some(tiberius::numeric::Numeric::new_with_scale(15005, 2))),
                RivetValue::Bytes(b"150.05".to_vec()),
            ),
        ];

        let mut wrong = Vec::new();
        for (label, data, want) in &cases {
            match cell_from_data(data) {
                Some(got) if got == *want => {}
                got => wrong.push(format!("{label}: want {want:?}, got {got:?}")),
            }
        }
        assert!(
            wrong.is_empty(),
            "data-only ColumnData arms diverged from their documented mapping:\n  {}",
            wrong.join("\n  ")
        );

        // A NULL of a data-only type, and a temporal type, must both defer to the
        // row rather than be answered here — otherwise `cell_to_rivet`'s fallback
        // is unreachable and every temporal value silently becomes NULL.
        assert_eq!(cell_from_data(&ColumnData::Bit(None)), None, "NULL defers");
        assert_eq!(
            cell_from_data(&ColumnData::Date(None)),
            None,
            "a temporal arm must NOT be answered from data — it needs the Row"
        );
    }

    /// Pins every arithmetic step of the `time` conversion.
    ///
    /// Deliberately no zeros: at 00:00:00.000000 the `*`, `+` and `/` in this
    /// expression all yield 0, so a fixture at midnight cannot tell the three
    /// operators apart — the same degenerate-fixture trap that hid the MySQL
    /// Time arm's six operator mutants.
    #[test]
    fn naive_time_to_micros_pins_every_arithmetic_step() {
        // 01:02:03 = 3723 s; .456789 s -> 456_789_000 ns -> 456_789 us
        let t = NaiveTime::from_hms_nano_opt(1, 2, 3, 456_789_000).unwrap();
        assert_eq!(naive_time_to_micros(t), 3723 * 1_000_000 + 456_789);
        // sub-microsecond nanos truncate, they do not round
        let t = NaiveTime::from_hms_nano_opt(0, 0, 1, 999).unwrap();
        assert_eq!(naive_time_to_micros(t), 1_000_000);
        // last representable instant of the day, so a `*`/`+` swap cannot coincide
        let t = NaiveTime::from_hms_nano_opt(23, 59, 59, 999_999_000).unwrap();
        assert_eq!(naive_time_to_micros(t), 86_399 * 1_000_000 + 999_999);
    }

    #[test]
    fn numeric_renders_exact_decimal() {
        assert_eq!(numeric_to_decimal_string(15005, 2), "150.05");
        assert_eq!(numeric_to_decimal_string(-7500, 3), "-7.500");
        assert_eq!(numeric_to_decimal_string(42, 0), "42");
        assert_eq!(numeric_to_decimal_string(5, 2), "0.05");
    }

    // The until_current termination contract: a bounded poll pins `@max` to the
    // open-time ceiling — it must never re-read `fn_cdc_get_max_lsn()` (the
    // moving target that keeps a hot table's drain from ever terminating), and
    // the daemon poll must keep doing exactly that.
    #[test]
    fn fill_sql_bounded_pins_max_and_daemon_chases_it() {
        let bounded = fill_sql(Poll {
            ci: "dbo_orders",
            from_expr: "sys.fn_cdc_get_min_lsn('dbo_orders')",
            batch: 500,
            bound: Some("0000002f000004d80005"),
            from_is_pin: false,
        });
        assert!(
            bounded.contains("DECLARE @max binary(10) = 0x0000002f000004d80005;"),
            "bounded poll must pin @max to the open-time LSN: {bounded}"
        );
        assert!(
            !bounded.contains("fn_cdc_get_max_lsn"),
            "bounded poll must not consult the moving max LSN: {bounded}"
        );
        // The min-LSN retention guard must survive the pinning.
        assert!(bounded.contains("sys.fn_cdc_get_min_lsn('dbo_orders')"));
        assert!(bounded.contains("TOP (500)"));

        let daemon = fill_sql(Poll {
            ci: "dbo_orders",
            from_expr: "sys.fn_cdc_increment_lsn(0xabcd)",
            batch: 500,
            bound: None,
            from_is_pin: false,
        });
        assert!(
            daemon.contains("DECLARE @max binary(10) = sys.fn_cdc_get_max_lsn();"),
            "daemon poll keeps chasing the head: {daemon}"
        );
    }

    /// The floor is only ever right for an UNCONSUMED pin, and `fill_sql` cannot
    /// tell — it believes whatever `from_is_pin` it is handed. This pins the
    /// supplier: the first advance must consume the pin, so poll 2 reaches the
    /// retention THROW instead of being floored past a purged span.
    ///
    /// Deliberately NOT a `fill_sql` test. That function's own matrix hand-sets
    /// `from_is_pin` and is correct on both values — it stayed green for the whole
    /// life of the bug, because the wrong value came from the layer above it.
    #[test]
    fn the_first_advance_consumes_the_pin_so_a_purge_is_thrown_not_floored() {
        let mut lsn = None;
        let mut is_pin = true;

        advance_cursor(&mut lsn, &mut is_pin, "0000abcd".to_string());
        assert_eq!(lsn.as_deref(), Some("0000abcd"), "cursor must move");
        assert!(
            !is_pin,
            "consuming real changes turns the anchor into a REACHED position — \
             leaving it pinned makes fill_sql floor a mid-run retention purge, \
             which is the silent skip the THROW exists to prevent"
        );

        // And the floor really does disappear from the SQL the next poll builds —
        // the observable the stream hands downstream.
        let after = fill_sql(Poll {
            ci: "dbo_orders",
            from_expr: "sys.fn_cdc_increment_lsn(0x0000abcd)",
            batch: 500,
            bound: None,
            from_is_pin: is_pin,
        });
        assert!(
            !after.contains("SET @from = @min"),
            "a position we reached must not be floored: {after}"
        );
        assert!(
            after.contains("THROW 51000"),
            "…it must reach the retention guard instead: {after}"
        );
    }

    /// A resume position that PREDATES the capture instance is floored, not
    /// refused — and the refusal survives for the case that is real loss.
    ///
    /// The floor belongs to an ANCHOR and must not exist for a RESUME position.
    ///
    /// A shape assertion on generated SQL is a weak instrument, so this asserts
    /// only what the server's behaviour turns on, and says what it cannot see.
    /// In particular it hand-sets `from_is_pin` and so cannot see whether the
    /// STREAM still deserves that flag — the gap
    /// `the_first_advance_consumes_the_pin_so_a_purge_is_thrown_not_floored`
    /// exists to close.
    ///
    /// The distinction is not cosmetic. `@from < @min` has two causes that no
    /// LSN comparison can separate, because the cleanup job ADVANCES
    /// `cdc.change_tables.start_lsn` and keeps it equal to `fn_cdc_get_min_lsn`
    /// — measured on a real SQL Server: before a forced cleanup both read
    /// `0x…71F80036` over 5 change rows, after it both read `0x…7C980005` over 3.
    /// An earlier version of this test asserted the opposite ("the instance's
    /// creation LSN, which never moves") and, being a shape check, stayed green
    /// while the floor it demanded made the retention THROW unreachable: with
    /// `@start == @min` the floor fires exactly when the guard would have, and
    /// always first, so `@start <= @from < @min` is an empty interval. The live
    /// test that DOES see the server —
    /// `mssql_cdc_resume_past_retention_errors_not_a_silent_gap` — went red and
    /// stayed red.
    ///
    /// So the discriminator comes from rivet, not the server: a pinned anchor may
    /// be floored (the instance is simply newer than it — nothing was captured to
    /// lose), a resume position may not (changes we had reached are gone, and
    /// skipping them silently is the harm).
    #[test]
    fn only_a_pinned_anchor_is_floored_a_resume_position_must_reach_the_retention_guard() {
        let pinned = fill_sql(Poll {
            ci: "dbo_orders",
            from_expr: "0xabcd",
            batch: 500,
            bound: None,
            from_is_pin: true,
        });
        assert!(
            pinned.contains("SET @from = @min"),
            "an anchor below a newer instance must be floored, not thrown on — throwing wedges \
             a brand-new export with a diagnosis that is wrong about the cause: {pinned}"
        );
        let floor_at = pinned.find("SET @from = @min").unwrap();
        let guard_at = pinned
            .find("THROW 51000")
            .expect("the retention guard must survive in both forms");
        assert!(
            floor_at < guard_at,
            "the floor must run BEFORE the guard reads @from, or the guard throws on a \
             position the next statement was about to make valid"
        );

        let resumed = fill_sql(Poll {
            ci: "dbo_orders",
            from_expr: "sys.fn_cdc_increment_lsn(0xabcd)",
            batch: 500,
            bound: None,
            from_is_pin: false,
        });
        assert!(
            !resumed.contains("SET @from = @min"),
            "a RESUME position must reach the retention guard — flooring it silently skips every \
             change the cleanup job purged past, which is what the guard's own message promises \
             not to do: {resumed}"
        );
        assert!(
            resumed.contains("THROW 51000"),
            "…and the guard must be there to reach: {resumed}"
        );
    }

    fn cfg(capture_instance: &str) -> MssqlCdcConfig {
        MssqlCdcConfig {
            from_is_pin: false,
            host: "127.0.0.1".into(),
            // The `mssql-cdc` instance (cdc profile, :1434) — SQL Server Agent on.
            port: 1434,
            database: "rivet".into(),
            user: "sa".into(),
            password: "Rivet_Passw0rd!".into(),
            capture_instance: capture_instance.into(),
            from_lsn: None,
        }
    }

    /// Run arbitrary T-SQL on a throwaway connection (test setup helper).
    fn exec(sql: &str) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut c = connect(&cfg("dbo_cdc_unit"), None).await.unwrap();
            for batch in sql.split(";\n") {
                if !batch.trim().is_empty() {
                    c.simple_query(batch)
                        .await
                        .unwrap()
                        .into_results()
                        .await
                        .unwrap();
                }
            }
        });
    }

    #[test]
    #[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC enabled"]
    fn streams_change_operations() {
        exec(
            "IF OBJECT_ID('cdc_unit','U') IS NOT NULL DROP TABLE cdc_unit;\n\
             CREATE TABLE cdc_unit (id INT PRIMARY KEY, v INT);\n\
             IF (SELECT is_cdc_enabled FROM sys.databases WHERE name='rivet')=0 EXEC sys.sp_cdc_enable_db;\n\
             IF EXISTS(SELECT 1 FROM cdc.change_tables WHERE capture_instance='dbo_cdc_unit') \
               EXEC sys.sp_cdc_disable_table @source_schema='dbo',@source_name='cdc_unit',@capture_instance='dbo_cdc_unit';\n\
             EXEC sys.sp_cdc_enable_table @source_schema='dbo',@source_name='cdc_unit',@role_name=NULL,@capture_instance='dbo_cdc_unit',@supports_net_changes=0",
        );
        exec(
            "INSERT INTO cdc_unit VALUES (1,10);\n\
             UPDATE cdc_unit SET v=20 WHERE id=1;\n\
             DELETE FROM cdc_unit WHERE id=1",
        );
        // let the capture Agent job scan the log (~5 s cycle)
        std::thread::sleep(std::time::Duration::from_secs(8));

        let mut s = MssqlChangeStream::open(
            &cfg("dbo_cdc_unit"),
            None,
            crate::source::cdc::PeekBound::Sized(10_000),
            DrainMode::Continuous,
            &[], // no configured tables in this fixture → cross-check inactive
        )
        .unwrap();
        let mut ops = Vec::new();
        while let Some(ev) = s.next_change() {
            ops.push(ev.unwrap().op);
        }
        exec(
            "EXEC sys.sp_cdc_disable_table @source_schema='dbo',@source_name='cdc_unit',@capture_instance='dbo_cdc_unit'",
        );
        assert_eq!(
            ops,
            vec![ChangeOp::Insert, ChangeOp::Update, ChangeOp::Delete],
            "CDC change table must yield insert(2), update-after(4), delete(1)"
        );
    }
}
