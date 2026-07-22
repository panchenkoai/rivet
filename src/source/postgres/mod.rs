//! PostgreSQL `Source` implementation.
//!
//! Module layout:
//!
//! - `mod.rs` (this file) — `PostgresSource` struct + connect/TLS path, the
//!   transaction-pooler detector, `PgTxnGuard`, sampling helpers
//!   (`sample_temp_bytes`, `pg_sample_checkpoints_req`, `pg_fetch_work_mem_bytes`),
//!   `introspect_pg_table_for_chunking`, the cursor + FETCH export loop
//!   (`pg_run_export`), the `Source` trait impl, and the catalog-hint
//!   resolver that bridges parsed FROM clauses to `pg_catalog`.
//! - [`arrow_convert`] — the entire row → Arrow `RecordBatch` pipeline: type
//!   mapping (`pg_columns_to_schema`, `rivet_type_for_pg_column`), per-cell
//!   decoders (INTERVAL, UUID, enum, NUMERIC), and the array builders. Kept
//!   in a sibling because it is the largest single-purpose cluster in this
//!   driver (~620 LoC) and has zero reverse dependency back into the
//!   connection / cursor layer.
//! - [`from_parse`] — pure `&str`/`&[u8]` parser that extracts the simple
//!   `<schema>.<table>` literal from a user query so the catalog-hint path
//!   can cast it to `regclass`.  Zero postgres-crate dependency, fully
//!   unit-tested in isolation.

mod arrow_convert;
pub(crate) mod cdc;
mod from_parse;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use postgres::types::Type;
use postgres::{Client, NoTls};

use crate::config::{SourceType, TlsConfig};
use crate::error::Result;
use crate::source::batch_controller::AdaptiveBatchController;
use crate::source::query::build_export_query;
use crate::source::tls::build_native_tls;
use crate::tuning::SourceTuning;
use crate::types::{ColumnOverrides, SourceColumn, TypeMapping};

use arrow_convert::{pg_columns_to_schema, rivet_type_for_pg_column, rows_to_record_batch_typed};
use from_parse::try_parse_pg_simple_from_regclass_literal;

pub struct PostgresSource {
    client: Client,
    /// True when two consecutive pg_backend_pid() calls returned different values,
    /// indicating a transaction-mode connection pooler (pgBouncer, Odyssey, etc.).
    transaction_pooler: bool,
}

/// Detect whether the connection is going through a transaction-mode pooler
/// (pgBouncer, Odyssey, etc.) by comparing backend PIDs across two implicit
/// transactions. Returns true when PIDs differ — impossible on a direct
/// connection or session-mode pooler where the same physical backend is kept.
///
/// False negatives are possible when pool_size = 1 (the same backend is always
/// reused), so this is a best-effort warning rather than a hard guarantee.
fn detect_pg_transaction_pooler(client: &mut Client) -> bool {
    let pid1: Option<i32> = client
        .query_one("SELECT pg_backend_pid()", &[])
        .ok()
        .and_then(|r| r.try_get(0).ok());
    let pid2: Option<i32> = client
        .query_one("SELECT pg_backend_pid()", &[])
        .ok()
        .and_then(|r| r.try_get(0).ok());
    matches!((pid1, pid2), (Some(a), Some(b)) if a != b)
}

impl PostgresSource {
    /// Connect with no transport security (legacy path). Prefer [`Self::connect_with_tls`]
    /// for production workloads so credentials and result sets are not visible on the wire.
    pub fn connect(url: &str) -> Result<Self> {
        let mut client = Client::connect(url, NoTls)?;
        let transaction_pooler = detect_pg_transaction_pooler(&mut client);
        if transaction_pooler {
            log::warn!(
                "transaction-mode connection pooler detected (pgBouncer/Odyssey) — \
                 SET LOCAL tuning is transaction-scoped; \
                 LISTEN/NOTIFY and advisory locks are unavailable"
            );
        }
        Ok(Self {
            client,
            transaction_pooler,
        })
    }

    /// Connect honoring the user's [`TlsConfig`]. When `tls.mode` is
    /// [`TlsMode::Disable`] this falls back to [`Self::connect`].
    pub fn connect_with_tls(url: &str, tls: Option<&TlsConfig>) -> Result<Self> {
        // Refuse remote plaintext (no `tls:` block) before any dial (CWE-319).
        crate::source::require_tls_or_loopback(url, tls)?;
        match tls {
            Some(cfg) if cfg.mode.is_enforced() => {
                let connector = build_native_tls(cfg)?;
                let make_tls = postgres_native_tls::MakeTlsConnector::new(connector);
                let mut client = Client::connect(url, make_tls)?;
                let transaction_pooler = detect_pg_transaction_pooler(&mut client);
                if transaction_pooler {
                    log::warn!(
                        "transaction-mode connection pooler detected (pgBouncer/Odyssey) — \
                         SET LOCAL tuning is transaction-scoped; \
                         LISTEN/NOTIFY and advisory locks are unavailable"
                    );
                }
                Ok(Self {
                    client,
                    transaction_pooler,
                })
            }
            _ => Self::connect(url),
        }
    }
}

/// RAII guard for an open `BEGIN ... COMMIT` block.
///
/// `commit()` runs `COMMIT` and marks the txn done; if the guard is dropped
/// before `commit()` (early return, `?`-bubbled error, or panic-driven unwind),
/// `Drop` issues a best-effort `ROLLBACK`. Postgres releases any open cursors
/// as part of ROLLBACK, so the cursor declared inside the txn is also cleaned
/// up. Closes the **G1** gap from the DBA audit (cursor leak on panic).
struct PgTxnGuard<'a> {
    client: &'a mut Client,
    committed: bool,
}

impl<'a> PgTxnGuard<'a> {
    fn begin(client: &'a mut Client) -> Result<Self> {
        client.batch_execute("BEGIN")?;
        Ok(Self {
            client,
            committed: false,
        })
    }

    fn client_mut(&mut self) -> &mut Client {
        self.client
    }

    fn commit(mut self) -> Result<()> {
        self.client.batch_execute("COMMIT")?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for PgTxnGuard<'_> {
    fn drop(&mut self) {
        if !self.committed
            && let Err(e) = self.client.batch_execute("ROLLBACK")
        {
            // Drop must not panic. Worst case the connection is poisoned and
            // the pool recycles it; log so operators see it.
            log::warn!("PgTxnGuard: ROLLBACK during drop failed: {e:#}");
        }
    }
}

/// Snapshot `pg_stat_database.temp_bytes` for the current database.
///
/// Used by the pipeline job to compute per-run cursor / sort spill: we capture
/// the cluster-wide counter immediately before and after each export and
/// surface the delta on the run summary card. Failures (connect, query) return
/// `None` — the metric is informational, not a correctness signal.
///
/// Note this is a cluster-level counter: concurrent activity from other
/// connections during the run inflates the delta. For a single-tenant test
/// box (the common pilot setup) it is accurate; for shared hosts it is a
/// noisy upper bound, useful as a "your workload was loud" signal.
pub(crate) fn sample_temp_bytes(url: &str, tls: Option<&TlsConfig>) -> Option<i64> {
    let mut client = connect_client(url, tls).ok()?;
    client
        .query_one(
            "SELECT temp_bytes::bigint FROM pg_stat_database WHERE datname = current_database()",
            &[],
        )
        .ok()
        .and_then(|r| r.try_get::<_, i64>(0).ok())
}

/// Snapshot the broader source-harm counters from `pg_stat_database` for the
/// current database — a superset of [`sample_temp_bytes`] (which the run summary
/// tracks on its own). Returns `(metric, cumulative_value)` pairs; the pipeline
/// captures these before and after the export and stores the per-metric delta in
/// `export_harm`.
///
/// All counters live in `pg_stat_database` and are readable by **any** role — no
/// `pg_monitor` membership or superuser needed (unlike `pg_stat_activity`'s view
/// of other sessions). These are cluster-level cumulative counters, so concurrent
/// activity inflates the delta; on a single-tenant pilot box it is the run's own
/// footprint. `None` on connect/query failure — informational, never blocks the
/// export.
pub(crate) fn sample_harm_counters(
    url: &str,
    tls: Option<&TlsConfig>,
) -> Option<Vec<(String, i64)>> {
    let mut client = connect_client(url, tls).ok()?;
    // `tup_returned` (rows the engine had to scan) is the read-amplification
    // signal; `blks_read`/`blks_hit` the I/O vs cache split; `temp_files` the
    // spill count; `deadlocks` contention. temp_bytes is intentionally omitted —
    // it's already on the run summary (export_metrics.pg_temp_bytes_delta).
    let row = client
        .query_one(
            "SELECT blks_read::bigint, blks_hit::bigint, tup_returned::bigint, \
             tup_fetched::bigint, temp_files::bigint, deadlocks::bigint \
             FROM pg_stat_database WHERE datname = current_database()",
            &[],
        )
        .ok()?;
    let names = [
        "pg_blks_read",
        "pg_blks_hit",
        "pg_tup_returned",
        "pg_tup_fetched",
        "pg_temp_files",
        "pg_deadlocks",
    ];
    let mut out = Vec::with_capacity(names.len());
    for (i, name) in names.iter().enumerate() {
        if let Ok(v) = row.try_get::<_, i64>(i) {
            out.push(((*name).to_string(), v));
        }
    }
    Some(out)
}

/// Probe `SHOW work_mem` and return the value in bytes.
///
/// PostgreSQL spills FETCH-cursor output to `pgsql_tmp/` once the in-flight
/// row set exceeds `work_mem` — on wide rows with the default 4 MB the spill
/// fires on every chunk and dominates `pg_stat_database.temp_bytes`. Knowing
/// the value lets the cursor loop cap FETCH N below `work_mem × 0.7`, keeping
/// the result set in memory.
///
/// Returns None on any parse / query failure — the cursor loop falls back to
/// the configured static batch_size in that case.
fn pg_fetch_work_mem_bytes(client: &mut Client) -> Option<i64> {
    let raw: Option<String> = client
        .query_one("SHOW work_mem", &[])
        .ok()
        .and_then(|r| r.try_get::<_, String>(0).ok());
    raw.as_deref().and_then(parse_work_mem)
}

/// Parse a `SHOW work_mem` value like `"4MB"`, `"16384kB"`, `"1GB"`, or a bare
/// number-of-kB string (the older PG default unit) into a byte count. Returns
/// `None` for anything else so callers can decide whether to fall back.
fn parse_work_mem(raw: &str) -> Option<i64> {
    let s = raw.trim();
    // Split numeric prefix from optional unit.
    let mut split = 0;
    for (i, ch) in s.char_indices() {
        if !ch.is_ascii_digit() && ch != '.' && ch != '-' {
            split = i;
            break;
        }
        split = i + ch.len_utf8();
    }
    if split == 0 {
        return None;
    }
    let (num_str, unit) = s.split_at(split);
    let num: f64 = num_str.parse().ok()?;
    let unit = unit.trim().to_ascii_lowercase();
    let multiplier: f64 = match unit.as_str() {
        // Postgres always uses 1024-based units, matching the syntax it
        // accepts in postgresql.conf.
        "" | "kb" => 1024.0,
        "mb" => 1024.0 * 1024.0,
        "gb" => 1024.0 * 1024.0 * 1024.0,
        "tb" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => return None,
    };
    let bytes = (num * multiplier) as i64;
    (bytes > 0).then_some(bytes)
}

/// Sample `checkpoints_req` from `pg_stat_bgwriter`.
///
/// PostgreSQL caches the statistics snapshot at the start of each transaction.
/// We call `pg_stat_clear_snapshot()` first to discard that cache so every
/// adaptive sample sees fresh counters rather than the frozen value from BEGIN.
fn pg_sample_checkpoints_req(client: &mut Client) -> Option<i64> {
    let _ = client.execute("SELECT pg_stat_clear_snapshot()", &[]);
    client
        .query_one("SELECT checkpoints_req FROM pg_stat_bgwriter", &[])
        .ok()
        .and_then(|r| r.try_get::<_, i64>(0).ok())
}

/// Probe `pg_class` and `pg_index` for the stats chunked-mode planning needs.
///
/// Returns a [`crate::source::TableIntrospection`] populated from one connection
/// (two round-trips total: one stats query, one PK query). Failure to connect
/// or to query bubbles up as `Err`; missing rows or unanalyzed tables are
/// represented as zero/None in the result so callers can decide policy.
///
/// The `qualified_table` argument is `<schema>.<table>` (e.g. `public.users`)
/// or bare `<table>` (resolved under `public`). It is split internally with
/// the same strict rules as the `table:` YAML shortcut — anything more
/// elaborate must use the explicit-column path.
pub(crate) fn introspect_pg_table_for_chunking(
    url: &str,
    tls: Option<&TlsConfig>,
    qualified_table: &str,
) -> Result<crate::source::TableIntrospection> {
    let (schema, table) = match qualified_table.split_once('.') {
        Some((s, t)) => (s.to_string(), t.to_string()),
        None => ("public".to_string(), qualified_table.to_string()),
    };
    let mut client = connect_client(url, tls)?;

    // ── reltuples + heap size, in one shot ──────────────────────────────
    let (row_estimate, rel_size_bytes) = match client.query_opt(
        "SELECT c.reltuples::bigint, pg_relation_size(c.oid)::bigint \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1::text AND c.relname = $2::text",
        &[&schema, &table],
    )? {
        Some(row) => {
            let rt: i64 = row.try_get(0).unwrap_or(0);
            let sz: i64 = row.try_get(1).unwrap_or(0);
            (rt.max(0), sz.max(0))
        }
        None => (0, 0),
    };
    let avg_row_bytes = if row_estimate > 0 {
        Some(rel_size_bytes / row_estimate)
    } else {
        None
    };

    // ── single int PK probe ─────────────────────────────────────────────
    let pk_rows = client.query(
        "SELECT a.attname::text, t.typname::text \
         FROM pg_index i \
         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
         JOIN pg_type t ON t.oid = a.atttypid \
         WHERE i.indrelid = (($1::text || '.' || $2::text)::regclass) \
           AND i.indisprimary",
        &[&schema, &table],
    )?;
    let single_int_pk = if pk_rows.len() == 1 {
        let col: String = pk_rows[0].get(0);
        let pg_type: String = pk_rows[0].get(1);
        // Only integer-family types are safe for range chunking via min/max →
        // BETWEEN slicing. Text/UUID/decimal would need different splitting
        // logic and are excluded from auto-resolution.
        if matches!(pg_type.as_str(), "int2" | "int4" | "int8") {
            Some(col)
        } else {
            log::debug!(
                "introspect_pg_table: PK '{col}' on {schema}.{table} has non-int type '{pg_type}' — skipping auto-resolve"
            );
            None
        }
    } else {
        None
    };

    // ── keyset keys (OPT-4): single-column, NOT NULL, UNIQUE indexes ────
    // `indnkeyatts = 1` keeps single-column indexes; `indkey[0] = a.attnum`
    // binds to a real column (not an expression index); `attnotnull` removes
    // NULL-ordering ambiguity. Index-backed + unique ⇒ keyset's `ORDER BY key
    // LIMIT n` is a range scan and `WHERE key > last` never skips dup keys.
    let keyset_rows = client.query(
        "SELECT a.attname::text, i.indisprimary \
         FROM pg_index i \
         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0] \
         WHERE i.indrelid = (($1::text || '.' || $2::text)::regclass) \
           AND i.indisunique AND i.indnkeyatts = 1 AND a.attnotnull",
        &[&schema, &table],
    )?;
    let mut keyset_keys: Vec<String> = Vec::new();
    for primary in [true, false] {
        for row in &keyset_rows {
            let col: String = row.get(0);
            let is_primary: bool = row.get(1);
            if is_primary == primary && !keyset_keys.contains(&col) {
                keyset_keys.push(col);
            }
        }
    }

    Ok(crate::source::TableIntrospection {
        single_int_pk,
        keyset_keys,
        row_estimate,
        avg_row_bytes,
    })
}

/// Open a bare `postgres::Client` honoring the configured TLS policy.
///
/// Shared by preflight, doctor, and `rivet init` so every code path that
/// connects to Postgres applies the same transport-security rules. Preflight
/// and doctor pass the YAML `tls:` block; init runs before any YAML exists,
/// so it derives a `TlsConfig` from the URL's `sslmode` parameter (see
/// `crate::init::postgres::connect`). `tls = None` or `mode: disable` falls
/// back to the insecure `NoTls` transport — a warning is logged from
/// `create_source` so operators know TLS is off.
pub(crate) fn connect_client(url: &str, tls: Option<&TlsConfig>) -> Result<Client> {
    // Refuse remote plaintext (no `tls:` block) before any dial (CWE-319).
    crate::source::require_tls_or_loopback(url, tls)?;
    match tls {
        Some(cfg) if cfg.mode.is_enforced() => {
            let connector = build_native_tls(cfg)?;
            let make_tls = postgres_native_tls::MakeTlsConnector::new(connector);
            Ok(Client::connect(url, make_tls)?)
        }
        _ => Ok(Client::connect(url, NoTls)?),
    }
}

/// Run the full export transaction against an open Postgres client.
///
/// All session-mutating SET commands use SET LOCAL so they are scoped to
/// the transaction and reset automatically on COMMIT or ROLLBACK. The caller
/// is responsible for issuing ROLLBACK if this function returns Err.
///
/// Returns (total_rows, had_schema). had_schema is false only when the query
/// returned zero rows; the caller must emit an empty schema in that case.
fn pg_run_export(
    client: &mut Client,
    built_sql: &str,
    tuning: &SourceTuning,
    column_overrides: &ColumnOverrides,
    sink: &mut dyn super::BatchSink,
    numeric_hints: Option<&HashMap<String, (u8, i8)>>,
) -> Result<(usize, bool)> {
    // Open the txn under guard *first* — if SET LOCAL or DECLARE fails below,
    // Drop will roll back. Without the guard, a failure between BEGIN and the
    // explicit ROLLBACK in the caller would leak a half-set-up txn into the pool.
    let mut guard = PgTxnGuard::begin(client)?;
    // Pin the read txn's TimeZone to UTC. The row READ is UTC-absolute (the binary
    // protocol yields instants regardless of session zone), but the incremental /
    // keyset cursor boundary is re-injected as an OFFSET-LESS naive-UTC literal
    // into `WHERE col > '<literal>'`; PostgreSQL coerces a naive literal to
    // `timestamptz` using the SESSION TimeZone, so on any non-UTC session (a common
    // production default via postgresql.conf or `ALTER ROLE/DATABASE ... SET
    // timezone`) the boundary shifts by the zone offset and every incremental run
    // silently SKIPS (west of UTC) or DUPLICATES (east) the offset-wide gap window —
    // a count-passing data loss invisible under a UTC test session. The MySQL path
    // pins the equivalent (`SET time_zone='+00:00'`). SET LOCAL is txn-scoped, so it
    // auto-resets on commit/rollback and never leaks into the pooled connection.
    guard
        .client_mut()
        .batch_execute("SET LOCAL TimeZone = 'UTC'")?;
    if tuning.statement_timeout_s > 0 {
        guard.client_mut().batch_execute(&format!(
            "SET LOCAL statement_timeout = '{}s'",
            tuning.statement_timeout_s
        ))?;
    }
    if tuning.lock_timeout_s > 0 {
        guard.client_mut().batch_execute(&format!(
            "SET LOCAL lock_timeout = '{}s'",
            tuning.lock_timeout_s
        ))?;
    }
    // Cap FETCH N under `work_mem × 0.7` so the cursor never spills to
    // `pgsql_tmp/`. Without this, a wide-row chunk with the default
    // `batch_size: 50000` × ~4 KB/row = ~200 MB easily exceeds the typical
    // `work_mem: 4 MB` and writes the entire chunk to disk before the first
    // FETCH returns. Measured cost on the content_items bench: ~3.2 GB of
    // temp_bytes per export, dominating the DB-side signal report.
    let work_mem_bytes = pg_fetch_work_mem_bytes(guard.client_mut());

    guard
        .client_mut()
        .batch_execute(&format!("DECLARE _rivet NO SCROLL CURSOR FOR {built_sql}"))?;

    // The first FETCH is intentionally a small `PROBE_BATCH_SIZE` row-width
    // probe (the controller starts there): without it we can't know
    // `arrow_bytes/row` before the cursor runs, and a single FETCH of
    // `tuning.batch_size` × wide rows already triggers a `pgsql_tmp/` spill.
    let configured_batch_size = tuning.batch_size;
    // Shared batch-size state machine; PG provides the FETCH N row source, the
    // work_mem (or schema-derived) cap target, and the checkpoint pressure proxy.
    let mut ctl = AdaptiveBatchController::new(tuning, configured_batch_size);
    ctl.seed_pressure(if tuning.adaptive {
        pg_sample_checkpoints_req(guard.client_mut()).map(|v| v as u64)
    } else {
        None
    });
    let mut schema: Option<SchemaRef> = None;
    let mut columns_cache: Option<Vec<(String, Type)>> = None;
    let mut total_rows: usize = 0;
    let mut cap_applied = false;
    // Per-value ceiling (MB→bytes; `0`/None disables), enforced pre-allocation
    // inside the batch builder so an oversized cell bails before Arrow reserves
    // the buffer. Same source of truth as the sink's backstop guard.
    let max_value_bytes = tuning.max_value_bytes();

    loop {
        let requested = ctl.target();
        let fetch_sql = format!("FETCH {} FROM _rivet", requested);
        let rows = guard.client_mut().query(&fetch_sql, &[])?;
        if rows.is_empty() {
            break;
        }

        if schema.is_none() {
            let stmt_cols: Vec<(String, Type)> = rows[0]
                .columns()
                .iter()
                .map(|c| (c.name().to_string(), c.type_().clone()))
                .collect();
            let s = Arc::new(pg_columns_to_schema(
                rows[0].columns(),
                column_overrides,
                numeric_hints,
            )?);
            sink.on_schema(s.clone())?;
            // When work_mem can't be read, fall back to the schema-derived
            // effective batch size as the cap target (controller clamps it).
            if work_mem_bytes.is_none() {
                let effective = tuning.effective_batch_size(Some(&s));
                ctl.apply_memory_cap(effective.max(requested));
                cap_applied = true;
            }
            schema = Some(s);
            columns_cache = Some(stmt_cols);
        }

        let row_count = rows.len();
        total_rows += row_count;

        let s = schema.as_ref().expect("schema set on first iteration");
        let cols = columns_cache
            .as_ref()
            .expect("columns set on first iteration");
        let batch = rows_to_record_batch_typed(s, cols, &rows, max_value_bytes)?;
        drop(rows);

        // After the first (probe) batch we know the actual row width. Cap the
        // FETCH N below `work_mem × 0.7` so the cursor never spills:
        //   pg_row_bytes ≈ arrow_per_row × 1.2 ; safe = work_mem×0.7 / pg_row_bytes
        // The controller clamps it to the configured `batch_size`.
        if !cap_applied
            && let Some(wm) = work_mem_bytes
            && row_count > 0
        {
            let arrow_bytes = crate::tuning::SourceTuning::batch_memory_bytes(&batch);
            let arrow_per_row = (arrow_bytes / row_count).max(1);
            let pg_per_row = ((arrow_per_row * 12) / 10).max(64);
            let safe = (((wm as f64) * 0.7) as usize / pg_per_row).max(100);
            let mut target = safe;
            if let Some(mem_mb) = tuning.batch_size_memory_mb {
                let arrow_target = (mem_mb * 1024 * 1024) / arrow_per_row;
                target = target.min(arrow_target.max(100));
            }
            if let Some(new) = ctl.apply_memory_cap(target) {
                log::info!(
                    "PG work_mem={} B, observed row={} B (arrow), pg≈{} B → FETCH N → {} (configured={})",
                    wm,
                    arrow_per_row,
                    pg_per_row,
                    new,
                    configured_batch_size,
                );
            }
            cap_applied = true;
        }

        sink.on_batch(&batch)?;

        if let Some((new, under_pressure)) =
            ctl.after_batch(|| pg_sample_checkpoints_req(guard.client_mut()).map(|v| v as u64))
        {
            log::info!(
                "adaptive batch size → {} ({})",
                new,
                if under_pressure {
                    "pressure"
                } else {
                    "recovery"
                }
            );
        }

        log::info!("fetched {} rows so far...", total_rows);

        if row_count < requested {
            break;
        }
        ctl.throttle(row_count);
    }

    // Explicit CLOSE is technically redundant — COMMIT releases the cursor —
    // but it documents intent and surfaces any close errors before COMMIT.
    guard.client_mut().batch_execute("CLOSE _rivet")?;
    guard.commit()?;
    Ok((total_rows, schema.is_some()))
}

impl super::Source for PostgresSource {
    fn export(
        &mut self,
        request: &super::ExportRequest<'_>,
        sink: &mut dyn super::BatchSink,
    ) -> Result<()> {
        let built = build_export_query(request, SourceType::Postgres);
        debug_assert!(
            built.cursor_param.is_none(),
            "Postgres path inlines cursor values as E'…' literals — binding is unused"
        );
        log::debug!(
            "executing query (connection={}): {}",
            if self.transaction_pooler {
                "transaction-pooler"
            } else {
                "direct"
            },
            built.sql
        );

        // Resolve NUMERIC precision from the *unwrapped* base query when the
        // caller wrapped `query` in a chunk/keyset subquery (which hides the
        // source table from the catalog parser). Falls back to `query`.
        let hint_query = request.catalog_hint_query.unwrap_or(request.query);
        let numeric_hints = pg_numeric_catalog_hints_opt(&mut self.client, hint_query);

        // PgTxnGuard inside pg_run_export rolls the txn back automatically on
        // any error or panic, so no explicit ROLLBACK is needed here.
        let (total_rows, had_schema) = pg_run_export(
            &mut self.client,
            &built.sql,
            request.tuning,
            request.column_overrides,
            sink,
            numeric_hints.as_ref(),
        )?;

        if !had_schema {
            sink.on_schema(Arc::new(Schema::empty()))?;
        }

        log::info!("total: {} rows", total_rows);
        Ok(())
    }

    fn query_scalar(&mut self, sql: &str) -> Result<Option<String>> {
        let rows = self.client.query(sql, &[])?;
        if rows.is_empty() {
            return Ok(None);
        }
        let row = &rows[0];
        if let Ok(Some(v)) = row.try_get::<_, Option<i64>>(0) {
            return Ok(Some(v.to_string()));
        }
        if let Ok(Some(v)) = row.try_get::<_, Option<i32>>(0) {
            return Ok(Some(v.to_string()));
        }
        if let Ok(Some(v)) = row.try_get::<_, Option<f64>>(0) {
            return Ok(Some(v.to_string()));
        }
        // TIMESTAMP / DATE / TIMESTAMPTZ — required for MIN/MAX on time columns (e.g. chunk_by_days)
        if let Ok(Some(v)) = row.try_get::<_, Option<chrono::NaiveDateTime>>(0) {
            return Ok(Some(v.format("%Y-%m-%d %H:%M:%S").to_string()));
        }
        if let Ok(Some(v)) = row.try_get::<_, Option<chrono::NaiveDate>>(0) {
            return Ok(Some(v.format("%Y-%m-%d").to_string()));
        }
        if let Ok(Some(v)) = row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(0) {
            return Ok(Some(v.format("%Y-%m-%d %H:%M:%S").to_string()));
        }
        if let Ok(Some(v)) = row.try_get::<_, Option<String>>(0) {
            return Ok(Some(v));
        }
        Ok(None)
    }

    fn type_mappings(
        &mut self,
        query: &str,
        column_overrides: &ColumnOverrides,
    ) -> Result<Vec<TypeMapping>> {
        let wrapped = format!("SELECT * FROM ({}) AS _rivet_type_probe LIMIT 0", query);
        let stmt = self.client.prepare(&wrapped)?;
        let hints = pg_numeric_catalog_hints_opt(&mut self.client, query);
        let mappings = stmt
            .columns()
            .iter()
            .map(|col| {
                let rivet = rivet_type_for_pg_column(col, column_overrides, hints.as_ref());
                let source = SourceColumn::simple(col.name(), col.type_().name(), true);
                TypeMapping::from_source(&source, rivet)
            })
            .collect();
        Ok(mappings)
    }

    /// Governor pressure proxy: `pg_stat_bgwriter.checkpoints_req` — the same
    /// monotonic counter the adaptive batch loop samples. Rising between samples
    /// means the source is checkpointing harder under write pressure.
    fn sample_pressure(&mut self) -> Option<u64> {
        pg_sample_checkpoints_req(&mut self.client).map(|v| v.max(0) as u64)
    }
}

/// When the query is a single-table `SELECT … FROM rel` (no joins, no subquery
/// in `FROM`), PostgreSQL result metadata does not carry `NUMERIC` typmod, but
/// `information_schema` / the table DDL does. We resolve the base relation with
/// a small parser and fetch declared precision/scale so `rivet init`-style
/// exports work without hand-written `columns:` overrides.
fn pg_numeric_catalog_hints_opt(
    client: &mut Client,
    query: &str,
) -> Option<HashMap<String, (u8, i8)>> {
    match pg_fetch_numeric_catalog_hints(client, query) {
        Ok(m) => m,
        Err(e) => {
            // Reaching this arm means the parser identified a single-table query
            // and we tried catalog lookup, but the lookup itself failed. That is
            // unexpected (not "this query has a JOIN"), so surface it — otherwise
            // a downstream NUMERIC mapping failure looks like a config problem
            // when the real cause is here.
            log::warn!(
                "PG numeric catalog lookup failed — NUMERIC columns will require explicit `columns:` overrides: {e}"
            );
            None
        }
    }
}

fn pg_fetch_numeric_catalog_hints(
    client: &mut Client,
    query: &str,
) -> crate::error::Result<Option<HashMap<String, (u8, i8)>>> {
    let Some(regclass_lit) = try_parse_pg_simple_from_regclass_literal(query) else {
        return Ok(None);
    };
    let locate_sql = "SELECT n.nspname::text, c.relname::text \
         FROM pg_catalog.pg_class c \
         JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.oid = ($1::text)::regclass";
    let row_opt = match client.query_opt(locate_sql, &[&regclass_lit]) {
        Ok(r) => r,
        Err(e) => {
            log::warn!("PG numeric catalog: '{regclass_lit}' regclass lookup failed: {e}");
            return Ok(None);
        }
    };
    let Some(row) = row_opt else {
        return Ok(None);
    };
    let schema: String = row.get(0);
    let table: String = row.get(1);
    let rows = client.query(
        "SELECT column_name::text, data_type::text, numeric_precision, numeric_scale \
             FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2 \
             ORDER BY ordinal_position",
        &[&schema, &table],
    )?;

    let mut map = HashMap::new();
    for row in rows {
        let col: String = row.get(0);
        let dt: String = row.get(1);
        if !is_pg_numeric_information_type(&dt) {
            continue;
        }
        let p: Option<i32> = row.get(2);
        let s: Option<i32> = row.get(3);
        if let (Some(p), Some(s)) = (p, s)
            && let Some(pair) = catalog_numeric_to_decimal_params(p, s)
        {
            map.insert(col, pair);
        }
    }

    if map.is_empty() {
        Ok(None)
    } else {
        log::debug!(
            "PG numeric catalog: resolved {} DECIMAL/NUMERIC column(s) for relation {regclass_lit}",
            map.len(),
        );
        Ok(Some(map))
    }
}

fn is_pg_numeric_information_type(dt: &str) -> bool {
    let d = dt.trim().to_ascii_lowercase();
    matches!(d.as_str(), "numeric" | "decimal")
        || d.starts_with("numeric(")
        || d.starts_with("decimal(")
}

/// Match Rivet YAML `decimal(p,s)` / Arrow limits (same bound as overrides).
fn catalog_numeric_to_decimal_params(precision: i32, scale: i32) -> Option<(u8, i8)> {
    if precision <= 0 || precision > 76 {
        return None;
    }
    let precision_u = precision as u8;
    if scale < i32::from(i8::MIN) || scale > i32::from(i8::MAX) {
        return None;
    }
    let scale_i = scale as i8;
    if scale_i > precision as i8 {
        return None;
    }
    Some((precision_u, scale_i))
}

#[cfg(test)]
mod tests {
    use super::catalog_numeric_to_decimal_params;

    // FROM-clause parser tests live in `from_parse.rs` alongside the parser.

    #[test]
    fn catalog_decimal_bounds() {
        assert_eq!(catalog_numeric_to_decimal_params(18, 2), Some((18, 2)));
        assert!(catalog_numeric_to_decimal_params(0, 2).is_none());
        assert!(catalog_numeric_to_decimal_params(77, 0).is_none());
        assert!(catalog_numeric_to_decimal_params(18, 19).is_none());
        // W4 boundary rows — the mutants lived exactly ON the bounds:
        // precision 76 is the LAST valid (a `> 76` -> `>= 76` mutant rejects it);
        assert_eq!(catalog_numeric_to_decimal_params(76, 0), Some((76, 0)));
        // scale == precision is valid (`>` not `>=` on the third check);
        assert_eq!(catalog_numeric_to_decimal_params(18, 18), Some((18, 18)));
        // scale == i8::MIN is the last valid negative;
        assert_eq!(
            catalog_numeric_to_decimal_params(10, -128),
            Some((10, -128))
        );
        assert!(catalog_numeric_to_decimal_params(10, -129).is_none());
        // an `|| -> &&` mutant on the range check lets scale=200 reach the
        // `as i8` cast, wrapping to -56 and CORRUPTING the params silently.
        assert!(catalog_numeric_to_decimal_params(76, 200).is_none());
    }

    #[test]
    fn parse_work_mem_handles_pg_units() {
        use super::parse_work_mem;
        // Postgres SHOW work_mem normally returns "<N>kB", "<N>MB", "<N>GB".
        // A bare integer is interpreted as kB (matches postgresql.conf parsing).
        assert_eq!(parse_work_mem("4MB"), Some(4 * 1024 * 1024));
        assert_eq!(parse_work_mem("16384kB"), Some(16384 * 1024));
        assert_eq!(parse_work_mem("1GB"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_work_mem("  4MB  "), Some(4 * 1024 * 1024));
        assert_eq!(parse_work_mem("4mb"), Some(4 * 1024 * 1024));
        assert_eq!(parse_work_mem("65536"), Some(65536 * 1024));
        assert_eq!(parse_work_mem(""), None);
        assert_eq!(parse_work_mem("garbage"), None);
        // We don't accept seconds / units PG would never emit for work_mem.
        assert_eq!(parse_work_mem("4s"), None);
        // W4: the TB arm and the zero guard had no rows — deleting the "tb"
        // arm and every `*` in its multiplier survived.
        assert_eq!(
            parse_work_mem("2TB"),
            Some(2 * 1024 * 1024 * 1024 * 1024),
            "terabytes are 1024^4"
        );
        // Fractional values exercise the float multiply exactly.
        assert_eq!(parse_work_mem("0.5MB"), Some(512 * 1024));
        // Zero is rejected (`> 0`, not `>= 0`) — callers fall back.
        assert_eq!(parse_work_mem("0"), None);
        assert_eq!(parse_work_mem("0MB"), None);
    }
}
