pub mod mysql;
pub(crate) mod pg_numeric_wire;
pub mod postgres;
pub(crate) mod query;
pub(crate) mod tls;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::config::SourceConfig;
use crate::error::Result;
use crate::plan::IncrementalCursorPlan;
use crate::tuning::SourceTuning;
use crate::types::{ColumnOverrides, CursorState, TypeMapping};

/// Summary of a source table relevant to chunked-mode planning. Source-neutral
/// shape so plan-build can ask either Postgres or MySQL for the same answer.
///
/// Populated by `crate::source::postgres::introspect_pg_table_for_chunking` and
/// `crate::source::mysql::introspect_mysql_table_for_chunking`. Both helpers
/// rely on catalog stats (`pg_class` / `information_schema.TABLES`) so the
/// numbers are only as fresh as the last `ANALYZE` / autoanalyse.
#[derive(Debug, Clone, Default)]
pub(crate) struct TableIntrospection {
    /// Name of the single integer-family PK column, if present and safe to
    /// range-chunk. `None` when the table has no PK, has a composite PK, or
    /// the PK type is not an integer family (text, uuid, decimal, …).
    pub single_int_pk: Option<String>,
    /// Single-column, NOT NULL, **unique** index columns usable as a keyset
    /// (seek) pagination key — PK first (any type), then other UNIQUE indexes
    /// (OPT-4). Index-backed and unique by construction, so `ORDER BY key
    /// LIMIT n` is a bounded index range scan (never a filesort) and
    /// `WHERE key > last` never skips rows with a duplicate key. Empty when the
    /// table has no such key.
    pub keyset_keys: Vec<String>,
    /// Best-effort row count: PG `reltuples`, MySQL `TABLE_ROWS`. `0` means
    /// the table is empty or stats are unavailable.
    pub row_estimate: i64,
    /// Heap-size-per-row in bytes. `None` for empty / unanalysed tables.
    /// Used to convert `chunk_size_memory_mb` into a row count.
    pub avg_row_bytes: Option<i64>,
}

impl TableIntrospection {
    /// The auto-selected keyset key: the first usable single-column unique
    /// NOT NULL key (PK preferred). `None` when the table has none.
    pub fn auto_keyset_key(&self) -> Option<&str> {
        self.keyset_keys.first().map(String::as_str)
    }

    /// Whether `col` is a usable keyset key (single-column, unique, NOT NULL,
    /// index-backed). Used to validate an explicit `chunk_by_key`.
    pub fn is_usable_keyset_key(&self, col: &str) -> bool {
        self.keyset_keys.iter().any(|k| k == col)
    }
}

/// Receives schema and batches from a source, one at a time.
pub trait BatchSink {
    fn on_schema(&mut self, schema: SchemaRef) -> Result<()>;
    fn on_batch(&mut self, batch: &RecordBatch) -> Result<()>;
}

/// Read-only inputs for a single export call.
///
/// Packs the parameters that used to live as 5 positional args on
/// `Source::export` into a named struct. `sink` is **not** part of this struct
/// — it is `&mut` and conceptually the output channel, separate from the
/// read-only request configuration.
pub struct ExportRequest<'a> {
    /// Already-materialized SQL (after `resolve_query`). The driver still wraps
    /// it with the dialect-specific incremental predicate via
    /// [`crate::source::query::build_incremental_query`] when `incremental` is set.
    pub query: &'a str,
    pub incremental: Option<&'a IncrementalCursorPlan>,
    pub cursor: Option<&'a CursorState>,
    pub tuning: &'a SourceTuning,
    /// Per-column type declarations from `rivet.yaml` (`exports[].columns:`).
    /// Drivers apply them during schema building so e.g. a `NUMERIC` column
    /// without declared precision can still be exported as `Decimal128(18,2)`
    /// when the user has stated the type explicitly.
    pub column_overrides: &'a ColumnOverrides,
    /// Keyset (seek) pagination page size (OPT-4). When `Some(n)` *and*
    /// `incremental` carries the key plan, the driver builds one keyset page
    /// (`WHERE key > cursor ORDER BY key LIMIT n`) instead of the unbounded
    /// incremental/snapshot query. The keyset runner drives the outer loop.
    pub page_limit: Option<usize>,
}

pub trait Source: Send {
    /// Execute `request.query` and stream batches into `sink`.
    fn export(&mut self, request: &ExportRequest<'_>, sink: &mut dyn BatchSink) -> Result<()>;

    fn query_scalar(&mut self, sql: &str) -> Result<Option<String>>;

    /// Return `TypeMapping` for every column in `query` without fetching rows.
    ///
    /// Used by `rivet check --type-report` to show the full type provenance
    /// (source native type → RivetType → Arrow type → fidelity) before export.
    /// Implementations execute `SELECT * FROM (...) AS _q LIMIT 0` so only
    /// server-side type metadata is transferred.
    fn type_mappings(
        &mut self,
        query: &str,
        column_overrides: &ColumnOverrides,
    ) -> Result<Vec<TypeMapping>>;

    /// Sample a monotonic source-pressure counter for the OPT-2 concurrency
    /// governor (`pipeline::chunked::exec`).
    ///
    /// Higher = more pressure. The governor compares successive samples
    /// (`cur > prev` ⇒ under pressure) — the same convention the adaptive
    /// batch-size loop already uses. Returns `None` when the engine can't
    /// cheaply sample a pressure proxy, in which case the governor holds
    /// parallelism flat. Default: `None`.
    fn sample_pressure(&mut self) -> Option<u64> {
        None
    }
}

pub fn create_source(config: &SourceConfig) -> Result<Box<dyn Source>> {
    use crate::config::SourceType;
    let url = config.resolve_url()?;
    warn_if_tls_disabled(config);
    match config.source_type {
        SourceType::Postgres => Ok(Box::new(postgres::PostgresSource::connect_with_tls(
            &url,
            config.tls.as_ref(),
        )?)),
        SourceType::Mysql => Ok(Box::new(mysql::MysqlSource::connect_with_tls(
            &url,
            config.tls.as_ref(),
        )?)),
    }
}

/// One-time nudge to enable TLS when the current config connects in plaintext.
/// Emitted at `warn` level so operators see it even at the default log level.
/// `create_source` is called multiple times per run (plan/preflight/exec/chunk
/// workers), so we gate the warning behind a `Once` to fire exactly once per
/// process rather than 3-4 times in stderr.
pub(crate) fn warn_if_tls_disabled(config: &SourceConfig) {
    let enforced = config.tls.as_ref().is_some_and(|t| t.mode.is_enforced());
    if !enforced {
        static WARNED: std::sync::Once = std::sync::Once::new();
        WARNED.call_once(|| {
            log::warn!(
                "source: TLS is not enforced — credentials and result rows cross the network in plaintext. \
                 Add `source.tls.mode: verify-full` (with `ca_file:` if your CA is private) to enable transport security."
            );
        });
    }
}
