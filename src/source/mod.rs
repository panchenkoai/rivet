pub mod mssql;
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
///
/// # Why this is a data-shape seam, not a trait
///
/// The two per-engine introspection functions have identical signatures
/// (`fn(url, tls, qualified_table) -> Result<TableIntrospection>`) and return
/// this shared struct. The parallel shape sometimes invites a refactor along
/// the lines of `trait Introspector { fn introspect_table(...) }` with one
/// impl per engine — that refactor adds ceremony without reducing duplication,
/// because the *bodies* share nothing useful: PG queries `pg_class` /
/// `pg_index` / `pg_attribute` / `pg_type` (PG-specific type names like
/// `int2`/`int4`/`int8`) via the `postgres` client; MySQL queries
/// `information_schema.TABLES` / `STATISTICS` with the InnoDB
/// `AVG_ROW_LENGTH` overflow correction via the `mysql` client. No shared
/// implementation logic exists to extract into trait-default methods. A
/// trait would only rename where the engine match happens
/// (`match config.source.source_type { … }` at the call site → factory
/// returning `Box<dyn Introspector>`); the match doesn't disappear.
///
/// The seam therefore lives at the **data shape**: this struct is the
/// shared contract, the two free functions are the adapters, the per-call
/// dispatch is an `enum`-driven `match`. See ADR-0015 for the full
/// rationale and the architecture-review walks that led here.
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
    /// The *unwrapped* base query to resolve catalog-dependent type hints from
    /// (PostgreSQL `NUMERIC` precision/scale, which the wire protocol omits — the
    /// driver parses the `FROM` clause and asks `pg_catalog`). Chunked, dense and
    /// keyset runners wrap `query` in a `SELECT … FROM (<base>) …` subquery that
    /// hides the source table from the catalog parser, so they pass the original
    /// base query here. `None` ⇒ resolve from `query` (full/incremental, where it
    /// is already the unwrapped form). Drivers that read precision from the wire
    /// (MySQL) ignore this field.
    pub catalog_hint_query: Option<&'a str>,
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

impl<'a> ExportRequest<'a> {
    /// A request whose `query` is already the **unwrapped base** form, so
    /// catalog type hints resolve directly from it. Use for snapshot,
    /// incremental and keyset runners: the driver applies any incremental /
    /// keyset predicate internally, so the source table stays visible to the
    /// catalog parser and `catalog_hint_query` is `None`.
    pub fn unwrapped(
        query: &'a str,
        tuning: &'a SourceTuning,
        column_overrides: &'a ColumnOverrides,
    ) -> Self {
        Self {
            query,
            catalog_hint_query: None,
            incremental: None,
            cursor: None,
            tuning,
            column_overrides,
            page_limit: None,
        }
    }

    /// A request whose `query` is a `SELECT … FROM (<base>) …` **wrapper** that
    /// hides the source table (chunked / dense / time-window). `base` — the
    /// unwrapped query catalog hints resolve from — is a required argument, so a
    /// wrapping runner cannot silently fall back to the table-hiding wrapper and
    /// lose PG `NUMERIC` precision (the bug the catalog-hint fix / ADR-0020
    /// closed). Drivers that read precision from the wire (MySQL) ignore it.
    pub fn wrapped(
        query: &'a str,
        base: &'a str,
        tuning: &'a SourceTuning,
        column_overrides: &'a ColumnOverrides,
    ) -> Self {
        Self {
            query,
            catalog_hint_query: Some(base),
            incremental: None,
            cursor: None,
            tuning,
            column_overrides,
            page_limit: None,
        }
    }

    /// Attach the incremental cursor plan (the driver builds the `WHERE cursor >
    /// ? ORDER BY` predicate). Pass-through `Option` so mode-polymorphic callers
    /// can forward `strategy.incremental_plan()` directly.
    pub fn with_incremental(mut self, plan: Option<&'a IncrementalCursorPlan>) -> Self {
        self.incremental = plan;
        self
    }

    /// Attach the last committed cursor value the next run resumes after.
    pub fn with_cursor(mut self, cursor: Option<&'a CursorState>) -> Self {
        self.cursor = cursor;
        self
    }

    /// Set the keyset (seek) page size — one bounded `… WHERE key > cursor ORDER
    /// BY key LIMIT n` page instead of the unbounded query.
    pub fn with_page_limit(mut self, page_limit: usize) -> Self {
        self.page_limit = Some(page_limit);
        self
    }
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
        SourceType::Mssql => Ok(Box::new(mssql::MssqlSource::connect_with_tls(
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
