pub(crate) mod batch_controller;
pub(crate) mod cdc;
pub mod mongo;
pub mod mssql;
pub mod mysql;
#[cfg(feature = "oracle")]
pub mod oracle;
pub(crate) mod pg_numeric_wire;
pub mod postgres;
pub(crate) mod query;
pub(crate) mod value_checksum;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::config::SourceConfig;
use crate::error::Result;
use crate::plan::IncrementalCursorPlan;
use crate::tuning::SourceTuning;
use crate::types::{ColumnOverrides, CursorState, TypeMapping};

pub(crate) use crate::connect::{
    TlsRequiredError, UnreachableTarget, describe_connect_error, is_tls_handshake_failure,
    require_tls_or_loopback, require_url_has_host,
};
pub use crate::error::{StatementDurationTimeout, TlsHandshakeFailed};

/// Summary of a source table relevant to chunked-mode planning. Source-neutral
/// shape so plan-build can ask either Postgres or MySQL for the same answer.
///
/// Populated by each engine's [`Source::introspect_for_chunking`]. The helpers
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
pub struct TableIntrospection {
    /// Name of the single integer-family PK column, if present and safe to
    /// range-chunk. `None` when the table has no PK, has a composite PK, or
    /// the PK type is not an integer family (text, uuid, decimal, …).
    pub single_int_pk: Option<String>,
    /// Single-column, NOT NULL, **unique** index columns usable as a keyset
    /// (seek) pagination key — PK first, then other UNIQUE indexes (OPT-4).
    /// Index-backed and unique by construction, so `ORDER BY key LIMIT n` is a
    /// bounded index range scan (never a filesort) and `WHERE key > last` never
    /// skips a duplicate key. Restricted to types the keyset CURSOR can read
    /// (`extract_last_cursor_value`: integer / float / string / timestamp / date /
    /// uuid) — `decimal`/`numeric` keys are EXCLUDED here so the planner refuses
    /// them up front rather than failing mid-run after a partial write (#dogfood).
    /// Empty when the table has no such key.
    pub keyset_keys: Vec<String>,
    /// Best-effort row count: PG `reltuples`, MySQL `TABLE_ROWS`. `0` means
    /// the table is empty or stats are unavailable.
    pub row_estimate: i64,
    /// Heap-size-per-row in bytes. `None` for empty / unanalysed tables.
    /// Used to convert `chunk_size_memory_mb` into a row count.
    pub avg_row_bytes: Option<i64>,
    /// Names of the table's integer-family columns (PG `int2`/`int4`/`int8`,
    /// MySQL `tinyint`…`bigint`, MSSQL `tinyint`/`smallint`/`int`/`bigint`). An
    /// explicit `chunk_column:` that is range-`BETWEEN`-sliced MUST be one of
    /// these: chunking derives integer min/max boundaries, so a non-integer key
    /// (numeric/decimal/real/float/…) silently DROPS every value that falls
    /// between two integer window boundaries. Empty when the engine does not
    /// populate it (e.g. Mongo, which does not SQL-range-chunk).
    pub int_columns: Vec<String>,
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

    /// Whether `col` is a known integer-family column — the safety precondition
    /// for range chunking (`chunk_column`), which slices via integer `BETWEEN`
    /// windows. A non-integer explicit `chunk_column` silently loses fractional
    /// rows, so the planner refuses it (see `chunked_strategy_from_introspection`).
    pub fn is_integer_column(&self, col: &str) -> bool {
        // Case-INSENSITIVE: the config may write `chunk_column: ID` while the
        // catalog stores `id` (MySQL is case-insensitive for column names; PG
        // folds unquoted idents to lowercase). A case-sensitive match falsely
        // refused a valid integer key with a "not an integer-family column" error
        // (bughunt MED). A guard should not reject on casing.
        //
        // ponytail: #8 narrow, documented non-fix. On PostgreSQL a table could in
        // principle hold BOTH `id` (int) and a quoted `"ID"` (numeric); the config
        // `chunk_column: ID` would then pass this guard (matching `id`) yet the
        // export SQL quotes `"ID"` case-sensitively and range-chunks the NUMERIC
        // one — the #103 loss. A precise guard needs the FULL column list + the
        // engine's quoting rule, not just the integer names, so it is not fixed
        // here. It is vanishingly rare (MySQL cannot hold both spellings; PG needs
        // deliberately quoted mixed-case twins), and WITHOUT the twin a case
        // mismatch fails loudly at query time ("column ... does not exist"), never
        // silently. The case-insensitive match's real, common benefit (MySQL)
        // outweighs guarding this exotic PG shape.
        self.int_columns.iter().any(|c| c.eq_ignore_ascii_case(col))
    }
}

/// Receives schema and batches from a source, one at a time.
pub trait BatchSink {
    fn on_schema(&mut self, schema: SchemaRef) -> Result<()>;
    fn on_batch(&mut self, batch: &RecordBatch) -> Result<()>;
    /// A source whose key type is richer than its output column can express
    /// reports its own keyset high-water mark here, as a lossless,
    /// engine-decodable token. The keyset/parallel runners prefer it over the
    /// string extracted from the output column — this is how MongoDB pages by a
    /// non-ObjectId BSON `_id` (int, string, …) whose hex/text rendering in the
    /// `_id` column would be type-ambiguous on the round-trip. No-op default:
    /// SQL engines carry their cursor losslessly in the column already.
    fn set_source_cursor(&mut self, _token: String) {}
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
    /// driver parses the `FROM` clause and asks `pg_catalog`). Chunked and
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
    /// The bare source relation this export reads, when it is a `table:`
    /// shortcut (`SELECT * FROM <ident>`) — the structured read-intent behind the
    /// SQL string. Computed once via [`crate::sql::strip_select_star_from`], so a
    /// non-SQL adapter (MongoDB reads a collection) uses it directly instead of
    /// re-parsing `query`. `None` for a hand-written `query:` / any wrapped or
    /// filtered form. SQL engines ignore it (they run `query`). See ADR-0027.
    pub base_relation: Option<&'a str>,
    /// INCLUSIVE upper bound on the keyset key for a parallel keyset worker's
    /// range: the page becomes `WHERE key > cursor AND key <= upper` (OPT
    /// parallel-keyset). Inlined, so it never consumes the cursor bind slot.
    /// `None` (the default) = the sequential single-worker page, unbounded above.
    pub upper_bound: Option<&'a str>,
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
            // `query` is the unwrapped base here, so the relation (if this is a
            // `table:` shortcut) is visible directly in it.
            base_relation: crate::sql::strip_select_star_from(query),
            upper_bound: None,
        }
    }

    /// A request whose `query` is a `SELECT … FROM (<base>) …` **wrapper** that
    /// hides the source table (chunked / time-window). `base` — the
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
            // `query` is a table-hiding wrapper; the relation lives in `base`.
            base_relation: crate::sql::strip_select_star_from(base),
            upper_bound: None,
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

    /// Set the INCLUSIVE upper bound on the keyset key — a parallel keyset
    /// worker's `(cursor, upper]` range. `None` leaves the page unbounded above
    /// (the sequential single-worker page).
    pub fn with_upper_bound(mut self, upper: Option<&'a str>) -> Self {
        self.upper_bound = upper;
        self
    }
}

/// The harm-counter key PostgreSQL reports `pg_stat_database.temp_bytes` under.
pub(crate) const PG_TEMP_BYTES_KEY: &str = "pg_temp_bytes";

pub trait Source: Send {
    /// Execute `request.query` and stream batches into `sink`.
    fn export(&mut self, request: &ExportRequest<'_>, sink: &mut dyn BatchSink) -> Result<()>;

    fn query_scalar(&mut self, sql: &str) -> Result<Option<String>>;

    /// The chunk planner's catalog probe of `qualified_table`, on this connection.
    fn introspect_for_chunking(&mut self, _qualified_table: &str) -> Result<TableIntrospection> {
        crate::rivet_bail!(
            crate::error::codes::CONFIG_SOURCE_MODE_UNSUPPORTED,
            "chunked mode is not supported for this source"
        )
    }

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

    /// Sample the monotonic FOREIGN-pressure counter for the OPT-2
    /// concurrency governor — write/redo pressure a read-only export cannot
    /// move (PG `checkpoints_req`, MySQL `Innodb_log_waits`, MSSQL `Log
    /// Flush Waits/sec`).
    ///
    /// This is the governor's ONLY signal, and it is deliberately NOT the
    /// adaptive batch loop's. The batch loops sample engine-internally:
    /// MySQL its own-extraction spill sum (`mysql_sample_extraction_pressure`
    /// — shrinking the batch genuinely shrinks the per-query spill), PG the
    /// same `checkpoints_req` both loops share (write-driven, own reads
    /// can't move it), MSSQL nothing (its batch adaptation is inert).
    /// Feeding the governor
    /// those same spill counters made it read its own exhaust (field find,
    /// 2026-08-13): a keyset export whose pages spill by design saw a
    /// permanently-rising counter on an idle server, shed workers 4→3→2→1,
    /// and never recovered — every keyset export 2–2.7× slower with zero
    /// foreign load. The governor's question is "is someone ELSE straining
    /// this server while I run?" — only a counter the export itself cannot
    /// inflate can answer it.
    ///
    /// Higher = more pressure; the governor compares successive samples
    /// (`cur > prev` ⇒ under pressure). Returns `None` when the engine has
    /// no such counter — the governor then holds parallelism flat.
    /// Default: `None`.
    fn sample_governor_pressure(&mut self) -> Option<u64> {
        None
    }

    /// The Tier-2 source-harm counter snapshot (locks, rows read, buffer
    /// misses, temp spills) the pipeline deltas around a run window and
    /// stores in `export_harm` — the third telemetry axis beside
    /// [`Source::sample_governor_pressure`] (governor) and each engine's
    /// internal batch-pressure sampling. `None` when the engine can't sample
    /// (e.g. MSSQL without `VIEW SERVER STATE`) — harm metrics are
    /// observability, never a gate. Default: `None`.
    fn harm_counters(&mut self) -> Option<Vec<(String, i64)>> {
        None
    }

    /// A best-effort JSON snapshot of the source SERVER's forensic context —
    /// version + the limits/session settings that shape failures (the
    /// statement-timeout that surfaces as `ERROR 3024`, the sql_mode/timezone that
    /// shape text rendering). Captured ONCE at run open onto the failed
    /// `export_metrics` row (`server_context_json`), so a post-mortem can explain a
    /// failure without re-querying a possibly-transient server. `None` when the
    /// engine can't cheaply gather it; never fails the run.
    fn server_context(&mut self) -> Option<String> {
        None
    }

    /// The primary key columns of `table` in key order; `None` when it has none
    /// or the engine cannot tell.
    fn primary_key(&mut self, _table: &str) -> Result<Option<Vec<String>>> {
        Ok(None)
    }

    /// `(column, full native type)` for `table` where the wire metadata lacks widths and
    /// labels (MySQL `COLUMN_TYPE`: `bit(8)`, `enum('a','b')`); empty where there is nothing to add.
    fn native_column_types(&mut self, _table: &str) -> Result<Vec<(String, String)>> {
        Ok(Vec::new())
    }
}

/// Split a catalog's unit-separator-joined key list; an empty list is no key.
pub(crate) fn split_key_list(joined: Option<String>) -> Option<Vec<String>> {
    non_empty_keys(
        joined?
            .split('\u{1f}')
            .filter(|c| !c.is_empty())
            .map(str::to_string)
            .collect(),
    )
}

/// The refusal for `source.type: oracle` in a build without the `oracle` feature.
#[cfg_attr(feature = "oracle", allow(dead_code))]
pub(crate) fn oracle_feature_missing() -> anyhow::Error {
    anyhow::anyhow!("source.type: oracle — this rivet was built without the `oracle` feature")
}

/// A key column list, or `None` when the table has no key.
pub(crate) fn non_empty_keys(cols: Vec<String>) -> Option<Vec<String>> {
    (!cols.is_empty()).then_some(cols)
}

/// The production bridge — LIVE-ONLY BY CONSTRUCTION, and deliberately not
/// unit-tested.
///
/// Building the `Box<dyn Source>` it is implemented for needs a real database
/// handle, so no offline test can call this `sample`; a unit test could only be
/// written against a fake `Source`, which would grade the fake rather than the
/// one line that matters (WHICH counter the governor listens to). Its oracle is
/// the three live shed tests, one per engine with a foreign-pressure counter —
/// `governor_backs_off_under_concurrent_write_pressure` (PostgreSQL),
/// `mysql_governor_backs_off_under_real_redo_pressure`,
/// `mssql_governor_backs_off_under_real_log_flush_pressure`
/// (`tests/live/live_governor.rs`). Each drives real foreign write pressure and
/// asserts the run logs a `backed off`, so all three go RED against a stubbed
/// bridge: `-> None` never sheds (an unreadable signal fails OPEN), and a
/// constant `Some(0)`/`Some(1)` never RISES, which is the only thing
/// `GovernorState::observe` reads as pressure. Verified by hand against the
/// `Some(0)` mutant, 2026-08-14.
///
/// The counterpart guard — that this must NOT be wired to the batch loop's own
/// extraction counters — is `mysql_governor_ignores_the_exports_own_spill_exhaust`.
impl crate::tuning::PressureSource for Box<dyn Source> {
    fn sample(&mut self) -> Option<u64> {
        // The governor's signal is the FOREIGN-pressure counter, not the batch
        // loop's own-extraction counter: a keyset export's own pages inflate
        // the spill counters by design, and a governor listening to them sheds
        // its own workers to the floor and never recovers (field find,
        // 2026-08-13 — see `Source::sample_governor_pressure`).
        Source::sample_governor_pressure(self.as_mut())
    }
}

pub fn create_source(config: &SourceConfig) -> Result<Box<dyn Source>> {
    let url = config.resolve_url()?;
    warn_if_tls_disabled(config);
    connect(
        config.source_type,
        &url,
        config.tls.as_ref(),
        config.mongo.as_ref(),
    )
}

/// Connect to a `source_type` source at a resolved `url` — the one engine switch every connection goes through.
pub fn connect(
    source_type: crate::config::SourceType,
    url: &str,
    tls: Option<&crate::config::TlsConfig>,
    mongo: Option<&crate::config::MongoConfig>,
) -> Result<Box<dyn Source>> {
    use crate::config::SourceType;
    Ok(match source_type {
        SourceType::Postgres => Box::new(postgres::PostgresSource::connect_with_tls(url, tls)?),
        SourceType::Mysql => Box::new(mysql::MysqlSource::connect_with_tls(url, tls)?),
        SourceType::Mssql => Box::new(mssql::MssqlSource::connect_with_tls(url, tls)?),
        #[cfg(feature = "oracle")]
        SourceType::Oracle => Box::new(oracle::OracleSource::connect_with_tls(url, tls)?),
        #[cfg(not(feature = "oracle"))]
        SourceType::Oracle => return Err(crate::source::oracle_feature_missing()),
        SourceType::Mongo => Box::new(mongo::MongoSource::connect(url, tls, mongo)?),
    })
}

/// Pre-allocation per-value size guard, shared by every engine's
/// `arrow_convert`. The sink-side `check_value_ceiling`
/// (`pipeline::sink::mod`) scans the *already-built* Arrow batch, so an
/// oversized cell costs the driver-decode copy **and** the Arrow-build copy
/// before that guard fires. This check runs at the decode/`Value` stage — after
/// the unavoidable driver copy, but *before* the value is appended into the
/// `StringBuilder` / `BinaryBuilder` — so the Arrow allocation never grows to
/// hold it. Only variable-length values (Utf8 / Binary) can be individually
/// huge; fixed-width arms (ints/floats/dates) never call this.
///
/// `max_value_bytes` is `tuning.max_value_bytes()` (MB → bytes with the
/// `Some(0)`/`None` ⇒ disabled semantics). The message mirrors the sink guard's
/// `RIVET_VALUE_TOO_LARGE` so both read identically; the sink guard stays as the
/// backstop (it also covers meta / enriched columns and is the contract test).
pub(crate) fn value_within_ceiling(
    column: &str,
    len: usize,
    max_value_bytes: Option<usize>,
) -> Result<()> {
    if let Some(limit) = max_value_bytes
        && len > limit
    {
        anyhow::bail!(
            "RIVET_VALUE_TOO_LARGE: column '{}' has a single value of {:.1} MB, exceeding the \
             per-value ceiling of {} MB. One oversized cell can OOM the process regardless of \
             batch size. Raise `tuning.max_value_mb` (or set it to 0 to disable the guard) if \
             this value is expected.",
            column,
            len as f64 / (1024.0 * 1024.0),
            limit / (1024 * 1024),
        );
    }
    Ok(())
}

#[cfg(test)]
mod key_list_tests {
    use super::split_key_list;

    #[test]
    fn a_joined_key_splits_in_key_order_and_an_empty_one_is_no_key() {
        assert_eq!(
            split_key_list(Some("tenant\u{1f}id".into())),
            Some(vec!["tenant".to_string(), "id".to_string()])
        );
        assert_eq!(
            split_key_list(Some("a,b".into())),
            Some(vec!["a,b".to_string()])
        );
        assert_eq!(split_key_list(Some(String::new())), None);
        assert_eq!(split_key_list(None), None);
    }
}

#[cfg(test)]
mod value_ceiling_tests {
    use super::value_within_ceiling;

    #[test]
    fn sec_value_ceiling_pre_alloc_over_limit_errors() {
        let err = value_within_ceiling("payload", 2 * 1024 * 1024, Some(1024 * 1024)).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("RIVET_VALUE_TOO_LARGE"), "got: {msg}");
        assert!(msg.contains("payload"), "names the column: {msg}");
    }

    #[test]
    fn sec_value_ceiling_pre_alloc_at_or_under_limit_ok() {
        assert!(value_within_ceiling("c", 1024 * 1024, Some(1024 * 1024)).is_ok());
        assert!(value_within_ceiling("c", 0, Some(1024 * 1024)).is_ok());
    }

    #[test]
    fn sec_value_ceiling_pre_alloc_disabled_never_errors() {
        // `None` (set when tuning.max_value_mb is 0 or unset) disables the guard.
        assert!(value_within_ceiling("c", usize::MAX, None).is_ok());
    }
}

/// One-time nudge to enable TLS when the current config connects in plaintext.
/// Emitted at `warn` level so operators see it even at the default log level.
/// `create_source` is called multiple times per run (plan/preflight/exec/chunk
/// workers), so we gate the warning behind a `Once` to fire exactly once per
/// process rather than 3-4 times in stderr.
pub(crate) fn warn_if_tls_disabled(config: &SourceConfig) {
    let enforced = config.tls.as_ref().is_some_and(|t| t.mode.is_enforced());
    if enforced {
        return;
    }
    // Loopback (localhost / 127.0.0.0/8 / ::1) is the local-dev / docker case:
    // the bytes never leave the box, so the plaintext warning is just noise on
    // a newcomer's laptop. Resolve best-effort — if the URL can't be resolved we
    // fall through and warn (fail-safe). The real CWE-319 signal still fires for
    // any remote host.
    if config.resolve_url().is_ok_and(|u| host_is_loopback(&u)) {
        return;
    }
    static WARNED: std::sync::Once = std::sync::Once::new();
    WARNED.call_once(|| {
        log::warn!(
            "source: TLS is not enforced — credentials and result rows cross the network in plaintext. \
             Add `source.tls.mode: verify-full` (with `ca_file:` if your CA is private — not yet supported for Oracle) to enable transport security."
        );
    });
}

pub(crate) use crate::config::url::{host_is_loopback, url_tls};

/// Batch positional-mapping guard: every engine's batch decoder indexes wire
/// rows by the RESOLVE-time column order (`SELECT *` is positional at the
/// protocol level), so a DDL slipping between chunk reads (parallel-worker
/// idle gaps, a chunk retry on a fresh connection) would misalign values
/// silently. The wire carries column NAMES on all three engines — verify them
/// against the resolved mapping before decoding each batch and fail loudly
/// instead. (The sequential paths are already server-serialized: PG holds
/// ACCESS SHARE across the export transaction, MySQL/InnoDB reads through a
/// snapshot with instant-DDL row versioning — both measured live; this guard
/// closes the residual windows.)
pub(crate) fn verify_wire_columns(expected: &[&str], wire: &[&str]) -> anyhow::Result<()> {
    if expected.len() != wire.len()
        || expected
            .iter()
            .zip(wire)
            .any(|(e, w)| !e.eq_ignore_ascii_case(w))
    {
        anyhow::bail!(
            "the source returned columns [{}] but this export resolved [{}] — the table's \
             schema changed while the export was running (a DDL mid-export). Re-run the \
             export: a fresh run resolves the new schema.",
            wire.join(", "),
            expected.join(", "),
        );
    }
    Ok(())
}

#[cfg(test)]
mod wire_guard_tests {
    use super::verify_wire_columns;

    #[test]
    fn verify_wire_columns_catches_every_drift_shape() {
        let ok = verify_wire_columns(&["id", "a", "b"], &["id", "a", "b"]);
        assert!(ok.is_ok());
        // case-insensitive (MySQL lowercases, MSSQL preserves)
        assert!(verify_wire_columns(&["id", "A"], &["ID", "a"]).is_ok());
        // dropped column
        assert!(verify_wire_columns(&["id", "a", "b"], &["id", "b"]).is_err());
        // added column
        assert!(verify_wire_columns(&["id", "b"], &["id", "b", "c"]).is_err());
        // same-arity rename/reorder — the shape positional decoding CANNOT see
        assert!(verify_wire_columns(&["id", "a", "b"], &["id", "b", "a"]).is_err());
        let err = verify_wire_columns(&["id", "a"], &["id"]).unwrap_err();
        assert!(err.to_string().contains("schema changed"));
    }
}

#[cfg(test)]
mod introspection_tests {
    use super::TableIntrospection;

    #[test]
    fn is_integer_column_is_case_insensitive() {
        // #bughunt MED: a case-sensitive match falsely refused `chunk_column: ID`
        // when the catalog stores `id` — a guard must not reject on casing.
        let intro = TableIntrospection {
            int_columns: vec!["id".into(), "user_id".into()],
            ..Default::default()
        };
        assert!(intro.is_integer_column("id"));
        assert!(intro.is_integer_column("ID"));
        assert!(intro.is_integer_column("User_Id"));
        assert!(!intro.is_integer_column("name"));
    }
}
