pub(crate) mod batch_controller;
pub(crate) mod cdc;
pub mod mongo;
pub mod mssql;
pub mod mysql;
pub(crate) mod pg_numeric_wire;
pub mod postgres;
pub(crate) mod query;
pub(crate) mod tls;
pub(crate) mod value_checksum;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::config::{SourceConfig, TlsConfig};
use crate::error::Result;
use crate::plan::IncrementalCursorPlan;
use crate::tuning::SourceTuning;
use crate::types::{ColumnOverrides, CursorState, TypeMapping};

/// A statement-DURATION timeout that **rivet itself** raised — distinct from a
/// driver-native timeout that carries a structured code (PG 57014, MySQL 3024).
///
/// The MSSQL engine has no server-side statement-duration `SET`, so rivet
/// enforces `tuning.statement_timeout_s` client-side and raises this when the
/// budget is exceeded (see [`mssql`]). Before this type the retry classifier's
/// permanence hinged on substring-matching rivet's OWN prose ("statement
/// timeout after …"); a reworded message would silently flip the error back to
/// *transient*, and the identical query would be retried until it burned the
/// budget N times (measured: 3×300 s = 20 min for 0 rows). Carrying a typed
/// marker means [`crate::pipeline::retry::classify_error`] downcasts the TYPE,
/// so permanence survives any change to the human-facing wording. The string
/// branches in the classifier remain a fallback for genuinely driver-native
/// timeout messages we do not control.
#[derive(Debug)]
pub struct StatementDurationTimeout {
    /// Full actionable message shown to the operator. The classifier keys off
    /// the TYPE, not this text — it exists only for Display.
    message: String,
}

impl StatementDurationTimeout {
    /// MSSQL client-side statement-duration timeout (no server-side `SET`).
    pub fn mssql(seconds: u64) -> Self {
        Self {
            message: format!(
                "mssql: statement timeout after {seconds}s (tuning.statement_timeout_s) — \
                 this query cannot finish within the budget; split it with `mode: chunked` \
                 (per-chunk statements stay under the limit) or raise \
                 `tuning.statement_timeout_s`"
            ),
        }
    }

    /// MySQL server-side `max_execution_time` timeout (ER_QUERY_TIMEOUT / 3024).
    /// Wraps the driver's terse "maximum statement execution time exceeded" with
    /// the actionable fix — including the WIDE-table case (a chunk that still
    /// times out), which the field's `*_version` tables hit and the raw driver
    /// error gave no guidance for.
    pub fn mysql(seconds: u64) -> Self {
        Self {
            message: format!(
                "mysql: statement timeout after {seconds}s (max_execution_time from \
                 tuning.statement_timeout_s) — this query exceeded its time budget (ERROR 3024). \
                 Split it with `mode: chunked` / `chunk_by_key` so per-chunk queries stay under \
                 the limit; if a CHUNK still times out on a WIDE table, lower `chunk_size` or use \
                 `chunk_size_memory_mb:` (width-aware chunking); or raise `tuning.statement_timeout_s`"
            ),
        }
    }
}

impl std::fmt::Display for StatementDurationTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for StatementDurationTimeout {}

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
        SourceType::Mongo => Ok(Box::new(mongo::MongoSource::connect(
            &url,
            config.tls.as_ref(),
            config.mongo.as_ref(),
        )?)),
    }
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
             Add `source.tls.mode: verify-full` (with `ca_file:` if your CA is private) to enable transport security."
        );
    });
}

/// Whether the host in a `scheme://[user[:pass]@]host[:port][/db][?…]`
/// connection URL is a loopback address (`127.0.0.0/8`, `::1`) or the literal
/// `localhost`.
///
/// Used by [`require_tls_or_loopback`] to decide TLS posture from the host:
/// loopback is the docker / local-dev case where the bytes never leave the box,
/// so plaintext is fine; a remote host without TLS leaks credentials and rows.
///
/// Fails **closed**: any URL we cannot confidently parse a loopback host out of
/// is treated as non-loopback, so a parse gap can only ever *tighten* the gate
/// (refuse a connection), never silently allow plaintext to an unverified host.
/// The `host[:port][,host:port…]` span of a URL — scheme stripped, path/query
/// dropped, `user[:pass]@` userinfo removed (rsplit the last `@` so an `@` in a
/// password stays with the userinfo). Empty when the URL carries no authority.
pub(crate) fn host_port_span(url: &str) -> &str {
    let after_scheme = match url.split_once("://") {
        Some((_, rest)) => rest,
        None => url,
    };
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(after_scheme);
    match authority.rsplit_once('@') {
        Some((_, hp)) => hp,
        None => authority,
    }
}

pub(crate) fn host_is_loopback(url: &str) -> bool {
    let host_port = host_port_span(url);
    // A comma seedlist (`host1:p1,host2:p2` — valid for MongoDB AND multi-host
    // PostgreSQL) is loopback ONLY if EVERY host is: reading just the first host
    // let `127.0.0.1:5432,evil.com:5432` dial evil.com in plaintext under the
    // gate (bug-hunt find). Empty authority ⇒ not loopback (fail closed).
    !host_port.is_empty() && host_port.split(',').all(one_host_is_loopback)
}

/// Loopback test for a single `host[:port]` (or bracketed `[ipv6][:port]`).
fn one_host_is_loopback(host_port: &str) -> bool {
    // IPv6 literals are bracketed (`[::1]:5432`); the host is the bracketed span,
    // and any `:` inside is part of the address.
    let host = if let Some(rest) = host_port.strip_prefix('[') {
        match rest.split_once(']') {
            Some((h, _)) => h,
            None => return false, // unterminated bracket — fail closed
        }
    } else {
        // Bare host or IPv4: the host ends at the (single) port `:`.
        host_port.split(':').next().unwrap_or(host_port)
    };

    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    // `IpAddr::is_loopback` covers the whole 127.0.0.0/8 block and `::1`.
    host.parse::<std::net::IpAddr>()
        .is_ok_and(|ip| ip.is_loopback())
}

/// Refuse a URL that carries no host authority (`mysql://`, `postgres:///db`)
/// with a clear parse error, BEFORE any engine-specific setup hint can blanket
/// it (dogfood LOW: `rivet cdc --source mysql://` reported a binlog-grants
/// problem for a host that doesn't exist). No URL echo — the userinfo may hold
/// credentials — and no `user:pass@` pattern in the message (the redactor
/// mangles it).
pub(crate) fn require_url_has_host(url: &str) -> Result<()> {
    if host_port_span(url).is_empty() {
        anyhow::bail!(
            "source: invalid URL — no host found. Expected a URL of the form \
             scheme://host:port/database."
        );
    }
    Ok(())
}

/// Gate plaintext / trust-any-cert connections by host (CWE-319 / CWE-295).
///
/// When no `tls:` block is configured (`tls == None`) **and** the resolved host
/// is not loopback, refuse the connection *before any network I/O* with a
/// TLS-required policy error. This stops the per-engine connect helpers from
/// silently dialing a remote database in cleartext (Postgres/MySQL `NoTls`) or
/// trusting any server certificate (MSSQL `trust_cert`).
///
/// Loopback hosts (docker / local dev) keep today's behaviour — plaintext is
/// allowed there because the bytes never leave the box. An explicit
/// `tls: { mode: disable }` is `Some(..)`, so it is the operator's opt-in to
/// remote plaintext and is **not** refused here.
/// Marker error for the TLS-required policy refusal, so callers whose remedy
/// differs (init: a FLAG, not a config block) can recognize it by type.
#[derive(Debug)]
pub(crate) struct TlsRequiredError;
impl std::fmt::Display for TlsRequiredError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TLS required for a remote host")
    }
}
impl std::error::Error for TlsRequiredError {}

pub(crate) fn require_tls_or_loopback(url: &str, tls: Option<&TlsConfig>) -> Result<()> {
    // An explicit `tls: {..}` (including `mode: disable`) is the operator's
    // opt-in and is never refused here — including for a host-LESS URL, which a
    // driver resolves as a LOCAL unix socket (`postgres:///db?host=/var/run/
    // postgresql`). The host-presence check must therefore live INSIDE the
    // no-tls branch: hoisting it above (da7abbf) rejected a valid socket URL that
    // worked on main whenever `tls: { mode: disable }` was set (#16 bughunt).
    if tls.is_none() {
        // A URL with NO host at all (`mysql://`, `postgres:///db`) is not a
        // "remote host" — it is malformed. Prescribing a TLS block there sends the
        // operator chasing a security setting for a host that doesn't exist.
        require_url_has_host(url)?;
    }
    if tls.is_none() && !host_is_loopback(url) {
        // The message must name TLS *and* that it is a policy refusal for a
        // remote host. Emit it at `error` level (→ stderr) as well as returning
        // it: callers like `doctor` print the `Err` to stdout in their own
        // `[FAIL]` style and only re-raise a generic summary, so the log line is
        // what guarantees the TLS-required reason reaches stderr. Deliberately
        // avoids socket-error vocabulary ("could not connect", "timeout", "os
        // error") so it is never mistaken for a connect-time failure.
        let msg = "source: TLS required — refusing to connect to a remote (non-loopback) \
             host without TLS; credentials and every exported row would cross the network \
             in cleartext. Add `source.tls: { mode: verify-full }` (with `ca_file:` for a \
             private CA) to enable transport security, or explicitly opt into remote \
             plaintext with `source.tls: { mode: disable }` if this network path is \
             already trusted.";
        log::error!("{msg}");
        // Typed, not just a string: `rivet init` has no config file to add a
        // `tls:` block TO (it generates one), so its dispatch matches on this
        // marker and re-prescribes the `--tls` flag instead — detection by
        // downcast, never by string-matching the message (#146).
        return Err(anyhow::Error::new(TlsRequiredError).context(msg));
    }
    Ok(())
}

#[cfg(test)]
mod tls_gate_tests {
    use super::{host_is_loopback, host_port_span, require_tls_or_loopback};
    use crate::config::{TlsConfig, TlsMode};

    /// The marker must be REACHABLE (downcast finds it on the chain) and must
    /// SAY something (a `Display` stubbed to nothing turns `{:#}` chains into
    /// a trailing colon and empty segment). Both halves lib-side, where the
    /// marker lives — init's flag-naming remedy is tested bin-side.
    #[test]
    fn tls_required_error_is_downcastable_and_self_describing() {
        let err = require_tls_or_loopback("mysql://u:p@203.0.113.9/db", None)
            .expect_err("remote + no tls refuses");
        assert!(
            err.chain()
                .any(|c| c.downcast_ref::<super::TlsRequiredError>().is_some()),
            "the refusal must carry the typed marker"
        );
        let display = format!("{}", super::TlsRequiredError);
        assert!(
            display.contains("TLS required"),
            "the marker's own text must name the policy: {display:?}"
        );
    }

    #[test]
    fn loopback_variants_are_loopback() {
        assert!(host_is_loopback(
            "postgresql://rivet:rivet@127.0.0.1:5432/rivet"
        ));
        assert!(host_is_loopback(
            "postgresql://rivet:rivet@localhost:5432/rivet"
        ));
        assert!(host_is_loopback("mysql://root@127.0.0.1:3306/db"));
        // Whole 127.0.0.0/8 block is loopback.
        assert!(host_is_loopback("postgresql://u:p@127.255.0.9/db"));
        // IPv6 loopback, bracketed with and without a port.
        assert!(host_is_loopback("postgresql://u:p@[::1]:5432/db"));
        assert!(host_is_loopback("sqlserver://sa:pw@[::1]/master"));
        // Case-insensitive host, no port, no db.
        assert!(host_is_loopback("mysql://root@LOCALHOST"));
        // An `@` inside the password must not be mistaken for the host boundary.
        assert!(host_is_loopback("postgresql://u:p@ss@127.0.0.1:5432/db"));
    }

    #[test]
    fn roast_seedlist_with_any_remote_host_is_not_loopback() {
        // Multi-host / seedlist authority (`host1:p1,host2:p2`): the TLS gate must
        // treat it as loopback ONLY if EVERY host is loopback. Reading just the
        // first host let `127.0.0.1:5432,evil.com:5432` (a valid PostgreSQL and
        // MongoDB seedlist) pass the gate and dial evil.com in plaintext
        // (bug-hunt find; the shared gate reaches every engine, PG supports
        // multi-host URLs).
        assert!(!host_is_loopback(
            "postgresql://u:p@127.0.0.1:5432,evil.com:5432/db"
        ));
        assert!(!host_is_loopback(
            "mongodb://u:p@127.0.0.1:27017,evil.com:27017/db"
        ));
        // All-loopback seedlist stays loopback.
        assert!(host_is_loopback(
            "mongodb://u:p@127.0.0.1:27017,[::1]:27018/db"
        ));
    }

    #[test]
    fn remote_hosts_are_not_loopback() {
        assert!(!host_is_loopback(
            "postgresql://rivet:rivet@10.255.255.1:5432/rivet"
        ));
        assert!(!host_is_loopback(
            "postgresql://u:p@db.example.com:5432/app"
        ));
        assert!(!host_is_loopback("mysql://root@192.168.1.10:3306/db"));
        assert!(!host_is_loopback("sqlserver://sa:pw@10.0.0.5:1433/master"));
        // Not loopback: an unbracketed IPv6-looking address won't parse here, so
        // it fails closed (treated as remote).
        assert!(!host_is_loopback("postgresql://u:p@::1:5432/db"));
    }

    #[test]
    fn gate_refuses_remote_plaintext_only() {
        let remote = "postgresql://rivet:rivet@10.255.255.1:5432/rivet";
        let loopback = "postgresql://rivet:rivet@127.0.0.1:5432/rivet";
        let disable = TlsConfig {
            mode: TlsMode::Disable,
            ..Default::default()
        };
        let verify = TlsConfig {
            mode: TlsMode::VerifyFull,
            ..Default::default()
        };

        // Remote + no tls block → refused.
        assert!(require_tls_or_loopback(remote, None).is_err());
        // Loopback + no tls block → allowed (docker / dev path).
        assert!(require_tls_or_loopback(loopback, None).is_ok());
        // Explicit `mode: disable` is the remote-plaintext opt-in → allowed.
        assert!(require_tls_or_loopback(remote, Some(&disable)).is_ok());
        // Enforced TLS to a remote host → allowed (the connect path uses TLS).
        assert!(require_tls_or_loopback(remote, Some(&verify)).is_ok());
    }

    #[test]
    fn host_port_span_extracts_the_authority() {
        assert_eq!(host_port_span("mysql://u:p@host:3306/db"), "host:3306");
        assert_eq!(
            host_port_span("postgres://127.0.0.1:5432/db"),
            "127.0.0.1:5432"
        );
        // No authority at all.
        assert_eq!(host_port_span("mysql://"), "");
        assert_eq!(host_port_span("postgres:///db"), "");
    }

    #[test]
    fn hostless_url_is_a_parse_error_not_a_tls_refusal() {
        // #dogfood LOW: `mysql://` has NO host, yet the gate reported "remote
        // (non-loopback) host, TLS required" and prescribed a TLS block for a
        // host that doesn't exist. It must be a clear parse error instead.
        for u in ["mysql://", "postgres:///db", "sqlserver://"] {
            let err = require_tls_or_loopback(u, None)
                .expect_err("a host-less URL must error, not connect");
            let msg = err.to_string();
            assert!(
                msg.contains("no host found"),
                "host-less URL must be a parse error: {msg}"
            );
            assert!(
                !msg.contains("TLS required"),
                "host-less URL must NOT prescribe a TLS block: {msg}"
            );
        }
    }

    #[test]
    fn hostless_socket_url_with_explicit_tls_disable_is_allowed() {
        // #16 bughunt: a unix-socket URL has no authority host (the socket path
        // lives in `?host=`), so the host-presence check rejected it. But an
        // explicit `tls: { mode: disable }` is the operator's opt-in for a LOCAL
        // connection — it must connect, as it did on main (where the gate was
        // skipped whenever tls was Some). The check now lives inside the no-tls
        // branch, so tls=Some(disable) is never refused.
        let disable = TlsConfig {
            mode: TlsMode::Disable,
            ..Default::default()
        };
        for u in [
            "postgres:///rivet?host=/var/run/postgresql",
            "mysql://",
            "postgres:///db",
        ] {
            require_tls_or_loopback(u, Some(&disable))
                .expect("an explicit tls: { mode: disable } must not be refused for a socket URL");
        }
    }
}

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
