pub(crate) mod batch_controller;
pub(crate) mod cdc;
pub mod mssql;
pub mod mysql;
pub(crate) mod pg_numeric_wire;
pub mod postgres;
pub(crate) mod query;
pub(crate) mod tls;

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
pub(crate) fn host_is_loopback(url: &str) -> bool {
    // Strip the scheme (`postgresql://`, `mysql://`, `sqlserver://`, …).
    let after_scheme = match url.split_once("://") {
        Some((_, rest)) => rest,
        None => url,
    };
    // Authority ends at the first `/`, `?` or `#`.
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(after_scheme);
    // Drop `user[:pass]@` — rsplit the last `@` so an `@` inside a password is
    // tolerated (it belongs to the userinfo, not the host).
    let host_port = match authority.rsplit_once('@') {
        Some((_, hp)) => hp,
        None => authority,
    };
    // Host vs port. IPv6 literals are bracketed (`[::1]:5432`); for those the
    // host is the bracketed span, and any `:` inside is part of the address.
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
pub(crate) fn require_tls_or_loopback(url: &str, tls: Option<&TlsConfig>) -> Result<()> {
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
        anyhow::bail!("{msg}");
    }
    Ok(())
}

#[cfg(test)]
mod tls_gate_tests {
    use super::{host_is_loopback, require_tls_or_loopback};
    use crate::config::{TlsConfig, TlsMode};

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
}
