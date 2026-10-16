//! **Layer: Cross-cutting**
//!
//! Error type alias plus the **exit-code taxonomy**: a small, stable set of
//! process exit codes so an *unattended scheduler* can branch on the failure
//! *class* instead of grepping stderr. Before this, `main` exited `1` for every
//! error, forcing operators to regex the error text to decide retry-vs-stop.

pub mod retry;

/// Machine-actionable exit-code taxonomy.
///
/// A scheduler keys its retry / alert policy off the numeric exit code:
///
/// | code | class | scheduler action |
/// |------|-------|------------------|
/// | `0`  | success | — (handled separately, not in this enum) |
/// | `1`  | [`Generic`](ExitClass::Generic): config / usage / unclassified error | fix the config; do **not** retry blindly |
/// | `2`  | [`Retryable`](ExitClass::Retryable): transient (connection reset, lock-wait timeout, capacity), or busy (another live rivet holds the lease or lock) | safe to retry the *same* command |
/// | `3`  | [`DataIntegrity`](ExitClass::DataIntegrity): quality gate / reconcile mismatch / `validate` verification failure / duplicate-guard / manifest inconsistency | **STOP** — data may be wrong, do **not** blindly retry |
/// | `4`  | [`SchemaDrift`](ExitClass::SchemaDrift): `on_schema_drift: fail` tripped | the source shape changed — needs human review |
/// | `5`  | [`Refusal`](ExitClass::Refusal): a protective stop (a foreign cursor, a newer state DB, a table rivet does not own) | a human decides; do **not** retry unchanged |
/// | `6`  | [`Internal`](ExitClass::Internal): an invariant did not hold | a bug — report it |
///
/// ## Overlap with clap's usage exit (also `2`)
///
/// clap exits `2` on an argument-parse error (bad flag, missing required arg).
/// That collides numerically with [`Retryable`](ExitClass::Retryable) `= 2`, but
/// the two are distinguishable: clap's exit happens **pre-dispatch**, before any
/// `rivet` work runs, so it prints *only* a clap usage block and **no** `Error:`
/// line. A retryable rivet failure always prints an `Error: …` line (or a JSON
/// object with `"exit_class": 2`). We deliberately do not fight clap by remapping
/// our retryable code — `2 = retryable` matches the spec, and the usage overlap
/// is documented and detectable by the absence of a rivet error line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ExitClass {
    /// `1` — config / usage / unclassified error. Fix the input; retrying the
    /// identical command will fail the same way.
    Generic = 1,
    /// `2` — transient failure (connection reset, lock-wait timeout, capacity), or a
    /// [`Busy`](ErrorKind::Busy) stop. Safe to retry the same command after a backoff.
    Retryable = 2,
    /// `3` — data-integrity failure (quality gate, reconcile mismatch, `validate`
    /// verification failure, duplicate-guard, manifest inconsistency). The
    /// exported data may be wrong; **stop** and investigate rather than retry.
    DataIntegrity = 3,
    /// `4` — schema-drift failure (`on_schema_drift: fail` tripped). The source
    /// shape changed; a human must review before re-running.
    SchemaDrift = 4,
    /// `5` — a protective REFUSAL: rivet stopped on purpose so as not to lose, duplicate
    /// or overwrite data. Retrying unchanged refuses again; a human decides.
    Refusal = 5,
    /// `6` — INTERNAL: an invariant rivet relies on did not hold. A bug — report it.
    Internal = 6,
}

impl ExitClass {
    /// The process exit code for this class.
    pub fn code(self) -> i32 {
        self as i32
    }

    /// How stop-worthy this class is when choosing one exit among many failures: integrity > internal > refusal > drift > retryable > generic.
    pub fn stop_rank(self) -> u8 {
        match self {
            ExitClass::Generic => 0,
            ExitClass::Retryable => 1,
            ExitClass::SchemaDrift => 2,
            ExitClass::Refusal => 3,
            ExitClass::Internal => 4,
            ExitClass::DataIntegrity => 5,
        }
    }

    /// The class a process exit code names, `None` for a signal or unknown code.
    pub fn from_code(code: i32) -> Option<Self> {
        [
            ExitClass::Generic,
            ExitClass::Retryable,
            ExitClass::DataIntegrity,
            ExitClass::SchemaDrift,
            ExitClass::Refusal,
            ExitClass::Internal,
        ]
        .into_iter()
        .find(|c| c.code() == code)
    }
}

/// Typed marker for a **data-integrity** failure (exit `3`).
///
/// Mirrors [`crate::source::StatementDurationTimeout`]: the *type*, not the
/// wording, carries the classification. [`classify_exit`] downcasts it through
/// the anyhow chain, so a reworded human message never silently flips the exit
/// code. Constructed at the data-integrity bail sites (quality-gate failure,
/// duplicate-guard) wrapping the existing message verbatim — `Display`
/// reproduces the original text unchanged, so operator-facing output is
/// identical.
#[derive(Debug)]
pub struct DataIntegrityError(String);

impl DataIntegrityError {
    /// Wrap an existing human-facing message as a data-integrity failure.
    /// The message text is preserved verbatim for `Display`.
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl std::fmt::Display for DataIntegrityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for DataIntegrityError {}

/// Typed marker for a **schema-drift** failure (exit `4`).
///
/// Same contract as [`DataIntegrityError`]: classification rides on the type via
/// downcast, `Display` reproduces the original message verbatim. Constructed
/// where `on_schema_drift: fail` aborts the run.
#[derive(Debug)]
pub struct SchemaDriftError(String);

impl SchemaDriftError {
    /// Wrap an existing human-facing message as a schema-drift failure.
    /// The message text is preserved verbatim for `Display`.
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl std::fmt::Display for SchemaDriftError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SchemaDriftError {}

/// Typed marker carrying an **already-decided** process exit code.
///
/// A parallel-export child runs in its own process, classifies its own failure,
/// and exits with that code; the typed marker itself cannot cross the process
/// boundary — only the integer code does. The parent wraps the aggregate failure
/// in this marker so [`classify_exit`] re-derives the SAME class instead of
/// stringifying `"exited with status 3"` and collapsing it to a generic `1`.
#[derive(Debug)]
pub struct PreclassifiedExit(pub i32);

impl std::fmt::Display for PreclassifiedExit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "exit class {}", self.0)
    }
}

impl std::error::Error for PreclassifiedExit {}

/// A statement-DURATION timeout that **rivet itself** raised — distinct from a
/// driver-native timeout that carries a structured code (PG 57014, MySQL 3024).
///
/// The MSSQL engine has no server-side statement-duration `SET`, so rivet
/// enforces `tuning.statement_timeout_s` client-side and raises this when the
/// budget is exceeded (see [`crate::source::mssql`]). Before this type the retry classifier's
/// permanence hinged on substring-matching rivet's OWN prose ("statement
/// timeout after …"); a reworded message would silently flip the error back to
/// *transient*, and the identical query would be retried until it burned the
/// budget N times (measured: 3×300 s = 20 min for 0 rows). Carrying a typed
/// marker means [`retry::classify_error`] downcasts the TYPE,
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

    /// Oracle statement-duration timeout: the driver call timeout plus a check between rows.
    #[cfg(feature = "oracle")]
    pub fn oracle(seconds: u64) -> Self {
        Self {
            message: format!(
                "oracle: statement timeout after {seconds}s (tuning.statement_timeout_s) — \
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

/// A connect that failed in the TLS handshake — a configuration fault no retry fixes (the classifier keys on the TYPE).
#[derive(Debug)]
pub struct TlsHandshakeFailed(String);

impl std::fmt::Display for TlsHandshakeFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl TlsHandshakeFailed {
    /// Put the TLS verdict (and its fix) in front of a driver error; `at` names the endpoint when known.
    pub(crate) fn wrap(err: anyhow::Error, at: Option<&str>) -> anyhow::Error {
        let with = at.map(|a| format!(" with {a}")).unwrap_or_default();
        err.context(Self(format!(
            "TLS handshake{with} failed — the server does not speak TLS or its certificate \
             is not trusted: set `tls.ca_file` for a private CA, or `tls.mode: disable` if the \
             server has no TLS (trusted networks only); retrying will not help"
        )))
    }
}

/// A warehouse job rivet stopped WAITING for, having waited out its budget.
///
/// Deliberately NOT a [`Refused`]: that one means "stopped before touching the
/// warehouse", and this is the opposite — the statement was submitted and may well
/// still be running server-side. The type is what the retry classifier keys off
/// ([`retry::classify_error`]), so the permanence of a deterministic
/// timeout cannot be undone by rewording its message; retrying a wait that already
/// expired only doubles the wait.
#[derive(Debug)]
pub struct JobWaitTimeout {
    message: String,
}

impl JobWaitTimeout {
    /// The BigQuery poll loop gave up on `job_id` after `seconds`.
    pub fn bigquery(job_id: &str, seconds: u64) -> Self {
        Self {
            message: format!(
                "bigquery: stopped waiting for job `{job_id}` after {seconds}s — the job may \
                 still be RUNNING in BigQuery, so check it there before re-running: \
                 SELECT state, error_result FROM `region-<your dataset's region>`.\
                 INFORMATION_SCHEMA.JOBS WHERE job_id = '{job_id}'. An append mode that \
                 re-consumes the same runs would double them"
            ),
        }
    }
}

impl std::fmt::Display for JobWaitTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for JobWaitTimeout {}

/// Self-consistency failures detected by [`RunManifest::validate_self_consistency`].
///
/// These represent writer bugs, not destination drift; M5 destination-state
/// checks live in the validate command path.
#[derive(Debug, PartialEq)]
pub enum ManifestInconsistency {
    UnsupportedVersion { found: u32, supported: u32 },
    PartCountMismatch { declared: u32, actual: usize },
    RowCountMismatch { declared: i64, actual: i64 },
    DuplicatePartId(u32),
}

impl std::fmt::Display for ManifestInconsistency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedVersion { found, supported } => write!(
                f,
                "manifest_version {found} is not supported by this build (expected {supported})"
            ),
            Self::PartCountMismatch { declared, actual } => write!(
                f,
                "part_count declares {declared} parts but {actual} committed parts found"
            ),
            Self::RowCountMismatch { declared, actual } => write!(
                f,
                "row_count declares {declared} rows but committed parts sum to {actual}"
            ),
            Self::DuplicatePartId(id) => {
                write!(f, "duplicate part_id {id} in manifest.parts")
            }
        }
    }
}

impl std::error::Error for ManifestInconsistency {}

/// A load that stopped before touching the warehouse. The ledger records such a stop as
/// `refused`, which never makes the target rivet's own — a `failed` row can.
#[derive(Debug)]
pub struct Refused(pub(crate) anyhow::Error);

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for Refused {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

impl Refused {
    /// The error that stopped the load: its code and exit class are the stop's own.
    pub(crate) fn cause(&self) -> &anyhow::Error {
        &self.0
    }
}

/// What KIND of failure a coded error is — the dimension an operator (and the release gate)
/// branches on: fix the input, fix the environment, decide, stop and investigate, report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Invalid config or command line: the same input fails the same way.
    Usage,
    /// The world outside rivet: network, credentials, permissions, a missing object.
    Environment,
    /// rivet stopped on purpose so as not to lose, duplicate or overwrite data.
    Refusal,
    /// A live rivet process holds the lease or lock this command needs: the same command succeeds once it ends.
    Busy,
    /// A verification found the data wrong.
    Integrity,
    /// An invariant did not hold: a bug.
    Internal,
}

impl ErrorKind {
    /// The lowercase name the docs and JSON use.
    pub fn name(self) -> &'static str {
        match self {
            ErrorKind::Usage => "usage",
            ErrorKind::Environment => "environment",
            ErrorKind::Refusal => "refusal",
            ErrorKind::Busy => "busy",
            ErrorKind::Integrity => "integrity",
            ErrorKind::Internal => "internal",
        }
    }
}

/// One registered error code: its stable id, kind, and the one thing an operator does about it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Code {
    pub id: &'static str,
    pub kind: ErrorKind,
    pub action: &'static str,
}

/// Typed marker carrying a **stable error code** (`RIVET_CONFIG_*` /
/// `RIVET_SOURCE_*`), for config / source failures that an operator's tooling
/// greps by code rather than by wording.
///
/// Same contract as [`DataIntegrityError`]: the code rides on the type via
/// downcast (so a reworded message never moves the code), and `Display`
/// reproduces the wrapped message verbatim — the console line is unchanged except
/// for the `[CODE]` prefix `main` adds. [`error_code`] reads `code` for the JSON
/// `code` field + the text prefix.
///
/// Its exit class comes from the code's [`ErrorKind`] (see `stop_class`); an
/// `Environment` kind is left to the transient check.
#[derive(Debug)]
pub struct CodedError {
    code: Code,
    message: String,
}

impl CodedError {
    /// Wrap a human-facing message with a registered `RIVET_*` code.
    pub fn new(code: Code, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    /// The stable `RIVET_*` code id.
    pub fn code(&self) -> &'static str {
        self.code.id
    }

    /// The kind the code is registered as.
    pub fn kind(&self) -> ErrorKind {
        self.code.kind
    }
}

impl std::fmt::Display for CodedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for CodedError {}

/// An error value carrying `code`, for `map_err` / `ok_or_else` / `Err(..)` sites.
pub fn coded(code: Code, message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(CodedError::new(code, message))
}

/// `message` built around `inner`, carrying `inner`'s code when it has one, else `fallback`.
pub fn recoded(inner: &anyhow::Error, fallback: Code, message: impl Into<String>) -> anyhow::Error {
    coded(code_of(inner).unwrap_or(fallback), message)
}

/// The stable `RIVET_*` error code for a failure, if one was tagged via
/// [`CodedError`] anywhere in the anyhow context chain. `main` surfaces it as the
/// JSON `code` field and a `[CODE]` prefix on the text error line.
pub fn error_code(err: &anyhow::Error) -> Option<&'static str> {
    let err = stop_cause(err);
    if let Some(c) = err.downcast_ref::<CodedError>() {
        return Some(c.code());
    }
    // The existing source-side statement-timeout marker also gets a stable code,
    // so the long-query failure an operator's `statement_timeout` tooling watches
    // for is greppable without re-tagging its construction site.
    if err.downcast_ref::<StatementDurationTimeout>().is_some() {
        return Some(codes::SOURCE_STATEMENT_TIMEOUT.id);
    }
    None
}

/// The registered [`Code`] a failure carries through [`CodedError`], if any.
pub(crate) fn code_of(err: &anyhow::Error) -> Option<Code> {
    stop_cause(err).downcast_ref::<CodedError>().map(|c| c.code)
}

/// Map an error to its process exit code per the [`ExitClass`] taxonomy.
///
/// Precedence (first match wins):
/// 1. [`PreclassifiedExit`] → its code verbatim.
/// 2. `stop_class`: [`SchemaDriftError`] → `4`; [`DataIntegrityError`] /
///    [`crate::manifest::ManifestInconsistency`] → `3`; a [`CodedError`] by kind
///    (refusal `5`, internal `6`, integrity `3`, usage `1`).
/// 3. otherwise, if [`retry::classify_error`] says the error is
///    transient → `2`.
/// 4. otherwise → `1` (generic).
///
/// ## Why a string bridge for the aggregated `run` path
///
/// The single-export `apply` path returns the typed marker straight to `main`,
/// so the downcasts below fire directly. The multi-export `run` path used to
/// flatten per-export failures into a `Vec<String>` and re-raise a fresh
/// `anyhow!`, erasing the concrete type — which once forced a substring bridge
/// here. `pipeline::run` now carries a **representative typed failure** instead
/// (the most stop-worthy class among the failures), so the marker survives and
/// the downcasts work for `rivet run` too. Classification is therefore purely
/// type-driven: an un-typed data-integrity / drift failure classifies as
/// `Generic` on purpose — a *visible* signal that a marker was dropped upstream,
/// rather than being silently rescued by string matching.
pub fn classify_exit(err: &anyhow::Error) -> i32 {
    let err = stop_cause(err);
    // Each check downcasts through anyhow's context chain.
    // A child process already classified itself and exited with that code; honor
    // it verbatim (parallel-export path) so the parent surfaces the same class.
    if let Some(p) = err.downcast_ref::<PreclassifiedExit>() {
        return p.0;
    }
    if let Some(c) = stop_class(err) {
        return c.code();
    }
    if retry::classify_error(err).is_transient() {
        return ExitClass::Retryable.code();
    }
    ExitClass::Generic.code()
}

/// The error under a load's stop-before-write marker, which carries the code and the class.
fn stop_cause(err: &anyhow::Error) -> &anyhow::Error {
    err.downcast_ref::<Refused>().map_or(err, Refused::cause)
}

/// The class a typed stop marker in the chain fixes regardless of wording; `None` leaves it to the transient check.
pub(crate) fn stop_class(err: &anyhow::Error) -> Option<ExitClass> {
    if err.downcast_ref::<SchemaDriftError>().is_some() {
        return Some(ExitClass::SchemaDrift);
    }
    if err.downcast_ref::<DataIntegrityError>().is_some()
        || err.downcast_ref::<ManifestInconsistency>().is_some()
    {
        return Some(ExitClass::DataIntegrity);
    }
    // An environment failure still goes through the transient check (a dropped
    // connection retries, a denied permission does not).
    match err.downcast_ref::<CodedError>()?.kind() {
        ErrorKind::Refusal => Some(ExitClass::Refusal),
        ErrorKind::Busy => Some(ExitClass::Retryable),
        ErrorKind::Internal => Some(ExitClass::Internal),
        ErrorKind::Integrity => Some(ExitClass::DataIntegrity),
        ErrorKind::Usage => Some(ExitClass::Generic),
        ErrorKind::Environment => None,
    }
}

/// Index of the most "stop-worthy" failure in a batch, ranked by
/// [`crate::error::ExitClass::stop_rank`]. The chosen error's typed marker then rides up so `classify_exit`
/// exits the process on the scariest reason rather than whichever export happened
/// to fail first. Returns `None` for an empty slice.
pub(crate) fn representative_failure_idx(failures: &[anyhow::Error]) -> Option<usize> {
    (0..failures.len()).max_by_key(|&i| failure_rank(&failures[i]))
}

/// The [`ExitClass::stop_rank`] of the class `err` exits by.
fn failure_rank(err: &anyhow::Error) -> u8 {
    ExitClass::from_code(classify_exit(err)).map_or(0, ExitClass::stop_rank)
}

/// Index of the most stop-worthy failure that carries a code or a typed class (the last of equals), `None` when none does.
pub(crate) fn worst_typed_failure_idx(failures: &[anyhow::Error]) -> Option<usize> {
    (0..failures.len())
        .filter(|&i| {
            let e = &failures[i];
            error_code(e).is_some()
                || stop_class(e).is_some()
                || e.downcast_ref::<PreclassifiedExit>().is_some()
        })
        .max_by_key(|&i| failure_rank(&failures[i]))
}

/// Stable, greppable error codes carried by [`CodedError`]. A scheduler / CI step
/// matches on these (the JSON `code` field or the `[CODE]` text prefix) instead
/// of the human wording, which is free to change. Every code shares the
/// `RIVET_CONFIG_` or `RIVET_SOURCE_` prefix; the `codes_*` guard tests assert
/// distinctness + the prefix, mirroring the verify-layer `RIVET_VERIFY_*` guard.
pub mod codes {
    use super::{Code, ErrorKind};

    const fn usage(id: &'static str, action: &'static str) -> Code {
        Code {
            id,
            kind: ErrorKind::Usage,
            action,
        }
    }
    const fn environment(id: &'static str, action: &'static str) -> Code {
        Code {
            id,
            kind: ErrorKind::Environment,
            action,
        }
    }

    pub const CONFIG_NO_EXPORTS: Code = usage(
        "RIVET_CONFIG_NO_EXPORTS",
        "declare at least one export under `exports:`",
    );
    pub const CONFIG_CHUNK_COUNT_INVALID: Code = usage(
        "RIVET_CONFIG_CHUNK_COUNT_INVALID",
        "set `chunk_count` to 1 or more",
    );
    pub const CONFIG_CHUNK_BY_DAYS_INVALID: Code = usage(
        "RIVET_CONFIG_CHUNK_BY_DAYS_INVALID",
        "set `chunk_by_days` to 1 or more",
    );
    /// A range-sliced `chunk_column` whose values are not all whole numbers: integer windows would skip the rest.
    pub const CONFIG_CHUNK_COLUMN_NOT_INTEGER: Code = usage(
        "RIVET_CONFIG_CHUNK_COLUMN_NOT_INTEGER",
        "use `chunk_by_key:` (keyset), an integer column, or `mode: full`",
    );
    pub const CONFIG_DUPLICATE_EXPORT: Code = usage(
        "RIVET_CONFIG_DUPLICATE_EXPORT",
        "give every export a unique `name`",
    );
    /// Two `mode: cdc` exports resolved to the same per-engine stream resource
    /// (PostgreSQL slot / MySQL server_id / checkpoint path) — including the
    /// defaults colliding, which is what a naive multi-table CDC config hits.
    pub const CONFIG_CDC_RESOURCE_CONFLICT: Code = usage(
        "RIVET_CONFIG_CDC_RESOURCE_CONFLICT",
        "give each CDC export its own slot / server_id / checkpoint path",
    );
    pub const CONFIG_CDC_ROLLOVER_INVALID: Code = usage(
        "RIVET_CONFIG_CDC_ROLLOVER_INVALID",
        "set `cdc.rollover` to 1 or more, or omit it",
    );
    /// `until_current: false` / `--stream` on an engine that only drains to an open-time bound (Oracle).
    pub const CONFIG_CDC_CONTINUOUS_UNSUPPORTED: Code = usage(
        "RIVET_CONFIG_CDC_CONTINUOUS_UNSUPPORTED",
        "omit `cdc.until_current` (or `--stream`) and run the bounded drain on a schedule",
    );
    pub const CONFIG_CSV_LOAD_UNSUPPORTED: Code = usage(
        "RIVET_CONFIG_CSV_LOAD_UNSUPPORTED",
        "use `format: parquet` for an export with a `load:` section",
    );
    /// A `partition:` form, option or column type the load target cannot take.
    pub const CONFIG_LOAD_PARTITION_UNSUPPORTED: Code = usage(
        "RIVET_CONFIG_LOAD_PARTITION_UNSUPPORTED",
        "partition by a date or time `column` + `granularity`, and drop the options the target refuses",
    );
    /// An export mode is not supported by the configured source type — today
    /// this is a non-SQL source (MongoDB) with any `mode:` other than `full`.
    pub const CONFIG_SOURCE_MODE_UNSUPPORTED: Code = usage(
        "RIVET_CONFIG_SOURCE_MODE_UNSUPPORTED",
        "use a mode this source supports (MongoDB: `full`)",
    );
    /// A raw `url:` whose scheme names a different engine than `source.type`.
    pub const CONFIG_SOURCE_URL_SCHEME_MISMATCH: Code = usage(
        "RIVET_CONFIG_SOURCE_URL_SCHEME_MISMATCH",
        "make `source.type` and the URL scheme name the same engine",
    );
    /// A MySQL keyset key under a `uuid` column override: the seek binds hex text, not the stored order.
    pub const CONFIG_KEYSET_KEY_UUID_OVERRIDE: Code = usage(
        "RIVET_CONFIG_KEYSET_KEY_UUID_OVERRIDE",
        "key the keyset on another unique column, or use `mode: full` for this table",
    );
    /// `cursor_column` matches a result-set column only when case is ignored, so the cursor never advances.
    pub const CONFIG_CURSOR_COLUMN_CASE: Code = usage(
        "RIVET_CONFIG_CURSOR_COLUMN_CASE",
        "spell `cursor_column` exactly as the result set names the column",
    );
    /// A `columns:` override key matches a result-set column only when case is ignored, so it would not apply.
    pub const CONFIG_COLUMN_OVERRIDE_CASE: Code = usage(
        "RIVET_CONFIG_COLUMN_OVERRIDE_CASE",
        "spell the `columns:` key exactly as the result set names the column",
    );
    /// The config file is missing or cannot be read.
    pub const CONFIG_FILE_UNREADABLE: Code = environment(
        "RIVET_CONFIG_FILE_UNREADABLE",
        "check the config path and its read permission, or run `rivet init` to generate one",
    );
    /// The YAML does not parse, or names an unknown or misplaced key.
    pub const CONFIG_YAML_INVALID: Code = usage(
        "RIVET_CONFIG_YAML_INVALID",
        "fix the YAML syntax or the key the message names, moving it to where the message shows",
    );
    /// The `source:` block names no connection method, more than one, or an incomplete one.
    pub const CONFIG_SOURCE_CONNECTION_INVALID: Code = usage(
        "RIVET_CONFIG_SOURCE_CONNECTION_INVALID",
        "give `source:` exactly one connection method (`url_env`, `url`, `url_file` or complete host/user/database fields) and one password setting",
    );
    /// An environment variable, `--param` or file a config key points at is not there.
    pub const CONFIG_SECRET_UNSET: Code = environment(
        "RIVET_CONFIG_SECRET_UNSET",
        "export the environment variable (or pass the `--param`) the message names, or make the named file readable",
    );
    /// An export does not name exactly one well-formed `query` / `query_file` / `table` / `tables`.
    pub const CONFIG_EXPORT_SOURCE_INVALID: Code = usage(
        "RIVET_CONFIG_EXPORT_SOURCE_INVALID",
        "give the export exactly one of `query`, `query_file` (a relative path inside the config directory), `table` (`<name>` or `<schema>.<name>`) or `tables`",
    );
    /// A mode lacks a setting it requires, or a setting is set that only another mode takes.
    pub const CONFIG_MODE_SETTING_INVALID: Code = usage(
        "RIVET_CONFIG_MODE_SETTING_INVALID",
        "add the setting the export's mode requires, or remove the setting the mode does not take, as the message names",
    );
    /// A cloud destination's credential settings contradict each other or are incomplete.
    pub const CONFIG_DESTINATION_AUTH_INVALID: Code = usage(
        "RIVET_CONFIG_DESTINATION_AUTH_INVALID",
        "configure one complete auth mode for the destination (keys, a credentials file, or `allow_anonymous`) and drop the others",
    );
    /// A custom cloud `endpoint` that is neither loopback nor anonymous.
    pub const CONFIG_DESTINATION_ENDPOINT_UNSAFE: Code = usage(
        "RIVET_CONFIG_DESTINATION_ENDPOINT_UNSAFE",
        "drop `endpoint:` to use the provider default, or point it at a loopback emulator with `allow_anonymous: true`",
    );
    /// A numeric, size, duration or level value outside what rivet accepts, or two sizing knobs set together.
    pub const CONFIG_VALUE_OUT_OF_RANGE: Code = usage(
        "RIVET_CONFIG_VALUE_OUT_OF_RANGE",
        "set the value in the range or form the message names, and set only one of two mutually exclusive sizing knobs",
    );
    /// The captured-table list or the `cdc:` baseline of a `mode: cdc` export is malformed.
    pub const CONFIG_CDC_SHAPE_INVALID: Code = usage(
        "RIVET_CONFIG_CDC_SHAPE_INVALID",
        "list each captured table once, in a shape this source and destination take, with at most one baseline per table",
    );
    /// `format: csv` with a compression codec or profile the CSV writer cannot apply.
    pub const CONFIG_CSV_COMPRESSION_UNSUPPORTED: Code = usage(
        "RIVET_CONFIG_CSV_COMPRESSION_UNSUPPORTED",
        "use `format: parquet` for compression, or set `compression: none` / `compression_profile: none`",
    );
    /// An explicit enforced `source.tls.mode` next to a knob that disables verification.
    pub const CONFIG_TLS_VERIFICATION_DOWNGRADE: Code = usage(
        "RIVET_CONFIG_TLS_VERIFICATION_DOWNGRADE",
        "drop the danger knob and trust a private CA with `tls.ca_file`, or omit the explicit `mode:` for a loopback dev container",
    );
    /// A `load:` block or override that does not fit the export or the target.
    pub const CONFIG_LOAD_BLOCK_INVALID: Code = usage(
        "RIVET_CONFIG_LOAD_BLOCK_INVALID",
        "declare the warehouse in a top-level `load:` block and override only what the export and target take",
    );
    /// A name or path that becomes a destination path segment escapes it or embeds a scheme.
    pub const CONFIG_PATH_UNSAFE: Code = usage(
        "RIVET_CONFIG_PATH_UNSAFE",
        "use a plain name and a bucket-relative path: no `/`, `\\`, `..`, NUL, leading `.` or URI scheme",
    );
    /// A declared `meta_columns.row_hash` list that is empty, repeats a column, or spans several tables.
    pub const CONFIG_ROW_HASH_INVALID: Code = usage(
        "RIVET_CONFIG_ROW_HASH_INVALID",
        "list each column of one table once under `meta_columns.row_hash`, or use `row_hash: true`",
    );
    /// A table-qualified `columns:` key on a query export, or naming a table the export does not capture.
    pub const CONFIG_COLUMN_OVERRIDE_TABLE_UNKNOWN: Code = usage(
        "RIVET_CONFIG_COLUMN_OVERRIDE_TABLE_UNKNOWN",
        "use a bare column key, or qualify it with a table this export captures",
    );
    /// A column whose mapping is lossy or unsupported, refused under `--strict`.
    pub const TYPE_UNSAFE_MAPPING: Code = usage(
        "RIVET_TYPE_UNSAFE_MAPPING",
        "add a `columns:` override the engine delivers exactly, CAST or drop the column in the export's `query:`, or run without `--strict` to accept it with a warning",
    );
    /// A statement that ran past the configured duration cap, carried by the existing
    /// `source::StatementDurationTimeout` marker (recognised in [`super::error_code`]).
    pub const SOURCE_STATEMENT_TIMEOUT: Code = environment(
        "RIVET_SOURCE_STATEMENT_TIMEOUT",
        "raise `tuning.statement_timeout_s`, or narrow the chunk",
    );

    const fn refusal(id: &'static str, action: &'static str) -> Code {
        Code {
            id,
            kind: ErrorKind::Refusal,
            action,
        }
    }
    const fn busy(id: &'static str, action: &'static str) -> Code {
        Code {
            id,
            kind: ErrorKind::Busy,
            action,
        }
    }
    const fn integrity(id: &'static str, action: &'static str) -> Code {
        Code {
            id,
            kind: ErrorKind::Integrity,
            action,
        }
    }
    const fn internal(id: &'static str, action: &'static str) -> Code {
        Code {
            id,
            kind: ErrorKind::Internal,
            action,
        }
    }

    /// `RIVET_STATE_URL` set to something that is not a PostgreSQL URL.
    pub const STATE_URL_SCHEME_UNSUPPORTED: Code = usage(
        "RIVET_STATE_URL_SCHEME_UNSUPPORTED",
        "set `RIVET_STATE_URL` to a `postgres://` or `postgresql://` URL, or unset it to keep the state in the SQLite file beside the config",
    );
    /// The state store cannot be written: read-only when it is opened, or it did not take a run's first write.
    pub const STATE_NOT_WRITABLE: Code = environment(
        "RIVET_STATE_NOT_WRITABLE",
        "make the state database writable (the SQLite file and its directory, a read-only filesystem, a read-only role) and run again; the export did not start",
    );
    /// A run delivered its rows and manifest, and its incremental cursor write failed.
    pub const STATE_CURSOR_NOT_STORED: Code = environment(
        "RIVET_STATE_CURSOR_NOT_STORED",
        "repair the state database before the next run; the rows are delivered, and the next run delivers them again from the previous cursor",
    );
    pub const STATE_SCHEMA_NEWER: Code = refusal(
        "RIVET_STATE_SCHEMA_NEWER",
        "upgrade rivet, or point this binary at a state DB it created",
    );
    /// A parallel incremental keyset run met an unfinished sequential run's anchor, whose cursor ran past its unmanifested pages.
    pub const STATE_KEYSET_SEQUENTIAL_ANCHOR_UNFINISHED: Code = refusal(
        "RIVET_STATE_KEYSET_SEQUENTIAL_ANCHOR_UNFINISHED",
        "re-run once with `parallel: 1` to finish the interrupted run, then raise `parallel:`",
    );
    pub const STATE_CURSOR_OWNER_MISMATCH: Code = refusal(
        "RIVET_STATE_CURSOR_OWNER_MISMATCH",
        "`rivet state reset -c <config> --export <name>` to start the new cursor with a full pass, or restore the previous cursor column",
    );
    /// The source cannot compare the stored incremental cursor with the cursor column (a value of another type).
    pub const STATE_CURSOR_INCOMPARABLE: Code = refusal(
        "RIVET_STATE_CURSOR_INCOMPARABLE",
        "`rivet state reset -c <config> --export <name>` starts the cursor over with a full pass",
    );
    /// The stored cursor is past the `cursor_high` of the destination's last success manifest: the rows between were never delivered there.
    pub const STATE_CURSOR_PAST_MANIFEST: Code = refusal(
        "RIVET_STATE_CURSOR_PAST_MANIFEST",
        "`rivet state reset -c <config> --export <name>` starts the cursor over with a full pass",
    );
    /// Stored progress (cursor, keyset high-water, resume anchor) belongs to another table or collection, another schema, or other rows of it.
    pub const STATE_CURSOR_STREAM_MISMATCH: Code = refusal(
        "RIVET_STATE_CURSOR_STREAM_MISMATCH",
        "give each export that shares this state database its own name; if this export was edited (another table, another schema, another `query:` filter), restore what it read, or `rivet state reset -c <config> --export <name>` starts what it reads now with a full pass (it discards the progress that name holds on this source), or `rivet state accept -c <config> --export <name>` keeps the stored progress and records it as belonging to what the export reads now (rows of it below that progress are not delivered)",
    );
    /// A stored cursor was written delivering to another destination, or in another format, than the export delivers to now.
    pub const STATE_CURSOR_DESTINATION_MISMATCH: Code = refusal(
        "RIVET_STATE_CURSOR_DESTINATION_MISMATCH",
        "restore the `destination` and `format` the export had to continue from the stored progress, or `rivet state reset -c <config> --export <name>` starts over with a full pass delivered where the config points now (the parts already delivered stay where they are: empty that location first if the full pass lands in it too)",
    );
    /// An export met an unfinished checkpointed run it does not continue: another mode opened it, its checkpoint setting is off now, or its parts are in another format.
    pub const STATE_INTERRUPTED_RUN_OWNER_MISMATCH: Code = refusal(
        "RIVET_STATE_INTERRUPTED_RUN_OWNER_MISMATCH",
        "restore the settings the interrupted run had (its mode, its checkpoint, its format) and run once to finish it, then change them; or abandon it with the command the message names (`rivet state reset` for a keyset run, `rivet state reset-chunks` for a range-chunk run) and the next run starts with a full pass",
    );
    /// A table joined a CDC stream that did not capture it on its last run, over a baseline already recorded complete.
    pub const STATE_CDC_TABLE_REJOINED: Code = refusal(
        "RIVET_STATE_CDC_TABLE_REJOINED",
        "re-baseline the table the message names: move every file out of its directory in the destination, delete its `cdc_snapshot` row from the state DB, truncate its `__changes` table if a warehouse load consumes the stream, then re-run",
    );
    /// An in-progress chunk run recorded no source, and its export name holds progress under more than one source.
    pub const STATE_CHUNK_RUN_OWNER_UNKNOWN: Code = refusal(
        "RIVET_STATE_CHUNK_RUN_OWNER_UNKNOWN",
        "`rivet state reset-chunks -c <config> --export <name>` abandons the run; the next run of each config starts a fresh pass",
    );
    /// An in-progress chunk run recorded no source, and a run of its export finished after it was opened.
    pub const STATE_CHUNK_RUN_SUPERSEDED: Code = refusal(
        "RIVET_STATE_CHUNK_RUN_SUPERSEDED",
        "`rivet state reset-chunks -c <config> --export <name>` abandons the run; the next run starts a fresh pass",
    );
    /// A command that rewrites an export's stored progress met a live run of that export.
    pub const STATE_RUN_IN_PROGRESS: Code = busy(
        "RIVET_STATE_RUN_IN_PROGRESS",
        "wait for the run to finish, or stop its process, then repeat the command",
    );
    /// A resume met an unfinished chunk run opened under another query or other chunk settings.
    pub const STATE_CHUNK_PLAN_CHANGED: Code = refusal(
        "RIVET_STATE_CHUNK_PLAN_CHANGED",
        "restore the query and the chunk settings the interrupted run had and run once to finish it; or `rivet state reset-chunks -c <config> --export <name>` abandons it and the next run starts over",
    );
    /// The chunk windows stored for an unfinished run are not one unbroken sequence of keys.
    pub const STATE_CHUNK_WINDOWS_BROKEN: Code = refusal(
        "RIVET_STATE_CHUNK_WINDOWS_BROKEN",
        "`rivet state reset-chunks -c <config> --export <name>` abandons the run; the next run plans the export again and reads every key",
    );
    /// A checkpointed run found its `chunk_run` row gone when it came to complete it.
    pub const STATE_CHUNK_CHECKPOINT_GONE: Code = refusal(
        "RIVET_STATE_CHUNK_CHECKPOINT_GONE",
        "run the export again: it starts a new chunk run (the parts the lost run wrote stay in the destination, in no manifest)",
    );
    pub const SOURCE_CURSOR_FINER_THAN_MICROSECOND: Code = refusal(
        "RIVET_SOURCE_CURSOR_FINER_THAN_MICROSECOND",
        "cursor on a column at microsecond precision or coarser, or cast the cursor to TIMESTAMP(6) in a curated query",
    );
    pub const SOURCE_CDC_FOREIGN_CHECKPOINT: Code = refusal(
        "RIVET_SOURCE_CDC_FOREIGN_CHECKPOINT",
        "re-baseline the stream: delete the checkpoint, empty the export's destination, clear its `cdc_snapshot` rows, give it `cdc.initial: snapshot`, then re-run",
    );
    pub const SOURCE_CDC_CHECKPOINT_INVALID: Code = refusal(
        "RIVET_SOURCE_CDC_CHECKPOINT_INVALID",
        "restore the checkpoint file, or re-baseline the stream: delete it, empty the export's destination, clear its `cdc_snapshot` rows, give it `cdc.initial: snapshot`, then re-run",
    );
    pub const SOURCE_CDC_LOG_GAP: Code = refusal(
        "RIVET_SOURCE_CDC_LOG_GAP",
        "restore the missing log, or re-baseline the stream: delete the checkpoint, empty the export's destination, clear its `cdc_snapshot` rows, give it `cdc.initial: snapshot`, then re-run",
    );
    /// A captured table was TRUNCATEd: its removed rows have no change events to retract them downstream.
    pub const SOURCE_CDC_TRUNCATED: Code = refusal(
        "RIVET_SOURCE_CDC_TRUNCATED",
        "re-baseline the stream: delete the checkpoint (PostgreSQL: first advance the slot past the truncate), empty the export's destination, clear its `cdc_snapshot` rows, give it `cdc.initial: snapshot`, then re-run",
    );
    pub const SOURCE_CDC_UNDECODABLE: Code = refusal(
        "RIVET_SOURCE_CDC_UNDECODABLE",
        "re-baseline the stream: delete the checkpoint, empty the export's destination, clear its `cdc_snapshot` rows, give it `cdc.initial: snapshot`, then re-run",
    );
    /// A captured cell holding a value the CDC decoder has no faithful reading for.
    pub const SOURCE_CDC_CELL_UNSUPPORTED: Code = refusal(
        "RIVET_SOURCE_CDC_CELL_UNSUPPORTED",
        "leave the column out of the capture (SQL Server: @captured_column_list), then re-baseline the stream: delete the checkpoint, empty the export's destination, clear its `cdc_snapshot` rows, give it `cdc.initial: snapshot`, then re-run",
    );
    pub const SOURCE_CDC_PREREQUISITE: Code = environment(
        "RIVET_SOURCE_CDC_PREREQUISITE",
        "apply the setup statement the message names, then re-run (docs/reference/cdc.md)",
    );
    /// A source value the column's Arrow type has no faithful reading for (PG `infinity`, `24:00:00`).
    pub const SOURCE_VALUE_UNREPRESENTABLE: Code = refusal(
        "RIVET_SOURCE_VALUE_UNREPRESENTABLE",
        "map the value to a representable one in the export's `query:`, or exclude the column",
    );
    /// A planner probe (range bound, keyset boundary or ceiling) returned a value of a type the adapter has no reader for.
    pub const SOURCE_PROBE_UNREADABLE: Code = refusal(
        "RIVET_SOURCE_PROBE_UNREADABLE",
        "chunk, page or partition the export on a column of a type the message lists, remove `parallel:` from a keyset export, or use `mode: full`",
    );
    /// A read returned fewer columns than its type probe described a moment earlier: the table changed between the two.
    pub const SOURCE_COLUMNS_CHANGED_UNDER_READ: Code = environment(
        "RIVET_SOURCE_COLUMNS_CHANGED_UNDER_READ",
        "run the export again: the next run probes and reads the table as it is now",
    );
    /// A `columns:` override declares a type the source's wire value cannot be read as.
    pub const SOURCE_OVERRIDE_WIRE_MISMATCH: Code = usage(
        "RIVET_SOURCE_OVERRIDE_WIRE_MISMATCH",
        "remove or correct the column's `columns:` override (batch: or CAST the column to that type in the export's `query:`; CDC: then re-snapshot the table)",
    );
    /// A PostgreSQL-state lease was not granted: the connection that renews this process's leases could not be opened.
    pub const STATE_LEASE_KEEPER_UNAVAILABLE: Code = environment(
        "RIVET_STATE_LEASE_KEEPER_UNAVAILABLE",
        "free a connection on the state database (rivet holds one per worker plus one for the lease keeper), raise the open-file limit, or lower `--pool`, then run again; nothing ran under the lease",
    );
    /// A PostgreSQL-state lease stopped being this process's while the work it guards was under way.
    pub const STATE_LEASE_LOST: Code = busy(
        "RIVET_STATE_LEASE_LOST",
        "wait for the other `rivet load` or `rivet compact` of that table to end, then run again; nothing was written to the warehouse",
    );
    /// `rivet plan` could not read the source it plans over (no connection, a refused login, a missing table or collection).
    pub const PLAN_SOURCE_UNREADABLE: Code = environment(
        "RIVET_PLAN_SOURCE_UNREADABLE",
        "fix what the message names (the connection, the credentials, or the table the export reads), then run `rivet plan` again",
    );
    /// A preflight met a MongoDB collection the database does not list.
    pub const SOURCE_COLLECTION_NOT_FOUND: Code = environment(
        "RIVET_SOURCE_COLLECTION_NOT_FOUND",
        "correct the export's `table:` (a dotted collection name is written whole), or create the collection",
    );
    /// The bucket or container a cloud destination names does not exist.
    pub const DEST_CONTAINER_NOT_FOUND: Code = environment(
        "RIVET_DEST_CONTAINER_NOT_FOUND",
        "create the bucket or container, or correct `destination.bucket`",
    );
    /// `run --resume` met a destination prefix that already holds a complete export.
    pub const DEST_ALREADY_COMPLETE: Code = refusal(
        "RIVET_DEST_ALREADY_COMPLETE",
        "pass `--force` with `--resume` to continue an interrupted run or, with none, run as a plain run does (new parts beside the complete export); or use another destination prefix",
    );
    pub const LOAD_VALUE_OUT_OF_TARGET_RANGE: Code = refusal(
        "RIVET_LOAD_VALUE_OUT_OF_TARGET_RANGE",
        "the warehouse type cannot hold this value; declare a wider type (e.g. String) for the column, or fix the source value",
    );
    pub const LOAD_COUNT_MISMATCH: Code = integrity(
        "RIVET_LOAD_COUNT_MISMATCH",
        "compare the warehouse table with the run's manifest before re-running; the source is kept",
    );
    /// A load or compact found its table lease lost after it had written the warehouse.
    pub const LOAD_LEASE_LOST_DURING_WRITE: Code = integrity(
        "RIVET_LOAD_LEASE_LOST_DURING_WRITE",
        "compare the warehouse table with the source before relying on it: another load or compact may have written it at the same time; the next `rivet load` loads the same runs again",
    );
    pub const LOAD_ADOPTION_COLUMN_MISMATCH: Code = refusal(
        "RIVET_LOAD_ADOPTION_COLUMN_MISMATCH",
        "add the export's new columns to the table (`ALTER TABLE … ADD COLUMN`) and re-run; do not rename it aside",
    );
    pub const LOAD_TARGET_NOT_RIVETS: Code = refusal(
        "RIVET_LOAD_TARGET_NOT_RIVETS",
        "the warehouse object exists and this state DB has no record of rivet loading it: drop or rename it, or load into another table",
    );
    /// A table, column or key name the loader splices into SQL is not a plain identifier.
    pub const LOAD_IDENTIFIER_UNSAFE: Code = usage(
        "RIVET_LOAD_IDENTIFIER_UNSAFE",
        "rename the column or table in the source, or alias it in the export's `query:`, to a plain identifier ([A-Za-z_][A-Za-z0-9_]*)",
    );
    /// A `pk`, `cluster_by`, partition or cursor column the export does not deliver, or the target cannot use.
    pub const LOAD_KEY_COLUMN_INVALID: Code = usage(
        "RIVET_LOAD_KEY_COLUMN_INVALID",
        "name only columns the export delivers, in a type and number the target takes, under `pk:` / `cluster_by:` / `partition:` in the export's `load:` block (or add the column to the export's SELECT), as the message names",
    );
    /// An append load (CDC, incremental, continued key) has no primary key for its dedup.
    pub const LOAD_PRIMARY_KEY_MISSING: Code = usage(
        "RIVET_LOAD_PRIMARY_KEY_MISSING",
        "declare the key under the export's `load:` block, e.g. `pk: [id]`",
    );
    /// A source column carries a `__` name rivet owns in a change log or base table.
    pub const LOAD_RESERVED_COLUMN: Code = usage(
        "RIVET_LOAD_RESERVED_COLUMN",
        "rename or exclude the column in the export's query, or load the table with `mode: full`",
    );
    /// A column name with Cyrillic look-alike letters the load cannot rename for this target.
    pub const LOAD_LOOKALIKE_COLUMN: Code = usage(
        "RIVET_LOAD_LOOKALIKE_COLUMN",
        "rename the column with Cyrillic look-alike letters in the source to Latin letters, as the message names",
    );
    /// Two exports of one config resolve to the same warehouse object.
    pub const LOAD_TARGET_COLLISION: Code = usage(
        "RIVET_LOAD_TARGET_COLLISION",
        "give each export its own `table:` or destination",
    );
    /// A `destination.prefix` the load cannot list the way the export wrote it.
    pub const LOAD_STAGING_PREFIX_INVALID: Code = usage(
        "RIVET_LOAD_STAGING_PREFIX_INVALID",
        "set a non-empty `destination.prefix` with no `{date}` or `{run_id}` above `{partition}`, and a literal segment before `{partition}`",
    );
    /// An export with `load:` staged to a destination type the load target does not read.
    pub const LOAD_DESTINATION_TYPE_UNSUPPORTED: Code = usage(
        "RIVET_LOAD_DESTINATION_TYPE_UNSUPPORTED",
        "stage the export to a destination the target reads (GCS; ClickHouse also S3 and Azure), or drop the `load:` block",
    );
    /// One Parquet file spans more partitions than one BigQuery load job may write.
    pub const LOAD_PARTITION_BUDGET_EXCEEDED: Code = usage(
        "RIVET_LOAD_PARTITION_BUDGET_EXCEEDED",
        "partition by a coarser `granularity`, or export a narrower range per run, as the message names",
    );
    /// More files than one Snowflake COPY takes.
    pub const LOAD_FILE_LIMIT_EXCEEDED: Code = usage(
        "RIVET_LOAD_FILE_LIMIT_EXCEEDED",
        "load after fewer runs, or write fewer, larger parts (lower the export's parallelism)",
    );
    /// A column whose type does not map to the load target.
    pub const LOAD_COLUMN_TYPE_UNSUPPORTED: Code = usage(
        "RIVET_LOAD_COLUMN_TYPE_UNSUPPORTED",
        "CAST or drop the column in the export's `query:`, or add a `columns:` override to a type the target takes",
    );
    /// `load.ca_file` holds no certificate.
    pub const LOAD_TLS_CA_INVALID: Code = usage(
        "RIVET_LOAD_TLS_CA_INVALID",
        "point `load.ca_file` at a PEM bundle holding the server's CA certificate",
    );
    /// The newest run exported no rows, and the target cannot be emptied to match.
    pub const LOAD_EMPTY_RUN_UNSUPPORTED: Code = refusal(
        "RIVET_LOAD_EMPTY_RUN_UNSUPPORTED",
        "TRUNCATE the warehouse table by hand, or keep its previous rows knowingly; nothing was written",
    );
    /// The runs to load are not complete under the prefix: no Success manifest, or parts gone.
    pub const LOAD_RUN_NOT_LOADABLE: Code = refusal(
        "RIVET_LOAD_RUN_NOT_LOADABLE",
        "re-run the export so a complete run (a Success manifest and every part it declares) is under the prefix the load reads, then load",
    );
    /// An object under the staging prefix a rivet export did not write: an unsafe name, an oversized manifest, a broken Parquet file.
    pub const LOAD_STAGING_OBJECT_UNSAFE: Code = refusal(
        "RIVET_LOAD_STAGING_OBJECT_UNSAFE",
        "investigate the staging bucket: remove the object the message names, then re-run the export and the load",
    );
    /// A prefix or target table shared by two exports or two sources.
    pub const LOAD_SOURCES_SHARED: Code = refusal(
        "RIVET_LOAD_SOURCES_SHARED",
        "give each export and each source its own destination prefix and its own target table",
    );
    /// A source column renamed between runs that are still unloaded.
    pub const LOAD_COLUMN_RESPELLED: Code = refusal(
        "RIVET_LOAD_COLUMN_RESPELLED",
        "load the older runs by hand under the old spelling as the message describes, then delete their manifests; every run stays staged",
    );
    /// The warehouse object at the target name is not what this load writes.
    pub const LOAD_TARGET_OBJECT_CONFLICT: Code = refusal(
        "RIVET_LOAD_TARGET_OBJECT_CONFLICT",
        "drop or rename the warehouse object the message names, or align the config with it; nothing was written",
    );
    /// `rivet compact` met a buffer whose base table does not exist.
    pub const LOAD_COMPACT_BASE_MISSING: Code = refusal(
        "RIVET_LOAD_COMPACT_BASE_MISSING",
        "load the backfill first (the `cdc.backfill:` export builds the base), or drop the buffer to discard its changes",
    );
    /// A change log whose partitioning differs from the one the load declares.
    pub const LOAD_CHANGELOG_PARTITION_CHANGED: Code = refusal(
        "RIVET_LOAD_CHANGELOG_PARTITION_CHANGED",
        "keep the change log's partitioning in the config, or rebuild it as the message names (`rivet load --rebuild-changelog`, or by hand where policies must be kept)",
    );
    /// A whole-table pass over a base whose buffer still holds older changes.
    pub const LOAD_BUFFER_NOT_COMPACTED: Code = refusal(
        "RIVET_LOAD_BUFFER_NOT_COMPACTED",
        "run `rivet compact` first if the buffer belongs to the current base, or drop the buffer, then re-run the load",
    );
    /// A CDC re-baseline snapshot over a change log that already holds changes.
    pub const LOAD_REBASELINE_OVER_CHANGES: Code = refusal(
        "RIVET_LOAD_REBASELINE_OVER_CHANGES",
        "TRUNCATE the change log the message names, then re-run the same `rivet load`",
    );
    /// Another `rivet load` or `rivet compact` holds the table's lease.
    pub const LOAD_TABLE_BUSY: Code = busy(
        "RIVET_LOAD_TABLE_BUSY",
        "wait for the other `rivet load` or `rivet compact` of that table to end, then run again",
    );
    /// A manifest whose row count disagrees with its own parts.
    pub const LOAD_MANIFEST_INCONSISTENT: Code = integrity(
        "RIVET_LOAD_MANIFEST_INCONSISTENT",
        "investigate the run's manifest and parts before loading; re-run the export to replace them",
    );
    /// A run extracted a different number of rows than its source held.
    pub const LOAD_SOURCE_FILE_MISMATCH: Code = integrity(
        "RIVET_LOAD_SOURCE_FILE_MISMATCH",
        "investigate the extract before loading; to accept the drift, set `allow_source_drift: true` in the export's `load:` block",
    );
    /// A warehouse copy or rename that changed the row count.
    pub const LOAD_COPY_COUNT_MISMATCH: Code = integrity(
        "RIVET_LOAD_COPY_COUNT_MISMATCH",
        "compare the tables the message names before re-running; rivet left them for inspection",
    );
    /// The state ledger that decides ownership or what is loaded could not be read.
    pub const LOAD_LEDGER_UNREADABLE: Code = environment(
        "RIVET_LOAD_LEDGER_UNREADABLE",
        "restore the state backend (lower `--pool` if workers could not reconnect), then re-run",
    );
    /// A run finished between typing the load and listing its runs.
    pub const LOAD_RUN_LANDED_DURING_LOAD: Code = environment(
        "RIVET_LOAD_RUN_LANDED_DURING_LOAD",
        "run `rivet load` again: it types from the newest run",
    );
    /// The warehouse credentials could not be obtained or were not accepted.
    pub const LOAD_WAREHOUSE_AUTH_FAILED: Code = environment(
        "RIVET_LOAD_WAREHOUSE_AUTH_FAILED",
        "renew the warehouse credentials (`gcloud auth application-default login`, a service-account key, or the target's connection settings)",
    );
    /// The warehouse refused the load's identity a permission.
    pub const LOAD_WAREHOUSE_ACCESS_DENIED: Code = environment(
        "RIVET_LOAD_WAREHOUSE_ACCESS_DENIED",
        "grant the load's identity the warehouse permission the message names",
    );
    /// The warehouse has no object (dataset, table, job) the request names.
    pub const LOAD_WAREHOUSE_OBJECT_NOT_FOUND: Code = environment(
        "RIVET_LOAD_WAREHOUSE_OBJECT_NOT_FOUND",
        "create the dataset or database the load writes into, or correct the names of the `load:` target",
    );
    /// The warehouse rejected a statement or job.
    pub const LOAD_WAREHOUSE_REJECTED: Code = environment(
        "RIVET_LOAD_WAREHOUSE_REJECTED",
        "read the warehouse's reason in the message and fix what it names, then re-run",
    );
    /// The warehouse kept answering with a transient failure.
    pub const LOAD_WAREHOUSE_UNAVAILABLE: Code = environment(
        "RIVET_LOAD_WAREHOUSE_UNAVAILABLE",
        "re-run the load later; the warehouse answered with a transient failure on every retry",
    );
    /// The warehouse answered in a shape rivet does not read.
    pub const LOAD_WAREHOUSE_ANSWER_UNREADABLE: Code = internal(
        "RIVET_LOAD_WAREHOUSE_ANSWER_UNREADABLE",
        "the warehouse answered in a shape rivet does not read — report it with the message",
    );
    /// A load invariant did not hold.
    pub const INTERNAL_LOAD_INVARIANT: Code = internal(
        "RIVET_INTERNAL_LOAD_INVARIANT",
        "a load invariant did not hold — a bug; report it with the message",
    );
    /// `run --validate` found the destination wrong after the export completed.
    pub const VALIDATE_FAILED: Code = integrity(
        "RIVET_VALIDATE_FAILED",
        "run `rivet validate` on the prefix and settle each failed part before loading it; the export itself completed",
    );
    /// `run --validate` could not read the destination back.
    pub const VALIDATE_UNVERIFIED: Code = environment(
        "RIVET_VALIDATE_UNVERIFIED",
        "restore read access to the destination and run `rivet validate`; the export itself completed",
    );
    pub const INTERNAL_VALUE_CONVERTER: Code = internal(
        "RIVET_INTERNAL_VALUE_CONVERTER",
        "a value changed between the source and the written part — a bug; report it with the column's type",
    );
    pub const INTERNAL_SPILL: Code = internal(
        "RIVET_INTERNAL_SPILL",
        "the CDC spill log is inconsistent — a bug or a damaged spill directory; report it and re-run",
    );
    pub const INTERNAL_TYPE_BUILDER: Code = internal(
        "RIVET_INTERNAL_TYPE_BUILDER",
        "a column builder got a type it cannot build — a bug; report it with the column's type",
    );

    /// A `--export` / `-e` name no export in the config carries.
    pub const CLI_EXPORT_UNKNOWN: Code = usage(
        "RIVET_CLI_EXPORT_UNKNOWN",
        "pass `--export` a name declared under `exports:` in this config",
    );
    /// `--run-id` names a run of another export than `--export`.
    pub const CLI_RUN_ID_EXPORT_MISMATCH: Code = usage(
        "RIVET_CLI_RUN_ID_EXPORT_MISMATCH",
        "pass the `--export` the run id belongs to, or omit `--run-id`",
    );
    /// An option or mode the command's subject cannot take (`--pool` on a plan artifact, `--prefix` without `--export`, `--resume` with nothing to resume, a mode `reconcile` or `repair` has no partitions for).
    pub const CLI_OPTION_UNSUPPORTED: Code = usage(
        "RIVET_CLI_OPTION_UNSUPPORTED",
        "drop the option, or give the command the config, export or mode it takes, as the message names",
    );
    /// The reconcile report passed to `rivet repair` does not parse or belongs to another export.
    pub const CLI_REPORT_INVALID: Code = usage(
        "RIVET_CLI_REPORT_INVALID",
        "pass the report `rivet reconcile --report` wrote for this export",
    );
    /// A plan, report or run artifact file the command writes beside the config could not be written.
    pub const CLI_OUTPUT_UNWRITABLE: Code = environment(
        "RIVET_CLI_OUTPUT_UNWRITABLE",
        "check the output path and its directory's write permission",
    );
    /// A file passed to the command could not be read.
    pub const CLI_INPUT_UNREADABLE: Code = environment(
        "RIVET_CLI_INPUT_UNREADABLE",
        "check the path passed to the command and its read permission",
    );
    /// The OS could not say which binary runs, so no child process could be spawned.
    pub const CLI_EXECUTABLE_UNRESOLVED: Code = environment(
        "RIVET_CLI_EXECUTABLE_UNRESOLVED",
        "run rivet from a binary the OS can resolve: not deleted, unmounted or replaced while it runs",
    );
    /// rivet's own test hook raised the fault `RIVET_TEST_ERROR_AT` names.
    pub const CLI_TEST_FAULT_INJECTED: Code = environment(
        "RIVET_CLI_TEST_FAULT_INJECTED",
        "unset `RIVET_TEST_ERROR_AT`; the fault is rivet's own test hook, not the source",
    );
    /// Every export in the config is a CDC stream or a backfill recipe, which `rivet plan` does not plan.
    pub const PLAN_NOTHING_TO_PLAN: Code = usage(
        "RIVET_PLAN_NOTHING_TO_PLAN",
        "run the CDC streams and backfill recipes with `rivet run`; `rivet plan` plans batch exports only",
    );
    /// A plan artifact older than 24 h, or whose cursor snapshot the state no longer matches.
    pub const PLAN_ARTIFACT_STALE: Code = refusal(
        "RIVET_PLAN_ARTIFACT_STALE",
        "regenerate the plan with `rivet plan`, or pass `--force` to apply it as it is",
    );
    /// A plan artifact made from an inline `url:` config, whose password was never persisted.
    pub const PLAN_ARTIFACT_NO_CREDENTIALS: Code = usage(
        "RIVET_PLAN_ARTIFACT_NO_CREDENTIALS",
        "re-plan from a config that gives the source `url_env:` or `url_file:`",
    );
    /// A plan whose destination still holds `{partition}`: a sealed artifact carries no `partition_by` expansion.
    pub const PLAN_PARTITION_TOKEN_UNEXPANDED: Code = usage(
        "RIVET_PLAN_PARTITION_TOKEN_UNEXPANDED",
        "run the config (`rivet apply <config.yaml>` or `rivet run`), or drop `{partition}` from the destination",
    );
    /// A plan-level compatibility rule rejected the export.
    pub const PLAN_REJECTED: Code = usage(
        "RIVET_PLAN_REJECTED",
        "fix the config so the rule the message names passes; `rivet check` reports the same rules",
    );
    /// Auto-sized range windows over a key so sparse the plan would make thousands of near-empty windows, or more than the window ceiling.
    pub const PLAN_CHUNK_KEY_SPARSE: Code = usage(
        "RIVET_PLAN_CHUNK_KEY_SPARSE",
        "set an explicit `chunk_count:` (or a larger `chunk_size:`), page by `chunk_by_key:`, or use `mode: full`",
    );
    /// A range or date `chunk_column` (or a `--pool --split` key) with NULL values, which every window's predicate would drop.
    pub const PLAN_CHUNK_KEY_NULL: Code = refusal(
        "RIVET_PLAN_CHUNK_KEY_NULL",
        "chunk on a NOT NULL column, drop the NULL rows with `WHERE <column> IS NOT NULL` in the query, or use `mode: full`",
    );
    /// `settle.column` names a column that is not a date or timestamp.
    pub const CONFIG_SETTLE_COLUMN_NOT_TEMPORAL: Code = usage(
        "RIVET_CONFIG_SETTLE_COLUMN_NOT_TEMPORAL",
        "point `settle.column` at a date or timestamp column, or omit it to settle on the cursor",
    );
    /// A `--pool --split` export whose destination holds a run-varying placeholder, so a split resume could not find its prior partition.
    pub const CONFIG_SPLIT_PREFIX_UNSTABLE: Code = usage(
        "RIVET_CONFIG_SPLIT_PREFIX_UNSTABLE",
        "give the split export a destination without run-varying placeholders (`{export}` and `{table}` are stable)",
    );
    /// An export named like a synthesized split unit (`<export>#<n>`).
    pub const CONFIG_EXPORT_NAME_RESERVED: Code = usage(
        "RIVET_CONFIG_EXPORT_NAME_RESERVED",
        "rename the export: `<name>#<n>` names are reserved for split units",
    );
    /// One batch outgrew `max_batch_memory_mb` under `on_batch_memory_exceeded: fail`.
    pub const CONFIG_BATCH_MEMORY_EXCEEDED: Code = usage(
        "RIVET_CONFIG_BATCH_MEMORY_EXCEEDED",
        "lower `batch_size`, or set `on_batch_memory_exceeded: auto_shrink`",
    );
    /// A SQL Server CDC export whose `table:` and `capture_instance:` name different relations.
    pub const CONFIG_CDC_CAPTURE_INSTANCE_MISMATCH: Code = usage(
        "RIVET_CONFIG_CDC_CAPTURE_INSTANCE_MISMATCH",
        "set `table:` to the relation the capture instance emits, or point `capture_instance:` at the table you meant",
    );
    /// A single cell larger than `tuning.max_value_mb`.
    pub const SOURCE_VALUE_TOO_LARGE: Code = usage(
        "RIVET_SOURCE_VALUE_TOO_LARGE",
        "raise `tuning.max_value_mb`, or set it to 0 to disable the guard",
    );
    /// A keyset page ended on the key value it started after: the key's rendering is coarser than its values, so the seek cannot advance.
    pub const SOURCE_KEYSET_KEY_COARSE: Code = refusal(
        "RIVET_SOURCE_KEYSET_KEY_COARSE",
        "page on a unique key rivet reads exactly: an integer, text, uuid or date column, or a timestamp at microsecond precision or coarser (cast it in a curated query)",
    );
    /// `rivet reconcile` found no chunk run recorded for the export.
    pub const STATE_CHUNK_RUN_MISSING: Code = usage(
        "RIVET_STATE_CHUNK_RUN_MISSING",
        "set `chunk_checkpoint: true` and run the export before `rivet reconcile`",
    );
    /// A plain run met an unfinished chunk checkpoint run of its export.
    pub const STATE_CHUNK_RUN_UNFINISHED: Code = refusal(
        "RIVET_STATE_CHUNK_RUN_UNFINISHED",
        "`rivet run … --resume` continues the unfinished chunk run; `rivet state reset-chunks` abandons it",
    );
    /// `rivet repair` found no `manifest.json` at the destination prefix to record the repair parts in.
    pub const DEST_MANIFEST_MISSING: Code = usage(
        "RIVET_DEST_MANIFEST_MISSING",
        "finish the export (a complete run writes `manifest.json` at the prefix) before `rivet repair`",
    );
    /// The run's parts are durable, and the manifest naming them could not be written.
    pub const DEST_MANIFEST_NOT_WRITTEN: Code = environment(
        "RIVET_DEST_MANIFEST_NOT_WRITTEN",
        "restore write access to the destination prefix and run again; until a run completes, no manifest names the parts already there",
    );
    /// A control artifact the destination listed a moment earlier is gone.
    pub const DEST_OBJECT_NOT_FOUND: Code = environment(
        "RIVET_DEST_OBJECT_NOT_FOUND",
        "run again: the object vanished between rivet's two reads of it; if it repeats, check what else writes to the prefix",
    );
    /// A part's local checksum differs from the one the store reported after upload.
    pub const DEST_UPLOAD_CORRUPTED: Code = environment(
        "RIVET_DEST_UPLOAD_CORRUPTED",
        "run again: the part is uploaded afresh; if it repeats, check the network path and any proxy in front of the destination",
    );
    /// A split resume could not list the destination prefix it reconstructs its partition from.
    pub const DEST_PREFIX_UNLISTABLE: Code = environment(
        "RIVET_DEST_PREFIX_UNLISTABLE",
        "restore list access to the destination prefix and retry the resume",
    );
    /// A part a checkpoint committed is gone from the destination, so a manifest naming it would declare absent rows.
    pub const DEST_COMMITTED_PART_GONE: Code = refusal(
        "RIVET_DEST_COMMITTED_PART_GONE",
        "`rivet state reset -c <config> --export <name>` (keyset) or `rivet state reset-chunks` (range chunks) so the range is re-read, or export into a fresh prefix",
    );
    /// A manifest at the prefix that does not parse, or is far larger than one rivet writes.
    pub const DEST_CONTROL_ARTIFACT_INVALID: Code = refusal(
        "RIVET_DEST_CONTROL_ARTIFACT_INVALID",
        "inspect the manifest at the prefix: one rivet wrote parses and is small; restore or remove it, then run again",
    );
    /// Under `validate: true`, a part just written does not hold the rows the sink counted.
    pub const DEST_PART_ROWS_MISMATCH: Code = integrity(
        "RIVET_DEST_PART_ROWS_MISMATCH",
        "stop and report it: a writer bug or a damaged disk; the part was not committed",
    );
    /// One or more units (chunks, keyset ranges, child exports) of the run failed and none carries its own code or class; the message lists each cause.
    pub const RUN_UNITS_FAILED: Code = environment(
        "RIVET_RUN_UNITS_FAILED",
        "fix the first cause the message names and run again; a checkpointed run resumes from where it stopped",
    );
    /// `run --reconcile` could not obtain the source count to compare.
    pub const RUN_RECONCILE_UNVERIFIED: Code = environment(
        "RIVET_RUN_RECONCILE_UNVERIFIED",
        "run again to obtain the reconcile assurance, or drop `--reconcile`; the export completed",
    );
    /// A run invariant did not hold.
    pub const INTERNAL_RUN_INVARIANT: Code = internal(
        "RIVET_INTERNAL_RUN_INVARIANT",
        "a run invariant did not hold — a bug; report it with the message",
    );

    /// A source URL rivet cannot parse into scheme, credentials, host, port and database.
    pub const CONFIG_SOURCE_URL_INVALID: Code = usage(
        "RIVET_CONFIG_SOURCE_URL_INVALID",
        "write the source URL in the form the message shows (`<scheme>://user:password@host:port/<database or service>`)",
    );
    /// A setting the configured source type does not support (an Oracle `tls.ca_file`, a MongoDB `query:`).
    pub const CONFIG_SOURCE_OPTION_UNSUPPORTED: Code = usage(
        "RIVET_CONFIG_SOURCE_OPTION_UNSUPPORTED",
        "remove the setting the message names: this source type does not support it",
    );
    /// A `source.type` this binary was built without.
    pub const CONFIG_SOURCE_FEATURE_MISSING: Code = usage(
        "RIVET_CONFIG_SOURCE_FEATURE_MISSING",
        "run a rivet built with the feature the message names (the release binaries include it)",
    );
    /// A CDC `table:` that does not name exactly one base table the engine's log can route changes to.
    pub const SOURCE_CDC_TABLE_UNRESOLVED: Code = usage(
        "RIVET_SOURCE_CDC_TABLE_UNRESOLVED",
        "set `table:` to the one base table the capture follows, qualified `schema.table` as the catalog spells it, as the message explains",
    );
    /// A table the CDC reader cannot decode as a whole (Oracle: a column type, name length or width LogMiner does not mine).
    pub const SOURCE_CDC_TABLE_UNSUPPORTED: Code = usage(
        "RIVET_SOURCE_CDC_TABLE_UNSUPPORTED",
        "export the table in batch (`mode: full`), or capture a table the message's supported list covers",
    );
    /// One source transaction larger than the in-memory buffer cap.
    pub const SOURCE_CDC_TRANSACTION_TOO_LARGE: Code = usage(
        "RIVET_SOURCE_CDC_TRANSACTION_TOO_LARGE",
        "split the source transaction, or raise RIVET_CDC_MAX_TX_ROWS / RIVET_CDC_MAX_TX_BYTES when a transaction this large is expected",
    );
    /// A query column that must be re-read through a conversion but whose name an outer query cannot reference.
    pub const SOURCE_QUERY_COLUMN_UNREFERENCEABLE: Code = usage(
        "RIVET_SOURCE_QUERY_COLUMN_UNREFERENCEABLE",
        "give the column an alias in the export's `query:` (e.g. `... AS row_id`)",
    );
    /// The async runtime a driver runs on could not be built: the process is out of descriptors or threads.
    pub const SOURCE_RUNTIME_UNAVAILABLE: Code = environment(
        "RIVET_SOURCE_RUNTIME_UNAVAILABLE",
        "the process could not start the async runtime its driver needs (out of file descriptors or threads): raise the open-files limit (`ulimit -n`) or lower parallelism, then rerun",
    );
    /// The connection to the source could not be opened: no route, a refused login, a failed handshake.
    pub const SOURCE_CONNECT_FAILED: Code = environment(
        "RIVET_SOURCE_CONNECT_FAILED",
        "check the host, port, credentials and TLS settings in the source URL; the driver's own words follow the colon",
    );
    /// A statement the source refused or a session that broke under it; the server's words are kept.
    pub const SOURCE_QUERY_FAILED: Code = environment(
        "RIVET_SOURCE_QUERY_FAILED",
        "fix what the server's words name (the statement, the login's rights on the object, or the session), then run again",
    );
    /// The spill directory's filesystem refused `flock`, which the spill sweep relies on.
    pub const SOURCE_CDC_SPILL_UNLOCKABLE: Code = environment(
        "RIVET_SOURCE_CDC_SPILL_UNLOCKABLE",
        "point RIVET_CDC_SPILL_DIR at a local, flock-capable filesystem, or raise the rollover memory cap so the transaction stays in memory",
    );
    /// A spill log could not be flushed to disk.
    pub const SOURCE_CDC_SPILL_UNWRITABLE: Code = environment(
        "RIVET_SOURCE_CDC_SPILL_UNWRITABLE",
        "free space on the spill directory's filesystem (RIVET_CDC_SPILL_DIR, else beside the checkpoint) and check its write permission, then re-run",
    );
    /// A source reader invariant did not hold: a driver answer, a log message or a position in a shape rivet never produces.
    pub const INTERNAL_SOURCE_INVARIANT: Code = internal(
        "RIVET_INTERNAL_SOURCE_INVARIANT",
        "a source reader invariant did not hold — a bug; report it with the message",
    );
    /// A MongoDB `_id` set a type-bracketed `$gt` seek cannot page over (NaN, or more than one BSON type).
    pub const SOURCE_KEYSET_KEY_UNORDERED: Code = refusal(
        "RIVET_SOURCE_KEYSET_KEY_UNORDERED",
        "remove `page_size`, `resume` and `parallel` so the collection is read in one ordered scan, or fix the `_id` values the message names",
    );

    /// An option given a value in a form it does not take (`--format`, `--mode`, `--date`, `--param`).
    pub const CLI_VALUE_INVALID: Code = usage(
        "RIVET_CLI_VALUE_INVALID",
        "pass the option a value in the form the message names",
    );
    /// A `--run-id` no run-status row carries.
    pub const CLI_RUN_ID_UNKNOWN: Code = usage(
        "RIVET_CLI_RUN_ID_UNKNOWN",
        "pass a run id `rivet state runs -c <config>` lists",
    );
    /// `rivet check` or `rivet doctor` ended with a failed check; each is printed above the stop.
    pub const CLI_PREFLIGHT_FAILED: Code = usage(
        "RIVET_CLI_PREFLIGHT_FAILED",
        "fix each check the output above names, then run the command again",
    );
    /// `rivet doctor` met a transient fault on a probe, so the verdict is not a verdict on the config.
    pub const CLI_PREFLIGHT_UNVERIFIED: Code = environment(
        "RIVET_CLI_PREFLIGHT_UNVERIFIED",
        "run `rivet doctor` again once the source or destination the output names answers",
    );
    /// A `target:` (or `--target`) naming no warehouse rivet types for.
    pub const CONFIG_TARGET_UNKNOWN: Code = usage(
        "RIVET_CONFIG_TARGET_UNKNOWN",
        "set `target:` (or `--target`) to one of the names the message lists",
    );
    /// `tls.ca_file` cannot be read or holds no PEM certificate.
    pub const CONFIG_TLS_CA_INVALID: Code = usage(
        "RIVET_CONFIG_TLS_CA_INVALID",
        "point `tls.ca_file` at a readable PEM file holding the CA certificate",
    );
    /// A column of a type the export's file format cannot write (CSV: a nested type; Parquet: a negative decimal scale).
    pub const CONFIG_FORMAT_COLUMN_UNSUPPORTED: Code = usage(
        "RIVET_CONFIG_FORMAT_COLUMN_UNSUPPORTED",
        "change `format:`, or cast or drop the column in the export's query",
    );
    /// A source column named like a column `meta_columns` adds.
    pub const CONFIG_META_COLUMN_COLLISION: Code = usage(
        "RIVET_CONFIG_META_COLUMN_COLLISION",
        "alias the source column in a `query:`, or turn the colliding meta column off",
    );
    /// A `row_hash` or `quality.unique_columns` column of a type rivet has no canonical form for.
    pub const CONFIG_CANON_TYPE_UNSUPPORTED: Code = usage(
        "RIVET_CONFIG_CANON_TYPE_UNSUPPORTED",
        "leave the column out of the gate the message names, or cast it in the query",
    );
    /// A `quality:` check naming a column the export does not project.
    pub const CONFIG_QUALITY_COLUMN_UNKNOWN: Code = usage(
        "RIVET_CONFIG_QUALITY_COLUMN_UNKNOWN",
        "name columns the export projects under `quality:`, or add them to the query",
    );
    /// A `columns:` override type rivet does not parse, or a decimal precision or scale out of range.
    pub const CONFIG_COLUMN_OVERRIDE_TYPE_INVALID: Code = usage(
        "RIVET_CONFIG_COLUMN_OVERRIDE_TYPE_INVALID",
        "give the `columns:` override one of the type names the message lists, with `decimal(precision,scale)` in range",
    );
    /// A cloud destination lacking a key its type requires (`bucket`).
    pub const CONFIG_DESTINATION_SETTING_MISSING: Code = usage(
        "RIVET_CONFIG_DESTINATION_SETTING_MISSING",
        "set the destination key the message names",
    );
    /// A destination credential that is malformed, expired or refused by the provider's token endpoint.
    pub const DEST_CREDENTIALS_INVALID: Code = environment(
        "RIVET_DEST_CREDENTIALS_INVALID",
        "issue the destination a fresh, complete credential (a new SAS, service-account key or `gcloud auth application-default login`)",
    );
    /// The destination prefix holds a manifest of another export mode (batch beside CDC).
    pub const DEST_PREFIX_MODE_MISMATCH: Code = refusal(
        "RIVET_DEST_PREFIX_MODE_MISMATCH",
        "give the export its own destination prefix; a batch and a CDC export must not share one",
    );
    /// The object-store I/O runtime could not start: the OS refused a thread or a file descriptor.
    pub const DEST_IO_RUNTIME_UNAVAILABLE: Code = environment(
        "RIVET_DEST_IO_RUNTIME_UNAVAILABLE",
        "raise the process's thread and open-file limits, or lower the export parallelism, then run again",
    );
    /// `rivet init -o` met a config at the output path.
    pub const INIT_CONFIG_EXISTS: Code = refusal(
        "RIVET_INIT_CONFIG_EXISTS",
        "move the existing config aside, or write elsewhere with `-o <new-path>`",
    );
    /// `rivet init --table` found no columns for the table.
    pub const INIT_TABLE_NOT_FOUND: Code = usage(
        "RIVET_INIT_TABLE_NOT_FOUND",
        "correct `--table` (and `--schema`), or grant the user SELECT on the table",
    );
    /// `rivet init` found no table or view to scaffold.
    pub const INIT_NO_TABLES: Code = usage(
        "RIVET_INIT_NO_TABLES",
        "point `--schema` at a schema the user can read, and loosen `--include` / `--exclude`",
    );
    /// A Full load over a `--pool --split` prefix whose parts span more than one split generation.
    pub const LOAD_SPLIT_GENERATIONS_MIXED: Code = refusal(
        "RIVET_LOAD_SPLIT_GENERATIONS_MIXED",
        "load from a fresh destination prefix, or clear the stale split generation, and run again",
    );
    /// A `rivet load` export with no column types recorded in the state DB.
    pub const LOAD_SPEC_MISSING: Code = refusal(
        "RIVET_LOAD_SPEC_MISSING",
        "run `rivet run` for the export the message names so its column types are recorded, then load again",
    );
    /// A JSON-RPC request the MCP server does not understand: an unknown method or tool, or missing params.
    pub const MCP_REQUEST_INVALID: Code = usage(
        "RIVET_MCP_REQUEST_INVALID",
        "send a method and tool name `tools/list` advertises, with `params`",
    );
    /// A plan file that is not one `rivet plan` wrote, or was edited after planning.
    pub const PLAN_ARTIFACT_INVALID: Code = usage(
        "RIVET_PLAN_ARTIFACT_INVALID",
        "pass a plan file `rivet plan` wrote, unedited; run `rivet plan` again to regenerate it",
    );
    /// A chunked export on a table with no key the planner can range-chunk or keyset-page.
    pub const PLAN_CHUNK_KEY_UNUSABLE: Code = usage(
        "RIVET_PLAN_CHUNK_KEY_UNUSABLE",
        "set `chunk_column:` (integer) or `chunk_by_key:` (a single-column NOT NULL unique key of a supported type), add such an index, or use `mode: full`",
    );
    /// The state database did not take a statement: a connection, a migration step or a claim failed.
    pub const STATE_BACKEND_FAILED: Code = environment(
        "RIVET_STATE_BACKEND_FAILED",
        "check the state database (the SQLite file, or the `RIVET_STATE_URL` server and the role's rights), then run again",
    );
    /// The SQLite state file is not a database.
    pub const STATE_FILE_CORRUPT: Code = environment(
        "RIVET_STATE_FILE_CORRUPT",
        "move the state file aside (rivet rebuilds it; cursors and skip-ledgers start fresh), or restore it from a backup",
    );
    /// Another rivet process holds the state migration lock.
    pub const STATE_MIGRATION_LOCKED: Code = environment(
        "RIVET_STATE_MIGRATION_LOCKED",
        "wait for the other rivet process migrating this state database to finish, then run again",
    );
    /// A destination or file-writer invariant did not hold.
    pub const INTERNAL_DEST_INVARIANT: Code = internal(
        "RIVET_INTERNAL_DEST_INVARIANT",
        "a destination invariant did not hold — a bug; report it with the message",
    );
    /// A state-store invariant did not hold.
    pub const INTERNAL_STATE_INVARIANT: Code = internal(
        "RIVET_INTERNAL_STATE_INVARIANT",
        "a state-store invariant did not hold — a bug; report it with the message",
    );

    /// Every registered code: the docs page and the guard tests read this one list.
    pub const ALL: &[Code] = &[
        CONFIG_NO_EXPORTS,
        CONFIG_CHUNK_COUNT_INVALID,
        CONFIG_CHUNK_BY_DAYS_INVALID,
        CONFIG_CHUNK_COLUMN_NOT_INTEGER,
        CONFIG_DUPLICATE_EXPORT,
        CONFIG_CDC_RESOURCE_CONFLICT,
        CONFIG_CDC_ROLLOVER_INVALID,
        CONFIG_CDC_CONTINUOUS_UNSUPPORTED,
        CONFIG_CSV_LOAD_UNSUPPORTED,
        CONFIG_LOAD_PARTITION_UNSUPPORTED,
        CONFIG_SOURCE_MODE_UNSUPPORTED,
        CONFIG_SOURCE_URL_SCHEME_MISMATCH,
        CONFIG_KEYSET_KEY_UUID_OVERRIDE,
        CONFIG_CURSOR_COLUMN_CASE,
        CONFIG_COLUMN_OVERRIDE_CASE,
        CONFIG_FILE_UNREADABLE,
        CONFIG_YAML_INVALID,
        CONFIG_SOURCE_CONNECTION_INVALID,
        CONFIG_SECRET_UNSET,
        CONFIG_EXPORT_SOURCE_INVALID,
        CONFIG_MODE_SETTING_INVALID,
        CONFIG_DESTINATION_AUTH_INVALID,
        CONFIG_DESTINATION_ENDPOINT_UNSAFE,
        CONFIG_VALUE_OUT_OF_RANGE,
        CONFIG_CDC_SHAPE_INVALID,
        CONFIG_CSV_COMPRESSION_UNSUPPORTED,
        CONFIG_TLS_VERIFICATION_DOWNGRADE,
        CONFIG_LOAD_BLOCK_INVALID,
        CONFIG_PATH_UNSAFE,
        CONFIG_ROW_HASH_INVALID,
        CONFIG_COLUMN_OVERRIDE_TABLE_UNKNOWN,
        TYPE_UNSAFE_MAPPING,
        SOURCE_STATEMENT_TIMEOUT,
        SOURCE_CURSOR_FINER_THAN_MICROSECOND,
        SOURCE_CDC_FOREIGN_CHECKPOINT,
        SOURCE_CDC_CHECKPOINT_INVALID,
        SOURCE_CDC_LOG_GAP,
        SOURCE_CDC_TRUNCATED,
        SOURCE_CDC_UNDECODABLE,
        SOURCE_CDC_CELL_UNSUPPORTED,
        SOURCE_CDC_PREREQUISITE,
        SOURCE_VALUE_UNREPRESENTABLE,
        SOURCE_PROBE_UNREADABLE,
        SOURCE_COLUMNS_CHANGED_UNDER_READ,
        SOURCE_OVERRIDE_WIRE_MISMATCH,
        STATE_URL_SCHEME_UNSUPPORTED,
        STATE_NOT_WRITABLE,
        STATE_CURSOR_NOT_STORED,
        STATE_SCHEMA_NEWER,
        STATE_CURSOR_OWNER_MISMATCH,
        STATE_CURSOR_INCOMPARABLE,
        STATE_CURSOR_PAST_MANIFEST,
        STATE_CURSOR_STREAM_MISMATCH,
        STATE_CURSOR_DESTINATION_MISMATCH,
        STATE_INTERRUPTED_RUN_OWNER_MISMATCH,
        STATE_CDC_TABLE_REJOINED,
        STATE_CHUNK_RUN_OWNER_UNKNOWN,
        STATE_CHUNK_RUN_SUPERSEDED,
        STATE_KEYSET_SEQUENTIAL_ANCHOR_UNFINISHED,
        STATE_RUN_IN_PROGRESS,
        STATE_CHUNK_CHECKPOINT_GONE,
        STATE_CHUNK_PLAN_CHANGED,
        STATE_CHUNK_WINDOWS_BROKEN,
        STATE_LEASE_KEEPER_UNAVAILABLE,
        STATE_LEASE_LOST,
        PLAN_SOURCE_UNREADABLE,
        SOURCE_COLLECTION_NOT_FOUND,
        DEST_CONTAINER_NOT_FOUND,
        DEST_ALREADY_COMPLETE,
        LOAD_VALUE_OUT_OF_TARGET_RANGE,
        LOAD_COUNT_MISMATCH,
        LOAD_LEASE_LOST_DURING_WRITE,
        LOAD_ADOPTION_COLUMN_MISMATCH,
        LOAD_TARGET_NOT_RIVETS,
        LOAD_IDENTIFIER_UNSAFE,
        LOAD_KEY_COLUMN_INVALID,
        LOAD_PRIMARY_KEY_MISSING,
        LOAD_RESERVED_COLUMN,
        LOAD_LOOKALIKE_COLUMN,
        LOAD_TARGET_COLLISION,
        LOAD_STAGING_PREFIX_INVALID,
        LOAD_DESTINATION_TYPE_UNSUPPORTED,
        LOAD_PARTITION_BUDGET_EXCEEDED,
        LOAD_FILE_LIMIT_EXCEEDED,
        LOAD_COLUMN_TYPE_UNSUPPORTED,
        LOAD_TLS_CA_INVALID,
        LOAD_EMPTY_RUN_UNSUPPORTED,
        LOAD_RUN_NOT_LOADABLE,
        LOAD_STAGING_OBJECT_UNSAFE,
        LOAD_SOURCES_SHARED,
        LOAD_COLUMN_RESPELLED,
        LOAD_TARGET_OBJECT_CONFLICT,
        LOAD_COMPACT_BASE_MISSING,
        LOAD_CHANGELOG_PARTITION_CHANGED,
        LOAD_BUFFER_NOT_COMPACTED,
        LOAD_REBASELINE_OVER_CHANGES,
        LOAD_TABLE_BUSY,
        LOAD_MANIFEST_INCONSISTENT,
        LOAD_SOURCE_FILE_MISMATCH,
        LOAD_COPY_COUNT_MISMATCH,
        LOAD_LEDGER_UNREADABLE,
        LOAD_RUN_LANDED_DURING_LOAD,
        LOAD_WAREHOUSE_AUTH_FAILED,
        LOAD_WAREHOUSE_ACCESS_DENIED,
        LOAD_WAREHOUSE_OBJECT_NOT_FOUND,
        LOAD_WAREHOUSE_REJECTED,
        LOAD_WAREHOUSE_UNAVAILABLE,
        LOAD_WAREHOUSE_ANSWER_UNREADABLE,
        INTERNAL_LOAD_INVARIANT,
        VALIDATE_FAILED,
        VALIDATE_UNVERIFIED,
        INTERNAL_VALUE_CONVERTER,
        INTERNAL_SPILL,
        INTERNAL_TYPE_BUILDER,
        CLI_EXPORT_UNKNOWN,
        CLI_RUN_ID_EXPORT_MISMATCH,
        CLI_OPTION_UNSUPPORTED,
        CLI_REPORT_INVALID,
        CLI_OUTPUT_UNWRITABLE,
        CLI_INPUT_UNREADABLE,
        CLI_EXECUTABLE_UNRESOLVED,
        CLI_TEST_FAULT_INJECTED,
        PLAN_NOTHING_TO_PLAN,
        PLAN_ARTIFACT_STALE,
        PLAN_ARTIFACT_NO_CREDENTIALS,
        PLAN_PARTITION_TOKEN_UNEXPANDED,
        PLAN_REJECTED,
        PLAN_CHUNK_KEY_SPARSE,
        PLAN_CHUNK_KEY_NULL,
        CONFIG_SETTLE_COLUMN_NOT_TEMPORAL,
        CONFIG_SPLIT_PREFIX_UNSTABLE,
        CONFIG_EXPORT_NAME_RESERVED,
        CONFIG_BATCH_MEMORY_EXCEEDED,
        CONFIG_CDC_CAPTURE_INSTANCE_MISMATCH,
        SOURCE_VALUE_TOO_LARGE,
        SOURCE_KEYSET_KEY_COARSE,
        STATE_CHUNK_RUN_MISSING,
        STATE_CHUNK_RUN_UNFINISHED,
        DEST_MANIFEST_MISSING,
        DEST_MANIFEST_NOT_WRITTEN,
        DEST_OBJECT_NOT_FOUND,
        DEST_UPLOAD_CORRUPTED,
        DEST_PREFIX_UNLISTABLE,
        DEST_COMMITTED_PART_GONE,
        DEST_CONTROL_ARTIFACT_INVALID,
        DEST_PART_ROWS_MISMATCH,
        RUN_UNITS_FAILED,
        RUN_RECONCILE_UNVERIFIED,
        INTERNAL_RUN_INVARIANT,
        CONFIG_SOURCE_URL_INVALID,
        CONFIG_SOURCE_OPTION_UNSUPPORTED,
        CONFIG_SOURCE_FEATURE_MISSING,
        SOURCE_CDC_TABLE_UNRESOLVED,
        SOURCE_CDC_TABLE_UNSUPPORTED,
        SOURCE_CDC_TRANSACTION_TOO_LARGE,
        SOURCE_QUERY_COLUMN_UNREFERENCEABLE,
        SOURCE_RUNTIME_UNAVAILABLE,
        SOURCE_CONNECT_FAILED,
        SOURCE_QUERY_FAILED,
        SOURCE_CDC_SPILL_UNLOCKABLE,
        SOURCE_CDC_SPILL_UNWRITABLE,
        INTERNAL_SOURCE_INVARIANT,
        SOURCE_KEYSET_KEY_UNORDERED,
        CLI_VALUE_INVALID,
        CLI_RUN_ID_UNKNOWN,
        CLI_PREFLIGHT_FAILED,
        CLI_PREFLIGHT_UNVERIFIED,
        CONFIG_TARGET_UNKNOWN,
        CONFIG_TLS_CA_INVALID,
        CONFIG_FORMAT_COLUMN_UNSUPPORTED,
        CONFIG_META_COLUMN_COLLISION,
        CONFIG_CANON_TYPE_UNSUPPORTED,
        CONFIG_QUALITY_COLUMN_UNKNOWN,
        CONFIG_COLUMN_OVERRIDE_TYPE_INVALID,
        CONFIG_DESTINATION_SETTING_MISSING,
        DEST_CREDENTIALS_INVALID,
        DEST_PREFIX_MODE_MISMATCH,
        DEST_IO_RUNTIME_UNAVAILABLE,
        INIT_CONFIG_EXISTS,
        INIT_TABLE_NOT_FOUND,
        INIT_NO_TABLES,
        LOAD_SPLIT_GENERATIONS_MIXED,
        LOAD_SPEC_MISSING,
        MCP_REQUEST_INVALID,
        PLAN_ARTIFACT_INVALID,
        PLAN_CHUNK_KEY_UNUSABLE,
        STATE_BACKEND_FAILED,
        STATE_FILE_CORRUPT,
        STATE_MIGRATION_LOCKED,
        INTERNAL_DEST_INVARIANT,
        INTERNAL_STATE_INVARIANT,
    ];
}

/// `return Err` with a registered code: drop-in for `anyhow::bail!`, the message unchanged,
/// the code (and so the exit class, via its kind) riding alongside it.
#[macro_export]
macro_rules! rivet_bail {
    ($code:expr, $($arg:tt)*) => {
        return ::core::result::Result::Err(::anyhow::Error::new(
            $crate::error::CodedError::new($code, format!($($arg)*))))
    };
}

/// A config-validation [`rivet_bail!`] (kept for the existing call sites).
#[macro_export]
macro_rules! config_bail {
    ($code:expr, $($arg:tt)*) => {
        return ::core::result::Result::Err(::anyhow::Error::new(
            $crate::error::CodedError::new($code, format!($($arg)*))))
    };
}

/// The exit code a code of `kind` produces (an environment failure: 2 when transient, else 1).
fn kind_exit(kind: ErrorKind) -> &'static str {
    match kind {
        ErrorKind::Usage => "1",
        ErrorKind::Environment => "2 if transient, else 1",
        ErrorKind::Refusal => "5",
        ErrorKind::Busy => "2",
        ErrorKind::Integrity => "3",
        ErrorKind::Internal => "6",
    }
}

/// The Markdown error-code reference, generated from [`codes::ALL`].
pub fn codes_markdown() -> String {
    let mut out = String::from(
        "# Error codes\n\n<!-- Generated by `rivet schema errors` from the code registry in \
         src/error.rs. Do not edit. -->\n\nEvery failure rivet names carries a stable \
         `RIVET_<FAMILY>_<NAME>` code: in `--json-errors` output as `code`, and as a `[CODE]` \
         prefix on the text error line. The KIND decides the exit code.\n\n\
         | exit | meaning |\n|---|---|\n\
         | 1 | usage — fix the config or the command; retrying fails the same way |\n\
         | 2 | a transient failure, or busy (another live rivet process holds the lease or lock \
         this command needs) — retry the same command |\n\
         | 3 | integrity — the data may be wrong; stop and investigate |\n\
         | 4 | schema drift — the source shape changed; review before re-running |\n\
         | 5 | refusal — rivet stopped on purpose to protect data; a human decides |\n\
         | 6 | internal — an invariant did not hold; a bug, please report it |\n\n\
         | code | kind | exit | what to do |\n|---|---|---|---|\n",
    );
    for c in codes::ALL {
        out.push_str(&format!(
            "| `{}` | {} | {} | {} |\n",
            c.id,
            c.kind.name(),
            kind_exit(c.kind),
            c.action.replace('|', "\\|")
        ));
    }
    out
}

pub type Result<T> = anyhow::Result<T>;

/// Test helper: `err` carries `code` and classifies to `exit`.
#[cfg(test)]
pub(crate) fn assert_coded(err: &anyhow::Error, code: &str, exit: i32) {
    assert_eq!(
        (error_code(err), classify_exit(err)),
        (Some(code), exit),
        "{err:#}"
    );
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_preclassified_exit_reads_as_its_class() {
        assert_eq!(super::PreclassifiedExit(5).to_string(), "exit class 5");
    }

    /// The cli.md exit-code table lists exactly 0 plus every code an [`ExitClass`] maps to.
    #[test]
    fn the_cli_exit_code_table_lists_every_exit_class() {
        let doc = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("docs/reference/cli.md"),
        )
        .unwrap();
        let table = doc.split_once("## Exit codes").unwrap().1;
        let documented: std::collections::BTreeSet<i32> = table
            .lines()
            .skip_while(|l| !l.starts_with('|'))
            .take_while(|l| l.starts_with('|'))
            .filter_map(|l| l.trim_matches('|').split('|').next()?.trim().parse().ok())
            .collect();
        let expected: std::collections::BTreeSet<i32> = (0..=255)
            .filter(|&c| c == 0 || super::ExitClass::from_code(c).is_some())
            .collect();
        assert_eq!(documented, expected);
    }

    use super::*;

    #[test]
    fn every_exit_class_round_trips_through_its_code_and_ranks_distinctly() {
        let all = [
            ExitClass::Generic,
            ExitClass::Retryable,
            ExitClass::DataIntegrity,
            ExitClass::SchemaDrift,
            ExitClass::Refusal,
            ExitClass::Internal,
        ];
        for c in all {
            assert_eq!(ExitClass::from_code(c.code()), Some(c));
        }
        assert_eq!(ExitClass::from_code(0), None);
        assert_eq!(ExitClass::from_code(143), None);
        let ascending = [
            ExitClass::Generic,
            ExitClass::Retryable,
            ExitClass::SchemaDrift,
            ExitClass::Refusal,
            ExitClass::Internal,
            ExitClass::DataIntegrity,
        ];
        for w in ascending.windows(2) {
            assert!(
                w[0].stop_rank() < w[1].stop_rank(),
                "{:?} !< {:?}",
                w[0],
                w[1]
            );
        }
    }

    #[test]
    fn the_codes_table_renders_one_row_per_kind_with_its_exit() {
        let md = codes_markdown();
        for row in [
            "| `RIVET_CONFIG_NO_EXPORTS` | usage | 1 | declare at least one export under `exports:` |",
            "| `RIVET_SOURCE_STATEMENT_TIMEOUT` | environment | 2 if transient, else 1 | raise `tuning.statement_timeout_s`, or narrow the chunk |",
            "| `RIVET_STATE_SCHEMA_NEWER` | refusal | 5 | upgrade rivet, or point this binary at a state DB it created |",
            "| `RIVET_LOAD_COUNT_MISMATCH` | integrity | 3 | compare the warehouse table with the run's manifest before re-running; the source is kept |",
            "| `RIVET_INTERNAL_SPILL` | internal | 6 | the CDC spill log is inconsistent — a bug or a damaged spill directory; report it and re-run |",
        ] {
            assert!(md.contains(row), "missing row: {row}\n{md}");
        }
    }

    #[test]
    fn the_committed_errors_reference_matches_the_registry() {
        let committed = include_str!("../docs/reference/errors.md");
        assert_eq!(
            committed.trim_end(),
            codes_markdown().trim_end(),
            "docs/reference/errors.md is stale: regenerate it with `rivet schema errors`"
        );
    }

    #[test]
    fn schema_drift_marker_classifies_to_4() {
        let err: anyhow::Error = SchemaDriftError::new("schema changed").into();
        assert_eq!(classify_exit(&err), 4);
        assert_eq!(ExitClass::SchemaDrift.code(), 4);
    }

    #[test]
    fn data_integrity_marker_classifies_to_3() {
        let err: anyhow::Error = DataIntegrityError::new("reconcile mismatch").into();
        assert_eq!(classify_exit(&err), 3);
        assert_eq!(ExitClass::DataIntegrity.code(), 3);
    }

    #[test]
    fn manifest_inconsistency_classifies_to_3() {
        let err: anyhow::Error = crate::manifest::ManifestInconsistency::DuplicatePartId(1).into();
        assert_eq!(
            classify_exit(&err),
            3,
            "manifest self-consistency failure is a data-integrity stop"
        );
    }

    #[test]
    fn transient_error_classifies_to_2_syntax_error_to_1() {
        // Transient (string fallback in retry::classify_error) → retryable.
        let transient = anyhow::anyhow!("connection reset by peer");
        assert_eq!(
            classify_exit(&transient),
            2,
            "connection reset is retryable"
        );

        // Permanent / generic → 1.
        let syntax = anyhow::anyhow!("syntax error at or near \"SELET\"");
        assert_eq!(classify_exit(&syntax), 1, "a syntax error is not retryable");
    }

    #[test]
    fn typed_markers_survive_anyhow_context_wrapping() {
        // The downcast walks the chain, so a context-wrapped marker still
        // classifies by type (the `apply` path wraps with context on the way up).
        let drift: anyhow::Error = SchemaDriftError::new("drift").into();
        let wrapped = drift.context("export 'orders' failed");
        assert_eq!(classify_exit(&wrapped), 4);

        let dup: anyhow::Error = DataIntegrityError::new("dup").into();
        let wrapped = dup.context("export 'orders' failed");
        assert_eq!(classify_exit(&wrapped), 3);
    }

    #[test]
    fn run_carries_typed_marker_through_multi_failure_context() {
        // `pipeline::run`'s multi-failure path returns the representative typed
        // failure wrapped in a context string listing the others. The marker
        // must still downcast through that context so the exit class is right.
        let dup: anyhow::Error =
            DataIntegrityError::new("export 'orders': cannot safely retry (would duplicate rows)")
                .into();
        let aggregated = dup.context("2 export(s) failed; representative error follows (also: export 'events': connection reset)");
        assert_eq!(
            classify_exit(&aggregated),
            3,
            "the carried data-integrity marker must survive run's multi-failure context wrapping"
        );
    }

    #[test]
    fn untyped_flattened_string_is_generic_not_string_matched() {
        // Deliberate behavior change: classification is type-driven only. A bare
        // string that merely *reads* like a quality-gate failure (no marker) is
        // Generic — a visible signal a marker was dropped, not a silent rescue.
        let bare = anyhow::anyhow!("export 'orders': 1 quality check(s) failed: row_count low");
        assert_eq!(
            classify_exit(&bare),
            1,
            "an un-typed string must NOT be string-matched into data-integrity"
        );
    }

    #[test]
    fn data_integrity_marker_display_is_verbatim() {
        // The marker must reproduce the wrapped message byte-for-byte so the
        // operator-facing error line is unchanged from before the type existed.
        let msg = "export 'orders': 1 quality check(s) failed";
        assert_eq!(format!("{}", DataIntegrityError::new(msg)), msg);
        assert_eq!(format!("{}", SchemaDriftError::new(msg)), msg);
    }

    /// The families a code may name (`RIVET_<FAMILY>_<NAME>`).
    const FAMILIES: &[&str] = &[
        "CONFIG", "CLI", "SOURCE", "CDC", "SPILL", "TYPE", "STATE", "DEST", "PLAN", "LOAD",
        "VALIDATE", "INIT", "INTERNAL", "RUN", "MCP",
    ];

    #[test]
    fn every_registered_code_is_distinct_named_by_family_and_actionable() {
        let mut seen = std::collections::HashSet::new();
        for c in codes::ALL {
            assert!(seen.insert(c.id), "duplicate code: {}", c.id);
            let family =
                c.id.strip_prefix("RIVET_")
                    .and_then(|r| r.split('_').next());
            assert!(
                family.is_some_and(|f| FAMILIES.contains(&f)),
                "{} is not RIVET_<FAMILY>_<NAME> with a known family",
                c.id
            );
            assert!(!c.action.trim().is_empty(), "{} names no action", c.id);
        }
    }

    #[test]
    fn every_code_constant_is_in_the_registry() {
        // Derived from the source, not a second list: a constant left out of ALL has no docs
        // row and no guard (three of nine were, before the registry).
        let src = include_str!("error.rs");
        let consts = src.matches("pub const ").count() - src.matches("pub const ALL").count();
        let module = &src[src.find("pub mod codes").unwrap()..];
        let declared = module
            .lines()
            .filter(|l| l.trim_start().starts_with("pub const ") && l.contains(": Code"))
            .count();
        assert_eq!(
            declared,
            codes::ALL.len(),
            "a `Code` constant is missing from codes::ALL"
        );
        assert!(consts >= declared);
    }

    #[test]
    fn a_code_s_kind_decides_its_exit_class() {
        let with = |kind| {
            let c = Code {
                id: "RIVET_STATE_TEST",
                kind,
                action: "x",
            };
            classify_exit(&anyhow::Error::new(CodedError::new(c, "permission denied")))
        };
        assert_eq!(with(ErrorKind::Refusal), 5);
        assert_eq!(with(ErrorKind::Busy), 2, "busy, whatever its text says");
        assert_eq!(with(ErrorKind::Internal), 6);
        assert_eq!(with(ErrorKind::Integrity), 3);
        assert_eq!(with(ErrorKind::Usage), 1);
        assert_eq!(
            with(ErrorKind::Environment),
            1,
            "a non-transient environment failure"
        );
    }

    /// A stop held by another live rivet process exits retryable (2) with its code, bare and through a load's pre-write seam; no refusal's remedy is to wait.
    #[test]
    fn a_stop_held_by_a_live_holder_exits_retryable_with_its_code() {
        let busy: Vec<Code> = codes::ALL
            .iter()
            .copied()
            .filter(|c| c.kind == ErrorKind::Busy)
            .collect();
        assert_eq!(
            busy.iter().map(|c| c.id).collect::<Vec<_>>(),
            [
                "RIVET_STATE_RUN_IN_PROGRESS",
                "RIVET_STATE_LEASE_LOST",
                "RIVET_LOAD_TABLE_BUSY"
            ]
        );
        let waits_refused: Vec<&str> = codes::ALL
            .iter()
            .filter(|c| c.kind == ErrorKind::Refusal && c.action.starts_with("wait for"))
            .map(|c| c.id)
            .collect();
        assert_eq!(
            waits_refused,
            Vec::<&str>::new(),
            "a refusal whose remedy is to wait"
        );
        for code in busy {
            let bare = || anyhow::Error::new(CodedError::new(code, "held"));
            assert_coded(&bare().context("export 'orders'"), code.id, 2);
            let stop = crate::load::before_write::<()>(Err(bare())).unwrap_err();
            assert_coded(&stop, code.id, 2);
        }
    }

    /// A stop before the warehouse write exits as what stopped it, and names its code.
    #[test]
    fn a_stop_before_the_write_keeps_the_code_and_the_exit_class_of_its_cause() {
        type Cause = fn() -> anyhow::Error;
        let causes: Vec<(Cause, i32, Option<&str>)> = vec![
            (
                || anyhow::Error::new(CodedError::new(codes::LOAD_TARGET_NOT_RIVETS, "foreign")),
                5,
                Some("RIVET_LOAD_TARGET_NOT_RIVETS"),
            ),
            (
                || {
                    anyhow::Error::new(CodedError::new(
                        codes::LOAD_ADOPTION_COLUMN_MISMATCH,
                        "columns",
                    ))
                },
                5,
                Some("RIVET_LOAD_ADOPTION_COLUMN_MISMATCH"),
            ),
            (
                || crate::manifest::ManifestInconsistency::DuplicatePartId(1).into(),
                3,
                None,
            ),
            (|| anyhow::anyhow!("connection reset by peer"), 2, None),
            (|| anyhow::anyhow!("no partition statistics"), 1, None),
        ];
        for (cause, exit, code) in causes {
            let stop = crate::load::before_write::<()>(Err(cause()))
                .unwrap_err()
                .context("loading orders");
            assert!(stop.is::<crate::load::Refused>());
            assert_eq!(
                (classify_exit(&stop), error_code(&stop)),
                (exit, code),
                "{stop:#}"
            );
            assert_eq!(
                (classify_exit(&cause()), error_code(&cause())),
                (exit, code),
                "the bare cause reads the same"
            );
        }
    }

    #[test]
    fn coded_error_surfaces_code_through_anyhow_context() {
        // The code rides on the type through `.context()`; `Display` is the
        // verbatim message (operator output unchanged but for the `[CODE]` prefix).
        // `classify_exit` returns `Generic` via its default (no `CodedError` arm),
        // proving the dropped `class` field changed nothing.
        let e = anyhow::Error::new(CodedError::new(
            codes::CONFIG_NO_EXPORTS,
            "exports: at least one export must be defined",
        ))
        .context("while loading config");
        assert_eq!(error_code(&e), Some(codes::CONFIG_NO_EXPORTS.id));
        assert_eq!(classify_exit(&e), ExitClass::Generic.code());
        assert!(format!("{e:#}").contains("at least one export must be defined"));
    }
}

#[cfg(test)]
mod representative_failure_tests {
    use super::representative_failure_idx;
    use crate::error::{DataIntegrityError, ExitClass, SchemaDriftError, classify_exit};

    #[test]
    fn empty_batch_has_no_representative() {
        assert_eq!(representative_failure_idx(&[]), None);
    }

    #[test]
    fn data_integrity_outranks_everything_regardless_of_position() {
        // Data-integrity sits LAST so a naive "first failure" or a flipped
        // min/max selector would pick the generic error instead.
        let failures = vec![
            anyhow::anyhow!("generic boom"),
            SchemaDriftError::new("shape changed").into(),
            anyhow::anyhow!("another generic"),
            DataIntegrityError::new("reconcile mismatch").into(),
        ];
        let idx = representative_failure_idx(&failures).unwrap();
        assert_eq!(
            classify_exit(&failures[idx]),
            ExitClass::DataIntegrity.code(),
            "a mixed batch must surface the data-integrity (exit 3) failure"
        );
    }

    #[test]
    fn schema_drift_outranks_retryable_and_generic() {
        // No data-integrity present → schema-drift (exit 4) is the scariest.
        let failures = vec![
            anyhow::anyhow!("generic"),
            SchemaDriftError::new("drift").into(),
        ];
        let idx = representative_failure_idx(&failures).unwrap();
        assert_eq!(classify_exit(&failures[idx]), ExitClass::SchemaDrift.code());
    }

    #[test]
    fn a_refusal_outranks_a_retryable_and_a_generic_failure() {
        let refused = || -> anyhow::Error {
            crate::error::CodedError::new(
                crate::error::codes::STATE_CURSOR_OWNER_MISMATCH,
                "cursor owned elsewhere",
            )
            .into()
        };
        let failures = vec![refused(), anyhow::anyhow!("connection reset by peer")];
        assert_eq!(classify_exit(&failures[1]), ExitClass::Retryable.code());
        let idx = representative_failure_idx(&failures).unwrap();
        assert_eq!(classify_exit(&failures[idx]), ExitClass::Refusal.code());
        let failures = vec![refused(), anyhow::anyhow!("generic")];
        let idx = representative_failure_idx(&failures).unwrap();
        assert_eq!(classify_exit(&failures[idx]), ExitClass::Refusal.code());
    }

    /// Only a failure with a code, a typed class or a decided exit is a candidate, however its text classifies; of equals the last.
    #[test]
    fn the_worst_typed_failure_is_picked_by_code_class_or_decided_exit() {
        use super::{PreclassifiedExit, worst_typed_failure_idx};
        let transient = || anyhow::anyhow!("connection reset by peer");
        assert_eq!(worst_typed_failure_idx(&[transient(), transient()]), None);
        let typed: [fn() -> anyhow::Error; 3] = [
            || crate::error::coded(crate::error::codes::RUN_UNITS_FAILED, "permanent"),
            || SchemaDriftError::new("drift").into(),
            || PreclassifiedExit(5).into(),
        ];
        for t in typed {
            assert_eq!(worst_typed_failure_idx(&[transient(), t()]), Some(1));
            assert_eq!(worst_typed_failure_idx(&[t(), transient()]), Some(0));
        }
        let refused = || crate::error::coded(crate::error::codes::STATE_CURSOR_OWNER_MISMATCH, "r");
        assert_eq!(
            worst_typed_failure_idx(&[refused(), refused(), transient()]),
            Some(1)
        );
        let integrity: anyhow::Error = DataIntegrityError::new("mismatch").into();
        assert_eq!(
            worst_typed_failure_idx(&[integrity, refused(), transient()]),
            Some(0)
        );
    }
}

#[cfg(test)]
mod stop_marker_tests {
    use super::{JobWaitTimeout, ManifestInconsistency, Refused};

    #[test]
    fn a_job_wait_timeout_names_the_job_and_how_long_it_waited() {
        let shown = JobWaitTimeout::bigquery("job_42", 900).to_string();
        assert_eq!(
            shown.split(" \u{2014} ").next(),
            Some("bigquery: stopped waiting for job `job_42` after 900s")
        );
    }

    #[test]
    fn a_manifest_inconsistency_names_the_two_counts_that_disagree() {
        let shown = |m: ManifestInconsistency| m.to_string();
        assert_eq!(
            shown(ManifestInconsistency::UnsupportedVersion {
                found: 9,
                supported: 1
            }),
            "manifest_version 9 is not supported by this build (expected 1)"
        );
        assert_eq!(
            shown(ManifestInconsistency::PartCountMismatch {
                declared: 3,
                actual: 2
            }),
            "part_count declares 3 parts but 2 committed parts found"
        );
        assert_eq!(
            shown(ManifestInconsistency::RowCountMismatch {
                declared: 10,
                actual: 7
            }),
            "row_count declares 10 rows but committed parts sum to 7"
        );
        assert_eq!(
            shown(ManifestInconsistency::DuplicatePartId(4)),
            "duplicate part_id 4 in manifest.parts"
        );
    }

    #[test]
    fn a_refused_stop_shows_its_cause_and_keeps_the_cause_chain() {
        let cause = anyhow::Error::new(std::io::Error::other("disk gone")).context("staging");
        let stop = Refused(cause);
        assert_eq!(stop.to_string(), "staging");
        assert_eq!(
            std::error::Error::source(&stop).map(ToString::to_string),
            Some("disk gone".to_string())
        );
    }

    #[test]
    fn recoded_keeps_the_inner_code_and_falls_back_without_one() {
        use crate::error::{assert_coded, coded, codes, error_code, recoded};
        let inner = coded(codes::CONFIG_SECRET_UNSET, "DB_URL is not set");
        let out = recoded(
            &inner,
            codes::CONFIG_SOURCE_CONNECTION_INVALID,
            format!("export 'o': the probe needs the URL: {inner}"),
        );
        assert_eq!(error_code(&out), Some("RIVET_CONFIG_SECRET_UNSET"));
        assert_eq!(
            format!("{out}"),
            "export 'o': the probe needs the URL: DB_URL is not set"
        );
        let plain = anyhow::anyhow!("database is locked");
        let out = recoded(
            &plain,
            codes::STATE_NOT_WRITABLE,
            format!("open: {plain:#}"),
        );
        assert_coded(&out, "RIVET_STATE_NOT_WRITABLE", 1);
    }
}
