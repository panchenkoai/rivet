//! **Layer: Cross-cutting**
//!
//! Error type alias plus the **exit-code taxonomy**: a small, stable set of
//! process exit codes so an *unattended scheduler* can branch on the failure
//! *class* instead of grepping stderr. Before this, `main` exited `1` for every
//! error, forcing operators to regex the error text to decide retry-vs-stop.

/// Machine-actionable exit-code taxonomy.
///
/// A scheduler keys its retry / alert policy off the numeric exit code:
///
/// | code | class | scheduler action |
/// |------|-------|------------------|
/// | `0`  | success | — (handled separately, not in this enum) |
/// | `1`  | [`Generic`](ExitClass::Generic): config / usage / unclassified error | fix the config; do **not** retry blindly |
/// | `2`  | [`Retryable`](ExitClass::Retryable): transient (connection reset, lock-wait timeout, capacity) | safe to retry the *same* command |
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
    /// `2` — transient failure (connection reset, lock-wait timeout, capacity).
    /// Safe to retry the same command after a backoff.
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
        write!(f, "child exited with status {}", self.0)
    }
}

impl std::error::Error for PreclassifiedExit {}

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
/// A `CodedError` is always exit class `Generic` (config / usage — fix it, don't
/// retry), which is already [`classify_exit`]'s default, so it carries no class
/// and needs no downcast arm there. The first coded error that needs a
/// non-`Generic` class (e.g. a retryable source failure) is where a class field
/// would be reintroduced — until then it is dead weight.
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

/// The stable `RIVET_*` error code for a failure, if one was tagged via
/// [`CodedError`] anywhere in the anyhow context chain. `main` surfaces it as the
/// JSON `code` field and a `[CODE]` prefix on the text error line.
pub fn error_code(err: &anyhow::Error) -> Option<&'static str> {
    if let Some(c) = err.downcast_ref::<CodedError>() {
        return Some(c.code());
    }
    // The existing source-side statement-timeout marker also gets a stable code,
    // so the long-query failure an operator's `statement_timeout` tooling watches
    // for is greppable without re-tagging its construction site.
    if err
        .downcast_ref::<crate::source::StatementDurationTimeout>()
        .is_some()
    {
        return Some(codes::SOURCE_STATEMENT_TIMEOUT.id);
    }
    None
}

/// Map an error to its process exit code per the [`ExitClass`] taxonomy.
///
/// Precedence (first match wins):
/// 1. [`SchemaDriftError`] downcast → `4`.
/// 2. [`DataIntegrityError`] **or** [`crate::manifest::ManifestInconsistency`]
///    downcast → `3`.
/// 3. otherwise, if [`crate::pipeline::retry::classify_error`] says the error is
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
    // Each check downcasts through anyhow's context chain.
    // A child process already classified itself and exited with that code; honor
    // it verbatim (parallel-export path) so the parent surfaces the same class.
    if let Some(p) = err.downcast_ref::<PreclassifiedExit>() {
        return p.0;
    }
    if err.downcast_ref::<SchemaDriftError>().is_some() {
        return ExitClass::SchemaDrift.code();
    }
    if err.downcast_ref::<DataIntegrityError>().is_some()
        || err
            .downcast_ref::<crate::manifest::ManifestInconsistency>()
            .is_some()
    {
        return ExitClass::DataIntegrity.code();
    }
    // A registered code decides by its kind; an environment failure still goes through the
    // transient check below (a dropped connection retries, a denied permission does not).
    if let Some(c) = err.downcast_ref::<CodedError>() {
        match c.kind() {
            ErrorKind::Refusal => return ExitClass::Refusal.code(),
            ErrorKind::Internal => return ExitClass::Internal.code(),
            ErrorKind::Integrity => return ExitClass::DataIntegrity.code(),
            ErrorKind::Usage => return ExitClass::Generic.code(),
            ErrorKind::Environment => {}
        }
    }
    if crate::pipeline::retry::classify_error(err).is_transient() {
        return ExitClass::Retryable.code();
    }
    ExitClass::Generic.code()
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
    pub const CONFIG_CSV_LOAD_UNSUPPORTED: Code = usage(
        "RIVET_CONFIG_CSV_LOAD_UNSUPPORTED",
        "use `format: parquet` for an export with a `load:` section",
    );
    /// An export mode is not supported by the configured source type — today
    /// this is a non-SQL source (MongoDB) with any `mode:` other than `full`.
    pub const CONFIG_SOURCE_MODE_UNSUPPORTED: Code = usage(
        "RIVET_CONFIG_SOURCE_MODE_UNSUPPORTED",
        "use a mode this source supports (MongoDB: `full`)",
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

    pub const STATE_SCHEMA_NEWER: Code = refusal(
        "RIVET_STATE_SCHEMA_NEWER",
        "upgrade rivet, or point this binary at a state DB it created",
    );
    pub const STATE_CURSOR_OWNER_MISMATCH: Code = refusal(
        "RIVET_STATE_CURSOR_OWNER_MISMATCH",
        "`rivet state reset --export <name>` to start the new cursor with a full pass, or restore the previous cursor column",
    );
    pub const SOURCE_CURSOR_FINER_THAN_MICROSECOND: Code = refusal(
        "RIVET_SOURCE_CURSOR_FINER_THAN_MICROSECOND",
        "cursor on a column at microsecond precision or coarser, or cast the cursor to TIMESTAMP(6) in a curated query",
    );
    pub const LOAD_COUNT_MISMATCH: Code = integrity(
        "RIVET_LOAD_COUNT_MISMATCH",
        "compare the warehouse table with the run's manifest before re-running; the source is kept",
    );
    pub const LOAD_ADOPTION_COLUMN_MISMATCH: Code = refusal(
        "RIVET_LOAD_ADOPTION_COLUMN_MISMATCH",
        "add the export's new columns to the table (`ALTER TABLE … ADD COLUMN`) and re-run; do not rename it aside",
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

    /// Every registered code: the docs page and the guard tests read this one list.
    pub const ALL: &[Code] = &[
        CONFIG_NO_EXPORTS,
        CONFIG_CHUNK_COUNT_INVALID,
        CONFIG_CHUNK_BY_DAYS_INVALID,
        CONFIG_DUPLICATE_EXPORT,
        CONFIG_CDC_RESOURCE_CONFLICT,
        CONFIG_CDC_ROLLOVER_INVALID,
        CONFIG_CSV_LOAD_UNSUPPORTED,
        CONFIG_SOURCE_MODE_UNSUPPORTED,
        SOURCE_STATEMENT_TIMEOUT,
        SOURCE_CURSOR_FINER_THAN_MICROSECOND,
        STATE_SCHEMA_NEWER,
        STATE_CURSOR_OWNER_MISMATCH,
        LOAD_COUNT_MISMATCH,
        LOAD_ADOPTION_COLUMN_MISMATCH,
        INTERNAL_VALUE_CONVERTER,
        INTERNAL_SPILL,
        INTERNAL_TYPE_BUILDER,
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
         | 2 | a transient failure — retry the same command |\n\
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

#[cfg(test)]
mod tests {
    use super::*;

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
        "VALIDATE", "INIT", "INTERNAL",
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
        assert_eq!(with(ErrorKind::Internal), 6);
        assert_eq!(with(ErrorKind::Integrity), 3);
        assert_eq!(with(ErrorKind::Usage), 1);
        assert_eq!(
            with(ErrorKind::Environment),
            1,
            "a non-transient environment failure"
        );
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
