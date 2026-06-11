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
/// | `3`  | [`DataIntegrity`](ExitClass::DataIntegrity): quality gate / reconcile mismatch / duplicate-guard / manifest inconsistency | **STOP** — data may be wrong, do **not** blindly retry |
/// | `4`  | [`SchemaDrift`](ExitClass::SchemaDrift): `on_schema_drift: fail` tripped | the source shape changed — needs human review |
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
    /// `3` — data-integrity failure (quality gate, reconcile mismatch,
    /// duplicate-guard, manifest inconsistency). The exported data may be wrong;
    /// **stop** and investigate rather than retry.
    DataIntegrity = 3,
    /// `4` — schema-drift failure (`on_schema_drift: fail` tripped). The source
    /// shape changed; a human must review before re-running.
    SchemaDrift = 4,
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
    if crate::pipeline::retry::classify_error(err).is_transient() {
        return ExitClass::Retryable.code();
    }
    ExitClass::Generic.code()
}

pub type Result<T> = anyhow::Result<T>;

#[cfg(test)]
mod tests {
    use super::*;

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
}
