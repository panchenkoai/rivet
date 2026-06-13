//! The shared shape of a *preflight schema error* — a permanent, author-fixable
//! failure (missing table/column, no SELECT grant, query syntax) that must make
//! `rivet check` FAIL loudly instead of sailing through to run time.
//!
//! ADR-0015 pattern: the seam lives at this **named data shape**, not a trait.
//! Each engine detects its own native error taxonomy and maps it here —
//!
//! - `postgres::schema_fail_pg`   — SQLSTATE class 42 (typed `postgres::Error`)
//! - `mysql::mysql_schema_error`  — codes 1146/1054/1142/1064 (typed `mysql::Error`)
//! - `mssql::schema_fail_mssql`    — "Invalid object/column name" + the SQL
//!   Server number parsed from the error string. The `query_scalar` seam erases
//!   the *typed* tiberius error; the number survives only in its Display, so a
//!   *typed* code would need probing off that seam (the ADR-0011 boundary).
//!
//! The actionable sentence and the `preflight:` framing live here, once, so they
//! cannot drift across engines. (They had: "the user" vs "the login", "the
//! export's query" vs "the query", and inconsistent code placement.)

/// A permanent, author-fixable schema error surfaced by preflight.
///
/// `detail` is the engine-phrased reason (`relation "ordrs" does not exist`);
/// `code_label` is the engine's parenthetical tag (`SQLSTATE 42P01`, `MySQL
/// error 1146`, or the SQL Server reason line). Build with [`Self::new`] and
/// render with [`Self::into_error`] — the message template lives in one place.
pub(crate) struct PreflightSchemaError {
    detail: String,
    code_label: String,
}

impl PreflightSchemaError {
    pub(crate) fn new(detail: impl Into<String>, code_label: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
            code_label: code_label.into(),
        }
    }

    /// Render to the single, shared author-facing preflight error.
    pub(crate) fn into_error(self) -> anyhow::Error {
        anyhow::anyhow!(
            "preflight: {} ({}). Check the table/column names in the export's query, \
             and that the connecting user has SELECT on them.",
            self.detail,
            self.code_label
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_the_single_actionable_sentence() {
        let msg = format!(
            "{:#}",
            PreflightSchemaError::new("relation \"ordrs\" does not exist", "SQLSTATE 42P01")
                .into_error()
        );
        assert_eq!(
            msg,
            "preflight: relation \"ordrs\" does not exist (SQLSTATE 42P01). \
             Check the table/column names in the export's query, and that the \
             connecting user has SELECT on them."
        );
    }

    #[test]
    fn every_engine_lands_in_the_same_sentence() {
        // MySQL- and MSSQL-shaped inputs fill the same slots — no per-engine
        // drift in wording, only the detail and code label vary.
        for (detail, code) in [
            ("Unknown column 'totl' in 'field list'", "MySQL error 1054"),
            (
                "a table/view in the export's query does not exist",
                "Invalid object name 'ordrs'.",
            ),
        ] {
            let msg = format!("{:#}", PreflightSchemaError::new(detail, code).into_error());
            assert!(msg.starts_with(&format!("preflight: {detail} ({code}).")));
            assert!(
                msg.ends_with("and that the connecting user has SELECT on them."),
                "shared trailer drifted: {msg}"
            );
        }
    }
}
