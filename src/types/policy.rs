//! TypePolicy — strict / warn / allow decisions for unsafe type mappings.
//!
//! Roadmap §7 ("TypePolicy"). A `TypePolicy` is created from config or CLI
//! flags (e.g. `--strict`) and applied to the `Vec<TypeMapping>` that the
//! driver computes before the first row is read. Violations are collected
//! rather than raised immediately so the caller can print all problems at once.

use serde::Serialize;

use super::{RivetType, TypeFidelity, TypeMapping};

/// What to do when a mapping is classified as lossy or unsupported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyAction {
    /// Exit non-zero before the export starts.
    Fail,
    /// Print a warning and continue.
    Warn,
    /// Silently continue.
    Allow,
}

/// Per-export type-safety policy (roadmap §7).
///
/// The default is `strict` — both lossy and unsupported mappings fail.
/// `--no-strict` / future config can relax individual axes.
#[derive(Debug, Clone)]
pub struct TypePolicy {
    /// Action when a column mapping is [`TypeFidelity::Lossy`].
    pub on_lossy_mapping: PolicyAction,
    /// Action when a column mapping is [`TypeFidelity::Unsupported`].
    pub on_unsupported_type: PolicyAction,
}

impl Default for TypePolicy {
    fn default() -> Self {
        Self::strict()
    }
}

impl TypePolicy {
    /// Strict mode: both lossy and unsupported mappings are fatal.
    pub fn strict() -> Self {
        Self {
            on_lossy_mapping: PolicyAction::Fail,
            on_unsupported_type: PolicyAction::Fail,
        }
    }

    /// Permissive mode: warn only, never fail. Useful for `--type-report`
    /// when the user just wants to see the table without aborting.
    pub fn warn_only() -> Self {
        Self {
            on_lossy_mapping: PolicyAction::Warn,
            on_unsupported_type: PolicyAction::Warn,
        }
    }
}

/// One policy violation produced by [`TypePolicy::validate`].
#[derive(Debug, Clone, Serialize)]
pub struct PolicyViolation {
    /// Column name in the export query.
    pub column_name: String,
    /// Fidelity that triggered the violation.
    pub fidelity: TypeFidelity,
    /// Human-readable description (printed by the CLI and `--json` output).
    pub message: String,
    /// Whether this violation is fatal under the active policy.
    pub fatal: bool,
}

impl TypePolicy {
    /// Validate `mappings` and return all violations (both warn and fail).
    pub fn validate(&self, mappings: &[TypeMapping]) -> Vec<PolicyViolation> {
        let mut out = Vec::new();
        for m in mappings {
            let (action, fidelity) = match m.fidelity {
                TypeFidelity::Lossy => (self.on_lossy_mapping, TypeFidelity::Lossy),
                TypeFidelity::Unsupported => (self.on_unsupported_type, TypeFidelity::Unsupported),
                _ => continue,
            };
            if action == PolicyAction::Allow {
                continue;
            }
            let detail = match &m.rivet_type {
                RivetType::Unsupported { reason, .. } => format!(": {}", reason),
                _ => String::new(),
            };
            out.push(PolicyViolation {
                column_name: m.column_name.clone(),
                fidelity,
                message: format!(
                    "column '{}' (source type '{}'): fidelity={}{}",
                    m.column_name,
                    m.source_native_type,
                    fidelity.label(),
                    detail
                ),
                fatal: action == PolicyAction::Fail,
            });
        }
        out
    }

    /// Return `Err` when any `fatal` violation exists, otherwise `Ok(())`.
    #[allow(dead_code)]
    pub fn check_fail(&self, violations: &[PolicyViolation]) -> crate::error::Result<()> {
        let fatal: Vec<&str> = violations
            .iter()
            .filter(|v| v.fatal)
            .map(|v| v.message.as_str())
            .collect();
        if !fatal.is_empty() {
            anyhow::bail!(
                "strict mode: {} unsafe type mapping(s):\n{}",
                fatal.len(),
                fatal.join("\n")
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{SourceColumn, TypeMapping};

    fn unsupported_mapping(name: &str, native: &str) -> TypeMapping {
        let col = SourceColumn::simple(name, native, true);
        TypeMapping::from_source(
            &col,
            RivetType::Unsupported {
                native_type: native.into(),
                reason: "test reason".into(),
            },
        )
    }

    fn exact_mapping(name: &str, native: &str) -> TypeMapping {
        let col = SourceColumn::simple(name, native, true);
        TypeMapping::from_source(&col, crate::types::RivetType::Int64)
    }

    #[test]
    fn strict_policy_fails_on_unsupported() {
        let policy = TypePolicy::strict();
        let mappings = vec![
            exact_mapping("id", "int8"),
            unsupported_mapping("location", "geometry"),
        ];
        let violations = policy.validate(&mappings);
        assert_eq!(violations.len(), 1);
        assert!(violations[0].fatal);
        assert_eq!(violations[0].column_name, "location");
        assert!(policy.check_fail(&violations).is_err());
    }

    #[test]
    fn warn_only_policy_does_not_fail() {
        let policy = TypePolicy::warn_only();
        let mappings = vec![unsupported_mapping("dur", "interval")];
        let violations = policy.validate(&mappings);
        assert_eq!(violations.len(), 1);
        assert!(!violations[0].fatal);
        assert!(policy.check_fail(&violations).is_ok());
    }

    #[test]
    fn allow_policy_produces_no_violations() {
        let policy = TypePolicy {
            on_lossy_mapping: PolicyAction::Allow,
            on_unsupported_type: PolicyAction::Allow,
        };
        let mappings = vec![unsupported_mapping("x", "hstore")];
        assert!(policy.validate(&mappings).is_empty());
    }

    #[test]
    fn exact_mappings_never_produce_violations() {
        let policy = TypePolicy::strict();
        let mappings = vec![
            exact_mapping("id", "int8"),
            TypeMapping::from_source(
                &SourceColumn::simple("name", "text", true),
                crate::types::RivetType::Text,
            ),
        ];
        assert!(policy.validate(&mappings).is_empty());
    }
}
