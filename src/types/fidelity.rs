//! Type fidelity classification.
//!
//! See `rivet_roadmap.md` §Epic 14. Type Fidelity Levels: Every
//! source-column → Arrow mapping is tagged with one of these levels so the
//! type policy and the type-report CLI can reason about what the user is
//! actually getting on disk.
//!
//! - [`TypeFidelity::Exact`]      — value and type semantics fully preserved
//!   (e.g. `numeric(18,2)` → `Decimal128(18,2)` → Parquet `DECIMAL(18,2)`).
//! - [`TypeFidelity::Compatible`] — value preserved, the on-disk physical
//!   type differs but a logical-type marker keeps the original semantics
//!   recoverable (e.g. `uuid` → `Utf8 + metadata logical=uuid`).
//! - [`TypeFidelity::LogicalString`] — value preserved as text, native
//!   semantics are *not* guaranteed (e.g. `jsonb` → `Utf8 + metadata
//!   logical=json`).
//! - [`TypeFidelity::Lossy`]      — precision or semantics may be lost
//!   (e.g. `decimal(18,2)` → `Float64`). Forbidden in strict mode.
//! - [`TypeFidelity::Unsupported`] — Rivet refuses to export the column
//!   without an explicit policy override.

use serde::Serialize;

/// Fidelity tag attached to every [`crate::types::TypeMapping`].
///
/// The variants are deliberately ordered from "best" to "worst" so that
/// downstream code (CLI report, strict-mode gate) can compare them with
/// `PartialOrd` semantics:
/// `Exact > Compatible > LogicalString > Lossy > Unsupported`.
// `Lossy` and `is_unsafe_for_strict_mode` are used by TypePolicy (Chunk 4).
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TypeFidelity {
    /// Value and type semantics fully preserved on disk.
    Exact,
    /// Value preserved; physical type differs, logical-type metadata
    /// recovers the original semantics.
    Compatible,
    /// Value preserved as text; native type semantics are not guaranteed.
    LogicalString,
    /// Precision or semantics may be lost. Strict mode rejects this.
    Lossy,
    /// Rivet does not safely support this type. Strict mode rejects this.
    Unsupported,
}

impl TypeFidelity {
    /// Stable lowercase string label for persistence, JSON output, and
    /// human-readable reports. Prefer this over `format!("{:?}")` —
    /// `Debug` output is not a stable contract.
    pub fn label(self) -> &'static str {
        match self {
            TypeFidelity::Exact => "exact",
            TypeFidelity::Compatible => "compatible",
            TypeFidelity::LogicalString => "logical_string",
            TypeFidelity::Lossy => "lossy",
            TypeFidelity::Unsupported => "unsupported",
        }
    }

    /// True when the fidelity level is one that strict mode must reject
    /// without an explicit policy override (roadmap §7 "Strict mode
    /// behavior"). Used by TypePolicy (Chunk 4).
    #[allow(dead_code)]
    pub fn is_unsafe_for_strict_mode(self) -> bool {
        matches!(self, TypeFidelity::Lossy | TypeFidelity::Unsupported)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fidelity_label_round_trips_through_json() {
        let cases = [
            (TypeFidelity::Exact, "\"exact\""),
            (TypeFidelity::Compatible, "\"compatible\""),
            (TypeFidelity::LogicalString, "\"logical_string\""),
            (TypeFidelity::Lossy, "\"lossy\""),
            (TypeFidelity::Unsupported, "\"unsupported\""),
        ];
        for (f, expected_json) in cases {
            assert_eq!(
                serde_json::to_string(&f).expect("serialize fidelity"),
                expected_json,
                "JSON shape for {:?} must match label() so CLI --json output is stable",
                f
            );
            assert_eq!(f.label(), expected_json.trim_matches('"'));
        }
    }

    #[test]
    fn ordering_is_best_to_worst() {
        assert!(TypeFidelity::Exact < TypeFidelity::Compatible);
        assert!(TypeFidelity::Compatible < TypeFidelity::LogicalString);
        assert!(TypeFidelity::LogicalString < TypeFidelity::Lossy);
        assert!(TypeFidelity::Lossy < TypeFidelity::Unsupported);
    }

    #[test]
    fn strict_mode_only_rejects_lossy_and_unsupported() {
        assert!(!TypeFidelity::Exact.is_unsafe_for_strict_mode());
        assert!(!TypeFidelity::Compatible.is_unsafe_for_strict_mode());
        assert!(!TypeFidelity::LogicalString.is_unsafe_for_strict_mode());
        assert!(TypeFidelity::Lossy.is_unsafe_for_strict_mode());
        assert!(TypeFidelity::Unsupported.is_unsafe_for_strict_mode());
    }
}
