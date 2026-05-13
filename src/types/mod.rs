//! Rivet's internal type system.
//!
//! See `rivet_roadmap.md` §Epic 14 (Warehouse Load Layer). North Star —
//! *"No silent type degradation"* — is enforced architecturally by
//! routing every source-column type through the pipeline:
//!
//! ```text
//! Source Native Type
//!     ↓
//! SourceColumn  ← what the driver knows about the column
//!     ↓
//! RivetType     ← canonical, vendor-independent type
//!     ↓
//! TypePolicy    ← strict / lossy / unsupported decisions  (Chunk 4)
//!     ↓
//! Arrow DataType + Field metadata  ← physical export type
//! ```
//!
//! This module owns the first three boxes. The fourth (Arrow) is built by
//! [`mapping::build_arrow_field`]; the fifth (TypePolicy) lands in Chunk 4
//! of the type-safety milestones (see roadmap §18).
//!
//! ## Layer
//!
//! Layer-classification (ADR-0003): this module is **planning-layer** — it
//! only describes / classifies types. It must not perform I/O, log
//! metrics, or hold any pipeline state. Vendor mappers live in
//! `crate::source::*` and call into this module.

mod cursor;
pub mod decimal;
mod fidelity;
mod mapping;
mod override_type;
pub mod policy;
mod rivet_type;
mod source_column;
pub mod target;

pub use cursor::CursorState;
pub use fidelity::TypeFidelity;
pub use mapping::{TypeMapping, build_arrow_field};
pub use override_type::parse_type_str;
pub use rivet_type::{RivetType, TimeUnit};
pub use source_column::SourceColumn;
// ColumnOverride is the planned public API for column type overrides (Chunk 6).
#[allow(unused_imports)]
pub use source_column::ColumnOverride;

/// Per-export column type overrides: column name → declared [`RivetType`].
///
/// Built at plan time from the `columns:` map in `rivet.yaml` (roadmap §8).
/// Passed to [`crate::source::Source::export`] so drivers can use the
/// declared precision/scale instead of autodetected (often unavailable) metadata.
pub type ColumnOverrides = std::collections::HashMap<String, RivetType>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::mapping::{META_FIDELITY, META_LOGICAL_TYPE, META_NATIVE_TYPE};
    use arrow::datatypes::DataType;

    /// Top-level smoke test: feeding a typical PostgreSQL `payments` table
    /// through `SourceColumn → RivetType → Arrow Field` produces the schema
    /// shape demanded by the roadmap's §20 "Definition of Done":
    ///
    /// ```text
    /// id          bigint          int64            Int64                  exact
    /// amount      numeric(18,2)   decimal(18,2)    Decimal128(18,2)       exact
    /// created_at  timestamptz     timestamp_tz     Timestamp(us, UTC)     exact
    /// payload     jsonb           json             Utf8 + metadata        logical_string
    /// ```
    #[test]
    fn end_to_end_payments_schema_matches_definition_of_done() {
        let cols: Vec<(SourceColumn, RivetType)> = vec![
            (
                SourceColumn::simple("id", "bigint", false),
                RivetType::Int64,
            ),
            (
                SourceColumn::decimal("amount", "numeric", false, 18, 2),
                RivetType::Decimal {
                    precision: 18,
                    scale: 2,
                },
            ),
            (
                SourceColumn::simple("created_at", "timestamptz", false),
                RivetType::Timestamp {
                    unit: TimeUnit::Microsecond,
                    timezone: Some("UTC".into()),
                },
            ),
            (
                SourceColumn::simple("payload", "jsonb", true),
                RivetType::Json,
            ),
        ];

        let mappings: Vec<TypeMapping> = cols
            .into_iter()
            .map(|(s, t)| TypeMapping::from_source(&s, t))
            .collect();

        // Fidelity matrix mirrors the table in the Definition of Done.
        assert_eq!(mappings[0].fidelity, TypeFidelity::Exact);
        assert_eq!(mappings[1].fidelity, TypeFidelity::Exact);
        assert_eq!(mappings[2].fidelity, TypeFidelity::Exact);
        assert_eq!(mappings[3].fidelity, TypeFidelity::LogicalString);

        // Arrow types are exactly what the roadmap demands — no Utf8 fallback for decimal.
        assert_eq!(mappings[0].arrow_type, Some(DataType::Int64));
        assert_eq!(mappings[1].arrow_type, Some(DataType::Decimal128(18, 2)));
        assert!(matches!(
            mappings[2].arrow_type,
            Some(DataType::Timestamp(_, Some(_)))
        ));
        assert_eq!(mappings[3].arrow_type, Some(DataType::Utf8));

        // Field-level metadata is preserved end-to-end.
        let amount_field = build_arrow_field(&mappings[1]).expect("amount");
        assert_eq!(
            amount_field
                .metadata()
                .get(META_NATIVE_TYPE)
                .map(String::as_str),
            Some("numeric")
        );
        assert_eq!(
            amount_field
                .metadata()
                .get(META_FIDELITY)
                .map(String::as_str),
            Some("exact")
        );

        let payload_field = build_arrow_field(&mappings[3]).expect("payload");
        assert_eq!(
            payload_field
                .metadata()
                .get(META_LOGICAL_TYPE)
                .map(String::as_str),
            Some("json")
        );
    }
}
