//! Native database column metadata captured before mapping to a [`RivetType`].
//!
//! See `rivet_roadmap.md` §Epic 14 (type safety). §6 — Internal Type System.
//! point of this struct is to keep *enough* native-DB metadata to make a
//! lossless mapping decision later — in particular `precision` / `scale` for
//! `numeric(p,s)` / `decimal(p,s)` — instead of going straight to
//! `arrow::DataType` and silently degrading to `Utf8`.
//!
//! The struct is intentionally driver-agnostic: PostgreSQL fills it in from
//! `pg_attribute` / `information_schema`, MySQL from `information_schema`,
//! and the planner can also synthesize a row from a YAML override (roadmap §8).

use serde::Serialize;

use super::RivetType;

/// Metadata captured from the source database for a single column,
/// before it is mapped to a [`RivetType`].
///
/// Names follow the roadmap struct (§6 SourceColumn) one-for-one so the
/// document and the code stay in lockstep. Optional fields are populated
/// only when the source driver actually provides them — Rivet must never
/// invent a precision/scale to "fill in" a missing one (that is exactly
/// the silent-degradation pattern the roadmap forbids).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SourceColumn {
    /// Column name as reported by the source database.
    pub name: String,
    /// Native database type identifier — vendor-specific string used for
    /// reports and policy decisions (e.g. `"numeric"`, `"timestamptz"`,
    /// `"jsonb"`). Always present.
    pub native_type: String,
    /// True when the source schema declares the column nullable.
    pub nullable: bool,
    /// Decimal precision — number of total significant digits. Only present
    /// for fixed-precision numeric types where the source actually declared
    /// a precision (e.g. `numeric(18,2)` → `Some(18)`; `numeric` → `None`).
    pub precision: Option<u8>,
    /// Decimal scale — number of digits to the right of the decimal point.
    /// Signed because PostgreSQL allows negative scale for `numeric` (e.g.
    /// `numeric(5,-2)` rounds to hundreds).
    pub scale: Option<i8>,
    /// Length for variable-length character/binary types when the source
    /// declares one (e.g. `varchar(255)` → `Some(255)`).
    pub length: Option<u64>,
    /// Fractional-second precision for temporal types when declared
    /// (e.g. `timestamp(6)` → `Some(6)`).
    pub datetime_precision: Option<u8>,
}

impl SourceColumn {
    /// Convenience constructor for the most common case: a column whose
    /// native type doesn't carry length / precision metadata.
    pub fn simple(name: impl Into<String>, native_type: impl Into<String>, nullable: bool) -> Self {
        Self {
            name: name.into(),
            native_type: native_type.into(),
            nullable,
            precision: None,
            scale: None,
            length: None,
            datetime_precision: None,
        }
    }

    /// Convenience constructor for declared decimal columns.
    #[allow(dead_code)]
    pub fn decimal(
        name: impl Into<String>,
        native_type: impl Into<String>,
        nullable: bool,
        precision: u8,
        scale: i8,
    ) -> Self {
        Self {
            name: name.into(),
            native_type: native_type.into(),
            nullable,
            precision: Some(precision),
            scale: Some(scale),
            length: None,
            datetime_precision: None,
        }
    }
}

/// A user-supplied YAML override (`exports[].columns:` in `rivet.yaml`,
/// roadmap §8) carries one of these per column. Stored separately from
/// [`SourceColumn`] so it's obvious in code review whether a particular
/// type came from autodetect or from an explicit override.
///
/// Wired into the planning pipeline by Chunk 6.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ColumnOverride {
    /// Name of the column the override applies to (case-sensitive — matches
    /// what the source driver returns; mismatch is rejected at config-load).
    pub name: String,
    /// The user-declared type. Replaces whatever autodetect produced.
    pub rivet_type: RivetType,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_constructor_leaves_optional_metadata_blank() {
        let c = SourceColumn::simple("id", "int8", false);
        assert_eq!(c.name, "id");
        assert_eq!(c.native_type, "int8");
        assert!(!c.nullable);
        assert_eq!(c.precision, None);
        assert_eq!(c.scale, None);
        assert_eq!(c.length, None);
        assert_eq!(c.datetime_precision, None);
    }

    #[test]
    fn decimal_constructor_sets_precision_scale() {
        let c = SourceColumn::decimal("amount", "numeric", true, 18, 2);
        assert_eq!(c.precision, Some(18));
        assert_eq!(c.scale, Some(2));
        assert!(c.nullable);
    }

    #[test]
    fn negative_scale_is_supported_for_postgres_numeric() {
        let c = SourceColumn::decimal("rough", "numeric", false, 5, -2);
        assert_eq!(c.scale, Some(-2));
    }

    #[test]
    fn source_column_serializes_nullable_optional_fields() {
        let c = SourceColumn::decimal("amount", "numeric", true, 18, 2);
        let json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&c).expect("serialize")).expect("parse");
        assert_eq!(json["name"], "amount");
        assert_eq!(json["native_type"], "numeric");
        assert_eq!(json["nullable"], true);
        assert_eq!(json["precision"], 18);
        assert_eq!(json["scale"], 2);
        assert!(json["length"].is_null());
        assert!(json["datetime_precision"].is_null());
    }
}
