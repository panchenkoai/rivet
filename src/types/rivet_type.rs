//! Rivet's canonical internal type system.
//!
//! See `rivet_roadmap.md` §Epic 14 (type safety). §5 — Type Mapping Pipeline, §6
//! ("Internal Type System"). Every database driver maps its native column
//! metadata into a [`RivetType`] *first*, then a single function maps
//! [`RivetType`] to `arrow::DataType`. This is the architectural fix for the
//! status-quo `DB type → Arrow type` shortcut that silently degrades types
//! to `Utf8` (roadmap §5 "incorrect pipeline").
//!
//! Three invariants enforced by this enum:
//!
//! 1. **No silent precision loss** — `Decimal { precision, scale }` carries
//!    the declared precision/scale; constructing one without a known
//!    precision is impossible by construction. Unbounded numeric columns
//!    therefore *must* go through [`RivetType::Unsupported`] or be resolved
//!    by `TypePolicy::decimal.unbounded` (Chunk 4).
//! 2. **No silent timezone loss** — `Timestamp { unit, timezone }` makes the
//!    timezone explicit; `timezone: None` means "no timezone semantics" and
//!    is *not* the same as `Some("UTC")`.
//! 3. **No silent fallback to string** — anything Rivet can't safely map is
//!    represented as [`RivetType::Unsupported`] with a reason string, so
//!    the type-policy layer can decide whether to fail / warn / fallback.
//!
//! `serde::Serialize` is implemented so the type-report CLI (Chunk 5) can
//! emit a stable JSON shape.

use serde::{Deserialize, Serialize};

/// Time-resolution unit for [`RivetType::Time`] / [`RivetType::Timestamp`].
///
/// Mirrors `arrow::datatypes::TimeUnit` but lives in the Rivet type system
/// so we don't leak Arrow as a public API surface and so the type-report
/// CLI can serialize the value without depending on Arrow's types.
// Only `Microsecond` is produced by current drivers; the remaining variants
// are live once ADBC / other drivers are added (roadmap §4).
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl TimeUnit {
    /// Stable lowercase string label for persistence and reports.
    #[allow(dead_code)]
    pub fn label(self) -> &'static str {
        match self {
            TimeUnit::Second => "second",
            TimeUnit::Millisecond => "millisecond",
            TimeUnit::Microsecond => "microsecond",
            TimeUnit::Nanosecond => "nanosecond",
        }
    }
}

/// Canonical Rivet type. Every source-driver column maps into exactly one
/// of these variants before we ever look at `arrow::DataType`.
///
/// Variants are kept narrow on purpose: adding a new variant is a deliberate
/// architectural choice (it usually means "we figured out how to safely
/// export a new shape of data"). Anything outside this enum becomes
/// [`RivetType::Unsupported`] until the type system gains first-class
/// support for it.
// `UInt64` and `Decimal` are live in MySQL/PG mappers once column overrides
// (Chunk 6) and exact decimal (Milestone 2) land; `Second`/`Millisecond`/
// `Nanosecond` TimeUnit variants serve future drivers.
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RivetType {
    /// Boolean.
    Bool,

    /// Signed 16-bit integer (PostgreSQL `int2`, MySQL `smallint`).
    Int16,
    /// Signed 32-bit integer (PostgreSQL `int4`, MySQL `int`).
    Int32,
    /// Signed 64-bit integer (PostgreSQL `int8`, MySQL `bigint signed`).
    Int64,
    /// Unsigned 64-bit integer (MySQL `bigint unsigned`).
    /// Target compatibility check required (BigQuery has no unsigned 64).
    UInt64,

    /// IEEE-754 32-bit float. Marked `exact-ish` in the roadmap mappings
    /// because float→float is bit-exact but float→target may not round-trip.
    Float32,
    /// IEEE-754 64-bit float.
    Float64,

    /// Fixed-precision decimal. Precision/scale are *required* — the whole
    /// point of having a separate variant from `Float64` is that we never
    /// route money/decimal through floats. Mapped to Arrow Decimal128 when
    /// `precision <= 38`, Decimal256 otherwise (roadmap §12).
    Decimal {
        /// Total number of significant digits.
        precision: u8,
        /// Digits to the right of the decimal point. Signed because
        /// PostgreSQL `numeric` allows negative scale.
        scale: i8,
    },

    /// Calendar date (no time, no timezone).
    Date,

    /// Time-of-day with the given resolution.
    Time { unit: TimeUnit },

    /// Timestamp with explicit timezone semantics. `timezone: None` means
    /// "no timezone" (PostgreSQL `timestamp`, MySQL `datetime`);
    /// `timezone: Some("UTC")` means timezone-normalized to UTC
    /// (PostgreSQL `timestamptz`, MySQL `timestamp` with session tz=+00:00).
    Timestamp {
        unit: TimeUnit,
        timezone: Option<String>,
    },

    /// Variable-length string (PostgreSQL `varchar`, `text`, `bpchar`,
    /// `name`; MySQL `varchar`, `text`).
    String,
    /// Long-form text. Currently treated identically to `String` on the
    /// Arrow layer (both → `Utf8`), but kept as a separate variant so the
    /// type-report can distinguish "the source declared this as text" from
    /// "the source declared this as fixed-length char".
    Text,
    /// Variable-length binary (PostgreSQL `bytea`, MySQL `varbinary`/`blob`).
    Binary,

    /// JSON / JSONB. Stored as `Utf8 + metadata logical=json` until proper
    /// struct inference is implemented (roadmap §14).
    Json,
    /// UUID. Stored as `Utf8 + metadata logical=uuid` by default; can be
    /// switched to FixedSizeBinary(16) by policy later (roadmap §14).
    Uuid,

    // ── M6: Complex Types ─────────────────────────────────────────────────
    /// Database enum type (PostgreSQL `ENUM`, MySQL `ENUM`/`SET`).
    /// Stored as `Utf8 + metadata logical=enum` (roadmap §15).
    Enum,

    /// Time interval (PostgreSQL `interval`).
    /// Stored as Arrow `Utf8` (ISO 8601 duration string, e.g. `"P1Y2M3D"`).
    /// `Interval(MonthDayNano)` cannot be written to Parquet, so lossless
    /// text serialisation is used instead (roadmap §15).
    Interval,

    /// One-dimensional array of a scalar Rivet type.
    /// PostgreSQL `int8[]`, `text[]`, `bool[]`, etc.
    /// Stored as Arrow `List(inner_type)` (roadmap §15).
    List { inner: Box<RivetType> },

    /// The driver knows about the type but Rivet does not have a safe
    /// mapping for it (e.g. PostgreSQL `geometry`, `hstore`).
    /// Carries enough context for an actionable error message in the
    /// type-report and policy layer.
    Unsupported { native_type: String, reason: String },
}

impl RivetType {
    /// Stable lowercase string label for persistence and human-readable
    /// reports. Round-trippable with the JSON shape of the variant when
    /// applicable (e.g. `decimal(18,2)`, `timestamp_tz(microsecond,UTC)`).
    /// Used by the type-report CLI (Chunk 5).
    #[allow(dead_code)]
    pub fn label(&self) -> String {
        match self {
            RivetType::Bool => "bool".into(),
            RivetType::Int16 => "int16".into(),
            RivetType::Int32 => "int32".into(),
            RivetType::Int64 => "int64".into(),
            RivetType::UInt64 => "uint64".into(),
            RivetType::Float32 => "float32".into(),
            RivetType::Float64 => "float64".into(),
            RivetType::Decimal { precision, scale } => format!("decimal({precision},{scale})"),
            RivetType::Date => "date".into(),
            RivetType::Time { unit } => format!("time({})", unit.label()),
            RivetType::Timestamp {
                unit,
                timezone: None,
            } => format!("timestamp({})", unit.label()),
            RivetType::Timestamp {
                unit,
                timezone: Some(tz),
            } => format!("timestamp_tz({},{tz})", unit.label()),
            RivetType::String => "string".into(),
            RivetType::Text => "text".into(),
            RivetType::Binary => "binary".into(),
            RivetType::Json => "json".into(),
            RivetType::Uuid => "uuid".into(),
            RivetType::Enum => "enum".into(),
            RivetType::Interval => "interval".into(),
            RivetType::List { inner } => format!("list<{}>", inner.label()),
            RivetType::Unsupported { native_type, .. } => format!("unsupported({native_type})"),
        }
    }

    /// True for the `Unsupported` variant — convenience for the strict-mode
    /// gate so callers don't have to `matches!()` everywhere.
    #[allow(dead_code)]
    pub fn is_unsupported(&self) -> bool {
        match self {
            RivetType::Unsupported { .. } => true,
            // A list of an unsupported element is itself unsupported — the run
            // can't build the field. Keep consistent with `derive_fidelity`.
            RivetType::List { inner } => inner.is_unsupported(),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn label_includes_decimal_precision_and_scale() {
        assert_eq!(
            RivetType::Decimal {
                precision: 18,
                scale: 2,
            }
            .label(),
            "decimal(18,2)"
        );
    }

    #[test]
    fn label_distinguishes_timestamp_with_and_without_timezone() {
        let naive = RivetType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: None,
        };
        let tz = RivetType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: Some("UTC".into()),
        };
        assert_eq!(naive.label(), "timestamp(microsecond)");
        assert_eq!(tz.label(), "timestamp_tz(microsecond,UTC)");
        assert_ne!(naive, tz, "tz=None and tz=Some(\"UTC\") must NOT be equal");
    }

    #[test]
    fn unsupported_carries_actionable_context() {
        let t = RivetType::Unsupported {
            native_type: "interval".into(),
            reason: "Arrow Interval mapping not implemented yet".into(),
        };
        assert!(t.is_unsupported());
        assert_eq!(t.label(), "unsupported(interval)");
    }

    #[test]
    fn json_serialization_uses_kind_tag() {
        let t = RivetType::Decimal {
            precision: 10,
            scale: 3,
        };
        let json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&t).expect("serialize")).expect("parse");
        assert_eq!(json["kind"], "decimal");
        assert_eq!(json["precision"], 10);
        assert_eq!(json["scale"], 3);
    }

    #[test]
    fn time_unit_labels_are_stable() {
        assert_eq!(TimeUnit::Second.label(), "second");
        assert_eq!(TimeUnit::Millisecond.label(), "millisecond");
        assert_eq!(TimeUnit::Microsecond.label(), "microsecond");
        assert_eq!(TimeUnit::Nanosecond.label(), "nanosecond");
    }
}
