//! `RivetType` → `arrow::DataType` + Arrow metadata.
//!
//! See `rivet_roadmap.md` §Epic 14. §5 — Type Mapping Pipeline, §14 —
//! ("Binary, UUID, JSON" — metadata example). This module is intentionally
//! the *only* place where `RivetType` becomes an `arrow::DataType`. Source
//! drivers must not poke at `arrow::DataType` directly any more — they
//! produce a [`SourceColumn`], call into a vendor-specific
//! `<vendor>_to_rivet()` (Chunks 2/3), then hand the resulting
//! [`TypeMapping`] to [`build_arrow_field`] here.
//!
//! Why funnel everything through one function:
//!
//! - It guarantees the metadata key set ([`META_NATIVE_TYPE`],
//!   [`META_LOGICAL_TYPE`], [`META_FIDELITY`]) is identical regardless of
//!   the source database, so downstream consumers (e.g. BigQuery target
//!   check) can rely on it.
//! - It keeps Arrow as a *target language*, not a public API — Chunks 4–8
//!   add policy and overrides without each one needing to know about
//!   `Field::with_metadata`.

use std::collections::HashMap;

use arrow::datatypes::{DataType, Field, TimeUnit as ArrowTimeUnit};
use arrow_schema::extension::{Json as ArrowJson, Uuid as ArrowUuid};
use serde::Serialize;
use std::sync::Arc;

use super::{RivetType, SourceColumn, TimeUnit, TypeFidelity};

/// Arrow field-metadata key carrying the native database type name.
/// Read by the type-report CLI (Chunk 5) and by future BigQuery / Snowflake
/// target checks so they can produce hints like
/// "this column came from `numeric(18,2)`".
pub const META_NATIVE_TYPE: &str = "rivet.native_type";
/// Arrow field-metadata key carrying the Rivet logical type — used for
/// types whose physical Arrow representation is `Utf8` but whose semantic
/// type is recoverable (e.g. `json`, `uuid`).
pub const META_LOGICAL_TYPE: &str = "rivet.logical_type";
/// Arrow field-metadata key carrying the [`TypeFidelity`] label.
/// CI / strict-mode tooling can sniff this to assert that no field in a
/// produced Parquet schema is `lossy` or `unsupported`.
pub const META_FIDELITY: &str = "rivet.fidelity";

/// One row of the Type Mapping Pipeline (roadmap §6 `TypeMapping`).
///
/// Carries the full provenance from a source-DB column to its eventual
/// Arrow representation. The struct is what the type-report CLI prints
/// and what `TypePolicy` validates against.
#[derive(Debug, Clone, Serialize)]
pub struct TypeMapping {
    /// Column name (matches `SourceColumn::name`).
    pub column_name: String,
    /// Native source-DB type identifier (`numeric(18,2)`, `timestamptz`,
    /// `jsonb`, …).
    pub source_native_type: String,
    /// The canonical Rivet type produced by the vendor mapper.
    pub rivet_type: RivetType,
    /// Resolved Arrow type, or `None` for [`RivetType::Unsupported`] until
    /// a policy turns it into something exportable.
    ///
    /// Kept as `arrow::DataType` (not a stringly-typed name) so the
    /// pipeline can build an `arrow::Schema` directly from a
    /// `Vec<TypeMapping>`.
    #[serde(serialize_with = "serialize_arrow_type_opt")]
    pub arrow_type: Option<DataType>,
    /// Fidelity classification — see [`TypeFidelity`].
    pub fidelity: TypeFidelity,
    /// True when the source schema declares the column nullable. Threaded
    /// from `SourceColumn::nullable` so [`build_arrow_field`] doesn't need
    /// the original column.
    pub nullable: bool,
    /// Diagnostic strings emitted by the mapper or the policy. Surfaced by
    /// the type-report and the strict-mode failure message.
    pub warnings: Vec<String>,
}

impl TypeMapping {
    /// Build a mapping from a [`SourceColumn`] and an already-resolved
    /// [`RivetType`]. The Arrow type is computed by [`rivet_type_to_arrow`]
    /// and the fidelity by [`derive_fidelity`].
    ///
    /// This is the canonical constructor used by every vendor mapper —
    /// Chunks 2/3 will call this once per source column.
    pub fn from_source(source: &SourceColumn, rivet_type: RivetType) -> Self {
        let fidelity = derive_fidelity(&rivet_type);
        let arrow_type = rivet_type_to_arrow(&rivet_type);
        Self {
            column_name: source.name.clone(),
            source_native_type: source.native_type.clone(),
            rivet_type,
            arrow_type,
            fidelity,
            nullable: source.nullable,
            warnings: Vec::new(),
        }
    }

    /// Append a warning visible to the type-report and to logs.
    #[allow(dead_code)]
    pub fn with_warning(mut self, msg: impl Into<String>) -> Self {
        self.warnings.push(msg.into());
        self
    }
}

fn serialize_arrow_type_opt<S: serde::Serializer>(
    v: &Option<DataType>,
    s: S,
) -> std::result::Result<S::Ok, S::Error> {
    match v {
        None => s.serialize_none(),
        Some(dt) => s.serialize_some(&format!("{dt:?}")),
    }
}

/// Map [`RivetType`] → [`arrow::DataType`].
///
/// This is the *only* place where Arrow types are constructed from Rivet
/// types. Source drivers must not duplicate this logic; they go through
/// [`TypeMapping::from_source`] instead.
///
/// Returns `None` for [`RivetType::Unsupported`] — the policy layer
/// (Chunk 4) is responsible for either failing the run or rewriting the
/// `RivetType` into something supported (e.g. `Unsupported -> String`).
pub fn rivet_type_to_arrow(t: &RivetType) -> Option<DataType> {
    match t {
        RivetType::Bool => Some(DataType::Boolean),
        RivetType::Int16 => Some(DataType::Int16),
        RivetType::Int32 => Some(DataType::Int32),
        RivetType::Int64 => Some(DataType::Int64),
        RivetType::UInt64 => Some(DataType::UInt64),
        RivetType::Float32 => Some(DataType::Float32),
        RivetType::Float64 => Some(DataType::Float64),
        RivetType::Decimal { precision, scale } => Some(decimal_arrow_type(*precision, *scale)),
        RivetType::Date => Some(DataType::Date32),
        RivetType::Time { unit } => Some(DataType::Time64(arrow_unit(*unit))),
        RivetType::Timestamp { unit, timezone } => Some(DataType::Timestamp(
            arrow_unit(*unit),
            timezone.as_deref().map(Into::into),
        )),
        // UUID: 16-byte canonical binary representation. Paired with the
        // `arrow.uuid` extension type metadata in `build_arrow_field`, this
        // lets parquet-rs emit native `LogicalType::Uuid` instead of
        // `String`. Downstream Parquet readers (DuckDB / ClickHouse /
        // pyarrow / BigQuery autodetect) recognise UUID natively without
        // an extra cast.
        RivetType::Uuid => Some(DataType::FixedSizeBinary(16)),

        // Logical-string types: physical Arrow is Utf8; the metadata
        // attached by `build_arrow_field` records that the source meant
        // something more specific (json/enum).
        RivetType::String | RivetType::Text | RivetType::Json | RivetType::Enum => {
            Some(DataType::Utf8)
        }

        RivetType::Binary => Some(DataType::Binary),

        // Interval → Utf8 (ISO 8601 duration string, e.g. "P1Y2M3D").
        // Arrow's Interval(MonthDayNano) cannot be written to Parquet, so we
        // serialise to a lossless text representation in the source driver.
        RivetType::Interval => Some(DataType::Utf8),

        // One-dimensional array: recursively resolve the inner element type.
        // Returns None if the inner type itself is Unsupported.
        RivetType::List { inner } => rivet_type_to_arrow(inner)
            .map(|inner_dt| DataType::List(Arc::new(Field::new("item", inner_dt, true)))),

        RivetType::Unsupported { .. } => None,
    }
}

/// Decimal128 vs Decimal256 selection per roadmap §12 ("Exact Decimal Support"):
/// `Decimal128(p,s)` when `p <= 38`, `Decimal256(p,s)` otherwise.
///
/// Negative scale is allowed by PostgreSQL `numeric(p,-s)` and by ARROW, so it is
/// forwarded through unchanged here. But the PARQUET decimal logical type requires
/// `scale >= 0`, so a negative-scale column is NOT Parquet-writable — the parquet
/// writer refuses it up front (see `ParquetFormat::create_writer`) with a clear
/// error rather than letting `check` pass and `run` fail mid-export. (CSV renders
/// it fine as text, so the reject is parquet-format-specific, not a type-level one.)
fn decimal_arrow_type(precision: u8, scale: i8) -> DataType {
    if precision <= 38 {
        DataType::Decimal128(precision, scale)
    } else {
        DataType::Decimal256(precision, scale)
    }
}

fn arrow_unit(u: TimeUnit) -> ArrowTimeUnit {
    match u {
        TimeUnit::Second => ArrowTimeUnit::Second,
        TimeUnit::Millisecond => ArrowTimeUnit::Millisecond,
        TimeUnit::Microsecond => ArrowTimeUnit::Microsecond,
        TimeUnit::Nanosecond => ArrowTimeUnit::Nanosecond,
    }
}

/// Compute the [`TypeFidelity`] for a freshly-resolved [`RivetType`].
///
/// The output of every vendor mapper goes through this so the fidelity
/// label is computed in *exactly one place* and the type-report stays
/// consistent across PostgreSQL / MySQL / future drivers.
pub fn derive_fidelity(t: &RivetType) -> TypeFidelity {
    match t {
        RivetType::Bool
        | RivetType::Int16
        | RivetType::Int32
        | RivetType::Int64
        | RivetType::UInt64
        | RivetType::Float32
        | RivetType::Float64
        | RivetType::Decimal { .. }
        | RivetType::Date
        | RivetType::Time { .. }
        | RivetType::Timestamp { .. }
        | RivetType::String
        | RivetType::Text
        | RivetType::Binary => TypeFidelity::Exact,

        // UUID now exports as canonical `FixedSizeBinary(16)` + the
        // `arrow.uuid` extension type (parquet-rs emits native
        // `LogicalType::Uuid`). Both value and physical type match the
        // canonical Parquet representation — `Exact`. Bumped from
        // `Compatible` when we still wrote the hyphenated text.
        RivetType::Uuid => TypeFidelity::Exact,

        // JSON is preserved byte-for-byte but its native semantics
        // (object/array tree) are not — call it `logical_string`.
        RivetType::Json => TypeFidelity::LogicalString,

        // Enum labels are text — value preserved, but native enum semantics
        // (ordered labels, constraint) are not enforced in Arrow.
        RivetType::Enum => TypeFidelity::Compatible,

        // Interval: Arrow IntervalMonthDayNano preserves all three components
        // exactly; downstream tools may interpret it differently.
        RivetType::Interval => TypeFidelity::Compatible,

        // List: Arrow List preserves element values (1-D only currently), but a
        // list is never *better* than its element — and never worse than
        // `Compatible` for a supported element. A list of an **Unsupported**
        // element (e.g. PG `numeric[]`, whose element precision can't be
        // resolved and has no override — array overrides aren't accepted) is
        // itself Unsupported: the run can't build the field, so `--strict` and
        // the type-report must say so too (otherwise `check` reports it safe
        // while `run` fails — a check↔run inconsistency). A Lossy element drags
        // the list to Lossy likewise.
        RivetType::List { inner } => match derive_fidelity(inner) {
            f @ (TypeFidelity::Unsupported | TypeFidelity::Lossy) => f,
            _ => TypeFidelity::Compatible,
        },

        RivetType::Unsupported { .. } => TypeFidelity::Unsupported,
    }
}

/// Build an `arrow::Field` from a [`TypeMapping`], attaching the standard
/// metadata keys ([`META_NATIVE_TYPE`], [`META_LOGICAL_TYPE`],
/// [`META_FIDELITY`]).
///
/// Returns `None` if the mapping has no resolved Arrow type (i.e. the
/// `RivetType` is `Unsupported` and no policy has rewritten it). Callers
/// must surface this as a type-policy decision, not as a panic.
pub fn build_arrow_field(mapping: &TypeMapping) -> Option<Field> {
    let dt = mapping.arrow_type.clone()?;
    let mut metadata: HashMap<String, String> = HashMap::new();
    metadata.insert(META_NATIVE_TYPE.into(), mapping.source_native_type.clone());
    metadata.insert(META_FIDELITY.into(), mapping.fidelity.label().into());
    if let Some(logical) = logical_type_label(&mapping.rivet_type) {
        metadata.insert(META_LOGICAL_TYPE.into(), logical.into());
    }
    let mut field = Field::new(&mapping.column_name, dt, mapping.nullable).with_metadata(metadata);

    // Attach the Arrow canonical extension type so that parquet-rs emits the
    // matching native Parquet `LogicalType` (Uuid / Json) instead of falling
    // back to `String`. The Rivet-side metadata (`rivet.logical_type=...`)
    // stays for Rivet-aware consumers; the extension type metadata is what
    // downstream generic engines (DuckDB, ClickHouse, pyarrow, BigQuery
    // autodetect) actually read.
    //
    // arrow-rs encodes the extension type as the `ARROW:extension:name`
    // metadata key on the Field; `try_with_extension_type` does the right
    // thing including any per-extension metadata (Json carries an empty
    // metadata object today).
    match mapping.rivet_type {
        RivetType::Json => {
            field
                .try_with_extension_type(ArrowJson::default())
                .expect("Json extension only valid on Utf8/LargeUtf8 — invariant in mapping");
        }
        RivetType::Uuid => {
            field
                .try_with_extension_type(ArrowUuid)
                .expect("Uuid extension only valid on FixedSizeBinary(16) — invariant in mapping");
        }
        _ => {}
    }
    Some(field)
}

/// Return the `rivet.logical_type` value for types whose physical Arrow
/// representation is a generic container (Utf8, Binary) but whose source
/// semantic is more specific (`json`, `uuid`). `None` when the physical
/// type already encodes the semantic (e.g. `Decimal128(18,2)` is already
/// "decimal" in Arrow / Parquet).
fn logical_type_label(t: &RivetType) -> Option<&'static str> {
    match t {
        RivetType::Json => Some("json"),
        RivetType::Uuid => Some("uuid"),
        RivetType::Enum => Some("enum"),
        RivetType::Interval => Some("interval"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_fidelity_propagates_unsupported_element() {
        // PG `numeric[]`: the element precision can't be resolved and array
        // overrides aren't accepted → `List<Unsupported>`. A list is never
        // better than its element, so the whole column is Unsupported —
        // `--strict` and the type-report must agree with `run`, which fails to
        // build the field. Regression: this was hardcoded `Compatible`, so
        // `check --strict` passed while `run` failed (a check↔run gap).
        let bad = RivetType::List {
            inner: Box::new(RivetType::Unsupported {
                native_type: "numeric".into(),
                reason: "precision unavailable".into(),
            }),
        };
        assert_eq!(derive_fidelity(&bad), TypeFidelity::Unsupported);
        assert!(bad.is_unsupported());

        // A list of a supported element stays `Compatible` (e.g. `int[]`).
        let good = RivetType::List {
            inner: Box::new(RivetType::Int32),
        };
        assert_eq!(derive_fidelity(&good), TypeFidelity::Compatible);
        assert!(!good.is_unsupported());
    }

    fn col(name: &str, native: &str) -> SourceColumn {
        SourceColumn::simple(name, native, true)
    }

    #[test]
    fn integer_types_map_one_to_one() {
        for (rt, expected) in [
            (RivetType::Bool, DataType::Boolean),
            (RivetType::Int16, DataType::Int16),
            (RivetType::Int32, DataType::Int32),
            (RivetType::Int64, DataType::Int64),
            (RivetType::UInt64, DataType::UInt64),
            (RivetType::Float32, DataType::Float32),
            (RivetType::Float64, DataType::Float64),
        ] {
            assert_eq!(
                rivet_type_to_arrow(&rt),
                Some(expected),
                "rivet type {rt:?}"
            );
            assert_eq!(derive_fidelity(&rt), TypeFidelity::Exact);
        }
    }

    #[test]
    fn decimal_p38_uses_decimal128() {
        for p in [1u8, 18, 38] {
            let dt = rivet_type_to_arrow(&RivetType::Decimal {
                precision: p,
                scale: 2,
            })
            .expect("decimal must map to an Arrow type");
            assert_eq!(dt, DataType::Decimal128(p, 2), "precision={p}");
        }
    }

    /// Roadmap §12: precision >38 must escalate to Decimal256, not silently
    /// truncate or fall back to Float64. This is the single most important
    /// invariant of the whole Type Safety Foundation.
    #[test]
    fn decimal_above_38_escalates_to_decimal256() {
        for p in [39u8, 76] {
            let dt = rivet_type_to_arrow(&RivetType::Decimal {
                precision: p,
                scale: 9,
            })
            .expect("decimal must map to an Arrow type");
            assert_eq!(
                dt,
                DataType::Decimal256(p, 9),
                "precision={p} must become Decimal256"
            );
        }
    }

    /// Roadmap §12: PostgreSQL `numeric(5,-2)` rounds to hundreds; the type
    /// system must round-trip the negative scale.
    #[test]
    fn decimal_supports_negative_scale_for_postgres_numeric() {
        let dt = rivet_type_to_arrow(&RivetType::Decimal {
            precision: 5,
            scale: -2,
        })
        .expect("decimal must map to an Arrow type");
        assert_eq!(dt, DataType::Decimal128(5, -2));
    }

    #[test]
    fn timestamp_preserves_timezone_semantics() {
        let naive = RivetType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: None,
        };
        let utc = RivetType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: Some("UTC".into()),
        };
        assert_eq!(
            rivet_type_to_arrow(&naive),
            Some(DataType::Timestamp(ArrowTimeUnit::Microsecond, None))
        );
        assert_eq!(
            rivet_type_to_arrow(&utc),
            Some(DataType::Timestamp(
                ArrowTimeUnit::Microsecond,
                Some("UTC".into())
            ))
        );
    }

    #[test]
    fn unsupported_returns_no_arrow_type() {
        let t = RivetType::Unsupported {
            native_type: "interval".into(),
            reason: "no mapping yet".into(),
        };
        assert_eq!(rivet_type_to_arrow(&t), None);
        assert_eq!(derive_fidelity(&t), TypeFidelity::Unsupported);
    }

    #[test]
    fn json_is_logical_string_with_metadata() {
        let mapping = TypeMapping::from_source(&col("payload", "jsonb"), RivetType::Json);
        assert_eq!(mapping.fidelity, TypeFidelity::LogicalString);
        assert_eq!(mapping.arrow_type, Some(DataType::Utf8));

        let field = build_arrow_field(&mapping).expect("field");
        assert_eq!(field.data_type(), &DataType::Utf8);
        assert_eq!(
            field.metadata().get(META_NATIVE_TYPE).map(String::as_str),
            Some("jsonb")
        );
        assert_eq!(
            field.metadata().get(META_LOGICAL_TYPE).map(String::as_str),
            Some("json")
        );
        assert_eq!(
            field.metadata().get(META_FIDELITY).map(String::as_str),
            Some("logical_string")
        );
    }

    #[test]
    fn uuid_is_exact_fixed_size_binary_with_logical_metadata() {
        // UUID exports as canonical FixedSizeBinary(16) + the `arrow.uuid`
        // extension; this combo lets parquet-rs emit native
        // `LogicalType::Uuid` (downstream engines autoload as UUID type).
        // Fidelity bumped from `compatible` to `exact` to reflect the
        // canonical Parquet representation.
        let mapping = TypeMapping::from_source(&col("id", "uuid"), RivetType::Uuid);
        assert_eq!(mapping.fidelity, TypeFidelity::Exact);
        assert_eq!(mapping.arrow_type, Some(DataType::FixedSizeBinary(16)));

        let field = build_arrow_field(&mapping).expect("field");
        assert_eq!(
            field.metadata().get(META_LOGICAL_TYPE).map(String::as_str),
            Some("uuid")
        );
        assert_eq!(
            field.metadata().get(META_FIDELITY).map(String::as_str),
            Some("exact")
        );
        // The Arrow extension key drives the Parquet native logical type.
        assert_eq!(
            field
                .metadata()
                .get("ARROW:extension:name")
                .map(String::as_str),
            Some("arrow.uuid")
        );
    }

    #[test]
    fn plain_string_has_no_logical_type_metadata() {
        let mapping = TypeMapping::from_source(&col("name", "text"), RivetType::String);
        let field = build_arrow_field(&mapping).expect("field");
        assert!(
            !field.metadata().contains_key(META_LOGICAL_TYPE),
            "plain string columns must NOT carry rivet.logical_type so consumers \
             can distinguish them from json/uuid columns"
        );
        assert_eq!(
            field.metadata().get(META_NATIVE_TYPE).map(String::as_str),
            Some("text")
        );
        assert_eq!(
            field.metadata().get(META_FIDELITY).map(String::as_str),
            Some("exact")
        );
    }

    #[test]
    fn binary_stays_binary_not_string() {
        // Roadmap §14: binary columns must never be silently exported as Utf8.
        let mapping = TypeMapping::from_source(&col("payload", "bytea"), RivetType::Binary);
        let field = build_arrow_field(&mapping).expect("field");
        assert_eq!(field.data_type(), &DataType::Binary);
        assert_eq!(mapping.fidelity, TypeFidelity::Exact);
    }

    #[test]
    fn unsupported_yields_no_field() {
        let unsupported = RivetType::Unsupported {
            native_type: "interval".into(),
            reason: "no mapping".into(),
        };
        let mapping = TypeMapping::from_source(&col("dur", "interval"), unsupported);
        assert!(
            build_arrow_field(&mapping).is_none(),
            "Unsupported must NOT silently produce a Utf8 field — that's exactly the \
             silent-degradation pattern the roadmap forbids (§5)"
        );
    }

    #[test]
    fn nullable_flag_propagates_from_source_column() {
        let nullable = SourceColumn::simple("a", "int4", true);
        let not_nullable = SourceColumn::simple("b", "int4", false);
        let m_nullable = TypeMapping::from_source(&nullable, RivetType::Int32);
        let m_required = TypeMapping::from_source(&not_nullable, RivetType::Int32);
        assert!(build_arrow_field(&m_nullable).expect("f").is_nullable());
        assert!(!build_arrow_field(&m_required).expect("f").is_nullable());
    }

    #[test]
    fn warnings_are_attachable_via_builder() {
        let mapping = TypeMapping::from_source(&col("x", "int4"), RivetType::Int32)
            .with_warning("autodetect uncertainty");
        assert_eq!(mapping.warnings, vec!["autodetect uncertainty".to_string()]);
    }
}
