//! Target-type resolver (ADR-0014 L4, roadmap §16).
//!
//! Given a column's canonical [`RivetType`] and a runtime-chosen
//! [`ExportTarget`], resolve what the column becomes in a downstream
//! warehouse: the **native** target type (full fidelity), the **autoload**
//! type a generic Parquet reader infers without a declared schema, a safety
//! [`TargetStatus`], a note, and an optional materialization (`cast_sql` /
//! load-schema hint).
//!
//! Design (locked in the type-support architecture review):
//!
//! - **Dispatch on `RivetType`, never the physical Arrow type.** The previous
//!   `bq_compat` matched on `arrow::DataType` and so was blind to `json` /
//!   `uuid` / `enum` (all `Utf8` / `FixedSizeBinary` by then) and hard-failed
//!   UUID. The resolver keys off the semantic type; Arrow is consulted only
//!   for decimal precision (and `RivetType::Decimal` already carries `p,s`).
//! - **Total & infallible.** Every `(RivetType, ExportTarget)` pair yields a
//!   populated [`TargetColumnSpec`]; an unmappable column is a `status: Fail`
//!   row, never an `Err`. This keeps the type-report table and `--json` shape
//!   stable.
//! - **`autoload_type` tells the truth.** It encodes the *empirically
//!   verified* behavior of each target's Parquet autoloader — notably that
//!   BigQuery autoload degrades Parquet `JsonType`/`UUIDType` to `BYTES`,
//!   `isAdjustedToUTC=false` timestamps to `TIMESTAMP` (not `DATETIME`), and
//!   3-level lists to `REPEATED RECORD{item}`. DuckDB honors all of them.
//!
//! BigQuery numeric limits (as of 2025):
//!   NUMERIC    — precision 1–29, scale 0–9
//!   BIGNUMERIC — precision 1–76, scale 0–38

use arrow::datatypes::DataType;
use serde::Serialize;

use super::{RivetType, TypeFidelity, TypeMapping};

/// A supported downstream warehouse target. Closed, in-tree, contract-tested
/// set; chosen at runtime from `--target X`, one per run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportTarget {
    /// Reference consumer of Rivet Parquet — honors every native logical type
    /// (JSON, UUID, decimal, list) on autoload.
    DuckDb,
    /// Cloud warehouse. Its Parquet autoloader is weaker than DuckDB's: see
    /// the per-type `autoload_type` notes in [`bigquery`].
    BigQuery,
}

impl ExportTarget {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "bigquery" | "bq" => Some(Self::BigQuery),
            "duckdb" | "duck" => Some(Self::DuckDb),
            _ => None,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::BigQuery => "bigquery",
            Self::DuckDb => "duckdb",
        }
    }

    /// Resolve one already-mapped column against this target.
    pub fn resolve_column(self, input: TargetInput<'_>) -> TargetColumnSpec {
        let mut spec = match self {
            ExportTarget::BigQuery => bigquery::resolve(&input),
            ExportTarget::DuckDb => duckdb::resolve(&input),
        };
        // Fidelity floor (ADR-0014 T6): the target status must not be rosier
        // than the source fidelity warrants — a lossy/unsupported source column
        // can never resolve to a clean `Ok`.
        if input.fidelity.is_unsafe_for_strict_mode() && spec.status == TargetStatus::Ok {
            spec.status = TargetStatus::Warn;
        }
        spec
    }

    /// Resolve a whole table's worth of columns, one spec per column in order.
    /// This is the dominant entry point (type-report, future plan-load).
    #[allow(dead_code)] // public L4 entry point; consumed by plan-load (Phase B)
    pub fn resolve_table(self, mappings: &[TypeMapping]) -> Vec<TargetColumnSpec> {
        mappings
            .iter()
            .map(|m| self.resolve_column(TargetInput::from(m)))
            .collect()
    }
}

/// Status of a column's resolution against a specific target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetStatus {
    Ok,
    Warn,
    Fail,
}

impl TargetStatus {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Warn => "warn",
            Self::Fail => "fail",
        }
    }
}

/// The borrowed subset a resolver is allowed to read. Built from a
/// [`TypeMapping`] via [`From`]. Dispatch is on `rivet_type`; `arrow_type` is
/// retained for callers that want it but the resolver reads precision from
/// `RivetType::Decimal` directly.
#[derive(Debug, Clone, Copy)]
pub struct TargetInput<'a> {
    pub column_name: &'a str,
    pub rivet_type: &'a RivetType,
    /// Retained for callers and future precision-sensitive targets; the
    /// resolver reads precision from `RivetType::Decimal` directly today.
    #[allow(dead_code)]
    pub arrow_type: Option<&'a DataType>,
    pub fidelity: TypeFidelity,
}

impl<'a> From<&'a TypeMapping> for TargetInput<'a> {
    fn from(m: &'a TypeMapping) -> Self {
        TargetInput {
            column_name: &m.column_name,
            rivet_type: &m.rivet_type,
            arrow_type: m.arrow_type.as_ref(),
            fidelity: m.fidelity,
        }
    }
}

/// One column's per-target materialization spec (ADR-0014 L4). Uniform across
/// targets so the type-report table and `--json` stay stable; an unmappable
/// column is a `status: Fail` row, not an error.
#[derive(Debug, Clone, Serialize)]
pub struct TargetColumnSpec {
    /// Name copied through so a `Vec<TargetColumnSpec>` is self-describing.
    pub column_name: String,
    /// Native warehouse type for full fidelity, e.g. "JSON", "UBIGINT", "NUMERIC".
    pub target_type: String,
    /// Type a generic Parquet reader infers without a declared schema. May
    /// differ from `target_type` (e.g. BigQuery autoloads JSON as "BYTES").
    pub autoload_type: String,
    pub status: TargetStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// Materialization snippet / load-schema hint (L5) to recover the native
    /// type when autoload diverges. `None` when `autoload_type == target_type`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cast_sql: Option<String>,
}

/// Internal per-type resolution result, before the column name and cast
/// substitution are applied.
struct Resolved {
    target_type: String,
    autoload_type: String,
    status: TargetStatus,
    note: Option<String>,
    /// `cast_sql` template with a `{col}` placeholder, or `None`.
    cast: Option<String>,
}

impl Resolved {
    fn ok(t: impl Into<String>) -> Self {
        let t = t.into();
        Self {
            autoload_type: t.clone(),
            target_type: t,
            status: TargetStatus::Ok,
            note: None,
            cast: None,
        }
    }
    /// Native type that *autoloads as something else* — the divergence the
    /// resolver exists to surface.
    fn diverge(
        native: impl Into<String>,
        autoload: impl Into<String>,
        note: impl Into<String>,
        cast: Option<&str>,
    ) -> Self {
        Self {
            target_type: native.into(),
            autoload_type: autoload.into(),
            status: TargetStatus::Warn,
            note: Some(note.into()),
            cast: cast.map(str::to_string),
        }
    }
    fn warn(t: impl Into<String>, note: impl Into<String>) -> Self {
        let t = t.into();
        Self {
            autoload_type: t.clone(),
            target_type: t,
            status: TargetStatus::Warn,
            note: Some(note.into()),
            cast: None,
        }
    }
    fn fail(note: impl Into<String>) -> Self {
        Self {
            target_type: "-".into(),
            autoload_type: "-".into(),
            status: TargetStatus::Fail,
            note: Some(note.into()),
            cast: None,
        }
    }
    fn into_spec(self, input: &TargetInput<'_>) -> TargetColumnSpec {
        TargetColumnSpec {
            column_name: input.column_name.to_string(),
            target_type: self.target_type,
            autoload_type: self.autoload_type,
            status: self.status,
            note: self.note,
            cast_sql: self.cast.map(|t| t.replace("{col}", input.column_name)),
        }
    }
}

fn unsupported_reason(t: &RivetType) -> String {
    match t {
        RivetType::Unsupported { reason, .. } => reason.clone(),
        _ => "no target mapping".into(),
    }
}

// ── BigQuery ─────────────────────────────────────────────────────────────────

mod bigquery {
    use super::*;

    /// BigQuery NUMERIC precision/scale limits.
    const NUMERIC_MAX_P: u8 = 29;
    const NUMERIC_MAX_S: i8 = 9;
    /// BigQuery BIGNUMERIC precision/scale limits.
    const BIGNUMERIC_MAX_P: u8 = 76;
    const BIGNUMERIC_MAX_S: i8 = 38;

    pub(super) fn resolve(input: &TargetInput<'_>) -> TargetColumnSpec {
        native(input.rivet_type).into_spec(input)
    }

    fn native(t: &RivetType) -> Resolved {
        match t {
            RivetType::Bool => Resolved::ok("BOOL"),
            RivetType::Int16 | RivetType::Int32 | RivetType::Int64 => Resolved::ok("INT64"),
            // u64 > 2^63-1 cannot ride as INT64; recommend NUMERIC, but a bare
            // Parquet UINT64 autoloads as INT64 and overflows.
            // No post-load cast: once a u64 > INT64_MAX autoloads as INT64 it
            // has already overflowed, so recovery is only possible by declaring
            // NUMERIC in the load schema (L5), not a SELECT-time cast.
            RivetType::UInt64 => Resolved::diverge(
                "NUMERIC",
                "INT64",
                "UINT64 has no BigQuery type; values > INT64_MAX overflow on INT64 autoload — \
                 declare NUMERIC in the load schema",
                None,
            ),
            RivetType::Float32 | RivetType::Float64 => Resolved::ok("FLOAT64"),
            RivetType::Decimal { precision, scale } => decimal(*precision, *scale),
            RivetType::Date => Resolved::ok("DATE"),
            RivetType::Time { .. } => Resolved::ok("TIME"),
            // tz-aware timestamp → instant → TIMESTAMP, autoloads cleanly.
            RivetType::Timestamp {
                timezone: Some(_), ..
            } => Resolved::ok("TIMESTAMP"),
            // naive timestamp → wall-clock → DATETIME, but BigQuery autoload
            // ignores Parquet isAdjustedToUTC=false and yields TIMESTAMP
            // (verified). Declare DATETIME to preserve wall-clock semantics.
            RivetType::Timestamp { timezone: None, .. } => Resolved::diverge(
                "DATETIME",
                "TIMESTAMP",
                "naive timestamp autoloads as TIMESTAMP (an instant); declare DATETIME in the \
                 load schema to keep wall-clock semantics",
                None,
            ),
            RivetType::String | RivetType::Text | RivetType::Enum => Resolved::ok("STRING"),
            RivetType::Binary => Resolved::ok("BYTES"),
            // Parquet JSON logical type autoloads as BYTES in BigQuery
            // (verified). Declare JSON in the load schema for native JSON.
            RivetType::Json => Resolved::diverge(
                "JSON",
                "BYTES",
                "Parquet JSON logical type autoloads as BYTES in BigQuery; declare JSON in the \
                 load schema",
                Some("PARSE_JSON(SAFE_CONVERT_BYTES_TO_STRING({col}))"),
            ),
            // UUID rides as FixedSizeBinary(16) + UUIDType; BigQuery has no UUID
            // type and autoloads it as 16-byte BYTES (verified). Native is
            // STRING (canonical text).
            RivetType::Uuid => Resolved::diverge(
                "STRING",
                "BYTES",
                "UUID autoloads as 16-byte BYTES in BigQuery; declare STRING and format, \
                 or keep BYTES",
                Some("TO_HEX({col})"),
            ),
            RivetType::Interval => Resolved::ok("STRING"),
            RivetType::List { inner } => list(inner),
            RivetType::Unsupported { .. } => Resolved::fail(unsupported_reason(t)),
        }
    }

    fn decimal(p: u8, s: i8) -> Resolved {
        if s < 0 {
            return Resolved::fail(format!(
                "BigQuery has no negative scale; decimal({p},{s}) needs a STRING/INT64 cast"
            ));
        }
        let native = if p <= NUMERIC_MAX_P && s <= NUMERIC_MAX_S {
            "NUMERIC"
        } else if p <= BIGNUMERIC_MAX_P && s <= BIGNUMERIC_MAX_S {
            "BIGNUMERIC"
        } else {
            return Resolved::fail(format!(
                "decimal({p},{s}) exceeds BigQuery BIGNUMERIC limits (max 76,38)"
            ));
        };
        Resolved::ok(native)
    }

    fn list(inner: &RivetType) -> Resolved {
        let inner_r = native(inner);
        if inner_r.status == TargetStatus::Fail {
            return Resolved::fail(format!("REPEATED of unsupported element: {}", inner_r.target_type));
        }
        // Arrow 3-level lists autoload as REPEATED RECORD{item ...} in BigQuery
        // (verified); declare REPEATED <element> in the load schema for a clean
        // array.
        Resolved::diverge(
            format!("REPEATED {}", inner_r.target_type),
            format!("REPEATED RECORD{{item {}}}", inner_r.autoload_type),
            "arrays autoload as REPEATED RECORD{item …}; declare REPEATED <type> in the load schema",
            None,
        )
    }
}

// ── DuckDB ───────────────────────────────────────────────────────────────────

mod duckdb {
    use super::*;

    pub(super) fn resolve(input: &TargetInput<'_>) -> TargetColumnSpec {
        native(input.rivet_type).into_spec(input)
    }

    /// DuckDB honors every native Parquet logical type Rivet writes, so
    /// `autoload_type == target_type` for all supported variants (verified).
    fn native(t: &RivetType) -> Resolved {
        match t {
            RivetType::Bool => Resolved::ok("BOOLEAN"),
            RivetType::Int16 => Resolved::ok("SMALLINT"),
            RivetType::Int32 => Resolved::ok("INTEGER"),
            RivetType::Int64 => Resolved::ok("BIGINT"),
            RivetType::UInt64 => Resolved::ok("UBIGINT"),
            RivetType::Float32 => Resolved::ok("FLOAT"),
            RivetType::Float64 => Resolved::ok("DOUBLE"),
            RivetType::Decimal { precision, scale } => {
                if *scale < 0 {
                    Resolved::warn(
                        "DECIMAL",
                        format!("DuckDB has no negative scale; decimal({precision},{scale}) loads via cast"),
                    )
                } else if *precision <= 38 {
                    Resolved::ok(format!("DECIMAL({precision},{scale})"))
                } else {
                    // DuckDB DECIMAL maxes at precision 38; wider rides as HUGEINT/DOUBLE.
                    Resolved::warn(
                        "DECIMAL(38,*)",
                        format!("decimal({precision},{scale}) exceeds DuckDB DECIMAL(38); widens"),
                    )
                }
            }
            RivetType::Date => Resolved::ok("DATE"),
            RivetType::Time { .. } => Resolved::ok("TIME"),
            RivetType::Timestamp {
                timezone: Some(_), ..
            } => Resolved::ok("TIMESTAMPTZ"),
            RivetType::Timestamp { timezone: None, .. } => Resolved::ok("TIMESTAMP"),
            RivetType::String | RivetType::Text | RivetType::Enum => Resolved::ok("VARCHAR"),
            RivetType::Binary => Resolved::ok("BLOB"),
            RivetType::Json => Resolved::ok("JSON"),
            RivetType::Uuid => Resolved::ok("UUID"),
            RivetType::Interval => Resolved::ok("INTERVAL"),
            RivetType::List { inner } => {
                let inner_r = native(inner);
                if inner_r.status == TargetStatus::Fail {
                    Resolved::fail(format!("LIST of unsupported element: {}", inner_r.target_type))
                } else {
                    Resolved::ok(format!("{}[]", inner_r.target_type))
                }
            }
            RivetType::Unsupported { .. } => Resolved::fail(unsupported_reason(t)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input<'a>(rt: &'a RivetType) -> TargetInput<'a> {
        TargetInput {
            column_name: "c",
            rivet_type: rt,
            arrow_type: None,
            fidelity: TypeFidelity::Exact,
        }
    }

    fn bq(rt: &RivetType) -> TargetColumnSpec {
        ExportTarget::BigQuery.resolve_column(input(rt))
    }
    fn duck(rt: &RivetType) -> TargetColumnSpec {
        ExportTarget::DuckDb.resolve_column(input(rt))
    }

    // ── dispatch on RivetType, not Arrow — the headline fix ──────────────────

    #[test]
    fn bq_uuid_resolves_not_fails() {
        // The old arrow-dispatch `bq_compat` hard-failed UUID (FixedSizeBinary
        // had no arm). Now it resolves on RivetType: native STRING, BYTES on
        // autoload.
        let s = bq(&RivetType::Uuid);
        assert_eq!(s.target_type, "STRING");
        assert_eq!(s.autoload_type, "BYTES");
        assert_eq!(s.status, TargetStatus::Warn);
        assert!(s.cast_sql.unwrap().contains("c"));
    }

    #[test]
    fn bq_json_native_is_json_autoload_is_bytes() {
        let s = bq(&RivetType::Json);
        assert_eq!(s.target_type, "JSON");
        assert_eq!(s.autoload_type, "BYTES");
        assert_eq!(s.status, TargetStatus::Warn);
        assert!(s.cast_sql.unwrap().starts_with("PARSE_JSON"));
    }

    #[test]
    fn bq_naive_timestamp_is_datetime_native_timestamp_autoload() {
        let naive = RivetType::Timestamp {
            unit: super::super::TimeUnit::Microsecond,
            timezone: None,
        };
        let s = bq(&naive);
        assert_eq!(s.target_type, "DATETIME");
        assert_eq!(s.autoload_type, "TIMESTAMP");
        assert_eq!(s.status, TargetStatus::Warn);
    }

    #[test]
    fn bq_tz_timestamp_is_timestamp_ok() {
        let tz = RivetType::Timestamp {
            unit: super::super::TimeUnit::Microsecond,
            timezone: Some("UTC".into()),
        };
        let s = bq(&tz);
        assert_eq!(s.target_type, "TIMESTAMP");
        assert_eq!(s.autoload_type, "TIMESTAMP");
        assert_eq!(s.status, TargetStatus::Ok);
    }

    #[test]
    fn bq_decimal_within_numeric_is_numeric() {
        let s = bq(&RivetType::Decimal { precision: 18, scale: 2 });
        assert_eq!(s.target_type, "NUMERIC");
        assert_eq!(s.status, TargetStatus::Ok);
    }

    #[test]
    fn bq_decimal_escalates_to_bignumeric() {
        let s = bq(&RivetType::Decimal { precision: 38, scale: 9 });
        assert_eq!(s.target_type, "BIGNUMERIC");
        assert_eq!(s.status, TargetStatus::Ok);
    }

    #[test]
    fn bq_decimal_negative_scale_fails() {
        let s = bq(&RivetType::Decimal { precision: 5, scale: -2 });
        assert_eq!(s.status, TargetStatus::Fail);
    }

    #[test]
    fn bq_uint64_recommends_numeric_warns_overflow() {
        let s = bq(&RivetType::UInt64);
        assert_eq!(s.target_type, "NUMERIC");
        assert_eq!(s.autoload_type, "INT64");
        assert_eq!(s.status, TargetStatus::Warn);
    }

    #[test]
    fn bq_list_is_repeated_native_record_autoload() {
        let t = RivetType::List { inner: Box::new(RivetType::String) };
        let s = bq(&t);
        assert_eq!(s.target_type, "REPEATED STRING");
        assert!(s.autoload_type.contains("REPEATED RECORD"));
        assert_eq!(s.status, TargetStatus::Warn);
    }

    #[test]
    fn bq_unsupported_is_fail_row_not_panic() {
        let t = RivetType::Unsupported {
            native_type: "geometry".into(),
            reason: "no mapping".into(),
        };
        let s = bq(&t);
        assert_eq!(s.status, TargetStatus::Fail);
        assert_eq!(s.target_type, "-");
    }

    #[test]
    fn bq_standard_scalars_ok() {
        for (rt, native) in [
            (RivetType::Bool, "BOOL"),
            (RivetType::Int64, "INT64"),
            (RivetType::Float64, "FLOAT64"),
            (RivetType::Date, "DATE"),
            (RivetType::String, "STRING"),
            (RivetType::Binary, "BYTES"),
            (RivetType::Enum, "STRING"),
        ] {
            let s = bq(&rt);
            assert_eq!(s.target_type, native, "{rt:?}");
            assert_eq!(s.autoload_type, native, "{rt:?}");
            assert_eq!(s.status, TargetStatus::Ok, "{rt:?}");
        }
    }

    // ── DuckDB honors every logical type — autoload == native ────────────────

    #[test]
    fn duckdb_reads_everything_natively() {
        let naive = RivetType::Timestamp {
            unit: super::super::TimeUnit::Microsecond,
            timezone: None,
        };
        for rt in [
            RivetType::Json,
            RivetType::Uuid,
            RivetType::UInt64,
            naive,
            RivetType::List { inner: Box::new(RivetType::Int64) },
        ] {
            let s = duck(&rt);
            assert_eq!(
                s.target_type, s.autoload_type,
                "DuckDB autoload must equal native for {rt:?}"
            );
            assert_ne!(s.status, TargetStatus::Fail, "{rt:?}");
        }
    }

    #[test]
    fn duckdb_native_type_names() {
        assert_eq!(duck(&RivetType::Json).target_type, "JSON");
        assert_eq!(duck(&RivetType::Uuid).target_type, "UUID");
        assert_eq!(duck(&RivetType::UInt64).target_type, "UBIGINT");
        assert_eq!(
            duck(&RivetType::Decimal { precision: 18, scale: 2 }).target_type,
            "DECIMAL(18,2)"
        );
        assert_eq!(
            duck(&RivetType::List { inner: Box::new(RivetType::Int64) }).target_type,
            "BIGINT[]"
        );
    }

    #[test]
    fn parse_accepts_aliases() {
        assert_eq!(ExportTarget::parse("bq"), Some(ExportTarget::BigQuery));
        assert_eq!(ExportTarget::parse("BigQuery"), Some(ExportTarget::BigQuery));
        assert_eq!(ExportTarget::parse("duckdb"), Some(ExportTarget::DuckDb));
        assert_eq!(ExportTarget::parse("nope"), None);
    }

    #[test]
    fn resolve_table_preserves_order_and_names() {
        use super::super::SourceColumn;
        let mappings = vec![
            TypeMapping::from_source(&SourceColumn::simple("a", "int8", true), RivetType::Int64),
            TypeMapping::from_source(&SourceColumn::simple("b", "jsonb", true), RivetType::Json),
        ];
        let specs = ExportTarget::BigQuery.resolve_table(&mappings);
        assert_eq!(specs.len(), 2);
        assert_eq!(specs[0].column_name, "a");
        assert_eq!(specs[1].column_name, "b");
        assert_eq!(specs[1].target_type, "JSON");
    }

    // ── edge cases: remediation hints must recover from the DEGRADED state ────
    // Regression guard for the bug class where the resolver proposes a post-load
    // cast that cannot actually recover an already-lossy value (e.g. a UINT64
    // that overflowed into INT64). See CLAUDE.md "Remediation hints must recover
    // from the degraded state".

    #[test]
    fn cast_sql_is_none_when_post_load_recovery_is_impossible() {
        // UINT64 > INT64_MAX has already overflowed by the time it autoloads as
        // INT64; a SELECT-time cast would operate on corrupted data. Recovery is
        // load-schema only — cast_sql MUST be None and the note point at it.
        let u = bq(&RivetType::UInt64);
        assert!(
            u.cast_sql.is_none(),
            "overflowed UINT64 has no lossless post-load recovery"
        );
        assert!(u.note.unwrap().to_lowercase().contains("load schema"));

        // A naive timestamp loses its wall-clock identity at autoload (becomes an
        // instant); the micros are reinterpreted, not recoverable by a cast.
        let naive = RivetType::Timestamp {
            unit: super::super::TimeUnit::Microsecond,
            timezone: None,
        };
        let t = bq(&naive);
        assert!(
            t.cast_sql.is_none(),
            "naive timestamp has no lossless post-load recovery"
        );
        assert!(t.note.unwrap().to_lowercase().contains("load schema"));
    }

    #[test]
    fn cast_sql_present_only_when_lossless_post_load() {
        // JSON/UUID autoload as BYTES but the bytes still hold the value
        // losslessly, so a post-load cast genuinely recovers it.
        assert!(bq(&RivetType::Json).cast_sql.unwrap().contains("PARSE_JSON"));
        assert!(bq(&RivetType::Uuid).cast_sql.unwrap().contains("TO_HEX"));
    }

    #[test]
    fn every_divergence_offers_a_recovery_path() {
        // Invariant: whenever BigQuery autoload diverges from the native type the
        // operator is given SOME recovery — a lossless post-load `cast_sql`, or a
        // note pointing at the load schema. Never a silent no-op (the bug class).
        let naive = RivetType::Timestamp {
            unit: super::super::TimeUnit::Microsecond,
            timezone: None,
        };
        let cases = [
            RivetType::Json,
            RivetType::Uuid,
            RivetType::UInt64,
            naive,
            RivetType::List {
                inner: Box::new(RivetType::String),
            },
        ];
        for rt in cases {
            let s = bq(&rt);
            assert_ne!(s.autoload_type, s.target_type, "case must diverge: {rt:?}");
            let has_cast = s.cast_sql.is_some();
            let points_to_load_schema = s
                .note
                .as_deref()
                .map(|n| n.to_lowercase().contains("load schema"))
                .unwrap_or(false);
            assert!(
                has_cast || points_to_load_schema,
                "divergent {rt:?} must offer a recovery (cast_sql or load-schema note)"
            );
        }
    }

    // ── edge cases: decimal precision/scale overflow at the target boundary ───

    #[test]
    fn bq_decimal_limit_boundaries() {
        // Exact BIGNUMERIC ceiling is ok.
        assert_eq!(
            bq(&RivetType::Decimal { precision: 76, scale: 38 }).status,
            TargetStatus::Ok
        );
        // One past precision overflows BIGNUMERIC → Fail, never a silent clamp.
        assert_eq!(
            bq(&RivetType::Decimal { precision: 77, scale: 38 }).status,
            TargetStatus::Fail
        );
        // One past scale → Fail.
        assert_eq!(
            bq(&RivetType::Decimal { precision: 76, scale: 39 }).status,
            TargetStatus::Fail
        );
        // Between NUMERIC and BIGNUMERIC escalates rather than overflowing NUMERIC.
        assert_eq!(
            bq(&RivetType::Decimal { precision: 30, scale: 0 }).target_type,
            "BIGNUMERIC"
        );
    }

    #[test]
    fn duckdb_decimal_over_38_warns_not_silently_clamps() {
        let s = duck(&RivetType::Decimal { precision: 40, scale: 2 });
        assert_eq!(s.status, TargetStatus::Warn);
    }
}
