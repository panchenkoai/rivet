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
    /// Cloud warehouse. Like BigQuery, its Parquet autoload degrades JSON,
    /// UUID, naive timestamps and TIME — see [`snowflake`]. Verified live.
    Snowflake,
}

impl ExportTarget {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "bigquery" | "bq" => Some(Self::BigQuery),
            "duckdb" | "duck" => Some(Self::DuckDb),
            "snowflake" | "sf" => Some(Self::Snowflake),
            _ => None,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::BigQuery => "bigquery",
            Self::DuckDb => "duckdb",
            Self::Snowflake => "snowflake",
        }
    }

    /// Resolve one already-mapped column against this target.
    pub fn resolve_column(self, input: TargetInput<'_>) -> TargetColumnSpec {
        let mut spec = match self {
            ExportTarget::BigQuery => bigquery::resolve(&input),
            ExportTarget::DuckDb => duckdb::resolve(&input),
            ExportTarget::Snowflake => snowflake::resolve(&input),
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

    /// SQL that recovers target-native types after a bare autoload degraded
    /// them (ADR-0014 L5). For BigQuery this is a `CREATE TABLE … AS SELECT`
    /// over the autoloaded staging table applying per-column casts — BigQuery's
    /// Parquet loader will NOT coerce a *declared* native type on load (verified
    /// against live BQ: BYTES→JSON, TIMESTAMP→DATETIME loads are rejected), so
    /// the recovery has to be a post-load transform, not a load schema. `None`
    /// when the target reads the interchange Parquet faithfully (DuckDB).
    pub fn recovery_sql(self, specs: &[TargetColumnSpec], table: &str) -> Option<String> {
        match self {
            ExportTarget::BigQuery => Some(bigquery_recovery_sql(specs, table)),
            ExportTarget::Snowflake => Some(snowflake_recovery_sql(specs, table)),
            ExportTarget::DuckDb => None,
        }
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

/// Emit a BigQuery type-recovery statement (ADR-0014 L5): load the interchange
/// Parquet with `--autodetect` into `<table>__staging`, then run this CTAS to
/// materialise the native types that bare autoload degrades (JSON/UUID→BYTES,
/// naive timestamp→TIMESTAMP). Columns with a `cast_sql` get that cast; the
/// rest pass through unchanged.
///
/// Verified against live BigQuery: a load schema that *declares* native types
/// is rejected (the Parquet loader won't coerce a column's type on load), so
/// the recovery must be this post-load transform.
/// The recovery `SELECT` body, shared by every degrading target. The cast
/// branch *is* the materialization contract (ADR-0014 L5) and is identical
/// across targets — only the passthrough form (identifier quoting, alias)
/// differs, so each target supplies that as `passthrough`. Deleting this and
/// inlining the fold would re-scatter the cast logic across N targets.
fn recovery_projection(specs: &[TargetColumnSpec], passthrough: impl Fn(&str) -> String) -> String {
    specs
        .iter()
        .map(|s| match &s.cast_sql {
            Some(cast) => format!("  {cast} AS {name}", name = s.column_name),
            None => passthrough(&s.column_name),
        })
        .collect::<Vec<_>>()
        .join(",\n")
}

fn bigquery_recovery_sql(specs: &[TargetColumnSpec], table: &str) -> String {
    let cols = recovery_projection(specs, |name| format!("  {name}"));
    format!(
        "-- 1) bq load --autodetect --parquet_enable_list_inference \
         --source_format=PARQUET {table}__staging <parquet>\n\
         -- 2) recover native types:\n\
         CREATE OR REPLACE TABLE `{table}` AS\n\
         SELECT\n{cols}\n\
         FROM `{table}__staging`;"
    )
}

/// Emit a Snowflake type-recovery script (ADR-0014 L5). Snowflake's Parquet
/// autoload degrades JSON→TEXT, UUID→BINARY, naive timestamp→NUMBER (µs) and
/// TIME→NUMBER, so the recovery is a post-load CTAS over a `MATCH_BY_COLUMN_NAME`
/// staging table. INFER_SCHEMA emits lowercase, case-sensitive names, so every
/// reference is double-quoted. Verified live (2026-06-01) via the `snow` CLI.
fn snowflake_recovery_sql(specs: &[TargetColumnSpec], table: &str) -> String {
    let cols = recovery_projection(specs, |name| format!("  \"{name}\" AS {name}"));
    format!(
        "-- 1) ALTER SESSION SET TIMEZONE='UTC';\n\
         -- 2) CREATE OR REPLACE FILE FORMAT rivet_pq TYPE=PARQUET BINARY_AS_TEXT=FALSE;\n\
         -- 3) PUT file://<parquet> @<stage> AUTO_COMPRESS=FALSE;\n\
         -- 4) CREATE OR REPLACE TABLE {table}__staging USING TEMPLATE (SELECT ARRAY_AGG(\n\
         --      OBJECT_CONSTRUCT(*)) FROM TABLE(INFER_SCHEMA(LOCATION=>'@<stage>', FILE_FORMAT=>'rivet_pq')));\n\
         --    COPY INTO {table}__staging FROM @<stage> FILE_FORMAT=(FORMAT_NAME='rivet_pq') MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;\n\
         -- 5) recover native types:\n\
         CREATE OR REPLACE TABLE {table} AS\n\
         SELECT\n{cols}\n\
         FROM {table}__staging;"
    )
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
            // u64 > i64::MAX overflows the INT64 autoload and cannot be
            // recovered post-load (the bits are already wrong). The only fix is
            // source-side: map the column to decimal(20,0) with a column
            // override so it rides as Parquet DECIMAL → BigQuery NUMERIC.
            RivetType::UInt64 => Resolved::diverge(
                "NUMERIC",
                "INT64",
                "UINT64 > INT64_MAX overflows the INT64 autoload and cannot be recovered after \
                 load — map the column to decimal(20,0) with a source column override",
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
            // (verified). `DATETIME(ts)` recovers the wall-clock after load.
            RivetType::Timestamp { timezone: None, .. } => Resolved::diverge(
                "DATETIME",
                "TIMESTAMP",
                "naive timestamp autoloads as TIMESTAMP (an instant); recover wall-clock with \
                 DATETIME(col) after load",
                Some("DATETIME({col})"),
            ),
            RivetType::String | RivetType::Text | RivetType::Enum => Resolved::ok("STRING"),
            RivetType::Binary => Resolved::ok("BYTES"),
            // Parquet JSON logical type autoloads as BYTES in BigQuery
            // (verified). Declare JSON in the load schema for native JSON.
            RivetType::Json => Resolved::diverge(
                "JSON",
                "BYTES",
                "Parquet JSON logical type autoloads as BYTES in BigQuery; recover native JSON \
                 with PARSE_JSON(SAFE_CONVERT_BYTES_TO_STRING(col)) after load",
                Some("PARSE_JSON(SAFE_CONVERT_BYTES_TO_STRING({col}))"),
            ),
            // UUID rides as FixedSizeBinary(16) + UUIDType; BigQuery has no UUID
            // type and autoloads it as 16-byte BYTES (verified). Native is
            // STRING (canonical text).
            RivetType::Uuid => Resolved::diverge(
                "STRING",
                "BYTES",
                "UUID autoloads as 16-byte BYTES in BigQuery; recover hex text with TO_HEX(col) \
                 after load (or keep BYTES)",
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
        // Rivet writes the Parquet list element as `item` (arrow-rs default,
        // not the spec's `element`), so BigQuery loads arrays as
        // REPEATED RECORD{item} even with `--parquet_enable_list_inference`
        // (without the flag they nest one level deeper as RECORD{list:...}).
        // Verified live: flattening with `UNNEST(col)` + `el.item` recovers a
        // clean REPEATED scalar.
        Resolved::diverge(
            format!("REPEATED {}", inner_r.target_type),
            format!("REPEATED RECORD{{item {}}}", inner_r.autoload_type),
            "arrays load as REPEATED RECORD{item}; load the staging table with \
             --parquet_enable_list_inference, then flatten with UNNEST after load",
            Some("ARRAY(SELECT el.item FROM UNNEST({col}) AS el)"),
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

// ── Snowflake ────────────────────────────────────────────────────────────────

mod snowflake {
    use super::*;

    pub(super) fn resolve(input: &TargetInput<'_>) -> TargetColumnSpec {
        native(input.rivet_type).into_spec(input)
    }

    /// Snowflake autoload (INFER_SCHEMA / COPY) + recovery casts — verified live
    /// (2026-06-01). Needs `BINARY_AS_TEXT=FALSE` in the file format; cast column
    /// refs are double-quoted because INFER_SCHEMA names are lowercase and
    /// case-sensitive.
    fn native(t: &RivetType) -> Resolved {
        match t {
            RivetType::Bool => Resolved::ok("BOOLEAN"),
            RivetType::Int16 | RivetType::Int32 | RivetType::Int64 => Resolved::ok("NUMBER(38,0)"),
            // u64 > INT64_MAX overflows the Parquet read; fix at source.
            RivetType::UInt64 => Resolved::diverge(
                "NUMBER(20,0)",
                "NUMBER(38,0)",
                "UINT64 > INT64_MAX overflows the Parquet read; map to decimal(20,0) at source",
                None,
            ),
            RivetType::Float32 | RivetType::Float64 => Resolved::ok("FLOAT"),
            RivetType::Decimal { precision, scale } => {
                if *scale < 0 {
                    Resolved::warn(
                        "NUMBER",
                        format!(
                            "Snowflake NUMBER has no negative scale; decimal({precision},{scale}) loads via cast"
                        ),
                    )
                } else {
                    Resolved::ok(format!("NUMBER({precision},{scale})"))
                }
            }
            RivetType::Date => Resolved::ok("DATE"),
            // TIME autoloads as NUMBER (µs of day); rebuild with TIME_FROM_PARTS.
            RivetType::Time { .. } => Resolved::diverge(
                "TIME",
                "NUMBER(38,0)",
                "TIME autoloads as NUMBER (µs of day); recover with TIME_FROM_PARTS after load",
                Some(r#"TIME_FROM_PARTS(0,0,FLOOR("{col}"/1000000),MOD("{col}",1000000)*1000)"#),
            ),
            // tz timestamp lands as TIMESTAMP_NTZ holding the UTC instant.
            RivetType::Timestamp {
                timezone: Some(_), ..
            } => Resolved::diverge(
                "TIMESTAMP_TZ",
                "TIMESTAMP_NTZ",
                "tz timestamp autoloads as TIMESTAMP_NTZ — ALTER SESSION SET TIMEZONE='UTC' before COPY so the instant matches",
                None,
            ),
            // naive timestamp autoloads as NUMBER (µs since epoch).
            RivetType::Timestamp { timezone: None, .. } => Resolved::diverge(
                "TIMESTAMP_NTZ",
                "NUMBER(38,0)",
                "naive timestamp autoloads as NUMBER (µs since epoch); recover with TO_TIMESTAMP_NTZ after load",
                Some(r#"TO_TIMESTAMP_NTZ("{col}", 6)"#),
            ),
            RivetType::String | RivetType::Text | RivetType::Enum => Resolved::ok("TEXT"),
            // bytea/blob needs BINARY_AS_TEXT=FALSE or non-UTF8 bytes fail.
            RivetType::Binary => Resolved::warn(
                "BINARY",
                "set BINARY_AS_TEXT=FALSE in the Parquet FILE FORMAT or non-UTF8 bytes fail to load",
            ),
            // JSON autoloads as TEXT; PARSE_JSON recovers native VARIANT.
            RivetType::Json => Resolved::diverge(
                "VARIANT",
                "TEXT",
                "JSON autoloads as TEXT; recover native VARIANT with PARSE_JSON after load",
                Some(r#"PARSE_JSON("{col}")"#),
            ),
            // UUID (FixedSizeBinary 16) autoloads as 16-byte BINARY.
            RivetType::Uuid => Resolved::diverge(
                "TEXT",
                "BINARY",
                "UUID autoloads as 16-byte BINARY; recover canonical text with HEX_ENCODE + REGEXP after load",
                Some(
                    r#"REGEXP_REPLACE(LOWER(HEX_ENCODE("{col}")),'^(.{8})(.{4})(.{4})(.{4})(.{12})$','\\1-\\2-\\3-\\4-\\5')"#,
                ),
            ),
            RivetType::Interval => Resolved::ok("TEXT"),
            // A Parquet list autoloads as VARIANT (holding the JSON array), not
            // native ARRAY — verified live 2026-06-01: INFER_SCHEMA reports
            // VARIANT for both `tags` (text[]) and `nums` (int[]). Recover the
            // native ARRAY with `::ARRAY` after load.
            RivetType::List { inner } => {
                let inner_r = native(inner);
                if inner_r.status == TargetStatus::Fail {
                    Resolved::fail(format!("ARRAY of unsupported element: {}", inner_r.target_type))
                } else {
                    Resolved::diverge(
                        "ARRAY",
                        "VARIANT",
                        "list autoloads as VARIANT (the JSON array); recover native ARRAY with ::ARRAY after load",
                        Some(r#""{col}"::ARRAY"#),
                    )
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
    fn sf(rt: &RivetType) -> TargetColumnSpec {
        ExportTarget::Snowflake.resolve_column(input(rt))
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
        // INT64; a SELECT-time cast would operate on corrupted bits. The only
        // fix is source-side (a decimal override) — cast_sql MUST be None and
        // the note must point there, not promise a post-load cast.
        let u = bq(&RivetType::UInt64);
        assert!(
            u.cast_sql.is_none(),
            "overflowed UINT64 has no lossless post-load recovery"
        );
        let note = u.note.unwrap().to_lowercase();
        assert!(
            note.contains("override"),
            "UINT64 note must point to the source-side override, got: {note}"
        );
    }

    #[test]
    fn cast_sql_present_only_when_lossless_post_load() {
        // JSON/UUID/naive-timestamp autoload to a degraded type but still hold
        // the value losslessly, so a post-load cast genuinely recovers it.
        assert!(bq(&RivetType::Json).cast_sql.unwrap().contains("PARSE_JSON"));
        assert!(bq(&RivetType::Uuid).cast_sql.unwrap().contains("TO_HEX"));
        let naive = RivetType::Timestamp {
            unit: super::super::TimeUnit::Microsecond,
            timezone: None,
        };
        assert!(bq(&naive).cast_sql.unwrap().contains("DATETIME"));
    }

    #[test]
    fn every_divergence_offers_a_recovery_path() {
        // Invariant: whenever BigQuery autoload diverges from the native type the
        // operator gets SOME recovery — a lossless post-load `cast_sql`, or a
        // note describing the fix (post-load transform, or a source override).
        // Never a silent no-op (the bug class).
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
            let note = s.note.as_deref().unwrap_or("").to_lowercase();
            let describes_recovery = note.contains("after load") || note.contains("override");
            assert!(
                has_cast || describes_recovery,
                "divergent {rt:?} must offer a recovery (cast_sql or a recovery note)"
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

    // ── L5 recovery SQL (the post-load transform for BigQuery autoload) ───────

    #[test]
    fn bq_recovery_sql_casts_native_types() {
        use super::super::{SourceColumn, TimeUnit};
        let naive = RivetType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: None,
        };
        let mappings = vec![
            TypeMapping::from_source(&SourceColumn::simple("id", "int8", true), RivetType::Int64),
            TypeMapping::from_source(&SourceColumn::simple("attrs", "jsonb", true), RivetType::Json),
            TypeMapping::from_source(&SourceColumn::simple("uid", "uuid", true), RivetType::Uuid),
            TypeMapping::from_source(
                &SourceColumn::simple("created_at", "timestamp", true),
                naive,
            ),
            TypeMapping::from_source(
                &SourceColumn::simple("tags", "_text", true),
                RivetType::List {
                    inner: Box::new(RivetType::String),
                },
            ),
        ];
        let specs = ExportTarget::BigQuery.resolve_table(&mappings);
        let sql = ExportTarget::BigQuery
            .recovery_sql(&specs, "payments")
            .expect("BigQuery has a recovery SQL");
        // The post-load casts that actually recover native types (verified live
        // against BigQuery — a declared-type load is rejected, a cast is not).
        assert!(sql.contains("PARSE_JSON(SAFE_CONVERT_BYTES_TO_STRING(attrs)) AS attrs"));
        assert!(sql.contains("TO_HEX(uid) AS uid"));
        assert!(sql.contains("DATETIME(created_at) AS created_at"));
        // Arrays flatten via UNNEST (verified live; staging loaded with
        // --parquet_enable_list_inference).
        assert!(sql.contains("ARRAY(SELECT el.item FROM UNNEST(tags) AS el) AS tags"));
        assert!(sql.contains("--parquet_enable_list_inference"));
        // OK columns pass through unchanged.
        assert!(sql.contains("SELECT\n  id"));
        // Reads the autoload staging table, writes the recovered table.
        assert!(sql.contains("CREATE OR REPLACE TABLE `payments`"));
        assert!(sql.contains("FROM `payments__staging`"));
    }

    #[test]
    fn duckdb_needs_no_recovery() {
        let mappings = vec![TypeMapping::from_source(
            &super::super::SourceColumn::simple("attrs", "json", true),
            RivetType::Json,
        )];
        let specs = ExportTarget::DuckDb.resolve_table(&mappings);
        assert!(
            ExportTarget::DuckDb.recovery_sql(&specs, "t").is_none(),
            "DuckDB autoloads every logical type natively — no recovery needed"
        );
    }

    #[test]
    fn recovery_sql_projects_every_column_once_and_only_casts_divergent() {
        use super::super::{SourceColumn, TimeUnit};
        let naive = RivetType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: None,
        };
        let cols: [(&str, RivetType); 6] = [
            ("id", RivetType::Int64),                               // ok → passthrough
            ("amount", RivetType::Decimal { precision: 18, scale: 2 }), // ok → passthrough
            ("attrs", RivetType::Json),                            // divergent → cast
            ("uid", RivetType::Uuid),                              // divergent → cast
            ("created_at", naive),                                 // divergent → cast
            ("tags", RivetType::List { inner: Box::new(RivetType::String) }), // divergent → cast
        ];
        let mappings: Vec<_> = cols
            .iter()
            .cloned()
            .map(|(n, rt)| TypeMapping::from_source(&SourceColumn::simple(n, "x", true), rt))
            .collect();
        let specs = ExportTarget::BigQuery.resolve_table(&mappings);
        let sql = ExportTarget::BigQuery.recovery_sql(&specs, "t").unwrap();

        // The SELECT projects exactly one item per input column — nothing dropped,
        // nothing duplicated.
        let body = sql
            .split("SELECT\n")
            .nth(1)
            .and_then(|s| s.split("\nFROM").next())
            .expect("recovery SQL has a SELECT … FROM body");
        assert_eq!(
            body.split(",\n").count(),
            cols.len(),
            "one projection per column, got:\n{body}"
        );
        for (name, _) in &cols {
            assert!(body.contains(name), "column {name} missing:\n{body}");
        }
        // OK columns pass through unchanged (bare `  name,`); divergent ones
        // carry their cast (`… AS name`).
        assert!(body.contains("  id,") && !body.contains("AS id"));
        assert!(body.contains("  amount,") && !body.contains("AS amount"));
        assert!(body.contains("PARSE_JSON(SAFE_CONVERT_BYTES_TO_STRING(attrs)) AS attrs"));
        assert!(body.contains("TO_HEX(uid) AS uid"));
        assert!(body.contains("DATETIME(created_at) AS created_at"));
        assert!(body.contains("UNNEST(tags) AS el) AS tags"));
    }

    // ── Snowflake (verified live 2026-06-01) ─────────────────────────────────

    #[test]
    fn snowflake_autoload_degradations_and_native_casts() {
        // JSON → TEXT autoload / VARIANT native, recover PARSE_JSON.
        let j = sf(&RivetType::Json);
        assert_eq!(j.target_type, "VARIANT");
        assert_eq!(j.autoload_type, "TEXT");
        assert!(j.cast_sql.unwrap().starts_with("PARSE_JSON"));
        // UUID → BINARY autoload / TEXT native, recover via HEX_ENCODE + REGEXP.
        let u = sf(&RivetType::Uuid);
        assert_eq!(u.target_type, "TEXT");
        assert_eq!(u.autoload_type, "BINARY");
        assert!(u.cast_sql.unwrap().contains("HEX_ENCODE"));
        // naive timestamp → NUMBER autoload / TIMESTAMP_NTZ native.
        let naive = RivetType::Timestamp {
            unit: super::super::TimeUnit::Microsecond,
            timezone: None,
        };
        let t = sf(&naive);
        assert_eq!(t.target_type, "TIMESTAMP_NTZ");
        assert_eq!(t.autoload_type, "NUMBER(38,0)");
        assert!(t.cast_sql.unwrap().contains("TO_TIMESTAMP_NTZ"));
        // TIME → NUMBER autoload, recover TIME_FROM_PARTS.
        let tm = sf(&RivetType::Time {
            unit: super::super::TimeUnit::Microsecond,
        });
        assert_eq!(tm.target_type, "TIME");
        assert!(tm.cast_sql.unwrap().contains("TIME_FROM_PARTS"));
        // decimal is native NUMBER(p,s) — no cast.
        let d = sf(&RivetType::Decimal { precision: 18, scale: 2 });
        assert_eq!(d.target_type, "NUMBER(18,2)");
        assert!(d.cast_sql.is_none());
        // list autoloads as VARIANT (verified live), recover native ARRAY with ::ARRAY.
        let l = sf(&RivetType::List {
            inner: Box::new(RivetType::Int64),
        });
        assert_eq!(l.target_type, "ARRAY");
        assert_eq!(l.autoload_type, "VARIANT");
        assert!(l.cast_sql.unwrap().ends_with("::ARRAY"));
    }

    #[test]
    fn snowflake_recovery_sql_quotes_columns_and_casts() {
        use super::super::{SourceColumn, TimeUnit};
        let naive = RivetType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: None,
        };
        let mappings = vec![
            TypeMapping::from_source(&SourceColumn::simple("id", "int8", true), RivetType::Int64),
            TypeMapping::from_source(&SourceColumn::simple("attrs", "jsonb", true), RivetType::Json),
            TypeMapping::from_source(&SourceColumn::simple("uid", "uuid", true), RivetType::Uuid),
            TypeMapping::from_source(
                &SourceColumn::simple("created_at", "timestamp", true),
                naive,
            ),
        ];
        let specs = ExportTarget::Snowflake.resolve_table(&mappings);
        let sql = ExportTarget::Snowflake.recovery_sql(&specs, "t").unwrap();
        // Staging columns are lowercase + quoted; passthrough quotes the source.
        assert!(sql.contains("\"id\" AS id"));
        assert!(sql.contains("PARSE_JSON(\"attrs\") AS attrs"));
        assert!(sql.contains("HEX_ENCODE(\"uid\")"));
        assert!(sql.contains("TO_TIMESTAMP_NTZ(\"created_at\", 6) AS created_at"));
        // The load preamble the recovery depends on.
        assert!(sql.contains("BINARY_AS_TEXT=FALSE"));
        assert!(sql.contains("MATCH_BY_COLUMN_NAME"));
        assert!(sql.contains("FROM t__staging"));
    }

    #[test]
    fn parse_accepts_snowflake() {
        assert_eq!(ExportTarget::parse("snowflake"), Some(ExportTarget::Snowflake));
        assert_eq!(ExportTarget::parse("sf"), Some(ExportTarget::Snowflake));
    }
}
