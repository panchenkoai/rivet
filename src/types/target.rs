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

use super::{RivetType, TimeUnit, TypeFidelity, TypeMapping};

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
    /// Columnar warehouse with the most faithful Parquet autoload of the
    /// warehouse targets: native `UInt64`, `Decimal`, `DateTime64` (naive *and*
    /// tz), and `Array`, so most types autoload exactly. Diverges only on `UUID`
    /// (lands as `FixedString(16)`), `JSON` (lands as `String`) and `TIME` (no
    /// native type) — see [`clickhouse`]. Verified live against `clickhouse_load`.
    ClickHouse,
}

impl ExportTarget {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "bigquery" | "bq" => Some(Self::BigQuery),
            "duckdb" | "duck" => Some(Self::DuckDb),
            "snowflake" | "sf" => Some(Self::Snowflake),
            "clickhouse" | "ch" => Some(Self::ClickHouse),
            _ => None,
        }
    }

    /// Human-readable list of every spelling [`parse`](Self::parse) accepts,
    /// for "unknown target" error messages. Single source so the message can't
    /// drift from `parse` (it once said "bigquery, duckdb" and missed
    /// snowflake). Aliases are shown in parens after each canonical name.
    pub fn valid_target_names() -> &'static str {
        "bigquery (bq), duckdb (duck), snowflake (sf), clickhouse (ch)"
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::BigQuery => "bigquery",
            Self::DuckDb => "duckdb",
            Self::Snowflake => "snowflake",
            Self::ClickHouse => "clickhouse",
        }
    }

    /// Resolve one already-mapped column against this target.
    pub fn resolve_column(self, input: TargetInput<'_>) -> TargetColumnSpec {
        let mut spec = match self {
            ExportTarget::BigQuery => bigquery::resolve(&input),
            ExportTarget::DuckDb => duckdb::resolve(&input),
            ExportTarget::Snowflake => snowflake::resolve(&input),
            ExportTarget::ClickHouse => clickhouse::resolve(&input),
        };
        // Fidelity floor (ADR-0014 T6): the target status must not be rosier
        // than the source fidelity warrants — a lossy/unsupported source column
        // can never resolve to a clean `Ok`.
        if input.fidelity.is_unsafe_for_strict_mode() && spec.status == TargetStatus::Ok {
            spec.status = TargetStatus::Warn;
        }
        self.grade_column_name(&mut spec);
        spec
    }

    /// Flags a column name `rivet load` renames (BigQuery look-alikes) or refuses.
    fn grade_column_name(self, spec: &mut TargetColumnSpec) {
        if self == ExportTarget::DuckDb || super::ident::is_safe_load_ident(&spec.column_name) {
            return;
        }
        let (status, note) = match super::ident::latin_fold(&spec.column_name) {
            Some(latin) if self == ExportTarget::BigQuery => (
                TargetStatus::Warn,
                format!("Cyrillic look-alike letters: loads as `{latin}`"),
            ),
            _ => (
                TargetStatus::Fail,
                "not a plain identifier: `rivet load` refuses it — rename it in the source"
                    .to_string(),
            ),
        };
        if spec.status != TargetStatus::Fail {
            spec.status = status;
        }
        spec.note = Some(match spec.note.take() {
            Some(n) => format!("{note}; {n}"),
            None => note,
        });
    }

    /// Resolve a whole table's worth of columns, one spec per column in order.
    /// Consumed today by the type-report's recovery-SQL emission
    /// (`preflight::type_report`); `plan-load` (ADR-0014 Phase B) would be a
    /// second consumer.
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
            ExportTarget::ClickHouse => Some(clickhouse_recovery_sql(specs, table)),
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
    pub target_type: TargetType,
    /// Type a generic Parquet reader infers without a declared schema. May
    /// differ from `target_type` (e.g. BigQuery autoloads JSON as "BYTES").
    pub autoload_type: TargetType,
    pub status: TargetStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// Materialization snippet / load-schema hint (L5) to recover the native
    /// type when autoload diverges. `None` when `autoload_type == target_type`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cast_sql: Option<String>,
}

/// A warehouse's native column type, closed per target; loaders match on it and `Display` renders the DDL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetType {
    BigQuery(BqType),
    Snowflake(SfType),
    ClickHouse(ChType),
    DuckDb(DuckType),
    /// No type: the column does not map (a `Fail` row), rendered `-`.
    Unmapped,
}

/// A BigQuery column type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BqType {
    Bool,
    Int64,
    Float64,
    Numeric,
    BigNumeric,
    Date,
    Time,
    Timestamp,
    DateTime,
    String,
    Bytes,
    Json,
    /// `ARRAY<STRUCT<item T>>`, the shape list inference loads a Parquet list as.
    Array(Box<BqType>),
}

/// A Snowflake column type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SfType {
    Boolean,
    /// `NUMBER` with no precision.
    Number,
    /// `NUMBER(p,s)`.
    NumberPs(u8, i8),
    Float,
    Date,
    Time,
    TimestampTz,
    TimestampNtz,
    Text,
    Varchar,
    Integer,
    Binary,
    Variant,
    Array,
}

/// A ClickHouse column type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChType {
    Bool,
    Int16,
    Int32,
    Int64,
    UInt64,
    Float32,
    Float64,
    /// `Decimal(p, s)`.
    Decimal(u16, i8),
    /// `Decimal64(s)`.
    Decimal64(u8),
    Date32,
    /// `DateTime64(p)` or `DateTime64(p, 'tz')`.
    DateTime64(u8, Option<String>),
    String,
    Json,
    Uuid,
    /// `FixedString(16)`.
    FixedString16,
    /// `Array(Nullable(T))`.
    Array(Box<ChType>),
}

/// A DuckDB column type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DuckType {
    Boolean,
    SmallInt,
    Integer,
    BigInt,
    UBigInt,
    Float,
    Double,
    /// `DECIMAL` with no precision.
    DecimalBare,
    /// `DECIMAL(p,s)`.
    Decimal(u8, i8),
    /// `DECIMAL(38,*)`, a decimal past DuckDB's precision.
    DecimalWide,
    Date,
    Time,
    TimestampTz,
    TimestampNs,
    Timestamp,
    Varchar,
    Blob,
    Json,
    Uuid,
    Interval,
    /// `T[]`.
    List(Box<DuckType>),
}

impl From<BqType> for TargetType {
    fn from(t: BqType) -> Self {
        Self::BigQuery(t)
    }
}

impl From<SfType> for TargetType {
    fn from(t: SfType) -> Self {
        Self::Snowflake(t)
    }
}

impl From<ChType> for TargetType {
    fn from(t: ChType) -> Self {
        Self::ClickHouse(t)
    }
}

impl From<DuckType> for TargetType {
    fn from(t: DuckType) -> Self {
        Self::DuckDb(t)
    }
}

impl std::fmt::Display for TargetType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BigQuery(t) => t.fmt(f),
            Self::Snowflake(t) => t.fmt(f),
            Self::ClickHouse(t) => t.fmt(f),
            Self::DuckDb(t) => t.fmt(f),
            Self::Unmapped => f.write_str("-"),
        }
    }
}

impl std::fmt::Display for BqType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Bool => "BOOL",
            Self::Int64 => "INT64",
            Self::Float64 => "FLOAT64",
            Self::Numeric => "NUMERIC",
            Self::BigNumeric => "BIGNUMERIC",
            Self::Date => "DATE",
            Self::Time => "TIME",
            Self::Timestamp => "TIMESTAMP",
            Self::DateTime => "DATETIME",
            Self::String => "STRING",
            Self::Bytes => "BYTES",
            Self::Json => "JSON",
            Self::Array(inner) => return write!(f, "ARRAY<STRUCT<item {inner}>>"),
        };
        f.write_str(name)
    }
}

impl std::fmt::Display for SfType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Boolean => "BOOLEAN",
            Self::Number => "NUMBER",
            Self::NumberPs(p, s) => return write!(f, "NUMBER({p},{s})"),
            Self::Float => "FLOAT",
            Self::Date => "DATE",
            Self::Time => "TIME",
            Self::TimestampTz => "TIMESTAMP_TZ",
            Self::TimestampNtz => "TIMESTAMP_NTZ",
            Self::Text => "TEXT",
            Self::Varchar => "VARCHAR",
            Self::Integer => "INTEGER",
            Self::Binary => "BINARY",
            Self::Variant => "VARIANT",
            Self::Array => "ARRAY",
        };
        f.write_str(name)
    }
}

impl std::fmt::Display for ChType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Bool => "Bool",
            Self::Int16 => "Int16",
            Self::Int32 => "Int32",
            Self::Int64 => "Int64",
            Self::UInt64 => "UInt64",
            Self::Float32 => "Float32",
            Self::Float64 => "Float64",
            Self::Decimal(p, s) => return write!(f, "Decimal({p}, {s})"),
            Self::Decimal64(s) => return write!(f, "Decimal64({s})"),
            Self::Date32 => "Date32",
            Self::DateTime64(p, None) => return write!(f, "DateTime64({p})"),
            Self::DateTime64(p, Some(tz)) => return write!(f, "DateTime64({p}, '{tz}')"),
            Self::String => "String",
            Self::Json => "JSON",
            Self::Uuid => "UUID",
            Self::FixedString16 => "FixedString(16)",
            Self::Array(inner) => return write!(f, "Array(Nullable({inner}))"),
        };
        f.write_str(name)
    }
}

impl std::fmt::Display for DuckType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Boolean => "BOOLEAN",
            Self::SmallInt => "SMALLINT",
            Self::Integer => "INTEGER",
            Self::BigInt => "BIGINT",
            Self::UBigInt => "UBIGINT",
            Self::Float => "FLOAT",
            Self::Double => "DOUBLE",
            Self::DecimalBare => "DECIMAL",
            Self::Decimal(p, s) => return write!(f, "DECIMAL({p},{s})"),
            Self::DecimalWide => "DECIMAL(38,*)",
            Self::Date => "DATE",
            Self::Time => "TIME",
            Self::TimestampTz => "TIMESTAMPTZ",
            Self::TimestampNs => "TIMESTAMP_NS",
            Self::Timestamp => "TIMESTAMP",
            Self::Varchar => "VARCHAR",
            Self::Blob => "BLOB",
            Self::Json => "JSON",
            Self::Uuid => "UUID",
            Self::Interval => "INTERVAL",
            Self::List(inner) => return write!(f, "{inner}[]"),
        };
        f.write_str(name)
    }
}

impl Serialize for TargetType {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

#[cfg(test)]
impl PartialEq<&str> for TargetType {
    fn eq(&self, other: &&str) -> bool {
        self.to_string().as_str() == *other
    }
}

/// Internal per-type resolution result, before the column name and cast
/// substitution are applied.
struct Resolved<T> {
    /// `None` when the column does not map (a `Fail`).
    target_type: Option<T>,
    autoload_type: Option<T>,
    status: TargetStatus,
    note: Option<String>,
    /// `cast_sql` template with a `{col}` placeholder, or `None`.
    cast: Option<String>,
}

impl<T: Clone + Into<TargetType>> Resolved<T> {
    fn ok(t: T) -> Self {
        Self {
            autoload_type: Some(t.clone()),
            target_type: Some(t),
            status: TargetStatus::Ok,
            note: None,
            cast: None,
        }
    }
    /// Native type that *autoloads as something else* — the divergence the
    /// resolver exists to surface.
    fn diverge(native: T, autoload: T, note: impl Into<String>, cast: Option<&str>) -> Self {
        Self {
            target_type: Some(native),
            autoload_type: Some(autoload),
            status: TargetStatus::Warn,
            note: Some(note.into()),
            cast: cast.map(str::to_string),
        }
    }
    fn warn(t: T, note: impl Into<String>) -> Self {
        Self {
            autoload_type: Some(t.clone()),
            target_type: Some(t),
            status: TargetStatus::Warn,
            note: Some(note.into()),
            cast: None,
        }
    }
    fn fail(note: impl Into<String>) -> Self {
        Self {
            target_type: None,
            autoload_type: None,
            status: TargetStatus::Fail,
            note: Some(note.into()),
            cast: None,
        }
    }
    fn into_spec(self, input: &TargetInput<'_>) -> TargetColumnSpec {
        TargetColumnSpec {
            column_name: input.column_name.to_string(),
            target_type: self.target_type.map_or(TargetType::Unmapped, Into::into),
            autoload_type: self.autoload_type.map_or(TargetType::Unmapped, Into::into),
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
    /// BIGNUMERIC's range is about ±5.79e38, so it holds at most 38 integer digits.
    const BIGNUMERIC_MAX_INT_DIGITS: i16 = 38;

    pub(super) fn resolve(input: &TargetInput<'_>) -> TargetColumnSpec {
        native(input.rivet_type).into_spec(input)
    }

    fn native(t: &RivetType) -> Resolved<BqType> {
        match t {
            RivetType::Bool => Resolved::ok(BqType::Bool),
            RivetType::Int16 | RivetType::Int32 | RivetType::Int64 => Resolved::ok(BqType::Int64),
            // u64 > i64::MAX overflows the INT64 autoload and cannot be
            // recovered post-load (the bits are already wrong). The only fix is
            // source-side: map the column to decimal(20,0) with a column
            // override so it rides as Parquet DECIMAL → BigQuery NUMERIC.
            RivetType::UInt64 => Resolved::diverge(
                BqType::Numeric,
                BqType::Int64,
                "UINT64 > INT64_MAX overflows the INT64 autoload and cannot be recovered after \
                 load — map the column to decimal(20,0) with a source column override",
                None,
            ),
            RivetType::Float32 | RivetType::Float64 => Resolved::ok(BqType::Float64),
            RivetType::Decimal { precision, scale } => decimal(*precision, *scale),
            RivetType::Date => Resolved::ok(BqType::Date),
            RivetType::Time { .. } => Resolved::ok(BqType::Time),
            // tz-aware timestamp → instant → TIMESTAMP, autoloads cleanly.
            RivetType::Timestamp {
                timezone: Some(_), ..
            } => Resolved::ok(BqType::Timestamp),
            // Nanosecond naive timestamp (the `timestamp_ns` override, e.g. SQL
            // Server datetime2(7)) has no BigQuery native temporal type: the loader
            // does not recognise the Parquet TIMESTAMP(NANOS) logical type and
            // autoloads the raw INT64 nanoseconds-since-epoch (verified live
            // 2026-06-07 — lossless as an integer). A native TIMESTAMP needs
            // TIMESTAMP_MICROS(DIV(col,1000)), which drops the sub-µs digits
            // (BigQuery TIMESTAMP is microsecond), so no lossless temporal cast
            // exists — keep INT64, or use the default `timestamp` for BigQuery.
            RivetType::Timestamp {
                unit: TimeUnit::Nanosecond,
                timezone: None,
            } => Resolved::diverge(
                BqType::Int64,
                BqType::Int64,
                "nanosecond timestamp has no BigQuery native type — autoloads as INT64 (raw \
                 nanos, lossless); a native TIMESTAMP via TIMESTAMP_MICROS(DIV(col,1000)) drops \
                 sub-µs precision. Prefer `timestamp` (microsecond) for BigQuery targets.",
                None,
            ),
            // naive timestamp → wall-clock → DATETIME, but BigQuery autoload
            // ignores Parquet isAdjustedToUTC=false and yields TIMESTAMP
            // (verified). `DATETIME(ts)` recovers the wall-clock after load.
            RivetType::Timestamp { timezone: None, .. } => Resolved::diverge(
                BqType::DateTime,
                BqType::Timestamp,
                "naive timestamp autoloads as TIMESTAMP (an instant); recover wall-clock with \
                 DATETIME(col) after load",
                Some("DATETIME({col})"),
            ),
            RivetType::String | RivetType::Text | RivetType::Enum => Resolved::ok(BqType::String),
            RivetType::Binary => Resolved::ok(BqType::Bytes),
            // Parquet JSON logical type autoloads as BYTES in BigQuery
            // (verified). Declare JSON in the load schema for native JSON.
            RivetType::Json => Resolved::diverge(
                BqType::Json,
                BqType::Bytes,
                "Parquet JSON logical type autoloads as BYTES in BigQuery; recover native JSON \
                 with PARSE_JSON(SAFE_CONVERT_BYTES_TO_STRING(col)) after load",
                Some("PARSE_JSON(SAFE_CONVERT_BYTES_TO_STRING({col}))"),
            ),
            // BigQuery has no UUID type: the landing zone keeps the 16 bytes; consumers render text in a view.
            RivetType::Uuid => Resolved::warn(
                BqType::Bytes,
                "BigQuery has no UUID type: the column lands as its 16 bytes; render the text in \
                 a view with TO_HEX(col)",
            ),
            RivetType::Interval => Resolved::ok(BqType::String),
            RivetType::List { inner } => list(inner),
            RivetType::Unsupported { .. } => Resolved::fail(unsupported_reason(t)),
        }
    }

    fn decimal(p: u8, s: i8) -> Resolved<BqType> {
        if s < 0 {
            return Resolved::fail(format!(
                "BigQuery has no negative scale; decimal({p},{s}) needs a STRING/INT64 cast"
            ));
        }
        let native = if p <= NUMERIC_MAX_P && s <= NUMERIC_MAX_S {
            BqType::Numeric
        } else if p <= BIGNUMERIC_MAX_P
            && s <= BIGNUMERIC_MAX_S
            && i16::from(p) - i16::from(s) <= BIGNUMERIC_MAX_INT_DIGITS
        {
            BqType::BigNumeric
        } else {
            return Resolved::fail(format!(
                "decimal({p},{s}) exceeds BigQuery BIGNUMERIC limits (max 76,38, and at most \
                 38 integer digits; this has {})",
                i16::from(p) - i16::from(s)
            ));
        };
        Resolved::ok(native)
    }

    fn list(inner: &RivetType) -> Resolved<BqType> {
        let Some(inner) = native(inner).target_type else {
            return Resolved::fail("ARRAY of unsupported element: -");
        };
        // Rivet writes the Parquet list element as `item` (arrow-rs default, not
        // the spec's `element`), so with `enable_list_inference` BigQuery loads an
        // array as ARRAY<STRUCT<item T>> (== REPEATED RECORD{item}). Declare THAT
        // exact shape in the LOAD DATA schema so `rivet load` succeeds: a bare
        // `REPEATED T` is invalid standard-SQL DDL, and a clean `ARRAY<T>` loads
        // EMPTY (BigQuery can't map the `item`-named element without the struct).
        // autoload == target here (BigQuery autodetect produces the same nested
        // shape), so this is a warn, not a divergence — flatten to a scalar array
        // after load with `ARRAY(SELECT el.item FROM UNNEST(col) AS el)`.
        Resolved::warn(
            BqType::Array(Box::new(inner)),
            "arrays load as ARRAY<STRUCT<item T>> (nested, element named `item`); \
             flatten to a scalar array with ARRAY(SELECT el.item FROM UNNEST(col) AS el)",
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
    fn native(t: &RivetType) -> Resolved<DuckType> {
        match t {
            RivetType::Bool => Resolved::ok(DuckType::Boolean),
            RivetType::Int16 => Resolved::ok(DuckType::SmallInt),
            RivetType::Int32 => Resolved::ok(DuckType::Integer),
            RivetType::Int64 => Resolved::ok(DuckType::BigInt),
            RivetType::UInt64 => Resolved::ok(DuckType::UBigInt),
            RivetType::Float32 => Resolved::ok(DuckType::Float),
            RivetType::Float64 => Resolved::ok(DuckType::Double),
            RivetType::Decimal { precision, scale } => {
                if *scale < 0 {
                    Resolved::warn(
                        DuckType::DecimalBare,
                        format!(
                            "DuckDB has no negative scale; decimal({precision},{scale}) loads via cast"
                        ),
                    )
                } else if *precision <= 38 {
                    Resolved::ok(DuckType::Decimal(*precision, *scale))
                } else {
                    // DuckDB DECIMAL maxes at precision 38; a wider decimal autoloads
                    // as DOUBLE (lossy past 2^53, verified live). Tell that truth as a
                    // divergence, not a same-type warn — and no cast recovers a DOUBLE,
                    // so the recovery is upstream (narrow the source precision).
                    Resolved::diverge(
                        DuckType::DecimalWide,
                        DuckType::Double,
                        format!(
                            "decimal({precision},{scale}) exceeds DuckDB DECIMAL(38); autoloads \
                             as DOUBLE (lossy past 2^53) — narrow the source precision if exact \
                             decimals matter"
                        ),
                        None,
                    )
                }
            }
            RivetType::Date => Resolved::ok(DuckType::Date),
            RivetType::Time { .. } => Resolved::ok(DuckType::Time),
            RivetType::Timestamp {
                timezone: Some(_), ..
            } => Resolved::ok(DuckType::TimestampTz),
            // DuckDB has a native nanosecond timestamp; the `timestamp_ns` override
            // round-trips losslessly (verified live 2026-06-07).
            RivetType::Timestamp {
                unit: TimeUnit::Nanosecond,
                timezone: None,
            } => Resolved::ok(DuckType::TimestampNs),
            RivetType::Timestamp { timezone: None, .. } => Resolved::ok(DuckType::Timestamp),
            RivetType::String | RivetType::Text | RivetType::Enum => {
                Resolved::ok(DuckType::Varchar)
            }
            RivetType::Binary => Resolved::ok(DuckType::Blob),
            RivetType::Json => Resolved::ok(DuckType::Json),
            RivetType::Uuid => Resolved::ok(DuckType::Uuid),
            RivetType::Interval => Resolved::ok(DuckType::Interval),
            RivetType::List { inner } => match native(inner).target_type {
                Some(inner) => Resolved::ok(DuckType::List(Box::new(inner))),
                None => Resolved::fail("LIST of unsupported element: -"),
            },
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
    fn native(t: &RivetType) -> Resolved<SfType> {
        match t {
            RivetType::Bool => Resolved::ok(SfType::Boolean),
            RivetType::Int16 | RivetType::Int32 | RivetType::Int64 => {
                Resolved::ok(SfType::NumberPs(38, 0))
            }
            // u64 > INT64_MAX overflows the Parquet read; fix at source.
            RivetType::UInt64 => Resolved::diverge(
                SfType::NumberPs(20, 0),
                SfType::NumberPs(38, 0),
                "UINT64 > INT64_MAX overflows the Parquet read; map to decimal(20,0) at source",
                None,
            ),
            RivetType::Float32 | RivetType::Float64 => Resolved::ok(SfType::Float),
            RivetType::Decimal { precision, scale } => {
                if *scale < 0 {
                    Resolved::warn(
                        SfType::Number,
                        format!(
                            "Snowflake NUMBER has no negative scale; decimal({precision},{scale}) loads via cast"
                        ),
                    )
                } else if *precision > 38 {
                    // Snowflake NUMBER maxes at precision 38 — NUMBER(50,10) is not a
                    // valid type. Never claim Ok for something the warehouse rejects;
                    // past 38 is a Fail, the same discipline BigQuery applies past its
                    // BIGNUMERIC ceiling.
                    Resolved::fail(format!(
                        "decimal({precision},{scale}) exceeds Snowflake NUMBER (max precision 38); \
                         narrow the source precision, or load as FLOAT via a declared schema (lossy)"
                    ))
                } else {
                    Resolved::ok(SfType::NumberPs(*precision, *scale))
                }
            }
            RivetType::Date => Resolved::ok(SfType::Date),
            // TIME autoloads as NUMBER (µs of day); rebuild with TIME_FROM_PARTS.
            RivetType::Time { .. } => Resolved::diverge(
                SfType::Time,
                SfType::NumberPs(38, 0),
                "TIME autoloads as NUMBER (µs of day); recover with TIME_FROM_PARTS after load",
                Some(r#"TIME_FROM_PARTS(0,0,FLOOR("{col}"/1000000),MOD("{col}",1000000)*1000)"#),
            ),
            // tz timestamp lands as TIMESTAMP_NTZ holding the UTC instant.
            RivetType::Timestamp {
                timezone: Some(_), ..
            } => Resolved::diverge(
                SfType::TimestampTz,
                SfType::TimestampNtz,
                "tz timestamp autoloads as TIMESTAMP_NTZ — ALTER SESSION SET TIMEZONE='UTC' before COPY so the instant matches",
                None,
            ),
            // Nanosecond naive timestamp autoloads as NUMBER (ns since epoch),
            // like the µs case but at scale 9. Snowflake TIMESTAMP_NTZ holds full
            // 9-digit precision, so TO_TIMESTAMP_NTZ(col, 9) recovers it
            // losslessly — verified live 2026-06-07 (NUMBER 1717761600123456700 →
            // 2024-06-07 12:00:00.123456700, the 7th digit intact).
            RivetType::Timestamp {
                unit: TimeUnit::Nanosecond,
                timezone: None,
            } => Resolved::diverge(
                SfType::TimestampNtz,
                SfType::NumberPs(38, 0),
                "nanosecond timestamp autoloads as NUMBER (ns since epoch); recover with \
                 TO_TIMESTAMP_NTZ(col, 9) after load — Snowflake TIMESTAMP_NTZ holds full ns precision",
                Some(r#"TO_TIMESTAMP_NTZ("{col}", 9)"#),
            ),
            // naive timestamp autoloads as NUMBER (µs since epoch).
            RivetType::Timestamp { timezone: None, .. } => Resolved::diverge(
                SfType::TimestampNtz,
                SfType::NumberPs(38, 0),
                "naive timestamp autoloads as NUMBER (µs since epoch); recover with TO_TIMESTAMP_NTZ after load",
                Some(r#"TO_TIMESTAMP_NTZ("{col}", 6)"#),
            ),
            RivetType::String | RivetType::Text | RivetType::Enum => Resolved::ok(SfType::Text),
            // bytea/blob needs BINARY_AS_TEXT=FALSE or non-UTF8 bytes fail.
            RivetType::Binary => Resolved::warn(
                SfType::Binary,
                "set BINARY_AS_TEXT=FALSE in the Parquet FILE FORMAT or non-UTF8 bytes fail to load",
            ),
            // JSON autoloads as TEXT; PARSE_JSON recovers native VARIANT.
            RivetType::Json => Resolved::diverge(
                SfType::Variant,
                SfType::Text,
                "JSON autoloads as TEXT; recover native VARIANT with PARSE_JSON after load",
                Some(r#"PARSE_JSON("{col}")"#),
            ),
            // UUID (FixedSizeBinary 16) autoloads as 16-byte BINARY.
            RivetType::Uuid => Resolved::diverge(
                SfType::Text,
                SfType::Binary,
                "UUID autoloads as 16-byte BINARY; recover canonical text with HEX_ENCODE + REGEXP after load",
                Some(
                    r#"REGEXP_REPLACE(LOWER(HEX_ENCODE("{col}")),'^(.{8})(.{4})(.{4})(.{4})(.{12})$','\\1-\\2-\\3-\\4-\\5')"#,
                ),
            ),
            RivetType::Interval => Resolved::ok(SfType::Text),
            // A Parquet list autoloads as VARIANT (holding the JSON array), not
            // native ARRAY — verified live 2026-06-01: INFER_SCHEMA reports
            // VARIANT for both `tags` (text[]) and `nums` (int[]). Recover the
            // native ARRAY with `::ARRAY` after load.
            RivetType::List { inner } => {
                if native(inner).target_type.is_none() {
                    Resolved::fail("ARRAY of unsupported element: -")
                } else {
                    Resolved::diverge(
                        SfType::Array,
                        SfType::Variant,
                        "list autoloads as VARIANT (the JSON array); recover native ARRAY with ::ARRAY after load",
                        Some(r#""{col}"::ARRAY"#),
                    )
                }
            }
            RivetType::Unsupported { .. } => Resolved::fail(unsupported_reason(t)),
        }
    }
}

// ── ClickHouse ───────────────────────────────────────────────────────────────

fn clickhouse_recovery_sql(specs: &[TargetColumnSpec], table: &str) -> String {
    let cols = recovery_projection(specs, |name| format!("  {name}"));
    format!(
        "-- 1) load the Parquet into a staging table, e.g.\n\
         --    CREATE TABLE {table}__staging ENGINE = MergeTree ORDER BY tuple() AS\n\
         --      SELECT * FROM file('<parquet>', 'Parquet');\n\
         -- 2) recover native types:\n\
         CREATE TABLE {table} ENGINE = MergeTree ORDER BY tuple() AS\n\
         SELECT\n{cols}\n\
         FROM {table}__staging;"
    )
}

mod clickhouse {
    use super::*;

    pub(super) fn resolve(input: &TargetInput<'_>) -> TargetColumnSpec {
        native(input.rivet_type).into_spec(input)
    }

    /// ClickHouse Parquet autoload (`file(..., 'Parquet')`) — pinned against the
    /// live `clickhouse_load` matrix. The most faithful warehouse target: native
    /// `UInt64`, `Decimal`, `DateTime64` (naive *and* tz) and `Array`, so most
    /// types autoload exactly. Divergences: `UUID` -> `FixedString(16)`, `JSON`
    /// -> `String`, and `TIME` (no native type -> `Int64`, µs of day).
    fn native(t: &RivetType) -> Resolved<ChType> {
        match t {
            RivetType::Bool => Resolved::ok(ChType::Bool),
            RivetType::Int16 => Resolved::ok(ChType::Int16),
            RivetType::Int32 => Resolved::ok(ChType::Int32),
            RivetType::Int64 => Resolved::ok(ChType::Int64),
            // Native unsigned 64-bit — ClickHouse holds the full UInt64 range, so
            // the overflow that forces BigQuery/Snowflake to a load-schema note
            // never happens here. The headline difference from the cloud warehouses.
            RivetType::UInt64 => Resolved::ok(ChType::UInt64),
            RivetType::Float32 => Resolved::ok(ChType::Float32),
            RivetType::Float64 => Resolved::ok(ChType::Float64),
            RivetType::Decimal { precision, scale } => {
                if *scale < 0 {
                    // A bare `Decimal` is ClickHouse's Decimal(10, 0) — not this type. The
                    // values are whole numbers, so widening the precision holds them exactly.
                    let width = u16::from(*precision) + u16::from(scale.unsigned_abs());
                    if width > 76 {
                        Resolved::fail(format!(
                            "decimal({precision},{scale}) needs Decimal({width}, 0) to hold its \
                             whole numbers, past ClickHouse's precision 76"
                        ))
                    } else {
                        Resolved::warn(
                            ChType::Decimal(width, 0),
                            format!(
                                "ClickHouse Decimal has no negative scale; decimal({precision},{scale}) \
                                 is declared Decimal({width}, 0), which holds its whole numbers exactly"
                            ),
                        )
                    }
                } else if *precision > 76 {
                    // ClickHouse Decimal caps at precision 76 (Decimal256) — the same
                    // silent-Ok class as Snowflake past 38: never claim a type the
                    // engine rejects. Fail past the ceiling.
                    Resolved::fail(format!(
                        "decimal({precision},{scale}) exceeds ClickHouse Decimal (max precision 76); \
                         narrow the source precision"
                    ))
                } else {
                    Resolved::ok(ChType::Decimal(u16::from(*precision), *scale))
                }
            }
            RivetType::Date => Resolved::ok(ChType::Date32),
            // No time-of-day type: an Int64 column keeps whole seconds only (measured:
            // 13:45:07.123456 -> 49507); Decimal64 keeps the fraction (49507.123456).
            RivetType::Time { unit } => {
                let p = match unit {
                    TimeUnit::Second => 0,
                    TimeUnit::Millisecond => 3,
                    TimeUnit::Microsecond => 6,
                    TimeUnit::Nanosecond => 9,
                };
                Resolved::diverge(
                    ChType::Decimal64(p),
                    ChType::Int64,
                    "ClickHouse has no TIME type: rivet load declares seconds since midnight as \
                     Decimal64, keeping the fraction; a plain Parquet autoload reads Int64 whole \
                     seconds",
                    None,
                )
            }
            // DateTime64 holds both naive and tz timestamps natively (verified:
            // naive -> DateTime64(6), tz -> DateTime64(6, 'UTC')).
            RivetType::Timestamp { unit, timezone } => {
                let p = match unit {
                    TimeUnit::Second => 0,
                    TimeUnit::Millisecond => 3,
                    TimeUnit::Microsecond => 6,
                    TimeUnit::Nanosecond => 9,
                };
                Resolved::warn(
                    ChType::DateTime64(p, timezone.clone()),
                    "DateTime64 holds 1900-01-01 to 2299-12-31; rivet load refuses a part holding a \
                     value outside it, but a load pulled through a named collection (or any other \
                     reader) gets the nearest end, silently",
                )
            }
            RivetType::String | RivetType::Text | RivetType::Enum => Resolved::ok(ChType::String),
            // ClickHouse String holds arbitrary bytes, so bytea/blob round-trips
            // losslessly — no BINARY_AS_TEXT caveat like Snowflake.
            RivetType::Binary => Resolved::ok(ChType::String),
            // Parquet JSON autoloads as String holding the valid JSON text, and `rivet
            // load` declares String: a JSON column refuses the Parquet insert (measured).
            RivetType::Json => Resolved::diverge(
                ChType::Json,
                ChType::String,
                "JSON lands as String holding the valid JSON text (rivet load declares String; \
                 a ClickHouse JSON column refuses the Parquet insert); read it with the \
                 JSONExtract* functions",
                None,
            ),
            // The 16-byte UUID field autoloads as FixedString(16) (verified live).
            // The bytes are the canonical UUID; recover the native UUID with the
            // hex -> dashed-text -> toUUID round-trip clickhouse_load pins.
            RivetType::Uuid => Resolved::diverge(
                ChType::Uuid,
                ChType::FixedString16,
                "UUID autoloads as FixedString(16); recover the native UUID with toUUID after load",
                Some(
                    "toUUID(concat(substring(lower(hex({col})),1,8),'-',substring(lower(hex({col})),9,4),'-',substring(lower(hex({col})),13,4),'-',substring(lower(hex({col})),17,4),'-',substring(lower(hex({col})),21,12)))",
                ),
            ),
            RivetType::Interval => Resolved::ok(ChType::String),
            // A Parquet list autoloads as a native Array (verified: tags ->
            // Array(Nullable(String)), nums -> Array(Nullable(Int32))).
            RivetType::List { inner } => {
                let inner_r = native(inner);
                if let (Some(inner), Some(inner_autoload)) =
                    (inner_r.target_type, inner_r.autoload_type)
                {
                    let null_note = "a ClickHouse Array cannot be NULL: a NULL list loads as [], \
                                     the same value as an empty list";
                    let note = match &inner_r.note {
                        Some(n) => format!("{null_note}; each element: {n}"),
                        None => null_note.to_string(),
                    };
                    Resolved::diverge(
                        ChType::Array(Box::new(inner)),
                        ChType::Array(Box::new(inner_autoload)),
                        note,
                        None,
                    )
                } else {
                    Resolved::fail("Array of unsupported element: -")
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

    #[test]
    fn check_names_the_lookalike_rename_and_the_name_load_refuses() {
        let named = |target: ExportTarget, name: &str| {
            target.resolve_column(TargetInput {
                column_name: name,
                ..input(&RivetType::String)
            })
        };
        let folded = named(ExportTarget::BigQuery, "\u{441}omment");
        assert_eq!(folded.status, TargetStatus::Warn);
        assert!(folded.note.unwrap().contains("loads as `comment`"));
        assert_eq!(
            named(ExportTarget::Snowflake, "\u{441}omment").status,
            TargetStatus::Fail
        );
        assert_eq!(
            named(ExportTarget::BigQuery, "\u{438}\u{43c}\u{44f}").status,
            TargetStatus::Fail
        );
        let plain = named(ExportTarget::BigQuery, "comment");
        assert_eq!((plain.status, plain.note), (TargetStatus::Ok, None));
    }
    /// Every target type renders the exact DDL spelling the resolver emitted as text before it was typed.
    #[test]
    fn every_target_type_renders_its_ddl_spelling() {
        let utc = Some("UTC".to_string());
        let cases: Vec<(TargetType, &str)> = vec![
            (BqType::Bool.into(), "BOOL"),
            (BqType::Int64.into(), "INT64"),
            (BqType::Float64.into(), "FLOAT64"),
            (BqType::Numeric.into(), "NUMERIC"),
            (BqType::BigNumeric.into(), "BIGNUMERIC"),
            (BqType::Date.into(), "DATE"),
            (BqType::Time.into(), "TIME"),
            (BqType::Timestamp.into(), "TIMESTAMP"),
            (BqType::DateTime.into(), "DATETIME"),
            (BqType::String.into(), "STRING"),
            (BqType::Bytes.into(), "BYTES"),
            (BqType::Json.into(), "JSON"),
            (
                BqType::Array(Box::new(BqType::Array(Box::new(BqType::Int64)))).into(),
                "ARRAY<STRUCT<item ARRAY<STRUCT<item INT64>>>>",
            ),
            (SfType::Boolean.into(), "BOOLEAN"),
            (SfType::Number.into(), "NUMBER"),
            (SfType::NumberPs(38, 0).into(), "NUMBER(38,0)"),
            (SfType::Float.into(), "FLOAT"),
            (SfType::Date.into(), "DATE"),
            (SfType::Time.into(), "TIME"),
            (SfType::TimestampTz.into(), "TIMESTAMP_TZ"),
            (SfType::TimestampNtz.into(), "TIMESTAMP_NTZ"),
            (SfType::Text.into(), "TEXT"),
            (SfType::Varchar.into(), "VARCHAR"),
            (SfType::Integer.into(), "INTEGER"),
            (SfType::Binary.into(), "BINARY"),
            (SfType::Variant.into(), "VARIANT"),
            (SfType::Array.into(), "ARRAY"),
            (ChType::Bool.into(), "Bool"),
            (ChType::Int16.into(), "Int16"),
            (ChType::Int32.into(), "Int32"),
            (ChType::Int64.into(), "Int64"),
            (ChType::UInt64.into(), "UInt64"),
            (ChType::Float32.into(), "Float32"),
            (ChType::Float64.into(), "Float64"),
            (ChType::Decimal(76, 2).into(), "Decimal(76, 2)"),
            (ChType::Decimal64(6).into(), "Decimal64(6)"),
            (ChType::Date32.into(), "Date32"),
            (ChType::DateTime64(9, None).into(), "DateTime64(9)"),
            (ChType::DateTime64(3, utc).into(), "DateTime64(3, 'UTC')"),
            (ChType::String.into(), "String"),
            (ChType::Json.into(), "JSON"),
            (ChType::Uuid.into(), "UUID"),
            (ChType::FixedString16.into(), "FixedString(16)"),
            (
                ChType::Array(Box::new(ChType::Array(Box::new(ChType::Uuid)))).into(),
                "Array(Nullable(Array(Nullable(UUID))))",
            ),
            (DuckType::Boolean.into(), "BOOLEAN"),
            (DuckType::SmallInt.into(), "SMALLINT"),
            (DuckType::Integer.into(), "INTEGER"),
            (DuckType::BigInt.into(), "BIGINT"),
            (DuckType::UBigInt.into(), "UBIGINT"),
            (DuckType::Float.into(), "FLOAT"),
            (DuckType::Double.into(), "DOUBLE"),
            (DuckType::DecimalBare.into(), "DECIMAL"),
            (DuckType::Decimal(18, 2).into(), "DECIMAL(18,2)"),
            (DuckType::DecimalWide.into(), "DECIMAL(38,*)"),
            (DuckType::Date.into(), "DATE"),
            (DuckType::Time.into(), "TIME"),
            (DuckType::TimestampTz.into(), "TIMESTAMPTZ"),
            (DuckType::TimestampNs.into(), "TIMESTAMP_NS"),
            (DuckType::Timestamp.into(), "TIMESTAMP"),
            (DuckType::Varchar.into(), "VARCHAR"),
            (DuckType::Blob.into(), "BLOB"),
            (DuckType::Json.into(), "JSON"),
            (DuckType::Uuid.into(), "UUID"),
            (DuckType::Interval.into(), "INTERVAL"),
            (
                DuckType::List(Box::new(DuckType::List(Box::new(DuckType::Json)))).into(),
                "JSON[][]",
            ),
            (TargetType::Unmapped, "-"),
        ];
        for (t, want) in cases {
            assert_eq!(t.to_string(), want, "{t:?}");
        }
    }

    /// The `--json` report carries a target type as its DDL string, as it did before the type was typed.
    #[test]
    fn a_target_type_serialises_as_its_ddl_string() {
        let spec = ch(&RivetType::List {
            inner: Box::new(RivetType::Uuid),
        });
        let v = serde_json::to_value(&spec).unwrap();
        assert_eq!(v["target_type"], "Array(Nullable(UUID))");
        assert_eq!(v["autoload_type"], "Array(Nullable(FixedString(16)))");
    }

    fn duck(rt: &RivetType) -> TargetColumnSpec {
        ExportTarget::DuckDb.resolve_column(input(rt))
    }
    fn sf(rt: &RivetType) -> TargetColumnSpec {
        ExportTarget::Snowflake.resolve_column(input(rt))
    }
    fn ch(rt: &RivetType) -> TargetColumnSpec {
        ExportTarget::ClickHouse.resolve_column(input(rt))
    }

    // ── nanosecond timestamp (`timestamp_ns` override) per-target autoload ────
    // The default `timestamp` is microsecond; ns is opt-in (datetime2(7)). These
    // pin the autoload truth verified live on 2026-06-07 so the preflight report
    // doesn't claim the microsecond behaviour for a ns column.

    #[test]
    fn bq_nanosecond_timestamp_autoloads_as_int64() {
        let ns = RivetType::Timestamp {
            unit: super::super::TimeUnit::Nanosecond,
            timezone: None,
        };
        let s = bq(&ns);
        assert_eq!(s.target_type, "INT64");
        assert_eq!(s.autoload_type, "INT64");
        assert_eq!(s.status, TargetStatus::Warn);
        // No lossless temporal recovery (ns→µs is lossy) — like the UINT64 case.
        assert!(s.cast_sql.is_none(), "ns→BQ has no lossless temporal cast");
    }

    #[test]
    fn duckdb_nanosecond_timestamp_is_native_timestamp_ns() {
        let ns = RivetType::Timestamp {
            unit: super::super::TimeUnit::Nanosecond,
            timezone: None,
        };
        let s = duck(&ns);
        assert_eq!(s.target_type, "TIMESTAMP_NS");
        assert_eq!(s.status, TargetStatus::Ok);
    }

    #[test]
    fn snowflake_nanosecond_timestamp_recovers_losslessly_at_scale_9() {
        let ns = RivetType::Timestamp {
            unit: super::super::TimeUnit::Nanosecond,
            timezone: None,
        };
        let s = sf(&ns);
        assert_eq!(s.target_type, "TIMESTAMP_NTZ");
        assert_eq!(s.autoload_type, "NUMBER(38,0)");
        // Lossless recovery (TIMESTAMP_NTZ holds 9 digits) → cast_sql is Some at
        // scale 9 (not the µs scale 6); `{col}` is substituted with the column.
        assert_eq!(s.cast_sql.as_deref(), Some(r#"TO_TIMESTAMP_NTZ("c", 9)"#));
    }

    // ── dispatch on RivetType, not Arrow — the headline fix ──────────────────

    #[test]
    fn bq_uuid_lands_as_the_bytes_it_autoloads_as() {
        // Declared STRING, the 16 bytes landed as invalid text: unreadable, and the
        // TO_HEX(col) hint did not compile against a STRING column.
        let s = bq(&RivetType::Uuid);
        assert_eq!(s.target_type, "BYTES");
        assert_eq!(s.autoload_type, "BYTES");
        assert_eq!(s.status, TargetStatus::Warn);
        assert!(s.note.unwrap().contains("TO_HEX(col)"));
        assert_eq!(s.cast_sql, None, "nothing diverges, so nothing to recover");
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
        let s = bq(&RivetType::Decimal {
            precision: 18,
            scale: 2,
        });
        assert_eq!(s.target_type, "NUMERIC");
        assert_eq!(s.status, TargetStatus::Ok);
    }

    #[test]
    fn bq_decimal_escalates_to_bignumeric() {
        let s = bq(&RivetType::Decimal {
            precision: 38,
            scale: 9,
        });
        assert_eq!(s.target_type, "BIGNUMERIC");
        assert_eq!(s.status, TargetStatus::Ok);
    }

    #[test]
    fn bq_decimal_negative_scale_fails() {
        let s = bq(&RivetType::Decimal {
            precision: 5,
            scale: -2,
        });
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
    fn bq_list_declares_loadable_array_struct_item_ddl() {
        // The array target_type MUST be a valid, loadable standard-SQL DDL that
        // matches rivet's `item`-named Parquet list element. Old code emitted
        // `REPEATED STRING` — invalid DDL, `rivet load` → BigQuery syntax error
        // ("Expected ) or , but got identifier STRING"). Guard the exact shape.
        let t = RivetType::List {
            inner: Box::new(RivetType::String),
        };
        let s = bq(&t);
        assert_eq!(s.target_type, "ARRAY<STRUCT<item STRING>>");
        // warn: BigQuery autodetect produces the same nested shape → no divergence.
        assert_eq!(s.autoload_type, "ARRAY<STRUCT<item STRING>>");
        assert!(
            !s.target_type.to_string().starts_with("REPEATED "),
            "must not emit invalid REPEATED DDL"
        );
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
            RivetType::List {
                inner: Box::new(RivetType::Int64),
            },
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
            duck(&RivetType::Decimal {
                precision: 18,
                scale: 2
            })
            .target_type,
            "DECIMAL(18,2)"
        );
        assert_eq!(
            duck(&RivetType::List {
                inner: Box::new(RivetType::Int64)
            })
            .target_type,
            "BIGINT[]"
        );
    }

    #[test]
    fn parse_accepts_aliases() {
        assert_eq!(ExportTarget::parse("bq"), Some(ExportTarget::BigQuery));
        assert_eq!(
            ExportTarget::parse("BigQuery"),
            Some(ExportTarget::BigQuery)
        );
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
    // that overflowed into INT64). See the process rules "Remediation hints must recover
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
        assert!(
            bq(&RivetType::Json)
                .cast_sql
                .unwrap()
                .contains("PARSE_JSON")
        );
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
        // NOTE: List is NOT here — arrays load natively as ARRAY<STRUCT<item T>>
        // (target == autoload, a warn), so they are no longer a divergence. See
        // `bq_list_declares_loadable_array_struct_item_ddl`.
        let cases = [RivetType::Json, RivetType::UInt64, naive];
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
            bq(&RivetType::Decimal {
                precision: 76,
                scale: 38
            })
            .status,
            TargetStatus::Ok
        );
        // One past precision overflows BIGNUMERIC → Fail, never a silent clamp.
        assert_eq!(
            bq(&RivetType::Decimal {
                precision: 77,
                scale: 38
            })
            .status,
            TargetStatus::Fail
        );
        // One past scale → Fail.
        assert_eq!(
            bq(&RivetType::Decimal {
                precision: 76,
                scale: 39
            })
            .status,
            TargetStatus::Fail
        );
        // BIGNUMERIC holds 38 integer digits: p - s past that is Fail, whatever p is.
        for (p, s, want) in [
            (38, 0, TargetStatus::Ok),
            (39, 1, TargetStatus::Ok),
            (39, 0, TargetStatus::Fail),
            (50, 0, TargetStatus::Fail),
            (65, 0, TargetStatus::Fail),
            (76, 0, TargetStatus::Fail),
        ] {
            let r = bq(&RivetType::Decimal {
                precision: p,
                scale: s,
            });
            assert_eq!(r.status, want, "decimal({p},{s}): {:?}", r.note);
        }
        // Between NUMERIC and BIGNUMERIC escalates rather than overflowing NUMERIC.
        assert_eq!(
            bq(&RivetType::Decimal {
                precision: 30,
                scale: 0
            })
            .target_type,
            "BIGNUMERIC"
        );
    }

    #[test]
    fn duckdb_decimal_over_38_autoloads_as_double_not_a_false_native_decimal() {
        // DuckDB DECIMAL maxes at precision 38; a wider decimal autoloads as DOUBLE
        // (lossy past 2^53) — verified live (pg_edge_decimal_boundaries_round_trip).
        // The resolver must tell that truth: autoload_type = DOUBLE, flagged as a
        // DIVERGENCE (target != autoload), and NO cast_sql — DOUBLE has already lost
        // precision at autoload, so a SELECT-time cast recovers nothing (narrow the
        // source precision instead).
        let s = duck(&RivetType::Decimal {
            precision: 40,
            scale: 2,
        });
        assert_eq!(s.status, TargetStatus::Warn);
        assert_eq!(
            s.autoload_type, "DOUBLE",
            "autoload_type must tell the truth (real DuckDB autoloads wide decimals as DOUBLE)"
        );
        assert_ne!(
            s.target_type, s.autoload_type,
            "a lossy autoload must be flagged as a divergence, not autoload==target"
        );
        assert!(
            s.cast_sql.is_none(),
            "DOUBLE is already lossy — no post-load cast recovers the dropped precision"
        );
    }

    #[test]
    fn snowflake_decimal_over_38_fails_not_falsely_ok() {
        // Snowflake NUMBER maxes at precision 38 — NUMBER(50,10) is not a valid
        // type. The resolver must NOT claim Ok for a type Snowflake would reject;
        // past 38 is a Fail (narrow precision at source, or load as FLOAT via a
        // declared schema), the same discipline BigQuery applies past BIGNUMERIC.
        assert_eq!(
            sf(&RivetType::Decimal {
                precision: 50,
                scale: 10
            })
            .status,
            TargetStatus::Fail,
            "p>38 has no exact Snowflake NUMBER type"
        );
        // The boundary is exactly 38: precision 38 is still ok.
        assert_eq!(
            sf(&RivetType::Decimal {
                precision: 38,
                scale: 10
            })
            .status,
            TargetStatus::Ok
        );
    }

    #[test]
    fn clickhouse_decimal_over_76_fails() {
        // ClickHouse Decimal caps at precision 76 (Decimal256) — past it is a Fail,
        // not a false Ok, mirroring the Snowflake(>38) / BigQuery(>76,38) guards.
        assert_eq!(
            ch(&RivetType::Decimal {
                precision: 80,
                scale: 2
            })
            .status,
            TargetStatus::Fail
        );
        // 76 (the Decimal256 ceiling) is still ok.
        assert_eq!(
            ch(&RivetType::Decimal {
                precision: 76,
                scale: 2
            })
            .status,
            TargetStatus::Ok
        );
    }

    /// `rivet load` refuses a non-identifier column on ClickHouse too, so `check` must say so.
    #[test]
    fn clickhouse_grades_a_column_name_the_load_refuses_as_fail() {
        let mut spec = TargetColumnSpec {
            column_name: "naïve col".into(),
            target_type: ChType::String.into(),
            autoload_type: ChType::String.into(),
            status: TargetStatus::Ok,
            note: None,
            cast_sql: None,
        };
        ExportTarget::ClickHouse.grade_column_name(&mut spec);
        assert_eq!(spec.status, TargetStatus::Fail, "{:?}", spec.note);
        let mut plain = TargetColumnSpec {
            column_name: "naive_col".into(),
            ..spec.clone()
        };
        plain.status = TargetStatus::Ok;
        plain.note = None;
        ExportTarget::ClickHouse.grade_column_name(&mut plain);
        assert_eq!(plain.status, TargetStatus::Ok);
    }

    #[test]
    fn clickhouse_time_loads_as_decimal_seconds_of_day() {
        // No TIME type: Int64 keeps whole seconds only, Decimal64(6) keeps the µs.
        let s = ch(&RivetType::Time {
            unit: super::super::TimeUnit::Microsecond,
        });
        assert_eq!(s.target_type, "Decimal64(6)");
        assert_eq!(s.autoload_type, "Int64");
        assert_eq!(s.status, TargetStatus::Warn);
    }

    #[test]
    fn clickhouse_says_what_its_timestamps_and_arrays_cannot_hold() {
        let ts = ch(&RivetType::Timestamp {
            unit: super::super::TimeUnit::Microsecond,
            timezone: None,
        });
        assert_eq!(
            (ts.target_type.to_string().as_str(), ts.status),
            ("DateTime64(6)", TargetStatus::Warn)
        );
        assert!(
            ts.note
                .as_deref()
                .unwrap_or("")
                .contains("1900-01-01 to 2299-12-31")
        );
        let list = ch(&RivetType::List {
            inner: Box::new(RivetType::Int32),
        });
        assert_eq!(list.status, TargetStatus::Warn);
        assert!(
            list.note
                .as_deref()
                .unwrap_or("")
                .contains("a NULL list loads as []")
        );
        let uuids = ch(&RivetType::List {
            inner: Box::new(RivetType::Uuid),
        });
        assert_eq!(
            (
                uuids.target_type.to_string().as_str(),
                uuids.autoload_type.to_string().as_str()
            ),
            ("Array(Nullable(UUID))", "Array(Nullable(FixedString(16)))"),
            "an array's autoload is its element's"
        );
        assert!(
            uuids
                .note
                .as_deref()
                .unwrap_or("")
                .contains("UUID autoloads as FixedString(16)"),
            "{:?}",
            uuids.note
        );
        let json = ch(&RivetType::List {
            inner: Box::new(RivetType::Json),
        });
        assert_eq!(json.autoload_type, "Array(Nullable(String))");
        let stamps = ch(&RivetType::List {
            inner: Box::new(RivetType::Timestamp {
                unit: super::super::TimeUnit::Microsecond,
                timezone: None,
            }),
        });
        assert!(
            stamps
                .note
                .as_deref()
                .unwrap_or("")
                .contains("1900-01-01 to 2299-12-31"),
            "an element's range warning reaches the array: {:?}",
            stamps.note
        );
    }

    #[test]
    fn clickhouse_nanosecond_timestamp_is_datetime64_9() {
        // DateTime64 holds a nanosecond naive timestamp natively at scale 9.
        let s = ch(&RivetType::Timestamp {
            unit: super::super::TimeUnit::Nanosecond,
            timezone: None,
        });
        assert_eq!(s.target_type, "DateTime64(9)");
        assert_eq!(
            s.status,
            TargetStatus::Warn,
            "the 1900-2299 range is said, not hidden"
        );
    }

    #[test]
    fn clickhouse_enum_autoloads_as_text() {
        // Enum labels ride as text (String), no divergence.
        let s = ch(&RivetType::Enum);
        assert_eq!(s.target_type, "String");
        assert_eq!(s.status, TargetStatus::Ok);
    }

    #[test]
    fn snowflake_enum_autoloads_as_text() {
        // Enum labels ride as text on Snowflake too — pins the SF Enum arm the
        // type-matrix live test never asserts.
        let s = sf(&RivetType::Enum);
        assert_eq!(
            s.status,
            TargetStatus::Ok,
            "enum labels are a clean text autoload"
        );
        assert_eq!(
            s.autoload_type, s.target_type,
            "a text enum has no autoload divergence"
        );
    }

    #[test]
    fn list_of_unsupported_element_fails_on_every_target() {
        // A nested unsupported element must be a Fail row (not a panic, not a
        // silently-dropped column) on every target — the arms exist per target
        // but were untested. `List<Unsupported>` is the only way to reach them.
        let bad = RivetType::List {
            inner: Box::new(RivetType::Unsupported {
                native_type: "geometry".into(),
                reason: "no Arrow mapping".into(),
            }),
        };
        for spec in [bq(&bad), duck(&bad), sf(&bad), ch(&bad)] {
            assert_eq!(
                spec.status,
                TargetStatus::Fail,
                "a list of an unsupported element must fail cleanly"
            );
        }
    }

    #[test]
    fn interval_resolves_to_a_text_or_native_type_per_target() {
        // Interval maps per target (BQ STRING / DuckDB INTERVAL / SF TEXT / CH String)
        // — pin it so a future arm edit can't silently drop it to Fail.
        assert_eq!(bq(&RivetType::Interval).target_type, "STRING");
        assert_eq!(duck(&RivetType::Interval).target_type, "INTERVAL");
        assert_eq!(sf(&RivetType::Interval).target_type, "TEXT");
        assert_eq!(ch(&RivetType::Interval).target_type, "String");
        for spec in [
            bq(&RivetType::Interval),
            duck(&RivetType::Interval),
            sf(&RivetType::Interval),
            ch(&RivetType::Interval),
        ] {
            assert_eq!(spec.status, TargetStatus::Ok);
        }
    }

    #[test]
    fn decimal_negative_scale_is_handled_not_dropped_per_target() {
        // Negative-scale decimals are rejected by the Parquet writer today, so this
        // arm is resolver-only — but it is live code and must not silently vanish.
        // BigQuery fails (no negative scale); DuckDB/Snowflake/ClickHouse warn +
        // route via a declared schema.
        let neg = RivetType::Decimal {
            precision: 10,
            scale: -2,
        };
        assert_eq!(bq(&neg).status, TargetStatus::Fail);
        assert_eq!(duck(&neg).status, TargetStatus::Warn);
        assert_eq!(sf(&neg).status, TargetStatus::Warn);
        assert_eq!(ch(&neg).status, TargetStatus::Warn);
        assert_eq!(
            ch(&neg).target_type,
            "Decimal(12, 0)",
            "never the bare `Decimal`, which ClickHouse reads as Decimal(10, 0)"
        );
        let wide = RivetType::Decimal {
            precision: 70,
            scale: -10,
        };
        assert_eq!(ch(&wide).status, TargetStatus::Fail);
        let edge = RivetType::Decimal {
            precision: 70,
            scale: -6,
        };
        assert_eq!(
            ch(&edge).target_type,
            "Decimal(76, 0)",
            "width 76 still fits"
        );
        let zero = RivetType::Decimal {
            precision: 10,
            scale: 0,
        };
        assert_eq!(
            (ch(&zero).status, ch(&zero).target_type.to_string().as_str()),
            (TargetStatus::Ok, "Decimal(10, 0)"),
            "scale 0 is an ordinary decimal"
        );
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
            TypeMapping::from_source(
                &SourceColumn::simple("attrs", "jsonb", true),
                RivetType::Json,
            ),
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
        assert!(
            !sql.contains("AS uid"),
            "uuid lands as the BYTES it autoloads as: no cast"
        );
        assert!(sql.contains("DATETIME(created_at) AS created_at"));
        // Arrays load natively as ARRAY<STRUCT<item T>> (== autoload), so they are
        // NOT recovered — no cast, no flatten (the warn note documents the optional
        // UNNEST). tags is still projected (see the projection-count test).
        assert!(!sql.contains("AS tags") && !sql.contains("UNNEST(tags)"));
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
            ("id", RivetType::Int64), // ok → passthrough
            (
                "amount",
                RivetType::Decimal {
                    precision: 18,
                    scale: 2,
                },
            ), // ok → passthrough
            ("attrs", RivetType::Json), // divergent → cast
            ("uid", RivetType::Uuid), // divergent → cast
            ("created_at", naive),    // divergent → cast
            (
                "tags",
                RivetType::List {
                    inner: Box::new(RivetType::String),
                },
            ), // native ARRAY<STRUCT<item T>> → passthrough (not a divergence)
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
        assert!(
            !body.contains("AS uid"),
            "uuid is BYTES on both sides: passthrough"
        );
        assert!(body.contains("DATETIME(created_at) AS created_at"));
        // tags (array) loads natively as ARRAY<STRUCT<item T>> → passthrough, not cast
        // (projected once — asserted by the count/contains checks above).
        assert!(!body.contains("AS tags") && !body.contains("UNNEST(tags)"));
    }

    #[test]
    fn clickhouse_recovery_sql_casts_uuid_from_its_own_cast_sql() {
        use super::super::SourceColumn;
        // The live clickhouse_load test hand-writes a toUUID expr; this pins the
        // resolver's OWN emitted recovery SQL so a regression in the Rust string is
        // caught offline. uuid diverges (FixedString(16) -> toUUID); json is a
        // load-schema note (cast_sql=None) so it passes through; scalars pass through.
        let cols: [(&str, RivetType); 4] = [
            ("id", RivetType::Int64),
            ("attrs", RivetType::Json),
            ("uid", RivetType::Uuid),
            ("k", RivetType::Int32),
        ];
        let mappings: Vec<_> = cols
            .iter()
            .cloned()
            .map(|(n, rt)| TypeMapping::from_source(&SourceColumn::simple(n, "x", true), rt))
            .collect();
        let specs = ExportTarget::ClickHouse.resolve_table(&mappings);
        let sql = ExportTarget::ClickHouse
            .recovery_sql(&specs, "events")
            .expect("ClickHouse has a recovery SQL");

        // uuid recovers via the resolver's emitted cast, not a hand-written expr.
        assert!(
            sql.contains("toUUID(concat(") && sql.contains("hex(uid)") && sql.contains("AS uid"),
            "uuid must recover via the emitted toUUID cast:\n{sql}"
        );
        // json has no lossless SELECT-time cast (declared at load) — passes through.
        assert!(sql.contains("  attrs") && !sql.contains("AS attrs"));
        // scalars pass through unchanged.
        assert!(sql.contains("  id") && !sql.contains("AS id"));
        // Reads the autoload staging table, writes the recovered MergeTree table.
        assert!(sql.contains("CREATE TABLE events ENGINE = MergeTree"));
        assert!(sql.contains("FROM events__staging"));
        // Exactly one projection per column — nothing dropped or duplicated.
        let body = sql
            .split("SELECT\n")
            .nth(1)
            .and_then(|s| s.split("\nFROM").next())
            .expect("recovery SQL has a SELECT … FROM body");
        assert_eq!(
            body.split(",\n").count(),
            cols.len(),
            "one projection per column:\n{body}"
        );
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
        let d = sf(&RivetType::Decimal {
            precision: 18,
            scale: 2,
        });
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
            TypeMapping::from_source(
                &SourceColumn::simple("attrs", "jsonb", true),
                RivetType::Json,
            ),
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
        assert_eq!(
            ExportTarget::parse("snowflake"),
            Some(ExportTarget::Snowflake)
        );
        assert_eq!(ExportTarget::parse("sf"), Some(ExportTarget::Snowflake));
    }

    #[test]
    fn parse_accepts_clickhouse() {
        assert_eq!(
            ExportTarget::parse("clickhouse"),
            Some(ExportTarget::ClickHouse)
        );
        assert_eq!(ExportTarget::parse("ch"), Some(ExportTarget::ClickHouse));
    }

    #[test]
    fn valid_target_names_lists_every_parseable_target() {
        // The "unknown target" error message reads from this; it must name
        // every target `parse` accepts, or the hint sends operators wrong.
        // snowflake regression: the old message hard-coded "bigquery, duckdb".
        let names = ExportTarget::valid_target_names();
        assert!(names.contains("snowflake"), "got: {names}");
        assert!(names.contains("bigquery"), "got: {names}");
        assert!(names.contains("duckdb"), "got: {names}");
        assert!(names.contains("clickhouse"), "got: {names}");
    }
}
