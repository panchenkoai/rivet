//! Export target compatibility checks (roadmap §16).
//!
//! Validates that an Arrow type produced by Rivet will load into the target
//! warehouse as the expected type. Currently supports BigQuery.
//!
//! BigQuery numeric limits (as of 2025):
//!   NUMERIC    — precision 1–29, scale 0–9
//!   BIGNUMERIC — precision 1–76, scale 0–38

use arrow::datatypes::DataType;
use serde::Serialize;

/// A supported downstream warehouse target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportTarget {
    BigQuery,
}

impl ExportTarget {
    #[allow(dead_code)]
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "bigquery" | "bq" => Some(Self::BigQuery),
            _ => None,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::BigQuery => "bigquery",
        }
    }
}

/// Status of a column's Arrow type against a specific target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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

/// One column's compatibility result against a target.
#[derive(Debug, Clone, Serialize)]
pub struct TargetCompat {
    /// Target type name (e.g. "NUMERIC", "BIGNUMERIC", "TIMESTAMP").
    pub target_type: String,
    pub status: TargetStatus,
    /// Optional note explaining a warning or failure.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl TargetCompat {
    fn ok(target_type: impl Into<String>) -> Self {
        Self {
            target_type: target_type.into(),
            status: TargetStatus::Ok,
            note: None,
        }
    }
    fn warn(target_type: impl Into<String>, note: impl Into<String>) -> Self {
        Self {
            target_type: target_type.into(),
            status: TargetStatus::Warn,
            note: Some(note.into()),
        }
    }
    fn fail(note: impl Into<String>) -> Self {
        Self {
            target_type: "-".into(),
            status: TargetStatus::Fail,
            note: Some(note.into()),
        }
    }
}

/// Check whether `arrow_type` (the type Rivet will write into Parquet) will
/// produce the expected type when loaded into `target`.
///
/// Returns `None` if `arrow_type` is `None` (unresolved / unsupported column).
pub fn check_target_compat(arrow_type: Option<&DataType>, target: ExportTarget) -> TargetCompat {
    match target {
        ExportTarget::BigQuery => bq_compat(arrow_type),
    }
}

// ── BigQuery ─────────────────────────────────────────────────────────────────

/// BigQuery NUMERIC precision/scale limits.
const BQ_NUMERIC_MAX_P: u8 = 29;
const BQ_NUMERIC_MAX_S: i8 = 9;
/// BigQuery BIGNUMERIC precision/scale limits.
const BQ_BIGNUMERIC_MAX_P: u8 = 76;
const BQ_BIGNUMERIC_MAX_S: i8 = 38;

fn bq_compat(arrow_type: Option<&DataType>) -> TargetCompat {
    let Some(dt) = arrow_type else {
        return TargetCompat::fail(
            "no Arrow type — column is unsupported or needs a type override",
        );
    };

    match dt {
        DataType::Boolean => TargetCompat::ok("BOOL"),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            TargetCompat::ok("INT64")
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => TargetCompat::ok("INT64"),
        DataType::UInt64 => TargetCompat::warn(
            "INT64",
            "UINT64 has no direct BigQuery type; values > INT64_MAX will overflow",
        ),
        DataType::Float32 => TargetCompat::ok("FLOAT64"),
        DataType::Float64 => TargetCompat::ok("FLOAT64"),

        DataType::Decimal128(p, s) => bq_decimal_compat(*p, *s),
        DataType::Decimal256(p, s) => bq_decimal256_compat(*p, *s),

        DataType::Date32 | DataType::Date64 => TargetCompat::ok("DATE"),
        DataType::Time32(_) | DataType::Time64(_) => TargetCompat::ok("TIME"),

        DataType::Timestamp(_, Some(_)) => TargetCompat::ok("TIMESTAMP"),
        DataType::Timestamp(_, None) => TargetCompat::ok("DATETIME"),

        DataType::Utf8 | DataType::LargeUtf8 => TargetCompat::ok("STRING"),
        DataType::Binary | DataType::LargeBinary => TargetCompat::ok("BYTES"),

        // BigQuery supports INTERVAL since 2022 but with limited arithmetic.
        DataType::Interval(_) => TargetCompat::warn(
            "INTERVAL",
            "BigQuery INTERVAL has limited arithmetic support; verify downstream queries",
        ),

        // Arrow List maps to BigQuery REPEATED fields in Parquet loads.
        DataType::List(field_ref) | DataType::LargeList(field_ref) => {
            let inner_compat = bq_compat(Some(field_ref.data_type()));
            match inner_compat.status {
                TargetStatus::Fail => TargetCompat::fail(format!(
                    "REPEATED {} (inner type unsupported: {})",
                    inner_compat.target_type,
                    inner_compat.note.unwrap_or_default()
                )),
                TargetStatus::Warn => TargetCompat::warn(
                    format!("REPEATED {}", inner_compat.target_type),
                    inner_compat.note.unwrap_or_default(),
                ),
                TargetStatus::Ok => {
                    TargetCompat::ok(format!("REPEATED {}", inner_compat.target_type))
                }
            }
        }

        other => TargetCompat::fail(format!(
            "Arrow type {other:?} has no supported BigQuery mapping"
        )),
    }
}

fn bq_decimal_compat(p: u8, s: i8) -> TargetCompat {
    // Negative scale (e.g. numeric(5,-2)) is not supported by BigQuery NUMERIC/BIGNUMERIC.
    if s < 0 {
        return TargetCompat::fail(format!(
            "BigQuery does not support negative scale; \
             Decimal128({p},{s}) would need to be cast to STRING or INT64"
        ));
    }
    if p <= BQ_NUMERIC_MAX_P && s <= BQ_NUMERIC_MAX_S {
        return TargetCompat::ok("NUMERIC");
    }
    // Fits in BIGNUMERIC?
    if p <= BQ_BIGNUMERIC_MAX_P && s <= BQ_BIGNUMERIC_MAX_S {
        return TargetCompat::ok("BIGNUMERIC");
    }
    TargetCompat::fail(format!(
        "Decimal128({p},{s}) exceeds BigQuery BIGNUMERIC limits \
         (max precision 76, max scale 38)"
    ))
}

fn bq_decimal256_compat(p: u8, s: i8) -> TargetCompat {
    if s < 0 {
        return TargetCompat::fail(format!(
            "BigQuery does not support negative scale; \
             Decimal256({p},{s}) cannot be loaded directly"
        ));
    }
    if p <= BQ_BIGNUMERIC_MAX_P && s <= BQ_BIGNUMERIC_MAX_S {
        // Close to the limit — warn if within 5 of the ceiling.
        if p > BQ_BIGNUMERIC_MAX_P - 5 || s > BQ_BIGNUMERIC_MAX_S - 5 {
            return TargetCompat::warn(
                "BIGNUMERIC",
                format!(
                    "Decimal256({p},{s}) is near BigQuery BIGNUMERIC limits \
                     (max 76,38); verify no overflow at load time"
                ),
            );
        }
        return TargetCompat::ok("BIGNUMERIC");
    }
    TargetCompat::fail(format!(
        "Decimal256({p},{s}) exceeds BigQuery BIGNUMERIC limits \
         (max precision 76, max scale 38)"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::TimeUnit as ArrowTimeUnit;

    fn bq(dt: DataType) -> TargetCompat {
        check_target_compat(Some(&dt), ExportTarget::BigQuery)
    }

    #[test]
    fn numeric_within_limits_maps_to_numeric() {
        let c = bq(DataType::Decimal128(18, 2));
        assert_eq!(c.target_type, "NUMERIC");
        assert_eq!(c.status, TargetStatus::Ok);
    }

    #[test]
    fn numeric_above_29_precision_escalates_to_bignumeric() {
        let c = bq(DataType::Decimal128(38, 9));
        assert_eq!(c.target_type, "BIGNUMERIC");
        assert_eq!(c.status, TargetStatus::Ok);
    }

    #[test]
    fn numeric_scale_above_9_goes_bignumeric() {
        let c = bq(DataType::Decimal128(18, 12));
        assert_eq!(c.target_type, "BIGNUMERIC");
        assert_eq!(c.status, TargetStatus::Ok);
    }

    #[test]
    fn decimal256_normal_maps_to_bignumeric() {
        let c = bq(DataType::Decimal256(60, 20));
        assert_eq!(c.target_type, "BIGNUMERIC");
        assert_eq!(c.status, TargetStatus::Ok);
    }

    #[test]
    fn decimal256_near_limit_warns() {
        let c = bq(DataType::Decimal256(75, 35));
        assert_eq!(c.target_type, "BIGNUMERIC");
        assert_eq!(c.status, TargetStatus::Warn);
    }

    #[test]
    fn decimal_negative_scale_fails() {
        let c = bq(DataType::Decimal128(5, -2));
        assert_eq!(c.status, TargetStatus::Fail);
    }

    #[test]
    fn timestamptz_maps_to_timestamp() {
        let c = bq(DataType::Timestamp(
            ArrowTimeUnit::Microsecond,
            Some("UTC".into()),
        ));
        assert_eq!(c.target_type, "TIMESTAMP");
        assert_eq!(c.status, TargetStatus::Ok);
    }

    #[test]
    fn timestamp_no_tz_maps_to_datetime() {
        let c = bq(DataType::Timestamp(ArrowTimeUnit::Microsecond, None));
        assert_eq!(c.target_type, "DATETIME");
        assert_eq!(c.status, TargetStatus::Ok);
    }

    #[test]
    fn none_arrow_type_fails() {
        let c = check_target_compat(None, ExportTarget::BigQuery);
        assert_eq!(c.status, TargetStatus::Fail);
    }

    #[test]
    fn uint64_warns_about_overflow() {
        let c = bq(DataType::UInt64);
        assert_eq!(c.target_type, "INT64");
        assert_eq!(c.status, TargetStatus::Warn);
    }

    #[test]
    fn standard_types_are_ok() {
        for (dt, expected_bq) in [
            (DataType::Boolean, "BOOL"),
            (DataType::Int64, "INT64"),
            (DataType::Float64, "FLOAT64"),
            (DataType::Date32, "DATE"),
            (DataType::Utf8, "STRING"),
            (DataType::Binary, "BYTES"),
        ] {
            let c = bq(dt);
            assert_eq!(
                c.status,
                TargetStatus::Ok,
                "Arrow {:?} should be ok",
                c.target_type
            );
            assert_eq!(c.target_type, expected_bq);
        }
    }
}
