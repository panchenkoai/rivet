//! Parser for column type override strings (roadmap §8).
//!
//! Users write short type strings in `rivet.yaml`:
//! ```yaml
//! exports:
//!   - name: payments
//!     columns:
//!       amount: decimal(18,2)
//!       created_at: timestamp_tz
//!       payload: json
//! ```
//! [`parse_type_str`] converts each string to the canonical [`RivetType`],
//! failing at config-load time (before any export runs) if the string is
//! invalid.

use super::{RivetType, TimeUnit};
use crate::error::Result;

/// Parse a user-supplied column type string into a [`RivetType`].
///
/// Case-insensitive. Whitespace around delimiters is trimmed.
/// Returns an error with an actionable message if the string is not
/// a recognised type — the error is surfaced before the export starts
/// so the user can fix their `rivet.yaml`.
pub fn parse_type_str(s: &str) -> Result<RivetType> {
    let normalised = s.trim().to_ascii_lowercase();
    let normalised = normalised.as_str();

    if let Some(inner) = normalised
        .strip_prefix("decimal(")
        .and_then(|r| r.strip_suffix(')'))
    {
        return parse_decimal_params(s, inner);
    }
    if let Some(inner) = normalised
        .strip_prefix("numeric(")
        .and_then(|r| r.strip_suffix(')'))
    {
        return parse_decimal_params(s, inner);
    }

    match normalised {
        "bool" | "boolean" => Ok(RivetType::Bool),
        "int2" | "smallint" | "int16" => Ok(RivetType::Int16),
        "int4" | "int" | "integer" | "int32" => Ok(RivetType::Int32),
        "int8" | "bigint" | "int64" => Ok(RivetType::Int64),
        "float4" | "real" | "float32" => Ok(RivetType::Float32),
        "float8" | "double" | "double precision" | "float64" => Ok(RivetType::Float64),
        "text" | "varchar" | "string" | "char" | "bpchar" | "name" => Ok(RivetType::String),
        "binary" | "bytea" | "blob" | "varbinary" => Ok(RivetType::Binary),
        "date" => Ok(RivetType::Date),
        "json" | "jsonb" => Ok(RivetType::Json),
        "uuid" => Ok(RivetType::Uuid),
        "timestamp" | "timestamp without time zone" => Ok(RivetType::Timestamp {
            unit: TimeUnit::Microsecond,
            timezone: None,
        }),
        // Nanosecond opt-in: preserves a source's sub-microsecond fractional
        // seconds (e.g. SQL Server `datetime2(7)`'s 100 ns tick) that the default
        // microsecond mapping truncates. Arrow nanosecond timestamps are i64 ns,
        // so the representable range is 1677-09-21 .. 2262-04-11 — values outside
        // it export as NULL. Default to `timestamp` (microsecond, full range)
        // unless you specifically need the extra precision and your data is in
        // range. See docs/type-mapping.md.
        "timestamp_ns" => Ok(RivetType::Timestamp {
            unit: TimeUnit::Nanosecond,
            timezone: None,
        }),
        "timestamp_tz" | "timestamptz" | "timestamp with time zone" | "timestamp_utc" => {
            Ok(RivetType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: Some("UTC".into()),
            })
        }
        "timestamp_tz_ns" | "timestamptz_ns" => Ok(RivetType::Timestamp {
            unit: TimeUnit::Nanosecond,
            timezone: Some("UTC".into()),
        }),
        _ => anyhow::bail!(
            "column override: unrecognised type '{}'. \
             Supported: bool, int2/int4/int8, float4/float8, decimal(p,s), \
             date, timestamp, timestamp_ns, timestamp_tz, timestamp_tz_ns, \
             text, binary, json, uuid",
            s
        ),
    }
}

fn parse_decimal_params(original: &str, inner: &str) -> Result<RivetType> {
    let mut parts = inner.splitn(2, ',');
    let precision_str = parts.next().ok_or_else(|| {
        anyhow::anyhow!(
            "column override: expected decimal(precision,scale) in '{}'",
            original
        )
    })?;
    let scale_str = parts.next().ok_or_else(|| {
        anyhow::anyhow!(
            "column override: missing scale in '{}' — use decimal(precision,scale)",
            original
        )
    })?;

    let precision: u8 = precision_str.trim().parse().map_err(|_| {
        anyhow::anyhow!(
            "column override: precision '{}' is not a valid integer (0–76) in '{}'",
            precision_str.trim(),
            original
        )
    })?;
    let scale: i8 = scale_str.trim().parse().map_err(|_| {
        anyhow::anyhow!(
            "column override: scale '{}' is not a valid integer (-128..127) in '{}'",
            scale_str.trim(),
            original
        )
    })?;

    if precision == 0 || precision > 76 {
        anyhow::bail!(
            "column override: precision {} is out of range (1..=76) in '{}'",
            precision,
            original
        );
    }
    if scale > precision as i8 {
        anyhow::bail!(
            "column override: scale {} exceeds precision {} in '{}'",
            scale,
            precision,
            original
        );
    }

    Ok(RivetType::Decimal { precision, scale })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_parses_precision_and_scale() {
        assert_eq!(
            parse_type_str("decimal(18,2)").unwrap(),
            RivetType::Decimal {
                precision: 18,
                scale: 2
            }
        );
        assert_eq!(
            parse_type_str("decimal(38,9)").unwrap(),
            RivetType::Decimal {
                precision: 38,
                scale: 9
            }
        );
    }

    #[test]
    fn decimal_with_spaces_around_comma() {
        assert_eq!(
            parse_type_str("decimal(18, 2)").unwrap(),
            RivetType::Decimal {
                precision: 18,
                scale: 2
            }
        );
    }

    #[test]
    fn decimal_allows_negative_scale() {
        assert_eq!(
            parse_type_str("decimal(5,-2)").unwrap(),
            RivetType::Decimal {
                precision: 5,
                scale: -2
            }
        );
    }

    #[test]
    fn numeric_is_alias_for_decimal() {
        assert_eq!(
            parse_type_str("numeric(18,2)").unwrap(),
            parse_type_str("decimal(18,2)").unwrap()
        );
    }

    #[test]
    fn scale_exceeding_precision_is_rejected() {
        assert!(parse_type_str("decimal(2,5)").is_err());
    }

    #[test]
    fn precision_out_of_range_is_rejected() {
        assert!(parse_type_str("decimal(0,0)").is_err());
        assert!(parse_type_str("decimal(77,0)").is_err());
    }

    #[test]
    fn decimal_without_params_is_rejected() {
        // bare "decimal" without (p,s) must fail — unbounded numeric is not safe
        assert!(parse_type_str("decimal").is_err());
        assert!(parse_type_str("numeric").is_err());
    }

    #[test]
    fn timestamp_variants() {
        assert_eq!(
            parse_type_str("timestamp").unwrap(),
            RivetType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: None
            }
        );
        assert_eq!(
            parse_type_str("timestamp_tz").unwrap(),
            RivetType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: Some("UTC".into())
            }
        );
        assert_eq!(
            parse_type_str("timestamptz").unwrap(),
            RivetType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: Some("UTC".into())
            }
        );
    }

    #[test]
    fn timestamp_nanosecond_opt_in_variants() {
        // The ns opt-in for sub-microsecond sources (e.g. SQL Server
        // datetime2(7)); default `timestamp` stays microsecond.
        assert_eq!(
            parse_type_str("timestamp_ns").unwrap(),
            RivetType::Timestamp {
                unit: TimeUnit::Nanosecond,
                timezone: None
            }
        );
        assert_eq!(
            parse_type_str("timestamp_tz_ns").unwrap(),
            RivetType::Timestamp {
                unit: TimeUnit::Nanosecond,
                timezone: Some("UTC".into())
            }
        );
        assert_eq!(
            parse_type_str("timestamptz_ns").unwrap(),
            RivetType::Timestamp {
                unit: TimeUnit::Nanosecond,
                timezone: Some("UTC".into())
            }
        );
    }

    #[test]
    fn primitive_types() {
        assert_eq!(parse_type_str("bool").unwrap(), RivetType::Bool);
        assert_eq!(parse_type_str("bigint").unwrap(), RivetType::Int64);
        assert_eq!(parse_type_str("json").unwrap(), RivetType::Json);
        assert_eq!(parse_type_str("uuid").unwrap(), RivetType::Uuid);
    }

    #[test]
    fn case_insensitive() {
        assert_eq!(
            parse_type_str("DECIMAL(18,2)").unwrap(),
            RivetType::Decimal {
                precision: 18,
                scale: 2
            }
        );
        assert_eq!(parse_type_str("BOOL").unwrap(), RivetType::Bool);
        assert_eq!(
            parse_type_str("TIMESTAMP_TZ").unwrap(),
            RivetType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: Some("UTC".into())
            }
        );
    }

    #[test]
    fn unrecognised_type_returns_actionable_error() {
        let err = parse_type_str("geometry").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("geometry"),
            "error should name the bad type: {msg}"
        );
        assert!(
            msg.contains("decimal(p,s)"),
            "error should list alternatives: {msg}"
        );
    }
}
