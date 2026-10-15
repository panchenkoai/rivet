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
        _ => crate::rivet_bail!(
            crate::error::codes::CONFIG_COLUMN_OVERRIDE_TYPE_INVALID,
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
        crate::error::coded(
            crate::error::codes::CONFIG_COLUMN_OVERRIDE_TYPE_INVALID,
            format!(
                "column override: expected decimal(precision,scale) in '{}'",
                original
            ),
        )
    })?;
    let scale_str = parts.next().ok_or_else(|| {
        crate::error::coded(
            crate::error::codes::CONFIG_COLUMN_OVERRIDE_TYPE_INVALID,
            format!(
                "column override: missing scale in '{}' — use decimal(precision,scale)",
                original
            ),
        )
    })?;

    let precision: u8 = precision_str.trim().parse().map_err(|_| {
        crate::error::coded(
            crate::error::codes::CONFIG_COLUMN_OVERRIDE_TYPE_INVALID,
            format!(
                "column override: precision '{}' is not a valid integer (0–76) in '{}'",
                precision_str.trim(),
                original
            ),
        )
    })?;
    let scale: i8 = scale_str.trim().parse().map_err(|_| {
        crate::error::coded(
            crate::error::codes::CONFIG_COLUMN_OVERRIDE_TYPE_INVALID,
            format!(
                "column override: scale '{}' is not a valid integer (-128..127) in '{}'",
                scale_str.trim(),
                original
            ),
        )
    })?;

    if precision == 0 || precision > 76 {
        crate::rivet_bail!(
            crate::error::codes::CONFIG_COLUMN_OVERRIDE_TYPE_INVALID,
            "column override: precision {} is out of range (1..=76) in '{}'",
            precision,
            original
        );
    }
    if scale > precision as i8 {
        crate::rivet_bail!(
            crate::error::codes::CONFIG_COLUMN_OVERRIDE_TYPE_INVALID,
            "column override: scale {} exceeds precision {} in '{}'",
            scale,
            precision,
            original
        );
    }

    Ok(RivetType::Decimal { precision, scale })
}

/// Parse the raw `columns:` map from `ExportConfig` into typed [`ColumnOverrides`].
///
/// Fails early (at plan-build time) with an actionable error so the user
/// fixes their `rivet.yaml` before the export runs.
pub fn parse_column_overrides(
    raw: &std::collections::HashMap<String, String>,
    export_name: &str,
) -> Result<crate::types::ColumnOverrides> {
    raw.iter()
        .map(|(col, type_str)| {
            crate::types::parse_type_str(type_str)
                .map(|t| (col.clone(), t))
                .map_err(|e| {
                    crate::error::recoded(
                        &e,
                        crate::error::codes::CONFIG_COLUMN_OVERRIDE_TYPE_INVALID,
                        format!(
                            "export '{}': column override for '{}': {}",
                            export_name, col, e
                        ),
                    )
                })
        })
        .collect()
}

/// Refuse a `columns:` key that names no result column as spelled but one ignoring case; `check` and `run` share it.
pub(crate) fn refuse_override_case_miss(keys: &[String], names: &[&str]) -> Result<()> {
    let miss = keys.iter().find_map(|k| {
        if names.contains(&k.as_str()) {
            return None;
        }
        names
            .iter()
            .find(|n| n.eq_ignore_ascii_case(k))
            .map(|n| (k.as_str(), *n))
    });
    if let Some((key, real)) = miss {
        crate::rivet_bail!(
            crate::error::codes::CONFIG_COLUMN_OVERRIDE_CASE,
            "`columns: {{ {key}: ... }}` names no result-set column; the result names it \
             `{real}`, and override keys match exactly, so the override would be silently \
             ignored. Spell the key `{real}`."
        );
    }
    Ok(())
}

/// Refuse a `cursor_column` that names no result column as spelled but one ignoring case; the cursor could never advance.
pub(crate) fn refuse_cursor_case_miss(cursor: &str, names: &[&str]) -> Result<()> {
    if names.contains(&cursor) {
        return Ok(());
    }
    if let Some(real) = names.iter().find(|n| n.eq_ignore_ascii_case(cursor)) {
        crate::rivet_bail!(
            crate::error::codes::CONFIG_CURSOR_COLUMN_CASE,
            "`cursor_column: {cursor}` is not in the result set, which names the column `{real}` — the \
             cursor could never advance and every run would re-read the whole table. Set \
             `cursor_column: {real}`."
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cursor_named_only_in_another_case_is_refused_by_code_and_names_the_spelling() {
        let names = ["id", "updated_at"];
        assert!(refuse_cursor_case_miss("updated_at", &names).is_ok());
        assert!(refuse_cursor_case_miss("missing", &names).is_ok());
        let err = refuse_cursor_case_miss("UPDATED_AT", &names).unwrap_err();
        assert_eq!(
            crate::error::error_code(&err),
            Some("RIVET_CONFIG_CURSOR_COLUMN_CASE")
        );
        assert!(format!("{err:#}").contains("Set `cursor_column: updated_at`."));
        assert!(refuse_cursor_case_miss("V", &["v", "V"]).is_ok());
    }

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

    /// Every spelling of each primitive names its own type, and a spelling of one is not another.
    #[test]
    fn every_primitive_spelling_names_its_own_type() {
        for (names, want) in [
            (&["int2", "smallint", "int16"][..], RivetType::Int16),
            (&["int4", "int", "integer", "int32"][..], RivetType::Int32),
            (&["int8", "bigint", "int64"][..], RivetType::Int64),
            (&["float4", "real", "float32"][..], RivetType::Float32),
            (
                &["float8", "double", "double precision", "float64"][..],
                RivetType::Float64,
            ),
            (&["date"][..], RivetType::Date),
            (&["bool", "boolean"][..], RivetType::Bool),
            (
                &["binary", "bytea", "blob", "varbinary"][..],
                RivetType::Binary,
            ),
            (
                &["text", "varchar", "string", "char", "bpchar", "name"][..],
                RivetType::String,
            ),
            (&["json", "jsonb"][..], RivetType::Json),
        ] {
            for name in names {
                assert_eq!(parse_type_str(name).unwrap(), want, "{name}");
            }
        }
    }

    /// The decimal bounds are inclusive: precision 76 and a scale equal to the precision are types; one past either is refused.
    #[test]
    fn the_decimal_bounds_are_inclusive() {
        let decimal = |precision, scale| RivetType::Decimal { precision, scale };
        assert_eq!(parse_type_str("decimal(76,0)").unwrap(), decimal(76, 0));
        assert_eq!(parse_type_str("decimal(5,5)").unwrap(), decimal(5, 5));
        assert_eq!(parse_type_str("decimal(1,1)").unwrap(), decimal(1, 1));
        let past = |s: &str| parse_type_str(s).unwrap_err().to_string();
        assert!(past("decimal(77,0)").contains("out of range (1..=76)"));
        assert!(past("decimal(5,6)").contains("scale 6 exceeds precision 5"));
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

#[cfg(test)]
mod error_code_tests {
    #[test]
    fn an_unparseable_override_type_is_refused_by_code_through_the_export_wrapper() {
        for s in [
            "nope",
            "decimal(0,0)",
            "decimal(5,9)",
            "decimal(x,1)",
            "decimal(5)",
        ] {
            let err = super::parse_type_str(s).unwrap_err();
            crate::error::assert_coded(&err, "RIVET_CONFIG_COLUMN_OVERRIDE_TYPE_INVALID", 1);
        }
        let raw = std::collections::HashMap::from([("c".to_string(), "nope".to_string())]);
        let err = super::parse_column_overrides(&raw, "orders").unwrap_err();
        crate::error::assert_coded(&err, "RIVET_CONFIG_COLUMN_OVERRIDE_TYPE_INVALID", 1);
        assert!(
            err.to_string()
                .starts_with("export 'orders': column override for 'c':")
        );
    }
}
