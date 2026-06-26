//! `rivet check --type-report` — tabular and JSON output.
//!
//! Roadmap §9 ("Type Fidelity Report") and §16 ("BigQuery Compatibility Layer").
//! Renders a `Vec<TypeMapping>` plus any `PolicyViolation`s as either a
//! fixed-width terminal table or newline-delimited JSON.

use serde::Serialize;

use crate::config::{Config, ExportConfig, FormatType, SourceType};
use crate::error::Result;
use crate::source;
use crate::types::{
    ColumnOverrides, TypeFidelity,
    policy::{PolicyAction, PolicyViolation, TypePolicy},
    target::{ExportTarget, TargetInput, TargetStatus},
};

/// One row in the type report (and the JSON output — roadmap §9).
#[derive(Serialize)]
pub struct TypeReportRow {
    pub column: String,
    pub source_type: String,
    pub rivet_type: String,
    pub arrow_type: String,
    pub fidelity: TypeFidelity,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    /// Present when `--target` is set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_status: Option<TargetStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_note: Option<String>,
    /// Type a generic Parquet reader infers without a declared schema, surfaced
    /// only when it diverges from `target_type` (e.g. BigQuery autoloads JSON
    /// as BYTES). Present when `--target` is set and autoload ≠ native.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub autoload_type: Option<String>,
    /// Materialization / load-schema hint (L5) to recover the native type.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cast_sql: Option<String>,
}

/// One export's type-report data.
#[derive(Serialize)]
pub struct ExportTypeReport {
    pub export: String,
    pub columns: Vec<TypeReportRow>,
    pub violations: Vec<PolicyViolation>,
    /// True when any column failed target-compatibility.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub target_failures: bool,
    /// Target-native recovery SQL (ADR-0014 L5): a post-load transform that
    /// recovers types bare autoload degrades (BigQuery JSON/UUID/DATETIME).
    /// `None` for targets that autoload faithfully (DuckDB) or when no target
    /// is set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery_sql: Option<String>,
}

impl ExportTypeReport {
    pub fn has_fatal(&self) -> bool {
        self.violations.iter().any(|v| v.fatal)
    }

    pub fn has_target_fail(&self) -> bool {
        self.target_failures
    }
}

/// Collect type mappings for one export from a live connection.
pub fn collect_report(
    config: &Config,
    export: &ExportConfig,
    column_overrides: &ColumnOverrides,
    policy: &TypePolicy,
    target: Option<ExportTarget>,
    config_dir: &std::path::Path,
    params: Option<&std::collections::HashMap<String, String>>,
) -> Result<ExportTypeReport> {
    let url = config.source.resolve_url()?;
    let tls = config.source.tls.as_ref();
    // Resolve the effective query the same way the export pipeline does, so the
    // `table:` shortcut (and `query_file:` / `${var}` params) produce a real
    // query instead of an empty string.
    let query = export.resolve_query(config_dir, params)?;

    let mut src: Box<dyn source::Source> = match config.source.source_type {
        SourceType::Postgres => Box::new(source::postgres::PostgresSource::connect_with_tls(
            &url, tls,
        )?),
        SourceType::Mysql => Box::new(source::mysql::MysqlSource::connect_with_tls(&url, tls)?),
        SourceType::Mssql => Box::new(source::mssql::MssqlSource::connect_with_tls(&url, tls)?),
    };

    let mut mappings = src.type_mappings(&query, column_overrides)?;

    // #32 (column-applicability): a `columns:` override that *narrows* the
    // source type — e.g. `price numeric(10,2)` → `decimal(20,0)` — drops the
    // two fractional digits at `run`, but `derive_fidelity` only sees the
    // resolved (overridden) `RivetType` and labels it `exact`. That makes
    // `check --type-report` disagree with what `run` actually does (a
    // check↔run gap). Re-probe the *autodetected* source types (no overrides)
    // and downgrade any overridden column whose override narrows scale or
    // integer-digit capacity to `Lossy`, with a warning that says why. We only
    // downgrade when the source type is confidently known (autodetect resolved
    // it) — never on a guess, so we don't fabricate a loss we can't prove.
    if !column_overrides.is_empty() {
        let source_mappings = src.type_mappings(&query, &ColumnOverrides::new())?;
        let source_by_name: std::collections::HashMap<&str, &crate::types::RivetType> =
            source_mappings
                .iter()
                .map(|m| (m.column_name.as_str(), &m.rivet_type))
                .collect();
        for m in &mut mappings {
            if !column_overrides.contains_key(&m.column_name) {
                continue;
            }
            if let Some(&src_type) = source_by_name.get(m.column_name.as_str())
                && let Some(reason) = override_narrows(src_type, &m.rivet_type)
            {
                m.fidelity = TypeFidelity::Lossy;
                m.warnings.push(reason);
            }
        }
    }

    let mut violations = policy.validate(&mappings);

    // Format-awareness: type resolution above is for the Parquet representation,
    // but a CSV export rejects columns CSV can't serialize (lists, etc.) up front
    // at writer creation. Surface those here so `check`/`--strict` agree with the
    // run — otherwise a list column reports "safe" only for the CSV run to fail
    // loud ("CSV cannot serialize column …"). Fatality follows the unsupported
    // policy action (Fail under `--strict`, Warn otherwise).
    if export.format == FormatType::Csv {
        let fatal = policy.on_unsupported_type == PolicyAction::Fail;
        for m in &mappings {
            if let Some(dt) = m.arrow_type.as_ref()
                && !crate::format::csv::csv_serializable(dt)
            {
                violations.push(PolicyViolation {
                    column_name: m.column_name.clone(),
                    fidelity: TypeFidelity::Unsupported,
                    message: format!(
                        "column '{}' (Arrow {dt:?}) cannot be serialized to CSV — \
                         use `format: parquet` or drop it from the query",
                        m.column_name
                    ),
                    fatal,
                });
            }
        }
    }

    let mut target_failures = false;
    let rows = mappings
        .iter()
        .map(|m| {
            let (target_type, target_status, target_note, autoload_type, cast_sql) =
                if let Some(tgt) = target {
                    let spec = tgt.resolve_column(TargetInput::from(m));
                    if spec.status == TargetStatus::Fail {
                        target_failures = true;
                    }
                    // Surface the autoloaded type only when it diverges from the
                    // native type — that divergence is the operator-facing point.
                    let autoload =
                        (spec.autoload_type != spec.target_type).then_some(spec.autoload_type);
                    (
                        Some(spec.target_type),
                        Some(spec.status),
                        spec.note,
                        autoload,
                        spec.cast_sql,
                    )
                } else {
                    (None, None, None, None, None)
                };
            TypeReportRow {
                column: m.column_name.clone(),
                source_type: m.source_native_type.clone(),
                rivet_type: rivet_type_label(&m.rivet_type),
                arrow_type: m
                    .arrow_type
                    .as_ref()
                    .map(|t| format!("{t:?}"))
                    .unwrap_or_else(|| "-".into()),
                fidelity: m.fidelity,
                warnings: m.warnings.clone(),
                target_type,
                target_status,
                target_note,
                autoload_type,
                cast_sql,
            }
        })
        .collect();

    // L5 recovery SQL (ADR-0014): a post-load transform for operators whose
    // bare autoload would degrade types. `None` for DuckDB (faithful autoload)
    // or when no target is set.
    let recovery_sql =
        target.and_then(|t| t.recovery_sql(&t.resolve_table(&mappings), &export.name));

    Ok(ExportTypeReport {
        export: export.name.clone(),
        columns: rows,
        violations,
        target_failures,
        recovery_sql,
    })
}

/// Print the report as a human-readable table to stdout.
pub fn print_table(report: &ExportTypeReport, target: Option<ExportTarget>) {
    let col_w = col_width(&report.columns, |r| r.column.len());
    let src_w = col_width(&report.columns, |r| r.source_type.len()).max("Source type".len());
    let rv_w = col_width(&report.columns, |r| r.rivet_type.len()).max("Rivet type".len());
    let arr_w = col_width(&report.columns, |r| r.arrow_type.len()).max("Arrow type".len());
    let fid_w = "logical_string".len();

    println!();
    if let Some(tgt) = target {
        println!("Export: {}  [target: {}]", report.export, tgt.label());
    } else {
        println!("Export: {}", report.export);
    }

    if target.is_some() {
        let tgt_w = col_width(&report.columns, |r| {
            r.target_type.as_deref().unwrap_or("-").len()
        })
        .max("Target type".len());
        let sta_w = "Status".len();

        println!(
            "  {:<col_w$}  {:<src_w$}  {:<rv_w$}  {:<arr_w$}  {:<fid_w$}  {:<tgt_w$}  {:<sta_w$}",
            "Column",
            "Source type",
            "Rivet type",
            "Arrow type",
            "Fidelity",
            "Target type",
            "Status"
        );
        println!(
            "  {:-<col_w$}  {:-<src_w$}  {:-<rv_w$}  {:-<arr_w$}  {:-<fid_w$}  {:-<tgt_w$}  {:-<sta_w$}",
            "", "", "", "", "", "", ""
        );
        for row in &report.columns {
            let status_label = row.target_status.as_ref().map(|s| s.label()).unwrap_or("-");
            let tgt_type = row.target_type.as_deref().unwrap_or("-");
            let status_marker = match &row.target_status {
                Some(TargetStatus::Fail) => " ✗",
                Some(TargetStatus::Warn) => " ~",
                _ => "",
            };
            println!(
                "  {:<col_w$}  {:<src_w$}  {:<rv_w$}  {:<arr_w$}  {}{:<rest$}  {:<tgt_w$}  {}{}",
                row.column,
                row.source_type,
                row.rivet_type,
                row.arrow_type,
                row.fidelity.label(),
                "",
                tgt_type,
                status_label,
                status_marker,
                rest = fid_w - row.fidelity.label().len(),
            );
            if let Some(autoload) = &row.autoload_type {
                println!("  {:<col_w$}    autoload: {}", "", autoload);
            }
            if let Some(note) = &row.target_note {
                println!("  {:<col_w$}    note: {}", "", note);
            }
            if let Some(cast) = &row.cast_sql {
                println!("  {:<col_w$}    recover: {}", "", cast);
            }
            for w in &row.warnings {
                println!("  {:<col_w$}    warning: {}", "", w);
            }
        }
    } else {
        println!(
            "  {:<col_w$}  {:<src_w$}  {:<rv_w$}  {:<arr_w$}  {:<fid_w$}",
            "Column", "Source type", "Rivet type", "Arrow type", "Fidelity"
        );
        println!(
            "  {:-<col_w$}  {:-<src_w$}  {:-<rv_w$}  {:-<arr_w$}  {:-<fid_w$}",
            "", "", "", "", ""
        );
        for row in &report.columns {
            println!(
                "  {:<col_w$}  {:<src_w$}  {:<rv_w$}  {:<arr_w$}  {}{}",
                row.column,
                row.source_type,
                row.rivet_type,
                row.arrow_type,
                row.fidelity.label(),
                fidelity_marker(row.fidelity),
            );
            for w in &row.warnings {
                println!("  {:<col_w$}    warning: {}", "", w);
            }
        }
    }

    if !report.violations.is_empty() {
        println!();
        for v in &report.violations {
            let prefix = if v.fatal { "  FAIL" } else { "  WARN" };
            println!("{}: {}", prefix, v.message);
        }
    }

    if let Some(sql) = &report.recovery_sql {
        println!();
        println!(
            "  {} type recovery — bare autoload degrades JSON/UUID→BYTES, naive",
            target.map(|t| t.label()).unwrap_or("target")
        );
        println!("  timestamp→TIMESTAMP, array→RECORD; load with --autodetect then run:");
        for line in sql.lines() {
            println!("    {line}");
        }
    }
}

fn col_width(rows: &[TypeReportRow], f: impl Fn(&TypeReportRow) -> usize) -> usize {
    rows.iter().map(f).max().unwrap_or(8).max(8)
}

fn fidelity_marker(f: TypeFidelity) -> &'static str {
    match f {
        TypeFidelity::Lossy | TypeFidelity::Unsupported => " ✗",
        TypeFidelity::LogicalString => " ~",
        _ => "",
    }
}

/// Detect whether a `columns:` override *narrows* the autodetected source type
/// in a value-losing way. Returns `Some(reason)` to flag the column `Lossy`,
/// `None` when the override preserves or widens the type (or when the comparison
/// is not applicable).
///
/// Today this covers the decimal case the audit exercises (#32): a scale or
/// integer-digit reduction on `numeric`/`decimal`. A scale reduction
/// (`numeric(10,2)` → `decimal(20,0)`) silently truncates the fractional digits
/// at `run`; an integer-digit reduction (`(20,0)` → `(10,0)`) overflows the
/// declared precision. Either is genuinely lossy and must not be reported
/// `exact`. Widening or an equal scale/precision is fine. Non-decimal overrides
/// are left to `derive_fidelity` (not narrowing-classified here).
fn override_narrows(
    source: &crate::types::RivetType,
    overridden: &crate::types::RivetType,
) -> Option<String> {
    use crate::types::RivetType::Decimal;
    if let (
        Decimal {
            precision: sp,
            scale: ss,
        },
        Decimal {
            precision: op,
            scale: os,
        },
    ) = (source, overridden)
    {
        // Fractional-digit loss: the override keeps fewer digits to the right
        // of the point than the source declared.
        if os < ss {
            return Some(format!(
                "override decimal({op},{os}) reduces scale from source numeric({sp},{ss}) — \
                 {} fractional digit(s) are truncated at run; this is lossy, not exact",
                (*ss as i16) - (*os as i16)
            ));
        }
        // Integer-digit loss: the override leaves fewer digits to the left of
        // the point than the source could hold, so large values overflow.
        let src_int_digits = *sp as i16 - *ss as i16;
        let ov_int_digits = *op as i16 - *os as i16;
        if ov_int_digits < src_int_digits {
            return Some(format!(
                "override decimal({op},{os}) reduces integer-digit capacity from source \
                 numeric({sp},{ss}) — large values overflow at run; this is lossy, not exact"
            ));
        }
    }
    None
}

fn rivet_type_label(t: &crate::types::RivetType) -> String {
    use crate::types::RivetType::*;
    match t {
        Bool => "bool".into(),
        Int16 => "int2".into(),
        Int32 => "int4".into(),
        Int64 => "int8".into(),
        UInt64 => "uint8".into(),
        Float32 => "float4".into(),
        Float64 => "float8".into(),
        Decimal { precision, scale } => format!("decimal({precision},{scale})"),
        Date => "date".into(),
        Time { .. } => "time".into(),
        Timestamp {
            timezone: Some(_), ..
        } => "timestamp_tz".into(),
        Timestamp { timezone: None, .. } => "timestamp".into(),
        String => "text".into(),
        Text => "text".into(),
        Binary => "binary".into(),
        Json => "json".into(),
        Uuid => "uuid".into(),
        Enum => "enum".into(),
        Interval => "interval".into(),
        List { inner } => format!("list<{}>", rivet_type_label(inner)),
        Unsupported { native_type, .. } => format!("unsupported({native_type})"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{RivetType, TypeFidelity};

    // ── override_narrows (#32: lossy scale/precision narrowing) ──────────────

    fn dec(precision: u8, scale: i8) -> RivetType {
        RivetType::Decimal { precision, scale }
    }

    #[test]
    fn narrows_flags_scale_reduction_as_lossy() {
        // The audit case: numeric(10,2) overridden to decimal(20,0) drops the
        // two fractional digits — must be flagged, never 'exact'.
        let reason = override_narrows(&dec(10, 2), &dec(20, 0)).expect("scale drop is lossy");
        assert!(
            reason.contains("scale"),
            "reason should name scale: {reason}"
        );
        assert!(
            reason.contains("lossy"),
            "reason should say lossy: {reason}"
        );
    }

    #[test]
    fn narrows_none_when_scale_preserved() {
        // Same scale, wider precision: no fractional loss → not narrowing.
        assert!(override_narrows(&dec(10, 2), &dec(20, 2)).is_none());
        // Identical type: not narrowing.
        assert!(override_narrows(&dec(10, 2), &dec(10, 2)).is_none());
    }

    #[test]
    fn narrows_none_when_scale_widened() {
        // More fractional digits than the source declared preserves every value.
        assert!(override_narrows(&dec(10, 2), &dec(12, 4)).is_none());
    }

    #[test]
    fn narrows_flags_integer_digit_reduction_as_lossy() {
        // Same scale but fewer integer digits: (20,0) → (10,0) overflows large
        // values, so it is lossy even though the scale is unchanged.
        let reason =
            override_narrows(&dec(20, 0), &dec(10, 0)).expect("integer-digit drop is lossy");
        assert!(
            reason.contains("integer-digit") && reason.contains("lossy"),
            "reason: {reason}"
        );
    }

    #[test]
    fn narrows_none_for_non_decimal_overrides() {
        // Non-decimal overrides are classified by derive_fidelity, not here.
        assert!(override_narrows(&RivetType::Int32, &RivetType::Int64).is_none());
        assert!(override_narrows(&RivetType::Int64, &RivetType::String).is_none());
    }

    // ── fidelity_marker ──────────────────────────────────────────────────────

    #[test]
    fn fidelity_marker_lossy_is_cross() {
        assert_eq!(fidelity_marker(TypeFidelity::Lossy), " ✗");
    }

    #[test]
    fn fidelity_marker_unsupported_is_cross() {
        assert_eq!(fidelity_marker(TypeFidelity::Unsupported), " ✗");
    }

    #[test]
    fn fidelity_marker_logical_string_is_tilde() {
        assert_eq!(fidelity_marker(TypeFidelity::LogicalString), " ~");
    }

    #[test]
    fn fidelity_marker_exact_is_empty() {
        assert_eq!(fidelity_marker(TypeFidelity::Exact), "");
    }

    #[test]
    fn fidelity_marker_compatible_is_empty() {
        assert_eq!(fidelity_marker(TypeFidelity::Compatible), "");
    }

    // ── rivet_type_label ─────────────────────────────────────────────────────

    #[test]
    fn label_bool() {
        assert_eq!(rivet_type_label(&RivetType::Bool), "bool");
    }

    #[test]
    fn label_int64() {
        assert_eq!(rivet_type_label(&RivetType::Int64), "int8");
    }

    #[test]
    fn label_float64() {
        assert_eq!(rivet_type_label(&RivetType::Float64), "float8");
    }

    #[test]
    fn label_decimal_with_precision_and_scale() {
        assert_eq!(
            rivet_type_label(&RivetType::Decimal {
                precision: 18,
                scale: 2
            }),
            "decimal(18,2)"
        );
    }

    #[test]
    fn label_text() {
        assert_eq!(rivet_type_label(&RivetType::Text), "text");
    }

    #[test]
    fn label_uuid() {
        assert_eq!(rivet_type_label(&RivetType::Uuid), "uuid");
    }

    #[test]
    fn label_list_of_int64() {
        let t = RivetType::List {
            inner: Box::new(RivetType::Int64),
        };
        assert_eq!(rivet_type_label(&t), "list<int8>");
    }

    #[test]
    fn label_unsupported_native_type() {
        let t = RivetType::Unsupported {
            native_type: "tsvector".into(),
            reason: "not supported".into(),
        };
        assert_eq!(rivet_type_label(&t), "unsupported(tsvector)");
    }

    // ── col_width ────────────────────────────────────────────────────────────

    #[test]
    fn col_width_empty_returns_minimum_8() {
        let rows: Vec<TypeReportRow> = vec![];
        assert_eq!(col_width(&rows, |_r| 0), 8);
    }

    #[test]
    fn col_width_short_values_returns_minimum_8() {
        let row = TypeReportRow {
            column: "a".into(),
            source_type: "b".into(),
            rivet_type: "c".into(),
            arrow_type: "d".into(),
            fidelity: TypeFidelity::Exact,
            warnings: vec![],
            target_type: None,
            target_status: None,
            target_note: None,
            autoload_type: None,
            cast_sql: None,
        };
        assert_eq!(col_width(&[row], |r| r.column.len()), 8);
    }

    #[test]
    fn col_width_long_value_returns_that_length() {
        let row = TypeReportRow {
            column: "a_very_long_column_name".into(),
            source_type: "int8".into(),
            rivet_type: "int8".into(),
            arrow_type: "Int64".into(),
            fidelity: TypeFidelity::Exact,
            warnings: vec![],
            target_type: None,
            target_status: None,
            target_note: None,
            autoload_type: None,
            cast_sql: None,
        };
        let w = col_width(&[row], |r| r.column.len());
        assert_eq!(w, "a_very_long_column_name".len());
    }
}
