//! `rivet check --type-report` — tabular and JSON output.
//!
//! Roadmap §9 ("Type Fidelity Report") and §16 ("BigQuery Compatibility Layer").
//! Renders a `Vec<TypeMapping>` plus any `PolicyViolation`s as either a
//! fixed-width terminal table or newline-delimited JSON.

use serde::Serialize;

use crate::config::{Config, ExportConfig, SourceType};
use crate::error::Result;
use crate::source;
use crate::types::{
    ColumnOverrides, TypeFidelity,
    policy::{PolicyViolation, TypePolicy},
    target::{ExportTarget, TargetCompat, TargetStatus, check_target_compat},
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
) -> Result<ExportTypeReport> {
    let url = config.source.resolve_url()?;
    let tls = config.source.tls.as_ref();
    let query = export
        .query
        .as_deref()
        .or(export.query_file.as_deref())
        .unwrap_or("");

    let mut src: Box<dyn source::Source> = match config.source.source_type {
        SourceType::Postgres => Box::new(source::postgres::PostgresSource::connect_with_tls(
            &url, tls,
        )?),
        SourceType::Mysql => Box::new(source::mysql::MysqlSource::connect_with_tls(&url, tls)?),
    };

    let mappings = src.type_mappings(query, column_overrides)?;
    let violations = policy.validate(&mappings);

    let mut target_failures = false;
    let rows = mappings
        .iter()
        .map(|m| {
            let (target_type, target_status, target_note) = if let Some(tgt) = target {
                let compat: TargetCompat = check_target_compat(m.arrow_type.as_ref(), tgt);
                if compat.status == TargetStatus::Fail {
                    target_failures = true;
                }
                (Some(compat.target_type), Some(compat.status), compat.note)
            } else {
                (None, None, None)
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
            }
        })
        .collect();

    Ok(ExportTypeReport {
        export: export.name.clone(),
        columns: rows,
        violations,
        target_failures,
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
            if let Some(note) = &row.target_note {
                println!("  {:<col_w$}    note: {}", "", note);
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
}

/// Emit newline-delimited JSON (one object per export).
pub fn print_json(report: &ExportTypeReport) -> Result<()> {
    let s = serde_json::to_string(report)?;
    println!("{}", s);
    Ok(())
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
