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

/// One export's type-report data — or, for a multiplex `tables:` CDC export, ONE
/// TABLE'S (see [`collect_reports`]).
#[derive(Serialize)]
pub struct ExportTypeReport {
    pub export: String,
    /// The source table this report describes. `Some` only for one table of a
    /// multiplex `tables:` stream, where the export is N tables and each gets its
    /// own document; `None` when the export IS the unit (`table:` / `query:`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
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

    /// How this report addresses itself in operator-facing output: the export
    /// name, or `<export>/<table>` for one table of a multiplex stream — the same
    /// addressing `rivet validate` prints for the same layout, so an operator
    /// reading `check` and `validate` side by side sees one naming scheme.
    pub fn display_name(&self) -> String {
        match &self.table {
            Some(t) => format!("{}/{}", self.export, t),
            None => self.export.clone(),
        }
    }
}

/// The `(table, query)` units one export resolves to for type resolution.
///
/// A multiplex `tables:` CDC export drives N tables through ONE change stream,
/// and each lands in its own destination sub-prefix and its own warehouse table
/// — so it needs one resolver document PER TABLE. It also has no `query:` /
/// `table:` at all, so asking it for one is not merely coarse: `resolve_query`
/// bails ("must specify exactly one of 'query', 'query_file', or 'table'"),
/// which is how a multiplex export got NO type report at all (#252).
///
/// The probe query is the same `SELECT * FROM {table}` the CDC capture itself
/// uses for each table's schema (`source::cdc::CdcSchemaResolver::resolve`),
/// through the same identifier guard — config-load gates a `tables:` entry only
/// for FILENAME safety, which is weaker than SQL interpolation needs.
fn report_units(
    export: &ExportConfig,
    config_dir: &std::path::Path,
    params: Option<&std::collections::HashMap<String, String>>,
) -> Result<Vec<(Option<String>, String)>> {
    match export.multiplex_tables() {
        Some(tables) => tables
            .iter()
            .map(|t| {
                crate::source::cdc::validate_table_ident(t)?;
                Ok((Some(t.clone()), format!("SELECT * FROM {t}")))
            })
            .collect(),
        // Resolve the effective query the same way the export pipeline does, so
        // the `table:` shortcut (and `query_file:` / `${var}` params) produce a
        // real query instead of an empty string.
        None => Ok(vec![(None, export.resolve_query(config_dir, params)?)]),
    }
}

/// Collect type mappings for one export from a live connection — **one report
/// per unit**: one for an ordinary export, one PER TABLE for a multiplex
/// `tables:` CDC stream (see [`report_units`]).
///
/// The fan-out lives here rather than at each caller because both callers need
/// it and neither can be trusted to re-derive it: `rivet check --target` renders
/// these documents and `rivet load` PLANS from them, so a caller that saw one
/// document per export would load N tables into one warehouse table (or, before
/// #252, fail outright). One connection serves every table — a 154-table
/// multiplex must not open 154 of them.
pub fn collect_reports(
    config: &Config,
    export: &ExportConfig,
    column_overrides: &ColumnOverrides,
    policy: &TypePolicy,
    target: Option<ExportTarget>,
    config_dir: &std::path::Path,
    params: Option<&std::collections::HashMap<String, String>>,
) -> Result<Vec<ExportTypeReport>> {
    let units = report_units(export, config_dir, params)?;
    let url = config.source.resolve_url()?;
    let tls = config.source.tls.as_ref();

    let mut src: Box<dyn source::Source> = match config.source.source_type {
        SourceType::Postgres => Box::new(source::postgres::PostgresSource::connect_with_tls(
            &url, tls,
        )?),
        SourceType::Mysql => Box::new(source::mysql::MysqlSource::connect_with_tls(&url, tls)?),
        SourceType::Mssql => Box::new(source::mssql::MssqlSource::connect_with_tls(&url, tls)?),
        SourceType::Mongo => Box::new(source::mongo::MongoSource::connect(&url, tls, None)?),
    };

    units
        .into_iter()
        .map(|(table, query)| {
            // A `columns:` override on a multiplex export can be qualified
            // (`orders.amount`) or bare — narrow it to THIS table exactly the way
            // the capture does, so `check`/`load` type a table the same way `run`
            // writes it.
            let overrides = match &table {
                // BARE name, per `overrides_for_table`'s contract ("`table` is the
                // bare table name (no schema part)") and, crucially, per what the
                // CAPTURE passes (`cdc_job.rs`, `plan/build.rs` — both rsplit the
                // schema off). A `tables: [public.orders]` entry keeps its schema
                // here, so passing it whole made a qualified key (`orders.amount`)
                // match nothing: the capture APPLIED the override and this
                // resolver DROPPED it. Since the load stopped shelling out to
                // `rivet check` and reads this report directly, that mismatch
                // types the created warehouse column from the raw catalog while
                // the Parquet already holds the overridden type.
                Some(t) => crate::types::overrides_for_table(
                    column_overrides,
                    t.rsplit('.').next().unwrap_or(t),
                ),
                None => column_overrides.clone(),
            };
            collect_one(
                src.as_mut(),
                export,
                table,
                &query,
                &overrides,
                policy,
                target,
            )
        })
        .collect()
}

/// #32 (column-applicability): downgrade every overridden column whose override
/// *narrows* the autodetected source type to [`TypeFidelity::Lossy`].
///
/// A `columns:` override like `price numeric(10,2)` → `decimal(20,0)` drops the
/// two fractional digits at `run`, but `derive_fidelity` only ever sees the
/// RESOLVED (overridden) type and labels it `exact` — so `check` disagrees with
/// what `run` actually does. Re-probing the source WITHOUT overrides is what
/// makes the narrowing visible; we downgrade only when autodetect resolved the
/// source type confidently, never on a guess, so we don't fabricate a loss we
/// can't prove.
///
/// `reprobe` is a closure rather than the `&mut dyn Source` itself so the
/// EARLY-RETURN guard is testable without a database. That guard is not a
/// micro-optimisation: it is one saved round-trip per report, and since a
/// multiplex `tables:` export produces one report per captured table, a
/// 154-table schema pays it 154 times. Losing it the other way is worse —
/// skipping the block when overrides ARE present turns a lossy override back
/// into a confident `exact`, which is the #32 bug returning. Both directions are
/// pinned by `narrowing_downgrade_reprobes_only_when_overrides_exist`.
fn downgrade_narrowing_overrides(
    mappings: &mut [crate::types::TypeMapping],
    column_overrides: &ColumnOverrides,
    reprobe: impl FnOnce() -> Result<Vec<crate::types::TypeMapping>>,
) -> Result<()> {
    if column_overrides.is_empty() {
        return Ok(());
    }
    let source_mappings = reprobe()?;
    let source_by_name: std::collections::HashMap<&str, &crate::types::RivetType> = source_mappings
        .iter()
        .map(|m| (m.column_name.as_str(), &m.rivet_type))
        .collect();
    for m in mappings {
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
    Ok(())
}

/// One unit's report, against an already-open source connection.
fn collect_one(
    src: &mut dyn source::Source,
    export: &ExportConfig,
    table: Option<String>,
    query: &str,
    column_overrides: &ColumnOverrides,
    policy: &TypePolicy,
    target: Option<ExportTarget>,
) -> Result<ExportTypeReport> {
    let mut mappings = src.type_mappings(query, column_overrides)?;

    downgrade_narrowing_overrides(&mut mappings, column_overrides, || {
        src.type_mappings(query, &ColumnOverrides::new())
    })?;

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
    // The recovery SQL names the table it recovers — for a multiplex unit that is
    // THIS table, not the export (154 tables all pointed at a `cdc` table would be
    // a hint the operator cannot run).
    let sql_table = table.as_deref().unwrap_or(&export.name);
    let recovery_sql = target.and_then(|t| t.recovery_sql(&t.resolve_table(&mappings), sql_table));

    Ok(ExportTypeReport {
        export: export.name.clone(),
        table,
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
        println!(
            "Export: {}  [target: {}]",
            report.display_name(),
            tgt.label()
        );
    } else {
        println!("Export: {}", report.display_name());
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

    // ── downgrade_narrowing_overrides (#32) ──────────────────────────────────

    fn mapping(column: &str, native: &str, t: RivetType) -> crate::types::TypeMapping {
        crate::types::TypeMapping {
            column_name: column.into(),
            source_native_type: native.into(),
            rivet_type: t,
            arrow_type: None,
            // `Exact` on purpose: that is exactly what `derive_fidelity` labels a
            // resolved override, and what makes the #32 gap silent.
            fidelity: TypeFidelity::Exact,
            nullable: true,
            warnings: vec![],
        }
    }

    /// Both directions of the re-probe guard, and the downgrade itself.
    ///
    /// The guard was untestable while it lived inline in `collect_one`, which
    /// needs a live database — the in-diff mutation gate graded `delete !` on it
    /// as MISSED. Deleting the negation is a two-sided bug and both sides are
    /// pinned here: with overrides present the block must RUN (or a lossy
    /// override reports `exact`, the #32 bug back), and with none it must NOT
    /// (or every report pays a second full type probe — 154 of them for a
    /// 154-table multiplex).
    ///
    /// The re-probe is observed through a flag rather than inferred from the
    /// result, because "no overrides" is precisely the case where running the
    /// block changes NOTHING about the output: every column `continue`s. A test
    /// that only compared mappings would be green against the mutant.
    #[test]
    fn narrowing_downgrade_reprobes_only_when_overrides_exist() {
        let source = || {
            vec![
                mapping("price", "numeric(10,2)", dec(10, 2)),
                mapping("id", "int8", RivetType::Int64),
            ]
        };

        // No overrides → the source is never re-probed, and nothing is touched.
        let mut untouched = source();
        let probed = std::cell::Cell::new(false);
        downgrade_narrowing_overrides(&mut untouched, &ColumnOverrides::new(), || {
            probed.set(true);
            Ok(source())
        })
        .unwrap();
        assert!(
            !probed.get(),
            "no `columns:` overrides ⇒ no second type probe — the round-trip is \
             per REPORT, and a multiplex export has one per captured table"
        );
        assert!(
            untouched.iter().all(|m| m.fidelity == TypeFidelity::Exact),
            "nothing to downgrade without an override"
        );

        // A narrowing override → re-probed, and the narrowed column is Lossy with
        // a reason. `id` is overridden too but WIDENS, so it must stay exact —
        // otherwise the test passes against a mutant that downgrades everything.
        let mut overrides = ColumnOverrides::new();
        overrides.insert("price".into(), dec(20, 0));
        overrides.insert("id".into(), RivetType::Int64);
        let mut mapped = vec![
            mapping("price", "numeric(10,2)", dec(20, 0)),
            mapping("id", "int8", RivetType::Int64),
        ];
        let probed = std::cell::Cell::new(false);
        downgrade_narrowing_overrides(&mut mapped, &overrides, || {
            probed.set(true);
            Ok(source())
        })
        .unwrap();
        assert!(
            probed.get(),
            "an override present ⇒ the source IS re-probed"
        );
        assert_eq!(
            mapped[0].fidelity,
            TypeFidelity::Lossy,
            "decimal(20,0) over numeric(10,2) drops two fractional digits at run \
             — reporting it `exact` is the check↔run gap #32 closed"
        );
        assert!(
            mapped[0].warnings.iter().any(|w| w.contains("scale")),
            "the downgrade must say WHY: {:?}",
            mapped[0].warnings
        );
        assert_eq!(
            mapped[1].fidelity,
            TypeFidelity::Exact,
            "a non-narrowing override is not downgraded — the rule is narrowing, \
             not overridden-at-all"
        );
    }

    // ── report_units (#252: the multiplex `tables:` fan-out) ─────────────────

    fn cfg_from(yaml: &str) -> Config {
        Config::from_yaml(yaml).unwrap()
    }

    /// A multiplex `tables:` CDC export must resolve ONE unit per captured table.
    ///
    /// Before #252 it resolved none at all: the export carries no `table:` and no
    /// `query:`, so `resolve_query` bailed ("must specify exactly one of 'query',
    /// 'query_file', or 'table'") and `rivet check --target` fell through to a
    /// diagnostic-only line while `rivet load` failed outright — for a config
    /// shape `rivet init --mode cdc` emits by default.
    ///
    /// The probe query must be the SAME `SELECT * FROM {table}` the capture uses
    /// for that table's schema; a per-table query that differs would type the
    /// table differently from the way `run` writes it.
    /// The THREE narrowing call sites must key on the SAME thing, or `check`/`load`
    /// type a table differently from the way `run` wrote it.
    ///
    /// `overrides_for_table`'s contract says "`table` is the bare table name (no
    /// schema part)", and the two CAPTURE sites honour it by rsplitting the schema
    /// off (`pipeline/cdc_job.rs`, `plan/build.rs`). The resolver added with the
    /// multiplex fan-out passed the raw `tables:` entry instead — so on a
    /// schema-qualified entry a qualified key matched NOTHING: the capture applied
    /// the override, the resolver dropped it. Since the load stopped shelling out
    /// to `rivet check`, that report is now the warehouse DDL, so the created
    /// column took the raw catalog type while the Parquet already held the
    /// overridden one.
    ///
    /// This test pins the SHARED key rather than one site's behaviour: it asserts
    /// the bare and qualified spellings of the same table narrow identically, which
    /// is exactly the property that failed. RED against passing `t` whole.
    #[test]
    fn every_narrowing_site_keys_on_the_bare_table_name() {
        let overrides: crate::types::ColumnOverrides = [
            ("orders.amount".to_string(), crate::types::RivetType::Int64),
            ("note".to_string(), crate::types::RivetType::Int64),
        ]
        .into_iter()
        .collect();

        // What the CAPTURE passes for `tables: [public.orders]`.
        let capture_key = "public.orders"
            .rsplit('.')
            .next()
            .unwrap_or("public.orders");
        let from_capture = crate::types::overrides_for_table(&overrides, capture_key);

        // What the resolver must pass for the SAME unit.
        let unit = "public.orders";
        let from_resolver =
            crate::types::overrides_for_table(&overrides, unit.rsplit('.').next().unwrap_or(unit));

        assert_eq!(
            from_capture.len(),
            from_resolver.len(),
            "the capture and the resolver must narrow a schema-qualified unit the same way"
        );
        assert!(
            from_resolver.contains_key("amount"),
            "the qualified override `orders.amount` must reach a `public.orders` unit — \
             dropping it types the warehouse column from the raw catalog while the \
             Parquet already holds the overridden type; got {from_resolver:?}"
        );
        assert!(
            from_resolver.contains_key("note"),
            "a bare key still applies to every table: {from_resolver:?}"
        );
        // …and the un-narrowed spelling is exactly the bug: nothing matches.
        assert!(
            !crate::types::overrides_for_table(&overrides, unit).contains_key("amount"),
            "guard the guard: passing the QUALIFIED name really does drop the override, \
             so this test is not vacuous"
        );
    }

    #[test]
    fn report_units_yields_one_unit_per_multiplex_table() {
        let config = cfg_from(
            r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: cdc
    tables: [orders, customers, line_items]
    mode: cdc
    format: parquet
    cdc:
      checkpoint: ./cdc.ckpt
    destination:
      type: local
      path: /tmp/x
"#,
        );
        let units = report_units(&config.exports[0], std::path::Path::new("."), None).unwrap();
        assert_eq!(
            units,
            vec![
                (Some("orders".into()), "SELECT * FROM orders".to_string()),
                (Some("customers".into()), "SELECT * FROM customers".into()),
                (Some("line_items".into()), "SELECT * FROM line_items".into()),
            ],
            "each captured table is its own resolver unit, probed the way the \
             capture probes it"
        );
    }

    /// The other side of the same branch: a single-table export is ONE unit whose
    /// `table` is `None` — the export IS the unit, so nothing downstream starts
    /// fanning out an ordinary export into per-table plans.
    #[test]
    fn report_units_leaves_a_single_table_export_as_one_unnamed_unit() {
        let config = cfg_from(
            r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: orders
    table: public.orders
    mode: full
    format: parquet
    destination:
      type: local
      path: /tmp/x
"#,
        );
        let units = report_units(&config.exports[0], std::path::Path::new("."), None).unwrap();
        assert_eq!(
            units,
            vec![(None, "SELECT * FROM public.orders".to_string())],
            "a `table:` export resolves its own query, unnamed — the export is the unit"
        );
    }

    /// `display_name` is what an operator reads in the type-report header, and it
    /// must distinguish the N documents a multiplex export now emits — otherwise
    /// 154 tables print 154 blocks all headed `Export: cdc`. Matches the
    /// `<export>/<table>` addressing `rivet validate` already prints.
    #[test]
    fn display_name_addresses_a_multiplex_unit_by_export_and_table() {
        let base = ExportTypeReport {
            export: "cdc".into(),
            table: None,
            columns: vec![],
            violations: vec![],
            target_failures: false,
            recovery_sql: None,
        };
        assert_eq!(base.display_name(), "cdc");
        let unit = ExportTypeReport {
            table: Some("orders".into()),
            ..base
        };
        assert_eq!(unit.display_name(), "cdc/orders");
    }

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
