use super::ExportDiagnostic;
use super::analysis::*;
use crate::config::{ExportConfig, ExportMode};
use crate::error::Result;

pub(super) fn check_mysql(url: &str, exports: &[&ExportConfig]) -> Result<()> {
    let pool = mysql::Pool::new(mysql::Opts::from_url(url)?)?;
    let mut conn = pool.get_conn()?;

    for export in exports {
        let diag = diagnose_mysql(&mut conn, export)?;
        super::print_diagnostic(&diag);
    }

    Ok(())
}

fn diagnose_mysql(conn: &mut mysql::PooledConn, export: &ExportConfig) -> Result<ExportDiagnostic> {
    use mysql::prelude::Queryable;

    let mode_str = match export.mode {
        ExportMode::Full => "full".to_string(),
        ExportMode::Incremental => format!(
            "incremental (cursor: {})",
            export.cursor_column.as_deref().unwrap_or("?")
        ),
        ExportMode::Chunked => format!(
            "chunked (column: {}, size: {})",
            export.chunk_column.as_deref().unwrap_or("?"),
            export.chunk_size
        ),
        ExportMode::TimeWindow => format!(
            "time_window (column: {}, days: {})",
            export.time_column.as_deref().unwrap_or("?"),
            export.days_window.unwrap_or(0)
        ),
    };

    let base_query = export.query.as_deref().unwrap_or("SELECT 1");
    let effective_query = if let Some(col) = &export.cursor_column {
        format!("SELECT * FROM ({}) AS _rivet ORDER BY {}", base_query, col)
    } else {
        base_query.to_string()
    };

    let row_estimate = {
        let explain_query = format!("EXPLAIN {}", effective_query);
        let rows: Vec<mysql::Row> = conn.query(&explain_query).unwrap_or_default();
        rows.first().and_then(|r| {
            let val: Option<mysql::Value> = r.get("rows");
            match val {
                Some(mysql::Value::Int(v)) => Some(v),
                Some(mysql::Value::UInt(v)) => Some(v as i64),
                Some(mysql::Value::Bytes(ref b)) => {
                    std::str::from_utf8(b).ok().and_then(|s| s.parse().ok())
                }
                _ => None,
            }
        })
    };

    let range_col = export
        .chunk_column
        .as_deref()
        .or(export.cursor_column.as_deref());

    let (range_min, range_max) = if let Some(col) = range_col {
        let range_query = format!(
            "SELECT CAST(min({col}) AS CHAR), CAST(max({col}) AS CHAR) FROM ({base}) AS _rivet",
            col = col,
            base = base_query,
        );
        match conn.query_first::<(Option<String>, Option<String>), _>(&range_query) {
            Ok(Some((min_v, max_v))) => (min_v, max_v),
            _ => (None, None),
        }
    } else {
        (None, None)
    };

    let (scan_type, uses_index) = {
        let explain_query = format!("EXPLAIN {}", effective_query);
        let rows: Vec<mysql::Row> = conn.query(&explain_query).unwrap_or_default();
        if let Some(row) = rows.first() {
            let access_type = mysql_row_get_string(row, "type");
            let key = mysql_row_get_string(row, "key");
            let has_index = matches!(
                access_type.as_deref(),
                Some("ref") | Some("range") | Some("index") | Some("eq_ref") | Some("const")
            );
            let desc = match (&access_type, &key) {
                (Some(t), Some(k)) => format!("{} using {}", t, k),
                (Some(t), None) => t.clone(),
                _ => "unknown".to_string(),
            };
            (Some(desc), has_index)
        } else {
            (None, false)
        }
    };

    let strategy = derive_strategy(export);
    let verdict = compute_verdict(row_estimate, uses_index, export.cursor_column.is_some());
    let recommended_profile = recommend_profile(row_estimate, uses_index, export);
    let recommended_parallel = recommend_parallelism(export, row_estimate, uses_index);
    let warnings = collect_warnings(
        export,
        row_estimate,
        range_min.as_deref(),
        range_max.as_deref(),
    );
    let suggestion = build_suggestion(&verdict, row_estimate, uses_index, export);

    Ok(ExportDiagnostic {
        export_name: export.name.clone(),
        strategy,
        mode: mode_str,
        cursor_column: export.cursor_column.clone(),
        row_estimate,
        cursor_min: range_min,
        cursor_max: range_max,
        scan_type,
        uses_index,
        verdict,
        recommended_profile,
        recommended_parallel,
        warnings,
        suggestion,
    })
}

fn mysql_row_get_string(row: &mysql::Row, col: &str) -> Option<String> {
    let val: Option<mysql::Value> = row.get(col);
    match val {
        Some(mysql::Value::Bytes(b)) => String::from_utf8(b).ok(),
        Some(mysql::Value::Int(v)) => Some(v.to_string()),
        Some(mysql::Value::UInt(v)) => Some(v.to_string()),
        _ => None,
    }
}
