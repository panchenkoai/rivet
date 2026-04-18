use super::ExportDiagnostic;
use super::analysis::*;
use super::cursor_expr::incremental_key_expr;
use crate::config::{ExportConfig, ExportMode, SourceType, TlsConfig};
use crate::error::Result;

pub(super) fn check_postgres(
    url: &str,
    tls: Option<&TlsConfig>,
    exports: &[&ExportConfig],
) -> Result<()> {
    let mut client = crate::source::postgres::connect_client(url, tls)?;
    let db_max_connections = fetch_max_connections_pg(&mut client);

    for export in exports {
        let diag = diagnose_pg(&mut client, export, db_max_connections)?;
        super::print_diagnostic(&diag);
    }

    Ok(())
}

/// Diagnose a single export without printing — used by `rivet plan`.
pub(super) fn diagnose_export_pg(
    url: &str,
    tls: Option<&TlsConfig>,
    export: &ExportConfig,
) -> Result<super::ExportDiagnostic> {
    let mut client = crate::source::postgres::connect_client(url, tls)?;
    let db_max_connections = fetch_max_connections_pg(&mut client);
    diagnose_pg(&mut client, export, db_max_connections)
}

fn fetch_max_connections_pg(client: &mut postgres::Client) -> Option<u32> {
    let rows = client
        .query("SELECT current_setting('max_connections')::int", &[])
        .ok()?;
    rows.first()?.get::<_, i32>(0).try_into().ok()
}

fn diagnose_pg(
    client: &mut postgres::Client,
    export: &ExportConfig,
    db_max_connections: Option<u32>,
) -> Result<ExportDiagnostic> {
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

    let range_col = export
        .chunk_column
        .as_deref()
        .or(export.cursor_column.as_deref());

    let effective_query = if let Some(order) = incremental_key_expr(export, SourceType::Postgres) {
        format!(
            "SELECT * FROM ({}) AS _rivet ORDER BY {}",
            base_query, order
        )
    } else {
        base_query.to_string()
    };

    let row_estimate = estimate_rows_pg(client, &effective_query);

    let (range_min, range_max) = if export.mode == ExportMode::Incremental {
        if let Some(expr) = incremental_key_expr(export, SourceType::Postgres) {
            let range_query = format!(
                "SELECT min({expr})::text, max({expr})::text FROM ({base}) AS _rivet",
                expr = expr,
                base = base_query,
            );
            match client.query(&range_query, &[]) {
                Ok(rows) if !rows.is_empty() => {
                    let min_val: Option<String> = rows[0].get(0);
                    let max_val: Option<String> = rows[0].get(1);
                    (min_val, max_val)
                }
                _ => (None, None),
            }
        } else {
            (None, None)
        }
    } else if let Some(col) = range_col {
        get_cursor_range_pg(client, base_query, col)
    } else {
        (None, None)
    };

    let (scan_type, uses_index) = analyze_plan_pg(client, &effective_query);

    let strategy = derive_strategy(export);
    let verdict = compute_verdict(row_estimate, uses_index, export.cursor_column.is_some());
    let recommended_profile = recommend_profile(row_estimate, uses_index, export);
    let recommended_parallel = recommend_parallelism(export, row_estimate, uses_index);
    let warnings = collect_warnings(
        export,
        row_estimate,
        range_min.as_deref(),
        range_max.as_deref(),
        db_max_connections,
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

fn estimate_rows_pg(client: &mut postgres::Client, query: &str) -> Option<i64> {
    let explain = format!("EXPLAIN {}", query);
    let rows = client.query(&explain, &[]).ok()?;
    let lines: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    let plan_text = lines.join("\n");
    parse_pg_row_estimate(&plan_text)
}

pub(crate) fn parse_pg_row_estimate(plan: &str) -> Option<i64> {
    for line in plan.lines() {
        if let Some(idx) = line.find("rows=") {
            let after = &line[idx + 5..];
            let num_str: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(n) = num_str.parse::<i64>() {
                return Some(n);
            }
        }
    }
    None
}

fn get_cursor_range_pg(
    client: &mut postgres::Client,
    base_query: &str,
    cursor_col: &str,
) -> (Option<String>, Option<String>) {
    let range_query = format!(
        "SELECT min({col})::text, max({col})::text FROM ({base}) AS _rivet",
        col = cursor_col,
        base = base_query,
    );
    match client.query(&range_query, &[]) {
        Ok(rows) if !rows.is_empty() => {
            let min_val: Option<String> = rows[0].get(0);
            let max_val: Option<String> = rows[0].get(1);
            (min_val, max_val)
        }
        _ => (None, None),
    }
}

fn analyze_plan_pg(client: &mut postgres::Client, query: &str) -> (Option<String>, bool) {
    let explain = format!("EXPLAIN {}", query);
    match client.query(&explain, &[]) {
        Ok(rows) => {
            let lines: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
            let plan_text = lines.join("\n");
            let uses_index = plan_text.contains("Index Scan")
                || plan_text.contains("Index Only Scan")
                || plan_text.contains("Bitmap Index Scan");
            let scan_type = extract_scan_type(&plan_text);
            (Some(scan_type), uses_index)
        }
        Err(_) => (None, false),
    }
}

pub(crate) fn extract_scan_type(plan: &str) -> String {
    for line in plan.lines() {
        let trimmed = line.trim().trim_start_matches("-> ");
        if trimmed.contains("Scan") || trimmed.contains("scan") {
            return trimmed.trim_start_matches("-> ").to_string();
        }
    }
    plan.lines().next().unwrap_or("unknown").trim().to_string()
}
