use crate::config::{Config, ExportConfig, ExportMode, SourceType};
use crate::error::Result;

#[derive(Debug)]
pub enum HealthVerdict {
    Efficient,
    Acceptable,
    Degraded,
    Unsafe,
}

impl std::fmt::Display for HealthVerdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Efficient => write!(f, "EFFICIENT"),
            Self::Acceptable => write!(f, "ACCEPTABLE"),
            Self::Degraded => write!(f, "DEGRADED"),
            Self::Unsafe => write!(f, "UNSAFE"),
        }
    }
}

struct ExportDiagnostic {
    export_name: String,
    mode: String,
    #[allow(dead_code)]
    cursor_column: Option<String>,
    row_estimate: Option<i64>,
    cursor_min: Option<String>,
    cursor_max: Option<String>,
    scan_type: Option<String>,
    #[allow(dead_code)]
    uses_index: bool,
    verdict: HealthVerdict,
    suggestion: Option<String>,
}

pub fn check(config_path: &str, export_name: Option<&str>) -> Result<()> {
    let config = Config::load(config_path)?;

    let exports: Vec<&ExportConfig> = if let Some(name) = export_name {
        let e = config
            .exports
            .iter()
            .find(|e| e.name == name)
            .ok_or_else(|| anyhow::anyhow!("export '{}' not found in config", name))?;
        vec![e]
    } else {
        config.exports.iter().collect()
    };

    let url = config.source.resolve_url()?;
    match config.source.source_type {
        SourceType::Postgres => check_postgres(&url, &exports)?,
        SourceType::Mysql => check_mysql(&url, &exports)?,
    }

    Ok(())
}

fn check_postgres(url: &str, exports: &[&ExportConfig]) -> Result<()> {
    let mut client = postgres::Client::connect(url, postgres::NoTls)?;

    for export in exports {
        let diag = diagnose_pg(&mut client, export)?;
        print_diagnostic(&diag);
    }

    Ok(())
}

fn diagnose_pg(client: &mut postgres::Client, export: &ExportConfig) -> Result<ExportDiagnostic> {
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
        format!(
            "SELECT * FROM ({}) AS _rivet ORDER BY {}",
            base_query, col
        )
    } else {
        base_query.to_string()
    };

    let row_estimate = estimate_rows_pg(client, &effective_query);

    let (cursor_min, cursor_max) = if let Some(col) = &export.cursor_column {
        get_cursor_range_pg(client, base_query, col)
    } else {
        (None, None)
    };

    let (scan_type, uses_index) = analyze_plan_pg(client, &effective_query);

    let verdict = compute_verdict(row_estimate, uses_index, export.cursor_column.is_some());
    let suggestion = build_suggestion(&verdict, row_estimate, uses_index, export);

    Ok(ExportDiagnostic {
        export_name: export.name.clone(),
        mode: mode_str,
        cursor_column: export.cursor_column.clone(),
        row_estimate,
        cursor_min,
        cursor_max,
        scan_type,
        uses_index,
        verdict,
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

fn analyze_plan_pg(
    client: &mut postgres::Client,
    query: &str,
) -> (Option<String>, bool) {
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
    plan.lines()
        .next()
        .unwrap_or("unknown")
        .trim()
        .to_string()
}

fn check_mysql(url: &str, exports: &[&ExportConfig]) -> Result<()> {
    let pool = mysql::Pool::new(mysql::Opts::from_url(url)?)?;
    let mut conn = pool.get_conn()?;

    for export in exports {
        let diag = diagnose_mysql(&mut conn, export)?;
        print_diagnostic(&diag);
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
        format!(
            "SELECT * FROM ({}) AS _rivet ORDER BY {}",
            base_query, col
        )
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

    let (cursor_min, cursor_max) = if let Some(col) = &export.cursor_column {
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

    let verdict = compute_verdict(row_estimate, uses_index, export.cursor_column.is_some());
    let suggestion = build_suggestion(&verdict, row_estimate, uses_index, export);

    Ok(ExportDiagnostic {
        export_name: export.name.clone(),
        mode: mode_str,
        cursor_column: export.cursor_column.clone(),
        row_estimate,
        cursor_min,
        cursor_max,
        scan_type,
        uses_index,
        verdict,
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

pub(crate) fn compute_verdict(
    row_estimate: Option<i64>,
    uses_index: bool,
    has_cursor: bool,
) -> HealthVerdict {
    let rows = row_estimate.unwrap_or(0);

    match (uses_index, has_cursor, rows) {
        (true, true, r) if r <= 10_000_000 => HealthVerdict::Efficient,
        (true, true, _) => HealthVerdict::Acceptable,
        (true, false, r) if r <= 10_000_000 => HealthVerdict::Acceptable,
        (false, _, r) if r <= 1_000_000 => HealthVerdict::Degraded,
        (false, true, r) if r <= 50_000_000 => HealthVerdict::Degraded,
        (false, _, _) => HealthVerdict::Unsafe,
        _ => HealthVerdict::Degraded,
    }
}

pub(crate) fn build_suggestion(
    verdict: &HealthVerdict,
    row_estimate: Option<i64>,
    uses_index: bool,
    export: &ExportConfig,
) -> Option<String> {
    let rows = row_estimate.unwrap_or(0);

    match verdict {
        HealthVerdict::Efficient => None,
        HealthVerdict::Acceptable => {
            if rows > 10_000_000 {
                Some(format!(
                    "Large dataset (~{}M rows). Consider using 'safe' tuning profile.",
                    rows / 1_000_000
                ))
            } else {
                None
            }
        }
        HealthVerdict::Degraded => {
            let mut parts = Vec::new();
            if !uses_index {
                parts.push("No index detected on the query plan -- will perform a full table scan.".to_string());
            }
            if export.cursor_column.is_none() {
                parts.push("No cursor column -- consider adding one for incremental mode.".to_string());
            }
            parts.push("Use 'safe' tuning profile to limit database impact.".to_string());
            Some(parts.join(" "))
        }
        HealthVerdict::Unsafe => {
            let mut parts = vec![format!(
                "~{}M row scan without index support.",
                rows / 1_000_000
            )];
            if export.cursor_column.is_none() {
                parts.push("Add an indexed cursor column and use incremental mode.".to_string());
            } else {
                parts.push("Create an index on the cursor column.".to_string());
            }
            parts.push("Use 'safe' tuning profile. Consider extracting during off-peak hours.".to_string());
            Some(parts.join(" "))
        }
    }
}

fn print_diagnostic(diag: &ExportDiagnostic) {
    println!();
    println!("Export: {}", diag.export_name);
    println!("  Mode:         {}", diag.mode);
    if let Some(est) = diag.row_estimate {
        if est >= 1_000_000 {
            println!("  Row estimate: ~{}M", est / 1_000_000);
        } else if est >= 1_000 {
            println!("  Row estimate: ~{}K", est / 1_000);
        } else {
            println!("  Row estimate: ~{}", est);
        }
    }
    if let (Some(min_v), Some(max_v)) = (&diag.cursor_min, &diag.cursor_max) {
        println!("  Cursor range: {} .. {}", min_v, max_v);
    }
    if let Some(scan) = &diag.scan_type {
        println!("  Scan type:    {}", scan);
    }
    println!("  Verdict:      {}", diag.verdict);
    if let Some(suggestion) = &diag.suggestion {
        println!("  Suggestion:   {}", suggestion);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DestinationConfig, DestinationType, ExportConfig, ExportMode, FormatType, TimeColumnType};

    fn make_export(name: &str, mode: ExportMode, cursor: Option<&str>) -> ExportConfig {
        ExportConfig {
            name: name.to_string(),
            query: Some("SELECT * FROM t".to_string()),
            query_file: None,
            mode,
            cursor_column: cursor.map(|s| s.to_string()),
            chunk_column: None,
            chunk_size: 100_000,
            parallel: 1,
            time_column: None,
            time_column_type: TimeColumnType::Timestamp,
            days_window: None,
            format: FormatType::Csv,
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                bucket: None,
                prefix: None,
                path: Some("./out".to_string()),
                region: None,
                endpoint: None,
                credentials_file: None,
                access_key_env: None,
                secret_key_env: None,
                aws_profile: None,
            },
        }
    }

    #[test]
    fn verdict_small_indexed_with_cursor_is_efficient() {
        let v = compute_verdict(Some(500_000), true, true);
        assert!(matches!(v, HealthVerdict::Efficient), "got: {v}");
    }

    #[test]
    fn verdict_large_indexed_with_cursor_is_acceptable() {
        let v = compute_verdict(Some(20_000_000), true, true);
        assert!(matches!(v, HealthVerdict::Acceptable), "got: {v}");
    }

    #[test]
    fn verdict_no_index_no_cursor_is_degraded() {
        let v = compute_verdict(Some(500_000), false, false);
        assert!(matches!(v, HealthVerdict::Degraded), "got: {v}");
    }

    #[test]
    fn verdict_huge_no_index_is_unsafe() {
        let v = compute_verdict(Some(100_000_000), false, false);
        assert!(matches!(v, HealthVerdict::Unsafe), "got: {v}");
    }

    #[test]
    fn parse_pg_row_estimate_from_sort_plan() {
        let plan = "Sort  (cost=12345.67..12456.78 rows=1000455 width=50)\n  ->  Seq Scan on orders  (cost=0.00..8765.43 rows=1000455 width=50)";
        assert_eq!(parse_pg_row_estimate(plan), Some(1_000_455));
    }

    #[test]
    fn parse_pg_row_estimate_from_index_scan() {
        let plan = "Index Scan using idx_updated on orders  (cost=0.42..81676.36 rows=500000 width=50)";
        assert_eq!(parse_pg_row_estimate(plan), Some(500_000));
    }

    #[test]
    fn extract_scan_type_detects_seq_scan() {
        let plan = "Sort  (cost=...)\n  ->  Seq Scan on users  (cost=...)";
        let st = extract_scan_type(plan);
        assert!(st.contains("Seq Scan"), "expected Seq Scan, got: {st}");
    }

    #[test]
    fn extract_scan_type_detects_index_scan() {
        let plan = "Index Scan using users_pkey on users  (cost=0.42..123.45 rows=100 width=50)";
        let st = extract_scan_type(plan);
        assert!(st.contains("Index Scan"), "expected Index Scan, got: {st}");
    }

    #[test]
    fn suggestion_for_efficient_verdict_is_none() {
        let e = make_export("t", ExportMode::Full, None);
        let s = build_suggestion(&HealthVerdict::Efficient, Some(1000), true, &e);
        assert!(s.is_none(), "efficient verdict should produce no suggestion");
    }

    #[test]
    fn suggestion_for_degraded_verdict_recommends_safe_profile() {
        let e = make_export("t", ExportMode::Full, None);
        let s = build_suggestion(&HealthVerdict::Degraded, Some(500_000), false, &e);
        let msg = s.expect("degraded verdict should produce a suggestion");
        assert!(msg.contains("safe"), "suggestion should recommend safe profile, got: {msg}");
    }
}
