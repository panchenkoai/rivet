use crate::config::{Config, DestinationType, ExportConfig, ExportMode, SourceType};
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
    strategy: String,
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
    recommended_profile: &'static str,
    recommended_parallel: (u32, &'static str),
    warnings: Vec<String>,
    suggestion: Option<String>,
}

pub fn doctor(config_path: &str) -> Result<()> {
    println!("rivet doctor: verifying auth for config '{}'", config_path);
    println!();

    // 1. Config parsing
    let config = match Config::load(config_path) {
        Ok(c) => {
            println!("[OK]  Config parsed successfully");
            c
        }
        Err(e) => {
            println!("[FAIL] Config error: {}", e);
            return Err(e);
        }
    };

    let mut all_ok = true;

    // 2. Source auth: try connecting
    match check_source_auth(&config) {
        Ok(()) => println!("[OK]  Source auth ({:?})", config.source.source_type),
        Err(e) => {
            all_ok = false;
            let category = categorize_source_error(&e);
            println!("[FAIL] Source {}: {}", category, e);
        }
    }

    // 3. Destination auth: try probe per unique destination
    let mut seen_destinations: Vec<String> = Vec::new();
    for export in &config.exports {
        let dest_key = format!(
            "{:?}:{}:{}",
            export.destination.destination_type,
            export.destination.bucket.as_deref().unwrap_or("-"),
            export.destination.endpoint.as_deref().unwrap_or("-"),
        );
        if seen_destinations.contains(&dest_key) {
            continue;
        }
        seen_destinations.push(dest_key);

        let label = match export.destination.destination_type {
            DestinationType::Local => format!(
                "Local({})",
                export.destination.path.as_deref().unwrap_or(".")
            ),
            DestinationType::S3 => format!(
                "S3({})",
                export.destination.bucket.as_deref().unwrap_or("?")
            ),
            DestinationType::Gcs => format!(
                "GCS({})",
                export.destination.bucket.as_deref().unwrap_or("?")
            ),
            DestinationType::Stdout => {
                log::info!("  Stdout: no auth check needed");
                continue;
            }
        };

        match check_destination_auth(&export.destination) {
            Ok(()) => println!("[OK]  Destination {}", label),
            Err(e) => {
                all_ok = false;
                let category = categorize_dest_error(&e, &export.destination);
                println!("[FAIL] Destination {} -- {}: {}", label, category, e);
            }
        }
    }

    println!();
    if all_ok {
        println!("All checks passed.");
    } else {
        println!("Some checks failed. Fix the issues above before running exports.");
    }

    Ok(())
}

fn check_source_auth(config: &Config) -> Result<()> {
    let url = config.source.resolve_url()?;
    match config.source.source_type {
        SourceType::Postgres => {
            let mut client = postgres::Client::connect(&url, postgres::NoTls)?;
            client.simple_query("SELECT 1")?;
            Ok(())
        }
        SourceType::Mysql => {
            let opts = mysql::Opts::from_url(&url)?;
            let pool = mysql::Pool::new(opts)?;
            let mut conn = pool.get_conn()?;
            use mysql::prelude::Queryable;
            conn.query_drop("SELECT 1")?;
            Ok(())
        }
    }
}

fn check_destination_auth(dest: &crate::config::DestinationConfig) -> Result<()> {
    use crate::destination::create_destination;
    let d = create_destination(dest)?;
    // Probe: try writing an empty health-check file and delete it.
    let probe_key = ".rivet_doctor_probe";
    let tmp = std::env::temp_dir().join(probe_key);
    std::fs::write(&tmp, b"ok")?;
    match d.write(&tmp, probe_key) {
        Ok(()) => {
            log::debug!("doctor: probe write succeeded, cleaning up");
        }
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
    }
    let _ = std::fs::remove_file(&tmp);
    Ok(())
}

fn categorize_source_error(err: &anyhow::Error) -> &'static str {
    let msg = err.to_string().to_lowercase();
    if msg.contains("password") || msg.contains("authentication") || msg.contains("access denied") {
        "auth error"
    } else if msg.contains("connect") || msg.contains("refused") || msg.contains("timed out")
        || msg.contains("could not translate host")
        || msg.contains("name or service not known")
    {
        "connectivity error"
    } else {
        "error"
    }
}

fn categorize_dest_error(err: &anyhow::Error, dest: &crate::config::DestinationConfig) -> &'static str {
    let msg = err.to_string().to_lowercase();
    if msg.contains("credential") || msg.contains("permission denied") || msg.contains("access denied")
        || msg.contains("unauthorized") || msg.contains("forbidden")
        || msg.contains("invalid_grant") || msg.contains("token")
    {
        "auth error"
    } else if msg.contains("not found") || msg.contains("nosuchbucket") || msg.contains("404") {
        match dest.destination_type {
            DestinationType::S3 => "bucket not found",
            DestinationType::Gcs => "bucket not found",
            DestinationType::Local | DestinationType::Stdout => "path not found",
        }
    } else if msg.contains("connect") || msg.contains("refused") || msg.contains("timed out")
        || msg.contains("dns") || msg.contains("endpoint")
    {
        "connectivity error"
    } else {
        "error"
    }
}

/// B1: Human-readable strategy name derived from mode + config.
pub(crate) fn derive_strategy(export: &ExportConfig) -> String {
    match export.mode {
        ExportMode::Full => {
            if export.parallel > 1 {
                format!("full-parallel({})", export.parallel)
            } else {
                "full-scan".to_string()
            }
        }
        ExportMode::Incremental => {
            let col = export.cursor_column.as_deref().unwrap_or("?");
            format!("incremental({})", col)
        }
        ExportMode::Chunked => {
            let col = export.chunk_column.as_deref().unwrap_or("?");
            if export.parallel > 1 {
                format!("chunked-parallel({}, size={}, p={})", col, export.chunk_size, export.parallel)
            } else {
                format!("chunked({}, size={})", col, export.chunk_size)
            }
        }
        ExportMode::TimeWindow => {
            let col = export.time_column.as_deref().unwrap_or("?");
            let days = export.days_window.unwrap_or(0);
            format!("time-window({}, {}d)", col, days)
        }
    }
}

/// B2: Recommend tuning profile based on row estimate and index usage.
/// No-index (seq scan) never recommends "fast" — at minimum "balanced",
/// because seq scans are inherently degraded and should not run aggressively.
pub(crate) fn recommend_profile(
    row_estimate: Option<i64>,
    uses_index: bool,
    export: &ExportConfig,
) -> &'static str {
    let rows = row_estimate.unwrap_or(0);
    match (uses_index, rows) {
        (true, r) if r <= 1_000_000 => "fast",
        (true, r) if r <= 10_000_000 => "balanced",
        (true, _) => "safe",
        (false, r) if r <= 100_000 => {
            if export.parallel > 1 { "safe" } else { "balanced" }
        }
        (false, r) if r <= 1_000_000 => "balanced",
        (false, _) => "safe",
    }
}

/// Key-range sparsity for chunked `BETWEEN` windows (aligned with `rivet check` B3).
#[derive(Debug, Clone, Copy)]
pub(crate) struct ChunkSparsityInfo {
    pub is_sparse: bool,
    pub density: f64,
    /// `max(chunk_column) - min(chunk_column)`, at least 1.
    pub range_span: i64,
    /// Ceil of `range_span / chunk_size` — expected number of ID windows.
    pub logical_windows: i64,
    pub row_count: i64,
}

/// `row_count` should be the number of rows in the export query (e.g. from `COUNT(*)`).
pub(crate) fn chunk_sparsity_from_counts(
    row_count: i64,
    min_i: i64,
    max_i: i64,
    chunk_size: usize,
) -> ChunkSparsityInfo {
    let range_span = (max_i - min_i).max(1);
    let density = row_count as f64 / range_span as f64;
    let logical_windows = if chunk_size == 0 {
        0
    } else {
        (range_span + chunk_size as i64 - 1) / chunk_size as i64
    };
    let is_sparse = row_count > 0 && density < 0.1 && logical_windows > 10;
    ChunkSparsityInfo {
        is_sparse,
        density,
        range_span,
        logical_windows,
        row_count,
    }
}

/// B3: Detect sparse range risk for chunked mode.
pub(crate) fn check_sparse_range(
    export: &ExportConfig,
    row_estimate: Option<i64>,
    cursor_min: Option<&str>,
    cursor_max: Option<&str>,
) -> Option<String> {
    if export.mode != ExportMode::Chunked {
        return None;
    }
    if export.chunk_dense {
        return None;
    }
    let rows = row_estimate.unwrap_or(0);
    if rows == 0 {
        return None;
    }

    let (min_val, max_val) = match (cursor_min, cursor_max) {
        (Some(a), Some(b)) => (a, b),
        _ => return None,
    };

    let min_i: i64 = min_val.parse().ok()?;
    let max_i: i64 = max_val.parse().ok()?;
    let info = chunk_sparsity_from_counts(rows, min_i, max_i, export.chunk_size);
    if !info.is_sparse {
        return None;
    }

    let empty_pct = ((1.0 - info.density).min(1.0).max(0.0) * 100.0) as u32;
    Some(format!(
        "Sparse key range: ~{}% of chunk windows will be empty (range {}..{}, ~{} rows). \
         Consider chunking on a dense surrogate (ROW_NUMBER) or switching to incremental mode.",
        empty_pct, min_val, max_val, rows
    ))
}

/// B4: Warn about dense surrogate sort cost when query uses ROW_NUMBER.
pub(crate) fn check_dense_surrogate_cost(export: &ExportConfig) -> Option<String> {
    let query = export.query.as_deref().unwrap_or("");
    let q_upper = query.to_uppercase();
    if export.mode == ExportMode::Chunked
        && (q_upper.contains("ROW_NUMBER") || export.chunk_dense)
    {
        Some(
            "Dense surrogate (ROW_NUMBER) requires a global sort -- this adds CPU and I/O cost \
             proportional to the full result set. For very large or hot tables, consider \
             incremental mode on an indexed cursor column, or a precomputed dense key."
                .to_string(),
        )
    } else {
        None
    }
}

/// B5: Warn about parallel memory risk.
pub(crate) fn check_parallel_memory_risk(
    export: &ExportConfig,
    row_estimate: Option<i64>,
) -> Option<String> {
    if export.parallel <= 1 {
        return None;
    }
    let rows = row_estimate.unwrap_or(0);
    if rows > 5_000_000 {
        Some(format!(
            "Parallel={} on ~{}M rows: each worker buffers batch_size rows in memory. \
             With wide rows this can cause high RSS. Monitor with memory_threshold_mb \
             or reduce parallel/batch_size.",
            export.parallel,
            rows / 1_000_000,
        ))
    } else {
        None
    }
}

/// L6: Recommend parallelism level based on mode, row estimate, index usage, and profile.
pub(crate) fn recommend_parallelism(
    export: &ExportConfig,
    row_estimate: Option<i64>,
    uses_index: bool,
) -> (u32, &'static str) {
    if export.mode != ExportMode::Chunked {
        return (1, "only chunked mode benefits from parallelism");
    }

    let rows = row_estimate.unwrap_or(0);

    if rows < 50_000 {
        return (1, "dataset too small to benefit from parallelism");
    }

    if !uses_index && rows > 5_000_000 {
        return (1, "no index — parallel scans would multiply source load");
    }

    if !uses_index {
        return (2, "no index — conservative parallelism to limit source impact");
    }

    match rows {
        r if r < 500_000 => (2, "moderate dataset — 2 workers sufficient"),
        r if r < 5_000_000 => (4, "large dataset with index support"),
        _ => (4, "very large dataset — cap at 4 to control memory; increase with memory_threshold_mb monitoring"),
    }
}

/// Collect all warnings (B3-B5) for an export.
fn collect_warnings(
    export: &ExportConfig,
    row_estimate: Option<i64>,
    chunk_min: Option<&str>,
    chunk_max: Option<&str>,
) -> Vec<String> {
    let mut warnings = Vec::new();
    if let Some(w) = check_sparse_range(export, row_estimate, chunk_min, chunk_max) {
        warnings.push(w);
    }
    if let Some(w) = check_dense_surrogate_cost(export) {
        warnings.push(w);
    }
    if let Some(w) = check_parallel_memory_risk(export, row_estimate) {
        warnings.push(w);
    }
    warnings
}

pub fn check(config_path: &str, export_name: Option<&str>, params: Option<&std::collections::HashMap<String, String>>) -> Result<()> {
    let config = Config::load_with_params(config_path, params)?;

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

    let range_col = export.chunk_column.as_deref()
        .or(export.cursor_column.as_deref());

    let effective_query = if let Some(col) = &export.cursor_column {
        format!(
            "SELECT * FROM ({}) AS _rivet ORDER BY {}",
            base_query, col
        )
    } else {
        base_query.to_string()
    };

    let row_estimate = estimate_rows_pg(client, &effective_query);

    let (range_min, range_max) = if let Some(col) = range_col {
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
        export, row_estimate, range_min.as_deref(), range_max.as_deref(),
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

    let range_col = export.chunk_column.as_deref()
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
        export, row_estimate, range_min.as_deref(), range_max.as_deref(),
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
                let mut msg = format!(
                    "Large dataset (~{}M rows).",
                    rows / 1_000_000
                );
                match export.mode {
                    ExportMode::Full => {
                        msg.push_str(" Switch to incremental mode with an indexed cursor column to avoid re-reading unchanged rows.");
                    }
                    ExportMode::Chunked if export.parallel <= 1 => {
                        msg.push_str(" Add parallel > 1 to speed up chunked extraction.");
                    }
                    _ => {
                        msg.push_str(" Use 'safe' tuning profile to limit database impact.");
                    }
                }
                Some(msg)
            } else {
                None
            }
        }
        HealthVerdict::Degraded => {
            let mut parts = Vec::new();
            if !uses_index {
                parts.push("No index detected -- full table scan.".to_string());
            }
            match export.mode {
                ExportMode::Full if export.cursor_column.is_none() => {
                    parts.push("Add an indexed cursor column and switch to incremental mode.".to_string());
                }
                ExportMode::Chunked => {
                    let col = export.chunk_column.as_deref().unwrap_or("chunk_column");
                    parts.push(format!("Create an index on '{}' to speed up range scans.", col));
                }
                ExportMode::TimeWindow => {
                    let col = export.time_column.as_deref().unwrap_or("time_column");
                    parts.push(format!("Create an index on '{}' for efficient time-window filtering.", col));
                }
                _ => {
                    if export.cursor_column.is_none() {
                        parts.push("Consider adding a cursor column for incremental mode.".to_string());
                    }
                }
            }
            parts.push("Use 'safe' tuning profile to limit database impact.".to_string());
            Some(parts.join(" "))
        }
        HealthVerdict::Unsafe => {
            let mut parts = vec![format!(
                "~{}M row scan without index support.",
                rows / 1_000_000
            )];
            match export.mode {
                ExportMode::Full => {
                    parts.push("Add an indexed cursor column and use incremental mode to avoid full re-reads.".to_string());
                }
                ExportMode::Chunked => {
                    let col = export.chunk_column.as_deref().unwrap_or("chunk_column");
                    parts.push(format!(
                        "Create an index on '{}'. Consider reducing chunk_size or adding parallel workers.",
                        col
                    ));
                }
                ExportMode::TimeWindow => {
                    let col = export.time_column.as_deref().unwrap_or("time_column");
                    parts.push(format!(
                        "Create an index on '{}'. Reduce days_window if possible.",
                        col
                    ));
                }
                ExportMode::Incremental => {
                    let col = export.cursor_column.as_deref().unwrap_or("cursor_column");
                    parts.push(format!("Create an index on '{}'.", col));
                }
            }
            parts.push("Use 'safe' tuning profile. Extract during off-peak hours.".to_string());
            Some(parts.join(" "))
        }
    }
}

fn print_diagnostic(diag: &ExportDiagnostic) {
    println!();
    println!("Export: {}", diag.export_name);
    println!("  Strategy:     {}", diag.strategy);
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
    println!("  Recommended:  tuning.profile: {}", diag.recommended_profile);
    let (par_level, par_reason) = diag.recommended_parallel;
    if par_level > 1 {
        println!("  Recommended:  parallel: {} ({})", par_level, par_reason);
    } else {
        println!("  Parallelism:  {} ({})", par_level, par_reason);
    }
    for w in &diag.warnings {
        println!("  Warning:      {}", w);
    }
    if let Some(suggestion) = &diag.suggestion {
        println!("  Suggestion:   {}", suggestion);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CompressionType, DestinationConfig, DestinationType, ExportConfig, ExportMode, FormatType, MetaColumns, TimeColumnType};

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
            compression: CompressionType::default(),
            compression_level: None,
            skip_empty: false,
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
                allow_anonymous: false,
            },
            meta_columns: MetaColumns::default(),
            quality: None,
            max_file_size: None,
            chunk_checkpoint: false,
            chunk_max_attempts: None,
            tuning: None,
            chunk_dense: false,
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

    // --- doctor: categorize_source_error ---

    fn src_err(msg: &str) -> &'static str {
        categorize_source_error(&anyhow::anyhow!("{}", msg))
    }

    #[test]
    fn source_password_rejected_is_auth_error() {
        assert_eq!(src_err("password authentication failed for user \"rivet\""), "auth error");
    }

    #[test]
    fn source_authentication_failed_is_auth_error() {
        assert_eq!(src_err("FATAL: authentication failed"), "auth error");
    }

    #[test]
    fn source_access_denied_is_auth_error() {
        assert_eq!(src_err("Access denied for user 'rivet'@'localhost'"), "auth error");
    }

    #[test]
    fn source_connection_refused_is_connectivity() {
        assert_eq!(src_err("connection refused (os error 61)"), "connectivity error");
    }

    #[test]
    fn source_timed_out_is_connectivity() {
        assert_eq!(src_err("connection timed out"), "connectivity error");
    }

    #[test]
    fn source_dns_translate_host_is_connectivity() {
        assert_eq!(src_err("could not translate host name \"db.bad\" to address"), "connectivity error");
    }

    #[test]
    fn source_name_not_known_is_connectivity() {
        assert_eq!(src_err("Name or service not known"), "connectivity error");
    }

    #[test]
    fn source_unknown_error_is_generic() {
        assert_eq!(src_err("something totally unexpected"), "error");
    }

    // --- doctor: categorize_dest_error ---

    fn dest_config(dtype: DestinationType) -> DestinationConfig {
        DestinationConfig {
            destination_type: dtype,
            bucket: Some("b".to_string()),
            prefix: None,
            path: None,
            region: None,
            endpoint: None,
            credentials_file: None,
            access_key_env: None,
            secret_key_env: None,
            aws_profile: None,
            allow_anonymous: false,
        }
    }

    fn dest_err(msg: &str, dtype: DestinationType) -> &'static str {
        let cfg = dest_config(dtype);
        categorize_dest_error(&anyhow::anyhow!("{}", msg), &cfg)
    }

    #[test]
    fn dest_credential_loading_is_auth_error() {
        assert_eq!(dest_err("loading credential to sign http request", DestinationType::Gcs), "auth error");
    }

    #[test]
    fn dest_permission_denied_is_auth_error() {
        assert_eq!(dest_err("permission denied on resource bucket", DestinationType::S3), "auth error");
    }

    #[test]
    fn dest_forbidden_is_auth_error() {
        assert_eq!(dest_err("403 Forbidden", DestinationType::Gcs), "auth error");
    }

    #[test]
    fn dest_unauthorized_is_auth_error() {
        assert_eq!(dest_err("401 Unauthorized", DestinationType::S3), "auth error");
    }

    #[test]
    fn dest_invalid_grant_is_auth_error() {
        assert_eq!(dest_err("invalid_grant: token has been revoked", DestinationType::Gcs), "auth error");
    }

    #[test]
    fn dest_nosuchbucket_s3_is_bucket_not_found() {
        assert_eq!(dest_err("NoSuchBucket: the specified bucket does not exist", DestinationType::S3), "bucket not found");
    }

    #[test]
    fn dest_not_found_gcs_is_bucket_not_found() {
        assert_eq!(dest_err("bucket not found (404)", DestinationType::Gcs), "bucket not found");
    }

    #[test]
    fn dest_not_found_local_is_path_not_found() {
        assert_eq!(dest_err("path not found: /tmp/missing", DestinationType::Local), "path not found");
    }

    #[test]
    fn dest_connection_refused_is_connectivity() {
        assert_eq!(dest_err("connection refused to endpoint", DestinationType::S3), "connectivity error");
    }

    #[test]
    fn dest_dns_error_is_connectivity() {
        assert_eq!(dest_err("dns error: failed to lookup address", DestinationType::S3), "connectivity error");
    }

    #[test]
    fn dest_timed_out_is_connectivity() {
        assert_eq!(dest_err("request timed out after 30s", DestinationType::Gcs), "connectivity error");
    }

    #[test]
    fn dest_unknown_error_is_generic() {
        assert_eq!(dest_err("something else entirely", DestinationType::S3), "error");
    }

    // --- B1: derive_strategy ---

    #[test]
    fn strategy_full_scan() {
        let e = make_export("t", ExportMode::Full, None);
        assert_eq!(derive_strategy(&e), "full-scan");
    }

    #[test]
    fn strategy_full_parallel() {
        let mut e = make_export("t", ExportMode::Full, None);
        e.parallel = 4;
        assert_eq!(derive_strategy(&e), "full-parallel(4)");
    }

    #[test]
    fn strategy_incremental() {
        let e = make_export("t", ExportMode::Incremental, Some("updated_at"));
        assert_eq!(derive_strategy(&e), "incremental(updated_at)");
    }

    #[test]
    fn strategy_chunked() {
        let mut e = make_export("t", ExportMode::Chunked, None);
        e.chunk_column = Some("id".to_string());
        e.chunk_size = 50_000;
        assert_eq!(derive_strategy(&e), "chunked(id, size=50000)");
    }

    #[test]
    fn strategy_chunked_parallel() {
        let mut e = make_export("t", ExportMode::Chunked, None);
        e.chunk_column = Some("id".to_string());
        e.chunk_size = 50_000;
        e.parallel = 3;
        assert_eq!(derive_strategy(&e), "chunked-parallel(id, size=50000, p=3)");
    }

    #[test]
    fn strategy_time_window() {
        let mut e = make_export("t", ExportMode::TimeWindow, None);
        e.time_column = Some("created_at".to_string());
        e.days_window = Some(7);
        assert_eq!(derive_strategy(&e), "time-window(created_at, 7d)");
    }

    // --- B2: recommend_profile ---

    #[test]
    fn profile_small_indexed_is_fast() {
        let e = make_export("t", ExportMode::Full, None);
        assert_eq!(recommend_profile(Some(500_000), true, &e), "fast");
    }

    #[test]
    fn profile_medium_indexed_is_balanced() {
        let e = make_export("t", ExportMode::Full, None);
        assert_eq!(recommend_profile(Some(5_000_000), true, &e), "balanced");
    }

    #[test]
    fn profile_large_indexed_is_safe() {
        let e = make_export("t", ExportMode::Full, None);
        assert_eq!(recommend_profile(Some(50_000_000), true, &e), "safe");
    }

    #[test]
    fn profile_small_no_index_is_balanced() {
        let e = make_export("t", ExportMode::Full, None);
        assert_eq!(recommend_profile(Some(50_000), false, &e), "balanced");
    }

    #[test]
    fn profile_small_no_index_parallel_is_safe() {
        let mut e = make_export("t", ExportMode::Full, None);
        e.parallel = 4;
        assert_eq!(recommend_profile(Some(50_000), false, &e), "safe");
    }

    #[test]
    fn profile_medium_no_index_is_balanced() {
        let e = make_export("t", ExportMode::Full, None);
        assert_eq!(recommend_profile(Some(500_000), false, &e), "balanced");
    }

    #[test]
    fn profile_large_no_index_is_safe() {
        let e = make_export("t", ExportMode::Full, None);
        assert_eq!(recommend_profile(Some(5_000_000), false, &e), "safe");
    }

    // --- B3: check_sparse_range ---

    #[test]
    fn sparse_range_warning_when_very_sparse() {
        let mut e = make_export("t", ExportMode::Chunked, None);
        e.chunk_column = Some("id".to_string());
        e.chunk_size = 100_000;
        // range 1..10_000_000, only 100k rows -> density 0.01
        let w = check_sparse_range(&e, Some(100_000), Some("1"), Some("10000000"));
        assert!(w.is_some(), "should warn about sparse range");
        let msg = w.unwrap();
        assert!(msg.contains("Sparse key range"), "got: {msg}");
        assert!(msg.contains("empty"), "got: {msg}");
    }

    #[test]
    fn sparse_range_no_warning_when_dense() {
        let mut e = make_export("t", ExportMode::Chunked, None);
        e.chunk_column = Some("id".to_string());
        e.chunk_size = 100_000;
        // range 1..100_000, 100k rows -> density 1.0
        let w = check_sparse_range(&e, Some(100_000), Some("1"), Some("100000"));
        assert!(w.is_none(), "should not warn for dense range");
    }

    #[test]
    fn sparse_range_skipped_when_chunk_dense() {
        let mut e = make_export("t", ExportMode::Chunked, None);
        e.chunk_column = Some("id".to_string());
        e.chunk_dense = true;
        e.chunk_size = 100_000;
        let w = check_sparse_range(&e, Some(100_000), Some("1"), Some("10000000"));
        assert!(w.is_none(), "chunk_dense uses ordinals, not physical id span");
    }

    #[test]
    fn dense_surrogate_warning_when_chunk_dense_builtin() {
        let mut e = make_export("t", ExportMode::Chunked, None);
        e.chunk_column = Some("id".to_string());
        e.chunk_dense = true;
        e.query = Some("SELECT id FROM orders".to_string());
        let w = check_dense_surrogate_cost(&e);
        assert!(w.is_some(), "should warn about built-in ROW_NUMBER cost");
        assert!(w.unwrap().contains("global sort"));
    }

    #[test]
    fn sparse_range_not_triggered_for_non_chunked() {
        let e = make_export("t", ExportMode::Full, None);
        let w = check_sparse_range(&e, Some(100), Some("1"), Some("1000000"));
        assert!(w.is_none(), "should not warn for non-chunked mode");
    }

    // --- B4: check_dense_surrogate_cost ---

    #[test]
    fn dense_surrogate_warning_with_row_number() {
        let mut e = make_export("t", ExportMode::Chunked, None);
        e.chunk_column = Some("rn".to_string());
        e.query = Some("SELECT *, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM orders".to_string());
        let w = check_dense_surrogate_cost(&e);
        assert!(w.is_some(), "should warn about ROW_NUMBER cost");
        assert!(w.unwrap().contains("global sort"));
    }

    #[test]
    fn no_dense_surrogate_warning_without_row_number() {
        let mut e = make_export("t", ExportMode::Chunked, None);
        e.chunk_column = Some("id".to_string());
        e.query = Some("SELECT * FROM orders".to_string());
        let w = check_dense_surrogate_cost(&e);
        assert!(w.is_none());
    }

    #[test]
    fn no_dense_surrogate_warning_for_non_chunked() {
        let mut e = make_export("t", ExportMode::Full, None);
        e.query = Some("SELECT ROW_NUMBER() OVER () AS rn FROM t".to_string());
        let w = check_dense_surrogate_cost(&e);
        assert!(w.is_none(), "should not warn for non-chunked mode");
    }

    // --- B5: check_parallel_memory_risk ---

    #[test]
    fn parallel_memory_warning_large_dataset() {
        let mut e = make_export("t", ExportMode::Chunked, None);
        e.parallel = 4;
        let w = check_parallel_memory_risk(&e, Some(10_000_000));
        assert!(w.is_some(), "should warn about memory risk");
        let msg = w.unwrap();
        assert!(msg.contains("Parallel=4"), "got: {msg}");
        assert!(msg.contains("memory"), "got: {msg}");
    }

    #[test]
    fn no_parallel_memory_warning_small_dataset() {
        let mut e = make_export("t", ExportMode::Chunked, None);
        e.parallel = 4;
        let w = check_parallel_memory_risk(&e, Some(1_000));
        assert!(w.is_none(), "should not warn for small dataset");
    }

    #[test]
    fn no_parallel_memory_warning_single_worker() {
        let e = make_export("t", ExportMode::Full, None);
        let w = check_parallel_memory_risk(&e, Some(100_000_000));
        assert!(w.is_none(), "should not warn when parallel=1");
    }

    // --- B6: mode-aware suggestions ---

    #[test]
    fn suggestion_degraded_full_recommends_incremental() {
        let e = make_export("t", ExportMode::Full, None);
        let s = build_suggestion(&HealthVerdict::Degraded, Some(500_000), false, &e).unwrap();
        assert!(s.contains("incremental"), "got: {s}");
    }

    #[test]
    fn suggestion_degraded_chunked_recommends_index() {
        let mut e = make_export("t", ExportMode::Chunked, None);
        e.chunk_column = Some("id".to_string());
        let s = build_suggestion(&HealthVerdict::Degraded, Some(500_000), false, &e).unwrap();
        assert!(s.contains("index on 'id'"), "got: {s}");
    }

    #[test]
    fn suggestion_degraded_time_window_recommends_index() {
        let mut e = make_export("t", ExportMode::TimeWindow, None);
        e.time_column = Some("created_at".to_string());
        e.days_window = Some(7);
        let s = build_suggestion(&HealthVerdict::Degraded, Some(500_000), false, &e).unwrap();
        assert!(s.contains("index on 'created_at'"), "got: {s}");
    }

    #[test]
    fn suggestion_unsafe_full_recommends_incremental() {
        let e = make_export("t", ExportMode::Full, None);
        let s = build_suggestion(&HealthVerdict::Unsafe, Some(100_000_000), false, &e).unwrap();
        assert!(s.contains("incremental"), "got: {s}");
    }

    #[test]
    fn suggestion_unsafe_chunked_recommends_index_and_parallel() {
        let mut e = make_export("t", ExportMode::Chunked, None);
        e.chunk_column = Some("id".to_string());
        let s = build_suggestion(&HealthVerdict::Unsafe, Some(100_000_000), false, &e).unwrap();
        assert!(s.contains("index on 'id'"), "got: {s}");
        assert!(s.contains("parallel"), "got: {s}");
    }

    #[test]
    fn suggestion_unsafe_incremental_recommends_index_on_cursor() {
        let e = make_export("t", ExportMode::Incremental, Some("updated_at"));
        let s = build_suggestion(&HealthVerdict::Unsafe, Some(100_000_000), false, &e).unwrap();
        assert!(s.contains("index on 'updated_at'"), "got: {s}");
    }

    #[test]
    fn suggestion_acceptable_large_full_recommends_incremental() {
        let e = make_export("t", ExportMode::Full, None);
        let s = build_suggestion(&HealthVerdict::Acceptable, Some(20_000_000), true, &e).unwrap();
        assert!(s.contains("incremental"), "got: {s}");
    }

    // --- L6: recommend_parallelism ---

    #[test]
    fn parallel_only_for_chunked_mode() {
        let e = make_export("t", ExportMode::Full, None);
        let (level, _) = recommend_parallelism(&e, Some(1_000_000), true);
        assert_eq!(level, 1, "non-chunked mode should recommend 1");
    }

    #[test]
    fn parallel_small_dataset_is_one() {
        let mut e = make_export("t", ExportMode::Chunked, None);
        e.chunk_column = Some("id".to_string());
        let (level, _) = recommend_parallelism(&e, Some(10_000), true);
        assert_eq!(level, 1, "small dataset should recommend 1");
    }

    #[test]
    fn parallel_moderate_indexed_is_two() {
        let mut e = make_export("t", ExportMode::Chunked, None);
        e.chunk_column = Some("id".to_string());
        let (level, _) = recommend_parallelism(&e, Some(200_000), true);
        assert_eq!(level, 2, "moderate indexed dataset should recommend 2");
    }

    #[test]
    fn parallel_large_indexed_is_four() {
        let mut e = make_export("t", ExportMode::Chunked, None);
        e.chunk_column = Some("id".to_string());
        let (level, _) = recommend_parallelism(&e, Some(2_000_000), true);
        assert_eq!(level, 4, "large indexed dataset should recommend 4");
    }

    #[test]
    fn parallel_no_index_large_is_one() {
        let mut e = make_export("t", ExportMode::Chunked, None);
        e.chunk_column = Some("id".to_string());
        let (level, reason) = recommend_parallelism(&e, Some(10_000_000), false);
        assert_eq!(level, 1, "no index + large should recommend 1");
        assert!(reason.contains("no index"), "got: {reason}");
    }

    #[test]
    fn parallel_no_index_moderate_is_conservative() {
        let mut e = make_export("t", ExportMode::Chunked, None);
        e.chunk_column = Some("id".to_string());
        let (level, _) = recommend_parallelism(&e, Some(200_000), false);
        assert_eq!(level, 2, "no index + moderate should recommend 2 (conservative)");
    }

    #[test]
    fn suggestion_acceptable_large_chunked_recommends_parallel() {
        let mut e = make_export("t", ExportMode::Chunked, None);
        e.chunk_column = Some("id".to_string());
        let s = build_suggestion(&HealthVerdict::Acceptable, Some(20_000_000), true, &e).unwrap();
        assert!(s.contains("parallel"), "got: {s}");
    }
}
