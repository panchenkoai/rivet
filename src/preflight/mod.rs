mod analysis;
mod doctor;
mod mysql;
mod postgres;

pub(crate) use analysis::chunk_sparsity_from_counts;
#[cfg(test)]
use analysis::{
    build_suggestion, check_dense_surrogate_cost, check_parallel_memory_risk, check_sparse_range,
    compute_verdict, derive_strategy, recommend_parallelism, recommend_profile,
};
pub use doctor::doctor;
#[cfg(test)]
use postgres::{extract_scan_type, parse_pg_row_estimate};

use crate::config::{Config, ExportConfig, SourceType};
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

pub(crate) struct ExportDiagnostic {
    pub export_name: String,
    pub strategy: String,
    pub mode: String,
    #[allow(dead_code)]
    pub cursor_column: Option<String>,
    pub row_estimate: Option<i64>,
    pub cursor_min: Option<String>,
    pub cursor_max: Option<String>,
    pub scan_type: Option<String>,
    #[allow(dead_code)]
    pub uses_index: bool,
    pub verdict: HealthVerdict,
    pub recommended_profile: &'static str,
    pub recommended_parallel: (u32, &'static str),
    pub warnings: Vec<String>,
    pub suggestion: Option<String>,
}

pub fn check(
    config_path: &str,
    export_name: Option<&str>,
    params: Option<&std::collections::HashMap<String, String>>,
) -> Result<()> {
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
        SourceType::Postgres => postgres::check_postgres(&url, &exports)?,
        SourceType::Mysql => mysql::check_mysql(&url, &exports)?,
    }

    Ok(())
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
    println!(
        "  Recommended:  tuning.profile: {}",
        diag.recommended_profile
    );
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
    use crate::config::{
        CompressionType, DestinationConfig, DestinationType, ExportConfig, ExportMode, FormatType,
        MetaColumns, TimeColumnType,
    };
    use doctor::{categorize_dest_error, categorize_source_error};

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
        let plan =
            "Index Scan using idx_updated on orders  (cost=0.42..81676.36 rows=500000 width=50)";
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
        assert!(
            s.is_none(),
            "efficient verdict should produce no suggestion"
        );
    }

    #[test]
    fn suggestion_for_degraded_verdict_recommends_safe_profile() {
        let e = make_export("t", ExportMode::Full, None);
        let s = build_suggestion(&HealthVerdict::Degraded, Some(500_000), false, &e);
        let msg = s.expect("degraded verdict should produce a suggestion");
        assert!(
            msg.contains("safe"),
            "suggestion should recommend safe profile, got: {msg}"
        );
    }

    fn src_err(msg: &str) -> &'static str {
        categorize_source_error(&anyhow::anyhow!("{}", msg))
    }

    #[test]
    fn source_password_rejected_is_auth_error() {
        assert_eq!(
            src_err("password authentication failed for user \"rivet\""),
            "auth error"
        );
    }

    #[test]
    fn source_authentication_failed_is_auth_error() {
        assert_eq!(src_err("FATAL: authentication failed"), "auth error");
    }

    #[test]
    fn source_access_denied_is_auth_error() {
        assert_eq!(
            src_err("Access denied for user 'rivet'@'localhost'"),
            "auth error"
        );
    }

    #[test]
    fn source_connection_refused_is_connectivity() {
        assert_eq!(
            src_err("connection refused (os error 61)"),
            "connectivity error"
        );
    }

    #[test]
    fn source_timed_out_is_connectivity() {
        assert_eq!(src_err("connection timed out"), "connectivity error");
    }

    #[test]
    fn source_dns_translate_host_is_connectivity() {
        assert_eq!(
            src_err("could not translate host name \"db.bad\" to address"),
            "connectivity error"
        );
    }

    #[test]
    fn source_name_not_known_is_connectivity() {
        assert_eq!(src_err("Name or service not known"), "connectivity error");
    }

    #[test]
    fn source_unknown_error_is_generic() {
        assert_eq!(src_err("something totally unexpected"), "error");
    }

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
        assert_eq!(
            dest_err(
                "loading credential to sign http request",
                DestinationType::Gcs
            ),
            "auth error"
        );
    }

    #[test]
    fn dest_permission_denied_is_auth_error() {
        assert_eq!(
            dest_err("permission denied on resource bucket", DestinationType::S3),
            "auth error"
        );
    }

    #[test]
    fn dest_forbidden_is_auth_error() {
        assert_eq!(
            dest_err("403 Forbidden", DestinationType::Gcs),
            "auth error"
        );
    }

    #[test]
    fn dest_unauthorized_is_auth_error() {
        assert_eq!(
            dest_err("401 Unauthorized", DestinationType::S3),
            "auth error"
        );
    }

    #[test]
    fn dest_invalid_grant_is_auth_error() {
        assert_eq!(
            dest_err(
                "invalid_grant: token has been revoked",
                DestinationType::Gcs
            ),
            "auth error"
        );
    }

    #[test]
    fn dest_nosuchbucket_s3_is_bucket_not_found() {
        assert_eq!(
            dest_err(
                "NoSuchBucket: the specified bucket does not exist",
                DestinationType::S3
            ),
            "bucket not found"
        );
    }

    #[test]
    fn dest_not_found_gcs_is_bucket_not_found() {
        assert_eq!(
            dest_err("bucket not found (404)", DestinationType::Gcs),
            "bucket not found"
        );
    }

    #[test]
    fn dest_not_found_local_is_path_not_found() {
        assert_eq!(
            dest_err("path not found: /tmp/missing", DestinationType::Local),
            "path not found"
        );
    }

    #[test]
    fn dest_connection_refused_is_connectivity() {
        assert_eq!(
            dest_err("connection refused to endpoint", DestinationType::S3),
            "connectivity error"
        );
    }

    #[test]
    fn dest_dns_error_is_connectivity() {
        assert_eq!(
            dest_err("dns error: failed to lookup address", DestinationType::S3),
            "connectivity error"
        );
    }

    #[test]
    fn dest_timed_out_is_connectivity() {
        assert_eq!(
            dest_err("request timed out after 30s", DestinationType::Gcs),
            "connectivity error"
        );
    }

    #[test]
    fn dest_unknown_error_is_generic() {
        assert_eq!(
            dest_err("something else entirely", DestinationType::S3),
            "error"
        );
    }

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

    #[test]
    fn sparse_range_warning_when_very_sparse() {
        let mut e = make_export("t", ExportMode::Chunked, None);
        e.chunk_column = Some("id".to_string());
        e.chunk_size = 100_000;
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
        assert!(
            w.is_none(),
            "chunk_dense uses ordinals, not physical id span"
        );
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
        assert_eq!(
            level, 2,
            "no index + moderate should recommend 2 (conservative)"
        );
    }

    #[test]
    fn suggestion_acceptable_large_chunked_recommends_parallel() {
        let mut e = make_export("t", ExportMode::Chunked, None);
        e.chunk_column = Some("id".to_string());
        let s = build_suggestion(&HealthVerdict::Acceptable, Some(20_000_000), true, &e).unwrap();
        assert!(s.contains("parallel"), "got: {s}");
    }
}
