mod analysis;
pub(crate) mod cursor_expr;
mod doctor;
mod mysql;
mod postgres;
pub mod type_report;

pub(crate) use analysis::chunk_sparsity_from_counts;
#[cfg(test)]
use analysis::{
    build_suggestion, check_connection_limit, check_dense_surrogate_cost,
    check_parallel_memory_risk, check_sparse_range, compute_verdict, derive_strategy,
    recommend_parallelism, recommend_profile,
};
#[allow(unused_imports)]
pub use doctor::doctor;
#[cfg(test)]
use postgres::{extract_scan_type, parse_pg_row_estimate};

use crate::config::{Config, ExportConfig, SourceType};
use crate::error::Result;
use crate::types::policy::TypePolicy;
use crate::types::target::ExportTarget;

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
    pub cursor_column: Option<String>,
    pub row_estimate: Option<i64>,
    pub cursor_min: Option<String>,
    pub cursor_max: Option<String>,
    pub scan_type: Option<String>,
    pub uses_index: bool,
    pub verdict: HealthVerdict,
    pub recommended_profile: &'static str,
    pub recommended_parallel: (u32, &'static str),
    pub warnings: Vec<String>,
    pub suggestion: Option<String>,
}

/// Return the diagnostic for a single export without printing anything.
///
/// Used by `rivet plan` to capture preflight data into a `PlanArtifact`.
pub(crate) fn get_export_diagnostic(
    config: &Config,
    export: &ExportConfig,
) -> Result<ExportDiagnostic> {
    let url = config.source.resolve_url()?;
    let tls = config.source.tls.as_ref();
    crate::source::warn_if_tls_disabled(&config.source);
    match config.source.source_type {
        SourceType::Postgres => postgres::diagnose_export_pg(&url, tls, export),
        SourceType::Mysql => mysql::diagnose_export_mysql(&url, tls, export),
        SourceType::Mssql => {
            anyhow::bail!("mssql preflight diagnostics not yet implemented (scaffold)")
        }
    }
}

/// Dedup identity for a destination, shared by `check`'s credential probe
/// and `doctor`'s write probe. Must include every field that changes where
/// a probe lands — notably `path`, so two local destinations with different
/// paths are probed separately. Keeping one helper prevents the two call
/// sites from drifting apart (doctor's inline copy once omitted `path` and
/// silently skipped the second local destination).
fn destination_identity(d: &crate::config::DestinationConfig) -> String {
    format!(
        "{:?}:{}:{}:{}",
        d.destination_type,
        d.bucket.as_deref().unwrap_or("-"),
        d.endpoint.as_deref().unwrap_or("-"),
        d.path.as_deref().unwrap_or("-"),
    )
}

pub fn check(
    config_path: &str,
    export_name: Option<&str>,
    params: Option<&std::collections::HashMap<String, String>>,
    show_type_report: bool,
    strict: bool,
    json_output: bool,
    target: Option<ExportTarget>,
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
    let tls = config.source.tls.as_ref();
    // Surface the plaintext-transport warning at preflight time too —
    // operators should hear it from `rivet check` before they wait
    // through a full `rivet run` to learn the same thing. `Once` inside
    // the helper keeps emission to one line per process even when both
    // `check` and `run` flow through it.
    crate::source::warn_if_tls_disabled(&config.source);
    match config.source.source_type {
        SourceType::Postgres => postgres::check_postgres(&url, tls, &exports, json_output)?,
        SourceType::Mysql => mysql::check_mysql(&url, tls, &exports, json_output)?,
        SourceType::Mssql => anyhow::bail!("mssql check not yet implemented (scaffold)"),
    }

    // Destination credential-resolution preflight.  Until 0.7.6 `check` only
    // probed the source: a config with `AWS_ACCESS_KEY_ID` unset would pass
    // `rivet check` (rc=0) and then explode on `run`, while `rivet doctor`
    // caught it.  We don't issue a write-probe here (that is `doctor`'s job
    // and has side effects) — but we *do* call `create_destination`, which
    // resolves env vars / credentials_file existence at construction time.
    // Each unique destination is probed once per `check` to keep multi-export
    // configs cheap.
    let mut seen_destinations: std::collections::HashSet<String> = std::collections::HashSet::new();
    for export in &exports {
        let dest_key = destination_identity(&export.destination);
        if !seen_destinations.insert(dest_key) {
            continue;
        }
        let expanded = crate::plan::build::expand_destination_templates(
            export.destination.clone(),
            &export.name,
        );
        crate::destination::create_destination(&expanded).map_err(|e| {
            anyhow::anyhow!(
                "export '{}': destination preflight failed: {:#}",
                export.name,
                e
            )
        })?;
    }

    if show_type_report {
        let policy = if strict {
            TypePolicy::strict()
        } else {
            TypePolicy::warn_only()
        };

        let mut any_fatal = false;
        for export in &exports {
            let column_overrides =
                crate::plan::parse_column_overrides_pub(&export.columns, &export.name)?;
            // CLI `--target` wins; otherwise fall back to the per-export
            // `target:` from the config (slice #2a). A declared-but-unknown
            // target is a loud error — never silently ignored.
            if let Some(t) = export.target.as_deref()
                && crate::types::target::ExportTarget::parse(t).is_none()
            {
                anyhow::bail!(
                    "export '{}': unknown target '{t}' (expected: bigquery, duckdb)",
                    export.name
                );
            }
            let eff_target = target.or_else(|| {
                export
                    .target
                    .as_deref()
                    .and_then(crate::types::target::ExportTarget::parse)
            });
            let config_dir = std::path::Path::new(config_path)
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."));
            match type_report::collect_report(
                &config,
                export,
                &column_overrides,
                &policy,
                eff_target,
                config_dir,
                params,
            ) {
                Ok(report) => {
                    if report.has_fatal() {
                        any_fatal = true;
                    }
                    if eff_target.is_some() && report.has_target_fail() {
                        any_fatal = true;
                    }
                    if json_output {
                        type_report::print_json(&report)?;
                    } else {
                        type_report::print_table(&report, eff_target);
                    }
                }
                Err(e) => {
                    log::warn!("type report for '{}' failed: {:#}", export.name, e);
                }
            }
        }

        if strict && any_fatal {
            anyhow::bail!("strict mode: unsafe type mappings found (see report above)");
        }
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
    if let Some(col) = &diag.cursor_column {
        println!("  Cursor col:   {}", col);
    }
    if let Some(scan) = &diag.scan_type {
        let idx_label = if diag.uses_index { " (indexed)" } else { "" };
        println!("  Scan type:    {}{}", scan, idx_label);
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
    use crate::config::{DestinationConfig, DestinationType, ExportConfig, ExportMode, FormatType};
    use doctor::{
        categorize_dest_error, categorize_source_error, destination_error_hint, source_error_hint,
    };

    fn make_export(name: &str, mode: ExportMode, cursor: Option<&str>) -> ExportConfig {
        // Baseline from the canonical test fixture; override only the fields
        // these preflight tests vary (mode, cursor, CSV format, query, dest).
        ExportConfig {
            mode,
            cursor_column: cursor.map(|s| s.to_string()),
            query: Some("SELECT * FROM t".to_string()),
            format: FormatType::Csv,
            destination: DestinationConfig {
                destination_type: DestinationType::Local,
                path: Some("./out".to_string()),
                ..Default::default()
            },
            ..crate::config::sample_export(name)
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
            ..Default::default()
        }
    }

    fn dest_err(msg: &str, dtype: DestinationType) -> &'static str {
        let cfg = dest_config(dtype);
        categorize_dest_error(&anyhow::anyhow!("{}", msg), &cfg)
    }

    fn local_dest(path: &str) -> DestinationConfig {
        DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some(path.to_string()),
            ..Default::default()
        }
    }

    // Regression (doctor-dedup): doctor's inline dedup key omitted `path`,
    // so two local destinations with different paths collapsed to one entry
    // and the second was never write-probed. The shared identity must keep
    // them distinct.
    #[test]
    fn destination_identity_distinguishes_local_paths() {
        assert_ne!(
            destination_identity(&local_dest("/tmp/a")),
            destination_identity(&local_dest("/tmp/b")),
        );
    }

    #[test]
    fn destination_identity_collapses_identical_local_destinations() {
        assert_eq!(
            destination_identity(&local_dest("/tmp/a")),
            destination_identity(&local_dest("/tmp/a")),
        );
    }

    #[test]
    fn destination_identity_distinguishes_buckets() {
        let a = DestinationConfig {
            bucket: Some("bucket-a".to_string()),
            ..dest_config(DestinationType::S3)
        };
        let b = DestinationConfig {
            bucket: Some("bucket-b".to_string()),
            ..dest_config(DestinationType::S3)
        };
        assert_ne!(destination_identity(&a), destination_identity(&b));
    }

    // Same bucket name on different endpoints (e.g. AWS vs MinIO) is two
    // distinct destinations and must be probed separately.
    #[test]
    fn destination_identity_distinguishes_endpoints_for_same_bucket() {
        let aws = dest_config(DestinationType::S3);
        let minio = DestinationConfig {
            endpoint: Some("http://localhost:9000".to_string()),
            ..dest_config(DestinationType::S3)
        };
        assert_ne!(destination_identity(&aws), destination_identity(&minio));
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

    #[test]
    fn connection_limit_warn_when_parallel_meets_max() {
        let w = check_connection_limit(20, Some(20));
        assert!(w.is_some(), "should warn when parallel == max_connections");
        let msg = w.unwrap();
        assert!(msg.contains("max_connections=20"), "got: {msg}");
        assert!(msg.contains("parallel=20"), "got: {msg}");
    }

    #[test]
    fn connection_limit_warn_when_parallel_exceeds_max() {
        let w = check_connection_limit(100, Some(20));
        assert!(w.is_some(), "should warn when parallel > max_connections");
        let msg = w.unwrap();
        assert!(msg.contains("max_connections=20"), "got: {msg}");
    }

    #[test]
    fn connection_limit_no_warn_when_parallel_below_max() {
        let w = check_connection_limit(4, Some(100));
        assert!(
            w.is_none(),
            "should not warn when parallel << max_connections"
        );
    }

    #[test]
    fn connection_limit_no_warn_when_parallel_is_one() {
        let w = check_connection_limit(1, Some(5));
        assert!(
            w.is_none(),
            "single worker never triggers connection warning"
        );
    }

    #[test]
    fn connection_limit_skipped_note_when_max_unknown_and_parallel_gt_one() {
        let w = check_connection_limit(100, None);
        assert!(w.is_some(), "should note that check was skipped");
        let msg = w.unwrap();
        assert!(msg.contains("skipped"), "got: {msg}");
    }

    #[test]
    fn connection_limit_no_note_when_max_unknown_and_parallel_is_one() {
        let w = check_connection_limit(1, None);
        assert!(
            w.is_none(),
            "single worker never triggers connection warning"
        );
    }

    #[test]
    fn connection_limit_suggests_headroom() {
        let w = check_connection_limit(25, Some(20)).unwrap();
        // Suggested safe max should be max_connections - 3 = 17
        assert!(
            w.contains("17"),
            "should suggest leaving headroom, got: {w}"
        );
    }

    // ── v0.7.4: actionable hints next to categorised errors ───────────

    fn src_hint(msg: &str, st: SourceType) -> Option<&'static str> {
        let err = anyhow::anyhow!("{}", msg);
        let cat = categorize_source_error(&err);
        source_error_hint(cat, &err, &st)
    }

    fn dest_hint(msg: &str, dt: DestinationType) -> Option<&'static str> {
        let err = anyhow::anyhow!("{}", msg);
        let dest = DestinationConfig {
            destination_type: dt,
            bucket: Some("b".into()),
            ..Default::default()
        };
        let cat = categorize_dest_error(&err, &dest);
        destination_error_hint(cat, &dest)
    }

    #[test]
    fn source_tls_handshake_returns_pg_specific_tls_hint() {
        let h = src_hint("TLS handshake failed", SourceType::Postgres).expect("hint");
        assert!(h.contains("tls.mode") && h.contains("ca_file"), "got: {h}");
    }

    #[test]
    fn source_tls_handshake_returns_mysql_specific_tls_hint() {
        let h = src_hint("certificate verify failed", SourceType::Mysql).expect("hint");
        assert!(h.contains("tls.mode"), "got: {h}");
    }

    #[test]
    fn source_auth_error_postgres_mentions_pg_hba() {
        let h = src_hint("password authentication failed", SourceType::Postgres).expect("hint");
        assert!(h.contains("pg_hba") && h.contains("SELECT"), "got: {h}");
    }

    #[test]
    fn source_auth_error_mysql_mentions_grant() {
        let h = src_hint(
            "Access denied for user 'rivet'@'localhost'",
            SourceType::Mysql,
        )
        .expect("hint");
        assert!(h.contains("GRANT") && h.contains("FLUSH"), "got: {h}");
    }

    #[test]
    fn source_connectivity_error_mentions_bastion_and_network() {
        let h = src_hint("connection refused", SourceType::Postgres).expect("hint");
        assert!(h.contains("bastion") || h.contains("VPN"), "got: {h}");
    }

    #[test]
    fn source_unknown_error_returns_no_hint() {
        // Generic "error" category should yield no hint — better to
        // print the raw driver message than to mislead.
        let h = src_hint("totally unexpected", SourceType::Postgres);
        assert!(h.is_none(), "unknown errors should not produce a hint");
    }

    #[test]
    fn dest_s3_auth_error_names_concrete_actions() {
        let h = dest_hint("permission denied", DestinationType::S3).expect("hint");
        assert!(
            h.contains("s3:PutObject") && h.contains("cloud-permissions"),
            "got: {h}"
        );
    }

    #[test]
    fn dest_gcs_auth_error_names_concrete_actions() {
        let h = dest_hint("403 Forbidden", DestinationType::Gcs).expect("hint");
        assert!(
            h.contains("storage.objects") && h.contains("cloud-permissions"),
            "got: {h}"
        );
    }

    #[test]
    fn categorize_dest_error_sas_expired_message_returns_sas_expired_category() {
        // Guard the load-bearing ordering in categorize_dest_error: the
        // "sas expired" early-return must fire before the generic "token"
        // branch, or destination_error_hint produces the wrong hint.
        // This test pins the *category string*, not just the final hint text.
        let err = anyhow::anyhow!(
            "Azure SAS token already expired (se=2024-01-01T00:00:00Z). Generate a new SAS and re-export."
        );
        let dest = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("c".into()),
            ..Default::default()
        };
        let cat = categorize_dest_error(&err, &dest);
        assert_eq!(
            cat, "sas expired",
            "expired-SAS error must categorise as 'sas expired', not '{cat}' — ordering in categorize_dest_error is load-bearing"
        );
    }

    #[test]
    fn dest_azure_sas_expired_returns_regenerate_hint() {
        // The Azure preflight (v0.7.4) bails with "expired (se=…)" —
        // the hint must steer the operator to `az storage container
        // generate-sas` not "your IAM role is broken".
        let h = dest_hint(
            "Azure SAS token already expired (se=2024-01-01T00:00:00Z)",
            DestinationType::Azure,
        )
        .expect("hint");
        assert!(
            h.contains("generate-sas") && h.contains("AZURE_STORAGE_SAS_TOKEN"),
            "got: {h}"
        );
    }

    #[test]
    fn dest_s3_bucket_not_found_says_no_auto_create() {
        let h = dest_hint("NoSuchBucket", DestinationType::S3).expect("hint");
        assert!(
            h.contains("does NOT auto-create") && h.contains("aws s3 mb"),
            "got: {h}"
        );
    }

    #[test]
    fn dest_s3_connectivity_error_warns_about_region_mismatch() {
        let h = dest_hint("dns error", DestinationType::S3).expect("hint");
        assert!(h.contains("region") || h.contains("endpoint"), "got: {h}");
    }
}
