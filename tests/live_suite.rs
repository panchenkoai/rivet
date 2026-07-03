//! Consolidated LIVE integration suite — ONE test binary instead of 49.
//!
//! Every `tests/live_*.rs` suite shared the same `mod common;` and was built as its
//! own integration-test target, so `cargo test --all-targets` (run on every PR and
//! by the pre-push hook) linked the `common` harness 49 separate times even though
//! the tests themselves are `#[ignore]` and only run under docker. That link cost
//! dominated the offline build.
//!
//! The 49 suites now live under `tests/live/` — a subdir, so cargo does NOT build
//! each as its own target. This one entry declares `mod common;` ONCE and
//! `#[path]`-includes every suite as a nested module, so the whole set LINKS ONCE.
//! Run these under cargo-nextest (process-per-test): that isolation is what makes the
//! consolidation safe. Run them WITHOUT nextest (plain `cargo test`) and it is GONE — every
//! `#[test]` runs as a thread in one process, so an abort / SIGKILL / `std::process::exit` /
//! global-state corruption in one test takes its siblings with it. The default libtest harness
//! still collects every `#[test]`, and every `--ignored` / `--skip <name>` filter CI uses works
//! unchanged (those filter by test-name, not by `--test` target).
//!
//! NOT consolidated (still their own targets, named individually by CI via `--test`):
//! `live_differential` and `live_type_golden`.

mod common;

#[path = "live/audit_cli_dispatch.rs"]
mod audit_cli_dispatch;
#[path = "live/audit_cloud_multipart.rs"]
mod audit_cloud_multipart;
#[path = "live/audit_column_validation.rs"]
mod audit_column_validation;
#[path = "live/audit_doctor_fastfail.rs"]
mod audit_doctor_fastfail;
#[path = "live/audit_doctor_probe.rs"]
mod audit_doctor_probe;
#[path = "live/audit_init_deferred.rs"]
mod audit_init_deferred;
#[path = "live/audit_maxfile.rs"]
mod audit_maxfile;
#[path = "live/audit_observability.rs"]
mod audit_observability;
#[path = "live/audit_plan_apply.rs"]
mod audit_plan_apply;
#[path = "live/audit_preflight_table.rs"]
mod audit_preflight_table;
#[path = "live/audit_repair.rs"]
mod audit_repair;
#[path = "live/audit_repair_chunk_index.rs"]
mod audit_repair_chunk_index;
#[path = "live/audit_rerun.rs"]
mod audit_rerun;
#[path = "live/audit_state.rs"]
mod audit_state;
#[path = "live/audit_target_typo.rs"]
mod audit_target_typo;
#[path = "live/batch_memory_policy.rs"]
mod batch_memory_policy;
#[path = "live/gremlin.rs"]
mod gremlin;
#[path = "live/gremlin_cdc.rs"]
mod gremlin_cdc;
#[path = "live/live_azure_multipart.rs"]
mod live_azure_multipart;
#[path = "live/live_batch_switch_golden.rs"]
mod live_batch_switch_golden;
#[path = "live/live_catalog_hints.rs"]
mod live_catalog_hints;
#[path = "live/live_cdc.rs"]
mod live_cdc;
#[path = "live/live_cdc_golden.rs"]
mod live_cdc_golden;
#[path = "live/live_cdc_mssql.rs"]
mod live_cdc_mssql;
#[path = "live/live_cdc_oracle.rs"]
mod live_cdc_oracle;
#[path = "live/live_cdc_property.rs"]
mod live_cdc_property;
#[path = "live/live_cdc_replica.rs"]
mod live_cdc_replica;
#[path = "live/live_chaos.rs"]
mod live_chaos;
#[path = "live/live_chunked_dense.rs"]
mod live_chunked_dense;
#[path = "live/live_chunked_recovery.rs"]
mod live_chunked_recovery;
#[path = "live/live_cli_flags.rs"]
mod live_cli_flags;
#[path = "live/live_content_load.rs"]
mod live_content_load;
#[path = "live/live_crash_recovery.rs"]
mod live_crash_recovery;
#[path = "live/live_crash_soak.rs"]
mod live_crash_soak;
#[path = "live/live_cross_db_parity.rs"]
mod live_cross_db_parity;
#[path = "live/live_destination_parity.rs"]
mod live_destination_parity;
#[path = "live/live_governor.rs"]
mod live_governor;
#[path = "live/live_harness_canary.rs"]
mod live_harness_canary;
#[path = "live/live_init.rs"]
mod live_init;
#[path = "live/live_init_extended.rs"]
mod live_init_extended;
#[path = "live/live_keyset.rs"]
mod live_keyset;
#[path = "live/live_metrics_persist.rs"]
mod live_metrics_persist;
#[path = "live/live_mssql_chunked.rs"]
mod live_mssql_chunked;
#[path = "live/live_mssql_chunked_recovery.rs"]
mod live_mssql_chunked_recovery;
#[path = "live/live_mssql_crash_recovery.rs"]
mod live_mssql_crash_recovery;
#[path = "live/live_mssql_harm_permission.rs"]
mod live_mssql_harm_permission;
#[path = "live/live_mssql_reconcile_repair.rs"]
mod live_mssql_reconcile_repair;
#[path = "live/live_mssql_resume.rs"]
mod live_mssql_resume;
#[path = "live/live_mysql_chunked.rs"]
mod live_mysql_chunked;
#[path = "live/live_mysql_chunked_recovery.rs"]
mod live_mysql_chunked_recovery;
#[path = "live/live_mysql_crash_recovery.rs"]
mod live_mysql_crash_recovery;
#[path = "live/live_mysql_reconcile_repair.rs"]
mod live_mysql_reconcile_repair;
#[path = "live/live_mysql_resume.rs"]
mod live_mysql_resume;
#[path = "live/live_mysql_retry_and_faults.rs"]
mod live_mysql_retry_and_faults;
#[path = "live/live_mysql_schema_drift.rs"]
mod live_mysql_schema_drift;
#[path = "live/live_oltp_load.rs"]
mod live_oltp_load;
#[path = "live/live_parquet_roundtrip.rs"]
mod live_parquet_roundtrip;
#[path = "live/live_partition_by.rs"]
mod live_partition_by;
#[path = "live/live_partition_cloud.rs"]
mod live_partition_cloud;
#[path = "live/live_performance_smoke.rs"]
mod live_performance_smoke;
#[path = "live/live_pg_state.rs"]
mod live_pg_state;
#[path = "live/live_plan_apply.rs"]
mod live_plan_apply;
#[path = "live/live_plan_output_ux.rs"]
mod live_plan_output_ux;
#[path = "live/live_pool_safety.rs"]
mod live_pool_safety;
#[path = "live/live_reconcile_repair.rs"]
mod live_reconcile_repair;
#[path = "live/live_resume.rs"]
mod live_resume;
#[path = "live/live_retry_and_faults.rs"]
mod live_retry_and_faults;
#[path = "live/live_schema_drift.rs"]
mod live_schema_drift;
#[path = "live/live_temp_spill.rs"]
mod live_temp_spill;
#[path = "live/live_wave_apply.rs"]
mod live_wave_apply;
#[path = "live/preflight_missing_table.rs"]
mod preflight_missing_table;
#[path = "live/preflight_target_fail_note.rs"]
mod preflight_target_fail_note;
#[path = "live/quality_live.rs"]
mod quality_live;
#[path = "live/roast_metric_validated_ordering.rs"]
mod roast_metric_validated_ordering;
#[path = "live/roast_mssql_decimal_scale.rs"]
mod roast_mssql_decimal_scale;
#[path = "live/roast_part_loss.rs"]
mod roast_part_loss;
#[path = "live/roast_pg_json_fidelity.rs"]
mod roast_pg_json_fidelity;
#[path = "live/roast_small_table_escape.rs"]
mod roast_small_table_escape;
#[path = "live/roast_validate_exit.rs"]
mod roast_validate_exit;
#[path = "live/sec_exit_codes.rs"]
mod sec_exit_codes;
#[path = "live/sec_mcp_tls.rs"]
mod sec_mcp_tls;
#[path = "live/sec_preflight_sqli.rs"]
mod sec_preflight_sqli;
#[path = "live/sec_terminal_inject.rs"]
mod sec_terminal_inject;
#[path = "live/sec_tls_defaults.rs"]
mod sec_tls_defaults;
