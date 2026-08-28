//! Consolidated offline integration suite — ONE test binary instead of N.
//!
//! These self-contained offline tests (no shared `mod common`, no live `#[ignore]`, not named
//! individually by CI, no path-relative includes) live under `tests/offline/` — a subdir, so
//! cargo does NOT build each as its own target. This one entry `#[path]`-includes them, so the
//! whole set LINKS ONCE instead of N times (PoC measured 21 files: 42s -> 8s, 5x). The default
//! harness still collects every `#[test]` from each module.
//!
//! Run these under cargo-nextest (process-per-test) — as the pre-push hook and CI do. Under the
//! plain libtest harness (`cargo test --test offline_suite`) every `#[test]` here runs as a THREAD
//! in one process, so an abort / SIGKILL / `std::process::exit` / corrupted process-global state in
//! one test takes its siblings down with it. That isolation was free in the old one-binary-per-file
//! layout; consolidation made it conditional on the runner. See `.config/nextest.toml`.

#[path = "offline/attestation_matrix_guard.rs"]
mod attestation_matrix_guard;
#[path = "offline/audit_validate_warning_label.rs"]
mod audit_validate_warning_label;
#[path = "offline/cargo_manifest_chef.rs"]
mod cargo_manifest_chef;
#[path = "offline/cdc_axis_matrix_guard.rs"]
mod cdc_axis_matrix_guard;
#[path = "offline/skip_is_not_a_pass_guard.rs"]
mod skip_is_not_a_pass_guard;

#[path = "offline/cdc_cli_surface_guard.rs"]
mod cdc_cli_surface_guard;

#[path = "offline/cdc_evidence_matrix_guard.rs"]
mod cdc_evidence_matrix_guard;
#[path = "offline/changelog_guard.rs"]
mod changelog_guard;
#[path = "offline/chunking_matrix_guard.rs"]
mod chunking_matrix_guard;
#[path = "offline/cli_contract.rs"]
mod cli_contract;
#[path = "offline/config_fuzz.rs"]
mod config_fuzz;
#[path = "offline/config_parse_errors.rs"]
mod config_parse_errors;
#[path = "offline/config_secrets.rs"]
mod config_secrets;
#[path = "offline/destructive_delete_gate.rs"]
mod destructive_delete_gate;
#[path = "offline/examples_parse.rs"]
mod examples_parse;
#[path = "offline/extension_seam.rs"]
mod extension_seam;
#[path = "offline/format_fuzz.rs"]
mod format_fuzz;
#[path = "offline/harness_metrics_guard.rs"]
mod harness_metrics_guard;
#[path = "offline/mssql_column_data_fixture_guard.rs"]
mod mssql_column_data_fixture_guard;
#[path = "offline/mutation_gate_config.rs"]
mod mutation_gate_config;
#[path = "offline/mutation_gate_priority_guard.rs"]
mod mutation_gate_priority_guard;
#[path = "offline/mysql_wire_type_fixture_guard.rs"]
mod mysql_wire_type_fixture_guard;
// Not a test module: the shared non-vacuity rule the grep-shaped guards call so
// a moved subject fails LOUDLY instead of grading the empty set.
#[path = "offline/nonvacuity.rs"]
mod nonvacuity;
#[path = "offline/oracle_read_scope_guard.rs"]
mod oracle_read_scope_guard;
#[path = "offline/perf_matrix_guard.rs"]
mod perf_matrix_guard;
#[path = "offline/planner_fuzz.rs"]
mod planner_fuzz;
#[path = "offline/redaction_invariant.rs"]
mod redaction_invariant;
#[path = "offline/release_gate_matrix_guard.rs"]
mod release_gate_matrix_guard;
#[path = "offline/release_oracle_entrypoint_guard.rs"]
mod release_oracle_entrypoint_guard;
#[path = "offline/resource_smoke.rs"]
mod resource_smoke;
#[path = "offline/retry_integration.rs"]
mod retry_integration;
#[path = "offline/run_summary_contract.rs"]
mod run_summary_contract;

#[path = "offline/cli_flag_coverage_guard.rs"]
mod cli_flag_coverage_guard;
#[path = "offline/live_only_purity_gate.rs"]
mod live_only_purity_gate;
#[path = "offline/live_service_ports_guard.rs"]
mod live_service_ports_guard;
#[path = "offline/rig_adoption_guard.rs"]
mod rig_adoption_guard;
#[path = "offline/runner_frame_gate.rs"]
mod runner_frame_gate;
#[path = "offline/scenario_artifact_matrix_guard.rs"]
mod scenario_artifact_matrix_guard;
#[path = "offline/schema_drift.rs"]
mod schema_drift;
#[path = "offline/schema_evolution.rs"]
mod schema_evolution;
#[path = "offline/state_compat.rs"]
mod state_compat;
#[path = "offline/time_window.rs"]
mod time_window;
#[path = "offline/trust_artifacts_integration.rs"]
mod trust_artifacts_integration;
#[path = "offline/validate_historical.rs"]
mod validate_historical;
#[path = "offline/validate_regression.rs"]
mod validate_regression;
#[path = "offline/workflow_stack_guard.rs"]
mod workflow_stack_guard;
