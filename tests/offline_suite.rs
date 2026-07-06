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

#[path = "offline/audit_validate_warning_label.rs"]
mod audit_validate_warning_label;
#[path = "offline/cargo_manifest_chef.rs"]
mod cargo_manifest_chef;
#[path = "offline/cli_contract.rs"]
mod cli_contract;
#[path = "offline/config_fuzz.rs"]
mod config_fuzz;
#[path = "offline/config_parse_errors.rs"]
mod config_parse_errors;
#[path = "offline/config_secrets.rs"]
mod config_secrets;
#[path = "offline/examples_parse.rs"]
mod examples_parse;
#[path = "offline/extension_seam.rs"]
mod extension_seam;
#[path = "offline/format_fuzz.rs"]
mod format_fuzz;
#[path = "offline/planner_fuzz.rs"]
mod planner_fuzz;
#[path = "offline/redaction_invariant.rs"]
mod redaction_invariant;
#[path = "offline/resource_smoke.rs"]
mod resource_smoke;
#[path = "offline/retry_integration.rs"]
mod retry_integration;
#[path = "offline/run_summary_contract.rs"]
mod run_summary_contract;
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
