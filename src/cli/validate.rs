//! Cross-flag validation that clap cannot express declaratively.
//!
//! Each rule here exists because the constraint depends on the *combination*
//! of two or more parsed values rather than the presence/absence of a single
//! flag. Anything expressible via clap attributes (mutual exclusion,
//! `requires`, argument groups) should stay on the type in `args.rs`.

use super::args::{Commands, PlanFormat, ReconcileFormat, ValidateFormat};
use crate::error::Result;

/// Validate cross-flag invariants on the parsed [`Commands`]. Returns `Err`
/// with a human-readable message that `main.rs` can render as plain text or
/// JSON depending on `--json-errors`.
pub(super) fn validate_cli(cmd: &Commands) -> Result<()> {
    match cmd {
        Commands::Run(a) => {
            if a.parallel_exports && a.parallel_export_processes {
                anyhow::bail!(
                    "--parallel-exports and --parallel-export-processes are mutually exclusive; \
                     choose one parallelism mode"
                );
            }
        }
        Commands::Plan(a) if matches!(a.format, PlanFormat::Pretty) && a.output.is_some() => {
            anyhow::bail!("--output requires --format json (output is ignored in pretty mode)");
        }
        Commands::Reconcile {
            format: ReconcileFormat::Pretty,
            output: Some(_),
            ..
        } => {
            anyhow::bail!("--output requires --format json (output is ignored in pretty mode)");
        }
        Commands::Validate(a)
            if matches!(a.format, ValidateFormat::Pretty) && a.output.is_some() =>
        {
            anyhow::bail!("--output requires --format json (output is ignored in pretty mode)");
        }
        Commands::Repair(a)
            if matches!(a.format, ReconcileFormat::Pretty) && a.output.is_some() =>
        {
            anyhow::bail!("--output requires --format json (output is ignored in pretty mode)");
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_run(parallel_exports: bool, parallel_export_processes: bool) -> Commands {
        Commands::Run(crate::cli::args::RunArgs {
            config: "c.yaml".into(),
            export: None,
            validate: false,
            reconcile: false,
            resume: false,
            force: false,
            parallel_exports,
            parallel_export_processes,
            summary_output: None,
            json: false,
            params: vec![],
        })
    }

    #[test]
    fn validate_run_rejects_both_parallel_flags() {
        let cmd = make_run(true, true);
        assert!(validate_cli(&cmd).is_err());
    }

    #[test]
    fn validate_run_accepts_single_parallel_flag() {
        assert!(validate_cli(&make_run(true, false)).is_ok());
        assert!(validate_cli(&make_run(false, true)).is_ok());
        assert!(validate_cli(&make_run(false, false)).is_ok());
    }

    #[test]
    fn validate_plan_rejects_output_with_pretty() {
        let cmd = Commands::Plan(crate::cli::args::PlanArgs {
            config: "c.yaml".into(),
            export: None,
            params: vec![],
            output: Some("out.json".into()),
            format: PlanFormat::Pretty,
            annotate_waves: false,
        });
        assert!(validate_cli(&cmd).is_err());
    }

    #[test]
    fn validate_plan_accepts_output_with_json() {
        let cmd = Commands::Plan(crate::cli::args::PlanArgs {
            config: "c.yaml".into(),
            export: None,
            params: vec![],
            output: Some("out.json".into()),
            format: PlanFormat::Json,
            annotate_waves: false,
        });
        assert!(validate_cli(&cmd).is_ok());
    }

    #[test]
    fn validate_reconcile_rejects_output_with_pretty() {
        let cmd = Commands::Reconcile {
            config: "c.yaml".into(),
            export: "e".into(),
            format: ReconcileFormat::Pretty,
            output: Some("out.json".into()),
            params: vec![],
        };
        assert!(validate_cli(&cmd).is_err());
    }

    #[test]
    fn validate_reconcile_accepts_output_with_json() {
        let cmd = Commands::Reconcile {
            config: "c.yaml".into(),
            export: "e".into(),
            format: ReconcileFormat::Json,
            output: Some("out.json".into()),
            params: vec![],
        };
        assert!(validate_cli(&cmd).is_ok());
    }

    #[test]
    fn validate_repair_rejects_output_with_pretty() {
        let cmd = Commands::Repair(crate::cli::args::RepairArgs {
            config: "c.yaml".into(),
            export: "e".into(),
            report: None,
            execute: false,
            format: ReconcileFormat::Pretty,
            output: Some("out.json".into()),
            params: vec![],
        });
        assert!(validate_cli(&cmd).is_err());
    }

    #[test]
    fn validate_repair_accepts_output_with_json() {
        let cmd = Commands::Repair(crate::cli::args::RepairArgs {
            config: "c.yaml".into(),
            export: "e".into(),
            report: None,
            execute: false,
            format: ReconcileFormat::Json,
            output: Some("out.json".into()),
            params: vec![],
        });
        assert!(validate_cli(&cmd).is_ok());
    }
}
