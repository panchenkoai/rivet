mod config;
mod destination;
mod error;
mod format;
mod pipeline;
mod preflight;
mod source;
mod state;
mod tuning;
mod types;

use clap::{Parser, Subcommand};
use error::Result;

#[derive(Parser)]
#[command(name = "rivet", version, about = "Export data from databases to files")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run export jobs defined in config
    Run {
        /// Path to YAML config file
        #[arg(short, long)]
        config: String,
        /// Run only a specific export by name
        #[arg(short, long)]
        export: Option<String>,
    },
    /// Preflight check: diagnose source health for each export
    Check {
        /// Path to YAML config file
        #[arg(short, long)]
        config: String,
        /// Check only a specific export by name
        #[arg(short, long)]
        export: Option<String>,
    },
    /// Manage export state
    State {
        #[command(subcommand)]
        action: StateAction,
    },
}

#[derive(Subcommand)]
enum StateAction {
    /// Show current state for all exports
    Show {
        #[arg(short, long)]
        config: String,
    },
    /// Reset state for an export
    Reset {
        #[arg(short, long)]
        config: String,
        /// Export name to reset
        #[arg(short, long)]
        export: String,
    },
}

fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Run { config, export } => {
            pipeline::run(&config, export.as_deref())?;
        }
        Commands::Check { config, export } => {
            preflight::check(&config, export.as_deref())?;
        }
        Commands::State { action } => match action {
            StateAction::Show { config } => {
                pipeline::show_state(&config)?;
            }
            StateAction::Reset { config, export } => {
                pipeline::reset_state(&config, &export)?;
            }
        },
    }

    Ok(())
}
