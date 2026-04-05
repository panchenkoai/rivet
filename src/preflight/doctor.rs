use crate::config::{Config, DestinationType, SourceType};
use crate::error::Result;

pub fn doctor(config_path: &str) -> Result<()> {
    println!("rivet doctor: verifying auth for config '{}'", config_path);
    println!();

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

    match check_source_auth(&config) {
        Ok(()) => println!("[OK]  Source auth ({:?})", config.source.source_type),
        Err(e) => {
            all_ok = false;
            let category = categorize_source_error(&e);
            println!("[FAIL] Source {}: {}", category, e);
        }
    }

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

pub(super) fn categorize_source_error(err: &anyhow::Error) -> &'static str {
    let msg = err.to_string().to_lowercase();
    if msg.contains("password") || msg.contains("authentication") || msg.contains("access denied") {
        "auth error"
    } else if msg.contains("connect")
        || msg.contains("refused")
        || msg.contains("timed out")
        || msg.contains("could not translate host")
        || msg.contains("name or service not known")
    {
        "connectivity error"
    } else {
        "error"
    }
}

pub(super) fn categorize_dest_error(
    err: &anyhow::Error,
    dest: &crate::config::DestinationConfig,
) -> &'static str {
    let msg = err.to_string().to_lowercase();
    if msg.contains("credential")
        || msg.contains("permission denied")
        || msg.contains("access denied")
        || msg.contains("unauthorized")
        || msg.contains("forbidden")
        || msg.contains("invalid_grant")
        || msg.contains("token")
    {
        "auth error"
    } else if msg.contains("not found") || msg.contains("nosuchbucket") || msg.contains("404") {
        match dest.destination_type {
            DestinationType::S3 => "bucket not found",
            DestinationType::Gcs => "bucket not found",
            DestinationType::Local | DestinationType::Stdout => "path not found",
        }
    } else if msg.contains("connect")
        || msg.contains("refused")
        || msg.contains("timed out")
        || msg.contains("dns")
        || msg.contains("endpoint")
    {
        "connectivity error"
    } else {
        "error"
    }
}
