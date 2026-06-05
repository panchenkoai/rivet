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
            if let Some(hint) = source_error_hint(category, &e, &config.source.source_type) {
                println!("       Hint: {}", hint);
            }
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
            DestinationType::Azure => format!(
                "Azure({})",
                export.destination.bucket.as_deref().unwrap_or("?")
            ),
            DestinationType::Stdout => {
                log::info!("  Stdout: no auth check needed");
                continue;
            }
        };

        // Apply `{date}`/`{export}`/`{table}` substitution so the probe
        // write lands at the same prefix `run` would use — otherwise we
        // leave a literal `runs/{date}/{export}/.rivet_doctor_probe` object
        // at the destination (visible on 2026-05-21 against real Azure).
        let expanded_dest = crate::plan::build::expand_destination_templates(
            export.destination.clone(),
            &export.name,
        );
        match check_destination_auth(&expanded_dest) {
            Ok(()) => println!("[OK]  Destination {}", label),
            Err(e) => {
                all_ok = false;
                let category = categorize_dest_error(&e, &expanded_dest);
                println!("[FAIL] Destination {} -- {}: {}", label, category, e);
                if let Some(hint) = destination_error_hint(category, &expanded_dest) {
                    println!("       Hint: {}", hint);
                }
            }
        }
    }

    println!();
    if all_ok {
        println!("All checks passed.");
        Ok(())
    } else {
        // F-NEW-A (0.7.5 audit): previously `doctor` printed
        // "Some checks failed" and returned `Ok(())`, so the exit
        // code was 0 even when source auth or destination probe
        // failed.  CI / cron orchestration that only inspects rc
        // could not tell a healthy environment from a broken one.
        // The fail-line is still printed; the exit code now matches.
        println!("Some checks failed. Fix the issues above before running exports.");
        anyhow::bail!("doctor: one or more preflight checks failed (see output above)")
    }
}

fn check_source_auth(config: &Config) -> Result<()> {
    let url = config.source.resolve_url()?;
    let tls = config.source.tls.as_ref();
    // `doctor` is meant to surface security misconfigurations *before*
    // the operator runs a real export. Plaintext-source connections are
    // a security misconfiguration; the warn was previously only
    // emitted from `create_source` (the `run` path), so an operator
    // could pass `doctor` clean and only learn about the TLS gap on
    // first `rivet run`. The helper is `Once`-gated so duplicate calls
    // from later phases don't restack the line.
    crate::source::warn_if_tls_disabled(&config.source);
    match config.source.source_type {
        SourceType::Postgres => {
            let mut client = crate::source::postgres::connect_client(&url, tls)?;
            client.simple_query("SELECT 1")?;
            Ok(())
        }
        SourceType::Mysql => {
            let pool = crate::source::mysql::connect_pool(&url, tls)?;
            let mut conn = pool.get_conn()?;
            use mysql::prelude::Queryable;
            conn.query_drop("SELECT 1")?;
            Ok(())
        }
        SourceType::Mssql => {
            // `connect_with_tls` runs a connect + `SELECT 1` round-trip itself,
            // so a successful construction is a green health-check.
            crate::source::mssql::MssqlSource::connect_with_tls(&url, tls)?;
            Ok(())
        }
    }
}

fn check_destination_auth(dest: &crate::config::DestinationConfig) -> Result<()> {
    use crate::destination::create_destination;
    let d = create_destination(dest)?;
    let probe_key = crate::manifest::DOCTOR_PROBE_FILENAME;
    let tmp = std::env::temp_dir().join(probe_key);
    std::fs::write(&tmp, b"ok")?;
    match d.write(&tmp, probe_key) {
        Ok(_) => {
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
    // CONTRACT: the pattern below must match the error text emitted by
    // `enforce_sas_expiry` in destination/azure.rs:
    //   "Azure SAS token already expired (se=…)"
    // If that message ever changes, update both places together.
    if msg.contains("already expired") && msg.contains("sas") {
        return "sas expired";
    }
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
            DestinationType::Azure => "container not found",
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

/// Map a categorised source error (+ raw text) to an actionable hint.
///
/// Returns `None` when nothing more useful than the underlying error
/// itself can be said.  The output is intentionally short — `doctor`
/// already prints the full driver message; the hint should add the
/// *next action*, not re-explain the failure.
///
/// Categories come from [`categorize_source_error`].
pub(super) fn source_error_hint(
    category: &'static str,
    err: &anyhow::Error,
    source_type: &crate::config::SourceType,
) -> Option<&'static str> {
    use crate::config::SourceType;
    let msg = err.to_string().to_lowercase();

    // TLS misconfig leaks through every category — check first so a
    // generic "error" with a TLS root cause still gets the right hint.
    if msg.contains("tls")
        || msg.contains("ssl")
        || msg.contains("certificate")
        || msg.contains("handshake")
    {
        return Some(match source_type {
            SourceType::Postgres => {
                "TLS handshake failed. Try `tls.mode: prefer` (downgrade gracefully) or set `tls.ca_file: /path/to/ca-bundle.pem` if your DB uses a private CA."
            }
            SourceType::Mysql => {
                "TLS handshake failed. Try `tls.mode: prefer` or set `tls.ca_file: /path/to/ca-bundle.pem` to trust the DB's certificate authority."
            }
            SourceType::Mssql => {
                "TLS handshake failed. SQL Server forces TLS on the login handshake; set `tls.ca_file: /path/to/ca-bundle.pem` to trust a private CA, or `tls.accept_invalid_certs: true` for a self-signed dev cert."
            }
        });
    }

    match category {
        "auth error" => Some(match source_type {
            SourceType::Postgres => {
                "Verify the user/password and that pg_hba.conf permits your client IP. The user also needs SELECT on the target tables and USAGE on the schema."
            }
            SourceType::Mysql => {
                "Verify the user/password and that the user has SELECT grants on the target tables. MySQL `GRANT SELECT ON db.* TO 'user'@'host'` plus `FLUSH PRIVILEGES`."
            }
            SourceType::Mssql => {
                "Verify the SQL login/password and that the login maps to a database user with SELECT on the target tables (`GRANT SELECT ON dbo.tbl TO [user]`). Check you are pointed at the right database — contained-DB users and server logins are resolved differently."
            }
        }),
        "connectivity error" => Some(
            "Verify host/port reachability from this machine. If the DB is behind a bastion or VPN, ensure the tunnel is up before running rivet. `rivet doctor` must run from the same network as `rivet run` will.",
        ),
        _ => None,
    }
}

/// Map a categorised destination error (+ raw text) to an actionable hint.
///
/// Mirrors [`source_error_hint`] but with backend-specific guidance
/// (S3 region / IAM / endpoint, GCS service account / ADC, Azure key /
/// SAS / RBAC).  Returns `None` when nothing better than the raw error
/// applies.
pub(super) fn destination_error_hint(
    category: &'static str,
    dest: &crate::config::DestinationConfig,
) -> Option<&'static str> {
    match category {
        "sas expired" => Some(
            "Azure SAS token is expired or near-expiry. Generate a new SAS via `az storage container generate-sas --permissions rwdlc --expiry <future-date>` and re-export AZURE_STORAGE_SAS_TOKEN.",
        ),
        "auth error" => Some(match dest.destination_type {
            DestinationType::S3 => {
                "Verify AWS credentials resolve (env / profile / instance role) and that the role has s3:PutObject + s3:GetObject + s3:ListBucket on the prefix. See docs/cloud-permissions.md."
            }
            DestinationType::Gcs => {
                "Verify the service account credentials resolve (ADC / env / explicit credentials_file) and that the principal has storage.objects.{create,get,list} on the bucket. See docs/cloud-permissions.md."
            }
            DestinationType::Azure => {
                "Verify Azure credentials. Account-key auth: check account_key_env. SAS auth: regenerate the SAS with rwdlc permissions and a future expiry. See docs/cloud-permissions.md."
            }
            DestinationType::Local | DestinationType::Stdout => {
                "Verify filesystem permissions on the destination directory."
            }
        }),
        "bucket not found" | "container not found" => Some(match dest.destination_type {
            DestinationType::S3 => {
                "Bucket must already exist; rivet does NOT auto-create. `aws s3 mb s3://<bucket>` (with the right region) before running."
            }
            DestinationType::Gcs => {
                "Bucket must already exist; rivet does NOT auto-create. `gcloud storage buckets create gs://<bucket>` before running."
            }
            DestinationType::Azure => {
                "Container must already exist; rivet does NOT auto-create. `az storage container create --account-name <acct> --name <container>` before running."
            }
            _ => "Path / bucket / container must already exist.",
        }),
        "connectivity error" => Some(match dest.destination_type {
            DestinationType::S3 => {
                "Verify endpoint and region. For non-AWS endpoints (MinIO / R2 / Wasabi) set `endpoint:` explicitly. For AWS, ensure `region:` matches the bucket's region — cross-region writes fail with a confusing redirect error."
            }
            DestinationType::Gcs => {
                "Verify network reachability to storage.googleapis.com. If using a custom endpoint, set `endpoint:` explicitly."
            }
            DestinationType::Azure => {
                "Verify network reachability to <account>.blob.core.windows.net. For Azurite or sovereign clouds, set `endpoint:` explicitly."
            }
            _ => "Verify network reachability to the destination.",
        }),
        "path not found" => Some(
            "Parent directory must exist. Create it with `mkdir -p` before running, or use a different `path:` in your config.",
        ),
        _ => None,
    }
}
