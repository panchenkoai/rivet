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
        let dest_key = super::destination_identity(&export.destination);
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
    // FINDING #26: the write-probe drops a `.rivet_doctor_probe` object at the
    // destination prefix to verify write access; we must remove it so `doctor`
    // leaves the prefix exactly as it found it (the local temp above is a
    // *separate* file in `temp_dir`, not the destination object). The probe
    // landed at the same key `write` resolved: `probe_key` joined to the
    // destination root.
    remove_destination_probe(dest, probe_key);
    Ok(())
}

/// Resolve the local destination root the way `LocalDestination::new` does.
///
/// Kept inline rather than reaching into the backend so `doctor` depends only
/// on the config, not on `LocalDestination`'s private field. MUST stay in sync
/// with `destination/local.rs`: `path` wins, then `prefix`, then `.`.
fn local_base_path(dest: &crate::config::DestinationConfig) -> String {
    dest.path
        .clone()
        .or_else(|| dest.prefix.clone())
        .unwrap_or_else(|| ".".to_string())
}

/// Best-effort removal of the destination-side write-probe (FINDING #26).
///
/// Local destinations are removed directly: `write` lands the probe at
/// `<base>/<probe_key>` (an atomic temp-then-rename), so a plain
/// `remove_file` restores the prefix to empty. A missing probe is benign
/// (`NotFound` swallowed) — `doctor` already passed the write check, this is
/// pure tidy-up and must never turn a healthy run into a failure.
///
/// Cloud backends (S3/GCS/Azure) expose no delete on the `Destination` trait,
/// so the probe object persists; `manifest_reconcile` already filters
/// `DOCTOR_PROBE_FILENAME` out of listings, so the residue never confuses
/// verification. It is logged (not silently ignored) but at DEBUG, not WARN:
/// on the happy path the leftover is benign and a WARN on every successful
/// cloud `doctor` run would be alarming noise — a fully clean removal needs a
/// `delete` on the `Destination` trait, tracked separately.
fn remove_destination_probe(dest: &crate::config::DestinationConfig, probe_key: &str) {
    match dest.destination_type {
        DestinationType::Local => {
            let probe_path = std::path::Path::new(&local_base_path(dest)).join(probe_key);
            match std::fs::remove_file(&probe_path) {
                Ok(()) => log::debug!("doctor: removed destination probe {}", probe_path.display()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => log::warn!(
                    "doctor: could not remove destination probe {} (left at prefix): {e}",
                    probe_path.display()
                ),
            }
        }
        DestinationType::Stdout => {} // streaming sink writes nothing to clean up
        DestinationType::S3 | DestinationType::Gcs | DestinationType::Azure => {
            log::debug!(
                "doctor: destination probe '{probe_key}' left at the {:?} prefix \
                 (no object-delete on this backend); manifest reconcile filters it from listings",
                dest.destination_type
            );
        }
    }
}

pub(super) fn categorize_source_error(err: &anyhow::Error) -> &'static str {
    // `{:#}` (alternate) walks the anyhow cause chain, not just the top
    // Display. Postgres surfaces a wrong password as the bare top-level
    // `"db error"` and buries `"password authentication failed for user …"`
    // in `.source()`; `{}` would never see the real reason and the operator
    // would get the useless generic "error" bucket. The alternate form joins
    // the chain so the auth/connectivity needles below match the true cause.
    let msg = format!("{err:#}").to_lowercase();
    if msg.contains("password")
        || msg.contains("authentication")
        || msg.contains("access denied")
        // MSSQL bad credentials: `"Login failed for user 'sa'"`.
        || msg.contains("login failed")
        // Postgres top-level Display when the real cause (auth) is nested and
        // `{:#}` still collapses to the bare wrapper — a server-side `DbError`
        // is never a connectivity failure (those say "connect"/"refused"), so
        // mapping it to auth is the actionable bucket, not a misroute.
        || msg.contains("db error")
    {
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
    // `{:#}` walks the cause chain: opendal wraps the underlying
    // reqwest/HTTP reason as a `.source()`, so the alternate form is what
    // surfaces "error sending request" / "InvalidAccessKeyId" / "status: 403"
    // when those are nested rather than on the top Display.
    let msg = format!("{err:#}").to_lowercase();
    // CONTRACT: the pattern below must match the error text emitted by
    // `enforce_sas_expiry` in destination/azure.rs:
    //   "Azure SAS token already expired (se=…)"
    // If that message ever changes, update both places together.
    if msg.contains("already expired") && msg.contains("sas") {
        return "sas expired";
    }
    if msg.contains("credential")
        || msg.contains("permission denied")
        // opendal lowercases its `ErrorKind` with no space: `PermissionDenied`.
        || msg.contains("permissiondenied")
        || msg.contains("access denied")
        || msg.contains("unauthorized")
        || msg.contains("forbidden")
        || msg.contains("invalid_grant")
        || msg.contains("token")
        // S3 rejects a bad/expired key with `InvalidAccessKeyId` + HTTP 403.
        || msg.contains("invalidaccesskeyid")
        || msg.contains("403")
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
        // reqwest/opendal phrasing for a transport-level failure reaching the
        // endpoint at all (bad host, unreachable network, TLS reset) — none of
        // the needles above match the bare "error sending request for url …".
        || msg.contains("error sending request")
        || msg.contains("send http request")
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

#[cfg(test)]
mod tests {
    use super::*;

    // doctor-dedup-path (regression): doctor's destination dedup key must
    // include `path`, so two local destinations with different `path:` values
    // are each probed independently — a buggy key of type+bucket+endpoint
    // (omitting `path`) collapses both to "Local:-:-" and only probes the
    // first, letting an unwritable second directory pass doctor and fail at
    // run time. The dedup key now flows through `super::destination_identity`
    // (preflight/mod.rs), which includes `path`; this drives the whole
    // `doctor()` to prove the inline copy did not drift.
    //
    // The destination-side `.rivet_doctor_probe` is now cleaned up
    // (FINDING #26), so a leftover probe FILE can no longer serve as the
    // "was this path probed?" signal. Instead each destination uses a
    // *nested* path that does not exist yet: probing it forces
    // `LocalDestination::write` to `create_dir_all` the leaf directory, and
    // cleanup removes only the probe file — so the created (now empty) leaf
    // directory is a durable witness that the path was probed AND that #26
    // cleanup ran. A buggy dedup that skipped the second path would leave its
    // leaf directory absent.
    //
    // The source check fails fast and offline via `resolve_url` on an unset
    // `url_env`, and `doctor` continues to the destination loop regardless.
    #[test]
    fn roast_doctor_write_probes_each_distinct_local_destination_path() {
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let config_dir = tempfile::tempdir().unwrap();

        // Nested leaves that doctor must create when it probes each path.
        let leaf_a = dir_a.path().join("probe_here");
        let leaf_b = dir_b.path().join("probe_here");

        let yaml = format!(
            r#"
source:
  type: postgres
  url_env: RIVET_ROAST_DOCTOR_DEDUP_UNSET_URL_ENV
exports:
  - name: roast_dest_a
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: "{a}"
  - name: roast_dest_b
    query: "SELECT 1"
    format: csv
    destination:
      type: local
      path: "{b}"
"#,
            a = leaf_a.display(),
            b = leaf_b.display(),
        );
        let config_path = config_dir.path().join("rivet.yaml");
        std::fs::write(&config_path, yaml).unwrap();

        // Returns Err (source auth fails on the unset env var); the
        // destination probes are the observable under test.
        let _ = doctor(config_path.to_str().unwrap());

        let probe = crate::manifest::DOCTOR_PROBE_FILENAME;

        // Each distinct path was probed → its leaf directory now exists; and
        // FINDING #26 cleanup ran → the probe file inside it is gone, leaving
        // the leaf empty.
        for (label, leaf) in [("first", &leaf_a), ("second", &leaf_b)] {
            assert!(
                leaf.exists(),
                "doctor never write-probed the {label} local destination {} — its dedup key \
                 must include `path`; a key that omits it collapses both local destinations to \
                 one entry and only probes the first, so an unwritable second directory would \
                 pass doctor and fail at run time",
                leaf.display()
            );
            assert!(
                !leaf.join(probe).exists(),
                "doctor left its write-probe `{probe}` at the {label} destination {} \
                 (FINDING #26: it must remove the destination-side probe, not only the local temp)",
                leaf.display()
            );
            assert!(
                std::fs::read_dir(leaf).unwrap().next().is_none(),
                "doctor must leave the {label} destination {} exactly as it created it (empty)",
                leaf.display()
            );
        }
    }

    // Build a bare DestinationConfig of the given type for hint dispatch.
    fn dest_of(t: DestinationType) -> crate::config::DestinationConfig {
        crate::config::DestinationConfig {
            destination_type: t,
            ..Default::default()
        }
    }

    // AUDIT-RED doctor-categorizer: Postgres wrong-password Display is just "db error"
    // (real cause nested in .source()); current auth needles miss it so it falls to
    // generic "error" with no hint. Asserts CORRECT behavior; expected to FAIL until fixed.
    #[test]
    fn audit_pg_db_error_is_auth_with_hint() {
        let err = anyhow::anyhow!("db error");
        let cat = categorize_source_error(&err);
        assert_eq!(
            cat, "auth error",
            "Postgres wrong-password surfaces as 'db error'; categorizer returned {:?} instead of 'auth error'",
            cat
        );
        let hint = source_error_hint(cat, &err, &SourceType::Postgres);
        assert!(
            hint.is_some(),
            "no actionable hint produced for Postgres 'db error' (category {:?}); operator gets no next step",
            cat
        );
    }

    // AUDIT-RED doctor-categorizer (#1): MSSQL wrong-login Display is
    // "Login failed for user 'sa'" — current auth needles (password/authentication/
    // access denied) miss it, so it falls to generic "error" with no hint.
    // Asserts CORRECT behavior; expected to FAIL until fixed.
    #[test]
    fn audit_mssql_login_failed_is_auth_with_hint() {
        let err = anyhow::anyhow!("login failed for user 'sa'");
        let cat = categorize_source_error(&err);
        assert_eq!(
            cat, "auth error",
            "MSSQL bad login surfaces as 'Login failed for user ...'; categorizer returned {:?} instead of 'auth error'",
            cat
        );
        let hint = source_error_hint(cat, &err, &SourceType::Mssql);
        assert!(
            hint.is_some(),
            "no actionable hint produced for MSSQL 'login failed for user' (category {:?})",
            cat
        );
    }

    // AUDIT-RED doctor-categorizer: MySQL 'Access denied for user' already
    // categorizes as auth — guard so a fix for pg/mssql does not regress it.
    // Asserts CORRECT behavior; expected to PASS today.
    #[test]
    fn audit_mysql_access_denied_is_auth_with_hint() {
        let err = anyhow::anyhow!("access denied for user");
        let cat = categorize_source_error(&err);
        assert_eq!(
            cat, "auth error",
            "MySQL 'access denied for user' must stay auth; categorizer returned {:?}",
            cat
        );
        let hint = source_error_hint(cat, &err, &SourceType::Mysql);
        assert!(
            hint.is_some(),
            "no actionable hint produced for MySQL 'access denied for user' (category {:?})",
            cat
        );
    }

    // AUDIT-RED doctor-categorizer (#2): opendal S3 auth failure surfaces as
    // "PermissionDenied ... InvalidAccessKeyId ... status: 403" — lowercased
    // 'permissiondenied' has no space (never matches "permission denied"),
    // '403'/'invalidaccesskeyid' are absent from the needles, so it falls to
    // generic "error" with no hint. Asserts CORRECT behavior; expected to FAIL.
    #[test]
    fn audit_s3_permission_denied_403_is_auth_with_hint() {
        let dest = dest_of(DestinationType::S3);
        // NB: deliberately omits the words forbidden/unauthorized/credential/token
        // and the spaced "permission denied"/"access denied" — those would match the
        // existing needles and mask the real #2 gap (no-space PermissionDenied,
        // InvalidAccessKeyId, 403). This is exactly opendal's S3 auth wording.
        let err = anyhow::anyhow!(
            "PermissionDenied at write => InvalidAccessKeyId, status: 403, https://bucket.s3.amazonaws.com/probe"
        );
        let cat = categorize_dest_error(&err, &dest);
        assert_eq!(
            cat, "auth error",
            "S3 'PermissionDenied/InvalidAccessKeyId/403' must categorize as auth; categorizer returned {:?}",
            cat
        );
        let hint = destination_error_hint(cat, &dest);
        assert!(
            hint.is_some(),
            "no actionable hint produced for S3 auth failure (category {:?}); operator gets no next step",
            cat
        );
    }

    // AUDIT-RED doctor-categorizer (#13 Azure): opendal connectivity failure to
    // a bad/unreachable Azure endpoint surfaces as "error sending request for url
    // (https://x.blob.core.windows.net/...)" — none of connect/refused/timed out/
    // dns/endpoint match, so it falls to generic "error" with no hint.
    // Asserts CORRECT behavior; expected to FAIL until fixed.
    #[test]
    fn audit_azure_send_request_error_is_connectivity_with_hint() {
        let dest = dest_of(DestinationType::Azure);
        let err = anyhow::anyhow!(
            "error sending request for url (https://x.blob.core.windows.net/probe)"
        );
        let cat = categorize_dest_error(&err, &dest);
        assert_eq!(
            cat, "connectivity error",
            "Azure 'error sending request for url' must categorize as connectivity; categorizer returned {:?}",
            cat
        );
        let hint = destination_error_hint(cat, &dest);
        assert!(
            hint.is_some(),
            "no actionable hint produced for Azure connectivity failure (category {:?})",
            cat
        );
    }

    // AUDIT-RED doctor-categorizer (#27 guard): a literal "connection refused"
    // already categorizes as connectivity — guard so a fix for the opendal
    // send-request wording does not regress it. Asserts CORRECT behavior;
    // expected to PASS today.
    #[test]
    fn audit_dest_connection_refused_is_connectivity_with_hint() {
        let dest = dest_of(DestinationType::S3);
        let err = anyhow::anyhow!("connection refused");
        let cat = categorize_dest_error(&err, &dest);
        assert_eq!(
            cat, "connectivity error",
            "'connection refused' must stay connectivity; categorizer returned {:?}",
            cat
        );
        let hint = destination_error_hint(cat, &dest);
        assert!(
            hint.is_some(),
            "no actionable hint produced for 'connection refused' (category {:?})",
            cat
        );
    }

    // ── regression coverage for the broadened needles (fixes #1/#2) ──────────

    // The pg auth reason is nested in `.source()`; only `{:#}` surfaces it.
    // This proves the categorizer reads the alternate form, not just the bare
    // top-level "db error" Display — so a real run shows the true cause.
    #[test]
    fn source_pg_nested_password_cause_via_alternate_is_auth() {
        let root = anyhow::anyhow!("password authentication failed for user \"rivet\"");
        let wrapped = root.context("db error");
        assert_eq!(categorize_source_error(&wrapped), "auth error");
        // And the bare form (no nested cause) still maps via the "db error"
        // needle — this is the exact shape the audit test pins.
        assert_eq!(
            categorize_source_error(&anyhow::anyhow!("db error")),
            "auth error"
        );
    }

    // Guard: a genuine connectivity failure (Display says "connect"/"refused",
    // never "db error") must NOT be swallowed by the new "db error" auth
    // needle. Postgres surfaces a refused connection as "error connecting to
    // server", which carries no auth needle.
    #[test]
    fn source_connection_refused_stays_connectivity_not_auth() {
        let err = anyhow::anyhow!("error connecting to server: Connection refused (os error 61)");
        assert_eq!(categorize_source_error(&err), "connectivity error");
    }

    // opendal lowercases its `PermissionDenied` ErrorKind with no space — the
    // pre-fix "permission denied" needle never matched it. Standalone case so
    // coverage can't shrink back to the spaced-only needle.
    #[test]
    fn dest_no_space_permissiondenied_is_auth() {
        let dest = dest_of(DestinationType::Gcs);
        let err = anyhow::anyhow!("PermissionDenied (persistent) at write");
        assert_eq!(categorize_dest_error(&err, &dest), "auth error");
        assert!(destination_error_hint("auth error", &dest).is_some());
    }

    // reqwest's other transport phrasing — "failed to send http request" —
    // must also categorize as connectivity (the audit test pins the sibling
    // "error sending request for url" wording).
    #[test]
    fn dest_send_http_request_is_connectivity() {
        let dest = dest_of(DestinationType::S3);
        // No connect/refused/timed out/dns/endpoint substring — only the new
        // "send http request" needle can route this to connectivity.
        let err = anyhow::anyhow!("failed to send http request to the store");
        assert_eq!(categorize_dest_error(&err, &dest), "connectivity error");
    }

    // Guard: an HTTP 404 (object/bucket missing) must stay "bucket not found",
    // not get pulled into auth by the new 403 needle.
    #[test]
    fn dest_404_stays_bucket_not_found_after_403_needle_added() {
        let dest = dest_of(DestinationType::S3);
        let err = anyhow::anyhow!("NoSuchBucket, status: 404");
        assert_eq!(categorize_dest_error(&err, &dest), "bucket not found");
    }

    // FINDING #26 (unit-level mirror of the live test): the destination-side
    // probe must be removed, leaving the prefix exactly as doctor found it.
    // Exercises the cleanup directly (no shared system-temp path) so it is
    // immune to parallel-test contention on `std::env::temp_dir()`.
    #[test]
    fn remove_destination_probe_local_deletes_the_probe_object() {
        let dir = tempfile::tempdir().unwrap();
        let probe_key = crate::manifest::DOCTOR_PROBE_FILENAME;
        // Stand in for what `LocalDestination::write` lands at the prefix.
        std::fs::write(dir.path().join(probe_key), b"ok").unwrap();
        let dest = crate::config::DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some(dir.path().to_string_lossy().into_owned()),
            ..Default::default()
        };
        remove_destination_probe(&dest, probe_key);
        assert!(
            std::fs::read_dir(dir.path()).unwrap().next().is_none(),
            "destination prefix must be left exactly as doctor found it (empty)"
        );
    }

    // `remove_destination_probe` resolves the prefix from `prefix` when `path`
    // is unset (mirrors `LocalDestination::new`'s `path → prefix → "."` order).
    #[test]
    fn remove_destination_probe_local_uses_prefix_when_path_unset() {
        let dir = tempfile::tempdir().unwrap();
        let probe_key = crate::manifest::DOCTOR_PROBE_FILENAME;
        std::fs::write(dir.path().join(probe_key), b"ok").unwrap();
        let dest = crate::config::DestinationConfig {
            destination_type: DestinationType::Local,
            prefix: Some(dir.path().to_string_lossy().into_owned()),
            ..Default::default()
        };
        remove_destination_probe(&dest, probe_key);
        assert!(
            !dir.path().join(probe_key).exists(),
            "cleanup must follow the same base-path resolution as the writer"
        );
    }

    // Benign no-op when the probe is already absent — cleanup runs after the
    // write check passed, so it must never turn a healthy run into a failure.
    #[test]
    fn remove_destination_probe_missing_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let dest = crate::config::DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some(dir.path().to_string_lossy().into_owned()),
            ..Default::default()
        };
        // No probe present; must not panic and must leave the dir untouched.
        remove_destination_probe(&dest, crate::manifest::DOCTOR_PROBE_FILENAME);
        assert!(std::fs::read_dir(dir.path()).unwrap().next().is_none());
    }
}
