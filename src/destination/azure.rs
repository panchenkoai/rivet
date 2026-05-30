use chrono::{DateTime, Utc};
use opendal::Operator;
use opendal::services::Azblob;

use super::cloud::{CloudBackend, CloudDestination};
use crate::config::DestinationConfig;
use crate::error::Result;

/// Threshold under which a SAS token is considered "near expiry" and a
/// warning is logged.  A long export that finishes in < `WARN_THRESHOLD`
/// will *probably* succeed, but the operator should know.  60 minutes is
/// a deliberate compromise:
///
/// - Long enough to cover a typical large chunked export.
/// - Short enough that drive-by token paste (e.g. a portal-generated SAS
///   with default 1-hour validity) is flagged before it expires
///   mid-flight.
///
/// The 0.7.4 release-gate matrix in `docs/cloud-smoke-tests.md` records
/// "near-expiry warn at 60 min" as the verified contract.
const SAS_NEAR_EXPIRY_THRESHOLD: chrono::Duration = chrono::Duration::minutes(60);

/// Result of parsing `se=` (signed-expiry) out of an Azure SAS token.
///
/// This is the input to the preflight in `AzureDestination::new`: the
/// destination either rejects the token (expired), warns and continues
/// (near-expiry / unparseable), or stays silent (healthy / token has no
/// `se=` at all).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SasExpiryStatus {
    /// `se=` parsed and is in the future by more than `SAS_NEAR_EXPIRY_THRESHOLD`.
    Healthy { expires_at: DateTime<Utc> },
    /// `se=` parsed and is within `SAS_NEAR_EXPIRY_THRESHOLD` of `now` (still in the future).
    NearExpiry {
        expires_at: DateTime<Utc>,
        remaining: chrono::Duration,
    },
    /// `se=` parsed and is in the past — the destination cannot be opened.
    Expired { expires_at: DateTime<Utc> },
    /// `se=` is missing (legacy SAS signed by a stored access policy, or
    /// a non-standard token shape).  Cannot warn meaningfully.
    NoExpiry,
    /// `se=` is present but the value did not parse as RFC3339.
    Unparseable { raw: String },
}

/// Parse `se=` (signed-expiry) out of a SAS token and classify it.
///
/// SAS tokens are URL-encoded query strings: `sv=…&se=2026-06-01T00:00:00Z&sig=…`.
/// The `se=` value is RFC3339 with `Z` suffix (UTC).  A leading `?` is
/// tolerated — `AzureDestination::new` strips it before delegating to
/// opendal, but we want this preflight to be safe to call on raw input.
///
/// Returns `NoExpiry` rather than an error when `se=` is missing — Azure
/// permits SAS tokens whose lifetime is governed by a server-side stored
/// access policy and Rivet has no opinion about those.
pub(crate) fn parse_sas_expiry_status(token: &str, now: DateTime<Utc>) -> SasExpiryStatus {
    // Trim the leading `?` operators sometimes copy from the portal URL.
    let body = token.trim_start_matches('?');

    for pair in body.split('&') {
        let mut kv = pair.splitn(2, '=');
        let Some(key) = kv.next() else { continue };
        if key != "se" {
            continue;
        }
        let Some(raw) = kv.next() else {
            return SasExpiryStatus::Unparseable { raw: String::new() };
        };

        // The portal-generated form is `2026-06-01T00:00:00Z` — already
        // RFC3339.  Some CLIs URL-encode colons (`%3A`) or the `+` sign in
        // timezone offsets (`%2B`); decode both before parsing.
        let decoded = raw
            .replace("%3A", ":")
            .replace("%3a", ":")
            .replace("%2B", "+")
            .replace("%2b", "+");

        match DateTime::parse_from_rfc3339(&decoded) {
            Ok(dt) => {
                let expires_at = dt.with_timezone(&Utc);
                if expires_at <= now {
                    return SasExpiryStatus::Expired { expires_at };
                }
                let remaining = expires_at - now;
                if remaining <= SAS_NEAR_EXPIRY_THRESHOLD {
                    return SasExpiryStatus::NearExpiry {
                        expires_at,
                        remaining,
                    };
                }
                return SasExpiryStatus::Healthy { expires_at };
            }
            Err(_) => {
                return SasExpiryStatus::Unparseable {
                    raw: decoded.to_string(),
                };
            }
        }
    }

    SasExpiryStatus::NoExpiry
}

/// Inspect a SAS token's `se=` and act on it: fail-fast on expired,
/// log::warn on near-expiry / unparseable, stay silent otherwise.
///
/// Centralised so `AzureDestination::new` and any future direct caller
/// (e.g. a JSON-format `rivet doctor`) share one decision.
fn enforce_sas_expiry(token: &str) -> Result<()> {
    match parse_sas_expiry_status(token, Utc::now()) {
        SasExpiryStatus::Healthy { .. } | SasExpiryStatus::NoExpiry => Ok(()),
        SasExpiryStatus::NearExpiry {
            expires_at,
            remaining,
        } => {
            // Total minutes is the operator-friendly unit; sub-minute
            // precision is noise (clock skew dominates anyway).
            let mins = remaining.num_minutes().max(0);
            log::warn!(
                "Azure SAS token expires in {} minute{} ({}). Long exports may fail mid-run; rotate the token before extraction.",
                mins,
                if mins == 1 { "" } else { "s" },
                expires_at.to_rfc3339()
            );
            Ok(())
        }
        SasExpiryStatus::Expired { expires_at } => {
            // CONTRACT: preflight/doctor.rs::categorize_dest_error matches on
            // "already expired" + "sas" to assign the "sas expired" category.
            // Keep those words present if this message is ever rephrased.
            anyhow::bail!(
                "Azure SAS token already expired (se={}). Generate a new SAS and re-export.",
                expires_at.to_rfc3339()
            )
        }
        SasExpiryStatus::Unparseable { raw } => {
            log::warn!(
                "Azure SAS token has unparseable 'se=' value ({:?}); skipping expiry check. The token may still authenticate, but Rivet cannot warn on near-expiry.",
                raw
            );
            Ok(())
        }
    }
}

/// Azure Blob Storage destination. The retry policy, blocking wrap, and
/// ADR-0013 read surface live in [`CloudDestination`]; this type only knows
/// how to authenticate against Azure (account key, SAS token, or anonymous).
pub type AzureDestination = CloudDestination<AzureBackend>;

/// Zero-sized backend marker carrying Azure's operator construction.
pub struct AzureBackend;

/// Read a credential from an env var into a `Zeroizing<String>`.
///
/// SecOps: same treatment as `S3Destination::read_credential_env` — the
/// underlying heap buffer is zeroed on drop. OpenDAL stores its own copy
/// inside reqsign; this wrapper only protects our transient handle.
fn read_credential_env(env_name: &str, label: &str) -> Result<zeroize::Zeroizing<String>> {
    let value = std::env::var(env_name)
        .map_err(|_| anyhow::anyhow!("env var '{}' not set for Azure {}", env_name, label))?;
    Ok(zeroize::Zeroizing::new(value))
}

impl CloudBackend for AzureBackend {
    const RUNTIME_LABEL: &'static str = "Azure";
    const SCHEME: &'static str = "az";

    fn build_operator(config: &DestinationConfig) -> Result<Operator> {
        // Azure's "container" is the equivalent of S3's bucket — reuse the
        // existing `bucket` config field rather than introducing a second
        // synonym at the YAML layer.
        let container = config.bucket.as_deref().ok_or_else(|| {
            anyhow::anyhow!("Azure destination requires 'bucket' (container name)")
        })?;

        let mut builder = Azblob::default().container(container);

        // opendal's Azblob builder does NOT auto-derive an endpoint from
        // account_name the way AWS does from region.  We do it ourselves so
        // operators only have to set `account_name`; explicit `endpoint:` in
        // config still wins (needed for Azurite, sovereign clouds, custom
        // domains).
        match (&config.endpoint, &config.account_name) {
            (Some(endpoint), _) => {
                builder = builder.endpoint(endpoint);
            }
            (None, Some(account_name)) => {
                let derived = format!("https://{account_name}.blob.core.windows.net");
                builder = builder.endpoint(&derived);
            }
            (None, None) => {
                // Both unset.  We bail in the auth check below with a clearer
                // message that names the missing field (account_name), since
                // an "endpoint is empty" error from opendal hides the root
                // cause from the operator.
            }
        }

        if config.allow_anonymous {
            // Azurite emulator and public read-only containers.  Skip every
            // auth path — opendal will still talk to the endpoint but won't
            // attach `Authorization`.
            log::info!("Azure: allow_anonymous (Azurite emulator or public container)");
        } else {
            let account_name = config.account_name.as_deref().ok_or_else(|| {
                anyhow::anyhow!(
                    "Azure destination requires 'account_name' (or 'allow_anonymous: true' for Azurite)"
                )
            })?;
            builder = builder.account_name(account_name);

            // v0.7.2 P0.4: account_key_env XOR sas_token_env.  Both being
            // set means the operator copy-pasted two auth blocks and we
            // can't safely pick one — fail loud.
            match (&config.account_key_env, &config.sas_token_env) {
                (Some(_), Some(_)) => anyhow::bail!(
                    "Azure destination: 'account_key_env' and 'sas_token_env' are mutually exclusive — pick one auth mode"
                ),
                (Some(env_name), None) => {
                    let key = read_credential_env(env_name, "account key")?;
                    builder = builder.account_key(key.as_str());
                }
                (None, Some(env_name)) => {
                    let raw = read_credential_env(env_name, "SAS token")?;
                    // Operators copy the full `?sv=…&sig=…` URL fragment from
                    // the portal — trim the leading `?` so opendal's
                    // signer sees the raw token body.  Defensive: opendal
                    // currently tolerates the `?`, but a future version
                    // may not.
                    let token = raw.trim_start_matches('?');
                    // Preflight `se=` (v0.7.4): fail fast on already-expired
                    // tokens, warn on near-expiry, stay silent otherwise.
                    // This runs before opendal touches the wire so a stale
                    // SAS produces a Rivet-shaped error message instead of
                    // an opaque 403 on the first PUT.
                    enforce_sas_expiry(token)?;
                    builder = builder.sas_token(token);
                }
                (None, None) => anyhow::bail!(
                    "Azure destination requires 'account_key_env' or 'sas_token_env' (or 'allow_anonymous: true' for Azurite)"
                ),
            }
        }

        Ok(Operator::new(builder)?.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DestinationConfig, DestinationType};

    // ── config validation (no network, no real credentials) ───────────────────

    /// Helper: run `AzureDestination::new` expecting an error.
    ///
    /// `AzureDestination` doesn't impl `Debug` (opendal `blocking::Operator`
    /// doesn't), so the usual `.unwrap_err()` shortcut won't type-check.
    fn expect_err(cfg: &DestinationConfig) -> String {
        match AzureDestination::new(cfg) {
            Err(e) => format!("{e:#}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn azure_destination_requires_bucket() {
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            account_name: Some("acct".into()),
            account_key_env: Some("AZURE_TEST_KEY_X".into()),
            ..Default::default()
        };
        // SAFETY: test-only; binary is single-threaded in this test context.
        unsafe { std::env::set_var("AZURE_TEST_KEY_X", "fake") };
        let msg = expect_err(&cfg);
        assert!(
            msg.contains("bucket") || msg.contains("container"),
            "missing bucket should be reported: {msg}"
        );
        unsafe { std::env::remove_var("AZURE_TEST_KEY_X") };
    }

    #[test]
    fn azure_destination_requires_account_name_when_not_anonymous() {
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("data".into()),
            account_key_env: Some("AZURE_TEST_KEY_Y".into()),
            ..Default::default()
        };
        unsafe { std::env::set_var("AZURE_TEST_KEY_Y", "fake") };
        let msg = expect_err(&cfg);
        assert!(
            msg.contains("account_name"),
            "missing account_name should be reported: {msg}"
        );
        unsafe { std::env::remove_var("AZURE_TEST_KEY_Y") };
    }

    #[test]
    fn azure_destination_requires_account_key_env_when_not_anonymous() {
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("data".into()),
            account_name: Some("acct".into()),
            ..Default::default()
        };
        let msg = expect_err(&cfg);
        assert!(
            msg.contains("account_key_env"),
            "missing account_key_env should be reported: {msg}"
        );
    }

    #[test]
    fn azure_destination_account_key_env_missing_var_errors() {
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("data".into()),
            account_name: Some("acct".into()),
            account_key_env: Some("RIVET_TEST_AZURE_KEY_UNSET_Z".into()),
            ..Default::default()
        };
        unsafe { std::env::remove_var("RIVET_TEST_AZURE_KEY_UNSET_Z") };
        let msg = expect_err(&cfg);
        assert!(
            msg.contains("RIVET_TEST_AZURE_KEY_UNSET_Z"),
            "missing env var name in error: {msg}"
        );
        assert!(
            msg.contains("account key"),
            "missing credential label in error: {msg}"
        );
    }

    #[test]
    fn azure_destination_allow_anonymous_skips_credential_requirements() {
        // Azurite-mode build should succeed without account_name / account_key.
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("data".into()),
            endpoint: Some("http://127.0.0.1:10000/devstoreaccount1".into()),
            allow_anonymous: true,
            ..Default::default()
        };
        // The Operator construction may still fail (no live emulator), but it
        // must NOT fail with "account_name required" / "account_key_env required".
        // We only assert the negative — that we got past the pre-flight check.
        let err = AzureDestination::new(&cfg).err().map(|e| format!("{e:#}"));
        if let Some(msg) = err {
            assert!(
                !msg.contains("account_name") && !msg.contains("account_key_env"),
                "allow_anonymous must bypass credential checks, got: {msg}"
            );
        }
    }

    #[test]
    fn azure_destination_auto_derives_endpoint_from_account_name() {
        // Regression test for the live-prod bug caught on 2026-05-21:
        // opendal's `Azblob` builder does NOT auto-derive an endpoint from
        // `account_name` the way AWS S3 does from `region`.  Without our
        // own derive step, `doctor` fails immediately with
        // `ConfigInvalid: endpoint is empty` and never reaches the
        // credential check.
        //
        // This test asserts: when `endpoint` is unset but `account_name` is
        // set, builder construction reaches *past* the endpoint validation.
        // We can't easily assert "endpoint was set to X" without poking at
        // opendal internals, so we assert the NEGATIVE — the error message,
        // if any, must NOT be the "endpoint is empty" failure mode.
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("data".into()),
            account_name: Some("mystorageacct".into()),
            account_key_env: Some("RIVET_TEST_AZURE_ENDPOINT_FAKE".into()),
            ..Default::default()
        };
        // SAFETY: test-only; binary is single-threaded in this test context.
        unsafe { std::env::set_var("RIVET_TEST_AZURE_ENDPOINT_FAKE", "fake-key-value") };
        let result = AzureDestination::new(&cfg);
        // The build may succeed (opendal doesn't validate the fake key
        // until first call) or fail for some other reason — but NEVER
        // for an empty endpoint, because we derive it from account_name.
        if let Err(e) = result {
            let msg = format!("{e:#}").to_lowercase();
            assert!(
                !msg.contains("endpoint is empty"),
                "auto-derive must populate endpoint from account_name; got: {msg}"
            );
        }
        unsafe { std::env::remove_var("RIVET_TEST_AZURE_ENDPOINT_FAKE") };
    }

    #[test]
    fn azure_destination_explicit_endpoint_wins_over_derived() {
        // Explicit `endpoint:` in config must take precedence over the
        // auto-derived `https://<account>.blob.core.windows.net` URL.
        // Critical for Azurite, sovereign clouds, and custom storage
        // accounts behind a corporate proxy.
        //
        // We can't read back the endpoint from opendal's builder, so we
        // verify the *path* taken: with both set, no failure is raised
        // before the credential check (i.e., we got past the endpoint
        // selection logic without panic).  The actual endpoint string
        // is exercised by the live smoke against Azurite.
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("data".into()),
            endpoint: Some("http://127.0.0.1:10000/devstoreaccount1".into()),
            account_name: Some("devstoreaccount1".into()),
            account_key_env: Some("RIVET_TEST_AZURE_EXPLICIT_EP_FAKE".into()),
            ..Default::default()
        };
        unsafe { std::env::set_var("RIVET_TEST_AZURE_EXPLICIT_EP_FAKE", "fake") };
        let result = AzureDestination::new(&cfg);
        if let Err(e) = result {
            let msg = format!("{e:#}").to_lowercase();
            assert!(
                !msg.contains("endpoint is empty"),
                "explicit endpoint must reach builder; got: {msg}"
            );
        }
        unsafe { std::env::remove_var("RIVET_TEST_AZURE_EXPLICIT_EP_FAKE") };
    }

    #[test]
    fn azure_destination_config_parses_from_yaml() {
        let yaml = r#"
type: azure
bucket: my-container
account_name: mystorageacct
account_key_env: AZURE_STORAGE_KEY
"#;
        let cfg: DestinationConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(cfg.destination_type, DestinationType::Azure);
        assert_eq!(cfg.bucket.as_deref(), Some("my-container"));
        assert_eq!(cfg.account_name.as_deref(), Some("mystorageacct"));
        assert_eq!(cfg.account_key_env.as_deref(), Some("AZURE_STORAGE_KEY"));
    }

    // ── v0.7.2 P0.4: SAS-token auth ───────────────────────────────────────

    #[test]
    fn azure_destination_sas_token_env_satisfies_auth() {
        // With `sas_token_env` set (and `account_key_env` unset), the
        // build path must reach *past* the credential validation — i.e.
        // no "account_key_env required" error.
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("data".into()),
            account_name: Some("mystorageacct".into()),
            sas_token_env: Some("RIVET_TEST_AZURE_SAS_OK".into()),
            ..Default::default()
        };
        // SAFETY: test-only; binary is single-threaded in this test context.
        unsafe { std::env::set_var("RIVET_TEST_AZURE_SAS_OK", "sv=2021-08-06&sig=fake") };
        let result = AzureDestination::new(&cfg);
        if let Err(e) = result {
            let msg = format!("{e:#}");
            assert!(
                !msg.contains("account_key_env"),
                "SAS-token auth must bypass account_key_env requirement; got: {msg}"
            );
        }
        unsafe { std::env::remove_var("RIVET_TEST_AZURE_SAS_OK") };
    }

    #[test]
    fn azure_destination_rejects_account_key_and_sas_token_together() {
        // Both auth modes set → can't safely pick one.  Fail with an
        // actionable message that names both fields.
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("data".into()),
            account_name: Some("acct".into()),
            account_key_env: Some("RIVET_TEST_AZURE_BOTH_KEY".into()),
            sas_token_env: Some("RIVET_TEST_AZURE_BOTH_SAS".into()),
            ..Default::default()
        };
        unsafe { std::env::set_var("RIVET_TEST_AZURE_BOTH_KEY", "fake") };
        unsafe { std::env::set_var("RIVET_TEST_AZURE_BOTH_SAS", "fake") };
        let msg = expect_err(&cfg);
        assert!(
            msg.contains("account_key_env") && msg.contains("sas_token_env"),
            "error must name both fields: {msg}"
        );
        assert!(
            msg.contains("mutually exclusive"),
            "error must say mutually exclusive: {msg}"
        );
        unsafe { std::env::remove_var("RIVET_TEST_AZURE_BOTH_KEY") };
        unsafe { std::env::remove_var("RIVET_TEST_AZURE_BOTH_SAS") };
    }

    #[test]
    fn azure_destination_missing_both_account_key_and_sas_token_errors() {
        // Updated error message must name BOTH auth modes so the
        // operator knows the alternatives.
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("data".into()),
            account_name: Some("acct".into()),
            ..Default::default()
        };
        let msg = expect_err(&cfg);
        assert!(
            msg.contains("account_key_env") && msg.contains("sas_token_env"),
            "error must name both auth modes: {msg}"
        );
    }

    #[test]
    fn azure_destination_sas_token_env_missing_var_errors_with_label() {
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("data".into()),
            account_name: Some("acct".into()),
            sas_token_env: Some("RIVET_TEST_AZURE_SAS_UNSET".into()),
            ..Default::default()
        };
        unsafe { std::env::remove_var("RIVET_TEST_AZURE_SAS_UNSET") };
        let msg = expect_err(&cfg);
        assert!(
            msg.contains("RIVET_TEST_AZURE_SAS_UNSET"),
            "missing env var name in error: {msg}"
        );
        assert!(
            msg.contains("SAS token"),
            "credential label must mention SAS token: {msg}"
        );
    }

    #[test]
    fn azure_destination_sas_token_config_parses_from_yaml() {
        let yaml = r#"
type: azure
bucket: my-container
account_name: mystorageacct
sas_token_env: AZURE_STORAGE_SAS_TOKEN
"#;
        let cfg: DestinationConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(cfg.destination_type, DestinationType::Azure);
        assert_eq!(cfg.bucket.as_deref(), Some("my-container"));
        assert_eq!(cfg.account_name.as_deref(), Some("mystorageacct"));
        assert_eq!(
            cfg.sas_token_env.as_deref(),
            Some("AZURE_STORAGE_SAS_TOKEN")
        );
        assert!(cfg.account_key_env.is_none());
    }

    // ── v0.7.4 P1: SAS-token expiry preflight ─────────────────────────────

    use chrono::TimeZone;

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn sas_expiry_parses_far_future_as_healthy() {
        let token = "sv=2021-08-06&se=2099-01-01T00:00:00Z&sig=fake";
        let now = Utc.with_ymd_and_hms(2026, 5, 23, 12, 0, 0).unwrap();
        match parse_sas_expiry_status(token, now) {
            SasExpiryStatus::Healthy { expires_at } => {
                assert_eq!(expires_at, ts("2099-01-01T00:00:00Z"));
            }
            other => panic!("expected Healthy, got {other:?}"),
        }
    }

    #[test]
    fn sas_expiry_parses_within_threshold_as_near_expiry() {
        // 30 minutes < 60-minute threshold → NearExpiry.
        let token = "sv=2021-08-06&se=2026-05-23T12:30:00Z&sig=fake";
        let now = Utc.with_ymd_and_hms(2026, 5, 23, 12, 0, 0).unwrap();
        match parse_sas_expiry_status(token, now) {
            SasExpiryStatus::NearExpiry {
                expires_at,
                remaining,
            } => {
                assert_eq!(expires_at, ts("2026-05-23T12:30:00Z"));
                assert_eq!(remaining.num_minutes(), 30);
            }
            other => panic!("expected NearExpiry, got {other:?}"),
        }
    }

    #[test]
    fn sas_expiry_parses_past_as_expired() {
        let token = "sv=2021-08-06&se=2024-01-01T00:00:00Z&sig=fake";
        let now = Utc.with_ymd_and_hms(2026, 5, 23, 12, 0, 0).unwrap();
        match parse_sas_expiry_status(token, now) {
            SasExpiryStatus::Expired { expires_at } => {
                assert_eq!(expires_at, ts("2024-01-01T00:00:00Z"));
            }
            other => panic!("expected Expired, got {other:?}"),
        }
    }

    #[test]
    fn sas_expiry_treats_now_equals_se_as_expired() {
        // Boundary: a token whose `se=` exactly equals `now` cannot be
        // used for any subsequent request, so it must be Expired.
        let token = "sv=2021-08-06&se=2026-05-23T12:00:00Z&sig=fake";
        let now = Utc.with_ymd_and_hms(2026, 5, 23, 12, 0, 0).unwrap();
        match parse_sas_expiry_status(token, now) {
            SasExpiryStatus::Expired { .. } => {}
            other => panic!("expected Expired, got {other:?}"),
        }
    }

    #[test]
    fn sas_expiry_handles_url_encoded_colons() {
        // `az` CLI sometimes emits `%3A` for `:`.  Both forms must
        // produce the same status.
        let token = "sv=2021-08-06&se=2099-01-01T00%3A00%3A00Z&sig=fake";
        let now = Utc.with_ymd_and_hms(2026, 5, 23, 12, 0, 0).unwrap();
        assert!(matches!(
            parse_sas_expiry_status(token, now),
            SasExpiryStatus::Healthy { .. }
        ));
    }

    #[test]
    fn sas_expiry_handles_url_encoded_plus_in_timezone_offset() {
        // Some tooling emits `+00:00` as `%2B00:00` in the se= value.
        // An expired token with this encoding must still be caught.
        let token = "sv=2021-08-06&se=2024-01-01T00%3A00%3A00%2B00%3A00&sig=fake";
        let now = Utc.with_ymd_and_hms(2026, 5, 23, 12, 0, 0).unwrap();
        assert!(matches!(
            parse_sas_expiry_status(token, now),
            SasExpiryStatus::Expired { .. }
        ));
    }

    #[test]
    fn sas_expiry_returns_no_expiry_when_se_missing() {
        // Tokens signed by a stored access policy have no `se=` of
        // their own — the policy on the server enforces lifetime.
        let token = "sv=2021-08-06&si=mypolicy&sig=fake";
        let now = Utc.with_ymd_and_hms(2026, 5, 23, 12, 0, 0).unwrap();
        assert_eq!(
            parse_sas_expiry_status(token, now),
            SasExpiryStatus::NoExpiry
        );
    }

    #[test]
    fn sas_expiry_returns_unparseable_for_bad_format() {
        let token = "sv=2021-08-06&se=NOT_A_DATE&sig=fake";
        let now = Utc.with_ymd_and_hms(2026, 5, 23, 12, 0, 0).unwrap();
        match parse_sas_expiry_status(token, now) {
            SasExpiryStatus::Unparseable { raw } => assert_eq!(raw, "NOT_A_DATE"),
            other => panic!("expected Unparseable, got {other:?}"),
        }
    }

    #[test]
    fn sas_expiry_tolerates_leading_question_mark() {
        // Operators paste `?sv=…&se=…&sig=…` straight from the portal.
        let token = "?sv=2021-08-06&se=2099-01-01T00:00:00Z&sig=fake";
        let now = Utc.with_ymd_and_hms(2026, 5, 23, 12, 0, 0).unwrap();
        assert!(matches!(
            parse_sas_expiry_status(token, now),
            SasExpiryStatus::Healthy { .. }
        ));
    }

    #[test]
    fn sas_expiry_at_threshold_is_near_expiry() {
        // Boundary: a token expiring exactly at the threshold (60 min)
        // must trigger NearExpiry — NearExpiry uses `<=`.
        let now = Utc.with_ymd_and_hms(2026, 5, 23, 12, 0, 0).unwrap();
        let exactly_at_threshold = now + SAS_NEAR_EXPIRY_THRESHOLD;
        let token = format!(
            "sv=2021-08-06&se={}&sig=fake",
            exactly_at_threshold.to_rfc3339()
        );
        assert!(matches!(
            parse_sas_expiry_status(&token, now),
            SasExpiryStatus::NearExpiry { .. }
        ));
    }

    #[test]
    fn sas_expiry_one_second_above_threshold_is_healthy() {
        // One second above the 60-minute threshold must be Healthy.
        let now = Utc.with_ymd_and_hms(2026, 5, 23, 12, 0, 0).unwrap();
        let above_threshold = now + SAS_NEAR_EXPIRY_THRESHOLD + chrono::Duration::seconds(1);
        let token = format!("sv=2021-08-06&se={}&sig=fake", above_threshold.to_rfc3339());
        assert!(matches!(
            parse_sas_expiry_status(&token, now),
            SasExpiryStatus::Healthy { .. }
        ));
    }

    #[test]
    fn azure_destination_rejects_expired_sas_token() {
        // Wire test: a token whose `se=` is in the past must fail fast
        // in `AzureDestination::new`, *before* opendal touches the wire.
        // The error message must name the field so the operator knows
        // what to rotate.
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("data".into()),
            account_name: Some("mystorageacct".into()),
            sas_token_env: Some("RIVET_TEST_AZURE_SAS_EXPIRED".into()),
            ..Default::default()
        };
        unsafe {
            std::env::set_var(
                "RIVET_TEST_AZURE_SAS_EXPIRED",
                "sv=2021-08-06&se=2024-01-01T00:00:00Z&sig=fake",
            )
        };
        let msg = expect_err(&cfg);
        assert!(
            msg.contains("expired") || msg.contains("Expired"),
            "expired SAS must surface as expiry error: {msg}"
        );
        unsafe { std::env::remove_var("RIVET_TEST_AZURE_SAS_EXPIRED") };
    }

    #[test]
    fn azure_destination_accepts_far_future_sas_token() {
        // Healthy path: a token whose `se=` is far in the future must
        // *not* fail at the expiry preflight.  We can't assert the
        // build succeeds (no live Azure), but we can assert the error
        // — if any — is *not* the expiry one.
        let cfg = DestinationConfig {
            destination_type: DestinationType::Azure,
            bucket: Some("data".into()),
            account_name: Some("mystorageacct".into()),
            sas_token_env: Some("RIVET_TEST_AZURE_SAS_HEALTHY".into()),
            ..Default::default()
        };
        unsafe {
            std::env::set_var(
                "RIVET_TEST_AZURE_SAS_HEALTHY",
                "sv=2021-08-06&se=2099-01-01T00:00:00Z&sig=fake",
            )
        };
        if let Err(e) = AzureDestination::new(&cfg) {
            let msg = format!("{e:#}");
            assert!(
                !msg.contains("expired") && !msg.contains("Expired"),
                "healthy SAS must pass expiry preflight; got: {msg}"
            );
        }
        unsafe { std::env::remove_var("RIVET_TEST_AZURE_SAS_HEALTHY") };
    }
}
