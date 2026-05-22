use std::path::Path;
use std::sync::Arc;

use opendal::Operator;
use opendal::blocking;
use opendal::layers::RetryLayer;
use opendal::services::Azblob;

use crate::config::DestinationConfig;
use crate::error::Result;

pub struct AzureDestination {
    _runtime: Arc<tokio::runtime::Runtime>,
    op: blocking::Operator,
    prefix: String,
}

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

impl AzureDestination {
    pub fn new(config: &DestinationConfig) -> Result<Self> {
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
                    builder = builder.sas_token(token);
                }
                (None, None) => anyhow::bail!(
                    "Azure destination requires 'account_key_env' or 'sas_token_env' (or 'allow_anonymous: true' for Azurite)"
                ),
            }
        }

        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| anyhow::anyhow!("failed to create tokio runtime for Azure: {}", e))?,
        );
        let _guard = runtime.enter();

        // Same RetryLayer as S3/GCS — opendal absorbs transient hyper/reqwest
        // failures (5xx, 429, TCP blips) so we don't escalate to a chunk-level
        // re-fetch.  See gcs.rs for the rationale.
        let async_op = Operator::new(builder)?
            .layer(
                RetryLayer::new()
                    .with_max_times(5)
                    .with_min_delay(std::time::Duration::from_millis(200))
                    .with_max_delay(std::time::Duration::from_secs(10))
                    .with_jitter(),
            )
            .finish();
        let op = blocking::Operator::new(async_op)?;

        let prefix = config.prefix.clone().unwrap_or_default();

        Ok(Self {
            _runtime: runtime,
            op,
            prefix,
        })
    }
}

impl super::Destination for AzureDestination {
    fn write(&self, local_path: &Path, remote_key: &str) -> Result<()> {
        let key = format!("{}{}", self.prefix, remote_key);
        let mut src = std::fs::File::open(local_path)?;
        let mut dst = self.op.writer(&key)?.into_std_write();
        std::io::copy(&mut src, &mut dst)?;
        dst.close()?;
        log::info!("uploaded az://{}", key);
        Ok(())
    }

    fn capabilities(&self) -> super::DestinationCapabilities {
        super::DestinationCapabilities {
            commit_protocol: super::WriteCommitProtocol::FinalizeOnClose,
            idempotent_overwrite: true,
            retry_safe: true,
            partial_write_risk: false,
        }
    }

    // ── ADR-0013 read surface (delegates to opendal) ─────────────────────
    //
    // Identical shape to S3/GCS; opendal abstracts the backend-specific
    // listing semantics.  Trailing slash required for directory listings.

    fn list_prefix(&self, prefix: &str) -> Result<Vec<super::ObjectMeta>> {
        let full = format!("{}{}", self.prefix, prefix);
        let listed = if full.is_empty() || full.ends_with('/') {
            self.op.list_options(
                &full,
                opendal::options::ListOptions {
                    recursive: true,
                    ..Default::default()
                },
            )?
        } else {
            self.op.list_options(
                &format!("{}/", full),
                opendal::options::ListOptions {
                    recursive: true,
                    ..Default::default()
                },
            )?
        };
        let mut out = Vec::with_capacity(listed.len());
        for entry in listed {
            if entry.metadata().mode() != opendal::EntryMode::FILE {
                continue;
            }
            let abs = entry.path().to_string();
            let rel = abs
                .strip_prefix(self.prefix.as_str())
                .unwrap_or(abs.as_str())
                .to_string();
            out.push(super::ObjectMeta {
                key: rel,
                size_bytes: entry.metadata().content_length(),
            });
        }
        Ok(out)
    }

    fn read(&self, key: &str) -> Result<Vec<u8>> {
        let full = format!("{}{}", self.prefix, key);
        let buf = self.op.read(&full)?;
        Ok(buf.to_vec())
    }

    fn head(&self, key: &str) -> Result<Option<super::ObjectMeta>> {
        let full = format!("{}{}", self.prefix, key);
        match self.op.stat(&full) {
            Ok(meta) => Ok(Some(super::ObjectMeta {
                key: key.to_string(),
                size_bytes: meta.content_length(),
            })),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn r#move(&self, from: &str, to: &str) -> Result<()> {
        // opendal 0.55 returns `Unsupported` for `rename` on Azure Blob (same
        // shape as S3/GCS).  Fall back to copy + delete.  ADR-0012 M9
        // best-effort: a partial copy-ok / delete-fail leaves the source
        // reachable at both paths and re-trips M9 on the next resume.
        let from_full = format!("{}{}", self.prefix, from);
        let to_full = format!("{}{}", self.prefix, to);
        self.op.copy(&from_full, &to_full)?;
        self.op.delete(&from_full)?;
        Ok(())
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
}
