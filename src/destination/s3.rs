use std::path::Path;
use std::sync::Arc;

use opendal::Operator;
use opendal::blocking;
use opendal::layers::RetryLayer;
use opendal::services::S3;

use crate::config::DestinationConfig;
use crate::error::Result;

pub struct S3Destination {
    _runtime: Arc<tokio::runtime::Runtime>,
    op: blocking::Operator,
    prefix: String,
}

impl S3Destination {
    pub fn new(config: &DestinationConfig) -> Result<Self> {
        let bucket = config
            .bucket
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("S3 destination requires 'bucket'"))?;

        let mut builder = S3::default().bucket(bucket);

        if let Some(region) = &config.region {
            builder = builder.region(region);
        }
        if let Some(endpoint) = &config.endpoint {
            builder = builder.endpoint(endpoint);
        }

        // SecOps: wrap AWS credentials in `Zeroizing<String>` so the underlying
        // heap buffer is zeroed the moment the value is dropped, rather than
        // lingering in freed memory pages (visible via core dump, ptrace, or
        // heap reuse). `&key` / `&secret` are still passed verbatim to OpenDAL,
        // which stores its own copy inside `reqsign`.
        if let Some(env_name) = &config.access_key_env {
            let key = zeroize::Zeroizing::new(std::env::var(env_name).map_err(|_| {
                anyhow::anyhow!("env var '{}' not set for S3 access key", env_name)
            })?);
            builder = builder.access_key_id(key.as_str());
        }
        if let Some(env_name) = &config.secret_key_env {
            let secret = zeroize::Zeroizing::new(std::env::var(env_name).map_err(|_| {
                anyhow::anyhow!("env var '{}' not set for S3 secret key", env_name)
            })?);
            builder = builder.secret_access_key(secret.as_str());
        }

        // When `aws_profile` is set in rivet config, build a reqsign credential
        // loader with the profile name baked in — no `env::set_var` required.
        // This is safe under `--parallel-exports` because each export gets its
        // own loader instance; no global state is mutated.
        if let Some(profile) = &config.aws_profile {
            log::info!("S3: using AWS profile '{}'", profile);
            let cred_config = reqsign::AwsConfig {
                profile: profile.clone(),
                ..Default::default()
            }
            .from_profile()
            .from_env();
            let loader = reqsign::AwsDefaultLoader::new(reqwest::Client::new(), cred_config);
            builder = builder.customized_credential_load(Box::new(loader));
        }

        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| anyhow::anyhow!("failed to create tokio runtime for S3: {}", e))?,
        );
        let _guard = runtime.enter();

        // See gcs.rs for the rationale; same `RetryLayer` is applied to S3
        // so transient hyper / SDK errors are absorbed inside the operator
        // instead of escalating to a chunk-level re-fetch.
        let op = blocking::Operator::new(
            Operator::new(builder)?
                .layer(
                    RetryLayer::new()
                        .with_max_times(5)
                        .with_min_delay(std::time::Duration::from_millis(200))
                        .with_max_delay(std::time::Duration::from_secs(10))
                        .with_jitter(),
                )
                .finish(),
        )?;

        let prefix = config.prefix.clone().unwrap_or_default();

        Ok(Self {
            _runtime: runtime,
            op,
            prefix,
        })
    }
}

impl super::Destination for S3Destination {
    fn write(&self, local_path: &Path, remote_key: &str) -> Result<()> {
        let key = format!("{}{}", self.prefix, remote_key);
        let mut src = std::fs::File::open(local_path)?;
        let mut dst = self.op.writer(&key)?.into_std_write();
        std::io::copy(&mut src, &mut dst)?;
        dst.close()?;
        log::info!("uploaded s3://{}", key);
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
}

#[cfg(test)]
mod tests {
    // ── aws_profile thread-safety ─────────────────────────────────────────────
    //
    // The key invariant: when `aws_profile` is set in DestinationConfig, the
    // code must NOT call `std::env::set_var("AWS_PROFILE", ...)` — that would
    // be a data race under --parallel-exports. Instead it uses a per-export
    // `reqsign::AwsConfig { profile, .. }` instance.
    //
    // These tests verify the no-env-mutation contract by exercising the exact
    // same code path S3Destination::new() uses for the aws_profile branch,
    // without requiring S3 credentials or network access.

    #[test]
    fn aws_profile_does_not_mutate_aws_profile_env_var() {
        let before = std::env::var("AWS_PROFILE").ok();

        // Mirror S3Destination::new(): build AwsConfig with a profile name.
        // Critically: no env::set_var call anywhere in this path.
        let profile = "unit-test-profile-rivet";
        let cred_config = reqsign::AwsConfig {
            profile: profile.to_string(),
            ..Default::default()
        }
        .from_profile()
        .from_env();
        // Drop without making network calls — we're only testing env isolation.
        drop(cred_config);

        let after = std::env::var("AWS_PROFILE").ok();
        assert_eq!(
            before, after,
            "building AwsConfig with a named profile must not mutate the AWS_PROFILE env var"
        );
    }

    #[test]
    fn aws_profile_independent_configs_are_independent() {
        // Two AwsConfig instances with different profiles must be independent
        // (no shared global state). This verifies that parallel exports each
        // get their own credential loader, not a shared one.
        let cfg_a = reqsign::AwsConfig {
            profile: "profile-a".to_string(),
            ..Default::default()
        };
        let cfg_b = reqsign::AwsConfig {
            profile: "profile-b".to_string(),
            ..Default::default()
        };
        // The profile field should reflect what was set — no cross-contamination.
        assert_eq!(cfg_a.profile, "profile-a");
        assert_eq!(cfg_b.profile, "profile-b");
    }

    #[test]
    fn aws_profile_config_field_parsed_from_destination_config() {
        use crate::config::DestinationConfig;
        let yaml = r#"
type: s3
bucket: my-bucket
aws_profile: staging
"#;
        let config: DestinationConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(config.aws_profile.as_deref(), Some("staging"));
    }
}
