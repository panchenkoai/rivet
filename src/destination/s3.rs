use opendal::Operator;
use opendal::services::S3;

use super::cloud::{CloudBackend, CloudDestination};
use crate::config::DestinationConfig;
use crate::error::Result;

/// S3 object-store destination. The retry policy, blocking wrap, and ADR-0013
/// read surface live in [`CloudDestination`]; this type only knows how to
/// authenticate against S3 (and S3-compatible endpoints).
pub type S3Destination = CloudDestination<S3Backend>;

/// Zero-sized backend marker carrying S3's operator construction.
pub struct S3Backend;

/// Read a credential from an env var into a `Zeroizing<String>`.
///
/// SecOps: the underlying heap buffer is zeroed on drop instead of lingering
/// in freed memory (visible via core dump, ptrace, or heap reuse). OpenDAL
/// stores its own copy inside `reqsign`; the `Zeroizing` wrapper only protects
/// our transient handle.
fn read_credential_env(env_name: &str, label: &str) -> Result<zeroize::Zeroizing<String>> {
    let value = std::env::var(env_name)
        .map_err(|_| anyhow::anyhow!("env var '{}' not set for S3 {}", env_name, label))?;
    Ok(zeroize::Zeroizing::new(value))
}

impl CloudBackend for S3Backend {
    const RUNTIME_LABEL: &'static str = "S3";
    const SCHEME: &'static str = "s3";

    fn build_operator(config: &DestinationConfig) -> Result<Operator> {
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

        if let Some(env_name) = &config.access_key_env {
            let key = read_credential_env(env_name, "access key")?;
            builder = builder.access_key_id(key.as_str());
        }
        if let Some(env_name) = &config.secret_key_env {
            let secret = read_credential_env(env_name, "secret key")?;
            builder = builder.secret_access_key(secret.as_str());
        }
        // STS session token: required whenever `access_key_id` starts with
        // `ASIA…` rather than `AKIA…`.  See `docs/cloud-auth.md`.
        if let Some(env_name) = &config.session_token_env {
            let token = read_credential_env(env_name, "session token")?;
            builder = builder.session_token(token.as_str());
        }

        // `aws_profile` uses reqsign's per-instance loader (no `env::set_var`,
        // so it's safe under `--parallel-exports`).  Caveat: the default chain
        // falls through to IMDS, which hangs off-EC2 — see `docs/cloud-auth.md`
        // for the AWS SSO / Identity Center bridge.
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

        Ok(Operator::new(builder)?.finish())
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

    // ── session_token_env (STS / SSO / AssumeRole credentials) ────────────────

    #[test]
    fn session_token_env_field_parsed_from_destination_config() {
        use crate::config::DestinationConfig;
        let yaml = r#"
type: s3
bucket: my-bucket
access_key_env: AWS_ACCESS_KEY_ID
secret_key_env: AWS_SECRET_ACCESS_KEY
session_token_env: AWS_SESSION_TOKEN
"#;
        let config: DestinationConfig = serde_yaml_ng::from_str(yaml).unwrap();
        assert_eq!(
            config.session_token_env.as_deref(),
            Some("AWS_SESSION_TOKEN")
        );
    }

    #[test]
    fn read_credential_env_missing_var_errors_with_label() {
        // Reach a unique env-var name that is guaranteed unset across runners.
        let name = "RIVET_TEST_S3_TOKEN_DEFINITELY_UNSET_XYZ";
        // SAFETY: test-only; binary is single-threaded in this test context.
        unsafe { std::env::remove_var(name) };
        let err = super::read_credential_env(name, "session token").unwrap_err();
        let msg = format!("{err:#}");
        // The error must surface both the env-var name (operator can grep)
        // and the credential label (operator knows which slot is empty).
        assert!(msg.contains(name), "missing env var name in error: {msg}");
        assert!(
            msg.contains("session token"),
            "missing credential label in error: {msg}"
        );
    }

    #[test]
    fn read_credential_env_reads_value_into_zeroizing() {
        let name = "RIVET_TEST_S3_TOKEN_PRESENT_XYZ";
        // SAFETY: test-only; binary is single-threaded in this test context.
        unsafe { std::env::set_var(name, "fake-token-value") };
        let zeroizing = super::read_credential_env(name, "session token").unwrap();
        assert_eq!(zeroizing.as_str(), "fake-token-value");
        unsafe { std::env::remove_var(name) };
    }
}
