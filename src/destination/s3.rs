use std::path::Path;
use std::sync::{Arc, Mutex};

use opendal::Operator;
use opendal::blocking;
use opendal::services::S3;

use crate::config::DestinationConfig;
use crate::error::Result;

pub struct S3Destination {
    _runtime: Arc<tokio::runtime::Runtime>,
    op: blocking::Operator,
    prefix: String,
}

/// Serializes the brief `AWS_PROFILE` env-var override needed to seed OpenDAL's
/// AWS credential chain. OpenDAL reads `AWS_PROFILE` from the process env at
/// `builder.build()` time (see `AwsConfig::from_profile`), and there is no public
/// builder setter for the profile name in opendal 0.55. Without this lock,
/// `--parallel-exports` with differing `aws_profile` values would race: the last
/// writer would win and both exports would silently use the same credentials,
/// potentially writing to the wrong AWS account.
///
/// We hold the lock across the full `Operator::new(builder)?.finish()` call, then
/// restore the previous `AWS_PROFILE` value before releasing it. A permanent
/// process-wide mutation is avoided.
static AWS_PROFILE_LOCK: Mutex<()> = Mutex::new(());

/// RAII guard that restores the previous `AWS_PROFILE` (or removes the var) on drop.
struct AwsProfileGuard<'a> {
    _mutex: std::sync::MutexGuard<'a, ()>,
    previous: Option<String>,
}

impl Drop for AwsProfileGuard<'_> {
    fn drop(&mut self) {
        // SAFETY: the mutex guarantees no other thread is reading or writing
        // AWS_PROFILE through this code path. External readers of the env are out
        // of our control regardless of the approach.
        unsafe {
            match &self.previous {
                Some(v) => std::env::set_var("AWS_PROFILE", v),
                None => std::env::remove_var("AWS_PROFILE"),
            }
        }
    }
}

fn scoped_aws_profile(profile: &str) -> AwsProfileGuard<'static> {
    let guard = AWS_PROFILE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let previous = std::env::var("AWS_PROFILE").ok();
    // SAFETY: guarded by AWS_PROFILE_LOCK (see module-level doc comment).
    unsafe { std::env::set_var("AWS_PROFILE", profile) };
    AwsProfileGuard {
        _mutex: guard,
        previous,
    }
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

        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| anyhow::anyhow!("failed to create tokio runtime for S3: {}", e))?,
        );
        let _guard = runtime.enter();

        // Any `AWS_PROFILE` mutation is scoped to the `Operator::new` call below.
        let _profile_guard = config.aws_profile.as_deref().map(|profile| {
            log::info!("S3: using AWS profile '{}'", profile);
            scoped_aws_profile(profile)
        });

        let async_op = Operator::new(builder)?.finish();
        let op = blocking::Operator::new(async_op)?;
        drop(_profile_guard);

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
