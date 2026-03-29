use std::path::Path;
use std::sync::Arc;

use opendal::blocking;
use opendal::services::S3;
use opendal::Operator;

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

        if let Some(env_name) = &config.access_key_env {
            let key = std::env::var(env_name)
                .map_err(|_| anyhow::anyhow!("env var '{}' not set for S3 access key", env_name))?;
            builder = builder.access_key_id(&key);
        }
        if let Some(env_name) = &config.secret_key_env {
            let secret = std::env::var(env_name)
                .map_err(|_| anyhow::anyhow!("env var '{}' not set for S3 secret key", env_name))?;
            builder = builder.secret_access_key(&secret);
        }

        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| anyhow::anyhow!("failed to create tokio runtime for S3: {}", e))?,
        );
        let _guard = runtime.enter();

        let async_op = Operator::new(builder)?.finish();
        let op = blocking::Operator::new(async_op)?;

        let prefix = config.prefix.clone().unwrap_or_default();

        Ok(Self { _runtime: runtime, op, prefix })
    }
}

impl super::Destination for S3Destination {
    fn write(&self, local_path: &Path, remote_key: &str) -> Result<()> {
        let key = format!("{}{}", self.prefix, remote_key);
        let data = std::fs::read(local_path)?;
        self.op.write(&key, data)?;
        log::info!("uploaded s3://{}", key);
        Ok(())
    }
}
