use std::path::Path;

use opendal::layers::BlockingLayer;
use opendal::services::S3;
use opendal::BlockingOperator;
use opendal::Operator;

use crate::config::DestinationConfig;
use crate::error::Result;

pub struct S3Destination {
    op: BlockingOperator,
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

        let op = Operator::new(builder)?
            .layer(BlockingLayer::create()?)
            .finish()
            .blocking();

        let prefix = config.prefix.clone().unwrap_or_default();

        Ok(Self { op, prefix })
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
