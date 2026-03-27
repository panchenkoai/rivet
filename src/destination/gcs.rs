use std::path::Path;

use opendal::layers::BlockingLayer;
use opendal::services::Gcs;
use opendal::BlockingOperator;
use opendal::Operator;

use crate::config::DestinationConfig;
use crate::error::Result;

pub struct GcsDestination {
    op: BlockingOperator,
    prefix: String,
}

impl GcsDestination {
    pub fn new(config: &DestinationConfig) -> Result<Self> {
        let bucket = config
            .bucket
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("GCS destination requires 'bucket'"))?;

        let mut builder = Gcs::default().bucket(bucket);

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

impl super::Destination for GcsDestination {
    fn write(&self, local_path: &Path, remote_key: &str) -> Result<()> {
        let key = format!("{}{}", self.prefix, remote_key);
        let data = std::fs::read(local_path)?;
        self.op.write(&key, data)?;
        log::info!("uploaded gs://{}", key);
        Ok(())
    }
}
