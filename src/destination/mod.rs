pub mod gcs;
mod gcs_auth;
pub mod local;
pub mod s3;

use std::path::Path;

use crate::config::DestinationConfig;
use crate::error::Result;

pub trait Destination {
    fn write(&self, local_path: &Path, remote_key: &str) -> Result<()>;
}

pub fn create_destination(config: &DestinationConfig) -> Result<Box<dyn Destination>> {
    use crate::config::DestinationType;
    match config.destination_type {
        DestinationType::Local => Ok(Box::new(local::LocalDestination::new(config)?)),
        DestinationType::S3 => Ok(Box::new(s3::S3Destination::new(config)?)),
        DestinationType::Gcs => Ok(Box::new(gcs::GcsDestination::new(config)?)),
    }
}
