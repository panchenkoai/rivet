use std::path::Path;

use crate::config::DestinationConfig;
use crate::error::Result;

pub struct LocalDestination {
    base_path: String,
}

impl LocalDestination {
    pub fn new(config: &DestinationConfig) -> Result<Self> {
        let base_path = config
            .path
            .clone()
            .or_else(|| config.prefix.clone())
            .unwrap_or_else(|| ".".to_string());
        Ok(Self { base_path })
    }
}

impl super::Destination for LocalDestination {
    fn write(&self, local_path: &Path, remote_key: &str) -> Result<()> {
        let target = Path::new(&self.base_path).join(remote_key);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::copy(local_path, &target)?;
        log::info!("wrote {}", target.display());
        Ok(())
    }
}
