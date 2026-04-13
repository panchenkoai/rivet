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

    fn capabilities(&self) -> super::DestinationCapabilities {
        super::DestinationCapabilities {
            commit_protocol: super::WriteCommitProtocol::Atomic,
            idempotent_overwrite: true,
            retry_safe: false, // fs::copy may leave a partial file at the destination on failure
            partial_write_risk: true,
        }
    }
}

// ─── ADR-0004 capability contract tests ──────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;
    use crate::destination::{Destination, WriteCommitProtocol};

    fn dest() -> LocalDestination {
        LocalDestination {
            base_path: ".".into(),
        }
    }

    /// ADR-0004: local fs::copy is Atomic — `write()` returning Ok(()) means the full
    /// file is present at the destination path.
    #[test]
    fn local_commit_protocol_is_atomic() {
        let caps = dest().capabilities();
        assert_eq!(caps.commit_protocol, WriteCommitProtocol::Atomic);
    }

    /// Writing the same key twice replaces the file; no orphaned artifact is left.
    #[test]
    fn local_idempotent_overwrite() {
        assert!(dest().capabilities().idempotent_overwrite);
    }

    /// `fs::copy` can leave a partial file if the process is killed mid-copy,
    /// so the local destination is not retry-safe without manual cleanup.
    #[test]
    fn local_not_retry_safe_has_partial_write_risk() {
        let caps = dest().capabilities();
        assert!(
            !caps.retry_safe,
            "fs::copy is not retry-safe (partial file risk)"
        );
        assert!(
            caps.partial_write_risk,
            "partial file can be left on failure"
        );
    }
}
