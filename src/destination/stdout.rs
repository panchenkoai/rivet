use std::io::Write;
use std::path::Path;

use crate::error::Result;

pub struct StdoutDestination;

impl StdoutDestination {
    pub fn new() -> Result<Self> {
        Ok(Self)
    }
}

impl super::Destination for StdoutDestination {
    fn write(&self, local_path: &Path, _remote_key: &str) -> Result<()> {
        let mut src = std::fs::File::open(local_path)?;
        let stdout = std::io::stdout();
        let mut handle = stdout.lock();
        std::io::copy(&mut src, &mut handle)?;
        handle.flush()?;
        Ok(())
    }

    fn capabilities(&self) -> super::DestinationCapabilities {
        super::DestinationCapabilities {
            commit_protocol: super::WriteCommitProtocol::Streaming,
            idempotent_overwrite: false,
            retry_safe: false,
            partial_write_risk: true,
        }
    }
}

// ─── ADR-0004 capability contract tests ──────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;
    use crate::destination::{Destination, WriteCommitProtocol};

    fn dest() -> StdoutDestination {
        StdoutDestination
    }

    /// ADR-0004: stdout uses Streaming — there is no atomic commit boundary.
    /// Partial output may be visible to the reader before `write()` returns.
    #[test]
    fn stdout_commit_protocol_is_streaming() {
        let caps = dest().capabilities();
        assert_eq!(caps.commit_protocol, WriteCommitProtocol::Streaming);
    }

    /// stdout cannot overwrite: once bytes are emitted they cannot be recalled.
    #[test]
    fn stdout_not_idempotent() {
        assert!(!dest().capabilities().idempotent_overwrite);
    }

    /// Retrying after a partial write to stdout duplicates output — not safe.
    #[test]
    fn stdout_not_retry_safe_has_partial_write_risk() {
        let caps = dest().capabilities();
        assert!(!caps.retry_safe, "stdout retry produces duplicate output");
        assert!(
            caps.partial_write_risk,
            "partial output is always possible with streaming"
        );
    }
}
