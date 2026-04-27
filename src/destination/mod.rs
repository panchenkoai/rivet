//! **Layer: Execution**
//!
//! Destination backends — local filesystem, S3, GCS, stdout.
//! Each backend declares its `DestinationCapabilities` so the pipeline can reason
//! about commit boundaries and recovery semantics without inspecting backend internals.

pub mod gcs;
mod gcs_auth;
pub mod local;
pub mod s3;
pub mod stdout;

use std::path::Path;

use crate::config::DestinationConfig;
use crate::error::Result;

/// The point at which a write is considered durably committed and visible to readers.
///
/// See ADR-0004 for the per-backend contract table and alignment with state invariants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteCommitProtocol {
    /// The write is atomic from the caller's perspective: `write()` returning `Ok(())`
    /// means the full file is present at the destination.  A failure leaves no partial
    /// artifact (or leaves one that must be cleaned up — see `partial_write_risk`).
    Atomic,
    /// The object is not committed until the internal writer handle is closed.
    /// The object is never partially visible to readers; a mid-upload failure means
    /// nothing was written.  S3 PutObject and GCS resumable uploads use this protocol.
    FinalizeOnClose,
    /// Data is streamed to an unbuffered output with no atomic commit boundary.
    /// Partial output may be observable before `write()` returns.
    /// Retrying after failure produces duplicate or corrupt output.
    Streaming,
}

/// Operational guarantees provided by a destination backend.
///
/// Returned by `Destination::capabilities()`.  Inspected at runtime in the execution
/// path to log backend semantics and warn when a non-retry-safe destination is used
/// with retries.  See ADR-0004 for the per-backend contract table.
#[derive(Debug, Clone)]
pub struct DestinationCapabilities {
    /// When the written output is considered durably committed.
    pub commit_protocol: WriteCommitProtocol,
    /// Whether writing the same key twice produces a clean replacement with no leftover
    /// artifacts from the previous attempt.  `false` for stdout and streaming sinks.
    pub idempotent_overwrite: bool,
    /// Whether a failed `write()` can be retried without manual cleanup.
    /// `false` when a partial artifact may be left at the destination on failure.
    pub retry_safe: bool,
    /// Whether a failed `write()` can leave a partial file or object at the destination.
    /// When `true`, the caller must remove the artifact before retrying.
    pub partial_write_risk: bool,
}

/// Object-safe surface for upload backends. `Send + Sync` so a single `Arc` can be shared
/// across parallel chunk workers (one OpenDAL/HTTP stack per export, not one Tokio runtime per chunk).
pub trait Destination: Send + Sync {
    fn write(&self, local_path: &Path, remote_key: &str) -> Result<()>;

    /// Describe the operational guarantees of this destination backend.
    ///
    /// Called at runtime before the write loop in `pipeline/single.rs` to log backend
    /// semantics and warn when a non-retry-safe destination is used after retries.
    ///
    /// State and manifest writes (ADR-0001 invariants I2–I4) must happen only after
    /// `write()` succeeds AND the backend's `commit_protocol` confirms the output is
    /// durably committed.  For `Atomic` and `FinalizeOnClose` backends this means after
    /// `write()` returns `Ok(())`; for `Streaming` there is no safe moment.
    fn capabilities(&self) -> DestinationCapabilities;
}

pub fn create_destination(config: &DestinationConfig) -> Result<Box<dyn Destination>> {
    use crate::config::DestinationType;
    match config.destination_type {
        DestinationType::Local => Ok(Box::new(local::LocalDestination::new(config)?)),
        DestinationType::S3 => Ok(Box::new(s3::S3Destination::new(config)?)),
        DestinationType::Gcs => Ok(Box::new(gcs::GcsDestination::new(config)?)),
        DestinationType::Stdout => Ok(Box::new(stdout::StdoutDestination::new()?)),
    }
}
