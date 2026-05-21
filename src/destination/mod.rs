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

/// Read-side metadata for a single object at the destination.
///
/// Returned by [`Destination::list_prefix`] and [`Destination::head`].  The
/// minimal field set is what ADR-0012 M5 verification needs: the relative
/// `key` (so the caller can correlate with `manifest.parts[].path`) and the
/// `size_bytes` (so M5's "size matches recorded value" check is one
/// comparison).  More fields (etag, last_modified, content_type) can be
/// added later without breaking the trait.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    /// Object key relative to the destination's configured prefix —
    /// the same shape `Destination::write`'s `remote_key` argument uses.
    pub key: String,
    pub size_bytes: u64,
}

/// Object-safe surface for upload backends. `Send + Sync` so a single `Arc` can be shared
/// across parallel chunk workers (one OpenDAL/HTTP stack per export, not one Tokio runtime per chunk).
pub trait Destination: Send + Sync {
    fn write(&self, local_path: &Path, remote_key: &str) -> Result<()>;

    /// Describe the operational guarantees of this destination backend.
    ///
    /// Called at runtime via `log_capabilities` in all pipeline entry points (single,
    /// chunked/exec, chunked/mod) to log backend semantics and warn when a non-retry-safe
    /// destination is configured with retries.
    ///
    /// State and manifest writes (ADR-0001 invariants I2–I4) must happen only after
    /// `write()` succeeds AND the backend's `commit_protocol` confirms the output is
    /// durably committed.  For `Atomic` and `FinalizeOnClose` backends this means after
    /// `write()` returns `Ok(())`; for `Streaming` there is no safe moment.
    fn capabilities(&self) -> DestinationCapabilities;

    // ── Read-side surface (ADR-0013 / Phase A for ADR-0012 M5/M8) ──────────
    //
    // Every cloud-or-local-file destination needs to be readable so that
    // post-run `--validate` (M5), `--reconcile` (M5 + source compare), and
    // `--resume` (M8 decision matrix) can inspect the prefix without keeping
    // a parallel local cache.  Streaming destinations (stdout) have no
    // coherent prefix and must surface a clear "unsupported" error from
    // every read method — the manifest writer already short-circuits the
    // streaming case (`SkippedStreaming`), so callers should never reach
    // these methods on stdout in practice.
    //
    // Default implementations return "unsupported" so adding a new backend
    // doesn't have to opt in immediately; the readers will surface the
    // gap explicitly the first time they need it.

    /// List every object at or below `prefix`, in **unspecified** order.
    ///
    /// `prefix` is relative to the destination root, mirrors the `remote_key`
    /// shape of [`write`].  Empty `prefix` lists everything under the root.
    /// Implementations may walk recursively (local FS) or use a single
    /// listing call (S3/GCS object stores).
    fn list_prefix(&self, prefix: &str) -> Result<Vec<ObjectMeta>> {
        let _ = prefix;
        anyhow::bail!("list_prefix is not supported by this destination backend")
    }

    /// Read the full body of `key` into memory.  Used for small artifacts
    /// only (`manifest.json`, `_SUCCESS`).  Per-part reads should go through
    /// a future streaming reader if and when `--validate --deep` lands.
    fn read(&self, key: &str) -> Result<Vec<u8>> {
        let _ = key;
        anyhow::bail!("read is not supported by this destination backend")
    }

    /// Stat `key`, returning `None` if the object does not exist and the
    /// underlying backend can disambiguate "absent" from a hard error.
    /// Implementations that cannot disambiguate must surface the underlying
    /// error rather than swallow it as `None`.
    fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        let _ = key;
        anyhow::bail!("head is not supported by this destination backend")
    }

    /// Move `from` to `to` at the destination prefix.
    ///
    /// ADR-0012 M9 quarantine: a part the resume preamble can't reuse
    /// (size drift, fingerprint mismatch, untracked surplus) gets
    /// moved out of the way to `_quarantine/<run_id>/<original-name>`
    /// so the next write doesn't have to share the prefix with stale
    /// data, and so a future operator can investigate.
    ///
    /// Best-effort semantics: the caller treats every failure as
    /// non-fatal (M9: "never bail on a quarantine failure").  The
    /// operation is allowed to be non-atomic on object stores
    /// (copy + delete inside opendal's `rename`); a partial failure
    /// leaves the object reachable at both paths and is a
    /// "clutter problem, not a correctness problem" per the ADR.
    fn r#move(&self, from: &str, to: &str) -> Result<()> {
        let _ = (from, to);
        anyhow::bail!("move is not supported by this destination backend")
    }
}

/// Log destination capabilities at DEBUG level and emit a WARN when the backend is not
/// retry-safe but retries are configured.  Call once per export after `create_destination`.
pub fn log_capabilities(export_name: &str, dest: &dyn Destination, max_retries: u32) {
    let caps = dest.capabilities();
    log::debug!(
        "export '{}': destination commit_protocol={:?} idempotent={} retry_safe={} partial_risk={}",
        export_name,
        caps.commit_protocol,
        caps.idempotent_overwrite,
        caps.retry_safe,
        caps.partial_write_risk,
    );
    if !caps.retry_safe && max_retries > 0 {
        log::warn!(
            "export '{}': destination is not retry-safe (max_retries={}); \
             partial artifacts may exist at destination on failure — manual cleanup may be needed",
            export_name,
            max_retries,
        );
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    struct MockDest {
        caps: DestinationCapabilities,
    }

    impl Destination for MockDest {
        fn write(&self, _local: &Path, _key: &str) -> Result<()> {
            Ok(())
        }
        fn capabilities(&self) -> DestinationCapabilities {
            self.caps.clone()
        }
    }

    fn atomic_safe() -> MockDest {
        MockDest {
            caps: DestinationCapabilities {
                commit_protocol: WriteCommitProtocol::Atomic,
                idempotent_overwrite: true,
                retry_safe: true,
                partial_write_risk: false,
            },
        }
    }

    fn streaming_unsafe() -> MockDest {
        MockDest {
            caps: DestinationCapabilities {
                commit_protocol: WriteCommitProtocol::Streaming,
                idempotent_overwrite: false,
                retry_safe: false,
                partial_write_risk: true,
            },
        }
    }

    // ── log_capabilities smoke tests — just verify no panic ─────────────────

    #[test]
    fn log_capabilities_retry_safe_no_panic() {
        log_capabilities("orders", &atomic_safe(), 3);
    }

    #[test]
    fn log_capabilities_not_retry_safe_with_retries_no_panic() {
        log_capabilities("orders", &streaming_unsafe(), 3);
    }

    #[test]
    fn log_capabilities_zero_retries_no_panic() {
        log_capabilities("orders", &streaming_unsafe(), 0);
    }

    // ── create_destination — local roundtrip ─────────────────────────────────

    #[test]
    fn create_destination_local_succeeds() {
        use crate::config::{DestinationConfig, DestinationType};
        let dir = tempfile::TempDir::new().unwrap();
        let config = DestinationConfig {
            destination_type: DestinationType::Local,
            bucket: None,
            prefix: None,
            path: Some(dir.path().to_str().unwrap().to_string()),
            region: None,
            endpoint: None,
            credentials_file: None,
            access_key_env: None,
            secret_key_env: None,
            aws_profile: None,
            session_token_env: None,
            allow_anonymous: false,
        };
        let dest = create_destination(&config).unwrap();
        let caps = dest.capabilities();
        assert_eq!(caps.commit_protocol, WriteCommitProtocol::Atomic);
        assert!(caps.idempotent_overwrite);
    }

    // ── create_destination — stdout succeeds ─────────────────────────────────

    #[test]
    fn create_destination_stdout_succeeds() {
        use crate::config::{DestinationConfig, DestinationType};
        let config = DestinationConfig {
            destination_type: DestinationType::Stdout,
            bucket: None,
            prefix: None,
            path: None,
            region: None,
            endpoint: None,
            credentials_file: None,
            access_key_env: None,
            secret_key_env: None,
            aws_profile: None,
            session_token_env: None,
            allow_anonymous: false,
        };
        let dest = create_destination(&config).unwrap();
        let caps = dest.capabilities();
        assert_eq!(caps.commit_protocol, WriteCommitProtocol::Streaming);
    }
}
