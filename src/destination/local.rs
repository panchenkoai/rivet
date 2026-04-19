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
    use crate::config::DestinationType;
    use crate::destination::{Destination, WriteCommitProtocol};
    use std::io::Write;

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

    // ─── Local destination edge cases (QA backlog Task 6.1) ────────────────

    /// Build a `LocalDestination` rooted at `base_path` without touching the
    /// full `DestinationConfig` schema; every field except `path` is None.
    fn dest_at(base_path: &std::path::Path) -> LocalDestination {
        LocalDestination::new(&DestinationConfig {
            destination_type: DestinationType::Local,
            bucket: None,
            prefix: None,
            path: Some(base_path.to_string_lossy().into_owned()),
            region: None,
            endpoint: None,
            credentials_file: None,
            access_key_env: None,
            secret_key_env: None,
            aws_profile: None,
            allow_anonymous: false,
        })
        .unwrap()
    }

    fn source_file_with(bytes: &[u8]) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(bytes).unwrap();
        f.flush().unwrap();
        f
    }

    /// Writing into a nested key must auto-create every missing parent
    /// directory — operators rely on this when `prefix` contains date/hour
    /// partitions that do not yet exist.
    #[test]
    fn write_auto_creates_nested_parent_directories() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        let src = source_file_with(b"hello\n");

        dest.write(src.path(), "a/b/c/payload.csv").unwrap();

        let target = dir.path().join("a/b/c/payload.csv");
        assert!(target.exists(), "nested target must exist");
        assert_eq!(std::fs::read(&target).unwrap(), b"hello\n");
    }

    /// Writing twice to the same key must produce a deterministic final
    /// state (second payload wins) — this is the documented
    /// `idempotent_overwrite: true` capability.
    #[test]
    fn writing_same_key_twice_replaces_content_deterministically() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());

        let first = source_file_with(b"first run\n");
        dest.write(first.path(), "data.csv").unwrap();

        let second = source_file_with(b"second run\n");
        dest.write(second.path(), "data.csv").unwrap();

        let target = dir.path().join("data.csv");
        assert_eq!(
            std::fs::read(&target).unwrap(),
            b"second run\n",
            "second write must replace the first; no stale content"
        );
    }

    /// Writing into a read-only directory must fail fast with an Err, not
    /// panic and not silently succeed.  The test skips on platforms where
    /// changing file permissions is not supported (covered elsewhere by CI).
    #[cfg(unix)]
    #[test]
    fn write_to_readonly_directory_returns_error_not_panic() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let readonly = dir.path().join("readonly");
        std::fs::create_dir(&readonly).unwrap();
        let mut perms = std::fs::metadata(&readonly).unwrap().permissions();
        perms.set_mode(0o500); // r-x for owner, no write
        std::fs::set_permissions(&readonly, perms).unwrap();

        let dest = dest_at(&readonly);
        let src = source_file_with(b"data");

        let result = dest.write(src.path(), "out.csv");

        // Restore perms so tempdir cleanup can proceed.
        let mut restore = std::fs::metadata(&readonly).unwrap().permissions();
        restore.set_mode(0o700);
        let _ = std::fs::set_permissions(&readonly, restore);

        assert!(
            result.is_err(),
            "writing into a read-only directory must return Err, not succeed"
        );
    }

    /// When the resolved destination path refers to an existing *file* (not a
    /// directory), `create_dir_all` on its parent succeeds but `fs::copy`
    /// then fails because the target inside it cannot be created. The
    /// operation must surface as Err and must not panic.
    #[test]
    fn write_when_base_path_points_at_a_file_returns_error() {
        // Create a regular file, then try to treat it as the destination root.
        let f = tempfile::NamedTempFile::new().unwrap();
        let dest = dest_at(f.path());
        let src = source_file_with(b"data");

        let result = dest.write(src.path(), "nested/out.csv");
        assert!(
            result.is_err(),
            "writing under a file-typed base_path must fail cleanly"
        );
    }

    /// A missing source file is an Err, not a panic.  This path is exercised
    /// if the pipeline hands a path whose temp file was cleaned up too early.
    #[test]
    fn write_with_nonexistent_source_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());

        let missing = dir.path().join("does_not_exist.csv");
        let result = dest.write(&missing, "out.csv");
        assert!(result.is_err(), "missing source must surface as Err");
    }

    /// The `remote_key` may contain characters that are legal on POSIX file
    /// systems but unusual (spaces, unicode).  Writing must round-trip their
    /// bytes verbatim.
    #[test]
    fn write_preserves_unusual_but_legal_key_characters() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        let src = source_file_with(b"payload");

        let key = "with space/🚀 файл.csv";
        dest.write(src.path(), key).unwrap();

        assert!(
            dir.path().join(key).exists(),
            "unicode-and-space key must be preserved verbatim"
        );
    }
}
