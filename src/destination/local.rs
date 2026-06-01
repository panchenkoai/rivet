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
    fn write(&self, local_path: &Path, remote_key: &str) -> Result<super::WriteOutcome> {
        let target = Path::new(&self.base_path).join(remote_key);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::copy(local_path, &target)?;
        log::info!("wrote {}", target.display());
        Ok(super::WriteOutcome::opaque()) // local FS reports no content checksum
    }

    fn capabilities(&self) -> super::DestinationCapabilities {
        super::DestinationCapabilities {
            commit_protocol: super::WriteCommitProtocol::Atomic,
            idempotent_overwrite: true,
            retry_safe: false, // fs::copy may leave a partial file at the destination on failure
            partial_write_risk: true,
        }
    }

    // ── ADR-0013 read surface ────────────────────────────────────────────
    //
    // Local FS is the simplest backend: walk recursively under `base_path`
    // joined with `prefix`, return every regular file's relative path and
    // size.  The walk is depth-first via a small stack; opendal would also
    // work but introduces a tokio dependency for what is a five-line
    // POSIX walk.

    fn list_prefix(&self, prefix: &str) -> Result<Vec<super::ObjectMeta>> {
        let root = Path::new(&self.base_path).join(prefix);
        if !root.exists() {
            return Ok(Vec::new());
        }
        let base = Path::new(&self.base_path);
        let mut out = Vec::new();
        let mut stack = vec![root];
        while let Some(dir) = stack.pop() {
            // A non-directory `prefix` (e.g. a single file) is a degenerate
            // call but should still report that file rather than 404.
            let meta = std::fs::metadata(&dir)?;
            if meta.is_file() {
                let rel = dir
                    .strip_prefix(base)
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_else(|_| dir.to_string_lossy().into_owned());
                out.push(super::ObjectMeta {
                    key: rel,
                    size_bytes: meta.len(),
                    content_md5: None, // local FS exposes no checksum in metadata
                });
                continue;
            }
            for entry in std::fs::read_dir(&dir)? {
                let entry = entry?;
                let path = entry.path();
                let ftype = entry.file_type()?;
                if ftype.is_dir() {
                    stack.push(path);
                } else if ftype.is_file() {
                    let m = entry.metadata()?;
                    let rel = path
                        .strip_prefix(base)
                        .map(|p| p.to_string_lossy().into_owned())
                        .unwrap_or_else(|_| path.to_string_lossy().into_owned());
                    out.push(super::ObjectMeta {
                        key: rel,
                        size_bytes: m.len(),
                        content_md5: None,
                    });
                }
                // Other file types (symlinks loops, sockets) — silently
                // skipped.  Symlinks pointing at regular files would be
                // reported via their followed metadata above; cyclic
                // symlinks are intentionally not handled here.
            }
        }
        Ok(out)
    }

    fn read(&self, key: &str) -> Result<Vec<u8>> {
        let path = Path::new(&self.base_path).join(key);
        Ok(std::fs::read(path)?)
    }

    fn head(&self, key: &str) -> Result<Option<super::ObjectMeta>> {
        let path = Path::new(&self.base_path).join(key);
        match std::fs::metadata(&path) {
            Ok(m) if m.is_file() => Ok(Some(super::ObjectMeta {
                key: key.to_string(),
                size_bytes: m.len(),
                content_md5: None,
            })),
            // Treat "is a directory" the same as absent for our purposes —
            // M5 only cares about file-shaped objects.
            Ok(_) => Ok(None),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn r#move(&self, from: &str, to: &str) -> Result<()> {
        // POSIX `rename` is atomic on the same filesystem; fall back to
        // copy + delete when the rename crosses devices (rare on a
        // single destination prefix but cheap to handle).
        let src = Path::new(&self.base_path).join(from);
        let dst = Path::new(&self.base_path).join(to);
        if let Some(parent) = dst.parent() {
            std::fs::create_dir_all(parent)?;
        }
        match std::fs::rename(&src, &dst) {
            Ok(()) => Ok(()),
            Err(e) if e.raw_os_error() == Some(libc::EXDEV) => {
                // cross-device fallback
                std::fs::copy(&src, &dst)?;
                std::fs::remove_file(&src)?;
                Ok(())
            }
            Err(e) => Err(e.into()),
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

    fn dest_at(base_path: &std::path::Path) -> LocalDestination {
        LocalDestination::new(&DestinationConfig {
            destination_type: DestinationType::Local,
            path: Some(base_path.to_string_lossy().into_owned()),
            ..Default::default()
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

        let key = "with space/🚀 αρχείο.csv";
        dest.write(src.path(), key).unwrap();

        assert!(
            dir.path().join(key).exists(),
            "unicode-and-space key must be preserved verbatim"
        );
    }

    // ── ADR-0013 Phase A: read surface ───────────────────────────────────

    #[test]
    fn list_prefix_returns_files_with_relative_keys_and_sizes() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        std::fs::write(dir.path().join("a.txt"), b"abc").unwrap();
        std::fs::create_dir_all(dir.path().join("nested/sub")).unwrap();
        std::fs::write(dir.path().join("nested/b.txt"), b"hello").unwrap();
        std::fs::write(dir.path().join("nested/sub/c.bin"), b"\0\0\0\0").unwrap();

        let mut listed = dest.list_prefix("").unwrap();
        listed.sort_by(|x, y| x.key.cmp(&y.key));
        let names: Vec<_> = listed.iter().map(|m| m.key.clone()).collect();
        assert_eq!(
            names,
            vec![
                "a.txt".to_string(),
                "nested/b.txt".to_string(),
                "nested/sub/c.bin".to_string(),
            ]
        );
        let sizes: Vec<_> = listed.iter().map(|m| m.size_bytes).collect();
        assert_eq!(sizes, vec![3u64, 5u64, 4u64]);
    }

    #[test]
    fn list_prefix_scopes_to_subdirectory() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        std::fs::write(dir.path().join("top.txt"), b"x").unwrap();
        std::fs::create_dir_all(dir.path().join("only_me")).unwrap();
        std::fs::write(dir.path().join("only_me/a.parquet"), b"yy").unwrap();
        std::fs::write(dir.path().join("only_me/b.parquet"), b"zzz").unwrap();

        let listed = dest.list_prefix("only_me").unwrap();
        let names: std::collections::HashSet<_> = listed.iter().map(|m| m.key.clone()).collect();
        assert_eq!(
            names,
            ["only_me/a.parquet", "only_me/b.parquet"]
                .iter()
                .map(|s| s.to_string())
                .collect()
        );
    }

    #[test]
    fn list_prefix_missing_returns_empty_not_error() {
        // Resume / validate must distinguish "no manifest yet" from "I/O
        // failure".  Local FS surfaces the former as Ok(empty).
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        let listed = dest.list_prefix("does_not_exist").unwrap();
        assert!(listed.is_empty());
    }

    #[test]
    fn read_round_trips_bytes_verbatim() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        let payload: &[u8] = b"manifest body goes here\n";
        std::fs::write(dir.path().join("manifest.json"), payload).unwrap();
        let got = dest.read("manifest.json").unwrap();
        assert_eq!(got, payload);
    }

    #[test]
    fn head_returns_some_for_existing_file_with_correct_size() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        std::fs::write(dir.path().join("part-000001.parquet"), [42u8; 1234]).unwrap();
        let m = dest.head("part-000001.parquet").unwrap().unwrap();
        assert_eq!(m.key, "part-000001.parquet");
        assert_eq!(m.size_bytes, 1234);
    }

    #[test]
    fn head_returns_none_for_absent_file_not_error() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        assert!(dest.head("missing.txt").unwrap().is_none());
    }

    #[test]
    fn head_returns_none_for_directory_not_file() {
        // M5 only checks file-shaped keys; a directory at the same path
        // is treated as absent so the "missing part" branch is taken.
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        std::fs::create_dir_all(dir.path().join("subdir")).unwrap();
        assert!(dest.head("subdir").unwrap().is_none());
    }

    #[test]
    fn read_returns_err_on_missing_key() {
        // Symmetric with head's None: read of a missing key surfaces an
        // I/O error rather than empty bytes.  The validation layer relies
        // on this to fail loudly.
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        assert!(dest.read("nope.json").is_err());
    }

    // ── ADR-0012 M9 quarantine: move primitive ─────────────────────────

    #[test]
    fn move_relocates_file_and_creates_parent_directories() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        std::fs::write(dir.path().join("part-000001.parquet"), b"payload").unwrap();

        dest.r#move(
            "part-000001.parquet",
            "_quarantine/run_xyz/part-000001.parquet",
        )
        .unwrap();

        // Source gone; destination present at new key with same body.
        assert!(!dir.path().join("part-000001.parquet").exists());
        let body = std::fs::read(
            dir.path()
                .join("_quarantine")
                .join("run_xyz")
                .join("part-000001.parquet"),
        )
        .unwrap();
        assert_eq!(body, b"payload");
    }

    #[test]
    fn move_returns_err_on_missing_source() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        let result = dest.r#move("absent.parquet", "_quarantine/r/absent.parquet");
        assert!(
            result.is_err(),
            "moving a non-existent file must surface as Err so the caller logs it"
        );
    }

    #[test]
    fn move_overwrites_existing_destination_object() {
        // POSIX rename overwrites the target if it exists; mirror that
        // contract so a second resume cleaning the same prefix doesn't
        // bail out on "destination already exists in quarantine".
        let dir = tempfile::tempdir().unwrap();
        let dest = dest_at(dir.path());
        std::fs::write(dir.path().join("a"), b"new").unwrap();
        std::fs::create_dir_all(dir.path().join("_quarantine/r")).unwrap();
        std::fs::write(dir.path().join("_quarantine/r/a"), b"old").unwrap();

        dest.r#move("a", "_quarantine/r/a").unwrap();
        assert!(!dir.path().join("a").exists());
        let body = std::fs::read(dir.path().join("_quarantine/r/a")).unwrap();
        assert_eq!(body, b"new", "rename overwrites target");
    }
}
