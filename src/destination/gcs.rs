use opendal::Operator;
use opendal::services::Gcs;
use std::sync::Arc;

use super::cloud::{CloudBackend, CloudDestination};
use super::gcs_auth;
use crate::config::DestinationConfig;
use crate::error::Result;

/// Build the async GCS [`Operator`] for `config`, using the same auth chain
/// (ADC refreshing loader / credentials_file / anonymous emulator) as the
/// streaming export destination. Reused by the load layer's one-off object ops
/// so they never shell out to `gcloud` and never hand-roll a second auth path.
pub(crate) fn operator_for(config: &DestinationConfig) -> Result<Operator> {
    GcsBackend::build_operator(config)
}

/// A blocking GCS handle for the load layer's one-off object ops — recursive
/// list (manifests / parquet), read (manifest bytes), and recursive delete
/// (source cleanup). Mirrors [`CloudDestination`]'s runtime + blocking wrap,
/// but exposes the read/list/delete surface the streaming `Destination` trait
/// does not. Holds the runtime the blocking operator drives.
pub(crate) struct GcsStore {
    _runtime: Arc<tokio::runtime::Runtime>,
    op: opendal::blocking::Operator,
}

impl GcsStore {
    /// Build a blocking GCS store for `config`'s bucket. Paths passed to the
    /// methods below are **bucket-relative** (no `gs://bucket/` prefix).
    pub(crate) fn new(config: &DestinationConfig) -> Result<Self> {
        Self::wrap(operator_for(config)?)
    }

    fn wrap(async_op: Operator) -> Result<Self> {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| anyhow::anyhow!("failed to create tokio runtime for GCS ops: {e}"))?,
        );
        let _guard = runtime.enter();
        let op = opendal::blocking::Operator::new(async_op)?;
        Ok(Self {
            _runtime: runtime,
            op,
        })
    }

    /// Bucket-root-relative keys of every FILE recursively under `path`.
    pub(crate) fn list_files(&self, path: &str) -> Result<Vec<String>> {
        let dir = if path.is_empty() || path.ends_with('/') {
            path.to_string()
        } else {
            format!("{path}/")
        };
        let listed = self.op.list_options(
            &dir,
            opendal::options::ListOptions {
                recursive: true,
                ..Default::default()
            },
        )?;
        Ok(listed
            .into_iter()
            .filter(|e| e.metadata().mode() == opendal::EntryMode::FILE)
            .map(|e| e.path().to_string())
            .collect())
    }

    /// Raw bytes of the object at the bucket-relative `path`.
    pub(crate) fn read(&self, path: &str) -> Result<Vec<u8>> {
        Ok(self.op.read(path)?.to_vec())
    }

    /// Recursively delete everything under the bucket-relative `path`.
    pub(crate) fn remove_all(&self, path: &str) -> Result<()> {
        self.op.remove_all(path)?;
        Ok(())
    }

    /// A store backed by a local filesystem root — for offline tests of the
    /// load layer's list/read/delete logic without a live bucket.
    #[cfg(test)]
    pub(crate) fn open_fs(root: &str) -> Result<Self> {
        Self::wrap(Operator::new(opendal::services::Fs::default().root(root))?.finish())
    }
}

/// GCS object-store destination. The retry policy, blocking wrap, and ADR-0013
/// read surface live in [`CloudDestination`]; this type only knows how to
/// authenticate against Google Cloud Storage.
pub type GcsDestination = CloudDestination<GcsBackend>;

/// Zero-sized backend marker carrying GCS's operator construction.
pub struct GcsBackend;

impl CloudBackend for GcsBackend {
    const RUNTIME_LABEL: &'static str = "GCS";
    const SCHEME: &'static str = "gs";

    fn build_operator(config: &DestinationConfig) -> Result<Operator> {
        let bucket = config
            .bucket
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("GCS destination requires 'bucket'"))?;

        let mut builder = Gcs::default().bucket(bucket);

        if let Some(endpoint) = &config.endpoint {
            builder = builder.endpoint(endpoint);
        }

        if config.allow_anonymous {
            builder = builder
                .allow_anonymous()
                .disable_vm_metadata()
                .disable_config_load();
            log::info!("GCS: allow_anonymous (emulator mode; no OAuth / service account)");
        } else if let Some(cred_file) = &config.credentials_file {
            builder = builder.credential_path(cred_file);
            log::info!("GCS: using credentials_file from config: {}", cred_file);
        } else if let Some(loader) = gcs_auth::try_authorized_user_loader()? {
            // A refreshing loader, not a static `.token()`: opendal pins a
            // static token with a usize::MAX expiry, so exports longer than
            // the ~1h ADC token TTL would 401 mid-run, non-retryably.
            builder = builder
                .disable_vm_metadata()
                .customized_token_loader(Box::new(loader));
            log::info!(
                "GCS: using ADC authorized_user credentials (access token auto-refreshes before expiry)"
            );
        } else {
            log::info!(
                "GCS: using Google default credential chain \
                 (service account JSON via GOOGLE_APPLICATION_CREDENTIALS, then VM metadata)"
            );
        }

        Ok(Operator::new(builder)?.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Write `bytes` to `root/rel`, creating parent dirs — a stand-in for objects
    /// landing under a bucket prefix.
    fn write_at(root: &std::path::Path, rel: &str, bytes: &[u8]) {
        let p = root.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, bytes).unwrap();
    }

    #[test]
    fn list_files_is_recursive_file_only_and_bucket_relative() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write_at(root, "base/a.parquet", b"a");
        write_at(root, "base/b.parquet", b"b");
        write_at(root, "base/sub/c.parquet", b"c");
        write_at(root, "base/manifest.json", b"{}");
        write_at(root, "other/d.parquet", b"d"); // outside `base` — must not appear

        let store = GcsStore::open_fs(root.to_str().unwrap()).unwrap();
        let mut got = store.list_files("base").unwrap();
        got.sort();
        assert_eq!(
            got,
            vec![
                "base/a.parquet".to_string(),
                "base/b.parquet".to_string(),
                "base/manifest.json".to_string(),
                "base/sub/c.parquet".to_string(),
            ],
            "every file under the prefix, recursively, keyed bucket-relative — dirs excluded, siblings excluded"
        );

        // Real callers pass a trailing-slash prefix (`gs://bucket/base/`); it must
        // list identically. Pins the `is_empty() || ends_with('/')` guard — an
        // `&&` there would append a second slash (`base//`) and match nothing.
        let mut with_slash = store.list_files("base/").unwrap();
        with_slash.sort();
        assert_eq!(
            with_slash, got,
            "a trailing-slash prefix lists the same files"
        );
    }

    #[test]
    fn read_returns_the_object_bytes() {
        let dir = tempfile::tempdir().unwrap();
        write_at(dir.path(), "p/hello.bin", b"payload");
        let store = GcsStore::open_fs(dir.path().to_str().unwrap()).unwrap();
        assert_eq!(store.read("p/hello.bin").unwrap(), b"payload");
    }

    #[test]
    fn remove_all_recursively_empties_the_prefix_and_spares_siblings() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write_at(root, "p/a.parquet", b"a");
        write_at(root, "p/sub/b.parquet", b"b");
        write_at(root, "keep/c.parquet", b"c");
        let store = GcsStore::open_fs(root.to_str().unwrap()).unwrap();

        // `delete_prefix` passes the bucket-relative prefix with no trailing
        // slash — the recursive delete must still drain the whole subtree.
        store.remove_all("p").unwrap();
        assert!(
            store.list_files("p").unwrap().is_empty(),
            "the prefix subtree is fully drained"
        );
        assert_eq!(
            store.list_files("keep").unwrap(),
            vec!["keep/c.parquet".to_string()],
            "objects outside the prefix are untouched"
        );
    }
}
