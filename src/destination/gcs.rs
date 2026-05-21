use std::path::Path;
use std::sync::Arc;

use opendal::Operator;
use opendal::blocking;
use opendal::layers::RetryLayer;
use opendal::services::Gcs;

use super::gcs_auth;
use crate::config::DestinationConfig;
use crate::error::Result;

pub struct GcsDestination {
    _runtime: Arc<tokio::runtime::Runtime>,
    op: blocking::Operator,
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

        if config.allow_anonymous {
            builder = builder
                .allow_anonymous()
                .disable_vm_metadata()
                .disable_config_load();
            log::info!("GCS: allow_anonymous (emulator mode; no OAuth / service account)");
        } else if let Some(cred_file) = &config.credentials_file {
            builder = builder.credential_path(cred_file);
            log::info!("GCS: using credentials_file from config: {}", cred_file);
        } else if let Some(token) = gcs_auth::try_authorized_user_token()? {
            builder = builder.disable_vm_metadata().token(token);
            log::info!("GCS: using access token from ADC authorized_user credentials");
        } else {
            log::info!(
                "GCS: using Google default credential chain \
                 (service account JSON via GOOGLE_APPLICATION_CREDENTIALS, then VM metadata)"
            );
        }

        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .map_err(|e| anyhow::anyhow!("failed to create tokio runtime for GCS: {}", e))?,
        );
        let _guard = runtime.enter();

        // OpenDAL's `RetryLayer` retries individual HTTP calls on hyper /
        // reqwest transient failures (`dispatch task is gone`, server-side
        // 5xx, partial-upload disconnects, …) without re-running the whole
        // chunk through the source.  The chunk worker's outer retry loop is
        // still the safety net for harder failures (auth, region issues,
        // SQL retries) — this just stops a single TCP blip from poisoning
        // a 5 MB streaming upload that otherwise costs us another full
        // SQL fetch + parquet encode.
        let async_op = Operator::new(builder)?
            .layer(
                RetryLayer::new()
                    .with_max_times(5)
                    .with_min_delay(std::time::Duration::from_millis(200))
                    .with_max_delay(std::time::Duration::from_secs(10))
                    .with_jitter(),
            )
            .finish();
        let op = blocking::Operator::new(async_op)?;

        let prefix = config.prefix.clone().unwrap_or_default();

        Ok(Self {
            _runtime: runtime,
            op,
            prefix,
        })
    }
}

impl super::Destination for GcsDestination {
    fn write(&self, local_path: &Path, remote_key: &str) -> Result<()> {
        let key = format!("{}{}", self.prefix, remote_key);
        let mut src = std::fs::File::open(local_path)?;
        let mut dst = self.op.writer(&key)?.into_std_write();
        std::io::copy(&mut src, &mut dst)?;
        dst.close()?;
        log::info!("uploaded gs://{}", key);
        Ok(())
    }

    fn capabilities(&self) -> super::DestinationCapabilities {
        super::DestinationCapabilities {
            commit_protocol: super::WriteCommitProtocol::FinalizeOnClose,
            idempotent_overwrite: true,
            retry_safe: true,
            partial_write_risk: false,
        }
    }

    // ── ADR-0013 read surface (delegates to opendal) ─────────────────────
    //
    // Identical implementation shape to S3; opendal abstracts the
    // backend-specific listing semantics.  Trailing slash required for
    // directory listings (mirrored from blocking::Operator::list docs).

    fn list_prefix(&self, prefix: &str) -> Result<Vec<super::ObjectMeta>> {
        let full = format!("{}{}", self.prefix, prefix);
        let listed = if full.is_empty() || full.ends_with('/') {
            self.op.list_options(
                &full,
                opendal::options::ListOptions {
                    recursive: true,
                    ..Default::default()
                },
            )?
        } else {
            self.op.list_options(
                &format!("{}/", full),
                opendal::options::ListOptions {
                    recursive: true,
                    ..Default::default()
                },
            )?
        };
        let mut out = Vec::with_capacity(listed.len());
        for entry in listed {
            if entry.metadata().mode() != opendal::EntryMode::FILE {
                continue;
            }
            let abs = entry.path().to_string();
            let rel = abs
                .strip_prefix(self.prefix.as_str())
                .unwrap_or(abs.as_str())
                .to_string();
            out.push(super::ObjectMeta {
                key: rel,
                size_bytes: entry.metadata().content_length(),
            });
        }
        Ok(out)
    }

    fn read(&self, key: &str) -> Result<Vec<u8>> {
        let full = format!("{}{}", self.prefix, key);
        let buf = self.op.read(&full)?;
        Ok(buf.to_vec())
    }

    fn head(&self, key: &str) -> Result<Option<super::ObjectMeta>> {
        let full = format!("{}{}", self.prefix, key);
        match self.op.stat(&full) {
            Ok(meta) => Ok(Some(super::ObjectMeta {
                key: key.to_string(),
                size_bytes: meta.content_length(),
            })),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn r#move(&self, from: &str, to: &str) -> Result<()> {
        // GCS rewrite (server-side copy) + delete; opendal abstracts this.
        let from_full = format!("{}{}", self.prefix, from);
        let to_full = format!("{}{}", self.prefix, to);
        self.op.rename(&from_full, &to_full)?;
        Ok(())
    }
}
