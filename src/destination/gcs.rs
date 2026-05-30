use opendal::Operator;
use opendal::services::Gcs;

use super::cloud::{CloudBackend, CloudDestination};
use super::gcs_auth;
use crate::config::DestinationConfig;
use crate::error::Result;

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
        } else if let Some(token) = gcs_auth::try_authorized_user_token()? {
            builder = builder.disable_vm_metadata().token(token);
            log::info!("GCS: using access token from ADC authorized_user credentials");
        } else {
            log::info!(
                "GCS: using Google default credential chain \
                 (service account JSON via GOOGLE_APPLICATION_CREDENTIALS, then VM metadata)"
            );
        }

        Ok(Operator::new(builder)?.finish())
    }
}
