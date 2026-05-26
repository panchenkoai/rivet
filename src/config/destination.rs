//! Output destination: cloud-bucket / local-path / stdout, with per-cloud auth.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct DestinationConfig {
    #[serde(rename = "type")]
    pub destination_type: DestinationType,
    pub bucket: Option<String>,
    pub prefix: Option<String>,
    pub path: Option<String>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub credentials_file: Option<String>,
    pub access_key_env: Option<String>,
    pub secret_key_env: Option<String>,
    /// Name of an env var holding an AWS STS session token, for use with
    /// short-lived credentials issued by AWS IAM Identity Center / SSO,
    /// `aws sts assume-role`, MFA-protected sessions, EKS IAM Roles for
    /// Service Accounts, etc.  Pair with `access_key_env` + `secret_key_env`.
    /// See `docs/cloud-auth.md` for the AWS auth-flow matrix.
    pub session_token_env: Option<String>,
    pub aws_profile: Option<String>,
    /// Azure storage account name (the prefix in `<account>.blob.core.windows.net`).
    /// Plain string — not a secret. Pair with `account_key_env`.
    /// See `docs/cloud-auth.md` for the Azure auth-flow matrix.
    pub account_name: Option<String>,
    /// Name of an env var holding the Azure Storage account key.  Treated as
    /// a credential and wiped from heap on drop — same SecOps treatment as
    /// `access_key_env`.  Pair with `account_name`.  Mutually exclusive with
    /// `sas_token_env`.
    pub account_key_env: Option<String>,
    /// Name of an env var holding an Azure Storage **SAS token** — typically
    /// a short-lived, scope-limited credential issued out-of-band (Azure
    /// portal / `az storage container generate-sas` / Azure SDK).  Use this
    /// instead of `account_key_env` when the operator does not have the
    /// long-lived account key or wants per-job scoped access.  Pair with
    /// `account_name`.  Mutually exclusive with `account_key_env`.
    ///
    /// The token value is wiped from heap on drop via the same
    /// `Zeroizing<String>` wrapper as `account_key_env`.  Leading `?` is
    /// trimmed transparently so the operator can paste either the full
    /// `?sv=…&sig=…` query string or the raw token body.
    pub sas_token_env: Option<String>,
    #[serde(default)]
    pub allow_anonymous: bool,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum DestinationType {
    #[default]
    Local,
    S3,
    Gcs,
    Azure,
    Stdout,
}

impl DestinationType {
    /// Stable lowercase string label for persistence and display.
    pub fn label(self) -> &'static str {
        match self {
            DestinationType::Local => "local",
            DestinationType::S3 => "s3",
            DestinationType::Gcs => "gcs",
            DestinationType::Azure => "azure",
            DestinationType::Stdout => "stdout",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn destination_type_labels_stable() {
        assert_eq!(DestinationType::Local.label(), "local");
        assert_eq!(DestinationType::S3.label(), "s3");
        assert_eq!(DestinationType::Gcs.label(), "gcs");
        assert_eq!(DestinationType::Azure.label(), "azure");
        assert_eq!(DestinationType::Stdout.label(), "stdout");
    }
}
