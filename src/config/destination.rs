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
    /// Cap on the RAM one-shot (single-PUT) upload buffers may hold, in MB
    /// (default 64; cloud destinations only). A one-shot PUT buffers the whole part;
    /// on GCS and Azure the store then records a `Content-MD5` that `validate` checks,
    /// and on every store it is one request instead of a sequential multipart (S3
    /// verifies size-only either way). A part that does not fit the remaining budget
    /// streams instead (memory-bounded). `0` streams every non-empty part.
    /// Each distinct value is one pool per rivet process, shared by every destination
    /// configured with it (including each table of a CDC export). Different values
    /// are separate pools, so worst-case one-shot RAM is the sum of the distinct
    /// values in use; under `parallel_export_processes` every child has its own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oneshot_budget_mb: Option<u64>,
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

impl DestinationConfig {
    /// The destination as an operator would type it to find the prefix again: `file://`, `s3://`, `gs://`, `az://<container>/` or `stdout`.
    pub fn uri(&self) -> String {
        let in_bucket = |scheme: &str| {
            format!(
                "{scheme}://{}/{}",
                self.bucket.as_deref().unwrap_or(""),
                self.prefix.as_deref().unwrap_or("")
            )
        };
        match self.destination_type {
            DestinationType::Local => {
                let path = self.path.as_deref().or(self.prefix.as_deref());
                format!("file://{}", path.unwrap_or("."))
            }
            DestinationType::S3 => in_bucket("s3"),
            DestinationType::Gcs => in_bucket("gs"),
            DestinationType::Azure => in_bucket("az"),
            DestinationType::Stdout => "stdout".to_string(),
        }
    }

    /// The state key of a CDC table's snapshot baseline: its destination.
    pub fn state_key(&self) -> String {
        format!(
            "{}/{}",
            self.bucket.as_deref().unwrap_or(""),
            self.path
                .as_deref()
                .or(self.prefix.as_deref())
                .unwrap_or("")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_destination_state_key_is_its_bucket_and_path_or_prefix() {
        let mut d = DestinationConfig {
            bucket: Some("b".into()),
            prefix: Some("p/".into()),
            ..Default::default()
        };
        assert_eq!(d.state_key(), "b/p/");
        d.path = Some("out/".into());
        assert_eq!(d.state_key(), "b/out/");
    }

    #[test]
    fn a_destination_uri_names_its_store_with_its_bucket_and_prefix_or_its_path() {
        use DestinationType::*;
        let at =
            |destination_type, bucket: Option<&str>, prefix: Option<&str>, path: Option<&str>| {
                DestinationConfig {
                    destination_type,
                    bucket: bucket.map(Into::into),
                    prefix: prefix.map(Into::into),
                    path: path.map(Into::into),
                    ..Default::default()
                }
                .uri()
            };
        assert_eq!(at(Local, None, Some("p/"), Some("/out")), "file:///out");
        assert_eq!(at(Local, None, Some("p/"), None), "file://p/");
        assert_eq!(at(Local, None, None, None), "file://.");
        assert_eq!(at(S3, Some("b"), Some("p/"), Some("/out")), "s3://b/p/");
        assert_eq!(at(S3, Some("b"), None, None), "s3://b/");
        assert_eq!(at(Gcs, Some("b"), Some("p/"), None), "gs://b/p/");
        assert_eq!(at(Gcs, Some("b"), None, None), "gs://b/");
        assert_eq!(at(Azure, Some("c"), Some("p/"), None), "az://c/p/");
        assert_eq!(at(Azure, Some("c"), None, None), "az://c/");
        assert_eq!(at(Stdout, Some("b"), Some("p/"), Some("/out")), "stdout");
    }

    #[test]
    fn destination_type_labels_stable() {
        assert_eq!(DestinationType::Local.label(), "local");
        assert_eq!(DestinationType::S3.label(), "s3");
        assert_eq!(DestinationType::Gcs.label(), "gcs");
        assert_eq!(DestinationType::Azure.label(), "azure");
        assert_eq!(DestinationType::Stdout.label(), "stdout");
    }
}
