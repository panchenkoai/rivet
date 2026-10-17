//! Output destination: cloud-bucket / local-path / stdout, with per-cloud auth.

use std::path::{Path, PathBuf};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, JsonSchema, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct DestinationConfig {
    #[serde(rename = "type")]
    pub destination_type: DestinationType,
    pub bucket: Option<String>,
    pub prefix: Option<String>,
    /// Local directory for the output files. A relative path is anchored to the directory of
    /// the config file, whatever directory rivet is started from (since 0.32; before, it was
    /// relative to the working directory).
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

    /// Anchor a relative local `path` (or the `prefix` a local destination uses as one) to `config_dir`, so one config names one directory from every working directory; returns the relative spelling it anchored.
    pub(crate) fn anchor(&mut self, config_dir: &Path) -> Option<String> {
        if self.destination_type != DestinationType::Local {
            return None;
        }
        let p = self.path.as_mut().or(self.prefix.as_mut())?;
        if Path::new(p.as_str()).is_absolute() {
            return None;
        }
        let written = p.clone();
        *p = normalized(&config_dir.join(p.as_str()))
            .to_string_lossy()
            .into_owned();
        Some(written)
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

/// Whether `written` is the working-directory-relative `file://` URI a run before 0.32 recorded and `anchored` the same directory as [`DestinationConfig::anchor`] names it.
pub(crate) fn anchors(written: &str, anchored: &str) -> bool {
    let (Some(w), Some(a)) = (
        written.strip_prefix("file://"),
        anchored.strip_prefix("file://"),
    ) else {
        return false;
    };
    let tail: PathBuf = Path::new(w)
        .components()
        .skip_while(|c| *c == std::path::Component::CurDir)
        .collect();
    !Path::new(w).is_absolute()
        && Path::new(a).is_absolute()
        && tail.components().next().is_some()
        && Path::new(a).ends_with(&tail)
}

/// `path` with its `.` and `..` components folded lexically: one spelling of a directory for the files rivet names and compares.
pub(crate) fn normalized(path: &Path) -> PathBuf {
    use std::path::Component;
    let mut out = PathBuf::new();
    for c in path.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir
                if matches!(out.components().next_back(), Some(Component::Normal(_))) =>
            {
                out.pop();
            }
            c => out.push(c),
        }
    }
    out
}

/// `path` up to its first `{placeholder}` component, normalised: the directory a templated destination lands under.
pub(crate) fn literal_root(path: &Path) -> PathBuf {
    normalized(path)
        .components()
        .take_while(|c| !c.as_os_str().to_string_lossy().contains('{'))
        .collect()
}

/// The transition decision for a relative local path: `old` (under the working directory, where a release before 0.32 wrote) holds `export`'s files while `new` (beside the config) holds none; local stat and reads only, never the same directory.
pub(crate) fn relative_path_moved(export: &str, old: &Path, new: &Path) -> bool {
    old != new && holds_export(old, export) && !holds_export(new, export)
}

/// Whether `dir` holds a `manifest.json` rivet wrote for `export`, at the directory or one level down (a date or table sub-directory).
fn holds_export(dir: &Path, export: &str) -> bool {
    let manifest_of = |d: &Path| {
        std::fs::read(d.join("manifest.json"))
            .ok()
            .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
            .is_some_and(|m| m.get("export_name").and_then(|v| v.as_str()) == Some(export))
    };
    manifest_of(dir)
        || std::fs::read_dir(dir)
            .ok()
            .into_iter()
            .flatten()
            .flatten()
            .any(|e| e.file_type().is_ok_and(|t| t.is_dir()) && manifest_of(&e.path()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The four cases of the transition decision: the same directory, the old place alone holds the export, both do, neither does; another export's manifest does not count.
    #[test]
    fn a_relative_path_has_moved_only_when_the_old_place_alone_holds_the_export() {
        let root = tempfile::tempdir().unwrap();
        let (old, new) = (root.path().join("cwd/out"), root.path().join("cfg/out"));
        let plant = |dir: &Path, export: &str| {
            std::fs::create_dir_all(dir).unwrap();
            std::fs::write(
                dir.join("manifest.json"),
                format!(r#"{{"export_name":"{export}","run_id":"r"}}"#),
            )
            .unwrap();
        };
        assert!(!relative_path_moved("orders", &old, &new), "neither holds");
        plant(&old.join("2026-10-16"), "orders");
        assert!(
            relative_path_moved("orders", &old, &new),
            "the old place alone holds it (one level down)"
        );
        assert!(
            !relative_path_moved("items", &old, &new),
            "another export's manifest is not this export's"
        );
        assert!(
            !relative_path_moved("orders", &old, &old),
            "the same directory"
        );
        plant(&new, "orders");
        assert!(!relative_path_moved("orders", &old, &new), "both hold");
        assert_eq!(
            literal_root(Path::new("/etc/rivet/out/{date}/x")),
            Path::new("/etc/rivet/out")
        );
        assert_eq!(
            normalized(Path::new("/home/op/../etc/rivet/./out/..")),
            Path::new("/home/etc/rivet")
        );
        assert_eq!(
            normalized(Path::new("../out/./x")),
            Path::new("../out/x"),
            "a leading `..` has nothing to fold into and stays"
        );
    }

    /// The one resolver refuses only from ANOTHER working directory whose place holds the export: from the config's own directory the same files are the destination itself.
    #[test]
    fn a_config_loaded_from_another_directory_is_refused_while_that_directory_holds_the_export() {
        let root = tempfile::tempdir().unwrap();
        let (cwd, cfg_dir) = (root.path().join("cwd"), root.path().join("cfg"));
        std::fs::create_dir_all(cwd.join("out")).unwrap();
        std::fs::create_dir_all(&cfg_dir).unwrap();
        std::fs::write(
            cwd.join("out/manifest.json"),
            r#"{"export_name":"orders","run_id":"r"}"#,
        )
        .unwrap();
        let config_path = cfg_dir.join("rivet.yaml");
        let load = || {
            crate::config::Config::from_yaml(
                "source: { type: postgres, url: \"postgresql://u:p@127.0.0.1:1/db\" }\n\
                 exports:\n  - name: orders\n    table: orders\n    mode: full\n    format: parquet\n\
                 \x20   destination: { type: local, path: ./out }\n",
            )
            .unwrap()
        };
        let err = load()
            .anchor_paths(config_path.to_str().unwrap(), Some(cwd.clone()))
            .unwrap_err();
        crate::error::assert_coded(&err, "RIVET_DEST_RELATIVE_PATH_MOVED", 5);
        let said = format!("{err:#}");
        for dir in [&cwd.join("out"), &cfg_dir.join("out")] {
            assert!(said.contains(dir.to_str().unwrap()), "{said}");
        }
        load()
            .anchor_paths(config_path.to_str().unwrap(), Some(cfg_dir.clone()))
            .expect("from the config's own directory nothing is checked");
        load()
            .anchor_paths(config_path.to_str().unwrap(), None)
            .expect("no working directory, nothing to compare");
    }

    /// The one resolver: a relative local path lands under the CONFIG's directory, not the working directory the test runs in.
    #[test]
    fn a_relative_local_path_is_anchored_to_the_config_directory() {
        let elsewhere = tempfile::tempdir().unwrap();
        let config_dir = elsewhere.path();
        assert_ne!(std::env::current_dir().unwrap(), config_dir);
        let local = |path: Option<&str>, prefix: Option<&str>| DestinationConfig {
            path: path.map(Into::into),
            prefix: prefix.map(Into::into),
            ..Default::default()
        };
        let mut d = local(Some("./out/{date}/"), None);
        assert_eq!(d.anchor(config_dir).as_deref(), Some("./out/{date}/"));
        assert_eq!(
            d.path.as_deref(),
            Some(config_dir.join("out/{date}").to_str().unwrap())
        );
        let mut as_prefix = local(None, Some("out"));
        as_prefix.anchor(config_dir);
        assert_eq!(
            as_prefix.prefix.as_deref(),
            Some(config_dir.join("out").to_str().unwrap())
        );
        let mut absolute = local(Some("/data/out"), None);
        assert_eq!(absolute.anchor(config_dir), None);
        assert_eq!(absolute.path.as_deref(), Some("/data/out"));
        let mut cloud = DestinationConfig {
            destination_type: DestinationType::S3,
            bucket: Some("b".into()),
            prefix: Some("exports/".into()),
            ..Default::default()
        };
        cloud.anchor(config_dir);
        assert_eq!(cloud.prefix.as_deref(), Some("exports/"));
    }

    /// `Config::load` is where every command anchors: a config in another directory, loaded from this one, names its own directory.
    #[test]
    fn a_loaded_config_anchors_its_relative_local_paths_once() {
        let elsewhere = tempfile::tempdir().unwrap();
        let config_path = elsewhere.path().join("rivet.yaml");
        std::fs::write(
            &config_path,
            "source: { type: postgres, url: \"postgresql://u:p@127.0.0.1:1/db\" }\n\
             exports:\n\
             \x20 - name: orders\n    table: orders\n    mode: full\n    format: parquet\n\
             \x20   destination: { type: local, path: ./out/orders }\n\
             \x20 - name: feed\n    table: feed\n    mode: full\n    format: csv\n\
             \x20   destination: { type: s3, bucket: b, prefix: feed/, region: us-east-1 }\n",
        )
        .unwrap();
        let cfg = crate::config::Config::load(config_path.to_str().unwrap()).unwrap();
        let anchored = elsewhere.path().join("out/orders");
        assert_eq!(
            cfg.exports[0].destination.path.as_deref(),
            anchored.to_str()
        );
        assert_eq!(
            cfg.exports[0].destination.uri(),
            format!("file://{}", anchored.display())
        );
        assert_eq!(cfg.exports[1].destination.prefix.as_deref(), Some("feed/"));
    }

    #[test]
    fn a_legacy_relative_spelling_anchors_to_its_absolute_form_only() {
        assert!(anchors(
            "file://./out/orders/",
            "file:///etc/rivet/out/orders"
        ));
        assert!(anchors("file://out/{date}", "file:///etc/rivet/out/{date}"));
        assert!(!anchors(
            "file://./out/orders",
            "file:///etc/rivet/out/items"
        ));
        assert!(!anchors("file:///etc/rivet/out", "file:///etc/rivet/out"));
        assert!(!anchors("file://./out", "file://./out"));
        assert!(!anchors("file://.", "file:///etc/rivet"));
        assert!(!anchors("s3://b/out", "file:///etc/rivet/out"));
    }

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
