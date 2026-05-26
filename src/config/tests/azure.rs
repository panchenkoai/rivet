//! Azure Blob Storage destination config — v0.7.2 P0.4 validation rules.

use super::*;

const AZURE_BASE: &str = r#"
source:
  type: postgres
  url: "postgresql://localhost/test"
exports:
  - name: t
    query: "SELECT 1"
    format: parquet
    destination:
      type: azure
      bucket: mycontainer
"#;

#[test]
fn azure_sas_token_env_accepted() {
    let yaml = format!(
        "{AZURE_BASE}      account_name: mystorageacct\n      sas_token_env: AZURE_STORAGE_SAS_TOKEN\n"
    );
    let cfg = Config::from_yaml(&yaml).unwrap();
    assert_eq!(
        cfg.exports[0].destination.sas_token_env.as_deref(),
        Some("AZURE_STORAGE_SAS_TOKEN")
    );
}

#[test]
fn azure_account_key_env_accepted() {
    let yaml = format!(
        "{AZURE_BASE}      account_name: mystorageacct\n      account_key_env: AZURE_STORAGE_KEY\n"
    );
    assert!(Config::from_yaml(&yaml).is_ok());
}

#[test]
fn azure_missing_account_name_rejected() {
    let yaml = format!("{AZURE_BASE}      sas_token_env: AZURE_STORAGE_SAS_TOKEN\n");
    let err = Config::from_yaml(&yaml).unwrap_err();
    assert!(
        err.to_string().contains("account_name"),
        "missing account_name must be reported: {err}"
    );
}

#[test]
fn azure_missing_auth_rejected() {
    let yaml = format!("{AZURE_BASE}      account_name: mystorageacct\n");
    let err = Config::from_yaml(&yaml).unwrap_err();
    assert!(
        err.to_string().contains("account_key_env") && err.to_string().contains("sas_token_env"),
        "error must name both auth options: {err}"
    );
}

#[test]
fn azure_both_key_and_sas_rejected() {
    let yaml = format!(
        "{AZURE_BASE}      account_name: mystorageacct\n      account_key_env: K\n      sas_token_env: S\n"
    );
    let err = Config::from_yaml(&yaml).unwrap_err();
    assert!(
        err.to_string().contains("mutually exclusive"),
        "error must say mutually exclusive: {err}"
    );
}

#[test]
fn azure_allow_anonymous_accepted() {
    let yaml = format!(
        "{AZURE_BASE}      endpoint: http://127.0.0.1:10000/devstoreaccount1\n      allow_anonymous: true\n"
    );
    assert!(Config::from_yaml(&yaml).is_ok());
}

#[test]
fn azure_allow_anonymous_with_sas_rejected() {
    let yaml = format!(
        "{AZURE_BASE}      endpoint: http://127.0.0.1:10000/devstoreaccount1\n      allow_anonymous: true\n      sas_token_env: S\n"
    );
    let err = Config::from_yaml(&yaml).unwrap_err();
    assert!(
        err.to_string().contains("allow_anonymous"),
        "error must mention allow_anonymous: {err}"
    );
}
