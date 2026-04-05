use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::Deserialize;

const GOOGLE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";

#[derive(Deserialize)]
struct AdcFile {
    #[serde(rename = "type")]
    cred_type: String,
    client_id: Option<String>,
    client_secret: Option<String>,
    refresh_token: Option<String>,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

/// Looks for `authorized_user` ADC credentials and returns a fresh access token.
///
/// Returns `Ok(None)` when the well-known ADC file is absent or is not `authorized_user` type
/// (i.e. the caller should let OpenDAL handle credentials normally).
pub fn try_authorized_user_token() -> Result<Option<String>> {
    let path = match adc_path() {
        Some(p) if p.exists() => p,
        _ => return Ok(None),
    };

    let data = std::fs::read_to_string(&path)
        .with_context(|| format!("reading ADC file {}", path.display()))?;
    let fields =
        parse_adc_file(&data).with_context(|| format!("parsing ADC file {}", path.display()))?;
    let (client_id, client_secret, refresh_token) = match fields {
        Some(f) => f,
        None => return Ok(None),
    };

    log::info!("GCS: refreshing access token from ADC authorized_user credentials");

    let body = format!(
        "grant_type=refresh_token&client_id={}&client_secret={}&refresh_token={}",
        urlenc(&client_id),
        urlenc(&client_secret),
        urlenc(&refresh_token),
    );

    let client = reqwest::blocking::Client::new();
    let resp = client
        .post(GOOGLE_TOKEN_URL)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .context("ADC token refresh request failed")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().unwrap_or_default();
        anyhow::bail!("ADC token refresh failed (HTTP {}): {}", status, body);
    }

    let token: TokenResponse = resp.json().context("parsing token response")?;
    Ok(Some(token.access_token))
}

fn urlenc(s: &str) -> String {
    s.bytes()
        .flat_map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![b as char]
            }
            _ => format!("%{:02X}", b).chars().collect(),
        })
        .collect()
}

pub(crate) fn adc_path() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
        return Some(PathBuf::from(p));
    }

    let config_dir = if let Ok(v) = std::env::var("APPDATA") {
        PathBuf::from(v)
    } else if let Ok(v) = std::env::var("XDG_CONFIG_HOME") {
        PathBuf::from(v)
    } else if let Ok(v) = std::env::var("HOME") {
        PathBuf::from(v).join(".config")
    } else {
        return None;
    };

    Some(
        config_dir
            .join("gcloud")
            .join("application_default_credentials.json"),
    )
}

/// Parse ADC JSON and validate fields without making a network request.
/// Returns `Ok(None)` when the file is not `authorized_user` type.
pub(crate) fn parse_adc_file(data: &str) -> Result<Option<(String, String, String)>> {
    let adc: AdcFile = serde_json::from_str(data).context("parsing ADC JSON")?;
    if adc.cred_type != "authorized_user" {
        return Ok(None);
    }
    let client_id = adc
        .client_id
        .ok_or_else(|| anyhow::anyhow!("ADC authorized_user: missing client_id"))?;
    let client_secret = adc
        .client_secret
        .ok_or_else(|| anyhow::anyhow!("ADC authorized_user: missing client_secret"))?;
    let refresh_token = adc
        .refresh_token
        .ok_or_else(|| anyhow::anyhow!("ADC authorized_user: missing refresh_token"))?;
    Ok(Some((client_id, client_secret, refresh_token)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urlenc_basic() {
        assert_eq!(urlenc("hello"), "hello");
        assert_eq!(urlenc("a b"), "a%20b");
        assert_eq!(urlenc("foo@bar.com"), "foo%40bar.com");
    }

    #[test]
    fn adc_path_uses_home_fallback() {
        let p = adc_path();
        // On a dev machine HOME is almost always set; just verify the function doesn't panic.
        // The path should end with the well-known gcloud file or come from GOOGLE_APPLICATION_CREDENTIALS.
        assert!(p.is_some() || std::env::var("HOME").is_err());
    }

    #[test]
    fn parse_adc_authorized_user_ok() {
        let json = r#"{
            "type": "authorized_user",
            "client_id": "cid",
            "client_secret": "csec",
            "refresh_token": "rtoken"
        }"#;
        let result = parse_adc_file(json).unwrap();
        assert_eq!(result, Some(("cid".into(), "csec".into(), "rtoken".into())));
    }

    #[test]
    fn parse_adc_service_account_returns_none() {
        let json = r#"{"type": "service_account", "project_id": "p"}"#;
        let result = parse_adc_file(json).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn parse_adc_missing_client_id_errors() {
        let json = r#"{
            "type": "authorized_user",
            "client_secret": "csec",
            "refresh_token": "rtoken"
        }"#;
        let err = parse_adc_file(json).unwrap_err();
        assert!(err.to_string().contains("client_id"), "got: {err}");
    }

    #[test]
    fn parse_adc_missing_refresh_token_errors() {
        let json = r#"{
            "type": "authorized_user",
            "client_id": "cid",
            "client_secret": "csec"
        }"#;
        let err = parse_adc_file(json).unwrap_err();
        assert!(err.to_string().contains("refresh_token"), "got: {err}");
    }

    #[test]
    fn parse_adc_invalid_json_errors() {
        let err = parse_adc_file("not json").unwrap_err();
        assert!(err.to_string().contains("parsing ADC JSON"), "got: {err}");
    }
}
