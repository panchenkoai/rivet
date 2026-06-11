use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use reqsign::{GoogleToken, GoogleTokenLoad};
use serde::Deserialize;
use zeroize::Zeroizing;

const GOOGLE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";

/// Scope stamped on minted tokens — opendal's default GCS scope (rivet never
/// overrides the builder's scope). Informational to reqsign's signer; the
/// refresh_token grant itself determines the actually granted scopes.
const GCS_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";

/// Remaining validity below which a cached access token is treated as stale
/// and the refresh_token grant is re-run. Wide enough that a multi-minute
/// part upload signed just before the check cannot outlive its token.
const REFRESH_THRESHOLD: Duration = Duration::from_secs(5 * 60);

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
    // RFC 6749 makes `expires_in` optional (Google always sends it). Default
    // 0 so an absent TTL reads as already-stale — refresh on every call —
    // never a token pinned with an invented lifetime.
    #[serde(default)]
    expires_in: u64,
}

/// Refreshing token source for opendal's `customized_token_loader` hook.
///
/// Holds the ADC `authorized_user` credentials and re-runs the refresh_token
/// grant whenever the cached access token is within [`REFRESH_THRESHOLD`] of
/// expiry, so an export of any duration never outlives its token. (A static
/// builder `.token()` is wrapped by opendal with a `usize::MAX` expiry —
/// never refreshed — so exports longer than the ~1h TTL would die mid-run
/// with 401s the RetryLayer cannot fix.)
pub(crate) struct AdcUserTokenLoader {
    client_id: String,
    // SecOps: long-lived secrets; heap zeroed on drop, never logged.
    client_secret: Zeroizing<String>,
    refresh_token: Zeroizing<String>,
    minted: Mutex<Option<MintedToken>>,
}

struct MintedToken {
    token: GoogleToken,
    minted_at: Instant,
    expires_in_secs: u64,
}

impl fmt::Debug for AdcUserTokenLoader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // SecOps: `GoogleTokenLoad` requires Debug; redact everything but the
        // (public) OAuth client id.
        f.debug_struct("AdcUserTokenLoader")
            .field("client_id", &self.client_id)
            .finish_non_exhaustive()
    }
}

impl AdcUserTokenLoader {
    fn cached_token(&self, now: Instant) -> Option<GoogleToken> {
        let cache = self.minted.lock().expect("ADC token cache poisoned");
        cache
            .as_ref()
            .filter(|c| token_still_fresh(c.minted_at, c.expires_in_secs, now))
            .map(|c| c.token.clone())
    }

    async fn mint_token(&self, client: &reqwest::Client) -> Result<GoogleToken> {
        log::info!("GCS: refreshing access token from ADC authorized_user credentials");

        // SecOps: the POST body carries `client_secret` and `refresh_token` in clear
        // form-urlencoded. Wrap so the heap buffer is zeroed after the request is
        // dispatched, and so accidental `Debug` logging of the builder does not leak.
        let body = Zeroizing::new(format!(
            "grant_type=refresh_token&client_id={}&client_secret={}&refresh_token={}",
            urlenc(&self.client_id),
            urlenc(&self.client_secret),
            urlenc(&self.refresh_token),
        ));

        let resp = client
            .post(GOOGLE_TOKEN_URL)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body.as_str().to_string())
            .send()
            .await
            .context("ADC token refresh request failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            // Do NOT surface the raw response body: Google's OAuth error responses
            // echo back the submitted `client_id` / `client_secret` in some failure
            // modes, which would end up in `summary.error_message` → SQLite / Slack.
            anyhow::bail!("ADC token refresh failed (HTTP {})", status);
        }

        let payload = resp.text().await.context("reading token response")?;
        let (access_token, expires_in) = parse_token_response(&payload)?;
        log::debug!("GCS: ADC access token minted (expires_in={expires_in}s)");

        let token = GoogleToken::new(
            &access_token,
            usize::try_from(expires_in).unwrap_or(usize::MAX),
            GCS_SCOPE,
        );
        {
            let mut cache = self.minted.lock().expect("ADC token cache poisoned");
            *cache = Some(MintedToken {
                token: token.clone(),
                minted_at: Instant::now(),
                expires_in_secs: expires_in,
            });
        }
        Ok(token)
    }
}

// Manual `async_trait` expansion — `reqsign::GoogleTokenLoad` is an
// async-trait trait and rivet does not depend on the `async-trait` macro.
impl GoogleTokenLoad for AdcUserTokenLoader {
    fn load<'a, 'b>(
        &'a self,
        client: reqwest::Client,
    ) -> Pin<Box<dyn Future<Output = Result<Option<GoogleToken>>> + Send + 'b>>
    where
        'a: 'b,
        Self: 'b,
    {
        Box::pin(async move {
            // Two concurrent stale loads may both mint; harmless — both
            // tokens are valid, last writer wins.
            if let Some(token) = self.cached_token(Instant::now()) {
                return Ok(Some(token));
            }
            Ok(Some(self.mint_token(&client).await?))
        })
    }
}

/// `true` while a token minted at `minted_at` with `expires_in_secs` of
/// validity still has more than [`REFRESH_THRESHOLD`] of life left at `now`.
fn token_still_fresh(minted_at: Instant, expires_in_secs: u64, now: Instant) -> bool {
    let lifetime = Duration::from_secs(expires_in_secs);
    let age = now.saturating_duration_since(minted_at);
    lifetime.saturating_sub(age) > REFRESH_THRESHOLD
}

/// Parse the OAuth token-endpoint response, capturing the access token and
/// its TTL (see [`TokenResponse`] for the absent-`expires_in` contract).
pub(crate) fn parse_token_response(data: &str) -> Result<(String, u64)> {
    let token: TokenResponse = serde_json::from_str(data).context("parsing token response")?;
    Ok((token.access_token, token.expires_in))
}

/// Looks for `authorized_user` ADC credentials and returns a refreshing token
/// loader to plug into opendal's `customized_token_loader`.
///
/// Returns `Ok(None)` when the well-known ADC file is absent or is not
/// `authorized_user` type (i.e. the caller should let OpenDAL handle
/// credentials normally). No network I/O happens here — the first
/// refresh_token grant runs on the first signed request.
pub(crate) fn try_authorized_user_loader() -> Result<Option<AdcUserTokenLoader>> {
    let path = match adc_path() {
        Some(p) if p.exists() => p,
        _ => return Ok(None),
    };

    // SecOps: the raw ADC file contains a long-lived `refresh_token`; wipe the
    // heap buffer as soon as parsing is done.
    let data = Zeroizing::new(
        std::fs::read_to_string(&path)
            .with_context(|| format!("reading ADC file {}", path.display()))?,
    );
    let fields =
        parse_adc_file(&data).with_context(|| format!("parsing ADC file {}", path.display()))?;
    let (client_id, client_secret, refresh_token) = match fields {
        Some(f) => f,
        None => return Ok(None),
    };

    Ok(Some(AdcUserTokenLoader {
        client_id,
        client_secret: Zeroizing::new(client_secret),
        refresh_token: Zeroizing::new(refresh_token),
        minted: Mutex::new(None),
    }))
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

    // ── roast: ADC token pinned for the run lifetime (gcs-token) ────────────
    //
    // The authorized_user path used to mint ONE access token at destination
    // construction and pin it via opendal's static `.token()` (wrapped with a
    // usize::MAX expiry — never refreshed; disable_vm_metadata removed the
    // fallback), so any export running past the ~1h TTL died mid-run with
    // 401s the RetryLayer cannot fix. These pin the corrected contract: a
    // *refreshing* loader whose freshness window forces a new refresh_token
    // grant before the token can expire under a running export.

    #[test]
    fn roast_gcs_adc_loader_plugs_into_opendal_refresh_hook_not_static_token() {
        // Compile-time pin: the loader satisfies `reqsign::GoogleTokenLoad`,
        // the bound `customized_token_loader` requires — the seam that
        // replaces the never-refreshed static token.
        fn requires_token_load<T: GoogleTokenLoad>() {}
        requires_token_load::<AdcUserTokenLoader>();
    }

    #[test]
    fn roast_gcs_adc_token_goes_stale_before_ttl_so_long_exports_refresh() {
        let minted = Instant::now();
        // Fresh right after the grant.
        assert!(token_still_fresh(minted, 3600, minted));
        // 56 min into a 60-min token (inside the 5-min window): must re-grant.
        assert!(!token_still_fresh(
            minted,
            3600,
            minted + Duration::from_secs(56 * 60)
        ));
        // Way past expiry (the old pinned-token failure mode): must re-grant,
        // and the saturating math must not panic.
        assert!(!token_still_fresh(
            minted,
            3600,
            minted + Duration::from_secs(3 * 3600)
        ));
    }

    #[test]
    fn token_freshness_boundary_exactly_at_threshold_refreshes() {
        let minted = Instant::now();
        // Exactly REFRESH_THRESHOLD of life left → stale (strict `>`):
        // refreshing one call early is cheap; trusting a boundary token is not.
        let at_threshold = minted + Duration::from_secs(3600 - 5 * 60);
        assert!(!token_still_fresh(minted, 3600, at_threshold));
        // One second more life → still fresh.
        let just_inside = minted + Duration::from_secs(3600 - 5 * 60 - 1);
        assert!(token_still_fresh(minted, 3600, just_inside));
    }

    #[test]
    fn token_shorter_lived_than_threshold_is_always_stale() {
        // A 4-minute token can never satisfy the 5-minute freshness window —
        // every load re-grants rather than trusting it.
        let minted = Instant::now();
        assert!(!token_still_fresh(minted, 240, minted));
        assert!(!token_still_fresh(minted, 0, minted));
    }

    #[test]
    fn cached_token_serves_fresh_and_rejects_near_expiry() {
        let loader = AdcUserTokenLoader {
            client_id: "cid".into(),
            client_secret: Zeroizing::new("csec".into()),
            refresh_token: Zeroizing::new("rtoken".into()),
            minted: Mutex::new(None),
        };
        let now = Instant::now();
        assert!(loader.cached_token(now).is_none(), "empty cache mints");
        {
            let mut cache = loader.minted.lock().unwrap();
            *cache = Some(MintedToken {
                token: GoogleToken::new("t", 3600, GCS_SCOPE),
                minted_at: now,
                expires_in_secs: 3600,
            });
        }
        assert!(
            loader.cached_token(now).is_some(),
            "fresh token served from cache"
        );
        assert!(
            loader
                .cached_token(now + Duration::from_secs(3400))
                .is_none(),
            "near-expiry token not served — forces a re-grant"
        );
    }

    #[test]
    fn parse_token_response_captures_expiry() {
        let (tok, ttl) = parse_token_response(
            r#"{"access_token": "ya29.x", "expires_in": 3599, "token_type": "Bearer", "scope": "s"}"#,
        )
        .unwrap();
        assert_eq!(tok, "ya29.x");
        assert_eq!(ttl, 3599);
    }

    #[test]
    fn parse_token_response_missing_expiry_reads_as_already_stale() {
        // RFC 6749 allows `expires_in` to be absent. Unknown TTL must degrade
        // to "refresh every call", never to a token pinned with an invented
        // lifetime.
        let (_, ttl) = parse_token_response(r#"{"access_token": "ya29.x"}"#).unwrap();
        assert_eq!(ttl, 0);
        assert!(!token_still_fresh(Instant::now(), ttl, Instant::now()));
    }

    #[test]
    fn parse_token_response_missing_access_token_errors() {
        let err = parse_token_response(r#"{"expires_in": 3600}"#).unwrap_err();
        assert!(
            err.to_string().contains("parsing token response"),
            "got: {err}"
        );
    }

    #[test]
    fn adc_loader_debug_never_leaks_secrets() {
        let loader = AdcUserTokenLoader {
            client_id: "cid".into(),
            client_secret: Zeroizing::new("SECRETVALUE".into()),
            refresh_token: Zeroizing::new("RTOKENVALUE".into()),
            minted: Mutex::new(None),
        };
        let dbg = format!("{loader:?}");
        assert!(!dbg.contains("SECRETVALUE"), "client_secret leaked: {dbg}");
        assert!(!dbg.contains("RTOKENVALUE"), "refresh_token leaked: {dbg}");
    }
}
