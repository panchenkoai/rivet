//! Google ADC credentials — rivet's ONE Google auth path.
//!
//! `reqsign`'s token loader has no `authorized_user` arm, so anything talking to
//! a Google API natively needs this module or it authenticates in CI and fails
//! on a developer laptop. Two consumers, one credential:
//!
//! - [`AdcUserTokenLoader`] — async, plugged into opendal's
//!   `customized_token_loader` for the GCS destination.
//! - [`BlockingAdcTokenSource`] — blocking, for the synchronous BigQuery REST
//!   loader (`src/load/bq_rest.rs`).
//!
//! Both read the file through [`load_adc_credentials`], send the body
//! [`AdcCredentials::grant_body`] builds, POST it to
//! [`AdcCredentials::token_url`], parse the reply with [`parse_token_response`],
//! and age it with [`token_still_fresh`]. Only the `reqwest` flavour differs.
//!
//! TWO credential shapes reach that one seam, and the grant differs per shape:
//!
//! - `authorized_user` — the OAuth `refresh_token` grant. Carries no `scope`:
//!   the token inherits whatever the human consented to at
//!   `gcloud auth application-default login`.
//! - `service_account` — the RFC 7523 `jwt-bearer` grant: a claim set signed
//!   RS256 with the key file's `private_key`, POSTed as an `assertion`. Here
//!   the `scope` IS load-bearing (nothing else grants one), so each consumer
//!   passes the scope its API needs.
//!
//! Everything else — the freshness window, the caching, the response parse, the
//! redaction — is shared. A third shape (`external_account`/workload identity,
//! which needs an STS exchange, and GCE metadata) still reads as `Ok(None)`
//! here; see `bq_rest::mint_token_via_gcloud_cli` for what happens then.

use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result};
use reqsign::{GoogleToken, GoogleTokenLoad};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

const GOOGLE_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";

/// Scope stamped on minted tokens for the ASYNC (GCS) consumer — opendal's own
/// default GCS scope (rivet never overrides the builder's scope).
///
/// It means two different things per credential shape, and both are correct:
///
/// - `authorized_user`: informational to reqsign's signer. The refresh_token
///   grant determines the actually granted scopes — whatever the user consented
///   to at `gcloud auth application-default login` (cloud-platform in practice)
///   — so changing this string would not widen or narrow anything.
/// - `service_account`: LOAD-BEARING. The jwt-bearer grant returns exactly the
///   scopes the assertion asks for, and this is the least privilege a GCS
///   destination needs (read + write of objects).
const GCS_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.read_write";

/// Scope requested for the BLOCKING (BigQuery REST) consumer's tokens.
///
/// Only the `service_account` grant reads it (a `refresh_token` grant has no
/// `scope` parameter — see [`UserCredentials::refresh_grant_body`]). It is
/// deliberately `cloud-platform` rather than the narrower `bigquery`: a
/// `LOAD DATA FROM FILES` job reads the staged objects out of GCS under the
/// caller's own token, so a bigquery-only scope would enqueue the job and then
/// fail reading the data. `cloud-platform` is also exactly what the
/// `gcloud auth application-default print-access-token` fallback returns, so
/// swapping the subprocess for in-process minting does not change the token's
/// authority — only who mints it.
const BIGQUERY_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

/// RFC 7523 grant type for the service-account assertion flow.
const JWT_BEARER_GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:jwt-bearer";

/// Lifetime stamped on the signed assertion (`exp - iat`). One hour is
/// Google's documented maximum; the assertion is single-use and dies with the
/// request, so nothing is gained by a shorter one.
const SA_ASSERTION_TTL_SECS: u64 = 3600;

/// Remaining validity below which a cached access token is treated as stale
/// and the refresh_token grant is re-run. Wide enough that a multi-minute
/// part upload signed just before the check cannot outlive its token.
const REFRESH_THRESHOLD: Duration = Duration::from_secs(5 * 60);

#[derive(Deserialize)]
struct AdcFile {
    #[serde(rename = "type")]
    cred_type: String,
    // ── authorized_user ──────────────────────────────────────────────────
    client_id: Option<String>,
    client_secret: Option<String>,
    refresh_token: Option<String>,
    /// The project billed for API quota when the caller is a USER credential.
    /// `gcloud auth application-default login` writes it; JSON APIs that meter
    /// per-project (BigQuery) reject a user token without it.
    quota_project_id: Option<String>,
    // ── service_account ──────────────────────────────────────────────────
    /// The service account's own identity — `…@….iam.gserviceaccount.com`.
    /// This is the `iss`/`sub` of the signed assertion and the `user_email`
    /// BigQuery records on the job.
    client_email: Option<String>,
    /// PEM-encoded RSA private key. THE secret in a key file.
    private_key: Option<String>,
    /// Token endpoint to POST the assertion to, and the assertion's `aud`.
    /// Present in every key file Google issues; defaults to the well-known
    /// endpoint when absent so a hand-trimmed file still works.
    token_uri: Option<String>,
}

/// ADC credentials — the ONE credential type rivet mints Google access tokens
/// from, shared by both consumers in this file: the async opendal/GCS signer
/// ([`AdcUserTokenLoader`]) and the blocking BigQuery REST client
/// ([`BlockingAdcTokenSource`]).
///
/// The grant body, the token endpoint and the response parse live here, once,
/// so the two transports differ only in which `reqwest` flavour dispatches the
/// request — and so teaching rivet a credential SHAPE (the `service_account`
/// arm) teaches both consumers at the same time.
pub(crate) enum AdcCredentials {
    /// `gcloud auth application-default login` — a human's consented grant.
    User(UserCredentials),
    /// A service-account key file (`GOOGLE_APPLICATION_CREDENTIALS`).
    ServiceAccount(ServiceAccountKey),
}

/// The `authorized_user` half of [`AdcCredentials`].
pub(crate) struct UserCredentials {
    client_id: String,
    // SecOps: long-lived secrets; heap zeroed on drop, never logged.
    client_secret: Zeroizing<String>,
    refresh_token: Zeroizing<String>,
    quota_project: Option<String>,
}

/// The `service_account` half of [`AdcCredentials`].
pub(crate) struct ServiceAccountKey {
    client_email: String,
    // SecOps: THE secret. Heap zeroed on drop; never logged, never rendered by
    // `Debug`, and never echoed into an error (see [`Self::encoding_key`]).
    private_key_pem: Zeroizing<String>,
    /// Both the POST target and the assertion's `aud` — Google requires them
    /// to match, so one field feeds both.
    token_uri: String,
}

impl fmt::Debug for AdcCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // SecOps: `Debug` is reachable from `Result::unwrap_err` and from any
        // consumer that derives it — render only the PUBLIC fields.
        f.debug_struct("AdcCredentials")
            .field("kind", &self.credential_kind())
            .field("principal", &self.principal())
            .field("quota_project", &self.quota_project())
            .finish_non_exhaustive()
    }
}

impl AdcCredentials {
    /// `authorized_user` credentials from literal values — the seam tests build
    /// a loader through without an ADC file on disk.
    #[cfg(test)]
    pub(crate) fn user_for_test(client_id: &str, client_secret: &str, refresh_token: &str) -> Self {
        Self::User(UserCredentials {
            client_id: client_id.to_string(),
            client_secret: Zeroizing::new(client_secret.to_string()),
            refresh_token: Zeroizing::new(refresh_token.to_string()),
            quota_project: None,
        })
    }

    /// Which credential shape this is — for logs and `Debug`. Never a secret.
    pub(crate) fn credential_kind(&self) -> &'static str {
        match self {
            Self::User(_) => "authorized_user",
            Self::ServiceAccount(_) => "service_account",
        }
    }

    /// The PUBLIC identifier of the calling identity: the OAuth client id for a
    /// user credential, the service account's email for a key file. Safe to log
    /// — and the only field that tells an operator WHICH identity a run acted
    /// as, which is the whole point of minting in process.
    pub(crate) fn principal(&self) -> &str {
        match self {
            Self::User(u) => &u.client_id,
            Self::ServiceAccount(sa) => &sa.client_email,
        }
    }

    /// The project billed for API quota, when the credential names one. A
    /// service account is billable on its own, so it never does — and it must
    /// not be given a fabricated one (see
    /// [`BlockingAdcTokenSource::quota_project_header`]).
    pub(crate) fn quota_project(&self) -> Option<&str> {
        match self {
            Self::User(u) => u.quota_project.as_deref(),
            Self::ServiceAccount(_) => None,
        }
    }

    /// Where the grant body is POSTed. The well-known endpoint for a user
    /// credential; the key file's own `token_uri` for a service account (which
    /// is also the assertion's `aud`, so the two cannot drift apart).
    pub(crate) fn token_url(&self) -> &str {
        match self {
            Self::User(_) => GOOGLE_TOKEN_URL,
            Self::ServiceAccount(sa) => &sa.token_uri,
        }
    }

    /// The form-urlencoded body that buys an access token, per credential shape.
    ///
    /// `scope` is honoured ONLY by the service-account arm — a `refresh_token`
    /// grant has no `scope` parameter and returns the user's consented scopes
    /// (see [`UserCredentials::refresh_grant_body`]). Passing it unconditionally
    /// keeps ONE call shape at both consumers.
    ///
    /// SecOps: the returned body carries secret material (a refresh token, or a
    /// freshly signed assertion) — it is `Zeroizing` so the heap buffer is wiped
    /// once the request is dispatched.
    pub(crate) fn grant_body(&self, scope: &str) -> Result<Zeroizing<String>> {
        match self {
            Self::User(u) => Ok(u.refresh_grant_body()),
            Self::ServiceAccount(sa) => sa.jwt_bearer_grant_body(scope, unix_now()?),
        }
    }
}

impl UserCredentials {
    /// The `refresh_token` grant body, form-urlencoded.
    ///
    /// SecOps: carries `client_secret` and `refresh_token` in clear. Wrapped so
    /// the heap buffer is zeroed once the request is dispatched, and so an
    /// accidental `Debug` of the caller's request builder does not leak it.
    /// Note there is NO `scope` parameter: a refresh_token grant returns
    /// whatever the user consented to at `application-default login`
    /// (cloud-platform in practice) — which is why one grant serves GCS and
    /// BigQuery alike.
    fn refresh_grant_body(&self) -> Zeroizing<String> {
        Zeroizing::new(format!(
            "grant_type=refresh_token&client_id={}&client_secret={}&refresh_token={}",
            urlenc(&self.client_id),
            urlenc(&self.client_secret),
            urlenc(&self.refresh_token),
        ))
    }
}

/// The claim set of the service-account assertion (RFC 7523 §3, as Google
/// profiles it in "Using OAuth 2.0 for Server to Server Applications").
///
/// Pure data with a pinned field ORDER: `serde_json` serialises in declaration
/// order, so the JSON below is byte-reproducible and a unit test can compare it
/// to a hand-written literal rather than to another serialisation of itself.
#[derive(Serialize, Debug, PartialEq, Eq)]
struct JwtBearerClaims {
    /// The service account's email — who is asking.
    iss: String,
    /// What is being asked FOR. Load-bearing here (unlike the refresh_token
    /// grant): the returned token carries exactly these scopes.
    scope: String,
    /// The token endpoint. Google rejects an assertion whose `aud` is not the
    /// endpoint it was presented to — that is what stops a captured assertion
    /// being replayed elsewhere.
    aud: String,
    /// Issued-at / expiry, in seconds since the Unix epoch. WALL clock, not the
    /// monotonic `Instant` the freshness rule uses: these are compared against
    /// GOOGLE's clock, so a monotonic reading would be meaningless. (A machine
    /// whose wall clock is off by more than the skew Google tolerates gets a
    /// clear `invalid_grant`, not a silent wrong result.)
    iat: u64,
    exp: u64,
}

impl ServiceAccountKey {
    /// The RFC 7523 claim set for `scope`, issued at `now_unix`.
    ///
    /// Pure — split out from the signing so the claims can be graded against a
    /// literal without a key, a clock, or a network.
    fn claims(&self, scope: &str, now_unix: u64) -> JwtBearerClaims {
        JwtBearerClaims {
            iss: self.client_email.clone(),
            scope: scope.to_string(),
            aud: self.token_uri.clone(),
            iat: now_unix,
            exp: now_unix + SA_ASSERTION_TTL_SECS,
        }
    }

    /// The `jwt-bearer` grant body: a claim set signed RS256 with the key
    /// file's `private_key`, form-urlencoded as `assertion`.
    ///
    /// This is the whole reason rivet no longer shells out to `gcloud` for a
    /// service account: everything the subprocess did is these three lines.
    fn jwt_bearer_grant_body(&self, scope: &str, now_unix: u64) -> Result<Zeroizing<String>> {
        let assertion = self.sign_assertion(&self.claims(scope, now_unix))?;
        Ok(jwt_bearer_grant_body(&assertion))
    }

    /// RS256-sign the claim set, returning the compact-serialised JWT.
    fn sign_assertion(&self, claims: &JwtBearerClaims) -> Result<Zeroizing<String>> {
        let key = self.encoding_key()?;
        let header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        // SecOps: an encoding failure must not carry the library's error text
        // upward — see `encoding_key` for why the underlying error is dropped.
        let token = jsonwebtoken::encode(&header, claims, &key).map_err(|_| {
            anyhow::anyhow!(
                "service account {}: signing the token assertion failed",
                self.client_email
            )
        })?;
        Ok(Zeroizing::new(token))
    }

    /// Parse the PEM into a signing key.
    ///
    /// SecOps: the underlying error is DELIBERATELY dropped rather than
    /// wrapped. `jsonwebtoken`/`pem` parse errors can quote the offending input,
    /// and this message reaches `summary.error_message` → SQLite / Slack. The
    /// replacement names the file's own `client_email` (public) and the field
    /// at fault, which is everything an operator needs to fix it.
    fn encoding_key(&self) -> Result<jsonwebtoken::EncodingKey> {
        jsonwebtoken::EncodingKey::from_rsa_pem(self.private_key_pem.as_bytes()).map_err(|_| {
            anyhow::anyhow!(
                "service account {}: `private_key` is not a PEM-encoded RSA private key",
                self.client_email
            )
        })
    }
}

/// The `jwt-bearer` grant body for an already-signed `assertion`. Pure.
///
/// SecOps: `Zeroizing` because the assertion is a bearer credential for its
/// hour — anyone holding it can mint the same access token.
fn jwt_bearer_grant_body(assertion: &str) -> Zeroizing<String> {
    Zeroizing::new(format!(
        "grant_type={}&assertion={}",
        urlenc(JWT_BEARER_GRANT_TYPE),
        urlenc(assertion),
    ))
}

/// Seconds since the Unix epoch, for the assertion's `iat`/`exp`.
///
/// Errors rather than defaulting to 0 on a pre-epoch clock: a fabricated `iat`
/// buys an assertion Google rejects with a message about the wrong thing.
fn unix_now() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .context("system clock is before the Unix epoch — cannot sign a token assertion")?
        .as_secs())
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
/// Holds the ADC credentials and re-runs the grant whenever the cached access
/// token is within [`REFRESH_THRESHOLD`] of expiry, so an export of any
/// duration never outlives its token. (A static builder `.token()` is wrapped
/// by opendal with a `usize::MAX` expiry — never refreshed — so exports longer
/// than the ~1h TTL would die mid-run with 401s the RetryLayer cannot fix.)
///
/// The name says `User` for one reason only: it is a first-party extension-seam
/// item (ADR-0026, compile-locked by `tests/offline/extension_seam.rs`), and
/// renaming it would break a consumer to describe a widening. It serves BOTH
/// credential shapes — see [`AdcCredentials`].
pub struct AdcUserTokenLoader {
    creds: AdcCredentials,
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
        // (public) principal — the OAuth client id, or the service account's
        // own email address.
        f.debug_struct("AdcUserTokenLoader")
            .field("kind", &self.creds.credential_kind())
            .field("principal", &self.creds.principal())
            .finish_non_exhaustive()
    }
}

impl AdcUserTokenLoader {
    /// Which credential shape this loader mints from (`authorized_user` /
    /// `service_account`) — so the GCS destination can SAY which identity it
    /// resolved instead of asserting one.
    pub fn credential_kind(&self) -> &'static str {
        self.creds.credential_kind()
    }

    /// The public identifier of the identity this loader acts as.
    pub fn principal(&self) -> &str {
        self.creds.principal()
    }

    fn cached_token(&self, now: Instant) -> Option<GoogleToken> {
        let cache = self.minted.lock().expect("ADC token cache poisoned");
        cache
            .as_ref()
            .filter(|c| token_still_fresh(c.minted_at, c.expires_in_secs, now))
            .map(|c| c.token.clone())
    }

    async fn mint_token(&self, client: &reqwest::Client) -> Result<GoogleToken> {
        log::info!(
            "GCS: minting an access token from ADC {} credentials ({})",
            self.creds.credential_kind(),
            self.creds.principal()
        );

        let body = self.creds.grant_body(GCS_SCOPE)?;

        let resp = client
            .post(self.creds.token_url())
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body.as_str().to_string())
            .send()
            .await
            .context("ADC token request failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            // Do NOT surface the raw response body: Google's OAuth error responses
            // echo back the submitted `client_id` / `client_secret` in some failure
            // modes, which would end up in `summary.error_message` → SQLite / Slack.
            anyhow::bail!("ADC token request failed (HTTP {})", status);
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

/// Looks for ADC credentials and returns a refreshing token loader to plug into
/// opendal's `customized_token_loader`.
///
/// Returns `Ok(None)` when the well-known ADC file is absent or holds a shape
/// rivet cannot mint in process (`external_account`) — the caller should then
/// let OpenDAL handle credentials normally. No network I/O happens here; the
/// first grant runs on the first signed request.
///
/// (The name is frozen by the extension seam, not by the behaviour: it serves
/// `service_account` key files too. See [`AdcUserTokenLoader`].)
pub fn try_authorized_user_loader() -> Result<Option<AdcUserTokenLoader>> {
    Ok(load_adc_credentials()?.map(|creds| AdcUserTokenLoader {
        creds,
        minted: Mutex::new(None),
    }))
}

/// Read the well-known ADC file and return the credentials it holds.
///
/// `Ok(None)` when the file is absent or holds a shape rivet cannot mint in
/// process — the caller then falls back to whatever its API client does
/// natively (OpenDAL for GCS; the `gcloud` CLI token for BigQuery REST). No
/// network I/O.
///
/// The ONE reader of the ADC file: both token sources in this module go through
/// it, so a credential shape rivet learns to read is learned once.
pub(crate) fn load_adc_credentials() -> Result<Option<AdcCredentials>> {
    // An EXPLICITLY-set GOOGLE_APPLICATION_CREDENTIALS pointing at a missing
    // file is an ERROR, not "no credentials": the old Ok(None) fell through to
    // a hint telling the operator to set the very variable they had just set,
    // never mentioning the missing file (round-6 hostile-input probe).
    if let Ok(p) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS")
        && !std::path::Path::new(&p).exists()
    {
        anyhow::bail!(
            "GOOGLE_APPLICATION_CREDENTIALS is set to '{p}', but no file exists there — \
             fix the path (or unset the variable to fall back to gcloud ADC)"
        );
    }
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
    parse_adc_file(&data).with_context(|| format!("parsing ADC file {}", path.display()))
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
///
/// Returns `Ok(None)` for a credential shape rivet cannot mint in process
/// (`external_account` / workload identity, which needs an STS exchange) — the
/// caller then falls back to whatever its API client does natively.
pub(crate) fn parse_adc_file(data: &str) -> Result<Option<AdcCredentials>> {
    let adc: AdcFile = serde_json::from_str(data).context("parsing ADC JSON")?;
    match adc.cred_type.as_str() {
        "authorized_user" => parse_authorized_user(adc).map(Some),
        "service_account" => parse_service_account(adc).map(Some),
        _ => Ok(None),
    }
}

fn parse_authorized_user(adc: AdcFile) -> Result<AdcCredentials> {
    let client_id = adc
        .client_id
        .ok_or_else(|| anyhow::anyhow!("ADC authorized_user: missing client_id"))?;
    let client_secret = adc
        .client_secret
        .ok_or_else(|| anyhow::anyhow!("ADC authorized_user: missing client_secret"))?;
    let refresh_token = adc
        .refresh_token
        .ok_or_else(|| anyhow::anyhow!("ADC authorized_user: missing refresh_token"))?;
    Ok(AdcCredentials::User(UserCredentials {
        client_id,
        client_secret: Zeroizing::new(client_secret),
        refresh_token: Zeroizing::new(refresh_token),
        quota_project: adc.quota_project_id,
    }))
}

fn parse_service_account(adc: AdcFile) -> Result<AdcCredentials> {
    let client_email = adc
        .client_email
        .ok_or_else(|| anyhow::anyhow!("ADC service_account: missing client_email"))?;
    let private_key = adc
        .private_key
        .ok_or_else(|| anyhow::anyhow!("ADC service_account: missing private_key"))?;
    let key = ServiceAccountKey {
        client_email,
        private_key_pem: Zeroizing::new(private_key),
        // A key file Google issues always names the endpoint; default it so a
        // hand-trimmed file is a working credential rather than a 400 from an
        // assertion whose `aud` is the empty string.
        token_uri: adc
            .token_uri
            .filter(|u| !u.trim().is_empty())
            .unwrap_or_else(|| GOOGLE_TOKEN_URL.to_string()),
    };
    // Parse the PEM NOW and drop the result: credential resolution is eager
    // precisely so a broken credential is reported before the first job is
    // enqueued, and an unusable `private_key` is exactly that. Costs one RSA
    // key parse per process.
    let _ = key.encoding_key()?;
    Ok(AdcCredentials::ServiceAccount(key))
}

/// Blocking sibling of [`AdcUserTokenLoader`] for the SYNCHRONOUS Google REST
/// consumers (the BigQuery loader).
///
/// Same credentials, same grant body, same response parse, same freshness rule
/// — only the transport differs (`reqwest::blocking` instead of the async
/// client opendal hands its signer). Keeping it beside its async twin is what
/// makes "rivet has one Google auth path" checkable by reading one file.
pub(crate) struct BlockingAdcTokenSource {
    creds: AdcCredentials,
    http: reqwest::blocking::Client,
    minted: Mutex<Option<MintedAccessToken>>,
}

/// A minted bearer token and the clock it was minted against — the same
/// (token, minted_at, ttl) triple [`MintedToken`] caches for the async side,
/// without reqsign's `GoogleToken` wrapper (the REST client sends a raw
/// `Authorization: Bearer` header, no signer involved).
struct MintedAccessToken {
    token: Zeroizing<String>,
    minted_at: Instant,
    expires_in_secs: u64,
}

impl fmt::Debug for BlockingAdcTokenSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // SecOps: redact everything but the (public) principal — this type is
        // reachable from a loader that derives Debug.
        f.debug_struct("BlockingAdcTokenSource")
            .field("kind", &self.creds.credential_kind())
            .field("principal", &self.creds.principal())
            .finish_non_exhaustive()
    }
}

impl BlockingAdcTokenSource {
    pub(crate) fn new(creds: AdcCredentials, http: reqwest::blocking::Client) -> Self {
        Self {
            creds,
            http,
            minted: Mutex::new(None),
        }
    }

    /// The value to send as `x-goog-user-project`, or `None` when the header
    /// must be OMITTED.
    ///
    /// A USER credential is not billable on its own: a metered JSON API rejects
    /// it without a quota project, so the header is sent — the one the ADC file
    /// names, else `fallback` (the project being loaded into).
    ///
    /// A SERVICE ACCOUNT is billable on its own, and naming a quota project
    /// makes the API require `serviceusage.services.use` ON that project — a
    /// permission `bigquery.jobUser` + `bigquery.dataOwner` do NOT include. So
    /// sending the header "just in case" turns a working service account into a
    /// 403 that reads as a BigQuery problem. `None` means: send nothing.
    pub(crate) fn quota_project_header<'a>(&'a self, fallback: &'a str) -> Option<&'a str> {
        match &self.creds {
            AdcCredentials::User(_) => Some(self.creds.quota_project().unwrap_or(fallback)),
            AdcCredentials::ServiceAccount(_) => None,
        }
    }

    /// Which credential shape backs this source, and who it acts as — for the
    /// loader's startup log. Never a secret.
    pub(crate) fn credential_kind(&self) -> &'static str {
        self.creds.credential_kind()
    }

    pub(crate) fn principal(&self) -> &str {
        self.creds.principal()
    }

    /// A live access token: the cached one while it has more than
    /// [`REFRESH_THRESHOLD`] of life left, otherwise a fresh refresh_token grant.
    pub(crate) fn access_token(&self) -> Result<Zeroizing<String>> {
        if let Some(t) = self.cached(Instant::now()) {
            return Ok(t);
        }
        self.mint()
    }

    fn cached(&self, now: Instant) -> Option<Zeroizing<String>> {
        let cache = self.minted.lock().expect("ADC token cache poisoned");
        cache
            .as_ref()
            .filter(|c| token_still_fresh(c.minted_at, c.expires_in_secs, now))
            .map(|c| c.token.clone())
    }

    fn mint(&self) -> Result<Zeroizing<String>> {
        log::info!(
            "BigQuery: minting an access token from ADC {} credentials ({})",
            self.creds.credential_kind(),
            self.creds.principal()
        );
        let body = self.creds.grant_body(BIGQUERY_SCOPE)?;
        let resp = self
            .http
            .post(self.creds.token_url())
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body.as_str().to_string())
            .send()
            .context("ADC token request failed")?;

        if !resp.status().is_success() {
            // Do NOT surface the raw body: Google's OAuth error responses echo
            // the submitted client_id/client_secret in some failure modes, and
            // this message reaches `summary.error_message` → SQLite / Slack.
            anyhow::bail!("ADC token request failed (HTTP {})", resp.status());
        }

        let payload = resp.text().context("reading token response")?;
        let (access_token, expires_in) = parse_token_response(&payload)?;
        let token = Zeroizing::new(access_token);
        {
            let mut cache = self.minted.lock().expect("ADC token cache poisoned");
            *cache = Some(MintedAccessToken {
                token: token.clone(),
                minted_at: Instant::now(),
                expires_in_secs: expires_in,
            });
        }
        Ok(token)
    }
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

    /// The `authorized_user` arm, unwrapped — panics on any other shape, so a
    /// test that means "user credentials" cannot silently grade something else.
    fn expect_user(c: &AdcCredentials) -> &UserCredentials {
        match c {
            AdcCredentials::User(u) => u,
            AdcCredentials::ServiceAccount(_) => panic!("expected authorized_user credentials"),
        }
    }

    #[test]
    fn parse_adc_authorized_user_ok() {
        let json = r#"{
            "type": "authorized_user",
            "client_id": "cid",
            "client_secret": "csec",
            "refresh_token": "rtoken"
        }"#;
        let c = parse_adc_file(json).unwrap().expect("authorized_user");
        assert_eq!(c.credential_kind(), "authorized_user");
        assert_eq!(c.principal(), "cid");
        let u = expect_user(&c);
        assert_eq!(u.client_id, "cid");
        assert_eq!(u.client_secret.as_str(), "csec");
        assert_eq!(u.refresh_token.as_str(), "rtoken");
        // An ADC file that names no quota project reads as None, never as a
        // fabricated one — the BigQuery client then bills its own project.
        assert_eq!(c.quota_project(), None);
        // A user credential always goes to the well-known endpoint.
        assert_eq!(c.token_url(), GOOGLE_TOKEN_URL);
    }

    /// `gcloud auth application-default login` writes `quota_project_id`, and a
    /// metered JSON API (BigQuery) rejects a USER token that does not name one.
    /// Pins that the parser carries it through rather than dropping it.
    #[test]
    fn parse_adc_captures_the_quota_project() {
        let json = r#"{
            "type": "authorized_user",
            "client_id": "cid",
            "client_secret": "csec",
            "refresh_token": "rtoken",
            "quota_project_id": "my-billing-proj"
        }"#;
        let c = parse_adc_file(json).unwrap().expect("authorized_user");
        assert_eq!(c.quota_project(), Some("my-billing-proj"));
    }

    /// The grant body is the seam BOTH token sources send. For a USER
    /// credential it carries the three credential fields url-encoded and —
    /// deliberately — NO `scope`, so the returned token keeps every scope the
    /// user consented to (that is what lets one grant serve GCS and BigQuery).
    /// The `scope` argument the seam passes is IGNORED on this arm; asserting
    /// that is what keeps the service-account widening from quietly changing
    /// what a user credential asks for.
    #[test]
    fn refresh_grant_body_is_the_shared_refresh_token_grant() {
        let c = parse_adc_file(
            r#"{"type":"authorized_user","client_id":"c id","client_secret":"s/ec","refresh_token":"r+tok"}"#,
        )
        .unwrap()
        .unwrap();
        let body = c.grant_body(BIGQUERY_SCOPE).unwrap();
        assert_eq!(
            body.as_str(),
            "grant_type=refresh_token&client_id=c%20id&client_secret=s%2Fec&refresh_token=r%2Btok"
        );
        assert!(!body.contains("scope"), "{}", body.as_str());
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
            creds: AdcCredentials::user_for_test("cid", "csec", "rtoken"),
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
            creds: AdcCredentials::user_for_test("cid", "SECRETVALUE", "RTOKENVALUE"),
            minted: Mutex::new(None),
        };
        let dbg = format!("{loader:?}");
        assert!(!dbg.contains("SECRETVALUE"), "client_secret leaked: {dbg}");
        assert!(!dbg.contains("RTOKENVALUE"), "refresh_token leaked: {dbg}");
        // …and the POSITIVE half, without which the absence assertions above are
        // satisfied by rendering NOTHING: the in-diff mutation gate found
        // `Debug::fmt -> Ok(Default::default())` (an empty impl) alive here,
        // because an empty string contains no secret either. A redaction test
        // must pin what the impl still SHOWS, or it stops guarding the impl and
        // only guards the string.
        assert!(
            dbg.contains("AdcUserTokenLoader"),
            "Debug must still name the type — an empty impl passes the leak \
             assertions while telling an operator nothing: {dbg}"
        );
        assert!(
            dbg.contains("cid"),
            "the PUBLIC client id is the one field this impl exists to render: {dbg}"
        );
    }

    // ── service_account: the in-process jwt-bearer grant ─────────────────────
    //
    // Why these exist: with `GOOGLE_APPLICATION_CREDENTIALS` pointing at a key
    // file, `parse_adc_file` used to return `Ok(None)` and the BigQuery loader
    // shelled out to `gcloud` for a token — so rivet wrote GCS as the service
    // account and called BigQuery as whatever HUMAN gcloud was logged in as.
    // The replacement is RFC 7523: a claim set signed RS256 with the file's own
    // `private_key`, exchanged for an access token. Every piece of that is
    // pure, so every piece is graded here without a network or a credential.

    /// A throwaway 2048-bit RSA key, generated for these tests and used NOWHERE
    /// else. It authorises nothing — no Google project has ever seen the
    /// matching public key, and it is a credential for nothing. It is checked
    /// in because RS256 signing cannot be graded without a key, and generating
    /// one at test time costs seconds per run in a debug build.
    const TEST_RSA_PKCS8_PEM: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCbCRikdpLmY2r2
K8M22FPwsBOVNRCBU8wmEBAy1H7ZxNbf5prq+fYh1jeDi4qNpUvtmFslX+5fd1Es
d8F8xqFNDMKA6kyjjguU+I6JbBWOfZfLROx+8GeILY9aYm62F/ab13xLPxXk/Kn9
kLCYJjnIuSOGUeluwCwJhb1lhayP3/tLG/zujgqL0JIkQe2M6+3LKCMqhOsScB5Q
7OAnDaOuteCxzhqoORIbT5+nK4J0CoSbzU7R959y8a4Z1PKeKcJX29y06j3eDEtd
yvJYX7tfUNyetRtxkbiEd+m+EbnRyaO0Dlynm6NCN7bg0+QsB/qi+1K72EaLL3NK
LLLWJY5JAgMBAAECggEAL1TPmSY6IuwnM5CYwJ26wrh+wga5S0JyCQzOZTZKo+Fc
WG7mzOYTJrFnsVlgq23TiG4DJZ3sAGlE9vZ4s8dQZ12F5CWj0CsFji10FrBQxHfD
Z+Na8EYk1YZTRZzXf/BA8cMPx0kzPf2FSExsHYdODOG43sETKySwTRfvgpcU37df
8ihI/UzBVTrGZ9+eM2JXdpG6MzW+9mkAhSzNbSZtX+Qlib8hYA+Cki14gX76/tB/
m80o+KS+isVz0GdhMNe7UBWLTu/I1q5RIG2tdQ0xIazgrkPJoqbVNw8zriUk1SNL
wp2106FeLAd3QE1NesBd7xefhcd2INsGWc/5sTlfIQKBgQDUmuefmW0+4DoI2xdw
KV06b2CQpbBfrg676XfxuCW29YosvhyMSYXyYTp8CMlyk/fuZk9osWeQ4dsOb3KK
JgieaMtHfcnhHojdVNO755ehUGr7R/wZKjVOTDxDI4dP39LmIVtGT2ue/bGMEqbn
pBI/iFn5e7SN/+M+kT9pfML2PwKBgQC6rg1TVQ36oK93ImWtj8UEUBhtGDpdLMzF
0W7BrWXlejNPdlirJKaaOqUfvgTBZOPULvtus8jGNIEylAd+KqHApd35XhXsrz/F
4AmP3hE1m3ZhjfB9ISzR1YCbEjAYVfeMQ0AHsKLXKR9BH2P7TVXwitjYT7YLWTq2
c6a3XqspdwKBgQCSA7lkYWkoL7Nr9ZzJSX9f7LJzZXgWnmI8KIJaHtC4MTaut/q/
VpeJ+cDDAv3nlTxIHTgYfQ7V8GgUJ/KQcsKkmPBTr06BMAlriSDKAqqe/a4b2jqH
xfdKSQ1yRupdrykXKH2Zcl5FmZLg0saSfJqTy7+PmqhMhhu6EuNsPWVg3QKBgGyu
K9/Z4puItN58zgkvbBnIr4/DzSa1kDmSZhKnam0gqYKXeaOZYMzlW4CRVZ9ppyG+
gG7AiCCsLJNPjtUq6KSJbCNjXQxAMrZ2adqyA/7blR3STrHqgHdg/tBI1Gs2kTm9
ZSeU9BijPijhp4mESoYRr5CvR2Jv/jh+OA8FNEydAoGBAKKVDPqomBLp26hdzrVA
O8M+Hej8+dYLCDO/nllv3S3F1SAJGvd1oNgNVFWBgyr/DqGzYbRbtKQ4quNt2nC2
xt3980QUv/ltOZmAePNaWyWw4Egr1r5TEnZtDk1JEiMz6k1ulkIDguwVbEb2G4m+
qPSokX7fAC0Ku7S5xJe4XfPd
-----END PRIVATE KEY-----
"#;

    const TEST_SA_EMAIL: &str = "rivet-test@rivet-unit.iam.gserviceaccount.com";

    /// The literal claim set the tests below grade against — written by hand,
    /// not produced by the code under test.
    const EXPECTED_CLAIMS_JSON: &str = concat!(
        r#"{"iss":"rivet-test@rivet-unit.iam.gserviceaccount.com","#,
        r#""scope":"https://www.googleapis.com/auth/devstorage.read_write","#,
        r#""aud":"https://oauth2.googleapis.com/token","iat":1700000000,"exp":1700003600}"#
    );

    /// A key file with the shape Google issues, minus the fields rivet ignores.
    fn sa_json(token_uri: Option<&str>, private_key: &str) -> String {
        let uri = token_uri
            .map(|u| format!(r#","token_uri":"{u}""#))
            .unwrap_or_default();
        format!(
            r#"{{"type":"service_account","project_id":"rivet-unit",
                 "client_email":"{TEST_SA_EMAIL}","private_key":{}{uri}}}"#,
            serde_json::to_string(private_key).unwrap()
        )
    }

    fn sa_credentials() -> AdcCredentials {
        parse_adc_file(&sa_json(None, TEST_RSA_PKCS8_PEM))
            .expect("a well-formed key file parses")
            .expect("service_account is a credential rivet mints from")
    }

    /// The `service_account` arm, unwrapped — panics on any other shape.
    fn expect_sa(c: &AdcCredentials) -> &ServiceAccountKey {
        match c {
            AdcCredentials::ServiceAccount(sa) => sa,
            AdcCredentials::User(_) => panic!("expected service_account credentials"),
        }
    }

    /// Decode one compact-JWT segment. Deliberately NOT `jsonwebtoken`'s own
    /// decoder: the point is to read the bytes rivet emitted with a different
    /// tool, so the assertion is not the encoder grading itself.
    fn b64url(segment: &str) -> Vec<u8> {
        use base64::Engine as _;
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(segment)
            .expect("a JWT segment must be unpadded base64url")
    }

    /// A service-account key file is a CREDENTIAL now, not an `Ok(None)` that
    /// sends the caller to a subprocess.
    #[test]
    fn parse_adc_service_account_is_a_credential_rivet_mints_from() {
        let c = sa_credentials();
        assert_eq!(c.credential_kind(), "service_account");
        // The principal is the identity BigQuery records as `user_email`.
        assert_eq!(c.principal(), TEST_SA_EMAIL);
        // No `token_uri` in the file → the well-known endpoint, never an empty
        // `aud` (which Google rejects with a message about the wrong thing).
        assert_eq!(c.token_url(), GOOGLE_TOKEN_URL);
        // A service account is billable on its own: it must NOT carry a quota
        // project, or the API demands `serviceusage.services.use` for it.
        assert_eq!(c.quota_project(), None);
    }

    /// The file's own `token_uri` wins — and the assertion's `aud` follows it,
    /// because Google rejects an assertion whose audience is not the endpoint
    /// it was presented to. One field, both uses, so they cannot drift.
    #[test]
    fn service_account_token_uri_drives_both_the_post_target_and_the_audience() {
        let c = parse_adc_file(&sa_json(
            Some("https://oauth2.example.test/token"),
            TEST_RSA_PKCS8_PEM,
        ))
        .unwrap()
        .unwrap();
        assert_eq!(c.token_url(), "https://oauth2.example.test/token");
        let claims = expect_sa(&c).claims(GCS_SCOPE, 1_700_000_000);
        assert_eq!(claims.aud, "https://oauth2.example.test/token");
    }

    #[test]
    fn parse_adc_service_account_missing_client_email_errors() {
        let json = format!(
            r#"{{"type":"service_account","private_key":{}}}"#,
            serde_json::to_string(TEST_RSA_PKCS8_PEM).unwrap()
        );
        let err = parse_adc_file(&json).unwrap_err();
        assert!(err.to_string().contains("client_email"), "got: {err}");
    }

    #[test]
    fn parse_adc_service_account_missing_private_key_errors() {
        let json = format!(r#"{{"type":"service_account","client_email":"{TEST_SA_EMAIL}"}}"#);
        let err = parse_adc_file(&json).unwrap_err();
        assert!(err.to_string().contains("private_key"), "got: {err}");
    }

    /// An unusable key must fail where credentials are RESOLVED — before a job
    /// is enqueued — and the message must name the file and the field without
    /// echoing the key material, since it lands in `summary.error_message`
    /// → SQLite / Slack.
    #[test]
    fn service_account_with_an_unusable_private_key_fails_at_load_without_echoing_it() {
        let junk =
            "-----BEGIN PRIVATE KEY-----\nNOT_A_KEY_BUT_STILL_SECRET\n-----END PRIVATE KEY-----\n";
        let err = parse_adc_file(&sa_json(None, junk)).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains(TEST_SA_EMAIL),
            "must name the credential: {msg}"
        );
        assert!(msg.contains("private_key"), "must name the field: {msg}");
        assert!(
            !msg.contains("NOT_A_KEY_BUT_STILL_SECRET"),
            "key material must never reach an error message: {msg}"
        );
    }

    /// The claim set, byte for byte, against a hand-written literal — the
    /// independent oracle for `iss` / `scope` / `aud` / `iat` / `exp`.
    #[test]
    fn jwt_bearer_claims_are_the_rfc7523_claim_set() {
        let c = sa_credentials();
        let claims = expect_sa(&c).claims(GCS_SCOPE, 1_700_000_000);
        assert_eq!(
            serde_json::to_string(&claims).unwrap(),
            EXPECTED_CLAIMS_JSON
        );
    }

    /// The assertion is a real RS256 JWT over exactly those claims.
    ///
    /// The oracle is INDEPENDENT of the signer: the segments are base64url
    /// decoded by the `base64` crate and the signature is verified by RustCrypto
    /// (`rsa` + `sha2`), never by `jsonwebtoken`'s own decoder — so the test
    /// grades what rivet emitted rather than round-tripping one library through
    /// itself. The tamper half is what keeps the verification from being
    /// vacuous.
    #[test]
    fn service_account_assertion_is_an_rs256_jwt_over_the_claim_set() {
        use rsa::pkcs1v15::{Signature, VerifyingKey};
        use rsa::pkcs8::DecodePrivateKey;
        use rsa::signature::Verifier;

        let c = sa_credentials();
        let sa = expect_sa(&c);
        let assertion = sa
            .sign_assertion(&sa.claims(GCS_SCOPE, 1_700_000_000))
            .unwrap();

        let parts: Vec<&str> = assertion.split('.').collect();
        assert_eq!(
            parts.len(),
            3,
            "compact JWT serialisation: {}",
            assertion.as_str()
        );

        let header: serde_json::Value = serde_json::from_slice(&b64url(parts[0])).unwrap();
        assert_eq!(
            header["alg"], "RS256",
            "Google accepts RS256 for a key file"
        );
        assert_eq!(header["typ"], "JWT");

        // The payload is the SAME hand-written literal, read back out of the
        // encoded assertion.
        assert_eq!(
            String::from_utf8(b64url(parts[1])).unwrap(),
            EXPECTED_CLAIMS_JSON
        );

        let key = rsa::RsaPrivateKey::from_pkcs8_pem(TEST_RSA_PKCS8_PEM).unwrap();
        let verifying = VerifyingKey::<sha2::Sha256>::new(key.to_public_key());
        let signature = Signature::try_from(b64url(parts[2]).as_slice()).unwrap();
        let signing_input = format!("{}.{}", parts[0], parts[1]);
        verifying
            .verify(signing_input.as_bytes(), &signature)
            .expect("the signature must verify under the key file's own key");
        // …and must NOT verify over anything else, or the check above would
        // pass for a signature over the wrong bytes.
        assert!(
            verifying.verify(b"some.other.input", &signature).is_err(),
            "verification must be sensitive to the signed bytes"
        );
    }

    /// The grant body, form-urlencoded, against a hand-written literal.
    #[test]
    fn jwt_bearer_grant_body_is_the_rfc7523_grant() {
        let body = jwt_bearer_grant_body("aGVhZGVy.cGF5bG9hZA.c2ln");
        assert_eq!(
            body.as_str(),
            "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer\
             &assertion=aGVhZGVy.cGF5bG9hZA.c2ln"
        );
    }

    /// The shared seam DISPATCHES on the credential shape: a key file produces
    /// the jwt-bearer grant carrying the scope the CONSUMER asked for (unlike
    /// the refresh_token arm, where the scope is inherited and ignored), and it
    /// carries no key material.
    #[test]
    fn service_account_grant_body_is_the_jwt_bearer_grant_at_the_callers_scope() {
        let c = sa_credentials();
        let body = c.grant_body(BIGQUERY_SCOPE).unwrap();
        let assertion = body
            .strip_prefix(
                "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=",
            )
            .unwrap_or_else(|| panic!("not a jwt-bearer grant: {}", body.as_str()));

        let payload: serde_json::Value =
            serde_json::from_slice(&b64url(assertion.split('.').nth(1).unwrap())).unwrap();
        assert_eq!(
            payload["scope"], BIGQUERY_SCOPE,
            "the SA arm's scope is load-bearing — the token grants exactly this"
        );
        assert_eq!(payload["iss"], TEST_SA_EMAIL);

        // The private key SIGNS; it is never sent. (A body carrying it would
        // exfiltrate the credential to the token endpoint on every refresh.)
        let key_body = TEST_RSA_PKCS8_PEM
            .lines()
            .nth(1)
            .expect("the PEM has a body line");
        assert!(
            !body.contains(key_body),
            "the grant body must not carry key material"
        );
    }

    /// The seam reads the WALL clock when it signs — `iat`/`exp` are compared
    /// against GOOGLE's clock, so a frozen or fabricated stamp buys an
    /// assertion that is rejected as expired.
    #[test]
    fn service_account_grant_body_stamps_the_current_time() {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let c = sa_credentials();
        let body = c.grant_body(GCS_SCOPE).unwrap();
        let assertion = body.rsplit("assertion=").next().unwrap();
        let payload: serde_json::Value =
            serde_json::from_slice(&b64url(assertion.split('.').nth(1).unwrap())).unwrap();
        let iat = payload["iat"].as_u64().expect("iat is a number");
        assert!(
            iat.abs_diff(now) < 60,
            "iat {iat} must be the current time (now {now}), not a frozen stamp"
        );
        assert_eq!(
            payload["exp"].as_u64().unwrap(),
            iat + SA_ASSERTION_TTL_SECS,
            "the assertion must expire an hour after it was issued"
        );
    }

    // ── x-goog-user-project: per credential shape ────────────────────────────

    fn blocking_source(creds: AdcCredentials) -> BlockingAdcTokenSource {
        BlockingAdcTokenSource::new(creds, reqwest::blocking::Client::new())
    }

    /// A USER credential is not billable on its own, so the header is always
    /// sent: the ADC file's quota project when it names one, else the project
    /// being loaded into.
    #[test]
    fn a_user_credential_always_names_a_quota_project() {
        let named = parse_adc_file(
            r#"{"type":"authorized_user","client_id":"cid","client_secret":"s","refresh_token":"r",
                "quota_project_id":"billing-proj"}"#,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            blocking_source(named).quota_project_header("load-proj"),
            Some("billing-proj")
        );

        let unnamed = parse_adc_file(
            r#"{"type":"authorized_user","client_id":"cid","client_secret":"s","refresh_token":"r"}"#,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            blocking_source(unnamed).quota_project_header("load-proj"),
            Some("load-proj")
        );
    }

    /// A SERVICE ACCOUNT must be sent NO quota project.
    ///
    /// `x-goog-user-project` makes BigQuery require `serviceusage.services.use`
    /// on the named project — a permission `bigquery.jobUser` +
    /// `bigquery.dataOwner` do not grant. So defaulting the header to "the
    /// project being loaded into" (right for a user credential) would turn a
    /// correctly-provisioned service account into a 403 that reads like a
    /// BigQuery problem.
    #[test]
    fn a_service_account_is_sent_no_quota_project() {
        assert_eq!(
            blocking_source(sa_credentials()).quota_project_header("load-proj"),
            None,
            "a service account bills itself — naming a quota project demands a \
             permission its load roles do not include"
        );
    }

    // ── redaction: the key file's private_key ────────────────────────────────

    /// Credentials whose key material is a MARKER, built without the parser
    /// (which would reject a non-PEM key) so redaction can be checked against a
    /// string the test controls.
    fn sa_credentials_with_marked_key() -> AdcCredentials {
        AdcCredentials::ServiceAccount(ServiceAccountKey {
            client_email: TEST_SA_EMAIL.to_string(),
            private_key_pem: Zeroizing::new(
                "-----BEGIN PRIVATE KEY-----PRIVATEKEYMARKER-----END PRIVATE KEY-----".to_string(),
            ),
            token_uri: GOOGLE_TOKEN_URL.to_string(),
        })
    }

    /// Both halves, per the redaction rule: the secret must be ABSENT, and what
    /// remains must still identify the credential — an empty `Debug` impl
    /// satisfies an absence-only test while telling an operator nothing.
    #[test]
    fn service_account_debug_never_leaks_the_private_key() {
        let creds = format!("{:?}", sa_credentials_with_marked_key());
        let loader = format!(
            "{:?}",
            AdcUserTokenLoader {
                creds: sa_credentials_with_marked_key(),
                minted: Mutex::new(None),
            }
        );
        let blocking = format!("{:?}", blocking_source(sa_credentials_with_marked_key()));

        for (what, dbg) in [
            ("AdcCredentials", &creds),
            ("AdcUserTokenLoader", &loader),
            ("BlockingAdcTokenSource", &blocking),
        ] {
            assert!(
                !dbg.contains("PRIVATEKEYMARKER"),
                "{what} leaked the private key: {dbg}"
            );
            assert!(dbg.contains(what), "{what} must still name the type: {dbg}");
            assert!(
                dbg.contains("service_account"),
                "{what} must say WHICH credential shape it holds: {dbg}"
            );
            assert!(
                dbg.contains(TEST_SA_EMAIL),
                "{what} must still name the (public) principal — the one field \
                 that tells an operator which identity a run acts as: {dbg}"
            );
        }
    }

    /// `external_account` (workload identity) needs an STS exchange rivet does
    /// not implement. It must keep reading as `Ok(None)` so the caller falls
    /// back to `gcloud` — a HALF-implemented STS flow would be worse than the
    /// subprocess, and returning a broken credential worse still.
    #[test]
    fn parse_adc_external_account_still_returns_none_for_the_gcloud_fallback() {
        let json = r#"{"type":"external_account","audience":"//iam.googleapis.com/x",
                       "subject_token_type":"urn:ietf:params:oauth:token-type:jwt",
                       "token_url":"https://sts.googleapis.com/v1/token"}"#;
        assert!(parse_adc_file(json).unwrap().is_none());
    }
}
