//! The BigQuery **REST transport** — `jobs.insert` + poll, in process.
//!
//! This is the seam that used to be one `Command::new("bq")`. Everything the
//! loader does is a query job (a free `LOAD DATA`, a billed CTAS, a `COUNT(*)`
//! metadata read), so one verb serves all of them:
//!
//! 1. `POST /bigquery/v2/projects/{project}/jobs` — insert the query job,
//!    labels in `configuration.labels`.
//! 2. `GET  /bigquery/v2/projects/{project}/jobs/{jobId}?location=…` — poll
//!    until `status.state == "DONE"`; `status.errorResult` is the verdict.
//! 3. `GET  /bigquery/v2/projects/{project}/queries/{jobId}?location=…` —
//!    `getQueryResults`, only when the caller wants the rows back (the count).
//!
//! ## What decides success
//!
//! Exactly what `bq`'s exit status decided: the job's `status.errorResult`. A
//! `status.errors[]` entry WITHOUT an `errorResult` is a non-fatal warning and
//! stays non-fatal here — it only enriches the message when the job did fail.
//!
//! ## Location
//!
//! Not sent by default: BigQuery infers a query job's location from the
//! datasets it references, exactly as the CLI relied on (verified live against
//! an `EU` dataset — the insert response came back `jobReference.location:
//! "EU"`). Polling then uses the `jobReference` the insert RETURNED, so a
//! non-US job is found by its `jobs.get`. `RIVET_BQ_LOCATION` pins it for the
//! case where inference cannot see a dataset.
//!
//! ## Retries
//!
//! Only the polling GETs are retried (they are idempotent). `jobs.insert` is
//! NOT: a retried insert would create a SECOND job, and `LOAD DATA INTO` (the
//! CDC change-log append) is not idempotent — a duplicate append is silent row
//! inflation. A transport error on insert fails the load loudly instead.

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use zeroize::Zeroizing;

use crate::destination::gcs_auth::{self, BlockingAdcTokenSource};

/// BigQuery's JSON API root. Overridable so a live check can be pointed at a
/// stub; not a config knob.
const DEFAULT_ENDPOINT: &str = "https://bigquery.googleapis.com";

/// Per-request ceiling. Generous: a `LOAD DATA` insert only ENQUEUES the job
/// (the wait happens in the poll loop), but a large statement body plus a slow
/// link should not trip a short timeout.
const HTTP_TIMEOUT: Duration = Duration::from_secs(120);

/// Poll backoff bounds. Starts tight so a metadata `COUNT(*)` (sub-second)
/// returns promptly, and widens so a multi-minute load costs a handful of
/// requests rather than hundreds.
const POLL_MIN: Duration = Duration::from_millis(200);
const POLL_MAX: Duration = Duration::from_secs(5);

/// Consecutive transient failures (429 / 5xx / connection error) tolerated on
/// one polling GET before the load gives up.
const MAX_TRANSIENT_RETRIES: u32 = 5;

// ── the client ───────────────────────────────────────────────────────────────

/// A blocking BigQuery JSON-API client scoped to one project.
pub(crate) struct BigQueryApi {
    project: String,
    /// `jobReference.location` for inserted jobs; `None` lets BigQuery infer it
    /// from the referenced datasets (the CLI's behaviour).
    location: Option<String>,
    endpoint: String,
    http: reqwest::blocking::Client,
    auth: Auth,
}

impl std::fmt::Debug for BigQueryApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // SecOps: never render `auth` — it can hold a bearer token.
        f.debug_struct("BigQueryApi")
            .field("project", &self.project)
            .field("location", &self.location)
            .finish_non_exhaustive()
    }
}

/// Where a bearer token comes from, resolved once at client construction.
enum Auth {
    /// An operator-supplied token (`RIVET_BQ_ACCESS_TOKEN`) — impersonation,
    /// CI, or any credential shape rivet cannot mint itself.
    Static(Zeroizing<String>),
    /// ADC credentials — `authorized_user` (refresh_token grant) or a
    /// `service_account` key file (RS256 jwt-bearer grant), both minted in
    /// process through the shared `gcs_auth` seam.
    Adc(BlockingAdcTokenSource),
    /// Documented fallback for the credential shapes rivet has no in-process
    /// minting path for: `external_account` / workload identity (needs an STS
    /// exchange against a provider rivet does not model) and GCE/GKE metadata
    /// (no ADC file at all). One token from the CLI, not the whole transport.
    GcloudCli,
}

impl BigQueryApi {
    /// Build a client for `project`, resolving the token source eagerly so a
    /// missing credential is reported before the first job is enqueued.
    pub(crate) fn new(project: &str) -> Result<Self> {
        let http = reqwest::blocking::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .context("building the BigQuery HTTP client")?;
        Ok(Self {
            project: project.to_string(),
            location: std::env::var("RIVET_BQ_LOCATION")
                .ok()
                .filter(|s| !s.is_empty()),
            endpoint: std::env::var("RIVET_BQ_API_ENDPOINT")
                .ok()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| DEFAULT_ENDPOINT.to_string()),
            auth: Auth::resolve(&http)?,
            http,
        })
    }

    /// Run a query job to completion, tagged with `labels`. `Ok(job_id)` when
    /// BigQuery reports no `errorResult`.
    pub(crate) fn run_query(&self, sql: &str, labels: &BTreeMap<String, String>) -> Result<String> {
        let job_ref = self.settle(self.insert_query_job(sql, labels)?)?;
        Ok(job_ref.job_id)
    }

    /// Run a query job and read the first cell of its first row as a `u64` —
    /// the `COUNT(*)` shape. Separate from [`BigQueryApi::run_query`] so the
    /// ordinary statement path never pays a `getQueryResults` round trip.
    pub(crate) fn run_query_scalar(
        &self,
        sql: &str,
        labels: &BTreeMap<String, String>,
    ) -> Result<u64> {
        let job_ref = self.settle(self.insert_query_job(sql, labels)?)?;
        let results = self.get_json(&self.query_results_url(&job_ref), "getQueryResults")?;
        parse_scalar_u64(&results)
    }

    /// Take an inserted job to a terminal state. A short statement often
    /// completes inside the insert call, so the body already in hand is graded
    /// before a poll is spent.
    fn settle(&self, job: Value) -> Result<JobRef> {
        let job_ref = parse_job_reference(&job, &self.project)?;
        match job_outcome(&job) {
            JobOutcome::Done => Ok(job_ref),
            JobOutcome::Failed(detail) => bail!("{}", job_failed_message(&job_ref.job_id, &detail)),
            JobOutcome::Running => self.await_job(&job_ref).map(|_| job_ref),
        }
    }

    fn insert_query_job(&self, sql: &str, labels: &BTreeMap<String, String>) -> Result<Value> {
        let body = query_job_body(sql, labels, &self.project, self.location.as_deref());
        let url = format!(
            "{}/bigquery/v2/projects/{}/jobs",
            self.endpoint, self.project
        );
        let resp = self
            .authorized(self.http.post(&url))?
            .json(&body)
            .send()
            .context("BigQuery jobs.insert request failed")?;
        self.read_json(resp, "jobs.insert")
    }

    /// Poll `jobs.get` until the job reaches a terminal state.
    fn await_job(&self, job_ref: &JobRef) -> Result<String> {
        let url = self.job_url(job_ref);
        for attempt in 0.. {
            std::thread::sleep(poll_interval(attempt));
            let job = self.get_json(&url, "jobs.get")?;
            match job_outcome(&job) {
                JobOutcome::Running => continue,
                JobOutcome::Done => return Ok(job_ref.job_id.clone()),
                JobOutcome::Failed(detail) => {
                    bail!("{}", job_failed_message(&job_ref.job_id, &detail))
                }
            }
        }
        unreachable!("the poll loop only exits through a terminal state")
    }

    fn job_url(&self, job_ref: &JobRef) -> String {
        format!(
            "{}/bigquery/v2/projects/{}/jobs/{}{}",
            self.endpoint,
            job_ref.project,
            job_ref.job_id,
            location_query(job_ref.location.as_deref())
        )
    }

    fn query_results_url(&self, job_ref: &JobRef) -> String {
        format!(
            "{}/bigquery/v2/projects/{}/queries/{}{}&maxResults=1",
            self.endpoint,
            job_ref.project,
            job_ref.job_id,
            // `?location=…` when set, else a bare `?` so `&maxResults` parses.
            match job_ref.location.as_deref() {
                Some(loc) => format!("?location={loc}"),
                None => "?".to_string(),
            }
        )
    }

    /// An idempotent GET with transient-failure retries. `what` names the
    /// endpoint in any error, so an operator reading the message knows whether
    /// the poll or the result fetch was refused.
    fn get_json(&self, url: &str, what: &str) -> Result<Value> {
        let mut last: anyhow::Error = anyhow::anyhow!("no attempt made");
        for attempt in 0..=MAX_TRANSIENT_RETRIES {
            if attempt > 0 {
                std::thread::sleep(poll_interval(attempt));
            }
            let sent = self.authorized(self.http.get(url))?.send();
            match sent {
                Ok(resp) if is_transient_status(resp.status().as_u16()) => {
                    last = anyhow::anyhow!(
                        "BigQuery replied HTTP {} (transient)",
                        resp.status().as_u16()
                    );
                }
                Ok(resp) => return self.read_json(resp, what),
                // A connection reset mid-poll is the same class as a 503.
                Err(e) => last = anyhow::Error::new(e).context("BigQuery request failed"),
            }
        }
        Err(last).context(format!(
            "BigQuery {what} kept failing after {MAX_TRANSIENT_RETRIES} retries"
        ))
    }

    /// Turn a response into JSON, mapping a non-2xx into the API's own message.
    fn read_json(&self, resp: reqwest::blocking::Response, what: &str) -> Result<Value> {
        let status = resp.status().as_u16();
        let body = resp.text().unwrap_or_default();
        if !(200..300).contains(&status) {
            bail!(
                "BigQuery {what} failed: {}",
                api_error_detail(status, &body)
            );
        }
        serde_json::from_str(&body)
            .with_context(|| format!("parsing the BigQuery {what} response as JSON"))
    }

    /// Attach the bearer token and, for a USER credential, the quota project
    /// BigQuery bills the call to.
    fn authorized(
        &self,
        req: reqwest::blocking::RequestBuilder,
    ) -> Result<reqwest::blocking::RequestBuilder> {
        let token = self.auth.access_token()?;
        let mut req = req.header("Authorization", format!("Bearer {}", token.as_str()));
        // ADC USER credentials are not billable on their own: a metered JSON
        // API rejects them without a quota project. A SERVICE ACCOUNT is
        // billable on its own and must NOT be sent one — the header would make
        // BigQuery demand `serviceusage.services.use`, which the roles that
        // grant a load (`bigquery.jobUser` + `bigquery.dataOwner`) do not
        // include. The credential decides; see `quota_project_header`.
        if let Auth::Adc(src) = &self.auth
            && let Some(quota_project) = src.quota_project_header(&self.project)
        {
            req = req.header("x-goog-user-project", quota_project);
        }
        Ok(req)
    }
}

/// Which token source wins, given what is available. Pure, so the precedence
/// is graded offline — the resolution itself reads process env and an ADC file
/// on disk, which no unit test can pin without racing its siblings.
///
/// An explicit token beats a discovered credential (that is the point of
/// setting one); a BLANK one is treated as unset, since an empty
/// `RIVET_BQ_ACCESS_TOKEN` from an unset shell variable must not shadow working
/// ADC credentials with a guaranteed 401.
pub(crate) fn choose_token_source(static_token: Option<&str>, has_adc: bool) -> TokenSourceKind {
    match static_token {
        Some(t) if !t.trim().is_empty() => TokenSourceKind::Static,
        _ if has_adc => TokenSourceKind::Adc,
        _ => TokenSourceKind::GcloudCli,
    }
}

/// The choice [`choose_token_source`] makes, named so a test can assert it
/// without holding a credential.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TokenSourceKind {
    Static,
    Adc,
    GcloudCli,
}

impl Auth {
    fn resolve(http: &reqwest::blocking::Client) -> Result<Self> {
        let static_token = std::env::var("RIVET_BQ_ACCESS_TOKEN").ok();
        let adc = gcs_auth::load_adc_credentials()?;
        match choose_token_source(static_token.as_deref(), adc.is_some()) {
            TokenSourceKind::Static => Ok(Auth::Static(Zeroizing::new(
                static_token.expect("a Static choice implies a token"),
            ))),
            TokenSourceKind::Adc => {
                let src = BlockingAdcTokenSource::new(
                    adc.expect("an Adc choice implies credentials"),
                    http.clone(),
                );
                // Say WHICH identity the jobs will run as, at resolution time.
                // A load that silently acts as a different principal than the
                // operator configured is an audit trail that reads as fiction
                // — and the only cheap way to notice is for rivet to name it.
                log::info!(
                    "BigQuery: authenticating with ADC {} credentials as {}",
                    src.credential_kind(),
                    src.principal()
                );
                Ok(Auth::Adc(src))
            }
            TokenSourceKind::GcloudCli => Ok(Auth::GcloudCli),
        }
    }

    fn access_token(&self) -> Result<Zeroizing<String>> {
        match self {
            Auth::Static(t) => Ok(t.clone()),
            Auth::Adc(src) => src.access_token(),
            Auth::GcloudCli => mint_token_via_gcloud_cli(),
        }
    }
}

/// The fallback's argv, as a VALUE rather than a literal buried in the spawn.
///
/// A test that greps this file's TEXT would kill mutants here without ever
/// executing the code — which contradicts the mutation gate's coverage-based
/// classification (`mint_token_via_gcloud_cli` measures at zero executions and
/// lands in the report-only class, yet a source-lint kills its mutants, and the
/// gate's own P2 audit correctly flags that as an oracle that lied). Naming the
/// argv makes the verb observable by VALUE, so the test executes real code and
/// the classification stays honest.
pub(crate) const GCLOUD_TOKEN_ARGV: [&str; 3] =
    ["auth", "application-default", "print-access-token"];

/// The ONE place rivet still shells out for BigQuery, and only for a token.
///
/// Reached when the ADC file is absent, or holds a shape rivet cannot mint in
/// process. That set is now exactly:
///
/// - `external_account` / workload identity — an STS token exchange against a
///   provider (AWS, OIDC, SAML, an executable source) rivet does not model. A
///   half-implemented subset here would be worse than the subprocess.
/// - GCE / GKE / Cloud Run metadata — no ADC file at all; the token comes from
///   the instance metadata server.
///
/// `authorized_user` (refresh_token) and `service_account` (RS256 jwt-bearer)
/// are BOTH minted in process by `destination::gcs_auth` and never reach here.
/// Nothing else about the transport is a subprocess.
///
/// THE VERB IS STILL LOAD-BEARING for the two shapes above: `auth
/// application-default print-access-token` resolves the SAME credential chain
/// the GCS leg uses, honouring `GOOGLE_APPLICATION_CREDENTIALS`. The bare `auth
/// print-access-token` mints for whatever human account `gcloud` happens to be
/// logged in as — measured 2026-08-23 against a real service account, before
/// that shape moved in process: the bare verb produced `user_email = <a human>`
/// in INFORMATION_SCHEMA.JOBS_BY_PROJECT while the GCS leg wrote as the service
/// account. A load must act as the identity the operator configured, or its
/// audit trail is fiction.
fn mint_token_via_gcloud_cli() -> Result<Zeroizing<String>> {
    let out = std::process::Command::new("gcloud")
        .args(GCLOUD_TOKEN_ARGV)
        .output()
        .context(
            "no ADC credentials rivet can mint in process (authorized_user / service_account) \
             and `gcloud` is not on PATH — run `gcloud auth application-default login`, point \
             GOOGLE_APPLICATION_CREDENTIALS at a service-account key file, or set \
             RIVET_BQ_ACCESS_TOKEN",
        )?;
    if !out.status.success() {
        bail!(
            "`gcloud auth application-default print-access-token` failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    let token = Zeroizing::new(String::from_utf8_lossy(&out.stdout).trim().to_string());
    if token.is_empty() {
        bail!("`gcloud auth print-access-token` returned an empty token");
    }
    Ok(token)
}

// ── pure request / response shapes ───────────────────────────────────────────

/// The `jobs.insert` body for a query job: the statement, `useLegacySql:false`
/// (the CLI's `--use_legacy_sql=false`), and the cost-attribution labels in
/// `configuration.labels` — where `INFORMATION_SCHEMA.JOBS.labels` reads them
/// from, so the billing queries in the loader's docs keep working unchanged.
///
/// `jobReference` is emitted ONLY to pin a location; omitting it lets BigQuery
/// generate the job id and infer the location from the referenced datasets.
pub(crate) fn query_job_body(
    sql: &str,
    labels: &BTreeMap<String, String>,
    project: &str,
    location: Option<&str>,
) -> Value {
    let mut body = json!({
        "configuration": {
            "query": { "query": sql, "useLegacySql": false },
            "labels": labels,
        }
    });
    if let Some(loc) = location {
        body["jobReference"] = json!({ "projectId": project, "location": loc });
    }
    body
}

/// The identity a job is polled by. `location` comes from what the insert
/// RETURNED — `jobs.get` on a non-US job 404s without it.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct JobRef {
    pub(crate) project: String,
    pub(crate) job_id: String,
    pub(crate) location: Option<String>,
}

/// Read the `jobReference` out of an inserted job. Falls back to the requesting
/// project so a reply missing `projectId` still polls the right project.
pub(crate) fn parse_job_reference(job: &Value, fallback_project: &str) -> Result<JobRef> {
    let r = job.get("jobReference");
    let job_id = r
        .and_then(|r| r.get("jobId"))
        .and_then(Value::as_str)
        .context("BigQuery jobs.insert returned no jobReference.jobId")?;
    Ok(JobRef {
        project: r
            .and_then(|r| r.get("projectId"))
            .and_then(Value::as_str)
            .unwrap_or(fallback_project)
            .to_string(),
        job_id: job_id.to_string(),
        location: r
            .and_then(|r| r.get("location"))
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

/// Where a polled job stands.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum JobOutcome {
    Running,
    Done,
    Failed(String),
}

/// The success verdict, and the ONLY one: `status.errorResult`.
///
/// `status.errors[]` without an `errorResult` is BigQuery's non-fatal warning
/// channel (it appears on jobs that succeeded), so it must not fail a load —
/// the CLI's exit status did not, either. It only enriches a failure message.
pub(crate) fn job_outcome(job: &Value) -> JobOutcome {
    let status = job.get("status");
    if let Some(err) = status.and_then(|s| s.get("errorResult")) {
        return JobOutcome::Failed(job_error_detail(err, status.and_then(|s| s.get("errors"))));
    }
    match status.and_then(|s| s.get("state")).and_then(Value::as_str) {
        Some("DONE") => JobOutcome::Done,
        _ => JobOutcome::Running,
    }
}

/// The user-visible reason a job failed: `errorResult.message`, then any
/// `errors[]` message that says something new, `|`-joined — the same
/// multi-part detail the CLI path assembled from stdout+stderr.
fn job_error_detail(error_result: &Value, errors: Option<&Value>) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(m) = error_result.get("message").and_then(Value::as_str) {
        parts.push(m.trim().to_string());
    }
    for m in errors
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .filter_map(|e| e.get("message").and_then(Value::as_str))
    {
        let m = m.trim().to_string();
        if !parts.contains(&m) {
            parts.push(m);
        }
    }
    if parts.is_empty() {
        // No message at all — surface the reason code rather than an empty
        // "failed:" that tells the operator nothing.
        parts.push(format!(
            "reason: {}",
            error_result
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
        ));
    }
    parts.join(" | ")
}

/// The failure message a caller sees, naming the job so the operator can look
/// it up in `INFORMATION_SCHEMA.JOBS` / the console.
pub(crate) fn job_failed_message(job_id: &str, detail: &str) -> String {
    format!("BigQuery job {job_id} failed: {detail}")
}

/// The reason behind a non-2xx JSON-API reply: `error.message`, plus any
/// `error.errors[]` message that adds something. Falls back to the raw body
/// (bounded) when the payload is not the standard error envelope — an HTML
/// proxy error must still reach the operator.
pub(crate) fn api_error_detail(status: u16, body: &str) -> String {
    let parsed: Option<Value> = serde_json::from_str(body).ok();
    let err = parsed.as_ref().and_then(|v| v.get("error"));
    if let Some(err) = err {
        let detail = job_error_detail(err, err.get("errors"));
        if !detail.is_empty() {
            return format!("HTTP {status}: {detail}");
        }
    }
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return format!("HTTP {status} (empty response body)");
    }
    format!("HTTP {status}: {}", truncate(trimmed, 500))
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let head: String = s.chars().take(max).collect();
    format!("{head}…")
}

/// The first cell of the first row of a `getQueryResults` reply, as a `u64`.
///
/// BigQuery renders every cell as a STRING (`rows[0].f[0].v`), including
/// INT64 — so this parses text, and an absent row is an error rather than a
/// silent zero (a `COUNT(*)` that returns nothing means the query did not run,
/// not that the table is empty).
pub(crate) fn parse_scalar_u64(results: &Value) -> Result<u64> {
    let cell = results
        .get("rows")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("f"))
        .and_then(Value::as_array)
        .and_then(|fields| fields.first())
        .and_then(|f| f.get("v"))
        .context("BigQuery getQueryResults returned no rows[0].f[0].v")?;
    match cell {
        Value::String(s) => s
            .parse::<u64>()
            .with_context(|| format!("BigQuery returned a non-numeric count: {s}")),
        Value::Number(n) => n
            .as_u64()
            .context("BigQuery returned a count that is not a non-negative integer"),
        other => bail!("BigQuery returned a count of an unexpected JSON type: {other}"),
    }
}

/// `?location=<loc>` when the job has one, else empty.
fn location_query(location: Option<&str>) -> String {
    location
        .map(|l| format!("?location={l}"))
        .unwrap_or_default()
}

/// Backoff for poll attempt `n`: exponential from [`POLL_MIN`], capped at
/// [`POLL_MAX`]. Attempt 0 waits the minimum, so a sub-second metadata job is
/// not billed a full second of latency.
fn poll_interval(attempt: u32) -> Duration {
    let scaled = POLL_MIN.saturating_mul(1u32 << attempt.min(16));
    scaled.min(POLL_MAX)
}

/// HTTP statuses worth retrying an IDEMPOTENT request on: rate limiting and
/// any server-side fault. Everything else (401, 403, 404, 400) is a decision
/// BigQuery has made and repeating it changes nothing.
fn is_transient_status(code: u16) -> bool {
    code == 429 || (500..600).contains(&code)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The gcloud fallback must ask for the APPLICATION-DEFAULT token, not the
    /// logged-in user's.
    ///
    /// `gcloud auth print-access-token` mints for whatever human account gcloud
    /// is logged in as and ignores `GOOGLE_APPLICATION_CREDENTIALS`; only
    /// `gcloud auth application-default print-access-token` resolves the same
    /// credential chain the GCS leg uses. With a service-account credential
    /// configured, the bare verb made rivet write GCS as the service account
    /// and call BigQuery as a PERSON — and the load still SUCCEEDED whenever
    /// that person held the rights, so nothing surfaced the substitution.
    /// Measured 2026-08-23 against a real service account: bare verb →
    /// `user_email = <a human>` in INFORMATION_SCHEMA.JOBS_BY_PROJECT, ADC verb
    /// → the service account.
    ///
    /// The source text is the subject because the verb is an argv, not a value
    /// this crate can observe without spawning gcloud. RED against the
    /// pre-fix argv.
    #[test]
    fn the_gcloud_fallback_asks_for_the_application_default_token() {
        // By VALUE, not by grepping this file: a source-lint would kill mutants
        // in a function the offline suite never executes, which is exactly the
        // contradiction the gate's P2 audit exists to catch.
        assert_eq!(
            GCLOUD_TOKEN_ARGV,
            ["auth", "application-default", "print-access-token"],
            "the fallback must mint the APPLICATION-DEFAULT token — the bare verb \
             authenticates as whatever human gcloud is logged in as while the GCS \
             leg uses the configured service account, so one load acts as two \
             identities and the warehouse audit trail becomes fiction"
        );
        assert!(
            GCLOUD_TOKEN_ARGV.contains(&"application-default"),
            "guard the guard: the ADC segment is the whole point of this argv"
        );
    }

    fn labels() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("managed_by".to_string(), "rivet".to_string()),
            ("rivet_op".to_string(), "load".to_string()),
        ])
    }

    /// The insert body must put the statement where BigQuery reads it, disable
    /// legacy SQL (the CLI's `--use_legacy_sql=false`), and put the labels in
    /// `configuration.labels` — the field `INFORMATION_SCHEMA.JOBS.labels`
    /// projects. A label map written anywhere else costs cost-attribution
    /// silently: the job runs, the billing query returns nothing.
    #[test]
    fn query_job_body_carries_sql_and_labels_where_bigquery_reads_them() {
        let b = query_job_body("SELECT 1", &labels(), "proj", None);
        assert_eq!(b["configuration"]["query"]["query"], "SELECT 1");
        assert_eq!(b["configuration"]["query"]["useLegacySql"], false);
        assert_eq!(b["configuration"]["labels"]["managed_by"], "rivet");
        assert_eq!(b["configuration"]["labels"]["rivet_op"], "load");
        // No location asked for ⇒ no jobReference at all, so BigQuery both
        // generates the job id and infers the location from the datasets.
        assert!(b.get("jobReference").is_none(), "{b}");
    }

    #[test]
    fn query_job_body_pins_the_location_when_one_is_configured() {
        let b = query_job_body("SELECT 1", &labels(), "proj", Some("EU"));
        assert_eq!(b["jobReference"]["location"], "EU");
        assert_eq!(b["jobReference"]["projectId"], "proj");
        // Still no jobId — a client-side id would collide across runs.
        assert!(b["jobReference"].get("jobId").is_none(), "{b}");
    }

    /// Polling a non-US job by id alone 404s, so the location the insert
    /// RETURNED has to survive into the JobRef. Fixture is the shape verified
    /// live against an EU dataset (2026-08-22).
    #[test]
    fn job_reference_keeps_the_location_the_insert_returned() {
        let job = json!({"jobReference": {
            "projectId": "rivet-data-tool", "jobId": "job_abc", "location": "EU"
        }});
        assert_eq!(
            parse_job_reference(&job, "other").unwrap(),
            JobRef {
                project: "rivet-data-tool".into(),
                job_id: "job_abc".into(),
                location: Some("EU".into()),
            }
        );
    }

    #[test]
    fn job_reference_falls_back_to_the_requesting_project_and_errors_without_an_id() {
        let job = json!({"jobReference": {"jobId": "job_abc"}});
        let r = parse_job_reference(&job, "fallback-proj").unwrap();
        assert_eq!(r.project, "fallback-proj");
        assert_eq!(r.location, None);
        assert!(parse_job_reference(&json!({"status": {"state": "DONE"}}), "p").is_err());
    }

    #[test]
    fn a_running_job_is_not_terminal() {
        assert_eq!(
            job_outcome(&json!({"status": {"state": "RUNNING"}})),
            JobOutcome::Running
        );
        assert_eq!(
            job_outcome(&json!({"status": {"state": "PENDING"}})),
            JobOutcome::Running
        );
        // No status at all is NOT success — poll again rather than declare done.
        assert_eq!(job_outcome(&json!({})), JobOutcome::Running);
    }

    #[test]
    fn a_done_job_without_an_error_result_succeeds() {
        assert_eq!(
            job_outcome(&json!({"status": {"state": "DONE"}})),
            JobOutcome::Done
        );
    }

    /// THE verdict test. `errorResult` fails the job; `errors[]` WITHOUT an
    /// `errorResult` is BigQuery's non-fatal warning channel and must stay
    /// non-fatal — treating it as failure would fail loads the `bq` exit status
    /// passed, which is exactly the "don't silently change what counts as
    /// success" line.
    #[test]
    fn error_result_fails_the_job_but_a_bare_errors_list_does_not() {
        let failed = json!({"status": {
            "state": "DONE",
            "errorResult": {"reason": "notFound", "message": "Not found: Table p:d.t"},
            "errors": [{"reason": "notFound", "message": "Not found: Table p:d.t"}]
        }});
        // The duplicate `errors[]` copy is folded away, not repeated.
        assert_eq!(
            job_outcome(&failed),
            JobOutcome::Failed("Not found: Table p:d.t".into())
        );

        let warned = json!({"status": {
            "state": "DONE",
            "errors": [{"reason": "stopped", "message": "a non-fatal warning"}]
        }});
        assert_eq!(job_outcome(&warned), JobOutcome::Done);
    }

    /// A distinct `errors[]` entry ADDS detail; the old CLI path joined its two
    /// streams with " | " and this keeps that shape.
    #[test]
    fn distinct_error_messages_are_pipe_joined() {
        let job = json!({"status": {
            "state": "DONE",
            "errorResult": {"reason": "invalid", "message": "top level"},
            "errors": [
                {"message": "top level"},
                {"message": "the underlying cause"}
            ]
        }});
        assert_eq!(
            job_outcome(&job),
            JobOutcome::Failed("top level | the underlying cause".into())
        );
    }

    /// A failure with no message must not render as "failed: " — the reason
    /// code is the last informative thing left.
    #[test]
    fn a_messageless_failure_still_names_its_reason() {
        let job = json!({"status": {"state": "DONE", "errorResult": {"reason": "backendError"}}});
        assert_eq!(
            job_outcome(&job),
            JobOutcome::Failed("reason: backendError".into())
        );
    }

    /// The partition-quota augmentation is the one error-mapping the loader
    /// promises by name. This drives BigQuery's REAL job-failure envelope
    /// through the outcome mapper into `augment_partition_limit`, proving the
    /// reason survives the REST transport the way it used to survive `bq`'s
    /// stdout. TEXT-MATCHED: the quota surfaces as an ordinary `invalidQuery`
    /// errorResult whose MESSAGE names the limit — there is no distinct
    /// machine-readable reason code for it, so the augmenter matches the words.
    #[test]
    fn the_partition_quota_reason_survives_the_rest_transport_and_is_augmented() {
        let job = json!({"status": {
            "state": "DONE",
            "errorResult": {
                "reason": "invalidQuery",
                "message": "Too many partitions produced by query, allowed 4000, \
                            query produces at least 4200 partitions"
            }
        }});
        let JobOutcome::Failed(detail) = job_outcome(&job) else {
            panic!("expected a failed job");
        };
        assert!(detail.contains("Too many partitions") && detail.contains("4000"));
        let err = crate::load::bigquery::augment_partition_limit(anyhow::anyhow!(
            "{}",
            job_failed_message("job_x", &detail)
        ));
        assert!(err.to_string().contains("split the"), "{err}");
        // The augmentation is a `.context()`, so the reason and the job id live
        // in the CHAIN — the rendering an operator sees (`{:#}`), not the
        // headline alone.
        let chain = format!("{err:#}");
        assert!(
            chain.contains("job_x"),
            "the job id must reach the operator: {chain}"
        );
        assert!(chain.contains("Too many partitions"), "{chain}");
    }

    /// HTTP-level rejections carry a different envelope than job failures.
    /// Fixture is the shape verified live (a bad label key, 2026-08-22).
    #[test]
    fn http_error_detail_reads_the_api_error_envelope() {
        let body = r#"{"error": {"code": 400,
            "message": "Label key \"BAD KEY\" has invalid characters.",
            "errors": [{"message": "Label key \"BAD KEY\" has invalid characters.",
                        "reason": "invalid"}],
            "status": "INVALID_ARGUMENT"}}"#;
        let d = api_error_detail(400, body);
        assert!(d.contains("HTTP 400"), "{d}");
        assert!(d.contains("Label key"), "{d}");
        // The duplicated `errors[]` copy folds away.
        assert_eq!(d.matches("Label key").count(), 1, "{d}");
    }

    /// A proxy / load balancer answers with HTML, not the API envelope. The
    /// operator still needs to see it — dropping an unparseable body is how a
    /// "failed: " with no reason reaches a bug report.
    #[test]
    fn http_error_detail_falls_back_to_a_bounded_raw_body() {
        let d = api_error_detail(502, "<html>Bad Gateway</html>");
        assert!(d.contains("HTTP 502") && d.contains("Bad Gateway"), "{d}");
        assert_eq!(
            api_error_detail(503, "   "),
            "HTTP 503 (empty response body)"
        );
        let long = api_error_detail(500, &"x".repeat(2000));
        assert!(long.len() < 700 && long.ends_with('…'), "{}", long.len());
    }

    /// BigQuery renders every cell as a STRING, so the count arrives as `"42"`.
    /// Fixture is the shape verified live (2026-08-22).
    #[test]
    fn scalar_count_parses_the_string_cell() {
        let r = json!({"rows": [{"f": [{"v": "42"}]}], "totalRows": "1"});
        assert_eq!(parse_scalar_u64(&r).unwrap(), 42);
        assert_eq!(
            parse_scalar_u64(&json!({"rows": [{"f": [{"v": "0"}]}]})).unwrap(),
            0
        );
    }

    /// A reply with no rows means the query did not produce one — that must be
    /// an ERROR, never a silent 0. A `COUNT(*)` that reads as 0 when nothing
    /// ran would let the load driver's row gate pass on a missing table.
    #[test]
    fn scalar_count_refuses_to_invent_a_zero() {
        assert!(parse_scalar_u64(&json!({"totalRows": "0"})).is_err());
        assert!(parse_scalar_u64(&json!({"rows": []})).is_err());
        assert!(parse_scalar_u64(&json!({"rows": [{"f": []}]})).is_err());
        assert!(parse_scalar_u64(&json!({"rows": [{"f": [{"v": "abc"}]}]})).is_err());
        // NULL cell (`{"v": null}`) is not a count either.
        assert!(parse_scalar_u64(&json!({"rows": [{"f": [{"v": null}]}]})).is_err());
    }

    /// The full precedence truth table. The blank-token row is the one that
    /// bites in practice: `RIVET_BQ_ACCESS_TOKEN=""` (an unset shell variable
    /// expanded into the environment) must fall THROUGH to ADC, not pin an
    /// empty bearer token that 401s every job.
    #[test]
    fn an_explicit_token_wins_but_a_blank_one_falls_through_to_adc() {
        use TokenSourceKind::*;
        assert_eq!(choose_token_source(Some("ya29.x"), true), Static);
        assert_eq!(choose_token_source(Some("ya29.x"), false), Static);
        assert_eq!(choose_token_source(Some(""), true), Adc);
        assert_eq!(choose_token_source(Some("   "), true), Adc);
        assert_eq!(choose_token_source(None, true), Adc);
        // Nothing rivet can mint in process ⇒ the documented CLI token fallback.
        assert_eq!(choose_token_source(None, false), GcloudCli);
        assert_eq!(choose_token_source(Some(""), false), GcloudCli);
    }

    #[test]
    fn poll_interval_grows_from_the_floor_to_the_ceiling() {
        assert_eq!(poll_interval(0), POLL_MIN);
        assert_eq!(poll_interval(1), POLL_MIN * 2);
        assert_eq!(poll_interval(3), POLL_MIN * 8);
        // Capped, and never longer than the cap however far the loop runs.
        assert_eq!(poll_interval(9), POLL_MAX);
        assert_eq!(poll_interval(1_000), POLL_MAX);
        assert!(
            poll_interval(0) < poll_interval(2),
            "must actually back off"
        );
    }

    /// Retrying a 4xx repeats a decision BigQuery already made; retrying a 5xx
    /// or a 429 is the only thing that survives a blip. Pins both sides — a
    /// predicate that returned `true` for 403 would spin on a permission error.
    #[test]
    fn only_rate_limits_and_server_faults_are_transient() {
        for code in [429, 500, 502, 503, 504] {
            assert!(is_transient_status(code), "{code} should retry");
        }
        for code in [200, 400, 401, 403, 404, 409] {
            assert!(!is_transient_status(code), "{code} must not retry");
        }
    }

    #[test]
    fn location_query_is_empty_when_unset() {
        assert_eq!(location_query(Some("EU")), "?location=EU");
        assert_eq!(location_query(None), "");
    }

    /// The `getQueryResults` URL must keep `maxResults` parseable whether or not
    /// a location is present — a `?location=…&maxResults=1` and a bare
    /// `?&maxResults=1` are both valid; `…queries/id&maxResults=1` is not.
    #[test]
    fn query_results_url_always_opens_a_query_string() {
        let api = BigQueryApi {
            project: "p".into(),
            location: None,
            endpoint: "https://e".into(),
            http: reqwest::blocking::Client::new(),
            auth: Auth::Static(Zeroizing::new("t".into())),
        };
        let with_loc = api.query_results_url(&JobRef {
            project: "p".into(),
            job_id: "j".into(),
            location: Some("EU".into()),
        });
        assert_eq!(
            with_loc,
            "https://e/bigquery/v2/projects/p/queries/j?location=EU&maxResults=1"
        );
        let without = api.query_results_url(&JobRef {
            project: "p".into(),
            job_id: "j".into(),
            location: None,
        });
        assert_eq!(
            without,
            "https://e/bigquery/v2/projects/p/queries/j?&maxResults=1"
        );
    }

    #[test]
    fn job_url_addresses_the_jobs_get_endpoint() {
        let api = BigQueryApi {
            project: "p".into(),
            location: None,
            endpoint: "https://e".into(),
            http: reqwest::blocking::Client::new(),
            auth: Auth::Static(Zeroizing::new("t".into())),
        };
        let url = api.job_url(&JobRef {
            project: "other".into(),
            job_id: "j".into(),
            location: Some("EU".into()),
        });
        // Polls the job's OWN project, not the client's.
        assert_eq!(
            url,
            "https://e/bigquery/v2/projects/other/jobs/j?location=EU"
        );
    }

    /// SecOps: the client is reachable from a `Debug`-derived loader, so its
    /// rendering must never contain a bearer token.
    #[test]
    fn debug_never_renders_the_token() {
        let api = BigQueryApi {
            project: "p".into(),
            location: Some("EU".into()),
            endpoint: DEFAULT_ENDPOINT.into(),
            http: reqwest::blocking::Client::new(),
            auth: Auth::Static(Zeroizing::new("ya29.SECRET-TOKEN".into())),
        };
        let rendered = format!("{api:?}");
        assert!(!rendered.contains("SECRET"), "{rendered}");
        assert!(rendered.contains("EU"), "{rendered}");
    }
}
