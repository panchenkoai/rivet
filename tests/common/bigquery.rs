//! BigQuery and real-GCS helpers for live `rivet load` tests; they skip when the warehouse env is unset.

#![allow(dead_code)]

use std::process::Command;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// One kept-alive HTTPS client for every BigQuery / GCS call a test makes. REST, not
/// the `bq` / `gcloud` CLIs: a CLI start cost 2.3 s per query (measured over the
/// shared-state cycle: 64 queries, 149 s), a request on this client well under 1 s.
/// Plain HTTP with the operator's `gcloud` identity — shares no code with rivet's
/// own BigQuery client, so the read stays an independent oracle.
fn http() -> &'static reqwest::blocking::Client {
    static C: OnceLock<reqwest::blocking::Client> = OnceLock::new();
    C.get_or_init(|| {
        reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(300))
            .build()
            .expect("http client")
    })
}

/// `gcloud auth print-access-token`, re-asked every 4 minutes or on `refresh`: gcloud
/// hands back its CACHED token until it has under 5 minutes left, so one fetched now
/// may expire in 5, not 60. None when gcloud fails — never a poisoned lock.
fn token(refresh: bool) -> Option<String> {
    static T: OnceLock<Mutex<Option<(String, Instant)>>> = OnceLock::new();
    let mut slot = T
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    if !refresh
        && let Some((tok, at)) = slot.as_ref()
        && at.elapsed() < Duration::from_secs(4 * 60)
    {
        return Some(tok.clone());
    }
    let out = Command::new("gcloud")
        .args(["auth", "print-access-token"])
        .output()
        .ok()
        .filter(|o| o.status.success())?;
    let tok = String::from_utf8_lossy(&out.stdout).trim().to_string();
    *slot = Some((tok.clone(), Instant::now()));
    Some(tok)
}

/// (status, JSON body) of one authorised request, or None when it could not be made;
/// a 401 re-asks gcloud for a token once. Never panics — the Drop cleanups use it.
fn try_call(
    method: reqwest::Method,
    url: &str,
    body: Option<&serde_json::Value>,
) -> Option<(u16, serde_json::Value)> {
    for refresh in [false, true] {
        let mut req = http()
            .request(method.clone(), url)
            .bearer_auth(token(refresh)?);
        if let Some(b) = body {
            req = req.json(b);
        }
        let resp = req.send().ok()?;
        let status = resp.status().as_u16();
        if status == 401 && !refresh {
            continue;
        }
        let text = resp.text().unwrap_or_default();
        return Some((
            status,
            serde_json::from_str(&text).unwrap_or(serde_json::Value::Null),
        ));
    }
    None
}

/// [`try_call`] for a test body: a request that cannot be made fails the test loudly.
fn call(
    method: reqwest::Method,
    url: &str,
    body: Option<serde_json::Value>,
) -> (u16, serde_json::Value) {
    try_call(method, url, body.as_ref())
        .unwrap_or_else(|| panic!("{url}: no response (gcloud token or network)"))
}

/// A REST cell as `bq --format=json` renders it: scalars as strings, NULL as null,
/// REPEATED as an array, RECORD as an object. (TIMESTAMP comes back as epoch
/// seconds, not the CLI's formatted text — CAST it to STRING to compare text.)
fn render_cell(field: &serde_json::Value, v: &serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    if v.is_null() {
        return Value::Null;
    }
    if field["mode"] == "REPEATED" {
        let mut one = field.clone();
        one["mode"] = Value::from("NULLABLE");
        return Value::Array(
            v.as_array()
                .into_iter()
                .flatten()
                .map(|e| render_cell(&one, &e["v"]))
                .collect(),
        );
    }
    if field["type"] == "RECORD" || field["type"] == "STRUCT" {
        return render_row(&field["fields"], v);
    }
    v.clone()
}

fn render_row(fields: &serde_json::Value, row: &serde_json::Value) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    for (f, cell) in fields
        .as_array()
        .into_iter()
        .flatten()
        .zip(row["f"].as_array().into_iter().flatten())
    {
        obj.insert(
            f["name"].as_str().unwrap_or_default().to_string(),
            render_cell(f, &cell["v"]),
        );
    }
    serde_json::Value::Object(obj)
}

/// Every row of `sql` in `project`, paged and waited for; panics on a query error.
fn bq_query(project: &str, sql: &str) -> Vec<serde_json::Value> {
    let base = format!("https://bigquery.googleapis.com/bigquery/v2/projects/{project}");
    let (st, mut page) = call(
        reqwest::Method::POST,
        &format!("{base}/queries"),
        Some(serde_json::json!({"query": sql, "useLegacySql": false, "timeoutMs": 120_000})),
    );
    assert_eq!(st, 200, "bq query failed: {sql}\n{page}");
    let job = page["jobReference"]["jobId"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let loc = page["jobReference"]["location"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let results = |token: Option<&str>| {
        let mut url = format!("{base}/queries/{job}?location={loc}&timeoutMs=120000");
        if let Some(t) = token {
            url.push_str(&format!("&pageToken={t}"));
        }
        let (st, p) = call(reqwest::Method::GET, &url, None);
        assert_eq!(st, 200, "bq query results failed: {sql}\n{p}");
        p
    };
    while page["jobComplete"] != true {
        page = results(None);
    }
    let fields = page["schema"]["fields"].clone();
    let mut rows = Vec::new();
    loop {
        rows.extend(
            page["rows"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|r| render_row(&fields, r)),
        );
        match page["pageToken"].as_str().map(str::to_string) {
            Some(t) => page = results(Some(&t)),
            None => return rows,
        }
    }
}

/// Drop `dataset` with its contents if it exists, then create it empty in the stand's location.
pub fn recreate_dataset(project: &str, dataset: &str) {
    let base = format!("https://bigquery.googleapis.com/bigquery/v2/projects/{project}/datasets");
    let (st, body) = call(
        reqwest::Method::DELETE,
        &format!("{base}/{dataset}?deleteContents=true"),
        None,
    );
    assert!(
        st == 200 || st == 204 || st == 404,
        "delete dataset {dataset}: {st} {body}"
    );
    let (st, body) = call(
        reqwest::Method::POST,
        &base,
        Some(serde_json::json!({
            "datasetReference": {"projectId": project, "datasetId": dataset},
            "location": super::registry::stand_bq_location(),
        })),
    );
    assert!(
        st == 200 || st == 409,
        "create dataset {dataset}: {st} {body}"
    );
}

/// Delete every object under `gs://bucket/prefix/` (best effort — a cleanup path).
fn gcs_delete_prefix(bucket: &str, prefix: &str) {
    let list = format!("https://storage.googleapis.com/storage/v1/b/{bucket}/o");
    let mut page_token: Option<String> = None;
    loop {
        let mut url = format!(
            "{list}?prefix={}/&fields=items(name),nextPageToken",
            urlencode(prefix)
        );
        if let Some(t) = &page_token {
            url.push_str(&format!("&pageToken={}", urlencode(t)));
        }
        let Some((200, page)) = try_call(reqwest::Method::GET, &url, None) else {
            return;
        };
        for item in page["items"].as_array().into_iter().flatten() {
            if let Some(name) = item["name"].as_str() {
                let _ = try_call(
                    reqwest::Method::DELETE,
                    &format!("{list}/{}", urlencode(name)),
                    None,
                );
            }
        }
        match page["nextPageToken"].as_str() {
            Some(t) => page_token = Some(t.to_string()),
            None => return,
        }
    }
}

fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// The live warehouse a test loads into, with a run-unique GCS prefix.
pub struct BqLive {
    pub project: String,
    pub dataset: String,
    pub bucket: String,
    pub prefix: String,
    /// The test created `dataset`, so dropping this drops it (with the GCS prefix).
    pub owned: bool,
}

impl BqLive {
    /// `BIGQUERY_TEST_PROJECT` + `RIVET_TEST_GCS_BUCKET`, or `None` with a skip note; the dataset is this test's own disposable one unless `RIVET_TEST_BQ_DATASET` names a shared one.
    pub fn from_env(label: &str) -> Option<Self> {
        let (Ok(project), Ok(bucket)) = (
            std::env::var("BIGQUERY_TEST_PROJECT"),
            std::env::var("RIVET_TEST_GCS_BUCKET"),
        ) else {
            super::skip_live(&format!(
                "{label}: BIGQUERY_TEST_PROJECT / RIVET_TEST_GCS_BUCKET unset"
            ));
            return None;
        };
        let unique = super::unique_name(label);
        let (dataset, owned) = match std::env::var("RIVET_TEST_BQ_DATASET") {
            Ok(shared) => (shared, false),
            Err(_) => {
                let safe: String = unique
                    .chars()
                    .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
                    .collect();
                let dataset = super::registry::stand_bq_tmp(&safe);
                let (st, body) = call(
                    reqwest::Method::POST,
                    &format!(
                        "https://bigquery.googleapis.com/bigquery/v2/projects/{project}/datasets"
                    ),
                    Some(serde_json::json!({
                        "datasetReference": {"projectId": project, "datasetId": dataset},
                        "location": super::registry::stand_bq_location(),
                    })),
                );
                assert!(
                    st == 200 || st == 409,
                    "create dataset {dataset}: {st} {body}"
                );
                (dataset, true)
            }
        };
        Some(Self {
            project,
            dataset,
            bucket,
            prefix: format!("rivet-live/{unique}"),
            owned,
        })
    }

    /// The top-level `load:` line for a rig, with `extra` appended inside the mapping.
    pub fn load_line(&self, extra: &str) -> String {
        format!(
            "load: {{ target: bigquery, project: {}, dataset: {}{extra} }}",
            self.project, self.dataset
        )
    }

    /// Rows of `sql` as JSON objects, rendered the way `bq --format=json` renders them.
    pub fn read_bq_rows(&self, sql: &str) -> Vec<serde_json::Value> {
        bq_query(&self.project, sql)
    }

    /// The clustering columns of `table`, in clustering order.
    pub fn read_bq_clustering(&self, table: &str) -> Vec<String> {
        self.read_bq_rows(&format!(
            "SELECT column_name FROM `{}.{}`.INFORMATION_SCHEMA.COLUMNS \
             WHERE table_name = '{table}' AND clustering_ordinal_position IS NOT NULL \
             ORDER BY clustering_ordinal_position",
            self.project, self.dataset
        ))
        .iter()
        .map(|r| r["column_name"].as_str().expect("column_name").to_string())
        .collect()
    }

    /// The column `table` is partitioned on, if any.
    pub fn read_bq_partitioning(&self, table: &str) -> Option<String> {
        self.read_bq_rows(&format!(
            "SELECT column_name FROM `{}.{}`.INFORMATION_SCHEMA.COLUMNS \
             WHERE table_name = '{table}' AND is_partitioning_column = 'YES'",
            self.project, self.dataset
        ))
        .first()
        .map(|r| r["column_name"].as_str().expect("column_name").to_string())
    }

    /// `BASE TABLE` / `VIEW` for `table`, or `None` when nothing has that name.
    pub fn read_bq_table_type(&self, table: &str) -> Option<String> {
        self.read_bq_rows(&format!(
            "SELECT table_type FROM `{}.{}`.INFORMATION_SCHEMA.TABLES \
             WHERE table_name = '{table}'",
            self.project, self.dataset
        ))
        .first()
        .map(|r| r["table_type"].as_str().expect("table_type").to_string())
    }

    /// `COUNT(*)` of `table` as text.
    pub fn read_bq_count(&self, table: &str) -> String {
        self.read_bq_rows(&format!(
            "SELECT COUNT(*) AS n FROM `{}.{}.{table}`",
            self.project, self.dataset
        ))[0]["n"]
            .as_str()
            .expect("count")
            .to_string()
    }

    /// The `tables.get` resource of `table` (`bq show --format=json`); the table must exist.
    pub fn read_bq_meta(&self, table: &str) -> serde_json::Value {
        let (st, meta) = call(
            reqwest::Method::GET,
            &format!(
                "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets/{}/tables/{table}",
                self.project, self.dataset
            ),
            None,
        );
        assert_eq!(st, 200, "tables.get failed for {table}: {meta}");
        meta
    }

    /// `(type, field)` of `table`'s time partitioning, e.g. `("DAY", Some("ts"))`;
    /// `field` is `None` for load-time partitions, the whole thing for no time partitioning.
    pub fn read_bq_time_partitioning(&self, table: &str) -> Option<(String, Option<String>)> {
        time_partitioning(&self.read_bq_meta(table))
    }

    /// `(column, start, end, interval)` of `table`'s integer-range partitioning.
    pub fn read_bq_range_partitioning(&self, table: &str) -> Option<(String, i64, i64, i64)> {
        let meta = self.read_bq_meta(table);
        let rp = meta.get("rangePartitioning")?;
        let int = |k: &str| rp["range"][k].as_str()?.parse::<i64>().ok();
        Some((
            rp["field"].as_str()?.to_string(),
            int("start")?,
            int("end")?,
            int("interval")?,
        ))
    }

    /// `partition_expiration_days` of `table`, from its `expirationMs`.
    pub fn read_bq_partition_expiration_days(&self, table: &str) -> Option<f64> {
        let meta = self.read_bq_meta(table);
        let ms = meta["timePartitioning"]["expirationMs"]
            .as_str()?
            .parse::<f64>()
            .ok()?;
        Some(ms / 86_400_000.0)
    }

    /// Whether `table` requires a partition filter.
    pub fn read_bq_requires_partition_filter(&self, table: &str) -> bool {
        let meta = self.read_bq_meta(table);
        meta["requirePartitionFilter"].as_bool().unwrap_or(false)
            || meta["timePartitioning"]["requirePartitionFilter"]
                .as_bool()
                .unwrap_or(false)
    }

    /// `COUNT(*)` of `table` under `where_sql` — for a table that requires a partition filter.
    pub fn read_bq_count_where(&self, table: &str, where_sql: &str) -> String {
        self.read_bq_rows(&format!(
            "SELECT COUNT(*) AS n FROM `{}.{}.{table}` WHERE {where_sql}",
            self.project, self.dataset
        ))[0]["n"]
            .as_str()
            .expect("count")
            .to_string()
    }

    /// One table option of `table` as BigQuery renders it, or `None` when unset.
    pub fn read_bq_option(&self, table: &str, option: &str) -> Option<String> {
        self.read_bq_rows(&format!(
            "SELECT option_value FROM `{}.{}`.INFORMATION_SCHEMA.TABLE_OPTIONS \
             WHERE table_name = '{table}' AND option_name = '{option}'",
            self.project, self.dataset
        ))
        .first()
        .map(|r| {
            r["option_value"]
                .as_str()
                .expect("option_value")
                .to_string()
        })
    }

    /// Run one DDL statement, panicking on failure.
    pub fn exec(&self, sql: &str) {
        bq_query(&self.project, sql);
    }

    /// Drops `tables` and the staged GCS prefix when the returned guard goes out of scope.
    pub fn cleanup(&self, tables: &[&str]) -> BqCleanup {
        BqCleanup {
            project: self.project.clone(),
            dataset: self.dataset.clone(),
            tables: tables.iter().map(|t| t.to_string()).collect(),
            bucket: self.bucket.clone(),
            prefix: self.prefix.clone(),
        }
    }
}

impl Drop for BqLive {
    fn drop(&mut self) {
        if !self.owned {
            return;
        }
        let _ = try_call(
            reqwest::Method::DELETE,
            &format!(
                "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets/{}?deleteContents=true",
                self.project, self.dataset
            ),
            None,
        );
        gcs_delete_prefix(&self.bucket, &self.prefix);
    }
}

/// `(type, field)` of a `tables.get` resource's time partitioning.
pub fn time_partitioning(meta: &serde_json::Value) -> Option<(String, Option<String>)> {
    let tp = meta.get("timePartitioning")?;
    Some((
        tp["type"].as_str()?.to_string(),
        tp["field"].as_str().map(String::from),
    ))
}

/// See [`BqLive::cleanup`].
pub struct BqCleanup {
    project: String,
    dataset: String,
    tables: Vec<String>,
    bucket: String,
    prefix: String,
}

impl Drop for BqCleanup {
    fn drop(&mut self) {
        for t in &self.tables {
            let url = format!(
                "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets/{}/tables/{t}",
                self.project, self.dataset
            );
            let _ = try_call(reqwest::Method::DELETE, &url, None);
        }
        gcs_delete_prefix(&self.bucket, &self.prefix);
    }
}
