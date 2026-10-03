//! ClickHouse loader (ADR-0035): rivet reads each staged Parquet part and sends it
//! as `INSERT … FORMAT Parquet` over the HTTP interface; a CDC change log is a
//! `ReplacingMergeTree(__ver)` the view reads with `FINAL`.
//!
//! Transport and DDL started from @ssyusyukalov's loader in #145.

use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{Context, Result, bail};

use super::cdc::{self, Warehouse};
use super::{GcsStore, ObjectKind, TargetLoader};
use crate::types::target::{ChType, TargetColumnSpec, TargetType};

/// HTTP timeout for one ClickHouse call (20 min); one part's INSERT is seconds on a LAN.
const HTTP_TIMEOUT: Duration = Duration::from_secs(1200);

/// Attempts per statement, the first included; retries back off 250 ms, 500 ms, 1 s, 2 s.
const MAX_ATTEMPTS: u32 = 5;
const RETRY_BASE_MS: u64 = 250;

/// Whether a statement may be sent again after a failure that leaves its outcome unknown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Repeat {
    /// A second run changes nothing: a read, `IF NOT EXISTS`/`OR REPLACE` DDL, an insert into a log that collapses copies.
    Safe,
    /// A second run changes the result (EXCHANGE, RENAME, a plain `MergeTree` insert): resend only what never connected.
    Undelivered,
}

/// How one HTTP attempt failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Failure {
    /// No connection was made, so the server never saw the statement.
    Connect,
    /// The connection broke or timed out after the statement was sent.
    Transport,
    /// The server answered with this status and `X-ClickHouse-Exception-Code`.
    Status(u16, Option<u32>),
}

/// Loads staged Parquet into ClickHouse over its HTTP interface.
pub struct ClickhouseLoader {
    url: String,
    database: String,
    user: String,
    password_env: String,
    destination: crate::config::DestinationConfig,
    store: OnceLock<GcsStore>,
    /// `ORDER BY` of a full-load table; empty = `tuple()`.
    cluster_by: Vec<String>,
    /// A CDC load: the change log is a `ReplacingMergeTree(__ver)` keyed on the PK.
    cdc: bool,
    /// Read parts server-side through this named collection instead of sending them.
    named_collection: Option<String>,
    /// `PARTITION BY` of every table the load creates (ADR-0035 CH8).
    partition_by: Option<String>,
    /// A PEM file of extra root certificates for an `https://` URL.
    ca_file: Option<String>,
    client: OnceLock<reqwest::blocking::Client>,
}

impl ClickhouseLoader {
    /// A loader for `database` at `url`; nothing is contacted until a method runs.
    pub(crate) fn new(
        url: &str,
        database: &str,
        user: &str,
        password_env: &str,
        destination: crate::config::DestinationConfig,
    ) -> Self {
        Self {
            url: url.trim_end_matches('/').to_string(),
            database: database.to_string(),
            user: user.to_string(),
            password_env: password_env.to_string(),
            destination,
            store: OnceLock::new(),
            cluster_by: Vec::new(),
            cdc: false,
            named_collection: None,
            partition_by: None,
            ca_file: None,
            client: OnceLock::new(),
        }
    }

    /// Trust the root certificates in `ca_file` beside the built-in ones.
    pub(crate) fn ca_file(mut self, ca_file: Option<String>) -> Self {
        self.ca_file = ca_file;
        self
    }

    /// Have ClickHouse read each part itself through `collection` (ADR-0035 CH6).
    pub(crate) fn named_collection(mut self, collection: Option<String>) -> Self {
        self.named_collection = collection;
        self
    }

    /// Set the full-load table's `ORDER BY` columns.
    pub(crate) fn cluster_by(mut self, cols: Vec<String>) -> Self {
        self.cluster_by = cols;
        self
    }

    /// Partition every table the load creates by `expr`.
    pub(crate) fn partition_by(mut self, expr: Option<String>) -> Self {
        self.partition_by = expr;
        self
    }

    /// Mark this loader as writing a CDC change log.
    pub(crate) fn cdc(mut self, cdc: bool) -> Self {
        self.cdc = cdc;
        self
    }

    fn quoted(&self, table: &str) -> String {
        Warehouse::ClickHouse.quote_fqtn(&TargetLoader::fqtn(self, table))
    }

    fn store(&self) -> Result<&GcsStore> {
        if let Some(s) = self.store.get() {
            return Ok(s);
        }
        let s = super::open_store(&self.destination)?;
        Ok(self.store.get_or_init(|| s))
    }

    fn client(&self) -> Result<&reqwest::blocking::Client> {
        if let Some(c) = self.client.get() {
            return Ok(c);
        }
        let c = http_client(self.ca_file.as_deref())?;
        Ok(self.client.get_or_init(|| c))
    }

    /// POST `body` with `params`, waiting for the statement to finish and retrying what
    /// `repeat` allows; returns the response text.
    fn post(&self, params: &[(&str, &str)], body: bytes::Bytes, repeat: Repeat) -> Result<String> {
        let pass = std::env::var(&self.password_env).with_context(|| {
            format!(
                "ClickHouse load: `password_env` names `{}`, which is not set",
                self.password_env
            )
        })?;
        let client = self.client()?;
        for attempt in 1..=MAX_ATTEMPTS {
            let (failure, err) = match self.send_once(client, &pass, params, body.clone()) {
                Ok(text) => return Ok(text),
                Err(e) => e,
            };
            if retry_after(failure, repeat, attempt) {
                log::warn!("ClickHouse attempt {attempt} failed, retrying: {err:#}");
                std::thread::sleep(Duration::from_millis(
                    crate::pipeline::retry::retry_backoff_ms(RETRY_BASE_MS, attempt, 0),
                ));
                continue;
            }
            return Err(err.context(format!(
                "ClickHouse statement failed on attempt {attempt} of {MAX_ATTEMPTS}"
            )));
        }
        unreachable!("retry_after refuses attempt {MAX_ATTEMPTS}, so the last attempt returns")
    }

    /// One HTTP attempt: the response text, or how it failed and why.
    fn send_once(
        &self,
        client: &reqwest::blocking::Client,
        pass: &str,
        params: &[(&str, &str)],
        body: bytes::Bytes,
    ) -> std::result::Result<String, (Failure, anyhow::Error)> {
        let resp = client
            .post(&self.url)
            .basic_auth(&self.user, Some(pass))
            .query(&[("wait_end_of_query", "1")])
            .query(params)
            .body(body)
            .send()
            .map_err(|e| {
                let failure = if e.is_connect() {
                    Failure::Connect
                } else {
                    Failure::Transport
                };
                let url = crate::redact::redact_secrets(&self.url);
                let err = anyhow::Error::new(e);
                (
                    failure,
                    err.context(format!("ClickHouse HTTP request to {url} failed")),
                )
            })?;
        let status = resp.status();
        let code = resp
            .headers()
            .get("X-ClickHouse-Exception-Code")
            .and_then(|v| v.to_str().ok()?.parse().ok());
        let text = resp.text().map_err(|e| {
            (
                Failure::Transport,
                anyhow::Error::new(e).context("reading the ClickHouse HTTP response"),
            )
        })?;
        match status.is_success() {
            true => Ok(text.trim().to_string()),
            false => Err((
                Failure::Status(status.as_u16(), code),
                anyhow::anyhow!("ClickHouse (HTTP {status}): {}", trim_ch_error(&text)),
            )),
        }
    }

    /// Run one idempotent SQL statement and return its output.
    fn query(&self, sql: &str) -> Result<String> {
        self.post(
            &[],
            bytes::Bytes::copy_from_slice(sql.as_bytes()),
            Repeat::Safe,
        )
    }

    /// Run one SQL statement that must not run twice unless it never reached the server.
    fn query_once(&self, sql: &str) -> Result<String> {
        self.post(
            &[],
            bytes::Bytes::copy_from_slice(sql.as_bytes()),
            Repeat::Undelivered,
        )
    }

    /// A single `u64` from a `SELECT` returning one number.
    fn number(&self, sql: &str) -> Result<u64> {
        let out = self.query(sql)?;
        out.parse()
            .with_context(|| format!("ClickHouse returned `{out}` for `{sql}`"))
    }

    /// Insert every part in `uris` into `target`; the rows those parts hold.
    ///
    /// The count comes from each part, not from `X-ClickHouse-Summary`: that counts rows
    /// materialized views write too and reads 0 under `async_insert`, while a statement that
    /// returns 200 under `wait_end_of_query` inserted all of its rows (ADR-0035 CH6).
    fn insert_uris(&self, target: &str, uris: &[String], repeat: Repeat) -> Result<u64> {
        uris.iter()
            .enumerate()
            .map(|(i, uri)| {
                let rows = self.insert_one(target, uri, repeat)?;
                crate::test_hook::maybe_panic_at_chunk("clickhouse_after_part", i as i64);
                Ok(rows)
            })
            .sum()
    }

    /// Insert one part into `target`; the rows it holds. Pushed or pulled, the part's footer
    /// is read first, so a timestamp ClickHouse would clamp refuses before any row lands.
    fn insert_one(&self, target: &str, uri: &str, repeat: Repeat) -> Result<u64> {
        let (bucket, key) = super::split_object_uri(uri)?;
        let pull = self
            .named_collection
            .as_ref()
            .filter(|_| pullable(bucket, key));
        let (query, body, meta) = match pull {
            Some(nc) => (
                format!(
                    "INSERT INTO {target} SELECT * FROM {}",
                    pull_source(nc, super::scheme_of(uri), bucket, key)
                ),
                bytes::Bytes::new(),
                super::partition_budget::read_footer(self.store()?, key)
                    .with_context(|| format!("reading the footer of {uri}"))?,
            ),
            None => {
                let bytes = self
                    .store()?
                    .read(key)
                    .with_context(|| format!("reading {uri} for the ClickHouse load"))?;
                let meta = parquet_footer(&bytes).with_context(|| format!("reading {uri}"))?;
                let insert = format!("INSERT INTO {target} FORMAT Parquet");
                (insert, bytes::Bytes::from(bytes), meta)
            }
        };
        refuse_unholdable_timestamps(&meta, uri)?;
        let rows = footer_rows(&meta)?;
        let params = [
            ("query", query.as_str()),
            ("input_format_null_as_default", "0"),
            ("async_insert", "0"),
            ("use_structure_from_insertion_table_in_table_functions", "1"),
        ];
        self.post(&params, body, repeat)
            .with_context(|| format!("inserting {uri} into {target}"))?;
        Ok(rows)
    }

    /// Why an existing `<table>__changes` cannot take this load, read from the catalog; `None` when absent or matching.
    fn existing_changelog_conflict(
        &self,
        table: &str,
        shape: &ChangelogShape<'_>,
        pk: &[String],
        full: &[TargetColumnSpec],
    ) -> Result<Option<String>> {
        let name = format!("{table}__changes");
        let found = self.query(&format!(
            "SELECT engine, sorting_key, partition_key FROM system.tables WHERE {} FORMAT TSVRaw",
            self.system_filter(&name, "name")
        ))?;
        let mut found = found.trim_end_matches('\n').splitn(3, '\t');
        let (Some(engine), Some(sorting_key), partition_key) =
            (found.next(), found.next(), found.next().unwrap_or(""))
        else {
            return Ok(None);
        };
        let cols = self.query(&format!(
            "SELECT name, type FROM system.columns WHERE {} FORMAT TSVRaw",
            self.system_filter(&name, "table")
        ))?;
        let existing: Vec<(&str, &str)> = cols.lines().filter_map(|l| l.split_once('\t')).collect();
        let declared: Vec<String> = full
            .iter()
            .map(|s| column_type(s, shape.not_null.contains(&s.column_name)))
            .collect();
        let canonical = self.query(&canonical_types_sql(&declared))?;
        let wanted: Vec<(&str, &str)> = full
            .iter()
            .map(|s| s.column_name.as_str())
            .zip(canonical.split('\t'))
            .collect();
        Ok(changelog_conflict(
            &TargetLoader::fqtn(self, &name),
            &TargetLoader::fqtn(self, table),
            shape,
            pk,
            (engine, sorting_key, partition_key),
            &existing,
            &wanted,
        ))
    }

    /// `name = 'x' AND database = 'y'` for a `system.*` lookup of `table`.
    fn system_filter(&self, table: &str, name_col: &str) -> String {
        format!(
            "database = {} AND {name_col} = {}",
            literal(&self.database),
            literal(table)
        )
    }
}

impl TargetLoader for ClickhouseLoader {
    fn fqtn(&self, table: &str) -> String {
        format!("{}.{}", self.database, table)
    }

    fn materialize(&self, table: &str, specs: &[TargetColumnSpec], uris: &[String]) -> Result<u64> {
        if let Some(c) = unsafe_column(&self.cluster_by) {
            bail!(
                "ClickHouse load: `cluster_by` column `{}` is not a plain SQL identifier",
                c.escape_default()
            );
        }
        let target = self.quoted(table);
        let swap = self.quoted(&format!("{table}__rivet_swap"));
        self.query(&create_table_sql(
            "CREATE OR REPLACE TABLE",
            &swap,
            &columns_ddl(specs, &[]),
            "MergeTree",
            self.partition_by.as_deref(),
            &order_by(&self.cluster_by),
        ))?;
        crate::test_hook::maybe_panic_at("clickhouse_full_after_swap_created");
        let rows = self.insert_uris(&swap, uris, Repeat::Undelivered)?;
        crate::test_hook::maybe_panic_at("clickhouse_full_before_swap_in");
        for (sql, repeat, hook) in swap_in(self.object_kind(table)?, &swap, &target) {
            self.post(&[], bytes::Bytes::from(sql), repeat)?;
            if let Some(point) = hook {
                crate::test_hook::maybe_panic_at(point);
            }
        }
        Ok(rows)
    }

    fn append_changelog(
        &self,
        table: &str,
        specs: &[TargetColumnSpec],
        uris: &[String],
        pk: &[String],
    ) -> Result<u64> {
        let full = changelog_specs(specs);
        let changes = self.quoted(&format!("{table}__changes"));
        let shape = changelog_shape(self.cdc, table, pk, self.partition_by.as_deref())?;
        if let Some(why) = self.existing_changelog_conflict(table, &shape, pk, &full)? {
            return Err(super::refused(why));
        }
        let ddl = columns_ddl(&full, shape.not_null) + &shape.version_column;
        self.query(&create_table_sql(
            "CREATE TABLE IF NOT EXISTS",
            &changes,
            &ddl,
            shape.engine,
            shape.partition,
            &shape.order_by,
        ))?;
        if let Some(alter) = alter_add_columns_sql(&changes, &full, shape.not_null) {
            self.query(&alter)
                .with_context(|| format!("adding new columns to `{table}__changes`"))?;
        }
        self.insert_uris(&changes, uris, changelog_repeat(self.cdc))
    }

    fn warehouse(&self) -> Warehouse {
        Warehouse::ClickHouse
    }

    fn create_view(&self, _table: &str, view_sql: &str) -> Result<()> {
        self.query(view_sql).map(|_| ())
    }

    fn create_current_view(
        &self,
        table: &str,
        pk: &[&str],
        order: &cdc::CompactOrder,
    ) -> Result<()> {
        let sql = match order {
            cdc::CompactOrder::Cdc(_) => cdc::clickhouse_final_view(
                &TargetLoader::fqtn(self, table),
                &TargetLoader::fqtn(self, &format!("{table}__changes")),
            ),
            cdc::CompactOrder::Cursor(column) => cdc::inc_dedup_view_sql(
                Warehouse::ClickHouse,
                &TargetLoader::fqtn(self, table),
                &TargetLoader::fqtn(self, &format!("{table}__changes")),
                pk,
                column,
            ),
        };
        self.create_view(table, &sql)
    }

    fn changes_has_prior_changes(&self, table: &str) -> Result<bool> {
        let changes = format!("{table}__changes");
        if let ObjectKind::Absent = self.object_kind(&changes)? {
            return Ok(false);
        }
        let out = self.query(&format!(
            "SELECT if(count() > 0, 'true', 'false') FROM {} WHERE __op IS NOT NULL",
            self.quoted(&changes)
        ))?;
        out.parse()
            .with_context(|| format!("ClickHouse returned `{out}` for a prior-changes probe"))
    }

    fn object_kind(&self, table: &str) -> Result<ObjectKind> {
        let engine = self.query(&format!(
            "SELECT engine FROM system.tables WHERE {} FORMAT TSV",
            self.system_filter(table, "name")
        ))?;
        Ok(object_kind_of(&engine))
    }

    fn column_overlap(&self, table: &str, names: &[&str]) -> Result<(u64, u64)> {
        let wanted = names
            .iter()
            .map(|n| literal(&n.to_lowercase()))
            .collect::<Vec<_>>()
            .join(", ");
        let out = self.query(&format!(
            "SELECT count(), countIf(lower(name) IN ({})) FROM system.columns WHERE {} FORMAT TSV",
            if wanted.is_empty() { "''" } else { &wanted },
            self.system_filter(table, "table")
        ))?;
        parse_pair(&out).with_context(|| {
            format!("ClickHouse returned `{out}` for a column-overlap probe of `{table}`")
        })
    }

    fn row_count(&self, table: &str) -> Result<u64> {
        self.number(&format!("SELECT count() FROM {}", self.quoted(table)))
    }

    fn adopt_as_changelog(&self, table: &str) -> Result<()> {
        let from = self.quoted(table);
        if self.cdc {
            return Err(super::refused(format!(
                "`{}` is a table from an earlier whole-table load; a ClickHouse CDC change log is \
                 a ReplacingMergeTree, which a table cannot become in place (ADR-0035 CH11). \
                 Drop or rename `{}` and re-run — nothing was changed. The change log holds only \
                 changes from the stream's anchor on: to keep serving the table's existing rows, \
                 also set `cdc.initial: snapshot` (or a `cdc.backfill:`) and `rivet run` again \
                 before the load — the stream is already anchored, so the snapshot overlaps it \
                 and no change falls between them",
                TargetLoader::fqtn(self, table),
                TargetLoader::fqtn(self, table)
            )));
        }
        let to = self.quoted(&format!("{table}__changes"));
        self.query_once(&format!("RENAME TABLE {from} TO {to}"))?;
        self.query(&format!(
            "ALTER TABLE {to} ADD COLUMN IF NOT EXISTS `__op` Nullable(String) FIRST, \
             ADD COLUMN IF NOT EXISTS `__pos` Nullable(String) AFTER `__op`, \
             ADD COLUMN IF NOT EXISTS `__seq` Nullable(Int64) AFTER `__pos`"
        ))
        .map(|_| ())
    }
}

/// Whether ClickHouse can read `bucket/key` through a table function as written: its
/// functions decode `%` and expand `?`, `*` and `{…}` as globs, and reject spaces and
/// non-ASCII, so any other key goes through rivet instead (measured: `a?b` read 5 objects).
fn pullable(bucket: &str, key: &str) -> bool {
    format!("{bucket}/{key}")
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '/' | '.' | '_' | '-' | '='))
}

/// The table function reading one part through `collection`: `gcs`/`s3` take the path under
/// the collection's URL, `azureBlobStorage` the container and the blob.
fn pull_source(collection: &str, scheme: &str, bucket: &str, key: &str) -> String {
    match scheme {
        "az" => format!(
            "azureBlobStorage({collection}, container = {}, blob_path = {}, format = 'Parquet')",
            literal(bucket),
            literal(key)
        ),
        s => format!(
            "{}({collection}, filename = {}, format = 'Parquet')",
            if s == "s3" { "s3" } else { "gcs" },
            literal(&format!("{bucket}/{key}"))
        ),
    }
}

/// The statements that put a filled `swap` table in `target`'s place, each with whether it
/// may be resent and the fault point after it: a rename when there is nothing there yet,
/// else an exchange (resent, it would swap the old table back) and a drop of what was there.
fn swap_in(
    existing: ObjectKind,
    swap: &str,
    target: &str,
) -> Vec<(String, Repeat, Option<&'static str>)> {
    match existing {
        ObjectKind::Absent => vec![(
            format!("RENAME TABLE {swap} TO {target}"),
            Repeat::Undelivered,
            None,
        )],
        _ => vec![
            (
                format!("EXCHANGE TABLES {swap} AND {target}"),
                Repeat::Undelivered,
                Some("clickhouse_full_after_exchange"),
            ),
            (format!("DROP TABLE IF EXISTS {swap}"), Repeat::Safe, None),
        ],
    }
}

/// A CDC log collapses a resent part's copies by key and version; a plain `MergeTree`
/// incremental log would keep both, so its inserts are resent only when undelivered.
fn changelog_repeat(cdc: bool) -> Repeat {
    if cdc {
        Repeat::Safe
    } else {
        Repeat::Undelivered
    }
}

/// Whether attempt `attempt` failing with `failure` earns another for a `repeat` statement:
/// a connection never made is always resent; a lost answer or an overloaded server only
/// when a second run is harmless; any other answer is the server's decision.
fn retry_after(failure: Failure, repeat: Repeat, attempt: u32) -> bool {
    let transient = match failure {
        Failure::Connect => true,
        Failure::Transport => repeat == Repeat::Safe,
        Failure::Status(status, code) => {
            repeat == Repeat::Safe
                && (matches!(status, 429 | 502 | 503 | 504) || matches!(code, Some(202 | 252)))
        }
    };
    transient && attempt < MAX_ATTEMPTS
}

/// The HTTP client, trusting `ca_file`'s certificates beside the built-in roots; no pooled
/// connection is reused, so a server-closed keep-alive cannot fail a statement.
fn http_client(ca_file: Option<&str>) -> Result<reqwest::blocking::Client> {
    let mut builder = reqwest::blocking::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .pool_max_idle_per_host(0);
    if let Some(path) = ca_file {
        let pem = std::fs::read(path)
            .with_context(|| format!("ClickHouse load: cannot read `load.ca_file` {path}"))?;
        let certs = reqwest::Certificate::from_pem_bundle(&pem)
            .with_context(|| format!("ClickHouse load: `load.ca_file` {path} is not PEM"))?;
        if certs.is_empty() {
            bail!("ClickHouse load: `load.ca_file` {path} holds no PEM certificate");
        }
        for cert in certs {
            builder = builder.add_root_certificate(cert);
        }
    }
    builder
        .build()
        .context("building the ClickHouse HTTP client")
}

/// Two tab-separated counts, as a `SELECT a, b … FORMAT TSV` returns them.
fn parse_pair(tsv: &str) -> Option<(u64, u64)> {
    let (a, b) = tsv.trim().split_once('\t')?;
    Some((a.parse().ok()?, b.parse().ok()?))
}

/// The first of `cols` that is not a plain SQL identifier.
fn unsafe_column(cols: &[String]) -> Option<&String> {
    cols.iter().find(|c| !super::is_safe_load_ident(c))
}

/// A change log's columns: rivet's meta columns, then the data columns.
fn changelog_specs(specs: &[TargetColumnSpec]) -> Vec<TargetColumnSpec> {
    let mut full = cdc::meta_column_specs(Warehouse::ClickHouse);
    full.extend(
        specs
            .iter()
            .filter(|s| !cdc::is_meta_column(&s.column_name))
            .cloned(),
    );
    full
}

/// The engine, key and version column of a change log (ADR-0035 CH2, CH10).
struct ChangelogShape<'a> {
    engine: &'static str,
    partition: Option<&'a str>,
    order_by: String,
    not_null: &'a [String],
    version_column: String,
}

/// A CDC log collapses versions by the PK; an incremental log is a plain `MergeTree`.
fn changelog_shape<'a>(
    cdc: bool,
    table: &str,
    pk: &'a [String],
    partition: Option<&'a str>,
) -> Result<ChangelogShape<'a>> {
    if !cdc {
        return Ok(ChangelogShape {
            engine: "MergeTree",
            partition,
            order_by: order_by(&[]),
            not_null: &[],
            version_column: String::new(),
        });
    }
    if pk.is_empty() {
        bail!(
            "ClickHouse CDC load of `{table}` needs a primary key: the change log collapses \
             versions by key (ADR-0035 CH2) — set `load.pk`"
        );
    }
    Ok(ChangelogShape {
        engine: "ReplacingMergeTree(__ver)",
        partition,
        order_by: order_by(pk),
        not_null: pk,
        version_column: format!(
            ",\n  `__ver` UInt256 MATERIALIZED {}",
            cdc::CLICKHOUSE_VERSION_EXPR
        ),
    })
}

/// Why an existing change log (`engine`, `sorting_key`, `partition_key`, column types) cannot
/// take a load shaped `shape` over `wanted`, or `None`: another engine (the export changed
/// mode), another key (a changed `load.pk`), another declared partition or another column type.
fn changelog_conflict(
    changes: &str,
    view: &str,
    shape: &ChangelogShape<'_>,
    pk: &[String],
    (engine, sorting_key, partition_key): (&str, &str, &str),
    existing: &[(&str, &str)],
    wanted: &[(&str, &str)],
) -> Option<String> {
    let want_engine = shape.engine.split('(').next().unwrap_or(shape.engine);
    let engine = ["Replicated", "Shared"]
        .iter()
        .find_map(|p| engine.strip_prefix(p))
        .unwrap_or(engine);
    let restart = format!(
        "Nothing was written. Drop it and `{view}`, then re-snapshot the export (a CDC stream) \
         or `rivet state reset` it (an incremental one) so the next load starts the log over"
    );
    if engine != want_engine {
        return Some(format!(
            "`{changes}` is a {engine}, but this load writes a {want_engine} change log \
             (the export's mode changed; ADR-0035). {restart}"
        ));
    }
    let key: Vec<String> = sorting_key
        .split(',')
        .map(|c| c.trim().trim_matches('`').to_string())
        .filter(|c| !c.is_empty())
        .collect();
    if want_engine == "ReplacingMergeTree" && key != pk {
        return Some(format!(
            "`{changes}` collapses versions by ({}), but `load.pk` is now ({}): rows the old key \
             already merged cannot be told apart again. {restart}",
            key.join(", "),
            pk.join(", ")
        ));
    }
    let bare = |e: &str| e.replace(['`', ' '], "");
    if let Some(declared) = shape.partition
        && bare(declared) != bare(partition_key)
    {
        let existing = if partition_key.is_empty() {
            "nothing"
        } else {
            partition_key
        };
        return Some(format!(
            "`{changes}` is partitioned by {existing}, but the load declares {declared}; \
             ClickHouse cannot re-partition a table in place. Set `partition:` back, or: {restart}"
        ));
    }
    wanted.iter().find_map(|&(name, want)| {
        let (_, have) = existing.iter().find(|(n, _)| *n == name)?;
        (*have != want).then(|| {
            format!(
                "column `{name}` of `{changes}` is {have}, but the export now resolves it to {want}; \
                 inserting would convert every value silently. Widen it with `ALTER TABLE \
                 {changes} MODIFY COLUMN `{name}` {want}` (not possible for a key column). \
                 Otherwise: {restart}"
            )
        })
    })
}

/// One row, tab-separated: how ClickHouse itself spells each of `types`, so a declared
/// `Decimal64(6)` compares equal to the catalog's `Decimal(18, 6)`.
fn canonical_types_sql(types: &[String]) -> String {
    let cols = types
        .iter()
        .map(|t| format!("toTypeName(defaultValueOfTypeName({}))", literal(t)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT {cols} FORMAT TSVRaw")
}

/// The `PARTITION BY` expression for a `partition:` column at a granularity (ADR-0035 CH8).
pub(crate) fn partition_expr(
    export: &str,
    spec: &crate::load::plan::PartitionSpec,
    column_type: &dyn Fn(&str) -> Result<TargetType>,
) -> Result<(crate::load::plan::PartitionKey, String)> {
    use crate::load::plan::{Granularity, PartitionForm, PartitionKey};
    if spec.expiration_days.is_some() || spec.require_filter {
        crate::rivet_bail!(
            crate::error::codes::CONFIG_LOAD_PARTITION_UNSUPPORTED,
            "export `{export}`: a ClickHouse load sets no partition expiry or partition filter — \
             drop `expiration_days` / `require_filter` from `partition` (a TTL is the table \
             owner's decision)"
        );
    }
    let (column, granularity) = match &spec.form {
        PartitionForm::Column {
            column,
            granularity,
        } => (column, *granularity),
        PartitionForm::Range { .. } => crate::rivet_bail!(
            crate::error::codes::CONFIG_LOAD_PARTITION_UNSUPPORTED,
            "export `{export}`: `range` is BigQuery's integer-range partitioning; a ClickHouse \
             load partitions by a date or time `column` + `granularity`"
        ),
        PartitionForm::Ingestion(_) => crate::rivet_bail!(
            crate::error::codes::CONFIG_LOAD_PARTITION_UNSUPPORTED,
            "export `{export}`: ClickHouse has no load-time partitions — partition by \
             `column: _rivet_exported_at` (the export stamp) instead"
        ),
    };
    let ty = column_type(column)?;
    let t = crate::load::plan::base_type(&ty);
    let TargetType::ClickHouse(ch @ (ChType::Date32 | ChType::DateTime64(..))) = &ty else {
        crate::rivet_bail!(
            crate::error::codes::CONFIG_LOAD_PARTITION_UNSUPPORTED,
            "export `{export}`: cannot partition on `{column}` ({t}); a ClickHouse load partitions \
             a Date32 or DateTime64 column by time"
        );
    };
    let c = Warehouse::ClickHouse.quote_ident(column);
    let expr = match granularity {
        Granularity::Hour if !matches!(ch, ChType::DateTime64(..)) => crate::rivet_bail!(
            crate::error::codes::CONFIG_LOAD_PARTITION_UNSUPPORTED,
            "export `{export}`: `{column}` is a {t}, which has no hours — partition it by day, \
             month or year"
        ),
        Granularity::Hour => format!("intDiv(toYYYYMMDDhhmmss({c}), 10000)"),
        Granularity::Day => format!("toYYYYMMDD({c})"),
        Granularity::Month => format!("toYYYYMM({c})"),
        Granularity::Year => format!("toYear({c})"),
    };
    Ok((
        PartitionKey::Time {
            column: Some(column.clone()),
            granularity,
        },
        expr,
    ))
}

/// `CREATE … <fqtn> (<ddl>) ENGINE = <engine> [PARTITION BY …] ORDER BY <key>`, allowing a Nullable key.
fn create_table_sql(
    verb: &str,
    fqtn: &str,
    ddl: &str,
    engine: &str,
    partition: Option<&str>,
    key: &str,
) -> String {
    let partition = partition.map_or(String::new(), |p| format!(" PARTITION BY {p}"));
    format!(
        "{verb} {fqtn} (\n{ddl}\n) ENGINE = {engine}{partition} ORDER BY {key} SETTINGS \
         allow_nullable_key = 1"
    )
}

/// One `` `name` Type `` line per spec; `not_null` columns are declared without `Nullable`.
fn columns_ddl(specs: &[TargetColumnSpec], not_null: &[String]) -> String {
    specs
        .iter()
        .map(|s| {
            format!(
                "  `{}` {}",
                s.column_name,
                column_type(s, not_null.contains(&s.column_name))
            )
        })
        .collect::<Vec<_>>()
        .join(",\n")
}

/// The native type a spec lands as: `Nullable(T)` unless it is a key column or a container.
/// JSON and UUID are declared as what their Parquet converts into (`String`, the 16 raw
/// bytes); a `JSON` or `UUID` column refuses the insert (measured).
fn column_type(spec: &TargetColumnSpec, not_null: bool) -> String {
    let t = match &spec.target_type {
        TargetType::ClickHouse(t) => TargetType::ClickHouse(landed_type(t)),
        other => other.clone(),
    };
    if not_null || matches!(t, TargetType::ClickHouse(ChType::Array(_))) {
        t.to_string()
    } else {
        format!("Nullable({t})")
    }
}

/// A resolved type with JSON and UUID, also as Array elements, replaced by what they land as.
fn landed_type(t: &ChType) -> ChType {
    match t {
        ChType::Json => ChType::String,
        ChType::Uuid => ChType::FixedString16,
        ChType::Array(inner) => ChType::Array(Box::new(landed_type(inner))),
        other => other.clone(),
    }
}

/// `ALTER TABLE … ADD COLUMN IF NOT EXISTS …` for every spec, or `None` when there are none.
fn alter_add_columns_sql(
    fqtn: &str,
    specs: &[TargetColumnSpec],
    not_null: &[String],
) -> Option<String> {
    if specs.is_empty() {
        return None;
    }
    let adds = specs
        .iter()
        .map(|s| {
            format!(
                "ADD COLUMN IF NOT EXISTS `{}` {}",
                s.column_name,
                column_type(s, not_null.contains(&s.column_name))
            )
        })
        .collect::<Vec<_>>()
        .join(",\n  ");
    Some(format!("ALTER TABLE {fqtn}\n  {adds}"))
}

/// `(`a`, `b`)`, or `tuple()` for no columns.
fn order_by(cols: &[String]) -> String {
    if cols.is_empty() {
        return "tuple()".to_string();
    }
    let quoted: Vec<String> = cols
        .iter()
        .map(|c| Warehouse::ClickHouse.quote_ident(c))
        .collect();
    format!("({})", quoted.join(", "))
}

/// A ClickHouse string literal.
fn literal(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
}

/// The kind of object a `system.tables.engine` value names; empty = absent.
fn object_kind_of(engine: &str) -> ObjectKind {
    match engine.trim() {
        "" => ObjectKind::Absent,
        "View" => ObjectKind::View,
        e if e.ends_with("MergeTree") => ObjectKind::Table,
        _ => ObjectKind::Other,
    }
}

/// ClickHouse `DateTime64`'s range in Unix seconds: 1900-01-01 00:00:00 to 2299-12-31 23:59:59.
const DATETIME64_MIN_SECS: i64 = -2_208_988_800;
const DATETIME64_MAX_SECS: i64 = 10_413_791_999;

/// Refuse a part holding a timestamp ClickHouse would silently clamp (measured on 24.8:
/// 9999-12-31 reads back as 2299-12-31 23:00, and `date_time_overflow_behavior` does not
/// reach the Parquet reader).
fn refuse_unholdable_timestamps(
    meta: &parquet::file::metadata::ParquetMetaData,
    uri: &str,
) -> Result<()> {
    let Some((column, value)) =
        super::partition_budget::timestamp_outside(meta, DATETIME64_MIN_SECS, DATETIME64_MAX_SECS)
    else {
        return Ok(());
    };
    match value {
        None => crate::rivet_bail!(
            crate::error::codes::LOAD_VALUE_OUT_OF_TARGET_RANGE,
            "{uri}: column `{column}` has no min/max statistics in the Parquet footer, so rivet \
             cannot check it against ClickHouse DateTime64's range (1900-01-01 to 2299-12-31), \
             which ClickHouse enforces by storing the nearest end silently; nothing was inserted. \
             Re-export the part with statistics enabled (rivet's writer keeps them), or declare \
             the column as String in the export's `columns:`."
        ),
        Some(value) => crate::rivet_bail!(
            crate::error::codes::LOAD_VALUE_OUT_OF_TARGET_RANGE,
            "{uri}: column `{column}` holds {value}, outside ClickHouse DateTime64's range \
             (1900-01-01 to 2299-12-31). ClickHouse would store the nearest end instead, \
             silently; nothing was inserted. Declare the column as String in the export's \
             `columns:` to keep the value, or correct it at the source."
        ),
    }
}

/// A Parquet file's footer metadata.
fn parquet_footer(file: &[u8]) -> Result<parquet::file::metadata::ParquetMetaData> {
    use parquet::file::metadata::{FooterTail, ParquetMetaDataReader};
    const TAIL: usize = 8;
    let tail: [u8; TAIL] = file
        .get(file.len().saturating_sub(TAIL)..)
        .and_then(|t| t.try_into().ok())
        .context("too short to be a Parquet file")?;
    let len = FooterTail::try_new(&tail)?.metadata_length();
    let start = file
        .len()
        .checked_sub(TAIL + len)
        .context("the Parquet footer is longer than the file")?;
    Ok(ParquetMetaDataReader::decode_metadata(
        &file[start..file.len() - TAIL],
    )?)
}

/// The row count a Parquet footer declares.
fn footer_rows(meta: &parquet::file::metadata::ParquetMetaData) -> Result<u64> {
    u64::try_from(meta.file_metadata().num_rows()).context("a negative Parquet row count")
}

/// The head of a ClickHouse error, without its stack trace.
fn trim_ch_error(text: &str) -> String {
    text.lines()
        .take(6)
        .collect::<Vec<_>>()
        .join("\n")
        .chars()
        .take(4000)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::target::{ExportTarget, TargetInput};
    use crate::types::{RivetType, TypeFidelity};

    /// The ClickHouse resolver's own spec for a column of `ty`.
    fn spec(name: &str, ty: RivetType) -> TargetColumnSpec {
        ExportTarget::ClickHouse.resolve_column(TargetInput {
            column_name: name,
            rivet_type: &ty,
            arrow_type: None,
            fidelity: TypeFidelity::Exact,
        })
    }

    /// A list of `inner`.
    fn list(inner: RivetType) -> RivetType {
        RivetType::List {
            inner: Box::new(inner),
        }
    }

    /// A microsecond UTC timestamp.
    fn utc_micros() -> RivetType {
        RivetType::Timestamp {
            unit: crate::types::TimeUnit::Microsecond,
            timezone: Some("UTC".into()),
        }
    }

    #[test]
    fn key_columns_are_not_null_and_the_rest_nullable() {
        let ddl = columns_ddl(
            &[
                spec("id", RivetType::Int64),
                spec("tags", list(RivetType::String)),
                spec("at", utc_micros()),
            ],
            &["id".to_string()],
        );
        assert_eq!(
            ddl,
            "  `id` Int64,\n  `tags` Array(Nullable(String)),\n  `at` Nullable(DateTime64(6, 'UTC'))"
        );
    }

    /// ClickHouse refuses a JSON or UUID column fed from Parquet, and `Array(Nullable(JSON))`
    /// cannot even be created (measured on 24.8); their arrays land by element like the scalars.
    #[test]
    fn json_and_uuid_land_as_their_parquet_types_also_inside_an_array() {
        let ddl = columns_ddl(
            &[
                spec("u", RivetType::Uuid),
                spec("j", RivetType::Json),
                spec("us", list(RivetType::Uuid)),
                spec("js", list(RivetType::Json)),
            ],
            &[],
        );
        assert_eq!(
            ddl,
            "  `u` Nullable(FixedString(16)),\n  `j` Nullable(String),\n  \
             `us` Array(Nullable(FixedString(16))),\n  `js` Array(Nullable(String))"
        );
    }

    #[test]
    fn a_cdc_log_collapses_by_key_and_an_incremental_log_does_not() {
        let pk = ["id".to_string()];
        let cdc = changelog_shape(true, "t", &pk, None).unwrap();
        assert_eq!(cdc.engine, "ReplacingMergeTree(__ver)");
        assert_eq!(cdc.order_by, "(`id`)");
        assert_eq!(cdc.not_null, &pk);
        assert!(cdc.version_column.contains("`__ver` UInt256 MATERIALIZED"));
        let inc = changelog_shape(false, "t", &pk, None).unwrap();
        assert_eq!(
            (inc.engine, inc.order_by.as_str()),
            ("MergeTree", "tuple()")
        );
        assert!(inc.not_null.is_empty() && inc.version_column.is_empty());
        let err = changelog_shape(true, "t", &[], None)
            .err()
            .expect("no key refuses");
        assert!(err.to_string().contains("needs a primary key"), "{err}");
    }

    #[test]
    fn the_log_puts_rivets_columns_first_and_never_twice() {
        let names: Vec<String> = changelog_specs(&[
            spec("__op", RivetType::String),
            spec("id", RivetType::Int64),
        ])
        .into_iter()
        .map(|s| s.column_name)
        .collect();
        assert_eq!(names, ["__op", "__pos", "__seq", "id"]);
    }

    /// An existing log is refused before any write when the load would corrupt it:
    /// another engine (a mode switch), another key (a changed `load.pk`), another type.
    #[test]
    fn an_existing_change_log_that_this_load_would_corrupt_is_refused() {
        let pk = ["id".to_string()];
        let cdc = changelog_shape(true, "t", &pk, None).unwrap();
        let inc = changelog_shape(false, "t", &pk, None).unwrap();
        let tz = "Nullable(DateTime64(6, 'UTC'))";
        let cdc_want = [("id", "Int64"), ("at", tz)];
        let inc_want = [("id", "Nullable(Int64)"), ("at", tz)];
        let cols = [("id", "Int64"), ("at", tz), ("__op", "Nullable(String)")];
        let inc_cols = [("id", "Nullable(Int64)"), ("at", tz)];
        let conflict = |shape: &ChangelogShape<'_>,
                        key: &[String],
                        existing: (&str, &str),
                        cols: &[(&str, &str)],
                        want: &[(&str, &str)]| {
            let (engine, sorting) = existing;
            changelog_conflict(
                "d.t__changes",
                "d.t",
                shape,
                key,
                (engine, sorting, ""),
                cols,
                want,
            )
        };
        assert_eq!(
            conflict(&cdc, &pk, ("ReplacingMergeTree", "id"), &cols, &cdc_want),
            None
        );
        assert_eq!(
            conflict(&inc, &pk, ("MergeTree", ""), &inc_cols, &inc_want),
            None
        );
        assert_eq!(
            conflict(
                &cdc,
                &pk,
                ("ReplicatedReplacingMergeTree", "id"),
                &cols,
                &cdc_want
            ),
            None,
            "a replicated or cloud engine is the same engine"
        );
        assert_eq!(
            conflict(&inc, &pk, ("SharedMergeTree", ""), &inc_cols, &inc_want),
            None
        );

        let mode = conflict(&inc, &pk, ("ReplacingMergeTree", "id"), &cols, &inc_want)
            .expect("cdc -> incremental");
        assert!(
            mode.contains("is a ReplacingMergeTree") && mode.contains("Nothing was written"),
            "{mode}"
        );
        assert!(
            conflict(&cdc, &pk, ("MergeTree", ""), &cols, &cdc_want).is_some(),
            "incremental -> cdc"
        );

        let wider = ["id".to_string(), "tenant".to_string()];
        let cdc2 = changelog_shape(true, "t", &wider, None).unwrap();
        let key = conflict(
            &cdc2,
            &wider,
            ("ReplacingMergeTree", "id"),
            &cols,
            &cdc_want,
        )
        .expect("pk changed");
        assert!(
            key.contains("collapses versions by (id)") && key.contains("(id, tenant)"),
            "{key}"
        );
        assert_eq!(
            conflict(
                &cdc2,
                &wider,
                ("ReplacingMergeTree", "id, `tenant`"),
                &cols,
                &cdc_want
            ),
            None,
            "the catalog's spelling of the same key"
        );

        let narrow = [("id", "Int64"), ("at", "Nullable(DateTime64(3, 'UTC'))")];
        let ty = conflict(&cdc, &pk, ("ReplacingMergeTree", "id"), &narrow, &cdc_want)
            .expect("a type changed");
        assert!(
            ty.contains("column `at`") && ty.contains("MODIFY COLUMN"),
            "{ty}"
        );
        assert_eq!(
            canonical_types_sql(&["Decimal64(6)".into(), tz.into()]),
            "SELECT toTypeName(defaultValueOfTypeName('Decimal64(6)')), \
             toTypeName(defaultValueOfTypeName('Nullable(DateTime64(6, \\'UTC\\'))')) FORMAT TSVRaw"
        );
    }

    /// A declared partition must match the log's (as the catalog spells it); an undeclared
    /// one leaves the log's partition alone.
    #[test]
    fn a_change_log_partitioned_otherwise_than_declared_is_refused() {
        let pk = ["id".to_string()];
        let cols = [("id", "Int64")];
        let declared = "toYYYYMM(`created_at`)";
        let conflict = |partition: Option<&str>, existing: &str| {
            let shape = changelog_shape(true, "t", &pk, partition).unwrap();
            changelog_conflict(
                "d.t__changes",
                "d.t",
                &shape,
                &pk,
                ("ReplacingMergeTree", "id", existing),
                &cols,
                &cols,
            )
        };
        assert_eq!(conflict(Some(declared), "toYYYYMM(created_at)"), None);
        assert_eq!(conflict(None, "toYear(created_at)"), None);
        assert_eq!(conflict(None, ""), None);
        let moved = conflict(Some(declared), "toYear(created_at)").expect("granularity changed");
        assert!(
            moved.contains("partitioned by toYear(created_at), but the load declares toYYYYMM")
                && moved.contains("Nothing was written"),
            "{moved}"
        );
        let added = conflict(Some(declared), "").expect("partition added to a flat log");
        assert!(added.contains("is partitioned by nothing"), "{added}");
    }

    #[test]
    fn a_cluster_column_that_is_not_an_identifier_is_named() {
        let cols = ["id".to_string(), "a;b".to_string()];
        assert_eq!(unsafe_column(&cols), Some(&cols[1]));
        assert_eq!(unsafe_column(&cols[..1]), None);
    }

    #[test]
    fn new_columns_are_added_only_when_there_are_some() {
        assert_eq!(alter_add_columns_sql("`d`.`t`", &[], &[]), None);
        let sql = alter_add_columns_sql(
            "`d`.`t`",
            &[spec("id", RivetType::Int64), spec("v", RivetType::String)],
            &["id".into()],
        )
        .expect("one ALTER");
        assert!(
            sql.contains("ADD COLUMN IF NOT EXISTS `id` Int64,"),
            "{sql}"
        );
        assert!(
            sql.contains("ADD COLUMN IF NOT EXISTS `v` Nullable(String)"),
            "{sql}"
        );
    }

    #[test]
    fn order_by_quotes_each_column_or_is_tuple() {
        assert_eq!(order_by(&[]), "tuple()");
        assert_eq!(
            order_by(&["id".to_string(), "order".to_string()]),
            "(`id`, `order`)"
        );
    }

    #[test]
    fn system_engine_maps_to_object_kind() {
        assert_eq!(object_kind_of(""), ObjectKind::Absent);
        assert_eq!(object_kind_of("View\n"), ObjectKind::View);
        assert_eq!(object_kind_of("ReplacingMergeTree"), ObjectKind::Table);
        assert_eq!(object_kind_of("MergeTree"), ObjectKind::Table);
        assert_eq!(object_kind_of("Dictionary"), ObjectKind::Other);
    }

    #[test]
    fn a_part_is_refused_only_past_either_end_of_datetime64() {
        use std::sync::Arc;
        let part = |secs: i64| {
            let col = arrow::array::TimestampMicrosecondArray::from(vec![secs * 1_000_000])
                .with_timezone("UTC");
            let batch = arrow::record_batch::RecordBatch::try_from_iter([(
                "ts",
                Arc::new(col) as arrow::array::ArrayRef,
            )])
            .unwrap();
            let mut buf = Vec::new();
            let mut w =
                parquet::arrow::ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
            parquet_footer(&buf).unwrap()
        };
        for ok in [DATETIME64_MIN_SECS, 0, DATETIME64_MAX_SECS] {
            assert!(
                refuse_unholdable_timestamps(&part(ok), "gs://b/p").is_ok(),
                "{ok}"
            );
        }
        for bad in [DATETIME64_MIN_SECS - 1, DATETIME64_MAX_SECS + 1] {
            let err = refuse_unholdable_timestamps(&part(bad), "gs://b/p").unwrap_err();
            assert!(
                format!("{err:#}").contains("outside ClickHouse DateTime64's range"),
                "{bad}"
            );
        }
    }

    /// A part whose timestamp the footer cannot bound is refused: an all-NULL row group
    /// beside 9999-12-31, and a part written without statistics.
    #[test]
    fn a_part_the_footer_cannot_clear_is_refused() {
        use parquet::file::properties::{EnabledStatistics, WriterProperties};
        use std::sync::Arc;
        let part = |groups: &[Vec<Option<i64>>], stats: EnabledStatistics| {
            let props = WriterProperties::builder()
                .set_statistics_enabled(stats)
                .build();
            let batches: Vec<_> = groups
                .iter()
                .map(|g| {
                    let secs: Vec<Option<i64>> =
                        g.iter().map(|s| s.map(|s| s * 1_000_000)).collect();
                    let col =
                        arrow::array::TimestampMicrosecondArray::from(secs).with_timezone("UTC");
                    arrow::record_batch::RecordBatch::try_from_iter([(
                        "ts",
                        Arc::new(col) as arrow::array::ArrayRef,
                    )])
                    .unwrap()
                })
                .collect();
            let mut buf = Vec::new();
            let mut w =
                parquet::arrow::ArrowWriter::try_new(&mut buf, batches[0].schema(), Some(props))
                    .unwrap();
            for batch in &batches {
                w.write(batch).unwrap();
                w.flush().unwrap();
            }
            w.close().unwrap();
            parquet_footer(&buf).unwrap()
        };
        let late = Some(DATETIME64_MAX_SECS + 1);
        let hidden = part(&[vec![None], vec![late]], EnabledStatistics::Chunk);
        let err = format!(
            "{:#}",
            refuse_unholdable_timestamps(&hidden, "gs://b/p").unwrap_err()
        );
        assert!(
            err.contains("outside ClickHouse DateTime64's range"),
            "{err}"
        );
        let blind = part(&[vec![Some(0)]], EnabledStatistics::None);
        let err = format!(
            "{:#}",
            refuse_unholdable_timestamps(&blind, "gs://b/p").unwrap_err()
        );
        assert!(err.contains("has no min/max statistics"), "{err}");
        assert!(
            refuse_unholdable_timestamps(
                &part(&[vec![None], vec![Some(0)]], EnabledStatistics::Chunk),
                "gs://b/p"
            )
            .is_ok(),
            "an all-NULL row group beside an in-range one loads"
        );
    }

    #[test]
    fn a_failed_request_does_not_print_the_urls_password() {
        unsafe { std::env::set_var("RIVET_CH_REDACT_TEST_PASSWORD", "x") };
        let loader = ClickhouseLoader::new(
            "http://u:hunter2@127.0.0.1:1",
            "d",
            "u",
            "RIVET_CH_REDACT_TEST_PASSWORD",
            crate::config::DestinationConfig::default(),
        );
        let err = format!("{:#}", loader.query("SELECT 1").unwrap_err());
        assert!(
            !err.contains("hunter2") && err.contains("127.0.0.1:1"),
            "{err}"
        );
    }

    #[test]
    fn the_row_count_is_the_parquet_footers() {
        use std::sync::Arc;
        let batch = arrow::record_batch::RecordBatch::try_from_iter([(
            "id",
            Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3])) as arrow::array::ArrayRef,
        )])
        .unwrap();
        let mut buf = Vec::new();
        let mut w = parquet::arrow::ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
        w.write(&batch).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        assert_eq!(footer_rows(&parquet_footer(&buf).unwrap()).unwrap(), 6);
        assert!(
            parquet_footer(&buf[..4]).is_err(),
            "a truncated file is refused"
        );
    }

    #[test]
    #[ignore = "live: requires docker compose clickhouse"]
    fn the_change_log_keeps_the_latest_source_position_whatever_the_insert_order() {
        unsafe { std::env::set_var("RIVET_CH_VERSION_TEST_PASSWORD", "rivet") };
        let db = format!("rivet_chver_{}", std::process::id());
        let loader = ClickhouseLoader::new(
            "http://127.0.0.1:8123",
            &db,
            "rivet",
            "RIVET_CH_VERSION_TEST_PASSWORD",
            crate::config::DestinationConfig::default(),
        )
        .cdc(true);
        loader
            .query(&format!("CREATE DATABASE IF NOT EXISTS {db}"))
            .unwrap();
        let specs = [spec("id", RivetType::Int64), spec("v", RivetType::String)];
        loader
            .append_changelog("t", &specs, &[], &["id".to_string()])
            .unwrap();
        // (id, __pos, __seq, v), each key newest-first; `v` names the row that must win.
        let rows = [
            (1, r#"{"lsn":"3D/484A4908"}"#, 0, "win"),
            (1, r#"{"lsn":"A/0"}"#, -1, "snapshot"),
            (1, r#"{"lsn":"9/FFFFFFFF"}"#, 3, "old"),
            (2, r#"{"file":"mysql-bin.1000000","pos":4}"#, 0, "win"),
            (2, r#"{"file":"mysql-bin.999999","pos":999999}"#, 7, "old"),
            (3, r#"{"lsn":"0000002b000000000001"}"#, 0, "win"),
            (3, r#"{"lsn":"0000002a000001a80004"}"#, 9, "old"),
            (4, r#"{"lsn":"A/0"}"#, -1, "win"),
            (4, r#"{"lsn":"9/FFFFFFFF"}"#, 0, "old"),
            (5, r#"{"lsn":"A/0"}"#, 1, "win"),
            (5, r#"{"lsn":"A/0"}"#, -1, "snapshot"),
        ];
        let changes = loader.quoted("t__changes");
        loader
            .query(&format!("SYSTEM STOP MERGES {changes}"))
            .unwrap();
        for (id, pos, seq, v) in rows {
            loader
                .query(&format!(
                    "INSERT INTO {changes} (__op, __pos, __seq, id, v) VALUES \
                     ('update', {}, {seq}, {id}, '{v}')",
                    literal(pos)
                ))
                .unwrap();
        }
        loader
            .create_current_view(
                "t",
                &["id"],
                &cdc::CompactOrder::Cdc(cdc::SourceEngine::Postgres),
            )
            .unwrap();
        let got = loader
            .query(&format!(
                "SELECT id, v FROM `{db}`.`t` ORDER BY id FORMAT TSV"
            ))
            .unwrap();
        loader.query(&format!("DROP DATABASE {db}")).unwrap();
        assert_eq!(got, "1\twin\n2\twin\n3\twin\n4\twin\n5\twin");
    }

    /// A `__pos` of no known shape (a Mongo resume token) fails the insert loudly instead
    /// of versioning the row 0, where insert order would decide the winner.
    #[test]
    #[ignore = "live: requires docker compose clickhouse"]
    fn an_unknown_position_shape_fails_the_insert() {
        unsafe { std::env::set_var("RIVET_CH_SHAPE_TEST_PASSWORD", "rivet") };
        let db = format!("rivet_chshape_{}", std::process::id());
        let loader = ClickhouseLoader::new(
            "http://127.0.0.1:8123",
            &db,
            "rivet",
            "RIVET_CH_SHAPE_TEST_PASSWORD",
            crate::config::DestinationConfig::default(),
        )
        .cdc(true);
        loader
            .query(&format!("CREATE DATABASE IF NOT EXISTS {db}"))
            .unwrap();
        loader
            .append_changelog(
                "t",
                &[spec("id", RivetType::Int64)],
                &[],
                &["id".to_string()],
            )
            .unwrap();
        let changes = loader.quoted("t__changes");
        let insert = |pos: &str| {
            loader.query(&format!(
                "INSERT INTO {changes} (__op, __pos, __seq, id) VALUES ('update', {}, 0, 1)",
                literal(pos)
            ))
        };
        let known = insert(r#"{"lsn":"0/1"}"#);
        let unknown = insert(r#"{"_data":"8263"}"#);
        loader.query(&format!("DROP DATABASE {db}")).unwrap();
        known.expect("a PostgreSQL LSN is versioned");
        let e = format!("{:#}", unknown.expect_err("a resume token has no version"));
        assert!(e.contains("unrecognised __pos shape"), "{e}");
    }

    /// Through a named collection ClickHouse reads the part itself: columns match by
    /// name whatever their order, the count is the insert's own, and a NULL key refuses.
    #[test]
    #[ignore = "live: requires docker compose clickhouse (with dev/clickhouse/named_collections.xml) + minio"]
    fn a_pulled_part_lands_by_column_name_and_a_null_key_refuses() {
        unsafe {
            std::env::set_var("RIVET_CH_PULL_TEST_PASSWORD", "rivet");
            std::env::set_var("RIVET_CH_PULL_TEST_MINIO_KEY", "minioadmin");
        }
        let db = format!("rivet_chpull_{}", std::process::id());
        let store: crate::config::DestinationConfig = serde_yaml_ng::from_str(
            "{ type: s3, bucket: rivet-qa-ch-pull, region: us-east-1, \
             endpoint: \"http://127.0.0.1:9000\", access_key_env: RIVET_CH_PULL_TEST_MINIO_KEY, \
             secret_key_env: RIVET_CH_PULL_TEST_MINIO_KEY }",
        )
        .expect("an S3 destination");
        let loader = ClickhouseLoader::new(
            "http://127.0.0.1:8123",
            &db,
            "rivet",
            "RIVET_CH_PULL_TEST_PASSWORD",
            store,
        )
        .cdc(true)
        .named_collection(Some("rivet_stand_minio".into()));
        // The MinIO container is found by the port it publishes, not by a compose project name.
        let ps = std::process::Command::new("docker")
            .args(["ps", "--filter", "publish=9000", "--format", "{{.Names}}"])
            .output()
            .expect("spawn `docker ps`");
        let minio = String::from_utf8_lossy(&ps.stdout)
            .lines()
            .next()
            .map(str::to_string)
            .expect("no running container publishes host port 9000 (MinIO)");
        let made = std::process::Command::new("docker")
            .args(["exec", &minio, "sh", "-c"])
            .arg(
                "mc alias set local http://127.0.0.1:9000 minioadmin minioadmin >/dev/null \
                 && mc mb --ignore-existing local/rivet-qa-ch-pull",
            )
            .output()
            .expect("spawn `docker exec`");
        assert!(
            made.status.success(),
            "`mc mb local/rivet-qa-ch-pull` in container `{minio}` failed ({}): {}",
            made.status,
            String::from_utf8_lossy(&made.stderr)
        );
        let write = |name: &str, select: &str| {
            loader
                .query(&format!(
                    "INSERT INTO FUNCTION s3(rivet_stand_minio, filename = 'rivet-qa-ch-pull/{db}/{name}', \
                     format = 'Parquet') {select} SETTINGS s3_truncate_on_insert = 1"
                ))
                .unwrap();
        };
        write(
            "ok.parquet",
            "SELECT 'b' AS v, toInt64(0) AS __seq, '{\"lsn\":\"0/2\"}' AS __pos, toInt64(1) AS id, \
             'update' AS __op UNION ALL SELECT 'a', 0, '{\"lsn\":\"0/1\"}', 1, 'insert' \
             UNION ALL SELECT 'c', 0, '{\"lsn\":\"0/1\"}', 2, 'insert'",
        );
        write(
            "null_key.parquet",
            "SELECT 'x' AS v, CAST(NULL, 'Nullable(Int64)') AS id",
        );
        loader
            .query(&format!("CREATE DATABASE IF NOT EXISTS {db}"))
            .unwrap();
        let specs = [spec("id", RivetType::Int64), spec("v", RivetType::String)];
        let pk = ["id".to_string()];
        let uri = |name: &str| format!("gs://rivet-qa-ch-pull/{db}/{name}");
        let written = loader
            .append_changelog("t", &specs, &[uri("ok.parquet")], &pk)
            .unwrap();
        let refused = loader.append_changelog("t", &specs, &[uri("null_key.parquet")], &pk);
        loader
            .create_current_view(
                "t",
                &["id"],
                &cdc::CompactOrder::Cdc(cdc::SourceEngine::Postgres),
            )
            .unwrap();
        let got = loader
            .query(&format!(
                "SELECT id, v FROM `{db}`.`t` ORDER BY id FORMAT TSV"
            ))
            .unwrap();
        loader.query(&format!("DROP DATABASE {db}")).unwrap();
        assert_eq!(written, 3, "the count is what ClickHouse wrote");
        assert_eq!(
            got, "1\tb\n2\tc",
            "columns matched by name, the later version won"
        );
        let e = format!("{:#}", refused.expect_err("a NULL key must refuse"));
        assert!(e.contains("`id`") && e.contains("NULL"), "{e}");
    }

    #[test]
    fn a_first_full_load_renames_and_a_later_one_exchanges_then_drops() {
        assert_eq!(
            swap_in(ObjectKind::Absent, "s", "t"),
            [("RENAME TABLE s TO t".to_string(), Repeat::Undelivered, None)]
        );
        assert_eq!(
            swap_in(ObjectKind::Table, "s", "t"),
            [
                (
                    "EXCHANGE TABLES s AND t".to_string(),
                    Repeat::Undelivered,
                    Some("clickhouse_full_after_exchange")
                ),
                ("DROP TABLE IF EXISTS s".to_string(), Repeat::Safe, None)
            ]
        );
        assert_eq!(parse_pair("7\t3\n"), Some((7, 3)));
        assert_eq!(parse_pair("7"), None);
        assert_eq!(parse_pair("x\t3"), None);
    }

    /// The pure SQL and naming helpers, each pinned to its exact text.
    #[test]
    fn ddl_names_and_error_trimming_render_exactly() {
        assert_eq!(
            create_table_sql(
                "CREATE TABLE IF NOT EXISTS",
                "`d`.`t`",
                "  `id` Int64",
                "ReplacingMergeTree(__ver)",
                None,
                "(`id`)"
            ),
            "CREATE TABLE IF NOT EXISTS `d`.`t` (\n  `id` Int64\n) ENGINE = ReplacingMergeTree(__ver) \
             ORDER BY (`id`) SETTINGS allow_nullable_key = 1"
        );
        assert_eq!(
            create_table_sql(
                "CREATE TABLE",
                "`d`.`t`",
                "  `id` Int64",
                "MergeTree",
                Some("toYYYYMM(`ts`)"),
                "tuple()"
            ),
            "CREATE TABLE `d`.`t` (\n  `id` Int64\n) ENGINE = MergeTree PARTITION BY toYYYYMM(`ts`) \
             ORDER BY tuple() SETTINGS allow_nullable_key = 1"
        );
        let loader = ClickhouseLoader::new("http://ch:8123/", "raw", "u", "P", Default::default());
        assert_eq!(TargetLoader::fqtn(&loader, "orders"), "raw.orders");
        assert_eq!(loader.quoted("orders"), "`raw`.`orders`");
        assert_eq!(
            loader.system_filter("o'r", "name"),
            "database = 'raw' AND name = 'o\\'r'"
        );
        let long = format!("Code: 60. DB::Exception: x\n{}", "trace\n".repeat(20));
        let trimmed = trim_ch_error(&long);
        assert_eq!(trimmed.lines().count(), 6, "the head, not the stack trace");
        assert!(trimmed.starts_with("Code: 60."));
    }

    /// build_loader marks a CDC plan's loader as CDC: adopting a full-load table then
    /// refuses before any HTTP call, while an incremental loader would go to the server.
    #[test]
    fn build_loader_wires_the_cdc_flag_from_the_plan_mode() {
        unsafe { std::env::set_var("RIVET_CH_WIRE_TEST_PASSWORD", "x") };
        let mut plan = crate::load::plan::test_plan(crate::load::plan::LoadMode::Cdc, "gs://b/p/");
        plan.load.target = crate::load::plan::LoadTarget::Clickhouse {
            url: "http://127.0.0.1:1".into(),
            database: "d".into(),
            user: "u".into(),
            password_env: "RIVET_CH_WIRE_TEST_PASSWORD".into(),
            named_collection: None,
            ca_file: None,
        };
        let cdc = crate::load::build_loader(&plan, "run");
        let err = cdc
            .adopt_as_changelog("t")
            .expect_err("a CDC log cannot adopt a table");
        assert!(err.is::<crate::load::Refused>(), "{err:#}");
        assert!(
            format!("{err:#}").contains(
                "also set `cdc.initial: snapshot` (or a `cdc.backfill:`) and `rivet run` again \
                 before the load — the stream is already anchored"
            ),
            "{err:#}"
        );
        plan.mode = crate::load::plan::LoadMode::Incremental;
        let inc = crate::load::build_loader(&plan, "run");
        let err = inc
            .adopt_as_changelog("t")
            .expect_err("nothing listens on :1");
        assert!(
            !err.is::<crate::load::Refused>(),
            "an incremental log adopts by RENAME, over HTTP: {err:#}"
        );
    }

    #[test]
    fn only_a_plain_key_is_read_by_clickhouse_itself() {
        assert!(pullable("b", "exports/t/snapshot/part-000=1_run.parquet"));
        for key in [
            "t/a?b.parquet",
            "t/a*b.parquet",
            "t/{a,b}.parquet",
            "t/x%20y.parquet",
            "Order Details/p.parquet",
            "Détails/p.parquet",
            "t/a#b.parquet",
        ] {
            assert!(!pullable("b", key), "{key}");
        }
    }

    #[test]
    fn a_pull_reads_the_part_with_the_function_of_its_store() {
        assert_eq!(
            pull_source("nc", "gs", "b", "p/part'1.parquet"),
            "gcs(nc, filename = 'b/p/part\\'1.parquet', format = 'Parquet')"
        );
        assert_eq!(
            pull_source("nc", "s3", "b", "p/a.parquet"),
            "s3(nc, filename = 'b/p/a.parquet', format = 'Parquet')"
        );
        assert_eq!(
            pull_source("nc", "az", "c", "p/a.parquet"),
            "azureBlobStorage(nc, container = 'c', blob_path = 'p/a.parquet', format = 'Parquet')"
        );
    }

    /// A lost answer is resent only where a second run is harmless; a connection never made
    /// always is; a server's own refusal never is; and the budget ends every loop.
    #[test]
    fn only_a_harmless_repeat_is_resent_and_never_past_the_budget() {
        use Failure::*;
        use Repeat::*;
        for repeat in [Safe, Undelivered] {
            assert!(retry_after(Connect, repeat, 1), "{repeat:?}");
            assert!(retry_after(Connect, repeat, MAX_ATTEMPTS - 1), "{repeat:?}");
            assert!(!retry_after(Connect, repeat, MAX_ATTEMPTS), "{repeat:?}");
            assert!(
                !retry_after(Status(500, Some(349)), repeat, 1),
                "a NULL key"
            );
            assert!(
                !retry_after(Status(400, Some(62)), repeat, 1),
                "a syntax error"
            );
            assert!(!retry_after(Status(403, None), repeat, 1), "auth");
        }
        assert!(retry_after(Transport, Safe, 1));
        assert!(!retry_after(Transport, Undelivered, 1));
        for status in [429, 502, 503, 504] {
            assert!(retry_after(Status(status, None), Safe, 1), "{status}");
            assert!(
                !retry_after(Status(status, None), Undelivered, 1),
                "{status}"
            );
        }
        for code in [202, 252] {
            assert!(retry_after(Status(500, Some(code)), Safe, 1), "{code}");
            assert!(
                !retry_after(Status(500, Some(code)), Undelivered, 1),
                "{code}"
            );
        }
        assert!(!retry_after(Transport, Safe, MAX_ATTEMPTS));
        assert_eq!(MAX_ATTEMPTS, 5);
        assert_eq!(changelog_repeat(true), Safe, "a CDC log collapses copies");
        assert_eq!(
            changelog_repeat(false),
            Undelivered,
            "an incremental log keeps them"
        );
    }

    /// `load.ca_file` is read when the client is built: a missing or non-PEM file refuses
    /// naming the path, a CA certificate is accepted.
    #[test]
    fn a_ca_file_must_exist_and_hold_a_certificate() {
        const CA: &str = "-----BEGIN CERTIFICATE-----
MIIBhzCCAS2gAwIBAgIUWztYYpCs1NyMKmvB7Bax29YmgMowCgYIKoZIzj0EAwIw
GDEWMBQGA1UEAwwNcml2ZXQtdGVzdC1jYTAgFw0yNjA5MjkyMDAyNDJaGA8yMTI2
MDkwNTIwMDI0MlowGDEWMBQGA1UEAwwNcml2ZXQtdGVzdC1jYTBZMBMGByqGSM49
AgEGCCqGSM49AwEHA0IABDnl0o+5mROtcCyvAOytFappXSft92U7GqO9OZ1F3yD+
WIdop7cCa/fMfPgTmZFsU4MUAD+7kclPWub8WFCPikGjUzBRMB0GA1UdDgQWBBSO
1pHNF9NUPaPAMrTPsin0x+VbCDAfBgNVHSMEGDAWgBSO1pHNF9NUPaPAMrTPsin0
x+VbCDAPBgNVHRMBAf8EBTADAQH/MAoGCCqGSM49BAMCA0gAMEUCIHphUR1wZCCD
XuvoDdmSxlWD2Luc2HNVLlb0+FfdTP66AiEA/kHRfo1HOlxB43y04rUf4ViuH1Fd
J10BsZgn5wxFlM4=
-----END CERTIFICATE-----
";
        let dir = tempfile::tempdir().unwrap();
        let write = |name: &str, body: &str| {
            let p = dir.path().join(name);
            std::fs::write(&p, body).unwrap();
            p.display().to_string()
        };
        let missing = dir.path().join("absent.pem").display().to_string();
        let err = format!("{:#}", http_client(Some(&missing)).unwrap_err());
        assert!(
            err.contains("cannot read `load.ca_file`") && err.contains(&missing),
            "{err}"
        );
        let junk = write("junk.pem", "not a certificate\n");
        let err = format!("{:#}", http_client(Some(&junk)).unwrap_err());
        assert!(
            err.contains("holds no PEM certificate") && err.contains(&junk),
            "{err}"
        );
        assert!(http_client(Some(&write("ca.pem", CA))).is_ok());
        assert!(http_client(None).is_ok());
    }

    #[test]
    fn a_literal_cannot_break_out_of_its_quotes() {
        assert_eq!(literal("a'b\\c"), r"'a\'b\\c'");
    }
}
