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
use crate::types::target::TargetColumnSpec;

/// HTTP timeout for one ClickHouse call; one part's INSERT is seconds on a LAN.
const HTTP_TIMEOUT: Duration = Duration::from_secs(20 * 60);

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
        }
    }

    /// Set the full-load table's `ORDER BY` columns.
    pub(crate) fn cluster_by(mut self, cols: Vec<String>) -> Self {
        self.cluster_by = cols;
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

    /// POST `body` with `params`; returns the response text and the `written_rows` summary.
    fn post(&self, params: &[(&str, &str)], body: Vec<u8>) -> Result<(String, u64)> {
        let pass = std::env::var(&self.password_env).with_context(|| {
            format!(
                "ClickHouse load: `password_env` names `{}`, which is not set",
                self.password_env
            )
        })?;
        let resp = reqwest::blocking::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .context("building the ClickHouse HTTP client")?
            .post(&self.url)
            .basic_auth(&self.user, Some(pass))
            .query(&[("wait_end_of_query", "1")])
            .query(params)
            .body(body)
            .send()
            .with_context(|| format!("ClickHouse HTTP request to {} failed", self.url))?;
        let status = resp.status();
        let written = resp
            .headers()
            .get("X-ClickHouse-Summary")
            .and_then(|h| h.to_str().ok())
            .map(written_rows)
            .transpose()?
            .unwrap_or(0);
        let text = resp
            .text()
            .context("reading the ClickHouse HTTP response")?;
        if !status.is_success() {
            bail!("ClickHouse (HTTP {status}): {}", trim_ch_error(&text));
        }
        Ok((text.trim().to_string(), written))
    }

    /// Run one SQL statement and return its output.
    fn query(&self, sql: &str) -> Result<String> {
        Ok(self.post(&[], sql.as_bytes().to_vec())?.0)
    }

    /// A single `u64` from a `SELECT` returning one number.
    fn number(&self, sql: &str) -> Result<u64> {
        let out = self.query(sql)?;
        out.parse()
            .with_context(|| format!("ClickHouse returned `{out}` for `{sql}`"))
    }

    /// Insert every part in `uris` into `target`; the rows ClickHouse reports written.
    fn insert_uris(&self, target: &str, uris: &[String]) -> Result<u64> {
        let insert = format!("INSERT INTO {target} FORMAT Parquet");
        let mut total = 0;
        for uri in uris {
            let (_, key) = super::split_gs_uri(uri)?;
            let bytes = self
                .store()?
                .read(key)
                .with_context(|| format!("reading {uri} for the ClickHouse load"))?;
            let params = [
                ("query", insert.as_str()),
                ("input_format_null_as_default", "0"),
            ];
            total += self
                .post(&params, bytes)
                .with_context(|| format!("inserting {uri} into {target}"))?
                .1;
        }
        Ok(total)
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
        for c in &self.cluster_by {
            if !super::is_safe_load_ident(c) {
                bail!(
                    "ClickHouse load: `cluster_by` column `{}` is not a plain SQL identifier",
                    c.escape_default()
                );
            }
        }
        let target = self.quoted(table);
        let swap = self.quoted(&format!("{table}__rivet_swap"));
        self.query(&create_table_sql(
            "CREATE OR REPLACE TABLE",
            &swap,
            &columns_ddl(specs, &[]),
            "MergeTree",
            &order_by(&self.cluster_by),
        ))?;
        let rows = self.insert_uris(&swap, uris)?;
        match self.object_kind(table)? {
            ObjectKind::Absent => self.query(&format!("RENAME TABLE {swap} TO {target}"))?,
            _ => {
                self.query(&format!("EXCHANGE TABLES {swap} AND {target}"))?;
                self.query(&format!("DROP TABLE {swap}"))?
            }
        };
        Ok(rows)
    }

    fn append_changelog(
        &self,
        table: &str,
        specs: &[TargetColumnSpec],
        uris: &[String],
        pk: &[String],
    ) -> Result<u64> {
        let mut full = cdc::meta_column_specs(Warehouse::ClickHouse);
        full.extend(
            specs
                .iter()
                .filter(|s| !cdc::is_meta_column(&s.column_name))
                .cloned(),
        );
        let changes = self.quoted(&format!("{table}__changes"));
        let (engine, key, extra) = if self.cdc {
            if pk.is_empty() {
                bail!(
                    "ClickHouse CDC load of `{table}` needs a primary key: the change log \
                     collapses versions by key (ADR-0035 CH2) — set `load.pk`"
                );
            }
            (
                "ReplacingMergeTree(__ver)",
                order_by(pk),
                format!(
                    ",\n  `__ver` UInt256 MATERIALIZED {}",
                    cdc::CLICKHOUSE_VERSION_EXPR
                ),
            )
        } else {
            ("MergeTree", order_by(&[]), String::new())
        };
        let key_cols: &[String] = if self.cdc { pk } else { &[] };
        let ddl = columns_ddl(&full, key_cols) + &extra;
        self.query(&create_table_sql(
            "CREATE TABLE IF NOT EXISTS",
            &changes,
            &ddl,
            engine,
            &key,
        ))?;
        let alter = alter_add_columns_sql(&changes, &full, key_cols);
        if !alter.is_empty() {
            self.query(&alter)
                .with_context(|| format!("adding new columns to `{table}__changes`"))?;
        }
        self.insert_uris(&changes, uris)
    }

    fn warehouse(&self) -> Warehouse {
        Warehouse::ClickHouse
    }

    fn create_view(&self, _table: &str, view_sql: &str) -> Result<()> {
        self.query(view_sql).map(|_| ())
    }

    fn changes_has_prior_changes(&self, table: &str) -> Result<bool> {
        let changes = format!("{table}__changes");
        if self.object_kind(&changes)? == ObjectKind::Absent {
            return Ok(false);
        }
        let n = self.number(&format!(
            "SELECT count() FROM {} WHERE __op IS NOT NULL",
            self.quoted(&changes)
        ))?;
        Ok(n > 0)
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
        let mut it = out.split('\t').map(str::parse::<u64>);
        match (it.next(), it.next()) {
            (Some(Ok(total)), Some(Ok(matched))) => Ok((total, matched)),
            _ => bail!("ClickHouse returned `{out}` for a column-overlap probe of `{table}`"),
        }
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
                 Drop or rename `{}` and re-run — nothing was changed",
                TargetLoader::fqtn(self, table),
                TargetLoader::fqtn(self, table)
            )));
        }
        let to = self.quoted(&format!("{table}__changes"));
        self.query(&format!("RENAME TABLE {from} TO {to}"))?;
        self.query(&format!(
            "ALTER TABLE {to} ADD COLUMN IF NOT EXISTS `__op` Nullable(String) FIRST, \
             ADD COLUMN IF NOT EXISTS `__pos` Nullable(String) AFTER `__op`, \
             ADD COLUMN IF NOT EXISTS `__seq` Nullable(Int64) AFTER `__pos`"
        ))
        .map(|_| ())
    }
}

/// `CREATE … <fqtn> (<ddl>) ENGINE = <engine> ORDER BY <key>`, allowing a Nullable key column.
fn create_table_sql(verb: &str, fqtn: &str, ddl: &str, engine: &str, key: &str) -> String {
    format!(
        "{verb} {fqtn} (\n{ddl}\n) ENGINE = {engine} ORDER BY {key} SETTINGS allow_nullable_key = 1"
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
fn column_type(spec: &TargetColumnSpec, not_null: bool) -> String {
    let t = &spec.target_type;
    if not_null || t.starts_with("Array(") || t.starts_with("LowCardinality(") {
        t.clone()
    } else {
        format!("Nullable({t})")
    }
}

/// `ALTER TABLE … ADD COLUMN IF NOT EXISTS …` for every spec, or `""` when there are none.
fn alter_add_columns_sql(fqtn: &str, specs: &[TargetColumnSpec], not_null: &[String]) -> String {
    if specs.is_empty() {
        return String::new();
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
    format!("ALTER TABLE {fqtn}\n  {adds}")
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

/// `written_rows` from an `X-ClickHouse-Summary` header.
fn written_rows(summary: &str) -> Result<u64> {
    let v: serde_json::Value = serde_json::from_str(summary)
        .with_context(|| format!("parsing X-ClickHouse-Summary `{summary}`"))?;
    let n = &v["written_rows"];
    n.as_u64()
        .or_else(|| n.as_str().and_then(|s| s.parse().ok()))
        .with_context(|| format!("X-ClickHouse-Summary has no written_rows: `{summary}`"))
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
    use crate::types::target::TargetStatus;

    fn spec(name: &str, ty: &str) -> TargetColumnSpec {
        TargetColumnSpec {
            column_name: name.to_string(),
            target_type: ty.to_string(),
            autoload_type: String::new(),
            status: TargetStatus::Ok,
            note: None,
            cast_sql: None,
        }
    }

    #[test]
    fn key_columns_are_not_null_and_the_rest_nullable() {
        let ddl = columns_ddl(
            &[
                spec("id", "Int64"),
                spec("tags", "Array(Nullable(String))"),
                spec("at", "DateTime64(6, 'UTC')"),
            ],
            &["id".to_string()],
        );
        assert_eq!(
            ddl,
            "  `id` Int64,\n  `tags` Array(Nullable(String)),\n  `at` Nullable(DateTime64(6, 'UTC'))"
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
    fn written_rows_reads_the_quoted_or_bare_number() {
        assert_eq!(written_rows(r#"{"written_rows":"42"}"#).unwrap(), 42);
        assert_eq!(written_rows(r#"{"written_rows":7}"#).unwrap(), 7);
        assert!(written_rows(r#"{"read_rows":"1"}"#).is_err());
    }

    /// Versions decide the winner, never insert order: each key's rows go in
    /// newest-first, and the oldest row must not survive (ADR-0035 CH3).
    #[test]
    #[ignore = "live: requires docker compose clickhouse"]
    fn the_change_log_keeps_the_latest_source_position_whatever_the_insert_order() {
        unsafe { std::env::set_var("RIVET_CH_VERSION_TEST_PASSWORD", "rivet") };
        let db = format!("rivet_tmp_chver_{}", std::process::id());
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
        let specs = [spec("id", "Int64"), spec("v", "String")];
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
        let view = cdc::dedup_view_sql(
            Warehouse::ClickHouse,
            &format!("{db}.t"),
            &format!("{db}.t__changes"),
            &["id"],
            cdc::SourceEngine::Postgres,
        );
        loader.create_view("t", &view).unwrap();
        let got = loader
            .query(&format!(
                "SELECT id, v FROM `{db}`.`t` ORDER BY id FORMAT TSV"
            ))
            .unwrap();
        loader.query(&format!("DROP DATABASE {db}")).unwrap();
        assert_eq!(got, "1\twin\n2\twin\n3\twin\n4\twin\n5\twin");
    }

    #[test]
    fn a_literal_cannot_break_out_of_its_quotes() {
        assert_eq!(literal("a'b\\c"), r"'a\'b\\c'");
    }
}
