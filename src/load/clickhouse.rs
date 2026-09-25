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
    /// Read parts server-side through this named collection instead of sending them.
    named_collection: Option<String>,
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
        }
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

    /// POST `body` with `params`, waiting for the statement to finish; returns the response text.
    fn post(&self, params: &[(&str, &str)], body: Vec<u8>) -> Result<String> {
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
        let text = resp
            .text()
            .context("reading the ClickHouse HTTP response")?;
        if !status.is_success() {
            bail!("ClickHouse (HTTP {status}): {}", trim_ch_error(&text));
        }
        Ok(text.trim().to_string())
    }

    /// Run one SQL statement and return its output.
    fn query(&self, sql: &str) -> Result<String> {
        self.post(&[], sql.as_bytes().to_vec())
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
    fn insert_uris(&self, target: &str, uris: &[String]) -> Result<u64> {
        let mut total = 0;
        for uri in uris {
            let (bucket, key) = super::split_gs_uri(uri)?;
            let (query, body, rows) = match &self.named_collection {
                Some(nc) => (
                    pull_insert_sql(target, nc, bucket, key),
                    Vec::new(),
                    self.number(&pull_count_sql(nc, bucket, key))?,
                ),
                None => {
                    let bytes = self
                        .store()?
                        .read(key)
                        .with_context(|| format!("reading {uri} for the ClickHouse load"))?;
                    let rows = parquet_rows(&bytes).with_context(|| format!("reading {uri}"))?;
                    (format!("INSERT INTO {target} FORMAT Parquet"), bytes, rows)
                }
            };
            let params = [
                ("query", query.as_str()),
                ("input_format_null_as_default", "0"),
                ("async_insert", "0"),
            ];
            self.post(&params, body)
                .with_context(|| format!("inserting {uri} into {target}"))?;
            total += rows;
        }
        Ok(total)
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
            "SELECT engine, sorting_key FROM system.tables WHERE {} FORMAT TSV",
            self.system_filter(&name, "name")
        ))?;
        let Some((engine, sorting_key)) = found.split_once('\t') else {
            return Ok(None);
        };
        let cols = self.query(&format!(
            "SELECT name, type FROM system.columns WHERE {} FORMAT TSV",
            self.system_filter(&name, "table")
        ))?;
        let existing: Vec<(&str, &str)> = cols.lines().filter_map(|l| l.split_once('\t')).collect();
        Ok(changelog_conflict(
            &TargetLoader::fqtn(self, &name),
            &TargetLoader::fqtn(self, table),
            shape,
            pk,
            (engine, sorting_key),
            &existing,
            full,
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
        let full = changelog_specs(specs);
        let changes = self.quoted(&format!("{table}__changes"));
        let shape = changelog_shape(self.cdc, table, pk)?;
        if let Some(why) = self.existing_changelog_conflict(table, &shape, pk, &full)? {
            return Err(super::refused(why));
        }
        let ddl = columns_ddl(&full, shape.not_null) + &shape.version_column;
        self.query(&create_table_sql(
            "CREATE TABLE IF NOT EXISTS",
            &changes,
            &ddl,
            shape.engine,
            &shape.order_by,
        ))?;
        if let Some(alter) = alter_add_columns_sql(&changes, &full, shape.not_null) {
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

/// `INSERT … SELECT * FROM gcs(<collection>, filename = '<bucket>/<key>')`: ClickHouse reads the part.
fn pull_insert_sql(target: &str, collection: &str, bucket: &str, key: &str) -> String {
    format!(
        "INSERT INTO {target} SELECT * FROM gcs({collection}, filename = {}, format = 'Parquet')",
        literal(&format!("{bucket}/{key}"))
    )
}

/// `SELECT count() FROM gcs(<collection>, filename = …)`: the rows ClickHouse sees in the part.
fn pull_count_sql(collection: &str, bucket: &str, key: &str) -> String {
    format!(
        "SELECT count() FROM gcs({collection}, filename = {}, format = 'Parquet')",
        literal(&format!("{bucket}/{key}"))
    )
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
    order_by: String,
    not_null: &'a [String],
    version_column: String,
}

/// A CDC log collapses versions by the PK; an incremental log is a plain `MergeTree`.
fn changelog_shape<'a>(cdc: bool, table: &str, pk: &'a [String]) -> Result<ChangelogShape<'a>> {
    if !cdc {
        return Ok(ChangelogShape {
            engine: "MergeTree",
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
        order_by: order_by(pk),
        not_null: pk,
        version_column: format!(
            ",\n  `__ver` UInt256 MATERIALIZED {}",
            cdc::CLICKHOUSE_VERSION_EXPR
        ),
    })
}

/// Why an existing change log (`engine`, `sorting_key`, column types) cannot take a load
/// shaped `shape` over `wanted`, or `None`: another engine (the export changed mode), another
/// key (a changed `load.pk`) or another column type would each corrupt it silently.
fn changelog_conflict(
    changes: &str,
    view: &str,
    shape: &ChangelogShape<'_>,
    pk: &[String],
    (engine, sorting_key): (&str, &str),
    existing: &[(&str, &str)],
    wanted: &[TargetColumnSpec],
) -> Option<String> {
    let want_engine = shape.engine.split('(').next().unwrap_or(shape.engine);
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
    let squash = |t: &str| t.replace(' ', "");
    wanted.iter().find_map(|s| {
        let want = column_type(s, shape.not_null.contains(&s.column_name));
        let (_, have) = existing.iter().find(|(n, _)| *n == s.column_name)?;
        (squash(have) != squash(&want)).then(|| {
            format!(
                "column `{}` of `{changes}` is {have}, but the export now resolves it to {want}; \
                 inserting would convert every value silently. Widen it with `ALTER TABLE \
                 {changes} MODIFY COLUMN `{}` {want}` (not possible for a key column). \
                 Otherwise: {restart}",
                s.column_name, s.column_name,
            )
        })
    })
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

/// The row count a Parquet file's footer declares.
fn parquet_rows(file: &[u8]) -> Result<u64> {
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
    let meta = ParquetMetaDataReader::decode_metadata(&file[start..file.len() - TAIL])?;
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
    fn a_cdc_log_collapses_by_key_and_an_incremental_log_does_not() {
        let pk = ["id".to_string()];
        let cdc = changelog_shape(true, "t", &pk).unwrap();
        assert_eq!(cdc.engine, "ReplacingMergeTree(__ver)");
        assert_eq!(cdc.order_by, "(`id`)");
        assert_eq!(cdc.not_null, &pk);
        assert!(cdc.version_column.contains("`__ver` UInt256 MATERIALIZED"));
        let inc = changelog_shape(false, "t", &pk).unwrap();
        assert_eq!(
            (inc.engine, inc.order_by.as_str()),
            ("MergeTree", "tuple()")
        );
        assert!(inc.not_null.is_empty() && inc.version_column.is_empty());
        let err = changelog_shape(true, "t", &[])
            .err()
            .expect("no key refuses");
        assert!(err.to_string().contains("needs a primary key"), "{err}");
    }

    #[test]
    fn the_log_puts_rivets_columns_first_and_never_twice() {
        let names: Vec<String> = changelog_specs(&[spec("__op", "String"), spec("id", "Int64")])
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
        let cdc = changelog_shape(true, "t", &pk).unwrap();
        let inc = changelog_shape(false, "t", &pk).unwrap();
        let specs = changelog_specs(&[spec("id", "Int64"), spec("v", "Decimal(10,2)")]);
        let cols = [
            ("id", "Int64"),
            ("v", "Nullable(Decimal(10, 2))"),
            ("__op", "Nullable(String)"),
        ];
        let inc_cols = [("id", "Nullable(Int64)"), ("v", "Nullable(Decimal(10, 2))")];
        let conflict = |shape: &ChangelogShape<'_>,
                        key: &[String],
                        existing: (&str, &str),
                        cols: &[(&str, &str)]| {
            changelog_conflict("d.t__changes", "d.t", shape, key, existing, cols, &specs)
        };
        assert_eq!(
            conflict(&cdc, &pk, ("ReplacingMergeTree", "id"), &cols),
            None,
            "a matching log"
        );
        assert_eq!(
            conflict(&inc, &pk, ("MergeTree", ""), &inc_cols),
            None,
            "a matching incremental log"
        );

        let mode =
            conflict(&inc, &pk, ("ReplacingMergeTree", "id"), &cols).expect("cdc -> incremental");
        assert!(
            mode.contains("is a ReplacingMergeTree") && mode.contains("Nothing was written"),
            "{mode}"
        );
        assert!(
            conflict(&cdc, &pk, ("MergeTree", ""), &cols).is_some(),
            "incremental -> cdc"
        );

        let wider = ["id".to_string(), "tenant".to_string()];
        let cdc2 = changelog_shape(true, "t", &wider).unwrap();
        let key = conflict(&cdc2, &wider, ("ReplacingMergeTree", "id"), &cols).expect("pk changed");
        assert!(
            key.contains("collapses versions by (id)") && key.contains("(id, tenant)"),
            "{key}"
        );
        assert_eq!(
            conflict(&cdc2, &wider, ("ReplacingMergeTree", "id, `tenant`"), &cols),
            None,
            "the catalog's spelling of the same key"
        );

        let narrow = [("id", "Int64"), ("v", "Nullable(Decimal(9, 2))")];
        let ty =
            conflict(&cdc, &pk, ("ReplacingMergeTree", "id"), &narrow).expect("a type changed");
        assert!(
            ty.contains("column `v`") && ty.contains("MODIFY COLUMN"),
            "{ty}"
        );
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
            &[spec("id", "Int64"), spec("v", "String")],
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
        assert_eq!(parquet_rows(&buf).unwrap(), 6);
        assert!(
            parquet_rows(&buf[..4]).is_err(),
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

    /// Through a named collection ClickHouse reads the part itself: columns match by
    /// name whatever their order, the count is the insert's own, and a NULL key refuses.
    #[test]
    #[ignore = "live: requires docker compose clickhouse (with dev/clickhouse/named_collections.xml) + minio"]
    fn a_pulled_part_lands_by_column_name_and_a_null_key_refuses() {
        unsafe { std::env::set_var("RIVET_CH_PULL_TEST_PASSWORD", "rivet") };
        let db = format!("rivet_chpull_{}", std::process::id());
        let loader = ClickhouseLoader::new(
            "http://127.0.0.1:8123",
            &db,
            "rivet",
            "RIVET_CH_PULL_TEST_PASSWORD",
            crate::config::DestinationConfig::default(),
        )
        .cdc(true)
        .named_collection(Some("rivet_stand_minio".into()));
        let _ = reqwest::blocking::Client::new()
            .put("http://127.0.0.1:9000/rivet-qa-ch-pull")
            .basic_auth("minioadmin", Some("minioadmin"))
            .send();
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
        let specs = [spec("id", "Int64"), spec("v", "String")];
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
    fn a_pull_reads_the_part_through_the_collection_by_name() {
        assert_eq!(
            pull_insert_sql("`d`.`t`", "gcs_raw", "b", "p/part'1.parquet"),
            "INSERT INTO `d`.`t` SELECT * FROM gcs(gcs_raw, filename = 'b/p/part\\'1.parquet', \
             format = 'Parquet')"
        );
    }

    #[test]
    fn a_literal_cannot_break_out_of_its_quotes() {
        assert_eq!(literal("a'b\\c"), r"'a\'b\\c'");
    }
}
