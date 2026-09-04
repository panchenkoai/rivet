//! ClickHouse loader.
//!
//! Loads Rivet Parquet from GCS into a native-typed ClickHouse table over the
//! HTTP interface (default port 8123, `https://host:8443` for the secure one).
//! ClickHouse has no `LOAD DATA` analogue and no external stage — so this
//! adapter, unlike BigQuery/Snowflake, cannot point the warehouse at the staged
//! Parquet. Instead it downloads each driver-selected object through the load
//! layer's [`GcsStore`] and streams the bytes straight to ClickHouse as an
//! `INSERT INTO <table> FORMAT Parquet` (the Parquet input format maps columns
//! BY NAME, so the file's column order never matters).
//!
//! Row counting for the driver's count-integrity gate: ClickHouse's HTTP
//! interface returns no row count for an `INSERT`, so the adapter reads
//! `SELECT count()` after an overwrite / before-and-after an append — the same
//! metadata-only COUNT seam the other adapters use.
//!
//! Transport is plain HTTP + basic auth (the CH native interface): no new
//! dependency (reqwest is already in the tree). Inserted data rides the URL's
//! `query` parameter with the bytes as the POST body — the one convention that
//! switches the body to INSERT data on 24.8 (the `X-ClickHouse-Query` header
//! does not; verified live).

use super::GcsStore;
use super::TargetLoader;
use crate::types::target::TargetColumnSpec;
use anyhow::{Context, Result, bail};
use reqwest::header::CONTENT_TYPE;
use std::time::Duration;

/// HTTP timeout for a single ClickHouse call. The INSERT of a budget-sized
/// Parquet object over localhost is seconds; over a WAN, tens of seconds.
/// 20 minutes is a generous ceiling that still fails a hung server loud.
const HTTP_TIMEOUT: Duration = Duration::from_secs(20 * 60);

/// Loads Rivet Parquet from GCS into ClickHouse over its HTTP interface.
#[derive(Debug, Clone)]
pub struct ClickhouseLoader {
    /// ClickHouse HTTP endpoint — `http://localhost:8123` for the native HTTP
    /// port, `https://host:8443` for the secure one.
    pub url: String,
    /// Database to load into. The loader never creates databases — it must
    /// exist (the loader is run as a single SQL user, not an admin).
    pub database: String,
    /// HTTP basic-auth user (ClickHouse's default superuser is `default`).
    pub user: String,
    /// Name of an env var holding the password — secrets stay out of config.
    pub password_env: String,
    /// The load layer's GCS store — downloads the driver-selected Parquet
    /// objects for the HTTP `INSERT`. Injected (not built here) so a test /
    /// fs-backed store exercises the whole path offline. `pub(crate)` — the
    /// store itself is crate-private.
    pub(crate) store: GcsStore,
    /// MergeTree sort key — the analog of BigQuery `CLUSTER BY` / the CDC
    /// log's PK clustering. Applied only at table creation. Empty = `tuple()`
    /// (insertion order, no re-sort).
    pub cluster_by: Vec<String>,
}

impl ClickhouseLoader {
    /// `password_env` is resolved per-request (a load may run hours after the
    /// process starts; re-reading the env keeps the secret out of the struct).
    pub(crate) fn new(
        url: impl Into<String>,
        database: impl Into<String>,
        user: impl Into<String>,
        password_env: impl Into<String>,
        store: GcsStore,
    ) -> Self {
        Self {
            url: url.into(),
            database: database.into(),
            user: user.into(),
            password_env: password_env.into(),
            store,
            cluster_by: Vec::new(),
        }
    }

    fn password(&self) -> Result<String> {
        std::env::var(&self.password_env).with_context(|| {
            format!(
                "ClickHouse load: `password_env` names env var `{}`, which is not set",
                self.password_env
            )
        })
    }

    /// The plain (unquoted) `db.table` — what the driver reads back in reports
    /// and re-quotes per warehouse for the dedup view.
    fn fqtn(&self, table: &str) -> String {
        format!("{}.{}", self.database, table)
    }

    /// The `db.table` quoted per ClickHouse (each dot-separated segment
    /// back-ticked — a single `` `db.table` `` would parse as ONE identifier).
    fn quoted(&self, table: &str) -> String {
        crate::load::cdc::Warehouse::ClickHouse.quote_fqtn(&self.fqtn(table))
    }

    /// Run `sql` via ClickHouse's HTTP interface, returning the response body.
    /// A POST whose body is the query (no `query` URL param) is the CH-native
    /// way to send statements too long for a URL.
    fn query(&self, sql: &str) -> Result<String> {
        let pass = self.password()?;
        let resp = reqwest::blocking::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .context("building the ClickHouse HTTP client")?
            .post(self.url.trim_end_matches('/'))
            .basic_auth(&self.user, Some(&pass))
            .header(CONTENT_TYPE, "text/plain")
            .body(sql.to_string())
            .send()
            .with_context(|| format!("ClickHouse HTTP request failed ({})", self.url))?;
        let status = resp.status();
        let text = resp
            .text()
            .with_context(|| format!("reading the ClickHouse HTTP response (HTTP {status})"))?;
        if !status.is_success() {
            bail!(
                "ClickHouse query failed (HTTP {status}): {}",
                trim_ch_error(&text)
            );
        }
        Ok(text.trim().to_string())
    }

    /// Stream `data` (raw Parquet) as an `INSERT`. The query rides the URL's
    /// `query` parameter (reqwest percent-encodes it); the request body is then
    /// the DATA, per the CH HTTP interface convention. (The documented
    /// `X-ClickHouse-Query` header was tried first — ClickHouse 24.8 ignores it
    /// for INSERT-with-data and parses the body as SQL; the URL parameter is the
    /// convention that actually switches the body to data. Verified live.)
    fn insert(&self, sql: &str, data: &[u8]) -> Result<()> {
        let pass = self.password()?;
        let resp = reqwest::blocking::Client::builder()
            .timeout(HTTP_TIMEOUT)
            .build()
            .context("building the ClickHouse HTTP client")?
            .post(self.url.trim_end_matches('/'))
            .basic_auth(&self.user, Some(&pass))
            .query(&[("query", sql)])
            .body(data.to_vec())
            .send()
            .with_context(|| format!("ClickHouse HTTP request failed ({})", self.url))?;
        let status = resp.status();
        let text = resp
            .text()
            .with_context(|| format!("reading the ClickHouse HTTP response (HTTP {status})"))?;
        if !status.is_success() {
            bail!(
                "ClickHouse INSERT failed (HTTP {status}): {}",
                trim_ch_error(&text)
            );
        }
        Ok(())
    }

    /// Download each driver-selected `gs://…` object and INSERT it into
    /// `target` (a quoted `db`.`table`). Each object is its own HTTP insert —
    /// an at-least-once re-presentation reproduces the same rows, and a partial
    /// failure fails the driver's count gate (overwrite cleans up next retry,
    /// append is absorbed by the dedup view).
    fn insert_uris(&self, target: &str, uris: &[String]) -> Result<()> {
        for uri in uris {
            let (_, key) = super::split_gs_uri(uri)?;
            let bytes = self
                .store
                .read(key)
                .with_context(|| format!("downloading {uri} for the ClickHouse load"))?;
            self.insert(&format!("INSERT INTO {target} FORMAT Parquet"), &bytes)
                .with_context(|| format!("ClickHouse INSERT of {uri} into {target} failed"))?;
        }
        Ok(())
    }

    /// `SELECT count()` — the metadata-only COUNT seam the other adapters use
    /// for the driver's row-integrity gate.
    fn count(&self, target: &str) -> Result<u64> {
        let body = self.query(&format!(
            "SELECT count() AS n FROM {target} FORMAT JSONEachRow"
        ))?;
        extract_count(&body)
            .with_context(|| format!("reading the row count of {target} from ClickHouse"))
    }
}

impl TargetLoader for ClickhouseLoader {
    fn fqtn(&self, table: &str) -> String {
        self.fqtn(table)
    }

    fn materialize(&self, table: &str, specs: &[TargetColumnSpec], uris: &[String]) -> Result<u64> {
        // cluster_by splices into the MergeTree `ORDER BY` (an identifier list,
        // no quoting) — the same is_safe_load_ident gate BigQuery applies to its
        // CLUSTER BY. Config-derived (operator self-harm), but gated for
        // consistency with the round-5/6 injection surface.
        for c in &self.cluster_by {
            if !super::is_safe_load_ident(c) {
                bail!(
                    "ClickHouse load: clustering column `{}` is not a plain SQL identifier \
                     ([A-Za-z_][A-Za-z0-9_]*) — it splices into the MergeTree ORDER BY. Rename it.",
                    c.escape_default()
                );
            }
        }
        let target = self.quoted(table);
        // Overwrite: storage is the source of truth (the other loaders' `LOAD
        // DATA OVERWRITE` / `CREATE OR REPLACE TABLE`). CREATE OR REPLACE is
        // idempotent under retry — re-presenting the same Parquet reproduces
        // the same table.
        let create = create_table_sql(
            &target,
            &build_schema_ddl(specs),
            &order_clause(&self.cluster_by),
            true,
        );
        self.query(&create)
            .with_context(|| format!("creating the ClickHouse table for `{table}`"))?;
        self.insert_uris(&target, uris)?;
        self.count(&target)
    }

    fn append_changelog(
        &self,
        table: &str,
        specs: &[TargetColumnSpec],
        uris: &[String],
        pk: &[String],
    ) -> Result<u64> {
        use crate::load::cdc::Warehouse;
        // Full change-log schema: rivet's `__op`/`__pos`/`__seq` meta columns
        // (not reported by `rivet check`) ahead of the resolved data columns.
        let mut full = crate::load::cdc::meta_column_specs(Warehouse::ClickHouse);
        full.extend(
            specs
                .iter()
                .filter(|s| !is_meta_column(&s.column_name))
                .cloned(),
        );

        let changes = format!("{table}__changes");
        let changes_q = self.quoted(&changes);
        // Clustered on the PK so the dedup view prunes efficiently.
        let create = create_table_sql(
            &changes_q,
            &build_schema_ddl(&full),
            &order_clause(pk),
            false,
        );
        self.query(&create)
            .with_context(|| format!("creating the change log for `{table}`"))?;

        // A log that ALREADY existed (rivet did not create it, or one that
        // predates a new column) is reconciled by ADDING what the declared
        // schema has — never replaced: the table may hold the customer's
        // history. `CREATE TABLE IF NOT EXISTS` is a no-op on such a table, so
        // without this the INSERT below names columns the table lacks and fails
        // the load after the extract was paid for.
        let alter = build_alter_add_columns_sql(&changes_q, &full);
        if !alter.is_empty() {
            self.query(&alter)
                .with_context(|| format!("reconciling the change log for `{table}`"))?;
        }

        // Count before / append / count after — the delta is what THIS load
        // added; the driver gates it against the manifest total.
        let before = self.count(&changes_q)?;
        self.insert_uris(&changes_q, uris)?;
        let after = self.count(&changes_q)?;
        Ok(after.saturating_sub(before))
    }

    fn warehouse(&self) -> crate::load::cdc::Warehouse {
        crate::load::cdc::Warehouse::ClickHouse
    }

    fn create_view(&self, _table: &str, view_sql: &str) -> Result<()> {
        self.query(view_sql)
            .with_context(|| "creating the ClickHouse dedup view")?;
        Ok(())
    }
}

/// Whether a column name is one of rivet's CDC meta columns — filtered out of
/// the data specs before the meta columns are prepended, so a schema can never
/// declare `__op`/`__pos`/`__seq` twice.
fn is_meta_column(name: &str) -> bool {
    crate::load::cdc::is_meta_column(name)
}

/// `CREATE OR REPLACE TABLE` / `CREATE TABLE IF NOT EXISTS` with the MergeTree
/// engine — the loaders create their own tables (BigQuery/Snowflake do the
/// same). `replace` is the full-snapshot overwrite verb; the append path uses
/// the idempotent IF NOT EXISTS so a pre-existing log is never dropped.
///
/// The loader declares every column `Nullable(…)` (see [`build_schema_ddl`]), so
/// a column-bearing sort key (`ORDER BY (id)` on the change log) is a NULLABLE
/// key — which MergeTree rejects by default (`allow_nullable_key=0`, caught
/// live on 24.8). The table-level `SETTINGS allow_nullable_key = 1` permits it
/// (the key columns are non-NULL in practice — a PK / cluster column). An
/// `ORDER BY tuple()` key has no columns and needs no setting.
fn create_table_sql(fqtn: &str, ddl: &str, order: &str, replace: bool) -> String {
    let kw = if replace {
        "CREATE OR REPLACE TABLE"
    } else {
        "CREATE TABLE IF NOT EXISTS"
    };
    let settings = if order == "tuple()" {
        ""
    } else {
        " SETTINGS allow_nullable_key = 1"
    };
    format!("{kw} {fqtn} (\n{ddl}\n) ENGINE = MergeTree ORDER BY {order}{settings}")
}

/// `  `id` Nullable(Int64),\n  `tags` Array(Nullable(String))` — the native
/// column DDL. Rivet's resolver emits the NON-nullable native type
/// (`Int64`, `DateTime64(6, 'UTC')`, …), but the exported Parquet fields are
/// all `Nullable(T)` — so the loader declares `Nullable(…)` to preserve NULLs
/// (the cloud warehouses' columns are nullable too; a non-Nullable CH column
/// would silently coerce NULLs to defaults). `Array(Nullable(…))` already
/// carries its own inner Nullable and is NOT wrapped again, matching the
/// Parquet autoload shape exactly.
fn build_schema_ddl(specs: &[TargetColumnSpec]) -> String {
    specs
        .iter()
        .map(|s| format!("  `{}` {}", s.column_name, ddl_type(s)))
        .collect::<Vec<_>>()
        .join(",\n")
}

/// The physical ClickHouse column type for a resolved spec (see
/// [`build_schema_ddl`] for the Nullable rationale).
fn ddl_type(spec: &TargetColumnSpec) -> String {
    let t = &spec.target_type;
    if t.starts_with("Array(") || t.starts_with("LowCardinality(") {
        t.clone()
    } else {
        format!("Nullable({t})")
    }
}

/// ` (a, b)` / `tuple()` — the MergeTree sort key (the analog of BigQuery's
/// CLUSTER BY), or insertion order when unset.
fn order_clause(cluster_by: &[String]) -> String {
    if cluster_by.is_empty() {
        "tuple()".to_string()
    } else {
        format!("({})", cluster_by.join(", "))
    }
}

/// Bring an EXISTING change log's schema up to the declared one by ADDING what
/// is missing — never by replacing the table, which would impose rivet's schema
/// on history rivet does not own. `ADD COLUMN IF NOT EXISTS` is the only safe
/// verb: additive, idempotent, metadata-only. Trailing newline-free (the caller
/// checks `is_empty`) so an empty spec list yields no statement at all.
fn build_alter_add_columns_sql(fqtn: &str, specs: &[TargetColumnSpec]) -> String {
    if specs.is_empty() {
        return String::new();
    }
    let adds = specs
        .iter()
        .map(|s| {
            format!(
                "ADD COLUMN IF NOT EXISTS `{}` {}",
                s.column_name,
                ddl_type(s)
            )
        })
        .collect::<Vec<_>>()
        .join(",\n  ");
    format!("ALTER TABLE {fqtn}\n  {adds}")
}

/// Pull the `n` column out of a `FORMAT JSONEachRow` body. ClickHouse serializes
/// 64-bit integers as JSON *strings* by default (`output_format_json_quote_64bit_integers`),
/// so both the number and string forms are accepted.
fn extract_count(body: &str) -> Result<u64> {
    let v: serde_json::Value = serde_json::from_str(body)
        .with_context(|| format!("parsing the ClickHouse count response: {body}"))?;
    let n = v
        .get("n")
        .with_context(|| format!("ClickHouse count response lacks `n`: {body}"))?;
    n.as_u64()
        .or_else(|| n.as_str().and_then(|s| s.parse().ok()))
        .with_context(|| format!("reading the count from the ClickHouse response: {body}"))
}

/// ClickHouse errors carry a `Code: N. DB::Exception: …` head and can trail a
/// long stack trace; keep the head so logs stay readable.
fn trim_ch_error(text: &str) -> String {
    let head = text.lines().take(6).collect::<Vec<_>>().join("\n");
    head.chars().take(4000).collect()
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
    fn schema_ddl_uses_nullable_scalars_and_backticked_names() {
        let specs = [
            spec("id", "Int64"),
            spec("amount", "Decimal(18, 2)"),
            spec("tags", "Array(Nullable(String))"),
            spec("at", "DateTime64(6, 'UTC')"),
        ];
        let ddl = build_schema_ddl(&specs);
        assert_eq!(
            ddl,
            "  `id` Nullable(Int64),\n  `amount` Nullable(Decimal(18, 2)),\n  \
             `tags` Array(Nullable(String)),\n  `at` Nullable(DateTime64(6, 'UTC'))"
        );
    }

    #[test]
    fn order_clause_is_tuple_or_the_cluster_columns() {
        assert_eq!(order_clause(&[]), "tuple()");
        assert_eq!(
            order_clause(&["created".to_string(), "customer".to_string()]),
            "(created, customer)"
        );
    }

    #[test]
    fn create_table_sql_spells_replace_verb_per_mode() {
        let replace = create_table_sql("`db`.`t`", "  `id` Nullable(Int64)", "tuple()", true);
        assert!(
            replace.starts_with("CREATE OR REPLACE TABLE `db`.`t`"),
            "{replace}"
        );
        assert!(replace.ends_with("ENGINE = MergeTree ORDER BY tuple()"));
        let if_not = create_table_sql("`db`.`t__changes`", "  `id` Nullable(Int64)", "(id)", false);
        assert!(
            if_not.starts_with("CREATE TABLE IF NOT EXISTS `db`.`t__changes`"),
            "{if_not}"
        );
        assert!(
            if_not.contains("ENGINE = MergeTree ORDER BY (id)"),
            "order clause must precede the table setting: {if_not}"
        );
        // A column-bearing (Nullable) sort key needs the table setting MergeTree
        // otherwise refuses (caught live on 24.8).
        assert!(
            if_not.contains("SETTINGS allow_nullable_key = 1"),
            "nullable sort key must carry the table setting: {if_not}"
        );
        assert!(
            !replace.contains("SETTINGS"),
            "tuple() sort key needs no setting: {replace}"
        );
    }

    #[test]
    fn alter_add_columns_reconciles_an_existing_log_and_never_replaces_it() {
        let specs = vec![
            spec("__op", "String"),
            spec("id", "Int64"),
            spec(crate::enrich::COL_ROW_HASH, "String"),
        ];
        let sql = build_alter_add_columns_sql("`db`.`t__changes`", &specs);
        assert!(
            sql.starts_with("ALTER TABLE `db`.`t__changes`"),
            "must ALTER the log in place; got: {sql}"
        );
        assert!(
            sql.contains("ADD COLUMN IF NOT EXISTS `__op` Nullable(String)")
                && sql.contains("ADD COLUMN IF NOT EXISTS `id` Nullable(Int64)"),
            "every declared column is added idempotently; got: {sql}"
        );
        assert!(
            !sql.to_uppercase().contains("REPLACE") && !sql.to_uppercase().contains("DROP"),
            "reconciliation must never replace or drop — the log holds history \
             rivet does not own; got: {sql}"
        );
        assert_eq!(build_alter_add_columns_sql("`db`.`t`", &[]), "");
    }

    #[test]
    fn fqtn_is_db_table_and_quoted_backticks_each_segment() {
        let l = ClickhouseLoader {
            url: "http://localhost:8123".into(),
            database: "rivet".into(),
            user: "rivet".into(),
            password_env: "RIVET_CLICKHOUSE_PASSWORD".into(),
            store: crate::load::GcsStore::open_fs("/tmp/opencode").unwrap(),
            cluster_by: Vec::new(),
        };
        assert_eq!(l.fqtn("orders"), "rivet.orders");
        assert_eq!(l.quoted("orders"), "`rivet`.`orders`");
        assert_eq!(l.quoted("public_orders"), "`rivet`.`public_orders`");
    }

    #[test]
    fn count_is_extracted_from_json_each_row_both_number_and_string() {
        assert_eq!(extract_count("{\"n\":42}").unwrap(), 42);
        assert_eq!(
            extract_count("{\"n\":\"18446744073709551615\"}").unwrap(),
            u64::MAX
        );
        assert!(extract_count("{\"m\":1}").is_err());
        assert!(extract_count("not json").is_err());
    }

    #[test]
    fn is_meta_column_matches_only_cdc_meta() {
        assert!(is_meta_column("__op"));
        assert!(is_meta_column("__pos"));
        assert!(is_meta_column("__seq"));
        assert!(!is_meta_column("id"));
        assert!(!is_meta_column("__other"));
    }
}
