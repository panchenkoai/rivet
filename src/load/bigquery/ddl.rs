//! The SQL the BigQuery loader runs: `LOAD DATA`, the change-log DDL, the adoption and
//! rebuild scripts, and the catalog probes.

use super::*;

/// `CREATE TABLE IF NOT EXISTS` for the change log, partitioned as the load declares and
/// clustered on `cluster_by` (capped at BigQuery's 4 clustering columns; none when empty).
/// Idempotent — the log is created once and appended to on every CDC load.
pub(super) fn build_create_changes_sql(
    fqtn: &str,
    schema: &str,
    partition: Option<&TablePartition>,
    cluster_by: &[String],
) -> String {
    let cluster: Vec<String> = cluster_by
        .iter()
        .take(MAX_CLUSTER_COLUMNS)
        .cloned()
        .collect();
    let options = partition.and_then(changelog_options_sql);
    format!(
        "CREATE TABLE IF NOT EXISTS `{fqtn}` (\n{schema}\n){};",
        table_shape_clauses(
            partition.map(|p| p.expr.as_str()),
            &cluster,
            options.as_deref()
        )
    )
}

/// The billed copy of the change log into `rebuild_fqtn`, shaped as the load declares and
/// carrying the table properties a `CREATE TABLE … AS SELECT` would otherwise drop.
pub(super) fn build_rebuild_copy_sql(
    rebuild_fqtn: &str,
    changes_fqtn: &str,
    partition: Option<&TablePartition>,
    cluster_by: &[String],
    props: &TableProps,
) -> String {
    let mut options: Vec<String> = partition
        .and_then(changelog_options_sql)
        .into_iter()
        .collect();
    options.extend(table_props_options(props));
    let options = (!options.is_empty()).then(|| options.join(", "));
    format!(
        "CREATE TABLE `{rebuild_fqtn}`{}\nAS SELECT * FROM `{changes_fqtn}`;",
        table_shape_clauses(
            partition.map(|p| p.expr.as_str()),
            cluster_by,
            options.as_deref()
        )
    )
}

/// A double-quoted SQL string literal.
pub(super) fn sql_string(s: &str) -> String {
    format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
}

/// Swap the rebuilt log in: the old one steps aside, the copy takes the name, the old one
/// is dropped. An interruption leaves `<name>__old` / `<name>__rebuild`, which the next
/// load refuses to proceed past (`rebuild_leftovers`).
pub(super) fn build_rebuild_swap_sql(
    changes_fqtn: &str,
    rebuild_fqtn: &str,
    old_fqtn: &str,
    changes_name: &str,
    old_name: &str,
) -> Vec<String> {
    vec![
        format!("ALTER TABLE `{changes_fqtn}` RENAME TO {old_name};"),
        format!("ALTER TABLE `{rebuild_fqtn}` RENAME TO {changes_name};"),
        format!("DROP TABLE `{old_fqtn}`;"),
    ]
}

/// Probe whose scalar has bit `i` set when `names[i]` exists in the dataset.
pub(super) fn build_leftovers_sql(project: &str, dataset: &str, names: &[String]) -> String {
    let terms = names
        .iter()
        .enumerate()
        .map(|(i, n)| format!("{} * COUNTIF(table_name = '{n}')", 1u64 << i))
        .collect::<Vec<_>>()
        .join(" + ");
    format!("SELECT {terms} AS n FROM `{project}.{dataset}`.INFORMATION_SCHEMA.TABLES")
}

/// The names whose bit is set in the leftovers probe's scalar.
pub(super) fn leftover_names(code: u64, names: &[String]) -> Vec<String> {
    names
        .iter()
        .enumerate()
        .filter(|(i, _)| code & (1 << i) != 0)
        .map(|(_, n)| n.clone())
        .collect()
}

/// Probe whose scalar is `1·table + 2·view + 4·other` for `table` in the dataset.
pub(super) fn build_object_kind_sql(project: &str, dataset: &str, table: &str) -> String {
    format!(
        "SELECT COUNTIF(table_type = 'BASE TABLE') + 2 * COUNTIF(table_type = 'VIEW') \
         + 4 * COUNTIF(table_type NOT IN ('BASE TABLE', 'VIEW')) AS n \
         FROM `{project}.{dataset}`.INFORMATION_SCHEMA.TABLES WHERE table_name = '{table}'"
    )
}

/// Probes counting all columns of `table` and those among `names`.
pub(super) fn build_column_overlap_sql(
    project: &str,
    dataset: &str,
    table: &str,
    names: &[&str],
) -> (String, String) {
    let from = format!(
        "FROM `{project}.{dataset}`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{table}'"
    );
    let list = if names.is_empty() {
        "''".to_string()
    } else {
        names
            .iter()
            .map(|n| format!("'{}'", n.to_lowercase()))
            .collect::<Vec<_>>()
            .join(", ")
    };
    (
        format!("SELECT COUNT(*) AS n {from}"),
        format!("SELECT COUNT(*) AS n {from} AND LOWER(column_name) IN ({list})"),
    )
}

/// Turn the full-load table into `<table>__changes` without a query: a rename keeps its
/// rows, partitioning, clustering and options, then the meta columns are added (NULL on
/// every existing row). A partition filter requirement is dropped, since the current-state
/// view reads the whole log, and so is expiry of load-date partitions.
pub(super) fn build_adoption_sql(
    src_fqtn: &str,
    table: &str,
    changes_fqtn: &str,
    shape: &TableShape,
) -> Vec<String> {
    let meta = crate::load::cdc::meta_column_specs(crate::load::cdc::Warehouse::BigQuery);
    let mut out = vec![format!(
        "ALTER TABLE `{src_fqtn}` RENAME TO {table}__changes;"
    )];
    out.extend(build_alter_add_columns_sql(changes_fqtn, &meta));
    if shape.require_partition_filter {
        out.push(format!(
            "ALTER TABLE `{changes_fqtn}` SET OPTIONS(require_partition_filter = false);"
        ));
    }
    if shape.expires_load_dates() {
        out.push(format!(
            "ALTER TABLE `{changes_fqtn}` SET OPTIONS(partition_expiration_days = NULL);"
        ));
    }
    out
}

/// Bring an EXISTING table's schema up to the declared one by ADDING what is
/// missing — never by replacing the table.
///
/// `CREATE TABLE IF NOT EXISTS` is a no-op on a table that already exists, so a
/// table rivet did not create — one an operator pointed rivet at — keeps
/// whatever shape its previous owner gave it. The next `LOAD DATA` then declares
/// columns the table does not have and fails; and a load written to overwrite
/// instead would impose our schema and destroy the customer's data. Neither is
/// acceptable on a table we were handed rather than created.
///
/// `ADD COLUMN IF NOT EXISTS` is the only verb that is safe here: additive,
/// idempotent, and metadata-only on BigQuery — no rewrite, no scan, and existing
/// rows read NULL for the new column, which is exactly the state §5i's per-key
/// fallback is built to handle.
///
/// `None` when there is nothing to add, so the caller skips the round trip
/// rather than sending a statement with an empty body.
pub(super) fn build_alter_add_columns_sql(
    fqtn: &str,
    specs: &[TargetColumnSpec],
) -> Option<String> {
    if specs.is_empty() {
        return None;
    }
    let adds = specs
        .iter()
        .map(|s| {
            format!(
                "ADD COLUMN IF NOT EXISTS `{}` {}",
                s.column_name, s.target_type
            )
        })
        .collect::<Vec<_>>()
        .join(",\n  ");
    Some(format!("ALTER TABLE `{fqtn}`\n  {adds};"))
}

/// `PARTITION BY … / CLUSTER BY … / OPTIONS(…)` clauses (empty when unset). All three
/// apply only at table creation, per BigQuery.
pub(super) fn table_shape_clauses(
    partition_expr: Option<&str>,
    cluster_by: &[String],
    options: Option<&str>,
) -> String {
    let mut s = String::new();
    if let Some(expr) = partition_expr {
        s.push_str(&format!("\nPARTITION BY {expr}"));
    }
    if !cluster_by.is_empty() {
        let quoted: Vec<String> = cluster_by.iter().map(|c| format!("`{c}`")).collect();
        s.push_str(&format!("\nCLUSTER BY {}", quoted.join(", ")));
    }
    if let Some(opts) = options {
        s.push_str(&format!("\nOPTIONS({opts})"));
    }
    s
}

/// Replace `target` with a zero-copy clone of `staging` — the atomic hand-off of a
/// whole-table load that arrived in several jobs.
pub(super) fn build_clone_sql(target: &str, staging: &str) -> String {
    format!("CREATE OR REPLACE TABLE `{target}` CLONE `{staging}`;")
}

/// A `FROM FILES(...)` Parquet source list.
///
/// `enable_list_inference = true` collapses rivet's 3-level Parquet LIST
/// (`col.list.item`) one level, so an array column loads as the declared
/// `ARRAY<STRUCT<item T>>` (== REPEATED RECORD{item}) instead of empty. It is a
/// no-op for non-list columns, so it is always safe to set.
pub(super) fn from_files(uris: &[String]) -> String {
    let list = uris
        .iter()
        .map(|u| format!("    '{u}'"))
        .collect::<Vec<_>>()
        .join(",\n");
    format!(
        "FROM FILES (\n  format = 'PARQUET',\n  enable_list_inference = true,\n  uris = [\n{list}\n  ]\n)"
    )
}

/// The BigQuery column schema declared inline in LOAD DATA, from each spec's
/// native `target_type`. Declaring native types makes BigQuery coerce the
/// Parquet on load — for FREE (a load job, not a query) — so JSON / DATETIME /
/// TIME / NUMERIC / … land natively without a post-load CTAS. Verified live.
pub(super) fn build_schema(specs: &[TargetColumnSpec]) -> String {
    // Backticked, like build_alter_add_columns_sql always was: names are
    // pre-gated to plain idents, so quoting is always safe — and without it a
    // reserved-word column (`end`, `order`, `interval`; `start`/`end` pairs
    // are everywhere) died on BigQuery's raw syntax error AFTER the extract
    // was paid, while cdc.rs promised backticks made it safe (round-6).
    specs
        .iter()
        .map(|s| format!("  `{}` {}", s.column_name, s.target_type))
        .collect::<Vec<_>>()
        .join(",\n")
}

/// A free `LOAD DATA` batch-load statement declaring the native `schema`, so
/// BigQuery coerces the Parquet to native types on load.
pub(super) fn build_load_data_sql(
    fqtn: &str,
    overwrite: bool,
    schema: &str,
    partition_expr: Option<&str>,
    cluster_by: &[String],
    options: Option<&str>,
    uris: &[String],
) -> String {
    let kw = if overwrite { "OVERWRITE" } else { "INTO" };
    let clauses = table_shape_clauses(partition_expr, cluster_by, options);
    format!(
        "LOAD DATA {kw} `{fqtn}` (\n{schema}\n){clauses}\n{};",
        from_files(uris)
    )
}
