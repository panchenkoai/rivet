//! The shape of a BigQuery table — partitioning, clustering, options and the properties a
//! rebuild carries — as `tables.get` reports it and as the load declares it.

use super::*;

/// Milliseconds in a day, the unit `tables.get` reports partition expiry in.
pub(super) const DAY_MS: u64 = 86_400_000;

/// The partitioning, clustering and partition options of an existing table, from `tables.get`.
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct TableShape {
    pub(super) partition: Option<PartitionKey>,
    pub(super) cluster: Vec<String>,
    pub(super) require_partition_filter: bool,
    pub(super) expiration_ms: Option<u64>,
    /// `numBytes` — what a rebuild would read.
    pub(super) bytes: Option<u64>,
}

/// Whether an adopted log keeps the clustering its table had: `cluster_by` differs but was
/// not written in the config.
pub(super) fn keeps_own_clustering(
    shape: &TableShape,
    cluster_by: &[String],
    declared: bool,
) -> bool {
    !declared && !same_columns(&shape.cluster, cluster_by)
}

/// The clustering a whole-table load writes: what the config wrote, else — for a table
/// that already exists — the clustering it has (an unwritten `cluster_by` follows the
/// table; `LOAD DATA OVERWRITE` must repeat an existing table's clustering exactly).
pub(super) fn table_clustering<'a>(
    clustering: &'a Clustering,
    existing: Option<&'a TableShape>,
) -> &'a [String] {
    match (clustering, existing) {
        (Clustering::Auto(_), Some(shape)) => &shape.cluster,
        (c, _) => c.columns(),
    }
}

/// Refuse a clustering list BigQuery would reject or that is not a plain identifier: each
/// column splices raw into `CLUSTER BY <cols>`, the same gate the table, column and pk
/// names get.
pub(super) fn check_cluster_columns(cluster_by: &[String]) -> Result<()> {
    if cluster_by.len() > MAX_CLUSTER_COLUMNS {
        bail!(
            "BigQuery allows at most {MAX_CLUSTER_COLUMNS} clustering columns, got {}",
            cluster_by.len()
        );
    }
    for c in cluster_by {
        if !crate::load::is_safe_load_ident(c) {
            bail!(
                "BigQuery load: clustering column `{}` is not a plain SQL identifier \
                 ([A-Za-z_][A-Za-z0-9_]*) — it splices into CLUSTER BY. Rename it.",
                c.escape_default()
            );
        }
    }
    Ok(())
}

/// Drift of an existing change log from what the load DECLARES: a declared partition that
/// differs comes first (only a rebuild fixes it), then a declared clustering that differs
/// (patched in place). Nothing declared → no drift; the log keeps its own shape.
pub(super) fn classify_drift(
    shape: &TableShape,
    partition: Option<&PartitionKey>,
    cluster_by: Option<&[String]>,
) -> Option<crate::load::ChangelogDrift> {
    if let Some(want) = partition
        && !same_partition(shape.partition.as_ref(), Some(want))
    {
        return Some(crate::load::ChangelogDrift::Partition {
            existing: shape
                .partition
                .as_ref()
                .map_or_else(|| "nothing".to_string(), PartitionKey::describe),
            declared: want.describe(),
            bytes: shape.bytes,
        });
    }
    if let Some(want) = cluster_by
        && !same_columns(&shape.cluster, want)
    {
        return Some(crate::load::ChangelogDrift::Cluster {
            existing: shape.cluster.clone(),
            declared: want.to_vec(),
        });
    }
    None
}

/// The `tables.patch` body setting — or, for no columns, clearing — a table's clustering.
pub(super) fn clustering_patch(cluster_by: &[String]) -> serde_json::Value {
    if cluster_by.is_empty() {
        serde_json::json!({ "clustering": null })
    } else {
        serde_json::json!({ "clustering": { "fields": cluster_by } })
    }
}

/// The properties of a table a rebuild must carry over — `CREATE TABLE … AS SELECT`
/// copies rows and nothing else — and the ones it cannot.
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) struct TableProps {
    pub(super) description: Option<String>,
    pub(super) friendly_name: Option<String>,
    pub(super) labels: Vec<(String, String)>,
    /// `expirationTime` — the whole table's expiry, in Unix milliseconds.
    pub(super) expiration_ms: Option<i64>,
    pub(super) kms_key_name: Option<String>,
    /// Columns carrying policy tags: a copy loses them.
    pub(super) policy_tagged: Vec<String>,
}

/// The carryable properties and policy-tagged columns a `tables.get` resource describes.
pub(super) fn parse_table_props(meta: &serde_json::Value) -> TableProps {
    use serde_json::Value;
    let text = |key: &str| meta.get(key).and_then(Value::as_str).map(String::from);
    let mut labels: Vec<(String, String)> = meta
        .get("labels")
        .and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_string())))
                .collect()
        })
        .unwrap_or_default();
    labels.sort();
    let policy_tagged = meta
        .pointer("/schema/fields")
        .and_then(Value::as_array)
        .map(|fields| {
            fields
                .iter()
                .filter(|f| {
                    f.pointer("/policyTags/names")
                        .and_then(Value::as_array)
                        .is_some_and(|names| !names.is_empty())
                })
                .filter_map(|f| f.get("name").and_then(Value::as_str))
                .map(String::from)
                .collect()
        })
        .unwrap_or_default();
    TableProps {
        description: text("description"),
        friendly_name: text("friendlyName"),
        labels,
        expiration_ms: text("expirationTime").and_then(|s| s.parse().ok()),
        kms_key_name: meta
            .pointer("/encryptionConfiguration/kmsKeyName")
            .and_then(Value::as_str)
            .map(String::from),
        policy_tagged,
    }
}

/// The `OPTIONS(...)` entries that carry a table's properties into its copy.
pub(super) fn table_props_options(props: &TableProps) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(d) = &props.description {
        out.push(format!("description = {}", sql_string(d)));
    }
    if let Some(n) = &props.friendly_name {
        out.push(format!("friendly_name = {}", sql_string(n)));
    }
    if !props.labels.is_empty() {
        let pairs: Vec<String> = props
            .labels
            .iter()
            .map(|(k, v)| format!("({}, {})", sql_string(k), sql_string(v)))
            .collect();
        out.push(format!("labels = [{}]", pairs.join(", ")));
    }
    if let Some(ms) = props.expiration_ms {
        out.push(format!("expiration_timestamp = TIMESTAMP_MILLIS({ms})"));
    }
    if let Some(k) = &props.kms_key_name {
        out.push(format!("kms_key_name = {}", sql_string(k)));
    }
    out
}

/// Why the log cannot be rebuilt by a copy: column policy tags and row access policies
/// do not survive `CREATE TABLE … AS SELECT`, and a log without them is a data exposure.
pub(super) fn rebuild_policy_refusal(
    changes: &str,
    props: &TableProps,
    row_policies: usize,
) -> Option<String> {
    let mut lost = Vec::new();
    if !props.policy_tagged.is_empty() {
        lost.push(format!(
            "policy tags on {}",
            crate::load::column_list(&props.policy_tagged)
        ));
    }
    if row_policies > 0 {
        lost.push(format!("{row_policies} row access policy(ies)"));
    }
    if lost.is_empty() {
        return None;
    }
    Some(format!(
        "`{changes}` carries {}, which a rebuilt copy would not — rebuild it by hand \
         (copy, re-apply the policies, swap) or keep its partitioning; nothing was changed",
        lost.join(" and ")
    ))
}

/// The partition as the change log takes it: never a filter requirement, and an expiry
/// only on a key that is not a load date (expiring load dates would drop unchanged rows).
pub(super) fn changelog_partition(p: &TablePartition) -> TablePartition {
    TablePartition {
        expiration_days: changelog_expiry(p),
        require_filter: false,
        ..p.clone()
    }
}

/// The expiry a change log keeps from a declared partition: none for a load-date key
/// (the load time or `_rivet_exported_at`) or a key that takes none.
pub(super) fn changelog_expiry(p: &TablePartition) -> Option<u32> {
    if p.key.is_load_date() || !p.key.takes_expiry() {
        return None;
    }
    p.expiration_days
}

impl TableShape {
    /// Partitioned by a load date with an expiry.
    pub(super) fn expires_load_dates(&self) -> bool {
        let load_date = self
            .partition
            .as_ref()
            .is_some_and(PartitionKey::is_load_date);
        load_date && self.expiration_ms.is_some()
    }
}

/// The shape a `tables.get` resource describes (`timePartitioning`, `rangePartitioning`,
/// `clustering`, `requirePartitionFilter`).
pub(super) fn parse_table_shape(meta: &serde_json::Value) -> TableShape {
    use serde_json::Value;
    let text = |v: &Value, key: &str| v.get(key).and_then(Value::as_str).map(String::from);
    let int = |v: &Value, key: &str| text(v, key).and_then(|s| s.parse::<i64>().ok());
    let mut shape = TableShape::default();
    if let Some(tp) = meta.get("timePartitioning") {
        let granularity = text(tp, "type")
            .as_deref()
            .and_then(Granularity::parse_sql)
            .unwrap_or(Granularity::Day);
        shape.partition = Some(PartitionKey::Time {
            column: text(tp, "field"),
            granularity,
        });
        shape.expiration_ms = text(tp, "expirationMs").and_then(|s| s.parse().ok());
        shape.require_partition_filter = tp
            .get("requirePartitionFilter")
            .and_then(Value::as_bool)
            .unwrap_or(false);
    }
    if let Some(rp) = meta.get("rangePartitioning")
        && let (Some(column), Some(range)) = (text(rp, "field"), rp.get("range"))
        && let (Some(start), Some(end), Some(interval)) = (
            int(range, "start"),
            int(range, "end"),
            int(range, "interval"),
        )
    {
        shape.partition = Some(PartitionKey::Range {
            column,
            start,
            end,
            interval,
        });
    }
    if let Some(required) = meta.get("requirePartitionFilter").and_then(Value::as_bool) {
        shape.require_partition_filter |= required;
    }
    shape.bytes = text(meta, "numBytes").and_then(|s| s.parse().ok());
    shape.cluster = meta
        .pointer("/clustering/fields")
        .and_then(Value::as_array)
        .map(|fields| {
            fields
                .iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect()
        })
        .unwrap_or_default();
    shape
}

/// Whether two clustering lists name the same columns in the same order.
pub(super) fn same_columns(a: &[String], b: &[String]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.eq_ignore_ascii_case(y))
}

/// Whether an existing table's partition key is the one the load declares.
pub(super) fn same_partition(existing: Option<&PartitionKey>, want: Option<&PartitionKey>) -> bool {
    match (existing, want) {
        (None, None) => true,
        (Some(a), Some(b)) => a.same_as(b),
        _ => false,
    }
}

/// How an existing table's partitioning or clustering differs from what this load declares,
/// or `None`. `cluster_by` is the clustering the config WROTE; `None` (`auto`) follows the
/// table's own.
pub(super) fn shape_conflict(
    shape: &TableShape,
    partition: Option<&PartitionKey>,
    cluster_by: Option<&[String]>,
) -> Option<String> {
    let describe =
        |k: Option<&PartitionKey>, none: &str| k.map_or(none.to_string(), PartitionKey::describe);
    let mut diffs = Vec::new();
    if !same_partition(shape.partition.as_ref(), partition) {
        diffs.push(format!(
            "it is partitioned by {}, the load declares {}",
            describe(shape.partition.as_ref(), "nothing"),
            describe(partition, "no partitioning")
        ));
    }
    if let Some(want) = cluster_by
        && !same_columns(&shape.cluster, want)
    {
        diffs.push(format!(
            "it is clustered on {}, `cluster_by` is {}",
            crate::load::column_list(&shape.cluster),
            crate::load::column_list(want)
        ));
    }
    (!diffs.is_empty()).then(|| diffs.join("; "))
}

/// The `OPTIONS(...)` a load creating the full table declares, or `None` when there is
/// no partition or nothing to set. `creating` is false when the table already exists —
/// BigQuery refuses an overwrite that declares different options, so they go through
/// [`options_drift`] instead.
pub(super) fn creation_options(
    creating: bool,
    partition: Option<&TablePartition>,
) -> Option<String> {
    if !creating {
        return None;
    }
    let p = partition?;
    let mut opts = Vec::new();
    if let Some(days) = p.expiration_days.filter(|_| p.key.takes_expiry()) {
        opts.push(format!("partition_expiration_days = {days}"));
    }
    if p.require_filter {
        opts.push("require_partition_filter = true".to_string());
    }
    (!opts.is_empty()).then(|| opts.join(", "))
}

/// The `OPTIONS(...)` of a change log rivet creates: the expiry of a partition that is
/// not a load date. Load-date partitions never expire (expiring them would drop rows that
/// never changed from the view), and the log never requires a partition filter.
pub(super) fn changelog_options_sql(partition: &TablePartition) -> Option<String> {
    changelog_expiry(partition).map(|days| format!("partition_expiration_days = {days}"))
}

/// `ALTER TABLE … SET OPTIONS(...)` bringing an existing table's partition options to
/// what the load declares, or `None` when they already match (or the table is new).
pub(super) fn options_drift(
    fqtn: &str,
    existing: Option<&TableShape>,
    partition: Option<&TablePartition>,
) -> Option<String> {
    let (shape, want) = (existing?, partition?);
    let mut opts = Vec::new();
    let want_ms = want.expiration_days.map(|d| u64::from(d) * DAY_MS);
    if shape.expiration_ms != want_ms {
        opts.push(match want.expiration_days {
            Some(days) => format!("partition_expiration_days = {days}"),
            None => "partition_expiration_days = NULL".to_string(),
        });
    }
    if shape.require_partition_filter != want.require_filter {
        opts.push(format!(
            "require_partition_filter = {}",
            want.require_filter
        ));
    }
    (!opts.is_empty()).then(|| format!("ALTER TABLE `{fqtn}` SET OPTIONS({});", opts.join(", ")))
}
