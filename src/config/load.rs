//! The `load:` block — the warehouse target and the per-table load settings — typed
//! and validated when the config is read, so `rivet check` and `rivet run` refuse a
//! malformed block before an extract runs, and the JSON schema documents it.

use std::borrow::Cow;

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::Deserialize;

/// BigQuery's cap on partitions per table.
pub(crate) const MAX_TABLE_PARTITIONS: i64 = 10_000;

/// Days before an hourly-partitioned table reaches [`MAX_TABLE_PARTITIONS`].
pub(crate) const HOURLY_LIFETIME_DAYS: u32 = (MAX_TABLE_PARTITIONS / 24) as u32;

/// The top-level `load:` block: one warehouse for every export, plus the per-table
/// defaults an export's own `load:` overrides.
#[derive(Debug, Clone, Deserialize)]
#[serde(try_from = "RawLoadSection")]
pub struct LoadSection {
    pub target: LoadTarget,
    /// After a successful load, delete the staged Parquet under the export prefix.
    pub cleanup_source: bool,
    /// Dedup key of the incremental/CDC current-state view; ignored for `full`.
    pub pk: KeyColumns,
    /// Load even when a run manifest's source count disagrees with what it extracted.
    pub allow_source_drift: bool,
    /// After a successful load, delete staged Parquet no `Success` manifest references.
    pub gc_orphans: bool,
    /// `CLUSTER BY` of the table the load writes.
    pub cluster_by: KeyColumns,
    /// How the table the load writes is partitioned; `None` leaves it flat.
    pub partition: Option<PartitionSpec>,
    /// Where the current state lives; `None` = derive it from the export's mode.
    pub layout: Option<LayoutChoice>,
    /// Whether a base-and-buffer table carries `__is_deleted`; `None` = derive it
    /// from the export's mode.
    pub deleted_flag: Option<bool>,
}

impl JsonSchema for LoadSection {
    fn schema_name() -> Cow<'static, str> {
        "LoadSection".into()
    }

    fn json_schema(g: &mut SchemaGenerator) -> Schema {
        RawLoadSection::json_schema(g)
    }
}

/// The block as written: `target` plus the union of both warehouses' fields.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RawLoadSection {
    /// The warehouse: `bigquery` or `snowflake`.
    target: LoadTargetKind,
    /// BigQuery: the project the dataset lives in.
    #[serde(default)]
    project: Option<String>,
    /// BigQuery: the dataset the tables are created in.
    #[serde(default)]
    dataset: Option<String>,
    /// Snowflake: the `snow` CLI connection name.
    #[serde(default)]
    connection: Option<String>,
    /// Snowflake: the virtual warehouse the load runs on.
    #[serde(default)]
    warehouse: Option<String>,
    /// Snowflake: the database the tables are created in.
    #[serde(default)]
    database: Option<String>,
    /// Snowflake: the schema the tables are created in.
    #[serde(default)]
    schema: Option<String>,
    /// Snowflake: a pre-created GCS `STORAGE INTEGRATION`.
    #[serde(default)]
    storage_integration: Option<String>,
    /// After a successful load, delete the staged Parquet under the export prefix.
    #[serde(default)]
    cleanup_source: bool,
    /// Dedup key of the incremental/CDC current-state view: `auto` (the source primary
    /// key `rivet run` recorded), `none`, or explicit columns; ignored for `full`.
    #[serde(default)]
    pk: KeyColumns,
    /// `log_view` or `base_buffer` — where the current state lives. Absent derives
    /// it from the mode: a CDC stream with a `backfill:` is base+buffer, the rest
    /// changelog+view. `base_buffer` needs `target: bigquery` — `rivet compact` is
    /// what merges the buffer into the base, and it is BigQuery-only.
    #[serde(default)]
    layout: Option<LayoutChoice>,
    /// Whether the base carries a `__is_deleted` column. Absent derives it from the
    /// mode: a CDC stream expresses deletes and gets the flag, a query-based export
    /// cannot express one and does not — an extra column per row otherwise.
    #[serde(default)]
    deleted_flag: Option<bool>,
    /// Load even when a run manifest's source count disagrees with what it extracted
    /// (source→file drift): warn instead of blocking.
    #[serde(default)]
    allow_source_drift: bool,
    /// After a successful load, delete staged Parquet under the export prefix that no
    /// `Success` manifest references — crash leftovers. Only when no extract writes the
    /// prefix concurrently.
    #[serde(default)]
    gc_orphans: bool,
    /// `CLUSTER BY` of the table the load writes: `auto` (the primary key), `none`, or
    /// explicit columns (at most 4 on BigQuery).
    #[serde(default)]
    cluster_by: KeyColumns,
    /// How the table the load writes is partitioned: `none` (default), or exactly one of
    /// `column` (+ `granularity`), an integer `range`, or `ingestion` time.
    #[serde(default, deserialize_with = "partition_setting")]
    #[schemars(with = "Option<PartitionSetting>")]
    partition: Option<PartitionSpec>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
enum LoadTargetKind {
    Bigquery,
    Snowflake,
}

impl LoadTargetKind {
    fn name(self) -> &'static str {
        match self {
            LoadTargetKind::Bigquery => "bigquery",
            LoadTargetKind::Snowflake => "snowflake",
        }
    }
}

impl TryFrom<RawLoadSection> for LoadSection {
    type Error = String;

    /// The target's own fields must be present and the other warehouse's absent.
    fn try_from(r: RawLoadSection) -> Result<Self, String> {
        let name = r.target.name();
        let bigquery = [("project", &r.project), ("dataset", &r.dataset)];
        let snowflake = [
            ("connection", &r.connection),
            ("warehouse", &r.warehouse),
            ("database", &r.database),
            ("schema", &r.schema),
            ("storage_integration", &r.storage_integration),
        ];
        let (own, foreign, other) = match r.target {
            LoadTargetKind::Bigquery => (&bigquery[..], &snowflake[..], "snowflake"),
            LoadTargetKind::Snowflake => (&snowflake[..], &bigquery[..], "bigquery"),
        };
        if let Some((field, _)) = foreign.iter().find(|(_, v)| v.is_some()) {
            return Err(format!(
                "`load:` targets `{name}` but carries `{field}`, a `{other}` field — remove it \
                 (it would be silently ignored, masking a mis-configured load)"
            ));
        }
        if let Some((field, _)) = own.iter().find(|(_, v)| v.is_none()) {
            return Err(format!("`load:` targets `{name}` but has no `{field}`"));
        }
        let take = |v: &Option<String>| v.clone().unwrap_or_default();
        let target = match r.target {
            LoadTargetKind::Bigquery => LoadTarget::Bigquery {
                project: take(&r.project),
                dataset: take(&r.dataset),
            },
            LoadTargetKind::Snowflake => LoadTarget::Snowflake {
                connection: take(&r.connection),
                warehouse: take(&r.warehouse),
                database: take(&r.database),
                schema: take(&r.schema),
                storage_integration: take(&r.storage_integration),
            },
        };
        Ok(LoadSection {
            target,
            cleanup_source: r.cleanup_source,
            pk: r.pk,
            allow_source_drift: r.allow_source_drift,
            gc_orphans: r.gc_orphans,
            cluster_by: r.cluster_by,
            partition: r.partition,
            layout: r.layout,
            deleted_flag: r.deleted_flag,
        })
    }
}

impl LoadSection {
    /// This section with one export's [`LoadOverride`] applied: each `Some` replaces,
    /// each `None` inherits. The target is never overridden.
    pub fn with_override(&self, o: &LoadOverride) -> LoadSection {
        let mut eff = self.clone();
        if let Some(pk) = &o.pk {
            eff.pk = pk.clone();
        }
        if let Some(c) = o.cleanup_source {
            eff.cleanup_source = c;
        }
        if let Some(g) = o.gc_orphans {
            eff.gc_orphans = g;
        }
        if let Some(cb) = &o.cluster_by {
            eff.cluster_by = cb.clone();
        }
        if let Some(d) = o.allow_source_drift {
            eff.allow_source_drift = d;
        }
        if let Some(p) = &o.partition {
            eff.partition = p.clone();
        }
        if let Some(l) = o.layout {
            eff.layout = Some(l);
        }
        if let Some(d) = o.deleted_flag {
            eff.deleted_flag = Some(d);
        }
        eff
    }
}

/// A warehouse and its connection config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoadTarget {
    Bigquery {
        project: String,
        dataset: String,
    },
    Snowflake {
        connection: String,
        warehouse: String,
        database: String,
        schema: String,
        storage_integration: String,
    },
}

impl LoadTarget {
    /// The target name the type resolver is keyed on (`ExportTarget::parse`).
    pub fn name(&self) -> &'static str {
        match self {
            LoadTarget::Bigquery { .. } => "bigquery",
            LoadTarget::Snowflake { .. } => "snowflake",
        }
    }
}

/// Per-export overrides of the top-level [`LoadSection`]: every field optional, `None`
/// inherits. The warehouse is shared, so `target` is not among them.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LoadOverride {
    /// Dedup key of this table's current-state view.
    #[serde(default)]
    pub pk: Option<KeyColumns>,
    #[serde(default)]
    pub cleanup_source: Option<bool>,
    #[serde(default)]
    pub gc_orphans: Option<bool>,
    /// `CLUSTER BY` of this table.
    #[serde(default)]
    pub cluster_by: Option<KeyColumns>,
    #[serde(default)]
    pub allow_source_drift: Option<bool>,
    /// Where this table's current state lives; inherits when absent.
    #[serde(default)]
    pub layout: Option<LayoutChoice>,
    /// Whether this table's base carries `__is_deleted`; inherits when absent.
    #[serde(default)]
    pub deleted_flag: Option<bool>,
    /// This table's partitioning; `none` clears an inherited one.
    #[serde(default, deserialize_with = "partition_override")]
    #[schemars(with = "Option<PartitionSetting>")]
    pub partition: Option<Option<PartitionSpec>>,
    /// On a multiplex `tables:` CDC export: the override for ONE captured table,
    /// keyed by its name, layered over this block — six tables through one stream
    /// rarely share a partition column or a key. Every name must be one of the
    /// export's `tables:`; a nested `tables:` is refused.
    #[serde(default)]
    pub tables: std::collections::BTreeMap<String, LoadOverride>,
}

/// Where an incremental or CDC table's CURRENT STATE lives in the warehouse.
///
/// Absent keeps today's rule: a CDC stream with a `backfill:` gets the base and
/// buffer, everything else the changelog and its view. Written, it decides —
/// which is how an ordinary query-based `incremental` export gets a PHYSICAL
/// base that `rivet compact` merges into, instead of a view that re-ranks the
/// whole log on every read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LayoutChoice {
    /// `<table>` is the dedup VIEW over `<table>__changes`.
    LogView,
    /// `<table>` is a physical base; `<table>__changes` is a disposable buffer
    /// `rivet compact` merges into it and drops.
    BaseBuffer,
}

/// A column list in a `load:` block: `auto` (from the recorded source primary key),
/// `none`, or explicit columns.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum KeyColumns {
    #[default]
    Auto,
    None,
    Columns(Vec<String>),
}

impl<'de> Deserialize<'de> for KeyColumns {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Word(String),
            List(Vec<String>),
        }
        match Raw::deserialize(d)? {
            Raw::Word(w) if w == "auto" => Ok(KeyColumns::Auto),
            Raw::Word(w) if w == "none" => Ok(KeyColumns::None),
            Raw::Word(w) => Err(serde::de::Error::custom(format!(
                "expected `auto`, `none` or a list of columns, got `{w}`"
            ))),
            Raw::List(cols) => Ok(KeyColumns::Columns(cols)),
        }
    }
}

impl JsonSchema for KeyColumns {
    fn schema_name() -> Cow<'static, str> {
        "KeyColumns".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "`auto` (the source primary key `rivet run` recorded), `none`, or a list of columns",
            "anyOf": [
                { "type": "string", "enum": ["auto", "none"] },
                { "type": "array", "items": { "type": "string" } }
            ]
        })
    }
}

/// A partition granularity: `hour`, `day`, `month` or `year`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, serde::Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Granularity {
    Hour,
    Day,
    Month,
    Year,
}

impl Granularity {
    /// The name BigQuery uses in `*_TRUNC(…)` and `timePartitioning.type`.
    pub fn as_sql(self) -> &'static str {
        match self {
            Granularity::Hour => "HOUR",
            Granularity::Day => "DAY",
            Granularity::Month => "MONTH",
            Granularity::Year => "YEAR",
        }
    }

    /// The name as the config writes it.
    pub fn as_str(self) -> &'static str {
        match self {
            Granularity::Hour => "hour",
            Granularity::Day => "day",
            Granularity::Month => "month",
            Granularity::Year => "year",
        }
    }

    /// `HOUR` / `DAY` / `MONTH` / `YEAR` back to a granularity.
    pub fn parse_sql(s: &str) -> Option<Self> {
        match s {
            "HOUR" => Some(Granularity::Hour),
            "DAY" => Some(Granularity::Day),
            "MONTH" => Some(Granularity::Month),
            "YEAR" => Some(Granularity::Year),
            _ => None,
        }
    }

    /// The next coarser granularity, if there is one.
    pub fn coarser(self) -> Option<Self> {
        match self {
            Granularity::Hour => Some(Granularity::Day),
            Granularity::Day => Some(Granularity::Month),
            Granularity::Month => Some(Granularity::Year),
            Granularity::Year => None,
        }
    }
}

/// A `partition:` block: one form plus the table options.
#[derive(Debug, Clone, PartialEq)]
pub struct PartitionSpec {
    pub form: PartitionForm,
    /// `partition_expiration_days`.
    pub expiration_days: Option<u32>,
    /// `require_partition_filter`.
    pub require_filter: bool,
}

/// The one partition form a `partition:` block names.
#[derive(Debug, Clone, PartialEq)]
pub enum PartitionForm {
    /// A DATE / DATETIME / TIMESTAMP column at a granularity.
    Column {
        column: String,
        granularity: Granularity,
    },
    /// An INT64 column in `interval`-wide buckets from `start` up to `end`.
    Range {
        column: String,
        start: i64,
        end: i64,
        interval: i64,
    },
    /// The load time, at a granularity.
    Ingestion(Granularity),
}

impl PartitionForm {
    /// The source column the form partitions by, `None` for ingestion time.
    pub fn column(&self) -> Option<&str> {
        match self {
            PartitionForm::Column { column, .. } | PartitionForm::Range { column, .. } => {
                Some(column)
            }
            PartitionForm::Ingestion(_) => None,
        }
    }
}

/// The schema of a `partition:` value: `none`, or a block.
#[derive(JsonSchema)]
#[schemars(untagged)]
#[allow(dead_code)]
enum PartitionSetting {
    None(NoneWord),
    Block(RawPartition),
}

/// The literal `none`.
#[derive(JsonSchema)]
#[schemars(rename_all = "lowercase")]
#[allow(dead_code)]
enum NoneWord {
    None,
}

/// A `partition:` block as written.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RawPartition {
    /// A DATE / DATETIME / TIMESTAMP column to partition by.
    #[serde(default)]
    column: Option<String>,
    /// `hour` / `day` (default) / `month` / `year`, with `column`.
    #[serde(default)]
    granularity: Option<Granularity>,
    /// Integer-range partitioning of an INT64 column.
    #[serde(default)]
    range: Option<RawRange>,
    /// Partition by load time at this granularity.
    #[serde(default)]
    ingestion: Option<Granularity>,
    /// `partition_expiration_days`.
    #[serde(default)]
    expiration_days: Option<u32>,
    /// `require_partition_filter` on the full-load table.
    #[serde(default)]
    require_filter: bool,
}

/// `range: { column, start, end, interval }`.
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RawRange {
    column: String,
    start: i64,
    end: i64,
    interval: i64,
}

impl RawPartition {
    /// Check the block names exactly one well-formed partition form.
    fn into_spec(self) -> Result<PartitionSpec, String> {
        let forms = [
            self.column.is_some(),
            self.range.is_some(),
            self.ingestion.is_some(),
        ]
        .iter()
        .filter(|f| **f)
        .count();
        if forms != 1 {
            return Err(
                "`partition` takes exactly one of `column`, `range` or `ingestion`".to_string(),
            );
        }
        if self.granularity.is_some() && self.column.is_none() {
            return Err(
                "`granularity` goes with `column`; an `ingestion` partition names its \
                 granularity directly (`ingestion: day`)"
                    .to_string(),
            );
        }
        if self.expiration_days == Some(0) {
            return Err(
                "`expiration_days` must be positive; omit it to keep partitions forever"
                    .to_string(),
            );
        }
        if self.range.is_some() && self.expiration_days.is_some() {
            return Err(
                "`expiration_days` does not apply to a `range` partition: BigQuery has no expiry \
                 for integer ranges"
                    .to_string(),
            );
        }
        let form = if let Some(column) = self.column {
            PartitionForm::Column {
                column,
                granularity: self.granularity.unwrap_or(Granularity::Day),
            }
        } else if let Some(r) = self.range {
            if r.interval <= 0 {
                return Err(format!(
                    "`range.interval` must be positive, got {}",
                    r.interval
                ));
            }
            if r.start >= r.end {
                return Err(format!(
                    "`range.start` ({}) must be below `range.end` ({})",
                    r.start, r.end
                ));
            }
            let (span, interval) = (
                i128::from(r.end) - i128::from(r.start),
                i128::from(r.interval),
            );
            let buckets = (span + interval - 1) / interval;
            if buckets > i128::from(MAX_TABLE_PARTITIONS) {
                return Err(format!(
                    "`range` makes {buckets} partitions; BigQuery allows {MAX_TABLE_PARTITIONS} \
                     per table — widen `interval`"
                ));
            }
            PartitionForm::Range {
                column: r.column,
                start: r.start,
                end: r.end,
                interval: r.interval,
            }
        } else {
            PartitionForm::Ingestion(self.ingestion.expect("one form is present"))
        };
        Ok(PartitionSpec {
            form,
            expiration_days: self.expiration_days,
            require_filter: self.require_filter,
        })
    }
}

/// `none`, or a partition block.
fn partition_setting<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<PartitionSpec>, D::Error> {
    use serde::de::Error;
    match serde_json::Value::deserialize(d)? {
        serde_json::Value::String(w) if w == "none" => Ok(None),
        serde_json::Value::String(w) => Err(D::Error::custom(format!(
            "expected `none` or a partition block, got `{w}`"
        ))),
        block => {
            let raw: RawPartition = serde_json::from_value(block).map_err(D::Error::custom)?;
            raw.into_spec().map(Some).map_err(D::Error::custom)
        }
    }
}

/// A per-export `partition:` — present means replace, `none` included.
fn partition_override<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<Option<PartitionSpec>>, D::Error> {
    partition_setting(d).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn section(v: serde_json::Value) -> Result<LoadSection, String> {
        serde_json::from_value::<LoadSection>(v).map_err(|e| e.to_string())
    }

    fn bigquery(extra: serde_json::Value) -> LoadSection {
        let mut v = serde_json::json!({ "target": "bigquery", "project": "p", "dataset": "d" });
        v.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        section(v).unwrap()
    }

    #[test]
    fn a_load_block_needs_its_own_targets_fields_and_none_of_the_others() {
        let bq = bigquery(serde_json::json!({}));
        assert_eq!(
            bq.target,
            LoadTarget::Bigquery {
                project: "p".into(),
                dataset: "d".into()
            }
        );
        let e = section(serde_json::json!({ "target": "bigquery", "project": "p" })).unwrap_err();
        assert!(e.contains("has no `dataset`"), "{e}");
        let e = section(serde_json::json!({
            "target": "snowflake", "connection": "c", "warehouse": "w", "database": "db",
            "schema": "s", "storage_integration": "si", "project": "p"
        }))
        .unwrap_err();
        assert!(
            e.contains("targets `snowflake` but carries `project`, a `bigquery` field"),
            "{e}"
        );
        let e = section(serde_json::json!({ "project": "p", "dataset": "d" })).unwrap_err();
        assert!(e.contains("target"), "{e}");
        let e = section(serde_json::json!({ "target": "redshift" })).unwrap_err();
        assert!(e.contains("redshift"), "{e}");
    }

    #[test]
    fn a_typo_in_either_load_block_is_refused() {
        let e = section(serde_json::json!({
            "target": "bigquery", "project": "p", "dataset": "d", "gc_orphan": true
        }))
        .unwrap_err();
        assert!(e.contains("gc_orphan"), "{e}");
        let typo = serde_json::json!({ "pk": ["id"], "cluster_bye": ["x"] });
        assert!(serde_json::from_value::<LoadOverride>(typo).is_err());
        let retarget = serde_json::json!({ "target": "snowflake" });
        assert!(serde_json::from_value::<LoadOverride>(retarget).is_err());
        let ok = serde_json::json!({ "pk": ["id"], "cleanup_source": true });
        assert!(serde_json::from_value::<LoadOverride>(ok).is_ok());
    }

    #[test]
    fn key_columns_read_auto_none_and_lists_and_default_to_auto() {
        let parse = serde_json::from_value::<KeyColumns>;
        assert_eq!(parse(serde_json::json!("auto")).unwrap(), KeyColumns::Auto);
        assert_eq!(parse(serde_json::json!("none")).unwrap(), KeyColumns::None);
        assert_eq!(
            parse(serde_json::json!(["a", "b"])).unwrap(),
            KeyColumns::Columns(vec!["a".into(), "b".into()])
        );
        let err = parse(serde_json::json!("id")).unwrap_err().to_string();
        assert!(err.contains("`auto`, `none` or a list"), "{err}");
        let defaults = bigquery(serde_json::json!({}));
        assert_eq!(
            (defaults.pk, defaults.cluster_by, defaults.partition),
            (KeyColumns::Auto, KeyColumns::Auto, None)
        );
    }

    #[test]
    fn with_override_replaces_some_fields_and_inherits_the_rest() {
        let top = bigquery(serde_json::json!({
            "cleanup_source": true, "gc_orphans": true, "allow_source_drift": true,
            "pk": ["a"], "cluster_by": ["a"], "partition": { "column": "ts" }
        }));
        let o: LoadOverride = serde_json::from_value(serde_json::json!({
            "pk": ["b"], "cleanup_source": false, "partition": "none"
        }))
        .unwrap();
        let eff = top.with_override(&o);
        assert_eq!(eff.pk, KeyColumns::Columns(vec!["b".into()]));
        assert!(!eff.cleanup_source);
        assert_eq!(eff.partition, None, "`none` clears the inherited partition");
        assert!(eff.gc_orphans && eff.allow_source_drift, "inherited");
        assert_eq!(eff.cluster_by, KeyColumns::Columns(vec!["a".into()]));
        assert_eq!(eff.target, top.target);
        let inherit = top.with_override(&LoadOverride::default());
        assert_eq!(inherit.partition, top.partition);
        let replaced: LoadOverride =
            serde_json::from_value(serde_json::json!({ "partition": { "ingestion": "hour" } }))
                .unwrap();
        assert_eq!(
            top.with_override(&replaced).partition.unwrap().form,
            PartitionForm::Ingestion(Granularity::Hour)
        );
    }

    fn partition_of(block: serde_json::Value) -> Result<Option<PartitionSpec>, String> {
        section(serde_json::json!({
            "target": "bigquery", "project": "p", "dataset": "d", "partition": block
        }))
        .map(|s| s.partition)
    }

    #[test]
    fn partition_block_reads_one_form_and_none() {
        assert_eq!(partition_of(serde_json::json!("none")).unwrap(), None);
        assert_eq!(
            partition_of(serde_json::json!({ "column": "ts" })).unwrap(),
            Some(PartitionSpec {
                form: PartitionForm::Column {
                    column: "ts".into(),
                    granularity: Granularity::Day
                },
                expiration_days: None,
                require_filter: false,
            }),
            "granularity defaults to day"
        );
        let full = partition_of(serde_json::json!({
            "column": "ts", "granularity": "hour", "expiration_days": 30, "require_filter": true
        }))
        .unwrap()
        .unwrap();
        assert_eq!(
            (full.expiration_days, full.require_filter),
            (Some(30), true)
        );
        assert!(matches!(
            full.form,
            PartitionForm::Column {
                granularity: Granularity::Hour,
                ..
            }
        ));
        let range = partition_of(serde_json::json!({
            "range": { "column": "n", "start": 0, "end": 1000, "interval": 10 }
        }))
        .unwrap()
        .unwrap();
        assert_eq!(
            range.form,
            PartitionForm::Range {
                column: "n".into(),
                start: 0,
                end: 1000,
                interval: 10
            }
        );
        assert_eq!(
            partition_of(serde_json::json!({ "ingestion": "month" }))
                .unwrap()
                .unwrap()
                .form,
            PartitionForm::Ingestion(Granularity::Month)
        );
    }

    #[test]
    fn partition_block_refuses_two_forms_bad_ranges_and_zero_expiry() {
        let err = |block| partition_of(block).unwrap_err();
        let e = err(serde_json::json!({ "column": "ts", "ingestion": "day" }));
        assert!(e.contains("exactly one of"), "{e}");
        let e = err(serde_json::json!({}));
        assert!(e.contains("exactly one of"), "{e}");
        let e = err(serde_json::json!({ "ingestion": "day", "granularity": "hour" }));
        assert!(e.contains("`granularity` goes with `column`"), "{e}");
        let e = err(serde_json::json!({ "column": "ts", "granularity": "week" }));
        assert!(e.contains("week"), "{e}");
        let e = err(serde_json::json!({ "column": "ts", "expiration_days": 0 }));
        assert!(e.contains("`expiration_days` must be positive"), "{e}");
        let e = err(serde_json::json!({
            "range": { "column": "n", "start": 0, "end": 100, "interval": 10 },
            "expiration_days": 30
        }));
        assert!(e.contains("does not apply to a `range` partition"), "{e}");
        let e = err(serde_json::json!({ "column": "ts", "expiration_day": 3 }));
        assert!(e.contains("expiration_day"), "{e}");
        let e = err(serde_json::json!("weekly"));
        assert!(e.contains("expected `none` or a partition block"), "{e}");
        let range = |start: i64, end: i64, interval: i64| {
            err(serde_json::json!({
                "range": { "column": "n", "start": start, "end": end, "interval": interval }
            }))
        };
        assert!(range(0, 10, 0).contains("`range.interval` must be positive"));
        assert!(range(10, 10, 1).contains("must be below `range.end`"));
        assert!(range(0, 1_000_000, 1).contains("allows 10000 per table"));
        assert!(
            partition_of(serde_json::json!({
                "range": { "column": "n", "start": 0, "end": 10000, "interval": 1 }
            }))
            .unwrap()
            .is_some(),
            "exactly the cap is allowed"
        );
    }

    #[test]
    fn granularities_round_trip_and_coarsen() {
        assert_eq!(Granularity::Hour.coarser(), Some(Granularity::Day));
        assert_eq!(Granularity::Year.coarser(), None);
        assert_eq!(Granularity::parse_sql("MONTH"), Some(Granularity::Month));
        assert_eq!(Granularity::parse_sql("WEEK"), None);
        assert_eq!(Granularity::Day.as_sql(), "DAY");
        assert_eq!(Granularity::Day.as_str(), "day");
        assert_eq!(HOURLY_LIFETIME_DAYS, 416);
    }

    #[test]
    fn the_json_schema_documents_the_block_as_written() {
        let schema = schemars::schema_for!(LoadSection);
        let text = serde_json::to_string(&schema).unwrap();
        for key in [
            "\"target\"",
            "\"project\"",
            "\"partition\"",
            "\"cluster_by\"",
            "\"none\"",
        ] {
            assert!(text.contains(key), "{key} missing from {text}");
        }
        let over = serde_json::to_string(&schemars::schema_for!(LoadOverride)).unwrap();
        assert!(
            over.contains("\"partition\"") && !over.contains("\"target\""),
            "{over}"
        );
    }
}

#[cfg(test)]
mod partition_form_tests {
    use super::*;

    /// The column each `partition:` form keys on. Both the compaction plan and
    /// `rivet check` resolve the partition column through here, so a wrong name
    /// prunes the wrong thing — or nothing.
    #[test]
    fn a_partition_form_names_the_column_it_keys_on() {
        let column = PartitionForm::Column {
            column: "created_at".into(),
            granularity: Granularity::Day,
        };
        assert_eq!(column.column(), Some("created_at"));
        let range = PartitionForm::Range {
            column: "bucket".into(),
            start: 0,
            end: 1000,
            interval: 10,
        };
        assert_eq!(range.column(), Some("bucket"));
        assert_eq!(
            PartitionForm::Ingestion(Granularity::Day).column(),
            None,
            "ingestion time is not a source column"
        );
    }
}
