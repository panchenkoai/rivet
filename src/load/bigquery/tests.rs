use super::*;
use crate::types::target::TargetStatus;

fn spec(name: &str, cast: Option<&str>, status: TargetStatus) -> TargetColumnSpec {
    TargetColumnSpec {
        column_name: name.into(),
        target_type: BqType::Json.into(),
        autoload_type: BqType::Bytes.into(),
        status,
        note: None,
        cast_sql: cast.map(String::from),
    }
}

fn uris() -> Vec<String> {
    vec!["gs://b/a.parquet".into(), "gs://b/b.parquet".into()]
}

/// The batched whole-table load ends with the target REPLACED by a clone of the
/// staging table — one statement, the table names in the right roles.
#[test]
fn the_clone_replaces_the_target_with_the_staging_table() {
    assert_eq!(
        build_clone_sql("p.d.t", "p.d.t__staging"),
        "CREATE OR REPLACE TABLE `p.d.t` CLONE `p.d.t__staging`;"
    );
}

#[test]
fn column_overlap_probes_lowercase_the_export_names() {
    let (total, matched) = build_column_overlap_sql("p", "d", "orders", &["Id", "amount"]);
    assert_eq!(
        total,
        "SELECT COUNT(*) AS n FROM `p.d`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'orders'"
    );
    assert!(
        matched.ends_with("AND LOWER(column_name) IN ('id', 'amount')"),
        "{matched}"
    );
}

#[test]
fn adoption_renames_the_full_table_then_adds_the_meta_columns() {
    let sql = build_adoption_sql(
        "p.d.orders",
        "orders",
        "p.d.orders__changes",
        &TableShape::default(),
    );
    assert_eq!(sql.len(), 2, "{sql:?}");
    assert_eq!(
        sql[0],
        "ALTER TABLE `p.d.orders` RENAME TO orders__changes;"
    );
    assert!(sql[1].starts_with("ALTER TABLE `p.d.orders__changes`"));
    for col in ["`__op` STRING", "`__pos` STRING", "`__seq` INT64"] {
        assert!(sql[1].contains(col), "{}", sql[1]);
    }
}

#[test]
fn changelog_drift_follows_only_a_declared_partition_or_clustering() {
    use super::super::ChangelogDrift;
    let shape = TableShape {
        partition: Some(time_key(Some("ts"), Granularity::Day)),
        cluster: vec!["id".into()],
        require_partition_filter: false,
        expiration_ms: None,
        bytes: Some(42),
    };
    let monthly = time_key(Some("ts"), Granularity::Month);
    let daily = time_key(Some("ts"), Granularity::Day);
    let v = vec!["v".to_string()];
    assert_eq!(
        classify_drift(&shape, Some(&monthly), Some(&v)),
        Some(ChangelogDrift::Partition {
            existing: "`ts` by day".into(),
            declared: "`ts` by month".into(),
            bytes: Some(42),
        }),
        "a partition difference comes first"
    );
    assert_eq!(
        classify_drift(&shape, Some(&daily), Some(&v)),
        Some(ChangelogDrift::Cluster {
            existing: vec!["id".into()],
            declared: v.clone(),
        })
    );
    assert_eq!(classify_drift(&shape, None, None), None, "nothing declared");
    assert_eq!(classify_drift(&shape, Some(&daily), None), None);
    assert_eq!(
        classify_drift(&shape, Some(&daily), Some(&["ID".to_string()])),
        None,
        "the same clustering, compared without case"
    );
    assert!(keeps_own_clustering(&shape, &v, false));
    assert!(!keeps_own_clustering(&shape, &v, true));
    assert!(!keeps_own_clustering(&shape, &["id".to_string()], false));
}

#[test]
fn a_recluster_patches_the_fields_or_clears_them() {
    assert_eq!(
        clustering_patch(&["a".into(), "b".into()]),
        serde_json::json!({ "clustering": { "fields": ["a", "b"] } })
    );
    assert_eq!(
        clustering_patch(&[]),
        serde_json::json!({ "clustering": null })
    );
}

#[test]
fn a_rebuild_copies_the_log_in_the_declared_shape_then_swaps_it_in() {
    let p = partition_at(time_key(Some("ts"), Granularity::Month), Some(30), true);
    let copy = build_rebuild_copy_sql(
        "p.d.t__changes__rebuild",
        "p.d.t__changes",
        Some(&p),
        &["id".into()],
        &TableProps::default(),
    );
    assert_eq!(
        copy,
        "CREATE TABLE `p.d.t__changes__rebuild`\nPARTITION BY TIMESTAMP_TRUNC(ts, DAY)\n\
         CLUSTER BY `id`\nOPTIONS(partition_expiration_days = 30)\nAS SELECT * FROM `p.d.t__changes`;"
    );
    assert!(!copy.contains("require_partition_filter"));
    let swap = build_rebuild_swap_sql(
        "p.d.t__changes",
        "p.d.t__changes__rebuild",
        "p.d.t__changes__old",
        "t__changes",
        "t__changes__old",
    );
    assert_eq!(
        swap,
        [
            "ALTER TABLE `p.d.t__changes` RENAME TO t__changes__old;",
            "ALTER TABLE `p.d.t__changes__rebuild` RENAME TO t__changes;",
            "DROP TABLE `p.d.t__changes__old`;",
        ]
    );
    let flat = build_rebuild_copy_sql("r", "c", None, &[], &TableProps::default());
    assert_eq!(flat, "CREATE TABLE `r`\nAS SELECT * FROM `c`;");
}

/// `CREATE TABLE … AS SELECT` carries rows only: the description, labels, friendly
/// name, table expiry and KMS key of the log ride on the copy's OPTIONS, and a log
/// whose columns carry policy tags (or that has row access policies) is refused, since
/// no copy carries those. RED against the bare copy.
#[test]
fn a_rebuild_carries_the_logs_properties_and_refuses_the_policies_it_cannot() {
    let meta = serde_json::json!({
        "description": "orders \"log\"",
        "friendlyName": "Orders",
        "labels": { "team": "data", "env": "prod" },
        "expirationTime": "1800000000000",
        "encryptionConfiguration": { "kmsKeyName": "projects/p/locations/eu/keyRings/r/cryptoKeys/k" },
        "schema": { "fields": [
            { "name": "id", "type": "INTEGER" },
            { "name": "email", "type": "STRING", "policyTags": { "names": ["projects/p/taxonomies/1/policyTags/2"] } },
            { "name": "v", "type": "STRING", "policyTags": { "names": [] } }
        ] }
    });
    let props = parse_table_props(&meta);
    assert_eq!(
        props,
        TableProps {
            description: Some("orders \"log\"".into()),
            friendly_name: Some("Orders".into()),
            labels: vec![
                ("env".into(), "prod".into()),
                ("team".into(), "data".into())
            ],
            expiration_ms: Some(1_800_000_000_000),
            kms_key_name: Some("projects/p/locations/eu/keyRings/r/cryptoKeys/k".into()),
            policy_tagged: vec!["email".into()],
        }
    );
    let plain = TableProps {
        policy_tagged: vec![],
        ..props.clone()
    };
    let copy = build_rebuild_copy_sql("r", "c", None, &[], &plain);
    assert_eq!(
        copy,
        "CREATE TABLE `r`\nOPTIONS(description = \"orders \\\"log\\\"\", friendly_name = \"Orders\", \
         labels = [(\"env\", \"prod\"), (\"team\", \"data\")], \
         expiration_timestamp = TIMESTAMP_MILLIS(1800000000000), \
         kms_key_name = \"projects/p/locations/eu/keyRings/r/cryptoKeys/k\")\nAS SELECT * FROM `c`;"
    );
    let p = partition_at(time_key(Some("ts"), Granularity::Day), Some(30), false);
    let with_partition = build_rebuild_copy_sql("r", "c", Some(&p), &[], &plain);
    assert!(
        with_partition.contains("OPTIONS(partition_expiration_days = 30, description = "),
        "{with_partition}"
    );
    assert_eq!(
        parse_table_props(&serde_json::json!({})),
        TableProps::default()
    );

    assert_eq!(rebuild_policy_refusal("p.d.t__changes", &plain, 0), None);
    let why = rebuild_policy_refusal("p.d.t__changes", &props, 2).unwrap();
    assert!(
        why.contains("policy tags on `email`") && why.contains("2 row access policy"),
        "{why}"
    );
    assert!(why.contains("nothing was changed"), "{why}");
    let why = rebuild_policy_refusal("p.d.t__changes", &plain, 1).unwrap();
    assert!(!why.contains("policy tags"), "{why}");
}

#[test]
fn the_leftovers_probe_names_what_an_interrupted_rebuild_left() {
    let names = [
        "t__changes__rebuild".to_string(),
        "t__changes__old".to_string(),
    ];
    let sql = build_leftovers_sql("p", "d", &names);
    assert_eq!(
        sql,
        "SELECT 1 * COUNTIF(table_name = 't__changes__rebuild') + 2 * COUNTIF(table_name = \
         't__changes__old') AS n FROM `p.d`.INFORMATION_SCHEMA.TABLES"
    );
    assert!(leftover_names(0, &names).is_empty());
    assert_eq!(leftover_names(1, &names), ["t__changes__rebuild"]);
    assert_eq!(leftover_names(2, &names), ["t__changes__old"]);
    assert_eq!(leftover_names(3, &names), names);
}

#[test]
fn the_log_takes_a_partition_without_a_filter_or_a_load_date_expiry() {
    let column = partition_at(time_key(Some("ts"), Granularity::Day), Some(400), true);
    let log = changelog_partition(&column);
    assert_eq!(
        (log.expiration_days, log.require_filter),
        (Some(400), false)
    );
    assert_eq!(log.key, column.key);
    let ingestion = partition_at(time_key(None, Granularity::Day), Some(3), true);
    let log = changelog_partition(&ingestion);
    assert_eq!((log.expiration_days, log.require_filter), (None, false));
    assert_eq!(changelog_options_sql(&ingestion), None);
}

/// Every row of a run shares one `_rivet_exported_at`, so a log partitioned on it is
/// partitioned by load date: an expiry would drop the never-changed baseline rows from
/// the current-state view. The stamp gets the load-time treatment.
#[test]
fn the_log_never_expires_partitions_on_the_export_stamp() {
    let stamp = partition_at(
        time_key(Some(crate::enrich::COL_EXPORTED_AT), Granularity::Day),
        Some(30),
        false,
    );
    assert_eq!(changelog_partition(&stamp).expiration_days, None);
    assert_eq!(changelog_options_sql(&stamp), None);
    let shape = TableShape {
        partition: Some(time_key(
            Some(crate::enrich::COL_EXPORTED_AT),
            Granularity::Day,
        )),
        expiration_ms: Some(30 * DAY_MS),
        ..TableShape::default()
    };
    assert!(shape.expires_load_dates());
    let adoption = build_adoption_sql("p.d.t", "t", "p.d.t__changes", &shape);
    assert!(
        adoption
            .iter()
            .any(|s| s.contains("partition_expiration_days = NULL")),
        "{adoption:?}"
    );
    // A range never expires anywhere: the config refuses the pair, and the SQL
    // builders emit no expiry for one either.
    let range = TablePartition {
        key: PartitionKey::Range {
            column: "n".into(),
            start: 0,
            end: 100,
            interval: 10,
        },
        expr: "RANGE_BUCKET(n, GENERATE_ARRAY(0, 100, 10))".into(),
        expiration_days: Some(30),
        require_filter: false,
    };
    assert_eq!(creation_options(true, Some(&range)), None);
    assert_eq!(changelog_options_sql(&range), None);
}

#[test]
fn clusterable_names_the_types_bigquery_clusters_on() {
    for t in [
        BqType::BigNumeric,
        BqType::Bool,
        BqType::Date,
        BqType::DateTime,
        BqType::Int64,
        BqType::Numeric,
        BqType::String,
        BqType::Timestamp,
    ] {
        assert!(clusterable(&t.clone().into()), "{t}");
    }
    for t in [
        BqType::Float64,
        BqType::Time,
        BqType::Bytes,
        BqType::Json,
        BqType::Array(Box::new(BqType::Int64)),
    ] {
        assert!(!clusterable(&t.clone().into()), "{t}");
    }
    assert!(
        !clusterable(&crate::types::target::SfType::Date.into()),
        "a Snowflake type is not a BigQuery clustering key"
    );
}

/// A `tables.get` resource with the given partitioning, clustering and options.
fn meta(partition: serde_json::Value, cluster: &[&str], require_filter: bool) -> serde_json::Value {
    let mut m = serde_json::json!({ "type": "TABLE", "numRows": "3" });
    if let Some(tp) = partition.get("time") {
        m["timePartitioning"] = tp.clone();
    }
    if let Some(rp) = partition.get("range") {
        m["rangePartitioning"] = rp.clone();
    }
    if !cluster.is_empty() {
        m["clustering"] = serde_json::json!({ "fields": cluster });
    }
    if require_filter {
        m["requirePartitionFilter"] = serde_json::json!(true);
    }
    m
}

fn time_key(column: Option<&str>, granularity: Granularity) -> PartitionKey {
    PartitionKey::Time {
        column: column.map(String::from),
        granularity,
    }
}

fn partition_at(
    key: PartitionKey,
    expiration_days: Option<u32>,
    require_filter: bool,
) -> TablePartition {
    TablePartition {
        expr: "TIMESTAMP_TRUNC(ts, DAY)".into(),
        key,
        expiration_days,
        require_filter,
    }
}

#[test]
fn adoption_drops_the_partition_filter_and_load_date_expiry() {
    let shape = parse_table_shape(&meta(
        serde_json::json!({ "time": { "type": "DAY", "expirationMs": "2592000000", "requirePartitionFilter": true } }),
        &[],
        false,
    ));
    let sql = build_adoption_sql("p.d.t", "t", "p.d.t__changes", &shape).join("\n");
    assert!(
        sql.contains("SET OPTIONS(require_partition_filter = false)"),
        "{sql}"
    );
    assert!(
        sql.contains("SET OPTIONS(partition_expiration_days = NULL)"),
        "{sql}"
    );

    let column_partitioned = parse_table_shape(&meta(
        serde_json::json!({ "time": { "type": "DAY", "field": "d", "expirationMs": "2592000000" } }),
        &[],
        false,
    ));
    let sql = build_adoption_sql("p.d.t", "t", "p.d.t__changes", &column_partitioned).join("\n");
    assert!(!sql.contains("partition_expiration_days"), "{sql}");
    assert!(!sql.contains("require_partition_filter"), "{sql}");
}

#[test]
fn tables_get_metadata_yields_the_shape() {
    let shape = parse_table_shape(&meta(
        serde_json::json!({ "time": { "type": "HOUR", "field": "ts", "expirationMs": "86400000" } }),
        &["v", "order"],
        true,
    ));
    assert_eq!(
        shape,
        TableShape {
            partition: Some(time_key(Some("ts"), Granularity::Hour)),
            cluster: vec!["v".into(), "order".into()],
            require_partition_filter: true,
            expiration_ms: Some(DAY_MS),
            bytes: None,
        }
    );
    let range = parse_table_shape(&meta(
        serde_json::json!({ "range": { "field": "n", "range": { "start": "0", "end": "1000", "interval": "10" } } }),
        &[],
        false,
    ));
    assert_eq!(
        range.partition,
        Some(PartitionKey::Range {
            column: "n".into(),
            start: 0,
            end: 1000,
            interval: 10
        })
    );
    let ingestion = parse_table_shape(&meta(
        serde_json::json!({ "time": { "type": "DAY", "expirationMs": "3" } }),
        &[],
        false,
    ));
    assert_eq!(ingestion.partition, Some(time_key(None, Granularity::Day)));
    assert!(ingestion.expires_load_dates());
    assert!(
        !shape.expires_load_dates(),
        "a column partition's expiry is not a load-date one"
    );
    let plain = parse_table_shape(&serde_json::json!({ "type": "TABLE" }));
    assert_eq!(plain, TableShape::default());
}

#[test]
fn creation_options_ride_only_on_a_creating_load() {
    let p = partition_at(time_key(Some("ts"), Granularity::Day), Some(400), true);
    assert_eq!(
        creation_options(true, Some(&p)).as_deref(),
        Some("partition_expiration_days = 400, require_partition_filter = true")
    );
    assert_eq!(
        creation_options(false, Some(&p)),
        None,
        "an existing table is altered instead"
    );
    assert_eq!(creation_options(true, None), None);
    let bare = partition_at(time_key(Some("ts"), Granularity::Day), None, false);
    assert_eq!(creation_options(true, Some(&bare)), None);
    let sql = build_load_data_sql(
        "p.d.t",
        true,
        "  `ts` TIMESTAMP",
        Some(&p.expr),
        &[],
        creation_options(true, Some(&p)).as_deref(),
        &uris(),
    );
    assert!(
        sql.contains("PARTITION BY TIMESTAMP_TRUNC(ts, DAY)\nOPTIONS(partition_expiration_days = 400, require_partition_filter = true)\nFROM FILES"),
        "{sql}"
    );
}

#[test]
fn options_drift_alters_only_what_differs() {
    let shape = TableShape {
        partition: Some(time_key(Some("ts"), Granularity::Day)),
        cluster: vec![],
        require_partition_filter: true,
        expiration_ms: Some(400 * DAY_MS),
        bytes: None,
    };
    let same = partition_at(time_key(Some("ts"), Granularity::Day), Some(400), true);
    assert_eq!(options_drift("p.d.t", Some(&shape), Some(&same)), None);
    let shorter = partition_at(time_key(Some("ts"), Granularity::Day), Some(30), true);
    assert_eq!(
        options_drift("p.d.t", Some(&shape), Some(&shorter)).as_deref(),
        Some("ALTER TABLE `p.d.t` SET OPTIONS(partition_expiration_days = 30);")
    );
    let cleared = partition_at(time_key(Some("ts"), Granularity::Day), None, false);
    assert_eq!(
        options_drift("p.d.t", Some(&shape), Some(&cleared)).as_deref(),
        Some(
            "ALTER TABLE `p.d.t` SET OPTIONS(partition_expiration_days = NULL, require_partition_filter = false);"
        )
    );
    assert_eq!(
        options_drift("p.d.t", None, Some(&shorter)),
        None,
        "a new table took its options at creation"
    );
    assert_eq!(options_drift("p.d.t", Some(&shape), None), None);
}

#[test]
fn a_changelog_is_partitioned_like_the_table_but_never_requires_a_filter() {
    let p = partition_at(time_key(Some("ts"), Granularity::Day), Some(400), true);
    let sql = build_create_changes_sql(
        "p.d.t__changes",
        "  `ts` TIMESTAMP",
        Some(&p),
        &["id".into()],
    );
    assert!(sql.contains("PARTITION BY TIMESTAMP_TRUNC(ts, DAY)\nCLUSTER BY `id`\nOPTIONS(partition_expiration_days = 400)"), "{sql}");
    assert!(!sql.contains("require_partition_filter"), "{sql}");
    let mut ingestion = partition_at(time_key(None, Granularity::Day), Some(3), false);
    ingestion.expr = "_PARTITIONDATE".into();
    let sql = build_create_changes_sql("p.d.t__changes", "  `id` INT64", Some(&ingestion), &[]);
    assert!(sql.ends_with("PARTITION BY _PARTITIONDATE;"), "{sql}");
    assert!(
        !sql.contains("expiration"),
        "load-date partitions of a log never expire: {sql}"
    );
}

fn typed(name: &str, target_type: BqType) -> TargetColumnSpec {
    TargetColumnSpec {
        column_name: name.into(),
        target_type: target_type.into(),
        autoload_type: BqType::Bytes.into(),
        status: TargetStatus::Ok,
        note: None,
        cast_sql: None,
    }
}

#[test]
fn schema_declares_each_columns_native_target_type() {
    let s = build_schema(&[
        typed("id", BqType::Int64),
        typed("json_col", BqType::Json),
        typed("dt_col", BqType::DateTime),
    ]);
    assert!(s.contains("`id` INT64"));
    assert!(s.contains("`json_col` JSON"));
    assert!(s.contains("`dt_col` DATETIME"));
}

#[test]
fn load_data_declares_native_schema_and_is_a_free_batch_load() {
    let schema = build_schema(&[typed("id", BqType::Int64), typed("json_col", BqType::Json)]);
    let sql = build_load_data_sql("p.d.orders", true, &schema, None, &[], None, &uris());
    assert!(sql.starts_with("LOAD DATA OVERWRITE `p.d.orders` ("));
    // Native types declared inline → BigQuery coerces on load, for free.
    // Backticked (round-6): a reserved-word column must survive the DDL.
    assert!(sql.contains("`json_col` JSON"));
    assert!(sql.contains("format = 'PARQUET'"));
    assert!(sql.contains("'gs://b/a.parquet'"));
    assert!(!sql.contains("PARTITION BY"));
}

#[test]
fn load_data_append_uses_into() {
    let schema = build_schema(&[typed("id", BqType::Int64)]);
    let sql = build_load_data_sql("p.d.orders", false, &schema, None, &[], None, &uris());
    assert!(sql.starts_with("LOAD DATA INTO `p.d.orders`"));
}

#[test]
fn load_data_emits_partition_and_cluster_when_configured() {
    let schema = build_schema(&[typed("id", BqType::Int64)]);
    let sql = build_load_data_sql(
        "p.d.orders",
        true,
        &schema,
        Some("DATE(created_at)"),
        &["customer_id".into(), "region".into()],
        None,
        &uris(),
    );
    assert!(sql.contains("PARTITION BY DATE(created_at)"));
    assert!(sql.contains("CLUSTER BY `customer_id`, `region`"));
    assert!(!sql.contains("OPTIONS"));
}

#[test]
fn an_existing_tables_shape_conflicts_when_partitioning_or_clustering_differ() {
    let existing = TableShape {
        partition: Some(time_key(Some("d"), Granularity::Day)),
        cluster: vec!["v".into()],
        require_partition_filter: false,
        expiration_ms: None,
        bytes: None,
    };
    let id = vec!["id".to_string()];
    let diff = shape_conflict(&existing, None, Some(&id)).expect("differs");
    assert!(diff.contains("partitioned by `d` by day"), "{diff}");
    assert!(diff.contains("declares no partitioning"), "{diff}");
    assert!(
        diff.contains("clustered on `v`, `cluster_by` is `id`"),
        "{diff}"
    );

    let same_key = time_key(Some("D"), Granularity::Day);
    assert_eq!(
        shape_conflict(&existing, Some(&same_key), Some(&["V".to_string()])),
        None,
        "the same shape, names compared without case"
    );
    let monthly = time_key(Some("d"), Granularity::Month);
    let diff =
        shape_conflict(&existing, Some(&monthly), Some(&["v".to_string()])).expect("differs");
    assert!(
        diff.contains("partitioned by `d` by day, the load declares `d` by month"),
        "{diff}"
    );
    assert!(!diff.contains("clustered"), "{diff}");

    let with_options = TableShape {
        require_partition_filter: true,
        expiration_ms: Some(DAY_MS),
        bytes: None,
        ..existing.clone()
    };
    assert_eq!(
        shape_conflict(&with_options, Some(&same_key), Some(&["v".to_string()])),
        None,
        "options are altered in place, never a conflict"
    );
    let plain = TableShape::default();
    assert_eq!(shape_conflict(&plain, None, Some(&[])), None);
    let diff = shape_conflict(&plain, None, Some(&id)).expect("differs");
    assert!(diff.contains("clustered on nothing"), "{diff}");
    assert!(!diff.contains("partitioned"), "{diff}");
}

/// A table a 0.23 load created unclustered meets a 0.26 config whose `cluster_by`
/// defaults to `auto` (= the key): nothing was written, so the load follows the
/// table — no conflict, and the overwrite repeats the table's own clustering (which
/// BigQuery insists on). A WRITTEN `cluster_by` still conflicts. RED against the
/// pre-fix check, which compared the resolved `auto` columns like written ones.
#[test]
fn an_unwritten_cluster_by_follows_the_table_it_overwrites() {
    let unclustered = TableShape::default();
    let on_v = TableShape {
        cluster: vec!["v".into()],
        ..TableShape::default()
    };
    assert_eq!(shape_conflict(&unclustered, None, None), None);
    assert_eq!(shape_conflict(&on_v, None, None), None);
    assert!(shape_conflict(&on_v, None, Some(&["id".to_string()])).is_some());

    let auto = Clustering::Auto(vec!["id".into()]);
    let written = Clustering::Written(vec!["id".into()]);
    assert_eq!(
        table_clustering(&auto, Some(&unclustered)),
        &[] as &[String]
    );
    assert_eq!(table_clustering(&auto, Some(&on_v)), ["v"]);
    assert_eq!(
        table_clustering(&auto, None),
        ["id"],
        "a new table takes the key"
    );
    assert_eq!(table_clustering(&written, Some(&on_v)), ["id"]);
    assert_eq!(table_clustering(&written, None), ["id"]);
}

#[test]
fn an_unclustered_changelog_carries_no_cluster_clause() {
    let create = build_create_changes_sql("p.d.t__changes", "  `id` INT64", None, &[]);
    assert!(!create.contains("CLUSTER BY"), "{create}");
    assert!(
        create.ends_with(")\n;") || create.ends_with(");"),
        "{create}"
    );
}

#[test]
fn create_changes_clusters_on_pk_capped_at_four_columns() {
    let schema = build_schema(&[typed("__op", BqType::String), typed("id", BqType::Int64)]);
    let sql = build_create_changes_sql("p.d.orders__changes", &schema, None, &["id".into()]);
    assert!(sql.starts_with("CREATE TABLE IF NOT EXISTS `p.d.orders__changes` ("));
    assert!(sql.contains("CLUSTER BY `id`"));
    // A >4-column PK is capped to BigQuery's clustering limit.
    let wide: Vec<String> = ["a", "b", "c", "d", "e"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let sql2 = build_create_changes_sql("t", &schema, None, &wide);
    let bt = |c: &str| format!("`{c}`");
    assert!(sql2.contains(&format!(
        "CLUSTER BY {}, {}, {}, {}",
        bt("a"),
        bt("b"),
        bt("c"),
        bt("d")
    )));
    assert!(!sql2.contains(&bt("e")));
}

#[test]
fn is_meta_column_matches_only_the_three_cdc_columns() {
    assert!(is_meta_column("__op") && is_meta_column("__pos") && is_meta_column("__seq"));
    assert!(!is_meta_column("id") && !is_meta_column("__op_code"));
}

#[test]
fn augment_partition_limit_fires_only_on_partition_plus_signal() {
    let aug = |m: &str| augment_partition_limit(anyhow::anyhow!("{m}")).to_string();
    // partition + exactly one of {4000, quota, exceed} → augmented (pins each `||`).
    assert!(aug("too many partitions, allowed 4000").contains("split the"));
    assert!(aug("partition quota reached").contains("split the"));
    assert!(aug("partition count will exceed the limit").contains("split the"));
    // partition alone, or a signal alone → NOT augmented (pins the outer `&&`).
    assert!(!aug("partition pruning is disabled").contains("split the"));
    assert!(!aug("row quota 4000 reached").contains("split the"));
}

#[test]
fn partition_limit_error_is_augmented() {
    let raw = anyhow::anyhow!("Too many partitions: cannot modify more than 4000 partitions");
    let msg = augment_partition_limit(raw).to_string();
    assert!(
        msg.contains("split the"),
        "expected the actionable hint: {msg}"
    );
}

/// The label SET is the cost-attribution contract; the transport that
/// carries it is not. This asserted `--label k:v` CLI pairs before the REST
/// rewrite — same keys, same (sanitized) values, now read as a map.
#[test]
fn job_labels_tag_managed_by_op_and_table() {
    let labels = build_labels("recover", "Orders", Some("Run-7"));
    assert_eq!(labels["managed_by"], "rivet");
    assert_eq!(labels["rivet_op"], "recover");
    // Case-folding is LOSSY, and case is the live-proven collision shape
    // (`CaseTwin` beside `casetwin`), so a folded value carries a digest of the
    // original — see `sanitize_label_does_not_collapse_two_distinct_tables_into_one`.
    // The folded form stays the PREFIX, so the label still reads as the table.
    assert!(labels["rivet_table"].starts_with("orders"), "{labels:?}");
    assert!(labels["rivet_run"].starts_with("run-7"), "{labels:?}");
    assert_ne!(
        labels["rivet_table"], "orders",
        "a table named `Orders` must not bill under the same label as one named \
         `orders`: {labels:?}"
    );
    assert_eq!(
        labels.len(),
        4,
        "no label beyond the documented four: {labels:?}"
    );
}

#[test]
fn no_run_id_omits_the_rivet_run_label() {
    let labels = build_labels("load", "orders", None);
    assert_eq!(labels["rivet_table"], "orders");
    assert!(!labels.contains_key("rivet_run"), "{labels:?}");
}

/// The labels the loader actually SENDS, taken from the loader (not
/// hand-built), and placed in the body BigQuery reads them from. The
/// producer-side half of the label contract: a run id that never reached
/// `configuration.labels` is a silent loss of cost attribution — every job
/// still runs, and the billing query returns nothing for the run.
#[test]
fn the_loader_sends_its_labels_in_the_job_configuration() {
    let l = BigQueryLoader::new("p", "d").run_id("Run-9");
    let body = crate::load::bq_rest::query_job_body(
        "SELECT 1",
        &l.labels("load", "Orders"),
        "p",
        None,
        "rivet_job",
    );
    assert_eq!(body["configuration"]["labels"]["managed_by"], "rivet");
    assert_eq!(body["configuration"]["labels"]["rivet_op"], "load");
    // Folded values carry a disambiguating digest (see `sanitize_label`), so the
    // transport assertion pins the PREFIX — what it exists to check is that the
    // label set reaches the job configuration, not how a value is spelled.
    for (key, want) in [("rivet_table", "orders"), ("rivet_run", "run-9")] {
        let got = body["configuration"]["labels"][key]
            .as_str()
            .unwrap_or_else(|| panic!("{key} missing: {body}"));
        assert!(got.starts_with(want), "{key}: {got}");
    }
}

#[test]
fn fqtn_qualifies_project_dataset_table() {
    let l = BigQueryLoader::new("proj", "ds");
    assert_eq!(l.fqtn("orders"), "proj.ds.orders");
}

#[test]
fn sanitize_label_coerces_to_bq_charset() {
    // Unchanged by the mapping → the exact string, so ordinary labels stay readable.
    assert_eq!(sanitize_label("ok-name_1"), "ok-name_1");
    assert_eq!(sanitize_label(""), "unnamed");

    // Altered by the mapping → the folded form PLUS a digest of the original.
    let folded = sanitize_label("My.Table!");
    assert!(folded.starts_with("my_table_"), "{folded}");
    assert!(
        folded.len() > "my_table_".len(),
        "a lossy fold must carry a disambiguator: {folded}"
    );

    // Within BigQuery's cap, always.
    assert!(sanitize_label(&"x".repeat(80)).len() <= 63);
    assert!(folded.len() <= 63);
}

/// Two distinct tables never share a `rivet_table` label.
///
/// The value is the per-table IDENTITY the cost query in this module's header
/// GROUPs by, and the mapping is lossy three ways: case-folding, `[^a-z0-9_-] → _`,
/// and the 63-char cap. Two tables folding to one label reported their jobs, bytes
/// and spend as ONE line with nothing indicating the merge — a confidently wrong
/// answer, which is worse than a missing one.
///
/// None of these inputs is exotic. `"CaseTwin"` beside `casetwin` is a shape
/// `yaml_scaffold` records as live-proven on PostgreSQL; MSSQL's `sysname` is 128
/// chars and a Mongo collection name ~235, so two names agreeing on their first 63
/// sanitized characters are ordinary.
///
/// Scope, kept from the hunt's refuters: nothing in rivet READS these labels back,
/// so the harm was bounded to cost attribution — no data or control flow moved.
///
/// RED against returning the folded form alone.
#[test]
fn sanitize_label_does_not_collapse_two_distinct_tables_into_one() {
    let pairs = [
        // case twins — the live-proven shape
        ("CaseTwin", "casetwin"),
        // differ only in a character the charset forbids
        ("orders.eu", "orders-eu"),
        // agree on the first 63 characters, differ past the cap
        (
            &format!("{}_2024_01", "history_archive".repeat(4)),
            &format!("{}_2024_02", "history_archive".repeat(4)),
        ),
    ];
    for (a, b) in pairs {
        assert_ne!(
            sanitize_label(a),
            sanitize_label(b),
            "`{a}` and `{b}` are different tables and must bill separately"
        );
        assert!(sanitize_label(a).len() <= 63 && sanitize_label(b).len() <= 63);
    }
    // Stable: the same input always yields the same label, or cost queries would
    // scatter one table across several lines instead of merging two into one.
    assert_eq!(sanitize_label("My.Table!"), sanitize_label("My.Table!"));
}

/// THE foreign-table safety test. A table rivet did not create keeps its
/// previous owner's shape — `CREATE TABLE IF NOT EXISTS` is a no-op on it —
/// so the only way to add a column is ALTER. A replace would impose our
/// schema on the customer's history and destroy it.
#[test]
fn schema_reconciliation_adds_columns_and_never_replaces() {
    let specs = [
        typed("id", BqType::Int64),
        typed("_rivet_row_hash", BqType::Int64),
    ];
    let sql = build_alter_add_columns_sql("p.d.t__changes", &specs).unwrap();
    assert!(sql.starts_with("ALTER TABLE `p.d.t__changes`"), "{sql}");
    // IF NOT EXISTS on every column: the statement runs on every load, and
    // a load must not fail because a column it declares is already there.
    assert_eq!(sql.matches("ADD COLUMN IF NOT EXISTS").count(), 2, "{sql}");
    assert!(sql.contains("`_rivet_row_hash` INT64"), "{sql}");
    for forbidden in ["REPLACE", "DROP", "CREATE", "TRUNCATE", "OVERWRITE"] {
        assert!(
            !sql.contains(forbidden),
            "reconciliation must be additive only, found {forbidden}: {sql}"
        );
    }
}

/// Nothing to add ⇒ no statement, so the loader skips the round trip
/// instead of sending `ALTER TABLE t ;`.
#[test]
fn schema_reconciliation_emits_nothing_for_an_empty_spec_list() {
    assert!(build_alter_add_columns_sql("p.d.t", &[]).is_none());
}

/// The changelog is only ever CREATEd IF NOT EXISTS and LOADed INTO —
/// never OVERWRITE. This pins the pairing: a pre-existing `__changes` table
/// must survive a load with its rows intact.
#[test]
fn changelog_sql_is_create_if_not_exists_plus_append_only() {
    let create = build_create_changes_sql("p.d.t__changes", "  `id` INT64", None, &["id".into()]);
    assert!(create.starts_with("CREATE TABLE IF NOT EXISTS"), "{create}");
    let load = build_load_data_sql(
        "p.d.t__changes",
        false,
        "  `id` INT64",
        None,
        &[],
        None,
        &uris(),
    );
    assert!(load.starts_with("LOAD DATA INTO"), "{load}");
    assert!(!load.contains("OVERWRITE"), "{load}");
}

#[test]
fn materialize_refuses_too_many_cluster_columns() {
    // A >4-column CLUSTER BY is a below-the-seam adapter limit (BigQuery's),
    // caught in `materialize` before any BigQuery job is enqueued. (Empty-URI and Fail-spec
    // refusals are the driver's — see `load::tests`.)
    let l = BigQueryLoader::new("p", "d").clustered_on(vec![
        "a".into(),
        "b".into(),
        "c".into(),
        "d".into(),
        "e".into(),
    ]);
    let err = l
        .materialize("t", &[spec("id", None, TargetStatus::Ok)], &uris(), None)
        .unwrap_err()
        .to_string();
    assert!(err.contains("clustering"), "{err}");
}

#[test]
fn materialize_refuses_a_non_identifier_cluster_column() {
    // A clustering column splices raw into `CLUSTER BY <cols>`; a
    // non-identifier name is an injection vector and must be refused in
    // `materialize` before any BigQuery job is enqueued — the sibling of the table/column/pk
    // gate for the BigQuery shape clause.
    let l = BigQueryLoader::new("p", "d").clustered_on(vec!["id) FROM secrets; --".into()]);
    let err = l
        .materialize("t", &[spec("id", None, TargetStatus::Ok)], &uris(), None)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("not a plain SQL identifier") && err.contains("CLUSTER BY"),
        "{err}"
    );
}

/// The stand's permanent BigQuery dataset (`RIVET_BQ_TEST_DATASET` overrides) and its GCS bucket, from dev/stand/registry.yaml.
fn stand_dataset_and_bucket() -> (String, String) {
    let r: serde_yaml_ng::Value =
        serde_yaml_ng::from_str(include_str!("../../../dev/stand/registry.yaml"))
            .expect("registry");
    let dataset = std::env::var("RIVET_BQ_TEST_DATASET").unwrap_or_else(|_| {
        r["bigquery"]["e2e"]
            .as_str()
            .expect("bigquery.e2e")
            .to_string()
    });
    (
        dataset,
        r["gcs"]["bucket"].as_str().expect("gcs.bucket").to_string(),
    )
}

/// Writes the rows of `select` as one Parquet object through BigQuery's own `EXPORT DATA`; its URI.
fn export_fixture(loader: &BigQueryLoader, bucket: &str, name: &str, select: &str) -> String {
    loader
        .run_sql(
            &format!(
                "EXPORT DATA OPTIONS (uri = 'gs://{bucket}/rivet-live/bq-unit/{name}/*.parquet', \
                 format = 'PARQUET', overwrite = true) AS {select}"
            ),
            "fixture",
            name,
        )
        .expect("export the Parquet fixture");
    format!("gs://{bucket}/rivet-live/bq-unit/{name}/000000000000.parquet")
}

/// Live BigQuery load of a Parquet fixture the test writes itself. Needs ADC and
/// `BIGQUERY_TEST_PROJECT`; the transport is the REST API, so no `bq` CLI on PATH.
#[test]
#[ignore = "live: needs a BigQuery project + ADC"]
fn bigquery_live_load_round_trips() {
    // Soft-skip when the live BigQuery project isn't configured: CI sweeps
    // `--ignored` (ci.yml) without warehouse creds, so a hard `.expect` here
    // would fail the run. With the project set (a live/nightly box) it runs.
    let Ok(project) = std::env::var("BIGQUERY_TEST_PROJECT") else {
        return crate::test_hook::skip_live("BIGQUERY_TEST_PROJECT unset");
    };
    let (dataset, bucket) = stand_dataset_and_bucket();
    let table = format!("rivet_bq_live_test_{}_0", std::process::id());
    let loader = BigQueryLoader::new(project, dataset);
    let uri = export_fixture(
        &loader,
        &bucket,
        "load_round_trips",
        "SELECT id FROM UNNEST([1, 2, 3]) AS id",
    );

    // A plain column (no cast) exercises the FREE LOAD DATA path.
    let specs = vec![typed("id", BqType::Int64)];

    // Drive it through the real driver (no gate, no cleanup) — same path prod
    // takes, exercising validate → materialize.
    let report = crate::load::run_load(
        &loader,
        &table,
        &specs,
        &[uri],
        None,
        None,
        crate::load::Ownership::Own,
    );
    let read_back = loader.row_count(&table);
    let _ = loader.run_sql(
        &format!("DROP TABLE IF EXISTS `{}`", loader.fqtn(&table)),
        "cleanup",
        &table,
    );
    assert_eq!(report.expect("live load should succeed").rows_loaded, 3);
    assert_eq!(read_back.expect("count the loaded table"), 3);
}

/// THE live proof of the REST transport, needing no GCS fixture: a real
/// query job through `jobs.insert` → poll → `getQueryResults`, then the
/// cost-attribution labels read back from BigQuery's OWN catalog
/// (`INFORMATION_SCHEMA.JOBS_BY_PROJECT`) rather than from the request body
/// this crate built — an independent oracle for the one contract the CLI
/// rewrite could silently drop. Also drives the failure path, so the error
/// mapping is exercised against a real `status.errorResult` and not only a
/// fixture. Drive it with:
///
///   BIGQUERY_TEST_PROJECT=rivet-data-tool RIVET_BQ_TEST_DATASET=rivet_type_lab \
///   BIGQUERY_TEST_LOCATION=EU \
///   cargo test --lib -- --ignored bigquery_rest_transport_live
#[test]
#[ignore = "live: needs a BigQuery project + ADC (no GCS fixture required)"]
fn bigquery_rest_transport_live_round_trips_a_query_job() {
    // Soft-skip when unconfigured — see bigquery_live_load_round_trips.
    let Ok(project) = std::env::var("BIGQUERY_TEST_PROJECT") else {
        return crate::test_hook::skip_live("BIGQUERY_TEST_PROJECT unset");
    };
    let (dataset, _) = stand_dataset_and_bucket();
    let region = std::env::var("BIGQUERY_TEST_LOCATION")
        .unwrap_or_else(|_| "US".to_string())
        .to_lowercase();

    // A run id unique to this invocation, so the label read-back below is
    // scoped to THIS run's jobs and cannot be satisfied by history.
    let run_id = format!(
        "rest-live-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    let table = "rivet_bq_rest_live";
    let loader = BigQueryLoader::new(&project, &dataset).run_id(&run_id);
    let fqtn = loader.fqtn(table);

    // 1. A statement job: insert → poll → terminal verdict.
    loader
        .run_sql(
            &format!("CREATE OR REPLACE TABLE `{fqtn}` AS SELECT 1 AS id UNION ALL SELECT 2"),
            "create",
            table,
        )
        .expect("CREATE OR REPLACE through the REST transport");

    // 2. A scalar job: the getQueryResults leg, against a count this test
    //    seeded itself (not one rivet reported).
    assert_eq!(
        loader
            .api()
            .unwrap()
            .run_query_scalar(
                &format!("SELECT COUNT(*) AS n FROM `{fqtn}`"),
                &loader.labels("count", table)
            )
            .expect("count over REST"),
        2,
        "the count must come back from getQueryResults"
    );
    assert_eq!(
        loader.count_rows(table).expect("numRows over REST"),
        2,
        "…and the same count from tables.get metadata"
    );

    // 3. The labels, read back from BigQuery's catalog. `run_id` is unique
    //    per invocation, so a nonzero count can only come from the jobs
    //    THIS test just ran.
    let labelled = loader
        .api()
        .unwrap()
        .run_query_scalar(
            &format!(
                "SELECT COUNT(*) FROM `{project}`.`region-{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT \
                 WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) \
                 AND EXISTS (SELECT 1 FROM UNNEST(labels) WHERE key = 'rivet_run' AND value = '{run_id}') \
                 AND EXISTS (SELECT 1 FROM UNNEST(labels) WHERE key = 'managed_by' AND value = 'rivet')",
            ),
            &loader.labels("audit", table),
        )
        .expect("reading INFORMATION_SCHEMA.JOBS_BY_PROJECT");
    assert!(
        labelled >= 2,
        "the run's jobs must carry rivet_run:{run_id} + managed_by:rivet in \
         configuration.labels — INFORMATION_SCHEMA saw {labelled}"
    );

    // 4. The failure path: a real `status.errorResult` must reach the caller
    //    with BigQuery's own reason text, not a bare "failed".
    let err = loader
        .run_sql(
            &format!("SELECT * FROM `{project}.{dataset}.no_such_table_ever`"),
            "probe",
            table,
        )
        .expect_err("a missing table must fail the job");
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("Not found") && rendered.contains("no_such_table_ever"),
        "the REST error detail must name the reason: {rendered}"
    );

    // 5. Clean up after ourselves.
    loader
        .run_sql(&format!("DROP TABLE IF EXISTS `{fqtn}`"), "drop", table)
        .expect("dropping the live fixture table");
}

/// Live BigQuery CDC round-trip: append a change-log Parquet into
/// `<table>__changes` and build the dedup view. Loading the **same** file
/// twice exercises the at-least-once path — `<table>__changes` doubles, but
/// the current-state view must be unchanged (duplicates lose the
/// `(__pos,__seq)` tiebreak). Soft delete: the view keeps one row per PK
/// including tombstones (`__is_deleted = true`). The fixture is a change log the
/// test writes itself: id 1 inserted then updated, id 2 inserted then deleted,
/// id 3 inserted, so the view holds 3 keys (incl. the tombstone).
#[test]
#[ignore = "live: needs a BigQuery project + ADC"]
fn bigquery_live_cdc_view_dedups_at_least_once() {
    // Soft-skip when unconfigured — see bigquery_live_load_round_trips.
    let Ok(project) = std::env::var("BIGQUERY_TEST_PROJECT") else {
        return crate::test_hook::skip_live("BIGQUERY_TEST_PROJECT unset");
    };
    let (dataset, bucket) = stand_dataset_and_bucket();
    let specs: Vec<TargetColumnSpec> = [("id", "INT64"), ("val", "STRING")]
        .into_iter()
        .map(|(name, ty)| {
            let ty = crate::types::parse_type_str(ty).expect("a column type");
            crate::types::target::ExportTarget::BigQuery.resolve_column(
                crate::types::target::TargetInput {
                    column_name: name,
                    rivet_type: &ty,
                    arrow_type: None,
                    fidelity: crate::types::TypeFidelity::Exact,
                },
            )
        })
        .collect();

    let table = format!("rivet_bq_live_cdc_test_{}_0", std::process::id());
    let table = table.as_str();
    let pk_cols = vec!["id".to_string()];
    let loader = BigQueryLoader::new(&project, &dataset);
    let uri = export_fixture(
        &loader,
        &bucket,
        "cdc_view_dedups",
        "SELECT * FROM UNNEST([\
         STRUCT('insert' AS __op, '1' AS __pos, 1 AS __seq, 1 AS id, 'a' AS val), \
         ('update', '2', 2, 1, 'b'), ('insert', '3', 3, 2, 'c'), \
         ('delete', '4', 4, 2, 'c'), ('insert', '5', 5, 3, 'd')])",
    );

    // Load the same change log twice (at-least-once).
    crate::load::run_load_cdc(
        &loader,
        table,
        &specs,
        std::slice::from_ref(&uri),
        &pk_cols,
        crate::load::cdc::SourceEngine::MySql,
        None,
        None,
        crate::load::Ownership::Own,
        false,
    )
    .expect("first CDC append + view build should succeed");
    let second = crate::load::run_load_cdc(
        &loader,
        table,
        &specs,
        &[uri],
        &pk_cols,
        crate::load::cdc::SourceEngine::MySql,
        None,
        None,
        crate::load::Ownership::Own,
        false,
    )
    .expect("second CDC append (at-least-once) should succeed");
    assert!(second.rows_appended > 0, "second append added rows");

    // The dedup VIEW must report the current state, independent of how many
    // times the log was appended.
    let state_rows = loader
        .api()
        .unwrap()
        .run_query_scalar(
            &format!("SELECT COUNT(*) AS n FROM `{}`", second.target),
            &loader.labels("count", table),
        )
        .expect("counting the dedup view should succeed");
    for drop in [
        format!("DROP VIEW IF EXISTS `{}`", second.target),
        format!(
            "DROP TABLE IF EXISTS `{}`",
            loader.fqtn(&format!("{table}__changes"))
        ),
    ] {
        let _ = loader.run_sql(&drop, "cleanup", table);
    }
    assert_eq!(
        state_rows, 3,
        "the view must collapse duplicates to 3 distinct-PK rows (incl tombstones), got {state_rows}"
    );
}

#[test]
#[ignore = "live: requires BIGQUERY_TEST_PROJECT"]
fn bigquery_live_adopts_a_full_load_table_as_the_changelog_baseline() {
    let Ok(project) = std::env::var("BIGQUERY_TEST_PROJECT") else {
        return crate::test_hook::skip_live("BIGQUERY_TEST_PROJECT unset");
    };
    let (dataset, _) = stand_dataset_and_bucket();
    let loader = BigQueryLoader::new(&project, &dataset);
    let table = format!("rivet_bq_live_adopt_{}_0", std::process::id());
    let changes = format!("{table}__changes");
    let (fq, changes_fq) = (loader.fqtn(&table), loader.fqtn(&changes));
    let probe = format!("{table}_probe");
    let probe_fq = loader.fqtn(&probe);
    let fixture = |fqtn: &str| {
        loader.run_sql(
            &format!(
                "CREATE OR REPLACE TABLE `{fqtn}` AS \
                 SELECT id, CONCAT('v', CAST(id AS STRING)) AS v FROM UNNEST([1, 2, 3]) AS id"
            ),
            "fixture",
            &table,
        )
    };

    fixture(&probe_fq).expect("probe table");
    let collision = loader.create_view(
        &probe,
        &format!("CREATE OR REPLACE VIEW `{probe_fq}` AS SELECT 1 AS id"),
    );
    let probe_kind = loader.object_kind(&probe);
    let _ = loader.run_sql(
        &format!("DROP TABLE IF EXISTS `{probe_fq}`"),
        "cleanup",
        &probe,
    );
    let _ = loader.run_sql(
        &format!("DROP VIEW IF EXISTS `{probe_fq}`"),
        "cleanup",
        &probe,
    );
    eprintln!(
        "view over a full-load table without adoption: {collision:?}, kind after: {probe_kind:?}"
    );
    assert!(
        collision.is_err(),
        "CREATE OR REPLACE VIEW over a table must fail, not replace it"
    );
    assert_eq!(probe_kind.unwrap(), crate::load::ObjectKind::Table);

    fixture(&fq).expect("fixture table");
    let before = loader.object_kind(&table);
    let specs = [typed("id", BqType::Int64), typed("v", BqType::String)];
    let adopted = crate::load::adopt_full_load_table(
        &loader,
        &table,
        &specs,
        crate::load::Ownership::Own,
        false,
    );
    let view_sql = crate::load::cdc::inc_dedup_view_sql(
        crate::load::cdc::Warehouse::BigQuery,
        &fq,
        &changes_fq,
        &["id"],
        "id",
    );
    let view = adopted
        .as_ref()
        .ok()
        .map(|_| loader.create_view(&table, &view_sql));
    let after = loader.object_kind(&table);
    let copied = loader.row_count(&changes);
    let viewed = loader.row_count(&table);
    for drop in [
        format!("DROP VIEW IF EXISTS `{fq}`"),
        format!("DROP TABLE IF EXISTS `{fq}`"),
        format!("DROP TABLE IF EXISTS `{changes_fq}`"),
    ] {
        let _ = loader.run_sql(&drop, "cleanup", &table);
    }

    assert_eq!(before.unwrap(), crate::load::ObjectKind::Table);
    assert_eq!(adopted.unwrap(), Some(3));
    view.unwrap().unwrap();
    assert_eq!(after.unwrap(), crate::load::ObjectKind::View);
    assert_eq!(copied.unwrap(), 3);
    assert_eq!(viewed.unwrap(), 3);
}

/// The partition cap is the TARGET table's: a disposable buffer is never
/// partitioned, so its append packs into one job without opening the footer
/// store — whatever the base declares. The `Some` arm proves the fixture is not
/// inert: the same loader, asked to pack under the base's partition, does reach
/// for the footers (and fails on the bogus store).
#[test]
fn an_unpartitioned_target_packs_into_one_job_without_reading_footers() {
    let dest: crate::config::DestinationConfig =
        serde_yaml_ng::from_str("type: gcs\nbucket: nonexistent-bughunt\nprefix: x").unwrap();
    let partition = partition_at(time_key(Some("ts"), Granularity::Day), None, false);
    let loader = BigQueryLoader::new("p", "d")
        .partition(partition.clone())
        .batched_by_footers(dest);
    let uris = uris();
    assert_eq!(loader.batches(&uris, None).unwrap(), vec![uris.clone()]);
    assert!(loader.batches(&uris, Some(&partition)).is_err());
}

/// The compaction script ends with `(changes_rows, merge_jobs)`; a row that does
/// not read as that pair is an error, never a report identical to an empty
/// buffer's.
#[test]
fn an_unreadable_compaction_summary_is_an_error_not_an_empty_report() {
    let ok = compact_summary(&[Some("12".into()), Some("2".into())]).unwrap();
    assert_eq!(ok, (12, 2));
    for row in [
        vec![],
        vec![Some("12".into())],
        vec![None, Some("2".into())],
        vec![Some("many".into()), Some("2".into())],
    ] {
        let e = compact_summary(&row).unwrap_err().to_string();
        assert!(
            e.contains("buffer dropped") && e.contains("summary row"),
            "{e}"
        );
    }
}

#[test]
fn a_renamed_column_loads_under_its_file_name_then_renames_and_appends_by_name() {
    let specs = vec![
        spec("id", None, TargetStatus::Ok),
        spec("comment", None, TargetStatus::Ok),
    ];
    let renames = vec![("\u{441}omment".to_string(), "comment".to_string())];
    let schema = build_file_schema(&specs, &renames);
    assert!(schema.contains("`\u{441}omment`"), "{schema}");
    assert!(
        !schema.contains("`comment`"),
        "the file name, not the warehouse name: {schema}"
    );
    assert_eq!(
        build_rename_columns_sql("p.d.t__staging", &renames).as_deref(),
        Some("ALTER TABLE `p.d.t__staging` RENAME COLUMN `\u{441}omment` TO `comment`;")
    );
    assert_eq!(build_rename_columns_sql("p.d.t__staging", &[]), None);
}

#[test]
fn only_one_job_with_nothing_to_rename_loads_straight_into_the_target() {
    let one = vec![vec!["gs://b/p0.parquet".to_string()]];
    let two = vec![one[0].clone(), vec!["gs://b/p1.parquet".to_string()]];
    let renames = vec![("\u{441}omment".to_string(), "comment".to_string())];
    assert!(loads_directly(&[], &one));
    assert!(loads_directly(&[], &[]));
    assert!(
        !loads_directly(&[], &two),
        "several jobs cannot OVERWRITE one table"
    );
    assert!(
        !loads_directly(&renames, &one),
        "a rename needs the staging table"
    );
}

#[test]
fn an_empty_full_load_truncates_an_existing_table_and_creates_a_missing_one() {
    assert_eq!(
        build_empty_table_sql("p.d.t", true, "  `id` INT64", Some("DATE(ts)"), &[], None),
        "TRUNCATE TABLE `p.d.t`;"
    );
    assert_eq!(
        build_empty_table_sql(
            "p.d.t",
            false,
            "  `id` INT64",
            None,
            &["id".to_string()],
            None
        ),
        "CREATE TABLE `p.d.t` (\n  `id` INT64\n)\nCLUSTER BY `id`;"
    );
}

#[test]
fn an_empty_run_says_whether_it_emptied_the_table_or_created_it() {
    assert_eq!(
        empty_run_note("p.d.t", true),
        "  note: the newest run exported 0 rows — `p.d.t` is emptied to match"
    );
    assert_eq!(
        empty_run_note("p.d.t", false),
        "  note: the newest run exported 0 rows — `p.d.t` is created empty"
    );
}

#[test]
fn a_compaction_with_neither_buffer_nor_leftover_has_nothing_to_merge() {
    assert!(nothing_to_compact(false, false));
    assert!(!nothing_to_compact(true, false), "a live buffer is merged");
    assert!(
        !nothing_to_compact(false, true),
        "a leftover of a merge that died is merged even with no live buffer"
    );
}

// ── the loader against a scripted BigQuery (bq_rest::stand) ─────────────────

use crate::load::ShapeControl;
use crate::load::bq_rest::stand::{Stand, json};

/// How a scripted query job ends.
enum Job {
    /// Done, and its result is this one number.
    Scalar(u64),
    /// Done with BigQuery's error.
    Fails(&'static str),
}

/// A scripted project `p`, dataset `d`: `tables` are (name, `tables.get` resource), a name not listed is absent, and each query job ends as `job` says for its statement.
fn world(tables: &'static [(&'static str, &'static str)], job: fn(&str) -> Job) -> Stand {
    let results = std::sync::Mutex::new(std::collections::HashMap::<String, u64>::new());
    Stand::start(move |seen, _| {
        let table = seen
            .path
            .strip_prefix("/bigquery/v2/projects/p/datasets/d/tables/");
        match (seen.method.as_str(), table) {
            ("GET", Some(name)) if name.ends_with("/rowAccessPolicies") => return json(200, "{}"),
            ("GET", Some(name)) => {
                let found = tables.iter().find(|(t, _)| *t == name);
                return found.map_or(
                    json(404, r#"{"error":{"message":"Not found"}}"#),
                    |(_, r)| json(200, r),
                );
            }
            ("PATCH", Some(_)) => return json(200, "{}"),
            _ => {}
        }
        if let Some(id) = seen.path.strip_prefix("/bigquery/v2/projects/p/queries/") {
            let n = results.lock().unwrap().get(id).copied().unwrap_or(0);
            return json(200, &format!(r#"{{"rows":[{{"f":[{{"v":"{n}"}}]}}]}}"#));
        }
        let id = seen.body["jobReference"]["jobId"]
            .as_str()
            .unwrap_or("none")
            .to_string();
        let sql = seen.body["configuration"]["query"]["query"]
            .as_str()
            .unwrap_or("");
        let status = match job(sql) {
            Job::Scalar(n) => {
                results.lock().unwrap().insert(id.clone(), n);
                r#"{"state":"DONE"}"#.to_string()
            }
            Job::Fails(why) => format!(r#"{{"state":"DONE","errorResult":{{"message":"{why}"}}}}"#),
        };
        json(
            200,
            &format!(r#"{{"jobReference":{{"projectId":"p","jobId":"{id}"}},"status":{status}}}"#),
        )
    })
}

/// A loader of `p.d` whose client is the stand's.
fn on(stand: &Stand) -> BigQueryLoader {
    let loader = BigQueryLoader::new("p", "d");
    assert!(loader.api.set(stand.api()).is_ok());
    loader
}

/// Every statement the stand was sent, in order.
fn statements(stand: &Stand) -> Vec<String> {
    let sql = |r: &crate::load::bq_rest::stand::Seen| {
        r.body["configuration"]["query"]["query"]
            .as_str()
            .map(String::from)
    };
    stand.seen().iter().filter_map(sql).collect()
}

/// A change log holds prior changes when its probe counts one; a log that is not there is the first cycle, and any other failure is a failure.
#[test]
fn a_change_log_has_prior_changes_only_when_its_probe_counts_one() {
    let stand = world(&[], |sql| match sql {
        s if s.contains("p.d.some__changes") => Job::Scalar(1),
        s if s.contains("p.d.none__changes") => Job::Scalar(0),
        s if s.contains("p.d.absent__changes") => {
            Job::Fails("Not found: Table p:d.absent__changes was not found")
        }
        _ => Job::Fails("Access Denied: Table p:d.denied__changes"),
    });
    let loader = on(&stand);
    assert!(loader.changes_has_prior_changes("some").unwrap());
    assert!(!loader.changes_has_prior_changes("none").unwrap());
    assert!(!loader.changes_has_prior_changes("absent").unwrap());
    let denied = format!(
        "{:#}",
        loader.changes_has_prior_changes("denied").unwrap_err()
    );
    assert!(denied.contains("Access Denied"), "{denied}");
    assert!(statements(&stand)[0].contains("WHERE __op IS NOT NULL LIMIT 1"));
}

/// A table's rows are its metadata count, with no job; a view's are counted by a query.
#[test]
fn rows_are_read_from_table_metadata_and_counted_only_for_a_view() {
    let stand = world(
        &[
            ("t", r#"{"type":"TABLE","numRows":"42"}"#),
            ("v", r#"{"type":"VIEW","numRows":"0"}"#),
        ],
        |_| Job::Scalar(7),
    );
    let loader = on(&stand);
    assert_eq!(loader.row_count("t").unwrap(), 42);
    assert!(
        statements(&stand).is_empty(),
        "a table's count costs no job"
    );
    assert_eq!(loader.row_count("v").unwrap(), 7);
    assert_eq!(statements(&stand), ["SELECT COUNT(*) AS n FROM `p.d.v`"]);
}

/// A statement is sent as written under the operation and table it serves, and its failure is the caller's.
#[test]
fn a_statement_is_sent_under_its_operation_and_table() {
    let stand = world(&[], |sql| match sql.contains("BROKEN") {
        true => Job::Fails("Syntax error"),
        false => Job::Scalar(0),
    });
    let loader = on(&stand).run_id("r1");
    loader
        .run_sql("CREATE TABLE `p.d.x` (a INT64)", "merge", "orders")
        .unwrap();
    let sent = &stand.seen()[0].body["configuration"];
    assert_eq!(sent["query"]["query"], "CREATE TABLE `p.d.x` (a INT64)");
    assert_eq!(sent["labels"]["rivet_op"], "merge");
    assert_eq!(sent["labels"]["rivet_table"], "orders");
    assert!(loader.run_sql("BROKEN", "load", "orders").is_err());
}

/// The shape of an existing table is read from its metadata; no table has no shape and no conflict.
#[test]
fn the_existing_shape_is_the_tables_own_and_an_absent_table_has_none() {
    let stand = world(
        &[(
            "t",
            r#"{"type":"TABLE","numRows":"1","clustering":{"fields":["a_col"]}}"#,
        )],
        |_| Job::Scalar(0),
    );
    let loader = on(&stand);
    assert_eq!(
        loader.existing_shape("t").unwrap().unwrap().cluster,
        ["a_col"]
    );
    assert_eq!(loader.existing_shape("absent").unwrap(), None);
    assert_eq!(loader.table_shape_conflict("absent").unwrap(), None);
    assert_eq!(
        loader.table_shape_conflict("t").unwrap(),
        None,
        "nothing was asked of its shape"
    );
    assert!(loader.shape().is_some(), "BigQuery controls shape");
    let mut asked = on(&stand);
    asked.clustering = Clustering::Written(vec!["b_col".into()]);
    let conflict = asked.table_shape_conflict("t").unwrap();
    assert_eq!(
        conflict.as_deref(),
        Some("it is clustered on `a_col`, `cluster_by` is `b_col`")
    );
}

/// A recluster is one metadata patch of the change log with the written columns.
#[test]
fn a_recluster_patches_the_change_log_with_the_written_columns() {
    let stand = world(&[], |_| Job::Scalar(0));
    let mut loader = on(&stand);
    loader.clustering = Clustering::Written(vec!["a".into(), "b".into()]);
    loader.recluster_changelog("t").unwrap();
    let patch = stand
        .seen()
        .into_iter()
        .find(|r| r.method == "PATCH")
        .expect("a PATCH");
    assert_eq!(
        patch.path,
        "/bigquery/v2/projects/p/datasets/d/tables/t__changes"
    );
    assert_eq!(
        patch.body["clustering"]["fields"],
        serde_json::json!(["a", "b"])
    );
    assert!(statements(&stand).is_empty(), "no job");
}

/// A rebuilt copy that does not hold the log's rows is left beside it: nothing is swapped.
#[test]
fn a_rebuild_whose_copy_lost_rows_swaps_nothing() {
    let run = |copy_rows: &'static str| {
        let tables: &'static [(&'static str, &'static str)] = match copy_rows {
            "5" => &[
                ("t__changes", r#"{"type":"TABLE","numRows":"5"}"#),
                ("t__changes__rebuild", r#"{"type":"TABLE","numRows":"5"}"#),
            ],
            _ => &[
                ("t__changes", r#"{"type":"TABLE","numRows":"5"}"#),
                ("t__changes__rebuild", r#"{"type":"TABLE","numRows":"4"}"#),
            ],
        };
        let stand = world(tables, |_| Job::Scalar(0));
        let result = on(&stand)
            .rebuild_changelog("t")
            .map_err(|e| format!("{e:#}"));
        (result, statements(&stand))
    };
    let (lost, sent) = run("4");
    let why = lost.unwrap_err();
    assert!(
        why.contains("holds 4 rows") && why.contains("nothing was swapped"),
        "{why}"
    );
    assert_eq!(sent.len(), 1, "the copy and no swap: {sent:?}");
    let (kept, sent) = run("5");
    assert_eq!(kept, Ok(()));
    assert!(sent.len() > 1, "the copy, then the swap: {sent:?}");
}

/// The clustering list is checked before anything is sent, and the partition expression is the configured one.
#[test]
fn a_bad_clustering_list_is_refused_and_the_partition_expression_is_the_plans() {
    let mut loader = BigQueryLoader::new("p", "d");
    assert_eq!(loader.partition_expr(), None);
    loader.check_cluster_by().unwrap();
    loader.clustering = Clustering::Written(vec!["a; DROP".into()]);
    assert!(loader.check_cluster_by().is_err());
    let part = TablePartition {
        key: crate::load::plan::PartitionKey::Time {
            column: Some("ts".into()),
            granularity: crate::load::plan::Granularity::Day,
        },
        expr: "DATE(ts)".into(),
        expiration_days: None,
        require_filter: false,
    };
    assert_eq!(loader.partition(part).partition_expr(), Some("DATE(ts)"));
}

/// A re-baseline buffers one tombstone per live base row its buffer does not hold, over the
/// source columns only, and reports the rows the buffer gained.
#[test]
fn a_rebaseline_buffers_a_tombstone_for_each_base_row_the_buffer_does_not_hold() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let inserted = std::sync::Arc::new(AtomicBool::new(false));
    let seen_insert = inserted.clone();
    let stand = Stand::start(move |seen, _| {
        let table = seen
            .path
            .strip_prefix("/bigquery/v2/projects/p/datasets/d/tables/");
        match (seen.method.as_str(), table) {
            ("GET", Some(name)) if name.ends_with("/rowAccessPolicies") => return json(200, "{}"),
            ("GET", Some("t__changes")) => {
                let rows = if seen_insert.load(Ordering::SeqCst) {
                    7
                } else {
                    5
                };
                return json(200, &format!(r#"{{"type":"TABLE","numRows":"{rows}"}}"#));
            }
            ("GET", Some(_)) => return json(404, r#"{"error":{"message":"Not found"}}"#),
            ("PATCH", Some(_)) => return json(200, "{}"),
            _ => {}
        }
        let id = seen.body["jobReference"]["jobId"]
            .as_str()
            .unwrap_or("none")
            .to_string();
        let sql = seen.body["configuration"]["query"]["query"]
            .as_str()
            .unwrap_or("");
        if sql.starts_with("INSERT INTO `p.d.t__changes`") {
            seen_insert.store(true, Ordering::SeqCst);
        }
        json(
            200,
            &format!(
                r#"{{"jobReference":{{"projectId":"p","jobId":"{id}"}},"status":{{"state":"DONE"}}}}"#
            ),
        )
    });
    let mut specs = crate::load::tests::spec_ok();
    let mut meta = specs[0].clone();
    meta.column_name = "__pos".into();
    specs.push(meta);
    let flagged = on(&stand)
        .flag_absent_from_buffer("t", &specs, &["id".to_string()])
        .unwrap();
    assert_eq!(flagged, 2, "the rows the buffer gained");
    let insert = statements(&stand)
        .into_iter()
        .find(|s| s.starts_with("INSERT INTO"))
        .expect("the tombstones are inserted");
    assert_eq!(
        insert,
        crate::load::cdc::absent_as_deleted_sql("p.d.t", "p.d.t__changes", &["id"], &["id"])
    );
    assert!(inserted.load(std::sync::atomic::Ordering::SeqCst));
}
