//! Source conformance (docs/source-conformance-matrix.yaml): every method of the `Source` trait
//! called directly, with ONE body for the four SQL engines and one for MongoDB. What an engine
//! answers differently is a named expectation of that engine below, never a second body.

use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;

use crate::common::*;
use rivet::config::SourceType;
use rivet::error::Result;
use rivet::source::{BatchSink, ExportRequest, Source};
use rivet::tuning::SourceTuning;

const ROWS: i64 = 40;
const MONGO_PORT: u16 = 27017;
const COLUMNS: [&str; 6] = [
    "id",
    "ext_id",
    "server_time",
    "updated_at",
    "time_spent",
    "label",
];

/// What one export handed its sink: each schema's column names, and the first column of every row.
#[derive(Default)]
struct Collected {
    schemas: Vec<Vec<String>>,
    first_column: Vec<i64>,
    rows: usize,
}

impl BatchSink for Collected {
    fn on_schema(&mut self, schema: SchemaRef) -> Result<()> {
        self.schemas
            .push(schema.fields().iter().map(|f| f.name().clone()).collect());
        Ok(())
    }
    fn on_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.rows += batch.num_rows();
        labels_are_the_ids_own(batch);
        if let Some(ids) = batch.column(0).as_any().downcast_ref::<Int64Array>() {
            self.first_column.extend(ids.iter().flatten());
        }
        Ok(())
    }
}

/// One whole read of `query` (of `relation`, for an engine that reads a collection by name).
fn read(source: &mut dyn Source, query: &str, relation: Option<&str>) -> Collected {
    let mut sink = Collected::default();
    let request = ExportRequest {
        query,
        catalog_hint_query: None,
        incremental: None,
        cursor: None,
        tuning: &SourceTuning::from_config(None),
        column_overrides: &Default::default(),
        page_limit: None,
        base_relation: relation,
        upper_bound: None,
    };
    source.export(&request, &mut sink).expect("export");
    sink
}

/// What an engine answers its own way.
struct Ways {
    source_type: SourceType,
    /// The engine name in `server_context`.
    label: &'static str,
    /// The prefix of every harm counter.
    counters: &'static str,
    /// The catalog gives a row size (`avg_row_bytes`).
    sizes_rows: bool,
    /// `native_column_types` adds full types the wire metadata lacks.
    adds_native_types: bool,
    /// `INT` is an integer column (Oracle holds it as `NUMBER(38)`).
    int_is_integer: bool,
    /// Statements that read the governor's counter straight from the server; the first the server answers is the one.
    pressure_sql: &'static [&'static str],
    /// The schemas a sink is handed for a result with no row: the query's columns (`true`) or none (`false`), in order.
    empty_result_schemas: &'static [bool],
}

fn ways(engine: SqlEngine) -> Ways {
    match engine {
        SqlEngine::Pg => Ways {
            source_type: SourceType::Postgres,
            label: "postgres",
            pressure_sql: &[
                "SELECT num_requested FROM pg_stat_checkpointer",
                "SELECT checkpoints_req FROM pg_stat_bgwriter",
            ],
            counters: "pg_",
            sizes_rows: true,
            adds_native_types: false,
            int_is_integer: true,
            empty_result_schemas: &[false],
        },
        SqlEngine::Mysql => Ways {
            source_type: SourceType::Mysql,
            label: "mysql",
            pressure_sql: &[
                "SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_log_waits'",
            ],
            counters: "mysql_",
            sizes_rows: true,
            adds_native_types: true,
            int_is_integer: true,
            empty_result_schemas: &[true, false],
        },
        SqlEngine::Mssql => Ways {
            source_type: SourceType::Mssql,
            label: "mssql",
            pressure_sql: &[
                "SELECT cntr_value FROM sys.dm_os_performance_counters WHERE counter_name LIKE 'Log Flush Waits%' AND instance_name = '_Total'",
            ],
            counters: "mssql_",
            sizes_rows: false,
            adds_native_types: false,
            int_is_integer: true,
            empty_result_schemas: &[true],
        },
        #[cfg(feature = "oracle")]
        SqlEngine::Oracle => Ways {
            source_type: SourceType::Oracle,
            label: "oracle",
            pressure_sql: &["SELECT value FROM v$sysstat WHERE name = 'redo log space requests'"],
            counters: "oracle_",
            sizes_rows: false,
            adds_native_types: false,
            int_is_integer: false,
            empty_result_schemas: &[true],
        },
    }
}

/// The server context is a JSON object naming its engine and a version.
fn context_names_the_engine(source: &mut dyn Source, label: &str) {
    let text = source.server_context().expect("a server context");
    let json: serde_json::Value = serde_json::from_str(&text).expect("server context is JSON");
    assert_eq!(json["engine"], label, "{text}");
    assert!(
        json["version"].as_str().is_some_and(|v| !v.is_empty()),
        "{text}"
    );
}

/// The harm counters are named after their engine, each once, and none is negative.
fn counters_are_the_engines_own(source: &mut dyn Source, prefix: &str) {
    let counters = source.harm_counters().expect("harm counters");
    assert!(!counters.is_empty());
    let names: std::collections::BTreeSet<&str> =
        counters.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(
        names.len(),
        counters.len(),
        "a counter is listed twice: {counters:?}"
    );
    for (name, value) in &counters {
        assert!(
            name.starts_with(prefix) && name.len() > prefix.len(),
            "{name}"
        );
        assert!(*value >= 0, "{name} = {value}");
    }
}

fn conforms(engine: SqlEngine) {
    engine.alive();
    let way = ways(engine);
    let col = |name: &str| match engine.folds_upper() {
        true => name.to_uppercase(),
        false => name.to_string(),
    };
    let cols = |names: &[&str]| names.iter().map(|n| col(n)).collect::<Vec<_>>();
    let (table, _guard) = engine.range_table("conf");
    engine.insert(&table, 1..=ROWS, 5, Some(7));
    engine.refresh_row_estimate(&table);
    let (two_key, _two) = engine.create(
        "conf_pk2",
        "a INT NOT NULL, b INT NOT NULL, v INT, PRIMARY KEY (b, a)",
    );
    let (keyless, _keyless) = engine.create("conf_nopk", "a INT");
    let mut source =
        rivet::source::connect(way.source_type, engine.url(), None, None).expect("connect");
    let s = source.as_mut();

    // query_scalar: a number as text, a NULL as none.
    let scalar = |s: &mut dyn Source, sql: String| s.query_scalar(&sql).expect("query_scalar");
    assert_eq!(
        scalar(s, format!("SELECT COUNT(*) FROM {table}")).as_deref(),
        Some("40")
    );
    assert_eq!(
        scalar(s, format!("SELECT MIN(id) FROM {table}")).as_deref(),
        Some("1")
    );
    assert_eq!(
        scalar(
            s,
            format!("SELECT MAX(time_spent) FROM {table} WHERE id < 0")
        ),
        None
    );

    // primary_key: the key columns in KEY order, none for a table without a key.
    assert_eq!(s.primary_key(&table).unwrap(), Some(cols(&["id"])));
    assert_eq!(s.primary_key(&two_key).unwrap(), Some(cols(&["b", "a"])));
    assert_eq!(s.primary_key(&keyless).unwrap(), None);

    // introspect_for_chunking: the integer key, the keyset keys with the primary key first, the row count.
    let seen = s.introspect_for_chunking(&table).expect("introspect");
    assert_eq!(seen.single_int_pk, Some(col("id")));
    assert_eq!(seen.keyset_keys, cols(&["id", "ext_id"]));
    assert_eq!(seen.row_estimate, ROWS);
    assert_eq!(
        seen.avg_row_bytes.is_some_and(|b| b > 0),
        way.sizes_rows,
        "{seen:?}"
    );
    let ints: std::collections::BTreeSet<String> = seen.int_columns.into_iter().collect();
    let want_ints: &[&str] = match way.int_is_integer {
        true => &["id", "ext_id", "time_spent"],
        false => &["id", "ext_id"],
    };
    assert_eq!(ints, cols(want_ints).into_iter().collect());
    let bare = s
        .introspect_for_chunking(&keyless)
        .expect("introspect a keyless table");
    assert_eq!(
        (bare.single_int_pk, bare.keyset_keys, bare.row_estimate),
        (None, vec![], 0)
    );

    // type_mappings: every column of the query, in order, with a native and an Arrow type.
    let types = s
        .type_mappings(&format!("SELECT * FROM {table}"), &Default::default())
        .expect("type_mappings");
    let names: Vec<&str> = types.iter().map(|t| t.column_name.as_str()).collect();
    assert_eq!(names, cols(&COLUMNS));
    assert!(
        types.iter().all(|t| !t.source_native_type.is_empty()),
        "{names:?}"
    );
    assert_eq!(types[0].arrow_type, Some(DataType::Int64));
    assert_eq!(
        types[2].arrow_type,
        Some(DataType::Timestamp(TimeUnit::Microsecond, None))
    );
    assert_eq!(types[5].arrow_type, Some(DataType::Utf8));

    // native_column_types: full types where the wire lacks them, nothing elsewhere.
    let native = s.native_column_types(&table).expect("native_column_types");
    match way.adds_native_types {
        true => assert!(
            native.contains(&("server_time".to_string(), "datetime(6)".to_string()))
                && native.len() == COLUMNS.len(),
            "{native:?}"
        ),
        false => assert_eq!(native, vec![]),
    }

    context_names_the_engine(s, way.label);
    counters_are_the_engines_own(s, way.counters);
    // sample_governor_pressure: the server's own counter, read between two direct reads of it.
    if let SqlEngine::Pg = engine {
        engine.exec("CHECKPOINT");
    }
    let counter = |s: &mut dyn Source| {
        let read = |sql: &&str| s.query_scalar(sql).ok().flatten()?.parse::<u64>().ok();
        way.pressure_sql
            .iter()
            .find_map(read)
            .expect("the governor's counter")
    };
    let (before, sampled, after) = (counter(s), s.sample_governor_pressure(), counter(s));
    assert!(
        sampled.is_some_and(|v| before <= v && v <= after),
        "the sample {sampled:?} is the server's counter, {before} then {after}"
    );

    // export: every row once, under the query's columns.
    let all = read(s, &format!("SELECT * FROM {table}"), None);
    assert_eq!(all.schemas, vec![cols(&COLUMNS)]);
    let mut ids = all.first_column.clone();
    ids.sort_unstable();
    assert_eq!(
        (all.rows as i64, ids),
        (ROWS, (1..=ROWS).collect::<Vec<_>>())
    );
    let none = read(s, &format!("SELECT * FROM {table} WHERE id < 0"), None);
    let empty_shapes: Vec<bool> = none.schemas.iter().map(|names| !names.is_empty()).collect();
    assert_eq!(
        (none.rows, empty_shapes.as_slice()),
        (0, way.empty_result_schemas)
    );
    assert!(
        none.schemas
            .iter()
            .all(|names| names.is_empty() || *names == cols(&COLUMNS))
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_source_answers_every_method_postgres() {
    conforms(SqlEngine::Pg);
}

#[test]
#[ignore = "live: requires docker compose mysql"]
fn a_source_answers_every_method_mysql() {
    conforms(SqlEngine::Mysql);
}

#[test]
#[ignore = "live: requires docker compose mssql"]
fn a_source_answers_every_method_mssql() {
    conforms(SqlEngine::Mssql);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires the oracle service"]
fn a_source_answers_every_method_oracle() {
    conforms(SqlEngine::Oracle);
}

#[test]
#[ignore = "live: requires docker compose up -d mongo"]
fn a_source_answers_every_method_mongo() {
    require_alive(LiveService::Mongo);
    let db = unique_name("conf");
    let _guard = MongoDbGuard {
        port: MONGO_PORT,
        db: db.clone(),
    };
    let m = MongoTest::connect(MONGO_PORT, &db);
    m.seed_int_id("t", ROWS);
    m.create_empty_collection("none");
    let mut source = rivet::source::connect(
        SourceType::Mongo,
        &MongoTest::url(MONGO_PORT, &db),
        None,
        None,
    )
    .expect("connect");
    let s = source.as_mut();

    // The reconcile count is the one scalar MongoDB answers; any other is none.
    let count = "SELECT COUNT(*) FROM (SELECT * FROM t) AS _rivet_reconcile";
    assert_eq!(s.query_scalar(count).unwrap().as_deref(), Some("40"));
    assert_eq!(s.query_scalar("SELECT MIN(_id) FROM t").unwrap(), None);
    assert_eq!(s.primary_key("t").unwrap(), Some(vec!["_id".to_string()]));
    let refused = s.introspect_for_chunking("t").expect_err("no chunked mode");
    assert!(
        format!("{refused:#}").contains("chunked mode is not supported"),
        "{refused:#}"
    );
    let types = s
        .type_mappings("SELECT * FROM t", &Default::default())
        .unwrap();
    let names: Vec<&str> = types.iter().map(|t| t.column_name.as_str()).collect();
    assert_eq!(names, ["_id", "document"]);
    assert!(types.iter().all(|t| t.arrow_type == Some(DataType::Utf8)));
    assert_eq!(s.native_column_types("t").unwrap(), vec![]);
    context_names_the_engine(s, "mongodb");
    counters_are_the_engines_own(s, "mongo_");
    assert_eq!(
        s.sample_governor_pressure(),
        None,
        "MongoDB has no foreign-pressure counter"
    );

    let document = vec!["_id".to_string(), "document".to_string()];
    let all = read(s, "SELECT * FROM t", Some("t"));
    assert_eq!(
        (all.rows as i64, &all.schemas),
        (ROWS, &vec![document.clone()])
    );
    let none = read(s, "SELECT * FROM none", Some("none"));
    assert_eq!((none.rows, none.schemas), (0, vec![document]));
}
