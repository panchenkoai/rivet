//! The batch-equals-CDC contract, generated from `docs/type-capability-matrix.yaml`
//! (ADR-0038 CP9, CP12). Per engine, one table holds a column for every ledger row,
//! seeded with the row's samples; CDC runs `initial: snapshot`, then every column is
//! rewritten and the samples inserted again, a second run captures that, and a batch run
//! reads the final state. One DuckDB session then reads every stage it can reach — the
//! source (ATTACHed read-only), the batch parts, the CDC snapshot and stream parts, the
//! CDC final image, and both runs' state DBs — and grades, per column: the delivered type,
//! each value as full-precision text, and COUNT(*), COUNT(col), COUNT(DISTINCT col).

use std::collections::BTreeMap;
use std::path::Path;

use crate::common::*;

#[derive(Clone, Debug, Default, PartialEq)]
struct Render {
    /// The column as text on the source's own client — only for an engine DuckDB cannot attach.
    source: Option<String>,
    /// The column as text in DuckDB, applied identically to every stage DuckDB reads.
    duck: Option<String>,
    /// The column rendered by the source server itself, read through DuckDB's passthrough, where the scanner would narrow it.
    server: Option<String>,
    canon: Option<String>,
}

#[derive(Clone, Debug)]
struct Row {
    native: String,
    sample: Vec<String>,
    delivery: String,
    render: Render,
    /// Why rivet misses this row's ADR target today; the row must keep failing until the named step fixes it.
    known_defect: Option<String>,
    /// Beside a known_defect: the delivery rivet ships today (the driver fails on any third type).
    today_delivery: Option<String>,
    /// Samples (`NULL` for a NULL cell) whose value differs from the source today: a known_defect's value class.
    defect_samples: Vec<String>,
    /// The ClickHouse column type `rivet load` builds for this row.
    clickhouse: Option<String>,
    /// Beside a marker: the ClickHouse type `rivet load` builds today.
    today_clickhouse: Option<String>,
    /// Like `known_defect`, for the ClickHouse stage alone.
    clickhouse_defect: Option<String>,
    /// Like `defect_samples`, for the ClickHouse stage alone.
    clickhouse_defect_samples: Vec<String>,
    /// Today's behaviour of a known_defect row: the batch run refuses the column by name.
    batch_refuses: bool,
}

struct Ledger {
    setup: Vec<String>,
    rows: Vec<Row>,
}

/// The ledger's rows for `engine`, graded alike in both modes.
fn ledger(engine: &str) -> Ledger {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("docs/type-capability-matrix.yaml");
    let doc: serde_yaml_ng::Value =
        serde_yaml_ng::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let e = &doc["engines"][engine];
    let s = |v: &serde_yaml_ng::Value| v.as_str().map(str::to_string);
    let list = |v: &serde_yaml_ng::Value| -> Vec<String> {
        v.as_sequence()
            .map(|l| l.iter().map(|x| s(x).expect("string")).collect())
            .unwrap_or_default()
    };
    // A row's own render keys override what its delivery determines (`renders:`).
    let render = |r: &serde_yaml_ng::Value, base: &serde_yaml_ng::Value| {
        let k = |key: &str| s(&r[key]).or_else(|| s(&base[key]));
        Render {
            source: k("source"),
            duck: k("duck"),
            server: k("server"),
            canon: k("canon"),
        }
    };
    let rows = e["rows"]
        .as_sequence()
        .unwrap_or_else(|| panic!("engines.{engine}.rows is not a list"))
        .iter()
        .map(|r| {
            let ch = &r["clickhouse"];
            Row {
                native: s(&r["native_type"]).expect("native_type"),
                sample: r["sample"]
                    .as_sequence()
                    .expect("sample")
                    .iter()
                    .map(|v| s(v).expect("sample literal"))
                    .collect(),
                delivery: s(&r["delivery"]).expect("delivery"),
                // A known_defect row is graded at full precision for what it delivers today.
                render: if r["today_render"].is_null() {
                    render(
                        &r["render"],
                        &e["renders"][r["delivery"].as_str().unwrap_or("")],
                    )
                } else {
                    render(&r["today_render"], &serde_yaml_ng::Value::Null)
                },
                known_defect: s(&r["known_defect"]),
                today_delivery: s(&r["today_delivery"]),
                defect_samples: list(&r["defect_samples"]),
                clickhouse: s(&ch["type"]),
                today_clickhouse: s(&ch["today"]),
                clickhouse_defect: s(&ch["defect"]),
                clickhouse_defect_samples: list(&ch["defect_samples"]),
                batch_refuses: r["batch_refuses"].as_bool().unwrap_or(false),
            }
        })
        .collect();
    Ledger {
        setup: e["setup"]
            .as_sequence()
            .map(|v| v.iter().filter_map(s).collect())
            .unwrap_or_default(),
        rows,
    }
}

/// `render.duck` for a type DuckDB cannot read exactly (Decimal256 reads as DOUBLE, ADR-0038 CP11).
const ARROW: &str = "arrow";

/// The ledger without the rows batch refuses today, and those rows.
fn split_refused(lg: Ledger) -> (Ledger, Vec<Row>) {
    let (refused, rows) = lg.rows.into_iter().partition(|r| r.batch_refuses);
    (
        Ledger {
            setup: lg.setup,
            rows,
        },
        refused,
    )
}

/// How the verdict reads the source.
enum Source<'a> {
    /// ATTACHed READ_ONLY in the verdict's own DuckDB session, with the engine's scanner settings and passthrough.
    Attach {
        engine: OracleEngine,
        database: &'static str,
    },
    /// No DuckDB scanner (Oracle): the database renders its own values through its client.
    #[cfg_attr(not(feature = "oracle"), allow(dead_code))]
    Client {
        text_rows: &'a dyn Fn(&str) -> Vec<Vec<Option<String>>>,
        render: &'static str,
    },
}

/// What one engine needs from its test: SQL against the source and the two rigs.
struct Stand<'a> {
    engine: &'static str,
    /// The table as DML names it.
    table: String,
    /// The table as the source catalog names it (DuckDB's ATTACH, the census).
    bare: String,
    exec: &'a dyn Fn(&str),
    source: Source<'a>,
    /// Called with the number of change rows the DML produced (SQL Server waits on its capture job).
    settle: &'a dyn Fn(i64),
    /// Both rigs carry `census_oracle()`: their parts and state DBs are visible to DuckDB.
    cdc: Rig,
    batch: Rig,
    /// A second CDC export into the fake-gcs bucket that `rivet load` puts into ClickHouse.
    warehouse: Option<Warehouse>,
}

/// A CDC rig loaded into a scratch ClickHouse database, dropped on drop.
struct Warehouse {
    rig: Rig,
    db: String,
    view: String,
}

impl Drop for Warehouse {
    fn drop(&mut self) {
        let _ = std::panic::catch_unwind(|| {
            clickhouse_run_sql_json(&format!("DROP DATABASE IF EXISTS {}", self.db))
        });
    }
}

const CH_BUCKET: &str = "rivet-qa-ledger-parity";
const CH_PASSWORD_ENV: &str = "RIVET_TEST_CH_PASSWORD";

/// `rig` exporting to the fake-gcs bucket and loading into a fresh ClickHouse database; the view is named `view`.
fn warehouse(rig: Rig, view: &str) -> Warehouse {
    require_alive(LiveService::ClickHouse);
    require_alive(LiveService::FakeGcs);
    ensure_gcs_bucket(CH_BUCKET);
    let db = unique_name("ledger_ch");
    clickhouse_run_sql_json(&format!("CREATE DATABASE {db}"));
    let rig = rig
        .cdc("initial: snapshot")
        .dest_gcs(CH_BUCKET, &unique_name("ledger"), FAKE_GCS_ENDPOINT)
        .top_line(&format!(
            "load: {{ target: clickhouse, url: \"{CLICKHOUSE_HTTP_URL}\", database: {db}, \
             user: {CLICKHOUSE_USER}, password_env: {CH_PASSWORD_ENV}, pk: [id] }}"
        ));
    Warehouse {
        rig,
        db,
        view: view.to_string(),
    }
}

/// Run the CDC export (and the warehouse export beside it).
fn capture(st: &Stand) {
    st.cdc.run_ok();
    if let Some(w) = &st.warehouse {
        w.rig.run_ok();
    }
}

/// `rivet load` of the warehouse export into ClickHouse.
fn load(st: &Stand) {
    if let Some(w) = &st.warehouse {
        w.rig
            .load_ok(&[], &[(CH_PASSWORD_ENV, CLICKHOUSE_PASSWORD)]);
    }
}

/// `sql` against the stand's ClickHouse as a DuckDB relation (Parquet over httpfs, from inside the DuckDB container).
fn clickhouse_relation(sql: &str) -> String {
    let q: String = format!("{sql} FORMAT Parquet")
        .bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' => (b as char).to_string(),
            _ => format!("%{b:02X}"),
        })
        .collect();
    format!(
        "read_parquet('http://{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}@clickhouse:8123/\
         ?output_format_parquet_string_as_string=0&query={q}')"
    )
}

/// Whether a delivery is a timestamp without a zone.
fn naive_ts(delivery: &str) -> bool {
    delivery.starts_with("Timestamp(") && !delivery.contains(',')
}

/// The session time zone of the warehouse leg that reads naive timestamps as ClickHouse's own text.
const CH_SESSION_TZ: &str = "Asia/Tokyo";

/// The loaded view as DuckDB reads it, re-typed to each row's delivery (text from the bytes a ClickHouse `String` travels as, UUID from `FixedString(16)`, time of day from its decimal seconds, a naive timestamp from Parquet's UTC, or with `as_text` from ClickHouse's own text under a non-UTC session); a column `ns` names keeps every digit as text.
fn warehouse_relation(
    rows: &[Row],
    w: &Warehouse,
    as_text: bool,
    ns: &[Option<arrow::datatypes::DataType>],
) -> String {
    use arrow::datatypes::DataType;
    let ns_ts = |k: usize| matches!(ns[k], Some(DataType::Timestamp(..)));
    let naive = |r: &Row| as_text && naive_ts(&r.delivery);
    let ch_text: Vec<String> = rows
        .iter()
        .zip(columns(rows))
        .enumerate()
        .filter_map(|(k, (r, c))| {
            if naive(r) {
                Some(format!("toString({c}) AS {c}"))
            } else if ns_ts(k) {
                Some(format!("toString({c}, 'UTC') AS {c}"))
            } else {
                None
            }
        })
        .collect();
    let text: Vec<String> = rows
        .iter()
        .zip(columns(rows))
        .enumerate()
        .filter_map(|(k, (r, c))| {
            let d = r.delivery.as_str();
            let e = if ns_ts(k) {
                format!("decode({c})")
            } else if ns[k].is_some() {
                format!(
                    "CAST(TIME '00:00:00' + to_seconds(CAST(floor({c}) AS BIGINT)) AS VARCHAR) \
                     || '.' || split_part(CAST({c} AS VARCHAR), '.', 2)"
                )
            } else if d == "Utf8"
                || d == "arrow.json"
                || d.starts_with(|c: char| c.is_ascii_lowercase()) && !d.starts_with("arrow.")
            {
                format!("decode({c})")
            } else if d == "List(Utf8)" {
                format!("list_transform({c}, x -> decode(x))")
            } else if d == "arrow.uuid" {
                format!(
                    "CAST(regexp_replace(lower(hex({c})), \
                     '^(.{{8}})(.{{4}})(.{{4}})(.{{4}})(.{{12}})$', '\\1-\\2-\\3-\\4-\\5') AS UUID)"
                )
            } else if naive(r) {
                format!("CAST(decode({c}) AS TIMESTAMP)")
            } else if naive_ts(d) {
                format!("CAST({c} AS TIMESTAMP)")
            } else if d.starts_with("Time64(") {
                format!("TIME '00:00:00' + to_microseconds(CAST({c} * 1000000 AS BIGINT))")
            } else {
                return None;
            };
            Some(format!("{e} AS {c}"))
        })
        .collect();
    let settings = if rows.iter().any(naive) {
        format!(" SETTINGS session_timezone = '{CH_SESSION_TZ}'")
    } else {
        String::new()
    };
    let from = clickhouse_relation(&if ch_text.is_empty() {
        format!("SELECT * FROM {}.{} WHERE NOT __is_deleted", w.db, w.view)
    } else {
        format!(
            "SELECT * REPLACE ({}) FROM {}.{} WHERE NOT __is_deleted{settings}",
            ch_text.join(", "),
            w.db,
            w.view
        )
    });
    if text.is_empty() {
        from
    } else {
        format!("(SELECT * REPLACE ({}) FROM {from})", text.join(", "))
    }
}

type Cells = BTreeMap<i64, Vec<Option<String>>>;

/// Rows of `[id, cells...]` keyed by id; an id seen twice is a failure.
fn keyed(rows: Vec<Vec<Option<String>>>) -> Cells {
    rows.into_iter()
        .map(|mut r| {
            let id = canon_num(r.remove(0).as_deref().expect("id"))
                .parse()
                .expect("integer id");
            (id, r)
        })
        .fold(Cells::new(), |mut m, (id, r)| {
            assert!(m.insert(id, r).is_none(), "id {id} read twice");
            m
        })
}

/// The `{columns, rows}` of one named query as optional strings.
fn cells(v: &serde_json::Value) -> Vec<Vec<Option<String>>> {
    v["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("duckdb: {v}"))
        .iter()
        .map(|r| {
            r.as_array()
                .unwrap()
                .iter()
                .map(|c| c.as_str().map(str::to_string))
                .collect()
        })
        .collect()
}

/// Arrow's own display of `cols` (all nine digits where `ns_text` applies) in every part under `dir` (only rows whose `__op` is in `ops`, when given), keyed by id.
fn arrow_cells(dir: &Path, cols: &[String], ops: Option<&[&str]>) -> Cells {
    use arrow::util::display::array_value_to_string;
    let mut out = Cells::new();
    for b in read_all_parts(dir) {
        let col = |name: &str| {
            let i = (0..b.num_columns())
                .find(|&i| b.schema().field(i).name().eq_ignore_ascii_case(name))
                .unwrap_or_else(|| panic!("{name} missing under {}", dir.display()));
            b.column(i).clone()
        };
        let (ids, op) = (col("id"), ops.map(|_| col("__op")));
        let vals: Vec<_> = cols.iter().map(|c| col(c)).collect();
        for r in 0..b.num_rows() {
            if op.as_ref().is_some_and(|o| {
                !ops.unwrap()
                    .contains(&array_value_to_string(o, r).unwrap().as_str())
            }) {
                continue;
            }
            let id = canon_num(&array_value_to_string(&ids, r).unwrap())
                .parse()
                .unwrap();
            let row = vals
                .iter()
                .map(|a| {
                    (!a.is_null(r)).then(|| {
                        ns_text(a, r).unwrap_or_else(|| array_value_to_string(a, r).unwrap())
                    })
                })
                .collect();
            assert!(
                out.insert(id, row).is_none(),
                "id {id} twice under {}",
                dir.display()
            );
        }
    }
    out
}

/// The Arrow schema every parquet part under `dir` shares.
fn schema(dir: &Path) -> arrow::datatypes::SchemaRef {
    let mut out: Option<arrow::datatypes::SchemaRef> = None;
    for p in std::fs::read_dir(dir).unwrap().flatten().map(|e| e.path()) {
        if p.extension().is_none_or(|x| x != "parquet") {
            continue;
        }
        let s = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(&p).unwrap(),
        )
        .unwrap()
        .schema()
        .clone();
        if let Some(prev) = &out {
            assert_eq!(
                prev,
                &s,
                "parts under {} disagree on the schema",
                dir.display()
            );
        }
        out = Some(s);
    }
    out.unwrap_or_else(|| panic!("no parquet part under {}", dir.display()))
}

/// A field's delivery as the ledger spells it: TextForm label, extension name, or Arrow type.
fn delivered(schema: &arrow::datatypes::Schema, col: &str) -> String {
    let Some(f) = schema
        .fields()
        .iter()
        .find(|f| f.name().eq_ignore_ascii_case(col))
    else {
        return "<missing>".into();
    };
    let md = f.metadata();
    if let Some(form) = md.get("rivet.text_form") {
        return match f.data_type() {
            arrow::datatypes::DataType::Utf8 => form.clone(),
            other => format!("{form} on {other}"),
        };
    }
    md.get("ARROW:extension:name")
        .cloned()
        .unwrap_or_else(|| f.data_type().to_string())
}

/// A field's Arrow type when it is nanosecond time: Time64(ns) or Timestamp(ns, any zone).
fn ns_type(schema: &arrow::datatypes::Schema, col: &str) -> Option<arrow::datatypes::DataType> {
    use arrow::datatypes::{DataType, TimeUnit};
    let f = schema
        .fields()
        .iter()
        .find(|f| f.name().eq_ignore_ascii_case(col))?;
    matches!(
        f.data_type(),
        DataType::Time64(TimeUnit::Nanosecond) | DataType::Timestamp(TimeUnit::Nanosecond, _)
    )
    .then(|| f.data_type().clone())
}

/// Whether DuckDB 1.5.5 reads the field at microseconds: Time64(ns) and Timestamp(ns, <zone>) (measured; only a zoneless Timestamp(ns) reads exactly).
fn duck_narrows(schema: &arrow::datatypes::Schema, col: &str) -> bool {
    matches!(
        ns_type(schema, col),
        Some(arrow::datatypes::DataType::Time64(_))
            | Some(arrow::datatypes::DataType::Timestamp(_, Some(_)))
    )
}

/// All nine digits of a Time64(ns) or zoned Timestamp(ns) cell, spelled as DuckDB spells TIME / TIMESTAMPTZ in UTC; None for any other type.
fn ns_text(a: &dyn arrow::array::Array, r: usize) -> Option<String> {
    use arrow::array::AsArray;
    use arrow::datatypes::{DataType, Time64NanosecondType, TimeUnit, TimestampNanosecondType};
    match a.data_type() {
        DataType::Time64(TimeUnit::Nanosecond) => {
            let v = a.as_primitive::<Time64NanosecondType>().value(r);
            let t = chrono::NaiveTime::from_num_seconds_from_midnight_opt(
                (v / 1_000_000_000) as u32,
                (v % 1_000_000_000) as u32,
            )
            .expect("time of day");
            Some(t.format("%H:%M:%S%.9f").to_string())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => {
            let v = a.as_primitive::<TimestampNanosecondType>().value(r);
            Some(
                chrono::DateTime::from_timestamp_nanos(v)
                    .format("%Y-%m-%d %H:%M:%S%.9f+00")
                    .to_string(),
            )
        }
        _ => None,
    }
}

#[test]
fn round_micros_rounds_the_server_tick_and_keeps_a_micro() {
    for (got, want) in [
        ("2026-01-15 13:45:30.1266667", "2026-01-15 13:45:30.126667"),
        ("2026-01-15 13:45:30.126667", "2026-01-15 13:45:30.126667"),
        ("2026-01-15 13:45:30.126666", "2026-01-15 13:45:30.126666"),
        (
            "2026-01-15 13:45:59.9999996+00",
            "2026-01-15 13:46:00.000000",
        ),
    ] {
        assert_eq!(round_micros(got), want);
    }
}

/// Write `[id, Time64(ns), Timestamp(ns, UTC)]` rows to one parquet part under `dir` with arrow-rs (rivet's writer).
fn write_ns_part(dir: &Path, rows: &[(i64, i64, i64)]) {
    use arrow::array::{Int64Array, Time64NanosecondArray, TimestampNanosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("c0", DataType::Time64(TimeUnit::Nanosecond), true),
        Field::new(
            "c1",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        ),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(Time64NanosecondArray::from_iter_values(
                rows.iter().map(|r| r.1),
            )),
            Arc::new(
                TimestampNanosecondArray::from_iter_values(rows.iter().map(|r| r.2))
                    .with_timezone("UTC"),
            ),
        ],
    )
    .unwrap();
    let f = std::fs::File::create(dir.join("part-0.parquet")).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

#[test]
fn arrow_cells_keeps_all_nine_digits_of_the_types_duckdb_narrows() {
    // 13:45:30.123456789 and 2026-06-23 04:30:00.123456789 UTC, and their microsecond truncations.
    let (t, ts) = (49_530_123_456_789_i64, 1_782_189_000_123_456_789_i64);
    let (full, cut) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    write_ns_part(full.path(), &[(1, t, ts)]);
    write_ns_part(cut.path(), &[(1, t - t % 1_000, ts - ts % 1_000)]);
    let cols = ["c0".to_string(), "c1".to_string()];
    let (a, b) = (
        arrow_cells(full.path(), &cols, None),
        arrow_cells(cut.path(), &cols, None),
    );
    assert_eq!(
        a[&1],
        vec![
            Some("13:45:30.123456789".to_string()),
            Some("2026-06-23 04:30:00.123456789+00".to_string())
        ]
    );
    let ts_canon = Some("timestamp".to_string());
    for k in 0..2 {
        assert_ne!(
            canon(&a[&1][k], &ts_canon),
            canon(&b[&1][k], &ts_canon),
            "c{k}: a truncated twin compares equal"
        );
    }
    let s = schema(full.path());
    assert!(duck_narrows(&s, "c0") && duck_narrows(&s, "c1") && !duck_narrows(&s, "id"));
    // Counted over the Arrow text, a sub-microsecond difference stays distinct.
    let mut two = Cells::new();
    two.insert(1, a[&1].clone());
    two.insert(2, vec![b[&1][0].clone(), b[&1][1].clone()]);
    assert_eq!(
        counts_of(&two, &[ts_canon.clone(), ts_canon]),
        vec![2, 2, 2, 2, 2]
    );
}

#[test]
fn arrow_overlay_grades_every_delivered_leg_on_arrow_text() {
    // DuckDB collapsed two ids that differ below the microsecond; Arrow keeps them apart.
    let duck = |v: &str| -> Cells {
        [
            (1, vec![Some(v.to_string())]),
            (2, vec![Some(v.to_string())]),
        ]
        .into()
    };
    let arrow: Cells = [
        (1, vec![Some("13:45:30.123456789".to_string())]),
        (2, vec![Some("13:45:30.123456".to_string())]),
    ]
    .into();
    let legs_named = [
        "batch",
        "batch_stream",
        "batch_snapshot",
        "stream",
        "snapshot",
        "final",
    ];
    let mut legs: BTreeMap<String, Cells> = legs_named
        .iter()
        .map(|l| (l.to_string(), duck("13:45:30.123456")))
        .collect();
    legs.insert("source".into(), arrow.clone());
    let mut counts: BTreeMap<String, Vec<i64>> =
        legs.keys().map(|l| (l.clone(), vec![2, 2, 1])).collect();
    let canons = [Some("timestamp".to_string())];
    arrow_overlay(&mut legs, &mut counts, 0, &canons, &arrow, &arrow, &arrow);
    for l in legs_named.iter().chain(&["source"]) {
        assert_eq!(legs[*l], arrow, "{l}");
        assert_eq!(counts[*l], vec![2, 2, 2], "{l}");
    }
}

#[test]
fn canon_interval_reads_both_renderings_alike() {
    for (iso, duck) in [
        ("P1Y2M3D", "1 year 2 months 3 days"),
        ("P1DT4H5M6.5S", "1 day 04:05:06.5"),
        ("PT-1H-2M", "-01:02:00"),
        ("PT0S", "00:00:00"),
    ] {
        assert_eq!(canon_interval(iso), canon_interval(duck), "{iso} vs {duck}");
    }
    assert_ne!(canon_interval("P1M"), canon_interval("1 day"));
}

/// Column names `c0..` of the ledger rows.
fn columns(rows: &[Row]) -> Vec<String> {
    (0..rows.len()).map(|i| format!("c{i}")).collect()
}

/// Sample `i` of every row, `NULL` where a row has fewer samples.
fn values(rows: &[Row], i: usize) -> Vec<String> {
    rows.iter()
        .map(|r| r.sample.get(i).cloned().unwrap_or_else(|| "NULL".into()))
        .collect()
}

/// Rows 1..=n hold the samples, rows n+1..=2n start NULL; returns n.
fn seed(lg: &Ledger, st: &Stand) -> i64 {
    assert!(
        lg.rows.iter().all(|r| !r.batch_refuses),
        "{}: split the batch-refused rows off first",
        st.engine
    );
    let (cols, t) = (columns(&lg.rows), &st.table);
    let n = lg.rows.iter().map(|r| r.sample.len()).max().unwrap() as i64;
    for i in 0..n as usize {
        (st.exec)(&format!(
            "INSERT INTO {t} (id, {}) VALUES ({}, {})",
            cols.join(", "),
            i + 1,
            values(&lg.rows, i).join(", ")
        ));
    }
    for id in n + 1..=2 * n {
        (st.exec)(&format!("INSERT INTO {t} (id) VALUES ({id})"));
    }
    (st.settle)(2 * n);
    n
}

/// After the snapshot: one UPDATE per row rewrites every column (rows n+1..=2n take the samples, row 1 goes NULL), and rows 2n+1..=3n insert them again.
fn rewrite(lg: &Ledger, st: &Stand, n: i64) {
    let (cols, t) = (columns(&lg.rows), &st.table);
    let set = |vals: &[String]| -> String {
        cols.iter()
            .zip(vals)
            .map(|(c, v)| format!("{c} = {v}"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    for i in 0..n as usize {
        (st.exec)(&format!(
            "UPDATE {t} SET {} WHERE id = {}",
            set(&values(&lg.rows, i)),
            n + 1 + i as i64
        ));
    }
    let nulls = vec!["NULL".to_string(); cols.len()];
    (st.exec)(&format!("UPDATE {t} SET {} WHERE id = 1", set(&nulls)));
    for i in 0..n as usize {
        (st.exec)(&format!(
            "INSERT INTO {t} (id, {}) VALUES ({}, {})",
            cols.join(", "),
            2 * n + 1 + i as i64,
            values(&lg.rows, i).join(", ")
        ));
    }
    (st.settle)(2 * n + 2 * (n + 1) + n);
}

/// The DuckDB projection of every row: the same text for every stage DuckDB reads.
fn projection(rows: &[Row]) -> String {
    rows.iter()
        .zip(columns(rows))
        .map(|(r, c)| match r.render.duck.as_deref() {
            Some(ARROW) => format!("CAST(NULL AS VARCHAR) AS {c}"),
            d => format!(
                "CAST({} AS VARCHAR) AS {c}",
                d.unwrap_or("{c}").replace("{c}", &c)
            ),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// The attached source `from`, with every `render.server` column replaced by the server's own rendering read through `pass`.
fn server_rendered(rows: &[Row], from: &str, pass: &str, bare: &str) -> String {
    let server: Vec<(String, &str)> = rows
        .iter()
        .zip(columns(rows))
        .filter_map(|(r, c)| r.render.server.as_deref().map(|e| (c, e)))
        .collect();
    if server.is_empty() {
        return from.to_string();
    }
    let sql = format!(
        "SELECT id, {} FROM {bare}",
        server
            .iter()
            .map(|(c, e)| format!("{} AS {c}", e.replace("{c}", c)))
            .collect::<Vec<_>>()
            .join(", ")
    );
    format!(
        "(SELECT t.* REPLACE ({}) FROM {from} t JOIN {} p ON p.id = t.id)",
        server
            .iter()
            .map(|(c, _)| format!("p.{c} AS {c}"))
            .collect::<Vec<_>>()
            .join(", "),
        pass.replace("{sql}", &sql.replace('\'', "''"))
    )
}

/// ATTACH the state backend a `census_oracle()` rig ran against as `alias`: Postgres when `RIVET_STATE_URL` names one, else the SQLite file beside its config.
fn state_attach(rig: &Rig, alias: &str) -> String {
    match std::env::var("RIVET_STATE_URL")
        .ok()
        .filter(|u| u.starts_with("postgres"))
    {
        Some(url) => format!(
            "INSTALL postgres; LOAD postgres; ATTACH '{url}' AS {alias} (TYPE postgres, READ_ONLY);"
        ),
        None => format!(
            "INSTALL sqlite; LOAD sqlite; ATTACH '{}/.rivet_state.db' AS {alias} (TYPE sqlite, READ_ONLY);",
            rig.oracle_container_out().trim_end_matches("/out")
        ),
    }
}

/// `COUNT(*)`, then `COUNT(col)` and `COUNT(DISTINCT col)` per column, over `leg`'s rows.
fn counts_of(rows: &Cells, canons: &[Option<String>]) -> Vec<i64> {
    let mut out = vec![rows.len() as i64];
    for (k, how) in canons.iter().enumerate() {
        let vals: Vec<String> = rows.values().filter_map(|r| canon(&r[k], how)).collect();
        let distinct: std::collections::BTreeSet<&String> = vals.iter().collect();
        out.push(vals.len() as i64);
        out.push(distinct.len() as i64);
    }
    out
}

/// Column `k` of every delivered leg replaced by Arrow's cells (DuckDB narrowed them), and every leg's (non-null, distinct) for `k` recounted over that text.
fn arrow_overlay(
    legs: &mut BTreeMap<String, Cells>,
    counts: &mut BTreeMap<String, Vec<i64>>,
    k: usize,
    canons: &[Option<String>],
    a_b: &Cells,
    a_s: &Cells,
    a_snap: &Cells,
) {
    for (leg, rows) in legs.iter_mut() {
        let from: &[&Cells] = match leg.as_str() {
            "batch" | "batch_stream" | "batch_snapshot" => &[a_b],
            "stream" => &[a_s],
            "snapshot" => &[a_snap],
            // The final image is the last streamed row of an id, else its snapshot row.
            "final" => &[a_s, a_snap],
            _ => &[],
        };
        if !from.is_empty() {
            for (id, row) in rows.iter_mut() {
                row[k] = from
                    .iter()
                    .find_map(|a| a.get(id))
                    .unwrap_or_else(|| panic!("{leg} id {id}: no Arrow row"))[k]
                    .clone();
            }
        }
        let n = counts_of(rows, canons);
        let c = counts
            .get_mut(leg)
            .unwrap_or_else(|| panic!("no counts for {leg}"));
        c[1 + 2 * k] = n[1 + 2 * k];
        c[2 + 2 * k] = n[2 + 2 * k];
    }
}

/// (leg, sample literal, id, canonical value, canonical source value).
type Graded = (&'static str, String, i64, Option<String>, Option<String>);

/// What one column showed across the stages, as `grade_column` grades it.
#[derive(Default)]
struct Seen {
    /// (stage, delivered type) per stage that carries a Parquet schema.
    types: Vec<(&'static str, String)>,
    /// Every leg graded against the source.
    values: Vec<Graded>,
    /// The ClickHouse column type, when a warehouse leg ran.
    clickhouse: Option<Option<String>>,
}

/// The ledger verdict on one column: wrong types and values, excused only by the class a marker declares, and every marker or defect sample that no longer fires.
fn grade_column(what: &str, r: &Row, seen: &Seen) -> Vec<String> {
    let mut bad = Vec::new();
    let (kd, chd) = (r.known_defect.is_some(), r.clickhouse_defect.is_some());
    let (mut kd_hits, mut ch_hits) = (0usize, 0usize);
    let mut hit_samples: std::collections::BTreeSet<(bool, &str)> = Default::default();
    for (mode, got) in &seen.types {
        let want = &r.delivery;
        if got == want {
            continue;
        }
        if kd && Some(got) == r.today_delivery.as_ref() {
            kd_hits += 1;
        } else {
            bad.push(format!(
                "{what}: {mode} delivers `{got}`, the ledger says `{want}` (today {:?})",
                r.today_delivery
            ));
        }
    }
    for (leg, lit, id, got, src) in &seen.values {
        if got == src {
            continue;
        }
        if r.defect_samples.contains(lit) {
            kd_hits += 1;
            hit_samples.insert((false, lit));
        } else if leg.starts_with("ClickHouse") && r.clickhouse_defect_samples.contains(lit) {
            ch_hits += 1;
            hit_samples.insert((true, lit));
        } else {
            bad.push(format!("{what} id {id}: {leg} {got:?}, source {src:?}"));
        }
    }
    if let Some(got) = &seen.clickhouse
        && *got != r.clickhouse
    {
        let today = *got == r.today_clickhouse;
        if kd && today {
            kd_hits += 1;
        } else if chd && today {
            ch_hits += 1;
        } else {
            bad.push(format!(
                "{what}: ClickHouse holds `{got:?}`, the ledger says `{:?}` (today {:?})",
                r.clickhouse, r.today_clickhouse
            ));
        }
    }
    if kd && kd_hits == 0 {
        bad.push(format!(
            "{what}: known_defect row now passes — remove the marker"
        ));
    }
    if chd && ch_hits == 0 {
        bad.push(format!(
            "{what}: clickhouse_defect row now passes in ClickHouse — remove the marker"
        ));
    }
    for (ch, list) in [
        (false, &r.defect_samples),
        (true, &r.clickhouse_defect_samples),
    ] {
        for s in list {
            if !hit_samples.contains(&(ch, s.as_str())) {
                bad.push(format!(
                    "{what}: defect sample `{s}` now matches the source — drop it from the ledger"
                ));
            }
        }
    }
    bad
}

/// A ledger row with only the fields `grade_column` reads.
fn graded_row(delivery: &str) -> Row {
    Row {
        native: "T".into(),
        sample: Vec::new(),
        delivery: delivery.into(),
        render: Render::default(),
        known_defect: None,
        today_delivery: None,
        today_clickhouse: None,
        defect_samples: Vec::new(),
        clickhouse: None,
        clickhouse_defect: None,
        clickhouse_defect_samples: Vec::new(),
        batch_refuses: false,
    }
}

#[test]
fn grade_column_excuses_only_the_declared_defect_class_and_flags_stale_markers() {
    let s = |v: &str| Some(v.to_string());
    let value = |leg: &'static str, lit: &str, got: &str, src: &str| {
        (leg, lit.to_string(), 1, s(got), s(src))
    };
    let kd = Row {
        known_defect: s("ADR-0038 step"),
        today_delivery: s("Boolean"),
        defect_samples: vec!["5".into(), "-1".into()],
        ..graded_row("Int8")
    };
    let chd = Row {
        clickhouse: s("Array(Nullable(String))"),
        today_clickhouse: s("Array(Nullable(String))"),
        clickhouse_defect: s("ADR-0038 step"),
        clickhouse_defect_samples: vec!["NULL".into()],
        ..graded_row("List(Utf8)")
    };
    let today = vec![("batch", "Boolean".to_string())];
    let cases: Vec<(&str, &Row, Seen, Vec<&str>)> = vec![
        (
            "an excused sample",
            &kd,
            Seen {
                types: today.clone(),
                values: vec![
                    value("batch", "5", "1", "5"),
                    value("batch", "-1", "1", "-1"),
                ],
                ..Seen::default()
            },
            vec![],
        ),
        (
            "an unexcused mismatch on a known_defect row",
            &kd,
            Seen {
                types: today.clone(),
                values: vec![
                    value("batch", "5", "1", "5"),
                    value("batch", "-1", "1", "-1"),
                    value("CDC final image", "'x'", "y", "x"),
                ],
                ..Seen::default()
            },
            vec!["c0 T id 1: CDC final image Some(\"y\"), source Some(\"x\")"],
        ),
        (
            "a clickhouse_defect sample on the batch leg",
            &chd,
            Seen {
                values: vec![
                    value("batch", "NULL", "[]", "NULL"),
                    value("ClickHouse", "NULL", "[]", "NULL"),
                ],
                clickhouse: Some(s("Array(Nullable(String))")),
                ..Seen::default()
            },
            vec!["c0 T id 1: batch Some(\"[]\"), source Some(\"NULL\")"],
        ),
        (
            "a stale marker",
            &kd,
            Seen {
                types: vec![("batch", "Int8".to_string())],
                ..Seen::default()
            },
            vec![
                "c0 T: known_defect row now passes — remove the marker",
                "c0 T: defect sample `5` now matches the source — drop it from the ledger",
                "c0 T: defect sample `-1` now matches the source — drop it from the ledger",
            ],
        ),
        (
            "a stale sample",
            &kd,
            Seen {
                types: today.clone(),
                values: vec![value("batch", "5", "1", "5")],
                ..Seen::default()
            },
            vec!["c0 T: defect sample `-1` now matches the source — drop it from the ledger"],
        ),
        (
            "a delivery that is neither the target nor today's",
            &kd,
            Seen {
                types: vec![
                    ("batch", "Boolean".to_string()),
                    ("cdc stream", "Utf8".to_string()),
                ],
                values: vec![
                    value("batch", "5", "1", "5"),
                    value("batch", "-1", "1", "-1"),
                ],
                ..Seen::default()
            },
            vec![
                "c0 T: cdc stream delivers `Utf8`, the ledger says `Int8` (today Some(\"Boolean\"))",
            ],
        ),
        (
            "a ClickHouse type that is neither the target nor today's",
            &chd,
            Seen {
                values: vec![value("ClickHouse", "NULL", "[]", "NULL")],
                clickhouse: Some(s("Nullable(String)")),
                ..Seen::default()
            },
            vec![
                "c0 T: ClickHouse holds `Some(\"Nullable(String)\")`, the ledger says \
                 `Some(\"Array(Nullable(String))\")` (today Some(\"Array(Nullable(String))\"))",
            ],
        ),
    ];
    for (name, row, seen, want) in cases {
        assert_eq!(grade_column("c0 T", row, &seen), want, "{name}");
    }
    let samples = [
        ("number", "1.50E2"),
        ("timestamp", "2024-01-01 00:00:00.500000+00"),
        ("float32", "1.5"),
        ("float64", "2.25"),
        ("interval", "P1Y2M3DT4H5M6.5S"),
        ("round_micros", "2026-01-15 13:45:30.1266667"),
    ];
    assert_eq!(
        samples.map(|(c, _)| c),
        CANONS,
        "a canon without a sample here"
    );
    for (how, v) in samples {
        assert!(canon(&s(v), &s(how)).is_some(), "{how}");
    }
}

/// Every stage of one engine, read by one DuckDB session (the source by its client when DuckDB cannot attach it), against the ledger and each other; returns every violation.
fn duckdb_ledger_verdict(lg: &Ledger, st: &Stand, n: i64) -> Vec<String> {
    let cols = columns(&lg.rows);
    let canons: Vec<Option<String>> = lg.rows.iter().map(|r| r.render.canon.clone()).collect();
    let (bdir, cdir) = (st.batch.out_dir(), st.cdc.out_dir());
    let (bc, cc) = (
        st.batch.oracle_container_out(),
        st.cdc.oracle_container_out(),
    );
    let (bs, cs, ss) = (schema(&bdir), schema(&cdir), schema(&cdir.join("snapshot")));
    let proj = projection(&lg.rows);
    let mut setup = format!(
        "{} {} \
         CREATE TEMP VIEW fin AS SELECT * EXCLUDE (rn) FROM (SELECT *, row_number() OVER \
           (PARTITION BY id ORDER BY __pos IS NULL, __seq DESC) AS rn FROM read_parquet(\
           ['{cc}/snapshot/*.parquet', '{cc}/*.parquet'], union_by_name = true)) \
           WHERE rn = 1 AND coalesce(__op, '') <> 'delete'; \
         CREATE TEMP VIEW legs AS \
           SELECT 'batch' AS leg, CAST(id AS VARCHAR) AS id, {proj} FROM read_parquet('{bc}/*.parquet') \
           UNION ALL SELECT 'snapshot', CAST(id AS VARCHAR), {proj} FROM read_parquet('{cc}/snapshot/*.parquet') \
           UNION ALL SELECT 'stream', CAST(id AS VARCHAR), {proj} FROM read_parquet('{cc}/*.parquet') \
             WHERE __op IN ('insert', 'update') \
           UNION ALL SELECT 'final', CAST(id AS VARCHAR), {proj} FROM fin \
           UNION ALL SELECT 'batch_stream', CAST(id AS VARCHAR), {proj} FROM read_parquet('{bc}/*.parquet') \
             WHERE id IN (SELECT id FROM read_parquet('{cc}/*.parquet') WHERE __op IN ('insert', 'update')) \
           UNION ALL SELECT 'batch_snapshot', CAST(id AS VARCHAR), {proj} FROM read_parquet('{bc}/*.parquet') \
             WHERE id BETWEEN {} AND {}",
        state_attach(&st.batch, "bst"),
        state_attach(&st.cdc, "cst"),
        n + 1,
        2 * n,
    );
    if let Source::Attach { engine, database } = &st.source {
        let (attach, from) = engine.source_sql(database, &st.bare);
        let from = server_rendered(&lg.rows, &from, engine.passthrough(), &st.bare);
        setup = format!(
            "{} {} {attach} {setup} UNION ALL SELECT 'source', CAST(id AS VARCHAR), {proj} FROM {from}",
            engine.load_sql(),
            engine.scanner_settings()
        );
    }
    // What `rivet load` put into ClickHouse is the CDC delivery.
    let wh_ns: Vec<_> = cols.iter().map(|c| ns_type(&cs, c)).collect();
    if let Some(w) = &st.warehouse {
        setup = format!(
            "INSTALL httpfs; LOAD httpfs; {setup} UNION ALL SELECT 'warehouse', CAST(id AS VARCHAR), {proj} FROM {}",
            warehouse_relation(&lg.rows, w, false, &wh_ns)
        );
        if lg.rows.iter().any(|r| naive_ts(&r.delivery)) {
            setup = format!(
                "{setup} UNION ALL SELECT 'warehouse_text', CAST(id AS VARCHAR), {proj} FROM {}",
                warehouse_relation(&lg.rows, w, true, &wh_ns)
            );
        }
    }
    let stats = cols
        .iter()
        .map(|c| format!("count({c}), count(DISTINCT {c})"))
        .collect::<Vec<_>>()
        .join(", ");
    let warehouse_types = st.warehouse.as_ref().map(|w| {
        (
            "ch_types",
            format!(
                "SELECT decode(name), decode(type) FROM {}",
                clickhouse_relation(&format!(
                    "SELECT name, type FROM system.columns WHERE database = '{}' AND table = '{}'",
                    w.db, w.view
                ))
            ),
        )
    });
    let mut queries = vec![
        ("legs", "SELECT * FROM legs".to_string()),
        (
            "counts",
            format!("SELECT leg, count(*), {stats} FROM legs GROUP BY leg"),
        ),
        // One query per state table: DuckDB 1.5.5 reuses one result for identical
        // aggregate subplans over same-named tables in two attached sqlite DBs (a UNION
        // ALL of ungrouped aggregates, a pair of scalar subqueries; measured 2026-09-30).
        (
            "bst_metrics",
            "SELECT export_name, sum(total_rows) FROM bst.export_metrics GROUP BY 1".to_string(),
        ),
        (
            "cst_metrics",
            "SELECT export_name, sum(total_rows) FROM cst.export_metrics GROUP BY 1".to_string(),
        ),
        (
            "bst_files",
            "SELECT export_name, sum(row_count) FROM bst.file_log GROUP BY 1".to_string(),
        ),
        (
            "cst_files",
            "SELECT export_name, sum(row_count) FROM cst.file_log GROUP BY 1".to_string(),
        ),
        (
            "bst_runs",
            "SELECT export_name, status, count(*) FROM bst.run_status GROUP BY 1, 2".to_string(),
        ),
        (
            "cst_runs",
            "SELECT export_name, status, count(*) FROM cst.run_status GROUP BY 1, 2".to_string(),
        ),
        (
            "cst_snapshot",
            "SELECT export_name, table_name, count(*) FROM cst.cdc_snapshot \
             WHERE completed_at IS NOT NULL GROUP BY 1, 2"
                .to_string(),
        ),
        (
            "parts",
            format!(
                "SELECT 'batch', count(*) FROM read_parquet('{bc}/*.parquet') \
                 UNION ALL SELECT 'snapshot', count(*) FROM read_parquet('{cc}/snapshot/*.parquet') \
                 UNION ALL SELECT 'stream', count(*) FROM read_parquet('{cc}/*.parquet')"
            ),
        ),
    ];
    if let Source::Attach { engine, database } = &st.source {
        let (_, from) = engine.source_sql(database, &st.bare);
        queries.push(("source_types", format!("DESCRIBE SELECT * FROM {from}")));
        for (r, c) in lg.rows.iter().zip(&cols) {
            if r.render.duck.as_deref() == Some(ARROW) {
                assert!(
                    !queries.iter().any(|(q, _)| *q == "arrow_source"),
                    "one `duck: arrow` row per engine"
                );
                if let Some(w) = &st.warehouse {
                    queries.push((
                        "arrow_warehouse",
                        format!(
                            "SELECT decode(i), decode(v) FROM {}",
                            clickhouse_relation(&format!(
                                "SELECT toString(id) AS i, toString({c}) AS v FROM {}.{} WHERE NOT __is_deleted",
                                w.db, w.view
                            ))
                        ),
                    ));
                }
                queries.push((
                    "arrow_source",
                    format!(
                        "SELECT CAST(id AS VARCHAR), CAST({c} AS VARCHAR) FROM {}",
                        engine.passthrough().replace(
                            "{sql}",
                            &format!("SELECT id, {c}::text AS {c} FROM {}", st.bare)
                        )
                    ),
                ));
            }
        }
    }
    queries.extend(warehouse_types);
    let out = duckdb_session_json(&setup, &queries);
    if std::env::var_os("LEDGER_DEBUG").is_some() {
        eprintln!("{}", serde_json::to_string_pretty(&out).unwrap());
    }

    let mut legs: BTreeMap<String, Cells> = BTreeMap::new();
    let mut by_leg: BTreeMap<String, Vec<Vec<Option<String>>>> = BTreeMap::new();
    for mut r in cells(&out["legs"]) {
        let leg = r.remove(0).expect("leg");
        by_leg.entry(leg).or_default().push(r);
    }
    for (leg, rows) in by_leg {
        legs.insert(leg, keyed(rows));
    }
    let mut counts: BTreeMap<String, Vec<i64>> = cells(&out["counts"])
        .into_iter()
        .map(|mut r| {
            let leg = r.remove(0).expect("leg");
            (
                leg,
                r.iter()
                    .map(|v| v.as_deref().unwrap().parse().unwrap())
                    .collect(),
            )
        })
        .collect();
    match &st.source {
        Source::Client { text_rows, render } => {
            let exprs = lg
                .rows
                .iter()
                .zip(&cols)
                .map(|(r, c)| {
                    r.render
                        .source
                        .as_deref()
                        .unwrap_or(render)
                        .replace("{c}", c)
                })
                .collect::<Vec<_>>()
                .join(", ");
            let id = render.replace("{c}", "id");
            let src = keyed(text_rows(&format!(
                "SELECT {id}, {exprs} FROM {}",
                st.table
            )));
            counts.insert("source".into(), counts_of(&src, &canons));
            legs.insert("source".into(), src);
        }
        Source::Attach { .. } => {
            for (r, c) in lg.rows.iter().zip(&cols) {
                assert!(
                    r.render.source.is_none(),
                    "{}: {c} names a client-side source render, but DuckDB reads this source",
                    st.engine
                );
            }
        }
    }
    let (a_b, a_s, a_snap) = (
        arrow_cells(&bdir, &cols, None),
        arrow_cells(&cdir, &cols, Some(&["update", "insert"])),
        arrow_cells(&cdir.join("snapshot"), &cols, None),
    );
    for (k, c) in cols.iter().enumerate() {
        if [&bs, &cs, &ss].iter().any(|s| duck_narrows(s, c)) {
            arrow_overlay(&mut legs, &mut counts, k, &canons, &a_b, &a_s, &a_snap);
        }
    }
    let arrow_source = keyed(out.get("arrow_source").map(cells).unwrap_or_default());
    let arrow_warehouse = keyed(out.get("arrow_warehouse").map(cells).unwrap_or_default());
    let ch_types: BTreeMap<String, String> = out
        .get("ch_types")
        .map(cells)
        .unwrap_or_default()
        .into_iter()
        .map(|r| (r[0].clone().unwrap(), r[1].clone().unwrap()))
        .collect();

    let all: Vec<i64> = (1..=3 * n).collect();
    let captured: Vec<i64> = std::iter::once(1).chain(n + 1..=3 * n).collect();
    let snapshot: Vec<i64> = (1..=2 * n).collect();
    let wh_text = legs.contains_key("warehouse_text");
    let mut bad = Vec::new();
    for (leg, want) in [
        ("source", &all),
        ("batch", &all),
        ("final", &all),
        ("stream", &captured),
        ("snapshot", &snapshot),
    ]
    .into_iter()
    .chain(st.warehouse.as_ref().map(|_| ("warehouse", &all)))
    .chain(wh_text.then_some(("warehouse_text", &all)))
    {
        let got: Vec<i64> = legs
            .get(leg)
            .map(|c| c.keys().copied().collect())
            .unwrap_or_default();
        if &got != want {
            bad.push(format!("{leg}: ids {got:?}, want {want:?}"));
        }
    }
    if !bad.is_empty() {
        return bad;
    }
    let v = |leg: &str, id: i64, k: usize| canon(&legs[leg][&id][k], &canons[k]);
    // The sample literal source id `id` holds: `NULL` for id 1 (the rewrite nulls it) and past a row's samples.
    let literal = |r: &Row, id: i64| -> String {
        (id != 1)
            .then(|| r.sample.get(((id - 1) % n) as usize).cloned())
            .flatten()
            .unwrap_or_else(|| "NULL".into())
    };
    // Snapshot id i holds what batch id `snap_twin(i)` holds (ids n+1..=2n were seeded NULL, like batch id 1).
    let snap_twin = |i: i64| if i <= n { n + i } else { 1 };
    for (k, (col, r)) in cols.iter().zip(&lg.rows).enumerate() {
        let what = format!("{col} {}", r.native);
        let arrow_row = r.render.duck.as_deref() == Some(ARROW);
        let mut seen = Seen {
            types: [("batch", &bs), ("cdc stream", &cs), ("cdc snapshot", &ss)]
                .into_iter()
                .map(|(mode, sch)| (mode, delivered(sch, col)))
                .collect(),
            values: Vec::new(),
            clickhouse: st
                .warehouse
                .as_ref()
                .map(|_| ch_types.get(col.as_str()).cloned()),
        };
        for &id in &all {
            let src = if arrow_row {
                canon(&arrow_source[&id][0], &canons[k])
            } else {
                v("source", id, k)
            };
            let mut push = |leg: &'static str, got: Option<String>| {
                seen.values
                    .push((leg, literal(r, id), id, got, src.clone()))
            };
            if arrow_row {
                push("batch", canon(&a_b[&id][k], &canons[k]));
            } else {
                push("batch", v("batch", id, k));
                push("CDC final image", v("final", id, k));
            }
            if st.warehouse.is_some() {
                push(
                    "ClickHouse",
                    if arrow_row {
                        canon(&arrow_warehouse[&id][0], &canons[k])
                    } else {
                        v("warehouse", id, k)
                    },
                );
            }
            if wh_text && naive_ts(&r.delivery) {
                push(
                    "ClickHouse text (session_timezone Asia/Tokyo)",
                    v("warehouse_text", id, k),
                );
            }
        }
        bad.extend(grade_column(&what, r, &seen));
        for &id in &captured {
            if !arrow_row && v("stream", id, k) != v("batch", id, k) {
                bad.push(format!(
                    "{what} id {id} (duckdb): cdc {:?}, batch {:?}",
                    v("stream", id, k),
                    v("batch", id, k)
                ));
            }
            if a_s[&id][k] != a_b[&id][k] {
                bad.push(format!(
                    "{what} id {id} (arrow): cdc {:?}, batch {:?}",
                    a_s[&id][k], a_b[&id][k]
                ));
            }
        }
        for &i in &snapshot {
            let twin = snap_twin(i);
            if !arrow_row && v("snapshot", i, k) != v("batch", twin, k) {
                bad.push(format!(
                    "{what} snapshot id {i} (duckdb): {:?}, batch id {twin} {:?}",
                    v("snapshot", i, k),
                    v("batch", twin, k)
                ));
            }
            if a_snap[&i][k] != a_b[&twin][k] {
                bad.push(format!(
                    "{what} snapshot id {i} (arrow): {:?}, batch id {twin} {:?}",
                    a_snap[&i][k], a_b[&twin][k]
                ));
            }
        }
        if !arrow_row {
            let stat = |leg: &str| (counts[leg][1 + 2 * k], counts[leg][2 + 2 * k]);
            // A leg with named defect samples is graded against the source per id above.
            let (kd_values, ch_values) = (
                !r.defect_samples.is_empty(),
                !r.clickhouse_defect_samples.is_empty(),
            );
            for (leg, of, excused) in [
                ("batch", "source", kd_values),
                ("final", "source", kd_values),
                ("stream", "batch_stream", false),
                ("snapshot", "batch_snapshot", false),
                ("warehouse", "source", kd_values || ch_values),
            ] {
                if excused || leg == "warehouse" && st.warehouse.is_none() {
                    continue;
                }
                if stat(leg) != stat(of) {
                    bad.push(format!(
                        "{what}: {leg} has (non-null, distinct) {:?}, {of} {:?}",
                        stat(leg),
                        stat(of)
                    ));
                }
            }
        }
    }
    for (leg, of) in [
        ("batch", "source"),
        ("final", "source"),
        ("stream", "batch_stream"),
        ("warehouse", "source"),
    ] {
        if leg == "warehouse" && st.warehouse.is_none() {
            continue;
        }
        if counts[leg][0] != counts[of][0] {
            bad.push(format!(
                "{leg}: COUNT(*) {}, {of} {}",
                counts[leg][0], counts[of][0]
            ));
        }
    }
    if counts["snapshot"][0] != 2 * n {
        bad.push(format!(
            "snapshot: COUNT(*) {}, want {}",
            counts["snapshot"][0],
            2 * n
        ));
    }
    bad.extend(state_violations(&out, st, n));
    bad
}

/// The rows and runs rivet recorded in both state DBs against what DuckDB reads in the parts.
fn state_violations(out: &serde_json::Value, st: &Stand, n: i64) -> Vec<String> {
    let table = |q: &str| -> BTreeMap<String, i64> {
        cells(&out[q])
            .into_iter()
            .map(|mut r| {
                let v = r.pop().unwrap().map_or(0, |v| v.parse().unwrap());
                (
                    r.into_iter()
                        .map(Option::unwrap)
                        .collect::<Vec<_>>()
                        .join("/"),
                    v,
                )
            })
            .collect()
    };
    let parts: BTreeMap<String, i64> = table("parts");
    let (bname, cname) = (st.batch.export_name(), st.cdc.export_name());
    let captured = cells(&out["cst_snapshot"])
        .into_iter()
        .find(|r| r[0].as_deref() == Some(cname))
        .and_then(|r| r[1].clone())
        .unwrap_or_default();
    let snap = format!("{cname}__snapshot_{captured}");
    let got = |q: &str, key: &str| table(q).get(key).copied();
    let mut bad = Vec::new();
    for (what, recorded, want) in [
        ("batch parts", Some(parts["batch"]), 3 * n),
        ("batch export_metrics", got("bst_metrics", bname), 3 * n),
        ("batch file_log", got("bst_files", bname), 3 * n),
        (
            "batch successful runs",
            got("bst_runs", &format!("{bname}/success")),
            1,
        ),
        ("snapshot parts", Some(parts["snapshot"]), 2 * n),
        ("snapshot export_metrics", got("cst_metrics", &snap), 2 * n),
        ("snapshot file_log", got("cst_files", &snap), 2 * n),
        (
            "snapshot run",
            got("cst_runs", &format!("{snap}/success")),
            1,
        ),
        ("stream parts", Some(parts["stream"]), 2 * n + 1),
        (
            "stream export_metrics",
            got("cst_metrics", cname),
            2 * n + 1,
        ),
        ("stream file_log", got("cst_files", cname), 2 * n + 1),
        (
            "stream runs",
            got("cst_runs", &format!("{cname}/success")),
            2,
        ),
        (
            "completed cdc_snapshot row",
            got("cst_snapshot", &format!("{cname}/{captured}")),
            1,
        ),
    ] {
        if recorded != Some(want) {
            bad.push(format!(
                "state: {what} is {recorded:?}, DuckDB reads {want}"
            ));
        }
    }
    for q in ["bst_runs", "cst_runs"] {
        for (key, count) in table(q) {
            // A shared Postgres state backend holds other tests' runs too.
            let ours = key.starts_with(bname) || key.starts_with(cname);
            if ours && !key.ends_with("/success") {
                bad.push(format!("state: {q} holds {count} run(s) `{key}`"));
            }
        }
    }
    bad
}

/// Rows 1..=n of the batch-refused table hold the samples; returns n.
fn seed_refused(rows: &[Row], st: &Stand) -> usize {
    let cols = columns(rows);
    let n = rows.iter().map(|r| r.sample.len()).max().unwrap();
    for i in 0..n {
        let vals = values(rows, i);
        (st.exec)(&format!(
            "INSERT INTO {} (id, {}) VALUES ({}, {})",
            st.table,
            cols.join(", "),
            i + 1,
            vals.join(", ")
        ));
    }
    (st.settle)(n as i64);
    n
}

/// Rows batch refuses today (known defects): batch must still refuse each by name, CDC deliver the target or its server text, and CDC values equal the source's own text; returns every violation.
fn duckdb_refused_verdict(
    rows: &[Row],
    st: &Stand,
    n: usize,
    batch_of: &dyn Fn(&str) -> Rig,
) -> Vec<String> {
    let cols = columns(rows);
    let cc = st.cdc.oracle_container_out();
    let cs = schema(&st.cdc.out_dir());
    let Source::Attach { engine, database } = &st.source else {
        panic!(
            "{}: batch-refused rows need a DuckDB-attached source",
            st.engine
        )
    };
    assert!(
        rows.iter().all(|r| r.known_defect.is_some()),
        "{}: batch-refused rows are known defects, graded against the server's own text",
        st.engine
    );
    let server_text = engine.server_text().unwrap_or_else(|| {
        panic!(
            "{}: batch-refused rows need the server's own text",
            st.engine
        )
    });
    let (attach, _) = engine.source_sql(database, &st.bare);
    let server_sql = format!(
        "SELECT id, {} FROM {}",
        cols.iter()
            .map(|c| format!("{} AS {c}", server_text.replace("{c}", c)))
            .collect::<Vec<_>>()
            .join(", "),
        st.bare
    );
    let from = engine
        .passthrough()
        .replace("{sql}", &server_sql.replace('\'', "''"));
    let proj = projection(rows);
    let stats = cols
        .iter()
        .map(|c| format!("count({c}), count(DISTINCT {c})"))
        .collect::<Vec<_>>()
        .join(", ");
    let setup = format!(
        "{} {} {attach} CREATE TEMP VIEW legs AS \
           SELECT 'source' AS leg, CAST(id AS VARCHAR) AS id, {proj} FROM {from} \
           UNION ALL SELECT 'stream', CAST(id AS VARCHAR), {proj} FROM read_parquet('{cc}/*.parquet') \
             WHERE __op = 'insert'",
        engine.load_sql(),
        engine.scanner_settings()
    );
    let out = duckdb_session_json(
        &setup,
        &[
            ("legs", "SELECT * FROM legs".to_string()),
            (
                "counts",
                format!("SELECT leg, count(*), {stats} FROM legs GROUP BY leg ORDER BY leg"),
            ),
        ],
    );
    let mut legs: BTreeMap<String, Vec<Vec<Option<String>>>> = BTreeMap::new();
    for mut r in cells(&out["legs"]) {
        let leg = r.remove(0).expect("leg");
        legs.entry(leg).or_default().push(r);
    }
    let (src, got) = (
        keyed(legs.remove("source").unwrap_or_default()),
        keyed(legs.remove("stream").unwrap_or_default()),
    );
    let mut bad = Vec::new();
    if got.len() != n || src.len() != n {
        bad.push(format!(
            "{} source rows and {} captured inserts, want {n} of each",
            src.len(),
            got.len()
        ));
        return bad;
    }
    // Rows ordered by leg: source, stream. A column DuckDB narrows takes Arrow's cells and is recounted over them.
    let mut counts = cells(&out["counts"]);
    let narrowed: Vec<usize> = (0..cols.len())
        .filter(|&k| duck_narrows(&cs, &cols[k]))
        .collect();
    let mut got = got;
    if !narrowed.is_empty() {
        let a = arrow_cells(&st.cdc.out_dir(), &cols, Some(&["insert"]));
        let canons: Vec<_> = rows.iter().map(|r| r.render.canon.clone()).collect();
        for (id, row) in got.iter_mut() {
            for &k in &narrowed {
                row[k] = a[id][k].clone();
            }
        }
        for (i, leg) in [&src, &got].into_iter().enumerate() {
            let n = counts_of(leg, &canons);
            for &k in &narrowed {
                counts[i][2 + 2 * k] = Some(n[1 + 2 * k].to_string());
                counts[i][3 + 2 * k] = Some(n[2 + 2 * k].to_string());
            }
        }
    }
    if counts[0][1..] != counts[1][1..] {
        bad.push(format!(
            "(COUNT(*), COUNT(col), COUNT(DISTINCT col)) per leg differ: {counts:?}"
        ));
    }
    for (k, (col, c)) in cols.iter().zip(rows).enumerate() {
        let what = format!("{col} {}", c.native);
        let run = batch_of(col).run_args(&[]);
        let said = format!(
            "{}{}",
            String::from_utf8_lossy(&run.stdout),
            String::from_utf8_lossy(&run.stderr)
        );
        let by_name = said.contains(&format!("'{col}'")) || said.contains(&format!("• {col} ("));
        if run.status.success() || !by_name {
            bad.push(format!(
                "{what}: known_defect row now passes in batch (no refusal by name) — declare what it \
                 delivers and remove the marker:\n{said}"
            ));
        }
        let d = delivered(&cs, col);
        if d != c.delivery && Some(&d) != c.today_delivery.as_ref() {
            bad.push(format!(
                "{what}: cdc delivers `{d}`, neither the target `{}` nor today's {:?}",
                c.delivery, c.today_delivery
            ));
        }
        for (id, row) in &got {
            let (s, g) = (
                canon(&src[id][k], &c.render.canon),
                canon(&row[k], &c.render.canon),
            );
            if s != g {
                bad.push(format!(
                    "{what} id {id}: cdc renders {g:?}, the source {s:?}"
                ));
            }
        }
    }
    bad
}

/// The `CREATE TABLE` column list for the ledger rows after `id_ddl`.
fn columns_ddl(rows: &[Row], id_ddl: &str) -> String {
    std::iter::once(id_ddl.to_string())
        .chain(
            rows.iter()
                .enumerate()
                .map(|(i, r)| format!("c{i} {}", r.native)),
        )
        .collect::<Vec<_>>()
        .join(", ")
}

/// Put `rig`'s parts and state DB where DuckDB reads them.
fn graded(rig: Rig) -> Rig {
    rig.census_oracle()
}

/// The batch rig's four-way census (source, parts, metrics, file_log, manifests) must agree at `want` rows.
fn census_violations(rig: &Rig, want: i64) -> Vec<String> {
    let c = rig.row_census();
    if c.agrees() && c.source == want {
        Vec::new()
    } else {
        vec![format!("batch row census disagrees (want {want}): {c:?}")]
    }
}

#[test]
#[ignore = "live: requires docker compose postgres-cdc (wal_level=logical) + duckdb"]
fn postgres_batch_and_cdc_deliver_every_ledger_row_alike() {
    use postgres::NoTls;
    let (lg, refused) = split_refused(ledger("postgres"));
    let connect = || postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    for s in &lg.setup {
        connect().batch_execute(s).unwrap();
    }
    let exec = |q: &str| {
        connect()
            .batch_execute(q)
            .unwrap_or_else(|e| panic!("{q}: {e}"))
    };
    let stand = |rows: &[Row], label: &str, snapshot: bool| {
        let table = unique_name(label);
        let slot = unique_name(&format!("{label}_slot"));
        exec(&format!(
            "CREATE TABLE {table} ({})",
            columns_ddl(rows, "id BIGINT PRIMARY KEY")
        ));
        let wslot = unique_name(&format!("{label}_ch_slot"));
        let guards = (
            PgTable::adopt_on(POSTGRES_CDC_URL, table.clone()),
            Slot(slot.clone()),
            Slot(wslot.clone()),
        );
        let mut cdc = Rig::pg_cdc(&table, &slot);
        if snapshot {
            cdc = cdc.cdc_line("initial: snapshot");
        }
        let batch = Rig::pg_batch(&table)
            .export_named(&format!("{table}_batch"))
            .source_url(POSTGRES_CDC_URL);
        let wh = snapshot.then(|| warehouse(Rig::pg_cdc(&table, &wslot), &table));
        let st = Stand {
            engine: "postgres",
            bare: table.clone(),
            table,
            exec: &exec,
            source: Source::Attach {
                engine: OracleEngine::PostgresCdc,
                database: "rivet",
            },
            settle: &|_| {},
            cdc: graded(cdc),
            batch: graded(batch),
            warehouse: wh,
        };
        (st, guards)
    };
    let (st, _g) = stand(&lg.rows, "ledger_pg", true);
    let n = seed(&lg, &st);
    capture(&st);
    rewrite(&lg, &st, n);
    capture(&st);
    load(&st);
    st.batch.run_ok();
    let mut bad = duckdb_ledger_verdict(&lg, &st, n);
    bad.extend(census_violations(&st.batch, 3 * n));
    assert!(
        bad.is_empty(),
        "{} ledger violations:\n{}",
        bad.len(),
        bad.join("\n")
    );

    let (st, _g2) = stand(&refused, "ledger_pg_refused", false);
    st.cdc.run_ok();
    let n = seed_refused(&refused, &st);
    st.cdc.run_ok();
    let t = st.table.clone();
    let bad = duckdb_refused_verdict(&refused, &st, n, &|col| {
        Rig::pg_batch(&t)
            .export_named(&format!("{t}_{col}"))
            .source_url(POSTGRES_CDC_URL)
            .query(&format!("SELECT id, {col} FROM {t}"))
    });
    assert!(
        bad.is_empty(),
        "{} violations on batch-refused rows:\n{}",
        bad.len(),
        bad.join("\n")
    );
}

#[test]
#[ignore = "live: requires docker compose --profile cdc mysql-cdc + duckdb"]
fn mysql_batch_and_cdc_deliver_every_ledger_row_alike() {
    use mysql::prelude::Queryable;
    let lg = ledger("mysql");
    let table = unique_name("ledger_my");
    cdc_conn()
        .query_drop(format!(
            "CREATE TABLE {table} ({}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
            columns_ddl(&lg.rows, "id BIGINT PRIMARY KEY")
        ))
        .unwrap();
    let _t = MysqlCdcTable(table.clone());
    let exec = |q: &str| {
        cdc_conn()
            .query_drop(q)
            .unwrap_or_else(|e| panic!("{q}: {e}"))
    };
    let cdc = Rig::mysql_cdc(&table).cdc_line("initial: snapshot");
    let batch = Rig::mysql_batch(&table)
        .export_named(&format!("{table}_batch"))
        .source_url(MYSQL_CDC_URL);
    let st = Stand {
        engine: "mysql",
        bare: table.clone(),
        table: table.clone(),
        exec: &exec,
        source: Source::Attach {
            engine: OracleEngine::MysqlCdc,
            database: "rivet",
        },
        settle: &|_| {},
        cdc: graded(cdc),
        batch: graded(batch),
        warehouse: Some(warehouse(Rig::mysql_cdc(&table), &table)),
    };
    let n = seed(&lg, &st);
    capture(&st);
    rewrite(&lg, &st, n);
    capture(&st);
    load(&st);
    st.batch.run_ok();
    let mut bad = duckdb_ledger_verdict(&lg, &st, n);
    bad.extend(census_violations(&st.batch, 3 * n));
    assert!(
        bad.is_empty(),
        "{} ledger violations:\n{}",
        bad.len(),
        bad.join("\n")
    );
}

#[test]
#[ignore = "live: requires docker compose mssql with SQL Server Agent + CDC + duckdb"]
fn mssql_batch_and_cdc_deliver_every_ledger_row_alike() {
    let _serial = cross_process_serial("mssql_cdc");
    let lg = ledger("mssql");
    let table = unique_name("ledger_ms");
    let ci = format!("dbo_{table}");
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table} ({})",
        columns_ddl(&lg.rows, "id INT PRIMARY KEY")
    ));
    let _t = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    enable_cdc(&table, &ci);
    let cdc = Rig::mssql_cdc(&table, &ci).cdc_line("initial: snapshot");
    let batch = Rig::mssql_batch(&table)
        .export_named(&format!("{table}_batch"))
        .source_url(MSSQL_CDC_URL);
    let st = Stand {
        engine: "mssql",
        bare: table.clone(),
        table: format!("dbo.{table}"),
        exec: &mssql_cdc_exec,
        source: Source::Attach {
            engine: OracleEngine::MssqlCdc,
            database: "rivet",
        },
        settle: &|rows| wait_for_capture(&ci, rows),
        cdc: graded(cdc),
        batch: graded(batch),
        warehouse: Some(warehouse(Rig::mssql_cdc(&table, &ci), &table)),
    };
    let n = seed(&lg, &st);
    capture(&st);
    rewrite(&lg, &st, n);
    capture(&st);
    load(&st);
    st.batch.run_ok();
    let mut bad = duckdb_ledger_verdict(&lg, &st, n);
    bad.extend(census_violations(&st.batch, 3 * n));
    assert!(
        bad.is_empty(),
        "{} ledger violations:\n{}",
        bad.len(),
        bad.join("\n")
    );
}

/// DuckDB has no Oracle scanner, so the source leg is Oracle's own rendering through its client; parts and state DBs are read by DuckDB.
#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites + duckdb"]
fn oracle_batch_and_cdc_deliver_every_ledger_row_alike() {
    let _serial = cross_process_serial("oracle_cdc");
    let lg = ledger("oracle");
    let t = crate::live_cdc_oracledb::cdc_table(
        "ledger_ora",
        &columns_ddl(&lg.rows, "id NUMBER(10) PRIMARY KEY"),
    );
    let cdc = Rig::oracle_cdc(t.name()).cdc_line("initial: snapshot");
    let batch = Rig::oracle_batch(t.name());
    let st = Stand {
        engine: "oracle",
        bare: t.name().to_string(),
        table: t.name().to_string(),
        exec: &ora_exec,
        source: Source::Client {
            text_rows: &ora_text_rows,
            render: "TO_CHAR({c})",
        },
        settle: &|_| {},
        cdc: graded(cdc),
        batch: graded(batch),
        warehouse: None,
    };
    let n = seed(&lg, &st);
    capture(&st);
    rewrite(&lg, &st, n);
    capture(&st);
    load(&st);
    st.batch.run_ok();
    let bad = duckdb_ledger_verdict(&lg, &st, n);
    assert!(
        bad.is_empty(),
        "{} ledger violations:\n{}",
        bad.len(),
        bad.join("\n")
    );
}
