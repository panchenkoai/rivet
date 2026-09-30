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
    over: Option<String>,
    render: Render,
    diverges: Option<String>,
    /// Why rivet misses this row's ADR target today; the row must keep failing until the named step fixes it.
    known_defect: Option<String>,
    /// The ClickHouse column type `rivet load` builds for this row (cdc rows).
    clickhouse: Option<String>,
    /// Like `known_defect`, for the ClickHouse stage alone.
    clickhouse_defect: Option<String>,
    /// Today's behaviour of a known_defect row: the batch run refuses the column by name.
    batch_refuses: bool,
}

struct Ledger {
    setup: Vec<String>,
    batch: Vec<Row>,
    cdc: Vec<Row>,
}

/// The ledger's rows for `engine`, the cdc rows paired with the batch rows by native type.
fn ledger(engine: &str) -> Ledger {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("docs/type-capability-matrix.yaml");
    let doc: serde_yaml_ng::Value =
        serde_yaml_ng::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let e = &doc["engines"][engine];
    let s = |v: &serde_yaml_ng::Value| v.as_str().map(str::to_string);
    let rows = |mode: &str| -> Vec<Row> {
        e[mode]
            .as_sequence()
            .unwrap_or_else(|| panic!("engines.{engine}.{mode} is not a list"))
            .iter()
            .map(|r| Row {
                native: s(&r["native_type"]).expect("native_type"),
                sample: r["sample"]
                    .as_sequence()
                    .expect("sample")
                    .iter()
                    .map(|v| s(v).expect("sample literal"))
                    .collect(),
                delivery: s(&r["delivery"]).expect("delivery"),
                over: s(&r["override"]),
                render: Render {
                    source: s(&r["render"]["source"]),
                    duck: s(&r["render"]["duck"]),
                    server: s(&r["render"]["server"]),
                    canon: s(&r["render"]["canon"]),
                },
                diverges: s(&r["diverges"]),
                known_defect: s(&r["known_defect"]),
                clickhouse: s(&r["clickhouse"]),
                clickhouse_defect: s(&r["clickhouse_defect"]),
                batch_refuses: r["batch_refuses"].as_bool().unwrap_or(false),
            })
            .collect()
    };
    let batch = rows("batch");
    let mut cdc = rows("cdc");
    cdc = batch
        .iter()
        .map(|b| {
            let i = cdc
                .iter()
                .position(|c| c.native == b.native)
                .unwrap_or_else(|| panic!("{engine}: `{}` has no cdc row", b.native));
            cdc.remove(i)
        })
        .collect();
    assert!(
        cdc.is_empty() || cdc.len() == batch.len(),
        "{engine}: cdc rows without a batch twin"
    );
    Ledger {
        setup: e["setup"]
            .as_sequence()
            .map(|v| v.iter().filter_map(s).collect())
            .unwrap_or_default(),
        batch,
        cdc,
    }
}

/// `render.duck` for a type DuckDB cannot read exactly (Decimal256 reads as DOUBLE, ADR-0038 CP11).
const ARROW: &str = "arrow";

/// The ledger without the rows batch refuses today, and those rows as (batch, cdc) twins.
fn split_refused(lg: Ledger) -> (Ledger, Vec<(Row, Row)>) {
    let (mut batch, mut cdc, mut refused) = (Vec::new(), Vec::new(), Vec::new());
    for (b, c) in lg.batch.into_iter().zip(lg.cdc) {
        if b.batch_refuses {
            refused.push((b, c));
        } else {
            batch.push(b);
            cdc.push(c);
        }
    }
    (
        Ledger {
            setup: lg.setup,
            batch,
            cdc,
        },
        refused,
    )
}

/// How the verdict reads the source.
enum Source<'a> {
    /// ATTACHed READ_ONLY in the verdict's own DuckDB session; `pass` runs server SQL there.
    Attach {
        engine: OracleEngine,
        database: &'static str,
        pass: &'static str,
        /// Scanner settings run before the ATTACH (a scanner must not narrow what it reads).
        settings: &'static str,
        /// The server's own text of `{c}` (its type output function), for `server_text` rows.
        server_text: &'static str,
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
fn warehouse(rig: Rig, rows: &[Row], view: &str) -> Warehouse {
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
        rig: match overrides(rows) {
            Some(line) => rig.export_line(&line),
            None => rig,
        },
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

/// The loaded view as DuckDB reads it, re-typed to each row's delivery (text from the bytes a ClickHouse `String` travels as, UUID from `FixedString(16)`, time of day from its decimal seconds, a naive timestamp from Parquet's UTC).
fn warehouse_relation(rows: &[Row], w: &Warehouse) -> String {
    let text: Vec<String> = rows
        .iter()
        .zip(columns(rows))
        .filter_map(|(r, c)| {
            let d = r.delivery.as_str();
            let e = if d == "Utf8"
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
            } else if d.starts_with("Timestamp(") && !d.contains(',') {
                format!("CAST({c} AS TIMESTAMP)")
            } else if d.starts_with("Time64(") {
                format!("TIME '00:00:00' + to_microseconds(CAST({c} * 1000000 AS BIGINT))")
            } else {
                return None;
            };
            Some(format!("{e} AS {c}"))
        })
        .collect();
    let from = clickhouse_relation(&format!(
        "SELECT * FROM {}.{} WHERE NOT __is_deleted",
        w.db, w.view
    ));
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

/// Arrow's own display of `cols` in every part under `dir` (only rows whose `__op` is in `ops`, when given), keyed by id.
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
                .map(|a| (!a.is_null(r)).then(|| array_value_to_string(a, r).unwrap()))
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

/// `v` canonicalised as the row's render says.
fn canon(v: &Option<String>, how: &Option<String>) -> Option<String> {
    v.as_ref().map(|s| match how.as_deref() {
        Some("number") => canon_num(s),
        Some("timestamp") => canon_ts(s.trim_start_matches('+').trim().trim_end_matches("+00")),
        Some("float32") => s
            .parse::<f32>()
            .map_or_else(|e| panic!("{s}: {e}"), |f| f.to_string()),
        Some("float64") => s
            .parse::<f64>()
            .map_or_else(|e| panic!("{s}: {e}"), |f| f.to_string()),
        Some("interval") => canon_interval(s),
        Some("datetime_tick") => {
            let s = s.trim_end_matches("+00");
            let (base, frac) = s.split_once('.').unwrap_or((s, "0"));
            let f: f64 = format!("0.{frac}").parse().unwrap();
            format!("{base}+{}/300", (f * 300.0).round() as i64)
        }
        Some(other) => panic!("unknown canon `{other}`"),
        None => s.clone(),
    })
}

/// An ISO 8601 duration (`P1Y2M3DT4H5M6.5S`) or DuckDB's interval text (`1 year 2 months 3 days 04:05:06.5`) as `<months>m<days>d<micros>us`.
fn canon_interval(s: &str) -> String {
    let (mut months, mut days, mut micros) = (0i64, 0i64, 0i64);
    let secs = |v: &str| -> i64 {
        let (neg, v) = v.strip_prefix('-').map_or((false, v), |r| (true, r));
        let (w, f) = v.split_once('.').unwrap_or((v, ""));
        let us =
            w.parse::<i64>().unwrap() * 1_000_000 + format!("{f:0<6}")[..6].parse::<i64>().unwrap();
        if neg { -us } else { us }
    };
    if let Some(iso) = s.strip_prefix('P') {
        let (date, time) = iso.split_once('T').unwrap_or((iso, ""));
        let mut num = String::new();
        for ch in date.chars() {
            match ch {
                'Y' => months += 12 * std::mem::take(&mut num).parse::<i64>().unwrap(),
                'M' => months += std::mem::take(&mut num).parse::<i64>().unwrap(),
                'D' => days += std::mem::take(&mut num).parse::<i64>().unwrap(),
                c => num.push(c),
            }
        }
        for ch in time.chars() {
            match ch {
                'H' => micros += 3_600_000_000 * std::mem::take(&mut num).parse::<i64>().unwrap(),
                'M' => micros += 60_000_000 * std::mem::take(&mut num).parse::<i64>().unwrap(),
                'S' => micros += secs(&std::mem::take(&mut num)),
                c => num.push(c),
            }
        }
    } else {
        let words: Vec<&str> = s.split_whitespace().collect();
        let mut i = 0;
        while i < words.len() {
            if let Some((h, rest)) = words[i].split_once(':') {
                let (m, sec) = rest.split_once(':').unwrap();
                let neg = h.starts_with('-');
                let us = h.trim_start_matches('-').parse::<i64>().unwrap() * 3_600_000_000
                    + m.parse::<i64>().unwrap() * 60_000_000
                    + secs(sec);
                micros += if neg { -us } else { us };
                i += 1;
                continue;
            }
            let v: i64 = words[i].parse().unwrap();
            match words[i + 1].trim_end_matches('s') {
                "year" => months += 12 * v,
                "month" | "mon" => months += v,
                "day" => days += v,
                unit => panic!("interval unit `{unit}` in `{s}`"),
            }
            i += 2;
        }
    }
    format!("{months}m{days}d{micros}us")
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
        lg.batch.iter().all(|r| !r.batch_refuses),
        "{}: split the batch-refused rows off first",
        st.engine
    );
    let (cols, t) = (columns(&lg.batch), &st.table);
    let n = lg.batch.iter().map(|r| r.sample.len()).max().unwrap() as i64;
    for i in 0..n as usize {
        (st.exec)(&format!(
            "INSERT INTO {t} (id, {}) VALUES ({}, {})",
            cols.join(", "),
            i + 1,
            values(&lg.batch, i).join(", ")
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
    let (cols, t) = (columns(&lg.batch), &st.table);
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
            set(&values(&lg.batch, i)),
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
            values(&lg.batch, i).join(", ")
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

/// The DuckDB state-DB path beside a `census_oracle()` rig's config.
fn state_db(rig: &Rig) -> String {
    format!(
        "{}/.rivet_state.db",
        rig.oracle_container_out().trim_end_matches("/out")
    )
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

/// Every stage of one engine, read by one DuckDB session (the source by its client when DuckDB cannot attach it), against the ledger and each other; returns every violation.
fn duckdb_ledger_verdict(lg: &Ledger, st: &Stand, n: i64) -> Vec<String> {
    let cols = columns(&lg.batch);
    let canons: Vec<Option<String>> = lg.batch.iter().map(|r| r.render.canon.clone()).collect();
    let (bdir, cdir) = (st.batch.out_dir(), st.cdc.out_dir());
    let (bc, cc) = (
        st.batch.oracle_container_out(),
        st.cdc.oracle_container_out(),
    );
    let (bs, cs, ss) = (schema(&bdir), schema(&cdir), schema(&cdir.join("snapshot")));
    let proj = projection(&lg.batch);
    let mut setup = format!(
        "INSTALL sqlite; LOAD sqlite; \
         ATTACH '{}' AS bst (TYPE sqlite, READ_ONLY); ATTACH '{}' AS cst (TYPE sqlite, READ_ONLY); \
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
             WHERE id IN (SELECT id FROM read_parquet('{cc}/*.parquet') WHERE __op IN ('insert', 'update'))",
        state_db(&st.batch),
        state_db(&st.cdc),
    );
    if let Source::Attach {
        engine,
        database,
        settings,
        pass,
        ..
    } = &st.source
    {
        let (attach, from) = engine.source_sql(database, &st.bare);
        let from = server_rendered(&lg.batch, &from, pass, &st.bare);
        setup = format!(
            "{} {settings} {attach} {setup} UNION ALL SELECT 'source', CAST(id AS VARCHAR), {proj} FROM {from}",
            engine.load_sql()
        );
    }
    if let Some(w) = &st.warehouse {
        setup = format!(
            "INSTALL httpfs; LOAD httpfs; {setup} UNION ALL SELECT 'warehouse', CAST(id AS VARCHAR), {proj} FROM {}",
            warehouse_relation(&lg.cdc, w)
        );
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
        // One query per state table: DuckDB 1.5.5 answers a UNION ALL of two sqlite
        // scans with the first scan twice (measured 2026-09-30).
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
    if let Source::Attach {
        engine,
        database,
        pass,
        ..
    } = &st.source
    {
        let (_, from) = engine.source_sql(database, &st.bare);
        queries.push(("source_types", format!("DESCRIBE SELECT * FROM {from}")));
        for (r, c) in lg.batch.iter().zip(&cols) {
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
                        pass.replace(
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
                .batch
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
            for (r, c) in lg.batch.iter().zip(&cols) {
                assert!(
                    r.render.source.is_none(),
                    "{}: {c} names a client-side source render, but DuckDB reads this source",
                    st.engine
                );
            }
        }
    }
    let (a_b, a_s) = (
        arrow_cells(&bdir, &cols, None),
        arrow_cells(&cdir, &cols, Some(&["update", "insert"])),
    );
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
    let mut bad = Vec::new();
    for (leg, want) in [
        ("source", &all),
        ("batch", &all),
        ("final", &all),
        ("stream", &captured),
        ("snapshot", &(1..=2 * n).collect::<Vec<i64>>()),
    ]
    .into_iter()
    .chain(st.warehouse.as_ref().map(|_| ("warehouse", &all)))
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
    for (k, (col, (b, c))) in cols.iter().zip(lg.batch.iter().zip(&lg.cdc)).enumerate() {
        let what = format!("{col} {}", b.native);
        let before = bad.len();
        let mut wbad = Vec::new();
        for (mode, want, sch) in [
            ("batch", &b.delivery, &bs),
            ("cdc stream", &c.delivery, &cs),
            ("cdc snapshot", &b.delivery, &ss),
        ] {
            let got = delivered(sch, col);
            if &got != want {
                bad.push(format!(
                    "{what}: {mode} delivers `{got}`, the ledger says `{want}`"
                ));
            }
        }
        let arrow_row = b.render.duck.as_deref() == Some(ARROW);
        for &id in &all {
            let src = if arrow_row {
                canon(&arrow_source[&id][0], &canons[k])
            } else {
                v("source", id, k)
            };
            let (batch, fin) = if arrow_row {
                (canon(&a_b[&id][k], &canons[k]), None)
            } else {
                (v("batch", id, k), Some(v("final", id, k)))
            };
            if batch != src {
                bad.push(format!("{what} id {id}: batch {batch:?}, source {src:?}"));
            }
            if let Some(fin) = fin.filter(|f| *f != src) {
                bad.push(format!(
                    "{what} id {id}: CDC final image {fin:?}, source {src:?}"
                ));
            }
            if st.warehouse.is_some() {
                let wh = if arrow_row {
                    canon(&arrow_warehouse[&id][0], &canons[k])
                } else {
                    v("warehouse", id, k)
                };
                if wh != src {
                    wbad.push(format!("{what} id {id}: ClickHouse {wh:?}, source {src:?}"));
                }
            }
        }
        if st.warehouse.is_some() {
            let got = ch_types.get(col.as_str()).cloned();
            if got != c.clickhouse {
                wbad.push(format!(
                    "{what}: ClickHouse holds `{got:?}`, the ledger says `{:?}`",
                    c.clickhouse
                ));
            }
        }
        for &id in &captured {
            if c.diverges.is_some() {
                continue;
            }
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
        for i in 1..=n {
            if !arrow_row && v("snapshot", i, k) != v("batch", n + i, k) {
                bad.push(format!(
                    "{what} sample {i}: snapshot {:?}, batch {:?}",
                    v("snapshot", i, k),
                    v("batch", n + i, k)
                ));
            }
        }
        if !arrow_row {
            let stat = |leg: &str| (counts[leg][1 + 2 * k], counts[leg][2 + 2 * k]);
            for (leg, of) in [
                ("batch", "source"),
                ("final", "source"),
                ("stream", "batch_stream"),
                ("warehouse", "source"),
            ] {
                if leg == "warehouse" && st.warehouse.is_none() {
                    continue;
                }
                if stat(leg) != stat(of) {
                    let to = if leg == "warehouse" {
                        &mut wbad
                    } else {
                        &mut bad
                    };
                    to.push(format!(
                        "{what}: {leg} has (non-null, distinct) {:?}, {of} {:?}",
                        stat(leg),
                        stat(of)
                    ));
                }
            }
        }
        match (&c.clickhouse_defect, wbad.is_empty()) {
            (Some(_), true) => bad.push(format!(
                "{what}: clickhouse_defect row now passes in ClickHouse — remove the marker"
            )),
            (Some(_), false) => {}
            (None, _) => bad.extend(wbad),
        }
        if b.known_defect.is_some() {
            if bad.len() == before {
                bad.push(format!(
                    "{what}: known_defect row now passes — remove the marker"
                ));
            } else {
                bad.truncate(before);
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
        (
            "stream export_metrics",
            got("cst_metrics", cname),
            parts["stream"],
        ),
        ("stream file_log", got("cst_files", cname), parts["stream"]),
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
            if !key.ends_with("/success") {
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
    rows: &[(Row, Row)],
    st: &Stand,
    n: usize,
    batch_of: &dyn Fn(&str) -> Rig,
) -> Vec<String> {
    let cdc_rows: Vec<Row> = rows.iter().map(|(_, c)| c.clone()).collect();
    let cols = columns(&cdc_rows);
    let cc = st.cdc.oracle_container_out();
    let cs = schema(&st.cdc.out_dir());
    let Source::Attach {
        engine,
        database,
        settings,
        pass,
        server_text,
    } = &st.source
    else {
        panic!(
            "{}: batch-refused rows need a DuckDB-attached source",
            st.engine
        )
    };
    assert!(
        !server_text.is_empty() && rows.iter().all(|(b, _)| b.known_defect.is_some()),
        "{}: batch-refused rows are known defects, graded against the server's own text",
        st.engine
    );
    let (attach, _) = engine.source_sql(database, &st.bare);
    let server_sql = format!(
        "SELECT id, {} FROM {}",
        cols.iter()
            .map(|c| format!("{} AS {c}", server_text.replace("{c}", c)))
            .collect::<Vec<_>>()
            .join(", "),
        st.bare
    );
    let from = pass.replace("{sql}", &server_sql.replace('\'', "''"));
    let proj = projection(&cdc_rows);
    let stats = cols
        .iter()
        .map(|c| format!("count({c}), count(DISTINCT {c})"))
        .collect::<Vec<_>>()
        .join(", ");
    let setup = format!(
        "{} {settings} {attach} CREATE TEMP VIEW legs AS \
           SELECT 'source' AS leg, CAST(id AS VARCHAR) AS id, {proj} FROM {from} \
           UNION ALL SELECT 'stream', CAST(id AS VARCHAR), {proj} FROM read_parquet('{cc}/*.parquet') \
             WHERE __op = 'insert'",
        engine.load_sql()
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
    let counts = cells(&out["counts"]);
    if counts[0][1..] != counts[1][1..] {
        bad.push(format!(
            "(COUNT(*), COUNT(col), COUNT(DISTINCT col)) per leg differ: {counts:?}"
        ));
    }
    for (k, (col, (b, c))) in cols.iter().zip(rows).enumerate() {
        let what = format!("{col} {}", b.native);
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
        if d != c.delivery && d != "server_text" {
            bad.push(format!(
                "{what}: cdc delivers `{d}`, neither the target `{}` nor today's server_text",
                c.delivery
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

/// The `columns:` overrides the rows declare, as one export line (none when no row has one).
fn overrides(rows: &[Row]) -> Option<String> {
    let o: Vec<String> = rows
        .iter()
        .enumerate()
        .filter_map(|(i, r)| r.over.as_ref().map(|o| format!("c{i}: {o}")))
        .collect();
    (!o.is_empty()).then(|| format!("columns: {{ {} }}", o.join(", ")))
}

/// Apply the rows' overrides, if any, to `rig`, and put its parts and state DB where DuckDB reads them.
fn graded(rig: Rig, rows: &[Row]) -> Rig {
    let rig = rig.census_oracle();
    match overrides(rows) {
        Some(line) => rig.export_line(&line),
        None => rig,
    }
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
        let wh = snapshot.then(|| warehouse(Rig::pg_cdc(&table, &wslot), rows, &table));
        let st = Stand {
            engine: "postgres",
            bare: table.clone(),
            table,
            exec: &exec,
            source: Source::Attach {
                engine: OracleEngine::PostgresCdc,
                database: "rivet",
                pass: "postgres_query('src', '{sql}')",
                settings: "SET pg_use_text_protocol = true;",
                server_text: "CASE WHEN {c} IS NOT NULL THEN format('%s', {c}) END",
            },
            settle: &|_| {},
            cdc: graded(cdc, rows),
            batch: graded(batch, rows),
            warehouse: wh,
        };
        (st, guards)
    };
    let (st, _g) = stand(&lg.batch, "ledger_pg", true);
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

    let refused_batch: Vec<Row> = refused.iter().map(|(b, _)| b.clone()).collect();
    let (st, _g2) = stand(&refused_batch, "ledger_pg_refused", false);
    st.cdc.run_ok();
    let n = seed_refused(&refused_batch, &st);
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
            columns_ddl(&lg.batch, "id BIGINT PRIMARY KEY")
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
            pass: "mysql_query('src', '{sql}')",
            settings: "SET mysql_tinyint1_as_boolean = false; SET mysql_session_time_zone = '+00:00';",
            server_text: "",
        },
        settle: &|_| {},
        cdc: graded(cdc, &lg.batch),
        batch: graded(batch, &lg.batch),
        warehouse: Some(warehouse(Rig::mysql_cdc(&table), &lg.batch, &table)),
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
        columns_ddl(&lg.batch, "id INT PRIMARY KEY")
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
            pass: "mssql_scan('src', '{sql}')",
            settings: "",
            server_text: "",
        },
        settle: &|rows| wait_for_capture(&ci, rows),
        cdc: graded(cdc, &lg.batch),
        batch: graded(batch, &lg.batch),
        warehouse: Some(warehouse(Rig::mssql_cdc(&table, &ci), &lg.batch, &table)),
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
        &columns_ddl(&lg.batch, "id NUMBER(10) PRIMARY KEY"),
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
        cdc: graded(cdc, &lg.batch),
        batch: graded(batch, &lg.batch),
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
