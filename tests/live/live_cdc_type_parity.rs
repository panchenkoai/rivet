//! The batch-equals-CDC contract, generated from `docs/type-capability-matrix.yaml`
//! (ADR-0038 CP9, CP12). Per engine, one table holds a column for every ledger row,
//! seeded with the row's samples; CDC runs `initial: snapshot`, then an UPDATE rewrites
//! every column and a second run captures it, and a batch run reads the final state.
//! Each column must arrive as the ledger declares in each mode, the two modes must hold
//! the same values (read by DuckDB), and both must equal the source's own rendering.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::common::*;

#[derive(Clone, Debug, Default)]
struct Render {
    source: Option<String>,
    parquet: Option<String>,
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
                    parquet: s(&r["render"]["parquet"]),
                    canon: s(&r["render"]["canon"]),
                },
                diverges: s(&r["diverges"]),
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
        cdc.len() == batch.len(),
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

/// `delivery` of a batch row whose type the batch run refuses by column.
const REFUSED: &str = "refused";

/// The ledger without its batch-refused rows, and those rows as (batch, cdc) twins.
fn split_refused(lg: Ledger) -> (Ledger, Vec<(Row, Row)>) {
    let (mut batch, mut cdc, mut refused) = (Vec::new(), Vec::new(), Vec::new());
    for (b, c) in lg.batch.into_iter().zip(lg.cdc) {
        if b.delivery == REFUSED {
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

/// What one engine needs from its test: SQL against the source and the two rigs.
struct Stand<'a> {
    engine: &'static str,
    table: String,
    exec: &'a dyn Fn(&str),
    text_rows: &'a dyn Fn(&str) -> Vec<Vec<Option<String>>>,
    /// The source's own rendering of `{c}` as text, when a row names none.
    source_text: &'static str,
    /// Called with the number of change rows the DML produced (SQL Server waits on its capture job).
    settle: &'a dyn Fn(i64),
    cdc: Rig,
    batch: Rig,
    host: PathBuf,
    container: String,
}

type Cells = BTreeMap<i64, Vec<Option<String>>>;

/// `render.parquet` value for a type DuckDB cannot read exactly (Decimal256 reads as DOUBLE, ADR-0038 CP11): Arrow's own display renders it.
const ARROW: &str = "arrow";

/// Rows of `[id, cells...]` keyed by id.
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

/// DuckDB's rows of `select` over the parquet parts under the container dir `dir`.
fn duck(select: &str, dir: &str, filter: &str) -> Cells {
    let v = duckdb_run_sql_json(&format!(
        "SELECT CAST(id AS VARCHAR), {select} FROM read_parquet('{dir}/*.parquet') {filter}"
    ));
    keyed(
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
            .collect(),
    )
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
        Some("timestamp") => canon_ts(s.trim_start_matches('+').trim()),
        Some("float32") => s
            .parse::<f32>()
            .map_or_else(|e| panic!("{s}: {e}"), |f| f.to_string()),
        Some("float64") => s
            .parse::<f64>()
            .map_or_else(|e| panic!("{s}: {e}"), |f| f.to_string()),
        Some(other) => panic!("unknown canon `{other}`"),
        None => s.clone(),
    })
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
        lg.batch.iter().all(|r| r.delivery != REFUSED),
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

/// Every ledger row, read back by DuckDB (and Arrow) from both modes, against the ledger and the source; returns every violation.
fn duckdb_ledger_verdict(lg: &Ledger, st: &Stand, n: i64) -> Vec<String> {
    let (cols, t) = (columns(&lg.batch), &st.table);
    let (bdir, cdir) = (st.host.join("batch"), st.host.join("cdc"));
    let (bs, cs, ss) = (schema(&bdir), schema(&cdir), schema(&cdir.join("snapshot")));
    let raw = cols.join(", ");
    let rendered = |rows: &[Row]| -> String {
        rows.iter()
            .zip(&cols)
            .map(|(r, c)| match r.render.parquet.as_deref() {
                Some(ARROW) => "NULL".to_string(),
                p => p.unwrap_or("CAST({c} AS VARCHAR)").replace("{c}", c),
            })
            .collect::<Vec<_>>()
            .join(", ")
    };
    let cb = format!("{}/batch", st.container);
    let cc = format!("{}/cdc", st.container);
    let b_raw = duck(&raw, &cb, "");
    let b_txt = duck(&rendered(&lg.batch), &cb, "");
    let upd = "WHERE __op IN ('update', 'insert')";
    let c_raw = duck(&raw, &cc, upd);
    let c_txt = duck(&rendered(&lg.cdc), &cc, upd);
    let s_raw = duck(&raw, &format!("{cc}/snapshot"), "");
    let source = |rows: &[Row]| -> Cells {
        let exprs = rows
            .iter()
            .zip(&cols)
            .map(|(r, c)| {
                r.render
                    .source
                    .as_deref()
                    .unwrap_or(st.source_text)
                    .replace("{c}", c)
            })
            .collect::<Vec<_>>()
            .join(", ");
        let id = st.source_text.replace("{c}", "id");
        keyed((st.text_rows)(&format!("SELECT {id}, {exprs} FROM {t}")))
    };
    let (src_b, src_c) = (source(&lg.batch), source(&lg.cdc));
    let (a_b, a_c) = (
        arrow_cells(&bdir, &cols, None),
        arrow_cells(&cdir, &cols, Some(&["update", "insert"])),
    );
    let text = |r: &Row, duck: &Cells, arrow: &Cells, id: i64, k: usize| {
        let cells = if r.render.parquet.as_deref() == Some(ARROW) {
            arrow
        } else {
            duck
        };
        canon(&cells[&id][k], &r.render.canon)
    };

    let captured: Vec<i64> = std::iter::once(1).chain(n + 1..=3 * n).collect();
    assert_eq!(
        c_raw.keys().copied().collect::<Vec<_>>(),
        captured,
        "{}: one captured change per rewritten or inserted row",
        st.engine
    );
    assert_eq!(
        b_raw.len() as i64,
        3 * n,
        "{}: the batch export holds every row",
        st.engine
    );
    let mut bad = Vec::new();
    for (k, (col, (b, c))) in cols.iter().zip(lg.batch.iter().zip(&lg.cdc)).enumerate() {
        let what = format!("{col} {}", b.native);
        for (mode, want, sch) in [("batch", &b.delivery, &bs), ("cdc", &c.delivery, &cs)] {
            let got = delivered(sch, col);
            if &got != want {
                bad.push(format!(
                    "{what}: {mode} delivers `{got}`, the ledger says `{want}`"
                ));
            }
        }
        if delivered(&ss, col) != delivered(&bs, col) {
            bad.push(format!(
                "{what}: the snapshot leg delivers `{}`, batch `{}`",
                delivered(&ss, col),
                delivered(&bs, col)
            ));
        }
        for i in 1..=n {
            if s_raw[&i][k] != b_raw[&(n + i)][k] {
                bad.push(format!(
                    "{what} sample {i}: snapshot {:?}, batch {:?}",
                    s_raw[&i][k],
                    b_raw[&(n + i)][k]
                ));
            }
        }
        for id in &captured {
            if c.diverges.is_none() {
                for (via, cv, bv) in [("duckdb", &c_raw, &b_raw), ("arrow", &a_c, &a_b)] {
                    if cv[id][k] != bv[id][k] {
                        bad.push(format!(
                            "{what} id {id} ({via}): cdc {:?}, batch {:?}",
                            cv[id][k], bv[id][k]
                        ));
                    }
                }
            }
            let (s, got) = (
                canon(&src_c[id][k], &c.render.canon),
                text(c, &c_txt, &a_c, *id, k),
            );
            if s != got {
                bad.push(format!(
                    "{what} id {id}: cdc renders {got:?}, the source {s:?}"
                ));
            }
        }
        for id in b_txt.keys() {
            let (s, got) = (
                canon(&src_b[id][k], &b.render.canon),
                text(b, &b_txt, &a_b, *id, k),
            );
            if s != got {
                bad.push(format!(
                    "{what} id {id}: batch renders {got:?}, the source {s:?}"
                ));
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

/// Batch refuses every row by column (`batch_of` exports one column); CDC delivers each as its cdc twin declares, equal to the source; returns every violation.
fn duckdb_refused_verdict(
    rows: &[(Row, Row)],
    st: &Stand,
    n: usize,
    batch_of: &dyn Fn(&str) -> Rig,
) -> Vec<String> {
    let cols = columns(&rows.iter().map(|(b, _)| b.clone()).collect::<Vec<_>>());
    let cdir = st.host.join("cdc");
    let cs = schema(&cdir);
    let txt = duck(
        &rows
            .iter()
            .zip(&cols)
            .map(|((_, c), col)| {
                c.render
                    .parquet
                    .as_deref()
                    .unwrap_or("CAST({c} AS VARCHAR)")
                    .replace("{c}", col)
            })
            .collect::<Vec<_>>()
            .join(", "),
        &format!("{}/cdc", st.container),
        "WHERE __op = 'insert'",
    );
    let src = keyed((st.text_rows)(&format!(
        "SELECT {}, {} FROM {}",
        st.source_text.replace("{c}", "id"),
        rows.iter()
            .zip(&cols)
            .map(|((_, c), col)| c
                .render
                .source
                .as_deref()
                .unwrap_or(st.source_text)
                .replace("{c}", col))
            .collect::<Vec<_>>()
            .join(", "),
        st.table
    )));
    assert_eq!(
        txt.len(),
        n,
        "{}: one captured insert per sample row",
        st.engine
    );
    let mut bad = Vec::new();
    for (k, (col, (b, c))) in cols.iter().zip(rows).enumerate() {
        let what = format!("{col} {}", b.native);
        let said = batch_of(col).run_expect_fail();
        if !said.contains(&format!("'{col}'")) && !said.contains(&format!("• {col} (")) {
            bad.push(format!(
                "{what}: the batch run did not refuse it by name:\n{said}"
            ));
        }
        let got = delivered(&cs, col);
        if got != c.delivery {
            bad.push(format!(
                "{what}: cdc delivers `{got}`, the ledger says `{}`",
                c.delivery
            ));
        }
        for (id, row) in &txt {
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

/// The `columns:` overrides the ledger declares, as one export line (empty when none).
fn overrides(rows: &[Row]) -> Option<String> {
    let o: Vec<String> = rows
        .iter()
        .enumerate()
        .filter_map(|(i, r)| r.over.as_ref().map(|o| format!("c{i}: {o}")))
        .collect();
    (!o.is_empty()).then(|| format!("columns: {{ {} }}", o.join(", ")))
}

/// Apply the ledger's overrides, if any, to `rig`.
fn with_overrides(rig: Rig, rows: &[Row]) -> Rig {
    match overrides(rows) {
        Some(line) => rig.export_line(&line),
        None => rig,
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
    let text_rows = |q: &str| -> Vec<Vec<Option<String>>> {
        let mut c = connect();
        c.batch_execute("SET TimeZone = 'UTC'; SET intervalstyle = 'iso_8601'")
            .unwrap();
        c.query(q, &[])
            .unwrap_or_else(|e| panic!("{q}: {e}"))
            .iter()
            .map(|r| {
                (0..r.len())
                    .map(|i| r.get::<_, Option<String>>(i))
                    .collect()
            })
            .collect()
    };
    let stand = |rows: &[Row], label: &str, snapshot: bool| {
        let table = unique_name(label);
        let slot = unique_name(&format!("{label}_slot"));
        exec(&format!(
            "CREATE TABLE {table} ({})",
            columns_ddl(rows, "id BIGINT PRIMARY KEY")
        ));
        let guards = (
            PgTable::adopt_on(POSTGRES_CDC_URL, table.clone()),
            Slot(slot.clone()),
        );
        let (host, container) = duckdb_shared_workdir(&table);
        let mut cdc = with_overrides(Rig::pg_cdc(&table, &slot), rows).dest_path(host.join("cdc"));
        if snapshot {
            cdc = cdc.cdc_line("initial: snapshot");
        }
        let batch = with_overrides(Rig::pg_batch(&table), rows)
            .export_named(&format!("{table}_batch"))
            .source_url(POSTGRES_CDC_URL)
            .dest_path(host.join("batch"));
        let st = Stand {
            engine: "postgres",
            table,
            exec: &exec,
            text_rows: &text_rows,
            source_text: "{c}::text",
            settle: &|_| {},
            cdc,
            batch,
            host,
            container,
        };
        (st, guards)
    };
    let (st, _g) = stand(&lg.batch, "ledger_pg", true);
    let n = seed(&lg, &st);
    st.cdc.run_ok();
    rewrite(&lg, &st, n);
    st.cdc.run_ok();
    st.batch.run_ok();
    let bad = duckdb_ledger_verdict(&lg, &st, n);
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
    let (host, container) = duckdb_shared_workdir(&table);
    let exec = |q: &str| {
        cdc_conn()
            .query_drop(q)
            .unwrap_or_else(|e| panic!("{q}: {e}"))
    };
    let text_rows = |q: &str| -> Vec<Vec<Option<String>>> {
        let mut c = cdc_conn();
        c.query_drop("SET time_zone = '+00:00'").unwrap();
        c.query::<mysql::Row, _>(q)
            .unwrap_or_else(|e| panic!("{q}: {e}"))
            .into_iter()
            .map(|r| {
                (0..r.len())
                    .map(|i| match r.get::<mysql::Value, _>(i).unwrap() {
                        mysql::Value::NULL => None,
                        mysql::Value::Bytes(b) => Some(String::from_utf8(b).expect("utf-8 text")),
                        other => panic!("{q}: column {i} is not text: {other:?}"),
                    })
                    .collect()
            })
            .collect()
    };
    let cdc = with_overrides(Rig::mysql_cdc(&table), &lg.batch)
        .cdc_line("initial: snapshot")
        .dest_path(host.join("cdc"));
    let batch = with_overrides(Rig::mysql_batch(&table), &lg.batch)
        .export_named(&format!("{table}_batch"))
        .source_url(MYSQL_CDC_URL)
        .dest_path(host.join("batch"));
    let st = Stand {
        engine: "mysql",
        table: table.clone(),
        exec: &exec,
        text_rows: &text_rows,
        source_text: "CAST({c} AS CHAR)",
        settle: &|_| {},
        cdc,
        batch,
        host,
        container,
    };
    let n = seed(&lg, &st);
    st.cdc.run_ok();
    rewrite(&lg, &st, n);
    st.cdc.run_ok();
    st.batch.run_ok();
    let bad = duckdb_ledger_verdict(&lg, &st, n);
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
    let (host, container) = duckdb_shared_workdir(&table);
    let cdc = with_overrides(Rig::mssql_cdc(&table, &ci), &lg.batch)
        .cdc_line("initial: snapshot")
        .dest_path(host.join("cdc"));
    let batch = with_overrides(Rig::mssql_batch(&format!("{table}_batch")), &lg.batch)
        .source_url(MSSQL_CDC_URL)
        .query(&format!("SELECT * FROM dbo.{table}"))
        .dest_path(host.join("batch"));
    let st = Stand {
        engine: "mssql",
        table: format!("dbo.{table}"),
        exec: &mssql_cdc_exec,
        text_rows: &mssql_cdc_text_rows,
        source_text: "CONVERT(nvarchar(max), {c})",
        settle: &|rows| wait_for_capture(&ci, rows),
        cdc,
        batch,
        host,
        container,
    };
    let n = seed(&lg, &st);
    st.cdc.run_ok();
    rewrite(&lg, &st, n);
    st.cdc.run_ok();
    st.batch.run_ok();
    let bad = duckdb_ledger_verdict(&lg, &st, n);
    assert!(
        bad.is_empty(),
        "{} ledger violations:\n{}",
        bad.len(),
        bad.join("\n")
    );
}

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
    let (host, container) = duckdb_shared_workdir(&t.name().to_lowercase());
    let cdc = with_overrides(Rig::oracle_cdc(t.name()), &lg.batch)
        .cdc_line("initial: snapshot")
        .dest_path(host.join("cdc"));
    let batch =
        with_overrides(Rig::oracle_batch(t.name()), &lg.batch).dest_path(host.join("batch"));
    let st = Stand {
        engine: "oracle",
        table: t.name().to_string(),
        exec: &ora_exec,
        text_rows: &ora_text_rows,
        source_text: "TO_CHAR({c})",
        settle: &|_| {},
        cdc,
        batch,
        host,
        container,
    };
    let n = seed(&lg, &st);
    st.cdc.run_ok();
    rewrite(&lg, &st, n);
    st.cdc.run_ok();
    st.batch.run_ok();
    let bad = duckdb_ledger_verdict(&lg, &st, n);
    assert!(
        bad.is_empty(),
        "{} ledger violations:\n{}",
        bad.len(),
        bad.join("\n")
    );
}
