//! Oracle batch source, end to end through the Rig. The independent oracle is the
//! database's own rendering of each value (`TO_CHAR`, `SYS_EXTRACT_UTC`,
//! `RAWTOHEX`, `DBMS_LOB.GETLENGTH`) read as text, compared cell by cell with the
//! Parquet rivet wrote — never rivet's own summary.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, TimeUnit};

use crate::common::*;

/// One Parquet column, every file in `dir`, keyed by the `ID` column, rendered canonically.
fn parquet_cells(dir: &Path, col: &str) -> BTreeMap<i64, Option<String>> {
    let mut out = BTreeMap::new();
    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        let p = entry.path();
        if p.extension().is_none_or(|e| e != "parquet") {
            continue;
        }
        let bytes = bytes::Bytes::from(std::fs::read(&p).unwrap());
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            let ids = batch.column_by_name("ID").expect("ID column");
            let vals = batch
                .column_by_name(col)
                .unwrap_or_else(|| panic!("{col} missing from {}", p.display()));
            for i in 0..batch.num_rows() {
                let id = canon_num(&arrow::util::display::array_value_to_string(ids, i).unwrap())
                    .parse::<i64>()
                    .unwrap();
                out.insert(id, render(vals.as_ref(), i));
            }
        }
    }
    out
}

/// The canonical text of one Arrow cell: decimals without trailing zeros,
/// timestamps as UTC `YYYY-MM-DDTHH:MM:SS[.frac]`, binary as upper hex.
fn render(a: &dyn Array, i: usize) -> Option<String> {
    if a.is_null(i) {
        return None;
    }
    Some(match a.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let us = a
                .as_primitive::<arrow::datatypes::TimestampMicrosecondType>()
                .value(i);
            let dt = chrono::DateTime::from_timestamp_micros(us).unwrap();
            // Oracle's SYYYY has no year 0 (-1 is 1 BC); chrono's year 0 is 1 BC.
            let year = match chrono::Datelike::year(&dt) {
                y if y <= 0 => y - 1,
                y => y,
            };
            canon_ts(&format!("{year:04}{}", dt.format("-%m-%dT%H:%M:%S%.6f")))
        }
        DataType::Binary => a
            .as_binary::<i32>()
            .value(i)
            .iter()
            .map(|b| format!("{b:02X}"))
            .collect(),
        DataType::Decimal128(..) | DataType::Int32 | DataType::Int64 => {
            canon_num(&arrow::util::display::array_value_to_string(a, i).unwrap())
        }
        _ => arrow::util::display::array_value_to_string(a, i).unwrap(),
    })
}

/// An exact decimal string in canonical form: no exponent, no trailing fraction zeros.
fn canon_num(s: &str) -> String {
    let s = s.trim();
    let (neg, s) = s.strip_prefix('-').map_or((false, s), |r| (true, r));
    let (mant, exp) = match s.split_once(['E', 'e']) {
        Some((m, e)) => (m, e.parse::<i64>().unwrap()),
        None => (s, 0),
    };
    let (int, frac) = mant.split_once('.').unwrap_or((mant, ""));
    let all = format!("{int}{frac}");
    let zeros = all.len() - all.trim_start_matches('0').len();
    let digits = &all[zeros..];
    // Position of the decimal point within `digits`.
    let point = int.len() as i64 + exp - zeros as i64;
    let (i, f) = if digits.is_empty() {
        ("0".to_string(), String::new())
    } else if point <= 0 {
        (
            "0".to_string(),
            format!("{}{digits}", "0".repeat((-point) as usize)),
        )
    } else if point as usize >= digits.len() {
        (
            format!("{digits}{}", "0".repeat(point as usize - digits.len())),
            String::new(),
        )
    } else {
        (
            digits[..point as usize].to_string(),
            digits[point as usize..].to_string(),
        )
    };
    let f = f.trim_end_matches('0');
    let body = if f.is_empty() { i } else { format!("{i}.{f}") };
    if neg && body != "0" {
        format!("-{body}")
    } else {
        body
    }
}

/// A timestamp's text with trailing fractional zeros (and a bare `.`) dropped.
fn canon_ts(s: &str) -> String {
    match s.split_once('.') {
        Some((a, f)) if f.trim_end_matches('0').is_empty() => a.to_string(),
        Some((a, f)) => format!("{a}.{}", f.trim_end_matches('0')),
        None => s.to_string(),
    }
}

/// The type-matrix table: every type the batch source maps, with edge values.
fn type_matrix_table() -> OracleTable {
    let t = OracleTable::create(
        "ora_types",
        "id NUMBER(10) PRIMARY KEY, n_bare NUMBER, n_dec NUMBER(38,10), n_int NUMBER(9), \
         n_big NUMBER(18), n_small NUMBER(5,2), bf BINARY_FLOAT, bd BINARY_DOUBLE, b BOOLEAN, \
         d DATE, ts TIMESTAMP(6), ts9 TIMESTAMP(9), tstz TIMESTAMP(6) WITH TIME ZONE, \
         tsltz TIMESTAMP(6) WITH LOCAL TIME ZONE, vc VARCHAR2(4000 CHAR), nvc NVARCHAR2(100), \
         ch CHAR(5 CHAR), cl CLOB, rw RAW(16), bl BLOB, js JSON",
    );
    let n = t.name();
    for row in [
        "1, 123.45, 1.5, 42, 9007199254740993, 1.25, 1.5, 2.25, TRUE, \
         TO_DATE('2024-02-29 13:14:15','YYYY-MM-DD HH24:MI:SS'), \
         TO_TIMESTAMP('2024-02-29 13:14:15.123456','YYYY-MM-DD HH24:MI:SS.FF'), \
         TO_TIMESTAMP('2024-02-29 13:14:15.123456789','YYYY-MM-DD HH24:MI:SS.FF'), \
         TO_TIMESTAMP_TZ('2024-02-29 10:00:00.5 +02:00','YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'), \
         TO_TIMESTAMP_TZ('2024-02-29 10:00:00 -03:00','YYYY-MM-DD HH24:MI:SS TZH:TZM'), \
         'plain', N'unicode ✓ 🦀', 'ab', 'short clob', HEXTORAW('00FF10'), HEXTORAW('DEADBEEF'), \
         JSON('{\"a\":1,\"b\":[true,null]}')",
        "2, 12345678901234567890.123456789, 1234567890123456789012345678.0123456789, \
         -999999999, -999999999999999999, -999.99, BINARY_FLOAT_NAN, BINARY_DOUBLE_INFINITY, \
         FALSE, TO_DATE('0001-01-01','YYYY-MM-DD'), \
         TO_TIMESTAMP('9999-12-31 23:59:59.999999','YYYY-MM-DD HH24:MI:SS.FF'), NULL, \
         TO_TIMESTAMP_TZ('2024-07-01 10:00:00 Europe/Berlin','YYYY-MM-DD HH24:MI:SS TZR'), NULL, \
         '   ', NULL, 'x', TO_CLOB(RPAD('a',4000,'a')) || TO_CLOB(RPAD('b',4000,'b')), NULL, \
         HEXTORAW('01'), JSON('\"str\"')",
        "3, 1E125, NULL, NULL, NULL, NULL, NULL, NULL, NULL, \
         TO_DATE('-4712-01-01','SYYYY-MM-DD'), NULL, NULL, NULL, NULL, '', N'日本語', NULL, \
         NULL, NULL, NULL, NULL",
        "4, 1E-130, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, \
         NULL, NULL, NULL, EMPTY_CLOB(), NULL, EMPTY_BLOB(), NULL",
        "5, -0.000000000000000000000000000000000000001, NULL, NULL, NULL, NULL, NULL, NULL, \
         NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL",
    ] {
        ora_exec(&format!("INSERT INTO {n} VALUES ({row})"));
    }
    t
}

/// The database's own rendering of column `expr` per id.
fn oracle_cells(table: &str, expr: &str) -> BTreeMap<i64, Option<String>> {
    ora_text_rows(&format!(
        "SELECT TO_CHAR(id), {expr} FROM {table} ORDER BY id"
    ))
    .into_iter()
    .map(|r| (r[0].as_deref().unwrap().parse().unwrap(), r[1].clone()))
    .collect()
}

fn ts_fmt(col: &str) -> String {
    format!("TO_CHAR({col}, 'SYYYY-MM-DD\"T\"HH24:MI:SS.FF6')")
}

#[test]
#[ignore = "live: requires docker compose oracle"]
fn oracle_full_export_matches_the_databases_own_rendering_for_every_type() {
    require_alive(LiveService::Oracle);
    let t = type_matrix_table();
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::oracle_batch(t.name()).dest_path(out.path().to_path_buf());
    let run = rig.run_args(&[]);
    assert!(
        run.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );

    let canon = |m: BTreeMap<i64, Option<String>>, f: &dyn Fn(&str) -> String| {
        m.into_iter()
            .map(|(k, v)| (k, v.map(|s| f(s.trim()))))
            .collect::<BTreeMap<_, _>>()
    };
    let ident = |s: &str| s.to_string();
    let num = |s: &str| canon_num(s);
    let ts = |s: &str| canon_ts(s.trim_start_matches('+').trim_start_matches(' '));
    // (column, the database's own expression, canonicaliser for that expression)
    type Canon<'a> = &'a dyn Fn(&str) -> String;
    let checks: Vec<(&str, String, Canon)> = vec![
        ("N_BARE", "TO_CHAR(n_bare, 'TM9')".into(), &num),
        ("N_DEC", "TO_CHAR(n_dec, 'TM9')".into(), &num),
        ("N_INT", "TO_CHAR(n_int)".into(), &num),
        ("N_BIG", "TO_CHAR(n_big)".into(), &num),
        ("N_SMALL", "TO_CHAR(n_small, 'TM9')".into(), &num),
        (
            "B",
            "CASE WHEN b IS NULL THEN NULL WHEN b THEN 'true' ELSE 'false' END".into(),
            &ident,
        ),
        ("D", ts_fmt("CAST(d AS TIMESTAMP)"), &ts),
        ("TS", ts_fmt("ts"), &ts),
        ("TSTZ", ts_fmt("SYS_EXTRACT_UTC(tstz)"), &ts),
        ("TSLTZ", ts_fmt("SYS_EXTRACT_UTC(tsltz)"), &ts),
        ("VC", "vc".into(), &ident),
        ("NVC", "TO_CHAR(nvc)".into(), &ident),
        ("CH", "ch".into(), &ident),
        ("RW", "RAWTOHEX(rw)".into(), &ident),
    ];
    for (col, expr, f) in checks {
        let want = canon(oracle_cells(t.name(), &expr), f);
        let got = canon(parquet_cells(out.path(), col), f);
        assert_eq!(got, want, "{col}: parquet vs the database's own {expr}");
    }

    // TIMESTAMP(9): microseconds, the sub-µs digits truncated (the documented warning).
    let ts9 = parquet_cells(out.path(), "TS9");
    assert_eq!(
        ts9[&1].as_deref(),
        Some("2024-02-29T13:14:15.123456"),
        "TIMESTAMP(9) keeps microseconds"
    );
    // BINARY_FLOAT/DOUBLE: NaN / Inf survive as IEEE values.
    let bf = parquet_cells(out.path(), "BF");
    assert_eq!(bf[&1].as_deref(), Some("1.5"));
    assert_eq!(bf[&2].as_deref(), Some("NaN"));
    let bd = parquet_cells(out.path(), "BD");
    assert_eq!(bd[&2].as_deref(), Some("inf"));
    // LOBs: length and content, measured by the database.
    let cl = parquet_cells(out.path(), "CL");
    let cl_len = oracle_cells(t.name(), "TO_CHAR(DBMS_LOB.GETLENGTH(cl))");
    for (id, v) in &cl {
        assert_eq!(
            v.as_ref().map(|s| s.chars().count().to_string()),
            cl_len[id],
            "CLOB length, id {id}"
        );
    }
    assert!(cl[&2].as_deref().unwrap().ends_with(&"b".repeat(100)));
    // A zero-length LOB is not NULL (the driver says NULL; rivet asks the server).
    assert_eq!(
        cl[&4].as_deref(),
        Some(""),
        "EMPTY_CLOB() is empty, not NULL"
    );
    assert_eq!(cl[&5], None, "a NULL CLOB stays NULL");
    let bl = parquet_cells(out.path(), "BL");
    // Oracle cannot render an empty RAW (it is NULL), so the server marks it explicitly.
    let want_bl: BTreeMap<i64, Option<String>> = oracle_cells(
        t.name(),
        "CASE WHEN DBMS_LOB.GETLENGTH(bl) = 0 THEN '<empty>' \
         ELSE RAWTOHEX(DBMS_LOB.SUBSTR(bl, 2000, 1)) END",
    )
    .into_iter()
    .map(|(k, v)| (k, v.map(|s| if s == "<empty>" { String::new() } else { s })))
    .collect();
    assert_eq!(bl, want_bl, "BLOB bytes; EMPTY_BLOB() is empty, not NULL");
    // JSON: content, re-parsed.
    let js = parquet_cells(out.path(), "JS");
    let want = oracle_cells(t.name(), "JSON_SERIALIZE(js)");
    for (id, v) in js {
        let parse = |s: &Option<String>| {
            s.as_deref()
                .map(|s| serde_json::from_str::<serde_json::Value>(s).unwrap())
        };
        assert_eq!(parse(&v), parse(&want[&id]), "JSON id {id}");
    }
}

/// The driver cannot decode a `TIMESTAMP WITH TIME ZONE` stored with a REGION
/// name (it panics); rivet fetches such columns through `SYS_EXTRACT_UTC`. RED
/// against removing that re-projection: the run aborts on a driver panic.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_region_named_time_zone_exports_as_its_utc_instant() {
    require_alive(LiveService::Oracle);
    let t = OracleTable::create(
        "ora_tzr",
        "id NUMBER(10) PRIMARY KEY, tstz TIMESTAMP WITH TIME ZONE",
    );
    ora_exec(&format!(
        "INSERT INTO {} VALUES (1, TO_TIMESTAMP_TZ('2024-07-01 10:00:00 Europe/Berlin', \
         'YYYY-MM-DD HH24:MI:SS TZR'))",
        t.name()
    ));
    let out = tempfile::tempdir().unwrap();
    let run = Rig::oracle_batch(t.name())
        .dest_path(out.path().to_path_buf())
        .run_args(&[]);
    assert!(
        run.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
    assert_eq!(
        parquet_cells(out.path(), "TSTZ")[&1].as_deref(),
        Some("2024-07-01T08:00:00"),
        "Berlin summer time is UTC+2"
    );
    assert!(matches!(
        parquet_column_type(out.path(), "TSTZ"),
        DataType::Timestamp(TimeUnit::Microsecond, Some(tz)) if tz.as_ref() == "UTC"
    ));
}

/// Keyset paging over a bare `NUMBER` primary key (the common `id NUMBER`):
/// every row once, across page boundaries.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn keyset_over_a_bare_number_pk_reads_every_row_once() {
    require_alive(LiveService::Oracle);
    let t = seed_oracle_numeric_table(2_500);
    let out = tempfile::tempdir().unwrap();
    let run = Rig::oracle_batch(t.name())
        .mode("chunked")
        .export_line("chunk_by_key: ID")
        .export_line("chunk_size: 400")
        .dest_path(out.path().to_path_buf())
        .run_args(&[]);
    assert!(
        run.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
    // DuckDB reads the parts; the source's own COUNT is the expected value.
    let source: i64 = ora_text_rows(&format!("SELECT TO_CHAR(COUNT(*)) FROM {}", t.name()))[0][0]
        .as_deref()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        duckdb_total_parquet_rows(out.path()) as i64,
        source,
        "every row"
    );
    assert_eq!(
        duckdb_dir_scalar(out.path(), "count(DISTINCT \"ID\")", None),
        source,
        "no row read twice"
    );
}

/// An incremental TIMESTAMP cursor: run 2 reads only what run 1 had not.
/// The cursor is re-injected as text and converted by the pinned NLS mask.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn an_incremental_timestamp_cursor_resumes_past_the_last_row() {
    require_alive(LiveService::Oracle);
    let t = OracleTable::create(
        "ora_inc",
        "id NUMBER(10) PRIMARY KEY, updated_at TIMESTAMP(6) NOT NULL",
    );
    ora_exec(&format!(
        "INSERT INTO {} SELECT LEVEL, TIMESTAMP '2024-01-01 00:00:00' + NUMTODSINTERVAL(LEVEL, 'SECOND') \
         + NUMTODSINTERVAL(LEVEL * 7, 'MINUTE') / 1000000 FROM dual CONNECT BY LEVEL <= 50",
        t.name()
    ));
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::oracle_batch(t.name())
        .mode("incremental")
        .export_line("cursor_column: UPDATED_AT")
        .dest_path(out.path().to_path_buf());
    let r1 = rig.run_args(&[]);
    assert!(
        r1.status.success(),
        "{}",
        String::from_utf8_lossy(&r1.stderr)
    );
    ora_exec(&format!(
        "INSERT INTO {} SELECT 50 + LEVEL, TIMESTAMP '2025-01-01 00:00:00' + NUMTODSINTERVAL(LEVEL, 'SECOND') \
         FROM dual CONNECT BY LEVEL <= 7",
        t.name()
    ));
    let r2 = rig.run_args(&[]);
    assert!(
        r2.status.success(),
        "{}",
        String::from_utf8_lossy(&r2.stderr)
    );
    let mut all: Vec<i64> = Vec::new();
    for e in std::fs::read_dir(out.path()).unwrap().flatten() {
        if e.path().extension().is_some_and(|x| x == "parquet") {
            all.extend(parquet_cells_file(&e.path()));
        }
    }
    all.sort_unstable();
    assert_eq!(
        all,
        (1..=57).collect::<Vec<_>>(),
        "each row exactly once across two runs"
    );
}

/// The ids in one parquet file, with multiplicity.
fn parquet_cells_file(p: &Path) -> Vec<i64> {
    let bytes = bytes::Bytes::from(std::fs::read(p).unwrap());
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
        .unwrap()
        .build()
        .unwrap();
    let mut out = Vec::new();
    for b in reader {
        let b = b.unwrap();
        let ids = b.column_by_name("ID").unwrap();
        for i in 0..b.num_rows() {
            out.push(
                canon_num(&arrow::util::display::array_value_to_string(ids, i).unwrap())
                    .parse()
                    .unwrap(),
            );
        }
    }
    out
}

/// A run records the engine's full source-harm counter set, deltas floored at 0.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_run_records_the_oracle_source_harm_counters() {
    require_alive(LiveService::Oracle);
    let t = seed_oracle_numeric_table(300);
    let export = unique_name("ora_harm");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::oracle_batch(t.name())
        .export_named(&export)
        .dest_path(out.path().to_path_buf());
    let run = rig.run_args(&["--export", &export]);
    assert!(
        run.status.success(),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );
    let db = StateDb::next_to_config(&rig.config_path());
    let run_id = db.latest_run_id(&export);
    let mut got: Vec<String> = db
        .harm_rows(&run_id)
        .into_iter()
        .map(|(m, d)| {
            assert!(d >= 0, "{m} floored at 0");
            m
        })
        .collect();
    got.sort();
    assert_eq!(
        got,
        [
            "oracle_consistent_gets",
            "oracle_lock_wait_ms",
            "oracle_lock_waits",
            "oracle_physical_reads",
            "oracle_temp_files"
        ]
    );
    assert_eq!(
        db.metrics_row(&run_id).source_type.as_deref(),
        Some("oracle")
    );
}

#[test]
fn canon_num_normalises_every_rendering_to_one_form() {
    assert_eq!(canon_num("1E+125"), format!("1{}", "0".repeat(125)));
    assert_eq!(canon_num("1E-3"), "0.001");
    assert_eq!(canon_num(".5"), "0.5");
    assert_eq!(canon_num("-.000100"), "-0.0001");
    assert_eq!(canon_num("1.2500"), "1.25");
    assert_eq!(canon_num("-0"), "0");
    assert_eq!(
        canon_num("12345678901234567890.123456789"),
        "12345678901234567890.123456789"
    );
}

/// Every table of the seeded classic + garbage schema (`make seed-oracle`)
/// exports in full: Parquet row count == the database's own `COUNT(*)`.
#[test]
#[ignore = "live: requires docker compose oracle + make seed-oracle"]
fn every_seeded_oracle_table_exports_every_row() {
    require_alive(LiveService::Oracle);
    let tables: Vec<String> = ora_text_rows(
        "SELECT table_name FROM user_tables WHERE table_name NOT LIKE 'ORA\\_%' ESCAPE '\\' \
         ORDER BY table_name",
    )
    .into_iter()
    .filter_map(|r| r[0].clone())
    .collect();
    assert!(tables.len() >= 15, "seed missing? tables: {tables:?}");
    let mut failures = Vec::new();
    for table in &tables {
        let want: usize = ora_text_rows(&format!("SELECT TO_CHAR(COUNT(*)) FROM {table}"))[0][0]
            .as_deref()
            .unwrap()
            .parse()
            .unwrap();
        let out = tempfile::tempdir().unwrap();
        let run = Rig::oracle_batch(table)
            .dest_path(out.path().to_path_buf())
            .run_args(&[]);
        if !run.status.success() {
            failures.push(format!(
                "{table}: run failed: {}",
                String::from_utf8_lossy(&run.stderr)
            ));
            continue;
        }
        // DuckDB counts what landed; the database counts what it holds.
        let got = duckdb_total_parquet_rows(out.path());
        if got != want {
            failures.push(format!("{table}: parquet {got} rows, source {want}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// `rivet init` on a NUMBER(19) key that straddles i64::MAX scaffolds keyset, and the run pages it exactly.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn an_init_generated_config_keysets_a_number_19_key_past_i64() {
    require_alive(LiveService::Oracle);
    let t = OracleTable::create("ora_n19", "id NUMBER(19) PRIMARY KEY, v VARCHAR2(20)");
    ora_exec(&format!(
        "INSERT INTO {} SELECT 9223372036854700000 + LEVEL, 'v' || LEVEL FROM dual CONNECT BY LEVEL <= 150001",
        t.name()
    ));
    let dir = tempfile::tempdir().unwrap();
    let env = [("ORACLE_URL", ORACLE_URL)];
    let init = run_rivet_in_dir(
        dir.path(),
        &[
            "init",
            "--source-env",
            "ORACLE_URL",
            "--table",
            t.name(),
            "--mode",
            "chunked",
            "-o",
            "rivet.yaml",
        ],
        &env,
    );
    assert!(
        init.status.success(),
        "init:\n{}",
        String::from_utf8_lossy(&init.stderr)
    );
    let yaml = std::fs::read_to_string(dir.path().join("rivet.yaml")).unwrap();
    assert!(
        yaml.contains("chunk_by_key: ID"),
        "init must keyset the NUMBER(19) key:\n{yaml}"
    );
    let run = run_rivet_in_dir(dir.path(), &["run", "-c", "rivet.yaml"], &env);
    assert!(
        run.status.success(),
        "run:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
    let out = dir.path().join("output").join(t.name());
    assert_eq!(duckdb_total_parquet_rows(&out), 150_001, "every row");
    assert_eq!(
        duckdb_dir_scalar(&out, "count(DISTINCT \"ID\")", None),
        150_001,
        "no row read twice"
    );
}

/// Keyset over the wide seeded ORDERS re-executes one page statement many times; every row lands once.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn keyset_over_a_wide_table_survives_many_page_reexecutions() {
    require_alive(LiveService::Oracle);
    let out = tempfile::tempdir().unwrap();
    let run = Rig::oracle_batch("ORDERS")
        .mode("chunked")
        .export_line("chunk_by_key: ID")
        .export_line("chunk_size: 7000")
        .dest_path(out.path().to_path_buf())
        .run_args(&[]);
    assert!(
        run.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
    let source: i64 = ora_text_rows("SELECT TO_CHAR(COUNT(*)) FROM ORDERS")[0][0]
        .as_deref()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        duckdb_total_parquet_rows(out.path()) as i64,
        source,
        "every row"
    );
    assert_eq!(
        duckdb_dir_scalar(out.path(), "count(DISTINCT \"ID\")", None),
        source,
        "no row twice"
    );
}

/// Types past Arrow's direct reach: s > p, negative scale, YEAR(9) intervals, BC dates, XMLTYPE, VECTOR.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn edge_oracle_types_export_losslessly() {
    require_alive(LiveService::Oracle);
    let t = OracleTable::create(
        "ora_edge",
        "id NUMBER(10) PRIMARY KEY, nsp NUMBER(3,5), nneg NUMBER(5,-2), \
         iym INTERVAL YEAR(9) TO MONTH, d DATE, x XMLTYPE, v VECTOR(3, FLOAT32)",
    );
    for row in [
        "1, 0.00123, 12300, INTERVAL '999999999-11' YEAR(9) TO MONTH, \
         TO_DATE('-0001-06-15','SYYYY-MM-DD'), XMLTYPE('<a>x</a>'), TO_VECTOR('[1.5, 2, -3]')",
        "2, -0.00999, -9999900, INTERVAL '-999999999-11' YEAR(9) TO MONTH, \
         TO_DATE('-4712-01-01','SYYYY-MM-DD'), NULL, NULL",
        "3, NULL, NULL, INTERVAL '0-0' YEAR TO MONTH, TO_DATE('2024-02-29','YYYY-MM-DD'), NULL, NULL",
    ] {
        ora_exec(&format!("INSERT INTO {} VALUES ({row})", t.name()));
    }
    let out = tempfile::tempdir().unwrap();
    let run = Rig::oracle_batch(t.name())
        .dest_path(out.path().to_path_buf())
        .run_args(&[]);
    assert!(
        run.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );

    for col in ["NSP", "NNEG"] {
        let want: BTreeMap<i64, Option<String>> =
            oracle_cells(t.name(), &format!("TO_CHAR({col}, 'TM9')"))
                .into_iter()
                .map(|(k, v)| (k, v.map(|s| canon_num(s.trim()))))
                .collect();
        assert_eq!(
            parquet_cells(out.path(), col),
            want,
            "{col} vs the database's TO_CHAR"
        );
    }
    let iym = parquet_cells(out.path(), "IYM");
    assert_eq!(iym[&1].as_deref(), Some("P999999999Y11M"));
    assert_eq!(iym[&2].as_deref(), Some("P-999999999Y-11M"));
    assert_eq!(iym[&3].as_deref(), Some("PT0S"));
    // DuckDB renders the calendar date (with its own BC marker); Oracle renders the same.
    let want_d: BTreeSet<String> = ora_text_rows(&format!(
        "SELECT TO_CHAR(d, 'YYYY-MM-DD') || CASE WHEN d < DATE '0001-01-01' THEN ' (BC)' END FROM {}",
        t.name()
    ))
    .into_iter()
    .map(|r| r[0].clone().unwrap())
    .collect();
    assert_eq!(
        duckdb_dir_parquet_distinct_strings(out.path(), "CAST(\"D\" AS DATE)"),
        want_d
    );
    let x = parquet_cells(out.path(), "X");
    assert_eq!(x[&1].as_deref().map(str::trim), Some("<a>x</a>"));
    let v = parquet_cells(out.path(), "V");
    let floats: Vec<f64> = serde_json::from_str(v[&1].as_deref().unwrap()).unwrap();
    assert_eq!(floats, vec![1.5, 2.0, -3.0]);
    assert_eq!(v[&2], None);
}

/// A logon trigger that makes comparison linguistic must not change what a keyset seek skips.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_linguistic_session_default_does_not_lose_keyset_rows() {
    require_alive(LiveService::Oracle);
    let t = OracleTable::create("ora_nls", "k VARCHAR2(20) PRIMARY KEY, n NUMBER");
    ora_exec(&format!(
        "INSERT INTO {} SELECT p || LPAD(l, 4, '0'), l \
         FROM (SELECT LEVEL l FROM dual CONNECT BY LEVEL <= 300) \
         CROSS JOIN (SELECT 'a' p FROM dual UNION ALL SELECT 'A' FROM dual \
                     UNION ALL SELECT TO_CHAR(UNISTR('\\00E4')) FROM dual UNION ALL SELECT 'Z' FROM dual)",
        t.name()
    ));
    let trigger = format!("{}_LOGON", t.name());
    ora_exec(&format!(
        "CREATE OR REPLACE TRIGGER {trigger} AFTER LOGON ON rivet.SCHEMA BEGIN \
         EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_SORT = GERMAN_CI'; \
         EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_COMP = LINGUISTIC'; END;"
    ));
    let out = tempfile::tempdir().unwrap();
    let run = Rig::oracle_batch(t.name())
        .mode("chunked")
        .export_line("chunk_by_key: K")
        .export_line("chunk_size: 70")
        .dest_path(out.path().to_path_buf())
        .run_args(&[]);
    ora_exec(&format!("DROP TRIGGER {trigger}"));
    assert!(
        run.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
    assert_eq!(duckdb_total_parquet_rows(out.path()), 1200, "every row");
    assert_eq!(
        duckdb_dir_scalar(out.path(), "count(DISTINCT \"K\")", None),
        1200,
        "no row twice"
    );
}

/// A TIMESTAMP(9) key is read at microseconds, so keyset on it is refused rather than looping.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn keyset_on_a_timestamp_9_key_fails_loudly_instead_of_looping() {
    require_alive(LiveService::Oracle);
    let t = OracleTable::create("ora_ts9", "t9 TIMESTAMP(9) PRIMARY KEY, n NUMBER");
    ora_exec(&format!(
        "INSERT INTO {} SELECT TIMESTAMP '2024-01-01 00:00:00.123456000' \
         + NUMTODSINTERVAL(LEVEL / 1e9, 'SECOND'), LEVEL FROM dual CONNECT BY LEVEL <= 50",
        t.name()
    ));
    let out = tempfile::tempdir().unwrap();
    let run = Rig::oracle_batch(t.name())
        .mode("chunked")
        .export_line("chunk_by_key: T9")
        .export_line("chunk_size: 7")
        .dest_path(out.path().to_path_buf())
        .run_with_envs_bounded(&[], std::time::Duration::from_secs(60))
        .expect("keyset on a TIMESTAMP(9) key must end, not loop");
    assert!(
        !run.status.success(),
        "a microsecond-read TIMESTAMP(9) key must be refused"
    );
    let err = String::from_utf8_lossy(&run.stderr);
    assert!(err.contains("T9"), "the refusal names the key:\n{err}");
}

/// `table:` written lower-case resolves the way Oracle does, so its primary key is recorded.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_lowercase_table_shortcut_records_its_primary_key() {
    require_alive(LiveService::Oracle);
    let t = seed_oracle_numeric_table(5);
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::oracle_batch(&t.name().to_lowercase())
        .export_named("lc")
        .dest_path(out.path().to_path_buf());
    let run = rig.run_args(&[]);
    assert!(
        run.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
    assert_eq!(
        recorded_primary_key(&rig.config_path(), "lc"),
        Some(vec!["ID".to_string()])
    );
}

/// `rivet check` refuses a lower-case strategy column and names Oracle's case rule.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn check_refuses_a_key_column_in_the_wrong_case() {
    require_alive(LiveService::Oracle);
    let t = seed_oracle_numeric_table(5);
    let check = Rig::oracle_batch(t.name())
        .mode("chunked")
        .export_line("chunk_column: id")
        .cli(&["check"]);
    assert!(
        !check.status.success(),
        "a key column the result lacks must fail check"
    );
    let err = String::from_utf8_lossy(&check.stderr);
    assert!(
        err.contains("column 'id' is not in the export's result; Oracle names match exactly and this one is spelled 'ID'"),
        "stderr:\n{err}"
    );
}

/// Range-chunking by day over a region-named TSTZ reads its bounds through the UTC re-projection.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn chunk_by_days_over_a_region_named_tstz_reads_every_row() {
    require_alive(LiveService::Oracle);
    let t = OracleTable::create(
        "ora_tzd",
        "id NUMBER(10) PRIMARY KEY, ts TIMESTAMP WITH TIME ZONE",
    );
    ora_exec(&format!(
        "INSERT INTO {} SELECT LEVEL, TO_TIMESTAMP_TZ('2024-07-01 10:00:00 Europe/Berlin', \
         'YYYY-MM-DD HH24:MI:SS TZR') + NUMTODSINTERVAL(LEVEL, 'HOUR') FROM dual CONNECT BY LEVEL <= 100",
        t.name()
    ));
    let out = tempfile::tempdir().unwrap();
    let run = Rig::oracle_batch(t.name())
        .mode("chunked")
        .export_line("chunk_column: TS")
        .export_line("chunk_by_days: 1")
        .dest_path(out.path().to_path_buf())
        .run_args(&[]);
    assert!(
        run.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
    assert_eq!(duckdb_total_parquet_rows(out.path()), 100, "every row");
    assert_eq!(
        duckdb_dir_scalar(out.path(), "count(DISTINCT \"ID\")", None),
        100,
        "no row twice"
    );
}

/// An unaliased ROWID cannot be re-read from an outer query; rivet says to alias it.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn an_unaliased_rowid_is_refused_with_the_alias_fix() {
    require_alive(LiveService::Oracle);
    let t = seed_oracle_numeric_table(3);
    let out = tempfile::tempdir().unwrap();
    let run = Rig::oracle_batch(t.name())
        .query(&format!("SELECT ROWID, id FROM {}", t.name()))
        .dest_path(out.path().to_path_buf())
        .run_args(&[]);
    assert!(!run.status.success(), "an unaliased ROWID must be refused");
    let err = String::from_utf8_lossy(&run.stderr);
    assert!(
        err.contains("column \"ROWID\" must be re-read through a conversion"),
        "stderr:\n{err}"
    );
}

/// Every source row landed exactly once: the database's own `COUNT(*)` against
/// DuckDB's row count and distinct-`ID` count over the parts in `out`.
fn assert_every_row_once(out: &Path, table: &str, ctx: &str) {
    let source: i64 = ora_text_rows(&format!("SELECT TO_CHAR(COUNT(*)) FROM {table}"))[0][0]
        .as_deref()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        duckdb_total_parquet_rows(out) as i64,
        source,
        "{ctx}: every row"
    );
    assert_eq!(
        duckdb_dir_scalar(out, "count(DISTINCT \"ID\")", None),
        source,
        "{ctx}: no row read twice"
    );
}

fn assert_ok(run: &std::process::Output, ctx: &str) {
    assert!(
        run.status.success(),
        "{ctx}: stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
}

/// Parallel keyset samples its range boundaries through a derived table Oracle must accept.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn parallel_keyset_reads_every_row_once() {
    require_alive(LiveService::Oracle);
    let t = seed_oracle_numeric_table(3_000);
    let out = tempfile::tempdir().unwrap();
    let run = Rig::oracle_batch(t.name())
        .mode("chunked")
        .export_line("chunk_by_key: ID")
        .export_line("parallel: 4")
        .export_line("chunk_size: 400")
        .dest_path(out.path().to_path_buf())
        .run_args(&[]);
    assert_ok(&run, "parallel keyset");
    assert_every_row_once(out.path(), t.name(), "parallel keyset");
}

/// `run --reconcile` and `rivet reconcile` both count the source through a derived table.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn reconcile_counts_the_source_on_both_paths() {
    require_alive(LiveService::Oracle);
    let t = seed_oracle_numeric_table(1_000);
    let export = unique_name("ora_rec");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::oracle_batch(t.name())
        .export_named(&export)
        .query(&format!("SELECT id, name FROM {}", t.name()))
        .mode("chunked")
        .export_line("chunk_column: ID")
        .export_line("chunk_size: 300")
        .export_line("chunk_checkpoint: true")
        .dest_path(out.path().to_path_buf());
    assert_ok(
        &rig.run_args(&["--export", &export, "--reconcile"]),
        "run --reconcile",
    );
    assert_every_row_once(out.path(), t.name(), "run --reconcile");
    let rec = rig.cli(&["reconcile", "--export", &export, "--format", "json"]);
    assert_ok(&rec, "rivet reconcile");
    let json: serde_json::Value =
        serde_json::from_str(String::from_utf8_lossy(&rec.stdout).trim()).expect("json report");
    let parts = json["partitions"].as_array().expect("partitions");
    assert_eq!(parts.len(), 4, "1000 rows / 300 per chunk: {json}");
    assert!(
        parts.iter().all(|p| p["status"] == "match"),
        "every partition matches the source: {json}"
    );
}

/// Range chunking over a `query:` export wraps it as a derived table.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn range_chunking_over_a_query_reads_every_row_once() {
    require_alive(LiveService::Oracle);
    let t = seed_oracle_numeric_table(1_000);
    let out = tempfile::tempdir().unwrap();
    let run = Rig::oracle_batch(t.name())
        .query(&format!("SELECT id, name, amount FROM {}", t.name()))
        .mode("chunked")
        .export_line("chunk_column: ID")
        .export_line("chunk_size: 300")
        .dest_path(out.path().to_path_buf())
        .run_args(&[]);
    assert_ok(&run, "range over query");
    assert_every_row_once(out.path(), t.name(), "range over query");
}

/// A `query:` ending in `;` or a `-- comment` still wraps, in full and chunked mode.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_query_with_a_trailing_semicolon_or_comment_still_exports() {
    require_alive(LiveService::Oracle);
    let t = seed_oracle_numeric_table(500);
    let n = t.name();
    for query in [
        format!("SELECT id, name FROM {n};"),
        format!("SELECT id, name FROM {n}\\n-- trailing comment"),
    ] {
        for mode in ["full", "chunked"] {
            let ctx = format!("{mode} over {query:?}");
            let out = tempfile::tempdir().unwrap();
            let mut rig = Rig::oracle_batch(n).query(&query).mode(mode);
            if mode == "chunked" {
                rig = rig
                    .export_line("chunk_column: ID")
                    .export_line("chunk_size: 200");
            }
            let run = rig.dest_path(out.path().to_path_buf()).run_args(&[]);
            assert_ok(&run, &ctx);
            assert_every_row_once(out.path(), n, &ctx);
        }
    }
}

/// The first `sid,serial#` of a `RIVET` session running (or last running) SQL that names `table`,
/// the describe probe excluded.
fn rivet_sessions_on(conn: &oracledb::Connection, table: &str) -> Vec<String> {
    let sql = format!(
        "SELECT TO_CHAR(s.sid) || ',' || TO_CHAR(s.serial#) FROM v$session s \
         JOIN v$sql q ON q.sql_id = NVL(s.sql_id, s.prev_sql_id) \
         WHERE s.username = 'RIVET' AND q.sql_text LIKE '%{table}%' \
         AND q.sql_text NOT LIKE '%v$session%' AND q.rows_processed > 0 ORDER BY 1"
    );
    let Ok(cursor) = conn.query(&sql, &[]) else {
        return vec![];
    };
    cursor
        .filter_map(|r| r.ok()?.get::<Option<String>>(0).ok()?)
        .collect()
}

/// A session killed on the server mid-export is retried on a fresh connection and
/// every row still lands exactly once.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_session_killed_mid_export_is_retried_and_delivers_every_row() {
    require_alive(LiveService::Oracle);
    let t = OracleTable::create(
        "ora_kill",
        "id NUMBER PRIMARY KEY, name VARCHAR2(40) NOT NULL, amount NUMBER(12,2)",
    );
    ora_exec(&format!(
        "INSERT INTO {} SELECT LEVEL, 'name_' || LEVEL, LEVEL * 1.25 FROM dual CONNECT BY LEVEL <= 40000",
        t.name()
    ));
    let export = unique_name("ora_kill");
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::oracle_batch(t.name())
        .export_named(&export)
        .mode("full")
        .export_line("tuning:")
        .export_line("  batch_size: 50")
        // 800 batches x 20 ms keeps the export's session alive well past the 2 s detection.
        .export_line("  throttle_ms: 20")
        .export_line("  max_retries: 3")
        .export_line("  retry_backoff_ms: 200")
        .dest_path(out.path().to_path_buf());

    let table = t.name().to_string();
    let killer = std::thread::spawn(move || {
        let sys = ora_system_conn();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
        // Once the export has fetched rows, kill every rivet session on the table that has
        // lived 2 s: the export's, and any idle probe connection (rivet must survive both).
        let mut seen: Option<(Vec<String>, std::time::Instant)> = None;
        while std::time::Instant::now() < deadline {
            let now = rivet_sessions_on(&sys, &table);
            match &seen {
                Some((prev, since)) if !now.is_empty() && *prev == now => {
                    if since.elapsed() >= std::time::Duration::from_secs(2) {
                        for sid in &now {
                            let kill = format!("ALTER SYSTEM KILL SESSION '{sid}' IMMEDIATE");
                            // ORA-00031: mid-call, the session dies when the call returns.
                            let _ = sys.execute(&kill, &[]);
                        }
                        return true;
                    }
                }
                _ if now.is_empty() => seen = None,
                _ => seen = Some((now, std::time::Instant::now())),
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        false
    });
    let run = rig.run_args(&["--export", &export]);
    assert!(
        killer.join().unwrap(),
        "the export's session was never seen to kill"
    );
    let stderr = String::from_utf8_lossy(&run.stderr);
    assert!(run.status.success(), "stderr:\n{stderr}");

    let journal = StateDb::next_to_config(&rig.config_path()).latest_journal_json(&export);
    assert!(
        journal.contains("RetryAttempted"),
        "the run must record a retry; journal:\n{journal}\nstderr:\n{stderr}"
    );
    let source: i64 = ora_text_rows(&format!("SELECT TO_CHAR(COUNT(*)) FROM {}", t.name()))[0][0]
        .as_deref()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(source, 40_000);
    assert_eq!(
        duckdb_total_parquet_rows(out.path()) as i64,
        source,
        "every row"
    );
    assert_eq!(
        duckdb_dir_scalar(out.path(), "count(DISTINCT \"ID\")", None),
        source,
        "no row twice"
    );
}

/// The doctor advisory for a user that cannot read the harm and governor views, verbatim.
const HARM_VIEWS_NOTE: &str = "[note] This Oracle user cannot read V$SYSSTAT / V$SYSTEM_EVENT, \
    so source-harm metrics and governor pressure will be absent. Data extraction is unaffected. \
    Grant with: GRANT SELECT_CATALOG_ROLE TO your_user; (or SELECT on V_$SYSSTAT and V_$SYSTEM_EVENT)";

/// A user with only CREATE SESSION and SELECT on one table, dropped on scope exit.
struct LeastPrivUser(String);

impl LeastPrivUser {
    const PASSWORD: &'static str = "Lp_passw0rd1";

    fn create(table: &str) -> Self {
        let name = unique_name("ora_lp").to_uppercase();
        ora_system_exec(&format!(
            "CREATE USER {name} IDENTIFIED BY \"{}\"",
            Self::PASSWORD
        ));
        let user = Self(name);
        ora_system_exec(&format!("GRANT CREATE SESSION TO {}", user.0));
        ora_system_exec(&format!("GRANT SELECT ON RIVET.{table} TO {}", user.0));
        user
    }

    fn url(&self) -> String {
        format!(
            "oracle://{}:{}@127.0.0.1:1521/FREEPDB1",
            self.0,
            Self::PASSWORD
        )
    }
}

impl Drop for LeastPrivUser {
    fn drop(&mut self) {
        let _ =
            std::panic::catch_unwind(|| ora_system_exec(&format!("DROP USER {} CASCADE", self.0)));
    }
}

/// `rivet doctor`'s stdout for an Oracle source authenticating via `url`; source auth must pass.
fn oracle_doctor_stdout(url: &str, table: &str) -> String {
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::oracle_batch(table)
        .export_named(&unique_name("ora_doctor"))
        .query(&format!("SELECT ID FROM RIVET.{table}"))
        .source_url(url)
        .dest_path(out.path().to_path_buf());
    let run = rig.cli(&["doctor"]);
    let stdout = String::from_utf8_lossy(&run.stdout).into_owned();
    assert!(
        stdout.contains("Source auth"),
        "source auth must pass for the note path to run; stdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
    stdout
}

/// Without catalog privileges `rivet doctor` says harm metrics and governor pressure will be absent.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn doctor_notes_unreadable_harm_views_for_a_least_privilege_user() {
    require_alive(LiveService::Oracle);
    let t = seed_oracle_numeric_table(5);
    let user = LeastPrivUser::create(t.name());
    let stdout = oracle_doctor_stdout(&user.url(), t.name());
    assert!(
        stdout.lines().any(|l| l == HARM_VIEWS_NOTE),
        "expected the exact note line; stdout:\n{stdout}"
    );
}

/// The stand's `rivet` user holds SELECT_CATALOG_ROLE, so the note stays silent.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn doctor_is_silent_on_harm_views_for_a_catalog_reader() {
    require_alive(LiveService::Oracle);
    let t = seed_oracle_numeric_table(5);
    let stdout = oracle_doctor_stdout(ORACLE_URL, t.name());
    assert!(
        !stdout.contains("V$SYSSTAT"),
        "no harm-view note for a catalog reader; stdout:\n{stdout}"
    );
}

/// Wide LOB rows honour `batch_size_memory_mb`: the probe and fetch array stay small until the width is known.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn wide_clob_rows_stay_within_the_memory_budget() {
    require_alive(LiveService::Oracle);
    let t = OracleTable::create("ora_wclob", "id NUMBER(10) PRIMARY KEY, c CLOB");
    ora_exec(&format!(
        "DECLARE l CLOB; BEGIN l := TO_CLOB(RPAD('x', 32000, 'x')); \
         FOR i IN 1..4 LOOP l := l || l; END LOOP; \
         FOR i IN 1..1000 LOOP INSERT INTO {} VALUES (i, l); END LOOP; END;",
        t.name()
    ));
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::oracle_batch(t.name())
        .export_named("wclob")
        .export_line("tuning:")
        .export_line("  batch_size_memory_mb: 16")
        .dest_path(out.path().to_path_buf());
    let run = rig.run_args(&[]);
    assert!(
        run.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
    assert_eq!(duckdb_total_parquet_rows(out.path()), 1000, "every row");
    assert_eq!(
        duckdb_dir_scalar(out.path(), "min(length(\"C\"))", None),
        512_000,
        "every CLOB whole"
    );
    let state = StateDb::next_to_config(&rig.config_path());
    let rss = state.metrics_row(&state.latest_run_id("wclob")).peak_rss_mb;
    // 1000 × 512 KB fetched 500 rows at a time peaked at ~1 GB; the budget keeps it bounded.
    assert!(
        rss.is_some_and(|mb| mb < 400),
        "peak RSS {rss:?} MB over a 16 MB batch budget"
    );
}

/// `statement_timeout_s` stops the server's work, not only the wait between fetched rows.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_statement_timeout_stops_a_long_query_on_the_server() {
    require_alive(LiveService::Oracle);
    let marker = "ROWNUM <= 60001";
    let out = tempfile::tempdir().unwrap();
    let run = Rig::oracle_batch("ORDERS")
        .query(&format!(
            "SELECT COUNT(*) AS c FROM ORDERS a CROSS JOIN (SELECT id FROM ORDERS WHERE {marker}) b"
        ))
        .export_line("tuning:")
        .export_line("  statement_timeout_s: 2")
        .dest_path(out.path().to_path_buf())
        .run_with_envs_bounded(&[], std::time::Duration::from_secs(60))
        .expect("a 2 s statement budget must end the run long before the query would");
    assert!(!run.status.success(), "the budget must fail the export");
    let err = String::from_utf8_lossy(&run.stderr);
    assert!(err.contains("statement timeout after 2s"), "stderr:\n{err}");
    let active = ora_text_rows(&format!(
        "SELECT TO_CHAR(COUNT(*)) FROM v$session s JOIN v$sql q ON q.sql_id = s.sql_id \
         WHERE s.status = 'ACTIVE' AND q.sql_text LIKE '%{marker}%' AND q.sql_text NOT LIKE '%v$session%'"
    ));
    assert_eq!(
        active[0][0].as_deref(),
        Some("0"),
        "the server stopped executing it"
    );
}

/// Schema-wide `rivet init` on Oracle discovers a table and scaffolds an Oracle source.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn init_oracle_schema_wide_discovers_seeded_table() {
    require_alive(LiveService::Oracle);
    let t = seed_oracle_numeric_table(3);
    let out = run_rivet(&["init", "--source", ORACLE_URL]);
    assert!(
        out.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let yaml = String::from_utf8_lossy(&out.stdout);
    assert!(yaml.contains("type: oracle"), "{yaml}");
    assert!(yaml.contains(&format!("- name: {}", t.name())), "{yaml}");
}

/// `rivet init --discover --table` names the Oracle table by its catalog owner and name.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn init_discover_names_the_oracle_table_scope() {
    require_alive(LiveService::Oracle);
    let t = seed_oracle_numeric_table(3);
    let out = run_rivet(&[
        "init",
        "--source",
        ORACLE_URL,
        "--table",
        t.name(),
        "--discover",
    ]);
    assert!(
        out.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let d: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(d["source_type"], "oracle");
    assert_eq!(d["scope"], format!("table \"RIVET\".\"{}\"", t.name()));
}

/// `time_window` on DATE and TIMESTAMP columns: the window literal parses under the pinned NLS masks.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn time_window_on_date_and_timestamp_reads_only_the_window() {
    require_alive(LiveService::Oracle);
    let t = OracleTable::create(
        "ora_tw",
        "id NUMBER(10) PRIMARY KEY, d DATE, ts TIMESTAMP(6)",
    );
    ora_exec(&format!(
        "INSERT INTO {} SELECT LEVEL, SYSDATE - CASE WHEN LEVEL <= 3 THEN 1 ELSE 100 END, \
         SYSTIMESTAMP - NUMTODSINTERVAL(CASE WHEN LEVEL <= 3 THEN 1 ELSE 100 END, 'DAY') \
         FROM dual CONNECT BY LEVEL <= 5",
        t.name()
    ));
    for col in ["D", "TS"] {
        let out = tempfile::tempdir().unwrap();
        let run = Rig::oracle_batch(t.name())
            .mode("time_window")
            .export_line(&format!("time_column: {col}"))
            .export_line("days_window: 7")
            .dest_path(out.path().to_path_buf())
            .run_args(&[]);
        assert!(
            run.status.success(),
            "{col} stderr:\n{}",
            String::from_utf8_lossy(&run.stderr)
        );
        assert_eq!(
            duckdb_total_parquet_rows(out.path()),
            3,
            "{col}: only the in-window rows"
        );
    }
}

/// `partition_by` on an Oracle DATE splits rows into day buckets. Not RED against a plain
/// `'YYYY-MM-DD'` bound: Oracle's lenient conversion accepts it under the pinned mask too.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn partition_by_a_date_column_buckets_every_row() {
    require_alive(LiveService::Oracle);
    let t = OracleTable::create("ora_part", "id NUMBER(10) PRIMARY KEY, d DATE");
    ora_exec(&format!(
        "INSERT INTO {} SELECT LEVEL, DATE '2024-01-01' + MOD(LEVEL, 3) FROM dual CONNECT BY LEVEL <= 9",
        t.name()
    ));
    let out = tempfile::tempdir().unwrap();
    let run = Rig::oracle_batch(t.name())
        .export_line("partition_by: D")
        .export_line("partition_granularity: day")
        .dest_path(out.path().join("{partition}"))
        .run_args(&[]);
    assert!(
        run.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
    for day in ["2024-01-01", "2024-01-02", "2024-01-03"] {
        assert_eq!(
            duckdb_total_parquet_rows(&out.path().join(format!("D={day}"))),
            3,
            "{day}"
        );
    }
}

/// A user column named like rivet's LOB empty-flag alias does not collide with it under a cursor wrap.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_column_named_like_the_lob_flag_does_not_collide() {
    require_alive(LiveService::Oracle);
    let t = OracleTable::create(
        "ora_flag",
        "id NUMBER(10) PRIMARY KEY, c CLOB, \"_rivet_empty_1\" VARCHAR2(10)",
    );
    for row in ["1, 'a', 'x'", "2, EMPTY_CLOB(), 'y'", "3, NULL, 'z'"] {
        ora_exec(&format!("INSERT INTO {} VALUES ({row})", t.name()));
    }
    let out = tempfile::tempdir().unwrap();
    let run = Rig::oracle_batch(t.name())
        .mode("incremental")
        .export_line("cursor_column: ID")
        .dest_path(out.path().to_path_buf())
        .run_args(&[]);
    assert!(
        run.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
    let c = parquet_cells(out.path(), "C");
    assert_eq!(
        (c[&2].as_deref(), c[&3].as_deref()),
        (Some(""), None),
        "empty vs NULL CLOB"
    );
    let user = parquet_cells(out.path(), "_rivet_empty_1");
    assert_eq!(
        user[&2].as_deref(),
        Some("y"),
        "the user's own column is intact"
    );
}

/// A 1000-column table with a LOB pages by key: the empty-value flags give way to Oracle's column cap.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_thousand_column_table_with_a_lob_pages_by_key() {
    require_alive(LiveService::Oracle);
    let cols: Vec<String> = (2..=999).map(|i| format!("c{i} VARCHAR2(5)")).collect();
    let t = OracleTable::create(
        "ora_w1000",
        &format!("id NUMBER(10) PRIMARY KEY, {}, cl CLOB", cols.join(", ")),
    );
    ora_exec(&format!(
        "INSERT INTO {} (id, c2, cl) VALUES (1, 'a', 'x')",
        t.name()
    ));
    ora_exec(&format!(
        "INSERT INTO {} (id, c2, cl) VALUES (2, 'b', 'y')",
        t.name()
    ));
    let out = tempfile::tempdir().unwrap();
    let run = Rig::oracle_batch(t.name())
        .mode("chunked")
        .export_line("chunk_by_key: ID")
        .export_line("chunk_size: 1")
        .dest_path(out.path().to_path_buf())
        .run_args(&[]);
    assert!(
        run.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
    assert_eq!(duckdb_total_parquet_rows(out.path()), 2, "every row");
    let err = String::from_utf8_lossy(&run.stderr);
    assert!(
        err.contains("read a zero-length value as NULL"),
        "the degradation is loud:\n{err}"
    );
}

/// A collection-typed column is refused by name, and `check --type-report --strict` fails on it.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn a_collection_column_is_refused_by_name_and_fails_strict_check() {
    require_alive(LiveService::Oracle);
    let ty = format!("{}_T", crate::common::unique_name("ora_va").to_uppercase());
    ora_exec(&format!("CREATE TYPE {ty} AS VARRAY(5) OF NUMBER"));
    let t = OracleTable::create("ora_va", &format!("id NUMBER(10) PRIMARY KEY, va {ty}"));
    ora_exec(&format!("INSERT INTO {} VALUES (1, {ty}(1, 2))", t.name()));
    let out = tempfile::tempdir().unwrap();
    let rig = Rig::oracle_batch(t.name()).dest_path(out.path().to_path_buf());
    let run = rig.run_args(&[]);
    let check = rig.cli(&["check", "--type-report", "--strict"]);
    drop(t);
    ora_exec(&format!("DROP TYPE {ty}"));
    assert!(!run.status.success(), "a VARRAY column must be refused");
    let err = String::from_utf8_lossy(&run.stderr);
    assert!(
        err.contains("column(s) [\"VA\"] are object or collection types"),
        "stderr:\n{err}"
    );
    assert!(
        !check.status.success(),
        "strict check must fail when the type report cannot be built"
    );
}

/// An INVISIBLE key column is named as such, not blamed on case.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn check_names_an_invisible_key_column() {
    require_alive(LiveService::Oracle);
    let t = OracleTable::create(
        "ora_inv",
        "k NUMBER(10) INVISIBLE PRIMARY KEY, a VARCHAR2(5)",
    );
    let check = Rig::oracle_batch(t.name())
        .mode("chunked")
        .export_line("chunk_by_key: K")
        .cli(&["check"]);
    assert!(!check.status.success());
    let err = String::from_utf8_lossy(&check.stderr);
    assert!(
        err.contains("column 'K' is INVISIBLE, and `table:` reads SELECT *, which leaves invisible columns out"),
        "stderr:\n{err}"
    );
}

/// A quoted lower-case column is suggested with its real spelling.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn check_suggests_the_real_spelling_of_a_quoted_lowercase_column() {
    require_alive(LiveService::Oracle);
    let t = OracleTable::create(
        "ora_lc",
        "\"id\" NUMBER(10) PRIMARY KEY, \"updated_at\" TIMESTAMP(6)",
    );
    let check = Rig::oracle_batch(t.name())
        .mode("incremental")
        .export_line("cursor_column: UPDATED_AT")
        .cli(&["check"]);
    assert!(!check.status.success());
    let err = String::from_utf8_lossy(&check.stderr);
    assert!(
        err.contains("column 'UPDATED_AT' is not in the export's result; Oracle names match exactly and this one is spelled 'updated_at'"),
        "stderr:\n{err}"
    );
}

/// `rivet check` probes a `query:`'s cursor range from the query, not from the table it reads.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn check_reads_a_query_cursor_range_from_the_query() {
    require_alive(LiveService::Oracle);
    let t = seed_oracle_numeric_table(10);
    let check = Rig::oracle_batch(t.name())
        .query(&format!(
            "SELECT ID + 1000000000 AS ID, NAME FROM {} WHERE ID > 5",
            t.name()
        ))
        .mode("incremental")
        .export_line("cursor_column: ID")
        .cli(&["check", "--json"]);
    assert!(
        check.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&check.stderr)
    );
    let d: serde_json::Value = serde_json::from_slice(&check.stdout).unwrap();
    let want = ora_text_rows(&format!(
        "SELECT TO_CHAR(MIN(ID + 1000000000)), TO_CHAR(MAX(ID + 1000000000)) FROM {} WHERE ID > 5",
        t.name()
    ));
    assert_eq!(
        d["diagnostic"]["cursor_min"].as_str(),
        want[0][0].as_deref()
    );
    assert_eq!(
        d["diagnostic"]["cursor_max"].as_str(),
        want[0][1].as_deref()
    );
}

/// `rivet init <init_args> -o rivet.yaml` then `rivet run`, both asserted green; returns the dir and the scaffold.
fn init_and_run(init_args: &[&str]) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let env = [("ORACLE_URL", ORACLE_URL)];
    let mut args = vec!["init", "--source-env", "ORACLE_URL"];
    args.extend_from_slice(init_args);
    args.extend_from_slice(&["-o", "rivet.yaml"]);
    let init = run_rivet_in_dir(dir.path(), &args, &env);
    assert!(
        init.status.success(),
        "init:\n{}",
        String::from_utf8_lossy(&init.stderr)
    );
    let yaml = std::fs::read_to_string(dir.path().join("rivet.yaml")).unwrap();
    let run = run_rivet_in_dir(dir.path(), &["run", "-c", "rivet.yaml"], &env);
    assert!(
        run.status.success(),
        "run:\n{}\n{yaml}",
        String::from_utf8_lossy(&run.stderr)
    );
    (dir, yaml)
}

/// A mixed-case table with an upper-case twin: init's config must read the mixed-case table, not the twin Oracle folds an unquoted name to.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn init_on_a_mixed_case_table_reads_it_not_its_upper_case_twin() {
    require_alive(LiveService::Oracle);
    let base = unique_name("ora_mx").to_uppercase();
    let (mixed_name, twin_name) = (format!("{base}_Mx"), format!("{base}_MX"));
    let cols = "id NUMBER(10) PRIMARY KEY, v VARCHAR2(10) NOT NULL";
    let mixed = OracleTable::create_exact(&mixed_name, cols);
    let twin = OracleTable::create_exact(&twin_name, cols);
    ora_exec(&format!(
        "INSERT INTO {} SELECT LEVEL, 'MIXED' FROM dual CONNECT BY LEVEL <= 3",
        mixed.name()
    ));
    ora_exec(&format!(
        "INSERT INTO {} SELECT LEVEL, 'TWIN' FROM dual CONNECT BY LEVEL <= 5",
        twin.name()
    ));
    let table_arg = format!("RIVET.\"{mixed_name}\"");
    let (dir, yaml) = init_and_run(&["--table", &table_arg, "--mode", "chunked"]);
    let out = dir.path().join("output").join(&mixed_name);
    assert_eq!(
        duckdb_total_parquet_rows(&out),
        3,
        "the mixed-case table's rows:\n{yaml}"
    );
    assert_eq!(
        duckdb_dir_scalar(&out, "count(*)", Some("\"V\" = 'MIXED'")),
        3,
        "no row from the twin:\n{yaml}"
    );
}

/// Oracle INTEGER (NUMBER, precision NULL, scale 0) is a keyset key and exports its 38-digit values, not a decimal(38,18) override.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn init_keysets_an_integer_pk_and_exports_a_38_digit_integer() {
    require_alive(LiveService::Oracle);
    let t = OracleTable::create("ora_int", "id INTEGER PRIMARY KEY, c_int INTEGER");
    let nines = "9".repeat(38);
    ora_exec(&format!(
        "INSERT INTO {} SELECT LEVEL, CASE WHEN LEVEL = 1 THEN {nines} ELSE LEVEL END \
         FROM dual CONNECT BY LEVEL <= 300",
        t.name()
    ));
    let (dir, yaml) = init_and_run(&["--table", t.name(), "--mode", "chunked"]);
    assert!(
        yaml.contains("chunk_by_key: ID"),
        "keyset on the INTEGER PK:\n{yaml}"
    );
    assert!(
        !yaml.contains("decimal(38,18)"),
        "no scaled override:\n{yaml}"
    );
    let out = dir.path().join("output").join(t.name());
    assert_eq!(duckdb_dir_scalar(&out, "count(DISTINCT \"ID\")", None), 300);
    assert_eq!(
        duckdb_dir_scalar(
            &out,
            "count(*)",
            Some(&format!("CAST(\"C_INT\" AS VARCHAR) = '{nines}'"))
        ),
        1,
        "the 38-digit value survives exactly"
    );
    assert_eq!(
        duckdb_dir_scalar(
            &out,
            "count(*)",
            Some("typeof(\"C_INT\") = 'DECIMAL(38,0)'")
        ),
        300,
        "the driver describes INTEGER as NUMBER(38,0), so the batch path types it decimal(38,0)"
    );
}

/// A never-analyzed table (NUM_ROWS NULL) is counted, not read as empty: 150K rows scaffold `chunked`.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn init_counts_a_never_analyzed_table_instead_of_calling_it_empty() {
    require_alive(LiveService::Oracle);
    let t = OracleTable::create("ora_unan", "id NUMBER(10) PRIMARY KEY, v VARCHAR2(20)");
    ora_exec(&format!(
        "INSERT INTO {} SELECT LEVEL, 'v' || LEVEL FROM dual CONNECT BY LEVEL <= 150000",
        t.name()
    ));
    let stats = ora_text_rows(&format!(
        "SELECT NVL(TO_CHAR(num_rows), 'NULL') FROM user_tables WHERE table_name = '{}'",
        t.name()
    ));
    assert_eq!(
        stats,
        vec![vec![Some("NULL".to_string())]],
        "fixture: no stats"
    );
    let (dir, yaml) = init_and_run(&["--table", t.name()]);
    assert!(yaml.contains("(~150K rows)"), "counted estimate:\n{yaml}");
    assert!(
        yaml.contains("mode: chunked"),
        "past the 100K threshold:\n{yaml}"
    );
    let out = dir.path().join("output").join(t.name());
    assert_eq!(duckdb_total_parquet_rows(&out), 150_000);
}

/// `init --table <synonym>` introspects the synonym's base table and the scaffold reads it.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn init_follows_a_synonym_to_its_base_table() {
    require_alive(LiveService::Oracle);
    struct DropSyn(String);
    impl Drop for DropSyn {
        fn drop(&mut self) {
            let _ = ora_conn().execute(&format!("DROP SYNONYM {}", self.0), &[]);
        }
    }
    let t = seed_oracle_numeric_table(40);
    let syn = unique_name("ora_syn").to_uppercase();
    ora_exec(&format!("CREATE SYNONYM {syn} FOR {}", t.name()));
    let _guard = DropSyn(syn.clone());
    let (dir, yaml) = init_and_run(&["--table", &syn, "--mode", "chunked"]);
    assert!(
        yaml.contains(&format!("table: RIVET.{}", t.name())),
        "the scaffold reads the base table:\n{yaml}"
    );
    let out = dir.path().join("output").join(t.name());
    assert_eq!(duckdb_dir_scalar(&out, "count(DISTINCT \"ID\")", None), 40);
}

/// `--schema` / `--table` / `--include` typed lower-case resolve as Oracle folds an unquoted name.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn init_folds_lower_case_cli_names_like_oracle_does() {
    require_alive(LiveService::Oracle);
    let t = seed_oracle_numeric_table(25);
    let lower = t.name().to_lowercase();
    let (dir, _) = init_and_run(&["--schema", "rivet", "--table", &lower]);
    assert_eq!(
        duckdb_total_parquet_rows(&dir.path().join("output").join(t.name())),
        25
    );
    let (dir, yaml) = init_and_run(&["--schema", "rivet", "--include", &lower]);
    assert!(yaml.contains(&format!("name: {}", t.name())), "{yaml}");
    assert_eq!(
        duckdb_total_parquet_rows(&dir.path().join("output").join(t.name())),
        25
    );
}

/// Parallel keyset over a DATE key: each range's upper bound converts through the pinned NLS mask.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn parallel_keyset_over_a_date_key_reads_every_row_once() {
    require_alive(LiveService::Oracle);
    let t = OracleTable::create(
        "ora_pkd",
        "id NUMBER(10) PRIMARY KEY, kdate DATE UNIQUE NOT NULL",
    );
    ora_exec(&format!(
        "INSERT INTO {} SELECT LEVEL, DATE '2020-01-01' + LEVEL * 37 / 86400 FROM dual CONNECT BY LEVEL <= 3000",
        t.name()
    ));
    let out = tempfile::tempdir().unwrap();
    let run = Rig::oracle_batch(t.name())
        .mode("chunked")
        .export_line("chunk_by_key: KDATE")
        .export_line("chunk_size: 300")
        .export_line("parallel: 3")
        .dest_path(out.path().to_path_buf())
        .run_args(&[]);
    assert!(
        run.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&run.stderr)
    );
    assert_eq!(duckdb_total_parquet_rows(out.path()), 3000, "every row");
    assert_eq!(
        duckdb_dir_scalar(out.path(), "count(DISTINCT \"ID\")", None),
        3000,
        "no row twice"
    );
}

/// `rivet check` on init's own column-list scaffold still finds the cursor's index.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn check_on_an_init_scaffold_finds_the_cursor_index() {
    require_alive(LiveService::Oracle);
    let dir = tempfile::tempdir().unwrap();
    let env = [("ORACLE_URL", ORACLE_URL)];
    let init = run_rivet_in_dir(
        dir.path(),
        &[
            "init",
            "--source-env",
            "ORACLE_URL",
            "--table",
            "ORDERS",
            "--mode",
            "incremental",
            "-o",
            "rivet.yaml",
        ],
        &env,
    );
    assert!(
        init.status.success(),
        "init:\n{}",
        String::from_utf8_lossy(&init.stderr)
    );
    let yaml = std::fs::read_to_string(dir.path().join("rivet.yaml")).unwrap();
    assert!(
        yaml.contains("query:"),
        "init writes a column-list query:\n{yaml}"
    );
    let check = run_rivet_in_dir(dir.path(), &["check", "-c", "rivet.yaml", "--json"], &env);
    assert!(
        check.status.success(),
        "check:\n{}",
        String::from_utf8_lossy(&check.stderr)
    );
    let d: serde_json::Value = serde_json::from_slice(&check.stdout).unwrap();
    assert_eq!(d["diagnostic"]["uses_index"], true, "{d}");
}

/// The type report calls a TIMESTAMP(9) column lossy: rivet keeps microseconds.
#[test]
#[ignore = "live: requires docker compose oracle"]
fn the_type_report_marks_a_timestamp_9_column_lossy() {
    require_alive(LiveService::Oracle);
    let t = OracleTable::create(
        "ora_ts9r",
        "id NUMBER(10) PRIMARY KEY, t9 TIMESTAMP(9), t6 TIMESTAMP(6)",
    );
    let check = Rig::oracle_batch(t.name()).cli(&["check", "--type-report", "--json"]);
    assert!(
        check.status.success(),
        "stderr:\n{}",
        String::from_utf8_lossy(&check.stderr)
    );
    let d: serde_json::Value = serde_json::from_slice(&check.stdout).unwrap();
    let fidelity = |col: &str| {
        d["columns"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["column"] == col)
            .unwrap()["fidelity"]
            .clone()
    };
    assert_eq!(fidelity("T9"), "lossy");
    assert_eq!(fidelity("T6"), "exact");
}
