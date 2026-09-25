//! Oracle batch source, end to end through the Rig. The independent oracle is the
//! database's own rendering of each value (`TO_CHAR`, `SYS_EXTRACT_UTC`,
//! `RAWTOHEX`, `DBMS_LOB.GETLENGTH`) read as text, compared cell by cell with the
//! Parquet rivet wrote — never rivet's own summary.

use std::collections::BTreeMap;
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
            canon_ts(&dt.format("%Y-%m-%dT%H:%M:%S%.6f").to_string())
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
