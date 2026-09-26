//! Spike: can rivet read Oracle through the pure-Rust `oracledb` beta without losing values?
//! Builds Arrow itself (never the driver's `query_arrow`), writes Parquet, and prints the
//! server's own rendering of every value beside it for an independent comparison.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use oracledb::{Connection, Metadata, OracleNumber, OracleTimestamp, Row};

/// `oracledb::Error` implements only `Debug`; lift it into anyhow.
trait Ora<T> {
    fn ora(self) -> Result<T>;
}
impl<T> Ora<T> for std::result::Result<T, oracledb::Error> {
    fn ora(self) -> Result<T> {
        self.map_err(|e| anyhow::anyhow!("{e:?}"))
    }
}

const SETUP: &[&str] = &[
    "BEGIN EXECUTE IMMEDIATE 'DROP TABLE spike_types PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;",
    "CREATE TABLE spike_types (
        id NUMBER(10) PRIMARY KEY,
        n_bare NUMBER, n_dec NUMBER(38,10), n_int18 NUMBER(18), n_small NUMBER(5,2),
        bf BINARY_FLOAT, bd BINARY_DOUBLE, b BOOLEAN,
        d DATE, ts TIMESTAMP(9), tstz TIMESTAMP(9) WITH TIME ZONE,
        tsltz TIMESTAMP(6) WITH LOCAL TIME ZONE,
        ivds INTERVAL DAY(3) TO SECOND(6), ivym INTERVAL YEAR(3) TO MONTH,
        vc VARCHAR2(4000 CHAR), nvc NVARCHAR2(100), ch CHAR(5 CHAR),
        cl CLOB, rw RAW(16), bl BLOB, js JSON)",
    "INSERT INTO spike_types VALUES (1,
        123.45, 1.5, 42, 1.25,
        1.5, 2.25, TRUE,
        TO_DATE('2024-02-29 13:14:15','YYYY-MM-DD HH24:MI:SS'),
        TO_TIMESTAMP('2024-02-29 13:14:15.123456789','YYYY-MM-DD HH24:MI:SS.FF9'),
        TO_TIMESTAMP_TZ('2024-02-29 10:00:00.5 +02:00','YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'),
        TO_TIMESTAMP_TZ('2024-02-29 10:00:00 +00:00','YYYY-MM-DD HH24:MI:SS TZH:TZM'),
        INTERVAL '3 04:05:06.789' DAY TO SECOND, INTERVAL '2-11' YEAR TO MONTH,
        'plain', N'unicode ✓', 'ab',
        'short clob', HEXTORAW('00FF10'), HEXTORAW('DEADBEEF'), JSON('{\"a\":1,\"b\":[true,null]}'))",
    "INSERT INTO spike_types VALUES (2,
        12345678901234567890.123456789, 1234567890123456789012345678.0123456789,
        -999999999999999999, -999.99,
        BINARY_FLOAT_NAN, BINARY_DOUBLE_INFINITY, NULL,
        TO_DATE('0001-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),
        TO_TIMESTAMP('9999-12-31 23:59:59.999999999','YYYY-MM-DD HH24:MI:SS.FF9'),
        TO_TIMESTAMP_TZ('2024-06-30 23:30:00 -05:30','YYYY-MM-DD HH24:MI:SS TZH:TZM'),
        NULL,
        INTERVAL '-1 00:00:00.000001' DAY TO SECOND, INTERVAL '-0-1' YEAR TO MONTH,
        '', NULL, '   ',
        TO_CLOB(RPAD('a', 4000, 'a')) || TO_CLOB(RPAD('b', 4000, 'b')) || TO_CLOB(RPAD('c', 4000, 'c')) || TO_CLOB(RPAD('d', 4000, 'd')) || TO_CLOB(RPAD('e', 4000, 'e')) || TO_CLOB(RPAD('f', 4000, 'f')) || TO_CLOB(RPAD('g', 4000, 'g')) || TO_CLOB(RPAD('h', 4000, 'h')) || TO_CLOB(RPAD('i', 4000, 'i')) || TO_CLOB(RPAD('j', 4000, 'j')) || TO_CLOB(RPAD('k', 4000, 'k')) || TO_CLOB(RPAD('l', 4000, 'l')) || TO_CLOB(RPAD('m', 4000, 'm')) || TO_CLOB(RPAD('n', 4000, 'n')) || TO_CLOB(RPAD('o', 4000, 'o')) || TO_CLOB(RPAD('p', 4000, 'p')) || TO_CLOB(RPAD('q', 4000, 'q')) || TO_CLOB(RPAD('r', 4000, 'r')) || TO_CLOB(RPAD('s', 4000, 's')) || TO_CLOB(RPAD('t', 4000, 't')) || TO_CLOB(RPAD('u', 4000, 'u')) || TO_CLOB(RPAD('v', 4000, 'v')) || TO_CLOB(RPAD('w', 4000, 'w')) || TO_CLOB(RPAD('x', 4000, 'x')) || TO_CLOB(RPAD('y', 4000, 'y')),
        NULL, EMPTY_BLOB(), JSON('\"str\"'))",
    "INSERT INTO spike_types (id, n_bare, vc, nvc) VALUES (3, 1E125, '   ', N'日本語 🦀')",
    "INSERT INTO spike_types (id, n_bare) VALUES (4, 1E-130)",
    "INSERT INTO spike_types (id, n_bare, d) VALUES (5, -0.000000000000000000000000000000000000001,
        TO_DATE('-4712-01-01','SYYYY-MM-DD'))",
    "BEGIN EXECUTE IMMEDIATE 'DROP TABLE spike_tz_region PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;",
    "CREATE TABLE spike_tz_region (id NUMBER PRIMARY KEY, tstz TIMESTAMP WITH TIME ZONE)",
    "INSERT INTO spike_tz_region VALUES (1, TO_TIMESTAMP_TZ('2024-07-01 10:00:00 Europe/Berlin','YYYY-MM-DD HH24:MI:SS TZR'))",
];

/// Server-side rendering of every column: Oracle's own text, the independent reference.
const TRUTH: &str = "SELECT TO_CHAR(id),
    TO_CHAR(n_bare,'TM9'), TO_CHAR(n_dec,'TM9'), TO_CHAR(n_int18), TO_CHAR(n_small,'TM9'),
    TO_CHAR(bf), TO_CHAR(bd), CASE WHEN b IS NULL THEN NULL WHEN b THEN 'true' ELSE 'false' END,
    TO_CHAR(d,'SYYYY-MM-DD\"T\"HH24:MI:SS'), TO_CHAR(ts,'YYYY-MM-DD\"T\"HH24:MI:SS.FF9'),
    TO_CHAR(SYS_EXTRACT_UTC(tstz),'YYYY-MM-DD\"T\"HH24:MI:SS.FF9'),
    TO_CHAR(SYS_EXTRACT_UTC(tsltz),'YYYY-MM-DD\"T\"HH24:MI:SS.FF9'),
    TO_CHAR(ivds), TO_CHAR(ivym), vc, nvc, ch, TO_CHAR(DBMS_LOB.GETLENGTH(cl)), RAWTOHEX(rw),
    TO_CHAR(DBMS_LOB.GETLENGTH(bl)), JSON_SERIALIZE(js)
  FROM spike_types ORDER BY id";

fn connect() -> Result<Connection> {
    let config = oracledb::Config::default()
        .set_credentials("spike", "rivet")
        .set_connect_string("localhost:15210/FREEPDB1").ora()?;
    let conn = oracledb::connect(config).ora().context("connect")?;
    for s in [
        "ALTER SESSION SET TIME_ZONE='+00:00'",
        "ALTER SESSION SET NLS_NUMERIC_CHARACTERS='.,'",
    ] {
        conn.execute(s, &[]).ora()?;
    }
    Ok(conn)
}

/// The Arrow type rivet would declare for one Oracle column.
fn arrow_type(m: &Metadata) -> DataType {
    match m.db_type().name() {
        "DB_TYPE_NUMBER" if m.precision() > 0 && m.scale() >= 0 => {
            DataType::Decimal128(m.precision(), m.scale())
        }
        "DB_TYPE_NUMBER" => DataType::Utf8,
        "DB_TYPE_BINARY_FLOAT" => DataType::Float32,
        "DB_TYPE_BINARY_DOUBLE" => DataType::Float64,
        "DB_TYPE_BOOLEAN" => DataType::Boolean,
        "DB_TYPE_DATE" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "DB_TYPE_TIMESTAMP" => DataType::Timestamp(TimeUnit::Nanosecond, None),
        "DB_TYPE_TIMESTAMP_TZ" | "DB_TYPE_TIMESTAMP_LTZ" => {
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        }
        "DB_TYPE_RAW" | "DB_TYPE_BLOB" | "DB_TYPE_LONG_RAW" => DataType::Binary,
        _ => DataType::Utf8,
    }
}

/// Seconds since the epoch for the timestamp's own fields, read as UTC.
fn epoch_secs(t: &OracleTimestamp) -> i64 {
    let (y, m, d) = (t.year() as i64, t.month() as i64, t.day() as i64);
    let (y2, m2) = if m <= 2 { (y - 1, m + 9) } else { (y, m - 3) };
    let era = y2.div_euclid(400);
    let yoe = y2 - era * 400;
    let doy = (153 * m2 + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146097 + doe - 719468;
    days * 86400 + t.hour() as i64 * 3600 + t.minute() as i64 * 60 + t.second() as i64
}

fn decimal_from_str(s: &str, scale: i8) -> Result<i128> {
    let (int, frac) = s.split_once('.').unwrap_or((s, ""));
    let neg = int.starts_with('-');
    let int = int.trim_start_matches('-');
    let mut frac = frac.to_string();
    anyhow::ensure!(frac.len() <= scale as usize, "{s} has more than {scale} fraction digits");
    while frac.len() < scale as usize {
        frac.push('0');
    }
    let v: i128 = format!("{}{}", if int.is_empty() { "0" } else { int }, frac).parse()?;
    Ok(if neg { -v } else { v })
}

fn column(rows: &[Row], i: usize, m: &Metadata, ty: &DataType) -> Result<ArrayRef> {
    Ok(match ty {
        DataType::Decimal128(p, s) => {
            let mut b = Decimal128Builder::new().with_precision_and_scale(*p, *s)?;
            for r in rows {
                match r.get::<Option<OracleNumber>>(i).ora()? {
                    Some(n) => b.append_value(decimal_from_str(&n.to_string(), *s)?),
                    None => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DataType::Float32 => Arc::new(Float32Array::from(
            rows.iter().map(|r| r.get::<Option<f32>>(i)).collect::<std::result::Result<Vec<_>, _>>().ora()?,
        )),
        DataType::Float64 => Arc::new(Float64Array::from(
            rows.iter().map(|r| r.get::<Option<f64>>(i)).collect::<std::result::Result<Vec<_>, _>>().ora()?,
        )),
        DataType::Boolean => Arc::new(BooleanArray::from(
            rows.iter().map(|r| r.get::<Option<bool>>(i)).collect::<std::result::Result<Vec<_>, _>>().ora()?,
        )),
        DataType::Timestamp(unit, tz) => {
            let mut out = Vec::new();
            for r in rows {
                out.push(r.get::<Option<OracleTimestamp>>(i).ora()?.and_then(|t| {
                    let secs = epoch_secs(&t);
                    match unit {
                        TimeUnit::Microsecond => Some(secs * 1_000_000),
                        _ => match secs.checked_mul(1_000_000_000) {
                            Some(v) => Some(v + t.nanoseconds() as i64),
                            None => {
                                println!("   !! {} {t}: past the i64 nanosecond range (year > 2262)", m.name());
                                None
                            }
                        },
                    }
                }));
            }
            match unit {
                TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(out).with_timezone_opt(tz.clone())),
                _ => Arc::new(TimestampNanosecondArray::from(out).with_timezone_opt(tz.clone())),
            }
        }
        DataType::Binary => Arc::new(BinaryArray::from(
            rows.iter()
                .map(|r| r.get::<Option<Vec<u8>>>(i))
                .collect::<std::result::Result<Vec<_>, _>>().ora()?
                .iter()
                .map(|v| v.as_deref())
                .collect::<Vec<_>>(),
        )),
        _ => {
            let mut b = StringBuilder::new();
            for r in rows {
                let v: Option<String> = match m.db_type().name() {
                    "DB_TYPE_NUMBER" => r.get::<Option<OracleNumber>>(i).ora()?.map(|n| n.to_string()),
                    "DB_TYPE_INTERVAL_DS" => r.get::<Option<oracledb::OracleIntervalDS>>(i).ora()?.map(|v| v.to_string()),
                    "DB_TYPE_INTERVAL_YM" => r.get::<Option<oracledb::OracleIntervalYM>>(i).ora()?.map(|v| v.to_string()),
                    "DB_TYPE_JSON" => r.get::<Option<oracledb::JsonValue>>(i).ora()?.map(|v| format!("{v:?}")),
                    _ => r.get::<Option<String>>(i).ora()?,
                };
                b.append_option(v);
            }
            Arc::new(b.finish())
        }
    })
}

fn fetch(conn: &Connection, sql: &str) -> Result<(Vec<Metadata>, Vec<Row>)> {
    let cursor = conn.query(sql, &[]).ora()?;
    let meta = cursor.columns().to_vec();
    let rows = cursor.collect::<std::result::Result<Vec<_>, _>>().ora()?;
    Ok((meta, rows))
}

/// Mine the redo for one fresh insert through the same driver: PL/SQL calls + V$LOGMNR_CONTENTS.
fn logminer() -> Result<()> {
    let app = connect()?;
    app.execute("DELETE FROM spike_types WHERE id = 42", &[]).ora()?;
    app.execute("INSERT INTO spike_types (id, n_bare, vc, d) VALUES (42, 3.14159, 'mined ✓', SYSDATE)", &[]).ora()?;
    app.commit().ora()?;
    let root = oracledb::connect(
        oracledb::Config::default()
            .set_credentials("system", "rivet")
            .set_connect_string("localhost:15210/FREE")
            .ora()?,
    )
    .ora()?;
    let logs: Vec<String> = root
        .query("SELECT member FROM v$logfile ORDER BY group#", &[])
        .ora()?
        .map(|r| r.and_then(|r| r.get::<String>(0)))
        .collect::<std::result::Result<_, _>>()
        .ora()?;
    for (i, f) in logs.iter().enumerate() {
        let opt = if i == 0 { "DBMS_LOGMNR.NEW" } else { "DBMS_LOGMNR.ADDFILE" };
        root.execute(&format!("BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => :1, OPTIONS => {opt}); END;"), &[f]).ora()?;
    }
    root.execute("BEGIN DBMS_LOGMNR.START_LOGMNR(OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG); END;", &[]).ora()?;
    for q in ["SELECT seg_owner||'.'||table_name||' '||operation||' '||COUNT(*) FROM v$logmnr_contents WHERE seg_owner NOT IN ('SYS','SYSTEM') OR seg_owner IS NULL GROUP BY seg_owner, table_name, operation ORDER BY 1"] {
        for r in root.query(q, &[]).ora()? {
            println!("seen: {}", r.ora()?.get::<Option<String>>(0).ora()?.unwrap_or_default());
        }
    }
    let cur = root
        .query(
            "SELECT TO_CHAR(scn), xidusn||'.'||xidslt||'.'||xidsqn, operation, table_name,
                    DBMS_LOGMNR.MINE_VALUE(redo_value, 'SPIKE.SPIKE_TYPES.ID')||' vc='||DBMS_LOGMNR.MINE_VALUE(redo_value, 'SPIKE.SPIKE_TYPES.VC'),
                    DBMS_LOGMNR.MINE_VALUE(redo_value, 'SPIKE.SPIKE_TYPES.N_BARE'),
                    SUBSTR(info, 1, 120)
               FROM v$logmnr_contents
              WHERE seg_owner = 'SPIKE' AND table_name = 'SPIKE_TYPES'",
            &[],
        )
        .ora()?;
    for r in cur {
        let r = r.ora()?;
        let cells: Vec<String> = (0..7).map(|i| r.get::<Option<String>>(i).ok().flatten().unwrap_or_else(|| "NULL".into())).collect();
        println!("mined: {}", cells.join(" | "));
    }
    root.execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;", &[]).ora()?;
    Ok(())
}

fn main() -> Result<()> {
    if std::env::args().nth(1).as_deref() == Some("logminer") {
        return logminer();
    }
    let conn = connect()?;
    for s in SETUP {
        conn.execute(s, &[]).ora().with_context(|| format!("setup: {}", &s[..s.len().min(60)]))?;
    }
    conn.commit().ora()?;

    let (meta, rows) = fetch(&conn, "SELECT * FROM spike_types ORDER BY id")?;
    let mut fields = Vec::new();
    let mut arrays = Vec::new();
    for (i, m) in meta.iter().enumerate() {
        let ty = arrow_type(m);
        println!(
            "col {:6} {:24} p={:3} s={:4} -> {ty:?}",
            m.name(),
            m.db_type().name(),
            m.precision(),
            m.scale()
        );
        match column(&rows, i, m, &ty) {
            Ok(a) => {
                fields.push(Field::new(m.name().to_lowercase(), ty, true));
                arrays.push(a);
            }
            Err(e) => println!("   !! {}: {e:#}", m.name()),
        }
    }
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?;
    let file = std::fs::File::create("/tmp/oracle-spike.parquet")?;
    let mut w = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None)?;
    w.write(&batch)?;
    w.close()?;
    println!("wrote /tmp/oracle-spike.parquet: {} rows x {} cols", batch.num_rows(), batch.num_columns());

    let (tmeta, truth) = fetch(&conn, TRUTH)?;
    let mut out = String::new();
    for r in &truth {
        let cells: Vec<String> = (0..tmeta.len())
            .map(|i| r.get::<Option<String>>(i).map(|v| v.unwrap_or_else(|| "NULL".into())).unwrap_or_else(|e| format!("ERR {e:?}")))
            .collect();
        out.push_str(&cells.join("\t"));
        out.push('\n');
    }
    std::fs::write("/tmp/oracle-spike-truth.tsv", out)?;
    println!("wrote /tmp/oracle-spike-truth.tsv");

    let region = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        fetch(&conn, "SELECT tstz FROM spike_tz_region").map(|(_, rows)| {
            rows.iter().map(|r| r.get::<Option<OracleTimestamp>>(0).map(|t| t.map(|t| t.to_string()))).collect::<Vec<_>>()
        })
    }));
    println!("TSTZ with a region name: {region:?}");
    Ok(())
}
