//! The type policy runs in the run (ADR-0038 CP6): a lossy or unsupported column warns by
//! default and refuses under `--strict` before any data is read, with the verdict and code
//! `rivet check --type-report --strict` gives. The oracle for delivered rows is DuckDB.

use crate::common::*;

/// Parquet files anywhere under `dir`.
fn parquet_parts(dir: &std::path::Path) -> usize {
    walkdir_files(dir)
        .into_iter()
        .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
        .count()
}

/// Every file under `dir`, recursively (an absent dir has none).
fn walkdir_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() {
            out.extend(walkdir_files(&p));
        } else {
            out.push(p);
        }
    }
    out
}

#[cfg(feature = "oracle")]
/// An Oracle table with one sub-microsecond `TIMESTAMP(9)` value (a Lossy mapping) in row 1.
fn oracle_ts9_table() -> OracleTable {
    let t = OracleTable::create("tp_ts9", "id NUMBER(10) PRIMARY KEY, ts9 TIMESTAMP(9)");
    ora_exec(&format!(
        "INSERT INTO {} VALUES (1, TIMESTAMP '2024-01-02 03:04:05.123456789')",
        t.name()
    ));
    t
}

#[test]
#[ignore = "live: requires docker compose postgres (wal_level=logical)"]
fn pg_cdc_bare_numeric_warns_by_default_and_strict_refuses_before_any_part_or_checkpoint() {
    use postgres::NoTls;
    let tbl = unique_name("tp_cdc_numeric");
    let slot = unique_name("tp_numeric_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} (id INT PRIMARY KEY, amount NUMERIC)"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _slot = Slot(slot.clone());
    c.batch_execute(&format!(
        "INSERT INTO {tbl} VALUES (1, 12345.678901234567890123456789)"
    ))
    .unwrap();

    let d = tempfile::tempdir().unwrap();
    let out = d.path().join("out");
    let ckpt = d.path().join("cdc.ckpt");
    let rig = Rig::pg_cdc(&tbl, &slot)
        .dest_path(out.clone())
        .checkpoint_path(ckpt.clone());

    let strict = rig.run_args(&["--strict"]);
    let err = String::from_utf8_lossy(&strict.stderr);
    assert!(
        !strict.status.success(),
        "--strict must refuse; stderr:\n{err}"
    );
    assert!(err.contains("RIVET_TYPE_UNSAFE_MAPPING"), "{err}");
    assert!(
        err.contains("column 'amount' (source type 'numeric'): fidelity=unsupported"),
        "{err}"
    );
    assert_eq!(parquet_parts(&out), 0, "a refused run wrote a part");
    assert!(!ckpt.exists(), "a refused run wrote a checkpoint");

    let run = rig.run_args(&[]);
    let err = String::from_utf8_lossy(&run.stderr);
    assert!(
        run.status.success(),
        "the default run warns only; stderr:\n{err}"
    );
    assert!(
        err.contains("column 'amount' (source type 'numeric'): fidelity=unsupported"),
        "the default run must warn naming the column; stderr:\n{err}"
    );
    // The refusal consumed nothing: the row inserted before it is delivered now.
    assert_eq!(
        duckdb_dir_scalar(&out, "count(*)", Some("__op = 'insert' AND id = 1")),
        1
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn oracle_batch_timestamp9_warns_by_default_and_strict_refuses_before_export() {
    require_alive(LiveService::Oracle);
    let t = oracle_ts9_table();
    let d = tempfile::tempdir().unwrap();
    let out = d.path().join("out");
    let rig = Rig::oracle_batch(t.name()).dest_path(out.clone());

    let strict = rig.run_args(&["--strict"]);
    let err = String::from_utf8_lossy(&strict.stderr);
    assert!(
        !strict.status.success(),
        "--strict must refuse; stderr:\n{err}"
    );
    assert!(err.contains("RIVET_TYPE_UNSAFE_MAPPING"), "{err}");
    assert!(
        err.contains("column 'TS9'") && err.contains("fidelity=lossy"),
        "{err}"
    );
    assert_eq!(parquet_parts(&out), 0, "a refused run wrote a part");

    let run = rig.run_args(&[]);
    let err = String::from_utf8_lossy(&run.stderr);
    assert!(
        run.status.success(),
        "the default run warns only; stderr:\n{err}"
    );
    assert!(
        err.contains("column 'TS9'") && err.contains("fidelity=lossy"),
        "the default run must warn naming the column; stderr:\n{err}"
    );
    assert_eq!(duckdb_dir_scalar(&out, "count(*)", Some("\"ID\" = 1")), 1);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires docker compose oracle"]
fn check_strict_and_run_strict_refuse_the_same_config_with_the_same_code() {
    require_alive(LiveService::Oracle);
    let t = oracle_ts9_table();
    let d = tempfile::tempdir().unwrap();
    let out = d.path().join("out");
    let rig = Rig::oracle_batch(t.name()).dest_path(out.clone());

    let check = rig.cli(&["check", "--type-report", "--strict"]);
    let run = rig.run_args(&["--strict"]);
    let check_err = String::from_utf8_lossy(&check.stderr);
    let run_err = String::from_utf8_lossy(&run.stderr);
    assert!(!check.status.success(), "check --strict:\n{check_err}");
    assert!(!run.status.success(), "run --strict:\n{run_err}");
    assert_eq!(check.status.code(), run.status.code(), "one exit class");
    for (who, err) in [("check", &check_err), ("run", &run_err)] {
        assert!(err.contains("RIVET_TYPE_UNSAFE_MAPPING"), "{who}: {err}");
        assert!(err.contains("column 'TS9'"), "{who}: {err}");
    }
    assert_eq!(parquet_parts(&out), 0, "run --strict wrote a part");

    // And both accept it without --strict.
    let check = rig.cli(&["check", "--type-report"]);
    assert!(
        check.status.success(),
        "{}",
        String::from_utf8_lossy(&check.stderr)
    );
}
