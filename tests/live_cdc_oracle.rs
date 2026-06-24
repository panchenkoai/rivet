//! CDC oracle round-trip harness (#4) — the CDC Parquet must load and read back
//! *correctly in external engines* (DuckDB + ClickHouse), not just rivet's own
//! reader. Codifies the manual type-matrix validation: capture a typed table via
//! CDC, then have two real OLAP engines read the output and assert the values.
//!
//! Needs the `cdc` profile (mysql-cdc) AND the `loaders` profile (rivet-duckdb +
//! rivet-clickhouse), so it runs in nightly. The test name carries `via_duckdb`
//! so the per-commit CI (which has no loaders) skips it like the other oracle tests.
//!
//!     docker compose --profile cdc --profile loaders up -d mysql-cdc duckdb clickhouse
//!     cargo test --test live_cdc_oracle -- --ignored

mod common;

use std::process::Command;

use common::*;
use mysql::prelude::Queryable;

#[test]
#[ignore = "live+loaders: cdc + loaders profiles (nightly); reads CDC parquet via_duckdb + clickhouse"]
fn cdc_types_round_trip_via_duckdb_and_clickhouse() {
    let mut c = mysql::Pool::new(MYSQL_CDC_URL)
        .expect("mysql-cdc pool")
        .get_conn()
        .expect("conn");
    let table = "cdc_oracle";
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table} (id INT PRIMARY KEY, amount DECIMAL(10,2), n BIGINT, meta JSON)"
    ))
    .unwrap();

    // Checkpoint, then one fully-typed row.
    let d = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("ckpt");
    let row: mysql::Row = c.query_first("SHOW MASTER STATUS").unwrap().unwrap();
    let (file, pos): (String, u64) = (row.get(0).unwrap(), row.get(1).unwrap());
    std::fs::write(&ckpt, format!(r#"{{"file":"{file}","pos":{pos}}}"#)).unwrap();
    c.query_drop(format!(
        r#"INSERT INTO {table} VALUES (1, 12.34, 9000000000, '{{"k":1}}')"#
    ))
    .unwrap();

    // Capture into the shared bind mount both engines can read.
    let (host, container) = clickhouse_shared_workdir("cdc_oracle");
    let yaml = format!(
        r#"source: {{type: mysql, url: "{MYSQL_CDC_URL}"}}
exports:
  - name: {table}
    table: {table}
    mode: cdc
    format: parquet
    cdc: {{ checkpoint: "{ckpt}", until_current: true, server_id: 8888 }}
    destination: {{ type: local, path: "{host}" }}
"#,
        ckpt = ckpt.display(),
        host = host.display(),
    );
    let cfg = write_config(&d, &yaml);
    let res = Command::new(RIVET_BIN)
        .args(["run", "--config", cfg.to_str().unwrap()])
        .output()
        .expect("spawn rivet");
    assert!(
        res.status.success(),
        "cdc capture failed:\n{}",
        String::from_utf8_lossy(&res.stderr)
    );

    // ---- DuckDB oracle: read_parquet over the absolute /work path ----
    let dj = duckdb_run_sql_json(&format!(
        "SELECT amount, n, meta FROM read_parquet('{container}/*.parquet') WHERE id = 1"
    ));
    let r0 = dj["rows"][0]
        .as_array()
        .unwrap_or_else(|| panic!("duckdb returned no row: {dj}"));
    assert_eq!(r0[0].as_str().unwrap(), "12.34", "duckdb: decimal value");
    assert_eq!(
        r0[1].as_str().unwrap(),
        "9000000000",
        "duckdb: bigint value"
    );
    let meta: serde_json::Value =
        serde_json::from_str(r0[2].as_str().unwrap()).expect("duckdb meta is JSON");
    assert_eq!(meta["k"], 1, "duckdb: json column round-trips");

    // ---- ClickHouse oracle: file() over the path relative to /work ----
    // ClickHouse is flaky to keep alive on some Docker hosts (exits 133 within
    // seconds locally); validate it when the server is reachable — which it always
    // is on nightly's Linux runner. The DuckDB oracle above already covers the core
    // "an external engine reads the CDC parquet correctly" claim.
    let reachable = std::net::TcpStream::connect_timeout(
        &"127.0.0.1:8123".parse().unwrap(),
        std::time::Duration::from_secs(1),
    )
    .is_ok();
    if reachable {
        let rel = container.trim_start_matches("/work/");
        let cj = clickhouse_run_sql_json(&format!(
            "SELECT toString(amount) AS amount, toString(n) AS n, meta FROM file('{rel}/*.parquet', 'Parquet') WHERE id = 1"
        ));
        assert_eq!(
            cj[0]["amount"].as_str().unwrap(),
            "12.34",
            "clickhouse: decimal value"
        );
        assert_eq!(
            cj[0]["n"].as_str().unwrap(),
            "9000000000",
            "clickhouse: bigint value"
        );
        let cmeta: serde_json::Value =
            serde_json::from_str(cj[0]["meta"].as_str().expect("meta string"))
                .expect("clickhouse meta is JSON");
        assert_eq!(cmeta["k"], 1, "clickhouse: json column round-trips");
    } else {
        eprintln!(
            "NOTE: ClickHouse unreachable (:8123) — skipping the ClickHouse oracle. \
             DuckDB validated above; ClickHouse runs on nightly's Linux runner."
        );
    }

    let _ = c.query_drop(format!("DROP TABLE IF EXISTS {table}"));
}
