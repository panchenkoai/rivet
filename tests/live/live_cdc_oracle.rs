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
//!     cargo test --test live_suite -- --ignored

use std::process::Command;

use crate::common::*;
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
    // Diagnose the parallel-suite flake: a successful run with ZERO captured
    // events writes no part and the oracle read fails later with a confusing
    // "no files" — surface the run summary instead.
    let stdout = String::from_utf8_lossy(&res.stdout);
    assert!(
        !stdout.contains("rows:         0"),
        "cdc capture succeeded but captured 0 events — run summary:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&res.stderr)
    );

    // ---- DuckDB oracle: read_parquet over the absolute /work path ----
    // Bind-mount visibility on macOS Docker lags under parallel FS load: the
    // part exists on the host but the container's view can trail by seconds.
    // Wait for the glob to resolve INSIDE the container before querying —
    // solo runs never need this; a full parallel suite does.
    for i in 0..30 {
        let seen = std::process::Command::new("docker")
            .args([
                "exec",
                "rivet-duckdb",
                "sh",
                "-lc",
                &format!("ls {container}/*.parquet >/dev/null 2>&1"),
            ])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if seen {
            break;
        }
        assert!(i < 29, "part never became visible inside rivet-duckdb");
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
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

// M5 — the full-surface cross-oracle, repo-native (the campaign's /tmp shell
// sweep, promoted to a test that nightly runs). One MySQL all-types table
// goes through CDC AND batch; DuckDB and (when alive) ClickHouse each read
// BOTH parquets; every metric must agree across all readings AND equal a
// formula-derived LITERAL — the readers agreeing with each other is not
// enough (they could agree on the same corruption); the literal anchors them.
#[test]
#[ignore = "live+loaders: cdc + loaders profiles (nightly); full-surface via_duckdb + clickhouse"]
fn cdc_full_surface_cross_oracle_matches_literals() {
    let mut c = mysql::Pool::new(MYSQL_CDC_URL)
        .expect("mysql-cdc pool")
        .get_conn()
        .expect("conn");
    let table = "cdc_oracle_full";
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    c.query_drop(format!(
        "CREATE TABLE {table} (
           id INT PRIMARY KEY, amount DECIMAL(18,4), big BIGINT UNSIGNED,
           bits BIT(8), yr YEAR, en ENUM('a','b','c'), st SET('x','y','z'),
           tm TIME(6), dt DATETIME(6), note VARCHAR(60), bin VARBINARY(8))"
    ))
    .unwrap();

    let d = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("ckpt");
    let row: mysql::Row = c.query_first("SHOW MASTER STATUS").unwrap().unwrap();
    let (file, pos): (String, u64) = (row.get(0).unwrap(), row.get(1).unwrap());
    std::fs::write(&ckpt, format!(r#"{{"file":"{file}","pos":{pos}}}"#)).unwrap();

    // Golden rows — every literal below derives from these three lines.
    c.query_drop(format!(
        "INSERT INTO {table} VALUES
           (1, 999999999999.9999, 18446744073709551615, b'10101010', 2024,
            'b', 'x,z', '23:59:59.999999', '2035-08-07 09:08:07.987654',
            'üñíçødé', 0xDEADBEEF),
           (2, 0.0001, 0, b'00000001', 1901, 'a', 'x,y,z',
            '00:00:00', '1970-01-01 00:00:01', '', 0x00),
           (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
    ))
    .unwrap();

    let (host, container) = clickhouse_shared_workdir("cdc_oracle_full");
    let cdc_dir = host.join("cdc");
    let batch_dir = host.join("batch");
    std::fs::create_dir_all(&cdc_dir).unwrap();
    std::fs::create_dir_all(&batch_dir).unwrap();
    let yaml = format!(
        r#"source: {{type: mysql, url: "{MYSQL_CDC_URL}"}}
exports:
  - name: {table}
    table: {table}
    mode: cdc
    format: parquet
    cdc: {{ checkpoint: "{ckpt}", until_current: true, server_id: 8889 }}
    destination: {{ type: local, path: "{o}" }}
  - name: {table}_batch
    query: "SELECT * FROM {table}"
    mode: full
    format: parquet
    destination: {{ type: local, path: "{b}" }}
"#,
        ckpt = ckpt.display(),
        o = cdc_dir.display(),
        b = batch_dir.display(),
    );
    let cfg = write_config(&d, &yaml);
    let res = std::process::Command::new(RIVET_BIN)
        .args(["run", "--config", cfg.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        res.status.success(),
        "cdc+batch runs failed:\n{}",
        String::from_utf8_lossy(&res.stderr)
    );
    let _ = c.query_drop(format!("DROP TABLE IF EXISTS {table}"));

    // metric → (duckdb expr, clickhouse expr, expected literal)
    let metrics: &[(&str, &str, &str, &str)] = &[
        ("rows", "count(*)", "count()", "3"),
        // Σ amount = 999999999999.9999 + 0.0001 = 10^12 exactly.
        (
            "sum_amount",
            "CAST(SUM(amount) AS VARCHAR)",
            "toString(sum(amount))",
            "1000000000000.0000",
        ),
        (
            "big_max_hits",
            "count(*) FILTER (big = 18446744073709551615)",
            "countIf(big = 18446744073709551615)",
            "1",
        ),
        (
            "bits_sum",
            "CAST(SUM(bits) AS VARCHAR)",
            "toString(sum(bits))",
            "171",
        ),
        (
            "yr_sum",
            "CAST(SUM(yr) AS VARCHAR)",
            "toString(sum(yr))",
            "3925",
        ),
        (
            "en_b",
            "count(*) FILTER (en = 'b')",
            "countIf(en = 'b')",
            "1",
        ),
        (
            "st_all",
            "count(*) FILTER (st = 'x,y,z')",
            "countIf(st = 'x,y,z')",
            "1",
        ),
        (
            "tm_max_us",
            "CAST(extract(epoch FROM max(tm))*1000000 AS BIGINT)",
            "toString(toUnixTimestamp64Micro(max(tm)))",
            "86399999999",
        ),
        (
            "dt_max_us",
            "CAST(epoch_us(max(dt)) AS VARCHAR)",
            "toString(toUnixTimestamp64Micro(max(dt)))",
            "2070090487987654",
        ),
        (
            "note_unicode",
            "count(*) FILTER (note = 'üñíçødé')",
            "countIf(note = 'üñíçødé')",
            "1",
        ),
        (
            "bin_len",
            "CAST(SUM(octet_length(bin)) AS VARCHAR)",
            "toString(sum(length(bin)))",
            "5",
        ),
        (
            "nulls_amount",
            "count(*) FILTER (amount IS NULL)",
            "countIf(isNull(amount))",
            "1",
        ),
    ];

    let ch_alive = std::net::TcpStream::connect_timeout(
        &"127.0.0.1:8123".parse().unwrap(),
        std::time::Duration::from_secs(1),
    )
    .is_ok();
    for (name, duck, ch, want) in metrics {
        for (side, dir_host, dir_cont) in [
            ("cdc", &cdc_dir, format!("{container}/cdc")),
            ("batch", &batch_dir, format!("{container}/batch")),
        ] {
            let _ = dir_host;
            let dj = duckdb_run_sql_json(&format!(
                "SELECT {duck} FROM read_parquet('{dir_cont}/*.parquet')"
            ));
            let got = dj["rows"][0][0]
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| dj["rows"][0][0].to_string());
            assert_eq!(
                got.as_str(),
                *want,
                "duckdb/{side}/{name}: expected the literal"
            );
            if ch_alive {
                let rel = dir_cont.trim_start_matches("/work/");
                // The decimal TEXT rendering depends on a session default
                // that flipped between ClickHouse versions
                // (output_format_decimal_trailing_zeros: local image renders
                // "…0000", newer CI image renders bare) — pin it, per the
                // session-state-dependent-rendering rule.
                // toString() on the SQL side: newer ClickHouse emits
                // decimals as JSON NUMBERS (older quoted them as strings), and
                // the number path re-renders through f64, eating the '.0000'
                // scale digits. A SQL-side string is version-stable and keeps
                // the full scale.
                let cj = clickhouse_run_sql_json(&format!(
                    "SELECT toString({ch}) AS v FROM file('{rel}/*.parquet', Parquet) \
                     SETTINGS output_format_decimal_trailing_zeros = 1"
                ));
                let got = cj[0]["v"]
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| cj[0]["v"].to_string());
                assert_eq!(
                    got.as_str(),
                    *want,
                    "clickhouse/{side}/{name}: expected the literal"
                );
            }
        }
    }
}
