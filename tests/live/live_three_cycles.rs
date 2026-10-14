//! Three cycles of ONE config on ONE state (docs/cycle-matrix.yaml): the first takes the whole
//! source, the second meets no change and delivers and loads nothing, the third takes exactly the
//! delta. One body for the batch pipelines of every SQL engine, one for MongoDB's whole-table
//! export, one for CDC on every engine, each into every warehouse `rivet load` writes and into the
//! local Parquet destination. Between cycles the warehouse is asked for metadata only; its values
//! are read once, after the last cycle, through a client the loader does not use.

use crate::common::*;

/// Rows at the source before the first cycle.
const FIRST: i64 = 20;
/// The rows the third cycle adds.
const DELTA: std::ops::RangeInclusive<i64> = 21..=25;

/// A batch pipeline of the cycle; each has its own idea of a cycle that meets no change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Batch {
    Full,
    Incremental,
    KeysetIncremental,
}

impl Batch {
    /// `rig` staged for this pipeline on `e`.
    fn stage(self, e: SqlEngine, rig: Rig) -> Rig {
        match self {
            Batch::Full => rig,
            Batch::Incremental => e.staged(rig, "incremental", &["cursor_column: id"]),
            Batch::KeysetIncremental => e.staged(
                rig,
                "chunked",
                &[
                    "chunk_by_key: id",
                    "chunk_size: 7",
                    "chunk_checkpoint: true",
                    "keyset_incremental: true",
                ],
            ),
        }
    }

    /// Whether a cycle over an unchanged source still delivers the whole table (a full pass keeps no cursor).
    fn whole(self) -> bool {
        matches!(self, Batch::Full)
    }
}

/// What a load with nothing new says.
fn loaded_nothing(said: &str) -> bool {
    said.contains("up to date") || said.contains("nothing to load")
}

/// The newest ledger row of `table` must be a success of `rows` rows.
fn last_load_is(cfg: &std::path::Path, table: &str, rows: i64, step: &str) {
    let loads = ledger_loads(cfg, table);
    assert_eq!(
        loads.last(),
        Some(&("success".to_string(), rows)),
        "{step}: the load ledger must end in a {rows}-row success; it holds {loads:?}"
    );
}

/// The `(id, ext_id)` pairs the shared fixture holds for `ids`.
fn fixture_pairs(ids: std::ops::RangeInclusive<i64>) -> Vec<(i64, i64)> {
    ids.map(|i| (i, i * 10)).collect()
}

/// One batch export of `e`, three cycles into `kind`.
fn three_batch_cycles(e: SqlEngine, batch: Batch, kind: WarehouseKind) {
    e.alive();
    let Some(mut w) = Warehouse::of(kind, "cyc") else {
        return;
    };
    let (table, _guard) = e.table("cyc");
    w.adopt(&table);
    e.insert(&table, 1..=FIRST, 10, Some(1));
    // The fixture's columns as the engine's catalog holds them (Oracle folds bare names up).
    let (key, value) = match e.folds_upper() {
        true => ("ID", "EXT_ID"),
        false => ("id", "ext_id"),
    };
    let rig = w.shape(batch.stage(e, e.rig(&table)), key);
    let cfg = rig.config_path();
    let export = rig.export_name().to_string();

    // Cycle 1: the whole source.
    rig.run_ok();
    let said = w.load(&rig);
    if w.loads() {
        assert!(said.contains("LOAD OK"), "cycle 1 must load:\n{said}");
        last_load_is(&cfg, &table, FIRST, "cycle 1");
    }
    let (runs1, cursor1) = run_statuses_and_cursor(&cfg, &export);
    let mark1 = w.fingerprint(&rig, &table);

    // Cycle 2: nothing changed at the source.
    let said_run = rig.run_ok_capture();
    let (runs2, cursor2) = run_statuses_and_cursor(&cfg, &export);
    assert_eq!(runs2.len(), runs1.len() + 1, "cycle 2 is a run of its own");
    assert!(runs2.iter().all(|s| s == "success"), "{runs2:?}");
    let said = w.load(&rig);
    if batch.whole() {
        if w.loads() {
            assert!(
                said.contains("LOAD OK"),
                "a whole-table cycle lands again:\n{said}"
            );
            last_load_is(&cfg, &table, FIRST, "cycle 2 (whole table)");
        }
    } else {
        if batch == Batch::Incremental {
            assert!(
                said_run.contains("0 rows") && said_run.contains("0 files"),
                "an idle incremental run reports no row and no file:\n{said_run}"
            );
        }
        assert_eq!(cursor2, cursor1, "an idle cycle moves no cursor");
        assert_eq!(
            w.fingerprint(&rig, &table),
            mark1,
            "an idle cycle writes nothing to the warehouse"
        );
        if w.loads() {
            assert!(loaded_nothing(&said), "cycle 2 must load nothing:\n{said}");
            last_load_is(&cfg, &table, 0, "cycle 2 (idle, consumed as a 0-row load)");
        }
    }

    // Cycle 3: five new rows, and only those travel.
    e.insert(&table, DELTA, 5, Some(2));
    rig.run_ok();
    let said = w.load(&rig);
    if w.loads() {
        assert!(said.contains("LOAD OK"), "cycle 3 must load:\n{said}");
        let rows = if batch.whole() { FIRST + 5 } else { 5 };
        last_load_is(&cfg, &table, rows, "cycle 3");
    }
    let (_, cursor3) = run_statuses_and_cursor(&cfg, &export);
    if !batch.whole() {
        assert_ne!(cursor3, cursor1, "the delta moves the cursor");
    }

    // Read once, after everything was written.
    assert_eq!(
        w.pairs(&rig, &table, key, value, false, batch.whole()),
        fixture_pairs(1..=FIRST + 5),
        "{batch:?} into {kind:?}: the warehouse must hold the source"
    );
    assert_eq!(
        w.log_rows(&rig, &table, key, batch.whole()),
        (FIRST + 5, FIRST + 5),
        "no row travels twice: {} rows would be a re-read of the first cycle",
        2 * FIRST + 5
    );
}

/// MongoDB's whole-table export, three cycles into `kind`.
fn three_mongo_full_cycles(kind: WarehouseKind) {
    require_alive(LiveService::Mongo);
    let Some(mut w) = Warehouse::of(kind, "cyc_mongo") else {
        return;
    };
    let db = unique_name("cyc");
    let coll = unique_name("cyc");
    let _guard = MongoDbGuard {
        port: 27017,
        db: db.clone(),
    };
    let m = MongoTest::connect(27017, &db);
    m.seed_int_id(&coll, FIRST);
    w.adopt(&coll);
    let rig = w.shape(
        Rig::mongo_batch(&coll).source_url(&MongoTest::url(27017, &db)),
        "_id",
    );
    let cfg = rig.config_path();
    let keys = |ids: std::ops::RangeInclusive<i64>| ids.map(|i| (i, i)).collect::<Vec<_>>();

    for (cycle, rows) in [(1, FIRST), (2, FIRST), (3, FIRST + 5)] {
        if cycle == 3 {
            let docs = DELTA
                .map(|i| mongodb::bson::doc! { "_id": i, "v": format!("row{i}") })
                .collect();
            m.insert_many(&coll, docs);
        }
        rig.run_ok();
        let said = w.load(&rig);
        if w.loads() {
            assert!(said.contains("LOAD OK"), "cycle {cycle} must load:\n{said}");
            last_load_is(
                &cfg,
                &coll,
                rows,
                &format!("cycle {cycle} (whole collection)"),
            );
        }
    }
    assert_eq!(
        w.pairs(&rig, &coll, "_id", "_id", false, true),
        keys(1..=FIRST + 5),
        "full into {kind:?}: the warehouse must hold every document once"
    );
}

/// `cols` of every SQL CDC scenario's table.
const CDC_COLS: &str = "id BIGINT PRIMARY KEY, v BIGINT";

/// `scn` with ids `1..=FIRST` inserted before its first run anchors (the engines whose anchor is the first run's).
fn seeded(mut scn: CdcScenario) -> CdcScenario {
    for id in 1..=FIRST {
        scn.insert(id);
    }
    scn
}

/// A CDC stream of one engine, three cycles into `kind`; `scenario` builds the engine's scenario shaped for the warehouse, with `1..=FIRST` at the source before anything is anchored.
fn three_cdc_cycles(
    kind: WarehouseKind,
    scenario: impl FnOnce(&dyn Fn(Rig, &str) -> Rig) -> CdcScenario,
) {
    let Some(mut w) = Warehouse::of(kind, "cyc_cdc") else {
        return;
    };
    let shape = |rig: Rig, pk: &str| w.shape(rig.cdc("initial: snapshot"), pk);
    let mut scn = scenario(&shape);
    let table = scn.table.clone();
    w.adopt(&table);
    let key = scn.pk();
    scn.settle();
    let cfg = scn.rig.config_path();

    // Cycle 1: the anchor, and the snapshot of what is there.
    scn.rig.run_ok();
    let said = w.load(&scn.rig);
    if w.loads() {
        assert!(said.contains("LOAD OK"), "cycle 1 must load:\n{said}");
        last_load_is(&cfg, &table, FIRST, "cycle 1 (snapshot)");
    }
    let mark1 = w.fingerprint(&scn.rig, &table);

    // Cycle 2: nothing changed at the source.
    scn.rig.run_ok();
    let said = w.load(&scn.rig);
    assert_eq!(
        w.fingerprint(&scn.rig, &table),
        mark1,
        "an idle cycle writes nothing to the warehouse"
    );
    if w.loads() {
        assert!(loaded_nothing(&said), "cycle 2 must load nothing:\n{said}");
        last_load_is(&cfg, &table, 0, "cycle 2 (idle, consumed as a 0-row load)");
    }

    // Cycle 3: five inserts, one update, one delete; exactly those travel.
    for id in DELTA {
        scn.insert(id);
    }
    scn.update(3);
    scn.delete(4);
    scn.settle();
    scn.rig.run_ok();
    let said = w.load(&scn.rig);
    if w.loads() {
        assert!(said.contains("LOAD OK"), "cycle 3 must load:\n{said}");
        last_load_is(&cfg, &table, 7, "cycle 3 (the delta)");
    }

    // Read once, after everything was written.
    let source = scn.id_v_pairs();
    assert_eq!(source.len() as i64, FIRST + 4, "the source after the delta");
    if w.loads() {
        let live = w.pairs(&scn.rig, &table, key, key, true, false);
        assert_eq!(
            live.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
            source.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
            "the live keys must be the source's: the deleted key gone, the new ones there"
        );
        if key == "id" {
            assert_eq!(
                w.pairs(&scn.rig, &table, "id", "v", true, false),
                source,
                "the live values must be the source's: the update must have landed"
            );
        }
        assert_eq!(
            w.log_rows(&scn.rig, &table, key, false),
            (FIRST + 7, FIRST + 5),
            "the log holds the snapshot and the seven changes, nothing twice"
        );
    } else {
        let stream = w.log_rows(&scn.rig, &table, key, false);
        assert_eq!(
            stream,
            (7, 7),
            "the stream's parts hold the seven changes once"
        );
        let snapshot =
            Warehouse::declared_pairs(&scn.rig.out_dir().join("snapshot"), key, key, false);
        assert_eq!(
            snapshot.len() as i64,
            FIRST,
            "the snapshot leg holds the rows that were there, and the idle cycle added none"
        );
    }
}

/// Until ADR-0037 lands its loader, `load:` over an Oracle CDC stream is refused when the config is read: by code, on every attempt, before a run anchors anything, with the state and the warehouse untouched. The cell becomes the three cycles the moment the refusal goes.
fn oracle_cdc_into_a_warehouse_is_refused(kind: WarehouseKind) {
    let Some(mut w) = Warehouse::of(kind, "cyc_cdc") else {
        return;
    };
    let shape = |rig: Rig, pk: &str| w.shape(rig.cdc("initial: snapshot"), pk);
    let scn = seeded(CdcScenario::oracle_with("cyc_cdc", |r, _| shape(r, "id")));
    let table = scn.table.clone();
    w.adopt(&table);
    let cfg = scn.rig.config_path();
    let before = w.fingerprint(&scn.rig, &table);
    for attempt in 1..=2 {
        let out = scn.rig.run_args(&[]);
        assert_refused(
            &out,
            Refused::by_code("RIVET_CONFIG_SOURCE_MODE_UNSUPPORTED", 1),
        );
        let said = String::from_utf8_lossy(&out.stderr);
        assert!(
            said.contains("loading an Oracle CDC stream is not supported yet")
                && said.contains("ADR-0037"),
            "attempt {attempt}: the refusal names the preview and its ADR:\n{said}"
        );
    }
    assert!(
        files_with_extension(&scn.rig.out_dir(), "parquet").is_empty(),
        "a config refused at load writes no part"
    );
    match state_url_under_test() {
        Some(_) => assert!(
            run_statuses_and_cursor(&cfg, scn.rig.export_name())
                .0
                .is_empty(),
            "a config refused at load records no run"
        ),
        None => assert!(
            !cfg.with_file_name(".rivet_state.db").exists(),
            "a config refused at load opens no state"
        ),
    }
    assert_eq!(
        w.fingerprint(&scn.rig, &table),
        before,
        "a config refused at load touches no warehouse object"
    );
}

// ── the cells, one per ledger row and engine (docs/cycle-matrix.yaml) ──────────

#[test]
#[ignore = "live: requires docker compose postgres + the duckdb validator"]
fn full_into_duckdb_three_cycles_postgres() {
    three_batch_cycles(SqlEngine::Pg, Batch::Full, WarehouseKind::DuckDb);
}

#[test]
#[ignore = "live: requires docker compose mysql + the duckdb validator"]
fn full_into_duckdb_three_cycles_mysql() {
    three_batch_cycles(SqlEngine::Mysql, Batch::Full, WarehouseKind::DuckDb);
}

#[test]
#[ignore = "live: requires docker compose mssql + the duckdb validator"]
fn full_into_duckdb_three_cycles_mssql() {
    three_batch_cycles(SqlEngine::Mssql, Batch::Full, WarehouseKind::DuckDb);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires the oracle service + the duckdb validator"]
fn full_into_duckdb_three_cycles_oracle() {
    three_batch_cycles(SqlEngine::Oracle, Batch::Full, WarehouseKind::DuckDb);
}

#[test]
#[ignore = "live: requires docker compose up -d mongo + the duckdb validator"]
fn full_into_duckdb_three_cycles_mongo() {
    three_mongo_full_cycles(WarehouseKind::DuckDb);
}

#[test]
#[ignore = "live: requires docker compose postgres + clickhouse + fake-gcs"]
fn full_into_clickhouse_three_cycles_postgres() {
    three_batch_cycles(SqlEngine::Pg, Batch::Full, WarehouseKind::ClickHouse);
}

#[test]
#[ignore = "live: requires docker compose mysql + clickhouse + fake-gcs"]
fn full_into_clickhouse_three_cycles_mysql() {
    three_batch_cycles(SqlEngine::Mysql, Batch::Full, WarehouseKind::ClickHouse);
}

#[test]
#[ignore = "live: requires docker compose mssql + clickhouse + fake-gcs"]
fn full_into_clickhouse_three_cycles_mssql() {
    three_batch_cycles(SqlEngine::Mssql, Batch::Full, WarehouseKind::ClickHouse);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires the oracle service + clickhouse + fake-gcs"]
fn full_into_clickhouse_three_cycles_oracle() {
    three_batch_cycles(SqlEngine::Oracle, Batch::Full, WarehouseKind::ClickHouse);
}

#[test]
#[ignore = "live: requires docker compose up -d mongo + clickhouse + fake-gcs"]
fn full_into_clickhouse_three_cycles_mongo() {
    three_mongo_full_cycles(WarehouseKind::ClickHouse);
}

#[test]
#[ignore = "live: requires docker compose postgres + BigQuery creds"]
fn full_into_bigquery_three_cycles_postgres() {
    three_batch_cycles(SqlEngine::Pg, Batch::Full, WarehouseKind::BigQuery);
}

#[test]
#[ignore = "live: requires docker compose mysql + BigQuery creds"]
fn full_into_bigquery_three_cycles_mysql() {
    three_batch_cycles(SqlEngine::Mysql, Batch::Full, WarehouseKind::BigQuery);
}

#[test]
#[ignore = "live: requires docker compose mssql + BigQuery creds"]
fn full_into_bigquery_three_cycles_mssql() {
    three_batch_cycles(SqlEngine::Mssql, Batch::Full, WarehouseKind::BigQuery);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires the oracle service + BigQuery creds"]
fn full_into_bigquery_three_cycles_oracle() {
    three_batch_cycles(SqlEngine::Oracle, Batch::Full, WarehouseKind::BigQuery);
}

#[test]
#[ignore = "live: requires docker compose up -d mongo + BigQuery creds"]
fn full_into_bigquery_three_cycles_mongo() {
    three_mongo_full_cycles(WarehouseKind::BigQuery);
}

#[test]
#[ignore = "live: requires docker compose postgres + Snowflake creds"]
fn full_into_snowflake_three_cycles_postgres() {
    three_batch_cycles(SqlEngine::Pg, Batch::Full, WarehouseKind::Snowflake);
}

#[test]
#[ignore = "live: requires docker compose mysql + Snowflake creds"]
fn full_into_snowflake_three_cycles_mysql() {
    three_batch_cycles(SqlEngine::Mysql, Batch::Full, WarehouseKind::Snowflake);
}

#[test]
#[ignore = "live: requires docker compose mssql + Snowflake creds"]
fn full_into_snowflake_three_cycles_mssql() {
    three_batch_cycles(SqlEngine::Mssql, Batch::Full, WarehouseKind::Snowflake);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires the oracle service + Snowflake creds"]
fn full_into_snowflake_three_cycles_oracle() {
    three_batch_cycles(SqlEngine::Oracle, Batch::Full, WarehouseKind::Snowflake);
}

#[test]
#[ignore = "live: requires docker compose up -d mongo + Snowflake creds"]
fn full_into_snowflake_three_cycles_mongo() {
    three_mongo_full_cycles(WarehouseKind::Snowflake);
}

#[test]
#[ignore = "live: requires docker compose postgres + the duckdb validator"]
fn incremental_into_duckdb_three_cycles_postgres() {
    three_batch_cycles(SqlEngine::Pg, Batch::Incremental, WarehouseKind::DuckDb);
}

#[test]
#[ignore = "live: requires docker compose mysql + the duckdb validator"]
fn incremental_into_duckdb_three_cycles_mysql() {
    three_batch_cycles(SqlEngine::Mysql, Batch::Incremental, WarehouseKind::DuckDb);
}

#[test]
#[ignore = "live: requires docker compose mssql + the duckdb validator"]
fn incremental_into_duckdb_three_cycles_mssql() {
    three_batch_cycles(SqlEngine::Mssql, Batch::Incremental, WarehouseKind::DuckDb);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires the oracle service + the duckdb validator"]
fn incremental_into_duckdb_three_cycles_oracle() {
    three_batch_cycles(SqlEngine::Oracle, Batch::Incremental, WarehouseKind::DuckDb);
}

#[test]
#[ignore = "live: requires docker compose postgres + clickhouse + fake-gcs"]
fn incremental_into_clickhouse_three_cycles_postgres() {
    three_batch_cycles(SqlEngine::Pg, Batch::Incremental, WarehouseKind::ClickHouse);
}

#[test]
#[ignore = "live: requires docker compose mysql + clickhouse + fake-gcs"]
fn incremental_into_clickhouse_three_cycles_mysql() {
    three_batch_cycles(
        SqlEngine::Mysql,
        Batch::Incremental,
        WarehouseKind::ClickHouse,
    );
}

#[test]
#[ignore = "live: requires docker compose mssql + clickhouse + fake-gcs"]
fn incremental_into_clickhouse_three_cycles_mssql() {
    three_batch_cycles(
        SqlEngine::Mssql,
        Batch::Incremental,
        WarehouseKind::ClickHouse,
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires the oracle service + clickhouse + fake-gcs"]
fn incremental_into_clickhouse_three_cycles_oracle() {
    three_batch_cycles(
        SqlEngine::Oracle,
        Batch::Incremental,
        WarehouseKind::ClickHouse,
    );
}

#[test]
#[ignore = "live: requires docker compose postgres + BigQuery creds"]
fn incremental_into_bigquery_three_cycles_postgres() {
    three_batch_cycles(SqlEngine::Pg, Batch::Incremental, WarehouseKind::BigQuery);
}

#[test]
#[ignore = "live: requires docker compose mysql + BigQuery creds"]
fn incremental_into_bigquery_three_cycles_mysql() {
    three_batch_cycles(
        SqlEngine::Mysql,
        Batch::Incremental,
        WarehouseKind::BigQuery,
    );
}

#[test]
#[ignore = "live: requires docker compose mssql + BigQuery creds"]
fn incremental_into_bigquery_three_cycles_mssql() {
    three_batch_cycles(
        SqlEngine::Mssql,
        Batch::Incremental,
        WarehouseKind::BigQuery,
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires the oracle service + BigQuery creds"]
fn incremental_into_bigquery_three_cycles_oracle() {
    three_batch_cycles(
        SqlEngine::Oracle,
        Batch::Incremental,
        WarehouseKind::BigQuery,
    );
}

#[test]
#[ignore = "live: requires docker compose postgres + Snowflake creds"]
fn incremental_into_snowflake_three_cycles_postgres() {
    three_batch_cycles(SqlEngine::Pg, Batch::Incremental, WarehouseKind::Snowflake);
}

#[test]
#[ignore = "live: requires docker compose mysql + Snowflake creds"]
fn incremental_into_snowflake_three_cycles_mysql() {
    three_batch_cycles(
        SqlEngine::Mysql,
        Batch::Incremental,
        WarehouseKind::Snowflake,
    );
}

#[test]
#[ignore = "live: requires docker compose mssql + Snowflake creds"]
fn incremental_into_snowflake_three_cycles_mssql() {
    three_batch_cycles(
        SqlEngine::Mssql,
        Batch::Incremental,
        WarehouseKind::Snowflake,
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires the oracle service + Snowflake creds"]
fn incremental_into_snowflake_three_cycles_oracle() {
    three_batch_cycles(
        SqlEngine::Oracle,
        Batch::Incremental,
        WarehouseKind::Snowflake,
    );
}

#[test]
#[ignore = "live: requires docker compose postgres + the duckdb validator"]
fn keyset_incremental_into_duckdb_three_cycles_postgres() {
    three_batch_cycles(
        SqlEngine::Pg,
        Batch::KeysetIncremental,
        WarehouseKind::DuckDb,
    );
}

#[test]
#[ignore = "live: requires docker compose mysql + the duckdb validator"]
fn keyset_incremental_into_duckdb_three_cycles_mysql() {
    three_batch_cycles(
        SqlEngine::Mysql,
        Batch::KeysetIncremental,
        WarehouseKind::DuckDb,
    );
}

#[test]
#[ignore = "live: requires docker compose mssql + the duckdb validator"]
fn keyset_incremental_into_duckdb_three_cycles_mssql() {
    three_batch_cycles(
        SqlEngine::Mssql,
        Batch::KeysetIncremental,
        WarehouseKind::DuckDb,
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires the oracle service + the duckdb validator"]
fn keyset_incremental_into_duckdb_three_cycles_oracle() {
    three_batch_cycles(
        SqlEngine::Oracle,
        Batch::KeysetIncremental,
        WarehouseKind::DuckDb,
    );
}

#[test]
#[ignore = "live: requires docker compose postgres + clickhouse + fake-gcs"]
fn keyset_incremental_into_clickhouse_three_cycles_postgres() {
    three_batch_cycles(
        SqlEngine::Pg,
        Batch::KeysetIncremental,
        WarehouseKind::ClickHouse,
    );
}

#[test]
#[ignore = "live: requires docker compose mysql + clickhouse + fake-gcs"]
fn keyset_incremental_into_clickhouse_three_cycles_mysql() {
    three_batch_cycles(
        SqlEngine::Mysql,
        Batch::KeysetIncremental,
        WarehouseKind::ClickHouse,
    );
}

#[test]
#[ignore = "live: requires docker compose mssql + clickhouse + fake-gcs"]
fn keyset_incremental_into_clickhouse_three_cycles_mssql() {
    three_batch_cycles(
        SqlEngine::Mssql,
        Batch::KeysetIncremental,
        WarehouseKind::ClickHouse,
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires the oracle service + clickhouse + fake-gcs"]
fn keyset_incremental_into_clickhouse_three_cycles_oracle() {
    three_batch_cycles(
        SqlEngine::Oracle,
        Batch::KeysetIncremental,
        WarehouseKind::ClickHouse,
    );
}

#[test]
#[ignore = "live: requires docker compose postgres + BigQuery creds"]
fn keyset_incremental_into_bigquery_three_cycles_postgres() {
    three_batch_cycles(
        SqlEngine::Pg,
        Batch::KeysetIncremental,
        WarehouseKind::BigQuery,
    );
}

#[test]
#[ignore = "live: requires docker compose mysql + BigQuery creds"]
fn keyset_incremental_into_bigquery_three_cycles_mysql() {
    three_batch_cycles(
        SqlEngine::Mysql,
        Batch::KeysetIncremental,
        WarehouseKind::BigQuery,
    );
}

#[test]
#[ignore = "live: requires docker compose mssql + BigQuery creds"]
fn keyset_incremental_into_bigquery_three_cycles_mssql() {
    three_batch_cycles(
        SqlEngine::Mssql,
        Batch::KeysetIncremental,
        WarehouseKind::BigQuery,
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires the oracle service + BigQuery creds"]
fn keyset_incremental_into_bigquery_three_cycles_oracle() {
    three_batch_cycles(
        SqlEngine::Oracle,
        Batch::KeysetIncremental,
        WarehouseKind::BigQuery,
    );
}

#[test]
#[ignore = "live: requires docker compose postgres + Snowflake creds"]
fn keyset_incremental_into_snowflake_three_cycles_postgres() {
    three_batch_cycles(
        SqlEngine::Pg,
        Batch::KeysetIncremental,
        WarehouseKind::Snowflake,
    );
}

#[test]
#[ignore = "live: requires docker compose mysql + Snowflake creds"]
fn keyset_incremental_into_snowflake_three_cycles_mysql() {
    three_batch_cycles(
        SqlEngine::Mysql,
        Batch::KeysetIncremental,
        WarehouseKind::Snowflake,
    );
}

#[test]
#[ignore = "live: requires docker compose mssql + Snowflake creds"]
fn keyset_incremental_into_snowflake_three_cycles_mssql() {
    three_batch_cycles(
        SqlEngine::Mssql,
        Batch::KeysetIncremental,
        WarehouseKind::Snowflake,
    );
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires the oracle service + Snowflake creds"]
fn keyset_incremental_into_snowflake_three_cycles_oracle() {
    three_batch_cycles(
        SqlEngine::Oracle,
        Batch::KeysetIncremental,
        WarehouseKind::Snowflake,
    );
}

#[test]
#[ignore = "live: requires postgres-cdc (wal_level=logical) + the duckdb validator"]
fn cdc_into_duckdb_three_cycles_postgres() {
    three_cdc_cycles(WarehouseKind::DuckDb, |shape| {
        CdcScenario::pg_seeded_with("cyc_cdc", CDC_COLS, 1..=FIRST, |r, _| shape(r, "id"))
    });
}

#[test]
#[ignore = "live: requires mysql-cdc + the duckdb validator"]
fn cdc_into_duckdb_three_cycles_mysql() {
    three_cdc_cycles(WarehouseKind::DuckDb, |shape| {
        seeded(CdcScenario::mysql_with("cyc_cdc", CDC_COLS, |r, _| {
            shape(r, "id")
        }))
    });
}

#[test]
#[ignore = "live: requires mssql-cdc (SQL Server Agent) + the duckdb validator"]
fn cdc_into_duckdb_three_cycles_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    three_cdc_cycles(WarehouseKind::DuckDb, |shape| {
        seeded(CdcScenario::mssql_with("cyc_cdc", CDC_COLS, |r, _| {
            shape(r, "id")
        }))
    });
}

#[test]
#[ignore = "live: requires mongo-rs + the duckdb validator"]
fn cdc_into_duckdb_three_cycles_mongo() {
    three_cdc_cycles(WarehouseKind::DuckDb, |shape| {
        seeded(CdcScenario::mongo_collection_with(
            "cyc_cdc",
            &unique_name("cyc_cdc"),
            |r, _| shape(r, "_id"),
        ))
    });
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires the oracle service (LogMiner) + the duckdb validator"]
fn cdc_into_duckdb_three_cycles_oracle() {
    let _serial = cross_process_serial("oracle_cdc");
    three_cdc_cycles(WarehouseKind::DuckDb, |shape| {
        seeded(CdcScenario::oracle_with("cyc_cdc", |r, _| shape(r, "id")))
    });
}

#[test]
#[ignore = "live: requires postgres-cdc (wal_level=logical) + clickhouse + fake-gcs"]
fn cdc_into_clickhouse_three_cycles_postgres() {
    three_cdc_cycles(WarehouseKind::ClickHouse, |shape| {
        CdcScenario::pg_seeded_with("cyc_cdc", CDC_COLS, 1..=FIRST, |r, _| shape(r, "id"))
    });
}

#[test]
#[ignore = "live: requires mysql-cdc + clickhouse + fake-gcs"]
fn cdc_into_clickhouse_three_cycles_mysql() {
    three_cdc_cycles(WarehouseKind::ClickHouse, |shape| {
        seeded(CdcScenario::mysql_with("cyc_cdc", CDC_COLS, |r, _| {
            shape(r, "id")
        }))
    });
}

#[test]
#[ignore = "live: requires mssql-cdc (SQL Server Agent) + clickhouse + fake-gcs"]
fn cdc_into_clickhouse_three_cycles_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    three_cdc_cycles(WarehouseKind::ClickHouse, |shape| {
        seeded(CdcScenario::mssql_with("cyc_cdc", CDC_COLS, |r, _| {
            shape(r, "id")
        }))
    });
}

#[test]
#[ignore = "live: requires postgres-cdc (wal_level=logical) + BigQuery creds"]
fn cdc_into_bigquery_three_cycles_postgres() {
    three_cdc_cycles(WarehouseKind::BigQuery, |shape| {
        CdcScenario::pg_seeded_with("cyc_cdc", CDC_COLS, 1..=FIRST, |r, _| shape(r, "id"))
    });
}

#[test]
#[ignore = "live: requires mysql-cdc + BigQuery creds"]
fn cdc_into_bigquery_three_cycles_mysql() {
    three_cdc_cycles(WarehouseKind::BigQuery, |shape| {
        seeded(CdcScenario::mysql_with("cyc_cdc", CDC_COLS, |r, _| {
            shape(r, "id")
        }))
    });
}

#[test]
#[ignore = "live: requires mssql-cdc (SQL Server Agent) + BigQuery creds"]
fn cdc_into_bigquery_three_cycles_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    three_cdc_cycles(WarehouseKind::BigQuery, |shape| {
        seeded(CdcScenario::mssql_with("cyc_cdc", CDC_COLS, |r, _| {
            shape(r, "id")
        }))
    });
}

#[test]
#[ignore = "live: requires mongo-rs + BigQuery creds"]
fn cdc_into_bigquery_three_cycles_mongo() {
    three_cdc_cycles(WarehouseKind::BigQuery, |shape| {
        seeded(CdcScenario::mongo_collection_with(
            "cyc_cdc",
            &unique_name("cyc_cdc"),
            |r, _| shape(r, "_id"),
        ))
    });
}

#[test]
#[ignore = "live: requires postgres-cdc (wal_level=logical) + Snowflake creds"]
fn cdc_into_snowflake_three_cycles_postgres() {
    three_cdc_cycles(WarehouseKind::Snowflake, |shape| {
        CdcScenario::pg_seeded_with("cyc_cdc", CDC_COLS, 1..=FIRST, |r, _| shape(r, "id"))
    });
}

#[test]
#[ignore = "live: requires mysql-cdc + Snowflake creds"]
fn cdc_into_snowflake_three_cycles_mysql() {
    three_cdc_cycles(WarehouseKind::Snowflake, |shape| {
        seeded(CdcScenario::mysql_with("cyc_cdc", CDC_COLS, |r, _| {
            shape(r, "id")
        }))
    });
}

#[test]
#[ignore = "live: requires mssql-cdc (SQL Server Agent) + Snowflake creds"]
fn cdc_into_snowflake_three_cycles_mssql() {
    let _serial = cross_process_serial("mssql_cdc");
    three_cdc_cycles(WarehouseKind::Snowflake, |shape| {
        seeded(CdcScenario::mssql_with("cyc_cdc", CDC_COLS, |r, _| {
            shape(r, "id")
        }))
    });
}

#[test]
#[ignore = "live: requires mongo-rs + Snowflake creds"]
fn cdc_into_snowflake_three_cycles_mongo() {
    three_cdc_cycles(WarehouseKind::Snowflake, |shape| {
        seeded(CdcScenario::mongo_collection_with(
            "cyc_cdc",
            &unique_name("cyc_cdc"),
            |r, _| shape(r, "_id"),
        ))
    });
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires the oracle service (LogMiner) + clickhouse + fake-gcs"]
fn cdc_into_clickhouse_three_cycles_oracle() {
    oracle_cdc_into_a_warehouse_is_refused(WarehouseKind::ClickHouse);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires the oracle service (LogMiner) + BigQuery creds"]
fn cdc_into_bigquery_three_cycles_oracle() {
    oracle_cdc_into_a_warehouse_is_refused(WarehouseKind::BigQuery);
}

#[cfg(feature = "oracle")]
#[test]
#[ignore = "live: requires the oracle service (LogMiner) + Snowflake creds"]
fn cdc_into_snowflake_three_cycles_oracle() {
    oracle_cdc_into_a_warehouse_is_refused(WarehouseKind::Snowflake);
}
