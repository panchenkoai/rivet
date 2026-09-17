//! The partner shape, end to end: ONE stream over N tables (`tables:`), a keyset
//! RECIPE per table, one `load:` — the config `rivet init --mode cdc` now
//! scaffolds. Anchor + backfill of every table → load → a delta in every table →
//! load (exactly the delta, per table) → a crash ON THE CDC LEG → plain run →
//! load → an idle run and load that append nothing.
//!
//! Two oracles, neither sharing code with rivet's write path: the warehouse
//! through the `bq` CLI (one `<table>` view + `<table>__changes` log PER table —
//! the multiplex fan-out is what this proves), the source through a re-query.
//! The recipes page by 2 rows over 5, so the baseline crosses the keyset
//! pagination threshold rather than fitting one page.
//!
//! Needs the CDC stand and the warehouse env (`BIGQUERY_TEST_PROJECT`,
//! `RIVET_TEST_GCS_BUCKET`, optional `RIVET_TEST_BQ_DATASET`); SKIPS without it.

use crate::common::*;

/// A source the cycle can write to and count — one impl per engine.
trait Source {
    fn exec(&mut self, sql: &str);
    fn count(&mut self, table: &str) -> i64;
}

impl Source for mysql::PooledConn {
    fn exec(&mut self, sql: &str) {
        use mysql::prelude::Queryable as _;
        self.query_drop(sql).expect(sql);
    }
    fn count(&mut self, table: &str) -> i64 {
        use mysql::prelude::Queryable as _;
        self.query_first(format!("SELECT COUNT(*) FROM {table}"))
            .expect("count")
            .expect("one row")
    }
}

impl Source for postgres::Client {
    fn exec(&mut self, sql: &str) {
        self.batch_execute(sql).expect(sql);
    }
    fn count(&mut self, table: &str) -> i64 {
        self.query_one(&format!("SELECT COUNT(*) FROM {table}"), &[])
            .expect("count")
            .get(0)
    }
}

fn load_ok(rig: &Rig) {
    let out = rig.cli(&["load"]);
    assert!(
        out.status.success(),
        "rivet load failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn count(bq: &BqLive, table: &str) -> i64 {
    bq.read_bq_count(table).parse().expect("a count")
}

/// Rows the warehouse has ACCUMULATED for one table: its changelog once it
/// exists, the plain baseline table before the first delta turns it into one.
fn accumulated(bq: &BqLive, table: &str) -> i64 {
    let changes = format!("{table}__changes");
    if bq.read_bq_table_type(&changes).is_some() {
        count(bq, &changes)
    } else {
        count(bq, table)
    }
}

/// The LIVE state of one table equals its source, row for row on `id`
/// (`WHERE NOT __is_deleted` once the object is the dedup view).
fn assert_table_is_source(bq: &BqLive, table: &str, src: &mut dyn Source, step: &str) {
    let source = src.count(table);
    let live = if bq
        .read_bq_table_type(&format!("{table}__changes"))
        .is_some()
    {
        "WHERE NOT __is_deleted"
    } else {
        ""
    };
    let row = &bq.read_bq_rows(&format!(
        "SELECT COUNT(*) AS n, COUNT(DISTINCT id) AS d FROM `{}.{}.{table}` {live}",
        bq.project, bq.dataset
    ))[0];
    let n: i64 = row["n"].as_str().expect("count").parse().expect("a count");
    let d: i64 = row["d"].as_str().expect("count").parse().expect("a count");
    assert_eq!(
        n, source,
        "{step}: {table}: the live state must equal the source"
    );
    assert_eq!(d, n, "{step}: {table}: one row per key");
}

/// The init shape: `tables: [..]` + `backfill: auto`, a keyset recipe per table
/// paging by 2, a live GCS destination and a `load:` keyed on `id`.
fn shaped(rig: Rig, tables: &[String], bq: &BqLive) -> Rig {
    let refs: Vec<&str> = tables.iter().map(String::as_str).collect();
    let mut rig = rig
        .tables(&refs)
        .cdc("backfill: auto")
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(", pk: [id]"));
    for t in tables {
        rig = rig
            .also_batch_export(t, t, "chunked")
            .also_export_line("chunk_by_key: id")
            .also_export_line("chunk_size: 2")
            .also_export_line("chunk_checkpoint: true");
    }
    rig
}

/// The scenario itself — one body, every engine that can express `tables:`.
fn cycle(rig: Rig, tables: Vec<String>, bq: BqLive, src: &mut dyn Source) {
    let changes: Vec<String> = tables.iter().map(|t| format!("{t}__changes")).collect();
    let all: Vec<&str> = tables
        .iter()
        .chain(changes.iter())
        .map(String::as_str)
        .collect();
    let _cleanup = bq.cleanup(&all);

    // Rows that exist BEFORE the anchor: the baseline's whole job, in every table.
    for t in &tables {
        for id in 1..=5 {
            src.exec(&format!("INSERT INTO {t} (id, v) VALUES ({id}, {id})"));
        }
    }

    // 0. Preflight accepts the shape (the stream a log reader, the recipes keyset).
    let check = rig.cli(&["check"]);
    assert!(
        check.status.success(),
        "rivet check must pass on the init shape:\n{}",
        String::from_utf8_lossy(&check.stderr)
    );

    // 1. Anchor + every table's baseline in ONE run, then the first load: one
    //    warehouse object PER table (the multiplex fan-out), each equal to its source.
    rig.run_ok();
    load_ok(&rig);
    for t in &tables {
        assert!(
            accumulated(&bq, t) >= 5,
            "run 1: {t}: the baseline must reach the warehouse"
        );
        assert_table_is_source(&bq, t, src, "run 1");
    }
    let base: Vec<i64> = tables.iter().map(|t| accumulated(&bq, t)).collect();

    // 2. A delta in EVERY table → run 2 → load 2: exactly the 5 changed rows each.
    for t in &tables {
        for id in 6..=8 {
            src.exec(&format!("INSERT INTO {t} (id, v) VALUES ({id}, {id})"));
        }
        src.exec(&format!("UPDATE {t} SET v = 100 WHERE id = 1"));
        src.exec(&format!("DELETE FROM {t} WHERE id = 2"));
    }
    rig.run_ok();
    load_ok(&rig);
    for (t, b) in tables.iter().zip(&base) {
        assert_eq!(
            accumulated(&bq, t),
            b + 5,
            "run 2: {t}: appends the 5 changed rows (3 inserts, 1 update, 1 delete) and nothing else"
        );
        assert_table_is_source(&bq, t, src, "run 2");
    }
    let base: Vec<i64> = tables.iter().map(|t| accumulated(&bq, t)).collect();

    // 3. A crash on the CDC leg AFTER the part is flushed, BEFORE the checkpoint
    //    advances: the next plain run re-reads the un-acked changes of every table.
    for t in &tables {
        src.exec(&format!("INSERT INTO {t} (id, v) VALUES (9, 9), (10, 10)"));
    }
    let crashed = rig.run_args_env(
        &[],
        &[("RIVET_TEST_PANIC_AT", "cdc_after_flush_before_ack")],
    );
    assert!(
        !crashed.status.success(),
        "the injected crash must fail the run — a run that exits 0 saw NO change to flush:\n{}{}",
        String::from_utf8_lossy(&crashed.stdout),
        String::from_utf8_lossy(&crashed.stderr)
    );
    rig.run_ok();
    load_ok(&rig);
    for (t, b) in tables.iter().zip(&base) {
        assert!(
            accumulated(&bq, t) >= b + 2,
            "run 3 (after the crash): {t}: rows 9 and 10 must land — nothing lost"
        );
        assert_table_is_source(&bq, t, src, "run 3 after cdc_after_flush_before_ack");
    }

    // 4. Idle: a run with no changes and a load with no new run append nothing.
    let before: Vec<i64> = tables.iter().map(|t| accumulated(&bq, t)).collect();
    rig.run_ok();
    load_ok(&rig);
    load_ok(&rig);
    for (t, b) in tables.iter().zip(&before) {
        assert_eq!(
            accumulated(&bq, t),
            *b,
            "idle: {t}: nothing may be appended"
        );
        assert_table_is_source(&bq, t, src, "idle");
    }
}

#[test]
#[ignore = "live: requires mysql-cdc + BigQuery creds"]
fn partner_shape_three_tables_one_stream_mysql() {
    let Some(bq) = BqLive::from_env("multi_my") else {
        return;
    };
    let mut conn = cdc_conn();
    let tables: Vec<String> = (0..3)
        .map(|i| unique_name(&format!("multi_my{i}")))
        .collect();
    let mut guards = Vec::new();
    for t in &tables {
        conn.exec(&format!("DROP TABLE IF EXISTS {t}"));
        conn.exec(&format!("CREATE TABLE {t} (id BIGINT PRIMARY KEY, v INT)"));
        guards.push(MysqlCdcTable(t.clone()));
    }
    // `stream`, as init names it — a recipe carries its TABLE's name.
    let rig = shaped(
        Rig::mysql_cdc(&tables[0]).export_named("stream"),
        &tables,
        &bq,
    );
    cycle(rig, tables, bq, &mut conn);
}

#[test]
#[ignore = "live: requires postgres-cdc (wal_level=logical) + BigQuery creds"]
fn partner_shape_three_tables_one_stream_postgres() {
    let Some(bq) = BqLive::from_env("multi_pg") else {
        return;
    };
    let mut client =
        postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls).expect("connect postgres-cdc");
    let tables: Vec<String> = (0..3)
        .map(|i| unique_name(&format!("multi_pg{i}")))
        .collect();
    let mut guards: Vec<Box<dyn std::any::Any>> = Vec::new();
    for t in &tables {
        client.exec(&format!(
            "DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id BIGINT PRIMARY KEY, v INT)"
        ));
        guards.push(Box::new(PgTable::adopt_on(POSTGRES_CDC_URL, t.clone())));
    }
    // The product creates the slot at the first run's anchor step (after the seed,
    // so the baseline — not the stream — carries the seed); the guard drops it.
    let slot = unique_name("multi_pg_slot");
    guards.push(Box::new(Slot(slot.clone())));
    let rig = shaped(
        Rig::pg_cdc(&tables[0], &slot).export_named("stream"),
        &tables,
        &bq,
    );
    cycle(rig, tables, bq, &mut client);
}
