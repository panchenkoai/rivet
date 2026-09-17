//! The base-and-buffer layout end to end, the cycle the partner runs:
//! `run → load → compact`, on MySQL CDC with a keyset backfill recipe into
//! BigQuery.
//!
//!   1. seed 5 rows → run (anchor + baseline) → load: the BASE `<table>` is a
//!      physical table with the source columns plus `__is_deleted`, every row
//!      `false` (never NULL — the flag travels inside the baseline Parquet), no
//!      view, no buffer yet (an idle drain writes nothing);
//!   2. compact with no buffer → a said no-op;
//!   3. three inserts, one update, one delete → run → load: the buffer
//!      `<table>__changes` holds exactly the five changes, the base is untouched;
//!   4. compact → base: 8 rows (nothing physically deleted), 7 live, the deleted
//!      key flagged with its last values kept, the updated key refreshed; the
//!      buffer is DROPPED;
//!   5. a second compact → no-op; the base unchanged;
//!   6. changes → run → load → compact that CRASHES between the MERGE and the
//!      DROP → the next compact re-merges the same buffer idempotently and drops
//!      it; the base equals the source's live rows exactly once.
//!
//! Oracles: the source (`COUNT`/`SUM`) and `bq` — never rivet's own report. Needs
//! the MySQL CDC stand and the warehouse env; SKIPS without them.

use crate::common::*;

fn load_ok(rig: &Rig) -> String {
    let out = rig.cli(&["load"]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.status.success(), "rivet load failed:\n{said}");
    said
}

fn compact(rig: &Rig, extra_env: &[(&str, &str)]) -> (bool, String) {
    let out = rig.cli_env(&["compact"], extra_env);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), said)
}

/// `(rows, live rows, flagged rows, NULL flags, SUM(id) over live rows)` of the base.
fn base_profile(bq: &BqLive, table: &str) -> (i64, i64, i64, i64, i64) {
    let row = &bq.read_bq_rows(&format!(
        "SELECT COUNT(*) AS n, COUNTIF(NOT __is_deleted) AS live, COUNTIF(__is_deleted) AS gone, \
         COUNTIF(__is_deleted IS NULL) AS unflagged, IFNULL(SUM(IF(__is_deleted, 0, id)), 0) AS s \
         FROM `{}.{}.{table}`",
        bq.project, bq.dataset
    ))[0];
    let g = |k: &str| -> i64 { row[k].as_str().expect(k).parse().expect("a number") };
    (g("n"), g("live"), g("gone"), g("unflagged"), g("s"))
}

fn v_of(bq: &BqLive, table: &str, id: i64) -> Option<i64> {
    let rows = bq.read_bq_rows(&format!(
        "SELECT v FROM `{}.{}.{table}` WHERE id = {id}",
        bq.project, bq.dataset
    ));
    rows.first()
        .and_then(|r| r["v"].as_str())
        .map(|s| s.parse().expect("v"))
}

#[test]
#[ignore = "live: requires mysql-cdc + BigQuery creds"]
fn base_and_buffer_cycle_run_load_compact_keeps_deletes_as_flags() {
    let Some(bq) = BqLive::from_env("compact") else {
        return;
    };
    let mut scn = CdcScenario::mysql_with("compact", "id BIGINT PRIMARY KEY, v INT", |r, t| {
        r.cdc("backfill: auto")
            .also_batch_export("baseline", t, "chunked")
            .also_export_line("chunk_by_key: id")
            .also_export_line("chunk_size: 2")
            .dest_gcs_live(&bq.bucket, &bq.prefix)
            .top_line(&bq.load_line(", pk: [id]"))
    });
    let table = scn.table.clone();
    let changes = format!("{table}__changes");
    let _cleanup = bq.cleanup(&[&table, &changes]);
    for id in 1..=5 {
        scn.insert(id);
    }
    scn.settle();

    // 1. Anchor + baseline → load: a physical base with the flag as data.
    scn.rig.run_ok();
    let said = load_ok(&scn.rig);
    assert!(said.contains("layout=base+buffer"), "{said}");
    assert_eq!(
        bq.read_bq_table_type(&table).as_deref(),
        Some("BASE TABLE"),
        "the base is a TABLE, not a view"
    );
    assert_eq!(
        base_profile(&bq, &table),
        (5, 5, 0, 0, 15),
        "five live rows, no NULL flag — the flag came in the Parquet"
    );

    // 2. Nothing to compact yet.
    let (ok, said) = compact(&scn.rig, &[]);
    assert!(ok && said.contains("COMPACT SKIP"), "{said}");

    // 3. Changes → run → load: the buffer holds exactly them, the base is untouched.
    for id in 6..=8 {
        scn.insert(id);
    }
    scn.update(1);
    scn.delete(2);
    scn.settle();
    scn.rig.run_ok();
    load_ok(&scn.rig);
    assert_eq!(
        bq.read_bq_count(&changes),
        "5",
        "the buffer holds the five changes"
    );
    assert_eq!(base_profile(&bq, &table).0, 5, "the base waits for compact");

    // 4. Compact: upsert, flag the delete, drop the buffer.
    let (ok, said) = compact(&scn.rig, &[]);
    assert!(
        ok && said.contains("COMPACT OK") && said.contains("5 change row(s)"),
        "{said}"
    );
    let (n, live, gone, nulls, sum) = base_profile(&bq, &table);
    assert_eq!(n, 8, "nothing is physically deleted");
    assert_eq!(gone, 1, "the deleted key is flagged");
    assert_eq!(nulls, 0);
    assert_eq!(live, scn.count(), "live rows == source");
    assert_eq!(
        sum,
        1 + 3 + 4 + 5 + 6 + 7 + 8,
        "the live ids are the source's"
    );
    assert_eq!(
        v_of(&bq, &table, 1),
        Some(99),
        "the update reached the base"
    );
    assert_eq!(
        v_of(&bq, &table, 2),
        Some(2),
        "the tombstone keeps its last values"
    );
    assert!(
        bq.read_bq_table_type(&changes).is_none(),
        "the buffer is dropped after the merge"
    );

    // 5. A second compact is a no-op.
    let (ok, said) = compact(&scn.rig, &[]);
    assert!(ok && said.contains("COMPACT SKIP"), "{said}");
    assert_eq!(base_profile(&bq, &table).0, 8);

    // 6. A crash between MERGE and DROP: the re-run merges the same rows again,
    //    idempotently, and drops the buffer.
    scn.insert(9);
    scn.update(3);
    scn.settle();
    scn.rig.run_ok();
    load_ok(&scn.rig);
    let (ok, said) = compact(&scn.rig, &[("RIVET_TEST_PANIC_AT", "compact_after_merge")]);
    assert!(!ok, "the injected crash must fail the compact:\n{said}");
    assert_eq!(
        bq.read_bq_table_type(&changes).as_deref(),
        Some("BASE TABLE"),
        "the buffer survives a crash before the DROP"
    );
    let (ok, said) = compact(&scn.rig, &[]);
    assert!(ok && said.contains("COMPACT OK"), "{said}");
    let (n, live, gone, _, sum) = base_profile(&bq, &table);
    assert_eq!(
        (n, live, gone),
        (9, 8, 1),
        "one insert, one update, still one tombstone"
    );
    assert_eq!(live, scn.count());
    assert_eq!(sum, 1 + 3 + 4 + 5 + 6 + 7 + 8 + 9);
    assert_eq!(v_of(&bq, &table, 3), Some(99), "merged once, not twice");
    assert!(bq.read_bq_table_type(&changes).is_none());
}

/// `(v, created_at as text)` of one key in the base.
fn row_of(bq: &BqLive, table: &str, id: i64) -> (Option<i64>, Option<String>) {
    let rows = bq.read_bq_rows(&format!(
        "SELECT v, CAST(created_at AS STRING) AS c FROM `{}.{}.{table}` WHERE id = {id}",
        bq.project, bq.dataset
    ));
    let r = &rows[0];
    (
        r["v"].as_str().map(|s| s.parse().expect("v")),
        r["c"].as_str().map(str::to_string),
    )
}

/// A partitioned base prunes its MERGE by the buffer's partition-column range, and
/// rows whose partition column is NULL merge on their own. A key with changes on
/// BOTH sides of that split (its `created_at` set to NULL and back within one
/// cycle) must still end at its LATEST change — the winner is chosen over the whole
/// buffer, never per subset, or the order of the MERGE jobs decides the row.
#[test]
#[ignore = "live: requires mysql-cdc + BigQuery creds"]
fn a_key_changed_across_the_partition_split_ends_at_its_latest_change() {
    let Some(bq) = BqLive::from_env("compact_split") else {
        return;
    };
    let mut scn =
        CdcScenario::mysql_with(
            "compact_split",
            "id BIGINT PRIMARY KEY, v INT, created_at DATETIME NULL",
            |r, t| {
                r.cdc("backfill: auto")
                    .also_batch_export("baseline", t, "full")
                    .dest_gcs_live(&bq.bucket, &bq.prefix)
                    .top_line(&bq.load_line(
                        ", pk: [id], partition: { column: created_at, granularity: day }",
                    ))
            },
        );
    let table = scn.table.clone();
    let changes = format!("{table}__changes");
    let _cleanup = bq.cleanup(&[&table, &changes]);
    scn.sql(&format!(
        "INSERT INTO {table} (id, v, created_at) VALUES (1, 1, '2024-01-01 00:00:00'), (2, 2, '2024-01-01 00:00:00')"
    ));
    scn.settle();
    scn.rig.run_ok();
    load_ok(&scn.rig);
    assert_eq!(base_profile(&bq, &table).0, 2);

    // Key 1: NULL first, dated last. Key 2: dated first, NULL last.
    scn.sql(&format!(
        "UPDATE {table} SET created_at = NULL, v = 20 WHERE id = 1"
    ));
    scn.sql(&format!(
        "UPDATE {table} SET created_at = '2024-01-01 00:00:00', v = 30 WHERE id = 1"
    ));
    scn.sql(&format!(
        "UPDATE {table} SET created_at = '2024-01-02 00:00:00', v = 200 WHERE id = 2"
    ));
    scn.sql(&format!(
        "UPDATE {table} SET created_at = NULL, v = 300 WHERE id = 2"
    ));
    scn.settle();
    scn.rig.run_ok();
    load_ok(&scn.rig);
    assert_eq!(bq.read_bq_count(&changes), "4", "four changes buffered");

    let (ok, said) = compact(&scn.rig, &[]);
    assert!(ok && said.contains("COMPACT OK"), "{said}");
    assert_eq!(
        row_of(&bq, &table, 1),
        (Some(30), Some("2024-01-01 00:00:00".to_string())),
        "key 1 ends at its latest change (dated), not at the older NULL one"
    );
    assert_eq!(
        row_of(&bq, &table, 2),
        (Some(300), None),
        "key 2 ends at its latest change (NULL), not at the older dated one"
    );
    assert_eq!(
        base_profile(&bq, &table),
        (2, 2, 0, 0, 3),
        "still two live rows"
    );
}
