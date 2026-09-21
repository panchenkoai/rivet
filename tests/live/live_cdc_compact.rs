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
//!   6. changes → run → load → compact whose process CRASHES right after the one
//!      scripted job (MERGEs + DROP) returned → the buffer is already gone, the
//!      next compact is a no-op, the base equals the source's live rows once.
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

    // 5b. Two loads with NO compact between them — the buffer accumulates both
    //     cycles (an insert, then its update), and one compact ranks the winner
    //     over the whole buffer: the key lands once, at its latest value.
    scn.insert(10);
    scn.settle();
    scn.rig.run_ok();
    load_ok(&scn.rig);
    scn.update(10);
    scn.settle();
    scn.rig.run_ok();
    load_ok(&scn.rig);
    assert_eq!(
        bq.read_bq_count(&changes),
        "2",
        "two cycles' changes wait in one buffer"
    );
    let (ok, said) = compact(&scn.rig, &[]);
    assert!(ok && said.contains("2 change row(s)"), "{said}");
    assert_eq!(
        base_profile(&bq, &table).0,
        9,
        "one new key, however many cycles carried it"
    );
    assert_eq!(
        v_of(&bq, &table, 10),
        Some(99),
        "the later cycle's update won"
    );

    // 6. rivet crashes right after the compaction job returned: the MERGEs and the
    //    DROP were ONE scripted job, so the warehouse is already consistent — the
    //    next compact finds no buffer, merges nothing twice, and the base equals
    //    the source.
    scn.insert(9);
    scn.update(3);
    scn.settle();
    scn.rig.run_ok();
    load_ok(&scn.rig);
    let (ok, said) = compact(&scn.rig, &[("RIVET_TEST_PANIC_AT", "compact_after_merge")]);
    assert!(!ok, "the injected crash must fail the compact:\n{said}");
    assert!(
        bq.read_bq_table_type(&changes).is_none(),
        "the buffer went with the script, before rivet crashed"
    );
    let (ok, said) = compact(&scn.rig, &[]);
    assert!(ok && said.contains("COMPACT SKIP"), "{said}");
    let (n, live, gone, _, sum) = base_profile(&bq, &table);
    assert_eq!(
        (n, live, gone),
        (10, 9, 1),
        "one insert, one update, still one tombstone"
    );
    assert_eq!(live, scn.count());
    assert_eq!(sum, 1 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10);
    assert_eq!(v_of(&bq, &table, 3), Some(99), "merged once, not twice");
}

// ── the same cycle on every engine ────────────────────────────────────────
//
// The tombstone fix (`f1ca6adc`) was measured on MySQL and PostgreSQL only; the
// contract it restores — a delete becomes `__is_deleted = TRUE`, an insert never
// lands with a NULL flag — is the base-and-buffer layout's, not one engine's.
// One body, the key column as a parameter (MongoDB's is `_id`), so SQL Server
// and MongoDB are graded by the same assertions rather than a paraphrase.

/// [`base_profile`] over an arbitrary key column.
fn base_profile_by(bq: &BqLive, table: &str, key: &str) -> (i64, i64, i64, i64, i64) {
    let row = &bq.read_bq_rows(&format!(
        "SELECT COUNT(*) AS n, COUNTIF(NOT __is_deleted) AS live, COUNTIF(__is_deleted) AS gone, \
         COUNTIF(__is_deleted IS NULL) AS unflagged, IFNULL(SUM(IF(__is_deleted, 0, {key})), 0) AS s \
         FROM `{}.{}.{table}`",
        bq.project, bq.dataset
    ))[0];
    let g = |k: &str| -> i64 { row[k].as_str().expect(k).parse().expect("a number") };
    (g("n"), g("live"), g("gone"), g("unflagged"), g("s"))
}

/// The `v` of one key, with both sides as SQL expressions: `key` is the key column
/// (or a cast of it) and `value` the expression that yields `v` — a column on the
/// SQL engines, a path into the `document` blob on MongoDB.
fn v_by(bq: &BqLive, table: &str, key: &str, value: &str, id: i64) -> Option<i64> {
    let rows = bq.read_bq_rows(&format!(
        "SELECT {value} AS v FROM `{}.{}.{table}` WHERE {key} = {id}",
        bq.project, bq.dataset
    ));
    rows.first()
        .and_then(|r| r["v"].as_str())
        .map(|s| s.parse().expect("v"))
}

/// The rig shape every engine shares: a stream with a whole-table baseline recipe,
/// landing base-and-buffer with `pk` as the merge key — the proven
/// `live_cdc_full_cycle` shape, so a red here is the compaction, not the recipe.
fn tombstone_shape(rig: Rig, table: &str, pk: &str, bq: &BqLive) -> Rig {
    rig.cdc("backfill: auto")
        .also_batch_export("baseline", table, "full")
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(&format!(", pk: [{pk}]")))
}

/// Steps 1–4 of the MySQL cycle above: the baseline lands with every flag `false`,
/// a delta (three inserts, one update, one delete) is buffered and not merged,
/// then compact flags the delete, refreshes the update and drops the buffer.
/// `key` and `value` are the oracle's SQL for the key column and the `v` value.
fn tombstone_cycle(mut scn: CdcScenario, key: &str, value: &str, bq: &BqLive) {
    let table = scn.table.clone();
    let changes = format!("{table}__changes");
    let _cleanup = bq.cleanup(&[&table, &changes]);
    for id in 1..=5 {
        scn.insert(id);
    }
    scn.settle();

    scn.rig.run_ok();
    let said = load_ok(&scn.rig);
    assert!(said.contains("layout=base+buffer"), "{said}");
    assert_eq!(
        base_profile_by(bq, &table, key),
        (5, 5, 0, 0, 15),
        "five live rows, no NULL flag — the flag came in the Parquet"
    );
    // Whether this first compact has anything to merge is the ANCHOR model's, not
    // the layout's: MySQL and SQL Server anchor at the first run, so the seed is
    // baseline-only and compact says SKIP; PostgreSQL pins its slot at creation,
    // BEFORE the seed, so the stream re-captures those five rows and the load
    // buffers them again — at-least-once by design (ADR-0034), merged
    // idempotently. What every engine must agree on is the base AFTERWARDS.
    let (ok, said) = compact(&scn.rig, &[]);
    assert!(ok, "{said}");
    assert_eq!(
        base_profile_by(bq, &table, key),
        (5, 5, 0, 0, 15),
        "a baseline re-captured by the stream merges into the same five keys — no \
         duplicate, no phantom tombstone: {said}"
    );
    assert!(
        bq.read_bq_table_type(&changes).is_none(),
        "no buffer survives the first compact, whether it merged or skipped"
    );

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
    assert_eq!(
        base_profile_by(bq, &table, key).0,
        5,
        "the base waits for compact"
    );

    let (ok, said) = compact(&scn.rig, &[]);
    assert!(ok && said.contains("COMPACT OK"), "{said}");
    let (n, live, gone, nulls, sum) = base_profile_by(bq, &table, key);
    assert_eq!(
        (n, gone, nulls),
        (8, 1, 0),
        "nothing physically deleted, the deleted key flagged, no NULL flag"
    );
    assert_eq!(live, scn.count(), "live rows == source");
    assert_eq!(
        sum,
        1 + 3 + 4 + 5 + 6 + 7 + 8,
        "the live keys are the source's"
    );
    assert_eq!(
        v_by(bq, &table, key, value, 1),
        Some(99),
        "the update reached the base"
    );
    assert_eq!(
        v_by(bq, &table, key, value, 2),
        Some(2),
        "the tombstone keeps its last values"
    );
    assert!(
        bq.read_bq_table_type(&changes).is_none(),
        "the buffer is dropped after the merge"
    );
}

#[test]
#[ignore = "live: requires postgres-cdc + BigQuery creds"]
fn base_and_buffer_cycle_keeps_deletes_as_flags_postgres() {
    let Some(bq) = BqLive::from_env("compact_pg") else {
        return;
    };
    let scn = CdcScenario::pg_with("compact_pg", "id BIGINT PRIMARY KEY, v INT", |r, t| {
        tombstone_shape(r, t, "id", &bq)
    });
    tombstone_cycle(scn, "id", "v", &bq);
}

#[test]
#[ignore = "live: requires mssql-cdc + BigQuery creds"]
fn base_and_buffer_cycle_keeps_deletes_as_flags_mssql() {
    let Some(bq) = BqLive::from_env("compact_ms") else {
        return;
    };
    let scn = CdcScenario::mssql_with("compact_ms", "id BIGINT PRIMARY KEY, v INT", |r, t| {
        tombstone_shape(r, t, "id", &bq)
    });
    tombstone_cycle(scn, "id", "v", &bq);
}

#[test]
#[ignore = "live: requires mongo-rs + BigQuery creds"]
fn base_and_buffer_cycle_keeps_deletes_as_flags_mongo() {
    let Some(bq) = BqLive::from_env("compact_mg") else {
        return;
    };
    require_alive(LiveService::MongoRs);
    let scn = CdcScenario::mongo_with("compact_mg", |r, t| tombstone_shape(r, t, "_id", &bq));
    // The merge KEY stays `_id`; the ORACLE adapts to the Mongo shape, measured on
    // the first two runs: `_id` lands as STRING (a BSON id is polymorphic, so the
    // exporter canonicalises it), and there is no `v` column at all — both the
    // baseline recipe and the stream ship `_id` + a `document` JSON blob, so the
    // value is read out of the blob. The tombstone assertions passed before either
    // of these; only the value oracle was SQL-shaped.
    tombstone_cycle(
        scn,
        "CAST(_id AS INT64)",
        "CAST(JSON_VALUE(document, '$.v') AS INT64)",
        &bq,
    );
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

/// Two `rivet compact` of one table AT ONCE (a scheduler double-fire): the lease
/// admits one; the other is refused by name — or, arriving after the winner's
/// DROP, finds no buffer. Either way the base ends with each change applied
/// exactly once: never a MERGE racing a MERGE.
#[test]
#[ignore = "live: requires mysql-cdc + BigQuery creds"]
fn two_compacts_at_once_admit_one_and_apply_each_change_once() {
    let Some(bq) = BqLive::from_env("compact_race") else {
        return;
    };
    let mut scn =
        CdcScenario::mysql_with("compact_race", "id BIGINT PRIMARY KEY, v INT", |r, t| {
            r.cdc("backfill: auto")
                .also_batch_export("baseline", t, "full")
                .dest_gcs_live(&bq.bucket, &bq.prefix)
                .top_line(&bq.load_line(", pk: [id]"))
        });
    let table = scn.table.clone();
    let changes = format!("{table}__changes");
    let _cleanup = bq.cleanup(&[&table, &changes]);
    for id in 1..=3 {
        scn.insert(id);
    }
    scn.settle();
    scn.rig.run_ok();
    load_ok(&scn.rig);
    for id in 4..=6 {
        scn.insert(id);
    }
    scn.update(1);
    scn.settle();
    scn.rig.run_ok();
    load_ok(&scn.rig);
    assert_eq!(bq.read_bq_count(&changes), "4");

    let cfg = scn.rig.config_path().to_string_lossy().to_string();
    let outs: Vec<std::process::Output> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let cfg = cfg.clone();
                s.spawn(move || run_rivet_env(&["compact", "-c", &cfg], &[]))
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("compact thread"))
            .collect()
    });
    let said: Vec<String> = outs
        .iter()
        .map(|o| {
            format!(
                "{}{}",
                String::from_utf8_lossy(&o.stdout),
                String::from_utf8_lossy(&o.stderr)
            )
        })
        .collect();
    let all = said.join("\n---\n");
    let merged = said.iter().filter(|s| s.contains("COMPACT OK")).count();
    let refused = said
        .iter()
        .filter(|s| s.contains("is writing") && s.contains("lease is held"))
        .count();
    let skipped = said.iter().filter(|s| s.contains("COMPACT SKIP")).count();
    assert_eq!(merged, 1, "exactly one compaction merged:\n{all}");
    assert_eq!(
        refused + skipped,
        1,
        "the other was refused by the lease or found no buffer:\n{all}"
    );
    let (n, live, gone, _, sum) = base_profile(&bq, &table);
    assert_eq!((n, live, gone), (6, 6, 0), "{all}");
    assert_eq!(live, scn.count());
    assert_eq!(sum, 1 + 2 + 3 + 4 + 5 + 6);
    assert_eq!(
        v_of(&bq, &table, 1),
        Some(99),
        "applied once, at the latest value"
    );
    assert!(bq.read_bq_table_type(&changes).is_none());
}

/// An old table under the name the export wants, which THIS state DB's ledger
/// has no record of rivet loading: the load refuses before writing anything, and
/// the stranger's rows stay exactly as they were. The buffer is never created, so
/// `compact` has nothing to merge into a table that is not ours.
#[test]
#[ignore = "live: requires mysql-cdc + BigQuery creds"]
fn a_base_table_rivet_never_loaded_is_refused_before_any_write() {
    let Some(bq) = BqLive::from_env("compact_foreign") else {
        return;
    };
    let mut scn =
        CdcScenario::mysql_with("compact_foreign", "id BIGINT PRIMARY KEY, v INT", |r, t| {
            r.cdc("backfill: auto")
                .also_batch_export("baseline", t, "full")
                .dest_gcs_live(&bq.bucket, &bq.prefix)
                .top_line(&bq.load_line(", pk: [id]"))
        });
    let table = scn.table.clone();
    let changes = format!("{table}__changes");
    let _cleanup = bq.cleanup(&[&table, &changes]);
    let fqtn = format!("{}.{}.{table}", bq.project, bq.dataset);
    bq.exec(&format!(
        "CREATE TABLE `{fqtn}` (id INT64, v INT64, note STRING) AS SELECT 1, 1, 'someone else'"
    ));

    scn.insert(1);
    scn.settle();
    scn.rig.run_ok();
    let out = scn.rig.cli(&["load"]);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!out.status.success(), "a foreign base must refuse:\n{said}");
    assert!(said.contains("no record of rivet loading it"), "{said}");
    assert_eq!(
        bq.read_bq_count(&table),
        "1",
        "the stranger's row is untouched"
    );
    assert!(
        bq.read_bq_table_type(&changes).is_none(),
        "no buffer was created for a table we may not write"
    );
    assert_eq!(
        ledger_load_statuses(&scn.rig.config_path(), &fqtn),
        ["refused"],
        "a stop before the write, never a `failed` row that would make the target ours"
    );
}

/// The base is dropped out from under a live stream (a hand-run DROP, a rebuilt
/// dataset). The next cycle still buffers its changes, and `compact` REFUSES by
/// name instead of passing BigQuery's `Not found: Table` through — with the buffer
/// left whole, so nothing of the cycle is lost.
#[test]
#[ignore = "live: requires mysql-cdc + BigQuery creds"]
fn a_missing_base_refuses_the_compaction_and_keeps_the_buffer() {
    let Some(bq) = BqLive::from_env("compact_nobase") else {
        return;
    };
    let mut scn =
        CdcScenario::mysql_with("compact_nobase", "id BIGINT PRIMARY KEY, v INT", |r, t| {
            r.cdc("backfill: auto")
                .also_batch_export("baseline", t, "full")
                .dest_gcs_live(&bq.bucket, &bq.prefix)
                .top_line(&bq.load_line(", pk: [id]"))
        });
    let table = scn.table.clone();
    let changes = format!("{table}__changes");
    let _cleanup = bq.cleanup(&[&table, &changes]);
    let fqtn = format!("{}.{}.{table}", bq.project, bq.dataset);

    scn.insert(1);
    scn.insert(2);
    scn.settle();
    scn.rig.run_ok();
    load_ok(&scn.rig);
    assert_eq!(base_profile(&bq, &table).0, 2, "the base landed");

    bq.exec(&format!("DROP TABLE `{fqtn}`"));
    scn.insert(3);
    scn.update(1);
    scn.settle();
    scn.rig.run_ok();
    load_ok(&scn.rig);
    assert_eq!(
        bq.read_bq_count(&changes),
        "2",
        "the cycle's changes are buffered whatever the base is doing"
    );

    let (ok, said) = compact(&scn.rig, &[]);
    assert!(!ok, "a missing base must refuse the merge:\n{said}");
    assert!(said.contains("the base table does not exist"), "{said}");
    assert!(
        !said.contains("Not found: Table"),
        "rivet names the cause, it does not pass BigQuery's error through: {said}"
    );
    assert_eq!(
        bq.read_bq_count(&changes),
        "2",
        "the buffer survives the refusal — the cycle is deferred, not dropped"
    );
    assert_eq!(
        ledger_load_statuses(&scn.rig.config_path(), &fqtn)
            .last()
            .map(String::as_str),
        Some("refused"),
        "recorded as a stop before the write"
    );
}

/// A compaction whose process dies BEFORE its merge job: the buffer must survive
/// whole, and the next compact applies every change exactly once. The sibling of
/// the crash-after-merge case — there the script had already dropped the buffer.
#[test]
#[ignore = "live: requires mysql-cdc + BigQuery creds"]
fn a_compaction_that_dies_before_its_merge_leaves_the_buffer_whole() {
    let Some(bq) = BqLive::from_env("compact_precrash") else {
        return;
    };
    let mut scn = CdcScenario::mysql_with(
        "compact_precrash",
        "id BIGINT PRIMARY KEY, v INT",
        |r, t| {
            r.cdc("backfill: auto")
                .also_batch_export("baseline", t, "full")
                .dest_gcs_live(&bq.bucket, &bq.prefix)
                .top_line(&bq.load_line(", pk: [id]"))
        },
    );
    let table = scn.table.clone();
    let changes = format!("{table}__changes");
    let _cleanup = bq.cleanup(&[&table, &changes]);

    for id in 1..=3 {
        scn.insert(id);
    }
    scn.settle();
    scn.rig.run_ok();
    load_ok(&scn.rig);
    assert_eq!(base_profile(&bq, &table).0, 3, "the baseline landed");

    scn.update(2);
    scn.delete(3);
    scn.insert(4);
    scn.settle();
    scn.rig.run_ok();
    load_ok(&scn.rig);
    assert_eq!(bq.read_bq_count(&changes), "3", "three changes buffered");

    let (ok, said) = compact(&scn.rig, &[("RIVET_TEST_PANIC_AT", "compact_before_merge")]);
    assert!(!ok, "the injected crash must fail the compact:\n{said}");
    assert_eq!(
        bq.read_bq_count(&changes),
        "3",
        "nothing merged, nothing dropped: the buffer is whole"
    );
    assert_eq!(
        base_profile(&bq, &table).0,
        3,
        "the base is exactly as the crash found it"
    );

    let (ok, said) = compact(&scn.rig, &[]);
    assert!(ok && said.contains("3 change row(s)"), "{said}");
    let (n, live, gone, unflagged, sum) = base_profile(&bq, &table);
    assert_eq!(
        (n, live, gone, unflagged),
        (4, 3, 1, 0),
        "one insert, one tombstone, the update in place"
    );
    assert_eq!(live, scn.count());
    assert_eq!(sum, 1 + 2 + 4);
    assert_eq!(
        v_of(&bq, &table, 2),
        Some(99),
        "applied once, at its latest value"
    );
}
