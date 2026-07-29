//! CDC matrix for the extraction-time canonical content hash
//! (`content_hash:` → `__content_hash`, `src/content_hash.rs`).
//!
//! One cell per engine, each proving on the LIVE change stream that the
//! hash the CDC sink computes in Rust equals what the engine's OWN SQL
//! recomputes over the same rows — the property the warehouse↔source
//! auditor stands on (the source side recomputes the hash inline in SQL;
//! the warehouse side reads the extracted column; if the two renderings
//! ever drift, every content audit diverges 100%).
//!
//! Per-engine expected values are computed by the ENGINE (never by the
//! code under test), with the exact SQL counterpart expressions of the
//! canonical rendering: pk digits ‖ '|' ‖ text raw ‖ '|' ‖ timestamp at
//! `YYYY-MM-DD HH:MM:SS`, NULL → `<NULL>`, sha256 lower-hex first 15.
//!
//! Delete images differ by engine and that is part of the matrix:
//! MySQL/MSSQL carry the FULL before-image (delete hash == pre-delete row
//! hash, engine-verified); PostgreSQL's default replica identity carries
//! the KEY ONLY (content columns NULL → sentinel text — asserted via an
//! independent sha256, labeled pipeline-consistency since the row is gone
//! from the source). MongoDB has no SQL side at all — its cell asserts
//! pipeline consistency only, matching the auditor's split-digest design
//! (the auditor computes Mongo digests client-side anyway).

use std::collections::BTreeMap;
use std::path::Path;

use crate::common::*;
use mysql::prelude::Queryable as _;

/// `(op, id) → __content_hash` over every parquet part under `dir`.
fn hashes_by_op_id(dir: &Path) -> BTreeMap<(String, i64), String> {
    use arrow::array::{Int64Array, StringArray};
    let mut out = BTreeMap::new();
    for b in read_all_parts(dir) {
        let col = |n: &str| b.column_by_name(n).unwrap_or_else(|| panic!("column {n}"));
        let op = col("__op").as_any().downcast_ref::<StringArray>().unwrap();
        let ids = col("id").as_any().downcast_ref::<Int64Array>().unwrap();
        let hashes = col("__content_hash")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..b.num_rows() {
            out.insert(
                (op.value(i).to_string(), ids.value(i)),
                hashes.value(i).to_string(),
            );
        }
    }
    out
}

/// Independent sha256 for the images an engine legitimately cannot
/// re-render (PG key-only deletes, Mongo) — first 15 lowercase hex chars
/// of the canonical text.
fn sha15(text: &str) -> String {
    use sha2::{Digest as _, Sha256};
    let mut hex: String = Sha256::digest(text.as_bytes())
        .iter()
        .take(8)
        .map(|b| format!("{b:02x}"))
        .collect();
    hex.truncate(15);
    hex
}

const CONTENT_HASH_LINE: &str = "content_hash: { pk: id, cols: [status, updated_at] }";

// ── MySQL ────────────────────────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose mysql-cdc (binlog)"]
fn mysql_cdc_content_hash_matches_engine_sql() {
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_chash");
    let mut c = mysql::Pool::new(MYSQL_CDC_URL).unwrap().get_conn().unwrap();
    let _guard = MysqlCdcTable(tbl.clone());
    c.query_drop(format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, status VARCHAR(32) NULL, \
         updated_at DATETIME NULL)"
    ))
    .unwrap();
    c.query_drop(format!(
        "INSERT INTO {tbl} VALUES (1, 'seed', '2024-01-01 00:01:00')"
    ))
    .unwrap();

    // Anchor at NOW (SHOW MASTER STATUS) so the run drains ONLY the
    // mutations below.
    let row: mysql::Row = c.query_first("SHOW MASTER STATUS").unwrap().unwrap();
    let (file, pos): (String, u64) = (row.get(0).unwrap(), row.get(1).unwrap());
    let ckpt = d.path().join("ck.json");
    std::fs::write(&ckpt, format!(r#"{{"file":"{file}","pos":{pos}}}"#)).unwrap();

    c.query_drop(format!(
        "INSERT INTO {tbl} VALUES (2, 'bob', '2026-07-29 12:00:00')"
    ))
    .unwrap();
    c.query_drop(format!(
        "UPDATE {tbl} SET status='flipped', updated_at=NULL WHERE id=1"
    ))
    .unwrap();
    // Engine-computed expected hashes for the CURRENT rows (after-images of
    // the insert and the update), captured BEFORE the delete.
    let expr = "SUBSTRING(SHA2(CONCAT_WS('|', id, \
                COALESCE(CAST(status AS CHAR),'<NULL>'), \
                COALESCE(CAST(DATE_FORMAT(updated_at,'%Y-%m-%d %H:%i:%s') AS CHAR),'<NULL>')),256),1,15)";
    let expected: BTreeMap<i64, String> = c
        .query_map(
            format!("SELECT id, {expr} FROM {tbl} ORDER BY id"),
            |(id, h): (i64, String)| (id, h),
        )
        .unwrap()
        .into_iter()
        .collect();
    // MySQL deletes carry the FULL before-image: the delete event's hash
    // must equal the row's last engine-computed hash.
    c.query_drop(format!("DELETE FROM {tbl} WHERE id=2"))
        .unwrap();

    let out = d.path().join("out");
    let rig = Rig::mysql_cdc(&tbl)
        .checkpoint_path(ckpt)
        .dest_path(out.clone())
        .export_line(CONTENT_HASH_LINE);
    rig.run_ok();

    let got = hashes_by_op_id(&out);
    assert_eq!(
        got.get(&("insert".into(), 2)),
        expected.get(&2),
        "insert after-image hash must match MySQL's own SQL rendering"
    );
    assert_eq!(
        got.get(&("update".into(), 1)),
        expected.get(&1),
        "update after-image hash (incl. NULL updated_at) must match MySQL SQL"
    );
    assert_eq!(
        got.get(&("delete".into(), 2)),
        expected.get(&2),
        "MySQL delete carries the full before-image — hash == pre-delete row hash"
    );
}

// ── PostgreSQL ───────────────────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres-cdc (wal_level=logical)"]
fn pg_cdc_content_hash_matches_engine_sql() {
    use postgres::NoTls;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_chash");
    let slot = unique_name("rivet_chash_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).unwrap();
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; \
         CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, status TEXT NULL, \
         updated_at TIMESTAMP NULL); \
         INSERT INTO {tbl} VALUES (1, 'seed', '2024-01-01 00:01:00');"
    ))
    .unwrap();
    // Server-side anchor: slot creation pins the stream position — only
    // the mutations below are in the capture window.
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _slot_guard = Slot(slot.clone());

    c.batch_execute(&format!(
        "INSERT INTO {tbl} VALUES (2, 'bob', '2026-07-29 12:00:00'); \
         UPDATE {tbl} SET status='flipped', updated_at=NULL WHERE id=1; \
         DELETE FROM {tbl} WHERE id=2;"
    ))
    .unwrap();
    // PG's expected values come from the engine over the canonical
    // renderings; id=2 is deleted, so its after-image expectation is
    // reconstructed from the same VALUES the insert used.
    let expr = |id: &str, status: &str, ts: &str| {
        format!(
            "substr(encode(sha256(convert_to({id}::text || '|' || \
             COALESCE(CAST({status} AS text),'<NULL>') || '|' || \
             COALESCE(CAST(to_char({ts}, 'YYYY-MM-DD HH24:MI:SS') AS text),'<NULL>'), 'UTF8')),'hex'),1,15)"
        )
    };
    let exp_insert: String = c
        .query_one(
            &format!(
                "SELECT {}",
                expr("2", "'bob'", "TIMESTAMP '2026-07-29 12:00:00'")
            ),
            &[],
        )
        .unwrap()
        .get(0);
    let exp_update: String = c
        .query_one(
            &format!(
                "SELECT {} FROM {tbl} WHERE id=1",
                expr("id", "status", "updated_at")
            ),
            &[],
        )
        .unwrap()
        .get(0);

    let out = d.path().join("out");
    let rig = Rig::pg_cdc(&tbl, &slot)
        .dest_path(out.clone())
        .export_line(CONTENT_HASH_LINE);
    rig.run_ok();

    let got = hashes_by_op_id(&out);
    assert_eq!(
        got.get(&("insert".into(), 2)).map(String::as_str),
        Some(exp_insert.as_str()),
        "insert after-image hash must match PostgreSQL's own SQL rendering"
    );
    assert_eq!(
        got.get(&("update".into(), 1)).map(String::as_str),
        Some(exp_update.as_str()),
        "update after-image hash (incl. NULL updated_at) must match PostgreSQL SQL"
    );
    // Default replica identity: the delete image is KEY-ONLY — content
    // columns are NULL by design, so the canonical text is the sentinel
    // form. The source row is gone; this leg is pipeline-consistency.
    assert_eq!(
        got.get(&("delete".into(), 2)).map(String::as_str),
        Some(sha15("2|<NULL>|<NULL>").as_str()),
        "PG key-only delete image must hash the sentinel text"
    );
}

// ── SQL Server ───────────────────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose mssql-cdc (SQL Agent + CDC)"]
fn mssql_cdc_content_hash_matches_engine_sql() {
    use std::time::Duration;
    let d = tempfile::tempdir().unwrap();
    let tbl = unique_name("cdc_chash");
    let ci = format!("dbo_{tbl}");
    mssql_cdc_exec(&format!(
        "CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, status VARCHAR(32) NULL, \
         updated_at DATETIME2(0) NULL)"
    ));
    mssql_cdc_exec(
        "IF NOT EXISTS(SELECT 1 FROM sys.databases WHERE name='rivet' AND is_cdc_enabled=1) \
         EXEC sys.sp_cdc_enable_db;",
    );
    mssql_cdc_exec(&format!(
        "EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N'{tbl}', \
         @role_name=NULL, @capture_instance=N'{ci}';"
    ));

    mssql_cdc_exec(&format!(
        "INSERT INTO {tbl} VALUES (2, 'bob', '2026-07-29 12:00:00'); \
         INSERT INTO {tbl} VALUES (1, 'seed', '2024-01-01 00:01:00'); \
         UPDATE {tbl} SET status='flipped', updated_at=NULL WHERE id=1;"
    ));
    // Engine-computed expectations for the final images, BEFORE the delete.
    let expr = "LOWER(SUBSTRING(CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', \
                CAST(id AS VARCHAR(40)) + '|' + \
                COALESCE(CAST(status AS VARCHAR(MAX)),'<NULL>') + '|' + \
                COALESCE(CAST(CONVERT(VARCHAR(19), updated_at, 120) AS VARCHAR(MAX)),'<NULL>')), 2), 1, 15))";
    let exp: BTreeMap<i64, String> =
        mssql_cdc_query_id_strings(&format!("SELECT id, {expr} FROM {tbl} ORDER BY id"));
    mssql_cdc_exec(&format!("DELETE FROM {tbl} WHERE id=2"));

    // The capture job is asynchronous — wait for all 4 change rows.
    for i in 0..60 {
        if mssql_cdc_query_i64(&format!("SELECT COUNT(*) FROM cdc.{ci}_CT")) >= 4 {
            break;
        }
        if i == 59 {
            panic!("capture job did not populate cdc.{ci}_CT in 30s");
        }
        std::thread::sleep(Duration::from_millis(500));
    }

    let out = d.path().join("out");
    let rig = Rig::mssql_cdc(&tbl, &ci)
        .dest_path(out.clone())
        .export_line(CONTENT_HASH_LINE);
    rig.run_ok();
    mssql_cdc_drop_table(&tbl);

    let got = hashes_by_op_id(&out);
    assert_eq!(
        got.get(&("insert".into(), 2)),
        exp.get(&2),
        "insert after-image hash must match SQL Server's own SQL rendering"
    );
    assert_eq!(
        got.get(&("update".into(), 1)),
        exp.get(&1),
        "update after-image hash (incl. NULL updated_at) must match SQL Server SQL"
    );
    assert_eq!(
        got.get(&("delete".into(), 2)),
        exp.get(&2),
        "MSSQL delete carries the full before-image — hash == pre-delete row hash"
    );
}

// ── MongoDB ──────────────────────────────────────────────────────────────

/// Mongo has NO SQL side to recompute the hash — the auditor computes Mongo
/// digests client-side by design (the split-digest model), so this cell
/// asserts PIPELINE consistency: the extracted `__content_hash` equals an
/// independent sha256 of the canonical text rebuilt from the SAME part's
/// data columns. Cross-engine parity does not apply here and the cell says
/// so — a tier label, not a weaker copy of the SQL cells.
#[test]
#[ignore = "live: requires docker compose mongo replica set (change streams)"]
fn mongo_cdc_content_hash_is_pipeline_consistent() {
    use mongodb::bson::doc;
    let d = tempfile::tempdir().unwrap();
    let db = unique_name("chash");
    let coll = "events";
    const PORT: u16 = 27018;
    let m = MongoTest::connect(PORT, &db);

    // Anchor run over the empty collection (change-stream token pinned).
    let out1 = d.path().join("out1");
    let ckpt = d.path().join("ck.json");
    let rig1 = Rig::mongo_cdc(coll)
        .source_url(&MongoTest::url(PORT, &db))
        .checkpoint_path(ckpt.clone())
        .dest_path(out1)
        .export_line("content_hash: { pk: _id, cols: [document] }");
    rig1.run_ok();

    m.insert_many(
        coll,
        vec![
            doc! {"_id": 1i64, "status": "alice"},
            doc! {"_id": 2i64, "status": "py|pe"},
        ],
    );

    let out2 = d.path().join("out2");
    let rig2 = Rig::mongo_cdc(coll)
        .source_url(&MongoTest::url(PORT, &db))
        .checkpoint_path(ckpt)
        .dest_path(out2.clone())
        .export_line("content_hash: { pk: _id, cols: [document] }");
    rig2.run_ok();

    // Rebuild the canonical text from the part's own data columns and
    // compare against the extracted hash column. The sink renders `_id` as
    // Utf8 decimal digits (the existing Mongo oracle pins this).
    let mut checked = 0;
    for b in read_all_parts(&out2) {
        use arrow::array::StringArray;
        let col = |n: &str| b.column_by_name(n).unwrap_or_else(|| panic!("column {n}"));
        let ids = col("_id").as_any().downcast_ref::<StringArray>().unwrap();
        let documents = col("document")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let hashes = col("__content_hash")
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..b.num_rows() {
            assert_eq!(
                hashes.value(i),
                sha15(&format!("{}|{}", ids.value(i), documents.value(i))),
                "row {i}: extracted hash must equal sha256 of the canonical text"
            );
            checked += 1;
        }
    }
    assert_eq!(checked, 2, "both inserted docs captured and checked");
}
