//! Live replica CDC harness (#2) — proves rivet can read a *replica's* binlog: the
//! "we were handed a product but have no master access" case. The replica re-logs
//! replicated changes into its own binlog (`log_replica_updates`), and rivet reads
//! that. Validates the "Reading from a replica" section of docs/reference/cdc.md
//! end to end.
//!
//! Gated `#[ignore]`: needs the `replica` profile (mysql-primary :3308 → mysql-replica
//! :3309). Run with:
//!     docker compose --profile replica up -d mysql-primary mysql-replica mysql-replica-nolog
//!     cargo test --test live_suite -- --ignored

use std::collections::HashMap;
use std::time::Duration;

use crate::common::*;
use mysql::prelude::Queryable;

const PRIMARY: &str = "mysql://root:rivet@127.0.0.1:3308/rivet";
const REPLICA_ROOT: &str = "mysql://root:rivet@127.0.0.1:3309/rivet";
const REPLICA_RIVET: &str = "mysql://rivet:rivet@127.0.0.1:3309/rivet";

fn conn(url: &str) -> mysql::PooledConn {
    mysql::Pool::new(url)
        .expect("pool")
        .get_conn()
        .expect("conn")
}

/// Wire up position-based replication from the primary's *current* position, so the
/// init transactions (both servers ran their own `MYSQL_USER` init) are not
/// re-applied — full GTID sync would conflict on "CREATE USER rivet already exists".
fn ensure_replication() {
    ensure_replication_on(REPLICA_ROOT);
}

/// [`ensure_replication`] for the replica at `replica_root`.
fn ensure_replication_on(replica_root: &str) {
    let mut p = conn(PRIMARY);
    let mut r = conn(replica_root);
    let _ = r.query_drop("STOP REPLICA");
    let _ = r.query_drop("RESET REPLICA ALL");
    let row: mysql::Row = p.query_first("SHOW MASTER STATUS").unwrap().unwrap();
    let (file, pos): (String, u64) = (row.get(0).unwrap(), row.get(1).unwrap());
    r.query_drop(format!(
        "CHANGE REPLICATION SOURCE TO SOURCE_HOST='mysql-primary', SOURCE_PORT=3306, \
         SOURCE_USER='repl', SOURCE_PASSWORD='repl', SOURCE_LOG_FILE='{file}', \
         SOURCE_LOG_POS={pos}, SOURCE_AUTO_POSITION=0"
    ))
    .unwrap();
    r.query_drop("START REPLICA").unwrap();
}

/// Poll the replica until `pred` holds (replication is async).
fn wait_replica<F: Fn(&mut mysql::PooledConn) -> bool>(what: &str, pred: F) {
    let mut r = conn(REPLICA_ROOT);
    for _ in 0..60 {
        if pred(&mut r) {
            return;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    panic!("replica did not reach: {what} (30s)");
}

fn read_cdc_rows(dir: &std::path::Path) -> Vec<(String, i32, Option<i32>)> {
    use arrow::array::{Array, Int32Array, StringArray};
    let f = std::fs::File::open(
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .find(|p| p.extension().is_some_and(|x| x == "parquet"))
            .expect("a .parquet part"),
    )
    .unwrap();
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f)
        .unwrap()
        .build()
        .unwrap();
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let col = |n: &str| batch.column(batch.schema().index_of(n).unwrap());
        let op = col("__op").as_any().downcast_ref::<StringArray>().unwrap();
        let id = col("id").as_any().downcast_ref::<Int32Array>().unwrap();
        let v = col("v").as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..batch.num_rows() {
            out.push((
                op.value(i).to_string(),
                id.value(i),
                (!v.is_null(i)).then(|| v.value(i)),
            ));
        }
    }
    out
}

#[test]
#[ignore = "live: requires docker compose --profile replica (mysql-primary :3308 → mysql-replica :3309)"]
fn cdc_reads_changes_from_a_replica() {
    let _serial = cross_process_serial("mysql_replica");
    ensure_replication();
    let mut p = conn(PRIMARY);
    let table = unique_name("rep_cdc");

    // Create the table on the PRIMARY; wait for it to replicate (rivet resolves the
    // schema from the replica, so the table must exist there).
    p.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    p.query_drop(format!("CREATE TABLE {table} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let exists = format!(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='rivet' AND table_name='{table}'"
    );
    wait_replica("table replicated", |r| {
        r.query_first::<i64, _>(&exists).unwrap().unwrap_or(0) == 1
    });

    // Checkpoint at the replica's own binlog position (rivet resumes the REPLICA's
    // binlog, not the primary's).
    let d = tempfile::tempdir().unwrap();
    let ckpt = d.path().join("ckpt");
    Rig::mysql_cdc(&table)
        .source_url(REPLICA_RIVET)
        .checkpoint_path(ckpt.clone())
        .pin_binlog_here();

    // Apply changes on the PRIMARY; they replicate into the replica's binlog.
    let mut expected: HashMap<u32, i32> = HashMap::new();
    for (id, v) in [(1, 10), (2, 20), (3, 30)] {
        p.exec_drop(format!("INSERT INTO {table} VALUES (?, ?)"), (id, v))
            .unwrap();
        expected.insert(id, v);
    }
    p.exec_drop(format!("UPDATE {table} SET v = 99 WHERE id = 1"), ())
        .unwrap();
    expected.insert(1, 99);
    p.exec_drop(format!("DELETE FROM {table} WHERE id = 2"), ())
        .unwrap();
    expected.remove(&2);

    // Wait for the replica to apply them (so its binlog has the full sequence).
    let count = format!("SELECT COUNT(*) FROM {table}");
    wait_replica("changes replicated", |r| {
        r.query_first::<i64, _>(&count).unwrap().unwrap_or(-1) == expected.len() as i64
    });

    // Capture from the REPLICA (rivet user, :3309).
    let out = d.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let rig = Rig::mysql_cdc(&table)
        .source_url(REPLICA_RIVET)
        .checkpoint_path(ckpt.clone())
        .dest_path(out.clone());
    let res = rig.run_args(&[]);
    assert!(
        res.status.success(),
        "cdc-from-replica failed:\n{}",
        String::from_utf8_lossy(&res.stderr)
    );

    // Replaying the changes captured from the replica reconstructs the source's
    // final state — proving the replica's binlog carried every change.
    let mut replayed: HashMap<u32, i32> = HashMap::new();
    for (op, id, v) in read_cdc_rows(&out) {
        match op.as_str() {
            "insert" | "update" => {
                replayed.insert(id as u32, v.unwrap());
            }
            "delete" => {
                replayed.remove(&(id as u32));
            }
            _ => {}
        }
    }
    assert_eq!(
        replayed, expected,
        "changes captured from the replica must reconstruct the source"
    );

    let _ = p.query_drop(format!("DROP TABLE IF EXISTS {table}")); // replicates the drop
}

const NOLOG_ROOT: &str = "mysql://root:rivet@127.0.0.1:3310/rivet";
const NOLOG_RIVET: &str = "mysql://rivet:rivet@127.0.0.1:3310/rivet";

#[test]
#[ignore = "live: requires docker compose --profile replica (mysql-primary :3308 → mysql-replica-nolog :3310)"]
fn cdc_from_a_replica_that_does_not_relog_refuses_instead_of_capturing_nothing() {
    let _serial = cross_process_serial("mysql_replica");
    ensure_replication_on(NOLOG_ROOT);
    let mut p = conn(PRIMARY);
    let mut r = conn(NOLOG_ROOT);
    let table = unique_name("rep_nolog");
    p.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    p.query_drop(format!("CREATE TABLE {table} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let rows = format!("SELECT COUNT(*) FROM {table}");
    let wait = |r: &mut mysql::PooledConn, n: i64| {
        for _ in 0..60 {
            if r.query_first::<i64, _>(&rows).ok().flatten() == Some(n) {
                return;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        panic!("fixture: the replica did not reach {n} row(s)");
    };
    wait(&mut r, 0);

    let rig = Rig::mysql_cdc(&table)
        .oracle_known_defect(
            "a failed run left: cdc-checkpoint",
            "known defect: a CDC run that refuses at open still writes its checkpoint at the position it started from; the anchor must be written after the open checks",
        )
        .source_url(NOLOG_RIVET);
    let refused = |out: &std::process::Output| {
        let said = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(
            said.contains("this server is a replica with log_replica_updates = OFF"),
            "a refusal must name the setting:\n{said}"
        );
    };
    let anchor = rig.run();
    if !anchor.status.success() {
        return refused(&anchor);
    }
    p.query_drop(format!("INSERT INTO {table} VALUES (1, 10)"))
        .unwrap();
    wait(&mut r, 1);
    let out = rig.run();
    if !out.status.success() {
        return refused(&out);
    }
    let delivered = if files_with_extension(&rig.out_dir(), "parquet").is_empty() {
        Default::default()
    } else {
        duckdb_declared_dir_id_set(&rig.out_dir())
    };
    assert_eq!(
        delivered,
        [1].into_iter().collect(),
        "the row reached the replica, and the run exited 0 — so it must be in the output"
    );
}

const PRIMARY_RIVET: &str = "mysql://rivet:rivet@127.0.0.1:3308/rivet";

/// A capture read from the replica, then the replica "fails" and rivet is pointed at the primary.
struct Failover {
    table: String,
    _dir: tempfile::TempDir,
    ckpt: std::path::PathBuf,
    out: std::path::PathBuf,
    parts_before: std::collections::BTreeSet<std::path::PathBuf>,
    on_primary: Rig,
}

/// Parquet parts under `dir`.
fn parts(dir: &std::path::Path) -> std::collections::BTreeSet<std::path::PathBuf> {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
        .collect()
}

/// `(op, id)` of every row in `batches`.
fn ops_of(batches: &[arrow::record_batch::RecordBatch]) -> Vec<(String, i32)> {
    use arrow::array::{Array, Int32Array, StringArray};
    let mut out = Vec::new();
    for b in batches {
        let col = |n: &str| b.column(b.schema().index_of(n).unwrap()).clone();
        let (op, id) = (col("__op"), col("id"));
        let op = op.as_any().downcast_ref::<StringArray>().unwrap();
        let id = id.as_any().downcast_ref::<Int32Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((op.value(i).to_string(), id.value(i)));
        }
    }
    out
}

/// Replica capture of ids 1-2, then ids 3-4 written on the primary while rivet is switched to it.
fn failover(tag: &str) -> Failover {
    ensure_replication();
    let mut p = conn(PRIMARY);
    let table = unique_name(tag);
    p.query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .unwrap();
    p.query_drop(format!("CREATE TABLE {table} (id INT PRIMARY KEY, v INT)"))
        .unwrap();
    let rows = format!("SELECT COUNT(*) FROM {table}");
    let replicated = |n: i64| {
        wait_replica("rows replicated", |r| {
            r.query_first::<i64, _>(&rows).ok().flatten() == Some(n)
        })
    };
    replicated(0);
    let dir = tempfile::tempdir().unwrap();
    let ckpt = dir.path().join("ckpt");
    let out = dir.path().join("out");
    std::fs::create_dir_all(&out).unwrap();
    let on = |url: &str| {
        Rig::mysql_cdc(&table)
            .source_url(url)
            .checkpoint_path(ckpt.clone())
            .dest_path(out.clone())
    };
    let on_replica = on(REPLICA_RIVET);
    on_replica.run_ok(); // anchors on the replica's binlog
    p.query_drop(format!("INSERT INTO {table} VALUES (1, 10), (2, 20)"))
        .unwrap();
    replicated(2);
    on_replica.run_ok();
    assert_eq!(
        ops_of(&read_all_parts(&out)),
        vec![("insert".into(), 1), ("insert".into(), 2)],
        "fixture: the replica capture holds ids 1-2"
    );
    p.query_drop(format!("INSERT INTO {table} VALUES (3, 30), (4, 40)"))
        .unwrap();
    replicated(4);
    Failover {
        parts_before: parts(&out),
        on_primary: on(PRIMARY_RIVET),
        table,
        _dir: dir,
        ckpt,
        out,
    }
}

/// A checkpoint from one cluster member resumed against another after a failover is refused,
/// never applied as binlog coordinates of a different server: no part, no checkpoint move. On this
/// stand both identity tiers refuse (server_uuid differs, and the replica's own GTIDs are not on the
/// primary), so it goes RED only when the whole check is off (verified by disabling `enforce`).
#[test]
#[ignore = "live: requires docker compose --profile replica (mysql-primary :3308 → mysql-replica :3309)"]
fn a_failover_to_another_cluster_member_is_refused_not_resumed_at_foreign_coordinates() {
    let _serial = cross_process_serial("mysql_replica");
    let f = failover("rep_failover_safe");
    let ckpt_before = std::fs::read(&f.ckpt).unwrap();
    let said = f.on_primary.run_expect_fail();
    assert!(
        said.contains("RIVET_SOURCE_CDC_FOREIGN_CHECKPOINT"),
        "the switch to another server is refused by name: {said}"
    );
    assert_eq!(
        std::fs::read(&f.ckpt).unwrap(),
        ckpt_before,
        "the checkpoint did not move"
    );
    assert_eq!(parts(&f.out), f.parts_before, "no part was written");
    let _ = conn(PRIMARY).query_drop(format!("DROP TABLE IF EXISTS {}", f.table));
}

/// Strict known defect (MySQL CDC failover step): resuming on another cluster member by GTID is not
/// implemented, so the run is refused; passes while that holds, fails ("did not panic") once rivet
/// follows the failover and captures exactly ids 3-4.
#[test]
#[ignore = "live: requires docker compose --profile replica (mysql-primary :3308 → mysql-replica :3309)"]
#[should_panic(expected = "rivet did not follow the failover")]
fn cdc_follows_a_failover_to_another_cluster_member_by_gtid() {
    let _serial = cross_process_serial("mysql_replica");
    let f = failover("rep_failover_follow");
    let res = f.on_primary.run_args(&[]);
    let _ = conn(PRIMARY).query_drop(format!("DROP TABLE IF EXISTS {}", f.table));
    assert!(
        res.status.success(),
        "rivet did not follow the failover to another cluster member: {}",
        String::from_utf8_lossy(&res.stderr)
    );
    assert_eq!(
        ops_of(&read_all_parts(&f.out)),
        vec![
            ("insert".into(), 1),
            ("insert".into(), 2),
            ("insert".into(), 3),
            ("insert".into(), 4)
        ],
        "across the failover every change is captured exactly once, none skipped"
    );
}
