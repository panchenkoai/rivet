//! `max_file_size` on a parquet export whose row groups `rivet init` sized
//! (`row_group_strategy: auto`, #307): parquet flushes bytes only when a row group
//! closes, so an auto-sized ~128 MB group kept every part whole. The cap must hold.

use crate::common::*;

const ROWS: i64 = 20_000;
const CAP: u64 = 256 * 1024;

/// A PG table of `ROWS` rows of poorly-compressible text, a few MB of parquet.
fn seed() -> PgTable {
    let tbl = unique_name("mfs");
    let mut c = postgres::Client::connect(POSTGRES_URL, postgres::NoTls).expect("connect postgres");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl};
         CREATE TABLE {tbl} (id BIGINT PRIMARY KEY, p TEXT);
         INSERT INTO {tbl} SELECT g, md5(g::text) || md5((g * 7)::text) || md5((g * 13)::text)
           FROM generate_series(1, {ROWS}) g;"
    ))
    .unwrap();
    PgTable::adopt(tbl)
}

#[test]
#[ignore = "live: requires docker compose postgres + the rivet-duckdb oracle"]
fn max_file_size_holds_under_auto_sized_row_groups() {
    let t = seed();
    let rig = Rig::pg_batch(t.name())
        .duckdb_oracle()
        .export_line("max_file_size: 256KB")
        .export_line("parquet: { row_group_strategy: auto, target_row_group_mb: 128 }");
    rig.run_ok();
    let sizes: Vec<u64> = files_with_extension(&rig.out_dir(), "parquet")
        .iter()
        .map(|p| std::fs::metadata(p).unwrap().len())
        .collect();
    rig.assert_complete("id", ROWS, "every row ships across the rotated parts");
    assert!(
        sizes.len() >= 3,
        "a multi-MB export under a 256 KB cap must rotate, got {} part(s) of {sizes:?}",
        sizes.len()
    );
    assert!(
        sizes.iter().all(|s| *s <= CAP + CAP / 4),
        "no part may exceed the cap by more than one quarter-cap row group: {sizes:?}"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_row_group_row_count_under_auto_is_said_to_be_ignored() {
    let t = seed();
    let rig = Rig::pg_batch(t.name()).export_line("parquet: { row_group_rows: 500 }");
    let said = rig.run_ok_capture();
    assert!(
        said.contains(
            "parquet.row_group_rows is ignored under row_group_strategy: auto (the default)"
        ),
        "a knob that does nothing must say so:\n{said}"
    );
}
