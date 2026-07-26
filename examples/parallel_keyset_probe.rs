//! Parallel-keyset PROTOTYPE probe (feat/parallel-keyset, MySQL).
//!
//! Measures whether splitting keyset (seek) pagination into N ROW-BALANCED
//! ranges and running them concurrently speeds up the read — and proves that a
//! ROW-percentile split (not a value min..max/N split) is what preserves
//! keyset's sparse-key immunity.
//!
//! Faithful to rivet's real loop: each worker pages `WHERE key > cursor
//! [AND key <= hi] ORDER BY key LIMIT n`, advancing its own high-water cursor.
//!
//! Usage:
//!   cargo run --release --example parallel_keyset_probe -- \
//!     "mysql://rivet:rivet@127.0.0.1:3310/bench" big id 4 10000

use mysql::Pool;
use mysql::prelude::*;
use std::time::Instant;

/// Page one range to exhaustion via seek. `lo` is an EXCLUSIVE lower bound
/// (None = from the start), `hi` an INCLUSIVE upper bound (None = to the end).
/// Returns rows read.
fn seek_range(
    pool: &Pool,
    table: &str,
    key: &str,
    page: u64,
    lo: Option<i64>,
    hi: Option<i64>,
) -> u64 {
    let mut c = pool.get_conn().expect("conn");
    let mut cursor = lo;
    let mut n: u64 = 0;
    loop {
        let where_clause = match (cursor, hi) {
            (Some(l), Some(h)) => format!("WHERE {key} > {l} AND {key} <= {h}"),
            (Some(l), None) => format!("WHERE {key} > {l}"),
            (None, Some(h)) => format!("WHERE {key} <= {h}"),
            (None, None) => String::new(),
        };
        let sql = format!(
            "SELECT {key}, payload FROM {table} {where_clause} ORDER BY {key} LIMIT {page}"
        );
        let rows: Vec<(i64, String)> = c.query(sql).expect("seek page");
        if rows.is_empty() {
            break;
        }
        n += rows.len() as u64;
        cursor = Some(rows.last().unwrap().0);
        if (rows.len() as u64) < page {
            break;
        }
    }
    n
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let url = a
        .get(1)
        .map(String::as_str)
        .unwrap_or("mysql://rivet:rivet@127.0.0.1:3310/bench");
    let table = a.get(2).map(String::as_str).unwrap_or("big");
    let key = a.get(3).map(String::as_str).unwrap_or("id");
    let parts: usize = a.get(4).and_then(|s| s.parse().ok()).unwrap_or(4);
    let page: u64 = a.get(5).and_then(|s| s.parse().ok()).unwrap_or(10_000);

    let pool = Pool::new(url).expect("pool");
    let mut c = pool.get_conn().expect("conn");

    let total: u64 = c
        .query_first::<u64, _>(format!("SELECT COUNT(*) FROM {table}"))
        .expect("count")
        .expect("count row");

    // ROW-percentile boundaries: the key value at row-offset total*i/parts.
    // (A prototype uses OFFSET; production would SAMPLE to avoid the O(offset)
    // scan — that trade-off is exactly what this probe measures.)
    let mut bounds: Vec<i64> = Vec::new();
    for i in 1..parts {
        let off = total * (i as u64) / (parts as u64);
        let b: i64 = c
            .query_first::<i64, _>(format!(
                "SELECT {key} FROM {table} ORDER BY {key} LIMIT 1 OFFSET {off}"
            ))
            .expect("boundary")
            .expect("boundary row");
        bounds.push(b);
    }
    // Ranges as (lo_exclusive, hi_inclusive).
    let mut ranges: Vec<(Option<i64>, Option<i64>)> = Vec::new();
    let mut prev: Option<i64> = None;
    for &b in &bounds {
        ranges.push((prev, Some(b)));
        prev = Some(b);
    }
    ranges.push((prev, None));

    // Sequential baseline: one worker pages the whole table.
    let t0 = Instant::now();
    let seq_rows = seek_range(&pool, table, key, page, None, None);
    let seq_ms = t0.elapsed().as_millis().max(1);

    // Parallel: N workers, each pages its row-balanced range concurrently.
    let t1 = Instant::now();
    let counts: Vec<u64> = std::thread::scope(|s| {
        let pool_ref = &pool;
        let handles: Vec<_> = ranges
            .iter()
            .map(|&(lo, hi)| s.spawn(move || seek_range(pool_ref, table, key, page, lo, hi)))
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let par_ms = t1.elapsed().as_millis().max(1);
    let par_rows: u64 = counts.iter().sum();

    let max_w = *counts.iter().max().unwrap_or(&0);
    let min_w = *counts.iter().filter(|&&c| c > 0).min().unwrap_or(&0);
    let skew = if min_w > 0 {
        max_w as f64 / min_w as f64
    } else {
        f64::INFINITY
    };

    println!("table={table} key={key} total={total} parts={parts} page={page}");
    println!("boundaries (row-percentile): {bounds:?}");
    println!("per-worker rows: {counts:?}  (skew max/min = {skew:.2}x)");
    println!("SEQUENTIAL: {seq_rows} rows in {seq_ms} ms");
    println!(
        "PARALLEL:   {par_rows} rows in {par_ms} ms   => speedup {:.2}x",
        seq_ms as f64 / par_ms as f64
    );
    assert_eq!(
        seq_rows, par_rows,
        "row-count parity: parallel must read every row"
    );
}
