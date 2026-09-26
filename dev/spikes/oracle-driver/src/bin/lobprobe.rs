//! Probe: locator-mode fetch with a large array, reading every LOB through its locator.
use std::io::Read;
fn main() {
    let sql = std::env::args().nth(1).expect("sql");
    let lob_cols: Vec<usize> = std::env::args().nth(2).expect("lob col idx csv").split(',').map(|s| s.parse().unwrap()).collect();
    let cfg = oracledb::Config::default().set_credentials("rivet", "rivet").set_connect_string("localhost:1521/FREEPDB1").unwrap();
    let conn = oracledb::connect(cfg).unwrap();
    let t = std::time::Instant::now();
    let stmt = conn.statement(&sql).unwrap().fetch_lobs().fetch_array_size(1000).prefetch_rows(1000).build().unwrap();
    let (mut n, mut bytes, mut empty, mut nulls) = (0usize, 0usize, 0usize, 0usize);
    for r in stmt.query(&[]).unwrap() {
        let mut r = r.unwrap();
        for &c in &lob_cols {
            match r.take::<Option<oracledb::Lob>>(c).unwrap() {
                Some(mut lob) => { let mut buf = Vec::new(); lob.read_to_end(&mut buf).unwrap(); if buf.is_empty() { empty += 1 } bytes += buf.len(); }
                None => nulls += 1,
            }
        }
        n += 1;
    }
    println!("rows={n} lob_bytes={bytes} empty={empty} nulls={nulls} in {:?}", t.elapsed());
}
