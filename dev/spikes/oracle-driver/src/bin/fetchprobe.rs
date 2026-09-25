//! Probe: fetch one query at several array sizes; report rows or the first error.
fn main() {
    let sql = std::env::args().nth(1).expect("sql");
    let _ = rustls_ring();
    for (size, pre) in [(1000u32, 1u32), (1000, 3)] {
        let cfg = oracledb::Config::default()
            .set_credentials("rivet", "rivet")
            .set_connect_string("localhost:1521/FREEPDB1")
            .unwrap();
        let conn = oracledb::connect(cfg).unwrap();
        let stmt = conn.statement(&sql).unwrap().fetch_array_size(size).prefetch_rows(pre).build().unwrap();
        let t = std::time::Instant::now();
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut n = 0usize;
            for r in stmt.query(&[]).unwrap() {
                match r {
                    Ok(_) => n += 1,
                    Err(e) => return format!("error after {n} rows: {e:?}"),
                }
            }
            format!("ok {n} rows")
        }));
        println!("array={size:5} prefetch={pre} -> {} in {:?}", res.unwrap_or_else(|_| "PANIC".into()), t.elapsed());
    }
}
fn rustls_ring() {}
