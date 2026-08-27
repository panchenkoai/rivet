//! SOAK STAND — the spill path under repetition, on every CDC engine.
//!
//! The unit tests answer "is one spilled transaction correct?". This answers the
//! questions that only repetition can: does the memory ceiling HOLD across cycles,
//! do the spill files accumulate, does the destination still hold every source row
//! after N runs into one prefix, and do rivet's own ledgers still add up.
//!
//! MongoDB is here too, and it is the CONTROL. It has no transaction buffer at all
//! — no `tx`, no cap, and `TxnFramer::single_event_commit` marks every event its own
//! commit — so there is nothing to spill AND the sink may roll after any event.
//! That makes it the one engine with an end-to-end memory ceiling, which the other
//! three do not have (see `rss_ceiling`). Running it beside them turns that from an
//! argument into two numbers: Mongo's peak RSS should stay FLAT as
//! `RIVET_SOAK_ROWS` grows, where the SQL engines' climbs.
//!
//! It is a REGRESSION stand, not a one-off: every cycle re-seeds, re-runs and
//! re-checks, so the same command can be run before a release, after a refactor, or
//! for an hour under `RIVET_SOAK_CYCLES=100`.
//!
//!     cargo test --test live_suite -- --ignored soak_spill --nocapture
//!
//! Knobs — small by default so the stand is cheap enough to run every time:
//!   RIVET_SOAK_CYCLES  cycles per engine (default 3)
//!   RIVET_SOAK_ROWS    rows in each cycle's oversized transaction (default 2000)
//!   RIVET_SOAK_RSS_MB  peak-RSS ceiling for any rivet spawned (default 256)
//!   RIVET_SOAK_PAD     bytes in each row's wide column (default 200) — raise it
//!                      and lower ROWS for a `content_items`-shaped table, where
//!                      the BYTE cap fires and the row cap never does
//!   RIVET_SOAK_CAP     row cap (default 50); above ROWS for the no-spill baseline
//!   RIVET_SOAK_BYTE_CAP  byte cap; unset leaves the product default
//!   RIVET_SOAK_TX_ROWS rows per transaction (default: the whole cycle in one) —
//!                      the knob that separates "a big transaction is expensive"
//!                      from "CDC is expensive"
//!
//! The oracle is DuckDB, per the rule this repo learned the hard way: rivet's own
//! count is not evidence about rivet. `row_census` reads the SOURCE table, the
//! delivered parquet, and the SUM of `export_metrics.total_rows` and
//! `file_log.row_count` across every cycle, from one session sharing no code with
//! the product.

use crate::common::*;

/// Rows in each cycle's oversized transaction.
fn soak_rows() -> usize {
    env_usize("RIVET_SOAK_ROWS", 2000)
}

fn soak_cycles() -> usize {
    env_usize("RIVET_SOAK_CYCLES", 3)
}

/// MEASURED, PostgreSQL, one cycle, `pad=200` — the table this file exists to keep
/// honest. Reproduce any row with the knobs above.
///
/// | rows | shape | cap | peak RSS |
/// |---|---|---|---|
/// | 2 000 | one tx | 50 | 41.7 MB |
/// | 20 000 | one tx | 50 | 78.5 MB |
/// | 100 000 | one tx | 50 | 201.1 MB |
/// | 100 000 | one tx | no spill | 226.1 MB |
/// | 1 000 000 | one tx | 50 | 1589.7 MB |
/// | 1 000 000 | one tx | PRODUCT DEFAULTS | 1814.9 MB |
/// | 1 000 000 | **1000 tx of 1000** | 50 | **233.1 MB** |
/// | 10 000 000 | one tx | 50 | 12389.6 MB |
/// | 1 000 000 | one tx, MONGO | n/a | 331.9 MB |
///
/// Three things fall out of that table, none of them visible from one row:
///
/// 1. The cost tracks TRANSACTION size, not volume. The same million rows is
///    1589.7 MB in one transaction and 233.1 MB in a thousand — 6.8x, same engine,
///    same data. Ordinary OLTP traffic is the second shape.
/// 2. Spilling is worth ~11%, and Mongo says why: it has no transaction buffer and
///    marks every event its own commit, so its sink CAN roll, and it lands at
///    331.9 MB where PostgreSQL is at 1589.7 MB.
/// 3. At PRODUCT DEFAULTS the spill is not even involved below 5M rows / 2 GiB —
///    a 1M-row transaction costs 1.8 GB and never spills. The caps sit far above
///    where the memory actually starts to hurt.
///
/// Peak RSS any spawned rivet may reach, in bytes.
///
/// Read this with the measurement below, not with the intuition the word "spill"
/// invites. Peak memory here is NOT bounded by the cap, and the soak is where that
/// was found: a 100k-row transaction peaked at 202 MB with spilling and 226 MB with
/// the cap raised above the fixture — ~11%, and RSS still grows with
/// `RIVET_SOAK_ROWS` (41.7 MB at 2k, 78.5 at 20k, 201.1 at 100k).
///
/// The reason is in the sink, not the adapter: `RolloverPolicy::should_roll`
/// requires a `committed` event — the "never split a transaction across parts"
/// invariant that makes crash resume transaction-atomic — so the sink holds a whole
/// transaction whatever the adapter does. Spilling removes the ADAPTER's copy,
/// which is the smaller one.
///
/// So this ceiling is a REGRESSION bound, generous on purpose: it catches memory
/// running away, not the spill failing to bound it (which it never claimed to do
/// end-to-end). Compare the two numbers yourself with `RIVET_SOAK_CAP` above
/// `RIVET_SOAK_ROWS`; when the sink learns to spill, that gap is the evidence.
fn rss_ceiling() -> u64 {
    env_usize("RIVET_SOAK_RSS_MB", 256) as u64 * 1024 * 1024
}

/// Rows per TRANSACTION — the knob that separates "a big transaction is expensive"
/// from "CDC is expensive".
///
/// Default `0` means "the whole cycle in one transaction", which is the spill
/// fixture. Set it lower and the same total volume arrives as many small
/// transactions instead, which is what an ordinary OLTP workload looks like. The
/// sink rolls a part on a `committed` event, so the two shapes should behave
/// completely differently — and if they do not, the cost is not about transaction
/// size at all and every conclusion here changes.
fn tx_rows() -> usize {
    match env_usize("RIVET_SOAK_TX_ROWS", 0) {
        0 => usize::MAX,
        n => n,
    }
}

/// Width of each row's `pad` column — the knob that turns a row-cap fixture into a
/// BYTE-cap one.
///
/// `check_tx_buffer_caps` fails on rows OR bytes, and a narrow fixture only ever
/// crosses the row half. A `content_items`-shaped table (long text, JSON, markup) is
/// the opposite case: few rows, each large, so `RIVET_CDC_MAX_TX_BYTES` is what
/// fires. Raise this and lower `RIVET_SOAK_ROWS` to soak that half.
fn soak_pad() -> usize {
    env_usize("RIVET_SOAK_PAD", 200)
}

/// The BYTE cap the soak runs under, or `None` to leave the product default.
fn byte_cap() -> Option<usize> {
    std::env::var("RIVET_SOAK_BYTE_CAP")
        .ok()
        .and_then(|v| v.parse().ok())
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// The cap every soak runs under — low enough by default that each cycle's big
/// transaction crosses it, so the spill path is exercised on every cycle.
///
/// Overridable (`RIVET_SOAK_CAP`) for the comparison that matters: run the same
/// fixture with a cap ABOVE the row count and the transaction never spills, so the
/// two peak-RSS numbers say what the spill actually buys. Asserting a ceiling
/// without that baseline measures rivet's floor, not the spill.
fn cap() -> usize {
    env_usize("RIVET_SOAK_CAP", 50)
}

/// Rows in each cycle's SMALL transaction, which follows the big one.
///
/// Without it, "the window ended to drain a tail" and "the window is drained" are
/// indistinguishable — the threshold a one-transaction fixture never crosses.
const SMALL: usize = 3;

/// Shared end-of-soak checks: the oracle, the memory ceiling, and the spill files.
///
/// A free function rather than three copies, because the *questions* are the same on
/// every engine and only the seeding differs. Copies drift; this is the seam.
fn assert_soak_is_sound(rig: &Rig, engine: &str, expected: usize, rss_before: u64) {
    let census = rig.row_census();
    assert_eq!(
        census.source, expected as i64,
        "{engine}: the fixture itself must hold what the soak seeded, or every \
         comparison below is against the wrong number: {census:?}"
    );
    assert!(
        census.agrees(),
        "{engine}: after the whole soak the SOURCE, the delivered parquet, \
         export_metrics and file_log must still agree. They are summed across every \
         cycle, so a single cycle that dropped a spilled tail — or recorded rows no \
         part declares — lands here: {census:?}"
    );

    let peak = peak_child_rss_bytes();
    println!(
        "soak/{engine}: {expected} rows over {} cycles, cap {}, tx {} | peak child \
         RSS \
         {:.1} MB (was {:.1} MB before) | census {census:?}",
        soak_cycles(),
        cap(),
        if tx_rows() == usize::MAX {
            "all-in-one".to_string()
        } else {
            tx_rows().to_string()
        },
        peak as f64 / 1e6,
        rss_before as f64 / 1e6,
    );
    assert!(
        peak <= rss_ceiling(),
        "{engine}: a rivet child peaked at {:.1} MB, past the {:.1} MB ceiling. The \
         spill exists to BOUND memory — growth here with RIVET_SOAK_ROWS is a spill \
         that is quietly still buffering, which delivers identical rows and no \
         ceiling at all.",
        peak as f64 / 1e6,
        rss_ceiling() as f64 / 1e6,
    );

    let leaked = spill_files_under(std::path::Path::new(".rivet/spill"));
    assert!(
        leaked.is_empty(),
        "{engine}: spill files outlived the runs that wrote them. One per oversized \
         transaction fills a disk on a scheduler, which is precisely what a soak is \
         for: {leaked:?}"
    );
}

/// One soak cycle's run, under whichever cap(s) the stand was asked for.
///
/// Both caps in one place: a caller that set only the byte cap must not silently
/// get the row cap's default as well, which would spill for the wrong reason and
/// report a byte-cap soak that never crossed a byte threshold.
fn run_cycle(rig: &Rig) -> std::process::Output {
    let rows_cap = cap().to_string();
    let mut envs: Vec<(&str, &str)> = vec![("RIVET_CDC_MAX_TX_ROWS", &rows_cap)];
    let bytes_cap;
    if let Some(b) = byte_cap() {
        bytes_cap = b.to_string();
        envs.push(("RIVET_CDC_MAX_TX_BYTES", &bytes_cap));
    }
    rig.run_with_envs(&envs)
}

fn spill_files_under(dir: &std::path::Path) -> Vec<String> {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    rd.flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("rivet-spill-"))
        })
        .map(|p| p.display().to_string())
        .collect()
}

/// Assert the cycle actually spilled, and that memory stopped at the cap.
///
/// Without this the whole soak passes on a build where spilling never happens: the
/// rows are identical either way. It is the fixture-is-not-inert check, per cycle.
fn assert_cycle_spilled(engine: &str, cycle: usize, stderr: &str) {
    if engine == "mongo" {
        // The CONTROL: nothing to spill, by construction. Asserted rather than
        // skipped — if Mongo ever grows a transaction buffer, this is the line that
        // says the control has stopped being one.
        assert!(
            !stderr.contains("passed the in-memory cap"),
            "mongo cycle {cycle}: mongo has no transaction buffer (every event is \
             its own commit), so a spill here means the engine changed shape and \
             the memory claim below is no longer about a control"
        );
        return;
    }
    if cap() > soak_rows() || tx_rows() <= cap() {
        // Baseline run: no transaction is large enough to cross the cap, so nothing
        // spills by design — either the cap was raised above the fixture, or the
        // fixture was split into transactions smaller than the cap.
        assert!(
            !stderr.contains("passed the in-memory cap"),
            "{engine} cycle {cycle}: the cap was raised above the fixture and it \
             spilled anyway — the baseline is not a baseline"
        );
        return;
    }
    let split = stderr
        .split("delivered ")
        .filter_map(|s| s.split_once(" rows from memory and "))
        .find_map(|(head, rest)| {
            let tail = rest.split_once(" from disk")?.0;
            Some((
                head.trim().parse::<usize>().ok()?,
                tail.trim().parse::<usize>().ok()?,
            ))
        });
    let (from_memory, from_disk) = split.unwrap_or_else(|| {
        panic!("{engine} cycle {cycle}: no spill was reported. stderr:\n{stderr}")
    });
    assert!(
        from_disk > 0,
        "{engine} cycle {cycle}: the cap was noticed but nothing reached disk"
    );
    assert_eq!(
        from_memory,
        cap() + 1,
        "{engine} cycle {cycle}: memory must stop at the cap (+1, since the cap is \
         checked after the row is pushed) — a larger head means the ceiling is not \
         being enforced"
    );
}

// ─── PostgreSQL ──────────────────────────────────────────────────────────────

#[test]
#[ignore = "soak: requires docker compose postgres (wal_level=logical)"]
fn soak_spill_postgres() {
    use postgres::NoTls;
    let (cycles, rows) = (soak_cycles(), soak_rows());
    let rss_before = peak_child_rss_bytes();

    let tbl = unique_name("soak_spill_pg");
    let slot = unique_name("soak_spill_slot");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, NoTls).expect("connect");
    c.batch_execute(&format!(
        "DROP TABLE IF EXISTS {tbl}; CREATE TABLE {tbl} {ONE_TRANSACTION_DDL}"
    ))
    .unwrap();
    let _tbl = PgTable::adopt_on(POSTGRES_CDC_URL, tbl.clone());
    c.execute(
        "SELECT pg_create_logical_replication_slot($1, 'test_decoding')",
        &[&slot],
    )
    .unwrap();
    let _slot = Slot(slot.clone());

    // ONE rig for the whole soak: the same slot, the same checkpoint and the same
    // destination prefix on every cycle. That is the scheduler's own shape
    // (`until_current` on an interval), and the shape where a part-name collision
    // silently overwrote a prior run's data.
    let rig = Rig::pg_cdc(&tbl, &slot).census_oracle();
    let mut seeded = 0usize;
    for cycle in 1..=cycles {
        // Split into transactions of `tx_rows` (default: one transaction for the
        // whole cycle). Same total volume either way — only the framing differs.
        let mut left = rows;
        while left > 0 {
            let n = left.min(tx_rows());
            c.batch_execute(&transaction_over_wide(
                &tbl,
                seeded + 1..=seeded + n,
                soak_pad(),
            ))
            .unwrap();
            seeded += n;
            left -= n;
        }
        c.batch_execute(&transaction_over_wide(
            &tbl,
            seeded + 1..=seeded + SMALL,
            soak_pad(),
        ))
        .unwrap();
        seeded += SMALL;

        let out = run_cycle(&rig);
        assert!(
            out.status.success(),
            "postgres cycle {cycle} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_cycle_spilled("postgres", cycle, &String::from_utf8_lossy(&out.stderr));
    }
    assert_soak_is_sound(&rig, "postgres", seeded, rss_before);
}

// ─── MySQL ───────────────────────────────────────────────────────────────────

#[test]
#[ignore = "soak: requires docker compose mysql-cdc (binlog ROW + REPLICATION grant)"]
fn soak_spill_mysql() {
    use mysql::prelude::Queryable;
    let (cycles, rows) = (soak_cycles(), soak_rows());
    let rss_before = peak_child_rss_bytes();

    let tbl = unique_name("soak_spill_my");
    let mut c = mysql::Pool::new(MYSQL_CDC_URL)
        .expect("pool")
        .get_conn()
        .expect("conn");
    c.query_drop(format!("DROP TABLE IF EXISTS {tbl}")).unwrap();
    c.query_drop(format!("CREATE TABLE {tbl} {ONE_TRANSACTION_DDL}"))
        .unwrap();
    let _t = MysqlCdcTable(tbl.clone());

    let rig = Rig::mysql_cdc(&tbl).census_oracle();
    // Anchor BEFORE any data, so the stream starts here and every cycle is captured.
    // MySQL has no server-side anchor: its checkpoint is client-side coordinates.
    let row: mysql::Row = c
        .query_first("SHOW MASTER STATUS")
        .expect("show master status")
        .expect("binlog enabled");
    let (file, pos): (String, u64) = (row.get(0).unwrap(), row.get(1).unwrap());
    std::fs::write(
        rig.checkpoint(),
        format!(r#"{{"file":"{file}","pos":{pos}}}"#),
    )
    .unwrap();

    let mut seeded = 0usize;
    for cycle in 1..=cycles {
        mysql_seed_one_transaction_wide(&mut c, &tbl, seeded + 1..=seeded + rows, soak_pad());
        seeded += rows;
        mysql_seed_one_transaction_wide(&mut c, &tbl, seeded + 1..=seeded + SMALL, soak_pad());
        seeded += SMALL;

        let out = run_cycle(&rig);
        assert!(
            out.status.success(),
            "mysql cycle {cycle} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_cycle_spilled("mysql", cycle, &String::from_utf8_lossy(&out.stderr));
    }
    assert_soak_is_sound(&rig, "mysql", seeded, rss_before);
}

// ─── SQL Server ──────────────────────────────────────────────────────────────

#[test]
#[ignore = "soak: requires docker compose mssql with SQL Server Agent + CDC"]
fn soak_spill_mssql() {
    let (cycles, rows) = (soak_cycles(), soak_rows());
    let rss_before = peak_child_rss_bytes();

    let _serial = cross_process_serial("mssql_cdc");
    let table = unique_name("soak_spill_ms");
    let ci = format!("dbo_{table}");
    mssql_cdc_drop_table(&format!("dbo.{table}"));
    mssql_cdc_exec(&format!("CREATE TABLE dbo.{table} {ONE_TRANSACTION_DDL}"));
    enable_cdc(&table, &ci);
    let _guard = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };

    let rig = Rig::mssql_cdc(&table, &ci).census_oracle();
    let mut seeded = 0usize;
    for cycle in 1..=cycles {
        mssql_seed_one_transaction_wide(&table, seeded + 1..=seeded + rows, soak_pad());
        seeded += rows;
        mssql_seed_one_transaction_wide(&table, seeded + 1..=seeded + SMALL, soak_pad());
        seeded += SMALL;
        // The capture job is ASYNCHRONOUS: running before it has copied the rows
        // reads a short window and the next cycle re-reads it, which looks like a
        // resume bug rather than a fixture that did not wait.
        wait_for_capture(&ci, seeded as i64);

        let out = run_cycle(&rig);
        assert!(
            out.status.success(),
            "mssql cycle {cycle} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_cycle_spilled("mssql", cycle, &String::from_utf8_lossy(&out.stderr));
    }
    assert_soak_is_sound(&rig, "mssql", seeded, rss_before);
}

// ─── MongoDB — the control ───────────────────────────────────────────────────

/// Mongo has NOTHING to spill, and that is the point of running it here.
///
/// Every event is its own commit (`single_event_commit`), so the sink may roll after
/// any of them and never holds a transaction. Its peak RSS should therefore stay
/// flat as `RIVET_SOAK_ROWS` grows, where PostgreSQL/MySQL/SQL Server climb — the
/// two numbers together are what make "the sink is the binding constraint" a
/// measurement instead of a claim.
#[test]
#[ignore = "soak: requires docker compose up -d mongo-rs"]
fn soak_spill_mongo() {
    require_alive(LiveService::MongoRs);
    require_alive(LiveService::DuckDb);
    let (cycles, rows) = (soak_cycles(), soak_rows());
    let rss_before = peak_child_rss_bytes();

    let db = unique_name("soak_spill_mg");
    let m = MongoTest::connect(27018, &db);
    m.drop_collection("t");

    let rig = Rig::mongo_cdc("t")
        .source_url(&MongoTest::url(27018, &db))
        .census_oracle();
    // Pin the anchor over the empty collection first: Mongo resumes from a token,
    // and a cycle seeded before the anchor exists is a cycle the stream never sees.
    rig.run_ok();

    let mut seeded = 0usize;
    for cycle in 1..=cycles {
        m.append_padded("t", seeded + 1..=seeded + rows, soak_pad());
        seeded += rows;
        m.append_padded("t", seeded + 1..=seeded + SMALL, soak_pad());
        seeded += SMALL;

        let out = run_cycle(&rig);
        assert!(
            out.status.success(),
            "mongo cycle {cycle} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_cycle_spilled("mongo", cycle, &String::from_utf8_lossy(&out.stderr));
    }
    assert_soak_is_sound(&rig, "mongo", seeded, rss_before);
}
