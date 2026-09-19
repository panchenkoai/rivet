//! The take-everything-then-only-the-delta guarantee, on configs `rivet init`
//! GENERATED — never one written here.
//!
//! Every other test of this property renders its YAML from the Rig, so what it
//! proves is the RUNNER's behaviour over a config a test author wrote. The
//! decisions that make the second run a delta are INIT's, though — the cursor
//! column, the resume position, the bounded run — and nothing executed a
//! generated config even once. These tests close that: the config under test
//! comes out of `rivet init` and is run exactly as its own next-steps text
//! tells an operator to run it.
//!
//! Measured, not assumed (2026-09-19, PostgreSQL 16):
//!
//! | scaffold | run 1 | run 2 |
//! |---|---|---|
//! | `--mode incremental` | everything | only the new rows |
//! | `--mode cdc`, one table | **nothing** — the slot anchors at the current WAL | only the new changes |
//! | `--mode cdc`, two tables (`backfill: auto`) | every table's baseline | only the new changes |
//!
//! The middle row is the one to read twice: a single-table CDC scaffold does
//! NOT take everything on the first run, and init says so in its own next-steps
//! ("the baseline is yours"). The guarantee holds for CDC only through the
//! multi-table scaffold's `backfill: auto`.

use crate::common::*;

/// Sorted `id`s of every part directly under `dir` — an INDEPENDENT read of the
/// parquet, never rivet's own manifest.
fn ids_in(dir: &std::path::Path) -> Vec<i64> {
    use arrow::array::{Array, Int64Array};
    let mut ids = Vec::new();
    for b in read_all_parts(dir) {
        let col = b.column_by_name("id").expect("every part carries `id`");
        let a = col
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("`id` is BIGINT, so Int64 on the Arrow side");
        ids.extend(a.iter().flatten());
    }
    ids.sort_unstable();
    ids
}

/// Parquet parts directly under `dir`, asserting the directory EXISTS first:
/// `files_with_extension` answers `[]` for a missing path, which would make
/// every "no parts yet" assertion pass on a typo'd one.
fn parts_in(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    assert!(
        dir.is_dir(),
        "{} must exist — an absent directory is a wrong path, not an empty export",
        dir.display()
    );
    files_with_extension(dir, "parquet")
}

/// The `slot:` the scaffold chose. Read from the generated file rather than
/// rebuilt here: how init sanitises a table name into a slot identifier is its
/// decision, and guessing it would leak a slot on every mismatch.
fn scaffolded_slot(generated: &str) -> String {
    generated
        .lines()
        .find_map(|l| l.trim().strip_prefix("slot:"))
        .and_then(|s| s.split_whitespace().next())
        .expect("a cdc scaffold must name a slot")
        .to_string()
}

fn init_ok(args: &[&str]) {
    let out = run_rivet(args);
    assert!(
        out.status.success(),
        "rivet init failed:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

// ── batch: incremental ────────────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn a_generated_incremental_config_takes_everything_then_only_the_delta() {
    require_alive(LiveService::Postgres);
    let e = SqlEngine::Pg;
    let (table, _guard) = e.create(
        "init_delta_inc",
        "id BIGINT PRIMARY KEY, name TEXT NOT NULL, \
         created_at TIMESTAMPTZ NOT NULL DEFAULT now(), \
         changed_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    );
    e.exec(&format!(
        "INSERT INTO {table} (id, name) SELECT g, 'row'||g FROM generate_series(1,10) g"
    ));

    let dir = tempfile::tempdir().expect("config dir");
    let cfg = dir.path().join("rivet.yaml");
    init_ok(&[
        "init",
        "--source",
        POSTGRES_URL,
        "--table",
        &table,
        "--mode",
        "incremental",
        "--output",
        cfg.to_str().unwrap(),
    ]);

    // The delta MECHANISM is init's own decision. Both stamps are populated
    // here, so picking the create-only one would still export — and then miss
    // every UPDATE for ever. (The scorer shipped exactly that once.)
    let generated = std::fs::read_to_string(&cfg).expect("generated config");
    assert!(
        generated.contains("cursor_column: changed_at"),
        "init must resume from the MUTATION stamp:\n{generated}"
    );

    let db = [("DATABASE_URL", POSTGRES_URL)];
    let run = || {
        let o = run_rivet_in_dir(dir.path(), &["run", "-c", "rivet.yaml"], &db);
        assert!(
            o.status.success(),
            "rivet run failed:\n{}",
            String::from_utf8_lossy(&o.stderr)
        );
    };

    run();
    let out = dir.path().join("output").join(&table);
    assert_eq!(
        ids_in(&out),
        (1..=10).collect::<Vec<i64>>(),
        "run 1 on a generated config must take EVERYTHING"
    );

    e.exec(&format!(
        "INSERT INTO {table} (id, name) SELECT g, 'row'||g FROM generate_series(11,15) g"
    ));
    run();

    let all = ids_in(&out);
    assert_eq!(
        all.len(),
        15,
        "run 2 must append ONLY the delta — a whole-table re-read lands 25 rows"
    );
    assert_eq!(all, (1..=15).collect::<Vec<i64>>());
}

// ── cdc: one table ────────────────────────────────────────────────────────

#[test]
#[ignore = "live: requires docker compose --profile cdc up -d postgres-cdc"]
fn a_generated_single_table_cdc_config_takes_no_baseline_only_later_changes() {
    let table = unique_name("init_delta_cdc");
    let mut c =
        postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls).expect("connect postgres-cdc");
    c.batch_execute(&format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
         INSERT INTO {table} SELECT g, 'row'||g FROM generate_series(1,10) g"
    ))
    .expect("seed the cdc table");
    let _table = PgTable::adopt_on(POSTGRES_CDC_URL, table.clone());

    let dir = tempfile::tempdir().expect("config dir");
    let cfg = dir.path().join("rivet.yaml");
    init_ok(&[
        "init",
        "--source",
        POSTGRES_CDC_URL,
        "--table",
        &table,
        "--mode",
        "cdc",
        "--output",
        cfg.to_str().unwrap(),
    ]);
    let generated = std::fs::read_to_string(&cfg).expect("generated config");
    let _slot = Slot(scaffolded_slot(&generated));
    assert!(
        generated.contains("until_current: true"),
        "an unbounded first run never returns, so there is no second run:\n{generated}"
    );

    let db = [("DATABASE_URL", POSTGRES_CDC_URL)];
    let run = || {
        let o = run_rivet_in_dir(dir.path(), &["run", "-c", "rivet.yaml"], &db);
        assert!(
            o.status.success(),
            "rivet run failed:\n{}",
            String::from_utf8_lossy(&o.stderr)
        );
    };

    run();
    let out = dir.path().join("output").join(&table).join("cdc");
    assert!(
        parts_in(&out).is_empty(),
        "the slot anchors at the CURRENT wal position, so the 10 pre-existing rows \
         are NOT the stream's to capture — init says so in its own next steps"
    );
    // …and no baseline leg ran at all. Without this the test reads as "no
    // baseline" while checking only "no CHANGE parts directly here": a snapshot
    // leg writes to `<dest>/snapshot/` (config::export docs), one level down,
    // where the non-recursive reader above cannot see it. Measured: scaffolding
    // `initial: snapshot` left every assertion green until this line existed.
    assert!(
        !out.join("snapshot").exists(),
        "a single-table cdc scaffold must not carry a baseline leg — the baseline \
         is the operator's, through `initial: snapshot` or a backfill recipe"
    );

    c.batch_execute(&format!(
        "INSERT INTO {table} SELECT g, 'row'||g FROM generate_series(11,15) g"
    ))
    .expect("insert after the anchor");
    run();

    assert_eq!(
        ids_in(&out),
        (11..=15).collect::<Vec<i64>>(),
        "run 2 must hold the changes since the anchor and nothing else"
    );
}

// ── cdc: two tables, the baseline through `backfill: auto` ────────────────

#[test]
#[ignore = "live: requires docker compose --profile cdc up -d postgres-cdc"]
fn a_generated_multi_table_cdc_config_takes_every_baseline_then_only_the_delta() {
    let (a, b) = (unique_name("init_delta_ma"), unique_name("init_delta_mb"));
    let mut c =
        postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls).expect("connect postgres-cdc");
    for (t, n) in [(&a, 10), (&b, 7)] {
        c.batch_execute(&format!(
            "CREATE TABLE {t} (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
             INSERT INTO {t} SELECT g, 'row'||g FROM generate_series(1,{n}) g"
        ))
        .expect("seed");
    }
    let _ta = PgTable::adopt_on(POSTGRES_CDC_URL, a.clone());
    let _tb = PgTable::adopt_on(POSTGRES_CDC_URL, b.clone());

    let dir = tempfile::tempdir().expect("config dir");
    let cfg = dir.path().join("rivet.yaml");
    init_ok(&[
        "init",
        "--source",
        POSTGRES_CDC_URL,
        "--schema",
        "public",
        "--include",
        &a,
        &b,
        "--mode",
        "cdc",
        "--output",
        cfg.to_str().unwrap(),
    ]);
    let generated = std::fs::read_to_string(&cfg).expect("generated config");
    let _slot = Slot(scaffolded_slot(&generated));
    assert!(
        generated.contains("backfill: auto"),
        "over two tables the scaffold must carry the baseline recipe — that is the \
         ONLY shape in which a cdc first run takes everything:\n{generated}"
    );

    let db = [("DATABASE_URL", POSTGRES_CDC_URL)];
    let run = || {
        let o = run_rivet_in_dir(dir.path(), &["run", "-c", "rivet.yaml"], &db);
        assert!(
            o.status.success(),
            "rivet run failed:\n{}",
            String::from_utf8_lossy(&o.stderr)
        );
    };
    let root = dir.path().join("output").join("cdc");
    let snap = |t: &str| root.join(t).join("snapshot");
    let changes = |t: &str| root.join(t);

    run();
    assert_eq!(ids_in(&snap(&a)), (1..=10).collect::<Vec<i64>>());
    assert_eq!(ids_in(&snap(&b)), (1..=7).collect::<Vec<i64>>());
    for t in [&a, &b] {
        assert!(
            parts_in(&changes(t)).is_empty(),
            "run 1 anchors and backfills; nothing has changed since, so the STREAM \
             must hold no part for {t}"
        );
    }

    for (t, r) in [(&a, "11,13"), (&b, "8,10")] {
        let (lo, hi) = r.split_once(',').unwrap();
        c.batch_execute(&format!(
            "INSERT INTO {t} SELECT g, 'row'||g FROM generate_series({lo},{hi}) g"
        ))
        .expect("insert after the anchor");
    }
    run();

    assert_eq!(ids_in(&changes(&a)), (11..=13).collect::<Vec<i64>>());
    assert_eq!(ids_in(&changes(&b)), (8..=10).collect::<Vec<i64>>());
    // …and the baseline is NOT re-read: a second pass over the recipes would
    // double every snapshot, which is what makes this the load-bearing half.
    assert_eq!(ids_in(&snap(&a)), (1..=10).collect::<Vec<i64>>());
    assert_eq!(ids_in(&snap(&b)), (1..=7).collect::<Vec<i64>>());
}
