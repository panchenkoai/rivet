//! Four configs, ONE export name (`users`), ONE shared Postgres state DB, run AT
//! THE SAME TIME — every engine the stand has (MySQL, PostgreSQL, SQL Server,
//! MongoDB), each into its own destination prefix and its own BigQuery dataset.
//! The shape a shared deployment takes: the same config template rolled out per
//! database, the state backend shared because that is what the docs say to do.
//!
//! Two cycles, both blessed then crashed:
//!   * CDC — anchor + backfill (a `full` recipe) → load → a delta → load → every
//!     stream crashes AT ONCE on the CDC leg → plain run → load → then the streams
//!     crash ONE AFTER ANOTHER while the others run clean → run → load. Each check
//!     of the warehouse first runs `rivet compact` when a buffer exists — the
//!     cycle the partner runs (`run → load → compact`).
//!   * batch (`mode: full`) — run → load → more rows → run → load → all crash at
//!     once after a part is written → run → load.
//!
//! What is graded, per engine: the warehouse equals the source (count, one row per
//! key, and SUM(id) — ids are offset per engine so a row routed under another
//! config's prefix shows), through `bq`; and the state DB says what happened:
//! one `cdc_snapshot` row per PREFIX (not per name), a per-run load spec for every
//! successful run, a `run_status` history whose newest row per prefix is `success`
//! and whose crash rows are not. Oracles share nothing with rivet's write path.
//!
//! Needs the CDC stands, `RIVET_TEST_STATE_URL` (Postgres) and the warehouse env
//! (`BIGQUERY_TEST_PROJECT`, `RIVET_TEST_GCS_BUCKET`); SKIPS without them.

use crate::common::*;
use std::path::PathBuf;

const EXPORT: &str = "users";

/// One engine's leg of the fleet: its scenario (table + executor + guards), its
/// warehouse handle and the ids it owns (`k*100 + n`).
struct Leg {
    engine: &'static str,
    k: i64,
    scn: CdcScenario,
    bq: BqLive,
    _cleanup: BqCleanup,
    /// Run ids this leg's SUCCESSFUL runs produced, read from `.rivet/runs`.
    seen_runs: Vec<String>,
}

fn state_url() -> Option<String> {
    let url = std::env::var("RIVET_TEST_STATE_URL").ok()?;
    if !url.starts_with("postgres") {
        skip_live("shared-state: RIVET_TEST_STATE_URL is not Postgres — nothing to share");
        return None;
    }
    Some(url)
}

/// A per-engine dataset, recreated empty, so four same-named exports can each
/// own their table and a crashed earlier invocation leaves nothing behind.
fn dataset_for(bq: &BqLive, engine: &str) -> BqLive {
    let dataset = stand_bq_tmp(&format!("same_{engine}"));
    recreate_dataset(&bq.project, &dataset);
    BqLive {
        project: bq.project.clone(),
        dataset,
        bucket: bq.bucket.clone(),
        prefix: format!("{}/{engine}", bq.prefix),
        owned: true,
    }
}

fn ids(k: i64, from: i64, to: i64) -> Vec<i64> {
    (from..=to).map(|n| k * 100 + n).collect()
}

/// The legs of the fleet, shaped by `shape` (CDC or batch), seeded with five rows each.
fn fleet(bq: &BqLive, shape: fn(Rig, &str, &BqLive, &str) -> Rig) -> Vec<Leg> {
    let mut legs = Vec::new();
    let cols = "id BIGINT PRIMARY KEY, v INT";
    for (k, engine) in ["mysql", "postgres", "mssql", "mongo"]
        .into_iter()
        .enumerate()
    {
        let k = k as i64;
        let ebq = dataset_for(bq, engine);
        let label = format!("same_{engine}");
        let scn = match engine {
            "mysql" => CdcScenario::mysql_with(&label, cols, |r, t| shape(r, t, &ebq, "id")),
            "postgres" => CdcScenario::pg_with(&label, cols, |r, t| shape(r, t, &ebq, "id")),
            "mssql" => CdcScenario::mssql_with(&label, "id INT PRIMARY KEY, v INT", |r, t| {
                shape(r, t, &ebq, "id")
            }),
            _ => CdcScenario::mongo_with(&label, |r, t| shape(r, t, &ebq, "_id")),
        };
        // The warehouse table is named after the SOURCE table, not the export.
        let cleanup = ebq.cleanup(&[&scn.table, &format!("{}__changes", scn.table)]);
        legs.push(Leg {
            engine,
            k,
            scn,
            bq: ebq,
            _cleanup: cleanup,
            seen_runs: Vec::new(),
        });
    }
    for leg in &mut legs {
        for id in ids(leg.k, 1, 5) {
            leg.scn.insert(id);
        }
        leg.scn.settle();
    }
    legs
}

/// The CDC shape: the stream `users` over the table, a `full` recipe as its
/// baseline, a live GCS prefix and a `load:` keyed on the engine's key.
fn cdc_shape(rig: Rig, table: &str, bq: &BqLive, pk: &str) -> Rig {
    rig.export_named(EXPORT)
        .cdc("backfill: auto")
        .also_batch_export("baseline", table, "full")
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(&format!(", pk: [{pk}]")))
}

/// The batch shape: the same export name as a `mode: full` snapshot — the
/// standard SQL approach — into its own prefix and dataset.
fn batch_shape(rig: Rig, _table: &str, bq: &BqLive, _pk: &str) -> Rig {
    rig.export_named(EXPORT)
        .into_full_batch()
        .dest_gcs_live(&bq.bucket, &bq.prefix)
        .top_line(&bq.load_line(""))
}

/// Every leg's `rivet <sub>` AT ONCE, with the shared state and `extra` env on
/// each; returns the outputs in leg order. Only paths and strings cross the
/// thread boundary — the scenarios stay on this thread.
fn all_at_once(
    legs: &[Leg],
    sub: &str,
    state: &str,
    extra: &[(&str, &str)],
) -> Vec<std::process::Output> {
    let cfgs: Vec<PathBuf> = legs.iter().map(|l| l.scn.rig.config_path()).collect();
    let timed: Vec<(std::process::Output, f64)> = std::thread::scope(|s| {
        let handles: Vec<_> = cfgs
            .iter()
            .map(|cfg| {
                let cfg = cfg.to_string_lossy().to_string();
                s.spawn(move || {
                    let mut envs: Vec<(&str, &str)> = vec![("RIVET_STATE_URL", state)];
                    envs.extend_from_slice(extra);
                    let t0 = std::time::Instant::now();
                    let out = run_rivet_env(&[sub, "-c", &cfg], &envs);
                    (out, t0.elapsed().as_secs_f64())
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("leg thread"))
            .collect()
    });
    let per_leg: Vec<String> = legs
        .iter()
        .zip(&timed)
        .map(|(l, (_, secs))| format!("{}={secs:.1}s", l.engine))
        .collect();
    eprintln!(
        "[timing] rivet {sub} x{}: {}",
        legs.len(),
        per_leg.join(" ")
    );
    timed.into_iter().map(|(out, _)| out).collect()
}

fn assert_all_ok(legs: &[Leg], outs: &[std::process::Output], step: &str) {
    for (leg, out) in legs.iter().zip(outs) {
        // Always shown: a step that exits 0 and does nothing ("up to date",
        // "nothing to load") is the shape a silent defect takes here.
        let tail = |b: &[u8]| {
            let t = String::from_utf8_lossy(b);
            t.lines()
                .rev()
                .take(6)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect::<Vec<_>>()
                .join("\n  | ")
        };
        eprintln!(
            "[{step}] {}: exit={}\n  | {}\n  | {}",
            leg.engine,
            out.status,
            tail(&out.stdout),
            tail(&out.stderr)
        );
        assert!(
            out.status.success(),
            "{step}: {} failed:\n{}{}",
            leg.engine,
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

fn assert_all_crashed(legs: &[Leg], outs: &[std::process::Output], step: &str) {
    for (leg, out) in legs.iter().zip(outs) {
        assert!(
            !out.status.success(),
            "{step}: {} must have crashed on the injected fault — exit 0 means the hook saw nothing:\n{}",
            leg.engine,
            String::from_utf8_lossy(&out.stdout)
        );
    }
}

/// Run ids of the runs recorded beside a leg's config — what a run writes on its
/// own host whatever the destination, so the state oracle is joined by RUN ID.
fn run_ids_beside(cfg: &std::path::Path) -> Vec<String> {
    let runs = cfg
        .parent()
        .expect("config dir")
        .join(".rivet")
        .join("runs");
    let mut out = Vec::new();
    if let Ok(dirs) = std::fs::read_dir(&runs) {
        for d in dirs.flatten() {
            let summary = d.path().join("summary.json");
            if let Ok(text) = std::fs::read_to_string(&summary)
                && let Ok(v) = serde_json::from_str::<serde_json::Value>(&text)
                && v["status"].as_str() == Some("success")
                // The stream's own runs: a baseline LEG (`users__snapshot_<table>`)
                // is a read recipe and records no spec by design.
                && v["export_name"].as_str() == Some(EXPORT)
                && let Some(id) = v["run_id"].as_str()
            {
                out.push(id.to_string());
            }
        }
    }
    out
}

fn remember_runs(legs: &mut [Leg]) {
    for leg in legs.iter_mut() {
        for id in run_ids_beside(&leg.scn.rig.config_path()) {
            if !leg.seen_runs.contains(&id) {
                leg.seen_runs.push(id);
            }
        }
    }
}

/// Every leg's live warehouse table equals its source: count, one row per key,
/// SUM(id) — the last catches a row routed under another leg's prefix. All legs
/// at once: each owns its dataset, and a check (a `rivet compact` plus three reads)
/// is ~14 s, so four in turn were half of the CDC cycle. A failing leg's panic is
/// re-raised as is.
fn assert_legs_are_source(legs: &mut [Leg], step: &str) {
    let want: Vec<(i64, String, String)> = legs
        .iter_mut()
        .map(|l| (l.scn.count(), l.scn.pk().to_string(), l.scn.table.clone()))
        .collect();
    let got: Vec<(i64, i64, i64)> = std::thread::scope(|s| {
        let handles: Vec<_> = legs
            .iter()
            .zip(&want)
            .map(|(l, (_, pk, wh))| {
                let (cfg, bq, engine) = (l.scn.rig.config_path(), &l.bq, l.engine);
                s.spawn(move || warehouse_counts(&cfg, bq, engine, wh, pk, step))
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap_or_else(|p| std::panic::resume_unwind(p)))
            .collect()
    });
    for ((leg, (source, _, _)), (n, d, s)) in legs.iter().zip(&want).zip(got) {
        assert_eq!(
            n, *source,
            "{step}: {}: warehouse rows must equal the source",
            leg.engine
        );
        assert_eq!(d, n, "{step}: {}: one row per key", leg.engine);
        // Every id this leg owns lies in [k*100+1, k*100+99]; a foreign row would
        // move the sum off that band.
        let lo = leg.k * 100;
        assert!(
            s > lo * n && s <= (lo + 99) * n,
            "{step}: {}: SUM(id)={s} over {n} rows is not this engine's band ({lo}+1..{lo}+99) — \
             a sibling config's rows landed here",
            leg.engine
        );
    }
}

/// `(rows, distinct keys, SUM(key))` of a leg's live warehouse table, compacting
/// first whenever a buffer exists (a CDC leg's cycle ends with `rivet compact`).
fn warehouse_counts(
    cfg: &std::path::Path,
    bq: &BqLive,
    engine: &str,
    wh: &str,
    pk: &str,
    step: &str,
) -> (i64, i64, i64) {
    if bq.read_bq_table_type(&format!("{wh}__changes")).is_some() {
        // The same shared state every other step of the leg runs against.
        let state = state_url().expect("the shared state URL that admitted this test");
        let cfg = cfg.to_string_lossy();
        let out = run_rivet_env(&["compact", "-c", &cfg], &[("RIVET_STATE_URL", &state)]);
        assert!(
            out.status.success(),
            "{step}: {engine}: rivet compact failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let flagged = !bq
        .read_bq_rows(&format!(
            "SELECT column_name FROM `{}.{}.INFORMATION_SCHEMA.COLUMNS` \
             WHERE table_name = '{wh}' AND column_name = '__is_deleted'",
            bq.project, bq.dataset
        ))
        .is_empty();
    let live = if flagged {
        "WHERE NOT __is_deleted"
    } else {
        ""
    };
    let row = &bq.read_bq_rows(&format!(
        "SELECT COUNT(*) AS n, COUNT(DISTINCT {pk}) AS d, \
         IFNULL(SUM(SAFE_CAST({pk} AS INT64)), 0) AS s FROM `{}.{}.{wh}` {live}",
        bq.project, bq.dataset
    ))[0];
    let num = |k: &str| -> i64 { row[k].as_str().expect("count").parse().expect("a count") };
    (num("n"), num("d"), num("s"))
}

/// The state DB's account of the fleet, read with a plain Postgres client.
struct StateOracle {
    client: postgres::Client,
}

impl StateOracle {
    fn connect(url: &str) -> Self {
        Self {
            client: postgres::Client::connect(url, postgres::NoTls).expect("connect shared state"),
        }
    }

    /// `cdc_snapshot` rows for `users` under this leg's prefix.
    fn snapshot_rows(&mut self, leg: &Leg) -> i64 {
        self.client
            .query_one(
                "SELECT COUNT(*) FROM cdc_snapshot WHERE export_name = $1 AND prefix LIKE $2",
                &[&EXPORT, &format!("%{}%", leg.bq.prefix)],
            )
            .expect("cdc_snapshot")
            .get(0)
    }

    /// How many of `runs` recorded a per-run load spec for `users`.
    fn specs_for(&mut self, runs: &[String]) -> i64 {
        self.client
            .query_one(
                "SELECT COUNT(DISTINCT run_id) FROM export_load_spec_run \
                 WHERE export_name = $1 AND run_id = ANY($2)",
                &[&EXPORT, &runs],
            )
            .expect("export_load_spec_run")
            .get(0)
    }

    /// `(status, run_id)` of every `run_status` row under this leg's prefix, oldest first.
    fn statuses(&mut self, leg: &Leg) -> Vec<(String, String)> {
        self.client
            .query(
                "SELECT status, run_id FROM run_status WHERE prefix LIKE $1 ORDER BY started_at",
                &[&format!("%{}%", leg.bq.prefix)],
            )
            .expect("run_status")
            .iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect()
    }
}

/// After every cycle: the state says one baseline per PREFIX, a spec per
/// successful run, and the newest run per prefix succeeded.
fn assert_state_accounts_for(oracle: &mut StateOracle, legs: &[Leg], step: &str, cdc: bool) {
    for leg in legs {
        if cdc {
            assert_eq!(
                oracle.snapshot_rows(leg),
                1,
                "{step}: {}: exactly one baseline row under this config's prefix — the four \
                 same-named streams must not share one",
                leg.engine
            );
        }
        assert_eq!(
            oracle.specs_for(&leg.seen_runs) as usize,
            leg.seen_runs.len(),
            "{step}: {}: every successful run records its own load spec (runs: {:?})",
            leg.engine,
            leg.seen_runs
        );
        let statuses = oracle.statuses(leg);
        assert_eq!(
            statuses.last().map(|(s, _)| s.as_str()),
            Some("success"),
            "{step}: {}: the newest run under this prefix must be a success: {statuses:?}",
            leg.engine
        );
    }
}

fn crash_rows_present(oracle: &mut StateOracle, leg: &Leg, step: &str) {
    let statuses = oracle.statuses(leg);
    assert!(
        statuses.iter().any(|(s, _)| s != "success"),
        "{step}: {}: the crashed run must have left a non-success row (running/failed): {statuses:?}",
        leg.engine
    );
}

fn cdc_cycle(mut legs: Vec<Leg>, state: &str) {
    let mut oracle = StateOracle::connect(state);

    // 1. Anchor + baseline in every config at once, then every load at once.
    assert_all_ok(&legs, &all_at_once(&legs, "run", state, &[]), "run 1");
    remember_runs(&mut legs);
    assert_all_ok(&legs, &all_at_once(&legs, "load", state, &[]), "load 1");
    assert_legs_are_source(&mut legs, "cdc run 1");
    assert_state_accounts_for(&mut oracle, &legs, "cdc run 1", true);

    // 2. A delta everywhere → run all → load all.
    for leg in legs.iter_mut() {
        for id in ids(leg.k, 6, 8) {
            leg.scn.insert(id);
        }
        leg.scn.update(leg.k * 100 + 1);
        leg.scn.delete(leg.k * 100 + 2);
        leg.scn.settle();
    }
    assert_all_ok(&legs, &all_at_once(&legs, "run", state, &[]), "run 2");
    remember_runs(&mut legs);
    assert_all_ok(&legs, &all_at_once(&legs, "load", state, &[]), "load 2");
    assert_legs_are_source(&mut legs, "cdc run 2");
    assert_state_accounts_for(&mut oracle, &legs, "cdc run 2", true);

    // 3. EVERY stream crashes at once after flushing, before acking; the plain
    //    runs that follow re-read the un-acked changes, and the view stays exact.
    for leg in legs.iter_mut() {
        for id in ids(leg.k, 9, 10) {
            leg.scn.insert(id);
        }
        leg.scn.settle();
    }
    let crashed = all_at_once(
        &legs,
        "run",
        state,
        &[("RIVET_TEST_PANIC_AT", "cdc_after_flush_before_ack")],
    );
    assert_all_crashed(&legs, &crashed, "simultaneous crash");
    for leg in &legs {
        crash_rows_present(&mut oracle, leg, "simultaneous crash");
    }
    assert_all_ok(
        &legs,
        &all_at_once(&legs, "run", state, &[]),
        "run 3 (recovery)",
    );
    remember_runs(&mut legs);
    assert_all_ok(&legs, &all_at_once(&legs, "load", state, &[]), "load 3");
    assert_legs_are_source(&mut legs, "cdc run 3 after a simultaneous crash");
    assert_state_accounts_for(&mut oracle, &legs, "cdc run 3", true);

    // 4. The streams crash ONE AT A TIME while the others run clean — the shared
    //    state must not let a neighbour's success or crash change this run's verdict.
    for victim in 0..legs.len() {
        for leg in legs.iter_mut() {
            leg.scn.insert(leg.k * 100 + 11 + victim as i64);
            leg.scn.settle();
        }
        let cfgs: Vec<PathBuf> = legs.iter().map(|l| l.scn.rig.config_path()).collect();
        let outs: Vec<std::process::Output> = std::thread::scope(|s| {
            let handles: Vec<_> = cfgs
                .iter()
                .enumerate()
                .map(|(i, cfg)| {
                    let cfg = cfg.to_string_lossy().to_string();
                    s.spawn(move || {
                        let mut envs: Vec<(&str, &str)> = vec![("RIVET_STATE_URL", state)];
                        if i == victim {
                            envs.push(("RIVET_TEST_PANIC_AT", "cdc_after_flush_before_ack"));
                        }
                        run_rivet_env(&["run", "-c", &cfg], &envs)
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("leg thread"))
                .collect()
        });
        for (i, (leg, out)) in legs.iter().zip(&outs).enumerate() {
            if i == victim {
                assert!(
                    !out.status.success(),
                    "victim {} must crash on the injected fault",
                    leg.engine
                );
            } else {
                assert!(
                    out.status.success(),
                    "{} must run clean while {} crashes:\n{}",
                    leg.engine,
                    legs[victim].engine,
                    String::from_utf8_lossy(&out.stderr)
                );
            }
        }
        assert_all_ok(
            &legs,
            &all_at_once(&legs, "run", state, &[]),
            &format!("recovery after {} crashed", legs[victim].engine),
        );
    }
    remember_runs(&mut legs);
    assert_all_ok(&legs, &all_at_once(&legs, "load", state, &[]), "load 4");
    assert_legs_are_source(&mut legs, "cdc after crashes in turn");
    assert_state_accounts_for(&mut oracle, &legs, "cdc after crashes in turn", true);
}

fn batch_cycle(mut legs: Vec<Leg>, state: &str) {
    let mut oracle = StateOracle::connect(state);

    // 1. Every full snapshot at once, then every load at once.
    assert_all_ok(&legs, &all_at_once(&legs, "run", state, &[]), "batch run 1");
    remember_runs(&mut legs);
    assert_all_ok(
        &legs,
        &all_at_once(&legs, "load", state, &[]),
        "batch load 1",
    );
    assert_legs_are_source(&mut legs, "batch run 1");
    assert_state_accounts_for(&mut oracle, &legs, "batch run 1", false);

    // 2. More rows → run all → load all: the full load overwrites with the latest run.
    for leg in legs.iter_mut() {
        for id in ids(leg.k, 6, 8) {
            leg.scn.insert(id);
        }
        leg.scn.settle();
    }
    assert_all_ok(&legs, &all_at_once(&legs, "run", state, &[]), "batch run 2");
    remember_runs(&mut legs);
    assert_all_ok(
        &legs,
        &all_at_once(&legs, "load", state, &[]),
        "batch load 2",
    );
    assert_legs_are_source(&mut legs, "batch run 2");
    assert_state_accounts_for(&mut oracle, &legs, "batch run 2", false);

    // 3. Every snapshot crashes at once after a part is written; the plain runs
    //    that follow re-read the table, the loads take the latest COMPLETE run.
    for leg in legs.iter_mut() {
        for id in ids(leg.k, 9, 10) {
            leg.scn.insert(id);
        }
        leg.scn.settle();
    }
    let crashed = all_at_once(
        &legs,
        "run",
        state,
        &[("RIVET_TEST_PANIC_AT", "after_file_write")],
    );
    assert_all_crashed(&legs, &crashed, "batch simultaneous crash");
    for leg in &legs {
        crash_rows_present(&mut oracle, leg, "batch simultaneous crash");
    }
    assert_all_ok(
        &legs,
        &all_at_once(&legs, "run", state, &[]),
        "batch run 3 (recovery)",
    );
    remember_runs(&mut legs);
    assert_all_ok(
        &legs,
        &all_at_once(&legs, "load", state, &[]),
        "batch load 3",
    );
    assert_legs_are_source(&mut legs, "batch run 3 after a simultaneous crash");
    assert_state_accounts_for(&mut oracle, &legs, "batch run 3", false);
}

#[test]
#[ignore = "live: requires every CDC stand + a Postgres state URL + BigQuery creds"]
fn same_named_configs_share_a_postgres_state_cdc_cycle() {
    let Some(bq) = BqLive::from_env("same_cdc") else {
        return;
    };
    let Some(state) = state_url() else {
        return;
    };
    require_alive(LiveService::MongoRs);
    let _serial = cross_process_serial("mssql_cdc");
    let legs = fleet(&bq, cdc_shape);
    cdc_cycle(legs, &state);
}

#[test]
#[ignore = "live: requires every CDC stand + a Postgres state URL + BigQuery creds"]
fn same_named_configs_share_a_postgres_state_batch_cycle() {
    let Some(bq) = BqLive::from_env("same_batch") else {
        return;
    };
    let Some(state) = state_url() else {
        return;
    };
    require_alive(LiveService::MongoRs);
    let _serial = cross_process_serial("mssql_cdc");
    let legs = fleet(&bq, batch_shape);
    batch_cycle(legs, &state);
}
