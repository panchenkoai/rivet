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
//! | `--mode cdc`, one table (`initial: snapshot`) | the table's baseline | only the new changes |
//! | `--mode cdc`, two tables (`backfill: auto`) | every table's baseline | only the new changes |
//!
//! Until 2026-10-01 the one-table row was "nothing": that scaffold carried no
//! baseline, and its first `rivet compact` refused a base never loaded.

use crate::common::*;

/// Sorted `id`s of every part directly under `dir` — an INDEPENDENT read of the
/// parquet, never rivet's own manifest.
fn ids_in(dir: &std::path::Path) -> Vec<i64> {
    use arrow::array::{Array, Int64Array};
    let mut ids = Vec::new();
    for b in read_all_parts(dir) {
        let col = b
            .column_by_name("id")
            .or_else(|| b.column_by_name("ID")) // Oracle's catalog spelling
            .expect("every part carries `id`");
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

/// Run rivet and return what it said, asserting it exited 0.
fn rivet_ok(args: &[&str], envs: &[(&str, &str)]) -> String {
    let out = run_rivet_env(args, envs);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.status.success(), "rivet {:?} failed:\n{said}", args[0]);
    said
}

/// Run rivet in `dir` (a generated config's relative paths resolve there) and return what it said, asserting it exited 0.
fn rivet_ok_in(dir: &std::path::Path, args: &[&str], envs: &[(&str, &str)]) -> String {
    let out = run_rivet_in_dir(dir, args, envs);
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.status.success(), "rivet {:?} failed:\n{said}", args[0]);
    said
}

/// Removes the GCS prefix a GENERATED config writes to. `BqLive::cleanup` cannot:
/// it owns `rivet-live/<uniq>`, while init derives `exports/<table>/` and takes no
/// prefix flag — so without this every run of this test leaks objects into the
/// shared bucket.
struct GcsPrefix(String);
impl Drop for GcsPrefix {
    fn drop(&mut self) {
        let _ = std::process::Command::new("timeout")
            .args(["300", "gcloud", "storage", "rm", "-r", "--quiet", &self.0])
            .output();
    }
}

// ── the warehouse half: init → run → load → compact, per engine ──────────

/// A timestamp type init scores as a cursor candidate, spelled per engine.
fn ts_type(e: SqlEngine) -> &'static str {
    match e {
        SqlEngine::Pg => "TIMESTAMP",
        SqlEngine::Mysql => "DATETIME(6)",
        SqlEngine::Mssql => "DATETIME2(6)",
        #[cfg(feature = "oracle")]
        SqlEngine::Oracle => "TIMESTAMP(6)",
    }
}

/// `ids` as rows `(id, 'r<id>', id*10, created, now)` — a VALUES list, since only
/// PostgreSQL has `generate_series`; `created_days_ago` backdates the business date.
fn insert_rows(
    e: SqlEngine,
    table: &str,
    ids: std::ops::RangeInclusive<i64>,
    created_days_ago: i64,
) {
    let now = e.ago(0);
    let created = e.ago(created_days_ago * 24 * 60);
    let rows: Vec<String> = ids
        .map(|i| format!("({i}, 'r{i}', {}, {created}, {now})", i * 10))
        .collect();
    e.exec(&format!(
        "INSERT INTO {table} (id, name, amount, created_at, changed_at) VALUES {}",
        rows.join(", ")
    ));
}

/// The export name init chose — the warehouse table and the bucket prefix both
/// follow it, so the test reads it back rather than re-deriving init's rule.
fn scaffolded_export(generated: &str) -> String {
    generated
        .lines()
        .find_map(|l| l.trim().strip_prefix("- name:"))
        .map(|s| s.trim().trim_matches('"').to_string())
        .expect("a single-table scaffold names one export")
}

/// The cycle init's own next-steps prescribes, driven end to end on the config it
/// generated: `run` stages Parquet, `load` lands it, `compact` merges the buffer.
///
/// Locked down per engine because the chain had no live test through `compact` on
/// the batch side, and the one defect this branch shipped there — the MERGE handed
/// specs without `__is_deleted`, so no tombstone was ever written — was found by
/// running it by hand, not by the suite. The scaffold's `--table` is `dbo.`-qualified
/// on SQL Server: init's bare-name default is `public`, which that engine lacks.
fn warehouse_chain(e: SqlEngine, label: &str) {
    let Some(bq) = BqLive::from_env(label) else {
        return;
    };
    e.alive();
    let ts = ts_type(e);
    let (table, _table_guard) = e.create(
        label,
        &format!(
            "id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL, amount INT NOT NULL, \
             created_at {ts} NOT NULL, changed_at {ts} NOT NULL"
        ),
    );
    insert_rows(e, &table, 1..=10, 0);

    let dir = tempfile::tempdir().expect("config dir");
    let cfg_path = dir.path().join("rivet.yaml");
    let cfg = cfg_path.to_str().unwrap();
    let init_table = match e {
        SqlEngine::Mssql => format!("dbo.{table}"),
        _ => table.clone(),
    };
    init_ok(&[
        "init",
        "--source",
        e.url(),
        "--table",
        &init_table,
        "--mode",
        "incremental",
        "--bigquery-project",
        &bq.project,
        "--bigquery-dataset",
        &bq.dataset,
        "--gcs-bucket",
        &bq.bucket,
        "--output",
        cfg,
    ]);
    let generated = std::fs::read_to_string(cfg).expect("generated config");
    let export = scaffolded_export(&generated);
    let changes = format!("{export}__changes");
    // Guards before the first write: `run` stages into the bucket, `load` into
    // the dataset, and a panic between them must still tear both down.
    let _bq_guard = bq.cleanup(&[&export, &changes]);
    let _gcs_guard = GcsPrefix(format!("gs://{}/exports/{export}/**", bq.bucket));
    let db = [("DATABASE_URL", e.url())];
    let fq = |t: &str| format!("`{}.{}.{t}`", bq.project, bq.dataset);

    // 1. First pass: the whole table lands as a plain base, and there is no
    //    buffer for compact to merge — said so rather than silently doing work.
    rivet_ok(&["run", "-c", cfg], &db);
    rivet_ok(&["load", "-c", cfg], &[]);
    assert_eq!(
        bq.read_bq_table_type(&export).as_deref(),
        Some("BASE TABLE"),
        "the first incremental pass lands a table, not a view"
    );
    assert_eq!(bq.read_bq_count(&export), "10");
    let said = rivet_ok(&["compact", "-c", cfg], &[]);
    assert!(said.contains("COMPACT SKIP"), "no buffer yet: {said}");

    // 2. A delta of five inserts and one UPDATE — the cursor must carry both. The
    //    whole delta is dated three days back (late-arriving rows; the UPDATE moves
    //    the row's business date), and init partitions the base by `created_at`: the
    //    base holds row 3 under its OLD day, which no buffer row shares, so a
    //    compaction that looks for it only under the buffer's days re-inserts it —
    //    two live rows for one key, from a config init wrote.
    insert_rows(e, &table, 11..=15, 3);
    e.exec(&format!(
        "UPDATE {table} SET amount = 999, created_at = {}, changed_at = {} WHERE id = 3",
        e.ago(3 * 24 * 60),
        e.ago(0)
    ));
    rivet_ok(&["run", "-c", cfg], &db);
    rivet_ok(&["load", "-c", cfg], &[]);
    assert_eq!(
        bq.read_bq_count(&changes),
        "6",
        "five inserts and the update are buffered, not merged"
    );
    assert_eq!(
        bq.read_bq_count(&export),
        "10",
        "the base waits for compact — a load never merges"
    );

    // 3. Compact merges the buffer and drops it; the base equals the source.
    let said = rivet_ok(&["compact", "-c", cfg], &[]);
    assert!(said.contains("COMPACT OK"), "{said}");
    assert_eq!(
        bq.read_bq_count(&export),
        "15",
        "one row per key — the moved row was matched under its old partition, not re-inserted"
    );
    assert!(
        bq.read_bq_table_type(&changes).is_none(),
        "the buffer is dropped after the merge"
    );
    let rows = bq.read_bq_rows(&format!(
        "SELECT COUNT(DISTINCT id) AS d, COUNTIF(id = 3 AND amount = 999) AS updated FROM {}",
        fq(&export)
    ));
    assert_eq!(rows[0]["d"].as_str(), Some("15"), "one row per key");
    assert_eq!(
        rows[0]["updated"].as_str(),
        Some("1"),
        "the UPDATE reached the base through the buffer"
    );
}

#[test]
#[ignore = "live: requires docker compose postgres + BigQuery creds"]
fn a_generated_config_drives_run_load_compact_into_the_warehouse_postgres() {
    warehouse_chain(SqlEngine::Pg, "init_chain_pg");
}

#[test]
#[ignore = "live: requires docker compose mysql + BigQuery creds"]
fn a_generated_config_drives_run_load_compact_into_the_warehouse_mysql() {
    warehouse_chain(SqlEngine::Mysql, "init_chain_my");
}

#[test]
#[ignore = "live: requires docker compose mssql + BigQuery creds"]
fn a_generated_config_drives_run_load_compact_into_the_warehouse_mssql() {
    warehouse_chain(SqlEngine::Mssql, "init_chain_ms");
}

/// A full load whose newest run exported 0 rows (the source was emptied) empties the
/// warehouse table; it used to report "up to date" and keep serving the deleted rows.
#[test]
#[ignore = "live: requires docker compose postgres + BigQuery creds"]
fn a_full_load_of_an_emptied_source_empties_the_warehouse_table() {
    let Some(bq) = BqLive::from_env("init_emptied") else {
        return;
    };
    let e = SqlEngine::Pg;
    e.alive();
    let (table, _table_guard) = e.create("init_emptied", "id BIGINT PRIMARY KEY, v TEXT NOT NULL");
    e.exec(&format!(
        "INSERT INTO {table} (id, v) SELECT g, 'v'||g FROM generate_series(1,5) g"
    ));
    let dir = tempfile::tempdir().expect("config dir");
    let cfg_path = dir.path().join("rivet.yaml");
    let cfg = cfg_path.to_str().unwrap();
    init_ok(&[
        "init",
        "--source",
        POSTGRES_URL,
        "--table",
        &table,
        "--mode",
        "full",
        "--bigquery-project",
        &bq.project,
        "--bigquery-dataset",
        &bq.dataset,
        "--gcs-bucket",
        &bq.bucket,
        "--output",
        cfg,
    ]);
    let export = scaffolded_export(&std::fs::read_to_string(cfg).expect("generated config"));
    let _bq_guard = bq.cleanup(&[&export]);
    let _gcs_guard = GcsPrefix(format!("gs://{}/exports/{export}/**", bq.bucket));
    let db = [("DATABASE_URL", POSTGRES_URL)];

    rivet_ok(&["run", "-c", cfg], &db);
    rivet_ok(&["load", "-c", cfg], &[]);
    assert_eq!(bq.read_bq_count(&export), "5");

    e.exec(&format!("TRUNCATE {table}"));
    rivet_ok(&["run", "-c", cfg], &db);
    let said = rivet_ok(&["load", "-c", cfg], &[]);
    assert!(
        !said.contains("LOAD SKIP"),
        "an emptied source is not 'up to date': {said}"
    );
    assert_eq!(
        bq.read_bq_count(&export),
        "0",
        "the warehouse table matches the source's latest snapshot, which is empty"
    );
}

/// A compaction whose job died after renaming the buffer to `<t>__changes__merging`
/// leaves rows in no base and no buffer; the next `compact` must merge them and drop
/// the leftover. The dead job is reproduced by making that rename by hand.
#[test]
#[ignore = "live: requires docker compose postgres + BigQuery creds"]
fn a_compaction_left_half_done_is_finished_by_the_next_one() {
    let Some(bq) = BqLive::from_env("init_merging") else {
        return;
    };
    let e = SqlEngine::Pg;
    e.alive();
    let (table, _table_guard) = e.create(
        "init_merging",
        // `t` and `s` are the aliases the compaction SQL once used; BigQuery resolved them to these columns.
        "id BIGINT PRIMARY KEY, v TEXT NOT NULL, t TEXT, s TEXT, \
         changed_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    );
    e.exec(&format!(
        "INSERT INTO {table} (id, v) SELECT g, 'v'||g FROM generate_series(1,10) g"
    ));
    let dir = tempfile::tempdir().expect("config dir");
    let cfg_path = dir.path().join("rivet.yaml");
    let cfg = cfg_path.to_str().unwrap();
    init_ok(&[
        "init",
        "--source",
        POSTGRES_URL,
        "--table",
        &table,
        "--mode",
        "incremental",
        "--bigquery-project",
        &bq.project,
        "--bigquery-dataset",
        &bq.dataset,
        "--gcs-bucket",
        &bq.bucket,
        "--output",
        cfg,
    ]);
    let export = scaffolded_export(&std::fs::read_to_string(cfg).expect("generated config"));
    let changes = format!("{export}__changes");
    let merging = format!("{export}__changes__merging");
    let _bq_guard = bq.cleanup(&[&export, &changes, &merging]);
    let _gcs_guard = GcsPrefix(format!("gs://{}/exports/{export}/**", bq.bucket));
    let db = [("DATABASE_URL", POSTGRES_URL)];
    let fq = |t: &str| format!("`{}.{}.{t}`", bq.project, bq.dataset);

    rivet_ok(&["run", "-c", cfg], &db);
    rivet_ok(&["load", "-c", cfg], &[]);
    e.exec(&format!(
        "INSERT INTO {table} (id, v) SELECT g, 'v'||g FROM generate_series(11,12) g"
    ));
    e.exec(&format!(
        "UPDATE {table} SET v = 'upd3', changed_at = now() WHERE id = 3"
    ));
    rivet_ok(&["run", "-c", cfg], &db);
    rivet_ok(&["load", "-c", cfg], &[]);
    assert_eq!(bq.read_bq_count(&changes), "3", "the delta is buffered");
    let renamed = std::process::Command::new("bq")
        .arg(format!("--project_id={}", bq.project))
        .args(["query", "--use_legacy_sql=false"])
        .arg(format!(
            "ALTER TABLE {} RENAME TO `{merging}`",
            fq(&changes)
        ))
        .output()
        .expect("`bq query` must run");
    assert!(
        renamed.status.success(),
        "the dead job's rename could not be reproduced: {}",
        String::from_utf8_lossy(&renamed.stderr)
    );
    assert!(
        bq.read_bq_table_type(&changes).is_none(),
        "the dead job took the buffer's name"
    );

    let said = rivet_ok(&["compact", "-c", cfg], &[]);
    assert!(said.contains("left over from a compaction"), "{said}");
    assert!(
        said.contains("COMPACT OK"),
        "the recovered merge is reported as work: {said}"
    );
    assert!(
        bq.read_bq_table_type(&merging).is_none(),
        "the leftover is dropped"
    );
    let rows = bq.read_bq_rows(&format!(
        "SELECT COUNT(*) AS n, COUNTIF(id = 3 AND v = 'upd3') AS updated FROM {}",
        fq(&export)
    ));
    assert_eq!(
        rows[0]["n"].as_str(),
        Some("12"),
        "the two inserts reached the base"
    );
    assert_eq!(
        rows[0]["updated"].as_str(),
        Some("1"),
        "the UPDATE reached the base"
    );
}

/// One export init cannot give a cursor must not cost the others their recorded key:
/// a whole-schema `--mode incremental` scaffold over a stamped table and a stamp-less
/// one records BOTH primary keys (recording used to validate the whole config first,
/// and the stamp-less export's missing `cursor_column:` left every export keyless).
#[test]
#[ignore = "live: requires docker compose postgres"]
fn an_export_init_cannot_give_a_cursor_does_not_cost_the_others_their_key() {
    let e = SqlEngine::Pg;
    e.alive();
    let (stamped, _g1) = e.create(
        "init_keys_stamped",
        "id BIGINT PRIMARY KEY, changed_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    );
    let (stampless, _g2) = e.create("init_keys_stampless", "code TEXT PRIMARY KEY, v TEXT");
    let dir = tempfile::tempdir().expect("config dir");
    let cfg = dir.path().join("rivet.yaml");
    let out = run_rivet(&[
        "init",
        "--source",
        POSTGRES_URL,
        "--schema",
        "public",
        "--include",
        &stamped,
        &stampless,
        "--mode",
        "incremental",
        "--output",
        cfg.to_str().unwrap(),
    ]);
    let said = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "rivet init failed:\n{said}");
    assert!(
        said.contains(&stampless) && said.contains("have no timestamp column"),
        "the fixture must really leave one export without a cursor: {said}"
    );
    let key = |t: &str| recorded_primary_key(&cfg, t);
    assert_eq!(key(&stamped), Some(vec!["id".to_string()]));
    assert_eq!(key(&stampless), Some(vec!["code".to_string()]));
}

/// A source column spelled with a Cyrillic look-alike (`сomment`, U+0441) lands as
/// `comment` through the base load, the buffer append and the compaction, with no
/// NULL anywhere: BigQuery matches Parquet columns by name, so a rename that only
/// edited the declared schema would load the column NULL with every count green.
#[test]
#[ignore = "live: requires docker compose postgres + BigQuery creds"]
fn a_cyrillic_lookalike_column_lands_under_its_latin_name_with_every_value() {
    let Some(bq) = BqLive::from_env("init_lookalike") else {
        return;
    };
    let e = SqlEngine::Pg;
    e.alive();
    let (table, _table_guard) = e.create(
        "init_lookalike",
        "id BIGINT PRIMARY KEY, \"\u{441}omment\" TEXT NOT NULL, \
         changed_at TIMESTAMPTZ NOT NULL DEFAULT now()",
    );
    e.exec(&format!(
        "INSERT INTO {table} (id, \"\u{441}omment\") SELECT g, 'c'||g FROM generate_series(1,10) g"
    ));

    let dir = tempfile::tempdir().expect("config dir");
    let cfg_path = dir.path().join("rivet.yaml");
    let cfg = cfg_path.to_str().unwrap();
    init_ok(&[
        "init",
        "--source",
        POSTGRES_URL,
        "--table",
        &table,
        "--mode",
        "incremental",
        "--bigquery-project",
        &bq.project,
        "--bigquery-dataset",
        &bq.dataset,
        "--gcs-bucket",
        &bq.bucket,
        "--output",
        cfg,
    ]);
    let export = scaffolded_export(&std::fs::read_to_string(cfg).expect("generated config"));
    let changes = format!("{export}__changes");
    let _bq_guard = bq.cleanup(&[&export, &changes]);
    let _gcs_guard = GcsPrefix(format!("gs://{}/exports/{export}/**", bq.bucket));
    let db = [("DATABASE_URL", POSTGRES_URL)];
    let fq = |t: &str| format!("`{}.{}.{t}`", bq.project, bq.dataset);
    let columns = |t: &str| {
        bq.read_bq_rows(&format!(
            "SELECT column_name FROM `{}.{}`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{t}'",
            bq.project, bq.dataset
        ))
        .iter()
        .filter_map(|r| r["column_name"].as_str().map(str::to_string))
        .collect::<Vec<_>>()
    };
    let nulls = |t: &str| {
        bq.read_bq_rows(&format!(
            "SELECT COUNT(*) AS n, COUNTIF(comment IS NULL) AS nul FROM {}",
            fq(t)
        ))[0]
            .clone()
    };

    rivet_ok(&["run", "-c", cfg], &db);
    let said = rivet_ok(&["load", "-c", cfg], &[]);
    assert!(
        said.contains("loads as `comment`"),
        "the rename is announced: {said}"
    );
    let base_cols = columns(&export);
    assert!(
        base_cols.iter().any(|c| c == "comment") && !base_cols.iter().any(|c| c == "\u{441}omment"),
        "the base carries the Latin name only: {base_cols:?}"
    );
    let base = nulls(&export);
    assert_eq!(
        (base["n"].as_str(), base["nul"].as_str()),
        (Some("10"), Some("0"))
    );

    e.exec(&format!(
        "INSERT INTO {table} (id, \"\u{441}omment\") SELECT g, 'c'||g FROM generate_series(11,15) g"
    ));
    e.exec(&format!(
        "UPDATE {table} SET \"\u{441}omment\" = 'upd3', changed_at = now() WHERE id = 3"
    ));
    rivet_ok(&["run", "-c", cfg], &db);
    rivet_ok(&["load", "-c", cfg], &[]);
    let buffered = nulls(&changes);
    assert_eq!(
        (buffered["n"].as_str(), buffered["nul"].as_str()),
        (Some("6"), Some("0")),
        "the renamed append carries every value into the buffer"
    );

    let said = rivet_ok(&["compact", "-c", cfg], &[]);
    assert!(said.contains("COMPACT OK"), "{said}");
    let rows = bq.read_bq_rows(&format!(
        "SELECT COUNT(*) AS n, COUNTIF(comment IS NULL) AS nul, \
         COUNTIF(id = 3 AND comment = 'upd3') AS updated FROM {}",
        fq(&export)
    ));
    assert_eq!(rows[0]["n"].as_str(), Some("15"));
    assert_eq!(rows[0]["nul"].as_str(), Some("0"));
    assert_eq!(
        rows[0]["updated"].as_str(),
        Some("1"),
        "the UPDATE reached the base"
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
        rivet_ok_in(dir.path(), &["run", "-c", "rivet.yaml"], &db);
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
fn a_generated_single_table_cdc_config_takes_its_baseline_then_only_later_changes() {
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
    let _slot = Slot::new(scaffolded_slot(&generated));
    assert!(
        generated.contains("until_current: true"),
        "an unbounded first run never returns, so there is no second run:\n{generated}"
    );

    let db = [("DATABASE_URL", POSTGRES_CDC_URL)];
    let run = || {
        rivet_ok_in(dir.path(), &["run", "-c", "rivet.yaml"], &db);
    };

    run();
    let out = dir.path().join("output").join(&table).join("cdc");
    assert!(
        parts_in(&out).is_empty(),
        "nothing changed after the anchor, so the STREAM holds no part"
    );
    assert_eq!(
        ids_in(&out.join("snapshot")),
        (1..=10).collect::<Vec<i64>>(),
        "the 10 pre-existing rows are the first run's baseline (`initial: snapshot`)"
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
    assert_eq!(
        ids_in(&out.join("snapshot")),
        (1..=10).collect::<Vec<i64>>(),
        "the baseline is not re-read"
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
    let _slot = Slot::new(scaffolded_slot(&generated));
    assert!(
        generated.contains("backfill: auto"),
        "over two tables the scaffold must carry the baseline recipe — that is the \
         ONLY shape in which a cdc first run takes everything:\n{generated}"
    );

    let db = [("DATABASE_URL", POSTGRES_CDC_URL)];
    let run = || {
        rivet_ok_in(dir.path(), &["run", "-c", "rivet.yaml"], &db);
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

// ── cdc: init --mode cdc → check → run → load → compact, per engine ───────

/// One change a CDC chain applies to its source.
#[derive(Clone, Copy)]
enum Op {
    Ins(i64),
    Rename(i64),
    Del(i64),
}

/// `op` as SQL on `table`, for the SQL engines (`name` is the only non-key column).
fn op_sql(table: &str, op: Op) -> String {
    match op {
        Op::Ins(i) => format!("INSERT INTO {table} (id, name) VALUES ({i}, 'r{i}')"),
        Op::Rename(i) => format!("UPDATE {table} SET name = 'changed' WHERE id = {i}"),
        Op::Del(i) => format!("DELETE FROM {table} WHERE id = {i}"),
    }
}

/// The ten seed rows, then the changes between the two cycles.
fn seed_ops() -> Vec<Op> {
    (1..=10).map(Op::Ins).collect()
}
fn change_ops() -> Vec<Op> {
    vec![
        Op::Ins(11),
        Op::Ins(12),
        Op::Ins(13),
        Op::Rename(2),
        Op::Del(3),
    ]
}

/// Applies a list of changes to a source.
type ApplyOps = Box<dyn Fn(&[Op])>;

/// A source a generated CDC config captures: the URL rivet gets, init's table flags,
/// how a change is applied, and the warehouse's reading of the key.
struct CdcSource {
    url: String,
    init_args: Vec<String>,
    apply: ApplyOps,
    bq_id: &'static str,
    /// The `name` field as the warehouse holds it.
    bq_name: &'static str,
    /// The warehouse table when it is not the export name (SQL Server qualifies it).
    bq_table: Option<String>,
}

/// init `--mode cdc` with a warehouse, then the cycle init's own next steps print:
/// `check`, `run`, `load`, changes, `run`, `load`, `compact`. The base must equal the
/// source afterwards: twelve live keys, the rename merged, the delete flagged.
fn cdc_warehouse_chain(src: CdcSource, bq: &BqLive) {
    (src.apply)(&seed_ops());
    let dir = tempfile::tempdir().expect("config dir");
    let cfg_path = dir.path().join("rivet.yaml");
    let cfg = cfg_path.to_str().unwrap();
    let mut args: Vec<&str> = vec!["init", "--source", &src.url, "--mode", "cdc"];
    args.extend(src.init_args.iter().map(String::as_str));
    args.extend([
        "--bigquery-project",
        &bq.project,
        "--bigquery-dataset",
        &bq.dataset,
        "--gcs-bucket",
        &bq.bucket,
        "--output",
        cfg,
    ]);
    // Graded after the cycle, so a scaffold with no baseline fails on what it does, not what it says.
    let next_steps = rivet_ok(&args, &[]);
    let generated = std::fs::read_to_string(cfg).expect("generated config");
    assert!(generated.contains("\nload:"), "{generated}");
    let export = scaffolded_export(&generated);
    let _gcs_guard = GcsPrefix(format!("gs://{}/exports/{export}/**", bq.bucket));
    let export = src.bq_table.clone().unwrap_or(export);
    let changes = format!("{export}__changes");
    let _bq_guard = bq.cleanup(&[&export, &changes]);
    let _slot = generated
        .contains("slot:")
        .then(|| Slot::new(scaffolded_slot(&generated)));
    let db = [("DATABASE_URL", src.url.as_str())];

    rivet_ok(&["check", "-c", cfg], &db);
    rivet_ok(&["run", "-c", cfg], &db);
    rivet_ok(&["load", "-c", cfg], &[]);
    assert_eq!(
        bq.read_bq_table_type(&export).as_deref(),
        Some("BASE TABLE"),
        "the first run's baseline lands as the base"
    );
    assert_eq!(bq.read_bq_count(&export), "10");

    (src.apply)(&change_ops());
    rivet_ok(&["run", "-c", cfg], &db);
    rivet_ok(&["load", "-c", cfg], &[]);
    let said = rivet_ok(&["compact", "-c", cfg], &[]);
    assert!(said.contains("COMPACT OK"), "{said}");
    assert!(
        bq.read_bq_table_type(&changes).is_none(),
        "the buffer is dropped after the merge"
    );
    let (id, name) = (src.bq_id, src.bq_name);
    let rows = bq.read_bq_rows(&format!(
        "SELECT COUNTIF(NOT __is_deleted) AS live, COUNT(DISTINCT {id}) AS keys, \
         COUNTIF({id} = 3 AND __is_deleted) AS gone, \
         COUNTIF({id} = 2 AND {name} = 'changed' AND NOT __is_deleted) AS renamed \
         FROM `{}.{}.{export}`",
        bq.project, bq.dataset
    ));
    let got = |k: &str| rows[0][k].as_str().map(str::to_string);
    assert_eq!(
        (got("live"), got("keys"), got("gone"), got("renamed")),
        (
            Some("12".into()),
            Some("13".into()),
            Some("1".into()),
            Some("1".into())
        ),
        "base after compact: {rows:?}"
    );
    assert!(
        next_steps.contains("rivet compact -c") && !next_steps.contains("CHANGES ONLY"),
        "init's next steps must prescribe the cycle that just ran:\n{next_steps}"
    );
}

#[test]
#[ignore = "live: requires postgres-cdc + BigQuery creds"]
fn a_generated_cdc_config_drives_run_load_compact_postgres() {
    let Some(bq) = BqLive::from_env("init_cdc_pg") else {
        return;
    };
    let table = unique_name("init_cdc_pg");
    let mut c = postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls).expect("connect");
    c.batch_execute(&format!(
        "CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT NOT NULL)"
    ))
    .unwrap();
    let _t = PgTable::adopt_on(POSTGRES_CDC_URL, table.clone());
    let t = table.clone();
    cdc_warehouse_chain(
        CdcSource {
            url: POSTGRES_CDC_URL.into(),
            init_args: vec!["--table".into(), table],
            apply: Box::new(move |ops| {
                let mut c = postgres::Client::connect(POSTGRES_CDC_URL, postgres::NoTls).unwrap();
                for op in ops {
                    c.batch_execute(&op_sql(&t, *op)).unwrap();
                }
            }),
            bq_id: "id",
            bq_name: "name",
            bq_table: None,
        },
        &bq,
    );
}

#[test]
#[ignore = "live: requires mysql-cdc + BigQuery creds"]
fn a_generated_cdc_config_drives_run_load_compact_mysql() {
    use mysql::prelude::Queryable as _;
    let Some(bq) = BqLive::from_env("init_cdc_my") else {
        return;
    };
    let table = unique_name("init_cdc_my");
    let pool = mysql::Pool::new(MYSQL_CDC_URL).expect("mysql-cdc");
    pool.get_conn()
        .unwrap()
        .query_drop(format!(
            "CREATE TABLE {table} (id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL)"
        ))
        .unwrap();
    let _t = MysqlCdcTable(table.clone());
    let t = table.clone();
    cdc_warehouse_chain(
        CdcSource {
            url: MYSQL_CDC_URL.into(),
            init_args: vec!["--table".into(), table],
            apply: Box::new(move |ops| {
                let mut c = pool.get_conn().unwrap();
                for op in ops {
                    c.query_drop(op_sql(&t, *op)).unwrap();
                }
            }),
            bq_id: "id",
            bq_name: "name",
            bq_table: None,
        },
        &bq,
    );
}

#[test]
#[ignore = "live: requires mssql-cdc + BigQuery creds"]
fn a_generated_cdc_config_drives_run_load_compact_mssql() {
    let Some(bq) = BqLive::from_env("init_cdc_ms") else {
        return;
    };
    let table = unique_name("init_cdc_ms");
    let ci = format!("dbo_{table}");
    mssql_cdc_exec(&format!(
        "CREATE TABLE dbo.{table} (id BIGINT PRIMARY KEY, name NVARCHAR(64) NOT NULL)"
    ));
    let _t = MssqlCdcTable {
        table: table.clone(),
        ci: ci.clone(),
    };
    enable_cdc(&table, &ci);
    let t = format!("dbo.{table}");
    let ci_table = format!("dbo_{table}"); // the load names the warehouse table `<schema>_<table>`
    let captured = std::cell::Cell::new(0i64);
    cdc_warehouse_chain(
        CdcSource {
            url: MSSQL_CDC_URL.into(),
            init_args: vec!["--table".into(), t.clone()],
            apply: Box::new(move |ops| {
                for op in ops {
                    mssql_cdc_exec(&op_sql(&t, *op));
                }
                // An UPDATE is two change rows (before and after image).
                let rows: i64 = ops
                    .iter()
                    .map(|o| if matches!(o, Op::Rename(_)) { 2 } else { 1 })
                    .sum();
                captured.set(captured.get() + rows);
                wait_for_capture(&ci, captured.get());
            }),
            bq_id: "id",
            bq_name: "name",
            bq_table: Some(ci_table),
        },
        &bq,
    );
}

#[test]
#[ignore = "live: requires mongo-rs + BigQuery creds"]
fn a_generated_cdc_config_drives_run_load_compact_mongo() {
    use mongodb::bson::doc;
    let Some(bq) = BqLive::from_env("init_cdc_mg") else {
        return;
    };
    let db = unique_name("init_cdc_mg");
    let _db = MongoDbGuard {
        port: 27018,
        db: db.clone(),
    };
    let m = MongoTest::connect(27018, &db);
    m.create_empty_collection("events");
    cdc_warehouse_chain(
        CdcSource {
            url: MongoTest::url(27018, &db),
            init_args: vec!["--table".into(), "events".into()],
            apply: Box::new(move |ops| {
                for op in ops {
                    match *op {
                        Op::Ins(i) => m.insert_many(
                            "events",
                            vec![doc! { "_id": i, "name": format!("r{i}") }],
                        ),
                        Op::Rename(i) => m.upsert_set("events", i, "name", "changed"),
                        Op::Del(i) => m.delete_one("events", i),
                    }
                }
            }),
            bq_id: "CAST(_id AS INT64)",
            bq_name: "JSON_VALUE(document, '$.name')",
            bq_table: None,
        },
        &bq,
    );
}

#[cfg(feature = "oracle")]
/// Oracle CDC does not load yet (ADR-0037): init writes no `load:` block even with a
/// warehouse named, says why, and the config it writes passes `check` and captures the
/// baseline to Parquet.
#[test]
#[ignore = "live: requires the oracle service with LogMiner prerequisites"]
fn a_generated_oracle_cdc_config_has_no_load_block_and_runs_to_parquet() {
    let _serial = cross_process_serial("oracle_cdc");
    let t = crate::live_cdc_oracledb::cdc_table(
        "init_cdc_ora",
        "id NUMBER(18) PRIMARY KEY, name VARCHAR2(64) NOT NULL",
    );
    for op in seed_ops() {
        ora_exec(&op_sql(t.name(), op));
    }
    let db = [("DATABASE_URL", ORACLE_CDC_URL)];
    let init = |dir: &std::path::Path, warehouse: &[&str]| {
        let mut args = vec![
            "init",
            "--source",
            ORACLE_CDC_URL,
            "--schema",
            "RIVET",
            "--table",
            t.name(),
            "--mode",
            "cdc",
            "--output",
            "rivet.yaml",
        ];
        args.extend(warehouse);
        let said = rivet_ok_in(dir, &args, &[]);
        let generated = std::fs::read_to_string(dir.join("rivet.yaml")).unwrap();
        rivet_ok_in(dir, &["check", "-c", "rivet.yaml"], &db);
        (said, generated)
    };

    // A warehouse named: no `load:` block, the reason in the file, no load promised.
    let wh = tempfile::tempdir().expect("config dir");
    let (said, generated) = init(
        wh.path(),
        &[
            "--gcs-bucket",
            "b",
            "--bigquery-project",
            "p",
            "--bigquery-dataset",
            "d",
        ],
    );
    assert!(
        !said.contains("rivet load    -c") && said.contains("No `load:` block"),
        "init's next steps must not promise a load:\n{said}"
    );
    assert!(
        !generated.contains("\nload:")
            && generated.contains("ADR-0037")
            && generated.contains("initial: snapshot"),
        "{generated}"
    );

    // What still works: run writes the baseline to Parquet.
    let dir = tempfile::tempdir().expect("config dir");
    let (_, generated) = init(dir.path(), &[]);
    rivet_ok_in(dir.path(), &["run", "-c", "rivet.yaml"], &db);
    let snap = dir
        .path()
        .join("output")
        .join(scaffolded_export(&generated))
        .join("cdc")
        .join("snapshot");
    assert_eq!(ids_in(&snap), (1..=10).collect::<Vec<i64>>());
}
