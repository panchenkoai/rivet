//! The `rivet cdc` subcommand's flags on PostgreSQL, SQL Server and MongoDB
//! (docs/cdc-cli-surface-matrix.yaml); MySQL and Oracle keep their own cells.

use crate::common::*;

/// A fresh `--output` directory the rig oracle can read, as `(path, its text)`.
fn shared_out(label: &str) -> (std::path::PathBuf, String) {
    let (host, _) = live_shared_workdir(&unique_name(label));
    let text = host.to_str().expect("utf-8 path").to_string();
    (host, text)
}

/// `--source-env` and `--source-file` each resolve to the stream `--source` names: one new change per form, one row declared for it (the rig oracle grades each run's values against the source).
fn source_env_and_file_resolve_the_stream(mut s: CdcScenario) {
    const VAR: &str = "RIVET_TEST_CDC_CLI_SURFACE_URL";
    let url = s.rig.cdc_source_url().to_string();
    let keep = tempfile::tempdir().unwrap();
    let url_file = keep.path().join("url.txt");
    std::fs::write(&url_file, &url).unwrap();
    let url_file = url_file.to_str().unwrap().to_string();

    let (host, out) = shared_out("cli_src_anchor");
    s.rig.cli_cdc(true, None, &["--output", &out], &[]);
    assert_eq!(
        dir_manifest_copy_total_rows(&host),
        0,
        "fixture: nothing changed before the anchor run"
    );
    let mut one_change_through = |id: i64, form: [&str; 2], envs: &[(&str, &str)]| {
        s.insert(id);
        s.settle();
        let (host, out) = shared_out("cli_src");
        s.rig.cli_cdc(true, Some(&form), &["--output", &out], envs);
        assert_eq!(
            dir_manifest_copy_total_rows(&host),
            1,
            "`{}` must read the same stream and deliver only the change made since the last drain",
            form[0]
        );
    };
    one_change_through(1, ["--source-env", VAR], &[(VAR, url.as_str())]);
    one_change_through(2, ["--source-file", url_file.as_str()], &[]);
}

/// `--output --format csv --max-events 2 --stream` (and `--rollover <n>` when given) over three single-row commits: two rows now, a part per `n` rows, the third on the following drain.
fn capped_csv_drain_defers_the_rest(mut s: CdcScenario, rollover: Option<&str>) {
    let (host, out) = shared_out("cli_csv");
    let sink = ["--output", out.as_str(), "--format", "csv"];
    s.rig.cli_cdc(true, None, &sink, &[]);
    assert_eq!(
        dir_manifest_copy_total_rows(&host),
        0,
        "fixture: nothing changed before the anchor run"
    );
    for id in 1..=3 {
        s.insert(id);
    }
    s.settle();
    let capped: Vec<&str> = sink
        .iter()
        .copied()
        .chain(["--max-events", "2", "--stream"])
        .chain(rollover.iter().flat_map(|n| ["--rollover", *n]))
        .collect();
    s.rig.cli_cdc(true, None, &capped, &[]);
    assert_eq!(
        dir_manifest_copy_total_rows(&host),
        2,
        "`--max-events 2` stops a `--stream` drain after two single-row commits"
    );
    if let Some(n) = rollover {
        assert!(
            files_with_extension(&host, "csv").len() >= 2,
            "`--rollover {n}` cuts a part per commit: {:?}",
            files_with_extension(&host, "csv")
        );
    }
    assert!(
        files_with_extension(&host, "parquet").is_empty(),
        "`--format csv` writes no parquet"
    );
    // The uncapped drain is the one the rig oracle grades against the source.
    s.rig.cli_cdc(true, None, &sink, &[]);
    assert_eq!(
        dir_manifest_copy_total_rows(&host),
        3,
        "the capped-off commit arrives on the following drain"
    );
}

fn pg(label: &str) -> CdcScenario {
    CdcScenario::pg_with(label, "id BIGINT PRIMARY KEY, v BIGINT", |r, _| {
        r.relative_checkpoint("cdc.ckpt")
    })
}

fn mssql(label: &str) -> CdcScenario {
    CdcScenario::mssql_with(label, "id BIGINT PRIMARY KEY, v BIGINT", |r, t| {
        r.repoint(&format!("dbo.{t}"))
    })
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn pg_cdc_cli_resolves_the_source_from_env_and_file_alike() {
    source_env_and_file_resolve_the_stream(pg("cli_src"));
}

#[test]
#[ignore = "live: requires docker compose mssql (CDC)"]
fn mssql_cdc_cli_resolves_the_source_from_env_and_file_alike() {
    let _serial = cross_process_serial("mssql_cdc");
    source_env_and_file_resolve_the_stream(mssql("cli_src"));
}

#[test]
#[ignore = "live: requires docker compose mongo-rs"]
fn mongo_cdc_cli_resolves_the_source_from_env_and_file_alike() {
    source_env_and_file_resolve_the_stream(CdcScenario::mongo_with("cli_src", |r, _| r));
}

#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn pg_cdc_cli_writes_csv_parts_and_stops_at_the_cap_on_a_commit() {
    capped_csv_drain_defers_the_rest(pg("cli_csv"), None);
}

/// The same drain at `--rollover 1`: the pending commits must still be delivered.
#[test]
#[ignore = "live+gate-only: docker compose --profile cdc postgres-cdc; open defect (rivet cdc --rollover 1 delivers nothing on PostgreSQL), acknowledged in dev/release_oracle/known_red.py"]
fn open_defect_pg_cdc_cli_rollover_1_delivers_the_pending_changes() {
    capped_csv_drain_defers_the_rest(pg("cli_roll1"), Some("1"));
}

#[test]
#[ignore = "live: requires docker compose mssql (CDC)"]
fn mssql_cdc_cli_writes_csv_parts_and_stops_at_the_cap_on_a_commit() {
    let _serial = cross_process_serial("mssql_cdc");
    capped_csv_drain_defers_the_rest(mssql("cli_csv"), Some("1"));
}

#[test]
#[ignore = "live: requires docker compose mongo-rs"]
fn mongo_cdc_cli_writes_csv_parts_and_stops_at_the_cap_on_a_commit() {
    capped_csv_drain_defers_the_rest(CdcScenario::mongo_with("cli_csv", |r, _| r), Some("1"));
}

/// PostgreSQL anchors on the slot; `--checkpoint` is the prior-run evidence that turns a dropped slot into a refusal instead of a fresh anchor.
#[test]
#[ignore = "live: requires docker compose --profile cdc postgres-cdc"]
fn pg_cdc_cli_checkpoint_file_turns_a_dropped_slot_into_a_refusal() {
    let mut s = pg("cli_ckpt");
    let (host, out) = shared_out("cli_ckpt");
    let sink = ["--output", out.as_str()];
    s.rig.cli_cdc(true, None, &sink, &[]);
    s.insert(1);
    s.rig.cli_cdc(true, None, &sink, &[]);
    assert_eq!(cdc_id_ops(&host), vec![(1, "insert".to_string())]);
    assert!(
        s.rig.checkpoint().is_file(),
        "`--checkpoint` must write the file it names: {}",
        s.rig.checkpoint().display()
    );
    let argv = s.rig.cdc_cli_argv(true);
    let slot = argv[argv.iter().position(|a| a == "--slot").expect("--slot") + 1].clone();
    s.sql(&format!("SELECT pg_drop_replication_slot('{slot}')"));
    s.insert(2);
    let argv: Vec<&str> = argv.iter().map(String::as_str).chain(sink).collect();
    let refused = run_rivet(&argv);
    let err = String::from_utf8_lossy(&refused.stderr);
    assert!(
        !refused.status.success()
            && err.contains("is missing but the checkpoint file holds a position from a prior run"),
        "a dropped slot with a checkpoint file must be refused, not re-anchored past id 2 (exit {:?}):\n{err}",
        refused.status.code()
    );
    assert_eq!(
        cdc_id_ops(&host),
        vec![(1, "insert".to_string())],
        "the refused drain delivers nothing"
    );
}
