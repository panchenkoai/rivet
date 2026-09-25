//! Live E2E tests for `rivet init` flag variants not covered by `live_init.rs`.
//!
//! | ID | Flag | Test |
//! |---|---|---|
//! | IE1 | `--source-env ENV` | `init_source_env_reads_url_from_env_var` |
//! | IE2 | `--source-file PATH` | `init_source_file_reads_url_from_file` |
//! | IE3 | `--schema public` | `init_schema_flag_filters_to_schema` |
//! | IE4 | `--discover` | `init_discover_flag_emits_json_artifact` |

use crate::common::*;

// ─── IE1: --source-env reads the DB URL from an environment variable ───────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn init_source_env_reads_url_from_env_var() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(5);

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "init",
            "--source-env",
            "RIVET_TEST_DB_URL",
            "--table",
            table.name(),
        ])
        .env("RIVET_TEST_DB_URL", POSTGRES_URL)
        .output()
        .expect("spawn rivet init --source-env");

    assert!(
        result.status.success(),
        "init --source-env must exit 0 when env var is set; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let yaml = String::from_utf8_lossy(&result.stdout);
    assert!(
        yaml.contains(table.name()),
        "emitted YAML must contain the seeded table name; got:\n{yaml}"
    );
    // With --source-env the scaffold embeds url_env reference, not plaintext URL.
    assert!(
        yaml.contains("url_env"),
        "scaffold must use url_env when built from --source-env; got:\n{yaml}"
    );
}

// ─── IE2: --source-file reads the DB URL from a file ──────────────────────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn init_source_file_reads_url_from_file() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(5);
    let url_file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(url_file.path(), POSTGRES_URL).expect("write url file");

    let result = std::process::Command::new(RIVET_BIN)
        .args([
            "init",
            "--source-file",
            url_file.path().to_str().unwrap(),
            "--table",
            table.name(),
        ])
        .output()
        .expect("spawn rivet init --source-file");

    assert!(
        result.status.success(),
        "init --source-file must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let yaml = String::from_utf8_lossy(&result.stdout);
    assert!(
        yaml.contains(table.name()),
        "emitted YAML must contain the seeded table name; got:\n{yaml}"
    );
    assert!(
        yaml.contains("type: postgres"),
        "emitted YAML must have type: postgres; got:\n{yaml}"
    );
}

// ─── IE3: --schema filters schema-wide discovery to the named schema ──────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn init_schema_flag_filters_to_schema() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(5);

    // --schema public is the default but passing it explicitly must still work.
    let result = std::process::Command::new(RIVET_BIN)
        .args(["init", "--source", POSTGRES_URL, "--schema", "public"])
        .output()
        .expect("spawn rivet init --schema public");

    assert!(
        result.status.success(),
        "init --schema public must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let yaml = String::from_utf8_lossy(&result.stdout);
    assert!(
        yaml.contains("exports:"),
        "schema-scoped init must produce exports section; got:\n{yaml}"
    );
    assert!(
        yaml.contains(table.name()),
        "seeded table must appear in the scaffold; got:\n{yaml}"
    );
}

// ─── IE4: --discover emits a machine-readable JSON discovery artifact ──────────

#[test]
#[ignore = "live: requires docker compose postgres"]
fn init_discover_flag_emits_json_artifact() {
    require_alive(LiveService::Postgres);

    let table = seed_pg_numeric_table(5);

    let result = std::process::Command::new(RIVET_BIN)
        .args(["init", "--source", POSTGRES_URL, "--discover"])
        .output()
        .expect("spawn rivet init --discover");

    assert!(
        result.status.success(),
        "init --discover must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let stdout = String::from_utf8_lossy(&result.stdout);
    let json: serde_json::Value =
        serde_json::from_str(stdout.trim()).expect("--discover output must be valid JSON");

    assert_eq!(
        json["source_type"].as_str().unwrap_or(""),
        "postgres",
        "discovery JSON must have source_type = 'postgres'"
    );
    assert!(
        json["tables"].is_array(),
        "discovery JSON must have a 'tables' array"
    );

    // Seeded table must appear in the tables list.
    let tables = json["tables"].as_array().unwrap();
    let found = tables
        .iter()
        .any(|t| t["table"].as_str().unwrap_or("") == table.name());
    assert!(
        found,
        "seeded table '{}' must appear in discovery JSON tables; got: {:?}",
        table.name(),
        tables
            .iter()
            .filter_map(|t| t["table"].as_str())
            .collect::<Vec<_>>()
    );

    // Each table entry must have suggested_mode and cursor_candidates.
    let entry = tables
        .iter()
        .find(|t| t["table"].as_str().unwrap_or("") == table.name())
        .unwrap();
    assert!(
        entry["suggested_mode"].is_string(),
        "each table must have suggested_mode; got:\n{entry}"
    );
    assert!(
        entry["cursor_candidates"].is_array(),
        "each table must have cursor_candidates array; got:\n{entry}"
    );
}

// ─── IE5: --source-env with unset var → non-zero exit ─────────────────────────

#[test]
#[ignore = "live: requires docker compose (any)"]
fn init_source_env_unset_exits_nonzero() {
    require_alive(LiveService::Postgres);

    let result = std::process::Command::new(RIVET_BIN)
        .args(["init", "--source-env", "RIVET_DEFINITELY_NOT_SET_XYZ"])
        .output()
        .expect("spawn rivet init --source-env unset");

    assert!(
        !result.status.success(),
        "init --source-env with unset var must exit non-zero"
    );
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("RIVET_DEFINITELY_NOT_SET_XYZ")
            || stderr.contains("not set")
            || stderr.contains("Error"),
        "error must mention the missing env var; got:\n{stderr}"
    );
}

// ─── IE7: the cloud-destination scaffolding flags, and the constraints on them ──

/// `rivet init --s3-bucket/--s3-region` and `--gcs-bucket/--gcs-credentials-file`
/// — four flags with ZERO references anywhere in the tree, found by deriving the
/// flag list from `args.rs` rather than reading the list I remembered.
///
/// What they produce is the destination block of a config a user then runs
/// unmodified, so a wrong scaffold is a wrong export. And they carry clap
/// constraints (`conflicts_with`, `requires`) that fail SILENTLY when wrong: a
/// dropped `requires` lets `--gcs-credentials-file` scaffold without a bucket,
/// and a dropped `conflicts_with` lets S3 and GCS both be named, where which one
/// wins is invisible.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn init_cloud_destination_flags_scaffold_and_exclude_each_other() {
    require_alive(LiveService::Postgres);
    let _table = seed_pg_numeric_table(5);

    let init = |extra: &[&str]| -> std::process::Output {
        let mut args = vec!["init", "--source", POSTGRES_URL];
        args.extend_from_slice(extra);
        std::process::Command::new(RIVET_BIN)
            .args(&args)
            .output()
            .expect("spawn rivet init")
    };

    // S3: the scaffold must name the backend AND carry the region it was given —
    // asserting only "s3" would pass on a scaffold that dropped the region and
    // left the export pointing at the wrong endpoint.
    let s3 = init(&[
        "--s3-bucket",
        "qa-scaffold-bucket",
        "--s3-region",
        "eu-central-1",
    ]);
    assert!(
        s3.status.success(),
        "init --s3-bucket must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&s3.stderr)
    );
    let yaml = String::from_utf8_lossy(&s3.stdout);
    assert!(
        yaml.contains("type: s3") && yaml.contains("qa-scaffold-bucket"),
        "the scaffold must declare an s3 destination with the given bucket; got:\n{yaml}"
    );
    assert!(
        yaml.contains("eu-central-1"),
        "--s3-region must reach the scaffold — a dropped region silently points the export \
         at the default endpoint; got:\n{yaml}"
    );

    // GCS: the twin backend.
    let gcs = init(&["--gcs-bucket", "qa-scaffold-gcs"]);
    assert!(
        gcs.status.success(),
        "init --gcs-bucket must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&gcs.stderr)
    );
    let yaml = String::from_utf8_lossy(&gcs.stdout);
    assert!(
        yaml.contains("type: gcs") && yaml.contains("qa-scaffold-gcs"),
        "the scaffold must declare a gcs destination with the given bucket; got:\n{yaml}"
    );

    // The constraints. Both are `assert!(!success)` on purpose: the failure mode
    // they guard is a SILENT resolution, not an error message.
    let both = init(&["--s3-bucket", "a", "--gcs-bucket", "b"]);
    assert!(
        !both.status.success(),
        "naming an S3 AND a GCS bucket must be REFUSED — otherwise one wins and which is \
         invisible in the scaffold"
    );
    let creds_alone = init(&["--gcs-credentials-file", "/tmp/does-not-matter.json"]);
    assert!(
        !creds_alone.status.success(),
        "--gcs-credentials-file without --gcs-bucket must be REFUSED (clap `requires`) — a \
         credentials path with no bucket scaffolds a destination that cannot resolve"
    );
}

/// `rivet init --bigquery-project/--bigquery-dataset` — the pair that decides whether the
/// generated config carries a `load:` block at all.
///
/// Without them init scaffolds an extract and nothing else; with them the same command
/// produces the whole cycle `rivet load` and `rivet compact` run from, including the
/// per-table partition guess. That guess is load-bearing: a part written past a load job's
/// partition budget cannot be loaded at any granularity, so the block these two flags emit
/// is what the writer's budget is measured against.
///
/// The two carry `requires` on each other, and a dropped `requires` fails SILENTLY in the
/// worst direction — a `load:` block naming a project with no dataset (or the reverse)
/// scaffolds a target that cannot resolve, discovered only when a load is attempted
/// against durable artifacts.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn init_bigquery_flags_scaffold_the_load_block_and_require_each_other() {
    require_alive(LiveService::Postgres);
    let _table = seed_pg_numeric_table(5);

    let init = |extra: &[&str]| -> std::process::Output {
        let mut args = vec!["init", "--source", POSTGRES_URL];
        args.extend_from_slice(extra);
        std::process::Command::new(RIVET_BIN)
            .args(&args)
            .output()
            .expect("spawn rivet init")
    };

    // The pair: the scaffold must name the warehouse AND carry both halves it was given.
    // Asserting only "bigquery" would pass on a block that dropped the dataset and points
    // the load at nothing.
    // `--gcs-bucket` rides along: the load reads GCS only, so a `load:` block over a
    // local destination is a config `rivet load` refuses — init refuses first.
    let both = init(&[
        "--bigquery-project",
        "qa-scaffold-project",
        "--bigquery-dataset",
        "qa_scaffold_dataset",
        "--gcs-bucket",
        "qa-scaffold-bucket",
    ]);
    assert!(
        both.status.success(),
        "init --bigquery-project/--bigquery-dataset --gcs-bucket must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&both.stderr)
    );
    let yaml = String::from_utf8_lossy(&both.stdout);
    assert!(
        yaml.contains("target: bigquery"),
        "the scaffold must declare the warehouse target; got:\n{yaml}"
    );
    assert!(
        yaml.contains("qa-scaffold-project") && yaml.contains("qa_scaffold_dataset"),
        "both halves must reach the scaffold — a dropped one leaves a load target that \
         cannot resolve; got:\n{yaml}"
    );

    // The same command WITHOUT them scaffolds an extract only. This is the half that keeps
    // the assertion above honest: without it the test would pass on a scaffold that emits
    // the load block unconditionally, which is a different product.
    let neither = init(&[]);
    assert!(
        neither.status.success(),
        "init with no warehouse flags must still exit 0; stderr:\n{}",
        String::from_utf8_lossy(&neither.stderr)
    );
    assert!(
        !String::from_utf8_lossy(&neither.stdout).contains("target: bigquery"),
        "a config scaffolded with no warehouse flags must carry no load target"
    );

    // The constraints, both `assert!(!success)`: what they guard is a half-populated
    // block, not an error message.
    let project_alone = init(&["--bigquery-project", "qa-scaffold-project"]);
    assert!(
        !project_alone.status.success(),
        "--bigquery-project without --bigquery-dataset must be REFUSED (clap `requires`) — \
         a project with no dataset names no table the load could create"
    );
    let dataset_alone = init(&["--bigquery-dataset", "qa_scaffold_dataset"]);
    assert!(
        !dataset_alone.status.success(),
        "--bigquery-dataset without --bigquery-project must be REFUSED (clap `requires`) — \
         a dataset with no project is not a resolvable target"
    );
    let no_bucket = init(&[
        "--bigquery-project",
        "qa-scaffold-project",
        "--bigquery-dataset",
        "qa_scaffold_dataset",
    ]);
    assert!(
        !no_bucket.status.success(),
        "--bigquery-* without --gcs-bucket must be REFUSED — the scaffold would pair a \
         `load:` block with a local destination, which `rivet load` refuses"
    );
}

/// `rivet init --clickhouse-url/--clickhouse-database[/--clickhouse-user]` scaffold a
/// ClickHouse `load:` block carrying every value it was given; each half without the
/// other, the user without a URL, a missing bucket and a second warehouse are refused.
#[test]
#[ignore = "live: requires docker compose postgres"]
fn init_clickhouse_flags_scaffold_the_load_block_and_require_each_other() {
    require_alive(LiveService::Postgres);
    let _table = seed_pg_numeric_table(5);
    let init = |extra: &[&str]| -> std::process::Output {
        let mut args = vec!["init", "--source", POSTGRES_URL];
        args.extend_from_slice(extra);
        std::process::Command::new(RIVET_BIN)
            .args(&args)
            .output()
            .expect("spawn rivet init")
    };
    let full = [
        "--clickhouse-url",
        "http://ch.example:8123",
        "--clickhouse-database",
        "qa_raw",
        "--gcs-bucket",
        "qa-scaffold-bucket",
    ];

    let named = init(&[&full[..], &["--clickhouse-user", "qa_loader"]].concat());
    assert!(
        named.status.success(),
        "init with the ClickHouse flags must exit 0; stderr:\n{}",
        String::from_utf8_lossy(&named.stderr)
    );
    let yaml = String::from_utf8_lossy(&named.stdout);
    for want in [
        "target: clickhouse",
        "url: http://ch.example:8123",
        "database: qa_raw",
        "user: qa_loader",
        "password_env: CLICKHOUSE_PASSWORD",
    ] {
        assert!(yaml.contains(want), "missing `{want}` in:\n{yaml}");
    }
    let defaulted = String::from_utf8_lossy(&init(&full).stdout).to_string();
    assert!(
        defaulted.contains("user: default"),
        "no --clickhouse-user scaffolds ClickHouse's own default user:\n{defaulted}"
    );
    let neither = init(&[]);
    assert!(
        neither.status.success()
            && !String::from_utf8_lossy(&neither.stdout).contains("clickhouse"),
        "no warehouse flags, no load block — and the user's default must not demand a URL"
    );

    for (refused, why) in [
        (
            &["--clickhouse-url", "http://x:8123", "--gcs-bucket", "b"][..],
            "a URL with no database",
        ),
        (
            &["--clickhouse-database", "d", "--gcs-bucket", "b"][..],
            "a database with no URL",
        ),
        (&["--clickhouse-user", "u"][..], "a user with no URL"),
        (
            &[
                "--clickhouse-url",
                "http://x:8123",
                "--clickhouse-database",
                "d",
            ][..],
            "no GCS bucket to load from",
        ),
        (
            &[
                &full[..],
                &["--bigquery-project", "p", "--bigquery-dataset", "d"],
            ]
            .concat()[..],
            "two warehouses at once",
        ),
    ] {
        assert!(
            !init(refused).status.success(),
            "must refuse {why}: {refused:?}"
        );
    }
}
