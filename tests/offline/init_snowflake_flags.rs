//! `rivet init --snowflake-*` refusals through the real binary. Every one fires
//! before init connects, so these need no stand (`.invalid` never resolves, RFC 6761).
//! The accepted path — the generated `load:` block parsing into a Snowflake target —
//! is pinned by `init::yaml_scaffold::load_block_tests` on the scaffold itself.

const SRC: [&str; 4] = [
    "--source",
    "postgresql://u:p@nosuch-host.invalid:5432/db",
    "--tls",
    "disable",
];

const SF: [&str; 10] = [
    "--snowflake-connection",
    "my_conn",
    "--snowflake-warehouse",
    "COMPUTE_WH",
    "--snowflake-database",
    "ANALYTICS",
    "--snowflake-schema",
    "PUBLIC",
    "--snowflake-storage-integration",
    "MY_GCS_INT",
];

fn init(extra: &[&str]) -> (Option<i32>, String) {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_rivet"))
        .arg("init")
        .args(SRC)
        .args(extra)
        .output()
        .expect("spawn rivet");
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// Each combination that cannot become a loadable Snowflake config is a usage
/// error naming the flag at fault, before any connection is attempted.
#[test]
fn init_refuses_every_snowflake_flag_combination_that_cannot_load() {
    let gcs = ["--gcs-bucket", "b"];
    let cases: Vec<(Vec<&str>, &str)> = vec![
        (SF.to_vec(), "--gcs-bucket"),
        ([&SF[..], &["--s3-bucket", "b"]].concat(), "--s3-bucket"),
        (
            [&gcs[..], &SF[..8]].concat(),
            "--snowflake-storage-integration",
        ),
        ([&gcs[..], &SF[2..]].concat(), "--snowflake-connection"),
        (
            [
                &gcs[..],
                &SF[..],
                &["--bigquery-project", "p", "--bigquery-dataset", "d"],
            ]
            .concat(),
            "--bigquery-project",
        ),
        (
            [
                &gcs[..],
                &SF[..],
                &[
                    "--clickhouse-url",
                    "http://ch:8123",
                    "--clickhouse-database",
                    "d",
                ],
            ]
            .concat(),
            "--clickhouse-url",
        ),
    ];
    for (args, names) in cases {
        let (code, stderr) = init(&args);
        assert_eq!(code, Some(2), "clap usage error for {args:?}:\n{stderr}");
        assert!(
            stderr.contains(names),
            "{args:?} must name {names}:\n{stderr}"
        );
        assert!(
            !stderr.contains("nosuch-host"),
            "refused before connecting: {stderr}"
        );
    }

    let mut blank = [&gcs[..], &SF[..]].concat();
    blank[7] = " ";
    let (code, stderr) = init(&blank);
    assert_eq!(code, Some(1), "{stderr}");
    assert!(
        stderr.contains("RIVET_CLI_VALUE_INVALID") && stderr.contains("--snowflake-database"),
        "a blank value is a coded refusal naming the flag:\n{stderr}"
    );
}
