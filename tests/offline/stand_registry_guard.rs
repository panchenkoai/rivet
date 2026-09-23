//! dev/stand/registry.yaml is the one list of stand names; a hard-coded copy must agree with it or not exist.

#[path = "../common/env.rs"]
mod env;
#[path = "../common/registry.rs"]
mod registry;

use std::path::Path;

/// Every file under `dir` with one of `exts`, recursively, skipping build and scratch trees.
fn files(dir: &Path, exts: &[&str], out: &mut Vec<std::path::PathBuf>) {
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        let name = e.file_name().to_string_lossy().into_owned();
        if p.is_dir() {
            if !matches!(
                name.as_str(),
                "target" | ".live-tmp" | "__pycache__" | "node_modules"
            ) {
                files(&p, exts, out);
            }
        } else if exts.iter().any(|x| name.ends_with(x)) {
            out.push(p);
        }
    }
}

#[test]
fn env_rs_endpoints_are_the_registry_endpoints() {
    let pairs = [
        ("postgres", env::POSTGRES_URL),
        ("postgres_cdc", env::POSTGRES_CDC_URL),
        ("mysql", env::MYSQL_URL),
        ("mysql_cdc", env::MYSQL_CDC_URL),
        ("mssql", env::MSSQL_URL),
        ("mssql_cdc", env::MSSQL_CDC_URL),
        ("mongo", env::MONGO_URL),
        ("mongo_rs", env::MONGO_RS_URL),
    ];
    for (source, hard_coded) in pairs {
        assert_eq!(
            registry::stand_url(source),
            hard_coded,
            "tests/common/env.rs disagrees with dev/stand/registry.yaml for `{source}`"
        );
    }
}

#[test]
fn no_bigquery_dataset_name_is_spelled_outside_the_registry() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut all = Vec::new();
    for dir in ["tests", "dev"] {
        files(&root.join(dir), &[".rs", ".py", ".sh"], &mut all);
    }
    all.push(root.join("Makefile"));
    let spelled = regex::Regex::new(
        r#"["'=]\s*(rivet_(e2e|matrix|blessed[a-z_]*|same_[a-z_]+|partner_[a-z_]+|tmp_[a-z_]*))\b"#,
    )
    .unwrap();
    let tmp = registry::stand()["bigquery"]["tmp_prefix"]
        .as_str()
        .unwrap();
    let mut offenders = Vec::new();
    for f in all
        .into_iter()
        .filter(|f| !f.ends_with("dev/pytools/registry.py"))
    {
        let text = std::fs::read_to_string(&f).unwrap_or_default();
        for (n, line) in text.lines().enumerate() {
            for c in spelled.captures_iter(line) {
                let name = &c[1];
                // The Makefile may default the gate dataset, but only to a disposable name.
                let allowed = f.ends_with("Makefile") && name.starts_with(tmp);
                if !allowed {
                    offenders.push(format!(
                        "{}:{}: {name}",
                        f.strip_prefix(root).unwrap().display(),
                        n + 1
                    ));
                }
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "BigQuery dataset names belong in dev/stand/registry.yaml (read them via tests/common/registry.rs or \
         dev/pytools/registry.py):\n{}",
        offenders.join("\n")
    );
}

#[test]
fn the_harness_names_no_endpoint_of_its_own() {
    let text =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/harness/mod.rs"))
            .unwrap();
    assert!(
        !text.contains("127.0.0.1:") && !text.contains("\"rivet_bench\""),
        "tests/harness/mod.rs must read hosts, ports and databases from dev/stand/registry.yaml"
    );
}

#[test]
fn the_python_harness_runs_the_pinned_duckdb_never_a_binary_on_path() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut all = Vec::new();
    files(&root.join("dev"), &[".py"], &mut all);
    let bare = regex::Regex::new(r#"[\[(,]\s*"duckdb"\s*,|which\(\s*"duckdb"\s*\)"#).unwrap();
    let offenders: Vec<String> = all
        .iter()
        .filter(|f| !f.ends_with("dev/pytools/duckcli.py"))
        .flat_map(|f| {
            let text = std::fs::read_to_string(f).unwrap_or_default();
            text.lines()
                .enumerate()
                .filter(|(_, l)| bare.is_match(l))
                .map(|(n, _)| format!("{}:{}", f.strip_prefix(root).unwrap().display(), n + 1))
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(
        offenders.is_empty(),
        "run DuckDB through dev/pytools/duckcli.py (`*DUCKDB`, the uv-pinned package), not the `duckdb` on PATH:\n{}",
        offenders.join("\n")
    );
}
