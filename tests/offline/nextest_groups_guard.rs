//! Each flock-backed nextest test-group names exactly the live tests that take its flock.

use std::collections::BTreeSet;

/// (test-group, the call that takes its cross-process flock)
const GROUPS: &[(&str, &str)] = &[
    ("toxiproxy", "toxiproxy_guard()"),
    ("mssql_cdc", "cross_process_serial(\"mssql_cdc\")"),
    ("oracle_cdc", "cross_process_serial(\"oracle_cdc\")"),
    ("mysql_replica", "cross_process_serial(\"mysql_replica\")"),
];

/// `module::test` for every `#[test]` under tests/live whose body holds `call`.
fn guard_users(root: &std::path::Path, call: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for e in std::fs::read_dir(root.join("tests/live"))
        .expect("read tests/live")
        .flatten()
    {
        let p = e.path();
        let (Some(stem), Some("rs")) = (
            p.file_stem().and_then(|s| s.to_str()),
            p.extension().and_then(|s| s.to_str()),
        ) else {
            continue;
        };
        let text = std::fs::read_to_string(&p).expect("read a live test");
        for body in text.split("#[test]").skip(1) {
            let name = body
                .split_once("fn ")
                .and_then(|(_, rest)| rest.split_once('('))
                .map(|(n, _)| n.trim().to_string())
                .expect("a #[test] is followed by its fn");
            if body.contains(call) {
                out.insert(format!("{stem}::{name}"));
            }
        }
    }
    out
}

/// `module::test` for every `test(=…)` in the override that assigns `test-group = '<group>'`.
fn grouped(toml: &str, group: &str) -> BTreeSet<String> {
    let assign = format!("test-group = '{group}'");
    let block = toml
        .split("[[profile.default.overrides]]")
        .find(|b| b.contains(&assign))
        .unwrap_or_else(|| panic!("no override assigns the {group} test-group"));
    block
        .split("test(=")
        .skip(1)
        .map(|s| s.split(')').next().unwrap().to_string())
        .collect()
}

#[test]
fn every_flock_test_group_names_every_flock_user_and_nothing_else() {
    let root = super::nonvacuity::repo_root();
    let toml = std::fs::read_to_string(root.join(".config/nextest.toml")).expect("nextest.toml");
    for (group, call) in GROUPS {
        let users = guard_users(&root, call);
        assert!(
            users.len() >= 2,
            "found {} `{call}` users — the scan is broken",
            users.len()
        );
        assert_eq!(
            grouped(&toml, group),
            users,
            "a test that takes `{call}` must sit in the `{group}` test-group (.config/nextest.toml), \
             or it waits on the flock inside its own timeout, holding a runner slot"
        );
        assert!(
            toml.contains(&format!("{group} = {{ max-threads = 1 }}")),
            "the `{group}` test-group must run one test at a time: its users share one flock"
        );
    }
}
