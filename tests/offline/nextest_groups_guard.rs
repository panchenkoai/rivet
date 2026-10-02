//! The `toxiproxy` nextest test-group names exactly the live tests that take `toxiproxy_guard()`.

use std::collections::BTreeSet;

/// `module::test` for every `#[test]` under tests/live whose body calls `toxiproxy_guard()`.
fn guard_users(root: &std::path::Path) -> BTreeSet<String> {
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
            if body.contains("toxiproxy_guard()") {
                out.insert(format!("{stem}::{name}"));
            }
        }
    }
    out
}

/// `module::test` for every `test(=…)` in the override that assigns `test-group = 'toxiproxy'`.
fn grouped(toml: &str) -> BTreeSet<String> {
    let block = toml
        .split("[[profile.default.overrides]]")
        .find(|b| b.contains("test-group = 'toxiproxy'"))
        .expect("an override assigns the toxiproxy test-group");
    block
        .split("test(=")
        .skip(1)
        .map(|s| s.split(')').next().unwrap().to_string())
        .collect()
}

#[test]
fn the_toxiproxy_test_group_names_every_flock_user_and_nothing_else() {
    let root = super::nonvacuity::repo_root();
    let users = guard_users(&root);
    assert!(
        users.len() >= 2,
        "found {} toxiproxy_guard users — the scan is broken",
        users.len()
    );
    let toml = std::fs::read_to_string(root.join(".config/nextest.toml")).expect("nextest.toml");
    assert_eq!(
        grouped(&toml),
        users,
        "a test that takes toxiproxy_guard() must sit in the `toxiproxy` test-group (.config/nextest.toml), \
         or it waits on the flock inside its own timeout"
    );
}
