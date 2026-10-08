//! Every `#[test]` checks something of its own. The scan, its allow-list and its
//! two shrink-only ceilings live in `dev/pytools/test_checks.py`; this runs them.

use std::process::Command;

fn scan(args: &[&str]) -> std::process::Output {
    Command::new("python3")
        .arg("dev/pytools/test_checks.py")
        .args(args)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("python3 is a prerequisite of every gate script in this repo")
}

#[test]
fn the_scan_grades_its_own_fixture() {
    let out = scan(&["--self-test"]);
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        "test_checks self-test: ok",
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn every_test_checks_something_or_is_listed_with_its_reason() {
    let out = scan(&[]);
    let said = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success() && said.contains("test_checks: "),
        "a test with no assertion cannot fail on a wrong answer. Give it one, or list it in \
         NO_CHECK with its reason (dev/pytools/test_checks.py):\n{said}{}",
        String::from_utf8_lossy(&out.stderr)
    );
}
