//! A fix cell must fail on the previous release. The stage, its derivation and its
//! shrink-only ledger live in `dev/release_oracle/fix_cells.py`; this holds the gate to running it.

#[test]
fn the_fix_cell_stage_grades_its_own_fixtures() {
    let out = std::process::Command::new("python3")
        .args(["-m", "dev.release_oracle.fix_cells", "--self-test"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("python3 is a prerequisite of every gate script in this repo");
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        "self-test ok: a fix cell that passes on the previous release fails the stage; a listed control does not",
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn the_gate_stage_table_runs_the_fix_cell_stage() {
    let main = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("dev/release_oracle/__main__.py"),
    )
    .expect("read the gate driver");
    let staged = main
        .lines()
        .filter(|l| l.trim_start().starts_with("table.append("))
        .filter(|l| l.contains("fix_cells.verify_fix_cells(led)"))
        .count();
    assert_eq!(
        staged, 1,
        "the gate's stage table must run fix_cells.verify_fix_cells exactly once: the module's \
         own `__main__` is a call site too, so the call-site guard cannot see the stage missing"
    );
}
