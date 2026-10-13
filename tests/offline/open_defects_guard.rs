//! An open defect is held to the ledger line it was recorded with. The command and its
//! matching live in `dev/release_oracle/open_defects.py`; this runs its self-test.

#[test]
fn the_open_defect_check_grades_its_own_fixtures() {
    let out = std::process::Command::new("python3")
        .args(["-m", "dev.release_oracle.open_defects", "--self-test"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("python3 is a prerequisite of every gate script in this repo");
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        "self-test ok: a cell red for another reason, a fixed one and one without an entry each fail; a cell not run is listed",
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}
