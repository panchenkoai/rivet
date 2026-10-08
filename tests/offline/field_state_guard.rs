//! Upgrade from a field state: the state an old release wrote by real runs, carried on by this build.
//! The stage and its graders live in `dev/release_oracle/field_state.py`; this holds the gate to
//! running it and to fetching the old binaries where the stage looks for them.

fn repo_file(path: &str) -> String {
    std::fs::read_to_string(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(path))
        .unwrap_or_else(|e| panic!("read {path}: {e}"))
}

#[test]
fn the_field_state_stage_grades_its_own_fixtures() {
    let out = std::process::Command::new("python3")
        .args(["-m", "dev.release_oracle.field_state", "--self-test"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("python3 is a prerequisite of every gate script in this repo");
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        "self-test ok: a re-baseline, a restart, a duplicate, a loss and a config that no longer loads each fail their row",
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn the_gate_stage_table_runs_the_field_state_stage() {
    let main = repo_file("dev/release_oracle/__main__.py");
    let staged = main
        .lines()
        .filter(|l| {
            l.trim_start()
                .starts_with("Stage(\"upgrade from a field state\"")
        })
        .filter(|l| l.contains("field_state.verify_upgrade_from_field_state(led)"))
        .count();
    assert_eq!(
        staged, 1,
        "the gate's stage table must run field_state.verify_upgrade_from_field_state exactly once: the \
         module's own `__main__` is a call site too, so the call-site guard cannot see the stage missing"
    );
}

#[test]
fn the_compatibility_floor_moves_only_with_this_guard() {
    let stage = repo_file("dev/release_oracle/field_state.py");
    assert!(
        stage.contains("\nCOMPATIBILITY_FLOOR = \"0.27.0\"\n"),
        "the compatibility floor is release 0.27.0 and moves only on the owner's word: change it here \
         and in field_state.py together"
    );
    assert!(
        stage.contains("if want[:1] != [COMPATIBILITY_FLOOR]:"),
        "the stage must refuse to grade a release list that does not start at the floor"
    );
}

#[test]
fn the_baseline_download_fetches_the_field_releases_beside_the_previous_one() {
    let makefile = repo_file("Makefile");
    let recipe = makefile
        .split("\nrelease-oracle-prev-bin:")
        .nth(1)
        .and_then(|rest| rest.split("\nrelease-oracle-full:").next())
        .expect("the Makefile has a release-oracle-prev-bin recipe before release-oracle-full");
    assert!(
        recipe.contains("-m dev.release_oracle.field_state --fetch $(PREV_RELEASE_DIR)/field"),
        "release-oracle-prev-bin must fetch the field releases into $(PREV_RELEASE_DIR)/field: the \
         stage reads them from `field/` beside the previous release's binary and fails without them"
    );
    assert!(
        repo_file("dev/release_oracle/field_state.py")
            .contains("return prev.parent.parent / \"field\""),
        "field_state.field_cache must read the directory the Makefile fetches into"
    );
}
