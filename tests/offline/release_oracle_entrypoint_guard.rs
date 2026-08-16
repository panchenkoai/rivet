//! Drift-guards over the release gate's ENTRY POINTS and the way it launches its
//! child harnesses. Both guards exist because a round-6 bughunt found the same
//! class twice: a rule was taught to ONE of several equivalent sites.
//!
//! 1. `_require_prev_binary` was changed so an absent `RIVET_PREV_RELEASE_BIN` is
//!    a FAIL rather than a SKIP (a check that grades nothing must fail — 0.24.4
//!    shipped a +1h48m governor regression through a green gate whose only
//!    comparison leg SKIPped). The change reached ONE of the three Makefile
//!    entry points: `make release-oracle` and `make release-oracle-bless` then
//!    went red on three stages for reasons unrelated to what they do, and
//!    `release-oracle-bless` did it while DEPENDING on the target that downloads
//!    the baseline it never passed.
//!
//! 2. The two prev-release stages shell out to harnesses that mutate a shared
//!    stand — `field_replay` flips SERVER-WIDE MySQL tmp-table globals,
//!    `ab_regression` seeds a 50 000-row fixture table — and clean up on the way
//!    out. `core.run`'s timeout is `subprocess.run(timeout=…)`, i.e. SIGKILL,
//!    which is the one signal neither cleanup can survive.
//!
//! The dimension in guard 1 is DERIVED (every Makefile recipe that launches the
//! driver), never a typed list of target names — a typed list would have graded
//! only the entry point whose author remembered it, which is the defect.
//!
//! Scope, stated rather than implied: this reads the Makefile only. CI invokes
//! the driver too (`.github/workflows/ci.yml`), but that file is not this
//! guard's subject and its invocation does not reach a gate stage.

use std::fs;
use std::process::Command;

fn root() -> &'static str {
    env!("CARGO_MANIFEST_DIR")
}

/// The field-replay harness's own verdict logic, run here so it has a CALL SITE.
///
/// `report()` decides three of the four release criteria, and the defect it now
/// guards — a leg killed at `LEG_TIMEOUT` handing a FLOOR to a directional
/// comparison, so `[PASS] 3 no makespan regression … (400.0s vs 3600.0s)` — is
/// unreachable from any live run: it needs a timed-out leg, which is exactly
/// what a passing gate never produces. `--self-test` grades it over synthetic
/// legs (no docker, no MySQL, no binaries), and this is what runs it. A
/// self-test nobody calls is the dead-code-behind-a-green-cell shape.
///
/// RED-proven by dropping the `_ungraded` guard from criterion 3 in
/// `dev/pytools/field_replay.py`.
#[test]
fn the_field_replay_verdict_logic_passes_its_own_self_test() {
    let out = Command::new("python3")
        .args(["-m", "dev.pytools.field_replay", "--self-test"])
        .current_dir(root())
        .output()
        .expect(
            "run `python3 -m dev.pytools.field_replay --self-test` — python3 is required by \
             every dev/ tool in this repo (the gate, the matrices, the sweeps), so it is a \
             prerequisite here rather than a reason to skip",
        );
    assert!(
        out.status.success(),
        "the field-replay harness fails its own verdict self-test:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

/// Makefile recipe lines with backslash-continuations JOINED, paired with the
/// target whose recipe they belong to. A gate invocation is written across
/// three continued lines in two of the three targets, so a per-physical-line
/// scan would see the `python3 -m …` line without the `RIVET_PREV_RELEASE_BIN=`
/// that sits two lines above it in the same shell command.
fn makefile_logical_recipe_lines() -> Vec<(String, String)> {
    let src = fs::read_to_string(format!("{}/Makefile", root())).expect("read Makefile");
    let mut out: Vec<(String, String)> = Vec::new();
    let mut target = String::from("<none>");
    let mut pending: Option<String> = None;
    for line in src.lines() {
        // A target header: `name:` / `name: deps  ## help` at column 0.
        if !line.starts_with('\t')
            && !line.starts_with(' ')
            && let Some(colon) = line.find(':')
            && colon > 0
            && line[..colon]
                .chars()
                .all(|c| c.is_alphanumeric() || "-_./%$()".contains(c))
        {
            target = line[..colon].to_string();
        }
        let joined = match pending.take() {
            Some(prev) => format!("{prev} {}", line.trim()),
            None => line.to_string(),
        };
        if let Some(stripped) = joined.strip_suffix('\\') {
            pending = Some(stripped.trim_end().to_string());
        } else {
            out.push((target.clone(), joined));
        }
    }
    if let Some(last) = pending {
        out.push((target, last));
    }
    out
}

/// Every Makefile entry point either CARRIES the baseline or GIVES THE
/// COMPARISON UP BY NAME. There is no third, silent option.
#[test]
fn every_makefile_gate_invocation_carries_a_baseline_or_gives_it_up_by_name() {
    let invocations: Vec<(String, String)> = makefile_logical_recipe_lines()
        .into_iter()
        .filter(|(_, l)| l.contains("python3 -m dev.release_oracle"))
        .collect();

    assert!(
        invocations.len() >= 3,
        "found only {} Makefile invocations of the release-oracle driver — this guard is \
         reading the wrong file or the recipes changed shape",
        invocations.len()
    );

    let offenders: Vec<String> = invocations
        .iter()
        .filter(|(_, l)| {
            !l.contains("RIVET_PREV_RELEASE_BIN")
                && !l.contains("--without-prev-release-comparison")
        })
        .map(|(t, _)| t.clone())
        .collect();

    assert!(
        offenders.is_empty(),
        "these Makefile targets launch the release gate with NEITHER a previous-release \
         baseline (RIVET_PREV_RELEASE_BIN=…) NOR the named escape \
         (--without-prev-release-comparison): {offenders:?}. Since an absent baseline became a \
         FAIL rather than a SKIP, such a target goes red on `release regression`, \
         `previous-release differential` and `field symptom replay` for a reason that has \
         nothing to do with what it checks — which is how `make release-oracle-bless` came to \
         download a baseline and then fail three stages over its absence."
    );
}

/// The prev-release stages must launch their child harness through the
/// SIGINT-first runner, never through `core.run` (whose timeout is SIGKILL).
///
/// RED-proven by putting either call back to `run(`: SIGKILL runs no `__exit__`,
/// no signal handler and no `atexit`, so a timed-out gate leaves `rivet-mysql-1`
/// at `internal_tmp_mem_storage_engine=MEMORY / tmp_table_size=16384` for every
/// later test on the machine, and leaves an `ab_src_<pid>` table per run.
#[test]
fn the_prev_release_child_harnesses_are_not_sigkilled_on_timeout() {
    let src = fs::read_to_string(format!("{}/dev/release_oracle/regression.py", root()))
        .expect("read regression.py");

    for module in ["_FR_MODULE", "_AB_MODULE"] {
        let needle = format!("\"-m\", {module},");
        let at = src
            .find(&needle)
            .unwrap_or_else(|| panic!("regression.py no longer launches {module} as a child"));
        let before = &src[at.saturating_sub(300)..at];
        let launcher = before
            .rfind("_run_interruptible(")
            .zip(before.rfind("= run("))
            .map(|(interruptible, plain)| interruptible > plain)
            .unwrap_or_else(|| before.contains("_run_interruptible("));
        assert!(
            launcher,
            "the {module} child harness is launched with something other than \
             `_run_interruptible` — `core.run`'s timeout is `subprocess.run(timeout=…)`, which \
             is SIGKILL, and this child owns cleanup on a SHARED stand (server-wide MySQL \
             globals / a seeded fixture table) that no SIGKILLed process can run"
        );
    }

    // …and the budgets must not be the child's own. Reading the SAME env var as
    // the harness's per-leg / per-case budget makes the wrapper's whole-harness
    // timeout the smaller number, so it always fires first and the harness's own
    // graceful timeout can never be reached.
    for (own, child) in [
        ("RIVET_ORACLE_FIELD_TIMEOUT", "RIVET_FIELD_TIMEOUT"),
        ("RIVET_ORACLE_AB_TIMEOUT", "RIVET_AB_TIMEOUT"),
    ] {
        assert!(
            src.contains(own),
            "the wrapper has no budget of its own ({own}) and would fall back to the child's \
             {child} — the conflation that made the wrapper expire first"
        );
    }
}
