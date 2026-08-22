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
//! WHAT EACH GUARD HERE CAN AND CANNOT SEE, because a round-7 review found three
//! of them satisfied by PROSE rather than by code:
//!
//! * A Makefile recipe is a SHELL COMMAND, and the operator hints printed by
//!   these recipes quote the very flag the guard looks for. Scanning the whole
//!   joined line therefore passed on an `echo` — measured: deleting
//!   `RIVET_PREV_RELEASE_BIN="$$prev"` from `release-oracle-bless` (the exact
//!   round-5 regression) left the offender list EMPTY. The guard now grades only
//!   the shell command that runs the driver.
//! * A Python `#` comment is not a call site. `RIVET_ORACLE_AB_TIMEOUT` appears
//!   in a comment three lines above the code that reads it, so a rename in the
//!   CODE left the guard green. Comments are stripped, and the env name must sit
//!   INSIDE the `_wrapper_budget(` call.
//! * A string check cannot see a SIGNAL. The behavioural half — SIGINT is
//!   delivered, the child's cleanup runs, the grace period is derived from that
//!   cleanup's own worst case, a SIGKILLed child cannot hang the gate — lives in
//!   `dev/release_oracle/regression.py::_self_test`, which the third test below
//!   RUNS. The string guards keep their narrower names.
//!
//! Scope, stated rather than implied: the Makefile guard reads the Makefile only.
//! CI invokes the driver too (`.github/workflows/ci.yml`), but that file is not
//! this guard's subject and its invocation does not reach a gate stage.

use std::fs;
use std::process::Command;

fn root() -> &'static str {
    env!("CARGO_MANIFEST_DIR")
}

/// Run one of the dev harnesses' `--self-test` entry points and demand it passes.
fn self_test(module: &str, why: &str) {
    let out = Command::new("python3")
        .args(["-m", module, "--self-test"])
        .current_dir(root())
        .output()
        .unwrap_or_else(|e| {
            panic!(
                "run `python3 -m {module} --self-test`: {e} — python3 is required by every \
                 dev/ tool in this repo (the gate, the matrices, the sweeps), so it is a \
                 prerequisite here rather than a reason to skip"
            )
        });
    assert!(
        out.status.success(),
        "{module} fails its own self-test ({why}):\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
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
/// `dev/pytools/field_replay.py`, and (round 7) by gating criterion 1's
/// "a shed was observed before it did" note on the TIMEOUT alone instead of on
/// the observation.
#[test]
fn the_field_replay_verdict_logic_passes_its_own_self_test() {
    self_test(
        "dev.pytools.field_replay",
        "its criterion verdicts over a timed-out leg",
    );
}

/// The differential harness's fixture lifecycle and its derived run count.
///
/// Both are invisible to a green live run: the teardown window only opens when a
/// Ctrl-C lands INSIDE `seed()`, and nothing about a passing comparison says
/// whether `RIVET_RUNS_TOTAL` — which sizes the release-oracle's whole-harness
/// timeout — still matches how many times the binaries are actually invoked.
///
/// RED-proven by moving `seed()` back above the `try` (the drop stops running),
/// and by adding a 7th scenario while leaving the run count re-typed (counted 22
/// against a declared 20).
#[test]
fn the_ab_regression_harness_passes_its_own_self_test() {
    self_test(
        "dev.pytools.ab_regression",
        "its fixture teardown and its derived rivet-invocation count",
    );
}

/// The WRAPPER's own decisions about a child harness — the behavioural half that
/// no string check in this file can see.
///
/// It grades: that a timed-out child is SIGINTed (its cleanup marker appears in
/// the transcript) rather than killed; that the grace period defaults to the
/// CHILD's imported cleanup worst case and that each stage passes its own child's
/// (observed at the boundary, with the launcher stubbed); that
/// `RIVET_ORACLE_CHILD_GRACE=0` means zero loudly and a malformed value cannot
/// take the gate down at import; that a SIGKILLed child whose grandchild holds
/// the pipes cannot hang the gate; that an unreadable — or busy — MySQL stand
/// produces a LOUD row instead of silence; and that the driver's banner and its
/// closing `NOT RELEASE-GRADED` line follow the BASELINE rather than the flag.
///
/// Each of those is RED-proven against the mutant that shipped it (see the
/// `_self_test` docstring in `dev/release_oracle/regression.py`).
#[test]
fn the_release_oracle_wrapper_passes_its_own_self_test() {
    self_test(
        "dev.release_oracle",
        "the child-harness signal, the grace period, the stand row and the banner",
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

/// The single shell COMMAND inside a recipe line that contains `needle`.
///
/// A recipe line is a shell script: `prev=$(…); echo "…"; env … python3 -m …`.
/// The `echo` in the middle is an operator HINT that quotes
/// `--without-prev-release-comparison` verbatim, so a check over the whole line
/// is satisfied by prose about the flag rather than by the flag being passed.
/// Splitting on `;` / `&&` and keeping the command that actually launches the
/// driver is what makes this guard grade a CALL SITE.
fn command_containing(line: &str, needle: &str) -> Option<String> {
    line.split(';')
        .flat_map(|c| c.split("&&"))
        .find(|c| c.contains(needle))
        .map(|c| c.trim().to_string())
}

/// Every Makefile entry point either CARRIES the baseline or GIVES THE
/// COMPARISON UP BY NAME. There is no third, silent option.
///
/// RED-proven (round 7) by deleting `RIVET_PREV_RELEASE_BIN="$$prev"` from
/// `release-oracle-bless` — the round-5 regression this was written against,
/// which the previous whole-line version of this guard no longer caught.
#[test]
fn every_makefile_gate_invocation_carries_a_baseline_or_gives_it_up_by_name() {
    const DRIVER: &str = "python3 -m dev.release_oracle";

    let invocations: Vec<(String, String)> = makefile_logical_recipe_lines()
        .into_iter()
        // Recipe lines only (a tab-indented rule body): a `#` comment at column 0
        // that MENTIONS the driver is documentation, not an entry point.
        .filter(|(_, l)| l.starts_with('\t') && l.contains(DRIVER))
        .collect();

    assert!(
        invocations.len() >= 3,
        "found only {} Makefile invocations of the release-oracle driver — this guard is \
         reading the wrong file or the recipes changed shape",
        invocations.len()
    );

    let offenders: Vec<String> = invocations
        .iter()
        .map(|(t, l)| {
            let cmd = command_containing(l, DRIVER).unwrap_or_else(|| {
                panic!("target {t}: the driver invocation is not inside any shell command")
            });
            (t.clone(), cmd)
        })
        .filter(|(_, cmd)| {
            !cmd.contains("RIVET_PREV_RELEASE_BIN")
                && !cmd.contains("--without-prev-release-comparison")
        })
        .map(|(t, _)| t)
        .collect();

    assert!(
        offenders.is_empty(),
        "these Makefile targets launch the release gate with NEITHER a previous-release \
         baseline (RIVET_PREV_RELEASE_BIN=…) NOR the named escape \
         (--without-prev-release-comparison) IN THE COMMAND THAT LAUNCHES IT: {offenders:?}. \
         Since an absent baseline became a FAIL rather than a SKIP, such a target goes red on \
         `release regression`, `previous-release differential` and `field symptom replay` for \
         a reason that has nothing to do with what it checks — which is how `make \
         release-oracle-bless` came to download a baseline and then fail three stages over its \
         absence. (Mentioning either one in an `echo` hint does not count.)"
    );
}

/// Python source with `#` comments removed, so a guard over it grades CODE.
///
/// Deliberately simple: it tracks single/double quotes and triple-quoted blocks
/// well enough to keep a `#` inside a string, which is all these checks need. A
/// docstring's prose is dropped along with the comments — also correct here,
/// since prose is exactly what must not satisfy a call-site check.
fn strip_python_comments(src: &str) -> String {
    let mut out = String::with_capacity(src.len());
    // Over CHARS, not bytes: this file is full of em-dashes, and byte-slicing it
    // panics on the first one.
    let mut in_triple: Option<char> = None;
    for line in src.lines() {
        let chars: Vec<char> = line.chars().collect();
        let mut i = 0usize;
        let mut quote: Option<char> = None;
        let mut kept = String::new();
        let triple_at = |i: usize, q: char| {
            chars.len() >= i + 3 && chars[i] == q && chars[i + 1] == q && chars[i + 2] == q
        };
        while i < chars.len() {
            if let Some(marker) = in_triple {
                if triple_at(i, marker) {
                    in_triple = None;
                    i += 3;
                } else {
                    i += 1;
                }
                continue;
            }
            if quote.is_none() && (triple_at(i, '"') || triple_at(i, '\'')) {
                in_triple = Some(chars[i]);
                i += 3;
                continue;
            }
            let c = chars[i];
            match quote {
                Some(q) => {
                    kept.push(c);
                    if c == '\\' {
                        if let Some(&next) = chars.get(i + 1) {
                            kept.push(next);
                        }
                        i += 2;
                        continue;
                    }
                    if c == q {
                        quote = None;
                    }
                }
                None => {
                    if c == '#' {
                        break; // a comment: the rest of the line is prose
                    }
                    if c == '"' || c == '\'' {
                        quote = Some(c);
                    }
                    kept.push(c);
                }
            }
            i += 1;
        }
        out.push_str(&kept);
        out.push('\n');
    }
    out
}

/// The prev-release stages must launch their child harness through the
/// SIGINT-first runner, never through `core.run` (whose timeout is SIGKILL).
///
/// This checks the LAUNCHER at the call site — it cannot observe a signal, and it
/// is named for what it checks (the old name promised "are not sigkilled on
/// timeout" over a body that never sent or watched one). The behavioural proof
/// that SIGINT is delivered and the child's cleanup runs is
/// `the_release_oracle_wrapper_passes_its_own_self_test` above.
///
/// RED-proven by putting either call back to `run(`: SIGKILL runs no `__exit__`,
/// no signal handler and no `atexit`, so a timed-out gate leaves `rivet-mysql-1`
/// at `internal_tmp_mem_storage_engine=MEMORY / tmp_table_size=16384` for every
/// later test on the machine, and leaves an `ab_src_<pid>` table per run.
#[test]
fn the_prev_release_child_harnesses_are_launched_through_the_sigint_first_runner() {
    let raw = fs::read_to_string(format!("{}/dev/release_oracle/regression.py", root()))
        .expect("read regression.py");
    // Comments stripped FIRST: this whole file explains `_run_interruptible` in
    // prose, and the previous version accepted that prose as evidence.
    let src = strip_python_comments(&raw);

    for module in ["_FR_MODULE", "_AB_MODULE"] {
        let needle = format!("\"-m\", {module},");
        let at = src
            .find(&needle)
            .unwrap_or_else(|| panic!("regression.py no longer launches {module} as a child"));
        let before = &src[at.saturating_sub(300)..at];
        // Whichever launcher is NEAREST the argv wins. Written with an explicit
        // match rather than `.zip()`: `rfind("= run(")` is None for both modules
        // today, so the zipped comparison was dead and the whole check fell
        // through to a `contains` over a window that still held comments.
        let launcher = match (before.rfind("_run_interruptible("), before.rfind("= run(")) {
            (Some(interruptible), Some(plain)) => interruptible > plain,
            (Some(_), None) => true,
            _ => false,
        };
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
    // graceful timeout can never be reached. The env name must appear INSIDE the
    // `_wrapper_budget(` call: both of these were previously satisfied by the
    // COMMENT that documents them, three lines above the code.
    for (own, child) in [
        ("RIVET_ORACLE_FIELD_TIMEOUT", "RIVET_FIELD_TIMEOUT"),
        ("RIVET_ORACLE_AB_TIMEOUT", "RIVET_AB_TIMEOUT"),
    ] {
        let quoted = format!("\"{own}\"");
        let at_call_site = src.match_indices(&quoted).any(|(i, _)| {
            let window = &src[i.saturating_sub(60)..i];
            window.contains("_wrapper_budget(")
        });
        assert!(
            at_call_site,
            "the wrapper's own budget knob {own} is not passed to `_wrapper_budget(` anywhere \
             in regression.py (a mention in a comment does not count) — without it the wrapper \
             falls back to the child's {child}, the conflation that made it expire first"
        );
    }
}

/// Every file the BigQuery stage stages into its SHARED `work` directory must
/// carry `{engine}` in its name.
///
/// `run_bigquery_golden` builds ONE `work` dir and hands the same `Path` to all
/// three engine legs, which it then runs CONCURRENTLY in a `ThreadPoolExecutor`.
/// So a staged file whose name is a constant has three writers and three
/// subprocess readers: whoever `write_text`s last wins the file, and a sibling's
/// content is uploaded under THIS engine's prefix.
///
/// Not hypothetical. `verify_gc_survival` staged its `running`-marker manifest as
/// a constant `manifest-gc-survival-probe.json`; on the 2026-08-22 gate the mssql
/// and mysql legs ran 0.12 s apart (their arm-1 loads land at 08:55:16.207/.329
/// in the `load_run` ledger), mssql uploaded MYSQL'S manifest into `bq/mssql/`,
/// and `rivet load` refused the prefix — correctly, with "holds manifests from 2
/// DIFFERENT SOURCES (mssql:…, mysql:…)", the guard that stops one source's rows
/// from replacing another's in a warehouse table. The cell recorded
/// `gc-load-failed-with-a-running-marker` and dropped the sentence explaining it.
///
/// Scope, said out loud: the subject is `bigquery.py` — the one gate module whose
/// `work` dir is shared ACROSS concurrently-running engine legs (`blessed_flow` /
/// `blessed_path` build a `work` per cell). It grades string LITERALS after
/// `work /`; a name computed into a variable escapes it, which is why the
/// non-vacuity floor insists the scan really found the staged files.
#[test]
fn every_file_the_concurrent_bigquery_legs_stage_into_the_shared_work_dir_is_engine_keyed() {
    let raw = fs::read_to_string(format!("{}/dev/release_oracle/bigquery.py", root()))
        .expect("read bigquery.py");
    // Comments and docstrings FIRST: this guard's own reason is written in prose
    // right above the code it grades, and prose is not evidence.
    let src = strip_python_comments(&raw);
    assert!(
        src.contains("ThreadPoolExecutor"),
        "the BigQuery stage no longer runs its engine legs concurrently — if that is \
         deliberate, this guard's premise (one `work` dir, three writers) is gone and it \
         should be deleted rather than left passing for the wrong reason"
    );

    let mut seen: Vec<String> = Vec::new();
    let mut offenders: Vec<String> = Vec::new();
    let mut rest = src.as_str();
    while let Some(at) = rest.find("work / ") {
        let tail = &rest[at + "work / ".len()..];
        rest = tail;
        let tail = tail.strip_prefix('f').unwrap_or(tail);
        let Some(tail) = tail.strip_prefix('"') else {
            continue; // not a literal (a variable, a nested call) — see the scope note
        };
        let Some(end) = tail.find('"') else { continue };
        let lit = tail[..end].to_string();
        if !lit.contains("{engine}") {
            offenders.push(lit.clone());
        }
        seen.push(lit);
    }
    assert!(
        seen.len() >= 3,
        "found only {} `work / \"…\"` literal(s) in bigquery.py — the scan is reading the \
         wrong shape and grades nothing (it must see at least the export config, the gc \
         config and the gc-survival marker)",
        seen.len()
    );
    assert!(
        offenders.is_empty(),
        "these files are staged into the BigQuery stage's SHARED work dir under a name that is \
         the SAME for all three concurrent engine legs: {offenders:?}. The last writer wins the \
         local file and a sibling's content is uploaded into this engine's prefix — key the \
         name on the engine, as bqload_<engine>.yaml and gc_<engine>.yaml already do"
    );
}
