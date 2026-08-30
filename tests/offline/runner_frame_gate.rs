//! The frame is the only door — a SOURCE lint, because the compiler cannot
//! say it (`create_destination` is necessarily public to the crate).
//!
//! History this gate exists for: the cross-shape manifest guard was skipped by
//! BOTH chunk-checkpoint runners (their own comment recorded it), the same
//! runner-bypass class that previously shipped the drift-gate and Form-B
//! gaps. `RunnerFrame::open` welds the guard to destination creation; this
//! test makes the next bypass a loud diff-time failure instead of a silent
//! per-runner omission.
//!
//! The allowlist is EXPLAINED, not just enumerated: readers verify or repair
//! what a writer already produced — the cross-shape guard protects writers.

use std::path::Path;

#[test]
fn batch_writers_obtain_destinations_only_through_the_frame() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/pipeline");
    // module -> why it may call create_destination directly
    let allowed: &[(&str, &str)] = &[
        ("frame.rs", "the frame IS the door"),
        (
            "finalize.rs",
            "verify/rehydrate READ back what a frame-holding writer produced",
        ),
        (
            "validate_cmd.rs",
            "rivet validate is a pure reader of an existing prefix",
        ),
        (
            "repair_cmd.rs",
            "repair rewrites sidecars under an existing manifest, mode-checked there",
        ),
        (
            "cdc_job.rs",
            "the CDC path guards with mode \"cdc\", not \"batch\" — its frame is the \
             DurableRoll follow-up (architecture review 2026-08-08, card 5)",
        ),
        (
            "chunked/resume_m8.rs",
            "M8 rehydration LISTS durable parts of a prior run; it writes nothing",
        ),
        (
            "keyset.rs",
            "round-5 resume probe: rehydrate_keyset_pages_probed LISTS the destination \
             to verify prior committed pages still exist before re-declaring them — a \
             reader; a missing page REFUSES the resume, writes nothing",
        ),
        (
            "split.rs",
            "#167 --split resume: completed_units_in_prefix READS sibling units' \
             manifest copies to decide per-unit skip — a reader, no parts written",
        ),
    ];

    // NON-VACUITY, before any verdict: this lint's whole subject is the token
    // `create_destination(`, and a lint over a token that no longer exists finds
    // no offenders and reports green. `frame.rs` is the door, so the door's own
    // call is the proof the token is still the one to lint for — rename it and
    // this fails HERE, naming the missing subject, instead of passing over a
    // tree that no longer contains it.
    super::nonvacuity::require_needle(
        &super::nonvacuity::subject_text("src/pipeline/frame.rs"),
        "src/pipeline/frame.rs",
        "create_destination(",
        1,
        "If destination creation was renamed or moved off the frame, re-point this gate at \
         the new call — the runner-bypass class it guards did not go away with the name.",
    );

    let mut offenders = Vec::new();
    let mut scanned = 0usize;
    let mut stack = vec![root.clone()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("read src/pipeline") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let rel = path
                .strip_prefix(&root)
                .expect("under pipeline")
                .to_string_lossy()
                .replace('\\', "/");
            if allowed.iter().any(|(m, _)| *m == rel) {
                continue;
            }
            let text = std::fs::read_to_string(&path).expect("read source");
            scanned += 1;
            for (ln, line) in text.lines().enumerate() {
                // Only CALLS count; a comment naming the function does not.
                let code = line.split("//").next().unwrap_or("");
                if code.contains("create_destination(") {
                    offenders.push(format!("{rel}:{} — {}", ln + 1, line.trim()));
                }
            }
        }
    }
    // The walk itself is a subject: `src/pipeline` split into submodules once
    // already, and a gate that walks a directory which has moved out from under
    // it reports "no offenders" over zero files. 40+ modules today.
    super::nonvacuity::require_enumerated(
        scanned,
        20,
        "non-allowlisted `.rs` files scanned under src/pipeline",
        "The runners moved out of src/pipeline — walk wherever they live now.",
    );
    assert!(
        offenders.is_empty(),
        "a batch writer bypassed RunnerFrame::open — the cross-shape guard does not\n\
         travel with a bare create_destination. Route it through the frame (or, if\n\
         this is genuinely a reader, add it to the allowlist WITH its reason):\n{}",
        offenders.join("\n")
    );
}

/// A self-grading pool must not measure its own instrumentation.
///
/// `run_pool` prints "actual makespan X vs predicted Y" and uses the same
/// window for the run aggregate. Closing the run-level harm bracket
/// (`RunHarmBracket::close_and_warn` → `job::harm_snapshot`) OPENS a source
/// connection and queries the server's counters, so stamping `finished_at`
/// after it charges that round-trip to the schedule the model is grading
/// (bughunt 2026-08-13). The stamp belongs immediately after the export loop
/// drains.
///
/// Scope honesty: this is a SOURCE-ORDER lint, the same shape as the gate
/// above, because `run_pool` needs a real source, state store and destination —
/// no unit test can observe its makespan. It goes RED when the statement moves
/// back below the snapshot (verified by moving it), but it cannot say the
/// resulting NUMBER is right; the value-level oracle would be a live pool run.
#[test]
fn the_pool_stamps_its_makespan_before_the_harm_snapshot_that_grades_it() {
    let run_rs = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/pipeline/run.rs");
    let text = std::fs::read_to_string(&run_rs).expect("read src/pipeline/run.rs");
    let start = text
        .find("pub(crate) fn run_pool(")
        .expect("run_pool must exist — rename it here too if it moved");
    let body = &text[start..];
    // Bound the slice at the next top-level `fn`, so a later function's
    // `finished_at` cannot answer for this one.
    let end = body[1..].find("\nfn ").map(|i| i + 1).unwrap_or(body.len());
    let body = &body[..end];

    let stamp = body
        .find("let finished_at = chrono::Utc::now();")
        .expect("run_pool must stamp finished_at");
    let loop_start = body
        .find("std::thread::scope(")
        .expect("run_pool runs its exports in a thread scope");
    // The CLOSING snapshot is the last one; the bracket's `open` is taken
    // before the loop and is not what this guard is about. Either shape counts
    // — the bracket seam (`close_and_warn`) or a bare `harm_snapshot` call.
    let closing_snapshot = body
        .rfind("close_and_warn(")
        .or_else(|| body.rfind("harm_snapshot("))
        .expect("run_pool closes a run-level harm bracket after the export loop");

    assert!(
        stamp > loop_start,
        "finished_at is stamped before the export loop even starts — the makespan \
         would measure nothing"
    );
    assert!(
        stamp < closing_snapshot,
        "finished_at is stamped AFTER the run-level harm_snapshot, so the printed \
         makespan (and the aggregate window) includes a source connection + counter \
         query the schedule never asked for. Move the stamp to just after the export \
         loop drains."
    );
}

/// EVERY concurrent parent must stamp its window before probing the source.
///
/// The pool guard above pins one site; this pins the class. `run.rs` closes a
/// run-level harm bracket in four places (process-parallel parent, in-process
/// parallel, `run_waves`' parallel path, `run_pool`) and each one feeds a
/// `finished_at` into the run aggregate — so each must stamp the window BEFORE
/// `close_and_warn` opens a source connection and queries the counters.
/// Otherwise the aggregate's duration and throughput charge the run for its own
/// instrumentation (bughunt 2026-08-13; three of the four shipped this way).
///
/// Scope honesty: a SOURCE-ORDER lint like its sibling — the emission needs a
/// live source, so no unit test can observe the numbers. RED-proven by moving
/// any one stamp back below its `close_and_warn`.
///
/// The search window is the BRACKET, not the enclosing function. An earlier
/// version sliced from the owning `fn`, which made the in-process-parallel site
/// (`run.rs`, the `window_end = Some(...)` stamp) VACUOUS: that close lives in
/// the same `pub fn run(` as the process-parallel one, so the sibling BRANCH's
/// `let finished_at = chrono::Utc::now();` — on a code path this close can never
/// take — already satisfied the search and deleting the real stamp stayed GREEN
/// (bughunt 2026-08-13, finding #11). Bounding at the nearest preceding
/// `RunHarmBracket::open(` narrows each window to the branch that actually opened
/// the bracket being closed, so every close is answered by a stamp on its OWN
/// path.
#[test]
fn every_run_harm_bracket_close_is_preceded_by_its_window_stamp() {
    let run_rs = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/pipeline/run.rs");
    let text = std::fs::read_to_string(&run_rs).expect("read src/pipeline/run.rs");
    // Function bodies, so a neighbour's stamp cannot answer for this one.
    let fn_starts: Vec<usize> = text
        .match_indices("\nfn ")
        .chain(text.match_indices("\npub(crate) fn "))
        .chain(text.match_indices("\npub fn "))
        .map(|(i, _)| i)
        .collect();
    let owning_fn = |pos: usize| {
        fn_starts
            .iter()
            .copied()
            .filter(|&s| s < pos)
            .max()
            .unwrap_or(0)
    };

    let closes: Vec<usize> = text
        .match_indices("close_and_warn(")
        // The method's own definition is not a call site.
        .filter(|(i, _)| !text[..*i].ends_with("fn "))
        .map(|(i, _)| i)
        .collect();
    assert_eq!(
        closes.len(),
        4,
        "expected one bracket close per concurrent parent (processes, threads, \
         waves-parallel, pool) — found {}. If a path was added or removed, update \
         this count deliberately.",
        closes.len()
    );

    // Each close is answered ONLY by a stamp inside its own bracket — from the
    // `RunHarmBracket::open(` that opened it to the close itself. The owning fn
    // is the outer bound (a bracket never spans functions); the open is what
    // makes the window branch-local, which is the whole point (see the doc
    // comment: the fn-wide slice let a sibling branch's stamp answer).
    let opens: Vec<usize> = text
        .match_indices("RunHarmBracket::open(")
        .map(|(i, _)| i)
        .collect();

    let mut offenders = Vec::new();
    for close in closes {
        let nearest_open = opens
            .iter()
            .copied()
            .filter(|&o| o < close)
            .max()
            .unwrap_or_else(|| {
                panic!(
                    "close_and_warn at run.rs:{} has no preceding RunHarmBracket::open — \
                     the bracket seam moved; update this gate deliberately",
                    text[..close].matches('\n').count() + 1
                )
            });
        let body_start = owning_fn(close).max(nearest_open);
        let before = &text[body_start..close];
        let stamped = before.contains("let finished_at = chrono::Utc::now();")
            || before.contains("window_end = Some(chrono::Utc::now());");
        if !stamped {
            let line = text[..close].matches('\n').count() + 1;
            offenders.push(format!("run.rs:{line}"));
        }
    }
    assert!(
        offenders.is_empty(),
        "a run-harm bracket closes BEFORE its window is stamped, so the aggregate's \
         duration includes the counter probe (a source connection + query) the run \
         never asked for. Stamp `finished_at` (or `window_end`) right after the \
         exports drain, then close the bracket: {}",
        offenders.join(", ")
    );
}

/// Both job entry points must bracket the source-harm window.
///
/// `run_export_job` (the `rivet run` path) and `run_export_job_with_chunk_source`
/// (the `rivet apply` path) are SEPARATE runners: apply replays a sealed artifact
/// and skips `build_plan`, so every per-export feature has to be re-applied there
/// by hand — the runner-bypass class this repo keeps paying for. Apply shipped
/// without the harm bracket and without the DIAGNOSIS line for the whole 0.24.x
/// series while its own doc comment claimed "everything else … is identical to
/// `run_export_job`" (round-3 bughunt). An `apply` run wrote no `export_harm`
/// rows and said nothing about a source it had just spilled to disk.
///
/// The body is bounded by BRACE MATCHING, not by the next `fn` — see
/// [`body_of`] for the vacuous-slice failure that rule exists for.
///
/// Scope honesty: a SOURCE-ORDER lint, like its siblings above — both functions
/// need a live source and a destination, so no unit test can observe the emitted
/// line. It pins that each entry point brackets the window and diagnoses; the
/// values are the business of `run_diagnosis`'s own unit matrix.
#[test]
fn both_job_entry_points_bracket_the_source_harm_window() {
    assert_both_job_entry_points_do(
        &[
            ("opens the harm bracket", "harm_snapshot(&plan.source)"),
            ("records the delta", "record_harm("),
            ("emits the DIAGNOSIS", "run_diagnosis("),
        ],
        "a job entry point skips the source-harm bracket, so runs through it record \
         no export_harm rows and never diagnose a spilling source",
    );
}

/// Both job entry points must PERSIST the run journal they fill.
///
/// Its own test rather than three more needles on the harm gate above, because
/// it is a different claim about the same seam and a name must not promise what
/// its body does not check.
///
/// Every runner either entry point dispatches to records into `summary.journal`
/// — `governor.drain_into` (`ParallelismAdjusted`, one per adaptive shed),
/// `commit.rs`'s `ChunkCompleted`/`FileWritten`, `schema_drift`'s
/// `SchemaChanged` — and the journal reaches `rivet journal` only through
/// `state.store_journal`. `run_export_job` recorded the terminal `RunCompleted`
/// and stored; `run_export_job_with_chunk_source` (the `rivet apply` path) did
/// NEITHER for the whole 0.24.x series, so an apply run's structured history was
/// built in memory and dropped — `rivet journal -e X` said "No journal entries
/// for export 'X' yet." after a completed run, and `final_outcome()` would read
/// "(incomplete)" for it even if a row existed (round-5 bughunt). The governor
/// wiring landed the round before makes the loss concrete: every shed an
/// adaptive apply takes is unrecorded.
///
/// Scope honesty: a SOURCE-ORDER lint, like its siblings — both functions need a
/// live source, a state store and a destination, so no unit test can observe the
/// stored row. It pins that each entry point records the terminal event and
/// persists; the CONTENT is `RunJournal`'s own business. RED-proven by deleting
/// the apply-side `store_journal` call.
#[test]
fn both_job_entry_points_persist_the_run_journal() {
    assert_both_job_entry_points_do(
        &[
            ("records the terminal event", "RunEvent::RunCompleted {"),
            (
                "persists the journal",
                "state.store_journal(&summary.journal)",
            ),
        ],
        "a job entry point fills a run journal and never stores it, so `rivet journal` \
         is blind to every run through it (the ParallelismAdjusted sheds, the chunk \
         history) — the runner-bypass class",
    );
}

/// The function body that starts at `from`, delimited by its own braces.
///
/// Brace matching, NOT "up to the next `fn`" — the first draft of the harm gate
/// sliced to end-of-file, so the `#[cfg(test)]` module's own eleven
/// `run_diagnosis(` calls satisfied it and it stayed green against the exact
/// mutant it exists to catch (this repo's "fixture against itself" class, caught
/// by RED-proving rather than by reading).
fn body_of(text: &str, from: usize) -> &str {
    let open = from + text[from..].find('{').expect("a fn has a body");
    let mut depth = 0usize;
    for (i, c) in text[open..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return &text[from..open + i + 1];
                }
            }
            _ => {}
        }
    }
    panic!("unbalanced braces from byte {from}");
}

/// Assert each `(what, needle)` appears in the body of BOTH job entry points.
fn assert_both_job_entry_points_do(needles: &[(&str, &str)], harm: &str) {
    let job_rs = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/pipeline/job.rs");
    let text = std::fs::read_to_string(&job_rs).expect("read src/pipeline/job.rs");

    let mut offenders = Vec::new();
    // Since the post-plan script was unified (arch-roast 2026-08-21), an entry
    // point satisfies the contract EITHER by carrying the needles itself OR by
    // routing through `execute_resolved_plan(` — the one script that owns them.
    // The script's own body is then graded for every needle directly, so the
    // contract cannot be satisfied by a route to a hollowed-out script.
    let script_entry = "fn execute_resolved_plan(";
    for entry in [
        "pub(super) fn run_export_job(",
        "pub(crate) fn run_export_job_with_chunk_source(",
        script_entry,
    ] {
        let start = text
            .find(entry)
            .unwrap_or_else(|| panic!("entry point moved or was renamed: {entry}"));
        let body = body_of(&text, start);
        // Guard the guard: a body that swallowed the test module would make every
        // assertion below vacuous, which is exactly how the first draft failed.
        assert!(
            !body.contains("#[cfg(test)]"),
            "{entry}: the slice ran past the function into the test module — the \
             assertions below would be satisfied by the tests' own calls"
        );
        // Comment-stripped, so a doc-comment MENTION of the script cannot
        // satisfy the contract (godsplit bughunt 2026-08-21, MED): only a real
        // call in code counts.
        let code: String = body
            .lines()
            .map(|l| l.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");
        let routes_through_script =
            entry != script_entry && code.contains("execute_resolved_plan(");
        for (what, needle) in needles {
            if !body.contains(needle) && !routes_through_script {
                offenders.push(format!("{entry} never {what} ({needle})"));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "{harm} (docs/runner-coverage-matrix.yaml). Re-apply it in that entry \
         point:\n{}",
        offenders.join("\n")
    );
}
