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
            "split.rs",
            "#167 --split resume: completed_units_in_prefix READS sibling units' \
             manifest copies to decide per-unit skip — a reader, no parts written",
        ),
    ];

    let mut offenders = Vec::new();
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
            for (ln, line) in text.lines().enumerate() {
                // Only CALLS count; a comment naming the function does not.
                let code = line.split("//").next().unwrap_or("");
                if code.contains("create_destination(") {
                    offenders.push(format!("{rel}:{} — {}", ln + 1, line.trim()));
                }
            }
        }
    }
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

    let mut offenders = Vec::new();
    for close in closes {
        let body_start = owning_fn(close);
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
