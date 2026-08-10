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
