//! Wiring pin for `memory_threshold_mb` — the RSS throttle exists ONLY in the
//! chunked runner family, with three deliberately different semantics
//! (sequential: pause-5s-once; plain-parallel: block-until-below;
//! checkpoint variants: pause-once), and `docs/reference/tuning.md` documents
//! exactly that. Round-4 found the claim guarded by NOTHING: the next edit to
//! any runner could silently un-wire the throttle (or wire it into a runner the
//! doc says doesn't have it) and every suite would stay green.
//!
//! HONEST STRENGTH: this is a source-text pin, not a behavioural proof — it
//! goes RED when the `check_memory` call is REMOVED from a wired runner (the
//! drift that motivated it), and says nothing about whether the sleep works.
//! The threshold decision itself (`resource::check_memory`) has its own units.

use std::path::PathBuf;

fn src(rel: &str) -> String {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(rel);
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

const CALL: &str = "check_memory(plan.tuning.memory_threshold_mb)";

#[test]
fn the_memory_throttle_is_wired_into_every_chunked_variant() {
    // exec.rs holds BOTH plain runners: sequential (pause-once) and parallel
    // (block-until-below, two call sites of its poll loop).
    let exec = src("src/pipeline/chunked/exec.rs");
    assert!(
        exec.matches(CALL).count() >= 3,
        "chunked exec.rs must throttle in both plain runners (sequential pause + \
         parallel poll loop) — wire check_memory back or update \
         docs/runner-coverage-matrix.yaml's memory_throttle row"
    );
    assert!(
        src("src/pipeline/chunked/sequential_checkpoint.rs").contains(CALL),
        "sequential_checkpoint.rs lost its memory throttle — wire check_memory back or \
         update the matrix row"
    );
    // The worker clones the plan, so the receiver is `plan_w` there.
    assert!(
        src("src/pipeline/chunked/parallel_checkpoint.rs")
            .contains("check_memory(plan_w.tuning.memory_threshold_mb)"),
        "parallel_checkpoint.rs lost its memory throttle — wire check_memory back or \
         update the matrix row"
    );
}

#[test]
fn the_memory_throttle_stays_out_of_the_runners_the_doc_says_lack_it() {
    // The na-cells' truth, failing DOWNWARD: single/keyset/mongo_parallel are
    // DOCUMENTED as not having the throttle (tuning.md). Wiring it in is fine —
    // but then this pin, the doc, and the matrix row must all move together,
    // or the ledger silently drifts from the code again.
    for f in [
        "src/pipeline/single.rs",
        "src/pipeline/keyset.rs",
        "src/source/mongo/mod.rs",
    ] {
        assert!(
            !src(f).contains("check_memory("),
            "{f} now calls check_memory — great, but flip the matrix row's na cell \
             to test and update docs/reference/tuning.md in the same commit"
        );
    }
}
