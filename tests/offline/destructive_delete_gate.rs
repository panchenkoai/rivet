//! Every delete under a shared export prefix must consult the run ledger.
//!
//! rivet has TWO deletes that target the same object-store prefix, and they are
//! guarded in inverse proportion to what they destroy:
//!
//!   `gc_orphans`      removes only parts NO `Success` manifest references —
//!                     crash leftovers. It asks the ledger
//!                     (`has_active_run_on_prefix`) and spares everything when a
//!                     run is live (`src/cli/dispatch.rs`, the `maybe_gc_orphans`
//!                     path), failing conservative on a query error.
//!
//!   `cleanup_source`  recursively deletes the WHOLE prefix — every part, every
//!                     manifest, `_SUCCESS` — via `maybe_cleanup` → `delete_under`
//!                     → `store.remove_all`. It asks nothing.
//!
//! `src/load/plan.rs` states the relationship in its own words: gc_orphans is
//! "strictly gentler than `cleanup_source`, which wipes the whole prefix". So the
//! gentler delete honours a classification the total one skips.
//!
//! WHY THIS GUARD IS SOURCE-SHAPED AND NOT BEHAVIOURAL, said plainly: the signal
//! is not plumbed. `maybe_cleanup` takes `Option<(&GcsStore, &str)>` — there is
//! no ledger parameter to pass a live-run verdict through, so no behavioural test
//! can drive the distinction that does not exist yet. What CAN be pinned is the
//! wiring: `state: Option<&StateStore>` is already in scope at each site that
//! builds the cleanup pair, so the ledger is reachable exactly where it is not
//! consulted. This guard fails while that stays true.
//!
//! It is deliberately the same shape as `every_gate_function_has_a_call_site`
//! (release_gate_matrix_guard) — a lint over the source, because the defect is a
//! LOCAL omission at a call site rather than a wrong value anywhere.

use std::path::PathBuf;

fn dispatch_src() -> String {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/cli/dispatch.rs");
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

/// Split a Rust source into `fn` bodies keyed by name, by brace depth.
///
/// Crude on purpose: the question is only "does this function mention X", and a
/// full parse would be a dependency and a second thing to be wrong about.
fn fn_bodies(src: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let lines: Vec<&str> = src.lines().collect();
    let mut i = 0;
    while i < lines.len() {
        let t = lines[i].trim_start();
        let is_fn = t.starts_with("fn ") || t.starts_with("pub fn ") || t.starts_with("async fn ");
        if !is_fn {
            i += 1;
            continue;
        }
        let name = t
            .split("fn ")
            .nth(1)
            .and_then(|r| r.split(['(', '<']).next())
            .unwrap_or("")
            .trim()
            .to_string();
        let mut depth = 0i32;
        let mut body = String::new();
        let mut started = false;
        while i < lines.len() {
            let l = lines[i];
            body.push_str(l);
            body.push('\n');
            depth += l.matches('{').count() as i32;
            depth -= l.matches('}').count() as i32;
            if l.contains('{') {
                started = true;
            }
            i += 1;
            if started && depth <= 0 {
                break;
            }
        }
        out.push((name, body));
    }
    out
}

/// The total-wipe delete must be gated on the same live-run signal the partial
/// delete already honours.
///
/// A function that turns `cleanup_source` into a delete target without ever
/// naming `has_active_run_on_prefix` will hand `maybe_cleanup` a prefix that a
/// concurrent extract may be writing into — and `delete_under` is recursive over
/// the whole prefix, so the concurrent run's committed parts go with it. On a CDC
/// or incremental export the source side has already advanced (slot acked,
/// cursor committed), so those rows then exist in neither the source log, nor
/// the destination, nor the warehouse.
/// STATUS: RED against current code, and `#[ignore]`d for that reason rather
/// than because it needs anything to be running.
///
/// The live suite has a convention for a guard that documents an open finding
/// (`AUDIT-RED … expected to FAIL until fixed`) because that suite is run with
/// `--ignored` and its reds are visible. The offline suite has no such
/// convention: it is the green gate every commit is measured against, so a red
/// test here would read as a broken build rather than as a recorded finding.
///
/// So it is opt-in: `cargo test --test offline_suite -- --ignored
/// cleanup_source_must_consult`. Remove the `#[ignore]` in the same commit that
/// gates the delete — at that point it becomes a real regression guard and
/// belongs in the gate.
// AUDIT-RED destructive-delete-gate: cleanup_source wipes the whole prefix with no live-run check, while the strictly gentler gc_orphans on the same prefix consults the ledger. Asserts CORRECT behavior; expected to FAIL until fixed.
#[test]
#[ignore = "AUDIT-RED: open finding — cleanup_source is ungated; run with --ignored"]
fn cleanup_source_must_consult_the_active_run_ledger() {
    let src = dispatch_src();
    let bodies = fn_bodies(&src);
    assert!(
        bodies.len() > 20,
        "parsed only {} fn bodies from dispatch.rs — this guard is reading it wrong",
        bodies.len()
    );

    // The sites that turn the flag into a delete target.
    let deleters: Vec<&(String, String)> = bodies
        .iter()
        .filter(|(_, b)| b.contains("cleanup_source") && b.contains("then_some"))
        .collect();
    assert!(
        !deleters.is_empty(),
        "no `cleanup_source … then_some(..)` site found in dispatch.rs — the wiring moved, and \
         this guard now checks nothing. Re-point it at the new delete-target construction."
    );

    // The signal the gentler delete already uses, in the same file.
    assert!(
        src.contains("has_active_run_on_prefix"),
        "dispatch.rs no longer references `has_active_run_on_prefix` at all — gc_orphans' own \
         gate is gone, which is a bigger problem than the one this guard was written for."
    );

    let ungated: Vec<&str> = deleters
        .iter()
        .filter(|(_, b)| !b.contains("has_active_run_on_prefix"))
        .map(|(n, _)| n.as_str())
        .collect();

    assert!(
        ungated.is_empty(),
        "these function(s) turn `cleanup_source` into a RECURSIVE delete of the whole export \
         prefix without asking whether a run is writing there: {}\n\n\
         Its sibling on the same prefix — `gc_orphans`, which only removes parts no Success \
         manifest references — does ask (`has_active_run_on_prefix`, failing conservative on a \
         query error). src/load/plan.rs calls gc_orphans \"strictly gentler than cleanup_source, \
         which wipes the whole prefix\", so the destructive path skips the classification the \
         gentle one must honour.\n\
         `state: Option<&StateStore>` is already in scope at each of these sites — the ledger is \
         reachable exactly where it is not consulted.",
        ungated.join(", ")
    );
}
