//! LIVE-ONLY PURITY: a body no offline test can reach may not DECIDE.
//!
//! `.cargo/mutants.toml` excludes a handful of functions WHOLESALE — `replace
//! run_pool -> Result`, `replace check -> Result<bool> with Ok`, … — under the
//! documented "`--lib` on a live-only path" class: they open a real source, a
//! real destination or a real process, so `cargo mutants --in-diff` (which has
//! no stand) reports every mutant in them MISSED whatever the assertions say.
//! That exclusion is honest about the BODY. It is not honest about the
//! DECISIONS inside the body: an `&&`, a `!`, a `x > y` in an `if` is a branch
//! whose mutant is now excluded too, and no unit test is asked for it.
//!
//! Measured this session, and the reason this gate exists: five decisions were
//! pulled out of `execute_resolved_plan` (`src/pipeline/job.rs`) BY HAND —
//! `should_reconcile`, `plan_rejection_error`, `resume_success_gate_applies`,
//! `rerun_warning_applies`, `dispatches_to_cdc_runner` — each time only because
//! the mutation gate pointed at that exact operator and someone had to argue it
//! out one at a time. `fold_failures` (`src/pipeline/run.rs`) came out of the
//! same pass and was a REAL gap, not a triage: the fold had no unit oracle at
//! all. Six extractions, six ad-hoc arguments, and nothing to stop the seventh
//! from being written inline tomorrow.
//!
//! So state the rule instead of re-litigating it: **a live-only body is glue —
//! sequencing, I/O, error context — and calls NAMED PREDICATES for anything it
//! decides.** The predicate is pure, offline-testable, and its mutants are
//! graded; the glue around it stays excluded for the reason the exclusion says.
//!
//! # What counts as a decision
//!
//! `&&`, `||` and negation `!` anywhere in the body, and a comparison
//! (`== != <= >= < >`) inside an `if`/`while` CONDITION. Those are exactly the
//! shapes cargo-mutants rewrites (`replace && with ||`, `delete !`, `replace >
//! with >=`), so the count is a count of excluded mutants.
//!
//! Scope honesty, stated rather than implied:
//!
//! * a comparison in a `let`, a `match` arm or a closure body is NOT counted —
//!   the rule as written covers `if`/`while`, and widening it later is a
//!   tightening, never a loosening. Do not read a zero here as "no comparison
//!   in this function";
//! * `match` and `if let` are not counted either. They dispatch on a shape, and
//!   cargo-mutants' `delete match arm` mutants on the live-only dispatchers are
//!   already triaged individually in `mutants.toml` with a live oracle each;
//! * this is a SOURCE lint. It grades the text of a body, not its behaviour. It
//!   cannot tell a decision that matters from one that does not — which is the
//!   point: the ratchet says "argue this one out", and the argument's home is a
//!   named predicate with a unit test, not a comment.
//!
//! # The ratchet
//!
//! [`BASELINE`] holds today's offenders at today's counts, the same shrink-only
//! shape as `rig_adoption_guard`'s bespoke-site ledger. A site that is not
//! listed must be CLEAN; a listed site may not grow; a listed site that has been
//! cleaned up must have its ceiling LOWERED in the same diff, so every
//! extraction is banked and cannot be spent later as silent slack.

use std::collections::BTreeMap;

/// Decisions counted in one live-only body, per shape.
///
/// Kept as separate counters rather than a total so a failure names WHICH
/// mutant class grew — the fix for an `&&` (a named predicate) is not the fix
/// for a `>` in a loop bound.
#[derive(Default, Clone, Copy, PartialEq, Eq, Debug)]
struct Decisions {
    /// `&&`, anywhere in the body.
    and: usize,
    /// `||` used as an operator — a zero-argument closure (`move || …`,
    /// `unwrap_or_else(|| …)`) is not a decision and is not counted.
    or: usize,
    /// Negation `!x`. A macro bang (`assert!`, `format!`) and the `!=`
    /// operator are not negations and are not counted here.
    not: usize,
    /// A comparison inside an `if`/`while` condition.
    cmp: usize,
}

impl Decisions {
    fn total(self) -> usize {
        self.and + self.or + self.not + self.cmp
    }
    fn exceeds(self, cap: Decisions) -> bool {
        self.and > cap.and || self.or > cap.or || self.not > cap.not || self.cmp > cap.cmp
    }
    fn under(self, cap: Decisions) -> bool {
        self.and < cap.and || self.or < cap.or || self.not < cap.not || self.cmp < cap.cmp
    }
}

/// `(site, and, or, not, cmp)` — the decisions each live-only body still makes
/// inline. Site is `<path>::<fn>`; regenerate a row from this gate's own
/// failure message, which prints the actual counts.
///
/// Shrink freely — LOWER a number the moment you extract a predicate. Growing
/// one, or adding a row, means a new decision is now excluded from the mutation
/// corpus with nothing asked in return: the reviewed alternative is a named
/// pure function next door plus its unit test, which is what the six
/// extractions this ledger was born from ended up as.
const BASELINE: &[(&str, usize, usize, usize, usize)] = &[
    // ── the export RUNNERS ───────────────────────────────────────────────
    // The three big-table runners each own their execution loop, and each loop
    // is dense with pagination/plan arithmetic. These are the ceilings most
    // worth spending: the runner-bypass class in CLAUDE.md is precisely a
    // per-runner decision that no offline test grades.
    ("src/pipeline/keyset.rs::run_keyset", 5, 1, 3, 2),
    ("src/pipeline/keyset.rs::run_keyset_parallel", 8, 0, 4, 4),
    (
        "src/pipeline/mongo_parallel.rs::run_mongo_parallel",
        1,
        0,
        1,
        1,
    ),
    ("src/pipeline/single.rs::run_with_reconnect", 0, 0, 0, 1),
    // run_pool is the bounded work-stealing executor. Its two headline
    // decisions (next_eligible, advise_split) are ALREADY extracted and
    // unit-tested — mutants.toml says so — and what remains is the HeavyGuard
    // reset and the giant-lookup glue. Those remaining operators are why
    // `delete ! in run_pool` and `replace == with != in run_pool` had to be
    // triaged as separate entries: this ledger is the list of such entries
    // waiting to be written.
    ("src/pipeline/run.rs::run_pool", 7, 1, 6, 2),
    (
        "src/pipeline/parallel_children.rs::run_exports_as_child_processes",
        0,
        0,
        5,
        3,
    ),
    // ── the orchestration scripts ────────────────────────────────────────
    // execute_resolved_plan is where the six hand-extractions came FROM, so its
    // row is the direct measure of how much of that pass is left.
    ("src/pipeline/job.rs::execute_resolved_plan", 2, 0, 0, 1),
    ("src/pipeline/plan_cmd.rs::run_plan_command", 1, 0, 3, 1),
    // The `rivet load` orchestrator (src/load/orchestrate.rs). Six bodies are
    // excluded there and FIVE are listed nowhere here, because they are clean:
    // every decision the in-diff gate reported alive in them was extracted into
    // a named predicate with a truth table (needs_source_engine,
    // conflicting_source_ident, up_to_date_label, ledger_says_active,
    // prefix_is_active, cleanup_verdict, consumable_run_ids, active_run_note,
    // append_done_line / full_done_line), and the mode router was made
    // exhaustive so its arm-deletion mutants stop compiling.
    //
    // `prepare_load`'s two remaining `&&` are LET-CHAINS (`if let Some(s) =
    // state && let Some((_, m)) = keyed.first()`), not boolean decisions: the
    // scanner counts the token, but `replace && with ||` on them does not
    // COMPILE (`error: || operators are not supported in let chain conditions`
    // — verified by hand 2026-08-21), so they are unviable rather than
    // uncaught. The comparison they guard IS extracted and unit-tested
    // (conflicting_source_ident). Widening the scanner to skip let-chains would
    // be a loosening; a row that says why is the reviewed alternative.
    ("src/load/orchestrate.rs::prepare_load", 2, 0, 0, 0),
    // ── source/introspection glue ────────────────────────────────────────
    ("src/init/mod.rs::introspect_all", 2, 0, 3, 1),
    ("src/init/mysql.rs::density_probe", 1, 0, 1, 2),
    ("src/init/postgres.rs::density_probe", 0, 0, 1, 0),
    ("src/source/postgres/mod.rs::pg_run_export", 2, 0, 2, 4),
    // ── preflight ────────────────────────────────────────────────────────
    // `check` diagnoses every export against a live source. The `==` in its
    // overlay export-match loop is already a named mutants.toml entry
    // (`replace == with != in check$`, anchored) — same story as run_pool.
    // 6→5 `and`: the target-FAIL tally (`if let Some(t) = eff_target && report
    // .has_target_fail()`, plus the `+=` it guarded) moved out to the pure
    // `TargetFailTally::add_export`, where the in-diff gate's `+=` → `*=`/`-=`
    // mutants are graded instead of MISSED.
    ("src/preflight/mod.rs::check", 5, 0, 7, 2),
];

// ── reading the live-only set out of the mutation config ─────────────────

/// The whole-function exclusions, as `(fn name, return-type prefix)`.
///
/// DERIVED from `.cargo/mutants.toml`, never typed in: the enumerated dimension
/// here is "which functions the gate excludes wholesale", and a hand-written
/// copy of it grades only what its author already knew (CLAUDE.md, "derive the
/// enumerated dimension"). A new live-only exclusion therefore enters this
/// gate's subject the moment it is added to the config — which is exactly the
/// moment someone is deciding whether the body it hides is glue or logic.
///
/// The link is load-bearing in BOTH directions, and RED-proven so: drifting the
/// `check` entry's discriminator to `-> Result<Verdict>` (a stand-in for the
/// function being renamed or its signature changing) makes it resolve to no
/// body, and the per-name non-vacuity assertion in [`graded_sites`] fails
/// naming the dead exclusion — rather than grading zero bodies and calling the
/// tree pure. Honest scope: the same class via an actual `fn` rename cannot be
/// exercised the same way, because the tree then does not compile and no test
/// runs at all — a louder failure, but not this one's.
///
/// A whole-function stub mutant is named `replace <fn> -> <ret> with <value>`
/// (or `replace <fn> with <value>` when there is no return type), so an entry
/// of that SHAPE is a live-only claim about the whole body. Operator entries
/// (`replace == with != in check$`, `delete ! in run_pool`) are equivalence or
/// per-operator triage, not a body claim, and are skipped — they are, in fact,
/// the symptom this gate exists to make unnecessary.
fn live_only_functions() -> Vec<(String, Option<String>)> {
    let mut out = Vec::new();
    for pat in super::mutation_gate_config::exclude_patterns() {
        let Some(rest) = pat.strip_prefix("replace ") else {
            continue;
        };
        let name: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if name.is_empty() {
            continue;
        }
        let tail = &rest[name.len()..];
        // `replace <fn> -> <ret> with <val>` / `replace <fn> with <val>`. The
        // `-> ` and `with ` spellings are cargo-mutants', not ours.
        let ret = if let Some(after) = tail.strip_prefix(" -> ") {
            let ret = after.split(" with ").next().unwrap_or(after).trim();
            // The entry is a REGEX, so its literal text carries backslash
            // escapes (`Result<\(\)>`); compare against source text unescaped.
            let ret: String = {
                let mut s = String::new();
                let mut chars = ret.chars();
                while let Some(c) = chars.next() {
                    if c == '\\' {
                        if let Some(n) = chars.next() {
                            s.push(n);
                        }
                    } else {
                        s.push(c);
                    }
                }
                s
            };
            if ret.is_empty() { None } else { Some(ret) }
        } else if tail.starts_with(" with ") || tail == " with" || tail.starts_with(" ->") {
            // `replace density_probe with …` (no return type) and
            // `replace pg_run_export ->` (truncated before it) are both
            // name-only claims: no discriminator, so grade every site of that
            // name.
            None
        } else {
            continue; // not a whole-function stub entry
        };
        out.push((name, ret));
    }
    out.sort();
    out.dedup();
    out
}

// ── reading Rust source without being fooled by it ───────────────────────

/// Blank out comments, string literals and char literals, preserving offsets.
///
/// A lint that reads raw source grades its own doc comments: `runner_frame_gate`
/// shipped satisfiable by a doc-COMMENT mention of the call it demanded, and the
/// fix was exactly this. Offsets are preserved (each removed byte becomes a
/// space) so a brace scan over the result still lands on the real braces.
fn blank_noncode(src: &str) -> String {
    let b: Vec<char> = src.chars().collect();
    let mut out = String::with_capacity(b.len());
    let mut i = 0usize;
    let keep = |out: &mut String, c: char| out.push(if c == '\n' { '\n' } else { ' ' });
    while i < b.len() {
        // line comment
        if b[i] == '/' && b.get(i + 1) == Some(&'/') {
            while i < b.len() && b[i] != '\n' {
                keep(&mut out, b[i]);
                i += 1;
            }
            continue;
        }
        // block comment (nesting, as rustc allows)
        if b[i] == '/' && b.get(i + 1) == Some(&'*') {
            let mut depth = 0usize;
            while i < b.len() {
                if b[i] == '/' && b.get(i + 1) == Some(&'*') {
                    depth += 1;
                    keep(&mut out, b[i]);
                    keep(&mut out, b[i + 1]);
                    i += 2;
                    continue;
                }
                if b[i] == '*' && b.get(i + 1) == Some(&'/') {
                    depth -= 1;
                    keep(&mut out, b[i]);
                    keep(&mut out, b[i + 1]);
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                    continue;
                }
                keep(&mut out, b[i]);
                i += 1;
            }
            continue;
        }
        // raw string: r"…" / r#"…"# / br##"…"##
        if (b[i] == 'r' || b[i] == 'b')
            && let Some(start) = raw_string_start(&b, i)
        {
            let hashes = start.1;
            let mut j = start.0;
            for c in &b[i..j] {
                keep(&mut out, *c);
            }
            'raw: while j < b.len() {
                if b[j] == '"' && b[j + 1..].iter().take(hashes).all(|c| *c == '#') {
                    for c in &b[j..(j + 1 + hashes).min(b.len())] {
                        keep(&mut out, *c);
                    }
                    j += 1 + hashes;
                    break 'raw;
                }
                keep(&mut out, b[j]);
                j += 1;
            }
            i = j;
            continue;
        }
        // ordinary string
        if b[i] == '"' {
            keep(&mut out, b[i]);
            i += 1;
            while i < b.len() && b[i] != '"' {
                if b[i] == '\\' {
                    keep(&mut out, b[i]);
                    i += 1;
                    if i >= b.len() {
                        break;
                    }
                }
                keep(&mut out, b[i]);
                i += 1;
            }
            if i < b.len() {
                keep(&mut out, b[i]);
                i += 1;
            }
            continue;
        }
        // char literal vs LIFETIME: `'a` is a lifetime, `'x'` and `'\n'` are not
        if b[i] == '\'' && (b.get(i + 1) == Some(&'\\') || b.get(i + 2) == Some(&'\'')) {
            keep(&mut out, b[i]);
            i += 1;
            while i < b.len() && b[i] != '\'' {
                if b[i] == '\\' {
                    keep(&mut out, b[i]);
                    i += 1;
                    if i >= b.len() {
                        break;
                    }
                }
                keep(&mut out, b[i]);
                i += 1;
            }
            if i < b.len() {
                keep(&mut out, b[i]);
                i += 1;
            }
            continue;
        }
        out.push(b[i]);
        i += 1;
    }
    out
}

/// If a raw-string literal opens at `i`, return `(index of the opening quote,
/// hash count)`.
fn raw_string_start(b: &[char], i: usize) -> Option<(usize, usize)> {
    let mut j = i;
    if b[j] == 'b' {
        j += 1;
    }
    if b.get(j) != Some(&'r') {
        return None;
    }
    // `r` must start a token, not end an identifier (`char`, `for`).
    if i > 0 && (b[i - 1].is_ascii_alphanumeric() || b[i - 1] == '_') {
        return None;
    }
    j += 1;
    let mut hashes = 0usize;
    while b.get(j) == Some(&'#') {
        hashes += 1;
        j += 1;
    }
    if b.get(j) == Some(&'"') {
        Some((j + 1, hashes))
    } else {
        None
    }
}

/// Every `fn <name>` in `code` (already blanked), as `(return type, body)`.
///
/// The body is the brace-matched text INCLUDING its braces; the return type is
/// the text between a depth-0 `->` and the opening brace, empty when the
/// function returns unit.
fn fn_sites(code: &str, name: &str) -> Vec<(String, String)> {
    let b: Vec<char> = code.chars().collect();
    let needle: Vec<char> = format!("fn {name}").chars().collect();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i + needle.len() < b.len() {
        if b[i..].starts_with(&needle[..])
            && (i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == '_'))
        {
            let mut j = i + needle.len();
            while b.get(j) == Some(&' ') {
                j += 1;
            }
            // The next token settles it: `fn foo(` / `fn foo<T>(` is our
            // function; `fn foo_bar(` was excluded by the word boundary above.
            if b.get(j) == Some(&'(') || b.get(j) == Some(&'<') {
                let mut depth = 0i32;
                let mut ret_at: Option<usize> = None;
                let mut k = j;
                while k < b.len() {
                    match b[k] {
                        '(' | '[' => depth += 1,
                        ')' | ']' => depth -= 1,
                        '<' => depth += 1,
                        '>' => depth -= 1,
                        ';' if depth <= 0 => break, // a trait signature, no body
                        '{' if depth <= 0 => {
                            let body_end = match_brace(&b, k);
                            let ret = ret_at
                                .map(|r| b[r..k].iter().collect::<String>().trim().to_string())
                                .unwrap_or_default();
                            if let Some(end) = body_end {
                                out.push((ret, b[k..=end].iter().collect::<String>()));
                            }
                            break;
                        }
                        _ => {}
                    }
                    if b[k] == '-' && b.get(k + 1) == Some(&'>') {
                        if depth == 0 && ret_at.is_none() {
                            ret_at = Some(k + 2);
                        }
                        k += 2; // `->`'s `>` is not an angle close
                        continue;
                    }
                    k += 1;
                }
            }
        }
        i += 1;
    }
    out
}

fn match_brace(b: &[char], open: usize) -> Option<usize> {
    let mut depth = 0i32;
    for (k, c) in b.iter().enumerate().skip(open) {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(k);
                }
            }
            _ => {}
        }
    }
    None
}

/// Count the decision shapes in a blanked body.
fn decisions(body: &str) -> Decisions {
    let b: Vec<char> = body.chars().collect();
    let mut d = Decisions::default();
    let identish = |c: char| c.is_ascii_alphanumeric() || c == '_';
    let mut i = 0usize;
    while i < b.len() {
        if b[i] == '&' && b.get(i + 1) == Some(&'&') {
            d.and += 1;
            i += 2;
            continue;
        }
        if b[i] == '|' && b.get(i + 1) == Some(&'|') {
            // `||` is a ZERO-ARG CLOSURE far more often than an or in this
            // code (`move || {}`, `unwrap_or_else(|| …)`), and cargo-mutants
            // does not mutate a closure's pipes. Decide by what precedes it:
            // a value (ident, `)`, `]`, `?`) means `or`; anything else, or the
            // `move`/`return` keyword, means closure.
            let mut k = i;
            while k > 0 && b[k - 1].is_whitespace() {
                k -= 1;
            }
            let prev = if k == 0 { ' ' } else { b[k - 1] };
            let mut word = String::new();
            if identish(prev) {
                let mut w = k;
                while w > 0 && identish(b[w - 1]) {
                    w -= 1;
                }
                word = b[w..k].iter().collect();
            }
            let closure_keyword = matches!(word.as_str(), "move" | "return" | "else" | "in");
            if (identish(prev) || matches!(prev, ')' | ']' | '?')) && !closure_keyword {
                d.or += 1;
            }
            i += 2;
            continue;
        }
        if b[i] == '!' {
            let prev = if i == 0 { ' ' } else { b[i - 1] };
            if identish(prev) || prev == ':' {
                // a macro bang: `assert!`, `vec!`, `write!`
            } else if b.get(i + 1) == Some(&'=') {
                i += 2; // `!=` — a comparison, counted only in an if/while
                continue;
            } else {
                d.not += 1;
            }
            i += 1;
            continue;
        }
        i += 1;
    }
    d.cmp = comparisons_in_conditions(&b);
    d
}

/// Comparisons inside `if`/`while` conditions.
///
/// The condition runs from the keyword to the `{` that opens its block at paren
/// depth 0 — which is also where an `if let` pattern lives, and a pattern holds
/// no comparison, so it contributes nothing.
fn comparisons_in_conditions(b: &[char]) -> usize {
    let identish = |c: char| c.is_ascii_alphanumeric() || c == '_';
    let mut n = 0usize;
    let mut i = 0usize;
    while i < b.len() {
        let kw = ["if", "while"].iter().find(|k| {
            let k: Vec<char> = k.chars().collect();
            b[i..].starts_with(&k[..])
                && (i == 0 || !identish(b[i - 1]))
                && b.get(i + k.len()).is_some_and(|c| !identish(*c))
        });
        let Some(kw) = kw else {
            i += 1;
            continue;
        };
        let mut j = i + kw.len();
        let mut depth = 0i32;
        while j < b.len() {
            match b[j] {
                '(' | '[' => depth += 1,
                ')' | ']' => depth -= 1,
                '{' if depth <= 0 => break,
                _ => {}
            }
            j += 1;
        }
        let cond = &b[i + kw.len()..j.min(b.len())];
        let mut k = 0usize;
        while k < cond.len() {
            let two = (cond[k], cond.get(k + 1).copied().unwrap_or(' '));
            if matches!(two, ('=', '=') | ('!', '=') | ('<', '=') | ('>', '=')) {
                n += 1;
                k += 2;
                continue;
            }
            // A bare `<`/`>` only when spaced on both sides: unspaced, it is a
            // generic or a turbofish (`Vec<u8>`, `as::<T>`).
            if matches!(cond[k], '<' | '>')
                && k > 0
                && cond[k - 1] == ' '
                && cond.get(k + 1) == Some(&' ')
            {
                n += 1;
            }
            k += 1;
        }
        i = j.max(i + 1);
    }
    n
}

// ── the graded set ───────────────────────────────────────────────────────

/// Every `<path>::<fn>` site a whole-function exclusion covers, with its
/// decision counts.
fn graded_sites() -> BTreeMap<String, Decisions> {
    let root = super::nonvacuity::repo_root().join("src");
    let mut files: Vec<std::path::PathBuf> = Vec::new();
    let mut stack = vec![root.clone()];
    while let Some(dir) = stack.pop() {
        for e in std::fs::read_dir(&dir).expect("read src/").flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|x| x == "rs") {
                files.push(p);
            }
        }
    }
    files.sort();
    // The walk is itself a subject: `src/` reorganising under a gate that
    // reads it is how `destructive_delete_gate` went blind while passing.
    super::nonvacuity::require_enumerated(
        files.len(),
        60,
        "`.rs` files walked under src/",
        "The crate's sources moved — re-point this gate, or it will grade zero live-only \
         bodies and call every one of them pure.",
    );
    let blanked: Vec<(String, String)> = files
        .iter()
        .map(|p| {
            let rel = p
                .strip_prefix(super::nonvacuity::repo_root())
                .expect("under repo root")
                .to_string_lossy()
                .replace('\\', "/");
            let text = super::nonvacuity::subject_text(&rel);
            (rel, blank_noncode(&text))
        })
        .collect();

    let mut sites: BTreeMap<String, Decisions> = BTreeMap::new();
    for (name, ret) in live_only_functions() {
        let mut hits = 0usize;
        for (rel, code) in &blanked {
            for (actual_ret, body) in fn_sites(code, &name) {
                // Same-named siblings are real (`check` also names a struct
                // helper in preflight/cdc_health.rs, `density_probe` exists per
                // engine). The exclusion regex carries the return type when it
                // needs to disambiguate, so honour it; with no return type in
                // the entry, grade EVERY site of that name — a gate that grades
                // one function too many is a nuisance, one that grades none is
                // the defect.
                if let Some(want) = &ret
                    && !actual_ret.starts_with(want.as_str())
                {
                    continue;
                }
                hits += 1;
                sites.insert(format!("{rel}::{name}"), decisions(&body));
            }
        }
        // NON-VACUITY, per name: an exclusion whose function was renamed,
        // inlined or whose return type drifted resolves to NO body, and a
        // gate over no bodies finds no offenders. `.cargo/mutants.toml` is
        // the claim; `src/` is the subject — they must still meet.
        assert!(
            hits > 0,
            "NON-VACUITY: the live-only exclusion for `{name}`{} resolves to no function in \
             src/. Either the function moved/was renamed (re-point the exclusion AND this \
             gate's baseline row), or the exclusion is DEAD — a dead exclusion triages \
             nothing while claiming to.",
            ret.as_deref()
                .map(|r| format!(" (-> {r})"))
                .unwrap_or_default()
        );
    }
    sites
}

// ── the guards ───────────────────────────────────────────────────────────

/// A live-only body decides nothing the offline gate has not been shown.
///
/// RED-proven twice, mutant applied to the PRODUCT and reverted:
///
/// * `let _mutant = true && false;` into the clean body of
///   `refuse_compressed_binlog` — fails with
///   `src/source/mysql/cdc.rs::refuse_compressed_binlog: and=1 … — NOT in the
///   ratchet at all`;
/// * `let _mutant = if 1 > 0 {…}` into the LISTED body of
///   `run_with_reconnect` — fails with `cmp=2 > ceiling … cmp=1`.
///
/// Also RED against a mutant in the GATE: stubbing [`blank_noncode`] to
/// `src.to_string()` (the `runner_frame_gate` defect — a lint that grades its
/// own doc comments) fails this test and the scanner's own test together.
#[test]
fn live_only_bodies_decide_only_through_named_predicates() {
    let baseline: BTreeMap<&str, Decisions> = BASELINE
        .iter()
        .map(|(s, and, or, not, cmp)| {
            (
                *s,
                Decisions {
                    and: *and,
                    or: *or,
                    not: *not,
                    cmp: *cmp,
                },
            )
        })
        .collect();
    let sites = graded_sites();

    // 16 sites today. The floor catches a config or a walk that stopped
    // resolving them — at which point every assertion below passes over the
    // empty set, which is how a source lint reports green while grading
    // nothing.
    super::nonvacuity::require_enumerated(
        sites.len(),
        10,
        "live-only function sites resolved from .cargo/mutants.toml",
        "The whole-function exclusions changed shape — re-read `live_only_functions`.",
    );
    // ONE clean site, not today's three: this floor asks only whether the
    // scanner can still say "pure" at all — a scanner that counts syntax as
    // decisions reports zero clean bodies and every ceiling in the ratchet
    // becomes noise. Set higher, it PREEMPTS the offender verdict below and
    // reports a vacuity failure for what is really a new decision (measured
    // while RED-proving this gate: an `&&` added to `refuse_compressed_binlog`
    // failed here, naming the scanner, instead of naming the offender). The
    // real guard against a noisy scanner is
    // `the_decision_scanner_counts_operators_not_syntax`, which grades it
    // against hand-written bodies.
    super::nonvacuity::require_enumerated(
        sites.values().filter(|d| d.total() == 0).count(),
        1,
        "live-only bodies that are already CLEAN",
        "No live-only body reads as pure — check the decision scanner before the repo.",
    );

    let mut grew: Vec<String> = Vec::new();
    for (site, d) in &sites {
        match baseline.get(site.as_str()) {
            Some(cap) if d.exceeds(*cap) => grew.push(format!(
                "{site}: and={} or={} not={} cmp={} > ceiling and={} or={} not={} cmp={}",
                d.and, d.or, d.not, d.cmp, cap.and, cap.or, cap.not, cap.cmp
            )),
            None if d.total() > 0 => grew.push(format!(
                "{site}: and={} or={} not={} cmp={} — NOT in the ratchet at all",
                d.and, d.or, d.not, d.cmp
            )),
            _ => {}
        }
    }
    assert!(
        grew.is_empty(),
        "a live-only body grew a DECISION the mutation gate cannot reach.\n\
         `.cargo/mutants.toml` excludes these bodies wholesale because no offline test \
         can execute them — so every branch written inline here is a mutant excluded \
         with nothing asked in return, and the next in-diff run will point at it and \
         someone will argue it out one at a time (six such extractions this session).\n\
         Do it once instead: move the decision into a NAMED pure predicate beside the \
         function (`should_reconcile`, `resume_success_gate_applies`, `fold_failures` are \
         the templates), unit-test it, and leave the glue calling it. If the branch \
         genuinely cannot be extracted, raise this site's ceiling in a reviewed diff \
         that says why.\n{}",
        grew.join("\n")
    );
}

/// The ratchet records its wins, or it is a growth allowance.
///
/// Slack in a shrink-only ledger is exactly as wide as every past extraction —
/// a body cleaned last month silently buys back its old operator count. Same
/// rule and same failure shape as `rig_adoption_guard`'s downward test.
///
/// RED-proven with the extraction this ledger exists to bank: deleting the `!`
/// in `src/init/postgres.rs::density_probe` (the shape of moving a negation
/// into `probe_trustworthy`'s caller) without lowering the row fails with
/// `actual … not=0 < ceiling … not=1 — lower it`. It also fired for real
/// while this gate was being written: four rows carried a `move ||` closure
/// counted as an `or`, and the downward test refused them.
#[test]
fn the_purity_ratchet_matches_reality_downward_too() {
    let sites = graded_sites();
    let mut slack: Vec<String> = Vec::new();
    for (site, and, or, not, cmp) in BASELINE {
        let cap = Decisions {
            and: *and,
            or: *or,
            not: *not,
            cmp: *cmp,
        };
        match sites.get(*site) {
            None => slack.push(format!(
                "{site}: listed but no longer a live-only site — drop the row (and check \
                 whether its `.cargo/mutants.toml` exclusion is dead too)"
            )),
            Some(d) if d.under(cap) => slack.push(format!(
                "{site}: actual and={} or={} not={} cmp={} < ceiling and={} or={} not={} \
                 cmp={} — lower it",
                d.and, d.or, d.not, d.cmp, cap.and, cap.or, cap.not, cap.cmp
            )),
            Some(_) => {}
        }
    }
    assert!(
        slack.is_empty(),
        "the live-only purity ratchet has slack; tighten it so each extraction stays \
         banked:\n{}",
        slack.join("\n")
    );
}

/// The scanner must count what cargo-mutants would mutate — and nothing else.
///
/// An INDEPENDENT oracle, not a re-read of src/: every expectation below is a
/// hand-written number over a hand-written body. The false positives it pins
/// are the ones this scanner actually had while being written (a `move ||`
/// closure read as an `or`, `assert!` read as a negation, `Vec<u8>` in an `if`
/// read as a comparison) — each of which would have banked a phantom operator
/// into the ratchet and made a real one invisible under it.
#[test]
fn the_decision_scanner_counts_operators_not_syntax() {
    let count = |src: &str| decisions(&blank_noncode(src));

    assert_eq!(
        count("{ if a && !b { x(); } }"),
        Decisions {
            and: 1,
            or: 0,
            not: 1,
            cmp: 0
        }
    );
    // Closures are not decisions; macro bangs are not negations; a comparison
    // outside a condition is out of this rule's stated scope.
    assert_eq!(
        count(
            "{ std::thread::spawn(move || { assert!(ok); }); \
             let f = || 1; v.unwrap_or_else(|| 0); let q = a == b; }"
        ),
        Decisions::default()
    );
    // …but a real `||` between two values is.
    assert_eq!(count("{ if a() || b { } }").or, 1);
    assert_eq!(count("{ if flags[i] || done { } }").or, 1);
    // Generics and turbofish inside a condition are not comparisons; spaced
    // relational operators are.
    assert_eq!(count("{ if v.parse::<u64>().is_ok() { } }").cmp, 0);
    assert_eq!(count("{ if n > 0 { } while i <= len { } }").cmp, 2);
    assert_eq!(count("{ if a != b { } }").cmp, 1);
    // `!=` is a comparison, never a negation — double-counting it would make
    // every ceiling in the ratchet a lie in the same direction.
    assert_eq!(count("{ if a != b { } }").not, 0);
    // Comments and strings are TEXT. A doc comment naming `&&` is how
    // `runner_frame_gate` was satisfiable by prose.
    assert_eq!(
        count("{ /* a && b */ let s = \"x || y\"; let r = r#\"!z\"#; }"),
        Decisions::default()
    );
    // A lifetime is not a char literal — mis-lexing `'a` swallows the rest of
    // the body as a string and reports a pure function.
    assert_eq!(count("{ let x: &'static str = \"\"; if !p { } }").not, 1);
}

/// The graded set is read out of the config, not typed in.
///
/// Pins the SHAPES `live_only_functions` must recognise against hand-written
/// input, and asserts the two kinds of entry stay separated: a whole-function
/// stub is a claim about a BODY (graded here), an operator entry is a triage of
/// one mutant (not graded here — and the thing this gate exists to stop needing).
#[test]
fn only_whole_function_exclusions_are_read_as_live_only_claims() {
    let real = live_only_functions();
    let names: Vec<&str> = real.iter().map(|(n, _)| n.as_str()).collect();
    for expect in ["run_pool", "check", "density_probe", "pg_run_export"] {
        assert!(
            names.contains(&expect),
            "`{expect}` is excluded wholesale in .cargo/mutants.toml but this gate did not \
             read it as a live-only claim: {names:?}"
        );
    }
    // Operator/equivalence entries name a function too — and must NOT be read
    // as a body claim, or the gate would demand purity of `hash_value` and
    // `overlay_measured_rows`, which are unit-tested and mutation-graded.
    for not_a_body in [
        "overlay_measured_rows",
        "hash_value",
        "compute_part_checksums",
    ] {
        assert!(
            !names.contains(&not_a_body),
            "`{not_a_body}` is an OPERATOR triage, not a whole-function live-only claim, \
             but this gate graded it: {names:?}"
        );
    }
    // The return type is what tells same-named siblings apart; losing it makes
    // `check` mean every `fn check` in the tree.
    let check_ret = real
        .iter()
        .find(|(n, _)| n == "check")
        .and_then(|(_, r)| r.clone());
    assert_eq!(
        check_ret.as_deref(),
        Some("Result<bool>"),
        "the `check` exclusion's return-type discriminator was not decoded"
    );
}
