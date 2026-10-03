//! A runner that cannot run must report `Skipped`, never an empty result.
//!
//! `assert_all_pass` (tests/oracle_fixture_matrix.rs) panics on a `Skipped`
//! outcome — the strength floor — and walks straight through an EMPTY list,
//! because a `for` over nothing executes nothing. So a runner that answers a
//! missing environment with `Ok(Vec::new())` makes every cell that calls it
//! report green while verifying nothing.
//!
//! MEASURED 2026-08-25: all six runners in `tests/harness/mod.rs` did exactly
//! that, and `cargo test --test oracle_fixture_matrix -- --ignored` reported
//! `20 passed` in **0.00s** on a machine with no warehouse credentials — which
//! is every machine, since `RIVET_TEST_GCS_BUCKET` appears in no workflow. The
//! doc comment on `run` had called an empty oracle list a strength-floor failure
//! since it was written.
//!
//! This guard is source-shaped rather than behavioural on purpose: the runners
//! read process environment, and a test that mutates env to observe them races
//! every other test in the binary. What it checks is exactly the shape that
//! failed — an early return of an empty vec from a runner whose result feeds the
//! floor.

use std::path::{Path, PathBuf};

fn harness_source() -> String {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/harness/mod.rs");
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
}

/// The runners are DERIVED from the source, never listed here: a seventh added
/// tomorrow is asked the same question without anyone remembering to add a row.
fn runner_bodies(src: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut rest = src;
    while let Some(i) = rest.find("    pub fn run") {
        let after = &rest[i + 4..];
        let name: String = after["pub fn ".len()..]
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        // Body = up to the next `    pub fn ` (or end): enough to see an early
        // return, which is all this guard is about.
        let body_start = i + 4;
        let body_end = after[4..]
            .find("\n    pub fn ")
            .map(|j| body_start + 4 + j)
            .unwrap_or(rest.len());
        out.push((name, rest[body_start..body_end].to_string()));
        rest = &rest[body_end..];
    }
    assert!(
        out.len() >= 6,
        "derived only {} runners from tests/harness/mod.rs — the derivation is broken, \
         and a broken derivation grades nothing while looking green",
        out.len()
    );
    out
}

#[test]
fn no_harness_runner_answers_a_missing_environment_with_an_empty_result() {
    let src = harness_source();
    let mut offenders = Vec::new();
    for (name, body) in runner_bodies(&src) {
        // Only runners whose result feeds the strength floor: the ones returning a
        // list of outcomes. A helper returning something else is not this class.
        if !body.contains("OracleOutcome") {
            continue;
        }
        // SHAPE, not one literal spelling. The first cut grepped `Ok(Vec::new())`
        // and `return Ok(vec![]);` walked straight past it — one word, on any of the
        // six runners, and the twenty warehouse cells go vacuous again. MEASURED:
        // that edit left all four guards in this file GREEN.
        let empty_return = ["Ok(Vec::new())", "Ok(vec![])", "Ok(Default::default())"]
            .iter()
            .any(|lit| body.contains(lit));
        // ...and the positive requirement, which is what the class is actually
        // about: a runner that can skip must be able to SAY so.
        let can_say_skipped = body.contains("Skipped");
        if empty_return || !can_say_skipped {
            offenders.push(name);
        }
    }
    assert!(
        offenders.is_empty(),
        "these runners return an EMPTY result instead of a `Skipped` outcome: {offenders:?}. \
         `assert_all_pass` panics on Skipped and walks straight through an empty list, so \
         every cell calling one of these reports green while verifying nothing — measured \
         at `20 passed in 0.00s` before this was closed. Return \
         `Ok(vec![(name, OracleOutcome::Skipped {{ why: SKIP_WHY.to_string() }})])`."
    );
}

/// The positive control: the thing this guard looks for must be findable. A grep
/// that matches nothing reads identically to a grep that found no problem — which
/// is the same defect one layer up.
#[test]
fn the_guard_can_actually_see_a_runner_body() {
    let src = harness_source();
    let bodies = runner_bodies(&src);
    let with_outcomes = bodies
        .iter()
        .filter(|(_, b)| b.contains("OracleOutcome"))
        .count();
    assert!(
        with_outcomes >= 6,
        "only {with_outcomes} of {} derived runners mention OracleOutcome — the parse is \
         reading the wrong thing, and the guard above would pass over any input",
        bodies.len()
    );
    assert!(
        bodies.iter().any(|(_, b)| b.contains("SKIP_WHY")),
        "no runner references SKIP_WHY — either the skip path was removed (fine, say so \
         here) or the parse is not reading bodies at all (not fine)"
    );
}

/// Every fault INJECTOR must verify its own injection landed.
///
/// A sibling of the class above, and the reason the toxiproxy tests survived the
/// 2026-08-25 audit: `toxi_add_latency` asserts the admin API answered 200, so a
/// toxic that fails to install panics the test instead of letting an ordinary,
/// un-degraded export pass as "survived the fault". Move that assertion into the
/// caller and eleven tests become vacuous at once; leave it in the helper and no
/// caller can get it wrong.
///
/// Injectors only. `toxi_reset_toxics` is TEARDOWN — asserting there would fail
/// tests during cleanup, reporting a fixture problem as a product one.
#[test]
fn every_toxiproxy_fault_injector_asserts_its_own_injection_landed() {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/common/toxi.rs");
    let src = std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()));

    let mut injectors = 0;
    let mut silent = Vec::new();
    let mut rest = src.as_str();
    while let Some(i) = rest.find("pub fn toxi_") {
        let name: String = rest[i + "pub fn ".len()..]
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        let end = rest[i + 5..]
            .find("\npub fn ")
            .map(|j| i + 5 + j)
            .unwrap_or(rest.len());
        let body = &rest[i..end];
        rest = &rest[end..];
        // An injector CHANGES the link; reset/teardown restores it.
        if name.contains("reset") {
            continue;
        }
        injectors += 1;
        // The assertion must reference the STATUS BINDING, not merely sit near the
        // word `code`. MEASURED: keeping `let (code, body) = toxi_admin(...)`, adding
        // `let _ = (code, body);` and asserting the toxic NAME is non-empty left this
        // guard GREEN while the toxic could silently fail to install — the "rivet
        // survived a fault that never happened" case it exists to prevent.
        let checks_the_status = ["code == 200", "code != 200", "code == 204", "status == 200"]
            .iter()
            .any(|pat| body.contains(pat));
        if !(body.contains("assert") && checks_the_status) {
            silent.push(name);
        }
    }
    assert!(
        injectors >= 5,
        "derived only {injectors} injectors from tests/common/toxi.rs — a broken \
         derivation grades nothing while looking green"
    );
    assert!(
        silent.is_empty(),
        "these fault injectors do not check that the fault was actually installed: \
         {silent:?}. A toxic that fails to install leaves the export running over a \
         healthy link, and the test then reports that rivet survived a fault that \
         never happened."
    );
}

/// A live test that does not run must say so in ONE countable form.
///
/// `cargo test` prints `ok` for a test that returned early, so a skip and a pass
/// are indistinguishable in the output — the same shape that let 20 warehouse
/// cells report `20 passed` in 0.00s. The harness half got an explicit `Skipped`
/// outcome; the live suite has no such channel, so the marker IS the channel.
///
/// Before this, the thirteen skip sites said `SKIP`, `skip:`, `skipping:` and
/// `RIVET_TINYFS_DIR not set — skipping` in four different shapes. A release lane
/// cannot grep for all of them, which is why the skips were invisible rather than
/// merely inconvenient.
///
/// Narrow ON PURPOSE. The obvious guard — find every early return and ask whether
/// it announced itself — was tried on the sibling class today and produced ten
/// false positives out of eleven (the word "kill" in a comment, assertions living
/// in a shared runner). This one looks for a specific string in a specific macro,
/// which is exactly the drift it exists to catch and nothing else.
#[test]
fn a_live_test_that_skips_says_so_through_the_one_marker() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut offenders = Vec::new();
    let mut marked = 0usize;
    let mut stack: Vec<PathBuf> = vec![root.clone()];
    while let Some(dir) = stack.pop() {
        for e in std::fs::read_dir(&dir).expect("read tests dir") {
            let p = e.expect("dir entry").path();
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            if p.extension().and_then(|x| x.to_str()) != Some("rs") {
                continue;
            }
            // The helper's own definition and this guard both mention the token.
            if p.ends_with("common/mod.rs") || p.ends_with("skip_is_not_a_pass_guard.rs") {
                continue;
            }
            let src = std::fs::read_to_string(&p).expect("read a test file");
            marked += src.matches("skip_live(").count();
            // WINDOW-based, not line-based. The first cut checked `is_print &&
            // says_skip` on ONE line and sailed past a multi-line macro — where
            // `eprintln!(` sits on its own line and the message on the next, which
            // is how rustfmt writes any message long enough to matter. Found by
            // RED-proving the guard rather than by reading it: reverting a site to
            // `eprintln!(\n  "SKIP: ...` left it green.
            let bytes = src.as_bytes();
            for m in ["println!(", "eprintln!("] {
                let mut from = 0usize;
                while let Some(rel) = src[from..].find(m) {
                    let at = from + rel;
                    from = at + m.len();
                    // `eprintln!(` also matches inside `println!(` — skip the inner hit.
                    if m == "println!(" && at > 0 && bytes[at - 1] == b'e' {
                        continue;
                    }
                    let line_start = src[..at].rfind('\n').map(|i| i + 1).unwrap_or(0);
                    if src[line_start..at].trim_start().starts_with("//") {
                        continue;
                    }
                    // The macro's arguments, bounded so a later unrelated "skip"
                    // cannot be attributed to this call.
                    let end = (at + 300).min(src.len());
                    let win = &src[at..src[at..end].find(");").map(|i| at + i).unwrap_or(end)];
                    if !win.to_lowercase().contains("skip") {
                        continue;
                    }
                    if win.contains("skip_live") || win.contains("RIVET-SKIP {who}") {
                        continue;
                    }
                    let line_no = src[..at].matches('\n').count() + 1;
                    offenders.push(format!(
                        "{}:{}  {}",
                        p.strip_prefix(&root).unwrap_or(&p).display(),
                        line_no,
                        win.replace('\n', " ").chars().take(90).collect::<String>()
                    ));
                }
            }
        }
    }
    assert!(
        marked >= 9,
        "found only {marked} `skip_live(` call sites — the sweep that introduced the \
         marker converted nine, so a number below that means they were reverted or \
         this guard is reading the wrong tree, and it would pass over anything"
    );
    assert!(
        offenders.is_empty(),
        "these announce a skip in their own words instead of through `skip_live`, so \
         a release lane cannot count them:\n  {}",
        offenders.join("\n  ")
    );
}

/// A missing environment variable is a counted skip, never a silent green pass.
///
/// The shape is `let Ok(x) = std::env::var(..) else { return; };` — measured 2026-10-01
/// in nine tests across src/ and tests/ (Snowflake, BigQuery, the Postgres state
/// backend), each printing `ok` with no credential and four of them printing nothing.
#[test]
fn a_missing_env_var_returns_through_skip_live() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut offenders = Vec::new();
    let mut seen = 0usize;
    let mut stack = vec![root.join("src"), root.join("tests")];
    while let Some(dir) = stack.pop() {
        for p in std::fs::read_dir(&dir)
            .expect("read dir")
            .map(|e| e.expect("dir entry").path())
        {
            if p.is_dir() {
                stack.push(p);
                continue;
            }
            if p.extension().and_then(|x| x.to_str()) != Some("rs")
                || p.ends_with("skip_is_not_a_pass_guard.rs")
            {
                continue;
            }
            let src = std::fs::read_to_string(&p).expect("read a source file");
            let mut from = 0usize;
            while let Some(rel) = src[from..].find("std::env::var(") {
                let at = from + rel;
                from = at + 1;
                let line_start = src[..at].rfind('\n').map_or(0, |i| i + 1);
                let head = src[line_start..at].trim_start();
                if !(head.starts_with("let Ok(") || head.starts_with("let Some(")) {
                    continue;
                }
                let stmt_end = src[at..].find(';').map_or(src.len(), |i| at + i);
                let Some(else_rel) = src[at..stmt_end].find("else {") else {
                    continue;
                };
                let body_start = at + else_rel;
                let body_end = src[body_start..]
                    .find("};")
                    .map_or(src.len(), |i| body_start + i);
                let body = &src[body_start..body_end];
                if !body.contains("return") {
                    continue;
                }
                seen += 1;
                if !body.contains("skip_live(") {
                    let line_no = src[..at].matches('\n').count() + 1;
                    offenders.push(format!(
                        "{}:{line_no}",
                        p.strip_prefix(&root).unwrap_or(&p).display()
                    ));
                }
            }
        }
    }
    // 9 -> 7 (2026-10-02): three tests/live sites now go through `pg_state_url()`, which
    // `every_env_gated_live_test_records_its_skip` below grades in every shape.
    assert!(
        seen >= 7,
        "found only {seen} env-var let-else returns — the scan lost its subject"
    );
    assert!(
        offenders.is_empty(),
        "these return early on a missing env var without `skip_live`, so a missing \
         credential reads as a pass:\n  {}",
        offenders.join("\n  ")
    );
}

/// Every `.rs` file under `dir`, recursively.
fn rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for e in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read {}: {e}", dir.display())) {
        let p = e.unwrap().path();
        if p.is_dir() {
            rs_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

/// `line` with its string literals blanked, so a `?` or `return` inside a message is not code.
fn without_strings(line: &str) -> String {
    let mut out = String::new();
    let mut in_str = false;
    let mut escaped = false;
    for c in line.chars() {
        match (in_str, c) {
            (true, '\\') if !escaped => escaped = true,
            (true, '"') if !escaped => in_str = false,
            (true, _) => escaped = false,
            (false, '"') => in_str = true,
            (false, c) => out.push(c),
        }
    }
    out
}

/// True when the function enclosing `lines[i]` returns a `Result`: its `?` names an error to a caller.
fn in_result_fn(lines: &[&str], i: usize) -> bool {
    let Some(start) = (0..=i).rev().find(|&j| lines[j].contains("fn ")) else {
        return false;
    };
    let sig: String = lines[start..=i]
        .iter()
        .take_while(|l| !l.contains('{'))
        .chain(lines[start..=i].iter().find(|l| l.contains('{')))
        .copied()
        .collect();
    sig.contains("Result<")
}

/// True for a line that leaves the function carrying nothing: `return;`, `return None`, `=> None`, a bare `None`.
fn is_silent_exit(code: &str) -> bool {
    let t = code
        .trim()
        .trim_end_matches(',')
        .trim_end_matches(';')
        .trim();
    t == "return"
        || t == "None"
        || t == "return None"
        || t.starts_with("return skip_live(")
        || t.contains("=> return")
        || t.contains("=> None")
        || t.contains("{ return }")
        || t.contains("{ return; }")
}

/// `(file:line, why)` for every env lookup whose failure branch leaves the test without a skip record.
fn silent_env_exits(path: &Path, src: &str) -> Vec<(String, &'static str)> {
    let lines: Vec<&str> = src.lines().collect();
    let mut out = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        if !(line.contains("env::var(") || line.contains("env::var_os(")) {
            continue;
        }
        let site = format!("{}:{}", path.display(), i + 1);
        if without_strings(line).contains('?') && !in_result_fn(&lines, i) {
            out.push((
                site,
                "a bare `?` on the lookup yields None/() with no skip record",
            ));
            continue;
        }
        // The branches the lookup feeds are written within the next few lines of the same item:
        // every exit that carries nothing needs a `skip_live` of its own, so two failure arms need two.
        let window: Vec<String> = lines[i..(i + 9).min(lines.len())]
            .iter()
            .enumerate()
            .take_while(|(k, l)| {
                *k == 0 || !(l.starts_with('}') || l.trim_start().starts_with("fn "))
            })
            .map(|(_, l)| without_strings(l))
            .collect();
        let exits = window.iter().filter(|l| is_silent_exit(l)).count();
        let skips = window.iter().filter(|l| l.contains("skip_live")).count();
        if exits > skips {
            out.push((
                site,
                "an exit after the lookup with no `skip_live` call of its own",
            ));
        }
    }
    out
}

/// Every env lookup under tests/live + tests/common, in ANY shape: the let-else scan above
/// missed `env::var(..).ok()?` (`state_url()`, which left the v30 cursor regression test green
/// in 0.00 s with no record) and `match .. { _ => return }` (ten live_pg_state tests).
#[test]
fn every_env_gated_live_test_records_its_skip() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    rs_files(&root.join("tests/live"), &mut files);
    rs_files(&root.join("tests/common"), &mut files);
    let mut lookups = 0usize;
    let mut offenders = Vec::new();
    for f in &files {
        let src = std::fs::read_to_string(f).unwrap();
        lookups += src.matches("env::var").count();
        offenders.extend(silent_env_exits(f, &src));
    }
    assert!(
        lookups >= 25,
        "derived only {lookups} env lookups under tests/live + tests/common — the derivation is \
         broken, and a broken derivation grades nothing while looking green"
    );
    assert!(
        offenders.is_empty(),
        "{} env lookup(s) leave a test green without a skip record — call `skip_live` (or a helper \
         that does, like `pg_state_url`) before returning:\n{}",
        offenders.len(),
        offenders
            .iter()
            .map(|(site, why)| format!("  {site}: {why}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn the_guard_sees_the_silent_shapes_and_not_the_recorded_ones() {
    let bad = [
        "fn a() -> Option<String> {",
        "    let u = std::env::var(\"X\").ok()?;",
        "    Some(u)",
        "}",
        "fn b() {",
        "    if std::env::var(\"Y\").is_err() {",
        "        return;",
        "    }",
        "}",
        "fn c() -> Option<String> {",
        "    match std::env::var(\"Z\") {",
        "        Ok(u) if u.starts_with(\"p\") => Some(u),",
        "        Ok(_) => {",
        "            skip_live(\"not p\");",
        "            None",
        "        }",
        "        Err(_) => None,",
        "    }",
        "}",
    ]
    .join("\n");
    let got = silent_env_exits(Path::new("t.rs"), &bad);
    assert_eq!(got.len(), 3, "{got:?}");
    let ok = [
        "fn c() {",
        "    let Ok(u) = std::env::var(\"X\") else {",
        "        return skip_live(\"X unset?\");",
        "    };",
        "}",
        "fn d(&self) -> Result<PathBuf, String> {",
        "    std::env::var(\"B\").map_err(|_| \"B\".to_string())?;",
        "    Ok(x)",
        "}",
        "fn e() -> String {",
        "    std::env::var(\"Z\").unwrap_or_else(|_| \"z\".into())",
        "}",
        "fn f() {",
        "    if std::env::var(\"W\").is_err() {",
        "        skip_live(",
        "            \"W unset\",",
        "        );",
        "        return;",
        "    }",
        "}",
    ]
    .join("\n");
    let got = silent_env_exits(Path::new("t.rs"), &ok);
    assert!(got.is_empty(), "{got:?}");
}
