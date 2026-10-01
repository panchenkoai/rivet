//! A live test that names a PostgreSQL replication slot builds the shared `Slot`
//! drop guard before its first command that can create the slot (#376 class).

use regex::Regex;

/// Every `fn` in `text` whose slot name has no `Slot::new`/`Slot::on` guard before the first run.
fn late_guards(text: &str) -> Vec<String> {
    let fn_start = Regex::new(r"(?m)^\s*(?:pub(?:\([a-z]+\))?\s+)?fn\s+(\w+)").unwrap();
    let named = Regex::new(r#"unique_name\(\s*(?:&format!\(\s*)?"[^"]*_slot(?:_\w)?""#).unwrap();
    let bound = Regex::new(
        r#"let\s+(?:mut\s+)?(\w+)\s*=\s*(?:super::)?unique_name\(\s*(?:&format!\(\s*)?"[^"]*_slot(?:_\w)?""#,
    )
    .unwrap();
    let run = Regex::new(
        r"(?:\b\w*run\w*|\brivet_ok|\binit_ok|\bcycle|\bfull_cycle|pg_create_logical_replication_slot|Command::new|\.cli|\.spawn\w*)\(",
    )
    .unwrap();
    let starts: Vec<(usize, String)> = fn_start
        .captures_iter(text)
        .map(|c| (c.get(0).unwrap().start(), c[1].to_string()))
        .collect();
    let mut bad = Vec::new();
    for (i, (start, name)) in starts.iter().enumerate() {
        let end = starts.get(i + 1).map_or(text.len(), |s| s.0);
        let body = &text[*start..end];
        let binds: Vec<_> = bound.captures_iter(body).collect();
        if named.find_iter(body).count() > binds.len() {
            bad.push(format!(
                "{name}: a slot name passed inline, bind it and guard it"
            ));
        }
        for b in binds {
            let var = &b[1];
            let rest = &body[b.get(0).unwrap().end()..];
            let Some(first_run) = run.find(rest).map(|m| m.start()) else {
                continue;
            };
            let guard =
                Regex::new(&format!(r"Slot::(?:new\(\s*{var}\b|on\([^,]+,\s*{var}\b)")).unwrap();
            if guard.find(rest).is_none_or(|g| g.start() > first_run) {
                bad.push(format!(
                    "{name}: `{var}` has no Slot guard before its first run"
                ));
            }
        }
    }
    bad
}

#[test]
fn every_named_slot_is_guarded_before_the_first_run() {
    let root = super::nonvacuity::repo_root();
    let mut files: Vec<_> = std::fs::read_dir(root.join("tests/live"))
        .expect("read tests/live")
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "rs"))
        .collect();
    files.push(root.join("tests/common/rig/mod.rs"));
    let mut bad = Vec::new();
    let mut slots = 0;
    for p in &files {
        let text =
            std::fs::read_to_string(p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()));
        slots += text.matches("Slot::new(").count();
        for b in late_guards(&text) {
            bad.push(format!("{}: {b}", p.file_name().unwrap().to_string_lossy()));
        }
    }
    super::nonvacuity::require_enumerated(
        slots,
        40,
        "`Slot::new(` guards under tests/live",
        "The slot guard moved or was renamed; point this guard at it.",
    );
    assert!(
        bad.is_empty(),
        "a slot a timed-out or panicking test can leak: build `Slot::new(name.clone())` \
         right after naming the slot, before any run or pg_create_logical_replication_slot:\n{}",
        bad.join("\n")
    );
}

#[test]
fn the_scan_flags_a_guard_built_after_the_first_run() {
    let late = "fn t() {\n let slot = unique_name(\"x_slot\");\n rig.run_ok();\n let _s = Slot::new(slot.clone());\n}\n";
    let early = "fn t() {\n let slot = unique_name(\"x_slot\");\n let _s = Slot::new(slot.clone());\n rig.run_ok();\n}\n";
    let inline =
        "fn t() {\n let r = Rig::pg_cdc(\"t\", &unique_name(\"x_slot\"));\n r.run_ok();\n}\n";
    assert_eq!(late_guards(late).len(), 1);
    assert!(late_guards(early).is_empty());
    assert_eq!(late_guards(inline).len(), 1);
}
