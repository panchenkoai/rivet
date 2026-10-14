//! The known-red ledger (dev/release_oracle/known_red.py) is held by three ratchets: its
//! entries never grow unseen, its horizon never moves unseen, and every open-defect entry
//! names a live cell that exists. A red gate is red ONLY for recorded reasons; this keeps the
//! record from quietly becoming a place where reds go to be forgotten.

use std::path::PathBuf;

const LEDGER: &str = "dev/release_oracle/known_red.py";

/// The ledger's entries: `_open(...)` cells and bare `KnownRed(...)` gate rows. Shrink-only.
// 25 (2026-10-14): 21 open-defect cells, 4 gate rows (Oracle cdc-load x2, Oracle TIMESTAMP(9) sentinels x2).
// A new entry is a new acknowledged red: raise this in the same diff, with the entry's reason;
// a fixed one lowers it.
const ENTRY_CEILING: usize = 25; // ratchet-pin: known-red-entries

/// The horizon every `_open` entry expires at; moves only here and in the ledger together.
const EXPIRES: &str = "2026-10-31"; // pinned: the ledger's `_EXPIRES`, moved only together with it

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn ledger() -> String {
    std::fs::read_to_string(root().join(LEDGER)).expect("the known-red ledger")
}

/// The body of `KNOWN_RED: tuple[KnownRed, ...] = ( ... )`.
fn entries_block(text: &str) -> &str {
    let start = text
        .find("KNOWN_RED: tuple[KnownRed, ...] = (")
        .expect("the ledger declares KNOWN_RED");
    let rest = &text[start..];
    let end = rest
        .find("\n)\n")
        .expect("the tuple closes on its own line");
    &rest[..end]
}

/// `(open cells, gate rows)`: the `module::fn` of every `_open(` entry and the match text of every bare `KnownRed(` entry.
fn entries(text: &str) -> (Vec<String>, Vec<String>) {
    let block = entries_block(text);
    let mut open = Vec::new();
    let mut rows = Vec::new();
    for line in block.lines() {
        let t = line.trim_start();
        if let Some(rest) = t.strip_prefix("_open(\"") {
            open.push(rest.split('"').next().unwrap().to_string());
        } else if let Some(rest) = t
            .strip_prefix("KnownRed('")
            .or_else(|| t.strip_prefix("KnownRed(\""))
        {
            rows.push(rest.split(['\'', '"']).next().unwrap().to_string());
        }
    }
    (open, rows)
}

#[test]
fn known_red_entries_never_grow_unseen() {
    let (open, rows) = entries(&ledger());
    super::nonvacuity::require_enumerated(
        open.len() + rows.len(),
        10,
        "known-red entries",
        "the ledger moved or changed shape",
    );
    assert_eq!(
        open.len() + rows.len(),
        ENTRY_CEILING,
        "known-red entries: {} open-defect cells + {} gate rows, ceiling {ENTRY_CEILING}. A new acknowledged red \
         raises the ceiling in the same diff with its reason; a fixed one lowers it.",
        open.len(),
        rows.len()
    );
}

#[test]
fn the_known_red_horizon_moves_only_with_this_guard() {
    let text = ledger();
    assert!(
        text.contains(&format!("\n_EXPIRES = \"{EXPIRES}\"\n")),
        "the ledger's horizon is {EXPIRES} and moves only together with this guard: an extension is a reviewed diff, not an edit"
    );
    // No entry outlives the horizon: every quoted date in the ledger is on or before it.
    let is_date = |s: &str| {
        s.len() == 10
            && s.bytes().enumerate().all(|(i, b)| {
                if i == 4 || i == 7 {
                    b == b'-'
                } else {
                    b.is_ascii_digit()
                }
            })
    };
    let later: Vec<&str> = text
        .match_indices('"')
        .filter_map(|(i, _)| {
            text.get(i + 1..i + 11)
                .filter(|s| is_date(s) && text[i + 11..].starts_with('"'))
        })
        .filter(|s| *s > EXPIRES)
        .collect();
    assert_eq!(
        later,
        Vec::<&str>::new(),
        "known-red dates past the horizon {EXPIRES}. An entry that must outlive the horizon moves the horizon here, in the open."
    );
}

#[test]
fn every_open_entry_names_a_live_cell_that_exists() {
    let (open, rows) = entries(&ledger());
    let tests = super::chunking_matrix_guard::all_test_fn_names();
    let missing: Vec<&String> = open
        .iter()
        .filter(|cell| {
            let (module, fn_name) = cell.rsplit_once("::").unwrap_or(("", cell));
            let file = root().join("tests/live").join(format!("{module}.rs"));
            !fn_name.starts_with("open_defect_")
                || !tests.contains(fn_name)
                || !std::fs::read_to_string(&file)
                    .is_ok_and(|t| t.contains(&format!("fn {fn_name}(")))
        })
        .collect();
    assert_eq!(
        missing,
        Vec::<&String>::new(),
        "known-red entries that name no `open_defect_*` test under tests/live/<module>.rs. \
         A fixed defect deletes its entry; a renamed cell renames its entry."
    );
    // A gate row names a cell family the gate composes (`upgrade[...]`, `sentinels[...]`).
    let gate: String = std::fs::read_dir(root().join("dev/release_oracle"))
        .expect("the gate's sources")
        .flatten()
        .filter(|e| e.path().extension().is_some_and(|x| x == "py"))
        .map(|e| std::fs::read_to_string(e.path()).unwrap_or_default())
        .collect();
    let orphan_rows: Vec<&String> = rows
        .iter()
        .filter(|m| {
            let family = m.split('[').next().unwrap_or("");
            family.is_empty() || !gate.contains(&format!("\"{family}["))
        })
        .collect();
    assert_eq!(
        orphan_rows,
        Vec::<&String>::new(),
        "known-red gate rows whose cell family the gate no longer composes"
    );
}

#[test]
fn the_entry_parser_reads_both_shapes() {
    let text = "x\nKNOWN_RED: tuple[KnownRed, ...] = (\n    _open(\"live_a::open_defect_one\",\n          \"s\", \"r\"),\n    KnownRed('upgrade[oracle/x]: boom',\n             \"why\",\n             \"2026-10-31\"),\n    KnownRed(\"sentinels[y]: z\", \"why\", \"2026-10-31\"),\n)\n\ndef match():\n";
    let (open, rows) = entries(text);
    assert_eq!(open, ["live_a::open_defect_one"]);
    assert_eq!(rows, ["upgrade[oracle/x]: boom", "sentinels[y]: z"]);
}
