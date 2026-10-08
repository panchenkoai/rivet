//! Drift guard for `docs/guarantee-matrix.yaml`: every guarantee rivet states to its users,
//! tied to the sentence that states it and to the cells that would fail if it were false.

use std::collections::BTreeSet;

use serde_yaml_ng::Value;

use super::chunking_matrix_guard::{all_test_fn_names, test_closure};
use super::nonvacuity::{repo_root, require_enumerated, subject_text};

const MATRIX: &str = "docs/guarantee-matrix.yaml";
const KNOWN_RED: &str = "dev/release_oracle/known_red.py";
const GATE_DIR: &str = "dev/release_oracle";

/// Rows not fully held: a `gap` cell or a live contradiction. Shrink-only; LOWER it with the row.
// 30 -> 28 (2026-10-08): CDC refusals carry their code (#497): the lost-log and stable-code rows lost their last contradiction.
const ROWS_NOT_HELD: usize = 28; // ratchet-pin: guarantee-rows-not-held
/// `gap` cells over all rows. Shrink-only; LOWER it when a cell is written.
const GAP_CELLS: usize = 43; // ratchet-pin: guarantee-gap-cells

fn rows() -> Vec<Value> {
    let doc: Value = serde_yaml_ng::from_str(&subject_text(MATRIX))
        .unwrap_or_else(|e| panic!("parse {MATRIX}: {e}"));
    let out = doc
        .get("guarantees")
        .and_then(Value::as_sequence)
        .unwrap_or_else(|| panic!("{MATRIX} must have a `guarantees:` sequence"))
        .clone();
    require_enumerated(
        out.len(),
        50,
        &format!("`guarantees:` rows in {MATRIX}"),
        "Restore the rows or re-point this reader: a guarantee ledger that parses to nothing \
         holds every promise in the docs.",
    );
    out
}

fn text(v: &Value, k: &str) -> Option<String> {
    v.get(k).and_then(Value::as_str).map(str::to_string)
}

fn id(row: &Value) -> String {
    text(row, "id").unwrap_or_else(|| "<no id>".into())
}

fn list(row: &Value, k: &str) -> Vec<Value> {
    row.get(k)
        .and_then(Value::as_sequence)
        .cloned()
        .unwrap_or_default()
}

/// Whitespace-insensitive form, so a sentence the doc wraps over lines is found as written.
fn squash(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// One cell: a Rust test, a gate stage, or an admitted gap.
enum Cell {
    Test { name: String, asserts: String },
    Gate { stage: String, asserts: String },
    Gap,
}

/// A row's `held_by` cells keyed by column, or what is malformed about them.
fn cells(row: &Value) -> Result<Vec<(String, Cell)>, String> {
    let held = row
        .get("held_by")
        .and_then(Value::as_mapping)
        .ok_or("no `held_by:` mapping")?;
    let mut out = Vec::new();
    for (col, cell) in held {
        let col = col.as_str().unwrap_or("<non-string column>").to_string();
        let kinds: Vec<&str> = ["test", "gate", "gap"]
            .into_iter()
            .filter(|k| cell.get(k).is_some())
            .collect();
        let parsed = match kinds.as_slice() {
            ["test"] | ["gate"] => {
                let name = text(cell, kinds[0]).unwrap_or_default();
                let asserts = text(cell, "asserts").unwrap_or_default();
                if name.is_empty() || asserts.trim().len() < 12 {
                    return Err(format!(
                        "{col}: a `{}` cell names the cell and an `asserts:` of at least 12 \
                         characters copied from its body",
                        kinds[0]
                    ));
                }
                if kinds[0] == "test" {
                    Cell::Test { name, asserts }
                } else {
                    Cell::Gate {
                        stage: name,
                        asserts,
                    }
                }
            }
            ["gap"] => {
                let why = text(cell, "gap").unwrap_or_default();
                if why.trim().len() < 20 {
                    return Err(format!("{col}: a `gap:` states its reason"));
                }
                Cell::Gap
            }
            _ => {
                return Err(format!(
                    "{col}: exactly one of test / gate / gap, got {kinds:?}"
                ));
            }
        };
        out.push((col, parsed));
    }
    if out.is_empty() {
        return Err("`held_by:` has no cell".into());
    }
    Ok(out)
}

/// Every `.py` under the gate package, as (relative path, text).
fn gate_sources() -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut stack = vec![repo_root().join(GATE_DIR)];
    while let Some(dir) = stack.pop() {
        for p in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
            let p = p.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|e| e == "py") {
                out.push((
                    p.display().to_string(),
                    std::fs::read_to_string(&p).unwrap_or_default(),
                ));
            }
        }
    }
    out
}

/// What is wrong with a gate cell: the stage must be defined, CALLED, and carry its assertion.
fn gate_cell_problem(sources: &[(String, String)], stage: &str, asserts: &str) -> Option<String> {
    let def = format!("def {stage}(");
    let Some((_, body)) = sources.iter().find(|(_, t)| t.contains(&def)) else {
        return Some(format!("no `{def}` under {GATE_DIR}"));
    };
    let calls: usize = sources
        .iter()
        .map(|(_, t)| t.matches(stage).count() - t.matches(&def).count())
        .sum();
    if calls == 0 {
        return Some(format!("`{stage}` is defined and never referenced"));
    }
    (!squash(body).contains(&squash(asserts)))
        .then(|| format!("the module of `{stage}` does not contain {asserts:?}"))
}

/// The contradictions a row lists, as (kind, reference).
fn contradictions(row: &Value) -> Vec<(String, String)> {
    let mut out = Vec::new();
    if let Some(c) = row.get("contradicted_by") {
        for kind in ["known_red", "matrix_gap", "doc"] {
            for v in list(c, kind) {
                out.push((kind.to_string(), v.as_str().unwrap_or_default().to_string()));
            }
        }
    }
    out
}

/// Whether a `file#needle` reference still resolves to text in that file.
fn file_ref_resolves(reference: &str) -> bool {
    let Some((file, needle)) = reference.split_once('#') else {
        return false;
    };
    std::fs::read_to_string(repo_root().join(file))
        .is_ok_and(|t| !needle.is_empty() && squash(&t).contains(&squash(needle)))
}

/// What no longer resolves among a row's contradictions.
fn contradiction_problems(row: &Value, known_red: &str) -> Vec<String> {
    contradictions(row)
        .into_iter()
        .filter(|(kind, r)| match kind.as_str() {
            "known_red" => r.len() < 12 || !known_red.contains(r.as_str()),
            _ => !file_ref_resolves(r),
        })
        .map(|(kind, r)| format!("{}: {kind} `{r}` no longer resolves", id(row)))
        .collect()
}

/// Whether `doc` still carries `sentence`, whitespace aside; a fragment under 20 characters ties nothing.
fn says(doc: &str, sentence: &str) -> bool {
    sentence.trim().len() >= 20 && squash(doc).contains(&squash(sentence))
}

/// The stated sentences of a row that are no longer in the file the row names.
fn missing_sentences(row: &Value) -> Vec<String> {
    let stated = list(row, "stated");
    if stated.is_empty() {
        return vec![format!("{}: no `stated:` entry", id(row))];
    }
    stated
        .iter()
        .filter_map(|s| {
            let (file, sentence) = (text(s, "file")?, text(s, "sentence")?);
            let doc = std::fs::read_to_string(repo_root().join(&file)).unwrap_or_default();
            (!says(&doc, &sentence))
                .then(|| format!("{}: {file} no longer says {sentence:?}", id(row)))
        })
        .chain(
            stated
                .iter()
                .filter(|s| text(s, "file").is_none() || text(s, "sentence").is_none())
                .map(|_| {
                    format!(
                        "{}: a `stated:` entry needs `file:` and `sentence:`",
                        id(row)
                    )
                }),
        )
        .collect()
}

/// A stated promise that was reworded or moved must be tied to its cells again.
#[test]
fn every_stated_sentence_is_still_in_its_file() {
    let bad: Vec<String> = rows().iter().flat_map(missing_sentences).collect();
    assert_eq!(
        bad,
        Vec::<String>::new(),
        "guarantee rows whose sentence left the docs. A reworded promise is a new promise: read \
         the new sentence, decide whether the row's cells still hold it, and re-tie the row:\n  {}",
        bad.join("\n  ")
    );
}

/// Ids are unique and every row has well-formed cells.
#[test]
fn every_row_is_well_formed() {
    let mut seen = BTreeSet::new();
    let mut bad = Vec::new();
    for row in rows() {
        if !seen.insert(id(&row)) {
            bad.push(format!("{}: duplicate id", id(&row)));
        }
        if let Err(e) = cells(&row) {
            bad.push(format!("{}: {e}", id(&row)));
        }
    }
    assert_eq!(
        bad,
        Vec::<String>::new(),
        "malformed guarantee rows:\n  {}",
        bad.join("\n  ")
    );
}

/// A cited cell runs as a test and its body still carries the assertion the row quotes.
#[test]
fn every_cited_cell_runs_and_carries_its_assertion() {
    let tests = all_test_fn_names();
    let gate = gate_sources();
    let mut bad = Vec::new();
    for row in rows() {
        for (col, cell) in cells(&row).unwrap_or_default() {
            let problem = match cell {
                Cell::Test { name, asserts } => {
                    if !tests.contains(&name) {
                        Some(format!(
                            "`{name}` is not a test function under src/ or tests/"
                        ))
                    } else {
                        let body = test_closure(&name).unwrap_or_default();
                        (!squash(&body).contains(&squash(&asserts))).then(|| {
                            format!("`{name}` and what it reaches in its file do not contain {asserts:?}")
                        })
                    }
                }
                Cell::Gate { stage, asserts } => gate_cell_problem(&gate, &stage, &asserts),
                Cell::Gap => None,
            };
            if let Some(p) = problem {
                bad.push(format!("{} / {col}: {p}", id(&row)));
            }
        }
    }
    assert_eq!(
        bad,
        Vec::<String>::new(),
        "guarantee cells that no longer hold what their row quotes. Open the cell: if it still \
         fails when the sentence is false, quote its assertion again; if not, the cell is a gap:\n  {}",
        bad.join("\n  ")
    );
}

/// A contradiction names a live known-red entry, a matrix row, or a doc sentence.
#[test]
fn every_contradiction_still_resolves() {
    let known_red = subject_text(KNOWN_RED);
    let bad: Vec<String> = rows()
        .iter()
        .flat_map(|r| contradiction_problems(r, &known_red))
        .collect();
    assert_eq!(
        bad,
        Vec::<String>::new(),
        "guarantee rows contradicted by something that is gone. A fixed defect leaves the ledger \
         with its known-red entry: delete the reference and lower the ceiling if the row is now held:\n  {}",
        bad.join("\n  ")
    );
}

/// The census of rows not held and of gap cells equals its ceiling.
#[test]
fn rows_not_held_and_gap_cells_match_their_ceilings() {
    let (mut not_held, mut gaps) = (Vec::new(), 0usize);
    for row in rows() {
        let row_gaps = cells(&row)
            .unwrap_or_default()
            .iter()
            .filter(|(_, c)| matches!(c, Cell::Gap))
            .count();
        gaps += row_gaps;
        if row_gaps > 0 || !contradictions(&row).is_empty() {
            not_held.push(id(&row));
        }
    }
    assert_eq!(
        (not_held.len(), gaps),
        (ROWS_NOT_HELD, GAP_CELLS),
        "(rows not held, gap cells) moved from the ceilings (ROWS_NOT_HELD, GAP_CELLS). A new \
         stated guarantee arrives with its cell; a cell written or a defect fixed LOWERS the pin. \
         Rows not held now: {not_held:?}"
    );
}

fn row(yaml: &str) -> Value {
    serde_yaml_ng::from_str(yaml).unwrap()
}

/// A sentence is found across a line wrap and not found once reworded.
#[test]
fn a_reworded_sentence_is_reported_and_a_wrapped_one_is_found() {
    let doc =
        "A\n**plain** `rivet run` (no `--resume`) never skips the chunks of a run that\nFINISHED";
    let wrapped =
        "A **plain** `rivet run` (no `--resume`) never skips the chunks of a run that FINISHED";
    assert!(says(doc, wrapped));
    assert!(!says(
        doc,
        "A plain run usually does not skip the chunks of a finished run"
    ));
    assert!(
        !says(doc, "never skips"),
        "a fragment under 20 characters ties nothing"
    );
    assert_eq!(missing_sentences(&row("id: x\n")).len(), 1);
    let gone = row(
        "id: x\nstated:\n  - file: Cargo.toml\n    sentence: \"rivet never loses a row under any circumstances\"\n",
    );
    assert_eq!(missing_sentences(&gone).len(), 1);
}

/// A cell is one kind, quotes an assertion, and a gap says why.
#[test]
fn a_malformed_cell_is_reported() {
    for bad in [
        "held_by: {}",
        "held_by: { all: { test: some_test } }",
        "held_by: { all: { test: some_test, asserts: short } }",
        "held_by: { all: { gap: later } }",
        "held_by: { all: { test: some_test, gap: \"both kinds at once, which is not one\", asserts: \"twelve chars at least\" } }",
        "id: x",
    ] {
        assert!(cells(&row(bad)).is_err(), "accepted: {bad}");
    }
    assert!(
        cells(&row(
            "held_by: { all: { gap: \"no cell reads the destination after a failed run\" } }"
        ))
        .is_ok()
    );
}

/// A gate cell needs a stage that is defined, referenced, and carries the quoted assertion.
#[test]
fn a_gate_cell_that_is_uncalled_or_misquoted_is_reported() {
    let src = |t: &str| vec![("m.py".to_string(), t.to_string())];
    let good = src("def stage_x(ctx):\n    assert rss < ceiling, 'grew'\n\nSTAGES = [stage_x]\n");
    assert_eq!(
        gate_cell_problem(&good, "stage_x", "assert rss < ceiling"),
        None
    );
    assert!(gate_cell_problem(&good, "stage_x", "assert rss < floor").is_some());
    assert!(gate_cell_problem(&good, "stage_y", "assert rss < ceiling").is_some());
    let uncalled = src("def stage_x(ctx):\n    assert rss < ceiling, 'grew'\n");
    assert!(gate_cell_problem(&uncalled, "stage_x", "assert rss < ceiling").is_some());
}

/// A contradiction that names nothing live is reported; one that resolves is not.
#[test]
fn a_dangling_contradiction_is_reported() {
    let ledger = "_open(\"live_x::open_defect_a_thing_postgres\", ...)";
    let live = row(
        "id: x\ncontradicted_by:\n  known_red: [\"live_x::open_defect_a_thing\"]\n  doc: [\"Cargo.toml#name = \\\"rivet-cli\\\"\"]\n",
    );
    assert_eq!(contradiction_problems(&live, ledger), Vec::<String>::new());
    let gone = row(
        "id: x\ncontradicted_by:\n  known_red: [\"live_x::open_defect_fixed_long_ago\"]\n  matrix_gap: [\"Cargo.toml#a sentence that is not there\"]\n",
    );
    assert_eq!(contradiction_problems(&gone, ledger).len(), 2);
}
