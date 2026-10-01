//! Every source engine runs the always-on Form A value checksum (driver cells vs the built Arrow batch), or is a named exception.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// Engines with no Form A, and why. A named gap that gains Form A must leave this list.
const EXCEPTIONS: &[(&str, &str)] = &[
    (
        "mongo",
        "by design: a document exports as one verbatim `document` blob, so there is no per-column source pass",
    ),
    (
        "oracle",
        "known gap (2026-10-01): no CellSource adapter yet; owned by the ADR-0038 Oracle engine step",
    ),
];

/// The concatenated non-test Rust source of `src/source/<engine>` (dir) or `src/source/<engine>.rs`.
fn engine_source(engine: &str) -> String {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/source");
    let mut out = String::new();
    let file = root.join(format!("{engine}.rs"));
    if file.is_file() {
        out.push_str(&std::fs::read_to_string(&file).unwrap());
    }
    let dir = root.join(engine);
    if dir.is_dir() {
        let mut files: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "rs"))
            .collect();
        files.sort();
        for f in files {
            out.push_str(&std::fs::read_to_string(&f).unwrap());
        }
    }
    assert!(
        !out.is_empty(),
        "no source found for engine `{engine}` under {}",
        root.display()
    );
    out
}

/// Whether `src` implements a `CellSource` AND feeds it to `source_checksums` and `verify`.
fn runs_form_a(src: &str) -> bool {
    let has_impl = src.contains("CellSource for ");
    let computes = src.contains("value_checksum::source_checksums(");
    let verifies = src.contains("value_checksum::verify(");
    has_impl && computes && verifies
}

/// Engine name → whether it runs Form A.
fn census() -> BTreeMap<String, bool> {
    crate::chunking_matrix_guard::source_engine_variants()
        .into_iter()
        .map(|e| {
            let runs = runs_form_a(&engine_source(&e));
            (e, runs)
        })
        .collect()
}

#[test]
fn every_source_engine_runs_form_a_or_is_a_named_exception() {
    let excepted: BTreeSet<&str> = EXCEPTIONS.iter().map(|(e, _)| *e).collect();
    let mut bad = Vec::new();
    for (engine, runs) in census() {
        match (runs, excepted.contains(engine.as_str())) {
            (false, false) => bad.push(format!(
                "{engine}: no Form A value checksum (implement CellSource and call \
                 value_checksum::source_checksums + verify before the batch is written)"
            )),
            (true, true) => bad.push(format!(
                "{engine}: runs Form A now — remove it from EXCEPTIONS in this guard"
            )),
            _ => {}
        }
    }
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}

#[test]
fn exceptions_name_real_engines_and_carry_a_reason() {
    let engines = crate::chunking_matrix_guard::source_engine_variants();
    for (engine, why) in EXCEPTIONS {
        assert!(
            engines.contains(*engine),
            "EXCEPTIONS names `{engine}`, not a SourceType"
        );
        assert!(
            why.len() > 20,
            "`{engine}` needs a real reason, got `{why}`"
        );
    }
}

#[test]
fn form_a_detection_requires_impl_compute_and_verify() {
    let full =
        "impl X::CellSource for A {} value_checksum::source_checksums( value_checksum::verify(";
    assert!(runs_form_a(full));
    for missing in [
        "CellSource for ",
        "value_checksum::source_checksums(",
        "value_checksum::verify(",
    ] {
        assert!(
            !runs_form_a(&full.replace(missing, "")),
            "dropping `{missing}` must count as no Form A"
        );
    }
}
