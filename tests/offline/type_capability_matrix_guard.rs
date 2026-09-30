//! Drift guard for `docs/type-capability-matrix.yaml` (ADR-0038 CP9): its dimensions are
//! derived from the product enums, never typed in.

use std::collections::BTreeSet;

use serde_yaml_ng::Value;

use super::chunking_matrix_guard::{enum_variants, source_engine_variants};

const LEDGER: &str = "docs/type-capability-matrix.yaml";
const DELIVERY_RS: &str = "src/types/delivery.rs";
const MODES: [&str; 2] = ["batch", "cdc"];

/// `IsoTimestampNanos` -> `iso_timestamp_nanos`, the label rule `TextForm::label` follows.
fn snake(ident: &str) -> String {
    let mut out = String::new();
    for (i, c) in ident.chars().enumerate() {
        if c.is_ascii_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(c.to_ascii_lowercase());
    }
    out
}

/// The ledger parsed from the repo root.
fn ledger() -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(LEDGER);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {LEDGER}: {e}"));
    serde_yaml_ng::from_str(&text).unwrap_or_else(|e| panic!("parse {LEDGER}: {e}"))
}

/// The string keys of a YAML mapping at `v[key]`.
fn keys(v: &Value, key: &str) -> BTreeSet<String> {
    v.get(key)
        .and_then(Value::as_mapping)
        .unwrap_or_else(|| panic!("`{key}:` is missing or not a mapping"))
        .keys()
        .map(|k| k.as_str().expect("string key").to_string())
        .collect()
}

#[test]
fn every_text_form_has_exactly_one_ledger_entry_with_a_recovery_per_target() {
    let derived: BTreeSet<String> = enum_variants(DELIVERY_RS, "TextForm")
        .iter()
        .map(|v| snake(v))
        .collect();
    assert!(
        derived.len() >= 14 && derived.contains("decimal_plain") && derived.contains("server_text"),
        "TextForm parse produced {derived:?}"
    );
    let doc = ledger();
    let declared = keys(&doc, "forms");
    let missing: Vec<_> = derived.difference(&declared).collect();
    let unknown: Vec<_> = declared.difference(&derived).collect();
    assert!(
        missing.is_empty() && unknown.is_empty(),
        "{LEDGER} forms: out of sync with TextForm — missing {missing:?}, no such form {unknown:?}"
    );

    let targets: BTreeSet<String> = enum_variants("src/types/target.rs", "ExportTarget")
        .iter()
        .map(|v| v.to_ascii_lowercase())
        .collect();
    let source =
        std::fs::read_to_string(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(DELIVERY_RS))
            .expect("read delivery.rs");
    for form in &declared {
        let entry = &doc["forms"][form.as_str()];
        let renderer = entry["renderer"]
            .as_str()
            .unwrap_or_else(|| panic!("forms.{form}.renderer missing"));
        assert!(
            renderer == "engine-produced" || source.contains(&format!("pub fn {renderer}(")),
            "forms.{form}.renderer names `{renderer}`, which is not a pub fn in {DELIVERY_RS}"
        );
        assert_eq!(
            keys(entry, "recover"),
            targets,
            "forms.{form}.recover must name exactly the ExportTarget variants"
        );
    }
}

#[test]
fn every_source_engine_has_a_batch_and_a_cdc_section() {
    let engines: BTreeSet<String> = source_engine_variants().into_iter().collect();
    let doc = ledger();
    assert_eq!(
        keys(&doc, "engines"),
        engines,
        "{LEDGER} engines: must list exactly the SourceType variants"
    );
    for engine in &engines {
        let modes = keys(&doc["engines"], engine);
        assert_eq!(
            modes,
            MODES.iter().map(|m| m.to_string()).collect(),
            "engines.{engine} must have exactly the modes {MODES:?}"
        );
    }
}
