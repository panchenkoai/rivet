//! Drift guard for `docs/type-capability-matrix.yaml` (ADR-0038 CP9): its dimensions are
//! derived from the product enums, never typed in.

use std::collections::BTreeSet;

use serde_yaml_ng::Value;

use super::chunking_matrix_guard::{enum_variants, source_engine_variants};

const LEDGER: &str = "docs/type-capability-matrix.yaml";
const DELIVERY_RS: &str = "src/types/delivery.rs";
const MODES: [&str; 2] = ["batch", "cdc"];
/// The `render.canon` values tests/live/live_cdc_type_parity.rs implements.
const CANONS: [&str; 6] = [
    "number",
    "timestamp",
    "float32",
    "float64",
    "interval",
    "datetime_tick",
];

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
        let mut modes = keys(&doc["engines"], engine);
        modes.remove("setup");
        assert_eq!(
            modes,
            MODES.iter().map(|m| m.to_string()).collect(),
            "engines.{engine} must have exactly the modes {MODES:?} (plus an optional setup:)"
        );
    }
}

/// One mode's rows of one engine, keyed by native type.
fn rows<'a>(doc: &'a Value, engine: &str, mode: &str) -> Vec<(String, &'a Value)> {
    doc["engines"][engine][mode]
        .as_sequence()
        .unwrap_or_else(|| panic!("engines.{engine}.{mode} must be a list of rows"))
        .iter()
        .map(|r| {
            let native = r["native_type"]
                .as_str()
                .unwrap_or_else(|| panic!("engines.{engine}.{mode}: a row has no native_type"));
            (native.to_string(), r)
        })
        .collect()
}

/// Every error in the engine rows of `doc`: samples, twins across modes, divergences, delivery spellings.
fn row_violations(doc: &Value) -> Vec<String> {
    use arrow_schema::extension::{ExtensionType, Json, Uuid};
    let forms = keys(doc, "forms");
    let extensions = [<Uuid as ExtensionType>::NAME, <Json as ExtensionType>::NAME];
    let valid_delivery = |d: &str| {
        d == "refused"
            || forms.contains(d)
            || extensions.contains(&d)
            || d.parse::<arrow_schema::DataType>()
                .is_ok_and(|t| t.to_string() == d)
    };
    let mut bad = Vec::new();
    for engine in keys(doc, "engines") {
        let (batch, cdc) = (rows(doc, &engine, "batch"), rows(doc, &engine, "cdc"));
        for (mode, list) in [("batch", &batch), ("cdc", &cdc)] {
            let mut seen = BTreeSet::new();
            for (native, r) in list.iter() {
                let at = format!("engines.{engine}.{mode}[{native}]");
                if !seen.insert(native) {
                    bad.push(format!("{at}: listed twice"));
                }
                if r["sample"].as_sequence().is_none_or(|s| s.is_empty()) {
                    bad.push(format!("{at}: no sample"));
                }
                let d = r["delivery"].as_str().unwrap_or("");
                if !valid_delivery(d) {
                    bad.push(format!(
                        "{at}: delivery `{d}` is no Arrow type, canonical extension, TextForm \
                         label or `refused`"
                    ));
                }
                if mode == "batch" && !r["diverges"].is_null() {
                    bad.push(format!("{at}: `diverges:` belongs on the cdc row"));
                }
                if let Some(render) = r["render"].as_mapping() {
                    for (k, v) in render {
                        let (k, v) = (k.as_str().unwrap_or(""), v.as_str().unwrap_or(""));
                        let known = match k {
                            "source" => engine == "oracle",
                            "server" => engine != "oracle",
                            "duck" => true,
                            "canon" => CANONS.contains(&v),
                            _ => false,
                        };
                        if !known {
                            bad.push(format!(
                                "{at}: render `{k}: {v}` is not one the parity driver reads"
                            ));
                        }
                    }
                }
            }
        }
        for (native, b) in &batch {
            let Some((_, c)) = cdc.iter().find(|(n, _)| n == native) else {
                bad.push(format!(
                    "engines.{engine}: `{native}` is declared for batch only"
                ));
                continue;
            };
            let at = format!("engines.{engine}[{native}]");
            for field in ["sample", "override", "render"] {
                if b[field] != c[field] {
                    bad.push(format!("{at}: batch and cdc disagree on `{field}`"));
                }
            }
            match (b["delivery"] != c["delivery"], c["diverges"].as_str()) {
                (true, None) => bad.push(format!(
                    "{at}: batch delivers {:?}, cdc {:?}, and the cdc row gives no `diverges:` \
                     reason",
                    b["delivery"], c["delivery"]
                )),
                (false, Some(_)) => {
                    bad.push(format!("{at}: `diverges:` on rows that deliver alike"))
                }
                _ => {}
            }
        }
        for (native, _) in &cdc {
            if !batch.iter().any(|(n, _)| n == native) {
                bad.push(format!(
                    "engines.{engine}: `{native}` is declared for cdc only"
                ));
            }
        }
    }
    bad
}

#[test]
fn every_ledger_row_has_a_sample_a_real_delivery_and_a_twin_in_the_other_mode() {
    let doc = ledger();
    let total: usize = keys(&doc, "engines")
        .iter()
        .map(|e| rows(&doc, e, "batch").len())
        .sum();
    assert!(total >= 90, "the ledger lost its rows: {total}");
    let bad = row_violations(&doc);
    assert!(bad.is_empty(), "{LEDGER}:\n{}", bad.join("\n"));
}

#[test]
fn a_row_declared_in_one_mode_only_is_refused() {
    let mut doc = ledger();
    let gone = doc["engines"]["mysql"]["cdc"]
        .as_sequence_mut()
        .unwrap()
        .remove(0);
    let bad = row_violations(&doc);
    assert!(
        bad.iter().any(|b| b.contains("declared for batch only")),
        "dropping the cdc twin of {:?} went unnoticed: {bad:?}",
        gone["native_type"]
    );
}

/// The column types of `fn type_table` in the Oracle CDC suite, the one hand list the ledger did not replace.
fn oracle_type_table_types() -> Vec<String> {
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/live/live_cdc_oracledb.rs");
    let src = std::fs::read_to_string(path).expect("read live_cdc_oracledb.rs");
    let body = &src[src.find("fn type_table()").expect("fn type_table")..];
    // cdc_table("<prefix>", "<columns>"): the columns are the second string literal.
    let ddl: String = body
        .split('"')
        .nth(3)
        .expect("type_table's column list")
        .split("\\\n")
        .map(str::trim)
        .collect::<Vec<_>>()
        .join(" ");
    let (mut cols, mut depth, mut cur) = (Vec::new(), 0, String::new());
    for ch in ddl.chars() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                cols.push(std::mem::take(&mut cur));
                continue;
            }
            _ => {}
        }
        cur.push(ch);
    }
    cols.push(cur);
    cols.iter()
        .filter(|c| !c.contains("PRIMARY KEY"))
        .map(|c| {
            c.trim()
                .split_once(' ')
                .expect("name type")
                .1
                .trim()
                .to_uppercase()
        })
        .collect()
}

#[test]
fn the_oracle_cdc_type_table_is_covered_by_ledger_rows() {
    let doc = ledger();
    let declared: BTreeSet<String> = rows(&doc, "oracle", "cdc")
        .into_iter()
        .map(|(n, _)| n.to_uppercase())
        .collect();
    let types = oracle_type_table_types();
    assert!(types.len() >= 15, "type_table parse produced {types:?}");
    let missing: Vec<_> = types.iter().filter(|t| !declared.contains(*t)).collect();
    assert!(
        missing.is_empty(),
        "live_cdc_oracledb.rs::type_table uses types with no oracle ledger row: {missing:?}"
    );
}
