//! Drift guard for `docs/type-capability-matrix.yaml` (ADR-0038 CP9): its dimensions are
//! derived from the product enums, never typed in.

use std::collections::BTreeSet;

use serde_yaml_ng::Value;

use super::chunking_matrix_guard::{enum_variants, source_engine_variants};

const LEDGER: &str = "docs/type-capability-matrix.yaml";
const DELIVERY_RS: &str = "src/types/delivery.rs";
const MODES: [&str; 2] = ["batch", "cdc"];
/// The `render.canon` values the parity driver implements, read from the driver's own canon module.
#[allow(dead_code)]
#[path = "../common/canon.rs"]
mod canon;
use canon::CANONS;

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
        forms.contains(d)
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
                        "{at}: delivery `{d}` is no Arrow type, canonical extension or TextForm \
                         label (a refused type declares its ADR target with known_defect and \
                         batch_refuses)"
                    ));
                }
                if mode == "batch" && !r["diverges"].is_null() {
                    bad.push(format!("{at}: `diverges:` belongs on the cdc row"));
                }
                for key in r.as_mapping().into_iter().flat_map(|m| m.keys()) {
                    let key = key.as_str().unwrap_or("");
                    let cdc_only = [
                        "clickhouse",
                        "today_clickhouse",
                        "clickhouse_defect",
                        "clickhouse_defect_samples",
                    ]
                    .contains(&key);
                    if !ROW_KEYS.contains(&key) || cdc_only && mode != "cdc" {
                        bad.push(format!("{at}: `{key}:` is not a {mode} row field"));
                    }
                }
                for key in ["known_defect", "clickhouse_defect"] {
                    if let Some(why) = r[key].as_str()
                        && !(why.contains("ADR-") && why.contains("step"))
                    {
                        bad.push(format!(
                            "{at}: {key} must name the ADR and the step that fixes it"
                        ));
                    }
                }
                if !r["known_defect"].is_null() && !r["diverges"].is_null() {
                    bad.push(format!(
                        "{at}: known_defect beside `diverges:` declares today's behaviour, not the ADR target"
                    ));
                }
                if !r["known_defect"].is_null() {
                    match r["today_delivery"].as_str() {
                        None => bad.push(format!(
                            "{at}: known_defect needs `today_delivery:`, the type rivet ships today"
                        )),
                        Some(t) if !valid_delivery(t) => bad.push(format!(
                            "{at}: today_delivery `{t}` is no Arrow type, canonical extension or TextForm label"
                        )),
                        _ => {}
                    }
                }
                if (!r["known_defect"].is_null() || !r["clickhouse_defect"].is_null())
                    && !r["clickhouse"].is_null()
                    && r["today_clickhouse"].is_null()
                {
                    bad.push(format!(
                        "{at}: a marked row with a ClickHouse type needs `today_clickhouse:`"
                    ));
                }
                if !r["today_clickhouse"].is_null()
                    && r["known_defect"].is_null()
                    && r["clickhouse_defect"].is_null()
                {
                    bad.push(format!("{at}: `today_clickhouse:` belongs beside a marker"));
                }
                for (key, marker) in [
                    ("today_render", "known_defect"),
                    ("today_delivery", "known_defect"),
                    ("defect_samples", "known_defect"),
                    ("clickhouse_defect_samples", "clickhouse_defect"),
                ] {
                    if !r[key].is_null() && r[marker].is_null() {
                        bad.push(format!("{at}: `{key}:` belongs beside `{marker}:`"));
                    }
                }
                let samples: Vec<&str> = r["sample"]
                    .as_sequence()
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .collect();
                for key in ["defect_samples", "clickhouse_defect_samples"] {
                    for d in r[key].as_sequence().into_iter().flatten() {
                        let d = d.as_str().unwrap_or("");
                        if d != "NULL" && !samples.contains(&d) {
                            bad.push(format!(
                                "{at}: {key} names `{d}`, neither a sample nor NULL"
                            ));
                        }
                    }
                }
                if r["batch_refuses"].as_bool() == Some(true) && r["known_defect"].is_null() {
                    bad.push(format!(
                        "{at}: `batch_refuses` is today's behaviour of a known_defect row; refusal is \
                         not an ADR target"
                    ));
                }
                for render in ["render", "today_render"].map(|k| r[k].as_mapping()) {
                    for (k, v) in render.into_iter().flatten() {
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
            for field in [
                "sample",
                "override",
                "render",
                "known_defect",
                "today_delivery",
                "today_render",
                "defect_samples",
                "batch_refuses",
            ] {
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

/// Rows per engine and mode; a ledger may grow past these, never fall below them.
const ROW_FLOOR: [(&str, usize); 4] = [
    ("postgres", 36),
    ("mysql", 31),
    ("mssql", 21),
    ("oracle", 15),
];

#[test]
fn every_ledger_row_has_a_sample_a_real_delivery_and_a_twin_in_the_other_mode() {
    let doc = ledger();
    for (engine, floor) in ROW_FLOOR {
        for mode in MODES {
            let got = rows(&doc, engine, mode).len();
            assert!(
                got >= floor,
                "engines.{engine}.{mode} lost rows: {got}, the floor is {floor}"
            );
        }
    }
    let bad = row_violations(&doc);
    assert!(bad.is_empty(), "{LEDGER}:\n{}", bad.join("\n"));
}

/// The rows the parity driver expects to miss their ADR target, as `engine:mode:native`
/// (` (ClickHouse)` for a clickhouse_defect). A marker must be named here; a fix may leave
/// its line, which is then deleted; a new defect is fixed, not added here.
const KNOWN_DEFECTS: [&str; 41] = [
    "postgres:batch:INTERVAL",
    "postgres:batch:NUMERIC",
    "postgres:batch:MONEY",
    "postgres:batch:INET",
    "postgres:batch:CIDR",
    "postgres:batch:DATE[]",
    "postgres:batch:TIMESTAMP[]",
    "postgres:batch:TIMESTAMPTZ[]",
    "postgres:batch:TIME[]",
    "postgres:batch:UUID[]",
    "postgres:batch:BYTEA[]",
    "postgres:batch:NUMERIC[]",
    "postgres:cdc:UUID (ClickHouse)",
    "postgres:cdc:INTERVAL",
    "postgres:cdc:TEXT[] (ClickHouse)",
    "postgres:cdc:INTEGER[] (ClickHouse)",
    "postgres:cdc:DOUBLE PRECISION[] (ClickHouse)",
    "postgres:cdc:NUMERIC",
    "postgres:cdc:MONEY",
    "postgres:cdc:INET",
    "postgres:cdc:CIDR",
    "postgres:cdc:DATE[]",
    "postgres:cdc:TIMESTAMP[]",
    "postgres:cdc:TIMESTAMPTZ[]",
    "postgres:cdc:TIME[]",
    "postgres:cdc:UUID[]",
    "postgres:cdc:BYTEA[]",
    "postgres:cdc:NUMERIC[]",
    "mysql:batch:BOOLEAN",
    "mysql:cdc:BOOLEAN",
    "mssql:batch:DATETIME2",
    "mssql:batch:DATETIMEOFFSET",
    "mssql:batch:TIME",
    "mssql:cdc:DATETIME2",
    "mssql:cdc:DATETIMEOFFSET",
    "mssql:cdc:TIME",
    "mssql:cdc:UNIQUEIDENTIFIER (ClickHouse)",
    "oracle:batch:NUMBER",
    "oracle:batch:TIMESTAMP(9)",
    "oracle:cdc:NUMBER",
    "oracle:cdc:TIMESTAMP(9)",
];

/// The fields a ledger row may carry (`clickhouse*` on cdc rows only).
const ROW_KEYS: [&str; 15] = [
    "native_type",
    "sample",
    "delivery",
    "override",
    "render",
    "diverges",
    "known_defect",
    "today_render",
    "today_delivery",
    "defect_samples",
    "clickhouse",
    "today_clickhouse",
    "clickhouse_defect",
    "clickhouse_defect_samples",
    "batch_refuses",
];

/// Every marked row of `doc` as `KNOWN_DEFECTS` spells it.
fn marked(doc: &Value) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for e in keys(doc, "engines") {
        for mode in MODES {
            for (n, r) in rows(doc, &e, mode) {
                if !r["known_defect"].is_null() {
                    out.insert(format!("{e}:{mode}:{n}"));
                }
                if !r["clickhouse_defect"].is_null() {
                    out.insert(format!("{e}:{mode}:{n} (ClickHouse)"));
                }
            }
        }
    }
    out
}

/// The markers of `doc` that `KNOWN_DEFECTS` does not name.
fn known_defect_violations(doc: &Value) -> Vec<String> {
    let named: BTreeSet<String> = KNOWN_DEFECTS.iter().map(|s| s.to_string()).collect();
    marked(doc)
        .difference(&named)
        .map(|m| format!("{m}: a new known defect, blessed into the ledger instead of fixed"))
        .collect()
}

#[test]
fn known_defect_rows_only_shrink() {
    let bad = known_defect_violations(&ledger());
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}

#[test]
fn a_known_defect_moved_to_another_row_is_refused() {
    let mut doc = ledger();
    let cdc = doc["engines"]["mysql"]["cdc"].as_sequence_mut().unwrap();
    let marker = cdc
        .iter_mut()
        .find(|r| r["native_type"] == "BOOLEAN")
        .and_then(|r| r.as_mapping_mut().unwrap().remove("known_defect"))
        .unwrap();
    let other = cdc
        .iter_mut()
        .find(|r| r["native_type"] == "DATETIME(6)")
        .unwrap();
    other
        .as_mapping_mut()
        .unwrap()
        .insert("known_defect".into(), marker);
    let bad = known_defect_violations(&doc);
    assert_eq!(
        bad,
        ["mysql:cdc:DATETIME(6): a new known defect, blessed into the ledger instead of fixed"],
        "a marker moved to another row kept the count and went unnoticed"
    );
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

#[test]
fn a_known_defect_without_today_delivery_is_refused() {
    let mut doc = ledger();
    let row = doc["engines"]["mysql"]["cdc"]
        .as_sequence_mut()
        .unwrap()
        .iter_mut()
        .find(|r| r["native_type"] == "BOOLEAN")
        .unwrap()
        .as_mapping_mut()
        .unwrap();
    row.remove("today_delivery").unwrap();
    row.remove("today_clickhouse").unwrap();
    let bad = row_violations(&doc);
    for want in ["needs `today_delivery:`", "needs `today_clickhouse:`"] {
        assert!(bad.iter().any(|b| b.contains(want)), "{want}: {bad:?}");
    }
}
