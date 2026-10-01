//! Drift guard for `docs/type-capability-matrix.yaml` (ADR-0038 CP9): its dimensions are
//! derived from the product enums, never typed in.

use std::collections::BTreeSet;

use serde_yaml_ng::Value;

use super::chunking_matrix_guard::{enum_variants, source_engine_variants};

const LEDGER: &str = "docs/type-capability-matrix.yaml";
const DELIVERY_RS: &str = "src/types/delivery.rs";
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
fn every_source_engine_has_one_rows_list() {
    let engines: BTreeSet<String> = source_engine_variants().into_iter().collect();
    let doc = ledger();
    assert_eq!(
        keys(&doc, "engines"),
        engines,
        "{LEDGER} engines: must list exactly the SourceType variants"
    );
    for engine in &engines {
        let mut sections = keys(&doc["engines"], engine);
        sections.remove("setup");
        sections.remove("renders");
        assert_eq!(
            sections,
            BTreeSet::from(["rows".to_string()]),
            "engines.{engine} must have exactly `rows:` (plus an optional setup: and renders:)"
        );
    }
}

/// The rows of one engine, keyed by native type.
fn rows<'a>(doc: &'a Value, engine: &str) -> Vec<(String, &'a Value)> {
    doc["engines"][engine]["rows"]
        .as_sequence()
        .unwrap_or_else(|| panic!("engines.{engine}.rows must be a list of rows"))
        .iter()
        .map(|r| {
            let native = r["native_type"]
                .as_str()
                .unwrap_or_else(|| panic!("engines.{engine}.rows: a row has no native_type"));
            (native.to_string(), r)
        })
        .collect()
}

/// Every error in the engine rows of `doc`: samples, markers and their today keys, delivery spellings, renders.
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
        let renders = &doc["engines"][engine.as_str()]["renders"];
        for (d, render) in renders.as_mapping().into_iter().flatten() {
            let d = d.as_str().unwrap_or("");
            let at = format!("engines.{engine}.renders[{d}]");
            if !valid_delivery(d) {
                bad.push(format!(
                    "{at}: `{d}` is no Arrow type, canonical extension or TextForm label"
                ));
            }
            bad.extend(render_violations(&engine, &at, render));
        }
        let (mut seen, mut arrow_rows) = (BTreeSet::new(), Vec::new());
        for (native, r) in rows(doc, &engine) {
            let at = format!("engines.{engine}.rows[{native}]");
            if !seen.insert(native.clone()) {
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
            for key in r.as_mapping().into_iter().flat_map(|m| m.keys()) {
                let key = key.as_str().unwrap_or("");
                if !ROW_KEYS.contains(&key) {
                    bad.push(format!("{at}: `{key}:` is not a row field"));
                }
            }
            let ch = &r["clickhouse"];
            for key in ch.as_mapping().into_iter().flat_map(|m| m.keys()) {
                let key = key.as_str().unwrap_or("");
                if !CLICKHOUSE_KEYS.contains(&key) {
                    bad.push(format!(
                        "{at}: `clickhouse.{key}:` is not a ClickHouse field"
                    ));
                }
            }
            if !ch.is_null() && ch["type"].is_null() {
                bad.push(format!("{at}: `clickhouse:` names no `type:`"));
            }
            let (kd, chd) = (&r["known_defect"], &ch["defect"]);
            for (key, why) in [("known_defect", kd), ("clickhouse.defect", chd)] {
                if let Some(why) = why.as_str()
                    && !(why.contains("ADR-") && why.contains("step"))
                {
                    bad.push(format!(
                        "{at}: {key} must name the ADR and the step that fixes it"
                    ));
                }
            }
            if !kd.is_null() {
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
            let marked = !kd.is_null() || !chd.is_null();
            match (marked, ch["today"].is_null()) {
                (true, true) if !ch.is_null() => bad.push(format!(
                    "{at}: a marked row with a ClickHouse type needs `clickhouse.today:`"
                )),
                (false, false) => {
                    bad.push(format!("{at}: `clickhouse.today:` belongs beside a marker"))
                }
                _ => {}
            }
            for (key, val, marker, by) in [
                ("today_render", &r["today_render"], "known_defect", kd),
                ("today_delivery", &r["today_delivery"], "known_defect", kd),
                ("defect_samples", &r["defect_samples"], "known_defect", kd),
                (
                    "clickhouse.defect_samples",
                    &ch["defect_samples"],
                    "clickhouse.defect",
                    chd,
                ),
            ] {
                if !val.is_null() && by.is_null() {
                    bad.push(format!("{at}: `{key}:` belongs beside `{marker}:`"));
                }
            }
            let samples: Vec<&str> = r["sample"]
                .as_sequence()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .collect();
            for (key, list) in [
                ("defect_samples", &r["defect_samples"]),
                ("clickhouse.defect_samples", &ch["defect_samples"]),
            ] {
                for d in list.as_sequence().into_iter().flatten() {
                    let d = d.as_str().unwrap_or("");
                    if d != "NULL" && !samples.contains(&d) {
                        bad.push(format!(
                            "{at}: {key} names `{d}`, neither a sample nor NULL"
                        ));
                    }
                }
            }
            if r["batch_refuses"].as_bool() == Some(true) && kd.is_null() {
                bad.push(format!(
                    "{at}: `batch_refuses` is today's behaviour of a known_defect row; refusal is \
                     not an ADR target"
                ));
            }
            for key in ["render", "today_render"] {
                bad.extend(render_violations(&engine, &at, &r[key]));
            }
            let duck = if r["today_render"].is_null() {
                r["render"]["duck"].as_str().or(renders[d]["duck"].as_str())
            } else {
                r["today_render"]["duck"].as_str()
            };
            if duck == Some("arrow") {
                arrow_rows.push(native);
            }
        }
        if arrow_rows.len() > usize::from(engine == "postgres") {
            bad.push(format!(
                "engines.{engine}: `duck: arrow` (Arrow's display instead of DuckDB) is for one \
                 postgres row at most, here {arrow_rows:?}"
            ));
        }
    }
    bad
}

/// Every render field in `render` the parity driver does not read for `engine`.
fn render_violations(engine: &str, at: &str, render: &Value) -> Vec<String> {
    let mut bad = Vec::new();
    for (k, v) in render.as_mapping().into_iter().flatten() {
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
    bad
}

/// Rows per engine; a ledger may grow past these, never fall below them.
const ROW_FLOOR: [(&str, usize); 4] = [
    ("postgres", 36),
    ("mysql", 31),
    ("mssql", 21),
    ("oracle", 15),
];

#[test]
fn every_ledger_row_has_a_sample_and_a_real_delivery() {
    let doc = ledger();
    for (engine, floor) in ROW_FLOOR {
        let got = rows(&doc, engine).len();
        assert!(
            got >= floor,
            "engines.{engine}.rows lost rows: {got}, the floor is {floor}"
        );
    }
    let bad = row_violations(&doc);
    assert!(bad.is_empty(), "{LEDGER}:\n{}", bad.join("\n"));
}

/// The rows the parity driver expects to miss their ADR target, as `engine:native`
/// (` (ClickHouse)` for a clickhouse.defect). Named and marked are the same set: a fix
/// deletes its line here, and a new defect is fixed, not added here.
const KNOWN_DEFECTS: &[&str] = &[
    "postgres:INTERVAL",
    "postgres:NUMERIC",
    "postgres:MONEY",
    "postgres:INET",
    "postgres:CIDR",
    "postgres:DATE[]",
    "postgres:TIMESTAMP[]",
    "postgres:TIMESTAMPTZ[]",
    "postgres:TIME[]",
    "postgres:UUID[]",
    "postgres:BYTEA[]",
    "postgres:NUMERIC[]",
    "postgres:UUID (ClickHouse)",
    "postgres:TEXT[] (ClickHouse)",
    "postgres:INTEGER[] (ClickHouse)",
    "postgres:DOUBLE PRECISION[] (ClickHouse)",
    "mysql:BOOLEAN",
    "mssql:DATETIME2",
    "mssql:DATETIMEOFFSET",
    "mssql:TIME",
    "mssql:UNIQUEIDENTIFIER (ClickHouse)",
    "oracle:NUMBER",
    "oracle:TIMESTAMP(9)",
];

/// The fields a ledger row may carry.
const ROW_KEYS: [&str; 10] = [
    "native_type",
    "sample",
    "delivery",
    "render",
    "known_defect",
    "today_render",
    "today_delivery",
    "defect_samples",
    "clickhouse",
    "batch_refuses",
];

/// The fields of a row's `clickhouse:` map.
const CLICKHOUSE_KEYS: [&str; 4] = ["type", "today", "defect", "defect_samples"];

/// Every marked row of `doc` as `KNOWN_DEFECTS` spells it.
fn marked(doc: &Value) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for e in keys(doc, "engines") {
        for (n, r) in rows(doc, &e) {
            if !r["known_defect"].is_null() {
                out.insert(format!("{e}:{n}"));
            }
            if !r["clickhouse"]["defect"].is_null() {
                out.insert(format!("{e}:{n} (ClickHouse)"));
            }
        }
    }
    out
}

/// Every marker of `doc` that `KNOWN_DEFECTS` does not name, and every name with no marker left.
fn known_defect_violations(doc: &Value) -> Vec<String> {
    let named: BTreeSet<String> = KNOWN_DEFECTS.iter().map(|s| s.to_string()).collect();
    let marked = marked(doc);
    marked
        .symmetric_difference(&named)
        .map(|m| {
            if marked.contains(m) {
                format!("{m}: a new known defect, blessed into the ledger instead of fixed")
            } else {
                format!("{m}: fixed, its marker is gone — delete its KNOWN_DEFECTS line")
            }
        })
        .collect()
}

#[test]
fn known_defect_rows_only_shrink() {
    let bad = known_defect_violations(&ledger());
    assert!(bad.is_empty(), "{}", bad.join("\n"));
}

/// The mapping of `engine`'s row `native`, for a test to edit.
fn row_mut<'a>(doc: &'a mut Value, engine: &str, native: &str) -> &'a mut serde_yaml_ng::Mapping {
    doc["engines"][engine]["rows"]
        .as_sequence_mut()
        .unwrap()
        .iter_mut()
        .find(|r| r["native_type"] == native)
        .and_then(Value::as_mapping_mut)
        .unwrap_or_else(|| panic!("no {engine} row {native}"))
}

#[test]
fn a_known_defect_moved_to_another_row_is_refused() {
    let mut doc = ledger();
    let marker = row_mut(&mut doc, "mysql", "BOOLEAN")
        .remove("known_defect")
        .unwrap();
    row_mut(&mut doc, "mysql", "DATETIME(6)").insert("known_defect".into(), marker);
    assert_eq!(
        known_defect_violations(&doc),
        [
            "mysql:BOOLEAN: fixed, its marker is gone — delete its KNOWN_DEFECTS line",
            "mysql:DATETIME(6): a new known defect, blessed into the ledger instead of fixed"
        ],
        "a marker moved to another row kept the count and went unnoticed"
    );
}

#[test]
fn a_fixed_defect_left_in_known_defects_is_refused() {
    let mut doc = ledger();
    row_mut(&mut doc, "mysql", "BOOLEAN")
        .remove("known_defect")
        .unwrap();
    assert_eq!(
        known_defect_violations(&doc),
        ["mysql:BOOLEAN: fixed, its marker is gone — delete its KNOWN_DEFECTS line"]
    );
}

#[test]
fn a_known_defect_without_today_delivery_is_refused() {
    let mut doc = ledger();
    let row = row_mut(&mut doc, "mysql", "BOOLEAN");
    row.remove("today_delivery").unwrap();
    row.get_mut("clickhouse")
        .and_then(Value::as_mapping_mut)
        .unwrap()
        .remove("today")
        .unwrap();
    let bad = row_violations(&doc);
    for want in ["needs `today_delivery:`", "needs `clickhouse.today:`"] {
        assert!(bad.iter().any(|b| b.contains(want)), "{want}: {bad:?}");
    }
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
    let declared: BTreeSet<String> = rows(&doc, "oracle")
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
fn duck_arrow_is_one_postgres_row_at_most() {
    let arrow = || -> Value { serde_yaml_ng::from_str("{duck: arrow}").unwrap() };
    let mut doc = ledger();
    row_mut(&mut doc, "mysql", "DOUBLE").insert("render".into(), arrow());
    row_mut(&mut doc, "postgres", "NUMERIC(18,2)").insert("render".into(), arrow());
    let bad: Vec<String> = row_violations(&doc)
        .into_iter()
        .filter(|b| b.contains("duck: arrow"))
        .collect();
    assert_eq!(bad.len(), 2, "{bad:?}");
    assert!(bad[0].starts_with("engines.mysql:") && bad[1].starts_with("engines.postgres:"));
}
