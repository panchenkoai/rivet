//! The scheduler contract of ADR-0039: the JSON shapes under `tests/fixtures/scheduler/`.
//!
//! A shape changes in the fixture, here and in the ADR together. Where the product already
//! emits part of a shape the test compares the fixture with the product's own output; the
//! keys it does not emit yet are listed in `NOT_YET_EMITTED`, which only shrinks.

use std::collections::BTreeSet;
use std::path::PathBuf;

use rivet::error::{ExitClass, codes};
use serde_json::Value;

const ADR: &str = "docs/adr/0039-scheduler-contract.md";

const ERROR_KEYS: &[&str] = &[
    "code",
    "kind",
    "class",
    "exit_code",
    "retryable",
    "action",
    "message",
];

/// Every fixture and the keys of its top-level object.
const SHAPES: &[(&str, &[&str])] = &[
    ("error_object.json", ERROR_KEYS),
    (
        "run_entry.json",
        &[
            "export_name",
            "status",
            "run_id",
            "rows",
            "files",
            "bytes",
            "bytes_read",
            "duration_ms",
            "mode",
            "error_message",
            "error",
            "stop_reason",
            "tables",
        ],
    ),
    (
        "json_errors.json",
        &[
            "error",
            "exit_class",
            "code",
            "exit_code",
            "class",
            "kind",
            "retryable",
            "action",
            "failures",
        ],
    ),
    ("load_result.json", &["run_id", "per_table"]),
    (
        "xcom_unit.json",
        &[
            "unit",
            "status",
            "run_id",
            "rows",
            "files",
            "stop_reason",
            "error",
        ],
    ),
];

/// Keys of `run_entry.json` that `RunAggregateEntry` does not serialize yet.
const NOT_YET_EMITTED: &[&str] = &[
    // ratchet-pin: scheduler-contract-not-yet-emitted strings
    "error",
    "stop_reason",
    "tables",
    // ratchet-pin: end
];

/// The class name ADR-0039 gives each exit code; `crashed` is the class of no exit code.
const CLASS_NAMES: &[(i32, &str)] = &[
    (1, "generic"),
    (2, "retryable"),
    (3, "data_integrity"),
    (4, "schema_drift"),
    (5, "refusal"),
    (6, "internal"),
];

/// The fixture directory.
fn dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/scheduler")
}

/// Parse one fixture.
fn fixture(name: &str) -> Value {
    let text = std::fs::read_to_string(dir().join(name)).unwrap_or_else(|e| panic!("{name}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{name}: not valid JSON: {e}"))
}

/// The key set of a JSON object.
fn keys(v: &Value) -> BTreeSet<String> {
    v.as_object()
        .unwrap_or_else(|| panic!("expected an object, got {v}"))
        .keys()
        .cloned()
        .collect()
}

/// A key list as a set.
fn set(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|s| s.to_string()).collect()
}

/// Every object in `v` that carries a `class` key, with where it was found.
fn error_objects(v: &Value, at: &str, out: &mut Vec<(String, Value)>) {
    match v {
        Value::Object(m) => {
            if m.contains_key("class") {
                out.push((at.to_string(), v.clone()));
            }
            for (k, child) in m {
                error_objects(child, &format!("{at}.{k}"), out);
            }
        }
        Value::Array(a) => {
            for (i, child) in a.iter().enumerate() {
                error_objects(child, &format!("{at}[{i}]"), out);
            }
        }
        _ => {}
    }
}

/// Every key name used anywhere inside `v`.
fn all_keys(v: &Value, out: &mut BTreeSet<String>) {
    match v {
        Value::Object(m) => {
            for (k, child) in m {
                out.insert(k.clone());
                all_keys(child, out);
            }
        }
        Value::Array(a) => a.iter().for_each(|child| all_keys(child, out)),
        _ => {}
    }
}

#[test]
fn every_fixture_is_pinned_and_every_pin_has_a_fixture() {
    let on_disk: BTreeSet<String> = std::fs::read_dir(dir())
        .expect("tests/fixtures/scheduler exists")
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    let pinned: BTreeSet<String> = SHAPES.iter().map(|(n, _)| n.to_string()).collect();
    assert_eq!(
        on_disk, pinned,
        "fixture files and SHAPES must list the same names"
    );
    for (name, want) in SHAPES {
        assert_eq!(keys(&fixture(name)), set(want), "{name}: top-level keys");
    }
}

#[test]
fn nested_shapes_have_their_pinned_keys() {
    let run = fixture("run_entry.json");
    let tables = run["tables"].as_array().expect("tables[] is an array");
    assert!(tables.len() >= 2, "the multi-table case needs two rows");
    for t in tables {
        assert_eq!(
            keys(t),
            set(&["table", "rows", "files"]),
            "run_entry tables[]"
        );
    }

    let failures = fixture("json_errors.json")["failures"].clone();
    let failures = failures.as_array().expect("failures[] is an array");
    assert!(
        failures.len() >= 2,
        "failures[] lists ALL failed units: show two"
    );
    let mut unit_keys = set(ERROR_KEYS);
    unit_keys.insert("export".into());
    for f in failures {
        assert_eq!(keys(f), unit_keys, "json_errors failures[]");
    }

    let load = fixture("load_result.json");
    let rows = load["per_table"]
        .as_array()
        .expect("per_table[] is an array");
    let statuses: BTreeSet<&str> = rows.iter().map(|r| r["status"].as_str().unwrap()).collect();
    assert!(statuses.contains("failed") && statuses.contains("skipped"));
    for r in rows {
        assert_eq!(
            keys(r),
            set(&["export", "table", "status", "skip_reason", "rows", "error"]),
            "load_result per_table[]"
        );
        assert!(
            ["loaded", "compacted", "nothing_to_do", "skipped", "failed"]
                .contains(&r["status"].as_str().unwrap())
        );
        assert_eq!(
            r["status"] == "failed",
            !r["error"].is_null(),
            "error iff failed"
        );
        assert_eq!(r["status"] == "skipped", !r["skip_reason"].is_null());
        if !r["error"].is_null() {
            assert_eq!(keys(&r["error"]), set(ERROR_KEYS), "load_result error");
        }
    }
}

#[test]
fn error_objects_agree_with_the_exit_classes_and_the_code_registry() {
    let mut found = Vec::new();
    for (name, _) in SHAPES {
        error_objects(&fixture(name), name, &mut found);
    }
    assert!(
        found.len() >= 6,
        "expected error objects in the fixtures, found {}",
        found.len()
    );
    let (mut coded, mut uncoded) = (0, 0);
    for (at, e) in &found {
        let exit = e["exit_code"]
            .as_i64()
            .unwrap_or_else(|| panic!("{at}: exit_code")) as i32;
        let class = ExitClass::from_code(exit).unwrap_or_else(|| panic!("{at}: exit {exit}"));
        let name = CLASS_NAMES.iter().find(|(c, _)| *c == exit).unwrap().1;
        assert_eq!(e["class"], name, "{at}: class is the name of exit_code");
        assert_eq!(
            e["retryable"],
            class.code() == ExitClass::Retryable.code(),
            "{at}: retryable is exit_code == Retryable"
        );
        match e["code"].as_str() {
            Some(id) => {
                let reg = codes::ALL
                    .iter()
                    .find(|c| c.id == id)
                    .unwrap_or_else(|| panic!("{at}: {id} is not a registered code"));
                assert_eq!(
                    e["kind"],
                    reg.kind.name(),
                    "{at}: kind comes from the registry"
                );
                assert_eq!(
                    e["action"], reg.action,
                    "{at}: action comes from the registry"
                );
                coded += 1;
            }
            None => {
                assert!(
                    e["kind"].is_null() && e["action"].is_null(),
                    "{at}: uncoded"
                );
                uncoded += 1;
            }
        }
    }
    assert!(
        coded > 0 && uncoded > 0,
        "show a coded and an uncoded failure"
    );
    assert_eq!(
        CLASS_NAMES.len(),
        (1..=6)
            .filter(|c| ExitClass::from_code(*c).is_some())
            .count()
    );
}

#[test]
fn the_process_object_keeps_the_integer_exit_class() {
    let obj = fixture("json_errors.json");
    assert!(obj["error"].is_string(), "error stays the redacted text");
    assert!(obj["exit_class"].is_i64(), "exit_class stays an integer");
    assert_eq!(obj["exit_class"], obj["exit_code"]);
    assert!(
        obj["class"].is_string(),
        "the class NAME lives under `class`"
    );
}

#[test]
fn the_run_entry_is_what_the_product_serializes_plus_the_pending_keys() {
    let entry = rivet::state::RunAggregateEntry {
        export_name: "orders_cdc".into(),
        status: "success".into(),
        run_id: "r".into(),
        rows: 1,
        files: 1,
        bytes: 1,
        bytes_read: 1,
        duration_ms: 1,
        mode: "cdc".into(),
        error_message: None,
    };
    let mut emitted = keys(&serde_json::to_value(&entry).unwrap());
    for k in NOT_YET_EMITTED {
        assert!(
            emitted.insert(k.to_string()),
            "`{k}` is emitted now: drop it from NOT_YET_EMITTED"
        );
    }
    assert_eq!(emitted, keys(&fixture("run_entry.json")));
}

#[test]
fn nothing_bound_for_xcom_carries_error_text() {
    let xcom = fixture("xcom_unit.json");
    let mut used = BTreeSet::new();
    all_keys(&xcom, &mut used);
    for banned in ["message", "error_message"] {
        assert!(!used.contains(banned), "XCom must not carry `{banned}`");
    }
    let mut want = set(ERROR_KEYS);
    want.remove("message");
    assert_eq!(
        keys(&xcom["error"]),
        want,
        "xcom error is the error object minus message"
    );
}

#[test]
fn the_adr_references_every_fixture() {
    let adr = std::fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(ADR))
        .expect("ADR-0039 exists");
    for (name, _) in SHAPES {
        let path = format!("tests/fixtures/scheduler/{name}");
        assert!(adr.contains(&path), "{ADR} does not reference {path}");
    }
}
