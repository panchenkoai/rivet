//! The scheduler contract of ADR-0039: the JSON shapes under `tests/fixtures/scheduler/`.
//!
//! A shape changes in the fixture, here and in the ADR together. Every fixture is checked for
//! its exact keys, value types and closed vocabularies. Where the product already emits part
//! of a shape the test compares the contract with the product's own output; the keys it does
//! not emit yet are listed in the two `*_NOT_YET_EMITTED` lists, which only shrink.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::process::Command;

use rivet::error::{ExitClass, codes};
use serde_json::{Value, json};

const ADR: &str = "docs/adr/0039-scheduler-contract.md";
const FIXTURES: &str = "tests/fixtures/scheduler";

type Check = Result<(), String>;

/// A fixture's rule: the value and where it is, for the message.
type Rule = fn(&Value, &str) -> Check;

/// The type a contract value has; `Nested` is an object or array checked by its own rule.
#[derive(Clone, Copy, Debug)]
enum Ty {
    Str,
    Int,
    Bool,
    Word(&'static [&'static str]),
    Nested,
}
use Ty::*;

/// A key, its type, and whether `null` is allowed.
type Field = (&'static str, Ty, bool);

const RUN_STATUS: &[&str] = &["success", "failed", "skipped", "interrupted"];
const STOP_REASON: &[&str] = &["caught_up", "max_events"];
const LOAD_STATUS: &[&str] = &["loaded", "nothing_to_do", "failed"];
const COMPACT_STATUS: &[&str] = &["compacted", "nothing_to_do", "skipped", "failed"];
const UNIT_STATUS: &[&str] = &[
    "success",
    "failed",
    "skipped",
    "interrupted",
    "loaded",
    "compacted",
    "nothing_to_do",
];

const ERROR: &[Field] = &[
    ("code", Str, true),
    ("kind", Str, true),
    ("class", Str, false),
    ("exit_code", Int, true),
    ("retryable", Bool, false),
    ("action", Str, true),
    ("message", Str, false),
];

const RUN_ENTRY: &[Field] = &[
    ("export_name", Str, false),
    ("status", Word(RUN_STATUS), false),
    ("run_id", Str, false),
    ("rows", Int, false),
    ("files", Int, false),
    ("bytes", Int, false),
    ("bytes_read", Int, false),
    ("duration_ms", Int, false),
    ("mode", Str, false),
    ("error_message", Str, true),
    ("error", Nested, true),
    ("stop_reason", Word(STOP_REASON), true),
    ("tables", Nested, true),
];

/// The top-level `--json-errors` object; `code` is the one key that may be absent.
const JSON_ERRORS: &[Field] = &[
    ("error", Str, false),
    ("exit_class", Int, false),
    ("code", Str, false),
    ("exit_code", Int, true),
    ("class", Str, false),
    ("kind", Str, true),
    ("retryable", Bool, false),
    ("action", Str, true),
    ("failures", Nested, false),
];

/// What names the unit of work on every NEW key: the export, and its table where one applies.
const UNIT: &[Field] = &[("export", Str, false), ("table", Str, true)];

/// Keys of the run entry that `RunAggregateEntry` does not serialize yet.
const NOT_YET_EMITTED: &[&str] = &[
    // ratchet-pin: scheduler-contract-not-yet-emitted strings
    "error",
    "stop_reason",
    "tables",
    // ratchet-pin: end
];

/// Keys of the `--json-errors` object that the binary does not print yet.
const JSON_ERRORS_NOT_YET_EMITTED: &[&str] = &[
    // ratchet-pin: scheduler-contract-json-errors-not-yet-emitted strings
    "exit_code",
    "class",
    "kind",
    "retryable",
    "action",
    "failures",
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

/// Every fixture and the rule its top-level object must hold.
const SHAPES: &[(&str, Rule)] = &[
    ("error_object.json", |v, at| check_error(v, at, true, &[])),
    ("error_object_crashed.json", |v, at| {
        check_error(v, at, true, &[])
    }),
    ("run_entry.json", check_run_entry),
    ("run_entry_failed.json", check_run_entry),
    ("run_entry_crashed.json", check_run_entry),
    ("json_errors.json", check_json_errors),
    ("json_errors_uncoded.json", check_json_errors),
    ("json_errors_crashed.json", check_json_errors),
    ("json_errors_load.json", check_json_errors),
    ("load_result.json", |v, at| {
        check_table_result(v, at, LOAD_STATUS)
    }),
    ("compact_result.json", |v, at| {
        check_table_result(v, at, COMPACT_STATUS)
    }),
    ("xcom_unit.json", check_xcom),
];

/// The repository root.
fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Parse one fixture.
fn fixture(name: &str) -> Value {
    let path = root().join(FIXTURES).join(name);
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{name}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{name}: not valid JSON: {e}"))
}

/// A tracked source file as text.
fn source(path: &str) -> String {
    std::fs::read_to_string(root().join(path)).unwrap_or_else(|e| panic!("{path}: {e}"))
}

/// `Err(msg)` unless `ok`.
fn require(ok: bool, msg: impl FnOnce() -> String) -> Check {
    if ok { Ok(()) } else { Err(msg()) }
}

/// `v` is an object with exactly `fields`, each of its type.
fn shape(v: &Value, at: &str, fields: &[Field]) -> Check {
    let obj = v
        .as_object()
        .ok_or_else(|| format!("{at}: expected an object, got {v}"))?;
    let want: BTreeSet<&str> = fields.iter().map(|f| f.0).collect();
    let got: BTreeSet<&str> = obj.keys().map(String::as_str).collect();
    require(want == got, || {
        format!("{at}: keys {got:?}, contract {want:?}")
    })?;
    for (key, ty, nullable) in fields {
        let x = &obj[*key];
        let ok = match ty {
            Str => x.is_string(),
            Int => x.is_u64(),
            Bool => x.is_boolean(),
            Word(words) => x.as_str().is_some_and(|s| words.contains(&s)),
            Nested => x.is_object() || x.is_array(),
        };
        require(ok || (x.is_null() && *nullable), || {
            format!("{at}.{key}: {x} is not {ty:?} (null allowed: {nullable})")
        })?;
    }
    Ok(())
}

/// The D1 error object, with `extra` unit keys, with or without `message`.
fn check_error(e: &Value, at: &str, message: bool, extra: &[Field]) -> Check {
    let mut fields: Vec<Field> = ERROR.to_vec();
    fields.retain(|f| message || f.0 != "message");
    fields.extend_from_slice(extra);
    shape(e, at, &fields)?;
    if e["class"] == "crashed" {
        require(
            e["exit_code"].is_null() && e["retryable"] == true && e["code"].is_null(),
            || format!("{at}: crashed is exit_code null, retryable true, uncoded"),
        )?;
    } else {
        let exit = e["exit_code"]
            .as_i64()
            .ok_or_else(|| format!("{at}: only `crashed` has no exit_code"))?
            as i32;
        let class = ExitClass::from_code(exit).ok_or_else(|| format!("{at}: exit {exit}"))?;
        let name = CLASS_NAMES.iter().find(|(c, _)| *c == exit).map(|c| c.1);
        require(e["class"].as_str() == name, || {
            format!("{at}: class {} is not the name of exit {exit}", e["class"])
        })?;
        require(
            e["retryable"] == (class.code() == ExitClass::Retryable.code()),
            || format!("{at}: retryable is exit_code == Retryable"),
        )?;
    }
    match e["code"].as_str() {
        Some(id) => {
            let reg = codes::ALL
                .iter()
                .find(|c| c.id == id)
                .ok_or_else(|| format!("{at}: {id} is not a registered code"))?;
            require(
                e["kind"] == reg.kind.name() && e["action"] == reg.action,
                || format!("{at}: kind and action come from the registry"),
            )?;
        }
        None => require(e["kind"].is_null() && e["action"].is_null(), || {
            format!("{at}: an uncoded failure has no kind and no action")
        })?,
    }
    require(!message || e["message"] != "", || {
        format!("{at}: empty message")
    })
}

/// One `per_export[]` entry of the run aggregate.
fn check_run_entry(v: &Value, at: &str) -> Check {
    shape(v, at, RUN_ENTRY)?;
    let (failed, cdc) = (v["status"] == "failed", v["mode"] == "cdc");
    require(failed != v["error"].is_null(), || {
        format!("{at}: error iff failed")
    })?;
    if failed {
        check_error(&v["error"], &format!("{at}.error"), true, &[])?;
        require(v["error_message"].is_string(), || {
            format!("{at}: a failed entry keeps error_message")
        })?;
    }
    require(
        v["stop_reason"].is_null() != (cdc && v["status"] == "success"),
        || format!("{at}: stop_reason iff a successful cdc entry"),
    )?;
    require(
        v["tables"].is_array() == cdc && v["tables"].is_null() != cdc,
        || format!("{at}: tables[] iff mode cdc"),
    )?;
    for (i, t) in v["tables"].as_array().into_iter().flatten().enumerate() {
        let fields = [
            ("table", Str, false),
            ("rows", Int, false),
            ("files", Int, false),
        ];
        shape(t, &format!("{at}.tables[{i}]"), &fields)?;
    }
    Ok(())
}

/// The text the two folds give N failures: the representative one last, the others listed.
fn folded_text(failures: &[Value], primary: usize, exit_class: i64) -> String {
    let msg = |f: &Value| f["message"].as_str().unwrap_or_default().to_string();
    if failures.len() == 1 {
        return msg(&failures[0]);
    }
    let others: Vec<String> = (0..failures.len())
        .filter(|i| *i != primary)
        .map(|i| msg(&failures[i]))
        .collect();
    let (n, others, primary) = (failures.len(), others.join("; "), msg(&failures[primary]));
    if failures[0]["table"].is_null() {
        format!(
            "{n} export(s) failed; representative error follows (also: {others}): \
             exit class {exit_class}: {primary}"
        )
    } else {
        format!("{n} load(s) failed; representative error follows (also: {others}): {primary}")
    }
}

/// The one-line `--json-errors` object.
fn check_json_errors(v: &Value, at: &str) -> Check {
    let coded = v.get("code").is_some();
    let mut fields = JSON_ERRORS.to_vec();
    fields.retain(|f| coded || f.0 != "code");
    shape(v, at, &fields)?;
    let mut as_error = json!({ "code": v.get("code"), "message": v["error"] });
    for key in ["kind", "class", "exit_code", "retryable", "action"] {
        as_error[key] = v[key].clone();
    }
    check_error(&as_error, at, true, &[])?;
    let exit_class = v["exit_class"].as_i64().unwrap_or_default();
    let want = if v["class"] == "crashed" {
        json!(ExitClass::Generic.code())
    } else {
        v["exit_code"].clone()
    };
    require(v["exit_class"] == want, || {
        format!("{at}: exit_class is the process exit code, {want} here")
    })?;
    let failures = v["failures"]
        .as_array()
        .ok_or_else(|| format!("{at}.failures: not an array"))?;
    for (i, f) in failures.iter().enumerate() {
        check_error(f, &format!("{at}.failures[{i}]"), true, UNIT)?;
        require(
            f["table"].is_null() == failures[0]["table"].is_null(),
            || format!("{at}.failures[{i}]: one command names tables on all entries or none"),
        )?;
    }
    if failures.is_empty() {
        return Ok(());
    }
    let code = v.get("code").cloned().unwrap_or(Value::Null);
    let primary = failures
        .iter()
        .position(|f| f["class"] == v["class"] && f["code"] == code)
        .ok_or_else(|| format!("{at}: the top level describes none of failures[]"))?;
    let text = folded_text(failures, primary, exit_class);
    require(v["error"] == text, || {
        format!("{at}.error: the fold emits `{text}`")
    })
}

/// The object `rivet load` / `rivet compact` print, with that command's `statuses`.
fn check_table_result(v: &Value, at: &str, statuses: &'static [&'static str]) -> Check {
    shape(
        v,
        at,
        &[("run_id", Str, false), ("per_table", Nested, false)],
    )?;
    let rows = v["per_table"]
        .as_array()
        .ok_or_else(|| format!("{at}.per_table: not an array"))?;
    for (i, r) in rows.iter().enumerate() {
        let at = format!("{at}.per_table[{i}]");
        let fields = [
            ("export", Str, false),
            ("table", Str, false),
            ("status", Word(statuses), false),
            ("skip_reason", Str, true),
            ("rows", Int, false),
            ("error", Nested, true),
        ];
        shape(r, &at, &fields)?;
        require((r["status"] == "failed") != r["error"].is_null(), || {
            format!("{at}: error iff failed")
        })?;
        require(
            (r["status"] == "skipped") != r["skip_reason"].is_null(),
            || format!("{at}: skip_reason iff skipped"),
        )?;
        if !r["error"].is_null() {
            check_error(&r["error"], &format!("{at}.error"), true, &[])?;
        }
    }
    Ok(())
}

/// What an integration may push to a scheduler's metadata store for one unit.
fn check_xcom(v: &Value, at: &str) -> Check {
    let mut fields = UNIT.to_vec();
    fields.extend_from_slice(&[
        ("status", Word(UNIT_STATUS), false),
        ("run_id", Str, false),
        ("rows", Int, false),
        ("files", Int, false),
        ("stop_reason", Word(STOP_REASON), true),
        ("error", Nested, true),
    ]);
    shape(v, at, &fields)?;
    require((v["status"] == "failed") != v["error"].is_null(), || {
        format!("{at}: error iff failed")
    })?;
    if !v["error"].is_null() {
        check_error(&v["error"], &format!("{at}.error"), false, &[])?;
    }
    Ok(())
}

/// Every string value stored under `key` anywhere inside `v`.
fn values_of(v: &Value, key: &str, out: &mut BTreeSet<String>) {
    match v {
        Value::Object(m) => {
            for (k, child) in m {
                if let (true, Some(s)) = (k == key, child.as_str()) {
                    out.insert(s.to_string());
                }
                values_of(child, key, out);
            }
        }
        Value::Array(a) => a.iter().for_each(|child| values_of(child, key, out)),
        _ => {}
    }
}

/// The JSON kind of a value, so a number is never compared with a string as equal shapes.
fn kind_of(v: &Value) -> std::mem::Discriminant<Value> {
    std::mem::discriminant(v)
}

#[test]
fn every_fixture_is_pinned_and_holds_its_shape() {
    let on_disk: BTreeSet<String> = std::fs::read_dir(root().join(FIXTURES))
        .expect("the fixture directory exists")
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    let pinned: BTreeSet<String> = SHAPES.iter().map(|(n, _)| n.to_string()).collect();
    assert_eq!(
        on_disk, pinned,
        "fixture files and SHAPES must list the same names"
    );
    for (name, check) in SHAPES {
        if let Err(why) = check(&fixture(name), name) {
            panic!("{why}");
        }
    }
}

#[test]
fn a_wrong_type_an_unknown_word_or_a_moved_key_is_refused() {
    let cases: &[(&str, &str, Value)] = &[
        ("run_entry.json", "/stop_reason", json!("whatever")),
        ("run_entry.json", "/rows", json!("many")),
        ("run_entry.json", "/status", json!("done")),
        ("run_entry.json", "/tables/0/rows", json!("1200")),
        ("run_entry.json", "/tables", Value::Null),
        ("run_entry_failed.json", "/error", Value::Null),
        ("run_entry_failed.json", "/error/retryable", json!("no")),
        ("run_entry_crashed.json", "/error/retryable", json!(false)),
        ("run_entry_crashed.json", "/error/exit_code", json!(1)),
        ("error_object.json", "/class", json!("fatal")),
        ("error_object.json", "/exit_code", json!("5")),
        ("error_object.json", "/kind", json!("usage")),
        ("json_errors.json", "/exit_class", json!("refusal")),
        ("json_errors.json", "/exit_class", json!(2)),
        ("json_errors.json", "/code", Value::Null),
        ("json_errors.json", "/error", json!("2 export(s) failed")),
        ("json_errors.json", "/failures/1/class", json!("transient")),
        ("json_errors.json", "/failures/0/table", json!("orders")),
        ("json_errors_crashed.json", "/exit_class", json!(2)),
        ("json_errors_crashed.json", "/class", json!("generic")),
        ("json_errors_load.json", "/failures/0/table", Value::Null),
        ("load_result.json", "/per_table/0/status", json!("skipped")),
        (
            "load_result.json",
            "/per_table/0/skip_reason",
            json!("a full load overwrites its table; nothing to merge"),
        ),
        (
            "compact_result.json",
            "/per_table/0/status",
            json!("loaded"),
        ),
        (
            "compact_result.json",
            "/per_table/2/skip_reason",
            Value::Null,
        ),
        ("xcom_unit.json", "/error/message", json!("text")),
        ("xcom_unit.json", "/stop_reason", json!("done")),
    ];
    for (name, pointer, bad) in cases {
        let check = SHAPES.iter().find(|s| s.0 == *name).expect(name).1;
        let mut v = fixture(name);
        let (parent, key) = pointer.rsplit_once('/').unwrap();
        let slot = v
            .pointer_mut(parent)
            .unwrap_or_else(|| panic!("{name}{parent}"));
        match slot {
            Value::Array(a) => a[key.parse::<usize>().unwrap()] = bad.clone(),
            other => other[key] = bad.clone(),
        }
        assert!(
            check(&v, name).is_err(),
            "{name}: {pointer} = {bad} must be refused"
        );
    }
    let mut renamed = fixture("xcom_unit.json");
    let export = renamed.as_object_mut().unwrap().remove("export").unwrap();
    renamed["unit"] = export;
    assert!(
        check_xcom(&renamed, "xcom").is_err(),
        "one name for the unit"
    );
    let mut uncoded = fixture("json_errors_uncoded.json");
    uncoded.as_object_mut().unwrap().remove("kind");
    assert!(check_json_errors(&uncoded, "uncoded").is_err());
}

#[test]
fn the_fixtures_show_every_case_the_contract_names() {
    let (mut classes, mut statuses, mut stops) =
        (BTreeSet::new(), BTreeSet::new(), BTreeSet::new());
    for (name, _) in SHAPES {
        let v = fixture(name);
        values_of(&v, "class", &mut classes);
        values_of(&v, "status", &mut statuses);
        values_of(&v, "stop_reason", &mut stops);
    }
    for class in ["crashed", "retryable", "refusal", "generic"] {
        assert!(classes.contains(class), "no fixture shows class `{class}`");
    }
    for status in LOAD_STATUS.iter().chain(COMPACT_STATUS) {
        assert!(statuses.contains(*status), "no fixture shows `{status}`");
    }
    assert!(stops.iter().all(|s| STOP_REASON.contains(&s.as_str())));
    assert!(fixture("json_errors_uncoded.json").get("code").is_none());
    assert!(fixture("json_errors.json")["code"].is_string());
    for name in ["json_errors.json", "json_errors_load.json"] {
        let n = fixture(name)["failures"].as_array().unwrap().len();
        assert!(
            n >= 2,
            "{name}: failures[] lists ALL failed units: show two"
        );
    }
    assert_eq!(
        CLASS_NAMES.len(),
        (0..=255)
            .filter(|c| ExitClass::from_code(*c).is_some())
            .count(),
        "every exit class has exactly one name"
    );
}

#[test]
fn the_run_entry_is_what_the_product_serializes_plus_the_pending_keys() {
    let entry = rivet::state::RunAggregateEntry {
        export_name: "orders_cdc".into(),
        status: "failed".into(),
        run_id: "r".into(),
        rows: 1,
        files: 1,
        bytes: 1,
        bytes_read: 1,
        duration_ms: 1,
        mode: "cdc".into(),
        error_message: Some("e".into()),
    };
    let emitted = serde_json::to_value(&entry).unwrap();
    let contract = fixture("run_entry_failed.json");
    let mut pending: BTreeSet<&str> = RUN_ENTRY.iter().map(|f| f.0).collect();
    for (key, value) in emitted.as_object().unwrap() {
        assert!(
            pending.remove(key.as_str()),
            "`{key}` is not in the contract"
        );
        assert!(
            kind_of(value) == kind_of(&contract[key]),
            "`{key}`: the product writes {value}, the contract {}",
            contract[key]
        );
    }
    let listed: BTreeSet<&str> = NOT_YET_EMITTED.iter().copied().collect();
    assert_eq!(
        pending, listed,
        "NOT_YET_EMITTED is exactly the contract keys the product does not write"
    );
    assert!(
        NOT_YET_EMITTED.len() <= 3,
        "three keys were pending when ADR-0039 was accepted; a key added later ships emitted"
    );
}

#[test]
fn the_binary_prints_the_integer_exit_class_and_only_contract_keys() {
    let out = Command::new(env!("CARGO_BIN_EXE_rivet"))
        .args(["--json-errors", "check", "--config"])
        .arg("/nonexistent/rivet-scheduler-contract.yaml")
        .output()
        .expect("spawn rivet");
    let stderr = String::from_utf8_lossy(&out.stderr);
    let line = stderr.lines().last().expect("a line on stderr");
    let v: Value = serde_json::from_str(line).unwrap_or_else(|e| panic!("{e}: {line}"));
    assert!(
        v["exit_class"].is_u64() && v["exit_class"] == out.status.code().unwrap(),
        "exit_class stays the INTEGER exit code: {line}"
    );
    assert!(v["error"].is_string(), "error stays the text: {line}");
    let mut pending: BTreeSet<&str> = JSON_ERRORS.iter().map(|f| f.0).collect();
    for key in v.as_object().unwrap().keys() {
        assert!(
            pending.remove(key.as_str()),
            "`{key}` is not in the contract"
        );
    }
    if pending.remove("code") {
        assert!(codes::ALL.iter().all(|c| !line.contains(c.id)));
    } else {
        let id = v["code"].as_str().expect("code is a string when present");
        assert!(codes::ALL.iter().any(|c| c.id == id), "{id} is registered");
    }
    let listed: BTreeSet<&str> = JSON_ERRORS_NOT_YET_EMITTED.iter().copied().collect();
    assert_eq!(
        pending, listed,
        "JSON_ERRORS_NOT_YET_EMITTED is exactly what the binary does not print"
    );
    assert!(
        JSON_ERRORS_NOT_YET_EMITTED.len() <= 6,
        "six keys were pending when ADR-0039 was accepted; a key added later ships emitted"
    );
}

#[test]
fn the_fixture_texts_are_the_products_own() {
    let run = source("src/pipeline/run.rs");
    assert!(
        run.contains(r#""{} export(s) failed{}; representative error follows (also: {others})""#),
        "fold_failures changed its text: update json_errors.json and folded_text"
    );
    let load = source("src/load/orchestrate.rs");
    assert!(
        load.contains(r#""{} load(s) failed; representative error follows (also: {others})""#),
        "aggregate_load_failures changed its text: update json_errors_load.json and folded_text"
    );
    assert!(
        source("src/error.rs").contains(r#"write!(f, "exit class {}", self.0)"#),
        "the preclassified-exit context changed its text"
    );
    let children = source("src/pipeline/parallel_children.rs");
    assert!(
        children.contains(r#"unwrap_or_else(|| "signal".to_string())"#)
            && children.contains(r#"format!("export '{name}' {msg}")"#)
            && children.contains(r#"format!("exited with status {code}")"#),
        "the killed-child text changed: update the crashed fixtures"
    );
    let compact = source("src/load/compact.rs");
    let mut reasons = BTreeSet::new();
    values_of(&fixture("compact_result.json"), "skip_reason", &mut reasons);
    assert!(!reasons.is_empty(), "show a skipped table");
    for reason in reasons {
        assert!(
            compact.contains(&format!("\"{reason}\"")),
            "`{reason}` is not a text compact_skip_reason returns"
        );
    }
}

#[test]
fn the_adr_links_every_fixture_by_a_path_that_resolves() {
    let adr = source(ADR);
    let adr_dir = root().join(ADR).parent().unwrap().to_path_buf();
    let mut linked = BTreeSet::new();
    for target in adr.split("](").skip(1).filter_map(|s| s.split(')').next()) {
        if !target.contains("fixtures/scheduler/") {
            continue;
        }
        let path = adr_dir.join(target);
        let real = path
            .canonicalize()
            .unwrap_or_else(|e| panic!("{ADR} links {target}, which does not resolve: {e}"));
        assert_eq!(
            real.parent(),
            root().join(FIXTURES).canonicalize().ok().as_deref(),
            "{target} is not a file of {FIXTURES}"
        );
        linked.insert(real.file_name().unwrap().to_string_lossy().into_owned());
    }
    let pinned: BTreeSet<String> = SHAPES.iter().map(|(n, _)| n.to_string()).collect();
    assert_eq!(
        linked, pinned,
        "{ADR} must link every fixture, and only fixtures"
    );
}
