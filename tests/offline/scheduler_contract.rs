//! The scheduler contract of ADR-0039: the JSON shapes under `tests/fixtures/scheduler/`.
//!
//! A shape changes in the fixture, here and in the ADR together. Every fixture is checked for
//! its exact keys, value types and closed vocabularies. Where the product already emits part
//! of a shape the test compares the contract with the product's own output; the keys it does
//! not emit yet are listed in the two `*_NOT_YET_EMITTED` lists, which only shrink.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::process::Command;

use rivet::error::{ExitClass, classify_exit, codes};
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

const RUN_STATUS: &[&str] = &["success", "failed", "skipped"];
const STOP_REASON: &[&str] = &["caught_up", "max_events"];
const LOAD_STATUS: &[&str] = &["loaded", "skipped", "failed"];
const LOAD_SKIP: &[&str] = &["up_to_date"];
const COMPACT_STATUS: &[&str] = &["compacted", "skipped", "failed"];
const COMPACT_SKIP: &[&str] = &[
    "no_buffer",
    "full_load",
    "log_view",
    "warehouse_never_compacts",
];

/// Each compact skip reason and a fragment of the text `src/load/compact.rs` prints for it.
const COMPACT_SKIP_TEXT: &[(&str, &str)] = &[
    ("no_buffer", "COMPACT SKIP [{}]: no `{}__changes` buffer"),
    ("full_load", "\"a full load overwrites its table;"),
    ("log_view", "\"a changelog + view table (no `cdc.backfill:`"),
    (
        "log_view",
        "\"a changelog + view table; `load.layout: base_buffer`",
    ),
    (
        "warehouse_never_compacts",
        "\"this warehouse keeps a change log behind a view and never compacts;",
    ),
];

/// Words the contract dropped; no fixture and no line of the ADR may carry one.
const RETIRED: &[&str] = &["nothing_to_do"];

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

/// Members of a killed child's run entry that the parent still writes blank.
const CRASHED_ENTRY_NOT_YET_FILLED: &[&str] = &[
    // ratchet-pin: scheduler-contract-crashed-entry-not-yet-filled strings
    "run_id", "mode",
    // ratchet-pin: end
];

/// Which of the product's three folds built a `--json-errors` text.
#[derive(Clone, Copy)]
enum Fold {
    Exports,
    Loads,
    Children,
}

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
    ("run_entry_caught_up.json", check_run_entry),
    ("run_entry_skipped.json", check_run_entry),
    ("run_entry_failed.json", check_run_entry),
    ("run_entry_crashed.json", check_run_entry),
    ("json_errors.json", |v, at| {
        check_json_errors(v, at, Fold::Exports)
    }),
    ("json_errors_uncoded.json", |v, at| {
        check_json_errors(v, at, Fold::Exports)
    }),
    ("json_errors_crashed.json", |v, at| {
        check_json_errors(v, at, Fold::Children)
    }),
    ("json_errors_mixed.json", |v, at| {
        check_json_errors(v, at, Fold::Children)
    }),
    ("json_errors_load.json", |v, at| {
        check_json_errors(v, at, Fold::Loads)
    }),
    ("load_result.json", |v, at| {
        check_table_result(v, at, LOAD_STATUS, LOAD_SKIP)
    }),
    ("compact_result.json", |v, at| {
        check_table_result(v, at, COMPACT_STATUS, COMPACT_SKIP)
    }),
    ("compact_result_log_only.json", |v, at| {
        check_table_result(v, at, COMPACT_STATUS, COMPACT_SKIP)
    }),
    ("xcom_unit.json", check_xcom),
    ("xcom_unit_load.json", check_xcom),
    ("exit_without_object.json", check_exit_without_object),
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
        require(
            v["error_message"].is_string() && v["error_message"] == v["error"]["message"],
            || format!("{at}: a failed entry keeps error_message, and error.message is that text"),
        )?;
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

/// How stop-worthy a failure is; `None` for a killed unit, which has no exit code.
fn stop_rank(f: &Value) -> Option<u8> {
    let class = ExitClass::from_code(f["exit_code"].as_i64()? as i32)?;
    Some(class.stop_rank())
}

/// The text each of the three folds gives N failures.
fn folded_text(failures: &[Value], primary: usize, exit_class: i64, fold: Fold) -> String {
    let msg = |f: &Value| f["message"].as_str().unwrap_or_default().to_string();
    if let Fold::Children = fold {
        let parts: Vec<String> = failures
            .iter()
            .map(|f| {
                let status = f["exit_code"]
                    .as_i64()
                    .map_or("signal".to_string(), |c| c.to_string());
                format!(
                    "export '{}' exited with status {status}",
                    f["export"].as_str().unwrap_or_default()
                )
            })
            .collect();
        let class = if failures.iter().any(|f| !f["exit_code"].is_null()) {
            format!(": exit class {exit_class}")
        } else {
            String::new()
        };
        return format!("{}{class}", parts.join("; "));
    }
    if failures.len() == 1 {
        return msg(&failures[0]);
    }
    let others: Vec<String> = (0..failures.len())
        .filter(|i| *i != primary)
        .map(|i| msg(&failures[i]))
        .collect();
    let (n, others, primary) = (failures.len(), others.join("; "), msg(&failures[primary]));
    match fold {
        Fold::Loads => {
            format!("{n} load(s) failed; representative error follows (also: {others}): {primary}")
        }
        _ => format!(
            "{n} export(s) failed; representative error follows (also: {others}): \
             exit class {exit_class}: {primary}"
        ),
    }
}

/// The one-line `--json-errors` object, its text built by `fold`.
fn check_json_errors(v: &Value, at: &str, fold: Fold) -> Check {
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
        require(f["table"].is_null() != matches!(fold, Fold::Loads), || {
            format!("{at}.failures[{i}]: only a load or compact failure names a table")
        })?;
    }
    if failures.is_empty() {
        return Ok(());
    }
    let primary = (0..failures.len())
        .filter(|i| stop_rank(&failures[*i]).is_some())
        .max_by_key(|i| stop_rank(&failures[*i]))
        .unwrap_or(0);
    for key in ["code", "kind", "class", "exit_code", "retryable", "action"] {
        let top = v.get(key).cloned().unwrap_or(Value::Null);
        require(top == failures[primary][key], || {
            format!(
                "{at}.{key}: the representative is failures[{primary}], the highest stop_rank \
                 among the units that reported an exit code"
            )
        })?;
    }
    let text = folded_text(failures, primary, exit_class, fold);
    require(v["error"] == text, || {
        format!("{at}.error: the fold emits `{text}`")
    })
}

/// The object `rivet load` / `rivet compact` print, with that command's `statuses`.
fn check_table_result(
    v: &Value,
    at: &str,
    statuses: &'static [&'static str],
    skips: &'static [&'static str],
) -> Check {
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
            ("skip_reason", Word(skips), true),
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
        ("status", Str, false),
        ("run_id", Str, false),
        ("rows", Int, false),
        ("files", Int, true),
        ("stop_reason", Word(STOP_REASON), true),
        ("error", Nested, true),
    ]);
    shape(v, at, &fields)?;
    let (status, of_table) = (
        v["status"].as_str().unwrap_or_default(),
        !v["table"].is_null(),
    );
    let known = if of_table {
        LOAD_STATUS.contains(&status) || COMPACT_STATUS.contains(&status)
    } else {
        RUN_STATUS.contains(&status)
    };
    require(known, || {
        format!("{at}: `{status}` is not a status of this kind of unit")
    })?;
    require(v["files"].is_null() == of_table, || {
        format!("{at}: files is a number for a run unit and null for a load or compact unit")
    })?;
    require(!of_table || v["stop_reason"].is_null(), || {
        format!("{at}: only a run unit has a stop_reason")
    })?;
    require((status == "failed") != v["error"].is_null(), || {
        format!("{at}: error iff failed")
    })?;
    if !v["error"].is_null() {
        check_error(&v["error"], &format!("{at}.error"), false, &[])?;
    }
    Ok(())
}

/// The class ADR-0039 D8 gives a process that ended without printing an error object.
fn class_without_object(exit_status: Option<i64>) -> (&'static str, Option<i64>) {
    match exit_status {
        None => ("crashed", None),
        Some(c @ (1 | 3..=6)) => (CLASS_NAMES[c as usize - 1].1, Some(c)),
        Some(101) => ("internal", Some(6)),
        Some(129..=255) => ("crashed", None),
        Some(_) => ("generic", Some(1)),
    }
}

/// The table an integration builds the error object from when rivet printed none.
fn check_exit_without_object(v: &Value, at: &str) -> Check {
    let rows = v.as_array().ok_or_else(|| format!("{at}: not an array"))?;
    let mut seen = BTreeSet::new();
    for (i, r) in rows.iter().enumerate() {
        let at = format!("{at}[{i}]");
        let fields = [
            ("exit_status", Int, true),
            ("signal", Int, true),
            ("object", Nested, false),
        ];
        shape(r, &at, &fields)?;
        let status = r["exit_status"].as_i64();
        require(status.is_some() != r["signal"].is_u64(), || {
            format!("{at}: a process ends on an exit status or on a signal, never both")
        })?;
        require(status != Some(0), || {
            format!("{at}: exit 0 is not a failure")
        })?;
        let coded = matches!(status, Some(1 | 3..=6));
        let mut object = r["object"].clone();
        require(status != Some(2) || object["retryable"] == false, || {
            format!("{at}: exit 2 with no error object is a usage error and is never retried")
        })?;
        require(object["message"].is_null() == coded, || {
            format!("{at}: exit 1, 3-6 carries no message; every other row names what was seen")
        })?;
        if coded {
            object["message"] = json!("-");
        }
        check_error(&object, &format!("{at}.object"), true, &[])?;
        let (class, exit_code) = class_without_object(status);
        require(
            object["class"] == class && object["exit_code"] == json!(exit_code),
            || format!("{at}: ADR-0039 D8 says {class}, exit_code {exit_code:?}"),
        )?;
        require(object["code"].is_null(), || {
            format!("{at}: a built object has no code")
        })?;
        seen.insert(match status {
            Some(c @ (1..=6 | 101)) => c,
            Some(129..=255) => 129,
            Some(_) => 0,
            None => -1,
        });
    }
    let all: BTreeSet<i64> = (-1..=6).chain([101, 129]).collect();
    require(seen == all, || {
        format!("{at}: one row per line of the D8 table; shown {seen:?}")
    })
}

/// No key and no string anywhere inside `v` carries a retired word.
fn no_retired_word(v: &Value, at: &str) -> Check {
    let clean = |s: &str| {
        require(RETIRED.iter().all(|w| !s.contains(w)), || {
            format!("{at}: `{s}` carries a word the contract retired")
        })
    };
    match v {
        Value::String(s) => clean(s),
        Value::Array(a) => a.iter().try_for_each(|child| no_retired_word(child, at)),
        Value::Object(m) => m.iter().try_for_each(|(k, child)| {
            clean(k)?;
            no_retired_word(child, at)
        }),
        _ => Ok(()),
    }
}

/// The fixture `name` holds its shape and carries no retired word.
fn holds(name: &str, v: &Value) -> Check {
    let rule = SHAPES.iter().find(|s| s.0 == name).expect(name).1;
    no_retired_word(v, name)?;
    rule(v, name)
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
    for (name, _) in SHAPES {
        if let Err(why) = holds(name, &fixture(name)) {
            panic!("{why}");
        }
    }
}

#[test]
fn a_wrong_type_an_unknown_word_or_a_moved_key_is_refused() {
    let cases: &[(&str, &str, Value)] = &[
        ("run_entry.json", "/stop_reason", json!("whatever")),
        ("run_entry.json", "/rows", json!("many")),
        ("run_entry_skipped.json", "/status", json!("done")),
        ("run_entry_skipped.json", "/status", json!("interrupted")),
        ("run_entry_skipped.json", "/stop_reason", json!("caught_up")),
        (
            "run_entry_crashed.json",
            "/error/message",
            json!("export 'events' exited with status signal"),
        ),
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
        (
            "json_errors_crashed.json",
            "/error",
            json!("exited with status signal"),
        ),
        ("json_errors_mixed.json", "/class", json!("crashed")),
        ("json_errors_mixed.json", "/exit_class", json!(1)),
        (
            "json_errors_mixed.json",
            "/error",
            json!(
                "export 'orders_cdc' exited with status 5; export 'events' exited with status signal"
            ),
        ),
        ("load_result.json", "/per_table/1/status", json!(RETIRED[0])),
        (
            "compact_result.json",
            "/per_table/1/status",
            json!(RETIRED[0]),
        ),
        (
            "compact_result.json",
            "/per_table/1/skip_reason",
            json!(RETIRED[0]),
        ),
        (
            "compact_result.json",
            "/per_table/1/skip_reason",
            json!("up_to_date"),
        ),
        ("xcom_unit_load.json", "/status", json!(RETIRED[0])),
        ("error_object.json", "/message", json!(RETIRED[0])),
        ("run_entry.json", "/export_name", json!(RETIRED[0])),
        ("xcom_unit.json", "/files", Value::Null),
        ("xcom_unit.json", "/status", json!("loaded")),
        ("xcom_unit_load.json", "/files", json!(3)),
        ("xcom_unit_load.json", "/status", json!("success")),
        ("xcom_unit_load.json", "/stop_reason", json!("caught_up")),
        ("exit_without_object.json", "/0/object/message", json!("x")),
        (
            "exit_without_object.json",
            "/6/object/class",
            json!("generic"),
        ),
        (
            "exit_without_object.json",
            "/7/object/class",
            json!("generic"),
        ),
        (
            "exit_without_object.json",
            "/7/object/retryable",
            json!(false),
        ),
        (
            "exit_without_object.json",
            "/10/object/class",
            json!("retryable"),
        ),
        ("exit_without_object.json", "/1/exit_status", json!(42)),
        (
            "exit_without_object.json",
            "/1/object/retryable",
            json!(true),
        ),
        (
            "exit_without_object.json",
            "/1/object",
            json!({ "code": null, "kind": null, "class": "retryable", "exit_code": 2,
                    "retryable": true, "action": null, "message": null }),
        ),
        (
            "exit_without_object.json",
            "/1/object",
            json!({ "code": null, "kind": null, "class": "retryable", "exit_code": 2,
                    "retryable": true, "action": null,
                    "message": "rivet exited 2 and printed no error object" }),
        ),
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
        (
            "compact_result.json",
            "/per_table/2/skip_reason",
            json!("a full load overwrites its table; nothing to merge"),
        ),
        ("xcom_unit.json", "/error/message", json!("text")),
        ("xcom_unit.json", "/stop_reason", json!("done")),
    ];
    for (name, pointer, bad) in cases {
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
            holds(name, &v).is_err(),
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
    let mut load = fixture("json_errors_load.json");
    let generic = load["failures"][0].clone();
    for key in ["kind", "class", "exit_code", "retryable", "action"] {
        load[key] = generic[key].clone();
    }
    load["exit_class"] = json!(1);
    load["error"] = json!(folded_text(
        load["failures"].as_array().unwrap(),
        0,
        1,
        Fold::Loads
    ));
    let why = check_json_errors(&load, "load", Fold::Loads).unwrap_err();
    assert!(
        why.contains("highest stop_rank"),
        "a generic failure never represents a batch that holds a retryable one: {why}"
    );
    let mut uncoded = fixture("json_errors_uncoded.json");
    uncoded.as_object_mut().unwrap().remove("kind");
    assert!(check_json_errors(&uncoded, "uncoded", Fold::Exports).is_err());
}

/// The words stored under `key` in the fixtures whose name starts with `family`.
fn shown(family: &str, key: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for (name, _) in SHAPES.iter().filter(|s| s.0.starts_with(family)) {
        values_of(&fixture(name), key, &mut out);
    }
    out
}

#[test]
fn the_fixtures_show_every_case_the_contract_names() {
    let classes = shown("", "class");
    for class in ["crashed", "retryable", "refusal", "generic", "internal"] {
        assert!(classes.contains(class), "no fixture shows class `{class}`");
    }
    assert!(fixture("json_errors_uncoded.json").get("code").is_none());
    assert!(fixture("json_errors.json")["code"].is_string());
    for name in [
        "json_errors.json",
        "json_errors_load.json",
        "json_errors_mixed.json",
    ] {
        let n = fixture(name)["failures"].as_array().unwrap().len();
        assert!(
            n >= 2,
            "{name}: failures[] lists ALL failed units: show two"
        );
    }
    let mixed = shown("json_errors_mixed.json", "class");
    assert!(
        mixed.contains("crashed") && mixed.len() >= 2,
        "the mixed fixture shows a killed child beside one that reported an exit code"
    );
    assert_eq!(
        CLASS_NAMES.len(),
        (0..=255)
            .filter(|c| ExitClass::from_code(*c).is_some())
            .count(),
        "every exit class has exactly one name"
    );
}

#[test]
fn the_adr_and_the_fixtures_spell_every_vocabulary_word() {
    let adr = source(ADR);
    let sets: &[(&str, &str, &[&str])] = &[
        ("run_entry", "status", RUN_STATUS),
        ("run_entry", "stop_reason", STOP_REASON),
        ("load_result", "status", LOAD_STATUS),
        ("load_result", "skip_reason", LOAD_SKIP),
        ("compact_result", "status", COMPACT_STATUS),
        ("compact_result", "skip_reason", COMPACT_SKIP),
    ];
    for (family, key, words) in sets {
        let want: BTreeSet<String> = words.iter().map(|w| w.to_string()).collect();
        assert_eq!(
            shown(family, key),
            want,
            "{family}*.json must show every `{key}` word of the contract, and no other"
        );
    }
    for word in RETIRED {
        assert!(
            !adr.contains(word),
            "{ADR} still writes the retired `{word}`"
        );
    }
    let classes = CLASS_NAMES.iter().map(|c| &c.1).chain(&["crashed"]);
    for word in sets.iter().flat_map(|s| s.2).chain(classes) {
        assert!(
            adr.contains(&format!("`{word}`")),
            "{ADR} never writes `{word}`, a word the fixtures and this test use"
        );
    }
}

#[test]
fn a_killed_childs_entry_is_the_target_shape_until_the_parent_fills_it() {
    let aggregate = source("src/pipeline/aggregate.rs");
    let fallback = aggregate
        .split("out.push(entry.unwrap_or_else(")
        .nth(1)
        .and_then(|rest| rest.split("}));").next())
        .expect("the entry the parent writes for a child with no metric row");
    let blank: BTreeSet<&str> = ["run_id", "mode"]
        .into_iter()
        .filter(|key| fallback.contains(&format!("{key}: String::new(),")))
        .collect();
    let listed: BTreeSet<&str> = CRASHED_ENTRY_NOT_YET_FILLED.iter().copied().collect();
    assert_eq!(
        blank, listed,
        "CRASHED_ENTRY_NOT_YET_FILLED is exactly what the parent still writes blank"
    );
    assert!(
        CRASHED_ENTRY_NOT_YET_FILLED.len() <= 2,
        "two members were blank when ADR-0039 was accepted"
    );
    let entry = fixture("run_entry_crashed.json");
    for key in ["run_id", "mode"] {
        assert!(entry[key] != "", "the contract fills `{key}`");
    }
}

#[test]
fn today_the_exit_of_an_untyped_fold_follows_its_text_documents_two_requirements() {
    let killed = |name: &str| {
        classify_exit(&anyhow::anyhow!(
            "export '{name}' exited with status signal"
        ))
    };
    assert_eq!(killed("events"), 1);
    assert_eq!(killed("dns_events"), 2, "the export NAME decided the exit");
    assert_eq!(killed("orders_timeout"), 2);

    let load = fixture("json_errors_load.json");
    let text = |i: usize| load["failures"][i]["message"].as_str().unwrap().to_string();
    assert_eq!(classify_exit(&anyhow::anyhow!(text(1))), 2);
    let folded = anyhow::anyhow!(text(1)).context(format!(
        "2 load(s) failed; representative error follows (also: {})",
        text(0)
    ));
    assert_eq!(
        format!("{folded:#}"),
        load["error"],
        "this is the text aggregate_load_failures builds"
    );
    assert_eq!(
        classify_exit(&folded),
        1,
        "the OTHER failure's text decided the exit; the contract says 2"
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
            && children.contains(r#"format!("exited with status {code}")"#)
            && children.contains("wait_failures.insert(name.clone(), msg.clone());")
            && children.contains(r#"let msg = failures.join("; ");"#)
            && children.contains("PreclassifiedExit(code)).context(msg)"),
        "the killed-child text or the child fold changed: update the crashed and mixed fixtures"
    );
    assert_eq!(
        fixture("run_entry_crashed.json")["error_message"],
        "exited with status signal",
        "the entry keeps the text the parent stores, which has no export prefix"
    );
    let compact = source("src/load/compact.rs");
    let predicate = compact
        .split("fn compact_skip_reason(")
        .nth(1)
        .and_then(|rest| rest.split("\n}\n").next())
        .expect("the predicate that passes a table by");
    for (word, text) in COMPACT_SKIP_TEXT {
        assert!(
            compact.contains(text),
            "compact no longer prints `{text}`: `{word}` names nothing"
        );
    }
    let worded: BTreeSet<&str> = COMPACT_SKIP_TEXT.iter().map(|t| t.0).collect();
    assert_eq!(
        worded,
        COMPACT_SKIP.iter().copied().collect(),
        "every compact skip reason names a text the product prints"
    );
    let in_predicate = |t: &&(&str, &str)| predicate.contains(t.1);
    assert_eq!(
        COMPACT_SKIP_TEXT.iter().filter(in_predicate).count(),
        predicate.matches("Some(").count(),
        "compact_skip_reason gained or lost a reason: give it a word in COMPACT_SKIP_TEXT"
    );
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
