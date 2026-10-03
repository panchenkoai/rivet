//! ADR-0038 locality metric: every `TextForm` renders in ONE place, `src/types/delivery.rs`.
//! Production code under `src/source/` that builds a form's text itself is a local renderer;
//! the set per form may only shrink.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// A shape is a set of substrings that must all appear on one code line.
type Shape = &'static [&'static str];

/// Canonical forms: (label, canonical renderer in delivery.rs, shapes that build the text locally).
const CANONICAL: &[(&str, &str, &[Shape])] = &[
    (
        "decimal_plain",
        "decimal_plain",
        &[
            &["to_plain_string()"],
            &["\"0\".repeat("],
            &[":0>width"],
            &["digits.len() - scale"],
        ],
    ),
    (
        "iso8601_duration",
        "iso8601_duration",
        &[&["String::from(\"P\")"], &["\"PT0S\""], &["format!(\"P{"]],
    ),
    (
        "iso_timestamp_nanos",
        "iso_timestamp_nanos",
        &[
            &["-%m-%dT"],
            &["}T{:02}"],
            &["DateTime(dt)", "dt.to_string()"],
        ],
    ),
    (
        "time_of_day_offset",
        "time_of_day_offset",
        &[&["{:06}{sign}"]],
    ),
    (
        "time_beyond_day",
        "time_beyond_day",
        &[&["}:{:02}:{:02}.{:06}"]],
    ),
    (
        "uuid36",
        "uuid36",
        &[
            &["Uuid", ".to_string()"],
            &["Guid", ".to_string()"],
            &[".hyphenated()"],
            &["{}-{}-{}-{}-{}"],
        ],
    ),
    (
        "hex_bytes",
        "hex_bytes",
        &[
            &["02x}"],
            &["02X}"],
            &["hex::encode"],
            &["encode_hex"],
            &["encode_upper"],
        ],
    ),
    (
        "bit_string",
        "bit_string",
        &[
            &["'1' } else { '0'"],
            &["\"1\" } else { \"0\""],
            &[":b}"],
            &[":08b}"],
        ],
    ),
];

/// Forms the engine produces itself (CP5): no canonical renderer exists to bypass.
const ENGINE_PRODUCED: &[&str] = &[
    "json",
    "hex_wkb",
    "inet_text",
    "range_text",
    "xml_text",
    "server_text",
];

/// Local renderers measured 2026-10-01 (`src/source/`-relative `file::fn`). Shrink-only.
const CEILING: &[(&str, &[&str])] = &[
    // ratchet-pin: local-renderers strings
    (
        "decimal_plain",
        &[
            "mssql/arrow_convert.rs::numeric_to_decimal_string",
            "oracle/cdc.rs::canonical_number",
            "pg_numeric_wire.rs::numeric_wire_normalized_plain",
        ],
    ),
    (
        "iso8601_duration",
        &[
            "oracle/arrow_convert.rs::interval_ym_iso",
            "postgres/arrow_convert.rs::pg_interval_to_iso8601",
        ],
    ),
    (
        "iso_timestamp_nanos",
        &[
            "cdc/value.rs::render_str",
            "cdc/value.rs::to_json",
            "oracle/arrow_convert.rs::timestamp_text",
        ],
    ),
    ("time_of_day_offset", &[]),
    ("time_beyond_day", &[]),
    (
        "uuid36",
        &[
            "mssql/arrow_convert.rs::cell_text",
            "postgres/arrow_convert.rs::build_pg_text_array",
            "postgres/arrow_convert.rs::utf8",
        ],
    ),
    (
        "hex_bytes",
        &[
            "cdc/value.rs::bytes_to_recoverable_string",
            "mssql/arrow_convert.rs::cell_text",
            "oracle/arrow_convert.rs::upper_hex",
        ],
    ),
    ("bit_string", &[]),
]; // ratchet-pin: end

/// Shape matches read and classified as NOT a value's delivered text: (form, site, why).
const NOT_A_VALUE: &[(&str, &str, &str)] = &[
    (
        "decimal_plain",
        "mssql/mod.rs::numeric_to_display",
        "query_scalar display of a bound",
    ),
    (
        "iso_timestamp_nanos",
        "oracle/mod.rs::cell_text",
        "catalog probe scalar",
    ),
    (
        "iso_timestamp_nanos",
        "query.rs::mssql_cursor_literal",
        "T-SQL cursor literal",
    ),
    (
        "iso_timestamp_nanos",
        "oracle/cdc.rs::parse_datetime",
        "parses mined text, renders nothing",
    ),
    (
        "time_beyond_day",
        "mysql/arrow_convert.rs::fmt_micros_as_time",
        "refusal message text",
    ),
    (
        "uuid36",
        "mssql/mod.rs::scalar_to_string",
        "query_scalar display of a bound",
    ),
    (
        "hex_bytes",
        "mongo/mod.rs::bytes_to_hex",
        "keyset cursor and resume-token encoding",
    ),
    (
        "hex_bytes",
        "mssql/cdc.rs::hex",
        "change-table LSN position",
    ),
];

/// Every `.rs` file under `dir`, sorted.
fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for e in std::fs::read_dir(dir).expect("read dir").flatten() {
        let p = e.path();
        if p.is_dir() {
            rust_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
    out.sort();
}

/// Net `{`/`}` depth change of `line`, skipping string, raw-string and char literals; `raw` carries an open raw string's `#` count.
fn brace_delta(line: &str, in_str: &mut bool, raw: &mut Option<usize>) -> i64 {
    let b = line.as_bytes();
    let (mut i, mut d) = (0, 0i64);
    while i < b.len() {
        if let Some(h) = *raw {
            let close = format!("\"{}", "#".repeat(h));
            match line[i..].find(&close) {
                Some(k) => {
                    i += k + close.len();
                    *raw = None;
                    continue;
                }
                None => return d,
            }
        }
        if *in_str {
            match b[i] {
                b'\\' => i += 2,
                b'"' => {
                    *in_str = false;
                    i += 1;
                }
                _ => i += 1,
            }
            continue;
        }
        match b[i] {
            b'/' if b.get(i + 1) == Some(&b'/') => return d,
            b'r' if b.get(i + 1).is_some_and(|c| *c == b'#' || *c == b'"')
                && (i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_')) =>
            {
                let h = b[i + 1..].iter().take_while(|c| **c == b'#').count();
                if b.get(i + 1 + h) == Some(&b'"') {
                    *raw = Some(h);
                    i += h + 2;
                } else {
                    i += 1;
                }
            }
            b'"' => {
                *in_str = true;
                i += 1;
            }
            b'\'' if b.get(i + 1) == Some(&b'\\') => {
                i += 2 + b[i + 2..]
                    .iter()
                    .position(|c| *c == b'\'')
                    .map_or(0, |k| k + 1);
            }
            b'\'' if b.get(i + 2) == Some(&b'\'') => i += 3,
            b'{' => {
                d += 1;
                i += 1;
            }
            b'}' => {
                d -= 1;
                i += 1;
            }
            _ => i += 1,
        }
    }
    d
}

/// The file's lines outside `#[cfg(test)]` items and full-line comments, each with its enclosing fn.
fn production_lines(text: &str) -> Vec<(String, &str)> {
    let mut out = Vec::new();
    let (mut in_str, mut raw) = (false, None);
    let mut skip: Option<(i64, bool)> = None;
    let mut cur_fn = String::new();
    for line in text.lines() {
        let t = line.trim_start();
        let d = brace_delta(line, &mut in_str, &mut raw);
        if let Some((depth, opened)) = skip.as_mut() {
            *depth += d;
            *opened |= line.contains('{') || t.starts_with("use ") || t.ends_with(';');
            let ended = *opened && *depth <= 0 && (t.ends_with('}') || t.ends_with(';'));
            if ended || (!*opened && t.ends_with(';')) {
                skip = None;
            }
            continue;
        }
        if t.starts_with("#[cfg(test)]") {
            skip = Some((d, line.contains('{')));
            continue;
        }
        if t.starts_with("//") {
            continue;
        }
        if let Some(name) = fn_name(t) {
            cur_fn = name;
        }
        out.push((cur_fn.clone(), line));
    }
    out
}

/// The identifier after a `fn ` keyword on this line, if one is declared here.
fn fn_name(line: &str) -> Option<String> {
    let mut from = 0;
    while let Some(k) = line[from..].find("fn ") {
        let at = from + k;
        let prev = line[..at].chars().last();
        if prev.is_none_or(|c| !(c.is_alphanumeric() || c == '_')) {
            let name: String = line[at + 3..]
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if !name.is_empty() {
                return Some(name);
            }
        }
        from = at + 3;
    }
    None
}

/// form label -> local renderer sites found under `src/source/`.
fn measure() -> BTreeMap<&'static str, BTreeSet<String>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/source");
    let mut files = Vec::new();
    rust_files(&root, &mut files);
    let mut found: BTreeMap<&str, BTreeSet<String>> = CANONICAL
        .iter()
        .map(|(f, _, _)| (*f, BTreeSet::new()))
        .collect();
    for p in files {
        let rel = p
            .strip_prefix(&root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        let text = std::fs::read_to_string(&p).unwrap();
        for (func, line) in production_lines(&text) {
            for (form, _, shapes) in CANONICAL {
                if shapes.iter().any(|s| s.iter().all(|n| line.contains(n))) {
                    found
                        .get_mut(form)
                        .unwrap()
                        .insert(format!("{rel}::{func}"));
                }
            }
        }
    }
    for (form, site, _) in NOT_A_VALUE {
        assert!(
            found.get_mut(form).unwrap().remove(*site),
            "NOT_A_VALUE lists {form} at {site}, which no longer matches a shape: delete the row"
        );
    }
    found
}

/// Production call sites of `name(` anywhere under `src/`, outside delivery.rs.
fn canonical_callers(name: &str) -> Vec<String> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    rust_files(&root, &mut files);
    let needle = format!("{name}(");
    let mut out = Vec::new();
    for p in files {
        let rel = p
            .strip_prefix(&root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        if rel == "types/delivery.rs" {
            continue;
        }
        let text = std::fs::read_to_string(&p).unwrap();
        for (func, line) in production_lines(&text) {
            let hit = line.match_indices(&needle).any(|(i, _)| {
                let prev = line[..i].chars().last();
                prev.is_none_or(|c| !(c.is_alphanumeric() || c == '_'))
                    && !line[..i].trim_end().ends_with("fn")
            });
            if hit {
                out.push(format!("{rel}::{func}"));
            }
        }
    }
    out
}

/// The `TextForm` labels, read from `TextForm::label` in delivery.rs.
fn text_form_labels() -> BTreeSet<String> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/types/delivery.rs");
    let text = std::fs::read_to_string(path).unwrap();
    text.lines()
        .filter_map(|l| {
            let l = l.trim();
            l.strip_prefix("TextForm::")?
                .split_once("=> \"")
                .map(|(_, r)| r.trim_end_matches("\",").to_string())
        })
        .collect()
}

#[test]
fn every_text_form_is_classified_and_every_canonical_renderer_exists() {
    let mut ours: BTreeSet<String> = CANONICAL.iter().map(|(f, _, _)| f.to_string()).collect();
    ours.extend(ENGINE_PRODUCED.iter().map(|f| f.to_string()));
    assert_eq!(
        ours,
        text_form_labels(),
        "a TextForm was added or renamed: classify it in CANONICAL or ENGINE_PRODUCED"
    );
    let delivery = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("src/types/delivery.rs"),
    )
    .unwrap();
    for (_, renderer, _) in CANONICAL {
        assert!(
            delivery.contains(&format!("pub fn {renderer}(")),
            "canonical renderer {renderer} is gone from delivery.rs"
        );
    }
}

#[test]
fn local_renderers_only_shrink() {
    let found = measure();
    let ceiling: BTreeMap<&str, BTreeSet<String>> = CEILING
        .iter()
        .map(|(f, s)| (*f, s.iter().map(|x| x.to_string()).collect()))
        .collect();
    assert_eq!(
        ceiling.keys().collect::<Vec<_>>(),
        found.keys().collect::<Vec<_>>(),
        "CEILING must name every canonical form, an empty list included"
    );
    println!("form | canonical | production callers | local renderers");
    for (form, renderer, _) in CANONICAL {
        let callers = canonical_callers(renderer);
        let local = &found[form];
        println!(
            "{form} | {renderer} | {} {callers:?} | {} {local:?}",
            callers.len(),
            local.len()
        );
    }
    let mut errors = Vec::new();
    for (form, local) in &found {
        for site in local.difference(&ceiling[form]) {
            errors.push(format!(
                "NEW local {form} renderer {site}: call crate::types::{form} instead \
                 (if the match is not a value's delivered text, add it to NOT_A_VALUE with why)"
            ));
        }
        for site in ceiling[form].difference(local) {
            errors.push(format!(
                "{form} renderer {site} is gone: remove it from the ceiling (CEILING) to bank it"
            ));
        }
    }
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}

#[test]
fn the_scanner_skips_test_items_and_names_the_enclosing_fn() {
    let src = "fn a() {\n    let s = format!(\"{b:02x}\");\n}\n#[cfg(test)]\nmod tests {\n    \
               fn t() { let x = \"}\"; let y = '{'; let z = r#\"{\"#; }\n    fn u() {}\n}\n\
               #[cfg(test)]\nuse std::fmt::{self, Write};\nfn b() {}\n";
    let lines = production_lines(src);
    let fns: BTreeSet<&str> = lines.iter().map(|(f, _)| f.as_str()).collect();
    assert_eq!(fns, BTreeSet::from(["a", "b"]));
    assert_eq!(lines[1].0, "a");
    assert!(lines[1].1.contains("02x}"));
}
