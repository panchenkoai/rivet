//! Module layering of `src/`: a top-level module references only the modules below it in `LAYERS`.
//! Today's upward references are `EXCEPTIONS`; that list only shrinks.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// Top-level modules of `src/`, bottom layer first: (module, what the layer is for).
const LAYERS: &[(&str, &str)] = &[
    ("error", "error type, exit codes and the bail macros"),
    ("test_hook", "fault-injection points the crash tests arm"),
    ("scalar", "source scalar text parsed into typed bounds"),
    ("resource", "process memory sampling and limits"),
    ("workers", "the bounded executor every pool runs on"),
    ("redact", "the credential redaction chokepoint"),
    ("tuning", "source tuning profiles and adaptive sizing"),
    ("types", "the internal type system and delivered text forms"),
    ("config", "the YAML config model and its validation"),
    ("format", "Parquet and CSV writers"),
    ("sql", "dialect SQL text sent to a source"),
    ("enrich", "meta columns added to a batch"),
    ("quality", "quality rules and their streaming tracker"),
    ("journal", "the in-memory record of one run"),
    ("destination", "file destinations: local, S3, GCS, stdout"),
    ("manifest", "the public run manifest"),
    ("state", "the state store: cursors, checkpoints, ledgers"),
    ("plan", "the resolved plan of a run and its artifacts"),
    ("source", "source engines: batch extract and CDC"),
    ("preflight", "check and doctor diagnostics"),
    ("init", "config scaffolding from a live source"),
    ("load", "warehouse loaders and the load driver"),
    ("notify", "run notifications"),
    ("pipeline", "the coordinator that runs an export end to end"),
    ("cli", "the command-line surface and dispatch"),
    ("mcp", "the MCP server"),
    ("fuzz", "fuzz entry points"),
    ("lib", "the library crate root and its re-export windows"),
    ("main", "the rivet binary"),
    ("bin", "the side binaries: rivet-mcp, seed"),
];

/// Upward references measured 2026-10-10 on main 3994d3ca, as `from -> to::item`. Shrink-only.
const EXCEPTIONS: &[&str] = &[
    // ratchet-pin: module-layer-exceptions strings
    "config -> plan::build::parse_column_overrides_pub",
    "config -> source::cdc::ORACLE_CONTINUOUS_REFUSAL",
    "config -> sql::wrappable_query",
    "enrich -> load::cdc::DELETE_FLAG_COLUMN",
    "error -> load::Refused",
    "error -> load::Refused::cause",
    "error -> manifest::ManifestInconsistency",
    "error -> pipeline::retry::classify_error",
    "error -> source::StatementDurationTimeout",
    "load -> pipeline::cdc_job::dest_for_table",
    "load -> pipeline::format_bytes",
    "load -> pipeline::retry::retry_backoff_ms",
    "load -> pipeline::run::representative_failure_idx",
    "load -> pipeline::validate_manifest::MANIFEST_MAX_BYTES",
    "notify -> pipeline::RunSummary",
    "plan -> load::partition_budget::MAX_PARTITIONS_PER_JOB",
    "plan -> load::plan::PartitionSpec",
    "plan -> load::plan::resolved_layout",
    "plan -> load::plan::resolved_partition",
    "plan -> preflight::ExportDiagnostic",
    "plan -> preflight::SMALL_TABLE_ROW_THRESHOLD",
    "plan -> source::Source",
    "plan -> source::TableIntrospection",
    "plan -> source::connect",
    "plan -> source::query::wrap_key_range",
    "preflight -> load::plan::RecordedKeys",
    "preflight -> load::plan::RecordedKeys::new",
    "preflight -> pipeline::chunked::strip_select_star_from",
    "preflight -> pipeline::destination_uri_for_manifest",
    "preflight -> pipeline::refuse_override_case_miss",
    "preflight -> pipeline::retry::classify_error",
    "redact -> pipeline::ipc::route_log_line",
    "source -> load::cdc::DELETE_FLAG_COLUMN",
    "source -> pipeline::batch_partition_buckets",
    "source -> pipeline::commit::PartRecord",
    "source -> pipeline::commit::write_part_file",
    "source -> pipeline::manifest_writer::write_manifest",
    "source -> pipeline::manifest_writer::write_manifest_without_success_marker",
    "source -> pipeline::validate::count_csv_records",
    "source -> preflight::cdc_health::pg_foreign_slots_warning",
    "source -> preflight::cdc_health::pg_retained_wal_warning",
    "state -> source::host_is_loopback",
    "state -> source::postgres::connect_client",
    "state -> source::url_tls",
    "tuning -> source::Source",
    "tuning -> source::Source::sample_governor_pressure",
    "tuning -> source::batch_controller::DEFAULT_BATCH_TARGET_MB",
]; // ratchet-pin: end

/// Keywords that open an item or a `let`, where a top-level `,` does not end it.
const ITEM_WORDS: &[&str] = &[
    "pub",
    "mod",
    "fn",
    "use",
    "impl",
    "struct",
    "enum",
    "union",
    "const",
    "static",
    "type",
    "trait",
    "unsafe",
    "async",
    "extern",
    "macro_rules",
    "let",
];

#[derive(Clone, Debug, PartialEq)]
enum Tok {
    Ident(String),
    Sep,
    Punct(char),
    Lit,
}

type Toks = Vec<(Tok, usize)>;

/// The index after the char literal opening at `c[i]`, or None when it is a lifetime.
fn char_literal_end(c: &[char], i: usize) -> Option<usize> {
    if c.get(i + 1) == Some(&'\\') {
        let close = c.get(i + 3..)?.iter().position(|x| *x == '\'')?;
        Some(i + 4 + close)
    } else {
        (c.get(i + 2) == Some(&'\'')).then_some(i + 3)
    }
}

/// Consumes a string body starting at `i`, closed by `"` plus `hashes` `#`; returns the index after it.
fn string_body(
    c: &[char],
    mut i: usize,
    hashes: usize,
    raw: bool,
    line: &mut usize,
    out: &mut Toks,
) -> usize {
    let (start, at) = (i, *line);
    let closes = |k: usize| c[k] == '"' && (1..=hashes).all(|h| c.get(k + h) == Some(&'#'));
    while i < c.len() && !closes(i) {
        if !raw && c[i] == '\\' {
            i += 1;
        }
        *line += usize::from(c.get(i) == Some(&'\n'));
        i += 1;
    }
    let body: String = c[start..i.min(c.len())].iter().collect();
    let path_chars = |x: char| x.is_alphanumeric() || x == '_' || x == ':';
    if body.starts_with("crate::") && body.chars().all(path_chars) {
        out.extend(lex(&body).into_iter().map(|(t, _)| (t, at)));
    } else {
        out.push((Tok::Lit, at));
    }
    i + 1 + hashes
}

/// Rust source as tokens with 1-based lines: comments dropped, literals blanked, a string that is exactly a `crate::` path kept as that path.
fn lex(text: &str) -> Toks {
    let c: Vec<char> = text.chars().collect();
    let word = |x: char| x.is_alphanumeric() || x == '_';
    let (mut i, mut line, mut out) = (0, 1, Toks::new());
    while i < c.len() {
        let (ch, next, at) = (c[i], c.get(i + 1).copied(), line);
        if ch.is_whitespace() {
            line += usize::from(ch == '\n');
            i += 1;
        } else if ch == '/' && next == Some('/') {
            while i < c.len() && c[i] != '\n' {
                i += 1;
            }
        } else if ch == '/' && next == Some('*') {
            let mut depth = 0;
            while i < c.len() {
                match (c[i], c.get(i + 1).copied()) {
                    ('/', Some('*')) => {
                        depth += 1;
                        i += 2;
                    }
                    ('*', Some('/')) => {
                        depth -= 1;
                        i += 2;
                        if depth == 0 {
                            break;
                        }
                    }
                    (x, _) => {
                        line += usize::from(x == '\n');
                        i += 1;
                    }
                }
            }
        } else if ch == '"' {
            i = string_body(&c, i + 1, 0, false, &mut line, &mut out);
        } else if ch == '\'' {
            match char_literal_end(&c, i) {
                Some(end) => {
                    out.push((Tok::Lit, at));
                    i = end;
                }
                None => {
                    i += 1;
                    while i < c.len() && word(c[i]) {
                        i += 1;
                    }
                }
            }
        } else if word(ch) {
            let start = i;
            while i < c.len() && word(c[i]) {
                i += 1;
            }
            let w: String = c[start..i].iter().collect();
            let hashes = c[i..].iter().take_while(|x| **x == '#').count();
            let quote = c.get(i + hashes) == Some(&'"');
            let byte_char = (w == "b" && c.get(i) == Some(&'\''))
                .then(|| char_literal_end(&c, i))
                .flatten();
            if matches!(w.as_str(), "r" | "br" | "cr") && quote {
                i = string_body(&c, i + hashes + 1, hashes, true, &mut line, &mut out);
            } else if matches!(w.as_str(), "b" | "c") && hashes == 0 && quote {
                i = string_body(&c, i + 1, 0, false, &mut line, &mut out);
            } else if let Some(end) = byte_char {
                out.push((Tok::Lit, at));
                i = end;
            } else if w == "r" && hashes == 1 && c.get(i + 1).is_some_and(|x| word(*x)) {
                i += 1;
            } else if ch.is_ascii_digit() {
                out.push((Tok::Lit, at));
            } else {
                out.push((Tok::Ident(w), at));
            }
        } else if ch == ':' && next == Some(':') {
            out.push((Tok::Sep, at));
            i += 2;
        } else {
            out.push((Tok::Punct(ch), at));
            i += 1;
        }
    }
    out
}

/// A path as written: its line, the module it sits in, its segments, and the name a `use` binds.
#[derive(Clone, Debug, PartialEq)]
struct PathRef {
    line: usize,
    ctx: Vec<String>,
    segs: Vec<String>,
    bind: Option<String>,
}

/// What one file contributes: its paths, module declarations, exported macros and bare macro calls.
#[derive(Default)]
struct FileScan {
    paths: Vec<PathRef>,
    test_mods: Vec<Vec<String>>,
    inline_mods: Vec<Vec<String>>,
    macros: Vec<String>,
    calls: Vec<(usize, String)>,
    unread: Vec<String>,
}

/// The token at `i`, when there is one.
fn tok(t: &[(Tok, usize)], i: usize) -> Option<&Tok> {
    t.get(i).map(|x| &x.0)
}

/// The identifier at `i`, when the token is one.
fn ident_at(t: &[(Tok, usize)], i: usize) -> Option<&str> {
    match tok(t, i) {
        Some(Tok::Ident(s)) => Some(s.as_str()),
        _ => None,
    }
}

/// The index of the delimiter that closes the one at `open`.
fn closer_of(t: &[(Tok, usize)], open: usize) -> usize {
    let mut depth = 0usize;
    for (k, (x, _)) in t.iter().enumerate().skip(open) {
        match x {
            Tok::Punct('(' | '[' | '{') => depth += 1,
            Tok::Punct(')' | ']' | '}') => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return k;
                }
            }
            _ => {}
        }
    }
    t.len()
}

/// Whether an attribute gates its item on `test`; None for a `cfg` shape this guard does not read.
fn test_gate(attr: &[(Tok, usize)]) -> Option<bool> {
    let words: Vec<&str> = (0..attr.len()).filter_map(|i| ident_at(attr, i)).collect();
    if words.first() != Some(&"cfg") || !words.contains(&"test") {
        return Some(false);
    }
    let all = words.get(1) == Some(&"all") && !words.iter().any(|w| matches!(*w, "not" | "any"));
    match words.as_slice() {
        ["cfg", "test"] => Some(true),
        ["cfg", "not", "test"] => Some(false),
        _ if all => Some(true),
        _ => None,
    }
}

/// The index after any attributes that start at `i`.
fn after_attributes(t: &[(Tok, usize)], mut i: usize) -> usize {
    while tok(t, i) == Some(&Tok::Punct('#')) && tok(t, i + 1) == Some(&Tok::Punct('[')) {
        i = closer_of(t, i + 1) + 1;
    }
    i
}

/// The name declared by an out-of-line `mod name;` that starts at `i`, attributes and visibility skipped.
fn mod_declaration(t: &[(Tok, usize)], i: usize) -> Option<String> {
    let mut i = after_attributes(t, i);
    if ident_at(t, i) == Some("pub") {
        i += 1;
        if tok(t, i) == Some(&Tok::Punct('(')) {
            i = closer_of(t, i) + 1;
        }
    }
    let declares = ident_at(t, i) == Some("mod") && tok(t, i + 2) == Some(&Tok::Punct(';'));
    declares
        .then(|| ident_at(t, i + 1).map(str::to_string))
        .flatten()
}

/// The index after the item, statement, field or arm that starts at `i`.
fn skip_item(t: &[(Tok, usize)], i: usize) -> usize {
    let mut i = after_attributes(t, i);
    let item = ident_at(t, i).is_some_and(|w| ITEM_WORDS.contains(&w));
    let mut depth = 0usize;
    while let Some(x) = tok(t, i) {
        match x {
            Tok::Punct('(' | '[' | '{') => depth += 1,
            Tok::Punct(')' | ']' | '}') => {
                if depth == 0 {
                    return i;
                }
                depth -= 1;
                if depth == 0 && *x == Tok::Punct('}') {
                    return i + 1;
                }
            }
            Tok::Punct(';') if depth == 0 => return i + 1,
            Tok::Punct(',') if depth == 0 && !item => return i + 1,
            _ => {}
        }
        i += 1;
    }
    i
}

/// One leaf of a `use` tree: its path, and the name it binds unless it is a glob or `as _`.
fn use_leaf(mut segs: Vec<String>, alias: Option<&str>, line: usize, ctx: &[String]) -> PathRef {
    let glob = segs.last().is_some_and(|s| s == "*");
    if segs.len() > 1 && segs.last().is_some_and(|s| s == "self") {
        segs.pop();
    }
    let bind = match alias {
        Some("_") => None,
        Some(a) => Some(a.to_string()),
        None => segs.last().filter(|_| !glob).cloned(),
    };
    PathRef {
        line,
        ctx: ctx.to_vec(),
        segs,
        bind,
    }
}

/// Reads one `use` tree at `i` into `out`; returns the index after it, or None for a shape it cannot read.
fn use_tree(
    t: &[(Tok, usize)],
    mut i: usize,
    mut pre: Vec<String>,
    ctx: &[String],
    out: &mut Vec<PathRef>,
) -> Option<usize> {
    loop {
        let line = t.get(i)?.1;
        match tok(t, i)? {
            Tok::Punct('$') if pre.is_empty() => i += 1,
            Tok::Sep if pre.is_empty() => {
                pre.push("::".into());
                i += 1;
            }
            Tok::Ident(s) => {
                pre.push(s.clone());
                i += 1;
                match tok(t, i) {
                    Some(Tok::Sep) => i += 1,
                    Some(Tok::Ident(a)) if a == "as" => {
                        out.push(use_leaf(pre, Some(ident_at(t, i + 1)?), line, ctx));
                        return Some(i + 2);
                    }
                    _ => {
                        out.push(use_leaf(pre, None, line, ctx));
                        return Some(i);
                    }
                }
            }
            Tok::Punct('*') => {
                pre.push("*".into());
                out.push(use_leaf(pre, None, line, ctx));
                return Some(i + 1);
            }
            Tok::Punct('{') => {
                i += 1;
                loop {
                    if tok(t, i)? == &Tok::Punct('}') {
                        return Some(i + 1);
                    }
                    i = use_tree(t, i, pre.clone(), ctx, out)?;
                    match tok(t, i)? {
                        Tok::Punct(',') => i += 1,
                        Tok::Punct('}') => {}
                        _ => return None,
                    }
                }
            }
            _ => return None,
        }
    }
}

/// Scans one file whose module path from its crate root is `base`, outside `#[cfg(test)]` items.
fn scan_file(text: &str, base: &[String]) -> FileScan {
    let t = lex(text);
    let mut out = FileScan::default();
    let (mut i, mut depth, mut exported, mut unbalanced) = (0, 0usize, false, false);
    let mut mods: Vec<(String, usize)> = Vec::new();
    let ctx = |mods: &[(String, usize)]| -> Vec<String> {
        let inline = mods.iter().map(|m| m.0.clone());
        base.iter().cloned().chain(inline).collect()
    };
    while i < t.len() {
        let line = t[i].1;
        match &t[i].0 {
            Tok::Punct('#') => {
                let inner = tok(&t, i + 1) == Some(&Tok::Punct('!'));
                let open = i + 1 + usize::from(inner);
                if tok(&t, open) != Some(&Tok::Punct('[')) {
                    i += 1;
                    continue;
                }
                let close = closer_of(&t, open);
                let attr = &t[(open + 1).min(close)..close.min(t.len())];
                match test_gate(attr) {
                    None => {
                        out.unread
                            .push(format!("{line}: a `cfg` on `test` this guard cannot read"));
                        i = close + 1;
                    }
                    Some(false) => {
                        exported |= ident_at(attr, 0) == Some("macro_export");
                        i = open + 1;
                    }
                    Some(true) if inner && depth == 0 => {
                        out.test_mods.push(base.to_vec());
                        return out;
                    }
                    Some(true) if inner => {
                        out.unread.push(format!(
                            "{line}: an inner `#![cfg(test)]` inside a block: gate the item instead"
                        ));
                        i = close + 1;
                    }
                    Some(true) => {
                        if let Some(name) = mod_declaration(&t, close + 1) {
                            let mut path = ctx(&mods);
                            path.push(name);
                            out.test_mods.push(path);
                        }
                        i = skip_item(&t, close + 1);
                    }
                }
            }
            Tok::Punct('{') => {
                depth += 1;
                i += 1;
            }
            Tok::Punct('}') => {
                unbalanced |= depth == 0;
                depth = depth.saturating_sub(1);
                if mods.last().is_some_and(|m| m.1 == depth) {
                    mods.pop();
                }
                i += 1;
            }
            Tok::Ident(w) if w == "mod" => match (ident_at(&t, i + 1), tok(&t, i + 2)) {
                (Some(name), Some(Tok::Punct('{'))) => {
                    mods.push((name.to_string(), depth));
                    out.inline_mods.push(ctx(&mods));
                    depth += 1;
                    i += 3;
                }
                _ => i += 1,
            },
            Tok::Ident(w) if w == "use" && tok(&t, i + 1) != Some(&Tok::Punct('<')) => {
                let end = use_tree(&t, i + 1, Vec::new(), &ctx(&mods), &mut out.paths);
                match end {
                    Some(end) if tok(&t, end) == Some(&Tok::Punct(';')) => i = end + 1,
                    _ => {
                        out.unread
                            .push(format!("{line}: a `use` tree this guard cannot read"));
                        i += 1;
                    }
                }
            }
            Tok::Ident(w) => {
                let mut segs = vec![w.clone()];
                let mut j = i + 1;
                while let (Some(Tok::Sep), Some(s)) = (tok(&t, j), ident_at(&t, j + 1)) {
                    segs.push(s.to_string());
                    j += 2;
                }
                let bang = tok(&t, j) == Some(&Tok::Punct('!'));
                if w == "macro_rules" {
                    if let (true, Some(name)) = (exported && bang, ident_at(&t, j + 1)) {
                        out.macros.push(name.to_string());
                    }
                    exported = false;
                } else if segs.len() > 1 {
                    out.paths.push(PathRef {
                        line,
                        ctx: ctx(&mods),
                        segs,
                        bind: None,
                    });
                } else if bang {
                    out.calls.push((line, w.clone()));
                }
                i = j;
            }
            _ => i += 1,
        }
    }
    if unbalanced || depth != 0 {
        out.unread
            .push("1: braces do not balance, the scan lost its place".to_string());
    }
    out
}

/// `a` followed by `b`.
fn chained(a: &[String], b: &[String]) -> Vec<String> {
    a.iter().chain(b).cloned().collect()
}

/// The path from the library root that `p` names, when it resolves inside the library.
fn resolve_path(
    p: &PathRef,
    root: &str,
    aliases: &BTreeMap<String, Vec<String>>,
    modules: &BTreeSet<Vec<String>>,
) -> Option<Vec<String>> {
    let head = p.segs[0].as_str();
    if head == root {
        return Some(p.segs[1..].to_vec());
    }
    if root == "crate" {
        let own = usize::from(head == "self");
        let ups = p.segs[own..].iter().take_while(|s| *s == "super").count();
        if own + ups > 0 {
            let keep = p.ctx.len().checked_sub(ups)?;
            return Some(chained(&p.ctx[..keep], &p.segs[own + ups..]));
        }
    }
    if let Some(target) = aliases.get(head) {
        return Some(chained(target, &p.segs[1..]));
    }
    let child = chained(&p.ctx, &p.segs[..1]);
    (root == "crate" && modules.contains(&child)).then(|| chained(&p.ctx, &p.segs))
}

/// The names a file's `use` declarations bind to library modules.
fn module_aliases(
    scan: &FileScan,
    root: &str,
    modules: &BTreeSet<Vec<String>>,
) -> BTreeMap<String, Vec<String>> {
    let mut map = BTreeMap::new();
    for _ in 0..3 {
        for p in &scan.paths {
            let (Some(name), Some(target)) = (&p.bind, resolve_path(p, root, &map, modules)) else {
                continue;
            };
            if modules.contains(&target) {
                map.insert(name.clone(), target);
            }
        }
    }
    map
}

/// One product reference from a top-level module to another.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Reference {
    from: String,
    to: String,
    item: String,
    at: String,
}

/// One scanned file: its `src/`-relative path, layer, module path, and the path head that names the library root.
struct Unit<'a> {
    rel: &'a str,
    layer: String,
    base: Vec<String>,
    root: &'static str,
    found: FileScan,
}

/// Scans one `src/`-relative file as part of the library or of a binary.
fn unit_of<'a>(rel: &'a str, text: &str) -> Unit<'a> {
    let mut base: Vec<String> = rel
        .trim_end_matches(".rs")
        .split('/')
        .map(str::to_string)
        .collect();
    let layer = base[0].clone();
    let binary = layer == "bin" || layer == "main";
    if binary {
        base.clear();
    } else if base.last().is_some_and(|p| p == "mod") || rel == "lib.rs" {
        base.pop();
    }
    Unit {
        rel,
        layer,
        root: if binary { "rivet" } else { "crate" },
        found: scan_file(text, &base),
        base,
    }
}

/// Every product reference between top-level modules of `files` (`src/`-relative path, text), and what the scan could not read.
fn references(files: &[(String, String)]) -> (Vec<Reference>, Vec<String>) {
    let units: Vec<Unit> = files.iter().map(|(rel, text)| unit_of(rel, text)).collect();
    let library = |u: &&Unit| u.root == "crate";
    let test_mods: BTreeSet<&[String]> = units
        .iter()
        .filter(library)
        .flat_map(|u| u.found.test_mods.iter().map(Vec::as_slice))
        .collect();
    let under_test = |u: &Unit| (0..=u.base.len()).any(|k| test_mods.contains(&u.base[..k]));
    let product: Vec<&Unit> = units
        .iter()
        .filter(|u| !library(u) || !under_test(u))
        .collect();
    let lib: Vec<&Unit> = product.iter().copied().filter(library).collect();
    let mut modules: BTreeSet<Vec<String>> = lib.iter().map(|u| u.base.clone()).collect();
    modules.extend(lib.iter().flat_map(|u| u.found.inline_mods.iter().cloned()));
    modules.remove(&Vec::new());
    let tops: BTreeSet<&str> = lib
        .iter()
        .filter_map(|u| u.base.first())
        .map(String::as_str)
        .collect();
    let macros: BTreeMap<&str, &str> = lib
        .iter()
        .flat_map(|u| {
            u.found
                .macros
                .iter()
                .map(|m| (m.as_str(), u.layer.as_str()))
        })
        .collect();
    let mut root_names: BTreeMap<&str, Option<Vec<String>>> = BTreeMap::new();
    for u in lib.iter().filter(|u| u.base.is_empty()) {
        let aliases = module_aliases(&u.found, u.root, &modules);
        for p in u.found.paths.iter().filter(|p| p.ctx.is_empty()) {
            if let Some(name) = &p.bind {
                root_names.insert(name, resolve_path(p, u.root, &aliases, &modules));
            }
        }
    }
    let (mut refs, mut unread) = (Vec::new(), Vec::new());
    for u in product {
        unread.extend(u.found.unread.iter().map(|e| format!("src/{}:{e}", u.rel)));
        let aliases = module_aliases(&u.found, u.root, &modules);
        let mut push = |target: Vec<String>, line: usize| {
            if target[0] != u.layer {
                refs.push(Reference {
                    from: u.layer.clone(),
                    to: target[0].clone(),
                    item: target.join("::"),
                    at: format!("src/{}:{line}", u.rel),
                });
            }
        };
        for p in &u.found.paths {
            let Some(mut target) =
                resolve_path(p, u.root, &aliases, &modules).filter(|t| !t.is_empty())
            else {
                continue;
            };
            let head = target[0].clone();
            if !tops.contains(head.as_str()) {
                target = match (macros.get(head.as_str()), root_names.get(head.as_str())) {
                    (Some(owner), _) => chained(&[owner.to_string()], &target),
                    (None, Some(Some(real))) => chained(real, &target[1..]),
                    (None, Some(None)) => continue,
                    (None, None) => chained(&["lib".to_string()], &target),
                };
            }
            push(target, p.line);
        }
        for (line, name) in &u.found.calls {
            if let Some(owner) = macros.get(name.as_str()) {
                push(vec![owner.to_string(), name.clone()], *line);
            }
        }
    }
    (refs, unread)
}

/// Every reason the tree breaks the layering: unplaced modules, unreadable shapes, new upward references, stale exceptions.
fn layer_verdict(files: &[(String, String)], layers: &[&str], exceptions: &[&str]) -> Vec<String> {
    let mut errors = Vec::new();
    let rank: BTreeMap<&str, usize> = layers.iter().enumerate().map(|(i, m)| (*m, i)).collect();
    if rank.len() != layers.len() {
        errors.push("LAYERS names a module twice".to_string());
    }
    let present: BTreeSet<&str> = files
        .iter()
        .map(|(rel, _)| rel.split('/').next().unwrap().trim_end_matches(".rs"))
        .collect();
    for module in &present {
        if !rank.contains_key(module) {
            errors.push(format!(
                "top-level module `{module}` is not placed: add it to LAYERS above everything it references"
            ));
        }
    }
    for module in rank.keys().filter(|m| !present.contains(*m)) {
        errors.push(format!(
            "LAYERS places `{module}`, which src/ no longer has: delete its line"
        ));
    }
    if exceptions.windows(2).any(|w| w[0] >= w[1]) {
        errors.push("EXCEPTIONS must be sorted and free of duplicates".to_string());
    }
    let (refs, unread) = references(files);
    errors.extend(unread);
    let listed: BTreeSet<&str> = exceptions.iter().copied().collect();
    let mut seen = BTreeSet::new();
    for r in &refs {
        let (Some(from), Some(to)) = (rank.get(r.from.as_str()), rank.get(r.to.as_str())) else {
            continue;
        };
        if to <= from {
            continue;
        }
        let key = format!("{} -> {}", r.from, r.item);
        if !listed.contains(key.as_str()) {
            errors.push(format!(
                "{}: `{}` (layer {from}) references `{}` in `{}` (layer {to}) above it: move the item \
                 down or pass it in from above",
                r.at, r.from, r.item, r.to
            ));
        }
        seen.insert(key);
    }
    for stale in listed.iter().filter(|e| !seen.contains(**e)) {
        errors.push(format!(
            "stale exception `{stale}`: that upward reference is gone, delete its line from EXCEPTIONS"
        ));
    }
    errors
}

/// Every `.rs` file under `dir` as (`root`-relative path, text), sorted.
fn read_rust_files(root: &Path, dir: &Path, out: &mut Vec<(String, String)>) {
    for e in std::fs::read_dir(dir).expect("read dir").flatten() {
        let p = e.path();
        if p.is_dir() {
            read_rust_files(root, &p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            let rel = p
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            out.push((
                rel,
                std::fs::read_to_string(&p).expect("read a source file"),
            ));
        }
    }
    out.sort();
}

/// The files of `src/`.
fn src_files() -> Vec<(String, String)> {
    let root = super::nonvacuity::repo_root().join("src");
    let mut files = Vec::new();
    read_rust_files(&root, &root, &mut files);
    files
}

/// An in-memory tree for the scanner's own tests.
fn in_memory_files(files: &[(&str, &str)]) -> Vec<(String, String)> {
    files
        .iter()
        .map(|(rel, text)| (rel.to_string(), text.to_string()))
        .collect()
}

/// The `from -> item` keys of every reference found in an in-memory tree.
fn found_in(files: &[(&str, &str)]) -> Vec<String> {
    let (refs, unread) = references(&in_memory_files(files));
    assert_eq!(unread, Vec::<String>::new());
    let mut keys: Vec<String> = refs
        .iter()
        .map(|r| format!("{} -> {}", r.from, r.item))
        .collect();
    keys.sort();
    keys
}

#[test]
fn src_references_only_point_down_except_the_listed_ones() {
    let files = src_files();
    let layers: Vec<&str> = LAYERS.iter().map(|(m, _)| *m).collect();
    assert!(
        LAYERS.iter().all(|(_, what)| !what.trim().is_empty()),
        "every layer says what it is for"
    );
    let (refs, _) = references(&files);
    super::nonvacuity::require_enumerated(
        refs.len(),
        1000,
        "product references between top-level modules of src/",
        "the scanner in module_layer_guard.rs no longer reads the tree",
    );
    let rank = |m: &str| layers.iter().position(|l| *l == m);
    let up: Vec<&Reference> = refs
        .iter()
        .filter(|r| rank(&r.to) > rank(&r.from))
        .collect();
    let pairs: BTreeSet<(&str, &str)> = up
        .iter()
        .map(|r| (r.from.as_str(), r.to.as_str()))
        .collect();
    println!(
        "module layers: {} references between modules, {} upward occurrences, {} distinct upward \
         items (exceptions listed: {}), {} module pairs",
        refs.len(),
        up.len(),
        up.iter()
            .map(|r| (&r.from, &r.item))
            .collect::<BTreeSet<_>>()
            .len(),
        EXCEPTIONS.len(),
        pairs.len()
    );
    for r in &up {
        println!("upward | {} -> {} | {}", r.from, r.item, r.at);
    }
    let mut weight: BTreeMap<(&str, &str), (usize, BTreeSet<&str>)> = BTreeMap::new();
    for r in &refs {
        let pair = weight.entry((r.from.as_str(), r.to.as_str())).or_default();
        pair.0 += 1;
        pair.1.insert(r.item.as_str());
    }
    for ((from, to), (n, items)) in weight {
        println!("pair | {from} | {to} | {n} | {}", items.len());
    }
    let errors = layer_verdict(&files, &layers, EXCEPTIONS);
    assert!(errors.is_empty(), "\n{}", errors.join("\n"));
}

#[test]
fn the_testing_reference_names_the_same_order() {
    let order: Vec<&str> = LAYERS.iter().map(|(m, _)| *m).collect();
    let line = format!("`{}`", order.join(" < "));
    let doc = super::nonvacuity::subject_text("docs/reference/testing.md");
    assert!(
        doc.contains(&line),
        "docs/reference/testing.md, \"Module layers\", must name the order of LAYERS:\n{line}"
    );
}

#[test]
fn a_new_upward_reference_is_named_with_its_place_and_both_layers() {
    let files = in_memory_files(&[
        ("lib.rs", "pub mod low;\npub mod high;\n"),
        (
            "low.rs",
            "pub fn a() {}\n\nfn b() {\n    crate::high::up();\n}\n",
        ),
        ("high.rs", "pub fn up() { crate::low::a(); }\n"),
    ]);
    let layers = ["low", "high", "lib"];
    assert_eq!(
        layer_verdict(&files, &layers, &[]),
        vec![
            "src/low.rs:4: `low` (layer 0) references `high::up` in `high` (layer 1) above it: move \
             the item down or pass it in from above"
        ]
    );
    assert_eq!(
        layer_verdict(&files, &layers, &["low -> high::up"]),
        Vec::<String>::new()
    );
}

#[test]
fn a_stale_exception_fails_so_the_list_only_shrinks() {
    let files = in_memory_files(&[
        ("lib.rs", "pub mod low;\npub mod high;\n"),
        ("low.rs", "pub fn a() {}\n"),
        ("high.rs", "pub fn up() { crate::low::a(); }\n"),
    ]);
    let layers = ["low", "high", "lib"];
    let errors = layer_verdict(&files, &layers, &["low -> high::up"]);
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(
        errors[0].starts_with("stale exception `low -> high::up`"),
        "{errors:?}"
    );
    let downward = layer_verdict(&files, &layers, &["high -> low::a"]);
    assert!(
        downward[0].starts_with("stale exception `high -> low::a`"),
        "{downward:?}"
    );
    let unsorted = layer_verdict(&files, &layers, &["b -> x", "a -> x"]);
    assert!(unsorted[0].contains("sorted"), "{unsorted:?}");
}

#[test]
fn an_unplaced_or_vanished_module_fails() {
    let files = in_memory_files(&[
        ("lib.rs", "pub mod low;\npub mod fresh;\n"),
        ("low.rs", "pub fn a() {}\n"),
        ("fresh/mod.rs", "pub fn f() { crate::low::a(); }\n"),
    ]);
    let errors = layer_verdict(&files, &["low", "gone", "lib"], &[]);
    assert_eq!(errors.len(), 2, "{errors:?}");
    assert!(
        errors[0].starts_with("top-level module `fresh` is not placed"),
        "{errors:?}"
    );
    assert!(errors[1].starts_with("LAYERS places `gone`"), "{errors:?}");
    let twice = layer_verdict(&files, &["low", "fresh", "low", "lib"], &[]);
    assert!(twice[0].contains("twice"), "{twice:?}");
}

#[test]
fn every_use_tree_shape_is_read() {
    let low = "\
use crate::{high::format_bytes, low::own, error::Result};
use crate::high::{self, deep::{Thing, Other as Renamed}, *};
pub use crate::high::deep as d;
pub(crate) use ::std::fmt;
use super::high::by_super;
use std::collections::{BTreeMap, BTreeSet};
";
    let found = found_in(&[
        ("lib.rs", "pub mod low;\npub mod high;\npub mod error;\n"),
        ("low.rs", low),
        ("high/mod.rs", "pub mod deep;\n"),
        ("high/deep.rs", "\n"),
        ("error.rs", "\n"),
    ]);
    assert_eq!(
        found,
        vec![
            "low -> error::Result",
            "low -> high",
            "low -> high::*",
            "low -> high::by_super",
            "low -> high::deep",
            "low -> high::deep::Other",
            "low -> high::deep::Thing",
            "low -> high::format_bytes",
        ]
    );
}

#[test]
fn a_name_bound_to_a_module_carries_its_later_paths() {
    let low = "\
use crate::high;
use crate::high::deep as d;
use high::deep::Chained;
fn f() -> high::Kind {
    let _ = d::make::<u8>();
    other::thing()
}
";
    let found = found_in(&[
        ("lib.rs", "pub mod low;\npub mod high;\n"),
        ("low.rs", low),
        ("high/mod.rs", "pub mod deep;\n"),
        ("high/deep.rs", "\n"),
    ]);
    assert_eq!(
        found,
        vec![
            "low -> high",
            "low -> high::Kind",
            "low -> high::deep",
            "low -> high::deep::Chained",
            "low -> high::deep::make",
        ]
    );
}

#[test]
fn super_chains_and_inline_modules_resolve_from_where_they_stand() {
    let nested = "\
use super::super::high::A;
mod inner {
    use super::super::super::high::B;
    fn f() { super::sibling(); super::super::super::high::c(); }
}
fn g() { self::inner::f(); }
";
    let found = found_in(&[
        ("lib.rs", "pub mod low;\npub mod high;\n"),
        ("low/mod.rs", "mod nested;\n"),
        ("low/nested.rs", nested),
        ("high.rs", "\n"),
    ]);
    assert_eq!(
        found,
        vec!["low -> high::A", "low -> high::B", "low -> high::c"]
    );
}

#[test]
fn test_only_code_is_not_product_code() {
    let low = "\
#[cfg(test)]
use crate::high::{OnlyInTests, Too};
#[cfg(test)]
mod tests;
#[cfg(test)]
pub(crate) mod fixtures;
#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn helper<A, B>(x: [u8; 2]) -> crate::high::T { crate::high::t() }
#[cfg(all(test, feature = \"x\"))]
impl S { fn f() { crate::high::t(); } }
struct S {
    #[cfg(test)]
    probe: crate::high::Probe,
    real: crate::high::Real,
}
#[cfg(not(test))]
fn shipped() { crate::high::shipped(); }
#[cfg(test)]
mod inline {
    fn f() { let s = \"}\"; let c = '}'; let r = r#\"}\"#; crate::high::t(); }
}
fn after() { crate::high::after(); }
";
    let found = found_in(&[
        ("lib.rs", "pub mod low;\npub mod high;\n"),
        ("low/mod.rs", low),
        ("low/tests.rs", "use crate::high::InTestFile;\n"),
        ("low/tests/deep.rs", "use crate::high::InTestDir;\n"),
        ("low/fixtures/mod.rs", "use crate::high::InFixtures;\n"),
        (
            "low/whole.rs",
            "#![cfg(test)]\nuse crate::high::InWholeFile;\n",
        ),
        ("high.rs", "\n"),
    ]);
    assert_eq!(
        found,
        vec![
            "low -> high::Real",
            "low -> high::after",
            "low -> high::shipped"
        ]
    );
}

#[test]
fn comments_strings_and_literals_hide_nothing_and_invent_nothing() {
    let low = "\
// use crate::high::InLineComment;
/* use crate::high::InBlock; /* nested */ crate::high::StillComment */
/// crate::high::InDoc
fn f<'a>(x: &'a str) -> char {
    let _ = \"use crate::high::InString; \\\" crate::high::AfterEscape\";
    let _ = r#\"crate::high::InRaw \" \"#;
    let _ = b\"crate::high::InBytes \\n\";
    let _ = ('\"', '\\'', b'\"', 0..crate::high::BOUND);
    crate::high::real::<u8>()
}
#[serde(with = \"crate::high::codec\")]
struct S;
#[arg(value_parser = crate::high::parse)]
struct A;
";
    let found = found_in(&[
        ("lib.rs", "pub mod low;\npub mod high;\n"),
        ("low.rs", low),
        ("high.rs", "\n"),
    ]);
    assert_eq!(
        found,
        vec![
            "low -> high::BOUND",
            "low -> high::codec",
            "low -> high::parse",
            "low -> high::real",
        ]
    );
}

#[test]
fn crate_root_names_are_followed_to_their_owner() {
    let lib = "\
pub mod low;
pub mod high;
pub use arrow;
pub use high::Lifted;
pub mod window { pub use crate::high::Seen; }
";
    let high = "\
#[macro_export]
macro_rules! shout { () => { $crate::low::a() }; }
macro_rules! private { () => {}; }
";
    let low = "\
use crate::Lifted;
fn f() { crate::shout!(); shout!(); private!(); crate::window::Seen; crate::arrow::array::X; }
";
    let found = found_in(&[("lib.rs", lib), ("low.rs", low), ("high.rs", high)]);
    assert_eq!(
        found,
        vec![
            "high -> low::a",
            "lib -> high::Lifted",
            "lib -> high::Seen",
            "low -> high::Lifted",
            "low -> high::shout",
            "low -> high::shout",
            "low -> lib::window::Seen",
        ]
    );
}

#[test]
fn a_binary_references_the_library_by_its_crate_name() {
    let found = found_in(&[
        ("lib.rs", "pub mod cli;\n"),
        ("cli.rs", "\n"),
        ("main.rs", "fn main() { rivet::cli::run(); }\n"),
        (
            "bin/seed/main.rs",
            "mod args;\nuse rivet::cli;\nfn main() { cli::go(); crate::args::x(); }\n",
        ),
        ("bin/seed/args.rs", "use crate::cli::NotTheLibrary;\n"),
    ]);
    assert_eq!(
        found,
        vec!["bin -> cli", "bin -> cli::go", "main -> cli::run"]
    );
}

#[test]
fn a_shape_the_scanner_cannot_read_is_an_error_not_a_pass() {
    let files = in_memory_files(&[
        ("lib.rs", "pub mod low;\n"),
        (
            "low.rs",
            "#[cfg(any(test, feature = \"x\"))]\nfn f() {}\nuse crate::low::{a b};\nfn g() -> impl Sized + use<> {}\n",
        ),
        (
            "lost.rs",
            "fn f() { let s = \"never closed; }\nfn g() { crate::low::f(); }\n",
        ),
    ]);
    assert_eq!(
        layer_verdict(&files, &["lost", "low", "lib"], &[]),
        vec![
            "src/low.rs:1: a `cfg` on `test` this guard cannot read",
            "src/low.rs:3: a `use` tree this guard cannot read",
            "src/lost.rs:1: braces do not balance, the scan lost its place",
        ]
    );
}
