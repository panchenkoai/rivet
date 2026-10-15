//! Every failure rivet names should carry a registered code (`rivet_bail!` / `config_bail!`),
//! so the release gate checks a code, not a substring, and an operator reads a kind, not
//! prose. The uncoded `bail!` sites may only shrink: a new one fails this test, and so does a
//! migration that forgets to lower the ceiling, so the win stays banked.

/// Uncoded error sites in product code under `src/`: `bail!(`, `ensure!(` and `anyhow!(`, outside
/// test items, comments and literals. 673 on 2026-10-15, when the count widened from `bail!` alone
/// (401 by the old count): a bare `bail!` rewritten as `anyhow!` no longer leaves the count.
// 673 -> 554 (2026-10-15): every error site under `src/config/` carries a registered code (-119).
// 554 -> 459 (2026-10-15): every error site under `src/load/` carries a registered code (-95).
// 459 -> 339 (2026-10-15): every error site under `src/pipeline/`, the rebuilt resolve_url error in
// `src/plan/build.rs`, and the two test-hook faults carry a registered code (-120).
const CEILING: usize = 339; // ratchet-pin: uncoded-bail-sites

/// Occurrences of a bare `bail!(`, `ensure!(` or `anyhow!(` (not `rivet_bail!` / `config_bail!`) in `text`.
fn bare_bails(text: &str) -> usize {
    ["bail!(", "ensure!(", "anyhow!("]
        .iter()
        .map(|form| {
            text.match_indices(form)
                .filter(|(i, _)| {
                    let before = text[..*i].chars().next_back();
                    !before.is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
                })
                .count()
        })
        .sum()
}

/// `text` with comments removed and the contents of string and char literals blanked, so braces and
/// macro names inside them count for nothing.
fn code_only(text: &str) -> String {
    let b = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'/' if b.get(i + 1) == Some(&b'/') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                let mut depth = 0usize;
                while i < b.len() {
                    if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
                        depth += 1;
                        i += 2;
                    } else if b[i] == b'*' && b.get(i + 1) == Some(&b'/') {
                        depth -= 1;
                        i += 2;
                        if depth == 0 {
                            break;
                        }
                    } else {
                        i += 1;
                    }
                }
            }
            b'r' if matches!(b.get(i + 1), Some(b'"' | b'#'))
                && !i
                    .checked_sub(1)
                    .is_some_and(|p| b[p].is_ascii_alphanumeric() || b[p] == b'_') =>
            {
                let mut j = i + 1;
                let mut hashes = 0;
                while b.get(j) == Some(&b'#') {
                    hashes += 1;
                    j += 1;
                }
                if b.get(j) != Some(&b'"') {
                    out.push('r');
                    i += 1;
                    continue;
                }
                j += 1;
                let close: Vec<u8> = std::iter::once(b'"')
                    .chain(std::iter::repeat_n(b'#', hashes))
                    .collect();
                while j < b.len() && !b[j..].starts_with(&close) {
                    j += 1;
                }
                out.push_str("\"\"");
                i = j + close.len();
            }
            b'"' => {
                i += 1;
                while i < b.len() && b[i] != b'"' {
                    i += if b[i] == b'\\' { 2 } else { 1 };
                }
                out.push_str("\"\"");
                i += 1;
            }
            b'\'' if b.get(i + 1) == Some(&b'\\') || b.get(i + 2) == Some(&b'\'') => {
                i += 1;
                while i < b.len() && b[i] != b'\'' {
                    i += if b[i] == b'\\' { 2 } else { 1 };
                }
                out.push_str("' '");
                i += 1;
            }
            _ => {
                let len = text[i..].chars().next().map_or(1, char::len_utf8);
                out.push_str(&text[i..i + len]);
                i += len;
            }
        }
    }
    out
}

/// `text`'s product code: no comments, blank literals, and no `#[cfg(test)]` item (a `mod`, `fn` or `impl` with a body).
fn product_code(text: &str) -> String {
    let code = code_only(text);
    let mut out = String::new();
    let mut rest = code.as_str();
    while let Some(at) = rest.find("#[cfg(test)]") {
        out.push_str(&rest[..at]);
        let after = &rest[at..];
        let head_end = after.find(['{', ';']).unwrap_or(after.len());
        if after[head_end..].starts_with('{') {
            let mut depth = 0usize;
            let mut end = after.len();
            for (i, c) in after[head_end..].char_indices() {
                match c {
                    '{' => depth += 1,
                    '}' => {
                        depth -= 1;
                        if depth == 0 {
                            end = head_end + i + 1;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            rest = &after[end..];
        } else {
            out.push_str("#[cfg(test)]");
            rest = &after["#[cfg(test)]".len()..];
        }
    }
    out.push_str(rest);
    out
}

/// Files a `#[cfg(test)] mod name;` declaration makes test-only: `name.rs` or `name/` beside it.
fn test_only_paths(file: &std::path::Path, text: &str) -> Vec<std::path::PathBuf> {
    let dir = match file.file_stem().and_then(|s| s.to_str()) {
        Some("mod" | "lib" | "main") => file.parent().unwrap().to_path_buf(),
        Some(stem) => file.parent().unwrap().join(stem),
        None => return Vec::new(),
    };
    let mut out = Vec::new();
    let mut rest = text;
    while let Some(at) = rest.find("#[cfg(test)]") {
        let after = &rest[at + "#[cfg(test)]".len()..];
        let decl = after.trim_start();
        let decl = decl.strip_prefix("pub(crate) ").unwrap_or(decl);
        if let Some(name) = decl
            .strip_prefix("mod ")
            .and_then(|d| d.split_once(';'))
            .map(|(n, _)| n.trim())
            .filter(|n| n.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
        {
            out.push(dir.join(format!("{name}.rs")));
            out.push(dir.join(name));
        }
        rest = after;
    }
    out
}

fn rust_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    for e in std::fs::read_dir(dir).expect("read src").flatten() {
        let p = e.path();
        if p.is_dir() {
            rust_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

/// The uncoded error sites of `src/`'s product code.
fn uncoded_sites() -> usize {
    let mut files = Vec::new();
    rust_files(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut files,
    );
    let texts: Vec<(std::path::PathBuf, String)> = files
        .into_iter()
        .map(|f| {
            let t = std::fs::read_to_string(&f).unwrap();
            (f, t)
        })
        .collect();
    let test_only: Vec<std::path::PathBuf> = texts
        .iter()
        .flat_map(|(f, t)| test_only_paths(f, &code_only(t)))
        .collect();
    texts
        .iter()
        .filter(|(f, _)| !test_only.iter().any(|t| f.starts_with(t)))
        .map(|(_, t)| bare_bails(&product_code(t)))
        .sum()
}

#[test]
fn uncoded_bails_only_shrink() {
    let n = uncoded_sites();
    assert!(
        n <= CEILING,
        "{n} uncoded error sites, ceiling {CEILING}: a new failure must carry a registered \
         code — `rivet_bail!(codes::…, …)`, adding the code to src/error.rs"
    );
    assert!(
        n == CEILING,
        "{n} uncoded error sites, below the ceiling {CEILING}: lower CEILING to {n} to bank it"
    );
}

#[test]
fn the_counter_sees_a_bare_bail_and_not_a_coded_one() {
    assert_eq!(bare_bails("anyhow::bail!(\"x\"); bail!(\"y\");"), 2);
    assert_eq!(
        bare_bails("rivet_bail!(C, \"x\"); config_bail!(C, \"y\");"),
        0
    );
}

#[test]
fn the_counter_sees_every_uncoded_form_and_only_product_code() {
    assert_eq!(
        bare_bails("ensure!(a, \"x\"); anyhow::anyhow!(\"y\"); anyhow!(\"z\");"),
        3
    );
    let text = "fn f() { bail!(\"p\") } // bail!(\"comment\")\n\
                #[cfg(test)]\nmod tests {\n fn t() { bail!(\"t\"); if x { anyhow!(\"u\"); } }\n}\n\
                #[cfg(test)]\nfn helper() -> anyhow::Error { anyhow!(\"h\") }\n\
                fn g() { ensure!(y, \"q\") }";
    assert_eq!(bare_bails(&product_code(text)), 2);
    let decl = "#[cfg(test)]\nmod tests;\n#[cfg(test)]\npub(crate) mod replay;\n#[cfg(test)]\nfn helper() {}";
    let file = std::path::Path::new("src/config/mod.rs");
    assert_eq!(
        test_only_paths(file, decl),
        [
            "src/config/tests.rs",
            "src/config/tests",
            "src/config/replay.rs",
            "src/config/replay"
        ]
        .map(std::path::PathBuf::from)
    );
    assert_eq!(
        test_only_paths(
            std::path::Path::new("src/init/oracle.rs"),
            "#[cfg(test)]\nmod t;"
        )[0],
        std::path::PathBuf::from("src/init/oracle/t.rs")
    );
}

#[test]
fn literals_and_comments_hide_nothing_and_count_nothing() {
    let text = "fn f() { let s = \"bail!( } {{\"; let c = '{'; let r = r#\"anyhow!( }\"#; /* ensure!( } */ bail!(\"x\") }\n\
                #[cfg(test)]\nmod t { fn g() { let s = \"}\"; anyhow!(\"t\") } }\n\
                fn h<'a>(x: &'a str) { ensure!(x.is_empty(), \"y\") }";
    assert_eq!(bare_bails(&product_code(text)), 2);
}
