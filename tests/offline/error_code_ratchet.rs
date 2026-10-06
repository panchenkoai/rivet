//! Every failure rivet names should carry a registered code (`rivet_bail!` / `config_bail!`),
//! so the release gate checks a code, not a substring, and an operator reads a kind, not
//! prose. The uncoded `bail!` sites may only shrink: a new one fails this test, and so does a
//! migration that forgets to lower the ceiling, so the win stays banked.

/// Uncoded `bail!(` sites under `src/` (429 on 2026-09-27, when the registry landed).
const CEILING: usize = 419; // ratchet-pin: uncoded-bail-sites

/// Occurrences of a bare `bail!(` (not `rivet_bail!` / `config_bail!`) in `text`.
fn bare_bails(text: &str) -> usize {
    text.match_indices("bail!(")
        .filter(|(i, _)| {
            let before = text[..*i].chars().next_back();
            !before.is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
        })
        .count()
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

#[test]
fn uncoded_bails_only_shrink() {
    let mut files = Vec::new();
    rust_files(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut files,
    );
    let n: usize = files
        .iter()
        .map(|f| bare_bails(&std::fs::read_to_string(f).unwrap()))
        .sum();
    assert!(
        n <= CEILING,
        "{n} uncoded bail! sites, ceiling {CEILING}: a new failure must carry a registered \
         code — `rivet_bail!(codes::…, …)`, adding the code to src/error.rs"
    );
    assert!(
        n == CEILING,
        "{n} uncoded bail! sites, below the ceiling {CEILING}: lower CEILING to {n} to bank it"
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
