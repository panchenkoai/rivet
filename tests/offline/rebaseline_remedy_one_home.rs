//! A CDC message that prescribes a re-baseline ends with `checkpoint_identity::RECOVER`, never its own `re-snapshot` wording.

use std::path::{Path, PathBuf};

/// The one remedy allowed to say `re-snapshot` in its own words: a `columns:` override fix, whose stream position is intact.
const OWN_WORDING: &[(&str, &str)] = &[
    ("src/error.rs", "CDC: then re-snapshot the table)"),
    ("src/source/cdc/value.rs", "then re-snapshot \\"),
];

/// Every `.rs` file under `dir`.
fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for e in std::fs::read_dir(dir).expect("read src").flatten() {
        let p = e.path();
        if p.is_dir() {
            rust_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

/// `(file, line)` of every product line (comments and `#[cfg(test)]` modules excluded) that says `re-snapshot`.
fn own_rebaseline_wordings(root: &Path) -> Vec<(String, String)> {
    let mut files = Vec::new();
    rust_files(&root.join("src"), &mut files);
    let mut hits = Vec::new();
    for f in files {
        let text = std::fs::read_to_string(&f).expect("read a source file");
        let product = text.split("#[cfg(test)]").next().unwrap_or("");
        let rel = f.strip_prefix(root).unwrap().to_string_lossy().into_owned();
        for line in product.lines() {
            let l = line.trim_start();
            if l.starts_with("//") {
                continue;
            }
            if l.to_ascii_lowercase().contains("re-snapshot") {
                hits.push((rel.clone(), l.to_string()));
            }
        }
    }
    hits
}

#[test]
fn every_rebaseline_remedy_routes_through_the_one_home() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let hits = own_rebaseline_wordings(root);
    let stray: Vec<_> = hits
        .iter()
        .filter(|(f, l)| !OWN_WORDING.iter().any(|(of, ol)| f == of && l.contains(ol)))
        .collect();
    assert!(
        stray.is_empty(),
        "a CDC message names a re-snapshot in its own words; end it with \
         checkpoint_identity::RECOVER instead (the one remedy that was run from every state): \
         {stray:#?}"
    );
    assert_eq!(
        hits.len(),
        OWN_WORDING.len(),
        "an OWN_WORDING entry no longer matches a line: {hits:#?}"
    );
}
