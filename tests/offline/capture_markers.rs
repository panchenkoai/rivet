//! The spellings of a rivet capture the batch and CDC oracle gates grade by, derived from the
//! rig and the runner, and the one way a gate finds a call to one (included by both gates).

use std::collections::BTreeSet;
use std::path::PathBuf;

/// `name(` for every `pub fn` of the rig and the runner whose name starts a capture (`run`, `cli`, `spawn`, `drain`, `apply`).
pub fn capture_markers() -> BTreeSet<String> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut sources: Vec<PathBuf> = std::fs::read_dir(root.join("tests/common/rig"))
        .expect("derive capture markers: read tests/common/rig")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "rs"))
        .collect();
    sources.push(root.join("tests/common/runner.rs"));
    let mut out = BTreeSet::new();
    for src in sources {
        let text = std::fs::read_to_string(&src)
            .unwrap_or_else(|e| panic!("derive capture markers: read {}: {e}", src.display()));
        for line in text.lines() {
            if let Some(rest) = line.trim_start().strip_prefix("pub fn ")
                && let Some(name) = rest.split('(').next()
                && ["run", "cli", "spawn", "drain", "apply"]
                    .iter()
                    .any(|p| name.starts_with(p))
            {
                out.insert(format!("{name}("));
            }
        }
    }
    assert!(
        out.len() >= 12,
        "capture-marker derivation collapsed to {} markers — the harness sources moved or the \
         derivation rotted, and a gate would skip every test: {out:?}",
        out.len()
    );
    out
}

/// Whether `text` holds `marker` as a call: a marker that starts with a name matches only where no longer name ends in it (`x_run(` is not `run(`).
pub fn calls(text: &str, marker: &str) -> bool {
    let ident = |c: char| c.is_ascii_alphanumeric() || c == '_';
    let named = marker.chars().next().is_some_and(ident);
    text.match_indices(marker)
        .any(|(at, _)| !named || !text[..at].chars().next_back().is_some_and(ident))
}
