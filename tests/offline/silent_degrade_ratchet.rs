//! A value a decoder cannot read must be refused, never written as NULL or a default.
//! Every shape below turns "could not read it" into "there was nothing there"; the
//! count under `src/source/` may only shrink.

/// Occurrences under `src/source/` on 2026-09-29, when the ratchet landed.
const CEILING: usize = 55;

/// The shapes that swap an unreadable value for NULL or nothing.
const SHAPES: &[&str] = &[
    "=> RivetValue::Null",
    "map_or(RivetValue::Null",
    "unwrap_or(RivetValue::Null)",
    "_ => b.append_null()",
    ".ok().flatten()",
];

fn rust_files(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    for e in std::fs::read_dir(dir).expect("read src/source").flatten() {
        let p = e.path();
        if p.is_dir() {
            rust_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

#[test]
fn silent_degrade_shapes_only_shrink() {
    let mut files = Vec::new();
    rust_files(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/source"),
        &mut files,
    );
    let n: usize = files
        .iter()
        .map(|f| {
            let text = std::fs::read_to_string(f).unwrap();
            SHAPES
                .iter()
                .map(|s| text.matches(s).count())
                .sum::<usize>()
        })
        .sum();
    assert!(
        n <= CEILING,
        "{n} silent-degrade shapes under src/source, ceiling {CEILING}: a value the decoder \
         cannot read must be refused (rivet_bail! with SOURCE_VALUE_UNREPRESENTABLE or \
         SOURCE_CDC_CELL_UNSUPPORTED), not written as NULL. A genuine SQL NULL arm is fine: \
         write it as an explicit NULL match instead of a catch-all."
    );
    assert!(
        n >= CEILING,
        "{n} silent-degrade shapes, below the ceiling {CEILING}: lower CEILING to {n} to bank it"
    );
}
