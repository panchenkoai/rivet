//! Fuzz / smoke tests for YAML config parsing and placeholder resolution.
//!
//! QA backlog Task 4A.1. Lightweight deterministic adversarial inputs run
//! through `Config::from_yaml` and `resolve_vars`; the parser must never panic.
//! Each input either parses or returns an error — both are acceptable outcomes.
//! The panic-on-regression is the only failure mode.
//!
//! Real corpus-driven fuzzing (`cargo-fuzz` target with libfuzzer) is deferred;
//! this file covers the "no panic on pathological inputs" subset that runs
//! inside regular `cargo test`.

use rivet::config::{Config, resolve_vars};

#[test]
fn pathological_yaml_does_not_panic() {
    let inputs: &[&str] = &[
        "",
        "\n\n\n",
        "source: {}",
        "not yaml at all: : :",
        "source:\n  type: postgres\n  url: \"\"\nexports: []\n",
        // deeply nested placeholders
        "source:\n  type: postgres\n  url: \"postgresql://${${${${A}}}}\"\nexports: []\n",
        // huge scalar — a realistic "someone pasted a blob into YAML" case
        &format!(
            "source:\n  type: postgres\n  url: \"{}\"\nexports: []\n",
            "a".repeat(64 * 1024)
        ),
        // unicode / emoji in values
        "source:\n  type: postgres\n  url: \"postgresql://localhost/🚀\"\nexports: []\n",
        // CRLF line endings
        "source:\r\n  type: postgres\r\n  url: \"postgresql://localhost/test\"\r\nexports: []\r\n",
        // tabs where yaml expects spaces
        "source:\n\ttype: postgres\n\turl: \"postgresql://localhost/test\"\nexports: []\n",
    ];

    for yaml in inputs {
        // Harness auto-fails on panic; Ok/Err verdict is irrelevant here.
        let _ = Config::from_yaml(yaml);
    }
}

#[test]
fn pathological_placeholder_inputs_do_not_panic() {
    let inputs: &[&str] = &[
        "",
        "${",
        "${}",
        "${A",
        "${A}${B}${C}${D}",
        "prefix ${NO_CLOSE and ${OTHER} tail",
        "${",
        "${nested ${value}}",
        "${🚀}",
    ];
    let mut params = std::collections::HashMap::new();
    params.insert("A".into(), "a".into());
    params.insert("B".into(), "b".into());
    params.insert("C".into(), "c".into());
    params.insert("D".into(), "d".into());
    params.insert("OTHER".into(), "o".into());
    params.insert("nested ${value".into(), "?".into()); // keys are literal

    for s in inputs {
        let _ = resolve_vars(s, Some(&params));
    }
}
