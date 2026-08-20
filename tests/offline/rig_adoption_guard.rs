//! The Rig exists because ~250 hand-rolled YAML templates and ~240 inline
//! `Command::new(RIVET_BIN)` sites drifted apart; it re-converged them. But
//! nothing STOPPED the drift from re-accumulating — new tests kept minting
//! bespoke config builders and raw binary invocations one file at a time,
//! because no gate ever asked. This is that gate: a shrink-only ratchet over
//! every live file's count of bespoke sites (raw `Command::new(RIVET_BIN)` +
//! `write_config(`), the same ledger shape as `docs/mutants-baseline.txt`.
//!
//! The ratchet does NOT demand migration — some files are legitimately bespoke
//! (`rivet init` produces configs, so a config-owning rig is the wrong tool;
//! signal tests kill a live child). It demands that bespokeness never GROWS
//! silently: a new bespoke site either migrates to a `Rig::*` affordance, or
//! raises its file's ceiling here in a reviewed diff that says why.

use std::collections::BTreeMap;

/// (file, bespoke-site ceiling) — regenerate a line by counting
/// `Command::new(RIVET_BIN)` + `write_config(` in the file. Shrink freely;
/// grow only with a reason in the PR.
const BASELINE: &[(&str, usize)] = &[
    // ── the init-subject class: REASONED ceilings, not migration targets ──
    // `rivet init` PRODUCES configs; the rig OWNS a config. A rig-shaped init
    // test would test the rig's YAML against init's YAML — two generators, no
    // subject. Raw invocations of `rivet init <flags>` ARE these files' subject,
    // so their ceilings hold them at today's counts rather than at zero.
    ("audit_init_deferred.rs", 4),
    ("audit_maxfile.rs", 2),
    // audit_metrics_validates_config_path's SUBJECT is a nonexistent --config
    // path — a rig owns a real config, so that one raw invocation is the test.
    ("audit_observability.rs", 1),
    ("audit_state.rs", 1), // missing-config-path IS the subject
    ("live_cdc.rs", 33),
    ("live_cli_flags.rs", 76),
    ("live_init_extended.rs", 6),
];

fn bespoke_sites(path: &std::path::Path) -> usize {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    text.matches("Command::new(RIVET_BIN)").count() + text.matches("write_config(").count()
}

#[test]
fn bespoke_runner_sites_never_grow_per_file() {
    let baseline: BTreeMap<&str, usize> = BASELINE.iter().copied().collect();
    let mut over: Vec<String> = Vec::new();
    let mut unknown: Vec<String> = Vec::new();
    for e in std::fs::read_dir("tests/live")
        .expect("read tests/live")
        .flatten()
    {
        let p = e.path();
        if p.extension().is_none_or(|x| x != "rs") {
            continue;
        }
        let name = p.file_name().unwrap().to_string_lossy().to_string();
        let n = bespoke_sites(&p);
        match baseline.get(name.as_str()) {
            Some(&cap) if n > cap => over.push(format!("{name}: {n} > {cap}")),
            None if n > 0 => unknown.push(format!("{name}: {n}")),
            _ => {}
        }
    }
    assert!(
        over.is_empty() && unknown.is_empty(),
        "bespoke rivet-runner sites grew past the ratchet: over {over:?}; new files with \
         bespoke sites {unknown:?}. Use a `Rig::*` constructor (`Rig::cli` for non-run \
         subcommands, `spawn_args_env` for a killable child, `also_export` for \
         multi-export configs), or raise this file's ceiling in a reviewed diff that \
         says why the rig cannot express the case."
    );
}

#[test]
fn the_baseline_matches_reality_downward_too() {
    // A shrink-only ratchet must RECORD its wins, or the slack it leaves is a
    // future silent growth allowance exactly as wide as every past migration.
    let baseline: BTreeMap<&str, usize> = BASELINE.iter().copied().collect();
    let mut slack: Vec<String> = Vec::new();
    for (name, &cap) in &baseline {
        let p = std::path::Path::new("tests/live").join(name);
        if !p.exists() {
            slack.push(format!("{name}: listed but deleted — drop the entry"));
            continue;
        }
        let n = bespoke_sites(&p);
        if n < cap {
            slack.push(format!("{name}: {n} actual < {cap} ceiling — lower it"));
        }
    }
    assert!(
        slack.is_empty(),
        "the ratchet has slack; tighten it so migrations stay banked: {slack:?}"
    );
}
