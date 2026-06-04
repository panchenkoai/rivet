//! Offline guard: every `examples/*.yaml` template must parse and pass config
//! validation against the current grammar. Runs without a database or network,
//! so it gates in CI — replacing the stale manual `rivet doctor --no-network`
//! line in the release checklist (doctor has no such flag and always connects).

use std::path::Path;

#[test]
fn all_example_configs_parse_and_validate() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples");
    let mut checked = Vec::new();
    let mut failures = Vec::new();

    for entry in std::fs::read_dir(&dir).expect("read examples/ dir") {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("yaml") {
            continue;
        }
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        let yaml = std::fs::read_to_string(&path).expect("read example yaml");
        // `from_yaml` = serde parse + business-rule validation, both offline.
        match rivet::config::Config::from_yaml(&yaml) {
            Ok(_) => checked.push(name),
            Err(e) => failures.push(format!("  {name}: {e:#}")),
        }
    }

    assert!(
        !checked.is_empty(),
        "no example configs found under {}",
        dir.display()
    );
    assert!(
        failures.is_empty(),
        "example config(s) failed to parse/validate:\n{}",
        failures.join("\n")
    );
}
