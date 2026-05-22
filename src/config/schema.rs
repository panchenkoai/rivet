//! **Layer: Config support** (JSON Schema generation, v0.7.3 P0.1/P0.2)
//!
//! Single source of truth for the JSON Schema that describes
//! `rivet.yaml`.  Used by:
//!
//! - `rivet schema config` — prints the schema to stdout so operators
//!   can drop it into their repo or pipe it to a file.
//! - `tests/schema_drift.rs` — pins the in-tree
//!   `schemas/rivet.schema.json` artifact to the value derived from the
//!   current binary's types, so a config field added without a schema
//!   refresh fails CI.
//!
//! The schema title carries the running binary's version so editor
//! tooling can show "Rivet 0.7.3 config" rather than a versionless
//! generic name.

use schemars::Schema;
use schemars::generate::SchemaSettings;

use crate::config::Config;

/// Generate the JSON Schema for the top-level [`Config`] struct.
///
/// Returns a [`Schema`] suitable for `serde_json::to_string_pretty`.
/// The schema embeds the binary's `CARGO_PKG_VERSION` in its `title`
/// so a single shipped schema artifact carries its provenance.
pub fn generate_config_schema() -> Schema {
    // Draft 2020-12 is what the YAML Language Server and VS Code's
    // RedHat YAML extension accept natively; `Draft07` works too but
    // loses oneOf/anyOf composition niceties that the schemars derive
    // emits for some of our enums.
    let settings = SchemaSettings::draft2020_12();
    let generator = settings.into_generator();
    let mut schema = generator.into_root_schema_for::<Config>();
    // Tag the schema with the binary version so editors / CI can
    // detect drift between a checked-in artifact and the running
    // toolchain.
    schema.insert(
        "title".into(),
        serde_json::Value::String(format!(
            "Rivet config (rivet-cli {})",
            env!("CARGO_PKG_VERSION")
        )),
    );
    schema
}

/// Render the schema as a pretty-printed JSON string, terminated by a
/// trailing newline (POSIX text-file convention; matches what
/// `serde_json::to_writer_pretty` + `writeln!` produces and what the
/// schema-drift test expects in the checked-in artifact).
pub fn generate_config_schema_pretty() -> crate::error::Result<String> {
    let schema = generate_config_schema();
    let mut s = serde_json::to_string_pretty(&schema)?;
    if !s.ends_with('\n') {
        s.push('\n');
    }
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_carries_binary_version_in_title() {
        let schema = generate_config_schema();
        let title = schema
            .as_object()
            .and_then(|m| m.get("title"))
            .and_then(|v| v.as_str())
            .expect("schema must have a title");
        assert!(
            title.contains(env!("CARGO_PKG_VERSION")),
            "title must embed CARGO_PKG_VERSION ({}): {title}",
            env!("CARGO_PKG_VERSION"),
        );
    }

    #[test]
    fn schema_is_valid_json_object() {
        let pretty = generate_config_schema_pretty().expect("schema must serialize");
        // Round-trip through serde_json::Value to prove the rendered
        // bytes parse back as a JSON object (sanity check for any
        // future custom Display/Serialize change on Schema).
        let v: serde_json::Value =
            serde_json::from_str(&pretty).expect("rendered schema must parse as JSON");
        assert!(v.is_object(), "schema root must be a JSON object");
    }

    #[test]
    fn schema_ends_with_newline() {
        let pretty = generate_config_schema_pretty().unwrap();
        assert!(
            pretty.ends_with('\n'),
            "schema artifact must end with a trailing newline so POSIX tools / diffs behave",
        );
    }

    #[test]
    fn schema_mentions_top_level_required_fields() {
        // Smoke check that the derive surfaces the Config root's
        // required keys.  If the Config struct gains/loses a required
        // field, this test fails before the in-tree schema file goes
        // stale.
        let pretty = generate_config_schema_pretty().unwrap();
        assert!(pretty.contains("\"source\""), "must include 'source' field");
        assert!(
            pretty.contains("\"exports\""),
            "must include 'exports' field"
        );
    }
}
