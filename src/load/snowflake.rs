//! Snowflake loader.
//!
//! Loads Rivet Parquet from GCS into a native-typed Snowflake table via a
//! `COPY INTO` off an external stage (a GCS `STORAGE INTEGRATION`). Unlike
//! BigQuery — where `LOAD DATA` with a declared schema coerces the Parquet for
//! free — Snowflake:
//!   * bills warehouse compute for the `COPY` (there is no free load), and
//!   * does NOT parse a Parquet JSON string into a navigable `VARIANT`: a plain
//!     `COPY` lands it as a `VARIANT`-wrapped string (`meta:key` → NULL). So a
//!     `VARIANT` column is loaded through a `PARSE_JSON($1:col)` transform in
//!     the same `COPY` (one billed pass), which yields a navigable `OBJECT`.
//!
//! Both facts were verified live before this was written.
//!
//! Cost attribution rides a `QUERY_TAG` (Snowflake's analogue of BigQuery job
//! labels): the tag shows up in `ACCOUNT_USAGE.QUERY_HISTORY`, so per-`rivet_op`
//! warehouse credits can be summed after the fact.

use super::TargetLoader;
use crate::types::target::TargetColumnSpec;
use anyhow::{Context, Result, bail};
use std::process::Command;

/// Loads Rivet Parquet from GCS into Snowflake.
#[derive(Debug, Default, Clone)]
pub struct SnowflakeLoader {
    /// The `snow` CLI connection name (e.g. `rivet`).
    pub connection: String,
    pub warehouse: String,
    pub database: String,
    pub schema: String,
    /// A pre-created GCS `STORAGE INTEGRATION` (grants Snowflake read on the
    /// bucket). The external stage is created per-load using it.
    pub storage_integration: String,
    /// `gcs://bucket/prefix/` — the external stage's URL (Snowflake wants the
    /// `gcs://` scheme, not `gs://`).
    pub gcs_url: String,
    /// Clustering key — column(s)/expression(s) for `CLUSTER BY`, enabling
    /// background auto-clustering. Empty = no clustering key. Applies only at
    /// table creation.
    pub cluster_by: Vec<String>,
    /// Absolute path to the connection's private key. The `snow` CLI does not
    /// expand `~`, so a `~`-relative `private_key_path` in the connection file
    /// must be overridden with an absolute path via env.
    pub private_key_path: Option<String>,
    /// Load-run correlation id, emitted in the `QUERY_TAG` JSON as `rivet_run`
    /// so every statement of one `rivet load` invocation shares a run key —
    /// cost slices per run (across tables) as well as per table. `None` omits it.
    pub run_id: Option<String>,
}

impl SnowflakeLoader {
    pub fn new(connection: impl Into<String>) -> Self {
        Self {
            connection: connection.into(),
            ..Default::default()
        }
    }

    /// Fully-qualified `db.schema.table`. Identifiers are passed **unquoted**:
    /// the warehouse/database/schema are pre-existing objects, and quoting a
    /// lowercase name would miss an unquoted-created (upper-cased) object. The
    /// tradeoff is that a reserved-word / special-char column is not handled —
    /// a hardening TODO once a real source needs it.
    fn fqtn(&self, table: &str) -> String {
        format!("{}.{}.{}", self.database, self.schema, table)
    }

    /// `  id NUMBER(38,0),\n  meta VARIANT` — the native column DDL.
    fn build_schema_ddl(specs: &[TargetColumnSpec]) -> String {
        specs
            .iter()
            .map(|s| format!("  {} {}", s.column_name, s.target_type))
            .collect::<Vec<_>>()
            .join(",\n")
    }

    /// The `COPY` transform projection: a `VARIANT` column is parsed with
    /// `PARSE_JSON` (a plain `COPY` would leave it a string), everything else is
    /// passed through `$1:col` (the path key preserves the Parquet field case)
    /// and coerced by the target column's type.
    fn build_copy_select(specs: &[TargetColumnSpec]) -> String {
        specs
            .iter()
            .map(|s| {
                let path = format!("$1:{}", s.column_name);
                if needs_parse_json(s) {
                    format!("PARSE_JSON({path})")
                } else {
                    path
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// `id, meta` — the explicit column list the transform loads into.
    fn build_column_list(specs: &[TargetColumnSpec]) -> String {
        specs
            .iter()
            .map(|s| s.column_name.clone())
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// ` CLUSTER BY (created, region)` — the clustering-key clause (empty when
    /// no key). Snowflake wraps the key in parentheses (unlike BigQuery).
    fn cluster_clause(cluster_by: &[String]) -> String {
        if cluster_by.is_empty() {
            String::new()
        } else {
            format!(" CLUSTER BY ({})", cluster_by.join(", "))
        }
    }

    /// A JSON query tag for post-hoc cost attribution in `QUERY_HISTORY`.
    /// Carries `rivet_run` too when a load-run id is set, so credits summed from
    /// `QUERY_ATTRIBUTION_HISTORY` slice per run as well as per table.
    fn query_tag(&self, table: &str) -> String {
        let run = self
            .run_id
            .as_deref()
            .map(|r| format!(r#","rivet_run":"{}""#, sanitize_tag(r)))
            .unwrap_or_default();
        format!(
            r#"{{"managed_by":"rivet","rivet_op":"load","rivet_table":"{}"{run}}}"#,
            sanitize_tag(table)
        )
    }

    /// Run a SQL script through `snow sql`, returning parsed JSON blocks.
    fn run_snow(&self, sql: &str) -> Result<serde_json::Value> {
        let mut cmd = Command::new("snow");
        cmd.args(["sql", "-c", &self.connection, "--format", "json", "-q", sql]);
        if let Some(key) = &self.private_key_path {
            // snow reads SNOWFLAKE_CONNECTIONS_<CONN>_PRIVATE_KEY_PATH.
            let env_key = format!(
                "SNOWFLAKE_CONNECTIONS_{}_PRIVATE_KEY_PATH",
                self.connection.to_uppercase()
            );
            cmd.env(env_key, key);
        }
        let out = cmd
            .output()
            .context("running `snow sql` — is the Snowflake CLI installed?")?;
        if !out.status.success() {
            bail!(
                "snow sql failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }
        serde_json::from_slice(&out.stdout).with_context(|| {
            format!(
                "parsing snow sql JSON output: {}",
                String::from_utf8_lossy(&out.stdout)
            )
        })
    }
}

impl TargetLoader for SnowflakeLoader {
    fn fqtn(&self, table: &str) -> String {
        format!("{}.{}.{}", self.database, self.schema, table)
    }

    fn materialize(&self, table: &str, specs: &[TargetColumnSpec], uris: &[String]) -> Result<u64> {
        let fqtn = self.fqtn(table);
        let ddl = Self::build_schema_ddl(specs);
        let select = Self::build_copy_select(specs);
        let columns = Self::build_column_list(specs);
        // A per-load external stage over the export's GCS prefix; the COPY loads
        // exactly the driver-selected files (`FILES=(…)`), NOT every Parquet under
        // the prefix — so the mode-aware/ledger per-run selection is honored (a
        // `PATTERN` over the prefix would load stale runs and fail the count gate).
        let stage = format!("rivet_stage_{}", sanitize_tag(table));
        let files = copy_files_clause(&self.gcs_url, uris)?;
        let cluster = Self::cluster_clause(&self.cluster_by);

        // `CREATE OR REPLACE` (overwrite): storage is the source of truth. Pin
        // the session to UTC before the COPY — Snowflake otherwise stamps a
        // Parquet timestamp with the session offset, shifting a `timestamptz`.
        let sql = format!(
            "ALTER SESSION SET QUERY_TAG = '{tag}';\n\
             ALTER SESSION SET TIMEZONE = 'UTC';\n\
             USE WAREHOUSE {wh};\n\
             USE SCHEMA {db}.{sc};\n\
             CREATE FILE FORMAT IF NOT EXISTS rivet_pq TYPE=PARQUET BINARY_AS_TEXT=FALSE;\n\
             CREATE OR REPLACE STAGE {stage} URL='{url}' STORAGE_INTEGRATION={si} FILE_FORMAT=rivet_pq;\n\
             CREATE OR REPLACE TABLE {fqtn} (\n{ddl}\n){cluster};\n\
             COPY INTO {fqtn} ({columns})\n\
             \x20 FROM (SELECT {select} FROM @{stage})\n\
             \x20 FILE_FORMAT=(FORMAT_NAME=rivet_pq) {files};\n\
             SELECT COUNT(*) AS ROWS_ FROM {fqtn};",
            tag = self.query_tag(table),
            wh = self.warehouse,
            db = self.database,
            sc = self.schema,
            si = self.storage_integration,
            url = self.gcs_url,
        );

        let result = self.run_snow(&sql)?;
        // ponytail: rows via COUNT(*); can become the COPY's `rows_loaded`
        // (metadata) behind this seam, no driver change.
        extract_count(&result)
            .context("COPY ran but the row count could not be read from snow output")
    }

    fn append_changelog(
        &self,
        table: &str,
        specs: &[TargetColumnSpec],
        uris: &[String],
        pk: &[String],
    ) -> Result<u64> {
        use crate::load::cdc::Warehouse;
        // Full change-log schema: rivet's `__op`/`__pos`/`__seq` meta columns
        // (not reported by `rivet check`) ahead of the resolved data columns.
        let mut full = crate::load::cdc::meta_column_specs(Warehouse::Snowflake);
        full.extend(
            specs
                .iter()
                .filter(|s| !is_meta_column(&s.column_name))
                .cloned(),
        );

        let changes = format!("{table}__changes");
        let changes_fqtn = self.fqtn(&changes);
        let ddl = Self::build_schema_ddl(&full);
        let select = Self::build_copy_select(&full);
        let columns = Self::build_column_list(&full);
        let cluster = Self::cluster_clause(pk);
        let stage = format!("rivet_stage_{}", sanitize_tag(&changes));
        let files = copy_files_clause(&self.gcs_url, uris)?;

        // Ensure the log exists (clustered on PK), COUNT before, append via COPY,
        // COUNT after — the delta is what THIS load added; the driver gates it.
        let sql = format!(
            "ALTER SESSION SET QUERY_TAG = '{tag}';\n\
             ALTER SESSION SET TIMEZONE = 'UTC';\n\
             USE WAREHOUSE {wh};\n\
             USE SCHEMA {db}.{sc};\n\
             CREATE FILE FORMAT IF NOT EXISTS rivet_pq TYPE=PARQUET BINARY_AS_TEXT=FALSE;\n\
             CREATE OR REPLACE STAGE {stage} URL='{url}' STORAGE_INTEGRATION={si} FILE_FORMAT=rivet_pq;\n\
             CREATE TABLE IF NOT EXISTS {changes_fqtn} (\n{ddl}\n){cluster};\n\
             SELECT COUNT(*) AS BEFORE_ FROM {changes_fqtn};\n\
             COPY INTO {changes_fqtn} ({columns})\n\
             \x20 FROM (SELECT {select} FROM @{stage})\n\
             \x20 FILE_FORMAT=(FORMAT_NAME=rivet_pq) {files};\n\
             SELECT COUNT(*) AS AFTER_ FROM {changes_fqtn};",
            tag = self.query_tag(&changes),
            wh = self.warehouse,
            db = self.database,
            sc = self.schema,
            si = self.storage_integration,
            url = self.gcs_url,
        );

        let result = self.run_snow(&sql)?;
        let before = extract_named(&result, "BEFORE_")
            .context("CDC load ran but the pre-append count (BEFORE_) could not be read")?;
        let after = extract_named(&result, "AFTER_")
            .context("CDC load ran but the post-append count (AFTER_) could not be read")?;
        Ok(after.saturating_sub(before))
    }

    fn warehouse(&self) -> crate::load::cdc::Warehouse {
        crate::load::cdc::Warehouse::Snowflake
    }

    fn create_view(&self, table: &str, view_sql: &str) -> Result<()> {
        // Fully-qualified DDL; a QUERY_TAG keeps it cost-attributable. CREATE VIEW
        // is metadata — no warehouse compute needed.
        let sql = format!(
            "ALTER SESSION SET QUERY_TAG = '{tag}';\n{view_sql}",
            tag = self.query_tag(table),
        );
        self.run_snow(&sql)?;
        Ok(())
    }
}

/// Whether a column name is one of rivet's CDC meta columns.
fn is_meta_column(name: &str) -> bool {
    matches!(name, "__op" | "__pos" | "__seq")
}

/// A column is loaded through `PARSE_JSON` iff its native type is `VARIANT`
/// (Rivet's Snowflake resolver maps JSON → `VARIANT`); a plain `COPY` would
/// leave it a string.
fn needs_parse_json(spec: &TargetColumnSpec) -> bool {
    spec.target_type.eq_ignore_ascii_case("VARIANT")
}

/// Keep a table name safe for a stage name / query tag (alnum + underscore).
fn sanitize_tag(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

/// Build the COPY `FILES=('a.parquet', 'b/c.parquet', …)` clause from the
/// driver-selected `uris`, each made relative to the stage URL (`gcs_url`). This
/// is what makes Snowflake honor the mode-aware/ledger per-run selection instead
/// of loading every Parquet under the prefix — a `PATTERN` over the prefix would
/// re-load stale/already-loaded runs and fail the count gate.
fn copy_files_clause(gcs_url: &str, uris: &[String]) -> Result<String> {
    if uris.is_empty() {
        bail!("Snowflake COPY: no Parquet URIs selected to load");
    }
    // Snowflake caps an explicit FILES=() list at 1000 entries; a normal run is a
    // handful. Batching past that is a follow-up — fail loud rather than silently
    // fall back to a whole-prefix PATTERN (the bug this fix closes).
    if uris.len() > 1000 {
        bail!(
            "Snowflake COPY FILES=() caps at 1000 files, got {} — batch the load or reduce parallelism",
            uris.len()
        );
    }
    let base = gcs_url.trim_end_matches('/');
    let files = uris
        .iter()
        .map(|u| {
            let rel = u.strip_prefix(base).unwrap_or(u).trim_start_matches('/');
            format!("'{rel}'")
        })
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!("FILES=({files})"))
}

/// Pull the `ROWS_` count out of snow's JSON (array of statement result blocks).
fn extract_count(value: &serde_json::Value) -> Option<u64> {
    extract_named(value, "ROWS_")
}

/// Pull a named integer column (e.g. `BEFORE_` / `AFTER_` / `ROWS_`) out of
/// snow's JSON — an array of statement result blocks, each an array of row
/// objects. Returns the first block carrying the key.
fn extract_named(value: &serde_json::Value, key: &str) -> Option<u64> {
    let blocks = value.as_array()?;
    for block in blocks {
        if let Some(rows) = block.as_array() {
            for row in rows {
                if let Some(n) = row.get(key).and_then(|v| v.as_u64()) {
                    return Some(n);
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::target::TargetStatus;

    fn spec(name: &str, ty: &str) -> TargetColumnSpec {
        TargetColumnSpec {
            column_name: name.to_string(),
            target_type: ty.to_string(),
            autoload_type: String::new(),
            status: TargetStatus::Ok,
            note: None,
            cast_sql: None,
        }
    }

    #[test]
    fn copy_files_clause_lists_the_selected_files_relative_to_the_stage() {
        // The COPY must name exactly the driver-selected files (not a whole-prefix
        // PATTERN), each relative to the stage URL — so Snowflake honors the
        // mode-aware/ledger per-run selection.
        let clause = copy_files_clause(
            "gs://bucket/exports/orders/",
            &[
                "gs://bucket/exports/orders/part-0.parquet".to_string(),
                "gs://bucket/exports/orders/snapshot/part-1.parquet".to_string(),
            ],
        )
        .unwrap();
        assert_eq!(
            clause, "FILES=('part-0.parquet', 'snapshot/part-1.parquet')",
            "each uri stripped to a stage-relative path; no PATTERN"
        );
        assert!(!clause.contains("PATTERN"));
        // A stage URL without a trailing slash still strips cleanly.
        assert_eq!(
            copy_files_clause("gs://b/p", &["gs://b/p/f.parquet".to_string()]).unwrap(),
            "FILES=('f.parquet')"
        );
        // Empty selection and the 1000-file cap both bail (never a silent
        // whole-prefix fallback).
        assert!(copy_files_clause("gs://b/p/", &[]).is_err());
        let many: Vec<String> = (0..1001)
            .map(|i| format!("gs://b/p/f{i}.parquet"))
            .collect();
        assert!(copy_files_clause("gs://b/p/", &many).is_err());
    }

    #[test]
    fn variant_columns_are_parsed_scalars_pass_through() {
        let specs = [
            spec("id", "NUMBER(38,0)"),
            spec("meta", "VARIANT"),
            spec("created", "DATE"),
        ];
        let sel = SnowflakeLoader::build_copy_select(&specs);
        assert_eq!(sel, "$1:id, PARSE_JSON($1:meta), $1:created");
    }

    #[test]
    fn schema_ddl_and_column_list_are_unquoted() {
        let specs = [spec("id", "NUMBER(38,0)"), spec("meta", "VARIANT")];
        assert_eq!(
            SnowflakeLoader::build_schema_ddl(&specs),
            "  id NUMBER(38,0),\n  meta VARIANT"
        );
        assert_eq!(SnowflakeLoader::build_column_list(&specs), "id, meta");
    }

    #[test]
    fn cluster_clause_wraps_key_in_parens_and_is_empty_when_unset() {
        assert_eq!(SnowflakeLoader::cluster_clause(&[]), "");
        assert_eq!(
            SnowflakeLoader::cluster_clause(&["created".to_string(), "customer".to_string()]),
            " CLUSTER BY (created, customer)"
        );
    }

    #[test]
    fn is_meta_column_matches_only_cdc_meta() {
        assert!(is_meta_column("__op"));
        assert!(is_meta_column("__pos"));
        assert!(is_meta_column("__seq"));
        assert!(!is_meta_column("id"));
        assert!(!is_meta_column("__other"));
    }

    #[test]
    fn fqtn_qualifies_database_schema_table() {
        let mut l = SnowflakeLoader::new("c");
        l.database = "DB".into();
        l.schema = "SC".into();
        assert_eq!(l.fqtn("orders"), "DB.SC.orders");
    }

    #[test]
    fn query_tag_carries_run_id_when_set_and_omits_it_otherwise() {
        let mut l = SnowflakeLoader::new("c");
        assert_eq!(
            l.query_tag("Orders"),
            r#"{"managed_by":"rivet","rivet_op":"load","rivet_table":"Orders"}"#
        );
        // Non-alphanumerics in the id are coerced to `_` so QUERY_TAG stays
        // valid JSON. The generated run id is pure hex, so this only bites a
        // user-supplied `--run-id` with punctuation.
        l.run_id = Some("r-7".to_string());
        assert_eq!(
            l.query_tag("Orders"),
            r#"{"managed_by":"rivet","rivet_op":"load","rivet_table":"Orders","rivet_run":"r_7"}"#
        );
    }

    #[test]
    fn count_is_extracted_from_snow_json() {
        let v = serde_json::json!([
            [{"status": "ok"}],
            [{"ROWS_": 50}]
        ]);
        assert_eq!(extract_count(&v), Some(50));
    }

    #[test]
    fn cdc_before_and_after_counts_are_extracted_by_name() {
        // The CDC script emits BEFORE_ and AFTER_ counts in separate blocks.
        let v = serde_json::json!([
            [{"status": "Statement executed successfully."}],
            [{"BEFORE_": 10}],
            [{"status": "rows loaded"}],
            [{"AFTER_": 35}]
        ]);
        assert_eq!(extract_named(&v, "BEFORE_"), Some(10));
        assert_eq!(extract_named(&v, "AFTER_"), Some(35));
        assert_eq!(extract_named(&v, "MISSING_"), None);
    }
}
