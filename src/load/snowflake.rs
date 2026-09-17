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
    /// The `load.partition` mapped to a leading clustering expression
    /// (`DATE_TRUNC('DAY', c)`), ahead of `cluster_by` — Snowflake has no partitions.
    pub partition_expr: Option<String>,
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

    /// The clustering key: the partition expression first, then `cluster_by`.
    fn cluster_keys(&self) -> Vec<String> {
        self.partition_expr
            .iter()
            .chain(self.cluster_by.iter())
            .cloned()
            .collect()
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
        // The shared gate accepts any-script letters because BigQuery
        // back-quotes column names. THIS driver splices them BARE (see `fqtn`
        // and `build_schema_ddl`), and Snowflake upper-cases what it parses
        // unquoted — so a non-ASCII name would be created as one object and
        // referenced as another. Refuse it here, narrowly, naming the remedy,
        // rather than widen the shared gate into a promise this leg cannot keep.
        for s in specs {
            if !crate::load::is_plain_ascii_ident(&s.column_name) {
                anyhow::bail!(
                    "Snowflake load `{table}`: column `{}` is not a plain ASCII identifier. \
                     This driver splices column names UNQUOTED (quoting a lowercase name \
                     would miss the unquoted-created, upper-cased object), so it cannot \
                     carry that name — the BigQuery leg can. Alias the column in the export \
                     query, or rename it at the source.",
                    s.column_name.escape_default()
                );
            }
        }
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
        let cluster = Self::cluster_clause(&self.cluster_keys());

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
        _pk: &[String],
    ) -> Result<u64> {
        let sql = self.build_append_changelog_sql(table, specs, uris)?;
        let result = self.run_snow(&sql)?;
        let before = extract_named(&result, "BEFORE_")
            .context("CDC load ran but the pre-append count (BEFORE_) could not be read")?;
        let after = extract_named(&result, "AFTER_")
            .context("CDC load ran but the post-append count (AFTER_) could not be read")?;
        Ok(after.saturating_sub(before))
    }

    fn changes_has_prior_changes(&self, table: &str) -> Result<bool> {
        let changes_fqtn = self.fqtn(&format!("{table}__changes"));
        let tag = sanitize_tag(table);
        let sql = format!(
            "ALTER SESSION SET QUERY_TAG = 'rivet_probe_{tag}';\n\
             SELECT COUNT(*) AS PROBE_ FROM (SELECT 1 FROM {changes_fqtn} \
             WHERE __op IS NOT NULL LIMIT 1);"
        );
        match self.run_snow(&sql) {
            // FAIL-CLOSED (round-8): a `snow` JSON-shape drift must not read
            // as "first cycle" and silently disarm the refusal — the exact
            // direction this guard exists for. BigQuery's scalar path already
            // errors; parity.
            Ok(v) => extract_named(&v, "PROBE_")
                .map(|n| n > 0)
                .context("re-baseline probe ran but PROBE_ was not in snow's output"),
            // A missing __changes table is the FIRST cycle, not an error.
            // Snowflake merges "does not exist" with "not authorized" in ONE
            // message, so a SELECT-denied role reads first-cycle here — the
            // residual is SELF-LIMITING (round-9): append_changelog's own
            // BEFORE_/AFTER_ COUNT gates need SELECT, so that role fails the
            // load loudly one statement later, never a silent doomed append.
            Err(e) if format!("{e:#}").contains("does not exist") => Ok(false),
            Err(e) => Err(e),
        }
    }

    fn object_kind(&self, table: &str) -> Result<super::ObjectKind> {
        let v = self.run_snow(&self.build_object_kind_sql(table))?;
        super::ObjectKind::from_probe(
            extract_named(&v, "KIND_")
                .context("object-kind probe ran but KIND_ was not in snow's output")?,
        )
    }

    fn column_overlap(&self, table: &str, names: &[&str]) -> Result<(u64, u64)> {
        let v = self.run_snow(&self.build_column_overlap_sql(table, names))?;
        let total = extract_named(&v, "TOTAL_")
            .context("column probe ran but TOTAL_ was not in snow's output")?;
        let matched = extract_named(&v, "MATCHED_")
            .context("column probe ran but MATCHED_ was not in snow's output")?;
        Ok((total, matched))
    }

    fn row_count(&self, table: &str) -> Result<u64> {
        let sql = format!(
            "ALTER SESSION SET QUERY_TAG = '{tag}';\n\
             USE WAREHOUSE {wh};\n\
             SELECT COUNT(*) AS N_ FROM {fqtn};",
            tag = self.query_tag(table),
            wh = self.warehouse,
            fqtn = self.fqtn(table),
        );
        extract_named(&self.run_snow(&sql)?, "N_")
            .context("row count ran but N_ was not in snow's output")
    }

    fn adopt_as_changelog(&self, table: &str) -> Result<()> {
        self.run_snow(&self.build_adoption_sql(table))?;
        Ok(())
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

impl SnowflakeLoader {
    /// Probe whose `KIND_` is `1·table + 2·view + 4·other` for `table`.
    fn build_object_kind_sql(&self, table: &str) -> String {
        format!(
            "ALTER SESSION SET QUERY_TAG = '{tag}';\n\
             USE WAREHOUSE {wh};\n\
             SELECT COUNT_IF(TABLE_TYPE = 'BASE TABLE') + 2 * COUNT_IF(TABLE_TYPE = 'VIEW') \
             + 4 * COUNT_IF(TABLE_TYPE NOT IN ('BASE TABLE', 'VIEW')) AS KIND_ \
             FROM {db}.INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_SCHEMA = UPPER('{sc}') AND TABLE_NAME = UPPER('{table}');",
            tag = self.query_tag(table),
            wh = self.warehouse,
            db = self.database,
            sc = self.schema,
        )
    }

    /// Probe returning the `TOTAL_` columns of `table` and how many are `MATCHED_` in `names`.
    fn build_column_overlap_sql(&self, table: &str, names: &[&str]) -> String {
        let list = if names.is_empty() {
            "''".to_string()
        } else {
            names
                .iter()
                .map(|n| format!("'{}'", n.to_lowercase()))
                .collect::<Vec<_>>()
                .join(", ")
        };
        format!(
            "ALTER SESSION SET QUERY_TAG = '{tag}';\n\
             USE WAREHOUSE {wh};\n\
             SELECT COUNT(*) AS TOTAL_, COUNT_IF(LOWER(COLUMN_NAME) IN ({list})) AS MATCHED_ \
             FROM {db}.INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_SCHEMA = UPPER('{sc}') AND TABLE_NAME = UPPER('{table}');",
            tag = self.query_tag(table),
            wh = self.warehouse,
            db = self.database,
            sc = self.schema,
        )
    }

    /// Rename `table` to `<table>__changes` and add the meta columns (NULL on every row),
    /// keeping its rows and clustering.
    fn build_adoption_sql(&self, table: &str) -> String {
        let changes = self.fqtn(&format!("{table}__changes"));
        let meta = crate::load::cdc::meta_column_specs(crate::load::cdc::Warehouse::Snowflake);
        format!(
            "ALTER SESSION SET QUERY_TAG = '{tag}';\n\
             USE WAREHOUSE {wh};\n\
             ALTER TABLE {src} RENAME TO {changes};\n\
             {add}",
            tag = self.query_tag(table),
            wh = self.warehouse,
            src = self.fqtn(table),
            add = build_alter_add_columns_sql(&changes, &meta),
        )
    }

    /// The append script for one CDC load, separated from its EXECUTION so the
    /// schema-reconciliation step can be asserted without a Snowflake account.
    /// The reconciling `ALTER` shipped on BigQuery only, so "the builder exists"
    /// is not evidence that this adapter calls it — that is what the test pins.
    fn build_append_changelog_sql(
        &self,
        table: &str,
        specs: &[TargetColumnSpec],
        uris: &[String],
    ) -> Result<String> {
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
        let cluster = Self::cluster_clause(&self.cluster_keys());
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
             {alter}\
             SELECT COUNT(*) AS BEFORE_ FROM {changes_fqtn};\n\
             COPY INTO {changes_fqtn} ({columns})\n\
             \x20 FROM (SELECT {select} FROM @{stage})\n\
             \x20 FILE_FORMAT=(FORMAT_NAME=rivet_pq) FORCE=TRUE {files};\n\
             SELECT COUNT(*) AS AFTER_ FROM {changes_fqtn};",
            tag = self.query_tag(&changes),
            wh = self.warehouse,
            db = self.database,
            sc = self.schema,
            si = self.storage_integration,
            url = self.gcs_url,
            alter = build_alter_add_columns_sql(&changes_fqtn, &full),
        );
        Ok(sql)
    }
}

/// Snowflake has no partitions: a `partition:` column maps to a leading `DATE_TRUNC`
/// clustering expression; the BigQuery-only forms and options are refused (ADR-0034 D6).
pub(crate) fn partition_expr(
    export: &str,
    spec: &crate::load::plan::PartitionSpec,
    column_type: &dyn Fn(&str) -> Result<String>,
) -> Result<(crate::load::plan::PartitionKey, String)> {
    use crate::load::plan::{PartitionForm, PartitionKey};
    if spec.expiration_days.is_some() || spec.require_filter {
        bail!(
            "export `{export}`: Snowflake has no partition expiry or partition filter — drop \
             `expiration_days` / `require_filter` from `partition`"
        );
    }
    let PartitionForm::Column {
        column,
        granularity,
    } = &spec.form
    else {
        bail!(
            "export `{export}`: Snowflake partitions by a date column only (`column` + \
             `granularity`); it has no `range` or `ingestion` partitions"
        );
    };
    let t = column_type(column)?;
    if !(t.starts_with("DATE") || t.starts_with("TIMESTAMP")) {
        bail!(
            "export `{export}`: cannot partition on `{column}` ({t}); Snowflake clusters a DATE \
             or TIMESTAMP column by time"
        );
    }
    Ok((
        PartitionKey::Time {
            column: Some(column.clone()),
            granularity: *granularity,
        },
        format!("DATE_TRUNC('{}', {column})", granularity.as_sql()),
    ))
}

/// Whether a column name is one of rivet's CDC meta columns.
fn is_meta_column(name: &str) -> bool {
    crate::load::cdc::is_meta_column(name)
}

/// Bring an EXISTING change log's schema up to the declared one by ADDING what
/// is missing — never by replacing the table, which would impose rivet's schema
/// on history rivet does not own.
///
/// `CREATE TABLE IF NOT EXISTS` is a no-op on a table that already exists, so a
/// log rivet did not create — one an operator pointed rivet at — keeps whatever shape its previous
/// owner gave it, and a log that predates a new column keeps the old one. The
/// `COPY INTO … (<declared columns>)` below then names a column the table lacks
/// and Snowflake fails the whole load with `invalid identifier` — after the
/// extract has already been paid for. `_rivet_row_hash` made that concrete: it
/// is written at extraction and gained a load spec, so every pre-existing log
/// was suddenly one column short.
///
/// `ADD COLUMN IF NOT EXISTS` is the only safe verb: additive, idempotent,
/// metadata-only, and existing rows read NULL for the new column. Trailing
/// newline (not `Option`) so the caller interpolates it unconditionally; an
/// empty spec list yields an empty string rather than a bare `ALTER TABLE t ;`.
fn build_alter_add_columns_sql(fqtn: &str, specs: &[TargetColumnSpec]) -> String {
    if specs.is_empty() {
        return String::new();
    }
    let adds = specs
        .iter()
        // Bare identifiers, matching `build_schema_ddl` / `build_column_list`:
        // this loader creates its columns unquoted (Snowflake upper-cases them),
        // so a quoted `"col"` here would ADD a second, case-sensitive column
        // instead of matching the one the COPY names.
        .map(|s| {
            format!(
                "ADD COLUMN IF NOT EXISTS {} {}",
                s.column_name, s.target_type
            )
        })
        .collect::<Vec<_>>()
        .join(",\n  ");
    format!("ALTER TABLE {fqtn}\n  {adds};\n")
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

/// Strip either GCS URI scheme — `gcs://` (Snowflake's stage scheme) or `gs://`
/// (what the load driver hands us) — so a stage URL and a driver-selected URI
/// compare on bucket/key alone, not scheme. Without this the two never matched.
fn strip_gcs_scheme(s: &str) -> &str {
    s.strip_prefix("gcs://")
        .or_else(|| s.strip_prefix("gs://"))
        .unwrap_or(s)
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
    // Scheme-blind: the stage URL is `gcs://` (Snowflake's scheme) but the driver
    // hands `gs://` uris. Strip both before matching so FILES entries come out
    // stage-RELATIVE — else strip_prefix never matches, the FULL uri leaks into
    // FILES=(), and Snowflake resolves it relative to the stage → a doubled
    // `gcs://…/gs://…` path (the live-caught "file not found" bug).
    let base = strip_gcs_scheme(gcs_url).trim_end_matches('/');
    let files = uris
        .iter()
        .map(|u| {
            let stripped = strip_gcs_scheme(u);
            let rel = stripped
                .strip_prefix(base)
                .unwrap_or(stripped)
                .trim_start_matches('/');
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
        // PRODUCTION REALITY: the stage URL is `gcs://` (Snowflake's scheme) while
        // the driver hands `gs://` uris. FILES must still come out stage-RELATIVE
        // across the scheme gap — a scheme-blind strip_prefix leaks the FULL uri,
        // which Snowflake resolves relative to the stage → a doubled
        // `gcs://…/gs://…` path ("file not found", caught live). This test now
        // mixes the schemes so it reproduces that bug.
        let clause = copy_files_clause(
            "gcs://bucket/exports/orders/",
            &[
                "gs://bucket/exports/orders/part-0.parquet".to_string(),
                "gs://bucket/exports/orders/snapshot/part-1.parquet".to_string(),
            ],
        )
        .unwrap();
        assert_eq!(
            clause, "FILES=('part-0.parquet', 'snapshot/part-1.parquet')",
            "each uri stripped to a stage-relative path across schemes; no PATTERN"
        );
        assert!(!clause.contains("PATTERN"));
        assert!(
            !clause.contains("gs://") && !clause.contains("gcs://"),
            "no absolute URI may leak into FILES=() — that doubles the stage prefix"
        );
        // A `gcs://` stage URL without a trailing slash still strips a `gs://` uri.
        assert_eq!(
            copy_files_clause("gcs://b/p", &["gs://b/p/f.parquet".to_string()]).unwrap(),
            "FILES=('f.parquet')"
        );
        // Empty selection and the 1000-file cap both bail (never a silent
        // whole-prefix fallback).
        assert!(copy_files_clause("gcs://b/p/", &[]).is_err());
        let many: Vec<String> = (0..1001)
            .map(|i| format!("gs://b/p/f{i}.parquet"))
            .collect();
        assert!(copy_files_clause("gcs://b/p/", &many).is_err());
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
    fn a_partition_expression_leads_the_clustering_key() {
        let mut l = SnowflakeLoader::new("c");
        l.cluster_by = vec!["customer".into()];
        l.partition_expr = Some("DATE_TRUNC('DAY', created)".into());
        assert_eq!(
            SnowflakeLoader::cluster_clause(&l.cluster_keys()),
            " CLUSTER BY (DATE_TRUNC('DAY', created), customer)"
        );
        l.database = "DB".into();
        l.schema = "SC".into();
        let append = l
            .build_append_changelog_sql(
                "t",
                &[spec("id", "NUMBER")],
                &["gs://b/p/part-0.parquet".to_string()],
            )
            .unwrap();
        assert!(
            append.contains(
                "CREATE TABLE IF NOT EXISTS DB.SC.t__changes (\n  __op VARCHAR,\n  __pos VARCHAR,\n  __seq INTEGER,\n  id NUMBER\n) CLUSTER BY (DATE_TRUNC('DAY', created), customer);"
            ),
            "the change log rivet creates takes the same key:\n{append}"
        );
        l.cluster_by.clear();
        assert_eq!(
            SnowflakeLoader::cluster_clause(&l.cluster_keys()),
            " CLUSTER BY (DATE_TRUNC('DAY', created))"
        );
        l.partition_expr = None;
        assert_eq!(SnowflakeLoader::cluster_clause(&l.cluster_keys()), "");
    }

    #[test]
    fn cluster_clause_wraps_key_in_parens_and_is_empty_when_unset() {
        assert_eq!(SnowflakeLoader::cluster_clause(&[]), "");
        assert_eq!(
            SnowflakeLoader::cluster_clause(&["created".to_string(), "customer".to_string()]),
            " CLUSTER BY (created, customer)"
        );
    }

    /// An existing change log must be reconciled by ADDING what it lacks, never
    /// by a replace. `CREATE TABLE IF NOT EXISTS` is a no-op on a table that
    /// already exists, so without this the `COPY INTO … (<declared columns>)`
    /// names a column the table does not have and Snowflake fails the load with
    /// `invalid identifier` — after the extract was already paid for. This is
    /// the Snowflake twin of bigquery.rs's `build_alter_add_columns_sql`; the
    /// reconciliation lives in each adapter, so one adapter having it proves
    /// nothing about the other (it shipped on BigQuery alone).
    #[test]
    fn alter_add_columns_reconciles_an_existing_log_and_never_replaces_it() {
        let specs = vec![
            spec("__op", "VARCHAR"),
            spec(crate::enrich::COL_ROW_HASH, "NUMBER(38,0)"),
        ];
        let sql = build_alter_add_columns_sql("DB.SC.t__changes", &specs);
        assert!(
            sql.starts_with("ALTER TABLE DB.SC.t__changes"),
            "must ALTER the log in place; got: {sql}"
        );
        assert!(
            sql.contains("ADD COLUMN IF NOT EXISTS __op VARCHAR")
                && sql.contains("ADD COLUMN IF NOT EXISTS _rivet_row_hash NUMBER(38,0)"),
            "every declared column is added idempotently; got: {sql}"
        );
        assert!(
            !sql.to_uppercase().contains("REPLACE") && !sql.to_uppercase().contains("DROP"),
            "reconciliation must never replace or drop — the log holds history \
             rivet does not own; got: {sql}"
        );
        // Bare identifiers: the loader creates its columns unquoted, so a quoted
        // name would add a SECOND case-sensitive column the COPY never names.
        assert!(!sql.contains('"'), "identifiers stay bare; got: {sql}");
        // Empty spec list ⇒ no statement at all, not a bare `ALTER TABLE t ;`.
        assert_eq!(build_alter_add_columns_sql("DB.SC.t", &[]), "");
    }

    /// The reconciliation must actually be IN the append script — a correct
    /// builder that no caller invokes is the bug this release shipped on
    /// Snowflake. Asserted on the emitted SQL's ORDER: the ALTER has to sit
    /// after the CREATE (which is the no-op on an existing table) and before
    /// the COPY that names the columns.
    #[test]
    fn the_append_script_alters_between_the_create_and_the_copy() {
        let specs = vec![spec("id", "NUMBER"), spec("__op", "VARCHAR")];
        let mut l = SnowflakeLoader::new("c");
        l.database = "DB".into();
        l.schema = "SC".into();
        let sql = l
            .build_append_changelog_sql("t", &specs, &["gs://b/p/part-0.parquet".to_string()])
            .unwrap();
        let create = sql
            .find("CREATE TABLE IF NOT EXISTS")
            .expect("create present");
        let alter = sql.find("ALTER TABLE DB.SC.t__changes").expect(
            "the append script must reconcile an existing log — without this the COPY \
             names columns the table lacks and the load fails after the extract",
        );
        let copy = sql.find("COPY INTO").expect("copy present");
        assert!(
            create < alter && alter < copy,
            "order must be CREATE → ALTER → COPY; got:\n{sql}"
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

    fn adoption_loader() -> SnowflakeLoader {
        let mut l = SnowflakeLoader::new("rivet".to_string());
        l.warehouse = "WH".into();
        l.database = "DB".into();
        l.schema = "SC".into();
        l
    }

    fn col(name: &str, ty: &str) -> TargetColumnSpec {
        TargetColumnSpec {
            column_name: name.into(),
            target_type: ty.into(),
            autoload_type: String::new(),
            status: TargetStatus::Ok,
            note: None,
            cast_sql: None,
        }
    }

    #[test]
    fn object_kind_probe_reads_the_schema_catalog_with_uppercased_names() {
        let sql = adoption_loader().build_object_kind_sql("orders");
        assert!(sql.contains("USE WAREHOUSE WH;"), "{sql}");
        assert!(
            sql.contains(
                "FROM DB.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = UPPER('SC') \
                 AND TABLE_NAME = UPPER('orders')"
            ),
            "{sql}"
        );
        assert!(sql.contains("AS KIND_"), "{sql}");
    }

    #[test]
    fn column_overlap_probe_lowercases_the_export_names() {
        let sql = adoption_loader().build_column_overlap_sql("orders", &["Id", "amount"]);
        assert!(
            sql.contains("COUNT_IF(LOWER(COLUMN_NAME) IN ('id', 'amount')) AS MATCHED_"),
            "{sql}"
        );
        assert!(sql.contains("COUNT(*) AS TOTAL_"), "{sql}");
    }

    #[test]
    fn adoption_renames_the_table_and_adds_the_meta_columns() {
        let sql = adoption_loader().build_adoption_sql("orders");
        assert!(
            sql.contains("ALTER TABLE DB.SC.orders RENAME TO DB.SC.orders__changes;"),
            "{sql}"
        );
        assert!(sql.contains("ALTER TABLE DB.SC.orders__changes"), "{sql}");
        for col in ["__op VARCHAR", "__pos VARCHAR", "__seq INTEGER"] {
            assert!(sql.contains(col), "{sql}");
        }
        assert!(!sql.contains("SELECT"), "no query reads the table: {sql}");
    }

    #[test]
    #[ignore = "live: requires SNOWFLAKE_TEST_CONNECTION"]
    fn snowflake_live_adopts_a_full_load_table_as_the_changelog_baseline() {
        let Ok(connection) = std::env::var("SNOWFLAKE_TEST_CONNECTION") else {
            eprintln!("skipping: SNOWFLAKE_TEST_CONNECTION unset");
            return;
        };
        let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
        let mut loader = SnowflakeLoader::new(connection);
        loader.warehouse = env("RIVET_SF_TEST_WAREHOUSE", "RIVET_LOAD_TEST");
        loader.database = env("RIVET_SF_TEST_DATABASE", "RIVET_LOAD_TEST");
        loader.schema = env("RIVET_SF_TEST_SCHEMA", "PUBLIC");
        loader.private_key_path = std::env::var("RIVET_SNOWFLAKE_KEY").ok();
        let table = format!("RIVET_SF_LIVE_ADOPT_{}", std::process::id());
        let changes = format!("{table}__changes");
        let (fq, changes_fq) = (loader.fqtn(&table), loader.fqtn(&changes));

        loader
            .run_snow(&format!(
                "USE WAREHOUSE {};\nCREATE OR REPLACE TABLE {fq} AS \
                 SELECT column1 AS id, 'v' || column1 AS v FROM VALUES (1), (2), (3);",
                loader.warehouse
            ))
            .expect("fixture table");
        let specs = [col("id", "NUMBER"), col("v", "VARCHAR")];
        let adopted = crate::load::adopt_full_load_table(
            &loader,
            &table,
            &specs,
            crate::load::Ownership::Own,
        );
        let view_sql = crate::load::cdc::inc_dedup_view_sql(
            crate::load::cdc::Warehouse::Snowflake,
            &fq,
            &changes_fq,
            &["id"],
            "id",
        );
        let view = adopted
            .as_ref()
            .ok()
            .map(|_| loader.create_view(&table, &view_sql));
        let kind = loader.object_kind(&table);
        let copied = loader.row_count(&changes);
        let viewed = loader.row_count(&table);
        for drop in [
            format!("DROP VIEW IF EXISTS {fq};"),
            format!("DROP TABLE IF EXISTS {fq};"),
            format!("DROP TABLE IF EXISTS {changes_fq};"),
        ] {
            let _ = loader.run_snow(&drop);
        }

        assert_eq!(adopted.unwrap(), Some(3));
        view.unwrap().unwrap();
        assert_eq!(kind.unwrap(), crate::load::ObjectKind::View);
        assert_eq!(copied.unwrap(), 3);
        assert_eq!(viewed.unwrap(), 3);
    }
}
