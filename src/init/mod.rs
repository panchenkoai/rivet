mod artifact;
mod candidates;
mod mysql;
mod postgres;
mod yaml_scaffold;

pub(crate) use artifact::{
    ChunkCandidate, CursorCandidate, CursorCandidateReason, DiscoveryArtifact, TableDiscovery,
};

use crate::error::Result;

/// Column metadata fetched from information_schema.
#[derive(Debug, Clone)]
pub(crate) struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub is_primary_key: bool,
    /// `true` when the column can be NULL. Epic B / ADR-0007: used for coalesce cursor hints.
    pub is_nullable: bool,
    /// Filled for NUMERIC / DECIMAL columns from `information_schema.columns`.
    /// `None` when not applicable or when precision is unbounded.
    pub numeric_precision: Option<u32>,
    pub numeric_scale: Option<u32>,
}

/// Table metadata used to generate the config scaffold and discovery artifact.
#[derive(Debug, Clone)]
pub(crate) struct TableInfo {
    pub schema: String,
    pub table: String,
    pub row_estimate: i64,
    /// Approximate physical size in bytes (heap + indexes), if available.
    pub total_bytes: Option<i64>,
    pub columns: Vec<ColumnInfo>,
}

impl TableInfo {
    /// Best candidate for `chunk_column`: integer primary key, or first integer column.
    pub(crate) fn best_chunk_column(&self) -> Option<&str> {
        // Prefer integer PK
        self.columns
            .iter()
            .find(|c| c.is_primary_key && is_integer_type(&c.data_type))
            .or_else(|| {
                // Fall back to first integer column
                self.columns.iter().find(|c| is_integer_type(&c.data_type))
            })
            .map(|c| c.name.as_str())
    }

    /// Best candidate for `cursor_column`: prefer updated_at, then created_at, then any timestamp.
    pub(crate) fn best_cursor_column(&self) -> Option<&str> {
        let ts_cols: Vec<&ColumnInfo> = self
            .columns
            .iter()
            .filter(|c| is_timestamp_type(&c.data_type))
            .collect();
        ts_cols
            .iter()
            .find(|c| c.name == "updated_at" || c.name == "modified_at")
            .or_else(|| ts_cols.iter().find(|c| c.name == "created_at"))
            .or_else(|| ts_cols.first())
            .map(|c| c.name.as_str())
    }

    /// Suggest extraction mode based on row count and available columns.
    pub(crate) fn suggest_mode(&self) -> &'static str {
        if self.row_estimate > 100_000 {
            if self.best_chunk_column().is_some() {
                return "chunked";
            }
            if self.best_cursor_column().is_some() {
                return "incremental";
            }
        }
        "full"
    }

    /// Enumerate ranked cursor candidates with structured reasons.
    pub(crate) fn cursor_candidates(&self) -> Vec<CursorCandidate> {
        candidates::cursor_candidates(self)
    }

    /// Enumerate chunk candidates (integer-typed columns, PK preferred).
    pub(crate) fn chunk_candidates(&self) -> Vec<ChunkCandidate> {
        candidates::chunk_candidates(self)
    }
}

fn is_integer_type(t: &str) -> bool {
    let t = t.to_lowercase();
    matches!(
        t.as_str(),
        "int"
            | "integer"
            | "bigint"
            | "int4"
            | "int8"
            | "int2"
            | "smallint"
            | "serial"
            | "bigserial"
            | "smallserial"
            | "tinyint"
            | "mediumint"
    )
}

fn is_timestamp_type(t: &str) -> bool {
    let t = t.to_lowercase();
    t.contains("timestamp") || t == "datetime" || t == "date"
}

pub(super) fn source_type(source_url: &str) -> Result<&'static str> {
    if source_url.starts_with("postgres") || source_url.starts_with("postgresql") {
        Ok("postgres")
    } else if source_url.starts_with("mysql") {
        Ok("mysql")
    } else {
        anyhow::bail!(
            "Unsupported source URL scheme. Expected postgresql:// or mysql://, got: {}",
            source_url
        )
    }
}

/// Output format for `rivet init` (Epic B).
#[derive(Debug, Clone, Copy)]
pub enum InitFormat {
    /// YAML scaffold (default — backward compatible).
    Yaml,
    /// JSON discovery artifact for machine consumers (`rivet init --discover`).
    DiscoveryJson,
}

/// Optional cloud destination for YAML scaffolds (`--gcs-bucket` / `--s3-bucket` on `rivet init`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InitYamlDestination {
    pub gcs_bucket: Option<String>,
    /// If set, scaffold includes `credentials_file:`; if `None`, use ADC (no key line in YAML).
    pub gcs_credentials_file: Option<String>,
    pub s3_bucket: Option<String>,
    pub s3_region: Option<String>,
}

impl InitYamlDestination {
    pub fn validate(&self) -> Result<()> {
        if self.gcs_bucket.is_some() && self.s3_bucket.is_some() {
            anyhow::bail!("use at most one of --gcs-bucket and --s3-bucket");
        }
        Ok(())
    }
}

/// Entry point for `rivet init`.
///
/// With `--table`: introspect one table (optional `schema.table`) and emit one export.
/// Without `--table`: introspect every table/view in a PostgreSQL schema (default `public`)
/// or in a MySQL database (from `--schema` or from the URL path).
pub fn init(
    source_url: &str,
    table: Option<&str>,
    schema: Option<&str>,
    output: Option<&str>,
    format: InitFormat,
    yaml_destination: InitYamlDestination,
) -> Result<()> {
    yaml_destination.validate()?;
    let (text, yaml_decimal_review) = match format {
        InitFormat::Yaml => init_yaml(source_url, table, schema, &yaml_destination)?,
        InitFormat::DiscoveryJson => {
            // Defensive backstop for non-CLI callers; the `rivet init` CLI
            // already rejects `--discover` together with any cloud flag via
            // `conflicts_with_all` in main.rs.
            if yaml_destination != InitYamlDestination::default() {
                eprintln!(
                    "rivet: note: --gcs-bucket / --s3-bucket are ignored for --discover (JSON has no destination)"
                );
            }
            (init_discovery_json(source_url, table, schema)?, false)
        }
    };

    match output {
        Some(path) => {
            std::fs::write(path, &text)?;
            let label_written = match format {
                InitFormat::Yaml => "Config",
                InitFormat::DiscoveryJson => "Discovery artifact",
            };
            eprintln!("{label_written} written to {path}");
            if matches!(format, InitFormat::Yaml) && yaml_decimal_review {
                eprintln!(
                    "rivet: note: YAML uses default decimal(38,18) for column(s) with NUMERIC without (p,s) in the DDL — search for `{}` under columns: and fix before production.",
                    yaml_scaffold::INIT_DECIMAL_REVIEW_MARKER
                );
            }
        }
        None => print!("{text}"),
    }

    Ok(())
}

fn init_yaml(
    source_url: &str,
    table: Option<&str>,
    schema: Option<&str>,
    dest: &InitYamlDestination,
) -> Result<(String, bool)> {
    if let Some(t) = table {
        let (sch, table_name) = yaml_scaffold::parse_table(t);
        let info = match source_type(source_url)? {
            "postgres" => postgres::introspect(source_url, &sch, table_name)?,
            "mysql" => mysql::introspect(source_url, table_name)?,
            _ => unreachable!(),
        };
        let hint = yaml_scaffold::table_has_unbounded_decimal_columns(&info);
        let yaml = yaml_scaffold::generate_config(&info, source_url, dest)?;
        return Ok((yaml, hint));
    }
    let infos = introspect_all(source_url, schema)?;
    if infos.is_empty() {
        anyhow::bail!("No tables or views found (check --schema and privileges)");
    }
    let label = schema_scope_label(source_url, schema, infos.len())?;
    let hint = infos
        .iter()
        .any(yaml_scaffold::table_has_unbounded_decimal_columns);
    let yaml = yaml_scaffold::generate_schema_config(&infos, source_url, &label, dest)?;
    Ok((yaml, hint))
}

fn init_discovery_json(
    source_url: &str,
    table: Option<&str>,
    schema: Option<&str>,
) -> Result<String> {
    let (infos, scope) = if let Some(t) = table {
        let (sch, table_name) = yaml_scaffold::parse_table(t);
        let info = match source_type(source_url)? {
            "postgres" => postgres::introspect(source_url, &sch, table_name)?,
            "mysql" => mysql::introspect(source_url, table_name)?,
            _ => unreachable!(),
        };
        let scope = match source_type(source_url)? {
            "postgres" => format!("table \"{}\".\"{}\"", info.schema, info.table),
            "mysql" => format!("table `{}`", info.table),
            _ => unreachable!(),
        };
        (vec![info], scope)
    } else {
        let infos = introspect_all(source_url, schema)?;
        if infos.is_empty() {
            anyhow::bail!("No tables or views found (check --schema and privileges)");
        }
        let label = schema_scope_label(source_url, schema, infos.len())?;
        (infos, label)
    };

    let st = source_type(source_url)?;
    let tables: Vec<TableDiscovery> = infos.iter().map(table_discovery).collect();

    let artifact = DiscoveryArtifact {
        rivet_version: env!("CARGO_PKG_VERSION").to_string(),
        source_type: st.to_string(),
        scope,
        tables,
    };
    artifact.to_json_pretty()
}

fn table_discovery(info: &TableInfo) -> TableDiscovery {
    let cursor_candidates = info.cursor_candidates();
    let chunk_candidates = info.chunk_candidates();
    let fallback = candidates::suggest_cursor_fallback(info);

    let mut notes: Vec<String> = Vec::new();
    if fallback.is_some() {
        notes.push(
            "Primary cursor candidate is nullable; consider \
             `incremental_cursor_mode: coalesce` with the suggested fallback (ADR-0007)."
                .into(),
        );
    }
    if info.columns.is_empty() {
        notes.push("No columns visible — check SELECT privileges.".into());
    }

    TableDiscovery {
        schema: info.schema.clone(),
        table: info.table.clone(),
        row_estimate: info.row_estimate,
        total_bytes: info.total_bytes,
        suggested_mode: info.suggest_mode().to_string(),
        cursor_candidates,
        suggested_cursor_fallback_column: fallback,
        chunk_candidates,
        notes,
    }
}

fn introspect_all(source_url: &str, schema: Option<&str>) -> Result<Vec<TableInfo>> {
    match source_type(source_url)? {
        "postgres" => {
            let sch = schema
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("public");
            let names = postgres::list_tables(source_url, sch)?;
            let mut out = Vec::with_capacity(names.len());
            for n in names {
                match postgres::introspect(source_url, sch, &n) {
                    Ok(info) => out.push(info),
                    // Table may have been dropped between list_tables and introspect.
                    // Skip it rather than aborting the whole schema scan.
                    Err(e) if e.to_string().contains("not found or has no columns") => {}
                    Err(e) => return Err(e),
                }
            }
            Ok(out)
        }
        "mysql" => {
            let db = mysql::resolve_database_for_listing(source_url, schema)?;
            let names = mysql::list_tables(source_url, &db)?;
            let mut out = Vec::with_capacity(names.len());
            for n in names {
                out.push(mysql::introspect(source_url, &n)?);
            }
            Ok(out)
        }
        _ => unreachable!(),
    }
}

fn schema_scope_label(source_url: &str, schema: Option<&str>, n: usize) -> Result<String> {
    let kind = source_type(source_url)?;
    let obj = if n == 1 { "object" } else { "objects" };
    Ok(match kind {
        "postgres" => {
            let sch = schema
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("public");
            format!("PostgreSQL schema \"{sch}\" ({n} {obj})")
        }
        "mysql" => {
            let db = mysql::resolve_database_for_listing(source_url, schema)?;
            format!("MySQL database \"{db}\" ({n} {obj})")
        }
        _ => unreachable!(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, ty: &str, pk: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: ty.to_string(),
            is_primary_key: pk,
            is_nullable: false,
            numeric_precision: None,
            numeric_scale: None,
        }
    }

    fn make_table(rows: i64, cols: Vec<ColumnInfo>) -> TableInfo {
        TableInfo {
            schema: "public".to_string(),
            table: "orders".to_string(),
            row_estimate: rows,
            total_bytes: None,
            columns: cols,
        }
    }

    #[test]
    fn suggest_chunked_for_large_table_with_int_pk() {
        let info = make_table(
            5_000_000,
            vec![
                col("id", "bigint", true),
                col("created_at", "timestamp", false),
            ],
        );
        assert_eq!(info.suggest_mode(), "chunked");
        assert_eq!(info.best_chunk_column(), Some("id"));
    }

    #[test]
    fn suggest_incremental_when_no_int_pk() {
        let info = make_table(500_000, vec![col("updated_at", "timestamp", false)]);
        assert_eq!(info.suggest_mode(), "incremental");
        assert_eq!(info.best_cursor_column(), Some("updated_at"));
    }

    #[test]
    fn suggest_full_for_small_table() {
        let info = make_table(500, vec![col("id", "bigint", true)]);
        assert_eq!(info.suggest_mode(), "full");
    }

    #[test]
    fn cursor_column_prefers_updated_at() {
        let info = make_table(
            0,
            vec![
                col("created_at", "timestamp", false),
                col("updated_at", "timestamp", false),
            ],
        );
        assert_eq!(info.best_cursor_column(), Some("updated_at"));
    }

    #[test]
    fn parse_table_with_schema() {
        let (s, t) = yaml_scaffold::parse_table("my_schema.orders");
        assert_eq!(s, "my_schema");
        assert_eq!(t, "orders");
    }

    #[test]
    fn parse_table_without_schema_defaults_to_public() {
        let (s, t) = yaml_scaffold::parse_table("orders");
        assert_eq!(s, "public");
        assert_eq!(t, "orders");
    }

    #[test]
    fn generate_config_chunked_contains_key_fields() {
        let info = make_table(
            2_000_000,
            vec![col("id", "bigint", true), col("name", "text", false)],
        );
        let yaml = yaml_scaffold::generate_config(
            &info,
            "postgresql://localhost/db",
            &InitYamlDestination::default(),
        )
        .unwrap();
        assert!(yaml.contains("mode: chunked"), "got:\n{yaml}");
        assert!(yaml.contains("chunk_column: id"), "got:\n{yaml}");
        assert!(yaml.contains("chunk_checkpoint: true"), "got:\n{yaml}");
        assert!(yaml.contains("parallel: 2"), "got:\n{yaml}");
        assert!(yaml.contains("SELECT id, name"), "got:\n{yaml}");
        assert!(yaml.contains("meta_columns:"), "got:\n{yaml}");
        assert!(yaml.contains("exported_at: true"), "got:\n{yaml}");
        assert!(yaml.contains("row_hash: true"), "got:\n{yaml}");
    }

    #[test]
    fn generate_config_incremental_contains_cursor() {
        let info = make_table(200_000, vec![col("updated_at", "timestamp", false)]);
        let yaml = yaml_scaffold::generate_config(
            &info,
            "mysql://localhost/db",
            &InitYamlDestination::default(),
        )
        .unwrap();
        assert!(yaml.contains("mode: incremental"), "got:\n{yaml}");
        assert!(yaml.contains("cursor_column: updated_at"), "got:\n{yaml}");
        assert!(yaml.contains("row_hash: true"), "got:\n{yaml}");
    }

    #[test]
    fn generate_schema_config_emits_multiple_exports() {
        let a = TableInfo {
            schema: "public".to_string(),
            table: "users".to_string(),
            row_estimate: 100,
            total_bytes: None,
            columns: vec![col("id", "bigint", true)],
        };
        let b = TableInfo {
            schema: "public".to_string(),
            table: "orders".to_string(),
            row_estimate: 200,
            total_bytes: None,
            columns: vec![col("id", "bigint", true), col("user_id", "int", false)],
        };
        let yaml = yaml_scaffold::generate_schema_config(
            &[a, b],
            "postgresql://localhost/db",
            r#"schema "public" (2)"#,
            &InitYamlDestination::default(),
        )
        .unwrap();
        assert!(yaml.contains("  - name: users"), "got:\n{yaml}");
        assert!(yaml.contains("  - name: orders"), "got:\n{yaml}");
        // Each export references its base relation. PG full-mode now uses the
        // `table:` shortcut (so catalog hints can resolve NUMERIC types); other
        // modes/dialects keep the `FROM <name>` form.
        assert!(
            yaml.contains("    table: users") || yaml.contains("FROM users"),
            "expected users reference; got:\n{yaml}"
        );
        assert!(
            yaml.contains("    table: orders") || yaml.contains("FROM orders"),
            "expected orders reference; got:\n{yaml}"
        );
        assert_eq!(yaml.matches("meta_columns:").count(), 2, "got:\n{yaml}");
    }

    #[test]
    fn generate_config_gcs_bucket_sets_per_table_prefix_without_credentials_file() {
        let info = make_table(
            2_000_000,
            vec![col("id", "bigint", true), col("name", "text", false)],
        );
        let dest = InitYamlDestination {
            gcs_bucket: Some("my-bucket".to_string()),
            gcs_credentials_file: None,
            s3_bucket: None,
            s3_region: None,
        };
        let yaml =
            yaml_scaffold::generate_config(&info, "postgresql://localhost/db", &dest).unwrap();
        assert!(yaml.contains("type: gcs"), "got:\n{yaml}");
        assert!(yaml.contains("bucket: my-bucket"), "got:\n{yaml}");
        assert!(yaml.contains("prefix: exports/orders/"), "got:\n{yaml}");
        assert!(
            !yaml.contains("credentials_file"),
            "ADC / env key: YAML should omit credentials_file, got:\n{yaml}"
        );
    }

    #[test]
    fn generate_config_gcs_adds_credentials_file_when_set() {
        let info = make_table(100, vec![col("id", "bigint", true)]);
        let dest = InitYamlDestination {
            gcs_bucket: Some("b".to_string()),
            gcs_credentials_file: Some("/path/sa.json".to_string()),
            s3_bucket: None,
            s3_region: None,
        };
        let yaml =
            yaml_scaffold::generate_config(&info, "postgresql://localhost/db", &dest).unwrap();
        assert!(
            yaml.contains("credentials_file: /path/sa.json"),
            "got:\n{yaml}"
        );
    }

    #[test]
    fn generate_config_s3_bucket_with_region_emits_full_destination() {
        let info = make_table(
            2_000_000,
            vec![col("id", "bigint", true), col("name", "text", false)],
        );
        let dest = InitYamlDestination {
            gcs_bucket: None,
            gcs_credentials_file: None,
            s3_bucket: Some("my-s3-bucket".to_string()),
            s3_region: Some("eu-central-1".to_string()),
        };
        let yaml =
            yaml_scaffold::generate_config(&info, "postgresql://localhost/db", &dest).unwrap();
        assert!(yaml.contains("type: s3"), "got:\n{yaml}");
        assert!(yaml.contains("bucket: my-s3-bucket"), "got:\n{yaml}");
        assert!(yaml.contains("prefix: exports/orders/"), "got:\n{yaml}");
        assert!(yaml.contains("region: eu-central-1"), "got:\n{yaml}");
        assert!(
            !yaml.contains("type: local"),
            "should not fall back to local:\n{yaml}"
        );
        assert!(
            !yaml.contains("type: gcs"),
            "must not mix s3 with gcs lines:\n{yaml}"
        );
    }

    #[test]
    fn generate_config_s3_without_region_omits_region_line() {
        let info = make_table(100, vec![col("id", "bigint", true)]);
        let dest = InitYamlDestination {
            gcs_bucket: None,
            gcs_credentials_file: None,
            s3_bucket: Some("b".to_string()),
            s3_region: None,
        };
        let yaml =
            yaml_scaffold::generate_config(&info, "postgresql://localhost/db", &dest).unwrap();
        assert!(yaml.contains("type: s3"), "got:\n{yaml}");
        assert!(!yaml.contains("region:"), "got:\n{yaml}");
    }

    #[test]
    fn validate_rejects_both_buckets_set() {
        let dest = InitYamlDestination {
            gcs_bucket: Some("g".into()),
            gcs_credentials_file: None,
            s3_bucket: Some("s".into()),
            s3_region: None,
        };
        let err = dest.validate().expect_err("conflict must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("--gcs-bucket") && msg.contains("--s3-bucket"),
            "expected conflict diagnostic, got: {msg}"
        );
    }

    #[test]
    fn validate_accepts_neither_or_one_bucket() {
        InitYamlDestination::default().validate().unwrap();
        InitYamlDestination {
            gcs_bucket: Some("g".into()),
            ..Default::default()
        }
        .validate()
        .unwrap();
        InitYamlDestination {
            s3_bucket: Some("s".into()),
            ..Default::default()
        }
        .validate()
        .unwrap();
    }

    #[test]
    fn yaml_quoting_is_noop_for_safe_values() {
        for v in ["bucket-1", "my_table", "exports/orders/", "us-east-1"] {
            assert_eq!(
                yaml_scaffold::yaml_quote_if_needed(v),
                v,
                "unexpected quoting for {v:?}"
            );
        }
    }

    #[test]
    fn yaml_quoting_handles_risky_values() {
        // Reserved-looking
        assert_eq!(yaml_scaffold::yaml_quote_if_needed("true"), "\"true\"");
        assert_eq!(yaml_scaffold::yaml_quote_if_needed("NO"), "\"NO\"");
        assert_eq!(yaml_scaffold::yaml_quote_if_needed("null"), "\"null\"");
        // Numeric-looking
        assert_eq!(yaml_scaffold::yaml_quote_if_needed("123"), "\"123\"");
        assert_eq!(yaml_scaffold::yaml_quote_if_needed("1.5"), "\"1.5\"");
        // Indicators / separators
        assert_eq!(
            yaml_scaffold::yaml_quote_if_needed("-leading-dash"),
            "\"-leading-dash\""
        );
        assert_eq!(
            yaml_scaffold::yaml_quote_if_needed("foo: bar"),
            "\"foo: bar\""
        );
        assert_eq!(
            yaml_scaffold::yaml_quote_if_needed("foo #bar"),
            "\"foo #bar\""
        );
        // Whitespace
        assert_eq!(
            yaml_scaffold::yaml_quote_if_needed(" leading"),
            "\" leading\""
        );
        assert_eq!(yaml_scaffold::yaml_quote_if_needed(""), "\"\"");
        // Embedded quote / backslash → escaped
        assert_eq!(yaml_scaffold::yaml_quote_if_needed("a\"b"), "\"a\\\"b\"");
        assert_eq!(yaml_scaffold::yaml_quote_if_needed("c\\d"), "\"c\\\\d\"");
    }

    #[test]
    fn generate_config_quotes_risky_bucket_name() {
        // A bucket literally named "true" must round-trip as a string.
        let info = make_table(100, vec![col("id", "bigint", true)]);
        let dest = InitYamlDestination {
            gcs_bucket: Some("true".to_string()),
            ..Default::default()
        };
        let yaml =
            yaml_scaffold::generate_config(&info, "postgresql://localhost/db", &dest).unwrap();
        // Plain `bucket: true` would deserialize to the boolean `true`.
        assert!(
            yaml.contains("bucket: \"true\""),
            "risky bucket name must be quoted, got:\n{yaml}"
        );
        let parsed: serde_yaml_ng::Value =
            serde_yaml_ng::from_str(&yaml).expect("YAML must remain valid");
        let bucket = &parsed["exports"][0]["destination"]["bucket"];
        assert_eq!(
            bucket.as_str(),
            Some("true"),
            "bucket must deserialize as a string, got: {bucket:?}"
        );
    }

    #[test]
    fn generate_config_quotes_credentials_path_with_spaces() {
        let info = make_table(100, vec![col("id", "bigint", true)]);
        let dest = InitYamlDestination {
            gcs_bucket: Some("b".into()),
            gcs_credentials_file: Some("/path with spaces/sa.json".into()),
            ..Default::default()
        };
        let yaml =
            yaml_scaffold::generate_config(&info, "postgresql://localhost/db", &dest).unwrap();
        let parsed: serde_yaml_ng::Value =
            serde_yaml_ng::from_str(&yaml).expect("YAML must remain valid");
        assert_eq!(
            parsed["exports"][0]["destination"]["credentials_file"].as_str(),
            Some("/path with spaces/sa.json"),
        );
    }

    #[test]
    fn generate_config_full_yaml_round_trips() {
        let info = make_table(
            2_000_000,
            vec![col("id", "bigint", true), col("name", "text", false)],
        );
        let dest = InitYamlDestination {
            s3_bucket: Some("ok-bucket".into()),
            s3_region: Some("eu-west-1".into()),
            ..Default::default()
        };
        let yaml =
            yaml_scaffold::generate_config(&info, "postgresql://localhost/db", &dest).unwrap();
        let parsed: serde_yaml_ng::Value =
            serde_yaml_ng::from_str(&yaml).expect("scaffold must be valid YAML");
        let exp = &parsed["exports"][0];
        assert_eq!(exp["name"].as_str(), Some("orders"));
        assert_eq!(exp["mode"].as_str(), Some("chunked"));
        assert_eq!(exp["chunk_column"].as_str(), Some("id"));
        assert_eq!(exp["chunk_checkpoint"].as_bool(), Some(true));
        assert_eq!(exp["meta_columns"]["row_hash"].as_bool(), Some(true));
        assert_eq!(exp["destination"]["type"].as_str(), Some("s3"));
        assert_eq!(exp["destination"]["bucket"].as_str(), Some("ok-bucket"));
        assert_eq!(exp["destination"]["region"].as_str(), Some("eu-west-1"));
        assert_eq!(
            exp["destination"]["prefix"].as_str(),
            Some("exports/orders/")
        );
    }

    #[test]
    fn table_discovery_surfaces_cursor_and_coalesce_hint() {
        // No integer PK so `suggest_mode` falls back to incremental.
        let info = make_table(
            300_000,
            vec![
                col("key", "uuid", true),
                col("created_at", "timestamp", false),
                ColumnInfo {
                    name: "updated_at".into(),
                    data_type: "timestamp".into(),
                    is_primary_key: false,
                    is_nullable: true,
                    numeric_precision: None,
                    numeric_scale: None,
                },
            ],
        );
        let td = table_discovery(&info);
        assert_eq!(td.suggested_mode, "incremental");
        assert_eq!(td.cursor_candidates[0].column, "updated_at");
        assert_eq!(
            td.suggested_cursor_fallback_column,
            Some("created_at".into())
        );
        assert!(
            td.notes.iter().any(|n| n.contains("coalesce")),
            "expected coalesce hint, got: {:?}",
            td.notes
        );
    }
}
