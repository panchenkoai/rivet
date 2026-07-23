mod artifact;
mod candidates;
mod mongo;
mod mssql;
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

    /// The single-column primary key, if the table has exactly one PK column.
    ///
    /// Keyset (`chunk_by_key`) pages by one index-backed unique key of ANY
    /// orderable type — int, uuid, string, timestamp — so, unlike
    /// [`best_chunk_column`](Self::best_chunk_column) (integer-only, for range
    /// chunking), this returns the PK regardless of type. `None` for a composite
    /// PK (keyset is single-column only) or a table with no PK — those fall back
    /// to range chunking on an integer column.
    pub(crate) fn single_pk_column(&self) -> Option<&str> {
        let mut pks = self.columns.iter().filter(|c| c.is_primary_key);
        let first = pks.next()?;
        // A composite PK has no single keyset key — fall back to range.
        if pks.next().is_some() {
            return None;
        }
        Some(first.name.as_str())
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

    /// One-line rationale for [`suggest_mode`]'s choice — emitted as an
    /// inline comment in the generated YAML so the operator can see *why*
    /// chunked / incremental / full was picked, not just *what*. Wording
    /// stays short because it lives in the YAML directly above `mode:`.
    pub(crate) fn mode_rationale(&self, mode: &str) -> String {
        match mode {
            "chunked" => {
                let base = format!(
                    "auto: ~{} rows ≥ 100K threshold and chunk column '{}' is available",
                    fmt_row_estimate(self.row_estimate),
                    self.best_chunk_column().unwrap_or("id"),
                );
                // A chunked re-run re-reads the whole table. If a cursor column
                // exists, point operators at incremental for scheduled re-runs —
                // the pilot showed a 655k-row table re-dumped 4× in 2 days under
                // chunked, re-reading ~570k unchanged rows each time.
                match self.best_cursor_column() {
                    Some(cursor) => format!(
                        "{base}. NOTE: chunked re-reads the whole table each run — for scheduled \
                         re-runs, `mode: incremental` on '{cursor}' pulls only changed rows"
                    ),
                    None => base,
                }
            }
            "incremental" => format!(
                "auto: ~{} rows ≥ 100K threshold; chunk column missing, falling back to incremental on '{}'",
                fmt_row_estimate(self.row_estimate),
                self.best_cursor_column().unwrap_or("updated_at"),
            ),
            "full" => format!(
                "auto: ~{} rows below 100K chunked threshold",
                fmt_row_estimate(self.row_estimate),
            ),
            _ => format!("mode={mode}"),
        }
    }

    /// Default chunk_size scaled by the row estimate. Goal: keep the
    /// per-table file count in a humane range (~10–50 files) regardless
    /// of how large the table is. The previous hard-coded `100_000`
    /// produced 100 files for a 10 M-row table and 1000+ for 100 M;
    /// operators read that as noise rather than progress.
    pub(crate) fn suggest_chunk_size(&self) -> u64 {
        match self.row_estimate {
            r if r < 1_000_000 => 100_000,     // < 1 M  → up to 10 files
            r if r < 10_000_000 => 250_000,    // 1–10 M → 4–40 files
            r if r < 100_000_000 => 1_000_000, // 10–100 M → 10–100 files
            _ => 2_500_000,                    // ≥ 100 M → 40+ files
        }
    }

    /// Average bytes per row, when the catalog gave us `total_bytes`. The cost
    /// signal behind parallelism: narrow rows (small value) are CPU-bound on
    /// row *count* and parallelise on every engine; wide rows already saturate
    /// a fast engine's sequential scan. `None` when the size is unknown —
    /// callers then fall back to a row-count-only heuristic.
    pub(crate) fn avg_row_bytes(&self) -> Option<i64> {
        match self.total_bytes {
            Some(b) if self.row_estimate > 0 => Some(b / self.row_estimate),
            _ => None,
        }
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

/// Render a row estimate as a short human-readable string for inline
/// YAML comments (`"1.0M"`, `"10K"`, `"950"`). Stays compact because the
/// rationale comment must fit on one line above `mode:`.
fn fmt_row_estimate(rows: i64) -> String {
    if rows >= 1_000_000 {
        format!("{:.1}M", rows as f64 / 1_000_000.0)
    } else if rows >= 1_000 {
        format!("{}K", rows / 1_000)
    } else {
        rows.to_string()
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
    } else if source_url.starts_with("sqlserver") || source_url.starts_with("mssql") {
        Ok("mssql")
    } else if source_url.starts_with("mongodb") {
        Ok("mongo")
    } else {
        anyhow::bail!(
            "Unsupported source URL scheme. Expected postgresql://, mysql://, sqlserver://, or mongodb://, got: {}",
            source_url
        )
    }
}

/// Default SQL Server schema when the user passes a bare table name.
/// [`yaml_scaffold::parse_table`] defaults an unqualified table to `public`
/// (the PostgreSQL default); SQL Server's is `dbo`, so the mssql arm rewrites a
/// `public` placeholder to `dbo` while honouring any schema the user *did*
/// qualify (`sales.orders`).
fn mssql_table_schema(parsed_schema: &str) -> String {
    if parsed_schema == "public" {
        "dbo".to_string()
    } else {
        parsed_schema.to_string()
    }
}

/// Whole-schema include/exclude filtering for `rivet init` (L1).
///
/// Both lists hold simple globs (`*` = any run, `?` = one char); a discovered
/// table is kept when it matches at least one `include` (or `include` is empty)
/// **and** matches no `exclude`. `--exclude` therefore wins over `--include`.
/// No flags ⇒ both empty ⇒ every table kept (the prior behaviour). Applied only
/// to the whole-schema path; `--table` names one relation explicitly.
#[derive(Debug, Clone, Default)]
pub struct TableFilter {
    pub include: Vec<String>,
    pub exclude: Vec<String>,
}

impl TableFilter {
    /// `true` when `name` survives the filter (kept in the scaffold).
    pub(super) fn matches(&self, name: &str) -> bool {
        if self.exclude.iter().any(|g| glob_match(g, name)) {
            return false;
        }
        self.include.is_empty() || self.include.iter().any(|g| glob_match(g, name))
    }
}

/// Minimal shell-style glob over the whole `name` (anchored at both ends):
/// `*` matches any run (including empty), `?` matches exactly one char, every
/// other char is literal. Backtracking on `*` is linear-enough for the short
/// identifiers init deals with — no regex / glob crate dependency (L1 keeps the
/// matcher in-tree on purpose). Comparison is byte-exact (case-sensitive),
/// matching how SQL identifiers are returned by the catalog.
fn glob_match(pattern: &str, name: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let txt: Vec<char> = name.chars().collect();
    // Classic two-pointer wildcard match with a single backtrack anchor for `*`.
    let (mut p, mut t) = (0usize, 0usize);
    let (mut star, mut star_t) = (None, 0usize);
    while t < txt.len() {
        if p < pat.len() && (pat[p] == '?' || pat[p] == txt[t]) {
            p += 1;
            t += 1;
        } else if p < pat.len() && pat[p] == '*' {
            star = Some(p);
            star_t = t;
            p += 1;
        } else if let Some(sp) = star {
            // Backtrack: let the last `*` swallow one more char.
            p = sp + 1;
            star_t += 1;
            t = star_t;
        } else {
            return false;
        }
    }
    // Consume trailing `*`s (they match the empty remainder).
    while p < pat.len() && pat[p] == '*' {
        p += 1;
    }
    p == pat.len()
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
/// Without `--table`: introspect every table/view in a PostgreSQL schema (default `public`),
/// a MySQL database (from `--schema` or from the URL path), or a SQL Server schema
/// (default `dbo`), optionally narrowed by the `--include` / `--exclude` globs in `filter`.
/// How the DB URL reached `rivet init` — determines which connection form the
/// scaffold writes so the prescribed next steps (`doctor` / `check` / `run`)
/// inherit a WORKING connection, instead of always emitting `url_env:
/// DATABASE_URL` and failing on a variable the user never set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceProvenance {
    /// `--source <url>`: typed inline. The scaffold keeps the secure
    /// `url_env: DATABASE_URL` default (it never writes the literal URL into a
    /// likely-committed file) and the next-steps block reminds the user to
    /// export it.
    Inline,
    /// `--source-env <NAME>`: write `url_env: NAME` — the variable the user has
    /// already exported, so the scaffold connects out of the box.
    Env(String),
    /// `--source-file <PATH>`: write `url_file: PATH`.
    File(String),
}

// Thin CLI dispatch shim: each argument maps 1:1 to a `rivet init` flag, so
// bundling them into a struct would only add indirection for the single caller.
#[allow(clippy::too_many_arguments)]
pub fn init(
    source_url: &str,
    provenance: &SourceProvenance,
    table: Option<&str>,
    schema: Option<&str>,
    output: Option<&str>,
    format: InitFormat,
    yaml_destination: InitYamlDestination,
    filter: &TableFilter,
    mode_override: Option<&str>,
) -> Result<()> {
    yaml_destination.validate()?;
    let (text, yaml_decimal_review) = match format {
        InitFormat::Yaml => init_yaml(
            source_url,
            provenance,
            table,
            schema,
            &yaml_destination,
            filter,
            mode_override,
        )?,
        InitFormat::DiscoveryJson => {
            // Defensive backstop for non-CLI callers; the `rivet init` CLI
            // already rejects `--discover` together with any cloud flag via
            // `conflicts_with_all` in main.rs.
            if yaml_destination != InitYamlDestination::default() {
                eprintln!(
                    "rivet: note: --gcs-bucket / --s3-bucket are ignored for --discover (JSON has no destination)"
                );
            }
            (
                init_discovery_json(source_url, table, schema, filter)?,
                false,
            )
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
            // Don't leave the user holding a cold artifact — show the path from
            // "I have a config" to "I have parquet files". Only for the YAML
            // scaffold (the discovery JSON isn't runnable).
            if matches!(format, InitFormat::Yaml) {
                eprint!("{}", next_steps_block(path, provenance));
            }
        }
        None => {
            // stdout stays pure (pipeable); the guidance still reaches the user
            // on stderr so `rivet init | tee rivet.yaml` isn't a dead end.
            print!("{text}");
            if matches!(format, InitFormat::Yaml) {
                eprint!("{}", next_steps_block("rivet.yaml", provenance));
            }
        }
    }

    Ok(())
}

/// The friendly "do this next" ladder printed after a YAML scaffold. For an
/// inline `--source` URL it leads with a step-0 export reminder, because the
/// scaffold deliberately writes `url_env: DATABASE_URL` (it never persists the
/// literal URL) and would otherwise fail on an unset variable.
fn next_steps_block(path: &str, provenance: &SourceProvenance) -> String {
    let mut s = String::from("\nNext steps:\n");
    if matches!(provenance, SourceProvenance::Inline) {
        s.push_str(
            "  0. export DATABASE_URL='<your-url>'    # the scaffold reads this (URL kept out of the file)\n",
        );
    }
    s.push_str(&format!(
        "  1. rivet doctor -c {path}            # test source + destination auth\n  \
         2. rivet check  -c {path}            # column-type & schema report\n  \
         3. rivet run    -c {path} --validate # export, then verify row counts\n"
    ));
    s.push_str(&format!(
        "\nOr seal a reviewable plan, then apply it (runs many tables by priority wave):\n  \
         rivet plan  -c {path}     # assigns waves + writes a reviewable plan\n  \
         rivet apply {path}        # runs wave-by-wave (parallel where safe)\n"
    ));
    s
}

fn init_yaml(
    source_url: &str,
    provenance: &SourceProvenance,
    table: Option<&str>,
    schema: Option<&str>,
    dest: &InitYamlDestination,
    filter: &TableFilter,
    mode_override: Option<&str>,
) -> Result<(String, bool)> {
    if let Some(t) = table {
        let (sch, table_name) = yaml_scaffold::parse_table(t);
        let info = match source_type(source_url)? {
            "postgres" => {
                let mut client = postgres::connect(source_url)?;
                postgres::introspect(&mut client, &sch, table_name)?
            }
            "mysql" => {
                let mut conn = mysql::connect(source_url)?;
                mysql::introspect(&mut conn, table_name)?
            }
            "mssql" => {
                let mut conn = mssql::connect(source_url)?;
                mssql::introspect(&mut conn, &mssql_table_schema(&sch), table_name)?
            }
            "mongo" => {
                let conn = mongo::connect(source_url)?;
                mongo::introspect(&conn, table_name)?
            }
            _ => unreachable!(),
        };
        let hint = yaml_scaffold::table_has_unbounded_decimal_columns(&info);
        let yaml =
            yaml_scaffold::generate_config(&info, source_url, provenance, dest, mode_override)?;
        return Ok((yaml, hint));
    }
    let infos = introspect_all(source_url, schema, filter)?;
    if infos.is_empty() {
        anyhow::bail!("No tables or views found (check --schema and privileges)");
    }
    let label = schema_scope_label(source_url, schema, infos.len())?;
    let hint = infos
        .iter()
        .any(yaml_scaffold::table_has_unbounded_decimal_columns);
    let yaml = yaml_scaffold::generate_schema_config(
        &infos,
        source_url,
        provenance,
        &label,
        dest,
        mode_override,
    )?;
    Ok((yaml, hint))
}

fn init_discovery_json(
    source_url: &str,
    table: Option<&str>,
    schema: Option<&str>,
    filter: &TableFilter,
) -> Result<String> {
    let (infos, scope) = if let Some(t) = table {
        let (sch, table_name) = yaml_scaffold::parse_table(t);
        let info = match source_type(source_url)? {
            "postgres" => {
                let mut client = postgres::connect(source_url)?;
                postgres::introspect(&mut client, &sch, table_name)?
            }
            "mysql" => {
                let mut conn = mysql::connect(source_url)?;
                mysql::introspect(&mut conn, table_name)?
            }
            "mssql" => {
                let mut conn = mssql::connect(source_url)?;
                mssql::introspect(&mut conn, &mssql_table_schema(&sch), table_name)?
            }
            "mongo" => {
                let conn = mongo::connect(source_url)?;
                mongo::introspect(&conn, table_name)?
            }
            _ => unreachable!(),
        };
        let scope = match source_type(source_url)? {
            "postgres" => format!("table \"{}\".\"{}\"", info.schema, info.table),
            "mysql" => format!("table `{}`", info.table),
            "mssql" => format!("table [{}].[{}]", info.schema, info.table),
            "mongo" => format!("collection {}", info.table),
            _ => unreachable!(),
        };
        (vec![info], scope)
    } else {
        let infos = introspect_all(source_url, schema, filter)?;
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

fn introspect_all(
    source_url: &str,
    schema: Option<&str>,
    filter: &TableFilter,
) -> Result<Vec<TableInfo>> {
    match source_type(source_url)? {
        "postgres" => {
            let sch = schema
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("public");
            // One connection for the whole scan — a fresh client per table
            // would mean N+1 TCP+auth(+TLS) handshakes on large schemas.
            let mut client = postgres::connect(source_url)?;
            let names = retain_filtered(postgres::list_tables(&mut client, sch)?, filter);
            let mut out = Vec::with_capacity(names.len());
            for n in names {
                match postgres::introspect(&mut client, sch, &n) {
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
            // One pooled connection for the whole scan — a fresh Pool per
            // table would mean N+1 TCP+auth(+TLS) handshakes on large schemas.
            let mut conn = mysql::connect(source_url)?;
            let names = retain_filtered(mysql::list_tables(&mut conn, &db)?, filter);
            let mut out = Vec::with_capacity(names.len());
            for n in names {
                out.push(mysql::introspect(&mut conn, &n)?);
            }
            Ok(out)
        }
        "mssql" => {
            let sch = schema
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("dbo");
            // One connection for the whole scan — `MssqlSource` owns its runtime
            // and block_on's each query, so reusing it avoids N+1 TLS logins.
            let mut conn = mssql::connect(source_url)?;
            let names = retain_filtered(mssql::list_tables(&mut conn, sch)?, filter);
            let mut out = Vec::with_capacity(names.len());
            for n in names {
                match mssql::introspect(&mut conn, sch, &n) {
                    Ok(info) => out.push(info),
                    // Dropped between list and introspect — skip, don't abort.
                    Err(e) if e.to_string().contains("not found or has no columns") => {}
                    Err(e) => return Err(e),
                }
            }
            Ok(out)
        }
        "mongo" => {
            let conn = mongo::connect(source_url)?;
            let names = retain_filtered(mongo::list_tables(&conn)?, filter);
            let mut out = Vec::with_capacity(names.len());
            for n in names {
                out.push(mongo::introspect(&conn, &n)?);
            }
            Ok(out)
        }
        _ => unreachable!(),
    }
}

/// Keep only the discovered table names the `--include` / `--exclude` globs
/// admit (L1). A no-op when both lists are empty (the default), so the
/// whole-schema scan is unchanged unless the operator asked to narrow it.
fn retain_filtered(mut names: Vec<String>, filter: &TableFilter) -> Vec<String> {
    names.retain(|n| filter.matches(n));
    names
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
        "mssql" => {
            let sch = schema
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("dbo");
            format!("SQL Server schema \"{sch}\" ({n} {obj})")
        }
        "mongo" => {
            // Database name from the URL path: mongodb://[user@]host[:port]/<db>[?opts]
            let db = source_url
                .split_once("://")
                .and_then(|(_, rest)| rest.split('/').nth(1))
                .map(|seg| seg.split('?').next().unwrap_or(seg))
                .filter(|s| !s.is_empty())
                .unwrap_or("(default)");
            format!("MongoDB database \"{db}\" ({n} {obj})")
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
    fn chunked_rationale_hints_incremental_when_cursor_column_exists() {
        // warranty-shaped: large table with BOTH an int PK and a cursor column.
        // suggest_mode picks chunked, but the rationale must point at incremental
        // for scheduled re-runs — chunked re-reads the whole table every run.
        let info = make_table(
            655_000,
            vec![
                col("warranty_id", "bigint", true),
                col("update_date", "timestamp", false),
            ],
        );
        assert_eq!(info.suggest_mode(), "chunked");
        let r = info.mode_rationale("chunked");
        assert!(
            r.contains("mode: incremental"),
            "expected an incremental hint: {r}"
        );
        assert!(
            r.contains("update_date"),
            "should name the cursor column: {r}"
        );
    }

    #[test]
    fn chunked_rationale_no_incremental_hint_without_cursor_column() {
        // No timestamp/cursor column → no incremental hint (would be misleading).
        let info = make_table(5_000_000, vec![col("id", "bigint", true)]);
        assert_eq!(info.suggest_mode(), "chunked");
        let r = info.mode_rationale("chunked");
        assert!(
            !r.contains("mode: incremental"),
            "no cursor column → no hint: {r}"
        );
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
    fn generate_config_chunked_single_pk_uses_keyset() {
        // A single-column PK → keyset (`chunk_by_key`), immune to sparse keys —
        // NOT range `chunk_column` (which blows up on a huge/gappy id). Sequential,
        // so no `parallel:`; chunk_checkpoint is ON by default (safe crash-recovery),
        // and the append-only incremental behaviour is a commented `keyset_incremental`.
        let info = make_table(
            2_000_000,
            vec![col("id", "bigint", true), col("name", "text", false)],
        );
        let yaml = yaml_scaffold::generate_config(
            &info,
            "postgresql://localhost/db",
            &super::SourceProvenance::Inline,
            &InitYamlDestination::default(),
            None,
        )
        .unwrap();
        assert!(yaml.contains("mode: chunked"), "got:\n{yaml}");
        assert!(yaml.contains("chunk_by_key: id"), "got:\n{yaml}");
        // No ACTIVE `chunk_column:` line (the keyset hint comment mentions it).
        assert!(
            !yaml.contains("\n    chunk_column:"),
            "PK must keyset, not range-chunk:\n{yaml}"
        );
        // keyset is sequential — no parallel workers emitted.
        assert!(
            !yaml.contains("\n    parallel:"),
            "keyset must not emit parallel:\n{yaml}"
        );
        // chunk_checkpoint is ON by default now (crash-recovery is safe: a clean
        // re-run still does a full pass). The append-only incremental behaviour —
        // the part that CAN skip rows — stays a commented `keyset_incremental` opt-in.
        assert!(
            yaml.contains("\n    chunk_checkpoint: true"),
            "keyset must default chunk_checkpoint on (safe crash-recovery):\n{yaml}"
        );
        assert!(
            yaml.contains("# keyset_incremental: true"),
            "append-only incremental must be a commented opt-in:\n{yaml}"
        );
        // Keyset needs the `table:` shortcut (planner introspects the unique index),
        // so the export emits `table:`, not a `SELECT … FROM` query.
        assert!(yaml.contains("table: orders"), "got:\n{yaml}");
        assert!(
            !yaml.contains("query:"),
            "keyset must use table:, not query:\n{yaml}"
        );
        assert!(yaml.contains("meta_columns:"), "got:\n{yaml}");
    }

    #[test]
    fn generate_config_chunked_no_single_pk_uses_range_column() {
        // No single-column PK (here: no PK at all) → range chunk on the best
        // integer column, with `chunk_checkpoint: true` and parallel as before.
        let info = make_table(
            2_000_000,
            vec![col("region_id", "int", false), col("name", "text", false)],
        );
        let yaml = yaml_scaffold::generate_config(
            &info,
            "postgresql://localhost/db",
            &super::SourceProvenance::Inline,
            &InitYamlDestination::default(),
            None,
        )
        .unwrap();
        assert!(yaml.contains("mode: chunked"), "got:\n{yaml}");
        assert!(yaml.contains("chunk_column: region_id"), "got:\n{yaml}");
        assert!(
            !yaml.contains("chunk_by_key:"),
            "no PK → range, not keyset:\n{yaml}"
        );
        assert!(yaml.contains("chunk_checkpoint: true"), "got:\n{yaml}");
    }

    #[test]
    fn single_pk_column_detects_single_composite_and_none() {
        // Exactly one PK column → that column.
        let one = make_table(1, vec![col("id", "bigint", true), col("n", "text", false)]);
        assert_eq!(one.single_pk_column(), Some("id"));
        // Composite PK → None (keyset is single-column only).
        let composite = make_table(1, vec![col("a", "bigint", true), col("b", "bigint", true)]);
        assert_eq!(composite.single_pk_column(), None);
        // No PK → None.
        let none = make_table(1, vec![col("x", "int", false)]);
        assert_eq!(none.single_pk_column(), None);
        // Non-integer single PK still keysets (unlike best_chunk_column).
        let uuid = make_table(1, vec![col("id", "uuid", true)]);
        assert_eq!(uuid.single_pk_column(), Some("id"));
        assert_eq!(uuid.best_chunk_column(), None);
    }

    #[test]
    fn generate_config_incremental_contains_cursor() {
        let info = make_table(200_000, vec![col("updated_at", "timestamp", false)]);
        let yaml = yaml_scaffold::generate_config(
            &info,
            "mysql://localhost/db",
            &super::SourceProvenance::Inline,
            &InitYamlDestination::default(),
            None,
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
            &super::SourceProvenance::Inline,
            r#"schema "public" (2)"#,
            &InitYamlDestination::default(),
            None,
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
        let yaml = yaml_scaffold::generate_config(
            &info,
            "postgresql://localhost/db",
            &super::SourceProvenance::Inline,
            &dest,
            None,
        )
        .unwrap();
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
        let yaml = yaml_scaffold::generate_config(
            &info,
            "postgresql://localhost/db",
            &super::SourceProvenance::Inline,
            &dest,
            None,
        )
        .unwrap();
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
        let yaml = yaml_scaffold::generate_config(
            &info,
            "postgresql://localhost/db",
            &super::SourceProvenance::Inline,
            &dest,
            None,
        )
        .unwrap();
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
        let yaml = yaml_scaffold::generate_config(
            &info,
            "postgresql://localhost/db",
            &super::SourceProvenance::Inline,
            &dest,
            None,
        )
        .unwrap();
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
        let yaml = yaml_scaffold::generate_config(
            &info,
            "postgresql://localhost/db",
            &super::SourceProvenance::Inline,
            &dest,
            None,
        )
        .unwrap();
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
        let yaml = yaml_scaffold::generate_config(
            &info,
            "postgresql://localhost/db",
            &super::SourceProvenance::Inline,
            &dest,
            None,
        )
        .unwrap();
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
        let yaml = yaml_scaffold::generate_config(
            &info,
            "postgresql://localhost/db",
            &super::SourceProvenance::Inline,
            &dest,
            None,
        )
        .unwrap();
        let parsed: serde_yaml_ng::Value =
            serde_yaml_ng::from_str(&yaml).expect("scaffold must be valid YAML");
        let exp = &parsed["exports"][0];
        assert_eq!(exp["name"].as_str(), Some("orders"));
        assert_eq!(exp["mode"].as_str(), Some("chunked"));
        // Single-column PK → keyset via the `table:` shortcut; chunk_checkpoint is
        // ON by default (safe crash-recovery), while keyset_incremental stays a
        // commented opt-in (absent from the parsed active config).
        assert_eq!(exp["chunk_by_key"].as_str(), Some("id"));
        assert_eq!(exp["table"].as_str(), Some("orders"));
        assert!(exp["chunk_column"].is_null());
        assert_eq!(exp["chunk_checkpoint"].as_bool(), Some(true));
        assert!(exp["keyset_incremental"].is_null());
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
    fn source_type_recognizes_sqlserver_and_mssql_schemes() {
        assert_eq!(
            source_type("sqlserver://sa:p@host:1433/db").unwrap(),
            "mssql"
        );
        assert_eq!(source_type("mssql://sa:p@host:1433/db").unwrap(), "mssql");
    }

    #[test]
    fn source_type_unsupported_scheme_names_sqlserver() {
        let err = source_type("oracle://host/db").expect_err("oracle is unsupported");
        let msg = format!("{err}");
        assert!(
            msg.contains("sqlserver://"),
            "error must list sqlserver:// as accepted, got: {msg}"
        );
    }

    #[test]
    fn mssql_table_schema_defaults_public_placeholder_to_dbo() {
        // `parse_table("orders")` yields schema "public"; mssql rewrites to dbo.
        assert_eq!(mssql_table_schema("public"), "dbo");
        // An explicitly-qualified schema is honoured verbatim.
        assert_eq!(mssql_table_schema("sales"), "sales");
    }

    #[test]
    fn glob_match_literal_star_and_question() {
        assert!(glob_match("orders", "orders"));
        assert!(!glob_match("orders", "orders2"));
        // `*` matches any run including empty.
        assert!(glob_match("bench_*", "bench_users"));
        assert!(glob_match("bench_*", "bench_"));
        assert!(!glob_match("bench_*", "orders"));
        assert!(glob_match("*_tmp", "orders_tmp"));
        assert!(glob_match("a*c", "abc"));
        assert!(glob_match("a*c", "ac"));
        assert!(!glob_match("a*c", "ab"));
        // `?` matches exactly one char.
        assert!(glob_match("order?", "orders"));
        assert!(!glob_match("order?", "order"));
        // Bare `*` matches everything.
        assert!(glob_match("*", "anything"));
        assert!(glob_match("*", ""));
        // Multiple stars.
        assert!(glob_match("*bench*", "x_bench_y"));
        assert!(!glob_match("*bench*", "orders"));
    }

    #[test]
    fn table_filter_empty_keeps_everything() {
        let f = TableFilter::default();
        assert!(f.matches("orders"));
        assert!(f.matches("bench_users"));
    }

    #[test]
    fn table_filter_include_only_keeps_matches() {
        let f = TableFilter {
            include: vec!["orders".into()],
            exclude: vec![],
        };
        assert!(f.matches("orders"));
        assert!(!f.matches("users"));
        assert!(!f.matches("bench_orders"));
    }

    #[test]
    fn table_filter_exclude_drops_matches() {
        let f = TableFilter {
            include: vec![],
            exclude: vec!["bench_*".into()],
        };
        assert!(f.matches("orders"));
        assert!(!f.matches("bench_users"));
    }

    #[test]
    fn table_filter_exclude_wins_over_include() {
        // A name that matches both include and exclude is excluded.
        let f = TableFilter {
            include: vec!["*".into()],
            exclude: vec!["bench_*".into()],
        };
        assert!(f.matches("orders"));
        assert!(!f.matches("bench_users"));
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
