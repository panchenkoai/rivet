mod artifact;
mod candidates;
/// Offline strategy-decision replay harness — test-only (no runtime caller).
#[cfg(test)]
mod catalog_replay;
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
///
/// `Serialize`/`Deserialize` so a real hostile DB's SCHEMA (types + PK shape, no
/// row data) can be distilled into a checked-in catalog fixture and replayed
/// offline against the strategy-decision logic (`catalog_replay`) — the messy DB
/// becomes a regression oracle with zero customer-data exposure.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
///
/// This is the ENTIRE input to the strategy decision (init `suggest_mode` +
/// keyset/chunk resolution) — schema, catalog stats (`row_estimate`,
/// `total_bytes`), and column shapes. No row data. So a `Vec<TableInfo>`
/// serialized from a real DB (anonymized) replays every decision the field hit
/// (see `catalog_replay`).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
    /// The cursor column init actually RECORDS and RENDERS — the scored top
    /// candidate. `best_cursor_column` (name-preference) is kept for MODE
    /// SELECTION ("is any cursor viable"), where only existence matters; but the
    /// value written into the YAML and the strategy_snapshot must be ONE picker,
    /// or the snapshot records a cursor the config does not contain (bug hunt
    /// 2026-08-08). This is that one picker.
    pub(crate) fn chosen_cursor_column(&self) -> Option<String> {
        crate::init::candidates::cursor_candidates(self)
            .first()
            .map(|c| c.column.clone())
    }

    /// [`chosen_cursor_column`] RESTRICTED to a timestamp column — for
    /// `time_window`, whose `time_column` MUST be a timestamp. An integer
    /// surrogate PK is a valid INCREMENTAL cursor (monotonic ids) and
    /// `cursor_candidates` scores it as one, so `chosen_cursor_column` can return
    /// `id` on a timestamp-less table; scaffolding that as a `time_column`
    /// compares a bigint against a timestamp window at run time — an error on
    /// PostgreSQL, a silently-empty export on MySQL (bug hunt 2026-08-09). Same
    /// scoring/order as `chosen_cursor_column` among the timestamp candidates, so
    /// a table WITH a timestamp gets the same column both modes would; `None`
    /// (no timestamp) drives the honest REVIEW marker instead of a wrong column.
    pub(crate) fn chosen_time_column(&self) -> Option<String> {
        crate::init::candidates::cursor_candidates(self)
            .into_iter()
            .find(|c| is_timestamp_type(&c.data_type))
            .map(|c| c.column)
    }

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

    /// The single-column PK if it is a KEYSET-usable type (see
    /// [`is_keysettable_type`]) — an integer / uuid / string / timestamp / date
    /// PK, but NOT a `decimal`/`numeric` PK (which the planner refuses as a keyset
    /// key). This is the signal that a large NON-INTEGER-PK table can still keyset
    /// (`chunk_by_key`) instead of falling to a full scan. `None` for a composite
    /// PK, no PK, or a decimal-typed PK.
    pub(crate) fn keysettable_pk_column(&self) -> Option<&str> {
        let pk = self.single_pk_column()?;
        let ty = self
            .columns
            .iter()
            .find(|c| c.name == pk)
            .map(|c| c.data_type.as_str())?;
        is_keysettable_type(ty).then_some(pk)
    }

    /// Suggest extraction mode based on row count and available columns.
    pub(crate) fn suggest_mode(&self) -> &'static str {
        if self.row_estimate > 100_000 {
            // A range-chunkable integer column OR a keyset-usable single PK (uuid /
            // string / … — any orderable indexed key) both mean "chunked": the
            // scaffold then routes an integer PK / integer column to range chunking
            // and a non-integer single PK to keyset (`chunk_by_key`). Before this a
            // large uuid/string-PK table with no integer column fell to `full` — an
            // unbounded scan that is not durability-safe on a large table (ADR-0020).
            // A decimal PK is NOT keysettable (planner refuses it), so it stays
            // `full` unless it also has an integer column to range-chunk.
            if self.best_chunk_column().is_some() || self.keysettable_pk_column().is_some() {
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
                // Gate the incremental SUGGESTION on best_cursor_column
                // (timestamp-only): incremental on an integer surrogate PK
                // catches inserts but MISSES updates, so "pulls only changed
                // rows" would be false advice — only suggest it when a real
                // timestamp cursor exists (roast 2026-08-09: the incremental
                // ARM below names chosen_, but this SUGGESTION must stay
                // timestamp-gated, unlike an actual incremental export).
                match self.best_cursor_column() {
                    Some(cursor) => format!(
                        "{base}. NOTE: chunked re-reads the whole table each run — for scheduled \
                         re-runs, `mode: incremental` on '{cursor}' pulls only changed rows"
                    ),
                    None => base,
                }
            }
            "incremental" => match self.chosen_cursor_column() {
                Some(cursor) => format!(
                    "auto: ~{} rows ≥ 100K threshold; chunk column missing, falling back to \
                     incremental on '{cursor}'",
                    fmt_row_estimate(self.row_estimate),
                ),
                // No timestamp candidate: do NOT name a phantom `updated_at` —
                // the scaffold emits a REVIEW marker here, so the rationale must
                // agree (roast 2026-08-09).
                None => format!(
                    "auto: ~{} rows ≥ 100K threshold; chunk column missing — set cursor_column \
                     manually (no timestamp column detected)",
                    fmt_row_estimate(self.row_estimate),
                ),
            },
            "full" => {
                // full is reached TWO ways: below the 100K threshold, OR above it
                // with no usable chunk key / cursor column (e.g. a keyless table, or
                // a Mongo collection whose `_id` isn't surfaced as a chunk key). The
                // message must say WHICH — claiming "below 100K" on a 150K table is a
                // false diagnostic that hides the real reason (no key to page by).
                if self.row_estimate > 100_000 {
                    format!(
                        "auto: ~{} rows ≥ 100K but no chunk key or cursor column available — full scan",
                        fmt_row_estimate(self.row_estimate),
                    )
                } else {
                    format!(
                        "auto: ~{} rows below 100K chunked threshold",
                        fmt_row_estimate(self.row_estimate),
                    )
                }
            }
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

/// Whether a column of this type can be a KEYSET (seek) key — i.e. the keyset
/// cursor can read + compare it (`WHERE key > last ORDER BY key`). MUST mirror the
/// planner's `keyset_keys` restriction (`source::TableIntrospection`): integer /
/// float / string / timestamp / date / uuid are readable; `decimal`/`numeric` (and
/// `money`) are EXCLUDED — the planner refuses a decimal keyset key, so init must
/// not scaffold `chunk_by_key` on one (it would fail the run). Used to decide
/// whether a large NON-INTEGER single-PK table can keyset instead of falling to
/// `full`.
fn is_keysettable_type(t: &str) -> bool {
    if is_integer_type(t) || is_timestamp_type(t) {
        return true;
    }
    let t = t.to_lowercase();
    // uuid, string (char/varchar/text families), and float/real/double — but NOT
    // decimal/numeric/money (the planner's keyset cursor excludes those).
    t.contains("uuid")
        || t.contains("uniqueidentifier")
        || t.contains("char") // char, varchar, nvarchar, character varying, bpchar
        || t.contains("text")
        || t == "float"
        || t == "real"
        || t == "double"
        || t == "double precision"
        || t == "float4"
        || t == "float8"
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
    // `--tls` / `--tls-ca` (#146): the posture for the introspection
    // connection init itself opens AND the `source.tls:` block the scaffold
    // records — one value, both uses, so the generated config connects the
    // same way init just did.
    tls: Option<&crate::config::TlsConfig>,
) -> Result<()> {
    yaml_destination.validate()?;
    let (text, yaml_decimal_review, snapshots) = match format {
        InitFormat::Yaml => init_yaml(
            tls,
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
            if mode_override.is_some() {
                // The discovery artifact reports the DB's shape + a *suggested*
                // mode; it has no per-export `mode:` to override. Silently
                // ignoring --mode made `--discover --mode cdc` byte-identical to
                // `--discover` with no signal.
                eprintln!(
                    "rivet: note: --mode is ignored for --discover (the artifact reports a suggested_mode, not a chosen one); run without --discover to scaffold a YAML with that mode"
                );
            }
            (
                init_discovery_json(tls, source_url, table, schema, filter)?,
                false,
                // `--discover` writes a SURVEY, not a config — there is no
                // strategy decision to record the evidence for.
                Vec::new(),
            )
        }
    };

    match output {
        Some(path) => {
            write_config_output(path, &text)?;
            record_strategy_snapshots(path, &snapshots);
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

/// The "nothing to scaffold" error, filter-aware. When an `--include`/`--exclude`
/// glob is active, an empty result usually means the globs filtered everything
/// out (the schema has tables) — blaming `--schema`/privileges then sends the
/// operator down the wrong path. Point at the filter first.
fn no_tables_error(filter: &TableFilter) -> anyhow::Error {
    if !filter.include.is_empty() || !filter.exclude.is_empty() {
        anyhow::anyhow!(
            "No tables/views matched the --include/--exclude globs (or the schema is empty). \
             Check the globs — the same source without filters may discover tables — and also \
             verify --schema and privileges."
        )
    } else {
        anyhow::anyhow!("No tables or views found (check --schema and privileges)")
    }
}

/// Write the scaffold to `-o <path>`, naming the path + cause on failure. The
/// bare `std::fs::write` error ("No such file or directory (os error 2)") named
/// neither the path nor the operation (dogfood LOW).
fn write_config_output(path: &str, text: &str) -> Result<()> {
    std::fs::write(path, text).map_err(|e| {
        anyhow::anyhow!(
            "init: could not write config to '{path}': {e} \
             (check the parent directory exists and is writable; `-o` takes a file path, not a directory)"
        )
    })
}

/// Resolve the effective schema for a single `--table`, honoring `--schema`.
/// `--table schema.name` embeds the schema; `--schema` supplies it when the
/// table is bare. Returns `(Some(schema), name)` when a schema is known, or
/// `(None, name)` to let the engine default apply (PG `public`, MSSQL `dbo`).
/// Both an embedded schema AND a differing `--schema` is a conflict — silently
/// dropping one let a nonexistent schema look valid (the config for `x.users`
/// and `--schema x --table users` came out identical to no-schema).
fn resolve_single_table_schema<'a>(
    table: &'a str,
    schema_flag: Option<&str>,
) -> Result<(Option<String>, &'a str)> {
    let (parsed_schema, name) = yaml_scaffold::parse_table(table);
    let has_embedded = table.contains('.');
    match schema_flag.map(str::trim).filter(|s| !s.is_empty()) {
        Some(s) if has_embedded && s != parsed_schema => anyhow::bail!(
            "init: --table '{table}' already names schema '{parsed_schema}', but --schema is \
             '{s}' — remove one so they agree"
        ),
        // Bare `--table` + `--schema`: the flag supplies the schema.
        Some(s) if !has_embedded => Ok((Some(s.to_string()), name)),
        // Embedded schema (matching or no flag), or no flag at all.
        _ => Ok((has_embedded.then_some(parsed_schema), name)),
    }
}

/// Introspect one `--table` (single-table init), honoring `--schema`. Shared by
/// the YAML and discovery-JSON paths so the schema wiring can't drift between
/// them. For MySQL, `--schema` selects the database via `USE`; for PG/MSSQL it
/// is the introspection schema (defaulting to `public`/`dbo`).
fn introspect_single_table(
    tls: Option<&crate::config::TlsConfig>,
    source_url: &str,
    table: &str,
    schema_flag: Option<&str>,
) -> Result<TableInfo> {
    let (eff_schema, table_name) = resolve_single_table_schema(table, schema_flag)?;
    Ok(match source_type(source_url)? {
        "postgres" => {
            let mut client = postgres::connect(source_url, tls)?;
            postgres::introspect(
                &mut client,
                eff_schema.as_deref().unwrap_or("public"),
                table_name,
            )?
        }
        "mysql" => {
            let mut conn = mysql::connect(source_url, tls)?;
            if let Some(db) = eff_schema.as_deref() {
                // Guard (bughunt HIGH): `--schema` selecting a DIFFERENT database
                // than the URL would introspect THAT db (via USE), but the
                // scaffold connects through the URL and emits an UNQUALIFIED
                // `table:` — so `rivet run` reads the URL's database instead, a
                // silent wrong-database export (dbA has a `users` too → wrong
                // data with dbB-derived columns; else run fails right after init
                // "verified" it). The config can't carry a cross-db table today,
                // so refuse: the database belongs in the URL.
                // Only a genuine CROSS-db mismatch is refused: the URL carries a
                // db AND it differs from --schema. A db-LESS URL (url_db None) is
                // the documented "database name if missing from the URL" use —
                // --schema legitimately provides the db there, so it must NOT bail
                // (#20 bughunt: `None != Some(db)` wrongly refused it).
                if let Some(url_db) = mysql::resolve_database_for_listing(source_url, None).ok()
                    && url_db != db
                {
                    anyhow::bail!(
                        "init: --schema '{db}' selects a different MySQL database than the source \
                         URL ('{url_db}') — the generated config connects via the URL and would \
                         export the URL's database, not '{db}'. Put the database in the URL instead \
                         (mysql://…/{db})."
                    );
                }
                mysql::use_database(&mut conn, db)?;
            }
            mysql::introspect(&mut conn, table_name)?
        }
        "mssql" => {
            let mut conn = mssql::connect(source_url, tls)?;
            mssql::introspect(
                &mut conn,
                &mssql_table_schema(eff_schema.as_deref().unwrap_or("public")),
                table_name,
            )?
        }
        "mongo" => {
            // #12 bughunt: --schema was silently ignored for Mongo (the cross-db
            // guard the SQL engines got was absent), so an operator scoping to a
            // schema got the URL's database instead. Mongo has no schema namespace
            // — refuse rather than mislead; the database lives in the URL.
            reject_mongo_schema(schema_flag)?;
            let conn = mongo::connect(source_url, tls)?;
            mongo::introspect(&conn, table_name)?
        }
        _ => unreachable!(),
    })
}

/// MongoDB has no schema namespace (collections live directly in a database), so
/// an explicit `--schema` cannot be honoured — it was silently ignored, exporting
/// the URL's database instead of what the operator asked for (#12 bughunt). Refuse
/// it loudly; the database belongs in the connection URL.
fn reject_mongo_schema(schema_flag: Option<&str>) -> Result<()> {
    if schema_flag.map(str::trim).is_some_and(|s| !s.is_empty()) {
        anyhow::bail!(
            "init: --schema is not supported for MongoDB — a collection has no schema \
             namespace. Put the database in the connection URL instead (mongodb://…/<db>)."
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // mirrors `init`'s own surface; a params
// struct here would be a second InitYamlDestination
fn init_yaml(
    tls: Option<&crate::config::TlsConfig>,
    source_url: &str,
    provenance: &SourceProvenance,
    table: Option<&str>,
    schema: Option<&str>,
    dest: &InitYamlDestination,
    filter: &TableFilter,
    mode_override: Option<&str>,
) -> Result<(String, bool, Vec<crate::state::StrategySnapshot>)> {
    if let Some(t) = table {
        let info = introspect_single_table(tls, source_url, t, schema)?;
        let hint = yaml_scaffold::table_has_unbounded_decimal_columns(&info);
        let snaps = vec![snapshot_of(&info, mode_override)];
        let yaml = yaml_scaffold::generate_config(
            &info,
            source_url,
            provenance,
            dest,
            mode_override,
            tls,
        )?;
        return Ok((yaml, hint, snaps));
    }
    let infos = introspect_all(tls, source_url, schema, filter)?;
    if infos.is_empty() {
        return Err(no_tables_error(filter));
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
        tls,
    )?;
    let snaps = infos
        .iter()
        .map(|i| snapshot_of(i, mode_override))
        .collect();
    Ok((yaml, hint, snaps))
}

/// The decision plus the EVIDENCE it was made from, for the state store.
///
/// Every field here is something `init` already read from the catalog to make
/// the choice; before this they were discarded the moment the YAML was written.
fn snapshot_of(info: &TableInfo, mode_override: Option<&str>) -> crate::state::StrategySnapshot {
    let d = yaml_scaffold::decided_strategy(info, mode_override);
    crate::state::StrategySnapshot {
        export_name: info.table.clone(),
        source_schema: (!info.schema.is_empty()).then(|| info.schema.clone()),
        source_table: info.table.clone(),
        row_estimate: (info.row_estimate > 0).then_some(info.row_estimate),
        total_bytes: info.total_bytes,
        avg_row_bytes: info.avg_row_bytes(),
        chosen_mode: d.mode,
        strategy_kind: Some(d.kind.to_string()),
        key_column: d.key_column,
        chunk_size: d.chunk_size,
    }
}

fn init_discovery_json(
    tls: Option<&crate::config::TlsConfig>,
    source_url: &str,
    table: Option<&str>,
    schema: Option<&str>,
    filter: &TableFilter,
) -> Result<String> {
    let (infos, scope) = if let Some(t) = table {
        let info = introspect_single_table(tls, source_url, t, schema)?;
        let scope = match source_type(source_url)? {
            "postgres" => format!("table \"{}\".\"{}\"", info.schema, info.table),
            "mysql" => format!("table `{}`", info.table),
            "mssql" => format!("table [{}].[{}]", info.schema, info.table),
            "mongo" => format!("collection {}", info.table),
            _ => unreachable!(),
        };
        (vec![info], scope)
    } else {
        let infos = introspect_all(tls, source_url, schema, filter)?;
        if infos.is_empty() {
            return Err(no_tables_error(filter));
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
        tls_mode: tls.map(|t| {
            match t.mode {
                crate::config::TlsMode::Disable => "disable",
                crate::config::TlsMode::Require => "require",
                crate::config::TlsMode::VerifyCa => "verify-ca",
                crate::config::TlsMode::VerifyFull => "verify-full",
            }
            .to_string()
        }),
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
    tls: Option<&crate::config::TlsConfig>,
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
            let mut client = postgres::connect(source_url, tls)?;
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
            // Guard (bughunt MED, sibling of the single-table HIGH): whole-schema
            // MySQL lists tables from `db` (which honors --schema) but introspects
            // via DATABASE() (the URL's db) — and the generated config connects
            // through the URL, so a --schema pointing at a DIFFERENT database
            // lists/describes one db and exports another. Refuse: the database
            // belongs in the URL, which the config actually uses.
            // Refuse only a genuine CROSS-db mismatch (URL carries a db AND it
            // differs). A db-LESS URL + --schema is the documented "database name
            // if missing from the URL" use and must be allowed (#20 bughunt).
            if schema.map(str::trim).is_some_and(|s| !s.is_empty())
                && let Some(url_db) = mysql::resolve_database_for_listing(source_url, None).ok()
                && url_db != db
            {
                anyhow::bail!(
                    "init: --schema '{db}' selects a different MySQL database than the source URL \
                     ('{url_db}') — `rivet init`/`run` connect via the URL and would list and export \
                     the URL's database, not '{db}'. Put the database in the URL instead (mysql://…/{db})."
                );
            }
            // One pooled connection for the whole scan — a fresh Pool per
            // table would mean N+1 TCP+auth(+TLS) handshakes on large schemas.
            let mut conn = mysql::connect(source_url, tls)?;
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
            let mut conn = mssql::connect(source_url, tls)?;
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
            reject_mongo_schema(schema)?;
            let conn = mongo::connect(source_url, tls)?;
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

/// Persist the shape each strategy was chosen from, into the state store the
/// runs will use — the one beside the config just written.
///
/// Best-effort and NEVER fatal: `init`'s product is the config, and a scaffold
/// that fails because an audit row could not be written would be trading the
/// thing the user asked for against a convenience. A failure is logged at
/// `debug`, matching how the run path treats its own harm-metric write.
///
/// Only on the `-o <path>` route, deliberately: without an output path there is
/// no config, so there is no `.rivet_state.db` beside one and nowhere the run
/// would later look. Printing a scaffold to stdout leaves no trace by design.
fn record_strategy_snapshots(config_path: &str, snapshots: &[crate::state::StrategySnapshot]) {
    if snapshots.is_empty() {
        return;
    }
    let store = match crate::state::StateStore::open(config_path) {
        Ok(s) => s,
        Err(e) => {
            log::debug!("init: strategy snapshot skipped (state store unavailable): {e:#}");
            return;
        }
    };
    for s in snapshots {
        if let Err(e) = store.record_strategy_snapshot(s) {
            log::debug!(
                "init: strategy snapshot for '{}' not written (informational): {e:#}",
                s.export_name
            );
        }
    }
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

    /// A3/A2 (roast 2026-08-09): where best_cursor_column (name-preference) and
    /// chosen_cursor_column (scored) DIVERGE, every user-facing surface that
    /// names the cursor must name the CHOSEN one — the column actually written as
    /// cursor_column — not the name-preferred phantom. Fixture: `updated_at` is
    /// nullable (name-preferred but -10) and `modified_at` is not-null (scores
    /// higher). RED against `mode_rationale` reading `best_cursor_column`.
    #[test]
    fn mode_rationale_names_the_chosen_cursor_not_the_name_preferred_one() {
        let nullable_ts = |name: &str| ColumnInfo {
            name: name.into(),
            data_type: "timestamp".into(),
            is_primary_key: false,
            is_nullable: true,
            numeric_precision: None,
            numeric_scale: None,
        };
        let info = make_table(
            500_000,
            vec![
                col("payload", "text", false),
                nullable_ts("updated_at"),
                col("modified_at", "timestamp", false),
            ],
        );
        // the divergence this test rests on (fixture is not inert)
        assert_eq!(info.best_cursor_column(), Some("updated_at"));
        assert_eq!(info.chosen_cursor_column().as_deref(), Some("modified_at"));
        // the incremental rationale is printed next to `cursor_column: modified_at`
        let r = info.mode_rationale("incremental");
        assert!(
            r.contains("modified_at"),
            "must name the chosen cursor: {r}"
        );
        assert!(
            !r.contains("updated_at"),
            "must NOT name the name-preferred phantom: {r}"
        );
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
    fn full_rationale_distinguishes_below_threshold_from_no_key_at_scale() {
        // Below 100K → the honest "below threshold" message.
        let small = make_table(500, vec![col("id", "bigint", true)]);
        assert_eq!(small.suggest_mode(), "full");
        assert!(
            small.mode_rationale("full").contains("below 100K"),
            "small table keeps the below-threshold message"
        );
        // ABOVE 100K but NO chunk key and NO cursor (a keyless table, or a Mongo
        // collection whose _id isn't surfaced as a key) → full, but the message must
        // NOT claim "below 100K" — the dogfooding bug read "~150K rows below 100K
        // chunked threshold" on a 150K Mongo collection. It must name the real cause.
        let big_keyless = make_table(150_000, vec![col("label", "text", false)]);
        assert_eq!(big_keyless.suggest_mode(), "full");
        let r = big_keyless.mode_rationale("full");
        assert!(
            !r.contains("below 100K"),
            "150K must not say below 100K: {r}"
        );
        assert!(r.contains("no chunk key"), "must name the real reason: {r}");
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
    fn init_keysets_a_large_non_int_pk_but_never_a_decimal_pk() {
        // The fix: a large table with a NON-INTEGER single-column PK of a
        // keyset-usable type (uuid / string) scaffolds keyset (chunk_by_key), not a
        // full scan — matching an integer PK. A DECIMAL PK is NOT keysettable (the
        // planner refuses it), so it stays `full`. Dogfood-confirmed on PG/MySQL/MSSQL.
        let uuid_pk = make_table(
            150_000,
            vec![col("id", "uuid", true), col("n", "int", false)],
        );
        assert_eq!(uuid_pk.suggest_mode(), "chunked");
        assert_eq!(uuid_pk.keysettable_pk_column(), Some("id"));

        let str_pk = make_table(
            150_000,
            vec![col("code", "varchar", true), col("n", "int", false)],
        );
        assert_eq!(str_pk.suggest_mode(), "chunked");
        assert_eq!(str_pk.keysettable_pk_column(), Some("code"));

        // Decimal PK: keyset refused at plan time → init must NOT keyset it.
        let dec_pk = make_table(
            150_000,
            vec![
                ColumnInfo {
                    name: "id".into(),
                    data_type: "decimal".into(),
                    is_primary_key: true,
                    is_nullable: false,
                    numeric_precision: Some(20),
                    numeric_scale: Some(0),
                },
                col("name", "text", false),
            ],
        );
        assert_eq!(
            dec_pk.keysettable_pk_column(),
            None,
            "decimal PK not keysettable"
        );
        assert_eq!(
            dec_pk.suggest_mode(),
            "full",
            "decimal PK (no int col) stays full"
        );

        // Below the row threshold, even a keysettable PK stays full (no chunking overhead).
        let small_uuid = make_table(
            50_000,
            vec![col("id", "uuid", true), col("n", "int", false)],
        );
        assert_eq!(small_uuid.suggest_mode(), "full");
    }

    /// The snapshot's decision must be the one the YAML actually carries.
    ///
    /// `decided_strategy` feeds the strategy snapshot; `generate_config` writes
    /// the config. They share the same underlying rules, and this asserts they
    /// still AGREE — by parsing what the scaffold emitted, not by re-deriving
    /// the rule a second time. A snapshot that describes a choice the config did
    /// not make is worse than no snapshot: it is an audit trail that lies.
    ///
    /// The matrix crosses the three shapes the scaffold can pick, so a change to
    /// any one arm is caught rather than only the one an author remembered.
    #[test]
    fn decided_strategy_agrees_with_the_generated_yaml() {
        let cases: Vec<(&str, TableInfo)> = vec![
            (
                "single int PK, large → keyset",
                make_table(
                    2_000_000,
                    vec![col("id", "bigint", true), col("name", "text", false)],
                ),
            ),
            (
                "no single PK, int column, large → range",
                make_table(
                    2_000_000,
                    vec![col("order_id", "bigint", false), col("name", "text", false)],
                ),
            ),
            (
                "small → full",
                make_table(10, vec![col("id", "bigint", true)]),
            ),
        ];
        for (label, info) in cases {
            let d = yaml_scaffold::decided_strategy(&info, None);
            let yaml = yaml_scaffold::generate_config(
                &info,
                "postgresql://localhost/db",
                &super::SourceProvenance::Inline,
                &Default::default(),
                None,
                None,
            )
            .expect("scaffold");
            // COMMENTS STRIPPED. The keyset scaffold's hint line mentions
            // `chunk_column:` as the alternative to consider, and matching raw
            // text read that as the config choosing range — the first run of
            // this test failed on exactly that. Compare against what the config
            // SAYS, not what it discusses.
            let yaml: String = yaml
                .lines()
                .filter(|l| !l.trim_start().starts_with('#'))
                .collect::<Vec<_>>()
                .join("\n");

            assert!(
                yaml.contains(&format!("mode: {}", d.mode)),
                "{label}: snapshot says mode {:?}, YAML says otherwise:\n{yaml}",
                d.mode
            );
            // The KIND is not written as a field — it is implied by which key
            // line the scaffold emitted, so that is what gets compared.
            let (expect, forbid) = match d.kind {
                "keyset" => ("chunk_by_key:", Some("chunk_column:")),
                "range" => ("chunk_column:", Some("chunk_by_key:")),
                _ => ("mode:", None),
            };
            assert!(
                yaml.contains(expect),
                "{label}: snapshot kind {:?} implies `{expect}` in the YAML:\n{yaml}",
                d.kind
            );
            if let Some(f) = forbid {
                assert!(
                    !yaml.contains(f),
                    "{label}: snapshot kind {:?} but the YAML also carries `{f}`:\n{yaml}",
                    d.kind
                );
            }
            if let Some(k) = &d.key_column {
                assert!(
                    yaml.contains(k),
                    "{label}: snapshot names key {k:?}, absent from the YAML:\n{yaml}"
                );
            }
        }
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
    fn reject_mongo_schema_refuses_explicit_schema_but_allows_absent() {
        // #12 bughunt: --schema was silently ignored for Mongo. It must be refused
        // (a collection has no schema namespace), while absent/blank is fine.
        assert!(super::reject_mongo_schema(Some("mydb")).is_err());
        assert!(super::reject_mongo_schema(Some("  ")).is_ok());
        assert!(super::reject_mongo_schema(None).is_ok());
        let err = super::reject_mongo_schema(Some("mydb"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("not supported for MongoDB"), "got: {err}");
    }

    #[test]
    fn resolve_single_table_schema_honors_schema_flag_and_detects_conflict() {
        // #dogfood MED: `--schema` was silently dropped when `--table` was given,
        // so a nonexistent schema looked valid (identical output). Bare table +
        // --schema now folds the schema in.
        assert_eq!(
            resolve_single_table_schema("users", Some("myschema")).unwrap(),
            (Some("myschema".to_string()), "users")
        );
        // No --schema on a bare table → None (engine default applies downstream).
        assert_eq!(
            resolve_single_table_schema("users", None).unwrap(),
            (None, "users")
        );
        // Embedded schema wins; a matching --schema is fine.
        assert_eq!(
            resolve_single_table_schema("sales.orders", Some("sales")).unwrap(),
            (Some("sales".to_string()), "orders")
        );
        assert_eq!(
            resolve_single_table_schema("sales.orders", None).unwrap(),
            (Some("sales".to_string()), "orders")
        );
        // Embedded schema AND a DIFFERING --schema is a loud conflict, not a
        // silent drop.
        let err = resolve_single_table_schema("sales.orders", Some("other"))
            .expect_err("conflicting schema must error");
        let msg = format!("{err}");
        assert!(
            msg.contains("already names schema 'sales'") && msg.contains("--schema is 'other'"),
            "conflict must name both schemas: {msg}"
        );
        // A blank/whitespace --schema is treated as absent, not a conflict.
        assert_eq!(
            resolve_single_table_schema("sales.orders", Some("  ")).unwrap(),
            (Some("sales".to_string()), "orders")
        );
    }

    #[test]
    fn no_tables_error_points_at_the_filter_when_globs_are_active() {
        // #dogfood LOW: an --include/--exclude that filters everything out used
        // to emit the schema/privileges message, sending the operator down the
        // wrong path. With a filter active the error names the globs first.
        let filtered = no_tables_error(&TableFilter {
            include: vec!["zzz_nomatch_*".into()],
            exclude: vec![],
        });
        assert!(
            format!("{filtered}").contains("--include/--exclude"),
            "filtered-out error must name the globs: {filtered}"
        );
        let exclude_all = no_tables_error(&TableFilter {
            include: vec![],
            exclude: vec!["*".into()],
        });
        assert!(format!("{exclude_all}").contains("--include/--exclude"));
        // No filter → the original schema/privileges message.
        let unfiltered = no_tables_error(&TableFilter::default());
        assert!(
            format!("{unfiltered}").contains("check --schema and privileges"),
            "unfiltered error keeps the schema/privileges hint: {unfiltered}"
        );
    }

    #[test]
    fn write_config_output_names_the_path_and_op_on_failure() {
        // #dogfood LOW: a bad `-o` path emitted a bare "No such file or directory
        // (os error 2)" naming neither the path nor the failed operation.
        let err = write_config_output("no_such_dir_xyz/sub/cfg.yaml", "source: {}\n")
            .expect_err("a missing parent dir must fail");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("no_such_dir_xyz/sub/cfg.yaml"),
            "must name the path: {msg}"
        );
        assert!(
            msg.contains("could not write config"),
            "must name the operation: {msg}"
        );
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
        // No usable single PK (a non-PK uuid) so `suggest_mode` falls back to
        // incremental on the timestamp cursor. (A uuid PRIMARY KEY would now keyset
        // — see `suggest_mode` / `keysettable_pk_column`.)
        let info = make_table(
            300_000,
            vec![
                col("key", "uuid", false),
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
