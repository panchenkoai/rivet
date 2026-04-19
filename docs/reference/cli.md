# CLI Reference

## Global

```
rivet [COMMAND] [OPTIONS]
```

```bash
rivet --version       # print version
rivet --help          # show help
```

---

## `rivet run`

Run export jobs defined in a config file.

```bash
rivet run --config <PATH> [OPTIONS]
```

| Flag | Short | Type | Description |
|------|-------|------|-------------|
| `--config` | `-c` | string | Path to YAML config file **(required)** |
| `--export` | `-e` | string | Run only a specific export by name |
| `--validate` | | bool | Validate output file row count after writing |
| `--reconcile` | | bool | Run `COUNT(*)` on source query and compare with exported rows |
| `--resume` | | bool | Resume a chunked export with `chunk_checkpoint: true` |
| `--parallel-exports` | | bool | Run all exports concurrently (ignored with `--export`) |
| `--parallel-export-processes` | | bool | Run each export as a separate child process |
| `--param` | `-p` | KEY=VALUE | Query parameter (repeatable). Substitutes `${key}` in queries |

### Examples

```bash
# Basic run
rivet run -c my_export.yaml

# Run with validation and reconciliation
rivet run -c my_export.yaml --validate --reconcile

# Run a single export
rivet run -c my_export.yaml -e orders_daily

# Resume interrupted chunked export
rivet run -c my_export.yaml -e big_table --resume

# Parameterized query
rivet run -c my_export.yaml -p region=us-east -p year=2026

# Parallel exports (all at once)
rivet run -c my_export.yaml --parallel-exports
```

---

## `rivet plan`

![Plan / apply walkthrough](../gifs/plan-apply.gif)

Generate a sealed execution plan artifact — no data is exported.

`rivet plan` runs preflight analysis (row estimate, index check, sparsity), computes chunk boundaries for chunked exports, snapshots the current cursor for incremental exports, and writes everything to a `PlanArtifact` JSON file. The artifact can be reviewed, committed, stored as a CI artifact, or passed to `rivet apply`.

```bash
rivet plan --config <PATH> [OPTIONS]
```

| Flag | Short | Type | Default | Description |
|------|-------|------|---------|-------------|
| `--config` | `-c` | string | — | Path to YAML config file **(required)** |
| `--export` | `-e` | string | all | Plan only a specific export |
| `--param` | `-p` | KEY=VALUE | — | Query parameter (repeatable) |
| `--output` | `-o` | string | stdout | Write plan JSON to this file |
| `--format` | | `pretty`\|`json` | `pretty` | `pretty` prints a human summary; `json` writes the full artifact |

### Examples

```bash
# Human-readable summary (no file written)
rivet plan -c rivet.yaml

# Write full JSON artifact to a file
rivet plan -c rivet.yaml --format json --output plan.json

# Plan a single export
rivet plan -c rivet.yaml -e orders --format json -o orders_plan.json
```

### Pretty output (example)

```
  Plan ID  : a1b2c3d4e5f6...
  Created  : 2026-04-14 10:00:00 UTC
  Expires  : 2026-04-15 10:00:00 UTC
  Export   : orders
  Strategy : chunked
  Chunks   : 42
  Row est. : ~2,100,000
  Verdict  : Acceptable
  Profile  : balanced
  Warnings :
    • sparse id range: ~12% fill
  Output   : local → ./out
  Format   : parquet + zstd
```

### Plan artifact structure

The JSON artifact (`--format json`) contains:

```json
{
  "rivet_version": "0.3.3",
  "plan_id": "a1b2c3d4...",
  "created_at": "2026-04-14T10:00:00Z",
  "expires_at": "2026-04-15T10:00:00Z",
  "export_name": "orders",
  "strategy": "chunked",
  "plan_fingerprint": "0123456789abcdef",
  "resolved_plan": { ... },
  "computed": {
    "chunk_ranges": [[1, 50000], [50001, 100000], "..."],
    "chunk_count": 42,
    "cursor_snapshot": null,
    "row_estimate": 2100000
  },
  "diagnostics": {
    "verdict": "Acceptable",
    "warnings": ["sparse id range: ~12% fill"],
    "recommended_profile": "balanced"
  }
}
```

> **Security note**: `resolved_plan` embeds the full source connection config including credentials. Treat plan files with the same care as your rivet config file.

---

## `rivet apply`

Execute a previously-generated plan artifact.

`rivet apply` deserializes the artifact, validates staleness and cursor integrity, then executes the export using the pre-computed chunk boundaries from the artifact — no `SELECT min/max` queries are run against the source.

```bash
rivet apply <PLAN_FILE> [OPTIONS]
```

| Argument/Flag | Type | Description |
|---|---|---|
| `PLAN_FILE` | string | Path to plan JSON file **(required)** |
| `--force` | bool | Skip staleness check (allow plans older than 24 h) |

### Staleness rules

| Plan age | Behavior |
|----------|----------|
| < 1 hour | Proceeds silently |
| 1–24 hours | Warns and proceeds |
| > 24 hours | Rejects — use `--force` to override |

### Cursor drift (Incremental exports)

If another `rivet run` completed after the plan was generated, the cursor will have advanced. `rivet apply` detects this and rejects the artifact to prevent re-exporting already-exported rows. Regenerate with `rivet plan`.

### Examples

```bash
# Apply the plan
rivet apply plan.json

# Apply an old plan (override staleness check)
rivet apply plan.json --force
```

### What apply does NOT do

- Does not re-read the config file
- Does not re-run preflight queries
- Does not recompute chunk boundaries (uses pre-computed ranges from the artifact)
- Does not enforce preflight verdict (diagnostics are advisory — see ADR-0005)

### State location

`rivet apply` opens `.rivet_state.db` from the directory containing the plan file. Place the plan file alongside the config file, or in the same directory, to ensure the correct state database is used.

---

## `rivet reconcile`

![Chunked + reconcile + repair walkthrough](../gifs/reconcile-repair.gif)

Partition/window reconciliation — re-runs per-chunk `COUNT(*)` on the source and compares with the stored per-chunk row counts from the last run. Surfaces **matches**, **mismatches**, and **repair candidates** without re-exporting data (Epic F).

```bash
rivet reconcile --config <PATH> --export <NAME> [OPTIONS]
```

| Flag | Short | Type | Description |
|---|---|---|---|
| `--config` | `-c` | string | Path to YAML config file **(required)** |
| `--export` | `-e` | string | Export name to reconcile **(required)** |
| `--format` | | `pretty` \| `json` | Output format (default `pretty`) |
| `--output` | `-o` | string | Write JSON report to this file (use with `--format json`) |
| `--param` | `-p` | KEY=VALUE | Query parameter (repeatable) |

### Scope (v1)

- **Chunked exports** — supported. Requires a previous run with `chunk_checkpoint: true` so per-chunk ranges and row counts are persisted in `.rivet_state.db`.
- **Time-window** — returns an error ("use chunked with `chunk_by_days`" for partition reconcile).
- **Snapshot / Incremental** — no natural partitions; use `rivet run --reconcile` for a whole-export count check.

### What it does

For each completed chunk task from the latest chunk run:

1. Rebuilds the exact chunk query the pipeline used (same `WHERE` predicate, same dense/range shape — `build_chunk_query_sql`).
2. Runs `SELECT COUNT(*) FROM (<chunk_query>) AS _rc`.
3. Compares the source count with the stored `rows_written` for that chunk.

Each partition is classified as:

- `match` — source and exported counts are equal.
- `mismatch` — counts differ; partition is a repair candidate (note includes `diff`).
- `unknown` — one of the counts is unavailable (chunk never completed, unparseable chunk keys); also a repair candidate.

### Examples

```bash
# Human-readable summary
rivet reconcile -c my_export.yaml -e orders

# JSON report to file
rivet reconcile -c my_export.yaml -e orders --format json -o reconcile.json
```

Reports are **advisory** — same policy as prioritization (ADR-0006) and plan artifacts (ADR-0005). They surface what needs repair; they do not re-export on their own.

---

## `rivet repair`

Targeted repair of chunks flagged by reconcile. Prints a `RepairPlan` by default; with `--execute`, re-exports only the flagged chunk ranges (Epic H, [ADR-0009 RR1–RR8](../adr/0009-reconcile-and-repair-contracts.md)).

```bash
rivet repair --config <PATH> --export <NAME> [OPTIONS]
```

| Flag | Short | Type | Description |
|------|-------|------|-------------|
| `--config` | `-c` | string | Path to YAML config file **(required)** |
| `--export` | `-e` | string | Export name to repair (must be `mode: chunked`) **(required)** |
| `--report` | | path | Path to a reconcile JSON report (from `rivet reconcile --format json`). Omit to run reconcile in-process against the latest chunk run |
| `--execute` | | bool | Actually re-export the flagged chunk ranges. Without this flag, the plan is printed and nothing is executed (RR2) |
| `--format` | | `pretty` \| `json` | Output format for the plan / post-execute report (default `pretty`) |
| `--output` | `-o` | string | Write plan / report JSON to this file (with `--format json`) |
| `--param` | `-p` | KEY=VALUE | Query parameter (repeatable) |

### Examples

```bash
# Dry run from the latest reconcile — prints the plan, nothing executes
rivet repair -c my_export.yaml -e orders

# Dry run from a saved reconcile report
rivet repair -c my_export.yaml -e orders --report reconcile.json

# Execute — re-runs only the flagged chunks
rivet repair -c my_export.yaml -e orders --report reconcile.json --execute
```

### What `--execute` does and does not do

- Re-runs only the flagged chunk ranges via `ChunkSource::Precomputed` — same SQL shape as extraction and reconcile (RR3).
- Writes **new** files alongside originals with `<export>_<ts>_chunk<idx>.<ext>` (RR5). Rivet does **not** delete or overwrite prior files. Downstream deduplication (or a versioned output prefix) is the operator's responsibility.
- Leaves `last_committed_*` untouched (RR4) — repair is corrective, not commitment. `last_verified_*` re-advances only if a subsequent clean `rivet reconcile` runs.

---

## `rivet check`

Preflight analysis: diagnose source health, estimate row counts, check indexes, recommend tuning.

```bash
rivet check --config <PATH> [OPTIONS]
```

| Flag | Short | Type | Description |
|------|-------|------|-------------|
| `--config` | `-c` | string | Path to YAML config file **(required)** |
| `--export` | `-e` | string | Check only a specific export |
| `--param` | `-p` | KEY=VALUE | Query parameter (repeatable) |

### Example

```bash
rivet check -c my_export.yaml
```

Output includes: table existence, estimated row count, index analysis, tuning recommendation.

---

## `rivet doctor`

Verify source and destination connectivity/auth before running exports.

```bash
rivet doctor --config <PATH>
```

| Flag | Short | Type | Description |
|------|-------|------|-------------|
| `--config` | `-c` | string | Path to YAML config file **(required)** |

### Example

```bash
rivet doctor -c my_export.yaml
```

Output:

```
[OK] Source postgres://host:5432/db — connected, version 16.2
[OK] Destination './output' — directory writable
[OK] Destination 's3://bucket/prefix/' — bucket accessible
```

---

## `rivet init`

Generate a YAML config scaffold (or a machine-readable discovery artifact) by connecting to PostgreSQL or MySQL and introspecting tables (read-only). Does **not** run an export.

```bash
rivet init (--source <URL> | --source-env <ENV_VAR> | --source-file <PATH>)
           [--table <NAME>] [--schema <NAME>] [-o <PATH>] [--discover]
```

Exactly one of `--source`, `--source-env`, `--source-file` must be provided (enforced by the argument group).

| Flag | Short | Type | Description |
|------|-------|------|-------------|
| `--source` | | string | Connection URL: `postgresql://` or `mysql://`. **Visible in shell history / `ps`** — avoid in production |
| `--source-env` | | env var name | Name of an env var that holds the URL (e.g. `DATABASE_URL`). URL never hits the command line. **Recommended.** |
| `--source-file` | | path | Path to a file containing just the URL on one line. Credentials stay on disk |
| `--table` | | string | Single table; optional `schema.table` on PostgreSQL. Omit to scaffold **all** tables/views in a Postgres schema or MySQL database |
| `--schema` | | string | **PostgreSQL:** schema to list (default `public`). **MySQL:** database name if not in the URL, or override URL database |
| `--output` | `-o` | string | Write output to file (default: print to stdout) |
| `--discover` | | bool | Emit a machine-readable JSON discovery artifact instead of YAML — includes ranked cursor/chunk candidates, row estimates, on-disk sizes, and coalesce-fallback hints |

**Examples**

```bash
# One table → one export block
rivet init --source-env DATABASE_URL --table orders -o rivet.yaml

# PostgreSQL: entire schema (default public)
rivet init --source-env DATABASE_URL --schema public -o all_public.yaml

# MySQL: entire database from URL path
rivet init --source-file /run/secrets/mysql_url -o all_mydb.yaml

# JSON discovery artifact — ranked cursor/chunk candidates per table
rivet init --source-env DATABASE_URL --schema public --discover -o discovery.json
```

Narrative guide, heuristics, and Docker Compose examples: **[init.md](init.md)**.

---

## `rivet metrics`

Show export run history (duration, row count, file size, status).

```bash
rivet metrics --config <PATH> [OPTIONS]
```

| Flag | Short | Type | Default | Description |
|------|-------|------|---------|-------------|
| `--config` | `-c` | string | — | Config file **(required)** |
| `--export` | `-e` | string | all | Filter by export name |
| `--last` | `-l` | integer | 20 | Number of recent runs to show |

### Example

```bash
rivet metrics -c my_export.yaml --last 10
rivet metrics -c my_export.yaml -e orders_daily
```

---

## `rivet state`

Manage export state (cursors, file manifests, chunk checkpoints).

### `rivet state show`

Show current cursor state for all incremental exports.

```bash
rivet state show --config <PATH>
```

### `rivet state reset`

Reset the cursor for a specific export (next run will re-export all rows).

```bash
rivet state reset --config <PATH> --export <NAME>
```

### `rivet state files`

List files produced by exports.

```bash
rivet state files --config <PATH> [--export <NAME>] [--last <N>]
```

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--export` | `-e` | all | Filter by export name |
| `--last` | `-l` | 50 | Number of recent files |

### `rivet state chunks`

Show chunk checkpoint status for a chunked export.

```bash
rivet state chunks --config <PATH> --export <NAME>
```

### `rivet state reset-chunks`

Clear persisted chunk plans for a chunked export (to re-export from scratch).

```bash
rivet state reset-chunks --config <PATH> --export <NAME>
```

### `rivet state progression`

Show explicit **committed** and **verified** export boundaries (Epic G / [ADR-0008](../adr/0008-export-progression.md)).

```bash
rivet state progression --config <PATH> [--export <NAME>]
```

| Column | Meaning |
|---|---|
| `COMM MODE` / `COMMITTED` | Strategy (`incremental` / `chunked`) and boundary value (cursor string or `chunk #N`) durably committed to the destination |
| `COMMITTED AT` | UTC timestamp of the committing run |
| `VERI MODE` / `VERIFIED` | Same shape, but only advanced by a full-match `rivet reconcile` (zero mismatches, zero unknowns) |

The progression table is **advisory**: it does not gate `rivet run`, `rivet apply`, or `rivet reconcile`. Consumers are operators and external monitoring.

---

## `rivet completions`

Generate shell completion scripts.

```bash
rivet completions <SHELL>
```

| Shell | Command |
|-------|---------|
| Bash | `rivet completions bash > ~/.local/share/bash-completion/completions/rivet` |
| Zsh | `rivet completions zsh > ~/.zfunc/_rivet` |
| Fish | `rivet completions fish > ~/.config/fish/completions/rivet.fish` |
| PowerShell | `rivet completions powershell > _rivet.ps1` |

---

## Environment variables

| Variable | Description |
|----------|-------------|
| `RUST_LOG` | Log level: `error`, `warn`, `info`, `debug`, `trace` |
| `DATABASE_URL` | Commonly used with `url_env: DATABASE_URL` |

### Example: verbose logging

```bash
RUST_LOG=debug rivet run -c my_export.yaml
```

---

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | All exports succeeded |
| 1 | One or more exports failed |
| 2 | Config parsing / validation error |
