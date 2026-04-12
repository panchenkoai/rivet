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

Generate a YAML config scaffold by connecting to PostgreSQL or MySQL and introspecting tables (read-only). Does **not** run an export.

```bash
rivet init --source <URL> [--table <NAME>] [--schema <NAME>] [-o <PATH>]
```

| Flag | Short | Type | Description |
|------|-------|------|-------------|
| `--source` | | string | Connection URL: `postgresql://` or `mysql://` **(required)** |
| `--table` | | string | Single table; optional `schema.table` on PostgreSQL. Omit to scaffold **all** tables/views in a Postgres schema or MySQL database |
| `--schema` | | string | **PostgreSQL:** schema to list (default `public`). **MySQL:** database name if not in the URL, or override URL database |
| `--output` | `-o` | string | Write YAML to file (default: print to stdout) |

**Examples**

```bash
# One table → one export block
rivet init --source "$DATABASE_URL" --table orders -o rivet.yaml

# PostgreSQL: entire schema (default public)
rivet init --source "$DATABASE_URL" --schema public -o all_public.yaml

# MySQL: entire database from URL path
rivet init --source 'mysql://user:pass@host:3306/mydb' -o all_mydb.yaml
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
