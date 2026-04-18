# `rivet init` — config scaffolding

`rivet init` connects to PostgreSQL or MySQL, introspects tables, and prints a **YAML scaffold** you can save and edit before running `rivet check` / `rivet run`.

Generated configs use `url_env: DATABASE_URL` so secrets are not embedded in the file. Set `DATABASE_URL` (or switch to `url:` / structured credentials) before running exports.

---

## Modes

### Single table

Provide `--table` (optionally `schema.table` on PostgreSQL).

```bash
export DATABASE_URL='postgresql://user:pass@localhost:5432/mydb'
rivet init --source "$DATABASE_URL" --table orders -o rivet.yaml

# Qualified name (PostgreSQL)
rivet init --source "$DATABASE_URL" --table analytics.facts -o rivet.yaml
```

```bash
export DATABASE_URL='mysql://user:pass@localhost:3306/mydb'
rivet init --source "$DATABASE_URL" --table orders -o rivet.yaml
```

Rivet emits **one** export block: `SELECT` of all columns, a suggested `mode` (`full`, `incremental`, or `chunked`) from row estimates and column types, plus `chunk_*` or `cursor_column` when applicable.

### Whole PostgreSQL schema

Omit `--table`. All **base tables and views** in the target schema are introspected; the file contains **one export per object**, sorted by name.

- **`--schema`** — PostgreSQL schema name (default: `public`).

```bash
export DATABASE_URL='postgresql://user:pass@localhost:5432/mydb'
rivet init --source "$DATABASE_URL" --schema public -o rivet_all_public.yaml

# Non-default schema
rivet init --source "$DATABASE_URL" --schema analytics -o rivet_analytics.yaml
```

The database itself comes from the connection URL path (`/mydb`).

### Whole MySQL database

Omit `--table`. All **base tables and views** in the database are listed from `information_schema`.

- If the URL already includes the database (`mysql://.../mydb`), that database is used.
- If the URL has **no** database path, pass **`--schema <database>`** (same flag name as for Postgres; on MySQL it selects the database name for listing).

```bash
rivet init --source 'mysql://user:pass@localhost:3306/rivet' -o rivet_mysql.yaml

# URL without database — name it explicitly
rivet init --source 'mysql://user:pass@localhost:3306/' --schema rivet -o rivet_mysql.yaml
```

---

## Heuristics (suggested `mode`)

| Condition | Suggested mode |
|-----------|----------------|
| Estimated rows ≤ 100k | `full` |
| Rows > 100k and integer PK (or first integer column) | `chunked` with `chunk_column` / `chunk_size` (and sometimes `parallel`) |
| Rows > 100k, no suitable integer chunk key, but a timestamp column | `incremental` with `cursor_column` (`updated_at` / `created_at` preferred) |

Row estimates are cheap metadata (`pg_class.reltuples` on PostgreSQL, `information_schema.TABLES.TABLE_ROWS` on MySQL), not exact `COUNT(*)`.

Always run **`rivet check --config <file>`** and adjust modes, destinations, and tuning before production runs.

---

## Flags (summary)

| Flag | Required | Description |
|------|----------|-------------|
| `--source` | one-of `--source*` | `postgresql://` or `mysql://` URL — **visible in shell history / `ps` output; avoid in production** |
| `--source-env` | one-of `--source*` | Name of an env var holding the URL (e.g. `DATABASE_URL`). URL never hits the command line. **Recommended.** |
| `--source-file` | one-of `--source*` | Path to a file containing just the URL on one line. Credentials stay on disk. |
| `--table` | no | Single table; omit for schema-wide / database-wide scaffold |
| `--schema` | no | **PostgreSQL:** schema to scan (default `public`). **MySQL:** database name if missing from URL or to override URL database |
| `-o` / `--output` | no | Write output to file; default is stdout |
| `--discover` | no | Emit a **JSON discovery artifact** (Epic B) instead of a YAML scaffold — see below |

### Avoiding credentials on the command line

Shell history, process listings (`ps`, `/proc/<pid>/cmdline`), and container inspect logs all capture `--source "postgresql://user:pass@host/db"` verbatim. For anything beyond local dev, use `--source-env` or `--source-file`:

```bash
# Recommended — env var resolved inside the process only.
export DATABASE_URL='postgresql://user:pass@host:5432/db'
rivet init --source-env DATABASE_URL --schema public -o cfg.yaml

# File-based — useful when the URL is managed by your secrets mount.
rivet init --source-file /run/secrets/database_url --table orders -o cfg.yaml
```

Exactly one of `--source`, `--source-env`, `--source-file` must be provided (enforced by clap's ArgGroup).

## Discovery artifact (`--discover`)

`rivet init --discover` runs the same introspection but emits a machine-readable JSON document (schema described in [`src/init/artifact.rs`](../../src/init/artifact.rs)). Intended consumers: external orchestration tools, code review, and automated config generators.

```bash
rivet init --source "$PG_URL" --schema public --discover -o discovery.json
rivet init --source "$MY_URL" --table orders   --discover    # pipes JSON to stdout
```

Per-table fields (`tables[]`):

| Field | Description |
|---|---|
| `schema`, `table`, `row_estimate` | Table identity and cheap row metadata |
| `total_bytes` | Physical size (`pg_total_relation_size`; `DATA_LENGTH + INDEX_LENGTH`) when available |
| `suggested_mode` | `full` / `incremental` / `chunked` — same heuristic as the YAML scaffold |
| `cursor_candidates[]` | Ranked list with `{column, data_type, is_nullable, is_primary_key, score, reasons[]}`. Reasons use a stable snake_case vocabulary: `name_suggests_updated`, `name_suggests_created`, `timestamp_type`, `integer_monotonic`, `primary_key`, `nullable` |
| `suggested_cursor_fallback_column` | Set when the top cursor is nullable **and** a NOT-NULL timestamp sibling exists — hint to enable [`incremental_cursor_mode: coalesce`](../modes/incremental-coalesce.md) (ADR-0007) |
| `chunk_candidates[]` | Ranked integer columns for chunked mode |
| `notes[]` | Advisory strings surfaced to operators reviewing the artifact |

The artifact is advisory — same policy as plan prioritization (ADR-0006): no runtime effect, no auto-application.

---

## Docker Compose in this repository

The repo root [`docker-compose.yaml`](../../docker-compose.yaml) defines **Postgres** and **MySQL** (`rivet` / `rivet` users, database `rivet`) with the same schema as [`dev/postgres/init.sql`](../../dev/postgres/init.sql) and [`dev/mysql/init.sql`](../../dev/mysql/init.sql).

```bash
docker compose up -d postgres mysql
export PG_URL='postgresql://rivet:rivet@localhost:5432/rivet?sslmode=disable'
export MY_URL='mysql://rivet:rivet@localhost:3306/rivet'

# One table
rivet init --source "$PG_URL" --table orders -o rivet_orders.yaml

# Whole PostgreSQL schema public
rivet init --source "$PG_URL" --schema public -o rivet_public.yaml

# Whole MySQL database from URL
rivet init --source "$MY_URL" -o rivet_mysql.yaml
```

To refresh many files at once (per-table YAMLs plus combined schema snapshots), run [`dev/regenerate_docker_init_configs.sh`](../../dev/regenerate_docker_init_configs.sh) from the repo root after the DBs are up (and optionally seeded).

---

## Limitations

- **Not** a migration or DDL tool — only read-only introspection and YAML output.
- Views are included in schema-wide / database-wide runs; ensure each view is selectable for your user.
- Suggested modes are heuristics; large or sparse tables may need manual `chunked` / `chunk_dense` / `chunk_by_days` tuning (see [chunked mode](../modes/chunked.md)).
