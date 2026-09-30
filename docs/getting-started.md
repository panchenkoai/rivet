_Last updated: 2026-09-30._

# Getting Started

Rivet exports tables from PostgreSQL, MySQL, SQL Server and Oracle (and collections from MongoDB) to Parquet (or CSV) files — locally, to S3, GCS, or Azure Blob Storage — and can load them into BigQuery, Snowflake or ClickHouse. Point it at a database, scaffold a config from your real tables, then run. (MongoDB and Oracle have their own references: [reference/mongodb.md](reference/mongodb.md), [reference/oracle.md](reference/oracle.md).)

```bash
brew install panchenkoai/rivet/rivet
export DATABASE_URL='postgresql://user:pass@localhost:5432/mydb'
# `orders` is a placeholder — use one of YOUR tables, or omit --table to scan the whole schema
rivet init --source-env DATABASE_URL --table orders -o rivet.yaml
rivet run -c rivet.yaml --validate
```

That's the whole flow. The four steps below explain each command, expected output, and where to go from each. Read time: ~3 minutes.

> **Already running it locally?** Jump to [§3 Preflight & run](#3--preflight--run). If you're evaluating it for production, finish this page first, then continue with [docs/pilot/](pilot/).

---

## 1 · Install

```bash
# macOS / Linux — Homebrew (recommended)
brew install panchenkoai/rivet/rivet
rivet --version
```

```bash
# Docker — try without installing anything
docker run --rm ghcr.io/panchenkoai/rivet:latest --version
```

Pre-built binaries are published for **Linux and macOS** (x86-64 + arm64). On
**Windows**, install from source with `cargo install rivet-cli` (a native binary
is not currently published). Other install paths — `cargo install rivet-cli`,
build from source, plus the full Docker recipe with database-on-host pointers
(`host.docker.internal` vs `--network host`) — live in the project
[README § Installation](https://github.com/panchenkoai/rivet/blob/main/README.md#installation).
Shell completions: `rivet completions bash|zsh|fish`.

### Try it in 60 seconds — no database of your own

Spin up a throwaway PostgreSQL, seed one table, and export it — nothing external
to configure:

```bash
docker run -d --name rivet-demo -e POSTGRES_PASSWORD=demo -p 5432:5432 postgres:16
sleep 3
docker exec -i rivet-demo psql -U postgres <<'SQL'
CREATE TABLE orders (id serial PRIMARY KEY, name text, price numeric(10,2),
                     updated_at timestamptz DEFAULT now());
INSERT INTO orders (name, price)
  SELECT 'order-'||g, (random()*500)::numeric(10,2) FROM generate_series(1,500) g;
SQL

export DATABASE_URL='postgresql://postgres:demo@localhost:5432/postgres'
rivet init --source-env DATABASE_URL --table orders -o rivet.yaml
rivet run -c rivet.yaml --validate
# → 500 rows of typed Parquet in ./output/orders/.  Clean up: docker rm -f rivet-demo
```

That is the whole flow against a real (throwaway) database. Then jump to
[§4 Inspect & iterate](#4--inspect--iterate), or read on to point Rivet at your
own database.

## 2 · Connect & scaffold a config

Recommended pattern: put the connection URL in an environment variable and reference it from the config so credentials never enter the file or shell history.

```bash
export DATABASE_URL='postgresql://user:pass@localhost:5432/mydb'
# MySQL: same flag, just a mysql:// URL
# export DATABASE_URL='mysql://user:pass@localhost:3306/mydb'

rivet init --source-env DATABASE_URL --table orders -o rivet.yaml
# `orders` is a placeholder — use one of YOUR tables, or omit --table to scan the whole schema
```

`rivet init` connects once, reads the column list + a rough row estimate from the live database, and writes a YAML file with `url_env: DATABASE_URL` and a sensible default mode. For a large table with a single-column primary key it picks **keyset** (`chunk_by_key`) — seek paging that stays flat-memory and is immune to sparse/gappy keys; keyset is scaffolded **sequential** (add `parallel: N` yourself to fan it into row-percentile ranges). A large table with no single-column PK gets a **range** `chunk_column` with a row-scaled `parallel:` (1 / 2 / 4) out of the box — measured ~1.4× faster on a wide 2 M-row table and ~4× on a narrow one. You can also point it at a whole schema (`--schema public`) or emit a richer JSON discovery artifact instead (`--discover -o discovery.json`).

Full flag reference: [reference/init.md](reference/init.md). For a manually-authored YAML instead of `rivet init`, see [reference/config.md](reference/config.md).

> **State file.** Rivet creates `.rivet_state.db` next to the config (cursors, chunk checkpoints, run history). Add it to `.gitignore` if the folder is version-controlled — see [SECURITY.md § Sensitive local artifacts](https://github.com/panchenkoai/rivet/blob/main/SECURITY.md#sensitive-local-artifacts).

## 3 · Preflight & run

```bash
rivet doctor -c rivet.yaml   # verify source + destination auth
rivet check  -c rivet.yaml   # dry-run analysis per export
rivet run    -c rivet.yaml --validate --reconcile
```

> A `mode: full` export does not replace the previous run's files: each run adds a new
> Parquet file beside the old one and warns about it. If you already ran the 60-second
> demo above, `rm -r ./output/orders` before this run — or read only the files
> `manifest.json` names.

The full basic workflow (`init` → `doctor` → `check` → `run` → `state`) recorded as a single terminal cast:

![Basic workflow](gifs/basic.gif)

What each step does:

- **`rivet doctor`** — connects to the source and writes a tiny probe object (`.rivet_doctor_probe`) to every destination prefix — removed afterwards on local destinations, while on S3 / GCS / Azure it stays at the prefix (the destination seam has no delete) and is filtered out of manifest and validate listings; fixes nothing, fails loudly on any auth / network issue.
- **`rivet check`** — runs `EXPLAIN` against your queries, estimates row counts, detects whether your cursor / chunk columns are indexed, and emits a verdict + concrete suggestion. Verdicts are `EFFICIENT` · `ACCEPTABLE` · `DEGRADED` · `UNSAFE`; on the SQL engines the last two carry a mode-aware `Suggestion:` line (MongoDB is full-scan-only, so its verdicts omit the mode suggestion).

  ![rivet check verdict block](gifs/check-verdict.gif)

- **`rivet run --validate --reconcile`** — extracts. `--validate` reads each output file back and verifies its row count; `--reconcile` runs `SELECT COUNT(*)` on the source query and compares with what was exported.

Example summary card after a successful run:

```
✓ orders        full             500 rows    1 files    11.4 KB      0.1s  RSS  40 MB

── orders ──────────────────────────────────────────────────
  run_id:         orders_20260930T101641.154_92474
  status:         success
  tuning:         profile=balanced (default), batch_size=10,000 (batch_size_memory_mb=32MiB → effective FETCH in logs)
  rows:           500
  files:          1
  output:         file://./output/orders/
  bytes read:     31.7 KB
  bytes written:  11.4 KB
  duration:       105ms
  peak RSS:       40 MB (sampled during run)
  validated:      pass
  schema:         unchanged
  reconcile:      MATCH (500/500)
```

## 4 · Inspect & iterate

```bash
rivet state show   -c rivet.yaml             # cursors (incremental exports)
rivet metrics      -c rivet.yaml --last 10   # per-run history
rivet state files  -c rivet.yaml             # files actually written
rivet journal      -c rivet.yaml --export orders   # per-run events / retries / quality issues
```

![Post-run inspection: state show, metrics, state files, state progression](gifs/inspect.gif)

To make later runs export only the rows that changed, scaffold the export in **incremental** mode. `rivet init` picks the cursor column (it must only ever grow — usually `updated_at` or a sequence id) and writes it into the config:

```bash
rivet init --source-env DATABASE_URL --table orders --mode incremental -o rivet-incremental.yaml
rivet run -c rivet-incremental.yaml     # first run: every row
rivet run -c rivet-incremental.yaml     # later runs: only rows past the stored cursor
rivet state show -c rivet-incremental.yaml   # the cursor each export will continue from
```

The generated export reads:

```yaml
exports:
  - name: orders
    query: >
      SELECT "id", "name", "price", "updated_at"
      FROM "orders"
    mode: incremental
    cursor_column: updated_at
    # … format, meta_columns, destination as in the full scaffold
```

Each run appends a file holding its delta to the same prefix; a run with nothing new writes no file and still succeeds. For tables larger than ~5 M rows, use `mode: chunked` instead — see [modes/chunked.md](modes/chunked.md).

---

## 5 · Many tables: plan once, apply by waves

When a config has several exports, `rivet plan` assigns each one a **wave** — a priority band derived from its size, chunking strategy, and risk ([ADR-0006](adr/0006-source-aware-prioritization.md)). By default `rivet plan` is **read-only**: it prints the schedule for you to review but does not touch the config. Add `--annotate-waves` to write the `wave:` / `parallel_safe:` fields back into the config, where you can see and hand-edit them:

```bash
rivet plan -c rivet.yaml                   # review the schedule (read-only)
rivet plan -c rivet.yaml --annotate-waves  # write `wave: N` onto every export, in place
```

```yaml
exports:
  - name: users
    wave: 1        # small / cheap → runs first
    # …
  - name: events
    wave: 3        # large → runs later
    # …
```

`rivet apply` then runs the whole config **wave by wave**, lowest first, with a barrier between waves — every export in wave 1 finishes before wave 2 starts. Exports with no `wave:` run last:

```bash
rivet apply rivet.yaml          # a .yaml path → wave-ordered execution
```

(A `.json` path still means the sealed single-artifact replay — see [reference/cli.md § rivet apply](reference/cli.md#rivet-apply).) The plan *suggests* the waves; you stay in control — hand-edit `wave:` and `apply` respects your order.

### Parallel within a wave — only where it's safe

Add `parallel_export_processes: true` (or pass `rivet apply --parallel-export-processes`) and, within each wave, the **cheap** exports — the ones `rivet plan` marked `parallel_safe: true` (cost class `Low`, under ~100K rows) — run concurrently as separate processes. A heavier export already chunk-parallelizes its own ranges *internally*, so it runs **alone** in its wave: two big tables at once would multiply the load on the source. The wave stays bounded because only the cheap `parallel_safe` exports run concurrently, and each child honors its own batch/memory caps (the adaptive back-pressure governor is a separate opt-in: `tuning.adaptive: true` with `parallel > 1`).

```yaml
parallel_export_processes: true   # top-level: parallelize the cheap (parallel_safe) exports within each wave
```

---

## Load into BigQuery, Snowflake or ClickHouse (optional)

Rivet stops at typed Parquet by default. To load it into a warehouse, the config
needs a cloud destination to stage in and a top-level `load:` block; `rivet load`
then derives the target table, column types and source files from the export
(nothing hand-typed). For BigQuery and ClickHouse `rivet init` writes both:

```bash
# BigQuery — stages in GCS
rivet init --source-env DATABASE_URL --table orders \
  --gcs-bucket my-bucket --bigquery-project my-gcp-project --bigquery-dataset analytics -o rivet.yaml

# ClickHouse — stages in GCS or S3 (`--s3-bucket`); the password is read from CLICKHOUSE_PASSWORD
rivet init --source-env DATABASE_URL --table orders \
  --gcs-bucket my-bucket --clickhouse-url http://clickhouse:8123 --clickhouse-database raw -o rivet.yaml
```

```bash
rivet run  -c rivet.yaml    # extract → the bucket
rivet load -c rivet.yaml    # load → warehouse (native types; count-gated before any cleanup)
```

The generated `load:` block carries `cleanup_source: true`: once a load is
row-count-verified, the staged Parquet is deleted. Snowflake has no `init` flags
yet — add its `load:` block by hand from [snowflake-load.md](recipes/snowflake-load.md).

The load follows the export's `mode:` — `full` overwrites the table with the
latest run; `incremental` / `cdc` append to `<table>__changes` and expose the
current state keyed on the source primary key `rivet run` recorded (set `pk: [id]`
in the `load:` block for a `query:` export or to override it). Recipes:
[snowflake-load.md](recipes/snowflake-load.md) ·
[cdc-bigquery-load.md](cdc-bigquery-load.md) ·
[clickhouse-load.md](recipes/clickhouse-load.md).

---

## When something is wrong

Rivet tries to fail early and say exactly what to fix — most mistakes are caught at `check` / `doctor` time, before a single row is read.

A query that references a table (or column) that doesn't exist is caught by `rivet check` — it exits non-zero with the offending name and SQLSTATE, instead of passing through to a half-finished run:

![rivet check catches a query against a missing table](gifs/error-missing-table.gif)

A typo in a config field is caught at parse time with a `Did you mean …?` suggestion that names the line:

![rivet names a mistyped config field and suggests the correct one](gifs/error-config-typo.gif)

An unreachable database — down, wrong host/port, or a tunnel that isn't up — is reported by `rivet doctor` with a reachability hint before you waste a run:

![rivet doctor reports an unreachable source with a hint](gifs/error-connection.gif)

More failure modes (retries, schema drift, crash/resume) and exactly what rivet does for each: [semantics.md](semantics.md).

---

## Next steps

| When you need to … | Go to |
|---|---|
| Pick the right export mode for each table | [modes/](modes/) — full · incremental · chunked · time_window · cdc |
| Configure S3 / GCS / Azure / stdout destinations | [destinations/](destinations/) |
| Load exports into BigQuery / Snowflake / ClickHouse | [recipes/snowflake-load.md](recipes/snowflake-load.md) · [cdc-bigquery-load.md](cdc-bigquery-load.md) · [recipes/clickhouse-load.md](recipes/clickhouse-load.md) |
| Look up a YAML field or a CLI flag | [reference/config.md](reference/config.md) · [reference/cli.md](reference/cli.md) |
| Understand `run_id` / cursor / chunk / manifest / journal | [concepts.md](concepts.md) |
| Tune for memory, throughput, source pressure | [reference/tuning.md](reference/tuning.md) · [best-practices/](best-practices/) |
| Take it to production (read replicas, poolers, monitoring) | [pilot/production-checklist.md](pilot/production-checklist.md) |
| Run a serious pilot (chunked + reconcile + repair on your data) | [pilot/pilot-walkthrough.md](pilot/pilot-walkthrough.md) |
| See exactly what happens under retry / crash / resume | [semantics.md](semantics.md) |
| Auditable plan/apply workflow for CI/CD | [reference/cli.md § rivet plan](reference/cli.md#rivet-plan) · [ADR-0005](adr/0005-plan-apply-contracts.md) |
