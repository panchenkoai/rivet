# Rivet

**Lightweight, source-safe data extraction from PostgreSQL and MySQL to Parquet/CSV.**

One Rust binary, ~10 MB. **Sub-second longest SQL on a 2 M-row wide table, ~280 MB peak RSS** ([reproducible cross-tool bench](docs/bench/)). Extract-only — no CDC, no SaaS console, no Kubernetes operator.

![rivet basic workflow — init, doctor, check, run, state](docs/gifs/basic.gif)

## Why Rivet

- **Gentle on the source.** The longest single SQL Rivet issues against a 2 M-row wide table is **0.19s** (a `FETCH 142` against a server-side cursor). Tools that pull `SELECT *` into a single client-side buffer hold the same query for 70–134s. Numbers, methodology, and a [steelman re-run](docs/bench/reports/REPORT_steelman.md) (each competitor at its best plausible config) live in [docs/bench/](docs/bench/).
- **Knows you're behind a pooler.** Auto-detects pgBouncer / Odyssey on Postgres (PID-flip probe) and ProxySQL / MaxScale / generic multiplexers on MySQL (4-signal classifier). Uses `SET LOCAL` inside RAII-guarded transactions so session state never leaks into the pool. Warns operators about what won't work (LISTEN/NOTIFY, advisory locks, per-session vars) before the export starts. ([demo](docs/gifs/pool-detect.gif))
- **Works when your cursor is messy.** Nullable `updated_at` mixed with `created_at`? `incremental_cursor_mode: coalesce` tracks `COALESCE(primary, fallback)` as a single scalar — no rewriting your query, no lost rows ([walkthrough](docs/modes/incremental-coalesce.md), [ADR-0007](docs/adr/0007-cursor-policy-contracts.md)).
- **Boring, resumable, auditable.** Chunk checkpoints, file manifest, run journal, schema-drift tracking, typed-retry classification — every run reconstructible from `.rivet_state.db`. `rivet plan` seals execution intent into a reviewable JSON artifact ([ADR-0005](docs/adr/0005-plan-apply-contracts.md)).
- **AI-friendly DB introspection.** `rivet-mcp` is a separate Model Context Protocol server binary (Claude Desktop / Claude Code / any MCP client) that exposes read-only `pg_stat_activity`, checkpoint pressure, `pg_stat_statements` I/O, MySQL processlist, and pgBouncer diagnostics. Lets an agent answer *"is this DB healthy enough to extract from right now?"* without ever giving it write access.
- **One binary, zero platform.** `brew install` · `cargo install rivet-cli` · `docker run` · GH release binary — pick one. No operator, no console, no CRDs.

## 30-second quickstart

```bash
brew install panchenkoai/rivet/rivet

export DATABASE_URL="postgresql://user:pass@host/db"
rivet init --source-env DATABASE_URL --table orders -o rivet.yaml
rivet run -c rivet.yaml
```

Output: Parquet files in `./output/`. Full walkthrough: [docs/getting-started.md](docs/getting-started.md). Want to try without your own DB? `docs/pilot/demo-quickstart.md` runs the whole flow against a pre-seeded 14-table fixture in ~10 min.

---

## What Rivet is (and is not)

| What Rivet does | What you bring |
|-----------------|----------------|
| Queries PostgreSQL 12–16 and MySQL 5.7 / 8.0 | The database and credentials |
| Streams rows → Arrow → Parquet or CSV | A destination (local path, S3 bucket, GCS bucket) |
| Retries failed batches with exponential backoff | Orchestration (cron, Airflow, dbt, etc.) |
| Validates row counts, null ratios, and uniqueness | Your warehouse or downstream pipeline |
| Checkpoints progress — resume after crashes | Schema management on the warehouse side |
| Protects the source DB — chunked + cursor + memory cap → longest single query ~0.2s on PG / ~9s on MySQL on a 2 M-row wide table ([benchmarks](docs/bench/)) | — |

Supported destinations: local filesystem, Amazon S3, Google Cloud Storage, stdout.
Export modes: `full`, `incremental` (cursor-based), `chunked`, `time_window`.
Formats: Parquet (zstd / snappy / gzip / lz4 / none) and CSV.

**Not for you if you need:**
- **CDC / streaming** — Rivet reads a snapshot per run; it has no event log or replication slot. Use [Debezium](https://debezium.io/) or [Estuary](https://estuary.dev/) instead.
- **A SaaS connector marketplace** — no cloud console, no managed infrastructure, no pre-built connectors beyond Postgres and MySQL.
- **A Kubernetes data platform** — no operator, no Helm chart, no CRD.
- **Loading or transformation** — Rivet stops at "file on disk or in a bucket"; bring dbt, Spark, or your own loader.

**Documentation language:** English-only. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Source pressure, measured

"Source-safe" and "lightweight" are easy claims to make and hard to verify, so we publish a [reproducible cross-tool benchmark harness](docs/bench/) that runs Rivet, sling, dlt, duckdb (`postgres_scanner` / `mysql_scanner`), clickhouse-local, and odbc2parquet (v11.0.0) against the same fixtures (22 PG tables / 17 MySQL tables, including a 2 M-row × 20-wide-column `content_items`).

**Longest single SQL statement on the wide table** — the number that decides whether your DBA's `statement_timeout = 60s` cuts you off:

| Tool | PostgreSQL | MySQL |
|---|---:|---:|
| **rivet** | **0.19s** (`FETCH 142 FROM _rive`) | **9s** (chunked + cursor) |
| sling | 134s (`select * from content_items`) | 137s |
| dlt | 1.20s (`FETCH FORWARD 10000`) — but 200 temp files / 3.2 GB temp_bytes | 208s |
| clickhouse-local | 132s (`COPY (SELECT ...)`) | 174s |
| duckdb (postgres_scanner) | 70s (12 backends in parallel) | 231s (single-threaded mysql_scanner) |
| odbc2parquet 11.0.0 | 136s (`SELECT * FROM content_items`) | n/a (no macOS arm64 driver) |

**Peak RSS** — what fits in a 2 GB CI runner:

| Tool | PostgreSQL | MySQL |
|---|---:|---:|
| **rivet** | **443 MB** | **280 MB** |
| dlt | 1.4 GB | 1.2 GB |
| clickhouse-local | 1.6 GB | 1.7 GB |
| sling | 6.0 GB | 6.3 GB |
| duckdb | 18.9 GB | 23.1 GB |
| odbc2parquet 11.0.0 | 29.1 GB | n/a |

**Failure count across all tables**: rivet 0 / 22 (PG), 0 / 17 (MySQL). Three other tools each failed at least one table in the suite (`dlt`, `duckdb`, `odbc2parquet` — the last on a JSON-payload column that overruns its pre-allocated column buffer even at `--column-length-limit 65536`).

How Rivet wins these axes is not magic — it's the deliberately boring extraction shape: PK-auto-resolved chunks (`mode: chunked`, `chunk_size_memory_mb: 256`), a server-side cursor with a `work_mem`-aware `FETCH N` cap on PG, and an Arrow-memory-budgeted row buffer on MySQL. The «one big `SELECT *` into a giant client-side buffer» shape that `sling`, `clickhouse-local`, and `odbc2parquet` use is what produces both the multi-minute single-query holds and the multi-GB RSS — even after `odbc2parquet`'s recent v11.0.0 refresh, the architectural shape is unchanged.

The numbers above use each tool **at its defaults** (with only the minimum flags needed to make the bench run). We also published a [**steelman**](docs/bench/reports/REPORT_steelman.md) re-run that gives each open-source competitor its best plausible configuration — `odbc2parquet`'s `--batch-size-memory 256MiB --sequential-fetching`, narrow-table `--column-length-limit`, etc. — and reports what happens. Short version: on narrow tables the gap closes substantially (e.g. `odbc2parquet` `page_views` RSS 6.0 GB → 2.2 GB); on the wide `content_items` fixture Rivet's edge survives largely intact (~58× peak RSS, ~700× longest single query). `sling`'s chunked mode is paywalled and therefore out of an open-source comparison entirely.

Methodology, exact configs, raw `gtime -v` output, and DB-side counter deltas (`pg_stat_database`, `Innodb_rows_read`, `processlist`): [docs/bench/](docs/bench/) — one-command repro.

## Core promise

Rivet tries to make database extraction boring:

1. **Plan before running** — `rivet plan` produces a sealed artifact you can review.
2. **Limit source pressure** — tuning profiles, statement timeouts, throttle, RSS guard.
3. **Write in resumable units** — chunks and checkpoints, not one giant transaction.
4. **Record state and journal progress** — every run is reconstructible from `.rivet_state.db` + journal.
5. **Recover safely from common failures** — `--resume`, typed retry classes, named semantic-gate tests.
6. **Validate and reconcile outputs** — quality gates, `rivet reconcile`, targeted `rivet repair`; explicit **committed** vs **verified** progression boundaries per export ([ADR-0008](docs/adr/0008-export-progression.md), `rivet state progression`).
7. **Notice when the source drifts** — `on_schema_drift: warn|continue|fail` for structural changes (column added/removed/retyped); `shape_drift_warn_factor` for unexpected growth in TEXT/JSON byte widths — both tracked across runs in `.rivet_state.db`.

The execution contract behind each of these — what is guaranteed, what is at-least-once, what isn't covered — is in [docs/semantics.md](docs/semantics.md).

## Trust contracts

| Question | Where to look |
|---|---|
| What happens if the process is killed mid-export? | [docs/semantics.md § Crash semantics](docs/semantics.md#crash-semantics) |
| What does Rivet *not* guarantee? | [docs/semantics.md § Known non-guarantees](docs/semantics.md#known-non-guarantees) |
| What is actually tested in PR CI vs nightly vs manual? | [docs/reliability-matrix.md](docs/reliability-matrix.md) |
| Which PostgreSQL / MySQL versions are exercised? | [docs/reference/compatibility.md](docs/reference/compatibility.md) |
| How are credentials handled? Where do sensitive artifacts land? | [SECURITY.md](SECURITY.md) |
| How were the benchmark numbers above produced — can I rerun them? | [docs/bench/](docs/bench/) (defaults) · [docs/bench/reports/REPORT_steelman.md](docs/bench/reports/REPORT_steelman.md) (each tool with its best plausible tuning) |

> **Sensitive local artifacts.** Generated files — `.rivet_state.db`, `plan.json`, `*.journal.jsonl`, and exported Parquet/CSV — may contain query SQL, cursor values, table metadata, and the data itself. Do not commit them. See [SECURITY.md § Sensitive local artifacts](SECURITY.md#sensitive-local-artifacts) for a `.gitignore` snippet.

## Stateless deployment

By default Rivet keeps cursors, manifests, chunk checkpoints, and the run journal in a SQLite file (`.rivet_state.db`) next to your config — perfect for local and single-node runs. For ephemeral containers / Kubernetes pods, set `RIVET_STATE_URL` to a PostgreSQL connection string and Rivet creates and migrates the state schema on first connect — no manual DDL, no init job. Details: [docs/reference/cli.md § State backend](docs/reference/cli.md#state-backend).

```bash
export RIVET_STATE_URL="postgresql://rivet:secret@state-db.internal/rivet_state?sslmode=require"
rivet run -c rivet.yaml
```

## More walkthroughs

[plan / apply](docs/gifs/plan-apply.gif) · [plan campaign — multi-export waves](docs/gifs/plan-campaign.gif) · [reconcile + repair](docs/gifs/reconcile-repair.gif) · [parallel cards UI](docs/gifs/parallel-cards.gif) · [composite cursor (COALESCE fallback)](docs/gifs/coalesce-cursor.gif) · [pool detection](docs/gifs/pool-detect.gif) · [discovery artifact (`rivet init --discover`)](docs/gifs/discover-artifact.gif) · [post-run inspect](docs/gifs/inspect.gif). Source scripts in [docs/gifs/](docs/gifs/).

---

## Installation

> **Names.** The project and CLI are **Rivet**; the command is **`rivet`**. On [crates.io](https://crates.io/crates/rivet-cli) the package is published as **`rivet-cli`** because the crate name `rivet` was already taken. Homebrew and release archives install the **`rivet`** binary.

### Homebrew (macOS / Linux) — recommended

```bash
brew install panchenkoai/rivet/rivet
rivet --version
```

### cargo install (crates.io)

Requires Rust 1.94+:

```bash
cargo install rivet-cli
rivet --version
```

> The binary is named `rivet`. The crate is published as [`rivet-cli`](https://crates.io/crates/rivet-cli) because the `rivet` name on crates.io is taken.

### Pre-built binaries

Download the latest release for your platform from [GitHub Releases](https://github.com/panchenkoai/rivet/releases):

```bash
# macOS (Apple Silicon)
curl -L https://github.com/panchenkoai/rivet/releases/latest/download/rivet-aarch64-apple-darwin.tar.gz | tar xz
sudo mv rivet-*/rivet /usr/local/bin/

# macOS (Intel)
curl -L https://github.com/panchenkoai/rivet/releases/latest/download/rivet-x86_64-apple-darwin.tar.gz | tar xz
sudo mv rivet-*/rivet /usr/local/bin/

# Linux (x86_64)
curl -L https://github.com/panchenkoai/rivet/releases/latest/download/rivet-x86_64-unknown-linux-gnu.tar.gz | tar xz
sudo mv rivet-*/rivet /usr/local/bin/

# Linux (arm64)
curl -L https://github.com/panchenkoai/rivet/releases/latest/download/rivet-aarch64-unknown-linux-gnu.tar.gz | tar xz
sudo mv rivet-*/rivet /usr/local/bin/
```

```bash
rivet --version
```

### Docker

```bash
docker run --rm ghcr.io/panchenkoai/rivet:latest --version

docker run --rm \
  -e DATABASE_URL="postgresql://user:pass@host.docker.internal:5432/db" \
  -v $(pwd)/examples/rivet.yaml:/config/rivet.yaml \
  -v $(pwd)/output:/output \
  ghcr.io/panchenkoai/rivet:latest \
  run --config /config/rivet.yaml
```

> From a container, `localhost` is not your machine. Use `host.docker.internal` (Docker Desktop) or `--add-host=host.docker.internal:host-gateway` on Linux. See [Getting Started](docs/getting-started.md) for details.

### Build from source

Requires Rust 1.94+:

```bash
git clone https://github.com/panchenkoai/rivet.git
cd rivet
cargo build --release
# binary is at target/release/rivet
```

---

## Documentation

| Topic | Link |
|-------|------|
| All docs (index) | [docs/README.md](docs/README.md) |
| **First run — install, connect, export** | [docs/getting-started.md](docs/getting-started.md) |
| **Concepts glossary** (`run_id`, `cursor`, `chunk`, `manifest`, `journal`, `progression`) | [docs/concepts.md](docs/concepts.md) |
| **Pilot guide** — full flow on your own database, production-ready | [docs/pilot/README.md](docs/pilot/README.md) |
| **Execution semantics** (crash / retry / resume contract) | [docs/semantics.md](docs/semantics.md) |
| **Reliability matrix** (what's in PR CI / nightly / manual) | [docs/reliability-matrix.md](docs/reliability-matrix.md) |
| **Security policy** (credentials, sensitive artifacts, disclosure) | [SECURITY.md](SECURITY.md) |
| **Cross-tool benchmark harness** (rivet vs sling, dlt, duckdb, clickhouse-local, odbc2parquet) | [docs/bench/](docs/bench/) |
| Export modes (`full`, `incremental`, `chunked`, `time_window`) | [docs/modes/](docs/modes/) |
| Destinations (local, S3, GCS, stdout) | [docs/destinations/](docs/destinations/) |
| Config YAML reference | [docs/reference/config.md](docs/reference/config.md) |
| CLI commands and flags | [docs/reference/cli.md](docs/reference/cli.md) |
| Tuning profiles | [docs/reference/tuning.md](docs/reference/tuning.md) |
| Scaffold config from a live DB (`rivet init`) | [docs/reference/init.md](docs/reference/init.md) |
| Pipeline, traits, memory model, source layout | [docs/architecture.md](docs/architecture.md) |
| Demo on a pre-seeded 14-table fixture (~10 min) | [docs/pilot/demo-quickstart.md](docs/pilot/demo-quickstart.md) |
| Pilot walkthrough — discovery → reconcile → repair | [docs/pilot/pilot-walkthrough.md](docs/pilot/pilot-walkthrough.md) |
| Production checklist | [docs/pilot/production-checklist.md](docs/pilot/production-checklist.md) |
| Architecture decision records | [docs/adr/](docs/adr/) |
| Contributing, tests, CI | [CONTRIBUTING.md](CONTRIBUTING.md) |

---

## Resource-aware extraction

Rivet v0.5.x adds explicit controls over how much RAM, CPU, and disk a run is allowed to consume. These are production-safety primitives, not performance knobs.

### Memory controls

| Setting | What it controls |
|---------|-----------------|
| `tuning.max_batch_memory_mb` | Hard cap on a single Arrow batch. When exceeded, the `on_batch_memory_exceeded` policy fires. |
| `tuning.on_batch_memory_exceeded` | `warn` (log + continue) · `fail` (abort) · `auto_shrink` (split batch recursively, then continue) |
| `tuning.memory_threshold_mb` | Process-level RSS guard — pauses fetching when RSS exceeds the threshold |
| `tuning.batch_size_memory_mb` | Adaptive batch sizing: Rivet samples the first batch to estimate row width, then adjusts subsequent batch sizes automatically |

### Output controls

| Setting | What it controls |
|---------|-----------------|
| `compression_profile` | `none` / `fast` (Snappy) / `balanced` (Zstd-3) / `compact` (Zstd-9) |
| `parquet.row_group_strategy` | `auto` (schema-based estimate) / `fixed_rows` / `fixed_memory` |
| `parquet.target_row_group_mb` | Target row group size; lower values reduce peak RSS during Parquet writes |

### Quality gates

| Setting | What it controls |
|---------|-----------------|
| `quality.row_count_min` / `row_count_max` | Fail the export if row count is outside this range — fires even when the source returns 0 rows |
| `quality.null_ratio_max` | Fail the export if the null ratio in a column exceeds the threshold |
| `quality.unique_columns` | Track column uniqueness via typed xxHash3-64 hashing |
| `quality.unique_max_entries` | Cap the uniqueness hash set to prevent unbounded memory growth on high-cardinality columns |

### Choosing settings for your environment

| Environment | Recommended starting point |
|-------------|---------------------------|
| Production database (shared) | `profile: safe`, `max_batch_memory_mb: 128`, `on_batch_memory_exceeded: warn` |
| CI / strict pipeline | `max_batch_memory_mb: 128`, `on_batch_memory_exceeded: fail` |
| Low-memory host (1–2 GB) | `profile: safe`, `max_batch_memory_mb: 64`, `on_batch_memory_exceeded: auto_shrink` |
| Read replica / fast backfill | `profile: fast`, `compression_profile: fast` |

See the **[Best Practices guides](docs/best-practices/)** for detailed explanations, trade-off analysis, and worked examples:

- [Resource-aware extraction](docs/best-practices/resource-aware-extraction.md) — memory budgets, policies, RSS formula
- [Parquet tuning](docs/best-practices/parquet-tuning.md) — row group strategies, targets, downstream read implications
- [Compression profiles](docs/best-practices/compression-profiles.md) — profile-to-codec mapping, CPU/size trade-offs
- [Quality checks](docs/best-practices/quality-checks.md) — row count gates, null ratio, uniqueness cap
- [Low-memory runners](docs/best-practices/low-memory-runners.md) — settings for 512 MB–4 GB hosts
- [Recovery and resume](docs/best-practices/recovery-and-resume.md) — `--resume` semantics, crash recovery

---

## Releases and roadmap

- **Latest release and version history:** [CHANGELOG.md](CHANGELOG.md).
- **Strategy, pains, and execution tracker:** [rivet_roadmap.md](rivet_roadmap.md) — the single source of truth for what is shipped and what is open.
