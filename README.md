<p align="center">
  <img src="docs/assets/rivet_logo.png" alt="Rivet" width="480">
</p>

<p align="center">
  <a href="https://github.com/panchenkoai/rivet/actions/workflows/ci.yml"><img src="https://github.com/panchenkoai/rivet/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/panchenkoai/rivet/releases/latest"><img src="https://img.shields.io/github/v/release/panchenkoai/rivet?label=release" alt="release"></a>
  <a href="docs/reliability-matrix.md"><img src="https://img.shields.io/badge/coverage-reliability%20matrix-blue" alt="coverage"></a>
  <a href="https://github.com/panchenkoai/rivet/blob/main/LICENSE"><img src="https://img.shields.io/github/license/panchenkoai/rivet" alt="license"></a>
  <a href="https://discord.gg/HT5DZNzNU"><img src="https://img.shields.io/badge/Discord-join%20chat-5865F2?logo=discord&logoColor=white" alt="Discord"></a>
</p>

<p align="center"><strong>Make database extraction boring.</strong></p>

<p align="center">One Rust binary, ~18 MB. Extracts PostgreSQL, MySQL, SQL Server, and MongoDB to Parquet/CSV — locally or to S3 / GCS / Azure — without holding long queries open on your production database (chunked/keyset reads keep each query short; on PostgreSQL a full export still runs inside one snapshot transaction for consistency — for multi-hour fulls, read from a replica). <strong>Batch snapshots or log-based change data capture.</strong> Resumable, auditable, source-safe.</p>

> Not sure if Rivet fits your problem? [docs/who-is-this-for.md](docs/who-is-this-for.md) is a 60-second fit-check.

![rivet basic workflow — init, doctor, check, run, state](https://raw.githubusercontent.com/panchenkoai/rivet/main/docs/gifs/basic.gif)

## 30-second quickstart

```bash
brew install panchenkoai/rivet/rivet

export DATABASE_URL="postgresql://user:pass@host/db"
# `orders` is a placeholder — use one of YOUR tables, or omit --table to scan the whole schema
rivet init --source-env DATABASE_URL --table orders -o rivet.yaml
rivet run -c rivet.yaml
```

Output: Parquet files in `./output/`. Full walkthrough: [docs/getting-started.md](docs/getting-started.md). Want to try without your own DB? `docs/pilot/demo-quickstart.md` runs the whole flow against a pre-seeded 14-table fixture in ~10 min.

---

## Why Rivet

Rivet tries to make database extraction boring:

1. **Plan before running** — `rivet plan` seals the extraction intent into a reviewable JSON artifact before any writes happen. Review it like a migration.
2. **Protect the source** — server-side cursor + `FETCH N` on PostgreSQL (longest single query: **0.19s** on a 2M-row table); adaptive PK-range chunking on MySQL (**9s**, vs 137–208s for alternatives). Neither shape holds an open transaction for minutes.
3. **Knows you're behind a pooler** — auto-detects pgBouncer / Odyssey on Postgres, ProxySQL / MaxScale on MySQL, and statement-level multiplexers (`@@SPID` drift) or the Azure SQL gateway on SQL Server. On Postgres it uses `SET LOCAL` inside RAII-guarded transactions so session state never leaks into the pool.
4. **Write in resumable units** — chunk checkpoints, not one giant transaction. The job can crash, the network can blip, the next `rivet run --resume` continues from the last committed chunk.
5. **Record everything** — run journal, file manifest, schema-drift tracker, all in `.rivet_state.db`. Every run is reconstructible. `rivet state` shows exactly what committed and what didn't.
6. **Validate outputs** — quality gates (row count, null ratio, uniqueness via xxHash3), `rivet validate`, `rivet reconcile`, `rivet repair`. Know before your downstream pipeline does.
7. **Notice when the source changes** — column adds/removes/retypes trigger `on_schema_drift: warn|continue|fail` on the next run. Shape drift in TEXT/JSON columns is tracked via byte-width sampling.

The execution contract behind each of these — what is guaranteed, what is at-least-once, what isn't covered — is in [docs/semantics.md](docs/semantics.md).

## Change data capture

Beyond batch snapshots, Rivet reads the source's **transaction log** — MySQL binlog, a PostgreSQL logical slot, SQL Server change tables, MongoDB change streams — and writes every INSERT / UPDATE / DELETE as typed Parquet/CSV (or, for MongoDB, the JSON-blob document image) through the same commit seam (destination + content-MD5 + manifest + `_SUCCESS`) the batch path uses. A CDC export lands in your bucket and shows up in `rivet metrics` exactly like a batch one.

![rivet CDC — scaffold a cdc config, capture binlog changes to typed Parquet, read them back as typed rows](https://raw.githubusercontent.com/panchenkoai/rivet/main/docs/gifs/cdc.gif)

### CDC quickstart

```bash
# 1. One-time source prep (per engine — full list: docs/reference/cdc.md):
#    MySQL: binlog_format=ROW + a REPLICATION SLAVE grant
#    PostgreSQL: wal_level=logical + a role with REPLICATION
#    SQL Server: enable CDC on the table + SQL Server Agent running
export DATABASE_URL="mysql://user:pass@host/db"

# 2. Scaffold a cdc config (engine-specific stream params pre-filled)
rivet init --source-env DATABASE_URL --table orders --mode cdc -o cdc.yaml

# 3. Capture: drain every change since the checkpoint to typed Parquet, then exit
rivet run -c cdc.yaml
```

First run starts from the current log position (or the checkpoint, on a resume); make a change to `orders`, run step 3 again, and it lands as a row (`__op` = insert/update/delete). Output: typed Parquet in `./output/` (or your bucket) + manifest + `_SUCCESS`, and the run appears in `rivet metrics`. Schedule step 3 (cron / Airflow) for continuous capture — each run resumes from the checkpoint. Full reference + grants: [docs/reference/cdc.md](docs/reference/cdc.md).

> **Missing a grant?** If step 1 isn't done, `rivet run` fails with the exact requirement and a pointer to the grants section — e.g. `MySQL CDC needs … a REPLICATION SLAVE + REPLICATION CLIENT grant …` — not a raw driver error.

The generated `cdc.yaml` (pass `--gcs-bucket` / `--s3-bucket` to `init` for a cloud destination instead of local):

```yaml
exports:
  - name: orders
    table: orders
    mode: cdc
    format: parquet
    cdc:
      checkpoint: ./cdc/orders.ckpt
      until_current: true        # drain to the current log end and exit (for a scheduler)
      server_id: 4271            # MySQL replica id (slot / capture_instance for PG / SQL Server)
    destination: { type: local, path: ./output/orders/ }
```

- **Source-safe, like the batch path** — reads the log, never a long `SELECT`: no locks, no snapshot. Catches changes that don't touch an `updated_at` (which a watermark sync silently misses).
- **At-least-once** on all three engines — commit-boundary checkpoint; PostgreSQL advances its slot only after a durable write.
- **Typed output matches the batch export** — real `Timestamp` / `Decimal` / `json` / `uuid`, not strings (same `build_arrow_field` pipeline).
- The upsert output shape (`[__op, __pos]` + after-image), the grants each engine needs, the per-engine retention/ack model, and current limitations are in **[docs/reference/cdc.md](docs/reference/cdc.md)**.

## Trust contracts

| Question | Where to look |
|---|---|
| What happens if the process is killed mid-export? | [docs/semantics.md § Crash semantics](docs/semantics.md#crash-semantics) |
| What does Rivet *not* guarantee? | [docs/semantics.md § Known non-guarantees](docs/semantics.md#known-non-guarantees) |
| What is actually tested in PR CI vs nightly vs manual? | [docs/reliability-matrix.md](docs/reliability-matrix.md) |
| Which PostgreSQL / MySQL versions are exercised? | [docs/reference/compatibility.md](docs/reference/compatibility.md) |
| How are credentials handled? Where do sensitive artifacts land? | [SECURITY.md](SECURITY.md) |
| What permissions does Rivet need on S3 / GCS / Azure? | [docs/cloud-permissions.md](docs/cloud-permissions.md) |
| How were the benchmark numbers produced — can I rerun them? | [docs/bench/](docs/bench/) |

> **Sensitive local artifacts.** Generated files — `.rivet_state.db`, `plan.json`, `*.journal.jsonl`, and exported Parquet/CSV — may contain query SQL, cursor values, table metadata, and the data itself. Do not commit them. See [SECURITY.md § Sensitive local artifacts](SECURITY.md#sensitive-local-artifacts) for a `.gitignore` snippet.

---

## Source pressure, measured

"Source-safe" is easy to claim and hard to verify, so Rivet publishes a [reproducible cross-tool benchmark harness](docs/bench/) against identical fixtures (22 PG tables / 17 MySQL tables, including a 2M-row × 20-column `content_items` table).

The primary metric is **longest single SQL statement** — the one that decides whether your DBA's `statement_timeout` cuts you off mid-run.

### PostgreSQL — server-side cursor enables sub-second longest query

| Tool | Longest single query | Peak RSS |
|---|---:|---:|
| **rivet** | **0.19s** (`FETCH 142 FROM _rive`) | **443 MB** |
| dlt | 1.20s (`FETCH FORWARD 10000`) — 3.2 GB temp_bytes | 1.4 GB |
| sling | 134s (`SELECT * FROM content_items`) | 6.0 GB |

### MySQL — no server-side cursor; chunked range scans are the fastest available shape

| Tool | Longest single query | Peak RSS |
|---|---:|---:|
| **rivet** | **9s** (chunked + cursor) | **280 MB** |
| sling | 137s | 6.3 GB |
| dlt | 208s | 1.2 GB |

The MySQL gap vs PostgreSQL is architectural: PostgreSQL exposes `DECLARE … CURSOR` / `FETCH N` which lets Rivet issue tiny sub-queries server-side; MySQL's protocol does not have a widely-supported equivalent in the current client stack. See [MySQL parity roadmap](#releases-and-roadmap) for what's planned.

**Failure count across all tables**: rivet 0 / 22 (PG), 0 / 17 (MySQL). At least one other tool in the suite failed at least one table.

How Rivet wins these axes is not magic — it's the deliberately boring extraction shape: PK-auto-resolved chunks, a server-side cursor with a `work_mem`-aware `FETCH N` cap on PG, and an Arrow-memory-budgeted row buffer on MySQL. The «one big `SELECT *` into a giant client-side buffer» shape that most alternatives use produces both the multi-minute single-query holds and the multi-GB RSS.

**As of 0.12.0, fast as well as gentle.** Source-safety never meant slow. Rivet now sizes each batch to a ~32 MB memory target instead of a fixed 10,000 rows — which on narrow tables (many rows, few/small columns) sped up extraction **~7.5× on MySQL and ~6× on SQL Server** (rivet 0.11 → 0.12, same 10.24M-row fixture, row-exact). The target is *shape-aware*: narrow tables get large batches, wide tables stay near the old size (so they don't regress). And it's source-neutral *by construction*: batch size governs only how fast Rivet drains its **client-side** buffer, never the SQL it issues — so the source query is held open *less* time, not more (verified: identical server-side rows scanned, zero extra temp-table spills). The trade is bounded *client* memory: narrow-table peak RSS rises to ~70 MB (MySQL) / ~90 MB (SQL Server) — capped by a 150k-row batch ceiling, still an order of magnitude under the multi-GB a giant client buffer needs, and `profile: safe` lowers it further. Reproduce with [`dev/bench/batch_throughput_ab.sh`](dev/bench/batch_throughput_ab.sh).

The numbers above use each tool **at its defaults**. We also published a [**steelman**](docs/bench/reports/REPORT_steelman.md) re-run that gives each competitor its best plausible configuration. Short version: on narrow tables the gap closes; on the wide `content_items` fixture Rivet's edge survives largely intact.

Methodology, exact configs, raw `gtime -v` output, and DB-side counter deltas: [docs/bench/](docs/bench/) — one-command repro.

---

## AI-native DB observability — `rivet-mcp`

`rivet-mcp` is a [Model Context Protocol](https://modelcontextprotocol.io/) server binary that lets an AI agent answer *"is this database healthy enough to extract from right now?"* — before any rows are touched.

Exposed read-only surfaces:

- **PostgreSQL** — `pg_stat_activity` (active queries, lock waits, idle-in-transaction), `pg_stat_statements` top I/O, checkpoint pressure (`pg_stat_bgwriter`), pgBouncer pool saturation and client wait time
- **MySQL** — `SHOW PROCESSLIST` (running queries and duration)

Works out-of-the-box with [Claude Desktop](https://claude.ai/), [Claude Code](https://claude.ai/code), and any MCP-compatible client. Runs as a separate binary — never requires write access to the source database.

```bash
export DATABASE_URL="postgresql://..."
rivet-mcp        # reads DATABASE_URL from the environment
```

Add to your MCP client config:

```json
{
  "mcpServers": {
    "rivet": {
      "command": "rivet-mcp",
      "env": { "DATABASE_URL": "postgresql://..." }
    }
  }
}
```

---

## What Rivet is (and is not)

| What Rivet does | What you bring |
|-----------------|----------------|
| Queries PostgreSQL 12–16 and MySQL 5.7 / 8.0 | The database and credentials |
| Streams rows → Arrow → Parquet or CSV | A destination (local path, S3 bucket, GCS bucket, Azure container) |
| Retries failed batches with exponential backoff | Orchestration (cron, [Airflow](docs/recipes/airflow/), dbt, etc.) |
| Validates row counts, null ratios, and uniqueness | Your warehouse or downstream pipeline |
| Checkpoints progress — resume after crashes | Schema management on the warehouse side |
| Protects the source DB — longest single query ~0.2s on PG / ~9s on MySQL on 2M-row tables | — |

Supported destinations: local filesystem, Amazon S3, Google Cloud Storage, Azure Blob Storage, stdout.
Export modes: `full`, `incremental` (cursor-based), `chunked`, `time_window`.
Formats: Parquet (zstd / snappy / gzip / lz4 / none) and CSV.

**Not for you if you need:**
- **Always-on streaming / continuous replication** — Rivet *does* capture CDC to files (`mode: cdc` — WAL/binlog inserts/updates/deletes into typed Parquet/CSV, one batch of changes per run), but it is not a continuously-running replication sink. For always-on near-real-time streaming into a live target use [Debezium](https://debezium.io/) or [Estuary](https://estuary.dev/).
- **Connectors to SaaS sources** — no Salesforce, Stripe, HubSpot, etc. Use [Airbyte](https://airbyte.com/) or [Fivetran](https://www.fivetran.com/).
- **A managed, always-on extract-and-load platform** — Rivet *does* load: `rivet load` writes a resolved export into BigQuery or Snowflake (a `mode: cdc` config additionally appends a change log and maintains a current-state dedup view). But it runs as a discrete command you schedule, not a continuously-managed EL service like [Fivetran](https://www.fivetran.com/) or [Airbyte](https://airbyte.com/); for warehouses it doesn't target (Redshift, Databricks, …) reach for [dlt](https://dlthub.com/) or [Sling](https://slingdata.io/).
- **In-warehouse transformation** — Rivet lands raw, faithfully-typed data; modelling, tests, and business logic (the `T` in ELT) are [dbt](https://www.getdbt.com/)'s or Spark's job.
- **A Kubernetes data platform** — Rivet runs as a single binary in a `Job` or `CronJob`; a full operator is a different architecture.

**Documentation language:** English-only. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Core workflow

```
rivet init      # scaffold config from a live DB (discovers tables, infers cursors)
rivet doctor    # verify credentials and destination auth before the run
rivet check     # validate config logic, warn about chunking and cursor choices
rivet plan      # seal execution intent — reviewable JSON artifact, no writes yet
rivet run       # execute the plan; checkpoint each chunk
rivet validate  # verify row counts and manifest against the destination
```

Branch commands: `rivet apply` (replay a saved plan, **or** run a config's exports wave-by-wave), `rivet reconcile` (compare manifest vs destination), `rivet repair` (re-upload orphaned chunks), `rivet state` (inspect progression and checkpoints).

For a first run, `rivet init + rivet run` is enough. The full workflow is for production pipelines where "it ran" is not sufficient — you need a verifiable record of what was written.

**Many tables, one command.** For a config with several exports, `rivet plan` assigns each a `wave:` (a priority band by size / strategy / risk) and writes it back into the config; `rivet apply rivet.yaml` then runs them wave by wave, lowest first, with a barrier between waves. With `parallel_export_processes: true` (or `rivet apply --parallel-export-processes`), the cheap (low-cost) exports within a wave run as concurrent processes while heavier ones — which already chunk-parallelize internally — run alone; see [docs/getting-started.md § 5](docs/getting-started.md#5--many-tables-plan-once-apply-by-waves).

## Stateless deployment

By default Rivet keeps cursors, manifests, chunk checkpoints, and the run journal in a SQLite file (`.rivet_state.db`) next to your config — perfect for local and single-node runs. For ephemeral containers / Kubernetes pods, set `RIVET_STATE_URL` to a PostgreSQL connection string and Rivet creates and migrates the state schema on first connect — no manual DDL, no init job. Details: [docs/reference/cli.md § State backend](docs/reference/cli.md#state-backend).

```bash
export RIVET_STATE_URL="postgresql://rivet:secret@state-db.internal/rivet_state?sslmode=require"
rivet run -c rivet.yaml
```

## More walkthroughs

[plan / apply](https://raw.githubusercontent.com/panchenkoai/rivet/main/docs/gifs/plan-apply.gif) · [plan campaign — multi-export waves](https://raw.githubusercontent.com/panchenkoai/rivet/main/docs/gifs/plan-campaign.gif) · [reconcile + repair](https://raw.githubusercontent.com/panchenkoai/rivet/main/docs/gifs/reconcile-repair.gif) · [parallel cards UI](https://raw.githubusercontent.com/panchenkoai/rivet/main/docs/gifs/parallel-cards.gif) · [composite cursor (COALESCE fallback)](https://raw.githubusercontent.com/panchenkoai/rivet/main/docs/gifs/coalesce-cursor.gif) · [pool detection](https://raw.githubusercontent.com/panchenkoai/rivet/main/docs/gifs/pool-detect.gif) · [discovery artifact (`rivet init --discover`)](https://raw.githubusercontent.com/panchenkoai/rivet/main/docs/gifs/discover-artifact.gif) · [post-run inspect](https://raw.githubusercontent.com/panchenkoai/rivet/main/docs/gifs/inspect.gif) · [CDC — batch + cdc on the same table, parallel](https://raw.githubusercontent.com/panchenkoai/rivet/main/docs/gifs/cdc-parallel.gif) · [CDC access error (missing grant)](https://raw.githubusercontent.com/panchenkoai/rivet/main/docs/gifs/error-cdc-access.gif). Source scripts in [docs/gifs/](https://github.com/panchenkoai/rivet/tree/main/docs/gifs).

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

**Verify the download** against the published checksums (every release ships `SHA256SUMS.txt`):

```bash
# Download the tarball + SHA256SUMS.txt from the same release, then:
sha256sum -c SHA256SUMS.txt        # Linux
shasum -a 256 -c SHA256SUMS.txt    # macOS
```

### Docker

```bash
docker run --rm ghcr.io/panchenkoai/rivet:latest --version

docker run --rm \
  -e DATABASE_URL="postgresql://user:pass@host.docker.internal:5432/db" \
  -v $(pwd)/examples/rivet.yaml:/config/rivet.yaml \
  -v $(pwd)/output:/output \
  ghcr.io/panchenkoai/rivet:latest \
  run -c /config/rivet.yaml
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

### Running tests

Tests run under [cargo-nextest](https://nexte.st) — it executes each test in its own process:

```bash
cargo install cargo-nextest --locked    # one-time
cargo nextest run                        # offline suite (live tests are #[ignore], skipped)

# Live engine tests need the docker services first:
docker compose up -d                     # services match docker-compose.yaml
make test-live                           # sweep stale fixtures, then run offline + live
```

A killed live run (slow-timeout / Ctrl-C) skips the per-test table cleanup, so `make test-live` first
runs `make sweep-test-db` to drop any `<prefix>_<pid>_<counter>` fixtures a prior interrupted run left in
the shared `rivet` database. `make sweep-test-db` is safe to run by hand anytime — it only matches those
ephemeral fixtures, never the `init.sql` / `seed.rs` seeded tables.

The offline integration tests are consolidated into single binaries (`tests/offline_suite.rs`,
`tests/live_suite.rs`) to keep link time down. **Run them with nextest, not plain `cargo test`** — the
consolidated binaries run their tests as threads in one process, so without nextest's per-test process
isolation a crashing or global-state test can take its siblings down with it. The pre-push hook
(`git config core.hooksPath .githooks`) and CI both use nextest.

---

## Resource-aware extraction

These are production-safety primitives, not performance knobs.

### Memory controls

| Setting | What it controls |
|---------|-----------------|
| `tuning.max_batch_memory_mb` | Hard cap on a single Arrow batch. When exceeded, the `on_batch_memory_exceeded` policy fires. |
| `tuning.on_batch_memory_exceeded` | `warn` (log + continue) · `fail` (abort) · `auto_shrink` (split batch recursively, then continue) |
| `tuning.memory_threshold_mb` | Process-level RSS guard — pauses fetching when RSS exceeds the threshold |
| `tuning.batch_size_memory_mb` | Memory-driven batch sizing: Rivet samples the first batch to estimate row width, then sizes each batch to that memory target — large on narrow tables, small on wide ones. **On by default** (32 MB on `balanced`, 64 MB on `fast`); set it explicitly to override. |

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
| **Cloud permissions** (least-privilege IAM / RBAC / SAS per command) | [docs/cloud-permissions.md](docs/cloud-permissions.md) |
| **Cross-tool benchmark harness** | [docs/bench/](docs/bench/) |
| Export modes (`full`, `incremental`, `chunked`, `time_window`) | [docs/modes/](docs/modes/) |
| Destinations (local, S3, GCS, Azure, stdout) | [docs/destinations/](docs/destinations/) |
| Config YAML reference | [docs/reference/config.md](docs/reference/config.md) |
| CLI commands and flags | [docs/reference/cli.md](docs/reference/cli.md) |
| Tuning profiles | [docs/reference/tuning.md](docs/reference/tuning.md) |
| Scaffold config from a live DB (`rivet init`) | [docs/reference/init.md](docs/reference/init.md) |
| Pipeline, traits, memory model, source layout | [docs/architecture.md](docs/architecture.md) |
| Demo on a pre-seeded 14-table fixture (~10 min) | [docs/pilot/demo-quickstart.md](docs/pilot/demo-quickstart.md) |
| Pilot walkthrough — discovery → reconcile → repair | [docs/pilot/pilot-walkthrough.md](docs/pilot/pilot-walkthrough.md) |
| Production checklist | [docs/pilot/production-checklist.md](docs/pilot/production-checklist.md) |
| Operator recipes (resume, idempotent load) | [docs/recipes/](docs/recipes/) |
| Architecture decision records | [docs/adr/](docs/adr/) |
| Contributing, tests, CI | [CONTRIBUTING.md](CONTRIBUTING.md) |

---

## Releases and roadmap

- **Latest release and version history:** [CHANGELOG.md](CHANGELOG.md).
- **Strategy and execution tracker:** [rivet_roadmap.md](rivet_roadmap.md) — the single source of truth for what is shipped and what is open.
- **Questions, issues, feature requests:** [GitHub Issues](https://github.com/panchenkoai/rivet/issues).
