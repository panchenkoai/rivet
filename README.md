# Rivet

**Lightweight, source-safe data extraction from PostgreSQL and MySQL to Parquet/CSV.**

Rivet is a single-binary CLI that exports query results from relational databases to files — locally, to S3, to GCS, or to stdout. It is **extract-only** (no loading, no merging, no CDC) and designed to be gentle on production databases through tuning profiles, preflight health checks, and intelligent retry with backoff.

> **Names.** The project and CLI are **Rivet**; the command is **`rivet`**. On [crates.io](https://crates.io/crates/rivet-cli) the package is published as **`rivet-cli`** because the crate name `rivet` was already taken. Homebrew and release archives install the **`rivet`** binary.

## What Rivet is (and is not)

| What Rivet does | What you bring |
|-----------------|----------------|
| Queries PostgreSQL 12–16 and MySQL 5.7 / 8.0 | The database and credentials |
| Streams rows → Arrow → Parquet or CSV | A destination (local path, S3 bucket, GCS bucket) |
| Retries failed batches with exponential backoff | Orchestration (cron, Airflow, dbt, etc.) |
| Validates row counts, null ratios, and uniqueness | Your warehouse or downstream pipeline |
| Checkpoints progress — resume after crashes | Schema management on the warehouse side |
| Protects the source DB via configurable tuning profiles | — |

Supported destinations: local filesystem, Amazon S3, Google Cloud Storage, stdout.
Export modes: `full`, `incremental` (cursor-based), `chunked`, `time_window`.
Formats: Parquet (zstd / snappy / gzip / lz4 / none) and CSV.

**Not for you if you need:**
- **CDC / streaming** — Rivet reads a snapshot per run; it has no event log or replication slot. Use [Debezium](https://debezium.io/) or [Estuary](https://estuary.dev/) instead.
- **A SaaS connector marketplace** — no cloud console, no managed infrastructure, no pre-built connectors beyond Postgres and MySQL.
- **A Kubernetes data platform** — no operator, no Helm chart, no CRD.
- **Loading or transformation** — Rivet stops at "file on disk or in a bucket"; bring dbt, Spark, or your own loader.

**Documentation language:** English-only. See [CONTRIBUTING.md](CONTRIBUTING.md).

## See it run

![Rivet basic workflow — init, doctor, check, run, state](docs/gifs/basic.gif)

More walkthroughs: [plan / apply](docs/gifs/plan-apply.gif) · [reconcile + repair](docs/gifs/reconcile-repair.gif). Source scripts in [docs/gifs/](docs/gifs/).

---

## Installation

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
| First run — install, connect, export | [docs/getting-started.md](docs/getting-started.md) |
| Export modes (`full`, `incremental`, `chunked`, `time_window`) | [docs/modes/](docs/modes/) |
| Destinations (local, S3, GCS, stdout) | [docs/destinations/](docs/destinations/) |
| Config YAML reference | [docs/reference/config.md](docs/reference/config.md) |
| CLI commands and flags | [docs/reference/cli.md](docs/reference/cli.md) |
| Tuning profiles | [docs/reference/tuning.md](docs/reference/tuning.md) |
| Scaffold config from a live DB (`rivet init`) | [docs/reference/init.md](docs/reference/init.md) |
| Pipeline, traits, memory model, source layout | [docs/architecture.md](docs/architecture.md) |
| **Pilot guide (ordered instructions)** | [docs/pilot/README.md](docs/pilot/README.md) |
| Quickstart: PostgreSQL | [docs/pilot/quickstart-postgres.md](docs/pilot/quickstart-postgres.md) |
| Quickstart: MySQL | [docs/pilot/quickstart-mysql.md](docs/pilot/quickstart-mysql.md) |
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
