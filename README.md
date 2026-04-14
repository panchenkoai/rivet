# Rivet

**Lightweight, source-safe data extraction from PostgreSQL and MySQL to Parquet/CSV.**

**Names:** the project and CLI are **Rivet**; the command you run is **`rivet`**. On [crates.io](https://crates.io/crates/rivet-cli) the package is published as **`rivet-cli`** because the crate name `rivet` was already taken. Homebrew and release archives still install the **`rivet`** binary.

Rivet is a CLI tool that exports query results from relational databases to files — locally or in cloud storage (S3, GCS). It is **extract-only**: no loading, no merging, no CDC. It is designed to be gentle on production databases through tuning profiles, preflight health checks, and intelligent retry with backoff.

### What Rivet does

- Extracts data from **PostgreSQL** and **MySQL** via standard SQL queries
- Writes **Parquet** (zstd-compressed by default; snappy, gzip, lz4, none) or **CSV** files
- Uploads to **local disk**, **Amazon S3**, **Google Cloud Storage**, or **stdout** (pipe workflows)
- Tracks incremental state in **SQLite** so the next run picks up where the last left off
- Diagnoses source health **before** extraction (`rivet check`)
- Verifies auth for all sources and destinations **before** running (`rivet doctor`)
- Prints a structured **run summary** after each export (run ID, rows, files, bytes, duration, RSS, retries, schema changes)
- Persists **metrics history**, **schema tracking**, and **file manifest** in SQLite
- Recommends **parallelism level** and **tuning profile** in preflight checks
- **Parameterized queries** via `--param key=value` and `${key}` placeholders
- **Data quality checks** — row count bounds, null ratio thresholds, uniqueness assertions
- **File size splitting** — `max_file_size: 512MB` automatically splits output into parts
- **Memory-based batch sizing** — `batch_size_memory_mb: 256` auto-tunes batch size from schema width
- **Slack notifications** on failure, schema change, or degraded verdict
- **Plan/Apply workflow** — sealed execution artifacts for auditable, pre-reviewed runs

### What Rivet does NOT do

- **No loading/merging** — it produces files; you bring them into a warehouse yourself
- **No CDC** — no WAL/binlog reading; query-based extraction only
- **No orchestration** — no built-in scheduler; use cron, Airflow, or similar
- **No exactly-once delivery** — at-least-once; duplicates are possible on crash recovery
- **No web UI / API** — CLI and YAML config only

**Documentation language:** English-only. See [CONTRIBUTING.md](CONTRIBUTING.md).

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
| First run — install, connect, export | [docs/getting-started.md](docs/getting-started.md) |
| All docs (modes, destinations, reference, pilot) | [docs/README.md](docs/README.md) |
| Export modes (`full`, `incremental`, `chunked`, `time_window`) | [docs/modes/](docs/modes/) |
| Destinations (local, S3, GCS, stdout) | [docs/destinations/](docs/destinations/) |
| Config YAML reference | [docs/reference/config.md](docs/reference/config.md) |
| CLI commands and flags | [docs/reference/cli.md](docs/reference/cli.md) |
| Tuning profiles | [docs/reference/tuning.md](docs/reference/tuning.md) |
| Scaffold config from a live DB (`rivet init`) | [docs/reference/init.md](docs/reference/init.md) |
| Quickstart: PostgreSQL | [docs/pilot/quickstart-postgres.md](docs/pilot/quickstart-postgres.md) |
| Quickstart: MySQL | [docs/pilot/quickstart-mysql.md](docs/pilot/quickstart-mysql.md) |
| Production checklist | [docs/pilot/production-checklist.md](docs/pilot/production-checklist.md) |
| Contributing, tests, CI | [CONTRIBUTING.md](CONTRIBUTING.md) |

---

## Roadmap

See [rivet_roadmap.md](rivet_roadmap.md) for the full roadmap (strategy + execution status).

| Milestone | Status | Focus |
|-----------|--------|-------|
| **v0.2.0-beta.7** | ✅ Released | Architecture stabilisation (Waves 1–3), plan/apply workflow, invariant/recovery/compatibility tests, semantic release gates |
| **v0.2.0** (stable) | 🔜 Pending stable tag | Durable state backend (PostgreSQL), pilot schedule automation, docs audit/reconcile tradeoffs |
| **v0.3.0** | Planned | Schema drift policy hooks, data shape drift detection, benchmark harness, curated example configs |
| **Future** | Later | CDC mode, Iceberg/Delta output, webhook destination, multi-source joins, plugin system |
