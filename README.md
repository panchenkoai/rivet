# Rivet

**Lightweight, source-safe data extraction from PostgreSQL and MySQL to Parquet/CSV.**

Rivet is a single-binary CLI that exports query results from relational databases to files — locally, to S3, to GCS, or to stdout. It is **extract-only** (no loading, no merging, no CDC) and designed to be gentle on production databases through tuning profiles, preflight health checks, and intelligent retry with backoff.

> **Names.** The project and CLI are **Rivet**; the command is **`rivet`**. On [crates.io](https://crates.io/crates/rivet-cli) the package is published as **`rivet-cli`** because the crate name `rivet` was already taken. Homebrew and release archives install the **`rivet`** binary.

## What Rivet is (and is not)

Rivet does extraction end-to-end — query, batch, retry, validate, reconcile, checkpoint, plan/apply — from PostgreSQL 12–16 and MySQL 5.7 / 8.0 to Parquet (zstd / snappy / gzip / lz4 / none) or CSV. Destinations: local, Amazon S3, Google Cloud Storage, stdout. See [docs/](docs/) for the full feature list and contracts.

Rivet is **not** a loader, a CDC platform, an ELT orchestrator, or a SaaS connector marketplace. It deliberately stops at "file on disk / in a bucket" — you bring the warehouse side yourself.

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
| Quickstart: PostgreSQL | [docs/pilot/quickstart-postgres.md](docs/pilot/quickstart-postgres.md) |
| Quickstart: MySQL | [docs/pilot/quickstart-mysql.md](docs/pilot/quickstart-mysql.md) |
| Demo on a pre-seeded 14-table fixture (~10 min) | [docs/pilot/demo-quickstart.md](docs/pilot/demo-quickstart.md) |
| Pilot walkthrough — discovery → reconcile → repair | [docs/pilot/pilot-walkthrough.md](docs/pilot/pilot-walkthrough.md) |
| Production checklist | [docs/pilot/production-checklist.md](docs/pilot/production-checklist.md) |
| Architecture decision records | [docs/adr/](docs/adr/) |
| Contributing, tests, CI | [CONTRIBUTING.md](CONTRIBUTING.md) |

---

## Releases and roadmap

- **Latest release and version history:** [CHANGELOG.md](CHANGELOG.md).
- **Strategy, pains, and execution tracker:** [rivet_roadmap.md](rivet_roadmap.md) — the single source of truth for what is shipped and what is open.
