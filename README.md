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

<h3 align="center">Production database → your warehouse, without hurting either.</h3>

<p align="center">
One ~30 MB Rust binary. <b>PostgreSQL · MySQL · SQL Server · MongoDB</b> → Parquet/CSV on <b>S3 · GCS · Azure · local</b> → <b>BigQuery · Snowflake</b>.<br>
Batch snapshots or log-based change data capture. Resumable, verifiable, source-safe.
</p>

<p align="center">
  <img src="docs/assets/rivet-flow.svg" alt="PostgreSQL, MySQL, SQL Server and MongoDB → rivet → S3, GCS, Azure Blob or local disk → BigQuery or Snowflake" width="900">
</p>

## What you get

- **Source-safe by construction.** Short keyset/chunked reads, a server-side cursor on PostgreSQL, CDC that reads the log instead of querying tables. rivet detects pgBouncer / Odyssey / ProxySQL / MaxScale and the Azure SQL gateway, and warns at run start when a sparse key would split one export into thousands of near-empty queries.
- **Change data capture on four engines, at-least-once.** Checkpoints sit on transaction boundaries, a large transaction survives a mid-flush crash whole, and PostgreSQL's slot advances only after the parts are durable. Each release is diffed against Debezium on the same change window.
- **A warehouse that follows the source.** On BigQuery, `rivet load` writes base tables with a `__is_deleted` flag and appends later changes to `<table>__changes`; `rivet compact` merges them in with one `MERGE` per table and drops the buffer — no full reloads. On Snowflake, `rivet load` keeps a change log and a current-state view.
- **Proof, not just a green exit code.** Every part is in a manifest with its row count and MD5. `rivet validate` re-reads what landed, `rivet reconcile` recounts the source chunk by chunk, `rivet repair` re-exports only the chunks it flagged, and the run journal records what committed.
- **Resumable everywhere.** A killed chunked, keyset or CDC run resumes from its last durable unit. Parts are named per run, so a re-run never overwrites the previous one.
- **One binary, no platform.** State lives in SQLite next to the config, or in Postgres (`RIVET_STATE_URL`) for containers and shared runners. Run it from cron, Airflow, a Kubernetes `CronJob`, or `docker run`. [`rivet-mcp`](docs/reference/mcp.md) lets an AI agent check source health before an extract.

What is guaranteed, what is at-least-once, and what is not covered: [docs/semantics.md](docs/semantics.md). What CI, nightly and the release gate actually exercise: [docs/reliability-matrix.md](docs/reliability-matrix.md).

## Quickstart

```bash
brew install panchenkoai/rivet/rivet
export DATABASE_URL="postgresql://user:pass@host/db"

rivet init --source-env DATABASE_URL -o rivet.yaml   # discovers tables, keys, cursors, types
rivet run  -c rivet.yaml                             # typed Parquet in ./output/
```

**Change data capture** — every INSERT / UPDATE / DELETE from the MySQL binlog, a PostgreSQL logical slot, SQL Server change tables or a MongoDB change stream:

```bash
rivet init --source-env DATABASE_URL --mode cdc -o cdc.yaml   # one stream over every table
rivet run  -c cdc.yaml                                        # first run: baseline snapshot; every run: drain to the log's current end, then exit
```

**All the way into the warehouse** — generate the config with a bucket and a dataset, then one cycle per schedule tick:

```bash
bq mk -d my-project:raw      # the dataset must exist; rivet load does not create it
rivet init --source-env DATABASE_URL --mode cdc \
  --gcs-bucket my-bucket --bigquery-project my-project --bigquery-dataset raw -o cdc.yaml

rivet run     -c cdc.yaml    # capture changes → Parquet in GCS
rivet load    -c cdc.yaml    # the first run's snapshot → base tables; later runs → <table>__changes
rivet compact -c cdc.yaml    # MERGE each buffer into its base table, drop the buffer
```

Put those three lines in cron or [Airflow](docs/recipes/airflow/) and the warehouse follows the source. Walkthrough: [docs/cdc-full-cycle.md](docs/cdc-full-cycle.md) · first run in depth: [docs/getting-started.md](docs/getting-started.md) · no database handy: [docs/pilot/demo-quickstart.md](docs/pilot/demo-quickstart.md) (pre-seeded 14-table fixture, ~10 min).

## Is it for you?

**Yes**, if you move tables from an operational PostgreSQL / MySQL / SQL Server / MongoDB into files or a warehouse, the source is production, and "it ran" is not enough — you want a record of what landed.

**Look elsewhere** for SaaS sources (Airbyte, Fivetran), an always-on sub-second replication sink (Debezium, Estuary), warehouses rivet does not load (dlt, Sling), or in-warehouse modelling (dbt). rivet runs as a command you schedule, not a hosted service.

## Install

```bash
brew install panchenkoai/rivet/rivet                   # macOS / Linux
cargo install rivet-cli                                # crates.io (the crate is rivet-cli; the binary is rivet), Rust 1.94+
docker run --rm ghcr.io/panchenkoai/rivet:latest --version
```

<details>
<summary>Pre-built binaries, checksums, building from source</summary>

Release assets are versioned (`rivet-<version>-<target>.tar.gz`) and ship with `SHA256SUMS.txt` and a cosign bundle:

```bash
VERSION=$(curl -s https://api.github.com/repos/panchenkoai/rivet/releases/latest | grep -oE '"tag_name": *"[^"]+"' | cut -d'"' -f4)
BASE="https://github.com/panchenkoai/rivet/releases/download/$VERSION"
curl -L "$BASE/rivet-$VERSION-aarch64-apple-darwin.tar.gz" | tar xz      # or x86_64-apple-darwin,
sudo mv rivet-*/rivet /usr/local/bin/                                     # x86_64-unknown-linux-gnu, aarch64-unknown-linux-gnu
```

Verify with `shasum -a 256 -c SHA256SUMS.txt` (macOS) or `sha256sum -c SHA256SUMS.txt` (Linux). Windows: `cargo install rivet-cli`.

From a container, `localhost` is not your machine — use `host.docker.internal` (Docker Desktop) or `--add-host=host.docker.internal:host-gateway` on Linux.

```bash
git clone https://github.com/panchenkoai/rivet.git && cd rivet
cargo build --release          # target/release/rivet
```

Running the test suites: [CONTRIBUTING.md § Running tests](CONTRIBUTING.md#running-tests).
</details>

## Documentation

| | |
|---|---|
| Start here | [getting started](docs/getting-started.md) · [concepts](docs/concepts.md) · [who is this for](docs/who-is-this-for.md) · [all docs](docs/README.md) |
| Run it in production | [pilot guide](docs/pilot/README.md) · [production checklist](docs/pilot/production-checklist.md) · [best practices](docs/best-practices/) · [recipes](docs/recipes/) |
| CDC and the warehouse | [CDC reference](docs/reference/cdc.md) · [full cycle](docs/cdc-full-cycle.md) · [BigQuery load](docs/cdc-bigquery-load.md) |
| Reference | [config](docs/reference/config.md) · [CLI](docs/reference/cli.md) · [tuning](docs/reference/tuning.md) · [init](docs/reference/init.md) · [destinations](docs/destinations/) |
| Trust | [semantics](docs/semantics.md) · [reliability matrix](docs/reliability-matrix.md) · [security](SECURITY.md) · [cloud permissions](docs/cloud-permissions.md) · [benchmarks](docs/bench/) |
| Internals | [architecture](docs/architecture.md) · [ADRs](docs/adr/) · [contributing](CONTRIBUTING.md) |

> Generated files — `.rivet_state.db`, `plan.json`, `*.journal.jsonl`, the exports themselves — can hold SQL, cursor values and data. Keep them out of git: [SECURITY.md § Sensitive local artifacts](SECURITY.md#sensitive-local-artifacts).

Releases: [CHANGELOG.md](CHANGELOG.md) · roadmap: [rivet_roadmap.md](rivet_roadmap.md) · questions and issues: [GitHub Issues](https://github.com/panchenkoai/rivet/issues) · [Discord](https://discord.gg/HT5DZNzNU).
