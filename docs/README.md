# Rivet Documentation

Rivet exports data from PostgreSQL and MySQL to Parquet/CSV files on local disk, S3, GCS, or stdout.

Install from Rust: `cargo install rivet-cli` (crates.io name is `rivet-cli`; the binary is `rivet`). Other install options live in the repo [README](../README.md).

This folder contains modular guides for running exports, a complete configuration and CLI reference, an architecture overview, and the full set of architecture decision records.

## Supported database versions

Every release is exercised against the full end-to-end suite on each of the following:

| Engine     | Versions covered by CI matrix |
|------------|-------------------------------|
| PostgreSQL | **12, 13, 14, 15, 16**        |
| MySQL      | **5.7, 8.0**                  |

See [reference/compatibility.md](reference/compatibility.md) for the version-support policy, the exact test matrix, and notes on engine-specific features.

## Start Here

| Guide | Description |
|-------|-------------|
| [Getting Started](getting-started.md) | Install Rivet, connect to your database, run your first export |
| [Quickstart: Postgres](pilot/quickstart-postgres.md) | One-table export in 5 minutes (PostgreSQL) |
| [Quickstart: MySQL](pilot/quickstart-mysql.md) | One-table export in 5 minutes (MySQL) |
| [Demo quickstart](pilot/demo-quickstart.md) | Scripted pilot demo on a pre-seeded 14-table fixture — prioritization, composite cursor, reconcile, repair, progression (≈10 min) |
| [Pilot walkthrough](pilot/pilot-walkthrough.md) | Conceptual end-to-end tour on your own data |

Short terminal walkthroughs in [gifs/](gifs/):

- [gifs/basic.gif](gifs/basic.gif) — scaffold config, preflight, run, inspect state (≈25 s)
- [gifs/plan-apply.gif](gifs/plan-apply.gif) — sealed plan/apply with credential redaction (≈20 s)
- [gifs/reconcile-repair.gif](gifs/reconcile-repair.gif) — chunked + reconcile + targeted repair (≈35 s)

## Export Modes

| Mode | When to Use | Guide |
|------|-------------|-------|
| **full** | Snapshot the entire result set each run | [modes/full.md](modes/full.md) |
| **incremental** | Only export rows newer than the last cursor | [modes/incremental.md](modes/incremental.md) · [composite cursor](modes/incremental-coalesce.md) |
| **chunked** | Split large tables into parallel ranges by ID; terminal progress bar while chunks run | [modes/chunked.md](modes/chunked.md) |
| **time_window** | Export a rolling N-day window | [modes/time-window.md](modes/time-window.md) |

## Destinations

| Destination | Guide |
|-------------|-------|
| Local filesystem | [destinations/local.md](destinations/local.md) |
| AWS S3 / MinIO / R2 | [destinations/s3.md](destinations/s3.md) |
| Google Cloud Storage | [destinations/gcs.md](destinations/gcs.md) |
| Stdout (pipe) | [destinations/stdout.md](destinations/stdout.md) |

## Reference

| Topic | Guide |
|-------|-------|
| Complete YAML config reference | [reference/config.md](reference/config.md) |
| CLI commands and flags | [reference/cli.md](reference/cli.md) |
| Tuning profiles and parameters | [reference/tuning.md](reference/tuning.md) |
| `rivet init` — scaffold YAML from the database | [reference/init.md](reference/init.md) |
| Supported PostgreSQL / MySQL versions and test matrix | [reference/compatibility.md](reference/compatibility.md) |
| Offline + live test matrix, harness, fault-injection hook | [reference/testing.md](reference/testing.md) |

## Architecture

| Topic | Guide |
|-------|-------|
| Data flow, pluggable traits, memory model, source layout | [architecture.md](architecture.md) |
| Source-aware extraction prioritization (advisory) | [planning/prioritization.md](planning/prioritization.md) |

## Production

| Topic | Guide |
|-------|-------|
| Production checklist | [pilot/production-checklist.md](pilot/production-checklist.md) |
| UAT checklist (pilot sign-off) | [pilot/uat-checklist.md](pilot/uat-checklist.md) |
| Plan/Apply for auditable extraction | [reference/cli.md#rivet-plan](reference/cli.md#rivet-plan) · [adr/0005-plan-apply-contracts.md](adr/0005-plan-apply-contracts.md) |
| Reconcile / targeted repair | [reference/cli.md#rivet-reconcile](reference/cli.md#rivet-reconcile) · [adr/0009-reconcile-and-repair-contracts.md](adr/0009-reconcile-and-repair-contracts.md) |
| Committed / verified progression | [reference/cli.md#rivet-state-progression](reference/cli.md#rivet-state-progression) · [adr/0008-export-progression.md](adr/0008-export-progression.md) |

## Architecture Decision Records

| # | Title |
|---|-------|
| [0001](adr/0001-state-update-invariants.md) | State update invariants (I1–I7) |
| [0002](adr/0002-cli-product-vs-library.md) | CLI product vs library |
| [0003](adr/0003-layer-classification.md) | Layer classification |
| [0004](adr/0004-destination-write-contracts.md) | Destination write contracts |
| [0005](adr/0005-plan-apply-contracts.md) | Plan/Apply contracts (PA1–PA9) |
| [0006](adr/0006-source-aware-prioritization.md) | Source-aware extraction prioritization |
| [0007](adr/0007-cursor-policy-contracts.md) | Cursor policy — single-column / coalesce (CC1–CC10) |
| [0008](adr/0008-export-progression.md) | Committed / verified progression (PG1–PG8) |
| [0009](adr/0009-reconcile-and-repair-contracts.md) | Reconcile and targeted repair (RC1–RC6, RR1–RR8) |

## Example Configs

Ready-to-use YAML templates live in the [`examples/`](../examples/) directory. To scaffold YAML from a live database (`rivet init`), see [reference/init.md](reference/init.md) and the root [`docker-compose.yaml`](../docker-compose.yaml).
