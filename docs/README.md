# Rivet Documentation

Rivet exports data from PostgreSQL and MySQL to Parquet/CSV files on local disk, S3, or GCS.

Install from Rust: `cargo install rivet-cli` (the crates.io name is `rivet-cli`; the binary is `rivet`).

This folder contains modular guides for running your first pilot exports and a complete configuration reference.

## Start Here

| Guide | Description |
|-------|-------------|
| [Getting Started](getting-started.md) | Install Rivet, connect to your database, run your first export |
| [Quickstart: Postgres](pilot/quickstart-postgres.md) | End-to-end pilot in 5 minutes (PostgreSQL) |
| [Quickstart: MySQL](pilot/quickstart-mysql.md) | End-to-end pilot in 5 minutes (MySQL) |

## Export Modes

| Mode | When to Use | Guide |
|------|-------------|-------|
| **full** | Snapshot the entire result set each run | [modes/full.md](modes/full.md) |
| **incremental** | Only export rows newer than the last cursor | [modes/incremental.md](modes/incremental.md) |
| **chunked** | Split large tables into parallel ranges by ID; terminal progress bar while chunks run (see guide) | [modes/chunked.md](modes/chunked.md) |
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
| Tuning profiles and parameters | [reference/tuning.md](reference/tuning.md) |
| CLI commands and flags | [reference/cli.md](reference/cli.md) |
| `rivet init` (scaffold YAML from the database) | [reference/init.md](reference/init.md) |

## Production

| Topic | Guide |
|-------|-------|
| Production checklist | [pilot/production-checklist.md](pilot/production-checklist.md) |
| UAT checklist (pilot sign-off) | [pilot/uat-checklist.md](pilot/uat-checklist.md) |
| Plan/Apply for auditable extraction | [reference/cli.md#rivet-plan](reference/cli.md) · [adr/0005-plan-apply-contracts.md](adr/0005-plan-apply-contracts.md) |

## Example Configs

Ready-to-use YAML templates live in the [`examples/`](../examples/) directory. To scaffold YAML from a live database (`rivet init`), see [reference/init.md](reference/init.md) and the root [`docker-compose.yaml`](../docker-compose.yaml).

