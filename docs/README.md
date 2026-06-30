# Rivet Documentation

Rivet exports data from PostgreSQL and MySQL to Parquet/CSV files on local disk, S3, GCS, Azure Blob Storage, or stdout.

Install from Rust: `cargo install rivet-cli` (crates.io name is `rivet-cli`; the binary is `rivet`). Other install options live in the repo [README](../README.md).

This folder contains modular guides for running exports, a complete configuration and CLI reference, an architecture overview, and the full set of architecture decision records.

## Supported database versions

Every release is exercised against the full end-to-end suite on each of the following:

| Engine     | Versions covered by CI matrix |
|------------|-------------------------------|
| PostgreSQL | **12, 13, 14, 15, 16**        |
| MySQL      | **5.7, 8.0**                  |

See [reference/compatibility.md](reference/compatibility.md) for the version-support policy, the exact test matrix, and notes on engine-specific features.

## Start here

Pick one — they're ordered shortest to deepest. Read top-to-bottom, then come back to this index when you need a reference.

| Guide | What it gives you | Time |
|-------|-------|------|
| [Who is Rivet for?](who-is-this-for.md) | Yes / no fit-check with named alternatives (Debezium / Airbyte / Fivetran / dbt / DuckDB) | ~1 min |
| [Getting Started](getting-started.md) | Install + your first export from a real table | ~3 min read · ~5 min hands-on |
| [Concepts glossary](concepts.md) | One-page orientation: `run_id`, `cursor`, `chunk`, `manifest`, `journal`, `progression` | ~3 min |
| [Pilot guide](pilot/README.md) | Operator runbook — full flow on your own database, production-ready guardrails | 1–2 sessions |

Short terminal walkthroughs in [gifs/](gifs/):

- [gifs/basic.gif](gifs/basic.gif) — scaffold config, preflight, run, inspect state (≈25 s)
- [gifs/plan-apply.gif](gifs/plan-apply.gif) — sealed plan/apply with credential redaction (≈20 s)
- [gifs/reconcile-repair.gif](gifs/reconcile-repair.gif) — chunked + reconcile + targeted repair (≈35 s)

## Export Modes

| Mode | When to Use | Guide |
|------|-------------|-------|
| **full** | Snapshot the entire result set each run | [modes/full.md](modes/full.md) |
| **incremental** | Only export rows newer than the last cursor | [modes/incremental.md](modes/incremental.md) · [composite cursor](modes/incremental-coalesce.md) |
| **chunked** | Split large tables into parallel ranges by ID, **or by date** (`chunk_by_days: 365` → one chunk per ~year, `>= AND <` semantics); checkpoint + `--resume` for crashed runs | [modes/chunked.md](modes/chunked.md) |
| **time_window** | Export a rolling N-day window | [modes/time-window.md](modes/time-window.md) |
| **cdc** | Stream INSERT/UPDATE/DELETE from the transaction log (MySQL binlog / PostgreSQL logical slot / SQL Server change tables) as typed Parquet/CSV — source-safe, at-least-once | [reference/cdc.md](reference/cdc.md) |

## Destinations

| Destination | Guide |
|-------------|-------|
| Local filesystem | [destinations/local.md](destinations/local.md) |
| AWS S3 / MinIO / R2 | [destinations/s3.md](destinations/s3.md) |
| Google Cloud Storage | [destinations/gcs.md](destinations/gcs.md) |
| Azure Blob Storage | [destinations/azure.md](destinations/azure.md) |
| Stdout (pipe) | [destinations/stdout.md](destinations/stdout.md) |

### Output layout

| Feature | What | Guide |
|---------|------|-------|
| **`partition_by`** | Split rows into Hive-style `col=value/` sub-folders by a date column (`day`/`month`/`year`); NULLs → `__HIVE_DEFAULT_PARTITION__`; orthogonal to `mode` | [partitioning.md](partitioning.md) |

## Reference

| Topic | Guide |
|-------|-------|
| Complete YAML config reference | [reference/config.md](reference/config.md) |
| CLI commands and flags | [reference/cli.md](reference/cli.md) |
| Tuning profiles and parameters | [reference/tuning.md](reference/tuning.md) |
| `rivet cdc` — log-based change data capture: per-engine grants/prereqs, output shape, why it's gentle on the source | [reference/cdc.md](reference/cdc.md) |
| `rivet init` — scaffold YAML from the database | [reference/init.md](reference/init.md) |
| `rivet init --discover` — machine-readable JSON discovery artifact (ranked cursor / chunk candidates, row estimates, on-disk sizes) for automation and code review | [reference/init.md#discovery-artifact---discover](reference/init.md#discovery-artifact---discover) · [gifs/discover-artifact.gif](gifs/discover-artifact.gif) |
| `rivet check --type-report --target bigquery` — per-column type fidelity report + warehouse compatibility (NUMERIC / BIGNUMERIC / TIMESTAMP overflow warnings); `--strict` exits non-zero on lossy mappings | [reference/cli.md#rivet-check](reference/cli.md#rivet-check) |
| Supported PostgreSQL / MySQL versions and test matrix | [reference/compatibility.md](reference/compatibility.md) |
| Offline + live test matrix, harness, fault-injection hook | [reference/testing.md](reference/testing.md) |

## Trust contracts

The five surfaces a serious operator inspects before adopting Rivet. Same five rows, same order, are mirrored at the top of the project [README](../README.md).

| Topic | Guide |
|-------|-------|
| **Execution semantics** — retry, crash, resume, repair, reconcile, known non-guarantees | [semantics.md](semantics.md) |
| **Reliability matrix** — what runs in PR CI vs nightly vs manual; pgBouncer & ProxySQL coverage | [reliability-matrix.md](reliability-matrix.md) |
| **Cloud smoke tests** — last-verified real-cloud matrix per release (S3 / GCS / Azure) | [cloud-smoke-tests.md](cloud-smoke-tests.md) |
| **Release checklist** — every gate every tag must clear before publish | [release-checklist.md](release-checklist.md) |
| **Cloud permissions** — least-privilege IAM / RBAC / SAS scopes for each backend | [cloud-permissions.md](cloud-permissions.md) |
| **Security policy** — what Rivet can access, sensitive artifacts, credential handling, reporting | [../SECURITY.md](../SECURITY.md) |
| **Compatibility matrix** — PG 12–16, MySQL 5.7 / 8.0 versions actually exercised in CI | [reference/compatibility.md](reference/compatibility.md) |
| **Cross-tool benchmark harness** — reproducible PG/MySQL → Parquet vs sling, dlt, duckdb, clickhouse-local, odbc2parquet (defaults + steelman) | [bench/README.md](bench/README.md) |

## Best Practices

Practical guides explaining *why* settings matter and *when* to use them.

| Guide | What it covers |
|-------|---------------|
| [Resource-aware extraction](best-practices/resource-aware-extraction.md) | Memory budgets, `warn`/`fail`/`auto_shrink` policies, RSS formula |
| [Parquet tuning](best-practices/parquet-tuning.md) | Row group strategies, target sizes, downstream read implications |
| [Compression profiles](best-practices/compression-profiles.md) | Profile-to-codec mapping, CPU/size trade-offs |
| [Quality checks](best-practices/quality-checks.md) | Row count gates, null ratio, uniqueness cap (`unique_max_entries`) |
| [Low-memory runners](best-practices/low-memory-runners.md) | Settings for 512 MB–4 GB hosts; `auto_shrink` guarantees and caveats |
| [Recovery and resume](best-practices/recovery-and-resume.md) | `--resume` semantics, crash recovery, state inspection |
| [Benchmark methodology](best-practices/benchmark-methodology.md) | How to run and interpret E2E and Criterion benchmarks; version comparison |
| [Benchmark report v0.5.x](benchmark_report_v0.5.x.md) | Measured results: compression profiles, row group targets, batch memory policies, quality uniqueness |

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

### Operator recipes

Action-first cookbooks for the most common production scenarios.

| Recipe | What it covers |
|--------|----------------|
| [recipes/recover-interrupted-run.md](recipes/recover-interrupted-run.md) | Resume after kill / crash, drive `validate` / `reconcile` / `repair`, unstick a stalled state DB |
| [recipes/idempotent-warehouse-load.md](recipes/idempotent-warehouse-load.md) | Build an idempotent BigQuery / Snowflake loader on top of `manifest.json` + `_SUCCESS` |
| [recipes/airflow/](recipes/airflow/) | Run Rivet on Airflow — a wave-aware DAG generated from `rivet plan` (heavy tables isolated, light ones parallelised, a barrier between waves), with per-table retries and a row-count reconcile gate |

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
| [0010](adr/0010-two-parallel-engines.md) | Two parallel engines (in-process scoped threads vs subprocess fan-out) |
| [0011](adr/0011-source-trait-send-not-sync.md) | `Source: Send` (not `Sync`) — one connection per chunk worker |
| [0012](adr/0012-cloud-manifest-contract.md) | Cloud manifest contract (M1–M9) |
| [0013](adr/0013-trust-flag-contract.md) | Trust flag contract (`--validate`, `--reconcile`, `--resume`) |
| [0014](adr/0014-target-type-materialization.md) | Target type materialization (interchange vs native load; DuckDB/BQ/…) |

## Example Configs

Ready-to-use YAML templates live in the [`examples/`](../examples/) directory. To scaffold YAML from a live database (`rivet init`), see [reference/init.md](reference/init.md) and the root [`docker-compose.yaml`](../docker-compose.yaml).
