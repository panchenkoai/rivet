# Reliability Matrix

What Rivet actually tests, where, and how often. This is the operational answer to "is this path covered or am I about to find out the hard way?"

The matrix is derived from the workflows in [.github/workflows/](../.github/workflows/) and the test suites under [tests/](../tests/). It is updated when a coverage tier changes — not on every test addition.

---

## Coverage tiers

| Tier | What runs | Trigger | Wall time budget |
|---|---|---|---|
| **PR CI** | unit + integration + named semantic gates + e2e + type-golden against live PG/MySQL | every push and PR to `main` | ~10 min |
| **Nightly** | full live suite incl. content_load against ~60k-row fixture; pgBouncer profile | 03:30 UTC cron + manual dispatch | up to 60 min |
| **Manual** | 1M-row stress, full legacy DB matrix (PG 12–15, MySQL 5.7), wide-table memory benchmarks | operator-invoked from `dev/` scripts | varies |

PR CI defines branch protection — the named gates (`fmt`, `clippy`, `test`, `test-invariants`, `test-recovery`, `test-compatibility`, `test-stability`) block merges on regression.

---

## Core extraction paths

| Area | PR CI | Nightly | Manual | Suite |
|---|:---:|:---:|:---:|---|
| PostgreSQL — full export | ✅ | ✅ | ✅ | `live_destination_parity`, e2e |
| PostgreSQL — incremental (cursor) | ✅ | ✅ | ✅ | `live_resume`, `live_cli_flags` |
| PostgreSQL — chunked | ✅ | ✅ | ✅ | `live_chunked_recovery`, `live_reconcile_repair` |
| PostgreSQL — time_window | ✅ | ✅ | — | `time_window`, `live_cli_flags` |
| MySQL — full export | ✅ | ✅ | partial | `live_destination_parity`, e2e |
| MySQL — incremental (cursor) | ✅ | ✅ | partial | `live_resume` |
| MySQL — chunked | ✅ | ✅ | partial | `live_chunked_recovery` |
| MySQL — time_window | ✅ | ✅ | — | `time_window` |
| Cross-DB parity (PG ↔ MySQL same query) | ✅ | ✅ | — | `live_cross_db_parity` |

---

## Failure-mode coverage

| Scenario | PR CI | Nightly | Manual | Suite |
|---|:---:|:---:|:---:|---|
| State invariants (ADR-0001 I1–I7) | ✅ gate | ✅ | — | `invariants` |
| Journal event ordering | ✅ gate | ✅ | — | `journal_invariants` |
| Chunk checkpoint resume (I5, I6) | ✅ gate | ✅ | — | `recovery` |
| Crash and resume (live DB, Postgres) | ✅ | ✅ | ✅ | `live_crash_recovery` |
| Crash and resume (live DB, MySQL) | ✅ | ✅ | — | `live_mysql_crash_recovery` (parallel matrix to the PG suite) |
| Chunked checkpoint resume (live DB, MySQL) | ✅ | ✅ | — | `live_mysql_chunked_recovery` (C1–C4 twins) |
| Resume across modes (live DB, MySQL) | ✅ | ✅ | — | `live_mysql_resume` (full / incremental / chunked --resume validation) |
| Schema drift (live DB, MySQL) | ✅ | ✅ | — | `live_mysql_schema_drift` (added / removed / stable matrix) |
| Retry + Toxiproxy faults (live DB, MySQL) | ✅ | ✅ | — | `live_mysql_retry_and_faults` (baseline / latency / disabled / mid-stream / permanent) |
| Reconcile + targeted repair (live DB, MySQL) | ✅ | ✅ | — | `live_mysql_reconcile_repair` (RR1–RR6 twins) |
| Retry classification under injected faults | ✅ | ✅ | — | `live_retry_and_faults`, `retry_integration` |
| Toxiproxy-driven network chaos | ✅ | ✅ | — | `live_chaos` |
| Schema drift between runs | ✅ | ✅ | — | `live_schema_drift`, `schema_evolution` |
| Reconcile + repair flow | ✅ | ✅ | — | `live_reconcile_repair` |
| Plan/apply contract (ADR-0005) | ✅ | ✅ | — | `live_plan_apply`, `validate_regression` |
| State-DB schema compatibility (v1 → v4 migrations) | ✅ | ✅ | — | `state_compat` |
| Config fuzz + planner fuzz | ✅ | ✅ | — | `config_fuzz`, `planner_fuzz` |
| Secret redaction in errors / artifacts | ✅ | ✅ | — | `config_secrets` |
| Gremlin (concurrent state mutation) | ✅ | ✅ | — | `gremlin` |

---

## Destination coverage

| Backend | PR CI | Nightly | Manual | Notes |
|---|:---:|:---:|:---:|---|
| Local filesystem | ✅ | ✅ | ✅ | Default for unit + e2e |
| S3 (MinIO container) | ✅ | ✅ | partial | `live_destination_parity` |
| GCS (fake-gcs container) | ✅ | ✅ | partial | `live_destination_parity` |
| stdout | ✅ | ✅ | — | Constrained — rejects chunked + max_file_size |

Per-backend commit contracts: [ADR-0004](adr/0004-destination-write-contracts.md). Production credentials for real S3 / GCS endpoints are not exercised in CI.

---

## Pool and load pressure

| Scenario | PR CI | Nightly | Manual | Notes |
|---|:---:|:---:|:---:|---|
| pgBouncer (transaction mode, pool_size=1) | ✅ | ✅ | ✅ | `live_pool_safety` — F1–F6 / G1 DBA-audit fixes |
| ProxySQL (MySQL transaction-persistent pool) | — | ✅ | ✅ | `live_pool_safety::mysql_proxysql_*` — detection + cleanup-through-proxy |
| MySQL proxy / multiplexer classification (unit) | ✅ | ✅ | — | `source::mysql::tests::proxy_*` — pure classifier over the 4 signals |
| Parallel chunk checkpoint recovery (panic + resume) | ✅ | ✅ | — | `live_chunked_recovery::parallel_chunked_*` (C3 / C4) |
| OLTP load on source during export | partial | ✅ | ✅ | `live_oltp_load` |
| ~60k-row content extraction under update pressure | — | ✅ | ✅ | `live_content_load` (nightly only — minutes to seed) |
| 1M-row full extraction under load | — | — | ✅ | `pg_full_content_export_max_pressure` (skipped in nightly; 3–5 min runtime) |
| Performance smoke (throughput regression) | partial | ✅ | ✅ | `live_performance_smoke` |

---

## Type system coverage

| Area | PR CI | Nightly | Manual | Notes |
|---|:---:|:---:|:---:|---|
| Per-type golden round-trip (PG + MySQL) | ✅ gate | ✅ | — | `live_type_golden` — runs in dedicated `test-type-golden` job with live DBs |
| Parquet round-trip | ✅ | ✅ | — | `live_parquet_roundtrip`, `format_golden`, `format_fuzz` |
| Format writer (CSV + Parquet, row-group golden) | ✅ gate | ✅ | — | `format_golden`, `test-stability` job |
| Type policy + ExportTarget compat (BigQuery) | ✅ | ✅ | — | covered in `live_cli_flags --type-report` |

---

## Database version coverage

| Engine | Version | PR CI | Manual | Notes |
|---|---|:---:|:---:|---|
| PostgreSQL | 16 | ✅ | ✅ | Primary target |
| PostgreSQL | 12, 13, 14, 15 | — | ✅ | `dev/legacy/run_full_matrix.sh`, opt-in compose profile |
| MySQL | 8.0 | ✅ | ✅ | Primary target |
| MySQL | 5.7 | — | ✅ | `dev/legacy/run_full_matrix.sh` — known view-syntax gap in `init.sql`, see [reference/compatibility.md](reference/compatibility.md#mysql-57--window-functions) |

Each legacy target runs the full 83-assertion e2e suite when selected. Status table in [reference/compatibility.md](reference/compatibility.md).

---

## Operational tooling

| Area | PR CI | Nightly | Manual | Suite |
|---|:---:|:---:|:---:|---|
| CLI flag contract (no silent flag drift) | ✅ | ✅ | — | `cli_contract`, `live_cli_flags` |
| `rivet init` scaffolding | ✅ | ✅ | — | `live_init`, `live_init_extended` |
| `rivet doctor` preflight | ✅ | ✅ | — | covered in `live_cli_flags` |
| Run-summary JSON contract | ✅ | ✅ | — | `run_summary_contract` |
| MCP server contract | ✅ | — | — | `mcp_contract` |
| Quality gates (row-count, null-ratio, uniqueness) | ✅ | ✅ | — | `quality_live`, `test-stability` |
| Resource sampler (RSS) | ✅ | ✅ | — | `resource_smoke` |
| Batch memory policy (`auto_shrink` / `warn` / `fail`) | ✅ | ✅ | — | `batch_memory_policy` |

---

## Supply chain

| Control | PR CI | Notes |
|---|:---:|---|
| RustSec advisory audit | ✅ | `audit` job — fails on any known CVE in declared deps |
| Rustfmt | ✅ gate | `fmt` |
| Clippy (`-D warnings`) | ✅ gate | `clippy` |

Release-artifact signing and checksums are roadmap (see [SECURITY.md § Supply chain](../SECURITY.md#supply-chain)).

---

## What is *not* in CI

These remain operator-driven:

- **Real S3 / GCS production endpoints** with real IAM. CI uses MinIO and fake-gcs containers; the real-cloud path is exercised manually before each release.
- **Cross-platform binaries.** Release builds run on the matrix in [.github/workflows/release.yml](../.github/workflows/release.yml); the **per-PR** `build-release` job only builds for Linux x86_64.
- **Long-horizon soak tests** (24h+ continuous extraction). Not run; planned for future hardware.
- **Real production-shape datasets** beyond the 60k content_items fixture and operator-seeded fixtures from `dev/`.

If your environment depends on any of the above, run the corresponding scripts under `dev/` before adopting Rivet.

---

## Updating this matrix

When you add or remove a coverage tier:

1. Edit the relevant row(s) here.
2. If you add a new semantic gate, also list it in the branch-protection comment at the top of [.github/workflows/ci.yml](../.github/workflows/ci.yml).
3. Note the change in `CHANGELOG.md` under a `### Reliability matrix` sub-section.
