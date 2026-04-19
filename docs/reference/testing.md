# Testing matrix

Rivet's test suite is organised into two tiers, selected by the standard
`#[ignore]` convention.  No test runner beyond `cargo test` is required.

## Tiers

| Tier | Selection | Infrastructure required | What it covers |
|------|-----------|-------------------------|----------------|
| **Offline** | `cargo test` | none | Unit tests, pure-function property/fuzz smoke, state-layer contracts, format round-trip, CLI help snapshots, invariants I1–I7, F1–F5 crash-boundary F-matrix, validation regressions |
| **Live** | `cargo test -- --ignored` | `docker compose up -d` | Full rivet binary against real Postgres/MySQL, MinIO (S3), fake-gcs, Toxiproxy; Parquet round-trip E2E; cross-database parity; destination parity; resume; retry and mid-stream faults; schema drift; performance smoke; crash-point recovery matrix |

Both tiers run in CI (`.github/workflows/ci.yml`):

- Offline suite runs in the `test` / `test-invariants` / `test-recovery` / `test-compatibility` jobs on every push and PR.
- Live suite runs in the `e2e` job, which also executes the bash-driven `dev/e2e/run_e2e.sh` matrix against the same docker-compose stack.

## Offline suite

Covers the full public API and every pure function in the crate.  Runs in
under two seconds on a developer laptop.

```bash
cargo test
# → cargo test: 1096 passed, 46 ignored (31 suites, ~2s)
```

Each integration file under `tests/` maps to one domain:

| File | Domain | QA backlog task |
|------|--------|-----------------|
| `invariants.rs` | ADR-0001 state invariants I1–I7 | – |
| `journal_invariants.rs` | Journal event ordering and PlanSnapshot contract | – |
| `recovery.rs` | F1–F5 crash-boundary state expectations | – |
| `state_compat.rs` | Corrupted DB handling + cross-version migration | Task 1.3, 1.4 |
| `schema_evolution.rs` | Schema drift detection algorithm | – |
| `chunked_sparse_ids.rs` | Sparse-ID chunk planner edge cases | – |
| `retry_integration.rs` | `classify_error` classifier table | Task 4.3 |
| `format_golden.rs` | CSV + Parquet writer goldens including extreme values | Task 2.4 |
| `format_fuzz.rs` | Deterministic fuzz-smoke for format serialization | Task 4A.3 |
| `validate_regression.rs` | Validate-output contract (row count, empty, corrupt) | Task 2.1 |
| `config_fuzz.rs` | YAML + placeholder fuzz-smoke | Task 4A.1 |
| `config_secrets.rs` | Error-message secret-redaction contract | Task 5.4 |
| `planner_fuzz.rs` | SQL-shaping and planner fuzz-smoke | Task 4A.2 |
| `cli_contract.rs` | `--help` structure and exit-code contract | Task 5.3 |
| `run_summary_contract.rs` | Structured `RunSummary` and journal contract | Task 8.1 |
| `time_window.rs` | Time-window SQL builder goldens | – |
| `resource_smoke.rs` | RSS sampler, memory threshold module | – |

Inline `#[cfg(test)] mod tests` blocks in `src/` cover pure-function unit
tests (config parsing, chunk math, cursor round-trip, format writers,
destination capabilities, Slack payload formation) and benefit from
`pub(crate)` access.  `cargo test --all-targets` runs them automatically.

## Live suite

Runs the full pipeline against the docker-compose stack.  Every test carries
`#[ignore = "live: ..."]` so the default offline run ignores them; invoking
with `--ignored` activates them.  If any service is unreachable, live tests
fail with an actionable message naming the missing container and port
(`require_alive` helper in `tests/common/mod.rs`).

```bash
docker compose up -d
cargo test -- --ignored
# → cargo test: 46 passed, 1096 filtered out (31 suites, ~14s)
```

| File | Domain | QA backlog task |
|------|--------|-----------------|
| `live_harness_canary.rs` | Reachability probe for every service (Postgres primary + via Toxiproxy, MySQL primary + via Toxiproxy, MinIO, fake-gcs, Toxiproxy admin); harness sanity (`PgTable`/`MysqlTable` RAII guards, `unique_name` no-collision, `CARGO_BIN_EXE_rivet` visibility) | Phase A |
| `live_parquet_roundtrip.rs` | Postgres → rivet → Parquet → reader; schema/row-count/nullability/unicode/empty-dataset contracts; `--validate` flag | Task 2.2 |
| `live_cross_db_parity.rs` | Same dataset via Postgres vs MySQL under full and chunked modes; row-count and id-set equivalence | Task 3.3 |
| `live_destination_parity.rs` | Local vs S3 (MinIO) vs GCS (fake-gcs); per-backend file materialisation + parity row-count | Task 6.3 |
| `live_resume.rs` | Full-mode file accumulation across runs; incremental cursor round-trip; `--resume` gate message | Task 1.2 |
| `live_retry_and_faults.rs` | Baseline via Toxiproxy; latency toxic tolerated; proxy disable → clean non-zero exit; mid-stream proxy disable/enable → recovery via retries; permanent-error short-circuit | Task 4.1, 4.2 |
| `live_chaos.rs` | High-latency false-positive guard; chunked export survives mid-stream outage; S3 missing bucket fails cleanly; S3 recovery after transient-outage simulation | Task 4A.4, 6.2 |
| `live_schema_drift.rs` | Added column / removed column / stable schema — detection flag in `export_metrics.schema_changed` | Task 7.1, 7.2 |
| `live_performance_smoke.rs` | 5 000-row + 200B payload finishes within 30 s; split-by-size produces multiple files with no row loss; parallel-4 chunked export materialises every id exactly once | Task 9.1, 9.2 |
| `live_crash_recovery.rs` | Four fault points (`after_source_read`, `after_file_write`, `after_manifest_update`, `after_cursor_commit`) × expected post-crash state × recovery run | Task 1.1 |

## Test-only fault injection

A small env-var-driven hook in `src/test_hook.rs` lets the crash-matrix tests
panic at precise pipeline boundaries without any cargo feature flag:

```bash
RIVET_TEST_PANIC_AT=after_file_write rivet run --config ... --export ...
# → process panics between dest.write() and record_file()
```

Valid point names are listed in `src/pipeline/single.rs` inline comments;
see also `dev/CRASH_MATRIX.md` and [ADR-0001](../adr/0001-state-update-invariants.md).
Cost when the env var is unset: one relaxed atomic load per call, roughly
a nanosecond.

## Live-test harness (`tests/common/mod.rs`)

| Helper | Purpose |
|--------|---------|
| `require_alive(service)` | Fast reachability probe; clear message if the service is down |
| `unique_name(prefix)` | PID + atomic counter → race-free table / export / prefix names for parallel test-threads |
| `pg_connect` / `seed_pg_numeric_table` / `PgTable` | Postgres client + seeded table + RAII `DROP TABLE` on scope exit |
| `mysql_connect` / `seed_mysql_numeric_table` / `MysqlTable` | MySQL analogue |
| `write_config` / `run_rivet` / `run_rivet_export` | Spawn the freshly-built `rivet` binary (`CARGO_BIN_EXE_rivet`) with a temp YAML config |
| `ensure_toxi_proxy` / `toxi_add_latency` / `toxi_disable` / `toxi_enable` / `toxi_reset_toxics` | Minimal Toxiproxy admin client over raw `TcpStream` (no `reqwest` blocking runtime) |
| `toxiproxy_guard` | Cross-process `flock(2)` lock on `$TMPDIR/rivet_qa_toxiproxy.lock` — serialises Toxiproxy mutations across cargo's parallel integration test binaries |
| `ensure_minio_bucket` / `ensure_gcs_bucket` | Idempotent bucket creation via `docker compose exec minio mc` and fake-gcs HTTP API |
| `files_with_extension` | Enumerate files produced by rivet under a test's tempdir |

## Running from scratch

```bash
# Offline (default — used by PR-gate jobs):
cargo test

# Live (requires docker compose):
docker compose up -d
cargo test -- --ignored
docker compose down
```

Both command lines are what the corresponding CI jobs execute.  If your
`cargo test` diverges from the CI matrix, something is out of sync —
check `.github/workflows/ci.yml` for the exact invocation.

## QA backlog mapping

The full design of the test matrix lives in `rivet_qa_backlog_v2.md` at the
repository root.  Each task in that document is annotated with the test file
and (for tasks that found real bugs) the referenced code fix.  The coverage
status footer at the bottom of the backlog lists what is shipped, what is
explicit CI strategy, and what is still open.
