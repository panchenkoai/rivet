# Testing matrix

Rivet's test suite is organised into two tiers, selected by the standard
`#[ignore]` convention.  No test runner beyond `cargo test` is required.

## Tiers

| Tier | Selection | Infrastructure required | What it covers |
|------|-----------|-------------------------|----------------|
| **Offline** | `cargo test` | none | Unit tests, pure-function property/fuzz smoke, state-layer contracts, format round-trip, CLI help snapshots, invariants I1–I7, F1–F5 crash-boundary F-matrix, validation regressions |
| **Live** | `cargo test -- --ignored` | `docker compose up -d` | Full rivet binary against real Postgres/MySQL/SQL Server/MongoDB, MinIO (S3), fake-gcs, Toxiproxy; Parquet round-trip E2E; **type/trust golden** DB → rivet → Parquet → Arrow read-back (postgres + mysql); cross-database parity; destination parity; resume; retry and mid-stream faults; schema drift; performance smoke; crash-point recovery matrix |

Both tiers run in CI (`.github/workflows/ci.yml`):

- Offline suite runs in the `test` / `test-invariants` / `test-recovery` / `test-compatibility` jobs on every push and PR.
- Live suite runs in two dedicated jobs:
  - **`test-type-golden`** — starts only Postgres + MySQL, runs `--test live_type_golden -- --ignored`. Named branch-protection gate for type-contract regressions.
  - **`e2e`** — full stack (Postgres, MySQL, SQL Server, MinIO, fake-gcs, Azurite, Toxiproxy, plus DuckDB/ClickHouse targets and the `cdc` / `replica` / `pool` compose profiles); seeds databases, builds release binary, runs `dev/e2e/run_e2e.sh`, then runs all remaining `--ignored` tests **except the MongoDB suites** (`live_mongo*` / `live_cdc_mongo`), which run in the dedicated nightly `mongo-versions` matrix (4.4 → 8.0, `nightly-live.yml`).

## Offline suite

Covers the full public API and every pure function in the crate.  Runs in
under two seconds on a developer laptop.

```bash
cargo test
# → example: cargo test: ~1360 passed, ~60 ignored (~32 suites, ~3s) — counts drift; check your local footer
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
# → counts vary (~50+ ignored live tests). Check the cargo footer after `cargo test -- --ignored`.
```

| File | Domain | QA backlog task |
|------|--------|-----------------|
| `live_harness_canary.rs` | Reachability probe for every service (Postgres primary + via Toxiproxy, MySQL primary + via Toxiproxy, MinIO, fake-gcs, Toxiproxy admin); harness sanity (`PgTable`/`MysqlTable` RAII guards, `unique_name` no-collision, `CARGO_BIN_EXE_rivet` visibility) | Phase A |
| [`type_roundtrip`](../tests/type_roundtrip/) (`make test-types` / `make test-types-live`) | 0.18.0 type matrix: offline YAML contracts + live PG/MySQL × Parquet/CSV | [`docs/type-mapping.md`](../type-mapping.md) |
| [`live_type_golden.rs`](#trust-milestone-type-golden-round-trip) | Trust & reproducibility: **paired** Postgres *and* MySQL golden pipelines | Trust milestone §1 (“Golden E2E for type safety”); complements `live_parquet_roundtrip.rs` |
| `live_parquet_roundtrip.rs` | Postgres → rivet → Parquet → reader; schema/row-count/nullability/unicode/empty-dataset contracts; `--validate` flag | Task 2.2 |
| `live_cross_db_parity.rs` | Same dataset via Postgres vs MySQL under full and chunked modes; row-count and id-set equivalence | Task 3.3 |
| `live_destination_parity.rs` | Local vs S3 (MinIO) vs GCS (fake-gcs); per-backend file materialisation + parity row-count | Task 6.3 |
| `live_resume.rs` | Full-mode file accumulation across runs; incremental cursor round-trip; `--resume` gate message | Task 1.2 |
| `live_retry_and_faults.rs` | Baseline via Toxiproxy; latency toxic tolerated; proxy disable → clean non-zero exit; mid-stream proxy disable/enable → recovery via retries; permanent-error short-circuit | Task 4.1, 4.2 |
| `live_chaos.rs` | High-latency false-positive guard; chunked export survives mid-stream outage; S3 missing bucket fails cleanly; S3 recovery after transient-outage simulation | Task 4A.4, 6.2 |
| `live_schema_drift.rs` | Added column / removed column / stable schema — detection flag in `export_metrics.schema_changed` | Task 7.1, 7.2 |
| `live_performance_smoke.rs` | 5 000-row + 200B payload finishes within 30 s; split-by-size produces multiple files with no row loss; parallel-4 chunked export materialises every id exactly once | Task 9.1, 9.2 |
| `live_crash_recovery.rs` | Four fault points (`after_source_read`, `after_file_write`, `after_manifest_update`, `after_cursor_commit`) × expected post-crash state × recovery run | Task 1.1 |
| `live_mongo*.rs` | MongoDB batch (JSON-blob `_id` + `document`): distinct-`_id` set vs source, verbatim document round-trip, crash recovery, retry/faults, permission-harm | – |
| `live_mssql_*.rs` | SQL Server batch: chunked (range + keyset), resume, crash recovery, reconcile/repair — twins of the Postgres/MySQL suites | – |
| `live_cdc*.rs` | CDC capture/resume for all four engines (`live_cdc.rs`, `live_cdc_mongo.rs`, `live_cdc_mssql.rs`, plus golden/oracle/property/MBT) — at-least-once, no gap/dup | – |

### Trust milestone: type golden round-trip

[`tests/live_type_golden.rs`](../../tests/live_type_golden.rs) implements the roadmap contract **database → Rivet (`rivet run`) → Parquet → Arrow read-back → exact assertions** so type handling stays provable end-to-end, not only in unit tests (`format_golden.rs` covers writers in isolation).

Each test targets **both engines** where the contract applies:

| Scenario | Postgres | MySQL |
|----------|----------|--------|
| Decimal exact sums + `Decimal128(p,s)` in Parquet | `NUMERIC(18,2/6)`, YAML `columns: decimal(...)` | `DECIMAL(18,2/6)`, same YAML overrides |
| Timestamp semantics (`tz=None` vs `UTC` tag + µs parity) | `TIMESTAMP` / `TIMESTAMPTZ` with offset row | `DATETIME(6)` / `TIMESTAMP(6)` (Rivet sets `SET time_zone = '+00:00'` on the MySQL session) |
| Binary round-trip | `BYTEA` | `BLOB` (avoid reserved identifiers like `blob` as column SQL names) |
| Canonical UUID-ish text (`Utf8`) | native `UUID` | `VARCHAR(36)` with hyphenated lowercase literal |
| INTERVAL → ISO 8601 `Utf8` | `INTERVAL '1 year 2 months 3 days'` → `"P1Y2M3D"`, `INTERVAL '-1 year'` → `"P-1Y"`, `INTERVAL '0'` → `"PT0S"` | — (no MySQL INTERVAL type) |

CI runs these in the dedicated `test-type-golden` job (`cargo test --release --test live_type_golden -- --ignored`) as well as in the full `e2e` job. Local:

```bash
docker compose up -d
cargo test --test live_type_golden -- --ignored
```

**Not yet in this matrix** (future roadmap items): JSON logical metadata parity, unsupported-type strict failures, classified schema-drift variants as dedicated goldens (`live_schema_drift.rs` already covers drift telemetry for Postgres).

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

## Shell regression matrices

Binary-level regression guards under [`dev/matrices/`](../../dev/matrices/README.md)
complement the Rust integration tests above. They drive the release `rivet`
binary through fixture scenarios and diff stdout/stderr/exit codes, file
layouts, EXPLAIN plans, and perf thresholds against committed baselines.

```bash
bash dev/matrices/setup_links.sh   # one-time
dev/matrices/run.sh --tier=pr      # cli + cfg + path (PR CI)
```

See [`dev/matrices/README.md`](../../dev/matrices/README.md) for the full taxonomy
and tier map.

## QA / roadmap alignment

Task IDs in tables above are historical QA labels. **Trust & reproducibility**
(golden DB → Rivet → Parquet → Arrow read-back, Postgres *and* MySQL) lives in
[`tests/live_type_golden.rs`](../../tests/live_type_golden.rs) and is described
above. Strategic tracking: [`rivet_roadmap.md`](../../rivet_roadmap.md) §Phase 1
(Epic 14 / execution status).

## CDC conformance gate

`tests/cdc_conformance_gate.rs` runs in plain `cargo test` and enforces two
hard rules over the live CDC suite's SOURCES:

1. **Every engine × every conformance case** (resume, idle-first-run,
   crash-before-ack, full type matrix, update/delete, initial snapshot,
   vanished anchor, mixed-transaction boundary, schema-qualified routing,
   non-UTC session, …) must have a live test — or an explicit `NA("reason")`
   in the matrix. A new engine cannot merge with a coverage hole; a new case
   must decide for all engines. Motivation: per-engine coverage drifts
   silently, and each engine's missing case is exactly where a real bug lived
   (mixed-transaction existed only for MySQL, qualified-name only for
   PostgreSQL — ultrareview found the two matching bugs).
2. **Every live CDC test that runs a capture must read back an outcome**
   (manifest rows, a batch comparison, a destination listing, the state DB) —
   a bare exit-0 assertion would wave a 0-row silent success through, which
   is how three of the campaign's worst bugs hid.

## Mutation testing (nightly)

`mutants-cdc` in `nightly-live.yml` runs `cargo-mutants` over the CDC value /
sink / parser / checksum / decimal modules against the offline suite. A
surviving mutant is a named test blind spot — the meta-gate that finds holes
in the gates above.

## Config-key composition gate (per PR)

`config-key-composition` in `ci.yml` diffs `schemas/rivet.schema.json` against
the PR base: a NEW config key must be named by at least one test. The
campaign's worst bugs lived at the intersection of two individually-correct
knobs (`initial` × vanished-slot, `initial` × `skip_empty`) — a new knob
enters review with its interaction tests or not at all.

## CDC gremlins (real faults)

`tests/live/gremlin_cdc.rs` (+ the capture-job stall in `live_cdc_mssql.rs`)
injects REAL fault classes — SIGKILL, a TCP cut mid-binlog-stream, a hard
destination outage, a failed checkpoint write, a stalled capture job — and
asserts the at-least-once contract from the outside: the failure is loud, and
after healing the union of all parts holds every source row (overlap fine,
gap never). Panic-hook tests cannot cover these: a panic unwinds and runs
`Drop` guards; none of the above do. Requires toxiproxy + fake-gcs from the
compose stack; each case is a row in the conformance matrix.

### Known-equivalent mutants (decimal canon)

The six surviving mutants in `src/types/decimal.rs` are all `<` → `<=` on
branch guards where BOTH branches compute identical values at the boundary
(e.g. `scale < 0` vs `scale <= 0`: at scale 0 the negative-scale arm divides
by 10⁰ = 1 — the same result as the plain path; `frac.len() < scale` vs `<=`:
at equality the pad loop pads zero characters). They are mathematically
unkillable; do not write pseudo-tests for them. Everything else in the file
is caught (92) or timeouts-as-caught (4).
