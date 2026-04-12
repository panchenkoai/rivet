# Changelog

## 0.2.0-beta.6 (2026-04-11)

### Fixes

- **Docker image:** install `build-essential` in the builder stage so `tikv-jemalloc-sys` can run `make` (slim images previously failed with `ENOENT` during `cargo build --release --locked`).
- **`.dockerignore`** — ignore `target/`, `.git/`, `.github/` so `docker build` / `buildx` context stays small.

### Documentation

- **`packaging/homebrew/README.md`** — troubleshooting for tap push `Authentication failed` / invalid PAT.

---

## 0.2.0-beta.4 (2026-04-12)

### New features

**`rivet init` — YAML scaffolding from a live database**

- **`--table`** — introspect one table (`schema.table` supported on PostgreSQL) and emit a single export with suggested `mode` (`full` / `incremental` / `chunked`) from metadata and row estimates.
- Omit **`--table`** — emit **one YAML** with an export per **base table and view** in a PostgreSQL schema (default `--schema public`) or in a MySQL database (from the URL path or `--schema` when the URL has no database).
- Output uses **`url_env: DATABASE_URL`** by default so passwords are not written into the file.

**Chunked export progress bar**

- Terminal progress (chunks completed, cumulative rows, ETA) during **`mode: chunked`** runs when stderr is a TTY (`indicatif`).

### Documentation

- [docs/reference/init.md](docs/reference/init.md) — full `rivet init` guide (Docker Compose, `dev/regenerate_docker_init_configs.sh`)
- [docs/reference/cli.md](docs/reference/cli.md) — `rivet init` command table
- [docs/getting-started.md](docs/getting-started.md) — optional “Scaffold with rivet init” step
- [README.md](README.md) — CLI quick reference; product vs `rivet-cli` crate name; chunked progress + `dev/bench_chunked_p4_safe.yaml` example
- [USER_GUIDE.md](USER_GUIDE.md) — `rivet init` optional section
- [docs/modes/chunked.md](docs/modes/chunked.md) — progress bar (independent of `RUST_LOG`), bench config examples
- [docs/README.md](docs/README.md) — `rivet init` and chunked progress pointers
- E2E: `dev/e2e/run_e2e.sh` section 16 — `rivet init` against docker-compose Postgres/MySQL

### Fixes

**PostgreSQL `query_scalar` and date-based chunking**

`MIN`/`MAX` on `timestamp` / `timestamptz` / `date` columns were not decoded in `PostgresSource::query_scalar` (only numeric and plain text types were). That returned a false empty result and broke **`chunk_by_days`** (and any path using time-column scalars). Chrono types are now formatted to strings that `parse_date_flexible` accepts.

### Other

- `Cargo.toml` `[package] exclude` moved from invalid `[[bin]]` key to `[package]`
- `dev/regenerate_docker_init_configs.sh` and sample `dev/init_generated/` YAMLs from docker-compose schemas
- `.gitignore` — `dev/e2e/.init_e2e_scratch/`

---

## 0.2.0-beta.3 (2026-04-12)

### New features

**Connection limit warning in `rivet check`**

`rivet check` now warns when `parallel` meets or exceeds the database's `max_connections` limit. The warning includes exact numbers and a safe recommendation (headroom of 3 reserved connections for monitoring and admin traffic).

```
WARNING: parallel=20 meets or exceeds DB max_connections=20 —
workers will compete for connections and some may fail.
Reduce parallel to at most 17.
```

If `max_connections` cannot be fetched (restricted user), rivet shows an informative "check skipped" message rather than silently passing.

Works for PostgreSQL (`current_setting('max_connections')`) and MySQL (`@@max_connections`).

**Date-native chunking (`chunk_by_days`)**

New `chunk_by_days` field on chunked exports. Partitions a table into calendar windows on a DATE or TIMESTAMP column — no unix-epoch arithmetic, correct open-end semantics for timestamps.

```yaml
exports:
  - name: orders_by_year
    query: "SELECT id, user_id, product, price, ordered_at FROM orders"
    mode: chunked
    chunk_column: ordered_at
    chunk_by_days: 365
    parallel: 4
    format: parquet
    destination:
      type: local
      path: ./output
```

Generated SQL per chunk:
```sql
WHERE ordered_at >= '2023-01-01' AND ordered_at < '2024-01-01'
```

- Works with `parallel: N` for concurrent date-window workers
- Compatible with `chunk_checkpoint` / `--resume`
- `rivet check` reports strategy as `date-chunked(ordered_at, 365d)`
- `chunk_dense: true` cannot be combined with `chunk_by_days` (rejected at config validation)
- Sparse-range check is skipped for date mode (calendar gaps ≠ numeric ID sparsity)

### Documentation

- `docs/modes/chunked.md` — new "Date-based chunking" section with SQL example and when-to-use guidance
- `docs/reference/config.md` — `chunk_by_days` field documented
- `USER_GUIDE.md` — date chunking section added
- `examples/pg_date_chunked_local.yaml` — two export examples (orders by year, events by month with parallel + checkpoint)

### Tests

- 11 new unit tests: `check_connection_limit` (7) + `parse_date_flexible` and date chunk query building (4+)
- 4 config validation tests for `chunk_by_days` edge cases
- E2E Section 14: connection limit warnings (PG + MySQL, safe and exceeded cases)
- E2E Section 15: date-chunked run, preflight strategy, `chunk_by_days + chunk_dense` rejection (PG + MySQL)

---

## 0.2.0-beta.2 (2026-03-28)

### New features

- **Homebrew tap** — `brew install panchenkoai/rivet/rivet-cli`
- **Docker image** — `ghcr.io/panchenkoai/rivet`
- **`cargo install rivet-cli`** — published to crates.io (crate renamed from `rivet` which was already taken)

### Fixes

- Homebrew tap workflow: moved `HOMEBREW_TAP_GITHUB_TOKEN` to job-level env to pass workflow validation
- `fastrand` updated to 2.4.1 (2.4.0 was yanked)

### Documentation

- README: added `cargo install` section

---

## 0.2.0-beta.1 (2026-04-05)

### Architecture

- **Split `pipeline.rs` (2447 lines) into `pipeline/` module** with 7 focused submodules:
  `chunked`, `cli`, `mod` (orchestration), `retry`, `single`, `sink`, `validate`.
- **Split `config.rs` (1708 lines) into `config/` module** with 4 submodules:
  `models`, `resolve`, `tests`, `mod` (validation & loading).
- **Split `preflight.rs` (1425 lines) into `preflight/` module** with 5 submodules:
  `analysis`, `doctor`, `mod` (orchestration), `mysql`, `postgres`.
- All public API paths unchanged — external callers unaffected.

### Reliability

- **Export failures now propagate to CLI exit code**: `run_export_job` returns
  `Result<()>` and failures are collected; `rivet run` exits with non-zero when
  any export fails (critical for CI, cron, and orchestrators).
- **SQLite migration errors are fatal**: `migrate()` returns `Result` and
  `StateStore::open()` fails if any migration step errors, preventing silent
  partial schema states.
- **Typed error classification**: `classify_error` now checks Postgres `SqlState`
  codes and MySQL numeric error codes before falling back to string matching,
  giving more precise transient-vs-permanent classification for retries.
- **Replaced production `unwrap()` calls** with `expect()` and descriptive messages
  across `pipeline/`, `config/`, `state.rs`, `format/csv.rs`, and `source/`.
- **Versioned SQLite schema migrations**: `schema_version` table tracks applied
  migrations; new databases start at v3, legacy databases are detected and upgraded
  automatically.  Future schema changes only require adding a new migration entry.
- **Graceful mutex poison handling**: parallel chunked workers use
  `unwrap_or_else(|e| e.into_inner())` instead of `expect()`, preventing
  cascading panics if a worker thread panics.

### Code quality

- **Zero clippy warnings**: resolved all `collapsible_if`, `too_many_arguments`,
  `derivable_impl`, `manual_clamp`, `if_let_some_result`, `manual_range_contains`,
  `write_literal`, `is_multiple_of`, unused-import, and needless-borrow lints.
- Added `// SAFETY:` documentation on the sole `unsafe` block (`resource.rs` macOS RSS).
- Added crate-level `//!` documentation to `lib.rs`.
- **Tuning `profile_name()` now returns the configured profile** rather than
  inferring from numeric fields, ensuring metrics and logs match the YAML config.

### Memory optimization

- **Streaming cloud uploads**: S3, GCS, and stdout destinations now use
  `std::io::copy` instead of loading entire temporary files into RAM. Memory
  footprint during upload is O(buffer) instead of O(file_size).
- **Early `drop(rows)` in Postgres source**: raw `Vec<Row>` is freed
  immediately after conversion to Arrow `RecordBatch`, reducing transient
  memory overlap.
- **jemalloc** (`tikv-jemallocator`) added as an optional default-on allocator.
  jemalloc aggressively returns freed pages to the OS, reducing peak RSS by
  ~30–40% at smaller batch sizes compared to the system allocator.

### Config validation

- **Misplaced tuning field detection**: if `batch_size`, `profile`,
  `throttle_ms`, or other tuning fields are placed directly under `source:`
  or in an `exports[]` entry instead of inside `tuning:`, Rivet now rejects
  the config with a clear error and a fix suggestion. Previously, these
  fields were silently ignored by serde, causing unexpected defaults.

### Testing

- **617 tests** (537 unit + 80 integration), up from 274.
- New test coverage for: cursor extraction (all Arrow types), strip internal
  column, quality tracking, validate_output (corrupt/empty/missing files),
  CSV golden tests (Binary, Float32, Int16, Boolean+nulls, multi-batch),
  Parquet nullable + multi-batch roundtrip, resolve_vars edge cases,
  parse_file_size regressions, notify trigger matching, quality multi-batch
  aggregation, parse_params, format_bytes boundary values.

### Dependencies

- **Replaced deprecated `serde_yaml`** with `serde_yml` 0.0.12.
- Updated 52 transitive dependencies (tokio 1.51, hyper 1.9, postgres 0.19.13,
  libc 0.2.184, and others).

### Documentation

- **USER_GUIDE.md**: added jemalloc section, memory optimization tips, streaming
  upload notes, misplaced tuning field detection, troubleshooting section,
  documented `--export` and `--last` flags for `metrics` and `state files`.
- **README.md**: added stdout destination to config reference.

### Packaging

- Added `license = "MIT"`, `repository`, `rust-version = "1.94"` to `Cargo.toml`.
- Added `LICENSE` (MIT) file.
- Added `rust-toolchain.toml` pinning toolchain to 1.94 with rustfmt + clippy.
- Added `exclude` list to `Cargo.toml` for clean `cargo publish` (excludes `dev/`,
  `tests/`, `.github/`, `USER_TEST_PLAN.md`).

### CI

- `.github/workflows/ci.yml` with five jobs: `rustfmt`, `clippy -D warnings`,
  `cargo test`, `cargo build --release`, and **`cargo audit`** (security).
- All jobs pinned to Rust **1.94** (matches `rust-version` and `rust-toolchain.toml`).

## 0.1.0

Initial release.
