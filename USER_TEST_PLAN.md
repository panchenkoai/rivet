# Rivet — User acceptance test plan

This document is a **manual checklist** for operators and pilot users who want to exercise Rivet end-to-end against the features that already exist. Use it for smoke tests, regression passes before a release, or onboarding.

**Language:** English (same convention as [CONTRIBUTING.md](CONTRIBUTING.md)).

---

## How to use this document

1. Work top to bottom for a full pass, or pick a **suite** (e.g. only CLI + Postgres).
2. Mark each row **Pass / Fail / Skip** and note the Rivet version or git commit.
3. For failures, capture: command, stderr, config path, and DB/storage state.

---

## Conventions

### Rivet command

If `rivet` is not on your `PATH`, prefix every command with:

```bash
RIVET='cargo run --release --bin rivet --'
```

Examples:

```bash
$RIVET check --config dev/pg_full.yaml
$RIVET run --config dev/pg_full.yaml --export pg_users_csv --validate
```

If you installed with `cargo install --path .`, use `rivet` instead of `$RIVET`.

### Working directory

Run commands from the **repository root** unless noted otherwise.

### Optional suites

Sections marked **(optional)** need extra services (MinIO, fake GCS, real GCS credentials) or longer runs. Skip them if you only want a quick smoke test.

---

## Preconditions

| # | Check | Pass |
|---|--------|------|
| P1 | Rust toolchain installed (`cargo --version`) | ☐ |
| P2 | `docker compose up -d` brings up Postgres + MySQL (see [README](README.md#development)) | ☐ |
| P3 | Databases seeded (e.g. `cargo run --release --bin seed -- --target both --users 100000`) or at least minimal data from `dev/postgres/init.sql` / `dev/mysql/init.sql` | ☐ |
| P4 | `dev/output/` exists or is creatable (Rivet writes exports there for many samples) | ☐ |

---

## Suite A — CLI and ergonomics

| ID | Scenario | Steps | Expected | Pass |
|----|-----------|-------|----------|------|
| A1 | Help | Run `rivet --help` or `$RIVET --help` | Lists subcommands: `run`, `check`, `doctor`, `state`, `metrics`, `completions` | ☐ |
| A2 | Completions | `$RIVET completions zsh \| head` | Prints `#compdef rivet` (or equivalent shell header) | ☐ |
| A3 | Invalid config | Point `check` at a non-existent file | Clear error, non-zero exit | ☐ |

---

## Suite B — Diagnostics (`check`, `doctor`)

| ID | Scenario | Steps | Expected | Pass |
|----|-----------|-------|----------|------|
| B1 | Preflight all exports (Postgres) | `$RIVET check --config dev/pg_full.yaml` | Completes; per-export strategy and health verdict printed | ☐ |
| B2 | Preflight one export | `$RIVET check --config dev/pg_full.yaml --export pg_users_csv` | Only that export is evaluated | ☐ |
| B3 | Preflight (MySQL) | `$RIVET check --config dev/mysql_full.yaml` | Completes without panic | ☐ |
| B4 | Doctor (auth) | `$RIVET doctor --config dev/pg_full.yaml` | Source connectivity OK; local destination OK (or clear failure if misconfigured) | ☐ |

---

## Suite C — Run: full and incremental (local disk)

| ID | Scenario | Steps | Expected | Pass |
|----|-----------|-------|----------|------|
| C1 | Full CSV + Parquet (Postgres) | `$RIVET run --config dev/pg_full.yaml` | New files under `dev/output/`; run summaries for each export | ☐ |
| C2 | Validate row counts | `$RIVET run --config dev/pg_full.yaml --export pg_users_parquet --validate` | `validated: pass` in summary; log says validation passed | ☐ |
| C3 | Incremental first run | `$RIVET run --config dev/pg_incremental.yaml --export pg_orders_incremental` | Parquet written; cursor updated (see Suite D) | ☐ |
| C4 | Incremental second run | Repeat C3 without changing data | May write 0-row file or skip depending on `skip_empty`; state reflects last cursor | ☐ |
| C5 | MySQL full | `$RIVET run --config dev/mysql_full.yaml` | Files under `dev/output/` | ☐ |
| C6 | MySQL incremental | `$RIVET run --config dev/mysql_incremental.yaml` | Completes; state file updated | ☐ |

---

## Suite D — State and metrics

| ID | Scenario | Steps | Expected | Pass |
|----|-----------|-------|----------|------|
| D1 | Show state | `$RIVET state show --config dev/pg_incremental.yaml` | Table lists exports with last cursor where applicable | ☐ |
| D2 | Metrics history | `$RIVET metrics --config dev/pg_incremental.yaml --last 5` | Recent runs with status, rows, duration | ☐ |
| D3 | Reset state (careful) | `$RIVET state reset --config dev/pg_incremental.yaml --export pg_orders_incremental` | Confirmation message; next incremental run behaves like first run | ☐ |

---

## Suite E — Chunked mode

| ID | Scenario | Steps | Expected | Pass |
|----|-----------|-------|----------|------|
| E1 | Chunked sequential | `$RIVET run --config dev/bench_chunked_seq.yaml` (or smaller `chunk_size` if too slow) | Multiple chunk files or one run completing; logs show chunk progress | ☐ |
| E2 | Chunked parallel | `$RIVET run --config dev/bench_chunked_p4.yaml` **(optional, heavy)** | Parallel chunk logs; all chunks succeed | ☐ |

---

## Suite F — Compression, skip empty, meta columns

| ID | Scenario | Steps | Expected | Pass |
|----|-----------|-------|----------|------|
| F1 | Default zstd Parquet | `$RIVET run --config dev/test_meta_columns.yaml --validate` | File readable; tool like `pyarrow` or `parquet-tools` shows column compression ZSTD | ☐ |
| F2 | Explicit codecs | `$RIVET run --config dev/test_compression.yaml` | `users_snappy` / `users_none` differ in size; summary shows `compression:` when not default | ☐ |
| F3 | Skip empty | `$RIVET run --config dev/test_compression.yaml --export users_skip_empty` | `status: skipped`, `files: 0`, no new parquet for that export | ☐ |
| F4 | Meta columns present | Inspect latest `users_meta_test_*.parquet` from F1 | Schema includes `_rivet_exported_at`, `_rivet_row_hash` when enabled in YAML | ☐ |

---

## Suite G — Structured source URL (A4-style)

| ID | Scenario | Steps | Expected | Pass |
|----|-----------|-------|----------|------|
| G1 | Postgres structured | `$RIVET check --config dev/pg_structured.yaml` | Parses host/user/database; connects | ☐ |
| G2 | MySQL structured | `$RIVET check --config dev/mysql_structured.yaml` | Same | ☐ |

---

## Suite H — Preflight edge cases (optional)

| ID | Scenario | Steps | Expected | Pass |
|----|-----------|-------|----------|------|
| H1 | Degraded scenario | `$RIVET check --config dev/pg_degraded.yaml` | Verdict/suggestions reflect planner output (not necessarily “green”) | ☐ |
| H2 | Wrong password | `$RIVET doctor --config dev/test_pg_wrongpass.yaml` | Fails with auth/connectivity message, not a panic | ☐ |

---

## Suite I — Object storage (optional)

| ID | Scenario | Steps | Expected | Pass |
|----|-----------|-------|----------|------|
| I1 | MinIO / S3 | `docker compose up -d minio`, follow [README](README.md) + `dev/rivet_s3_minio_test.yaml` / `dev/run_s3_export.sh` | Objects appear in bucket | ☐ |
| I2 | fake-gcs-server | `docker compose up -d fake-gcs`, `dev/rivet_gcs_fake_test.yaml` / `dev/run_gcs_fake_export.sh` | Upload succeeds with `allow_anonymous` or emulator config | ☐ |
| I3 | Real GCS | `dev/rivet_gcs_rivet_data_test.yaml` + valid `credentials_file` | Files in bucket under `prefix` | ☐ |

---

## Suite J — Time window mode

There is no dedicated sample under `dev/` yet. Use a **minimal config** (adjust table/column names to your seeded DB):

```yaml
exports:
  - name: events_window
    query: "SELECT id, user_id, event_type, created_at FROM events"
    mode: time_window
    time_column: created_at
    time_column_type: timestamp
    days_window: 7
    format: parquet
    destination:
      type: local
      path: ./dev/output
```

| ID | Scenario | Steps | Expected | Pass |
|----|-----------|-------|----------|------|
| J1 | Time window run | Add `source:` matching your DB, save as `dev/_uat_time_window.yaml`, run `$RIVET run --config dev/_uat_time_window.yaml` | Completes; file contains only rows in window | ☐ |

---

## Suite K — Schema evolution (optional script)

| ID | Scenario | Steps | Expected | Pass |
|----|-----------|-------|----------|------|
| K1 | Schema change detection | Run `dev/test_schema_evolution.sh` (read script first) | Second run logs schema change warnings if script mutates columns | ☐ |

---

## Sign-off

| Field | Value |
|-------|--------|
| Tester | |
| Date | |
| Rivet version / commit | |
| Postgres image | |
| MySQL image | |
| Notes (failed IDs, env quirks) | |

---

## Reference — Config files used above

| File | Purpose |
|------|---------|
| `dev/pg_full.yaml` | Postgres full exports, CSV + Parquet |
| `dev/pg_incremental.yaml` | Postgres incremental |
| `dev/mysql_full.yaml` | MySQL full |
| `dev/mysql_incremental.yaml` | MySQL incremental |
| `dev/pg_structured.yaml` / `dev/mysql_structured.yaml` | Structured DB credentials |
| `dev/bench_chunked_seq.yaml` | Chunked sequential (large table) |
| `dev/bench_chunked_p4.yaml` | Chunked parallel (heavy) |
| `dev/test_meta_columns.yaml` | Meta columns + default zstd |
| `dev/test_compression.yaml` | Snappy / none / skip_empty |
| `dev/rivet_s3_minio_test.yaml` | S3-compatible (MinIO) |
| `dev/rivet_gcs_fake_test.yaml` | GCS emulator |
| `dev/rivet_gcs_rivet_data_test.yaml` | Real GCS (credentials required) |

For automated regression, run `cargo test` (see [PRODUCT.md](PRODUCT.md) test coverage).
