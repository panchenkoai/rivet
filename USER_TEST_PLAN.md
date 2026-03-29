# koRivet — User acceptance test plan

This document is a **manual checklist** for operators and pilot users who want to exercise Rivet end-to-end against the features that already exist. Use it for smoke tests, regression passes before a release, or onboarding.

To mark a test: change `[ ]` to `[x]` in the Pass column.

---

## Conventions

Install `rivet` to your PATH: `cargo install --path .`

Run all commands from the **repository root**.

Sections marked **(optional)** need extra services or longer runs.

---

## Preconditions


| ID  | Check                                                                               | Pass |
| --- | ----------------------------------------------------------------------------------- | ---- |
| P1  | Rust toolchain installed (`cargo --version`)                                        | [x]  |
| P2  | `docker compose up -d` brings up Postgres + MySQL                                   | [x]  |
| P3  | Databases seeded (`cargo run --release --bin seed -- --target both --users 100000`) | [x]  |
| P4  | `dev/output/` directory exists                                                      | [x]  |


---

## Suite A — CLI and ergonomics


| ID  | Scenario       | Command / Steps                         | Expected                                                           | Pass                                  |
| --- | -------------- | --------------------------------------- | ------------------------------------------------------------------ | ------------------------------------- |
| A1  | Help           | `rivet --help`                          | Lists subcommands: run, check, doctor, state, metrics, completions | [x]                                   |
| A2  | Completions    | `rivet completions zsh                  | head`                                                              | Prints `#compdef rivet` or equivalent |
| A3  | Invalid config | `rivet check --config nonexistent.yaml` | Clear error, non-zero exit                                         | [x]                                   |


---

## Suite B — Diagnostics (`check`, `doctor`)


| ID  | Scenario             | Command / Steps                                               | Expected                               | Pass |
| --- | -------------------- | ------------------------------------------------------------- | -------------------------------------- | ---- |
| B1  | Preflight all (PG)   | `rivet check --config dev/pg_full.yaml`                       | Per-export strategy and health verdict | [x]  |
| B2  | Preflight one export | `rivet check --config dev/pg_full.yaml --export pg_users_csv` | Only that export evaluated             | [x]  |
| B3  | Preflight (MySQL)    | `rivet check --config dev/mysql_full.yaml`                    | Completes without panic                | [x]  |
| B4  | Doctor (auth)        | `rivet doctor --config dev/pg_full.yaml`                      | Source OK; local destination OK        | [x]  |


---

## Suite C — Run: full and incremental (local disk)


| ID  | Scenario              | Command / Steps                                                             | Expected                                      | Pass |
| --- | --------------------- | --------------------------------------------------------------------------- | --------------------------------------------- | ---- |
| C1  | Full CSV+Parquet (PG) | `rivet run --config dev/pg_full.yaml`                                       | Files in `dev/output/`; run summaries printed | [x]  |
| C2  | Validate row counts   | `rivet run --config dev/pg_full.yaml --export pg_users_parquet --validate`  | `validated: pass` in summary                  | [x]  |
| C3  | Incremental 1st run   | `rivet run --config dev/pg_incremental.yaml --export pg_orders_incremental` | Parquet written; cursor updated               | [x]  |
| C4  | Incremental 2nd run   | Repeat C3 without changing data                                             | 0 rows or skip; state unchanged               | [x]  |
| C5  | MySQL full            | `rivet run --config dev/mysql_full.yaml`                                    | Files in `dev/output/`                        | [x]  |
| C6  | MySQL incremental     | `rivet run --config dev/mysql_incremental.yaml`                             | Completes; state updated                      | [x]  |


---

## Suite D — State and metrics


| ID  | Scenario        | Command / Steps                                                                     | Expected                                   | Pass |
| --- | --------------- | ----------------------------------------------------------------------------------- | ------------------------------------------ | ---- |
| D1  | Show state      | `rivet state show --config dev/pg_incremental.yaml`                                 | Table with exports and last cursor         | [x]  |
| D2  | Metrics history | `rivet metrics --config dev/pg_incremental.yaml --last 5`                           | Recent runs with status, rows, duration    | [x]  |
| D3  | Reset state     | `rivet state reset --config dev/pg_incremental.yaml --export pg_orders_incremental` | Confirmation; next run acts like first run | [x]  |


---

## Suite E — Chunked mode


| ID  | Scenario                        | Command / Steps                                 | Expected                                | Pass |
| --- | ------------------------------- | ----------------------------------------------- | --------------------------------------- | ---- |
| E1  | Chunked sequential              | `rivet run --config dev/bench_chunked_seq.yaml` | Chunk files created; logs show progress | [x]  |
| E2  | Chunked parallel **(optional)** | `rivet run --config dev/bench_chunked_p4.yaml`  | Parallel logs; all chunks succeed       | [x]  |


---

## Suite F — Compression, skip empty, meta columns


| ID  | Scenario        | Command / Steps                                                          | Expected                                                       | Pass |
| --- | --------------- | ------------------------------------------------------------------------ | -------------------------------------------------------------- | ---- |
| F1  | Default zstd    | `rivet run --config dev/test_meta_columns.yaml --validate`               | Parquet compression = ZSTD                                     | [x]  |
| F2  | Explicit codecs | `rivet run --config dev/test_compression.yaml`                           | snappy/none files differ in size; summary shows `compression:` | [x]  |
| F3  | Skip empty      | `rivet run --config dev/test_compression.yaml --export users_skip_empty` | `status: skipped`, `files: 0`                                  | [x]  |
| F4  | Meta columns    | Inspect `users_meta_test_*.parquet` from F1                              | Has `_rivet_exported_at` and `_rivet_row_hash`                 | [x]  |


---

## Suite G — Structured source URL


| ID  | Scenario         | Command / Steps                                                | Expected                            | Pass |
| --- | ---------------- | -------------------------------------------------------------- | ----------------------------------- | ---- |
| G1  | PG structured    | `PGPASSWORD=rivet rivet check --config dev/pg_structured.yaml` | Parses host/user/database; connects | [x]  |
| G2  | MySQL structured | `rivet check --config dev/mysql_structured.yaml`               | Same                                | [x]  |


---

## Suite H — Preflight edge cases (optional)


| ID  | Scenario          | Command / Steps                                    | Expected                        | Pass |
| --- | ----------------- | -------------------------------------------------- | ------------------------------- | ---- |
| H1  | Degraded scenario | `rivet check --config dev/pg_degraded.yaml`        | Verdict and suggestions printed | [x]  |
| H2  | Wrong password    | `rivet doctor --config dev/test_pg_wrongpass.yaml` | Auth failure message, not panic | [x]  |


---

## Suite I — Object storage (optional)


| ID  | Scenario   | Command / Steps                                                   | Expected          | Pass |
| --- | ---------- | ----------------------------------------------------------------- | ----------------- | ---- |
| I1  | MinIO / S3 | `docker compose up -d minio` then `dev/run_s3_export.sh`          | Objects in bucket | [x]  |
| I2  | fake-gcs   | `docker compose up -d fake-gcs` then `dev/run_gcs_fake_export.sh` | Upload succeeds   | [x]  |
| I3  | Real GCS   | `rivet run --config dev/rivet_gcs_rivet_data_test.yaml`           | Files in bucket   | [x]  |


---

## Suite J — Time window mode

Save this config as `dev/_uat_time_window.yaml`:

```yaml
source:
  type: postgres
  url: "postgresql://rivet:rivet@localhost:5432/rivet"

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


| ID  | Scenario        | Command / Steps                                | Expected                                 | Pass |
| --- | --------------- | ---------------------------------------------- | ---------------------------------------- | ---- |
| J1  | Time window run | `rivet run --config dev/_uat_time_window.yaml` | Completes; only rows within 7-day window | [x]  |


---

## Suite K — Schema evolution (optional)


| ID  | Scenario      | Command / Steps                                 | Expected                               | Pass |
| --- | ------------- | ----------------------------------------------- | -------------------------------------- | ---- |
| K1  | Schema change | Run `dev/test_schema_evolution.sh` (read first) | Second run logs schema change warnings | [x]  |


---

## Suite L — Stdout destination (v4.1)


| ID  | Scenario      | Command / Steps                                                     | Expected                          | Pass |
| --- | ------------- | ------------------------------------------------------------------- | --------------------------------- | ---- |
| L1  | CSV to stdout | `rivet run --config dev/test_stdout.yaml \| head`                   | CSV rows printed to terminal (header + 5 rows) | [x] |
| L2  | Pipe to file  | `rivet run --config dev/test_stdout.yaml > /tmp/rivet_stdout.csv`   | File created; logs on stderr only | [x]  |


---

## Suite M — Parameterized queries (v4.1)


| ID  | Scenario           | Command / Steps                                                | Expected                                 | Pass |
| --- | ------------------ | -------------------------------------------------------------- | ---------------------------------------- | ---- |
| M1  | Param substitution | `rivet run --config dev/test_params.yaml --param MAX_ID=10`    | CSV with only ids 1-10                   | [x]  |
| M2  | Multiple params    | `rivet run --config dev/test_params.yaml --param MAX_ID=5`     | CSV with only ids 1-5                    | [x]  |
| M3  | Param in check     | `rivet check --config dev/test_params.yaml --param MAX_ID=100` | Preflight completes (no unresolved `${}` | [x]  |


---

## Suite N — Data quality checks (v4.1)


| ID  | Scenario           | Command / Steps                                                                   | Expected                                          | Pass |
| --- | ------------------ | --------------------------------------------------------------------------------- | ------------------------------------------------- | ---- |
| N1  | Quality pass       | `rivet run --config dev/test_quality.yaml --export users_quality_pass --validate` | `quality: pass` in summary, file produced         | [x]  |
| N2  | Quality fail (max) | `rivet run --config dev/test_quality.yaml --export users_quality_fail_max`        | `quality: FAIL`, export aborted, no file uploaded | [x]  |


---

## Suite O — Memory-based batch sizing (v4.1)


| ID  | Scenario            | Command / Steps                                               | Expected                                                                        | Pass |
| --- | ------------------- | ------------------------------------------------------------- | ------------------------------------------------------------------------------- | ---- |
| O1  | Memory batch sizing | `RUST_LOG=info rivet run --config dev/test_memory_batch.yaml` | Log shows `batch_size_memory_mb=1: estimated row ~NNB, computed batch_size=NNN` | [x]  |


---

## Suite P — File size splitting (v4.1)


| ID  | Scenario         | Command / Steps                                          | Expected                                                                  | Pass |
| --- | ---------------- | -------------------------------------------------------- | ------------------------------------------------------------------------- | ---- |
| P1  | Split files      | `rivet run --config dev/test_file_split.yaml --validate` | Multiple `users_split_*_part0.parquet`, `_part1.parquet`, etc. in output  | [x]  |
| P2  | Check file count | `ls dev/output/users_split_*_part* \| wc -l`             | More than 1 file                                                          | [x]  |


---

## Suite Q — Retry resilience with Toxiproxy (optional)

**What is Toxiproxy?** A TCP proxy that sits between Rivet and the database. You can inject faults (latency, connection drops) to verify that Rivet's retry logic works correctly.

```
  Rivet ──► localhost:15432 (Toxiproxy) ──► postgres:5432 (real DB)
```

### Setup (one time)

```bash
# 1. Start Postgres and Toxiproxy
docker compose up -d postgres toxiproxy

# 2. Wait for Toxiproxy API to be ready (~5 sec)
sleep 5

# 3. Create proxy endpoints (Postgres on 15432, MySQL on 13306)
bash dev/setup_toxiproxy.sh

# 4. Make sure the DB is seeded
cargo run --release --bin seed -- --target pg --users 10000
```

Verify the proxy API is up: `curl -s http://localhost:8474/proxies | head` should list `pg` and `mysql`.

### Tests


| ID  | Scenario                                   | Steps                                                                                                                                                                            | Expected                                                                                                                                  | Pass |
| --- | ------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- | ---- |
| Q1  | Baseline: export through proxy (no faults) | `rivet run --config dev/test_toxiproxy_pg.yaml --validate`                                                                                                                       | Completes, `status: success`, same result as direct connection                                                                            | [x]  |
| Q2  | Latency: add 3 s delay to every packet     | **Inject:** `curl -s -X POST http://localhost:8474/proxies/pg/toxics -H 'Content-Type: application/json' -d '{"name":"latency","type":"latency","attributes":{"latency":3000}}'` | Returns `{"name":"latency",...}`                                                                                                          | [x]  |
| Q3  | Export under latency                       | `RUST_LOG=info rivet run --config dev/test_toxiproxy_pg.yaml --validate`                                                                                                         | Slower but still succeeds (safe profile allows 120 s statement timeout)                                                                   | [x]  |
| Q4  | Remove latency                             | `curl -s -X DELETE http://localhost:8474/proxies/pg/toxics/latency`                                                                                                              | Returns empty (toxic removed)                                                                                                             | [x]  |
| Q5  | Connection kill: cut connection after 5 KB | **Inject:** `curl -s -X POST http://localhost:8474/proxies/pg/toxics -H 'Content-Type: application/json' -d '{"name":"limit","type":"limit_data","attributes":{"bytes":5000}}'`  | Returns `{"name":"limit",...}`                                                                                                            | [x]  |
| Q6  | Export under connection kill               | `RUST_LOG=info rivet run --config dev/test_toxiproxy_pg.yaml`                                                                                                                    | Logs show retry attempts (`retry 1/3`, `[reconnecting]`). May succeed (if retry gets through) or fail with clear error — no panic or hang | [x]  |
| Q7  | Remove connection-kill toxic               | `curl -s -X DELETE http://localhost:8474/proxies/pg/toxics/limit`                                                                                                                | Toxic removed                                                                                                                             | [x]  |
| Q8  | Final: confirm clean proxy works           | `rivet run --config dev/test_toxiproxy_pg.yaml --validate`                                                                                                                       | `status: success` — proxy is back to normal                                                                                               | [x]  |


> **Shortcut:** run `bash dev/test_retry_toxiproxy.sh` to execute Q1–Q8 automatically.

---

## Sign-off


| Field          | Value                                    |
| -------------- | ---------------------------------------- |
| Tester         | Andrii Panchenko                         |
| Date           | 2026-03-29                               |
| Rivet commit   | c3788a5eb3a0635dd1dee898d362dfbacb20b0c8 |
| Postgres image | `dockercompose postgres:16`              |
| MySQL image    | `dockercompose postgres:16`              |
| Notes          |                                          |


---

## Reference — Config files


| File                                 | Purpose                          |
| ------------------------------------ | -------------------------------- |
| `dev/pg_full.yaml`                   | PG full exports, CSV + Parquet   |
| `dev/pg_incremental.yaml`            | PG incremental                   |
| `dev/mysql_full.yaml`                | MySQL full                       |
| `dev/mysql_incremental.yaml`         | MySQL incremental                |
| `dev/pg_structured.yaml`             | PG structured credentials        |
| `dev/mysql_structured.yaml`          | MySQL structured credentials     |
| `dev/bench_chunked_seq.yaml`         | Chunked sequential               |
| `dev/bench_chunked_p4.yaml`          | Chunked parallel                 |
| `dev/test_meta_columns.yaml`         | Meta columns + zstd              |
| `dev/test_compression.yaml`          | Snappy / none / skip_empty       |
| `dev/rivet_s3_minio_test.yaml`       | S3-compatible (MinIO)            |
| `dev/rivet_gcs_fake_test.yaml`       | GCS emulator                     |
| `dev/rivet_gcs_rivet_data_test.yaml` | Real GCS                         |
| `dev/test_stdout.yaml`               | Stdout destination (v4.1)        |
| `dev/test_params.yaml`               | Parameterized queries (v4.1)     |
| `dev/test_quality.yaml`              | Data quality checks (v4.1)       |
| `dev/test_file_split.yaml`           | File size splitting (v4.1)       |
| `dev/test_memory_batch.yaml`         | Memory-based batch sizing (v4.1) |
| `dev/test_toxiproxy_pg.yaml`         | Retry via Toxiproxy              |


