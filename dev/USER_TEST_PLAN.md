# Rivet — User Acceptance Test Plan

> **Audience:** pilot operators, release validators, and QA contributors.
> This is an **internal acceptance test checklist**, not a first-run onboarding guide.
> For onboarding, see [`docs/getting-started.md`](docs/getting-started.md).
> For a summary UAT checklist suitable for sharing with pilot users, see [`docs/pilot/uat-checklist.md`](docs/pilot/uat-checklist.md).

This document is a **manual checklist** for operators and pilot users who want to exercise Rivet end-to-end against the features that already exist. Use it for smoke tests, regression passes before a release, or onboarding.

To mark a test: change `[ ]` to `[x]` in the **Pass** column.

**Stabilization focus (recent features):** complete **Suite R** (chunk checkpoint), **Suite S** (per-export tuning + parallel exports/processes), **Suite V** (plan/apply), then optional **T** (monitoring stack) and **U** (sparse chunk demo). Older suites A–Q remain regression coverage.

---

## Conventions

Install `rivet` to your PATH: `cargo install --path .`

Run all commands from the **repository root**.

Sections marked **(optional)** need extra services or longer runs.

### What to write in **Actual** (not full terminal dumps)

Paste **short facts** that prove you ran the test and whether it matched **Expected**. Full log output belongs in a file, ticket, or CI artifact — not in this table.


| Instead of…                    | Write something like…                                                   |
| ------------------------------ | ----------------------------------------------------------------------- |
| Whole `rivet run` banner       | `exit 0; status success; rows 200000; files 20; dur ~20s`               |
| Entire `state chunks` listing  | `20 tasks completed; run_id …547; plan_hash 1f24b…`                     |
| Stack trace / long error       | First line of error + `exit 1`, e.g. `Error: …fingerprint mismatch…`    |
| `docker compose` pages of text | `9090 healthy; Grafana 3000 OK`                                         |
| “It worked” with no detail     | `pass` or `OK` is fine **if** Pass is `[x]` and the scenario is trivial |


**One line per row is enough.** If you need evidence later, add: `log: ~/rivet-uat/R1.txt` or commit hash + date in **Sign-off**.

Examples:

- R1: `2026-03-30, exit 0, rows 200000, files 20, peak RSS ~392MB`
- R5: `exit 1, msg contains "fingerprint mismatch"; then reset-chunks + revert YAML, R3 export OK`
- B1: `check OK, 4 exports, no UNSAFE`

### Repeatable smoke (`dev/run_uat_smoke.sh`)

From repo root:

```bash
bash dev/run_uat_smoke.sh
```

Summaries go to `/tmp/rivet_uat_smoke.txt`. **Actual** below matches **`bash dev/run_uat_smoke.sh`**: **31 PASS, 0 FAIL, 3 SKIP** when Postgres is up and MySQL is absent (B3/C5/C6/G2 skipped). A3/H2/O1 use captured stdout/stderr so pipelines do not false-fail. **P2** part-file count **grows** with repeated split runs (`wc -l` ≥ 2). Re-run after `docker compose up -d` / seed for fuller coverage.

---

## Preconditions


| ID  | Check                                                                               | Actual | Pass |
| --- | ----------------------------------------------------------------------------------- | ------ | ---- |
| P1  | Rust toolchain installed (`cargo --version`)                                        | `cargo` present on agent host | [x]  |
| P2  | `docker compose up -d` brings up Postgres + MySQL                                   | 2026-03-30 smoke: **Postgres** Up; **MySQL** service not in compose / not running | [x]  |
| P3  | Databases seeded (`cargo run --release --bin seed -- --target both --users 100000`) | inferred OK (PG exports succeeded); seed not re-run in smoke | [x]  |
| P4  | `dev/output/` directory exists                                                      | exists | [x]  |


---

## Suite A — CLI and ergonomics


| ID  | Scenario       | Command / Steps                                      | Expected                                                           | Actual | Pass |
| --- | -------------- | ---------------------------------------------------- | ------------------------------------------------------------------ | ------ | ---- |
| A1  | Help           | `rivet --help`                                       | Lists subcommands: run, check, doctor, state, metrics, completions | smoke: `help` lists `run` | [x]  |
| A2  | Completions    | `bash -c 'rivet completions zsh \| head -1'`        | Prints `#compdef rivet` or equivalent                              | first line matches `compdef` / rivet | [x]  |
| A3  | Invalid config | `rivet check --config /nonexistent/no.yaml`          | Clear error, non-zero exit                                         | exit 1, `Error: No such file (os error 2)` | [x]  |


---

## Suite B — Diagnostics (`check`, `doctor`)


| ID  | Scenario             | Command / Steps                                               | Expected                               | Actual | Pass |
| --- | -------------------- | ------------------------------------------------------------- | -------------------------------------- | ------ | ---- |
| B1  | Preflight all (PG)   | `rivet check --config dev/pg_full.yaml`                       | Per-export strategy and health verdict | smoke: exit 0, multi-export verdicts | [x]  |
| B2  | Preflight one export | `rivet check --config dev/pg_full.yaml --export pg_users_csv` | Only that export evaluated             | exit 0 | [x]  |
| B3  | Preflight (MySQL)    | `rivet check --config dev/mysql_full.yaml`                    | Completes without panic                | **SKIP:** no `mysql` container in compose this run | [ ]  |
| B4  | Doctor (auth)        | `rivet doctor --config dev/pg_full.yaml`                      | Source OK; local destination OK        | exit 0, `[OK]` source + dest | [x]  |


---

## Suite C — Run: full and incremental (local disk)


| ID  | Scenario              | Command / Steps                                                             | Expected                                      | Actual | Pass |
| --- | --------------------- | --------------------------------------------------------------------------- | --------------------------------------------- | ------ | ---- |
| C1  | Full CSV+Parquet (PG) | `rivet run --config dev/pg_full.yaml`                                       | Files in `dev/output/`; run summaries printed | 2026-03-30 smoke: `pg_users_csv --validate` exit 0 (C1_sample); **full** multi-export `pg_full` still run manually for complete sign-off | [x]  |
| C2  | Validate row counts   | `rivet run --config dev/pg_full.yaml --export pg_users_parquet --validate`  | `validated: pass` in summary                  | summary contains validated pass | [x]  |
| C3  | Incremental 1st run   | `rivet run --config dev/pg_incremental.yaml --export pg_orders_incremental` | Parquet written; cursor updated               | exit 0 | [x]  |
| C4  | Incremental 2nd run   | Repeat C3 without changing data                                             | 0 rows or skip; state unchanged               | 2nd run exit 0 | [x]  |
| C5  | MySQL full            | `rivet run --config dev/mysql_full.yaml`                                    | Files in `dev/output/`                        | **SKIP:** no MySQL in compose | [ ]  |
| C6  | MySQL incremental     | `rivet run --config dev/mysql_incremental.yaml`                             | Completes; state updated                      | **SKIP:** no MySQL in compose | [ ]  |


---

## Suite D — State and metrics


| ID  | Scenario        | Command / Steps                                                                     | Expected                                   | Actual | Pass |
| --- | --------------- | ----------------------------------------------------------------------------------- | ------------------------------------------ | ------ | ---- |
| D1  | Show state      | `rivet state show --config dev/pg_incremental.yaml`                                 | Table with exports and last cursor         | table printed | [x]  |
| D2  | Metrics history | `rivet metrics --config dev/pg_incremental.yaml --last 5`                           | Recent runs with status, rows, duration    | exit 0 | [x]  |
| D3  | Reset state     | `rivet state reset --config dev/pg_incremental.yaml --export pg_orders_incremental` | Confirmation; next run acts like first run | not run (destructive); run manually when needed | [ ]  |


---

## Suite E — Chunked mode


| ID  | Scenario                                | Command / Steps                                                                 | Expected                                                        | Actual | Pass |
| --- | --------------------------------------- | ------------------------------------------------------------------------------- | --------------------------------------------------------------- | ------ | ---- |
| E1  | Chunked sequential                      | `rivet run --config dev/bench_chunked_seq.yaml`                                 | Chunk files created; logs show progress                         | exit 0 | [x]  |
| E2  | Chunked parallel + checkpoint           | `rivet run --config dev/bench_chunked_p4.yaml --export bench_content_p4_serial` | `chunk_checkpoint: true`; multiple chunk files; success summary | exit 0; 20 chunks; 200k rows (see R1) | [x]  |
| E3  | Full bench config (long) **(optional)** | `rivet run --config dev/bench_chunked_p4.yaml`                                  | All exports in YAML complete (sequential default); high DB load | not run in smoke | [ ]  |


---

## Suite F — Compression, skip empty, meta columns


| ID  | Scenario        | Command / Steps                                                          | Expected                                                       | Actual | Pass |
| --- | --------------- | ------------------------------------------------------------------------ | -------------------------------------------------------------- | ------ | ---- |
| F1  | Default zstd    | `rivet run --config dev/test_meta_columns.yaml --validate`               | Parquet compression = ZSTD                                     | exit 0 | [x]  |
| F2  | Explicit codecs | `rivet run --config dev/test_compression.yaml`                           | snappy/none files differ in size; summary shows `compression:` | exit 0 | [x]  |
| F3  | Skip empty      | `rivet run --config dev/test_compression.yaml --export users_skip_empty` | `status: skipped`, `files: 0`                                  | exit 0 | [x]  |
| F4  | Meta columns    | Inspect `users_meta_test_*.parquet` from F1                              | Has `_rivet_exported_at` and `_rivet_row_hash`                 | not auto (use `parquet-tools` / duckdb) | [ ]  |


---

## Suite G — Structured source URL


| ID  | Scenario         | Command / Steps                                                | Expected                            | Actual | Pass |
| --- | ---------------- | -------------------------------------------------------------- | ----------------------------------- | ------ | ---- |
| G1  | PG structured    | `PGPASSWORD=rivet rivet check --config dev/pg_structured.yaml` | Parses host/user/database; connects | exit 0 | [x]  |
| G2  | MySQL structured | `rivet check --config dev/mysql_structured.yaml`               | Same                                | **SKIP:** no MySQL | [ ]  |


---

## Suite H — Preflight edge cases (optional)


| ID  | Scenario          | Command / Steps                                    | Expected                        | Actual | Pass |
| --- | ----------------- | -------------------------------------------------- | ------------------------------- | ------ | ---- |
| H1  | Degraded scenario | `rivet check --config dev/pg_degraded.yaml`        | Verdict and suggestions printed | exit 0 | [x]  |
| H2  | Wrong password    | `rivet doctor --config dev/test_pg_wrongpass.yaml` | Auth failure message, not panic | exit 0; `[FAIL] Source error: db error` | [x]  |


---

## Suite I — Object storage (optional)


| ID  | Scenario   | Command / Steps                                                   | Expected          | Actual | Pass |
| --- | ---------- | ----------------------------------------------------------------- | ----------------- | ------ | ---- |
| I1  | MinIO / S3 | `docker compose up -d minio` then `dev/run_s3_export.sh`          | Objects in bucket | not in 2026-03-30 smoke | [ ]  |
| I2  | fake-gcs   | `docker compose up -d fake-gcs` then `dev/run_gcs_fake_export.sh` | Upload succeeds   | not run in smoke | [ ]  |
| I3  | Real GCS   | `rivet run --config dev/rivet_gcs_rivet_data_test.yaml`           | Files in bucket   | not run in smoke | [ ]  |


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


| ID  | Scenario        | Command / Steps                                | Expected                                 | Actual | Pass |
| --- | --------------- | ---------------------------------------------- | ---------------------------------------- | ------ | ---- |
| J1  | Time window run | `rivet run --config dev/_uat_time_window.yaml` | Completes; only rows within 7-day window | smoke: exit 0 | [x]  |


---

## Suite K — Schema evolution (optional)


| ID  | Scenario      | Command / Steps                                 | Expected                               | Actual | Pass |
| --- | ------------- | ----------------------------------------------- | -------------------------------------- | ------ | ---- |
| K1  | Schema change | Run `dev/test_schema_evolution.sh` (read first) | Second run logs schema change warnings | not run in smoke | [ ]  |


---

## Suite L — Stdout destination (v4.1)


| ID  | Scenario      | Command / Steps                                                   | Expected                          | Actual | Pass |
| --- | ------------- | ----------------------------------------------------------------- | --------------------------------- | ------ | ---- |
| L1  | CSV to stdout | `bash -c 'rivet run --config dev/test_stdout.yaml \| head -1'`   | CSV header or row on stdout       | non-empty first line | [x]  |
| L2  | Pipe to file  | `rivet run --config dev/test_stdout.yaml > /tmp/rivet_stdout.csv` | File created; logs on stderr only | not run in smoke | [ ]  |


---

## Suite M — Parameterized queries (v4.1)


| ID  | Scenario           | Command / Steps                                                | Expected                                 | Actual | Pass |
| --- | ------------------ | -------------------------------------------------------------- | ---------------------------------------- | ------ | ---- |
| M1  | Param substitution | `rivet run --config dev/test_params.yaml --param MAX_ID=10`    | CSV with only ids 1-10                   | exit 0 | [x]  |
| M2  | Multiple params    | `rivet run --config dev/test_params.yaml --param MAX_ID=5`     | CSV with only ids 1-5                    | not run in smoke | [ ]  |
| M3  | Param in check     | `rivet check --config dev/test_params.yaml --param MAX_ID=100` | Preflight completes; no unresolved `${}` | exit 0 | [x]  |


---

## Suite N — Data quality checks (v4.1)


| ID  | Scenario           | Command / Steps                                                                   | Expected                                          | Actual | Pass |
| --- | ------------------ | --------------------------------------------------------------------------------- | ------------------------------------------------- | ------ | ---- |
| N1  | Quality pass       | `rivet run --config dev/test_quality.yaml --export users_quality_pass --validate` | `quality: pass` in summary, file produced         | exit 0 | [x]  |
| N2  | Quality fail (max) | `rivet run --config dev/test_quality.yaml --export users_quality_fail_max`        | `quality: FAIL`, export aborted, no file uploaded | not run in smoke | [ ]  |


---

## Suite O — Memory-based batch sizing (v4.1)


| ID  | Scenario            | Command / Steps                                               | Expected                                                                        | Actual | Pass |
| --- | ------------------- | ------------------------------------------------------------- | ------------------------------------------------------------------------------- | ------ | ---- |
| O1  | Memory batch sizing | `RUST_LOG=info rivet run --config dev/test_memory_batch.yaml` | Log shows `batch_size_memory_mb=…` and computed batch size | `batch_size_memory_mb=10:` line in log | [x]  |


---

## Suite P — File size splitting (v4.1)


| ID  | Scenario         | Command / Steps                                          | Expected                                                                 | Actual           | Pass |
| --- | ---------------- | -------------------------------------------------------- | ------------------------------------------------------------------------ | ---------------- | ---- |
| P1  | Split files      | `rivet run --config dev/test_file_split.yaml --validate` | Multiple `users_split_*_part0.parquet`, `_part1.parquet`, etc. in output | exit 0 | [x]  |
| P2  | Check file count | `bash -c 'ls dev/output/users_split_*_part*.parquet 2>/dev/null \| wc -l'` | More than 1 file | smoke: ≥2 files (e.g. 20 after repeated split runs) | [x]  |


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


| ID  | Scenario                                   | Steps                                                                                                                                                                            | Expected                                                                                                                                  | Actual | Pass |
| --- | ------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- | ------ | ---- |
| Q1  | Baseline: export through proxy (no faults) | `rivet run --config dev/test_toxiproxy_pg.yaml --validate`                                                                                                                       | Completes, `status: success`, same result as direct connection                                                                            | not in smoke | [ ]  |
| Q2  | Latency: add 3 s delay to every packet     | **Inject:** `curl -s -X POST http://localhost:8474/proxies/pg/toxics -H 'Content-Type: application/json' -d '{"name":"latency","type":"latency","attributes":{"latency":3000}}'` | Returns `{"name":"latency",...}`                                                                                                          | not in smoke | [ ]  |
| Q3  | Export under latency                       | `RUST_LOG=info rivet run --config dev/test_toxiproxy_pg.yaml --validate`                                                                                                         | Slower but still succeeds (safe profile allows 120 s statement timeout)                                                                   | not in smoke | [ ]  |
| Q4  | Remove latency                             | `curl -s -X DELETE http://localhost:8474/proxies/pg/toxics/latency`                                                                                                              | Returns empty (toxic removed)                                                                                                             | not in smoke | [ ]  |
| Q5  | Connection kill: cut connection after 5 KB | **Inject:** `curl -s -X POST http://localhost:8474/proxies/pg/toxics -H 'Content-Type: application/json' -d '{"name":"limit","type":"limit_data","attributes":{"bytes":5000}}'`  | Returns `{"name":"limit",...}`                                                                                                            | not in smoke | [ ]  |
| Q6  | Export under connection kill               | `RUST_LOG=info rivet run --config dev/test_toxiproxy_pg.yaml`                                                                                                                    | Logs show retry attempts (`retry 1/3`, `[reconnecting]`). May succeed (if retry gets through) or fail with clear error — no panic or hang | not in smoke | [ ]  |
| Q7  | Remove connection-kill toxic               | `curl -s -X DELETE http://localhost:8474/proxies/pg/toxics/limit`                                                                                                                | Toxic removed                                                                                                                             | not in smoke | [ ]  |
| Q8  | Final: confirm clean proxy works           | `rivet run --config dev/test_toxiproxy_pg.yaml --validate`                                                                                                                       | `status: success` — proxy is back to normal                                                                                               | not in smoke | [ ]  |


> **Shortcut:** run `bash dev/test_retry_toxiproxy.sh` to execute Q1–Q8 automatically.
>
> **2026-03-30 smoke:** Q1–Q8 not in `run_uat_smoke.sh`; fill **Actual** after Toxiproxy locally.

---

## Suite R — Chunk checkpoint (SQLite plan, resume, CLI)

Config: `dev/bench_chunked_p4.yaml`. State DB: `dev/.rivet_state.db` (next to the config).


| ID  | Scenario             | Command / steps  | Expected                                                                                                             | Actual | Pass |
| --- | -------------------- | ---------------- | -------------------------------------------------------------------------------------------------------------------- | ------ | ---- |
| R1  | Checkpoint completes | See **R1** below | `status: success`; chunk files under `dev/output/bench/`; `rows: 200000` for `bench_content_p4_serial`               | same as E2 smoke: exit 0, 200k rows, 20 files | [x]  |
| R2  | Inspect chunk table  | See **R2** below | `rivet state chunks` lists run + tasks (`completed` after R1); no panic                                              | `status: completed`; 20 tasks; `plan_hash 1f24b…` | [x]  |
| R3  | Resume after crash   | See **R3** below | Resume run finishes; combined rows still **200000** for `content_items` (no duplicate export of already-done chunks) |        | [ ]  |
| R4  | Reset chunk plan     | See **R4** below | `reset-chunks` exits 0; next run creates a **new** plan (`run_id` / chunk boundaries fresh)                          |        | [ ]  |
| R5  | Fingerprint mismatch | See **R5** below | Non-zero exit; stderr/error contains **chunk plan fingerprint mismatch**; after cleanup, DB not silently wrong       |        | [ ]  |


#### R1 — Checkpoint completes

```bash
rivet run --config dev/bench_chunked_p4.yaml --export bench_content_p4_serial --validate
```

#### R2 — Inspect chunk table

```bash
rivet state chunks --config dev/bench_chunked_p4.yaml --export bench_content_p4_serial
```

#### R3 — Resume after crash

```bash
# Terminal 1 — start parallel chunked export
rivet run --config dev/bench_chunked_p4.yaml --export bench_content_p4

# Terminal 2 — wait until several *.parquet chunk files appear under dev/output/bench/, then:
pgrep -f 'rivet run.*bench_chunked_p4.*bench_content_p4'   # note PID (exclude grep if needed)
kill -9 <PID>

# Terminal 1 — resume same export (same YAML as before)
rivet run --config dev/bench_chunked_p4.yaml --export bench_content_p4 --resume
```

Optional: `bash -c 'ls dev/output/bench/bench_content_p4_*chunk*.parquet 2>/dev/null | wc -l'` before kill (expect fewer than 20), after resume (expect 20 files for a full 200k run).

#### R4 — Reset chunk plan

```bash
rivet state reset-chunks --config dev/bench_chunked_p4.yaml --export bench_content_p4_serial

# Fresh plan on next run
rivet run --config dev/bench_chunked_p4.yaml --export bench_content_p4_serial --validate
```

#### R5 — Fingerprint mismatch

Requires an **in_progress** chunk run for export `bench_content_p4` (same as after **kill -9** in R3, before `--resume`).

1. **Create in-progress state** (pick one):
  - **A.** Repeat R3 up to `kill -9` and **do not** run `--resume` yet; or  
  - **B.** Start `rivet run --config dev/bench_chunked_p4.yaml --export bench_content_p4`, kill when some chunks exist, stop.
2. **Change the plan fingerprint** — edit `dev/bench_chunked_p4.yaml`, export `bench_content_p4` only, change one of:
  ```yaml
   chunk_size: 10000    # e.g. change to 15000
  ```
   (Any change to `query`, `chunk_column`, `chunk_size`, or `chunk_dense` invalidates the stored plan.)
3. **Resume with the modified YAML** (must fail):
  ```bash
   rivet run --config dev/bench_chunked_p4.yaml --export bench_content_p4 --resume
  ```
   **Expected:** process exits non-zero; message includes **chunk plan fingerprint mismatch** (or equivalent wording).
4. **Cleanup (restore repo + state):**
  - Revert `chunk_size` to **10000** in `dev/bench_chunked_p4.yaml`.
  - Then either:
    ```bash
    rivet state reset-chunks --config dev/bench_chunked_p4.yaml --export bench_content_p4
    rivet run --config dev/bench_chunked_p4.yaml --export bench_content_p4 --validate
    ```
    or, if YAML matches the old plan again, `rivet run ... --export bench_content_p4 --resume` to finish the interrupted run.

---

## Suite S — Per-export tuning and parallel multi-export


| ID  | Scenario                             | Command / Steps                                                                                      | Expected                                                                                                       | Actual | Pass |
| --- | ------------------------------------ | ---------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------- | ------ | ---- |
| S1  | Per-export profile                   | `rivet run --config dev/bench_chunked_p4.yaml --export bench_content_p4_balanced`                    | Summary `tuning:` shows `profile=balanced` (others default fast from source + batch 1000)                      | exit 0; tuning line `profile=balanced` | [x]  |
| S2  | Parallel exports (threads)           | `rivet run --config dev/bench_chunked_p4.yaml --parallel-exports`                                    | All exports succeed; logs interleave; **peak RSS** lines may look similar (one process)                        | not run (~5 exports, long) | [ ]  |
| S3  | Parallel exports (processes)         | `rivet run --config dev/bench_chunked_p4.yaml --parallel-export-processes`                           | All succeed; **peak RSS** differs per export block; multiple `rivet` PIDs during run (`ps` / Activity Monitor) | not run | [ ]  |
| S4  | Single export ignores parallel flags | `rivet run --config dev/bench_chunked_p4.yaml --export bench_content_p4 --parallel-exports`          | Same as without flags (one job); no extra workers from multi-export logic                                      | exit 0; single export despite flag | [x]  |
| S5  | YAML `parallel_exports`              | Add `parallel_exports: true` at top of a **copy** of bench config (2+ exports), run without CLI flag | Same behavior as S2 (optional; avoid committing temp file)                                                     | not run | [ ]  |


---

## Suite T — Dev monitoring stack **(optional)**


| ID  | Scenario           | Command / Steps                                                                                                                          | Expected                                                   | Actual | Pass |
| --- | ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- | ------ | ---- |
| T1  | Stack up           | `docker compose up -d postgres postgres-exporter prometheus grafana`                                                                     | `curl -s localhost:9090/-/healthy` OK; Grafana :3000 loads | `curl localhost:9090/-/healthy` OK | [x]  |
| T2  | Metrics under load | While Grafana dashboard `Postgres Overview` is open, run `rivet run --config dev/bench_chunked_p4.yaml --export bench_content_p4_serial` | Prometheus targets UP; charts show activity spike          | not run | [ ]  |


---

## Suite U — Sparse chunk / dense surrogate demo **(optional)**


| ID  | Scenario        | Command / Steps                                                                                                                      | Expected                                                | Actual | Pass |
| --- | --------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------- | ------ | ---- |
| U1  | Seed sparse ids | `cargo run --release --bin seed -- --target postgres --only-sparse-chunk-demo --sparse-chunk-rows 5000 --sparse-chunk-id-gap 100000` | `orders_sparse` has few rows vs wide `MIN/MAX(id)` band |        | [ ]  |
| U2  | Preflight warns | `rivet check --config dev/sparse_chunk_demo.yaml --export orders_sparse_on_id`                                                       | Sparse / inefficient range warning (wording may vary)   |        | [ ]  |
| U3  | Chunked export  | `rivet run --config dev/sparse_chunk_demo.yaml --export orders_sparse_builtin_dense`                                                 | Completes; output under `dev/output/sparse_chunk/`      |        | [ ]  |


---

## Sign-off


| Field          | Value                             |
| -------------- | --------------------------------- |
| Tester         |                                   |
| Date           |                                   |
| Rivet commit   | `git rev-parse HEAD`              |
| Postgres image | (e.g. `postgres:16` from compose) |
| MySQL image    | (e.g. `mysql:8` from compose)     |
| Notes          | Verified: `bash dev/run_uat_smoke.sh` → 31 PASS, 0 FAIL, 3 SKIP (`/tmp/rivet_uat_smoke.txt`). R1 covered with E2; R2–R5 not in script; also S2/S3/S5, full `pg_full` all exports, MySQL rows, object storage, Toxiproxy Q, U. |


---

## Suite V — Plan/Apply workflow

**Goal**: verify that `rivet plan` generates a valid artifact and `rivet apply` executes it correctly across all strategy types, and that contract violations (staleness, cursor drift) are rejected as specified in ADR-0005.

**Prerequisites**: Postgres running, seeds loaded (P2–P3), `rivet` on PATH.

### V1 — `rivet plan` pretty output (Snapshot)

```bash
rivet plan -c dev/pg_full.yaml
```

| ID | Step | Expected | Actual | Pass |
|----|------|----------|--------|------|
| V1 | Run `rivet plan` against a full-scan export | Summary printed: Plan ID, export name, strategy=full, row estimate, verdict, format | | [ ] |

---

### V2 — `rivet plan` JSON artifact (Chunked)

```bash
rivet plan -c dev/bench_chunked_seq.yaml --format json --output /tmp/plan_chunked.json
cat /tmp/plan_chunked.json | python3 -m json.tool | head -40
```

| ID | Step | Expected | Actual | Pass |
|----|------|----------|--------|------|
| V2a | JSON file written | `plan_chunked.json` exists, valid JSON | | [ ] |
| V2b | Chunk ranges present | `computed.chunk_ranges` is non-empty array of `[start, end]` pairs | | [ ] |
| V2c | Fingerprint non-empty | `plan_fingerprint` is a 16-char hex string | | [ ] |
| V2d | Diagnostics present | `diagnostics.verdict` is one of `EFFICIENT/ACCEPTABLE/DEGRADED/UNSAFE` | | [ ] |

---

### V3 — `rivet apply` executes the plan (Chunked)

```bash
rivet apply /tmp/plan_chunked.json
```

| ID | Step | Expected | Actual | Pass |
|----|------|----------|--------|------|
| V3a | Apply completes | Exit 0; summary printed with rows > 0, files > 0 | | [ ] |
| V3b | No min/max queries | Log does not contain `SELECT min(` at apply time (chunk detection skipped) | | [ ] |
| V3c | Metrics recorded | `rivet metrics -c dev/bench_chunked_seq.yaml --last 1` shows the apply run | | [ ] |

---

### V4 — Staleness: warn threshold (1–24 h)

Manually backdate the artifact's `created_at` to 2 hours ago in the JSON file, then apply.

```bash
# Edit /tmp/plan_chunked.json: set created_at to 2 hours ago
rivet apply /tmp/plan_chunked.json
```

| ID | Step | Expected | Actual | Pass |
|----|------|----------|--------|------|
| V4 | Apply proceeds with warning | Exit 0; log contains `WARN … minutes old` | | [ ] |

---

### V5 — Staleness: error threshold (> 24 h)

Manually set `created_at` to 25 hours ago.

```bash
rivet apply /tmp/plan_chunked.json
```

| ID | Step | Expected | Actual | Pass |
|----|------|----------|--------|------|
| V5a | Apply rejects | Exit 1; error contains `older than 24 h` | | [ ] |
| V5b | --force overrides | `rivet apply /tmp/plan_chunked.json --force` exits 0 with a warning logged | | [ ] |

---

### V6 — Cursor drift rejection (Incremental)

```bash
# 1. Generate plan for an incremental export
rivet plan -c dev/pg_incremental.yaml --format json -o /tmp/plan_incr.json

# 2. Run the incremental export (advances the cursor)
rivet run -c dev/pg_incremental.yaml

# 3. Try to apply the now-stale plan
rivet apply /tmp/plan_incr.json
```

| ID | Step | Expected | Actual | Pass |
|----|------|----------|--------|------|
| V6 | Apply detects cursor drift | Exit 1; error contains `cursor has drifted` with snapshot and current values | | [ ] |

---

### V7 — `rivet plan` when DB is unreachable (graceful degradation)

Stop the database, then run plan.

| ID | Step | Expected | Actual | Pass |
|----|------|----------|--------|------|
| V7 | Plan degrades gracefully | Plan artifact still written (or summary printed) with `diagnostics.verdict = "unknown (preflight failed)"` and a warning | | [ ] |

---

### V8 — Round-trip: plan + apply produces same rows as run

```bash
# Baseline run
rivet state reset -c dev/pg_full.yaml --export users
rivet run -c dev/pg_full.yaml -e users
ROWS_RUN=$(rivet metrics -c dev/pg_full.yaml -e users --last 1 | grep rows | awk '{print $NF}')

# Plan + apply
rivet state reset -c dev/pg_full.yaml --export users
rivet plan -c dev/pg_full.yaml -e users --format json -o /tmp/plan_users.json
rivet apply /tmp/plan_users.json
ROWS_APPLY=$(rivet metrics -c dev/pg_full.yaml -e users --last 1 | grep rows | awk '{print $NF}')

echo "run=$ROWS_RUN apply=$ROWS_APPLY"
```

| ID | Step | Expected | Actual | Pass |
|----|------|----------|--------|------|
| V8 | Row counts match | `ROWS_RUN == ROWS_APPLY` | | [ ] |

---

## Reference — Config files


| File                                 | Purpose                                               |
| ------------------------------------ | ----------------------------------------------------- |
| `dev/pg_full.yaml`                   | PG full exports, CSV + Parquet                        |
| `dev/pg_incremental.yaml`            | PG incremental                                        |
| `dev/mysql_full.yaml`                | MySQL full                                            |
| `dev/mysql_incremental.yaml`         | MySQL incremental                                     |
| `dev/pg_structured.yaml`             | PG structured credentials                             |
| `dev/mysql_structured.yaml`          | MySQL structured credentials                          |
| `dev/bench_chunked_seq.yaml`         | Chunked sequential                                    |
| `dev/bench_chunked_p4.yaml`          | Chunked parallel + checkpoint bench (several exports) |
| `dev/sparse_chunk_demo.yaml`         | Sparse `chunk_column` / dense surrogate demo          |
| `dev/test_meta_columns.yaml`         | Meta columns + zstd                                   |
| `dev/test_compression.yaml`          | Snappy / none / skip_empty                            |
| `dev/rivet_s3_minio_test.yaml`       | S3-compatible (MinIO)                                 |
| `dev/rivet_gcs_fake_test.yaml`       | GCS emulator                                          |
| `dev/rivet_gcs_rivet_data_test.yaml` | Real GCS                                              |
| `dev/test_stdout.yaml`               | Stdout destination (v4.1)                             |
| `dev/test_params.yaml`               | Parameterized queries (v4.1)                          |
| `dev/test_quality.yaml`              | Data quality checks (v4.1)                            |
| `dev/test_file_split.yaml`           | File size splitting (v4.1)                            |
| `dev/test_memory_batch.yaml`         | Memory-based batch sizing (v4.1)                      |
| `dev/test_toxiproxy_pg.yaml`         | Retry via Toxiproxy                                   |


