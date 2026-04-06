# Rivet Roadmap and Task Breakdown

## Goal

Shift the next phase from "building the extractor core" to **making Rivet predictable, auditable, and ready for real pilot usage**.

---

## Current State (v0.2.0-beta.1, 2026-04-05)

Rivet core is feature-complete for beta:

- PostgreSQL and MySQL streaming extraction
- Parquet and CSV output (zstd default, gzip, lz4, snappy, none)
- Local, S3, GCS, and **stdout** destinations (streaming uploads)
- full / incremental / chunked / time_window modes
- File size splitting (`max_file_size`)
- Parameterized queries (`--param KEY=VALUE`, `${VAR}` expansion)
- Skip empty exports (`skip_empty: true`)
- Shell completions (bash, zsh, fish, powershell)
- Memory-based batch sizing (`batch_size_memory_mb`)
- jemalloc allocator (default-on, ~30–40% RSS reduction)
- preflight check with strategy output, profile recommendation, sparse warnings
- `rivet doctor` for auth diagnostics
- SQLite state with versioned migrations
- metrics history, file manifest, chunk checkpoints
- schema tracking with change detection
- retry with typed error classification (SQLSTATE / MySQL error codes)
- validation (`--validate`)
- safe / balanced / fast tuning profiles
- Slack notifications (failure, schema_change, degraded)
- Data quality checks (row count bounds, null ratio, uniqueness)
- Meta columns for deduplication
- Misplaced config field detection with fix suggestions
- **617 tests** (537 unit + 80 integration), zero clippy warnings
- CI: rustfmt, clippy, test, release build, cargo audit

---

## Phase 1 — Pilot Alpha Stabilization ✅ COMPLETE

All tasks from the original Phase 1 are done.

### Epic A — Auth and Connectivity ✅

| Task | Status | Notes |
|------|--------|-------|
| A1. Credential precedence matrix | ✅ | README §Credential precedence, 4-layer model |
| A2. GCS ADC support | ✅ | `gcloud auth application-default login` works |
| A3. GCS explicit JSON credentials | ✅ | `credentials_file` in config, validated at load |
| A4. DB credential normalization | ✅ | URL and structured config, mutual exclusion check |
| A5. Auth diagnostics command | ✅ | `rivet doctor` verifies source + all destinations |

### Epic B — Preflight and Planner 2.0 ✅

| Task | Status | Notes |
|------|--------|-------|
| B1. Selected strategy output | ✅ | Check shows extraction strategy per export |
| B2. Profile recommendation | ✅ | Recommends safe/balanced/fast with reason |
| B3. Sparse range warning | ✅ | Chunk sparsity detection + warning |
| B4. Dense surrogate / sort cost warning | ✅ | Tradeoff explanation in check |
| B5. Parallel safety hints | ✅ | Memory/concurrency caution in check |
| B6. Better verdict suggestions | ✅ | 1–3 actionable steps per degraded verdict |

### Epic C — Execution Semantics Freeze ✅

| Task | Status | Notes |
|------|--------|-------|
| C1. Export lifecycle spec | ✅ | USER_GUIDE §Pipeline architecture |
| C2. State update point freeze | ✅ | Cursor advances after successful write+upload |
| C3. Duplicate semantics | ✅ | At-least-once documented, no exactly-once claim |
| C4. Retry semantics | ✅ | Typed classification, backoff table in docs |
| C5. Validation semantics | ✅ | Row-count validation scope clearly documented |

### Epic D — Observability and Run Summary ✅

| Task | Status | Notes |
|------|--------|-------|
| D1. Run summary schema | ✅ | name, rows, files, bytes, duration, RSS, retries, verdict |
| D2. End-of-run summary output | ✅ | Printed after each export |
| D3. Manifest-style accounting | ✅ | `rivet state files` links files to run_id |
| D4. Metrics-summary alignment | ✅ | Same identifiers in summary and `rivet metrics` |

### Epic E — Documentation Rewrite ✅

| Task | Status | Notes |
|------|--------|-------|
| E1. README repositioning | ✅ | Lightweight, source-safe, predictable, extract-only |
| E2. Choosing a mode guide | ✅ | USER_GUIDE §§3,7,8,9 with decision rules |
| E3. Choosing a profile guide | ✅ | USER_GUIDE §6 with production/replica examples |
| E4. Auth guide | ✅ | README + USER_GUIDE: GCS ADC/JSON, DB creds, env vars |
| E5. Guarantees and limitations | ✅ | README §Limitations, no CDC, no exactly-once |

### Epic M — Output & CLI Improvements

| Task | Status | Notes |
|------|--------|-------|
| M1. Configurable Parquet compression | ✅ | zstd default, snappy/gzip/lz4/none |
| M2. Skip empty exports | ✅ | `skip_empty: true`, state not advanced |
| M3. File size splitting | ✅ | `max_file_size: 512MB` |
| M4. Memory-based batch sizing | ✅ | `batch_size_memory_mb`, clamped [1000, 500000] |
| M5. Shell completions | ✅ | bash, zsh, fish, powershell |
| M6. Stdout destination | ✅ | `destination: { type: stdout }` |
| M7. Parameterized queries | ✅ | `--param KEY=VALUE`, `${VAR}` expansion |
| M8. Per-column Parquet encoding | ⏳ | Future — not started |

### Bonus: Completed items not in original roadmap

| Item | Notes |
|------|-------|
| jemalloc integration | Optional default-on allocator, ~30–40% RSS reduction |
| Streaming cloud uploads | S3/GCS/stdout use `std::io::copy`, no peak allocation |
| Misplaced tuning field detection | Config validation with fix suggestions |
| Versioned SQLite migrations | `schema_version` table, auto-upgrade |
| Graceful mutex poisoning | `unwrap_or_else` instead of `expect` for mutexes |
| Typed error classification | Postgres SQLSTATE + MySQL error codes |
| CI pipeline | 5 GitHub Actions jobs (fmt, clippy, test, build, audit) |
| 617 tests | Up from ~274, covering golden, regression, config validation |
| Slack notifications | failure, schema_change, degraded triggers |
| Data quality checks | Row count, null ratio, uniqueness per export |
| `cargo publish` packaging | exclude list, license, repository, rust-version |

---

## Phase 2 — Pilot Readiness and Battle Testing

### Epic F — Auditability and Correctness Confidence

| Task | Status | Priority | Notes |
|------|--------|----------|-------|
| F1. Export audit model | ✅ Partial | P1 | Rows read/written/validated tracked, but no formal reconciliation mode |
| F2. Bounded source count verification | ⏳ | P1 | Optional `COUNT(*)` for bounded modes |
| F3. Per-file row counts | ✅ | — | Every file has row_count in state DB |
| F4. Export reconciliation summary | ⏳ | P1 | `--reconcile` flag: source COUNT vs output rows |
| F5. Audit mode tradeoff docs | ⏳ | P2 | Document strict vs cheap verification |

### Epic G — Real-World Test Harness

| Task | Status | Priority | Notes |
|------|--------|----------|-------|
| G1. MinIO in dev environment | ⏳ | P1 | S3-compatible local storage (have fake-gcs-server for GCS) |
| G2. Network fault injection | ⏳ | P2 | Toxiproxy for timeout/reset/latency simulation |
| G3. Mutation runner | ✅ Partial | P1 | `seed.rs` inserts, but no update/late-arrive/sparse mutations |
| G4. Bad source fixtures | ✅ Partial | P1 | `dev/` has many configs; need no-index, huge-text, lock-contention scenarios |
| G5. Automated E2E matrix | ⏳ | P0 | `run_uat_smoke.sh` exists, need full Docker Compose matrix |

### Epic H — Crash and Recovery Battle Tests

| Task | Status | Priority | Notes |
|------|--------|----------|-------|
| H1. Crash matrix | ⏳ | P1 | Enumerate failure stages |
| H2. Failure injection hooks | ⏳ | P2 | Force failures at chosen lifecycle points |
| H3. Recovery integration tests | ⏳ | P1 | Verify rerun behavior after forced crash |
| H4. Rerun behavior documentation | ✅ Partial | P1 | Chunk resume documented, but not full crash matrix |

### Epic I — Performance and Capacity Envelope

| Task | Status | Priority | Notes |
|------|--------|----------|-------|
| I1. Benchmark datasets | ✅ Partial | P1 | Manual benchmarks done (narrow, wide, 2GB); need standardized fixtures |
| I2. Benchmark automation | ⏳ | P1 | One-command benchmark suite |
| I3. Recommended defaults | ✅ | — | USER_GUIDE §6, §17 with scenario guidance |
| I4. Memory-heavy warnings | ✅ | — | Check includes memory/concurrency warnings |
| I5. Capacity notes | ✅ Partial | P2 | Memory benchmarks documented; need formalized capacity guide |

### Epic J — Product UX Polish

| Task | Status | Priority | Notes |
|------|--------|----------|-------|
| J1. Example configs by scenario | ✅ Partial | P1 | `dev/` has many configs; need curated `examples/` directory |
| J2. Better error messages | ✅ | — | Typed errors, misplaced field detection |
| J3. "Next action" failure hints | ✅ | — | Troubleshooting section in USER_GUIDE |
| J4. `rivet doctor` | ✅ | — | Verifies auth + source + destination + config |

### Epic K — First Pilot Rollout

| Task | Status | Priority | Notes |
|------|--------|----------|-------|
| K1. Select pilot tables | ✅ | — | content_items (wide), users, orders tested |
| K2. Repeated pilot runs | ✅ Partial | P1 | Manual runs done; need multi-day automated runs |
| K3. Feedback template | ⏳ | P2 | Standard pilot feedback form |
| K4. Pilot findings document | ⏳ | P2 | Top pain points, wins, unsupported patterns |

---

## Phase 3 — Release Engineering & Ecosystem

### Epic L — Release and Distribution

| Task | Priority | Notes |
|------|----------|-------|
| L1. Cross-platform CI builds | P0 | GitHub Actions matrix: Linux x86_64/arm64, macOS arm64, Windows |
| L2. GitHub Releases with binaries | P0 | Automated tag → release with prebuilt binaries |
| L3. `cargo publish` to crates.io | P0 | `exclude` already configured in Cargo.toml |
| L4. Docker image | P1 | `ghcr.io/rivet-data/rivet` — FROM scratch + binary |
| L5. Homebrew formula | P2 | `brew install rivet-data/tap/rivet` |

### Epic N — Advanced Features (post-pilot)

| Task | Priority | Notes |
|------|----------|-------|
| N1. Per-column Parquet encoding | P2 | PLAIN, RLE, DELTA_BINARY_PACKED hints |
| N2. Data shape drift detection | P2 | Track text column max length across runs |
| N3. Auto-parallel mode | P3 | Heuristic parallelism from mode/source/profile |
| N4. `deny_unknown_fields` strict mode | P2 | Opt-in strict YAML validation |
| N5. Webhook destination | P3 | POST batches to HTTP endpoints |
| N6. Rate limiting | P3 | Configurable QPS/bandwidth cap per source |

### Epic O — Future Vision

| Task | Priority | Notes |
|------|----------|-------|
| O1. Delta / CDC mode | P3 | Logical replication or trigger-based change export |
| O2. Apache Iceberg / Delta Lake output | P3 | Write directly to lakehouse formats |
| O3. Multi-source joins | P3 | Export data joined across Postgres + MySQL |
| O4. Encryption at rest | P3 | AES-256 for output files |
| O5. Prometheus metrics endpoint | P3 | `/metrics` during long-running exports |
| O6. Plugin system | P3 | Custom source/destination/format via dynamic libraries |
| O7. Web UI dashboard | P3 | Monitoring exports |
| O8. ClickHouse / DuckDB source | P3 | Native source connectors |
| O9. Arrow Flight destination | P3 | Stream directly to consumers |
| O10. Serverless mode | P3 | AWS Lambda / Cloud Run compatible |

---

## Suggested Next Execution Order

### Immediate (this week)

1. **G5. Automated E2E matrix** — Docker Compose + `cargo test` for Postgres + MySQL end-to-end
2. **L1. Cross-platform CI builds** — GitHub Actions release workflow
3. **L2. GitHub Releases** — tag v0.2.0-beta.1 → prebuilt binaries

### Next week

4. **G1. MinIO** — local S3-compatible storage for E2E tests
5. **F2 + F4. Source count verification + reconciliation summary** — `--reconcile` flag
6. **H1 + H3. Crash matrix + recovery tests** — enumerate and test failure modes
7. **J1. Curated example configs** — `examples/` directory with 5 scenario configs

### Following weeks

8. **I1 + I2. Standardized benchmarks** — automated benchmark suite
9. **L3. `cargo publish`** — publish to crates.io
10. **L4. Docker image** — for non-Rust users
11. **K2. Multi-day pilot runs** — automated repeated exports
12. **N2. Data shape drift** — text column width tracking

---

## Definition of Done for Stable Release (v0.2.0)

- [ ] Auth is predictable and documented ✅
- [ ] `rivet check` gives actionable strategy and safety guidance ✅
- [ ] Execution semantics are frozen and documented ✅
- [ ] Run summary and basic reconciliation exist ✅ (reconciliation partial)
- [ ] Crash/recovery behavior is tested ⏳
- [ ] Local battle lab exists (MinIO + faults + mutation) ⏳
- [ ] Docs explain real scenarios, not only features ✅
- [ ] 2–3 pilot tables have been run repeatedly ✅ (manual)
- [ ] Cross-platform release binaries available ⏳
- [ ] E2E test matrix passes ⏳
- [ ] Published to crates.io ⏳
