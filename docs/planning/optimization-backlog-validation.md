# Optimization Backlog — Code Validation & Dev Prep

Validation of every assumption behind roadmap §10 (OPT-1…OPT-7) against the
actual source tree, with file:line evidence, the corrected residual scope, and a
development-ready implementation plan per item.

**Why this doc exists:** the first-pass §10 findings were written from a partial
read. A line-by-line validation corrected two of them substantially (OPT-4,
OPT-5) and narrowed two more (OPT-1, OPT-2). Treat the *verdict* column here as
authoritative over the original §10 prose.

## Verdict summary

| ID | Original claim | Verdict | Corrected scope |
|---|---|---|---|
| OPT-1 | "no hard memory bound, only sampling" | **Corrected** (done earlier) | Adaptive byte-budget cap exists; residual = probe warmup + per-value cap |
| OPT-2 | "sensing half built, not wired" | **Narrowed** | Batch-size adaptation exists; gap = govern *parallelism* + richer signals |
| OPT-3 | "no property-based type test" | **Confirmed** | Smoke-fuzz exists; no proptest type round-trip |
| OPT-4 | "silently degrades to buffered read on MySQL" | **Corrected** | Hard *refusal*, not silent; real gap = no keyset chunking ⇒ non-int-PK MySQL has no safe shape |
| OPT-5 | "no dedup token in manifest" | **Corrected** | Deterministic per-part `content_fingerprint` already exists; gap = guarantee/expose it + lib-stable logical hash |
| OPT-6 | "verify crash-matrix symmetry" | **Confirmed (gap real)** | Subprocess engine has *zero* crash tests; no signal handling |
| OPT-7 | "checksums ship but docs say otherwise" | **Confirmed** | Doc/roadmap drift, unchanged |

---

## OPT-1 — Memory-bound residual hardening

**Verdict: corrected (downgraded P0→P2).** A row-width-adaptive byte-budget cap
already backs the headline claim.

**Evidence (exists):**
- MySQL: `src/source/mysql/mod.rs:385-440` — 500-row probe → cap `effective_bs`
  to `batch_size_memory_mb` (default `MYSQL_BATCH_TARGET_MB_DEFAULT = 64`).
- PG: `src/source/postgres/mod.rs:369-375` — `FETCH N` capped under
  `work_mem × 0.7` (`pg_fetch_work_mem_bytes`) to avoid cursor spill.
- Reactive RSS sampler `src/resource.rs`; pause-gate
  `src/pipeline/chunked/exec.rs:66,269,271`.

**Evidence (residual — confirmed absent):**
- No per-value / per-cell size guard. Grep for `too_large|value.*size|oversize`
  returns only `column_max_bytes` (shape-drift *observability*,
  `src/pipeline/single.rs:482-506`) and file-split `max_rows`
  (`src/config/format.rs:115`) — neither bounds a single fat cell.
- Probe batch (500 rows) is buffered before bytes/row is known; uncapped warmup.

**Implementation plan.**
1. Add a hard per-value ceiling in the row→Arrow conversion
   (`src/source/{mysql,postgres}/arrow_convert.rs`): when a single value exceeds
   `tuning.max_value_bytes` (new, default e.g. 256 MB), fail with a typed
   `RIVET_VALUE_TOO_LARGE` error rather than risk OOM.
2. Shrink the probe when preflight `avg_row_bytes` (already computed in
   `src/plan/build.rs:272` / introspection) is large, so warmup respects budget.
3. Feed measured row width into the `check` predictor to tighten the pessimistic
   UNSAFE/DEGRADED verdict (`src/preflight/analysis.rs:check_parallel_memory_risk`).

**Tests.** Soak-matrix fixture with a 256 MB JSONB column: asserts clean
`RIVET_VALUE_TOO_LARGE` (or bounded RSS), never OOM.

**Effort: S–M. Risk: low** (additive guard).

---

## OPT-2 — Adaptive governor → parallelism

**Verdict: narrowed.** Batch-size adaptation already exists; extend it to govern
connection/parallelism count off the richer `rivet-mcp` signal set.

**Evidence (exists):**
- `tuning.adaptive` loop resizes batch via `next_adaptive_batch_size`:
  PG `pg_sample_checkpoints_req` (`src/source/postgres/mod.rs:395,501`),
  MySQL `mysql_sample_innodb_log_waits` (`src/source/mysql/mod.rs:444-464`).

**Evidence (gap):**
- Parallelism is static: `resource::Semaphore::new(parallel)`
  (`src/pipeline/chunked/exec.rs:246`), default `parallel = 1`
  (`src/config/export.rs:319`). Each worker opens its own connection
  (`exec.rs:305`); MySQL+adaptive opens a *second* sampling connection per
  worker (`src/source/mysql/mod.rs:343-348,515`). Preflight only *warns*
  (`src/preflight/analysis.rs:164-182`, `CONNECTION_HEADROOM = 3`).

**Implementation plan.**
1. Make `resource::Semaphore` permit count dynamic (add `resize(n)` /
   acquire-against-current-limit) — `src/resource.rs`.
2. In the chunked dispatch loop, sample pressure each `ADAPTIVE_SAMPLE_INTERVAL`
   and adjust permits + `throttle_ms` within `[min, max]`.
3. Reuse the `rivet-mcp` read-only pressure surface (`src/mcp.rs`) as one shared
   pressure model (lock waits / idle-in-txn / replication lag / active queries),
   not just checkpoint/log-wait proxies.
4. Log governor decisions to the run journal (`src/journal.rs`).

**Tests.** Concurrent-OLTP load fixture: adaptive run keeps a source-side
pressure metric below a threshold a static `parallel=N` run breaches; decisions
visible in `rivet journal`.

**Effort: L. Risk: medium** (touches the hot dispatch loop + both engines per
OPT-6).

---

## OPT-3 — Property-based type round-trip

**Verdict: confirmed.** No property-based type test exists.

**Evidence:**
- No `proptest`/`quickcheck`/`arbitrary` dependency in `Cargo.toml`.
- Existing `tests/{config_fuzz,planner_fuzz,format_fuzz}.rs` are explicitly
  *smoke* tests; headers say "Real corpus-driven fuzzing (cargo-fuzz) is
  deferred."
- Type coverage is enumerated fixtures + `tests/type_roundtrip/pg_edge_cases.rs`
  — by-example, which is why `UNSIGNED BIGINT → Decimal128` was caught late.

**Implementation plan.**
1. Add `proptest` (dev-dep). New `tests/type_roundtrip/property.rs`.
2. Strategy: generate random schemas over the supported type universe + random
   in-range values; seed into a live PG/MySQL fixture; export; read back via the
   existing DuckDB/pyarrow helpers (`tests/type_roundtrip/{duckdb,pyarrow}_load.rs`);
   assert value + field-metadata equality. Use proptest shrinking to minimize.
3. CI tiers: small N on PR, large N nightly (mirror the existing
   `dev/matrices/run.sh --tier` split).

**Tests.** The harness *is* the test; any mismatch fails with a minimized fixture
committed under `tests/type_roundtrip/fixtures/`.

**Effort: M. Risk: low** (additive; most autonomous item — good first build).

---

## OPT-4 — MySQL safe-shape gap (keyset chunking)

**Verdict: corrected.** It does **not** silently degrade — it *hard-refuses*.
The real gap is narrower and sharper: a MySQL table without a single-integer PK
has **no safe extraction shape at all**.

**Evidence:**
- MySQL reads client-side via `conn.exec_iter` — no server-side cursor
  (`src/source/mysql/mod.rs:358-360`).
- Chunk auto-resolve rejects composite PKs (`mod.rs:261-271`) and non-integer
  PKs (`mod.rs:273-293`; accepts only `tinyint|smallint|mediumint|int|bigint`).
- On no usable PK the planner **bails** with an actionable error, no silent
  fallback (`src/plan/build.rs:259-267`).
- Chunk SQL is integer `BETWEEN` only (`src/pipeline/chunked/math.rs:~102`); no
  keyset pagination anywhere.
- Snapshot mode *is* memory-bounded (`row_buf` + 64 MB cap) and streams
  row-by-row.

**The actual problem:** for a non-int / composite / UUID-PK MySQL table the only
options are (a) `mode: full` snapshot — bounded RSS but **one long-held
`SELECT`** (the exact "don't hold a long query on prod" risk the product
exists to avoid, and MySQL has no server cursor to shorten it), or (b) chunked —
**refused**. So the safety promise has a real hole for a common table shape.

**Implementation plan.**
1. Implement keyset (seek) pagination as a chunk strategy for MySQL: order by
   the best available unique index, page with
   `WHERE (k1,k2,…) > (last…) ORDER BY k1,k2,… LIMIT chunk_size`. Touches
   `src/source/mysql/mod.rs` (introspection to find a usable unique index),
   `src/pipeline/chunked/math.rs` / `detect.rs` (a new keyset chunk source), and
   `src/source/query.rs` (SQL shaping — note it already has a composite-cursor
   `COALESCE` ORDER BY path to model on, lines 72-123).
2. Until keyset lands: upgrade the snapshot-on-large-MySQL-table case from silent
   to an explicit preflight verdict — `rivet check` flags "no chunkable key;
   snapshot will hold a single long query for ~Ns" so the operator opts in
   knowingly.

**Tests.** Live MySQL fixture with a UUID PK: keyset chunked export bounds both
RSS and longest-query time; preflight verdict asserted.

**Effort: L. Risk: medium** (new read shape; correctness of keyset boundaries).

---

## OPT-5 — Dedup: expose & guarantee the existing fingerprint

**Verdict: corrected.** A **deterministic per-part content hash already exists**
in the manifest. OPT-5 shrinks from "build a dedup token" to "make the existing
one a guaranteed, documented dedup key + harden it across parquet lib versions."

**Evidence (exists):**
- `ManifestPart.content_fingerprint: String` (`src/manifest.rs:160`), an xxh3 of
  the file bytes, computed by streaming the temp file
  (`src/pipeline/manifest_writer.rs:168` `compute_part_fingerprint`), at every
  `dest.write()` in the I2/I3 window (`record_committed_part`,
  `manifest_writer.rs:~200`).
- Proven content-dependent + deterministic-for-same-content by unit tests
  (`manifest_writer.rs:471,484,494`).
- Manifest also carries `schema_fingerprint` (`manifest.rs:114`) and `_SUCCESS`
  carries a manifest fingerprint (`manifest.rs:65`).

**Residual gaps (the real scope):**
1. The fingerprint is over **file bytes**, not row content — so two
   re-extractions of the same rows produce the same key **only if Parquet output
   is byte-deterministic**. `src/format/parquet.rs:59` sets `WriterProperties`
   (compression) but the default `created_by` carries the parquet-rs version
   string; a lib bump changes the bytes for identical rows. **Verify** whether a
   fixed `created_by` / sorted metadata is needed for cross-version stability.
2. No logical dedup *key* tying the part to `(chunk_id, cursor_range)`; consumers
   must dedup on the byte-hash, which is undocumented as a contract.

**Implementation plan.**
1. Document the `content_fingerprint` as the supported dedup key in
   `recipes/idempotent-warehouse-load.md` and `docs/semantics.md` (it currently
   exists but isn't promised).
2. Pin Parquet determinism: set an explicit, version-independent `created_by`
   and stable key-value metadata ordering in `src/format/parquet.rs` so identical
   rows → identical bytes → identical fingerprint across releases.
3. (Optional) Add a logical row-content hash (xxh3 over canonical row encoding)
   alongside the byte hash for consumers who can't depend on parquet bytes.

**Tests.** Re-extract the same window after a simulated SIGKILL (ties into the
§9.6.1 open I3 test): assert the two parts share `content_fingerprint`. Add a
determinism test: same rows, two writes, equal bytes.

**Effort: S (doc + pin) / M (logical hash). Risk: low.**

---

## OPT-6 — Crash-matrix symmetry across engines

**Verdict: confirmed — the gap is real and precise.**

**Evidence:**
- In-process engines covered symmetrically with fault injection at each ADR-0001
  boundary: `tests/live_crash_recovery.rs:138-326` (F1–F4 single/incremental),
  `tests/live_chunked_recovery.rs` (C1–C4 sequential + parallel checkpoint),
  `tests/live_mysql_crash_recovery.rs` (MySQL variants).
- Subprocess fan-out engine (`src/pipeline/parallel_children.rs`) has **zero**
  crash/kill tests — only a happy-path smoke test in `tests/live_cli_flags.rs`
  (`run_parallel_export_processes_flag_runs_both_exports`).
- No SIGTERM/SIGINT/SIGKILL handlers; controlled panics use `RIVET_TEST_PANIC_AT`
  (`src/test_hook.rs`). I1 (finalize-before-write) is enforced at all dest.write
  sites (`single.rs:220-222,322`; `exec.rs:106-108`;
  `parallel_checkpoint.rs:252-254,289`); temp files via `tempfile::NamedTempFile`
  (`src/pipeline/sink/mod.rs:81-82`).

**Risk characterization:** because of `NamedTempFile` + I1, a SIGKILL can't push a
footerless parquet to the destination (the footer is written to the temp file
*before* the copy). The residual risk is **orphan temp files** on hard kill and
**unverified parent-side handling** of a child that dies mid-export.

**Implementation plan.**
1. Add subprocess crash tests: spawn `--parallel-export-processes`, SIGKILL one
   child mid-export, assert (a) parent reports the failure and continues the
   others, (b) no partial file at the destination, (c) temp dir has no orphan on
   the next clean run.
2. Optional: a SIGTERM handler for graceful drain (finish current batch, skip
   dispatch) — but only if it can be shared across both engines (avoid the
   ADR-0010 double-implement trap).

**Tests.** As above; mirror the F/C boundary matrix for the subprocess engine.

**Effort: M. Risk: low** (mostly test code).

---

## OPT-7 — Doc / roadmap drift

**Verdict: confirmed (unchanged).**

**Evidence:**
- `.github/workflows/release.yml:153` generates `SHA256SUMS.txt`; the v0.7.8
  release publishes it as an asset — yet `SECURITY.md:165-169` still says "verify
  by rebuilding from source… `git checkout v0.6.0`" and §5.1 marks checksums
  `⏳ Open`.
- §9.6.1 ⏳ items already shipped in 0.7.8 (retry-safe WARN→DEBUG, TLS warning in
  doctor/check, init mode-choice comment, chunk_size scaling, skipped-run
  context) per the 0.7.8 `polish(ux)` notes.

**Implementation plan.** Edit `SECURITY.md` + `README.md` with checksum
verification against `SHA256SUMS.txt`; strike the shipped §9.6.1 items; flip §5.1
+ §9.7 checksum rows to done; mention checksums in the v0.7.8 release notes.

**Effort: S. Risk: none.** Do first.

---

## Recommended build order (post-validation)

1. **OPT-7** — hours, removes a trust leak, corrects the planning baseline.
2. **OPT-3** — most autonomous; converts type rigor to provable.
3. **OPT-5** — small (doc + parquet determinism pin) on an existing mechanism.
4. **OPT-4** — keyset chunking closes a real MySQL safety hole.
5. **OPT-2** — adaptive governor (build after OPT-6 so it lands in both engines).
6. **OPT-6** — subprocess crash tests (prerequisite for OPT-2 symmetry).
7. **OPT-1** — per-value cap + probe warmup (narrow edge-case hardening).
