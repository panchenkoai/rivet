# Changelog

## Unreleased

### Added

- **CDC resource-conflict validation** (`RIVET_CONFIG_CDC_RESOURCE_CONFLICT`). Two `mode: cdc` exports
  resolving to the same PostgreSQL slot, MySQL `server_id`, or checkpoint path are now rejected at config
  load — **including the defaults colliding** (`rivet_slot` / `4271`), which is what a naive multi-table
  CDC config hits: a shared slot is acked past changes the other export never read (mutual silent data
  loss), a shared `server_id` gets the older replica connection killed by the server, and a shared
  checkpoint file gets its resume position overwritten. SQL Server `capture_instance` sharing stays
  allowed (read-only poll; resume state is the per-export checkpoint).

### Fixed

- **MySQL CDC: an idle first run now pins the resume position.** The checkpoint file was written only
  at a part commit, and a MySQL run without a checkpoint anchors to the *current* master position
  (`SHOW MASTER STATUS`). So a bounded first run that drained zero changes left no checkpoint, the next
  run re-anchored to a newer "now", and every change in between was **silently skipped** — the realistic
  "enable CDC during a quiet period" sequence lost data. `open_or_resume` now persists the open
  coordinates immediately when no checkpoint exists (the client-side analogue of PostgreSQL's
  server-side slot pinning at creation; SQL Server was never exposed — no checkpoint there floors at
  `fn_cdc_get_min_lsn`, over-reading instead of skipping). Regression (live, two-run):
  `first_run_with_zero_changes_pins_the_checkpoint_at_open`.

- **CDC parts no longer overwrite across runs.** CDC part files were named `cdc-{seq:06}` with the
  sequence restarting at 0 every run, so the documented scheduler model (`until_current: true` re-run
  into the same destination prefix) silently overwrote the previous cycle's parts — *after* the source
  had been acked past those changes, making them unrecoverable (not in the slot, not in the destination).
  Part names now carry the run id (`cdc-<run_id>-000000.parquet`, filename-sanitized), mirroring the
  batch path's run-stamped naming, so consecutive runs append alongside each other; the run id's
  millisecond precision keeps back-to-back scheduler cycles collision-free (two live cycles landed
  125 ms apart). `manifest.json`/`_SUCCESS` still describe the latest run only — same as a batch re-run.
  Regression: `roast_second_run_into_same_prefix_must_not_clobber_prior_parts`.

## 0.16.3 (2026-06-30) — dependency bump: arrow / parquet 58 → 59

A pure dependency bump (no config-grammar change — the schema bump is the version title only).

### Changed

- Bumped `arrow`, `arrow-schema`, and `parquet` 58 → 59. parquet 59 reshaped `LogicalType`'s parametrized
  variants (struct-variants with inline fields → tuple-variants wrapping nested structs); the only code
  affected was one type-round-trip test asserting Parquet logical types, migrated to parquet 59's public
  `LogicalType::{integer,decimal,time,timestamp}` constructors. **No behavioral change** — the live
  `parquet_schema_pins_{postgres,mysql}_matrix_logical_types` tests confirm arrow 59 emits byte-identical
  Parquet logical types. The bump also drops three transitive deps (`integer-encoding`, `ordered-float`,
  `thrift`), 497 → 494 crates.

## 0.16.2 (2026-06-30) — operator diagnostics: stable codes, `--json` everywhere, warning severity, `doctor --json`

Backward-compatible operator-UX additions (roadmap §9.6 item 7). The config grammar is unchanged — the
schema bump is the version title only.

### Added

- **Stable error codes.** Config and source failures now carry a stable `RIVET_CONFIG_*` / `RIVET_SOURCE_*`
  code on the typed error, surfaced as a JSON `code` field and a `[CODE]` prefix on the text error line, so
  a scheduler / CI step matches on the code, not the wording. Codes: `RIVET_CONFIG_NO_EXPORTS`,
  `RIVET_CONFIG_CHUNK_COUNT_INVALID`, `RIVET_CONFIG_CHUNK_BY_DAYS_INVALID`, `RIVET_CONFIG_DUPLICATE_EXPORT`,
  `RIVET_SOURCE_STATEMENT_TIMEOUT`.
- **`--json` on the remaining inspect commands** — `metrics`, `state show`, `state files`, `state chunks`
  (the rest already had `--format json`). `state chunks --json` is a run-header + per-chunk `tasks[]` composite.
- **Per-warning severity.** `rivet check`'s preflight warnings carry a `low` / `medium` / `high` severity:
  `check --json` emits `warnings: [{ severity, message }]` and the text path prefixes `[SEVERITY]`, so CI can
  gate on `high` and treat the rest as advisory.
- **`doctor --json`** — `rivet doctor --json` emits `{ config_path, all_ok, checks: [{ name, ok, detail?,
  hint? }] }` for CI / cron that wants a structured health verdict instead of grepping `[OK]`/`[FAIL]` lines.

### Internal

- `doctor` text + JSON now render from a single `DoctorCheck` collection (was a dual-emit `println!` + push
  maintained in parallel, which could drift); the text output is byte-for-byte unchanged.
- Dropped two pieces of dead weight a self-review found: a never-constructed `Severity::Blocking` token and
  the always-`Generic` `class` field on the coded-error marker (whose `classify_exit` arm was a no-op).
- Roadmap doc hygiene — struck several already-shipped items the backlog still listed as open (release
  checksums, the I3 at-least-once dupe test, the §9.6.1 UX items), each verified against the code first.
- Bumped `anyhow` 1.0.102 → 1.0.103, clearing RUSTSEC-2026-0190 (a `downcast_mut` unsoundness Rivet does
  not reach — it uses only `downcast_ref`); the upstream fix is preferred over an audit-ignore entry.

## 0.16.1 (2026-06-29) — release Docker images build again

0.16.0 published to crates.io, shipped its binaries, and cut a GitHub release — but both Docker images
(`linux/amd64`, `linux/arm64`) failed to build. `cargo chef prepare` (the dependency-caching step) uses a
spec-strict TOML parser that rejects **multi-line inline tables**, which `cargo build` tolerates. Two
`dep = {` … `}` entries (`postgres`, `postgres-types`) tripped it with `invalid inline table`, so the
publish landed half-complete and could not be re-run from the tag.

### Fixed

- Collapsed the `postgres` and `postgres-types` dependency tables onto one line each (no version/feature
  change — `Cargo.lock` untouched). The release Docker build passes again.

### Internal

- Regression guard: an offline test (`cargo_manifest_chef`) asserts `Cargo.toml` carries no multi-line
  inline tables, so the parser mismatch fails loud and local instead of at release time.

## 0.16.0 (2026-06-28) — the plan→apply cycle: wave-ordered, cost-gated, resumable execution

`rivet plan` becomes executable. It already scored and waved your exports; now `rivet apply <config>`
runs them — wave by wave, lowest priority first, with a barrier between waves and cost-gated within-wave
parallelism. This closes the OSS engine's loop: `init` → `plan` (annotates the config in place) →
[review/edit] → `apply`. This release also ships the operator-UX and verify-depth work that was merged in
#53 but never released (v0.15.0 cherry-picked only the always-on checksum from it).

### Added — the plan → apply cycle

- **`rivet apply <config>.yaml` runs exports wave-by-wave.** `rivet plan` writes a `wave:` and
  `parallel_safe:` onto each export (in place, comments and field order preserved); `apply` runs them in
  ascending `wave:` order with a barrier between waves. Tables are independent, so a failed export does
  **not** block its wave-mates — `apply` collects failures, runs the rest, and exits non-zero
  (continue/isolate). A `.json` path still means the sealed single-artifact replay.
- **Cost-gated within-wave parallelism** (`--parallel-export-processes`, or `parallel_export_processes:
  true`). The cheap exports — `parallel_safe: true` (cost class `Low`, and not flagged
  `isolate_on_source`) — run concurrently as separate processes; a heavier export runs alone in its wave
  (it already chunk-parallelizes internally), so two big tables never multiply load on the source.
- **`rivet apply <config> --resume`** skips exports a prior run already completed (`_SUCCESS` present)
  and resumes incomplete chunked exports from their checkpoints, so recovering a partially-failed run
  does not redo the tables that already succeeded.
- **`rivet plan` writes the advisory schedule into the config** and prints a compact one-line-per-export
  table (Wave | Export | Strategy | Rows | Verdict). `rivet init`'s next-steps surface the plan→apply path.

### Added — operator UX & verify (#53)

- **Graded `rivet validate --depth light|sample|full`** with stable `RIVET_VERIFY_*` error codes — one
  per failure variant (JSON `code` + the pretty `[CODE]` prefix).
- **Memory-aware `rivet check` verdict** — an unindexed large scan is DEGRADED (not UNSAFE) when the
  estimated peak RSS fits the budget; **`check --json`** emits the per-export verdict / strategy /
  warnings / recommended profile + parallel, not just the type report.
- **`rivet plan` explains the strategy** — why this mode, chunk geometry, parallelism, RSS-budget fit,
  resumability and memory risk.
- **Signed releases** — keyless cosign (Sigstore).

### Changed — multi-export console UX

- **Strict aligned card table.** Per-export cards render as a table: the name plus every metric column
  (rows / files / size / duration / RSS) is fixed-width and aligned, and the name column is seeded
  wave-wide so it stays aligned across the cost gate's separate safe/lone batches.
- **Chunk progress ticks per source batch, not only per chunk** — a wide first chunk (e.g. 250k rows /
  90 MB) no longer reads as idle while it streams; the bar shows rows/rate climbing throughout the read.
- **Captured child stderr goes to a file artifact** (`rivet-child-stderr-<ts>.log`) with a one-line
  console pointer, instead of flooding the run summary with every child's per-export log.

### Fixed

- **Chunked boundary probes no longer force a full scan.** The NULL-key guard is a presence probe
  (`SELECT 1 … WHERE col IS NULL LIMIT 1`, `TOP 1` on SQL Server) instead of `COUNT(*) - COUNT(col)` —
  the planner returns nothing without scanning for a `NOT NULL` column, and stops at the first NULL
  otherwise. A plain `SELECT <cols> FROM <table>` projection now probes the bare table (index-only)
  instead of wrapping the wide query in a subquery.

### Internal

- Test-infra consolidation (#53). plan→apply pre-PR review fixes (orphaned doc-comment, table-geometry
  split across two files, `parallel_safe` now respecting the campaign's `isolate_on_source`). Live tests:
  a failed wave isolates its successors; `--resume` skips completed exports. A "measure cold, don't
  theorize" performance-diagnosis rule added to `CLAUDE.md`.

## 0.15.0 (2026-06-25) — always-on value-integrity checksum

Every export now cross-checks its data end-to-end, on by default. An always-on per-column `xxh3`
value checksum catches silent value corruption the manifest's row-count + content hash could not: a
`build_array` converter fault (caught in process) and an `Arrow→Parquet` encode / post-write fault
(caught on `rivet validate`). The cost is hidden by the I/O-bound export path (+2.8% wall on a 1.93 M-row
table).

### Added

- **Form A — in-process value cross-check (always-on).** An independent source-side pass over the raw
  driver values is compared against the built Arrow batch on every export; a mismatch fails the run loud,
  naming the column, instead of writing the corrupted batch. Catches the per-engine value converter
  (`build_array`).
- **Form B — manifest-recorded checksum + `rivet validate` re-read (always-on).** The per-column `xxh3`
  is recorded in the manifest (`column_checksums`, name-keyed) and `rivet validate` re-reads the exported
  Parquet to re-verify it — catching an encoder fault or post-write / bit-rot corruption Form A cannot
  see. Live round-trip proven: export → validate passes → flip one byte → validate fails naming the column.
- **Swap-resistant when keyed.** When the export has a cursor / key column, each cell hashes as
  `xxh3(key ‖ value)` (`checksum_key_column` in the manifest), so a value swapped between two rows — which
  an order-independent combine would otherwise miss — is caught.
- **Visible coverage, never a silent "verified".** Types not yet covered (UUID / `FixedSizeBinary`, List,
  `Decimal256`, nanosecond timestamps) are logged once per export ("value checksum covers N/M columns;
  NOT checked: …") instead of contributing a silent zero that reads as verified.

### Notes

- **Manifest stays back-compatible — `MANIFEST_VERSION` unchanged at 1.** `column_checksums` +
  `checksum_key_column` are additive Optional fields; older manifests omit them and newer readers tolerate
  their absence, so `rivet validate` keeps working across versions.
- **Honest scope.** The guarantee is *decoded-value → file* for covered types, probabilistic (xxh3-64),
  with the DB-wire decode trusted (the same `FromSql` feeds both sides) — **not** byte-for-byte
  source→file. CSV exports get Form A only (the Parquet re-read is Parquet-only).
- **Cost vs 0.14.1** (content_items, 1.93 M × 20 cols): +2.8% wall / +11.3% CPU / ~0 RSS. The dominant
  cost is the independent re-decode + passes, not the hashing — the I/O-bound export hides most of it on
  wall time.

### Internal

- Permanent matrix-guard regression oracle: the source-side and Arrow-side checksums must agree
  byte-for-byte across the full PostgreSQL / MySQL / SQL Server type matrices (the test that would have
  caught the PG-enum side-A gap before ship). Validated live end-to-end through DuckDB, ClickHouse,
  pyarrow, BigQuery, and Snowflake.
- Docs: `README` + `docs/semantics.md` now distinguish **CDC-to-files** (`mode: cdc`, shipped 0.14.0)
  from continuous / live-streaming replication (the actual non-goal); roadmap accuracy revision.

## 0.14.1 (2026-06-24) — SQL Server CDC retention fix + recovery docs

A patch on 0.14.0: one real data-loss fix on the SQL Server CDC resume path, plus the
operator-facing "what happens when X fails" documentation the CDC reference was missing.

### Fixed

- **`fix(cdc)` — SQL Server CDC resume past retention no longer silently skips changes.** When the
  saved LSN had fallen below the change table's min LSN (the cleanup job removed the changes after
  it), the poll silently clamped to min and resumed — skipping the cleaned-up changes with **no
  error**. It now `THROW`s (*"the resume position is older than the change-table retention …
  re-snapshot"*), so the gap is recovered by a fresh snapshot rather than hidden. First run
  (`from = min`) is unaffected; only a stale resume checkpoint trips it. Regression test added (a
  below-min checkpoint must error, not skip).

### Docs

- **CDC failure modes & recovery** (`docs/reference/cdc.md`): the shared flush → checkpoint → ack
  model (at-least-once; dedupe by primary key + `__op`; a partial run leaves no `_SUCCESS`), then
  per engine — PostgreSQL slot/disk (an abandoned slot pins WAL; `max_slot_wal_keep_size` bounds
  it), MySQL purged binlog (ERROR 1236 → re-snapshot), SQL Server retention + a stopped Agent.
- **Reading from a replica** (no primary access), per engine: MySQL (`log_replica_updates`),
  PostgreSQL 16+ (logical decoding on a standby), SQL Server Always On readable secondary.

### Internal

- CDC test harnesses (property: replaying the captured changes reconstructs the source, and the
  same under a mid-flight crash; schema-drift; read-from-a-replica; oracle round-trip via DuckDB +
  ClickHouse; throughput/scale) and generative batch differential harnesses (boundary completeness,
  crash-resume, multi-export isolation, CSV escaping, MySQL + SQL Server engine variants); plus a
  unit test for the durable-sequence failure branch (flush error ⇒ no checkpoint/ack).
- Bench: a declarative row-size RSS sweep and a PostgreSQL cross-tool **DB-harm matrix** (snapshot
  hold-time + peak backends across rivet, ingestr, sling, duckdb, odbc2parquet).
- CI: nightly now starts Azurite — it was missing, so the azure-multipart test failed the whole
  suite every night.

## 0.14.0 (2026-06-24) — change data capture to files (Epic 15)

Rivet learns to read the source's transaction log. Beyond batch snapshots, it now captures
every INSERT / UPDATE / DELETE — MySQL binlog, a PostgreSQL logical slot, SQL Server change
tables — and writes them as typed Parquet/CSV through the **same commit seam** (destination +
content-MD5 + manifest + `_SUCCESS`) the batch path uses. A CDC export lands in your bucket and
shows up in `rivet metrics` / `rivet journal` exactly like a batch one. This is CDC **to files**
— bounded, resumable, source-safe — not a streaming platform.

### Added

- **`feat(cdc)` — log-based change data capture across all three engines.** A new `mode: cdc`
  (in `rivet run` config) and a `rivet cdc` CLI command capture row changes from the MySQL
  binlog, a PostgreSQL `test_decoding` logical slot, or SQL Server change tables, emitting
  `[__op, __pos]` + the source columns — typed, as the after-image (the key columns for a
  DELETE). `rivet init --mode cdc` scaffolds the config (incl. `--gcs-bucket` / `--s3-bucket`).
- **At-least-once, resumable, crash-tested.** Durable position state per engine — a checkpoint
  file (MySQL / SQL Server) or slot advance (PostgreSQL) — written only after the part is
  durable (flush → checkpoint → ack). A crash before the checkpoint re-reads on resume and never
  loses a change; validated live per engine via a `RIVET_TEST_PANIC_AT` fault point.
- **Full type parity with the batch export.** The CDC sink reuses `build_arrow_field`, so a
  captured column lands as the identical Arrow type a batch export produces — int widths,
  decimal precision/scale, JSON, UUID, naive **and tz-aware** timestamps — validated
  byte-for-byte against DuckDB, ClickHouse, and BigQuery and locked by a regression test
  (`mssql_cdc_full_type_matrix_matches_batch`).
- **Same observability + commit seam as batch.** Each part uploads via the existing destination
  + content-MD5 + manifest + `_SUCCESS` path (so `--output gs://…` / `s3://…` works); a CDC run
  records an `export_metrics` row + a `run_journal` entry, so `rivet metrics` and `rivet journal`
  show it like a batch run. `mode: full` + `mode: cdc` of the same table run in parallel under
  one `rivet run`.
- **Operational guardrails.** Per-engine permission/setup hints on a failed CDC start; fail-fast
  through a MySQL query proxy (ProxySQL / MaxScale can't carry the replication stream); a TLS
  gate on every adapter; `--table` identifier validation; memory-budget part rollover
  (`rollover_memory_mb`); `--until-current` (drain to the current end and exit).

### Fixed

- **`fix(mssql)` — SQL Server CDC now resumes by LSN (was re-reading the whole change table).**
  The adapter read `fn_cdc_get_all_changes(min_lsn, max_lsn)` on every poll — re-reading the
  entire retained change table each run (at-least-*everything*) because `open()` never consumed
  the checkpoint LSN. It now resumes from `fn_cdc_increment_lsn(checkpoint)`, guarded so
  nothing-new / nothing-captured skips the call instead of erroring.
- **`fix(mssql)` — `datetimeoffset` was silently dropped in CDC and failed the export in batch.**
  Both paths read it via `try_get::<NaiveDateTime>` — the wrong type for a tz-aware column. CDC
  silently nulled the value; **a batch export errored out entirely on any `datetimeoffset`
  column** (the table was unexportable). Both now read it as the UTC instant and land it as a
  tz-aware Timestamp.
- **`fix(cdc)` — SQL Server `uniqueidentifier` was dropped to NULL in CDC.** It was mapped to its
  36-char string, which doesn't fit the UUID `FixedSizeBinary(16)` column and silently became
  NULL; it now carries the 16 canonical bytes, matching the batch export.
- **`fix(mssql)` — decimal precision now matches batch in CDC and `rivet check`.** The scan-free
  type probe skipped the `sys.columns` catalog hint the full export applies, so a SQL Server
  decimal resolved as the placeholder `DECIMAL(38,…)` instead of the declared `DECIMAL(18,4)`. A
  single shared `resolve_decimal` now serves both paths.

## 0.13.1 (2026-06-23) — pace-aware throttle (wide-table extraction no longer sleeps away wall-clock)

A profiling pass found the default `balanced` profile spending most of a wide-table
extraction's wall-clock asleep in the inter-batch throttle for ~0 source benefit.
This patch makes the throttle pace by rows pulled instead of per batch — output is
byte-identical and the source harm is unchanged, the run just stops idling. Also
picks up a transitive-dependency security advisory.

### Fixed

- **`perf(throttle)` — the inter-batch throttle now paces by rows pulled, not a
  fixed sleep per batch (ADR-0022).** `throttle()` was
  `thread::sleep(throttle_ms)` after every batch, so its cost scaled with the
  batch *count* — which `work_mem` blows up on wide tables (PostgreSQL caps
  `FETCH N` to avoid a spill). On `content_items` (1.93 M wide rows, ~420-row
  FETCHes) the `balanced` 50 ms/batch became **~228 s of sleep — 74 % of
  wall-clock — for ~0 measured source-gentleness** (same `pg_tup_returned` /
  `pg_blks_read`, and a 6× longer MVCC snapshot hold, by rivet's own
  `export_harm`). The sleep is now `throttle_ms × rows_in_batch / configured`
  (in µs), so total pacing is `throttle_ms × total_rows / configured` —
  independent of how `work_mem` split the FETCHes. A **full** configured-size
  batch (narrow tables) still pauses exactly `throttle_ms`, so a narrow-table
  run's pacing is unchanged. Validated live: `content_items` full export on the
  default `balanced` profile **269.9 s → 55.2 s**, **byte-identical output**
  (same content hash), `export_harm` unchanged. PostgreSQL is where the pathology
  lived (work_mem-capped FETCHes); MySQL is a modest ~11 % win, SQL Server
  unaffected (target-MB batches were already ~full size). See
  `docs/bench/reports/REPORT_throttle_vs_harm.md`.

### Security

- **`deps` — bump `quinn-proto` 0.11.14 → 0.11.15 for RUSTSEC-2026-0185**
  (remote memory exhaustion from unbounded out-of-order stream reassembly; high).
  Transitive dependency; no API or behaviour change.

### Internal

- **`refactor(pg)` — the Arrow text-decode reads each cell as a borrowed `&str`,
  not an owned `String`.** Drops a per-value allocation + copy on the row→Arrow
  path (the JSON arm already read zero-copy); output is byte-identical. Profiling
  found this is *not* the wide-table bottleneck (that was the throttle above), so
  this is a cleanup, not a speed-up — kept for the symmetry and reduced allocator
  churn. Adds a `profiling` cargo profile (release codegen + debug symbols).

## 0.13.0 (2026-06-19) — chunked-mode parity, source-safety & source-harm observability

A pilot run surfaced three chunked-mode gaps; this release closes them. Chunked
exports — the default for large tables — now get scan-free range planning (no
pre-chunk `COUNT(*)`), column-level schema-drift detection at parity with single
mode, and a `rivet check` warning when a chunk would hold one statement open too
long. **MINOR** — chunked `on_schema_drift: fail` now aborts runs that
previously sailed through (it never recorded a baseline before).

It also lands the first slice of **source-harm observability**: the same pilot
had no way to quantify what an export actually *cost* its source. Every run now
persists an extended `export_metrics` row plus a per-counter `export_harm` delta
to `.rivet_state.db` — SQL-queryable for analysis, not (yet) surfaced in
`rivet metrics` — and `rivet doctor` flags when a SQL Server login can't read the
harm counters.

### Added

- **`feat(chunked)` — chunked exports now get column-level schema-drift
  detection, at parity with single mode (ADR-0021).** Previously
  `detect_schema_change` / `store_schema` ran only in single mode, so chunked
  exports (the default for large tables) never recorded a schema snapshot —
  `rivet state` showed nothing and drift went unnoticed across re-runs. Drift is
  now checked **pre-chunk**, from a scan-free `type_mappings` schema, so
  `on_schema_drift: fail` aborts **before any chunk is written** (single detects
  post-write; chunks have already written by then, so a post-hoc check could not
  prevent the bad output). All four chunked execution modes (sequential /
  parallel, checkpoint / non-checkpoint) share one helper with single mode.
  Cross-mode caveat (a single→chunked baseline can log a one-time, self-healing
  drift) is documented in ADR-0021.
- **`feat(check)` — `rivet check` warns when `chunk_size` would make one chunk
  query scan too much.** The source-harm lever the 0.12 A/B measured is how long
  a single chunk statement holds its snapshot / locks (SQL Server's longest
  request fell 1839 ms → 276 ms once chunks shrank). `check` now projects
  bytes-per-chunk (`rows_per_chunk × avg_row_bytes`) and, above ~256 MB, advises
  a smaller `chunk_size`. Row width comes free from stats already read — PG
  EXPLAIN `width`, SQL Server `dm_db_partition_stats` — so no extra round-trip;
  MySQL is skipped (no trustworthy scan-free estimate). Advisory only; never
  blocks a run.
- **`feat(metrics)` — every run persists an extended `export_metrics` row for
  post-hoc analysis.** Alongside the original 16 columns, the table now records
  15 more per-run signals: completeness (`files_committed`, `reconciled`,
  `source_count`, `quality_passed`), source cost (`pg_temp_bytes_delta`, and
  `longest_chunk_ms` — how long the single longest chunk held its snapshot /
  locks, the #5 source-harm lever the 0.12 A/B measured), effective tuning
  (`batch_size`, `batch_size_memory_mb`, `chunk_size`, `parallel`,
  `skip_reason`), and run identity (`schema_fingerprint`, `source_type`,
  `destination_type`, `rivet_version`). Written on the `run` and `apply` paths
  and queryable via SQL; `rivet metrics` still prints the core row — these
  columns are for analysis, not the summary card.
- **`feat(metrics)` — per-run source-harm deltas in a new `export_harm` table.**
  Each run brackets the source with an engine-specific counter snapshot and
  stores the delta per counter: PostgreSQL `pg_stat_database` (`pg_tup_returned`
  read-amplification, `pg_blks_read`/`hit`, `pg_temp_files`, `pg_deadlocks`),
  MySQL `SHOW GLOBAL STATUS` (`Innodb_rows_read`, row-lock waits, tmp-disk
  tables, …), SQL Server `sys.dm_os_wait_stats` (`LCK%` wait count + wait-ms).
  Best-effort observability — a failed or unavailable probe is logged and
  ignored, never affecting the run verdict. Captured on the `run` path; `apply`
  persists the metric row but skips the harm + `temp_bytes` probes.
- **`feat(doctor)` — `rivet doctor` advises when a SQL Server login lacks
  `VIEW SERVER STATE`.** The harm probe reads `sys.dm_os_wait_stats`, which needs
  that permission; doctor now prints a `[note]` (never a `[FAIL]`) with the exact
  `GRANT VIEW SERVER STATE` to run if the operator wants lock-wait metrics —
  data extraction is unaffected either way.

### Fixed

- **`perf(chunked)` — chunked range planning no longer runs a full `COUNT(*)` on
  the source before the first chunk.** That count fed *only* the sparsity
  diagnostic log — the chunk boundaries come from `min`/`max`, never the count —
  yet on a 484M-row table it meant **~12 minutes of silence** before any chunk,
  and a full scan on a large production table is exactly the source-harm rivet
  exists to avoid (in the hot path, contradicting the source-safety promise). The
  diagnostic now reads a **scan-free row estimate** for the `table:` shortcut on
  engines with a trustworthy cheap count — PostgreSQL `reltuples` and SQL Server
  `dm_db_partition_stats`. MySQL has none (its `TABLE_ROWS` / `EXPLAIN` figure is
  a ±30-50% random-dive estimate), so the density line is skipped there, as it is
  for any curated query — boundaries are still logged, just without a scan.
  `chunk_dense` is unchanged (its `COUNT(*)` sizes the ordinal chunks). No output
  or boundary change — only the pre-chunk source footprint.

### Changed

- **`feat(init)` — `rivet init` now hints at incremental when it auto-picks
  `chunked` for a re-runnable table.** When a large table gets `mode: chunked`
  but *also* has a cursor column (`updated_at`/`created_at`), the generated YAML
  comment points at `mode: incremental` for scheduled re-runs — chunked re-reads
  the whole table every run (a pilot re-dumped a 655k-row table 4× in two days,
  re-reading ~570k unchanged rows each time). Advisory comment only; the selected
  mode is unchanged.
- **`feat(ux)` — the run summary nudges verification.** A successful run that
  wrote files but ran neither `--validate` nor `--reconcile` now shows an
  advisory `verify: not run — …` line, so skipping completeness checks is a
  deliberate choice rather than an oversight (a pilot loaded hundreds of millions
  of rows across five runs with zero verified). Advisory only.

### Internal

- **`refactor` — architecture deepening over the modules the pilot fixes
  touched (no behaviour change).** Schema-drift became the third runner-write
  facade (`pipeline::schema_drift`: two column-source adapters over one
  detect→policy→store core), and the four chunked Detect arms now share one
  `prepare_chunk_plan` preamble instead of each hand-wiring detect + drift-check
  (ADR-0018 / ADR-0021 updated). The two DB-scalar parsers were unified into a
  `crate::scalar` leaf (the inbound mirror of `crate::sql`), and
  `RunSummary::render` became an ordered manifest of per-row providers — both
  gaining direct unit tests they previously lacked.
- **`refactor(preflight, chunked)` — further deepening over the metrics-touched
  modules (no behaviour change).** `collect_warnings` became a declarative
  manifest (C1), the four chunked Detect arms dedup their plan-build and own the
  detect connection (C3/C4), and the mode-string + base-query were hoisted out of
  the per-engine `diagnose_*` paths (C2). A speculative chunk-range dedup token
  was reverted before shipping — no consumer (#6).
- **`test` — metrics-persistence coverage.** New unit tests pin the
  summary→`MetricRow` builder and the harm-delta math (floor + name
  intersection); live tests prove the extended columns and per-engine
  `export_harm` rows actually land in a freshly-migrated state DB on
  PostgreSQL / MySQL / SQL Server, plus the `apply`-path metric row and the
  doctor `VIEW SERVER STATE` note (restricted login + sysadmin control).

## 0.12.0 (2026-06-14) — memory-driven default batch sizing: MySQL ~7.5×, SQL Server ~6× faster on narrow tables

Sizes the extraction batch to a memory target instead of a static row count, so
narrow tables stop paying a per-batch handoff tax. **MINOR** — the default
`balanced` batching behaviour changes: narrow tables get much larger batches
(higher but bounded peak RSS — see Memory below), wide tables change little.
Source-friendlier too: the source query is held open ~7× less time (verified —
identical server-side scan, same SQL).

### ⚠️ Behaviour change (why this is a MINOR bump)

- **`perf(source)` — the default (`balanced`) batch size is now memory-driven
  (~32 MB per Arrow flush) rather than a static 10,000 rows; `fast` targets
  ~64 MB.** On narrow tables the static 10k pinned the engine to tiny Arrow
  batches (one parquet row-group per 10k rows), so the per-flush handoff
  dominated wall-clock. The memory target sizes the batch to the row width:
  large for narrow tables (clamped to a 150k-row hard-max that bounds peak RSS),
  and *small* for wide ones (≈32 MB ÷ row width ≈ the old static size — a larger
  target overshot medium-wide tables for no flush-count gain). The static
  `batch_size` remains as the no-schema fallback and advisory base.

### Performance

- **Narrow-table throughput** (`bench_narrow`, 10.24M rows, chunked, default
  tuning, rivet-vs-rivet, row-exact verified by an independent parquet re-count):
  - **MySQL ≈ 7.5× faster** (58.7s → 7.7s) — its keyset row-accumulator was
    pinned at 10k.
  - **SQL Server ≈ 6× faster** (62s → 10.5s) — the MSSQL stream loop now seeds
    its batch controller from `effective_batch_size` once the column shape is
    known (the first `tiberius` metadata token, which precedes all rows), so the
    memory-driven ceiling reaches it too. Proven in isolation: the batch-size
    default *alone* moved MSSQL 1.00× (the loop was unwired); the wiring is the
    cause.
  - **PostgreSQL neutral** (≈ 1.0×) — its server-side cursor + `work_mem`-bounded
    `FETCH N` was already efficient; no regression.
  - **Wide tables: no regression.** The ~32 MB target keeps wide batches near
    the old static size (MySQL `content_items` 2M: 80.6s → 71.0s, RSS 126 → 101
    MB — slightly *better* on both). A larger 64 MB target had grown them to
    ~21k rows and cost ~10% — which is why the target is 32, not 64.

### Memory (peak RSS)

The bigger batch *is* the speedup, so narrow-table peak RSS rises — bounded by
the 150k-row cap on the raw-row accumulator (for narrow rows that raw `Vec<Row>`,
not the ~32 MB Arrow batch, dominates RSS, so the row cap is the lever). Measured,
OLD (static 10k) → NEW, `bench_narrow` 10.24M:

| engine | OLD | NEW | note |
|---|---:|---:|---|
| PostgreSQL | 29 MB | 30 MB | flat — `work_mem`-bounded `FETCH N` never grew |
| MySQL | 34 MB | 67 MB | ~2× — bounded by the 150k row cap |
| SQL Server | 32 MB | 89 MB | ~2.8× — heavier `tiberius::Row` accumulator |

The 150k cap is a deliberate speed/RAM point: a 500k cap was ~20% faster on
narrow but pushed SQL Server to 352 MB; 150k keeps most of the win at a third of
the RAM. Wide tables stay flat-to-lower (PG `content_items` 51→57 MB; MySQL
128→101 MB). All well under the multi-GB a single-`SELECT *` client buffer
incurs. Memory-constrained host? `profile: safe`, a lower `batch_size_memory_mb`,
or `memory_threshold_mb` cap it further.

### Internal

- `AdaptiveBatchController::raise_configured_ceiling` — lets an engine that
  discovers its row width mid-stream (SQL Server) raise the batch ceiling once
  the schema is known, so the post-probe memory cap can grow the batch past the
  static `batch_size`. Raises only, one-shot before the cap; PG/MySQL seed the
  ceiling up-front and don't use it.

## 0.11.1 (2026-06-14) — crash-matrix symmetry: no orphaned subprocess children

A reliability fix for the subprocess-export engine (`--parallel-export-processes`)
plus the crash-test coverage that locks it in. **PATCH** — a bug fix + tests + docs,
no contract change.

### Fixed

- **`fix(pipeline)` — a targeted SIGTERM/SIGINT to a `--parallel-export-processes`
  run now reaps its export children** instead of orphaning them. A `kill <pid>`
  from a scheduler / k8s / systemd hit only the parent; the children kept running
  and holding source connections — the exact fragility rivet protects against.
  (Terminal Ctrl-C, which signals the whole process group, already reached them;
  this closes the *targeted*-signal case.) Implemented as a signal-safe child
  reaper (`parallel_children::child_reaper`).

### Internal / tests

- **Crash-matrix symmetry (OPT-6)** across both execution engines (ADR-0010):
  - SIGKILL mid-commit proven to leave no corrupt committed file — temp+rename,
    not `Drop`, carries the no-corrupt-output guarantee under a non-unwinding signal.
  - Subprocess panic recovery across all four write-cycle boundaries (no row loss).
  - `dev/CRASH_MATRIX.md` rewritten with the verified two-engine coverage matrix
    and the panic-vs-signal rationale; residuals (object-store `FinalizeOnClose`,
    single-child external-signal accounting) documented as known non-guarantees.
  - Reusable test-only `RIVET_TEST_BLOCK_AT` / `_MS` fault hook for deterministic
    mid-export crash windows (no-op when unset).

## 0.11.0 (2026-06-13) — new-user UX: friendly errors, guided onboarding, preflight that catches mistakes

A new-user experience pass: the first ten minutes with rivet read as guidance,
not archaeology. `rivet init` scaffolds a *working* config and names the next
command; `rivet check` catches a mistyped table/column **before** any run instead
of passing through to a half-finished export; errors name the failing thing and
suggest the fix; the success card shows where the files landed. Every
instructional GIF is re-recorded against the new behaviour, plus three new ones
showing rivet catching mistakes. **MINOR** bump — the `rivet check` exit-code
change below is behaviour-affecting.

### ⚠️ Behaviour change (why this is a MINOR bump)

- **`feat(ux/check)` — `rivet check` now FAILS (exit non-zero) on a query against
  a table/column that doesn't exist**, instead of passing with exit `0` and only
  failing at run time. A permanent, author-fixable schema error (PG SQLSTATE
  class 42 / MySQL 1146·1054·1142·1064 / MSSQL 208·207) is caught at preflight;
  operational/transient errors stay fail-soft. **CI that asserted `check` exits
  `0` on such a config must be updated.**
- **`feat(ux/run)` — a failed `rivet run` against a closed port fails fast** (one
  connect attempt) instead of ~14 s of retry spam. Refused / no-route / DNS are
  treated as dead endpoints; resets and timeouts still retry.
- **`feat(ux/init)` — local exports scaffold a per-export subdir**
  (`./output/<table>/`) so multi-table configs don't collide in one folder (and
  the double-count `_SUCCESS` WARN is gone).

### Added

- **`feat(ux/run)` — the success card shows the destination** (`output: file://…`)
  so you can see where the files landed without guessing.
- **`docs` — three error/recovery GIFs + a "When something is wrong" section** in
  getting-started: `check` catching a missing table, a config-field "did you
  mean", and `doctor` reporting an unreachable source.

### Changed

- **`feat(ux/init)` — scaffolds a *working* connection per source provenance**
  (`url_env` / `url_file` / inline) and ends with an on-voice next-steps ladder
  (`doctor` → `check` → `run`). Credentials are never written into the file.
- **`feat(ux/check)` — friendlier preflight**: a plain access-path line (no raw
  EXPLAIN dump), a verdict legend, a next-step pointer, and no false `DEGRADED` on
  small tables.
- **`feat(ux/cli)` — friendlier `--help`** (a getting-started map; internal
  Epic/Step labels dropped) and an `init` hint when `-c` is missing.
- **`feat(ux/errors)` — config errors point at the fix**: corrected "did you mean"
  ranking (Levenshtein-first, so `export` → `exports`), missing-field hints, and a
  TAB-indentation hint.
- **`feat(ux/run)` — a failed run carries doctor's connect-error category +
  remediation hint** (auth / TLS / connectivity), bridged via `.context()` so the
  typed cause and the retry-classifier substrings stay intact underneath.

### Fixed

- **`fix(ux/init)` — the scaffold's local-dest note fits 80 columns** (was
  wrapping mid-word in narrow terminals / GIFs).
- **`docs(getting-started)` — the example summary card matches real number
  formatting** (`5,432`, `10,000`, `15 MB`).

### Internal

- **`refactor(preflight)` — the schema-error shape is named**
  (`PreflightSchemaError`, the ADR-0015 data-shape-seam pattern): one message
  template, three per-engine mappers, so the actionable sentence can no longer
  drift across PG / MySQL / MSSQL. Live-verified on all three engines.
- **`chore(deps)` — postgres-protocol 0.6.12 / tokio-postgres 0.7.18**
  (RUSTSEC-2026-0178 / 0179 / 0180).
- **`docs` — all instructional GIFs re-recorded** against the new behaviour
  (`-c` short form, the `output:` row, the friendlier scaffold/check). `doctor-gcs`
  and `pool-detect` are unchanged (need GCS ADC / the `pool` docker profile).

## 0.10.0 (2026-06-11) — machine-actionable exit codes + externally-proven correctness

Adds a stable process-exit-code taxonomy (the headline, contract-affecting
change), hardens a pre-allocation OOM vector, labels SQL Server **beta** with an
honest CVE disclosure, and lands a trust program that proves data correctness
with an **independent reader** (DuckDB) — including after a crash — rather than
trusting rivet's own counters. A data-correctness audit (10 adversarially-verified
candidates) found **zero live data-loss bugs**; the work below mostly *locks* that
result against future regressions.

### ⚠️ Behaviour change (why this is a MINOR bump)

- **`feat(error)` — process exit codes now encode the failure class.** A failed
  run used to exit `1` for everything. Now an unattended scheduler can branch on
  the code without grepping stderr:
  - `1` generic / config / usage
  - `2` retryable — transient (connection reset, lock-wait/timeout, S3 5xx); safe
    to re-run the same command
  - `3` data-integrity — quality gate, **reconcile mismatch**, **`validate`
    verification failure**, duplicate-guard, manifest inconsistency; **STOP**, do
    not blindly retry
  - `4` schema-drift — `on_schema_drift: fail` tripped; needs human review

  `--json-errors` gains an `"exit_class"` field. Classification is **type-driven**
  (typed markers downcast through the anyhow chain), not string-matched: a reworded
  message can never silently flip the code. **A script that asserted "failure ==
  exit 1" must be updated.** (Note: clap's own usage-error exit `2` overlaps
  numerically with Retryable but is distinguishable — clap exits pre-dispatch with
  no `Error:` line.)

### Added

- **`feat(correctness)` — `dev/correctness/verify_export.py`.** Prove an export is
  faithful *on your own data*, independent of rivet's bookkeeping: it compares a
  content fingerprint of the source query against the exported Parquet, both
  computed by DuckDB (source via its Postgres/MySQL scanner). Exit `0`=PASS /
  `1`=FAIL, so it drops into `rivet run && verify_export && deploy`. Single dep
  (`pip install duckdb`). Recipe: `docs/recipes/verify-your-export.md`.
- **`test` — Azure Blob (Azurite) live-test infrastructure** (`azurite` service in
  `docker-compose.yaml`, `LiveService::Azurite`, container-provisioning helper).

### Security / hardening

- **`feat(source)` — per-value byte ceiling enforced *before* Arrow allocation.**
  An oversized cell was previously only rejected after materialization (the Arrow
  buffer was already reserved/filled); a single hostile/corrupt huge value could
  OOM the builder first. Now `value_within_ceiling` bails with `RIVET_VALUE_TOO_LARGE`
  (naming the column) before allocation, in every engine. Defense-in-depth with the
  existing post-materialization sink guard.
- **`docs` — SQL Server marked Beta + procurement advisory.** `tiberius` 0.12 pins
  `rustls` 0.21 → `rustls-webpki` 0.101 (RUSTSEC-2026-0098/0099/0104). No newer
  `tiberius` exists. The path is reachable **only** under `tls.mode: verify-ca|
  verify-full` against a name-constraint-asserting private CA on MSSQL; strict
  validation emits a one-time WARN; advisories are documented + suppressed in
  `.cargo/audit.toml`. Security-sensitive deployments should pin to PG/MySQL.

### Fixed

- **`fix(error)` — reconcile mismatch + `validate` verification failure now
  classify as data-integrity (exit 3).** They previously bailed with bare strings
  and exited `1`, contradicting the documented exit-code table. ("Could not verify"
  — reconcile `unknown` partitions, validate operational read errors — stays
  generic, not 3.)

### Trust program (correctness, externally verified)

- **`test(differential)` — independent-reader correctness at scale.** DuckDB
  fingerprints the exported Parquet and must agree with the source DB field-for-
  field (count, sums, distinct), neither side reading rivet's counters. Covers the
  multi-part chunked write path (20k rows → 5 files), **and a post-crash variant**:
  crash → resume → DuckDB's exactly-once (deduped) view still equals the source.
- **`test(crash-soak)` — no fault point loses a row.** Sweeps the crash matrix; after
  `--resume`, the destination (re-read independently) holds every source id at least
  once, at-least-once being the only allowed surplus.
- **`test` — closed the systemic self-oracle blind spot (31 tests).** The
  recovery/resume/chunked/cli/plan completeness suites trusted rivet's own
  `file_log`/summary/file-count counters; **every completeness test now re-reads the
  destination row content** — across PostgreSQL, MySQL, and SQL Server. A regression
  that drops/duplicates rows while keeping rivet's counters self-consistent (the
  shape of the original rotated-part loss) can no longer ship green.
- **`docs` — documented the dense-chunking tied-column ordering hazard.** `chunk_dense`
  on a non-unique, concurrently-written `chunk_column` could in principle map an
  ordinal to a different physical row across independent per-chunk queries. Proven
  not to occur on a static table on any tested engine; documented in
  `semantics.md` + `modes/chunked.md` next to the analogous incremental-cursor-tie,
  guarded by `tests/live_chunked_dense.rs`.

### Internal

- **`refactor` — de-duplicated the per-value ceiling** (one `SourceTuning::max_value_bytes()`
  + one shared `source::value_within_ceiling`, was copy-pasted across the sink and
  three engines) and the **test Parquet read-back boilerplate** (shared
  `tests/common/parquet.rs` primitives + `XTable::adopt` guards).

## 0.9.5 (2026-06-09) — hot-path perf + benchmark-driven strategy corrections

Squeezes the row→Arrow→Parquet decode path and turns a three-engine A/B
benchmark (full vs chunked-parallel, on the same 2 M-wide and 10 M-narrow
tables, with perf **and** DBA-harm signals) into concrete, measured fixes. No
public-API breaks.

### Performance

- **`perf(mysql)` — SIMD-validate text columns on decode.** The MySQL
  row→Arrow text-column append used `String::from_utf8_lossy` (scalar
  validation) while every other text path in the file — and the whole Postgres
  decoder — already used the `simdutf8` helper. Switched the high-volume path
  to a simdutf8 fast path with a lossy fallback: **2.33× faster UTF-8
  validation** on wide text (criterion `mysql_utf8_text_append`), byte-identical
  output. (Rides on this line's earlier `opt-level = 3` and on-by-default write
  pipelining.)

### Fixed

- **`fix(retry)` — a statement-duration timeout is no longer retried.** PG
  `statement_timeout` (57014), MySQL `max_execution_time`, and the MSSQL
  client-side cap were classed transient, so an unchunked full-scan that
  exceeds the budget re-failed identically up to 3× — measured at **20 m 22 s
  for 0 rows** on a 2 M-row MSSQL full export. Now classified permanent (lock-
  wait / network timeouts stay transient) and the MSSQL message says to use
  `mode: chunked` or raise the budget. Same export now fails in **5 m 20 s
  (one attempt)**. Regression tests added.

### Changed

- **`feat(init)` — strategy defaults are cost-, engine-, and memory-aware.**
  The A/B benchmark showed "parallel = faster" is false in general: a win on
  Postgres (2.1× + ~5× less VACUUM-horizon bloat), a regression on wide MySQL
  (already a fast sequential scan), and mandatory on SQL Server (a full
  statement times out). `suggest_parallel` now factors introspected
  `avg_row_bytes` and the engine (wide MySQL stays single-threaded), and emits
  the **predicted peak RSS** as a comment — a memory sweep established that peak
  RSS scales with `batch_size` × row width × worker count, **not** `chunk_size`.

### Docs

- **`docs(bench)` — `REPORT_full_vs_parallel.md`**: the full three-engine A/B
  (perf + DBA-harm: longest open txn, PG xmin-horizon age, peak connections),
  the narrow-vs-wide reversal, the memory-footprint sweep, and the six
  corrections it justifies.

## 0.9.4 (2026-06-08) — source-engine parity (Epic 18): MySQL/SQL Server → PostgreSQL gold standard

Closes the per-engine gap between MySQL / SQL Server and the PostgreSQL
reference on the axes a DBA actually feels — keyset coverage, pooler-safe
session state, and the adaptive governor's pressure signal — driven measure-
first, so the most expensive idea (a MySQL server-side cursor) was disproven
with evidence before any code was forked. No public-API breaks.

### Added

- **`feat(mssql)` — keyset (seek) pagination on any single-column unique NOT
  NULL index, not just the PK** (parity with PostgreSQL/MySQL, which already
  did). Tables with a surrogate/business unique key (and a composite or non-int
  PK) now take the safer keyset path instead of falling back to the less-safe
  chunked path, and `chunk_by_key: <unique non-PK col>` is accepted.

### Fixed

- **`fix(mysql)` / `fix(mssql)` — pooler-safe session teardown.** Both drivers
  now reset the per-connection session state they mutate (`time_zone` +
  `max_execution_time` on MySQL; `LOCK_TIMEOUT` on SQL Server) on a `Drop` path,
  so a connection returned to the pool — or reused behind ProxySQL / MaxScale /
  an Azure SQL gateway — can't carry our settings into the next checkout, even
  on a panic or early return. MySQL gains a `MysqlSessionGuard` (analogue of
  `PgTxnGuard`); SQL Server gains a `Drop` reset (best-effort, time-boxed). This
  also closes a MariaDB-specific leak (it spells the timeout `max_statement_time`,
  so the old end-of-function reset's `?` could skip the `time_zone` reset).

### Changed

- **`feat(governor)` — MySQL/SQL Server adaptive governor now samples
  extraction pressure, not write pressure.** The old `Innodb_log_waits` /
  `Log Flush Waits` signals are redo/log **write** pressure that barely moves
  during a read-only export. Replaced with the PostgreSQL `temp_bytes` analogue:
  MySQL sums `Created_tmp_disk_tables` + `Innodb_buffer_pool_wait_free` (sort
  spill + buffer-pool memory pressure); SQL Server sums cumulative
  `Workfiles Created` + `Worktables Created` (sort/hash spill to tempdb). Both
  monotonic, so the governor's `cur > prev` contract is unchanged.
- **`docs(tuning)` — `chunk_size` documented as the statement-duration lever.**
  Lowering `chunk_size` is the supported, measured way to keep each MySQL/SQL
  Server read short under a strict `statement_timeout` (one statement per chunk),
  with the trade-off (more files, ~25% throughput, not a speed win) stated
  honestly. A MySQL server-side cursor was investigated and rejected with a
  `libmysqlclient` measurement (`dev/spikes/mysql_cursor_efficacy.c`): its
  read-only cursor materialises the result into temp tables at open, so it would
  be a regression vs lowering `chunk_size`, not an improvement.

## 0.9.3 (2026-06-07) — correctness & safety pass: data-loss guard, credential redaction, nanosecond timestamps

A focused review pass (structural → domain-correctness → security) that found
and closed four genuinely-broken things — two data-correctness, one security,
one precision — and added an opt-in high-precision timestamp. No public-API
breaks; the one runtime behaviour change (chunked NULL-key) converts a silent
data loss into a clear error.

### Fixed

- **`fix(chunked)` — chunked export silently dropped rows with a NULL chunk
  column.** Range chunking emits `WHERE col BETWEEN min AND max` and date
  chunking `>= start AND < end`; both exclude NULL, so a row whose
  `chunk_column` was NULL fell into no chunk and vanished from the export with
  no signal (the keyset path already refused a nullable key; the range path did
  not). Detection now runs one `COUNT(*) - COUNT(col)` and **bails** with
  remediation (NOT NULL column / `WHERE col IS NOT NULL` / `chunk_dense` /
  `mode: full`) rather than dropping rows. Detected on the live data, so a
  nullable column with zero actual NULLs is unaffected.
- **`fix(redact)` — credentials could leak to stderr via the log sink.** The
  redaction module names "logs" in its scope, but enforcement was wired only at
  the error/summary call sites; the `env_logger` sink was bare, so a
  `log::warn!("…{e}", e)` whose error captured a `scheme://user:password@host`
  connect string printed the password (and the default filter is `warn`, so
  those lines show). The sink now routes every record through the redaction
  chokepoint — no call site has to remember, present and future.

### Added

- **`feat(types)` — opt-in nanosecond timestamps (`timestamp_ns` /
  `timestamp_tz_ns` column overrides).** Preserves a source's sub-microsecond
  fractional seconds — notably SQL Server `datetime2(7)`'s 100 ns tick that the
  default microsecond mapping truncates. The default stays microsecond on
  purpose: Arrow nanosecond timestamps span only 1677–2262, so a blanket ns
  mapping would corrupt out-of-range dates — the opt-in keeps the full-range
  default and lets in-range columns carry the extra precision. Per-target
  autoload verified live: DuckDB → native `TIMESTAMP_NS` (lossless); Snowflake →
  `NUMBER`, `TO_TIMESTAMP_NTZ(col, 9)` recovers losslessly; BigQuery → `INT64`
  raw nanos (a native `TIMESTAMP` cast is lossy µs). The incremental cursor
  carries the full 9-digit value, fixing the `datetime2(7)` boundary re-export.

### Changed

- **`docs(incremental)` — documented the strict-`>` cursor tie hazard.**
  Incremental resume uses `WHERE cursor > last_value`; rows that tie on the
  high-watermark value and arrive after it is passed are skipped. This is
  inherent high-watermark semantics (keyset is unaffected — its key is unique),
  now stated where keyset's uniqueness contract already lives (semantics.md
  "Known non-guarantees", config.md, the query builder).
- **`refactor(config)`** — split the 379-line `Config::validate` into three
  cohesive, independently-testable validators (exports-list / source-connection
  / per-export). Behaviour-preserving; all 27 validation/secops tests green.
- **`refactor(pipeline)`** — deleted `src/pipeline/child.rs`, a 461-line orphan
  fork never wired into the module tree (so never compiled, its tests never
  ran); its byte-identical `render_child_stderr` tests moved onto the live
  `parallel_children.rs`. Gave the parallel chunk drivers' mutex
  poison-recovery decision a single documented home (`chunked/poison.rs`).
- **`test(csv)`** — pinned the float `NaN`/`±Infinity` CSV contract (emit the
  literal, never an empty cell that would conflate with NULL) with a
  characterization test + docs; verified correct-by-design.

### Dependency policy

- **`ci(deps)`** — Dependabot retuned to cut churn: routine crate updates are
  scoped to **major** bumps only, **grouped** into one weekly PR, and held by a
  **14-day cooldown** so a freshly shipped `.0` never lands before its first
  round of fixes. GitHub Actions bumps are likewise grouped + cooled down.
  Security PRs still arrive out-of-band (no cooldown), and the `cargo audit`
  gate (pre-commit + CI) remains the hard backstop for any fixable advisory.

## 0.9.2 (2026-06-07) — SQL Server engine maturity + shared batch controller

> The SQL Server source grows up: the export **streams** (peak RSS bounded by
> `batch_size`, not the chunk — `mode: full` on a 2 M-row heavy table went from a
> ~10 GB OOM to 1 file at 171 MB), **detects connection poolers / the Azure SQL
> gateway**, honours `lock_timeout` / `statement_timeout` / `throttle_ms`, and is
> covered by full live parity suites (resume / chunked-recovery / crash-recovery
> / reconcile-repair) plus a type matrix round-tripped through DuckDB, ClickHouse
> **and** live BigQuery. Internally, the probe → memory-cap → adaptive → throttle
> batch policy that was triplicated across the PG / MySQL / MSSQL export loops is
> now one unit-tested `AdaptiveBatchController` — PG/MySQL fully revalidated, no
> behaviour change.

### SQL Server — streaming export + pooler detection + parity test suites

- **`refactor(source)`** — the probe → memory-cap → adaptive-resize → throttle
  batch-sizing policy, previously triplicated across the PG / MySQL / SQL Server
  export loops, is now one unit-tested `AdaptiveBatchController`. Engines provide
  only what differs (row source + memory-cap formula). SQL Server gains the same
  first-batch memory-cap probe PG/MySQL have. No behaviour change — full live
  validation across all three engines (parity, recovery, type matrices).
- **`feat(mssql)`** — the export now honours the source-safety `SourceTuning`
  knobs it previously ignored (it read only `batch_size`): **`lock_timeout`**
  (server-side `SET LOCK_TIMEOUT` so a blocked read fails fast), **`statement_timeout`**
  (client-side wall-clock budget — SQL Server has no statement-duration `SET`;
  live-verified to abort + retry), and **`throttle_ms`** (sleep between batches).
  Brings MSSQL to in-export tuning parity with the PG/MySQL engines.
- **`feat(mssql)`** — the export now **streams**: it consumes the `tiberius`
  result set incrementally and emits one Arrow batch per `tuning.batch_size`
  rows instead of materialising the whole chunk (`into_first_result`). Peak RSS
  is bounded by `batch_size × row_bytes`, **independent of `chunk_size`** — the
  SQL Server analogue of the PG cursor's `FETCH N`. So a large `chunk_size` (or
  `mode: full`) gives few large files at low memory; `chunk_size` now controls
  file count, `batch_size` controls memory. Measured on 2 M heavy rows:
  `mode: full` went from a ~10 GB materialise (OOM) to **1 file at 171 MB**.
- **`feat(mssql)`** — connection pooler / gateway detection (`MssqlProxyKind`):
  `@@SPID` drift across two queries → transaction-mode multiplexer (`Multiplexed`);
  `SERVERPROPERTY('EngineEdition')` 5/8 (or an Azure `@@VERSION` banner) →
  `AzureGateway`. One connect-time warning, mirroring PG (`pg_backend_pid` drift)
  and MySQL (`CONNECTION_ID()` drift). Pure classifier exhaustively unit-tested;
  live direct-connection guard in `live_pool_safety`.
- **`test(mssql)`** — full live parity suites mirroring the PG/MySQL twins:
  `live_mssql_resume`, `live_mssql_chunked_recovery`, `live_mssql_crash_recovery`,
  `live_mssql_reconcile_repair`, `live_mssql_chunked`. Wired into the per-PR
  `e2e` job and Nightly (mssql service + seed step added to both).
- **`docs(mssql)`** — `datetime2` sub-microsecond truncation documented as a
  tracked gap: rivet maps timestamps to microsecond, so a bare `datetime2`
  (precision 7 = 100 ns) incremental cursor lands one tick below the source max
  and re-exports the boundary row each run — use `datetime2(6)` or coarser.
  Reliability / type-mapping / tuning matrices gained SQL Server rows.
- **`docs(mssql)`** — [Gentle SQL Server extraction](docs/best-practices/mssql-gentle-extraction.md)
  best-practice + copy-paste config (`rivet_mssql_gentle.yaml`): how to stay easy
  on both the source DB and the rivet worker. Documents the one MSSQL footgun —
  use an explicit `chunk_size` (rows), **not** `chunk_size_memory_mb` (which can't
  size by bytes yet on MSSQL and falls back to ~500k-row chunks, so wide rows
  buffer multiple GB). Measured live on a 2 M-row heavy table: **2 759 MB →
  101 MB peak RSS (~27×) at the same wall time** by switching to `chunk_size`.
  Includes a row-width sizing table and the `environment: production` governor lever.
- **`bench(mssql)`** — DBA-harm matrix for SQL Server
  ([`REPORT_mssql.md`](docs/bench/reports/REPORT_mssql.md) +
  `mssql_db_bench.sh`): measured against live SQL Server 2022, rivet's chunked
  autocommit reads hold **no long transaction** (0 ms), pin **nothing** back
  from log truncation, add **zero** write pressure (read-only), and take a
  **3–4 lock** peak footprint. A **per-tool comparison** (`mssql_harm_compare.sh`,
  2 M-row `content_items`) puts that next to the competitors: rivet's longest
  single query is **6.6 s** vs **~9 min** for sling/dlt scanning the table in one
  shot, and rivet holds **no** open transaction where **dlt holds one for ~8 min**
  (version-store / log-truncation hazard).
- **`bench(mssql)`** — competitive performance matrix
  ([`REPORT_mssql.md`](docs/bench/reports/REPORT_mssql.md)) vs sling / dlt on
  live SQL Server 2022: rivet wins throughput on narrow-to-medium tables
  (sub-second, 3–30× faster) and — with the streaming export — holds the lowest
  or competitive peak RSS on every table, including the wide ones. The one
  remaining gap is wall-time on heavy-text rows (the `tiberius` row decode), not
  memory.
- **`test(mssql)`** — `bigquery_validates_mssql_type_matrix_parquet`: the SQL
  Server type matrix now also round-trips through live BigQuery (autoload types,
  microsecond TIME/TIMESTAMP, `uniqueidentifier`→BYTES, decimal sums). **All
  three type matrices (PG / MySQL / SQL Server) now pass through every oracle —
  DuckDB, ClickHouse, and live BigQuery.**

### Upgrade notes

- **No config or API changes.** Existing PostgreSQL / MySQL / SQL Server exports
  are unaffected; the `AdaptiveBatchController` refactor is internal and fully
  re-validated on all three engines.
- **SQL Server chunked exports open a fresh connection per chunk** (as the PG and
  MySQL engines do) and run the one-time pooler / gateway detection on each. On a
  many-chunk export that is real connection + auth churn — prefer a larger
  `chunk_size` (fewer, larger files; the streaming export keeps memory bounded
  regardless) over many small chunks. See
  [gentle SQL Server extraction](docs/best-practices/mssql-gentle-extraction.md).

## 0.9.1 (2026-06-06) — SQL Server Source Engine

> Rivet gains a third source engine: **SQL Server (MSSQL)**. Point it at a
> `sqlserver://` URL and it extracts the same way it does for PostgreSQL/MySQL —
> snapshot, incremental, chunked (range + dense), and **keyset (seek)** for
> non-integer PKs — into Parquet/CSV on local/S3/GCS/Azure. Every type and mode
> is live-verified against SQL Server 2022 and round-tripped through the same
> DuckDB / ClickHouse / BigQuery oracles that gate PostgreSQL and MySQL, so a
> `decimal(18,2)`, a `uniqueidentifier`, or a `datetime2` lands byte-exact
> downstream. No change for existing PG/MySQL exports.

### SQL Server source

- **`feat(mssql)`** — new `source.type: mssql` engine on the async `tiberius`
  driver, bridged to the sync `Source` trait via a per-source tokio runtime
  (ADR-0011 keeps `Source` sync). Routed through every shared seam: identifier
  quoting `[col]`, cursor literal `N'…'`, `sys.*` chunk-planning introspection,
  preflight/doctor, and the `OPT-2` back-pressure governor (`Log Flush Waits`
  perfmon counter — the SQL Server analog of MySQL `Innodb_log_waits`).
- **`feat(mssql)`** — export modes: `full` / snapshot, `incremental`, `chunked`
  (range + dense), and **keyset (seek)** via `chunk_by_key` for non-integer PKs
  (UUID / string). The keyset page builder emits the T-SQL
  `OFFSET 0 ROWS FETCH NEXT n ROWS ONLY` (T-SQL has no `LIMIT`).
- **`feat(mssql)`** — type matrix → Arrow/Parquet, live-validated: int family,
  `bit`, `decimal`/`numeric` (scale recovered from the data, since `tiberius`
  drops declared precision), `real`/`float`, `money`, `date`, `time`,
  `datetime2`, `nvarchar`/`varchar`/`char`, `varbinary`, and `uniqueidentifier`
  → native Parquet `LogicalType::Uuid`. Unmapped types fail loud.
- **`test(mssql)`** — `{duckdb,clickhouse}_validates_mssql_type_matrix_parquet`
  added to the type-roundtrip harness, the same oracle pattern as PG/MySQL; CI
  `test-type-validators` provisions and seeds the `mssql` service.

### Security & maintenance

- **`ci(audit)`** — `cargo audit` is now a gate in both the `.githooks/pre-commit`
  hook and CI, reading one accepted-advisory list in `.cargo/audit.toml`. The
  MSSQL driver's unavoidable `tiberius → rustls 0.21` advisories are documented
  there with a reachability analysis (operator-specified server, pinned CA /
  `trust_cert`, no CRL checking); the gate still fails on every *fixable* vuln.
- **`ci(deps)`** — added Dependabot for weekly grouped Rust + GitHub-Actions
  updates, so security patches flow in automatically and the accepted advisories
  retire themselves the day upstream ships a fix.

### Docs

- README / getting-started / config / compatibility now list SQL Server as a
  source, with a worked `sqlserver://` example, the supported mode/type scope,
  and the TLS-on-handshake note.

## 0.9.0 (2026-06-04) — Value-Based Output Partitioning

> **`partition_by`** splits one export's rows into one destination sub-folder
> per value bucket of a date column — the Hive `col=value/` layout that
> Snowflake external tables, BigQuery, Spark, DuckDB, and Athena discover
> automatically. Verified end-to-end on live PostgreSQL and on object storage
> (MinIO + GCS) with both `--reconcile` (per-partition source `COUNT(*)` ==
> exported) and per-partition `--validate` (manifest + `_SUCCESS`, content-MD5
> with no download — confirmed against live GCS).

### Features

- **`feat(partition)`** — new per-export `partition_by:` (+ `partition_granularity:`
  `day` | `month` | `year`). Rivet reads the column's `[min, max]` span, expands
  the export into one child per bucket (`WHERE col >= lo AND col < hi`,
  half-open), and resolves a required `{partition}` destination token to
  `col=value`. Each partition is an independent prefix with its own
  `manifest.json` + `_SUCCESS`, so `--validate` and downstream consumers work
  per-partition with no new machinery. Orthogonal to `mode`: `mode: chunked`
  chunks *within* a partition. See [docs/partitioning.md](docs/partitioning.md).
- **`feat(partition)`** — rows with a **NULL** partition value land in
  `col=__HIVE_DEFAULT_PARTITION__/` (Hive default-partition convention) so no
  row is silently dropped; the NULL bucket is queryable by Hive/Spark/DuckDB
  partition discovery.

### Notes

- `partition_by` is rejected without a `{partition}` token in
  `destination.path`/`prefix` (every partition would otherwise overwrite the
  same prefix), and is not compatible with `mode: time_window`.
- `--parallel-export-processes` is disabled while partitioning is active (child
  processes re-load the config and can't see synthesised partitions); the run
  executes in-process.
- Not compatible with `chunk_by_key` (keyset needs the `table:` shortcut to
  verify the index; partitioning rewrites the export into a `query:` subquery) —
  rejected up front. `plan` / `check` do not expand partitions yet (they show the
  parent as one un-partitioned job).
- **Per-partition chunking trade-offs (source load, not correctness):** with
  `mode: chunked`, chunk windows come from the partition's key `[min,max]`, not
  its row count. A range key that is *dense/correlated* within the partition is
  one clean pass; a *sparse* key amplifies queries/I/O; `chunk_dense` is
  `O(chunks × rows)` (fine for small partitions, not for huge ones); `mode: full`
  is a long PG transaction / MySQL client-side OOM at scale. Row counts stay
  exact in every case. See [docs/partitioning.md](docs/partitioning.md).
- Validating every partition of an export by its parent name in one command is
  not yet wired — point `validate --prefix` at a concrete partition prefix.

### Internal

- **`refactor(test)`** — the three hand-written full-field `ExportConfig` test
  fixtures are consolidated into one `config::sample_export()`; adding a config
  field is now a single-site edit.
- **`refactor(sql)`** — dialect-aware SQL string helpers shared across layers
  (`parse_date_flexible`, `strip_select_star_from`, `aggregate_sql`) now live in
  the `sql` leaf module instead of being duplicated between `pipeline::chunked`,
  `preflight`, and `plan::partition`. Behaviour-preserving; the `table:` fast
  path now also applies to partition min/max probes.

## 0.8.1 (2026-06-03) — Check↔Run Consistency + Trust-Gate Fixes

> A correctness pass over the export pipeline: wherever `check` (or a contract,
> or a doc) promised one thing and `run` did another, they now agree. Every fix
> was reproduced on live PostgreSQL/MySQL + DuckDB/ClickHouse/BigQuery and is
> closed by a test. Plus a full reality-check of the ADRs and the CLI/config
> references against the code.
>
> **Two operator-visible behaviour changes** (both fixes restoring the intended
> contract): `rivet reconcile` now **exits non-zero on a detected mismatch** (an
> `unknown` partition warns but does not fail) so CI/orchestrators can gate on
> it, and `run --reconcile` again **implies `--validate`** (ADR-0013). Everything
> else is additive or internal.

### Correctness — `check` ↔ `run`

- **`fix(chunked)`** — chunked / dense / keyset exports of a PostgreSQL
  `NUMERIC(p,s)` column now resolve precision/scale correctly. These modes wrap
  the query in `SELECT * FROM (base) WHERE …`, which hid the source table from
  the catalog-hint parser (PG omits NUMERIC precision from the wire), so the
  driver now resolves hints from the unwrapped base query.
- **`fix(time_window)`** — same NUMERIC resolution for `time_window` (its
  `resolve_query` wraps too).
- **`fix(types)`** — a list of an unsupported element type is itself reported
  `Unsupported` in `check`, matching what `run` does — no more `numeric[]`
  showing "compatible" in preflight then failing the run.
- **`fix(check)`** — `check --type-report --strict` is now CSV-format-aware: a
  column CSV cannot serialize (e.g. `int[]`) is flagged at preflight using the
  exact predicate the CSV writer uses, instead of being called safe and then
  failing the run.

### Trust & exit codes

- **`fix(reconcile)`** — `reconcile` exits non-zero on a detected mismatch (the
  audit gate now gates; `unknown` warns).
- **`fix(run)`** — `--reconcile` implies `--validate` again (ADR-0013
  subsumption restored).
- **`fix(repair)`** — collision-proof chunk filenames (random nonce) so a
  `repair --execute` re-export can never overwrite the original part on a
  sub-second filename collision.

### Docs & `--help`

- **`docs(adr)`** — reality-checked all 20 ADRs against the code. Fixed real
  drift: ADR-0012 (an in-code comment claimed M9 quarantine-move "not wired"
  while the file implements and calls it), ADR-0002 (module-visibility table
  out of sync with `lib.rs`), ADR-0014 (`uuid` listed as Arrow `Utf8`; it is
  `FixedSizeBinary(16)` + `arrow.uuid` / native `LogicalType::Uuid`).
- **`docs(reference)`** — documented the `validate` and `schema` subcommands and
  `run --force`; added the `chunk_by_key` and `chunk_size_memory_mb` config
  fields.
- **`docs(cli)`** — `rivet --help` now states that `run --reconcile` implies
  `--validate` and that `reconcile` exits non-zero on mismatch.

### Internal

- **`refactor(source)`** — `ExportRequest::{unwrapped, wrapped}` constructors
  replace hand-written struct literals across the runners; a query-wrapping
  runner must supply the unwrapped base, so the NUMERIC-hint regression cannot
  silently return.
- **`test(governor)`** — the deadlock-when-chunks-fail regression now forces the
  failure at the destination-write stage (chunked `NUMERIC(p,s)` resolves and
  succeeds now, so the old "numeric has no safe mapping" premise no longer
  fails any chunk).

## 0.8.0 (2026-06-01) — Type-Support Resolver + No-Download Content Integrity

> Two arcs. **Type support** gains a per-target type resolver (DuckDB,
> BigQuery, Snowflake) that maps every column to its native type, the
> degraded type a generic loader autoloads, and the SQL to recover the
> difference — with Snowflake added and live-verified, BigQuery array/JSON
> recovery, MySQL decimal precision read from the wire, `Decimal256`/`i256`
> past the old ~38-digit ceiling, and CSV failing loud on unsupported
> columns instead of writing silent empties. **Integrity** gains
> **no-download content verification**: each part's MD5, computed once before
> upload, is compared to the checksum the store already exposes in its
> listing (GCS `md5Hash`, S3 single-PUT ETag, Azure single `Put Blob`
> `Content-MD5`) — so `--validate` confirms content, not just size, without
> pulling a byte back. A new per-export `verify: size | content` makes that
> a declarable contract. Verified end-to-end on live GCS, S3, and Azure.
>
> No breaking changes for operators: `verify` defaults to `size`, the new
> `summary.json` fields are additive, and the type pipeline is unchanged for
> existing exports.

### Type support

- **`feat(types)`** — `src/types/target.rs`: a `RivetType` → target resolver.
  `ExportTarget::{DuckDb, BigQuery, Snowflake}` each map a column to a
  `TargetColumnSpec` (native type, autoload type, status, note, recovery
  `cast_sql`). Dispatch keys off the semantic `RivetType`, not the physical
  Arrow type, so `json` / `uuid` / `enum` resolve correctly. Total and
  infallible — an unmappable column is a `status: Fail` row, never an error.
  Per-export `target:` config + `rivet check --type-report --target <t>`
  prints the autoload-vs-native table and a ready-to-run recovery
  `CREATE TABLE … AS SELECT` over the staging table.
- **`feat(types)`** — **Snowflake** target added and **live-verified**
  (`snowflake_validates_*` harness): json→TEXT/`PARSE_JSON`, uuid→BINARY/
  `HEX_ENCODE`+`REGEXP`, naive ts→NUMBER/`TO_TIMESTAMP_NTZ`, time→NUMBER/
  `TIME_FROM_PARTS`, list→VARIANT/`::ARRAY`, plus the `BINARY_AS_TEXT=FALSE`
  load-format requirement.
- **`feat(types)`** — BigQuery type-recovery SQL (L5, post-load CTAS — BQ
  rejects declared native types on load) and array recovery via
  `--parquet_enable_list_inference` + `UNNEST`.
- **`fix(types)`** — `Decimal256` parses straight into `i256`, removing the
  i128 ~38-digit bottleneck (now up to 76 digits).
- **`feat(types)`** — MySQL `DECIMAL` precision/scale read from wire metadata
  (works for any query, unlike PG's catalog-only path).
- **`fix(csv)`** — CSV fails loud on array / unsupported columns instead of
  writing a silent empty value; `uuid: string` and similar overrides honoured.

### Integrity — no-download content verification

- **`feat(validate)`** — `--validate` confirms each part's **content** by
  comparing its manifest MD5 to the checksum the store surfaces in object
  listings — **no download**. Encodings are normalised to raw digest bytes so
  GCS base64 `md5Hash` and S3 hex ETag of the same content compare equal; an
  S3 multipart composite ETag or a checksum-less object degrades to size-only.
- **`feat(destination)`** — small parts upload as a single PUT
  (`op.write`) so the store computes and stores a content checksum the listing
  exposes; this is the only way to get a `Content-MD5` on **Azure** (a single
  `Put Blob`, never `Put Block List`). A process-wide byte budget bounds the
  RAM held in one-shot buffers regardless of upload concurrency; larger parts
  stream (size-only).
- **`feat(destination)`** — `Destination::write` returns `WriteOutcome`
  carrying the store's upload-response checksum; the commit path compares it to
  the locally computed MD5 for a fail-fast, no-round-trip transit-integrity
  check (catches a part corrupted in flight at write time).
- **`feat(validate)`** — per-export **`verify: size | content`**. `content`
  fails validation for any part only size-verified, with an actionable message
  (lower `max_file_size` so parts fit a single PUT, or the backend exposes no
  checksum). The run report and `rivet validate` show coverage explicitly:
  `N verified (M md5, K size-only)`.

### Architecture — verification seam

- **`refactor(pipeline)`** — one pure `reconcile_manifest_against_listing`
  (manifest × destination listing) with two thin consumers: destination verify
  (`Presence → Failure`) and chunked resume (`Presence → ResumeDecision`).
  Replaces two near-identical walks; destination verify now derives part
  presence from a single `list_prefix` instead of per-part HEADs.
- **`refactor(validate)`** — `ManifestVerification.passed` is derived
  (`manifest_found` and no *fatal* failure — advisory `UntrackedObject` does
  not count) in one place, so a new failure variant is fatal by default rather
  than relying on every site to flip a bool. Per-run `parts_md5_verified`
  reports content coverage. The single-variant `IntegrityLevel` (a no-op after
  re-download verification was rejected) was removed.
- **`refactor(pipeline)`** — part xxh3 fingerprint + MD5 computed in a single
  streamed read (`compute_part_checksums`).

### Live verification

- Type fidelity re-confirmed on DuckDB, ClickHouse, **BigQuery**, and
  **Snowflake** after the integrity changes. Content verification + transit
  check + `verify` policy verified end-to-end on live **GCS**, **AWS S3**
  (eu-north-1), and **Azure** buckets, including manifest-tamper detection.

## 0.7.9 (2026-05-30) — Optimization Backlog + Seam Consolidation + CI Invariant Gates

> Closes the §10 optimization backlog (OPT-1…OPT-7), proves Parquet type
> fidelity end-to-end with four independent readers (DuckDB, ClickHouse,
> pyarrow, BigQuery) with native logical types for UUID/JSON, and
> consolidates the per-runner commit + post-finalize state-write paths
> behind two shared seams (`commit::record_part` for the per-part write
> ordering, `RunStore` for the cursor + progression tail) so the
> ADR-0001 / ADR-0008 ordering invariants live in interfaces rather
> than in per-runner conventions. Adds CI debug-build invariant gates
> that catch the next M1-shape bug at finalize time. Six new ADRs
> (0015–0020) document the architectural decisions made along the way
> and the deferred work (nullability propagation, PG UUID-PK
> auto-keyset).

### Architecture — seam consolidation

- **`refactor(pipeline)`** — `pipeline::commit::record_part` is the
  single home for the per-part commit ordering: I1 finalize → dest.write
  → ADR-0012 M1 manifest add → counters → journal event → I7 file-log
  warn-on-fail → fault hooks. Six runners (`single`, `keyset`,
  `chunked::run_chunked_sequential`, `chunked::run_chunked_parallel`,
  `chunked::sequential_checkpoint`, `chunked::parallel_checkpoint`)
  now share one body each instead of hand-copying. See ADR-0018.
- **`fix(pipeline)`** — `chunked::parallel_checkpoint` previously
  populated `state.file_log` per chunk but never appended to
  `summary.manifest_parts`, so the cloud manifest (ADR-0012 M1) shipped
  empty for every `parallel>1 + chunk_checkpoint:true` run. Migration
  onto `commit::record_part` (with `state=None` in the drain to avoid
  double-writing the per-chunk durable file_log) closes the gap.
- **`refactor(pipeline)`** — `pipeline::run_store::RunStore` is the
  builder facade for the post-finalize cursor + progression writes
  (`with_cursor` is fatal-on-error per ADR-0001 I3; `with_progression`
  is warn-on-fail per ADR-0008 PG2 / PG7). Four runners use it; the
  ordering contract is now an interface property, not a convention.
  See ADR-0018. Schema-drift stays in `single.rs` because it's a
  policy state machine, not a persistence ordering.
- **`refactor(tuning)`** — `tuning::Governor` extracts the OPT-2
  adaptive-concurrency loop out of an inline `thread::scope` closure
  in `chunked::exec::run_chunked_parallel`. The loop is now
  unit-testable on a fake `PressureSource` in microseconds instead of
  a 2-4 s live test. See ADR-0019.

### Extraction & memory hardening (optimization backlog)

- **`feat(pipeline)`** — **adaptive concurrency governor** (OPT-2): in chunked
  mode with `parallel > 1` and `tuning.adaptive: true`, a governor samples
  source write-pressure on a dedicated monitoring connection and resizes the
  live worker/connection count within `[min_parallel, parallel]` — backing off
  under load, recovering when it eases. Decisions land in the run journal
  (`ParallelismAdjusted`). Read-only credentials suffice.
- **`fix(pipeline)`** — **governor deadlock under chunk failure** (OPT-2):
  workers bumped the `completed` counter only on success, but the governor's
  exit condition was keyed on it — so any failing chunk left the governor
  spinning forever and `thread::scope` could never join. Workers now bump a
  separate `finished` counter on every exit path (success or failure).
- **`test(pipeline)`** — **governor concurrent-write back-off** (OPT-2):
  deterministic live coverage of the closed-loop reaction to source pressure
  — a background `UPDATE`/`CHECKPOINT` writer drives `checkpoints_req` past
  the 80 ms sampler, and the governor's `backed off` log lines fire as
  expected.
- **`feat(pipeline)`** — **MySQL keyset (seek) pagination** (OPT-4): tables with
  a UUID / string / composite (non-integer) PK now have a safe chunked shape via
  `chunk_by_key:` (auto-resolved on MySQL when there's no single-int PK but a
  usable unique key). Pages by an index-backed unique key
  (`WHERE key > last ORDER BY key LIMIT n`) — bounded RSS *and* bounded
  longest-query time, EXPLAIN-verified as an index range scan (never a
  full-scan + filesort). A non-indexed `chunk_by_key` is refused.
- **`feat(pipeline)`** — **PG UUID-PK keyset via explicit `chunk_by_key:`**:
  `extract_last_cursor_value` learned the `FixedSizeBinary(16)` arm so PG
  `uuid` columns (ADR-0014 → arrow.uuid extension → native Parquet
  `LogicalType::Uuid`) now serve as keyset cursors. Auto-resolution on PG
  remains scoped to integer PKs (see ADR-0020 for the asymmetry rationale
  vs MySQL); operators with UUID-PK tables can opt into keyset paging by
  declaring `chunk_by_key: <uuid_col>`.
- **`feat(sink)`** — **per-value size ceiling** (OPT-1): `tuning.max_value_mb`
  (default 256 MB; `0` disables) aborts with `RIVET_VALUE_TOO_LARGE` when a
  single text/JSON/blob cell would OOM the process — the average-based batch
  cap can't bound a lone giant value.
- **`test(types)`** — **proptest MySQL value round-trip** (OPT-3): 1000
  randomized values per supported type prove Rivet's MySQL value-decoder
  contract under the property-testing fuzzer.
- **`test(pipeline)`** — **subprocess crash coverage** (OPT-6): every chunked
  fault point (`after_chunk_file`, `after_chunk_complete`) now has crash-
  recovery coverage on the `parallel-export-processes` engine. Brings the two
  engines to per-fault symmetry.
- **`feat(format)`** — **stable Parquet `created_by`** (OPT-5): pinned to a
  release-stable string so per-part `content_fingerprint` survives across
  builds and the manifest dedup token is reliable.

### Supply chain

- **`docs(security)`** — documented release-checksum verification. Every release
  already publishes `SHA256SUMS.txt`; README + SECURITY.md now show
  `sha256sum -c` / `shasum -a 256 -c` (the docs previously said "rebuild from
  source"). Signing/SBOM remain on the roadmap.

### Types — native Parquet logical types + round-trip proof

- **`feat(pipeline)`** — **adaptive concurrency governor** (OPT-2): in chunked
  mode with `parallel > 1` and `tuning.adaptive: true`, a governor samples
  source write-pressure on a dedicated monitoring connection and resizes the
  live worker/connection count within `[min_parallel, parallel]` — backing off
  under load, recovering when it eases. Decisions land in the run journal
  (`ParallelismAdjusted`). Read-only credentials suffice.
- **`feat(pipeline)`** — **MySQL keyset (seek) pagination** (OPT-4): tables with
  a UUID / string / composite (non-integer) PK now have a safe chunked shape via
  `chunk_by_key:` (auto-resolved on MySQL when there's no single-int PK but a
  usable unique key). Pages by an index-backed unique key
  (`WHERE key > last ORDER BY key LIMIT n`) — bounded RSS *and* bounded
  longest-query time, EXPLAIN-verified as an index range scan (never a
  full-scan + filesort). A non-indexed `chunk_by_key` is refused.
- **`feat(sink)`** — **per-value size ceiling** (OPT-1): `tuning.max_value_mb`
  (default 256 MB; `0` disables) aborts with `RIVET_VALUE_TOO_LARGE` when a
  single text/JSON/blob cell would OOM the process — the average-based batch
  cap can't bound a lone giant value.

### Supply chain

- **`docs(security)`** — documented release-checksum verification. Every release
  already publishes `SHA256SUMS.txt`; README + SECURITY.md now show
  `sha256sum -c` / `shasum -a 256 -c` (the docs previously said "rebuild from
  source"). Signing/SBOM remain on the roadmap.

### Types — native Parquet logical types + round-trip proof

- **`feat(types)`** — UUID columns now emit native Parquet
  `LogicalType::Uuid` as `FixedSizeBinary(16)` carrying the Arrow canonical
  `arrow.uuid` extension type; JSON/JSONB carry `arrow.json`. Downstream
  engines (DuckDB → native `UUID`, ClickHouse → `Nullable(UUID)`,
  BigQuery) load these without a cast. Enabled via the parquet
  `arrow_canonical_extension_types` feature. See `src/types/mapping.rs`.
- **`test(types)`** — four-reader validator matrix: every PG/MySQL type
  round-trips through Parquet and is read back by DuckDB, ClickHouse,
  pyarrow, and (live) BigQuery to pin field metadata + row-group stats
  (`tests/type_roundtrip/`).
- **`fix(types/mysql)`** — `UNSIGNED BIGINT` (UINT64) overflows `INT64`;
  now mapped to `Decimal128` so the full range survives.

### Bug fixes — validation surface

- **`fix(pipeline/chunked)`** — the sequential checkpoint path ran
  `--validate` on every chunk file but never recorded the result, so
  `mode: chunked` + `chunk_checkpoint: true` + default `parallel: 1` runs
  stored `validated = NULL` in `export_metrics` and dropped the
  `validated: pass` line from the run summary. It now sets the flag like
  the other three export paths (regression test in
  `tests/live_chunked_recovery.rs`).
- **`fix(preflight/doctor)`** — `rivet doctor` drops a `.rivet_doctor_probe`
  writability test object at the destination and never removes it. A
  subsequent `rivet run --validate` flagged it as an `untracked_object`
  and downgraded the run to `validated: FAIL`. The probe is now a
  recognised Rivet sidecar (`manifest::DOCTOR_PROBE_FILENAME`) and skipped
  by the manifest-aware `--validate` pass (regression test in
  `src/pipeline/validate_manifest.rs`).

### Preflight + UX

- **`fix(preflight)`** — chunked / incremental exports on an indexed
  cursor / chunk column no longer report a false `DEGRADED` verdict. A
  catalog `btree` probe replaces the `EXPLAIN`-on-base-query heuristic, so
  an indexed PK reads as `ACCEPTABLE` with an `(indexed)` scan-type
  suffix.
- **`polish(ux)`** — `rivet init` explains its mode choice inline
  (`# auto: ~N rows ≥ 100K threshold and chunk column 'id' is available`)
  and scales `chunk_size` to the row estimate; skipped incremental runs
  print `status: skipped (no new rows since cursor 'X')`; the plaintext-
  password and TLS warnings fire from `doctor` / `check`; the retry-safe
  WARN is demoted to DEBUG for local destinations.

### Docs + assets

- **`docs`** — validated every command in the user-facing guides against
  the binary; fixed drift (the `file_log` state table, real `rivet doctor`
  output, the pilot walkthrough's missing `decimal(10,2)` override).
- **`docs(gifs)`** — regenerated all instructional GIFs against current
  behavior (card UI, `(indexed)` scan type, `validated: pass`).

### Invariant audit — CI gates and paper trail

- **`test(invariants)`** — `RunSummary::check_post_run_invariants` runs
  as a `cfg!(debug_assertions)` gate at the top of
  `pipeline::finalize::finalize_manifest`. Catches the next M1-shape
  bug (runner bumps `files_committed` / `bytes_written` without going
  through `commit::record_part`) the moment a debug-build test
  finishes a run. Closes gaps #2 + #3 ("completed table must have
  manifest entries"; "summary totals derivable from manifest") from
  the release-checklist invariant audit.
- **`test(invariants)`** — companion gates close gaps #1
  (`success && total_rows > 0 ⇒ files_committed > 0` — no rows
  extracted-then-dropped on the floor) and #4 (live test
  `successful_run_writes_summary_artifacts_under_dot_rivet` asserts
  `.rivet/runs/<run_id>/summary.{json,md}` exist on disk after every
  successful run, pinning ADR-0001 I8 at the on-disk layer).

### Cloud destinations — consolidate retry / runtime / read surface

- **`refactor(destination)`** — `CloudBackend` trait + generic
  `CloudDestination<B>` consolidate retry policy, blocking-operator
  wrap, prefix join, and the ADR-0013 read surface
  (`write` / `list_prefix` / `read` / `head` / `move`) across S3,
  GCS, and Azure. Per-backend modules now only supply `build_operator`
  + a label and a scheme. Net: -424 LoC duplication across the three
  cloud backends. The local filesystem destination stays separate
  (no OpenDAL runtime, partial-write semantics genuinely differ).

### CI / infra

- **`ci`** — `jlumbroso/free-disk-space@main` runs before the heavy
  build + test-profile rebuild in the `e2e` job (ci.yml) and the
  nightly-live job, freeing ~30 GB by pruning .NET / Android SDK /
  Haskell GHC / CodeQL / tool-cache (Rivet never touches them).
  Recent nightly-live failures (`Process completed with exit code
  101` with no test annotation because cargo's stderr garbled under
  ENOSPC) are the prompt; `df -h /` snapshots before and after
  surface any future regression directly in the run log.

### Architecture decisions

- **`docs(adr)`** — ADR-0015: Source introspection is a data-shape
  seam, not a trait. Documents the dismissal of the recurrent "unify
  `introspect_pg_table_for_chunking` + `introspect_mysql_table_for_chunking`
  under a trait" suggestion — the two functions share a return type
  but no implementation logic (different catalogs, dialects, quirks).
- **`docs(adr)`** — ADR-0016: Nullability propagation deferred to v0.8
  Phase A. Replaces the earlier "by design" dismissal of Gap #5 with
  an honest deferred-decision record; names the four
  `SourceColumn::simple(…, true)` hardcode sites, the per-query-shape
  resolvability matrix, the operator workaround, and the revisit
  trigger.
- **`docs(adr)`** — ADR-0017: Per-runner durability ordering map.
  Documents the asymmetric file_log timing (four runners inline
  per part; `chunked_parallel` post-scope drain;
  `parallel_checkpoint` split sync-worker + post-scope), names the
  C3 live-test invariant that forced the split, and acknowledges
  the per-chunk `StateStore::open` smell that the split kept.
- **`docs(adr)`** — ADR-0018: Builder facades for runner-level
  invariant ordering. Positive paper trail for `commit::record_part`
  + `RunStore`: why builder over single-method / type-state, why
  facades and not traits, what stays *outside* the facade (schema
  drift in single.rs, metrics in job.rs).
- **`docs(adr)`** — ADR-0019: Governor as extracted policy with
  injectable `PressureSource`. Documents the testability win, why the
  trait lives in `tuning::` not `source::`, and the deadlock-class
  regression cover.
- **`docs(adr)`** — ADR-0020: PostgreSQL UUID-PK chunking asymmetry
  vs MySQL. Two-layer gap: layer 1 (planner's PG-no-auto-keyset
  default — deferred design choice; `DECLARE CURSOR` is RAM-bounded
  but not wall-time-bounded) and layer 2 (sink runtime missing
  `FixedSizeBinary(16)` arm — closed in this release).
- **`docs(CLAUDE.md)`** — added "Verify before publishing agent-walk
  claims" process rule: when an Agent(Explore, …) walk returns
  claims with specific file paths / line numbers, the next action
  is a `Read` / graph query on the named site before writing the
  claim into a deliverable. Lesson from a real architecture-review
  walk that produced six false claims unverified.

### Dependencies

- Bumped `mach2` 0.4 → 0.6, `tikv-jemallocator` 0.6 → 0.7,
  `criterion` 0.5 → 0.8 (dev), `brotli` 8.0.2 → 8.0.3.

## 0.7.7 (2026-05-26) — Audit-Gap Closure

> Closes the five remaining audit gaps from the 0.7.6 sweep.

### Fixes

- **`fix(state/reset)`** — `state reset --export <X>` validates the export
  against the loaded config before touching state. A typo (`--export
  pa_audi` for declared `pa_audit`) now bails with a hint listing the
  declared names and a follow-up `rivet state show` command instead of
  silently DELETE-ing zero rows and printing "State reset for export ..."
  as if it worked.

### Test infrastructure

- **`test(path_matrix)`** — pin data-accounting fields (`total_rows`,
  `files_produced`, `status`, `format`, `compression`, `export_name`)
  from every `summary.json` produced by `rivet run`. Catches the class
  of regression where layout matches but row count is wrong.
- **`test(query_matrix)`** — NEW. Fourth matrix. Pins PG `EXPLAIN
  (COSTS OFF)` plan shape per representative query. Catches operator
  query wrapped in subquery / CTE, planner dropping the PK index, sort
  steps appearing without intent.
- **`test(gremlin)`** — added 5 new scenarios (G6–G10): `row_count_max`
  upper bound, `row_count_min` boundary inclusivity, NULL handling in
  `unique_columns`, chunked + sparse-ID quality, multi-export
  one-fails-others-continue.
- **`test(soak_matrix)`** — NEW. Fifth matrix. Pins order-of-magnitude
  perf and memory thresholds on a 10k-row PG table per export mode
  (full / chunked / incremental). Catches "50x slower" / "10x memory"
  regressions; tunable thresholds, deliberately generous to avoid
  CI runner noise.
- **`test(dev/matrices)`** — orchestrator + taxonomy (Surface /
  Execution / Resources / Compatibility layers) + shared `_common/lib/`
  for helpers used by ≥2 matrices. `run.sh --tier=pr|nightly|release`
  binds each matrix to a CI tier.

## 0.7.6 (2026-05-25) — Operator-Surface Test Matrices

> Focus: close the test-coverage gaps that let the 0.7.5 audit
> findings live for so long.  Tests in 0.7.6 pin the *operator
> surface* — exit codes, stderr/stdout text, file layout, log
> dedup, artifact wire format — not just function return values.
> Four new matrix harnesses under `dev/` codify what every release
> must clear.

### Bug fixes — config + preflight + source

- **`fix(source/mysql)`** — MySQL connections panicked from inside
  the `mysql` crate ("Client had asked for TLS connection but TLS
  support is disabled") whenever `tls.mode != disable`.  Switch the
  crate to `default-features = false, features = ["minimal",
  "native-tls"]` so MySQL + TLS works the same way Postgres + TLS
  already does (shared OpenSSL stack the workspace already vendors).
  Net `Cargo.lock` reduction: drops `mysql-common-derive` +
  `darling` transitive macro deps.
- **`fix(plan/build)`** — `mode: chunked` with the `table:` shortcut
  silently auto-resolved `chunk_column` from the primary key with
  no operator-visible signal (the log was `info!`, below the
  default `warn` level).  A typo'd `chunk_column` fell back to PK
  without any indication.  Elevated to `warn!` with "Set
  `chunk_column:` explicitly to silence."
- **`fix(config)`** — `query_file: ../../../etc/passwd` passed
  `rivet check` and `rivet doctor` and was only caught at plan
  time inside `ExportConfig::resolve_query`.  Syntactic checks
  (absolute path, `..` traversal) lifted into `Config::validate`
  so check/doctor reject early at config-load.  `resolve_query`
  keeps its `canonicalize`-based symlink check for the read-time
  race.
- **`fix(preflight/check)`** — `rivet check` previously skipped
  destination credential preflight: a config with
  `AWS_ACCESS_KEY_ID` unset returned rc=0 from check and died on
  run.  Doctor caught it; check did not.  `check` now calls
  `create_destination` per unique destination so env-var resolution
  and `credentials_file` existence are validated up-front.  No
  write-probe side effect — that stays doctor-only.

### Bug fixes — log dedup

- **`fix(config/resolve)`** — the F10 warning ("`--param X was not
  referenced`") fired once per code path that touched param
  resolution — twice or more per `--param` for typical exports.
  Split `warn_unused_params` out of `resolve_vars` and call it
  exactly once in `Config::load_with_params`.  Adds
  `find_unused_params` as the testable kernel; 5 new unit tests
  pin used/unused/partial/mixed/None.
- **`fix(config/models)`** — "source URL contains plaintext password
  — consider using url_env or url_file" warning emitted 3–4× per
  run.  Gated with `Once::call_once` so it fires once per process.
- **`fix(source/mod)`** — same `Once::call_once` dedup applied to
  the "TLS is not enforced" warning.

### Bug fixes — state

- **`fix(state/reset)`** — `rivet state reset --export <name>` now
  bails with an informative hint when `<name>` is not defined in
  the config (previously a silent no-op that looked like success).

### Regression harnesses (new under `dev/`)

The 0.7.5 audit revealed that unit + live tests pinned code
behaviour, not the operator-facing surface.  These four matrices
cover that gap and are wired into CI so a regression of any kind
— exit code, file layout, stderr text, version skew — blocks merge.

- **`dev/cli_matrix/`** — expanded to **88 scenarios** across
  doctor / check / run / plan / apply / state / metrics / schema /
  journal / validate / reconcile / repair / init.  New
  `check_msg.sh` adds **32 message-substring assertions**
  (positive / negative / exact-count) on stderr+stdout per
  scenario, plus state sub-actions, type-report variants, journal
  flags, frozen-plan apply.
- **`dev/cfg_matrix/`** [NEW] — **83 YAML fixtures** across 7 axes
  (source / TLS / export-mode / destination / edge / multi /
  negative) each probed with `doctor` + `check` + `plan`.  17
  msg-substring assertions pinning what the four config/preflight
  fixes above changed.
- **`dev/path_matrix/`** [NEW] — **7 scenarios** that run `rivet
  run` and diff produced file layout against frozen, timestamp-
  normalized baselines.  Catches `_SUCCESS` drops, chunk naming
  renames, `.rivet_state.db` relocation, stdout-leaks-into-out/.
  Subsequent commits added per-scenario data-accounting assertions
  from `summary.json` (rows / files / bytes / status) and EXPLAIN
  plan-shape pins per representative query.
- **`dev/cross_version_matrix/`** [NEW] — `doctor` / `check` /
  `plan` against PG 12/13/14/15/16 + MySQL 5.7/8.0; asserts rc
  agreement across versions of the same engine.  Designed for CI
  as a fast smoke gate (lighter than the full live matrix).

### Regression tests

- **`tests/artifact_legacy_compat.rs`** [NEW] + 4 frozen v0.7.5
  artifacts under `tests/fixtures/artifacts_legacy/`.  Pins the
  wire format of `plan.json` / `summary.json` so a future serde
  rename that breaks committed plan artifacts is caught before
  users hit it on disk.
- **`tests/gremlin.rs`** — 5 new boundary scenarios (G6–G10):
  - **G6** `row_count_max` symmetric with `row_count_min` (catches
    "my predicate is wrong, daily summary returned 50 000 rows").
  - **G7** Off-by-one boundary: 10 rows ≥ min=10 passes; 10 rows
    < min=11 fails.  Pins the `>=` vs `>` choice in the
    comparator.
  - **G8** NULL on a non-uniqueness column does not false-positive
    on the indexed column (SQL `NULL != NULL` convention).
  - **G9** Sparse-ID chunks aggregate at export level, not per
    chunk.  Pins aggregation semantics against per-chunk gate
    refactors.
  - **G10** Multi-export with one failing + one healthy export:
    the healthy parquet still lands even though the run exits
    non-zero.

### Notes

The 0.7.5 audit's `F-NEW-E` (reconcile/repair require `--export`
while run/check/plan do not) was reviewed in this cycle and left
as the documented contract — these commands need a single chunked
target by design.  See `dev/cli_matrix/expected_rc.txt` and
`dev/cfg_matrix/README.md` for the rationale.

## 0.7.5 (unreleased) — Plan/Apply UX Audit + Regression Harness

> Focus: drive the binary through every subcommand × flag combination
> against PostgreSQL and MySQL fixtures, capture the actual behaviour,
> and fix the inconsistencies that showed up.  Tests pinned by
> observed output, not by code reading.  The harness lives in
> `dev/cli_matrix/` and is now the regression guard for future
> releases — diff per-scenario `stdout`/`stderr`/exit codes between
> the previous and next release before tagging.

### Bug fixes (apply path)

- **`fix(pipeline/apply)`** [F1] — `--force` now correctly bypasses the
  cursor-drift gate (previously the error message claimed it would,
  but the code never honoured the flag).
- **`fix(pipeline/apply)`** [F13] — apply resolves state DB from the
  original config's directory (recorded in `PlanArtifact.config_path`
  at plan time), not the plan file's directory.  Storing a plan JSON
  separately from its config no longer produces bogus *cursor drift
  (current: None)* on every incremental apply.  Pre-0.7.5 artifacts
  fall back to the plan file's directory with a `WARN`.
- **`feat(pipeline/summary)`** [F5] — every apply run records an
  `apply_context: { plan_id, forced, force_bypassed }` block in
  `summary.json`.  Closes the audit-trail gap surfaced by the 0.7.5
  audit.

### Bug fixes (UX polish)

- **`fix(redact)`** [F-NEW-C] — `redact_url_passwords` no longer
  double-encodes multi-byte UTF-8 codepoints.  Every error message
  containing an em-dash, Cyrillic, or any non-ASCII glyph passing
  through the redactor came out mojibaked.  Pinned by tests.
- **`fix(preflight/doctor)`** [F-NEW-A] — `rivet doctor` exits
  non-zero when any probe fails.  Previously printed `[FAIL]` but
  exited 0 so cron/CI could not detect a broken environment by rc.
- **`fix(config)`** [F11, F12] — config-file-not-found and YAML
  parse errors now name the file path.
- **`fix(plan/artifact)`** [F8] — `apply` on a non-JSON file produces
  a Rivet-shaped message instead of the raw `expected ident at line
  1 column 2`.
- **`fix(pipeline/apply)`** [F6] — stale-plan error switches to days
  + creation date for ages ≥ 48 h ("2334 days old (created 2020-01-01)")
  instead of "56035 hours old".
- **`fix(plan)`** [F3] — `diagnostics.warnings` populates from
  `build_suggestion` when the verdict is non-Efficient and no specific
  warning was collected.  JSON consumers no longer see `DEGRADED`
  with `warnings: []`.
- **`fix(plan/chunked)`** [F4] — chunked `row_estimate` is derived
  from `chunk_ranges` instead of `pg_class.reltuples` (stale before
  `ANALYZE`).  PG and MySQL artifacts of the same fixture now agree.
- **`fix(config/resolve)`** [F10] — warns when `--param key=value`
  is passed but `${key}` never appears in the config.
- **`fix(pipeline/run)`** [F-NEW-B] — `run --force` without `--resume`
  warns that the flag is a no-op in this combination.
- **`feat(main)`** [F-NEW-F] — `env_logger` default level is now
  `warn` (was implicit `error`).  Every `log::warn!` surfaces by
  default; `RUST_LOG` still overrides.

### Regression harness

- **`feat(dev/cli_matrix)`** — new harness drives 75 scenarios across
  13 subcommands × {PG, MySQL} × flag combinations.  Each scenario
  captures `stdout`, `stderr`, exit code, command line, and description.
  Used as a release gate: diff per-scenario rc between previous and
  next release before tagging.  See `dev/cli_matrix/README.md`.
- **`ci`** — new `cli-matrix` job runs the harness against the docker
  compose fixtures on every PR and the nightly schedule.  Failures
  block merge.

## 0.7.4 (unreleased) — Trust Hardening + First-User Story

> Focus: close the trust-documentation layer, add a small high-leverage
> cloud-safety preflight, and remove the most common first-user
> footguns.  No new extraction modes, no new destinations.  This
> release tightens what the existing product already does and removes
> the time-to-first-success friction that previously bounced
> evaluators back to the docs.

### Highlights

- **Azure SAS-token expiry preflight.**  When a destination uses
  `sas_token_env`, Rivet now parses `se=` (signed-expiry) at
  construction time.  Already-expired tokens fail fast with a
  named-field error message; tokens within 60 minutes of expiry
  log a warning so an operator knows before kicking off a
  multi-hour export.  Unparseable `se=` values warn instead of
  crashing — the token may still authenticate.  Closes the
  known-limitation note in `docs/cloud-destinations.md` § Known
  limitations.
- **First-user-friendly error messages.**  Every config-layer
  and `rivet doctor` error a first-time operator hits now carries
  a `Hint:` line with a concrete next action — the exact `export
  VAR=…` command for missing env vars, the `tls.mode: prefer` /
  `tls.ca_file` knob for TLS handshake failures, the per-backend
  `cloud-permissions.md` link for IAM denials, the
  `az storage container generate-sas` invocation for expired SAS,
  and so on.  The wording is pinned by 15 unit tests so dropping
  a hint accidentally trips CI.
- **`docs/who-is-this-for.md`.**  Explicit Yes / No fit-check with
  named alternatives (Debezium, Airbyte, Fivetran, dbt, DuckDB,
  Trino) so visitors with the wrong problem leave fast and the
  ones who stay are pre-qualified.  Surfaced as a callout block
  at the top of the README.
- **README reorganization.**  Workflow story (`init → doctor → check
  → plan → run → validate`) now precedes the dense benchmark
  methodology section.  Hero numbers stay in the headline; the
  cross-tool tables move below "Trust contracts" so first-time
  visitors see the product narrative before the speeds-and-feeds.
- **Trust-documentation layer.**  Five new evergreen artifacts
  consolidate operator discipline that previously lived in
  individual maintainer's heads:
  - `docs/release-checklist.md` — every gate every tag must clear.
  - `docs/cloud-smoke-tests.md` — manual real-cloud verification
    matrix with last-verified dates per backend.
  - `docs/cloud-permissions.md` — least-privilege IAM / RBAC / SAS
    scopes for S3 / GCS / Azure split per Rivet command.
  - `docs/recipes/recover-interrupted-run.md` — action-first
    cookbook: resume, validate, reconcile, repair, state reset.
  - `docs/recipes/idempotent-warehouse-load.md` — manifest-driven
    BigQuery / Snowflake load patterns layered over Rivet's
    at-least-once delivery contract.

### Changes

- **`feat(destination/azure)`** — `parse_sas_expiry_status` plus
  `enforce_sas_expiry` preflight in `AzureDestination::new`.  Ten
  new unit tests cover RFC3339 parsing, URL-encoded colons,
  threshold boundaries, the no-`se=` (stored-access-policy) path,
  and the wire path through the destination constructor.
- **`feat(config/error-messages)`** — the most common first-run
  failures now carry actionable `Hint:` lines:
  - missing source block → "Add `url_env: DATABASE_URL`" with the
    full alternatives matrix (url / url_env / url_file / structured);
  - `url_env` referencing an unset env var → the concrete
    `export DATABASE_URL='postgresql://…'` shell command;
  - `password_env` referencing an unset env var → the concrete
    `export <NAME>='your-password'` shell command;
  - mixed URL + structured fields → "remove whichever block you
    don't want; mixing the two is ambiguous";
  - structured config missing `host` / `user` / `database` → the
    concrete YAML edit and the URL-based alternative.
- **`feat(preflight/doctor)`** — `source_error_hint` and
  `destination_error_hint` map a categorised error to a concrete
  next-step hint, printed below the failure line in `rivet doctor`.
  Covers TLS handshake (PG and MySQL), auth errors (`pg_hba.conf`
  for PG, `GRANT … FLUSH PRIVILEGES` for MySQL,
  `s3:PutObject` / `storage.objects.*` /
  `az storage container generate-sas` for the cloud backends),
  bucket / container "does NOT auto-create" + the exact
  `aws s3 mb` / `gcloud storage buckets create` /
  `az storage container create` invocation, and connectivity
  errors (`region:` mismatch, bastion / VPN reminders).
- **`docs(README)`** — new "Is this for you?" callout above
  `## Why Rivet`, linking to `docs/who-is-this-for.md`.  The
  "Source pressure, measured" benchmark section moves below
  "Trust contracts" so the workflow story comes first; the
  cross-reference in the Trust contracts table now uses
  `[Source pressure, measured](#…) below`.
- **`docs(who-is-this-for)`** — new doc with explicit Yes / No
  rows and named alternatives (Debezium / Estuary / Materialize
  for CDC; Airbyte / Fivetran / Stitch for connector
  marketplaces; Fivetran / Airbyte Cloud / dlt / Sling for
  managed extract+load; dbt / SQLMesh for transforms; Airflow /
  Dagster / Prefect for orchestration; DuckDB / Trino /
  ClickHouse for query-time integration).  Edge-case section
  covers very-large dumps, weak primary keys, replication lag,
  SSH bastions, and stateless runners.
- **`test`** — 15 new unit tests pin the error-message contract:
  5 in `src/config/tests.rs` (no-source-block, missing host with
  hint, missing url_env, missing password_env, mixed-fields), and
  10 in `src/preflight/mod.rs::tests` (TLS hint per engine, auth
  hint per engine, connectivity hint, no-hint for unknown errors,
  and the four destination categories with their backend-specific
  guidance).  Existing `no_url_at_all_rejected` updated to assert
  the new `connection method` + `url_env` + `DATABASE_URL`
  wording.
- **`docs(README)`** — new `## Core workflow` section between the
  30-second quickstart and "What Rivet is" surfaces the canonical
  command path (`init → doctor → check → plan → run → validate`,
  with `apply` and `reconcile`/`repair` branches).  Documentation
  table extended with rows for cloud smoke tests, release
  checklist, cloud permissions, and operator recipes.
- **`docs(reliability-matrix)`** — new "Manual / release-gated
  coverage" section names the cloud backends with their
  last-verified dates and links to `docs/cloud-smoke-tests.md`.
  Resolves the long-standing tension between "what's in CI" and
  "what was hand-verified before publish".
- **`docs(README index)`** — new "Trust contracts" rows for cloud
  smoke tests, release checklist, and cloud permissions; new
  "Operator recipes" sub-section under Production.

### Bug fixes (preflight hardening)

- **`fix(destination/azure)`** — URL-encoded `%2B` / `%2b` (plus sign) in the
  `se=` expiry value is now decoded alongside `%3A` / `%3a` (colon), so
  timezone offsets like `+00:00` in tokens from `az … -o tsv` parse
  correctly.
- **`fix(destination/azure)`** — Near-expiry threshold comparison changed from
  `<` to `<=`: a token whose remaining validity is exactly 60 minutes now
  correctly triggers the `NearExpiry` warning (previously slipped through as
  `Healthy`).
- **`fix(preflight/doctor)`** — `categorize_dest_error` now returns the
  dedicated `"sas expired"` category before the generic `"auth error"` check.
  Prior ordering let the token-keyword match in `"auth error"` win and route
  expired-SAS errors to the wrong hint.
- **`fix(preflight/doctor)`** — `destination_error_hint` signature simplified:
  the unused `err: &anyhow::Error` parameter (formerly `_err`) has been
  removed; the Azure SAS guard is now the `"sas expired"` match arm instead
  of a pre-match string scan.
- **`test`** — direct test for `categorize_dest_error` category ordering: pins
  that an expired-SAS error message returns `"sas expired"`, not `"auth
  error"`, with a descriptive assertion message that calls out the
  load-bearing nature of the ordering.
- **`chore`** — `cargo update`: 20 packages refreshed including openssl 0.10.80,
  serde_json 1.0.150, mysql_common 0.37.2, winnow 1.0.3, tokio 1.50.0.

### Notes

The Azure SAS preflight runs at destination construction, so
`rivet doctor` and `rivet run` both pick it up without any new
flag.  An expired token now produces a Rivet-shaped error message
("Azure SAS token already expired (se=…)") instead of an opaque
opendal 403 on the first PUT.  The 60-minute near-expiry threshold
is fixed in this release; making it operator-tunable is tracked
for a future minor.

## 0.7.3 (2026-05-22) — Developer Experience Polish

> Focus: make Rivet easier to configure, inspect, and try correctly.
> No new extraction modes — every change reduces first-touch friction
> for new operators.

### Highlights so far

- **JSON Schema for `rivet.yaml`** — `schemas/rivet.schema.json` (and the
  stable `schemas/latest/` mirror) ships in-tree so editors with the YAML
  Language Server (VS Code, Neovim, Helix) autocomplete fields, flag
  typos, and surface required keys.  Reference it from a config via:

      # yaml-language-server: $schema=https://raw.githubusercontent.com/panchenkoai/rivet/main/schemas/latest/rivet.schema.json

- **`rivet schema config`** — emits the JSON Schema for the running
  binary's Config types to stdout.  Pipe to a file or check it in to
  pin a per-version schema in your own repo.

### Changes

- **`feat(config)`** — `#[derive(JsonSchema)]` propagated across the
  full `Config` type tree (`SourceConfig`, `ExportConfig`,
  `DestinationConfig`, `TuningConfig`, `TlsConfig`, every nested enum).
  The schema embeds the binary's `CARGO_PKG_VERSION` in its `title` so
  drift is detectable by inspection.
- **`feat(cli)`** — new `rivet schema config` subcommand.  Output
  format: pretty-printed JSON Schema (draft 2020-12), trailing newline.
- **`docs(examples)`** — `examples/pg_full_local.yaml` now carries a
  `# yaml-language-server: $schema=…` header so editor validation is
  on by default for any operator who runs that example.
- **`test`** — `tests/schema_drift.rs` pins the in-tree schema artifact
  to the runtime output; CI fails when a Config change forgets to
  regenerate the schema.  Three guard checks: byte-equality vs the
  generated schema, byte-equality between primary and `latest/`
  mirrors, and `CARGO_PKG_VERSION` presence in the title.
- **`feat(config)`** — strict-mode YAML parsing (P1.2).  Eleven
  Config-tree structs now carry `#[serde(deny_unknown_fields)]`:
  `Config`, `SourceConfig`, `TlsConfig`, `ExportConfig`,
  `DestinationConfig`, `TuningConfig`, `QualityConfig`, `MetaColumns`,
  `ParquetConfig`, `NotificationsConfig`, `SlackConfig`.  Previously
  silent-drop typos (`acccess_key_env`, `database_name`, `profil`,
  `expoorts`) now fail fast at parse time.  The schema artifact
  reflects this: 11 new `"additionalProperties": false` declarations.
- **`feat(config)`** — did-you-mean suggestions on parse errors
  (P1.2).  New `crate::config::lints` module post-processes serde's
  `unknown field` error and appends a one-line `Did you mean \`X\`?`
  hint when the typo is lexically close to a real field name
  (Levenshtein ≤ longer/3 OR substring relation).  The
  `closest_match` heuristic prefers prefix/suffix matches (so
  `database_name` correctly suggests `database`) but stays
  conservative — no suggestion is shown when nothing is close enough.
- **`docs(examples)`** — new `examples/README.md` clarifies that the
  YAML files are CLI sample configs, not `cargo run --example`
  targets, lists each file's source/mode/destination at a glance,
  and points operators to `demo/` for turn-key Docker setups
  (roadmap P1.1; physical directory move deferred to preserve URLs
  already linked from blog posts and external docs).
- **`test`** — `tests/config_parse_errors.rs` pins the
  unknown-field + did-you-mean contract across every level operators
  can mistype (top-level, `source:`, `exports[]`, `destination:`,
  `tuning:`), plus the absence of a suggestion when nothing is
  close enough, plus the preservation of the existing dedicated
  misplaced-tuning hint.
- **`ci`** — bump `actions/checkout`, `actions/upload-artifact`, and
  `actions/download-artifact` from v4 → v5 for Node 24 compatibility
  (the Node 20 runtime is on the GitHub deprecation path).  No
  behavior change for the workflow shape itself.
- **`ci`** — `Swatinem/rust-cache@v2` now also caches the `fmt` job's
  toolchain extraction (with `cache-targets: false` since `cargo fmt`
  never touches `target/`).  Small win — one fewer cold-start cost
  on a PR push.
- **`ci(release)`** — Docker image now built on **native amd64 + arm64
  runners** instead of QEMU-emulated arm64.  The `docker-build`
  matrix pushes per-arch digests (`name=…,push-by-digest=true`); a
  follow-on `docker-manifest` job downloads both digests and stitches
  them into the final multi-arch tag via
  `docker buildx imagetools create`.  Expected arm64 build wall-time
  drops from ~25 min (QEMU) to ~6 min (native).  Pattern follows the
  upstream
  [`docker/build-push-action`](https://github.com/docker/build-push-action)
  "Distribute build across multiple runners" recipe.

## 0.7.2 (2026-05-22) — Cloud Landing Polish

> Focus: make cloud outputs historically verifiable and safer to operate.
> No new extraction modes, no new database sources — every change tightens
> the existing cloud-output contract.

### Highlights so far

- **`rivet validate --date / --run-id / --prefix`** — re-verify a prior run
  without re-running the export.  Lifts the implicit "today only" anchor
  that previously made `rivet validate` blind to yesterday's `{date}` prefix.
- **Shared placeholder resolver** (`crate::destination::placeholder`) — one
  module substitutes `{date}` / `{export}` / `{table}` / `{run_id}` for
  every command that resolves a destination prefix (`run`, `doctor`,
  `validate`, future `reconcile` / `repair`).  New `{run_id}` token; unknown
  `{token}`s are preserved verbatim so a typo fails loudly at open time.

### Changes

- **`feat(validate)`** — `rivet validate` accepts:
  - `--date YYYY-MM-DD`: anchor `{date}` substitution at a prior day.
  - `--run-id <RID>`: substitute `{run_id}` in destination templates.
    Composes with `--date`.
  - `--prefix <STRING>`: bypass placeholder resolution and verify exactly
    this prefix.  Rejected when scope spans multiple exports
    (`--prefix requires --export <name>`).
  - Resolved physical prefix is surfaced in both pretty and JSON output
    (`resolved_prefix`) so operators can confirm at a glance which bytes
    were checked.  Hard-failure error messages include it too.
- **`refactor(destination)`** — new `destination::placeholder` module with
  `PlaceholderContext::{for_today, for_date, with_run_id}` and
  `expand_destination(dest, &ctx)`.  Old `plan::build::expand_destination_templates`
  becomes a thin wrapper that delegates to the new module.
- **`test`** — `tests/validate_historical.rs` regression-tests the anchor
  scenario: "run happened yesterday, validate runs today, `--date` still
  hits the correct physical prefix".
- **`feat(redact)`** — credential redaction is now a defined invariant
  (P0.3).  New `crate::redact` module strips
  `scheme://user:password@host` userinfo from any string about to land
  in an operator-visible artifact.  Applied at the error → artifact
  boundary in `pipeline::job`, `pipeline::single`, `pipeline::repair_cmd`,
  the chunked sequential/parallel checkpoints, the top-level CLI
  `eprintln!` path, and validate's hard-failure messages.  Covers
  `summary.json` / `summary.md`, `.rivet_state.db` journal,
  Slack / webhook payloads, and CLI stderr.
- **`docs(security)`** — SECURITY.md "Redaction in errors and artifacts"
  rewritten: explicit list of redacted paths, known limitations
  (third-party driver output, in-memory secrets, shapes outside the
  URL userinfo pattern), and the test files that pin the contract.
- **`test`** — `tests/redaction_invariant.rs` proves the redactor is
  idempotent, doesn't damage non-URL prose, walks `anyhow::Context`
  chains, and that `summary.json` / `summary.md` written via the
  public run-report writer never carry the URL-embedded password.
- **`feat(destination/azure)`** — Azure SAS-token auth (P0.4).  New
  `sas_token_env` field on the destination config alongside the
  existing `account_key_env`.  Mutually exclusive with `account_key_env`
  — both being set is refused with an actionable error.  Leading `?` on
  the operator-supplied token is trimmed transparently so either the
  full `?sv=…&sig=…` query string or the raw token body works.  The
  token value receives the same `Zeroizing<String>` SecOps treatment
  as `account_key_env`.

  Example:

  ```yaml
  destination:
    type: azure
    bucket: my-container
    account_name: mystorageacct
    sas_token_env: AZURE_STORAGE_SAS_TOKEN
  ```
- **`docs(cloud-destinations)`** — new `docs/cloud-destinations.md`
  consolidates the local / S3 / GCS / Azure auth-and-output contract
  in one place: shared output contract (`manifest.json` + `_SUCCESS`),
  per-backend auth matrix (key/SAS/anonymous for Azure;
  env/profile/session-token for S3; ADC/service-account for GCS),
  resume / validate / quarantine behavior, and known limitations.

## 0.7.1 (2026-05-21)

### Highlights

- **Azure Blob Storage destination** (new) — third cloud target after S3 / GCS, opendal-backed, full M1–M9 trust-contract parity (manifest + `_SUCCESS` + resume + quarantine).  Verified end-to-end against a real Azure account (`belgiumcentral`).
- **AWS S3 STS / SSO / IAM Identity Center / AssumeRole / MFA** support via `session_token_env`.  Verified against a real S3 bucket.
- **Cross-cloud bug fix**: `rivet validate` and `rivet doctor` now substitute `{date}` / `{export}` / `{table}` placeholders in `destination.prefix` (previously only `rivet run` did, so validate/doctor mis-read prefixes with templates).
- **Cohesion pass** on `DestinationConfig`: `#[derive(Default)]` + `..Default::default()` in test fixtures.  Adding a new optional field now touches ~5 helper functions, not ~28 init sites.

### Azure Blob Storage

A new `type: azure` destination, behaviourally on par with S3 and GCS:

```yaml
destination:
  type: azure
  bucket: my-container          # Azure container name (rivet reuses the `bucket` field across S3/GCS/Azure)
  account_name: mystorageacct    # the `<acct>` in `<acct>.blob.core.windows.net`
  account_key_env: RIVET_AZURE_KEY
```

- **`feat(destination/azure)`** — new `AzureDestination` (write / list / read / head / move) on top of `opendal::services::Azblob`.  Same `RetryLayer` and `FinalizeOnClose` capability profile as GCS/S3.  Server-side copy + delete fallback for `r#move` (opendal 0.55 returns Unsupported on `rename` for Azure Blob — same as S3/GCS).
- **`feat(config)`** — new `account_name` (public string) and `account_key_env` (env-var name) fields on `DestinationConfig`.  Key is wrapped in `Zeroizing<String>` on read — same SecOps treatment as `access_key_env`.
- **`feat(destination/azure)`** — endpoint is auto-derived from `account_name` (`https://<account_name>.blob.core.windows.net`); explicit `endpoint:` still wins (needed for Azurite, sovereign clouds, custom DNS).
- **`feat(config)`** — `allow_anonymous: true` for [Azurite](https://learn.microsoft.com/azure/storage/common/storage-use-azurite) emulator and public read-only containers.  Refuses to combine with explicit `account_name` / `account_key_env`.
- **`feat(pipeline/finalize)`** — manifest's `destination.uri` field renders as `az://<container>/<prefix>` (HDFS / azcopy convention).
- **`feat(preflight/doctor)`** — Azure-aware label (`Azure(<container>)`) and error category (`container not found`).
- **`docs(cloud-auth.md)`** — new Azure section: Path A (account_key) + Path B (Azurite) + troubleshooting + use-case recommendations.  Reserved 0.7.2 surface documented: SAS token, service principal, managed identity, connection string — all additive, no breaking changes.

Not in 0.7.1 (planned for 0.7.2, additive):
- SAS token (`sas_token_env`)
- Service principal (`tenant_id` / `client_id` / `client_secret_env`)
- Managed identity (when rivet runs inside Azure VM / AKS / Functions)
- Connection string (`connection_string_env`)

### `--validate` and `doctor` now expand placeholders

Found while live-testing Azure on 2026-05-21: `rivet validate` against a
config with `prefix: "runs/{date}/{export}/"` returned `status: legacy_run`
because the verifier looked at the literal template, not the substituted
`runs/2026-05-21/orders_azure_smoke/` path where data actually lives.
`doctor` exhibited the same symptom by writing a probe at the literal
`runs/{date}/{export}/.rivet_doctor_probe`.

- **`fix(validate)`** — `run_validate_command` now applies `{date}`/`{export}`/`{table}` substitution via `plan::build::expand_destination_templates` before constructing the destination.  Same expansion `rivet run` already used.
- **`fix(doctor)`** — same substitution applied at the `check_destination_auth` probe site so doctor no longer leaves literal-template stub objects in cloud buckets.
- This is **cross-cloud** — the bug affected S3 / GCS / Azure equally.  No engine-specific code touched.
- The substitution uses **today's UTC date**.  If validate is invoked the day after a run, the operator should inline the absolute prefix in config; a planned `--run-id` / `--date` flag (0.7.2) will allow re-targeting historical runs.

### Cohesion pass on `DestinationConfig`

- **`refactor(config)`** — `#[derive(Default)]` on `DestinationType` (Local) and `DestinationConfig`.
- **`refactor(destination/s3)`** — extracted `read_credential_env(env_name, label) -> Result<Zeroizing<String>>` helper, used in all three credential paths (access key, secret key, session token).  Trimmed the 13-line inline IMDS warning down to a 4-line pointer at `docs/cloud-auth.md`.
- **`refactor(tests)`** — all 28 literal `DestinationConfig { ... }` init sites across `pipeline/*`, `plan/*`, `destination/*`, `preflight/*`, and integration tests converted to `..Default::default()`.  Net **−227 lines**.

### AWS S3 — STS/SSO/AssumeRole/MFA support + auth-flow docs

Found while live-testing 0.7.0 against a real S3 bucket (`s3://rivet-data-test/`,
`eu-north-1`): the `aws_profile:` config path uses reqsign's default-chain
loader, which silently falls through to EC2 IMDS on developer laptops when
the named profile carries an AWS IAM Identity Center / "AWS Login" session
(short-lived creds in `~/.aws/login/cache/`, not in `~/.aws/credentials`).
IMDS is unreachable off-EC2 → ~3 minutes of retries → confusing hang.

- **`feat(config)`** — new `session_token_env` field on `DestinationConfig`.
  Pair with `access_key_env` + `secret_key_env` to authenticate as a
  short-lived STS session (any access key starting with `ASIA…`):
  AWS IAM Identity Center / SSO, `aws sts assume-role`, MFA-protected
  sessions, EKS IRSA, GitHub Actions OIDC, etc.

  ```yaml
  destination:
    type: s3
    bucket: my-bucket
    region: eu-north-1
    access_key_env: AWS_ACCESS_KEY_ID
    secret_key_env: AWS_SECRET_ACCESS_KEY
    session_token_env: AWS_SESSION_TOKEN
  ```

  Bridge from AWS CLI v2 (any auth flow):
  ```bash
  eval "$(aws configure export-credentials --profile default --format env)"
  ```

- **`docs(cloud-auth.md)`** — new auth-flow matrix covering all six
  S3/GCS paths (static IAM key, STS/SSO temporary creds, `aws_profile`
  static, ADC for GCS, service-account JSON, anonymous/emulator) plus
  a troubleshooting table for the most common operator-confusing errors
  (IMDS timeout, `InvalidAccessKeyId`, region mismatch, ADC expired).

- **`docs(s3.rs)`** — added a warning at the `aws_profile` code site
  pointing at `docs/cloud-auth.md` when the operator sees an IMDS
  timeout, and clarifying the chain's failure mode.

## 0.7.0 (2026-05-21)

### Cloud manifest contract — write, verify, resume, quarantine

A trust-contract release.  Every export now leaves behind an inspectable
on-disk run report, **and** every cloud-or-local-file destination gains a
public JSON manifest + `_SUCCESS` marker that downstream consumers
(Airflow sensors, CI gating, custom verifiers) can read directly.
`--validate` and `--resume` now consult the manifest to certify the
dataset and to reconcile prior committed parts before re-running work.

ADR-0012 ("Cloud manifest contract") and ADR-0013 ("Trust flag contract")
are the wire-format and operator-facing CLI contracts respectively.
Both lock the surface so future releases can extend semantics without
breaking existing automation.

#### Per-run reports

- **`feat(report)`** — every run writes two files under
  `.rivet/runs/<run_id>/` (next to `.rivet_state.db`):
  - `summary.json` — machine-readable run report with a stable JSON
    schema (`run_id`, `status`, timing, plan, throughput counters,
    validation / reconciliation verdicts, error message, resume hint,
    manifest verification verdict).
  - `summary.md` — operator-friendly Markdown for pull requests, support
    tickets, and incident reviews.
  - Failures to write are non-fatal (ADR-0001 §I7): the pipeline keeps
    its exit code and the resume hint is still surfaced to stderr even
    when disk-full prevents the report from landing.
- **`feat(cli)`** — the stderr run-summary block is followed by a
  `report:` line pointing at the on-disk Markdown, and (on a failed run
  with at least one committed file) a `resume:` line containing a
  copy-pasteable `rivet run --config <path> --resume` command.

#### Cloud manifest + `_SUCCESS` (ADR-0012)

- **`feat(manifest)`** — every export to a non-streaming destination
  writes:
  - `<dest>/manifest.json` — versioned JSON with `run_id`, `export_name`,
    `started_at`/`finished_at`, `status`, `source.{engine,schema,table}`,
    `destination.{kind,uri}`, `format`, `compression`, `schema_fingerprint`,
    `row_count`, `part_count`, and a `parts[]` array (`part_id`, `path`,
    `rows`, `size_bytes`, `content_fingerprint`, `status`).
  - `<dest>/_SUCCESS` — single-line `xxh3:<16-hex>` body whose value is
    the xxh3 of the just-written `manifest.json` bytes.  An orchestrator
    can poll `_SUCCESS` (cheap GET) to detect manifest changes between
    runs (ADR-0012 §M2).
- **M1/M2 ordering**: parts before manifest; manifest before `_SUCCESS`.
  Both writes use the destination's atomic-PUT path (S3 / GCS) or
  `fs::copy` (local).  `_SUCCESS` is written iff `status: success`.
- **M3 fingerprints**: schema fingerprint (`xxh3` over sorted
  `[{name, type}]`) and per-part content fingerprint (`xxh3` over the
  written bytes) — both `xxh3:<16-hex>` shape, prefix reserved so future
  sha256/blake3 hashers can coexist without a schema break.
- **M4 atomicity**: a given `run_id` produces exactly one manifest;
  resumed runs that complete additional parts write a fresh manifest
  atomically (server-side replace on cloud, `rename` on local) — never
  amended in place.

#### Manifest-aware `--validate` (ADR-0012 §M5/M6, ADR-0013)

- **`feat(validate)`** — `rivet run --validate` now extends the existing
  per-file row-count check with manifest-aware verification:
  - Reads `manifest.json` from the destination.
  - For each committed part: confirms the object exists at the recorded
    `size_bytes`.
  - Verifies `_SUCCESS` body matches `xxh3(manifest.json bytes)`.
  - Surfaces self-consistency violations (declared `row_count` vs
    actual sum, duplicate `part_id`, unsupported `manifest_version`).
  - Lists the prefix and flags untracked surplus objects.
- **M6 legacy fallback**: when no manifest is present at the prefix
  (pre-0.7.0 export), the report carries `legacy_run: true` so an
  operator sees the reduced assurance explicitly — silent fallback is
  forbidden.
- **`feat(rivet validate)`** — new standalone subcommand:
  `rivet validate [--config path] [--export name] [--format pretty|json]`
  re-runs the same M5/M6 checks against an existing destination
  without performing an extraction.  Useful for between-run polling
  (Airflow sensors, CI gating, triage on a suspected-broken dataset).
  Exit 0 when every export passed (or when only legacy_run labels were
  emitted); exit non-zero on any explicit M5 failure.  Failure variants
  in the JSON report carry a `kind` discriminator for stable consumer
  parsing.

#### Manifest-aware `--reconcile`

- **`fix(reconcile)`** — `--reconcile` now compares the source's
  `SELECT COUNT(*)` against the manifest's *cumulative* row total
  (sum of committed parts), not just this run's writes.  Before this
  fix, a resume run that re-exported only a single chunk would falsely
  report MISMATCH because `total_rows` reflected just the resumed
  chunk's rows.  Now: cumulative-vs-source is the correct invariant.

#### Manifest-aware `--resume` (ADR-0012 §M8/M9)

- **`feat(--resume)`** — chunked-checkpoint resume now reconciles state
  with destination: at resume start, read `manifest.json` + listing,
  apply ADR-0012 M8's decision matrix per part:
  - `Skip` (manifest part exists at recorded size) → state row stays
    `completed`, no re-export.
  - `Rewrite` (manifest part missing) → state row reset to `pending`
    so the worker re-exports it.
  - `Quarantine` (size or fingerprint drift) → state row reset, AND
    the divergent destination object is moved to
    `_quarantine/<run_id>/<original-name>` so the active prefix stays
    clean for the new write.
  - Untracked surplus objects (under prefix but not in manifest) are
    quarantined too, never deleted.
- **M9 quarantine** is best-effort per ADR (`Destination::r#move` —
  `fs::rename` on local, server-side rewrite + delete on S3/GCS).
  Failures are logged at WARN and never fatal — a clutter problem
  is never escalated to an extraction failure.
- **`feat(--force)`** — new safety override on `rivet run`.
  Required when `--resume`'s gate refuses to start (destination prefix
  already has `_SUCCESS` from a prior completed run).  Per ADR-0013
  this is the *one* `--force` flag, scoped per-gate; future gates
  reuse the same flag rather than adding `--force-overwrite`,
  `--force-resume`, etc.

#### CLI surface

- **`rivet run`** flags: `--validate`, `--reconcile`, `--resume`,
  `--force` — pinned by ADR-0013 acceptance criterion.  M5/M6/M8/M9
  land entirely under existing flags; no new trust noun on `run`.
- **`rivet validate`** is the only new top-level subcommand.  Standalone
  driver for the manifest-verify flow (ADR-0013 "Subcommand carveouts").
- An anchor test in `tests/trust_artifacts_integration.rs` §24 pins
  the exact flag set on `rivet run --help`; future PRs that add a
  trust-related flag will trip the test until ADR-0013 is amended.

#### Internal: layer hygiene + state schema v8

- The internal SQLite file ledger was renamed from `file_manifest` to
  `file_log` (schema migration v8) to free the `manifest` name for the
  0.7.0 public JSON contract.  Existing 0.6.0 state DBs migrate
  transparently.
- Layer assignments updated in ADR-0003: a new "Trust contract types"
  category covers `manifest.rs`, `pipeline::resume_decisions`, and
  `destination::ObjectMeta` (pure data + pure functions, no L1-L4
  classification).
- `pipeline::finalize` extracted from `pipeline::job` so the four
  end-of-run hooks (manifest write, validate-against-destination, run
  report, notification) sit in one focused module.  ADR-0001 §I8
  (Finalize Order: Manifest → Verification → Report) pins the call
  order so future refactors can't silently re-order observability
  artifacts.
- `pipeline::for_tests` module — public CLI surface vs test-only window
  cleanly separated.  Tests reach internal items via
  `rivet::pipeline::for_tests::*`; the public `rivet::pipeline::*` API
  shrank to just the CLI command drivers + `RunSummary`.
- `RunSummary::stub_for_testing` + chainable setters — one canonical
  builder shared by 7+ test sites.  Adding a field to `RunSummary` now
  costs one default-value entry rather than a 9-place edit.

## 0.6.1 (folded into 0.7.0)

The trust-polish work originally scoped for 0.6.1 (per-run reports,
schema-evidence storage, resume-command hints) ships as part of 0.7.0
above.  Releasing 0.6.1 separately would have left the report
unaware of the manifest, which is most of its operator value.

#### New artifacts

- **`feat(report)`** — every run now writes two files under
  `.rivet/runs/<run_id>/` (placed next to `.rivet_state.db`):
  - `summary.json` — machine-readable run report with a stable JSON schema
    (`run_id`, `status`, timing, plan, throughput counters, validation /
    reconciliation verdicts, error message, resume hint).
  - `summary.md` — operator-friendly Markdown for pull requests, support
    tickets, and incident reviews.
  - Source: `src/pipeline/report.rs`. Failures to write are non-fatal: the
    pipeline keeps its exit code and the resume hint is still surfaced to
    stderr even when disk-full prevents the report from landing.
- **`feat(cli)`** — the stderr run-summary block is now followed by a
  `report:` line pointing at the on-disk Markdown, and (when the run failed
  after committing at least one file) a `resume:` line containing a
  copy-pasteable `rivet run --config <path> --resume` command.

#### Internal: state schema v8 — `file_manifest` → `file_log`

The internal SQLite ledger of files written by an export has been renamed
from `file_manifest` to `file_log`. The name **`manifest`** is reclaimed for
the 0.7.0 cloud-output JSON contract (a separate, public artifact); the
internal log retains the same shape and is migrated automatically on first
open via schema migration v8 (`ALTER TABLE … RENAME TO …`, plus a rename of
the supporting index).

Existing 0.6.0 state DBs are upgraded transparently; no operator action is
required. The Rust module is now `src/state/file_log.rs`; the `FileRecord`
re-export at `rivet::state::FileRecord` is unchanged.

## 0.6.0 (2026-05-19)

### Configuration ergonomics, MySQL parity, pooler-awareness, and a published cross-tool benchmark

A user-facing release. Seven new features (the `table:` config shortcut,
auto-resolved chunk columns from primary keys, memory-budgeted chunking on
both engines, `work_mem`-aware `FETCH` capping on Postgres, a `MysqlProxyKind`
classifier for ProxySQL / MaxScale, and a batch-memory-aware MySQL row
buffer); a packaging change — the MCP server is now shipped as a separate
binary, `rivet-mcp`; and a published cross-tool benchmark harness with a
steelman re-run that gives every competitor its best plausible config.

#### Breaking / packaging

- **`rivet-mcp` is now a separate binary.** Previously available as
  `rivet mcp <…>` subcommands, the MCP (Model Context Protocol) server is
  now its own crate binary under `src/bin/rivet-mcp.rs`. Homebrew, Docker,
  GitHub Release tarballs, and `cargo install rivet-cli` all install both
  `rivet` and `rivet-mcp` from 0.6.0 onwards. Claude Desktop / Claude Code
  configs that previously invoked `rivet mcp` should be updated to call
  `rivet-mcp --stdio` directly. The crate name remains `rivet-cli` on
  crates.io.

#### New features

- **`feat(config)`** — `table:` shortcut + `source.environment`. An export
  can now be expressed as `table: orders` instead of writing a full
  `query: "SELECT ... FROM orders"` block; Rivet derives the column list
  from the catalog and resolves the schema automatically.
  `source.environment` lets a single config carry per-environment defaults
  without YAML anchors.
- **`feat(chunked)`** — `chunk_column` is auto-resolved from the primary
  key on the `table:` shortcut. No more manually copying the PK name into
  the YAML.
- **`feat(chunked)`** — `chunk_size_memory_mb` (memory-budgeted chunk
  sizing) + a small-table escape path that runs sub-threshold tables in a
  single chunk instead of forcing the planner to emit a one-row range.
- **`feat(source)`** — `work_mem`-aware `FETCH` cap on the Postgres server
  side cursor. The longest single SQL statement Rivet issues on a 2 M-row
  wide table is now **0.19s** (`FETCH 142 FROM _rive`) instead of the
  multi-second hold that `FETCH FORWARD 10000` produced under tight
  `work_mem`. Run summary now includes `pg_temp_bytes` so an operator can
  see how much spill the export caused.
- **`feat(mysql)`** — full chunking parity with Postgres: auto-resolved
  `chunk_column` from the PK + `chunk_size_memory_mb` derived from
  `information_schema.TABLES.AVG_ROW_LENGTH` (with a `/3` correction
  above 8 KB to compensate InnoDB's BLOB inflation).
- **`feat(mysql)`** — batch-memory-aware `row_buf` cap. RSS on the wide
  MySQL `content_items` fixture no longer scales with chunk size: peak RSS
  stays at **280 MB** regardless of `chunk_size_memory_mb` setting.
- **`feat(mysql)`** — `MysqlProxyKind` 4-signal classifier identifies
  ProxySQL, MaxScale, and generic multiplexers at connect time and emits
  the pool-safety warning before the export starts (mirrors the existing
  Postgres pgBouncer / Odyssey PID-flip probe). ProxySQL is now wired into
  the nightly live test job.

#### Fixes

- **`fix(source)`** — use the requested `FETCH N` size for the cursor
  exhaustion check on Postgres. Previously the code compared returned-row
  count against the hard-coded default, so adaptive batch sizing could
  declare the cursor exhausted one batch early on tables whose row width
  triggered an early `work_mem` cap.
- **`fix(lib)`** — drop the blanket `#[allow(dead_code)]` on the
  `preflight` module by exposing it as `pub mod`. Eliminates 30+ legitimate
  dead-code warnings that the blanket allow was hiding.
- **`fix(clippy)`** — struct-init shorthand for `TuningConfig` across
  live tests (no behavior change).

#### Refactors

- **`refactor(mcp)`** — extract the MCP server into the `rivet-mcp` binary
  (see "Breaking / packaging" above).
- **`refactor(cli)`** — split the ~1000-line `src/cli/mod.rs` into
  `args.rs` / `validate.rs` / `params.rs` / `dispatch.rs` so each file owns
  a single concern.
- **`refactor(tuning)`** — split the 678-line `src/tuning.rs` into
  `tuning/{profile,memory,adaptive}.rs`. `next_adaptive_batch_size` from
  0.5.3 stays a pure function, now in `adaptive.rs`.
- **`refactor(chunked)`** — split the 661-line `pipeline/chunked/mod.rs`
  into sibling `sequential_checkpoint.rs` + `parallel_checkpoint.rs`,
  with chunk math extracted to `math.rs`. Cuts the largest pipeline file
  by ~75%.
- **`refactor(source)`** — extract Arrow-conversion machinery into
  `source/postgres/arrow_convert.rs` and `source/mysql/arrow_convert.rs`,
  leaving the `mod.rs` files focused on transport / cursor / chunking.
- **`refactor(chunked)`** — drop the subquery wrap on min/max/COUNT
  boundary queries for the `table:` shortcut. Eliminates a planner
  pessimization on tables with a partial index on the chunk column.
- **`refactor(mysql)`** — `AVG_ROW_LENGTH / 3` correction above 8 KB to
  compensate InnoDB BLOB inflation. Wide-MySQL `chunk_size_memory_mb`
  derivations are now within ~10% of measured per-row width.

#### Tests / CI

- **MySQL symmetry suite** — seven new live test files mirror the
  Postgres coverage: `live_mysql_chunked_recovery`,
  `live_mysql_crash_recovery`, `live_mysql_reconcile_repair`,
  `live_mysql_resume`, `live_mysql_retry_and_faults`,
  `live_mysql_schema_drift`, `live_mysql_chunked`. The MySQL twin of
  `live_oltp_load` validates retry / streaming under concurrent INSERTs.
- **`test(chunked)`** — parallel checkpoint recovery (cases C3, C4) +
  panic hooks in the parallel worker so a `panic!()` inside `BatchSink`
  no longer leaks a thread, the run aborts cleanly, and the checkpoint
  state machine is in a resumable state.
- **`ci`** — ProxySQL added to `docker-compose.yaml` (`pool` profile) and
  wired into `.github/workflows/nightly-live.yml` alongside the existing
  pgBouncer coverage.

#### Benchmarks

- **`bench`** — first published cross-tool benchmark harness in
  `docs/bench/`. Compares Rivet against `sling`, `dlt`, `duckdb`
  (`postgres_scanner` / `mysql_scanner`), `clickhouse-local`, and
  `odbc2parquet` 11.0.0 on a 22-table Postgres fixture (incl. a 2M-row ×
  20-wide-column `content_items`) and a 17-table MySQL fixture. Reports
  longest single SQL, peak RSS, wall, failure count, DB-side
  counter deltas (`pg_stat_database`, `Innodb_rows_read`, `processlist`).
  Rivet: PG 0.19s / 443 MB peak, MySQL 9s / 280 MB peak, 0 / 22 + 0 / 17
  table failures.
- **`bench(steelman)`** — second report that re-runs every competitor at
  its best plausible configuration (e.g. `odbc2parquet --batch-size-memory
  256MiB --sequential-fetching`, narrow-table `--column-length-limit`).
  On narrow tables the gap closes substantially; on wide `content_items`
  Rivet's edge survives (~58× peak RSS, ~700× longest single query).
- **`bench(odbc2parquet)`** — numbers refreshed against v11.0.0 (was
  v6.0.7). Wall improved ~15% on the full suite; architectural shape
  unchanged.

#### Docs

- **Trust contracts surface** — `docs/semantics.md § Crash semantics` +
  `§ Known non-guarantees`, `docs/reliability-matrix.md` (PR CI / nightly
  / manual breakdown), `docs/reference/compatibility.md` (exact PG 12–16,
  MySQL 5.7 / 8.0 versions exercised), `SECURITY.md § Sensitive local
  artifacts`, all linked from the README "Trust contracts" table.
- **Onboarding funnel consolidation** — one canonical document per stage:
  `getting-started.md` (first run), `docs/concepts.md` (glossary),
  `docs/pilot/README.md` (operator runbook). Removed parallel
  `pilot/quickstart-postgres.md` + `pilot/quickstart-mysql.md` whose
  content was duplicated from `getting-started.md`.
- **Architecture refresh** — `docs/architecture.md` updated for the
  `tuning/` and `chunked/` splits and the new `rivet-mcp` binary.
- **GIFs** — all 12 default scenarios (and the opt-in `pool-detect`)
  re-rendered against the 0.6.0 binary. New `coalesce-cursor.gif` shows
  `incremental_cursor_mode: coalesce` correctly tracking 30 late-arriving
  rows whose `updated_at` is `NULL`.
- **README** — claims now back-linked to measured numbers (the
  "Source pressure, measured" table is generated from
  `docs/bench/reports/REPORT_*.md`).

---

## 0.5.3 (2026-05-17)

### Architecture audit pass — fault-tolerance, observability, CI hardening

No new user-facing features. All changes are bug fixes, internal refactors,
new unit/CI coverage, and ADR-documented design decisions. The CLI surface
(flags, config schema, output formats) is unchanged.

#### Fixes

- **`fix(postgres)`** — RAII `PgTxnGuard` around the cursor txn. Closes G1
  from the DBA audit: a panic between `BEGIN` and `COMMIT` (e.g. inside a
  `BatchSink::on_batch`) used to leak the open transaction back into the
  pool. The guard's `Drop` now issues a best-effort `ROLLBACK` so the
  connection is returned clean even on unwind. Regression test
  `pg_panic_in_sink_releases_cursor_and_aborts_txn` exercises the panic
  path via `std::panic::catch_unwind`.
- **`fix(adaptive)`** — call `pg_stat_clear_snapshot()` before each
  `checkpoints_req` sample. PostgreSQL caches the stats snapshot at
  transaction start, so every adaptive sample inside the cursor txn was
  returning the frozen value from `BEGIN` time — making the feature blind
  to checkpoints that accumulated during the export. Adaptive batch sizing
  now actually reacts (verified end-to-end: a 200K-row content_items
  export under WAL pressure goes from `batches=40 min=5000 max=5000` to
  `batches=121 min=500 max=5000` with `adaptive: true`).
- **`fix(test)`** — `live_oltp_load.rs` had a long-standing precondition
  bug: `SELECT *` on a `NUMERIC(12,2)` column loses precision metadata in
  the subquery wrap. Tests now declare an explicit `amount` decimal
  override; the property under test (retry/streaming under concurrent
  inserts) is unrelated to NUMERIC inference.

#### Refactors

- **`refactor(retry)`** — `classify_error` now returns the typed
  `RetryClass { Permanent | Transient { needs_reconnect, extra_delay_ms } }`
  instead of a `(bool, bool, u64)` tuple. Positional destructuring made
  the two booleans easy to confuse; named accessors (`is_transient`,
  `needs_reconnect`, `extra_delay_ms`) are now used at every call site.
- **`refactor(source)`** — `Source::export` packs its 5 read-only
  parameters into a named `ExportRequest` struct. Call sites read like
  `ExportRequest { query, incremental, cursor, tuning, column_overrides }`
  instead of relying on positional order.
- **`refactor(journal)`** — `RunJournal`, `RunEvent`, `JournalEntry`, and
  `PlanSnapshot` move out of `pipeline::journal` to a new top-level
  `crate::journal`. This eliminates a layering inversion where
  `state::journal_store` was importing from `pipeline::*` (state →
  pipeline is the wrong direction). The `From<&ResolvedRunPlan>` impl
  moves to `pipeline/summary.rs` beside its sole caller.
- **`refactor(adaptive)`** — `next_adaptive_batch_size` extracted into a
  pure function in `tuning.rs` and shared between `PostgresSource` and
  `MysqlSource`. The shrink/grow decision is now unit-testable without a
  live database (6 unit tests covering floor, ceiling, oscillation
  convergence). `ADAPTIVE_SAMPLE_INTERVAL` and `ADAPTIVE_MIN_BATCH`
  promoted to public constants in `tuning`.
- **`refactor(sink)`** — `pipeline/sink.rs` converted to `pipeline/sink/`
  with `extract_last_cursor_value` and its 11 Arrow-type tests moved into
  `pipeline/sink/cursor.rs`. `sink/mod.rs` drops from 1525 → 1295 LOC.

#### Performance

- **`perf(chunked)`** — replace the busy-wait worker semaphore in
  `pipeline/chunked/exec.rs` (atomic + 50ms sleep loop) with a
  `Mutex<usize> + Condvar`-backed `resource::Semaphore`. Blocked
  acquirers now park in the kernel until a worker calls `release()`,
  instead of polling 20×/sec per blocked thread.

#### Tests

- **22 new unit tests** covering the previously-zero-coverage checkpoint
  state machine (`ensure_chunk_checkpoint_plan` 5-transition matrix +
  `record_chunked_commit` boundary advancement), the duplicate-write
  guard in `run_with_reconnect` (`decide_export_retry` 6-case matrix),
  the chunked quality gate (`run_chunked_quality_gate` 7 cases),
  adaptive batch sizing math, and `mcp::ascii_table` formatter.
- Coverage delta on critical pipeline files:
  - `pipeline/chunked/mod.rs`: 0% → 37.26% line coverage
  - `pipeline/single.rs`: 41% → 48.53%
  - `pipeline/job.rs`: 21.80% → 56.83%
  - Total: 66.78% → 69.73%

#### CI / Operational

- **`ci`** — live tests now run on every PR. Swap the upstream-yanked
  `bitnami/pgbouncer:latest` for `edoburu/pgbouncer:latest` in
  `docker-compose.yaml`, bring up pgBouncer in the per-PR `e2e` job, and
  filter the heavy 50K+-row `content_export` tests out of e2e (they ran
  for minutes and required a separate fixture). The full live suite
  including `content_load` runs nightly via the new
  `.github/workflows/nightly-live.yml` workflow (03:30 UTC +
  `workflow_dispatch`). 151 live tests now gate every merge to `main`.

#### Docs

- **ADR-0010** — "Two parallel execution engines": documents the
  in-process `thread::scope` engine (chunked-single-table) vs the
  subprocess engine (`parallel_children`) as a deliberate split, with
  the conditions that would trigger unification.
- **ADR-0011** — "`Source: Send` not `Sync`": records that the
  `Mutex<Client>` prototype produced a measured 1.7× slowdown on a
  4-thread chunked export, so per-worker connections remain the right
  shape for blocking SQL drivers.

---

## 0.5.2 (2026-05-15)

### Wave 2 live E2E test suite — full CLI flag coverage with behavioral assertions

56 live integration tests across 5 new test files. Every CLI flag now has at least
one end-to-end test that verifies observable behavior, not just exit code.

#### New test files

- **`tests/live_init.rs`** (I1–I5) — schema-wide discovery, single-table init,
  `--out` flag, MySQL init, unreachable-URL error message.
- **`tests/live_init_extended.rs`** (IE1–IE5) — `--source-env`, `--source-file`,
  `--schema`, `--discover` JSON artifact, unset env var error.
- **`tests/live_plan_apply.rs`** (PA-L1–PA-L8) — plan+apply round-trips (full +
  chunked), credential redaction, staleness gate, `--force`, missing plan file,
  `--param` substitution with DB row-count verification.
- **`tests/live_reconcile_repair.rs`** (RR1–RR6 + RR4b) — reconcile pretty/JSON/file
  output, repair dry-run, `--execute`, `--format json`, and `--report` flag
  exercising the precomputed reconcile JSON path.
- **`tests/live_cli_flags.rs`** (31 tests) — `rivet run`, `rivet check`,
  `rivet doctor`, `rivet state`, `rivet metrics`, `rivet journal`, `rivet completions`.

#### Assertion hardening (previously "conscious compromise")

All 8 previously weak behavioral assertions replaced with concrete checks:

| Test | Before | After |
|---|---|---|
| `run_reconcile_flag_exits_zero_when_counts_match` | exit 0 only | `total_rows==25` in JSON + `"MATCH"` in stderr |
| `check_json_flag_outputs_type_report_as_json` | parse first `{` line | parse full `ExportTypeReport`; verify export name, column names, zero violations |
| `check_param_flag_substitutes_in_query` | exit 0 only | `--json` verifies column discovery + zero violations |
| `check_type_report_shows_column_table` | `contains("id") \|\| contains("name")` | `contains("int8")` AND `contains("exact")` |
| `state_chunks_shows_checkpoint_table` | output text only | DB query: `COUNT(chunk_task WHERE status='completed') == 2` |
| `metrics_last_flag_limits_output` | fragile `!starts_with("EXPORT")` filter | run_id–based: newer present, older absent |
| `journal_shows_run_summary` | `"success" \|\| table.name()` | two `&&` assertions: both required |
| `check_mysql_basic_exits_zero` | `\|\| contains("pass")` matched "password" | removed; asserts `contains(table.name())` only |

#### Source fixes uncovered during test authoring

- `src/init/postgres.rs`: `::regclass` cast → `to_regclass()` for graceful
  table-disappear handling during schema-wide discovery.
- `src/init/mod.rs`: `introspect_all` skips "not found or has no columns" errors
  gracefully instead of aborting the entire init.

---

## 0.5.1 (2026-05-15)

### Stabilization: correctness tests, benchmark evidence, best-practice docs

This release closes the v0.5.x stabilization roadmap. No new features; all
changes are tests, documentation, and benchmark infrastructure.

#### Tests added

- **`tests/gremlin.rs`** (G1–G5) — live fault tests covering `row_count_min`
  on empty tables, exhausted incremental cursors, `unique_max_entries` cap
  warning, `auto_shrink` + quality gate correctness, and crash-before-quality-check
  recovery.
- **`tests/live_chunked_recovery.rs`** (C1–C2) — chunked pipeline crash+resume
  matrix. C1: crash after `complete_chunk_task` → resume skips completed chunk,
  no duplicates. C2: crash after file written but before commit → chunk 0 stuck
  in `running` → resume resets and re-runs it (at-least-once delivery documented).
- **Chunk-level fault injection** (`src/test_hook.rs`, `src/pipeline/chunked/mod.rs`)
  — new `maybe_panic_at_chunk("after_chunk_file", N)` and
  `maybe_panic_at_chunk("after_chunk_complete", N)` hooks, matching
  `RIVET_TEST_PANIC_AT=after_chunk_file:0` etc.

#### Benchmark evidence

Full Phase 2 benchmark suite run against live Postgres. Report:
[docs/benchmark_report_v0.5.x.md](docs/benchmark_report_v0.5.x.md).

Key results (measured, not estimated):

| Claim | Result |
|---|---|
| `balanced` vs `none`: same wall time, 2.6× smaller output | ✓ confirmed |
| `compact` adds no compression improvement over `balanced` on numeric data | ✓ confirmed |
| Smaller row group targets cut RSS with zero wall-time cost | ✓ confirmed |
| Memory policy cap: zero overhead when cap doesn't trigger | ✓ confirmed |
| `auto_shrink` on wide real-world table: 5.7× RSS reduction (878 → 154 MB) | ✓ confirmed |
| xxHash3-64 uniqueness tracking: < 1 MB overhead on 200K-row table | ✓ confirmed |

#### Benchmark infrastructure fixes

- Bench configs used `${VAR:-default}` bash syntax unsupported by Rivet's env
  interpolation; replaced with plain `${VAR}`.
- Integer fields (`target_row_group_mb`, `max_batch_memory_mb`) were quoted in
  YAML, causing `invalid type: string` parse errors; removed quotes so values
  interpolate as YAML integers.
- `run_bench.sh`: added `--export` flag support; introduced `BENCH_RUN_OUT`
  per-scenario output dir so each run writes to a clean, isolated path;
  added output-dir cleanup before each scenario to prevent file accumulation.

#### Documentation

All v0.5.x best-practice guides are complete:

- [Resource-aware extraction](docs/best-practices/resource-aware-extraction.md)
- [Parquet tuning](docs/best-practices/parquet-tuning.md)
- [Compression profiles](docs/best-practices/compression-profiles.md)
- [Quality checks](docs/best-practices/quality-checks.md)
- [Low-memory runners](docs/best-practices/low-memory-runners.md) — numbers measured, not estimated
- [Recovery and resume](docs/best-practices/recovery-and-resume.md)
- [Benchmark methodology](docs/best-practices/benchmark-methodology.md)

README `Resource-aware extraction` section added. `rivet plan` memory estimate
documented as advisory heuristic in `docs/reference/cli.md`.

---

## 0.5.0 (2026-05-15)

### Parquet row group auto tuning

A new `parquet:` block on any Parquet export controls how many rows Rivet places in each row group. Row group size affects memory usage during write, compression ratio, and downstream read performance (predicate pushdown, column skipping).

```yaml
exports:
  - name: events
    format: parquet
    parquet:
      row_group_strategy: auto      # auto | fixed_rows | fixed_memory
      target_row_group_mb: 128
      max_row_group_mb: 256
```

The `auto` strategy estimates row width from Arrow schema column types and computes rows-per-group to fit within the target memory budget. Narrow tables (IDs, timestamps) get large groups; wide tables (TEXT/JSON) get smaller groups automatically — no manual tuning required.

| Strategy | Behavior |
|---|---|
| `auto` | Compute from schema + `target_row_group_mb` (default 128 MB) |
| `fixed_rows` | Use `row_group_rows` as a literal row count |
| `fixed_memory` | Same math as `auto`, explicit label in logs |

When `parquet:` is omitted, Rivet uses the library default (1,048,576 rows/group). `rivet plan` shows the selected strategy and target when the block is present.

### Compression profiles

A new `compression_profile` field replaces the need to manually choose a codec and level. Set it on any export and Rivet picks the right `(codec, level)` pair:

| Profile | Codec | Best for |
|---|---|---|
| `none` | uncompressed | Debug / scratch |
| `fast` | snappy | Backfills, low-CPU |
| `balanced` | zstd level 3 | Production default |
| `compact` | zstd level 9 | Storage/network-sensitive |

```yaml
exports:
  - name: events
    format: parquet
    compression_profile: balanced
```

`compression_profile` takes precedence over `compression` + `compression_level`. Existing configs are unaffected — those fields still work.

### Batch memory hard cap (`max_batch_memory_mb`)

A new tuning parameter adds a **batch-level** memory guard independent of the process-level `memory_threshold_mb`:

```yaml
tuning:
  max_batch_memory_mb: 128
  on_batch_memory_exceeded: warn   # warn (default) | fail | auto_shrink
```

Rivet measures the actual Arrow buffer footprint of each batch using `get_array_memory_size()`. When a batch exceeds the limit:

- **`warn`** — logs the actual size, the limit, and a suggested `batch_size`. Export continues.
- **`fail`** — returns an error immediately. Use in CI to block oversized batches.
- **`auto_shrink`** — splits the batch in half recursively until each sub-batch fits, then writes them individually. Transparent to the rest of the pipeline — row count and output are identical.

The warning includes an actionable suggestion:

```
batch memory 184 MB exceeds max_batch_memory_mb=128 MB (5000 rows). Consider lowering batch_size to ~3478.
```

### Resource plan output

`rivet plan` now shows a **Resources** section in pretty output with per-batch memory estimates:

```
  Resources:
    Batch size   :  10,000 rows
    Batch memory : ~2 MB (narrow) – ~95 MB (wide)
    RSS guard    : 4,096 MB
    Throttle     : 50 ms between batches
```

The narrow/wide bounds bracket the expected per-batch memory at ~200 B/row and ~10 KB/row respectively. A `⚠` advisory appears when the upper bound exceeds 128 MB/batch, suggesting `batch_size_memory_mb` or a lower `batch_size`. No database connection is required to compute the estimate — it is derived from tuning settings alone.

### Quality uniqueness: typed hashing and memory cap

`unique_columns` quality checks now use **typed xxHash3-64** instead of string formatting. Numeric and binary columns are hashed directly from raw bytes — no intermediate string allocation. CPU overhead for quality-enabled runs is ~2.6–2.8× lower on Int64 and Utf8 columns.

A new **`unique_max_entries`** field caps the number of distinct values tracked per column:

```yaml
quality:
  unique_columns: [id, email]
  unique_max_entries: 1000000
```

When the cap is reached, a `Warn` issue is emitted and tracking stops for that column. Without this field, tracking is still unbounded — the cap is opt-in. This prevents uncontrolled RAM growth on high-cardinality columns (UUIDs, event IDs, email addresses) on very large tables.

### `rivet init` scaffolds `parquet:` auto-tuning

`rivet init` now includes a `parquet:` block in the generated YAML for every chunked export and any full-mode export estimated at more than 100 k rows. The block is pre-filled with the right strategy and a schema-aware target:

- **Narrow tables** (fewer than 5 text/JSON/bytea columns) → `target_row_group_mb: 128`
- **Wide tables** (5 or more text/JSON/bytea columns) → `target_row_group_mb: 64`

```yaml
# generated for content_items (2M rows, body + raw_html + metadata jsonb + …)
  - name: content_items
    mode: chunked
    format: parquet
    parquet:
      row_group_strategy: auto
      target_row_group_mb: 64
```

No action needed for existing configs — the block is only added to newly generated scaffolds.

### Fix: `--resume` with no checkpoint exits non-zero

Previously, running `rivet run --resume` on a chunked export that had no in-progress checkpoint would log a warning and silently start a fresh run. This masked operator mistakes (wrong `--config`, post-`reset-chunks` double-resume). The command now exits non-zero with an actionable message:

```
error: export 'big_table': --resume but no in-progress chunk checkpoint;
       run without --resume first or `rivet state reset-chunks --config <cfg> --export big_table`
```

---

## 0.4.0 (2026-05-14)

PostgreSQL state backend, type safety layer, destination path templates, and a set of reliability fixes.

### PostgreSQL state backend

`StateStore` now supports PostgreSQL as an alternative to the default SQLite file. Set `RIVET_STATE_URL` to a PostgreSQL connection string to activate:

```bash
export RIVET_STATE_URL=postgresql://rivet:rivet@localhost:5433/rivet_state
rivet run --config rivet.yaml
```

All state tables — cursor, metrics, manifest, schema drift, shape drift, chunk checkpoints, progression, run journal, run aggregate — are created automatically on first connect via versioned `PG_MIGRATIONS`. The same schema version sequence (`v1`–`v7`) is enforced for both backends; the migration runner verifies the final version after each run and cleans up superseded version rows.

**Parallel chunk workers** open their own short-lived connections per `claim` / `complete` / `fail` operation so they do not contend on a shared connection.

**Security:** passwords are redacted from log and error messages (`postgresql://user:***@host/db`). A `WARN` is emitted when connecting to a non-localhost host without TLS, prompting the use of `sslmode=require`.

A dedicated `postgres-state` service is included in `docker-compose.yaml` (port 5433) for local development.

### Type safety layer (`rivet check --type-report`)

New flags on `rivet check` surface the full type pipeline for every column in a query:

- `--type-report` — prints a table: column name, source native type, Rivet type, Arrow type, and fidelity level.
- `--strict` — exits non-zero if any column mapping is `lossy` or `unsupported`.
- `--json` — emits the report as newline-delimited JSON (one object per export); pipe-friendly.
- `--target bigquery` — adds `Target type` / `Status` columns showing BigQuery mapping (`NUMERIC`, `BIGNUMERIC`, `TIMESTAMP`, `REPEATED …`) with `ok` / `warn` / `fail` and inline notes for edge cases.

**Type fidelity levels:**

| Level | Meaning |
|---|---|
| `exact` | Round-trips without loss |
| `compatible` | Structurally compatible; minor representation difference |
| `logical_string` | Serialised to STRING/text — no native Arrow type available |
| `lossy` | Precision or range reduction |
| `unsupported` | No mapping; column is skipped in Parquet output |

**Column type overrides (`columns:`)** — per-column override block in the export YAML pins a decimal type when the inferred type is wider than needed:

```yaml
exports:
  - name: orders
    columns:
      amount: decimal(15,4)
```

**Complex types — Postgres and MySQL:**

- **Enum** (`pg: enum`, `mysql: ENUM/SET`) — written as `Utf8`.
- **Interval** (`pg: INTERVAL`) — written as `Utf8` ISO 8601 string (`P1Y2M3D`, `PT0S`).
- **Arrays / lists** (`pg: _text`, `_int8`, etc.) — written as Arrow `List(inner_type)`; BigQuery `REPEATED <inner>`.
- **MySQL TIME/TIME2** — written as `Time64(Microsecond)`.

**Unsupported-column errors** now collect all unmappable columns before returning (previously failed on the first one).

### `rivet run --json`

Prints the run aggregate summary as JSON to stdout after the run completes. Useful for CI pipelines and scripted post-processing. Compatible with `--summary-output` (both can be used together).

```bash
rivet run --config rivet.yaml --json | jq '.total_rows'
```

### Destination path templates

`path` (local) and `prefix` (S3/GCS) fields now support placeholders substituted at plan-build time:

| Placeholder | Value |
|---|---|
| `{date}` | UTC date as `YYYY-MM-DD` |
| `{export}` | Export name from config |
| `{table}` | Alias for `{export}` |

```yaml
destination:
  type: s3
  bucket: my-data
  prefix: exports/{date}/{export}/
```

### `rivet state reset-chunks --stuck-checkpoints`

Clears chunk checkpoint rows for every export in the config that still has `chunk_run.status = 'in_progress'` — a single command to recover from a crash or SIGKILL that left multiple exports stuck. Alias: `--failed`. Exports whose chunk run completed normally are skipped. Names present in state but removed from the YAML are skipped with a printed note.

```bash
rivet state reset-chunks --config rivet.yaml --stuck-checkpoints
rivet run --config rivet.yaml --resume
```

### `rivet init` — unbounded DECIMAL scaffolding

Plain `numeric` / `decimal` columns without `(p,s)` in the DDL now scaffold as `decimal(38,18)` (runs without manual editing), with a `# REVIEW:` marker on each affected line, a `# NOTE:` in the file header, and a `rivet: note` on stderr when writing `-o <file>`. Previously only a commented-out TODO line was emitted.

### RunJournal persistence

`RunJournal` is now persisted to the state database at the end of every export run (state DB migration **v7**: `run_journal` table). `store_journal`, `load_journal`, and `recent_journals` are available for auditing and future `rivet journal` CLI commands.

### Postgres NUMERIC catalog hints

For simple single-table `SELECT … FROM rel` queries, `rivet` now looks up `NUMERIC(p,s)` precision and scale from `information_schema` at export time. This resolves decimal types automatically without requiring hand-written `columns:` overrides in the common case.

### Changed

- **`rivet run --resume`** — when `--resume` is specified but no in-progress chunk checkpoint exists (e.g. after `reset-chunks` already cleared it), the run now starts fresh with a warning instead of exiting with an error. The previous workflow `reset-chunks && rivet run --resume` now works correctly.
- **`rivet init` unbounded DECIMAL** — see above; `decimal(38,18)` + `# REVIEW:` replaces the old commented-out `# TODO` line.

### Fixed

- **`memory_threshold_mb` defaults** — `balanced` profile now defaults to 4096 MB and `safe` to 2048 MB (was 0 = disabled). Prevents unbounded RSS growth on wide-row tables without any config change.
- **Retry safety guard** — transient-error retry fails fast when `dest.write()` already succeeded in a previous attempt. Prevents silent duplicate-row writes on retry.
- **PG state backend — migrate_pg** — migration runner now verifies the final schema version after migration and cleans up superseded version rows (parity with SQLite behaviour).
- **PG state backend — password in logs** — PostgreSQL URLs are redacted in all log and error messages. Passwords containing `@` are handled correctly via `rfind('@')`.
- **`chunk_count` validation** — `chunk_count: 0` is now rejected at plan-build time (would have caused a division by zero at run time). `chunk_count` combined with `chunk_dense: true` or `chunk_by_days` is also rejected (the options are mutually exclusive; previously `chunk_count` was silently ignored when either of the other two was set).
- **MySQL `--parallel-exports` connection exhaustion** — the `mysql` crate's default connection pool eagerly opens `min=10` connections per pool. With many exports running simultaneously each pool was created at startup, causing `Too many connections` when N×10 exceeded MySQL's `max_connections` (default 151). The pool is now configured with `min=1` so connections are opened lazily on demand.

### Performance

- **BufWriter 256 KB** — temp-file write buffer increased from 8 KB to 256 KB (32× fewer `write` syscalls per batch). −37% User CPU on a 1.5 M-row benchmark.
- **Inline cursor extraction** — `ExportSink` extracts the cursor value inline and frees all column buffers immediately after `on_batch` returns. Saves one full batch worth of RAM during post-run state writes.
- **`insert_chunk_tasks` batch transaction** — chunk task initialisation wraps all INSERTs in a single SQLite transaction (one WAL sync). Eliminates quadratic I/O on large chunk counts.
- **Quality unique tracking** — `HashSet<String>` → `HashSet<u64>` (xxh3-64). Eliminates one heap-allocated `String` per non-null row per tracked column in the hot path.

### Compatibility

- `RIVET_STATE_URL` is optional; SQLite remains the default.
- No YAML config changes required for any of the above features.
- `columns:` overrides are optional; Parquet output is unchanged without them.

---

## 0.3.5 (2026-04-30)

Polish release on top of 0.3.4: every multi-export mode (`--parallel-exports`
threads and `--parallel-export-processes` children) now renders the same live
cards UI, end-of-run summaries collapse to a single compact block instead of
seven lines per export, the chunked retry path no longer leaves stale progress
bars or zombie child processes behind, and a few rough edges around recovery
hints, `database is locked`, and transient OpenDAL errors are gone. No YAML
config changes; no on-disk artifact changes; `--single-export` and sequential
runs print exactly as before.

### `--parallel-exports` (threads) shares the cards UI

`--parallel-exports` previously rendered one `indicatif::MultiProgress` bar
per chunk worker, which interleaved with per-export `RunSummary` blocks and
left "ghost" 0/N bars behind on retries. 0.3.5 replaces that path with the
same `parent_ui::run_ui` renderer used by `--parallel-export-processes`:

- A single `mpsc::Sender<UiMessage>` is installed in `pipeline::ipc` for the
  duration of the run; chunked workers and `RunSummary::print` route their
  events (`Started`, `ProgressInit`, `Progress`, `Finished`) through that
  channel instead of writing directly to stderr.
- A dedicated UI thread drains the channel and redraws the card stack — one
  card per export — exactly as in `--parallel-export-processes`.
- Chunked progress bars under multi-export runs are `ProgressDrawTarget::hidden()`;
  `ChunkProgress` now implements `Drop` and calls `finish_and_clear()` on
  scope exit so that retries (a fresh `ChunkProgress` per attempt) don't
  accumulate orphaned bars in the renderer.

Sequential / single-export runs still draw a normal `indicatif` bar to
stderr — the cards path only activates when more than one export is in
flight.

### Compact end-of-run summaries

`RunSummary::print` used to emit a 7-line block per export (header, run_id,
status, mode, tuning, batch_size, metrics). Under multi-export mode that
turned a 15-export run into ~100 lines of scrollback even on success.
0.3.5:

- Sets a `MULTI_EXPORT_MODE` flag for the duration of any multi-export
  invocation; while it's set, `RunSummary::print` short-circuits to a new
  `render_compact()` path that produces a single status-icon + one-line
  summary per export.
- The aggregate `Run summary` block (`pipeline::aggregate::print`) strips
  per-export recovery hints (`run rivet state reset-chunks --export …`)
  from individual error messages and prints them once, consolidated, in a
  single `Recovery` section at the bottom.
- Long error messages are clamped to a fixed character budget; in
  particular, `parallel checkpoint worker errors` (which previously dumped
  every failed chunk's GCS URL into the summary) collapse to a single
  cause-line via `summary::compact_error` /
  `summary::summarize_parallel_chunk_errors`.

### Recovery hints always include `--config`

`rivet state reset-chunks --export <name>` and the `--resume` suggestions
emitted by `pipeline::chunked` previously omitted the `--config <path>`
flag, so copy-pasting them straight from the error message failed for
anyone whose YAML wasn't named `rivet.yaml`. Both error paths now go
through a shared `config_hint(config_path)` helper and emit fully-qualified
commands.

### `database is locked` on first-time `--parallel-export-processes`

Spawning N children against a brand-new `state.db` raced N copies of the
v1 schema migration on an exclusive lock; the loser printed
`migration v1 failed: database is locked` and exited. Fixed in two
places:

- `pipeline::run_exports_as_child_processes` now opens `StateStore` once
  in the parent before spawning children, so the migration runs exactly
  once and every child sees a fully-migrated DB.
- The ad-hoc connections opened by `state::checkpoint` for parallel chunk
  workers go through a new `open_connection` helper that applies
  `journal_mode=WAL` and `busy_timeout = 10_000` per connection. Brief
  contention on `chunk_task` / `export_metrics` now waits up to 10 s
  instead of surfacing `SQLITE_BUSY` immediately.

### No more zombie children in `htop`

`--parallel-export-processes` previously reaped children sequentially
(`for child in children { child.wait()?; }`), so on a 15-export config the
14 fastest children sat as defunct entries until the slowest one finished.
0.3.5 spawns one dedicated reaper thread per child (`rivet-reap-<name>`)
and joins them in any order; each child is `wait()`ed within milliseconds
of its actual exit. Visible improvement: the long stack of
`rivet --export …` rows in `htop` / Activity Monitor disappears as soon
as each card flips to its final metrics.

### OpenDAL transient retries (GCS + S3)

GCS occasionally returns `dispatch task is gone: runtime dropped the
dispatch task` and other `(temporary)` errors mid-stream; before 0.3.5 a
single hiccup tore down the chunk worker and forced a fresh connection +
re-fetch from Postgres. Two layers of fixes:

- `destination::gcs` / `destination::s3` now wrap the async `Operator`
  with `opendal::layers::RetryLayer::new().max_times(5).min_delay(200ms)
  .max_delay(10s).jitter()` before converting to `blocking::Operator`.
  Transient HTTP failures retry inside OpenDAL with no chunk-worker
  involvement.
- `pipeline::retry::classify_error` recognises `(temporary)` and
  `dispatch task is gone` / `runtime dropped the dispatch task` as
  transient-but-non-reconnecting, with a 500 ms extra delay. The chunk
  worker keeps its existing connection and retries the chunk write
  instead of falling back to the outer reconnect loop.

### Documentation

- `docs/gifs/chunked-progress.gif` and `docs/gifs/parallel-cards.gif` were
  re-recorded against 0.3.5 binaries so the demos reflect the new compact
  summary, the cleaned-up parallel cards (no interleaving, no ghost
  bars), and the consolidated recovery section.

### Compatibility

- No YAML config changes. `rivet check` / `rivet plan` behaviour
  unchanged.
- Sequential / `--single-export` runs print exactly as before.
- `--parallel-exports` and `--parallel-export-processes` produce strictly
  cleaner output; scripts that scraped the per-export `RunSummary` block
  from stderr should already be on `RIVET_IPC_EVENTS=1` + `Finished`
  events on stdout (stable since 0.3.4).

## 0.3.4 (2026-04-27)

Multi-process parallel exports get a real UI: `--parallel-export-processes`
now renders one card per export with a live progress bar, ETA, and rows, and
in-place final metrics — instead of four child processes' output stomping on
each other in the terminal. No YAML changes, no behavioural change for
single-process or `--parallel-exports` runs.

![Parallel cards UI](docs/gifs/parallel-cards.gif)

### `rivet run --parallel-export-processes` — parent-side cards UI

`--parallel-exports` runs every export as a thread in one process; it has
worked since 0.2 but shares a global allocator, a single connection pool, and
a single set of progress bars across exports. `--parallel-export-processes`
spawns one child `rivet` per export instead — full memory and connection
isolation — but until 0.3.4 each child wrote its own progress bar to stderr,
which interleaved badly with the others.

0.3.4 introduces a small NDJSON IPC protocol between parent and children and
a hand-rolled ANSI renderer in the parent that owns the screen for the
duration of the run:

- Children spawned with `RIVET_IPC_EVENTS=1` emit four event kinds on stdout
  — `Started`, `ProgressInit`, `Progress`, `Finished` — with `serde_json`,
  one event per line. Children also suppress their own progress bars
  (`ProgressDrawTarget::hidden()`) and skip the per-export stderr
  `RunSummary` block — that data is carried by `Finished` instead, so the
  parent has all the metrics without scraping logs.
- The parent reader thread per child decodes events with `serde_json` and
  forwards them through an `mpsc::Sender<UiMessage>`. A single dedicated UI
  thread drains the channel, redraws the card stack on every event, and on a
  200 ms idle timer so elapsed-time / ETA fields keep ticking even when no
  IPC arrives.
- Each card is seven lines: a header (`── orders ─────…`), five fixed-width
  meta lines (`run_id`, `status`, `mode`, `tuning`, `batch_size`), and a
  bottom line that is either a live progress bar with ETA or, once
  `Finished` arrives, the export's final metrics in place. Cards stay in
  scrollback after the run; below them the existing aggregate `Run summary`
  block prints exactly once.
- If a child's stdout closes without a `Finished` event (crash, OOM,
  `SIGKILL`), the parent marks the card `failed` with a synthetic warning
  line. The parent never silently loses an export.

The renderer is implemented as raw ANSI escape sequences (`\x1b[nA`, `\r`,
`\x1b[2K`) instead of `indicatif::MultiProgress` — same observable output in
real terminals, but deterministic across `vhs` / `ttyd` recordings and CI
pipes (where the cursor controls become harmless no-ops). See
`src/pipeline/parent_ui.rs` for the rationale and the corner cases handled.

### `Destination` is now `Send + Sync`

To make the cards UI possible, `Destination` instances are now shared across
threads via `Arc<dyn Destination + Send + Sync>` instead of being moved per
job. Every built-in destination (`local`, `s3`, `gcs`, `bigquery`) was
already thread-safe; the bound is added explicitly so out-of-tree
destinations get a clear compile-time error rather than a runtime
data-race.

### Documentation

- New `docs/gifs/parallel-cards.gif` (1280 × 780, ~30 s) recorded against a
  4-export Postgres fixture (`orders`, `users`, `events`, `sessions`,
  ~210 k rows total) seeded by `docs/gifs/render.sh`. The tape is
  reproducible from a clean Docker Compose stack — `./docs/gifs/render.sh
  parallel-cards`.
- `docs/reference/cli.md`: new `--parallel-export-processes — one card per
  export` subsection embedding `parallel-cards.gif`, with the seven-line
  card layout and a short note on the IPC protocol.
- `docs/gifs/README.md` lists the new scenario alongside the existing ten.

### Internals

- `src/pipeline/ipc.rs` — `ChildEvent` enum + `emit()` + `ipc_events_enabled()`.
- `src/pipeline/parent_ui.rs` — `UiMessage`, `Renderer`, `CardState`,
  cursor-positioned ANSI redraw loop. ~12 unit tests cover header padding,
  meta-line vertical alignment, progress-bar endpoints, number / duration
  formatting, and synthetic-failure rendering.
- `src/pipeline/progress.rs` — `ChunkProgressHandle` cloneable handle that
  emits `Progress` IPC events on each `inc()` and falls back to the visible
  `indicatif` bar when IPC is off.
- `src/pipeline/summary.rs` — `RunSummary::new` emits `Started`,
  `RunSummary::print` emits `Finished` and short-circuits the stderr block
  under `RIVET_IPC_EVENTS=1`.

### Security

- **RUSTSEC-2026-0104** — bumped transitive `rustls-webpki` 0.103.12 → 0.103.13.
  The advisory describes a reachable panic when parsing CRL extensions with a
  syntactically valid empty `BIT STRING` in the `onlySomeReasons` element of
  an `IssuingDistributionPoint`, before the CRL signature is verified. Rivet
  itself does not consume CRLs directly, but the bump closes the audit
  warning end-to-end (advisory pulled in via `rustls 0.23.38` →
  `hyper-rustls`/`tokio-rustls`/`rustls-platform-verifier` → `reqwest` /
  `opendal`). `cargo audit` is now clean across all 480 dependencies.

### Compatibility

- No YAML config changes. `rivet check` / `rivet plan` behaviour unchanged.
- `--parallel-exports` (single-process, multi-thread) prints exactly as
  before.
- `--parallel-export-processes` previously had garbled per-export output
  with no aggregate summary; the new behaviour is strictly better but the
  on-screen layout is different. Scripts that scraped the per-child
  `RunSummary` block from stderr should switch to `RIVET_IPC_EVENTS=1` +
  `Finished` events on stdout — the format is documented in
  `src/pipeline/ipc.rs` and stable for 0.3.x.

## 0.3.3 (2026-04-19)

QA test matrix + panic-safety follow-up. No YAML config deserializes
differently; no export artifact format changes. Three latent panics in the
pipeline are now graceful; four config combinations that used to be accepted
silently are now rejected at validation time (see **Fail-fast validation**
below — not a YAML-format break, but configs that relied on silent
acceptance will surface the error they were always hiding).

### Panic-safety fixes

Surfaced by the new fuzz suites (`tests/planner_fuzz.rs`,
`tests/format_fuzz.rs`) and by `src/pipeline/chunked/math.rs::mod tests`:

- **`generate_chunks` near `i64::MAX`** — `start + chunk_size - 1` overflowed
  and panicked when the cursor column reached the BIGINT upper bound. Fixed
  with saturating arithmetic and an explicit exit when `end == i64::MAX`.
  (`src/pipeline/chunked/math.rs`)
- **`build_time_window_query` on `days_window: u32::MAX`** — naive
  `now - Duration::days(u32::MAX as i64)` walks back ~12 million years and
  falls outside chrono's representable range. Replaced with
  `Duration::try_days().and_then(checked_sub_signed)` saturating at
  `DateTime::MIN_UTC`. (`src/plan/mod.rs`)
- **CSV writer on pathological `Date32` values** — `NaiveDate + Duration::days(i64)`
  panicked for values near `i32::MAX` (roughly 1.5 million years from
  1970-01-01). Uses `checked_add_signed` with a fallback to an empty cell
  (matching the null-cell convention already used by this writer).
  (`src/format/csv.rs`)

None of these reachable from sensible input, but each was a panic surface the
fuzz suites turned into deterministic regression tests.

### Fail-fast validation

`Config::validate` now rejects four combinations that previously parsed
successfully but produced broken runs at execution time:

- empty `exports: []` — used to be a silent no-op that looked like success
  in schedulers;
- duplicate export names — would silently share `export_state` /
  `file_manifest` / `chunk_run` rows (all keyed by name);
- `mode: chunked` with `parallel: 0` — zero workers never claim a task, so
  the run hung forever;
- `mode: chunked` with `chunk_size: 0` — before the saturating fix in
  `generate_chunks`, this was an infinite loop at the planner level.

All four come with actionable error messages that name the offending field.
Configs that hit any of these never produced usable output — this is a
fail-fast, not a breaking change.

### QA test matrix — `rivet_qa_backlog_v2.md` + `docs/reference/testing.md`

The full coverage of QA backlog tasks is now shipped as automated tests. See
the new `docs/reference/testing.md` for the file-level map and
`rivet_qa_backlog_v2.md`'s footer for the task-level status.

- **1096 offline tests** across 21 integration files + inline unit modules.
  Runs on every PR via `cargo test`.
- **46 live tests** across 10 `tests/live_*.rs` binaries exercising the full
  `rivet` binary against the docker-compose stack (Postgres 16 + MySQL 8 +
  MinIO + fake-gcs + Toxiproxy). Gated by `#[ignore]`; activated with
  `cargo test -- --ignored`.
- Shared harness in `tests/common/mod.rs`:
  - `require_alive(service)` — fast TCP reachability probe with actionable
    failure messages;
  - RAII `PgTable` / `MysqlTable` guards for per-test unique tables;
  - `unique_name(prefix)` — PID + atomic counter naming for race-free
    parallel runs;
  - Minimal Toxiproxy admin client over raw `TcpStream` (no blocking
    tokio runtime);
  - Cross-process `flock(2)` lock on `$TMPDIR/rivet_qa_toxiproxy.lock` so
    cargo's parallel integration binaries do not race on the shared admin
    API;
  - `ensure_minio_bucket` / `ensure_gcs_bucket` idempotent bucket creation.

### Test-only fault-injection hook

New env-var-driven hook in `src/test_hook.rs` reads `RIVET_TEST_PANIC_AT`
once at startup and panics at one of four named pipeline boundaries if the
value matches: `after_source_read`, `after_file_write`,
`after_manifest_update`, `after_cursor_commit`. The `tests/live_crash_recovery.rs`
suite injects each one, asserts the observable post-crash state, then
re-runs without the injection and asserts recovery produces the expected
final state. Zero overhead when unset (one relaxed atomic load per call).

See `dev/CRASH_MATRIX.md` for the boundary table and the invariant windows
(ADR-0001 I2–I4).

### Slack / webhook internals

- Extracted `build_slack_payload` and `should_notify` from `maybe_send` as
  `pub(crate)` pure functions so contract tests can pin payload shape
  without spinning up `reqwest`'s blocking client. Field marker test
  guarantees notifications include only `export_name` / `status` /
  `total_rows` / `duration_ms` / `error_message` — not `tuning_profile`,
  `format`, `mode`, or `compression` (guards against accidental over-
  exposure in webhooks).
- Mock HTTP receiver based on `std::net::TcpListener` exercises the
  webhook path end-to-end for `200 OK`, `429`, `500`, and "no trigger
  matched — no connection made" cases.

### CI

`.github/workflows/ci.yml`:

- `e2e` job now also starts `toxiproxy` (was only postgres/mysql/minio/fake-gcs);
- new step runs `cargo test --release -- --ignored` after the bash E2E
  script, wiring the entire live suite into branch-gate CI.

### Docs

- `docs/reference/testing.md` — new offline + live matrix reference.
- `docs/README.md` — reference link to the above.
- `dev/CRASH_MATRIX.md` — automated coverage section with the env-var hook
  boundary table.
- `rivet_roadmap.md` — Epic G (Toxiproxy) and Epic H (fault-injection hook)
  flipped from ⏳ to ✅ with evidence.

### Internal refactoring

- Collapsed `tests/v2_golden.rs` (5-in-1 mixed file) into focused domain
  files: `tests/time_window.rs`, `tests/resource_smoke.rs`, with
  validate / chunk parsing tests folded into
  `tests/validate_regression.rs` and `src/config/tests.rs`.

---

## 0.3.2 (2026-04-18)

SecOps audit follow-up plus an expanded database-version compatibility matrix.
No YAML config is deserialized differently; no export artifact changes format.
The one behavior change that could bite an existing config is the strict
environment-variable resolver — see **Breaking changes** below.

### Transport security — optional TLS for source connections

New `source.tls` block (defaults to plaintext so existing configs are
unchanged). Four modes, matching libpq semantics where it makes sense:

```yaml
source:
  type: postgres
  url_env: DATABASE_URL
  tls:
    mode: verify-full         # disable | require | verify-ca | verify-full
    ca_file: /etc/ssl/certs/rds-ca-2019-root.pem
    accept_invalid_certs: false
    accept_invalid_hostnames: false
```

- Postgres via `postgres-native-tls` (new dep).
- MySQL via `OptsBuilder::ssl_opts` (existing `mysql` crate).
- When `tls` is absent Rivet emits one startup warn-level log — `credentials
  and result rows cross the network in plaintext` — so operators aren't
  silently on NoTls in prod.
- On Linux, `native-tls` statically links vendored OpenSSL, so
  `cargo install rivet-cli` works on a bare Ubuntu / Alpine / CI runner
  without `libssl-dev`. macOS uses `SecureTransport` — no OpenSSL.

### SecOps audit fixes

Addresses the findings from the credential-leak review (see commit 08c16f2).

- **Cursor values are no longer string-interpolated into SQL.** MySQL binds
  the value via `?`; Postgres embeds it via a dedicated `E'…'` literal
  helper (`escape_pg_literal`) that escapes both `'` and `\` regardless of
  `standard_conforming_strings`. Injection-attempt tests (`'; DROP TABLE …`,
  `O'Brien`, backslash payloads) now live in `src/source/query.rs`.
- **AWS_PROFILE env mutation is serialized.** Parallel exports with
  differing `aws_profile` values no longer race — a `static Mutex` plus an
  RAII guard scopes the override and restores the previous value on drop.
- **Secrets wrapped in `zeroize::Zeroizing<String>`:** AWS access/secret
  keys, GCS `client_secret` / `refresh_token` / OAuth POST body, and the
  resolved DB password. Heap buffers are zeroed on drop.
- **GCS 4xx response body no longer propagates into anyhow errors.**
  Google's `/token` endpoint occasionally echoes back `client_id` /
  `client_secret` on failure; this was landing in `summary.error_message`
  (SQLite) and Slack notifications.
- **`executing query: …` lowered from `info` to `debug`** in both source
  drivers, so `RUST_LOG=info` (the common CI / docs setting) no longer
  surfaces SQL text that can contain `${VAR}`-expanded secrets.

### `rivet init` — credentials off the command line

New `--source-env <ENV_VAR>` and `--source-file <PATH>` accept the DB URL
from an env var or a one-line file, keeping the credential out of shell
history, `ps aux`, and `/proc/<pid>/cmdline`. Mutually exclusive with the
existing `--source <URL>`, which stays supported for local dev.

```
rivet init --source-env DATABASE_URL --table orders -o orders.yaml
rivet init --source-file ~/.rivet/source.url --schema public -o pg_all.yaml
```

### Database version compatibility matrix

The full end-to-end suite now runs against every supported server version:

| Engine     | Versions                   |
|------------|----------------------------|
| PostgreSQL | 12, 13, 14, 15, 16         |
| MySQL      | 5.7, 8.0                   |

Legacy servers opt in via `docker compose --profile legacy up -d`. Ports
5412 / 5413 / 5414 / 5415 for PG 12–15; 3357 for MySQL 5.7 (amd64 platform
on Apple Silicon). Run the full matrix with:

```
docker compose up -d postgres mysql minio fake-gcs
docker compose --profile legacy up -d \
    postgres-12 postgres-13 postgres-14 postgres-15 mysql-57
cargo build --release --bin rivet --bin seed
bash dev/legacy/run_full_matrix.sh
```

Seven targets × 83 e2e assertions = 581 per-version checks, all green.
`dev/e2e/*.yaml` and `dev/fixtures/test_*.yaml` now read their URL from
`RIVET_PG_URL` / `RIVET_MYSQL_URL` (with localhost defaults) so the same
suite re-targets cleanly without YAML edits.

MySQL 5.7 compat notes (full details in `docs/reference/compatibility.md`):

- `dev/mysql/init_57.sql` ships a view-free init because MySQL 5.7 lacks
  window functions (`ROW_NUMBER() OVER (…)`); `seed` auto-detects 5.x
  and skips the corresponding `CREATE VIEW`.
- Probe uses bash `/dev/tcp/127.0.0.1/<port>` rather than `mysqladmin` —
  Homebrew's `mysql-client@9` dropped the `mysql_native_password` plugin
  library, so modern macOS clients can't connect to 5.7 servers, but the
  Rust `mysql` crate (which Rivet depends on) still can.

### Breaking changes

- **Unset `${VAR}` in config is now a hard error.** Previously
  `resolve_vars` silently substituted an empty string, so
  `postgres://u:${DB_PASS}@host/db` with `DB_PASS` unexported would
  become `postgres://u:@host/db` — a silent auth-bypass footgun. The
  loader now refuses with a clear message. An explicit empty value
  (`export DB_PASS=""`) is still accepted. Migrate by either setting the
  variable or removing the reference; add an explicit empty assignment
  only when you really mean "no password".

### Docs

- New `docs/reference/compatibility.md` — version-support policy, test
  matrix, engine-specific notes (window functions, arm64 emulation, auth
  plugins). Linked from `docs/README.md` and `docs/getting-started.md`.

### Verification

- `cargo test --lib` — 411 passed, 0 failed.
- `cargo test --tests` (serial, live DBs) — 970 passed, 0 failed.
- `dev/legacy/run_full_matrix.sh` (7 targets × 83) — 581 passed, 0 failed.
- `dev/legacy/run_legacy.sh` (compat smoke) — 44 passed, 0 failed.
- Demo pipelines — 12 PG + 8 MySQL exports succeeded.

**Total: 2026 assertions, zero failures.**

---

## 0.3.1 (2026-04-18)

Security patch release — closes five advisories raised against `v0.3.0` by
`rustsec/audit-check@v2` (CI security-audit job).

### Fixed by upgrade

- **RUSTSEC-2026-0098** — `rustls-webpki` name constraints for URI names were
  incorrectly accepted. `rustls-webpki` 0.103.10 → 0.103.12.
- **RUSTSEC-2026-0099** — `rustls-webpki` name constraints accepted for
  wildcard DNS names (similar to CVE-2025-61727). Same bump.
- **RUSTSEC-2025-0119** — `number_prefix` unmaintained. Fixed by bumping
  `indicatif` 0.17 → 0.18, which migrated to the maintained `unit-prefix`.
  `number_prefix` is no longer in `Cargo.lock`.
- **RUSTSEC-2026-0097** (×3) — `rand` unsound under custom `log::Log` that
  calls `rand::rng()` in the reseed path. Fixed by the transitive bump
  `rand` 0.8.5 / 0.9.2 / 0.10.0 → 0.8.6 / 0.9.4 / 0.10.1.

### Policy

`.cargo/audit.toml` rewritten with explicit reachability analysis for each
remaining entry. Policy:

> An advisory stays here **only** if (a) there is no upstream fix **and**
> (b) the vulnerable code path is not reachable from Rivet's runtime.

Ignore list is now down to two entries — `RUSTSEC-2023-0071` (rsa timing
sidechannel; GCS JWT signing only; not practically exploitable for a batch
CLI) and `RUSTSEC-2024-0436` (`paste` unmaintained; `proc-macro = true`
crate, zero runtime code). Each is documented in-file with the exact
preconditions that would trigger the advisory and a pointer to the upstream
fix to track.

### Other changes

- Bulk transitive `cargo update` (rustls 0.23.37 → 0.23.38, tokio 1.51.0 →
  1.52.1, wasm-bindgen family, webpki-roots, uuid, lru, pkg-config, misc.).
- No direct-dependency API changes — all crates in `Cargo.toml` are already
  at the latest stable major/minor.

### Verification

- `cargo build --release --bin rivet --bin seed` — clean.
- `cargo test --lib` — 411 passed, 0 failed.
- `cargo audit` — 479 crates scanned, 0 advisories after ignore.

---

## 0.3.0 (2026-04-18)

Source-aware planning release — Epics A–I plus credential-hardening follow-ups.

### New features

- **Source-aware extraction prioritization** (Epic A, [ADR-0006](docs/adr/0006-source-aware-prioritization.md)) — `rivet plan` now emits per-export `priority_score` / `priority_class` / `cost_class` / `risk_class` / `recommended_wave` plus explainable `reasons[]`. Multi-export plans embed a full campaign view with ordered exports, grouped waves, and `source_group` collision warnings. Pretty and JSON output.
- **Metadata-driven discovery** (Epic B) — `rivet init --discover -o out.json` emits a machine-readable artifact with ranked cursor + chunk candidates, nullability, total bytes, and automatic `coalesce` fallback hints when the best cursor is NULL-able.
- **Composite cursor** (Epic D, [ADR-0007](docs/adr/0007-cursor-policy-contracts.md)) — `incremental_cursor_mode: coalesce` progresses on `COALESCE(primary, fallback)` for tables with a nullable `updated_at`. Synthetic `_rivet_coalesced_cursor` column is stripped before Parquet/CSV write (CC5). Single-level SQL with outer `ORDER BY` so the last batch carries the max coalesced value (CC6).
- **Partition / window reconciliation** (Epic F, [ADR-0009](docs/adr/0009-reconcile-and-repair-contracts.md)) — new `rivet reconcile -c <cfg> -e <export>` re-counts every chunk partition on the source and emits a structured `ReconcileReport`. Requires `chunk_checkpoint: true` (v1).
- **Committed / verified progression** (Epic G, [ADR-0008](docs/adr/0008-export-progression.md)) — new schema v4 migration for `export_progression` table plus `rivet state progression` command that surfaces both boundaries per export.
- **Targeted repair** (Epic H) — `rivet repair -c <cfg> -e <export> [--report rec.json] [--execute]` derives a `RepairPlan` from a `ReconcileReport` and re-runs only the flagged chunk ranges. New files land alongside originals; committed boundary is not re-stamped (RR4).
- **Historical recommendation refinement** (Epic I) — `rivet plan` folds the last ~20 rows of `export_metrics` into scoring with bounded contribution (≤ ~15 points combined). Adds `high_retry_rate_history`, `recent_failure_history`, `slow_history` reason kinds.

### Security / hardening

- **PA9 — Artifact Credential Redaction** ([ADR-0005](docs/adr/0005-plan-apply-contracts.md#pa9--artifact-credential-redaction-acr)) — `PlanArtifact::new` silently strips plaintext `password:` and rewrites `scheme://user:pass@…` → `scheme://REDACTED@…` before serialization. Operators see a WARN log so they know to migrate to `password_env:` / `url_env:`.
- Composite-cursor coalesce mode: `@` inside path/query strings is no longer confused with userinfo (dedicated path-aware parser).

### Configuration additions

- `exports[].source_group: <string>` — logical shared-source label driving campaign-level warnings (Epic A).
- `exports[].reconcile_required: bool` — advisory flag feeding into prioritization risk class (Epic C).
- `exports[].cursor_fallback_column`, `exports[].incremental_cursor_mode` — composite cursor policy (Epic D).

### CLI additions

- `rivet reconcile`
- `rivet repair`
- `rivet state progression`
- `rivet init --discover`

### Demo assets

- `demo/setup_demo_tables.sql` + `demo/setup_demo_tables_mysql.sql` — reproducible 7-table fixture with varied scale (500 → 800k rows), nullable-cursor coalesce cases, and shared-source collisions.
- `demo/demo_pipeline.yaml` / `demo_pipeline_mysql.yaml` — 12/8-export showcase configs with `source_group` declarations.
- `docs/pilot/demo-quickstart.md` — 10-minute scripted pilot demo (discovery → plan → apply → reconcile → repair → verified).

### Documentation

- New ADRs: 0006 (prioritization), 0007 (cursor policy), 0008 (progression), 0009 (reconcile & repair).
- New pilot guide: `docs/pilot/pilot-walkthrough.md` end-to-end on user data; `docs/pilot/demo-quickstart.md` for the pre-seeded demo.
- New reference: `docs/modes/incremental-coalesce.md`.
- Moved planning working docs into `docs/planning/`.
- PRODUCT.md: v5 (plan/apply) + v6 (Epics A–I) + v6.1 (SecOps) sections.

### Bug fixes

- `ExportSink` was stripping the synthetic coalesce cursor column before capturing `last_batch`/`schema`, so incremental cursors never advanced in `coalesce` mode. Fixed by keeping the raw (with-synthetic) batch + schema for cursor extraction and building a stripped `dest_batch`/`dest_schema` on the fly for the file writer.
- `on_schema` with an empty schema (zero-row runs) no longer errors when attempting to strip a missing synthetic column.

### Tests

- `cargo test --lib`: 411 passed.
- `cargo test --tests`: 434 passed.
- Integration suites (chunked_sparse_ids, format_golden, invariants, journal_invariants, recovery, retry_integration, schema_evolution, v2_golden, validate_regression): 125 passed.
- Zero failures, zero warnings.

---

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

- [docs/reference/init.md](docs/reference/init.md) — full `rivet init` guide (Docker Compose, `dev/scripts/regenerate_docker_init_configs.sh`)
- [docs/reference/cli.md](docs/reference/cli.md) — `rivet init` command table
- [docs/getting-started.md](docs/getting-started.md) — optional “Scaffold with rivet init” step
- [README.md](README.md) — CLI quick reference; product vs `rivet-cli` crate name; chunked progress + `dev/scenarios/chunked_postgres_bench.yaml` example
- [USER_GUIDE.md](USER_GUIDE.md) — `rivet init` optional section
- [docs/modes/chunked.md](docs/modes/chunked.md) — progress bar (independent of `RUST_LOG`), bench config examples
- [docs/README.md](docs/README.md) — `rivet init` and chunked progress pointers
- E2E: `dev/e2e/run_e2e.sh` section 16 — `rivet init` against docker-compose Postgres/MySQL

### Fixes

**PostgreSQL `query_scalar` and date-based chunking**

`MIN`/`MAX` on `timestamp` / `timestamptz` / `date` columns were not decoded in `PostgresSource::query_scalar` (only numeric and plain text types were). That returned a false empty result and broke **`chunk_by_days`** (and any path using time-column scalars). Chrono types are now formatted to strings that `parse_date_flexible` accepts.

### Other

- `Cargo.toml` `[package] exclude` moved from invalid `[[bin]]` key to `[package]`
- `dev/scripts/regenerate_docker_init_configs.sh` — populates gitignored `dev/init_generated/` locally from docker-compose schemas
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
