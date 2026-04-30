# Changelog

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
`dev/e2e/*.yaml` and `dev/test_*.yaml` now read their URL from
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
