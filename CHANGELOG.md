# Changelog

## 0.7.7 (unreleased) — Audit-Gap Closure

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
