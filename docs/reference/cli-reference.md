# Command-Line Help for `rivet`

This document contains the help content for the `rivet` command-line program.

**Command Overview:**

* [`rivet`↴](#rivet)
* [`rivet run`↴](#rivet-run)
* [`rivet check`↴](#rivet-check)
* [`rivet doctor`↴](#rivet-doctor)
* [`rivet cdc`↴](#rivet-cdc)
* [`rivet state`↴](#rivet-state)
* [`rivet state show`↴](#rivet-state-show)
* [`rivet state reset`↴](#rivet-state-reset)
* [`rivet state files`↴](#rivet-state-files)
* [`rivet state reset-chunks`↴](#rivet-state-reset-chunks)
* [`rivet state chunks`↴](#rivet-state-chunks)
* [`rivet state progression`↴](#rivet-state-progression)
* [`rivet completions`↴](#rivet-completions)
* [`rivet init`↴](#rivet-init)
* [`rivet plan`↴](#rivet-plan)
* [`rivet apply`↴](#rivet-apply)
* [`rivet repair`↴](#rivet-repair)
* [`rivet validate`↴](#rivet-validate)
* [`rivet reconcile`↴](#rivet-reconcile)
* [`rivet metrics`↴](#rivet-metrics)
* [`rivet schema`↴](#rivet-schema)
* [`rivet schema config`↴](#rivet-schema-config)
* [`rivet schema cli`↴](#rivet-schema-cli)
* [`rivet journal`↴](#rivet-journal)

## `rivet`

Export data from databases to files

**Usage:** `rivet [OPTIONS] <COMMAND>`

Getting started (the happy path):
  1. rivet init     scaffold a config from your database
  2. rivet doctor   test source + destination auth
  3. rivet check    column-type & schema report
  4. rivet run      export your data

Docs: https://github.com/panchenkoai/rivet/blob/main/docs/getting-started.md

###### **Subcommands:**

* `run` — Run export jobs defined in config
* `check` — Column-type & schema report for each export (needs a working connection; run `doctor` first if it can't connect)
* `doctor` — Verify source + destination auth/connectivity (run this first)
* `cdc` — Stream change data capture (CDC) from a source's transaction log
* `state` — Manage export state
* `completions` — Generate shell completions
* `init` — Generate a config scaffold from a live database (connect + introspect)
* `plan` — Generate an execution plan artifact (no data exported)
* `apply` — Execute a sealed plan artifact, or run a config's exports wave-by-wave
* `repair` — Targeted repair of chunks flagged by reconcile: emit a repair plan, or re-export only mismatched ranges
* `validate` — Re-run manifest-aware verification against an existing destination, no extraction
* `reconcile` — Partition/window reconciliation: re-count per-partition on source and report mismatches. Requires a chunked export previously run with `chunk_checkpoint: true`. Exits non-zero when a mismatch is detected, so CI / orchestrators can gate on it (an `unknown` partition warns but does not fail)
* `metrics` — Show export metrics history
* `schema` — Emit machine-readable schemas for Rivet's data contracts
* `journal` — Inspect structured run journal (events, files, retries, quality issues)

###### **Options:**

* `--json-errors` — Output errors as {"error":"..."} JSON to stderr; useful for machine-readable orchestration



## `rivet run`

Run export jobs defined in config

**Usage:** `rivet run [OPTIONS] --config <CONFIG>`

###### **Options:**

* `-c`, `--config <CONFIG>` — Path to YAML config file
* `-e`, `--export <EXPORT>` — Run only a specific export by name
* `--validate` — Validate output files after writing
* `--reconcile` — Row-count audit: run COUNT(*) on the source and compare with the exported row count; a mismatch fails the run. Implies `--validate` (also verifies the output file manifest)
* `--resume` — Resume a chunked export with `chunk_checkpoint: true` (same query/chunk_column/chunk_size)
* `--force` — Override safety gates that would otherwise refuse the run.

   Today: with `--resume`, allows starting against a destination prefix whose `_SUCCESS` marker is already present.  Without `--force`, resume against an already-complete run refuses, so an operator cannot accidentally re-export over a verified dataset.
* `--parallel-exports` — Run all exports from the config concurrently (ignored with `--export`; needs 2+ exports)
* `--parallel-export-processes` — Run each export as a separate `rivet` child process (parallel; true per-export peak RSS; more overhead than threads)
* `--summary-output <PATH>` — Write the run aggregate summary as JSON to this file (in addition to .rivet_state.db)
* `--json` — Print the run aggregate summary as JSON to stdout at the end of the run
* `-p`, `--param <KEY=VALUE>` — Query parameter: key=value (repeatable, substitutes ${key} in queries)



## `rivet check`

Column-type & schema report for each export (needs a working connection; run `doctor` first if it can't connect)

**Usage:** `rivet check [OPTIONS] --config <CONFIG>`

###### **Options:**

* `-c`, `--config <CONFIG>` — Path to YAML config file
* `-e`, `--export <EXPORT>` — Check only a specific export by name
* `-p`, `--param <KEY=VALUE>` — Query parameter: key=value (repeatable, substitutes ${key} in queries)
* `--type-report` — Show per-column type fidelity report (source type → Rivet type → Arrow type)
* `--strict` — Fail with non-zero exit code if any column has an unsafe type mapping
* `--json` — Output type report as JSON (implies --type-report)
* `--target <TARGET>` — Check compatibility against a target warehouse (e.g. bigquery)



## `rivet doctor`

Verify source + destination auth/connectivity (run this first)

**Usage:** `rivet doctor [OPTIONS] --config <CONFIG>`

###### **Options:**

* `-c`, `--config <CONFIG>` — Path to YAML config file
* `--json` — Emit the probe results as a JSON object (`{config_path, all_ok, checks: [{name, ok, detail?, hint?}]}`) instead of the text report



## `rivet cdc`

Stream change data capture (CDC) from a source's transaction log.

The engine is chosen from the URL scheme: `mysql://` (binlog), `postgresql://` (logical slot), `sqlserver://` (change tables), or `mongodb://` (change stream). Emits one JSON object per row change to stdout (NDJSON) and, with `--checkpoint`, persists a resume position; `--output` writes typed Parquet/CSV instead. Per-engine prerequisites (ROW binlog + REPLICATION grant, `wal_level=logical`, enabled CDC, a replica set) are in docs/reference/cdc.md. The fuller, config-driven path is `rivet run` with `mode: cdc`.

**Usage:** `rivet cdc [OPTIONS] <--source <SOURCE>|--source-env <ENV_VAR>|--source-file <PATH>>`

###### **Options:**

* `--source <SOURCE>` — Database URL — `postgresql://`, `mysql://`, `sqlserver://`, or `mongodb://` (engine chosen from the scheme). Visible in `ps`; prefer `--source-env`/`--source-file` outside local dev
* `--source-env <ENV_VAR>` — Name of an environment variable holding the database URL
* `--source-file <PATH>` — Path to a file containing just the database URL (one line)
* `--server-id <SERVER_ID>` — Replica server-id for the binlog connection (must be distinct from the source's and any other replica)

  Default value: `4271`
* `--checkpoint <PATH>` — Persist/resume the binlog position to this file. Omit to tail from the current position without checkpointing
* `--table <TABLE>` — Only emit changes for this table (repeatable; default: all tables)
* `--max-events <N>` — Stop after N change events (default: stream until interrupted)
* `--output <DIR>` — Write typed Parquet/CSV files to this directory (the upsert/after-image shape) instead of NDJSON to stdout. Requires exactly one `--table` — its schema is resolved from the source
* `--format <FORMAT>` — Output file format when `--output` is set: `parquet` (default) or `csv`

  Default value: `parquet`
* `--rollover <N>` — Rows per output file (rollover) when `--output` is set. Larger ⇒ fewer, bigger files but more drain memory (the PostgreSQL peek reads a part's worth per batch: memory is O(rollover)). Turn it up/down per workload

  Default value: `100000`
* `--slot <NAME>` — PostgreSQL logical slot name (CDC; created if absent)

  Default value: `rivet_slot`
* `--capture-instance <INSTANCE>` — SQL Server CDC capture instance, e.g. `dbo_orders` — required for `sqlserver://` sources
* `--until-current` — Catch up to the source's current end and exit, instead of streaming indefinitely — the bounded "read to now and stop" model, ideal for a scheduler. For MySQL this is a non-blocking binlog dump; PostgreSQL / SQL Server already drain their backlog and exit



## `rivet state`

Manage export state

**Usage:** `rivet state <COMMAND>`

###### **Subcommands:**

* `show` — Show current state for all exports
* `reset` — Reset state for an export
* `files` — Show file manifest (files produced by exports)
* `reset-chunks` — Clear persisted chunk checkpoint rows (`chunk_run` / `chunk_task`)
* `chunks` — Show chunk checkpoint status for an export
* `progression` — Show committed / verified export boundaries (the last fully-exported cursor position)



## `rivet state show`

Show current state for all exports

**Usage:** `rivet state show [OPTIONS] --config <CONFIG>`

###### **Options:**

* `-c`, `--config <CONFIG>`
* `--json` — Emit the incremental-cursor state as a JSON array to stdout instead of the text table. Empty → `[]`



## `rivet state reset`

Reset state for an export

**Usage:** `rivet state reset --config <CONFIG> --export <EXPORT>`

###### **Options:**

* `-c`, `--config <CONFIG>`
* `-e`, `--export <EXPORT>` — Export name to reset



## `rivet state files`

Show file manifest (files produced by exports)

**Usage:** `rivet state files [OPTIONS] --config <CONFIG>`

###### **Options:**

* `-c`, `--config <CONFIG>`
* `-e`, `--export <EXPORT>` — Show files for a specific export
* `-l`, `--last <LAST>` — Number of recent files to show

  Default value: `50`
* `--json` — Emit the file list as a JSON array to stdout (CI completeness checks) instead of the text table. Empty → `[]`



## `rivet state reset-chunks`

Clear persisted chunk checkpoint rows (`chunk_run` / `chunk_task`)

**Usage:** `rivet state reset-chunks [OPTIONS] --config <CONFIG>`

###### **Options:**

* `-c`, `--config <CONFIG>`
* `-e`, `--export <EXPORT>` — Export whose chunk checkpoints should be cleared (same as `chunk_checkpoint` runs)
* `--stuck-checkpoints` [alias: `failed`] — Reset checkpoints for **every export named in this config** that currently has `chunk_run.status = 'in_progress'` (crash, SIGKILL, stale concurrent worker).

   Ignores exports whose latest chunk run already finished (`completed`). Runs listed in the database but removed from the YAML are skipped with a printed note.

   Alias `--failed` refers to "checkpoint state stuck", not HTTP-style failures or metric rows.



## `rivet state chunks`

Show chunk checkpoint status for an export

**Usage:** `rivet state chunks [OPTIONS] --config <CONFIG> --export <EXPORT>`

###### **Options:**

* `-c`, `--config <CONFIG>`
* `-e`, `--export <EXPORT>`
* `--json` — Emit the checkpoint (run header + per-chunk tasks) as a JSON object to stdout instead of the text table. No checkpoint → `null`



## `rivet state progression`

Show committed / verified export boundaries (the last fully-exported cursor position)

**Usage:** `rivet state progression [OPTIONS] --config <CONFIG>`

###### **Options:**

* `-c`, `--config <CONFIG>`
* `-e`, `--export <EXPORT>` — Show progression for a specific export



## `rivet completions`

Generate shell completions

**Usage:** `rivet completions <SHELL>`

###### **Arguments:**

* `<SHELL>` — Shell to generate completions for

  Possible values: `bash`, `elvish`, `fish`, `powershell`, `zsh`




## `rivet init`

Generate a config scaffold from a live database (connect + introspect)

**Usage:** `rivet init [OPTIONS] <--source <SOURCE>|--source-env <ENV_VAR>|--source-file <PATH>>`

###### **Options:**

* `--source <SOURCE>` — Database URL (postgresql://, mysql://, or sqlserver://). Visible in shell history / `ps`; prefer `--source-env` or `--source-file` for anything other than local dev
* `--source-env <ENV_VAR>` — Name of an environment variable holding the database URL (e.g. DATABASE_URL). The URL never touches the command line
* `--source-file <PATH>` — Path to a file containing just the database URL (one line). Credentials stay on disk instead of entering the process command line
* `--table <TABLE>` — Single table, optionally schema-qualified (e.g. public.orders, dbo.orders). Omit to emit all tables/views in a Postgres/SQL Server schema or MySQL database
* `--schema <SCHEMA>` — PostgreSQL: schema to export (default public). SQL Server: schema (default dbo). MySQL: database name if missing from the URL, or override URL database
* `--include <GLOB>` — Whole-schema only: keep only tables/views matching this glob (`*`/`?`). Repeatable; a table is kept if it matches any `--include`. No `--include` = keep all
* `--exclude <GLOB>` — Whole-schema only: drop tables/views matching this glob (`*`/`?`). Repeatable; `--exclude` wins over `--include`
* `-o`, `--output <OUTPUT>` — Write output to this file instead of stdout
* `--discover` — Emit a machine-readable JSON discovery artifact instead of a YAML scaffold. Includes row estimates, size bytes, ranked cursor candidates, chunk candidates, and advisory notes. Mutually exclusive with the YAML-only `--gcs-bucket` / `--s3-bucket` flags
* `--mode <MODE>` — Override the suggested extraction mode for every scaffolded export. `cdc` scaffolds a change-data-capture export (mode: cdc + a cdc: block with engine-specific stream params) instead of a batch query. Other values (full / incremental / chunked / time_window) just override the auto-suggested mode
* `--gcs-bucket <NAME>` — Scaffold `destination: type: gcs` with this bucket (each export gets `prefix: exports/<table>/`). Incompatible with `--s3-bucket` and `--discover`
* `--gcs-credentials-file <PATH>` — Optional path for `credentials_file:` on GCS scaffolds. Omit entirely to use ADC (`gcloud auth application-default login`) or `GOOGLE_APPLICATION_CREDENTIALS` — no key in YAML
* `--s3-bucket <NAME>` — Scaffold `destination: type: s3` with this bucket (each export gets `prefix: exports/<table>/`). Incompatible with `--gcs-bucket` and `--discover`
* `--s3-region <REGION>` — Optional AWS region for S3 scaffolds (when using `--s3-bucket`)



## `rivet plan`

Generate an execution plan artifact (no data exported)

**Usage:** `rivet plan [OPTIONS] --config <CONFIG>`

###### **Options:**

* `-c`, `--config <CONFIG>` — Path to YAML config file
* `-e`, `--export <EXPORT>` — Plan only a specific export by name
* `-p`, `--param <KEY=VALUE>` — Query parameter: key=value (repeatable)
* `-o`, `--output <OUTPUT>` — Write plan JSON to this file (default: print summary to stdout)
* `--format <FORMAT>` — Output format: "pretty" (human summary) or "json" (machine-readable)

  Default value: `pretty`

  Possible values:
  - `pretty`:
    Human-readable summary printed to stdout
  - `json`:
    Pretty-printed JSON (written to --output file or stdout)




## `rivet apply`

Execute a sealed plan artifact, or run a config's exports wave-by-wave

**Usage:** `rivet apply [OPTIONS] <PLAN_FILE>`

###### **Arguments:**

* `<PLAN_FILE>` — A plan JSON artifact from `rivet plan` (sealed single-export replay), OR a YAML config (`.yaml`/`.yml`) to run its exports wave-by-wave in ascending `wave:` order — the wave each export was assigned by `rivet plan`

###### **Options:**

* `--parallel-export-processes` — Run the cheap (low-cost) exports within each wave concurrently, as separate processes (same as `parallel_export_processes: true` in the config). Config-wave mode only; heavier exports — which already chunk-parallelize internally — still run one at a time
* `--resume` — Config-wave mode: skip exports a prior run already completed (`_SUCCESS` present) and resume incomplete chunked exports from their checkpoints, so a re-run after a partial failure does not redo finished tables. Independent tables are never re-exported
* `--force` — Skip staleness check (allow plans older than 24 h)



## `rivet repair`

Targeted repair of chunks flagged by reconcile: emit a repair plan, or re-export only mismatched ranges

**Usage:** `rivet repair [OPTIONS] --config <CONFIG> --export <EXPORT>`

###### **Options:**

* `-c`, `--config <CONFIG>` — Path to YAML config file
* `-e`, `--export <EXPORT>` — Export name to repair (must be `mode: chunked`)
* `--report <REPORT>` — Path to a reconcile JSON report produced by `rivet reconcile --format json`. Omit to run reconcile in-process against the latest chunk run
* `--execute` — Actually re-export the affected chunks. Without this flag, the plan is printed and nothing is executed
* `--format <FORMAT>` — Output format for plan / report

  Default value: `pretty`

  Possible values: `pretty`, `json`

* `-o`, `--output <OUTPUT>` — Write plan / report JSON to this file (with `--format json`)
* `-p`, `--param <KEY=VALUE>` — Query parameter: key=value (repeatable)



## `rivet validate`

Re-run manifest-aware verification against an existing destination, no extraction.

The same file-manifest checks `rivet run --validate` performs at end-of-run, exposed as a standalone command for between-run polling and triage.  Reads manifest.json + _SUCCESS at the destination, head-checks every committed part for presence and recorded size_bytes.  Source is not queried — use `rivet reconcile` for a source-vs-export row audit.

By default `validate` resolves the destination prefix the same way `run` does — `{date}` becomes today's UTC date.  Use `--date`, `--run-id`, or `--prefix` to point at a prior run instead of today.

**Usage:** `rivet validate [OPTIONS] --config <CONFIG>`

###### **Options:**

* `-c`, `--config <CONFIG>` — Path to YAML config file
* `-e`, `--export <EXPORT>` — Validate only this export (default: every export in the config)
* `--format <FORMAT>` — Output format: "pretty" (human summary) or "json" (machine-readable)

  Default value: `pretty`

  Possible values: `pretty`, `json`

* `--depth <DEPTH>` — How deep to verify: "light" (manifest + _SUCCESS only, no prefix listing), "sample" (light + part reconcile + untracked surplus), or "full" (sample + the value-checksum re-read of every part).

   `full` is the default and matches the pre-graded behaviour. Use `light` for a fast "is this a complete, marked run?" poll, or `sample` for full structural verification without downloading parts.

  Default value: `full`

  Possible values:
  - `light`:
    Manifest read + self-consistency + `_SUCCESS` only (no prefix listing)
  - `sample`:
    Light + part reconcile + untracked surplus (one `list_prefix`)
  - `full`:
    Sample + the Form B value-checksum re-read (downloads parts)

* `-o`, `--output <OUTPUT>` — Write JSON report to this file (only with `--format json`)
* `--date <YYYY-MM-DD>` — Resolve `{date}` to this ISO-8601 day (e.g. `2026-05-21`) instead of today.

   Use when a run that landed on a prior day's prefix needs to be re-verified — without this flag `validate` looks at today's resolved prefix and reports "no manifest" for yesterday's data.
* `--run-id <RUN_ID>` — Substitute `{run_id}` in the destination template with this value.

   Composes with `--date`.  Has no effect if the template does not contain `{run_id}`.
* `--prefix <PREFIX>` — Skip placeholder resolution entirely and verify exactly this prefix.

   Use when the resolved template no longer matches the physical layout (e.g. data was relocated, or the template changed since the run landed).  The destination *type* still comes from config (`local`, `s3`, `gcs`, `azure`); only the resolved `path`/`prefix` string is overridden.



## `rivet reconcile`

Partition/window reconciliation: re-count per-partition on source and report mismatches. Requires a chunked export previously run with `chunk_checkpoint: true`. Exits non-zero when a mismatch is detected, so CI / orchestrators can gate on it (an `unknown` partition warns but does not fail)

**Usage:** `rivet reconcile [OPTIONS] --config <CONFIG> --export <EXPORT>`

###### **Options:**

* `-c`, `--config <CONFIG>` — Path to YAML config file
* `-e`, `--export <EXPORT>` — Export name to reconcile (must be `mode: chunked`)
* `--format <FORMAT>` — Output format: "pretty" (human summary) or "json" (machine-readable report)

  Default value: `pretty`

  Possible values: `pretty`, `json`

* `-o`, `--output <OUTPUT>` — Write report JSON to this file (only with `--format json`)
* `-p`, `--param <KEY=VALUE>` — Query parameter: key=value (repeatable)



## `rivet metrics`

Show export metrics history

**Usage:** `rivet metrics [OPTIONS] --config <CONFIG>`

###### **Options:**

* `-c`, `--config <CONFIG>` — Path to YAML config file
* `-e`, `--export <EXPORT>` — Show metrics for a specific export
* `-l`, `--last <LAST>` — Number of recent runs to show

  Default value: `20`
* `--json` — Emit the metrics as a JSON array to stdout (for CI / dashboards) instead of the text table. Empty history prints `[]`



## `rivet schema`

Emit machine-readable schemas for Rivet's data contracts.

Today: `rivet schema config` prints the JSON Schema for the `rivet.yaml` config to stdout.  Operators pipe this into a file and reference it via a `# yaml-language-server: $schema=...` header so VS Code / Neovim's YAML language server highlights invalid keys, suggests enum values, and surfaces required fields as the YAML is edited.  See `docs/cloud-destinations.md` for the broader contract.

**Usage:** `rivet schema <COMMAND>`

###### **Subcommands:**

* `config` — Print the JSON Schema describing `rivet.yaml` to stdout
* `cli` — Print a Markdown CLI reference (every command + flag) to stdout, generated from the clap definitions — the same source as `--help`, so it cannot drift from the actual commands



## `rivet schema config`

Print the JSON Schema describing `rivet.yaml` to stdout.

The schema is generated from the running binary's Rust types, so it always matches the config grammar this version accepts. Pipe to a file and reference it via a `# yaml-language-server: $schema=…` header in your config:

rivet schema config > rivet.schema.json

**Usage:** `rivet schema config`



## `rivet schema cli`

Print a Markdown CLI reference (every command + flag) to stdout, generated from the clap definitions — the same source as `--help`, so it cannot drift from the actual commands.

rivet schema cli > docs/reference/cli-reference.md

**Usage:** `rivet schema cli`



## `rivet journal`

Inspect structured run journal (events, files, retries, quality issues)

**Usage:** `rivet journal [OPTIONS] --config <CONFIG> --export <EXPORT>`

###### **Options:**

* `-c`, `--config <CONFIG>` — Path to YAML config file
* `-e`, `--export <EXPORT>` — Export name to show journal for
* `-l`, `--last <LAST>` — Number of recent runs to show (newest first)

  Default value: `5`
* `--run-id <RUN_ID>` — Show journal for a specific run_id instead of recent runs



<hr/>

<small><i>
    This document was generated automatically by
    <a href="https://crates.io/crates/clap-markdown"><code>clap-markdown</code></a>.
</i></small>
