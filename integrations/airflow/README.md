# airflow-provider-rivet

Apache Airflow operators and DAG builders that run the [rivet](https://github.com/panchenkoai/rivet)
binary on the worker as a subprocess and turn its result into Airflow's: retry or stop, which
unit failed, and what to do about it. The contract it implements is
[ADR-0039](../../docs/adr/0039-scheduler-contract.md).

Status: first cut, not published to PyPI. It is versioned independently of the binary and checks
the binary's version at run time (minimum **rivet 0.31.0**; a lower one is refused with both
versions in the message). Supported: Airflow 2.10+ and 3.x, Python 3.9+.

## Install

```bash
pip install -e /path/to/rivet/integrations/airflow            # from a checkout
pip install -e "/path/to/rivet/integrations/airflow[slack]"   # with the Slack provider
```

The rivet binary must be on the worker's `PATH`, or pass `rivet_bin="/abs/path/rivet"`.

## Works today / needs a newer rivet

ADR-0039 describes several things no released binary emits yet. The package detects what the
installed binary can do and takes a recorded fallback for the rest: every task's XCom carries
`degraded: [<key>, ...]` and the same list is in its `rivet.result` log line. Nothing is guessed
without a key.

**How it detects.** A flag is detected by probing `rivet <subcommand> --help` once per process
(not by a version table: the features below have no release number yet, and a development or
backported build has a flag whatever its version string says). An output member (`class`,
`failures[]`, `stop_reason`) is detected by its presence in the output itself. The version is
used for one thing only: the minimum-version refusal.

"Needs" names the ADR-0039 section; none of it is in a released rivet at this package's date,
so the "rivet >=" column is the first release that carries that section.

| Key | Missing in rivet 0.31.0 | What the package does today | Needs (ADR-0039) |
|---|---|---|---|
| `no_notify` | global `--no-notify` | does not pass it; warns when the config has a `notifications:` block | D9, rivet >= unreleased |
| `lock_wait` | global `--lock-wait` and the per-export run lease | does not pass it; the builders set `max_active_runs=1` and `max_active_tis_per_dag=1` | D7, rivet >= unreleased |
| `state_url_sqlite` | `RIVET_STATE_URL=sqlite:<dir>` | copies the config into the state directory, so `.rivet_state.db` lands beside it there | D5, rivet >= unreleased |
| `error_object` | `class`, `retryable`, `kind`, `action` on the `--json-errors` line | derives `class` and `retryable` from the printed line's integer `exit_class`; `kind` and `action` are null | D1/D2, rivet >= unreleased |
| `per_unit_error` | `failures[]` and `per_export[].error` | when several units of one process failed, each carries the process-level object | D2/D3, rivet >= unreleased |
| `stop_reason` | CDC `stop_reason` and `tables[]` | `stop_reason` is null: a `max_events` stop cannot be told from `caught_up` | D4, rivet >= unreleased |
| `apply_summary` | `--summary-output` on `rivet apply` | reads run id and counts from `rivet metrics --json` after a successful apply | D3, rivet >= unreleased |
| `load_filter` | `--export` on `rivet load` / `rivet compact` | writes a single-export copy of the config and loads that | D3, rivet >= unreleased |
| `load_table_filter` | `--table` on `rivet load` / `rivet compact` | the per-table task handles every table of its export; tasks of one export are serialised by a file lock in the state directory | D3, rivet >= unreleased |
| `load_result` | `--summary-output` on `rivet load` / `rivet compact` | status is `loaded` / `compacted` on exit 0 and `failed` otherwise; `skipped`, `table` and row counts are unknown (null) | D3, rivet >= unreleased |

Four more keys record something the worker or one output lacks, not the binary's version:

| Key | What is missing | What the package does |
|---|---|---|
| `stderr_temp` | a state directory or `stderr_dir` on the worker | writes rivet's stdout and stderr to a private per-user directory under the temp directory (see Secrets); a pod loses them with its disk |
| `crashed_ledger` | a readable and writable crashed-budget ledger | without a state directory: a crash is retried only within the first `crashed_retries` tries of the task instance; with a ledger that cannot be read or written: no crash retry at all |
| `result_malformed` | an error object or a result entry with the contract's keys and types | a malformed error object is ignored and the task is classified by the exit-code table; a value of the wrong type is null; a status outside the contract's words counts as `failed` |
| `exit_zero_failed_unit` | a non-zero exit when the summary reports a failed unit | the task fails, classified by the worst failed unit (`data_integrity` > `internal` > `refusal` > `schema_drift` > `retryable` > `generic`, ADR-0039 D2) |

Not detectable, so not recorded per task:

- A child process killed by a signal is not reported as `crashed` by today's binary: the parent
  exits 1 (or 2, by a text match) and the task follows that exit. A kill of the rivet process
  the operator started is seen directly and is `crashed` today.
- There is no `RIVET_STATE_FOREIGN` refusal yet. The package's own marker check (below) is the
  only guard against a wrong state directory.
- rivet does not yet warn about a relative `cdc.checkpoint` under PostgreSQL state.

The list lives in one place in the code, `airflow_provider_rivet/capabilities.py`
(`DEGRADATIONS`), and a test fails when a key has no row in this table.

## Operators

All of them take `config`, `export`, `state_dir`, `rivet_bin`, `env`, `env_from_connections`,
`env_passthrough`, `cloud_credentials`, `deployment` (`auto` / `local` / `pod`), `extra_args`,
`lock_wait` (default 600), `crashed_retries` (default 1), `stderr_dir`, `stream_stderr`, `log_keep`
(default 10) and `cwd`. Templated: `config`, `export`, `table`, `state_dir`, `plan_file`,
`extra_args`, `stderr_dir`, `cwd`. **`env` and `env_from_connections` are not templated**, so their
values are never stored as rendered fields. No operator builds argv: one module, `invocation.py`,
builds it, runs the process, reads the outputs and classifies.

| Operator | Runs | Notes |
|---|---|---|
| `RivetApplyOperator` | `rivet plan -e X --format json -o <artifact>` then `rivet apply <artifact>` | a retry replays the same sealed artifact while it is unexpired |
| `RivetRunOperator` | `rivet run [-e X] --summary-output <file>` | the fallback when sealed apply is not wanted; also runs a whole config |
| `RivetCdcRunOperator` | `rivet run -e X --summary-output <file>` for a `mode: cdc` export | one bounded drain; succeeds on `max_events`, reports it, never loops |
| `RivetLoadOperator` | `rivet load` | per export, per table where the binary can narrow |
| `RivetCompactOperator` | `rivet compact` | a compaction rivet reports as `skipped` becomes an Airflow skip |
| `RivetPlanOperator` | `rivet plan --format json` | rewrites the plan layout file atomically, only when the output is a layout `build_batch_dag` can read (waves with exports); the file keeps its mode |

```python
from airflow_provider_rivet.operators import RivetApplyOperator

orders = RivetApplyOperator(
    task_id="orders",
    config="/opt/pipelines/postgres.yaml",
    export="orders",
    state_dir="/var/lib/rivet/postgres",
    env_from_connections={"RIVET_PG_URL": "rivet_postgres"},
)
```

Every task returns (and, on failure, pushes before raising) one XCom value under `return_value`:

```json
{ "command": "apply", "decision": "success",
  "units": [ { "export": "orders", "table": null, "status": "success", "run_id": "orders_...",
               "rows": 2500, "files": 1, "stop_reason": null, "error": null } ],
  "error": null, "degraded": ["apply_summary", "lock_wait", "no_notify", "state_url_sqlite"],
  "exit_status": 0, "signal": null, "stderr_path": "/var/lib/rivet/postgres/logs/...",
  "skip_reason": null, "state_marker": "1d86...", "rivet_version": "0.31.0" }
```

A unit has exactly the shape of `tests/fixtures/scheduler/xcom_unit*.json`; a package test
compares the two, so the package and the Rust contract cannot drift.

## DAG builders

### Batch: `build_batch_dag`

```python
from airflow_provider_rivet.dags import build_batch_dag

dag = build_batch_dag(
    "rivet_postgres",
    config="/opt/pipelines/postgres.yaml",
    state_dir="/var/lib/rivet/postgres",
    plan_file="/opt/pipelines/postgres.plan.json",   # or exports=["users", "orders"]
    compact_exports=["orders"],
    operator_kwargs={"env_from_connections": {"RIVET_PG_URL": "rivet_postgres"}},
)
```

```
plan ──> apply: [a] [b] ─> wave_2_done ─> [big] ─> after_big ─> [huge]
  │              │   │                     │                     │
  └─> (every     │   │                     │                     │
      apply)    [a] [b]            load: [big]                 [huge]
         load:                             │
      compact:                           [big]          every task ─> watcher
```

- Three TaskGroups, `apply`, `load`, `compact`. They are the picture; **dependencies run per
  table**: `apply.X >> load.X >> compact.X`, all `all_success`. A failed `apply.a` leaves
  `apply.b`, `load.b` and every later wave runnable; it blocks `load.a` and `compact.a`.
- Waves come from the plan file, as in the recipe: cheap exports of a wave run in parallel,
  the others one at a time, and wave N+1 starts after wave N. Ordering goes through tasks that
  do nothing, `apply.wave_<n>_done` and `apply.after_<export>`, with `trigger_rule="all_done"`:
  the next export waits for the earlier one to *finish*, not to succeed.
- `plan` is a direct upstream of every `apply` task: a failed plan refresh blocks the whole
  run (`upstream_failed`), because a run that could not plan has nothing it should extract.
- **A run with a failed task is a failed run.** Airflow colours a run by its leaf tasks, and an
  ordering task that succeeds after a failure would otherwise leave the run green. Every DAG the
  builders make ends in one task, `watcher` (`trigger_rule="one_failed"`, no retries), downstream
  of every other task: it is skipped when nothing failed and fails when anything did (the
  watcher pattern of Airflow's documentation).

| What fails | Its own table | Other tables | Run |
|---|---|---|---|
| `plan` | every `apply`, `load`, `compact`: `upstream_failed`; nothing reaches rivet | the same | failed |
| `apply.X` | `load.X`, `compact.X`: `upstream_failed` | run; the next of a heavy chain and the next wave still start | failed |
| `load.X` | `compact.X`: `upstream_failed` | run | failed |
| `compact.X` | | run | failed |
| nothing | | | success (`watcher` skipped) |
- `plan_file` is read at parse time (no database access); without one, pass `exports=[...]`.
- `extract="run"` uses `rivet run -e X` instead of plan + apply.
- **Compact tasks are created only for the exports named in `compact_exports`.** Whether a table
  compacts depends on the load mode, `load.layout` and the warehouse, which only rivet resolves
  (`full` never compacts; see ADR-0039 D3 for the skip reasons), and today's binary cannot report
  `skipped` in a form a machine can read. A task that could only skip would show green having done
  nothing. Once `rivet compact` reports per-table status, a named export that rivet skips shows as
  an Airflow skip with its `skip_reason` in XCom.
- `load=False` drops the load group (a config with no `load:` block).

### CDC: `build_cdc_dag`

```python
from airflow_provider_rivet.dags import build_cdc_dag

dag = build_cdc_dag(
    "rivet_orders_cdc",
    config="/opt/pipelines/cdc.yaml",
    export="app_cdc",
    tables=["orders", "users"],
    state_dir="/var/lib/rivet/cdc",
)
```

- `run` is ONE task for the stream. `load.T` and `compact.T` are separate tasks per table.
- `run >> load.T >> compact.T`, all `all_success`: **nothing is loaded or compacted after a
  drain that did not succeed** (a refusal such as a log gap means the files of this cycle are
  not the stream), and a failed `load.T` blocks `compact.T` but not another table. The parts of
  an earlier cycle whose load failed are loaded by the next run whose drain succeeds: the load
  consumes what rivet's ledger says is unloaded. A load that finds nothing to do is a success
  (never an Airflow skip: a skip would propagate to `compact.T`).
- The same `watcher` task ends the DAG: a failed `run`, `load.T` or `compact.T` fails the run.
- On `stop_reason: max_events` the run task succeeds, puts it in XCom and logs
  `rivet.cdc.max_events`. It does not loop.

## Retries

| What rivet did | Object | Airflow |
|---|---|---|
| exit 0, no failed unit in the summary | none | success |
| exit 0, a failed unit in the summary | the worst failed unit's (`exit_zero_failed_unit`) | by that object: `retryable` is retried, anything else is not |
| a printed object that is malformed (no `retryable`, `"retryable": "true"`, an unknown `class`, an `exit_class` that is not the exit status) | built from the exit status by the rows below (`result_malformed`) | by the built object; never an unhandled exception |
| exit 2, error line printed with `retryable: true` (today: `exit_class: 2`) | `retryable` | `AirflowException`: retried under the task's `retries` |
| exit 1, 3, 4, 5, 6 | `generic`, `data_integrity`, `schema_drift`, `refusal`, `internal` | `AirflowFailException`: no retry |
| exit 2, no error line (argument error) | built: `generic`, exit_code 1 | `AirflowFailException`: no retry |
| exit 101 (panic) | built: `internal`, exit_code 6 | `AirflowFailException`: no retry |
| killed by a signal, or exit 129-255 | built: `crashed`, exit_code null | retried under its OWN budget, `crashed_retries` (default 1), then `AirflowFailException` |
| exit 1 with a printed line saying `class: "crashed"` (a killed child, future binary) | `crashed` | the same crashed budget |
| any other status | built: `generic` | `AirflowFailException`: no retry |
| the package's own preflight refused | `preflight_refusal: RIVET_AIRFLOW_*` in XCom | `AirflowFailException`: no retry |

The exception text is `[code] class: action (export)`. It never contains rivet's error message.

**The crashed budget across tries.** Airflow clears a task instance's XCom at the start of every
try, so the count cannot live there. On a local worker the operator keeps a small ledger file,
`<state_dir>/airflow/<dag_id>/crashed/<run_id>__<task_id>-<hash>.json`, holding `[try, max_tries]` for
every try that ended in a crash. A crash is retried when fewer than `crashed_retries` earlier
crashes carry the task instance's current `max_tries`. Airflow moves `max_tries` when a task is
cleared, so a manual clear gets a fresh budget; changing `retries` in the DAG file in the middle
of a series does not move it, so it cannot reset the budget.

Every way the ledger can fail grants NO crash retry (`crashed_ledger` in `degraded`, the reason
in the `rivet.crashed` record under `ledger_problem`): a directory that cannot be created or
written, and a file that is not the ledger's JSON, which is left as it is until a person deletes
it. On a worker with no state directory (a pod) there is no file: a crash is retried only within
the first `crashed_retries` tries of the task instance, so neither a crash after a transient
failure nor a crash after a manual clear is retried.

Limit: the crashed budget can only shorten Airflow's own budget, never extend it. With
`retries=0` Airflow retries nothing, a crash included. The builders default to `retries=2`.

The operators never run `rivet state reset-chunks` before a retry: a crashed checkpointed run is
resumed by the next process.

## State, by deployment

| Worker | State | What the operator does |
|---|---|---|
| Ephemeral pod (`KUBERNETES_SERVICE_HOST` in the environment, or `deployment="pod"`) | PostgreSQL, required | refuses SQLite (`RIVET_AIRFLOW_STATE_SQLITE_ON_POD`); refuses every CDC export (`RIVET_AIRFLOW_CDC_ON_POD`: the checkpoint is a file and would live on the pod's disk) until rivet stores it in the state database; runs the config in place; keeps rivet's stderr in a private file on the pod's disk (`stderr_temp`), never in the task log |
| Local or long-lived worker (`deployment="local"` overrides the detection for a long-lived Kubernetes worker with a persistent volume) | SQLite in a declared directory, or PostgreSQL | requires `state_dir` for SQLite state and for CDC; checks it; refuses state or a checkpoint left beside the original config; warns once per task that SQLite is single-host |

`state_dir` is an operator parameter, not an Airflow Variable. The builders pass one value to
every task they create, so "the same path for every task" holds by construction, it is visible
in the rendered fields, and reading it needs no metadata-database access on the worker.

PostgreSQL state is `RIVET_STATE_URL=postgresql://...` in the task's environment:
`env_from_connections={"RIVET_STATE_URL": "rivet_state"}`, or set it on the worker and name it in
`env_passthrough=["RIVET_STATE_URL"]` (a worker variable is not inherited unless it is named).

**A `RIVET_STATE_URL` set on the worker and not passed on is refused.** If the worker's
environment has a non-empty `RIVET_STATE_URL` and the task's environment would not carry one
(not in `env_passthrough`, `env` or `env_from_connections`), the task fails before rivet starts
with `RIVET_AIRFLOW_STATE_ENV_NOT_PASSED` and is not retried: rivet would otherwise run green on
an empty SQLite state while the real state sits in the database the worker names. The message
names the variable and never its value. Fix it one of two ways: inherit the worker's value with
`env_passthrough=["RIVET_STATE_URL"]`, or give the task its own value with
`env_from_connections={"RIVET_STATE_URL": "<conn_id>"}` (or `env`). If the worker's variable is
not meant for this pipeline, unset it on the worker.

### Local worker setup guide

1. **Create the state directory yourself, once, on storage that outlives the worker process.**
   A host path or a named volume, never a container's own filesystem. One directory per
   pipeline (per source database), for example `/var/lib/rivet/<pipeline>`. The operator never
   creates it: a directory that silently appears empty is exactly the failure this guide exists
   to prevent, so a missing one is refused (`RIVET_AIRFLOW_STATE_DIR_MISSING`).
2. **Every task of every DAG that touches the pipeline must see the SAME absolute path.** With
   Docker, mount the same host path at the same container path in every worker. Pass that path
   as `state_dir` (the builders do it for all their tasks).
3. **What is in it.**
   - `.rivet_state.db`: rivet's state, under its default name.
   - `<config name>.<hash>.yaml` (and `<config name>.<hash>--<export>.yaml`): the operator's
     copy of your config, byte for byte. `<hash>` is the first 8 hex digits of the SHA-256 of the
     original's absolute path, so two configs with one file name never share a copy. Today's
     rivet can only keep SQLite state beside the config file, so the operator materialises the
     config here and runs that copy (`state_url_sqlite`). Edit your original; the copy is
     rewritten on every task.
   - every relative `query_file` of the config, copied to the same relative path (see 5), and
     `.rivet_airflow_query_files.json`, which records the config directory each one came from.
   - a relative `cdc.checkpoint`, which resolves beside the copied config, that is, here.
   - `airflow/<dag_id>/`: sealed plan artifacts (`plans/<run_id>/<export>.json`), the crashed
     ledger, scratch files; `airflow/locks/`.
   - `logs/<dag_id>/<task_id>/`: rivet's stdout and stderr, one file per step and try.
   - **How a name becomes a path.** A DAG id, task id, run id or export name that is one lower-case
     word of `a-z 0-9 _ . -` (at most 80 characters, not starting with `.` or `-`) is used as it
     is. Any other name (other characters such as `:` `+` or non-ASCII letters, upper case, a
     mapped task's `task_id[index]`) is written as its readable form (each run of other
     characters replaced by `_`) followed by `-` and 12 hex digits of the SHA-256 of the exact
     name: run id `scheduled__2026-10-06T00:00:00+00:00` becomes
     `scheduled__2026-10-06T00_00_00_00_00-b07b0ae9e551`. Two different names
     therefore never share a directory, a sealed plan, a log file or a ledger, also on a file
     system that ignores case.
   - A sealed plan artifact records the export it was made for. `apply` refuses one that records
     another export (`RIVET_AIRFLOW_PLAN_ARTIFACT_FOREIGN`) and leaves the file as it is, so the
     retry is refused for the same reason until a person deletes it.
   - `.rivet_airflow_marker`: a random id written by the first task that used the directory.

   The copies and every file under `airflow/` and `logs/` are created with mode `0600`, in
   directories of mode `0700` (an existing `airflow/` or `logs/` directory is tightened).
   **Moving an existing pipeline under Airflow:** if `.rivet_state.db` or a relative
   `cdc.checkpoint` file already sits beside your original config and not here, the task is
   refused before rivet starts (`RIVET_AIRFLOW_STATE_BESIDE_CONFIG`,
   `RIVET_AIRFLOW_CHECKPOINT_BESIDE_CONFIG`): the copy would not see it, and rivet would start
   from an empty state or re-anchor the stream. Stop the runs, move the file into the state
   directory (the state database with its `-wal` and `-shm` files; the checkpoint to the same
   relative path), and run again. Using the config's own directory as `state_dir` needs no copy
   and no move.
4. **What breaks when a task sees a different or an empty directory.** rivet creates a new,
   empty state there and treats every export as a first run: an incremental export re-reads
   from the start, a CDC stream re-anchors and the changes between the lost position and the new
   anchor are gone, with no error. What the operator can check without rivet's state identity:
   the directory exists, is absolute and writable, and its marker equals the marker of each
   direct upstream rivet task of the same DAG run (taken from their XCom); a difference is
   refused with `RIVET_AIRFLOW_STATE_DIR_DIFFERS`. It cannot see a directory that was emptied
   between two DAG runs, or two hosts that each have their own copy.
5. **Relative paths in the config.** The copy changes the directory rivet takes as the
   config's, and nothing else:

   | Key | rivet resolves it against | Under this package |
   |---|---|---|
   | `query_file` | the config's directory; an absolute path, `..` and a symlink out of it are refused by rivet | stays as written; the file is copied to the same relative path in the state directory before every task. Refused before rivet starts (`RIVET_AIRFLOW_QUERY_FILE`) when it is absolute, has `..`, leaves the directory through a symlink, is missing for the task's export, or when a config from ANOTHER directory already put a file at that relative path in this state directory |
   | `cdc.checkpoint` | the config's directory | resolves in the state directory; a file left beside the original is refused, see 3 |
   | `destination.path` (and the other paths rivet opens as written, such as `tls.ca_file`) | the process's working directory | unchanged: the working directory defaults to the directory of your ORIGINAL config; set `cwd` to change it |
6. **Single host.** SQLite state and the lock files are visible to one host only. Two workers
   on two hosts need PostgreSQL state.
7. **Moving to PostgreSQL state.** Stop the DAGs; create a database; set `RIVET_STATE_URL` to
   its `postgresql://` URL for every task (the worker's environment, or
   `env_from_connections`). This package does not copy the SQLite state into PostgreSQL: the
   new state starts empty, so plan the switch as a first run of every export (a full export
   simply runs again; an incremental export has no cursor to resume from). Keep `state_dir`
   for CDC: a checkpoint is a file on every state backend today, and it stays where it is.

## Secrets

- rivet is configured through environment variables and nothing else, and **the subprocess gets
  a minimal environment, not the worker's**. It is built from three things:
  1. The worker's variables on one allow-list (`ENV_ALLOWLIST` in `invocation.py`): `PATH`,
     `HOME`, `USER`, `LOGNAME`, `TZ`, `LANG`, `LC_*`, `TMPDIR`, `SSL_CERT_FILE`, `SSL_CERT_DIR`,
     `HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY`, `ALL_PROXY`, `http_proxy`, `https_proxy`,
     `no_proxy`, `all_proxy`, `KUBERNETES_SERVICE_HOST`, `KUBERNETES_SERVICE_PORT`. Nothing else
     is inherited: not `AIRFLOW__*`, not `AIRFLOW_CONN_*`, not a `RIVET_*` variable set on the
     worker. `env_passthrough=["RIVET_STATE_URL", "MY_*"]` extends the list with names or
     shell-style patterns. `cloud_credentials=["gcp", "aws", "azure"]` adds
     `GOOGLE_APPLICATION_CREDENTIALS` and `GOOGLE_CLOUD_PROJECT`, `AWS_*`, `AZURE_*` for a
     destination or warehouse that authenticates from the environment.
  2. `env={NAME: value}`. It is not templated and not stored in the metadata database, but it
     is whatever your DAG file computes: do not write a secret literal there.
  3. `env_from_connections={ENV_NAME: conn_id}`, which builds a URL from an Airflow Connection's
     fields inside `execute`, credentials and the database name percent-encoded. The scheme
     comes from `{ENV_NAME: (conn_id, "postgresql")}`, else the connection extra `rivet_scheme`,
     else the connection type (`postgres`, `mysql`, `mssql`, `mongo`, `oracle`); a type that
     names no rivet scheme, such as `generic`, is refused before rivet starts
     (`RIVET_AIRFLOW_CONNECTION_SCHEME`). A password without a login is kept, an IPv6 host is
     bracketed, an Oracle connection's extra `service_name` is used as the path, and the extra
     `rivet_params` adds a query string.

  argv carries the config path, export names and flags.
- XCom carries code, class, exit_code, retryable, action, kind, counts, `stop_reason`, the
  degradations and file paths. It never carries the error `message`: redaction covers
  credentials only, and error text can embed source cell values.
- The task log gets one-line JSON records: `rivet.start` (argv), `rivet.unit` (one per unit),
  `rivet.result`, `rivet.warning`, `rivet.crashed`, `rivet.cdc.max_events`, `rivet.refused`.
- **rivet's raw stdout and stderr go to files and never to the task log or to the worker
  process's own stderr** (which Airflow 3, and Airflow 2 with `run_as_user`, copy into the task
  log). The files are
  `<base>/<dag_id>/<task_id>/<run_id>__try<N>.<step>.stderr.log` (and `.stdout.log`), mode `0600`
  in `0700` directories; the `rivet.start` record and XCom name the path. `<base>` is
  `stderr_dir` when given, else `<state_dir>/logs`, else, on a worker with no state directory,
  `<temp dir>/rivet-airflow-<uid>/logs` (recorded as `stderr_temp`; a pod loses the files with
  its disk, and what a failure was is still in the error object).
- **Retention.** When a task ends, the files of its newest `log_keep` tries (default 10, counted
  per task across runs) are kept and the older ones are removed. `log_keep=None` keeps
  everything. Sealed plan artifacts and crashed ledgers under `airflow/<dag_id>/` are small and
  are not pruned.
- `stream_stderr=True` copies stderr into the task log when each step ends. It is the one way
  raw stderr reaches the task log, and it is for debugging: it puts possibly data-bearing text
  into a store with wider access than the worker.

## Slack

Mutual exclusion: under Airflow, rivet's own notifications are off and alerting is Airflow's.
The operators pass `--no-notify` to a binary that has it. Until then **do not configure
`notifications:` in a rivet config run under Airflow**: every failure would be sent twice. The
operator warns (`rivet.warning`, `notifications_block`) when it sees the block.

```python
from airflow_provider_rivet.callbacks import slack_failure_callback

dag = build_batch_dag(
    "rivet_postgres", config=..., state_dir=..., exports=[...],
    default_args={"retries": 2, "on_failure_callback": slack_failure_callback("slack_webhook")},
)
```

The `watcher` task carries no failure callback: the builders set its `on_failure_callback` to
none, so a failed run sends one message per failed task and nothing for the watcher (which has no
rivet result to show).

It needs `apache-airflow-providers-slack` (the `slack` extra) and a Slack incoming-webhook
connection. The message shows task, run, and per failed unit the export, `[code]`, class,
whether it is retryable, the registry action, the stderr file path and a link to the task log;
never rivet's error text. `format_failure(payload, ...)` is the pure formatter if you send
through another channel.

## Overlap

Two runs of the same export must not overlap. rivet's lease (`--lock-wait`, default 600 s: the
second run waits) is not released yet. Until then the builders set `max_active_runs=1` on the
DAG and `max_active_tis_per_dag=1` on every rivet task. **This does not protect against a run
started outside Airflow** (cron, a shell), nor against a second DAG over the same export.

## Known limits

- Not published; the Kubernetes mode has unit tests only (no pod was run).
- `load` and `compact` against a warehouse were exercised only through the fake binary.
- Narrowing a load to one export today means rewriting the config through a YAML reader and
  writer: comments are dropped and YAML anchors are expanded in the copy.
- With `load_table_filter` degraded, the per-table tasks of a multi-table CDC export each load
  the whole export; the first does the work, the others find the ledger up to date.
- `rivet apply` prints no result today, so a successful apply task reads its counts from
  `rivet metrics`; a failed one has no counts.
- A failure callback runs where Airflow runs callbacks; on Airflow 3 that was not exercised.

## Development

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python "apache-airflow==2.10.5" \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt
uv pip install --python .venv/bin/python --no-deps -e . pytest
.venv/bin/python -m pytest
```

The tests need no rivet binary and no running Airflow: a fake `rivet` script replays each
scenario (and enforces rivet's own `query_file` rule, tied to the Rust source by a test), and the
contract fixtures are read from `tests/fixtures/scheduler/` of this repository.
`tests/test_dag_runs.py` runs the builders' DAGs through `dag.test()` against a SQLite metadata
database created in a throwaway `AIRFLOW_HOME`, with real task instances and trigger rules.
