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
`deployment` (`auto` / `local` / `pod`), `extra_args`, `lock_wait` (default 600), `crashed_retries`
(default 1), `stderr_dir`, `stream_stderr` and `cwd`. Templated: `config`, `export`, `table`,
`state_dir`, `plan_file`, `extra_args`, `env`, `stderr_dir`, `cwd`. No operator builds argv: one
module, `invocation.py`, builds it, runs the process, reads the outputs and classifies.

| Operator | Runs | Notes |
|---|---|---|
| `RivetApplyOperator` | `rivet plan -e X --format json -o <artifact>` then `rivet apply <artifact>` | a retry replays the same sealed artifact while it is unexpired |
| `RivetRunOperator` | `rivet run [-e X] --summary-output <file>` | the fallback when sealed apply is not wanted; also runs a whole config |
| `RivetCdcRunOperator` | `rivet run -e X --summary-output <file>` for a `mode: cdc` export | one bounded drain; succeeds on `max_events`, reports it, never loops |
| `RivetLoadOperator` | `rivet load` | per export, per table where the binary can narrow |
| `RivetCompactOperator` | `rivet compact` | a compaction rivet reports as `skipped` becomes an Airflow skip |
| `RivetPlanOperator` | `rivet plan --format json` | rewrites the plan layout file atomically, only when the output is a plan list |

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
plan ──> apply: [a] [b] ─> wave_2_done ─> [big] ─> [huge]
                 │   │                     │        │
         load:  [a] [b]                  [big]    [huge]
                                           │
      compact:                           [big]
```

- Three TaskGroups, `apply`, `load`, `compact`. They are the picture; **dependencies run per
  table**: `apply.X >> load.X >> compact.X`. A failed `apply.a` leaves `load.b` runnable.
- Waves come from the plan file, as in the recipe: cheap exports of a wave run in parallel,
  the others one at a time, and wave N+1 starts after wave N. Those edges are ordering only
  (`trigger_rule="all_done"`): a later wave waits for the earlier one to *finish*, not to succeed.
  For the same reason a failed `plan` refresh fails its own task and does not stop extraction;
  each `apply` task plans its own export from the config, the plan file only shapes the graph.
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
- `load.T` has `trigger_rule="all_done"`: it runs whatever the drain delivered and however it
  ended, because the load consumes what rivet's ledger says is unloaded, including parts of an
  earlier cycle whose load failed. A load that finds nothing to do is a success (never an
  Airflow skip: a skip would propagate to `compact.T`).
- On `stop_reason: max_events` the run task succeeds, puts it in XCom and logs
  `rivet.cdc.max_events`. It does not loop.

## Retries

| What rivet did | Object | Airflow |
|---|---|---|
| exit 0 | none | success |
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
`<state_dir>/airflow/<dag_id>/crashed/<run_id>__<task_id>.json`, holding the try numbers that
ended in a crash. A crash is retried when fewer than `crashed_retries` earlier crashes exist
*since the last clear*; the start of the current series is `max_tries - retries`, which is how
Airflow itself moves `max_tries` when a task is cleared, so a manual clear gets a fresh budget.
On a worker with no state directory (a pod) there is no file: a crash is retried only when it
happened within the first `crashed_retries` tries of the series, which is stricter (a transient
failure followed by a crash is not retried).

Limit: the crashed budget can only shorten Airflow's own budget, never extend it. With
`retries=0` Airflow retries nothing, a crash included. The builders default to `retries=2`.

The operators never run `rivet state reset-chunks` before a retry: a crashed checkpointed run is
resumed by the next process.

## State, by deployment

| Worker | State | What the operator does |
|---|---|---|
| Ephemeral pod (`KUBERNETES_SERVICE_HOST` in the environment, or `deployment="pod"`) | PostgreSQL, required | refuses SQLite (`RIVET_AIRFLOW_STATE_SQLITE_ON_POD`); refuses every CDC export (`RIVET_AIRFLOW_CDC_ON_POD`: the checkpoint is a file and would live on the pod's disk) until rivet stores it in the state database; runs the config in place; relays rivet's stderr to the pod's log stream |
| Local or long-lived worker (`deployment="local"` overrides the detection for a long-lived Kubernetes worker with a persistent volume) | SQLite in a declared directory, or PostgreSQL | requires `state_dir` for SQLite state and for CDC; checks it; warns once per task that SQLite is single-host |

`state_dir` is an operator parameter, not an Airflow Variable. The builders pass one value to
every task they create, so "the same path for every task" holds by construction, it is visible
in the rendered fields, and reading it needs no metadata-database access on the worker.

PostgreSQL state is `RIVET_STATE_URL=postgresql://...` in the task's environment: set it on the
worker, or `env_from_connections={"RIVET_STATE_URL": "rivet_state"}`.

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
   - `<config name>.yaml` (and `<config name>--<export>.yaml`): the operator's copy of your
     config. Today's rivet can only keep SQLite state beside the config file, so the operator
     materialises the config here and runs that copy (`state_url_sqlite`). Edit your original;
     the copy is rewritten on every task.
   - a relative `cdc.checkpoint`, which resolves beside the copied config, that is, here.
   - `airflow/<dag_id>/`: sealed plan artifacts, the crashed ledger, scratch files; `airflow/locks/`.
   - `logs/<dag_id>/`: rivet's stdout and stderr, one file per step and try.
   - `.rivet_airflow_marker`: a random id written by the first task that used the directory.
4. **What breaks when a task sees a different or an empty directory.** rivet creates a new,
   empty state there and treats every export as a first run: an incremental export re-reads
   from the start, a CDC stream re-anchors and the changes between the lost position and the new
   anchor are gone, with no error. What the operator can check without rivet's state identity:
   the directory exists, is absolute and writable, and its marker equals the marker of each
   direct upstream rivet task of the same DAG run (taken from their XCom); a difference is
   refused with `RIVET_AIRFLOW_STATE_DIR_DIFFERS`. It cannot see a directory that was emptied
   between two DAG runs, or two hosts that each have their own copy.
5. **Working directory.** Relative `destination.path` values resolve against the process's
   working directory, which defaults to the directory of your original config; set `cwd` to
   change it. A relative `query_file` is rewritten to an absolute path in the copy.
6. **Single host.** SQLite state and the lock files are visible to one host only. Two workers
   on two hosts need PostgreSQL state.
7. **Moving to PostgreSQL state.** Stop the DAGs; create a database; set `RIVET_STATE_URL` to
   its `postgresql://` URL for every task (the worker's environment, or
   `env_from_connections`). This package does not copy the SQLite state into PostgreSQL: the
   new state starts empty, so plan the switch as a first run of every export (a full export
   simply runs again; an incremental export has no cursor to resume from). Keep `state_dir`
   for CDC: a checkpoint is a file on every state backend today, and it stays where it is.

## Secrets

- rivet is configured through environment variables and nothing else. The subprocess gets the
  worker's environment, plus `env` (templated, visible in the UI: non-secret values only), plus
  `env_from_connections={ENV_NAME: conn_id}`, which builds a URL from an Airflow Connection's
  fields inside `execute`, credentials percent-encoded. The scheme comes from
  `{ENV_NAME: (conn_id, "postgresql")}`, else the connection extra `rivet_scheme`, else the
  connection type (`postgres`, `mysql`, `mssql`, `mongo`, `oracle`); a type that names no rivet
  scheme, such as `generic`, is refused before rivet starts
  (`RIVET_AIRFLOW_CONNECTION_SCHEME`). The extra `rivet_params` adds a query string. It is not
  a templated field. argv carries the config path, export names and flags.
- XCom carries code, class, exit_code, retryable, action, kind, counts, `stop_reason`, the
  degradations and file paths. It never carries the error `message`: redaction covers
  credentials only, and error text can embed source cell values.
- The task log gets one-line JSON records: `rivet.start` (argv), `rivet.unit` (one per unit),
  `rivet.result`, `rivet.warning`, `rivet.crashed`, `rivet.cdc.max_events`, `rivet.refused`.
- rivet's raw stdout and stderr go to files, not to the task log: on a local worker
  `<state_dir>/logs/<dag_id>/<run_id>__<task_id>__try<N>.<step>.stderr.log` (the `rivet.start`
  record and XCom name the path; `stderr_dir` moves them). On a pod, stderr is relayed to the
  pod's own log stream and nothing is kept.
- `stream_stderr=True` copies stderr into the task log when each step ends. It is for
  debugging: it puts possibly data-bearing text into a store with wider access than the worker.

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

The tests need no Airflow database and no rivet binary: a fake `rivet` script replays each
scenario, and the contract fixtures are read from `tests/fixtures/scheduler/` of this repository.
