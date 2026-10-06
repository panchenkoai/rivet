# Changelog

All notable changes to `airflow-provider-rivet`. The package is versioned independently of the
rivet binary; its release tags will use the prefix `airflow-v`.

## 0.1.0 (unreleased)

First cut, against rivet 0.31.0 and the contract of ADR-0039.

- Operators: `RivetApplyOperator`, `RivetRunOperator`, `RivetCdcRunOperator`, `RivetLoadOperator`,
  `RivetCompactOperator`, `RivetPlanOperator`; one invocation module builds argv and env, runs
  the process, reads its outputs and classifies the result.
- DAG builders: `build_batch_dag` (plan, then apply / load / compact groups with per-table
  edges and ordered waves) and `build_cdc_dag` (one run task, load and compact per table).
- Retries by the contract: a printed retryable failure is retried, exits 1 and 3-6 are not, an
  exit 2 with no error object is not, a kill is retried under its own budget.
- Preflight: binary present and at least 0.31.0, state location by deployment (a pod needs
  PostgreSQL state and refuses CDC), state directory checks, a warning for `notifications:`.
- Secrecy: XCom and the task log never carry the error message; raw stderr goes to a file.
- `slack_failure_callback` for the Slack provider's webhook hook.
- Every fallback taken against a binary that lacks a contract feature is recorded as
  `degraded` in the task's result and listed in the README.

Changed before the first release, after review (see the README sections named):

- **A DAG run with a failed task is a failed run.** Both builders end in a `watcher` task
  (`one_failed`). A failed `plan` blocks every extract; a failed CDC `run` blocks every load and
  compaction (`load.T` was `all_done`); extract tasks are `all_success` and ordering goes through
  no-op tasks (`apply.wave_<n>_done`, new `apply.after_<export>`). ("DAG builders")
- A multi-wave batch DAG no longer fails on Airflow 2.10 when an upstream published no XCom.
- A relative `query_file` stays relative and the file is copied beside the config's copy; it was
  rewritten to an absolute path, which rivet refuses. The copy is named
  `<name>.<hash of the source path>.yaml`. State or a CDC checkpoint left beside the original
  config is refused (`RIVET_AIRFLOW_STATE_BESIDE_CONFIG`, `RIVET_AIRFLOW_CHECKPOINT_BESIDE_CONFIG`,
  `RIVET_AIRFLOW_QUERY_FILE`). ("Local worker setup guide")
- The subprocess gets an allow-listed environment instead of the worker's; `env_passthrough`
  and `cloud_credentials` extend it. `env` is no longer a templated field. ("Secrets")
- Raw stderr is never written to the worker's own stderr; without a state directory it goes to
  a private temp directory (`stderr_temp`). Log files are `0600` in `0700` directories, under
  `logs/<dag_id>/<task_id>/`, and only the newest `log_keep` tries of a task are kept. ("Secrets")
- Exit 0 with a failed unit in the summary fails the task (`exit_zero_failed_unit`); a malformed
  error object or result entry falls back to the exit-code table (`result_malformed`) instead of
  raising; the error line is searched in the last 1 MiB of stderr, not the last 200 lines.
- The crashed-budget ledger fails closed (`crashed_ledger`) and is keyed by `max_tries`, so
  lowering `retries` cannot reset it. ("Retries")
- `RivetPlanOperator` swaps the layout file in only when a builder can read it.
- `connection_url`: a password without a login, IPv6 hosts, Oracle `service_name`, an encoded
  database name. The version has one source, `airflow_provider_rivet/__init__.py`.
