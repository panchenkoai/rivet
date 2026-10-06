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
