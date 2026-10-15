- **The Airflow provider package moved to its own repository.** `integrations/airflow` (the
  `airflow-provider-rivet` package: operators for `run`, `load` and `compact`, retry by error class,
  CDC) left the rivet repository for
  [`panchenkoai/airflow-provider-rivet`](https://github.com/panchenkoai/airflow-provider-rivet), with its
  history, so the Rust repository does not publish to PyPI. **Now** the scheduler contract fixtures
  (`tests/fixtures/scheduler/`) stay here and the provider's CI reads them from a pinned rivet
  checkout. **What to do:** install the provider from its own repository; nothing changes in rivet.
