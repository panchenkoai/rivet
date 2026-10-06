"""Example: a batch DAG built by `build_batch_dag` against the repo's PostgreSQL fixture.

Set on the worker before the scheduler parses this file:
  RIVET_EXAMPLE_STATE_DIR  an existing directory on a host volume; state, plans and stderr files live there
  RIVET_EXAMPLE_OUT_DIR    an existing directory; Parquet lands in <dir>/<export>/
  RIVET_PG_URL             the source URL, e.g. postgresql://rivet:rivet@127.0.0.1:5432/rivet
  RIVET_BIN                optional path of the rivet binary (default: `rivet` on PATH)
  RIVET_EXAMPLE_LOAD=1     optional: add the load group (the config then needs a `load:` block)
"""

from __future__ import annotations

import os
from pathlib import Path

from airflow_provider_rivet.dags import build_batch_dag

HERE = Path(__file__).resolve().parent
CONFIG = os.environ.get("RIVET_EXAMPLE_BATCH_CONFIG", str(HERE / "configs" / "postgres_batch.yaml"))

rivet_provider_batch = build_batch_dag(
    "rivet_provider_batch",
    config=CONFIG,
    state_dir=os.environ.get("RIVET_EXAMPLE_STATE_DIR"),
    exports=["users", "orders", "events"],
    load=os.environ.get("RIVET_EXAMPLE_LOAD") == "1",
    operator_kwargs={
        "rivet_bin": os.environ.get("RIVET_BIN", "rivet"),
        "cwd": os.environ.get("RIVET_EXAMPLE_OUT_DIR"),
    },
)
