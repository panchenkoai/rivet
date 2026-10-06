"""Example: a CDC DAG built by `build_cdc_dag` against the repo's logical-replication fixture.

Set on the worker before the scheduler parses this file:
  RIVET_EXAMPLE_STATE_DIR  an existing directory on a host volume; state, the CDC checkpoint and stderr files live there
  RIVET_EXAMPLE_OUT_DIR    an existing directory; Parquet lands in <dir>/orders_cdc/
  RIVET_PG_CDC_URL         a source with wal_level=logical, e.g. postgresql://rivet:rivet@127.0.0.1:5434/rivet
  RIVET_BIN                optional path of the rivet binary (default: `rivet` on PATH)
  RIVET_EXAMPLE_LOAD=1     optional: add load and compact per table (the config then needs a `load:` block)
"""

from __future__ import annotations

import os
from pathlib import Path

from airflow_provider_rivet.dags import build_cdc_dag

HERE = Path(__file__).resolve().parent
CONFIG = os.environ.get("RIVET_EXAMPLE_CDC_CONFIG", str(HERE / "configs" / "postgres_cdc.yaml"))

rivet_provider_cdc = build_cdc_dag(
    "rivet_provider_cdc",
    config=CONFIG,
    export="orders_cdc",
    tables=["orders"],
    state_dir=os.environ.get("RIVET_EXAMPLE_STATE_DIR"),
    load=os.environ.get("RIVET_EXAMPLE_LOAD") == "1",
    operator_kwargs={
        "rivet_bin": os.environ.get("RIVET_BIN", "rivet"),
        "cwd": os.environ.get("RIVET_EXAMPLE_OUT_DIR"),
    },
)
