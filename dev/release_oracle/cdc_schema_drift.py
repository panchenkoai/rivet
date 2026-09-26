"""`on_schema_drift` on CDC exports, on the gate's binary.

A column retyped between two runs must be refused under `fail` before anything is
acknowledged (checkpoint and slot unmoved), and every deferred change must land with
its value once the export switches to `warn`. One Rig test per SQL engine; MongoDB's
CDC schema is a fixed document blob and cannot drift a column.
"""

from __future__ import annotations

from .core import Ledger
from .shared_state import run_rig_tests

CELLS = {
    "mysql_cdc_retyped_column_refuses_under_fail_and_defers_not_drops": "mysql",
    "pg_cdc_retyped_column_refuses_under_fail_and_defers_not_drops": "postgres",
    "mssql_cdc_retyped_column_refuses_under_fail_and_defers_not_drops": "mssql",
}


def verify_cdc_schema_drift(led: Ledger) -> None:
    """Run the CDC schema-drift Rig tests against the local CDC engines."""
    led.phase("CDC schema drift — refuse under fail, defer not drop, per engine")
    run_rig_tests(
        led, "cdc-schema-drift", tuple(CELLS),
        cell=CELLS.__getitem__,
        msg=lambda n: f"cdc-schema-drift[{CELLS[n]}] · {n.replace('_', ' ')}",
        cloud=False,
        services=(("postgres-cdc", 5434), ("mysql-cdc", 3307), ("mssql-cdc", 1434)),
    )
