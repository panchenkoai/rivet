"""`rivet load` into ClickHouse (ADR-0035) on the gate's binary.

The cell runs the Rig tests that pin the ClickHouse load against a live server on
the stand: a CDC stream from each of MySQL, PostgreSQL and SQL Server whose view
must equal the source row for row with the deleted key flagged; a load killed
after appending that re-runs without duplicating the view; a full load replacing
the table; an incremental export adopting it behind a view; and `rivet init`'s
ClickHouse flags. Oracles are the tests': the source and ClickHouse read back
directly — never rivet's report.
"""

from __future__ import annotations

from .core import Ledger
from .shared_state import run_rig_tests

CELLS = {
    "a_mysql_cdc_stream_loads_into_clickhouse_and_the_view_matches_the_source": "cdc:mysql",
    "a_postgres_cdc_stream_loads_into_clickhouse_and_the_view_matches_the_source": "cdc:postgres",
    "a_sql_server_cdc_stream_loads_into_clickhouse_and_the_view_matches_the_source": "cdc:mssql",
    "a_load_that_dies_after_appending_is_re_run_without_duplicating_the_view": "cdc:crash",
    "a_full_load_into_clickhouse_replaces_the_table_with_the_current_source": "full",
    "an_incremental_export_into_clickhouse_adopts_the_table_and_serves_the_latest_rows": "incremental",
    "init_clickhouse_flags_scaffold_the_load_block_and_require_each_other": "init",
}


def verify_clickhouse_load(led: Ledger) -> None:
    led.phase("ClickHouse load — CDC per engine, crash re-run, full, incremental, init")
    run_rig_tests(
        led, "clickhouse", tuple(CELLS),
        cell=CELLS.__getitem__,
        msg=lambda n: f"clickhouse[{CELLS[n]}] · {n.replace('_', ' ')}",
    )
