"""The guarantee path on configs `rivet init` WROTE — never a hand-typed YAML.

Run 1 takes everything, run 2 takes only the delta, on the file the product itself
scaffolds: `init --bigquery-project/--bigquery-dataset` emits the `load:` section, then
`run → load → compact` lands the base and its buffer in the warehouse. Ten cells,
each a Rig test over a generated config:

  * batch on PostgreSQL / MySQL / SQL Server — init picks the cursor (the SQL Server
    `datetime2` miss lived here), run 1 loads a BASE TABLE, run 2 buffers the delta and
    `compact` merges it: an UPDATE lands, the buffer is gone;
  * an incremental export: run 1 = every row, run 2 = the delta only;
  * a single-table CDC scaffold: run 1 captures NOTHING (the anchor), run 2 the changes;
  * a multi-table `backfill: auto` scaffold: run 1 every baseline, run 2 the changes.
  * a source column spelled with a Cyrillic look-alike (`сomment`): it lands as `comment`
    through the base load, the buffer append and the compaction, with no NULL.
  * a whole-schema scaffold where one export gets no cursor: every export still has
    its primary key recorded.
  * a compaction whose job died after renaming the buffer to `__changes__merging`: the
    next `compact` merges the leftover into the base and drops it.
  * a full load whose source was emptied: the warehouse table is emptied too.

The two init flags no blessed cell carries (`--bigquery-project`, `--bigquery-dataset`)
are exercised here, which is what their `FLAG_EXCUSED` entries point at. Oracles are
the tests': the source, `bq`, the Parquet on disk — never rivet's report. SKIP — never
a silent pass — without cargo, the Postgres state URL, the BigQuery project or `bq`.
"""

from __future__ import annotations

from .core import Ledger
from .shared_state import run_rig_tests

CELLS = {
    "a_generated_config_drives_run_load_compact_into_the_warehouse_postgres": "warehouse:postgres",
    "a_generated_config_drives_run_load_compact_into_the_warehouse_mysql": "warehouse:mysql",
    "a_generated_config_drives_run_load_compact_into_the_warehouse_mssql": "warehouse:mssql",
    "a_generated_incremental_config_takes_everything_then_only_the_delta": "delta:incremental",
    "a_generated_single_table_cdc_config_takes_no_baseline_only_later_changes": "delta:cdc-single",
    "a_generated_multi_table_cdc_config_takes_every_baseline_then_only_the_delta": "delta:cdc-multi",
    "a_cyrillic_lookalike_column_lands_under_its_latin_name_with_every_value": "warehouse:lookalike",
    "an_export_init_cannot_give_a_cursor_does_not_cost_the_others_their_key": "keys:per-export",
    "a_compaction_left_half_done_is_finished_by_the_next_one": "compact:leftover",
    "a_full_load_of_an_emptied_source_empties_the_warehouse_table": "full:emptied",
}


def verify_init_delta(led: Ledger) -> None:
    led.phase("Generated configs — run 1 takes everything, run 2 only the delta; init → run → load → compact")
    run_rig_tests(
        led, "init_delta", tuple(CELLS),
        cell=CELLS.__getitem__,
        msg=lambda n: f"init_delta[{CELLS[n]}] · {n.replace('_', ' ')}",
    )
