# Engine maturity: Preview and GA

A new source or warehouse ships as **Preview** first and becomes **GA** only when every
criterion below holds and is shown by a test or a gate cell. A criterion argued from reading the
code does not count.

## The criteria

| # | Criterion | Preview | GA |
|---|---|---|---|
| M1 | Every value type the engine documents either round-trips losslessly, compared cell by cell with an independent reader (DuckDB, or the engine's own rendering), or is refused by name. | the common types | every documented type |
| M2 | No silent loss or change: every lossy path either refuses or appears as a divergence in the type report. | required | required |
| M3 | Crash and resume: a fault at every hook the runner has loses nothing and duplicates only within at-least-once. | the main paths | every hook, in the fault catalog |
| M4 | Session and server state: one live test in a non-default state (zone, NLS, datestyle…) matches the default. | required | required |
| M5 | Evidence: every ledger with an engine column has a `test` cell or a justified `na`. Every `sound` cell is RED-proven. | required | required |
| M6 | Release gate: the engine has cells in the gate, green on the release candidate. | live module in the gate | gate cells for each mode, plus the version matrix |
| M7 | Mutation: in-diff mutants for the engine's code are CAUGHT or have a written, reviewed exclusion. | required | required |
| M8 | Docs: the reference page states prerequisites, refusals and known limits, and a document-driven run of it (followed literally) finds no divergence. | the reference page | a driven run |
| M9 | Field: a real workload outside the stand has run it. | — | required |

## Where each engine stands (2026-09-28)

### Oracle source, batch — Preview (#309; GA items 2026-09-29)

- **Met:**
  - M1 (the type matrix and every `columns:` override decoder — `int2`, `float4`,
    `float8` from BINARY_FLOAT, `date`, `timestamp_ns`, `timestamp_tz(_ns)`, `uuid`,
    `text` from every type — compared with Oracle's own rendering; a WE8ISO8859P1
    database's character types compared byte for byte with `UTL_I18N.STRING_TO_RAW`);
  - M2 (a `date` override on a value with a time of day, a `uuid` override on a RAW
    that is not 16 bytes and a `timestamp_ns` value past Arrow's range fail the run
    with the column named);
  - M3 (every batch runner hook — single, incremental, chunked range with and
    without the checkpoint, sequential and parallel, keyset sequential and parallel —
    faulted on Oracle, then recovered, with every declared row compared with the
    database's own values; four Oracle faults in `dev/fault_catalog.yaml`, all
    detected);
  - M4 (a non-default NLS session);
  - M5;
  - M6 (the `live_oracle` module and an engine-matrix cell);
  - `rivet init` scaffolds no keyset key the planner refuses
    (`an_init_generated_config_runs_on_a_key_the_oracle_planner_cannot_seek`);
  - a `columns:` key in the wrong case is refused by `rivet check` as well as the run.
- **Open for GA:**
  - an override pairing with no decoder (a number type on a character column,
    `decimal(p > 38, s)`) passes `check` and fails at run; the cross-engine refusal
    is designed separately;
  - M6's version matrix: only 23ai/26ai Free is gated. 21c XE is amd64-only and stops
    with ORA-00442 under the arm64 stand's Rosetta emulation; 19c has no gvenzl image
    (Oracle's registry image needs a sign-in and license acceptance);
  - M7: a partial `--lib` run over the Oracle files (65 of 361 mutants) missed 36, all
    in functions that need an Oracle connection except `budget_spent`, `unique_alias`
    and `native_type`'s decision, which now have unit tests; the full and in-diff runs
    are still to do;
  - M8's driven run;
  - M9.

### Oracle source, CDC through LogMiner — Preview (#324, ADR-0037)

- **Met:**
  - M1 (preview types: NUMBER, FLOAT, BINARY_*, DATE, TIMESTAMP in every form, character types, RAW; everything else is refused by name);
  - M2;
  - M3 (crash before ack, a large transaction crashed mid-flush);
  - M4 (a hostile logon NLS session);
  - M5 (every `cdc-evidence-matrix` cell sound or na);
  - M8 (the reference page).
- **Open for GA:**
  - redo not yet flushed at the bound;
  - `CSF` continuation rows;
  - direct-path NOLOGGING loads;
  - `SEQUENCE#` across a log switch;
  - PDB point-in-time recovery;
  - LOB and other refused types;
  - `rivet load` of an Oracle stream;
  - gate cells beyond the live module;
  - the full in-diff mutation run;
  - M9.

### ClickHouse load target — Preview (#308, ADR-0035)

- **Met:**
  - M1 (the common types: the load matrix and the uuid/json/time/array cell, read back from ClickHouse);
  - M2 (a part holding a timestamp past DateTime64's range, or one whose footer cannot bound it, is refused before insert whether rivet sends it or ClickHouse pulls it, CH12; the rest are divergences in the type report);
  - M3 (a load killed after appending re-runs without changing the view; a load killed after adopting the table resumes into the log; a full load killed at each of its four fault points in `src/test_hook.rs` re-runs to the source; a lost INSERT answer is resent only where a copy collapses, CH13);
  - M4 (a naive timestamp read in three session zones);
  - M5;
  - M6 (`clickhouse_load` gate cells for CDC per engine, full, incremental, the crash re-runs, the range refusal pushed and pulled, the retries, and the loader's three live lib tests);
  - M7 (the loader's HTTP methods are excluded in `.cargo/mutants.toml`, each exclusion naming the live test that kills it, all of which the gate runs);
  - `partition:` by column and granularity in every mode, the change log's view pinned to a cross-partition `FINAL` (CH8; live cells for full, CDC with a moving partition value, incremental, and a changed partition refused);
  - M8 (the reference page, [recipes/clickhouse-load.md](recipes/clickhouse-load.md), with its known limits).
- **Open for GA:**
  - version order across a binlog renumbering (failover, `RESET MASTER`);
  - `load.ca_file` (a private CA) is unit-tested only: no live `https://` run, the stand's ClickHouse serves HTTP;
  - a real-GCS run of the named-collection pull;
  - Mongo CDC (refused, CH7);
  - M8's driven run;
  - M9.
