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

### Oracle source, batch — Preview (#309)

- **Met:**
  - M1 (the common types; the type matrix is compared with Oracle's own rendering);
  - M2;
  - M4 (a non-default NLS session);
  - M5;
  - M6 (the `live_oracle` module and an engine-matrix cell);
  - M7.
- **Open for GA:**
  - `rivet init` scaffolds keys the planner refuses, on BINARY_FLOAT/DOUBLE/FLOAT/TSTZ;
  - column overrides with no decoder pass `check` and then fail at run;
  - override keys are case-sensitive;
  - NVARCHAR2 on a non-Unicode character set is unmeasured;
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
  - M8 (the reference page, [recipes/clickhouse-load.md](recipes/clickhouse-load.md), with its known limits).
- **Open for GA:**
  - version order across a binlog renumbering (failover, `RESET MASTER`);
  - `load.ca_file` (a private CA) is unit-tested only: no live `https://` run, the stand's ClickHouse serves HTTP;
  - a real-GCS run of the named-collection pull;
  - Mongo CDC (refused, CH7);
  - `partition:` (refused, CH8);
  - M8's driven run;
  - M9.
