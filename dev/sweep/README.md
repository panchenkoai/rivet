# Source-parity sweep — the independent oracle

These scripts are rivet's strongest correctness check: they compare a per-column
profile of the **source** (computed by a direct DB query) against the **destination
parquet** (read by DuckDB), and **do not trust rivet's own counters**. A mismatch is
silent data corruption.

They exist because self-oracles are not enough: re-reading rivet's output, or
checking its reported `row_count`, cannot catch a value that was silently nulled,
collapsed, or truncated on the way out. The `uuid -> null` field bug (100% of a
column became NULL while every count/sum check passed) and the incremental
part-name clobber both belonged to that class.

## What it checks

Per engine, a hostile fixture (1000 rows: low-cardinality + null-heavy + `DECIMAL(38,10)`
differing in the 10th decimal + unicode/emoji + `uuid` + `json` + `date`) is exported
and compared column-by-column on:

- **row count** — no row loss
- **non-null count** — no null injection (this is what kills the uuid->null class)
- **distinct count** — no distinct collapse (catches precision truncation)
- **sum** (numeric columns) — exact value fidelity, incl. `DECIMAL(38,10)` to the 10th decimal

`source_parity_sweep.sh` covers the **batch** export path; `source_parity_cdc.sh`
covers the **CDC** path (a separate per-engine value-decode path), deduping the CDC
parquet to current-state (latest change per id by `__pos,__seq`, deletes excluded)
before comparing.

Not covered here (by design): a uniform value shift (e.g. a timezone-offset error)
preserves count/distinct/sum-of-others — that class is covered by the dedicated
non-UTC tests in `tests/live/live_cdc.rs`.

## Running

```sh
# stack up (batch + cdc profile), rivet built, duckdb on PATH
docker compose --profile cdc up -d postgres mysql mssql postgres-cdc mysql-cdc mssql-cdc
docker compose exec -T mssql-cdc /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa \
  -P 'Rivet_Passw0rd!' -C -Q "IF DB_ID('rivet') IS NULL CREATE DATABASE rivet;"
cargo build --bin rivet

bash dev/sweep/source_parity_sweep.sh   # batch  -> exit 1 on any mismatch
bash dev/sweep/source_parity_cdc.sh     # cdc    -> exit 1 on any mismatch
```

Override the binary with `RIVET=/path/to/rivet`.

## CI enforcement

`tests/live/live_source_parity_sweep.rs` wraps both scripts as `#[ignore]` live
tests, so the nightly `cargo test --test live_suite -- --ignored` run enforces them
— no separate CI job needed. A silent-corruption regression fails that run.
