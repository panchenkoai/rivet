# Query-shape matrix

Part of the matrix family under [`dev/matrices/`](../matrices/README.md).

Fourth matrix in the family. Where cli_matrix / cfg_matrix / path_matrix
pin **what the CLI does**, this one pins **what the DB planner does**
with the query that came out of `rivet plan`.

## Why

Exit codes, error messages, on-disk layout, and row accounting can
all be green while the actual SQL hitting Postgres regresses in a way
that costs the operator real money:

- A refactor wraps the operator's `query:` in a subquery / CTE.
  Result: Seq Scan inside a Subquery Scan instead of a direct Index
  Scan. Same rows out, much slower. Hidden from every other guard.
- A future planner change uses `OFFSET LIMIT` for pagination instead
  of the keyset `WHERE id BETWEEN $1 AND $2` pattern that scales.
- An `ORDER BY id` query stops using the PK index because someone
  changed the column type from `int` to `text` in a future schema.
- An `AGGREGATE` query gains a Sort step because the planner can no
  longer prove uniqueness on the input.

None of these change rc, message, layout, or `total_rows`. They show
up in `EXPLAIN`.

## How

For each fixture under `cfg/`:

1. `rivet plan -c <yaml> -e pa_audit --format json` — gives us the
   materialized `base_query` (including any `${VAR}` / `--param`
   substitution).
2. `EXPLAIN (COSTS OFF)` on that query, against the real PG container.
3. Normalize (trim trailing whitespace, drop blank lines).
4. Diff against `expected/<id>.plan`.

`COSTS OFF` strips per-environment cost numbers, which are noisy
across PG versions and statistics freshness. Structural plan shape
(node types, indentation, `Index Cond:` etc.) is the contract.

## Scenarios pinned

| id | query | expected shape |
|---|---|---|
| q01_full_scan | `SELECT id, name FROM pa_audit` | `Seq Scan on pa_audit` |
| q02_pk_filter | `... WHERE id <= 10` | `Bitmap Heap Scan` + `Bitmap Index Scan on pa_audit_pkey` |
| q03_non_index_filter | `... WHERE name LIKE 'row_1%'` | `Seq Scan` + `Filter: ...` |
| q04_order_by_pk | `... ORDER BY id` | `Index Scan using pa_audit_pkey` |
| q05_aggregate | `SELECT COUNT(*)::INTEGER FROM ...` | `Aggregate` + `Seq Scan` |

## Running

```bash
docker compose up -d postgres
cargo build --bin rivet --release
cp target/release/rivet dev/query_matrix/rivet
cd dev/query_matrix
./matrix.sh
```

Non-zero exit when at least one EXPLAIN diverged.

## Updating baselines

When a plan change is intentional:

1. `./matrix.sh` captures the new explain in `logs/<id>/explain`.
2. Inspect `logs/<id>/explain.diff`.
3. `cp logs/<id>/explain expected/<id>.plan`.
4. Document in the CHANGELOG of the PR that lands the change.

## Limitations

- **PG only.** MySQL has a different EXPLAIN format; adding it is a
  separate fixture pass. For now this matrix gates the PG planner.
- **`base_query` only.** The chunked-mode runtime rewrite (`WHERE
  column >= ? AND column <= ?`) happens at execution time, not in
  plan.json. Catching that needs a runtime SQL-capture path (PG
  `log_statement = all` + `docker logs` parsing); future work.
- **PG version sensitive.** The exact EXPLAIN shape depends on PG
  version and table statistics. Baselines are pinned to whatever PG
  the docker-compose primary container runs (currently 16). When
  bumping the primary PG version, re-promote baselines in the same PR.
