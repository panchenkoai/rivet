# Prove an export is correct — on your own data

You should not have to *trust* that rivet copied your table faithfully. Verify
it: compare a content **fingerprint** of the source query against the same
fingerprint of the exported Parquet. If rivet dropped, duplicated, or corrupted
a single row, a fingerprint field diverges.

The check is **independent of rivet's own bookkeeping** — it never reads rivet's
counters, manifest, or summary. Both fingerprints are computed by DuckDB: one
over the live source (via DuckDB's Postgres/MySQL scanner), one over the Parquet
rivet wrote. The data sources are independent; DuckDB is just the calculator.

## One-time setup

```bash
pip install duckdb        # the only dependency; the postgres/mysql scanner
                          # extensions auto-install on first use
```

The script lives at [`dev/correctness/verify_export.py`](https://github.com/panchenkoai/rivet/blob/main/dev/correctness/verify_export.py).

## Run it

Point it at the **same query** your `rivet.yaml` export used and the Parquet it
produced:

```bash
python dev/correctness/verify_export.py \
    --source-type postgres \
    --dsn "host=127.0.0.1 port=5432 dbname=mydb user=me password=secret" \
    --query "SELECT id, name, amount, updated_at FROM orders" \
    --parquet "/data/exports/orders/*.parquet" \
    --key id
```

```
field           source                export
rows              1000000             1000000
distinct_id       1000000             1000000
nn_id             1000000             1000000
sum_id        500000500000        500000500000
nn_name            1000000             1000000
len_name          18994214            18994214
sum_amount     42130995.51         42130995.51
...
PASS: source and export agree on all 11 fingerprint fields (1000000 rows).
      The export is complete and uncorrupted.
```

Exit code is **0 on PASS, 1 on FAIL**, so it drops straight into a gate:

```bash
rivet run --config rivet.yaml --export orders \
  && python dev/correctness/verify_export.py --source-type postgres \
       --dsn "$DSN" --query "$Q" --parquet "/data/exports/orders/*.parquet" --key id \
  && deploy
```

MySQL is the same with `--source-type mysql` and a MySQL DSN
(`host=… user=… password=… database=…`).

## What the fingerprint covers

The fingerprint is built automatically from the source query's schema, so it
adapts to your columns:

| Field | Built for | Catches |
|---|---|---|
| `rows` | always | row loss / duplication |
| `distinct_<key>` | `--key` | duplication of the key |
| `nn_<col>` | every column | per-column loss (non-null count) |
| `sum_<col>` | numeric columns | value corruption |
| `len_<col>` | text columns | truncation / mangling |

A row that is dropped, duplicated, or whose value changed moves at least one
field. The check is **order-independent** (it is all aggregates), so chunk
ordering and multi-file output don't matter.

## Limits (be honest about them)

- **At-least-once duplication after a crash** is real (see
  [semantics.md](../semantics.md#known-non-guarantees)). If you verify a prefix
  that includes an orphaned pre-crash part, `rows`/`sum` read *high* — that is
  the documented duplicate, not corruption. Verify the parts named in
  `manifest.json` for the exactly-once view, or run `rivet reconcile`.
- The fingerprint is strong but not cryptographic: it does not compare
  date/time/blob/boolean *values* directly (only their non-null counts), to stay
  free of cross-engine representation differences. For those, rivet's live test
  suite uses DuckDB/ClickHouse/pyarrow as full-type oracles
  ([type_roundtrip](https://github.com/panchenkoai/rivet/tree/main/tests/type_roundtrip)).
- It verifies the **data**, not your `query:`. A query that selects the wrong
  rows will fingerprint-match a faithful export of those wrong rows.

The same technique runs continuously in rivet's own CI as
[`tests/live_differential.rs`](https://github.com/panchenkoai/rivet/blob/main/tests/live_differential.rs) — this script
is that test, pointed at *your* database.
