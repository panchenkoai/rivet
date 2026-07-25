# Rivet Release Oracle

**The go/no-go gate. Green everywhere ⇒ releasable.**

It automates the manual pre-release dogfood into one deterministic, repeatable
run: every **engine** at every pinned **version** is brought up from a clean
image, seeded from the canonical seed, and put through every **scenario** against
the local object-store fakes (MinIO=S3, fake-gcs=GCS, Azurite=Azure), then a final
**BigQuery** stage checked against a committed golden.

```
make release-oracle                 # full gate (local stage + BigQuery if creds set)
dev/release-oracle/run.sh --no-cloud        # local stage only
dev/release-oracle/run.sh --engines postgres,mysql
dev/release-oracle/run.sh --bless-bigquery-golden   # re-capture the BQ golden (on purpose)
```

## What it checks (per engine × version)

| scenario | asserts |
|---|---|
| **verdicts** | `init` picks the right strategy per table: clean-PK seeds → **keyset**, `orders_sparse` → full, garbage `decimal_key` → full-bail, `ref_id_history` → range (non-unique), `unindexed_id` → **not** keyset (name-trap), `bigint_pk` → keyset; **zero** phantom heavy-chunk warnings. Mongo → full + the accurate rationale (not the false "below 100K"). |
| **integrity+types** | export (parquet) → DuckDB independent oracle vs source: row count, distinct-id set (no loss/dup), and typed values (decimal precision, uuid distinct). MSSQL/Mongo compare vs a source count+distinct query. |
| **load** | `rivet run --validate` to each store {s3/MinIO, gcs/fake-gcs, azure/Azurite}: rivet re-reads its parts back **from the store** and checks Form-B integrity — a real write→store→read round-trip on each backend. |
| **gc_survival** | the concurrent-extract bucket-erasure guard (spare an in-flight part while a run is active, delete a true orphan). Runs in the BigQuery stage (needs a warehouse load target). |

## Final stage — BigQuery golden

The only non-emulator stage. rivet exports the type-matrix tables → Parquet →
`bq load` → `bq query` reads them back → **every column value is compared to
`golden/bigquery_type_matrix.json`**, the blessed warehouse-side representation of
a full rivet→Parquet→BigQuery round-trip of all types. That golden makes the stage
**deterministic**: a diff means rivet's type export or BigQuery's Parquet mapping
changed, and the release stops until the golden is re-blessed on purpose.

Set `BQ_ORACLE_PROJECT` + `BQ_ORACLE_DATASET` (with ADC) to run it. Absent creds →
the stage is **SKIP**, never a silent pass — but a real release build must run it
green.

## Contract

- A cell is **PASS** only when its check RAN and MATCHED.
- A down service / absent cloud cred is **SKIP** — never a silent pass.
- The gate exits non-zero (**NOT RELEASABLE**) if any non-skipped cell fails.

## Layout

```
matrix.yaml              # declarative source of truth (engines × versions × scenarios × stores)
run.sh                   # the driver (orchestration loop)
lib/cfg.py               # matrix query interface
lib/scenarios.sh         # the scenario implementations (verdicts / integrity_types / load / gc)
lib/bigquery.sh          # the BigQuery golden stage
lib/normalize_bq.py      # canonicalize the BQ read-back for the golden diff
golden/bigquery_type_matrix.json   # the blessed BQ round-trip of the type-matrix
```

Requires: docker, the `rivet` release binary, `duckdb`, `python3` (+ pyyaml,
auto-installed), and — for the final stage — `bq` + ADC.
