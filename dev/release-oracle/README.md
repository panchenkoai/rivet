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
| **verdicts** | `init` picks the right strategy for **every seed AND garbage table** (both, per engine), checked against a committed golden (`golden/verdicts.json`): clean-PK seeds → **keyset**, `orders_sparse` → full, garbage `decimal_key` → full-bail, `ref_id_history` → range (non-unique), `unindexed_id` → **not** keyset (name-trap), `bigint_pk` → keyset; **zero** phantom heavy-chunk warnings. PG/MSSQL garbage lives in schema `ext` (a second init); MySQL in the same DB. Mongo → full. |
| **integrity+types** | (1) users 150K loss/dup: source count+distinct == DuckDB read of the parts. (2) a per-engine DuckDB **golden** (`golden/duckdb_type_matrix.json`) with **two fidelity arguments per type matrix**: `<tmt>` = PARQUET readback (binary/typed — decimal, uuid, timestamp, enum) and `<tmt>__csv` = CSV readback read ALL-VARCHAR (the text-writer path parquet never exercises — escape/quote/unicode/null; an array column CSV can't represent records a *refusal* sentinel, itself the guarantee of no silent lossy array→CSV). Both read by DuckDB, never rivet. |
| **load** | `rivet run` extracts to each store {s3/MinIO, gcs/fake-gcs, azure/Azurite}; the readback is **INDEPENDENT** — the store's own client + DuckDB (`httpfs` for MinIO, the fake-gcs JSON API, `az` for Azurite), never rivet's own `--validate` — so a rivet read bug can't rubber-stamp its own write. Row count must equal the source. A run-unique prefix isolates each run (run-unique part names never clobber, so a stable prefix would sum every past run). |
| **gc_survival** | the concurrent-extract bucket-erasure guard (spare an in-flight part while a run is active, delete a true orphan). Runs in the BigQuery stage (needs a warehouse load target). |

## CDC end-to-end stage (all engines, independent oracle)

The batch scenarios above never exercise **change-data-capture** — the most
engine-divergent, correctness-critical surface. `verify_cdc_e2e` (`lib/cdc.sh`)
codifies the manual CDC dogfood as a preflight: for each engine whose
`RIVET_CDC_<ENGINE>_URL` is set it

1. **anchors** a typed table, applies `INSERT`/`UPDATE`/`DELETE` (3 + 1 + 1 = **5
   change events**), and **captures** them (`mode: cdc`, `until_current`) to the
   object store;
2. reads them back **INDEPENDENTLY** — DuckDB over the store, never rivet — and
   asserts **5** events, then `rivet validate` re-reads its own parts (`PASSED`);
3. asserts the **state-DB metabase** is populated (`run_status` all-success);
4. proves **at-least-once crash recovery**: a `cdc_after_flush_before_ack` panic
   holds the per-engine anchor (PG slot / MySQL binlog ckpt / MSSQL from-LSN /
   Mongo resume token), and the re-run **re-reads** the delta (`id=4`), never
   losing it;
5. **SQLite-vs-Postgres state PARITY**: a PG CDC run against both state backends
   must populate the **same** table set, matching `golden/cdc_state_snapshot.json`
   (the reference snapshot — a release that stops populating `run_status`, or
   drifts the state schema, fails here).

Env-driven and **SKIP** (never a silent pass) when a URL is absent:

```
RIVET_CDC_POSTGRES_URL   postgresql://rivet:rivet@host:port/db   # wal_level=logical
RIVET_CDC_MYSQL_URL      mysql://rivet:rivet@host:port/db        # log_bin, ROW
RIVET_CDC_MSSQL_URL      sqlserver://rivet:rivet@host:port/db    # CDC enabled + Agent
RIVET_CDC_MONGO_URL      mongodb://rivet:rivet@host:port/db?authSource=admin&directConnection=true
RIVET_CDC_STATE_URL      postgresql://…                          # the state-parity leg (else that leg SKIPs)
```

Regenerate the snapshot golden on purpose with `run.sh --bless-cdc`.

## Final stage — BigQuery golden

The only non-emulator stage, and the load goes through rivet on BOTH legs:
`rivet run` stages the type-matrix parts to a **real GCS bucket** → `rivet load`
loads them GCS→BigQuery → `bq query` (gcloud, an INDEPENDENT reader) reads them
back → **every column value is compared to `golden/bigquery_type_matrix.json`**,
the blessed warehouse-side representation of a full rivet→Parquet→BigQuery
round-trip of all types. That golden makes the stage **deterministic**: a diff
means rivet's type export or BigQuery's Parquet mapping changed, and the release
stops until the golden is re-blessed on purpose.

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
run.sh                   # the driver (orchestration loop); --bless-local re-blesses the local goldens
lib/cfg.py               # matrix query interface (dependency-free — no PyYAML)
lib/scenarios.sh         # the scenario implementations (verdicts / integrity_types / load / gc)
lib/bigquery.sh          # the BigQuery golden stage (rivet run → GCS, rivet load → BQ, bq query)
lib/parse_verdicts.py    # parse `rivet check` → {table: {strategy, verdict}}
lib/gcs_pull.py          # independent fake-gcs readback (no gsutil needed)
lib/normalize_bq.py      # canonicalize a read-back for the golden diff
golden/verdicts.json               # blessed strategy+verdict of every seed+garbage table per engine
golden/duckdb_type_matrix.json     # blessed per-engine type + CSV-fidelity round-trip (local oracle)
golden/bigquery_type_matrix.json   # blessed BQ round-trip of the type-matrix (final stage)
```

Re-bless the local goldens (verdicts + DuckDB type/fidelity) on purpose with
`run.sh --bless-local`; the BQ golden with `run.sh --bless-bigquery-golden`.

Requires: docker, the `rivet` release binary, `duckdb`, `python3` (stdlib only),
and — for the final stage — `bq` + ADC + a real GCS staging bucket
(`BQ_ORACLE_BUCKET`, default `rivet_data_test`).
