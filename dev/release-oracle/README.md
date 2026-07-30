# Rivet Release Oracle

**The go/no-go gate. Green everywhere ⇒ releasable.**

It automates the manual pre-release dogfood into one deterministic, repeatable
run: every **engine** at every pinned **version** is brought up from a clean
image, seeded from the canonical seed, and put through every **scenario** against
the local object-store fakes (MinIO=S3, fake-gcs=GCS, Azurite=Azure), then a final
**BigQuery** stage checked against a committed golden.

```
make release-oracle                 # full gate (local stage + BigQuery if creds set)
python3 -m dev.release_oracle --no-cloud        # local stage only
python3 -m dev.release_oracle --engines postgres,mysql
python3 -m dev.release_oracle --bless-bigquery-golden   # re-capture the BQ golden (on purpose)
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
engine-divergent, correctness-critical surface. `verify_cdc_e2e` (`dev/release_oracle/cdc.py`)
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
5. proves **large-transaction atomicity** (the committed-boundary invariant, PG):
   a single transaction of 12 rows at `rollover: 5` must roll as ONE unit (the
   adapter marks only its LAST event `committed`), so a `cdc_after_ack` crash
   holds the anchor BEFORE the whole transaction and recovery re-reads it entire —
   **all 12 rows survive**;
6. **SQLite-vs-Postgres state PARITY**: a PG CDC run against both state backends
   must populate the **same** table set, matching `golden/cdc_state_snapshot.json`
   (the reference snapshot — a release that stops populating `run_status`, or
   drifts the state schema, fails here).

**RED-proven, not just green** (mutation-tested against the release binary): a
data-loss mutant (drop one captured event in the shared sink) makes the INDEPENDENT
readback go **RED on all four engines** (`4 != 5`) while rivet's own `rows` counter
AND `rivet validate` stay green — the independent oracle is the load-bearing axis, a
self-check cannot catch this. A `committed:true`-on-every-event mutant (PG adapter)
makes the large-transaction leg go **RED** (the tx splits at the shared commit-LSN,
the mid-flush crash advances the anchor past it, resume skips the tail — **5/12**
rows survive, 7 lost). Both mutants revert cleanly; the gate is green only on
correct code.

Env-driven and **SKIP** (never a silent pass) when a URL is absent:

```
RIVET_CDC_POSTGRES_URL   postgresql://rivet:rivet@host:port/db   # wal_level=logical
RIVET_CDC_MYSQL_URL      mysql://rivet:rivet@host:port/db        # log_bin, ROW
RIVET_CDC_MSSQL_URL      sqlserver://rivet:rivet@host:port/db    # CDC enabled + Agent
RIVET_CDC_MONGO_URL      mongodb://rivet:rivet@host:port/db?authSource=admin&directConnection=true
RIVET_CDC_STATE_URL      postgresql://…                          # the state-parity leg (else that leg SKIPs)
```

Regenerate the snapshot golden on purpose with `python3 -m dev.release_oracle --bless-cdc`.

## Release build path (pre-tag preflight)

The scenarios above prove **correctness of what ships**; they assume a working binary
exists. But the release pipeline runs **stricter tooling than `cargo build`**, and the
gap only surfaces at the **tag — after crates.io, the binaries, and the GitHub release
have published**, when the failure is no longer re-runnable from the immutable tag.
`verify_release_build_path` (`dev/release_oracle/release_path.py`) runs that path **first, pre-tag**:

- **`cargo metadata --locked`** — `Cargo.lock` in sync with `Cargo.toml`. The release
  publishes with `cargo publish --locked` and the Docker builder cooks + builds
  `--locked`; a stale committed lock aborts both (**0.16.1**, post-tag). Runs FIRST,
  before any other cargo command reconciles the lock out from under it.
- **`cargo_manifest_chef` + `schema_drift`** (offline guards) — no multi-line inline
  tables in `Cargo.toml` (the spec-strict `cargo-manifest` parser the Docker
  `cargo chef` step uses rejects them — **0.16.0**, post-tag) and the checked-in JSON
  schema matches the binary's derived one (a version bump that forgot to regen).
- **`cargo chef prepare`** — the Dockerfile's actual `planner` step (line 11). It
  distils the dependency graph with **no compilation**, so it is cheap yet runs the
  exact cargo-manifest parse the 0.16.0 image failed on — the real command, not a proxy.
- **`docker build`** — the full release image, opt-in via `RIVET_ORACLE_DOCKER=1`
  (fat-LTO in-image, minutes); the cheap steps above cover the known classes every run.

RED-proven: a stale `Cargo.lock` reddens the lock check; a multi-line inline table
reddens **both** the offline guard AND `cargo chef prepare`. SKIP when `cargo` is absent.

## Regression vs the previous release (pre-tag preflight)

The gate compares to checked-in **goldens** and to **itself**, never to the version
users are actually running. Two regressions ship green through every correctness
check yet are release-blocking. `verify_release_regression` (`dev/release_oracle/regression.py`)
benchmarks against the **DOWNLOADED previous-release binary** (`RIVET_PREV_RELEASE_BIN`
— a GitHub release asset / brew bottle, the artifact users run, never a rebuilt
parent) over a seeded 100K keyset+zstd fixture:

- **B-format (cross-version read)** — the previous release WRITES; the current binary
  must READ its manifest + parts (`rivet validate` PASSED + an independent DuckDB
  row-count == source). A format bump the new release can't read silently breaks every
  existing user's data on upgrade — a quiet loss, worse than a crash.
- **B-perf** — current wall-clock ≤ the previous release's × tolerance
  (`RIVET_REGRESSION_WALL_TOL`, default 1.5×; RSS reported alongside). A 3× slowdown
  or an RSS blow-up passes every count/value check.

Each binary runs in its **own env dir** (its own `.rivet_state.db`, which lives next
to the config): the new binary UPGRADES the state schema (v18→v19), which the old
binary then cannot open — never share a state dir across versions.

RED-proven: corrupting a part the prev release wrote reddens the format check (cur
`validate` no longer PASSES); a genuinely slower binary (a debug build, ~1.5×) reddens
the perf check below its slowdown. SKIP when `RIVET_PREV_RELEASE_BIN` /
`RIVET_REGRESSION_SOURCE_URL` are absent.

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
python3 -m dev.release_oracle  # the driver (orchestration loop); --bless-local re-blesses the local goldens
lib/cfg.py               # matrix query interface (dependency-free — no PyYAML)
dev/release_oracle/scenarios.py         # the scenario implementations (verdicts / integrity_types / load / gc)
dev/release_oracle/bigquery.py          # the BigQuery golden stage (rivet run → GCS, rivet load → BQ, bq query)
lib/parse_verdicts.py    # parse `rivet check` → {table: {strategy, verdict}}
lib/gcs_pull.py          # independent fake-gcs readback (no gsutil needed)
lib/normalize_bq.py      # canonicalize a read-back for the golden diff
golden/verdicts.json               # blessed strategy+verdict of every seed+garbage table per engine
golden/duckdb_type_matrix.json     # blessed per-engine type + CSV-fidelity round-trip (local oracle)
golden/bigquery_type_matrix.json   # blessed BQ round-trip of the type-matrix (final stage)
```

Re-bless the local goldens (verdicts + DuckDB type/fidelity) on purpose with
`python3 -m dev.release_oracle --bless-local`; the BQ golden with
`python3 -m dev.release_oracle --bless-bigquery-golden`.

Requires: docker, the `rivet` release binary, `duckdb`, `python3` (stdlib only),
and — for the final stage — `bq` + ADC + a real GCS staging bucket
(`BQ_ORACLE_BUCKET`, default `rivet_data_test`).
