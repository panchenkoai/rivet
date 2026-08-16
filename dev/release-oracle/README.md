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

### The rest of the environment — and why a SKIP tally is the first thing to read

The CDC block above is only part of it. A gate run with none of the below is
**95 PASS / 60 SKIP** and — apart from the previous-release stages, which now FAIL
rather than skip when their baseline is missing — still prints `RELEASE-READY`, which
is literally true ("every non-skipped cell is green") and nearly meaningless. With
everything set it is **~125 PASS / ~36 SKIP**. Always read the SKIP count before the
verdict.

```
RIVET_PREV_RELEASE_BIN      /path/to/DOWNLOADED release binary   # REQUIRED for a release run: regression + differential + field replay (absent ⇒ FAIL, not SKIP); also the scale baseline
RIVET_REGRESSION_SOURCE_URL postgresql://…                       # a PG the cell may seed regr_probe into
RIVET_SCALE_<ENGINE>_URL    …                                    # batch-tier DBs, per engine
BQ_ORACLE_PROJECT           …                                    # BigQuery golden stage
BQ_ORACLE_DATASET           …                                    # one dataset PER SOURCE is derived from this
```

`RIVET_PREV_RELEASE_BIN` must be a **downloaded release asset**
(`gh release download vX.Y.Z --pattern "*-$(uname -m)-apple-darwin.tar.gz"`), never a
locally rebuilt parent: the release profile is fat-LTO, so a rebuild costs minutes and
compares against an approximation instead of what users actually run.

Two traps measured on 2026-08-03, both of which reported a plausible number rather
than an error:

* **The batch and CDC tiers of the dev stand are different servers with different
  auth.** The CDC mongo takes `rivet:rivet` + `authSource=admin`; the BATCH mongo
  (`:27105`) has **no auth at all** and refuses those credentials. Copying the CDC URL
  shape gave `scale[mongo]` an authentication failure whose dead process measured
  ~15 MB — and the cell reported `flat×1.01` **PASS**. The batch mongo also has no
  `users` collection (it holds `orders` 200k / `events` 500k), so
  `RIVET_SCALE_MONGO_SMALL=orders` is required or the export reads zero rows.
* **`--state-url` is process-wide.** `__main__` sets `RIVET_STATE_URL` for every child,
  which defeats the per-binary state isolation the regression and scale cells build on
  purpose: the previous release meets a schema the current tree just migrated and
  refuses to start (`expected schema v19 but reached v20`). Those cells now pin
  `RIVET_STATE_URL=""` themselves. If you add a cell that runs a DIFFERENT binary
  version, pin it too.

Both cells now print `RUN FAILED — no measurement` instead of a number when a
timed run does not succeed. A measurement of a process that died is not a
measurement, and next to a real one it reads as a regression that is not there.

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

## Comparison against the previous release — three stages, and they BLOCK

The gate compares to checked-in **goldens** and to **itself**, never to the version
users are actually running. That gap shipped **0.24.4**: a governor regression
(+1h48m makespan, 52 exports shedding workers in a field run) walked through a full
green gate, because the one stage that would have seen it **reported a non-failure —
it never ran**. Three stages now close it, all keyed off the same **DOWNLOADED
previous-release binary** (`RIVET_PREV_RELEASE_BIN` — a GitHub release asset / brew
bottle, the artifact users run, never a locally rebuilt parent):

| stage (`dev/release_oracle/regression.py`) | compares | fails the release when | ledger rows |
|---|---|---|---|
| **`verify_release_regression`** | prev WRITES → cur READS (format compat), and cur's wall-clock/RSS vs prev over a seeded 100K keyset+zstd fixture | cur cannot open prev's manifest+parts (`rivet validate` + an independent DuckDB count == source), or cur is slower than prev × `RIVET_REGRESSION_WALL_TOL` (default 1.5×) | one (`release/regression`) |
| **`verify_previous_release_differential`** | runs `dev/pytools/ab_regression.py` — **both** binaries over 8 identical scenarios (full, chunked-parallel, keyset-parallel, `chunk_checkpoint`, incremental, csv, crash+resume, error-exit + `plan --format json`) | **any** observable difference: exit code, DuckDB readback, file count, or the manifest's accounting **including per-part content fingerprints and column checksums** — or fewer than 8 scenarios were compared | one per scenario **+ a `scenarios` row carrying the COUNT** |
| **`verify_field_symptom_replay`** | runs `dev/pytools/field_replay.py` — the field run's own shape (tiny `full` exports, a keyset majority at parallel 1/2/4, a few chunked, under a pool), both binaries, adaptive on and off | any of the four criteria fixed in the harness fails: **1** the OLD binary must shed at least once, **2** the NEW binary sheds zero on an idle source, **3** new makespan ≤ old × 1.05, **4** identical rows per export | one per criterion **+ a `criteria` row** |

**Criterion 1 is the activation guard and is reported as such.** If the old binary
never sheds, the fixture reproduced nothing and criteria 2–4 grade *air* — a green
"symptom gone" then means only that nothing happened. That row fails **loudly**, and
criteria 2–4 that "passed" underneath it are recorded **SKIP — vacuous**, never PASS.
The same rule governs the differential's per-scenario rows (a cell the harness marks
`GRADED NOTHING` — both sides empty, the injected crash never fired — is a **FAIL**,
not agreement) and its count row: a harness that compared zero scenarios also reports
"no difference", so the **number of scenarios graded** is asserted (ratchet: 8) and
printed in the row a reader sees.

> **The replay fixture has a shelf life — plan its retirement, don't tune it away.**
> Criterion 1 asks the *previous release* to reproduce the symptom, so this stage is
> evidence only while `RIVET_PREV_RELEASE_BIN` is a release that **carries** the
> regression (0.24.4). Measured 2026-08-14 against the 0.24.3 asset, which predates
> it: criterion 1 went RED (`old shed 0x`) and 2–4 were recorded vacuous — the stage
> behaving exactly as designed. Once the fix has shipped, the newest release no
> longer sheds either and this stage goes permanently red on criterion 1. Pin its
> baseline to the last regressing release, or retire the fixture for the next symptom
> worth reproducing. Do **not** soften criterion 1 into a warning — that is the
> vacuity the stage exists to prevent.

Each binary runs in its **own env dir** (its own `.rivet_state.db`, which lives next
to the config), and all three stages pin `RIVET_STATE_URL=""`: the new binary UPGRADES
the state schema (v18→v19), which the old binary then cannot open — never share a
state dir across versions. The field replay additionally reads its shed counts out of
that SQLite `run_journal`, so a leaked process-wide state URL would make a live
fixture read as dead.

### A missing baseline FAILS — that is the point

`RIVET_PREV_RELEASE_BIN` absent (or not executable) is **FAIL**, not SKIP, for all
three stages. This is the one deliberate exception to the gate's "a down service is
SKIP" contract, and 0.24.4 is the reason: **a check that grades nothing must not
report a non-failure.** Everything else in `regression.py` keeps the ordinary
contract — an absent `RIVET_REGRESSION_SOURCE_URL` or `RIVET_SCALE_<ENGINE>_URL` is a
down service, not a missing baseline, and still SKIPs.

The escape is explicit, named after what it costs, and never a default:

```
python3 -m dev.release_oracle --without-prev-release-comparison   # or RIVET_ORACLE_WITHOUT_PREV_RELEASE=1
```

It turns those three stages back into SKIPs for a local partial run. Every row it
records says the run *does not grade the release against the binary users are
running*, the driver prints it beside the state backend at start-up, and it prints
`NOT RELEASE-GRADED` after the final verdict — because `RELEASE-READY` is derived
from the rows and is literally true ("every non-skipped cell is green"), which is
exactly the sentence 0.24.4 shipped under. **A run carrying this flag cannot support
a tag.** `make release-oracle-full` downloads the baseline for you and never needs it,
and so does `make release-oracle-bless` (which depends on the download and now passes
it through). `make release-oracle` — the BARE target, which by design carries only
what is already in your shell — passes the flag for you: an entry point with no
baseline must give the comparison up BY NAME rather than go red on three stages over
an absence it was never going to carry.

Timeouts come in TWO layers, deliberately: the harness's own per-unit budget
(`RIVET_AB_TIMEOUT`, default 900s per case-run; `RIVET_FIELD_TIMEOUT`, default 3600s
per leg) and the *wrapper's* whole-harness budget (`RIVET_ORACLE_AB_TIMEOUT` /
`RIVET_ORACLE_FIELD_TIMEOUT`, each defaulting to the harness's budget × its unit
count + slack). They must not be one knob: reading the same variable for both made
the wrapper's budget the smaller number, so it always fired first and the harness's
own graceful timeout could never be reached. The wrapper's expiry sends **SIGINT**
and waits `RIVET_ORACLE_CHILD_GRACE` (default 300s) before SIGKILL, because both
harnesses clean up a SHARED stand on the way out (server-wide MySQL tmp-table
globals; a seeded fixture table) and no SIGKILLed process runs its cleanup. After
the replay returns — for any reason, including a kill — the wrapper reads
`@@GLOBAL.{internal_tmp_mem_storage_engine,tmp_table_size,max_heap_table_size}` back
off `rivet-mysql-1`, restores them to `DEFAULT` if the flip is still there, and
records a FAIL row naming the poisoned stand. A harness that times out or dies
without naming a scenario/criterion is a FAIL with its last output attached, never a
quiet pass. The replay needs the **batch
MySQL stand** (`rivet-mysql-1` at 127.0.0.1:3306) and the differential the **batch
Postgres stand** (`rivet-postgres-1`) plus `duckdb` — with the stand down, criterion 1
(or the harness's own `expected_rows` guard) fails, which is the intended report:
"this did not grade anything", not a skip.

RED-proven: corrupting a part the prev release wrote reddens the format check (cur
`validate` no longer PASSES); a genuinely slower binary (a debug build, ~1.5×) reddens
the perf check below its slowdown.

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
- **A missing previous-release baseline is a FAIL, not a SKIP.** A gate that grades
  nothing must not report a non-failure; the only way to make those three stages skip
  is `--without-prev-release-comparison`, which says so in every row and in the final
  line. (0.24.4.)
- A harness stage that graded **fewer subjects than it must** — scenarios compared,
  criteria evaluated — fails on the COUNT, before anyone reads its verdict.
- The gate exits non-zero (**NOT RELEASABLE**) if any non-skipped cell fails.

## Layout

```
matrix.yaml              # declarative source of truth (engines × versions × scenarios × stores)
python3 -m dev.release_oracle  # the driver (orchestration loop); --bless-local re-blesses the local goldens
lib/cfg.py               # matrix query interface (dependency-free — no PyYAML)
dev/release_oracle/scenarios.py         # the scenario implementations (verdicts / integrity_types / load / gc)
dev/release_oracle/regression.py        # the three previous-release stages (format+perf, differential, field replay)
dev/release_oracle/bigquery.py          # the BigQuery golden stage (rivet run → GCS, rivet load → BQ, bq query)
dev/pytools/ab_regression.py            # the differential harness the gate drives (also runnable standalone)
dev/pytools/field_replay.py             # the field-symptom replay harness (four criteria, fixed in the file)
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
