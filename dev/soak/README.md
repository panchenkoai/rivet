# Soak harness

A multi-hour run of rivet against the CDC stand under sustained write load, graded by
an independent ground-truth journal. **Not part of the release gate** — run it from time
to time, before a release that touches the runners or the CDC adapters, or after a
refactor you do not trust.

```bash
dev/pytools/cdc_stand.py up                  # the four CDC-stand engines (5434 / 3307 / 1434 / 27018)
cargo build --release                        # or pass --bin
make soak                                    # 4 h, all engines, batch + CDC, 200 ops/s/engine
make soak SOAK_ARGS="--duration 10m"         # smoke
uv run python -m dev.pytools.soak --engines pg,mysql --modes cdc --duration 1h --chaos kill
```

| flag | default | meaning |
|---|---|---|
| `--engines` | `pg,mysql,mssql,mongo` | engines to soak; one that is down is a loud **SKIP** with the reason |
| `--modes` | `batch,cdc` | batch = scheduled `incremental` + periodic full snapshots; cdc = bounded `until_current` cycles |
| `--duration` | `4h` | `10m`, `90s`, `1h30m` |
| `--rate` | `200` | target write ops/s per engine (achieved rate is reported per minute) |
| `--chaos` | `none` | `kill`: SIGKILL every `--kill-every`th run of each stream mid-flight; `net`: toxiproxy latency / connection resets (PostgreSQL via a harness-owned proxy on 15433, MySQL via `mysql_cdc_gremlin` on 13307; SQL Server and MongoDB have no free proxy port and run without it, reported as a SKIP) |
| `--red` | — | mutate the harness **input** to prove an oracle bites (below) |

Output goes to `dev/soak_runs/<timestamp>/` (gitignored): `soak-report.json`,
`soak-report.md`, one log per rivet run under `logs/`, the journals, the generated
configs under `work/` and any snapshot that failed its checks under `snapshots/`. Exit
code: `0` all verdicts PASS, `1` any FAIL (or a harness error), `2` nothing could be set
up.

## How it works

- **One table per engine**, `soak_load(id, version, updated_at, amount, payload)`, owned
  by the harness: created at start, dropped in a `finally` together with the PostgreSQL
  slot and the SQL Server capture instance (disabled *before* the drop).
- **Configs come from `rivet init`** (`--mode cdc | incremental | chunked`), generated in
  place so init's primary-key record sits beside them. `init.yaml` is kept verbatim; the
  run config `c.yaml` carries a header naming every soak-only edit: `cdc.rollover: 1000`,
  `chunk_size: 5000`, `chunk_by_key → chunk_column` for the range snapshot (init has no
  flag for range chunking), and on MongoDB `source.mongo.page_size` (keyset) /
  `parallel: 4` (range). The harness refuses to start an engine when init did not emit
  what it depends on (a bounded CDC export, `cursor_column: updated_at`,
  `chunk_by_key: id`) rather than patching it in.
- **A writer thread per engine** issues INSERT 60 / UPDATE 30 / DELETE 10, one
  transaction per ~1 s tick through the container CLI (pymongo for MongoDB). Updates
  bump `version` and `updated_at`; 30 % of them hit a 50-id hot set; a 2 000-row seed
  transaction opens the run and a 5 000-row transaction repeats every
  `--big-tx-every` seconds, crossing the CDC rollover and — because CDC runs set
  `RIVET_CDC_MAX_TX_ROWS=1000 RIVET_CDC_SPILL_DIR=1` — the spill path.
- **The journal** (`journal-<engine>.jsonl`) records every COMMITTED op: id, op,
  version, the `updated_at` it wrote, the values, send and ack time. A failed batch is
  probed (did its first insert land?) so a rolled-back batch is never journaled and a
  committed-but-errored one always is. The journal is the oracle; rivet's manifests and
  ledgers are never used to decide a verdict (snapshots read their *declared* parts, the
  consumer's view, and are graded against the journal).
- **Schedulers**, concurrent with the writers: CDC every `--cdc-interval`; batch
  `incremental` every `--inc-interval` with a full snapshot every `--snapshot-every`
  cycles, alternating keyset and range. The CDC anchor is pinned by one run **before**
  the writer starts (MySQL and MongoDB have client-side anchors; PostgreSQL creates its
  slot there; SQL Server's capture instance is enabled and waited ready first).
- **End of run**: writers stop, SQL Server's Agent is waited on until the change table
  holds every journaled op, one final drain / incremental run straight at the source,
  then the end-of-run oracles.

## What each check proves

| check | proves | how |
|---|---|---|
| `cdc.no_gap` | every committed op reached the parts | each journal ins/upd has an event with that `(id, version)`, each delete a delete event; re-delivered duplicates are counted, not failed (at-least-once) |
| `cdc.no_gap.periodic` | no gap opened mid-soak, not just at the end | every `--check-every` cycles: ops acked more than `--settle` s before the cycle opened must already be in the parts |
| `cdc.replay_matches_source` | the parts replay to the source's final state, per column | latest event per id by `(__pos, __seq)` (per-engine position parse), deletes applied, full outer join with the source in ONE DuckDB session (PostgreSQL / MySQL / SQL Server ATTACHed; MongoDB exported by pymongo into the same session) |
| `cdc.order_consistent` | `(__pos, __seq)` orders changes correctly | the position-latest event of each id is also its highest `version` |
| `harness.journal_matches_source` | the oracle itself is right | the journal's expected state equals the source; if this fails, no other verdict of that engine means anything |
| `incremental.complete(.periodic)` | the cursor never skipped a committed change | every never-deleted id whose last change is older than the parts' own max `updated_at` minus `--settle` is present at that version or later; at the end, at exactly the journal's last version |
| `incremental.values` | the exported row carries the committed values | per `(id, version)`: `amount`, `payload`, `updated_at` equal the journal's |
| `snapshot.*` | each full snapshot is a sound non-atomic read | no duplicate PK; every id alive for the whole run present; no id that was never alive during it; count within `[start − deletes during, start + inserts during]` |
| `cycles.<stream>` | runs succeed or recover | every run exits 0, or fails (or is chaos-killed) and the next run of the same stream succeeds (up to 3 in a row under `--chaos net`); the last must succeed |
| `rss.<stream>` | no memory leak | peak RSS (`/usr/bin/time`) of the last quarter ≤ 1.5× the first quarter, or within +16 MiB; gated for `cdc` and `incremental`, INFO for snapshots (their input grows with the table) |
| `retention.slot_lag_bytes` | rivet does not pin PostgreSQL's WAL | slot lag sampled after every CDC cycle; FAIL when the last quarter's **minimum** exceeds 2× the first quarter's maximum and 64 MiB (a linearly growing, pinned lag has last-min ≈ 3× first-max; a healthy one falls back after every ack) |
| `retention.*` (others) | observability | MySQL binlog files/bytes, SQL Server change-table rows, MongoDB oplog window: retention there is reader-independent, so rivet cannot pin it — reported, not graded |
| `duration.<stream>` | slowdowns | median cycle time first vs last quarter; WARN past 3× |
| `harm.<dir>` | rivet's own source-harm counters | `export_harm` rows from each config's state DB — reported, never graded |

## What it cannot prove

- **Deletes are invisible to incremental.** A cursor read never sees a DELETE; the
  harness exempts deleted ids from the incremental checks and reports how many deleted
  ids still sit in the incremental output (expected, not a defect).
- **Single serial writer per engine.** Commit order equals `updated_at` order, so the
  classic concurrent-writer cursor race (a late commit with an early timestamp) is not
  exercised. The settle window exists for engines whose capture lags (SQL Server's
  Agent), not for that race.
- **One table, one shape.** No type fidelity beyond `BIGINT / TIMESTAMP(6) /
  DECIMAL(18,2) / VARCHAR` with a non-ASCII + quote payload; the type matrices cover
  that.
- **Local destination only.** No cloud store, no warehouse load.
- **Stand noise.** The machine is shared; duration trends are WARN only, and the RSS
  rule has a 16 MiB floor so allocator noise on a ~25 MiB process does not fail a run.
- **Under 8 samples, no trend verdict** (INFO), so a short smoke says nothing about
  leaks; and a pinned slot is only failed once its lag passes 64 MiB, which a 10-minute
  smoke at the default rate may not reach. The grading rules themselves are checked
  offline by `python -m dev.pytools.soak --self-test` (a pinned series and a leaking RSS
  series must FAIL, noise must PASS).
- **The PostgreSQL slot is the only log rivet can pin.** The other three engines'
  retention is reported for trend-reading only.
- **Periodic checks are sampled**, every `--check-every` cycles; a gap that opens and
  heals between two samples is caught only by the end-of-run check if it is a real loss.

## Proving the oracles bite (`--red`)

Each mutation changes the harness's own input, never rivet, and must turn exactly its
target check red:

| `--red` | mutation | must FAIL |
|---|---|---|
| `drop-cdc-part` | the newest CDC part is left out of the read-back | `cdc.no_gap`, `cdc.replay_matches_source` |
| `corrupt-source` | one source `amount` is changed after the final drain, unjournaled | `harness.journal_matches_source`, `cdc.replay_matches_source` |
| `dup-snapshot-part` | a declared snapshot part is read twice | `snapshot.no_duplicate_pk` |
| `drop-inc-part` | the newest incremental part is left out | `incremental.complete` |

```bash
uv run python -m dev.pytools.soak --engines pg --duration 2m --rate 50 --red corrupt-source   # must exit 1
```
