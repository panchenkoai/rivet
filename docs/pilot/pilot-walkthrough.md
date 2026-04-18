# Pilot Walkthrough — From Discovery to Verified Repair

This is the end-to-end pilot guide that exercises the full contract stack: discovery, plan/apply, prioritization, chunked extraction with checkpoint, partition-level reconcile, targeted repair, and the committed/verified progression boundary.

If you just want to export one table, start with [Quickstart: Postgres](quickstart-postgres.md) or [Quickstart: MySQL](quickstart-mysql.md). This walkthrough is for pilots preparing a real production rollout.

> Contracts referenced below:
> [PA1–PA8](../adr/0005-plan-apply-contracts.md) plan/apply ·
> [CC1–CC10](../adr/0007-cursor-policy-contracts.md) cursor policy ·
> [PG1–PG8](../adr/0008-export-progression.md) progression ·
> [RC1–RC6 / RR1–RR8](../adr/0009-reconcile-and-repair-contracts.md) reconcile / repair.

---

## Prerequisites

- Postgres or MySQL you can reach (structured creds or a `DATABASE_URL`).
- `rivet --version` works.
- A writeable local path or an S3/GCS bucket for output.

The repo ships a `docker-compose.yaml` with both engines pre-seeded by [`dev/postgres/init.sql`](../../dev/postgres/init.sql) / [`dev/mysql/init.sql`](../../dev/mysql/init.sql) and the bench seed tool (`cargo run --bin seed`). Follow along on that if you don't have a source handy.

```bash
docker compose up -d postgres mysql
cargo run --bin seed -- --target both      # fills orders, events, orders_coalesce, orders_sparse
export DATABASE_URL='postgresql://rivet:rivet@localhost:5432/rivet'
```

For a bigger / richer fixture (14 tables, source-group conflict scenarios, composite cursor) use the dedicated demo fixture — see [demo-quickstart.md](demo-quickstart.md).

### Production note — TLS and credential handling

Everything below works with local-dev settings. For a real pilot against a managed database:

- **TLS is on by default when you set `tls:`**. The recommended shape:

  ```yaml
  source:
    type: postgres
    url_env: DATABASE_URL
    tls:
      mode: verify-full
      ca_file: /etc/ssl/certs/rds-ca-2019-root.pem   # if your CA is not in system trust
  ```

  See [reference/config.md § TLS](../reference/config.md#tls) for the full matrix (`disable` | `require` | `verify-ca` | `verify-full`). Omitting `tls:` connects in plaintext and logs a WARN.

- **Never put the DB URL on the command line in prod.** Use `--source-env` for `rivet init`:

  ```bash
  export DATABASE_URL='postgresql://…'
  rivet init --source-env DATABASE_URL --schema public --discover -o discovery.json
  ```

  And use `url_env:` / `password_env:` in YAML. See [reference/init.md](../reference/init.md#avoiding-credentials-on-the-command-line).

---

## Step 1 — Discovery (`rivet init`)

Scaffold a YAML from the live schema and, in parallel, emit a **machine-readable discovery artifact** for review or automation (Epic B).

```bash
# YAML scaffold for a whole schema
rivet init --source-env DATABASE_URL --schema public -o pilot.yaml

# JSON discovery artifact — per-table ranked cursor + chunk candidates,
# row estimate, on-disk size, coalesce hints when `updated_at` is nullable.
rivet init --source-env DATABASE_URL --schema public --discover -o discovery.json
```

Inspect `discovery.json` to decide modes and cursor policies:

```bash
jq '.tables[] | {table, suggested_mode, best_cursor: (.cursor_candidates[0].column // null),
                 coalesce_fallback: .suggested_cursor_fallback_column, notes}' discovery.json
```

---

## Step 2 — Write a chunked + checkpoint config

For any non-trivial table, use chunked mode with `chunk_checkpoint: true`. Checkpointing is what unlocks reconcile (Epic F), repair (Epic H), and progression (Epic G).

```yaml
source:
  type: postgres
  url_env: DATABASE_URL
  tuning:
    profile: balanced

exports:
  - name: orders
    query: "SELECT id, user_id, product, price, status, updated_at FROM orders"
    mode: chunked
    chunk_column: id
    chunk_size: 100000
    chunk_checkpoint: true          # required for reconcile/repair/progression (PG/RC/RR)
    parallel: 2
    format: parquet
    destination:
      type: local
      path: ./output

  # Composite cursor fixture — `updated_at` is nullable, fall back to `created_at`.
  - name: orders_coalesce
    query: "SELECT id, product, price, updated_at, created_at FROM orders_coalesce"
    mode: incremental
    cursor_column: updated_at
    cursor_fallback_column: created_at
    incremental_cursor_mode: coalesce    # ADR-0007 CC1
    format: parquet
    skip_empty: true
    destination:
      type: local
      path: ./output
```

Validate structural constraints:

```bash
rivet check --config pilot.yaml
rivet doctor --config pilot.yaml
```

---

## Step 3 — Plan (see the full intent)

`rivet plan` seals the execution intent into an auditable artifact (ADR-0005 PA1) and embeds source-aware prioritization (ADR-0006) when multiple exports are planned.

```bash
rivet plan -c pilot.yaml
```

A single-export plan prints a `Priority` block; a multi-export plan adds a `Campaign` block with waves and `source_group` warnings (if set). A JSON artifact is what CI/CD pipelines should consume:

```bash
rivet plan -c pilot.yaml --format json -o plan.json
```

**What the plan guarantees (PA1–PA8):**
- PA1 — the artifact is the sole input to `apply`.
- PA3 — apply bails on plans older than 24h (override with `--force`).
- PA4 — for incremental exports, apply bails if another run moved the cursor in the meantime.
- PA5 — chunk ranges in the artifact are monotonic by construction.

---

## Step 4 — Run (or apply the sealed plan)

Either run live:

```bash
rivet run -c pilot.yaml --validate
```

…or apply the plan for an auditable split between "what will happen" and "do it":

```bash
rivet apply plan.json
```

What happens under the hood for chunked:

- For each chunk task: `SELECT ... WHERE id BETWEEN start AND end ORDER BY id` → Arrow → Parquet → destination → manifest entry → `chunk_task.status = 'completed'`.
- Ordering: **write → manifest → cursor → metric** (ADR-0001 I1–I4).
- On success: `last_committed_chunk_index` advances in `export_progression` (PG2, PG4).

---

## Step 5 — Inspect progression

Get the explicit committed / verified boundary per export:

```bash
rivet state progression -c pilot.yaml

EXPORT            COMM MODE    COMMITTED         COMMITTED AT             VERI MODE  VERIFIED
orders            chunked      chunk #9          2026-04-18 12:20:15 UTC  -          -
orders_coalesce   incremental  2026-04-18T00:05  2026-04-18 12:21:02 UTC  -          -
```

At this point:
- Committed = "data is at the destination and recorded in the manifest" (PG2).
- Verified is still empty — no reconcile has run yet (PG5).

---

## Step 6 — Reconcile (Epic F)

Partition-level `COUNT(*)` on the source, compared with per-chunk `rows_written` stored in the checkpoint.

```bash
rivet reconcile -c pilot.yaml -e orders
```

Possible outcomes per partition (RC3):

- `match` — source and exported counts equal.
- `mismatch` — both counts known but differ → repair candidate.
- `unknown` — a count is missing (chunk never completed, unparseable keys) → repair candidate.

If **every** partition matches (zero mismatches and zero unknowns), `last_verified_chunk_index` advances (RC6 / PG5). Save a JSON report for audit:

```bash
rivet reconcile -c pilot.yaml -e orders --format json -o reconcile.json
```

The reconcile SQL uses exactly the same `build_chunk_query_sql` shape the pipeline used during extraction (RC2), so the comparison is apples-to-apples.

---

## Step 7 — Targeted repair (Epic H)

If the reconcile report is dirty, derive a repair plan from it:

```bash
# Dry run — prints the plan, runs no queries, writes no files (RR2).
rivet repair -c pilot.yaml -e orders --report reconcile.json

# Execute just the flagged chunks.
rivet repair -c pilot.yaml -e orders --report reconcile.json --execute
```

What `--execute` does:

- Re-runs only the flagged chunk ranges via `run_chunked_sequential(ChunkSource::Precomputed)` — same SQL shape as extraction and reconcile (RR3).
- Writes **new** output files alongside originals with `<export>_<ts>_chunk<idx>.<ext>` naming — Rivet does **not** delete or overwrite prior files (RR5). Downstream deduplication is the operator's responsibility (or put the output under a versioned prefix / partitioned path).
- Leaves `last_committed_*` untouched (RR4) — the chunk index was already covered at the original run; repair is corrective, not commitment.

---

## Step 8 — Re-verify

After repair, rerun reconcile to advance verified:

```bash
rivet reconcile -c pilot.yaml -e orders

rivet state progression -c pilot.yaml

EXPORT            COMM MODE    COMMITTED    COMMITTED AT             VERI MODE  VERIFIED
orders            chunked      chunk #9     2026-04-18 12:20:15 UTC  chunked    chunk #9
orders_coalesce   incremental  ...          ...                      -          -
```

Now both boundaries agree: everything committed is also verified against the source.

---

## Step 9 — Automate

A minimal daily cron that runs, reconciles, and fails loudly on unresolved mismatches:

```bash
#!/usr/bin/env bash
set -euo pipefail
cd /opt/rivet && export DATABASE_URL='…'

rivet run       -c pilot.yaml --validate
rivet reconcile -c pilot.yaml -e orders --format json -o /var/log/rivet/reconcile-$(date +%F).json

# Fail the job if reconcile is not clean (zero mismatches AND zero unknowns).
if ! jq -e '.summary.mismatches == 0 and .summary.unknown == 0' \
      /var/log/rivet/reconcile-$(date +%F).json > /dev/null; then
  echo "reconcile dirty — see report"
  exit 1
fi
```

For CI-style review, use the plan/apply split:

```bash
# In CI (build stage)
rivet plan -c pilot.yaml --format json -o plan.json
# Review plan.json in a PR — prioritization block tells you what's heavy/risky.

# In CI (deploy stage)
rivet apply plan.json
```

---

## Contract cheat sheet

| Question | Contract | Answer |
|---|---|---|
| "Will apply run on a stale plan?" | PA3 | No, hard reject at 24h without `--force`. |
| "Can apply run if another `rivet run` advanced the cursor?" | PA4 | No, apply bails with a drift message (incremental only). |
| "Can repair accidentally regress the cursor?" | PG3, RR4 | No: incremental committed is monotonic; repair never touches committed. |
| "Does `coalesce` mode leak a synthetic column to my files?" | CC5 | No, `_rivet_coalesced_cursor` is stripped before write. |
| "Is a chunk whose file landed but whose manifest write failed lost?" | I7, PG2 | No — file is at the destination; only manifest is missing. `rivet reconcile` surfaces it as `unknown`. |
| "Does reconcile write anything other than progression?" | RC5, PG5 | No — reports are ephemeral JSON; only `last_verified_*` is persisted when all partitions match. |
| "Does `rivet repair --execute` delete old bad files?" | RR5 | No. New files sit alongside originals. Clean up downstream. |

---

## What's next

- [Production checklist](production-checklist.md) — hardening before real workloads.
- [UAT checklist](uat-checklist.md) — pilot sign-off structure.
- [Tuning](../reference/tuning.md) — profiles, batch_size, memory-aware FETCH.
- [Planning / prioritization](../planning/prioritization.md) — reading and trusting the advisory block.
