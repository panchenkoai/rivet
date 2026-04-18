# Demo Quickstart — Pilot Evaluation in ≈10 Minutes

A scripted, reproducible end-to-end demo that exercises every post-Epic feature against a pre-seeded fixture. Use this when evaluating Rivet for a pilot: you get a 14-table database, a 12-export campaign, partition-level reconcile, targeted repair, and the full committed/verified progression — all wired together.

> For the conceptual tour of the same features with your own data, see [pilot-walkthrough.md](pilot-walkthrough.md).
> For supported database versions and the CI compat matrix, see [reference/compatibility.md](../reference/compatibility.md).

---

## What this demo shows

| Capability | Where it surfaces | ADR |
|---|---|---|
| Metadata-driven discovery | `rivet init --discover` — ranked cursor + chunk candidates per table | [0006](../adr/0006-source-aware-prioritization.md) |
| Source-aware prioritization | `rivet plan` emits a per-export score, class, and wave | 0006 |
| Campaign-level planning | Multi-export plan includes ordered list + `source_group` warnings | 0006 |
| Cursor policy (`coalesce`) | Composite cursor `COALESCE(updated_at, created_at)` for nullable primaries | [0007](../adr/0007-cursor-policy-contracts.md) |
| Plan / Apply contract | Sealed JSON artifact (`PlanArtifact`) + staleness + credential redaction | [0005](../adr/0005-plan-apply-contracts.md) |
| Chunked + checkpoint | 800k-row `audit_log` split into chunks, each tracked in state | ADR-0001 I5 |
| Partition reconcile | `rivet reconcile` re-counts every chunk on the source | [0009](../adr/0009-reconcile-and-repair-contracts.md) |
| Targeted repair | Inject mismatch → `rivet repair --execute` fixes only affected chunks | 0009 |
| Committed / verified progression | `rivet state progression` surfaces both boundaries | [0008](../adr/0008-export-progression.md) |

---

## Prerequisites

- Docker Desktop running, `docker compose up -d postgres mysql` finished healthy.
- Rust toolchain; build once:

  ```bash
  cargo build --release --bin rivet --bin seed
  ```

- `python3` and `jq` (both used only for pretty-printing JSON artifacts below).

Container quick check:

```bash
docker compose ps postgres mysql
```

---

## 0 — Seed the demo fixtures

Two SQL files in `demo/` create the 14-table landscape with varied cardinalities, cursor qualities, and source-group scenarios:

```bash
# PostgreSQL fixture — ≈2 seconds. Adds 7 tables alongside the bundled dev schema.
PGPASSWORD=rivet psql -h localhost -U rivet -d rivet \
    -f demo/setup_demo_tables.sql

# MySQL fixture — ≈10 seconds. Same 7 tables, idiomatic MySQL.
mysql -h 127.0.0.1 -P 3306 -u rivet -privet rivet \
    < demo/setup_demo_tables_mysql.sql

# Bundled dev tables + orders_coalesce (composite-cursor fixture) come from the
# Rust seeder — tunable scale.
cargo run --release --bin seed -- --target postgres \
    --users 2000 --orders-per-user 5 --events-per-user 20 \
    --page-views 200000 --content-items 20000 \
    --sparse-chunk-demo --sparse-chunk-rows 500 --sparse-chunk-id-gap 5000 \
    --coalesce-rows 5000 --coalesce-null-ratio 0.35
```

Verify the landscape (PostgreSQL):

```bash
PGPASSWORD=rivet psql -h localhost -U rivet -d rivet -c "
SELECT relname AS table_name, reltuples::bigint AS est_rows,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('r','p')
ORDER BY reltuples::bigint DESC;"
```

Expected counts (≈): `audit_log` 800k · `metric_samples` 400k · `transactions` 300k · `page_views` 200k · `logs_archive` 100k · `sessions` 50k · `events` 40k · `content_items` 20k · `email_queue` 20k · `orders` 10k · `orders_coalesce` 5k · `product_catalog` 3k · `users` 2k · `orders_sparse` 500.

---

## 1 — Discovery (`rivet init --discover`)

```bash
export DATABASE_URL='postgresql://rivet:rivet@localhost:5432/rivet'
cd demo && mkdir -p {out,plans}

# Credentials never hit the command line.
../target/release/rivet init \
    --source-env DATABASE_URL \
    --schema public --discover -o discovery.json
```

Per-table summary:

```bash
python3 <<'PY'
import json
d = json.load(open('discovery.json'))
print(f"{d['scope']}\n")
for t in d['tables']:
    top = t['cursor_candidates'][0] if t['cursor_candidates'] else None
    top_s = f"{top['column']}({top['score']})" if top else '-'
    fb = t.get('suggested_cursor_fallback_column') or '-'
    note = '⚠ coalesce' if fb != '-' else ''
    print(f"{t['table']:<20} {t['row_estimate']:>7}  mode={t['suggested_mode']:<11} "
          f"cursor={top_s:<16} fallback={fb:<12} {note}")
PY
```

Expected: `page_views`, `audit_log`, `metric_samples`, `transactions` → `chunked`. `orders_coalesce` and `logs_archive` → `⚠ coalesce` (automatic hint when the best cursor is nullable and a NOT NULL sibling exists).

---

## 2 — The demo campaign (`rivet plan`)

A curated, 12-export YAML lives at `demo/demo_pipeline.yaml` with deliberate `source_group` collisions to trigger the campaign-level warning.

```bash
../target/release/rivet plan \
    -c demo_pipeline.yaml \
    --format json > plans.jsonstream
```

Render the embedded `campaign` block:

```bash
python3 <<'PY'
import json
raw = open('plans.jsonstream').read()
dec = json.JSONDecoder()
arts, pos = [], 0
while pos < len(raw):
    while pos < len(raw) and raw[pos].isspace(): pos += 1
    if pos >= len(raw): break
    obj, end = dec.raw_decode(raw, pos); arts.append(obj); pos = end
camp = arts[0]['prioritization']['campaign']
print(f"{'score':>5}  {'wave':<4}  {'export':<18}  {'class':<7}  {'cost':<10}  {'group':<20}")
for e in camp['ordered_exports']:
    sg = e.get('source_group') or '-'
    print(f"{e['priority_score']:>5}  w{e['recommended_wave']:<3}  {e['export_name']:<18}  "
          f"{e['priority_class']:<7}  {e['cost_class']:<10}  {sg:<20}")
print('\nSource-group warnings:')
for w in camp['source_group_warnings'] or ['(none)']: print(f"  ⚠ {w}")
PY
```

Expected:
- **Wave 1** (score 76–88) — indexed-cursor incrementals: `events`, `sessions`, `transactions`.
- **Wave 3** — small full exports.
- **Wave 4** — heavy chunked exports; `audit_log` lowest (reconcile_required + chunking_heavy + degraded verdict).
- Warning: `Source group 'replica_primary': 3 exports share this source — stagger large runs`.

---

## 3 — Plan / Apply (sealed workflow)

Pick one wave-1 export and run it the plan/apply way:

```bash
../target/release/rivet plan \
    -c demo_pipeline.yaml -e events \
    --format json -o plan_events.json

# Verify the artifact does not leak secrets (PA9 — ADR-0005).
grep -c 'password' plan_events.json  # ≥ 0 matches as field names; the values are null
grep -E '"password":\s*"[^"]+"' plan_events.json || echo "✅ no plaintext password"

../target/release/rivet apply plan_events.json
```

Expected: `40209 rows, success`, a Parquet in `out/`, and `last_cursor` advanced.

---

## 4 — Chunked + reconcile + progression

```bash
../target/release/rivet run -c demo_pipeline.yaml -e audit_log
# 800000 rows, 4 chunks, ≈3s, `chunk_checkpoint: true` persists per-chunk state.

../target/release/rivet reconcile -c demo_pipeline.yaml -e audit_log
# Partitions: 4 (4 match, 0 mismatch, 0 unknown)

../target/release/rivet state progression -c demo_pipeline.yaml
# audit_log  chunked  chunk #3  ...  chunked  chunk #3       ← committed = verified
```

---

## 5 — Composite cursor demo

`orders_coalesce` and `logs_archive` have nullable `updated_at`; the demo YAML declares `incremental_cursor_mode: coalesce` with `cursor_fallback_column: created_at`:

```bash
../target/release/rivet run -c demo_pipeline.yaml -e logs_archive
# 100000 rows exported; the stored cursor is the max of COALESCE(updated_at, created_at)

# Second run — predicate filters everything out:
../target/release/rivet run -c demo_pipeline.yaml -e logs_archive
# status: success, rows: 0
```

The synthetic `_rivet_coalesced_cursor` column never reaches the Parquet file (ADR-0007 CC5):

```bash
# Peek at Parquet schema — no _rivet_coalesced_cursor column.
python3 - <<'PY'
import pyarrow.parquet as pq, glob
fn = sorted(glob.glob('out/logs_archive_*.parquet'))[-1]
print(pq.read_schema(fn))
PY
```

---

## 6 — Targeted repair (simulated mismatch)

Inject a 50k-row delete that flows through reconcile → repair:

```bash
# 1) Break chunk 2 on the source:
PGPASSWORD=rivet psql -h localhost -U rivet -d rivet -c \
    "DELETE FROM audit_log WHERE id BETWEEN 400001 AND 450000;"

# 2) Reconcile surfaces exactly one dirty partition:
../target/release/rivet reconcile -c demo_pipeline.yaml -e audit_log
# Partitions: 4 (3 match, 1 mismatch, 0 unknown)
# Repair candidates: chunk 2 [400001..600000] — diff=-50000

# 3) Dry-run the repair plan (RR2 — nothing executes without --execute):
../target/release/rivet repair -c demo_pipeline.yaml -e audit_log
# Actions: 1 — chunk 2 [400001..600000]

# 4) Execute — only the flagged chunk range runs, new file written alongside:
../target/release/rivet repair -c demo_pipeline.yaml -e audit_log --execute
# Summary: planned 1 · executed 1 · rows 150000
```

RR4 holds: `last_committed_*` in `rivet state progression` is **not** re-stamped by repair. The committed boundary tracks first-write coverage; verified re-advances only if a subsequent clean reconcile runs.

---

## 7 — MySQL parity (same demo, different engine)

Everything above works on MySQL too. One command sets up a parallel stack:

```bash
export DATABASE_URL='mysql://rivet:rivet@localhost:3306/rivet'

../target/release/rivet init \
    --source-env DATABASE_URL \
    --schema rivet --discover -o discovery_mysql.json

../target/release/rivet plan -c demo_pipeline_mysql.yaml --format json \
    > plans_mysql.jsonstream
```

A few MySQL notes:
- The same `source_group` warnings fire when 3+ exports share a replica.
- `rivet preflight` gives weaker cursor signals on MySQL than on PostgreSQL (MySQL `EXPLAIN` doesn't always annotate `type=range` for indexed incrementals), so scores tilt slightly lower. This is an observable difference, not a bug.
- Composite cursor SQL uses backticks (`` `updated_at` ``) instead of double-quoted identifiers — same contract, different dialect (ADR-0007 CC9).

---

## Cleanup

```bash
# Destroy demo output + state, keep containers.
rm -rf demo/{out,out_mysql,plans,*.json,*.jsonstream,.rivet_state.db}

# Drop the demo tables (keeps the dev base schema).
PGPASSWORD=rivet psql -h localhost -U rivet -d rivet -c "
    DROP TABLE IF EXISTS transactions, audit_log, sessions, product_catalog,
                         logs_archive, email_queue, metric_samples CASCADE;"
mysql -h 127.0.0.1 -P 3306 -u rivet -privet rivet -e "
    DROP TABLE IF EXISTS transactions, audit_log, sessions, product_catalog,
                         logs_archive, email_queue, metric_samples,
                         orders_coalesce;"
```

---

## What to report after the demo

For a pilot sign-off ([pilot/uat-checklist.md](uat-checklist.md)) the demo above exercises every box; record:

1. Output of `rivet state progression` before and after each stage.
2. The reconcile report (saved JSON from step 4 / 6) for audit.
3. The `plan` artifact used by apply (confirms PA9 redaction, PA6 fingerprint).
4. `cargo test` result from your own build (should match `2026 assertions, zero failures` per the release notes).

If any command above produces output unexpectedly, capture the full log with `RUST_LOG=debug` — that level includes the effective SQL queries and per-chunk state transitions.
