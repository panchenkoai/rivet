_Last updated: 2026-06-13._

# Getting Started

Rivet exports tables from PostgreSQL, MySQL, or SQL Server to Parquet (or CSV) files — locally, to S3, GCS, or Azure Blob Storage. Point it at a database, scaffold a config from your real tables, then run.

```bash
brew install panchenkoai/rivet/rivet
export DATABASE_URL='postgresql://user:pass@localhost:5432/mydb'
# `orders` is a placeholder — use one of YOUR tables, or omit --table to scan the whole schema
rivet init --source-env DATABASE_URL --table orders -o rivet.yaml
rivet run -c rivet.yaml --validate
```

That's the whole flow. The four steps below explain each command, expected output, and where to go from each. Read time: ~3 minutes.

> **Already running it locally?** Jump to [§3 Preflight & run](#3-preflight--run). If you're evaluating it for production, finish this page first, then continue with [docs/pilot/](pilot/README.md).

---

## 1 · Install

```bash
# macOS / Linux — Homebrew (recommended)
brew install panchenkoai/rivet/rivet
rivet --version
```

```bash
# Docker — try without installing anything
docker run --rm ghcr.io/panchenkoai/rivet:latest --version
```

Other install paths — pre-built binaries for every platform, `cargo install rivet-cli`, build from source, plus the full Docker recipe with database-on-host pointers (`host.docker.internal` vs `--network host`) — live in the project [README § Installation](../README.md#installation). Shell completions: `rivet completions bash|zsh|fish`.

## 2 · Connect & scaffold a config

Recommended pattern: put the connection URL in an environment variable and reference it from the config so credentials never enter the file or shell history.

```bash
export DATABASE_URL='postgresql://user:pass@localhost:5432/mydb'
# MySQL: same flag, just a mysql:// URL
# export DATABASE_URL='mysql://user:pass@localhost:3306/mydb'

rivet init --source-env DATABASE_URL --table orders -o rivet.yaml
# `orders` is a placeholder — use one of YOUR tables, or omit --table to scan the whole schema
```

`rivet init` connects once, reads the column list + a rough row estimate from the live database, and writes a YAML file with `url_env: DATABASE_URL` and a sensible default mode. For large tables it picks `mode: chunked`, auto-resolves the chunk key from the primary key, and **scales `parallel:` to the row estimate** (1 / 2 / 4) so a big export uses several connections out of the box — measured ~1.4× faster on a wide 2 M-row table and ~4× on a narrow one, with no flags to set. You can also point it at a whole schema (`--schema public`) or emit a richer JSON discovery artifact instead (`--discover -o discovery.json`).

Full flag reference: [reference/init.md](reference/init.md). For a manually-authored YAML instead of `rivet init`, see [reference/config.md](reference/config.md).

> **State file.** Rivet creates `.rivet_state.db` next to the config (cursors, chunk checkpoints, run history). Add it to `.gitignore` if the folder is version-controlled — see [SECURITY.md § Sensitive local artifacts](../SECURITY.md#sensitive-local-artifacts).

## 3 · Preflight & run

```bash
rivet doctor -c rivet.yaml   # verify source + destination auth
rivet check  -c rivet.yaml   # dry-run analysis per export
rivet run    -c rivet.yaml --validate --reconcile
```

The full basic workflow (`init` → `doctor` → `check` → `run` → `state`) recorded as a single terminal cast:

![Basic workflow](gifs/basic.gif)

What each step does:

- **`rivet doctor`** — connects to the source and writes a 1-byte probe to every destination; fixes nothing, fails loudly on any auth / network issue.
- **`rivet check`** — runs `EXPLAIN` against your queries, estimates row counts, detects whether your cursor / chunk columns are indexed, and emits a verdict + concrete suggestion. Verdicts are `EFFICIENT` · `ACCEPTABLE` · `DEGRADED` · `UNSAFE`; the last two always carry a mode-aware `Suggestion:` line.

  ![rivet check verdict block](gifs/check-verdict.gif)

- **`rivet run --validate --reconcile`** — extracts. `--validate` reads each output file back and verifies its row count; `--reconcile` runs `SELECT COUNT(*)` on the source query and compares with what was exported.

Example summary card after a successful run:

```
── orders ──
  run_id:      orders_20260519T120000.123
  status:      success
  tuning:      profile=balanced (default), batch_size=10,000
  rows:        5,432
  files:       1
  output:      file://./output
  bytes:       847 KB
  duration:    1.2s
  peak RSS:    15 MB (sampled during run)
  validated:   pass
  schema:      unchanged
  reconcile:   MATCH (5,432/5,432)
```

## 4 · Inspect & iterate

```bash
rivet state show   -c rivet.yaml             # cursors (incremental exports)
rivet metrics      -c rivet.yaml --last 10   # per-run history
rivet state files  -c rivet.yaml             # files actually written
rivet journal      -c rivet.yaml --export orders   # per-run events / retries / quality issues
```

![Post-run inspection: state show, metrics, state files, state progression](gifs/inspect.gif)

To make the second run only export rows that changed, switch the export to **incremental** mode with a `cursor_column:` (must be monotonically increasing — usually `updated_at` or a sequence id):

```yaml
exports:
  - name: orders
    query: "SELECT id, name, updated_at FROM orders"
    mode: incremental
    cursor_column: updated_at
    format: parquet
    skip_empty: true            # no file when there are no new rows
    destination:
      type: local
      path: ./output
```

Subsequent `rivet run` invocations will only fetch rows with `updated_at >` the stored cursor. For tables larger than ~5 M rows, switch to `mode: chunked` instead — see [modes/chunked.md](modes/chunked.md).

---

## 5 · Many tables: plan once, apply by waves

When a config has several exports, `rivet plan` assigns each one a **wave** — a priority band derived from its size, chunking strategy, and risk ([ADR-0006](adr/0006-source-aware-prioritization.md)) — and writes it back into the config as a `wave:` field you can see and hand-edit:

```bash
rivet plan -c rivet.yaml        # writes `wave: N` onto every export, in place
```

```yaml
exports:
  - name: users
    wave: 1        # small / cheap → runs first
    # …
  - name: events
    wave: 3        # large → runs later
    # …
```

`rivet apply` then runs the whole config **wave by wave**, lowest first, with a barrier between waves — every export in wave 1 finishes before wave 2 starts. Exports with no `wave:` run last:

```bash
rivet apply rivet.yaml          # a .yaml path → wave-ordered execution
```

(A `.json` path still means the sealed single-artifact replay — see [reference/cli.md § rivet apply](reference/cli.md#rivet-apply).) The plan *suggests* the waves; you stay in control — hand-edit `wave:` and `apply` respects your order.

### Parallel within a wave — only where it's safe

Add `parallel_export_processes: true` (or pass `rivet apply --parallel-export-processes`) and, within each wave, the **cheap** exports — the ones `rivet plan` marked `parallel_safe: true` (cost class `Low`, under ~100K rows) — run concurrently as separate processes. A heavier export already chunk-parallelizes its own ranges *internally*, so it runs **alone** in its wave: two big tables at once would multiply the load on the source. Every child still self-throttles on source pressure + memory (the adaptive governor), so a whole wave at once stays bounded.

```yaml
parallel_export_processes: true   # top-level: parallelize the cheap (parallel_safe) exports within each wave
```

---

## When something is wrong

Rivet tries to fail early and say exactly what to fix — most mistakes are caught at `check` / `doctor` time, before a single row is read.

A query that references a table (or column) that doesn't exist is caught by `rivet check` — it exits non-zero with the offending name and SQLSTATE, instead of passing through to a half-finished run:

![rivet check catches a query against a missing table](gifs/error-missing-table.gif)

A typo in a config field is caught at parse time with a `Did you mean …?` suggestion that names the line:

![rivet names a mistyped config field and suggests the correct one](gifs/error-config-typo.gif)

An unreachable database — down, wrong host/port, or a tunnel that isn't up — is reported by `rivet doctor` with a reachability hint before you waste a run:

![rivet doctor reports an unreachable source with a hint](gifs/error-connection.gif)

More failure modes (retries, schema drift, crash/resume) and exactly what rivet does for each: [semantics.md](semantics.md).

---

## Next steps

| When you need to … | Go to |
|---|---|
| Pick the right export mode for each table | [modes/](modes/) — full · incremental · chunked · time_window |
| Configure S3 / GCS / Azure / stdout destinations | [destinations/](destinations/) |
| Look up a YAML field or a CLI flag | [reference/config.md](reference/config.md) · [reference/cli.md](reference/cli.md) |
| Understand `run_id` / cursor / chunk / manifest / journal | [concepts.md](concepts.md) |
| Tune for memory, throughput, source pressure | [reference/tuning.md](reference/tuning.md) · [best-practices/](best-practices/) |
| Take it to production (read replicas, poolers, monitoring) | [pilot/production-checklist.md](pilot/production-checklist.md) |
| Run a serious pilot (chunked + reconcile + repair on your data) | [pilot/pilot-walkthrough.md](pilot/pilot-walkthrough.md) |
| See exactly what happens under retry / crash / resume | [semantics.md](semantics.md) |
| Auditable plan/apply workflow for CI/CD | [reference/cli.md § rivet plan](reference/cli.md#rivet-plan) · [ADR-0005](adr/0005-plan-apply-contracts.md) |
