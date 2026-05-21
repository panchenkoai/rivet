_Last updated: 2026-05-19._

# Getting Started

Rivet exports tables from PostgreSQL or MySQL to Parquet (or CSV) files — locally, to S3, GCS, or Azure Blob Storage. Point it at a database, scaffold a config from your real tables, then run.

```bash
brew install panchenkoai/rivet/rivet
export DATABASE_URL='postgresql://user:pass@localhost:5432/mydb'
rivet init --source-env DATABASE_URL --table orders -o rivet.yaml
rivet run --config rivet.yaml --validate
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
```

`rivet init` connects once, reads the column list + a rough row estimate from the live database, and writes a YAML file with `url_env: DATABASE_URL` and a sensible default mode. You can also point it at a whole schema (`--schema public`) or emit a richer JSON discovery artifact instead (`--discover -o discovery.json`).

Full flag reference: [reference/init.md](reference/init.md). For a manually-authored YAML instead of `rivet init`, see [reference/config.md](reference/config.md).

> **State file.** Rivet creates `.rivet_state.db` next to the config (cursors, chunk checkpoints, run history). Add it to `.gitignore` if the folder is version-controlled — see [SECURITY.md § Sensitive local artifacts](../SECURITY.md#sensitive-local-artifacts).

## 3 · Preflight & run

```bash
rivet doctor --config rivet.yaml   # verify source + destination auth
rivet check  --config rivet.yaml   # dry-run analysis per export
rivet run    --config rivet.yaml --validate --reconcile
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
  tuning:      profile=balanced (default), batch_size=10000
  rows:        5432
  files:       1
  bytes:       847 KB
  duration:    1.2s
  peak RSS:    15MB (sampled during run)
  validated:   pass
  schema:      unchanged
  reconcile:   MATCH (5432/5432)
```

## 4 · Inspect & iterate

```bash
rivet state show   --config rivet.yaml             # cursors (incremental exports)
rivet metrics      --config rivet.yaml --last 10   # per-run history
rivet state files  --config rivet.yaml             # files actually written
rivet journal      --config rivet.yaml --export orders   # per-run events / retries / quality issues
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
