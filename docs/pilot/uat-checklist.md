# UAT Checklist

**Audience:** pilot users validating Rivet before production use.

**When to use:** at the end of a pilot, before promoting to production, or when verifying a new release.

**Prerequisites:** completed [Getting Started](../getting-started.md) and at least one successful export.

For the full internal acceptance test plan with detailed suites and smoke-test scripts, see [dev/USER_TEST_PLAN.md](../../dev/USER_TEST_PLAN.md).

---

## Pre-flight

- [ ] `rivet doctor` passes — source and all destinations authenticated
- [ ] `rivet check` passes — all exports show `EFFICIENT` or `ACCEPTABLE` verdict
- [ ] No `UNSAFE` exports (full table scans on very large tables)

## Basic export

- [ ] `rivet run --config rivet.yaml --validate` completes with `status: success`
- [ ] Row count in summary matches expected
- [ ] Output files exist at the configured destination

## Incremental / re-run

- [ ] Second run produces only new rows (cursor advanced correctly)
- [ ] `rivet state show` reflects the updated cursor
- [ ] `rivet metrics` shows both runs in history

## Mode-specific

- [ ] Full mode: complete snapshot on each run
- [ ] Incremental mode: only new/updated rows on subsequent runs
- [ ] Chunked mode: all chunks complete, `rivet state chunks` shows no pending tasks
- [ ] Time-window mode: only rows within the configured window

## Destinations

- [ ] Local: files written to correct path
- [ ] S3 (if used): files visible in bucket with correct prefix
- [ ] GCS (if used): files visible in bucket with correct prefix

## Plan/Apply (if using auditable execution)

- [ ] `rivet plan --config rivet.yaml -o plan.json` succeeds
- [ ] `rivet apply plan.json` runs and matches the plan artifact
- [ ] Re-running `rivet apply` with an unchanged plan succeeds; altered config is rejected

## Observability

- [ ] `rivet metrics --last 10` shows accurate run history
- [ ] `rivet state files` lists files produced by each run
- [ ] Schema change warnings appear when column structure changes

## Error recovery

- [ ] Interrupted export can be safely re-run without data loss
- [ ] `rivet state reset --export <name>` correctly resets cursor for a re-export

## Progression, reconcile, and repair (chunked exports with `chunk_checkpoint: true`)

- [ ] `rivet state progression` shows `COMMITTED` boundary per export after a successful run
- [ ] `rivet reconcile --export <name>` runs cleanly (all partitions `match`) and advances the `VERIFIED` boundary
- [ ] Injected mismatch: `rivet reconcile` surfaces it; `rivet repair --execute` writes corrective files without touching `COMMITTED`
- [ ] Post-repair `rivet reconcile` re-advances `VERIFIED`

---

## Next steps

- [Production checklist](production-checklist.md) — readiness gates before go-live
- [Reference: CLI](../reference/cli.md) — full command reference
- [Reference: Config](../reference/config.md) — all YAML fields
