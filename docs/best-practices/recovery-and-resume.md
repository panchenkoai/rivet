# Recovery and Resume

Rivet stores export progress in a SQLite state file (`rivet_state.db`) located
next to the config file. This guide covers how to inspect, resume, and reset
export state correctly.

---

## State file location

The state file is always created next to the config file:

```
./rivet.yaml          ← config
./rivet_state.db      ← state (created automatically on first run)
```

To use a different location, point `--config` at the desired directory.

---

## Export modes and state

| Mode | What is stored | Resume behaviour |
|---|---|---|
| `full` | Completed file list (manifest) | No resume needed — re-run starts a fresh export |
| `incremental` | Last cursor value | Re-run starts from where it left off |
| `chunked` | Per-chunk completion status | `--resume` continues from the last completed chunk |

---

## `--resume` for chunked exports

`--resume` is only meaningful for `chunked` mode. It requires an in-progress
(not yet completed) export in the state file.

### Resume an interrupted export

```bash
# Start the export
rivet run --config rivet.yaml --export big_table

# If it was interrupted, resume it
rivet run --config rivet.yaml --export big_table --resume
```

### What happens if no checkpoint exists

If `--resume` is called without a prior in-progress run, Rivet exits non-zero
with a clear message:

```
error: --resume requires an in-progress chunked export in state;
       run without --resume to start a fresh export.
```

**Do not use `--resume` to start a fresh export.** It is only for continuing
interrupted runs.

### What happens after a completed export

After a chunked export completes normally, `--resume` also exits non-zero:

```
error: --resume found a completed export (not in-progress);
       use `rivet run` (without --resume) to start a new run.
```

This prevents accidentally treating a completed export as resumable.

### `--resume` on `full` or `incremental` mode

`--resume` is silently validated for full/incremental exports — a plan
validation warning is emitted:

```
[resume-no-checkpoint] export 'X': --resume has no effect on full/incremental
exports. Remove --resume to suppress this warning.
```

The export proceeds normally. The flag is ignored.

---

## Inspecting state

```bash
rivet state inspect --config rivet.yaml --export big_table
```

This shows the current state: cursor value for incremental exports, chunk
completion status for chunked exports, and the run history.

---

## Resetting state

### Reset cursor for incremental exports

```bash
rivet state reset --config rivet.yaml --export incremental_export
```

The next run will re-export all rows from the beginning.

### Reset chunk state for chunked exports

```bash
rivet state reset-chunks --config rivet.yaml --export big_table
```

After reset, the next `rivet run` (without `--resume`) starts fresh from
chunk 0.

**Important:** After `reset-chunks`, do not use `--resume` — there is no
checkpoint to resume from.

---

## Common operator mistakes

### Mistake 1: Using `--resume` after reset

```bash
rivet state reset-chunks --config rivet.yaml --export big_table
rivet run --config rivet.yaml --export big_table --resume  # WRONG
```

Fix: omit `--resume` after a reset.

```bash
rivet run --config rivet.yaml --export big_table  # correct
```

### Mistake 2: Using `--resume` to start a fresh chunked export

```bash
# First run ever — no state exists
rivet run --config rivet.yaml --export big_table --resume  # WRONG
```

Fix: do not use `--resume` on the first run.

### Mistake 3: Pointing to a different config file for resume

The state file is tied to its config directory. If you copy the config to a new
location, the state file is not copied with it — the resumed export starts fresh.

---

## Crash recovery

If the process is killed mid-export:

- **Incremental** — the cursor in state was last written at the end of the
  previous successful batch. Re-running picks up from there; the last partial
  batch is re-exported. Parquet output is idempotent by default
  (`idempotent_overwrite: true`), so the re-exported batch overwrites the
  partial file.

- **Chunked** — each chunk is committed to state only after it writes
  successfully. A crash mid-chunk means that chunk is retried on `--resume`.
  Completed chunks are not re-exported. This holds for both the **sequential**
  checkpoint loop (`parallel: 1`) and the **parallel** worker pool
  (`parallel: N` with `chunk_checkpoint: true`); when one parallel worker
  panics, `reset_stale_running_chunk_tasks` resets every `running` task back
  to `pending` on resume so no work is lost. Coverage: `live_chunked_recovery`
  C1–C4 (see [reliability-matrix.md § Failure-mode coverage](../reliability-matrix.md#failure-mode-coverage)).

- **Full** — full exports have no cursor. Re-running after a crash starts from
  the beginning. Partial output files from the crashed run are overwritten.

---

## See also

- [CLI reference — `rivet state`](../reference/cli.md)
- [Chunked mode](../modes/chunked.md)
- [Incremental mode](../modes/incremental.md)
