# Rivet Crash Matrix

Enumerates failure stages during an export and documents the expected behavior when the user re-runs the same command after a crash.

## Failure Stages

| Stage | When | State DB written? | Output file? | Cursor advanced? | Rerun behavior |
|-------|------|-------------------|--------------|------------------|----------------|
| **1. Config load** | YAML parse error, missing file | No | No | No | Safe rerun — nothing changed |
| **2. Source connect** | DB unreachable, auth failure | No | No | No | Safe rerun |
| **3. Preflight / check** | Permission denied, table not found | No | No | No | Safe rerun |
| **4. Query execution** | Query timeout, syntax error | No | No | No | Safe rerun |
| **5. Mid-batch fetch** | Network drop during FETCH | No partial | No | No | Safe rerun — batch is atomic |
| **6. Format / write** | Disk full, write error | Partial metric | Partial temp file | No | Safe rerun — temp file abandoned |
| **7. Upload to destination** | S3/GCS network error | Metric with error | Temp file exists | No | Safe rerun — cursor not advanced |
| **8. Cursor update** | SQLite write failure after upload | Metric recorded | File uploaded | **Race condition** | May re-export same data (at-least-once) |
| **9. Post-export** | Notification failure | All recorded | File uploaded | Yes | Complete — notification best-effort |

## Mode-Specific Behavior

### Full mode
- Rerun always re-exports all data (no cursor)
- Output files are timestamped, so reruns create new files (no overwrite)

### Incremental mode
- Cursor advances only after successful write+upload
- Crash at stage 5-7: rerun re-exports from last successful cursor position
- Crash at stage 8: rare race — may duplicate the last batch (at-least-once guarantee)
- **Safe pattern**: use idempotent downstream processing

### Chunked mode (without checkpoint)
- Each chunk is independent; crash re-exports all chunks
- Output files timestamped per chunk

### Chunked mode (with checkpoint)
- `chunk_checkpoint: true` persists completed chunk IDs in SQLite
- Crash at any stage: `--resume` flag skips already-completed chunks
- If a chunk was mid-upload, it will be re-exported entirely (at-least-once per chunk)

### Time-window mode
- Behaves like `full` for a bounded time range
- Rerun re-exports the same window (no cursor)

## Guarantees

1. **No data loss**: A crash never causes the source data to be modified
2. **At-least-once delivery**: In the worst case, a crash may cause duplicate rows in the output
3. **No partial files**: Temp files are written atomically; only complete files are uploaded
4. **Cursor safety**: Cursors advance only after successful write+upload
5. **State consistency**: SQLite WAL mode ensures no corruption on crash

## Recommendations for Production

1. Use `--reconcile` to verify exported row counts match source
2. Use `chunk_checkpoint: true` for large chunked exports to enable resume
3. Use `--validate` to verify output file integrity
4. Downstream consumers should handle duplicate rows (dedup by primary key + `_rivet_run_id`)

## Automated Crash-Point Coverage (test-only fault injection)

The four high-risk boundaries in the write cycle are wired to a lightweight
env-var-driven hook in `src/test_hook.rs`.  Setting `RIVET_TEST_PANIC_AT=<point>`
causes the pipeline to panic exactly at the named boundary.  No cargo feature
flag is required and the runtime cost when the env var is unset is a single
relaxed atomic load per call (≈ 1 ns).

| Env value | Boundary | ADR-0001 invariant window | Post-crash state expected |
|-----------|----------|---------------------------|---------------------------|
| `after_source_read` | After source stream drained, before writer finalise | Pre-I2 | No file, no manifest, no cursor |
| `after_file_write` | After `dest.write` Ok, before manifest row | I2→I3 crash window | File on disk, manifest empty, cursor absent |
| `after_manifest_update` | After `record_file`, before cursor advance | I2 written, I3 pending | File + manifest, cursor absent |
| `after_cursor_commit` | After `state.update`, before final metric | I3 written, I4 pending | File + manifest + cursor; metric may or may not exist |

The crash-point recovery matrix lives in `tests/live_crash_recovery.rs` —
each row in the table above has an automated test that (1) injects the crash,
(2) asserts the observable state, (3) re-runs without the injection and
asserts recovery produces the full export.  Run with:

```bash
docker compose up -d postgres
cargo test --test live_crash_recovery -- --ignored
```

See also: [docs/reference/testing.md](../docs/reference/testing.md) for the
full offline + live test matrix.

## Engine symmetry (OPT-6)

Rivet has two execution engines (ADR-0010); a crash scenario is only as covered
as the **weaker** engine:

- **In-process** — `src/pipeline/chunked/exec.rs` (+ `parallel_checkpoint.rs` /
  `sequential_checkpoint.rs`). Worker threads sharing one write cycle.
- **Subprocess fan-out** — `src/pipeline/parallel_children.rs`
  (`parallel_export_processes`). One child `rivet` per export; the parent
  spawns, waits, and aggregates child exit codes.

### Crash-trigger × engine coverage (verified 2026-06-13)

| Trigger | In-process | Subprocess |
|---|---|---|
| Panic at a write-cycle boundary (`RIVET_TEST_PANIC_AT`) | ✅ `live_crash_recovery.rs` (all 4 boundaries) + per-chunk `maybe_panic_at_chunk` | ✅ `after_source_read` (no partial — `parallel_processes_hard_crash_writes_no_partial_file`); `after_file_write` / `after_manifest_update` / `after_cursor_commit` recover with no row loss (`parallel_processes_recovers_from_child_crash_at_each_boundary`) |
| One child fails (sibling isolation) | n/a | ✅ `parallel_processes_one_child_failure_isolated_from_siblings` — healthy sibling completes, failed child leaves no partial |
| External **SIGKILL** mid-write (no Drop) | ✅ `sigkill_in_commit_window_leaves_no_committed_file` — staged temp, no committed file, clean recovery | ✅ same write cycle (each child is a single export through `local::write`) |
| External **SIGTERM / SIGINT** to the orchestrator | n/a (no child processes) | ✅ `child_reaper` reaps tracked children, then re-raises (`parallel_processes_sigterm_reaps_children_no_orphans`) |
| A child killed by signal (parent accounting) | n/a | ⚠️ ranking unit-tested only (`parallel_children.rs::exit_propagation_tests`); no end-to-end real-kill test |

### Panic ≠ signal (why the table above is not redundant)

The automated matrix injects **panics**, which *unwind* and run destructors — so
`ParquetWriter`'s `ArrowWriter::close()` (the footer) and the temp→rename can
still complete. A real **SIGKILL** runs **no** destructors, so under signals the
"no corrupt output" property does **not** rest on `Drop` — it rests on the
destination's commit protocol (`src/destination/mod.rs`):

- **Local** (`WriteCommitProtocol::Atomic`) — temp-then-rename. A SIGKILL leaves
  an **abandoned temp** (footerless or whole), never a corrupt *committed* file,
  so guarantee #3 holds **by design** — but this is unproven under a real signal.
- **Object stores** (`FinalizeOnClose`, `partial_write_risk`) — finalize is
  **non-atomic** (opendal `rename` = copy+delete). A crash mid-finalize is a
  known non-atomic window, flagged via destination capabilities, untested under
  signal.

### OPT-6 work

1. ✅ **Done — SIGTERM/SIGINT reaper** (`parallel_children::child_reaper`): a
   signal-safe PID registry + a `sigaction` handler that `kill`s the tracked
   children and re-raises the default disposition, so a targeted SIGTERM/SIGINT
   to the parent no longer orphans them. Proven RED→GREEN by
   `parallel_processes_sigterm_reaps_children_no_orphans` (orphans survive the
   8 s window without it; reaped in <1 s with it).
2. ✅ **Done — SIGKILL-mid-write proof** (`sigkill_in_commit_window_leaves_no_committed_file`):
   `RIVET_TEST_BLOCK_AT=before_commit_rename` parks the export with the staged
   `.tmp` written but the atomic rename not yet run; a real SIGKILL there leaves
   NO committed `.parquet` (only the abandoned temp) and a clean re-run recovers
   exactly one file — temp+rename, not `Drop`, carries guarantee #3 under a
   non-unwinding signal. (Object-store `FinalizeOnClose` remains unproven — needs
   a live cloud target.)
3. ✅ **Done — subprocess panic coverage across the full write cycle**
   (`parallel_processes_recovers_from_child_crash_at_each_boundary`): a child
   panic at `after_file_write` / `after_manifest_update` / `after_cursor_commit`
   through `parallel_export_processes` → the parent reports the crash and a clean
   subprocess rerun recovers the full source id set with no loss. Symmetric with
   the in-process matrix.

### Residual (out of the OPT-6 slices)

- **Object-store SIGKILL / `FinalizeOnClose`** — the non-atomic finalize window is
  unproven; needs a live cloud target.
- **A child killed by an external signal** (vs the parent) — parent accounting is
  unit-tested for ranking (`exit_propagation_tests`); no end-to-end real-kill of a
  single child. Low risk (a signal-killed child is a non-zero wait status the
  parent already aggregates).
