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
