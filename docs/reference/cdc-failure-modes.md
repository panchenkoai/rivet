# CDC failure modes & recovery

What rivet does when a CDC run hits an operational failure, what you do to
recover, and how to prevent it. Two guarantees frame every row:

- **Loud stop, never a silent gap.** When the source can no longer supply the
  changes since the checkpoint (a dropped/invalidated slot, purged binlog,
  aged-out change table), rivet **fails the run with a specific error and a
  recovery hint** — it never silently re-anchors at "now" and skips the gap.
  The cost is a re-snapshot; the benefit is you always know the numbers are
  right.
- **At-least-once, so a crash or outage is a delay, not a loss.** rivet reads,
  durably writes, then acks (`peek → flush → ack`). A crash between the write
  and the checkpoint re-reads the un-acked changes on the next run. Duplicates
  are the downstream MERGE's job; loss does not happen.

**`rivet doctor -c <config>` is the preventive layer.** For `mode: cdc` exports
it probes the engine and turns most of the rows below from an incident into a
warning *before* the run — run it in your scheduler's pre-flight step.

## The table

| Symptom | What rivet does | Operator recovery | Prevention |
|---|---|---|---|
| **PostgreSQL slot dropped or invalidated** (with a resume checkpoint present) | **Fails loud** — refuses to re-create the slot (a fresh slot would anchor at *current* and skip everything since the drop). Error names the re-snapshot path. | Re-snapshot the table (`initial: snapshot` → delete the `snapshot/_SUCCESS` marker, or delete the checkpoint), then resume. The WAL since the drop is gone — no tool can recover it. | `rivet doctor` flags a slot holding **> 1 GiB** retained WAL; set `max_slot_wal_keep_size` (PG 13+) so PG invalidates the slot instead of filling the disk. |
| **PostgreSQL slot filling the source disk** (consumer stopped / cadence too slow) | The slot pins WAL until consumed — this is PostgreSQL behavior; rivet does not fill it, but an abandoned slot will. | Resume draining (the slot advances on ack), or drop the slot + re-snapshot if it is beyond retention. | `rivet doctor` fails the slot check above 1 GiB retained WAL; monitor `pg_replication_slots.restart_lsn` vs current LSN; cap with `max_slot_wal_keep_size`. |
| **An abandoned *other* slot pinning WAL** (left by a previous tool) | Not rivet's slot, but it fills the same disk — the #1 CDC foot-gun. | `SELECT pg_drop_replication_slot('<name>')` for the dead slot. | `rivet doctor` reports **every** inactive slot pinning WAL, not just the export's own. |
| **MySQL binlog purged** (retention shorter than the drain cadence) | The next run fails with **`ERROR 1236`** (requested position no longer in the binlog). Loud, not silent. | Re-snapshot (`mode: full`) and restart CDC from a fresh checkpoint. | Size `binlog_expire_logs_seconds` **above** your CDC cadence; `rivet doctor` predicts it — flags a checkpoint already below retention before the run fails. |
| **SQL Server change table aged out** (checkpoint LSN below `fn_cdc_get_min_lsn`) | Loud stop — the saved LSN is below the capture instance's minimum retained LSN. | Re-snapshot and restart from a fresh checkpoint. | Size the CDC retention (`sys.sp_cdc_change_job @retention`) above your cadence; `rivet doctor` checks the checkpoint stays above `fn_cdc_get_min_lsn`. |
| **SQL Server Agent stopped** | Capture freezes — no new change-table rows are produced; a run drains what exists and then sees nothing new. | Start SQL Server Agent; capture resumes and the next run catches up. | `rivet doctor` reports the Agent service state; a stopped Agent is flagged. |
| **Corrupt or unreadable checkpoint file** | **Fails loud** on all shapes (garbage / truncated / wrong-engine format / empty) — a serde/shape error surfaces; never a silent re-anchor. | Restore the checkpoint from backup, or delete it and re-snapshot to accept a fresh anchor. | Keep the checkpoint on durable, non-ephemeral storage; back it up alongside the destination. |
| **Missing checkpoint parent directory** (first run) | The checkpoint save **creates parent directories** — the scaffolded `./cdc/<table>.ckpt` no longer fails a fresh quickstart (fixed in 0.16.6). | None — handled. | — |
| **DDL inside a capture window** | PostgreSQL & SQL Server map images **by column name** — a mid-window `DROP`/`ADD COLUMN` captures correctly. MySQL under `binlog_row_metadata=FULL` does too; under the default `MINIMAL` the binlog is nameless and rivet **fails loud** rather than misalign values. | Under MySQL MINIMAL: re-snapshot past the DDL, or reset the checkpoint. Same-arity **type** changes (undetectable without schema history) — re-snapshot through the migration. | Set `binlog_row_metadata=FULL` (MySQL 8.0.1+); run type-changing migrations + their backfills through a re-snapshot. |
| **A single transaction larger than memory** | The MySQL adapter buffers a whole transaction until its COMMIT (never splits it — the resume invariant); memory is **O(largest transaction)**, ~1.4 KB RSS per buffered row (100k rows ≈ 170 MB). A truly huge single transaction can exhaust memory. | Split bulk backfills into batched transactions, or run them through `mode: full` / `initial: snapshot` (the batch path streams). | Do bulk operations in batches; transaction spilling to disk is roadmap. |
| **Destination outage mid-drain** (S3/GCS/Azure unreachable) | **No loss** — `peek → flush → ack`: an un-flushed part is not acked, so the next run re-reads those changes. The run fails loud on the write error. | Restore the destination and re-run; the un-acked changes replay. | Alert on run failure; the at-least-once contract makes this a delay, not a loss. |
| **Process crash mid-drain** (`kill -9`, OOM, node reboot) | **No loss** — the checkpoint advances only after parts are durably committed and acked; a crash re-reads the un-acked tail. Verified: kill mid-5k-drain → resume captures all 5,000. | Re-run; resume continues from the last committed position. | — |
| **Destination disk full (ENOSPC)** | **Fails loud** naming the full disk; the checkpoint does **not** move. | Free space or point the export at a roomy destination; the full backlog is captured after healing (verified). | Monitor destination capacity; a full disk is a delay, not a loss. |
| **`REPLICATION` grant revoked mid-stream** | **Fails loud** pointing at the grants; the checkpoint does not move. | Restore the grant; the next run resumes with zero loss. | Alert on run failure; the checkpoint freeze makes this recoverable. |
| **A batch and a CDC export share one destination prefix** | **Fails loud** before the first part lands — refuses to overwrite the other shape's `manifest.json` (which would orphan its parts from `rivet validate`). | Give each export its own prefix; the CDC scaffold uses `exports/<table>/cdc/`. | Keep one shape per prefix (the scaffold does this by default). |
| **MySQL 8.4** (`SHOW MASTER STATUS` removed) | Handled transparently — rivet uses `SHOW BINARY LOG STATUS` (8.2+) with a legacy fallback. | None. | — |

## The shape of every recovery

Two recovery paths cover the table:

1. **Re-snapshot** — when the source no longer has the changes since the
   checkpoint (slot invalidated, binlog purged, change table aged out, a
   same-arity type change). Take a fresh consistent baseline
   (`initial: snapshot` or `mode: full`), then resume CDC from the new anchor.
   The gap is not recoverable from the log — the honest fix is a new baseline.
2. **Re-run** — when the changes are still in the source but a write or process
   failed (destination outage, crash, ENOSPC, revoked grant). The at-least-once
   contract replays the un-acked tail; no baseline needed.

The rule of thumb: **source-side loss ⇒ re-snapshot; sink-side or process
failure ⇒ re-run.** rivet always fails loud enough to tell you which.
