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

**`rivet doctor -c rivet.yaml` is the preventive layer.** For `mode: cdc` exports
it probes the engine and turns most of the rows below from an incident into a
warning *before* the run — run it in your scheduler's pre-flight step.

## The table

| Symptom | What rivet does | Operator recovery | Prevention |
|---|---|---|---|
| **PostgreSQL slot dropped or invalidated** (with a resume checkpoint present) | **Fails loud** (`RIVET_SOURCE_CDC_LOG_GAP`, exit 5) — refuses to re-create the slot (a fresh slot would anchor at *current* and skip everything since the drop). Error names the re-baseline. Without a checkpoint or a completed baseline, rivet cannot tell a dropped slot from a first run: it creates the slot and **warns** with the same re-baseline. | [Re-baseline](#the-shape-of-every-recovery). The WAL since the drop is gone — no tool can recover it. | `rivet doctor` flags a slot holding **> 1 GiB** retained WAL; set `max_slot_wal_keep_size` (PG 13+) so PG invalidates the slot instead of filling the disk. |
| **PostgreSQL slot filling the source disk** (consumer stopped / cadence too slow) | The slot pins WAL until consumed — this is PostgreSQL behavior; rivet does not fill it, but an abandoned slot will. | Resume draining (the slot advances on ack), or drop the slot and [re-baseline](#the-shape-of-every-recovery) if it is beyond retention. | `rivet doctor` fails the slot check above 1 GiB retained WAL; monitor `pg_replication_slots.restart_lsn` vs current LSN; cap with `max_slot_wal_keep_size`. |
| **An abandoned *other* slot pinning WAL** (left by a previous tool) | Not rivet's slot, but it fills the same disk — the #1 CDC foot-gun. | `SELECT pg_drop_replication_slot('slot_name')` for the dead slot. | `rivet doctor` reports **every** inactive slot pinning WAL, not just the export's own. |
| **MySQL binlog purged** (retention shorter than the drain cadence) | The next run is refused as `RIVET_SOURCE_CDC_LOG_GAP` (exit 5), quoting the server's **`ERROR 1236`** (requested position no longer in the binlog) and the re-baseline. Loud, not silent. | [Re-baseline](#the-shape-of-every-recovery). | Size `binlog_expire_logs_seconds` **above** your CDC cadence; `rivet doctor` predicts it — flags a checkpoint already below retention before the run fails. |
| **SQL Server change table aged out, or the capture instance re-created** (checkpoint LSN below `fn_cdc_get_min_lsn`) | Loud stop (exit 5, `RIVET_SOURCE_CDC_LOG_GAP`), from the first run after the baseline — the saved LSN is below the capture instance's minimum retained LSN. | [Re-baseline](#the-shape-of-every-recovery). | Size the CDC retention (`sys.sp_cdc_change_job @retention`) above your cadence; `rivet doctor` checks the checkpoint stays above `fn_cdc_get_min_lsn`. |
| **SQL Server Agent stopped** | Capture freezes — no new change-table rows are produced; a run drains what exists and then sees nothing new. | Start SQL Server Agent; capture resumes and the next run catches up. | `rivet doctor` reports the Agent service state; a stopped Agent is flagged. |
| **Corrupt or unreadable checkpoint file** | **Fails loud** (`RIVET_SOURCE_CDC_CHECKPOINT_INVALID`, exit 5) on garbage / truncated / empty checkpoints (invalid JSON; never a silent re-anchor). Deleting the file alone does not clear it once the stream has a baseline: the missing checkpoint is refused under the same code. A wrong-engine checkpoint that is still *valid JSON* passes the shared loader (the position is stored as an opaque JSON blob) and only fails when the engine interprets it — don't rely on that as a guard. | Restore the checkpoint from backup, or [re-baseline](#the-shape-of-every-recovery). | Keep the checkpoint on durable, non-ephemeral storage; back it up alongside the destination. |
| **MongoDB oplog rolled past the resume token** (oplog window shorter than the drain cadence) | The next run is refused as `RIVET_SOURCE_CDC_LOG_GAP` (exit 5), quoting the server's `ChangeStreamHistoryLost` (error 286) and the re-baseline. | [Re-baseline](#the-shape-of-every-recovery). | Size the oplog (`replSetResizeOplog`, `minRetentionHours`) above your CDC cadence. |
| **A captured table TRUNCATEd or DROPped** (MongoDB: a captured collection or its database dropped) | Refused as `RIVET_SOURCE_CDC_TRUNCATED` (exit 5): the removed rows have no change events, so skipping it would leave them live in the destination. Every re-run stops at the same event. | [Re-baseline](#the-shape-of-every-recovery) (PostgreSQL: first advance the slot past the truncate, as the error says). | Empty a captured table with `DELETE`, which is logged row by row. |
| **Missing checkpoint parent directory** (first run) | The checkpoint save **creates parent directories** — the scaffolded `./cdc/TABLE.ckpt` no longer fails a fresh quickstart (fixed in 0.16.5). | None — handled. | — |
| **DDL inside a capture window** | PostgreSQL & SQL Server map images **by column name** — a `DROP COLUMN` or rename between runs captures correctly, and an equal-arity `DROP`+`ADD` leaves the new column NULL for older images (unless the dropped column sat at the new one's position, which is read as a rename). A column **added while a run is open** is not in that run's schema: its values for that run are dropped and acked ([re-baseline](#the-shape-of-every-recovery) to recover them). MySQL behaves the same under `binlog_row_metadata=FULL`, which rivet requires: a server at `MINIMAL` is refused at open (`RIVET_SOURCE_CDC_PREREQUISITE`), and an event written under `MINIMAL` is refused when read (`RIVET_SOURCE_CDC_UNDECODABLE`, naming the table and binlog position), never mapped by position. | For a `MINIMAL` backlog: switch to FULL, then [re-baseline](#the-shape-of-every-recovery). Same-arity **type** changes (undetectable without schema history): [re-baseline](#the-shape-of-every-recovery) through the migration. | Set `binlog_row_metadata=FULL` (MySQL 8.0.1+); run type-changing migrations + their backfills through a [re-baseline](#the-shape-of-every-recovery). |
| **A single transaction larger than memory** | The MySQL adapter buffers a whole transaction until its COMMIT (never splits it — the resume invariant); memory is **O(largest transaction)**, ~1.4 KB RSS per buffered row (100k rows ≈ 170 MB). Hard per-transaction caps bail loudly before OOM: 5M buffered rows and 2 GiB estimated bytes by default (`RIVET_CDC_MAX_TX_ROWS` / `RIVET_CDC_MAX_TX_BYTES` override them — raise only when a transaction this large is genuinely expected). | Split bulk backfills into batched transactions, or run them through `mode: full` / `initial: snapshot` (the batch path streams). | Do bulk operations in batches. Opt-in spilling exists: set `RIVET_CDC_SPILL_DIR` (a directory — relative forms resolve against the config's directory — or `1` to place it beside the checkpoint, falling back to `<config dir>/.rivet/spill` when the export has none) and a transaction past the cap spills its tail to disk instead of failing — the transaction is still delivered whole. (PostgreSQL, MySQL and SQL Server; Oracle ignores the variable and always refuses at the cap.) Note the measured limit: this moves the *adapter's* copy only (~11% of peak RSS on a 100k-row transaction); the sink still buffers a whole transaction, so the caps stay the honest guard and spilling off stays the default. |
| **Destination outage mid-drain** (S3/GCS/Azure unreachable) | **No loss** — `peek → flush → ack`: an un-flushed part is not acked, so the next run re-reads those changes. The run fails loud on the write error. | Restore the destination and re-run; the un-acked changes replay. | Alert on run failure; the at-least-once contract makes this a delay, not a loss. |
| **Process crash mid-drain** (`kill -9`, OOM, node reboot) | **No loss** — the checkpoint advances only after parts are durably committed and acked; a crash re-reads the un-acked tail. Verified: kill mid-5k-drain → resume captures all 5,000. | Re-run; resume continues from the last committed position. | — |
| **Destination disk full (ENOSPC)** | **Fails loud** naming the full disk; the checkpoint does **not** move. | Free space or point the export at a roomy destination; the full backlog is captured after healing (verified). | Monitor destination capacity; a full disk is a delay, not a loss. |
| **`REPLICATION` grant revoked mid-stream** | **Fails loud** pointing at the grants; the checkpoint does not move. | Restore the grant; the next run resumes with zero loss. | Alert on run failure; the checkpoint freeze makes this recoverable. |
| **A batch and a CDC export share one destination prefix** | **Fails loud** before the first part lands — refuses to overwrite the other shape's `manifest.json` (which would orphan its parts from `rivet validate`). | Give each export its own prefix; the CDC scaffold uses `exports/TABLE/cdc/`. | Keep one shape per prefix (the scaffold does this by default). |
| **MySQL 8.4** (`SHOW MASTER STATUS` removed) | Handled transparently — rivet uses `SHOW BINARY LOG STATUS` (8.2+) with a legacy fallback. | None. | — |

## The shape of every recovery

Two recovery paths cover the table:

1. **Re-baseline** — when the source no longer has the changes since the
   checkpoint (slot invalidated, binlog purged, change table aged out, a
   same-arity type change, a TRUNCATE). The gap is not recoverable from the
   log; the fix is a new baseline taken by the stream itself, in one run:
   - delete the checkpoint file if there is one (PostgreSQL after a TRUNCATE:
     first move the slot past it with `pg_replication_slot_advance`, as the
     error says);
   - move every file out of the export's destination (for a `tables:` export,
     every table's directory under it). The parts there still hold rows the
     source may no longer have — a row the TRUNCATE removed, or one deleted
     during the gap, has no event that retracts it — so a reader of the prefix
     would keep serving it. Each table's `snapshot/_SUCCESS` marker goes with
     them. A reader that already copied earlier parts elsewhere drops them too;
   - delete the export's `cdc_snapshot` rows from the state DB, one per table:
     `DELETE FROM cdc_snapshot WHERE export_name = '<export>' AND (prefix = ''
     OR prefix LIKE '%<destination path or prefix>%')`. The `prefix` clause
     matters on a state DB that several configs share: `export_name` alone
     also matches another config's export of the same name. The state row and
     the marker are OR-ed: either one left in place skips that table's
     baseline;
   - give the export `cdc.initial: snapshot` if it has none;
   - if a warehouse load consumes this stream, truncate its `<table>__changes`
     table before the next load. A key deleted during the gap has no row in the
     new baseline, so its older change rows would stay live; on PostgreSQL, and
     under `layout: base_buffer`, the baseline rows also carry no `__pos`, so
     every older change outranks them. The load refuses the baseline until the
     log is empty;
   - re-run. The run anchors FIRST and re-reads every table after, so nothing
     falls between the two.

   A separate `mode: full` export is not a re-baseline. With the `cdc:` block
   kept it is refused (`a cdc: block is only valid with mode: cdc`); as a batch
   export into the stream's destination it is refused (`destination already
   holds a 'cdc' manifest`); and a snapshot taken before the new anchor leaves
   the changes in between in neither.
2. **Re-run** — when the changes are still in the source but a write or process
   failed (destination outage, crash, ENOSPC, revoked grant). The at-least-once
   contract replays the un-acked tail; no baseline needed.

The rule of thumb: **source-side loss ⇒ re-baseline; sink-side or process
failure ⇒ re-run.** rivet always fails loud enough to tell you which.
