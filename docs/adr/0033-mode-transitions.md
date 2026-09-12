# ADR-0033: Mode Transitions — What a New Cursor Inherits

- **Status:** Accepted
- **Date:** 2026-09-11
- **Context:** The usual onboarding path is a full (often keyset) load, then `mode: incremental` on the same export. `export_state` holds one cursor per export NAME, so a field switch on 0.25.0 — keyset on `idvisit` → incremental on `visit_last_action_time` — compared the new column against the old key's value: on MySQL a silent zero-row match, on every run, with exit 0. This ADR fixes what a transition may inherit.

---

## Definitions

| Term | Meaning |
|------|---------|
| **Cursor identity** | What a persisted cursor value refers to: the incremental `cursor_column`, `coalesce(primary,fallback)` in coalesce mode, or the keyset `chunk_by_key`. Stored in `export_state.cursor_column` (state schema v26). |
| **Carrying mode** | A mode that persists a cursor: `incremental`, and keyset (`chunk_by_key`) in every variant — sequential, parallel, with or without `chunk_checkpoint`, `keyset_incremental`. |
| **Non-carrying mode** | `full`, `time_window`, range `chunked` (`chunk_column`) — no cursor is persisted. |

---

## Contract

| ID | Name | Statement | Enforced by |
|----|------|-----------|-------------|
| **MT1** | No cursor, full pass | After a non-carrying mode the first incremental run exports every row. The overlap with the full load is at-least-once; the warehouse dedups by key. | `StateStore::get_owned` on an empty row |
| **MT2** | Same identity continues | After a carrying mode, a run with the SAME identity continues past the stored value: keyset → incremental on the key, incremental → `keyset_incremental` on the column, adding `settle`. | `ExtractionStrategy::cursor_identity` equality |
| **MT3** | Different identity refuses | A run with a DIFFERENT identity fails before exporting anything, naming both identities and `rivet state reset`; after the reset it is a full pass (MT1). Changing `incremental_cursor_mode` changes the identity. | `StateStore::get_owned` |
| **MT4** | Writers record identity | Every cursor write records its identity: the incremental commit and the keyset page checkpoint / final high-water. | `StateStore::update_with_column` via `RunStore::commit` and `run_keyset` |
| **MT5** | Legacy keyset cursors | A pre-v26 cursor (no identity) is attributed to the latest successful keyset run when that run's `cursor_max` equals the stored value; MT2 / MT3 then apply. | `StateStore::legacy_cursor_owner` |
| **MT6** | Legacy incremental cursors (gap) | A pre-v26 cursor written by `incremental` has no recoverable identity — no column in state, metrics or the run journal — so a changed `cursor_column` is not detected. Reset when changing the cursor of an export whose last run predates v26. | — (documented gap) |
| **MT7** | Load follows the export | `rivet load` follows what each run holds. A run that re-read the whole table — `full` / `chunked` / `time_window`, or an incremental run with no cursor to resume from — OVERWRITES `<table>`, and only a table rivet's ledger says it loaded, with the partitioning and clustering the config declares; anything else (a foreign table, a hand-reshaped one, a view left by an append mode) fails naming the difference, and nothing is changed. A delta (`incremental` with a cursor, `cdc`) appends to `<table>__changes` behind a `<table>` view; the first delta over a `<table>` table renames it to `<table>__changes` (rows, partitioning and clustering kept; `__op` / `__pos` / `__seq` added as NULL) — no copy, nothing dropped. It refuses, touching nothing, when rivet did not load the table, its columns differ from the export's, or a `<table>__changes` already exists beside it. | `load::adopt_full_load_table` and `load::ensure_overwritable` in the shared drivers |

---

## Evidence

`docs/mode-transition-matrix.yaml`: every transition × engine is a live test in `tests/live/live_mode_transition.rs`. Each runs the previous mode over 10 rows through the Rig, inserts 3 more, restages the same Rig (same state DB) to the new mode, runs it and re-reads the destination. MT3 cells are RED on the 0.25.0 binary (`RIVET_BIN_OVERRIDE`).
