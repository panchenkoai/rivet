- **A stored incremental cursor moved past rows the export never delivered was continued from.**
  With `last_cursor_value` edited above the last delivered value, the next run exported only the
  rows past it and exited 0, while the destination's own `manifest.json` ended below it (every
  version through 0.31.0). **Now** a run that continues from a stored cursor (incremental,
  `keyset_incremental`) compares it with the `cursor_high` of the success manifest at its
  destination and refuses a cursor past it with `RIVET_STATE_CURSOR_PAST_MANIFEST` (exit 5); a
  cursor equal or below, a destination with no success manifest of this export, or one that keeps
  no objects (a warehouse load) proceed as before. **What to do:** `rivet state reset -c <config>
  --export <name>` starts the cursor over with a full pass.
