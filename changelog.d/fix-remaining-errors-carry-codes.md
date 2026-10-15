- **Every remaining error rivet raises outside its source readers carried no `RIVET_*` code.** The
  CLI, destination, plan, init, state, preflight, format, manifest, MCP, type-override and TLS
  stops (152 sites) printed prose only, so a scheduler could match on wording alone, since the
  codes were introduced. **Now** every stop carries a registered code (`[CODE]` prefix on the text
  line, `code` in `--json-errors`), with 28 new codes in `docs/reference/errors.md`; message text
  is unchanged. A stop's exit follows its code's class, which moves these stops from exit 1 to the
  class exit: `rivet init -o` over an existing config (`RIVET_INIT_CONFIG_EXISTS`, 5); an export
  whose destination prefix holds a manifest of the other mode (`RIVET_DEST_PREFIX_MODE_MISMATCH`,
  5); a Full load over a `--pool --split` prefix spanning two split generations
  (`RIVET_LOAD_SPLIT_GENERATIONS_MIXED`, 5); `rivet load` for an export with no recorded column
  types (`RIVET_LOAD_SPEC_MISSING`, 5); a state migration that did not reach the binary's schema
  version, an empty state path, a run aggregate that does not serialise
  (`RIVET_INTERNAL_STATE_INVARIANT`, 6); a destination backend method the backend does not
  implement, a destination key with no file name, a CSV cell type the writer never rejected
  (`RIVET_INTERNAL_DEST_INVARIANT`, 6); a `row_hash` column gone from the batch or read as the
  wrong array kind, a worker panic, a `COUNT(*)` that is not a number, a committed cursor with no
  column to record it under (`RIVET_INTERNAL_RUN_INVARIANT`, 6); an Oracle current schema the
  server did not return, a MySQL MCP query with no result set (`RIVET_INTERNAL_SOURCE_INVARIANT`,
  6). **What to do:** a script that keyed on exit 1 for one of these stops treats 5 as "a human
  decides, do not retry unchanged" and 6 as "a bug, report it"; the text is unchanged, so a script
  matching text keeps working. Other stops keep their exit: usage codes exit 1 and environment
  codes exit 2 when transient, else 1, as before.
