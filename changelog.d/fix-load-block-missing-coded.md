- **`rivet load` and `rivet compact` on a config without a `load:` block stopped with no code.**
  Through 0.32.1 they exited 1 with `config has no top-level \`load:\` block` and no `RIVET_*` code, so a
  scheduler could not tell the config mistake from any other failure. **Now** the stop is
  `RIVET_LOAD_BLOCK_MISSING` (usage, exit 1), raised by one function both commands call. **What to
  do:** nothing; add a top-level `load:` block to load or compact.
