- **A float `chunk_column` under `query:` lost its fractional keys with exit 0.** With `table:` the
  planner refused a non-integer range key; with `query:` it could not read a catalog, only warned,
  and integer `BETWEEN` windows skipped every key between two whole numbers (a `double precision` key
  delivered 40 of 43 rows) (0.31.0 and earlier). **Now** the run reads the query's result type before
  it plans the windows and refuses a float or scaled-decimal key with
  `RIVET_CONFIG_CHUNK_COLUMN_NOT_INTEGER` (exit 1), the same code the `table:` refusal now carries.
  **What to do:** use `chunk_by_key:` (keyset) or an integer column for such a key.
