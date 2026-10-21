- **A first CDC load into a BigQuery base-and-buffer table reported nothing loaded.** The load landed
  the baseline in the base, but its `per_table[]` entry (`--json`, `--summary-output`) and its
  `CDC LOAD OK` line counted only the stream leg: `rows: 0`, `target: <table>__changes`, "no changes
  buffered" for a 6000-row baseline (0.32.0 and 0.32.1). **Now** `rows` is both legs' rows, `target`
  is the base when a baseline landed, `buffer` names the change log when the same load also
  appended changes (ADR-0039 D3, amended), and the line says what the baseline landed and where.
  **What to do:** nothing; a scheduler that read `target` of a CDC load as the change log now gets
  the base on a cycle that landed a baseline.
