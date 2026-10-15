- **The run summary of a CDC export's first run counted the stream only, not its snapshot.** A
  `mode: cdc` export with `cdc.initial: snapshot` writes each table's baseline through a snapshot leg
  (`<export>__snapshot_<table>`) and then drains the stream; its `per_export[]` entry in
  `--summary-output`, `--json` and `run_aggregate` carried the drain's counts alone, so a run that wrote
  the whole table read `rows: 0, files: 0` (and so did a scheduler task built on it), and a failed leg
  showed up as an entry named after the leg instead of the CDC export (0.31.0 and earlier). **Now** the
  CDC export's entry adds what its snapshot legs wrote in the same run to `rows`, `files`, `bytes` and
  `bytes_read`, whether the exports run in one process or as `--parallel-export-processes` children,
  and a failed leg fails the entry under the CDC export's name. The legs still have
  their own `export_metrics` rows, so `rivet metrics` counts each write once, and `duration_ms` stays the
  drain's. **What to do:** nothing; summaries written by earlier runs are not rewritten.
