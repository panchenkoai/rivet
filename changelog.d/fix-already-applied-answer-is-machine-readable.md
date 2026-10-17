- **`rivet apply` answers a replay of an applied plan in the summary document, and gains `--summary-output` / `--json`.**
  A replay of a full, chunked or keyset plan that already applied to exit 0 (0.31.0, #616) answered
  only in prose on stderr: no summary file, no JSON line, so a scheduler could not tell "already
  applied by run X" from "succeeded, result missing" without parsing text. **Now** `rivet apply`
  takes `--summary-output <PATH>` and `--json` like `rivet run` (in both plan-artifact and config
  modes), and the replay writes the same document a run writes, built from the run that applied the
  plan: its `run_id`, rows, files and bytes from its `export_metrics` row, `status: "already_applied"`
  and `answered_by_run: "<run_id>"`. Every `per_export[]` entry now carries `answered_by_run`
  (`null` unless the entry answers a replay); the stderr line is unchanged, the exit stays 0, and
  the replay still writes no run, manifest or metrics row. ADR-0039 lists the new status word and
  key. **What to do:** a scheduler reads `status` on the entry; a `status` table that treats an
  unknown word as failed needs `already_applied` added.
