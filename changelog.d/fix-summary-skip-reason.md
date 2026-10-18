- **A `skipped` entry of the run summary did not say why it was skipped.** `--summary-output`
  and `--json` of `rivet run` / `rivet apply` wrote `status: "skipped"` for a `skip_empty` run
  with no reason, while the console card and the metrics row had it (0.32.0 and earlier).
  **Now** every `per_export[]` entry carries `skip_reason`: the reason for a `skipped` entry ("no
  new rows since cursor 'updated_at'", "source returned 0 rows"), `null` for every other, on the
  sequential, thread and process runners. **What to do:** nothing; a scheduler may read it.
