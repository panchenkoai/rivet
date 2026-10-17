- **A plan applied after UTC midnight wrote under the planning day.** `rivet plan` resolved
  `{date}` in the destination into the sealed plan, so a plan made before midnight and applied
  after it wrote under the planning day while the data and the part files were of the apply
  (P-26, since `rivet plan` exists). **Now** the sealed plan keeps `{date}` as written and
  `rivet apply` resolves it to the UTC day it runs on, through the same resolver `rivet run`
  uses: a plan made before midnight UTC and applied after now writes under the apply date.
  `rivet plan --date YYYY-MM-DD` freezes that day into the plan instead. A plan sealed by an
  earlier release holds an already-resolved path and applies as it did. **What to do:**
  nothing; pass `--date` to `rivet plan` when the planning day is the one wanted.
