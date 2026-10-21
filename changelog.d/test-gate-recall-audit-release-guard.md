- **Harness / release process: a tag could be cut on a commit the full gate never graded, with seeded
  recall, `cargo audit` and the generated docs outside the gate.** Seeded-defect recall was opt-in, so
  24 seed patches had stopped applying to main unseen. **Now** `make release-oracle-full` runs the
  recall (a MISSED or STALE seed fails the gate), `cargo audit` and `docgen --check` are gate stages,
  the gate records its verdict with the full commit, its kind and whether the tree was clean at its
  start, and `dev/pytools/release.py` refuses to tag a commit without a clean RELEASE-READY
  `release-oracle-full` run of exactly that commit. The 24 patches are re-cut, and the offline
  self-test fails a PR whose seed no longer applies. **What to do:** release from a commit the full
  gate passed; pass `--gate-record FILE` when the gate ran on another machine.
- **Harness: the rig oracle read rivet's ledger from the first schema that held a table of its name.**
  A schema an older test left in the stand's PostgreSQL state (`rivet_wayback_nocreate`) came first
  for `file_log`, so every cell graded on that state read `file_log records 0 of this run's N declared
  part(s)` (the pool-ledger and state-ledger reds). **Now** the oracle resolves each table through the
  state connection's `search_path`, as rivet does, and the seeded recall's runs use its scratch state
  DB under every state knob, never the shared one. **What to do:** nothing.
