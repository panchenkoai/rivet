- **Harness / release process: a tag could be cut on a commit the full gate never graded, with seeded
  recall, `cargo audit` and the generated docs outside the gate.** Seeded-defect recall was opt-in, so
  25 of 157 seeds had stopped applying to main unseen. **Now** `make release-oracle-full` runs the
  recall (a MISSED or STALE seed fails the gate), `cargo audit` and `docgen --check` are gate stages,
  the gate records its verdict with the full commit, its kind and whether the tree was clean, and
  `dev/pytools/release.py` refuses to tag a commit without a clean RELEASE-READY `release-oracle-full`
  run of exactly that commit. The 25 seeds are re-cut, and an offline check fails a PR whose seed no
  longer applies. **What to do:** release from a commit the full gate passed; pass `--gate-record FILE`
  when the gate ran on another machine.
