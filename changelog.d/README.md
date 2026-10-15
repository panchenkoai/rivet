# Unreleased changelog fragments

Every pull request that changes what an operator sees adds **one file here** instead of editing
`CHANGELOG.md`, so open pull requests never conflict on the top of the `## Unreleased` section.

- Name: anything ending in `.md` except this README, e.g. the branch name with `/` replaced by `-`.
- Content: one or more entries in the CHANGELOG format, each starting with `- **` at column 0, its
  continuation lines indented by two spaces:

  ```
  - **What was wrong, in one sentence.** What happened and since which version. **Now** what
    rivet does instead. **What to do:** what the operator does, or nothing.
  ```

- `python3 dev/pytools/changelog.py check` validates the fragments; `pr_ready.py` accepts a new
  fragment as the CHANGELOG entry of a `src/` change.
- Before tagging a release, `python3 dev/pytools/changelog.py assemble` moves every fragment into
  `## Unreleased` of `CHANGELOG.md` (newest first, by the commit that added each) and deletes the
  files; the release workflow refuses a tag while fragments remain.
