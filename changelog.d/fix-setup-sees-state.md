- **`rivet init`, `rivet check` and `rivet doctor` were green over a state no run could open.**
  With `RIVET_STATE_URL` pointing at a state that does not answer, `init` warned "primary keys
  not recorded" and wrote the config, exit 0; `check` printed `Looks good.`, exit 0; `doctor`
  did not look at the state at all, so setup passed and every run after it failed (0.32.0 and
  earlier). **Now** `doctor` opens the state the config uses without migrating it and prints
  `[OK]  State (postgres …)` / `(sqlite …)` or fails; `check` exits non-zero
  (`RIVET_CHECK_REPORTED_ERRORS`) when it logged an ERROR line, including a state that does not
  answer; `init -o` with `RIVET_STATE_URL` set refuses before writing the config when that state
  does not answer. **What to do:** nothing; fix what the new failure names.
