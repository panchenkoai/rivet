- **The rollback steps left the previous release refusing every table the newer release
  created.** After putting the state copy back, a warehouse table first loaded after the upgrade
  has no row in the restored load ledger, so the previous release's `rivet load` refused it on
  every cycle ("no record of rivet loading it", exit 1), and `docs/reference/state.md` did not say
  so (0.32.0). **Now** the rollback steps list those tables (`rivet state loads` before and after
  the restore) and drop them, and the previous release's next load lands them again from the runs
  still staged. **What to do:** when rolling back, follow steps 2, 4 and 5 of "Rolling back an
  upgrade".
