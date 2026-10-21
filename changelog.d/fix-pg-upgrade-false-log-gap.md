- **Fixed: the upgrade from 0.32.1 no longer stops a PostgreSQL CDC stream with a false log gap.**
  0.32.1 released a trailing span of uncaptured transactions by advancing its slot without saving the
  checkpoint, so its slot could sit past its checkpoint. The 0.32.2 guard against another reader of
  the slot (#649) read that as changes consumed by someone else and refused the first upgraded run
  `RIVET_SOURCE_CDC_LOG_GAP`, exit 5, with a re-baseline remedy, although nothing was lost. **Now** a
  checkpoint without the `confirmed_first` mark that every 0.32.2 checkpoint carries (one an older
  release wrote) is moved to the slot's position once, with a warning naming both positions, and
  rewritten in the new form; from then on a slot past the checkpoint is refused as before. 0.32.1
  still reads a checkpoint 0.32.2 wrote. **What to do:** nothing.
