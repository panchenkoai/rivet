- **A relative `destination.path` followed the working directory, not the config.** A local
  export whose `path` is relative (`./output/orders`, what `rivet init` writes) was resolved
  against the directory rivet was started from, while its cursor and state live beside the
  config: the same config run from a cron entry and from a shell delivered ids 1..10 under one
  directory and 11..13 under the other, each run green (P-27). **Now** a relative local `path`
  is anchored to the config file's directory, resolved once at config load and used the same
  way by `run`, `plan`/`apply`, `validate`, `reconcile`, `repair`, `state`, `doctor` and
  `load`; a cursor stored under the old relative spelling continues under the anchored path.
  **Transition guard:** a command started from another directory, whose working-directory
  place still holds the export's files (its `manifest.json`) while the directory beside the
  config holds none, is refused with `RIVET_DEST_RELATIVE_PATH_MOVED` (exit 5) naming both
  directories, before anything is read or written; the check is one decision in the config
  loader, so `run`, `load`, `reconcile`, `repair`, `validate` and every other command that
  resolves the path meet it, and it costs local file reads only. **What to do:** nothing for
  a run started from the config's directory, where the path resolves to the same place as
  before. After the refusal, move the files to the named directory beside the config (the
  stored cursor continues there), or make `destination.path` absolute to keep the old location.
