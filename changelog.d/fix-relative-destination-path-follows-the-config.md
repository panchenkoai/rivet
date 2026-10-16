- **A relative `destination.path` followed the working directory, not the config.** A local
  export whose `path` is relative (`./output/orders`, what `rivet init` writes) was resolved
  against the directory rivet was started from, while its cursor and state live beside the
  config: the same config run from a cron entry and from a shell delivered ids 1..10 under one
  directory and 11..13 under the other, each run green (P-27). **Now** a relative local `path`
  is anchored to the config file's directory, resolved once at config load and used the same
  way by `run`, `plan`/`apply`, `validate`, `reconcile`, `repair`, `state`, `doctor` and
  `load`; a cursor stored under the old relative spelling continues under the anchored path.
  **What to do:** nothing for a run started from the config's directory, where the path
  resolves to the same place as before. A run started from another directory now writes
  beside the config: move the parts it wrote there, or make the path absolute to keep the old
  location.
