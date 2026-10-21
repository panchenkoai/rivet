- **`rivet load` refused every table on a host without the source's variable.** Since the load spec
  became keyed by source (state v38, not released), the load read that key from the config's source,
  which does not resolve where `url_env:` is unset; a load task that runs without the source credential
  (the common shape, and every `rivet init` config) found no spec and stopped with
  `RIVET_LOAD_SPEC_MISSING`, exit 5. **Now** a load whose source does not resolve reads the one source
  that recorded a spec of the export and table on this state; when several did, it is refused with
  `RIVET_LOAD_SPEC_SOURCE_UNRESOLVED` (exit 1) instead of guessing. **What to do:** nothing; with
  several configs of one export name on one state, give `rivet load` the source's variable too.
