- **A file error whose path read like a network word was retried as transient.** The retry
  classifier scanned the whole error text, paths included, so an unwritable `--summary-output`
  under a directory whose name contained `dns` (a temp dir such as `.tmprDnsA7`), `reset` or
  `broken pipe` exited 2 (retry) instead of 1 (0.32.1 and earlier). **Now** the kind of an
  `io::Error` in the chain decides first (a missing or unwritable path is permanent, a dropped
  connection transient), and the text is read only when no kind applies. **What to do:** nothing.
