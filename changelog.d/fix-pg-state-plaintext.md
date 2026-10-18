- **A PostgreSQL state on a remote host without TLS could not be used at all.** Every command
  with `RIVET_STATE_URL` naming a non-loopback host was refused `RIVET_STATE_BACKEND_FAILED`,
  exit 1, with a text that told the operator to add `source.tls`, a key the state does not
  have, and even `?sslmode=disable` was refused (0.32.0 and earlier). **Now** `?sslmode=disable`
  is honoured as the operator's opt-out with one warning line, and a remote URL that names no
  `sslmode` is refused `RIVET_STATE_URL_TLS_REQUIRED`, exit 5, naming the two state remedies.
  **What to do:** add `?sslmode=require` to `RIVET_STATE_URL` with TLS on the state server, or
  `?sslmode=disable` on a private network.
