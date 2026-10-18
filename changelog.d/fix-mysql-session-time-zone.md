- **A MySQL source behind a proxy that dropped `SET time_zone` delivered every TIMESTAMP shifted
  by the server's offset, exit 0.** rivet reads MySQL in a UTC session; a ProxySQL that answered
  the SET without forwarding it left the read at the server's zone (a pilot's Europe/Moscow:
  `12:00:00Z` written for `09:00:00Z`) (0.32.0 and earlier). **Now** rivet checks on the
  connection that reads, before the first row, that the session is at UTC, and refuses
  `RIVET_SOURCE_SESSION_TIME_ZONE_NOT_APPLIED`, exit 5, naming the offset otherwise. **What to
  do:** set `default_time_zone = '+00:00'` on the server, keep rivet's session variables on one
  backend in the proxy, or connect directly; re-export what a proxied run delivered before.
