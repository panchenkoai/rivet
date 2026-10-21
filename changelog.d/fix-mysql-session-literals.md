- **Behind ProxySQL every chunk sent its MySQL session SETs again, some of them expressions.** Through
  0.32.1 each read of a chunked export sent `SET time_zone`, `SET SESSION sql_mode = REPLACE(@@sql_mode, ..)`
  and `max_execution_time` before it and `@@global.*` resets after it; ProxySQL cannot parse the
  expressions, logs `Unable to parse unknown SET query`, and locks the session to its hostgroup. The
  values were already right on 0.32.x (measured through a default-config ProxySQL in front of a +03:00
  server: every TIMESTAMP and DATETIME equal to the server's UTC read, single and parallel-4; an off-UTC
  session is refused since 0.32.1). **Now** rivet sets the session by literal SETs only, once per pooled
  connection (`sql_mode` is the server's without `NO_BACKSLASH_ESCAPES`, read once with `SHOW GLOBAL
  VARIABLES`), and reads no `@@` variable on a reading session, so ProxySQL keeps multiplexing it and
  replays the tracked variables on whichever backend it picks; a later export with another
  `statement_timeout_s` sends that one literal again. **What to do:** nothing.
