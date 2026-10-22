- **Every MySQL run opened an extra source connection to detect a proxy.** The fix that keeps
  ProxySQL multiplexed (0.32.2, literal session SETs) detected the proxy on a connection dialled for
  that alone, so a batch run opened 4 MySQL connections instead of 2 and a CDC run 3 instead of 2.
  **Now** the proxy is detected once per source URL per process, on the pool's own first connection,
  by statements that keep a ProxySQL session multiplexed (`PROXYSQL INTERNAL SESSION`,
  `SHOW GLOBAL VARIABLES LIKE …` in place of `SELECT @@…`): a batch run opens 2 and a CDC run 2
  again. **What to do:** nothing.
