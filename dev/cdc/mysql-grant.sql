-- CDC-only: the REPLICATION privileges the binlog dump (COM_BINLOG_DUMP) needs.
-- Granted on the dedicated `mysql-cdc` instance so the shared `mysql` service
-- stays minimal. The `rivet` user itself is created by MYSQL_USER/MYSQL_DATABASE.
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'rivet'@'%';
-- SYSTEM_VARIABLES_ADMIN so a live test can flip `binlog_row_metadata` to
-- MINIMAL — MySQL's OWN default, and the only setting under which the sink's
-- positional image mapping and its arity guard are reachable. The stack pins
-- FULL (see docker-compose), so without this grant the engine's default
-- configuration is the one configuration nothing tests.
GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'rivet'@'%';
FLUSH PRIVILEGES;
