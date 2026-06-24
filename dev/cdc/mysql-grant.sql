-- CDC-only: the REPLICATION privileges the binlog dump (COM_BINLOG_DUMP) needs.
-- Granted on the dedicated `mysql-cdc` instance so the shared `mysql` service
-- stays minimal. The `rivet` user itself is created by MYSQL_USER/MYSQL_DATABASE.
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'rivet'@'%';
FLUSH PRIVILEGES;
