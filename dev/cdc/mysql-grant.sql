-- CDC-only: the REPLICATION privileges the binlog dump (COM_BINLOG_DUMP) needs.
-- Granted on the dedicated `mysql-cdc` instance so the shared `mysql` service
-- stays minimal. The `rivet` user itself is created by MYSQL_USER/MYSQL_DATABASE.
-- SYSTEM_VARIABLES_ADMIN (8.0+ only, hence the versioned comment the 5.7 scout
-- lane skips) lets a live test flip `binlog_row_metadata` to MINIMAL.
GRANT REPLICATION SLAVE, REPLICATION CLIENT /*!80000 , SYSTEM_VARIABLES_ADMIN */ ON *.* TO 'rivet'@'%';
FLUSH PRIVILEGES;
