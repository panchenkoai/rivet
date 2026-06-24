-- rivet reads the REPLICA's own binlog (re-logged via log_replica_updates).
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'rivet'@'%';
FLUSH PRIVILEGES;
