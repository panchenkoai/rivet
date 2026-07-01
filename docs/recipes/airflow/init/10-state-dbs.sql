-- Auto-created on rivet-state-db's first boot (docker-entrypoint-initdb.d).
-- One state database per source × destination, so same-named tables never
-- collide on cursor / shape / file-log state.
CREATE DATABASE rivet_state_postgres;
CREATE DATABASE rivet_state_mysql;
CREATE DATABASE rivet_state_mssql;
CREATE DATABASE rivet_state_postgres_s3;
CREATE DATABASE rivet_state_mysql_s3;
CREATE DATABASE rivet_state_mssql_s3;
CREATE DATABASE rivet_state_postgres_gcs;
