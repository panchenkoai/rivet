#!/bin/bash
# LogMiner prerequisites for Oracle CDC (ADR-0037), run once at the image's first start:
# ARCHIVELOG, minimal supplemental logging, and the common capture user C##RIVETCDC.
# Tables opt in themselves (ALTER TABLE … ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS).
set -euo pipefail
mkdir -p /opt/oracle/oradata/archive
sqlplus -s / as sysdba <<'SQL'
WHENEVER SQLERROR EXIT FAILURE
ALTER SYSTEM SET log_archive_dest_1 = 'LOCATION=/opt/oracle/oradata/archive' SCOPE = BOTH;
SHUTDOWN IMMEDIATE
STARTUP MOUNT
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
ALTER PLUGGABLE DATABASE ALL OPEN;
ALTER PLUGGABLE DATABASE ALL SAVE STATE;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
CREATE USER c##rivetcdc IDENTIFIED BY rivet CONTAINER = ALL;
GRANT CREATE SESSION, SET CONTAINER, LOGMINING TO c##rivetcdc CONTAINER = ALL;
GRANT EXECUTE_CATALOG_ROLE TO c##rivetcdc CONTAINER = ALL;
GRANT SELECT ON v_$database TO c##rivetcdc CONTAINER = ALL;
GRANT SELECT ON v_$archived_log TO c##rivetcdc CONTAINER = ALL;
GRANT SELECT ON v_$log TO c##rivetcdc CONTAINER = ALL;
GRANT SELECT ON v_$logfile TO c##rivetcdc CONTAINER = ALL;
GRANT SELECT ON v_$logmnr_contents TO c##rivetcdc CONTAINER = ALL;
GRANT SELECT ON v_$logmnr_logs TO c##rivetcdc CONTAINER = ALL;
GRANT SELECT ON v_$transaction TO c##rivetcdc CONTAINER = ALL;
EXIT
SQL
