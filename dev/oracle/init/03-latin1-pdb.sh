#!/bin/bash
# A second PDB, LATIN1PDB, whose database character set is WE8ISO8859P1 (national: AL16UTF16),
# for the non-Unicode character-set tests. Oracle Free ships one AL32UTF8 database; a CDB whose
# root is AL32UTF8 may hold PDBs of other character sets, and a fresh clone of the seed holds no
# user data, so INTERNAL_USE relabels it safely. Stand-only: never do this to a database with data.
# Only where RIVET_STAND_LATIN1_PDB=1: the compose `oracle-latin1` service, never the CDC
# stand's `oracle` — LogMiner refuses to mine a CDB holding a PDB of another character set
# (ORA-01305). The release gate's engine containers mount this directory too and skip it.
set -euo pipefail
if [ "${RIVET_STAND_LATIN1_PDB:-}" != "1" ]; then
  echo "RIVET_STAND_LATIN1_PDB is not 1: no LATIN1PDB"
  exit 0
fi
exists=$(sqlplus -s / as sysdba <<'SQL'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0
SELECT COUNT(*) FROM v$pdbs WHERE name = 'LATIN1PDB';
EXIT
SQL
)
if [ "$(echo "$exists" | tr -d '[:space:]')" != "0" ]; then
  echo "LATIN1PDB already exists"
  exit 0
fi
sqlplus -s / as sysdba <<'SQL'
WHENEVER SQLERROR EXIT FAILURE
CREATE PLUGGABLE DATABASE LATIN1PDB ADMIN USER pdbadmin IDENTIFIED BY rivet
  FILE_NAME_CONVERT = ('/opt/oracle/oradata/FREE/pdbseed/', '/opt/oracle/oradata/FREE/LATIN1PDB/');
ALTER PLUGGABLE DATABASE LATIN1PDB OPEN RESTRICTED;
ALTER SESSION SET CONTAINER = LATIN1PDB;
ALTER DATABASE CHARACTER SET INTERNAL_USE WE8ISO8859P1;
ALTER SESSION SET CONTAINER = CDB$ROOT;
ALTER PLUGGABLE DATABASE LATIN1PDB CLOSE IMMEDIATE;
ALTER PLUGGABLE DATABASE LATIN1PDB OPEN;
ALTER PLUGGABLE DATABASE LATIN1PDB SAVE STATE;
ALTER SESSION SET CONTAINER = LATIN1PDB;
CREATE USER rivet IDENTIFIED BY rivet QUOTA UNLIMITED ON SYSTEM;
GRANT CREATE SESSION, CREATE TABLE TO rivet;
GRANT SELECT_CATALOG_ROLE TO rivet;
EXIT
SQL
