#!/usr/bin/env bash
# Create the rivet state schema (version + tables) in every state database up
# front, so the parallel wave tasks never race to create it on a fresh DB.
# `rivet state show` connects to the state DB and runs the migration; the source
# URL is only needed so the config parses (state show doesn't query the source).
set -u
export RIVET_PG_URL="postgresql://rivet:rivet@host.docker.internal:5432/rivet"
export RIVET_MY_URL="mysql://rivet:rivet@host.docker.internal:3306/rivet"
export RIVET_MS_URL="sqlserver://sa:Rivet_Passw0rd!@host.docker.internal:1433/rivet"
warm() {  # <state-db> <config>
  RIVET_STATE_URL="postgresql://rivet:rivet@rivet-state-db:5432/$1?sslmode=require" \
    rivet state show --config "/opt/airflow/dags/$2" >/dev/null 2>&1 || true
  echo "  state schema ready: $1"
}
warm rivet_state_postgres      postgres.yaml
warm rivet_state_mysql         mysql.yaml
warm rivet_state_mssql         mssql.yaml
warm rivet_state_postgres_s3   postgres.s3.yaml
warm rivet_state_mysql_s3      mysql.s3.yaml
warm rivet_state_mssql_s3      mssql.s3.yaml
warm rivet_state_postgres_gcs  postgres.gcs.yaml
echo "all state schemas initialised"
