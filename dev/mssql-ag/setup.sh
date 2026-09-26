#!/usr/bin/env bash
# Build the read-scale availability group mssql-ag-primary -> mssql-ag-secondary (idempotent).
set -euo pipefail
P=rivet-mssql-ag-primary-1
S=rivet-mssql-ag-secondary-1
PW='Rivet_Passw0rd!'
sql() { docker exec "$1" /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$PW" -C -b -Q "$2"; }

for c in "$P" "$S"; do
  n=${c#rivet-}; n=${n%-1}
  sql "$c" "IF NOT EXISTS (SELECT 1 FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##') CREATE MASTER KEY ENCRYPTION BY PASSWORD = '$PW';
            IF NOT EXISTS (SELECT 1 FROM sys.certificates WHERE name = '${n//-/_}_cert') CREATE CERTIFICATE ${n//-/_}_cert WITH SUBJECT = '$n';"
  docker exec "$c" rm -f "/tmp/${n}.cer" "/tmp/${n}.pvk"
  sql "$c" "BACKUP CERTIFICATE ${n//-/_}_cert TO FILE = '/tmp/${n}.cer' WITH PRIVATE KEY (FILE = '/tmp/${n}.pvk', ENCRYPTION BY PASSWORD = '$PW');"
done

tmp=$(mktemp -d)
for pair in "$P:mssql-ag-primary" "$S:mssql-ag-secondary"; do
  c=${pair%%:*}; n=${pair##*:}
  docker cp "$c:/tmp/$n.cer" "$tmp/$n.cer"; docker cp "$c:/tmp/$n.pvk" "$tmp/$n.pvk"
done
for pair in "$P:mssql-ag-secondary" "$S:mssql-ag-primary"; do
  c=${pair%%:*}; other=${pair##*:}; u=${other//-/_}
  docker cp "$tmp/$other.cer" "$c:/tmp/$other.cer"; docker cp "$tmp/$other.pvk" "$c:/tmp/$other.pvk"
  docker exec -u 0 "$c" chown mssql /tmp/$other.cer /tmp/$other.pvk
  sql "$c" "IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = '${u}_login') CREATE LOGIN ${u}_login WITH PASSWORD = '$PW';
            IF NOT EXISTS (SELECT 1 FROM master.sys.database_principals WHERE name = '${u}_user') CREATE USER ${u}_user FOR LOGIN ${u}_login;
            IF NOT EXISTS (SELECT 1 FROM sys.certificates WHERE name = '${u}_cert') CREATE CERTIFICATE ${u}_cert AUTHORIZATION ${u}_user FROM FILE = '/tmp/$other.cer' WITH PRIVATE KEY (FILE = '/tmp/$other.pvk', DECRYPTION BY PASSWORD = '$PW');"
done
rm -rf "$tmp"

for pair in "$P:mssql_ag_primary:mssql_ag_secondary" "$S:mssql_ag_secondary:mssql_ag_primary"; do
  IFS=: read -r c own other <<<"$pair"
  sql "$c" "IF NOT EXISTS (SELECT 1 FROM sys.endpoints WHERE name = 'hadr_endpoint')
              CREATE ENDPOINT hadr_endpoint STATE = STARTED AS TCP (LISTENER_PORT = 5022)
              FOR DATABASE_MIRRORING (ROLE = ALL, AUTHENTICATION = CERTIFICATE ${own}_cert, ENCRYPTION = REQUIRED ALGORITHM AES);
            GRANT CONNECT ON ENDPOINT::hadr_endpoint TO ${other}_login;"
done

sql "$P" "IF NOT EXISTS (SELECT 1 FROM sys.availability_groups WHERE name = 'ag1')
            CREATE AVAILABILITY GROUP ag1 WITH (CLUSTER_TYPE = NONE) FOR REPLICA ON
              N'mssql-ag-primary' WITH (ENDPOINT_URL = N'tcp://mssql-ag-primary:5022', AVAILABILITY_MODE = SYNCHRONOUS_COMMIT,
                FAILOVER_MODE = MANUAL, SEEDING_MODE = AUTOMATIC, SECONDARY_ROLE (ALLOW_CONNECTIONS = ALL)),
              N'mssql-ag-secondary' WITH (ENDPOINT_URL = N'tcp://mssql-ag-secondary:5022', AVAILABILITY_MODE = SYNCHRONOUS_COMMIT,
                FAILOVER_MODE = MANUAL, SEEDING_MODE = AUTOMATIC, SECONDARY_ROLE (ALLOW_CONNECTIONS = ALL));"
sql "$S" "IF NOT EXISTS (SELECT 1 FROM sys.availability_groups WHERE name = 'ag1')
            BEGIN ALTER AVAILABILITY GROUP ag1 JOIN WITH (CLUSTER_TYPE = NONE); ALTER AVAILABILITY GROUP ag1 GRANT CREATE ANY DATABASE; END"

sql "$P" "IF DB_ID('rivet') IS NULL BEGIN CREATE DATABASE rivet; END"
sql "$P" "ALTER DATABASE rivet SET RECOVERY FULL; BACKUP DATABASE rivet TO DISK = N'/var/opt/mssql/data/rivet.bak' WITH INIT;
          IF NOT EXISTS (SELECT 1 FROM sys.dm_hadr_database_replica_states s JOIN sys.databases d ON d.database_id = s.database_id WHERE d.name = 'rivet')
            ALTER AVAILABILITY GROUP ag1 ADD DATABASE rivet;"
sql "$P" "USE rivet; IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = 'rivet') = 0 EXEC sys.sp_cdc_enable_db;"
for i in $(seq 1 60); do
  if sql "$S" "SET NOCOUNT ON; IF DB_ID('rivet') IS NULL OR DATABASEPROPERTYEX('rivet','Updateability') IS NULL RAISERROR('wait',16,1);" >/dev/null 2>&1; then
    echo "availability group ag1 ready: rivet readable on mssql-ag-secondary"; exit 0
  fi
  sleep 2
done
echo "rivet never reached the secondary" >&2; exit 1
