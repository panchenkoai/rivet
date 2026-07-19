#!/usr/bin/env bash
# Runs once during the primary's initdb bootstrap (before the real server
# starts). POSTGRES_HOST_AUTH_METHOD=trust only appends `host all all all trust`
# — it does NOT cover the `replication` pseudo-database, so the standby's
# pg_basebackup is rejected ("no pg_hba.conf entry for replication connection").
# Add the replication trust line here; the post-init server restart re-reads it.
set -e
echo "host replication all all trust" >> "$PGDATA/pg_hba.conf"
