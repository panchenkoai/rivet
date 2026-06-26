#!/usr/bin/env bash
#
# Pre-run sweep — drop test-fixture tables left behind by INTERRUPTED live-test runs.
#
# Live tests name their fixtures `<prefix>_<pid>_<counter>` (tests/common::unique_name)
# and drop them via an RAII Drop guard on test exit. That guard does NOT fire when the
# test PROCESS is killed — nextest slow-timeout, SIGKILL, Ctrl-C — which is common for
# the slow cloud suites (Snowflake / BigQuery / differential). Each interrupted run can
# therefore leave fixture tables behind, and they accumulate in the shared `rivet`
# database (which is ALSO the init.sql + seed.rs fixture DB), polluting it over time.
#
# The `_<digits>_<digits>` suffix uniquely marks a unique_name fixture: the persistent
# fixtures (users, orders, content_items, rivet_type_matrix, … seeded by init.sql /
# seed.rs) never carry it, so this sweep cannot touch them.
#
# Best-effort per engine: a service that isn't up is skipped, never fatal. The nextest
# setup script (.config/nextest.toml) runs this before a live run; `make sweep-test-db`
# runs it by hand.
set -uo pipefail

# unique_name suffix: `_<pid>_<counter>` at end of name.
PG_PAT='_[0-9]+_[0-9]+$'
MY_PAT='_[0-9]+_[0-9]+$'

echo "sweep-test-cruft: dropping stale unique_name fixtures (suffix _<pid>_<counter>)"

# ── PostgreSQL (rivet-postgres-1 :5432/rivet) ────────────────────────────────
if docker exec rivet-postgres-1 true 2>/dev/null; then
  docker exec -i rivet-postgres-1 psql -U rivet -d rivet -q -v ON_ERROR_STOP=0 >/dev/null 2>&1 <<SQL
DO \$\$
DECLARE r RECORD; n int := 0;
BEGIN
  FOR r IN SELECT tablename FROM pg_tables
           WHERE schemaname='public' AND tablename ~ '${PG_PAT}' LOOP
    EXECUTE 'DROP TABLE IF EXISTS public.' || quote_ident(r.tablename) || ' CASCADE';
    n := n + 1;
  END LOOP;
  RAISE NOTICE 'postgres: dropped % stale fixtures', n;
END \$\$;
SQL
  echo "  postgres: swept"
else
  echo "  postgres: not up — skipped"
fi

# ── MySQL (rivet-mysql-1 :3306/rivet) ────────────────────────────────────────
if docker exec rivet-mysql-1 true 2>/dev/null; then
  docker exec rivet-mysql-1 mysql -urivet -privet rivet -N -e \
    "SELECT CONCAT('DROP TABLE IF EXISTS \`', table_name, '\`;') FROM information_schema.tables WHERE table_schema='rivet' AND table_name REGEXP '${MY_PAT}'" \
    2>/dev/null | docker exec -i rivet-mysql-1 mysql -urivet -privet rivet 2>/dev/null
  echo "  mysql: swept"
else
  echo "  mysql: not up — skipped"
fi

# ── SQL Server: follow-up. T-SQL has no regex; a precise `_<pid>_<counter>$`
#    match needs PATINDEX gymnastics or a CLR function. The mssql live suites
#    leak far less (fewer, slower), so this is deferred rather than done loosely
#    (a loose LIKE risks dropping a real fixture). Track in the roadmap.

echo "sweep-test-cruft: done"
