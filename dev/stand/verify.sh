#!/usr/bin/env bash
# Cross-engine seed verification. The common seed is deterministic, so every
# BATCH SQL engine must report the IDENTICAL tuple of
#   users_count | orders_count | events_count | sum(balance_cents) | sum(amount_cents) | sum(events.user_id)
# and every CDC SQL engine the identical light tuple. Sums are compared in
# integer CENTS to dodge decimal formatting differences between clients.
# Mongo instances are checked for doc counts only (the Mongo seed is
# volume-parity, not row-parity, by design).
set -uo pipefail
cd "$(dirname "$0")"
SA_PASS='Rivet_Passw0rd!'
fail=0

pg_tuple() { # port
  psql "postgresql://rivet:rivet@127.0.0.1:$1/rivet" -tA -F'|' -c \
    "SELECT (SELECT count(*) FROM users), (SELECT count(*) FROM orders), (SELECT count(*) FROM events),
            (SELECT sum((balance*100)::bigint) FROM users),
            (SELECT sum((amount*100)::bigint) FROM orders),
            (SELECT sum(user_id) FROM events)" 2>&1 | head -1
}

mysql_tuple() { # port
  mysql -h 127.0.0.1 -P "$1" -urivet -privet rivet -N -B -e \
    "SELECT (SELECT count(*) FROM users), (SELECT count(*) FROM orders), (SELECT count(*) FROM events),
            (SELECT sum(CAST(balance*100 AS SIGNED)) FROM users),
            (SELECT sum(CAST(amount*100 AS SIGNED)) FROM orders),
            (SELECT sum(user_id) FROM events)" 2>/dev/null | tr '\t' '|'
}

mssql_tuple() { # port
  sqlcmd -S "127.0.0.1,$1" -U sa -P "$SA_PASS" -C -d rivet -h -1 -W -s'|' -Q \
    "SET NOCOUNT ON; SELECT (SELECT count(*) FROM dbo.users), (SELECT count(*) FROM dbo.orders), (SELECT count(*) FROM dbo.events),
            (SELECT sum(CAST(balance*100 AS BIGINT)) FROM dbo.users),
            (SELECT sum(CAST(amount*100 AS BIGINT)) FROM dbo.orders),
            (SELECT sum(CAST(user_id AS BIGINT)) FROM dbo.events)" 2>&1 | head -1
}

mongo_counts() { # port
  mongosh --quiet "mongodb://127.0.0.1:$1/rivet?directConnection=true" --eval \
    'print(db.orders.countDocuments({}) + "|" + db.events.countDocuments({}))' 2>/dev/null
}

check_group() { # name, expected engines as "label tuple" lines on stdin
  local name="$1" ref="" bad=0
  echo "== $name"
  while read -r label tuple; do
    printf '  %-14s %s\n' "$label" "$tuple"
    if [ -z "$tuple" ]; then bad=1; continue; fi
    if [ -z "$ref" ]; then ref="$tuple"; elif [ "$tuple" != "$ref" ]; then bad=1; fi
  done
  if [ "$bad" = "1" ]; then echo "  MISMATCH in $name"; fail=1; else echo "  OK — all identical"; fi
}

check_group "batch SQL (heavy common seed)" <<EOF
pg14:5514 $(pg_tuple 5514)
pg18:5518 $(pg_tuple 5518)
mysql57:3557 $(mysql_tuple 3557)
mysql80:3580 $(mysql_tuple 3580)
mssql19:1519 $(mssql_tuple 1519)
mssql22:1522 $(mssql_tuple 1522)
EOF

check_group "cdc SQL (light seed)" <<EOF
pg14:5614 $(pg_tuple 5614)
pg18:5618 $(pg_tuple 5618)
mysql57:3657 $(mysql_tuple 3657)
mysql80:3680 $(mysql_tuple 3680)
mssql19:1619 $(mssql_tuple 1619)
mssql22:1622 $(mssql_tuple 1622)
EOF

check_group "batch Mongo (orders|events doc counts)" <<EOF
mongo44:27104 $(mongo_counts 27104)
mongo50:27105 $(mongo_counts 27105)
mongo80:27108 $(mongo_counts 27108)
EOF

check_group "cdc Mongo (orders|events doc counts)" <<EOF
mongo44:27204 $(mongo_counts 27204)
mongo50:27205 $(mongo_counts 27205)
mongo80:27208 $(mongo_counts 27208)
EOF

exit $fail
