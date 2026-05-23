#!/usr/bin/env bash
# Seed the `pa_audit` fixture table on both PG and MySQL.
# 30 rows is enough for the matrix scenarios and keeps applies fast.
set -eu

docker exec rivet-postgres-1 psql -U rivet -d rivet -c "
DROP TABLE IF EXISTS pa_audit;
CREATE TABLE pa_audit (
  id INT PRIMARY KEY,
  name TEXT,
  updated_at TIMESTAMP DEFAULT NOW()
);
INSERT INTO pa_audit (id, name)
  SELECT i, 'row_' || i FROM generate_series(1, 30) i;
" >/dev/null
echo "PG pa_audit: $(docker exec rivet-postgres-1 psql -U rivet -d rivet -tAc 'SELECT COUNT(*) FROM pa_audit;') rows"

docker exec rivet-mysql-1 mysql -urivet -privet rivet -e "
DROP TABLE IF EXISTS pa_audit;
CREATE TABLE pa_audit (
  id INT PRIMARY KEY,
  name VARCHAR(255),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
" 2>/dev/null
# MySQL INSERT loop: easier to drive from a sequence than the awkward
# self-join trick.
for i in $(seq 1 30); do
  docker exec rivet-mysql-1 mysql -urivet -privet rivet \
    -e "INSERT INTO pa_audit (id, name) VALUES ($i, 'row_$i');" 2>/dev/null
done
echo "MySQL pa_audit: $(docker exec rivet-mysql-1 mysql -urivet -privet -BN -e 'SELECT COUNT(*) FROM rivet.pa_audit;' 2>/dev/null) rows"
