#!/usr/bin/env bash
# Skewed-key MySQL bench fixture for the parallel-keyset probe.
# 1M rows, id BIGINT PK, 95% dense in [1..950k], 5% sparse out to ~501M
# (span_per_row ~= 501) — the sparse footgun a value-split blows up on.
#
#   dev/parallel_keyset/fixture.sh            # brings up mysql:8.4 on :3310, seeds `big`
#   cargo run --release --example parallel_keyset_probe -- \
#     "mysql://rivet:rivet@127.0.0.1:3310/bench" big id 4 10000
set -euo pipefail
docker rm -f pk-mysql >/dev/null 2>&1 || true
docker run -d --name pk-mysql -e MYSQL_ROOT_PASSWORD=rivet -e MYSQL_DATABASE=bench \
  -e MYSQL_USER=rivet -e MYSQL_PASSWORD=rivet -p 3310:3306 mysql:8.4 >/dev/null
for i in $(seq 1 45); do docker exec pk-mysql mysql -urivet -privet bench -e "SELECT 1" >/dev/null 2>&1 && break; sleep 3; done
docker exec -i pk-mysql mysql -urivet -privet bench <<'SQL'
DROP TABLE IF EXISTS big;
CREATE TABLE big (id BIGINT PRIMARY KEY, payload VARCHAR(120) NOT NULL, n INT NOT NULL) ENGINE=InnoDB;
INSERT INTO big (id, payload, n)
WITH RECURSIVE nums(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM nums WHERE n < 999)
SELECT
  CASE WHEN rn <= 950000 THEN rn ELSE 1000000 + (rn-950000)*10000 END AS id,
  CONCAT('row-', rn, '-', REPEAT('x',80)), rn
FROM (SELECT a.n*1000 + b.n + 1 AS rn FROM nums a CROSS JOIN nums b) t
WHERE rn BETWEEN 1 AND 1000000;
SELECT COUNT(*) AS row_cnt, MIN(id) AS min_id, MAX(id) AS max_id,
       ROUND((MAX(id)-MIN(id))/COUNT(*),1) AS span_per_row FROM big;
SQL
echo "fixture ready: mysql://rivet:rivet@127.0.0.1:3310/bench table=big"
