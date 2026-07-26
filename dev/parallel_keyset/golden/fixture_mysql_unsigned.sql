-- Parallel-keyset sparse GOLDEN fixture — MySQL, BIGINT UNSIGNED key.
-- Sibling of fixture_mysql.sql, but the key lives ENTIRELY ABOVE i64::MAX
-- (id = 10^19 + n*10^8, so every id is in (10^19, 1.0001*10^19), all >
-- 9,223,372,036,854,775,807). This is the exact regime that broke unsigned
-- keyset in the field (values above i64::MAX mishandled as signed): the boundary
-- literals AND the paging cursor must round-trip as u64. Same golden counts as the
-- signed fixture (keyset pages by ROW, not key magnitude): 10,000,000 rows / 20
-- files at parallel=4, chunk_size=500000. Heavy: seed on the devbox.
-- MySQL-only: PostgreSQL and SQL Server have no unsigned BIGINT.
SET SESSION cte_max_recursion_depth = 20000;

DROP TABLE IF EXISTS keyset_sparse_unsigned;
CREATE TABLE keyset_sparse_unsigned (
    id          BIGINT UNSIGNED PRIMARY KEY,   -- sparse key ABOVE i64::MAX
    grp         INT             NOT NULL,
    amount      DECIMAL(18,4)   NOT NULL,
    ratio       DOUBLE          NOT NULL,
    label       VARCHAR(64)     NOT NULL,
    descr       VARCHAR(200)    NOT NULL,
    code        CHAR(10)        NOT NULL,
    flag        TINYINT(1)      NOT NULL,
    created_at  DATETIME        NOT NULL,
    updated_on  DATE            NOT NULL,
    big_n       BIGINT          NOT NULL,
    note        TEXT            NOT NULL
) ENGINE=InnoDB;

INSERT INTO keyset_sparse_unsigned
WITH RECURSIVE
  seq10k(a) AS (SELECT 0 UNION ALL SELECT a + 1 FROM seq10k WHERE a < 9999),
  seq1k(b)  AS (SELECT 0 UNION ALL SELECT b + 1 FROM seq1k  WHERE b < 999)
SELECT
    CAST(10000000000000000000 AS UNSIGNED) + CAST(n AS UNSIGNED) * 100000000 AS id,  -- > i64::MAX; CAST the literal so arithmetic stays integer (a bare 1e19 promotes to slow DECIMAL)
    n % 1000                                        AS grp,
    CAST((n % 100000) + (n % 10000) / 10000.0 AS DECIMAL(18,4)) AS amount,
    n / 7.0                                         AS ratio,
    CONCAT('lbl-', n % 500)                         AS label,
    CONCAT('row-', n, '-payload')                   AS descr,
    LPAD(HEX(n), 10, '0')                           AS code,
    n % 2 = 0                                        AS flag,
    DATE_ADD('2020-01-01 00:00:00', INTERVAL n SECOND)   AS created_at,
    DATE_ADD('2020-01-01', INTERVAL (n % 3650) DAY)      AS updated_on,
    CAST(n AS SIGNED) * CAST(n AS SIGNED)           AS big_n,
    CONCAT('note-', n)                              AS note
FROM (SELECT a * 1000 + b + 1 AS n FROM seq10k CROSS JOIN seq1k) t;

-- Sanity: min_id must exceed i64::MAX (9,223,372,036,854,775,807) — proves the
-- whole key set is unsigned-territory.
SELECT COUNT(*) AS row_count, MIN(id) AS min_id, MAX(id) AS max_id,
       MIN(id) > 9223372036854775807 AS all_above_i64_max
FROM keyset_sparse_unsigned;
