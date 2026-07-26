-- Parallel-keyset sparse GOLDEN fixture — SQL Server 2019+.
-- 10,000,000 rows, wide, sparse key id = n*997. Same rows as the PG/MySQL
-- fixtures (all columns derived from n). Heavy: seed on the devbox.
-- 10M rows via a cross-join tally (e8 = 10^8 rows, TOP 10M) — fast, set-based;
-- a recursive CTE to 10M would be impractically slow even with MAXRECURSION 0.
-- NOTE: the single 10M INSERT grows the transaction log; on the devbox set the
-- DB to SIMPLE recovery first (ALTER DATABASE ... SET RECOVERY SIMPLE) or batch.
IF OBJECT_ID('dbo.keyset_sparse', 'U') IS NOT NULL DROP TABLE dbo.keyset_sparse;
CREATE TABLE dbo.keyset_sparse (
    id          BIGINT        NOT NULL PRIMARY KEY,   -- sparse keyset key (n*997)
    grp         INT           NOT NULL,
    amount      DECIMAL(18,4) NOT NULL,
    ratio       FLOAT         NOT NULL,
    label       VARCHAR(64)   NOT NULL,
    descr       VARCHAR(200)  NOT NULL,
    code        CHAR(10)      NOT NULL,
    flag        BIT           NOT NULL,
    created_at  DATETIME2     NOT NULL,
    updated_on  DATE          NOT NULL,
    big_n       BIGINT        NOT NULL,
    note        VARCHAR(MAX)  NOT NULL
);

;WITH
  e1(x) AS (SELECT 1 FROM (VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1)) v(x)),  -- 10
  e2(x) AS (SELECT 1 FROM e1 a CROSS JOIN e1 b),                                    -- 100
  e4(x) AS (SELECT 1 FROM e2 a CROSS JOIN e2 b),                                    -- 10,000
  e8(x) AS (SELECT 1 FROM e4 a CROSS JOIN e4 b),                                    -- 100,000,000
  nums AS (SELECT TOP (10000000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n FROM e8)
INSERT INTO dbo.keyset_sparse
SELECT
    CAST(n AS BIGINT) * 997                             AS id,
    n % 1000                                            AS grp,
    CAST((n % 100000) + (n % 10000) / 10000.0 AS DECIMAL(18,4)) AS amount,
    n / 7.0                                             AS ratio,
    CONCAT('lbl-', n % 500)                             AS label,
    CONCAT('row-', n, '-payload')                       AS descr,
    RIGHT('0000000000' + FORMAT(n, 'X'), 10)            AS code,
    CAST(n % 2 AS BIT)                                  AS flag,
    DATEADD(SECOND, n, CAST('2020-01-01' AS DATETIME2)) AS created_at,
    DATEADD(DAY, n % 3650, CAST('2020-01-01' AS DATE))  AS updated_on,
    CAST(n AS BIGINT) * CAST(n AS BIGINT)               AS big_n,
    CONCAT('note-', n)                                  AS note
FROM nums;

SELECT COUNT(*) AS rows, MIN(id) AS min_id, MAX(id) AS max_id,
       ROUND(CAST(MAX(id) - MIN(id) AS FLOAT) / COUNT(*), 1) AS span_per_row
FROM dbo.keyset_sparse;
