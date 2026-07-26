-- Parallel-keyset sparse GOLDEN fixture — PostgreSQL.
-- 10,000,000 rows, wide, sparse key id = n*997 (span_per_row ~= 997).
-- Every column is derived from n, so the rows are IDENTICAL across engines.
-- Heavy: seed on the devbox, not the local suite.
DROP TABLE IF EXISTS keyset_sparse;
CREATE TABLE keyset_sparse (
    id          BIGINT           PRIMARY KEY,   -- sparse keyset key (n*997)
    grp         INTEGER          NOT NULL,
    amount      DECIMAL(18,4)    NOT NULL,
    ratio       DOUBLE PRECISION NOT NULL,
    label       VARCHAR(64)      NOT NULL,
    descr       VARCHAR(200)     NOT NULL,
    code        CHAR(10)         NOT NULL,
    flag        BOOLEAN          NOT NULL,
    created_at  TIMESTAMP        NOT NULL,
    updated_on  DATE             NOT NULL,
    big_n       BIGINT           NOT NULL,
    note        TEXT             NOT NULL
);

INSERT INTO keyset_sparse
SELECT
    n::bigint * 997                                      AS id,  -- cast: generate_series is int4, n*997 overflows past n≈2.15M
    (n % 1000)::int                                      AS grp,
    ((n % 100000) + (n % 10000) / 10000.0)::decimal(18,4) AS amount,
    n / 7.0                                              AS ratio,
    'lbl-' || (n % 500)                                  AS label,
    'row-' || n || '-payload'                            AS descr,
    lpad(to_hex(n), 10, '0')                             AS code,
    (n % 2 = 0)                                          AS flag,
    TIMESTAMP '2020-01-01 00:00:00' + (n * INTERVAL '1 second') AS created_at,
    DATE '2020-01-01' + (n % 3650)                       AS updated_on,
    (n::bigint) * (n::bigint)                            AS big_n,
    'note-' || n                                         AS note
FROM generate_series(1, 10000000) AS g(n);

-- Sanity: expect 10,000,000 rows, min id 997, max id 9,970,000,000,
-- span_per_row ~= 997.
SELECT count(*) AS rows, min(id) AS min_id, max(id) AS max_id,
       round((max(id) - min(id))::numeric / count(*), 1) AS span_per_row
FROM keyset_sparse;
