-- Benchmark seed for SQL Server — DBA-harm harness baseline.
--
-- SQL Server twin of the `bench_narrow` profile in `seed_bench_pg.sql`: a
-- 500 000-row numeric/date table on a single-integer PK, the shape the
-- DBA-harm probe (`mssql_db_bench.sh`) drives a chunked rivet export over to
-- measure rivet's footprint on the source (longest open transaction, lock
-- count, Log Flush Waits). One table is enough — the harm signals are about
-- *how* rivet reads, not the column zoo (that's the type matrix).
--
-- Usage (database `rivet` must exist — dev/mssql/init.sql creates it):
--   sqlcmd -S localhost -U sa -P 'Rivet_Passw0rd!' -C -d rivet -i dev/bench/seed_bench_mssql.sql
--
-- SQL Server 2022 ships `GENERATE_SERIES`, so the 500k rows seed without a
-- recursive CTE or a numbers table.

IF OBJECT_ID('bench_narrow', 'U') IS NOT NULL DROP TABLE bench_narrow;
GO

CREATE TABLE bench_narrow (
    id          BIGINT       PRIMARY KEY,
    score       FLOAT        NOT NULL,
    category    SMALLINT     NOT NULL,
    flag        BIT          NOT NULL,
    -- DATETIME2(6) = microsecond, lossless through rivet's Timestamp(µs) map
    -- (a bare datetime2(7) cursor would re-export its boundary row — see
    -- docs/type-mapping.md known gap 4).
    created_at  DATETIME2(6) NOT NULL
);
GO

INSERT INTO bench_narrow (id, score, category, flag, created_at)
SELECT
    value,
    value * 1.23456,
    CAST(value % 100 AS SMALLINT),
    CAST(CASE WHEN value % 3 = 0 THEN 1 ELSE 0 END AS BIT),
    DATEADD(SECOND, -(500000 - value), SYSUTCDATETIME())
FROM GENERATE_SERIES(CONVERT(BIGINT, 1), CONVERT(BIGINT, 500000));
GO
