-- MSSQL source-engine fixture (piped through sqlcmd after the container is
-- healthy; the 2022 image has no auto-init hook). Covers the core type set the
-- tiberius → Arrow path maps today.
IF DB_ID('rivet') IS NULL CREATE DATABASE rivet;
GO
USE rivet;
GO

IF OBJECT_ID('dbo.type_matrix', 'U') IS NOT NULL DROP TABLE dbo.type_matrix;
GO
CREATE TABLE dbo.type_matrix (
    id          INT          NOT NULL PRIMARY KEY,
    c_bigint    BIGINT       NULL,
    c_tinyint   TINYINT      NULL,
    c_smallint  SMALLINT     NULL,
    c_bit       BIT          NULL,
    c_float     FLOAT        NULL,
    c_real      REAL         NULL,
    c_decimal   DECIMAL(12,2) NULL,
    c_nvarchar  NVARCHAR(100) NULL,
    c_varchar   VARCHAR(100) NULL,
    c_uuid      UNIQUEIDENTIFIER NULL,
    c_varbinary VARBINARY(16) NULL,
    c_date      DATE         NULL,
    c_datetime2 DATETIME2    NULL
);
GO

INSERT INTO dbo.type_matrix
    (id, c_bigint, c_tinyint, c_smallint, c_bit, c_float, c_real, c_decimal,
     c_nvarchar, c_varchar, c_uuid, c_varbinary, c_date, c_datetime2)
VALUES
    (1, 9000000000, 200, 30000, 1, 3.14159, 2.5, 1234.56,
     N'héllo wörld', 'ascii', '6F9619FF-8B86-D011-B42D-00C04FC964FF',
     0x00112233445566778899AABBCCDDEEFF, '2026-01-15', '2026-01-15T13:45:30.1234567'),
    (2, -1, 0, -30000, 0, -0.5, 0.0, -0.01,
     N'second', NULL, NULL, NULL, '1999-12-31', '2000-01-01T00:00:00'),
    (3, 0, 255, 0, 1, 0.0, 0.0, 0.00,
     NULL, 'three', NEWID(), 0xDEADBEEF, NULL, NULL);
GO

-- Larger table for chunked-mode / keyset planning probes.
--
-- NOT named `dbo.orders`. That name belongs to the canonical cross-engine seed
-- (`src/bin/seed/mssql.rs`), which creates `orders` with the same shape and row
-- count as PostgreSQL and MySQL — the fixture every cross-engine comparison
-- rests on. This file used to create `dbo.orders` too, with an incompatible
-- schema (`id, name, amount`) and only 500 rows, and the seed DROP+CREATEs, so
-- whichever ran last won. The conflict stayed invisible for as long as nobody
-- ran `seed --target mssql` — and nobody did, because `seed-release` named
-- postgres and mysql and skipped SQL Server. Closing that gap on 2026-08-04
-- broke `init_mssql_single_table_emits_valid_config_that_passes_check`, which
-- had been asserting against this file's schema.
IF OBJECT_ID('dbo.planning_probe', 'U') IS NOT NULL DROP TABLE dbo.planning_probe;
GO
CREATE TABLE dbo.planning_probe (
    id     BIGINT       NOT NULL PRIMARY KEY,
    name   NVARCHAR(50) NOT NULL,
    amount DECIMAL(12,2) NOT NULL
);
GO
INSERT INTO dbo.planning_probe (id, name, amount)
SELECT TOP (500)
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)),
    CONCAT(N'order_', ROW_NUMBER() OVER (ORDER BY (SELECT NULL))),
    CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS DECIMAL(12,2)) * 1.5
FROM sys.all_objects a CROSS JOIN sys.all_objects b;
GO

-- ─── Type-matrix demo, the SQL Server twin of the PG/MySQL fixture ──────────
--
-- `dev/postgres/init.sql` and `dev/mysql/init.sql` have declared
-- `rivet_type_matrix` for a long time; this file did not, and the cross-engine
-- stand tests that read it (`chunking_stand::stand_meta_columns_mssql`,
-- `stand_type_matrix_every_consumer_mssql`) passed locally only because a
-- hand-created table happened to sit in a long-lived container. On a fresh CI
-- stand they failed with `Invalid object name 'dbo.rivet_type_matrix'`. The
-- fixture belongs to the stand, so it is declared here — same four rows and the
-- same semantic columns as PostgreSQL, in the T-SQL types rivet maps them to.
--
-- `created_at` (datetime2) and `created_at_tz` (datetimeoffset) are the pair
-- that matters: naive and zone-carrying in one export, which is what makes the
-- row-hash / checksum / uniqueness consumers face a real timestamp instead of
-- two integers.
IF OBJECT_ID('dbo.rivet_type_matrix', 'U') IS NOT NULL DROP TABLE dbo.rivet_type_matrix;
GO
CREATE TABLE dbo.rivet_type_matrix (
    id            BIGINT           NOT NULL PRIMARY KEY,
    label         NVARCHAR(100)    NOT NULL,
    amount        DECIMAL(18,2)    NULL,
    fee           DECIMAL(18,6)    NULL,
    created_at    DATETIME2(6)     NOT NULL,
    created_at_tz DATETIMEOFFSET(6) NOT NULL,
    raw_bytes     VARBINARY(64)    NOT NULL,
    uid           UNIQUEIDENTIFIER NOT NULL,
    attrs         NVARCHAR(MAX)    NULL
);
GO
INSERT INTO dbo.rivet_type_matrix
    (id, label, amount, fee, created_at, created_at_tz, raw_bytes, uid, attrs)
VALUES
  (1, N'payments-like', 0.10, 0.000001,
      '2035-08-07T09:08:07.987654', '2035-08-07T09:08:07.987654+00:00',
      0x00FF012345, 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380011', N'{"tier":"gold","n":1}'),
  (2, N'payments-like', 0.20, 0.000002,
      '2019-02-03T03:07:06.554433', '2019-02-03T08:07:06.554433+05:00',
      0xDEADBEEF, 'B0EEBC99-9C0B-4EF8-BB6D-6BB9BD380022', N'["a","b"]'),
  (3, N'payments-like', 999999999999.99, 10.123456,
      '2020-01-15T00:00:00.000001', '2020-01-15T00:00:00.000001+00:00',
      0xCAFE, 'C0EEBC99-9C0B-4EF8-BB6D-6BB9BD380033', N'{"big":true}'),
  (4, N'payments-like', -100.05, -0.123456,
      '2021-06-30T12:59:59.999999', '2021-06-30T12:59:59.999999+00:00',
      0x00, 'D0EEBC99-9C0B-4EF8-BB6D-6BB9BD380044', N'{}');
GO
