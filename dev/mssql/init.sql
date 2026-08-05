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
