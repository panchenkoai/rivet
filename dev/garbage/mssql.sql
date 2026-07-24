-- Obfuscated GARBAGE-profile fixture (SQL Server) — see dev/garbage/postgres.sql
-- for the rationale. Materialized in a NON-default schema `ext`. ZERO source
-- identity, ZERO real data. Deterministic + idempotent. Via `make seed-garbage`.

IF SCHEMA_ID('ext') IS NOT NULL
BEGIN
    DECLARE @drop NVARCHAR(MAX) = N'';
    SELECT @drop = @drop + N'DROP TABLE ext.' + QUOTENAME(name) + N';'
    FROM sys.tables WHERE schema_id = SCHEMA_ID('ext');
    EXEC(@drop);
END
GO
IF SCHEMA_ID('ext') IS NULL EXEC('CREATE SCHEMA ext');
GO

-- Fleet majority: bigint PK + BOTH timestamps → keyset(id).
CREATE TABLE ext.bigint_pk_dual_ts (
    id BIGINT PRIMARY KEY,
    payload NVARCHAR(200) NOT NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 NULL
);
INSERT INTO ext.bigint_pk_dual_ts (id, payload, updated_at)
SELECT value, CONCAT(N'row', value), DATEADD(MINUTE, -value, SYSUTCDATETIME())
FROM GENERATE_SERIES(CAST(1 AS BIGINT), CAST(500 AS BIGINT));
GO

-- Int-PK minority.
CREATE TABLE ext.int_pk_dual_ts (
    id INT PRIMARY KEY,
    payload NVARCHAR(200) NOT NULL,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 NULL
);
INSERT INTO ext.int_pk_dual_ts (id, payload, updated_at)
SELECT value, CONCAT(N'row', value), DATEADD(MINUTE, -value, SYSUTCDATETIME())
FROM GENERATE_SERIES(1, 300);
GO

-- Sparse key: span vastly exceeds the row count (the sparse-guard shape).
CREATE TABLE ext.sparse_key (id BIGINT PRIMARY KEY, payload INT NOT NULL);
INSERT INTO ext.sparse_key (id, payload)
SELECT 1 + value * 1000000, value FROM GENERATE_SERIES(CAST(0 AS BIGINT), CAST(199 AS BIGINT));
GO

-- Scale-0 DECIMAL PK → an explicit range chunk_column must LOUDLY bail (#103).
CREATE TABLE ext.decimal_key (dkey DECIMAL(15,0) PRIMARY KEY, payload NVARCHAR(200) NOT NULL);
INSERT INTO ext.decimal_key (dkey, payload)
SELECT value, CONCAT(N'row', value) FROM GENERATE_SERIES(CAST(1 AS BIGINT), CAST(250 AS BIGINT));
GO

-- Keyless, cursorless → full-mode fallback.
CREATE TABLE ext.no_pk_no_ts (label NVARCHAR(200) NOT NULL, amount INT NOT NULL);
INSERT INTO ext.no_pk_no_ts (label, amount)
SELECT CONCAT(N'label', value), value % 100 FROM GENERATE_SERIES(1, 150);
GO

-- WIDE table: 160 int columns of 30-char names. The introspection STRING_AGG of
-- the names exceeds the nvarchar 4000-byte cap and raised Msg 9829 before the
-- CONVERT(nvarchar(max)) fix (#21) — every chunked/keyset plan on it failed.
DECLARE @cols NVARCHAR(MAX) = N'';
DECLARE @i INT = 0;
WHILE @i < 160
BEGIN
    SET @cols = @cols + N', ' + N'col_' + RIGHT(REPLICATE('0', 26) + CAST(@i AS VARCHAR(3)), 26) + N' INT NOT NULL DEFAULT 0';
    SET @i = @i + 1;
END
EXEC(N'CREATE TABLE ext.wide_cols (id BIGINT PRIMARY KEY' + @cols + N')');
INSERT INTO ext.wide_cols (id) VALUES (1),(2),(3),(4),(5);
GO
