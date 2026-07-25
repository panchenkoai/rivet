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
FROM GENERATE_SERIES(CAST(1 AS BIGINT), CAST(150000 AS BIGINT));
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
FROM GENERATE_SERIES(1, 150000);
GO

-- Sparse key: span vastly exceeds the row count (the sparse-guard shape).
CREATE TABLE ext.sparse_key (id BIGINT PRIMARY KEY, payload INT NOT NULL);
INSERT INTO ext.sparse_key (id, payload)
SELECT 1 + value * 1000000, value FROM GENERATE_SERIES(CAST(0 AS BIGINT), CAST(149999 AS BIGINT));
GO

-- Scale-0 DECIMAL PK → an explicit range chunk_column must LOUDLY bail (#103).
CREATE TABLE ext.decimal_key (dkey DECIMAL(15,0) PRIMARY KEY, payload NVARCHAR(200) NOT NULL);
INSERT INTO ext.decimal_key (dkey, payload)
SELECT value, CONCAT(N'row', value) FROM GENERATE_SERIES(CAST(1 AS BIGINT), CAST(150000 AS BIGINT));
GO

-- Keyless, cursorless → full-mode fallback.
CREATE TABLE ext.no_pk_no_ts (label NVARCHAR(200) NOT NULL, amount INT NOT NULL);
INSERT INTO ext.no_pk_no_ts (label, amount)
SELECT CONCAT(N'label', value), value % 100 FROM GENERATE_SERIES(1, 150000);
GO

-- ── The MESSY reality (distilled from a real stuck run's state DB) ───────────
-- HISTORY/VERSION table: NO `id`, keyed by a non-PK integer `ref_id` (range-
-- chunked). Non-unique index on ref_id, no PK.
CREATE TABLE ext.ref_id_history (
    ref_id BIGINT NOT NULL,
    version INT NOT NULL,
    field NVARCHAR(200),
    field_cur NVARCHAR(10),
    sign SMALLINT NOT NULL DEFAULT 1,
    status NVARCHAR(50),
    -- money as DECIMAL(11,4) VALUE columns — the DOMINANT field profile (177 such
    -- columns across the real DB); init emits a `columns:` override for each.
    cart DECIMAL(11,4),
    earning DECIMAL(11,4),
    subtotal DECIMAL(11,4),
    total DECIMAL(11,4),
    amount_cur NVARCHAR(10),
    user_cur NCHAR(3),
    purchase_type NVARCHAR(50),
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2
);
CREATE INDEX ix_ref_id_history_ref_id ON ext.ref_id_history (ref_id);
INSERT INTO ext.ref_id_history (ref_id, version, field, field_cur, sign, status, cart, earning, subtotal, total, amount_cur, user_cur, purchase_type, created_at, updated_at)
SELECT (value % 400) + 1, (value / 400) + 1, CONCAT(N'f', value), N'EUR', IIF(value % 2 = 0, 1, -1),
       CHOOSE(1 + value % 3, N'pending', N'done', N'void'),
       CAST((value % 900000 + 1) AS DECIMAL(11,4)) / 100, CAST((value % 500000 + 1) AS DECIMAL(11,4)) / 100,
       CAST((value % 300000 + 1) AS DECIMAL(11,4)) / 100, CAST((value % 1200000 + 1) AS DECIMAL(11,4)) / 100,
       N'USD', CHOOSE(1 + value % 3, N'EUR', N'USD', N'PLN'), CHOOSE(1 + value % 2, N'a', N'b'),
       DATEADD(MINUTE, -value, SYSUTCDATETIME()), DATEADD(SECOND, -value, SYSUTCDATETIME())
FROM GENERATE_SERIES(1, 150000);
GO

-- Keyed by `order_id` (NOT `id`): a unique index makes it keyset-able on a
-- non-`id` key. Wide-ish string columns.
CREATE TABLE ext.order_keyed (
    order_id BIGINT NOT NULL,
    md5 CHAR(32) NOT NULL,
    currency CHAR(3) NOT NULL,
    status NVARCHAR(50) NOT NULL,
    subid NVARCHAR(50),
    advcampaign_id INT
);
CREATE UNIQUE INDEX ux_order_keyed_order_id ON ext.order_keyed (order_id);
INSERT INTO ext.order_keyed (order_id, md5, currency, status, subid, advcampaign_id)
SELECT value, CONVERT(CHAR(32), HASHBYTES('MD5', CAST(value AS VARCHAR(20))), 2),
       CHOOSE(1 + value % 3, 'EUR', 'USD', 'PLN'), CHOOSE(1 + value % 2, N'approved', N'declined'),
       CONCAT(N'sub', value % 50), (value % 900) + 100
FROM GENERATE_SERIES(CAST(1 AS BIGINT), CAST(150000 AS BIGINT));
GO

-- HEAP: no PK, no index → full mode only.
CREATE TABLE ext.heap_no_key (payload NVARCHAR(200) NOT NULL, n INT NOT NULL);
INSERT INTO ext.heap_no_key (payload, n)
SELECT REPLICATE('x', 20 + value % 50), value FROM GENERATE_SERIES(1, 150000);
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
