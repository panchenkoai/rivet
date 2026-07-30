-- LIGHT CDC seed — SQL Server (2019 and 2022). Same schema + formulas as the
-- common seed, 10 rows per table. Applied by up.sh via host sqlcmd (no initdb
-- hook in the image). Idempotent. Enabling CDC itself
-- (sys.sp_cdc_enable_db / _table) is left to the tests, as in the canonical
-- stand — the Agent is already running (MSSQL_AGENT_ENABLED).

IF DB_ID('rivet') IS NULL CREATE DATABASE rivet;
GO
USE rivet;
GO

IF OBJECT_ID('dbo.users') IS NULL
BEGIN
    CREATE TABLE dbo.users (
        id         INT PRIMARY KEY,
        email      VARCHAR(64)   NOT NULL,
        name       VARCHAR(64)   NOT NULL,
        country    VARCHAR(2)    NOT NULL,
        active     BIT           NOT NULL,
        balance    DECIMAL(12,2) NOT NULL,
        created_at DATETIME2(0)  NOT NULL
    );
    CREATE TABLE dbo.orders (
        id         INT PRIMARY KEY,
        user_id    INT           NOT NULL,
        amount     DECIMAL(12,2) NOT NULL,
        status     VARCHAR(16)   NOT NULL,
        order_date DATE          NOT NULL,
        created_at DATETIME2(0)  NOT NULL
    );
    CREATE TABLE dbo.events (
        id          BIGINT PRIMARY KEY,
        user_id     INT          NOT NULL,
        event_type  VARCHAR(16)  NOT NULL,
        payload     VARCHAR(64)  NOT NULL,
        occurred_at DATETIME2(0) NOT NULL
    );
END
GO

IF NOT EXISTS (SELECT 1 FROM dbo.users)
BEGIN
    ;WITH t AS (
        SELECT TOP (10) CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS INT) AS n
        FROM sys.all_objects
    )
    INSERT INTO dbo.users
    SELECT n, CONCAT('user', n, '@example.com'), CONCAT('User ', n),
           CHOOSE((n % 10) + 1, 'US','DE','FR','GB','UA','PL','ES','IT','NL','SE'),
           CAST(CASE WHEN n % 7 <> 0 THEN 1 ELSE 0 END AS BIT),
           CAST((n * 37) % 1000000 AS DECIMAL(14,2)) / 100,
           DATEADD(SECOND, n % 86400, DATEADD(DAY, n % 1000, CAST('2020-01-01T00:00:00' AS DATETIME2(0))))
    FROM t;
END
GO

IF NOT EXISTS (SELECT 1 FROM dbo.orders)
BEGIN
    ;WITH t AS (
        SELECT TOP (10) CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS INT) AS n
        FROM sys.all_objects
    )
    INSERT INTO dbo.orders
    SELECT n, (n % 10) + 1,
           CAST((n * 17) % 500000 AS DECIMAL(14,2)) / 100,
           CHOOSE((n % 5) + 1, 'new','paid','shipped','done','cancelled'),
           DATEADD(DAY, n % 730, CAST('2021-01-01' AS DATE)),
           DATEADD(SECOND, n % 86400, DATEADD(DAY, n % 730, CAST('2021-01-01T00:00:00' AS DATETIME2(0))))
    FROM t;
END
GO

IF NOT EXISTS (SELECT 1 FROM dbo.events)
BEGIN
    ;WITH t AS (
        SELECT TOP (10) CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS INT) AS n
        FROM sys.all_objects
    )
    INSERT INTO dbo.events
    SELECT n, (n % 10) + 1,
           CHOOSE((n % 8) + 1, 'view','click','signup','login','logout','purchase','refund','error'),
           CONCAT('{"seq":', n, ',"grp":', n % 100, '}'),
           DATEADD(SECOND, n % 31536000, CAST('2022-01-01T00:00:00' AS DATETIME2(0)))
    FROM t;
END
GO
