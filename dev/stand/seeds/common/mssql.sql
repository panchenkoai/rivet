-- Common HEAVY deterministic seed — SQL Server dialect (2019 and 2022).
-- Formulas match postgres.sql / mysql.sql row-for-row.
-- The MSSQL image has no initdb hook: up.sh pipes this through host sqlcmd
-- after the container is healthy. Idempotent (guards on object + row count).

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
    CREATE INDEX ix_orders_user_id  ON dbo.orders (user_id);
    CREATE INDEX ix_events_user_id  ON dbo.events (user_id);
    CREATE INDEX ix_events_occurred ON dbo.events (occurred_at);
END
GO

IF NOT EXISTS (SELECT 1 FROM dbo.users)
BEGIN
    ;WITH t AS (
        SELECT TOP (50000) CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS INT) AS n
        FROM sys.all_objects a CROSS JOIN sys.all_objects b
    )
    INSERT INTO dbo.users (id, email, name, country, active, balance, created_at)
    SELECT n,
           CONCAT('user', n, '@example.com'),
           CONCAT('User ', n),
           CHOOSE((n % 10) + 1, 'US','DE','FR','GB','UA','PL','ES','IT','NL','SE'),
           CAST(CASE WHEN n % 7 <> 0 THEN 1 ELSE 0 END AS BIT),
           CAST((n * 37) % 1000000 AS DECIMAL(14,2)) / 100,
           DATEADD(SECOND, n % 86400,
             DATEADD(DAY, n % 1000, CAST('2020-01-01T00:00:00' AS DATETIME2(0))))
    FROM t;
END
GO

IF NOT EXISTS (SELECT 1 FROM dbo.orders)
BEGIN
    ;WITH t AS (
        SELECT TOP (200000) CAST(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS INT) AS n
        FROM sys.all_objects a CROSS JOIN sys.all_objects b
    )
    INSERT INTO dbo.orders (id, user_id, amount, status, order_date, created_at)
    SELECT n,
           (n % 50000) + 1,
           CAST((n * 17) % 500000 AS DECIMAL(14,2)) / 100,
           CHOOSE((n % 5) + 1, 'new','paid','shipped','done','cancelled'),
           DATEADD(DAY, n % 730, CAST('2021-01-01' AS DATE)),
           DATEADD(SECOND, n % 86400,
             DATEADD(DAY, n % 730, CAST('2021-01-01T00:00:00' AS DATETIME2(0))))
    FROM t;
END
GO

IF NOT EXISTS (SELECT 1 FROM dbo.events)
BEGIN
    ;WITH t AS (
        SELECT TOP (500000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
        FROM sys.all_objects a CROSS JOIN sys.all_objects b
    )
    INSERT INTO dbo.events (id, user_id, event_type, payload, occurred_at)
    SELECT n,
           (n % 50000) + 1,
           CHOOSE((n % 8) + 1, 'view','click','signup','login','logout','purchase','refund','error'),
           CONCAT('{"seq":', n, ',"grp":', n % 100, '}'),
           DATEADD(SECOND, n % 31536000, CAST('2022-01-01T00:00:00' AS DATETIME2(0)))
    FROM t;
END
GO
