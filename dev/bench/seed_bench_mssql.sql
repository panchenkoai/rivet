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

-- ── bench_wide ───────────────────────────────────────────────────────────────
-- 100 000 rows × 10 NVARCHAR(200) columns — memory-pressure profile (the shape
-- where a streaming extractor's RSS pulls away from a buffer-the-result one).
IF OBJECT_ID('bench_wide', 'U') IS NOT NULL DROP TABLE bench_wide;
GO

CREATE TABLE bench_wide (
    id         BIGINT        PRIMARY KEY,
    col_a      NVARCHAR(200) NOT NULL,
    col_b      NVARCHAR(200) NOT NULL,
    col_c      NVARCHAR(200) NOT NULL,
    col_d      NVARCHAR(200) NOT NULL,
    col_e      NVARCHAR(200) NOT NULL,
    col_f      NVARCHAR(200) NOT NULL,
    col_g      NVARCHAR(200) NOT NULL,
    col_h      NVARCHAR(200) NOT NULL,
    col_i      NVARCHAR(200) NOT NULL,
    col_j      NVARCHAR(200) NOT NULL,
    updated_at DATETIME2(6)  NOT NULL
);
GO

INSERT INTO bench_wide (id, col_a, col_b, col_c, col_d, col_e,
                        col_f, col_g, col_h, col_i, col_j, updated_at)
SELECT
    value,
    REPLICATE(CHAR(65 + (value      ) % 26), 200),
    REPLICATE(CHAR(65 + (value +  1) % 26), 200),
    REPLICATE(CHAR(65 + (value +  2) % 26), 200),
    REPLICATE(CHAR(65 + (value +  3) % 26), 200),
    REPLICATE(CHAR(65 + (value +  4) % 26), 200),
    REPLICATE(CHAR(65 + (value +  5) % 26), 200),
    REPLICATE(CHAR(65 + (value +  6) % 26), 200),
    REPLICATE(CHAR(65 + (value +  7) % 26), 200),
    REPLICATE(CHAR(65 + (value +  8) % 26), 200),
    REPLICATE(CHAR(65 + (value +  9) % 26), 200),
    DATEADD(SECOND, -value, SYSUTCDATETIME())
FROM GENERATE_SERIES(CONVERT(BIGINT, 1), CONVERT(BIGINT, 100000));
GO

-- ── bench_hc — high cardinality (UUID + email + hash) ────────────────────────
-- 200 000 rows. Quality-uniqueness RAM-risk profile.
IF OBJECT_ID('bench_hc', 'U') IS NOT NULL DROP TABLE bench_hc;
GO
CREATE TABLE bench_hc (
    id         BIGINT           PRIMARY KEY,
    uuid_col   UNIQUEIDENTIFIER NOT NULL,
    email      NVARCHAR(200)    NOT NULL,
    session_id CHAR(32)         NOT NULL,
    updated_at DATETIME2(6)     NOT NULL
);
GO
INSERT INTO bench_hc (id, uuid_col, email, session_id, updated_at)
SELECT
    value,
    NEWID(),
    CONCAT('user', value, '@bench.example.com'),
    CONVERT(CHAR(32), HASHBYTES('MD5', CONCAT(CAST(value AS VARCHAR(20)), 'session')), 2),
    DATEADD(SECOND, -(200000 - value), SYSUTCDATETIME())
FROM GENERATE_SERIES(CONVERT(BIGINT, 1), CONVERT(BIGINT, 200000));
GO

-- ── bench_decimal — type conversion pressure ─────────────────────────────────
-- 200 000 rows of NUMERIC columns across scales.
IF OBJECT_ID('bench_decimal', 'U') IS NOT NULL DROP TABLE bench_decimal;
GO
CREATE TABLE bench_decimal (
    id         BIGINT        PRIMARY KEY,
    price      DECIMAL(18,4) NOT NULL,
    qty        DECIMAL(10,2) NOT NULL,
    discount   DECIMAL(5,4)  NOT NULL,
    total      DECIMAL(20,4) NOT NULL,
    updated_at DATETIME2(6)  NOT NULL
);
GO
INSERT INTO bench_decimal (id, price, qty, discount, total, updated_at)
SELECT
    value,
    ROUND(0.01 + (value % 99999) * 0.01, 4),
    ROUND(1 + (value % 9999) * 0.1, 2),
    ROUND(0.0001 + (value % 9999) * 0.0001, 4),
    ROUND(0.01 + (value % 99999) * 0.01 * (1 + (value % 9999) * 0.1), 4),
    DATEADD(SECOND, -(200000 - value), SYSUTCDATETIME())
FROM GENERATE_SERIES(CONVERT(BIGINT, 1), CONVERT(BIGINT, 200000));
GO

-- ── bench_sparse — null-ratio quality pressure ───────────────────────────────
-- 10 000 rows, many nullable columns at staggered null ratios.
IF OBJECT_ID('bench_sparse', 'U') IS NOT NULL DROP TABLE bench_sparse;
GO
CREATE TABLE bench_sparse (
    id         BIGINT       PRIMARY KEY,
    val_a      NVARCHAR(16) NULL,
    val_b      NVARCHAR(16) NULL,
    val_c      NVARCHAR(16) NULL,
    val_d      FLOAT        NULL,
    val_e      FLOAT        NULL,
    updated_at DATETIME2(6) NOT NULL
);
GO
INSERT INTO bench_sparse (id, val_a, val_b, val_c, val_d, val_e, updated_at)
SELECT
    value,
    CASE WHEN value % 10 = 0 THEN N'x' ELSE NULL END,
    CASE WHEN value %  5 = 0 THEN N'y' ELSE NULL END,
    CASE WHEN value %  3 = 0 THEN N'z' ELSE NULL END,
    CASE WHEN value %  4 = 0 THEN value * 1.1 ELSE NULL END,
    CASE WHEN value %  7 = 0 THEN value * 2.2 ELSE NULL END,
    DATEADD(SECOND, -(10000 - value), SYSUTCDATETIME())
FROM GENERATE_SERIES(CONVERT(BIGINT, 1), CONVERT(BIGINT, 10000));
GO

-- ── users / orders / events — the e2e app tables in the matrix ───────────────
-- Matched to tables_mysql.txt (500 / 2500 / 5000). NB: this re-creates `orders`
-- with the rich app schema (vs the simple chunking-probe `orders` in
-- dev/mssql/init.sql) — re-run init.sql to restore that before the source-engine
-- tests if you ran this bench seed in the same database.
IF OBJECT_ID('events', 'U') IS NOT NULL DROP TABLE events;
GO
IF OBJECT_ID('orders', 'U') IS NOT NULL DROP TABLE orders;
GO
IF OBJECT_ID('users', 'U') IS NOT NULL DROP TABLE users;
GO

CREATE TABLE users (
    id         INT IDENTITY(1,1) PRIMARY KEY,
    name       NVARCHAR(100)  NOT NULL,
    email      NVARCHAR(200)  NOT NULL,
    age        INT            NULL,
    balance    DECIMAL(12,2)  NULL,
    is_active  BIT            NOT NULL DEFAULT 1,
    bio        NVARCHAR(MAX)  NULL,
    created_at DATETIME2(6)   NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2(6)   NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
INSERT INTO users (name, email, age, balance, is_active, created_at, updated_at)
SELECT
    CONCAT(N'User ', value),
    CONCAT('user', value, '@example.com'),
    18 + (value % 60),
    ROUND(100 + (value % 9000) + 0.99, 2),
    CAST(CASE WHEN value % 5 <> 0 THEN 1 ELSE 0 END AS BIT),
    DATEADD(HOUR, -(500 - value), SYSUTCDATETIME()),
    DATEADD(HOUR, -(500 - value), SYSUTCDATETIME())
FROM GENERATE_SERIES(1, 500);
GO

CREATE TABLE orders (
    id         INT IDENTITY(1,1) PRIMARY KEY,
    user_id    INT            NOT NULL,
    product    NVARCHAR(200)  NOT NULL,
    quantity   INT            NOT NULL,
    price      DECIMAL(10,2)  NOT NULL,
    status     NVARCHAR(20)   NOT NULL DEFAULT 'pending',
    notes      NVARCHAR(MAX)  NULL,
    ordered_at DATETIME2(6)   NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2(6)   NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
INSERT INTO orders (user_id, product, quantity, price, status, ordered_at, updated_at)
SELECT
    ((value - 1) % 500) + 1,
    CONCAT(N'Product ', value % 100),
    1 + (value % 10),
    ROUND(10 + (value % 500) + 0.99, 2),
    CASE value % 4 WHEN 0 THEN N'pending' WHEN 1 THEN N'shipped'
                   WHEN 2 THEN N'delivered' ELSE N'cancelled' END,
    DATEADD(MINUTE, -(2500 - value), SYSUTCDATETIME()),
    DATEADD(MINUTE, -(2500 - value), SYSUTCDATETIME())
FROM GENERATE_SERIES(1, 2500);
GO

CREATE TABLE events (
    id         BIGINT IDENTITY(1,1) PRIMARY KEY,
    user_id    INT           NOT NULL,
    event_type NVARCHAR(50)  NOT NULL,
    payload    NVARCHAR(MAX) NULL,   -- SQL Server has no native JSON type
    ip_address NVARCHAR(45)  NULL,
    created_at DATETIME2(6)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
INSERT INTO events (user_id, event_type, payload, ip_address, created_at)
SELECT
    ((value - 1) % 500) + 1,
    CASE value % 5 WHEN 0 THEN N'click' WHEN 1 THEN N'view'
                   WHEN 2 THEN N'purchase' WHEN 3 THEN N'login' ELSE N'logout' END,
    CONCAT(N'{"seq":', value, N',"v":', value % 1000, N'}'),
    CONCAT('10.', value % 256, '.', (value / 256) % 256, '.1'),
    DATEADD(SECOND, -(5000 - value), SYSUTCDATETIME())
FROM GENERATE_SERIES(1, 5000);
GO

-- ── content_items — heavy-text worst case (memory showcase) ──────────────────
-- 60 227 rows × ~5 KB each (body + raw_html each ~2.4 KB of lorem) — the table
-- the PG headline (REPORT_pg.md) stresses. Mirrors dev/postgres/init.sql +
-- src/bin/seed/fast.rs (`repeat('lorem ipsum ', 200)`).
IF OBJECT_ID('content_items', 'U') IS NOT NULL DROP TABLE content_items;
GO
CREATE TABLE content_items (
    id            BIGINT IDENTITY(1,1) PRIMARY KEY,
    title         NVARCHAR(MAX)  NOT NULL,
    body          NVARCHAR(MAX)  NOT NULL,
    raw_html      NVARCHAR(MAX)  NOT NULL,
    metadata      NVARCHAR(MAX)  NULL,
    tags          NVARCHAR(MAX)  NULL,
    author_name   NVARCHAR(100)  NOT NULL,
    author_email  NVARCHAR(200)  NOT NULL,
    source_url    NVARCHAR(MAX)  NULL,
    category      NVARCHAR(50)   NULL,
    status        NVARCHAR(20)   NOT NULL DEFAULT 'draft',
    priority      INT            NOT NULL DEFAULT 0,
    view_count    INT            NOT NULL DEFAULT 0,
    comment_count INT            NOT NULL DEFAULT 0,
    word_count    INT            NOT NULL DEFAULT 0,
    language      CHAR(2)        NOT NULL DEFAULT 'en',
    published_at  DATETIME2(6)   NULL,
    updated_at    DATETIME2(6)   NOT NULL DEFAULT SYSUTCDATETIME(),
    created_at    DATETIME2(6)   NOT NULL DEFAULT SYSUTCDATETIME(),
    extra_data    NVARCHAR(MAX)  NULL
);
GO
INSERT INTO content_items (title, body, raw_html, metadata, tags, author_name,
    author_email, source_url, category, status, priority, view_count,
    comment_count, word_count, language, published_at)
SELECT
    CONCAT(N'Article ', value),
    REPLICATE(CAST(N'lorem ipsum ' AS NVARCHAR(MAX)), 200),
    CONCAT(N'<p>', REPLICATE(CAST(N'lorem ipsum ' AS NVARCHAR(MAX)), 200), N'</p>'),
    CONCAT(N'{"id":', value, N',"k":"v"}'),
    N'tag1,tag2,tag3',
    CONCAT(N'Author ', value % 100),
    CONCAT('author', value % 100, '@example.com'),
    CONCAT('https://example.com/', value),
    CASE value % 4 WHEN 0 THEN N'tech' WHEN 1 THEN N'news' WHEN 2 THEN N'sports' ELSE N'misc' END,
    CASE value % 3 WHEN 0 THEN N'published' WHEN 1 THEN N'draft' ELSE N'archived' END,
    value % 10, value % 10000, value % 500, 2400, 'en',
    DATEADD(SECOND, -value, SYSUTCDATETIME())
FROM GENERATE_SERIES(CONVERT(BIGINT, 1), CONVERT(BIGINT, 60227));
GO

-- ── page_views — wide analytics row (30 columns, 5 000 rows) ──────────────────
IF OBJECT_ID('page_views', 'U') IS NOT NULL DROP TABLE page_views;
GO
CREATE TABLE page_views (
    id              BIGINT IDENTITY(1,1) PRIMARY KEY,
    session_id      NVARCHAR(36)  NOT NULL,
    user_id         INT           NULL,
    url             NVARCHAR(MAX) NOT NULL,
    referrer        NVARCHAR(MAX) NULL,
    user_agent      NVARCHAR(MAX) NULL,
    ip_address      NVARCHAR(45)  NULL,
    country_code    CHAR(2)       NULL,
    region          NVARCHAR(100) NULL,
    city            NVARCHAR(100) NULL,
    device_type     NVARCHAR(20)  NULL,
    browser         NVARCHAR(50)  NULL,
    os              NVARCHAR(50)  NULL,
    screen_width    INT           NULL,
    screen_height   INT           NULL,
    viewport_width  INT           NULL,
    viewport_height INT           NULL,
    page_load_ms    INT           NULL,
    dom_ready_ms    INT           NULL,
    time_on_page_ms INT           NULL,
    scroll_depth_pct SMALLINT     NULL,
    click_count     SMALLINT      NULL,
    is_bounce       BIT           NOT NULL DEFAULT 0,
    utm_source      NVARCHAR(100) NULL,
    utm_medium      NVARCHAR(100) NULL,
    utm_campaign    NVARCHAR(200) NULL,
    utm_term        NVARCHAR(200) NULL,
    utm_content     NVARCHAR(200) NULL,
    custom_props    NVARCHAR(MAX) NULL,
    created_at      DATETIME2(6)  NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
INSERT INTO page_views (session_id, user_id, url, referrer, user_agent, ip_address,
    country_code, region, city, device_type, browser, os, screen_width, screen_height,
    viewport_width, viewport_height, page_load_ms, dom_ready_ms, time_on_page_ms,
    scroll_depth_pct, click_count, is_bounce, utm_source, utm_medium, utm_campaign,
    utm_term, utm_content, custom_props, created_at)
SELECT
    LOWER(CONVERT(CHAR(36), NEWID())),
    ((value - 1) % 500) + 1,
    CONCAT('https://example.com/page/', value % 1000),
    CONCAT('https://ref.example.com/', value % 50),
    N'Mozilla/5.0 (compatible; bench)',
    CONCAT('10.', value % 256, '.', (value / 256) % 256, '.1'),
    CASE value % 3 WHEN 0 THEN 'US' WHEN 1 THEN 'DE' ELSE 'UA' END,
    CONCAT(N'Region ', value % 20), CONCAT(N'City ', value % 50),
    CASE value % 3 WHEN 0 THEN N'desktop' WHEN 1 THEN N'mobile' ELSE N'tablet' END,
    N'Chrome', N'Linux',
    1920, 1080, 1280, 720, value % 3000, value % 1500, value % 60000,
    value % 100, value % 20,
    CAST(CASE WHEN value % 5 = 0 THEN 1 ELSE 0 END AS BIT),
    N'google', N'cpc', CONCAT(N'campaign_', value % 10),
    N'term', N'content', CONCAT(N'{"v":', value % 100, N'}'),
    DATEADD(SECOND, -value, SYSUTCDATETIME())
FROM GENERATE_SERIES(1, 5000);
GO
