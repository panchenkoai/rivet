-- seeds/common/mssql.sql — SELF-CONTAINED canonical seed (DDL + data + garbage).
-- Requires SQL Server 2022+ (GENERATE_SERIES). DDL from src/bin/seed/mssql.rs
-- SCHEMA_DDL; fill = the seeder's GENERATE_SERIES fast path; garbage appended.
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS page_views;
DROP TABLE IF EXISTS content_items;
DROP TABLE IF EXISTS users;
DROP VIEW IF EXISTS orders_sparse_for_export;
DROP TABLE IF EXISTS orders_sparse;
DROP TABLE IF EXISTS orders_coalesce;
DROP TABLE IF EXISTS rivet_type_matrix;
DROP TABLE IF EXISTS rivet_type_matrix_full;
CREATE TABLE users (
    id INT IDENTITY(1,1) PRIMARY KEY, name NVARCHAR(100) NOT NULL, email NVARCHAR(200) NOT NULL,
    age INT NULL, balance DECIMAL(12,2) NULL, is_active BIT NOT NULL, bio NVARCHAR(MAX) NULL,
    created_at DATETIME2(6) NOT NULL, updated_at DATETIME2(6) NOT NULL);
CREATE TABLE orders (
    id INT IDENTITY(1,1) PRIMARY KEY, user_id INT NOT NULL, product NVARCHAR(200) NOT NULL,
    quantity INT NOT NULL, price DECIMAL(10,2) NOT NULL, status NVARCHAR(20) NOT NULL,
    notes NVARCHAR(MAX) NULL, ordered_at DATETIME2(6) NOT NULL, updated_at DATETIME2(6) NOT NULL);
CREATE TABLE events (
    id BIGINT IDENTITY(1,1) PRIMARY KEY, user_id INT NOT NULL, event_type NVARCHAR(50) NOT NULL,
    payload NVARCHAR(MAX) NULL, ip_address NVARCHAR(45) NULL, created_at DATETIME2(6) NOT NULL);
CREATE TABLE page_views (
    id BIGINT IDENTITY(1,1) PRIMARY KEY, session_id NVARCHAR(36) NOT NULL, user_id INT NULL,
    url NVARCHAR(MAX) NOT NULL, referrer NVARCHAR(MAX) NULL, user_agent NVARCHAR(MAX) NULL,
    ip_address NVARCHAR(45) NULL, country_code CHAR(2) NULL, region NVARCHAR(100) NULL, city NVARCHAR(100) NULL,
    device_type NVARCHAR(20) NULL, browser NVARCHAR(50) NULL, os NVARCHAR(50) NULL,
    screen_width INT NULL, screen_height INT NULL, viewport_width INT NULL, viewport_height INT NULL,
    page_load_ms INT NULL, dom_ready_ms INT NULL, time_on_page_ms INT NULL,
    scroll_depth_pct SMALLINT NULL, click_count SMALLINT NULL, is_bounce BIT NOT NULL,
    utm_source NVARCHAR(100) NULL, utm_medium NVARCHAR(100) NULL, utm_campaign NVARCHAR(200) NULL,
    utm_term NVARCHAR(200) NULL, utm_content NVARCHAR(200) NULL, custom_props NVARCHAR(MAX) NULL,
    created_at DATETIME2(6) NOT NULL);
CREATE TABLE content_items (
    id BIGINT IDENTITY(1,1) PRIMARY KEY, title NVARCHAR(MAX) NOT NULL, body NVARCHAR(MAX) NOT NULL,
    raw_html NVARCHAR(MAX) NOT NULL, metadata NVARCHAR(MAX) NULL, tags NVARCHAR(MAX) NULL,
    author_name NVARCHAR(100) NOT NULL, author_email NVARCHAR(200) NOT NULL, source_url NVARCHAR(MAX) NULL,
    category NVARCHAR(50) NULL, status NVARCHAR(20) NOT NULL, priority INT NOT NULL, view_count INT NOT NULL,
    comment_count INT NOT NULL, word_count INT NOT NULL, language CHAR(2) NOT NULL,
    published_at DATETIME2(6) NULL, updated_at DATETIME2(6) NOT NULL, created_at DATETIME2(6) NOT NULL,
    extra_data NVARCHAR(MAX) NULL);
GO
INSERT INTO users (name,email,age,balance,is_active,bio,created_at,updated_at)
SELECT CONCAT(N'User ',value), CONCAT('user',value,'@example.com'), 18+(value%48), ROUND((value%200000)+0.99,2),
       CAST(CASE WHEN value%10<>0 THEN 1 ELSE 0 END AS BIT), CASE WHEN value%3<>0 THEN CONCAT(N'seed bio ',value) ELSE NULL END,
       DATEADD(HOUR,value%730,CONVERT(DATETIME2(6),'2023-01-01')), DATEADD(HOUR,value%910,CONVERT(DATETIME2(6),'2023-01-01'))
FROM GENERATE_SERIES(CONVERT(BIGINT,1),CONVERT(BIGINT,150000));
GO
INSERT INTO orders (user_id,product,quantity,price,status,notes,ordered_at,updated_at)
SELECT CAST(LEAST(1+(value-1)/10,2000) AS INT),
       CASE value%10 WHEN 0 THEN N'MacBook Pro 16"' WHEN 1 THEN N'Dell XPS 15' WHEN 2 THEN N'ThinkPad X1 Carbon' WHEN 3 THEN N'Surface Laptop' WHEN 4 THEN N'Ergonomic Chair' WHEN 5 THEN N'Standing Desk' WHEN 6 THEN N'Monitor Arm' WHEN 7 THEN N'USB-C Hub' WHEN 8 THEN N'Mechanical Keyboard' ELSE N'Magic Mouse' END,
       1+(value%10), ROUND(5+(value%4995)+0.0,2),
       CASE value%4 WHEN 0 THEN N'pending' WHEN 1 THEN N'shipped' WHEN 2 THEN N'delivered' ELSE N'cancelled' END,
       CASE WHEN value%3=0 THEN CONCAT(N'note ',value) ELSE NULL END,
       DATEADD(MINUTE,value%730,CONVERT(DATETIME2(6),'2023-01-01')), DATEADD(MINUTE,value%760,CONVERT(DATETIME2(6),'2023-01-01'))
FROM GENERATE_SERIES(CONVERT(BIGINT,1),CONVERT(BIGINT,150000));
GO
INSERT INTO events (user_id,event_type,payload,ip_address,created_at)
SELECT CAST(LEAST(1+(value-1)/25,2000) AS INT),
       CASE value%10 WHEN 0 THEN N'login' WHEN 1 THEN N'logout' WHEN 2 THEN N'page_view' WHEN 3 THEN N'purchase' WHEN 4 THEN N'signup' WHEN 5 THEN N'settings_change' WHEN 6 THEN N'password_reset' WHEN 7 THEN N'search' WHEN 8 THEN N'export' ELSE N'api_call' END,
       CONCAT(N'{"seed":true,"i":',value,N'}'), CONCAT('10.',value%255,'.',(value*7)%255,'.1'),
       DATEADD(MINUTE,value%730,CONVERT(DATETIME2(6),'2023-01-01'))
FROM GENERATE_SERIES(CONVERT(BIGINT,1),CONVERT(BIGINT,150000));
GO
INSERT INTO page_views (session_id,user_id,url,referrer,user_agent,ip_address,country_code,region,city,device_type,browser,os,screen_width,screen_height,viewport_width,viewport_height,page_load_ms,dom_ready_ms,time_on_page_ms,scroll_depth_pct,click_count,is_bounce,utm_source,utm_medium,utm_campaign,utm_term,utm_content,custom_props,created_at)
SELECT LOWER(CONVERT(CHAR(36),NEWID())), CASE WHEN value%3=0 THEN NULL ELSE CAST(1+(value%2000) AS INT) END,
       CONCAT('/page/',value%26), CASE WHEN value%4=0 THEN 'https://google.com' ELSE NULL END, N'Mozilla/5.0 seed',
       CONCAT('192.168.',value%255,'.',(value*3)%254+1),
       CASE value%5 WHEN 0 THEN 'US' WHEN 1 THEN 'GB' WHEN 2 THEN 'DE' WHEN 3 THEN 'FR' ELSE 'CA' END,
       CONCAT(N'Region ',value%10), CONCAT(N'City ',value%20),
       CASE value%3 WHEN 0 THEN N'desktop' WHEN 1 THEN N'mobile' ELSE N'tablet' END,
       CASE value%3 WHEN 0 THEN N'chrome' WHEN 1 THEN N'firefox' ELSE N'safari' END,
       CASE value%3 WHEN 0 THEN N'macOS' WHEN 1 THEN N'Windows' ELSE N'Linux' END,
       1280+(value%2560),720+(value%1440),800+(value%1200),600+(value%900),
       100+(value%5000),50+(value%2500),500+(value%60000),CAST(value%101 AS SMALLINT),CAST(value%51 AS SMALLINT),
       CAST(CASE WHEN value%3=0 THEN 1 ELSE 0 END AS BIT),
       CASE WHEN value%4=0 THEN N'google' ELSE NULL END, CASE WHEN value%4=0 THEN N'cpc' ELSE NULL END,
       CASE WHEN value%8=0 THEN N'spring_sale' ELSE NULL END, NULL, NULL,
       CASE WHEN value%5=0 THEN N'{"seed":true}' ELSE NULL END, DATEADD(MINUTE,value%730,CONVERT(DATETIME2(6),'2023-01-01'))
FROM GENERATE_SERIES(CONVERT(BIGINT,1),CONVERT(BIGINT,150000));
GO
INSERT INTO content_items (title,body,raw_html,metadata,tags,author_name,author_email,source_url,category,status,priority,view_count,comment_count,word_count,language,published_at,updated_at,created_at,extra_data)
SELECT CONCAT(N'Seed title ',value), REPLICATE(CAST(N'lorem ipsum ' AS NVARCHAR(MAX)),20),
       CONCAT(N'<p>',REPLICATE(CAST(N'lorem ipsum ' AS NVARCHAR(MAX)),20),N'</p>'),
       CONCAT(N'{"seed":true,"i":',value,N'}'), N'rust,postgres,data', CONCAT(N'Author ',value%1000), CONCAT('author',value%1000,'@example.com'),
       CONCAT('https://blog.example.com/posts/',value),
       CASE value%3 WHEN 0 THEN N'engineering' WHEN 1 THEN N'product' ELSE N'tutorial' END,
       CASE value%3 WHEN 0 THEN N'draft' WHEN 1 THEN N'review' ELSE N'published' END,
       value%5,value%100000,value%500,200,'en',
       CASE WHEN value%3<>0 THEN DATEADD(DAY,value%365,CONVERT(DATETIME2(6),'2024-01-01')) ELSE NULL END,
       DATEADD(DAY,value%400,CONVERT(DATETIME2(6),'2024-01-01')), DATEADD(DAY,value%730,CONVERT(DATETIME2(6),'2023-01-01')), N'{"revisions":1}'
FROM GENERATE_SERIES(CONVERT(BIGINT,1),CONVERT(BIGINT,150000));
GO

-- Sparse BIGINT ids — the 3-row sparse-footgun fixture (parity with PG/MySQL).
CREATE TABLE orders_sparse (id BIGINT PRIMARY KEY, payload NVARCHAR(MAX) NOT NULL);
GO
CREATE VIEW orders_sparse_for_export AS
SELECT id, payload, ROW_NUMBER() OVER (ORDER BY id) AS chunk_rownum FROM orders_sparse;
GO
INSERT INTO orders_sparse (id, payload) VALUES (1,N's0'),(2000001,N's1'),(4000001,N's2');
GO
-- Composite-cursor fixture (ADR-0007): ~33% NULL updated_at (deterministic value%3)
-- forces COALESCE(updated_at, created_at) progression.
CREATE TABLE orders_coalesce (
    id BIGINT IDENTITY(1,1) PRIMARY KEY, product NVARCHAR(200) NOT NULL, quantity INT NOT NULL,
    price DECIMAL(10,2) NOT NULL, updated_at DATETIME2(6) NULL, created_at DATETIME2(6) NOT NULL);
CREATE INDEX idx_orders_coalesce_updated_at ON orders_coalesce(updated_at);
CREATE INDEX idx_orders_coalesce_created_at ON orders_coalesce(created_at);
GO
INSERT INTO orders_coalesce (product,quantity,price,updated_at,created_at)
SELECT CASE value%5 WHEN 0 THEN N'MacBook Pro 16"' WHEN 1 THEN N'Dell XPS 15' WHEN 2 THEN N'ThinkPad X1 Carbon' WHEN 3 THEN N'Surface Laptop' ELSE N'Ergonomic Chair' END,
       1+(value%10), ROUND(5+(value%4995)+0.0,2),
       CASE WHEN value%3=0 THEN NULL ELSE DATEADD(DAY,value%365,CONVERT(DATETIME2(6),'2025-01-01')) END,
       DATEADD(DAY,value%365,CONVERT(DATETIME2(6),'2024-01-01'))
FROM GENERATE_SERIES(CONVERT(BIGINT,1),CONVERT(BIGINT,150000));
GO
-- Type-matrix demo (parity with PG/MySQL) — SQL Server native types.
CREATE TABLE rivet_type_matrix (
    id BIGINT PRIMARY KEY, label NVARCHAR(200) NOT NULL, amount DECIMAL(18,2) NULL, fee DECIMAL(18,6) NULL,
    created_at DATETIME2(6) NOT NULL, created_at_tz DATETIMEOFFSET(6) NOT NULL,
    raw_bytes VARBINARY(4) NOT NULL, uid UNIQUEIDENTIFIER NOT NULL, attrs NVARCHAR(MAX) NULL);
GO
INSERT INTO rivet_type_matrix (id,label,amount,fee,created_at,created_at_tz,raw_bytes,uid,attrs) VALUES
  (1,N'payments-like',0.10,0.000001,'2035-08-07 09:08:07.987654','2035-08-07 09:08:07.987654 +00:00',0x00FF0123,'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380011',N'{"tier":"gold","n":1}'),
  (2,N'payments-like',0.20,0.000002,'2019-02-03 03:07:06.554433','2019-02-03 08:07:06.554433 +05:00',0xDEADBEEF,'B0EEBC99-9C0B-4EF8-BB6D-6BB9BD380022',N'["a","b"]'),
  (3,N'payments-like',999999999999.99,10.123456,'2020-01-15 00:00:00.000001','2020-01-15 00:00:00.000001 +00:00',0xCAFE,'C0EEBC99-9C0B-4EF8-BB6D-6BB9BD380033',N'{"big":true}'),
  (4,N'payments-like',-100.05,-0.123456,'2021-06-30 12:59:59.999999','2021-06-30 12:59:59.999999 +00:00',0x00,'D0EEBC99-9C0B-4EF8-BB6D-6BB9BD380044',N'{}');
GO
-- Full type-matrix — SQL Server native scalar types (its analogue of PG arrays/
-- enum and MySQL bit/year: datetimeoffset, uniqueidentifier, varbinary, real).
CREATE TABLE rivet_type_matrix_full (
    id BIGINT PRIMARY KEY, flag BIT, tiny_col TINYINT, small_col SMALLINT, int_col INT, real_col REAL,
    date_col DATE, time_col TIME(6), dto_col DATETIMEOFFSET(6), uid_col UNIQUEIDENTIFIER, vb_col VARBINARY(4));
GO
INSERT INTO rivet_type_matrix_full (id,flag,tiny_col,small_col,int_col,real_col,date_col,time_col,dto_col,uid_col,vb_col) VALUES
  (1,1,255,32767,2147483647,3.14,'2024-03-15','14:30:00.123456','2024-03-15 14:30:00.123456 +02:00','A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380011',0xDEADBEEF),
  (2,0,0,-32768,-2147483648,-1.5,'1970-01-01','00:00:00.000000','1970-01-01 00:00:00.000000 +00:00','B0EEBC99-9C0B-4EF8-BB6D-6BB9BD380022',0x00000000),
  (3,NULL,NULL,NULL,0,0.0,'2000-02-29','23:59:59.999999','2000-02-29 23:59:59.999999 -05:00',NULL,NULL);
GO

-- === GARBAGE PROFILE ===
GO
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

-- NAME-TRAP: a column literally named `id` that is NOT a PK and NOT indexed.
-- init picks keyset ONLY for a catalog single-column PRIMARY KEY (never by the
-- name `id`), so this must NOT scaffold `chunk_by_key: id` — it falls to range/
-- full and `check` flags the missing index. Proves every keyset key is indexed.
DROP TABLE IF EXISTS ext.unindexed_id;
CREATE TABLE ext.unindexed_id (id BIGINT NOT NULL, label NVARCHAR(200) NOT NULL, amount INT NOT NULL);
INSERT INTO ext.unindexed_id (id, label, amount)
SELECT value, CONCAT(N'row', value), value % 100 FROM GENERATE_SERIES(CAST(1 AS BIGINT), CAST(150000 AS BIGINT));
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
