-- seeds/common/mysql.sql — SELF-CONTAINED canonical seed (DDL + data + garbage). MySQL 8.0+.
-- Assembled from dev/mysql/init.sql (DDL) + a fast-profile CTE fill + dev/garbage/mysql.sql.

SET FOREIGN_KEY_CHECKS=0;
DROP TABLE IF EXISTS content_items, page_views, events, orders, users, orders_sparse, orders_coalesce, rivet_type_matrix, rivet_type_matrix_full;
SET FOREIGN_KEY_CHECKS=1;

CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200) NOT NULL,
    age INT,
    balance DECIMAL(12,2),
    is_active BOOLEAN DEFAULT true,
    bio TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    product VARCHAR(200) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    notes TEXT,
    ordered_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE events (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    payload JSON,
    ip_address VARCHAR(45),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE INDEX idx_orders_updated_at ON orders(updated_at);
CREATE INDEX idx_events_created_at ON events(created_at);
CREATE INDEX idx_events_user_id ON events(user_id);

-- Wide table, NO index on created_at -- intentionally degraded
CREATE TABLE page_views (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    session_id VARCHAR(36) NOT NULL,
    user_id INT,
    url TEXT NOT NULL,
    referrer TEXT,
    user_agent TEXT,
    ip_address VARCHAR(45),
    country_code CHAR(2),
    region VARCHAR(100),
    city VARCHAR(100),
    device_type VARCHAR(20),
    browser VARCHAR(50),
    os VARCHAR(50),
    screen_width INT,
    screen_height INT,
    viewport_width INT,
    viewport_height INT,
    page_load_ms INT,
    dom_ready_ms INT,
    time_on_page_ms INT,
    scroll_depth_pct SMALLINT,
    click_count SMALLINT,
    is_bounce BOOLEAN NOT NULL DEFAULT false,
    utm_source VARCHAR(100),
    utm_medium VARCHAR(100),
    utm_campaign VARCHAR(200),
    utm_term VARCHAR(200),
    utm_content VARCHAR(200),
    custom_props JSON,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Heavy-text table, NO index on created_at -- worst case for memory
CREATE TABLE content_items (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    title TEXT NOT NULL,
    body LONGTEXT NOT NULL,
    raw_html LONGTEXT NOT NULL,
    metadata JSON,
    tags TEXT,
    author_name VARCHAR(100) NOT NULL,
    author_email VARCHAR(200) NOT NULL,
    source_url TEXT,
    category VARCHAR(50),
    status VARCHAR(20) NOT NULL DEFAULT 'draft',
    priority INT NOT NULL DEFAULT 0,
    view_count INT NOT NULL DEFAULT 0,
    comment_count INT NOT NULL DEFAULT 0,
    word_count INT NOT NULL DEFAULT 0,
    language CHAR(2) NOT NULL DEFAULT 'en',
    published_at DATETIME,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    extra_data JSON
);

-- Sparse BIGINT ids — chunked mode demo (MySQL 8+ window functions)
CREATE TABLE orders_sparse (
    id BIGINT PRIMARY KEY,
    payload TEXT NOT NULL
);

CREATE OR REPLACE VIEW orders_sparse_for_export AS
SELECT
    id,
    payload,
    ROW_NUMBER() OVER (ORDER BY id) AS chunk_rownum
FROM orders_sparse;

-- Composite cursor fixture (ADR-0007): some rows have NULL in the primary
-- `updated_at` column, forcing `COALESCE(updated_at, created_at)` progression.
CREATE TABLE orders_coalesce (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    product VARCHAR(200) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    updated_at DATETIME NULL,                                  -- primary
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP     -- fallback, never NULL
);

CREATE INDEX idx_orders_coalesce_updated_at ON orders_coalesce(updated_at);
CREATE INDEX idx_orders_coalesce_created_at ON orders_coalesce(created_at);

-- ─── Type-matrix demo (parquet → BigQuery / tooling checks) ───
-- See dev/workbench/mysql_type_matrix.yaml and dev/bigquery/type_matrix_bigquery.md
CREATE TABLE rivet_type_matrix (
    id BIGINT PRIMARY KEY,
    label VARCHAR(200) NOT NULL,
    amount DECIMAL(18, 2) NULL,
    fee DECIMAL(18, 6) NULL,
    created_at_dt DATETIME(6) NOT NULL,
    created_at_ts TIMESTAMP(6) NOT NULL,
    raw_bytes BINARY(4) NOT NULL,
    uid VARCHAR(36) NOT NULL,
    extras JSON NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO rivet_type_matrix (
    id, label, amount, fee, created_at_dt, created_at_ts, raw_bytes, uid, extras
) VALUES
  (1, 'payments-like', 0.10, 0.000001,
      '2035-08-07 09:08:07.987654',
      '2035-08-07 09:08:07.987654',
      UNHEX('00ff0123'),
      'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380011',
      JSON_OBJECT('tier', 'gold', 'n', 1)),
  (2, 'payments-like', 0.20, 0.000002,
      '2019-02-03 03:07:06.554433',
      '2019-02-03 03:07:06.554433',
      UNHEX('deadbeef'),
      'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380022',
      JSON_ARRAY('a', 'b')),
  (3, 'payments-like', 999999999999.99, 10.123456,
      '2020-01-15 00:00:00.000001',
      '2020-01-15 00:00:00.000001',
      UNHEX('cafe'),
      'c0eebc99-9c0b-4ef8-bb6d-6bb9bd380033',
      CAST('{"big":true}' AS JSON)),
  (4, 'payments-like', -100.05, -0.123456,
      '2021-06-30 12:59:59.999999',
      '2021-06-30 12:59:59.999999',
      UNHEX('00'),
      'd0eebc99-9c0b-4ef8-bb6d-6bb9bd380044',
      CAST('{}' AS JSON));

-- ─── Full type-matrix: covers every Rivet-mapped MySQL type ────────────────
-- See dev/workbench/mysql_type_matrix.yaml and tests/live_type_golden.rs
CREATE TABLE rivet_type_matrix_full (
    id            BIGINT PRIMARY KEY,
    flag          BOOLEAN,                         -- TINYINT(1) → Bool
    bit1_col      BIT(1),                          -- BIT(1)     → Bool
    bit8_col      BIT(8),                          -- BIT(8)     → Int64
    tiny_col      TINYINT,                         -- TINYINT    → Int16
    date_col      DATE,                            -- DATE       → Date32
    time_col      TIME(6),                         -- TIME(6)    → Time64(µs)
    year_col      YEAR,                            -- YEAR       → Int16
    enum_col      ENUM('a', 'b', 'c'),             -- ENUM       → Utf8
    varbinary_col VARBINARY(4),                    -- VARBINARY  → Binary
    blob_col      BLOB                             -- BLOB       → Binary
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO rivet_type_matrix_full
    (id, flag, bit1_col, bit8_col, tiny_col, date_col, time_col, year_col, enum_col, varbinary_col, blob_col)
VALUES
  (1, TRUE,  b'1', b'10101010',  127, '2024-03-15', '14:30:00.123456', 2024, 'b', 0xDEADBEEF, 0x0102030405),
  (2, FALSE, b'0', b'00000001', -128, '1970-01-01', '00:00:00.000000', 2000, 'a', 0x00000000, 0xCAFE),
  (3, NULL,  NULL, NULL,           0, '2000-02-29', '23:59:59.999999', NULL, NULL, NULL,       NULL);

-- ============================================================================
-- DATA FILL — fast-profile reproduction of the Rust seeder (fast.rs, MySQL path).
-- Recursive CTE = the SQL analog of the seeder's fast path. Requires MySQL 8.0+
-- (recursive CTE). Row counts dogfood-sized; edit each `i < N` to scale.
-- ============================================================================
SET SESSION cte_max_recursion_depth = 200000;

INSERT INTO users (name, email, age, balance, is_active, bio, created_at, updated_at)
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < 2000)
SELECT CONCAT('User ',i), CONCAT('user',i,'@example.com'), 18+(i%48), ROUND(RAND(i)*200000,2),
       (i%10<>0), IF(i%3=0,CONCAT('seed bio ',i),NULL),
       DATE_SUB('2023-01-01', INTERVAL -(i%730) HOUR), DATE_SUB('2023-01-01', INTERVAL -(i%910) HOUR)
FROM n;

INSERT INTO orders (user_id, product, quantity, price, status, notes, ordered_at, updated_at)
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < 20000)
SELECT LEAST(1+FLOOR((i-1)/10),2000),
       ELT((i%10)+1,'MacBook Pro','Dell XPS','ThinkPad','Surface','Ergonomic Chair','Standing Desk','Monitor Arm','USB-C Hub','Mechanical Keyboard','Magic Mouse'),
       1+(i%10), ROUND(5+(i%4995),2),
       ELT((i%4)+1,'pending','shipped','delivered','cancelled'),
       IF(i%3=0,CONCAT('note ',i),NULL),
       DATE_SUB('2023-01-01', INTERVAL -(i%730) MINUTE), DATE_SUB('2023-01-01', INTERVAL -(i%760) MINUTE)
FROM n;

INSERT INTO events (user_id, event_type, payload, ip_address, created_at)
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < 50000)
SELECT LEAST(1+FLOOR((i-1)/25),2000),
       ELT((i%10)+1,'login','logout','page_view','purchase','signup','settings_change','password_reset','search','export','api_call'),
       JSON_OBJECT('seed',TRUE,'i',i), CONCAT('10.',i MOD 255,'.',(i*7) MOD 255,'.1'),
       DATE_SUB('2023-01-01', INTERVAL -(i%730) MINUTE)
FROM n;

INSERT INTO page_views (session_id,user_id,url,referrer,user_agent,ip_address,country_code,region,city,device_type,browser,os,screen_width,screen_height,viewport_width,viewport_height,page_load_ms,dom_ready_ms,time_on_page_ms,scroll_depth_pct,click_count,is_bounce,utm_source,utm_medium,utm_campaign,utm_term,utm_content,custom_props,created_at)
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < 20000)
SELECT LPAD(HEX(i),32,'0'), IF(i%3=0,1+(i%2000),NULL), CONCAT('/page/',i MOD 26),
       IF(i%4=0,'https://google.com',NULL), 'Mozilla/5.0 seed', CONCAT('192.168.',i MOD 255,'.',(i*3) MOD 254 +1),
       ELT((i MOD 5)+1,'US','GB','DE','FR','CA'), CONCAT('Region ',i MOD 10), CONCAT('City ',i MOD 20),
       ELT((i MOD 3)+1,'desktop','mobile','tablet'), ELT((i MOD 3)+1,'chrome','firefox','safari'), ELT((i MOD 3)+1,'macOS','Windows','Linux'),
       1280+(i MOD 2560), 720+(i MOD 1440), 800+(i MOD 1200), 600+(i MOD 900),
       100+(i MOD 5000), 50+(i MOD 2500), 500+(i MOD 60000), i MOD 101, i MOD 51, (i MOD 3=0),
       IF(i MOD 4=0,'google',NULL), IF(i MOD 4=0,'cpc',NULL), IF(i MOD 8=0,'spring_sale',NULL), NULL, NULL,
       IF(i MOD 5=0,JSON_OBJECT('seed',TRUE),NULL), DATE_SUB('2023-01-01', INTERVAL -(i MOD 730) MINUTE)
FROM n;

INSERT INTO content_items (title,body,raw_html,metadata,tags,author_name,author_email,source_url,category,status,priority,view_count,comment_count,word_count,language,published_at,updated_at,created_at,extra_data)
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < 5000)
SELECT CONCAT('Seed title ',i), REPEAT('lorem ipsum ',200), CONCAT('<p>',REPEAT('lorem ipsum ',200),'</p>'),
       JSON_OBJECT('seed',TRUE,'i',i), 'rust,postgres,data', CONCAT('Author ',i MOD 1000), CONCAT('author',i MOD 1000,'@example.com'),
       CONCAT('https://blog.example.com/posts/',i), ELT((i MOD 3)+1,'engineering','product','tutorial'), ELT((i MOD 3)+1,'draft','review','published'),
       i MOD 5, i MOD 100000, i MOD 500, 200, 'en',
       IF(i MOD 3<>0, DATE_SUB('2024-01-01', INTERVAL -(i MOD 365) DAY), NULL),
       DATE_SUB('2024-01-01', INTERVAL -(i MOD 400) DAY), DATE_SUB('2023-01-01', INTERVAL -(i MOD 730) DAY), JSON_OBJECT('revisions',1)
FROM n;

INSERT INTO orders_sparse (id, payload) VALUES (1,'s0'),(2000001,'s1'),(4000001,'s2');

INSERT INTO orders_coalesce (product, quantity, price, updated_at, created_at)
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < 2000)
SELECT ELT((i%5)+1,'MacBook Pro','Dell XPS','ThinkPad','Surface','Ergonomic Chair'), 1+(i%10), ROUND(5+(i%4995),2),
       IF(RAND(i)<0.35, NULL, DATE_SUB('2025-01-01', INTERVAL -(i%365) DAY)),
       DATE_SUB('2024-01-01', INTERVAL -(i%365) DAY)
FROM n;

-- === GARBAGE PROFILE ===
-- Obfuscated GARBAGE-profile fixture (MySQL) — see dev/garbage/postgres.sql for
-- the rationale. Materialized as `ext_`-prefixed tables in the `rivet` database (the rivet
-- MySQL user cannot CREATE a database; the non-default-schema hazard is PG-
-- specific anyway — #13 is n/a for MySQL, which resolves the db from the URL).
-- The MySQL-relevant hazard here is the BIGINT UNSIGNED key. ZERO source
-- identity, ZERO real data — only the profile that triggered the field bugs.
-- Deterministic + idempotent. Seeded via `make seed-garbage`.

DROP TABLE IF EXISTS ext_bigint_pk_dual_ts;
CREATE TABLE ext_bigint_pk_dual_ts (
    id BIGINT PRIMARY KEY,
    payload TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NULL
);
SET SESSION cte_max_recursion_depth = 200000;
INSERT INTO ext_bigint_pk_dual_ts (id, payload, updated_at)
WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 150000)
SELECT n, CONCAT('row', n), NOW() - INTERVAL n MINUTE FROM seq;

DROP TABLE IF EXISTS ext_int_pk_dual_ts;
CREATE TABLE ext_int_pk_dual_ts (
    id INT PRIMARY KEY,
    payload TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NULL
);
INSERT INTO ext_int_pk_dual_ts (id, payload, updated_at)
WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 150000)
SELECT n, CONCAT('row', n), NOW() - INTERVAL n MINUTE FROM seq;

-- The field DB CASTs id AS UNSIGNED — a BIGINT UNSIGNED PK with ids PAST
-- i64::MAX. Keyset must read the unsigned high-water key (#bc512a3), or it
-- loses/duplicates the tail. MySQL-only: PG/MSSQL have no unsigned integer type.
DROP TABLE IF EXISTS ext_bigint_unsigned_pk;
CREATE TABLE ext_bigint_unsigned_pk (
    id BIGINT UNSIGNED PRIMARY KEY,
    payload INT NOT NULL
);
INSERT INTO ext_bigint_unsigned_pk (id, payload)
WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 150000)
SELECT n, n FROM seq;
-- Three ids past i64::MAX (9223372036854775807); u64::MAX = 18446744073709551615.
INSERT INTO ext_bigint_unsigned_pk (id, payload) VALUES
    (18446744073709551613, 100), (18446744073709551614, 101), (18446744073709551615, 102);

-- Sparse key: id span vastly exceeds the row count (the sparse-guard shape).
DROP TABLE IF EXISTS ext_sparse_key;
CREATE TABLE ext_sparse_key (id BIGINT PRIMARY KEY, payload INT NOT NULL);
INSERT INTO ext_sparse_key (id, payload)
WITH RECURSIVE seq AS (SELECT 0 n UNION ALL SELECT n+1 FROM seq WHERE n < 149999)
SELECT 1 + CAST(n AS SIGNED) * 1000000, n FROM seq;

-- Scale-0 DECIMAL PK (Oracle/ERP shape) — an explicit range chunk_column on it
-- must LOUDLY bail (#103); `chunk_by_key: dkey` (keyset) IS accepted.
DROP TABLE IF EXISTS ext_decimal_key;
CREATE TABLE ext_decimal_key (dkey DECIMAL(15,0) PRIMARY KEY, payload TEXT NOT NULL);
INSERT INTO ext_decimal_key (dkey, payload)
WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 150000)
SELECT n, CONCAT('row', n) FROM seq;

-- Keyless, cursorless → full-mode fallback.
DROP TABLE IF EXISTS ext_no_pk_no_ts;
CREATE TABLE ext_no_pk_no_ts (label TEXT NOT NULL, amount INT NOT NULL);
INSERT INTO ext_no_pk_no_ts (label, amount)
WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 150000)
SELECT CONCAT('label', n), n % 100 FROM seq;

-- ── The MESSY reality (distilled from a real stuck run's state DB) ───────────
-- HISTORY/VERSION table: NO `id`, keyed by a non-PK integer `ref_id`. The planner
-- range-CHUNKS on ref_id. In the field these `*_version` tables were the parallel-
-- checkpoint timeouts. Non-unique index on ref_id, no PK.
DROP TABLE IF EXISTS ext_ref_id_history;
CREATE TABLE ext_ref_id_history (
    ref_id BIGINT NOT NULL,
    version INT NOT NULL,
    field TEXT,
    field_cur TEXT,
    sign SMALLINT NOT NULL DEFAULT 1,
    status TEXT,
    -- money as DECIMAL(11,4) VALUE columns — the DOMINANT field profile (177 such
    -- columns across the real DB); init emits a `columns:` override for each.
    cart DECIMAL(11,4),
    earning DECIMAL(11,4),
    subtotal DECIMAL(11,4),
    total DECIMAL(11,4),
    amount_cur TEXT,
    user_cur CHAR(3),
    purchase_type TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NULL,
    KEY ix_ref_id (ref_id)
);
INSERT INTO ext_ref_id_history (ref_id, version, field, field_cur, sign, status, cart, earning, subtotal, total, amount_cur, user_cur, purchase_type, created_at, updated_at)
WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 150000)
SELECT (n % 400) + 1, (n DIV 400) + 1, CONCAT('f', n), 'EUR', IF(n % 2 = 0, 1, -1),
       ELT(1 + n % 3, 'pending','done','void'),
       (n % 900000 + 1) / 100.0, (n % 500000 + 1) / 100.0, (n % 300000 + 1) / 100.0, (n % 1200000 + 1) / 100.0,
       'USD', ELT(1 + n % 3, 'EUR','USD','PLN'), ELT(1 + n % 2, 'a','b'),
       NOW() - INTERVAL n MINUTE, NOW() - INTERVAL n SECOND FROM seq;

-- Keyed by `order_id` (NOT `id`): a unique key makes it keyset-able on a non-`id`
-- key. Wide-ish string columns (md5/currency/subid).
DROP TABLE IF EXISTS ext_order_keyed;
CREATE TABLE ext_order_keyed (
    order_id BIGINT NOT NULL,
    md5 CHAR(32) NOT NULL,
    currency CHAR(3) NOT NULL,
    status TEXT NOT NULL,
    subid TEXT,
    advcampaign_id INT,
    UNIQUE KEY ux_order_id (order_id)
);
INSERT INTO ext_order_keyed (order_id, md5, currency, status, subid, advcampaign_id)
WITH RECURSIVE seq AS (SELECT 1 n UNION ALL SELECT n+1 FROM seq WHERE n < 150000)
SELECT n, MD5(n), ELT(1 + n % 3, 'EUR','USD','PLN'), ELT(1 + n % 2, 'approved','declined'),
       CONCAT('sub', n % 50), (n % 900) + 100 FROM seq;

-- HEAP: no PK, no index → full mode only.
DROP TABLE IF EXISTS ext_heap_no_key;
CREATE TABLE ext_heap_no_key (payload TEXT NOT NULL, n INT NOT NULL);
INSERT INTO ext_heap_no_key (payload, n)
WITH RECURSIVE seq AS (SELECT 1 s UNION ALL SELECT s+1 FROM seq WHERE s < 150000)
SELECT REPEAT('x', 20 + s % 50), s FROM seq;
