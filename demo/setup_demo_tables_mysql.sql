-- Demo fixtures for MySQL 8+ — mirrors demo/setup_demo_tables.sql semantics.
-- Generates 7 varied tables via cross-joined digit table (fast: ~3–5 s total).
--
-- Usage:
--   mysql -h localhost -P 3306 -u rivet -privet rivet < demo/setup_demo_tables_mysql.sql

SET @OLD_SQL_MODE = @@SQL_MODE;
SET SESSION SQL_MODE = 'NO_ENGINE_SUBSTITUTION';
SET SESSION FOREIGN_KEY_CHECKS = 0;

-- ─── helper: numbers(1..1_000_000) via 6-way cross join on digits ─────────
DROP TABLE IF EXISTS _digits;
CREATE TABLE _digits (d TINYINT UNSIGNED) ENGINE=InnoDB;
INSERT INTO _digits VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

DROP TABLE IF EXISTS _numbers;
CREATE TABLE _numbers (n INT UNSIGNED PRIMARY KEY) ENGINE=InnoDB;
INSERT INTO _numbers (n)
SELECT 1 + d1.d + d2.d*10 + d3.d*100 + d4.d*1000 + d5.d*10000 + d6.d*100000
FROM _digits d1, _digits d2, _digits d3, _digits d4, _digits d5, _digits d6;

-- ─── 1) transactions — 300k rows, PK, indexed updated_at (strong cursor) ───
DROP TABLE IF EXISTS transactions;
CREATE TABLE transactions (
    id BIGINT PRIMARY KEY,
    user_id INT NOT NULL,
    amount DECIMAL(12,2) NOT NULL,
    status VARCHAR(16) NOT NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    KEY idx_transactions_updated_at (updated_at)
) ENGINE=InnoDB;
INSERT INTO transactions (id, user_id, amount, status, created_at, updated_at)
SELECT
    n,
    (n % 2000) + 1,
    ((n * 173) % 100000) / 100.0,
    ELT((n % 4) + 1, 'pending', 'completed', 'refunded', 'disputed'),
    TIMESTAMPADD(SECOND, n * 47, '2024-01-01 00:00:00'),
    TIMESTAMPADD(SECOND, n * 53, '2024-01-01 00:00:00')
FROM _numbers WHERE n <= 300000;

-- ─── 2) audit_log — 800k rows, heavy chunked, NO index on timestamp ───────
DROP TABLE IF EXISTS audit_log;
CREATE TABLE audit_log (
    id BIGINT PRIMARY KEY,
    actor_id INT NOT NULL,
    action_hash CHAR(32) NOT NULL,
    action VARCHAR(16) NOT NULL,
    logged_at DATETIME NOT NULL
) ENGINE=InnoDB;
INSERT INTO audit_log (id, actor_id, action_hash, action, logged_at)
SELECT
    n,
    (n % 5000) + 1,
    MD5(CAST(n AS CHAR)),
    ELT((n % 6) + 1, 'login', 'logout', 'update', 'delete', 'create', 'view'),
    TIMESTAMPADD(SECOND, n * 3, '2024-06-01 00:00:00')
FROM _numbers WHERE n <= 800000;

-- ─── 3) sessions — 50k rows, indexed session_start ────────────────────────
DROP TABLE IF EXISTS sessions;
CREATE TABLE sessions (
    id BIGINT PRIMARY KEY,
    session_uuid CHAR(36) NOT NULL,
    user_id INT NOT NULL,
    session_start DATETIME NOT NULL,
    session_end DATETIME NOT NULL,
    KEY idx_sessions_start (session_start)
) ENGINE=InnoDB;
INSERT INTO sessions (id, session_uuid, user_id, session_start, session_end)
SELECT
    n,
    CONCAT(
        SUBSTRING(MD5(CAST(n AS CHAR)), 1, 8), '-',
        SUBSTRING(MD5(CAST(n AS CHAR)), 9, 4), '-',
        SUBSTRING(MD5(CAST(n AS CHAR)), 13, 4), '-',
        SUBSTRING(MD5(CAST(n AS CHAR)), 17, 4), '-',
        SUBSTRING(MD5(CAST(n AS CHAR)), 21, 12)
    ),
    (n % 2000) + 1,
    TIMESTAMPADD(MINUTE, n, '2025-01-01 00:00:00'),
    TIMESTAMPADD(SECOND, (n * 60) + (n % 3600), '2025-01-01 00:00:00')
FROM _numbers WHERE n <= 50000;

-- ─── 4) product_catalog — 3k rows, small reference ────────────────────────
DROP TABLE IF EXISTS product_catalog;
CREATE TABLE product_catalog (
    sku VARCHAR(24) PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    category VARCHAR(60) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    active TINYINT(1) NOT NULL DEFAULT 1,
    updated_at DATETIME NOT NULL
) ENGINE=InnoDB;
INSERT INTO product_catalog (sku, name, category, price, active, updated_at)
SELECT
    CONCAT('SKU-', LPAD(n, 6, '0')),
    CONCAT('Product ', n),
    ELT((n % 5) + 1, 'electronics', 'books', 'apparel', 'grocery', 'home'),
    ((n * 37) % 50000) / 100.0,
    CASE WHEN n % 7 <> 0 THEN 1 ELSE 0 END,
    TIMESTAMPADD(HOUR, n, '2024-01-01 00:00:00')
FROM _numbers WHERE n <= 3000;

-- ─── 5) logs_archive — 100k rows, nullable updated_at (coalesce fallback) ──
DROP TABLE IF EXISTS logs_archive;
CREATE TABLE logs_archive (
    id BIGINT PRIMARY KEY,
    payload TEXT,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NULL,
    KEY idx_logs_archive_created (created_at)
) ENGINE=InnoDB;
INSERT INTO logs_archive (id, payload, created_at, updated_at)
SELECT
    n,
    MD5(CAST(n AS CHAR)),
    TIMESTAMPADD(MINUTE, n, '2024-01-01 00:00:00'),
    CASE WHEN n % 3 = 0 THEN NULL ELSE TIMESTAMPADD(SECOND, n * 45, '2024-06-01 00:00:00') END
FROM _numbers WHERE n <= 100000;

-- ─── 6) email_queue — 20k rows, no timestamp cursor ───────────────────────
DROP TABLE IF EXISTS email_queue;
CREATE TABLE email_queue (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    to_email VARCHAR(200) NOT NULL,
    subject VARCHAR(300) NOT NULL,
    body TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    scheduled_at DATETIME NOT NULL
) ENGINE=InnoDB;
INSERT INTO email_queue (to_email, subject, body, status, scheduled_at)
SELECT
    CONCAT('user', n, '@example.com'),
    CONCAT('Notification #', n),
    REPEAT('lorem ', (n % 30) + 1),
    ELT((n % 3) + 1, 'pending', 'sent', 'failed'),
    TIMESTAMPADD(MINUTE, n * 3, '2025-01-01 00:00:00')
FROM _numbers WHERE n <= 20000;

-- ─── 7a) orders_coalesce — 5k rows, nullable updated_at + NOT NULL created_at
--          Smaller coalesce fixture (parity with the Postgres demo).
DROP TABLE IF EXISTS orders_coalesce;
CREATE TABLE orders_coalesce (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    product VARCHAR(200) NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    updated_at DATETIME NULL,
    created_at DATETIME NOT NULL,
    KEY idx_orders_coalesce_updated_at (updated_at),
    KEY idx_orders_coalesce_created_at (created_at)
) ENGINE=InnoDB;
INSERT INTO orders_coalesce (product, quantity, price, updated_at, created_at)
SELECT
    CONCAT('Product ', n),
    (n % 10) + 1,
    ((n * 31) % 500000) / 100.0,
    CASE WHEN n % 3 = 0 THEN NULL
         ELSE TIMESTAMPADD(SECOND, n * 117, '2025-01-01 00:00:00') END,
    TIMESTAMPADD(SECOND, n * 53, '2024-06-01 00:00:00')
FROM _numbers WHERE n <= 5000;

-- ─── 7b) metric_samples — 400k rows, numeric id, no indexed cursor ────────
DROP TABLE IF EXISTS metric_samples;
CREATE TABLE metric_samples (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    host VARCHAR(80) NOT NULL,
    metric_name VARCHAR(80) NOT NULL,
    value DOUBLE NOT NULL,
    captured_at DATETIME NOT NULL
) ENGINE=InnoDB;
INSERT INTO metric_samples (host, metric_name, value, captured_at)
SELECT
    CONCAT('host-', LPAD(((n % 50) + 1), 3, '0')),
    ELT((n % 4) + 1, 'cpu_user', 'cpu_system', 'mem_used', 'disk_io'),
    ((n * 97) % 100000) / 100.0,
    TIMESTAMPADD(SECOND, n * 2, '2025-01-01 00:00:00')
FROM _numbers WHERE n <= 400000;

-- cleanup helpers
DROP TABLE _numbers;
DROP TABLE _digits;

-- refresh statistics so rivet preflight sees realistic row estimates
ANALYZE TABLE transactions, audit_log, sessions, product_catalog,
              logs_archive, email_queue, orders_coalesce, metric_samples;

SET SESSION FOREIGN_KEY_CHECKS = 1;
SET SESSION SQL_MODE = @OLD_SQL_MODE;
