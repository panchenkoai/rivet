-- Minimal E2E seed for MySQL 8.0+.
-- Populates only the tables required by run_e2e.sh (users, orders, events).
-- Uses a recursive CTE numbers table to avoid loops/procedures.
--
-- Usage:
--   mysql -h 127.0.0.1 -u rivet -privet rivet < dev/e2e/seed_mysql.sql
--
-- Row counts: 500 users · 2 500 orders · 5 000 events

SET cte_max_recursion_depth = 10000;
SET foreign_key_checks = 0;
TRUNCATE events;
TRUNCATE orders;
TRUNCATE users;
SET foreign_key_checks = 1;

-- ── numbers CTE helper (1..5000) ─────────────────────────────────────────────

-- ── users (500) ───────────────────────────────────────────────────────────────

INSERT INTO users (name, email, age, balance, is_active, created_at, updated_at)
WITH RECURSIVE n(i) AS (
    SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 500
)
SELECT
    CONCAT('User ', i),
    CONCAT('user', i, '@example.com'),
    18 + (i % 60),
    ROUND(100 + (i % 9000) + 0.99, 2),
    (i % 5 <> 0),
    DATE_SUB(NOW(), INTERVAL (500 - i) HOUR),
    DATE_SUB(NOW(), INTERVAL (500 - i) HOUR)
FROM n;

-- ── orders (2500 = 5 per user) ───────────────────────────────────────────────

INSERT INTO orders (user_id, product, quantity, price, status, ordered_at, updated_at)
WITH RECURSIVE n(i) AS (
    SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 2500
)
SELECT
    1 + FLOOR((i - 1) / 5),
    ELT((i % 5) + 1, 'Widget','Gadget','Doohickey','Thingamajig','Whatchamacallit'),
    1 + (i % 10),
    ROUND(9.99 + (i % 990), 2),
    ELT((i % 5) + 1, 'pending','paid','shipped','delivered','cancelled'),
    DATE_SUB(NOW(), INTERVAL (2500 - i) MINUTE),
    DATE_SUB(NOW(), INTERVAL (2500 - i) MINUTE)
FROM n;

-- ── events (5000 = 10 per user) ──────────────────────────────────────────────

INSERT INTO events (user_id, event_type, ip_address, created_at)
WITH RECURSIVE n(i) AS (
    SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 5000
)
SELECT
    1 + FLOOR((i - 1) / 10),
    ELT((i % 5) + 1, 'click','view','purchase','signup','logout'),
    CONCAT('10.', i % 255, '.', (i * 7) % 255, '.1'),
    DATE_SUB(NOW(), INTERVAL (5000 - i) MINUTE)
FROM n;
