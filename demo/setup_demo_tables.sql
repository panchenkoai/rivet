-- Demo fixtures: varied table shapes to exercise rivet init/plan/apply
-- Drops and recreates only the demo-specific tables (keeps the existing
-- rivet dev tables alone).
BEGIN;

DROP TABLE IF EXISTS transactions, audit_log, sessions, product_catalog,
                     logs_archive, email_queue, metric_samples CASCADE;

-- 1) transactions — 300k rows, strong numeric id, indexed created_at.
--    Target: incremental OR chunked by id; strong cursor candidate.
CREATE TABLE transactions AS
SELECT
    i AS id,
    (i % 2000) + 1 AS user_id,
    (((i * 173) % 100000)::numeric / 100.0) AS amount,
    CASE (i % 4) WHEN 0 THEN 'pending' WHEN 1 THEN 'completed' WHEN 2 THEN 'refunded' ELSE 'disputed' END AS status,
    (TIMESTAMP '2024-01-01' + (i * INTERVAL '47 seconds')) AS created_at,
    (TIMESTAMP '2024-01-01' + (i * INTERVAL '53 seconds')) AS updated_at
FROM generate_series(1, 300000) AS s(i);
ALTER TABLE transactions ADD PRIMARY KEY (id);
CREATE INDEX idx_transactions_updated_at ON transactions(updated_at);

-- 2) audit_log — 800k rows, heavy chunked scenario, no cursor index.
--    Target: chunked + heavy; shares source_group 'replica_primary'.
CREATE TABLE audit_log AS
SELECT
    i AS id,
    (i % 5000) + 1 AS actor_id,
    md5(i::text) AS action_hash,
    CASE (i % 6)
        WHEN 0 THEN 'login'
        WHEN 1 THEN 'logout'
        WHEN 2 THEN 'update'
        WHEN 3 THEN 'delete'
        WHEN 4 THEN 'create'
        ELSE 'view'
    END AS action,
    (TIMESTAMP '2024-06-01' + (i * INTERVAL '3 seconds')) AS logged_at
FROM generate_series(1, 800000) AS s(i);
ALTER TABLE audit_log ADD PRIMARY KEY (id);
-- deliberately no index on logged_at — "degraded" preflight

-- 3) sessions — 50k rows, indexed session_start — clean incremental.
CREATE TABLE sessions AS
SELECT
    i AS id,
    md5((i || 'sess')::text)::uuid AS session_uuid,
    (i % 2000) + 1 AS user_id,
    (TIMESTAMP '2025-01-01' + (i * INTERVAL '1 minute')) AS session_start,
    (TIMESTAMP '2025-01-01' + (i * INTERVAL '1 minute') + ((i % 3600) * INTERVAL '1 second')) AS session_end
FROM generate_series(1, 50000) AS s(i);
ALTER TABLE sessions ADD PRIMARY KEY (id);
CREATE INDEX idx_sessions_start ON sessions(session_start);

-- 4) product_catalog — 3k rows, small reference table.
CREATE TABLE product_catalog (
    sku VARCHAR(24) PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    category VARCHAR(60) NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    active BOOLEAN DEFAULT true,
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);
INSERT INTO product_catalog(sku, name, category, price, active, updated_at)
SELECT
    'SKU-' || lpad(i::text, 6, '0'),
    'Product ' || i,
    CASE (i % 5) WHEN 0 THEN 'electronics' WHEN 1 THEN 'books' WHEN 2 THEN 'apparel' WHEN 3 THEN 'grocery' ELSE 'home' END,
    ((i * 37) % 50000)::numeric / 100.0,
    (i % 7) <> 0,
    TIMESTAMP '2024-01-01' + (i * INTERVAL '1 hour')
FROM generate_series(1, 3000) AS s(i);

-- 5) logs_archive — 100k rows, nullable updated_at — coalesce fallback demo.
CREATE TABLE logs_archive AS
SELECT
    i AS id,
    md5(i::text)::text AS payload,
    (TIMESTAMP '2024-01-01' + (i * INTERVAL '1 minute'))::timestamp AS created_at,
    CASE WHEN (i % 3) = 0 THEN NULL ELSE (TIMESTAMP '2024-06-01' + (i * INTERVAL '45 seconds'))::timestamp END AS updated_at
FROM generate_series(1, 100000) AS s(i);
ALTER TABLE logs_archive ADD PRIMARY KEY (id);
CREATE INDEX idx_logs_archive_created ON logs_archive(created_at);

-- 6) email_queue — 20k rows, pending/sent, no timestamp cursor.
CREATE TABLE email_queue (
    id BIGSERIAL PRIMARY KEY,
    to_email VARCHAR(200) NOT NULL,
    subject VARCHAR(300) NOT NULL,
    body TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    scheduled_at TIMESTAMP NOT NULL DEFAULT now()
);
INSERT INTO email_queue(to_email, subject, body, status, scheduled_at)
SELECT
    'user' || i || '@example.com',
    'Notification #' || i,
    repeat('lorem ', (i % 30) + 1),
    CASE (i % 3) WHEN 0 THEN 'pending' WHEN 1 THEN 'sent' ELSE 'failed' END,
    TIMESTAMP '2025-01-01' + (i * INTERVAL '3 minutes')
FROM generate_series(1, 20000) AS s(i);

-- 7) metric_samples — 400k rows, numeric id, no timestamp cursor (pure int surrogate).
--    Shares source_group 'replica_analytics' with logs_archive.
CREATE TABLE metric_samples (
    id BIGSERIAL PRIMARY KEY,
    host VARCHAR(80) NOT NULL,
    metric_name VARCHAR(80) NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    captured_at TIMESTAMP NOT NULL
);
INSERT INTO metric_samples(host, metric_name, value, captured_at)
SELECT
    'host-' || lpad(((i % 50) + 1)::text, 3, '0'),
    CASE (i % 4) WHEN 0 THEN 'cpu_user' WHEN 1 THEN 'cpu_system' WHEN 2 THEN 'mem_used' ELSE 'disk_io' END,
    ((i * 97) % 100000)::double precision / 100.0,
    TIMESTAMP '2025-01-01' + (i * INTERVAL '2 seconds')
FROM generate_series(1, 400000) AS s(i);

COMMIT;

ANALYZE transactions, audit_log, sessions, product_catalog,
        logs_archive, email_queue, metric_samples;
