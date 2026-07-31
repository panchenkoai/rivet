-- Common HEAVY deterministic seed — MySQL dialect (works on BOTH 5.7 and 8.0:
-- no CTEs / window functions; numbers come from a digits cross-join).
-- Formulas match postgres.sql / mssql.sql row-for-row. DATETIME (not
-- TIMESTAMP) so no session-timezone conversion touches the stored values.

CREATE TABLE users (
    id         INT PRIMARY KEY,
    email      VARCHAR(64)   NOT NULL,
    name       VARCHAR(64)   NOT NULL,
    country    VARCHAR(2)    NOT NULL,
    active     TINYINT(1)    NOT NULL,
    balance    DECIMAL(12,2) NOT NULL,
    created_at DATETIME      NOT NULL
);

CREATE TABLE orders (
    id         INT PRIMARY KEY,
    user_id    INT           NOT NULL,
    amount     DECIMAL(12,2) NOT NULL,
    status     VARCHAR(16)   NOT NULL,
    order_date DATE          NOT NULL,
    created_at DATETIME      NOT NULL
);

CREATE TABLE events (
    id          BIGINT PRIMARY KEY,
    user_id     INT          NOT NULL,
    event_type  VARCHAR(16)  NOT NULL,
    payload     VARCHAR(64)  NOT NULL,
    occurred_at DATETIME     NOT NULL
);

CREATE TABLE seed_d (d INT NOT NULL);
INSERT INTO seed_d VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

INSERT INTO users (id, email, name, country, active, balance, created_at)
SELECT n,
       CONCAT('user', n, '@example.com'),
       CONCAT('User ', n),
       ELT((n % 10) + 1, 'US','DE','FR','GB','UA','PL','ES','IT','NL','SE'),
       (n % 7) <> 0,
       CAST((n * 37) % 1000000 AS DECIMAL(14,2)) / 100,
       DATE_ADD(DATE_ADD('2020-01-01 00:00:00', INTERVAL (n % 1000) DAY),
                INTERVAL (n % 86400) SECOND)
FROM (SELECT 1 + a.d + 10*b.d + 100*c.d + 1000*e.d + 10000*f.d AS n
      FROM seed_d a, seed_d b, seed_d c, seed_d e, seed_d f) t
WHERE n <= 50000;

INSERT INTO orders (id, user_id, amount, status, order_date, created_at)
SELECT n,
       (n % 50000) + 1,
       CAST((n * 17) % 500000 AS DECIMAL(14,2)) / 100,
       ELT((n % 5) + 1, 'new','paid','shipped','done','cancelled'),
       DATE_ADD('2021-01-01', INTERVAL (n % 730) DAY),
       DATE_ADD(DATE_ADD('2021-01-01 00:00:00', INTERVAL (n % 730) DAY),
                INTERVAL (n % 86400) SECOND)
FROM (SELECT 1 + a.d + 10*b.d + 100*c.d + 1000*e.d + 10000*f.d + 100000*g.d AS n
      FROM seed_d a, seed_d b, seed_d c, seed_d e, seed_d f, seed_d g) t
WHERE n <= 200000;

INSERT INTO events (id, user_id, event_type, payload, occurred_at)
SELECT n,
       (n % 50000) + 1,
       ELT((n % 8) + 1, 'view','click','signup','login','logout','purchase','refund','error'),
       CONCAT('{"seq":', n, ',"grp":', n % 100, '}'),
       DATE_ADD('2022-01-01 00:00:00', INTERVAL (n % 31536000) SECOND)
FROM (SELECT 1 + a.d + 10*b.d + 100*c.d + 1000*e.d + 10000*f.d + 100000*g.d AS n
      FROM seed_d a, seed_d b, seed_d c, seed_d e, seed_d f, seed_d g) t
WHERE n <= 500000;

DROP TABLE seed_d;

CREATE INDEX ix_orders_user_id  ON orders (user_id);
CREATE INDEX ix_events_user_id  ON events (user_id);
CREATE INDEX ix_events_occurred ON events (occurred_at);
