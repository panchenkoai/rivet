-- Common HEAVY deterministic seed — PostgreSQL dialect.
-- Every value is a pure function of the row number n, with the SAME formulas
-- in mysql.sql and mssql.sql, so counts / sums / per-row values match across
-- all engines exactly. Sizes: users 50k, orders 200k, events 500k.
-- Runs once, on first boot of an empty data volume (docker-entrypoint-initdb.d).

CREATE TABLE users (
    id         INTEGER PRIMARY KEY,
    email      VARCHAR(64)  NOT NULL,
    name       VARCHAR(64)  NOT NULL,
    country    VARCHAR(2)   NOT NULL,
    active     BOOLEAN      NOT NULL,
    balance    NUMERIC(12,2) NOT NULL,
    created_at TIMESTAMP    NOT NULL
);

CREATE TABLE orders (
    id         INTEGER PRIMARY KEY,
    user_id    INTEGER      NOT NULL,
    amount     NUMERIC(12,2) NOT NULL,
    status     VARCHAR(16)  NOT NULL,
    order_date DATE         NOT NULL,
    created_at TIMESTAMP    NOT NULL
);

CREATE TABLE events (
    id         BIGINT PRIMARY KEY,
    user_id    INTEGER      NOT NULL,
    event_type VARCHAR(16)  NOT NULL,
    payload    VARCHAR(64)  NOT NULL,
    occurred_at TIMESTAMP   NOT NULL
);

INSERT INTO users (id, email, name, country, active, balance, created_at)
SELECT n,
       'user' || n || '@example.com',
       'User ' || n,
       (ARRAY['US','DE','FR','GB','UA','PL','ES','IT','NL','SE'])[(n % 10) + 1],
       (n % 7) <> 0,
       ((n * 37) % 1000000)::numeric / 100,
       TIMESTAMP '2020-01-01 00:00:00'
         + (n % 1000)  * INTERVAL '1 day'
         + (n % 86400) * INTERVAL '1 second'
FROM generate_series(1, 50000) AS n;

INSERT INTO orders (id, user_id, amount, status, order_date, created_at)
SELECT n,
       (n % 50000) + 1,
       ((n * 17) % 500000)::numeric / 100,
       (ARRAY['new','paid','shipped','done','cancelled'])[(n % 5) + 1],
       DATE '2021-01-01' + (n % 730),
       TIMESTAMP '2021-01-01 00:00:00'
         + (n % 730)   * INTERVAL '1 day'
         + (n % 86400) * INTERVAL '1 second'
FROM generate_series(1, 200000) AS n;

INSERT INTO events (id, user_id, event_type, payload, occurred_at)
SELECT n,
       (n % 50000) + 1,
       (ARRAY['view','click','signup','login','logout','purchase','refund','error'])[(n % 8) + 1],
       '{"seq":' || n || ',"grp":' || (n % 100) || '}',
       TIMESTAMP '2022-01-01 00:00:00' + (n % 31536000) * INTERVAL '1 second'
FROM generate_series(1, 500000) AS n;

CREATE INDEX ix_orders_user_id  ON orders (user_id);
CREATE INDEX ix_events_user_id  ON events (user_id);
CREATE INDEX ix_events_occurred ON events (occurred_at);
