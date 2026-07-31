-- LIGHT CDC seed — PostgreSQL. Same schema + formulas as the common seed,
-- 10 rows per table: enough for a CDC test to have a baseline, cheap enough
-- to never slow a slot/stream test down.

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

INSERT INTO users
SELECT n, 'user' || n || '@example.com', 'User ' || n,
       (ARRAY['US','DE','FR','GB','UA','PL','ES','IT','NL','SE'])[(n % 10) + 1],
       (n % 7) <> 0,
       ((n * 37) % 1000000)::numeric / 100,
       TIMESTAMP '2020-01-01 00:00:00' + (n % 1000) * INTERVAL '1 day' + (n % 86400) * INTERVAL '1 second'
FROM generate_series(1, 10) AS n;

INSERT INTO orders
SELECT n, (n % 10) + 1,
       ((n * 17) % 500000)::numeric / 100,
       (ARRAY['new','paid','shipped','done','cancelled'])[(n % 5) + 1],
       DATE '2021-01-01' + (n % 730),
       TIMESTAMP '2021-01-01 00:00:00' + (n % 730) * INTERVAL '1 day' + (n % 86400) * INTERVAL '1 second'
FROM generate_series(1, 10) AS n;

INSERT INTO events
SELECT n, (n % 10) + 1,
       (ARRAY['view','click','signup','login','logout','purchase','refund','error'])[(n % 8) + 1],
       '{"seq":' || n || ',"grp":' || (n % 100) || '}',
       TIMESTAMP '2022-01-01 00:00:00' + (n % 31536000) * INTERVAL '1 second'
FROM generate_series(1, 10) AS n;
